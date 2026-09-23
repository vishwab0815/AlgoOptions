"""
scripts/backtest_week.py — multi-day backtest driver.

Same principle as backtest_today.py (real DhanHQ candles replayed through the
ACTUAL live OptionsEngine methods — not a reimplementation), extended across
several days, each with its own real opening spot, its own resolved band,
and — unlike backtest_today.py's fake margin client — REAL per-trade margin
calculator lookups, so the reported leverage/margin figures are genuine
DhanHQ numbers for each day's actual contract, not a hardcoded guess.

Known, disclosed limitation: DhanHQ has no historical margin API, so a
margin lookup for a past day's contract reflects TODAY's volatility for
that strike, not the volatility on the day it actually traded. This is the
best available real-data proxy, not a claim of perfect historical margin
reproduction.

A contract only exists for lookup from the day it's first listed — a
weekly series that already expired has no security id, no scrip-master
entry, and no chart data available via any current API call. Days before
that are silently skipped with a clear log line, never faked.

Usage:
    python scripts/backtest_week.py 2026-09-15 2026-09-16 2026-09-17 2026-09-18
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("backtest_week")

from algopilot.options.config import load_options_config
from algopilot.options.dhan_client import OptionsDhanClient
from algopilot.options.engine import OptionsEngine, resolve_band

_IST = ZoneInfo("Asia/Kolkata")
_LEGS = ("CE", "PE")


async def replay_day(engine: OptionsEngine, client: OptionsDhanClient, day: str, expiry: str) -> dict:
    """Returns a dict with that day's trades, leverage figures, and any skip reason."""
    from algopilot.utils.market_hours import get_market_status

    spot_candles = client.get_intraday_candles(
        "13", "IDX_I", "INDEX", 5, f"{day} 09:15:00", f"{day} 09:30:00",
    )
    if not spot_candles:
        return {"day": day, "skipped": "no spot data for this day (holiday or before this account's data window)"}
    opening_spot = spot_candles[0]["open"]
    put_strike, call_strike = resolve_band(opening_spot)

    ce_contract = client.resolve_contract(expiry, call_strike, "CE")
    pe_contract = client.resolve_contract(expiry, put_strike, "PE")
    if ce_contract is None or pe_contract is None:
        return {
            "day": day, "skipped": (
                f"PUT {put_strike:.0f}/CALL {call_strike:.0f} not resolvable for expiry {expiry} — "
                "this day likely traded a different (now-expired) weekly series with no current API access."
            ),
        }

    ce_candles = client.get_intraday_candles(ce_contract.security_id, "NSE_FNO", "OPTIDX", 5, f"{day} 09:15:00", f"{day} 15:30:00")
    pe_candles = client.get_intraday_candles(pe_contract.security_id, "NSE_FNO", "OPTIDX", 5, f"{day} 09:15:00", f"{day} 15:30:00")
    if not ce_candles or not pe_candles:
        return {"day": day, "skipped": f"no option candle data for PUT {put_strike:.0f}/CALL {call_strike:.0f} on this day."}

    # Fresh per-day state — mirrors the live engine's own day-rollover reset.
    engine.put_strike, engine.call_strike, engine.expiry = put_strike, call_strike, expiry
    engine.active_side = "ANY"
    engine.legs = {side: engine._new_leg(side) for side in _LEGS}
    engine.legs["CE"].strike, engine.legs["CE"].security_id, engine.legs["CE"].lot_size = call_strike, ce_contract.security_id, ce_contract.lot_size
    engine.legs["PE"].strike, engine.legs["PE"].security_id, engine.legs["PE"].lot_size = put_strike, pe_contract.security_id, pe_contract.lot_size

    stream = [("CE", c) for c in ce_candles] + [("PE", c) for c in pe_candles]
    stream.sort(key=lambda item: item[1]["timestamp"])

    trades_before = len(engine.ledger.get_all_trades())
    errors = 0
    for side, c in stream:
        candle_time_ist = datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).astimezone(_IST)
        squareoff_due = get_market_status(now=candle_time_ist).eod_squareoff_due
        leg = engine.legs[side]
        try:
            if leg.position is not None:
                pos = leg.position
                if squareoff_due:
                    engine._exit(side, leg, c["close"], "EOD_SQUAREOFF", at_time=candle_time_ist)
                elif pos.is_cover_level_triggered(c["high"]):
                    engine._exit(side, leg, pos.cover_level, "TRAILING_STOP", at_time=candle_time_ist)

            fake_candle = type("C", (), {
                "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
                "volume": c.get("volume", 0.0), "as_dict": lambda self=None: {
                    "open": c["open"], "high": c["high"], "low": c["low"],
                    "close": c["close"], "volume": c.get("volume", 0.0),
                },
                "start_ts": candle_time_ist.astimezone(timezone.utc),
                "end_ts": candle_time_ist.astimezone(timezone.utc),
            })()
            await engine._on_candle_close(side, leg, fake_candle, squareoff_due)
        except Exception:
            errors += 1
            logger.exception("CRASH replaying %s candle on %s at %s.", side, day, candle_time_ist)

    all_trades = engine.ledger.get_all_trades()
    day_trades = sorted(
        (t for t in all_trades if t["entry_time"].startswith(day)),
        key=lambda t: t["entry_time"],
    )

    return {
        "day": day, "put_strike": put_strike, "call_strike": call_strike, "expiry": expiry,
        "trades": day_trades, "errors": errors,
    }


async def main() -> int:
    days = sys.argv[1:]
    if not days:
        logger.error("Usage: python scripts/backtest_week.py YYYY-MM-DD [YYYY-MM-DD ...]")
        return 1

    cfg = load_options_config()
    engine = OptionsEngine(cfg)
    # Real client throughout — including real margin lookups, not a fake one.
    real_client = OptionsDhanClient(cfg.client_id, cfg.access_token, chain_min_interval_secs=1.0)
    engine.client = real_client

    expiries = real_client.get_expiry_list(cfg.nifty_security_id, cfg.nifty_index_segment)
    if not expiries:
        logger.error("Could not resolve an expiry — aborting.")
        return 1
    expiry = expiries[0]
    logger.info("Using expiry %s for all days (this week's active weekly series).", expiry)

    results = []
    for day in days:
        await asyncio.sleep(1.2)  # stay clear of the chain/margin cooldown between days
        res = await replay_day(engine, real_client, day, expiry)
        results.append(res)
        if res.get("skipped"):
            logger.warning("SKIPPED %s: %s", day, res["skipped"])
        else:
            logger.info(
                "%s: band PUT %.0f / CALL %.0f | %d trade(s), %d error(s)",
                day, res["put_strike"], res["call_strike"], len(res["trades"]), res["errors"],
            )

    _print_week_table(results, cfg, real_client)

    real_client.close()
    engine.ledger.close()
    total_errors = sum(r.get("errors", 0) for r in results)
    return 1 if total_errors else 0


def _print_week_table(results, cfg, client: OptionsDhanClient) -> None:
    def ist(iso: str) -> str:
        return datetime.fromisoformat(iso).astimezone(_IST).strftime("%m-%d %H:%M")

    cols = ["Day", "Side", "Strike", "Entry", "Entry Rs", "Exit", "Exit Rs", "Qty", "P&L Rs", "Result", "Margin/lot Rs", "Leverage"]
    rows = []
    wins = losses = 0
    total_pnl = 0.0
    skipped_days = []

    for r in results:
        if r.get("skipped"):
            skipped_days.append((r["day"], r["skipped"]))
            continue
        for t in r["trades"]:
            pnl = t["pnl"]
            total_pnl += pnl
            result = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT"
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
            # Real margin/leverage for this exact trade's contract & entry price.
            margin_str, lev_str = "-", "-"
            try:
                contract_side = t["leg"]
                strike_val = t["strike"]
                sec = client.resolve_contract(r["expiry"], strike_val, contract_side)
                if sec is not None:
                    resp = client._session.post(
                        "https://api.dhan.co/v2/margincalculator",
                        json={"securityId": sec.security_id, "exchangeSegment": "NSE_FNO",
                              "transactionType": "SELL", "quantity": sec.lot_size,
                              "productType": "INTRADAY", "price": t["entry_price"]},
                        timeout=5.0,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        margin_str = f"{data.get('totalMargin', 0.0):.0f}"
                        lev_str = str(data.get("leverage", "-"))
            except Exception:
                pass
            rows.append([
                t["entry_time"][5:10], t["leg"], f"{t['strike']:.0f}", ist(t["entry_time"]).split()[1],
                f"{t['entry_price']:.2f}", ist(t["exit_time"]).split()[1], f"{t['exit_price']:.2f}",
                str(t["qty"]), f"{pnl:+.2f}", result, margin_str, lev_str,
            ])

    widths = [max(len(cols[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(cols))]
    def fmt_row(vals):
        return " | ".join(v.ljust(widths[i]) for i, v in enumerate(vals))
    sep = "-+-".join("-" * w for w in widths)

    logger.info("")
    logger.info("=" * len(sep))
    logger.info("WEEKLY BACKTEST — REAL DhanHQ DATA, REAL MARGIN LOOKUPS, LIVE ENGINE CODE PATH")
    logger.info("=" * len(sep))
    if skipped_days:
        for day, reason in skipped_days:
            logger.info("SKIPPED %s — %s", day, reason)
        logger.info("-" * len(sep))
    logger.info(fmt_row(cols))
    logger.info(sep)
    for r in rows:
        logger.info(fmt_row(r))
    logger.info(sep)

    total = wins + losses
    win_rate = (wins / total * 100.0) if total else 0.0
    ending = cfg.paper_capital + total_pnl
    logger.info("")
    logger.info("Trades: %d total | %d WIN | %d LOSS | win rate %.0f%%", total, wins, losses, win_rate)
    logger.info("Starting capital: Rs %s | Ending balance: Rs %s | Net P&L: Rs %+.2f (%+.3f%%)",
                f"{cfg.paper_capital:,.2f}", f"{ending:,.2f}", total_pnl, total_pnl / cfg.paper_capital * 100.0)
    logger.info("=" * len(sep))


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
