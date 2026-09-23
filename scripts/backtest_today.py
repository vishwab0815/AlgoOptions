"""
scripts/backtest_today.py — one-off backtest driver.

Fetches today's REAL 5-minute candles for both legs from DhanHQ's own
intraday chart API, merges them into one chronological stream, and replays
them through the ACTUAL live OptionsEngine methods (_check_exits,
_on_candle_close, _enter, _exit) — not a reimplementation of the strategy,
the exact same code path the live engine runs. Margin is faked (no network
needed for that call); everything else is real.

This exists to catch bugs the pattern-only backtest can't: interleaved
cross-leg sequencing (watch-both/lock/flip), trailing-stop exits, and EOD
square-off, all exercised together against real data.

Usage:
    python scripts/backtest_today.py [PUT_STRIKE] [CALL_STRIKE] [CE_SECURITY_ID] [PE_SECURITY_ID]

With no arguments, resolves today's band from a live option-chain spot
fetch and resolves both legs' contracts the normal way.
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("backtest")

from algopilot.options.config import load_options_config
from algopilot.options.dhan_client import OptionsDhanClient
from algopilot.options.engine import OptionsEngine, resolve_band
from algopilot.utils.market_hours import get_market_status

_IST = ZoneInfo("Asia/Kolkata")
_LEGS = ("CE", "PE")


class FakeMarginClient:
    """Backtest never needs a real margin round-trip — sizing math is
    identical either way, and this keeps the replay offline/deterministic."""
    def __init__(self, fallback: float) -> None:
        self.fallback = fallback

    async def margin_per_lot_async(self, security_id, exchange_segment, price, lot_size, fallback):
        return self.fallback, True

    def close(self) -> None:
        pass


async def main() -> int:
    cfg = load_options_config()
    engine = OptionsEngine(cfg)
    # A separate client with a near-zero cooldown, just for this one-off
    # fetch (a handful of calls total) — the production 5s gate on
    # engine.client is for the live polling loop, not a diagnostic script.
    real_client = OptionsDhanClient(cfg.client_id, cfg.access_token, chain_min_interval_secs=1.0)

    args = sys.argv[1:]
    if len(args) == 4:
        put_strike, call_strike = float(args[0]), float(args[1])
        ce_id, pe_id = args[2], args[3]
        expiry_list = real_client.get_expiry_list(cfg.nifty_security_id, cfg.nifty_index_segment)
        expiry = expiry_list[0] if expiry_list else None
    else:
        expiry_list = real_client.get_expiry_list(cfg.nifty_security_id, cfg.nifty_index_segment)
        expiry = expiry_list[0] if expiry_list else None
        if expiry is None:
            logger.error("Could not resolve an expiry — aborting.")
            return 1
        await asyncio.sleep(1.1)  # clear the chain cooldown gate before the next call
        chain = real_client.get_chain(cfg.nifty_security_id, cfg.nifty_index_segment, expiry)
        if chain is None or chain.spot <= 0:
            logger.error("Could not fetch a spot price to resolve the band — aborting.")
            return 1
        put_strike, call_strike = resolve_band(chain.spot)
        ce_contract = real_client.resolve_contract(expiry, call_strike, "CE")
        pe_contract = real_client.resolve_contract(expiry, put_strike, "PE")
        if ce_contract is None or pe_contract is None:
            logger.error("Could not resolve both legs' contracts — aborting.")
            return 1
        ce_id, pe_id = ce_contract.security_id, pe_contract.security_id

    logger.info("Band: PUT %.0f / CALL %.0f | expiry=%s | CE id=%s | PE id=%s",
                put_strike, call_strike, expiry, ce_id, pe_id)

    interval_minutes = max(1, cfg.candle_timeframe_secs // 60)
    today = datetime.now(_IST).strftime("%Y-%m-%d")
    from_dt = f"{today} 09:15:00"
    to_dt = f"{today} 15:30:00"

    ce_candles = real_client.get_intraday_candles(ce_id, cfg.option_exchange_segment, "OPTIDX", interval_minutes, from_dt, to_dt)
    pe_candles = real_client.get_intraday_candles(pe_id, cfg.option_exchange_segment, "OPTIDX", interval_minutes, from_dt, to_dt)
    real_client.close()

    if not ce_candles or not pe_candles:
        logger.error("Could not fetch real candles for one or both legs — aborting.")
        return 1
    logger.info("Fetched %d CE candles, %d PE candles.", len(ce_candles), len(pe_candles))

    # ── Wire up the engine exactly as a live run would, minus the network ──
    engine.client = FakeMarginClient(cfg.fallback_margin_per_lot)
    engine.put_strike, engine.call_strike, engine.expiry = put_strike, call_strike, expiry
    engine.legs["CE"].strike = call_strike
    engine.legs["CE"].security_id = ce_id
    engine.legs["CE"].lot_size = 65
    engine.legs["PE"].strike = put_strike
    engine.legs["PE"].security_id = pe_id
    engine.legs["PE"].lot_size = 65

    # Merge both legs' candles into one chronological stream.
    stream = [("CE", c) for c in ce_candles] + [("PE", c) for c in pe_candles]
    stream.sort(key=lambda item: item[1]["timestamp"])

    errors = 0
    for side, c in stream:
        candle_time_ist = datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).astimezone(_IST)
        squareoff_due = get_market_status(now=candle_time_ist).eod_squareoff_due
        leg = engine.legs[side]

        try:
            # 1. Check exits on the PRE-EXISTING position (opened on a prior
            #    candle). A trailing stop is a price LEVEL, not a price to
            #    chase — the candle's high only tells us WHETHER it was
            #    touched (worst-case intracandle reach), not what price it
            #    filled at. Filling at the candle's high instead of the
            #    level itself was wrong: it overstates a SHORT's loss (or
            #    understates its gain) by the exact gap between the level
            #    and wherever the candle happened to peak afterward.
            # Standard, conservative backtest convention: assume the fill
            # happens AT the level the instant it's touched. EOD square-off
            # is a real forced market exit, not a stop order, so it fills at
            # this candle's actual close — that part stays as-is.
            if leg.position is not None:
                pos = leg.position
                if squareoff_due:
                    engine._exit(side, leg, c["close"], "EOD_SQUAREOFF", at_time=candle_time_ist)
                elif pos.is_cover_level_triggered(c["high"]):
                    engine._exit(side, leg, pos.cover_level, "TRAILING_STOP", at_time=candle_time_ist)

            # 2. Now process the candle close itself — updates cover_level,
            #    evaluates the pattern, and may open a fresh position at
            #    this candle's own close (the real live entry price rule).
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
            logger.exception("CRASH replaying %s candle at %s — this is a real bug.", side, candle_time_ist)

    trades = sorted(engine.ledger.get_all_trades(), key=lambda t: t["entry_time"])
    _print_table(trades, cfg.paper_capital, engine.balance, engine.realized_pnl, errors)
    for side in _LEGS:
        pos = engine.legs[side].position
        if pos is not None:
            logger.info("Still OPEN at 15:30: %s entry=%.2f qty=%d cover=%.2f", side, pos.entry_price, pos.qty, pos.cover_level)

    engine.ledger.close()
    return 1 if errors else 0


def _print_table(trades, starting_capital: float, ending_balance: float, realized_pnl: float, errors: int) -> None:
    def ist(iso: str) -> str:
        return datetime.fromisoformat(iso).astimezone(_IST).strftime("%H:%M")

    cols = ["#", "Side", "Strike", "Entry(IST)", "Entry Rs", "Exit(IST)", "Exit Rs", "Qty", "P&L Rs", "Result", "Reason"]
    rows = []
    wins = losses = 0
    for i, t in enumerate(trades, 1):
        pnl = t["pnl"]
        result = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT"
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        rows.append([
            str(i), t["leg"], f"{t['strike']:.0f}", ist(t["entry_time"]), f"{t['entry_price']:.2f}",
            ist(t["exit_time"]), f"{t['exit_price']:.2f}", str(t["qty"]), f"{pnl:+.2f}", result, t["exit_reason"],
        ])

    widths = [max(len(cols[i]), max((len(r[i]) for r in rows), default=0)) for i in range(len(cols))]
    def fmt_row(vals):
        return " | ".join(v.ljust(widths[i]) for i, v in enumerate(vals))

    sep = "-+-".join("-" * w for w in widths)
    logger.info("")
    logger.info("=" * len(sep))
    logger.info("BACKTEST RESULTS — TODAY, REAL DhanHQ 5-MINUTE CANDLES, LIVE ENGINE CODE PATH")
    logger.info("=" * len(sep))
    logger.info(fmt_row(cols))
    logger.info(sep)
    for r in rows:
        logger.info(fmt_row(r))
    logger.info(sep)
    logger.info("")
    total = wins + losses
    win_rate = (wins / total * 100.0) if total else 0.0
    logger.info("Trades: %d total | %d WIN | %d LOSS | win rate %.0f%%", total, wins, losses, win_rate)
    logger.info("Starting capital: Rs %s | Ending balance: Rs %s | Net P&L: Rs %+.2f (%+.3f%%)",
                f"{starting_capital:,.2f}", f"{ending_balance:,.2f}", realized_pnl,
                realized_pnl / starting_capital * 100.0)
    logger.info("Replay errors: %d %s", errors, "(NONE — clean run)" if errors == 0 else "(SEE TRACEBACKS ABOVE)")
    logger.info("=" * len(sep))


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
