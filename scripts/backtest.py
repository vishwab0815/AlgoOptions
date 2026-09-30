"""
scripts/backtest.py — replay real DhanHQ days through the live engine's own code.

    python scripts/backtest.py 2026-09-25                  one day
    python scripts/backtest.py 2026-09-22 2026-09-25       every weekday in the range
    python scripts/backtest.py 2026-09-25 --independent    every signal on each leg,
                                                           no one-trade-at-a-time rule
    python scripts/backtest.py 2026-09-25 --verbose        also print the engine's own log

Entries, stops, skips, charges and P&L all go through OptionsEngine itself
(_on_candle_close / _check_exits / _exit), so a backtest cannot drift from
what the live engine does.

Replaces backtest_today.py, backtest_week.py, backtest_options.py and
_compare_sequencing.py, which between them:
  - wrote into the PRODUCTION ledger (data/options_ledger.db) — and would now
    also have filled candle_log with replayed bars tagged "live";
  - replayed past days on TODAY's expiry contract;
  - kept DhanHQ's post-close bars (15:30, 15:35), where a replay could open
    a trade after the market had shut;
  - stamped each candle's END time as its START (every time 5 min early);
  - filled the 15:00 exit at that bar's close, not the first tick after 15:00;
  - (backtest_options.py) still simulated the removed profit ratchet.

Known limits, stated rather than hidden:
  - inside a bar the path is assumed O-L-H-C (rising bar) or O-H-L-C
    (falling bar) — see _walk_bar;
  - the strike band is the day's OPENING band; intraday re-banding when spot
    crosses a hundred is not replayed;
  - inside one 5-minute bar the order of high and low is unknown, so a stop is
    assumed hit whenever the bar's high reaches it;
  - an already-expired weekly series can't be fetched from any API, so for
    older days the nearest STILL-LISTED expiry is used and flagged.
"""
import argparse
import asyncio
import logging
import shutil
import sys
import tempfile
import time
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from algopilot.core.candle_builder import Candle
from algopilot.options.config import load_options_config
from algopilot.options.dhan_client import OptionsDhanClient
from algopilot.options.engine import OptionsEngine, _in_session, resolve_band

_IST = ZoneInfo("Asia/Kolkata")
_LEGS = ("CE", "PE")


class _OfflineMarginClient:
    """Margin is the only network call the engine makes while trading; a
    past day has no historical margin API anyway, so size with the configured
    fallback (1 lot at Rs 2.5L either way — live NIFTY margin is ~Rs 1.9-2L)."""
    def __init__(self, fallback: float) -> None:
        self.fallback = fallback
        self.auth_failed = False

    async def margin_per_lot_async(self, security_id, exchange_segment, price, lot_size, fallback):
        return self.fallback, False

    def close(self) -> None:
        pass


def _walk_bar(e, side, leg, c, at) -> None:
    """Feed one bar to the engine as the ticks it most likely went through:
    open, then low before high on a rising bar (O-L-H-C), high before low on
    a falling one (O-H-L-C). On every rise a tick is added exactly at each
    exit level passed (stop, profit lock), so an exit fills at its level —
    or at the open when the bar gapped past it. The order of high and low
    inside a 5-minute bar is a guess; that's the backtest's main limit."""
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    path = [o, l, h, cl] if cl >= o else [o, h, l, cl]
    prev = None
    for p in path:
        pos = leg.position
        if pos is None:
            return
        if prev is not None and p > prev:
            for level in sorted(x for x in (pos.cover_level, pos.lock_price) if x > 0 and prev < x < p):
                e._check_exits(side, leg, level, False, at_time=at)
                if leg.position is None:
                    return
        e._check_exits(side, leg, p, False, at_time=at)
        prev = p


def _weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _pick_expiry(listed, day: date, roll_on_expiry_day: bool):
    upcoming = sorted(e for e in listed if e >= day.isoformat())
    if not upcoming:
        return None, None
    chosen = upcoming[0]
    if chosen == day.isoformat() and roll_on_expiry_day and len(upcoming) > 1:
        chosen = upcoming[1]
    gap = (date.fromisoformat(chosen) - day).days
    note = None
    if gap >= 7:
        note = (f"the weekly series that was nearest on {day} has expired and can't be "
                f"fetched; using {chosen} ({gap} days out — more time value than the real trade had)")
    return chosen, note


def _new_engine(cfg, db_path: str) -> OptionsEngine:
    # ALWAYS paper, whatever .env says. With OPTIONS_PAPER_TRADING=false a
    # replay would otherwise place a REAL order at every historical signal.
    # official_candles=False: the replayed bars ARE DhanHQ's bars already.
    engine = OptionsEngine(replace(cfg, db_path=db_path, paper_trading=True, official_candles=False))
    assert engine.broker is None, "backtest must never have a live broker"
    engine.client.close()
    engine.client = _OfflineMarginClient(cfg.fallback_margin_per_lot)
    return engine


async def _replay_day(cfg, client, day: date, expiry: str, independent: bool, tmp: Path):
    ds = day.isoformat()
    # From 09:00, never 09:15:00: a request starting exactly at 09:15:00 gets
    # a 09:15 bar without the opening prints (see engine._backfill_leg).
    spot = client.get_intraday_candles("13", "IDX_I", "INDEX", 5, f"{ds} 09:00:00", f"{ds} 15:30:00")
    spot = [c for c in (spot or []) if _in_session(c["timestamp"])]
    if not spot:
        return None, "no index data (holiday, or older than DhanHQ's intraday window)"
    put, call = resolve_band(spot[0]["open"])
    strikes = {"CE": call, "PE": put}

    bars, seed = {}, {}
    seed_from = (day - timedelta(days=5)).isoformat()
    for side in _LEGS:
        con = client.resolve_contract(expiry, strikes[side], side)
        if con is None:
            return None, f"{side} {strikes[side]:.0f} not listed for expiry {expiry}"
        time.sleep(0.4)
        raw = client.get_intraday_candles(con.security_id, "NSE_FNO", "OPTIDX", 5,
                                          f"{seed_from} 09:00:00", f"{ds} 15:30:00")
        raw = [c for c in (raw or []) if _in_session(c["timestamp"])]
        on_day = lambda c: datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).astimezone(_IST).date() == day
        # Earlier sessions only continue the Heikin-Ashi series (as live does).
        seed[side] = [c for c in raw if not on_day(c)]
        bars[side] = (con, [c for c in raw if on_day(c)])
        if not bars[side][1]:
            return None, f"no candles for {side} {strikes[side]:.0f}"

    # One engine normally; one per leg in --independent mode, so each leg
    # trades every signal regardless of the other.
    engines = {}
    for side in _LEGS:
        key = side if independent else "both"
        if key not in engines:
            engines[key] = _new_engine(cfg, str(tmp / f"{ds}_{key}.db"))
        e = engines[key]
        e.put_strike, e.call_strike, e.expiry = put, call, expiry
        leg = e.legs[side]
        leg.strike, leg.security_id, leg.lot_size = strikes[side], bars[side][0].security_id, bars[side][0].lot_size
        for c in seed[side]:
            leg.ha_engine.append_candle({"open": c["open"], "high": c["high"], "low": c["low"],
                                         "close": c["close"], "volume": c.get("volume", 0.0)})

    tf = timedelta(seconds=cfg.candle_timeframe_secs)
    stream = sorted(((c["timestamp"], side, c) for side in _LEGS for c in bars[side][1]),
                    key=lambda x: (x[0], x[1]))
    for ts, side, c in stream:
        e = engines[side if independent else "both"]
        leg = e.legs[side]
        start = datetime.fromtimestamp(ts, tz=timezone.utc)
        start_ist = start.astimezone(_IST)

        # Exits inside this bar, priced as the live engine would see them.
        pos = leg.position
        if pos is not None:
            if start_ist.strftime("%H:%M") >= "15:00":
                e._check_exits(side, leg, c["open"], True, at_time=start_ist)      # first tick after 15:00
            else:
                _walk_bar(e, side, leg, c, start_ist)

        candle = Candle(symbol=side, exchange_segment=2, security_id=leg.security_id,
                        start_ts=start, end_ts=start + tf, open=c["open"], high=c["high"],
                        low=c["low"], close=c["close"], volume=c.get("volume", 0.0))
        squareoff_at_close = (start + tf).astimezone(_IST).strftime("%H:%M") >= "15:00"
        if independent:
            # The 'my sheet' model: every signal on each leg, so the live
            # engine's fresh-pattern-after-exit rule doesn't apply here.
            e._last_exit_at.clear()
        await e._on_candle_close(side, leg, candle, squareoff_at_close)

    trades, skips = [], Counter()
    for e in engines.values():
        trades += e.ledger.get_all_trades(limit=10000)
        rows = [(r["details"],) for r in e.ledger.get_events("SIGNAL_SKIPPED")]
        for (details,) in rows:
            reason = details.split("reason=", 1)[1].split(" ha_close=")[0]
            skips[reason.split(" - ")[0] if " - " in reason else reason] += 1
        e.ledger.close()
    return dict(day=ds, expiry=expiry, put=put, call=call,
                trades=sorted(trades, key=lambda t: t["entry_time"]), skips=skips), None


def _hm(iso: str) -> str:
    return datetime.fromisoformat(iso).astimezone(_IST).strftime("%H:%M")


async def main() -> int:
    ap = argparse.ArgumentParser(description="Replay real days through the live engine code.")
    ap.add_argument("start", help="YYYY-MM-DD")
    ap.add_argument("end", nargs="?", help="YYYY-MM-DD (inclusive); default = start")
    ap.add_argument("--independent", action="store_true",
                    help="trade every signal on each leg (the 'my sheet' model)")
    ap.add_argument("--verbose", action="store_true", help="show the engine's own log lines")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s")
    cfg = load_options_config()
    client = OptionsDhanClient(cfg.client_id, cfg.access_token, chain_min_interval_secs=1.0)
    listed = client.get_expiry_list(cfg.nifty_security_id, cfg.nifty_index_segment)
    if client.auth_failed or not listed:
        print("Could not read the expiry list — see the error above (usually an expired token).")
        return 1

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else start
    tmp = Path(tempfile.mkdtemp(prefix="algopilot_bt_"))
    mode = "independent legs (every signal)" if args.independent else "engine rules (one trade at a time)"
    print(f"\nBACKTEST {start} -> {end} | {mode} | isolated ledger (production untouched)\n")

    all_trades, all_skips = [], Counter()
    try:
        for day in _weekdays(start, end):
            expiry, note = _pick_expiry(listed, day, cfg.roll_on_expiry_day)
            if expiry is None:
                print(f"{day}: skipped — no listed expiry on/after this day")
                continue
            res, why = await _replay_day(cfg, client, day, expiry, args.independent, tmp)
            if res is None:
                print(f"{day}: skipped — {why}")
                continue
            if note:
                print(f"{day}: NOTE {note}")
            g = sum(t["gross_pnl"] for t in res["trades"])
            n = sum(t["pnl"] for t in res["trades"])
            print(f"{day}  PE {res['put']:.0f} / CE {res['call']:.0f}  exp {expiry}  "
                  f"{len(res['trades'])} trades  gross {g:+9.2f}  net {n:+9.2f}"
                  + (f"  | skipped: {dict(res['skips'])}" if res["skips"] else ""))
            all_trades += [dict(t, day=res["day"]) for t in res["trades"]]
            all_skips += res["skips"]
    finally:
        client.close()
        shutil.rmtree(tmp, ignore_errors=True)

    if not all_trades:
        print("\nNo trades.")
        return 0
    print(f"\n{'day':<11}{'leg':<4}{'strike':>7}{'in':>7}{'sell':>8}{'out*':>7}{'buy':>8}"
          f"{'gross':>10}{'charges':>9}{'net':>10}  reason")
    print("-" * 94)
    for t in all_trades:
        print(f"{t['day']:<11}{t['leg']:<4}{t['strike']:>7.0f}{_hm(t['entry_time']):>7}{t['entry_price']:>8.2f}"
              f"{_hm(t['exit_time']):>7}{t['exit_price']:>8.2f}{t['gross_pnl']:>+10.2f}{t['charges']:>9.2f}"
              f"{t['pnl']:>+10.2f}  {t['exit_reason']}")
    G = sum(t["gross_pnl"] for t in all_trades)
    C = sum(t["charges"] for t in all_trades)
    N = sum(t["pnl"] for t in all_trades)
    W = sum(1 for t in all_trades if t["pnl"] > 0)
    print("-" * 94)
    print(f"{len(all_trades)} trades | {W} win / {len(all_trades) - W} loss (net) | "
          f"gross {G:+,.2f} - charges {C:,.2f} = NET {N:+,.2f}")
    if all_skips:
        print("signals not traded: " + ", ".join(f"{k} x{v}" for k, v in all_skips.most_common()))
    print("* 'out' is the 5-minute bar in which the stop was hit (exact second not in bar data).")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
