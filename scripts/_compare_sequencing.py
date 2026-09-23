"""
Temporary comparison script (not a permanent feature) — reuses the exact
same live OptionsEngine._enter/_check_exits/_on_candle_close code path as
backtest_week.py, differing ONLY in what happens to active_side after an
exit: the real engine flips to specifically the OTHER leg; this variant
resets to "ANY" (watch both again, same leg can re-fire immediately).
Everything else — pattern detection, trailing stop, margin, P&L — is
identical, real DhanHQ data, same week already validated.
"""
import asyncio, logging, sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
logging.basicConfig(level=logging.WARNING, format="%(message)s")  # quiet — only want the summary

from algopilot.options.config import load_options_config
from algopilot.options.dhan_client import OptionsDhanClient
from algopilot.options.engine import OptionsEngine, resolve_band
from algopilot.utils.market_hours import get_market_status

_IST = ZoneInfo("Asia/Kolkata")
_LEGS = ("CE", "PE")


class WatchBothEngine(OptionsEngine):
    """Identical to OptionsEngine except: after an exit, watch BOTH legs
    again instead of locking to specifically the opposite one."""
    def _exit(self, side, leg, exit_price, reason, at_time=None):
        super()._exit(side, leg, exit_price, reason, at_time=at_time)
        self.active_side = "ANY"


async def replay_day(engine, client, day, expiry):
    spot_candles = client.get_intraday_candles("13", "IDX_I", "INDEX", 5, f"{day} 09:15:00", f"{day} 09:30:00")
    if not spot_candles:
        return None
    opening_spot = spot_candles[0]["open"]
    put_strike, call_strike = resolve_band(opening_spot)
    ce_contract = client.resolve_contract(expiry, call_strike, "CE")
    pe_contract = client.resolve_contract(expiry, put_strike, "PE")
    if ce_contract is None or pe_contract is None:
        return None
    ce_candles = client.get_intraday_candles(ce_contract.security_id, "NSE_FNO", "OPTIDX", 5, f"{day} 09:15:00", f"{day} 15:30:00")
    pe_candles = client.get_intraday_candles(pe_contract.security_id, "NSE_FNO", "OPTIDX", 5, f"{day} 09:15:00", f"{day} 15:30:00")
    if not ce_candles or not pe_candles:
        return None

    engine.put_strike, engine.call_strike, engine.expiry = put_strike, call_strike, expiry
    engine.active_side = "ANY"
    engine.legs = {side: engine._new_leg(side) for side in _LEGS}
    engine.legs["CE"].strike, engine.legs["CE"].security_id, engine.legs["CE"].lot_size = call_strike, ce_contract.security_id, ce_contract.lot_size
    engine.legs["PE"].strike, engine.legs["PE"].security_id, engine.legs["PE"].lot_size = put_strike, pe_contract.security_id, pe_contract.lot_size

    stream = sorted([("CE", c) for c in ce_candles] + [("PE", c) for c in pe_candles], key=lambda i: i[1]["timestamp"])
    for side, c in stream:
        t_ist = datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).astimezone(_IST)
        squareoff_due = get_market_status(now=t_ist).eod_squareoff_due
        leg = engine.legs[side]
        if leg.position is not None:
            pos = leg.position
            if squareoff_due:
                engine._exit(side, leg, c["close"], "EOD_SQUAREOFF", at_time=t_ist)
            elif pos.is_cover_level_triggered(c["high"]):
                engine._exit(side, leg, pos.cover_level, "TRAILING_STOP", at_time=t_ist)
        fake_candle = type("C", (), {
            "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": 0.0,
            "as_dict": lambda self=None: {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": 0.0},
            "start_ts": t_ist.astimezone(timezone.utc), "end_ts": t_ist.astimezone(timezone.utc),
        })()
        await engine._on_candle_close(side, leg, fake_candle, squareoff_due)
    return [t for t in engine.ledger.get_all_trades() if t["entry_time"].startswith(day)]


async def main():
    days = ["2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"]
    cfg = load_options_config()
    client = OptionsDhanClient(cfg.client_id, cfg.access_token, chain_min_interval_secs=1.0)
    expiry = client.get_expiry_list(cfg.nifty_security_id, cfg.nifty_index_segment)[0]

    for label, engine_cls, dbfile in [("ALTERNATING (current)", OptionsEngine, "data/_cmp_alt.db"),
                                        ("WATCH-BOTH (proposed)", WatchBothEngine, "data/_cmp_both.db")]:
        Path(dbfile).unlink(missing_ok=True)
        Path(dbfile + "-wal").unlink(missing_ok=True)
        Path(dbfile + "-shm").unlink(missing_ok=True)
        engine = engine_cls.__new__(engine_cls)
        OptionsEngine.__init__(engine, cfg)
        engine.ledger.close()
        from algopilot.options.ledger import OptionsLedger
        engine.ledger = OptionsLedger(dbfile)
        engine.client = client

        all_trades = []
        for day in days:
            await asyncio.sleep(1.2)
            trades = await replay_day(engine, client, day, expiry)
            if trades:
                all_trades.extend(trades)

        wins = sum(1 for t in all_trades if t["pnl"] > 0)
        losses = sum(1 for t in all_trades if t["pnl"] < 0)
        total_pnl = sum(t["pnl"] for t in all_trades)
        print(f"\n=== {label} ===")
        print(f"Trades: {len(all_trades)} | Wins: {wins} | Losses: {losses} | Net P&L: Rs {total_pnl:+.2f}")
        for t in sorted(all_trades, key=lambda t: t["entry_time"]):
            et = datetime.fromisoformat(t["entry_time"]).astimezone(_IST).strftime("%m-%d %H:%M")
            xt = datetime.fromisoformat(t["exit_time"]).astimezone(_IST).strftime("%H:%M")
            print(f"  {et} {t['leg']} entry={t['entry_price']:.2f} exit@{xt}={t['exit_price']:.2f} pnl={t['pnl']:+.2f}")
        engine.ledger.close()

    client.close()

if __name__ == "__main__":
    asyncio.run(main())
