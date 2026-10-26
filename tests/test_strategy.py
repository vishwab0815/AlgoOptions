"""
tests/test_strategy.py — the trading rules, offline: a fake broker, a temporary ledger, no network, no real orders.

    python tests/test_strategy.py

Checks the strategy settings (entry at candle-2 HA Low - 0.5, stop HA High + 0.5,
profit lock, 6 trades a day, 15:10), a paper sell + stop exit, the live sell order,
the startup funds warning, the profit lock end to end and strike picking by premium.
"""
import asyncio, logging, os, sys, tempfile, time
from dataclasses import replace, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = str(Path(__file__).resolve().parent.parent)
os.chdir(ROOT); sys.path.insert(0, ROOT)
os.environ.setdefault("KITE_API_KEY", "testkey"); os.environ.setdefault("KITE_ACCESS_TOKEN", "testtoken")
import algopilot.options.engine as E
from algopilot.core.candle_builder import Candle
from algopilot.options.broker import OrderResult
from algopilot.options.config import load_options_config

IST = ZoneInfo("Asia/Kolkata")
TMP = Path(tempfile.mkdtemp())
ok = []
logs = []


class Grab(logging.Handler):
    def emit(self, r): logs.append((r.levelname, r.getMessage()))


E.logger.addHandler(Grab()); E.logger.setLevel(logging.INFO)


def check(name, cond): ok.append((name, bool(cond)))


class Open:
    is_trading_allowed = True; eod_squareoff_due = False
E.get_market_status = lambda *a, **k: Open()


class Margin:
    auth_failed = False
    async def margin_per_lot_async(self, *a, **k): return 150000.0, True
    def margin_cache_age(self, *a): return 0.0
    async def get_intraday_candles_async(self, *a, **k): return []
    def close(self): pass


class FakeBroker:
    def __init__(self, funds=400000.0):
        self.orders, self.log, self.n, self.net, self.funds = {}, [], 0, {}, funds
    async def place_async(self, side, sec, qty, otype, price=0.0, trigger=0.0, tag=None):
        self.log.append(("place", side, otype, qty, price, trigger)); self.n += 1; oid = f"O{self.n}"
        st = "PENDING" if otype == "STOP_LOSS" else "TRADED"
        self.orders[oid] = dict(side=side, sec=sec, qty=qty, status=st, filled=0 if st == "PENDING" else qty, avg=0.0,
                                trigger=trigger, price=price, otype=otype, tag=tag or "")
        return oid, st
    async def modify_async(self, oid, otype, qty, price, trigger): return True, ""
    async def cancel_async(self, oid):
        self.log.append(("cancel", oid)); self.orders[oid]["status"] = "CANCELLED"; return True, ""
    async def status_async(self, oid):
        o = self.orders.get(oid); return OrderResult(oid, o["status"], o["filled"], o["avg"]) if o else OrderResult(oid, "ERROR")
    async def wait_for_fill(self, oid, timeout=6.0, poll=0.25, exact=False): return await self.status_async(oid)
    async def positions_async(self): return [{"securityId": s, "netQty": q, "exchangeSegment": "NSE_FNO"} for s, q in self.net.items()]
    async def orders_async(self): return []
    async def trades_async(self): return []
    async def funds_async(self): return self.funds
    stream = None; last_call_at = 0.0
    def close(self): pass


BASE = replace(load_options_config(), official_candles=False, entry_start="00:00", max_daily_loss=2000.0)


def engine(live=False, **kw):
    cfg = replace(BASE, db_path=str(TMP / f"{time.time_ns()}.db"), paper_trading=not live, **kw)
    e = E.OptionsEngine(cfg)
    e.client.close(); e.client = Margin()
    if live:
        e.broker = FakeBroker()
    for s, sid, k in (("CE", "CE1", 23400.0), ("PE", "PE1", 23300.0)):
        e.legs[s].security_id, e.legs[s].strike, e.legs[s].lot_size = sid, k, 65
    e._running = True
    return e


def cand(o, h, l, c, t0, i, side="CE"):
    s = (t0 + timedelta(minutes=5 * i)).astimezone(timezone.utc)
    return Candle(symbol=side, exchange_segment=2, security_id="x", start_ts=s, end_ts=s + timedelta(minutes=5),
                  open=o, high=h, low=l, close=c, volume=0)


SEQ = [(400, 401, 399, 400), (400, 415, 399, 414), (414, 416, 390, 392)]       # flat, GREEN, RED


async def main():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=now.minute % 5) - timedelta(minutes=10)

    # 1 config / engine have no AI at all
    names = {f.name for f in fields(BASE)}
    e = engine()
    check(f"1 no AI: config has no ai_* settings ({sorted(n for n in names if n.startswith('ai_'))}); engine has no agent",
          not any(n.startswith("ai_") for n in names) and not hasattr(e, "agent"))
    check(f"1 strategy settings (final-dhan, lock 50/2): entry {BASE.entry_mode}, offsets {BASE.entry_offset_pts}/{BASE.stop_offset_pts}, "
          f"lock {BASE.profit_lock_start_pts:g}/{BASE.profit_lock_step_pts:g}, {BASE.max_trades_per_day} trades/day, square-off {BASE.squareoff_at}",
          BASE.entry_mode == "break" and BASE.entry_offset_pts == 0.5 and BASE.stop_offset_pts == 0.5
          and BASE.profit_lock_start_pts == 50 and BASE.profit_lock_step_pts == 2 and BASE.max_trades_per_day == 6
          and BASE.squareoff_at == "15:10")

    # 2 paper: GREEN -> RED arms the sell at HA Low - 0.5; a tick below fills it
    for i, b in enumerate(SEQ):
        await e._on_candle_close("CE", e.legs["CE"], cand(*b, t0, i, "CE"), False)
    await asyncio.sleep(0.05)
    a = e._armed
    check(f"2 paper: GREEN->RED arms a SELL at candle-2 HA Low - 0.5 ({a and a.trigger})", a is not None and a.side == "CE")
    trig = a.trigger if a else 0
    e._on_tick_price("CE", e.legs["CE"], trig - 0.4, False, t0 + timedelta(minutes=15, seconds=20))
    await asyncio.sleep(0.05)
    pos = e.legs["CE"].position
    check(f"2 paper: price under the trigger -> sold ({pos and pos.entry_price}), stop {pos and pos.cover_level}",
          pos is not None and abs(pos.cover_level - 416.5) < 1e-6)
    e._on_tick_price("CE", e.legs["CE"], 417.0, False, t0 + timedelta(minutes=16))
    await asyncio.sleep(0.05)
    e.ledger.flush(); tr = e.ledger.get_all_trades(limit=10)
    check(f"2 paper: price above the stop -> bought back ({tr and tr[0]['exit_reason']})", e.legs["CE"].position is None and tr)

    # 3 live path with a fake broker: the SELL stop-limit goes to the exchange
    e = engine(live=True)
    for i, b in enumerate(SEQ):
        await e._on_candle_close("CE", e.legs["CE"], cand(*b, t0, i, "CE"), False)
    await asyncio.sleep(0.1)
    sells = [x for x in e.broker.log if x[:3] == ("place", "SELL", "STOP_LOSS")]
    check(f"3 live (fake broker): SELL stop-limit placed {sells[:1]}", sells and abs(sells[0][5] - trig) < 1e-6)

    # 4 startup funds warning
    for funds, want in ((0.0, True), (300000.0, False)):
        e = engine(live=True); e.broker.funds = funds; logs.clear()
        await e._reconcile_live()
        crit = any(lv == "CRITICAL" and "FUNDS TOO LOW" in m for lv, m in logs)
        check(f"4 startup funds Rs {funds:,.0f} -> CRITICAL warning {'shown' if want else 'not shown'}", crit == want)

    # 5 profit lock 5/2 end to end (paper): sell, fall 5+ pts, bounce -> bought back at the lock
    e = engine()
    for i, b in enumerate(SEQ):
        await e._on_candle_close("CE", e.legs["CE"], cand(*b, t0, i, "CE"), False)
    await asyncio.sleep(0.05)
    a = e._armed
    e._on_tick_price("CE", e.legs["CE"], a.trigger, False, t0 + timedelta(minutes=15, seconds=10)); await asyncio.sleep(0.05)
    sold = e.legs["CE"].position.entry_price
    ts = t0 + timedelta(minutes=16)
    for px in (sold - 30, sold - 50, sold - 52.2, sold - 51.0, sold - 50.0):     # best = 52.2 pts -> lock = 50 pts
        e._on_tick_price("CE", e.legs["CE"], round(px, 2), False, ts); ts += timedelta(seconds=10)
        await asyncio.sleep(0.02)
    e.ledger.flush(); tr = e.ledger.get_all_trades(limit=10)
    t = tr[0] if tr else {}
    check(f"5 lock 50/2: sold {sold}, low {sold - 52.2:.2f}, bounce -> {t.get('exit_reason')} at {t.get('exit_price')} "
          f"(= sold - 50, {sold - t.get('exit_price', 0):.2f} pts kept)",
          t.get("exit_reason") == "PROFIT_LOCK" and abs(sold - t.get("exit_price", 0) - 50.0) < 0.06)

    # 6 strikes by premium 300-400 (buffer 0)
    import types
    Q = lambda x: types.SimpleNamespace(last_price=x)
    def chain(ce, pe):
        ks = sorted(set(ce) | set(pe))
        return types.SimpleNamespace(legs={k: {"ce": Q(ce.get(k, 0)), "pe": Q(pe.get(k, 0))} for k in ks}, spot=22700)
    e = engine(); e.put_strike = e.call_strike = None
    ce = {22300: 418, 22400: 334, 22500: 256, 22600: 187}; pe = {23100: 425, 23000: 341, 22900: 262, 22800: 192}
    e._maybe_pick_strikes(chain(ce, pe)); first = (e.call_strike, e.put_strike)
    e._maybe_pick_strikes(chain({22300: 395, 22400: 302, 22500: 230}, {23100: 470, 23000: 398, 22900: 300}))
    kept = (e.call_strike, e.put_strike)
    e._maybe_pick_strikes(chain({22300: 382, 22400: 299, 22500: 228}, {23100: 492, 23000: 401, 22900: 318}))
    moved = (e.call_strike, e.put_strike)
    check(f"6 premium above 300: start CE/PE {first} | 302 / 398 kept {kept} | CE 299 -> {moved[0]} (Rs 382, cheapest >= 300) | PE 401 kept {moved[1]} (no upper limit)",
          first == (22400.0, 23000.0) and kept == first and moved == (22300.0, 23000.0))

asyncio.run(main())
for n, p in ok:
    print(("PASS  " if p else "FAIL  ") + n)
print("\nALL GREEN" if all(p for _, p in ok) else "\n*** FAILURES ***")
