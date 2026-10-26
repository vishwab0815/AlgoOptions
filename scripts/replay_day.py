"""
scripts/replay_day.py — replay a real trading day through the real engine (PAPER).

    python scripts/replay_day.py 2026-10-23
    python scripts/replay_day.py 2026-10-23 OPTIONS_PREMIUM_MIN=100 OPTIONS_PREMIUM_MAX=150 OPTIONS_PROFIT_LOCK_START_POINTS=18

The day's real 1-minute candles come from Zerodha Kite Connect's historical API
(NIFTY 50, and every option strike of that day's weekly expiry around the day's
range, from today's instrument list). No orders, no quote polling. The
OptionsEngine then runs that day minute by minute on a virtual clock — strike
picking, Heikin-Ashi candles, the pattern, the sell order at candle-2 HA Low -
0.5, the stop, the profit lock, the 15:10 square-off — in PAPER mode with a
temporary ledger (never data/options_ledger.db).

Settings come from .env; KEY=VALUE arguments override them for this run only
(e.g. to compare against version-5's 100-150 / 18-2).

Output: a table of the trades, and the engine's full log of the day in
data/replay/replay_<day>.log. Downloaded data is cached in data/replay/.

Limits: fills are simulated on 1-minute bars (the path open -> low -> high ->
close, or open -> high -> low -> close); the real engine sees every tick.
Margin is assumed available. Kite serves candles only for contracts that are
still listed: days of an EXPIRED weekly series can't be replayed (the script
says so instead of quietly using the wrong week's contracts).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
import time as _time
import types
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from algopilot.utils.network import ensure_tls_trust_store, force_ipv4  # noqa: E402

force_ipv4()
ensure_tls_trust_store()

import pandas as pd  # noqa: E402
from dotenv import dotenv_values  # noqa: E402

import algopilot.options.engine as E  # noqa: E402
import algopilot.options.ledger as LG  # noqa: E402
import algopilot.utils.market_hours as MH  # noqa: E402
from algopilot.core.candle_builder import Candle  # noqa: E402
from algopilot.options.config import load_options_config  # noqa: E402
from algopilot.options.instruments import InstrumentRegistry  # noqa: E402
from algopilot.options.kite_api import KiteHTTP  # noqa: E402
from algopilot.options.kite_client import ChainSnapshot, LegQuote  # noqa: E402
from algopilot.utils.secrets import SecretStr  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
OUT = Path("data/replay")
NIFTY_50_TOKEN = 256265
_STRIKE_SPAN = 1000          # strikes fetched: the day's NIFTY range +/- this many points


# ── fetching one day (Kite historical candles) ──────────────────────────────
class Fetcher:
    def __init__(self) -> None:
        env = dotenv_values(".env")
        self.http = KiteHTTP(str(env.get("KITE_API_KEY") or ""), SecretStr(str(env.get("KITE_ACCESS_TOKEN") or "")),
                             timeout=30.0)
        self.registry = InstrumentRegistry(self.http)
        self.calls = 0

    def candles(self, token: int, interval: str, frm: datetime, to: datetime, oi: bool = False) -> dict:
        """Kite candles as {timestamp, open, high, low, close, volume[, open_interest]} lists."""
        self.calls += 1
        reply = self.http.call("GET", f"/instruments/historical/{int(token)}/{interval}", family="historical",
                               params={"from": frm.strftime("%Y-%m-%d %H:%M:%S"), "to": to.strftime("%Y-%m-%d %H:%M:%S"),
                                       "continuous": 0, "oi": 1 if oi else 0})
        if not reply.ok:
            if self.http.auth_failed or reply.status in (400, 403):
                raise RuntimeError(f"Kite refused the request ({reply.reason()}) — log in: python scripts/kite_login.py")
            raise RuntimeError(f"Kite candles for {token}: {reply.reason()}")
        out = {k: [] for k in ("timestamp", "open", "high", "low", "close", "volume")}
        if oi:
            out["open_interest"] = []
        for c in (reply.data or {}).get("candles") or []:
            out["timestamp"].append(int(datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").timestamp()))
            for i, k in enumerate(("open", "high", "low", "close", "volume"), start=1):
                out[k].append(float(c[i]))
            if oi:
                out["open_interest"].append(float(c[6]) if len(c) > 6 else 0.0)
        return out

    def day(self, d: date) -> dict:
        D = datetime(d.year, d.month, d.day, tzinfo=IST)
        day0, day1 = D.replace(hour=9), D.replace(hour=15, minute=30)
        ds = d.isoformat()
        raw = {"day": ds, "nifty_1m": self.candles(NIFTY_50_TOKEN, "minute", day0, day1)}
        if not raw["nifty_1m"]["timestamp"]:
            raise RuntimeError(f"{ds}: no NIFTY 50 candles (holiday, weekend, or not yet traded)")
        self.registry.ensure()
        expiries = [e for e in self.registry.expiries() if e >= ds]
        if not expiries:
            raise RuntimeError("no listed NIFTY expiry on or after that day")
        exp = expiries[0]
        if (date.fromisoformat(exp) - d).days > 7:
            raise RuntimeError(f"{ds}'s weekly series has expired and Kite no longer serves it (the nearest listed "
                               f"expiry is {exp}). Only days of a still-listed expiry can be replayed.")
        raw["expiry"] = exp
        lo, hi = min(raw["nifty_1m"]["low"]), max(raw["nifty_1m"]["high"])
        k_lo, k_hi = int(lo // 100 * 100) - _STRIKE_SPAN, int(hi // 100 * 100) + _STRIKE_SPAN
        raw["options"] = {}
        for ins in sorted(self.registry.chain(exp), key=lambda i: (i.strike, i.instrument_type)):
            if not (k_lo <= ins.strike <= k_hi) or ins.strike % 100:
                continue
            m1 = self.candles(ins.token, "minute", day0, day1, oi=True)
            if not m1["timestamp"]:
                continue
            raw["options"][f"{int(ins.strike)}{ins.instrument_type}"] = {
                "m1": m1, "m5_hist": self.candles(ins.token, "5minute", D - timedelta(days=6), day0)}
        if len(raw["options"]) < 10:
            raise RuntimeError(f"{ds}: only {len(raw['options'])} option series came back for expiry {exp}")
        return raw


# ── the virtual clock ───────────────────────────────────────────────────────
VT = [0.0]


class VDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        d = datetime.fromtimestamp(VT[0], tz=timezone.utc)
        return d.astimezone(tz) if tz else d.replace(tzinfo=None)


class VTime:
    time = staticmethod(lambda: VT[0])
    monotonic = staticmethod(lambda: VT[0])
    perf_counter = staticmethod(_time.perf_counter)
    sleep = staticmethod(_time.sleep)


def _virtual_clock() -> None:
    for mod in (E, LG):
        mod.datetime = VDatetime
    E.time = VTime
    MH._now_ist = lambda: datetime.fromtimestamp(VT[0], tz=IST)


# ── the day's data ──────────────────────────────────────────────────────────
def frame(d, oi=False):
    if not d or not d.get("timestamp"):
        return None
    f = pd.DataFrame({k: d[k] for k in ("timestamp", "open", "high", "low", "close")})
    f["volume"] = d.get("volume") or [0.0] * len(f)
    if oi and d.get("open_interest"):
        f["oi"] = d["open_interest"]
    f["t"] = pd.to_datetime(f.timestamp, unit="s", utc=True).dt.tz_convert(IST)
    f["day"] = f.t.dt.date
    f["hm"] = f.t.dt.strftime("%H:%M")
    return f[(f.hm >= "09:15") & (f.hm < "15:30")].reset_index(drop=True)


def to5(f1):
    g = f1.assign(b=f1.timestamp // 300 * 300).groupby("b")
    out = pd.DataFrame({"timestamp": g.timestamp.first() // 300 * 300, "open": g.open.first(), "high": g.high.max(),
                        "low": g.low.min(), "close": g.close.last(), "volume": g.volume.sum()}).reset_index(drop=True)
    out["t"] = pd.to_datetime(out.timestamp, unit="s", utc=True).dt.tz_convert(IST)
    out["day"] = out.t.dt.date
    return out


class Day:
    def __init__(self, raw: dict) -> None:
        self.raw = raw
        self.n1 = frame(raw["nifty_1m"])
        self.today = self.n1.day.iloc[0]
        self.opt = {k: {"m1": frame(o["m1"], oi=True), "hist5": frame(o.get("m5_hist")),
                        "five": None} for k, o in raw["options"].items()}
        for o in self.opt.values():
            o["five"] = to5(o["m1"])


class SimClient:
    """The engine's Kite client during a replay: candles from the day's data, margin always available."""
    auth_failed = False

    def __init__(self, day: Day) -> None:
        self.day = day

    async def margin_per_lot_async(self, *a, **k):
        return 150000.0, True

    def margin_cache_age(self, *a):
        return 0.0

    async def resolve_contract_async(self, expiry, strike, side):
        return types.SimpleNamespace(security_id=f"{int(strike)}{side}", lot_size=65)

    async def get_intraday_candles_async(self, sec, seg, inst, interval, frm, to):
        o = self.day.opt.get(sec)
        if o is None:
            return []
        parts = [o["hist5"][o["hist5"].day < self.day.today]] if o["hist5"] is not None else []
        five = o["five"]
        parts.append(five[five.timestamp + 300 <= VT[0]])
        rows = pd.concat(parts)
        return [dict(timestamp=int(r.timestamp), open=float(r.open), high=float(r.high), low=float(r.low),
                     close=float(r.close), volume=float(r.volume)) for r in rows.itertuples()]

    async def get_chain_async(self, *a):
        return None

    def close(self):
        pass


def chain_at(day: Day, minute_ts: float, spot: float) -> ChainSnapshot:
    """The option chain at a minute: each strike's last 1-minute close before it."""
    legs = {}
    for key, o in day.opt.items():
        k, side = float(key[:-2]), key[-2:]
        m = o["m1"]
        past = m[m.timestamp < minute_ts]
        if past.empty:
            continue
        last = past.iloc[-1]
        legs.setdefault(k, {})[side.lower()] = LegQuote(last_price=float(last.close),
                                                        oi=float(last.oi) if "oi" in past else 0.0)
    return ChainSnapshot(spot=spot, legs=legs)


def walk_minute(e, side, leg, bar, ts):
    """A 1-minute bar as 4 ticks; stop / lock / sell trigger levels crossed between ticks are hit exactly."""
    o, h, l, c = bar
    path = [o, l, h, c] if c >= o else [o, h, l, c]
    prev = None
    for i, p in enumerate(path):
        t = ts + timedelta(seconds=15 * i)
        pos, a = leg.position, e._armed
        waiting = a is not None and a.side == side and a.leg is leg and a.state == "ARMED"
        if prev is not None and p > prev and pos is not None:
            for level in sorted(x for x in (pos.cover_level, pos.lock_price) if x > 0 and prev < x < p):
                if leg.position is not None:
                    e._on_tick_price(side, leg, level, False, t)
        elif prev is not None and p < prev and waiting and p < a.trigger < prev:
            e._on_tick_price(side, leg, a.trigger, False, t)
        e._on_tick_price(side, leg, p, False, t)
        prev = p


async def settle(e):
    for _ in range(200):
        pend = [t for t in list(e._background_tasks) if not t.done()]
        if not pend:
            return
        await asyncio.wait(pend, timeout=300)


async def replay(raw: dict, log_path: Path) -> tuple:
    day = Day(raw)
    VT[0] = float(day.n1.timestamp.iloc[0])
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")

    class VFmt(logging.Formatter):
        def format(self, r):
            return f"{datetime.fromtimestamp(VT[0], tz=IST):%Y-%m-%d %H:%M:%S} [{r.levelname}] {r.name.split('.')[-1]:<10}: {r.getMessage()}"
    handler.setFormatter(VFmt())
    root = logging.getLogger("algopilot")
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    root.propagate = False

    tmp = Path(tempfile.mkdtemp())
    cfg = replace(load_options_config(), paper_trading=True, db_path=str(tmp / "replay.db"), official_candles=False,
                  auto_stop_at="", paper_capital=250000.0)
    e = E.OptionsEngine(cfg)
    e.client.close()
    e.client = SimClient(day)
    e.expiry = raw["expiry"]
    e._running = True
    end_ts = float(day.n1.timestamp.iloc[-1])
    for r in day.n1.itertuples():
        m = float(r.timestamp)
        VT[0] = m
        before = (e.put_strike, e.call_strike)
        e._maybe_pick_strikes(chain_at(day, m, float(r.open)))
        if (e.put_strike, e.call_strike) != before:
            await e._resolve_contracts_and_subscribe()
        for side in ("CE", "PE"):
            leg = e.legs[side]
            if not leg.security_id or leg.security_id not in day.opt:
                continue
            bars = day.opt[leg.security_id]["m1"]
            b = bars[bars.timestamp == r.timestamp]
            if b.empty:
                continue
            b = b.iloc[0]
            VT[0] = m + 1
            mt = datetime.fromtimestamp(m, tz=timezone.utc)
            if r.hm >= e.config.squareoff_at and leg.position is not None:
                e._check_exits(side, leg, float(b.open), True, at_time=mt)
            walk_minute(e, side, leg, (float(b.open), float(b.high), float(b.low), float(b.close)), mt)
            e._last_premium[side] = float(b.close)
        await settle(e)
        if (m + 60) % 300 == 0:                                   # a 5-minute candle closes
            B = m + 60
            VT[0] = B
            Bdt = datetime.fromtimestamp(B, tz=timezone.utc)
            await e._expire_armed(Bdt)
            for side in ("CE", "PE"):
                leg = e.legs[side]
                if not leg.security_id or leg.security_id not in day.opt:
                    continue
                five = day.opt[leg.security_id]["five"]
                c5 = five[five.timestamp == B - 300]
                if c5.empty:
                    continue
                c5 = c5.iloc[0]
                cdl = Candle(symbol=side, exchange_segment=2, security_id=leg.security_id,
                             start_ts=Bdt - timedelta(seconds=300), end_ts=Bdt, open=float(c5.open), high=float(c5.high),
                             low=float(c5.low), close=float(c5.close), volume=0.0)
                await e._on_candle_close(side, leg, cdl, MH.get_market_status(now=Bdt).eod_squareoff_due, arm_now=False)
            await settle(e)
            await e._arm_best(Bdt)
            await settle(e)
        if m >= end_ts:
            break
    e.ledger.flush()
    trades = sorted(e.ledger.get_all_trades(limit=500), key=lambda t: t["entry_time"])
    e.ledger.close()
    handler.close()
    return trades, cfg


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 1
    d = date.fromisoformat(args[0])
    for kv in args[1:]:
        k, _, v = kv.partition("=")
        os.environ[k.strip()] = v.strip()                         # wins over .env for this run
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / f"raw_{d}.json"
    if cache.exists():
        raw = json.loads(cache.read_text(encoding="utf-8"))
        print(f"{d}: using cached data ({len(raw['options'])} option series)")
    else:
        f = Fetcher()
        raw = f.day(d)
        cache.write_text(json.dumps(raw), encoding="utf-8")
        print(f"{d}: fetched {len(raw['options'])} option series (expiry {raw['expiry']}), {f.calls} data requests")
    _virtual_clock()
    log_path = OUT / f"replay_{d}.log"
    trades, cfg = asyncio.run(replay(raw, log_path))

    hi = cfg.premium_max
    rule = f"above Rs {cfg.premium_min:g}" if hi <= 0 else f"Rs {cfg.premium_min:g}-{hi:g}"
    print(f"\nsettings: strikes by premium {rule} | sell HA Low - {cfg.entry_offset_pts:g} | stop HA High + "
          f"{cfg.stop_offset_pts:g} | lock {cfg.profit_lock_start_pts:g}/{cfg.profit_lock_step_pts:g} | "
          f"{cfg.max_trades_per_day} trades/day | daily loss limit Rs {cfg.max_daily_loss:,.0f} | square-off {cfg.squareoff_at}")
    print(f"\n{'sold':<6}{'side':<5}{'strike':<8}{'at':>9}{'bought back':>13}{'at':>7}{'points':>8}  {'exit':<15}{'net Rs':>9}")
    pts = net = 0.0
    for t in trades:
        ent = datetime.fromisoformat(t["entry_time"]).astimezone(IST)
        ext = datetime.fromisoformat(t["exit_time"]).astimezone(IST) if t.get("exit_time") else None
        p = t["entry_price"] - t["exit_price"]
        pts += p
        net += t["pnl"]
        print(f"{ent:%H:%M} {t['leg']:<5}{int(t['strike']):<8}{t['entry_price']:>9.2f}{t['exit_price']:>13.2f}"
              f"{(ext.strftime('%H:%M') if ext else ''):>7}{p:>+8.2f}  {t['exit_reason']:<15}{t['pnl']:>+9,.0f}")
    won = sum(1 for t in trades if t["pnl"] > 0)
    print(f"\n{len(trades)} trades, {won} won | points {pts:+.2f} | net Rs {net:+,.0f} (after charges, 1 lot)")
    print(f"engine log of the day: {log_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
