"""
scripts/agent_cases.py — grow the AI strategist's memory (data/agent_cases.jsonl).

    python scripts/agent_cases.py                     today (run after the close)
    python scripts/agent_cases.py 2026-10-14          one day
    python scripts/agent_cases.py 2026-09-07 2026-10-07   every trading day in a range

For each day: the day's real 1-minute data is fetched from Dhan (data API only —
no orders, no option-chain requests), the day is replayed through the real
OptionsEngine in PAPER mode with a temporary ledger (never data/options_ledger.db)
and a free stand-in for the AI that allows every sell, so the sells are exactly
the version-5 ones. Each sell is saved with the numbers the Market Reader saw at
its pre-close / 2/3 moment and its net result. The strategist compares every
new pattern with these cases (algopilot/agent/cases.py).

Contracts still listed in data/dhan_scrip_master.csv are fetched by security id;
older weeks come from Dhan's expired-options API (rolling ATM offsets, re-keyed
by the real strike). The daily pipeline runs this after every session when
OPTIONS_AI_AGENT=true.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import sys
import tempfile
import time as _time
import types
from collections import defaultdict
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
import requests  # noqa: E402
from dotenv import dotenv_values  # noqa: E402

import algopilot.agent.agent as AG  # noqa: E402
import algopilot.agent.data as AD  # noqa: E402
import algopilot.agent.features as AF  # noqa: E402
import algopilot.options.engine as E  # noqa: E402
import algopilot.options.ledger as LG  # noqa: E402
import algopilot.utils.market_hours as MH  # noqa: E402
from algopilot.agent.cases import CASES_PATH, case_features  # noqa: E402
from algopilot.agent.llm import Usage  # noqa: E402
from algopilot.core.candle_builder import Candle  # noqa: E402
from algopilot.options.config import load_options_config  # noqa: E402
from algopilot.options.dhan_client import ChainSnapshot, LegQuote  # noqa: E402
from algopilot.utils.market_calendar import trading_day_status  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger("agent_cases")
_CHARTS = "https://api.dhan.co/v2/charts/intraday"
_ROLLING = "https://api.dhan.co/v2/charts/rollingoption"
_SCRIP = Path("data/dhan_scrip_master.csv")


# ── fetching one day ────────────────────────────────────────────────────────
class Fetcher:
    def __init__(self) -> None:
        env = dotenv_values(".env")
        self.s = requests.Session()
        self.s.headers.update({"access-token": env.get("DHAN_ACCESS_TOKEN", ""), "client-id": env.get("DHAN_CLIENT_ID", ""),
                               "Content-Type": "application/json", "Accept": "application/json"})
        self.calls = 0
        self.rows = list(csv.DictReader(open(_SCRIP, encoding="utf-8"))) if _SCRIP.exists() else []

    def post(self, url: str, body: dict) -> dict:
        r = None
        for i in range(4):
            r = self.s.post(url, json=body, timeout=60)
            _time.sleep(0.4)
            self.calls += 1
            if r.status_code == 200:
                return r.json()
            _time.sleep(2 + 2 * i)
        raise RuntimeError(f"Dhan {r.status_code}: {r.text[:160]}")

    def bars(self, sid, seg, inst, interval, frm, to, oi=False) -> dict:
        return self.post(_CHARTS, {"securityId": str(sid), "exchangeSegment": seg, "instrument": inst, "interval": interval,
                                   "oi": oi, "fromDate": frm.strftime("%Y-%m-%d %H:%M:%S"),
                                   "toDate": to.strftime("%Y-%m-%d %H:%M:%S")})

    def _nifty(self, kind: str, ds: str):
        return sorted((r["SM_EXPIRY_DATE"], r["SECURITY_ID"], r) for r in self.rows
                      if r.get("UNDERLYING_SYMBOL") == "NIFTY" and r.get("INSTRUMENT") == kind
                      and r.get("SM_EXPIRY_DATE", "") >= ds)

    def day(self, d: date) -> dict:
        D = datetime(d.year, d.month, d.day, tzinfo=IST)
        day0, day1 = D.replace(hour=9), D.replace(hour=15, minute=30)
        ds = d.isoformat()
        raw = {"nifty_1m": self.bars(13, "IDX_I", "INDEX", "1", day0, day1)}
        if not raw["nifty_1m"].get("timestamp"):
            raise RuntimeError(f"{ds}: no NIFTY bars")
        raw["nifty_hist_5m"] = {}
        for a, b in ((D - timedelta(days=85), D - timedelta(days=43)), (D - timedelta(days=42), D.replace(hour=8))):
            h = self.bars(13, "IDX_I", "INDEX", "5", a.replace(hour=9), b)
            for k in ("timestamp", "open", "high", "low", "close"):
                raw["nifty_hist_5m"].setdefault(k, []).extend(h.get(k, []))
        raw["vix_5m"] = self.bars(21, "IDX_I", "INDEX", "5", day0, day1)
        raw["vix_1m"] = self.bars(21, "IDX_I", "INDEX", "1", day0, day1)
        raw["bnf_5m"] = self.bars(25, "IDX_I", "INDEX", "5", day0, day1)
        futs = self._nifty("FUTIDX", ds)
        if not futs:
            raise RuntimeError("no NIFTY future in the contract list")
        fexp, fsid, _ = futs[0]
        raw["future"] = {"expiry": fexp, "sid": fsid, "bars": self.bars(fsid, "NSE_FNO", "FUTIDX", "5", day0, day1, oi=True),
                         "m1": self.bars(fsid, "NSE_FNO", "FUTIDX", "1", day0, day1, oi=True)}
        lo, hi = min(raw["nifty_1m"]["low"]), max(raw["nifty_1m"]["high"])
        k_lo, k_hi = int(lo // 100 * 100) - 500, int(hi // 100 * 100) + 600
        opts = self._nifty("OPTIDX", ds)
        raw["options"] = {}
        if opts:
            exp = opts[0][0]
            raw["expiry"] = exp
            for e, sid, r in opts:
                k = float(r["STRIKE_PRICE"])
                if e != exp or not (k_lo <= k <= k_hi) or k % 100:
                    continue
                m1 = self.bars(sid, "NSE_FNO", "OPTIDX", "1", day0, day1, oi=True)
                if not m1.get("timestamp"):
                    continue
                raw["options"][f"{int(k)}{r['OPTION_TYPE']}"] = {
                    "sid": f"{int(k)}{r['OPTION_TYPE']}", "m1": m1,
                    "m5_hist": self.bars(sid, "NSE_FNO", "OPTIDX", "5", D - timedelta(days=6), day0)}
        if len(raw["options"]) < 10:                       # an expired week: the expired-options API
            raw["expiry"] = (d + timedelta(days=(1 - d.weekday()) % 7)).isoformat()
            raw["options"] = self.rolling(d)
        return raw

    def rolling(self, d: date) -> dict:
        series = defaultdict(dict)
        for side, flag in (("CE", "CALL"), ("PE", "PUT")):
            for off in range(-10, 11):
                x = (self.post(_ROLLING, {
                    "exchangeSegment": "NSE_FNO", "interval": "1", "securityId": 13, "instrument": "OPTIDX",
                    "expiryFlag": "WEEK", "expiryCode": 1, "strike": "ATM" if off == 0 else f"ATM{off:+d}",
                    "drvOptionType": flag, "requiredData": ["open", "high", "low", "close", "volume", "oi", "strike"],
                    "fromDate": d.isoformat(), "toDate": d.isoformat()}).get("data") or {}).get(side.lower()) or {}
                for i, ts in enumerate(x.get("timestamp") or []):
                    k = float(x["strike"][i])
                    if k % 100 == 0:
                        series[(k, side)][ts] = (x["open"][i], x["high"][i], x["low"][i], x["close"][i],
                                                 x["volume"][i], x["oi"][i])
        out = {}
        for (k, side), s in series.items():
            if len(s) < 200:
                continue
            ts = sorted(s)
            out[f"{int(k)}{side}"] = {"sid": f"{int(k)}{side}", "m5_hist": {},
                                      "m1": {"timestamp": ts, **{f: [s[t][j] for t in ts] for j, f in enumerate(
                                          ("open", "high", "low", "close", "volume", "open_interest"))}}}
        return out


# ── replaying one day (virtual clock) ───────────────────────────────────────
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
    for mod in (E, LG, AG, AD, AF):
        mod.datetime = VDatetime
    for mod in (E, AG, AD):
        mod.time = VTime
    MH._now_ist = lambda: datetime.fromtimestamp(VT[0], tz=IST)


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
    out["hm"] = out.t.dt.strftime("%H:%M")
    return out


def done(f, sec):
    return None if f is None else f[f.timestamp + sec <= VT[0]]


class Day:
    def __init__(self, raw: dict) -> None:
        self.raw = raw
        self.n1 = frame(raw["nifty_1m"])
        self.n5 = to5(self.n1)
        self.today = self.n1.day.iloc[0]
        h = frame(raw["nifty_hist_5m"])
        self.hist = h[h.day < self.today] if h is not None else None
        self.vix5, self.vix1, self.bnf5 = frame(raw.get("vix_5m")), frame(raw.get("vix_1m")), frame(raw.get("bnf_5m"))
        self.fut5, self.fut1 = frame(raw["future"].get("bars"), oi=True), frame(raw["future"].get("m1"), oi=True)
        self.opt = {k: {"m1": frame(o["m1"], oi=True), "hist5": frame(o.get("m5_hist"))} for k, o in raw["options"].items()}


class SimClient:
    auth_failed = False

    def __init__(self, day: Day) -> None:
        self.day = day

    async def margin_per_lot_async(self, *a, **k):
        return 165000.0, True

    def margin_cache_age(self, *a):
        return 0.0

    async def resolve_contract_async(self, expiry, strike, side):
        return types.SimpleNamespace(security_id=f"{int(strike)}{side}", lot_size=65)

    async def get_intraday_candles_async(self, sec, seg, inst, interval, frm, to):
        o = self.day.opt.get(sec)
        if o is None:
            return []
        parts = [o["hist5"][o["hist5"].day < self.day.today]] if o["hist5"] is not None else []
        parts.append(done(to5(o["m1"]), 300))
        rows = pd.concat(parts)
        return [dict(timestamp=int(r.timestamp), open=float(r.open), high=float(r.high), low=float(r.low),
                     close=float(r.close), volume=float(r.volume)) for r in rows.itertuples()]

    async def get_chain_async(self, *a):
        return None

    def close(self):
        pass


class ReplayData(AD.MarketData):
    def __init__(self, day: Day) -> None:
        super().__init__("x", "y")
        self.d = day

    def today(self, sid, segment="IDX_I", instrument="INDEX", interval="5", oi=False):
        d = self.d
        if sid == AD.NIFTY_ID:
            return done(d.n1, 60) if interval == "1" else done(d.n5, 300)
        if sid == AD.VIX_ID:
            return done(d.vix1, 60) if interval == "1" else done(d.vix5, 300)
        if sid == AD.BANKNIFTY_ID:
            return done(d.bnf5, 300)
        if sid == d.raw["future"]["sid"]:
            return done(d.fut1, 60) if interval == "1" else done(d.fut5, 300)
        if sid in d.opt and interval == "1":
            return done(d.opt[sid]["m1"], 60)
        return None

    def history(self):
        return self.d.hist

    def future(self):
        return self.d.raw["future"]["expiry"], self.d.raw["future"]["sid"]

    def quote(self, fno_ids, eq_ids=()):
        return None


class StubLLM:
    """Allows every sell, free: the replay trades exactly like version 5."""
    def __init__(self) -> None:
        self.spend = types.SimpleNamespace(usd=0.0, calls=0, by_model={})

    def budget_left(self):
        return 1e9

    async def decide(self, snapshot, stage, news, effort, timeout):
        return ({"decision": "SELL", "confidence": 100, "push_state": "unclear", "evidence_for_sell": [],
                 "evidence_against": [], "invalidation": ""}, Usage(model="stub"))

    async def close(self):
        pass


def chain_at(day: Day, minute_ts: float, spot: float):
    payload, legs = {"last_price": spot, "oc": {}}, {}
    for key, o in day.opt.items():
        k, side = float(key[:-2]), key[-2:]
        m = o["m1"]
        past = m[m.timestamp < minute_ts]
        if past.empty:
            continue
        last = past.iloc[-1]
        oi = float(last.oi) if "oi" in past else 0.0
        poi = float(m.oi.iloc[0]) if "oi" in m else 0.0
        payload["oc"].setdefault(f"{k:.6f}", {})[side.lower()] = {"last_price": float(last.close), "oi": oi, "previous_oi": poi}
        legs.setdefault(k, {})[side.lower()] = LegQuote(last_price=float(last.close), oi=oi)
    return payload, ChainSnapshot(spot=spot, legs=legs)


def walk_minute(e, side, leg, bar, ts):
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
        if e.agent is not None:
            pend += [t for t in list(e.agent._background) if not t.done()]
        if not pend:
            return
        await asyncio.wait(pend, timeout=300)


def preclose(e, day: Day, m: float):
    B = m + 60
    for side in ("CE", "PE"):
        leg = e.legs[side]
        if not leg.security_id:
            continue
        bars = day.opt[leg.security_id]["m1"]
        part = bars[(bars.timestamp >= B - 300) & (bars.timestamp < m)]
        cb = leg.candle_builder
        if part.empty:
            cb._current, cb._current_start = None, None
            continue
        cb._current_start = datetime.fromtimestamp(B - 300, tz=timezone.utc)
        cb._current = {"open": float(part.open.iloc[0]), "high": float(part.high.max()), "low": float(part.low.min()),
                       "close": float(part.close.iloc[-1]), "volume": 0.0}
    e._ai_preclose(datetime.fromtimestamp(B, tz=timezone.utc))


async def replay_cases(raw: dict, ds: str) -> list:
    day = Day(raw)
    snaps = []
    real_build = AF.build_snapshot

    def rec(data, ctx):
        s = real_build(data, ctx)
        snaps.append(s)
        return s
    AG.build_snapshot = rec
    os.environ.setdefault("ANTHROPIC_API_KEY", "not-used-by-the-replay")
    VT[0] = float(day.n1.timestamp.iloc[0])
    tmp = Path(tempfile.mkdtemp())
    cfg = replace(load_options_config(), paper_trading=True, db_path=str(tmp / "replay.db"), official_candles=False,
                  auto_stop_at="", paper_capital=250000.0, ai_agent=True, ai_news_every_mins=0, ai_preclose_secs=60,
                  ai_decide_timeout_secs=150.0, ai_prepare_timeout_secs=200.0)
    e = E.OptionsEngine(cfg)
    e.client.close()
    e.client = SimClient(day)
    e.expiry = raw["expiry"]
    e._running = True
    await e.agent.llm.close()
    e.agent.llm = StubLLM()
    e.agent.data.close()
    e.agent.data = ReplayData(day)
    e.agent.cases.cases = []
    end_ts = float(day.n1.timestamp.iloc[-1])
    for r in day.n1.itertuples():
        m = float(r.timestamp)
        VT[0] = m
        payload, chain = chain_at(day, m, float(r.open))
        e.agent.on_chain(payload)
        if (m + 60) % 300 == 0:
            preclose(e, day, m)
            await settle(e)
        before = (e.put_strike, e.call_strike)
        e._maybe_pick_strikes(chain)
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
        if (m + 60) % 300 == 0:
            B = m + 60
            VT[0] = B
            Bdt = datetime.fromtimestamp(B, tz=timezone.utc)
            await e._expire_armed(Bdt)
            for side in ("CE", "PE"):
                leg = e.legs[side]
                if not leg.security_id or leg.security_id not in day.opt:
                    continue
                five = to5(day.opt[leg.security_id]["m1"])
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
    await e.agent.llm.close()
    e.ledger.close()
    AG.build_snapshot = real_build
    cases = []
    for t in trades:
        ent = datetime.fromisoformat(t["entry_time"])
        c2 = datetime.fromtimestamp(ent.timestamp() // 300 * 300, tz=timezone.utc)
        pick = None
        for want in ("2/3-pre", "2/3"):
            for s in snaps:
                pat = s.get("pattern", {})
                ce = pat.get("candle2_end")
                if pat.get("side") == t["leg"] and pat.get("stage") == want and ce and datetime.fromisoformat(ce) == c2:
                    pick = s
            if pick is not None:
                break
        if pick is None:          # the decision re-used the 1/3 analysis: its snapshot (candle 1's close)
            c1_close = f"{(c2 - timedelta(minutes=5)).astimezone(IST):%Y-%m-%d %H:%M}"
            for s in snaps:
                pat = s.get("pattern", {})
                if pat.get("side") == t["leg"] and pat.get("stage") == "1/3" and s.get("time_ist") == c1_close:
                    pick = s
        if pick is None:
            continue
        cases.append({"day": ds, "time": f"{ent.astimezone(IST):%H:%M}", "side": t["leg"], "strike": t["strike"],
                      "sell": t["entry_price"], "net_rs": round(t["pnl"], 2), "exit_reason": t["exit_reason"],
                      "stage": pick["pattern"].get("stage"), "features": case_features(pick)})
    return cases


def save(cases: list, ds: str, path: Path = Path(CASES_PATH)) -> None:
    """Replace that day's cases in the memory file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    keep = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                if json.loads(line).get("day") != ds:
                    keep.append(line)
            except ValueError:
                continue
    keep += [json.dumps(c, separators=(",", ":")) for c in cases]
    path.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")


def days_between(a: date, b: date) -> list:
    out, d = [], a
    while d <= b:
        if trading_day_status(d)[0]:
            out.append(d)
        d += timedelta(days=1)
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    for name in ("algopilot", "httpx", "anthropic", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)
    args = sys.argv[1:]
    today = datetime.now(IST).date()
    if not args:
        if datetime.now(IST).strftime("%H:%M") < "15:31":
            print("Run this after the close (15:31), or give a past date.")
            return 1
        days = [today]
    elif len(args) == 1:
        days = [date.fromisoformat(args[0])]
    else:
        days = days_between(date.fromisoformat(args[0]), date.fromisoformat(args[1]))
    _virtual_clock()
    f = Fetcher()
    total = 0
    for d in days:
        try:
            raw = f.day(d)
            cases = asyncio.run(replay_cases(raw, d.isoformat()))
        except Exception as exc:
            print(f"{d}: skipped — {exc}")
            continue
        save(cases, d.isoformat())
        total += len(cases)
        won = sum(1 for c in cases if c["net_rs"] > 0)
        print(f"{d}: {len(cases)} sells saved ({won} won, net Rs {sum(c['net_rs'] for c in cases):,.0f})", flush=True)
    print(f"done: {total} sells added to {CASES_PATH} ({f.calls} data requests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
