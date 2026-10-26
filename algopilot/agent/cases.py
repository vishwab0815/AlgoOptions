"""
algopilot/agent/cases.py — the strategist's memory: past sell patterns and how they ended.

Every case is one real sell of this system (from a replay of a past day through
the engine, AI off) with the numbers the Market Reader saw at the moment the
order was due, and its net result in rupees. For a new pattern, the most
similar past cases (same side, similar push / trend / room / option flow /
stop risk) are summarised for the strategist as evidence: "of 10 similar
sells, 4 made money, average -Rs 210".

The cases live in algopilot/agent/cases_seed.jsonl (shipped: a month of real
days, Sep-Oct 2026) plus data/agent_cases.jsonl (grown on the VM after every
session by scripts/agent_cases.py). One JSON object per line. Plain code, no AI.
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

CASES_PATH = "data/agent_cases.jsonl"
SEED_PATH = str(Path(__file__).with_name("cases_seed.jsonl"))
_K = 10
_MIN_SAME_SIDE = 8

# feature -> (description shown to the strategist, cap applied before scaling)
FEATURES = {
    "day_trend_with_push_pct": ("NIFTY change today in the push's direction (+ = the day trend is the push)", 3.0),
    "push_size_pts": ("size of the NIFTY push in the last 30 min", 250.0),
    "given_back_pct": ("share of the push already given back", 200.0),
    "room_to_day_extreme_pts": ("points from NIFTY to the day high (CE) / day low (PE)", 300.0),
    "minutes_since_open": ("minutes since 09:15", 375.0),
    "vix_day_chg_pct": ("India VIX change today, %", 20.0),
    "fut_30min_with_push_pts": ("futures move in the last 30 min, in the push's direction", 200.0),
    "opt_premium_15min_pct": ("our premium change in the last 15 min, %", 100.0),
    "opt_oi_30min_pct": ("our option's OI change in the last 30 min, %", 100.0),
    "risk_to_stop_pct": ("stop distance as % of the sell price", 50.0),
}


def _num(x) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) or math.isinf(v) else v


def case_features(snap: dict) -> Dict[str, float]:
    """The comparable numbers of one decision, from a Market Reader snapshot."""
    pat = snap.get("pattern") or {}
    side = pat.get("side")
    sign = 1.0 if side == "CE" else -1.0
    nifty, push = snap.get("nifty") or {}, snap.get("the_push") or {}
    flow, fut = snap.get("our_option_flow") or {}, snap.get("nifty_future") or {}
    vix1, vix5 = snap.get("india_vix_1min") or {}, snap.get("india_vix") or {}
    out: Dict[str, Optional[float]] = {}
    chg = _num(nifty.get("change_pct"))
    out["day_trend_with_push_pct"] = None if chg is None else chg * sign
    out["push_size_pts"] = _num(push.get("push_size_pts_30min"))
    out["given_back_pct"] = _num(push.get("given_back_pct_of_push"))
    out["room_to_day_extreme_pts"] = _num(push.get("distance_to_day_high_pts" if side == "CE" else "distance_to_day_low_pts"))
    t = str(snap.get("time_ist", ""))[-5:]
    if len(t) == 5 and t[2] == ":":
        out["minutes_since_open"] = int(t[:2]) * 60 + int(t[3:]) - 555
    out["vix_day_chg_pct"] = _num(vix1.get("day_chg_pct", vix5.get("day_chg_pct")))
    f30 = _num((fut.get("last_30min") or {}).get("price_chg"))
    out["fut_30min_with_push_pts"] = None if f30 is None else f30 * sign
    p15, pnow = _num((flow.get("last_15min") or {}).get("premium_chg")), _num(flow.get("premium_now"))
    out["opt_premium_15min_pct"] = None if p15 is None or not pnow else p15 / pnow * 100
    out["opt_oi_30min_pct"] = _num((flow.get("last_30min") or {}).get("oi_chg_pct"))
    risk, sell = _num(pat.get("risk_to_stop_pts")), _num(pat.get("sell_price"))
    out["risk_to_stop_pct"] = None if risk is None or not sell else risk / sell * 100
    clean = {}
    for k, v in out.items():
        if v is not None:
            cap = FEATURES[k][1]
            clean[k] = round(max(-cap, min(cap, float(v))), 3)
    return clean


def _bucket_oi(f):
    x = f.get("opt_oi_30min_pct")
    return None if x is None else ("writers covering (our OI falling)" if x < -2 else "writers adding (our OI rising)" if x > 2 else "our OI flat")


def _bucket_stop(f):
    x = f.get("risk_to_stop_pct")
    return None if x is None else ("stop <8% of price" if x < 8 else "stop 8-14%" if x < 14 else "stop >14% of price")


def _bucket_time(f):
    x = f.get("minutes_since_open")
    return None if x is None else ("09:15-10:30" if x < 75 else "10:30-12:30" if x < 195 else "12:30-14:00" if x < 285 else "14:00-15:10")


def _bucket_back(f):
    x = f.get("given_back_pct")
    return None if x is None else ("push <40% given back" if x < 40 else "push 40-80% given back" if x < 80 else "push >80% given back")


def _bucket_vix(f):
    x = f.get("vix_day_chg_pct")
    return None if x is None else ("VIX up >2% today" if x > 2 else "VIX down >2%" if x < -2 else "VIX flat")


def _bucket_trend(f):
    x = f.get("day_trend_with_push_pct")
    return None if x is None else ("push WITH the day trend" if x >= 0.25 else "push AGAINST the day trend" if x <= -0.25 else "flat day")


BUCKETS = {"our_option_oi_30min": _bucket_oi, "stop_distance": _bucket_stop, "time_of_day": _bucket_time,
           "push_given_back": _bucket_back, "vix_today": _bucket_vix, "push_vs_day_trend": _bucket_trend}
_STATS_DAYS = 20
_WEAK_MIN_SELLS = 10
_WEAK_WIN_PCT = 25


class CaseLibrary:
    def __init__(self, path: str = CASES_PATH, seed: Optional[str] = SEED_PATH) -> None:
        self.path = Path(path)
        self.seed = Path(seed) if seed else None
        self.cases: List[dict] = []
        self.load()

    def load(self) -> None:
        """The seed first, then the VM's own file; the same day + time + side is kept once (the newer copy)."""
        found: Dict[tuple, dict] = {}
        for p in (self.seed, self.path):
            if p is None or not p.exists():
                continue
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    c = json.loads(line)
                except ValueError:
                    continue
                if c.get("side") in ("CE", "PE") and isinstance(c.get("features"), dict) and c.get("net_rs") is not None:
                    found[(c.get("day"), c.get("time"), c["side"])] = c
        self.cases = sorted(found.values(), key=lambda c: (c.get("day", ""), c.get("time", "")))

    def add(self, case: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(case, separators=(",", ":")) + "\n")
        self.cases.append(case)

    def days(self) -> List[str]:
        return sorted({c["day"] for c in self.cases})

    def setup_stats(self, snap: dict, before_day: Optional[str] = None, days: int = _STATS_DAYS) -> dict:
        """This system's own record: of the real sells in the last `days` trading days before
        `before_day`, how did the ones in the same bucket as this pattern end? One line per bucket
        (our option's OI flow, stop size, time of day, push given back, VIX, trend). A bucket with
        >= 10 sells and <= 25% won marks a WEAK setup (the agent raises its bar for it)."""
        pool = [c for c in self.cases if before_day is None or c["day"] < before_day]
        use_days = sorted({c["day"] for c in pool})[-days:]
        pool = [c for c in pool if c["day"] in use_days]
        if len(pool) < 5:
            return {"available": False, "note": "not enough past sells yet"}
        q = case_features(snap)
        nets = [c["net_rs"] for c in pool]
        out = {"available": True, "days": len(use_days), "from": use_days[0], "to": use_days[-1],
               "all_sells": {"sells": len(nets), "won_pct": round(100 * sum(n > 0 for n in nets) / len(nets)),
                             "avg_net_rs": round(sum(nets) / len(nets))},
               "this_pattern": [], "weak_setup": None, "how_to_read": (
                   "each line = real sells of THIS system in the same bucket as this pattern and how they ended; "
                   "a line with many sells and a low won_pct is strong evidence against the sell")}
        weakest = None
        for name, fn in BUCKETS.items():
            key = fn(q)
            if key is None:
                continue
            m = [c["net_rs"] for c in pool if fn(c["features"]) == key]
            if not m:
                out["this_pattern"].append({"bucket": name, "value": key, "sells": 0})
                continue
            won = sum(n > 0 for n in m)
            row = {"bucket": name, "value": key, "sells": len(m), "won": won, "won_pct": round(100 * won / len(m)),
                   "avg_net_rs": round(sum(m) / len(m))}
            out["this_pattern"].append(row)
            if len(m) >= _WEAK_MIN_SELLS and row["won_pct"] <= _WEAK_WIN_PCT and (weakest is None or row["won_pct"] < weakest["won_pct"]):
                weakest = row
        if weakest:
            out["weak_setup"] = f"{weakest['value']}: {weakest['won']}/{weakest['sells']} won, avg Rs {weakest['avg_net_rs']}"
        return out

    def similar(self, snap: dict, before_day: Optional[str] = None, k: int = _K) -> dict:
        """The k most similar past sells (only days before `before_day`) and how they ended."""
        side = (snap.get("pattern") or {}).get("side")
        pool = [c for c in self.cases if before_day is None or c["day"] < before_day]
        if not pool:
            return {"available": False, "note": "no past cases in memory yet"}
        q = case_features(snap)
        same = [c for c in pool if c["side"] == side]
        use = same if len(same) >= _MIN_SAME_SIDE else pool
        scale = {}
        for f in FEATURES:
            vals = [c["features"][f] for c in use if f in c["features"]]
            if len(vals) >= 3:
                m = sum(vals) / len(vals)
                sd = math.sqrt(sum((v - m) ** 2 for v in vals) / len(vals))
                scale[f] = sd if sd > 1e-9 else 1.0
        scored = []
        for c in use:
            dims = [((q[f] - c["features"][f]) / scale[f]) ** 2 for f in scale if f in q and f in c["features"]]
            if len(dims) >= 4:
                scored.append((math.sqrt(sum(dims) / len(dims)), c))
        if not scored:
            return {"available": False, "note": "not enough comparable numbers"}
        scored.sort(key=lambda x: x[0])
        top = [c for _, c in scored[:k]]
        nets = sorted(c["net_rs"] for c in top)
        won = sum(1 for n in nets if n > 0)

        def stats(cs):
            n = [c["net_rs"] for c in cs]
            return {"cases": len(n), "won": sum(1 for x in n if x > 0),
                    "win_rate_pct": round(100 * sum(1 for x in n if x > 0) / len(n)), "avg_net_rs": round(sum(n) / len(n))}

        return {
            "available": True,
            "memory": f"{len(pool)} past sells from {len({c['day'] for c in pool})} days",
            "all_past_sells_this_side": stats(same) if same else None,
            "similar_sells": {"cases": len(top), "won": won, "lost": len(top) - won,
                              "win_rate_pct": round(100 * won / len(top)), "avg_net_rs": round(sum(nets) / len(nets)),
                              "median_net_rs": round(nets[len(nets) // 2]),
                              "same_side_only": use is same},
            "closest_examples": [{"day": c["day"], "time": c["time"], "side": c["side"], "net_rs": round(c["net_rs"]),
                                  "exit": c.get("exit_reason", ""), "numbers": c["features"]} for c in top[:5]],
            "this_pattern_numbers": q,
            "feature_meaning": {f: d for f, (d, _) in FEATURES.items()},
        }
