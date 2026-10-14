"""
algopilot/agent/features.py — the Market Reader: raw data -> the numbers an
options professional looks at. Plain code (no AI), so every number the
strategist sees is computed the same way every time and can be checked.

build_snapshot() returns a JSON-able dict. Any part whose data is missing is
None / omitted rather than guessed; the strategist is told to treat missing data
as uncertainty.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .data import BANKNIFTY_ID, NIFTY_ID, VIX_ID, MarketData

_IST = ZoneInfo("Asia/Kolkata")


def _r(x, n=2):
    if x is None:
        return None
    try:
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        return round(float(x), n)
    except (TypeError, ValueError):
        return None


def nifty_section(n5: Optional[pd.DataFrame], n1: Optional[pd.DataFrame], hist: Optional[pd.DataFrame],
                  spot: Optional[float]) -> dict:
    if n5 is None or n5.empty:
        return {"available": False}
    today = n5[n5.day == n5.day.max()]
    spot = spot or float(today.close.iloc[-1])
    out = {"available": True, "spot": _r(spot), "day_open": _r(today.open.iloc[0]), "day_high": _r(today.high.max()),
           "day_low": _r(today.low.min())}
    if hist is not None and not hist.empty:
        prev = hist[hist.day < today.day.iloc[0]]
        if not prev.empty:
            pc = float(prev.close.iloc[-1])
            out["prev_close"] = _r(pc)
            out["change_pct"] = _r((spot / pc - 1) * 100)
            daily = prev.groupby("day").agg(h=("high", "max"), lo=("low", "min")).tail(20)
            out["avg_daily_range_20d"] = _r((daily.h - daily.lo).mean(), 0)
    out["day_range"] = _r(today.high.max() - today.low.min(), 0)
    out["position_in_day_range_pct"] = _r((spot - today.low.min()) / max(1e-9, today.high.max() - today.low.min()) * 100, 0)
    first30 = today[today.hm < "09:45"]
    if not first30.empty:
        out["opening_30min_range"] = [_r(first30.low.min(), 0), _r(first30.high.max(), 0)]
    last12 = today.tail(12)
    out["last_60min"] = {"high": _r(last12.high.max(), 0), "low": _r(last12.low.min(), 0),
                         "range_pts": _r(last12.high.max() - last12.low.min(), 0)}
    if len(last12) >= 4:
        slope = float(np.polyfit(np.arange(len(last12)), last12.close.values, 1)[0])
        out["last_60min"]["slope_pts_per_5min"] = _r(slope)
    out["last_12_closes_5min"] = [round(float(c)) for c in last12.close]
    out["code_regime_hint"] = regime_hint(today, spot)
    if n1 is not None and not n1.empty:
        t1 = n1[n1.day == n1.day.max()]
        last15 = t1.tail(15)
        if len(last15) >= 10:
            a, b = last15.head(7), last15.tail(7)
            out["last_15min_1m"] = {"high": _r(last15.high.max(), 0), "low": _r(last15.low.min(), 0),
                                    "first_half_high_low": [_r(a.high.max(), 0), _r(a.low.min(), 0)],
                                    "second_half_high_low": [_r(b.high.max(), 0), _r(b.low.min(), 0)]}
    return out


def regime_hint(today: pd.DataFrame, spot: float) -> str:
    """A simple, explainable label from the last hour; the strategist makes the real call."""
    last12 = today.tail(12)
    if len(last12) < 6:
        return "too_early"
    rng = float(last12.high.max() - last12.low.min())
    slope = float(np.polyfit(np.arange(len(last12)), last12.close.values, 1)[0])
    travel = abs(slope) * (len(last12) - 1)
    if rng < spot * 0.0025:
        return "sideways_tight"
    if travel > 0.6 * rng and rng > spot * 0.002:
        return "trend_up" if slope > 0 else "trend_down"
    return "mixed_choppy"


def futures_section(fut: Optional[pd.DataFrame], spot: Optional[float]) -> dict:
    if fut is None or fut.empty or "volume" not in fut:
        return {"available": False}
    f = fut[fut.day == fut.day.max()].reset_index(drop=True)
    tp = (f.high + f.low + f.close) / 3
    vwap = (tp * f.volume).cumsum() / f.volume.cumsum().replace(0, np.nan)
    above = f.close > vwap
    since10 = above[f.hm >= "10:00"]
    out = {"available": True, "price": _r(f.close.iloc[-1]), "vwap": _r(vwap.iloc[-1]),
           "vs_vwap_pts": _r(f.close.iloc[-1] - vwap.iloc[-1]),
           "bars_above_vwap_last12": int(above.tail(12).sum()), "vwap_crosses_since_10": int(max(0, (since10 != since10.shift()).sum() - 1))}
    if spot:
        out["basis_pts"] = _r(f.close.iloc[-1] - spot)
    if "oi" in f and len(f) > 1:
        def build(dp, doi):
            if dp >= 0 and doi >= 0:
                return "long_buildup"
            if dp >= 0 and doi < 0:
                return "short_covering"
            if dp < 0 and doi >= 0:
                return "short_buildup"
            return "long_unwinding"
        dp_day, doi_day = float(f.close.iloc[-1] - f.open.iloc[0]), float(f.oi.iloc[-1] - f.oi.iloc[0])
        out["day"] = {"price_chg": _r(dp_day, 1), "oi_chg_lakh": _r(doi_day / 1e5, 2), "reading": build(dp_day, doi_day)}
        if len(f) > 6:
            dp30, doi30 = float(f.close.iloc[-1] - f.close.iloc[-7]), float(f.oi.iloc[-1] - f.oi.iloc[-7])
            out["last_30min"] = {"price_chg": _r(dp30, 1), "oi_chg_lakh": _r(doi30 / 1e5, 2), "reading": build(dp30, doi30)}
    return out


def index_section(df: Optional[pd.DataFrame]) -> dict:
    if df is None or df.empty:
        return {"available": False}
    t = df[df.day == df.day.max()]
    out = {"available": True, "now": _r(t.close.iloc[-1]), "open": _r(t.open.iloc[0]),
           "day_chg_pct": _r((t.close.iloc[-1] / t.open.iloc[0] - 1) * 100)}
    if len(t) > 6:
        out["last_30min_chg_pct"] = _r((t.close.iloc[-1] / t.close.iloc[-7] - 1) * 100)
    return out


def chain_section(payload: Optional[dict], data: MarketData, side: str, strike: Optional[float],
                  spot: Optional[float]) -> dict:
    if not payload or not payload.get("oc"):
        return {"available": False}
    rows = []
    for k, r in payload["oc"].items():
        try:
            kk = float(k)
        except ValueError:
            continue
        for s in ("ce", "pe"):
            o = (r or {}).get(s) or {}
            g = o.get("greeks") or {}
            rows.append(dict(k=kk, s=s.upper(), ltp=float(o.get("last_price") or 0), oi=float(o.get("oi") or 0),
                             poi=float(o.get("previous_oi") or 0), iv=o.get("implied_volatility"),
                             delta=g.get("delta"), gamma=g.get("gamma"), theta=g.get("theta"),
                             bid=o.get("top_bid_price"), ask=o.get("top_ask_price")))
    if not rows:
        return {"available": False}
    C = pd.DataFrame(rows)
    ce, pe = C[C.s == "CE"].set_index("k"), C[C.s == "PE"].set_index("k")
    spot = spot or float(payload.get("last_price") or 0)
    out = {"available": True, "pcr_all": _r(pe.oi.sum() / max(1.0, ce.oi.sum())),
           "new_oi_today_lakh": {"puts": _r((pe.oi - pe.poi).sum() / 1e5, 0), "calls": _r((ce.oi - ce.poi).sum() / 1e5, 0)},
           "call_walls_top3": [[int(k), _r(v / 1e5, 0)] for k, v in ce.oi.nlargest(3).items()],
           "put_walls_top3": [[int(k), _r(v / 1e5, 0)] for k, v in pe.oi.nlargest(3).items()]}
    grid = np.arange(math.floor(spot / 100) * 100 - 1000, math.floor(spot / 100) * 100 + 1100, 10)
    pain = [float((ce.oi * np.maximum(0, x - ce.index)).sum() + (pe.oi * np.maximum(0, pe.index - x)).sum()) for x in grid]
    out["max_pain"] = int(grid[int(np.argmin(pain))])
    atm = min(ce.index, key=lambda k: abs(k - spot))
    out["atm_strike"] = int(atm)
    out["atm_straddle"] = _r(ce.ltp.get(atm, 0) + pe.ltp.get(atm, 0))
    out["atm_iv"] = {"ce": _r(ce.iv.get(atm)), "pe": _r(pe.iv.get(atm))}
    flow = data.oi_change(30)
    near = [k for k in sorted(ce.index) if abs(k - spot) <= 250 and k % 50 == 0]
    out["near_strikes"] = [{"strike": int(k), "ce_ltp": _r(ce.ltp.get(k)), "pe_ltp": _r(pe.ltp.get(k)),
                            "ce_oi_lakh": _r(ce.oi.get(k, 0) / 1e5, 0), "pe_oi_lakh": _r(pe.oi.get(k, 0) / 1e5, 0),
                            "ce_oi_chg_day_lakh": _r((ce.oi.get(k, 0) - ce.poi.get(k, 0)) / 1e5, 0),
                            "pe_oi_chg_day_lakh": _r((pe.oi.get(k, 0) - pe.poi.get(k, 0)) / 1e5, 0),
                            "ce_oi_chg_30min_lakh": _r(flow.get((k, "CE"), 0) / 1e5, 1) if flow else None,
                            "pe_oi_chg_30min_lakh": _r(flow.get((k, "PE"), 0) / 1e5, 1) if flow else None} for k in near]
    out["oi_flow_window_min"] = data.oi_window_minutes()
    if strike is not None:
        tab = ce if side == "CE" else pe
        if strike in tab.index:
            r = tab.loc[strike]
            out["our_option"] = {"side": side, "strike": int(strike), "ltp": _r(r.ltp), "bid": _r(r.bid), "ask": _r(r.ask),
                                 "iv": _r(r.iv), "delta": _r(r.delta, 3), "gamma": _r(r.gamma, 5), "theta_per_day": _r(r.theta),
                                 "oi_lakh": _r(r.oi / 1e5, 0), "oi_chg_day_lakh": _r((r.oi - r.poi) / 1e5, 0),
                                 "moneyness": ("ITM" if (side == "CE" and strike < spot) or (side == "PE" and strike > spot) else "OTM"),
                                 "distance_pts": _r(abs(strike - spot), 0)}
    return out


def base_rates(hist: Optional[pd.DataFrame], spot: float, now: datetime) -> dict:
    """How far NIFTY moved from this time of day over the last ~60 sessions."""
    if hist is None or hist.empty:
        return {"available": False}
    slot = (now.hour * 60 + now.minute) // 5 * 5 - 5
    shm = f"{slot // 60:02d}:{slot % 60:02d}"
    to_end, next30, n = [], [], 0
    for day, g in hist.groupby("day"):
        if day >= now.date():
            continue
        g = g.set_index("hm")
        if shm not in g.index or "15:05" not in g.index or shm >= "15:05":
            continue
        base = float(g.loc[shm, "close"])
        seq = g.loc[shm:"15:05", "close"].values[1:]
        if len(seq):
            n += 1
            to_end.append(spot * (seq[-1] / base - 1))
            nxt = seq[:6]
            next30.append(spot * (nxt[-1] / base - 1))
    if not n:
        return {"available": False}
    a, b = np.array(to_end), np.array(next30)
    return {"available": True, "days": n, "from_slot": shm,
            "to_1510": {"median_abs_pts": _r(np.median(np.abs(a)), 0), "p90_abs_pts": _r(np.percentile(np.abs(a), 90), 0),
                        "share_up_pct": _r(np.mean(a > 0) * 100, 0)},
            "next_30min": {"median_abs_pts": _r(np.median(np.abs(b)), 0), "p90_abs_pts": _r(np.percentile(np.abs(b), 90), 0),
                           "share_moves_over_30pts_pct": _r(np.mean(np.abs(b) > 30) * 100, 0)}}


def build_snapshot(data: MarketData, ctx: dict) -> dict:
    """Everything the strategist sees for one decision. ctx comes from the engine."""
    now = datetime.now(_IST)
    n5 = data.today(NIFTY_ID)
    n1 = data.today(NIFTY_ID, interval="1")
    vix = data.today(VIX_ID)
    bnf = data.today(BANKNIFTY_ID)
    fut = None
    fid = data.future()
    if fid:
        fut = data.today(fid[1], "NSE_FNO", "FUTIDX", oi=True)
    hist = data.history()
    payload = data.chain()
    spot = float(payload["last_price"]) if payload and payload.get("last_price") else None
    nifty = nifty_section(n5, n1, hist, spot)
    spot = spot or nifty.get("spot")
    vix_s = index_section(vix)
    return {
        "time_ist": now.strftime("%Y-%m-%d %H:%M"),
        "pattern": ctx.get("pattern", {}),
        "engine": ctx.get("engine", {}),
        "nifty": nifty,
        "nifty_future": futures_section(fut, spot),
        "india_vix": vix_s,
        "bank_nifty": index_section(bnf),
        "option_chain_today_expiry": chain_section(payload, data, ctx.get("pattern", {}).get("side", ""),
                                                   ctx.get("pattern", {}).get("strike"), spot),
        "history_base_rates": base_rates(hist, spot, now) if spot else {"available": False},
    }


def spot_now(data: MarketData) -> Optional[float]:
    payload = data.chain(max_age_secs=30)
    if payload and payload.get("last_price"):
        return float(payload["last_price"])
    n1 = data.today(NIFTY_ID, interval="1")
    return float(n1.close.iloc[-1]) if n1 is not None and not n1.empty else None
