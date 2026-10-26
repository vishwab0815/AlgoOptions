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

from .data import BANKNIFTY_ID, HEAVYWEIGHTS, NIFTY_ID, VIX_ID, MarketData

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
            last = prev[prev.day == prev.day.max()]
            ph, pl = float(last.high.max()), float(last.low.min())
            out["prev_day"] = {"high": _r(ph, 0), "low": _r(pl, 0), "close": _r(pc, 0)}
            out["vs_prev_day"] = "above_prev_high" if spot > ph else "below_prev_low" if spot < pl else "inside_prev_range"
    out["day_range"] = _r(today.high.max() - today.low.min(), 0)
    out["position_in_day_range_pct"] = _r((spot - today.low.min()) / max(1e-9, today.high.max() - today.low.min()) * 100, 0)
    first30 = today[today.hm < "09:45"]
    if not first30.empty:
        out["opening_30min_range"] = [_r(first30.low.min(), 0), _r(first30.high.max(), 0)]
        if len(first30) >= 6:
            out["vs_opening_range"] = ("above_high" if spot > first30.high.max() else
                                       "below_low" if spot < first30.low.min() else "inside")
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
    above = ce[(ce.index >= spot) & (ce.index <= spot + 600)]
    below = pe[(pe.index <= spot) & (pe.index >= spot - 600)]
    if not above.empty:
        k = above.oi.idxmax()
        out["call_wall_above"] = {"strike": int(k), "oi_lakh": _r(above.oi.max() / 1e5, 0), "distance_pts": _r(k - spot, 0)}
    if not below.empty:
        k = below.oi.idxmax()
        out["put_wall_below"] = {"strike": int(k), "oi_lakh": _r(below.oi.max() / 1e5, 0), "distance_pts": _r(spot - k, 0)}
    flow = data.oi_change(30)
    near = [k for k in sorted(ce.index) if abs(k - spot) <= 250 and k % 50 == 0]
    out["near_strikes"] = [{"strike": int(k), "ce_ltp": _r(ce.ltp.get(k)), "pe_ltp": _r(pe.ltp.get(k)),
                            "ce_oi_lakh": _r(ce.oi.get(k, 0) / 1e5, 0), "pe_oi_lakh": _r(pe.oi.get(k, 0) / 1e5, 0),
                            "ce_oi_chg_day_lakh": _r((ce.oi.get(k, 0) - ce.poi.get(k, 0)) / 1e5, 0),
                            "pe_oi_chg_day_lakh": _r((pe.oi.get(k, 0) - pe.poi.get(k, 0)) / 1e5, 0),
                            "ce_oi_chg_30min_lakh": _r(flow.get((k, "CE"), 0) / 1e5, 1) if flow else None,
                            "pe_oi_chg_30min_lakh": _r(flow.get((k, "PE"), 0) / 1e5, 1) if flow else None} for k in near]
    out["oi_flow_window_min"] = data.oi_window_minutes()
    if flow:
        ce_add = sum(v for (k, s), v in flow.items() if s == "CE" and abs(k - spot) <= 300)
        pe_add = sum(v for (k, s), v in flow.items() if s == "PE" and abs(k - spot) <= 300)
        out["fresh_writing_near_atm_30min_lakh"] = {"calls": _r(ce_add / 1e5, 1), "puts": _r(pe_add / 1e5, 1)}
    pt = data.pcr_trend(30)
    if pt:
        out["pcr_30min_then_now"] = list(pt)
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


def push_section(side: str, n1: Optional[pd.DataFrame], nifty: dict, fut: dict, chain: dict) -> dict:
    """The NIFTY move that pumped this option (up for a CE, down for a PE), how much of it is
    already given back, and whether it runs with or against the day's main trend."""
    if n1 is None or n1.empty or not nifty.get("available") or side not in ("CE", "PE"):
        return {"available": False}
    t1 = n1[n1.day == n1.day.max()]
    spot = float(nifty["spot"])
    last30, last15 = t1.tail(30), t1.tail(15)
    if len(last30) < 10:
        return {"available": False}
    push_dir = "up" if side == "CE" else "down"
    hi, lo = float(last30.high.max()), float(last30.low.min())
    if side == "CE":                          # the up-move that pumped the call
        start = float(last30.low.iloc[: int(last30.high.values.argmax()) + 1].min())
        extreme, size, given_back = hi, hi - start, hi - spot
    else:                                     # the down-move that pumped the put
        start = float(last30.high.iloc[: int(last30.low.values.argmin()) + 1].max())
        extreme, size, given_back = lo, start - lo, spot - lo
    chg = nifty.get("change_pct")
    fday = (fut.get("day") or {}) if fut.get("available") else {}
    day_dir = "flat"
    if (chg is not None and chg <= -0.15) or fday.get("reading") in ("short_buildup", "long_unwinding") and (fday.get("price_chg") or 0) < -30:
        day_dir = "down"
    elif (chg is not None and chg >= 0.15) or fday.get("reading") in ("long_buildup", "short_covering") and (fday.get("price_chg") or 0) > 30:
        day_dir = "up"
    out = {"available": True, "push_that_pumped_this_option": push_dir,
           "push_size_pts_30min": _r(size, 0), "push_extreme": _r(extreme, 0), "given_back_pts": _r(given_back, 0),
           "given_back_pct_of_push": _r(given_back / size * 100 if size > 0 else 0, 0),
           "nifty_move_last_15min": _r(spot - float(last15.open.iloc[0]), 0),
           "nifty_move_last_30min": _r(spot - float(last30.open.iloc[0]), 0),
           "day_trend": day_dir,
           "push_vs_day_trend": ("against" if day_dir not in ("flat", push_dir) else "with" if day_dir == push_dir else "flat_day"),
           "distance_to_day_high_pts": _r(nifty["day_high"] - spot, 0), "distance_to_day_low_pts": _r(spot - nifty["day_low"], 0)}
    straddle = chain.get("atm_straddle") if chain.get("available") else None
    if fut.get("available") and straddle:
        out["vwap_stretch_vs_expected_move"] = _r(fut["vs_vwap_pts"] / straddle, 2)
    return out


def _reading(dp: float, doi: float) -> str:
    """Price change vs open-interest change, the standard F&O reading."""
    if dp >= 0 and doi >= 0:
        return "long_buildup"
    if dp >= 0:
        return "short_covering"
    if doi >= 0:
        return "short_buildup"
    return "long_unwinding"


def _today(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    return df[df.day == df.day.max()].reset_index(drop=True)


def option_flow(opt1: Optional[pd.DataFrame], side: str) -> dict:
    """Our option's own 1-minute premium, volume and open interest: who is acting in THIS contract."""
    o = _today(opt1)
    if o is None or len(o) < 5:
        return {"available": False}
    now = float(o.close.iloc[-1])
    out = {"available": True, "premium_now": _r(now), "day_open": _r(o.open.iloc[0]), "day_high": _r(o.high.max()),
           "day_low": _r(o.low.min())}
    if "volume" in o and o.volume.sum() > 0:
        tp = (o.high + o.low + o.close) / 3
        vwap = float((tp * o.volume).sum() / o.volume.sum())
        out["premium_vwap"] = _r(vwap)
        out["premium_vs_vwap_pct"] = _r((now / vwap - 1) * 100, 1)
        per5 = o.volume.groupby(o.index // 5).sum()
        if len(per5) >= 3:
            out["volume_last5min_vs_day_avg"] = _r(float(o.volume.tail(5).sum()) / max(1.0, float(per5.iloc[:-1].mean())), 2)
    for m in (5, 15, 30):
        if len(o) > m:
            dp = now - float(o.close.iloc[-m - 1])
            row = {"premium_chg": _r(dp)}
            if "oi" in o:
                doi = float(o.oi.iloc[-1] - o.oi.iloc[-m - 1])
                row["oi_chg_pct"] = _r(doi / max(1.0, float(o.oi.iloc[-m - 1])) * 100, 1)
                row["reading"] = _reading(dp, doi)
            out[f"last_{m}min"] = row
    if "oi" in o:
        out["oi_chg_day_pct"] = _r((o.oi.iloc[-1] / max(1.0, float(o.oi.iloc[0])) - 1) * 100, 1)
    out["how_to_read"] = ("premium down + OI up = writers adding (helps our sell); premium up + OI up = buyers adding; "
                          "premium up + OI down = writers covering (hurts our sell); premium down + OI down = buyers exiting")
    return out


def futures_minute(fut1: Optional[pd.DataFrame]) -> dict:
    f = _today(fut1)
    if f is None or len(f) < 6:
        return {"available": False}
    out = {"available": True}
    for m in (3, 5, 15):
        if len(f) > m:
            dp = float(f.close.iloc[-1] - f.close.iloc[-m - 1])
            row = {"price_chg": _r(dp, 1)}
            if "oi" in f:
                doi = float(f.oi.iloc[-1] - f.oi.iloc[-m - 1])
                row.update(oi_chg_lakh=_r(doi / 1e5, 2), reading=_reading(dp, doi))
            out[f"last_{m}min"] = row
    if "volume" in f and f.volume.sum() > 0 and len(f) > 15:
        out["volume_last5min_vs_avg5min"] = _r(float(f.volume.tail(5).sum()) / max(1.0, float(f.volume.iloc[:-5].mean()) * 5), 2)
    return out


def vix_minute(vix1: Optional[pd.DataFrame]) -> dict:
    v = _today(vix1)
    if v is None or len(v) < 5:
        return {"available": False}
    now = float(v.close.iloc[-1])
    out = {"available": True, "now": _r(now), "day_open": _r(v.open.iloc[0]),
           "day_chg_pct": _r((now / float(v.open.iloc[0]) - 1) * 100)}
    for m in (5, 15, 30):
        if len(v) > m:
            out[f"chg_pct_last_{m}min"] = _r((now / float(v.close.iloc[-m - 1]) - 1) * 100)
    return out


def minute_tape(n1, fut1, vix1, opt1, rows: int = 20) -> dict:
    """The last `rows` minutes side by side: what a trader watches on the screen."""
    n = _today(n1)
    if n is None or n.empty:
        return {"available": False}
    tape = n[["timestamp", "hm", "close"]].rename(columns={"close": "nifty"}).tail(rows)
    for name, df, cols in (("fut", fut1, ("close", "oi")), ("vix", vix1, ("close",)),
                           ("opt", opt1, ("close", "volume", "oi"))):
        d = _today(df)
        if d is None:
            continue
        keep = d[["timestamp"] + [c for c in cols if c in d]].copy()
        if "oi" in keep:
            keep["oi"] = keep["oi"].diff()
        keep.columns = ["timestamp"] + [f"{name}_{'oi_chg' if c == 'oi' else c}" for c in keep.columns[1:]]
        tape = tape.merge(keep, on="timestamp", how="left")
    cols = [c for c in tape.columns if c != "timestamp"]
    out_rows = []
    for _, r in tape.iterrows():
        row = []
        for c in cols:
            x = r[c]
            if c == "hm":
                row.append(x)
            elif pd.isna(x):
                row.append(None)
            elif c in ("fut_oi_chg", "opt_oi_chg", "opt_volume"):
                row.append(int(x))
            else:
                row.append(round(float(x), 2))
        out_rows.append(row)
    return {"available": True, "columns": cols, "rows": out_rows,
            "note": "1-minute closes, oldest first; *_oi_chg = open interest added in that minute"}


def depth_section(quote: Optional[dict], opt_sid, fut_sid) -> dict:
    """Live order book (5 levels) and day totals for our option and the NIFTY future."""
    quote = (quote or {}).get("NSE_FNO") if quote and "NSE_FNO" in quote else quote
    if not quote:
        return {"available": False}
    out = {"available": True}
    for name, sid in (("our_option", opt_sid), ("nifty_future", fut_sid)):
        q = quote.get(str(sid)) if sid else None
        if not q:
            continue
        dep = q.get("depth") or {}
        bids, asks = dep.get("buy") or [], dep.get("sell") or []
        bq = sum(float(x.get("quantity") or 0) for x in bids)
        aq = sum(float(x.get("quantity") or 0) for x in asks)
        row = {"ltp": _r(q.get("last_price")), "day_avg_price": _r(q.get("average_price"))}
        if bids and asks:
            row.update(best_bid=_r(bids[0].get("price")), best_ask=_r(asks[0].get("price")),
                       spread=_r(float(asks[0].get("price") or 0) - float(bids[0].get("price") or 0)),
                       top5_bid_qty=int(bq), top5_ask_qty=int(aq), top5_bid_to_ask=_r(bq / max(1.0, aq)))
        tb, ts = float(q.get("buy_quantity") or 0), float(q.get("sell_quantity") or 0)
        if tb and ts:
            row["total_buy_to_sell_qty"] = _r(tb / ts)
        if q.get("oi"):
            row.update(oi_lakh=_r(float(q["oi"]) / 1e5, 1), oi_day_high_lakh=_r(float(q.get("oi_day_high") or 0) / 1e5, 1),
                       oi_day_low_lakh=_r(float(q.get("oi_day_low") or 0) / 1e5, 1))
        out[name] = row
    out["how_to_read"] = ("bid_to_ask < 1 = more offers than bids (sellers in control); for our option that helps the sell. "
                          "LTP below its day_avg_price = the premium is weak today")
    return out if len(out) > 2 else {"available": False}


def heavyweights_section(quote: Optional[dict]) -> dict:
    """NIFTY's biggest stocks right now: day change and position vs their own day VWAP."""
    eq = (quote or {}).get("NSE_EQ") or {}
    rows, up, down, above = [], 0, 0, 0
    for sid, name in HEAVYWEIGHTS.items():
        q = eq.get(sid) or eq.get(int(sid)) if isinstance(eq, dict) else None
        if not q or not q.get("last_price"):
            continue
        ltp = float(q["last_price"])
        prev = float((q.get("ohlc") or {}).get("close") or 0) or (ltp - float(q.get("net_change") or 0))
        pct = (ltp / prev - 1) * 100 if prev else 0.0
        vw = float(q.get("average_price") or 0)
        up += pct > 0.1
        down += pct < -0.1
        above += bool(vw) and ltp > vw
        rows.append({"stock": name, "day_chg_pct": _r(pct), "vs_day_vwap": ("above" if vw and ltp > vw else "below" if vw else None)})
    if not rows:
        return {"available": False}
    return {"available": True, "stocks": rows, "up": int(up), "down": int(down), "above_vwap": int(above), "of": len(rows)}


def direction_section(nifty: dict, fut: dict, fut1: dict, chain: dict, vix1: dict, bnf: dict, heavy: dict) -> dict:
    """Up or down? Each signal votes up / down / neutral, with its number; the score is ups minus downs.
    A professional's checklist, computed the same way every time."""
    votes = []

    def vote(name, side, why):
        votes.append({"signal": name, "vote": side, "why": why})

    if fut.get("available") and fut.get("vs_vwap_pts") is not None:
        d = float(fut["vs_vwap_pts"])
        vote("price_vs_vwap", "up" if d > 10 else "down" if d < -10 else "neutral", f"futures {d:+.0f} pts vs VWAP")
    if nifty.get("vs_opening_range"):
        o = nifty["vs_opening_range"]
        vote("opening_range", "up" if o == "above_high" else "down" if o == "below_low" else "neutral", o)
    if nifty.get("vs_prev_day"):
        p = nifty["vs_prev_day"]
        vote("previous_day", "up" if p == "above_prev_high" else "down" if p == "below_prev_low" else "neutral", p)
    r30 = (fut.get("last_30min") or {}).get("reading") if fut.get("available") else None
    if r30:
        vote("futures_oi_30min", "up" if r30 == "long_buildup" else "down" if r30 == "short_buildup" else "neutral",
             r30 + (" (weak move, often fades)" if r30 in ("short_covering", "long_unwinding") else ""))
    fw = chain.get("fresh_writing_near_atm_30min_lakh") if chain.get("available") else None
    if fw and fw.get("calls") is not None and fw.get("puts") is not None:
        c, p = float(fw["calls"]), float(fw["puts"])
        side = "up" if p > max(0.0, c) * 1.2 and p > 1 else "down" if c > max(0.0, p) * 1.2 and c > 1 else "neutral"
        vote("fresh_option_writing", side, f"puts +{p:.1f} lakh vs calls +{c:.1f} lakh near ATM in 30 min")
    pcr = chain.get("pcr_all") if chain.get("available") else None
    if pcr:
        vote("pcr", "up" if pcr > 1.2 else "down" if pcr < 0.8 else "neutral", f"PCR {pcr}")
    v15 = vix1.get("chg_pct_last_15min") if vix1.get("available") else None
    if v15 is not None:
        vote("india_vix_15min", "down" if v15 > 2 else "neutral", f"VIX {v15:+.1f}% in 15 min"
             + (" (fear rising)" if v15 > 2 else " (calm: good for option selling)" if v15 < 0 else ""))
    if bnf.get("available") and nifty.get("change_pct") is not None and bnf.get("day_chg_pct") is not None:
        n, b = float(nifty["change_pct"]), float(bnf["day_chg_pct"])
        same = (n > 0.1 and b > 0.1) or (n < -0.1 and b < -0.1)
        vote("bank_nifty", ("up" if n > 0 else "down") if same else "neutral",
             f"BANK NIFTY {b:+.2f}% vs NIFTY {n:+.2f}%" + ("" if same else " (not confirming)"))
    if heavy.get("available") and heavy.get("of"):
        u, d, n = heavy["up"], heavy["down"], heavy["of"]
        vote("heavyweights", "up" if u >= 0.6 * n else "down" if d >= 0.6 * n else "neutral", f"{u} up / {d} down of {n}")
    l5 = fut1.get("last_5min") if fut1.get("available") else None
    vr = fut1.get("volume_last5min_vs_avg5min") if fut1.get("available") else None
    if l5 and vr is not None:
        pc = float(l5.get("price_chg") or 0)
        side = ("up" if pc > 0 else "down") if vr >= 1.5 and abs(pc) >= 5 else "neutral"
        vote("volume_5min", side, f"futures volume {vr}x normal, price {pc:+.0f} pts in 5 min")
    if not votes:
        return {"available": False}
    ups = sum(v["vote"] == "up" for v in votes)
    downs = sum(v["vote"] == "down" for v in votes)
    score = ups - downs
    return {"available": True, "score": score, "ups": ups, "downs": downs, "signals": len(votes),
            "reading": "trend_up" if score >= 3 else "trend_down" if score <= -3 else "range_or_mixed",
            "votes": votes}


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
    pattern = ctx.get("pattern", {})
    side, strike, opt_sid = pattern.get("side", ""), pattern.get("strike"), pattern.get("security_id")
    fut1 = data.today(fid[1], "NSE_FNO", "FUTIDX", interval="1", oi=True) if fid else None
    vix1 = data.today(VIX_ID, interval="1")
    opt1 = data.today(str(opt_sid), "NSE_FNO", "OPTIDX", interval="1", oi=True) if opt_sid else None
    quote = data.quote([opt_sid, fid[1] if fid else None], HEAVYWEIGHTS.keys())
    fut_s = futures_section(fut, spot)
    chain_s = chain_section(payload, data, side, strike, spot)
    if chain_s.get("available"):
        trend = data.chain_trend(strike, side, 30)
        if trend:
            chain_s["last_30min_trend"] = trend
    fut1_s, vix1_s, bnf_s = futures_minute(fut1), vix_minute(vix1), index_section(bnf)
    heavy = heavyweights_section(quote)
    return {
        "time_ist": now.strftime("%Y-%m-%d %H:%M"),
        "market_direction": direction_section(nifty, fut_s, fut1_s, chain_s, vix1_s, bnf_s, heavy),
        "pattern": pattern,
        "engine": ctx.get("engine", {}),
        "today_so_far": ctx.get("today", {}),
        "the_push": push_section(side, n1, nifty, fut_s, chain_s),
        "our_option_flow": option_flow(opt1, side),
        "minute_tape_last_20min": minute_tape(n1, fut1, vix1, opt1),
        "market_depth_now": depth_section(quote, opt_sid, fid[1] if fid else None),
        "nifty": nifty,
        "nifty_future": fut_s,
        "nifty_future_1min": fut1_s,
        "india_vix": index_section(vix),
        "india_vix_1min": vix1_s,
        "bank_nifty": bnf_s,
        "heavyweights": heavy,
        "option_chain_today_expiry": chain_s,
        "history_base_rates": base_rates(hist, spot, now) if spot else {"available": False},
    }


def spot_now(data: MarketData) -> Optional[float]:
    payload = data.chain(max_age_secs=30)
    if payload and payload.get("last_price"):
        return float(payload["last_price"])
    n1 = data.today(NIFTY_ID, interval="1")
    return float(n1.close.iloc[-1]) if n1 is not None and not n1.empty else None
