"""
algopilot/agent/data.py — the market data the agent reads.

Candles (NIFTY 1-min / 5-min, India VIX, BANK NIFTY, NIFTY future and our own
option with volume and open interest) come from DhanHQ's data API, one request at
a time with a gap between them so the agent never crowds the engine's own requests.
Market depth (5 levels of bids / offers, total buy vs sell quantity) comes from
the market-quote API (1 request per second), only at decision time.

The OPTION CHAIN is never requested here: Dhan allows one chain request per 3 s
per account and the engine already polls it every ~5 s. The engine hands each
poll to on_chain(); a per-minute sample of every strike's open interest is kept,
so the agent can see who added positions in the last 30 minutes for free.
"""
from __future__ import annotations

import csv
import logging
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Deque, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
_CHARTS = "https://api.dhan.co/v2/charts/intraday"
_QUOTE = "https://api.dhan.co/v2/marketfeed/quote"
_QUOTE_GAP_SECS = 1.05     # Dhan: 1 market-quote request per second
_GAP_SECS = 0.3            # between the agent's own data-API calls (Dhan: 5 requests/s per account)
_TODAY_TTL_SECS = 20.0     # today's candles are re-used for this long (CE and PE at the same close)

NIFTY_ID, VIX_ID, BANKNIFTY_ID = "13", "21", "25"
# NIFTY heavyweights (NSE_EQ security ids): together about 45% of the index.
HEAVYWEIGHTS = {"2885": "RELIANCE", "1333": "HDFCBANK", "4963": "ICICIBANK", "1594": "INFY", "11536": "TCS",
                "10604": "BHARTIARTL", "11483": "LT", "1660": "ITC"}


class MarketData:
    def __init__(self, client_id: str, access_token: str, scrip_csv: str = "data/dhan_scrip_master.csv") -> None:
        self._session = requests.Session()
        self._session.headers.update({"access-token": access_token, "client-id": client_id,
                                      "Content-Type": "application/json", "Accept": "application/json"})
        self._scrip_csv = Path(scrip_csv)
        self._lock = threading.Lock()
        self._last_call = 0.0
        self._cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
        self._history: Optional[pd.DataFrame] = None
        self._history_day: Optional[date] = None
        self._future: Optional[Tuple[date, str, str]] = None      # (day, expiry, security id)
        self._chain: Optional[Tuple[float, dict]] = None            # (monotonic time, payload)
        self._oi_samples: Deque[Tuple[datetime, Dict[Tuple[float, str], float]]] = deque(maxlen=95)
        # Per-minute chain picture: (time, spot, {(strike, side): (ltp, iv)}) for the
        # ATM straddle / IV trend of the last 30 minutes.
        self._px_samples: Deque[Tuple[datetime, float, Dict[Tuple[float, str], Tuple[float, Optional[float]]]]] = deque(maxlen=95)
        self._last_quote = 0.0

    def close(self) -> None:
        self._session.close()

    # ── DhanHQ data API ─────────────────────────────────────────────────────
    def _candles(self, sid: str, segment: str, instrument: str, interval: str, frm: datetime, to: datetime,
                 oi: bool = False) -> Optional[pd.DataFrame]:
        body = {"securityId": str(sid), "exchangeSegment": segment, "instrument": instrument, "interval": interval,
                "oi": oi, "fromDate": frm.strftime("%Y-%m-%d %H:%M:%S"), "toDate": to.strftime("%Y-%m-%d %H:%M:%S")}
        with self._lock:
            wait = self._last_call + _GAP_SECS - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                resp = self._session.post(_CHARTS, json=body, timeout=15)
            except requests.RequestException as exc:
                logger.warning("agent data: %s %s request failed: %s", sid, interval, exc)
                return None
            finally:
                self._last_call = time.monotonic()
        if resp.status_code != 200:
            logger.warning("agent data: %s %s -> HTTP %d %s", sid, interval, resp.status_code, resp.text[:120])
            return None
        d = resp.json()
        if not d.get("close"):
            return None
        df = pd.DataFrame({k: d[k] for k in ("timestamp", "open", "high", "low", "close")})
        if d.get("volume"):
            df["volume"] = d["volume"]
        if d.get("open_interest"):
            df["oi"] = d["open_interest"]
        df["t"] = pd.to_datetime(df.timestamp, unit="s", utc=True).dt.tz_convert(_IST)
        df["day"] = df.t.dt.date
        df["hm"] = df.t.dt.strftime("%H:%M")
        return df[(df.hm >= "09:15") & (df.hm < "15:30")].reset_index(drop=True)

    def today(self, sid: str, segment: str = "IDX_I", instrument: str = "INDEX", interval: str = "5",
              oi: bool = False) -> Optional[pd.DataFrame]:
        key = f"{sid}|{interval}|{oi}"
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < _TODAY_TTL_SECS:
            return hit[1]
        now = datetime.now(_IST)
        df = self._candles(sid, segment, instrument, interval, now.replace(hour=9, minute=0, second=0), now, oi)
        if df is not None:
            self._cache[key] = (time.monotonic(), df)
        return df

    def quote(self, fno_ids, eq_ids=()) -> Optional[dict]:
        """Live market depth + day stats in ONE request: {"NSE_FNO": {sid: {...}}, "NSE_EQ": {...}} or None."""
        body = {}
        fno = [int(s) for s in fno_ids if s]
        eq = [int(s) for s in eq_ids if s]
        if fno:
            body["NSE_FNO"] = fno
        if eq:
            body["NSE_EQ"] = eq
        if not body:
            return None
        with self._lock:
            wait = self._last_quote + _QUOTE_GAP_SECS - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                resp = self._session.post(_QUOTE, json=body, timeout=10)
            except requests.RequestException as exc:
                logger.warning("agent data: market quote failed: %s", exc)
                return None
            finally:
                self._last_quote = time.monotonic()
        if resp.status_code != 200:
            logger.warning("agent data: market quote -> HTTP %d %s", resp.status_code, resp.text[:120])
            return None
        return (resp.json() or {}).get("data") or None

    def history(self) -> Optional[pd.DataFrame]:
        """NIFTY 5-min candles of the last ~60 sessions (two requests, once a day)."""
        today = datetime.now(_IST).date()
        if self._history is not None and self._history_day == today:
            return self._history
        now = datetime.now(_IST)
        parts = []
        for a, b in ((now - timedelta(days=85), now - timedelta(days=43)), (now - timedelta(days=42), now)):
            df = self._candles(NIFTY_ID, "IDX_I", "INDEX", "5", a.replace(hour=9, minute=0, second=0), b)
            if df is not None:
                parts.append(df)
        if parts:
            self._history = pd.concat(parts).drop_duplicates("timestamp").reset_index(drop=True)
            self._history_day = today
        return self._history

    def future(self) -> Optional[Tuple[str, str]]:
        """(expiry, security id) of the nearest NIFTY future, from the contract list."""
        today = datetime.now(_IST).date()
        if self._future and self._future[0] == today:
            return self._future[1], self._future[2]
        if not self._scrip_csv.exists():
            return None
        best = None
        with open(self._scrip_csv, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if (r.get("UNDERLYING_SYMBOL") == "NIFTY" and r.get("INSTRUMENT") == "FUTIDX"
                        and r.get("SM_EXPIRY_DATE", "") >= today.isoformat()):
                    if best is None or r["SM_EXPIRY_DATE"] < best[0]:
                        best = (r["SM_EXPIRY_DATE"], r["SECURITY_ID"])
        if best:
            self._future = (today, best[0], best[1])
            return best
        return None

    # ── option chain, handed over by the engine ─────────────────────────────
    def on_chain(self, payload: dict) -> None:
        if not payload or not payload.get("oc"):
            return
        self._chain = (time.monotonic(), payload)
        now = datetime.now(_IST)
        if self._oi_samples and (now - self._oi_samples[-1][0]).total_seconds() < 55:
            return
        sample, px = {}, {}
        for k, row in payload["oc"].items():
            try:
                strike = float(k)
            except ValueError:
                continue
            for s in ("ce", "pe"):
                o = (row or {}).get(s) or {}
                if o.get("oi") is not None:
                    sample[(strike, s.upper())] = float(o.get("oi") or 0)
                if o.get("last_price"):
                    iv = o.get("implied_volatility")
                    px[(strike, s.upper())] = (float(o["last_price"]), float(iv) if iv else None)
        self._oi_samples.append((now, sample))
        self._px_samples.append((now, float(payload.get("last_price") or 0), px))

    def chain(self, max_age_secs: float = 60.0) -> Optional[dict]:
        if self._chain and time.monotonic() - self._chain[0] <= max_age_secs:
            return self._chain[1]
        return None

    def oi_change(self, minutes: int = 30) -> Dict[Tuple[float, str], float]:
        """Open interest added per (strike, side) over the last `minutes` (from the engine's polls)."""
        if len(self._oi_samples) < 2:
            return {}
        now, latest = self._oi_samples[-1]
        past = next((s for t, s in self._oi_samples if (now - t).total_seconds() <= minutes * 60 + 30), None)
        if past is None:
            return {}
        return {k: v - past.get(k, v) for k, v in latest.items()}

    def chain_trend(self, strike: Optional[float], side: str, minutes: int = 30) -> dict:
        """How the ATM straddle (the market's expected move) and our option's IV moved over
        the last `minutes`, from the engine's chain polls. {} until there is enough history."""
        if len(self._px_samples) < 2:
            return {}
        now, spot, latest = self._px_samples[-1]
        past = next(((t, s, p) for t, s, p in self._px_samples if (now - t).total_seconds() <= minutes * 60 + 30), None)
        if past is None or past[0] == now:
            return {}

        def straddle(spot_, px):
            ks = sorted({k for k, _ in px})
            if not ks or not spot_:
                return None
            atm = min(ks, key=lambda k: abs(k - spot_))
            ce, pe = px.get((atm, "CE")), px.get((atm, "PE"))
            return (ce[0] + pe[0]) if ce and pe else None

        out = {"minutes": int((now - past[0]).total_seconds() // 60)}
        a, b = straddle(past[1], past[2]), straddle(spot, latest)
        if a and b:
            out["atm_straddle_then_now"] = [round(a, 1), round(b, 1)]
        if strike is not None:
            then, nowv = past[2].get((float(strike), side)), latest.get((float(strike), side))
            if then and nowv:
                out["our_option_ltp_then_now"] = [round(then[0], 2), round(nowv[0], 2)]
                if then[1] and nowv[1]:
                    out["our_option_iv_then_now"] = [round(then[1], 2), round(nowv[1], 2)]
        return out

    def pcr_trend(self, minutes: int = 30) -> Optional[Tuple[float, float]]:
        """Put/call open-interest ratio then (about `minutes` ago) and now, from the engine's polls."""
        if len(self._oi_samples) < 2:
            return None
        now, latest = self._oi_samples[-1]
        past = next((s for t, s in self._oi_samples if (now - t).total_seconds() <= minutes * 60 + 30), None)
        if past is None or past is latest:
            return None

        def pcr(s):
            ce = sum(v for (k, side), v in s.items() if side == "CE")
            pe = sum(v for (k, side), v in s.items() if side == "PE")
            return pe / ce if ce else None
        a, b = pcr(past), pcr(latest)
        return (round(a, 2), round(b, 2)) if a and b else None

    def oi_window_minutes(self) -> int:
        if len(self._oi_samples) < 2:
            return 0
        return int((self._oi_samples[-1][0] - self._oi_samples[0][0]).total_seconds() // 60)
