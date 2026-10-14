"""
algopilot/agent/data.py — the market data the agent reads.

Candles (NIFTY 1-min / 5-min, India VIX, BANK NIFTY, NIFTY future with volume
and open interest) come from DhanHQ's data API, one request at a time with a gap
between them so the agent never crowds the engine's own requests.

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
_GAP_SECS = 0.3            # between the agent's own data-API calls (Dhan: 5 requests/s per account)
_TODAY_TTL_SECS = 20.0     # today's candles are re-used for this long (CE and PE at the same close)

NIFTY_ID, VIX_ID, BANKNIFTY_ID = "13", "21", "25"


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
        sample = {}
        for k, row in payload["oc"].items():
            try:
                strike = float(k)
            except ValueError:
                continue
            for s in ("ce", "pe"):
                o = (row or {}).get(s) or {}
                if o.get("oi") is not None:
                    sample[(strike, s.upper())] = float(o.get("oi") or 0)
        self._oi_samples.append((now, sample))

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

    def oi_window_minutes(self) -> int:
        if len(self._oi_samples) < 2:
            return 0
        return int((self._oi_samples[-1][0] - self._oi_samples[0][0]).total_seconds() // 60)
