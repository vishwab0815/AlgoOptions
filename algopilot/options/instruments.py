"""
algopilot/options/instruments.py — Zerodha's instrument list (contract master).

    GET /instruments/NFO and /instruments/NSE  -> CSV (gzip on the wire), one row per contract:
    instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,
    instrument_type,segment,exchange

Kite builds the list once a day; it is downloaded after 08:30 IST and cached in
data/ for the rest of the day. NIFTY options are the rows with name "NIFTY",
instrument_type CE / PE and segment NFO-OPT; expiry dates and trading symbols are
always taken from the list, never built by hand (weekly symbols look like
NIFTY26N0325000CE, the month's last expiry NIFTY26OCT25000CE, and a holiday moves
the expiry to Monday).

Orders are placed by exchange + tradingsymbol; prices stream by instrument_token.
The engine carries the token (as a string) as each leg's id; this registry maps
it to the tradingsymbol, lot size and tick size.
"""
from __future__ import annotations

import csv
import io
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .kite_api import KiteHTTP

logger = logging.getLogger(__name__)
_IST = ZoneInfo("Asia/Kolkata")
_FRESH_AFTER = (8, 30)              # the day's list is published around 08:30 IST


@dataclass(frozen=True)
class Instrument:
    token: int
    tradingsymbol: str
    name: str
    expiry: str                     # YYYY-MM-DD, "" for indices / equity
    strike: float
    tick_size: float
    lot_size: int
    instrument_type: str            # CE / PE / FUT / EQ
    segment: str                    # NFO-OPT / NFO-FUT / INDICES / NSE
    exchange: str                   # NFO / NSE

    @property
    def quote_key(self) -> str:
        return f"{self.exchange}:{self.tradingsymbol}"


def parse_instruments(text: str) -> List[Instrument]:
    out: List[Instrument] = []
    for r in csv.DictReader(io.StringIO(text)):
        try:
            out.append(Instrument(
                token=int(r["instrument_token"]), tradingsymbol=r["tradingsymbol"].strip(),
                name=(r.get("name") or "").strip().strip('"'), expiry=(r.get("expiry") or "").strip(),
                strike=float(r.get("strike") or 0), tick_size=float(r.get("tick_size") or 0),
                lot_size=int(float(r.get("lot_size") or 0)), instrument_type=(r.get("instrument_type") or "").strip(),
                segment=(r.get("segment") or "").strip(), exchange=(r.get("exchange") or "").strip()))
        except (KeyError, ValueError):
            continue
    return out


class InstrumentRegistry:
    """Today's NIFTY options + the index rows, by (expiry, strike, CE|PE) and by token."""

    def __init__(self, http: KiteHTTP, cache_dir: str = "data", underlying: str = "NIFTY") -> None:
        self._http = http
        self._dir = Path(cache_dir)
        self._underlying = underlying
        self._lock = threading.Lock()
        self._loaded_for: Optional[str] = None
        self.options: Dict[Tuple[str, float, str], Instrument] = {}
        self.by_token: Dict[int, Instrument] = {}
        self.by_key: Dict[str, Instrument] = {}

    # ── loading ─────────────────────────────────────────────────────────────
    def _cache_file(self, exchange: str) -> Path:
        return self._dir / f"kite_instruments_{exchange}.csv"

    @staticmethod
    def _is_current(path: Path) -> bool:
        """Downloaded today after 08:30 IST (or today before 08:30 and it is still before 08:30)."""
        if not path.exists():
            return False
        now = datetime.now(_IST)
        got = datetime.fromtimestamp(path.stat().st_mtime, _IST)
        cutoff = now.replace(hour=_FRESH_AFTER[0], minute=_FRESH_AFTER[1], second=0, microsecond=0)
        if got.date() != now.date():
            return False
        return got >= cutoff or now < cutoff

    def _text(self, exchange: str) -> Optional[str]:
        path = self._cache_file(exchange)
        if self._is_current(path):
            return path.read_text(encoding="utf-8", errors="replace")
        try:
            reply = self._http.call("GET", f"/instruments/{exchange}", timeout=60.0)
            if reply.ok and isinstance(reply.data, str) and reply.data.startswith("instrument_token"):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(reply.data, encoding="utf-8")
                return reply.data
            logger.error("Kite instrument list %s: %s", exchange, reply.reason())
        except Exception as exc:
            logger.error("Kite instrument list %s download failed: %s", exchange, exc)
        if path.exists():
            logger.warning("Using the cached %s instrument list from %s.", exchange,
                           datetime.fromtimestamp(path.stat().st_mtime, _IST).strftime("%d-%b %H:%M"))
            return path.read_text(encoding="utf-8", errors="replace")
        return None

    def ensure(self) -> None:
        today = datetime.now(_IST).strftime("%Y-%m-%d %p")    # reload once after 08:30 too
        with self._lock:
            if self._loaded_for == today and self.options:
                return
            nfo, nse = self._text("NFO"), self._text("NSE")
            options: Dict[Tuple[str, float, str], Instrument] = {}
            by_token: Dict[int, Instrument] = {}
            by_key: Dict[str, Instrument] = {}
            for ins in (parse_instruments(nfo) if nfo else []):
                if ins.name == self._underlying and ins.instrument_type in ("CE", "PE") and ins.expiry:
                    options[(ins.expiry, float(ins.strike), ins.instrument_type)] = ins
                    by_token[ins.token] = ins
                    by_key[ins.quote_key] = ins
                elif ins.name == self._underlying and ins.instrument_type == "FUT":
                    by_token[ins.token] = ins
                    by_key[ins.quote_key] = ins
            for ins in (parse_instruments(nse) if nse else []):
                if ins.segment == "INDICES":
                    by_token[ins.token] = ins
                    by_key[ins.quote_key] = ins
            if options:
                self.options, self.by_token, self.by_key = options, by_token, by_key
                self._loaded_for = today
                logger.info("Kite instruments: %d %s option contracts, %d expiries.", len(options),
                            self._underlying, len(self.expiries()))
            else:
                logger.error("Kite instruments: no %s options found — contracts can't be resolved.", self._underlying)

    # ── lookups ─────────────────────────────────────────────────────────────
    def expiries(self) -> List[str]:
        return sorted({k[0] for k in self.options})

    def option(self, expiry: str, strike: float, side: str) -> Optional[Instrument]:
        return self.options.get((expiry, float(strike), side.upper()))

    def chain(self, expiry: str) -> List[Instrument]:
        return [ins for (e, _, _), ins in self.options.items() if e == expiry]

    def token(self, token) -> Optional[Instrument]:
        try:
            return self.by_token.get(int(token))
        except (TypeError, ValueError):
            return None

    def age_secs(self) -> float:
        p = self._cache_file("NFO")
        return time.time() - p.stat().st_mtime if p.exists() else float("inf")
