"""
algopilot/options/kite_client.py — Zerodha Kite Connect market data for the NIFTY options engine.

Same interface the engine has always used (it replaces the old DhanHQ client);
orders live in broker.py.

  1. Option chain — Kite has no option-chain API. The chain is ONE batched
     GET /quote for every strike of the expiry (CE and PE) plus the NIFTY 50
     index (~210 instruments; the limit is 500 per call, 1 call per second).
  2. Expiries — from today's instrument list (instruments.py).
  3. Per-lot margin — POST /margins/orders (JSON): the real SPAN + exposure
     Zerodha would block to SELL one lot, for position sizing; a clearly
     labelled fallback if it can't be fetched.
  4. Candles — GET /instruments/historical/{instrument_token}/{interval}
     (exchange OHLC; the newest candle may still be forming — the engine only
     ever uses candles whose 5 minutes have ended).

Every call also has an `_async` wrapper (asyncio.to_thread) so the event loop,
where ticks and exit checks run, never waits on a REST round trip.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

from .instruments import InstrumentRegistry
from .kite_api import KiteHTTP
from ..utils.secrets import SecretStr

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN_MIN_INTERVAL_SECS = 2.0
_CHAIN_429_BACKOFF_SECS = 10.0
_QUOTE_MAX = 500
_MARGIN_TTL_SECS = 900.0
_INTERVALS = {1: "minute", 3: "3minute", 5: "5minute", 10: "10minute", 15: "15minute", 30: "30minute",
              60: "60minute"}


@dataclass(frozen=True)
class LegQuote:
    last_price: float
    oi: float = 0.0
    volume: float = 0.0


@dataclass(frozen=True)
class ChainSnapshot:
    spot: float
    legs: Dict[float, Dict[str, LegQuote]]  # strike -> {"ce": LegQuote, "pe": LegQuote}


@dataclass(frozen=True)
class ContractInfo:
    security_id: str                         # Kite instrument_token, as text
    lot_size: int
    tradingsymbol: str = ""


def _epoch(ts: str) -> float:
    """Kite candle time '2026-10-26T09:15:00+0530' -> unix seconds."""
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z").timestamp()


class OptionsKiteClient:
    def __init__(self, api_key: str, access_token: SecretStr, product: str = "MIS",
                 chain_min_interval_secs: float = _DEFAULT_CHAIN_MIN_INTERVAL_SECS,
                 cache_dir: str = "data") -> None:
        self.http = KiteHTTP(api_key, access_token)
        self.registry = InstrumentRegistry(self.http, cache_dir=cache_dir)
        self.product = product
        self._chain_min_interval = chain_min_interval_secs
        self._chain_next_allowed = 0.0
        self._margin_cache: Dict[str, Tuple[float, float]] = {}

    @property
    def auth_failed(self) -> bool:
        return self.http.auth_failed

    def close(self) -> None:
        self.http.close()

    # ── expiries ─────────────────────────────────────────────────────────────
    def get_expiry_list(self, underlying_scrip: str = "", underlying_seg: str = "") -> List[str]:
        self.registry.ensure()
        return self.registry.expiries()

    async def get_expiry_list_async(self, underlying_scrip: str = "", underlying_seg: str = "") -> List[str]:
        return await asyncio.to_thread(self.get_expiry_list, underlying_scrip, underlying_seg)

    # ── option chain (batched quotes) ────────────────────────────────────────
    def get_chain(self, underlying_scrip: str, underlying_seg: str, expiry: str) -> Optional[ChainSnapshot]:
        """underlying_scrip / underlying_seg: the index's tradingsymbol and exchange ("NIFTY 50", "NSE")."""
        if time.monotonic() < self._chain_next_allowed:
            return None
        self.registry.ensure()
        index_key = f"{underlying_seg}:{underlying_scrip}"
        contracts = self.registry.chain(expiry)
        if not contracts:
            return None
        if len(contracts) + 1 > _QUOTE_MAX:          # keep the strikes nearest the middle of the list
            contracts = sorted(contracts, key=lambda c: c.strike)
            mid = len(contracts) // 2
            half = (_QUOTE_MAX - 1) // 2
            contracts = contracts[max(0, mid - half):mid + half]
        keys = [index_key] + [c.quote_key for c in contracts]
        try:
            reply = self.http.call("GET", "/quote", family="quote", params=[("i", k) for k in keys])
        except requests.RequestException as exc:
            self._chain_next_allowed = time.monotonic() + self._chain_min_interval
            logger.error("Options: option-chain quote failed: %s", exc)
            return None
        self._chain_next_allowed = time.monotonic() + (_CHAIN_429_BACKOFF_SECS if reply.status == 429
                                                       else self._chain_min_interval)
        if not reply.ok:
            if reply.status == 429:
                logger.warning("Options: quote HTTP 429 (rate limited) — backing off %.0fs.", _CHAIN_429_BACKOFF_SECS)
            elif not self.http.auth_failed:
                logger.error("Options: option-chain quote refused: %s", reply.reason())
            return None
        data = reply.data or {}
        spot = float((data.get(index_key) or {}).get("last_price") or 0.0)
        legs: Dict[float, Dict[str, LegQuote]] = {}
        for c in contracts:
            q = data.get(c.quote_key)
            if not q:
                continue                             # Kite leaves out instruments with no data
            legs.setdefault(float(c.strike), {})[c.instrument_type.lower()] = LegQuote(
                last_price=float(q.get("last_price") or 0.0), oi=float(q.get("oi") or 0.0),
                volume=float(q.get("volume") or 0.0))
        return ChainSnapshot(spot=spot, legs=legs)

    async def get_chain_async(self, underlying_scrip: str, underlying_seg: str, expiry: str) -> Optional[ChainSnapshot]:
        return await asyncio.to_thread(self.get_chain, underlying_scrip, underlying_seg, expiry)

    # ── candles ──────────────────────────────────────────────────────────────
    def get_intraday_candles(self, security_id: str, exchange_segment: str, instrument: str,
                             interval_minutes: int, from_dt: str, to_dt: str) -> Optional[List[Dict[str, float]]]:
        """Exchange OHLC candles, oldest first: [{open, high, low, close, volume, timestamp(unix s, candle start)}].
        None on any failure — callers treat that as "no candles", never as fatal."""
        interval = _INTERVALS.get(int(interval_minutes))
        if interval is None or not security_id:
            return None
        try:
            reply = self.http.call("GET", f"/instruments/historical/{int(security_id)}/{interval}",
                                   family="historical", params={"from": from_dt, "to": to_dt, "continuous": 0, "oi": 0},
                                   timeout=15.0)
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Options: candles for %s failed (%s).", security_id, exc)
            return None
        if not reply.ok:
            if not self.http.auth_failed:
                logger.warning("Options: candles for %s refused: %s", security_id, reply.reason())
            return None
        out = []
        for c in (reply.data or {}).get("candles") or []:
            try:
                out.append({"timestamp": _epoch(c[0]), "open": float(c[1]), "high": float(c[2]),
                            "low": float(c[3]), "close": float(c[4]), "volume": float(c[5] if len(c) > 5 else 0)})
            except (TypeError, ValueError, IndexError):
                continue
        return out

    async def get_intraday_candles_async(self, security_id: str, exchange_segment: str, instrument: str,
                                         interval_minutes: int, from_dt: str, to_dt: str) -> Optional[List[Dict[str, float]]]:
        return await asyncio.to_thread(self.get_intraday_candles, security_id, exchange_segment, instrument,
                                       interval_minutes, from_dt, to_dt)

    # ── margin ───────────────────────────────────────────────────────────────
    def margin_per_lot(self, security_id: Optional[str], exchange_segment: str, price: float,
                       lot_size: int, fallback: float, force: bool = False) -> Tuple[float, bool]:
        """(margin to SELL one lot, is_live). is_live=False = `fallback` was used."""
        if not security_id or price <= 0:
            return fallback, False
        cached = self._margin_cache.get(security_id)
        if not force and cached is not None and (time.monotonic() - cached[1]) < _MARGIN_TTL_SECS:
            return cached[0], True
        ins = self.registry.token(security_id)
        if ins is None:
            return fallback, False
        # The stand-alone figure for ONE sell (consider_positions=false): /margins/orders would
        # fold in open positions and orders, which is not what one new lot needs.
        body = [{"exchange": ins.exchange, "tradingsymbol": ins.tradingsymbol, "transaction_type": "SELL",
                 "variety": "regular", "product": self.product, "order_type": "LIMIT",
                 "quantity": int(lot_size), "price": float(price), "trigger_price": 0}]
        try:
            reply = self.http.call("POST", "/margins/basket", params={"consider_positions": "false"},
                                   json_body=body, timeout=5.0)
        except requests.RequestException as exc:
            logger.warning("Options: margin calculation failed (%s) — using fallback Rs %.0f/lot.", exc, fallback)
            return fallback, False
        if not reply.ok or not isinstance(reply.data, dict):
            logger.warning("Options: margin calculation %s — using fallback Rs %.0f/lot.",
                           reply.reason() if not reply.ok else "empty", fallback)
            return fallback, False
        try:
            required = float((reply.data.get("initial") or {}).get("total") or 0.0)
        except (TypeError, ValueError, AttributeError):
            required = 0.0
        if required <= 0:
            return fallback, False
        self._margin_cache[security_id] = (required, time.monotonic())
        return required, True

    async def margin_per_lot_async(self, security_id: Optional[str], exchange_segment: str, price: float,
                                   lot_size: int, fallback: float, force: bool = False) -> Tuple[float, bool]:
        """A cached figure is returned inline (this sits on the entry path); only a miss goes to a thread."""
        if not force and security_id and price > 0:
            cached = self._margin_cache.get(security_id)
            if cached is not None and (time.monotonic() - cached[1]) < _MARGIN_TTL_SECS:
                return cached[0], True
        return await asyncio.to_thread(self.margin_per_lot, security_id, exchange_segment, price, lot_size,
                                       fallback, force)

    def margin_cache_age(self, security_id: Optional[str]) -> Optional[float]:
        cached = self._margin_cache.get(security_id) if security_id else None
        return None if cached is None else time.monotonic() - cached[1]

    # ── contracts ────────────────────────────────────────────────────────────
    def resolve_contract(self, expiry: str, strike: float, option_type: str) -> Optional[ContractInfo]:
        self.registry.ensure()
        ins = self.registry.option(expiry, strike, option_type)
        if ins is None:
            return None
        return ContractInfo(security_id=str(ins.token), lot_size=ins.lot_size, tradingsymbol=ins.tradingsymbol)

    async def resolve_contract_async(self, expiry: str, strike: float, option_type: str) -> Optional[ContractInfo]:
        return await asyncio.to_thread(self.resolve_contract, expiry, strike, option_type)

    async def warm_contracts_async(self) -> None:
        await asyncio.to_thread(self.registry.ensure)
