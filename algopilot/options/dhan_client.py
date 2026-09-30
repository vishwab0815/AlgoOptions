"""
algopilot/options/dhan_client.py — DhanHQ market-data REST access for the
NIFTY options engine (orders live in broker.py).

Four things, nothing else:
  1. Option-chain snapshot (POST /v2/optionchain) — spot AND every strike's
     CE/PE last_price in ONE call. Used for spot/band resolution; live leg
     premiums come from the WebSocket feed (see websocket.py), not this.
  2. Expiry list (POST /v2/optionchain/expirylist) — the nearest weekly
     expiry, refreshed roughly hourly.
  3. Per-lot margin (POST /v2/margincalculator) — the real SPAN+exposure
     figure DhanHQ would charge to sell one lot, used for position sizing.
     Falls back to a clearly-labelled estimate if the option's security id
     hasn't been resolved yet or the call fails — see margin_per_lot().
  4. Intraday candles (POST /v2/charts/intraday) — today's REAL exchange-
     computed OHLC so far, used once per leg on startup/band-resolution to
     rebuild Heikin-Ashi/pattern state instead of starting blind — see
     get_intraday_candles() and engine.py's backfill.

NO order is EVER placed from this module, or anywhere in this engine — this
is pure paper trading (see config.py's hard refusal to start with
OPTIONS_PAPER_TRADING=false). Every call here is read-only market data.

Contract resolution (security id + real lot size, needed for the margin call
and for sizing — the option-chain endpoint above already gives premiums
without either) reads DhanHQ's public scrip master CSV, cached locally for a
day. Column names are matched by substring rather than pinned to an exact
header, and the whole resolver fails soft (logs and returns None) if the
published shape ever changes, since a margin/lot-size lookup must never be
allowed to block paper trading.

Pacing for the option-chain endpoints is a non-blocking cooldown gate, not a
blocking token bucket: DhanHQ documents "1 req/3s" here, but live testing
showed even correctly-3s-spaced requests can still draw an HTTP 429, and a
blocking wait would stall the engine's whole asyncio loop anyway. A miss
just means the engine reuses the last known spot for that cycle — never
fatal, and it never affects premiums (those are WebSocket-pushed).

Every network call also has an `_async` wrapper (via asyncio.to_thread) so
callers on the main event loop — where real-time WebSocket ticks and exit
checks need to keep flowing — never block on a REST round-trip.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime as _dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from ..utils.network import force_ipv4
from ..utils.rate_limiter import RateLimiter
from ..utils.secrets import SecretStr

logger = logging.getLogger(__name__)

force_ipv4()

_BASE_URL = "https://api.dhan.co/v2"
_OPTION_CHAIN_ENDPOINT = f"{_BASE_URL}/optionchain"
_EXPIRY_LIST_ENDPOINT = f"{_BASE_URL}/optionchain/expirylist"
_MARGIN_CALC_ENDPOINT = f"{_BASE_URL}/margincalculator"
_INTRADAY_CHART_ENDPOINT = f"{_BASE_URL}/charts/intraday"

# How long to wait between option-chain-family calls (chain + expiry list
# share this cooldown — they are the same rate-limited endpoint family).
# DhanHQ documents "1 req/3s" here, but live testing showed even
# correctly-3s-spaced requests can still draw an HTTP 429. 5s is a safer
# working default; OPTIONS_CHAIN_MIN_INTERVAL_SECS overrides it.
_DEFAULT_CHAIN_MIN_INTERVAL_SECS = 5.0
# Extra cool-off applied ONLY after an actual 429, on top of the normal
# interval — backs off harder instead of immediately hammering the same
# limit again next cycle.
_CHAIN_429_BACKOFF_SECS = 15.0

_MARGIN_RATE_PER_SEC = 9.0  # shares the Order-API family's 10/sec cap; low
                            # frequency (entries only) so a brief blocking
                            # wait here is harmless, unlike the chain endpoint.
_MARGIN_TTL_SECS = 900.0    # SPAN/exposure margin is stable within a session

_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
_SCRIP_MASTER_CACHE_TTL = 24 * 3600.0

# A rejected token is logged at most this often, not on every poll.
_AUTH_FAIL_LOG_EVERY_SECS = 60.0

_EXCLUDED_UNDERLYINGS = ("BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT")


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
    security_id: str
    lot_size: int


def _normalize_date(raw: str) -> Optional[str]:
    """Best-effort parse of a date string to 'YYYY-MM-DD', trying every
    format DhanHQ's option-chain API and its scrip-master CSV have used."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%b-%Y", "%d %b %Y"):
        try:
            return _dt.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    return None


class OptionsDhanClient:
    def __init__(
        self, client_id: str, access_token: SecretStr,
        scrip_cache_path: str = "data/dhan_scrip_master.csv",
        chain_min_interval_secs: float = _DEFAULT_CHAIN_MIN_INTERVAL_SECS,
    ) -> None:
        self.client_id = client_id
        self._access_token = access_token
        self._session = requests.Session()
        self._session.headers.update({
            "access-token": access_token.get_secret(),
            "client-id": client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._chain_min_interval = chain_min_interval_secs
        self._chain_next_allowed = 0.0   # monotonic epoch; 0 = allowed immediately
        self._margin_limiter = RateLimiter(rate=_MARGIN_RATE_PER_SEC, capacity=1)
        self._margin_cache: Dict[str, Tuple[float, float]] = {}
        self._scrip_cache_path = Path(scrip_cache_path)
        # (expiry YYYY-MM-DD, strike, "CE"|"PE") -> ContractInfo
        self._scrip_index: Optional[Dict[Tuple[str, float, str], ContractInfo]] = None
        # Set on the first HTTP 401/403 from any endpoint — the engine stops on
        # it rather than running blind (see OptionsEngine.maintenance_tick).
        self.auth_failed = False
        self._last_auth_log = -1e9

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass

    def _auth_rejected(self, resp: requests.Response, what: str) -> bool:
        """BUG FIXED: a dead token used to surface only as a generic
        "expiry list lookup failed: 401 Client Error" repeated every few
        seconds while the engine sat there doing nothing — no expiry, so no
        chain, so no band, so no trades, and nothing said why. A 401/403 is
        now named for what it is, logged at most once a minute."""
        if resp.status_code not in (401, 403):
            return False
        self.auth_failed = True
        now = time.monotonic()
        if now - self._last_auth_log >= _AUTH_FAIL_LOG_EVERY_SECS:
            self._last_auth_log = now
            logger.error(
                "DhanHQ REJECTED THE ACCESS TOKEN (HTTP %d on %s). It has expired (tokens last 24h), "
                "been revoked, or belongs to a different client id than DHAN_CLIENT_ID%s. Nothing can "
                "be fetched until it is fixed: generate a new token (scripts/generate_token.py), set "
                "DHAN_ACCESS_TOKEN (and a matching DHAN_CLIENT_ID) in .env, and restart.",
                resp.status_code, what,
                "" if resp.status_code == 401 else ", or this account lacks DhanHQ Data API access",
            )
        return True

    # ── Option-chain pacing (non-blocking cooldown, not a blocking wait) ──────

    def _chain_gate_open(self) -> bool:
        return time.monotonic() >= self._chain_next_allowed

    def _note_chain_call(self, was_429: bool) -> None:
        cooldown = _CHAIN_429_BACKOFF_SECS if was_429 else self._chain_min_interval
        self._chain_next_allowed = time.monotonic() + cooldown

    # ── Option chain ─────────────────────────────────────────────────────────

    def get_expiry_list(self, underlying_scrip: str, underlying_seg: str) -> List[str]:
        if not self._chain_gate_open():
            logger.debug(
                "Options: expiry list call skipped — chain cooldown active for another %.1fs.",
                self._chain_next_allowed - time.monotonic(),
            )
            return []
        try:
            resp = self._session.post(
                _EXPIRY_LIST_ENDPOINT,
                json={"UnderlyingScrip": int(underlying_scrip), "UnderlyingSeg": underlying_seg},
                timeout=10.0,
            )
            self._note_chain_call(was_429=resp.status_code == 429)
            if self._auth_rejected(resp, "expiry list"):
                return []
            resp.raise_for_status()
            expiries = resp.json().get("data", [])
            return sorted(expiries)
        except (requests.RequestException, ValueError) as exc:
            logger.error("Options: expiry list lookup failed: %s", exc)
            return []

    async def get_expiry_list_async(self, underlying_scrip: str, underlying_seg: str) -> List[str]:
        """Non-blocking wrapper — runs the blocking HTTP call in a thread pool
        so the asyncio event loop stays live for exit checks during the call."""
        return await asyncio.to_thread(self.get_expiry_list, underlying_scrip, underlying_seg)

    def get_chain(self, underlying_scrip: str, underlying_seg: str, expiry: str) -> Optional[ChainSnapshot]:
        if not self._chain_gate_open():
            logger.debug(
                "Options: option-chain call skipped — chain cooldown active for another %.1fs.",
                self._chain_next_allowed - time.monotonic(),
            )
            return None
        try:
            resp = self._session.post(
                _OPTION_CHAIN_ENDPOINT,
                json={
                    "UnderlyingScrip": int(underlying_scrip),
                    "UnderlyingSeg": underlying_seg,
                    "Expiry": expiry,
                },
                timeout=10.0,
            )
            self._note_chain_call(was_429=resp.status_code == 429)
            if resp.status_code == 429:
                logger.warning(
                    "Options: option-chain HTTP 429 (rate limited) — backing off %.0fs.",
                    _CHAIN_429_BACKOFF_SECS,
                )
                return None
            if self._auth_rejected(resp, "option chain"):
                return None
            resp.raise_for_status()
            payload = resp.json().get("data", {})
            spot = float(payload.get("last_price", 0.0))
            legs: Dict[float, Dict[str, LegQuote]] = {}
            for strike_str, row in (payload.get("oc") or {}).items():
                try:
                    strike = float(strike_str)
                except ValueError:
                    continue
                leg_map: Dict[str, LegQuote] = {}
                for side_key in ("ce", "pe"):
                    side = row.get(side_key)
                    if side:
                        leg_map[side_key] = LegQuote(
                            last_price=float(side.get("last_price", 0.0)),
                            oi=float(side.get("oi", 0.0)),
                            volume=float(side.get("volume", 0.0)),
                        )
                if leg_map:
                    legs[strike] = leg_map
            return ChainSnapshot(spot=spot, legs=legs)
        except (requests.RequestException, ValueError) as exc:
            logger.error("Options: option-chain fetch failed: %s", exc)
            return None

    async def get_chain_async(
        self, underlying_scrip: str, underlying_seg: str, expiry: str
    ) -> Optional[ChainSnapshot]:
        """Non-blocking wrapper — runs the blocking HTTP call in a thread pool
        so the asyncio event loop stays live for exit checks during the call."""
        return await asyncio.to_thread(self.get_chain, underlying_scrip, underlying_seg, expiry)

    # ── Historical/intraday candles (startup pattern-state backfill) ──────────

    def get_intraday_candles(
        self, security_id: str, exchange_segment: str, instrument: str,
        interval_minutes: int, from_dt: str, to_dt: str,
    ) -> Optional[List[Dict[str, float]]]:
        """Real exchange-computed OHLC candles for TODAY so far, used only to
        rebuild a leg's Heikin-Ashi/pattern state on startup — never rate-
        limited against the option-chain cooldown gate above (separate
        endpoint family; DhanHQ documents no explicit limit here, and this
        is called once per leg per day, not in the live polling loop).

        Returns a list of {open, high, low, close, volume, timestamp} dicts,
        oldest first, or None on any failure — callers must treat that as
        "no backfill available," not as an error worth blocking startup over.
        """
        try:
            resp = self._session.post(
                _INTRADAY_CHART_ENDPOINT,
                json={
                    "securityId": security_id,
                    "exchangeSegment": exchange_segment,
                    "instrument": instrument,
                    "interval": str(interval_minutes),
                    "oi": False,
                    "fromDate": from_dt,
                    "toDate": to_dt,
                },
                timeout=15.0,
            )
            if self._auth_rejected(resp, "intraday candles"):
                return None
            if resp.status_code != 200:
                logger.warning(
                    "Options: intraday candles HTTP %d for security %s — no DhanHQ bars this request.",
                    resp.status_code, security_id,
                )
                return None
            data = resp.json()
            opens = data.get("open") or []
            highs = data.get("high") or []
            lows = data.get("low") or []
            closes = data.get("close") or []
            volumes = data.get("volume") or []
            timestamps = data.get("timestamp") or []
            n = len(opens)
            if n == 0 or not (len(highs) == len(lows) == len(closes) == len(timestamps) == n):
                return []
            return [
                {
                    "open": float(opens[i]), "high": float(highs[i]),
                    "low": float(lows[i]), "close": float(closes[i]),
                    "volume": float(volumes[i]) if i < len(volumes) else 0.0,
                    "timestamp": float(timestamps[i]),
                }
                for i in range(n)
            ]
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "Options: intraday candle backfill failed for security %s (%s) — skipping backfill.",
                security_id, exc,
            )
            return None

    async def get_intraday_candles_async(
        self, security_id: str, exchange_segment: str, instrument: str,
        interval_minutes: int, from_dt: str, to_dt: str,
    ) -> Optional[List[Dict[str, float]]]:
        """Non-blocking wrapper — runs the blocking HTTP call in a thread pool."""
        return await asyncio.to_thread(
            self.get_intraday_candles, security_id, exchange_segment, instrument,
            interval_minutes, from_dt, to_dt,
        )

    # ── Margin ───────────────────────────────────────────────────────────────

    def margin_per_lot(
        self, security_id: Optional[str], exchange_segment: str, price: float,
        lot_size: int, fallback: float,
    ) -> Tuple[float, bool]:
        """Returns (per_lot_margin, is_live). is_live=False means `fallback`
        was used because the option's security id isn't known yet or the
        live call failed — never silently treated as the real figure by the
        caller (see engine.py, which logs `margin_live` on every entry)."""
        if not security_id or price <= 0:
            return fallback, False

        cached = self._margin_cache.get(security_id)
        if cached is not None and (time.monotonic() - cached[1]) < _MARGIN_TTL_SECS:
            return cached[0], True

        try:
            self._margin_limiter.acquire()
            resp = self._session.post(
                _MARGIN_CALC_ENDPOINT,
                json={
                    "securityId": security_id,
                    "exchangeSegment": exchange_segment,
                    "transactionType": "SELL",
                    "quantity": int(lot_size),
                    "productType": "INTRADAY",
                    "price": float(price),
                },
                timeout=5.0,
            )
            if self._auth_rejected(resp, "margin calculator"):
                return fallback, False
            if resp.status_code != 200:
                logger.warning(
                    "Options: margin calculator HTTP %d — using fallback Rs %.0f/lot.",
                    resp.status_code, fallback,
                )
                return fallback, False
            data = resp.json()
            required = float(data.get("totalMargin", 0.0)) + float(data.get("variableMargin", 0.0))
            if required <= 0:
                return fallback, False
            self._margin_cache[security_id] = (required, time.monotonic())
            return required, True
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "Options: margin calculator call failed (%s) — using fallback Rs %.0f/lot.",
                exc, fallback,
            )
            return fallback, False

    async def margin_per_lot_async(
        self, security_id: Optional[str], exchange_segment: str, price: float,
        lot_size: int, fallback: float,
    ) -> Tuple[float, bool]:
        """Non-blocking wrapper. LATENCY: a cached figure is returned inline —
        this sits on the entry path the instant a candle closes, and even a
        thread-pool hop costs time there. Only a cache miss goes to a thread
        (token-bucket wait + HTTP), and the engine pre-fetches in the
        background so that miss never lands at the trigger."""
        if security_id and price > 0:
            cached = self._margin_cache.get(security_id)
            if cached is not None and (time.monotonic() - cached[1]) < _MARGIN_TTL_SECS:
                return cached[0], True
        return await asyncio.to_thread(
            self.margin_per_lot, security_id, exchange_segment, price, lot_size, fallback
        )

    def margin_cache_age(self, security_id: Optional[str]) -> Optional[float]:
        """Seconds since this contract's margin was fetched, or None if never."""
        cached = self._margin_cache.get(security_id) if security_id else None
        return None if cached is None else time.monotonic() - cached[1]

    # ── NIFTY option contract resolution (scrip master) ────────────────────────

    def resolve_contract(self, expiry: str, strike: float, option_type: str) -> Optional[ContractInfo]:
        """The security id AND the real, currently-listed lot size for this
        NIFTY option contract — lot size is set by NSE and revised
        periodically, so this is preferred over a hardcoded config default
        whenever it's available (see engine.py)."""
        self._ensure_scrip_index()
        if not self._scrip_index:
            return None
        expiry_norm = _normalize_date(expiry) or expiry
        return self._scrip_index.get((expiry_norm, float(strike), option_type.upper()))

    async def resolve_contract_async(
        self, expiry: str, strike: float, option_type: str
    ) -> Optional[ContractInfo]:
        """Non-blocking wrapper — the first call may trigger a CSV download
        (up to 60s); running it in a thread pool keeps the event loop live."""
        return await asyncio.to_thread(self.resolve_contract, expiry, strike, option_type)

    async def warm_contracts_async(self) -> None:
        """Load and index NSE's contract list now (a ~2.5 s download when the
        cached copy is stale) instead of at the first strike lookup."""
        await asyncio.to_thread(self._ensure_scrip_index)

    def _ensure_scrip_index(self) -> None:
        if self._scrip_index is not None and self._scrip_cache_path.exists():
            age = time.time() - self._scrip_cache_path.stat().st_mtime
            if age < _SCRIP_MASTER_CACHE_TTL:
                return
        raw_text = self._load_or_download_scrip_master()
        if raw_text is None:
            self._scrip_index = self._scrip_index or {}
            return
        self._scrip_index = self._parse_nifty_options(raw_text)
        logger.info("Options: scrip master indexed — %d NIFTY option contracts.", len(self._scrip_index))

    def _load_or_download_scrip_master(self) -> Optional[str]:
        try:
            if self._scrip_cache_path.exists():
                age = time.time() - self._scrip_cache_path.stat().st_mtime
                if age < _SCRIP_MASTER_CACHE_TTL:
                    return self._scrip_cache_path.read_text(encoding="utf-8", errors="replace")
            resp = requests.get(_SCRIP_MASTER_URL, timeout=60.0)
            resp.raise_for_status()
            self._scrip_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._scrip_cache_path.write_text(resp.text, encoding="utf-8")
            return resp.text
        except (requests.RequestException, OSError) as exc:
            logger.error("Options: scrip master download failed (%s) — trying stale cache if any.", exc)
            if self._scrip_cache_path.exists():
                try:
                    return self._scrip_cache_path.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    return None
            return None

    @staticmethod
    def _parse_nifty_options(raw_text: str) -> Dict[Tuple[str, float, str], ContractInfo]:
        index: Dict[Tuple[str, float, str], ContractInfo] = {}
        reader = csv.DictReader(io.StringIO(raw_text))
        if not reader.fieldnames:
            return index

        def find_col(*needles: str) -> Optional[str]:
            for name in reader.fieldnames:
                lname = name.lower()
                if all(n in lname for n in needles):
                    return name
            return None

        col_security_id = find_col("security", "id")
        # Confirmed against DhanHQ's real scrip-master header: it has BOTH
        # "UNDERLYING_SECURITY_ID" (a numeric id) and "UNDERLYING_SYMBOL"
        # (the name, e.g. "NIFTY") — a bare find_col("underlying") matches
        # the numeric-id column first (it appears earlier in the header),
        # so every row silently failed the "== NIFTY" check below and the
        # index came back empty. Matching "underlying"+"symbol" together
        # targets the name column specifically.
        col_underlying = find_col("underlying", "symbol")
        col_symbol = find_col("trading", "symbol") or find_col("symbol")
        col_strike = find_col("strike")
        col_option_type = find_col("option", "type")
        col_expiry = find_col("expiry", "date") or find_col("expiry")
        col_instrument = find_col("instrument")
        col_lot_size = find_col("lot", "size")
        col_exch = find_col("exch", "id") or find_col("exchange")

        name_field = col_underlying or col_symbol
        required = (col_security_id, name_field, col_strike, col_option_type, col_expiry, col_lot_size)
        if not all(required):
            logger.error(
                "Options: scrip master column detection failed (columns seen=%s) — "
                "cannot resolve option contracts; margin/lot sizing will use configured/fallback values.",
                reader.fieldnames,
            )
            return index

        for row in reader:
            name_val = (row.get(name_field) or "").upper().strip()
            if col_underlying:
                if name_val != "NIFTY":
                    continue
            else:
                if "NIFTY" not in name_val or any(x in name_val for x in _EXCLUDED_UNDERLYINGS):
                    continue
            if col_instrument and "OPT" not in (row.get(col_instrument) or "").upper():
                continue
            if col_exch and (row.get(col_exch) or "").upper() not in ("NSE", ""):
                continue

            try:
                strike = float(row.get(col_strike) or 0)
            except ValueError:
                continue
            if strike <= 0:
                continue

            option_type = (row.get(col_option_type) or "").upper()
            if option_type not in ("CE", "PE"):
                continue

            expiry_raw = (row.get(col_expiry) or "").strip()
            security_id = (row.get(col_security_id) or "").strip()
            if not expiry_raw or not security_id:
                continue
            expiry_norm = _normalize_date(expiry_raw)
            if expiry_norm is None:
                continue

            try:
                lot_size = int(float(row.get(col_lot_size) or 0))
            except ValueError:
                lot_size = 0
            if lot_size <= 0:
                continue

            index[(expiry_norm, strike, option_type)] = ContractInfo(
                security_id=security_id, lot_size=lot_size,
            )
        return index
