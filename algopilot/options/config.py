"""
algopilot/options/config.py — configuration for the NIFTY options
paper-trading engine.

Pure paper trading only — DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN are used purely
to read live market data (option chain, expiry list, margin calculator,
scrip master, and the real-time WebSocket feed). No order is ever placed;
OPTIONS_PAPER_TRADING=false is refused at startup.

Entries are pure Heikin-Ashi pattern (GREEN->RED->RED) — there is no RSI
gate, no volume gate, and no profit ratchet. The only two things that ever
close a position are the Heikin-Ashi trailing stop and the EOD square-off.
"""
from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from ..utils.secrets import SecretStr

_IST = ZoneInfo("Asia/Kolkata")


class OptionsConfigError(RuntimeError):
    """Raised when OPTIONS_* configuration is invalid or missing."""


def token_expiry_ist(raw_token: str) -> Optional[datetime]:
    """The DhanHQ access token's JWT expiry, in IST — or None if it isn't a
    JWT we recognise. Used to warn BEFORE a silent disconnect happens: when
    DhanHQ's WebSocket drops a connection for an expired token, the SDK
    reports it via a bare print() to stdout (see websocket.py's docstring),
    which never reaches our log file — this is the proactive alternative,
    checked once at startup and printed loudly in the banner."""
    try:
        payload = raw_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return datetime.fromtimestamp(float(claims["exp"]), timezone.utc).astimezone(_IST)
    except Exception:
        return None


_CANDLE_TIMEFRAMES = {"1m": 60, "5m": 300}


@dataclass(frozen=True)
class OptionsConfig:
    client_id: str
    access_token: SecretStr

    enabled: bool = True
    paper_trading: bool = True
    paper_capital: float = 250000.0
    candle_timeframe_secs: int = 300
    max_concurrent: int = 1
    max_lots_per_trade: int = 5
    # Used only until the live scrip master resolves the leg's real,
    # currently-listed lot size (NSE revises this periodically — engine.py
    # prefers the live value the moment it's available; see dhan_client.py's
    # ContractInfo). Confirmed against a live scrip-master pull: 65 as of
    # this writing, NOT 75 — double-check nseindia.com before relying on it.
    lot_size: int = 65
    db_path: str = "data/options_ledger.db"

    # DhanHQ identity for the NIFTY 50 index (IDX_I segment) — a stable,
    # published constant, but overridable in case an account ever needs a
    # different underlying/segment.
    nifty_security_id: str = "13"
    nifty_index_segment: str = "IDX_I"
    option_exchange_segment: str = "NSE_FNO"

    # Used ONLY when a live DhanHQ margin-calculator lookup cannot be
    # completed (option security id not resolved yet, or the call fails) —
    # see dhan_client.py:margin_per_lot(). Always clearly labelled as an
    # estimate in logs/ledger, never silently treated as the real figure.
    fallback_margin_per_lot: float = 130000.0

    # Outer maintenance-loop cadence — spot re-poll, candle-flush safety net,
    # and the heartbeat log. Real-time premium ticks and exit checks run off
    # the WebSocket feed, not this loop — see engine.py's on_market_tick().
    poll_interval_secs: float = 1.0

    # Minimum gap between actual calls to DhanHQ's option-chain endpoints
    # (chain + expiry list share this — used only for spot/band resolution,
    # not premiums, which come from the WebSocket feed). DhanHQ documents
    # "1 req/3s" here, but live testing showed even correctly-3s-spaced
    # requests can still draw an HTTP 429, so the default is more
    # conservative, with an additional 15s cool-off automatically applied
    # after any 429 (see dhan_client.py).
    chain_min_interval_secs: float = 5.0


def load_options_config() -> OptionsConfig:
    """Load OPTIONS_* configuration from environment variables (.env file)."""
    load_dotenv()

    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    raw_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    if not client_id:
        raise OptionsConfigError("Missing DHAN_CLIENT_ID environment variable.")
    if not raw_token:
        raise OptionsConfigError("Missing DHAN_ACCESS_TOKEN environment variable.")
    access_token = SecretStr(raw_token)
    del raw_token

    paper_trading = os.getenv("OPTIONS_PAPER_TRADING", "true").strip().lower() == "true"
    if not paper_trading:
        raise OptionsConfigError(
            "OPTIONS_PAPER_TRADING must stay 'true' — no live order path is wired "
            "for options in this engine. Refusing to start rather than silently "
            "run as if orders were being placed when none would be."
        )

    raw_tf = os.getenv("OPTIONS_CANDLE_TIMEFRAME", "5m").strip().lower()
    if raw_tf not in _CANDLE_TIMEFRAMES:
        raise OptionsConfigError(
            f"Invalid OPTIONS_CANDLE_TIMEFRAME '{raw_tf}'. Supported: "
            f"{', '.join(sorted(_CANDLE_TIMEFRAMES))}."
        )

    max_concurrent = int(os.getenv("OPTIONS_MAX_CONCURRENT", "1"))
    if max_concurrent < 1:
        raise OptionsConfigError("OPTIONS_MAX_CONCURRENT must be >= 1.")

    max_lots = int(os.getenv("OPTIONS_MAX_LOTS_PER_TRADE", "5"))
    if max_lots < 1:
        raise OptionsConfigError("OPTIONS_MAX_LOTS_PER_TRADE must be >= 1.")

    paper_capital = float(os.getenv("OPTIONS_PAPER_CAPITAL", "250000.0"))
    if paper_capital <= 0:
        raise OptionsConfigError("OPTIONS_PAPER_CAPITAL must be positive.")

    lot_size = int(os.getenv("OPTIONS_LOT_SIZE", "65"))
    if lot_size < 1:
        raise OptionsConfigError("OPTIONS_LOT_SIZE must be >= 1.")

    chain_min_interval = float(os.getenv("OPTIONS_CHAIN_MIN_INTERVAL_SECS", "5.0"))
    if chain_min_interval <= 0:
        raise OptionsConfigError("OPTIONS_CHAIN_MIN_INTERVAL_SECS must be positive.")

    return OptionsConfig(
        client_id=client_id,
        access_token=access_token,
        enabled=os.getenv("OPTIONS_ENABLED", "true").strip().lower() == "true",
        paper_trading=paper_trading,
        paper_capital=paper_capital,
        candle_timeframe_secs=_CANDLE_TIMEFRAMES[raw_tf],
        max_concurrent=max_concurrent,
        max_lots_per_trade=max_lots,
        lot_size=lot_size,
        db_path=os.getenv("OPTIONS_DB_PATH", "data/options_ledger.db").strip(),
        nifty_security_id=os.getenv("OPTIONS_NIFTY_SECURITY_ID", "13").strip(),
        nifty_index_segment=os.getenv("OPTIONS_NIFTY_SEGMENT", "IDX_I").strip(),
        option_exchange_segment=os.getenv("OPTIONS_EXCHANGE_SEGMENT", "NSE_FNO").strip(),
        fallback_margin_per_lot=float(os.getenv("OPTIONS_FALLBACK_MARGIN_PER_LOT", "130000.0")),
        poll_interval_secs=float(os.getenv("OPTIONS_POLL_INTERVAL_SECS", "1.0")),
        chain_min_interval_secs=chain_min_interval,
    )
