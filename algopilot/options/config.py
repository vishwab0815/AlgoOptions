"""
algopilot/options/config.py — configuration for the NIFTY options engine.

OPTIONS_PAPER_TRADING=true  -> paper: live market data, simulated fills, no order placed.
OPTIONS_PAPER_TRADING=false -> LIVE: real orders on DhanHQ (see broker.py), real money.
Live mode always runs with a daily loss limit (OPTIONS_MAX_DAILY_LOSS, default
Rs 3,000) after which no new trade is opened.

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


def token_client_id(raw_token: str) -> Optional[str]:
    """The Dhan client id a JWT access token was issued for (its
    `dhanClientId` claim), or None if it isn't a JWT we recognise. A token
    only works together with that exact client id — see load_options_config."""
    try:
        payload = raw_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return str(json.loads(base64.urlsafe_b64decode(payload))["dhanClientId"])
    except Exception:
        return None


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

# Live-mode daily loss limit when OPTIONS_MAX_DAILY_LOSS isn't set (Rs).
_LIVE_DEFAULT_DAILY_LOSS = 3000.0


def _hhmm(raw: str) -> str:
    """Validate an HH:MM time and return it zero-padded ("9:30" -> "09:30"),
    so string comparison against other HH:MM values is correct."""
    try:
        return datetime.strptime(raw.strip(), "%H:%M").strftime("%H:%M")
    except ValueError:
        raise OptionsConfigError(f"Invalid time '{raw}' — expected HH:MM, e.g. 09:30.")


@dataclass(frozen=True)
class OptionsConfig:
    client_id: str
    access_token: SecretStr

    enabled: bool = True
    paper_trading: bool = True

    # ── LIVE ORDERS (ignored in paper mode) ─────────────────────────────────
    # Hard cap on lots per live trade, whatever the capital would allow.
    live_max_lots: int = 1
    # Stop opening new trades once the day's NET loss reaches this (Rs).
    # 0 = off (paper only); live mode refuses to start without it.
    max_daily_loss: float = 0.0
    # Entry/exit orders: MARKET, or LIMIT at the last price +/- a buffer
    # (a marketable limit — fills now, but never worse than the buffer).
    live_order_type: str = "MARKET"
    live_limit_buffer_pct: float = 3.0
    # The protective stop resting at the exchange is a STOP-LIMIT (NSE does
    # not allow stop-market on options): trigger = the engine's stop level,
    # limit = trigger + this %, so it still fills on a fast move.
    stop_limit_buffer_pct: float = 10.0
    # If price is past the stop and that exchange order still hasn't filled
    # after this long (it gapped beyond its limit), the engine cancels it and
    # buys back at market itself.
    stop_escalate_secs: float = 3.0
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

    # Candles close at EXACTLY the timeframe boundary (hh:mm:00.000). A
    # positive value waits that many ms after the boundary so a trade printed
    # in the final milliseconds, still in flight on the network, is included
    # — at the cost of deciding that much later. 0 = decide at the boundary.
    candle_close_grace_ms: int = 0
    # Decide each candle on DhanHQ's own 5-minute bar (what the chart shows)
    # instead of the one built from live-feed ticks. The feed sends snapshots,
    # not every trade, so it misses highs/lows (30-Sep, CE 10:30-10:35: feed
    # low 116.75, exchange low 115.35). DhanHQ published each bar ~45 ms after
    # the close on the VM; if it's later than the wait below, the live-feed
    # candle is used for that close.
    official_candles: bool = True
    official_candle_wait_ms: int = 1500

    # Profit lock (an extra exit; the Heikin-Ashi stop is unchanged): once the
    # premium is `start` points below the entry, buy back if it comes back to
    # that level; every further `step` points down moves the lock one step
    # behind (sold 123: touch 113 -> lock 113; 110 -> 113; 107 -> 110; ...).
    # Entry (version 2). "break": when candle 2 (RED after GREEN) closes, a
    # SELL is parked at the exchange at candle 2's HA Low minus
    # entry_offset_pts; it fills the moment candle 3's price reaches it, and is
    # cancelled if candle 3 closes first. "close": the original entry — sell
    # at candle 3's close when it broke the level.
    entry_mode: str = "break"
    entry_offset_pts: float = 1.0
    # The parked sell is a stop-limit: once triggered it may fill down to this
    # % below the sell price (so a fast drop still fills).
    entry_limit_buffer_pct: float = 3.0

    profit_lock: bool = True
    profit_lock_start_pts: float = 10.0
    profit_lock_step_pts: float = 3.0

    # No new entries before this IST time (HH:MM). 09:15 = from the open
    # (the pattern needs 3 candles, so the first possible sell is 09:25-09:30).
    entry_start: str = "09:15"
    # On expiry day, trade next week's contract instead of the one settling
    # at 15:30 today (0 DTE). Off by default = trade the nearest expiry.
    roll_on_expiry_day: bool = False
    # Stop the engine by itself once the market has closed (HH:MM IST, and only
    # when no position is open). "" = run until stopped by hand.
    auto_stop_at: str = "15:31"

    # Cost model applied to every paper fill — see charges.py. Defaults are
    # ESTIMATES of published Indian F&O rates and change with budgets and
    # exchange circulars; verify against a real Dhan contract note. Without
    # these the engine reports frictionless P&L, which at 1 lot is wrong by
    # roughly Rs 50 a round trip — enough to turn small paper wins into real
    # losses. Set OPTIONS_APPLY_CHARGES=false to see gross numbers instead.
    apply_charges: bool = True
    brokerage_per_order: float = 20.0
    stt_pct: float = 0.10
    exchange_txn_pct: float = 0.0495
    sebi_pct: float = 0.0001
    stamp_duty_pct: float = 0.003
    gst_pct: float = 18.0

    # Minimum gap between actual calls to DhanHQ's option-chain endpoints
    # (chain + expiry list share this — used only for spot/band resolution,
    # not premiums, which come from the WebSocket feed). DhanHQ documents
    # "1 req/3s" here, but live testing showed even correctly-3s-spaced
    # requests can still draw an HTTP 429, so the default is more
    # conservative, with an additional 15s cool-off automatically applied
    # after any 429 (see dhan_client.py).
    chain_min_interval_secs: float = 5.0


def _entry_mode(raw: str) -> str:
    mode = raw.strip().lower()
    if mode not in ("break", "close"):
        raise OptionsConfigError(f"OPTIONS_ENTRY_MODE={raw!r} — use 'break' or 'close'.")
    return mode


def _positive(raw: str, name: str) -> float:
    try:
        v = float(raw)
    except ValueError:
        raise OptionsConfigError(f"{name}={raw!r} is not a number.")
    if v <= 0:
        raise OptionsConfigError(f"{name} must be greater than 0 (got {raw}).")
    return v


def load_options_config() -> OptionsConfig:
    """Load OPTIONS_* configuration from environment variables (.env file)."""
    load_dotenv()

    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    raw_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    if not client_id:
        raise OptionsConfigError("Missing DHAN_CLIENT_ID environment variable.")
    if not raw_token:
        raise OptionsConfigError("Missing DHAN_ACCESS_TOKEN environment variable.")
    token_owner = token_client_id(raw_token)
    if token_owner is not None and token_owner != client_id:
        # Every DhanHQ call sends both; a token issued for another account is
        # rejected with a bare HTTP 401 that reads exactly like an expired
        # token. Catch it here, by name, before any network call.
        raise OptionsConfigError(
            f"DHAN_ACCESS_TOKEN was issued for Dhan client id {token_owner}, but "
            f"DHAN_CLIENT_ID is {client_id}. They must be the same account — set "
            f"DHAN_CLIENT_ID={token_owner}, or use a token generated for {client_id}."
        )
    access_token = SecretStr(raw_token)
    del raw_token

    paper_trading = os.getenv("OPTIONS_PAPER_TRADING", "true").strip().lower() == "true"
    live_order_type = os.getenv("OPTIONS_LIVE_ORDER_TYPE", "MARKET").strip().upper()
    if live_order_type not in ("MARKET", "LIMIT"):
        raise OptionsConfigError("OPTIONS_LIVE_ORDER_TYPE must be MARKET or LIMIT.")
    raw_loss = os.getenv("OPTIONS_MAX_DAILY_LOSS", "").strip()
    max_daily_loss = float(raw_loss) if raw_loss else (0.0 if paper_trading else _LIVE_DEFAULT_DAILY_LOSS)
    if not paper_trading and max_daily_loss <= 0:
        raise OptionsConfigError(
            "OPTIONS_MAX_DAILY_LOSS must be above 0 in live mode (Rs; no new trades after the day's "
            "net loss reaches it). Remove the line to use the default Rs 3,000."
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
        live_max_lots=max(1, int(os.getenv("OPTIONS_LIVE_MAX_LOTS", "1"))),
        max_daily_loss=max_daily_loss,
        live_order_type=live_order_type,
        live_limit_buffer_pct=float(os.getenv("OPTIONS_LIVE_LIMIT_BUFFER_PCT", "3.0")),
        stop_limit_buffer_pct=float(os.getenv("OPTIONS_STOP_LIMIT_BUFFER_PCT", "10.0")),
        stop_escalate_secs=float(os.getenv("OPTIONS_STOP_ESCALATE_SECS", "3.0")),
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
        candle_close_grace_ms=max(0, int(os.getenv("OPTIONS_CANDLE_CLOSE_GRACE_MS", "0"))),
        official_candles=os.getenv("OPTIONS_OFFICIAL_CANDLES", "true").strip().lower() == "true",
        official_candle_wait_ms=max(0, int(os.getenv("OPTIONS_OFFICIAL_CANDLE_WAIT_MS", "1500"))),
        entry_mode=_entry_mode(os.getenv("OPTIONS_ENTRY_MODE", "break")),
        entry_offset_pts=max(0.0, float(os.getenv("OPTIONS_ENTRY_OFFSET_POINTS", "1"))),
        entry_limit_buffer_pct=max(0.0, float(os.getenv("OPTIONS_ENTRY_LIMIT_BUFFER_PCT", "3"))),
        profit_lock=os.getenv("OPTIONS_PROFIT_LOCK", "true").strip().lower() == "true",
        profit_lock_start_pts=_positive(os.getenv("OPTIONS_PROFIT_LOCK_START_POINTS", "10"),
                                        "OPTIONS_PROFIT_LOCK_START_POINTS"),
        profit_lock_step_pts=max(0.0, float(os.getenv("OPTIONS_PROFIT_LOCK_STEP_POINTS", "3"))),
        entry_start=_hhmm(os.getenv("OPTIONS_ENTRY_START", "09:15")),
        roll_on_expiry_day=os.getenv("OPTIONS_ROLL_ON_EXPIRY_DAY", "false").strip().lower() == "true",
        auto_stop_at=(_hhmm(os.getenv("OPTIONS_AUTO_STOP_AT", "15:31"))
                      if os.getenv("OPTIONS_AUTO_STOP_AT", "15:31").strip() else ""),
        apply_charges=os.getenv("OPTIONS_APPLY_CHARGES", "true").strip().lower() == "true",
        brokerage_per_order=float(os.getenv("OPTIONS_BROKERAGE_PER_ORDER", "20.0")),
        stt_pct=float(os.getenv("OPTIONS_STT_PCT", "0.10")),
        exchange_txn_pct=float(os.getenv("OPTIONS_EXCHANGE_TXN_PCT", "0.0495")),
        sebi_pct=float(os.getenv("OPTIONS_SEBI_PCT", "0.0001")),
        stamp_duty_pct=float(os.getenv("OPTIONS_STAMP_DUTY_PCT", "0.003")),
        gst_pct=float(os.getenv("OPTIONS_GST_PCT", "18.0")),
        poll_interval_secs=float(os.getenv("OPTIONS_POLL_INTERVAL_SECS", "1.0")),
        chain_min_interval_secs=chain_min_interval,
    )
