"""
utils/market_hours.py — NSE/BSE market hours guard (IST timezone).

NSE/BSE trading hours: 09:15 AM – 03:30 PM IST, Monday–Friday.
We enforce a 5-minute buffer before close (no new orders after 03:25 PM)
to avoid end-of-day volatility and partial fills.

Pre-open session (09:00–09:15) is deliberately excluded — prices are not
reliable during the call auction phase.

New ENTRIES are additionally gated out of two narrower windows inside the
main session: 09:15-09:30 (post-open settle buffer) and 15:10-15:30 (our
own pre-close EOD square-off buffer, ahead of Dhan's own ~15:18-15:19 RMS
auto square-off — see should_square_off_now()). EXITS are never gated by
either of these — an open position must always be closeable.

Public API:
    is_market_open()          → bool
    get_market_status()       → MarketStatus (details + reason)
    assert_market_open()      → raises MarketClosedError if not open
    assert_algo_trading_allowed() → raises MarketClosedError outside the entry window
    should_square_off_now()   → bool — True when open positions should be force-closed
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from zoneinfo import ZoneInfo

# Indian Standard Time
_IST = ZoneInfo("Asia/Kolkata")

# NSE/BSE regular session window (inclusive on both ends)
_MARKET_OPEN = time(9, 15)   # 09:15 IST
_MARKET_CLOSE = time(15, 25)  # 03:25 IST (5-min buffer before actual 03:30 close)

# The algorithm deliberately holds off trading for the first 15 minutes after
# the exchange opens — 09:15-09:30 is typically the most volatile/gappy part
# of the session (overnight news, opening auctions settling). The feed still
# connects and the dashboard still shows live prices during this window; only
# entries are gated.
_ALGO_START_TIME = time(9, 30)  # 09:30 IST

# DhanHQ's own Risk Management System (RMS) starts force-closing any open
# INTRADAY position from 03:18-03:19 PM IST for NSE equity, at whatever price
# it can get, plus an auto-square-off charge (₹20+GST) — confirmed directly
# from Dhan's support docs (see should_square_off_now() below). New intraday
# orders also stop being accepted from that same point. Our own engine
# deliberately closes any open position a few minutes BEFORE that — at a
# price we choose via our own market order, not Dhan's RMS — and stops
# opening brand-new positions from the same cutoff, so nothing we open this
# late ever gets caught by our own square-off on its very next tick.
_EOD_SQUAREOFF_TIME = time(15, 10)  # default; the engine sets it from OPTIONS_SQUAREOFF_AT


def set_squareoff_time(hhmm: str) -> None:
    """The square-off time (HH:MM IST): from then on no new entries, and every
    open position is bought back. Set once by the engine from the config."""
    global _EOD_SQUAREOFF_TIME
    h, m = (int(x) for x in hhmm.split(":"))
    _EOD_SQUAREOFF_TIME = time(h, m)


def squareoff_time() -> str:
    return _EOD_SQUAREOFF_TIME.strftime("%H:%M")

# Weekdays: Monday=0 … Friday=4
_TRADING_DAYS = {0, 1, 2, 3, 4}


class MarketState(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    PRE_OPEN = "PRE_OPEN"   # 09:00 – 09:14 (call auction, unreliable prices)
    POST_CLOSE = "POST_CLOSE"  # 03:25 – 03:30 (buffer, no new orders)
    WEEKEND = "WEEKEND"


@dataclass(frozen=True)
class MarketStatus:
    state: MarketState
    ist_time: str           # Human-readable current IST time
    reason: str             # Explanation for the current state
    is_trading_allowed: bool  # True only when state == OPEN
    # True only in the 09:30-square-off (15:10) IST window — past the post-open settle
    # buffer AND before the pre-close EOD square-off window. New entries are
    # gated on this; existing positions are never gated on it (an exit must
    # always be allowed regardless of this flag).
    algo_trading_allowed: bool = False
    eod_squareoff_due: bool = False  # True from the square-off time (15:10) — see should_square_off_now()


class MarketClosedError(RuntimeError):
    """Raised when an order is attempted outside market hours."""


def _now_ist() -> datetime:
    """Return current time in IST timezone."""
    return datetime.now(tz=_IST)


def get_market_status(now: datetime | None = None) -> MarketStatus:
    """
    Return a detailed MarketStatus for the given datetime (defaults to now).

    Args:
        now: Override the current time (useful for testing). Must be timezone-aware.
    """
    if now is None:
        now = _now_ist()
    else:
        # Normalise to IST regardless of input timezone
        now = now.astimezone(_IST)

    current_time = now.time()
    weekday = now.weekday()
    time_str = now.strftime("%H:%M:%S IST, %A %d-%b-%Y")

    # ── Weekend check ─────────────────────────────────────────────────────────
    if weekday not in _TRADING_DAYS:
        return MarketStatus(
            state=MarketState.WEEKEND,
            ist_time=time_str,
            reason="Weekend — markets are closed on Saturday and Sunday.",
            is_trading_allowed=False,
        )

    # ── Pre-open session (09:00 – 09:14) ─────────────────────────────────────
    if time(9, 0) <= current_time < _MARKET_OPEN:
        return MarketStatus(
            state=MarketState.PRE_OPEN,
            ist_time=time_str,
            reason="Pre-open call auction session (09:00–09:15). Prices are unreliable. Orders not allowed.",
            is_trading_allowed=False,
        )

    # ── Regular trading session (09:15 – 15:25) ───────────────────────────────
    if _MARKET_OPEN <= current_time <= _MARKET_CLOSE:
        squareoff_due = current_time >= _EOD_SQUAREOFF_TIME
        algo_allowed = (current_time >= _ALGO_START_TIME) and not squareoff_due
        if squareoff_due:
            reason = (
                f"EOD square-off window (from {squareoff_time()} IST) — no new entries; "
                "any open position is being closed before Dhan's own 15:18-15:19 RMS cutoff."
            )
        elif algo_allowed:
            reason = "NSE/BSE regular trading session is active."
        else:
            reason = (
                "Market just opened — algorithm holds off entries until 09:30 IST "
                "(post-open volatility buffer). Live prices are already streaming."
            )
        return MarketStatus(
            state=MarketState.OPEN,
            ist_time=time_str,
            reason=reason,
            is_trading_allowed=True,
            algo_trading_allowed=algo_allowed,
            eod_squareoff_due=squareoff_due,
        )

    # ── Post-close buffer (15:25 – 15:30) ────────────────────────────────────
    if _MARKET_CLOSE < current_time <= time(15, 30):
        return MarketStatus(
            state=MarketState.POST_CLOSE,
            ist_time=time_str,
            reason="Market closing buffer (15:25–15:30). No new orders allowed to avoid partial fills.",
            is_trading_allowed=False,
            eod_squareoff_due=True,  # still catch a straggler position right up to real close
        )

    # ── Market closed (before 09:00 or after 15:30) ───────────────────────────
    return MarketStatus(
        state=MarketState.CLOSED,
        ist_time=time_str,
        reason="Market is closed. Opens at 09:15 IST on the next trading day.",
        is_trading_allowed=False,
    )


def is_market_open(now: datetime | None = None) -> bool:
    """Return True only if NSE/BSE regular trading session is active."""
    return get_market_status(now).is_trading_allowed


def should_square_off_now(now: datetime | None = None) -> bool:
    """
    True from the square-off time (15:10 IST, our own hard cutoff) through real market close — the
    window in which any still-open position should be force-closed by US,
    at a price we choose, rather than left for DhanHQ's RMS to square off.

    Confirmed directly from Dhan's own support docs (dhan.co/support —
    "What are the Dhan Intraday auto square-off timings?" and "What is
    intraday square-off?"): for NSE equity intraday (MIS) positions, Dhan's
    RMS begins automatically closing anything still open from 15:18-15:19
    IST "at the best available price", new intraday orders stop being
    accepted from that same point, and a broker auto-square-off charge
    (₹20 + GST) applies to whatever it closes. Squaring off ourselves a few
    minutes earlier, at a price of our own choosing via a normal market
    order, avoids both the fee and Dhan's less predictable execution price.

    Computed directly rather than via get_market_status() on purpose: this is
    called on EVERY tick for every open position, and get_market_status()
    builds a MarketStatus plus a strftime()'d human-readable timestamp that
    this caller throws away. The comparisons below are the whole decision.
    """
    ist = _now_ist() if now is None else now.astimezone(_IST)
    if ist.weekday() not in _TRADING_DAYS:
        return False
    current_time = ist.time()
    return _EOD_SQUAREOFF_TIME <= current_time <= time(15, 30)


def is_within_trading_session(ts: datetime) -> bool:
    """
    True if *ts* falls within the full NSE/BSE cash session (09:15-15:30 IST)
    on a trading weekday.

    This is intentionally wider than get_market_status()'s order-blocking
    window (which cuts off 5 minutes early at 15:25) — it answers "did the
    exchange have a live tick feed at this instant", which is what candle
    gap-filling needs, not "are new orders currently allowed".
    """
    ist = ts.astimezone(_IST)
    if ist.weekday() not in _TRADING_DAYS:
        return False
    minutes = ist.hour * 60 + ist.minute
    return (9 * 60 + 15) <= minutes < (15 * 60 + 30)


def assert_market_open(now: datetime | None = None) -> None:
    """
    Raise MarketClosedError if trading is not currently allowed.
    Call this as the FIRST check inside any order execution path.
    """
    status = get_market_status(now)
    if not status.is_trading_allowed:
        raise MarketClosedError(
            f"Order blocked — market is {status.state.value}. "
            f"Reason: {status.reason} | Current time: {status.ist_time}"
        )


def assert_algo_trading_allowed(now: datetime | None = None) -> None:
    """
    Raise MarketClosedError if the market is closed, we're still inside the
    post-open settle window (09:15-09:30 IST), or we're inside the pre-close
    EOD square-off window (from the square-off time — see should_square_off_now()).
    Call this alongside assert_market_open() before executing any NEW ENTRY
    — it does not replace it, and it must never be used to gate an exit;
    closing an existing position has to remain allowed at any time.
    """
    status = get_market_status(now)
    if not status.is_trading_allowed or not status.algo_trading_allowed:
        raise MarketClosedError(
            f"Order blocked — {status.reason} | Current time: {status.ist_time}"
        )
