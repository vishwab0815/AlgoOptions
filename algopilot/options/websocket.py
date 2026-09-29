"""
algopilot/options/websocket.py — Live WebSocket Feed Manager for Options

Handles the DhanHQ MarketFeed connection, subscribing to the active CE/PE
contracts, and pushing ticks to the OptionsEngine.

Reconnection: the installed dhanhq SDK already retries the connection loop
on its own (see its MarketFeed._run_async — it reconnects roughly once a
second while disconnected), so this module doesn't need to implement retry
logic itself. What it DOES need to do is make disconnects loud, because of
one real gap confirmed directly against the SDK's own source: a
server-initiated disconnect (DhanHQ's documented response code 50 — token
expired, invalid client, too many connections, etc. — see
dhanhq.co/docs/v2/live-market-feed/) is reported by the SDK via a bare
print() to stdout, not through Python's logging module. That means the
actual reason for a disconnect is invisible in data/options_engine.log —
it only appears on whatever console happens to be attached, if any. This
module's on_close/on_error handlers exist to at least flag loudly that this
gap exists whenever a disconnect happens, and run_options.py's startup
banner checks the access token's own expiry proactively so the single most
common cause (a lapsed 24h token) is caught before it can cause a silent
disconnect at all.

DhanHQ also documents a ping every 10s from their side, expecting a pong
within 40s or the connection is dropped — handled transparently by the
`websockets` library's default ping/pong, not something this module needs
to manage itself.
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional, Callable
from zoneinfo import ZoneInfo
from datetime import datetime, timezone

from dhanhq.marketfeed import MarketFeed
from dhanhq import DhanContext

from algopilot.options.config import OptionsConfig

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")

# How far the exchange's trade time may sit from our clock and still be
# trusted: a trade can't be from the future (small allowance for clock
# drift), and one older than a few seconds is a re-sent/stale quote.
_LTT_MAX_AHEAD_SECS = 2.0
_LTT_MAX_BEHIND_SECS = 5.0


class ClockCheck:
    """How far this machine's clock is from the exchange's.

    Candles now close at hh:mm:00.000 by the LOCAL clock, so "exactly on the
    5th minute" is only as true as that clock. Each tick carries the
    exchange's trade time (whole seconds, truncated), so arrival minus trade
    time = network delay + clock offset + the dropped fraction of a second.
    Over a few hundred ticks, the SMALLEST of those converges on network
    delay + clock offset. Near zero (a few ms) = clock is right. Negative =
    our clock is BEHIND the exchange. Large positive = clock ahead, or a slow
    network. Reported once after warm-up, then only if it goes bad.
    """
    WINDOW = 300          # ticks per estimate (~2 min with both legs live)
    BAD_BEHIND = -0.050   # clock behind the exchange by more than 50 ms
    BAD_AHEAD = 0.250     # clock ahead / network slower than 250 ms

    def __init__(self) -> None:
        self._min_lag = float("inf")
        self._n = 0
        self._reported = False

    def observe(self, ltt: Optional[str], now_utc: datetime) -> None:
        if not ltt:
            return
        try:
            h, m, s = (int(x) for x in str(ltt).split(":"))
            now_ist = now_utc.astimezone(_IST)
            lag = (now_ist - now_ist.replace(hour=h, minute=m, second=s, microsecond=0)).total_seconds()
        except (ValueError, TypeError):
            return
        if abs(lag) > _LTT_MAX_BEHIND_SECS:
            return                       # a stale re-sent quote says nothing about the clock
        self._min_lag = min(self._min_lag, lag)
        self._n += 1
        if self._n < self.WINDOW:
            return
        est_ms = self._min_lag * 1000.0
        bad = self._min_lag < self.BAD_BEHIND or self._min_lag > self.BAD_AHEAD
        if bad:
            logger.warning(
                "Clock check: this machine is %s the exchange by ~%.0f ms (incl. network). Candles "
                "close by THIS clock, so boundaries are off by that much. Resync it: "
                "w32tm /resync (Windows, as admin) or enable NTP.",
                "BEHIND" if est_ms < 0 else "ahead of", abs(est_ms),
            )
        elif not self._reported:
            logger.info(
                "Clock check vs exchange trade times: offset %+.0f ms (incl. network delay; "
                "negative = this clock is behind) — candle boundaries are on time.", est_ms,
            )
        self._reported = True
        self._min_lag, self._n = float("inf"), 0


def tick_time(ltt: Optional[str], now_utc: datetime) -> datetime:
    """The time a tick belongs to, for candle bucketing.

    BUG FIXED: every tick used to be stamped with the moment it ARRIVED here.
    A trade printed at 14:49:59.9 that reaches us 150 ms later was put in the
    14:50 candle instead of 14:45 — so the live candles' OHLC (and therefore
    the Heikin-Ashi colours) disagreed with the exchange's, with DhanHQ's own
    historical bars, and with the chart, by whatever network delay happened
    to straddle each boundary. DhanHQ's Ticker packet carries LTT, the
    exchange's own last-trade time (HH:MM:SS, IST); use it whenever it is
    plausibly current, and fall back to arrival time otherwise.
    """
    if not ltt:
        return now_utc
    try:
        h, m, s = (int(x) for x in str(ltt).split(":"))
        now_ist = now_utc.astimezone(_IST)
        traded = now_ist.replace(hour=h, minute=m, second=s, microsecond=0)
    except (ValueError, TypeError):
        return now_utc
    lag = (now_ist - traded).total_seconds()
    if -_LTT_MAX_AHEAD_SECS <= lag <= _LTT_MAX_BEHIND_SECS:
        return traded.astimezone(timezone.utc)
    return now_utc


class OptionsWebSocketManager:
    def __init__(self, config: OptionsConfig, on_tick_callback: Callable[[str, float, datetime], None], loop: asyncio.AbstractEventLoop):
        self.config = config
        self.on_tick_callback = on_tick_callback
        self.loop = loop
        
        self.client_id = config.client_id
        self.access_token = config.access_token.get_secret()
        
        self.context = DhanContext(self.client_id, self.access_token)
        
        self.feed: Optional[MarketFeed] = None
        self._subscriptions: List[tuple] = []
        self._running = False
        # Observability — how do I know the feed is actually alive, not just
        # "connected"? Ticks received and connection uptime, surfaced to
        # engine.py's heartbeat log via the tick_count property below.
        self._tick_count = 0
        self._connected_at: Optional[float] = None
        self.clock_check = ClockCheck()

    @property
    def tick_count(self) -> int:
        return self._tick_count

    def start(self, instruments: List[tuple]):
        """
        Starts the WebSocket connection in a background thread.
        instruments: List of tuples e.g. [("NSE_FNO", "56999")]
        """
        self._subscriptions = instruments
        self._running = True

        def on_message(ws, data):
            if isinstance(data, dict):
                if "security_id" in data and ("LTP" in data or "last_price" in data):
                    sec_id = str(data["security_id"])

                    if "last_price" in data:
                        price_str = data["last_price"]
                    elif "LTP" in data:
                        price_str = data["LTP"]
                    else:
                        return

                    try:
                        price = float(price_str)
                    except (ValueError, TypeError):
                        return

                    self._tick_count += 1
                    now = datetime.now(timezone.utc)
                    ts = tick_time(data.get("LTT"), now)
                    self.clock_check.observe(data.get("LTT"), now)
                    # Route to the main asyncio loop safely
                    self.loop.call_soon_threadsafe(self.on_tick_callback, sec_id, price, ts)
                elif logger.isEnabledFor(logging.DEBUG):
                    # Anything else — a market-depth/quote/OI packet, or a
                    # disconnection notice the SDK already handled itself
                    # (see server_disconnection() in dhanhq/marketfeed.py,
                    # which prints known codes directly to stdout rather
                    # than raising here) — logged only at DEBUG since it's
                    # expected traffic, not an error.
                    logger.debug("MarketFeed: unrecognised message shape: %s", data)

        def on_connect(ws):
            self._connected_at = time.monotonic()
            logger.info("Connected to DhanHQ WebSocket (%d instrument(s) subscribed).", len(self._subscriptions))

        def on_close(ws):
            if not self._running:
                return
            uptime = f"{time.monotonic() - self._connected_at:.0f}s" if self._connected_at else "unknown"
            self._connected_at = None
            logger.warning(
                "WebSocket disconnected after %s uptime, %d ticks received. "
                "The SDK reports the EXACT DhanHQ reason (token expired / invalid client / "
                "too many connections / etc, response code 50) via a direct print() to the "
                "console, not through this logger — check the terminal output above this line "
                "if one was attached. Reconnection is handled automatically by the SDK.",
                uptime, self._tick_count,
            )

        def on_error(ws, err):
            if not self._running:
                # Our own shutdown: the SDK reports the socket we just closed
                # ("sent 1000 (OK); no close frame received") as an error.
                logger.debug("MarketFeed closed during shutdown: %s", err)
                return
            logger.error("MarketFeed error: %s", err)

        # Initialize the market feed client
        self.feed = MarketFeed(
            self.context,
            instruments=self._subscriptions,
            on_connect=on_connect,
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        
        logger.info("Starting WebSocket feed for %d instrument(s)...", len(instruments))
        self.feed.start()  # Runs in a background thread

    def update_subscriptions(self, instruments: List[tuple]):
        """Update subscriptions dynamically. Unsubscribes whatever is no
        longer needed (e.g. the previous band's strikes) before adding the
        new ones, so the subscription list doesn't grow unbounded across
        every band change and every trading day the process stays up — the
        SDK's subscribe_symbols() only ever adds to its instrument set, it
        never prunes on its own (confirmed against the installed SDK
        source)."""
        if self.feed and self._running:
            stale = [i for i in self._subscriptions if i not in instruments]
            fresh = [i for i in instruments if i not in self._subscriptions]
            if stale:
                self.feed.unsubscribe_symbols(stale)
                logger.info("WebSocket unsubscribed from stale instruments: %s", stale)
            if fresh:
                self.feed.subscribe_symbols(fresh)
                logger.info("WebSocket subscribed to new instruments: %s", fresh)
            self._subscriptions = list(instruments)

    def stop(self):
        self._running = False
        if not self.feed:
            return
        # The SDK's close_connection() calls loop.run_until_complete() on ITS
        # own loop. We call stop() from inside the engine's running asyncio
        # loop (run()'s finally block), and asyncio refuses to start a loop
        # while another is running — "Cannot run the event loop while another
        # loop is running". That turned every Ctrl+C into a RuntimeError
        # traceback that buried the real shutdown, and left
        # MarketFeed.disconnect() as an un-awaited coroutine. The feed runs
        # on a daemon thread that dies with the process anyway, so a failure
        # here is cosmetic — log it and let shutdown finish cleanly.
        try:
            self.feed.close_connection()
        except RuntimeError as exc:
            logger.debug("MarketFeed close skipped (%s) — feed thread exits with the process.", exc)
        except Exception as exc:
            logger.warning("MarketFeed close failed: %s — continuing shutdown.", exc)
        logger.info("WebSocket feed stopped (%d ticks received this run).", self._tick_count)
