"""
algopilot/options/websocket.py — Zerodha Kite Connect live price feed for the option legs.

    wss://ws.kite.trade?api_key=<api_key>&access_token=<access_token>

Client -> server (JSON text frames, tokens are integer instrument_tokens):
    {"a": "subscribe",   "v": [token, ...]}
    {"a": "unsubscribe", "v": [token, ...]}
    {"a": "mode",        "v": ["full", [token, ...]]}

Server -> client:
  - BINARY frames = market data, big-endian, unsigned:
        [0:2] number of packets, then per packet [2-byte length][packet bytes]
    The packet type is its length: 8 = LTP, 28 / 32 = index quote / full,
    44 = tradable quote, 184 = tradable full. A tradable packet:
        0 token | 4 last price | 8 last qty | 12 avg price | 16 volume | 20 buy qty | 24 sell qty
        28 open | 32 high | 36 low | 40 close (prev day) | 44 last trade time (unix s) | 48 OI
        52 OI day high | 56 OI day low | 60 exchange time (unix s) | 64.. 10 depth entries x 12 bytes
    Prices are in paise for NSE / NFO / indices: divide by 100.
    A 1-byte binary frame is the heartbeat, sent every couple of seconds when idle.
  - TEXT frames = {"type": "order" | "error" | "message", "data": ...}

Only FULL mode carries the trade time, and candles are bucketed by the exchange's
trade time (see tick_time), so the legs are subscribed in full mode.

Kite forgets subscriptions when a connection drops: every (re)connect re-sends
subscribe + mode for all tokens. A connection with no frame at all for
_SILENCE_SECS (not even a heartbeat) is treated as dead and reopened.
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from datetime import datetime, timezone
from typing import Callable, List, Optional, Set
from zoneinfo import ZoneInfo

from algopilot.options.config import OptionsConfig

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
WS_URL = "wss://ws.kite.trade"

# How far the exchange's trade time may sit from our clock and still be
# trusted: a trade can't be from the future (small allowance for clock
# drift), and one older than a few seconds is a re-sent / stale snapshot.
_LTT_MAX_AHEAD_SECS = 2.0
_LTT_MAX_BEHIND_SECS = 5.0
_SILENCE_SECS = 10.0            # no frame at all (Kite heartbeats every couple of seconds when idle)

_U32 = struct.Struct(">I")
_U16 = struct.Struct(">H")
SEG_INDICES, SEG_CDS, SEG_BCD, SEG_NCO = 9, 3, 6, 12


def _divisor(token: int) -> float:
    seg = token & 0xFF
    if seg == SEG_CDS:
        return 10_000_000.0
    if seg in (SEG_BCD, SEG_NCO):
        return 10_000.0
    return 100.0


def split_packets(buf: bytes) -> List[bytes]:
    """The packets of one binary frame. A heartbeat (shorter than 2 bytes) has none;
    a truncated frame stops at the last complete packet."""
    if len(buf) < 2:
        return []
    n = _U16.unpack_from(buf, 0)[0]
    out, j = [], 2
    for _ in range(n):
        if j + 2 > len(buf):
            break
        plen = _U16.unpack_from(buf, j)[0]
        if j + 2 + plen > len(buf):
            break
        out.append(buf[j + 2:j + 2 + plen])
        j += 2 + plen
    return out


def parse_packet(p: bytes) -> Optional[dict]:
    """One packet -> {token, last_price, ...}; None for an unknown length."""
    L = len(p)
    if L < 8:
        return None
    token = _U32.unpack_from(p, 0)[0]
    div = _divisor(token)
    tick = {"token": token, "last_price": _U32.unpack_from(p, 4)[0] / div,
            "tradable": (token & 0xFF) != SEG_INDICES}
    if L == 8:
        tick["mode"] = "ltp"
        return tick
    if L in (28, 32):                                # index: high, low, open, close
        tick.update(mode="quote" if L == 28 else "full",
                    high=_U32.unpack_from(p, 8)[0] / div, low=_U32.unpack_from(p, 12)[0] / div,
                    open=_U32.unpack_from(p, 16)[0] / div, close=_U32.unpack_from(p, 20)[0] / div)
        if L == 32:
            tick["exchange_ts"] = _U32.unpack_from(p, 28)[0] or None
        return tick
    if L in (44, 184):                               # tradable: open, high, low, close
        tick.update(mode="quote" if L == 44 else "full",
                    last_qty=_U32.unpack_from(p, 8)[0], avg_price=_U32.unpack_from(p, 12)[0] / div,
                    volume=_U32.unpack_from(p, 16)[0], buy_qty=_U32.unpack_from(p, 20)[0],
                    sell_qty=_U32.unpack_from(p, 24)[0],
                    open=_U32.unpack_from(p, 28)[0] / div, high=_U32.unpack_from(p, 32)[0] / div,
                    low=_U32.unpack_from(p, 36)[0] / div, close=_U32.unpack_from(p, 40)[0] / div)
        if L == 184:
            tick.update(last_trade_time=_U32.unpack_from(p, 44)[0] or None, oi=_U32.unpack_from(p, 48)[0],
                        oi_day_high=_U32.unpack_from(p, 52)[0], oi_day_low=_U32.unpack_from(p, 56)[0],
                        exchange_ts=_U32.unpack_from(p, 60)[0] or None)
            depth = {"buy": [], "sell": []}
            for i, off in enumerate(range(64, 184, 12)):
                depth["sell" if i >= 5 else "buy"].append({
                    "quantity": _U32.unpack_from(p, off)[0], "price": _U32.unpack_from(p, off + 4)[0] / div,
                    "orders": _U16.unpack_from(p, off + 8)[0]})
            tick["depth"] = depth
        return tick
    return None


def parse_binary(buf: bytes) -> List[dict]:
    return [t for t in (parse_packet(p) for p in split_packets(buf)) if t is not None]


class ClockCheck:
    """How far this machine's clock is from the exchange's.

    Candles close at hh:mm:00.000 by the LOCAL clock, so "exactly on the 5th
    minute" is only as true as that clock. Each full-mode tick carries the
    exchange's last trade time (whole seconds), so arrival minus trade time =
    network delay + clock offset + the dropped fraction of a second. Over a few
    hundred ticks the SMALLEST of those converges on network delay + clock
    offset. Reported once after warm-up, then only if it goes bad."""
    WINDOW = 300
    BAD_BEHIND = -0.050
    BAD_AHEAD = 0.250

    def __init__(self) -> None:
        self._min_lag = float("inf")
        self._n = 0
        self._reported = False

    def observe(self, ltt: Optional[int], now_utc: datetime) -> None:
        if not ltt:
            return
        lag = now_utc.timestamp() - float(ltt)
        if abs(lag) > _LTT_MAX_BEHIND_SECS:
            return                       # a stale snapshot says nothing about the clock
        self._min_lag = min(self._min_lag, lag)
        self._n += 1
        if self._n < self.WINDOW:
            return
        est_ms = self._min_lag * 1000.0
        if self._min_lag < self.BAD_BEHIND or self._min_lag > self.BAD_AHEAD:
            logger.warning(
                "Clock check: this machine is %s the exchange by ~%.0f ms (incl. network). Candles close by "
                "THIS clock, so boundaries are off by that much. Resync it: w32tm /resync (Windows, as admin).",
                "BEHIND" if est_ms < 0 else "ahead of", abs(est_ms))
        elif not self._reported:
            logger.info("Clock check vs exchange trade times: offset %+.0f ms (incl. network delay; negative = "
                        "this clock is behind) — candle boundaries are on time.", est_ms)
        self._reported = True
        self._min_lag, self._n = float("inf"), 0


def tick_time(ltt: Optional[int], now_utc: datetime) -> datetime:
    """The time a tick belongs to, for candle bucketing: the exchange's last trade
    time (unix seconds) when it is plausibly current, otherwise arrival time. A
    trade printed at 14:49:59.9 that reaches us 150 ms later belongs to the 14:45
    candle, not 14:50."""
    if not ltt:
        return now_utc
    lag = now_utc.timestamp() - float(ltt)
    if -_LTT_MAX_AHEAD_SECS <= lag <= _LTT_MAX_BEHIND_SECS:
        return datetime.fromtimestamp(float(ltt), tz=timezone.utc)
    return now_utc


class OptionsWebSocketManager:
    """Streams the option legs' prices into the engine (on_tick_callback(token_str, price, ts)).

    Runs as a task on the engine's own asyncio loop; reconnects by itself."""

    def __init__(self, config: OptionsConfig, on_tick_callback: Callable[[str, float, datetime], None],
                 loop: asyncio.AbstractEventLoop):
        self.config = config
        self.on_tick_callback = on_tick_callback
        self.loop = loop
        self._tokens: Set[int] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws = None
        self._tick_count = 0
        self._connected_at: Optional[float] = None
        self._last_rx = 0.0
        self.clock_check = ClockCheck()
        self.on_text: Optional[Callable[[dict], None]] = None     # order updates, if anyone listens

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def connected(self) -> bool:
        return self._ws is not None

    @staticmethod
    def _tokens_of(instruments: List[tuple]) -> Set[int]:
        out = set()
        for item in instruments:
            sid = item[-1] if isinstance(item, (tuple, list)) else item
            try:
                out.add(int(sid))
            except (TypeError, ValueError):
                logger.warning("WebSocket: %r is not a Kite instrument_token — not subscribed.", sid)
        return out

    def start(self, instruments: List[tuple]) -> None:
        """instruments: [(segment, instrument_token), ...] — only the token is used."""
        self._tokens = self._tokens_of(instruments)
        self._running = True
        self._task = asyncio.ensure_future(self._run(), loop=self.loop)
        logger.info("Starting Kite WebSocket feed for %d instrument(s)...", len(self._tokens))

    def update_subscriptions(self, instruments: List[tuple]) -> None:
        new = self._tokens_of(instruments)
        stale, fresh = self._tokens - new, new - self._tokens
        self._tokens = new
        if self._ws is not None and (stale or fresh):
            asyncio.ensure_future(self._send_changes(stale, fresh), loop=self.loop)
        if stale:
            logger.info("WebSocket unsubscribed from stale instruments: %s", sorted(stale))
        if fresh:
            logger.info("WebSocket subscribed to new instruments: %s", sorted(fresh))

    async def _send_changes(self, stale: Set[int], fresh: Set[int]) -> None:
        ws = self._ws
        if ws is None:
            return                          # the next (re)connect subscribes everything
        try:
            if stale:
                await ws.send(json.dumps({"a": "unsubscribe", "v": sorted(stale)}))
            if fresh:
                await ws.send(json.dumps({"a": "subscribe", "v": sorted(fresh)}))
                await ws.send(json.dumps({"a": "mode", "v": ["full", sorted(fresh)]}))
        except Exception as exc:
            logger.warning("WebSocket: subscription change not sent (%s) — re-sent on reconnect.", exc)

    async def _subscribe_all(self, ws) -> None:
        if self._tokens:
            toks = sorted(self._tokens)
            await ws.send(json.dumps({"a": "subscribe", "v": toks}))
            await ws.send(json.dumps({"a": "mode", "v": ["full", toks]}))

    async def _run(self) -> None:
        import websockets
        from websockets.exceptions import InvalidStatus
        url = f"{WS_URL}?api_key={self.config.api_key}&access_token={self.config.access_token.get_secret()}"
        delay = 1.0
        while self._running:
            try:
                async with websockets.connect(url, additional_headers={"X-Kite-Version": "3"}, open_timeout=10,
                                              close_timeout=2, ping_interval=None, max_size=2 ** 22) as ws:
                    self._ws, self._connected_at, self._last_rx, delay = ws, time.monotonic(), time.monotonic(), 1.0
                    await self._subscribe_all(ws)
                    logger.info("Connected to Kite WebSocket (%d instrument(s) subscribed, full mode).",
                                len(self._tokens))
                    watchdog = asyncio.ensure_future(self._watchdog(ws))
                    try:
                        async for raw in ws:
                            self._last_rx = time.monotonic()
                            if isinstance(raw, (bytes, bytearray)):
                                self._on_binary(bytes(raw))
                            else:
                                self._on_text_frame(raw)
                    finally:
                        watchdog.cancel()
            except asyncio.CancelledError:
                raise
            except InvalidStatus as exc:
                code = getattr(getattr(exc, "response", None), "status_code", "?")
                logger.error("Kite WebSocket refused the connection (HTTP %s) — the access token has expired or "
                             "is invalid. Log in again: python scripts/kite_login.py", code)
                delay = 30.0
            except Exception as exc:
                if self._running:
                    uptime = (f"{time.monotonic() - self._connected_at:.0f}s" if self._connected_at else "-")
                    logger.warning("Kite WebSocket dropped (%s: %s) after %s, %d ticks so far — reconnecting in "
                                   "%.0fs and re-subscribing.", type(exc).__name__, str(exc)[:120], uptime,
                                   self._tick_count, delay)
            finally:
                self._ws, self._connected_at = None, None
            if self._running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _watchdog(self, ws) -> None:
        while True:
            await asyncio.sleep(2.0)
            if time.monotonic() - self._last_rx > _SILENCE_SECS:
                logger.warning("Kite WebSocket silent for %.0fs (not even a heartbeat) — reconnecting.",
                               time.monotonic() - self._last_rx)
                try:
                    await asyncio.wait_for(ws.close(), 2.0)
                except Exception:
                    ws.transport.abort()
                return

    def _on_binary(self, raw: bytes) -> None:
        if len(raw) < 2:
            return                                   # heartbeat
        now = datetime.now(timezone.utc)
        for t in parse_binary(raw):
            if not t["tradable"] or t["token"] not in self._tokens or t["last_price"] <= 0:
                continue
            self._tick_count += 1
            ltt = t.get("last_trade_time")
            self.clock_check.observe(ltt, now)
            try:
                self.on_tick_callback(str(t["token"]), float(t["last_price"]), tick_time(ltt, now))
            except Exception:
                logger.exception("tick handler failed for token %s", t["token"])

    def _on_text_frame(self, raw) -> None:
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        kind = str(msg.get("type", ""))
        if kind == "error":
            logger.error("Kite WebSocket error message: %s", msg.get("data"))
        elif kind == "message":
            logger.info("Kite WebSocket message: %s", msg.get("data"))
        elif kind == "order" and self.on_text is not None:
            self.on_text(msg)

    def stop(self) -> None:
        self._running = False
        ws, task = self._ws, self._task
        if ws is not None:
            asyncio.ensure_future(ws.close(), loop=self.loop)
        if task is not None:
            task.cancel()
        logger.info("Kite WebSocket feed stopped (%d ticks received this run).", self._tick_count)
