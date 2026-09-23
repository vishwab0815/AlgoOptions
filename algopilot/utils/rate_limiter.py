"""
utils/rate_limiter.py — client-side token bucket for pacing outbound calls
to a rate-limited API.

DhanHQ's official Order API limit (docs.dhanhq.co, Rate Limit section) is
10 requests/second (also 250/min, 1000/hour, 7000/day). Exceeding it returns
error DH-904 ("Too many requests... Try throttling API calls").

Without this, a candle close that fires SELL signals on several symbols at
once sends all their order (and margin-check) calls in one burst — some get
rejected with DH-904 and must retry, wasting a round trip and risking the
existing 3-retry ceiling on a big enough burst. Pacing calls to just under
the documented limit removes that risk entirely without reducing how many
orders actually get placed: Dhan's 10/sec is a hard wall either way, this
just avoids finding out about it via rejections.
"""
from __future__ import annotations

import threading
import time


class RateLimiter:
    """Thread-safe token bucket. Blocking acquire() paces callers to `rate`/sec.

    Safe to call from a worker thread (e.g. inside asyncio.to_thread) — it
    only blocks that thread, never the event loop.
    """

    def __init__(self, rate: float, capacity: int | None = None) -> None:
        self._rate = rate
        self._capacity = capacity or max(1, int(rate))
        self._tokens = float(self._capacity)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            time.sleep(wait)
