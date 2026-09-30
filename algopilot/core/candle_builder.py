from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from ..utils.market_hours import is_within_trading_session

logger = logging.getLogger(__name__)


@dataclass
class Candle:
    symbol: str
    exchange_segment: int
    security_id: str
    start_ts: datetime
    end_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    synthetic: bool = False

    def as_dict(self) -> Dict[str, float]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "synthetic": float(self.synthetic),
        }


class CandleBuilder:
    """
    Aggregates raw ticks into fixed-timeframe OHLCV candles.

    Gap minutes between ticks are filled automatically with synthetic
    flat candles that carry the last known close price, keeping the
    Heikin-Ashi engine's rolling DataFrame gapless.
    """

    def __init__(
        self,
        symbol: str,
        exchange_segment: int,
        security_id: str,
        timeframe_seconds: int = 60,
    ) -> None:
        self.symbol = symbol
        self.exchange_segment = exchange_segment
        self.security_id = security_id
        self.timeframe_seconds = timeframe_seconds

        self._current_start: Optional[datetime] = None
        self._current: Optional[Dict[str, float]] = None
        self._last_close_price: Optional[float] = None
        # Start of the last bucket handed out (real or synthetic). BUG FIXED
        # (30-Sep live): the candle clock closes a candle AT the boundary,
        # which leaves no candle in progress; a tick stamped just BEFORE the
        # boundary that arrived just after it then re-opened the old bucket,
        # and the next tick closed it a SECOND time — CE 12:50-12:55 and
        # 13:30-13:35 were evaluated twice, adding a bogus Heikin-Ashi row.
        self._last_closed_start: Optional[datetime] = None
        # Ticks dropped for arriving with a timestamp earlier than the candle
        # already in progress (see the else branch in update_from_tick) —
        # exposed for visibility/telemetry, not currently read anywhere else.
        self.dropped_out_of_order_ticks = 0

        # NOTE: a `buffer` (bounded deque of every closed Candle, `rolling_
        # window` sized) used to live here. Removed — it was written on every
        # candle close and read nowhere in the codebase at all (confirmed by
        # grep across the whole project): up to 3,000 retained Candle objects
        # per symbol, times every configured symbol, for zero consumers.

    @staticmethod
    def _to_utc(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _floor_to_timeframe(self, ts: datetime) -> datetime:
        ts_utc = self._to_utc(ts)
        floored_epoch = int(ts_utc.timestamp() // self.timeframe_seconds) * self.timeframe_seconds
        return datetime.fromtimestamp(floored_epoch, tz=timezone.utc)

    def _new_current_candle(self, candle_start: datetime, price: float, volume: float) -> None:
        self._current_start = candle_start
        self._current = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
        }

    def _close_current(self, synthetic: bool = False) -> Optional[Candle]:
        if self._current_start is None or self._current is None:
            return None

        candle = Candle(
            symbol=self.symbol,
            exchange_segment=self.exchange_segment,
            security_id=self.security_id,
            start_ts=self._current_start,
            end_ts=self._current_start + timedelta(seconds=self.timeframe_seconds),
            open=float(self._current["open"]),
            high=float(self._current["high"]),
            low=float(self._current["low"]),
            close=float(self._current["close"]),
            volume=float(self._current["volume"]),
            synthetic=synthetic,
        )

        self._last_close_price = candle.close
        self._last_closed_start = candle.start_ts
        self._current_start = None
        self._current = None
        return candle

    def _create_synthetic_candle(self, start_ts: datetime, close_price: float) -> Candle:
        return Candle(
            symbol=self.symbol,
            exchange_segment=self.exchange_segment,
            security_id=self.security_id,
            start_ts=start_ts,
            end_ts=datetime.fromtimestamp(start_ts.timestamp() + self.timeframe_seconds, tz=timezone.utc),
            open=close_price,
            high=close_price,
            low=close_price,
            close=close_price,
            volume=0.0,
            synthetic=True,
        )

    def update_from_tick(self, ts: datetime, price: float, volume: float = 0.0) -> List[Candle]:
        """Process a single tick. Returns any candles that were closed."""
        closed: List[Candle] = []
        if price <= 0:
            return closed

        bucket_start = self._floor_to_timeframe(ts)

        if self._current_start is None:
            if self._last_closed_start is not None and bucket_start <= self._last_closed_start:
                self.dropped_out_of_order_ticks += 1      # belongs to a candle already closed
                return closed
            self._new_current_candle(bucket_start, price, volume)
            return closed

        if bucket_start == self._current_start:
            assert self._current is not None
            self._current["high"] = max(self._current["high"], price)
            self._current["low"] = min(self._current["low"], price)
            self._current["close"] = price
            self._current["volume"] += volume
            return closed

        if bucket_start > self._current_start:
            closed_candle = self._close_current(synthetic=False)
            if closed_candle is not None:
                closed.append(closed_candle)

            # Fill missing minute buckets with synthetic flat candles, BUT ONLY during market hours.
            next_bucket = datetime.fromtimestamp(
                closed[-1].start_ts.timestamp() + self.timeframe_seconds, tz=timezone.utc
            )
            while next_bucket < bucket_start and self._last_close_price is not None:
                # Only fill synthetic candles while the exchange was actually in session.
                if is_within_trading_session(next_bucket):
                    synthetic = self._create_synthetic_candle(next_bucket, self._last_close_price)
                    closed.append(synthetic)
                    self._last_close_price = synthetic.close
                    self._last_closed_start = synthetic.start_ts

                next_bucket = datetime.fromtimestamp(
                    next_bucket.timestamp() + self.timeframe_seconds, tz=timezone.utc
                )

            self._new_current_candle(bucket_start, price, volume)
            return closed

        # BUG FIXED (architecture review): bucket_start < self._current_start
        # had NO branch at all — this tick was silently discarded, with no
        # log, no counter, nothing. Its price never updated the candle it
        # belonged to, and there was zero visibility into how often this
        # happened. A single stable WebSocket connection delivers ticks
        # in-order (TCP), so this is narrow in practice — the realistic
        # trigger is the overlap window right around a feed reconnect/hard-
        # restart. Rare, but silent data loss on a live-money system should
        # never be silent. There's no safe way to retroactively fix an
        # already-closed, already-evaluated candle, so this still can't be
        # applied — but it's now counted and logged instead of vanishing.
        self.dropped_out_of_order_ticks += 1
        # DEBUG, not WARNING: with candles closed at the exact boundary, a
        # trade printed in the last few ms that is still on the wire when the
        # boundary passes is EXPECTED to miss its candle. It's counted, and
        # the total is reported in the session summary.
        logger.debug(
            "%s: dropped a late tick for bucket %s — that candle had already closed "
            "(in-progress candle is %s). Usually a trade printed just before a boundary "
            "that arrived after the close grace, or a reconnect overlap. Total dropped: %d.",
            self.symbol, bucket_start.isoformat(), self._current_start.isoformat(),
            self.dropped_out_of_order_ticks,
        )
        return closed

    def flush_completed(self, now_ts: datetime) -> List[Candle]:
        """Force-close the current open candle if its timeframe has elapsed."""
        closed: List[Candle] = []
        if self._current_start is None:
            return closed

        now_bucket = self._floor_to_timeframe(now_ts)
        if now_bucket <= self._current_start:
            return closed

        current_close = self._close_current(synthetic=False)
        if current_close is None:
            # _close_current() returns None when _current is None, which the early
            # return above does not rule out — and the next statement dereferences it.
            # Not reachable today (the two fields are written together), but this sits
            # in the 1s flush loop for every symbol, so make the invariant explicit.
            return closed
        closed.append(current_close)

        next_bucket = datetime.fromtimestamp(
            current_close.start_ts.timestamp() + self.timeframe_seconds, tz=timezone.utc
        )
        while next_bucket < now_bucket and self._last_close_price is not None:
            if is_within_trading_session(next_bucket):
                synthetic = self._create_synthetic_candle(next_bucket, self._last_close_price)
                closed.append(synthetic)
                self._last_close_price = synthetic.close
                self._last_closed_start = synthetic.start_ts
            next_bucket = datetime.fromtimestamp(
                next_bucket.timestamp() + self.timeframe_seconds, tz=timezone.utc
            )

        return closed
