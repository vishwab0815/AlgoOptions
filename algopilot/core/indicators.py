from __future__ import annotations

from dataclasses import dataclass
from math import isnan
from typing import Optional


@dataclass
class IndicatorSnapshot:
    """Immutable snapshot of indicator values for a single candle."""
    ema: float
    atr: float
    rsi: float


class IndicatorState:
    """
    Stateful, streaming computation of EMA, ATR, and RSI.

    All three indicators are updated incrementally on each candle close
    without storing the full price history — O(1) memory per symbol.
    """

    def __init__(self, ema_period: int = 50, atr_period: int = 14, rsi_period: int = 14) -> None:
        self.ema_period = ema_period
        self.atr_period = atr_period
        self.rsi_period = rsi_period

        self._prev_close: Optional[float] = None

        self._ema: Optional[float] = None

        self._atr: Optional[float] = None
        self._tr_count: int = 0

        self._avg_gain: Optional[float] = None
        self._avg_loss: Optional[float] = None
        self._rsi_count: int = 0

    @property
    def ema(self) -> float:
        return float("nan") if self._ema is None else self._ema

    @property
    def atr(self) -> float:
        return float("nan") if self._atr is None else self._atr

    @property
    def rsi(self) -> float:
        """Wilder's RSI(rsi_period). NaN until rsi_period candles have been seen.

        BUG FIXED (real-time correctness): this used to return a value from
        the SECOND candle onward — with _avg_loss still 0 because only one
        gain/loss sample existed, it returned exactly 100.0 (or 0.0 the first
        time a loss appeared), regardless of how thin the sample was. On a
        fresh 09:15 start this produced RSI=0 or RSI=100 for the first several
        candles of every symbol — pinned, not real-time — and the RSI gate
        (SHORT needs >30, LONG needs <=40) was making entry/reject decisions
        off a number with no statistical meaning. Measured on 5m data: 15% of
        warm-up-window values were exactly 0 or 100, some lasting past 09:40.

        The gate already treats NaN as "block" (`math.isnan(...)` in
        tick_pipeline.py), so returning NaN here for an underfilled window
        costs a few blocked entries at the open — never a wrong one.
        """
        if self._avg_gain is None or self._avg_loss is None:
            return float("nan")
        if self._rsi_count < self.rsi_period:
            return float("nan")
        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def update(self, high: float, low: float, close: float) -> IndicatorSnapshot:
        """Process a new candle and return the current indicator snapshot."""
        self._update_ema(close)
        self._update_atr(high, low, close)
        self._update_rsi(close)

        self._prev_close = close
        return IndicatorSnapshot(ema=self.ema, atr=self.atr, rsi=self.rsi)

    def _update_ema(self, close: float) -> None:
        if self._ema is None:
            self._ema = close
            return
        alpha = 2.0 / (self.ema_period + 1.0)
        self._ema = alpha * close + (1.0 - alpha) * self._ema

    def _update_atr(self, high: float, low: float, close: float) -> None:
        if self._prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self._prev_close), abs(low - self._prev_close))

        if self._atr is None:
            self._atr = tr
            self._tr_count = 1
            return

        if self._tr_count < self.atr_period:
            self._tr_count += 1
            self._atr = ((self._atr * (self._tr_count - 1)) + tr) / self._tr_count
            return

        self._atr = ((self._atr * (self.atr_period - 1)) + tr) / self.atr_period

    def _update_rsi(self, close: float) -> None:
        if self._prev_close is None:
            return

        change = close - self._prev_close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        if self._avg_gain is None or self._avg_loss is None:
            self._avg_gain = gain
            self._avg_loss = loss
            self._rsi_count = 1
            return

        if self._rsi_count < self.rsi_period:
            self._rsi_count += 1
            self._avg_gain = ((self._avg_gain * (self._rsi_count - 1)) + gain) / self._rsi_count
            self._avg_loss = ((self._avg_loss * (self._rsi_count - 1)) + loss) / self._rsi_count
            return

        self._avg_gain = ((self._avg_gain * (self.rsi_period - 1)) + gain) / self.rsi_period
        self._avg_loss = ((self._avg_loss * (self.rsi_period - 1)) + loss) / self.rsi_period


def is_valid_number(value: float) -> bool:
    """Return True if the value is a finite, non-NaN float."""
    return not isnan(value)
