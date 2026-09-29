"""
Core data-processing sub-package.

  - CandleBuilder    : aggregates raw ticks into fixed-timeframe OHLCV candles.
  - HeikinAshiEngine : transforms OHLCV candles into Heikin-Ashi.
"""

from .candle_builder import Candle, CandleBuilder
from .heikin_ashi import HeikinAshiEngine, HeikinAshiRow

__all__ = ["Candle", "CandleBuilder", "HeikinAshiEngine", "HeikinAshiRow"]
