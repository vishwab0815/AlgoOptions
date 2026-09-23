"""
Core data-processing sub-package.

Contains:
  - CandleBuilder  : Aggregates raw ticks into fixed-timeframe OHLCV candles.
  - HeikinAshiEngine : Transforms OHLCV candles into Heikin-Ashi format.
  - IndicatorState : Streaming EMA / ATR / RSI computation.
  - RatchetConfig  : Step trailing profit-lock ladder (pure arithmetic).
"""

from .candle_builder import Candle, CandleBuilder
from .heikin_ashi import HeikinAshiEngine, HeikinAshiRow
from .indicators import IndicatorSnapshot, IndicatorState, is_valid_number
from .profit_ratchet import NOT_ARMED, RatchetConfig

__all__ = [
    "Candle",
    "CandleBuilder",
    "HeikinAshiEngine",
    "HeikinAshiRow",
    "IndicatorSnapshot",
    "IndicatorState",
    "is_valid_number",
    "NOT_ARMED",
    "RatchetConfig",
]
