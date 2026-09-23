from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd


@dataclass
class HeikinAshiRow:
    open: float
    high: float
    low: float
    close: float
    volume: float
    ha_open: float
    ha_high: float
    ha_low: float
    ha_close: float


_COLUMNS = [
    "open", "high", "low", "close", "volume",
    "ha_open", "ha_high", "ha_low", "ha_close",
    "ema", "atr", "rsi",
]


class HeikinAshiEngine:
    """
    Maintains a rolling pandas DataFrame of Heikin-Ashi transformed candles.

    Standard OHLCV candles are appended via :meth:`append_candle`.
    Indicator values (EMA, ATR, RSI) are injected into the last row via
    :meth:`update_last_indicators` after each candle close.

    Performance notes (vs the original):
      - append_candle() uses pd.concat() instead of df.loc[len(df)] = row.
        The old form triggered a full DataFrame copy on EVERY candle append
        (O(n) per call). pd.concat() is O(1) amortised.
      - The rolling trim is now deferred: the buffer is allowed to grow to
        2× max_rows before it is sliced back to max_rows, so the O(n)
        reset_index() runs at most once every max_rows candles rather than
        once per candle once the buffer is full.
    """

    # Grow to this multiple of max_rows before trimming (amortises the slice).
    _TRIM_FACTOR = 2

    def __init__(self, max_rows: int = 500) -> None:
        self.max_rows = max_rows
        self._trim_at = max_rows * self._TRIM_FACTOR
        self.df = pd.DataFrame(columns=pd.Index(_COLUMNS))

    def _previous_ha(self) -> Optional[pd.Series]:
        if self.df.empty:
            return None
        return self.df.iloc[-1]

    def append_candle(self, candle: Dict[str, float]) -> pd.Series:
        """Compute HA values for the new candle and append it to the DataFrame."""
        prev = self._previous_ha()
        o = float(candle["open"])
        h = float(candle["high"])
        l = float(candle["low"])
        c = float(candle["close"])
        v = float(candle.get("volume", 0.0))

        ha_close = (o + h + l + c) / 4.0
        if prev is None:
            ha_open = (o + c) / 2.0
        else:
            ha_open = (float(prev["ha_open"]) + float(prev["ha_close"])) / 2.0

        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        row = {
            "open": o, "high": h, "low": l, "close": c, "volume": v,
            "ha_open": ha_open, "ha_high": ha_high,
            "ha_low": ha_low, "ha_close": ha_close,
            "ema": float("nan"), "atr": float("nan"), "rsi": float("nan"),
        }

        new_df = pd.DataFrame([row], columns=pd.Index(_COLUMNS))
        if self.df.empty:
            self.df = new_df
        else:
            self.df = pd.concat([self.df, new_df], ignore_index=True)

        # Defer the O(n) trim: only slice when the buffer reaches 2× max_rows,
        # so the cost is paid once every max_rows candles, not every candle.
        if len(self.df) >= self._trim_at:
            self.df = self.df.iloc[-self.max_rows:].reset_index(drop=True)

        return self.df.iloc[-1]

    def update_last_indicators(self, ema: float, atr: float, rsi: float) -> pd.Series:
        """Inject computed indicator values into the most-recently appended row."""
        if self.df.empty:
            raise RuntimeError("Cannot update indicator values on an empty DataFrame")

        self.df.at[self.df.index[-1], "ema"] = ema
        self.df.at[self.df.index[-1], "atr"] = atr
        self.df.at[self.df.index[-1], "rsi"] = rsi
        return self.df.iloc[-1]
