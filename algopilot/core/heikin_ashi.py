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
]


class HeikinAshiEngine:
    """
    Maintains a rolling pandas DataFrame of Heikin-Ashi transformed candles.

    Standard OHLCV candles are appended via :meth:`append_candle`.

    Cost: each append copies the frame (pd.concat), ~0.25 ms at a few
    hundred rows — measured, and negligible next to a 5-minute candle. The
    buffer grows to 2x max_rows before being trimmed back, so the trim runs
    once every max_rows candles, not on every one.
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
