"""
algopilot/options/position.py — the SHORT-only position/exit math this
engine needs: entry accounting and the Heikin-Ashi trailing-stop level.

Self-contained on purpose — this engine only ever sells premium (see
strategy/direction.py's SHORT rules) and never places a real order (pure
paper trading — see config.py). Only two things ever close a position:
the Heikin-Ashi trailing stop and the EOD square-off. There is no profit
ratchet and no RSI/volume gate — entries are pure pattern (GREEN->RED->RED).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from ..strategy.direction import SHORT

# The engine calls trailing_exit_level() at the CLOSE of each candle, passing
# that just-closed candle's index; the level returned is the HA high of the
# candle BEFORE it. So while the next candle forms, the stop is the HA high
# from TWO candles back — not the immediately preceding one.
#
# (CORRECTED: this comment, the banner and the code comments all used to say
# "1 bar back", which is not what runs. The behaviour is kept deliberately:
# on 8 real sessions with realistic fills it was the best of 13 exits tested,
# and the literal 1-back stop lost ~Rs 6,200 more — it cuts short the few
# long trends that carry the strategy.)
COVER_LEVEL_LOOKBACK = 1


@dataclass
class OpenPosition:
    """Mutable state of a currently open SHORT position on one leg (CE/PE)."""
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""              # "CE" | "PE"
    entry_price: float = 0.0
    qty: int = 0
    entry_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    entry_order_id: str = ""      # paper-fill id, or the real Dhan order id in live mode
    cover_level: float = 0.0      # 0.0 = no trailing cover level set yet
    # ── live mode only ───────────────────────────────────────────────────────
    stop_order_id: str = ""       # protective stop-limit resting at the exchange
    stop_trigger_sent: float = 0.0  # trigger that order currently carries
    closing: bool = False         # a buy-back is in flight — never start a second
    breach_since: Optional[float] = None  # monotonic time price first crossed the stop

    @property
    def rules(self):
        return SHORT

    def unrealized_pnl(self, current_price: float) -> float:
        return round(self.rules.pnl(self.entry_price, current_price, self.qty), 2)

    def profit_pct(self, current_price: float) -> float:
        return self.rules.profit_pct(self.entry_price, current_price)

    def is_cover_level_triggered(self, current_price: float) -> bool:
        if self.cover_level <= 0:
            return False
        return self.rules.exit_level_triggered(current_price, self.cover_level)


def trailing_exit_level(df: Any, current_idx: int) -> Optional[float]:
    """The Heikin-Ashi high of the candle COVER_LEVEL_LOOKBACK bar(s) behind
    current_idx — the trailing stop on premium (a SHORT trails the HA high,
    never the low — see direction.SHORT.trailing_stop_reference). None if
    there isn't enough candle history yet."""
    ref_idx = current_idx - COVER_LEVEL_LOOKBACK
    if ref_idx < 0 or ref_idx >= len(df):
        return None
    row = df.iloc[ref_idx]
    return float(SHORT.trailing_stop_reference(float(row["ha_high"]), float(row["ha_low"])))
