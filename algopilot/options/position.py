"""
algopilot/options/position.py — the SHORT-only position/exit math this
engine needs: entry accounting and the Heikin-Ashi trailing-stop level.

Self-contained on purpose — this engine only ever sells premium (see
strategy/direction.py's SHORT rules). Three things close a position: the
Heikin-Ashi trailing stop, the profit lock (see profit_lock_level), and the
EOD square-off. No RSI/volume gate — entries are pure pattern (GREEN->RED->RED).
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
    # ── profit lock (see profit_lock_level) ──────────────────────────────────
    best_price: float = 0.0       # lowest premium seen since entry (0 = none yet)
    lock_price: float = 0.0       # buy back if premium comes back up to this (0 = no lock yet)
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


def profit_lock_level(entry_price: float, best_price: float, start_pts: float, step_pts: float) -> Optional[float]:
    """The profit lock for a SHORT, or None while profit hasn't reached `start_pts`.

    Marks are start, start+step, start+2*step ... points below the entry.
    Touching the first mark locks it; after that the lock stays ONE STEP
    behind the best mark touched. Sold at 123 with 10/3:
        touches 113 (10 pts) -> buy back if it comes back to 113
        touches 110 (13 pts) -> still 113
        touches 107 (16 pts) -> 110
        touches 104 (19 pts) -> 107      ... and so on, every 3 points."""
    gained = entry_price - best_price
    if best_price <= 0 or gained + 1e-9 < start_pts:
        return None
    mark = start_pts
    if step_pts > 0:
        mark += int((gained - start_pts) / step_pts + 1e-9) * step_pts
    locked_pts = max(start_pts, mark - step_pts) if step_pts > 0 else start_pts
    return round(entry_price - locked_pts, 2)


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
