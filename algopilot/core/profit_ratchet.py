"""core/profit_ratchet.py — per-tick step trailing profit lock.

Once an open position has earned `arm_pct`, a floor is placed under its
profit. That floor only ever moves UP. Price falling back to it closes the
trade immediately, on the tick — not at the next candle close.

    profit reaches 0.30%  ->  floor 0.25%   next rung 0.35%
    profit reaches 0.35%  ->  floor 0.30%   next rung 0.40%
    profit reaches 0.40%  ->  floor 0.35%   next rung 0.45%
    ...unbounded, in `step_pct` increments, identically for SHORT and LONG.

Below `arm_pct` the ratchet does nothing at all: the position is governed by
the candle trailing exit and the EOD square-off, exactly as before.

Percentages are of the ENTRY price and are GROSS — a pure price move, with no
charge modelling (deliberate: the ladder is meant to be read straight off a
chart). The floor is always `ceiling_pct - floor_pct` below the highest rung
reached, so the give-back is bounded between one step and one band width.

This module is pure arithmetic: no I/O, no clock, no position state. It is
the single place the ladder is defined.
"""
from __future__ import annotations

from dataclasses import dataclass

# The position has not yet earned `arm_pct`, so no floor exists.
NOT_ARMED = -1

# Guards the rung arithmetic against binary float representation: 0.45 - 0.40
# is 0.04999999999999999, which would floor() to the rung BELOW the one the
# price has actually reached and hand back an extra step of profit.
_EPS = 1e-9


@dataclass(frozen=True)
class RatchetConfig:
    """The ladder's geometry. Validated at construction."""

    arm_pct: float = 0.30       # profit that first places a floor
    floor_pct: float = 0.25     # the floor once armed
    ceiling_pct: float = 0.35   # reaching this lifts the whole band
    step_pct: float = 0.05      # how far the band moves each rung

    def __post_init__(self) -> None:
        if self.step_pct <= 0:
            raise ValueError("ratchet step_pct must be > 0")
        if self.floor_pct <= 0:
            raise ValueError("ratchet floor_pct must be > 0")
        if self.ceiling_pct <= self.floor_pct:
            raise ValueError(
                "ratchet ceiling_pct (%.4f) must be above floor_pct (%.4f), "
                "or the band would be inverted" % (self.ceiling_pct, self.floor_pct)
            )
        if not self.floor_pct <= self.arm_pct <= self.ceiling_pct:
            # Arming outside the band is a silent trap: arm below the floor and
            # the position exits the instant it arms; arm above the ceiling and
            # the first rung is skipped entirely.
            raise ValueError(
                "ratchet arm_pct (%.4f) must lie between floor_pct (%.4f) and "
                "ceiling_pct (%.4f)" % (self.arm_pct, self.floor_pct, self.ceiling_pct)
            )

    def step_for_profit(self, profit_pct: float) -> int:
        """The rung this profit has reached. NOT_ARMED below `arm_pct`.

        Closed form rather than a loop: a position sitting 40% in profit would
        otherwise walk 800 iterations on every tick.
        """
        if profit_pct < self.arm_pct - _EPS:
            return NOT_ARMED
        if profit_pct < self.ceiling_pct - _EPS:
            return 0
        return 1 + int((profit_pct - self.ceiling_pct + _EPS) / self.step_pct)

    def floor_pct_at(self, step: int) -> float:
        """Profit % that closes the position, at this rung."""
        return self.floor_pct + step * self.step_pct

    def ceiling_pct_at(self, step: int) -> float:
        """Profit % that lifts the band to the next rung."""
        return self.ceiling_pct + step * self.step_pct
