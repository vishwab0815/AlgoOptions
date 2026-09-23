"""
Strategy sub-package — strict 3-candle Consecutive Breakout, direction-agnostic.

BUG FIXED (stale docstring): this used to describe the strategy as
"Heikin-Ashi flip-based signal generation" — that HA_FLIP model was
explored early on, never wired into TradingEngine, and was removed as dead
code (see signal_engine.py's own docstring). The one strategy actually
running is the strict GREEN->RED->RED breakout (SHORT) / its RED->GREEN->
GREEN mirror (LONG), parametrized by DirectionRules — see direction.py.
"""

from .direction import BY_SIDE, LONG, SHORT, DirectionRules
from .signal_engine import SignalEngine, StrategyDecision

__all__ = [
    "SignalEngine", "StrategyDecision",
    "DirectionRules", "SHORT", "LONG", "BY_SIDE",
]
