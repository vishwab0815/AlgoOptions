"""
strategy/direction.py — everything that differs between a SHORT (sell-to-open)
and a LONG (buy-to-open) breakout, in one small object.

Injected into the shared pattern state machine (signal_engine.py), the order
guards (runtime/order_flow.py), and the position math (execution/
position_tracker.py) instead of duplicating each of those for a second
direction. A candle is either GREEN or RED — never both — so a SHORT setup
and a LONG setup can never fire off the same candle close; two symbols can
independently be SHORT and LONG at the same time, but one symbol never holds
both at once (see PositionTracker).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class DirectionRules:
    side: str            # "SHORT" | "LONG" — matches PositionSide.value
    entry_signal: str    # "SELL" | "BUY"   — order sent to OPEN
    exit_signal: str     # "BUY"  | "SELL"  — order sent to CLOSE
    is_short: bool        # True: SHORT (arms on GREEN). False: LONG (arms on RED).

    def is_starter_candle(self, is_bullish: bool) -> bool:
        """SHORT's Candle 1 is GREEN; LONG's Candle 1 is RED."""
        return is_bullish if self.is_short else not is_bullish

    def setup_level(self, ha_high: float, ha_low: float) -> float:
        """The level Candle 3 must break: SHORT watches the setup candle's HA
        Low; LONG watches its HA High."""
        return ha_low if self.is_short else ha_high

    def broke_level(self, ha_high: float, ha_low: float, ha_close: float, level: float) -> Tuple[bool, bool]:
        """(broke_on_extreme, broke_on_close) — SHORT breaks DOWN through the
        level, LONG breaks UP through it."""
        if self.is_short:
            return ha_low < level, ha_close < level
        return ha_high > level, ha_close > level

    def rsi_passes(self, rsi: float, short_min: float, long_max: float) -> bool:
        """SHORT wants RSI strictly ABOVE short_min (not already sold off);
        LONG wants it AT OR BELOW long_max (still cheap enough to buy).

        The two sides take separate numbers because they are not symmetric in
        practice: the three-candle pattern needs two green candles for a LONG,
        and those push RSI up — so a LONG threshold set as low as the SHORT one
        rejected ~94% of real LONG setups. The comparisons differ too: SHORT is
        exclusive (>), LONG inclusive (<=), as configured."""
        return rsi > short_min if self.is_short else rsi <= long_max

    def volume_passes(self, buy_qty: float, sell_qty: float, ratio: float) -> bool:
        """SHORT wants sell-side order-book pressure to dominate; LONG wants
        buy-side pressure to dominate, by the same configured ratio."""
        if self.is_short:
            return buy_qty > 0 and sell_qty >= buy_qty * ratio
        return sell_qty > 0 and buy_qty >= sell_qty * ratio

    def trailing_stop_reference(self, raw_high: float, raw_low: float) -> float:
        """The trailing exit level trails the Heikin-Ashi HIGH for a SHORT
        (price rising against you) and the Heikin-Ashi LOW for a LONG (price
        falling against you), both from the candle before the last closed one
        (two candles behind the one currently forming — see options/position.py)."""
        return raw_high if self.is_short else raw_low

    def profit_pct(self, entry_price: float, current_price: float) -> float:
        """Unrealised profit as a % of entry — positive when the trade is
        winning, negative when it is losing. SHORT profits as price falls."""
        if entry_price <= 0:
            return 0.0
        move = entry_price - current_price if self.is_short else current_price - entry_price
        return move / entry_price * 100.0

    def price_at_profit_pct(self, entry_price: float, pct: float) -> float:
        """The price at which this position shows exactly `pct` profit.
        Inverse of profit_pct(); for a SHORT that is BELOW the entry."""
        return entry_price * (1 - pct / 100) if self.is_short else entry_price * (1 + pct / 100)

    def exit_level_triggered(self, current_price: float, exit_level: float) -> bool:
        return current_price >= exit_level if self.is_short else current_price <= exit_level

    def pnl(self, entry_price: float, exit_price: float, qty: int) -> float:
        return (entry_price - exit_price) * qty if self.is_short else (exit_price - entry_price) * qty


SHORT = DirectionRules(side="SHORT", entry_signal="SELL", exit_signal="BUY", is_short=True)
LONG = DirectionRules(side="LONG", entry_signal="BUY", exit_signal="SELL", is_short=False)

BY_SIDE = {SHORT.side: SHORT, LONG.side: LONG}
