from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple

from .direction import DirectionRules


@dataclass
class StrategyDecision:
    """Result returned by the signal engine for each closed candle."""
    signal: Optional[str]   # "BUY", "SELL", or None
    trend: str              # "Bullish" | "Bearish" | "Neutral"
    strength: str           # "Strong" | "Moderate" | "Weak"
    reason: str             # Human-readable explanation
    target_benchmark: Optional[float] = None


class SignalEngine:
    """
    CONSECUTIVE_BREAKOUT: strict 3-candle breakout, direction-agnostic.

    A SHORT setup is GREEN -> RED -> RED (breaks DOWN); a LONG setup is its
    mirror, RED -> GREEN -> GREEN (breaks UP). Candle 3 gets exactly one
    chance to break Candle 2's level; if it misses, the setup dies and a
    fresh starter candle must begin a new one. Which direction this call
    evaluates is entirely driven by the `rules` argument (see direction.py)
    — the state machine itself has no SHORT/LONG assumption baked in.
    See evaluate_breakout() for the full state machine.

    (This engine previously also offered an HA_FLIP model — a Heikin-Ashi
    flip generator with an EMA/ATR/RSI filter, via an evaluate() method. It
    was never wired into TradingEngine — only evaluate_breakout() ever was —
    and was removed as dead code rather than left to silently drift out of
    sync with the live strategy.)
    """

    # Position within the strict 3-candle pattern. Plain strings (not an
    # Enum) because these values are persisted onto SymbolState, sent over
    # the status/broadcast JSON, and restored by the startup backfill replay
    # — a str stays readable and round-trips through JSON unchanged. Names
    # are kept generic ("GREEN_SEEN" = "starter candle seen") since the same
    # three stages apply to both directions.
    STAGE_WAIT_GREEN = "WAIT_GREEN"   # no setup running; only a starter candle moves us on
    STAGE_GREEN_SEEN = "GREEN_SEEN"   # Candle 1 done; next opposite-color candle sets the level
    STAGE_LEVEL_SET = "LEVEL_SET"     # Candle 2 done; next candle is Candle 3

    def __init__(self, config: Any = None) -> None:
        # evaluate_breakout() below is pure, direction-driven state-machine
        # logic — it never reads self.config. `config` is accepted only so a
        # caller with its own settings object can still pass one through;
        # it's fine to construct this with no config at all (SignalEngine()).
        self.config = config

    def evaluate_breakout(
        self,
        ha_open: float,
        ha_high: float,
        ha_close: float,
        ha_low: float,
        target_breakout_level: Optional[float],
        setup_stage: str,
        rules: DirectionRules,
    ) -> Tuple[StrategyDecision, Optional[float], str]:
        """
        Evaluate the STRICT three-candle breakout ENTRY for one direction.
        Returns (StrategyDecision, new_target_level, new_setup_stage).

        This is called on EVERY candle close, including while the symbol
        already holds a position. That is deliberate: the pattern state
        machine must keep tracking the market so that, the moment the symbol
        goes flat again, its stage/level reflect CURRENT candles rather than
        a frozen pre-trade memory. Only the ORDER is gated —
        TickPipelineMixin._evaluate_entry() drops a signal raised while a
        position is open (logged as SKIPPED_POSITION_OPEN) instead of
        dispatching it, and PositionTracker enforces one slot per symbol
        regardless.

        (CORRECTED: this docstring previously claimed _on_candle_close()
        skips evaluation entirely while holding. That was true until the
        stale-pattern fix changed it; the text had not caught up.)

        There is deliberately no "current position" parameter: an earlier
        version had one purely to short-circuit into a "Holding X" no-op
        decision, which is now handled by the caller's gate instead.

        The pattern is exactly three candles and never more:

            Candle 1  starter — the wake-up candle. Clears any previous setup.
                      GREEN for a SHORT watch, RED for a LONG watch.
            Candle 2  confirm — records its level (HA Low for SHORT, HA High
                      for LONG) as the level to break.
            Candle 3  trigger — gets ONE chance: if it breaks that level (on
                      its HA extreme OR its HA Close), the entry fires.
                      If it does not break, the setup is DEAD and the engine
                      waits for a fresh starter candle before it will look at
                      anything again.

        Deliberate consequence (confirmed with the operator, SHORT side):
        once Candle 3 fails, later same-color candles are IGNORED no matter
        how far price moves, until a starter candle restarts the pattern.
        This is the whole point of the strict version — it replaces the
        earlier "rolling" behaviour where each failing candle simply
        re-anchored the level and kept hunting, which in practice let an
        entry fire many candles later than intended.

        `setup_stage` carries that position in the pattern between calls:
            WAIT_GREEN  — nothing going on; only a starter candle moves us on.
            GREEN_SEEN  — Candle 1 done. The next opposite-color candle sets
                          the level. Another starter candle just stays here
                          (the latest starter wins).
            LEVEL_SET   — Candle 2 done. The very next candle is Candle 3 and
                          is the only one allowed to trigger.

        Exit is NOT decided here. Once in a position, TradingEngine tracks a
        trailing exit level (the Heikin-Ashi high/low of the candle 1 bar
        behind the current one, per `rules`) and checks it against live price on
        every tick — see PositionTracker.check_cover_level(). This method
        only ever proposes the entry signal; it never proposes the exit.
        """
        is_bullish = ha_close > ha_open
        trend = "Bullish" if is_bullish else "Bearish"

        extreme_label = "HA Low" if rules.is_short else "HA High"
        extreme_val = ha_low if rules.is_short else ha_high
        starter_color = "Green" if rules.is_short else "Red"
        confirm_color = "red" if rules.is_short else "green"

        # ── Candle 1: the starter candle always (re)starts the pattern ────────
        # This covers both "first starter after a dead setup" and several
        # starters in a row, where the latest one is the new Candle 1.
        if rules.is_starter_candle(is_bullish):
            return (
                StrategyDecision(
                    signal=None,
                    trend=trend,
                    strength="Weak",
                    reason=f"{starter_color} candle — pattern reset. Waiting for the first {confirm_color} candle.",
                ),
                None,
                self.STAGE_GREEN_SEEN,
            )

        # Everything below here is the opposite-color (confirm/trigger) candle.

        # ── Candle 2: the first confirm candle after the starter records the level ──
        if setup_stage == self.STAGE_GREEN_SEEN:
            level = rules.setup_level(ha_high, ha_low)
            return (
                StrategyDecision(
                    signal=None,
                    trend=trend,
                    strength="Moderate",
                    reason=f"Setup candle: level to break is {extreme_label} {level:.2f}. "
                            f"The next candle gets the only chance.",
                    target_benchmark=level,
                ),
                level,
                self.STAGE_LEVEL_SET,
            )

        # ── Candle 3: the one and only chance to trigger ──────────────────────
        if setup_stage == self.STAGE_LEVEL_SET and target_breakout_level is not None:
            broke_extreme, broke_close = rules.broke_level(ha_high, ha_low, ha_close, target_breakout_level)
            if broke_extreme or broke_close:
                if broke_extreme and broke_close:
                    basis = f"{extreme_label} ({extreme_val:.2f}) and HA Close ({ha_close:.2f}) both broke"
                elif broke_extreme:
                    basis = f"{extreme_label} ({extreme_val:.2f}) broke"
                else:
                    basis = f"HA Close ({ha_close:.2f}) broke"
                direction_word = "below" if rules.is_short else "above"
                return (
                    StrategyDecision(
                        signal=rules.entry_signal,
                        trend=trend,
                        strength="Strong",
                        reason=f"Confirmed Breakout: {basis} {direction_word} the setup candle's "
                                f"{extreme_label} ({target_breakout_level:.2f}).",
                        target_benchmark=target_breakout_level,
                    ),
                    rules.setup_level(ha_high, ha_low),
                    # A fired setup is finished: the next entry must start again
                    # from a brand-new starter candle, exactly like a failed one.
                    self.STAGE_WAIT_GREEN,
                )

            # Candle 3 did not break — the setup is dead.
            return (
                StrategyDecision(
                    signal=None,
                    trend=trend,
                    strength="Weak",
                    reason=f"Setup failed: {extreme_label} {extreme_val:.2f} / HA Close {ha_close:.2f} did not "
                            f"break {target_breakout_level:.2f}. Waiting for a fresh {starter_color.lower()} candle.",
                ),
                None,
                self.STAGE_WAIT_GREEN,
            )

        # ── Confirm-color candle while no pattern is running — ignored by design ──
        return (
            StrategyDecision(
                signal=None,
                trend=trend,
                strength="Weak",
                reason=f"{confirm_color.capitalize()} candle ignored — no live setup. "
                        f"Waiting for a {starter_color.lower()} candle to start one.",
            ),
            None,
            self.STAGE_WAIT_GREEN,
        )
