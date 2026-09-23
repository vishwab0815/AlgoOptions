"""
algopilot/options/engine.py — OptionsEngine: NIFTY premium-selling PAPER
TRADING composition root.

Real-time pipeline:
    WebSocket tick (CE/PE premium, pushed by DhanHQ — see websocket.py)
        -> per-leg CandleBuilder -> HeikinAshiEngine
        -> SignalEngine.evaluate_breakout (GREEN->RED->RED, on THIS leg's own
           premium) — advanced for BOTH legs on every candle close
        -> paper fill (sell to open) -> ledger
        -> every tick on an open leg: Heikin-Ashi trailing stop, EOD square-off

Entries are PURE pattern — no RSI gate, no volume gate, no profit ratchet.
The only two things that ever close a position are the trailing stop and
the 15:00 IST EOD square-off.

Sequencing: at the start of the day (or after the previous trade closes),
BOTH legs are watched — whichever breaks its pattern FIRST fires. The moment
that happens, the OTHER leg is locked out (only one position open at a time,
per OPTIONS_MAX_CONCURRENT=1) until this one closes, at which point watching
flips to specifically the other leg.

A slower maintenance loop (poll_interval_secs) handles spot polling for band
resolution, a safety-net candle flush, and a heartbeat log — see
maintenance_tick(). It never gates entries or exits; those run off the
WebSocket feed for minimal lag.

Startup is never pattern-blind: the moment a leg's contract resolves, its
real intraday candles for today so far are pulled from DhanHQ and replayed
through the same Heikin-Ashi/pattern logic (see _backfill_leg()) — no live
entries fire from this replay, it only seeds setup_stage/target_level so the
first LIVE candle continues the day's real story instead of starting fresh.

Crash/restart recovery: band, expiry, active side, and any open position are
persisted to the ledger on every state change (see _save_state()) and
restored on the next startup (_restore_state()) — a restart while short a
leg resumes managing that exact position rather than losing track of it.
Guarded by trading day, so a fresh day never resurrects yesterday's state.

This module places NO real order, anywhere — every fill here is simulated at
the live-quoted premium. See config.py for the hard refusal to start
otherwise.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from ..core.candle_builder import Candle, CandleBuilder
from ..core.heikin_ashi import HeikinAshiEngine
from ..strategy.direction import SHORT
from ..strategy.signal_engine import SignalEngine
from ..utils.market_hours import get_market_status
from .config import OptionsConfig
from .dhan_client import OptionsDhanClient
from .ledger import OptionsLedger
from .position import OpenPosition, trailing_exit_level
from .websocket import OptionsWebSocketManager

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
_LEGS = ("CE", "PE")
_NSE_FNO_SEGMENT_CODE = 2  # dhanhq.marketfeed.MarketFeed.NSE_FNO

# evaluate_breakout() never reads self.config — it is pure, direction-driven
# state-machine logic — so no config object is needed here.
_SIGNAL_ENGINE = SignalEngine()


def resolve_band(spot: float) -> tuple:
    """PUT strike = floor(spot/100)*100; CALL strike = PUT strike + 100."""
    put_strike = math.floor(spot / 100.0) * 100.0
    return put_strike, put_strike + 100.0


def _today_ist() -> date:
    return datetime.now(_IST).date()


@dataclass
class LegState:
    option_type: str  # "CE" | "PE"
    candle_builder: CandleBuilder
    ha_engine: HeikinAshiEngine
    setup_stage: str = SignalEngine.STAGE_WAIT_GREEN
    target_level: Optional[float] = None
    strike: Optional[float] = None
    security_id: Optional[str] = None
    # Real, currently-listed lot size from the scrip master — preferred over
    # config.lot_size the moment it resolves (NSE revises lot sizes
    # periodically; the config value is only a starting default).
    lot_size: Optional[int] = None
    position: Optional[OpenPosition] = None
    # True once today's real intraday candles have been replayed into
    # ha_engine/setup_stage/target_level — see OptionsEngine._backfill_leg().
    # Without this, starting the engine mid-session means it pattern-hunts
    # from a blank slate with no memory of the day's prior candles.
    backfilled: bool = False


class OptionsEngine:
    """Composition root for the NIFTY options paper-trading engine."""

    def __init__(self, config: OptionsConfig) -> None:
        self.config = config
        self.client = OptionsDhanClient(
            client_id=config.client_id,
            access_token=config.access_token,
            chain_min_interval_secs=config.chain_min_interval_secs,
        )
        self.ledger = OptionsLedger(config.db_path)
        self.ws_manager: Optional[OptionsWebSocketManager] = None

        self.balance = config.paper_capital
        self.realized_pnl = 0.0

        self.put_strike: Optional[float] = None
        self.call_strike: Optional[float] = None
        self.expiry: Optional[str] = None
        self._expiry_checked_epoch: float = 0.0
        # Debounce for _maybe_resolve_band — see there for why this exists
        # (spot sitting on a hundred-boundary flips the computed band on
        # ordinary tick noise; this requires the same candidate twice in a
        # row before it's actually committed).
        self._pending_band: Optional[tuple] = None

        # "ANY" = watch both legs, first pattern to fire wins. Pinned to one
        # specific side the instant a position opens (see _enter), and
        # flipped to the OTHER side the instant it closes (see _exit) — it
        # only ever returns to "ANY" on a fresh trading day.
        self.active_side: str = "ANY"
        self.legs: Dict[str, LegState] = {side: self._new_leg(side) for side in _LEGS}

        self._trading_day: date = _today_ist()
        self._last_premium: Dict[str, float] = {}
        self._last_spot_poll_epoch: float = 0.0
        self._running = False
        # asyncio only holds a WEAK reference to a task — one with no other
        # reference anywhere can be garbage-collected while it's suspended
        # (e.g. mid-await on the margin lookup), silently aborting it with
        # no error. Confirmed live: a SELL TRIGGER fired, the spawned
        # _on_candle_close task got GC'd while awaiting margin_per_lot_async,
        # and the entry never happened — no log, no exception, nothing.
        # This set keeps every spawned task alive until it actually finishes.
        self._background_tasks: set = set()

    def _new_leg(self, side: str) -> LegState:
        return LegState(
            option_type=side,
            candle_builder=CandleBuilder(
                symbol=side, exchange_segment=0, security_id="",
                timeframe_seconds=self.config.candle_timeframe_secs,
            ),
            ha_engine=HeikinAshiEngine(max_rows=500),
        )

    @property
    def band_frozen(self) -> bool:
        """The strike band structurally refuses to move while any leg is open."""
        return any(leg.position is not None for leg in self.legs.values())

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info(
            "OptionsEngine starting | paper capital=Rs %.0f | max lots/trade=%d | "
            "lot size=%d (config default) | trailing stop only, no RSI/ratchet",
            self.balance, self.config.max_lots_per_trade, self.config.lot_size,
        )
        self.ledger.log_event("ENGINE_START", details=f"paper_capital={self.balance}")

        had_prior_state = self._restore_state()

        self.ws_manager = OptionsWebSocketManager(
            config=self.config,
            on_tick_callback=self.on_market_tick,
            loop=asyncio.get_running_loop(),
        )
        # No subscriptions yet — the band hasn't resolved. maintenance_tick()
        # polls spot via REST until it does, then subscribes the two legs'
        # real security ids over the WebSocket for real-time premium ticks.
        self.ws_manager.start([])

        if had_prior_state:
            # Band/position came from a restart, not from maintenance_tick's
            # own "band just resolved" path — resolve contracts, backfill
            # both legs' pattern state, and subscribe explicitly here instead
            # of waiting on that path (which only fires on a FRESH resolve).
            await self._resolve_contracts_and_subscribe()

        try:
            while self._running:
                try:
                    await self.maintenance_tick()
                except Exception:
                    logger.exception("Options maintenance tick crashed — continuing on the next cycle.")
                await asyncio.sleep(self.config.poll_interval_secs)
        finally:
            if self.ws_manager is not None:
                self.ws_manager.stop()
            self.client.close()
            self.ledger.close()

    def stop(self) -> None:
        self._running = False

    def _spawn(self, coro) -> None:
        """asyncio.create_task(), but keeping a strong reference so the task
        can never be garbage-collected mid-flight (see _background_tasks)."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)

        def _done(t: "asyncio.Task") -> None:
            self._background_tasks.discard(t)
            exc = t.exception() if not t.cancelled() else None
            if exc is not None:
                logger.exception("Background task crashed: %s", exc, exc_info=exc)

        task.add_done_callback(_done)

    # ── Real-time tick entrypoint (called from the WebSocket thread) ──────────

    def on_market_tick(self, sec_id: str, price: float, ts: datetime) -> None:
        """Called on the main event loop for every premium tick DhanHQ pushes."""
        self._maybe_roll_day()

        mkt = get_market_status()
        if not mkt.is_trading_allowed:
            return
        squareoff_due = mkt.eod_squareoff_due

        for side in _LEGS:
            leg = self.legs[side]
            if leg.security_id != sec_id:
                continue

            # Ticks are the data plane, not the log plane: they update state
            # and drive candle/exit checks silently. The only thing worth
            # narrating is the 5-minute candle close — see _on_candle_close's
            # 1/3 -> 2/3 -> 3/3 staging, which now serves as the periodic
            # pulse instead of a tick stream or a separate heartbeat.
            self._last_premium[side] = price

            if leg.position is not None:
                self._check_exits(side, leg, price, squareoff_due, at_time=ts)

            for candle in leg.candle_builder.update_from_tick(ts, price):
                self._spawn(self._on_candle_close(side, leg, candle, squareoff_due))
            return

    # ── Band resolution + WebSocket subscription (spot has no reliable feed) ──

    async def _resolve_contracts_and_subscribe(self) -> None:
        await self._ensure_expiry_async()
        if not self.expiry:
            return

        subscriptions: List[tuple] = []
        for side in _LEGS:
            leg = self.legs[side]
            leg.strike = self.call_strike if side == "CE" else self.put_strike
            await self._ensure_contract_async(side, leg)
            if leg.security_id:
                subscriptions.append((_NSE_FNO_SEGMENT_CODE, leg.security_id))
                await self._backfill_leg(side, leg)

        if self.ws_manager is not None and subscriptions:
            # BUG FIXED: this used to require len(subscriptions) == 2 — if
            # ONE leg's contract resolution failed (transient scrip-master/
            # network glitch), the OTHER leg, even though it resolved fine,
            # never got subscribed either. Subscribe to whatever succeeded;
            # maintenance_tick() retries the missing leg on the next cycle.
            self.ws_manager.update_subscriptions(subscriptions)
            missing = [s for s in _LEGS if self.legs[s].security_id is None]
            if missing:
                logger.warning(
                    "Contract resolution incomplete (%s unresolved) — subscribed to "
                    "what succeeded, will retry %s next maintenance cycle | CE %s (id=%s) | PE %s (id=%s)",
                    "/".join(missing), "/".join(missing),
                    self.legs["CE"].strike, self.legs["CE"].security_id,
                    self.legs["PE"].strike, self.legs["PE"].security_id,
                )
            else:
                logger.info(
                    "Subscribed to live premium ticks | CE %s (id=%s) | PE %s (id=%s)",
                    self.legs["CE"].strike, self.legs["CE"].security_id,
                    self.legs["PE"].strike, self.legs["PE"].security_id,
                )
            self._save_state()
        elif self.ws_manager is not None:
            logger.warning(
                "Contract resolution failed for BOTH legs (CE strike=%s, PE strike=%s) — "
                "will retry next maintenance cycle.", self.call_strike, self.put_strike,
            )

    async def _backfill_leg(self, side: str, leg: LegState) -> None:
        """Rebuild this leg's Heikin-Ashi/pattern state from today's REAL
        exchange candles so far — a fresh start (or restart) never
        pattern-hunts blind. Never fires an entry; only seeds setup_stage/
        target_level so the next LIVE candle continues correctly."""
        if leg.backfilled or leg.security_id is None:
            return
        leg.backfilled = True  # set first — a failed fetch must not retry every tick

        now_ist = datetime.now(_IST)
        market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        if now_ist <= market_open:
            return

        interval_minutes = max(1, self.config.candle_timeframe_secs // 60)
        candles = await self.client.get_intraday_candles_async(
            leg.security_id, self.config.option_exchange_segment, "OPTIDX",
            interval_minutes,
            market_open.strftime("%Y-%m-%d %H:%M:%S"),
            now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        )
        if not candles:
            logger.info("[%s] no intraday backfill available — starting pattern-blind from here.", side)
            return

        # Never replay a candle whose bucket hasn't fully closed yet relative
        # to wall-clock now — the live feed must be the one to close it, or
        # it would get counted twice (once here, once for real later) and
        # shift the whole GREEN/RED sequence by one.
        interval_secs = self.config.candle_timeframe_secs
        current_bucket_start = (int(datetime.now(timezone.utc).timestamp()) // interval_secs) * interval_secs
        candles = [c for c in candles if (int(c["timestamp"]) // interval_secs) * interval_secs < current_bucket_start]

        for c in candles:
            row = leg.ha_engine.append_candle(
                {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": c["volume"]}
            )
            old_stage, old_target = leg.setup_stage, leg.target_level
            color = "GREEN" if float(row["ha_close"]) > float(row["ha_open"]) else "RED"
            decision, new_level, new_stage = _SIGNAL_ENGINE.evaluate_breakout(
                ha_open=float(row["ha_open"]), ha_high=float(row["ha_high"]),
                ha_close=float(row["ha_close"]), ha_low=float(row["ha_low"]),
                target_breakout_level=leg.target_level, setup_stage=leg.setup_stage, rules=SHORT,
            )
            leg.target_level = new_level
            leg.setup_stage = new_stage

            c_start = datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).astimezone(_IST)
            c_end = c_start + timedelta(seconds=interval_secs)
            window = f"{c_start:%H:%M}-{c_end:%H:%M}"
            self._log_pattern_stage(
                side, window, row, color, decision, old_stage, old_target, new_stage, new_level,
                candle_start=c_start, candle_end=c_end, strike=leg.strike, replay=True,
            )

        level_note = f" (level Rs {leg.target_level:.2f})" if leg.target_level else ""
        logger.info(
            "[%s] backfill complete — %d real candle(s) replayed, pattern stage now %s%s",
            side, len(candles), leg.setup_stage, level_note,
        )

    # ── Maintenance loop: spot polling + candle-flush safety net ──────────────
    # No heartbeat here — the 5-minute candle-close log (1/3 -> 2/3 -> 3/3,
    # per leg) IS the periodic pulse now. If you're seeing those lines every
    # 5 minutes, the feed is alive; you don't need a second signal for it.

    async def maintenance_tick(self) -> None:
        mkt = get_market_status()
        if not mkt.is_trading_allowed:
            return

        now = datetime.now(timezone.utc)
        squareoff_due = mkt.eod_squareoff_due

        # Spot has no reliable index feed over the WebSocket, so it's polled
        # over REST periodically — only to resolve/shift the strike band.
        # Leg premiums themselves are real-time via the WebSocket, not this.
        if time.monotonic() - self._last_spot_poll_epoch > self.config.chain_min_interval_secs:
            await self._ensure_expiry_async()
            if self.expiry:
                chain = await self.client.get_chain_async(
                    self.config.nifty_security_id, self.config.nifty_index_segment, self.expiry,
                )
                if chain is not None and chain.spot > 0:
                    band_was_unset = self.put_strike is None
                    if band_was_unset:
                        # Explicit two-step sequence, logged in this order on
                        # purpose: check the real NIFTY 50 spot FIRST, THEN
                        # derive the strike band from it — never the reverse.
                        logger.info("NIFTY 50 spot = %.2f", chain.spot)
                    before = (self.put_strike, self.call_strike)
                    self._maybe_resolve_band(chain.spot)
                    # BUG FIXED: this used to re-subscribe ONLY on the very
                    # first resolution (band_was_unset) — any LATER band
                    # change (a legitimate mid-day shift, or spot chopping
                    # right on a hundred-boundary) silently left the legs
                    # with security_id=None and no live subscription. The
                    # engine looked "stuck": band kept announcing changes,
                    # but nothing was ever actually wired up to trade them.
                    if self.put_strike is not None and (self.put_strike, self.call_strike) != before:
                        await self._resolve_contracts_and_subscribe()
                    elif self.put_strike is not None and any(
                        self.legs[s].security_id is None for s in _LEGS
                    ):
                        # BUG FIXED: band unchanged, but a leg never resolved
                        # its contract last attempt (transient failure) and
                        # nothing was retrying it — it would've stayed
                        # stranded until the next band change, however long
                        # that took. _ensure_contract_async/_backfill_leg are
                        # no-ops for a leg that already succeeded, so this is
                        # a cheap, safe retry every maintenance cycle.
                        await self._resolve_contracts_and_subscribe()
            self._last_spot_poll_epoch = time.monotonic()

        # Safety net: force-close any candle whose timeframe has elapsed even
        # if a tick hasn't arrived to trigger it (a quiet market, or a brief
        # WebSocket gap). Normal ticks already close candles in on_market_tick.
        for side in _LEGS:
            leg = self.legs[side]
            if leg.security_id:
                for candle in leg.candle_builder.flush_completed(now):
                    await self._on_candle_close(side, leg, candle, squareoff_due)

    def _maybe_roll_day(self) -> None:
        today = _today_ist()
        if today != self._trading_day:
            logger.info("New trading day (%s) — resetting band and pattern state.", today)
            self._trading_day = today
            self.put_strike = self.call_strike = None
            self.expiry = None
            self.active_side = "ANY"
            self.legs = {side: self._new_leg(side) for side in _LEGS}
            self._pending_band = None
            self._save_state()

    # ── State persistence (crash/restart recovery) ───────────────────────────

    def _current_state_dict(self) -> dict:
        positions: Dict[str, Optional[dict]] = {}
        for side, leg in self.legs.items():
            if leg.position is None:
                positions[side] = None
                continue
            p = leg.position
            positions[side] = {
                "trade_id": p.trade_id, "entry_price": p.entry_price, "qty": p.qty,
                "entry_time": p.entry_time.isoformat(), "entry_order_id": p.entry_order_id,
                "cover_level": p.cover_level,
                "strike": leg.strike, "security_id": leg.security_id, "lot_size": leg.lot_size,
            }
        return {
            "put_strike": self.put_strike, "call_strike": self.call_strike,
            "expiry": self.expiry, "active_side": self.active_side,
            "positions": positions,
        }

    def _save_state(self) -> None:
        self.ledger.save_state(self._current_state_dict())

    def _restore_state(self) -> bool:
        """True if a same-day band/position was found and restored. Called
        once at startup, before the WebSocket feed connects."""
        state = self.ledger.load_state()
        if state is None:
            return False

        self.put_strike = state.get("put_strike")
        self.call_strike = state.get("call_strike")
        self.expiry = state.get("expiry")
        self.active_side = state.get("active_side", "ANY")
        if self.put_strike is None:
            return False

        restored_position = False
        for side, pdata in (state.get("positions") or {}).items():
            leg = self.legs.get(side)
            if leg is None:
                continue
            leg.strike = self.call_strike if side == "CE" else self.put_strike
            if pdata is None:
                continue
            leg.security_id = pdata.get("security_id")
            leg.lot_size = pdata.get("lot_size")
            leg.position = OpenPosition(
                trade_id=pdata["trade_id"], symbol=side,
                entry_price=pdata["entry_price"], qty=pdata["qty"],
                entry_time=datetime.fromisoformat(pdata["entry_time"]),
                entry_order_id=pdata.get("entry_order_id", ""),
                cover_level=pdata.get("cover_level", 0.0),
            )
            restored_position = True
            logger.info(
                "[%s] restored an OPEN position from a previous run: entry=%.2f qty=%d cover=%.2f",
                side, leg.position.entry_price, leg.position.qty, leg.position.cover_level,
            )

        logger.info(
            "Restored state from an earlier run today: PUT %.0f / CALL %.0f | expiry=%s | active_side=%s%s",
            self.put_strike, self.call_strike, self.expiry, self.active_side,
            " | open position restored" if restored_position else " | no open position",
        )
        return True

    async def _ensure_expiry_async(self) -> None:
        now = time.monotonic()
        if self.expiry is not None and (now - self._expiry_checked_epoch) < 3600.0:
            return
        expiries = await self.client.get_expiry_list_async(
            self.config.nifty_security_id, self.config.nifty_index_segment
        )
        if expiries:
            self.expiry = expiries[0]
            self._expiry_checked_epoch = now

    def _maybe_resolve_band(self, spot: float) -> None:
        if self.band_frozen:
            self._pending_band = None
            return
        new_put, new_call = resolve_band(spot)
        if new_put == self.put_strike and new_call == self.call_strike:
            self._pending_band = None
            return
        # Debounce: spot sitting right on a hundred-boundary (e.g. 23400.00)
        # flips resolve_band()'s output on ordinary tick noise, every single
        # poll — confirmed live (band alternated 3x in 10 seconds). Require
        # the SAME candidate band on two CONSECUTIVE polls before committing,
        # so single-tick noise (which alternates, never repeats) never
        # commits, while a genuine sustained move (same candidate twice in a
        # row) still resolves within one extra poll interval.
        candidate = (new_put, new_call)
        if self._pending_band != candidate:
            self._pending_band = candidate
            return
        self._pending_band = None
        logger.info("Band resolved: PUT %.0f / CALL %.0f (spot=%.2f)", new_put, new_call, spot)
        self.put_strike, self.call_strike = new_put, new_call
        # A genuinely different contract per leg — candle/pattern history resets.
        self.legs = {side: self._new_leg(side) for side in _LEGS}

    async def _ensure_contract_async(self, side: str, leg: LegState) -> None:
        """The first call may trigger a scrip-master CSV download (up to
        60s) — runs off the event loop (see dhan_client.py) so it never
        blocks ticks."""
        if leg.security_id is not None or self.expiry is None or leg.strike is None:
            return
        contract = await self.client.resolve_contract_async(self.expiry, leg.strike, side)
        if contract is None:
            logger.warning(
                "Could not resolve a contract for %s %.0f (expiry %s) — margin sizing will use "
                "the fallback estimate and lot size will use the configured default (%d) until it can.",
                side, leg.strike, self.expiry, self.config.lot_size,
            )
            return
        leg.security_id = contract.security_id
        leg.lot_size = contract.lot_size
        if contract.lot_size != self.config.lot_size:
            logger.warning(
                "Live lot size for %s %.0f is %d, not the configured OPTIONS_LOT_SIZE=%d — "
                "using the live value for this leg.",
                side, leg.strike, contract.lot_size, self.config.lot_size,
            )

    # ── Candle close: pure pattern on this leg's own premium ──────────────────

    def _log_pattern_stage(
        self, side: str, window: str, ha_row, color: str, decision,
        old_stage: str, old_target: Optional[float], new_stage: str, new_target: Optional[float],
        candle_start: datetime, candle_end: datetime, strike: Optional[float],
        replay: bool = False,
    ) -> None:
        """Shared by live candle closes AND backfill replay, so both show the
        identical 1/3 -> 2/3 -> 3/3 narrative — backfill used to only print
        a one-line summary, silently hiding every candle that happened
        before the engine started. `replay=True` tags a backfilled line so
        it's never mistaken for something happening live right now.

        Also the single choke point that persists every candle to the
        `candle_log` table (exact HA OHLC + stage transition + timestamps) —
        the structured, queryable record for offline analysis. The text line
        below is for reading in the moment; that table is for the numbers."""
        prefix = f"[{side:<2}] REPLAY " if replay else f"[{side:<2}] "
        base = (
            f"{prefix}{window} IST {color:<5} | "
            f"HA O={ha_row['ha_open']:>7.2f} H={ha_row['ha_high']:>7.2f} "
            f"L={ha_row['ha_low']:>7.2f} C={ha_row['ha_close']:>7.2f}"
        )
        # Explicit 1/3 -> 2/3 -> 3/3 staging — GREEN always means "this IS
        # candle 1 of a fresh sequence" (evaluate_breakout's own rule: a
        # starter candle always (re)arms, even mid-sequence), so it's
        # labelled 1/3 regardless of what stage came before it.
        if decision.signal == "SELL":
            # old_target (captured BEFORE evaluate_breakout ran) is the level
            # candle 2 actually set and candle 3 just broke — new_target has
            # ALREADY been overwritten by evaluate_breakout on a SELL (it
            # returns this candle's own HA low, staged for what comes next).
            level = old_target if old_target is not None else 0.0
            logger.info("%s || 3/3 TRIGGER — broke %.2f, SELL @ %.2f", base, level, float(ha_row["ha_close"]))
        elif new_stage == SignalEngine.STAGE_GREEN_SEEN:
            logger.info("%s || 1/3 ARMED   — GREEN start, watch for RED", base)
        elif new_stage == SignalEngine.STAGE_LEVEL_SET:
            logger.info("%s || 2/3 SET     — level %.2f, next candle decides", base, new_target)
        elif old_stage == SignalEngine.STAGE_LEVEL_SET:
            # This WAS candle 3 (the one shot) — it just didn't break.
            level = old_target if old_target is not None else 0.0
            logger.info("%s || 3/3 FAILED  — held %.2f, resets to 1/3", base, level)
        else:
            logger.info("%s || ·           idle, waiting for GREEN", base)

        self.ledger.log_candle(
            leg=side, strike=strike, candle_start=candle_start, candle_end=candle_end,
            source="backfill" if replay else "live",
            ha_open=float(ha_row["ha_open"]), ha_high=float(ha_row["ha_high"]),
            ha_low=float(ha_row["ha_low"]), ha_close=float(ha_row["ha_close"]), color=color,
            stage_before=old_stage, stage_after=new_stage, target_level=new_target,
            signal=decision.signal or "NONE",
        )

    async def _on_candle_close(self, side: str, leg: LegState, candle: Candle, squareoff_due: bool) -> None:
        ha_row = leg.ha_engine.append_candle(candle.as_dict())
        candle_index = len(leg.ha_engine.df) - 1

        if leg.position is not None:
            trailing = trailing_exit_level(leg.ha_engine.df, candle_index)
            if trailing is not None:
                leg.position.cover_level = trailing
                self._save_state()

        old_stage, old_target = leg.setup_stage, leg.target_level
        is_bullish = float(ha_row["ha_close"]) > float(ha_row["ha_open"])
        color = "GREEN" if is_bullish else "RED"

        decision, new_level, new_stage = _SIGNAL_ENGINE.evaluate_breakout(
            ha_open=float(ha_row["ha_open"]), ha_high=float(ha_row["ha_high"]),
            ha_close=float(ha_row["ha_close"]), ha_low=float(ha_row["ha_low"]),
            target_breakout_level=leg.target_level, setup_stage=leg.setup_stage, rules=SHORT,
        )
        leg.target_level = new_level
        leg.setup_stage = new_stage

        # Always logged, both legs, every close — the locked-out side's
        # pattern keeps tracking silently underneath regardless (so it's
        # instantly ready the moment it becomes watchable), and hiding that
        # from the log made it look like it wasn't being checked at all.
        # Only ENTRY-FIRING is gated by watch state, not visibility.
        window = f"{candle.start_ts.astimezone(_IST):%H:%M}-{candle.end_ts.astimezone(_IST):%H:%M}"
        self._log_pattern_stage(
            side, window, ha_row, color, decision, old_stage, old_target, new_stage, new_level,
            candle_start=candle.start_ts, candle_end=candle.end_ts, strike=leg.strike,
        )

        watching = side == self.active_side or self.active_side == "ANY"
        # Only a leg being watched may open a fresh position; a leg already
        # holding one never re-fires (see the "watch both, pin to first" rule
        # in _enter/_exit).
        if not watching or leg.position is not None:
            return
        if decision.signal != "SELL":
            return
        if squareoff_due:
            return

        initial_cover = trailing_exit_level(leg.ha_engine.df, candle_index) or 0.0
        await self._enter(
            side, leg, entry_price=float(ha_row["ha_close"]), initial_cover=initial_cover,
            at_time=candle.end_ts,
        )

    # ── Entry (paper fill) ──────────────────────────────────────────────────

    async def _enter(
        self, side: str, leg: LegState, entry_price: float, initial_cover: float = 0.0,
        at_time: Optional[datetime] = None,
    ) -> None:
        lot_size = leg.lot_size or self.config.lot_size
        per_lot_margin, is_live = await self.client.margin_per_lot_async(
            leg.security_id, self.config.option_exchange_segment, entry_price,
            lot_size, self.config.fallback_margin_per_lot,
        )
        lots = 0
        if per_lot_margin > 0:
            lots = math.floor(self.balance / self.config.max_concurrent / per_lot_margin)
        lots = min(lots, self.config.max_lots_per_trade)
        if lots < 1:
            self.ledger.log_event(
                "BLOCKED_MARGIN", symbol=side,
                details=f"per_lot_margin={per_lot_margin:.0f} live={is_live} balance={self.balance:.0f}",
            )
            logger.warning(
                "[%s] entry blocked — balance=Rs %.0f cannot afford 1 lot at margin/lot=Rs %.0f.",
                side, self.balance, per_lot_margin,
            )
            return

        qty = lots * lot_size
        entry_order_id = f"PAPER-{uuid.uuid4().hex[:10]}"
        leg.position = OpenPosition(
            symbol=side, entry_price=entry_price, qty=qty,
            entry_order_id=entry_order_id, cover_level=initial_cover,
            entry_time=at_time or datetime.now(timezone.utc),
        )
        # Pin watching to this side — the other leg is locked out until this
        # position closes (see _exit, which flips it to the opposite side).
        self.active_side = side
        self._save_state()

        self.ledger.log_event(
            "ENTRY", symbol=side,
            details=(
                f"strike={leg.strike} price={entry_price:.2f} lots={lots} qty={qty} "
                f"lot_size={lot_size}({'live' if leg.lot_size else 'configured'}) "
                f"margin_per_lot={per_lot_margin:.0f}({'live' if is_live else 'fallback'})"
            ),
        )
        logger.info(
            "[%-2s] SELL  %7.0f | premium=%7.2f | lots=%d qty=%-4d (lot=%d, %-10s) | margin/lot=Rs %8.0f (%s)",
            side, leg.strike, entry_price, lots, qty, lot_size,
            "live" if leg.lot_size else "configured", per_lot_margin,
            "live" if is_live else "fallback",
        )

    # ── Exits (checked every tick) ────────────────────────────────────────────

    def _check_exits(
        self, side: str, leg: LegState, premium: float, squareoff_due: bool,
        at_time: Optional[datetime] = None,
    ) -> None:
        pos = leg.position
        if pos is None:
            return

        if squareoff_due:
            # A forced, time-based exit, not a pattern/level decision — there
            # is no HA level to sell at here, so this is the one place the
            # real live premium is used as the fill price, same as the
            # entry side of the coin: the market price at the moment you're
            # mechanically forced out.
            self._exit(side, leg, premium, "EOD_SQUAREOFF", at_time=at_time)
            return

        # Pure Heikin-Ashi economics: the live tick only answers "has price
        # reached the level yet" (it has to be tick-driven for responsiveness
        # — waiting for the next 5m candle close to check a stop would be far
        # too slow). Once triggered, the FILL price is the HA-derived level
        # itself — cover_level — never whatever raw tick happened to cross
        # it. Selling "at that price" means the HA level, exactly.
        if pos.is_cover_level_triggered(premium):
            self._exit(side, leg, pos.cover_level, "TRAILING_STOP", at_time=at_time)
            return

    def _exit(
        self, side: str, leg: LegState, exit_price: float, reason: str,
        at_time: Optional[datetime] = None,
    ) -> None:
        pos = leg.position
        if pos is None:
            return
        pnl = round(pos.rules.pnl(pos.entry_price, exit_price, pos.qty), 2)
        self.realized_pnl += pnl
        self.balance += pnl
        exit_order_id = f"PAPER-{uuid.uuid4().hex[:10]}"
        exit_time = at_time or datetime.now(timezone.utc)

        self.ledger.log_trade(
            trade_id=pos.trade_id, symbol=side, strike=leg.strike or 0.0,
            entry_price=pos.entry_price, exit_price=exit_price, qty=pos.qty,
            pnl=pnl, entry_order_id=pos.entry_order_id, exit_order_id=exit_order_id,
            entry_time=pos.entry_time, exit_time=exit_time, exit_reason=reason,
        )
        logger.info(
            "[%-2s] COVER | entry=%7.2f exit=%7.2f qty=%-4d | P&L=Rs %+9.2f | reason=%-13s | balance=Rs %11.2f",
            side, pos.entry_price, exit_price, pos.qty, pnl, reason, self.balance,
        )

        leg.position = None
        # Sequential single-side rule: the instant this closes, flip
        # watching to specifically the other leg for the next fresh entry.
        self.active_side = "PE" if side == "CE" else "CE"
        self._save_state()
