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

Entries are PURE pattern — no RSI gate, no volume gate. Three things close a
position: the Heikin-Ashi trailing stop, the profit lock (10 points below the
entry, then one 3-point step behind the best — see _track_profit), and the
15:00 IST EOD square-off. Whichever is reached first.

Sequencing: BOTH legs are watched all day — whichever breaks its pattern
FIRST fires, and the other leg can't open while that position is open (one
position at a time, OPTIONS_MAX_CONCURRENT=1). Once it closes, EITHER leg may
trade next; the leg that just closed only on a fresh pattern (its GREEN must
close after the exit).

A slower maintenance loop (poll_interval_secs) handles spot polling for band
resolution, a safety-net candle flush, and a heartbeat log — see
maintenance_tick(). It never gates entries or exits; those run off the
WebSocket feed for minimal lag.

Startup is never pattern-blind: the moment a leg's contract resolves, its
real intraday candles for today so far are pulled from DhanHQ and replayed
through the same Heikin-Ashi/pattern logic (see _backfill_leg()) — no live
entries fire from this replay, it only seeds setup_stage/target_level so the
first LIVE candle continues the day's real story instead of starting fresh.

Crash/restart recovery: band, expiry, last exit per side, and any open position are
persisted to the ledger on every state change (see _save_state()) and
restored on the next startup (_restore_state()) — a restart while short a
leg resumes managing that exact position rather than losing track of it.
Guarded by trading day, so a fresh day never resurrects yesterday's state.

Paper mode (OPTIONS_PAPER_TRADING=true) simulates every fill at the
live-quoted premium. Live mode (false) sends real orders through
broker.DhanBroker — every live branch is gated on `self.broker is not None`.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass, field, replace as dc_replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from ..core.candle_builder import Candle, CandleBuilder
from ..core.heikin_ashi import HeikinAshiEngine
from ..strategy.direction import SHORT
from ..strategy.signal_engine import SignalEngine
from ..utils.market_hours import get_market_status
from .config import OptionsConfig
from .dhan_client import OptionsDhanClient
from .ledger import OptionsLedger
from .broker import DhanBroker, OrderResult, OrderUpdateStream, new_tag, to_tick
from .charges import ChargeRates, round_trip_charges
from .position import OpenPosition, profit_lock_level, trailing_exit_level
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


# How far past a hundred-boundary spot must travel before an ALREADY-SET band
# moves (see _maybe_resolve_band). Spot parked at 23400.0x would otherwise
# flip the band on every poll, and a band change resets both legs' pattern
# state — so the cost of flapping is very high and the cost of lagging a real
# shift by a few points is nil.
_BAND_HYSTERESIS_POINTS = 15.0

# The candle clock sleeps until this close to the boundary, then spins on the
# wall clock for the rest. Windows timers tick every ~15.6 ms, so a plain
# sleep wakes up to ~40 ms late (measured); with a 60 ms final spin the worst
# miss measured was 0.03 ms. Cost: ~60 ms of spinning per 5-minute candle.
_CLOCK_SPIN_SECS = 0.060

# Live: warm the order connection this long before each candle close if it
# hasn't been used for _WARM_IF_IDLE_SECS (Dhan keeps an idle one open >100 s).
_WARM_BEFORE_SECS = 2.0
_WARM_IF_IDLE_SECS = 20.0

# Calendar days of earlier sessions fetched to continue the Heikin-Ashi series
# (see _backfill_leg). 5 covers a weekend plus a holiday; one full session is
# already enough — the seed's influence halves with every candle.
_HA_SEED_LOOKBACK_DAYS = 5

# Fetching DhanHQ's bar for a candle that just closed (see _finalize_candle).
# The half-formed candle at start MUST be completed, so it waits longer.
# Asking every 0.5 s keeps two legs within Dhan's 5 requests/s data limit.
_PARTIAL_FETCH_MAX_SECS = 5.0
_BAR_FETCH_POLL_SECS = 0.5
# How long after using a bar to fetch it again and check Dhan didn't revise it.
_BAR_RECHECK_SECS = 20.0

# Kill switch: create this file (any content) and the engine squares off
# every open position and stops. It refuses to START while the file exists.
KILL_SWITCH_FILE = Path("data/KILL")

# Live mode: how often the broker's available funds are re-read (seconds).
_FUNDS_REFRESH_SECS = 60.0

# Live mode: how often an open position is checked against Dhan's positions,
# to notice a position closed OUTSIDE the engine (by hand in the Dhan app).
_POSITION_CHECK_SECS = 5.0

# Refresh a leg's cached margin this long before its 15-minute cache expires.
_MARGIN_REFRESH_SECS = 600.0


def _today_ist() -> date:
    return datetime.now(_IST).date()


def _decided_after(candle_end: datetime) -> str:
    """How long after the candle's close the decision was taken, e.g.
    " | +0.4 ms after 14:50:00" — shown on every SELL/SKIPPED line so trigger
    timing is visible, not assumed. Blank for replays (their close is history)."""
    ms = (datetime.now(timezone.utc) - candle_end).total_seconds() * 1000.0
    if not 0.0 <= ms < 60_000.0:
        return ""
    return f" | +{ms:.1f} ms after {candle_end.astimezone(_IST):%H:%M:%S}"


def _in_session(epoch_seconds: float) -> bool:
    """True for a candle that starts inside the cash session (09:15 up to,
    not including, 15:30 IST) — right for both 1m and 5m bars."""
    hhmm = datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).astimezone(_IST).strftime("%H:%M")
    return "09:15" <= hhmm < "15:30"


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
    # The candle that was already half-formed when this leg started receiving
    # ticks (engine start, restart, or a strike change). The live feed only
    # sees its second half, so at its close the complete bar is fetched from
    # DhanHQ before it's evaluated — see _finalize_candle().
    partial_bucket: Optional[datetime] = None
    # Set when that fetch failed: the candle still feeds the pattern, but a
    # signal on it is not traded (its prices couldn't be verified).
    unverified_bucket: Optional[datetime] = None
    # When the latest GREEN (the pattern's starter candle) closed. The
    # pattern is GREEN -> RED -> RED, so at a signal this is when the setup
    # began — used to tell a fresh setup from one formed during the previous
    # trade on this side (see _entry_skip_reason).
    starter_end: Optional[datetime] = None


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
        # One position at a time; after it closes EITHER side may trade next.
        # (Alternation — "after a CE trade only PE" — was removed on 30-Sep: it
        # skipped the 11:20 CE signal that day.) A side that just closed needs
        # a FRESH pattern: its GREEN must close after that exit, so the engine
        # can't jump straight back in on a setup formed during the trade.
        # Rebuilt from the ledger on restart (_restore_realized_pnl).
        self._last_exit_at: Dict[str, datetime] = {}
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

        # LIVE ORDERS: only constructed when OPTIONS_PAPER_TRADING=false (and
        # the confirm phrase is set — see config.py). Every live branch below
        # is gated on `self.broker is not None`, so paper mode is untouched.
        self.broker: Optional[DhanBroker] = None if config.paper_trading else DhanBroker(
            config.client_id, config.access_token, config.option_exchange_segment,
        )
        self._broker_funds: Optional[float] = None
        self._funds_checked_at = 0.0
        self._positions_checked_at = 0.0
        # Contracts where Dhan's position differs from the engine's (manual
        # trades etc.) — re-checked every few seconds, see _check_foreign_positions.
        self._foreign_contracts: set = set()
        # Contracts where Dhan shows a position this engine didn't open — it
        # won't trade them (see _reconcile_live).
        self._blocked_contracts: set = set()
        # BUG FIXED (paper too): "one trade at a time" was checked BEFORE the
        # await in _enter (margin lookup; in live, waiting for the fill). If CE
        # and PE both signalled on the same candle, the second passed the
        # check while the first was still being filled, and both opened.
        self._entry_in_flight: Optional[str] = None

        self._charge_rates = ChargeRates(
            brokerage_per_order=config.brokerage_per_order,
            stt_pct=config.stt_pct,
            exchange_txn_pct=config.exchange_txn_pct,
            sebi_pct=config.sebi_pct,
            stamp_duty_pct=config.stamp_duty_pct,
            gst_pct=config.gst_pct,
        )

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
        """The strike band refuses to move while a position is open OR an entry
        order is still in flight.

        BUG FIXED: this only checked for a RECORDED position. A live SELL takes
        ~0.2-0.3 s from send to confirmed fill, and the position is recorded
        only after that — so a strike switch inside that window replaced the
        legs, the fill was then attached to the discarded old leg, and the
        engine lost a real short: no trailing stop, no 15:00 buy-back, not in
        saved state (reproduced in a test before this fix)."""
        return (self._entry_in_flight is not None
                or any(leg.position is not None for leg in self.legs.values()))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        if self.broker is not None:
            logger.info(
                "OptionsEngine starting | LIVE | max lots/trade=%d | sized on Dhan's available funds | "
                "lot size=%d (config default) | trailing stop only, no RSI/ratchet",
                min(self.config.max_lots_per_trade, self.config.live_max_lots), self.config.lot_size,
            )
        else:
            logger.info(
                "OptionsEngine starting | PAPER | capital=Rs %.0f | max lots/trade=%d | "
                "lot size=%d (config default) | trailing stop only, no RSI/ratchet",
                self.balance, self.config.max_lots_per_trade, self.config.lot_size,
            )
        self.ledger.log_event("ENGINE_START", details=f"paper_capital={self.balance}")

        had_prior_state = self._restore_state()
        self._restore_realized_pnl()
        if self.broker is not None:
            await self._reconcile_live()

        self.ws_manager = OptionsWebSocketManager(
            config=self.config,
            on_tick_callback=self.on_market_tick,
            loop=asyncio.get_running_loop(),
        )
        # No subscriptions yet — the band hasn't resolved. maintenance_tick()
        # polls spot via REST until it does, then subscribes the two legs'
        # real security ids over the WebSocket for real-time premium ticks.
        self.ws_manager.start([])
        # Candle closes run on their own task so nothing in the maintenance
        # loop (a chain poll mid-HTTP, a backfill) can ever sit in front of
        # the boundary. See _candle_clock().
        self._spawn(self._candle_clock())
        if self.broker is not None:
            # Fills are PUSHED by Dhan (see broker.OrderUpdateStream) — the
            # fast way to know an order filled; REST polling stays as fallback.
            self.broker.stream = OrderUpdateStream(self.config.client_id, self.config.access_token)
            self._spawn(self.broker.stream.run())

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
            if self.broker is not None and self.broker.stream is not None:
                await self.broker.stream.stop()
            await self._cancel_background_tasks()
            self.client.close()
            if self.broker is not None:
                self.broker.close()
            self.ledger.close()

    async def _cancel_background_tasks(self, timeout: float = 3.0) -> None:
        """End every task this engine spawned before shutting down. Left to
        asyncio.run() they were cancelled only after the summary printed, and
        on 30-Sep the order stream's close sat there 10 s until a second Ctrl+C
        killed it mid-close ("Task was destroyed but it is pending!").
        State is saved on every change, so a restart resumes any open trade."""
        pending = [t for t in self._background_tasks if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.wait(pending, timeout=timeout)

    async def _candle_clock(self) -> None:
        """Close every leg's candle at EXACTLY the timeframe boundary.

        Before: a candle closed on the first tick of the NEXT bucket (0.01-1+ s
        late in the live logs — ticks arrive ~1.25/s) or whenever the 1s
        maintenance loop came round, and that loop could itself be stuck
        behind an option-chain HTTP call. Now a dedicated task sleeps to just
        short of hh:mm:00.000, spins the last few ms on the wall clock, and
        closes the candle at the boundary with whatever it has. A tick from
        the new bucket that happens to arrive first closes it the same way
        (on_market_tick) — whichever is first; the other finds nothing to do.
        """
        tf = self.config.candle_timeframe_secs
        grace = self.config.candle_close_grace_ms / 1000.0
        while self._running:
            boundary = (time.time() // tf + 1) * tf
            target = boundary + grace
            if self.broker is not None:
                # Make sure the HTTPS connection to Dhan's order server is OPEN
                # when the order goes out: a fresh connection measured 327 ms,
                # an open one 39 ms. The 5 s position check normally keeps it
                # open; this covers the case where that has been quiet.
                await asyncio.sleep(max(0.0, boundary - _WARM_BEFORE_SECS - time.time()))
                if time.monotonic() - self.broker.last_call_at > _WARM_IF_IDLE_SECS:
                    self._spawn(self.broker.keep_warm_async())
            await asyncio.sleep(max(0.0, target - time.time() - _CLOCK_SPIN_SECS))
            while time.time() < target:
                await asyncio.sleep(0)       # yields, so ticks keep flowing while we wait
            try:
                await self._close_candles_at(datetime.fromtimestamp(boundary, tz=timezone.utc))
            except Exception:
                logger.exception("Candle clock: closing candles at the boundary failed.")

    async def _close_candles_at(self, boundary: datetime) -> None:
        mkt = get_market_status(now=boundary)
        if (not mkt.is_trading_allowed and not mkt.eod_squareoff_due) or self.client.auth_failed:
            return
        closed = [(side, self.legs[side], candle) for side in _LEGS if self.legs[side].security_id
                  for candle in self.legs[side].candle_builder.flush_completed(boundary)]
        # Both legs' DhanHQ bars are fetched AT THE SAME TIME, then evaluated
        # in the usual order (CE, then PE) — never one leg waiting on the other's fetch.
        final = await asyncio.gather(*(self._finalize_candle(side, leg, c) for side, leg, c in closed))
        for (side, leg, _), candle in zip(closed, final):
            if candle is not None:
                await self._on_candle_close(side, leg, candle, mkt.eod_squareoff_due, finalized=True)

    def _restore_realized_pnl(self) -> None:
        """BUG FIXED: balance and realized P&L lived only in memory, so any
        restart reset them to the starting capital — the closing balance was
        wrong and sizing ignored the day's result. Rebuilt from today's
        closed trades in the ledger (the source of truth), so it can't drift."""
        trades = self.ledger.get_today_trades()
        if not trades:
            return
        self.realized_pnl = round(sum(t["pnl"] for t in trades), 2)
        self.balance = self.config.paper_capital + self.realized_pnl
        for t in trades:
            try:
                self._note_exit(t["leg"], datetime.fromisoformat(t["exit_time"]))
            except (TypeError, ValueError):
                pass
        logger.info(
            "Resumed today's result from the ledger: %d closed trade(s), net Rs %+.2f, balance Rs %.2f",
            len(trades), self.realized_pnl, self.balance,
        )

    def _note_exit(self, side: str, at: datetime) -> None:
        """Remember when `side` last closed a trade (latest wins)."""
        if side not in _LEGS:
            return
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        prev = self._last_exit_at.get(side)
        if prev is None or at > prev:
            self._last_exit_at[side] = at

    def _last_exits_text(self) -> str:
        if not self._last_exit_at:
            return "none"
        return ", ".join(f"{s} {ts.astimezone(_IST):%H:%M:%S}" for s, ts in sorted(self._last_exit_at.items()))

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
        squareoff_due = mkt.eod_squareoff_due
        # BUG FIXED: this used to gate on is_trading_allowed alone, which is
        # False during POST_CLOSE (15:25-15:30) — the exact window
        # market_hours.py sets eod_squareoff_due for, to "still catch a
        # straggler position right up to real close". Returning here defeated
        # that, and contradicted that module's own rule that an exit must
        # never be gated. Letting the square-off window through cannot open
        # anything: _on_candle_close refuses every entry while squareoff_due.
        if not mkt.is_trading_allowed and not squareoff_due:
            return

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
        if self.broker is not None:
            # New strikes: is there already a position in them that the engine
            # didn't open (e.g. your manual trade)? Then don't trade them.
            await self._check_foreign_positions()
        for side in _LEGS:
            leg = self.legs[side]
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

        # Ticks for this leg start after this point, so the candle in progress
        # right now will only be seen from here on — mark it for completion.
        tf = self.config.candle_timeframe_secs
        leg.partial_bucket = datetime.fromtimestamp((int(time.time()) // tf) * tf, tz=timezone.utc)

        now_ist = datetime.now(_IST)
        today = now_ist.date()

        # BUG FIXED (1): this asked DhanHQ for candles "from 09:15:00". When a
        # request starts exactly on 09:15:00, DhanHQ leaves the opening prints
        # out of the 09:15 bar (confirmed on 29-Sep: 22600 CE came back with
        # O=146.70 H=153.45 instead of O=H=199.85; 22500 PE with L=8.65
        # instead of 4.00) — and for a past day it drops the 09:15 bar
        # entirely. The opening print is the most important print of the day:
        # without it both legs' first HA candle came out GREEN when the chart
        # shows RED. Asking from 09:00 returns the complete bar.
        #
        # BUG FIXED (2): each HA open is built from the previous HA candle, and
        # the chart carries that series over from earlier sessions — but this
        # started it fresh at 09:15 every day, so the first candles' HA open
        # (and near-tie colours) disagreed with the chart. Prior sessions are
        # now fed through the HA engine first (values only — no pattern, no
        # logging), so today's HA values continue the same series the chart
        # draws. The PATTERN still starts fresh at 09:15.
        interval_minutes = max(1, self.config.candle_timeframe_secs // 60)
        fetch_from = (now_ist - timedelta(days=_HA_SEED_LOOKBACK_DAYS)).replace(
            hour=9, minute=0, second=0, microsecond=0)
        candles = await self.client.get_intraday_candles_async(
            leg.security_id, self.config.option_exchange_segment, "OPTIDX",
            interval_minutes,
            fetch_from.strftime("%Y-%m-%d %H:%M:%S"),
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
        # DhanHQ's intraday endpoint also returns bars outside the session
        # (confirmed: 15:30 and 15:35 option bars, and a 20:00 bar on the
        # index). They'd enter the HA recursion and shift every colour after
        # them, so only regular-session bars are replayed.
        candles = [c for c in candles if _in_session(c["timestamp"])]

        def _day(c):
            return datetime.fromtimestamp(c["timestamp"], tz=timezone.utc).astimezone(_IST).date()

        prior = [c for c in candles if _day(c) < today]
        candles = [c for c in candles if _day(c) == today]
        for c in prior:
            leg.ha_engine.append_candle(
                {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": c["volume"]}
            )
        if prior:
            logger.info(
                "[%s] Heikin-Ashi continued from %d candle(s) of %s..%s, so today's HA values "
                "match the chart's series (pattern still starts fresh at 09:15).",
                side, len(prior), f"{_day(prior[0]):%d-%b}", f"{_day(prior[-1]):%d-%b}",
            )
        else:
            logger.info("[%s] no earlier session for this contract — Heikin-Ashi starts at 09:15.", side)

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
            if color == "GREEN":
                leg.starter_end = c_end
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
        squareoff_due = mkt.eod_squareoff_due
        # Same fix as on_market_tick: POST_CLOSE (15:25-15:30) has
        # is_trading_allowed=False but eod_squareoff_due=True, and a position
        # still open at that point has to be closed, not abandoned until
        # tomorrow. No entry can result — _on_candle_close blocks them all
        # while squareoff_due.
        if not mkt.is_trading_allowed and not squareoff_due:
            return

        if self.client.auth_failed:
            # A rejected token can't recover in-process (it's read once at
            # startup) and every feed is dead with it — a process that keeps
            # "running" here only looks healthy while doing nothing. State is
            # saved, so a restart with a fresh token resumes any open position.
            logger.critical(
                "Stopping: DhanHQ rejected the access token. Open positions (if any) are saved "
                "and will be resumed after a restart with a fresh DHAN_ACCESS_TOKEN."
            )
            self.stop()
            return

        now = datetime.now(timezone.utc)

        if KILL_SWITCH_FILE.exists():
            await self._kill_switch()
            return

        if self.broker is not None:
            await self._poll_live_stops()
            if time.monotonic() - self._funds_checked_at > _FUNDS_REFRESH_SECS:
                self._funds_checked_at = time.monotonic()
                funds = await self.broker.funds_async()
                if funds is not None:
                    self._broker_funds = funds

        # Close any candle whose timeframe has elapsed (quiet strike, feed
        # gap, or simply no tick yet after the boundary). Done FIRST: the
        # chain poll below is a network round trip (up to its 10s timeout),
        # and running it first delayed the candle close — and so the entry
        # decision — by however long that call took.
        for side in _LEGS:
            leg = self.legs[side]
            if leg.security_id:
                for candle in leg.candle_builder.flush_completed(now):
                    await self._on_candle_close(side, leg, candle, squareoff_due)

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

                    self._check_exits_from_chain(chain, squareoff_due, now)
                    await self._prefetch_margins(chain)
            self._last_spot_poll_epoch = time.monotonic()


    async def _prefetch_margins(self, chain) -> None:
        """LATENCY: keep each flat leg's margin figure fresh in the background,
        so the entry at a candle close never waits on the margin calculator
        (50-300 ms over the network on a cache miss, every 15 minutes)."""
        for side in _LEGS:
            leg = self.legs[side]
            if leg.position is not None or not leg.security_id or leg.strike is None:
                continue
            age = self.client.margin_cache_age(leg.security_id)
            if age is not None and age < _MARGIN_REFRESH_SECS:
                continue
            quote = ((chain.legs or {}).get(leg.strike) or {}).get(side.lower())
            price = getattr(quote, "last_price", 0.0) or self._last_premium.get(side, 0.0)
            if price > 0:
                await self.client.margin_per_lot_async(
                    leg.security_id, self.config.option_exchange_segment, price,
                    leg.lot_size or self.config.lot_size, self.config.fallback_margin_per_lot,
                )

    def _check_exits_from_chain(self, chain, squareoff_due: bool, now: datetime) -> None:
        """Exit safety net that does NOT depend on the WebSocket.

        BUG FIXED: _check_exits used to be reachable only from
        on_market_tick(), making every exit — the trailing stop AND the 15:00
        square-off — entirely tick-driven. Any quiet feed meant an open SHORT
        sat with its stop-loss silently not running: an expired token (DhanHQ
        closes the socket with code 50, which the SDK only print()s), a
        strike illiquid enough to go minutes without a trade, or a TLS
        failure that stops the feed connecting at all. On sold premium that
        is unbounded downside with no stop.

        The option chain is already polled every few seconds for spot, and
        its response carries each strike's live LTP — so this costs no extra
        API call. Ticks still drive exits sub-second when the feed is
        healthy; this only ever fires when they aren't arriving.
        """
        for side in _LEGS:
            leg = self.legs[side]
            if leg.position is None or leg.strike is None:
                continue
            quotes = (chain.legs or {}).get(leg.strike)
            if not quotes:
                continue
            quote = quotes.get(side.lower())
            premium = getattr(quote, "last_price", 0.0) or 0.0
            if premium <= 0:
                continue
            self._last_premium[side] = premium
            self._check_exits(side, leg, premium, squareoff_due, at_time=now)

    def _maybe_roll_day(self) -> None:
        today = _today_ist()
        if today != self._trading_day:
            logger.info("New trading day (%s) — resetting band and pattern state.", today)
            self._trading_day = today
            self.put_strike = self.call_strike = None
            self.expiry = None
            self._last_exit_at = {}
            self.legs = {side: self._new_leg(side) for side in _LEGS}
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
                "best_price": p.best_price, "lock_price": p.lock_price,
                "stop_order_id": p.stop_order_id, "stop_trigger_sent": p.stop_trigger_sent,
                "strike": leg.strike, "security_id": leg.security_id, "lot_size": leg.lot_size,
            }
        return {
            "put_strike": self.put_strike, "call_strike": self.call_strike,
            "expiry": self.expiry,
            "last_exit_at": {s: ts.isoformat() for s, ts in self._last_exit_at.items()},
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
        for s, iso in (state.get("last_exit_at") or {}).items():
            try:
                self._note_exit(s, datetime.fromisoformat(iso))
            except (TypeError, ValueError):
                pass
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
            if self.ledger.trade_booked(pdata["trade_id"]):
                logger.warning("[%s] saved state still showed an open position, but its trade %s is "
                               "already booked as closed — not resuming it.", side, pdata["trade_id"])
                continue
            leg.security_id = pdata.get("security_id")
            leg.lot_size = pdata.get("lot_size")
            leg.position = OpenPosition(
                trade_id=pdata["trade_id"], symbol=side,
                entry_price=pdata["entry_price"], qty=pdata["qty"],
                entry_time=datetime.fromisoformat(pdata["entry_time"]),
                entry_order_id=pdata.get("entry_order_id", ""),
                cover_level=pdata.get("cover_level", 0.0),
                best_price=pdata.get("best_price", 0.0) or 0.0,
                lock_price=pdata.get("lock_price", 0.0) or 0.0,
                stop_order_id=pdata.get("stop_order_id", "") or "",
                stop_trigger_sent=pdata.get("stop_trigger_sent", 0.0) or 0.0,
            )
            restored_position = True
            logger.info(
                "[%s] restored an OPEN position from a previous run: entry=%.2f qty=%d cover=%.2f%s",
                side, leg.position.entry_price, leg.position.qty, leg.position.cover_level,
                f" profit lock={leg.position.lock_price:.2f}" if leg.position.lock_price else "",
            )

        logger.info(
            "Restored state from an earlier run today: PUT %.0f / CALL %.0f | expiry=%s | last exit %s%s",
            self.put_strike, self.call_strike, self.expiry, self._last_exits_text(),
            " | open position restored" if restored_position else " | no open position",
        )
        return True

    async def _ensure_expiry_async(self) -> None:
        """Choose the day's expiry ONCE; it holds until the trading day rolls.

        BUG FIXED: this used to re-read the list every hour and take
        whatever came first. Two problems: (1) a contract switch mid-session
        left the legs on the old expiry's security ids while any later band
        change resolved the NEW expiry — two expiries live at once; (2) it
        never checked the date, so an already-expired entry at the head of
        the list would have been traded. It also never said which expiry it
        picked, so an expiry-day (0 DTE) session looked like any other.
        """
        if self.expiry is not None:
            return
        expiries = await self.client.get_expiry_list_async(
            self.config.nifty_security_id, self.config.nifty_index_segment
        )
        today = _today_ist()
        upcoming = [e for e in expiries if e >= today.isoformat()]
        if not upcoming:
            return

        chosen = upcoming[0]
        rolled = False
        if chosen == today.isoformat() and self.config.roll_on_expiry_day and len(upcoming) > 1:
            chosen, rolled = upcoming[1], True
        self.expiry = chosen

        dte = (date.fromisoformat(chosen) - today).days
        if dte == 0:
            logger.warning(
                "Expiry %s — EXPIRY DAY (0 DTE): these contracts settle at 15:30 today. "
                "Premiums decay fast and can move violently near the close. "
                "Set OPTIONS_ROLL_ON_EXPIRY_DAY=true to trade next week's contract instead.", chosen,
            )
        else:
            logger.info(
                "Expiry %s (%d day%s out)%s.", chosen, dte, "" if dte == 1 else "s",
                " — rolled past today's expiry (OPTIONS_ROLL_ON_EXPIRY_DAY=true)" if rolled else "",
            )

    def _maybe_resolve_band(self, spot: float) -> None:
        if self.band_frozen:
            return
        new_put, new_call = resolve_band(spot)
        if new_put == self.put_strike and new_call == self.call_strike:
            return

        if self.put_strike is not None:
            # HYSTERESIS, not a debounce. A "same candidate twice in a row"
            # debounce does NOT fix a spot that SITS on the boundary: at
            # 23400.0x it genuinely reads the same new band on every poll,
            # so the debounce confirms it and commits — then one tick later
            # reads 23399.9x and commits back. Confirmed live on the VM:
            # the band flipped 23300/23400 <-> 23400/23500 eight times in
            # six minutes, and because a band change resets both legs, each
            # flip threw away all pattern state and re-ran a full backfill.
            # Fix: once a band is established, spot must clear the boundary
            # by a real margin before it moves. Inside the deadband, the
            # CURRENT band simply stands.
            boundary = self.call_strike if new_put > self.put_strike else self.put_strike
            moving_up = new_put > self.put_strike
            cleared = (
                spot >= boundary + _BAND_HYSTERESIS_POINTS if moving_up
                else spot <= boundary - _BAND_HYSTERESIS_POINTS
            )
            if not cleared:
                logger.debug(
                    "Band hold: spot %.2f is within %.0f pts of the %.0f boundary — "
                    "keeping PUT %.0f / CALL %.0f.",
                    spot, _BAND_HYSTERESIS_POINTS, boundary, self.put_strike, self.call_strike,
                )
                return

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
        replay: bool = False, synthetic: bool = False,
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
            # Worded as a SIGNAL, not a sale: whether it trades is decided
            # after this line and always gets its own SELL or SKIPPED line.
            level = old_target if old_target is not None else 0.0
            if replay:
                logger.info("%s || 3/3 TRIGGER — broke %.2f (history only, not traded)", base, level)
            else:
                logger.info("%s || 3/3 TRIGGER — broke %.2f, signal", base, level)
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
            source="backfill" if replay else ("synthetic" if synthetic else "live"),
            ha_open=float(ha_row["ha_open"]), ha_high=float(ha_row["ha_high"]),
            ha_low=float(ha_row["ha_low"]), ha_close=float(ha_row["ha_close"]), color=color,
            stage_before=old_stage, stage_after=new_stage, target_level=new_target,
            signal=decision.signal or "NONE",
        )

    async def _on_candle_close(self, side: str, leg: LegState, candle: Candle, squareoff_due: bool,
                               finalized: bool = False) -> None:
        if not finalized:
            candle = await self._finalize_candle(side, leg, candle)
            if candle is None:
                return
        if leg is not self.legs.get(side):
            return          # a candle of strikes the engine has already moved off
        ha_row = leg.ha_engine.append_candle(candle.as_dict())
        candle_index = len(leg.ha_engine.df) - 1

        if leg.position is not None and candle.start_ts >= leg.position.entry_time:
            # The feed sends snapshots, so a brief touch of a profit mark can be
            # missed between them; the bar's low (DhanHQ's, when available) has it.
            self._track_profit(side, leg, float(candle.low))
        if leg.position is not None:
            trailing = trailing_exit_level(leg.ha_engine.df, candle_index)
            if trailing is not None:
                leg.position.cover_level = trailing
                self._save_state()
                if self.broker is not None and not leg.position.closing:
                    await self._sync_protective_stop(side, leg)

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
        if is_bullish:
            leg.starter_end = candle.end_ts

        # Always logged, both legs, every close — the locked-out side's
        # pattern keeps tracking silently underneath regardless (so it's
        # instantly ready the moment it becomes watchable), and hiding that
        # from the log made it look like it wasn't being checked at all.
        # Only ENTRY-FIRING is gated by watch state, not visibility.
        window = f"{candle.start_ts.astimezone(_IST):%H:%M}-{candle.end_ts.astimezone(_IST):%H:%M}"
        self._log_pattern_stage(
            side, window, ha_row, color, decision, old_stage, old_target, new_stage, new_level,
            candle_start=candle.start_ts, candle_end=candle.end_ts, strike=leg.strike,
            synthetic=getattr(candle, "synthetic", False),
        )

        if decision.signal != "SELL":
            return

        # BUG FIXED: every signal now gets exactly one outcome line — either
        # the SELL line from _enter() or a SKIPPED line saying why. Before,
        # the log printed "SELL @ X" for signals the engine then silently
        # refused, which is how a day with 9 real trades read as 13.
        after = _decided_after(candle.end_ts)
        skip = self._entry_skip_reason(side, leg, candle, squareoff_due)
        if skip:
            logger.info("[%-2s] %s IST    -> SKIPPED: %s%s", side, window, skip, after)
            self.ledger.log_event(
                "SIGNAL_SKIPPED", symbol=side,
                details=f"candle={window} reason={skip} ha_close={float(ha_row['ha_close']):.2f} "
                        f"raw_close={float(candle.close):.2f}",
            )
            return

        # BUG FIXED: the fill used to be the HA close — an average of the
        # candle's OHLC, not a price anyone can trade. On a falling trigger
        # candle it sits above the market, so every paper entry was
        # flattered (8 real days: +0.70 pts/trade, ~Rs 3,400 that could never
        # have been filled). The HA close stays the SIGNAL; the fill is the
        # candle's raw close — the last traded price at the moment the
        # signal became known, which is what a market sell would actually get.
        initial_cover = trailing_exit_level(leg.ha_engine.df, candle_index) or 0.0
        await self._enter(
            side, leg, entry_price=float(candle.close), initial_cover=initial_cover,
            at_time=candle.end_ts, signal_price=float(ha_row["ha_close"]), decided=after,
        )

    async def _fetch_bar(self, leg: LegState, candle: Candle, max_wait: float) -> Optional[dict]:
        """DhanHQ's own bar for `candle`'s bucket, asked for until it appears
        or `max_wait` seconds pass (None then). Never blocks longer than that,
        even if a request hangs."""
        tf = self.config.candle_timeframe_secs
        # Ask from one bar EARLIER: a request that starts exactly on a bar's
        # start time comes back without that bar's first prints (see _backfill_leg).
        frm = (candle.start_ts - timedelta(seconds=tf)).astimezone(_IST).strftime("%Y-%m-%d %H:%M:%S")
        to = (candle.end_ts + timedelta(seconds=tf)).astimezone(_IST).strftime("%Y-%m-%d %H:%M:%S")
        want = int(candle.start_ts.timestamp())
        deadline = time.monotonic() + max_wait
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                return None
            try:
                rows = await asyncio.wait_for(self.client.get_intraday_candles_async(
                    leg.security_id, self.config.option_exchange_segment, "OPTIDX", max(1, tf // 60), frm, to),
                    timeout=left)
            except asyncio.TimeoutError:
                return None
            bar = next((r for r in (rows or []) if int(r["timestamp"]) == want), None)
            if bar is not None:
                return bar
            if deadline - time.monotonic() <= _BAR_FETCH_POLL_SECS:
                return None
            await asyncio.sleep(_BAR_FETCH_POLL_SECS)

    @staticmethod
    def _with_bar(candle: Candle, bar: dict) -> Candle:
        return dc_replace(candle, open=float(bar["open"]), high=float(bar["high"]), low=float(bar["low"]),
                          close=float(bar["close"]), volume=float(bar.get("volume", candle.volume) or 0.0),
                          synthetic=False)

    async def _finalize_candle(self, side: str, leg: LegState, candle: Candle) -> Optional[Candle]:
        """The candle to evaluate: DhanHQ's own bar for it when available, else
        the live-feed candle. None = drop it (strikes changed meanwhile).

        1. The candle half-formed when this leg started (engine start, restart,
           strike change) was only partly seen — start at 10:02 and 10:00-10:02
           is missing. It is ALWAYS completed from DhanHQ; if that fails, a
           signal on it is not traded.
        2. Every other candle (OPTIONS_OFFICIAL_CANDLES=true): the live feed
           sends snapshots, not every trade, so its highs/lows can differ from
           the exchange's — and the pattern level is an HA low. DhanHQ's bar is
           what the chart shows; if it hasn't appeared within
           OPTIONS_OFFICIAL_CANDLE_WAIT_MS, the live-feed candle is used."""
        if leg is not self.legs.get(side):
            return None
        partial = leg.partial_bucket is not None and candle.start_ts == leg.partial_bucket
        if partial:
            leg.partial_bucket = None
        elif not self.config.official_candles:
            return candle
        max_wait = _PARTIAL_FETCH_MAX_SECS if partial else self.config.official_candle_wait_ms / 1000.0
        t0 = time.monotonic()
        bar = await self._fetch_bar(leg, candle, max_wait)
        took_ms = (time.monotonic() - t0) * 1000.0
        if leg is not self.legs.get(side):
            # Strikes changed while the bar was being fetched: the old strike's
            # signal must not be traded after the engine left it.
            logger.info("[%-2s] strikes changed during the candle fetch — old strike's candle dropped.", side)
            return None
        window = f"{candle.start_ts.astimezone(_IST):%H:%M}-{candle.end_ts.astimezone(_IST):%H:%M}"
        if bar is None:
            if partial:
                leg.unverified_bucket = candle.start_ts
                logger.warning(
                    "[%-2s] %s was only partly seen (engine started mid-candle) and DhanHQ hadn't published the "
                    "full candle within %.0fs — it still updates the pattern, but a signal on it will NOT be traded.",
                    side, window, max_wait)
            else:
                logger.warning("[%-2s] %s: DhanHQ's bar not available within %.0f ms — using the live-feed candle.",
                               side, window, max_wait * 1000.0)
            return candle
        if partial:
            logger.info(
                "[%-2s] %s completed from DhanHQ (engine started mid-candle): live feed saw O=%.2f H=%.2f L=%.2f "
                "C=%.2f, full candle O=%.2f H=%.2f L=%.2f C=%.2f",
                side, window, candle.open, candle.high, candle.low, candle.close,
                bar["open"], bar["high"], bar["low"], bar["close"])
        else:
            logger.debug("[%-2s] %s DhanHQ bar in %.0f ms: O=%.2f H=%.2f L=%.2f C=%.2f (live feed O=%.2f H=%.2f "
                         "L=%.2f C=%.2f)", side, window, took_ms, bar["open"], bar["high"], bar["low"], bar["close"],
                         candle.open, candle.high, candle.low, candle.close)
        used = self._with_bar(candle, bar)
        self._spawn(self._recheck_bar(side, leg, used))
        return used

    async def _recheck_bar(self, side: str, leg: LegState, used: Candle) -> None:
        """Fetch the bar again a little later: if DhanHQ revised it after the
        engine acted on it, say so (it was final at first sight in every check
        so far — this keeps proving that on the VM)."""
        await asyncio.sleep(_BAR_RECHECK_SECS)
        bar = await self._fetch_bar(leg, used, 5.0)
        if bar is None:
            return
        diff = [f"{k[0].upper()} {getattr(used, k):.2f}->{float(bar[k]):.2f}"
                for k in ("open", "high", "low", "close") if abs(getattr(used, k) - float(bar[k])) > 1e-6]
        if diff:
            window = f"{used.start_ts.astimezone(_IST):%H:%M}-{used.end_ts.astimezone(_IST):%H:%M}"
            logger.warning("[%-2s] %s: DhanHQ revised this bar after it was used (%s).", side, window, ", ".join(diff))
            self.ledger.log_event("BAR_REVISED", symbol=side, details=f"candle={window} {' '.join(diff)}")

    def _entry_skip_reason(
        self, side: str, leg: LegState, candle: Candle, squareoff_due: bool,
    ) -> Optional[str]:
        """Why a valid SELL signal will NOT be traded, or None to trade it."""
        if leg.unverified_bucket is not None and candle.start_ts == leg.unverified_bucket:
            return "first candle after start was only partly seen and couldn't be verified"
        if self._entry_in_flight is not None:
            return f"{self._entry_in_flight} entry still being placed (one trade at a time)"
        if self.config.max_daily_loss > 0 and self.realized_pnl <= -self.config.max_daily_loss:
            return (f"daily loss limit reached (net Rs {self.realized_pnl:.2f}, "
                    f"limit Rs -{self.config.max_daily_loss:.0f})")
        if leg.security_id in self._foreign_contracts:
            return "Dhan shows a position in this contract that the engine did not open - not trading it"
        if leg.security_id in self._blocked_contracts:
            return "Dhan shows a position in this contract that the engine did not open - blocked"
        if leg.position is not None:
            return "already short this leg"
        other_open = [s for s in _LEGS if s != side and self.legs[s].position is not None]
        if other_open:
            return f"{other_open[0]} position still open (one trade at a time)"
        last_exit = self._last_exit_at.get(side)
        if last_exit is not None and (leg.starter_end is None or leg.starter_end <= last_exit):
            return (f"not a fresh pattern - its GREEN closed before this side's last trade was covered "
                    f"({last_exit.astimezone(_IST):%H:%M:%S}); needs a new GREEN -> RED -> RED")
        if squareoff_due:
            return "after 15:00 - no new entries"
        close_hhmm = candle.end_ts.astimezone(_IST).strftime("%H:%M")
        if close_hhmm < self.config.entry_start:
            # market_hours.py documents a post-open buffer, but the engine
            # never enforced it — entries could fire from 09:15.
            return f"before {self.config.entry_start} - opening buffer, no entries yet"
        if getattr(candle, "synthetic", False):
            # A synthetic candle is FABRICATED by CandleBuilder to fill a feed
            # gap: flat at the last known price, zero volume, no real trade.
            # It still feeds the HA sequence (dropping it would shift every
            # later candle); it just can't open a position at a stale price.
            return "candle is synthetic (feed gap, no real trades)"
        return None

    # ── Entry (paper fill) ──────────────────────────────────────────────────

    async def _enter(self, side: str, leg: LegState, *args, **kwargs) -> None:
        # Taken synchronously, before the first await, so no other candle close
        # can slip an entry in while this one is being sized / filled.
        self._entry_in_flight = side
        try:
            await self._enter_locked(side, leg, *args, **kwargs)
        finally:
            self._entry_in_flight = None

    async def _enter_locked(
        self, side: str, leg: LegState, entry_price: float, initial_cover: float = 0.0,
        at_time: Optional[datetime] = None, signal_price: Optional[float] = None,
        decided: str = "",
    ) -> None:
        if leg is not self.legs.get(side):
            logger.warning("[%-2s] signal dropped: strikes changed before the order could be sent.", side)
            return
        lot_size = leg.lot_size or self.config.lot_size
        per_lot_margin, is_live = await self.client.margin_per_lot_async(
            leg.security_id, self.config.option_exchange_segment, entry_price,
            lot_size, self.config.fallback_margin_per_lot,
        )
        if leg is not self.legs.get(side):
            logger.warning("[%-2s] signal dropped: strikes changed before the order could be sent.", side)
            return
        capital = self.balance
        if self.broker is not None and self._broker_funds is not None:
            # Never size beyond what the account actually has free.
            capital = min(capital, self._broker_funds)
        lots = 0
        if per_lot_margin > 0:
            lots = math.floor(capital / self.config.max_concurrent / per_lot_margin)
        lots = min(lots, self.config.max_lots_per_trade)
        if self.broker is not None:
            lots = min(lots, self.config.live_max_lots)
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
        mode = "market"
        if self.broker is not None:
            fill = await self._live_order("SELL", side, leg, qty, ref_price=entry_price)
            if not fill.ok:
                self.ledger.log_event(
                    "ORDER_FAILED", symbol=side,
                    details=f"SELL x{qty} {fill.status} order={fill.order_id} reason={fill.message}",
                )
                logger.error("[%-2s] LIVE SELL NOT FILLED (%s): %s — no position opened.",
                             side, fill.status, fill.message)
                return
            # The position is what the exchange actually filled, at its price.
            entry_price, qty, entry_order_id = fill.avg_price, fill.filled_qty, fill.order_id
            mode = f"LIVE order {fill.order_id}"
        leg.position = OpenPosition(
            symbol=side, entry_price=entry_price, qty=qty,
            entry_order_id=entry_order_id, cover_level=initial_cover,
            entry_time=at_time or datetime.now(timezone.utc),
        )
        # The ENTRY event and the state holding the new position commit as ONE
        # transaction — never one without the other.
        self.ledger.record_entry(
            ("ENTRY", side,
             f"strike={leg.strike} fill={entry_price:.2f} "
             f"signal_ha_close={signal_price if signal_price is not None else entry_price:.2f} "
             f"lots={lots} qty={qty} "
             f"lot_size={lot_size}({'live' if leg.lot_size else 'configured'}) "
             f"margin_per_lot={per_lot_margin:.0f}({'live' if is_live else 'fallback'}) "
             f"stop={initial_cover:.2f} order={entry_order_id}"),
            self._current_state_dict(),
        )
        logger.info(
            "[%-2s] SELL  %7.0f | fill %7.2f (%s) | HA signal %7.2f | x%-4d (%d lot) | "
            "stop %7.2f | margin/lot Rs %8.0f (%s)%s",
            side, leg.strike, entry_price, mode,
            signal_price if signal_price is not None else entry_price,
            qty, lots, initial_cover, per_lot_margin, "live" if is_live else "fallback", decided,
        )
        if self.broker is not None:
            await self._place_protective_stop(side, leg)
            self._spawn(self._verify_entry_price(side, leg, entry_order_id, entry_price))

    # ── Live order handling (only reachable when self.broker is set) ──────────

    async def _live_order(self, txn: str, side: str, leg: LegState, qty: int, ref_price: float) -> OrderResult:
        """Send a BUY/SELL for this leg and wait for the exchange's fill."""
        order_type, price = "MARKET", 0.0
        if self.config.live_order_type == "LIMIT" and ref_price > 0:
            buf = self.config.live_limit_buffer_pct / 100.0
            # Marketable limit: fills now, but never worse than the buffer.
            price = to_tick(ref_price * (1 - buf), up=False) if txn == "SELL" else to_tick(ref_price * (1 + buf), up=True)
            order_type = "LIMIT"
        order_id, msg = await self.broker.place_async(txn, leg.security_id, qty, order_type, price=price, tag=new_tag())
        self.ledger.log_event("ORDER_SENT", symbol=side,
                              details=f"{txn} x{qty} {order_type} price={price:.2f} order={order_id} {msg}")
        if order_id is None:
            return OrderResult(None, "REJECTED", message=msg)
        # SELL (entry): act on the pushed fill at once — the protective stop
        # goes in right after, so every ms here is time unprotected; the price
        # is re-checked over REST in the background (_verify_entry_price).
        # BUY (exit): re-read the exact fill, since it's booked into the trade.
        res = await self.broker.wait_for_fill(order_id, exact=(txn == "BUY"))
        self.ledger.log_event("ORDER_RESULT", symbol=side,
                              details=f"{txn} order={order_id} status={res.status} filled={res.filled_qty} "
                                      f"avg={res.avg_price:.2f} {res.message}")
        return res

    async def _verify_entry_price(self, side: str, leg: LegState, order_id: str, pushed_price: float) -> None:
        """The entry was confirmed from Dhan's push; confirm its average price
        over REST too, and correct the position if they differ."""
        st = await self.broker.status_async(order_id)
        pos = leg.position
        if pos is None or pos.entry_order_id != order_id or st.avg_price <= 0:
            return
        if abs(st.avg_price - pushed_price) > 1e-6:
            logger.info("[%-2s] entry price corrected from Dhan's order book: %.2f -> %.2f", side, pushed_price, st.avg_price)
            pos.entry_price = st.avg_price
            self._save_state()

    async def _place_protective_stop(self, side: str, leg: LegState) -> None:
        pos = leg.position
        if pos is None or pos.closing:
            return
        if pos.cover_level <= 0:
            logger.warning("[%-2s] no stop level yet — watched by the engine until the next candle close sets one.", side)
            return
        trigger = to_tick(pos.cover_level, up=True)
        limit = to_tick(trigger * (1 + self.config.stop_limit_buffer_pct / 100.0), up=True)
        order_id, msg = await self.broker.place_async(
            "BUY", leg.security_id, pos.qty, "STOP_LOSS", price=limit, trigger=trigger, tag=new_tag())
        if order_id is None:
            # Usually: the stop level is already at/below the market (the
            # algorithm would exit right now anyway) — or the order was refused
            # outright. Either way a live short must not sit unprotected.
            logger.critical("[%-2s] protective stop REJECTED (%s) — buying back now.", side, msg)
            self.ledger.log_event("STOP_REJECTED", symbol=side, details=msg)
            await self._live_close(side, leg, "STOP_REJECTED")
            return
        pos.stop_order_id, pos.stop_trigger_sent = order_id, trigger
        self._save_state()
        logger.info("[%-2s] protective stop at the exchange: BUY x%d trigger %.2f limit %.2f (order %s)",
                    side, pos.qty, trigger, limit, order_id)

    async def _sync_protective_stop(self, side: str, leg: LegState) -> None:
        """Move the exchange stop to the engine's new stop level."""
        pos = leg.position
        if pos is None or pos.closing:
            return
        if not pos.stop_order_id:
            await self._place_protective_stop(side, leg)
            return
        trigger = to_tick(pos.cover_level, up=True)
        if abs(trigger - pos.stop_trigger_sent) < 1e-9:
            return
        limit = to_tick(trigger * (1 + self.config.stop_limit_buffer_pct / 100.0), up=True)
        ok, msg = await self.broker.modify_async(pos.stop_order_id, "STOP_LOSS", pos.qty, limit, trigger)
        if ok:
            pos.stop_trigger_sent = trigger
            self._save_state()
            logger.info("[%-2s] stop moved to %.2f (limit %.2f)", side, trigger, limit)
        else:
            st = await self.broker.status_async(pos.stop_order_id)
            if st.status in ("PENDING", "TRANSIT") and st.filled_qty == 0:
                # Still resting but refused the change — typically Dhan's cap on
                # modifications per order, which a long trade (a stop move every
                # candle) can reach. Replace it with a fresh order at the new level.
                ok_c, _ = await self.broker.cancel_async(pos.stop_order_id)
                st2 = await self.broker.status_async(pos.stop_order_id)
                if ok_c and st2.filled_qty == 0:
                    logger.warning("[%-2s] stop move refused (%s) — replacing the stop order at %.2f.", side, msg, trigger)
                    pos.stop_order_id, pos.stop_trigger_sent = "", 0.0
                    await self._place_protective_stop(side, leg)
                    return
            # Otherwise it just filled, or the new level is already through the
            # market; _poll_live_stops / the tick check resolve that.
            logger.warning("[%-2s] could not move the exchange stop to %.2f: %s", side, trigger, msg)

    def _check_exits_live(self, side: str, leg: LegState, premium: float, squareoff_due: bool,
                          locked: bool = False) -> None:
        pos = leg.position
        if pos is None or pos.closing:
            return
        if squareoff_due:
            pos.closing = True
            self._spawn(self._live_close(side, leg, "EOD_SQUAREOFF"))
            return
        if locked and self._lock_is_tighter(pos):
            # The profit lock lives in the engine (the exchange stop order stays
            # the Heikin-Ashi stop): buy back now. _live_close cancels the
            # exchange stop first, so the account can never be bought twice.
            logger.info("[%-2s] profit lock %.2f reached at %.2f — buying back.", side, pos.lock_price, premium)
            pos.closing = True
            self._spawn(self._live_close(side, leg, "PROFIT_LOCK"))
            return
        if not pos.is_cover_level_triggered(premium):
            return
        if not pos.stop_order_id:
            pos.closing = True
            self._spawn(self._live_close(side, leg, "TRAILING_STOP"))
            return
        # The exchange stop should be filling. Give it stop_escalate_secs;
        # after that (it gapped past its limit) buy back at market ourselves.
        now = time.monotonic()
        if pos.breach_since is None:
            pos.breach_since = now
            logger.info("[%-2s] stop %.2f crossed at %.2f — exchange stop order should fill.", side, pos.cover_level, premium)
        elif now - pos.breach_since >= self.config.stop_escalate_secs:
            pos.closing = True
            self._spawn(self._live_close(side, leg, "TRAILING_STOP"))

    async def _broker_net(self, security_id: str) -> Optional[Tuple[int, float]]:
        """(net qty, buy average) Dhan shows for this contract today, or None
        if Dhan couldn't be read. A short is negative; 0 = flat."""
        positions = await self.broker.positions_async()
        if positions is None:
            return None
        for p in positions:
            if str(p.get("securityId")) == str(security_id):
                return int(float(p.get("netQty", 0) or 0)), float(p.get("buyAvg", 0) or 0)
        return 0, 0.0

    async def _closed_outside(self, side: str, leg: LegState) -> bool:
        """True (and the trade is booked) if Dhan shows this leg already flat —
        i.e. it was bought back outside the engine, by hand in the Dhan app.

        BUG FIXED: the engine only ever learned about its OWN orders. After a
        manual exit it still believed it was short, so it (a) re-placed the
        stop when it found it cancelled — a stop that, if triggered, would BUY
        65 into a flat account = an unintended LONG — and (b) at 15:00 sent a
        market BUY for the same reason. Now any buy-back or re-placed stop is
        preceded by asking Dhan whether the short still exists."""
        pos = leg.position
        got = await self._broker_net(leg.security_id)
        if pos is None or got is None:
            return False
        net, buy_avg = got
        if net != 0:
            return False
        if pos.stop_order_id:
            st = await self.broker.status_async(pos.stop_order_id)
            if st.filled_qty >= pos.qty:
                # Flat because OUR exchange stop filled — a normal stop-out,
                # booked at the stop's own fill price, not a manual exit.
                stop_id, pos.stop_order_id = pos.stop_order_id, ""
                self._exit(side, leg, st.avg_price, "TRAILING_STOP", exit_order_id=stop_id)
                return True
            if st.status in ("PENDING", "TRANSIT", "TRIGGERED"):
                await self.broker.cancel_async(pos.stop_order_id)     # it would open a LONG now
            pos.stop_order_id = ""
        price = buy_avg if buy_avg > 0 else self._last_premium.get(side, pos.cover_level)
        logger.warning("[%-2s] position was closed OUTSIDE the engine (Dhan shows it flat) — booking the "
                       "exit at Dhan's buy average %.2f; its protective stop is cancelled.", side, price)
        self._exit(side, leg, price, "CLOSED_OUTSIDE_ENGINE")
        return True

    async def _check_foreign_positions(self) -> None:
        """Compare every NSE_FNO position at Dhan with what the engine itself
        holds. Anything else — a manual trade, or a position the engine lost —
        blocks that contract for new entries and raises a CRITICAL alert once.
        The engine does NOT take over positions it didn't open: that's your
        call, and the protective stop placed at entry stays at the exchange."""
        positions = await self.broker.positions_async()
        if positions is None:
            return
        mine = {leg.security_id: -leg.position.qty for leg in self.legs.values()
                if leg.position is not None and leg.security_id}
        foreign = set()
        for p in positions:
            if str(p.get("exchangeSegment", "")).upper() != self.config.option_exchange_segment:
                continue
            sec = str(p.get("securityId"))
            net = int(float(p.get("netQty", 0) or 0))
            if net != mine.get(sec, 0):
                foreign.add(sec)
                if sec not in self._foreign_contracts:
                    ours = mine.get(sec, 0)
                    logger.critical(
                        "Dhan shows net %d in security %s (%s) but the engine holds %d there — a position the "
                        "engine didn't open, or one it lost track of. That contract is blocked for new entries; "
                        "check the Dhan app.", net, sec, p.get("tradingSymbol", "?"), ours)
                    self.ledger.log_event("FOREIGN_POSITION", details=f"security={sec} dhan_net={net} engine={ours}")
        self._foreign_contracts = foreign

    async def _poll_live_stops(self) -> None:
        """Once a second: did an exchange stop fill? Did one vanish? Does a
        crossed stop need escalating even though no new tick has arrived?
        Every few seconds: is the position still open at Dhan at all?"""
        check_positions = time.monotonic() - self._positions_checked_at >= _POSITION_CHECK_SECS
        if check_positions:
            self._positions_checked_at = time.monotonic()
            await self._check_foreign_positions()
        for side in _LEGS:
            leg = self.legs[side]
            pos = leg.position
            if pos is None or pos.closing:
                continue
            if check_positions and await self._closed_outside(side, leg):
                continue
            if pos.stop_order_id:
                st = await self.broker.status_async(pos.stop_order_id)
                if st.status == "TRADED" and st.filled_qty >= pos.qty:
                    stop_id = pos.stop_order_id
                    pos.stop_order_id = ""
                    self._exit(side, leg, st.avg_price, "TRAILING_STOP", exit_order_id=stop_id)
                    continue
                if st.status in ("REJECTED", "CANCELLED", "EXPIRED"):
                    pos.stop_order_id, pos.stop_trigger_sent = "", 0.0
                    if st.filled_qty > 0:
                        self._exit(side, leg, st.avg_price, "TRAILING_STOP", qty=st.filled_qty)
                    # Cancelled by hand usually means closed by hand: never
                    # re-place a BUY stop into a position that's already flat.
                    if leg.position is not None and await self._closed_outside(side, leg):
                        continue
                    if leg.position is not None:
                        logger.critical("[%-2s] exchange stop is %s (%s) but Dhan still shows the short — "
                                        "re-placing it.", side, st.status, st.message)
                        await self._place_protective_stop(side, leg)
                    continue
            if (pos.breach_since is not None
                    and time.monotonic() - pos.breach_since >= self.config.stop_escalate_secs):
                pos.closing = True
                await self._live_close(side, leg, "TRAILING_STOP")

    async def _live_close(self, side: str, leg: LegState, reason: str) -> None:
        """Buy the leg back for real. Cancels the exchange stop FIRST and
        accounts for anything it already filled, so the account can never end
        up bought twice (net long)."""
        pos = leg.position
        if pos is None:
            return
        pos.closing = True
        try:
            remaining = pos.qty
            if pos.stop_order_id:
                ok, msg = await self.broker.cancel_async(pos.stop_order_id)
                st = await self.broker.status_async(pos.stop_order_id)
                if st.filled_qty > 0:
                    stop_id = pos.stop_order_id
                    filled = min(st.filled_qty, pos.qty)
                    pos.stop_order_id = "" if filled >= pos.qty else pos.stop_order_id
                    self._exit(side, leg, st.avg_price, "TRAILING_STOP", exit_order_id=stop_id, qty=filled)
                    if leg.position is None:
                        return
                    remaining = leg.position.qty
                if st.status not in ("CANCELLED", "TRADED", "REJECTED", "EXPIRED"):
                    # The stop is still live and couldn't be cancelled: sending
                    # a market buy now could double the buy-back. Retry shortly.
                    logger.critical("[%-2s] could not cancel exchange stop %s (%s: %s) — will retry.",
                                    side, pos.stop_order_id, st.status, msg)
                    return
                pos.stop_order_id = ""
            if await self._closed_outside(side, leg):
                return                     # already flat at Dhan: a BUY now would make it LONG
            ref = self._last_premium.get(side, pos.cover_level)
            res = await self._live_order("BUY", side, leg, remaining, ref_price=ref)
            if res.filled_qty > 0:
                self._exit(side, leg, res.avg_price, reason, exit_order_id=res.order_id, qty=res.filled_qty)
            if leg.position is not None:
                logger.critical("[%-2s] LIVE BUY-BACK INCOMPLETE (%s: %s) — %d still short. Retrying; "
                                "check the Dhan app.", side, res.status, res.message, leg.position.qty)
        finally:
            if leg.position is not None:
                leg.position.closing = False          # allow the next cycle to retry

    async def _reconcile_live(self) -> None:
        """At startup, compare what this engine thinks is open with what Dhan
        says is open, before anything is traded."""
        positions = await self.broker.positions_async()
        if positions is None:
            logger.critical("Could not read positions from Dhan — refusing to trade until it can. "
                            "(Token / Trading API access / network.)")
            self.stop()
            return
        broker = {str(p.get("securityId")): int(float(p.get("netQty", 0) or 0)) for p in positions
                  if str(p.get("exchangeSegment", "")).upper() == self.config.option_exchange_segment}
        buy_avg = {str(p.get("securityId")): float(p.get("buyAvg", 0) or 0) for p in positions}
        known = set()
        for side in _LEGS:
            leg = self.legs[side]
            pos = leg.position
            if pos is None or not leg.security_id:
                continue
            known.add(leg.security_id)
            net = broker.get(leg.security_id, 0)
            if net == -pos.qty:
                logger.info("[%-2s] live position confirmed at Dhan: short %d. Resuming.", side, pos.qty)
                if pos.stop_order_id:
                    st = await self.broker.status_async(pos.stop_order_id)
                    if st.status not in ("PENDING", "TRANSIT", "TRIGGERED", "PART_TRADED"):
                        pos.stop_order_id = ""
                if not pos.stop_order_id:
                    await self._place_protective_stop(side, leg)
            elif net == 0:
                st = await self.broker.status_async(pos.stop_order_id) if pos.stop_order_id else None
                # BUG FIXED: a hand-closed position used to be booked at the
                # engine's STOP LEVEL (e.g. 131.30) instead of the real price
                # it was bought back at (123.60). Dhan's buy average for the
                # contract is the actual price.
                price = (buy_avg.get(leg.security_id) or 0.0) or (st.avg_price if st and st.filled_qty else 0.0) \
                    or pos.cover_level
                if st and st.status in ("PENDING", "TRANSIT", "TRIGGERED"):
                    await self.broker.cancel_async(pos.stop_order_id)   # flat now: it could only open a LONG
                logger.warning("[%-2s] Dhan shows this position already CLOSED (stop filled or closed by hand "
                               "while the engine was down) — booking it at Dhan's price %.2f.", side, price)
                pos.stop_order_id = ""
                self._exit(side, leg, price, "CLOSED_WHILE_OFFLINE")
            else:
                logger.critical("[%-2s] Dhan shows net %d but the engine expected short %d — blocking this "
                                "contract. Fix it in the Dhan app, then restart.", side, net, pos.qty)
                self._blocked_contracts.add(leg.security_id)
        for sec, net in broker.items():
            if net != 0 and sec not in known:
                self._blocked_contracts.add(sec)
                logger.critical("Dhan shows an open NSE_FNO position the engine didn't open (security %s, "
                                "net %d). The engine will not trade that contract.", sec, net)
        funds = await self.broker.funds_async()
        if funds is not None:
            self._broker_funds, self._funds_checked_at = funds, time.monotonic()
            logger.info("Dhan available funds: Rs %.2f", funds)

    async def _kill_switch(self) -> None:
        logger.critical("KILL SWITCH (%s exists) — squaring off everything and stopping.", KILL_SWITCH_FILE)
        for side in _LEGS:
            leg = self.legs[side]
            if leg.position is None:
                continue
            if self.broker is not None:
                await self._live_close(side, leg, "KILL_SWITCH")
            else:
                self._exit(side, leg, self._last_premium.get(side, leg.position.entry_price), "KILL_SWITCH")
        self.stop()

    # ── Exits (checked every tick) ────────────────────────────────────────────

    def _check_exits(
        self, side: str, leg: LegState, premium: float, squareoff_due: bool,
        at_time: Optional[datetime] = None,
    ) -> None:
        pos = leg.position
        if pos is None:
            return
        # Checked against the lock set by EARLIER prices, then this price
        # updates it — so the tick that first touches a mark never exits on
        # itself; the exit is when price COMES BACK to the lock.
        locked = pos.lock_price > 0 and premium >= pos.lock_price - 1e-9
        self._track_profit(side, leg, premium)

        if self.broker is not None:
            self._check_exits_live(side, leg, premium, squareoff_due, locked)
            return

        if squareoff_due:
            self._exit(side, leg, premium, "EOD_SQUAREOFF", at_time=at_time)
            return
        if locked and self._lock_is_tighter(pos):
            self._exit(side, leg, premium, "PROFIT_LOCK", at_time=at_time)
            return

        # The stop LEVEL is Heikin-Ashi (see position.trailing_exit_level);
        # the FILL is the tick that crossed it. BUG FIXED: the fill used to
        # be the level itself, even when price jumped straight past it — a
        # stop-loss order fills where the market is, not where you wished
        # it had stopped. Filling at the level understated losses on every
        # gap through the stop (~Rs 1,375 over 8 real days).
        if pos.is_cover_level_triggered(premium):
            self._exit(side, leg, max(premium, pos.cover_level), "TRAILING_STOP", at_time=at_time)
            return

    @staticmethod
    def _lock_is_tighter(pos: OpenPosition) -> bool:
        """Price rising back hits the LOWER of the two exit levels first. When
        the Heikin-Ashi stop is lower, it's the one that closes the trade."""
        return pos.cover_level <= 0 or pos.lock_price <= pos.cover_level + 1e-9

    def _track_profit(self, side: str, leg: LegState, price: float) -> None:
        """Follow the lowest premium since entry and move the profit lock."""
        pos = leg.position
        if pos is None or not self.config.profit_lock or price <= 0:
            return
        if pos.best_price > 0 and price >= pos.best_price:
            return
        pos.best_price = price
        lock = profit_lock_level(pos.entry_price, price, self.config.profit_lock_start_pts,
                                 self.config.profit_lock_step_pts)
        if lock is None or (pos.lock_price > 0 and lock >= pos.lock_price - 1e-9):
            return
        pos.lock_price = lock
        logger.info("[%-2s] profit %.2f pts (premium %.2f) — locked %.2f pts: buy back if it comes back to %.2f",
                    side, pos.entry_price - price, price, pos.entry_price - lock, lock)
        self.ledger.log_event("PROFIT_LOCK_SET", symbol=side,
                              details=f"entry={pos.entry_price:.2f} best={price:.2f} lock={lock:.2f}")
        self._save_state()

    def _exit(
        self, side: str, leg: LegState, exit_price: float, reason: str,
        at_time: Optional[datetime] = None, exit_order_id: Optional[str] = None,
        qty: Optional[int] = None,
    ) -> None:
        """Close `qty` (default: all) of the leg's position at `exit_price`.
        A partial close (live buy-back that didn't fully fill) books that part
        as its own trade row and leaves the rest open."""
        pos = leg.position
        if pos is None:
            return
        closed = pos.qty if qty is None else max(0, min(int(qty), pos.qty))
        if closed <= 0:
            return
        partial = closed < pos.qty
        gross_pnl = round(pos.rules.pnl(pos.entry_price, exit_price, closed), 2)

        # Brokerage, STT, exchange/SEBI fees, stamp duty and GST on BOTH legs.
        # Dominated by the flat per-order fee, so at 1 lot a round trip costs
        # ~Rs 50 no matter how far price moved — which is the difference
        # between a small paper "win" and a real loss. See charges.py.
        charges = 0.0
        charge_note = ""
        if self.config.apply_charges:
            entry_c, exit_c, charges = round_trip_charges(
                pos.entry_price, exit_price, closed, self._charge_rates,
            )
            charge_note = f" | entry[{entry_c.breakdown()}] exit[{exit_c.breakdown()}]"
        net_pnl = round(gross_pnl - charges, 2)

        self.realized_pnl += net_pnl
        self.balance += net_pnl
        exit_order_id = exit_order_id or f"PAPER-{uuid.uuid4().hex[:10]}"
        exit_time = at_time or datetime.now(timezone.utc)

        trade_row = dict(
            trade_id=pos.trade_id if not partial else f"{pos.trade_id}-p{uuid.uuid4().hex[:4]}",
            symbol=side, strike=leg.strike or 0.0,
            entry_price=pos.entry_price, exit_price=exit_price, qty=closed,
            pnl=net_pnl, entry_order_id=pos.entry_order_id, exit_order_id=exit_order_id,
            entry_time=pos.entry_time, exit_time=exit_time, exit_reason=reason,
            gross_pnl=gross_pnl, charges=charges,
        )
        exit_event = ("EXIT", side, f"reason={reason} gross={gross_pnl:.2f} charges={charges:.2f} "
                                    f"net={net_pnl:.2f} order={exit_order_id}{charge_note}")
        logger.info(
            "[%-2s] COVER %7.0f | %7.2f -> %7.2f x%-4d | gross Rs %+9.2f  charges Rs %6.2f  "
            "NET Rs %+9.2f | %-14s | %s",
            side, leg.strike or 0.0, pos.entry_price, exit_price, closed,
            gross_pnl, charges, net_pnl, reason,
            f"day net Rs {self.realized_pnl:+.2f}" if self.broker is not None else f"bal Rs {self.balance:11.2f}",
        )

        if partial:
            pos.qty -= closed
            logger.critical("[%-2s] PARTIAL close: %d still open — the engine keeps managing it.", side, pos.qty)
        else:
            leg.position = None
            # Either side may trade next; this side only on a fresh pattern.
            self._note_exit(side, exit_time)
        # The trade row, its event and the state that no longer holds the
        # position commit as ONE transaction. Before, a crash between them left
        # the position "open" in saved state with the trade already booked.
        self.ledger.record_exit(trade_row, exit_event, self._current_state_dict())
