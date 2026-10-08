"""
run_options.py — standalone entry point for the NIFTY options engine.

OPTIONS_PAPER_TRADING=true -> paper (simulated fills at live prices);
false -> LIVE: real orders on Dhan. Market data (option chain, expiry,
margin, candles, real-time premium ticks) always comes from DhanHQ.

Logs stream to both console AND data/options_engine.log for monitoring.

Usage:
    python run_options.py
"""
import asyncio
import atexit
import logging
import queue
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from algopilot.options.config import OptionsConfigError, load_options_config, token_expiry_ist
from algopilot.options.engine import OptionsEngine
from algopilot.utils.network import ensure_tls_trust_store, force_ipv4

_IST = ZoneInfo("Asia/Kolkata")

# Force UTF-8 on Windows console so unicode log chars don't crash the stream.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class _ShortNameFormatter(logging.Formatter):
    """Every log line names its SOURCE module (engine / dhan_client /
    websocket / ledger / run_options) — without this, the console shows
    only the message text and there's no way to tell which component
    produced any given line. Shortened to the last dotted component
    (algopilot.options.engine -> engine) so it stays scannable."""
    def format(self, record: logging.LogRecord) -> str:
        record.shortname = record.name.rsplit(".", 1)[-1]
        return super().format(record)


# ── Logging: console + rotating file ──────────────────────────────────────────
LOG_FILE = Path("data/options_engine.log")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# Milliseconds on every line: the trigger is meant to fire at hh:mm:00.000,
# and a seconds-only timestamp can't show whether it did.
_FMT = "%(asctime)s.%(msecs)03d [%(levelname)s] %(shortname)-11s: %(message)s"
_DATE_FMT = "%H:%M:%S"

_root = logging.getLogger()
_root.setLevel(logging.INFO)

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_ShortNameFormatter(_FMT, datefmt=_DATE_FMT))

from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler as _RFH
_file_handler = _RFH(str(LOG_FILE), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
_file_handler.setFormatter(_ShortNameFormatter(
    "%(asctime)s [%(levelname)s] %(shortname)-11s: %(message)s"
))

# LATENCY: logging never runs on the trading loop. Writing a line to the
# console and the file used to happen right there, inside the tick / candle-
# close / order code: 0.05 ms normally, 6-44 ms on a slow console (measured),
# and on Windows a click inside the console window (QuickEdit) blocks every
# write — which froze the WHOLE engine (ticks, stop checks, candle closes)
# until a key was pressed. Now a log call only queues the record (~0.03 ms)
# and a background thread writes it. Each destination has its own queue and
# thread, so a paused console can't hold up the log file either. Timestamps
# are taken when the line is logged, not when it's written.
_log_listeners = []
for _handler in (_console, _file_handler):
    _q = queue.SimpleQueue()
    _root.addHandler(QueueHandler(_q))
    _listener = QueueListener(_q, _handler)
    _listener.start()
    _log_listeners.append(_listener)


def _flush_logs() -> None:
    """Write out everything still queued (runs at exit, Ctrl+C included)."""
    for listener in _log_listeners:
        try:
            listener.stop()
        except Exception:
            pass


atexit.register(_flush_logs)

# Third-party libraries are noisy at INFO and add nothing here.
for _noisy in ("urllib3", "websockets", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger("run_options")

force_ipv4()
# Must run before the first HTTPS/WebSocket connection is made.
ensure_tls_trust_store()


def _high_resolution_timers() -> None:
    """Windows schedules timers on a ~15.6 ms tick by default, so a sleep can
    wake that late. timeBeginPeriod(1) asks for 1 ms for this process (reset
    automatically when it exits). No-op elsewhere."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        ctypes.WinDLL("winmm").timeBeginPeriod(1)
    except Exception:
        pass


_high_resolution_timers()


def _disable_console_quick_edit() -> None:
    """Windows: a click inside the console window starts a text selection
    (QuickEdit) that PAUSES the program's console output until a key is
    pressed. Logging no longer runs on the trading loop, so that can't freeze
    trading any more — but the console would still stop updating. Turned off
    for this window only (right-click still pastes; Mark via the window menu
    still copies). No-op elsewhere, or when there is no console."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-10)             # STD_INPUT_HANDLE
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            ENABLE_QUICK_EDIT_MODE, ENABLE_EXTENDED_FLAGS = 0x0040, 0x0080
            kernel32.SetConsoleMode(handle, (mode.value & ~ENABLE_QUICK_EDIT_MODE) | ENABLE_EXTENDED_FLAGS)
    except Exception:
        pass


_disable_console_quick_edit()


def _token_status_line(config) -> str:
    """DhanHQ tokens are valid 24h. If it lapses mid-session, the WebSocket
    gets disconnected with code 807 ("Access Token is expired") — but the
    SDK reports that via a bare print(), not our logger (see websocket.py),
    so it would never show up in data/options_engine.log. This is the
    proactive alternative: check it loudly, once, before that can happen."""
    expires = token_expiry_ist(config.access_token.get_secret())
    if expires is None:
        return f"│  {'access token':<18} not a recognisable JWT — expiry unknown"
    now = datetime.now(_IST)
    remaining = (expires - now).total_seconds() / 3600.0
    line = f"│  {'access token':<18} valid to {expires:%d-%b %H:%M IST}"
    if remaining <= 0:
        return line + "  *** EXPIRED — run: python scripts/auto_token.py ***"
    if _expires_during_session(expires, now):
        return line + "  *** DIES DURING TODAY'S SESSION — regenerate before 09:15 ***"
    if remaining < 2.0:
        return line + f"  *** {remaining * 60:.0f} min left — regenerate now ***"
    return line + f"  ({remaining:.1f}h left)"


def _expires_during_session(expires: datetime, now: datetime) -> bool:
    """A token generated at, say, 10:30 yesterday dies at 10:30 today —
    mid-session, silently killing the feed with a position possibly open.
    True when the token's expiry falls inside today's 09:15-15:30 window."""
    if expires.date() != now.date():
        return False
    open_, close_ = now.replace(hour=9, minute=15), now.replace(hour=15, minute=30)
    return open_ <= expires <= close_ and expires > now


_RULE = "─" * 78


def _section(title: str) -> str:
    return f"┌─ {title} " + "─" * max(0, 76 - len(title))


def _print_banner(config) -> None:
    tf = config.candle_timeframe_secs // 60
    if config.apply_charges:
        costs = (f"Rs {config.brokerage_per_order:.0f}/order + STT {config.stt_pct}% (sell) "
                 f"+ txn {config.exchange_txn_pct}% + GST {config.gst_pct:.0f}%")
    else:
        costs = "DISABLED — P&L shown GROSS (OPTIONS_APPLY_CHARGES=false)"

    lines = [
        "",
        "╔" + "═" * 76 + "╗",
        "║" + "  AlgoPilotX Options - NIFTY Premium-Selling".ljust(76) + "║",
        ("║" + "  PAPER TRADING - no real order is placed anywhere".ljust(76) + "║") if config.paper_trading
        else ("║" + "  *** LIVE TRADING - REAL ORDERS, REAL MONEY ***".ljust(76) + "║"),
        "╚" + "═" * 76 + "╝",
        _section("SESSION"),
        (f"│  {'capital':<18} Rs {config.paper_capital:,.0f}" if config.paper_trading
         else f"│  {'capital':<18} Dhan available funds (never more than Rs {config.paper_capital:,.0f})"),
        f"│  {'timeframe':<18} {tf}m Heikin-Ashi candles",
        f"│  {'lot size':<18} {config.lot_size} (config default — live scrip-master value wins)",
        f"│  {'max lots/trade':<18} "
        + (f"{config.max_lots_per_trade}" if config.paper_trading
           else f"{min(config.max_lots_per_trade, config.live_max_lots)} (live cap)"),
        f"│  {'sizing':<18} floor(capital / {config.max_concurrent} / live DhanHQ margin-per-lot)",
        _section("STRATEGY"),
        (f"│  {'entry':<18} HA GREEN -> RED: SELL parked at the exchange at candle 2's HA Low - "
         f"{config.entry_offset_pts:g}, fills during candle 3 (else cancelled); sell-to-open only"
         if config.entry_mode == "break" else
         f"│  {'entry':<18} HA GREEN -> RED -> RED breakout at candle 3's close, sell-to-open only"),
        f"│  {'gates':<18} pattern only — no RSI, no volume filter",
        (f"│  {'doji':<18} HA body <= {config.doji_body_pct:g}% of the candle's height = neither colour, "
         f"skipped while waiting for candle 1 / 2" if config.doji_body_pct > 0 else f"│  {'doji':<18} off"),
        (f"│  {'strikes':<18} out of the money, premium Rs {config.premium_min:g}-{config.premium_max:g} "
         f"(the farthest); kept until it leaves {config.premium_min - config.premium_buffer:g}-"
         f"{config.premium_max + config.premium_buffer:g}; never while a trade/order is open"
         if config.strike_pick == "premium" else
         f"│  {'strike band':<18} floor-hundred(spot) / +100, frozen while a leg is open"),
        f"│  {'sequencing':<18} one trade at a time; then either leg (same leg needs a fresh pattern)",
        f"│  {'entries':<18} from {config.entry_start} IST, none after {config.squareoff_at}",
        (f"│  {'fills':<18} the exchange fills the sell at its price; buy stop placed right after the fill"
         if config.entry_mode == "break" else
         f"│  {'fills':<18} market price at signal (raw close); stop fills at the tick that hits it"),
        f"│  {'exit':<18} stop = HA high 2 candles back + {config.stop_offset_pts:g} (trails each close) "
        f"+ {config.squareoff_at} square-off",
        (f"│  {'profit lock':<18} {config.profit_lock_start_pts:g} pts below entry, then every "
         f"{config.profit_lock_step_pts:g} pts (one step behind); buy back when it comes back"
         if config.profit_lock else f"│  {'profit lock':<18} off"),
        f"│  {'candle close':<18} exactly at the boundary"
        + (f" + {config.candle_close_grace_ms} ms grace" if config.candle_close_grace_ms else " (hh:mm:00.000), exchange trade time"),
        f"│  {'expiry':<18} nearest weekly" + (" — rolls to next week on expiry day" if config.roll_on_expiry_day else " (0 DTE on expiry day)"),
        *([] if config.paper_trading else [
            _section("LIVE ORDERS"),
            f"│  {'account':<18} Dhan client {config.client_id}, product INTRADAY, NSE_FNO",
            f"│  {'entry / exit':<18} {config.live_order_type}"
            + (f" (limit {config.live_limit_buffer_pct:.1f}% through the last price)" if config.live_order_type == "LIMIT" else ""),
            f"│  {'protective stop':<18} stop-limit AT THE EXCHANGE at the closer of the HA stop and the "
            f"profit lock, limit +{config.stop_limit_buffer_pct:.0f}%",
            f"│  {'escalation':<18} stop crossed but unfilled after {config.stop_escalate_secs:.0f}s -> market buy-back",
            f"│  {'max lots':<18} {config.live_max_lots} per trade (hard cap)",
            f"│  {'daily loss limit':<18} Rs {config.max_daily_loss:,.0f} net — no new trades after it",
            f"│  {'kill switch':<18} create data/KILL -> square off everything and stop",
        ]),
        _section("COSTS"),
        f"│  {'model':<18} {costs}",
        f"│  {'note':<18} rates are ESTIMATES — verify against a Dhan contract note",
        _section("DATA"),
        f"│  {'premium feed':<18} live WebSocket ticks; chain LTP as exit safety net",
        f"│  {'spot':<18} REST option-chain poll every {config.chain_min_interval_secs:.0f}s (band only)",
        _token_status_line(config),
        _section("OUTPUT"),
        f"│  {'ledger':<18} {config.db_path}  (trades + events + candle_log)",
        f"│  {'log file':<18} {LOG_FILE}",
        f"│  {'analysis':<18} python scripts/export_candle_log.py",
        _RULE,
    ]
    for line in lines:
        logger.info(line)


async def main() -> int:
    try:
        config = load_options_config()
    except OptionsConfigError as exc:
        logger.error("Options config error: %s", exc)
        return 1

    if not config.enabled:
        logger.warning("OPTIONS_ENABLED=false — nothing to do.")
        return 0

    _print_banner(config)

    from algopilot.utils.market_calendar import trading_day_status
    is_open, why = trading_day_status(datetime.now(_IST).date())
    if not is_open:
        logger.info("Market closed today — %s. Nothing to do.", why)
        return 0
    if why != "trading day":
        logger.warning(why)
    if config.auto_stop_at and datetime.now(_IST).strftime("%H:%M") >= config.auto_stop_at:
        logger.info("Market has already closed for today (after %s IST). Nothing to do.", config.auto_stop_at)
        return 0

    expires = token_expiry_ist(config.access_token.get_secret())
    from algopilot.options.engine import KILL_SWITCH_FILE
    if KILL_SWITCH_FILE.exists():
        logger.error("Kill switch file %s exists — refusing to start. Delete it to run again.", KILL_SWITCH_FILE)
        return 1

    if expires is not None and expires <= datetime.now(_IST):
        # BUG FIXED: the banner flagged an expired token but the engine ran
        # anyway — every API call then failed with 401 and it sat doing
        # nothing (no expiry list -> no chain -> no band -> no trades).
        logger.error(
            "Access token expired at %s. Refusing to start. Generate a new one "
            "(python scripts/auto_token.py) and run again.",
            f"{expires:%d-%b %H:%M IST}",
        )
        return 1

    engine = OptionsEngine(config)
    try:
        await engine.run()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Options engine crashed.")
        return 1
    finally:
        _print_session_summary(engine, config)
    return 0


def _print_session_summary(engine, config) -> None:
    """What actually happened this session, in numbers — so shutdown ends on
    a result rather than a bare 'stopped' line."""
    # BUG FIXED: this read engine.ledger, which engine.run() has already
    # closed by the time the summary prints — the read failed and every
    # session reported "none today", hiding real trades. Read through a
    # fresh handle on the same file instead.
    from algopilot.options.ledger import OptionsLedger
    try:
        ledger = OptionsLedger(config.db_path, readonly=True)
        try:
            trades = ledger.get_today_trades()
        finally:
            ledger.close()
    except Exception:
        logger.exception("Session summary: could not read today's trades.")
        trades = []

    logger.info("")
    logger.info(_section("SESSION SUMMARY"))
    if trades:
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        # `is None`, not `or`: a trade with exactly Rs 0.00 gross is falsy and
        # used to fall back to the NET figure, miscounting the gross total.
        gross = sum(t["pnl"] if t.get("gross_pnl") is None else t["gross_pnl"] for t in trades)
        charges = sum(t.get("charges") or 0.0 for t in trades)
        net = sum(t["pnl"] for t in trades)
        logger.info(f"│  {'trades closed':<18} {len(trades)}   ({wins} win / {losses} loss)")
        logger.info(f"│  {'gross P&L':<18} Rs {gross:+,.2f}")
        logger.info(f"│  {'charges':<18} Rs {charges:,.2f}")
        logger.info(f"│  {'NET P&L':<18} Rs {net:+,.2f}")
        if engine.broker is None:
            logger.info(f"│  {'closing balance':<18} Rs {engine.balance:,.2f}  (paper)")
    else:
        logger.info(f"│  {'trades closed':<18} none today")

    open_legs = [s for s, leg in engine.legs.items() if leg.position is not None]
    if open_legs:
        for side in open_legs:
            p = engine.legs[side].position
            logger.info(f"│  {'STILL OPEN':<18} {side} {engine.legs[side].strike:.0f} "
                        f"entry {p.entry_price:.2f} x{p.qty}, stop {p.cover_level:.2f}")
        logger.info(f"│  {'':<18} restart to resume managing it — state is saved")
    late = sum(leg.candle_builder.dropped_out_of_order_ticks for leg in engine.legs.values())
    if late:
        logger.info(f"│  {'late ticks':<18} {late} excluded — traded before a boundary, arrived after it closed")
    logger.info(f"│  {'log file':<18} {LOG_FILE.resolve()}")
    logger.info(f"│  {'analysis':<18} python scripts/export_candle_log.py")
    logger.info(_RULE)


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        logger.warning("Aborted by user (Ctrl+C).")
        sys.exit(130)
