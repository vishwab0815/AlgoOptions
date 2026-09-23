"""
run_options.py — standalone entry point for the NIFTY options engine.

Pure paper trading: market data (option chain, expiry, margin, and
real-time premium ticks) comes from the live DhanHQ API; no order is ever
placed anywhere in this codebase.

Logs stream to both console AND data/options_engine.log for monitoring.

Usage:
    python run_options.py
"""
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from algopilot.options.config import OptionsConfigError, load_options_config, token_expiry_ist
from algopilot.options.engine import OptionsEngine
from algopilot.utils.network import force_ipv4

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

_FMT = "%(asctime)s [%(levelname)s] %(shortname)-11s: %(message)s"
_DATE_FMT = "%H:%M:%S"

_root = logging.getLogger()
_root.setLevel(logging.INFO)

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_ShortNameFormatter(_FMT, datefmt=_DATE_FMT))
_root.addHandler(_console)

from logging.handlers import RotatingFileHandler as _RFH
_file_handler = _RFH(str(LOG_FILE), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
_file_handler.setFormatter(_ShortNameFormatter(
    "%(asctime)s [%(levelname)s] %(shortname)-11s: %(message)s"
))
_root.addHandler(_file_handler)

# Third-party libraries are noisy at INFO and add nothing here.
for _noisy in ("urllib3", "websockets", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger("run_options")

force_ipv4()


def _token_status_line(config) -> str:
    """DhanHQ tokens are valid 24h. If it lapses mid-session, the WebSocket
    gets disconnected with code 807 ("Access Token is expired") — but the
    SDK reports that via a bare print(), not our logger (see websocket.py),
    so it would never show up in data/options_engine.log. This is the
    proactive alternative: check it loudly, once, before that can happen."""
    expires = token_expiry_ist(config.access_token.get_secret())
    if expires is None:
        return "  access token     : not a recognisable JWT — expiry unknown"
    now = datetime.now(_IST)
    remaining = (expires - now).total_seconds() / 3600.0
    line = f"  access token      : valid until {expires:%Y-%m-%d %H:%M IST}"
    if remaining <= 0:
        return line + "   *** ALREADY EXPIRED — regenerate with scripts/generate_token.py ***"
    if remaining < 2.0:
        return line + f"   *** expires in {remaining * 60:.0f} min — regenerate before that ***"
    return line + f"  ({remaining:.1f}h remaining)"


def _print_banner(config) -> None:
    lines = [
        "=" * 74,
        "  AlgoPilotX Options — NIFTY Premium-Selling — PAPER TRADING",
        "=" * 74,
        _token_status_line(config),
        f"  paper capital     : Rs {config.paper_capital:,.0f}",
        f"  candle timeframe  : {config.candle_timeframe_secs // 60}m",
        f"  strike band       : floor-hundred(spot) / +100 — frozen while a leg is open",
        f"  pattern           : Heikin-Ashi GREEN->RED->RED, sell-to-open only, per leg",
        f"  entry gate        : pattern only — no RSI, no volume, no ratchet",
        f"  sequencing        : watch both legs, first match wins, then locked to that side",
        f"  premium feed      : real-time WebSocket ticks (spot polled over REST for the band)",
        f"  lot size          : {config.lot_size} (config default; live value preferred once resolved)",
        f"  max lots/trade    : {config.max_lots_per_trade}",
        f"  sizing            : floor(capital / max_concurrent / live DhanHQ margin-per-lot)",
        f"  exit              : Heikin-Ashi trailing stop (1 bar back) + 15:00 IST EOD square-off",
        f"  ledger (SQLite)   : {config.db_path}",
        f"  log file          : {LOG_FILE.resolve()}",
        "=" * 74,
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

    engine = OptionsEngine(config)
    try:
        await engine.run()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Options engine crashed.")
        return 1
    finally:
        logger.info("Options engine stopped. Log saved to: %s", LOG_FILE.resolve())
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        logger.warning("Aborted by user (Ctrl+C).")
        sys.exit(130)
