"""
scripts/daily_pipeline.py — start it once; it trades every trading day by itself.

    python scripts/daily_pipeline.py              run forever (Ctrl+C to stop)
    python scripts/daily_pipeline.py --once       only today's session
    python scripts/daily_pipeline.py --install    also start it automatically when you sign in to Windows
    python scripts/daily_pipeline.py --uninstall  undo --install

Running forever:
  * every night at 21:00 IST — a new access token (PIN + TOTP, see auto_token.py),
    checked with Dhan, written to .env. Dhan has no "revoke" call: the old token
    is simply replaced and expires by itself. A token lasts 24 h, so tonight's
    covers tomorrow's session. If the VM was off at 21:00, one is made before
    the session instead.
  * every trading day from 08:45 IST — the session (run_day):
      token valid for today? -> start the engine (python run_options.py)
      09:17-09:25: did NIFTY trade? if not (a closure missing from the holiday list), stop the engine
      engine crashed before the close? restart it (max 5; never with data/KILL); open positions resume
      the engine stops by itself after the close (15:31)
  * weekends / NSE holidays (algopilot/utils/market_calendar.py) — nothing.

Logged to data/pipeline.log (and the console). Only one copy can run at a time.
"""
import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, time as dtime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from algopilot.utils.network import ensure_tls_trust_store, force_ipv4

force_ipv4()
ensure_tls_trust_store()

from dotenv import dotenv_values

from algopilot.options.auto_token import TokenError, ensure_fresh_token
from algopilot.utils.market_calendar import trading_day_status

IST = ZoneInfo("Asia/Kolkata")
KILL_FILE = Path("data/KILL")
STOP_FILE = Path("data/STOP")
LOG_FILE = Path("data/pipeline.log")
LOCK_FILE = Path("data/pipeline.lock")
STARTUP_FILE = Path(os.environ.get("APPDATA", "")) / r"Microsoft\Windows\Start Menu\Programs\Startup\AlgoPilotX.cmd"

NIGHT_TOKEN_AT = dtime(21, 0)
SESSION_FROM = dtime(8, 45)
OPEN_CHECK_FROM = dtime(9, 17)
OPEN_CHECK_GIVE_UP = dtime(9, 25)
DAY_END = dtime(15, 40)
MAX_RESTARTS = 5
RESTART_DELAY_SECS = 15
POLL_SECS = 5
LOOP_SECS = 30
TOKEN_RETRY_SECS = 600

log = logging.getLogger("pipeline")


def now_ist() -> datetime:
    return datetime.now(IST)


# ── one trading session ─────────────────────────────────────────────────────

def market_opened_today(now: datetime):
    """True: NIFTY has traded today. False: nothing yet. None: couldn't ask."""
    try:
        from algopilot.options.dhan_client import OptionsDhanClient
        from algopilot.utils.secrets import SecretStr
        env = dotenv_values(".env")              # read fresh; never via os.environ (see _engine_env)
        client = OptionsDhanClient(str(env.get("DHAN_CLIENT_ID", "")), SecretStr(str(env.get("DHAN_ACCESS_TOKEN", ""))))
        try:
            day = now.strftime("%Y-%m-%d")
            rows = client.get_intraday_candles("13", "IDX_I", "INDEX", 1, f"{day} 09:00:00",
                                               now.strftime("%Y-%m-%d %H:%M:%S"))
        finally:
            client.close()
    except Exception as exc:                     # noqa: BLE001 — a failed check must not end the day
        log.warning("Market-open check failed (%s) — will try again.", type(exc).__name__)
        return None
    if rows is None:
        return None
    return any(datetime.fromtimestamp(r["timestamp"], tz=IST).date() == now.date() for r in rows)


def _engine_env() -> dict:
    """The engine reads .env itself; no DHAN_* value inherited by this process
    (an older token) may override the file the nightly refresh just wrote."""
    env = os.environ.copy()
    for key in ("DHAN_ACCESS_TOKEN", "DHAN_CLIENT_ID"):
        env.pop(key, None)
    return env


def run_day(popen=subprocess.Popen, now=now_ist, sleep=time.sleep, opened=market_opened_today,
            token=ensure_fresh_token) -> int:
    today = now()
    is_open, why = trading_day_status(today.date())
    if not is_open:
        log.info("No trading today (%s) — nothing to do.", why)
        return 0
    if why != "trading day":
        log.warning(why)
    if KILL_FILE.exists():
        log.error("%s exists — not trading today. Delete it to allow trading.", KILL_FILE)
        return 1
    if today.time() >= DAY_END:
        log.info("The market has already closed today — nothing to do.")
        return 0
    if STOP_FILE.exists():
        STOP_FILE.unlink()                       # a leftover stop request must not stop today's engine

    try:
        log.info("Token: %s", token())
    except TokenError as exc:
        log.critical("NO valid access token — the engine is NOT started today. %s", exc)
        return 2

    restarts, open_checked = 0, False
    while True:
        started = time.monotonic()
        proc = popen([sys.executable, "run_options.py"], cwd=str(ROOT), env=_engine_env())
        log.info("Engine started (pid %s).", proc.pid)
        try:
            while proc.poll() is None:
                t = now()
                if not open_checked and t.time() >= OPEN_CHECK_FROM:
                    seen = opened(t)
                    if seen:
                        open_checked = True
                        log.info("Market is open — NIFTY has traded today.")
                    elif seen is False and t.time() >= OPEN_CHECK_GIVE_UP:
                        open_checked = True
                        log.warning("NIFTY has not traded by %s — the market looks closed today (not in the "
                                    "holiday list?). Stopping the engine.", OPEN_CHECK_GIVE_UP.strftime("%H:%M"))
                        STOP_FILE.parent.mkdir(parents=True, exist_ok=True)
                        STOP_FILE.touch()
                sleep(POLL_SECS)
        except KeyboardInterrupt:
            log.warning("Ctrl+C — letting the engine stop cleanly.")
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
            raise

        code = proc.returncode
        ran = time.monotonic() - started
        if code == 0:
            log.info("Engine finished for the day (ran %.0f min).", ran / 60)
            return 0
        if code == 130:
            log.info("Engine stopped by Ctrl+C.")
            return 130
        if KILL_FILE.exists():
            log.error("Engine stopped (exit %s) and %s exists — not restarting.", code, KILL_FILE)
            return code
        if now().time() >= DAY_END:
            log.warning("Engine exited with code %s after the close — not restarting.", code)
            return code
        restarts += 1
        if restarts > MAX_RESTARTS:
            log.critical("Engine keeps stopping (exit %s, %d restarts) — giving up for today. "
                         "Check data/options_engine.log.", code, MAX_RESTARTS)
            return code
        log.warning("Engine exited with code %s after %.0f s — restarting in %d s (restart %d of %d); "
                    "an open position resumes from saved state.", code, ran, RESTART_DELAY_SECS, restarts, MAX_RESTARTS)
        sleep(RESTART_DELAY_SECS)
        try:
            log.info("Token: %s", token())
        except TokenError as exc:
            log.critical("NO valid access token for the restart — stopping for today. %s", exc)
            return 2


# ── forever ─────────────────────────────────────────────────────────────────

def run_forever(now=now_ist, sleep=time.sleep, day=run_day, token=ensure_fresh_token, clock=time.monotonic,
                max_loops=None) -> int:
    """Nightly token at 21:00, a session every trading day from 08:45."""
    token_done = session_done = None             # the date each was last done
    retry_token_at = 0.0
    loops = 0
    log.info("Running every trading day: token every night at %s, session from %s IST. Ctrl+C to stop.",
             NIGHT_TOKEN_AT.strftime("%H:%M"), SESSION_FROM.strftime("%H:%M"))
    while max_loops is None or loops < max_loops:
        loops += 1
        t = now()
        if t.time() >= NIGHT_TOKEN_AT and token_done != t.date() and clock() >= retry_token_at:
            try:
                log.info("Nightly token: %s", token(force=True))
                token_done = t.date()
            except TokenError as exc:
                log.error("Nightly token failed: %s — retrying in %d min.", exc, TOKEN_RETRY_SECS // 60)
                retry_token_at = clock() + TOKEN_RETRY_SECS
        if SESSION_FROM <= t.time() < DAY_END and session_done != t.date():
            session_done = t.date()
            log.info("──── %s ────", t.strftime("%A %d-%b-%Y"))
            day()
            log.info("Session over. Next: token at %s, next session from %s on the next trading day.",
                     NIGHT_TOKEN_AT.strftime("%H:%M"), SESSION_FROM.strftime("%H:%M"))
        sleep(LOOP_SECS)
    return 0


# ── setup helpers ───────────────────────────────────────────────────────────

def _single_instance():
    """Hold a lock for as long as this process runs; None if another copy holds it."""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    f = open(LOCK_FILE, "a+")
    try:
        if os.name == "nt":
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.close()
        return None
    return f


def install() -> int:
    """Start the pipeline when you sign in to Windows (after a reboot, say)."""
    STARTUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    STARTUP_FILE.write_text(f'@echo off\r\ntitle AlgoPilotX\r\n"{sys.executable}" "{Path(__file__).resolve()}"\r\n',
                            encoding="utf-8")
    print(f"Installed: {STARTUP_FILE}\nIt starts the pipeline every time you sign in to Windows.")
    return 0


def uninstall() -> int:
    if STARTUP_FILE.exists():
        STARTUP_FILE.unlink()
        print(f"Removed: {STARTUP_FILE}")
    else:
        print("Nothing to remove.")
    return 0


def _setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] pipeline   : %(message)s")
    fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(ch)
    log.setLevel(logging.INFO)
    log.propagate = False


def main() -> int:
    ap = argparse.ArgumentParser(description="Start once; trades every trading day by itself.")
    ap.add_argument("--once", action="store_true", help="only today's session, then exit")
    ap.add_argument("--install", action="store_true", help="also start automatically when you sign in to Windows")
    ap.add_argument("--uninstall", action="store_true", help="undo --install")
    args = ap.parse_args()
    if args.install:
        return install()
    if args.uninstall:
        return uninstall()

    _setup_logging()
    lock = _single_instance()
    if lock is None:
        log.error("The pipeline is already running (another window?). Not starting a second copy.")
        return 1
    log.info("──── pipeline started ────")
    try:
        code = run_day() if args.once else run_forever()
    except KeyboardInterrupt:
        code = 130
    log.info("──── pipeline stopped (exit %s) ────", code)
    lock.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
