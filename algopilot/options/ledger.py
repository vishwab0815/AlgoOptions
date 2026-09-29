"""
algopilot/options/ledger.py — the engine's SQLite ledger.

Tables: `trades` (completed round-trips, NET P&L plus gross and charges),
`events` (every entry/exit/skip/order decision), `engine_state` (one JSON row:
band, expiry, side lock, any open position — for restart recovery) and
`candle_log` (one row per candle per leg, for analysis).

How it stays FAST and ACCURATE — each point fixes a failure reproduced on the
previous version:

  1. The engine never waits on the disk. Writes are handed to a background
     writer thread (microseconds); only that thread touches the database.
     Before, a write while any other program held the file (a DB viewer, a
     script mid-write) FROZE the engine for the 5 s busy-timeout — no ticks,
     no stop checks, no candle closes — and then silently DROPPED the write.

  2. Nothing is lost if the process dies. Every write is first appended to a
     journal file (data/<db>.journal); the writer marks it done once committed,
     and anything not marked done is replayed on the next start. A write that
     can't be committed because the database is held is retried until it can,
     never dropped.

  3. An exit is one transaction. The trade row, its event and the new state
     ("position closed") commit together or not at all. Before, a crash
     between them left the position "open" in saved state with the trade
     already booked — the restart resumed it and could book it twice.

  4. Every timestamp is stored in UTC. The live path wrote UTC and the startup
     backfill wrote IST — the same candle as two different strings, which the
     duplicate guard couldn't see, so a restart duplicated candle rows.

  5. Readers never write. OptionsLedger(path, readonly=True) opens the file
     read-only (the export script, the session summary); opening a reader used
     to run CREATE/DELETE statements against the live database.

The writer uses synchronous=FULL (every commit reaches disk) — affordable
because it runs off the engine's thread.
"""
from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")

_CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        TEXT NOT NULL UNIQUE,
    trading_day     TEXT NOT NULL,
    leg             TEXT NOT NULL,        -- CE | PE
    strike          REAL NOT NULL,
    entry_price     REAL NOT NULL,
    exit_price      REAL NOT NULL,
    qty             INTEGER NOT NULL,
    pnl             REAL NOT NULL,        -- NET, after charges
    gross_pnl       REAL,                 -- before charges
    charges         REAL,                 -- brokerage + taxes, both legs
    exit_reason     TEXT NOT NULL,
    entry_order_id  TEXT NOT NULL,
    exit_order_id   TEXT NOT NULL,
    entry_time      TEXT NOT NULL,        -- UTC ISO-8601
    exit_time       TEXT NOT NULL         -- UTC ISO-8601
);
"""

_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL,            -- UTC ISO-8601
    trading_day TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    symbol      TEXT,
    details     TEXT
);
"""

# Single row (id=1). trading_day guards against restoring YESTERDAY's
# band/position into a fresh day — see load_state().
_CREATE_ENGINE_STATE = """
CREATE TABLE IF NOT EXISTS engine_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    trading_day TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    state_json  TEXT NOT NULL
);
"""

_CREATE_CANDLE_LOG = """
CREATE TABLE IF NOT EXISTS candle_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    trading_day   TEXT NOT NULL,
    leg           TEXT NOT NULL,        -- CE | PE
    strike        REAL,
    candle_start  TEXT NOT NULL,        -- UTC ISO-8601
    candle_end    TEXT NOT NULL,        -- UTC ISO-8601
    source        TEXT NOT NULL,        -- live | backfill | synthetic
    ha_open       REAL NOT NULL,
    ha_high       REAL NOT NULL,
    ha_low        REAL NOT NULL,
    ha_close      REAL NOT NULL,
    color         TEXT NOT NULL,        -- GREEN | RED
    stage_before  TEXT NOT NULL,
    stage_after   TEXT NOT NULL,
    target_level  REAL,
    signal        TEXT NOT NULL         -- SELL | NONE
);
"""

_SQL_TRADE = (
    "INSERT OR IGNORE INTO trades (trade_id, trading_day, leg, strike, entry_price, exit_price, "
    "qty, pnl, gross_pnl, charges, exit_reason, entry_order_id, exit_order_id, entry_time, exit_time) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
_SQL_EVENT = "INSERT INTO events (created_at, trading_day, event_type, symbol, details) VALUES (?, ?, ?, ?, ?)"
_SQL_STATE = (
    "INSERT INTO engine_state (id, trading_day, updated_at, state_json) VALUES (1, ?, ?, ?) "
    "ON CONFLICT(id) DO UPDATE SET trading_day=excluded.trading_day, "
    "updated_at=excluded.updated_at, state_json=excluded.state_json"
)
_SQL_CANDLE = (
    "INSERT OR IGNORE INTO candle_log (trading_day, leg, strike, candle_start, candle_end, source, "
    "ha_open, ha_high, ha_low, ha_close, color, stage_before, stage_after, target_level, signal) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

# A write that can't commit because another program holds the database is
# retried (never dropped). It warns once it has been waiting this long.
_BLOCKED_WARN_SECS = 10.0

Op = List[Tuple[str, Sequence[Any]]]      # one transaction: [(sql, params), ...]


class OptionsLedger:
    def __init__(self, db_path: str = "data/options_ledger.db", readonly: bool = False) -> None:
        self._path = Path(db_path)
        self.readonly = readonly
        self._read_lock = threading.Lock()

        if readonly:
            self._reader = sqlite3.connect(f"file:{self._path.as_posix()}?mode=ro", uri=True,
                                           check_same_thread=False)
            self._reader.execute("PRAGMA busy_timeout=2000;")
            self._writer_thread = None
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._journal_path = self._path.with_name(self._path.name + ".journal")

        # Writer connection: used ONLY by the writer thread after __init__.
        self._wconn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._wconn.execute("PRAGMA journal_mode=WAL;")
        self._wconn.execute("PRAGMA synchronous=FULL;")
        self._wconn.execute("PRAGMA busy_timeout=1000;")
        with self._wconn:
            for ddl in (_CREATE_TRADES, _CREATE_EVENTS, _CREATE_ENGINE_STATE, _CREATE_CANDLE_LOG):
                self._wconn.execute(ddl)
            self._wconn.execute("CREATE INDEX IF NOT EXISTS ix_trades_day ON trades(trading_day)")
            self._wconn.execute("CREATE INDEX IF NOT EXISTS ix_events_day ON events(trading_day)")
            self._wconn.execute("CREATE INDEX IF NOT EXISTS ix_candle_log_day_leg ON candle_log(trading_day, leg, candle_end)")
            self._migrate_trades_columns()
            self._normalize_times_to_utc()
            self._dedupe_candle_log()
            self._wconn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_candle_log_candle "
                "ON candle_log(trading_day, leg, strike, candle_end)"
            )
        self._replay_journal()

        # Reader connection (WAL: reads never block the writer, or vice versa).
        self._reader = sqlite3.connect(str(self._path), check_same_thread=False)
        self._reader.execute("PRAGMA busy_timeout=2000;")

        self._journal = self._journal_path.open("a", encoding="utf-8")
        self._seq_lock = threading.Lock()
        self._seq = 0
        self._q: "queue.Queue[Optional[Tuple[int, Op]]]" = queue.Queue()
        self._pending = 0
        self._pending_cv = threading.Condition()
        self._writer_thread = threading.Thread(target=self._writer_loop, name="ledger-writer", daemon=True)
        self._writer_thread.start()
        logger.info("OptionsLedger ready at %s", self._path.resolve())

    # ── public write API (never blocks on the database) ──────────────────────

    def log_trade(
        self, trade_id: str, symbol: str, strike: float, entry_price: float, exit_price: float,
        qty: int, pnl: float, entry_order_id: str, exit_order_id: str,
        entry_time: datetime, exit_time: datetime, exit_reason: str,
        gross_pnl: Optional[float] = None, charges: float = 0.0,
    ) -> None:
        self._submit([self._trade_stmt(trade_id, symbol, strike, entry_price, exit_price, qty, pnl,
                                       entry_order_id, exit_order_id, entry_time, exit_time,
                                       exit_reason, gross_pnl, charges)])

    def log_event(self, event_type: str, symbol: Optional[str] = None, details: Optional[str] = None) -> None:
        self._submit([self._event_stmt(event_type, symbol, details)])

    def save_state(self, state: Dict[str, Any]) -> None:
        self._submit([self._state_stmt(state)])

    def record_exit(self, trade: Dict[str, Any], event: Tuple[str, Optional[str], Optional[str]],
                    state: Dict[str, Any]) -> None:
        """Trade row + its event + the new state, as ONE transaction."""
        self._submit([self._trade_stmt(**trade), self._event_stmt(*event), self._state_stmt(state)])

    def record_entry(self, event: Tuple[str, Optional[str], Optional[str]], state: Dict[str, Any]) -> None:
        """Entry event + the state that now holds the position, as ONE transaction."""
        self._submit([self._event_stmt(*event), self._state_stmt(state)])

    def log_candle(
        self, leg: str, strike: Optional[float], candle_start: datetime, candle_end: datetime,
        source: str, ha_open: float, ha_high: float, ha_low: float, ha_close: float, color: str,
        stage_before: str, stage_after: str, target_level: Optional[float], signal: str,
    ) -> None:
        # OR IGNORE on (day, leg, strike, candle_end): a backfill replays candles
        # already recorded live; first write wins, so a live row is never
        # replaced by a later replay of the same candle.
        self._submit([(_SQL_CANDLE, (
            _ist_day(candle_end), leg, strike, _utc_iso(candle_start), _utc_iso(candle_end), source,
            ha_open, ha_high, ha_low, ha_close, color, stage_before, stage_after, target_level, signal,
        ))])

    # ── public read API ─────────────────────────────────────────────────────

    def flush(self, timeout: float = 30.0) -> bool:
        """Wait until every write handed over so far is committed. Reads call
        this first so they always see the engine's latest writes."""
        if self.readonly or self._writer_thread is None:
            return True
        deadline = time.monotonic() + timeout
        with self._pending_cv:
            while self._pending > 0:
                left = deadline - time.monotonic()
                if left <= 0:
                    logger.warning("OptionsLedger: %d write(s) still waiting after %.0fs.", self._pending, timeout)
                    return False
                self._pending_cv.wait(left)
        return True

    def get_candle_log(self, trading_day: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._query("SELECT * FROM candle_log WHERE trading_day = ? ORDER BY candle_end, leg",
                           (trading_day or _ist_day(),))

    def get_today_trades(self) -> List[Dict[str, Any]]:
        return self._query("SELECT * FROM trades WHERE trading_day = ? ORDER BY id DESC", (_ist_day(),))

    def get_all_trades(self, limit: int = 500) -> List[Dict[str, Any]]:
        return self._query("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))

    def get_events(self, event_type: Optional[str] = None, trading_day: Optional[str] = None) -> List[Dict[str, Any]]:
        sql, params = "SELECT * FROM events WHERE 1=1", []
        if event_type:
            sql += " AND event_type = ?"; params.append(event_type)
        if trading_day:
            sql += " AND trading_day = ?"; params.append(trading_day)
        return self._query(sql + " ORDER BY id", tuple(params))

    def trade_booked(self, trade_id: str) -> bool:
        return bool(self._query("SELECT 1 FROM trades WHERE trade_id = ? LIMIT 1", (trade_id,)))

    def load_state(self) -> Optional[Dict[str, Any]]:
        """Saved state, but ONLY if it's from TODAY's trading day."""
        rows = self._query("SELECT trading_day, state_json FROM engine_state WHERE id = 1", ())
        if not rows or rows[0]["trading_day"] != _ist_day():
            return None
        return json.loads(rows[0]["state_json"])

    def close(self) -> None:
        if self.readonly or self._writer_thread is None:
            try:
                self._reader.close()
            except sqlite3.Error:
                pass
            return
        self.flush()
        self._q.put(None)
        self._writer_thread.join(timeout=10)
        try:
            self._journal.close()
            if self._pending == 0:
                self._journal_path.write_text("", encoding="utf-8")   # everything committed
        except OSError:
            pass
        for conn in (self._reader, self._wconn):
            try:
                if conn is self._wconn:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                conn.close()
            except sqlite3.Error:
                pass
        self._writer_thread = None

    # ── internals ───────────────────────────────────────────────────────────

    @staticmethod
    def _trade_stmt(trade_id, symbol, strike, entry_price, exit_price, qty, pnl, entry_order_id,
                    exit_order_id, entry_time, exit_time, exit_reason, gross_pnl=None, charges=0.0):
        # `pnl` is NET (what lands in the account); gross and charges kept
        # beside it so "was the signal right" and "did costs eat it" separate.
        return (_SQL_TRADE, (
            trade_id, _ist_day(exit_time), symbol, float(strike), round(float(entry_price), 4),
            round(float(exit_price), 4), int(qty), round(float(pnl), 2),
            round(float(gross_pnl if gross_pnl is not None else pnl), 2), round(float(charges), 2),
            exit_reason, entry_order_id, exit_order_id, _utc_iso(entry_time), _utc_iso(exit_time),
        ))

    @staticmethod
    def _event_stmt(event_type, symbol=None, details=None):
        now = datetime.now(timezone.utc)
        return (_SQL_EVENT, (_utc_iso(now), _ist_day(now), event_type, symbol, details))

    @staticmethod
    def _state_stmt(state):
        now = datetime.now(timezone.utc)
        return (_SQL_STATE, (_ist_day(now), _utc_iso(now), json.dumps(state)))

    def _submit(self, op: Op) -> None:
        if self.readonly:
            raise RuntimeError("OptionsLedger opened readonly — writes are not allowed.")
        with self._seq_lock:
            self._seq += 1
            seq = self._seq
            # Journal FIRST (a few microseconds, no locks on the database):
            # if the process dies before the writer commits, it's replayed.
            self._journal.write(json.dumps({"seq": seq, "op": op}) + "\n")
            self._journal.flush()
            with self._pending_cv:
                self._pending += 1
            self._q.put((seq, op))

    def _writer_loop(self) -> None:
        while True:
            item = self._q.get()
            if item is None:
                return
            seq, op = item
            self._commit_until_done(op, f"seq {seq}")
            try:
                with self._seq_lock:
                    self._journal.write(json.dumps({"done": seq}) + "\n")
                    self._journal.flush()
            except (OSError, ValueError):
                pass
            with self._pending_cv:
                self._pending -= 1
                self._pending_cv.notify_all()

    def _commit_until_done(self, op: Op, label: str) -> None:
        """Commit `op` as one transaction. A locked/busy database is retried
        until it frees up — the write is never dropped. Anything else (a bug:
        a bad statement) can't succeed on retry, so it is logged loudly and
        kept in data/<db>.failed for inspection."""
        started, warned, delay = time.monotonic(), False, 0.05
        while True:
            try:
                with self._wconn:
                    for sql, params in op:
                        self._wconn.execute(sql, params)
                if warned:
                    logger.warning("OptionsLedger: database free again — %s committed after %.1fs.",
                                   label, time.monotonic() - started)
                return
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "locked" not in msg and "busy" not in msg:
                    self._give_up(op, label, exc)
                    return
                waited = time.monotonic() - started
                if not warned and waited >= _BLOCKED_WARN_SECS:
                    warned = True
                    logger.warning("OptionsLedger: another program is holding the database — %s has waited "
                                   "%.0fs. The engine keeps running; the write is kept and will commit.",
                                   label, waited)
                time.sleep(delay)
                delay = min(delay * 2, 1.0)
            except sqlite3.Error as exc:
                self._give_up(op, label, exc)
                return

    def _give_up(self, op: Op, label: str, exc: Exception) -> None:
        logger.critical("OptionsLedger: %s could not be written (%s) — saved to %s.failed.",
                        label, exc, self._path.name)
        try:
            with self._path.with_name(self._path.name + ".failed").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"error": str(exc), "op": op}) + "\n")
        except OSError:
            pass

    def _replay_journal(self) -> None:
        """Commit anything the previous run handed over but never committed."""
        if not self._journal_path.exists():
            return
        ops, done = {}, set()
        try:
            for line in self._journal_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue                      # a line cut off by the crash itself
                if "done" in rec:
                    done.add(rec["done"])
                elif "seq" in rec:
                    ops[rec["seq"]] = rec["op"]
        except OSError:
            return
        missing = [s for s in sorted(ops) if s not in done]
        for s in missing:
            self._commit_until_done([(sql, tuple(p)) for sql, p in ops[s]], f"journal replay seq {s}")
        if missing:
            logger.warning("OptionsLedger: recovered %d write(s) from the last run that had not reached "
                           "the database before it stopped.", len(missing))
        self._journal_path.write_text("", encoding="utf-8")

    def _query(self, sql: str, params: tuple) -> List[Dict[str, Any]]:
        self.flush()
        try:
            with self._read_lock:
                cur = self._reader.execute(sql, params)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
        except sqlite3.Error as exc:
            logger.error("OptionsLedger read failed: %s", exc)
            return []

    def _migrate_trades_columns(self) -> None:
        """gross_pnl/charges on a `trades` table from before the cost model:
        those rows were frictionless, so gross = net and charges = 0."""
        existing = {r[1] for r in self._wconn.execute("PRAGMA table_info(trades)")}
        for column in ("gross_pnl", "charges"):
            if column not in existing:
                default = "pnl" if column == "gross_pnl" else "0.0"
                self._wconn.execute(f"ALTER TABLE trades ADD COLUMN {column} REAL")
                self._wconn.execute(f"UPDATE trades SET {column} = {default} WHERE {column} IS NULL")
                logger.info("ledger: added trades.%s (existing rows backfilled).", column)

    def _normalize_times_to_utc(self) -> None:
        """Rewrite any non-UTC timestamp (older rows) as UTC, so the same
        instant is always the same string."""
        fixed = 0
        for table, cols in (("candle_log", ("candle_start", "candle_end")),
                            ("trades", ("entry_time", "exit_time")),
                            ("events", ("created_at",))):
            rows = self._wconn.execute(
                f"SELECT id, {', '.join(cols)} FROM {table} WHERE "
                + " OR ".join(f"{c} NOT LIKE '%+00:00'" for c in cols)
            ).fetchall()
            for row in rows:
                new = [_utc_iso_str(v) for v in row[1:]]
                self._wconn.execute(f"UPDATE {table} SET {', '.join(c + '=?' for c in cols)} WHERE id=?",
                                    (*new, row[0]))
                fixed += 1
        if fixed:
            logger.info("ledger: normalised %d row(s) of timestamps to UTC.", fixed)

    def _dedupe_candle_log(self) -> None:
        """Drop duplicate candle rows (earliest kept — the live one where both
        exist) so the unique index can hold."""
        removed = self._wconn.execute(
            "DELETE FROM candle_log WHERE id NOT IN ("
            "  SELECT MIN(id) FROM candle_log GROUP BY trading_day, leg, strike, candle_end)"
        ).rowcount
        if removed:
            logger.warning("candle_log: removed %d duplicate candle row(s); counts from this table "
                           "were inflated until now.", removed)


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _utc_iso_str(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return _utc_iso(datetime.fromisoformat(value))
    except ValueError:
        return value


def _ist_day(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(timezone.utc)).astimezone(_IST).strftime("%Y-%m-%d")
