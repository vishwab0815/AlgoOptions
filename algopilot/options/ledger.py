"""
algopilot/options/ledger.py — minimal SQLite paper-trading ledger for the
options engine.

Four tables: `trades` (completed round-trips), `events` (entry/exit/blocked
decisions and engine lifecycle), `engine_state` (a single JSON blob — band,
expiry, active side, and any currently-open position — kept current so a
restart mid-session can resume the exact position it was managing instead of
silently forgetting it; see engine.py's _save_state()/_restore_state()), and
`candle_log` (one row per candle close per leg — exact HA OHLC, pattern
stage transition, and target level — the structured record for offline
analysis; see engine.py's _log_pattern_stage()). SQLite over CSV specifically
because this file is written by the live engine AND read by the user at the
same time — WAL mode lets a reader (DB Browser, a pandas query, the export
script) see consistent data without ever blocking or corrupting the writer,
which a CSV opened in Excel mid-session cannot guarantee on Windows.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
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
    pnl             REAL NOT NULL,
    exit_reason     TEXT NOT NULL,
    entry_order_id  TEXT NOT NULL,
    exit_order_id   TEXT NOT NULL,
    entry_time      TEXT NOT NULL,
    exit_time       TEXT NOT NULL
);
"""

_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL,
    trading_day TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    symbol      TEXT,
    details     TEXT
);
"""

# Single row (id=1), overwritten on every state change. trading_day guards
# against ever restoring YESTERDAY's band/position into a fresh day — see
# load_state(), which only returns a row if its trading_day matches today's.
_CREATE_ENGINE_STATE = """
CREATE TABLE IF NOT EXISTS engine_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    trading_day TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    state_json  TEXT NOT NULL
);
"""

# One row per candle close per leg — live AND backfill-replayed, distinguished
# by `source`, so the record matches reality exactly (backfill candles are
# real market data too, they just never fire a trade — see engine.py). This
# is the number/timing source for offline analysis; the text log line is for
# reading in the moment, this table is for querying afterward.
_CREATE_CANDLE_LOG = """
CREATE TABLE IF NOT EXISTS candle_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    trading_day   TEXT NOT NULL,
    leg           TEXT NOT NULL,        -- CE | PE
    strike        REAL,
    candle_start  TEXT NOT NULL,
    candle_end    TEXT NOT NULL,
    source        TEXT NOT NULL,        -- live | backfill
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


class OptionsLedger:
    def __init__(self, db_path: str = "data/options_ledger.db") -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        with self._lock, self._conn:
            self._conn.execute(_CREATE_TRADES)
            self._conn.execute(_CREATE_EVENTS)
            self._conn.execute(_CREATE_ENGINE_STATE)
            self._conn.execute(_CREATE_CANDLE_LOG)
            self._conn.execute("CREATE INDEX IF NOT EXISTS ix_trades_day ON trades(trading_day)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS ix_events_day ON events(trading_day)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS ix_candle_log_day_leg ON candle_log(trading_day, leg, candle_end)")
        logger.info("OptionsLedger ready at %s", self._path.resolve())

    def log_trade(
        self, trade_id: str, symbol: str, strike: float, entry_price: float, exit_price: float,
        qty: int, pnl: float, entry_order_id: str, exit_order_id: str,
        entry_time: datetime, exit_time: datetime, exit_reason: str,
    ) -> None:
        sql = (
            "INSERT OR IGNORE INTO trades (trade_id, trading_day, leg, strike, entry_price, exit_price, "
            "qty, pnl, exit_reason, entry_order_id, exit_order_id, entry_time, exit_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            trade_id, _ist_day(exit_time), symbol, strike, entry_price, exit_price,
            qty, pnl, exit_reason, entry_order_id, exit_order_id,
            entry_time.isoformat(), exit_time.isoformat(),
        )
        self._write(sql, params, "log_trade")

    def log_event(self, event_type: str, symbol: Optional[str] = None, details: Optional[str] = None) -> None:
        created = datetime.now(timezone.utc)
        sql = "INSERT INTO events (created_at, trading_day, event_type, symbol, details) VALUES (?, ?, ?, ?, ?)"
        self._write(sql, (created.isoformat(), _ist_day(created), event_type, symbol, details), "log_event")

    def log_candle(
        self, leg: str, strike: Optional[float], candle_start: datetime, candle_end: datetime,
        source: str, ha_open: float, ha_high: float, ha_low: float, ha_close: float, color: str,
        stage_before: str, stage_after: str, target_level: Optional[float], signal: str,
    ) -> None:
        sql = (
            "INSERT INTO candle_log (trading_day, leg, strike, candle_start, candle_end, source, "
            "ha_open, ha_high, ha_low, ha_close, color, stage_before, stage_after, target_level, signal) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            _ist_day(candle_end), leg, strike, candle_start.isoformat(), candle_end.isoformat(), source,
            ha_open, ha_high, ha_low, ha_close, color, stage_before, stage_after, target_level, signal,
        )
        self._write(sql, params, "log_candle")

    def get_candle_log(self, trading_day: Optional[str] = None) -> List[Dict[str, Any]]:
        day = trading_day or _ist_day()
        return self._read(
            lambda: _rows(self._conn.execute(
                "SELECT * FROM candle_log WHERE trading_day = ? ORDER BY candle_end, leg", (day,),
            )), [],
        )

    def get_today_trades(self) -> List[Dict[str, Any]]:
        return self._read(
            lambda: _rows(self._conn.execute(
                "SELECT * FROM trades WHERE trading_day = ? ORDER BY id DESC", (_ist_day(),),
            )), [],
        )

    def get_all_trades(self, limit: int = 500) -> List[Dict[str, Any]]:
        return self._read(
            lambda: _rows(self._conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))), [],
        )

    def save_state(self, state: Dict[str, Any]) -> None:
        """Overwrite the single resumable-state row. Called on every band
        resolution, entry, cover-level update, and exit — cheap (one row,
        WAL mode) and means a crash never loses more than the last change."""
        sql = (
            "INSERT INTO engine_state (id, trading_day, updated_at, state_json) VALUES (1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET trading_day=excluded.trading_day, "
            "updated_at=excluded.updated_at, state_json=excluded.state_json"
        )
        now = datetime.now(timezone.utc)
        self._write(sql, (_ist_day(now), now.isoformat(), json.dumps(state)), "save_state")

    def load_state(self) -> Optional[Dict[str, Any]]:
        """The saved state, but ONLY if it's from TODAY's trading day — a
        restart on a fresh day must never resurrect yesterday's band or
        position."""
        def _q() -> Optional[Dict[str, Any]]:
            row = self._conn.execute(
                "SELECT trading_day, state_json FROM engine_state WHERE id = 1"
            ).fetchone()
            if row is None:
                return None
            trading_day, state_json = row
            if trading_day != _ist_day():
                return None
            return json.loads(state_json)
        return self._read(_q, None)

    def _write(self, sql: str, params: tuple, context: str) -> None:
        try:
            with self._lock, self._conn:
                self._conn.execute(sql, params)
        except sqlite3.Error as exc:
            logger.error("OptionsLedger write failed [%s]: %s", context, exc)

    def _read(self, fn: Callable[[], Any], default: Any) -> Any:
        try:
            with self._lock:
                return fn()
        except sqlite3.Error as exc:
            logger.error("OptionsLedger read failed: %s", exc)
            return default

    def close(self) -> None:
        try:
            with self._lock:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self._conn.close()
        except sqlite3.Error:
            pass


def _ist_day(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(timezone.utc)).astimezone(_IST).strftime("%Y-%m-%d")


def _rows(cursor: sqlite3.Cursor) -> List[Dict[str, Any]]:
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
