"""
algopilot/options — NIFTY options premium-selling engine (paper or live).

Its own config (config.py), its own DhanHQ market-data access (dhan_client.py),
live orders (broker.py, only when OPTIONS_PAPER_TRADING=false), its own real-time WebSocket
feed (websocket.py), its own SQLite ledger (ledger.py), and its own
composition root (engine.py). Run it with run_options.py at the repo root.

Floor-hundred strike band, GREEN->RED->RED Heikin-Ashi pattern sell-to-open,
watching both legs, one position at a time, real DhanHQ margin-based lot
sizing. Entries are pure pattern — no RSI, no volume gate. Exits: the
Heikin-Ashi trailing stop, the profit lock, and the EOD square-off (15:10 IST, OPTIONS_SQUAREOFF_AT).
"""
