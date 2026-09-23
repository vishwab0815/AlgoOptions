"""
algopilot/options — NIFTY options premium-selling PAPER TRADING engine.

Its own config (config.py), its own DhanHQ REST access (dhan_client.py,
read-only — quotes and margin, never an order), its own real-time WebSocket
feed (websocket.py), its own SQLite ledger (ledger.py), and its own
composition root (engine.py). Run it with run_options.py at the repo root.

Floor-hundred strike band, GREEN->RED->RED Heikin-Ashi pattern sell-to-open,
watching both legs at once until the first one fires (then locked to that
side until it closes), real DhanHQ margin-based lot sizing. Entries are pure
pattern — no RSI, no volume gate, no profit ratchet. Exit is the Heikin-Ashi
trailing stop plus the 15:00 IST EOD square-off.
"""
