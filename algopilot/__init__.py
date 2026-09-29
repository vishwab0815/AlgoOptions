"""
AlgoPilotX — NIFTY options premium-selling engine (paper or live).

Sub-package layout
------------------
  algopilot/
    core/          — CandleBuilder, HeikinAshiEngine: pure market math.
    strategy/      — DirectionRules (SHORT), SignalEngine (the strict
                      GREEN->RED->RED breakout state machine).
    utils/         — Market-hours guard, IPv4 + TLS setup, rate limiter, SecretStr.
    options/       — config, DhanHQ market data (dhan_client), real orders
                      (broker — live mode only), charges, SQLite ledger,
                      position/stop maths, and the OptionsEngine itself.

Entry point: run_options.py, at the repo root.

OPTIONS_PAPER_TRADING=true (default) -> paper: nothing is ever sent to the
exchange. OPTIONS_PAPER_TRADING=false -> live: real orders on DhanHQ.
"""
