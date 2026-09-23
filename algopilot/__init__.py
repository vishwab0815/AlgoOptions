"""
AlgoPilotX — NIFTY Options Premium-Selling PAPER TRADING engine.

Sub-package layout
------------------
  algopilot/
    core/          — CandleBuilder, HeikinAshiEngine, IndicatorState (RSI),
                      RatchetConfig — pure, side-effect-free market math.
    strategy/      — DirectionRules (SHORT), SignalEngine (the strict
                      GREEN->RED->RED breakout state machine).
    utils/         — Market-hours guard, IPv4 fix, rate limiter, SecretStr.
    options/       — The engine itself: config, DhanHQ REST access (quotes
                      + margin, read-only), SQLite ledger, position/exit
                      math, and the OptionsEngine composition root.

Entry point: run_options.py, at the repo root.

There is no live order path anywhere in this codebase — see
algopilot/options/config.py, which refuses to start with
OPTIONS_PAPER_TRADING=false.
"""
