# AlgoPilotX Options

A NIFTY options premium-selling **paper-trading** engine. Pure backend, no
UI, no web server — a single asyncio loop that polls DhanHQ's REST API and
logs every decision to the console and a local SQLite ledger.

No real order is ever placed. `OPTIONS_PAPER_TRADING=false` is refused at
startup — see [algopilot/options/config.py](algopilot/options/config.py).

## Strategy, in short

- **Strike band**: NIFTY spot always sits between two round hundreds.
  `PUT = floor(spot/100)*100`, `CALL = PUT + 100`. The band freezes the
  moment either leg has an open position, and only re-resolves once both
  legs are flat.
- **Pattern**: on each leg's *own premium* candles (never the index) —
  Heikin-Ashi GREEN → RED → RED. Candle 3 gets one chance to break Candle
  2's low; if it does, sell to open. Gated by RSI(14) > 30 on that leg.
- **Sequential sides**: only CE or PE is "active" at a time (starts on CE).
  The instant a position closes, the active side flips to the other leg.
- **Sizing**: `lots = floor(capital / max_concurrent / live_margin_per_lot)`,
  capped at `OPTIONS_MAX_LOTS_PER_TRADE`. Margin is fetched live from
  DhanHQ's margin calculator, not guessed.
- **Exits**: a profit ratchet (arm/floor/ceiling/step, on premium), a
  Heikin-Ashi trailing stop, and an EOD square-off from 15:00 IST.

## Layout

```
algopilot/
  core/      Candle building, Heikin-Ashi, RSI, the profit-ratchet ladder
  strategy/  DirectionRules (SHORT) + the GREEN->RED->RED pattern engine
  utils/     Market-hours guard, IPv4 fix, rate limiter, SecretStr
  options/   config, DhanHQ REST client (quotes/margin — read only),
             SQLite ledger, position/exit math, and the engine itself
run_options.py       entry point
scripts/generate_token.py   daily DhanHQ access-token generator
```

## Running it

```bash
pip install -r requirements.txt
cp .env.example .env      # fill in DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN
python run_options.py
```

Trades, blocked signals, and engine events land in
`data/options_ledger.db` (`trades` and `events` tables).
