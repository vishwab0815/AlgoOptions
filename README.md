# AlgoPilotX Options

A NIFTY options premium-selling engine on DhanHQ. Pure backend, no UI: one
asyncio process that reads live market data, runs a Heikin-Ashi pattern on
each option's own premium, and either **paper-trades** or places **real
orders**, recording everything in a local SQLite ledger.

| `.env` | What happens |
|---|---|
| `OPTIONS_PAPER_TRADING=true` (default) | Paper: live data, simulated fills, **no order is ever sent** |
| `OPTIONS_PAPER_TRADING=false` | **Live: real orders, real money** on your Dhan account |

## Strategy

1. **Strikes** — from NIFTY spot (polled every 5 s): `PUT = floor(spot/100)*100`,
   `CALL = PUT + 100`. They move only once spot is 15+ points past the
   hundred, and never while a trade is open.
2. **Expiry** — the nearest weekly, fixed for the day (0 DTE on expiry day
   unless `OPTIONS_ROLL_ON_EXPIRY_DAY=true`).
3. **Candles** — live ticks, timed by the exchange's trade time, become
   5-minute candles closed at exactly hh:mm:00.000, then Heikin-Ashi. The
   Heikin-Ashi series continues from previous days (like a chart); the
   opening candle is fetched complete.
4. **Pattern**, on each leg's own premium (CE and PE tracked all day):
   `1/3` a GREEN candle arms it; `2/3` the first RED after it sets
   **level = its HA low**; `3/3` the very next candle — if price trades below
   the level, that's a signal. Otherwise the pattern is dead until a new GREEN.
5. **Filters** (skipped signals are logged with the reason): one trade at a
   time; after a trade either leg may trade next (the same leg only on a fresh
   pattern — its GREEN must close after the exit); entries only 09:30–15:00;
   never on a gap-filled candle; daily loss limit; blocked contracts (live).
6. **Entry** — sell 1 lot at market (sizing: capital / live margin per lot).
7. **Stop** — while candle N trades, the stop is the **HA high of candle N−2**,
   moved at every candle close. Checked on every tick, with the option-chain
   price every 5 s as a backup.
8. **Profit lock** — sold at 123: once the premium touches 113 (10 points),
   buy back if it comes back to 113; at 110 the lock stays 113; at 107 it moves
   to 110; at 104 to 107 — every 3 points, one step behind the best. Live, the
   lock is parked AT THE EXCHANGE (see below); the engine also watches every tick.
9. **Exit** — stop hit, profit lock hit, or everything bought back at 15:00.
9. **P&L** — net of brokerage, STT, exchange/SEBI fees, stamp duty and GST
   (rates in `.env`, estimates — check a contract note).

## Live mode

- Entry: market SELL; the exchange's actual fill price and quantity are recorded.
- A **stop-limit BUY rests at the exchange** at whichever is closer to the price:
  the HA stop (moved every candle) or the profit lock (moved as soon as a new
  lock is set, once the price is below it). The exchange fills it the moment
  price touches it, and you stay protected even if the engine or VM stops.
  If price gaps past its limit, the engine cancels it and buys back at market.
  If the lock isn't parked yet when price comes back, the engine buys back itself.
- Hard cap of 1 lot (`OPTIONS_LIVE_MAX_LOTS`), daily loss limit
  (`OPTIONS_MAX_DAILY_LOSS`, default ₹3,000).
- On restart it checks Dhan's real positions before trading.
- **Kill switch:** create an empty file `data/KILL` → it squares off
  everything and stops.
- Dhan only accepts orders from the account's registered static IPv4 — run
  live on that machine.

## Running it

```bash
pip install -r requirements.txt
cp .env.example .env      # DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN (same account)
python run_options.py
```

The access token lasts 24 h. The engine refuses to start with an expired
token, or one issued for a different client id, and on a weekend or NSE
holiday (`algopilot/utils/market_calendar.py`). It stops by itself at 15:31
once flat (`OPTIONS_AUTO_STOP_AT`), or cleanly at any time when `data/STOP`
exists.

## Running unattended (VM)

Once, on the VM:

1. Dhan → My Profile → Access DhanHQ APIs → **Set-up TOTP** (keep the text key).
2. Copy `secrets.env.example` to `secrets.env` and fill in `DHAN_PIN` and
   `DHAN_TOTP_SECRET`. It stays on the VM: git-ignored, never logged.
3. `python scripts/auto_token.py --check` — checks the setup (no Dhan call);
   the code it shows must match your authenticator app.
4. `python scripts/daily_pipeline.py` — **start it once and leave it running.**
5. Optional: `python scripts/daily_pipeline.py --install` — it also starts by
   itself whenever you sign in to Windows (e.g. after a reboot).

While it runs:

- **every night at 21:00 IST** a new access token (PIN + TOTP) is made,
  checked with Dhan and written to `.env` (Dhan has no revoke call — the old
  token is replaced and expires by itself; a token lasts 24 h, so tonight's
  covers tomorrow). If the VM was off at 21:00, one is made before the session.
- **every trading day from 08:45 IST** the engine is started; at 09:17–09:25
  it checks NIFTY actually traded (else stops the engine); a crash before the
  close is restarted (max 5, never with `data/KILL`); the engine stops itself
  after the close.
- weekends and NSE holidays: nothing. Update `market_calendar.py` each December.

Log: `data/pipeline.log`. One copy at a time. Keep the VM on (clock on India
Standard Time); disconnect RDP rather than signing out.

Updating: stop it (Ctrl+C), `git pull`, start it again. `.env`, `secrets.env`
and `data\` are not in git and stay as they are.

## Data and analysis

Everything is in `data/options_ledger.db`:

| Table | Contents |
|---|---|
| `trades` | every closed trade: prices, qty, gross, charges, **net** P&L |
| `events` | entries, exits, skipped signals with reasons, every order sent and its result |
| `candle_log` | every candle on both legs: HA OHLC, colour, pattern stage, level |
| `engine_state` | what's open right now (restart recovery) |

```bash
python scripts/export_candle_log.py [YYYY-MM-DD]     # tables + CSVs in data/exports/
python scripts/backtest.py 2026-09-22 2026-09-25     # replay real days through the engine
```

Both are safe to run while the engine is live (read-only / separate ledger).

## Layout

```
algopilot/
  core/       candle building, Heikin-Ashi
  strategy/   SHORT rules + the GREEN->RED->RED pattern
  utils/      market hours, IPv4/TLS setup, rate limiter, secrets
  options/    config, market data (dhan_client), orders (broker), charges,
              ledger, position/stop maths, engine
run_options.py              entry point
scripts/backtest.py         real-data backtest through the engine's own code
scripts/export_candle_log.py
scripts/auto_token.py       today's access token from PIN + TOTP (no browser)
scripts/daily_pipeline.py   one trading day, unattended (--install creates the Windows task)
```
