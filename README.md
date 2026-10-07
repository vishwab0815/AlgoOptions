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
4. **Pattern**, on each side's own premium (CE and PE watched all day):
   candle 1 GREEN, then candle 2 RED.
5. **Sell order** — the moment candle 2 closes, a sell order goes in at
   **candle 2's HA Low − 0.5** (`OPTIONS_ENTRY_OFFSET_POINTS`). It sells when
   the price reaches it during candle 3, and is cancelled when candle 3 ends.
   If the price is already below it, it sells at once.
6. **Rules** — one trade at a time and one order at a time. Both sides ready
   together: the side with fewer trades today (if equal, the closer price).
   A pattern that completes while the **other** side's trade is open waits:
   if that trade closes before its candle 3 ends, the sell order goes in then
   (or it sells at once if the price is already below). The same side trades
   again only after a GREEN that closes after its exit (the exit candle itself
   counts if it closes GREEN). No new trades after the square-off time or
   after the daily loss limit; from 09:15.
7. **Stop** — **HA High + 0.5** (`OPTIONS_STOP_OFFSET_POINTS`): candle 2's
   while candles 3 and 4 form, then the HA High from 2 candles back, moved at
   every candle close.
8. **Profit lock** — first at 5 points, then every 3 points (5, 8, 11, 14, 17 …),
   always one step behind the best (`OPTIONS_PROFIT_LOCK_START_POINTS`,
   `OPTIONS_PROFIT_LOCK_STEP_POINTS`). Live, it sits at the exchange. Sold at 100:

   | Lowest price | Points | Buy back at | Points kept |
   |---|---|---|---|
   | 95 | 5 | 95 | 5 |
   | 92 | 8 | 95 | 5 |
   | 89 | 11 | 92 | 8 |
   | 86 | 14 | 89 | 11 |
   | 83 | 17 | 86 | 14 |
9. **Exit** — stop hit, profit lock hit, or everything bought back at **15:10**
   (`OPTIONS_SQUAREOFF_AT`).
10. **P&L** — net of brokerage, STT, exchange/SEBI fees, stamp duty and GST
   (rates in `.env`, estimates — check a contract note).

## Live mode

- Entry: a sell stop-limit parked at the exchange at the sell price (fills the
  moment the price gets there); the exchange's fill price and quantity are recorded.
  The buy stop goes in right after the fill.
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
