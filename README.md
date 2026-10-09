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

1. **Strikes** — by premium (`OPTIONS_STRIKE_PICK=premium`): CE and PE are picked
   **separately**, each the strike whose premium is **₹100–150** — in or out of
   the money doesn't matter. Several in range → the one closest to ₹125.
   NIFTY 22,233: CE 22100 ₹145 / CE 22200 ₹112 → **CE 22200**. A side keeps its
   strike until its premium leaves ₹95–155, and never switches while a trade is
   open — the range only picks the strike for the next trade, it never closes
   one. Checked every ~5 s. (`spot` = PUT = NIFTY rounded down to the hundred,
   CALL = +100.)
2. **Expiry** — the nearest weekly, fixed for the day (0 DTE on expiry day
   unless `OPTIONS_ROLL_ON_EXPIRY_DAY=true`).
3. **Candles** — live ticks, timed by the exchange's trade time, become
   5-minute candles closed at exactly hh:mm:00.000 (Dhan's own bar is used,
   the same one the chart shows), then Heikin-Ashi. The Heikin-Ashi series
   continues from previous days (like a chart).
4. **Pattern — GREEN → RED → RED**, on each side's own premium (CE and PE watched
   all day): candle 1 GREEN; candle 2 RED — its **HA Low** is the level;
   candle 3 RED and **breaks the level** (its HA Low or HA Close below it) →
   **sell at candle 3's close** (market order).
5. **Rules** — one trade at a time; **at most 6 trades a day**, CE and PE
   together (`OPTIONS_MAX_TRADES_PER_DAY`). The same side trades again only after
   a GREEN that closes after its exit. No new trades after the square-off time
   or after the daily loss limit; from 09:15.
6. **Stop — HA High 2 candles back**, moved at every candle close: candle 2's
   while candle 4 trades, candle 3's while candle 5 trades, and so on.
   Example: sold at 75 after candle 3; candle 2's HA High 116 → stop 116;
   candle 3's HA High 93 → stop 93 while candle 5 trades.
7. **Profit lock** — off (`OPTIONS_PROFIT_LOCK=false`): the exit is the HA stop.
   Switched on, it adds a lock at 8 points, then every 3 points, one step
   behind the best.
8. **Exit** — stop hit, or everything bought back at **15:10**
   (`OPTIONS_SQUAREOFF_AT`).
9. **P&L** — net of brokerage, STT, exchange/SEBI fees, stamp duty and GST
   (rates in `.env`, estimates — check a contract note).

## Live mode

- Entry: a market SELL at candle 3's close; the exchange's fill price and
  quantity are recorded, and the buy stop goes in right after the fill.
- A **stop-limit BUY rests at the exchange** at the HA stop (moved every candle;
  with the profit lock on, at whichever of the two is closer). The exchange
  fills it the moment price touches it, and you stay protected even if the
  engine or VM stops. If price gaps past its limit, the engine cancels it and
  buys back at market.
- Funds are read fresh from Dhan right after every exit, so the next trade is
  never refused for "insufficient funds" with money still held for the last one.
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
