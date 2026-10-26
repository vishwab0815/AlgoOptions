# AlgoPilotX Options

A NIFTY options premium-selling engine on Zerodha (Kite Connect v3). Pure backend, no UI: one
asyncio process that reads live market data, runs a Heikin-Ashi pattern on
each option's own premium, and either **paper-trades** or places **real
orders**, recording everything in a local SQLite ledger.

| `.env` | What happens |
|---|---|
| `OPTIONS_PAPER_TRADING=true` (default) | Paper: live data, simulated fills, **no order is ever sent** |
| `OPTIONS_PAPER_TRADING=false` | **Live: real orders, real money** on your Zerodha account |

## Strategy

1. **Strikes** — by premium (`OPTIONS_STRIKE_PICK=premium`): CE and PE are picked
   **separately**, each the strike whose premium is **above ₹300**, no upper
   limit — the strike just above ₹300 (in or out of the money doesn't matter).
   26-Oct, NIFTY 22,651: CE 22300 ₹418 / **CE 22400 ₹329** / CE 22500 ₹243 →
   **CE 22400**. A side keeps its strike while its premium stays ₹300 or above
   (`OPTIONS_PREMIUM_MAX=0`, `OPTIONS_PREMIUM_BUFFER=0`), and never switches while a trade or
   order is open or a pattern is waiting — the range only picks the strike for
   the next trade, it never closes one. Checked every ~5 s. (`spot` = PUT = NIFTY
   rounded down to the hundred, CALL = +100.)
2. **Expiry** — the nearest weekly, fixed for the day (0 DTE on expiry day
   unless `OPTIONS_ROLL_ON_EXPIRY_DAY=true`).
3. **Candles** — live ticks, timed by the exchange's trade time, become
   5-minute candles closed at exactly hh:mm:00.000, then Heikin-Ashi. The
   Heikin-Ashi series continues from previous days (like a chart); the
   opening candle is fetched complete.
4. **Pattern**, on each side's own premium (CE and PE watched all day):
   candle 1 GREEN, then candle 2 RED (GREEN = HA close above HA open, else RED).
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
   after the daily loss limit; from 09:15. **At most 6 trades a day**, CE and PE
   together (`OPTIONS_MAX_TRADES_PER_DAY`).
7. **Stop** — **HA High + 0.5** (`OPTIONS_STOP_OFFSET_POINTS`): candle 2's
   while candles 3 and 4 form, then the HA High from 2 candles back, moved at
   every candle close.
8. **Profit lock** — first at 50 points, then every 2 points (50, 52, 54, 56 …),
   always one step behind the best (`OPTIONS_PROFIT_LOCK_START_POINTS`,
   `OPTIONS_PROFIT_LOCK_STEP_POINTS`). Live, it sits at the exchange. Sold at 400:

   | Lowest price | Points | Buy back at | Points kept |
   |---|---|---|---|
   | 351 | 49 | no lock yet (only the HA stop) | — |
   | 350 | 50 | 350 | 50 |
   | 348 | 52 | 350 | 50 |
   | 346 | 54 | 348 | 52 |
   | 344 | 56 | 346 | 54 |
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
- On restart it checks Zerodha's real positions before trading.
- **Kill switch:** create an empty file `data/KILL` → it squares off
  everything and stops.
- Kite accepts orders only from the static IP registered on
  developers.kite.trade → Profile → IP Whitelist (IPv4, the address orders
  leave from) — run live on that machine. Reads work from anywhere.
- Kite specifics handled in `broker.py`: the stop is an `SL` (stop-limit;
  SL-M is blocked on index options), every MARKET order carries
  `market_protection=-1` (required since Apr-2026), and a stop moved more
  than 25 times (Kite's cap) is cancelled and re-placed.

## Running it

```bash
pip install -r requirements.txt
cp .env.example .env              # KITE_API_KEY (developers.kite.trade -> your app)
cp secrets.env.example secrets.env  # KITE_API_SECRET (VM only, never in git)
python scripts/kite_login.py      # every morning: log in on Zerodha's page
python run_options.py
```

You need a Kite Connect app on the paid **Connect** plan (live + historical
data), with its redirect URL set to `http://127.0.0.1:5000/` so
`kite_login.py` can catch the login by itself (or paste the redirect URL to it).

Zerodha requires a person to **log in every trading day**; the token then works
until **06:00 IST the next morning**. There is no unattended token (no API takes
a password or TOTP). The engine asks Kite whether the token works and refuses to
start if it doesn't, and doesn't start on a weekend or NSE holiday
(`algopilot/utils/market_calendar.py`). It stops by itself at 15:31 once flat
(`OPTIONS_AUTO_STOP_AT`), or cleanly at any time when `data/STOP` exists.

## Running unattended (VM)

Once, on the VM:

1. developers.kite.trade → your app: redirect URL `http://127.0.0.1:5000/`;
   Profile → **IP Whitelist**: the VM's public IPv4 (135.13.32.41).
2. Copy `secrets.env.example` to `secrets.env` and fill in `KITE_API_SECRET`.
   It stays on the VM: git-ignored, never logged.
3. `python scripts/latency_check.py` — checks the token and the connection.
4. `python scripts/daily_pipeline.py` — **start it once and leave it running.**
5. Optional: `python scripts/daily_pipeline.py --install` — it also starts by
   itself whenever you sign in to Windows (e.g. after a reboot).

Every morning (after 06:00): `python scripts/kite_login.py` on the VM.

While it runs:

- **every trading day from 08:45 IST** it waits for that morning's Kite login
  (a loud log line, then a quiet check every minute) and starts the engine the
  moment the token works; at 09:17–09:25
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
python scripts/replay_day.py 2026-10-23              # replay a real day through the engine (paper)
```

Kite serves candles only for contracts still listed, so `replay_day.py` can
replay days of the current / next weekly expiry, not expired weeks.

Both are safe to run while the engine is live (read-only / separate ledger).

## Layout

```
algopilot/
  core/       candle building, Heikin-Ashi
  strategy/   SHORT rules + the GREEN->RED->RED pattern
  utils/      market hours, IPv4/TLS setup, secrets
  options/    config, Kite HTTP (kite_api), instrument list (instruments),
              market data (kite_client), orders + order updates (broker),
              price feed (websocket), login (kite_auth), charges, ledger,
              position/stop maths, engine
run_options.py              entry point
scripts/kite_login.py       the daily Kite login -> KITE_ACCESS_TOKEN in .env
scripts/replay_day.py       replay a real day through the engine (paper)
scripts/export_candle_log.py
scripts/latency_check.py    connection speed to Kite (read-only)
scripts/daily_pipeline.py   every trading day, unattended (waits for the morning login)
tests/test_kite.py          offline tests of the Kite layer + the engine on a fake Kite
tests/test_strategy.py      offline tests of the trading rules
```
