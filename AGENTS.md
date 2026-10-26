# AGENTS.md — guide for AI coding agents

AlgoPilotX Options: a **live, real-money** NIFTY weekly-options selling engine on
DhanHQ v2. One asyncio Python process; Heikin-Ashi pattern on each option's own
premium; sell-to-open only; SQLite ledger. Read this before changing anything.

## 1. Safety rules (never break these)

1. **Real money.** With `OPTIONS_PAPER_TRADING=false` every order is real
   (account 1101141313). Never run the engine live to "try something".
2. **Tests never place real orders.** Use paper mode or a fake broker object;
   never construct a real `DhanBroker` call path in a test.
3. **Never touch the production ledger** `data/options_ledger.db` in tests or
   experiments — give every test engine its own temporary `db_path`.
4. **The strategy is frozen.** Do not change entry / exit / sizing rules
   without the owner's explicit approval. Infrastructure, logging, speed and
   realism fixes are fine; say what you changed.
5. **Secrets.** `.env` (token), `secrets.env` (Dhan PIN + TOTP key) and
   `data/` are git-ignored — never commit, print, log or zip them. Never ask the
   owner to paste the PIN or TOTP key in chat.
6. **No token generation during market hours** (09:15–15:30 IST): a new token
   can invalidate the one the running engine uses. The nightly job runs at 21:00.
7. **Orders only from the VM.** Dhan accepts orders only from the registered
   static IP (the Windows VM, 135.13.32.41). The VM updates with `git pull`;
   its `.env` / `secrets.env` / `data/` stay on the VM and are not changed by git.
8. **Don't disturb a running engine** during market hours (no restarts, no
   live smoke tests on the same account).
9. **Commit messages**: plain descriptions of the change; do not mention AI
   tools or assistants in them.

## 2. Branches

| Branch | Tag | What it is |
|---|---|---|
| `main` | `v1.0` | Original strategy (GREEN→RED→RED at candle 3's close), older code. GitHub's default branch |
| `archive/v2-early-sell` | `v2.0` | History: SELL parked at candle-2 HA Low − 1, fills in candle 3 |
| `archive/token-automation` | — | History: nightly token + pipeline (merged into v3+) |
| `archive/v3-offsets-1510` | `v3.0` | History: offsets −0.5 / +0.5, waiting order, lock 5/3, 15:10 square-off |
| `archive/v4-premium-doji` | `v4.0` | History: strikes by premium (OTM), doji rule, fresh funds, faster orders, lock 8/3 |
| `version-5` | `v5.0` | Version 4's strategy without the doji rule + strikes by premium ₹100–150 (any moneyness) + profit lock 18/2 + max 6 trades a day. **Latest without AI** |
| **`aiagent-version-6`** | `v6.0`, `v6.1` | **Current.** version-5 + the AI options strategist (`algopilot/agent`): 1/3 analysis, pre-close check, can block the sell at 2/3 |
| `experiment/old-strategy` | — | Side idea: GREEN→RED→RED at candle 3's close, no doji/offsets, 6 trades/day, lock off |

Branches were renamed on 26-Oct-2026 (old names: `version-2`, `version-3`, `version-4`,
`generate_token_automation`, `old-strategy`); only the names changed, not the commits.
Tags mark each version: `git checkout v5.0` returns to it exactly.

Work only on the branch the owner names; create new versions as new branches
from the one they name. Don't merge between strategy branches unasked.

## 3. The strategy on `version-5` (5-minute Heikin-Ashi)

- **Strikes** (`OPTIONS_STRIKE_PICK=premium`): CE and PE picked separately —
  the strike whose premium is ₹100–150, in or out of the money; several →
  closest to ₹125. Kept while the premium stays ₹95–155. Never switched while
  a position, a sell order, an entry in flight or a waiting pattern exists.
  Checked on every option-chain poll (~5 s).
- **Pattern**: candle 1 GREEN, candle 2 RED (GREEN = HA close above HA open,
  otherwise RED; there is no doji rule on version-5 — it was removed 13-Oct).
- **Entry** (`OPTIONS_ENTRY_MODE=break`): when candle 2 closes, a SELL
  stop-limit is parked at the exchange at **candle 2's HA Low − 0.5**; it fills
  during candle 3 or is cancelled when candle 3 ends; if the price is already
  below, sell at market. One order at a time; CE+PE together → fewer trades
  today, then closer price.
- **Waiting order**: if the other side's trade is open when candle 2 closes,
  the order goes in the moment that trade exits (only until candle 3 ends).
- **Stop**: HA High + 0.5 — candle 2's while candles 3–4 form, then the HA High
  2 candles back, moved every close. Live it rests at the exchange as a BUY
  stop-limit (limit +10%); not filled 3 s after crossing → market buy.
- **Profit lock 18/2**: first lock at 18 points, then every 2, one step behind
  (sold 100: 82→82, 80→82, 78→80, 76→78). Parked at the exchange when tighter.
- **Limits**: max 6 trades/day (CE+PE), 1 lot live, daily loss limit
  (`OPTIONS_MAX_DAILY_LOSS`), same side needs a fresh GREEN after its exit,
  entries from 09:15, square-off at **15:10**, engine auto-stops 15:31.

Every setting lives in `.env`; `.env.example` lists all of them with comments,
in the same order. When you add a setting, add it to both and to `config.py`
with validation.

## 3b. The AI strategist (`aiagent-version-6`, `OPTIONS_AI_AGENT=true`)

- **1/3 (GREEN candle):** `TradeAgent.prepare()` builds a market snapshot in code (`agent/features.py`) and asks Claude Opus 5.5 in the background for SELL/SKIP, a 0–100 confidence (its probability the sell makes money) and evidence, as structured JSON.
- **Pre-close (`engine._ai_preclose_clock`, `OPTIONS_AI_PRECLOSE_SECS`=60):** a candle 2 forming RED (provisional HA from `candle_builder._current`) gets a second analysis, stage `2/3-pre`, ready by the close.
- **2/3 (sell order due):** `TradeAgent.decide()` uses the pre-close answer if NIFTY moved ≤ 15 pts and the final HA Low is within 3% of the provisional one; else a fresh 1/3 answer; else a re-check within `OPTIONS_AI_DECIDE_TIMEOUT_SECS`. The order goes in only for SELL with confidence ≥ `OPTIONS_AI_MIN_CONFIDENCE`. The gate is in `engine._arm`, plus `_arm_best` when both sides are ready.
- **Snapshot extras:** `market_direction` (features.direction_section: VWAP, opening range, previous day, futures OI, fresh writing near ATM, PCR, VIX, BANK NIFTY, 8 heavyweights, futures volume - each votes up/down, score >= 3 = trend), the minute tape, our option's 1-min flow (premium/volume/OI), futures and VIX 1-min, market depth (`/marketfeed/quote`, live only), the 30-min straddle/IV trend, today's trades per side, and `similar_past_setups` from `agent/cases.py` (kNN over 10 numbers, only days before today).
- **Memory:** `algopilot/agent/cases_seed.jsonl` (shipped) + `data/agent_cases.jsonl` (VM). `scripts/agent_cases.py [day | from to]` replays days through the engine (paper, temp ledger, stub AI) and saves every sell; the pipeline runs it after each session when the AI is on. Old weeks come from `/charts/rollingoption`.
- **Rulebook (`prompts.py`):** the GREEN→RED pattern bets that the NIFTY push which pumped the option fades. The AI judges whether that push is tiring (SELL: against the day trend, at a wall or the day high/low, short covering, stretched from VWAP) or strong (SKIP: with the day trend on fresh positions, breaking the day high/low). `features.push_section` gives it those numbers. Default bar: 35 (a winner pays more than a typical loss), 50 for a weak setup (`OPTIONS_AI_WEAK_SETUP_CONFIDENCE`): `cases.setup_stats` splits the last 20 days of real sells by this pattern's buckets (our OI flow, stop %, time of day, push given back, VIX, trend); a bucket with >= 10 sells and <= 25% won is weak. Which buckets are weak shifts week to week (the 30-Sep→14-Oct and 07→21-Oct samples disagreed), so nothing is hard-coded: the numbers are recomputed from the memory on every call. News brief is off by default (web search kept failing). A pre-close SKIP is re-checked only on a real turn (15 pts / 8%). Log lines are pointwise: `+` for, `-` against, `=` the record.
- **The agent may only block a sell.** It never places, moves or closes orders.
- **Any failure follows the pattern** (fail-open): timeout, API error, budget used up.
- **Never add an extra option-chain request.** The engine hands its polls to the agent (`dhan_client.last_chain_payload`).
- **Tests must set `ai_agent=False`,** or use a fake LLM. Never call the paid API from tests.
- **Key and spend:** `ANTHROPIC_API_KEY` lives only in `.env`. Spend is capped per day by `OPTIONS_AI_DAILY_BUDGET_USD`, and every call's tokens and $ are logged.

## 4. Code map

| Path | Role |
|---|---|
| `run_options.py` | Entry point: logging, startup banner, runs `OptionsEngine` |
| `algopilot/options/engine.py` | The engine: candle clock, pattern, entries, exits, strikes, live order handling, state restore |
| `algopilot/options/config.py` | All `OPTIONS_*` settings, defaults and validation |
| `algopilot/options/broker.py` | Live orders (DhanBroker), order-update stream |
| `algopilot/options/dhan_client.py` | Option chain, candles, margin, contract list |
| `algopilot/options/position.py` | Open position, HA trailing stop level, profit-lock level |
| `algopilot/options/ledger.py` | SQLite ledger: trades, events, candle log, saved state |
| `algopilot/options/websocket.py` | Live price feed |
| `algopilot/options/charges.py` | Brokerage/STT/fees/GST model |
| `algopilot/options/auto_token.py` | Token from PIN + TOTP |
| `algopilot/core/` | Candle builder, Heikin-Ashi |
| `algopilot/strategy/` | GREEN→RED→RED state machine (`signal_engine.py`) |
| `algopilot/agent/` | AI strategist: `data.py` (Dhan data, depth, chain hand-over), `features.py` (Market Reader), `cases.py` (memory of past sells), `prompts.py` (rulebook + JSON schema), `llm.py` (Claude calls, cost, budget), `agent.py` (1/3 / pre-close / 2/3 orchestration) |
| `scripts/agent_cases.py` | Grows the AI's memory: replays a day (AI off) and saves every sell with its numbers |
| `algopilot/utils/market_hours.py`, `market_calendar.py` | Session times, square-off time, NSE holidays |
| `scripts/daily_pipeline.py` | Run-forever automation on the VM (token 21:00, session 08:45) |
| `scripts/backtest.py` | Replays real Dhan days through the engine (opening spot band only) |
| `scripts/export_candle_log.py` | Candle-log tables/CSVs for analysis |

Runtime files (git-ignored): `data/options_engine.log`, `data/options_ledger.db`,
`data/pipeline.log`, `data/STOP` (clean stop), `data/KILL` (buy back all + stop).

## 5. Working on it

- **Verify with real data.** Before claiming a cause or a fix, reproduce it from
  the real log / real Dhan candles and the engine's own code. Ask the owner for
  `data/options_engine.log` from the VM when a live issue is reported.
- **Test offline**: a fake broker (orders, fills, positions, funds) and a fake
  margin client; temporary ledger; patch `get_market_status` for time. Check
  both paper and live code paths. Run `python -m pyflakes run_options.py algopilot scripts`
  — it must be clean.
- **Files**: Windows, many files use CRLF — preserve line endings when editing.
- **Logs** are for a human: one line per decision, with the reason and numbers
  (e.g. `-> SKIPPED: 6 trades already today`).
- **Talking to the owner**: short, simple words, numbers and an example; when
  asked to "just answer", don't change anything.

## 6. Open items (as of 12-Oct-2026)

- On 09-Oct the owner reported strike picking "not working" on the VM with
  version 4; the VM log has not been seen yet. Robustness fixes are in v5, but
  the real cause is unconfirmed.
- `scripts/backtest.py` uses the opening spot band, not the premium picker.
- Month replays (07-Sep → 07-Oct) were net negative for every variant tested;
  charges (~₹7–9k/month at 1 lot) outweigh the gross edge.
