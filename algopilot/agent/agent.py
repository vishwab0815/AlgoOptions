"""
algopilot/agent/agent.py — TradeAgent: when to analyse, and the verdict the engine gets.

1/3 (candle 1 GREEN on a side)      -> prepare(): a deep analysis starts in the
                                       background (the engine never waits for it).
2/3-pre (candle 2 forming RED,      -> prepare(): a second deep analysis on the
         ~1 minute before its close)   minute-by-minute picture, ready by the close.
2/3 (candle 2 RED, order due)       -> decide(): the pre-close answer is used if the
                                       final candle 2 and NIFTY match it; else the
                                       1/3 answer if fresh; else a quick re-check,
                                       all within a time limit.

Every analysis gets the Market Reader snapshot plus the most similar past sells
of this system and how they ended (cases.py), and today's results so far.
The verdict ALLOWS the sell only for SELL with confidence >= OPTIONS_AI_MIN_CONFIDENCE.
Any failure (no answer in time, API error, daily budget used up) returns a
"follow the pattern" verdict: the engine then trades exactly as without the agent.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .cases import CaseLibrary
from .data import MarketData
from .features import build_snapshot, spot_now
from .llm import BudgetExceeded, ClaudeClient

logger = logging.getLogger(__name__)
_IST = ZoneInfo("Asia/Kolkata")

_REUSE_SECS = 600.0       # a newer GREEN on the same side re-uses an analysis this recent ...
_REUSE_MOVE_PTS = 15.0    # ... if NIFTY moved less than this since
_FRESH_SECS = 420.0       # at 2/3, the 1/3 analysis counts as fresh for this long ...
_FRESH_MOVE_PTS = 20.0    # ... and while NIFTY is within this many points of where it was
_PRE_MOVE_PTS = 15.0      # the pre-close answer holds if NIFTY moved less than this by the close ...
_PRE_LEVEL_PCT = 0.03     # ... and candle 2's final HA Low is within 3% (min 2 pts) of the provisional one
_TURN_PTS = 15.0          # a pre-close SKIP is re-checked at the close if NIFTY moved this much against the push ...
_TURN_PCT = 0.08          # ... or our premium fell this much in the last minute (5 pts / 3%: 3 flips, 3 losers)
_NEWS_FROM, _NEWS_TO = "08:45", "15:15"


@dataclass
class Verdict:
    allow: bool
    decision: str                       # SELL | SKIP | FOLLOW (agent unavailable -> follow the pattern)
    confidence: int = 0
    push_state: str = ""
    reasons: List[str] = field(default_factory=list)     # evidence for the sell
    risks: List[str] = field(default_factory=list)       # evidence against
    invalidation: str = ""
    source: str = "ai"                  # ai | fail-open
    note: str = ""
    spot: Optional[float] = None
    made_at: float = 0.0                # time.monotonic()
    stage: str = ""
    cost_usd: float = 0.0
    usage: str = ""
    candle2_end: str = ""               # 2/3-pre only: the candle it judged ...
    level: Optional[float] = None       # ... and its provisional HA Low
    ltp: Optional[float] = None         # ... and our premium at that moment
    similar: str = ""                   # one-line summary of the similar past sells
    bar: int = 0                        # the confidence the sell needed
    stats: str = ""                     # one-line setup statistics (this system's record)
    direction: str = ""                 # market_direction reading, e.g. trend_down(-5)

    def headline(self) -> str:
        if self.source != "ai":
            return f"AI unavailable ({self.note}) — following the pattern"
        cmp = ">=" if self.allow else "<"
        return f"AI {self.decision} {self.confidence}% {cmp} bar {self.bar}"

    def lines(self) -> str:
        """The pointwise log: + evidence for, - evidence against, = this system's record."""
        out = [f"   + {r}" for r in self.reasons[:4]] + [f"   - {r}" for r in self.risks[:3]]
        if self.stats or self.similar:
            out.append("   = " + " | ".join(x for x in (self.stats, f"similar {self.similar}" if self.similar else "") if x))
        if self.invalidation:
            out.append(f"   ? wrong if {self.invalidation}")
        return "\n".join(out)

    def why(self) -> str:
        parts = "; ".join(self.reasons[:4])
        if self.risks:
            parts += " | against: " + "; ".join(self.risks[:3])
        if self.similar:
            parts += f" | similar past sells: {self.similar}"
        return parts + (f" | wrong if: {self.invalidation}" if self.invalidation else "")


class TradeAgent:
    def __init__(self, config, ledger, client_id: str, access_token: str,
                 scrip_csv: str = "data/dhan_scrip_master.csv") -> None:
        self.cfg = config
        self.ledger = ledger
        self.data = MarketData(client_id, access_token, scrip_csv)
        self.llm = ClaudeClient(config.ai_model, config.ai_news_model, config.ai_daily_budget_usd)
        self.cases = CaseLibrary()
        self._latest: Dict[str, Verdict] = {}
        self._pre: Dict[str, Verdict] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._pre_tasks: Dict[str, Tuple[str, asyncio.Task]] = {}
        self._final: Dict[Tuple[str, str], Verdict] = {}
        self._background: set = set()
        self.news_text, self.news_risk, self.news_at = "", "UNKNOWN", None
        self._budget_warned = False
        self._running = False

    # ── lifecycle ───────────────────────────────────────────────────────────
    async def start(self) -> None:
        self._running = True
        logger.info("AI agent on: %s (effort %s) | bar %d%%, weak setups %d%% | pre-close check %s | news %s | "
                    "budget $%.2f/day | record: %d real sells from %d days.", self.cfg.ai_model, self.cfg.ai_effort,
                    self.cfg.ai_min_confidence, self.cfg.ai_weak_setup_confidence,
                    f"{self.cfg.ai_preclose_secs} s before" if self.cfg.ai_preclose_secs else "off",
                    f"every {self.cfg.ai_news_every_mins} min" if self.cfg.ai_news_every_mins else "off",
                    self.cfg.ai_daily_budget_usd, len(self.cases.cases), len(self.cases.days()))
        self._spawn(asyncio.to_thread(self.data.history))
        if self.cfg.ai_news_every_mins > 0:
            self._spawn(self._news_loop())

    async def close(self) -> None:
        self._running = False
        for t in list(self._background):
            t.cancel()
        if self._background:
            await asyncio.wait(list(self._background), timeout=3)
        await self.llm.close()
        self.data.close()
        s = self.llm.spend
        logger.info("AI agent: today %d call(s), $%.4f spent (%s).", s.calls, s.usd,
                    ", ".join(f"{m} ${v:.4f}" for m, v in s.by_model.items()) or "nothing")

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    def on_chain(self, payload: dict) -> None:
        self.data.on_chain(payload)

    # ── 1/3 and 2/3-pre ─────────────────────────────────────────────────────
    def prepare(self, ctx: dict) -> None:
        pat = ctx["pattern"]
        side, stage = pat["side"], pat.get("stage", "1/3")
        if self.llm.budget_left() <= 0:
            self._warn_budget()
            return
        if stage == "2/3-pre":
            c2 = str(pat.get("candle2_end", ""))
            running = self._pre_tasks.get(side)
            if running is not None and running[0] == c2 and not running[1].done():
                return
            logger.info("[%-2s] %s candle 2 forming RED -> AI pre-close check", side, pat.get("window", ""))
            self._pre_tasks[side] = (c2, self._spawn(self._analyse(ctx, stage, self.cfg.ai_effort,
                                                                   self.cfg.ai_prepare_timeout_secs)))
            return
        running = self._tasks.get(side)
        if running is not None and not running.done():
            return
        prev = self._latest.get(side)
        if prev is not None and prev.source == "ai" and time.monotonic() - prev.made_at < _REUSE_SECS:
            payload = self.data.chain(30)
            spot = float(payload.get("last_price") or 0) if payload else 0.0
            if spot and prev.spot and abs(spot - prev.spot) < _REUSE_MOVE_PTS:
                logger.info("[%-2s] AI: re-using the analysis from %.0f s ago (NIFTY within %.0f pts).",
                            side, time.monotonic() - prev.made_at, _REUSE_MOVE_PTS)
                return
        logger.info("[%-2s] %s 1/3 GREEN -> AI analysis", side, pat.get("window", ""))
        self._tasks[side] = self._spawn(self._analyse(ctx, "1/3", self.cfg.ai_effort, self.cfg.ai_prepare_timeout_secs))

    # ── 2/3 ─────────────────────────────────────────────────────────────────
    async def decide(self, ctx: dict) -> Verdict:
        pat = ctx["pattern"]
        side = pat["side"]
        c2 = str(pat.get("candle2_end", ""))
        key = (side, c2)
        if key in self._final:
            return self._final[key]
        deadline = time.monotonic() + self.cfg.ai_decide_timeout_secs
        pre_task = self._pre_tasks.get(side)
        waits = [t for t in ((pre_task[1] if pre_task and pre_task[0] == c2 else None), self._tasks.get(side))
                 if t is not None and not t.done()]
        for t in waits:
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=max(0.0, deadline - time.monotonic()))
            except (asyncio.TimeoutError, Exception):
                pass
        spot = await asyncio.to_thread(spot_now, self.data)
        level = pat.get("candle2_ha_low")
        pre, prev = self._pre.get(side), self._latest.get(side)
        pre_ok = (pre is not None and pre.source == "ai" and pre.candle2_end == c2
                  and (spot is None or pre.spot is None or abs(spot - pre.spot) <= _PRE_MOVE_PTS)
                  and (level is None or pre.level is None
                       or abs(float(level) - pre.level) <= max(2.0, _PRE_LEVEL_PCT * float(level))))
        fresh = (prev is not None and prev.source == "ai" and time.monotonic() - prev.made_at <= _FRESH_SECS
                 and (spot is None or prev.spot is None or abs(spot - prev.spot) <= _FRESH_MOVE_PTS))
        left = deadline - time.monotonic()
        still_running = any(not t.done() for t in waits)
        pre_same = pre is not None and pre.source == "ai" and pre.candle2_end == c2
        if pre_same and not pre.allow and self._turned_for_sell(side, pre, spot, pat.get("option_ltp")) and left >= 3.0 \
                and self.llm.budget_left() > 0:
            # 14-Oct 12:30 CE: the pre-close check saw NIFTY back at the push high (SKIP); in the last minute
            # the push failed (NIFTY -11, premium 110 -> 103.5) and the sell made +Rs 1,369. Look again.
            logger.info("[%-2s] %s last minute turned for the sell (NIFTY %.0f -> %s, premium %s -> %s) -> AI re-check",
                        side, pat.get("window", ""), pre.spot or 0, f"{spot:.0f}" if spot else "?", pre.ltp,
                        pat.get("option_ltp"))
            v = await self._analyse(ctx, "2/3", "medium", left)
        elif pre_ok:
            v = replace(pre, stage="2/3", note="from the pre-close check")
        elif fresh and not (pre_task and pre_task[0] == c2):
            v = replace(prev, stage="2/3", note="from the 1/3 analysis")
        elif still_running and left < 3.0:
            v = self._follow(f"the analysis is still running after {self.cfg.ai_decide_timeout_secs:.0f} s")
        elif self.llm.budget_left() <= 0:
            self._warn_budget()
            v = self._follow("daily budget used up")
        elif left < 3.0:
            v = (replace(prev, stage="2/3", note="from the 1/3 analysis (no time to re-check)") if fresh
                 else self._follow("no time left for a fresh check"))
        else:
            v = await self._analyse(ctx, "2/3", "medium", left)
        self._final[key] = v
        if len(self._final) > 200:
            for k in list(self._final)[:100]:
                self._final.pop(k, None)
        self.ledger.log_event("AI_DECISION", symbol=side, details=json.dumps(
            {"window": pat.get("window"), "allow": v.allow, "decision": v.decision, "confidence": v.confidence,
             "bar": v.bar, "push": v.push_state, "direction": v.direction, "stats": v.stats, "source": v.source, "note": v.note,
             "sell_price": pat.get("sell_price"), "stop": pat.get("stop"),
             "reasons": v.reasons[:4], "against": v.risks[:3], "similar": v.similar,
             "invalidation": v.invalidation}, separators=(",", ":")))
        return v

    # ── one analysis ────────────────────────────────────────────────────────
    async def _analyse(self, ctx: dict, stage: str, effort: str, timeout: float) -> Verdict:
        pat = ctx["pattern"]
        side = pat["side"]
        snap: dict = {}
        similar = ""
        try:
            snap = await asyncio.to_thread(build_snapshot, self.data, ctx)
            today = datetime.now(_IST).date().isoformat()
            sim = self.cases.similar(snap, before_day=today)
            snap["similar_past_setups"] = sim
            if sim.get("available"):
                s = sim["similar_sells"]
                similar = f"{s['won']}/{s['cases']} won, avg Rs {s['avg_net_rs']}"
            st = self.cases.setup_stats(snap, before_day=today)
            snap["setup_stats"] = st
            bar = self.cfg.ai_min_confidence
            stats = ""
            if st.get("available"):
                stats = "; ".join(f"{r['value']} {r['won']}/{r['sells']} won" for r in st["this_pattern"] if r.get("sells"))
                if st.get("weak_setup"):
                    bar = max(bar, self.cfg.ai_weak_setup_confidence)
                    stats = f"WEAK SETUP ({st['weak_setup']}) -> bar {bar} | " + stats
            snap["confidence_bar_for_this_sell"] = bar
            out, use = await asyncio.wait_for(self.llm.decide(snap, stage, self._news_line(), effort, timeout),
                                              timeout=timeout + 2)
            conf = max(0, min(100, int(out.get("confidence", 0))))
            decision = str(out.get("decision", "SKIP")).upper()
            md = snap.get("market_direction") or {}
            direction = f"{md.get('reading', '?')}({md.get('score', 0):+d})" if md.get("available") else "?"
            v = Verdict(allow=(decision == "SELL" and conf >= bar), decision=decision,
                        confidence=conf, push_state=out.get("push_state", ""),
                        reasons=list(out.get("evidence_for_sell", []))[:4],
                        risks=list(out.get("evidence_against", []))[:4], invalidation=out.get("invalidation", ""),
                        stage=stage, cost_usd=use.usd, usage=use.line(), similar=similar, bar=bar, stats=stats,
                        direction=direction)
            logger.info("[%-2s] %s %s %s | push %s | day %s | $%.3f %.0fs | today $%.2f\n%s", side,
                        pat.get("window", ""), stage, v.headline(), v.push_state or "?", direction, use.usd,
                        use.seconds, self.llm.spend.usd, v.lines())
        except BudgetExceeded as exc:
            self._warn_budget()
            v = self._follow(str(exc))
        except asyncio.TimeoutError:
            v = self._follow(f"no answer within {timeout:.0f} s")
            logger.warning("[%-2s] %s %s — AI gave no answer within %.0f s.", side, pat.get("window", ""), stage, timeout)
        except Exception as exc:                     # any API / data error: never stop the engine
            v = self._follow(f"{type(exc).__name__}: {str(exc)[:120]}")
            logger.warning("[%-2s] %s %s — AI failed: %s", side, pat.get("window", ""), stage, v.note)
        nifty = snap.get("nifty") or {}
        v.spot = nifty.get("spot")
        v.made_at = time.monotonic()
        v.stage = stage
        if stage == "1/3":
            self._latest[side] = v
        elif stage == "2/3-pre":
            v.candle2_end = str(pat.get("candle2_end", ""))
            v.level = pat.get("candle2_ha_low")
            v.ltp = pat.get("option_ltp")
            self._pre[side] = v
        self.ledger.log_event("AI_ANALYSIS", symbol=side, details=json.dumps(
            {"stage": stage, "window": pat.get("window"), "decision": v.decision, "confidence": v.confidence,
             "bar": v.bar, "push": v.push_state, "direction": v.direction, "stats": v.stats, "source": v.source,
             "note": v.note, "spot": v.spot, "similar": similar,
             "usd": round(v.cost_usd, 5), "usage": v.usage, "today_usd": round(self.llm.spend.usd, 4)},
            separators=(",", ":")))
        return v

    @staticmethod
    def _turned_for_sell(side: str, pre: Verdict, spot: Optional[float], ltp) -> bool:
        """Since the pre-close check: NIFTY moved against the push (down for a CE, up for a PE)
        by _TURN_PTS, or our premium fell by _TURN_PCT."""
        if spot and pre.spot:
            moved = pre.spot - spot if side == "CE" else spot - pre.spot
            if moved >= _TURN_PTS:
                return True
        try:
            return bool(pre.ltp and ltp and float(ltp) <= float(pre.ltp) * (1 - _TURN_PCT))
        except (TypeError, ValueError):
            return False

    def _follow(self, why: str) -> Verdict:
        return Verdict(allow=True, decision="FOLLOW", source="fail-open", note=why, made_at=time.monotonic())

    def _warn_budget(self) -> None:
        if not self._budget_warned:
            self._budget_warned = True
            logger.warning("AI agent: daily budget $%.2f used up — patterns trade as usual for the rest of today.",
                           self.cfg.ai_daily_budget_usd)

    # ── news ────────────────────────────────────────────────────────────────
    def _news_line(self) -> str:
        if not self.news_text or self.news_risk == "UNKNOWN":
            return ""
        age = (datetime.now(_IST) - self.news_at).total_seconds() / 60 if self.news_at else 0
        return f"(as of {self.news_at:%H:%M} IST, {age:.0f} min ago) {self.news_text}"

    async def _news_loop(self) -> None:
        while self._running:
            hm = datetime.now(_IST).strftime("%H:%M")
            if _NEWS_FROM <= hm <= _NEWS_TO and self.llm.budget_left() > 0:
                try:
                    text, risk, use = await self.llm.news()
                    self.news_text, self.news_risk, self.news_at = text, risk, datetime.now(_IST)
                    logger.info("AI news (%s risk): %s | %s", risk, text[:300], use.line())
                    self.ledger.log_event("AI_NEWS", details=json.dumps({"risk": risk, "text": text[:1500], "usd": round(use.usd, 5)}))
                except Exception as exc:
                    logger.warning("AI news brief failed: %s", str(exc)[:160])
            await asyncio.sleep(max(60, self.cfg.ai_news_every_mins * 60))
