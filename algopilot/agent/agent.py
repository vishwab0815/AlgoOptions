"""
algopilot/agent/agent.py — TradeAgent: when to analyse, and the verdict the engine gets.

1/3 (candle 1 GREEN on a side)  -> prepare(): a deep analysis starts in the
                                   background (the engine never waits for it).
2/3 (candle 2 RED, order due)   -> decide(): the 1/3 analysis is used if it is
                                   fresh and NIFTY hasn't moved much since;
                                   otherwise a quick re-check, with a time limit.

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

from .data import MarketData
from .features import build_snapshot, spot_now
from .llm import BudgetExceeded, ClaudeClient

logger = logging.getLogger(__name__)
_IST = ZoneInfo("Asia/Kolkata")

_REUSE_SECS = 600.0       # a newer GREEN on the same side re-uses an analysis this recent ...
_REUSE_MOVE_PTS = 15.0    # ... if NIFTY moved less than this since
_FRESH_SECS = 420.0       # at 2/3, the 1/3 analysis counts as fresh for this long ...
_FRESH_MOVE_PTS = 20.0    # ... and while NIFTY is within this many points of where it was
_NEWS_FROM, _NEWS_TO = "08:45", "15:15"


@dataclass
class Verdict:
    allow: bool
    decision: str                       # SELL | SKIP | FOLLOW (agent unavailable -> follow the pattern)
    confidence: int = 0
    regime: str = ""
    nifty_bias: str = ""
    side_alignment: str = ""
    reasons: List[str] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    invalidation: str = ""
    source: str = "ai"                  # ai | fail-open
    note: str = ""
    spot: Optional[float] = None
    made_at: float = 0.0                # time.monotonic()
    stage: str = ""
    cost_usd: float = 0.0
    usage: str = ""

    def headline(self) -> str:
        if self.source != "ai":
            return f"AI unavailable ({self.note}) — following the pattern"
        return f"AI: {self.decision} {self.confidence}% ({self.regime}, {self.side_alignment})"

    def why(self) -> str:
        parts = "; ".join(self.reasons[:5])
        return parts + (f" | wrong if: {self.invalidation}" if self.invalidation else "")


class TradeAgent:
    def __init__(self, config, ledger, client_id: str, access_token: str,
                 scrip_csv: str = "data/dhan_scrip_master.csv") -> None:
        self.cfg = config
        self.ledger = ledger
        self.data = MarketData(client_id, access_token, scrip_csv)
        self.llm = ClaudeClient(config.ai_model, config.ai_news_model, config.ai_daily_budget_usd)
        self._latest: Dict[str, Verdict] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._final: Dict[Tuple[str, str], Verdict] = {}
        self._background: set = set()
        self.news_text, self.news_risk, self.news_at = "", "UNKNOWN", None
        self._budget_warned = False
        self._running = False

    # ── lifecycle ───────────────────────────────────────────────────────────
    async def start(self) -> None:
        self._running = True
        logger.info("AI agent on: strategist %s (effort %s), news %s every %d min, allow at >= %d%% confidence, "
                    "daily budget $%.2f.", self.cfg.ai_model, self.cfg.ai_effort, self.cfg.ai_news_model,
                    self.cfg.ai_news_every_mins, self.cfg.ai_min_confidence, self.cfg.ai_daily_budget_usd)
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

    # ── 1/3 ─────────────────────────────────────────────────────────────────
    def prepare(self, ctx: dict) -> None:
        side = ctx["pattern"]["side"]
        if self.llm.budget_left() <= 0:
            self._warn_budget()
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
        logger.info("[%-2s] %s 1/3 — AI analysis started (%s, effort %s).", side, ctx["pattern"].get("window", ""),
                    self.cfg.ai_model, self.cfg.ai_effort)
        self._tasks[side] = self._spawn(self._analyse(ctx, "1/3", self.cfg.ai_effort, self.cfg.ai_prepare_timeout_secs))

    # ── 2/3 ─────────────────────────────────────────────────────────────────
    async def decide(self, ctx: dict) -> Verdict:
        side = ctx["pattern"]["side"]
        key = (side, str(ctx["pattern"].get("candle2_end", "")))
        if key in self._final:
            return self._final[key]
        deadline = time.monotonic() + self.cfg.ai_decide_timeout_secs
        running = self._tasks.get(side)
        if running is not None and not running.done():
            try:
                await asyncio.wait_for(asyncio.shield(running), timeout=self.cfg.ai_decide_timeout_secs)
            except (asyncio.TimeoutError, Exception):
                pass
        prev = self._latest.get(side)
        spot = await asyncio.to_thread(spot_now, self.data)
        fresh = (prev is not None and prev.source == "ai" and time.monotonic() - prev.made_at <= _FRESH_SECS
                 and (spot is None or prev.spot is None or abs(spot - prev.spot) <= _FRESH_MOVE_PTS))
        left = deadline - time.monotonic()
        if fresh:
            v = replace(prev, stage="2/3", note="from the 1/3 analysis")
        elif running is not None and not running.done():
            v = self._follow(f"the 1/3 analysis is still running after {self.cfg.ai_decide_timeout_secs:.0f} s")
        elif self.llm.budget_left() <= 0:
            self._warn_budget()
            v = self._follow("daily budget used up")
        elif left < 3.0:
            v = self._follow("no time left for a fresh check")
        else:
            v = await self._analyse(ctx, "2/3", "medium", left)
        self._final[key] = v
        if len(self._final) > 200:
            for k in list(self._final)[:100]:
                self._final.pop(k, None)
        self.ledger.log_event("AI_DECISION", symbol=side, details=json.dumps(
            {"window": ctx["pattern"].get("window"), "allow": v.allow, "decision": v.decision, "confidence": v.confidence,
             "regime": v.regime, "alignment": v.side_alignment, "source": v.source, "note": v.note,
             "sell_price": ctx["pattern"].get("sell_price"), "stop": ctx["pattern"].get("stop"),
             "reasons": v.reasons[:5], "invalidation": v.invalidation}, separators=(",", ":")))
        return v

    # ── one analysis ────────────────────────────────────────────────────────
    async def _analyse(self, ctx: dict, stage: str, effort: str, timeout: float) -> Verdict:
        side = ctx["pattern"]["side"]
        snap: dict = {}
        try:
            snap = await asyncio.to_thread(build_snapshot, self.data, ctx)
            out, use = await asyncio.wait_for(self.llm.decide(snap, stage, self._news_line(), effort, timeout),
                                              timeout=timeout + 2)
            conf = max(0, min(100, int(out.get("confidence", 0))))
            decision = str(out.get("decision", "SKIP")).upper()
            v = Verdict(allow=(decision == "SELL" and conf >= self.cfg.ai_min_confidence), decision=decision,
                        confidence=conf, regime=out.get("regime", ""), nifty_bias=out.get("nifty_bias", ""),
                        side_alignment=out.get("side_alignment", ""), reasons=list(out.get("reasons", []))[:5],
                        risks=list(out.get("risks", []))[:3], invalidation=out.get("invalidation", ""),
                        stage=stage, cost_usd=use.usd, usage=use.line())
            logger.info("[%-2s] %s %s — %s | %s | %s | today $%.3f", side, ctx["pattern"].get("window", ""), stage,
                        v.headline(), v.why(), use.line(), self.llm.spend.usd)
        except BudgetExceeded as exc:
            self._warn_budget()
            v = self._follow(str(exc))
        except asyncio.TimeoutError:
            v = self._follow(f"no answer within {timeout:.0f} s")
            logger.warning("[%-2s] %s %s — AI gave no answer within %.0f s.", side, ctx["pattern"].get("window", ""), stage, timeout)
        except Exception as exc:                     # any API / data error: never stop the engine
            v = self._follow(f"{type(exc).__name__}: {str(exc)[:120]}")
            logger.warning("[%-2s] %s %s — AI failed: %s", side, ctx["pattern"].get("window", ""), stage, v.note)
        nifty = snap.get("nifty") or {}
        v.spot = nifty.get("spot")
        v.made_at = time.monotonic()
        v.stage = stage
        if stage == "1/3":
            self._latest[side] = v
        self.ledger.log_event("AI_ANALYSIS", symbol=side, details=json.dumps(
            {"stage": stage, "window": ctx["pattern"].get("window"), "decision": v.decision, "confidence": v.confidence,
             "regime": v.regime, "alignment": v.side_alignment, "source": v.source, "note": v.note, "spot": v.spot,
             "usd": round(v.cost_usd, 5), "usage": v.usage, "today_usd": round(self.llm.spend.usd, 4)},
            separators=(",", ":")))
        return v

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
