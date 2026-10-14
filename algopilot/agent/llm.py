"""
algopilot/agent/llm.py — the calls to Claude: the strategist decision (structured
JSON) and the news brief (web search). Tracks tokens and cost per call and per
day, and refuses to spend past the daily budget.

The API key is read by the SDK from ANTHROPIC_API_KEY (loaded from .env).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from .prompts import DECISION_SCHEMA, NEWS_SYSTEM, STRATEGIST_SYSTEM, strategist_user

logger = logging.getLogger(__name__)
_IST = ZoneInfo("Asia/Kolkata")

# $ per million tokens: (input, output, cache read, cache write). Web search: $10 per 1,000 searches.
_PRICES = {
    "claude-fable-5-1": (10.0, 50.0, 1.00, 12.50),
    "claude-opus-5-5": (4.0, 20.0, 0.20, 5.00),
    "claude-opus-5": (5.0, 25.0, 0.50, 6.25),
    "claude-sonnet-5-5": (2.0, 10.0, 0.20, 2.50),
    "claude-haiku-5-5": (0.10, 0.50, 0.01, 0.125),
}
_WEB_SEARCH_USD = 0.01


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class Usage:
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read: int = 0
    cache_write: int = 0
    web_searches: int = 0
    usd: float = 0.0
    seconds: float = 0.0

    def line(self) -> str:
        return (f"{self.model}: in {self.input_tokens} (+{self.cache_read} cached, {self.cache_write} cache-write) "
                f"out {self.output_tokens}{f' web {self.web_searches}' if self.web_searches else ''} "
                f"= ${self.usd:.4f} in {self.seconds:.1f}s")


@dataclass
class DailySpend:
    day: Optional[date] = None
    usd: float = 0.0
    calls: int = 0
    by_model: dict = field(default_factory=dict)


class ClaudeClient:
    def __init__(self, model: str, news_model: str, daily_budget_usd: float) -> None:
        import anthropic                      # imported only when the agent is on
        self._anthropic = anthropic
        self.client = anthropic.AsyncAnthropic(max_retries=1)
        self.model, self.news_model, self.budget = model, news_model, daily_budget_usd
        self.spend = DailySpend()
        self._fallbacks_ok = True

    async def close(self) -> None:
        await self.client.close()

    # ── cost ────────────────────────────────────────────────────────────────
    def _roll(self) -> None:
        today = datetime.now(_IST).date()
        if self.spend.day != today:
            self.spend = DailySpend(day=today)

    def budget_left(self) -> float:
        self._roll()
        return self.budget - self.spend.usd

    def _account(self, model: str, u, seconds: float) -> Usage:
        p_in, p_out, p_cr, p_cw = _PRICES.get(model, _PRICES["claude-opus-5-5"])
        searches = 0
        stu = getattr(u, "server_tool_use", None)
        if stu is not None:
            searches = int(getattr(stu, "web_search_requests", 0) or 0)
        use = Usage(model=model, input_tokens=int(u.input_tokens or 0), output_tokens=int(u.output_tokens or 0),
                    cache_read=int(getattr(u, "cache_read_input_tokens", 0) or 0),
                    cache_write=int(getattr(u, "cache_creation_input_tokens", 0) or 0), web_searches=searches,
                    seconds=seconds)
        use.usd = (use.input_tokens * p_in + use.output_tokens * p_out + use.cache_read * p_cr
                   + use.cache_write * p_cw) / 1e6 + searches * _WEB_SEARCH_USD
        self._roll()
        self.spend.usd += use.usd
        self.spend.calls += 1
        self.spend.by_model[model] = self.spend.by_model.get(model, 0.0) + use.usd
        return use

    # ── the strategist decision ─────────────────────────────────────────────
    async def decide(self, snapshot: dict, stage: str, news: str, effort: str, timeout: float) -> Tuple[dict, Usage]:
        if self.budget_left() <= 0:
            raise BudgetExceeded(f"daily AI budget ${self.budget:.2f} used up")
        kwargs = dict(
            model=self.model, max_tokens=16000,
            system=[{"type": "text", "text": STRATEGIST_SYSTEM, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": strategist_user(json.dumps(snapshot, separators=(",", ":")), stage, news)}],
            output_config={"effort": effort, "format": {"type": "json_schema", "schema": DECISION_SCHEMA}},
        )
        t0 = asyncio.get_running_loop().time()
        resp = await asyncio.wait_for(self._create_with_fallback(kwargs, timeout), timeout=timeout)
        use = self._account(self.model, resp.usage, asyncio.get_running_loop().time() - t0)
        if resp.stop_reason == "refusal":
            raise RuntimeError("the model declined to answer")
        text = next((b.text for b in resp.content if getattr(b, "type", "") == "text"), "")
        return json.loads(text), use

    async def _create_with_fallback(self, kwargs: dict, timeout: float):
        """Opus 5.5 with the server-side refusal fallback; if the API rejects that option
        for this request shape, the call is repeated without it (and remembered)."""
        client = self.client.with_options(timeout=timeout)
        if self._fallbacks_ok:
            try:
                return await client.beta.messages.create(betas=["server-side-fallback-2026-07-01"], fallbacks="default",
                                                         **kwargs)
            except self._anthropic.BadRequestError as exc:
                if "fallback" not in str(exc).lower():
                    raise
                self._fallbacks_ok = False
                logger.warning("agent: refusal fallback not accepted (%s) — calling without it.", exc)
        return await client.messages.create(**kwargs)

    # ── the news brief ──────────────────────────────────────────────────────
    async def news(self, timeout: float = 120.0) -> Tuple[str, str, Usage]:
        if self.budget_left() <= 0:
            raise BudgetExceeded(f"daily AI budget ${self.budget:.2f} used up")
        now = datetime.now(_IST)
        messages = [{"role": "user", "content": f"Now: {now:%A %d %B %Y, %H:%M} IST. Brief me."}]
        # No site list: several big Indian news sites block Anthropic's search crawler, and naming one
        # makes the whole request fail (seen 14-Oct). The open web search finds the same stories.
        tools = [{"type": "web_search_20260209", "name": "web_search", "max_uses": 3}]
        client = self.client.with_options(timeout=timeout)
        texts, use = [], Usage(model=self.news_model)
        for _ in range(3):                              # a long search may pause the turn; continue it
            t0 = asyncio.get_running_loop().time()
            resp = await asyncio.wait_for(client.messages.create(
                model=self.news_model, max_tokens=4000, system=NEWS_SYSTEM, tools=tools,
                output_config={"effort": "low"}, messages=messages), timeout=timeout)
            part = self._account(self.news_model, resp.usage, asyncio.get_running_loop().time() - t0)
            for f in ("input_tokens", "output_tokens", "cache_read", "cache_write", "web_searches", "usd", "seconds"):
                setattr(use, f, getattr(use, f) + getattr(part, f))
            texts += [b.text for b in resp.content if getattr(b, "type", "") == "text"]
            if resp.stop_reason != "pause_turn":
                break
            messages = messages + [{"role": "assistant", "content": resp.content}]
        text = " ".join(t.strip() for t in texts if t.strip())
        m = re.search(r"RISK:\s*(LOW|MEDIUM|HIGH|UNKNOWN)", text, re.I)
        return text, (m.group(1).upper() if m else "UNKNOWN"), use
