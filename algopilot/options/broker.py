"""
algopilot/options/broker.py — REAL order placement on DhanHQ (live mode only).

The strategy never talks to this module directly: OptionsEngine decides WHAT
to do exactly as in paper mode (same signals, same stop levels, same timing)
and calls this only to carry it out. In paper mode it is never constructed.

Endpoints and payloads mirror the installed dhanhq SDK (v2) exactly:
    POST   /v2/orders                          place
    PUT    /v2/orders/{orderId}                modify
    DELETE /v2/orders/{orderId}                cancel
    GET    /v2/orders/{orderId}                status / fill
    GET    /v2/orders/external/{correlationId} look-up by our own tag
    GET    /v2/positions                       broker's view of open positions
    GET    /v2/fundlimit                       available funds

Rules this module keeps no matter what:
  - A placement that fails at the NETWORK level is never blindly re-sent — a
    request can reach Dhan and still time out on the way back, and re-sending
    would open a second real position. It is looked up by its correlationId
    first; only a confirmed "not found" counts as not placed.
  - Every price sent is on NSE's 0.05 tick.
  - A rejection is returned with Dhan's own reason text, never swallowed.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..utils.network import force_ipv4
from ..utils.secrets import SecretStr

# Dhan allows orders only from the registered static IPv4 address. A request
# that leaves over IPv6 (confirmed: this module alone went out as IPv6 and
# Dhan answered ordersAllowed=False) is refused even from the right machine.
force_ipv4()

logger = logging.getLogger(__name__)

_BASE = "https://api.dhan.co/v2"
_TICK = 0.05
_FINAL = {"TRADED", "REJECTED", "CANCELLED", "EXPIRED"}


def to_tick(price: float, up: bool) -> float:
    """Round onto NSE's 0.05 tick — up for a buy stop's trigger/limit (never
    below the intended level), down for a sell limit (never above it)."""
    n = price / _TICK
    n = math.ceil(n - 1e-9) if up else math.floor(n + 1e-9)
    return round(n * _TICK, 2)


def new_tag() -> str:
    """correlationId: our own id on every order, so a placement whose response
    was lost can still be found instead of being re-sent."""
    return "AP" + uuid.uuid4().hex[:16]


@dataclass
class OrderResult:
    order_id: Optional[str]
    status: str                 # TRADED | PART_TRADED | PENDING | REJECTED | CANCELLED | EXPIRED | TIMEOUT | ERROR
    filled_qty: int = 0
    avg_price: float = 0.0
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.filled_qty > 0


class DhanBroker:
    def __init__(self, client_id: str, access_token: SecretStr,
                 exchange_segment: str = "NSE_FNO", product_type: str = "INTRADAY") -> None:
        self.client_id = client_id
        self.segment = exchange_segment
        self.product = product_type
        self._s = requests.Session()
        self._s.headers.update({
            "access-token": access_token.get_secret(), "client-id": client_id,
            "Content-Type": "application/json", "Accept": "application/json",
        })

    def close(self) -> None:
        try:
            self._s.close()
        except Exception:
            pass

    # ── HTTP ─────────────────────────────────────────────────────────────────

    def _call(self, method: str, path: str, payload: Optional[dict] = None,
              timeout: float = 5.0) -> Tuple[int, Any]:
        """(status_code, parsed body). Raises requests.RequestException on a
        network failure — callers decide what that means (see place())."""
        if payload is not None:
            payload = dict(payload, dhanClientId=self.client_id)
        resp = self._s.request(method, _BASE + path, json=payload, timeout=timeout)
        try:
            body = resp.json() if resp.content else {}
        except ValueError:
            body = {"raw": resp.text[:300]}
        return resp.status_code, body

    @staticmethod
    def _reason(code: int, body: Any) -> str:
        if isinstance(body, dict):
            ec = body.get("errorCode") or ""
            msg = body.get("errorMessage") or body.get("omsErrorDescription") or body.get("raw") or ""
            if code in (401, 403) or ec in ("DH-901", "DH-902"):
                return (f"HTTP {code} {ec}: {msg} — token expired/invalid, or this account has no "
                        "Trading API access. Nothing will be placed until it is fixed.")
            if ec == "DH-905" or "ip" in str(msg).lower() and "invalid" in str(msg).lower():
                return (f"{ec}: {msg} — this machine's public IP is not whitelisted for Dhan's order "
                        "APIs. Register it: web.dhan.co -> DhanHQ Trading APIs -> Static IP.")
            return f"HTTP {code} {ec}: {msg}".strip()
        return f"HTTP {code}: {body}"

    # ── Orders ───────────────────────────────────────────────────────────────

    def place(self, side: str, security_id: str, qty: int, order_type: str,
              price: float = 0.0, trigger: float = 0.0, tag: Optional[str] = None) -> Tuple[Optional[str], str]:
        """(order_id, message). order_id None = definitely not placed."""
        tag = tag or new_tag()
        payload = {
            "correlationId": tag, "transactionType": side, "exchangeSegment": self.segment,
            "productType": self.product, "orderType": order_type, "validity": "DAY",
            "securityId": str(security_id), "quantity": int(qty), "disclosedQuantity": 0,
            "price": float(price), "triggerPrice": float(trigger), "afterMarketOrder": False,
        }
        try:
            code, body = self._call("POST", "/orders", payload)
        except requests.RequestException as exc:
            # The request may have landed. Find it by our tag; never re-send.
            logger.warning("Order %s %s x%d: network error (%s) — checking whether Dhan received it.",
                           side, security_id, qty, exc)
            found = self.get_order_by_tag(tag)
            if found and found.get("orderId"):
                return str(found["orderId"]), "placed (confirmed by correlationId after a network error)"
            return None, f"network error and no order found for correlationId {tag}: {exc}"
        if code in (200, 201) and isinstance(body, dict) and body.get("orderId"):
            return str(body["orderId"]), str(body.get("orderStatus", ""))
        return None, self._reason(code, body)

    def modify(self, order_id: str, order_type: str, qty: int, price: float, trigger: float) -> Tuple[bool, str]:
        payload = {"orderId": str(order_id), "orderType": order_type, "legName": "",
                   "quantity": int(qty), "price": float(price), "disclosedQuantity": 0,
                   "triggerPrice": float(trigger), "validity": "DAY"}
        try:
            code, body = self._call("PUT", f"/orders/{order_id}", payload)
        except requests.RequestException as exc:
            return False, f"network error: {exc}"
        return (code in (200, 201, 202)), ("" if code in (200, 201, 202) else self._reason(code, body))

    def cancel(self, order_id: str) -> Tuple[bool, str]:
        try:
            code, body = self._call("DELETE", f"/orders/{order_id}")
        except requests.RequestException as exc:
            return False, f"network error: {exc}"
        return (code in (200, 201, 202)), ("" if code in (200, 201, 202) else self._reason(code, body))

    def get_order(self, order_id: str) -> Optional[dict]:
        try:
            code, body = self._call("GET", f"/orders/{order_id}")
        except requests.RequestException:
            return None
        if code != 200:
            return None
        return body[0] if isinstance(body, list) and body else (body if isinstance(body, dict) else None)

    def get_order_by_tag(self, tag: str) -> Optional[dict]:
        try:
            code, body = self._call("GET", f"/orders/external/{tag}")
        except requests.RequestException:
            return None
        if code != 200:
            return None
        return body[0] if isinstance(body, list) and body else (body if isinstance(body, dict) else None)

    def positions(self) -> Optional[List[dict]]:
        try:
            code, body = self._call("GET", "/positions")
        except requests.RequestException:
            return None
        return body if code == 200 and isinstance(body, list) else None

    def available_funds(self) -> Optional[float]:
        try:
            code, body = self._call("GET", "/fundlimit")
        except requests.RequestException:
            return None
        if code != 200 or not isinstance(body, dict):
            return None
        # Dhan's own field name is misspelt "availabelBalance".
        for key in ("availabelBalance", "availableBalance"):
            if key in body:
                try:
                    return float(body[key])
                except (TypeError, ValueError):
                    return None
        return None

    @staticmethod
    def parse(order: Optional[dict], order_id: Optional[str]) -> OrderResult:
        if not order:
            return OrderResult(order_id, "ERROR", message="order status unavailable")
        status = str(order.get("orderStatus", "")).upper()
        filled = int(float(order.get("filledQty", 0) or 0))
        avg = float(order.get("averageTradedPrice", 0) or 0)
        msg = str(order.get("omsErrorDescription") or "")
        return OrderResult(order_id, status, filled, avg, msg)

    # ── async wrappers (never block the event loop) ──────────────────────────

    async def place_async(self, *a, **k):
        return await asyncio.to_thread(self.place, *a, **k)

    async def modify_async(self, *a, **k):
        return await asyncio.to_thread(self.modify, *a, **k)

    async def cancel_async(self, *a, **k):
        return await asyncio.to_thread(self.cancel, *a, **k)

    async def status_async(self, order_id: str) -> OrderResult:
        return self.parse(await asyncio.to_thread(self.get_order, order_id), order_id)

    async def positions_async(self):
        return await asyncio.to_thread(self.positions)

    async def funds_async(self):
        return await asyncio.to_thread(self.available_funds)

    async def wait_for_fill(self, order_id: str, timeout: float = 6.0, poll: float = 0.25) -> OrderResult:
        """Poll until the order is final or `timeout` passes. On timeout the
        remainder is cancelled and whatever filled is returned — a market
        order still working after several seconds should not be left live."""
        deadline = time.monotonic() + timeout
        res = OrderResult(order_id, "PENDING")
        while time.monotonic() < deadline:
            res = await self.status_async(order_id)
            if res.status in _FINAL:
                return res
            await asyncio.sleep(poll)
        await self.cancel_async(order_id)
        final = await self.status_async(order_id)
        final.status = final.status if final.status in _FINAL else "TIMEOUT"
        final.message = (final.message + " | " if final.message else "") + f"not complete after {timeout:.0f}s, remainder cancelled"
        return final
