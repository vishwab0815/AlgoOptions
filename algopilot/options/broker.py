"""
algopilot/options/broker.py — REAL order placement on Zerodha Kite Connect v3 (live mode only).

The strategy never talks to this module directly: OptionsEngine decides WHAT
to do exactly as in paper mode (same signals, same stop levels, same timing)
and calls this only to carry it out. In paper mode it is never constructed.

    POST   /orders/regular                 place      (form-encoded)
    PUT    /orders/regular/{order_id}      modify
    DELETE /orders/regular/{order_id}      cancel
    GET    /orders/{order_id}              order history (array, newest last)
    GET    /orders                         today's order book
    GET    /trades                         today's fills
    GET    /portfolio/positions            {"net": [...], "day": [...]}  (a short has quantity < 0)
    GET    /user/margins/equity            "net" = money free for a new F&O order

Kite specifics this module handles so the engine doesn't have to:
  - Orders go by exchange + tradingsymbol; the engine identifies a contract by
    its instrument_token (as text). The instrument registry maps one to the other.
  - Order types: the engine's STOP_LOSS = Kite "SL" (stop-limit; SL-M is
    blocked on index options), MARKET / LIMIT as they are. Every MARKET order
    carries market_protection=-1 (required since 1-Apr-2026); Kite turns it
    into a limit at a protection price, which can stay OPEN in a fast market —
    wait_for_fill cancels any remainder after its timeout.
  - Statuses are mapped onto the engine's vocabulary:
        COMPLETE -> TRADED | CANCELLED | REJECTED
        OPEN with a partial fill -> PART_TRADED | OPEN (triggered SL) -> TRIGGERED
        OPEN (limit) / TRIGGER PENDING -> PENDING | every in-between state -> TRANSIT
  - A modify / cancel of an order that is finished or mid-change is refused
    ("being processed. Try later."); callers re-read the order instead of retrying.
  - At most 25 modifications per order: the engine replaces the stop when a move
    is refused (see engine._sync_protective_stop_once).

Rules kept no matter what:
  - A placement that fails at the NETWORK level is never blindly re-sent — it
    may have reached Kite. It is looked up by its tag first; only a confirmed
    "not found" counts as not placed.
  - Every price sent is on the 0.05 tick.
  - A rejection is returned with Kite's own reason text, never swallowed.
  - Orders are accepted only from the static IP registered on developers.kite.trade.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

from .instruments import InstrumentRegistry
from .kite_api import KiteHTTP
from ..utils.secrets import SecretStr

logger = logging.getLogger(__name__)

_TICK = 0.05
_FINAL = {"TRADED", "REJECTED", "CANCELLED", "EXPIRED"}
_KITE_TYPE = {"STOP_LOSS": "SL", "STOP_LOSS_MARKET": "SL-M", "MARKET": "MARKET", "LIMIT": "LIMIT"}
_ENGINE_TYPE = {"SL": "STOP_LOSS", "SL-M": "STOP_LOSS_MARKET", "MARKET": "MARKET", "LIMIT": "LIMIT"}
_MARKET_PROTECTION = "-1"          # automatic protection (Kite's own percentage table)


def to_tick(price: float, up: bool) -> float:
    """Round onto the 0.05 tick — up for a buy stop's trigger/limit (never below
    the intended level), down for a sell limit (never above it)."""
    n = price / _TICK
    n = math.ceil(n - 1e-9) if up else math.floor(n + 1e-9)
    return round(n * _TICK, 2)


def new_tag() -> str:
    """Our own id on every order (alphanumeric, Kite allows up to 20), so a placement
    whose response was lost can still be found instead of being re-sent."""
    return "AP" + uuid.uuid4().hex[:16]


def engine_status(order: dict) -> str:
    """Kite order status -> the engine's status words."""
    st = str(order.get("status") or "").upper()
    qty = int(float(order.get("quantity") or 0))
    filled = int(float(order.get("filled_quantity") or 0))
    if st == "COMPLETE":
        return "TRADED"
    if st in ("REJECTED", "CANCELLED"):
        return st
    if st == "OPEN":
        if 0 < filled < qty:
            return "PART_TRADED"
        return "TRIGGERED" if str(order.get("order_type") or "").upper() in ("SL", "SL-M") else "PENDING"
    if st == "TRIGGER PENDING":
        return "PENDING"
    return "TRANSIT"                 # PUT ORDER REQ RECEIVED, VALIDATION PENDING, OPEN PENDING, MODIFY ..., CANCEL PENDING, UPDATE


def _tag_of(order: dict) -> str:
    tag = order.get("tag") or ""
    if tag:
        return str(tag)
    for t in order.get("tags") or []:
        if str(t).startswith("AP"):
            return str(t)
    return ""


def normalize_order(o: dict) -> dict:
    """A Kite order in the shape the engine reads (it was written against Dhan's field names)."""
    return {
        "orderId": str(o.get("order_id") or ""), "orderStatus": engine_status(o),
        "transactionType": str(o.get("transaction_type") or "").upper(),
        "orderType": _ENGINE_TYPE.get(str(o.get("order_type") or "").upper(), str(o.get("order_type") or "")),
        "correlationId": _tag_of(o), "tradingSymbol": o.get("tradingsymbol"),
        "securityId": str(o.get("instrument_token") or ""), "triggerPrice": o.get("trigger_price"),
        "price": o.get("price"), "quantity": o.get("quantity"), "filledQty": o.get("filled_quantity"),
        "averageTradedPrice": o.get("average_price"), "kiteStatus": o.get("status"),
        "statusMessage": o.get("status_message") or o.get("status_message_raw") or "",
    }


class OrderUpdateStream:
    """Kite pushes every change to your orders over the WebSocket as a text message
    {"type": "order", "data": {...order...}} — for all of the account's orders. Waiting
    on this instead of asking "is it filled yet?" over REST is what makes fill
    confirmation fast. A connection of its own (Kite allows 3 per api_key), no
    instruments subscribed; it reconnects by itself and logs nothing secret.
    Updates are snapshots that can repeat or arrive out of order, so a status never
    goes back from final, and REST stays the source of truth."""

    def __init__(self, api_key: str, access_token: SecretStr) -> None:
        self._api_key = api_key
        self._token = access_token
        self._updates: Dict[str, dict] = {}
        self._events: Dict[str, asyncio.Event] = {}
        self._stop = False
        self.connected = False
        self._ws = None

    async def run(self) -> None:
        import websockets
        url = f"wss://ws.kite.trade?api_key={self._api_key}&access_token={self._token.get_secret()}"
        delay = 1.0
        while not self._stop:
            try:
                async with websockets.connect(url, additional_headers={"X-Kite-Version": "3"}, open_timeout=10,
                                              close_timeout=2, ping_interval=None) as ws:
                    self._ws = ws
                    self.connected, delay = True, 1.0
                    logger.info("Order-update stream connected (fills are pushed by Kite).")
                    async for raw in ws:
                        if not isinstance(raw, (bytes, bytearray)):
                            self._on_message(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._stop:
                    logger.warning("Order-update stream dropped (%s) — reconnecting in %.0fs; fills are still "
                                   "confirmed over REST meanwhile.", type(exc).__name__, delay)
            finally:
                self.connected, self._ws = False, None
            if not self._stop:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def stop(self) -> None:
        self._stop = True
        ws = self._ws
        if ws is not None:
            try:
                await asyncio.wait_for(ws.close(), 2.5)
            except Exception:
                pass

    def _on_message(self, raw) -> None:
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        if msg.get("type") != "order" or not isinstance(msg.get("data"), dict):
            return
        self.ingest(msg["data"])

    def ingest(self, data: dict) -> None:
        oid = str(data.get("order_id") or "")
        if not oid:
            return
        prev = self._updates.get(oid)
        if prev is not None and engine_status(prev) in _FINAL and engine_status(data) not in _FINAL:
            return                                   # a late / repeated update: never go back from final
        self._updates[oid] = data
        ev = self._events.get(oid)
        if ev is not None:
            ev.set()

    def latest(self, order_id: str) -> Optional[dict]:
        return self._updates.get(str(order_id))

    async def wait_change(self, order_id: str, timeout: float) -> None:
        ev = self._events.setdefault(str(order_id), asyncio.Event())
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        ev.clear()

    @staticmethod
    def to_result(order_id: str, data: dict) -> "OrderResult":
        return KiteBroker.parse(data, order_id)


@dataclass
class OrderResult:
    order_id: Optional[str]
    status: str                 # TRADED | PART_TRADED | PENDING | TRIGGERED | TRANSIT | REJECTED | CANCELLED | TIMEOUT | ERROR
    filled_qty: int = 0
    avg_price: float = 0.0
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.filled_qty > 0


class KiteBroker:
    def __init__(self, api_key: str, access_token: SecretStr, registry: InstrumentRegistry,
                 exchange: str = "NFO", product: str = "MIS") -> None:
        self.http = KiteHTTP(api_key, access_token, timeout=5.0)
        self.registry = registry
        self.exchange = exchange
        self.product = product
        self.stream: Optional[OrderUpdateStream] = None   # set by the engine in live mode

    @property
    def last_call_at(self) -> float:
        return self.http.last_call_at

    def close(self) -> None:
        self.http.close()

    # ── helpers ─────────────────────────────────────────────────────────────
    @staticmethod
    def _reason(reply) -> str:
        msg = reply.message or ""
        if re.search(r"not allowed to place orders|No IPs are configured", msg, re.IGNORECASE):
            return (f"{reply.reason()} — this machine's public IP is not registered for Kite orders. Register it: "
                    "developers.kite.trade -> Profile -> IP Whitelist (IPv4, the address orders leave from).")
        if reply.status == 403 or reply.error_type == "TokenException":
            return f"{reply.reason()} — the access token has expired (06:00 daily) or was replaced by a new login."
        return reply.reason()

    def _symbol(self, security_id: str) -> Optional[str]:
        ins = self.registry.token(security_id)
        if ins is None:
            self.registry.ensure()
            ins = self.registry.token(security_id)
        return ins.tradingsymbol if ins is not None else None

    # ── orders ──────────────────────────────────────────────────────────────
    def place(self, side: str, security_id: str, qty: int, order_type: str,
              price: float = 0.0, trigger: float = 0.0, tag: Optional[str] = None) -> Tuple[Optional[str], str]:
        """(order_id, message). order_id None = definitely not placed."""
        tag = tag or new_tag()
        symbol = self._symbol(security_id)
        if symbol is None:
            return None, f"unknown contract {security_id} (not in today's Kite instrument list)"
        ktype = _KITE_TYPE.get(order_type, order_type)
        data: Dict[str, Any] = {"tradingsymbol": symbol, "exchange": self.exchange, "transaction_type": side,
                                "order_type": ktype, "quantity": int(qty), "product": self.product,
                                "validity": "DAY", "tag": tag}
        if ktype in ("LIMIT", "SL"):
            data["price"] = f"{float(price):.2f}"
        if ktype in ("SL", "SL-M"):
            data["trigger_price"] = f"{float(trigger):.2f}"
        if ktype in ("MARKET", "SL-M"):
            data["market_protection"] = _MARKET_PROTECTION
        try:
            reply = self.http.call("POST", "/orders/regular", family="order", data=data)
        except requests.RequestException as exc:
            logger.warning("Order %s %s x%d: network error (%s) — checking whether Kite received it.",
                           side, symbol, qty, exc)
            found = self.get_order_by_tag(tag)
            if found and found.get("order_id"):
                return str(found["order_id"]), "placed (confirmed by tag after a network error)"
            return None, f"network error and no order found with tag {tag}: {exc}"
        if reply.ok and isinstance(reply.data, dict) and reply.data.get("order_id"):
            return str(reply.data["order_id"]), "placed"
        return None, self._reason(reply)

    def modify(self, order_id: str, order_type: str, qty: int, price: float, trigger: float) -> Tuple[bool, str]:
        ktype = _KITE_TYPE.get(order_type, order_type)
        data: Dict[str, Any] = {"order_type": ktype, "quantity": int(qty), "validity": "DAY"}
        if ktype in ("LIMIT", "SL"):
            data["price"] = f"{float(price):.2f}"
        if ktype in ("SL", "SL-M"):
            data["trigger_price"] = f"{float(trigger):.2f}"
        if ktype in ("MARKET", "SL-M"):
            data["market_protection"] = _MARKET_PROTECTION
        try:
            reply = self.http.call("PUT", f"/orders/regular/{order_id}", family="order", data=data)
        except requests.RequestException as exc:
            return False, f"network error: {exc}"
        return reply.ok, ("" if reply.ok else self._reason(reply))

    def cancel(self, order_id: str) -> Tuple[bool, str]:
        try:
            reply = self.http.call("DELETE", f"/orders/regular/{order_id}", family="order")
        except requests.RequestException as exc:
            return False, f"network error: {exc}"
        return reply.ok, ("" if reply.ok else self._reason(reply))

    def get_order(self, order_id: str) -> Optional[dict]:
        """The order's current state (the last entry of its history), raw Kite fields."""
        try:
            reply = self.http.call("GET", f"/orders/{order_id}")
        except requests.RequestException:
            return None
        if not reply.ok or not isinstance(reply.data, list) or not reply.data:
            return None
        return reply.data[-1]

    def get_order_by_tag(self, tag: str) -> Optional[dict]:
        try:
            reply = self.http.call("GET", "/orders")
        except requests.RequestException:
            return None
        if not reply.ok or not isinstance(reply.data, list):
            return None
        hits = [o for o in reply.data if o.get("tag") == tag or tag in (o.get("tags") or [])]
        return hits[-1] if hits else None

    def orders(self) -> Optional[List[dict]]:
        """Today's order book, in the engine's field names."""
        try:
            reply = self.http.call("GET", "/orders")
        except requests.RequestException:
            return None
        if not reply.ok or not isinstance(reply.data, list):
            return None
        return [normalize_order(o) for o in reply.data if o.get("exchange") == self.exchange]

    def trades(self) -> Optional[List[dict]]:
        """Today's fills: securityId, transactionType, tradedQuantity, tradedPrice, exchangeTime (IST text)."""
        try:
            reply = self.http.call("GET", "/trades")
        except requests.RequestException:
            return None
        if not reply.ok or not isinstance(reply.data, list):
            return None
        return [{"securityId": str(t.get("instrument_token") or ""), "orderId": str(t.get("order_id") or ""),
                 "transactionType": str(t.get("transaction_type") or "").upper(),
                 "tradedQuantity": t.get("quantity"), "tradedPrice": t.get("average_price"),
                 "exchangeTime": t.get("fill_timestamp") or t.get("exchange_timestamp"),
                 "tradingSymbol": t.get("tradingsymbol")}
                for t in reply.data if t.get("exchange") == self.exchange]

    def positions(self) -> Optional[List[dict]]:
        """Open positions (Kite's "net" list): securityId, exchangeSegment, netQty (short < 0), buyAvg, tradingSymbol."""
        try:
            reply = self.http.call("GET", "/portfolio/positions")
        except requests.RequestException:
            return None
        if not reply.ok or not isinstance(reply.data, dict):
            return None
        return [{"securityId": str(p.get("instrument_token") or ""), "exchangeSegment": str(p.get("exchange") or ""),
                 "netQty": p.get("quantity"), "buyAvg": p.get("buy_price"), "sellAvg": p.get("sell_price"),
                 "tradingSymbol": p.get("tradingsymbol"), "productType": p.get("product")}
                for p in reply.data.get("net") or []]

    def available_funds(self) -> Optional[float]:
        try:
            reply = self.http.call("GET", "/user/margins/equity")
        except requests.RequestException:
            return None
        if not reply.ok or not isinstance(reply.data, dict):
            return None
        seg = reply.data.get("equity", reply.data)
        try:
            return float(seg.get("net"))
        except (TypeError, ValueError, AttributeError):
            return None

    @staticmethod
    def parse(order: Optional[dict], order_id: Optional[str]) -> OrderResult:
        if not order:
            return OrderResult(order_id, "ERROR", message="order status unavailable")
        filled = int(float(order.get("filled_quantity") or 0))
        avg = float(order.get("average_price") or 0)
        msg = str(order.get("status_message") or order.get("status_message_raw") or "")
        return OrderResult(order_id, engine_status(order), filled, avg, msg)

    # ── async wrappers (never block the event loop) ─────────────────────────
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

    async def trades_async(self):
        return await asyncio.to_thread(self.trades)

    async def orders_async(self):
        return await asyncio.to_thread(self.orders)

    async def funds_async(self):
        return await asyncio.to_thread(self.available_funds)

    async def wait_for_fill(self, order_id: str, timeout: float = 6.0, exact: bool = False) -> OrderResult:
        """Wait until the order is final, then return its fill.

        The pushed order update (if the stream is connected) answers the moment Kite
        knows; REST is asked every 100 ms as well, in case a push is missed.
        exact=True re-reads the final fill over REST before returning (used for exits,
        whose price is booked straight into the trade record).

        On timeout the remainder is cancelled and whatever filled is returned — a
        market order (a protected limit at Kite) still working after several seconds
        must not be left live."""
        deadline = time.monotonic() + timeout
        live = bool(self.stream and self.stream.connected)
        next_poll = time.monotonic() + (0.1 if live else 0.0)
        poll_task: Optional[asyncio.Task] = None
        try:
            while time.monotonic() < deadline:
                pushed = self.stream.latest(order_id) if self.stream else None
                if pushed and engine_status(pushed) in _FINAL:
                    res = self.parse(pushed, order_id)
                    if exact or res.avg_price <= 0:
                        rest = await self.status_async(order_id)
                        if rest.status in _FINAL:
                            return rest
                    return res
                if poll_task is None and time.monotonic() >= next_poll:
                    poll_task = asyncio.ensure_future(self.status_async(order_id))
                if poll_task is not None and poll_task.done():
                    res = poll_task.result()
                    poll_task = None
                    if res.status in _FINAL:
                        return res
                    next_poll = time.monotonic() + 0.1
                    continue
                wait = max(0.0, min(next_poll if poll_task is None else deadline, deadline) - time.monotonic())
                waiters = []
                if self.stream and self.stream.connected:
                    waiters.append(asyncio.ensure_future(self.stream.wait_change(order_id, wait)))
                if poll_task is not None:
                    waiters.append(poll_task)
                if waiters:
                    await asyncio.wait(waiters, timeout=wait, return_when=asyncio.FIRST_COMPLETED)
                    for w in waiters:
                        if w is not poll_task and not w.done():
                            w.cancel()
                else:
                    await asyncio.sleep(wait)
        finally:
            if poll_task is not None and not poll_task.done():
                poll_task.cancel()
        await self.cancel_async(order_id)
        final = await self.status_async(order_id)
        final.status = final.status if final.status in _FINAL else "TIMEOUT"
        final.message = (final.message + " | " if final.message else "") + f"not complete after {timeout:.0f}s, remainder cancelled"
        return final
