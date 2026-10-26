"""
algopilot/options/kite_api.py — the one place that talks HTTP to Zerodha Kite Connect v3.

    Base URL      https://api.kite.trade
    Headers       X-Kite-Version: 3
                  Authorization: token <api_key>:<access_token>
    POST / PUT    form-encoded (application/x-www-form-urlencoded); the margin
                  calculator alone takes a JSON body
    Response      {"status": "success", "data": ...}
    Error         {"status": "error", "message": "...", "error_type": "TokenException" | "InputException" |
                   "OrderException" | "NetworkException" | "PermissionException" | "MarginException" |
                   "GeneralException" | "DataException"}  with HTTP 400 / 403 / 404 / 405 / 410 / 429 / 500 / 502 / 503 / 504

Rate limits (per api_key): quotes 1 request / s, historical candles 3 / s, order
placement 10 / s (and 200 / min, 3,000 / day), everything else 10 / s. Each
family has its own pacing gate here, so a burst on one never starves another.

The engine's network calls run in worker threads (asyncio.to_thread), so this
module is plain blocking `requests`; one Session per caller keeps the TCP + TLS
connection open (an order on an open connection costs ~40 ms, a new one ~300 ms).
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests

from ..utils.network import force_ipv4
from ..utils.secrets import SecretStr

force_ipv4()                      # Kite accepts orders only from the registered static IP (IPv4)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.kite.trade"
KITE_VERSION = "3"

# requests per second, per endpoint family (Kite Connect documented limits, kept just under)
_RATES = {"quote": 1.0, "historical": 3.0, "order": 9.0, "other": 9.0}
_AUTH_FAIL_LOG_EVERY_SECS = 60.0


class KiteError(RuntimeError):
    """A Kite error reply: HTTP status, Kite's error_type and message."""

    def __init__(self, status: int, error_type: str, message: str) -> None:
        super().__init__(f"HTTP {status} {error_type}: {message}")
        self.status, self.error_type, self.message = status, error_type, message


@dataclass
class KiteReply:
    status: int
    ok: bool
    data: Any
    error_type: str = ""
    message: str = ""

    def reason(self) -> str:
        return f"HTTP {self.status} {self.error_type}: {self.message}".strip()


class _Pace:
    """At most `rate` calls per second for one endpoint family (blocking, thread-safe)."""

    def __init__(self, rate: float) -> None:
        self._gap = 1.0 / rate
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
                now = time.monotonic()
            self._next = now + self._gap


# One gate per family for the whole process: the engine's data client and the
# broker share the same api_key limits.
_PACE = {k: _Pace(v) for k, v in _RATES.items()}


class KiteHTTP:
    """Blocking Kite Connect REST access with Kite's headers, envelope and errors."""

    def __init__(self, api_key: str, access_token: SecretStr, timeout: float = 10.0) -> None:
        self.api_key = api_key
        self._token = access_token
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "X-Kite-Version": KITE_VERSION,
            "Authorization": f"token {api_key}:{access_token.get_secret()}",
            "User-Agent": "AlgoPilotX/kite-connect",
        })
        self.auth_failed = False          # set on a TokenException (expired / invalid access token)
        self.last_call_at = 0.0
        self._last_auth_log = -1e9

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def call(self, method: str, path: str, *, family: str = "other", params: Any = None,
             data: Optional[Dict[str, Any]] = None, json_body: Any = None,
             timeout: Optional[float] = None) -> KiteReply:
        """One request. Raises requests.RequestException on a NETWORK failure (the
        caller decides what that means — an order may still have reached Kite);
        every HTTP answer, success or error, comes back as a KiteReply."""
        _PACE.get(family, _PACE["other"]).wait()
        self.last_call_at = time.monotonic()
        resp = self.session.request(method, BASE_URL + path, params=params, data=data, json=json_body,
                                    timeout=timeout or self.timeout)
        return self._reply(resp, path)

    def _reply(self, resp: requests.Response, path: str) -> KiteReply:
        ctype = resp.headers.get("Content-Type", "")
        body: Any = None
        if "json" in ctype:
            try:
                body = resp.json()
            except ValueError:
                body = None
        if isinstance(body, dict) and body.get("status") == "success":
            return KiteReply(resp.status_code, True, body.get("data"))
        if resp.status_code == 200 and body is None:
            return KiteReply(resp.status_code, True, resp.text)          # CSV (instruments) and similar
        err_type = str((body or {}).get("error_type") or "") if isinstance(body, dict) else ""
        msg = str((body or {}).get("message") or "") if isinstance(body, dict) else resp.text[:300]
        if resp.status_code == 403 or err_type == "TokenException":
            self.auth_failed = True
            now = time.monotonic()
            if now - self._last_auth_log >= _AUTH_FAIL_LOG_EVERY_SECS:
                self._last_auth_log = now
                logger.error(
                    "KITE REJECTED THE ACCESS TOKEN (HTTP %d %s on %s: %s). It expires every morning around "
                    "06:00 and a new login replaces it. Log in again (python scripts/kite_login.py) and restart.",
                    resp.status_code, err_type or "TokenException", path, msg)
        return KiteReply(resp.status_code, False, body.get("data") if isinstance(body, dict) else None,
                         err_type or f"HTTP{resp.status_code}", msg)


def ok_or_raise(reply: KiteReply) -> Any:
    if not reply.ok:
        raise KiteError(reply.status, reply.error_type, reply.message)
    return reply.data


def kite_get(http: KiteHTTP, path: str, family: str = "other", **params) -> Tuple[bool, Any, str]:
    """(ok, data, reason) — network errors become (False, None, reason)."""
    try:
        r = http.call("GET", path, family=family, params=params or None)
    except requests.RequestException as exc:
        return False, None, f"network error: {exc}"
    return r.ok, r.data, ("" if r.ok else r.reason())
