"""
algopilot/options/kite_auth.py — the daily Kite Connect access token.

Zerodha requires a person to log in every trading day (user id, password, TOTP
on Zerodha's own page); there is no API that takes a password or TOTP, and no
refresh token for personal apps. The token then works until 06:00 IST the next
morning (a fixed time, not 24 hours).

    1. open  https://kite.zerodha.com/connect/login?v=3&api_key=<api_key>
    2. after the login Kite redirects to the app's registered redirect URL with
       ?request_token=...  (one-time, valid a few minutes)
    3. POST https://api.kite.trade/session/token   (form-encoded, header X-Kite-Version: 3)
           api_key, request_token, checksum = sha256(api_key + request_token + api_secret) hex
       -> data.access_token, data.user_id, data.login_time
    4. every call: Authorization: token <api_key>:<access_token>

scripts/kite_login.py runs steps 1-3 and writes KITE_ACCESS_TOKEN and
KITE_LOGIN_AT to .env. The API secret lives in secrets.env, never in .env.
The engine never calls DELETE /session/token: that would end the day's session.
"""
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from ..utils.network import force_ipv4

force_ipv4()

_IST = ZoneInfo("Asia/Kolkata")
ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
SECRETS_FILE = ROOT / "secrets.env"
LOGIN_URL = "https://kite.zerodha.com/connect/login"
API = "https://api.kite.trade"


class TokenError(RuntimeError):
    pass


def login_url(api_key: str) -> str:
    return f"{LOGIN_URL}?v=3&api_key={api_key}"


def checksum(api_key: str, request_token: str, api_secret: str) -> str:
    return hashlib.sha256((api_key + request_token + api_secret).encode("utf-8")).hexdigest()


def request_token_from(text: str) -> str:
    """A request_token, or the redirect URL that carries one."""
    m = re.search(r"request_token=([A-Za-z0-9]+)", text)
    return m.group(1) if m else text.strip()


def expiry_ist(login_at: Optional[datetime]) -> Optional[datetime]:
    """06:00 IST after the login (the regulatory cut-off), or None if the login time is unknown."""
    if login_at is None:
        return None
    t = login_at.astimezone(_IST)
    six = t.replace(hour=6, minute=0, second=0, microsecond=0)
    return six if t < six else six + timedelta(days=1)


def parse_login_at(raw: str) -> Optional[datetime]:
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=_IST)
    except (ValueError, AttributeError):
        return None


def read_env(path: Path = ENV_FILE) -> dict:
    out = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            m = re.match(r"^\s*([A-Z0-9_]+)\s*=\s*(.*)$", line)
            if m:
                out[m.group(1)] = m.group(2).strip()
    return out


def api_secret(path: Path = SECRETS_FILE) -> str:
    secret = read_env(path).get("KITE_API_SECRET") or os.environ.get("KITE_API_SECRET", "")
    if not secret:
        raise TokenError(f"KITE_API_SECRET is missing — put it in {path.name} (VM only, never in .env or git).")
    return secret


def write_env(updates: dict, path: Path = ENV_FILE) -> None:
    """Set KEY=value lines in .env, keeping every other line and the file's line endings."""
    raw = open(path, encoding="utf-8", newline="").read() if path.exists() else ""     # newline="": keep CRLF
    nl = "\r\n" if "\r\n" in raw else "\n"
    lines = raw.replace("\r\n", "\n").split("\n") if raw else []
    for key, val in updates.items():
        pat = re.compile(rf"^\s*{re.escape(key)}\s*=")
        for i, line in enumerate(lines):
            if pat.match(line):
                lines[i] = f"{key}={val}"
                break
        else:
            lines.append(f"{key}={val}")
    path.write_text(nl.join(lines).rstrip(nl) + nl, encoding="utf-8", newline="")


def generate_session(api_key: str, request_token: str, secret: str, timeout: float = 20.0) -> dict:
    """Exchange a request_token for the day's session (access_token, user_id, login_time)."""
    try:
        resp = requests.post(f"{API}/session/token", headers={"X-Kite-Version": "3"}, timeout=timeout,
                             data={"api_key": api_key, "request_token": request_token,
                                   "checksum": checksum(api_key, request_token, secret)})
    except requests.RequestException as exc:
        raise TokenError(f"network error: {exc}") from exc
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if resp.status_code != 200 or body.get("status") != "success":
        raise TokenError(f"Kite refused the login: HTTP {resp.status_code} {body.get('error_type', '')}: "
                         f"{body.get('message', resp.text[:200])} (a request_token works once, for a few minutes)")
    return body["data"]


def verify(api_key: str, access_token: str, timeout: float = 15.0) -> Tuple[str, str]:
    """(user_id, user_name) if the token works now; TokenError otherwise."""
    if not api_key or not access_token:
        raise TokenError("KITE_API_KEY / KITE_ACCESS_TOKEN not set — log in: python scripts/kite_login.py")
    try:
        resp = requests.get(f"{API}/user/profile", timeout=timeout,
                            headers={"X-Kite-Version": "3", "Authorization": f"token {api_key}:{access_token}"})
    except requests.RequestException as exc:
        raise TokenError(f"network error: {exc}") from exc
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if resp.status_code == 200 and body.get("status") == "success":
        d = body.get("data") or {}
        return str(d.get("user_id", "")), str(d.get("user_name", ""))
    raise TokenError(f"the Kite access token is not valid now (HTTP {resp.status_code} {body.get('error_type', '')}: "
                     f"{body.get('message', '')}) — log in again: python scripts/kite_login.py")


def mask(token: str) -> str:
    return f"{token[:4]}…{token[-4:]}" if len(token) > 10 else "****"


def ensure_fresh_token(env_path: Path = ENV_FILE, force: bool = False) -> str:
    """For the pipeline: a one-line status if today's token works, TokenError if not.
    (Kite tokens can't be made without a person logging in; `force` is accepted for
    compatibility and does nothing.)"""
    env = read_env(env_path)
    user, name = verify(env.get("KITE_API_KEY", ""), env.get("KITE_ACCESS_TOKEN", ""))
    exp = expiry_ist(parse_login_at(env.get("KITE_LOGIN_AT", "")))
    until = f", valid to {exp:%d-%b %H:%M IST}" if exp else ""
    return f"Kite session OK for {user} ({name}){until}, token {mask(env.get('KITE_ACCESS_TOKEN', ''))}"
