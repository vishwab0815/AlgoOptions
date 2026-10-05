"""
algopilot/options/auto_token.py — the daily DhanHQ access token, without a browser.

Dhan's PIN + TOTP login (dhanhq.co/docs/v2/authentication):
    POST https://auth.dhan.co/app/generateAccessToken?dhanClientId=..&pin=..&totp=..
returns a 24-hour access token. The TOTP code is computed here (RFC 6238 —
the same 6 digits an authenticator app shows) from the secret Dhan gave when
TOTP was set up, so nothing has to be typed in each morning.

Secrets: DHAN_PIN and DHAN_TOTP_SECRET live in secrets.env, on the VM only —
git-ignored, never zipped, never logged. Together they can log into the
account, so this module never prints them, nor the request URL that carries
them, nor a full token.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import shutil
import struct
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from dotenv import dotenv_values

from .config import token_expiry_ist

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"
FUNDS_URL = "https://api.dhan.co/v2/fundlimit"
ENV_FILE = Path(".env")
SECRETS_FILE = Path("secrets.env")

TOTP_STEP = 30
# A code with fewer seconds than this left could expire on its way to Dhan:
# wait for the next one instead.
_MIN_SECS_LEFT = 5
# The token in .env is kept only if it stays valid past this time today
# (the engine stops at 15:31).
KEEP_IF_VALID_PAST = dtime(15, 45)


class TokenError(RuntimeError):
    """Generating or checking the token failed. Messages never contain secrets."""


# ── TOTP (RFC 6238, HMAC-SHA1, 30 s, 6 digits) ─────────────────────────────

def _b32(secret: str) -> bytes:
    s = secret.replace(" ", "").replace("-", "").upper()
    s += "=" * ((-len(s)) % 8)
    try:
        return base64.b32decode(s, casefold=True)
    except (ValueError, TypeError) as exc:
        raise TokenError("DHAN_TOTP_SECRET is not a valid TOTP key (expected the base32 text "
                         "Dhan shows under 'Set-up TOTP', letters A-Z and digits 2-7).") from exc


def totp_code(secret: str, at: Optional[float] = None, step: int = TOTP_STEP, digits: int = 6) -> str:
    key = _b32(secret)
    counter = int((time.time() if at is None else at) // step)
    mac = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = mac[-1] & 0x0F
    value = (struct.unpack(">I", mac[offset:offset + 4])[0] & 0x7FFFFFFF) % (10 ** digits)
    return str(value).zfill(digits)


def seconds_left(at: Optional[float] = None, step: int = TOTP_STEP) -> float:
    now = time.time() if at is None else at
    return step - (now % step)


# ── secrets / .env ──────────────────────────────────────────────────────────

def read_secrets(path: Path = SECRETS_FILE) -> Tuple[str, str]:
    """(pin, totp_secret) from secrets.env (or the environment). Validated."""
    values = dotenv_values(path) if path.exists() else {}
    pin = str(values.get("DHAN_PIN") or os.getenv("DHAN_PIN") or "").strip()
    secret = str(values.get("DHAN_TOTP_SECRET") or os.getenv("DHAN_TOTP_SECRET") or "").strip()
    if not pin or not secret:
        raise TokenError(f"DHAN_PIN and DHAN_TOTP_SECRET must be set in {path} (copy secrets.env.example).")
    if not pin.isdigit() or not 4 <= len(pin) <= 6:
        raise TokenError("DHAN_PIN must be your Dhan PIN: digits only.")
    totp_code(secret)                               # raises TokenError if the key is malformed
    return pin, secret


def mask(token: str) -> str:
    return f"{token[:6]}…{token[-4:]}" if len(token) > 12 else "…"


def replace_env_token(env_path: Path, token: str) -> None:
    """Write DHAN_ACCESS_TOKEN into .env, touching no other line. Atomic: the
    file is either the old one or the new one, never half-written; the old
    one is kept as .env.bak."""
    with open(env_path, encoding="utf-8", newline="") as f:      # newline="": keep \r\n exactly as it is
        text = f.read()
    lines = text.splitlines(keepends=True)
    newline = "\r\n" if "\r\n" in text else "\n"
    out, found = [], False
    for line in lines:
        if line.lstrip().startswith("DHAN_ACCESS_TOKEN="):
            out.append(f"DHAN_ACCESS_TOKEN={token}{newline}")
            found = True
        else:
            out.append(line)
    if not found:
        if out and not out[-1].endswith(("\n", "\r")):
            out.append(newline)
        out.append(f"DHAN_ACCESS_TOKEN={token}{newline}")
    tmp = env_path.with_name(env_path.name + ".tmp")
    tmp.write_text("".join(out), encoding="utf-8", newline="")
    shutil.copy2(env_path, env_path.with_name(env_path.name + ".bak"))
    os.replace(tmp, env_path)


# ── Dhan calls ──────────────────────────────────────────────────────────────

def request_token(client_id: str, pin: str, secret: str, session: requests.Session,
                  sleep=time.sleep, clock=time.time) -> dict:
    """One token request. Returns Dhan's JSON (accessToken, expiryTime, ...)."""
    if seconds_left(clock()) < _MIN_SECS_LEFT:
        sleep(seconds_left(clock()) + 0.5)
    code = totp_code(secret, at=clock())
    try:
        resp = session.post(TOKEN_URL, params={"dhanClientId": client_id, "pin": pin, "totp": code}, timeout=20)
    except requests.RequestException as exc:
        # str(exc) would include the URL — and with it the PIN and TOTP.
        raise TokenError(f"network error reaching Dhan ({type(exc).__name__})") from None
    try:
        body = resp.json()
    except ValueError:
        body = {}
    token = body.get("accessToken") if isinstance(body, dict) else None
    if resp.status_code != 200 or not token:
        reason = ""
        if isinstance(body, dict):
            reason = str(body.get("errorMessage") or body.get("message") or body.get("errorCode") or "")
        raise TokenError(f"Dhan refused the token request (HTTP {resp.status_code}{': ' + reason if reason else ''}). "
                         "Check DHAN_PIN, DHAN_TOTP_SECRET and that TOTP is enabled.")
    got = str(body.get("dhanClientId") or "")
    if got and got != str(client_id):
        raise TokenError(f"Dhan returned a token for client {got}, but DHAN_CLIENT_ID is {client_id}.")
    return body


def verify_token(client_id: str, token: str, session: requests.Session) -> float:
    """A real read with the new token (funds). Returns available funds."""
    try:
        resp = session.get(FUNDS_URL, headers={"access-token": token, "client-id": client_id,
                                               "Accept": "application/json"}, timeout=15)
    except requests.RequestException as exc:
        raise TokenError(f"network error checking the new token ({type(exc).__name__})") from None
    if resp.status_code != 200:
        raise TokenError(f"the new token was not accepted by Dhan (HTTP {resp.status_code}).")
    body = resp.json() if resp.content else {}
    for key in ("availabelBalance", "availableBalance"):
        if key in body:
            return float(body[key])
    return 0.0


def ensure_fresh_token(env_path: Path = ENV_FILE, secrets_path: Path = SECRETS_FILE, force: bool = False,
                       attempts: int = 3, session: Optional[requests.Session] = None,
                       now: Optional[datetime] = None, sleep=time.sleep, clock=time.time) -> str:
    """Make sure .env holds a token that lasts through today's session.
    Keeps the current one if it does (unless force); otherwise generates,
    checks it against Dhan, and only then writes it. Returns a summary."""
    values = dotenv_values(env_path) if env_path.exists() else {}
    client_id = str(values.get("DHAN_CLIENT_ID") or "").strip()
    if not client_id:
        raise TokenError(f"DHAN_CLIENT_ID is missing in {env_path}.")
    now = now or datetime.now(_IST)
    current = str(values.get("DHAN_ACCESS_TOKEN") or "").strip()
    expires = token_expiry_ist(current) if current else None
    keep_until = now.replace(hour=KEEP_IF_VALID_PAST.hour, minute=KEEP_IF_VALID_PAST.minute, second=0, microsecond=0)
    if not force and expires is not None and expires > keep_until:
        return f"current token is valid until {expires:%d-%b %H:%M IST} — kept"

    pin, secret = read_secrets(secrets_path)
    session = session or requests.Session()
    last: Optional[TokenError] = None
    for attempt in range(1, attempts + 1):
        try:
            body = request_token(client_id, pin, secret, session, sleep=sleep, clock=clock)
            token = str(body["accessToken"])
            funds = verify_token(client_id, token, session)
            replace_env_token(env_path, token)
            new_exp = token_expiry_ist(token)
            return (f"new token {mask(token)} for {body.get('dhanClientName') or client_id}, valid until "
                    f"{new_exp:%d-%b %H:%M IST}" if new_exp else f"new token {mask(token)}") + \
                   f" — checked with Dhan (funds Rs {funds:,.2f}) and saved to {env_path}"
        except TokenError as exc:
            last = exc
            logger.warning("Token attempt %d of %d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                sleep(seconds_left(clock()) + 1.0)      # a fresh TOTP code for the next try
    raise TokenError(f"no token after {attempts} tries — last error: {last}")
