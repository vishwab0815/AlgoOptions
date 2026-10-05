"""
scripts/auto_token.py — today's DhanHQ access token from PIN + TOTP (no browser).

    python scripts/auto_token.py            new token only if the one in .env won't last today's session
    python scripts/auto_token.py --force    new token now
    python scripts/auto_token.py --check    check secrets.env and .env; shows the current TOTP code
                                            (must match your authenticator app). Calls nothing at Dhan.

Needs secrets.env (copy secrets.env.example) with DHAN_PIN and DHAN_TOTP_SECRET,
on the VM only. Exit code 0 = OK, 2 = failed.
"""
import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from algopilot.utils.network import ensure_tls_trust_store, force_ipv4

force_ipv4()
ensure_tls_trust_store()

from dotenv import dotenv_values

from algopilot.options.auto_token import (ENV_FILE, SECRETS_FILE, TokenError, ensure_fresh_token, seconds_left,
                                          token_expiry_ist, totp_code)

log = logging.getLogger("auto_token")


def check() -> int:
    if not SECRETS_FILE.exists():
        log.error("%s not found — copy secrets.env.example to %s and fill it in.", SECRETS_FILE, SECRETS_FILE)
        return 2
    values = dotenv_values(SECRETS_FILE)
    pin = str(values.get("DHAN_PIN") or "").strip()
    secret = str(values.get("DHAN_TOTP_SECRET") or "").strip()
    good = True

    if not pin:
        log.error("DHAN_PIN        : MISSING — add your Dhan PIN to %s", SECRETS_FILE)
        good = False
    elif not pin.isdigit() or not 4 <= len(pin) <= 6:
        log.error("DHAN_PIN        : invalid — digits only")
        good = False
    else:
        log.info("DHAN_PIN        : set (%d digits)", len(pin))

    if not secret:
        log.error("DHAN_TOTP_SECRET: MISSING")
        good = False
    else:
        try:
            log.info("DHAN_TOTP_SECRET: valid — current code %s (next in %.0f s); it must match your authenticator app",
                     totp_code(secret), seconds_left())
        except TokenError as exc:
            log.error("DHAN_TOTP_SECRET: %s", exc)
            good = False

    token = str(dotenv_values(ENV_FILE).get("DHAN_ACCESS_TOKEN") or "")
    exp = token_expiry_ist(token) if token else None
    log.info("token in %s    : %s", ENV_FILE, f"valid until {exp:%d-%b %H:%M IST}" if exp else "none / unreadable")
    return 0 if good else 2


def main() -> int:
    ap = argparse.ArgumentParser(description="Today's DhanHQ access token from PIN + TOTP.")
    ap.add_argument("--force", action="store_true", help="generate a new token even if the current one is valid")
    ap.add_argument("--check", action="store_true", help="check the setup only; no call to Dhan")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    if args.check:
        return check()
    try:
        log.info("%s", ensure_fresh_token(force=args.force))
        return 0
    except TokenError as exc:
        log.error("Token NOT generated: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
