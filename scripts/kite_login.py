"""
scripts/kite_login.py — today's Zerodha Kite Connect access token (one login a day).

    python scripts/kite_login.py                 open the Kite login page and catch the redirect here
    python scripts/kite_login.py <request_token> exchange a request_token (or paste the whole redirect URL)
    python scripts/kite_login.py --check         is today's token working?

Zerodha requires a person to log in every trading day (user id, password, TOTP,
on Zerodha's own page). After the login Kite redirects the browser to your app's
redirect URL with ?request_token=... . Set that redirect URL on developers.kite.trade
to   http://127.0.0.1:5000/   (or another port in KITE_REDIRECT_PORT) and this
script catches it by itself; otherwise copy the address-bar URL and pass it in.

The request_token is exchanged at POST /session/token with
checksum = sha256(api_key + request_token + api_secret), and the access token is
written to .env as KITE_ACCESS_TOKEN (with KITE_USER_ID and KITE_LOGIN_AT). It
works until 06:00 IST tomorrow. KITE_API_SECRET is read from secrets.env and is
never printed or written anywhere else.
"""
from __future__ import annotations

import sys
import threading
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from algopilot.utils.network import ensure_tls_trust_store, force_ipv4  # noqa: E402

force_ipv4()
ensure_tls_trust_store()

from algopilot.options.kite_auth import (ENV_FILE, TokenError, api_secret, ensure_fresh_token,  # noqa: E402
                                         expiry_ist, generate_session, login_url, mask, read_env,
                                         request_token_from, verify, write_env)

IST = ZoneInfo("Asia/Kolkata")
WAIT_SECS = 300


def _catch_redirect(port: int) -> str:
    """Serve http://127.0.0.1:<port>/ until Kite's redirect brings a request_token."""
    got = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):                                           # noqa: N802
            q = parse_qs(urlparse(self.path).query)
            tok = (q.get("request_token") or [""])[0]
            ok = bool(tok) and (q.get("status") or ["success"])[0] == "success"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            msg = ("Logged in. You can close this tab and go back to the terminal." if ok
                   else "The login did not return a request_token. Try again.")
            self.wfile.write(f"<html><body style='font-family:sans-serif'><h3>{msg}</h3></body></html>".encode())
            if ok:
                got["token"] = tok

        def log_message(self, *args):                               # quiet
            pass

    srv = HTTPServer(("127.0.0.1", port), Handler)
    srv.timeout = 1.0
    deadline = datetime.now().timestamp() + WAIT_SECS
    while "token" not in got and datetime.now().timestamp() < deadline:
        srv.handle_request()
    srv.server_close()
    if "token" not in got:
        raise TokenError(f"no login redirect arrived on http://127.0.0.1:{port}/ within {WAIT_SECS // 60} min. "
                         "Check the app's redirect URL on developers.kite.trade, or pass the redirect URL "
                         "to this script.")
    return got["token"]


def main() -> int:
    args = [a for a in sys.argv[1:] if a]
    env = read_env()
    api_key = env.get("KITE_API_KEY", "")
    if "--check" in args:
        try:
            print(ensure_fresh_token())
            return 0
        except TokenError as exc:
            print(f"NOT OK: {exc}")
            return 2
    if not api_key:
        print("KITE_API_KEY is missing in .env (your Kite Connect app's API key from developers.kite.trade).")
        return 2
    try:
        secret = api_secret()
        now = datetime.now(IST)
        if now.strftime("%H:%M") >= "09:15" and now.strftime("%H:%M") <= "15:30":
            print("Note: logging in during market hours. A running engine keeps the token it started with; "
                  "if Kite issues a different one, restart the engine after this.")
        if args:
            request_token = request_token_from(args[0])
        else:
            port = int(env.get("KITE_REDIRECT_PORT", "5000") or 5000)
            url = login_url(api_key)
            print(f"Opening the Kite login page (if it doesn't open, paste this into a browser):\n  {url}\n"
                  f"Waiting for the redirect on http://127.0.0.1:{port}/ ...")
            threading.Timer(0.5, lambda: webbrowser.open(url)).start()
            request_token = _catch_redirect(port)
        session = generate_session(api_key, request_token, secret)
        token = str(session.get("access_token") or "")
        if not token:
            raise TokenError("Kite answered without an access_token.")
        login_at = str(session.get("login_time") or datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
        user, name = verify(api_key, token)
        write_env({"KITE_ACCESS_TOKEN": token, "KITE_USER_ID": user, "KITE_LOGIN_AT": login_at}, ENV_FILE)
    except TokenError as exc:
        print(f"FAILED: {exc}")
        return 2
    from algopilot.options.kite_auth import parse_login_at
    exp = expiry_ist(parse_login_at(login_at))
    print(f"OK: logged in as {user} ({name}); token {mask(token)} saved to {ENV_FILE.name}"
          + (f", valid until {exp:%d-%b %H:%M IST}." if exp else "."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
