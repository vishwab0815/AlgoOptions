"""
scripts/latency_check.py — how fast can THIS machine talk to Zerodha Kite? (read-only)

Places no orders. Measures, from wherever it's run (run it on the VM):
  - DNS lookup of api.kite.trade
  - a request on a NEW connection vs an already-OPEN one (the engine keeps the
    connection open, so orders pay the "open" figure) — GET /user/profile
  - opening the Kite WebSocket (prices and order updates)

    python scripts/latency_check.py

Orders are accepted only from the static IP registered on developers.kite.trade
(Profile -> IP Whitelist); Kite has no API to check that, so the first live order
is the real test. Reads (this script) work from any IP.
"""
import asyncio
import socket
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from algopilot.utils.network import ensure_tls_trust_store, force_ipv4  # noqa: E402

force_ipv4()
ensure_tls_trust_store()

import requests  # noqa: E402

from algopilot.options.config import load_options_config  # noqa: E402

HOST = "api.kite.trade"


def ms(t0: float) -> float:
    return (time.perf_counter() - t0) * 1000.0


def main() -> int:
    cfg = load_options_config()
    headers = {"X-Kite-Version": "3", "Authorization": f"token {cfg.api_key}:{cfg.access_token.get_secret()}"}
    t0 = time.perf_counter()
    ip = socket.getaddrinfo(HOST, 443, socket.AF_INET)[0][4][0]
    print(f"DNS {HOST} -> {ip}: {ms(t0):.0f} ms")

    new = []
    for _ in range(3):
        t0 = time.perf_counter()
        r = requests.get(f"https://{HOST}/user/profile", headers=headers, timeout=15)
        new.append(ms(t0))
    print(f"GET /user/profile on a NEW connection: {statistics.median(new):.0f} ms (HTTP {r.status_code})")
    if r.status_code != 200:
        print(f"  Kite says: {r.text[:200]} — log in: python scripts/kite_login.py")
        return 2

    s = requests.Session()
    s.headers.update(headers)
    s.get(f"https://{HOST}/user/profile", timeout=15)
    kept = []
    for _ in range(5):
        t0 = time.perf_counter()
        s.get(f"https://{HOST}/user/profile", timeout=15)
        kept.append(ms(t0))
        time.sleep(0.15)
    print(f"GET /user/profile on an OPEN connection: {statistics.median(kept):.0f} ms (median of 5)")

    async def ws_open() -> float:
        import websockets
        url = f"wss://ws.kite.trade?api_key={cfg.api_key}&access_token={cfg.access_token.get_secret()}"
        t0 = time.perf_counter()
        async with websockets.connect(url, additional_headers={"X-Kite-Version": "3"}, open_timeout=10):
            return ms(t0)

    try:
        print(f"Kite WebSocket opened in {asyncio.run(ws_open()):.0f} ms")
    except Exception as exc:
        print(f"Kite WebSocket failed: {type(exc).__name__}: {exc}")
    print("Static IP: orders work only from the IP registered on developers.kite.trade (IPv4, the address "
          "requests leave from). The first live order confirms it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
