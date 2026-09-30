"""
scripts/latency_check.py — how fast can THIS machine talk to Dhan? (read-only)

Places no orders. Measures, from wherever it's run (run it on the VM):
  - DNS lookup and one network round trip to api.dhan.co
  - a request on a NEW connection vs an already-OPEN one (the engine keeps
    the connection open, so orders pay the "open" figure)
  - connecting and logging in to Dhan's order-update stream (fills pushed)
  - whether Dhan will accept ORDERS from this machine's IP (static-IP check)

    python scripts/latency_check.py
"""
import asyncio
import socket
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from algopilot.utils.network import ensure_tls_trust_store, force_ipv4

force_ipv4()
ensure_tls_trust_store()

import requests

from algopilot.options.broker import DhanBroker, OrderUpdateStream
from algopilot.options.config import load_options_config


def ms(t0: float) -> float:
    return (time.perf_counter() - t0) * 1000.0


def main() -> int:
    cfg = load_options_config()
    headers = {"access-token": cfg.access_token.get_secret(), "client-id": cfg.client_id, "Accept": "application/json"}
    url = "https://api.dhan.co/v2/fundlimit"

    t = time.perf_counter(); socket.getaddrinfo("api.dhan.co", 443); dns = ms(t)
    t = time.perf_counter(); socket.create_connection(("api.dhan.co", 443), timeout=5).close(); rtt = ms(t)
    cold = []
    for _ in range(3):
        s = requests.Session(); t = time.perf_counter(); s.get(url, headers=headers, timeout=10); cold.append(ms(t)); s.close()
    s = requests.Session(); s.get(url, headers=headers, timeout=10); warm = []
    for _ in range(10):
        time.sleep(0.3); t = time.perf_counter(); s.get(url, headers=headers, timeout=10); warm.append(ms(t))
    s.close()

    async def stream_login() -> float:
        st = OrderUpdateStream(cfg.client_id, cfg.access_token)
        task = asyncio.create_task(st.run()); t0 = time.perf_counter()
        while not st.connected and time.perf_counter() - t0 < 10:
            await asyncio.sleep(0.01)
        took = ms(t0) if st.connected else -1.0
        await st.stop(); task.cancel()
        return took
    stream = asyncio.run(stream_login())

    b = DhanBroker(cfg.client_id, cfg.access_token)
    code, ip = b._call("GET", "/ip/getIP"); b.close()
    ip = ip if code == 200 and isinstance(ip, dict) else {}

    w = statistics.median(warm)
    print("\nDhan latency from this machine")
    print(f"  DNS lookup                    {dns:7.1f} ms")
    print(f"  one network round trip        {rtt:7.1f} ms")
    print(f"  request, NEW connection       {statistics.median(cold):7.1f} ms")
    print(f"  request, OPEN connection      {w:7.1f} ms   <- what an order costs (engine keeps it open)")
    print(f"  order-update stream login     {stream:7.1f} ms" if stream >= 0 else "  order-update stream           FAILED to connect")
    print(f"  orders allowed from this IP   {ip.get('ordersAllowed')}  (this IP {ip.get('detectedIP')}, registered {ip.get('primaryIP')})")
    if rtt > 20:
        print("\n  Round trip > 20 ms: a VM in a Mumbai-region data centre would cut every order's time.")
    if ip and not ip.get("ordersAllowed"):
        print("\n  *** Dhan will REFUSE orders from this machine — run live only where ordersAllowed is True. ***")
    return 0


if __name__ == "__main__":
    sys.exit(main())
