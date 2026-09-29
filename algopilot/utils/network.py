"""
utils/network.py — Force all outbound HTTP(S) traffic in this process onto IPv4.

Root cause this exists to fix: on this machine, DhanHQ order placements were
intermittently rejected with "DH-905: Invalid IP" even though the whitelisted
IPv6 address matched a `get_ip()` check taken moments before or after the
failed order. Confirmed via a direct SDK call surfacing the specific reason
("Invalid IP") right after a `get_ip()` check reported a MATCH.

The explanation is Windows' IPv6 Privacy Extensions (RFC 4941): the OS can
hand out a different temporary IPv6 source address per outbound connection
from the same delegated prefix, so "the IP matches right now" does not mean
"the IP matched for the connection that just placed the order" — it's a
per-connection coin-flip, not a one-time whitelist problem. The already-
whitelisted IPv4 secondary address does not have this ambiguity (a home/
office NAT presents one single stable public IPv4 address), so forcing IPv4
resolves it permanently instead of chasing a moving IPv6 target.

Usage — call once, as early as possible (main.py does this at startup,
before anything else runs):

    from algopilot.utils.network import force_ipv4
    force_ipv4()

BUG FIXED (architecture review): this used to patch ONLY
`urllib3.util.connection.allowed_gai_family` — which fixes `requests`-based
HTTP calls (OrderExecutor's session, the DhanHQ SDK's REST calls), but does
NOTHING for either of this project's two WebSocket connections
(feed/dhan_stream.py's market feed, feed/order_update_stream.py's order
updates). The `websockets` library resolves hostnames through asyncio's own
default resolver, which never goes through urllib3 at all — so the exact
same IPv6 privacy-extension rotation this function exists to fix could still
silently affect the two connections that have shown the most instability in
production (FEED_STALE, forced hard-restarts).

Fixed by patching `socket.getaddrinfo` itself instead: every networking
library in this process — urllib3, requests, asyncio's default resolver, and
therefore `websockets` — ultimately calls this one function to resolve a
hostname before connecting. Patching it here is the single, general fix that
covers all of them, rather than needing a second, library-specific patch for
every new networking library this project ever adds. The urllib3-specific
patch is kept alongside it, redundant but harmless, in case some code path
ever calls urllib3 directly without going through socket.getaddrinfo.

Safe to call multiple times — subsequent calls are no-ops.
"""
from __future__ import annotations

import logging
import os
import socket
from typing import Any

logger = logging.getLogger(__name__)

_already_forced = False
_tls_configured = False


def ensure_tls_trust_store() -> None:
    """Point this process's TLS verification at certifi's CA bundle.

    Root cause this exists to fix: on a fresh Windows VM the market feed died
    instantly with
        [SSL: CERTIFICATE_VERIFY_FAILED] unable to get local issuer certificate
    and never reconnected — so the engine ran a full session on REST polling
    alone, receiving zero ticks and opening zero trades, while looking
    healthy in the log. A Python install that has no usable system trust
    store cannot verify api.dhan.co, and `ssl.create_default_context()`
    (which `websockets` and `requests` both use) silently has nothing to
    verify against.

    `ssl` reads SSL_CERT_FILE when building its default context, so setting
    it to certifi's bundle fixes every TLS client in the process at once.
    setdefault, so an operator who has deliberately pointed these at a
    corporate CA bundle keeps their own setting.
    """
    global _tls_configured
    if _tls_configured:
        return
    _tls_configured = True

    if os.environ.get("SSL_CERT_FILE") and os.path.isfile(os.environ["SSL_CERT_FILE"]):
        logger.debug("TLS trust store: using preset SSL_CERT_FILE=%s", os.environ["SSL_CERT_FILE"])
        return

    try:
        import certifi
    except ImportError:
        logger.warning(
            "certifi is not installed — TLS verification falls back to the system trust "
            "store, which on some Windows/VM Python installs is empty and fails every "
            "HTTPS and WebSocket connection with CERTIFICATE_VERIFY_FAILED. "
            "Fix with: pip install certifi"
        )
        return

    bundle = certifi.where()
    if not os.path.isfile(bundle):
        logger.warning("certifi reported a CA bundle at %s but it does not exist.", bundle)
        return

    os.environ.setdefault("SSL_CERT_FILE", bundle)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", bundle)
    logger.info("TLS trust store: verifying against certifi's CA bundle.")


def force_ipv4() -> None:
    """Force EVERY networking library in this process — HTTP and WebSocket
    alike — to resolve only IPv4 addresses."""
    global _already_forced
    if _already_forced:
        return

    # ── General fix: covers urllib3, requests, asyncio, and websockets ──────
    _original_getaddrinfo = socket.getaddrinfo

    def _ipv4_only_getaddrinfo(
        host: Any, port: Any, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0,
    ):
        return _original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = _ipv4_only_getaddrinfo

    # ── Redundant belt-and-suspenders: the original urllib3-specific patch ──
    try:
        import urllib3.util.connection as urllib3_conn

        def _allowed_gai_family() -> int:
            return socket.AF_INET

        urllib3_conn.allowed_gai_family = _allowed_gai_family
    except ImportError:
        pass  # the socket.getaddrinfo patch above already covers this case

    _already_forced = True
    logger.info(
        "Outbound network forced to IPv4 for HTTP AND WebSocket connections "
        "(avoids DhanHQ IPv6 whitelist rotation on any connection type)."
    )
