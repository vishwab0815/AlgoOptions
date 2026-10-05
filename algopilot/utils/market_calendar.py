"""
algopilot/utils/market_calendar.py — is NSE (equity derivatives) open today?

Weekends and NSE's published trading holidays. Update NSE_HOLIDAYS once a
year when NSE publishes the next list (usually in December). A day missing
from the list is still caught live: the daily pipeline checks at 09:17 that
NIFTY actually traded, and stops the engine if it didn't.

2026 list as published for the Equity / Equity Derivatives segments
(cross-checked against several broker calendars in October 2026).
"""
from __future__ import annotations

from datetime import date
from typing import Dict, Tuple

NSE_HOLIDAYS: Dict[date, str] = {
    date(2026, 1, 15): "Municipal Corporation General Elections (Maharashtra)",
    date(2026, 1, 26): "Republic Day",
    date(2026, 3, 3): "Holi",
    date(2026, 3, 26): "Shri Ram Navami",
    date(2026, 3, 31): "Shri Mahavir Jayanti",
    date(2026, 4, 3): "Good Friday",
    date(2026, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2026, 5, 1): "Maharashtra Day",
    date(2026, 5, 28): "Bakri Id",
    date(2026, 6, 26): "Muharram",
    date(2026, 9, 14): "Ganesh Chaturthi",
    date(2026, 10, 2): "Mahatma Gandhi Jayanti",
    date(2026, 10, 20): "Dussehra",
    date(2026, 11, 10): "Diwali Balipratipada",
    date(2026, 11, 24): "Prakash Gurpurb Sri Guru Nanak Dev",
    date(2026, 12, 25): "Christmas",
}

# Special sessions outside normal days/hours. The engine does NOT trade these
# (different timings); listed so the log says why nothing happened.
SPECIAL_SESSIONS: Dict[date, str] = {
    date(2026, 11, 8): "Diwali Muhurat trading (Sunday, special evening session — not traded)",
}

LISTED_YEARS = {d.year for d in NSE_HOLIDAYS}


def trading_day_status(d: date) -> Tuple[bool, str]:
    """(is a normal trading day, why)."""
    if d in SPECIAL_SESSIONS:
        return False, SPECIAL_SESSIONS[d]
    if d.weekday() >= 5:
        return False, f"weekend ({d:%A})"
    if d in NSE_HOLIDAYS:
        return False, f"NSE holiday — {NSE_HOLIDAYS[d]}"
    if d.year not in LISTED_YEARS:
        return True, (f"trading day assumed — NO NSE holiday list for {d.year} yet "
                      "(add it to algopilot/utils/market_calendar.py; the 09:17 live check still guards)")
    return True, "trading day"
