"""
utils/secrets.py — SecretStr wrapper.

Wraps sensitive strings (API tokens, passwords) so they NEVER appear in
plain text in log output, repr(), str(), or accidental print() calls.

Usage:
    token = SecretStr("my-secret-token")
    print(token)              # → "****"
    print(token.get_secret()) # → "my-secret-token"  (only when explicitly needed)
"""
from __future__ import annotations


class SecretStr:
    """
    Immutable wrapper for a sensitive string value.

    The value is stored privately. Any attempt to convert it to a string
    (for logging, printing, or repr) returns '****' instead of the real value.
    The real value is only accessible via .get_secret().
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        object.__setattr__(self, "_value", value)

    # ── Prevent accidental exposure ───────────────────────────────────────────

    def __repr__(self) -> str:
        return "SecretStr(****)"

    def __str__(self) -> str:
        return "****"

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("SecretStr is immutable.")

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SecretStr):
            return self._value == other._value
        return False

    def __hash__(self) -> int:
        return hash(self._value)

    # ── Controlled access ─────────────────────────────────────────────────────

    def get_secret(self) -> str:
        """Return the real secret value. Only call this when you need to send it over the wire."""
        return self._value
