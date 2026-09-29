"""
algopilot/options/charges.py — what a round trip actually costs.

Paper fills used to be frictionless: P&L was (entry - exit) * qty and nothing
else. On NIFTY options that is badly optimistic, because the dominant cost is
FLAT per order (~Rs 20 brokerage + 18% GST on it), not proportional. At 1 lot
of 65 the premium turnover is small, so those flat legs swamp it — a round
trip costs roughly Rs 45-55 regardless of how big the move was. A "+Rs 40"
paper win is a real-money loss, and a "-Rs 21" paper scratch is really about
-Rs 70. Any judgement about whether the strategy works has to be made on
NET numbers.

Every rate below is CONFIGURABLE (see OptionsConfig) and every default is an
ESTIMATE of publicly published Indian F&O rates. They change with budgets and
exchange circulars. Check them against a real Dhan contract note before
trusting the net figures, and override in .env if they differ:

    OPTIONS_BROKERAGE_PER_ORDER   flat fee per executed order
    OPTIONS_STT_PCT               securities transaction tax, SELL side only
    OPTIONS_EXCHANGE_TXN_PCT      NSE transaction charge, both sides
    OPTIONS_SEBI_PCT              SEBI turnover fee, both sides
    OPTIONS_STAMP_DUTY_PCT        stamp duty, BUY side only
    OPTIONS_GST_PCT               GST, on (brokerage + txn + SEBI)

All percentage rates apply to PREMIUM turnover (price * qty), which is the
correct base for options — not notional (strike * qty).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChargeRates:
    """Rates as PERCENTAGES of premium turnover, except brokerage (flat Rs)."""
    brokerage_per_order: float = 20.0
    stt_pct: float = 0.10           # sell side only
    exchange_txn_pct: float = 0.0495
    sebi_pct: float = 0.0001
    stamp_duty_pct: float = 0.003   # buy side only
    gst_pct: float = 18.0           # on brokerage + txn + sebi


@dataclass(frozen=True)
class OrderCharges:
    """The cost of ONE executed order (one leg of the round trip)."""
    brokerage: float
    stt: float
    exchange_txn: float
    sebi: float
    stamp_duty: float
    gst: float

    @property
    def total(self) -> float:
        return round(
            self.brokerage + self.stt + self.exchange_txn
            + self.sebi + self.stamp_duty + self.gst, 2
        )

    def breakdown(self) -> str:
        return (
            f"brokerage={self.brokerage:.2f} stt={self.stt:.2f} txn={self.exchange_txn:.2f} "
            f"sebi={self.sebi:.2f} stamp={self.stamp_duty:.2f} gst={self.gst:.2f}"
        )


def order_charges(price: float, qty: int, is_buy: bool, rates: ChargeRates) -> OrderCharges:
    """Charges for a single order. `is_buy` picks the side-specific taxes:
    STT falls on the SELL leg of an options trade, stamp duty on the BUY leg."""
    turnover = max(0.0, price * qty)

    brokerage = rates.brokerage_per_order
    stt = 0.0 if is_buy else turnover * rates.stt_pct / 100.0
    exchange_txn = turnover * rates.exchange_txn_pct / 100.0
    sebi = turnover * rates.sebi_pct / 100.0
    stamp_duty = turnover * rates.stamp_duty_pct / 100.0 if is_buy else 0.0
    gst = (brokerage + exchange_txn + sebi) * rates.gst_pct / 100.0

    return OrderCharges(
        brokerage=round(brokerage, 2), stt=round(stt, 2),
        exchange_txn=round(exchange_txn, 2), sebi=round(sebi, 2),
        stamp_duty=round(stamp_duty, 2), gst=round(gst, 2),
    )


def round_trip_charges(
    entry_price: float, exit_price: float, qty: int, rates: ChargeRates,
) -> tuple:
    """(entry_charges, exit_charges, total) for a SHORT round trip —
    sell to open, buy to close. Returns both legs so the ledger can record
    where the money went, not just how much."""
    entry = order_charges(entry_price, qty, is_buy=False, rates=rates)   # sell to open
    exit_ = order_charges(exit_price, qty, is_buy=True, rates=rates)     # buy to close
    return entry, exit_, round(entry.total + exit_.total, 2)
