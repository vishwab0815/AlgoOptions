"""
scripts/export_candle_log.py — read today's (or any day's) candle_log +
trades out of the live ledger and turn them into something you can actually
read: a clean aligned table on screen, a CSV you can open in Excel, and a
funnel summary that answers "is my algorithm working" in numbers instead of
scrollback.

Safe to run WHILE the engine is live: the ledger is opened in WAL mode, which
lets a reader see a consistent snapshot without blocking or corrupting the
writer — unlike opening a CSV mid-session in Excel, which can lock the file
out from under the engine's next write.

Usage:
    python scripts/export_candle_log.py [YYYY-MM-DD]

With no argument, uses today (IST). Writes to data/exports/candle_log_<day>.csv
and data/exports/trades_<day>.csv.
"""
import csv
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from algopilot.options.config import load_options_config
from algopilot.options.ledger import OptionsLedger

_IST = ZoneInfo("Asia/Kolkata")


def _hhmm(iso: str) -> str:
    return datetime.fromisoformat(iso).astimezone(_IST).strftime("%H:%M")


def _print_table(rows: list, cols: list, widths: dict) -> None:
    def fmt_row(vals):
        return " | ".join(str(v).ljust(widths[c]) for c, v in zip(cols, vals))
    header = [c.upper() for c in cols]
    sep = "-+-".join("-" * widths[c] for c in cols)
    print(fmt_row(header))
    print(sep)
    for r in rows:
        print(fmt_row(r))


def main() -> int:
    day = sys.argv[1] if len(sys.argv) > 1 else datetime.now(_IST).strftime("%Y-%m-%d")

    cfg = load_options_config()
    # Read-only: safe while the engine is running — can't lock or change the live file.
    ledger = OptionsLedger(cfg.db_path, readonly=True)
    candles = ledger.get_candle_log(day)
    trades = [t for t in ledger.get_all_trades(limit=2000) if t["trading_day"] == day]
    ledger.close()

    if not candles:
        print(f"No candle_log rows for {day}.")
        return 0

    # ── Candle-by-candle table ──────────────────────────────────────────
    cols = ["time", "leg", "src", "o", "h", "l", "c", "color", "stage", "target", "signal"]
    widths = {"time": 11, "leg": 3, "src": 8, "o": 7, "h": 7, "l": 7, "c": 7,
              "color": 5, "stage": 22, "target": 7, "signal": 6}
    rows = []
    for r in candles:
        stage = f"{r['stage_before']}->{r['stage_after']}"
        rows.append([
            f"{_hhmm(r['candle_start'])}-{_hhmm(r['candle_end'])}", r["leg"], r["source"],
            f"{r['ha_open']:.2f}", f"{r['ha_high']:.2f}", f"{r['ha_low']:.2f}", f"{r['ha_close']:.2f}",
            r["color"], stage, f"{r['target_level']:.2f}" if r["target_level"] is not None else "-",
            r["signal"],
        ])

    print(f"\n{'=' * 100}\nCANDLE LOG — {day} ({len(candles)} candles, both legs)\n{'=' * 100}")
    _print_table(rows, cols, widths)

    # ── Funnel summary: does the pattern actually convert? ──────────────
    print(f"\n{'-' * 100}\nPATTERN FUNNEL\n{'-' * 100}")
    for leg in ("CE", "PE"):
        leg_rows = [r for r in candles if r["leg"] == leg]
        if not leg_rows:
            continue
        greens = sum(1 for r in leg_rows if r["color"] == "GREEN")
        reds = len(leg_rows) - greens
        armed = sum(1 for r in leg_rows if r["stage_after"] == "GREEN_SEEN")
        set_ = sum(1 for r in leg_rows if r["stage_after"] == "LEVEL_SET")
        triggered = sum(1 for r in leg_rows if r["signal"] == "SELL")
        live_n = sum(1 for r in leg_rows if r["source"] == "live")
        backfill_n = len(leg_rows) - live_n
        conv = (triggered / set_ * 100.0) if set_ else 0.0
        print(
            f"  {leg}: {len(leg_rows)} candles ({live_n} live, {backfill_n} backfill) | "
            f"{greens} GREEN / {reds} RED | 1/3 armed {armed}x -> 2/3 set {set_}x -> "
            f"3/3 triggered {triggered}x  (set->trigger conversion {conv:.0f}%)"
        )

    # ── Trades for the day ───────────────────────────────────────────────
    if trades:
        trades.sort(key=lambda t: t["entry_time"])
        def _gross(t):
            g = t.get("gross_pnl")
            return g if g is not None else t["pnl"]

        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        total_gross = sum(_gross(t) for t in trades)
        total_chg = sum(t.get("charges") or 0.0 for t in trades)
        total_pnl = sum(t["pnl"] for t in trades)
        gross_wins = sum(1 for t in trades if _gross(t) > 0)

        print(f"\n{'-' * 100}")
        print(f"TRADES — {len(trades)} total | NET {wins} win / {losses} loss | "
              f"gross Rs {total_gross:+.2f} - charges Rs {total_chg:.2f} = NET Rs {total_pnl:+.2f}")
        if gross_wins != wins:
            print(f"  NOTE: {gross_wins - wins} trade(s) profitable on price alone "
                  f"became losses once costs were applied.")
        print('-' * 100)
        tcols = ["leg", "entry", "exit", "entry_rs", "exit_rs", "qty", "gross_rs", "charges", "net_rs", "reason"]
        twidths = {"leg": 3, "entry": 6, "exit": 6, "entry_rs": 9, "exit_rs": 9, "qty": 5,
                   "gross_rs": 10, "charges": 8, "net_rs": 10, "reason": 14}
        trows = []
        for t in trades:
            trows.append([
                t["leg"], _hhmm(t["entry_time"]), _hhmm(t["exit_time"]),
                f"{t['entry_price']:.2f}", f"{t['exit_price']:.2f}", str(t["qty"]),
                f"{_gross(t):+.2f}", f"{t.get('charges') or 0.0:.2f}",
                f"{t['pnl']:+.2f}", t["exit_reason"],
            ])
        _print_table(trows, tcols, twidths)
    else:
        print(f"\nNo trades for {day}.")

    # ── CSV export ────────────────────────────────────────────────────────
    out_dir = Path("data/exports")
    out_dir.mkdir(parents=True, exist_ok=True)

    candle_csv = out_dir / f"candle_log_{day}.csv"
    with candle_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["candle_start", "candle_end", "leg", "strike", "source", "ha_open", "ha_high",
                    "ha_low", "ha_close", "color", "stage_before", "stage_after", "target_level", "signal"])
        for r in candles:
            w.writerow([r["candle_start"], r["candle_end"], r["leg"], r["strike"], r["source"],
                        r["ha_open"], r["ha_high"], r["ha_low"], r["ha_close"], r["color"],
                        r["stage_before"], r["stage_after"], r["target_level"], r["signal"]])

    trades_csv = out_dir / f"trades_{day}.csv"
    with trades_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["leg", "strike", "entry_time", "exit_time", "entry_price", "exit_price",
                    "qty", "gross_pnl", "charges", "net_pnl", "exit_reason"])
        for t in trades:
            g = t.get("gross_pnl") if t.get("gross_pnl") is not None else t["pnl"]
            w.writerow([t["leg"], t["strike"], t["entry_time"], t["exit_time"], t["entry_price"],
                        t["exit_price"], t["qty"], g, t.get("charges") or 0.0, t["pnl"], t["exit_reason"]])

    print(f"\nCSV written: {candle_csv.resolve()}")
    print(f"CSV written: {trades_csv.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
