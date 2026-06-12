"""Render the live numbers line and helper tables."""

from __future__ import annotations

import time

from market import MarketView, seconds_left


def _f(v: float | None, fmt: str = "{:.1f}", dash: str = "-") -> str:
    return fmt.format(v) if v is not None else dash


def _signed(v: float | None) -> str:
    return f"{v:+.1f}" if v is not None else "-"

def _age(ts_mono: float) -> str:
    if ts_mono == 0.0:
        return "-"
    a = time.monotonic() - ts_mono
    return f"{a:.0f}s" if a >= 9.95 else f"{a:.1f}s"


def _quote(q) -> str:
    if q.bid is None and q.ask is None:
        return "-/-"
    bid = f"{q.bid:.2f}" if q.bid is not None else "-"
    ask = f"{q.ask:.2f}" if q.ask is not None else "-"
    return f"{bid}/{ask}"


def snapshot(view: MarketView) -> str:
    """One status line: clock, time left, CB/PM prices + deltas, edge, books, ages."""
    clock = time.strftime("%H:%M:%S")
    left = seconds_left()
    parts = [
        f"{clock} left {left:3.0f}s",
        f"CB {_f(view.cb_last, '{:.1f}')} Δ{_signed(view.cb_delta)}",
        f"PM {_f(view.pm_last, '{:.1f}')} Δ{_signed(view.pm_delta)}",
        f"edge {_signed(view.edge)}",
        f"UP {_quote(view.up)}",
        f"DN {_quote(view.dn)}",
        f"age cb{_age(view.cb_ts_mono)} pm{_age(view.pm_ts_mono)} "
        f"up{_age(view.up.ts_mono)} dn{_age(view.dn.ts_mono)}",
    ]
    pos = position_summary(view)
    if pos:
        parts.insert(6, pos)
    return " | ".join(parts)


def position_summary(view: MarketView) -> str:
    """Short summary of positions in the *current* window's tokens."""
    if not view.positions:
        return ""
    current = {view.token_up: "UP", view.token_down: "DN"}
    bits = []
    other = 0
    for p in view.positions:
        size = float(p.get("size", 0) or 0)
        if size == 0:
            continue
        label = current.get(str(p.get("asset", "")))
        if label:
            avg = float(p.get("avgPrice", 0) or 0)
            bits.append(f"{label} {size:g}@{avg:.2f}")
        else:
            other += 1
    if other:
        bits.append(f"+{other} other")
    return ("pos " + ", ".join(bits)) if bits else ""


def positions_table(view: MarketView) -> str:
    """Full positions listing for the `p` command."""
    if not view.positions:
        return "no positions"
    lines = []
    for p in view.positions:
        size = float(p.get("size", 0) or 0)
        if size == 0:
            continue
        title = p.get("title") or p.get("slug") or "?"
        outcome = p.get("outcome") or "?"
        avg = float(p.get("avgPrice", 0) or 0)
        cur = float(p.get("curPrice", 0) or 0)
        pnl = float(p.get("cashPnl", 0) or 0)
        lines.append(
            f"  {title} [{outcome}] size={size:g} avg={avg:.3f} "
            f"cur={cur:.3f} pnl={pnl:+.2f}"
        )
    return "\n".join(lines) if lines else "no positions"


def orders_table(view: MarketView, orders: list[dict]) -> str:
    """Open orders listing for the `o` command."""
    if not orders:
        return "no open orders"
    lines = []
    for o in orders:
        oid = str(o.get("id") or o.get("orderID") or "?")
        label = view.token_label(str(o.get("asset_id", "")))
        side = o.get("side", "?")
        price = float(o.get("price", 0) or 0)
        size = float(o.get("original_size", 0) or 0)
        matched = float(o.get("size_matched", 0) or 0)
        lines.append(
            f"  {oid}  {side} {label} {size:g} @ {price:.3f} (filled {matched:g})"
        )
    return "\n".join(lines)
