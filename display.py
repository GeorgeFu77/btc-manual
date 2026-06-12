"""Render the live numbers line and helper tables (ANSI-colored)."""

from __future__ import annotations

import time

from market import MarketView

GREEN = "\x1b[32m"
RED = "\x1b[31m"
DIM = "\x1b[2m"
RESET = "\x1b[0m"


def _f(v: float | None, fmt: str = "{:.1f}", dash: str = "-") -> str:
    return fmt.format(v) if v is not None else dash


def _signed(v: float | None) -> str:
    """Signed value, green when positive (up), red when negative (down)."""
    if v is None:
        return f"{DIM}-{RESET}"
    color = GREEN if v >= 0 else RED
    return f"{color}{v:+.1f}{RESET}"


def _age(ts_mono: float) -> str:
    if ts_mono == 0.0:
        return "-"
    a = time.monotonic() - ts_mono
    return f"{a:.0f}s" if a >= 9.95 else f"{a:.1f}s"


def _quote(q, color: str) -> str:
    if q.bid is None and q.ask is None:
        return f"{DIM}-/-{RESET}"
    bid = f"{q.bid:.2f}" if q.bid is not None else "-"
    ask = f"{q.ask:.2f}" if q.ask is not None else "-"
    return f"{color}{bid}/{ask}{RESET}"


def snapshot(view: MarketView) -> str:
    """One status line: clock, time left, prices + deltas, edge (last 45s), books, ages."""
    clock = time.strftime("%H:%M:%S")
    left = view.seconds_left()
    edge = _signed(view.edge) if view.edge_active else f"{DIM}-{RESET}"
    # '~' marks an approximate anchor (official window-open tick not seen yet)
    approx = "" if view.pm_official is not None else "~"
    beat = view.pm_official if view.pm_official is not None else view.pm_ptb
    parts = [
        f"{clock} left {left:3.0f}s",
        f"CB {_f(view.cb_last, '{:.1f}')} Δ{_signed(view.cb_delta)}",
        f"BN {_f(view.bn_last, '{:.1f}')} Δ{_signed(view.bn_delta)}",
        f"PM {_f(view.pm_last, '{:.1f}')} Δ{approx}{_signed(view.pm_delta)}",
        f"{DIM}beat {_f(beat, '{:.2f}')}{approx}{RESET}",
        f"edge {edge}",
        f"{GREEN}UP{RESET} {_quote(view.up, GREEN)}",
        f"{RED}DN{RESET} {_quote(view.dn, RED)}",
        f"{DIM}age cb{_age(view.cb_ts_mono)} bn{_age(view.bn_ts_mono)} "
        f"pm{_age(view.pm_ts_mono)} "
        f"up{_age(view.up.ts_mono)} dn{_age(view.dn.ts_mono)}{RESET}",
    ]
    pos = position_summary(view)
    if pos:
        parts.insert(len(parts) - 1, pos)  # before the age block
    return " | ".join(parts)


def change_key(view: MarketView) -> tuple:
    """Everything that should trigger a new tape line when it changes."""
    return (
        view.cb_last, view.bn_last, view.pm_last, view.pm_official,
        view.up.bid, view.up.ask, view.dn.bid, view.dn.ask,
        view.edge_active, view.slug,
    )


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
            color = GREEN if label == "UP" else RED
            bits.append(f"{color}{label} {size:g}@{avg:.2f}{RESET}")
        else:
            other += 1
    if other:
        bits.append(f"{DIM}+{other} other{RESET}")
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
        pnl_color = GREEN if pnl >= 0 else RED
        lines.append(
            f"  {title} [{outcome}] size={size:g} avg={avg:.3f} "
            f"cur={cur:.3f} pnl={pnl_color}{pnl:+.2f}{RESET}"
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
