"""BTC up/down manual trader — live numbers + manual Polymarket orders.

5-minute markets by default (--window 15 for the 15m market). A new tape
line is printed whenever any number changes; the old lines stay in the
scrollback. Edge is only shown in the final 45 seconds of a window.

Usage:
    python main.py            # live tape + status bar + trading REPL
    python main.py --web      # localhost web UI (open in Chrome)
    python main.py --watch    # tape only, Ctrl-C to exit

Commands (REPL):
    b u 0.55 10 [ttl]   buy 10 UP @ 0.55 (optional ttl seconds -> GTD)
    s d 0.47 5          sell 5 DOWN @ 0.47
    b u m 10            price 'm' = take the current best ask (bid when selling)
    o                   open orders        c <id> | c all   cancel
    p                   positions          line             print one snapshot
    h                   help               q                quit
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from dotenv import load_dotenv

import display
from feeds import anchor_task, bn_task, cb_task, clob_task, pm_task
from market import MarketView
from trader import Trader, positions_poll_task, user_fills_task

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("main")

HELP = __doc__.split("Commands (REPL):", 1)[1]


async def tape(view: MarketView, stop: asyncio.Event) -> None:
    """Print a new line whenever any number changes (old lines stay above)."""
    # plain print() gets its ANSI colors escaped by patch_stdout —
    # print_formatted_text renders them properly above the prompt
    from prompt_toolkit import print_formatted_text
    from prompt_toolkit.formatted_text import ANSI

    last_key: tuple | None = None
    min_gap = 0.2  # don't exceed ~5 lines/sec when books churn
    last_print = 0.0
    import time as _time

    while not stop.is_set():
        key = display.change_key(view)
        now = _time.monotonic()
        if key != last_key and now - last_print >= min_gap:
            print_formatted_text(ANSI(display.snapshot(view)))
            last_key = key
            last_print = now
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except asyncio.TimeoutError:
            pass


def _parse_outcome(tok: str, view: MarketView) -> tuple[str, str] | None:
    """Map 'u'/'up'/'d'/'down' to (label, token_id)."""
    t = tok.lower()
    if t in ("u", "up"):
        return "UP", view.token_up
    if t in ("d", "dn", "down"):
        return "DOWN", view.token_down
    return None


async def handle_trade(
    trader: Trader, view: MarketView, side: str, args: list[str]
) -> None:
    if len(args) < 3:
        print("usage: b|s u|d <price|m> <qty> [ttl_sec]")
        return
    outcome = _parse_outcome(args[0], view)
    if outcome is None:
        print(f"unknown outcome '{args[0]}' (use u/up or d/down)")
        return
    label, token_id = outcome
    if not token_id:
        print("no active window discovered yet — wait for the CLOB feed")
        return

    quote = view.up if label == "UP" else view.dn
    if args[1].lower() == "m":
        px = quote.ask if side == "BUY" else quote.bid
        if px is None:
            print("no quote available for market-price order")
            return
        price = min(max(px, 0.01), 0.99)
    else:
        try:
            price = float(args[1])
        except ValueError:
            print(f"bad price '{args[1]}'")
            return
        if not 0.0 < price < 1.0:
            print("price must be between 0 and 1")
            return
    try:
        qty = float(args[2])
    except ValueError:
        print(f"bad qty '{args[2]}'")
        return
    ttl = None
    if len(args) >= 4:
        try:
            ttl = int(args[3])
        except ValueError:
            print(f"bad ttl '{args[3]}'")
            return

    kind = f"GTD({ttl}s)" if ttl is not None else "GTC"
    print(f"placing {side} {label} {qty:g} @ {price:.3f} {kind} [{view.slug}] ...")
    try:
        order_id = await trader.place(token_id, side, price, qty, ttl)
        print(f"order accepted: {order_id}")
    except Exception as e:
        print(f"order FAILED: {e}")


async def repl(view: MarketView, stop: asyncio.Event, trading_tasks: list) -> None:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.formatted_text import ANSI
    from prompt_toolkit.patch_stdout import patch_stdout

    trader: Trader | None = None
    if os.environ.get("POLYMARKET_PRIVATE_KEY"):
        print("connecting trader ...")
        try:
            trader = await Trader.connect()
            print(f"trading ready: {trader.address}")
            trading_tasks.append(asyncio.create_task(user_fills_task(trader, view, stop)))
            trading_tasks.append(asyncio.create_task(positions_poll_task(trader, view, stop)))
        except Exception as e:
            print(f"trader init failed ({e}) — display only")
    else:
        print("no POLYMARKET_PRIVATE_KEY in .env — display only")

    session: "PromptSession[str]" = PromptSession()
    with patch_stdout():
        trading_tasks.append(asyncio.create_task(tape(view, stop)))
        while not stop.is_set():
            try:
                line = await session.prompt_async(
                    "> ",
                    bottom_toolbar=lambda: ANSI(display.snapshot(view)),
                    refresh_interval=0.5,
                )
            except (EOFError, KeyboardInterrupt):
                break
            parts = line.strip().split()
            if not parts:
                continue
            cmd, args = parts[0].lower(), parts[1:]

            if cmd in ("q", "quit", "exit"):
                break
            elif cmd in ("h", "help", "?"):
                print(HELP)
            elif cmd == "line":
                print(display.snapshot(view))
            elif cmd in ("b", "buy", "s", "sell"):
                if trader is None:
                    print("trading not available (no credentials)")
                    continue
                await handle_trade(
                    trader, view, "BUY" if cmd.startswith("b") else "SELL", args
                )
            elif cmd in ("o", "orders"):
                if trader is None:
                    print("trading not available")
                    continue
                try:
                    print(display.orders_table(view, await trader.open_orders()))
                except Exception as e:
                    print(f"failed: {e}")
            elif cmd in ("c", "cancel"):
                if trader is None:
                    print("trading not available")
                    continue
                if not args:
                    print("usage: c <order_id> | c all")
                    continue
                try:
                    if args[0].lower() == "all":
                        resp = await trader.cancel_all()
                    else:
                        resp = await trader.cancel(args[0])
                    print(f"cancel ok: {resp}")
                except Exception as e:
                    print(f"cancel failed: {e}")
            elif cmd in ("p", "pos", "positions"):
                if trader is None:
                    print("trading not available")
                    continue
                try:
                    view.positions = await trader.positions()
                    print(display.positions_table(view))
                except Exception as e:
                    print(f"failed: {e}")
            else:
                print(f"unknown command '{cmd}' — h for help")


async def amain() -> None:
    parser = argparse.ArgumentParser(description="BTC up/down manual trader")
    parser.add_argument(
        "--watch", action="store_true",
        help="display only: print the live tape, no REPL",
    )
    parser.add_argument(
        "--window", type=int, choices=(5, 15), default=5,
        help="market window in minutes (default 5)",
    )
    parser.add_argument(
        "--web", action="store_true",
        help="serve the web UI on localhost instead of the REPL",
    )
    parser.add_argument(
        "--port", type=int, default=8080,
        help="web UI port (default 8080)",
    )
    cli = parser.parse_args()

    load_dotenv()
    view = MarketView(
        bucket_sec=cli.window * 60,
        slug_prefix=f"btc-updown-{cli.window}m-",
    )
    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(cb_task(view, stop)),
        asyncio.create_task(bn_task(view, stop)),
        asyncio.create_task(pm_task(view, stop)),
        asyncio.create_task(clob_task(view, stop)),
        asyncio.create_task(anchor_task(view, stop)),
    ]

    try:
        if cli.watch:
            await tape(view, stop)
        elif cli.web:
            from web import run_web

            trader = None
            if os.environ.get("POLYMARKET_PRIVATE_KEY"):
                print("connecting trader ...")
                try:
                    trader = await Trader.connect()
                    print(f"trading ready: {trader.address}")
                    tasks.append(asyncio.create_task(user_fills_task(trader, view, stop)))
                    tasks.append(asyncio.create_task(positions_poll_task(trader, view, stop)))
                except Exception as e:
                    print(f"trader init failed ({e}) — display only")
            else:
                print("no POLYMARKET_PRIVATE_KEY in .env — display only")
            await run_web(view, trader, stop, port=cli.port)
        else:
            await repl(view, stop, tasks)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
