"""Manual order placement via the Polymarket CLOB, plus live fill notifications.

Credentials come from .env (see .env.example). The py-clob-client is synchronous,
so every call is wrapped in asyncio.to_thread.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import aiohttp

from market import MarketView

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


class Trader:
    """Thin wrapper over py-clob-client-v2 for manual limit orders."""

    def __init__(self) -> None:
        key = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
        if not key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY not set — copy .env.example to .env")
        funder = os.environ.get("POLYMARKET_FUNDER") or None
        sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "0"))
        host = os.environ.get("POLYMARKET_HOST", "https://clob.polymarket.com")

        # Polymarket's server terminates HTTP/2 connections during auth —
        # force the clob client onto HTTP/1.1.
        import httpx
        import py_clob_client_v2.http_helpers.helpers as _clob_helpers
        _clob_helpers._http_client = httpx.Client(http2=False, timeout=30)

        from py_clob_client_v2.client import ClobClient

        self._client = ClobClient(
            host, key=key, chain_id=137, signature_type=sig_type, funder=funder
        )
        self.creds = self._client.create_or_derive_api_key()
        self._client.set_api_creds(self.creds)
        self.address = funder or self._client.get_address()

        # Deposit-wallet accounts (signature_type=3) need the CLOB's
        # balance/allowance bookkeeping refreshed before the first order.
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

            self._client.update_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL, signature_type=sig_type
                )
            )
        except Exception as e:
            logger.warning("balance allowance refresh failed: %s", e)

        self._tick_size_cache: dict[str, str] = {}
        self._neg_risk_cache: dict[str, bool] = {}
        logger.info("Trader ready: address=%s sig_type=%d", self.address, sig_type)

    @classmethod
    async def connect(cls) -> "Trader":
        return await asyncio.to_thread(cls)

    async def _preflight(self, token_id: str) -> tuple[str, bool]:
        tick = self._tick_size_cache.get(token_id)
        neg = self._neg_risk_cache.get(token_id)
        if tick is None:
            tick = await asyncio.to_thread(self._client.get_tick_size, token_id)
            self._tick_size_cache[token_id] = tick
        if neg is None:
            neg = await asyncio.to_thread(self._client.get_neg_risk, token_id)
            self._neg_risk_cache[token_id] = neg
        return tick, neg

    async def place(
        self,
        token_id: str,
        side: str,  # "BUY" | "SELL"
        price: float,
        qty: float,
        ttl_sec: int | None = None,
    ) -> str:
        """Place a limit order. GTC by default; GTD if ttl_sec given.

        Polymarket requires GTD expiration >= now + 60s, so the wire value is
        now + 60 + ttl_sec.
        """
        from py_clob_client_v2.clob_types import (
            OrderArgs,
            OrderType,
            PartialCreateOrderOptions,
        )
        from py_clob_client_v2.order_builder.constants import BUY, SELL

        tick, neg_risk = await self._preflight(token_id)
        args = OrderArgs(
            token_id=token_id,
            price=price,
            size=qty,
            side=BUY if side == "BUY" else SELL,
        )
        order_type = OrderType.GTC
        if ttl_sec is not None:
            args.expiration = int(time.time()) + 60 + ttl_sec
            order_type = OrderType.GTD

        options = PartialCreateOrderOptions(tick_size=tick, neg_risk=neg_risk)
        signed = await asyncio.to_thread(self._client.create_order, args, options)
        resp = await asyncio.to_thread(self._client.post_order, signed, order_type)

        if not resp.get("success", False):
            raise RuntimeError(f"order rejected: {resp.get('errorMsg') or resp}")
        order_id = (
            resp.get("orderID") or resp.get("orderId")
            or resp.get("order_id") or resp.get("id")
        )
        if not order_id:
            raise RuntimeError(f"no order_id in response: {resp}")
        return str(order_id)

    async def cancel(self, order_id: str):
        from py_clob_client_v2.clob_types import OrderPayload

        return await asyncio.to_thread(
            self._client.cancel_order, OrderPayload(orderID=order_id)
        )

    async def cancel_all(self):
        return await asyncio.to_thread(self._client.cancel_all)

    async def open_orders(self) -> list[dict]:
        from py_clob_client_v2.clob_types import OpenOrderParams

        orders = await asyncio.to_thread(
            self._client.get_open_orders, OpenOrderParams()
        )
        return orders or []

    async def positions(self) -> list[dict]:
        """Current positions from the Polymarket data API."""
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{DATA_API}/positions",
                params={"user": self.address},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        return data if isinstance(data, list) else []


async def positions_poll_task(
    trader: Trader, view: MarketView, stop: asyncio.Event, interval: float = 0.25
) -> None:
    """Refresh view.positions continuously; back off briefly on API errors."""
    delay = interval
    while not stop.is_set():
        try:
            view.positions = await trader.positions()
            view.positions_ts_mono = time.monotonic()
            delay = interval
        except Exception as e:
            logger.warning("positions poll failed: %s", e)
            delay = 3.0  # back off so a rate limit can clear
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


async def user_fills_task(trader: Trader, view: MarketView, stop: asyncio.Event) -> None:
    """Stream order/trade events from the authenticated user channel and print fills."""
    creds = trader.creds
    auth_msg = {
        "auth": {
            "apiKey": getattr(creds, "api_key", ""),
            "secret": getattr(creds, "api_secret", ""),
            "passphrase": getattr(creds, "api_passphrase", ""),
        },
        "markets": [],
        "assets_ids": [],
        "type": "user",
    }
    ping_interval = 10.0
    backoff = 1.0
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    USER_WS_URL, receive_timeout=ping_interval * 3
                ) as ws:
                    await ws.send_str(json.dumps(auth_msg))
                    logger.info("user channel connected")
                    backoff = 1.0
                    last_ping = time.monotonic()
                    while not stop.is_set():
                        if time.monotonic() - last_ping >= ping_interval:
                            await ws.send_str("PING")
                            last_ping = time.monotonic()
                        try:
                            msg = await ws.receive(timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        if msg.data.strip().upper() in {"PING", "PONG", "OK"}:
                            continue
                        _print_user_events(view, msg.data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("user channel error: %s", e)
        if stop.is_set():
            return
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


def _print_user_events(view: MarketView, data: str) -> None:
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError:
        return
    for msg in parsed if isinstance(parsed, list) else [parsed]:
        et = msg.get("event_type") or msg.get("type")
        if et == "trade" and (msg.get("status") or "").upper() == "MATCHED":
            label = view.token_label(str(msg.get("asset_id", "")))
            size = float(msg.get("size", 0) or 0)
            price = float(msg.get("price", 0) or 0)
            print(f"\n*** FILL {msg.get('side', '?')} {label} {size:g} @ {price:.3f}")
            view.add_fill({
                "ts": time.strftime("%H:%M:%S"),
                "kind": "FILL",
                "side": msg.get("side", "?"),
                "label": label,
                "size": size,
                "price": price,
            })
        elif et == "order":
            matched = float(msg.get("size_matched", 0) or 0)
            if matched > 0:
                label = view.token_label(str(msg.get("asset_id", "")))
                price = float(msg.get("price", 0) or 0)
                status = msg.get("status") or "?"
                print(
                    f"\n*** ORDER {msg.get('side', '?')} {label} matched "
                    f"{matched:g} @ {price:.3f} ({status})"
                )
                view.add_fill({
                    "ts": time.strftime("%H:%M:%S"),
                    "kind": "ORDER",
                    "side": msg.get("side", "?"),
                    "label": label,
                    "size": matched,
                    "price": price,
                    "status": status,
                })
