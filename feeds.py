"""WebSocket feeds: Coinbase, Binance, Polymarket Chainlink price, Polymarket CLOB.

All feeds auto-reconnect with exponential backoff and write into a shared
MarketView. Protocol quirks carried over from the proven implementation:
- Polymarket WS keepalives are the literal string "PING", not WS ping frames.
- Gamma's clobTokenIds field is a JSON-encoded *string* (parse twice).
- Window boundaries are tracked on the monotonic clock.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp

from market import GAMMA_URL, MarketView

logger = logging.getLogger(__name__)

CB_URL = "wss://ws-feed.exchange.coinbase.com"
BN_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
PM_URL = "wss://ws-live-data.polymarket.com"
CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

_BACKOFF_MAX = 30.0


async def cb_task(view: MarketView, stop: asyncio.Event) -> None:
    """Coinbase BTC-USD ticker feed."""
    backoff = 1.0
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    CB_URL, heartbeat=20, receive_timeout=30
                ) as ws:
                    await ws.send_json({
                        "type": "subscribe",
                        "product_ids": ["BTC-USD"],
                        "channels": ["ticker"],
                    })
                    logger.info("CB connected")
                    backoff = 1.0
                    async for msg in ws:
                        if stop.is_set():
                            return
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        d = json.loads(msg.data)
                        if d.get("type") == "ticker" and d.get("price"):
                            view.update_cb(float(d["price"]))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("CB feed error: %s", e)
        if stop.is_set():
            return
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)


async def bn_task(view: MarketView, stop: asyncio.Event) -> None:
    """Binance BTC-USDT trade feed."""
    backoff = 1.0
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    BN_URL, heartbeat=20, receive_timeout=30
                ) as ws:
                    logger.info("BN connected")
                    backoff = 1.0
                    async for msg in ws:
                        if stop.is_set():
                            return
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        d = json.loads(msg.data)
                        if d.get("p"):
                            view.update_bn(float(d["p"]))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("BN feed error: %s", e)
        if stop.is_set():
            return
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)


async def pm_task(view: MarketView, stop: asyncio.Event) -> None:
    """Polymarket Chainlink reference price (RTDS) feed."""
    backoff = 1.0
    stale_sec = 10.0  # PM ticks ~1/s; silence means a half-dead connection
    while not stop.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    PM_URL, heartbeat=20, receive_timeout=40
                ) as ws:
                    await ws.send_json({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": '{"symbol":"btc/usd"}',
                        }],
                    })
                    logger.info("PM connected")
                    backoff = 1.0
                    last_data = time.monotonic()
                    while not stop.is_set():
                        if time.monotonic() - last_data > stale_sec:
                            logger.warning("PM feed stale, reconnecting")
                            break
                        try:
                            msg = await ws.receive(timeout=2.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT or not msg.data:
                            continue
                        px = _parse_pm(msg.data)
                        if px is not None:
                            view.update_pm(px)
                            last_data = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("PM feed error: %s", e)
        if stop.is_set():
            return
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)


def _parse_pm(data: str) -> float | None:
    """Extract the latest price from a PM RTDS message (batch or single)."""
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError:
        return None
    payload = parsed.get("payload") or {}
    batch = payload.get("data")
    if isinstance(batch, list) and batch:
        item = batch[-1]
    elif payload.get("value") is not None:
        item = payload
    else:
        return None
    try:
        return float(item["value"])
    except (KeyError, ValueError, TypeError):
        return None


async def discover_window(
    session: aiohttp.ClientSession, view: MarketView
) -> tuple[str, str, str] | None:
    """Find the active market via the Gamma API.

    Returns (slug, token_up, token_down) or None. Tries the current bucket
    first, then next, then previous.
    """
    base = view.window_start()
    for ts in (base, base + view.bucket_sec, base - view.bucket_sec):
        slug = f"{view.slug_prefix}{ts}"
        try:
            async with session.get(
                GAMMA_URL, params={"slug": slug},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    continue
                events = await resp.json()
            if not events:
                continue
            markets = events[0].get("markets") or []
            if not markets:
                continue
            raw_ids = markets[0].get("clobTokenIds")
            if isinstance(raw_ids, str):  # JSON-encoded string, parse again
                raw_ids = json.loads(raw_ids)
            if not raw_ids or len(raw_ids) < 2:
                continue
            return slug, raw_ids[0], raw_ids[1]
        except (aiohttp.ClientError, json.JSONDecodeError, KeyError, IndexError, asyncio.TimeoutError):
            continue
    return None


async def clob_task(view: MarketView, stop: asyncio.Event) -> None:
    """Order book feed for the active window's UP/DOWN tokens; rotates each window."""
    ping_interval = 10.0
    async with aiohttp.ClientSession() as session:
        while not stop.is_set():
            found = await discover_window(session, view)
            if found is None:
                await asyncio.sleep(2.0)
                continue
            slug, token_up, token_down = found
            view.set_window(slug, token_up, token_down)
            logger.info("CLOB window: %s", slug)

            window_ts = int(slug.rsplit("-", 1)[-1])
            boundary_mono = time.monotonic() + (
                window_ts + view.bucket_sec - time.time()
            )

            try:
                async with session.ws_connect(CLOB_URL, receive_timeout=ping_interval * 3) as ws:
                    await ws.send_str(json.dumps({
                        "assets_ids": [token_up, token_down],
                        "type": "market",
                    }))
                    last_ping = time.monotonic()
                    while not stop.is_set():
                        now = time.monotonic()
                        if now >= boundary_mono:
                            break  # window over — rediscover
                        if now - last_ping >= ping_interval:
                            await ws.send_str("PING")
                            last_ping = now
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
                        _apply_clob(view, msg.data, token_up, token_down)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("CLOB feed error: %s", e)
                if not stop.is_set():
                    await asyncio.sleep(1.0)


def _apply_clob(view: MarketView, data: str, token_up: str, token_down: str) -> None:
    """Apply book / price_change events to the view's quotes."""
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError:
        return
    side_of = {token_up: "up", token_down: "down"}
    for msg in parsed if isinstance(parsed, list) else [parsed]:
        et = msg.get("event_type")
        if et == "book":
            side = side_of.get(msg.get("asset_id", ""))
            if side is None:
                continue
            buys, sells = msg.get("buys") or [], msg.get("sells") or []
            bid = float(buys[0]["price"]) if buys else None
            ask = float(sells[0]["price"]) if sells else None
            view.update_quote(side, bid, ask)
        elif et == "price_change":
            for change in msg.get("price_changes") or []:
                side = side_of.get(change.get("asset_id", ""))
                if side is None:
                    continue
                bid = change.get("best_bid")
                ask = change.get("best_ask")
                view.update_quote(
                    side,
                    float(bid) if bid is not None else None,
                    float(ask) if ask is not None else None,
                )
