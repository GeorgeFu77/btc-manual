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

# Candle endpoints used to recover each venue's official window-open price
# (1m candle whose time == window start; its open IS the price at the boundary)
PM_CANDLES_URL = "https://polymarket.com/api/chainlink-candles"
CB_CANDLES_URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
BN_KLINES_URL = "https://api.binance.com/api/v3/klines"
_UA = {"User-Agent": "Mozilla/5.0"}

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
                        ticks = _parse_pm(msg.data)
                        for ts_ms, px in ticks:
                            view.update_pm(px, ts_ms)
                        if ticks:
                            last_data = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("PM feed error: %s", e)
        if stop.is_set():
            return
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)


def _parse_pm(data: str) -> list[tuple[int, float]]:
    """Extract (timestamp_ms, price) ticks from a PM RTDS message.

    The first message after connect is a ~60s backfill batch — every tick is
    returned (in timestamp order) so the window-open tick can be recovered.
    """
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError:
        return []
    payload = parsed.get("payload") or {}
    batch = payload.get("data")
    if isinstance(batch, list):
        items = batch
    elif payload.get("value") is not None:
        items = [payload]
    else:
        return []
    ticks = []
    for item in items:
        try:
            ticks.append((int(item["timestamp"]), float(item["value"])))
        except (KeyError, ValueError, TypeError):
            continue
    ticks.sort()
    return ticks


async def anchor_task(view: MarketView, stop: asyncio.Event) -> None:
    """Backfill official window-open prices from each venue's candle API.

    The live feeds capture the boundary tick when the app is running at a
    window change; this task covers mid-window starts (and acts as a check)
    so the deltas match Polymarket's site exactly. Retries every second
    until each anchor for the current window is known.
    """
    timeout = aiohttp.ClientTimeout(total=4)
    async with aiohttp.ClientSession() as session:
        while not stop.is_set():
            wid = view.window_start()
            try:
                if view.pm_official is None:
                    async with session.get(
                        PM_CANDLES_URL,
                        params={
                            "symbol": "BTC", "interval": "1m", "limit": "15",
                            "endTime": str((wid + 120) * 1000),
                        },
                        headers=_UA, timeout=timeout,
                    ) as r:
                        if r.status == 200:
                            candles = (await r.json()).get("candles") or []
                            row = [c for c in candles if c.get("time") == wid]
                            if row:
                                view.set_official("pm", float(row[0]["open"]), wid)
                                logger.info("PM anchor (official beat): %s", row[0]["open"])
            except Exception as e:
                logger.debug("pm anchor fetch failed: %s", e)
            try:
                if view.cb_official is None:
                    async with session.get(
                        CB_CANDLES_URL,
                        params={"granularity": "60", "start": str(wid - 60), "end": str(wid + 60)},
                        headers=_UA, timeout=timeout,
                    ) as r:
                        if r.status == 200:
                            rows = [c for c in await r.json() if c and c[0] == wid]
                            if rows:  # [time, low, high, open, close, vol]
                                view.set_official("cb", float(rows[0][3]), wid)
            except Exception as e:
                logger.debug("cb anchor fetch failed: %s", e)
            try:
                if view.bn_official is None:
                    async with session.get(
                        BN_KLINES_URL,
                        params={
                            "symbol": "BTCUSDT", "interval": "1m",
                            "startTime": str(wid * 1000), "limit": "1",
                        },
                        headers=_UA, timeout=timeout,
                    ) as r:
                        if r.status == 200:
                            rows = await r.json()
                            if rows and int(rows[0][0]) == wid * 1000:
                                view.set_official("bn", float(rows[0][1]), wid)
            except Exception as e:
                logger.debug("bn anchor fetch failed: %s", e)

            try:
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass


async def _slug_tokens(
    session: aiohttp.ClientSession, slug: str
) -> tuple[str, str] | None:
    """Fetch (token_up, token_down) for a market slug via the Gamma API."""
    try:
        async with session.get(
            GAMMA_URL, params={"slug": slug},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status != 200:
                return None
            events = await resp.json()
        if not events:
            return None
        markets = events[0].get("markets") or []
        if not markets:
            return None
        raw_ids = markets[0].get("clobTokenIds")
        if isinstance(raw_ids, str):  # JSON-encoded string, parse again
            raw_ids = json.loads(raw_ids)
        if not raw_ids or len(raw_ids) < 2:
            return None
        return raw_ids[0], raw_ids[1]
    except (aiohttp.ClientError, json.JSONDecodeError, KeyError, IndexError, asyncio.TimeoutError):
        return None


async def discover_window(
    session: aiohttp.ClientSession, view: MarketView
) -> tuple[str, str, str] | None:
    """Find the active market via the Gamma API.

    Returns (slug, token_up, token_down) or None. The next window's market
    pre-exists on Gamma, so it is only tried when the boundary is actually
    near — otherwise one transient failure on the current slug would route
    trading to a window that hasn't started yet.
    """
    base = view.window_start()
    candidates = [base]
    if view.seconds_left() <= 20:
        candidates.append(base + view.bucket_sec)
    candidates.append(base - view.bucket_sec)
    for ts in candidates:
        slug = f"{view.slug_prefix}{ts}"
        toks = await _slug_tokens(session, slug)
        if toks:
            return slug, toks[0], toks[1]
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
            window_ts = int(slug.rsplit("-", 1)[-1])
            boundary_mono = time.monotonic() + (
                window_ts + view.bucket_sec - time.time()
            )
            if boundary_mono <= time.monotonic():
                # stale fallback window (already expired) — never activate it
                await asyncio.sleep(2.0)
                continue
            view.set_window(slug, token_up, token_down)
            logger.info("CLOB window: %s", slug)

            if not view.prev_token_up:
                # recover the previous window's tokens after a fresh start so
                # the positions panel can show the just-settled market too
                prev = await _slug_tokens(
                    session, f"{view.slug_prefix}{window_ts - view.bucket_sec}"
                )
                if prev:
                    view.prev_token_up, view.prev_token_down = prev

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
