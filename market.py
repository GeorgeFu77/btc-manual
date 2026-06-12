"""Shared market state. Feeds write into MarketView; the display and REPL read it."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

GAMMA_URL = "https://gamma-api.polymarket.com/events"
EDGE_WINDOW_SEC = 45  # edge is only meaningful (and shown) in the final 45s


@dataclass
class Quote:
    bid: float | None = None
    ask: float | None = None
    ts_mono: float = 0.0

    @property
    def mid(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2


@dataclass
class MarketView:
    # Window config: 5-minute markets by default (btc-updown-5m-<unix>)
    bucket_sec: int = 300
    slug_prefix: str = "btc-updown-5m-"

    # Spot prices
    cb_last: float | None = None  # Coinbase BTC-USD
    cb_ts_mono: float = 0.0
    pm_last: float | None = None  # Polymarket Chainlink reference
    pm_ts_mono: float = 0.0
    bn_last: float | None = None  # Binance BTC-USDT
    bn_ts_mono: float = 0.0

    # Price at the top of the current window, per source (settlement anchor)
    cb_window_id: int = 0
    cb_ptb: float | None = None
    pm_window_id: int = 0
    pm_ptb: float | None = None
    bn_window_id: int = 0
    bn_ptb: float | None = None

    # Active Polymarket window + order books
    slug: str = ""
    token_up: str = ""
    token_down: str = ""
    up: Quote = field(default_factory=Quote)
    dn: Quote = field(default_factory=Quote)

    # Latest positions snapshot from the data API (when trading is enabled)
    positions: list[dict] = field(default_factory=list)
    positions_ts_mono: float = 0.0

    def window_start(self, now: float | None = None) -> int:
        """Unix second the active window started."""
        if now is None:
            now = time.time()
        return int(now // self.bucket_sec) * self.bucket_sec

    def seconds_left(self, now: float | None = None) -> float:
        if now is None:
            now = time.time()
        return self.bucket_sec - (now % self.bucket_sec)

    @property
    def edge_active(self) -> bool:
        return self.seconds_left() <= EDGE_WINDOW_SEC

    def _roll(self, attr: str, px: float) -> None:
        wid = self.window_start()
        if wid != getattr(self, f"{attr}_window_id"):
            setattr(self, f"{attr}_window_id", wid)
            setattr(self, f"{attr}_ptb", px)
        setattr(self, f"{attr}_last", px)
        setattr(self, f"{attr}_ts_mono", time.monotonic())

    def update_cb(self, px: float) -> None:
        self._roll("cb", px)

    def update_pm(self, px: float) -> None:
        self._roll("pm", px)

    def update_bn(self, px: float) -> None:
        self._roll("bn", px)

    def set_window(self, slug: str, token_up: str, token_down: str) -> None:
        self.slug = slug
        self.token_up = token_up
        self.token_down = token_down
        self.up = Quote()
        self.dn = Quote()

    def update_quote(self, side: str, bid: float | None, ask: float | None) -> None:
        q = self.up if side == "up" else self.dn
        if bid is not None:
            q.bid = bid
        if ask is not None:
            q.ask = ask
        q.ts_mono = time.monotonic()

    def _delta(self, attr: str) -> float | None:
        last = getattr(self, f"{attr}_last")
        ptb = getattr(self, f"{attr}_ptb")
        if last is None or ptb is None:
            return None
        return last - ptb

    # Deltas since window start — UP wins if the Chainlink (PM) price ends >= start.
    # CB leads PM by ~2s, so cb_delta is the early signal, edge the divergence.
    @property
    def cb_delta(self) -> float | None:
        return self._delta("cb")

    @property
    def pm_delta(self) -> float | None:
        return self._delta("pm")

    @property
    def bn_delta(self) -> float | None:
        return self._delta("bn")

    @property
    def edge(self) -> float | None:
        if self.cb_delta is None or self.pm_delta is None:
            return None
        return self.cb_delta - self.pm_delta

    def token_label(self, token_id: str) -> str:
        if token_id == self.token_up:
            return "UP"
        if token_id == self.token_down:
            return "DOWN"
        return token_id[:10] + "…"
