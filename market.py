"""Shared market state. Feeds write into MarketView; the display and REPL read it."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

BUCKET_SEC = 900  # 15-minute windows
SLUG_PREFIX = "btc-updown-15m-"
GAMMA_URL = "https://gamma-api.polymarket.com/events"


def current_window_start(now: float | None = None) -> int:
    """Unix second the active 15m window started."""
    if now is None:
        now = time.time()
    return int(now // BUCKET_SEC) * BUCKET_SEC


def seconds_left(now: float | None = None) -> float:
    if now is None:
        now = time.time()
    return BUCKET_SEC - (now % BUCKET_SEC)


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

    def age(self) -> float | None:
        if self.ts_mono == 0.0:
            return None
        return time.monotonic() - self.ts_mono


@dataclass
class MarketView:
    # Spot prices
    cb_last: float | None = None  # Coinbase BTC-USD
    cb_ts_mono: float = 0.0
    pm_last: float | None = None  # Polymarket Chainlink reference
    pm_ts_mono: float = 0.0

    # Price at the top of the current window, per source (settlement anchor)
    cb_window_id: int = 0
    cb_ptb: float | None = None
    pm_window_id: int = 0
    pm_ptb: float | None = None

    # Active Polymarket window + order books
    slug: str = ""
    token_up: str = ""
    token_down: str = ""
    up: Quote = field(default_factory=Quote)
    dn: Quote = field(default_factory=Quote)

    # Latest positions snapshot from the data API (when trading is enabled)
    positions: list[dict] = field(default_factory=list)
    positions_ts_mono: float = 0.0

    def update_cb(self, px: float) -> None:
        wid = current_window_start()
        if wid != self.cb_window_id:
            self.cb_window_id = wid
            self.cb_ptb = px
        self.cb_last = px
        self.cb_ts_mono = time.monotonic()

    def update_pm(self, px: float) -> None:
        wid = current_window_start()
        if wid != self.pm_window_id:
            self.pm_window_id = wid
            self.pm_ptb = px
        self.pm_last = px
        self.pm_ts_mono = time.monotonic()

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

    # Deltas since window start — UP wins if the Chainlink (PM) price ends >= start.
    # CB leads PM by ~2s, so cb_delta is the early signal, edge the divergence.
    @property
    def cb_delta(self) -> float | None:
        if self.cb_last is None or self.cb_ptb is None:
            return None
        return self.cb_last - self.cb_ptb

    @property
    def pm_delta(self) -> float | None:
        if self.pm_last is None or self.pm_ptb is None:
            return None
        return self.pm_last - self.pm_ptb

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
