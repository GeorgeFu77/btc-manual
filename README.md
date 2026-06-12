# btc_manual

Minimal manual-trading console for Polymarket BTC 15-minute up/down markets.
Live market numbers + manual limit orders. No bot, no strategies.

## What it shows

One live status line (bottom bar in the REPL, scrolling lines in `--watch`):

```
14:32:05 left 433s | CB 76512.3 Δ+12.4 | PM 76500.1 Δ+8.2 | edge +4.2 | UP 0.52/0.53 | DN 0.47/0.48 | pos UP 10@0.52 | age cb0.1s pm0.4s up0.2s dn0.2s
```

- **CB** — Coinbase BTC-USD spot, **Δ** = change since the window started
- **PM** — Polymarket's Chainlink reference price (what settlement uses), **Δ** same
- **edge** — CB Δ minus PM Δ (CB leads PM by ~2s)
- **UP/DN** — best bid/ask for the window's UP and DOWN tokens
- **age** — seconds since each feed last updated (watch for stale data)

Fills stream in live (`*** FILL BUY UP 10 @ 0.520`) when trading is enabled.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in keys — only needed for trading
```

## Run

```bash
.venv/bin/python main.py           # REPL + live status bar
.venv/bin/python main.py --watch   # numbers only, Ctrl-C to quit
```

## Commands

```
b u 0.55 10 [ttl]   buy 10 UP @ 0.55 (optional ttl seconds -> GTD order)
s d 0.47 5          sell 5 DOWN @ 0.47
b u m 10            'm' = pay the current best ask (best bid when selling)
o                   list open orders
c <id> | c all      cancel one order / all orders
p                   positions (from the Polymarket data API)
line                print one snapshot line into the scrollback
h / q               help / quit
```

Orders are limit orders on the CLOB. Prices are in probability (0.01–0.99).
`ttl` makes the order good-til-date; Polymarket enforces a minimum of 60s,
so the wire expiration is `now + 60 + ttl`.
