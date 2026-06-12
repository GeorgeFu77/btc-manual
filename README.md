# btc_manual

Minimal manual-trading console for Polymarket BTC up/down markets
(5-minute windows by default). Live market numbers + manual limit orders.
No bot, no strategies.

## What it shows

A live tape — a new line is printed whenever any number changes, and the old
lines stay in the scrollback. The same line also sits in the bottom status bar
in the REPL. UP is green, DOWN is red; deltas are green when positive, red
when negative.

```
23:29:15 left  45s | CB 63063.0 Δ-6.7 | BN 63155.0 Δ-10.1 | PM 63064.9 Δ-7.7 | edge +1.0 | UP 0.03/0.04 | DN 0.96/0.97 | age cb0.2s bn0.3s pm1.0s up0.0s dn0.0s
```

- **CB** — Coinbase BTC-USD spot, **Δ** = change since the window started
- **BN** — Binance BTC-USDT, same Δ
- **PM** — Polymarket's Chainlink reference price (what settlement uses)
- **edge** — CB Δ minus PM Δ (CB leads PM by ~2s); only shown in the
  **final 45 seconds** of the window, dimmed `-` before that
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
.venv/bin/python main.py               # tape + REPL (5m market)
.venv/bin/python main.py --window 15   # the 15m market instead
.venv/bin/python main.py --watch       # tape only, Ctrl-C to quit
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
