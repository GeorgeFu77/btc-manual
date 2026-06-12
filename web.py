"""Localhost web UI: live numbers + click trading, served by aiohttp.

GET  /            single-page UI
WS   /ws          state snapshots pushed every 250ms
POST /api/order   {outcome: "up"|"down", side: "BUY"|"SELL", price: float|"m", qty, ttl?}
POST /api/cancel  {order_id: "..." | "all"}
GET  /api/orders  open orders
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from aiohttp import WSMsgType, web

from market import MarketView
from trader import Trader

logger = logging.getLogger(__name__)


def _state(view: MarketView, trading: bool) -> dict:
    def age(ts):
        return round(time.monotonic() - ts, 1) if ts else None

    return {
        "clock": time.strftime("%H:%M:%S"),
        "left": round(view.seconds_left()),
        "bucket": view.bucket_sec,
        "slug": view.slug,
        "cb": view.cb_last, "cb_d": view.cb_delta,
        "bn": view.bn_last, "bn_d": view.bn_delta,
        "pm": view.pm_last, "pm_d": view.pm_delta,
        "beat": view.pm_official if view.pm_official is not None else view.pm_ptb,
        "official": view.pm_official is not None,
        "edge": view.edge if view.edge_active else None,
        "edge_active": view.edge_active,
        "up_bid": view.up.bid, "up_ask": view.up.ask,
        "dn_bid": view.dn.bid, "dn_ask": view.dn.ask,
        "ages": {
            "cb": age(view.cb_ts_mono), "bn": age(view.bn_ts_mono),
            "pm": age(view.pm_ts_mono),
            "up": age(view.up.ts_mono), "dn": age(view.dn.ts_mono),
        },
        "fills": view.fills[-15:],
        "positions": [
            {
                "title": p.get("title") or p.get("slug") or "?",
                "outcome": p.get("outcome") or "?",
                "size": float(p.get("size", 0) or 0),
                "avg": float(p.get("avgPrice", 0) or 0),
                "cur": float(p.get("curPrice", 0) or 0),
                "pnl": float(p.get("cashPnl", 0) or 0),
            }
            for p in view.positions
            if float(p.get("size", 0) or 0) != 0
        ],
        "trading": trading,
    }


def make_app(view: MarketView, trader: Trader | None, stop: asyncio.Event) -> web.Application:
    app = web.Application()

    async def index(_req):
        return web.Response(text=PAGE, content_type="text/html")

    async def ws_handler(req):
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(req)
        try:
            while not stop.is_set() and not ws.closed:
                await ws.send_json(_state(view, trader is not None))
                # also drain any client messages so pings don't pile up
                try:
                    msg = await ws.receive(timeout=0.25)
                    if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                        break
                except asyncio.TimeoutError:
                    pass
        finally:
            await ws.close()
        return ws

    async def api_order(req):
        if trader is None:
            return web.json_response({"error": "trading disabled (no credentials)"}, status=400)
        body = await req.json()
        outcome = body.get("outcome")
        side = body.get("side")
        if outcome not in ("up", "down") or side not in ("BUY", "SELL"):
            return web.json_response({"error": "bad outcome/side"}, status=400)
        token_id = view.token_up if outcome == "up" else view.token_down
        if not token_id:
            return web.json_response({"error": "no active window yet"}, status=400)
        quote = view.up if outcome == "up" else view.dn
        price = body.get("price")
        if price == "m":
            price = quote.ask if side == "BUY" else quote.bid
            if price is None:
                return web.json_response({"error": "no quote for market order"}, status=400)
            price = min(max(float(price), 0.01), 0.99)
        try:
            price = round(float(price), 2)
            qty = float(body.get("qty"))
        except (TypeError, ValueError):
            return web.json_response({"error": "bad price/qty"}, status=400)
        if not 0.0 < price < 1.0 or qty <= 0:
            return web.json_response({"error": "price must be 0-1, qty > 0"}, status=400)
        ttl = body.get("ttl")
        ttl = int(ttl) if ttl else None
        label = "UP" if outcome == "up" else "DOWN"
        try:
            order_id = await trader.place(token_id, side, price, qty, ttl)
        except Exception as e:
            logger.warning("web order failed: %s", e)
            return web.json_response({"error": str(e)}, status=500)
        view.add_fill({
            "ts": time.strftime("%H:%M:%S"), "kind": "SENT",
            "side": side, "label": label, "size": qty, "price": price,
        })
        return web.json_response({"order_id": order_id, "price": price})

    async def api_cancel(req):
        if trader is None:
            return web.json_response({"error": "trading disabled"}, status=400)
        body = await req.json()
        oid = body.get("order_id")
        try:
            if oid == "all":
                await trader.cancel_all()
            else:
                await trader.cancel(str(oid))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
        return web.json_response({"ok": True})

    async def api_orders(_req):
        if trader is None:
            return web.json_response([])
        try:
            orders = await trader.open_orders()
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
        return web.json_response([
            {
                "id": str(o.get("id") or o.get("orderID") or "?"),
                "label": view.token_label(str(o.get("asset_id", ""))),
                "side": o.get("side", "?"),
                "price": float(o.get("price", 0) or 0),
                "size": float(o.get("original_size", 0) or 0),
                "matched": float(o.get("size_matched", 0) or 0),
            }
            for o in orders
        ])

    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/api/order", api_order)
    app.router.add_post("/api/cancel", api_cancel)
    app.router.add_get("/api/orders", api_orders)
    return app


async def run_web(
    view: MarketView, trader: Trader | None, stop: asyncio.Event,
    host: str = "127.0.0.1", port: int = 8080,
) -> None:
    runner = web.AppRunner(make_app(view, trader, stop))
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"open http://{host}:{port} in Chrome"
          + ("" if trader else "   (display only — no credentials)"))
    try:
        await stop.wait()
    finally:
        await runner.cleanup()


PAGE = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>btc manual</title>
<style>
  :root { --bg:#15191f; --fg:#d6dde6; --dim:#5c6773; --green:#3fb950; --red:#f85149;
          --card:#1d232b; --line:#2a323c; }
  * { box-sizing:border-box; }
  body { background:var(--bg); color:var(--fg); font:14px/1.45 "SF Mono",Menlo,monospace;
         margin:0; padding:16px; }
  .row { display:flex; gap:12px; flex-wrap:wrap; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:8px;
          padding:10px 14px; }
  .big { font-size:26px; font-weight:600; }
  .lbl { color:var(--dim); font-size:11px; text-transform:uppercase; letter-spacing:.08em; }
  .green { color:var(--green); } .red { color:var(--red); } .dim { color:var(--dim); }
  #leftbar { height:4px; background:var(--line); border-radius:2px; margin-top:6px; }
  #leftfill { height:100%; border-radius:2px; background:var(--green); width:100%; }
  table { border-collapse:collapse; width:100%; }
  td, th { padding:2px 8px 2px 0; text-align:left; white-space:nowrap; }
  th { color:var(--dim); font-weight:normal; font-size:11px; }
  button { background:#2a323c; color:var(--fg); border:1px solid #3a4450; border-radius:6px;
           padding:6px 12px; font:inherit; cursor:pointer; }
  button:hover { border-color:#5c6773; }
  button.buy { background:#1d3325; border-color:#2e5c3a; }
  button.sell { background:#36201f; border-color:#6b2f2c; }
  input { background:#11151a; color:var(--fg); border:1px solid #3a4450; border-radius:6px;
          padding:6px 8px; font:inherit; width:80px; }
  #tape { max-height:200px; overflow-y:auto; font-size:12px; }
  #fills div { padding:1px 0; }
  .clickpx { cursor:pointer; text-decoration:underline dotted; }
  #toast { position:fixed; bottom:16px; right:16px; background:#2a323c; padding:10px 16px;
           border-radius:8px; display:none; max-width:420px; }
</style></head><body>

<div class="row" id="topcards">
  <div class="card"><div class="lbl">window <span id="slug" class="dim"></span></div>
    <div class="big" id="left">-</div><div id="leftbar"><div id="leftfill"></div></div></div>
  <div class="card"><div class="lbl">Chainlink (PM) — settlement</div>
    <div class="big" id="pm">-</div><div>Δ <span id="pm_d">-</span>
    <span class="dim">beat</span> <span id="beat" class="dim">-</span></div></div>
  <div class="card"><div class="lbl">Coinbase</div>
    <div class="big" id="cb">-</div><div>Δ <span id="cb_d">-</span></div></div>
  <div class="card"><div class="lbl">Binance</div>
    <div class="big" id="bn">-</div><div>Δ <span id="bn_d">-</span></div></div>
  <div class="card"><div class="lbl">edge (last 45s)</div>
    <div class="big" id="edge">-</div></div>
</div>

<div class="row" style="margin-top:12px">
  <div class="card" style="flex:1">
    <div class="lbl green">UP</div>
    <div class="big"><span class="clickpx" id="up_bid">-</span> /
      <span class="clickpx" id="up_ask">-</span></div>
    <div style="margin-top:8px">
      <button class="buy" onclick="order('up','BUY')">Buy UP</button>
      <button class="sell" onclick="order('up','SELL')">Sell UP</button>
    </div>
  </div>
  <div class="card" style="flex:1">
    <div class="lbl red">DOWN</div>
    <div class="big"><span class="clickpx" id="dn_bid">-</span> /
      <span class="clickpx" id="dn_ask">-</span></div>
    <div style="margin-top:8px">
      <button class="buy" onclick="order('down','BUY')">Buy DOWN</button>
      <button class="sell" onclick="order('down','SELL')">Sell DOWN</button>
    </div>
  </div>
  <div class="card">
    <div class="lbl">order ticket</div>
    <div style="margin-top:6px">
      price <input id="price" placeholder="m = market">
      qty <input id="qty" value="10">
      ttl <input id="ttl" placeholder="sec (opt)">
    </div>
    <div class="dim" style="margin-top:6px">price blank or “m” = take best bid/ask</div>
    <div id="cost" style="margin-top:6px"></div>
    <div style="margin-top:8px"><button onclick="cancelAll()">Cancel all</button>
      <span id="trading_state" class="dim"></span></div>
  </div>
</div>

<div class="row" style="margin-top:12px">
  <div class="card" style="flex:1"><div class="lbl">open orders</div>
    <table id="orders"></table></div>
  <div class="card" style="flex:1"><div class="lbl">positions</div>
    <table id="positions"></table></div>
  <div class="card" style="flex:1"><div class="lbl">fills / events</div>
    <div id="fills"></div></div>
</div>

<div class="card" style="margin-top:12px"><div class="lbl">tape</div><div id="tape"></div></div>
<div class="dim" style="margin-top:8px">ages: <span id="ages"></span></div>
<div id="toast"></div>

<script>
const $ = id => document.getElementById(id);
let S = null, lastTapeKey = "";

function fmt(v, d=1) { return v == null ? "-" : v.toFixed(d); }
function signed(el, v) {
  if (v == null) { el.textContent = "-"; el.className = "dim"; return; }
  el.textContent = (v >= 0 ? "+" : "") + v.toFixed(1);
  el.className = v >= 0 ? "green" : "red";
}
function toast(msg, bad) {
  const t = $("toast");
  t.textContent = msg; t.style.display = "block";
  t.style.borderLeft = "4px solid " + (bad ? "#f85149" : "#3fb950");
  clearTimeout(t._h); t._h = setTimeout(() => t.style.display = "none", 5000);
}

function render(s) {
  S = s;
  $("slug").textContent = s.slug;
  $("left").textContent = s.left + "s";
  $("leftfill").style.width = (100 * s.left / s.bucket) + "%";
  $("leftfill").style.background = s.left <= 45 ? "var(--red)" : "var(--green)";
  $("pm").textContent = fmt(s.pm); signed($("pm_d"), s.pm_d);
  $("beat").textContent = s.beat == null ? "-" : s.beat.toFixed(2) + (s.official ? "" : "~");
  $("cb").textContent = fmt(s.cb); signed($("cb_d"), s.cb_d);
  $("bn").textContent = fmt(s.bn); signed($("bn_d"), s.bn_d);
  if (s.edge_active) { signed($("edge"), s.edge); } else { $("edge").textContent="-"; $("edge").className="dim"; }
  $("up_bid").textContent = fmt(s.up_bid, 2); $("up_ask").textContent = fmt(s.up_ask, 2);
  $("dn_bid").textContent = fmt(s.dn_bid, 2); $("dn_ask").textContent = fmt(s.dn_ask, 2);
  $("ages").textContent = Object.entries(s.ages).map(([k,v]) => k + (v==null?"-":v+"s")).join("  ");
  $("trading_state").textContent = s.trading ? "trading ON" : "display only";

  $("positions").innerHTML = "<tr><th>market</th><th>side</th><th>size</th><th>avg</th><th>cur</th><th>pnl</th></tr>" +
    s.positions.map(p => `<tr><td>${p.title}</td><td>${p.outcome}</td><td>${p.size}</td>
      <td>${p.avg.toFixed(3)}</td><td>${p.cur.toFixed(3)}</td>
      <td class="${p.pnl>=0?'green':'red'}">${(p.pnl>=0?"+":"")+p.pnl.toFixed(2)}</td></tr>`).join("");

  $("fills").innerHTML = s.fills.slice().reverse().map(f =>
    `<div><span class="dim">${f.ts}</span> ${f.kind} ${f.side}
     <span class="${f.label=='UP'?'green':'red'}">${f.label}</span>
     ${f.size} @ ${f.price.toFixed(3)} ${f.status||""}</div>`).join("");

  renderCost();
  const key = [s.cb, s.bn, s.pm, s.up_bid, s.up_ask, s.dn_bid, s.dn_ask].join(",");
  if (key !== lastTapeKey) {
    lastTapeKey = key;
    const line = `${s.clock} left ${s.left}s | CB ${fmt(s.cb)} ${fmtd(s.cb_d)} | BN ${fmt(s.bn)} ${fmtd(s.bn_d)}` +
      ` | PM ${fmt(s.pm)} ${fmtd(s.pm_d)} | UP ${fmt(s.up_bid,2)}/${fmt(s.up_ask,2)} DN ${fmt(s.dn_bid,2)}/${fmt(s.dn_ask,2)}` +
      (s.edge_active ? ` | edge ${fmtd(s.edge)}` : "");
    const t = $("tape");
    const atBottom = t.scrollTop + t.clientHeight >= t.scrollHeight - 5;
    t.insertAdjacentHTML("beforeend", `<div>${line}</div>`);
    while (t.children.length > 300) t.removeChild(t.firstChild);
    if (atBottom) t.scrollTop = t.scrollHeight;
  }
}
function fmtd(v) { return v == null ? "Δ-" : `Δ${v>=0?"+":""}${v.toFixed(1)}`; }

function renderCost() {
  if (!S) return;
  const qty = parseFloat($("qty").value) || 0;
  const manual = parseFloat($("price").value);
  const upPx = isNaN(manual) ? S.up_ask : manual;
  const dnPx = isNaN(manual) ? S.dn_ask : manual;
  const part = (n, px) => px == null ? "-" :
    `$${(n*px).toFixed(2)} → wins $${n.toFixed(2)}`;
  $("cost").innerHTML =
    `buying ${qty} shares: <span class="green">UP ${part(qty, upPx)}</span><br>` +
    `&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <span class="red">DN ${part(qty, dnPx)}</span>`;
}
$("qty").addEventListener("input", renderCost);
$("price").addEventListener("input", renderCost);

async function order(outcome, side) {
  if (!S || !S.trading) { toast("trading not enabled (no credentials)", true); return; }
  const price = $("price").value.trim() || "m";
  const qty = $("qty").value.trim();
  const ttl = $("ttl").value.trim();
  const px = price === "m" ? (side === "BUY" ? (outcome=="up"?S.up_ask:S.dn_ask) : (outcome=="up"?S.up_bid:S.dn_bid)) : price;
  if (!confirm(`${side} ${qty} ${outcome.toUpperCase()} @ ${px}${ttl?` (ttl ${ttl}s)`:""}?`)) return;
  const r = await fetch("/api/order", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({outcome, side, price: price==="m"?"m":parseFloat(price), qty: parseFloat(qty), ttl: ttl||null})});
  const d = await r.json();
  if (d.error) toast("order failed: " + d.error, true);
  else { toast(`order accepted @ ${d.price}: ${d.order_id.slice(0,18)}…`); loadOrders(); }
}

async function cancelAll() {
  const r = await fetch("/api/cancel", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({order_id:"all"})});
  const d = await r.json();
  toast(d.error ? "cancel failed: " + d.error : "cancelled all", !!d.error);
  loadOrders();
}
async function cancelOne(id) {
  await fetch("/api/cancel", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({order_id:id})});
  loadOrders();
}
async function loadOrders() {
  const r = await fetch("/api/orders"); const d = await r.json();
  if (!Array.isArray(d)) return;
  $("orders").innerHTML = "<tr><th>side</th><th>token</th><th>size</th><th>px</th><th>filled</th><th></th></tr>" +
    d.map(o => `<tr><td>${o.side}</td><td class="${o.label=='UP'?'green':'red'}">${o.label}</td>
      <td>${o.size}</td><td>${o.price.toFixed(2)}</td><td>${o.matched}</td>
      <td><button onclick="cancelOne('${o.id}')">x</button></td></tr>`).join("");
}

// click a quoted price to prefill the ticket
for (const id of ["up_bid","up_ask","dn_bid","dn_ask"])
  $(id).onclick = () => { $("price").value = $(id).textContent; };

function connect() {
  const ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = e => render(JSON.parse(e.data));
  ws.onclose = () => setTimeout(connect, 1000);
}
connect();
loadOrders();
setInterval(loadOrders, 10000);
</script>
</body></html>
"""
