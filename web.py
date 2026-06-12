"""Localhost web UI: live numbers + click trading, served by aiohttp.

GET  /            single-page UI
WS   /ws          state snapshots pushed every 250ms
POST /api/order   {outcome: "up"|"down", side: "BUY"|"SELL", price: float|"m", qty}
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

    relevant = {
        view.token_up, view.token_down,
        view.prev_token_up, view.prev_token_down,
    }
    relevant.discard("")
    current = {view.token_up, view.token_down}

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
                "window": "current" if str(p.get("asset", "")) in current else "previous",
            }
            for p in view.positions
            if float(p.get("size", 0) or 0) != 0
            and str(p.get("asset", "")) in relevant
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
            # a 0.0 quote means an empty book side, not a tradable price
            if not price or price <= 0:
                return web.json_response({"error": "no quote for market order"}, status=400)
        try:
            price = round(float(price), 2)
            qty = float(body.get("qty"))
        except (TypeError, ValueError):
            return web.json_response({"error": "bad price/qty"}, status=400)
        # strict range check (also rejects NaN) — never silently clamp money
        if not (0.01 <= price <= 0.99):
            return web.json_response({"error": "price out of range (0.01-0.99)"}, status=400)
        if not (qty > 0):
            return web.json_response({"error": "qty must be > 0"}, status=400)
        label = "UP" if outcome == "up" else "DOWN"
        try:
            order_id = await trader.place(token_id, side, price, qty, None)
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
          padding:4px 6px; font:inherit; width:56px; }
  .chip { display:inline-block; padding:2px 9px; margin:2px 4px 2px 0; cursor:pointer;
          border:1px solid #3a4450; border-radius:12px; font-size:12px; color:var(--fg); }
  .chip:hover { border-color:#5c6773; }
  .chip.sel, input.sel { background:#2e5c3a; border-color:#3fb950; }
  #tape { max-height:200px; overflow-y:auto; font-size:12px; }
  #fills div { padding:1px 0; }
  #toast { position:fixed; bottom:16px; right:16px; background:#2a323c; padding:10px 16px;
           border-radius:8px; display:none; max-width:420px; }
</style></head><body>

<div class="row" id="topcards">
  <div class="card"><div class="lbl">window <span id="slug" class="dim"></span></div>
    <div class="big" id="left">-</div><div id="leftbar"><div id="leftfill"></div></div></div>
  <div class="card"><div class="lbl">Polymarket — settlement</div>
    <div class="big" id="pm_d">-</div>
    <div class="dim">age <span id="age_pm">-</span> · beat <span id="beat">-</span></div></div>
  <div class="card"><div class="lbl">Coinbase</div>
    <div class="big" id="cb_d">-</div>
    <div class="dim">age <span id="age_cb">-</span></div></div>
  <div class="card"><div class="lbl">Binance</div>
    <div class="big" id="bn_d">-</div>
    <div class="dim">age <span id="age_bn">-</span></div></div>
  <div class="card"><div class="lbl">edge (last 45s)</div>
    <div class="big" id="edge">-</div></div>
</div>

<div class="row" style="margin-top:12px">
  <div class="card" style="flex:1">
    <div class="lbl green">UP</div>
    <div class="big"><span id="up_bid">-</span> / <span id="up_ask">-</span></div>
    <div style="margin-top:6px"><span class="dim">shares</span> <span id="qty_up"></span></div>
    <div style="margin-top:8px">
      <button class="buy" onclick="order('up','BUY')">Buy UP</button>
      <button class="sell" onclick="order('up','SELL')">Sell UP</button>
    </div>
  </div>
  <div class="card" style="flex:1">
    <div class="lbl red">DOWN</div>
    <div class="big"><span id="dn_bid">-</span> / <span id="dn_ask">-</span></div>
    <div style="margin-top:6px"><span class="dim">shares</span> <span id="qty_dn"></span></div>
    <div style="margin-top:8px">
      <button class="buy" onclick="order('down','BUY')">Buy DOWN</button>
      <button class="sell" onclick="order('down','SELL')">Sell DOWN</button>
    </div>
  </div>
</div>

<div class="row" style="margin-top:12px">
  <div class="card" style="flex:1">
    <div class="lbl green">UP — at offset from current price</div>
    <div style="margin-top:6px"><span id="off_up_chips"></span>
      <input id="off_up_custom" placeholder="+9/-9"></div>
    <div class="dim" id="off_up_info" style="margin-top:6px"></div>
    <div style="margin-top:8px">
      <button class="buy" id="off_up_buy" onclick="orderOffset('up','BUY')">Buy</button>
      <button class="sell" id="off_up_sell" onclick="orderOffset('up','SELL')">Sell</button>
    </div>
  </div>
  <div class="card" style="flex:1">
    <div class="lbl red">DOWN — at offset from current price</div>
    <div style="margin-top:6px"><span id="off_dn_chips"></span>
      <input id="off_dn_custom" placeholder="+9/-9"></div>
    <div class="dim" id="off_dn_info" style="margin-top:6px"></div>
    <div style="margin-top:8px">
      <button class="buy" id="off_dn_buy" onclick="orderOffset('down','BUY')">Buy</button>
      <button class="sell" id="off_dn_sell" onclick="orderOffset('down','SELL')">Sell</button>
    </div>
  </div>
  <div class="card">
    <div class="lbl">controls</div>
    <div style="margin-top:8px"><button onclick="cancelAll()">Cancel all</button></div>
    <div class="dim" id="trading_state" style="margin-top:8px"></div>
  </div>
</div>

<div class="row" style="margin-top:12px">
  <div class="card" style="flex:1"><div class="lbl">open orders</div>
    <table id="orders"></table></div>
  <div class="card" style="flex:1"><div class="lbl">positions (current + previous market)</div>
    <table id="positions"></table></div>
  <div class="card" style="flex:1"><div class="lbl">fills / events</div>
    <div id="fills"></div></div>
</div>

<div class="card" style="margin-top:12px"><div class="lbl">tape</div><div id="tape"></div></div>
<div id="toast"></div>

<script>
const $ = id => document.getElementById(id);
let S = null, lastTapeKey = "";

// ---- share size (shared by every button) ----
// chips select on pointerdown (delegated on the container) so the 250ms
// state refresh can never swallow a click; chip HTML is only rebuilt when
// the selection itself changes, never per-frame
const QTY_OPTS = [1, 5, 10, 25, 50];
let QTY = 10;
function setQty(v) {
  v = parseFloat(v);
  if (!v || v <= 0) return;
  QTY = v;
  renderQtyChips();
  renderOffsetInfo();
}
function renderQtyChips() {
  for (const box of ["qty_up", "qty_dn"]) {
    $(box).innerHTML = QTY_OPTS.map(n =>
      `<span class="chip ${n===QTY?'sel':''}" data-v="${n}">${n}</span>`).join("") +
      `<input style="width:48px" placeholder="#" title="custom share count"
        class="${QTY_OPTS.includes(QTY)?'':'sel'}"
        onchange="setQty(this.value)" ${QTY_OPTS.includes(QTY)?'':`value="${QTY}"`}>`;
  }
}
$("qty_up").addEventListener("pointerdown", e => e.target.dataset.v && setQty(e.target.dataset.v));
$("qty_dn").addEventListener("pointerdown", e => e.target.dataset.v && setQty(e.target.dataset.v));

// ---- price offsets (cents from current best price) ----
const OFF_OPTS = [-5, -3, -1, 1, 3];
let OFF = { up: -1, down: -1 };
function setOff(side, v) {
  v = parseInt(v);
  if (isNaN(v)) return;
  OFF[side] = v;
  const custom = $(`off_${side === "up" ? "up" : "dn"}_custom`);
  const isCustom = !OFF_OPTS.includes(v);
  custom.value = isCustom ? (v > 0 ? "+" : "") + v : "";
  custom.classList.toggle("sel", isCustom);  // highlight when active
  renderOffsetChips();
  renderOffsetInfo();
}
function offPrice(side, action) {
  if (!S) return null;
  const base = action === "BUY"
    ? (side === "up" ? S.up_ask : S.dn_ask)
    : (side === "up" ? S.up_bid : S.dn_bid);
  if (base == null || base <= 0) return null;
  let px = Math.round(base * 100 + OFF[side]) / 100;
  return Math.min(Math.max(px, 0.01), 0.99);
}
function renderOffsetChips() {
  for (const side of ["up", "down"]) {
    const key = side === "up" ? "up" : "dn";
    $(`off_${key}_chips`).innerHTML = OFF_OPTS.map(n =>
      `<span class="chip ${n===OFF[side]?'sel':''}" data-v="${n}">${n>0?"+"+n:n}</span>`).join("");
  }
}
// per-frame updates touch text only — chips and inputs are left alone
function renderOffsetInfo() {
  for (const side of ["up", "down"]) {
    const key = side === "up" ? "up" : "dn";
    const bpx = offPrice(side, "BUY"), spx = offPrice(side, "SELL");
    $(`off_${key}_buy`).textContent = bpx == null ? "Buy -" : `Buy @ ${bpx.toFixed(2)}`;
    $(`off_${key}_sell`).textContent = spx == null ? "Sell -" : `Sell @ ${spx.toFixed(2)}`;
    const offtxt = (OFF[side] > 0 ? "+" : "") + OFF[side];
    let info;
    if (bpx == null && spx == null) info = "no quote yet";
    else if (bpx == null) info = `${offtxt}¢ → no ask, sell only`;
    else info = `${offtxt}¢ → buy ${QTY} for $${(QTY*bpx).toFixed(2)}, wins $${QTY.toFixed(2)}`;
    $(`off_${key}_info`).textContent = info;
  }
}
$("off_up_chips").addEventListener("pointerdown", e => e.target.dataset.v && setOff("up", e.target.dataset.v));
$("off_dn_chips").addEventListener("pointerdown", e => e.target.dataset.v && setOff("down", e.target.dataset.v));
$("off_up_custom").addEventListener("change", e => setOff("up", e.target.value.replace("+","")));
$("off_dn_custom").addEventListener("change", e => setOff("down", e.target.value.replace("+","")));

function fmt(v, d=1) { return v == null ? "-" : v.toFixed(d); }
function signed(el, v, extra="") {
  if (v == null) { el.textContent = "-"; el.className = (extra + "dim").trim(); return; }
  el.textContent = (v >= 0 ? "+" : "") + v.toFixed(1);
  el.className = (extra + (v >= 0 ? "green" : "red")).trim();
}
function fmtd(v) { return v == null ? "Δ-" : `Δ${v>=0?"+":""}${v.toFixed(1)}`; }
function agetxt(v) { return v == null ? "-" : v.toFixed(1) + "s"; }
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
  signed($("pm_d"), s.pm_d, "big ");
  signed($("cb_d"), s.cb_d, "big ");
  signed($("bn_d"), s.bn_d, "big ");
  $("beat").textContent = s.beat == null ? "-" : s.beat.toFixed(2) + (s.official ? "" : "~");
  $("age_pm").textContent = agetxt(s.ages.pm);
  $("age_cb").textContent = agetxt(s.ages.cb);
  $("age_bn").textContent = agetxt(s.ages.bn);
  if (s.edge_active) { signed($("edge"), s.edge, "big "); }
  else { $("edge").textContent="-"; $("edge").className="big dim"; }
  $("up_bid").textContent = fmt(s.up_bid, 2); $("up_ask").textContent = fmt(s.up_ask, 2);
  $("dn_bid").textContent = fmt(s.dn_bid, 2); $("dn_ask").textContent = fmt(s.dn_ask, 2);
  $("trading_state").textContent = s.trading ? "trading ON" : "display only";
  renderOffsetInfo();

  $("positions").innerHTML = "<tr><th>market</th><th>side</th><th>size</th><th>avg</th><th>cur</th><th>pnl</th></tr>" +
    s.positions.map(p => `<tr><td>${p.title} ${p.window=="previous"?'<span class="dim">(prev)</span>':""}</td>
      <td class="${p.outcome.toLowerCase().startsWith('u')?'green':'red'}">${p.outcome}</td>
      <td>${p.size}</td><td>${p.avg.toFixed(3)}</td><td>${p.cur.toFixed(3)}</td>
      <td class="${p.pnl>=0?'green':'red'}">${(p.pnl>=0?"+":"")+p.pnl.toFixed(2)}</td></tr>`).join("");

  $("fills").innerHTML = s.fills.slice().reverse().map(f =>
    `<div><span class="dim">${f.ts}</span> ${f.kind} ${f.side}
     <span class="${f.label=='UP'?'green':'red'}">${f.label}</span>
     ${f.size} @ ${f.price.toFixed(3)} ${f.status||""}</div>`).join("");

  const key = [s.cb, s.bn, s.pm, s.up_bid, s.up_ask, s.dn_bid, s.dn_ask].join(",");
  if (key !== lastTapeKey) {
    lastTapeKey = key;
    const line = `${s.clock} left ${s.left}s | CB ${fmtd(s.cb_d)} | BN ${fmtd(s.bn_d)}` +
      ` | PM ${fmtd(s.pm_d)} | UP ${fmt(s.up_bid,2)}/${fmt(s.up_ask,2)} DN ${fmt(s.dn_bid,2)}/${fmt(s.dn_ask,2)}` +
      (s.edge_active ? ` | edge ${fmtd(s.edge)}` : "");
    const t = $("tape");
    const atBottom = t.scrollTop + t.clientHeight >= t.scrollHeight - 5;
    t.insertAdjacentHTML("beforeend", `<div>${line}</div>`);
    while (t.children.length > 300) t.removeChild(t.firstChild);
    if (atBottom) t.scrollTop = t.scrollHeight;
  }
}

async function send(outcome, side, price) {
  const label = outcome.toUpperCase();
  if (!confirm(`${side} ${QTY} ${label} @ ${price === "m" ? "market" : price.toFixed(2)}?`)) return;
  let d;
  try {
    const r = await fetch("/api/order", {method:"POST", headers:{"Content-Type":"application/json"},
      body: JSON.stringify({outcome, side, price, qty: QTY})});
    d = await r.json();
  } catch (e) {
    toast("network error — order state UNKNOWN, check open orders: " + e, true);
    loadOrders();
    return;
  }
  if (d.error) toast("order failed: " + d.error, true);
  else { toast(`order accepted @ ${d.price}: ${d.order_id.slice(0,18)}…`); loadOrders(); }
}
function order(outcome, side) {
  if (!S || !S.trading) { toast("trading not enabled (no credentials)", true); return; }
  send(outcome, side, "m");
}
function orderOffset(outcome, side) {
  if (!S || !S.trading) { toast("trading not enabled (no credentials)", true); return; }
  const px = offPrice(outcome, side);
  if (px == null) { toast("no quote yet", true); return; }
  send(outcome, side, px);
}

async function cancelAll() {
  let d;
  try {
    const r = await fetch("/api/cancel", {method:"POST", headers:{"Content-Type":"application/json"},
      body: JSON.stringify({order_id:"all"})});
    d = await r.json();
  } catch (e) { toast("network error — cancel state unknown: " + e, true); return; }
  toast(d.error ? "cancel failed: " + d.error : "cancelled all", !!d.error);
  loadOrders();
}
async function cancelOne(id) {
  try {
    await fetch("/api/cancel", {method:"POST", headers:{"Content-Type":"application/json"},
      body: JSON.stringify({order_id:id})});
  } catch (e) { toast("network error — cancel state unknown: " + e, true); }
  loadOrders();
}
async function loadOrders() {
  let d;
  try {
    const r = await fetch("/api/orders"); d = await r.json();
  } catch (e) { return; } // transient; retried every 10s
  if (!Array.isArray(d)) return;
  $("orders").innerHTML = "<tr><th>side</th><th>token</th><th>size</th><th>px</th><th>filled</th><th></th></tr>" +
    d.map(o => `<tr><td>${o.side}</td><td class="${o.label=='UP'?'green':'red'}">${o.label}</td>
      <td>${o.size}</td><td>${o.price.toFixed(2)}</td><td>${o.matched}</td>
      <td><button onclick="cancelOne('${o.id}')">x</button></td></tr>`).join("");
}

function connect() {
  const ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = e => render(JSON.parse(e.data));
  ws.onclose = () => setTimeout(connect, 1000);
}
renderQtyChips();
renderOffsetChips();
renderOffsetInfo();
connect();
loadOrders();
setInterval(loadOrders, 10000);
</script>
</body></html>
"""
