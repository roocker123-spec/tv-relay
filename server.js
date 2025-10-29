// server.js
require('dotenv').config();
const express = require('express');
const crypto  = require('crypto');

// Node 18+ has global fetch
const fetch = global.fetch;

const app = express();

/* ---------------------- parsing & normalization ---------------------- */
app.use(express.json({ type: '*/*' }));
app.use(express.urlencoded({ extended: true }));

// Accept raw strings / form-encoded TV payloads too
app.use((req, _res, next) => {
  if (typeof req.body === 'string') {
    try { req.body = JSON.parse(req.body); }
    catch {
      const qs = require('querystring');
      req.body = qs.parse(req.body);
    }
  }
  if (req.body && typeof req.body.qty !== 'undefined') {
    const q = parseInt(req.body.qty, 10);
    if (!Number.isNaN(q)) req.body.qty = q;
  }
  next();
});

/* ----------------------------- env ---------------------------------- */
const API_KEY       = process.env.DELTA_API_KEY || process.env.API_KEY || '';
const API_SECRET    = process.env.DELTA_API_SECRET || process.env.API_SECRET || '';
const BASE_URL      = (process.env.DELTA_BASE || process.env.DELTA_BASE_URL || 'https://api.india.delta.exchange').replace(/\/+$/,'');
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || ''; // optional shared secret
const PORT          = process.env.PORT || 3000;

/* ---- Delta expects **seconds** timestamp ---- */
function nowTsSec(){ return Math.floor(Date.now()/1000).toString(); } // seconds

function toProductSymbol(sym){
  if(!sym) return sym;
  let s = String(sym).replace('.P','');           // TV → Exchange (e.g., CAKEUSD.P -> CAKEUSD)
  if(s.includes(':')) s = s.split(':').pop();     // strip BINANCE: prefix if present
  return s;
}

/* ----------------------- Delta request helper ----------------------- */
// Signature prehash: method + timestamp + path + query + body
async function dcall(method, path, payload=null, query='') {
  const body = payload ? JSON.stringify(payload) : '';
  const MAX_TRIES = 3;

  for (let attempt = 1; attempt <= MAX_TRIES; attempt++) {
    const ts   = nowTsSec(); // << seconds
    const prehash = method + ts + path + (query||'') + body;
    const signature = crypto.createHmac('sha256', API_SECRET).update(prehash).digest('hex');
    const url  = BASE_URL + path + (query||'');

    const headers = {
      'Content-Type':'application/json',
      'Accept':'application/json',
      'api-key':API_KEY,
      'signature':signature,
      'timestamp':ts,
      'User-Agent':'tv-relay-node'
    };

    // -------- DEBUG: timestamp logger (remove after debugging) --------
    console.log('delta ts =', ts, '(length:', ts.length + ')');

    try {
      const res  = await fetch(url,{ method, headers, body: body || undefined });
      const text = await res.text();
      let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }

      if (!res.ok || json?.success === false) {
        const code = json?.error?.code || res.status;
        if ([429,500,502,503,504].includes(code) && attempt < MAX_TRIES) {
          await new Promise(r=>setTimeout(r, 300*attempt));
          continue;
        }
        throw new Error(`Delta API error: ${JSON.stringify({ url, status: res.status, json })}`);
      }
      return json;
    } catch (e) {
      if (attempt === MAX_TRIES) throw e;
      await new Promise(r=>setTimeout(r, 300*attempt));
    }
  }
}

/* -------------------------- order helpers --------------------------- */
async function placeEntry(m){
  const side = (m.side||'').toLowerCase()==='buy' ? 'buy' : 'sell';
  const qty  = parseInt(m.qty,10);
  if(!qty || qty < 1) throw new Error('qty must be integer >= 1');
  return dcall('POST','/v2/orders',{
    product_symbol: toProductSymbol(m.symbol || m.product_symbol),
    order_type:'market_order',
    side,
    size: qty
  });
}

async function placeBracket(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = toProductSymbol(m.product_symbol || m.symbol);
  return dcall('POST','/v2/orders/bracket', body);
}

async function placeBatch(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = toProductSymbol(m.product_symbol || m.symbol);
  return dcall('POST','/v2/orders/batch', body);
}

/* --------------- list / cancel (regular + stop orders) -------------- */
const cancelAllOrders   = () => dcall('DELETE','/v2/orders/all');                 // regular
const closeAllPositions = () => dcall('POST','/v2/positions/close_all', {});      // flatten

async function listOpenOrdersAllPages(){
  let all = [];
  let page = 1;
  while (true){
    const q = `?states=open,pending&page=${page}&per_page=200`;
    const oo = await dcall('GET','/v2/orders', null, q);
    const arr = Array.isArray(oo?.result) ? oo.result
              : Array.isArray(oo?.orders) ? oo.orders
              : Array.isArray(oo) ? oo : [];
    all = all.concat(arr);
    if (arr.length < 200) break;
    page++;
  }
  return all;
}

async function listPositionsArray(){
  const pos = await dcall('GET','/v2/positions');
  const arr = Array.isArray(pos?.result?.positions) ? pos.result.positions
            : Array.isArray(pos?.result) ? pos.result
            : Array.isArray(pos?.positions) ? pos.positions
            : Array.isArray(pos) ? pos : [];
  return arr;
}

// ---- STOP ORDERS (triggers) ----
async function listOpenStopOrdersAllPages() {
  let all = [];
  let page = 1;
  while (true) {
    const q = `?states=untriggered,triggered&page=${page}&per_page=200`;
    const so = await dcall('GET', '/v2/stop_orders', null, q);
    const arr = Array.isArray(so?.result) ? so.result
              : Array.isArray(so?.stop_orders) ? so.stop_orders
              : Array.isArray(so) ? so : [];
    all = all.concat(arr);
    if (arr.length < 200) break;
    page++;
  }
  return all;
}

// Try DELETE first; if API expects POST, fall back
async function cancelAllStopOrders() {
  try {
    return await dcall('DELETE', '/v2/stop_orders/all');
  } catch (e) {
    return dcall('POST', '/v2/stop_orders/cancel_all', {});
  }
}

// Cancel BOTH regular & stop orders
async function cancelAllOrdersBoth() {
  const out = { regular: null, stops: null };
  try { out.regular = await cancelAllOrders(); } catch (e) { out.regular = { error: String(e.message || e) }; }
  try { out.stops   = await cancelAllStopOrders(); } catch (e) { out.stops   = { error: String(e.message || e) }; }
  return out;
}

/* ----------------------- flat gate (robust) ------------------------- */
async function waitUntilFlat(timeoutMs = Number(process.env.FLAT_TIMEOUT_MS || 15000), pollMs = 400) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const [oo, so, pos] = await Promise.all([
        listOpenOrdersAllPages(),
        listOpenStopOrdersAllPages(),
        listPositionsArray()
      ]);

      const hasOrders = oo.some(o =>
        ['open','pending'].includes(String(o?.state || o?.status || '').toLowerCase())
      );
      const hasStops  = so.some(s =>
        ['untriggered','triggered','pending'].includes(String(s?.state || s?.status || '').toLowerCase())
      );
      const hasPos    = pos.some(p => Math.abs(Number(p?.size || p?.position_size || 0)) > 0);

      if (!hasOrders && !hasStops && !hasPos) return true;

      console.log('…still flattening', {
        openOrders: oo.length,
        stopOrders: so.length,
        positions : pos.map(p => ({ product_id: p.product_id, symbol: p.product_symbol || p.product_id, size: p.size }))
      });
    } catch (e) {
      console.warn('waitUntilFlat poll error (ignored):', e?.message || e);
    }
    await new Promise(r => setTimeout(r, pollMs));
  }
  return false;
}

/* ------------------------------ health ------------------------------ */
app.get('/health', (_req,res)=>res.json({ok:true}));
app.get('/healthz', (_req,res)=>res.send('ok'));

/* --------------------------- AUTH TEST ------------------------------ */
// Harmless, read-only private call to verify API key/secret/base/whitelist/clock.
app.get('/auth-test', async (_req, res) => {
  try {
    const r = await dcall('GET','/v2/positions');
    res.json({ ok: true, sample: r });
  } catch (e) {
    res.status(401).json({ ok: false, error: String(e.message || e) });
  }
});

/* --------------------------- webhook route -------------------------- */
app.post('/tv', async (req, res) => {
  try {
    if (WEBHOOK_TOKEN) {
      const hdr = req.headers['x-webhook-token'];
      if (hdr !== WEBHOOK_TOKEN) return res.status(401).json({ ok:false, error:'unauthorized' });
    }

    const msg    = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body || {});
    const action = String(msg.action || '').toUpperCase();
    console.log('\n=== INCOMING /tv ===\n', JSON.stringify(msg));

    // Manual cleanup from Pine (return 401 if exchange rejects)
    if (action === 'DELTA_CANCEL_ALL' || action === 'CANCEL_ALL') {
      try {
        const out = await cancelAllOrdersBoth();
        return res.json({ ok:true, did:'cancel_all_orders_and_stops', delta: out });
      } catch (e) {
        return res.status(401).json({ ok:false, error:String(e.message||e) });
      }
    }
    if (action === 'CLOSE_POSITION') {
      try {
        const out = await closeAllPositions();
        return res.json({ ok:true, did:'close_all_positions', delta: out });
      } catch (e) {
        return res.status(401).json({ ok:false, error:String(e.message||e) });
      }
    }

    // Direct bracket passthrough (if your Pine sends a bracket payload)
    if (msg.stop_loss_order || msg.take_profit_order) {
      const r = await placeBracket(msg);
      return res.json({ ok:true, step:'bracket', r });
    }

    // Batch limits passthrough (array of orders)
    if (msg.orders) {
      const r = await placeBatch(msg);
      return res.json({ ok:true, step:'batch', r });
    }

    // ENTER / FLIP: clean slate first, then gate until flat, then place entry
    if (action === 'ENTER' || action === 'FLIP') {
      try { await cancelAllOrdersBoth(); } catch(e) { console.warn('cancelAllOrdersBoth failed:', e?.message || e); }
      try { await closeAllPositions();   } catch(e) { console.warn('closeAllPositions failed:', e?.message || e); }

      const flat = await waitUntilFlat();
      console.log('flat gate result:', flat);
      // proceed regardless — but gate prevents most races
    }

    // Plain market entry (expects {symbol or product_symbol, side, qty})
    const r = await placeEntry(msg);
    return res.json({ ok:true, step:'entry', r });

  } catch (e) {
    console.error('✖ ERROR:', e);
    return res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

/* ------------------------------ start ------------------------------- */
app.listen(PORT, ()=>{
  console.log(`Relay listening on http://localhost:${PORT}  (BASE=${BASE_URL})`);
  // Helpful to confirm you're running the right file:
  console.log('Loaded from', require('fs').realpathSync(__filename));
});
