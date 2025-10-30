// server.js
require('dotenv').config();
const express = require('express');
const crypto  = require('crypto');
const fetch   = global.fetch; // Node 18+

// ===================== Utils =====================
function nowTsSec(){ return Math.floor(Date.now()/1000).toString(); }

// Remove venue prefix like "DELTAIN:" and return the right side
function stripVenue(sym) {
  if (!sym) return sym;
  const s = String(sym);
  return s.includes(':') ? s.split(':').pop() : s;
}

// (legacy helper kept for completeness)
function underlyingFromSymbol(sym) {
  const core = stripVenue(sym || '');   // e.g. "BTCUSDT"
  return core.replace(/USDT.*$/,'').replace(/USD.*$/,''); // "BTC"
}

// ============ App bootstrap / parsing ============
const app = express();
process.env.__STARTED_AT = new Date().toISOString();

app.use(express.json({ type: '*/*' }));
app.use(express.urlencoded({ extended: true }));
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

// ===================== Env / Auth =====================
const API_KEY       = process.env.DELTA_API_KEY || '';
const API_SECRET    = process.env.DELTA_API_SECRET || '';
const BASE_URL      = (process.env.DELTA_BASE || process.env.DELTA_BASE_URL || 'https://api.india.delta.exchange').replace(/\/+$/,'');
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || '';
const PORT          = process.env.PORT || 3000;

// Auth mode + customizable header names
const AUTH_MODE   = (process.env.DELTA_AUTH || 'hmac').toLowerCase(); // 'hmac' | 'keyonly'
const HDR_API_KEY = process.env.DELTA_HDR_API_KEY || 'api-key';
const HDR_SIG     = process.env.DELTA_HDR_SIG     || 'signature';
const HDR_TS      = process.env.DELTA_HDR_TS      || 'timestamp';

// ===================== Idempotency =====================
const SEEN = new Map();              // key -> ts(ms)
const SEEN_TTL_MS = 60_000;

function seenKey(msg){
  // Stable key across retries; include sig_id if Pine sends it
  const p = [
    String(msg.action||''),
    String(msg.product_symbol||msg.symbol||''),
    String(msg.side||''),
    String(msg.qty||''),
    String(msg.entry||''),
    String(msg.strategy_id||''),
    String(msg.sig_id||'')
  ].join('|');
  return crypto.createHash('sha1').update(p).digest('hex');
}
function remember(k){
  SEEN.set(k, Date.now());
  // prune
  for (const [kk, ts] of SEEN) {
    if (Date.now()-ts > SEEN_TTL_MS) SEEN.delete(kk);
  }
  // cap size
  if (SEEN.size > 300) {
    for (const kk of SEEN.keys()) { SEEN.delete(kk); if (SEEN.size <= 200) break; }
  }
}

// ===================== Delta client =====================
async function dcall(method, path, payload=null, query='') {
  const body = payload ? JSON.stringify(payload) : '';
  const MAX_TRIES = 3;

  for (let attempt = 1; attempt <= MAX_TRIES; attempt++) {
    const ts   = nowTsSec();
    const url  = BASE_URL + path + (query||'');
    const headers = {
      'Content-Type':'application/json',
      'Accept':'application/json',
      'User-Agent':'tv-relay-node'
    };

    // Auth: HMAC (default) or key-only
    if (AUTH_MODE === 'hmac') {
      const prehash = method + ts + path + (query||'') + body;
      const signature = crypto.createHmac('sha256', API_SECRET).update(prehash).digest('hex');
      headers[HDR_API_KEY] = API_KEY;
      headers[HDR_SIG]     = signature;
      headers[HDR_TS]      = ts;
    } else {
      headers[HDR_API_KEY] = API_KEY;
    }

    try {
      const res  = await fetch(url,{ method, headers, body: body || undefined });
      const text = await res.text(); let json;
      try { json = JSON.parse(text); } catch { json = { raw: text }; }

      if (!res.ok || json?.success === false) {
        const code = Number(json?.error?.code || res.status);
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

// ===================== Order helpers (raw) =====================
// NOTE: these expect a correct product_symbol in the payload.
// The "Smart" wrappers below will provide it.
async function placeEntryRaw(m){
  const side = (m.side||'').toLowerCase()==='buy' ? 'buy' : 'sell';
  const qty  = parseInt(m.qty,10);
  if(!qty || qty < 1) throw new Error('qty must be integer >= 1');
  return dcall('POST','/v2/orders',{
    product_symbol: m.product_symbol || m.symbol,
    order_type:'market_order',
    side, size: qty
  });
}
async function placeBracketRaw(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = m.product_symbol || m.symbol;
  return dcall('POST','/v2/orders/bracket', body);
}
async function placeBatchRaw(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = m.product_symbol || m.symbol;
  return dcall('POST','/v2/orders/batch', body);
}

// ===================== Smart product_symbol resolver =====================
// Tries with and without ".P" while stripping any venue prefix like "DELTAIN:"
async function resolveAnd(fn, msg, which='entry') {
  const raw   = stripVenue(msg.product_symbol || msg.symbol || '');
  if (!raw) throw new Error(`Missing symbol/product_symbol for ${which}`);

  const noDot = raw.endsWith('.P') ? raw.slice(0, -2) : raw;
  const candidates = raw.endsWith('.P') ? [raw, noDot] : [raw, noDot];

  let lastErr;
  for (const c of candidates) {
    const payload = { ...msg, product_symbol: c, symbol: c };
    try {
      const out = await fn(payload);
      console.log(`[OK] ${which} -> ${c}`);
      return out;
    } catch (e) {
      const m = String(e?.message || e);
      const is4xx = /\b(400|404)\b/.test(m);
      const prodHint = /product|symbol/i.test(m);
      if (!(is4xx && prodHint)) throw e; // non-product error: rethrow
      console.warn(`[retry] ${which} failed for ${c}; trying next candidate`, m);
      lastErr = e;
    }
  }
  throw lastErr || new Error(`Unable to resolve product_symbol for ${which}`);
}

async function placeEntrySmart  (m){ return resolveAnd(placeEntryRaw,   m, 'entry');   }
async function placeBracketSmart(m){ return resolveAnd(placeBracketRaw, m, 'bracket'); }
async function placeBatchSmart  (m){ return resolveAnd(placeBatchRaw,   m, 'batch');   }

// ===================== Cancel/Close/Lists =====================
const cancelAllOrders   = () => dcall('DELETE','/v2/orders/all');                 // no body
const closeAllPositions = () => dcall('POST','/v2/positions/close_all', {});       // explicit {}

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

async function waitUntilFlat(timeoutMs = Number(process.env.FLAT_TIMEOUT_MS||15000), pollMs = 400) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const oo  = await listOpenOrdersAllPages();
      const hasOrders = oo.some(o => ['open','pending','triggered','untriggered']
        .includes(String(o?.state||o?.status||'').toLowerCase()));

      const pos = await listPositionsArray();
      const hasPos = pos.some(p => Math.abs(Number(p?.size||p?.position_size||0)) > 0);

      if (!hasOrders && !hasPos) return true;

      console.log('…still flattening', {
        openOrders: oo.length,
        positions: pos.map(p=>({ product_id:p.product_id, size:p.size }))
      });
    } catch(e) {
      console.warn('waitUntilFlat poll error (ignoring):', e?.message || e);
    }
    await new Promise(r => setTimeout(r, pollMs));
  }
  return false;
}

// ===================== Health/debug =====================
app.get('/health', (_req,res)=>res.json({ok:true, started_at:process.env.__STARTED_AT, base:BASE_URL, auth:AUTH_MODE}));
app.get('/healthz', (_req,res)=>res.send('ok'));
app.get('/debug/seen', (_req,res)=>res.json({ size: SEEN.size }));

// ===================== TradingView webhook =====================
app.post('/tv', async (req, res) => {
  try {
    if (WEBHOOK_TOKEN) {
      const hdr = req.headers['x-webhook-token'];
      if (hdr !== WEBHOOK_TOKEN) return res.status(401).json({ ok:false, error:'unauthorized' });
    }

    const msg    = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body || {});
    const action = String(msg.action || '').toUpperCase();
    console.log('\n=== INCOMING /tv ===\n', JSON.stringify(msg));

    // Idempotency: drop duplicates for ~60s
    const key = seenKey(msg);
    if (SEEN.has(key)) return res.json({ ok:true, dedup:true });
    remember(key);

    // EXIT messages from Pine are informational for the relay; ignore
    if (action === 'EXIT') return res.json({ ok:true, ignored:'EXIT' });

    // 0) Explicit cleanup
    if (action === 'DELTA_CANCEL_ALL' || action === 'CANCEL_ALL') {
      const out = await cancelAllOrders();
      return res.json({ ok:true, did:'cancel_all_orders', delta: out });
    }
    if (action === 'CLOSE_POSITION') {
      const out = await closeAllPositions();
      return res.json({ ok:true, did:'close_all_positions', delta: out });
    }

    // 1) Bracket passthrough (SL/TP container from Pine)
    if (msg.stop_loss_order || msg.take_profit_order) {
      const r = await placeBracketSmart(msg);
      return res.json({ ok:true, step:'bracket', r });
    }

    // 2) Batch limits passthrough (TPs from Pine)
    if (msg.orders) {
      const r = await placeBatchSmart(msg);
      return res.json({ ok:true, step:'batch', r });
    }

    // 3) ENTER / FLIP → cancel orders + close positions, then gate until flat
    if (action === 'ENTER' || action === 'FLIP') {
      try { await cancelAllOrders(); }  catch(e) { console.warn('cancelAllOrders failed:', e?.message||e); }
      try { await closeAllPositions(); } catch(e) { console.warn('closeAllPositions failed:', e?.message||e); }

      const flat = await waitUntilFlat(); // 15s default
      console.log('flat gate result:', flat);
      // proceed even if false (belt & suspenders)
    }

    // 4) Market entry (symbol/side/qty required)
    const r = await placeEntrySmart(msg);
    return res.json({ ok:true, step:'entry', r });

  } catch (e) {
    console.error('✖ ERROR:', e);
    return res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

app.listen(PORT, ()=>console.log(`Relay listening http://localhost:${PORT} (BASE=${BASE_URL}, AUTH=${AUTH_MODE})`));
