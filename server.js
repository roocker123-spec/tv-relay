// server.js
require('dotenv').config();
const express = require('express');
const crypto  = require('crypto');
const fetch   = global.fetch; // Node 18+

// -------------------- utils --------------------
function nowTsSec(){ return Math.floor(Date.now()/1000).toString(); }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
function clamp(n,min,max){ return Math.min(Math.max(n,min),max); }
function nnum(x, d=0){ const n = Number(x); return Number.isFinite(n) ? n : d; }
function toProductSymbol(sym){
  if(!sym) return sym;
  let s = String(sym).replace('.P',''); // remove TradingView .P suffix if present
  if(s.includes(':')) s = s.split(':').pop(); // remove prefix like BINANCE:
  return s;
}

// -------------------- app --------------------
const app = express();
process.env.__STARTED_AT = new Date().toISOString();

// ---------- parsing ----------
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

// ---------- env / auth ----------
const API_KEY       = process.env.DELTA_API_KEY || '';
const API_SECRET    = process.env.DELTA_API_SECRET || '';
const BASE_URL      = (process.env.DELTA_BASE || process.env.DELTA_BASE_URL || 'https://api.india.delta.exchange').replace(/\/+$/,'');
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || ''; // optional header check
const PORT          = process.env.PORT || 3000;

const AUTH_MODE     = (process.env.DELTA_AUTH || 'hmac').toLowerCase(); // 'hmac' | 'keyonly'
const HDR_API_KEY   = process.env.DELTA_HDR_API_KEY || 'api-key';
const HDR_SIG       = process.env.DELTA_HDR_SIG     || 'signature';
const HDR_TS        = process.env.DELTA_HDR_TS      || 'timestamp';

// Amount-based sizing defaults
const DEFAULT_LEVERAGE   = nnum(process.env.DEFAULT_LEVERAGE, 10);
const FX_INR_FALLBACK    = nnum(process.env.FX_INR_FALLBACK, 85);  // ₹ per USD
const MARGIN_BUFFER_PCT  = nnum(process.env.MARGIN_BUFFER_PCT, 0.03); // 3%
const MAX_LOTS_PER_ORDER = nnum(process.env.MAX_LOTS_PER_ORDER, 200000);

// ---------- idempotency (drops dupes ~60s) ----------
const SEEN = new Map();              // key -> ts(ms)
const SEEN_TTL_MS = 60_000;
function seenKey(msg){
  const p = [
    String(msg.action||''),
    String(msg.product_symbol||msg.symbol||''),
    String(msg.side||''),
    String(msg.qty||''),
    String(msg.entry||''),
    String(msg.strategy_id||''),
    String(msg.sig_id||''),
    String(msg.amount||msg.amount_inr||msg.amount_usd||msg.order_amount||'')
  ].join('|');
  return crypto.createHash('sha1').update(p).digest('hex');
}
function remember(k){
  SEEN.set(k, Date.now());
  for (const [kk, ts] of SEEN) {
    if (Date.now()-ts > SEEN_TTL_MS) SEEN.delete(kk);
  }
  if (SEEN.size > 300) {
    for (const kk of SEEN.keys()) { SEEN.delete(kk); if (SEEN.size <= 200) break; }
  }
}

// ---------- Delta request helper (retries/backoff) ----------
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
          await sleep(300*attempt);
          continue;
        }
        throw new Error(`Delta API error: ${JSON.stringify({ url, status: res.status, json })}`);
      }
      return json;
    } catch (e) {
      if (attempt === MAX_TRIES) throw e;
      await sleep(300*attempt);
    }
  }
}

// ---------- product + price helpers (no hardcoding) ----------
let _products = null, _products_ts = 0;
async function getProducts(){
  const STALE_MS = 5*60*1000;
  if(!_products || (Date.now()-_products_ts) > STALE_MS){
    const r = await dcall('GET','/v2/products');
    const arr = Array.isArray(r?.result) ? r.result
            : Array.isArray(r?.products) ? r.products
            : Array.isArray(r) ? r : [];
    _products = arr;
    _products_ts = Date.now();
  }
  return _products;
}
async function getProductMeta(product_symbol){
  const ps = String(product_symbol||'').toUpperCase();
  const list = await getProducts();
  return list.find(p => String(p?.symbol||p?.product_symbol||'').toUpperCase() === ps);
}

const LOT_MULT_CACHE = new Map(); // product_symbol -> { m, ts }
app.get('/debug/lotcache', (_req,res)=>{
  const o = {}; for (const [k,v] of LOT_MULT_CACHE) o[k]=v;
  res.json(o);
});

// Try to infer multiplier from meta fields (robust, no hardcode)
function lotMultiplierFromMeta(meta){
  const cand = [
    meta?.contract_value,
    meta?.contract_size,
    meta?.lot_size,
    meta?.contract_unit,
    meta?.qty_step
  ].map(n => Number.isFinite(+n) ? +n : NaN).filter(n => n && n > 0);

  const ints = cand.filter(n => Math.abs(n - Math.round(n)) < 1e-6);
  let m = (ints.length ? Math.max(...ints) : (cand.length ? Math.max(...cand) : 1));

  if (!Number.isFinite(m) || m < 1) m = 1;
  return Math.round(m);
}
function getCachedLotMult(psym){ return LOT_MULT_CACHE.get(psym)?.m || null; }
function setCachedLotMult(psym, m){ if (m && m >= 1 && m <= 1e9) LOT_MULT_CACHE.set(psym,{m: Math.round(m), ts: Date.now()}); }

async function getTickerPriceUSD(psym){
  try {
    const q = `?symbol=${encodeURIComponent(psym)}`;
    const r = await dcall('GET','/v2/tickers', null, q);
    const arr = Array.isArray(r?.result) ? r.result : Array.isArray(r) ? r : [];
    const t = arr.find(x => (x?.symbol||x?.product_symbol)===psym);
    const px = nnum(t?.mark_price || t?.last_price || t?.index_price, 0);
    return px > 0 ? px : null;
  } catch { return null; }
}

// ---------- sizing: amount → lots ----------
function lotsFromAmount({ amount, ccy='INR', leverage=DEFAULT_LEVERAGE, entryPxUSD, lotMult=1, fxInrPerUsd=FX_INR_FALLBACK }){
  leverage = Math.max(1, Math.floor(nnum(leverage, DEFAULT_LEVERAGE)));
  lotMult  = Math.max(1, Math.floor(nnum(lotMult, 1)));
  const fx  = nnum(fxInrPerUsd, FX_INR_FALLBACK);
  const px  = nnum(entryPxUSD, 0);
  const amt = nnum(amount, 0);
  if (amt <= 0 || px <= 0) return 0;

  const marginUSD   = (ccy.toUpperCase()==='USD') ? amt : (amt / fx);
  const notionalUSD = marginUSD * leverage * (1 - MARGIN_BUFFER_PCT);
  const coins       = notionalUSD / px;
  const lots        = Math.floor(coins / lotMult);
  return Math.max(1, lots);
}

// ---------- last entry side (for TP auto-correct) ----------
const LAST_SIDE = new Map();
function rememberSide(productSymbol, side){
  if (!productSymbol) return;
  const s = String(side||'').toLowerCase()==='buy' ? 'buy' : 'sell';
  LAST_SIDE.set(productSymbol, s);
}
function oppositeSide(side){ return (String(side||'').toLowerCase()==='buy') ? 'sell' : 'buy'; }

// ---------- learning: coins-per-lot from actual position ----------
const LAST_ENTRY_SENT = new Map(); // psym -> { lots, ts, side }
async function learnLotMultFromPositions(psym){
  const last = LAST_ENTRY_SENT.get(psym);
  if (!last || (Date.now()-last.ts) > 15_000) return;

  try {
    const pos = await listPositionsArray();
    const row = pos.find(p => String(p?.product_symbol||p?.symbol||'').toUpperCase() === psym.toUpperCase());
    if (!row) return;

    const coinsAbs = Math.abs(Number(row.size||row.position_size||0));
    const lotsSent = Math.max(1, Number(last.lots||0));
    if (!coinsAbs || !lotsSent) return;

    const m = coinsAbs / lotsSent;
    if (Math.abs(m - Math.round(m)) < 1e-4 && Math.round(m) >= 1) {
      setCachedLotMult(psym, Math.round(m));
      LAST_ENTRY_SENT.delete(psym);
      console.log('learned lot multiplier', { product_symbol: psym, m: Math.round(m) });
    }
  } catch {}
}

// ---------- order helpers ----------
async function placeEntry(m){
  const side = (m.side||'').toLowerCase()==='buy' ? 'buy' : 'sell';
  const product_symbol = toProductSymbol(m.symbol || m.product_symbol);

  // lot multiplier from cache or meta
  let lotMult = getCachedLotMult(product_symbol);
  if (!lotMult) {
    const meta = await getProductMeta(product_symbol);
    lotMult = lotMultiplierFromMeta(meta);
    setCachedLotMult(product_symbol, lotMult);
  }

  // 1) qty (lots) provided -> use directly
  let sizeLots = parseInt(m.qty,10);
  let usedMode = 'qty';

  // 2) else derive from amount
  if (!sizeLots || sizeLots < 1) {
    const fxHint   = nnum(m.fxQuoteToINR || m.fx_quote_to_inr || m.fx || FX_INR_FALLBACK, FX_INR_FALLBACK);
    const leverage = Math.max(1, Math.floor(nnum(m.leverage || m.leverage_x || DEFAULT_LEVERAGE, DEFAULT_LEVERAGE)));
    const ccy      = String(m.amount_ccy || m.ccy || (typeof m.amount_usd !== 'undefined' ? 'USD' : 'INR')).toUpperCase();

    let entryPxUSD = nnum(m.entry, 0);
    if (!(entryPxUSD > 0)) entryPxUSD = nnum(await getTickerPriceUSD(product_symbol), 0);
    if (!(entryPxUSD > 0)) throw new Error(`No price available for ${product_symbol}`);

    let amount = undefined;
    if (typeof m.amount_inr   !== 'undefined') amount = nnum(m.amount_inr, 0);
    else if (typeof m.amount_usd !== 'undefined') amount = nnum(m.amount_usd, 0);
    else if (typeof m.order_amount !== 'undefined') amount = nnum(m.order_amount, 0);
    else if (typeof m.amount !== 'undefined') amount = nnum(m.amount, 0);
    if (!(amount > 0)) throw new Error('Amount-based entry requires amount_inr/amount_usd/order_amount/amount');

    sizeLots = lotsFromAmount({ amount, ccy, leverage, entryPxUSD, lotMult, fxInrPerUsd: fxHint });
    usedMode = `${ccy==='USD'?'amount_usd':'amount_inr'}`;
  }

  sizeLots = clamp(sizeLots, 1, MAX_LOTS_PER_ORDER);

  console.log('entry size normalization', { product_symbol, side, lotMult, usedMode, sizeLots });

  const out = await dcall('POST','/v2/orders',{
    product_symbol,
    order_type:'market_order',
    side,
    size: sizeLots
  });

  rememberSide(product_symbol, side);
  LAST_ENTRY_SENT.set(product_symbol, { lots: sizeLots, ts: Date.now(), side });
  // best-effort learning (no await)
  learnLotMultFromPositions(product_symbol);

  return out;
}

async function placeBracket(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = toProductSymbol(m.product_symbol || m.symbol);
  return dcall('POST','/v2/orders/bracket', body);
}

async function placeBatch(m){
  const {action, ...bodyIn} = m;
  const body = { ...bodyIn };
  if(!body.product_symbol && !body.product_id) body.product_symbol = toProductSymbol(m.product_symbol || m.symbol);

  try {
    const psym = body.product_symbol;
    const last = LAST_SIDE.get(psym);

    // ensure we know multiplier
    let lotMult = getCachedLotMult(psym);
    if (!lotMult) {
      const meta  = await getProductMeta(psym);
      lotMult = lotMultiplierFromMeta(meta);
      setCachedLotMult(psym, lotMult);
    }

    if (Array.isArray(body.orders)) {
      body.orders = body.orders.map(o => {
        const oo = { ...o };

        // If client provides coins (size_coins/coins), convert to lots
        const coins = nnum(oo.size_coins ?? oo.coins, 0);
        if (coins > 0) oo.size = Math.max(1, Math.floor(coins / lotMult));

        // If size looks suspiciously like coins (very large and divisible), normalize
        if (!coins && nnum(oo.size,0) > lotMult && Math.abs(nnum(oo.size,0) / lotMult - Math.round(nnum(oo.size,0) / lotMult)) < 1e-6) {
          oo.size = Math.max(1, Math.floor(nnum(oo.size,0) / lotMult));
        }

        if (last && (!oo.side || String(oo.side).toLowerCase() === last)) {
          oo.side = oppositeSide(last);
        }
        if (typeof oo.reduce_only === 'undefined') oo.reduce_only = true;
        return oo;
      });
    }
  } catch(e){
    console.warn('batch auto-correct warning:', e?.message || e);
  }

  return dcall('POST','/v2/orders/batch', body);
}

// ---------- CANCEL/CLOSE + listings ----------
const cancelAllOrders   = () => dcall('DELETE','/v2/orders/all');
const closeAllPositions = () => dcall('POST','/v2/positions/close_all', {});

// List helpers
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
  try {
    const pos = await dcall('GET','/v2/positions');
    const arr = Array.isArray(pos?.result?.positions) ? pos.result.positions
              : Array.isArray(pos?.result) ? pos.result
              : Array.isArray(pos?.positions) ? pos.positions
              : Array.isArray(pos) ? pos : [];
    if (arr.length || pos?.success !== false) return arr;
  } catch(e) {}
  try {
    const pos2 = await dcall('GET','/v2/positions/margined');
    const arr2 = Array.isArray(pos2?.result?.positions) ? pos2.result.positions
               : Array.isArray(pos2?.result) ? pos2.result
               : Array.isArray(pos2?.positions) ? pos2.positions
               : Array.isArray(pos2) ? pos2 : [];
    if (arr2.length) return arr2;
  } catch(e) {}
  return [];
}

// Gate until there are no open orders and no positions.
async function waitUntilFlat(timeoutMs = nnum(process.env.FLAT_TIMEOUT_MS,15000), pollMs = 400) {
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
        positions: pos.map(p=>({ product_id:p.product_id, symbol:p.product_symbol||p.symbol, size:p.size }))
      });
    } catch(e) {
      console.warn('waitUntilFlat poll error (ignoring):', e?.message || e);
    }
    await sleep(pollMs);
  }
  return false;
}

// ---------- health ----------
app.get('/health', (_req,res)=>res.json({ok:true, started_at:process.env.__STARTED_AT}));
app.get('/healthz', (_req,res)=>res.send('ok'));
app.get('/debug/seen', (_req,res)=>{ res.json({ size: SEEN.size }); });

// ---------- TradingView webhook ----------
app.post('/tv', async (req, res) => {
  try {
    if (WEBHOOK_TOKEN) {
      const hdr = req.headers['x-webhook-token'];
      if (hdr !== WEBHOOK_TOKEN) return res.status(401).json({ ok:false, error:'unauthorized' });
    }

    const msg    = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body || {});
    const action = String(msg.action || '').toUpperCase();
    console.log('\n=== INCOMING /tv ===\n', JSON.stringify(msg));

    // ---- idempotency guard ----
    const key = seenKey(msg);
    if (SEEN.has(key)) return res.json({ ok:true, dedup:true });
    remember(key);

    // Ignore pure EXIT logs from chart
    if (action === 'EXIT') return res.json({ ok:true, ignored:'EXIT' });

    // Explicit cleanup from Pine
    if (action === 'DELTA_CANCEL_ALL' || action === 'CANCEL_ALL') {
      const out = await cancelAllOrders();
      return res.json({ ok:true, did:'cancel_all_orders', delta: out });
    }
    if (action === 'CLOSE_POSITION') {
      const out = await closeAllPositions();
      return res.json({ ok:true, did:'close_all_positions', delta: out });
    }

    // Bracket passthrough
    if (msg.stop_loss_order || msg.take_profit_order) {
      const r = await placeBracket(msg);
      return res.json({ ok:true, step:'bracket', r });
    }

    // Batch (TPs) passthrough — with auto-correct & optional size-normalize
    if (msg.orders) {
      const r = await placeBatch(msg);
      return res.json({ ok:true, step:'batch', r });
    }

    // ENTER / FLIP: cancel+close, then gate until flat
    if (action === 'ENTER' || action === 'FLIP') {
      try { await cancelAllOrders(); } catch(e) { console.warn('cancelAllOrders failed:', e?.message||e); }
      try { await closeAllPositions(); } catch(e) { console.warn('closeAllPositions failed:', e?.message||e); }
      const flat = await waitUntilFlat();
      console.log('flat gate result:', flat);
    }

    // Market entry
    const r = await placeEntry(msg);
    return res.json({ ok:true, step:'entry', r });

  } catch (e) {
    console.error('✖ ERROR:', e);
    return res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

app.listen(PORT, ()=>console.log(`Relay listening http://localhost:${PORT} (BASE=${BASE_URL}, AUTH=${AUTH_MODE})`));
