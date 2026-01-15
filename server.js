// server.js (FULL) — GLOBAL queue/lock, sig_id+seq idempotency,
// STRICT sequencing: expects CANCAL(seq0) -> ENTER(seq1) -> BATCH_TPS(seq2),
// ENTER can self-recover flips *only when not already flat* (so missed CANCAL won't brick flips),
// require_flat gating blocks ENTER if flattening not complete,
// and BATCH_TPS is blocked unless ENTER happened for same sig_id.
//
// ✅ INCLUDED:
// 1) Buffer/queue early BATCH_TPS (seq2 arriving before ENTER) instead of ignoring it.
// 2) Flush queued BATCH_TPS immediately after ENTER succeeds.
// 3) ENTER will NOT flatten if already flat; but if require_flat=true and it detects NOT flat,
//    it will auto-set cancel_orders+close_position to true (unless explicitly provided) to self-recover.
// 4) STRICT mode requires numeric seq whenever sig_id exists (prevents late CANCAL w/out seq nuking fresh ENTER).
// 5) Fire-and-forget learnLotMultFromPositions is .catch()’d to avoid unhandled promise rejections.
//
// ✅ UPDATES ADDED (your request):
// A) flattenFromMsg(): default cancel_orders_scope is SYMBOL unless scope is ALL
// B) CANCAL handler: if cancel_orders_scope not provided, set it to SYMBOL (extra safety)
//
// ✅ FIX ADDED (your error):
// C) Symbol-scoped cancel now uses correct Delta endpoint:
//    DELETE /v2/orders with body { id, product_id } (NOT /v2/orders/{id})
//
// ✅ NEW (your request: keep CANCAL+ENTER same signal bar; BATCH next bar; but ENTER must be FAST):
// D) FAST_ENTER mode: do a short flat-wait (e.g. 2000ms), then attempt entry immediately.
//    If entry fails (exchange rejects due to not-flat), retry once after a longer wait.
//
// ENV you can set:
// FAST_ENTER=true
// FAST_ENTER_WAIT_MS=2000
// FAST_ENTER_RETRY_MS=8000

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
  let s = String(sym).replace('.P','');           // remove TradingView .P suffix if present
  if(s.includes(':')) s = s.split(':').pop();     // remove prefix like BINANCE:
  return s;
}
function isScopeAll(msg){
  return String(msg.scope||'').toUpperCase() === 'ALL' || !!msg.close_all;
}
function safeUpper(x){ return String(x||'').toUpperCase(); }

// ---------- global queue (serializes webhook execution) ----------
const QUEUE = new Map(); // key -> Promise chain
function enqueue(key, fn) {
  const prev = QUEUE.get(key) || Promise.resolve();
  const next = prev
    .catch(() => {})          // keep chain alive even if prev errored
    .then(fn)
    .finally(() => {
      if (QUEUE.get(key) === next) QUEUE.delete(key);
    });
  QUEUE.set(key, next);
  return next;
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
const FX_INR_FALLBACK    = nnum(process.env.FX_INR_FALLBACK, 85);     // ₹ per USD
const MARGIN_BUFFER_PCT  = nnum(process.env.MARGIN_BUFFER_PCT, 0.03); // 3%
const MAX_LOTS_PER_ORDER = nnum(process.env.MAX_LOTS_PER_ORDER, 200000);

const FLAT_TIMEOUT_MS    = nnum(process.env.FLAT_TIMEOUT_MS, 15000);
const FLAT_POLL_MS       = nnum(process.env.FLAT_POLL_MS, 400);

// ---------- STRICT sequence (default ON) ----------
const STRICT_SEQUENCE = String(process.env.STRICT_SEQUENCE || 'true').toLowerCase() !== 'false';

// ✅ FAST_ENTER mode (new)
const FAST_ENTER          = String(process.env.FAST_ENTER || 'false').toLowerCase() === 'true';
const FAST_ENTER_WAIT_MS  = nnum(process.env.FAST_ENTER_WAIT_MS, 2000);
const FAST_ENTER_RETRY_MS = nnum(process.env.FAST_ENTER_RETRY_MS, 8000);

// ---------- idempotency (drops dupes ~60s) ----------
const SEEN = new Map();              // key -> ts(ms)
const SEEN_TTL_MS = 60_000;

// ✅ includes sig_id + seq
function seenKey(msg){
  const sig = String(msg.sig_id || msg.signal_id || '');
  const seq = String(msg.seq ?? '');
  const p = [
    String(msg.action||''),
    String(msg.product_symbol||msg.symbol||''),
    String(msg.side||''),
    String(msg.qty||''),
    String(msg.entry||''),
    String(msg.strategy_id||''),
    sig,
    seq,
    String(msg.amount||msg.amount_inr||msg.amount_usd||msg.order_amount||'')
  ].join('|');
  return crypto.createHash('sha1').update(p).digest('hex');
}

function rememberSeen(k){
  SEEN.set(k, Date.now());
  for (const [kk, ts] of SEEN) {
    if (Date.now()-ts > SEEN_TTL_MS) SEEN.delete(kk);
  }
  if (SEEN.size > 300) {
    for (const kk of SEEN.keys()) { SEEN.delete(kk); if (SEEN.size <= 200) break; }
  }
}

// ---------- STRICT sequence state (sig_id -> last seq) ----------
const SIG_STATE = new Map();         // sig_id -> { lastSeq, ts, symbol }
const SIG_STATE_TTL_MS = 10 * 60 * 1000;

function cleanupSigState(){
  const now = Date.now();
  for (const [k,v] of SIG_STATE) {
    if (!v || (now - v.ts) > SIG_STATE_TTL_MS) SIG_STATE.delete(k);
  }
}
function getSigState(sig_id){
  cleanupSigState();
  const key = String(sig_id||'');
  if (!key) return null;
  return SIG_STATE.get(key) || null;
}
function setSigState(sig_id, patch){
  cleanupSigState();
  const key = String(sig_id||'');
  if (!key) return;
  const prev = SIG_STATE.get(key) || { lastSeq: -1, ts: 0, symbol: '' };
  SIG_STATE.set(key, { ...prev, ...patch, ts: Date.now() });
}

// ✅ Pending BATCH buffer (sig_id -> queued batch msg)
const PENDING_BATCH = new Map(); // sig_id -> { msg, ts }
const PENDING_TTL_MS = 60_000;

function cleanupPendingBatch(){
  const now = Date.now();
  for (const [k,v] of PENDING_BATCH) {
    if (!v || (now - v.ts) > PENDING_TTL_MS) PENDING_BATCH.delete(k);
  }
}
function queuePendingBatch(sigId, msg){
  cleanupPendingBatch();
  if (!sigId) return false;
  PENDING_BATCH.set(String(sigId), { msg, ts: Date.now() });
  return true;
}
function takePendingBatch(sigId){
  cleanupPendingBatch();
  if (!sigId) return null;
  const key = String(sigId);
  const v = PENDING_BATCH.get(key) || null;
  if (v) PENDING_BATCH.delete(key);
  return v?.msg || null;
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
async function getProductIdBySymbol(psym){
  const meta = await getProductMeta(psym);
  const pid = meta?.id ?? meta?.product_id;
  return Number.isFinite(+pid) ? +pid : null;
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

  // ✅ fire-and-forget safely (no unhandled rejection)
  learnLotMultFromPositions(product_symbol).catch(()=>{});

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

// ✅ FIX: Correct cancel endpoint is DELETE /v2/orders with body {id, product_id}
async function cancelOrder({ id, client_order_id, product_id, product_symbol }){
  const payload = {};

  if (Number.isFinite(+id)) payload.id = +id;
  if (client_order_id) payload.client_order_id = String(client_order_id);

  let pid = Number.isFinite(+product_id) ? +product_id : null;
  if (!pid && product_symbol) pid = await getProductIdBySymbol(product_symbol);

  if (!pid) {
    // If we can't supply product_id, Delta may reject; safer to throw so caller can fallbackAll if enabled.
    throw new Error(`cancelOrder: missing product_id (id=${id}, client_order_id=${client_order_id}, product_symbol=${product_symbol})`);
  }
  payload.product_id = pid;

  if (!payload.id && !payload.client_order_id) {
    return { ok:true, skipped:true, reason:'missing_id_and_client_order_id' };
  }

  return dcall('DELETE', '/v2/orders', payload);
}

// Try symbol-scoped cancel (preferred if you trade multiple symbols)
// If the endpoint differs or order objects lack required fields, it can fallback to cancelAllOrders if enabled.
async function cancelOrdersBySymbol(psym, { fallbackAll=false } = {}){
  const sym = toProductSymbol(psym);
  if (!sym) return { ok:true, skipped:true, reason:'missing_symbol' };

  let open = [];
  try {
    open = await listOpenOrdersAllPages();
  } catch(e) {
    if (fallbackAll) {
      await cancelAllOrders();
      return { ok:true, fallback:'cancel_all_orders' };
    }
    throw e;
  }

  const mine = open.filter(o => safeUpper(o?.product_symbol||o?.symbol) === safeUpper(sym));
  if (!mine.length) {
    return { ok:true, skipped:true, reason:'no_open_orders_for_symbol', symbol: sym };
  }

  let cancelled = 0, failed = 0;
  for (const o of mine){
    const oid = o?.id ?? o?.order_id;
    const pid = o?.product_id;
    const coid = o?.client_order_id;

    try {
      await cancelOrder({ id: oid, client_order_id: coid, product_id: pid, product_symbol: sym });
      cancelled++;
    } catch(e){
      failed++;
      console.warn('cancelOrdersBySymbol: cancel failed', { symbol: sym, oid, pid, err: e?.message || e });
    }
  }

  if (failed && fallbackAll) {
    await cancelAllOrders();
    return { ok:true, cancelled, failed, fallback:'cancel_all_orders' };
  }

  return { ok:true, cancelled, failed, symbol: sym };
}

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

// Close ONLY the symbol from webhook by sending a reduce-only market order
async function closePositionBySymbol(symbolOrProductSymbol){
  const psym = toProductSymbol(symbolOrProductSymbol);
  if (!psym) throw new Error('closePositionBySymbol: missing symbol/product_symbol');

  const pos = await listPositionsArray();
  const row = pos.find(p =>
    safeUpper(p?.product_symbol || p?.symbol) === safeUpper(psym)
  );

  const sizeCoins = Number(row?.size || row?.position_size || 0);
  if (!sizeCoins || Math.abs(sizeCoins) < 1e-12) {
    console.log('closePositionBySymbol: no open position for', psym);
    return { ok:true, skipped:true, reason:'no_position' };
  }

  // lot multiplier from cache or meta
  let lotMult = getCachedLotMult(psym);
  if (!lotMult) {
    const meta = await getProductMeta(psym);
    lotMult = lotMultiplierFromMeta(meta);
    setCachedLotMult(psym, lotMult);
  }

  const absCoins = Math.abs(sizeCoins);
  // Use CEIL so we don't leave a tiny remainder; reduce_only prevents flipping/opening extra
  const lots = Math.max(1, Math.ceil((absCoins / lotMult) - 1e-12));
  const side = sizeCoins > 0 ? 'sell' : 'buy';

  console.log('closePositionBySymbol:', { psym, sizeCoins, lotMult, lots, side });

  return dcall('POST','/v2/orders',{
    product_symbol: psym,
    order_type: 'market_order',
    side,
    size: lots,
    reduce_only: true
  });
}

// Gate until there are no open orders and no positions (GLOBAL).
async function waitUntilFlat(timeoutMs = FLAT_TIMEOUT_MS, pollMs = FLAT_POLL_MS) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const oo  = await listOpenOrdersAllPages();
      const hasOrders = oo.some(o => ['open','pending','triggered','untriggered']
        .includes(String(o?.state||o?.status||'').toLowerCase()));
      const pos = await listPositionsArray();
      const hasPos = pos.some(p => Math.abs(Number(p?.size||p?.position_size||0)) > 0);
      if (!hasOrders && !hasPos) return true;

      console.log('…still flattening (GLOBAL)', {
        openOrders: oo.length,
        positions: pos.map(p=>({
          product_id: p.product_id,
          symbol: p.product_symbol||p.symbol,
          size: p.size || p.position_size
        }))
      });
    } catch(e) {
      console.warn('waitUntilFlat poll error (ignoring):', e?.message || e);
    }
    await sleep(pollMs);
  }
  return false;
}

// Gate until there are no open orders and no position for ONE SYMBOL.
async function waitUntilFlatSymbol(psym, timeoutMs = FLAT_TIMEOUT_MS, pollMs = FLAT_POLL_MS) {
  const sym = toProductSymbol(psym);
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const oo  = await listOpenOrdersAllPages();
      const mineOrders = oo.filter(o => safeUpper(o?.product_symbol||o?.symbol) === safeUpper(sym));
      const hasOrders = mineOrders.some(o => ['open','pending','triggered','untriggered']
        .includes(String(o?.state||o?.status||'').toLowerCase()));

      const pos = await listPositionsArray();
      const minePos = pos.find(p => safeUpper(p?.product_symbol||p?.symbol) === safeUpper(sym));
      const hasPos = minePos ? (Math.abs(Number(minePos?.size||minePos?.position_size||0)) > 0) : false;

      if (!hasOrders && !hasPos) return true;

      console.log('…still flattening (SYMBOL)', {
        symbol: sym,
        openOrders: mineOrders.length,
        position: minePos ? { size: minePos.size || minePos.position_size } : null
      });
    } catch(e) {
      console.warn('waitUntilFlatSymbol poll error (ignoring):', e?.message || e);
    }
    await sleep(pollMs);
  }
  return false;
}

// One-shot checks (used to decide whether ENTER should self-flatten)
async function isFlatNowGlobal(){
  try {
    const oo  = await listOpenOrdersAllPages();
    const hasOrders = oo.some(o => ['open','pending','triggered','untriggered']
      .includes(String(o?.state||o?.status||'').toLowerCase()));
    const pos = await listPositionsArray();
    const hasPos = pos.some(p => Math.abs(Number(p?.size||p?.position_size||0)) > 0);
    return !hasOrders && !hasPos;
  } catch {
    return false;
  }
}
async function isFlatNowSymbol(psym){
  const sym = toProductSymbol(psym);
  try {
    const oo  = await listOpenOrdersAllPages();
    const mineOrders = oo.filter(o => safeUpper(o?.product_symbol||o?.symbol) === safeUpper(sym));
    const hasOrders = mineOrders.some(o => ['open','pending','triggered','untriggered']
      .includes(String(o?.state||o?.status||'').toLowerCase()));

    const pos = await listPositionsArray();
    const minePos = pos.find(p => safeUpper(p?.product_symbol||p?.symbol) === safeUpper(sym));
    const hasPos = minePos ? (Math.abs(Number(minePos?.size||minePos?.position_size||0)) > 0) : false;

    return !hasOrders && !hasPos;
  } catch {
    return false;
  }
}

// Performs flatten: cancel orders + close positions (symbol or all), then optional wait-flat.
async function flattenFromMsg(msg, psym){
  const scopeAll = isScopeAll(msg);

  const doCancelOrders = (typeof msg.cancel_orders === 'undefined') ? true : !!msg.cancel_orders;
  const doClosePos     = (typeof msg.close_position === 'undefined') ? true : !!msg.close_position;

  // ✅ UPDATED: default to SYMBOL unless scope is ALL
  const cancelScope = String(
    msg.cancel_orders_scope || (scopeAll ? 'ALL' : 'SYMBOL')
  ).toUpperCase(); // "SYMBOL" | "ALL"

  const cancelFallbackAll = !!msg.cancel_fallback_all;

  const steps = {
    cancel_orders: false,
    close_position: false,
    cancel_mode: null,
    close_mode: null,
    cancel_error: null,
    close_error: null
  };

  if (doCancelOrders) {
    try {
      if (scopeAll || cancelScope === 'ALL') {
        await cancelAllOrders();
        steps.cancel_mode = 'cancel_all_orders';
      } else {
        await cancelOrdersBySymbol(psym, { fallbackAll: cancelFallbackAll });
        steps.cancel_mode = 'cancel_symbol_orders';
      }
      steps.cancel_orders = true;
    } catch (e) {
      steps.cancel_error = String(e?.message || e);
      console.warn('flatten cancel failed:', e?.message || e);
    }
  }

  if (doClosePos) {
    try {
      if (scopeAll) {
        await closeAllPositions();
        steps.close_mode = 'close_all_positions';
      } else {
        const sym = msg.symbol || msg.product_symbol || psym;
        if (sym) {
          await closePositionBySymbol(sym);
          steps.close_mode = 'close_by_symbol';
        } else {
          await closeAllPositions();
          steps.close_mode = 'close_all_positions';
        }
      }
      steps.close_position = true;
    } catch (e) {
      steps.close_error = String(e?.message || e);
      console.warn('flatten close failed:', e?.message || e);
    }
  }

  return steps;
}

// ---------- health ----------
app.get('/health', (_req,res)=>res.json({ok:true, started_at:process.env.__STARTED_AT}));
app.get('/healthz', (_req,res)=>res.send('ok'));
app.get('/debug/seen', (_req,res)=>{ res.json({ size: SEEN.size }); });
app.get('/debug/sigstate', (_req,res)=>{
  cleanupSigState();
  const out = {};
  for (const [k,v] of SIG_STATE) out[k] = v;
  res.json(out);
});
app.get('/debug/pending_batch', (_req,res)=>{
  cleanupPendingBatch();
  const out = {};
  for (const [k,v] of PENDING_BATCH) out[k] = { ts: v.ts, action: v?.msg?.action, symbol: v?.msg?.product_symbol || v?.msg?.symbol };
  res.json({ size: PENDING_BATCH.size, items: out });
});

// ---------- TradingView webhook ----------
app.post('/tv', async (req, res) => {
  try {
    if (WEBHOOK_TOKEN) {
      const hdr = req.headers['x-webhook-token'];
      if (hdr !== WEBHOOK_TOKEN) return res.status(401).json({ ok:false, error:'unauthorized' });
    }

    const msg    = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body || {});
    const action = String(msg.action || '').toUpperCase();
    const sigId  = String(msg.sig_id || msg.signal_id || '');
    const seq    = (typeof msg.seq !== 'undefined') ? Number(msg.seq) : NaN;
    const symTV  = msg.symbol || msg.product_symbol || '';
    const psym   = toProductSymbol(symTV);

    console.log('\n=== INCOMING /tv ===\n', JSON.stringify(msg));

    const out = await enqueue('GLOBAL', async () => {

      // ---- idempotency guard ----
      const key = seenKey(msg);
      if (SEEN.has(key)) return { ok:true, dedup:true };
      rememberSeen(key);

      // Ignore pure EXIT logs from chart
      if (action === 'EXIT') return { ok:true, ignored:'EXIT' };

      // ✅ STRICT: if sig_id is present, seq must be numeric
      if (STRICT_SEQUENCE && sigId) {
        if (!Number.isFinite(seq)) {
          return { ok:true, ignored:'missing_or_invalid_seq_in_strict_mode', sig_id: sigId, action };
        }
      }

      // ---- STRICT SEQUENCE SYMBOL BIND (sig_id) ----
      if (STRICT_SEQUENCE && sigId && Number.isFinite(seq)) {
        const st0 = getSigState(sigId) || { lastSeq: -1, symbol: psym };
        if (st0.symbol && psym && st0.symbol.toUpperCase() !== psym.toUpperCase()) {
          setSigState(sigId, { lastSeq: -1, symbol: psym });
        }
      }

      // ✅ STRICT out-of-order / late message guard
      if (STRICT_SEQUENCE && sigId && Number.isFinite(seq)) {
        const st = getSigState(sigId);
        if (st && Number.isFinite(st.lastSeq) && st.lastSeq >= seq) {
          return {
            ok: true,
            ignored: 'out_of_order_or_duplicate_seq',
            sig_id: sigId,
            have_lastSeq: st.lastSeq,
            got_seq: seq,
            action
          };
        }
      }

      // --------- CANCAL / CANCEL handler ---------
      if (action === 'CANCAL' || action === 'CANCEL') {
        if (STRICT_SEQUENCE && sigId && Number.isFinite(seq) && seq !== 0) {
          return { ok:true, ignored:'CANCAL_seq_mismatch', expected:0, got: seq, sig_id: sigId };
        }

        // ✅ UPDATED: default cancel_orders_scope=SYMBOL if not provided
        if (typeof msg.cancel_orders_scope === 'undefined') msg.cancel_orders_scope = 'SYMBOL';

        const steps = await flattenFromMsg(msg, psym);

        const requireFlat = (typeof msg.require_flat === 'undefined') ? true : !!msg.require_flat;
        let flat = true;
        if (requireFlat) {
          flat = isScopeAll(msg) ? await waitUntilFlat() : await waitUntilFlatSymbol(psym);
        }

        if (STRICT_SEQUENCE && sigId) {
          setSigState(sigId, { lastSeq: 0, symbol: psym });
        }

        return { ok:true, did:'CANCAL', steps, flat };
      }
      // ---------------------------------------------------------------------------

      if (action === 'DELTA_CANCEL_ALL' || action === 'CANCEL_ALL') {
        const out = await cancelAllOrders();
        return { ok:true, did:'cancel_all_orders', delta: out };
      }
      if (action === 'CLOSE_POSITION') {
        const out = await closeAllPositions();
        return { ok:true, did:'close_all_positions', delta: out };
      }

      if (msg.stop_loss_order || msg.take_profit_order) {
        const r = await placeBracket(msg);
        return { ok:true, step:'bracket', r };
      }

      // Batch (TPs)
      if (msg.orders) {
        if (action !== 'BATCH_TPS') return { ok:true, ignored:'orders_without_BATCH_TPS' };
        if (Number.isFinite(seq) && seq !== 2) return { ok:true, ignored:'batch_seq_mismatch', expected:2, got: msg.seq };

        if (STRICT_SEQUENCE) {
          if (!sigId) return { ok:true, ignored:'BATCH_TPS_missing_sig_id' };
          const st = getSigState(sigId);

          if (!st || st.lastSeq < 1) {
            queuePendingBatch(sigId, msg);
            return { ok:true, queued:'BATCH_TPS_waiting_for_ENTER', sig_id: sigId, have: st ? st.lastSeq : null };
          }
        }

        const r = await placeBatch(msg);

        if (STRICT_SEQUENCE && sigId) {
          setSigState(sigId, { lastSeq: 2, symbol: psym });
        }

        return { ok:true, step:'batch', r };
      }

      // ENTER / FLIP
      if (action === 'ENTER' || action === 'FLIP') {

        if (STRICT_SEQUENCE) {
          if (!sigId) return { ok:true, ignored:'ENTER_missing_sig_id' };
          if (Number.isFinite(seq) && seq !== 1) return { ok:true, ignored:'ENTER_seq_mismatch', expected:1, got: msg.seq };
        }

        const st = (STRICT_SEQUENCE && sigId) ? (getSigState(sigId) || { lastSeq: -1, symbol: psym }) : null;

        if (STRICT_SEQUENCE && st && st.lastSeq >= 1) {
          return { ok:true, ignored:'ENTER_already_seen', sig_id: sigId, have: st.lastSeq };
        }

        const requireFlat = (typeof msg.require_flat === 'undefined') ? true : !!msg.require_flat;

        if (typeof msg.cancel_orders_scope === 'undefined') msg.cancel_orders_scope = 'SYMBOL';

        // Check if already flat (so we don't waste time flattening)
        let flatNow = true;
        if (requireFlat) {
          flatNow = isScopeAll(msg) ? await isFlatNowGlobal() : await isFlatNowSymbol(psym);
        }

        // If require_flat and NOT flat, default to flatten true; if flat, keep false
        if (typeof msg.cancel_orders === 'undefined') msg.cancel_orders = requireFlat ? (!flatNow) : false;
        if (typeof msg.close_position === 'undefined') msg.close_position = requireFlat ? (!flatNow) : false;

        // Start flatten steps immediately (cancel/close)
        const steps = await flattenFromMsg(msg, psym);

        // Helper: after entry, flush queued BATCH_TPS (if any)
        async function flushQueuedBatchIfAny(){
          let batch = null;
          let batch_error = null;
          const pendingMsg = STRICT_SEQUENCE && sigId ? takePendingBatch(sigId) : null;
          if (pendingMsg) {
            try {
              batch = await placeBatch(pendingMsg);
              if (STRICT_SEQUENCE && sigId) setSigState(sigId, { lastSeq: 2, symbol: psym });
            } catch (e) {
              batch_error = String(e?.message || e);
            }
          }
          return { pendingMsg, batch, batch_error };
        }

        // ---------------- FAST ENTER mode (NEW) ----------------
        if (requireFlat && FAST_ENTER) {
          // Short wait only
          const flatOk = isScopeAll(msg)
            ? await waitUntilFlat(FAST_ENTER_WAIT_MS, FLAT_POLL_MS)
            : await waitUntilFlatSymbol(psym, FAST_ENTER_WAIT_MS, FLAT_POLL_MS);

          console.log('FAST_ENTER flat gate (short):', { flatOk, waitMs: FAST_ENTER_WAIT_MS, psym });

          // Try entry immediately
          try {
            const r = await placeEntry(msg);

            if (STRICT_SEQUENCE && sigId) {
              setSigState(sigId, { lastSeq: 1, symbol: psym });
            }

            const { pendingMsg, batch, batch_error } = await flushQueuedBatchIfAny();

            const recovered = (STRICT_SEQUENCE && st && st.lastSeq < 0);
            return {
              ok:true,
              step: pendingMsg ? 'entry+batch' : 'entry',
              fast_enter: true,
              flatOk_short_wait: flatOk,
              recovered_without_cancal: !!recovered,
              self_flattened: !flatNow,
              steps,
              r,
              batch,
              batch_error
            };

          } catch (e) {
            console.warn('FAST_ENTER first attempt failed:', e?.message || e);

            // Retry once after longer wait
            const flat2 = isScopeAll(msg)
              ? await waitUntilFlat(FAST_ENTER_RETRY_MS, FLAT_POLL_MS)
              : await waitUntilFlatSymbol(psym, FAST_ENTER_RETRY_MS, FLAT_POLL_MS);

            console.log('FAST_ENTER flat gate (retry):', { flat2, waitMs: FAST_ENTER_RETRY_MS, psym });

            if (!flat2) {
              return { ok:false, error:'require_flat_timeout_fast_retry', sig_id: sigId, steps };
            }

            const r = await placeEntry(msg);

            if (STRICT_SEQUENCE && sigId) {
              setSigState(sigId, { lastSeq: 1, symbol: psym });
            }

            const { pendingMsg, batch, batch_error } = await flushQueuedBatchIfAny();

            const recovered = (STRICT_SEQUENCE && st && st.lastSeq < 0);
            return {
              ok:true,
              step: pendingMsg ? 'entry+batch' : 'entry',
              fast_enter: true,
              retried: true,
              recovered_without_cancal: !!recovered,
              self_flattened: !flatNow,
              steps,
              r,
              batch,
              batch_error
            };
          }
        }
        // -------------- end FAST ENTER mode --------------------

        // Original strict wait (if FAST_ENTER off)
        if (requireFlat) {
          const flat = isScopeAll(msg) ? await waitUntilFlat() : await waitUntilFlatSymbol(psym);
          console.log('flat gate result:', flat);
          if (!flat) return { ok:false, error:'require_flat_timeout', sig_id: sigId, steps };
        }

        const r = await placeEntry(msg);

        if (STRICT_SEQUENCE && sigId) {
          setSigState(sigId, { lastSeq: 1, symbol: psym });
        }

        let batch = null;
        let batch_error = null;
        const pendingMsg = STRICT_SEQUENCE && sigId ? takePendingBatch(sigId) : null;
        if (pendingMsg) {
          try {
            batch = await placeBatch(pendingMsg);
            if (STRICT_SEQUENCE && sigId) setSigState(sigId, { lastSeq: 2, symbol: psym });
          } catch (e) {
            batch_error = String(e?.message || e);
          }
        }

        const recovered = (STRICT_SEQUENCE && st && st.lastSeq < 0);

        return {
          ok:true,
          step: pendingMsg ? 'entry+batch' : 'entry',
          recovered_without_cancal: !!recovered,
          self_flattened: requireFlat ? (!flatNow) : false,
          steps,
          r,
          batch,
          batch_error
        };
      }

      if (STRICT_SEQUENCE) {
        return { ok:true, ignored:'unknown_action_in_strict_mode', action, sig_id: sigId, seq: msg.seq };
      }

      const r = await placeEntry(msg);
      return { ok:true, step:'entry', r };
    });

    return res.json(out);

  } catch (e) {
    console.error('✖ ERROR:', e);
    return res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

app.listen(PORT, ()=>console.log(
  `Relay listening http://localhost:${PORT} (BASE=${BASE_URL}, AUTH=${AUTH_MODE}, STRICT_SEQUENCE=${STRICT_SEQUENCE}, FAST_ENTER=${FAST_ENTER})`
));
