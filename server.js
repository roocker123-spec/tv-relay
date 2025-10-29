// server.js — Delta Ladder Gateway (ESM)

// 0) Env & imports
import 'dotenv/config.js';
import express from 'express';
import axios from 'axios';
import pino from 'pino';
import pinoHttp from 'pino-http';

// ---------- Config ----------
const PORT = Number(process.env.PORT ?? 3000);

// If you don’t have correct signing yet, keep MOCK=true (no real API calls)
const MOCK = (process.env.MOCK ?? 'true').toLowerCase() === 'true';

// IMPORTANT: use the exact var name from your .env
// Example for Delta India: https://api.india.delta.exchange
const DELTA_BASE_URL =
  process.env.DELTA_BASE ||
  process.env.DELTA_BASE_URL ||
  'https://api.delta.exchange';

const DELTA_KEY    = process.env.DELTA_API_KEY ?? '';
const DELTA_SECRET = process.env.DELTA_API_SECRET ?? '';
const DELTA_PPH    = process.env.DELTA_API_PASSPHRASE ?? '';

// --- ENTER barrier knobs
const FLAT_TIMEOUT_MS  = 20_000;   // total wait for flatness
const FLAT_INTERVAL_MS = 700;      // poll cadence
const SIZE_EPS         = 1e-8;     // treat <= this as flat

// ---------- Logger ----------
const log = pino({ level: process.env.LOG_LEVEL ?? 'info' });
const httpLogger = pinoHttp({ logger: log });

// ---------- Express ----------
const app = express();
app.use(express.json({ limit: '256kb' }));
app.use(httpLogger);

// ---------- Axios (Delta client) ----------
const delta = axios.create({
  baseURL: DELTA_BASE_URL,
  timeout: 15_000,
});

// TODO (LIVE): add the real HMAC signing per Delta docs here.
// Leaving a placeholder so you can drop your working signer.
delta.interceptors.request.use((cfg) => {
  if (MOCK) return cfg;

  // Example sketch — REPLACE with your proven signing:
  // const ts = String(Math.floor(Date.now() / 1000));
  // const prehash = ts + cfg.method.toUpperCase() + cfg.url + (cfg.data ? JSON.stringify(cfg.data) : '');
  // const sig = crypto.createHmac('sha256', DELTA_SECRET).update(prehash).digest('hex');
  // cfg.headers['api-key'] = DELTA_KEY;
  // cfg.headers['timestamp'] = ts;
  // cfg.headers['signature'] = sig;
  // if (DELTA_PPH) cfg.headers['passphrase'] = DELTA_PPH;

  return cfg;
});

// ---------- Utils ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nowISO = () => new Date().toISOString();

// =====================================================================
// 1) Readers used by the barrier (fill with real endpoints when LIVE)
// =====================================================================
async function deltaListOpenOrders({ product_symbol, strategy_id }) {
  if (MOCK) return [];
  // Replace with your actually working “list open orders” endpoint:
  // e.g. GET /v2/orders?status=open&product_symbol=...&strategy_id=...
  const { data } = await delta.get('/v2/orders', {
    params: { status: 'open', product_symbol, ...(strategy_id ? { strategy_id } : {}) },
  });
  return Array.isArray(data?.orders) ? data.orders : [];
}

async function deltaGetPosition({ product_symbol }) {
  if (MOCK) return { size: 0 };
  // Replace with your actually working “positions” endpoint:
  // e.g. GET /v2/positions?product_symbol=...
  const { data } = await delta.get('/v2/positions', { params: { product_symbol } });
  const arr = Array.isArray(data?.positions) ? data.positions : [];
  const pos = arr.find(p => p.product_symbol === product_symbol);
  const n = Number(pos?.size ?? 0);
  return { size: isFinite(n) ? n : 0 };
}

// =====================================================================
// 2) Writers — calls you use from Pine / PowerShell
// =====================================================================
async function deltaCancelAll({ product_symbol }) {
  if (MOCK) {
    log.info({ product_symbol }, 'MOCK: CANCEL_ALL');
    return { ok: true, mock: true };
  }
  // Replace with your working one (401/404 means the path/auth is wrong)
  const { data } = await delta.post('/v2/orders/cancel_all', { product_symbol });
  return data ?? { ok: true };
}

async function deltaClosePosition({ product_symbol, side, strategy_id }) {
  if (MOCK) {
    log.info({ product_symbol, side, strategy_id }, 'MOCK: CLOSE_POSITION');
    return { ok: true, mock: true };
  }
  // Typical shape: market reduce-only opposite side
  const payload = {
    product_symbol,
    order_type: 'market_order',
    side,                 // "buy" or "sell" — opposite of the open position
    size: 'close',        // or the current abs(size)
    reduce_only: true,
    time_in_force: 'ioc',
    ...(strategy_id ? { strategy_id } : {}),
  };
  const { data } = await delta.post('/v2/orders', payload);
  return data ?? { ok: true };
}

async function deltaEnterMarket({ product_symbol, side, qty, strategy_id }) {
  if (MOCK) {
    log.info({ product_symbol, side, qty, strategy_id }, 'MOCK: ENTER MARKET');
    return { ok: true, mock: true };
  }
  const payload = {
    product_symbol,
    order_type: 'market_order',
    side,                      // "buy" | "sell"
    size: Number(qty),
    reduce_only: false,
    time_in_force: 'ioc',
    ...(strategy_id ? { strategy_id } : {}),
  };
  const { data } = await delta.post('/v2/orders', payload);
  return data ?? { ok: true };
}

// =====================================================================
// 3) Flat barrier (cancel + close + wait) — before ENTER/FLIP
// =====================================================================
async function waitUntilFlat({ product_symbol, strategy_id },
  { timeoutMs = FLAT_TIMEOUT_MS, intervalMs = FLAT_INTERVAL_MS } = {}) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const [orders, pos] = await Promise.all([
      deltaListOpenOrders({ product_symbol, strategy_id }).catch(() => []),
      deltaGetPosition({ product_symbol }).catch(() => ({ size: 0 })),
    ]);
    const noOrders = !orders || orders.length === 0;
    const flatPos = !pos || Math.abs(Number(pos.size) || 0) <= SIZE_EPS;
    if (noOrders && flatPos) return true;
    await sleep(intervalMs);
  }
  return false;
}

async function ensureFlatThenEnter(msg, logger = log) {
  await deltaCancelAll({ product_symbol: msg.product_symbol }).catch(e =>
    logger.warn({ err: e?.message }, 'cancel_all failed (continuing)')
  );
  await deltaClosePosition({
    product_symbol: msg.product_symbol,
    side: msg.side === 'buy' ? 'sell' : 'buy',
    strategy_id: msg.strategy_id,
  }).catch(e => logger.warn({ err: e?.message }, 'close_position failed (continuing)'));

  logger.info({ ps: msg.product_symbol, sid: msg.strategy_id }, 'waiting to become FLAT…');
  const ok = await waitUntilFlat(
    { product_symbol: msg.product_symbol, strategy_id: msg.strategy_id },
    { timeoutMs: FLAT_TIMEOUT_MS, intervalMs: FLAT_INTERVAL_MS }
  );
  if (!ok) {
    const err = { ok: false, error: 'Timeout: still not flat; entry aborted' };
    logger.error(err);
    return err;
  }
  return deltaEnterMarket({
    product_symbol: msg.product_symbol,
    side: msg.side,
    qty: msg.qty ?? msg.size,
    strategy_id: msg.strategy_id,
  });
}

// =====================================================================
// 4) Simple serial queue so /tv requests don’t overlap
// =====================================================================
let _serial = Promise.resolve();
function runSerial(fn) {
  _serial = _serial.then(fn, fn).catch(e => log.error(e));
  return _serial;
}

// =====================================================================
// 5) Endpoints — /tv (your working format) + /webhook (alias)
// =====================================================================
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    ts: nowISO(),
    mock: MOCK,
    port: PORT,
    delta_base_url: DELTA_BASE_URL,
    has_keys: Boolean(DELTA_KEY && DELTA_SECRET),
  });
});

function handleMsg(msg) {
  const a = String(msg.action ?? '').toUpperCase();
  log.info({ a, msg }, 'TV/Webhook received');

  switch (a) {
    case 'DELTA_CANCEL_ALL':
    case 'CANCEL_ALL':
      return deltaCancelAll({ product_symbol: msg.product_symbol });

    case 'CLOSE_POSITION':
      return deltaClosePosition({
        product_symbol: msg.product_symbol,
        side: msg.side === 'buy' ? 'sell' : 'buy',
        strategy_id: msg.strategy_id,
      });

    case 'ENTER':
    case 'FLIP':
      return ensureFlatThenEnter(msg, log);

    case 'PANIC':
      // optional: cancel then close
      return (async () => {
        await deltaCancelAll({ product_symbol: msg.product_symbol }).catch(() => {});
        return deltaClosePosition({
          product_symbol: msg.product_symbol,
          side: msg.side === 'buy' ? 'sell' : 'buy',
          strategy_id: msg.strategy_id,
        });
      })();

    default:
      log.warn({ a }, 'Unknown action; ignoring');
      return Promise.resolve({ ok: true, ignored: true });
  }
}

// Your “working” endpoint
app.post('/tv', (req, res) => {
  const msg = req.body || {};
  res.json({ ok: true, enqueued: true });
  runSerial(() => handleMsg(msg).catch(e =>
    log.error({ err: e?.message, stack: e?.stack }, 'Action failed')
  ));
});

// Back-compat alias (Pine can still post here)
app.post('/webhook', (req, res) => {
  const msg = req.body || {};
  res.json({ ok: true, enqueued: true });
  runSerial(() => handleMsg(msg).catch(e =>
    log.error({ err: e?.message, stack: e?.stack }, 'Action failed')
  ));
});

// ---------- Start ----------
app.listen(PORT, () => {
  log.info({ msg: `Delta Ladder Gateway listening on :${PORT}` });
});
