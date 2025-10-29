// server.js  —  Delta Ladder Gateway (ESM)
// ----------------------------------------

// 0) Env & imports
import 'dotenv/config.js';
import express from 'express';
import axios from 'axios';
import pino from 'pino';
import pinoHttp from 'pino-http';

// ---------- Config ----------
const PORT = Number(process.env.PORT ?? 3000);

// If you don't have real API details yet, leave MOCK=true and it will "fake" the exchange
const MOCK = (process.env.MOCK ?? 'true').toLowerCase() === 'true';

const DELTA_BASE_URL = process.env.DELTA_BASE_URL ?? 'https://api.delta.exchange'; // <- adjust when you wire
const DELTA_KEY       = process.env.DELTA_API_KEY ?? '';
const DELTA_SECRET    = process.env.DELTA_API_SECRET ?? '';
const DELTA_PASSPHRASE= process.env.DELTA_API_PASSPHRASE ?? '';

// --- ENTER barrier knobs (Step 2) ---
const FLAT_TIMEOUT_MS  = 20_000;   // total wait time
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

// NOTE: Real Delta India auth is usually HMAC. Add headers/signature here if you have spec:
// delta.interceptors.request.use(cfg => {
//   // TODO: sign request — set cfg.headers with API key, timestamp, signature, passphrase
//   return cfg;
// });

// ---------- Small utils ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nowISO = () => new Date().toISOString();

// =====================================================================
// 1) ******* READERS used by the barrier (FILL THESE 2 WHEN GOING LIVE)
// =====================================================================

// A) List open orders for a product (optionally filter by strategy_id)
// Must return an ARRAY ( [] when none )
async function deltaListOpenOrders({ product_symbol, strategy_id }) {
  if (MOCK) {
    // In mock mode we pretend cancels are instant, so no open orders.
    return [];
  }
  // TODO #1: Replace with the real "list open orders" endpoint
  // The exact path/params depend on Delta docs. Typical shape:
  //   GET /v2/orders?status=open&product_symbol=...&strategy_id=...
  const { data } = await delta.get('/v2/orders', {
    params: {
      status: 'open',
      product_symbol,
      ...(strategy_id ? { strategy_id } : {}),
    },
  });
  // TODO #2: Return the correct array from response
  return Array.isArray(data?.orders) ? data.orders : [];
}

// B) Get current position for a product
// Must return { size: number }  (0 if flat)
async function deltaGetPosition({ product_symbol }) {
  if (MOCK) {
    // Mock mode: always flat
    return { size: 0 };
  }
  // TODO #3: Replace with the real "get positions" endpoint
  // Typical shape:
  //   GET /v2/positions?product_symbol=...
  const { data } = await delta.get('/v2/positions', {
    params: { product_symbol },
  });

  // TODO #4: pick the right item and return numeric size
  const arr = Array.isArray(data?.positions) ? data.positions : [];
  const pos = arr.find(p => p.product_symbol === product_symbol);
  const size = Number(pos?.size ?? 0);
  return { size: isFinite(size) ? size : 0 };
}

// =====================================================================
// 2) ********* WRITERS (your existing actions) — make idempotent
// =====================================================================

// Cancel all open orders for the product (all strategy_ids)
async function deltaCancelAll({ product_symbol }) {
  if (MOCK) {
    log.info({ product_symbol }, 'MOCK: CANCEL_ALL');
    return { ok: true, mock: true };
  }
  // TODO #5: Replace with Delta "cancel all" for a product
  // Typical: POST /v2/orders/cancel_all { product_symbol }
  const { data } = await delta.post('/v2/orders/cancel_all', { product_symbol });
  return data ?? { ok: true };
}

// Reduce-only full close for the product
async function deltaClosePosition({ product_symbol, side /* opp side of entry */, strategy_id }) {
  if (MOCK) {
    log.info({ product_symbol, side, strategy_id }, 'MOCK: CLOSE_POSITION');
    return { ok: true, mock: true };
  }
  // TODO #6: Replace with real “close position” call (reduce-only)
  // Common patterns:
  //   POST /v2/orders  (market, reduce_only=true, side=opposite)
  const payload = {
    product_symbol,
    order_type: 'market_order',
    side,
    size: 'close',             // or compute current abs(size)
    reduce_only: true,
    time_in_force: 'ioc',
    ...(strategy_id ? { strategy_id } : {}),
  };
  const { data } = await delta.post('/v2/orders', payload);
  return data ?? { ok: true };
}

// Market entry
async function deltaEnterMarket({ product_symbol, side, qty, strategy_id }) {
  if (MOCK) {
    log.info({ product_symbol, side, qty, strategy_id }, 'MOCK: ENTER MARKET');
    return { ok: true, mock: true };
  }
  // TODO #7: Replace with real market order creation
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

// Optional: place SL as bracket
async function deltaPlaceBracket({ product_symbol, side, stop_price, size, trigger = 'last_traded_price' }) {
  if (MOCK) {
    log.info({ product_symbol, side, stop_price, size, trigger }, 'MOCK: BRACKET (SL)');
    return { ok: true, mock: true };
  }
  // Example only — adapt to Delta’s bracket schema if supported
  const payload = {
    action: 'bracket',
    product_symbol,
    stop_loss_order: {
      order_type: 'stop_market_order',
      stop_price,
      reduce_only: true,
      trigger_method: trigger,
      time_in_force: 'gtc',
      size,
      side: side === 'buy' ? 'sell' : 'buy',
    },
  };
  // If bracket isn’t a single endpoint, you can submit just a stop market order
  const { data } = await delta.post('/v2/brackets', payload);
  return data ?? { ok: true };
}

// Optional: batch TPs as multiple limit orders
async function deltaPlaceBatchTPs({ product_symbol, side, legs /* [{price,size},...] */ }) {
  if (MOCK) {
    log.info({ product_symbol, side, legs }, 'MOCK: BATCH TPs');
    return { ok: true, mock: true };
  }
  // Example only — if no batch API, loop and send one-by-one
  const payload = {
    action: 'batch',
    product_symbol,
    orders: legs.map(l => ({
      order_type: 'limit_order',
      limit_price: l.price,
      size: l.size,
      side: side === 'buy' ? 'sell' : 'buy',
      reduce_only: true,
      post_only: false,
      time_in_force: 'gtc',
    })),
  };
  const { data } = await delta.post('/v2/batch/orders', payload);
  return data ?? { ok: true };
}

// =====================================================================
// 3) ********** FLAT BARRIER — exactly as we discussed
// =====================================================================

async function waitUntilFlat({ product_symbol, strategy_id },
                             { timeoutMs = FLAT_TIMEOUT_MS, intervalMs = FLAT_INTERVAL_MS } = {}) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const [orders, pos] = await Promise.all([
      deltaListOpenOrders({ product_symbol, strategy_id }).catch(e => {
        log.warn({ err: e?.message }, 'orders poll failed');
        return [];
      }),
      deltaGetPosition({ product_symbol }).catch(e => {
        log.warn({ err: e?.message }, 'position poll failed');
        return { size: 0 };
      }),
    ]);

    const noOrders = !orders || orders.length === 0;
    const flatPos  = !pos || Math.abs(Number(pos.size) || 0) <= SIZE_EPS;

    if (noOrders && flatPos) return true;
    await sleep(intervalMs);
  }
  return false;
}

async function ensureFlatThenEnter(msg, logger = log) {
  // 1) cancel
  await deltaCancelAll({ product_symbol: msg.product_symbol }).catch(e =>
    logger.warn({ err: e?.message }, 'cancel_all failed (continuing)')
  );

  // 2) close
  await deltaClosePosition({
    product_symbol: msg.product_symbol,
    side: msg.side === 'buy' ? 'sell' : 'buy',
    strategy_id: msg.strategy_id,
  }).catch(e => logger.warn({ err: e?.message }, 'close_position failed (continuing)'));

  // 3) barrier
  logger.info({ ps: msg.product_symbol, sid: msg.strategy_id }, 'waiting to become FLAT…');
  const ok = await waitUntilFlat(
    { product_symbol: msg.product_symbol, strategy_id: msg.strategy_id },
    { timeoutMs: FLAT_TIMEOUT_MS, intervalMs: FLAT_INTERVAL_MS }
  );

  if (!ok) {
    const err = { ok: false, error: 'Timeout: still not flat; entry aborted' };
    logger.error(err);
    return err; // DO NOT ENTER
  }

  // 4) safe enter
  const out = await deltaEnterMarket({
    product_symbol: msg.product_symbol,
    side: msg.side,
    qty:  msg.qty ?? msg.size,
    strategy_id: msg.strategy_id,
  });

  return out ?? { ok: true };
}

// =====================================================================
// 4) ********** Simple serial queue so webhooks never overlap
// =====================================================================
let _serial = Promise.resolve();
function runSerial(fn) {
  _serial = _serial.then(fn, fn).catch(e => log.error(e));
  return _serial;
}

// =====================================================================
// 5) ********** HTTP endpoints
// =====================================================================

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    ts: nowISO(),
    mock: MOCK,
    port: PORT,
    delta_base_url: DELTA_BASE_URL,
    has_keys: Boolean(DELTA_KEY && DELTA_SECRET && DELTA_PASSPHRASE),
  });
});

// Main webhook (from Pine)
app.post('/webhook', (req, res) => {
  const msg = req.body || {};
  // Respond immediately and process in the queue (keeps TV happy)
  res.json({ ok: true, enqueued: true });

  runSerial(async () => {
    const a = String(msg.action ?? '').toUpperCase();
    log.info({ a, msg }, 'Webhook received');

    try {
      switch (a) {
        case 'CANCEL_ALL':
        case 'DELTA_CANCEL_ALL':
          await deltaCancelAll({ product_symbol: msg.product_symbol });
          break;

        case 'CLOSE_POSITION':
          await deltaClosePosition({
            product_symbol: msg.product_symbol,
            side: msg.side === 'buy' ? 'sell' : 'buy',
            strategy_id: msg.strategy_id,
          });
          break;

        case 'ENTER':
        case 'FLIP':
          await ensureFlatThenEnter(msg, log);
          break;

        case 'DELTA_BRACKET': {
          // optional: only SL branch (as in your Pine)
          const stop_price = Number(msg?.stop_loss_order?.stop_price);
          const side = msg.side;
          const size = Number(msg.size ?? msg.qty ?? 0);
          if (isFinite(stop_price) && size > 0) {
            await deltaPlaceBracket({
              product_symbol: msg.product_symbol,
              side,
              stop_price,
              size,
              trigger: msg.bracket_stop_trigger_method ?? 'last_traded_price',
            });
          } else {
            log.warn({ msg }, 'BRACKET payload incomplete; skipping');
          }
          break;
        }

        case 'DELTA_BATCH': {
          // optional: TPs as a batch of limit orders
          const raw = Array.isArray(msg.orders) ? msg.orders : [];
          const legs = raw.map(o => ({
            price: Number(o.limit_price),
            size:  Number(o.size),
          })).filter(x => isFinite(x.price) && x.size > 0);
          if (legs.length) {
            await deltaPlaceBatchTPs({
              product_symbol: msg.product_symbol,
              side: msg.side,
              legs,
            });
          } else {
            log.warn({ msg }, 'BATCH has no valid legs; skipping');
          }
          break;
        }

        case 'EXIT': {
          // Optional single EXIT notification (from TP/SL fills) – no venue write
          log.info({ msg }, 'EXIT notice (no-op server side)');
          break;
        }

        default:
          log.warn({ a }, 'Unknown action; ignoring');
      }
    } catch (e) {
      log.error({ err: e?.message, stack: e?.stack }, 'Action failed');
    }
  });
});

// ---------- Start ----------
app.listen(PORT, () => {
  log.info({ msg: `Delta Ladder Gateway listening on :${PORT}` });
});
