require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const fetch = global.fetch; // Node 18+

const app = express();

// Accept JSON, urlencoded and raw string; coerce to object
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

const API_KEY   = process.env.DELTA_API_KEY;
const API_SECRET= process.env.DELTA_API_SECRET;
const BASE_URL  = (process.env.DELTA_BASE_URL || 'https://api.india.delta.exchange').replace(/\/+$/,'');
const PORT      = process.env.PORT || 3000;

function nowTsSec(){ return Math.floor(Date.now()/1000).toString(); }
function toProductSymbol(sym){
  if(!sym) return sym;
  let s = String(sym).replace('.P','');
  if(s.includes(':')) s = s.split(':').pop();
  return s;
}
async function dcall(method, path, payload=null, query=''){
  const body = payload ? JSON.stringify(payload) : '';
  const ts = nowTsSec();
  const prehash = method + ts + path + (query||'') + body;
  const signature = crypto.createHmac('sha256', API_SECRET).update(prehash).digest('hex');
  const url = BASE_URL + path + (query||'');
  const headers = {
    'Content-Type':'application/json','Accept':'application/json',
    'api-key':API_KEY,'signature':signature,'timestamp':ts,'User-Agent':'tv-relay-node'
  };
  const res = await fetch(url,{method,headers,body: body || undefined});
  const text = await res.text(); let json;
  try{ json = JSON.parse(text); }catch{ json = { raw:text }; }
  if(!res.ok || json?.success===false) throw new Error('Delta API error: '+JSON.stringify({url,status:res.status,json}));
  return json;
}
async function placeEntry(m){
  const side = (m.side||'').toLowerCase()==='buy'?'buy':'sell';
  const qty = parseInt(m.qty,10); if(!qty||qty<1) throw new Error('qty must be integer >=1');
  return dcall('POST','/v2/orders',{product_symbol: toProductSymbol(m.symbol), order_type:'market_order', side, size: qty});
}
async function placeBracket(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = toProductSymbol(m.product_symbol);
  return dcall('POST','/v2/orders/bracket', body);
}
async function placeBatch(m){
  const {action, ...body} = m;
  if(!body.product_symbol && !body.product_id) body.product_symbol = toProductSymbol(m.product_symbol);
  return dcall('POST','/v2/orders/batch', body);
}

app.get('/health', (_req,res)=>res.json({ok:true}));

app.post('/tv', async (req, res) => {
  const raw = (typeof req.body === 'string') ? req.body : JSON.stringify(req.body);
  console.log('\n=== INCOMING /tv ===\n', raw);
  try{
    const msg = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body||{});
    if (msg.stop_loss_order || msg.take_profit_order) {
      console.log('→ FORWARD: /v2/orders/bracket');
      const r = await placeBracket(msg);
      console.log('✔ BRACKET OK:', JSON.stringify(r));
      return res.json({ok:true, step:'bracket', r});
    }
    if (msg.orders) {
      console.log('→ FORWARD: /v2/orders/batch');
      const r = await placeBatch(msg);
      console.log('✔ BATCH OK:', JSON.stringify(r));
      return res.json({ok:true, step:'batch', r});
    }
    console.log('→ FORWARD: /v2/orders (entry)');
    const r = await placeEntry(msg);
    console.log('✔ ENTRY OK:', JSON.stringify(r));
    return res.json({ok:true, step:'entry', r});
  }catch(e){
    console.error('✖ ERROR:', e);
    return res.status(400).json({ok:false, error:String(e)});
  }
});
app.listen(PORT, ()=>console.log(`Relay listening http://localhost:${PORT}`));
