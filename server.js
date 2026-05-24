require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const WS = require('ws');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || 'velox_jwt_2025';
const ADMIN_KEY = process.env.ADMIN_SECRET_KEY || 'admin123';
const TD_KEY = process.env.TWELVE_DATA_API_KEY || '';

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ── IN-MEMORY STORE ───────────────────────────────────────────
const users = new Map();
const usersById = new Map();
const positions = new Map(); // key: userId_accountType
const orders = new Map();
const deposits = new Map();
const withdrawals = new Map();

// ── PRICES ────────────────────────────────────────────────────
const prices = {
  EURUSD:1.1610,GBPUSD:1.2680,USDJPY:143.50,USDCHF:0.8950,
  AUDUSD:0.6430,USDCAD:1.3840,NZDUSD:0.5910,EURGBP:0.9160,
  EURJPY:166.50,GBPJPY:181.90,XAUUSD:3320.0,XAGUSD:33.20,
  WTIUSD:61.50,BRENTUSD:64.80,NGAS:3.45,
  BTCUSD:108500,ETHUSD:2560,BNBUSD:665,SOLUSD:178,XRPUSD:2.41,
  ADAUSD:0.775,LTCUSD:102,DOGEUSD:0.225,AVAXUSD:23.5,
  LINKUSD:16.8,DOTUSD:4.85,UNIUSD:8.90,MATICUSD:0.52,
  US30:42800,SPX500:5880,NAS100:21200,DAX40:23800,FTSE100:8620,
  NIKKEI:37800,ASX200:8250,
  AAPL:203,MSFT:449,AMZN:207,GOOGL:178,META:628,
  TSLA:342,NVDA:134,NFLX:1285,JPM:267,BAC:47,
};

// Simulation
const mom = {};
Object.keys(prices).forEach(s => mom[s] = 0);
function startSim() {
  const VOL = { BTCUSD:.0004,ETHUSD:.0004,SOLUSD:.0005,XAUUSD:.0002,
    WTIUSD:.0003,US30:.0002,EURUSD:.00005,GBPUSD:.00005,DEFAULT:.0001 };
  setInterval(() => {
    for (const s in prices) {
      const v = VOL[s] || VOL.DEFAULT;
      mom[s] = mom[s]*.85 + (Math.random()-.5)*v;
      prices[s] = parseFloat((prices[s]*(1+mom[s])).toFixed(s.includes('JPY')?3:s==='BTCUSD'||s==='NAS100'||s==='US30'?2:5));
    }
  }, 1000);
}

// Twelve Data
let tdLive = false;
function connectTD() {
  if (!TD_KEY || TD_KEY.length < 10) { startSim(); return; }
  const ws = new WS(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${TD_KEY}`);
  const syms = 'EUR/USD,GBP/USD,USD/JPY,XAU/USD,BTC/USD,ETH/USD,SOL/USD,BNB/USD,XRP/USD';
  ws.on('open', () => { tdLive=true; ws.send(JSON.stringify({action:'subscribe',params:{symbols:syms}})); });
  ws.on('message', raw => {
    try {
      const m = JSON.parse(raw);
      if (m.event==='price' && m.price > 0) {
        const map = {'EUR/USD':'EURUSD','GBP/USD':'GBPUSD','USD/JPY':'USDJPY','XAU/USD':'XAUUSD',
          'BTC/USD':'BTCUSD','ETH/USD':'ETHUSD','SOL/USD':'SOLUSD','BNB/USD':'BNBUSD','XRP/USD':'XRPUSD'};
        const sym = map[m.symbol];
        if (sym) prices[sym] = parseFloat(m.price);
      }
    } catch {}
  });
  ws.on('close', () => { tdLive=false; setTimeout(connectTD,5000); });
  ws.on('error', () => {});
}

setInterval(() => io.emit('prices', { p: prices, live: tdLive }), 500);

// ── SL/TP MONITOR ─────────────────────────────────────────────
async function checkSLTP() {
  for (const [key, posArr] of positions) {
    const [userId, acctType] = key.split('_');
    for (const pos of [...posArr]) {
      const cur = prices[pos.symbol];
      if (!cur || (!pos.sl && !pos.tp)) continue;
      let reason = null;
      if (pos.side==='buy') {
        if (pos.sl && cur<=pos.sl) reason='SL';
        if (pos.tp && cur>=pos.tp) reason='TP';
      } else {
        if (pos.sl && cur>=pos.sl) reason='SL';
        if (pos.tp && cur<=pos.tp) reason='TP';
      }
      if (reason) {
        const pnl = (cur-pos.avgPrice)*pos.quantity*(pos.side==='buy'?1:-1);
        positions.set(key, posArr.filter(p=>p.symbol!==pos.symbol));
        const u = usersById.get(userId);
        if (u) {
          if (acctType==='live') u.liveBalance = (u.liveBalance||0)+pnl;
          else u.demoBalance = (u.demoBalance||10000)+pnl;
        }
        io.to(`user:${userId}`).emit('positionClosed',{symbol:pos.symbol,reason,price:cur,pnl:pnl.toFixed(2)});
      }
    }
  }
}
setInterval(checkSLTP, 1000);

// ── AUTH ──────────────────────────────────────────────────────
function auth(req,res,next) {
  const tok = req.header('x-auth-token');
  if (!tok) return res.status(401).json({msg:'No token'});
  try { req.user = jwt.verify(tok,JWT_SECRET); next(); }
  catch { res.status(401).json({msg:'Invalid token'}); }
}
function adminAuth(req,res,next) {
  if (req.headers['admin-key']!==ADMIN_KEY) return res.status(403).json({msg:'Forbidden'});
  next();
}

// ── AUTH ROUTES ───────────────────────────────────────────────
app.post('/api/auth/register', async(req,res) => {
  try {
    const {email,password,fullName,country} = req.body;
    if (!email||!password||!fullName||!country) return res.status(400).json({msg:'All fields required'});
    if (password.length<6) return res.status(400).json({msg:'Password min 6 chars'});
    if (users.get(email.toLowerCase())) return res.status(400).json({msg:'Email already registered'});
    const id = crypto.randomUUID();
    const hashed = await bcrypt.hash(password,12);
    const u = {id,email:email.toLowerCase(),password:hashed,fullName,country,
      demoBalance:10000,liveBalance:0,activeAccount:'demo',
      kycStatus:'unverified',status:'active',createdAt:new Date().toISOString()};
    users.set(email.toLowerCase(),u); usersById.set(id,u);
    const token = jwt.sign({id,email:email.toLowerCase()},JWT_SECRET,{expiresIn:'7d'});
    res.json({token,user:{id,email:u.email,fullName,country,demoBalance:10000,liveBalance:0,activeAccount:'demo',kycStatus:'unverified'}});
  } catch(e) { res.status(500).json({msg:e.message}); }
});

app.post('/api/auth/login', async(req,res) => {
  try {
    const {email,password} = req.body;
    const u = users.get(email?.toLowerCase());
    if (!u||!await bcrypt.compare(password,u.password)) return res.status(400).json({msg:'Invalid credentials'});
    u.lastLogin = new Date().toISOString();
    const token = jwt.sign({id:u.id,email:u.email},JWT_SECRET,{expiresIn:'7d'});
    res.json({token,user:{id:u.id,email:u.email,fullName:u.fullName,country:u.country,
      demoBalance:u.demoBalance||10000,liveBalance:u.liveBalance||0,
      activeAccount:u.activeAccount||'demo',kycStatus:u.kycStatus}});
  } catch(e) { res.status(500).json({msg:e.message}); }
});

// ── ACCOUNT ROUTES ────────────────────────────────────────────
app.get('/api/account/me', auth, (req,res) => {
  const u = usersById.get(req.user.id);
  if (!u) return res.status(404).json({msg:'Not found'});
  const {password:_,...safe} = u;
  res.json(safe);
});

app.post('/api/account/switch', auth, (req,res) => {
  const {accountType} = req.body;
  if (!['demo','live'].includes(accountType)) return res.status(400).json({msg:'Invalid'});
  const u = usersById.get(req.user.id);
  if (!u) return res.status(404).json({msg:'Not found'});
  u.activeAccount = accountType;
  res.json({activeAccount:accountType,demoBalance:u.demoBalance||10000,liveBalance:u.liveBalance||0});
});

app.get('/api/account/positions', auth, (req,res) => {
  const u = usersById.get(req.user.id);
  const acct = req.query.account || u?.activeAccount || 'demo';
  res.json(positions.get(`${req.user.id}_${acct}`) || []);
});

app.get('/api/account/orders', auth, (req,res) => {
  const u = usersById.get(req.user.id);
  const acct = req.query.account || u?.activeAccount || 'demo';
  res.json((orders.get(`${req.user.id}_${acct}`) || []).slice(0,200));
});

app.post('/api/account/deposit', auth, (req,res) => {
  const {amount,method,txHash} = req.body;
  if (!amount||amount<1) return res.status(400).json({msg:'Min deposit $1'});
  const dep = {id:crypto.randomUUID(),userId:req.user.id,amount:parseFloat(amount),
    method:method||'crypto',txHash:txHash||'',status:'pending',createdAt:new Date().toISOString()};
  const arr = deposits.get(req.user.id)||[];
  arr.unshift(dep); deposits.set(req.user.id,arr);
  io.emit('adminAlert',{type:'deposit',msg:`New deposit $${amount}`});
  res.json({msg:'Deposit submitted. Pending approval.',depositId:dep.id});
});

app.post('/api/account/withdraw', auth, (req,res) => {
  const {amount,method,address} = req.body;
  if (!amount||amount<1) return res.status(400).json({msg:'Min withdrawal $1'});
  const u = usersById.get(req.user.id);
  if ((u?.liveBalance||0) < amount) return res.status(400).json({msg:'Insufficient live balance'});
  const wd = {id:crypto.randomUUID(),userId:req.user.id,amount:parseFloat(amount),
    method:method||'crypto',address:address||'',status:'pending',createdAt:new Date().toISOString()};
  const arr = withdrawals.get(req.user.id)||[];
  arr.unshift(wd); withdrawals.set(req.user.id,arr);
  res.json({msg:'Withdrawal request submitted.',withdrawalId:wd.id});
});

// ── TRADING ───────────────────────────────────────────────────
app.post('/api/trading/order', auth, (req,res) => {
  const {symbol,side,quantity,sl,tp} = req.body;
  if (!symbol||!side||!quantity||quantity<=0) return res.status(400).json({msg:'Invalid params'});
  const price = prices[symbol];
  if (!price) return res.status(400).json({msg:'Symbol not available'});
  const u = usersById.get(req.user.id);
  if (!u) return res.status(404).json({msg:'User not found'});
  const acctType = u.activeAccount||'demo';
  const bal = acctType==='live'?(u.liveBalance||0):(u.demoBalance||10000);
  const margin = price*quantity*0.002;
  if (bal < margin) return res.status(400).json({msg:'Insufficient balance'});

  const key = `${req.user.id}_${acctType}`;
  const posArr = positions.get(key)||[];
  const existing = posArr.find(p=>p.symbol===symbol);

  if (side==='buy') {
    if (acctType==='live') u.liveBalance = (u.liveBalance||0)-margin;
    else u.demoBalance = (u.demoBalance||10000)-margin;
    if (existing) {
      const totalQty = existing.quantity+quantity;
      existing.avgPrice = ((existing.avgPrice*existing.quantity)+(price*quantity))/totalQty;
      existing.quantity = totalQty;
      if (sl) existing.sl = sl;
      if (tp) existing.tp = tp;
    } else {
      posArr.push({symbol,quantity,avgPrice:price,side:'buy',sl:sl||null,tp:tp||null,openTime:new Date().toISOString()});
      positions.set(key, posArr);
    }
  } else {
    const pos = posArr.find(p=>p.symbol===symbol);
    if (!pos||pos.quantity<quantity) return res.status(400).json({msg:'No position to sell'});
    const pnl = (price-pos.avgPrice)*quantity;
    if (pos.quantity===quantity) positions.set(key, posArr.filter(p=>p.symbol!==symbol));
    else pos.quantity -= quantity;
    if (acctType==='live') u.liveBalance = (u.liveBalance||0)+margin+pnl;
    else u.demoBalance = (u.demoBalance||10000)+margin+pnl;
  }

  const orderKey = `${req.user.id}_${acctType}`;
  const orderArr = orders.get(orderKey)||[];
  orderArr.unshift({symbol,side,quantity,price,sl:sl||null,tp:tp||null,
    status:'filled',accountType:acctType,createdAt:new Date().toISOString()});
  orders.set(orderKey, orderArr);

  res.json({msg:'Order executed',price,
    demoBalance:u.demoBalance||10000,liveBalance:u.liveBalance||0,activeAccount:acctType});
});

app.post('/api/trading/close/:symbol', auth, (req,res) => {
  const u = usersById.get(req.user.id);
  if (!u) return res.status(404).json({msg:'Not found'});
  const acctType = u.activeAccount||'demo';
  const key = `${req.user.id}_${acctType}`;
  const posArr = positions.get(key)||[];
  const pos = posArr.find(p=>p.symbol===req.params.symbol);
  if (!pos) return res.status(404).json({msg:'Position not found'});
  const price = prices[req.params.symbol]||pos.avgPrice;
  const qty = req.body.quantity||pos.quantity;
  const pnl = (price-pos.avgPrice)*qty*(pos.side==='buy'?1:-1);
  if (pos.quantity<=qty) positions.set(key, posArr.filter(p=>p.symbol!==req.params.symbol));
  else pos.quantity -= qty;
  if (acctType==='live') u.liveBalance = (u.liveBalance||0)+pnl;
  else u.demoBalance = (u.demoBalance||10000)+pnl;
  const orderArr = orders.get(key)||[];
  orderArr.unshift({symbol:req.params.symbol,side:'sell',quantity:qty,price,pnl,
    status:'closed',closeReason:'manual',accountType:acctType,createdAt:new Date().toISOString()});
  orders.set(key, orderArr);
  res.json({msg:'Closed',pnl:pnl.toFixed(2),demoBalance:u.demoBalance||10000,liveBalance:u.liveBalance||0});
});

// ── ADMIN ─────────────────────────────────────────────────────
app.get('/api/admin/stats', adminAuth, (req,res) => {
  const allUsers = [...usersById.values()];
  const allDeps = [...deposits.values()].flat();
  const allWds = [...withdrawals.values()].flat();
  res.json({
    users:allUsers.length,
    verified:allUsers.filter(u=>u.kycStatus==='verified').length,
    pendingKyc:allUsers.filter(u=>u.kycStatus==='pending').length,
    pendingDeposits:allDeps.filter(d=>d.status==='pending').length,
    pendingWithdrawals:allWds.filter(w=>w.status==='pending').length,
    totalLiveBalance:allUsers.reduce((s,u)=>s+(u.liveBalance||0),0),
    live:tdLive,uptime:Math.floor(process.uptime())
  });
});

app.get('/api/admin/users', adminAuth, (req,res) => {
  res.json([...usersById.values()].map(({password:_,...u})=>u));
});

app.get('/api/admin/deposits', adminAuth, (req,res) => {
  const all = [...deposits.values()].flat().sort((a,b)=>new Date(b.createdAt)-new Date(a.createdAt));
  res.json(all.map(d=>({...d,userName:usersById.get(d.userId)?.fullName||'—',userEmail:usersById.get(d.userId)?.email||'—'})));
});

app.post('/api/admin/deposits/:id/approve', adminAuth, (req,res) => {
  for (const [uid, arr] of deposits) {
    const dep = arr.find(d=>d.id===req.params.id);
    if (dep) {
      dep.status='approved'; dep.reviewedAt=new Date().toISOString();
      const u = usersById.get(dep.userId);
      if (u) { u.liveBalance=(u.liveBalance||0)+dep.amount; }
      io.to(`user:${dep.userId}`).emit('depositApproved',{amount:dep.amount,balance:u?.liveBalance});
      return res.json({msg:'Approved',newBalance:u?.liveBalance});
    }
  }
  res.status(404).json({msg:'Not found'});
});

app.post('/api/admin/deposits/:id/reject', adminAuth, (req,res) => {
  for (const [uid, arr] of deposits) {
    const dep = arr.find(d=>d.id===req.params.id);
    if (dep) { dep.status='rejected'; return res.json({msg:'Rejected'}); }
  }
  res.status(404).json({msg:'Not found'});
});

app.get('/api/admin/withdrawals', adminAuth, (req,res) => {
  const all = [...withdrawals.values()].flat().sort((a,b)=>new Date(b.createdAt)-new Date(a.createdAt));
  res.json(all.map(w=>({...w,userName:usersById.get(w.userId)?.fullName||'—',userEmail:usersById.get(w.userId)?.email||'—'})));
});

app.post('/api/admin/withdrawals/:id/process', adminAuth, (req,res) => {
  for (const [uid, arr] of withdrawals) {
    const wd = arr.find(w=>w.id===req.params.id);
    if (wd) {
      wd.status='processed'; wd.processedAt=new Date().toISOString();
      const u = usersById.get(wd.userId);
      if (u) u.liveBalance=Math.max(0,(u.liveBalance||0)-wd.amount);
      return res.json({msg:'Processed'});
    }
  }
  res.status(404).json({msg:'Not found'});
});

app.patch('/api/admin/users/:id/kyc', adminAuth, (req,res) => {
  const u = usersById.get(req.params.id);
  if (!u) return res.status(404).json({msg:'Not found'});
  u.kycStatus = req.body.status;
  io.to(`user:${req.params.id}`).emit('kycUpdated',{status:req.body.status});
  res.json({ok:true});
});

app.patch('/api/admin/users/:id/status', adminAuth, (req,res) => {
  const u = usersById.get(req.params.id);
  if (!u) return res.status(404).json({msg:'Not found'});
  u.status = req.body.status;
  res.json({ok:true});
});

app.post('/api/admin/users/:id/balance', adminAuth, (req,res) => {
  const u = usersById.get(req.params.id);
  if (!u) return res.status(404).json({msg:'Not found'});
  const {amount,accountType} = req.body;
  if (accountType==='live') u.liveBalance=(u.liveBalance||0)+parseFloat(amount||0);
  else u.demoBalance=(u.demoBalance||10000)+parseFloat(amount||0);
  res.json({ok:true,liveBalance:u.liveBalance,demoBalance:u.demoBalance});
});

app.get('/api/admin/positions', adminAuth, (req,res) => {
  const all = [];
  for (const [key,posArr] of positions) {
    const [uid,acct] = key.split('_');
    const u = usersById.get(uid);
    for (const p of posArr) {
      const cur = prices[p.symbol]||p.avgPrice;
      const pnl = (cur-p.avgPrice)*p.quantity*(p.side==='buy'?1:-1);
      all.push({...p,currentPrice:cur,pnl:pnl.toFixed(2),accountType:acct,
        userName:u?.fullName||'—',userEmail:u?.email||'—'});
    }
  }
  res.json(all);
});

app.get('/api/markets', (req,res) => res.json({prices,live:tdLive}));
app.get('/api/health', (req,res) => res.json({ok:true,live:tdLive,uptime:Math.floor(process.uptime())}));

// ── SOCKET ────────────────────────────────────────────────────
io.on('connection', socket => {
  const tok = socket.handshake.auth?.token;
  if (tok) { try { const d=jwt.verify(tok,JWT_SECRET); socket.join(`user:${d.id}`); } catch {} }
  socket.emit('prices',{p:prices,live:tdLive});
});

// ── START ─────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n🚀 Velox on port ${PORT}`);
  setTimeout(connectTD, 500);
});

// ── PRICE PROXY (Yahoo Finance + CoinGecko via server) ────────
const https = require('https');
function httpsGet(url) {
  return new Promise((res, rej) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      }
    }, (r) => {
      let data = '';
      r.on('data', c => data += c);
      r.on('end', () => {
        try { res(JSON.parse(data)); } catch { rej(new Error('Parse error')); }
      });
    });
    req.on('error', rej);
    req.setTimeout(8000, () => { req.destroy(); rej(new Error('Timeout')); });
  });
}

// Batch price fetch — called by client
app.get('/api/prices/yahoo', async (req, res) => {
  const symbols = (req.query.symbols || '').split(',').filter(Boolean).slice(0, 40);
  const results = {};
  await Promise.allSettled(symbols.map(async (yt) => {
    try {
      const d = await httpsGet(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yt)}?interval=1m&range=1d`);
      const meta = d?.chart?.result?.[0]?.meta;
      if (meta?.regularMarketPrice > 0) results[yt] = meta.regularMarketPrice;
    } catch {}
  }));
  res.json(results);
});

// OHLC for chart
app.get('/api/chart/ohlc', async (req, res) => {
  const { symbol, interval, range } = req.query;
  if (!symbol) return res.status(400).json({ error: 'symbol required' });
  try {
    const d = await httpsGet(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${interval||'1h'}&range=${range||'5d'}`);
    const r = d?.chart?.result?.[0];
    if (!r) return res.status(404).json({ error: 'No data' });
    const ts = r.timestamp;
    const q  = r.indicators.quote[0];
    const candles = [];
    for (let i = 0; i < ts.length; i++) {
      if (q.open[i] == null) continue;
      candles.push({ t: ts[i], o: q.open[i], h: q.high[i], l: q.low[i], c: q.close[i] });
    }
    res.json({ candles, price: r.meta?.regularMarketPrice });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// CoinGecko proxy
app.get('/api/prices/crypto', async (req, res) => {
  try {
    const d = await httpsGet('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,binancecoin,solana,ripple,cardano,litecoin,dogecoin,avalanche-2,chainlink,polkadot,uniswap&vs_currencies=usd');
    res.json(d);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
