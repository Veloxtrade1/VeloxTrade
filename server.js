require('dotenv').config();
const express   = require('express');
const http      = require('http');
const {Server}  = require('socket.io');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const bcrypt    = require('bcryptjs');
const jwt       = require('jsonwebtoken');
const crypto    = require('crypto');
const WS        = require('ws');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {cors:{origin:'*'}});

const PORT       = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || 'velox_jwt_2025_secure';
const ADMIN_KEY  = process.env.ADMIN_SECRET_KEY || 'admin123';
const TD_KEY     = process.env.TWELVE_DATA_API_KEY || '';

app.use(helmet({contentSecurityPolicy:false}));
app.use(cors());
app.use(express.json());
app.use(express.static('public'));
app.use('/api/', rateLimit({windowMs:15*60*1000, max:500}));

// ── IN-MEMORY STORE ───────────────────────────────────────────
const store = {
  users: new Map(), usersById: new Map(),
  positions: new Map(), orders: new Map(), transactions: new Map()
};
let useDB = false, mongoose = null;

async function tryConnectDB() {
  const uri = process.env.MONGO_URI;
  if (!uri || uri.length < 20) { console.log('⚡ Memory mode'); return; }
  try {
    mongoose = require('mongoose');
    await mongoose.connect(uri, {serverSelectionTimeoutMS:8000});
    const uSchema = new mongoose.Schema({
      id:String, email:{type:String,unique:true}, password:String,
      fullName:String, country:String, balance:{type:Number,default:0},
      kycStatus:{type:String,default:'unverified'}, createdAt:{type:Date,default:Date.now}
    });
    const pSchema = new mongoose.Schema({
      userId:String, symbol:String, quantity:Number, avgPrice:Number,
      side:String, sl:Number, tp:Number, openTime:{type:Date,default:Date.now}
    });
    const oSchema = new mongoose.Schema({
      userId:String, symbol:String, side:String, quantity:Number,
      price:Number, sl:Number, tp:Number, status:{type:String,default:'filled'},
      closeReason:String, createdAt:{type:Date,default:Date.now}
    });
    try { mongoose.model('User'); } catch { mongoose.model('User', uSchema); }
    try { mongoose.model('Position'); } catch { mongoose.model('Position', pSchema); }
    try { mongoose.model('Order'); } catch { mongoose.model('Order', oSchema); }
    useDB = true;
    console.log('✅ MongoDB connected');
  } catch(e) {
    console.log('⚡ MongoDB failed:', e.message.slice(0,50));
    useDB = false;
  }
}

// DB helpers
async function dbFindUser(email) {
  if (useDB) return mongoose.model('User').findOne({email:email.toLowerCase()});
  return store.users.get(email.toLowerCase())||null;
}
async function dbFindUserById(id) {
  if (useDB) return mongoose.model('User').findOne({id});
  return store.usersById.get(id)||null;
}
async function dbCreateUser(d) {
  if (useDB) { const u=new (mongoose.model('User'))(d); await u.save(); return u; }
  store.users.set(d.email.toLowerCase(),d); store.usersById.set(d.id,d); return d;
}
async function dbUpdateBalance(id, bal) {
  if (useDB) { await mongoose.model('User').updateOne({id},{balance:bal}); return; }
  const u=store.usersById.get(id); if(u) u.balance=bal;
}
async function dbGetPositions(userId) {
  if (useDB) return mongoose.model('Position').find({userId});
  return store.positions.get(userId)||[];
}
async function dbSavePosition(pos) {
  if (useDB) {
    await mongoose.model('Position').updateOne({userId:pos.userId,symbol:pos.symbol},pos,{upsert:true});
    return;
  }
  const arr=store.positions.get(pos.userId)||[];
  const i=arr.findIndex(p=>p.symbol===pos.symbol);
  if(i>=0) arr[i]=pos; else arr.push(pos);
  store.positions.set(pos.userId,arr);
}
async function dbRemovePosition(userId, symbol) {
  if (useDB) { await mongoose.model('Position').deleteOne({userId,symbol}); return; }
  store.positions.set(userId,(store.positions.get(userId)||[]).filter(p=>p.symbol!==symbol));
}
async function dbSaveOrder(ord) {
  if (useDB) { await new (mongoose.model('Order'))(ord).save(); return ord; }
  const arr=store.orders.get(ord.userId)||[];
  arr.unshift(ord); store.orders.set(ord.userId,arr); return ord;
}
async function dbGetOrders(userId) {
  if (useDB) return mongoose.model('Order').find({userId}).sort('-createdAt').limit(100);
  return (store.orders.get(userId)||[]).slice(0,100);
}
async function dbAllUsers() {
  if (useDB) return mongoose.model('User').find().select('-password');
  return [...store.usersById.values()].map(({password:_,...u})=>u);
}

// ── PRICES — seed with real May 2026 values ───────────────────
const prices = {
  EURUSD:1.1610,GBPUSD:1.2680,USDJPY:143.50,USDCHF:0.8950,AUDUSD:0.6430,
  USDCAD:1.3840,NZDUSD:0.5910,EURGBP:0.9160,EURJPY:166.50,GBPJPY:181.90,
  XAUUSD:3320.0,XAGUSD:33.20,WTIUSD:61.50,BRENTUSD:64.80,NGAS:3.45,
  BTCUSD:108500,ETHUSD:2560.0,BNBUSD:665.0,SOLUSD:178.0,XRPUSD:2.41,
  ADAUSD:0.775,LTCUSD:102.0,DOGEUSD:0.225,AVAXUSD:23.5,MATICUSD:0.52,
  LINKUSD:16.8,DOTUSD:4.85,UNIUSD:8.90,AAVEUSD:195.0,
  US30:42800,SPX500:5880,NAS100:21200,DAX40:23800,FTSE100:8620,
  NIKKEI:37800,ASX200:8250,CAC40:7860,STOXX50:5420,
  AAPL:203.0,MSFT:449.0,AMZN:207.0,GOOGL:178.0,META:628.0,
  TSLA:342.0,NVDA:134.0,NFLX:1285.0,BABA:95.0,JPM:267.0,
  BAC:47.0,WMT:105.0,JNJ:157.0,V:354.0,PG:170.0,
  XOM:106.0,CVX:152.0,PFE:24.5,KO:73.0,DIS:105.0,
};

// Twelve Data symbol map
const TD_MAP = {
  EURUSD:'EUR/USD',GBPUSD:'GBP/USD',USDJPY:'USD/JPY',USDCHF:'USD/CHF',
  AUDUSD:'AUD/USD',USDCAD:'USD/CAD',NZDUSD:'NZD/USD',EURGBP:'EUR/GBP',
  EURJPY:'EUR/JPY',GBPJPY:'GBP/JPY',
  XAUUSD:'XAU/USD',XAGUSD:'XAG/USD',
  BTCUSD:'BTC/USD',ETHUSD:'ETH/USD',BNBUSD:'BNB/USD',SOLUSD:'SOL/USD',
  XRPUSD:'XRP/USD',ADAUSD:'ADA/USD',LTCUSD:'LTC/USD',DOGEUSD:'DOGE/USD',
  AVAXUSD:'AVAX/USD',LINKUSD:'LINK/USD',DOTUSD:'DOT/USD',
  AAPL:'AAPL',MSFT:'MSFT',AMZN:'AMZN',GOOGL:'GOOGL',META:'META',
  TSLA:'TSLA',NVDA:'NVDA',NFLX:'NFLX',JPM:'JPM',BAC:'BAC',
  WMT:'WMT',JNJ:'JNJ',V:'V',PG:'PG',XOM:'XOM',
  CVX:'CVX',PFE:'PFE',KO:'KO',DIS:'DIS',
};
const TD_REV = {};
Object.entries(TD_MAP).forEach(([k,v])=>{TD_REV[v]=k;});

let tdConnected = false;
let tdWs = null;

// ── TWELVE DATA WEBSOCKET ─────────────────────────────────────
function connectTD() {
  if (!TD_KEY || TD_KEY.length < 10) { startSim(); return; }
  console.log('🔌 Connecting Twelve Data WebSocket...');
  tdWs = new WS(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${TD_KEY}`);

  tdWs.on('open', () => {
    tdConnected = true;
    console.log('✅ Twelve Data WebSocket connected — REAL prices active');
    const syms = Object.values(TD_MAP).join(',');
    tdWs.send(JSON.stringify({action:'subscribe',params:{symbols:syms}}));
    console.log(`📡 Subscribed to ${Object.values(TD_MAP).length} symbols`);
  });

  tdWs.on('message', raw => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.event === 'price') {
        const sym = TD_REV[msg.symbol];
        if (sym && msg.price) {
          const p = parseFloat(msg.price);
          if (p > 0) prices[sym] = p;
        }
      }
    } catch {}
  });

  tdWs.on('error', e => console.log('TD WS error:', e.message.slice(0,60)));
  tdWs.on('close', () => {
    tdConnected = false;
    console.log('🔄 TD WS closed — reconnecting in 5s');
    setTimeout(connectTD, 5000);
  });
}

// REST fallback for indices
function fetchTDRest(symbols) {
  if (!TD_KEY) return;
  const https = require('https');
  const url = `https://api.twelvedata.com/price?symbol=${encodeURIComponent(symbols)}&apikey=${TD_KEY}`;
  https.get(url, res => {
    let d=''; res.on('data',c=>d+=c);
    res.on('end',()=>{
      try {
        const j=JSON.parse(d);
        const proc=(sym,obj)=>{
          const internal=TD_REV[sym]||sym;
          if(obj&&obj.price&&parseFloat(obj.price)>0) prices[internal]=parseFloat(obj.price);
        };
        if(j.price) proc(symbols,j);
        else Object.entries(j).forEach(([s,o])=>proc(s,o));
      } catch {}
    });
  }).on('error',()=>{}).setTimeout(8000);
}

// Fetch indices every 2 minutes
setInterval(()=>fetchTDRest('DJI,SPX,NDX,DAX,FTSE'), 2*60*1000);
setTimeout(()=>fetchTDRest('DJI,SPX,NDX,DAX,FTSE'), 3000);

// ── SIMULATION FALLBACK ───────────────────────────────────────
function startSim() {
  console.log('🎲 Simulation mode (add TWELVE_DATA_API_KEY for real prices)');
  const VOL={BTCUSD:.003,ETHUSD:.003,SOLUSD:.004,XAUUSD:.0015,WTIUSD:.002,US30:.001,EURUSD:.0004,GBPUSD:.0005};
  const mom={};
  Object.keys(prices).forEach(s=>mom[s]=0);
  setInterval(()=>{
    for(const s in prices){
      const v=VOL[s]||.0005;
      mom[s]=mom[s]*.88+(Math.random()-.499)*v*.2;
      prices[s]*=(1+mom[s]);
    }
  },800);
}

// ── SL/TP MONITOR — checks every second ──────────────────────
async function checkSLTP() {
  const allUsers = await dbAllUsers();
  for (const user of allUsers) {
    const positions = await dbGetPositions(user.id);
    for (const pos of positions) {
      const cur = prices[pos.symbol];
      if (!cur || (!pos.sl && !pos.tp)) continue;
      let closeReason = null;
      if (pos.side === 'buy') {
        if (pos.sl && cur <= pos.sl) closeReason = 'SL';
        if (pos.tp && cur >= pos.tp) closeReason = 'TP';
      } else {
        if (pos.sl && cur >= pos.sl) closeReason = 'SL';
        if (pos.tp && cur <= pos.tp) closeReason = 'TP';
      }
      if (closeReason) {
        // Close position
        await dbRemovePosition(user.id, pos.symbol);
        const pnl = (cur - pos.avgPrice) * pos.quantity * (pos.side === 'buy' ? 1 : -1);
        const newBal = (user.balance || 0) + pnl;
        await dbUpdateBalance(user.id, newBal);
        await dbSaveOrder({
          userId:user.id, symbol:pos.symbol, side:'sell',
          quantity:pos.quantity, price:cur,
          sl:pos.sl, tp:pos.tp,
          status:'closed', closeReason,
          createdAt:new Date().toISOString()
        });
        // Push notification to client
        io.to(`user:${user.id}`).emit('positionClosed', {
          symbol:pos.symbol, reason:closeReason, price:cur,
          pnl:pnl.toFixed(2), balance:newBal.toFixed(2)
        });
        console.log(`🔔 ${closeReason} triggered: ${user.email} ${pos.symbol} @ ${cur} PnL=$${pnl.toFixed(2)}`);
      }
    }
  }
}
setInterval(checkSLTP, 1000);

// ── BROADCAST PRICES every 500ms ─────────────────────────────
setInterval(()=>{
  io.emit('prices', {p:prices, live:tdConnected, ts:Date.now()});
}, 500);

// ── AUTH MIDDLEWARE ───────────────────────────────────────────
function auth(req,res,next){
  const tok=req.header('x-auth-token')||(req.headers.authorization||'').split(' ')[1];
  if(!tok) return res.status(401).json({msg:'No token'});
  try{req.user=jwt.verify(tok,JWT_SECRET);next();}
  catch{res.status(401).json({msg:'Invalid token'});}
}
function adminAuth(req,res,next){
  if(req.headers['admin-key']!==ADMIN_KEY) return res.status(403).json({msg:'Forbidden'});
  next();
}

// ── AUTH ROUTES ───────────────────────────────────────────────
app.post('/api/auth/register', async(req,res)=>{
  try{
    const{email,password,fullName,country}=req.body;
    if(!email||!password||!fullName||!country) return res.status(400).json({msg:'All fields required'});
    if(password.length<8) return res.status(400).json({msg:'Password min 8 characters'});
    if(await dbFindUser(email)) return res.status(400).json({msg:'Email already registered'});
    const id=crypto.randomUUID();
    const hashed=await bcrypt.hash(password,12);
    await dbCreateUser({id,email:email.toLowerCase(),password:hashed,fullName,country,balance:10000,kycStatus:'unverified',createdAt:new Date().toISOString()});
    const token=jwt.sign({id,email:email.toLowerCase()},JWT_SECRET,{expiresIn:'7d'});
    res.json({token,user:{id,email,fullName,country,balance:10000,kycStatus:'unverified'}});
  }catch(e){res.status(500).json({msg:e.message});}
});

app.post('/api/auth/login', async(req,res)=>{
  try{
    const{email,password}=req.body;
    if(!email||!password) return res.status(400).json({msg:'Email and password required'});
    const user=await dbFindUser(email);
    if(!user||!await bcrypt.compare(password,user.password)) return res.status(400).json({msg:'Invalid credentials'});
    const token=jwt.sign({id:user.id,email:user.email},JWT_SECRET,{expiresIn:'7d'});
    res.json({token,user:{id:user.id,email:user.email,fullName:user.fullName,country:user.country,balance:user.balance,kycStatus:user.kycStatus}});
  }catch(e){res.status(500).json({msg:e.message});}
});

// ── ACCOUNT ROUTES ────────────────────────────────────────────
app.get('/api/account/me', auth, async(req,res)=>{
  const u=await dbFindUserById(req.user.id);
  if(!u) return res.status(404).json({msg:'Not found'});
  const{password:_,...safe}=u; res.json(safe);
});
app.get('/api/account/positions', auth, async(req,res)=>res.json(await dbGetPositions(req.user.id)));
app.get('/api/account/orders', auth, async(req,res)=>res.json(await dbGetOrders(req.user.id)));

app.post('/api/account/deposit', auth, async(req,res)=>{
  try{
    const{amount,method,txHash}=req.body;
    const u=await dbFindUserById(req.user.id);
    const newBal=(u.balance||0)+parseFloat(amount);
    await dbUpdateBalance(req.user.id,newBal);
    res.json({msg:'Deposit confirmed',balance:newBal});
  }catch(e){res.status(500).json({msg:e.message});}
});

// ── TRADING ROUTES ────────────────────────────────────────────
app.post('/api/trading/order', auth, async(req,res)=>{
  try{
    const{symbol,side,quantity,sl,tp}=req.body;
    if(!symbol||!side||!quantity||quantity<=0) return res.status(400).json({msg:'Invalid parameters'});
    const price=prices[symbol];
    if(!price) return res.status(400).json({msg:'Symbol not available'});
    const user=await dbFindUserById(req.user.id);
    const margin=price*quantity*0.002; // 0.2% margin

    if(side==='buy'){
      if((user.balance||0)<margin) return res.status(400).json({msg:'Insufficient balance'});
      await dbUpdateBalance(req.user.id,(user.balance||0)-margin);
      const positions=await dbGetPositions(req.user.id);
      const existing=positions.find(p=>p.symbol===symbol);
      if(existing){
        const totalQty=existing.quantity+quantity;
        const avgPrice=((existing.avgPrice*existing.quantity)+(price*quantity))/totalQty;
        await dbSavePosition({...existing,quantity:totalQty,avgPrice,sl:sl||existing.sl,tp:tp||existing.tp});
      } else {
        await dbSavePosition({userId:req.user.id,symbol,quantity,avgPrice:price,side:'buy',sl:sl||null,tp:tp||null,openTime:new Date().toISOString()});
      }
    } else {
      const positions=await dbGetPositions(req.user.id);
      const pos=positions.find(p=>p.symbol===symbol);
      if(!pos||pos.quantity<quantity) return res.status(400).json({msg:'Insufficient position'});
      const pnl=(price-pos.avgPrice)*quantity;
      if(pos.quantity===quantity) await dbRemovePosition(req.user.id,symbol);
      else await dbSavePosition({...pos,quantity:pos.quantity-quantity});
      await dbUpdateBalance(req.user.id,(user.balance||0)+margin+pnl);
    }
    await dbSaveOrder({userId:req.user.id,symbol,side,quantity,price,sl:sl||null,tp:tp||null,status:'filled',createdAt:new Date().toISOString()});
    const updated=await dbFindUserById(req.user.id);
    res.json({msg:'Order executed',balance:updated.balance,price});
  }catch(e){res.status(500).json({msg:e.message});}
});

app.post('/api/trading/close/:symbol', auth, async(req,res)=>{
  try{
    const{symbol}=req.params;
    const{quantity}=req.body;
    const positions=await dbGetPositions(req.user.id);
    const pos=positions.find(p=>p.symbol===symbol);
    if(!pos) return res.status(404).json({msg:'Position not found'});
    const price=prices[symbol]||pos.avgPrice;
    const qty=quantity||pos.quantity;
    const pnl=(price-pos.avgPrice)*qty;
    if(pos.quantity<=qty) await dbRemovePosition(req.user.id,symbol);
    else await dbSavePosition({...pos,quantity:pos.quantity-qty});
    const user=await dbFindUserById(req.user.id);
    const newBal=(user.balance||0)+pnl;
    await dbUpdateBalance(req.user.id,newBal);
    await dbSaveOrder({userId:req.user.id,symbol,side:'sell',quantity:qty,price,status:'closed',closeReason:'manual',createdAt:new Date().toISOString()});
    res.json({msg:'Position closed',pnl:pnl.toFixed(2),balance:newBal});
  }catch(e){res.status(500).json({msg:e.message});}
});

// ── ADMIN ROUTES ──────────────────────────────────────────────
app.get('/api/admin/stats', adminAuth, async(req,res)=>{
  const users=await dbAllUsers();
  const totalBal=users.reduce((s,u)=>s+(u.balance||0),0);
  res.json({users:users.length,totalBalance:totalBal,live:tdConnected,db:useDB?'MongoDB':'Memory',symbols:Object.keys(prices).length,uptime:Math.floor(process.uptime())});
});
app.get('/api/admin/users', adminAuth, async(req,res)=>res.json(await dbAllUsers()));
app.post('/api/admin/user/:id/balance', adminAuth, async(req,res)=>{
  const u=await dbFindUserById(req.params.id);
  if(!u) return res.status(404).json({msg:'Not found'});
  const newBal=(u.balance||0)+parseFloat(req.body.amount||0);
  await dbUpdateBalance(req.params.id,newBal);
  res.json({balance:newBal});
});
app.patch('/api/admin/user/:id/kyc', adminAuth, async(req,res)=>{
  if(useDB){
    await mongoose.model('User').updateOne({id:req.params.id},{kycStatus:req.body.status});
  } else {
    const u=store.usersById.get(req.params.id);
    if(u) u.kycStatus=req.body.status;
  }
  res.json({ok:true});
});

// ── MARKETS API ───────────────────────────────────────────────
app.get('/api/markets', (req,res)=>res.json({prices,live:tdConnected,ts:Date.now()}));

// ── HEALTH ────────────────────────────────────────────────────
app.get('/health',(req,res)=>res.json({ok:true}));
app.get('/api/health',(req,res)=>res.json({ok:true,live:tdConnected,db:useDB?'mongodb':'memory',uptime:Math.floor(process.uptime())}));

// ── SOCKET.IO ─────────────────────────────────────────────────
io.on('connection', socket => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;
  let userId = null;
  if (token) {
    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      userId = decoded.id;
      socket.join(`user:${userId}`);
    } catch {}
  }
  // Send initial prices immediately
  socket.emit('prices', {p:prices, live:tdConnected, ts:Date.now()});
  socket.on('disconnect', ()=>{});
});

// ── START ─────────────────────────────────────────────────────
tryConnectDB().then(()=>{
  server.listen(PORT, ()=>{
    console.log(`\n🚀 Velox v4.0 on port ${PORT}`);
    console.log(`   DB:  ${useDB?'✅ MongoDB':'⚡ Memory'}`);
    console.log(`   API: ${TD_KEY?'✅ Twelve Data':'⚠️  Simulation'}\n`);
    setTimeout(()=>{ TD_KEY&&TD_KEY.length>10 ? connectTD() : startSim(); }, 500);
  });
}).catch(()=>{
  server.listen(PORT);
  TD_KEY&&TD_KEY.length>10 ? connectTD() : startSim();
});
