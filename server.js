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
  // positions/orders stored per account: key = userId_accountType
  positions: new Map(), orders: new Map(),
  deposits: new Map(),    // userId -> deposits[]
  withdrawals: new Map(), // userId -> withdrawals[]
};
let useDB = false, mongoose = null;

async function tryConnectDB() {
  const uri = process.env.MONGO_URI;
  if (!uri || uri.length < 20) { console.log('⚡ Memory mode'); return; }
  try {
    mongoose = require('mongoose');
    await mongoose.connect(uri, {serverSelectionTimeoutMS:8000});
    // User schema — has both demo and live balances + account type
    const uSchema = new mongoose.Schema({
      id:String, email:{type:String,unique:true}, password:String,
      fullName:String, country:String, phone:String,
      demoBalance:{type:Number,default:10000},
      liveBalance:{type:Number,default:0},
      activeAccount:{type:String,default:'demo'}, // 'demo' or 'live'
      kycStatus:{type:String,default:'unverified'},
      kycSubmittedAt:Date,
      createdAt:{type:Date,default:Date.now},
      lastLogin:Date,
      status:{type:String,default:'active'}, // 'active','suspended'
    });
    const pSchema = new mongoose.Schema({
      userId:String, accountType:String,
      symbol:String, quantity:Number, avgPrice:Number,
      side:String, sl:Number, tp:Number,
      openTime:{type:Date,default:Date.now},
      notional:Number,
    });
    const oSchema = new mongoose.Schema({
      userId:String, accountType:String,
      symbol:String, side:String, quantity:Number,
      price:Number, sl:Number, tp:Number,
      pnl:Number, balanceAfter:Number,
      status:{type:String,default:'filled'},
      closeReason:String,
      createdAt:{type:Date,default:Date.now}
    });
    const depSchema = new mongoose.Schema({
      userId:String, amount:Number, method:String,
      txHash:String, status:{type:String,default:'pending'},
      reviewedAt:Date, createdAt:{type:Date,default:Date.now}
    });
    const wdSchema = new mongoose.Schema({
      userId:String, amount:Number, method:String,
      address:String, status:{type:String,default:'pending'},
      processedAt:Date, createdAt:{type:Date,default:Date.now}
    });
    try{mongoose.model('User');}catch{mongoose.model('User',uSchema);}
    try{mongoose.model('Position');}catch{mongoose.model('Position',pSchema);}
    try{mongoose.model('Order');}catch{mongoose.model('Order',oSchema);}
    try{mongoose.model('Deposit');}catch{mongoose.model('Deposit',depSchema);}
    try{mongoose.model('Withdrawal');}catch{mongoose.model('Withdrawal',wdSchema);}
    useDB = true;
    console.log('✅ MongoDB connected');
  } catch(e) {
    console.log('⚡ MongoDB failed:', e.message.slice(0,60));
    useDB = false;
  }
}

// ── DB HELPERS ────────────────────────────────────────────────
const posKey = (uid, acct) => `${uid}_${acct}`;

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
async function dbUpdateUser(id, fields) {
  if (useDB) { await mongoose.model('User').updateOne({id}, fields); return; }
  const u=store.usersById.get(id);
  if(u) Object.assign(u, fields);
}
async function dbGetBalance(user) {
  return user.activeAccount==='live' ? (user.liveBalance||0) : (user.demoBalance||10000);
}
async function dbUpdateBalance(id, bal, acctType) {
  const field = acctType==='live' ? 'liveBalance' : 'demoBalance';
  if (useDB) { await mongoose.model('User').updateOne({id},{[field]:bal}); return; }
  const u=store.usersById.get(id);
  if(u) u[field]=bal;
}
async function dbGetPositions(userId, acctType) {
  if (useDB) return mongoose.model('Position').find({userId,accountType:acctType});
  return store.positions.get(posKey(userId,acctType))||[];
}
async function dbSavePosition(pos) {
  if (useDB) {
    await mongoose.model('Position').updateOne({userId:pos.userId,symbol:pos.symbol,accountType:pos.accountType},pos,{upsert:true});
    return;
  }
  const k=posKey(pos.userId,pos.accountType);
  const arr=store.positions.get(k)||[];
  const i=arr.findIndex(p=>p.symbol===pos.symbol);
  if(i>=0) arr[i]=pos; else arr.push(pos);
  store.positions.set(k,arr);
}
async function dbRemovePosition(userId, symbol, acctType) {
  if (useDB) { await mongoose.model('Position').deleteOne({userId,symbol,accountType:acctType}); return; }
  const k=posKey(userId,acctType);
  store.positions.set(k,(store.positions.get(k)||[]).filter(p=>p.symbol!==symbol));
}
async function dbSaveOrder(ord) {
  if (useDB) { await new (mongoose.model('Order'))(ord).save(); return ord; }
  const k=posKey(ord.userId,ord.accountType);
  const arr=store.orders.get(k)||[];
  arr.unshift(ord); store.orders.set(k,arr); return ord;
}
async function dbGetOrders(userId, acctType) {
  if (useDB) return mongoose.model('Order').find({userId,accountType:acctType}).sort('-createdAt').limit(200);
  return (store.orders.get(posKey(userId,acctType))||[]).slice(0,200);
}
async function dbAllUsers() {
  if (useDB) return mongoose.model('User').find().select('-password').sort('-createdAt');
  return [...store.usersById.values()].map(({password:_,...u})=>u).sort((a,b)=>new Date(b.createdAt)-new Date(a.createdAt));
}
async function dbSaveDeposit(dep) {
  if (useDB) { await new (mongoose.model('Deposit'))(dep).save(); return dep; }
  const arr=store.deposits.get(dep.userId)||[];
  arr.unshift(dep); store.deposits.set(dep.userId,arr); return dep;
}
async function dbGetDeposits(userId) {
  if (useDB) return mongoose.model('Deposit').find(userId?{userId}:{}).sort('-createdAt').limit(100);
  if (userId) return store.deposits.get(userId)||[];
  const all=[]; store.deposits.forEach(arr=>all.push(...arr));
  return all.sort((a,b)=>new Date(b.createdAt)-new Date(a.createdAt));
}
async function dbUpdateDeposit(id, fields) {
  if (useDB) { await mongoose.model('Deposit').updateOne({_id:id}, fields); return; }
  // Memory: find and update
  store.deposits.forEach(arr=>{
    const dep=arr.find(d=>d.id===id);
    if(dep) Object.assign(dep,fields);
  });
}
async function dbSaveWithdrawal(wd) {
  if (useDB) { await new (mongoose.model('Withdrawal'))(wd).save(); return wd; }
  const arr=store.withdrawals.get(wd.userId)||[];
  arr.unshift(wd); store.withdrawals.set(wd.userId,arr); return wd;
}
async function dbGetWithdrawals(userId) {
  if (useDB) return mongoose.model('Withdrawal').find(userId?{userId}:{}).sort('-createdAt').limit(100);
  if (userId) return store.withdrawals.get(userId)||[];
  const all=[]; store.withdrawals.forEach(arr=>all.push(...arr));
  return all.sort((a,b)=>new Date(b.createdAt)-new Date(a.createdAt));
}
async function dbUpdateWithdrawal(id, fields) {
  if (useDB) { await mongoose.model('Withdrawal').updateOne({_id:id}, fields); return; }
  store.withdrawals.forEach(arr=>{
    const wd=arr.find(w=>w.id===id);
    if(wd) Object.assign(wd,fields);
  });
}

// ── PRICES — real May 2026 seeds ─────────────────────────────
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
const TD_MAP = {
  EURUSD:'EUR/USD',GBPUSD:'GBP/USD',USDJPY:'USD/JPY',USDCHF:'USD/CHF',
  AUDUSD:'AUD/USD',USDCAD:'USD/CAD',NZDUSD:'NZD/USD',EURGBP:'EUR/GBP',
  EURJPY:'EUR/JPY',GBPJPY:'GBP/JPY',XAUUSD:'XAU/USD',XAGUSD:'XAG/USD',
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

let tdConnected = false, tdWs = null;

function connectTD() {
  if (!TD_KEY||TD_KEY.length<10) { startSim(); return; }
  console.log('🔌 Connecting Twelve Data...');
  tdWs = new WS(`wss://ws.twelvedata.com/v1/quotes/price?apikey=${TD_KEY}`);
  tdWs.on('open',()=>{
    tdConnected=true; console.log('✅ Twelve Data LIVE');
    tdWs.send(JSON.stringify({action:'subscribe',params:{symbols:Object.values(TD_MAP).join(',')}}));
  });
  tdWs.on('message',raw=>{
    try{
      const msg=JSON.parse(raw.toString());
      if(msg.event==='price'){
        const sym=TD_REV[msg.symbol];
        if(sym&&msg.price&&parseFloat(msg.price)>0) prices[sym]=parseFloat(msg.price);
      }
    }catch{}
  });
  tdWs.on('error',e=>console.log('TD error:',e.message.slice(0,50)));
  tdWs.on('close',()=>{tdConnected=false;console.log('🔄 TD closed, reconnecting...');setTimeout(connectTD,5000);});
}

function fetchTDRest(syms) {
  if(!TD_KEY) return;
  const https=require('https');
  https.get(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(syms)}&apikey=${TD_KEY}`,res=>{
    let d=''; res.on('data',c=>d+=c);
    res.on('end',()=>{
      try{
        const j=JSON.parse(d);
        const proc=(s,o)=>{const k=TD_REV[s]||s;if(o&&o.price&&parseFloat(o.price)>0)prices[k]=parseFloat(o.price);};
        if(j.price) proc(syms,j); else Object.entries(j).forEach(([s,o])=>proc(s,o));
      }catch{}
    });
  }).on('error',()=>{}).setTimeout(8000);
}
setInterval(()=>fetchTDRest('DJI,SPX,NDX,DAX,FTSE'),2*60*1000);
setTimeout(()=>fetchTDRest('DJI,SPX,NDX,DAX,FTSE'),3000);

function startSim() {
  console.log('🎲 Simulation mode');
  const VOL={BTCUSD:.003,ETHUSD:.003,SOLUSD:.004,XAUUSD:.0015,WTIUSD:.002,US30:.001,EURUSD:.0004,GBPUSD:.0005};
  const mom={};Object.keys(prices).forEach(s=>mom[s]=0);
  setInterval(()=>{for(const s in prices){const v=VOL[s]||.0005;mom[s]=mom[s]*.88+(Math.random()-.499)*v*.2;prices[s]*=(1+mom[s]);}},800);
}

// Broadcast prices every 500ms
setInterval(()=>io.emit('prices',{p:prices,live:tdConnected,ts:Date.now()}),500);

// ── SL/TP MONITOR ─────────────────────────────────────────────
async function checkSLTP() {
  const users=await dbAllUsers();
  for(const user of users){
    for(const acctType of ['demo','live']){
      const positions=await dbGetPositions(user.id,acctType);
      for(const pos of positions){
        const cur=prices[pos.symbol];
        if(!cur||(!pos.sl&&!pos.tp)) continue;
        let closeReason=null;
        if(pos.side==='buy'){
          if(pos.sl&&cur<=pos.sl) closeReason='SL';
          if(pos.tp&&cur>=pos.tp) closeReason='TP';
        } else {
          if(pos.sl&&cur>=pos.sl) closeReason='SL';
          if(pos.tp&&cur<=pos.tp) closeReason='TP';
        }
        if(closeReason){
          await dbRemovePosition(user.id,pos.symbol,acctType);
          const pnl=(cur-pos.avgPrice)*(pos.quantity||1)*(pos.side==='buy'?1:-1);
          const bal=acctType==='live'?(user.liveBalance||0):(user.demoBalance||10000);
          const newBal=bal+pnl;
          await dbUpdateBalance(user.id,newBal,acctType);
          await dbSaveOrder({userId:user.id,accountType:acctType,symbol:pos.symbol,side:'sell',quantity:pos.quantity,price:cur,sl:pos.sl,tp:pos.tp,pnl,balanceAfter:newBal,status:'closed',closeReason,createdAt:new Date().toISOString()});
          io.to(`user:${user.id}`).emit('positionClosed',{symbol:pos.symbol,reason:closeReason,price:cur,pnl:pnl.toFixed(2),balance:newBal.toFixed(2),accountType:acctType});
        }
      }
    }
  }
}
setInterval(checkSLTP,1000);

// ── AUTH ──────────────────────────────────────────────────────
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
    if(password.length<8) return res.status(400).json({msg:'Password min 8 chars'});
    if(await dbFindUser(email)) return res.status(400).json({msg:'Email already registered'});
    const id=crypto.randomUUID();
    const hashed=await bcrypt.hash(password,12);
    const newUser=await dbCreateUser({
      id,email:email.toLowerCase(),password:hashed,fullName,country,
      demoBalance:10000,liveBalance:0,
      activeAccount:'demo',
      kycStatus:'unverified',
      status:'active',
      createdAt:new Date().toISOString()
    });
    await dbUpdateUser(id,{lastLogin:new Date()});
    const token=jwt.sign({id,email:email.toLowerCase()},JWT_SECRET,{expiresIn:'7d'});
    res.json({token,user:{id,email,fullName,country,demoBalance:10000,liveBalance:0,activeAccount:'demo',kycStatus:'unverified'}});
  }catch(e){res.status(500).json({msg:e.message});}
});

app.post('/api/auth/login', async(req,res)=>{
  try{
    const{email,password}=req.body;
    if(!email||!password) return res.status(400).json({msg:'Email and password required'});
    const user=await dbFindUser(email);
    if(!user||!await bcrypt.compare(password,user.password)) return res.status(400).json({msg:'Invalid credentials'});
    await dbUpdateUser(user.id,{lastLogin:new Date()});
    const token=jwt.sign({id:user.id,email:user.email},JWT_SECRET,{expiresIn:'7d'});
    res.json({token,user:{id:user.id,email:user.email,fullName:user.fullName,country:user.country,demoBalance:user.demoBalance||10000,liveBalance:user.liveBalance||0,activeAccount:user.activeAccount||'demo',kycStatus:user.kycStatus}});
  }catch(e){res.status(500).json({msg:e.message});}
});

// ── ACCOUNT ROUTES ────────────────────────────────────────────
app.get('/api/account/me', auth, async(req,res)=>{
  const u=await dbFindUserById(req.user.id);
  if(!u) return res.status(404).json({msg:'Not found'});
  const{password:_,...safe}=u; res.json(safe);
});

// Switch between demo and live
app.post('/api/account/switch', auth, async(req,res)=>{
  const{accountType}=req.body;
  if(!['demo','live'].includes(accountType)) return res.status(400).json({msg:'Invalid account type'});
  await dbUpdateUser(req.user.id,{activeAccount:accountType});
  const user=await dbFindUserById(req.user.id);
  res.json({activeAccount:accountType,demoBalance:user.demoBalance||10000,liveBalance:user.liveBalance||0});
});

app.get('/api/account/positions', auth, async(req,res)=>{
  const user=await dbFindUserById(req.user.id);
  const acct=req.query.account||user.activeAccount||'demo';
  res.json(await dbGetPositions(req.user.id,acct));
});

app.get('/api/account/orders', auth, async(req,res)=>{
  const user=await dbFindUserById(req.user.id);
  const acct=req.query.account||user.activeAccount||'demo';
  res.json(await dbGetOrders(req.user.id,acct));
});

app.get('/api/account/deposits', auth, async(req,res)=>{
  res.json(await dbGetDeposits(req.user.id));
});

app.get('/api/account/withdrawals', auth, async(req,res)=>{
  res.json(await dbGetWithdrawals(req.user.id));
});

// Deposit request (creates pending deposit)
app.post('/api/account/deposit', auth, async(req,res)=>{
  try{
    const{amount,method,txHash,reference}=req.body;
    if(!amount||amount<10) return res.status(400).json({msg:'Minimum deposit $10'});
    const dep={
      id:crypto.randomUUID(),
      userId:req.user.id,
      amount:parseFloat(amount),
      method:method||'crypto',
      txHash:txHash||'',
      reference:reference||'',
      status:'pending',
      createdAt:new Date().toISOString()
    };
    await dbSaveDeposit(dep);
    // Notify admins
    io.emit('adminAlert',{type:'deposit',msg:`New deposit request: $${amount} via ${method}`});
    res.json({msg:'Deposit request submitted. Pending admin approval.',depositId:dep.id});
  }catch(e){res.status(500).json({msg:e.message});}
});

// Withdrawal request
app.post('/api/account/withdraw', auth, async(req,res)=>{
  try{
    const{amount,method,address}=req.body;
    if(!amount||amount<10) return res.status(400).json({msg:'Minimum withdrawal $10'});
    const user=await dbFindUserById(req.user.id);
    const livebal=user.liveBalance||0;
    if(livebal<amount) return res.status(400).json({msg:'Insufficient live balance'});
    const wd={
      id:crypto.randomUUID(),
      userId:req.user.id,
      amount:parseFloat(amount),
      method:method||'crypto',
      address:address||'',
      status:'pending',
      createdAt:new Date().toISOString()
    };
    await dbSaveWithdrawal(wd);
    io.emit('adminAlert',{type:'withdrawal',msg:`Withdrawal request: $${amount} via ${method}`});
    res.json({msg:'Withdrawal request submitted.',withdrawalId:wd.id});
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
    const acctType=user.activeAccount||'demo';
    const currentBal=acctType==='live'?(user.liveBalance||0):(user.demoBalance||10000);
    const margin=price*quantity*0.002;
    if(side==='buy'){
      if(currentBal<margin) return res.status(400).json({msg:'Insufficient balance'});
      await dbUpdateBalance(req.user.id,currentBal-margin,acctType);
      const positions=await dbGetPositions(req.user.id,acctType);
      const existing=positions.find(p=>p.symbol===symbol);
      if(existing){
        const totalQty=existing.quantity+quantity;
        const avgPrice=((existing.avgPrice*existing.quantity)+(price*quantity))/totalQty;
        await dbSavePosition({...existing,quantity:totalQty,avgPrice,sl:sl||existing.sl,tp:tp||existing.tp});
      } else {
        await dbSavePosition({userId:req.user.id,accountType:acctType,symbol,quantity,avgPrice:price,side:'buy',sl:sl||null,tp:tp||null,notional:price*quantity,openTime:new Date().toISOString()});
      }
    } else {
      const positions=await dbGetPositions(req.user.id,acctType);
      const pos=positions.find(p=>p.symbol===symbol);
      if(!pos||pos.quantity<quantity) return res.status(400).json({msg:'Insufficient position'});
      const pnl=(price-pos.avgPrice)*quantity;
      if(pos.quantity===quantity) await dbRemovePosition(req.user.id,symbol,acctType);
      else await dbSavePosition({...pos,quantity:pos.quantity-quantity});
      const newBal=currentBal+margin+pnl;
      await dbUpdateBalance(req.user.id,newBal,acctType);
    }
    await dbSaveOrder({userId:req.user.id,accountType:acctType,symbol,side,quantity,price,sl:sl||null,tp:tp||null,status:'filled',createdAt:new Date().toISOString()});
    const updated=await dbFindUserById(req.user.id);
    res.json({msg:'Order executed',price,
      demoBalance:updated.demoBalance||10000,
      liveBalance:updated.liveBalance||0,
      activeAccount:acctType});
  }catch(e){res.status(500).json({msg:e.message});}
});

app.post('/api/trading/close/:symbol', auth, async(req,res)=>{
  try{
    const{symbol}=req.params;
    const{quantity}=req.body;
    const user=await dbFindUserById(req.user.id);
    const acctType=user.activeAccount||'demo';
    const positions=await dbGetPositions(req.user.id,acctType);
    const pos=positions.find(p=>p.symbol===symbol);
    if(!pos) return res.status(404).json({msg:'Position not found'});
    const price=prices[symbol]||pos.avgPrice;
    const qty=quantity||pos.quantity;
    const pnl=(price-pos.avgPrice)*qty*(pos.side==='buy'?1:-1);
    if(pos.quantity<=qty) await dbRemovePosition(req.user.id,symbol,acctType);
    else await dbSavePosition({...pos,quantity:pos.quantity-qty});
    const bal=acctType==='live'?(user.liveBalance||0):(user.demoBalance||10000);
    const newBal=bal+pnl;
    await dbUpdateBalance(req.user.id,newBal,acctType);
    await dbSaveOrder({userId:req.user.id,accountType:acctType,symbol,side:'sell',quantity:qty,price,pnl,balanceAfter:newBal,status:'closed',closeReason:'manual',createdAt:new Date().toISOString()});
    res.json({msg:'Position closed',pnl:pnl.toFixed(2),demoBalance:acctType==='demo'?newBal:(user.demoBalance||10000),liveBalance:acctType==='live'?newBal:(user.liveBalance||0)});
  }catch(e){res.status(500).json({msg:e.message});}
});

// ── ADMIN ROUTES (ALL REAL DATA) ──────────────────────────────
app.get('/api/admin/stats', adminAuth, async(req,res)=>{
  const users=await dbAllUsers();
  const deps=await dbGetDeposits();
  const wds=await dbGetWithdrawals();
  const totalLive=users.reduce((s,u)=>s+(u.liveBalance||0),0);
  const totalDemo=users.reduce((s,u)=>s+(u.demoBalance||10000),0);
  const pendingDeps=deps.filter(d=>d.status==='pending');
  const pendingWds=wds.filter(w=>w.status==='pending');
  const verified=users.filter(u=>u.kycStatus==='verified');
  const pendingKyc=users.filter(u=>u.kycStatus==='pending');
  res.json({
    users:users.length,
    verified:verified.length,
    pendingKyc:pendingKyc.length,
    totalLiveBalance:totalLive,
    totalDemoBalance:totalDemo,
    pendingDeposits:pendingDeps.length,
    pendingDepositAmount:pendingDeps.reduce((s,d)=>s+d.amount,0),
    pendingWithdrawals:pendingWds.length,
    pendingWithdrawalAmount:pendingWds.reduce((s,w)=>s+w.amount,0),
    live:tdConnected, db:useDB?'MongoDB':'Memory',
    symbols:Object.keys(prices).length,
    uptime:Math.floor(process.uptime())
  });
});

app.get('/api/admin/users', adminAuth, async(req,res)=>{
  const users=await dbAllUsers();
  res.json(users);
});

app.get('/api/admin/deposits', adminAuth, async(req,res)=>{
  const deps=await dbGetDeposits();
  // Enrich with user info
  const enriched=await Promise.all(deps.map(async d=>{
    const u=await dbFindUserById(d.userId);
    return {...d, userName:u?.fullName||'—', userEmail:u?.email||'—'};
  }));
  res.json(enriched);
});

app.get('/api/admin/withdrawals', adminAuth, async(req,res)=>{
  const wds=await dbGetWithdrawals();
  const enriched=await Promise.all(wds.map(async w=>{
    const u=await dbFindUserById(w.userId);
    return {...w, userName:u?.fullName||'—', userEmail:u?.email||'—'};
  }));
  res.json(enriched);
});

// Approve deposit — credit user live balance
app.post('/api/admin/deposits/:id/approve', adminAuth, async(req,res)=>{
  try{
    const{id}=req.params;
    const deps=await dbGetDeposits();
    const dep=deps.find(d=>d.id===id||(d._id&&d._id.toString()===id));
    if(!dep) return res.status(404).json({msg:'Deposit not found'});
    const user=await dbFindUserById(dep.userId);
    if(!user) return res.status(404).json({msg:'User not found'});
    const newBal=(user.liveBalance||0)+dep.amount;
    await dbUpdateBalance(dep.userId,newBal,'live');
    await dbUpdateDeposit(dep.id||dep._id?.toString(), {status:'approved',reviewedAt:new Date()});
    io.to(`user:${dep.userId}`).emit('depositApproved',{amount:dep.amount,balance:newBal});
    res.json({msg:'Deposit approved',newBalance:newBal});
  }catch(e){res.status(500).json({msg:e.message});}
});

// Reject deposit
app.post('/api/admin/deposits/:id/reject', adminAuth, async(req,res)=>{
  try{
    const deps=await dbGetDeposits();
    const dep=deps.find(d=>d.id===req.params.id||(d._id&&d._id.toString()===req.params.id));
    if(!dep) return res.status(404).json({msg:'Not found'});
    await dbUpdateDeposit(dep.id||dep._id?.toString(),{status:'rejected',reviewedAt:new Date()});
    io.to(`user:${dep.userId}`).emit('depositRejected',{amount:dep.amount});
    res.json({msg:'Deposit rejected'});
  }catch(e){res.status(500).json({msg:e.message});}
});

// Process withdrawal
app.post('/api/admin/withdrawals/:id/process', adminAuth, async(req,res)=>{
  try{
    const wds=await dbGetWithdrawals();
    const wd=wds.find(w=>w.id===req.params.id||(w._id&&w._id.toString()===req.params.id));
    if(!wd) return res.status(404).json({msg:'Not found'});
    const user=await dbFindUserById(wd.userId);
    if(!user) return res.status(404).json({msg:'User not found'});
    const newBal=Math.max(0,(user.liveBalance||0)-wd.amount);
    await dbUpdateBalance(wd.userId,newBal,'live');
    await dbUpdateWithdrawal(wd.id||wd._id?.toString(),{status:'processed',processedAt:new Date()});
    io.to(`user:${wd.userId}`).emit('withdrawalProcessed',{amount:wd.amount});
    res.json({msg:'Withdrawal processed'});
  }catch(e){res.status(500).json({msg:e.message});}
});

// KYC approve/reject
app.patch('/api/admin/users/:id/kyc', adminAuth, async(req,res)=>{
  await dbUpdateUser(req.params.id,{kycStatus:req.body.status});
  const user=await dbFindUserById(req.params.id);
  if(user) io.to(`user:${req.params.id}`).emit('kycUpdated',{status:req.body.status});
  res.json({ok:true,status:req.body.status});
});

// Adjust balance
app.post('/api/admin/users/:id/balance', adminAuth, async(req,res)=>{
  const{amount,accountType}=req.body;
  const user=await dbFindUserById(req.params.id);
  if(!user) return res.status(404).json({msg:'Not found'});
  const acct=accountType||'demo';
  const cur=acct==='live'?(user.liveBalance||0):(user.demoBalance||10000);
  const newBal=cur+parseFloat(amount||0);
  await dbUpdateBalance(req.params.id,newBal,acct);
  res.json({balance:newBal,accountType:acct});
});

// Suspend / activate user
app.patch('/api/admin/users/:id/status', adminAuth, async(req,res)=>{
  await dbUpdateUser(req.params.id,{status:req.body.status});
  res.json({ok:true});
});

// All positions across all users (live monitor)
app.get('/api/admin/positions', adminAuth, async(req,res)=>{
  const users=await dbAllUsers();
  const all=[];
  for(const u of users){
    for(const acct of ['demo','live']){
      const positions=await dbGetPositions(u.id,acct);
      for(const p of positions){
        const cur=prices[p.symbol]||p.avgPrice;
        const pnl=(cur-p.avgPrice)*(p.quantity||1)*(p.side==='buy'?1:-1);
        all.push({...p,currentPrice:cur,pnl:pnl.toFixed(2),
          userName:u.fullName||u.email,userEmail:u.email});
      }
    }
  }
  res.json(all);
});

app.get('/api/markets', (req,res)=>res.json({prices,live:tdConnected,ts:Date.now()}));
app.get('/health',(req,res)=>res.json({ok:true}));
app.get('/api/health',(req,res)=>res.json({ok:true,live:tdConnected,db:useDB?'mongodb':'memory',uptime:Math.floor(process.uptime())}));

// ── SOCKET.IO ─────────────────────────────────────────────────
io.on('connection', socket => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;
  if(token){try{const d=jwt.verify(token,JWT_SECRET);socket.join(`user:${d.id}`);}catch{}}
  socket.emit('prices',{p:prices,live:tdConnected,ts:Date.now()});
  socket.on('disconnect',()=>{});
});

// ── START ─────────────────────────────────────────────────────
tryConnectDB().then(()=>{
  server.listen(PORT,()=>{
    console.log(`\n🚀 Velox v6.0 on port ${PORT}`);
    console.log(`   DB:  ${useDB?'✅ MongoDB':'⚡ Memory'}`);
    console.log(`   API: ${TD_KEY?'✅ Twelve Data':'⚠️  Simulation mode'}\n`);
    setTimeout(()=>TD_KEY&&TD_KEY.length>10?connectTD():startSim(),500);
  });
}).catch(()=>{server.listen(PORT);TD_KEY&&TD_KEY.length>10?connectTD():startSim();});
