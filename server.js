require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const QRCode = require('qrcode');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || 'velox_default_secret_change_me';
const ADMIN_KEY = process.env.ADMIN_SECRET_KEY || 'admin123';

// ── MIDDLEWARE ────────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());
app.use(express.static('public'));
app.use('/api/', rateLimit({ windowMs: 15 * 60 * 1000, max: 200 }));

// ── IN-MEMORY STORE (fallback when no MongoDB) ───────────────
const store = {
  users: new Map(),       // email -> user object
  usersById: new Map(),   // id -> user object
  positions: new Map(),   // userId -> positions[]
  orders: new Map(),      // userId -> orders[]
  transactions: new Map() // userId -> transactions[]
};

// ── MONGODB (optional) ────────────────────────────────────────
let mongoose = null;
let User = null, Position = null, Order = null, Transaction = null;
let useDB = false;

async function tryConnectDB() {
  const uri = process.env.MONGO_URI;
  if (!uri || uri.includes('localhost') || uri.length < 20) {
    console.log('⚡ No MONGO_URI — using in-memory storage');
    return;
  }
  try {
    mongoose = require('mongoose');
    await mongoose.connect(uri, { serverSelectionTimeoutMS: 8000 });

    const userSchema = new mongoose.Schema({
      id: String, email: { type: String, unique: true }, password: String,
      fullName: String, country: String, balance: { type: Number, default: 0 },
      kycStatus: { type: String, default: 'unverified' }, createdAt: { type: Date, default: Date.now }
    });
    User        = mongoose.model('User', userSchema);
    Position    = mongoose.model('Position', new mongoose.Schema({ userId:String, symbol:String, quantity:Number, avgPrice:Number, side:String, createdAt:{ type:Date, default:Date.now } }));
    Order       = mongoose.model('Order', new mongoose.Schema({ userId:String, symbol:String, side:String, quantity:Number, price:Number, status:{ type:String, default:'filled' }, createdAt:{ type:Date, default:Date.now } }));
    Transaction = mongoose.model('Transaction', new mongoose.Schema({ userId:String, type:String, amount:Number, currency:String, method:String, status:String, txHash:String, createdAt:{ type:Date, default:Date.now } }));

    useDB = true;
    console.log('✅ MongoDB connected');
  } catch (e) {
    console.log('⚡ MongoDB failed (' + e.message.slice(0,60) + ') — using memory');
    useDB = false;
  }
}

// ── DB HELPERS ────────────────────────────────────────────────
const crypto = require('crypto');

async function dbFindUser(email) {
  if (useDB) return await User.findOne({ email: email.toLowerCase() });
  return store.users.get(email.toLowerCase()) || null;
}
async function dbFindUserById(id) {
  if (useDB) return await User.findOne({ id });
  return store.usersById.get(id) || null;
}
async function dbCreateUser(data) {
  if (useDB) { const u = new User(data); await u.save(); return u; }
  store.users.set(data.email.toLowerCase(), data);
  store.usersById.set(data.id, data);
  return data;
}
async function dbUpdateBalance(userId, newBalance) {
  if (useDB) { await User.updateOne({ id: userId }, { balance: newBalance }); return; }
  const u = store.usersById.get(userId);
  if (u) u.balance = newBalance;
}
async function dbGetPositions(userId) {
  if (useDB) return await Position.find({ userId });
  return store.positions.get(userId) || [];
}
async function dbSavePosition(pos) {
  if (useDB) { const p = new Position(pos); await p.save(); return p; }
  const arr = store.positions.get(pos.userId) || [];
  const existing = arr.find(p => p.symbol === pos.symbol);
  if (existing) { existing.quantity = pos.quantity; existing.avgPrice = pos.avgPrice; }
  else arr.push(pos);
  store.positions.set(pos.userId, arr);
}
async function dbRemovePosition(userId, symbol) {
  if (useDB) { await Position.deleteOne({ userId, symbol }); return; }
  const arr = (store.positions.get(userId) || []).filter(p => p.symbol !== symbol);
  store.positions.set(userId, arr);
}
async function dbSaveOrder(ord) {
  if (useDB) { const o = new Order(ord); await o.save(); return o; }
  const arr = store.orders.get(ord.userId) || [];
  arr.unshift(ord); store.orders.set(ord.userId, arr); return ord;
}
async function dbGetOrders(userId) {
  if (useDB) return await Order.find({ userId }).sort('-createdAt').limit(50);
  return (store.orders.get(userId) || []).slice(0, 50);
}
async function dbSaveTransaction(tx) {
  if (useDB) { const t = new Transaction(tx); await t.save(); return t; }
  const arr = store.transactions.get(tx.userId) || [];
  arr.unshift(tx); store.transactions.set(tx.userId, arr); return tx;
}
async function dbGetTransactions(userId) {
  if (useDB) return await Transaction.find({ userId }).sort('-createdAt').limit(50);
  return (store.transactions.get(userId) || []).slice(0, 50);
}

// ── AUTH MIDDLEWARE ───────────────────────────────────────────
function authMW(req, res, next) {
  const token = req.header('x-auth-token') || req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ msg: 'No token' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({ msg: 'Invalid token' }); }
}
function adminMW(req, res, next) {
  if (req.headers['admin-key'] !== ADMIN_KEY) return res.status(403).json({ msg: 'Forbidden' });
  next();
}

// ── PRICES ────────────────────────────────────────────────────
const prices = {
  EURUSD:1.0843, GBPUSD:1.2678, USDJPY:149.23, USDCHF:0.9012, AUDUSD:0.6534,
  USDCAD:1.3642, NZDUSD:0.5923, EURGBP:0.8562, EURJPY:161.82, GBPJPY:189.40,
  XAUUSD:2314.5, XAGUSD:27.34, WTIUSD:78.42, BRENTUSD:82.15,
  BTCUSD:67842, ETHUSD:3428.1, BNBUSD:432.1, SOLUSD:142.3,
  XRPUSD:0.5234, ADAUSD:0.4521, LTCUSD:84.2, DOGEUSD:0.1523,
  US30:38420, SPX500:5124, NAS100:17834, DAX40:18234, FTSE100:7834,
  AAPL:192.62, TSLA:213.06, NVDA:874.5, AMZN:185.4, MSFT:415.3,
};
const mom = {}; Object.keys(prices).forEach(s => mom[s] = 0);
const VOL = { BTCUSD:.003, ETHUSD:.003, SOLUSD:.004, XAUUSD:.0015, WTIUSD:.002, US30:.001, EURUSD:.0004, GBPUSD:.0005 };

// Tick prices
setInterval(() => {
  for (const s in prices) {
    const v = VOL[s] || .0005;
    mom[s] = mom[s] * .9 + (Math.random() - .499) * v * .15;
    prices[s] *= (1 + mom[s]);
  }
  io.emit('allPrices', { prices, ts: Date.now() });
}, 500);

// Twelve Data refresh (if API key set)
const AV_KEY = process.env.TWELVE_DATA_API_KEY;
if (AV_KEY && AV_KEY.length > 5) {
  const https = require('https');
  const pairs = [['EUR/USD','EURUSD'],['GBP/USD','GBPUSD'],['XAU/USD','XAUUSD'],['BTC/USD','BTCUSD'],['ETH/USD','ETHUSD']];
  function fetchTD(td, sym) {
    const req = https.get(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(td)}&apikey=${AV_KEY}`, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { try { const p = JSON.parse(d); if (p.price) prices[sym] = parseFloat(p.price); } catch {} });
    }); req.on('error', () => {}); req.setTimeout(5000, () => req.destroy());
  }
  setInterval(() => pairs.forEach(([td,s]) => fetchTD(td,s)), 30000);
  setTimeout(() => pairs.forEach(([td,s]) => fetchTD(td,s)), 3000);
}

// ── AUTH ROUTES ───────────────────────────────────────────────
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password, fullName, country } = req.body;
    if (!email || !password || !fullName || !country)
      return res.status(400).json({ msg: 'All fields required' });
    if (password.length < 8)
      return res.status(400).json({ msg: 'Password must be at least 8 characters' });
    const allowed = ['Pakistan','Bangladesh','Sri Lanka','Nepal','Bhutan','Maldives'];
    if (!allowed.includes(country))
      return res.status(400).json({ msg: 'Country not supported' });
    if (await dbFindUser(email))
      return res.status(400).json({ msg: 'Email already registered' });

    const hashed = await bcrypt.hash(password, 12);
    const id = crypto.randomUUID();
    const user = await dbCreateUser({
      id, email: email.toLowerCase(), password: hashed,
      fullName, country, balance: 10000, kycStatus: 'unverified'
    });
    const token = jwt.sign({ id, email: email.toLowerCase() }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id, email, fullName, country, balance: 10000, kycStatus: 'unverified' } });
  } catch (e) {
    console.error('Register error:', e);
    res.status(500).json({ msg: 'Registration failed: ' + e.message });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ msg: 'Email and password required' });
    const user = await dbFindUser(email);
    if (!user) return res.status(400).json({ msg: 'Invalid email or password' });
    const match = await bcrypt.compare(password, user.password);
    if (!match) return res.status(400).json({ msg: 'Invalid email or password' });
    const token = jwt.sign({ id: user.id, email: user.email }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: user.id, email: user.email, fullName: user.fullName, country: user.country, balance: user.balance, kycStatus: user.kycStatus } });
  } catch (e) { res.status(500).json({ msg: 'Login failed' }); }
});

// ── ACCOUNT ROUTES ────────────────────────────────────────────
app.get('/api/account/me', authMW, async (req, res) => {
  const user = await dbFindUserById(req.user.id);
  if (!user) return res.status(404).json({ msg: 'User not found' });
  const { password: _, ...safe } = user;
  res.json(safe);
});

app.get('/api/account/positions', authMW, async (req, res) => {
  res.json(await dbGetPositions(req.user.id));
});

app.get('/api/account/orders', authMW, async (req, res) => {
  res.json(await dbGetOrders(req.user.id));
});

app.get('/api/account/deposit-address', authMW, async (req, res) => {
  const btc = process.env.CRYPTO_BTC_ADDRESS || '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa';
  const usdt = process.env.CRYPTO_USDT_ADDRESS || 'TYourUSDTAddressHere';
  const btcQR = await QRCode.toDataURL(btc);
  const usdtQR = await QRCode.toDataURL(usdt);
  res.json({ btc, usdt, btcQR, usdtQR });
});

app.post('/api/account/confirm-deposit', authMW, async (req, res) => {
  try {
    const { txHash, amount, currency } = req.body;
    const user = await dbFindUserById(req.user.id);
    const newBalance = (user.balance || 0) + parseFloat(amount);
    await dbUpdateBalance(req.user.id, newBalance);
    await dbSaveTransaction({ userId: req.user.id, type: 'deposit', amount, currency, txHash, status: 'completed' });
    res.json({ msg: 'Deposit confirmed', balance: newBalance });
  } catch (e) { res.status(500).json({ msg: e.message }); }
});

// ── TRADING ROUTES ────────────────────────────────────────────
app.post('/api/trading/order', authMW, async (req, res) => {
  try {
    const { symbol, side, quantity } = req.body;
    if (!symbol || !side || !quantity || quantity <= 0)
      return res.status(400).json({ msg: 'Invalid order parameters' });
    const price = prices[symbol];
    if (!price) return res.status(400).json({ msg: 'Symbol not available' });

    const user = await dbFindUserById(req.user.id);
    const notional = quantity * price;

    if (side === 'buy') {
      if ((user.balance || 0) < notional) return res.status(400).json({ msg: 'Insufficient balance' });
      await dbUpdateBalance(req.user.id, (user.balance || 0) - notional);
      const positions = await dbGetPositions(req.user.id);
      const existing = positions.find(p => p.symbol === symbol);
      if (existing) {
        const totalQty = existing.quantity + quantity;
        const avgPrice = ((existing.avgPrice * existing.quantity) + (price * quantity)) / totalQty;
        await dbSavePosition({ ...existing, quantity: totalQty, avgPrice });
      } else {
        await dbSavePosition({ userId: req.user.id, symbol, quantity, avgPrice: price, side: 'buy' });
      }
    } else if (side === 'sell') {
      const positions = await dbGetPositions(req.user.id);
      const pos = positions.find(p => p.symbol === symbol);
      if (!pos || pos.quantity < quantity) return res.status(400).json({ msg: 'Insufficient position' });
      if (pos.quantity === quantity) await dbRemovePosition(req.user.id, symbol);
      else await dbSavePosition({ ...pos, quantity: pos.quantity - quantity });
      await dbUpdateBalance(req.user.id, (user.balance || 0) + notional);
    }

    const order = await dbSaveOrder({ userId: req.user.id, symbol, side, quantity, price, status: 'filled' });
    const updatedUser = await dbFindUserById(req.user.id);
    res.json({ msg: 'Order executed', balance: updatedUser.balance, order });
  } catch (e) { res.status(500).json({ msg: e.message }); }
});

// ── ADMIN ROUTES ──────────────────────────────────────────────
app.get('/api/admin/users', adminMW, async (req, res) => {
  if (useDB) {
    const users = await User.find().select('-password');
    return res.json(users);
  }
  const users = [...store.usersById.values()].map(({ password, ...u }) => u);
  res.json(users);
});

app.post('/api/admin/user/:id/balance', adminMW, async (req, res) => {
  const { amount } = req.body;
  const user = await dbFindUserById(req.params.id);
  if (!user) return res.status(404).json({ msg: 'Not found' });
  const newBalance = (user.balance || 0) + parseFloat(amount);
  await dbUpdateBalance(req.params.id, newBalance);
  res.json({ balance: newBalance });
});

app.get('/api/admin/stats', adminMW, async (req, res) => {
  const userCount = useDB ? await User.countDocuments() : store.usersById.size;
  res.json({ users: userCount, symbols: Object.keys(prices).length, db: useDB ? 'MongoDB' : 'Memory' });
});

// ── MARKETS ───────────────────────────────────────────────────
app.get('/api/markets/prices', (req, res) => res.json({ prices }));

// ── WEBSOCKET ─────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.emit('initPrices', { prices });
  socket.on('disconnect', () => {});
});

// ── HEALTH ────────────────────────────────────────────────────
app.get('/api/health', (req, res) => res.json({ ok: true, db: useDB ? 'mongodb' : 'memory', uptime: process.uptime() }));
app.get('/health', (req, res) => res.json({ ok: true }));

// ── START ─────────────────────────────────────────────────────
tryConnectDB().then(() => {
  server.listen(PORT, () => {
    console.log(`\n🚀 Velox running on port ${PORT}`);
    console.log(`   DB: ${useDB ? '✅ MongoDB' : '⚡ Memory (set MONGO_URI for persistence)'}`);
    console.log(`   Prices: ${Object.keys(prices).length} symbols\n`);
  });
}).catch(e => {
  console.error('Startup error:', e);
  // Start anyway even if tryConnectDB throws
  server.listen(PORT, () => console.log(`Velox running on port ${PORT} (degraded mode)`));
});

const http = require('http');
const { Server } = require('socket.io');
const connectDB = require('./config/db');
const authRoutes = require('./routes/auth');
const accountRoutes = require('./routes/account');
const tradingRoutes = require('./routes/trading');
const adminRoutes = require('./routes/admin');
const { startWebSocketStream } = require('./services/marketData');
const marketFeed = require('./websocket/marketFeed');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 100 });
app.use('/api/', limiter);

connectDB();

app.use('/api/auth', authRoutes);
app.use('/api/account', accountRoutes);
app.use('/api/trading', tradingRoutes);
app.use('/api/admin', adminRoutes);

marketFeed(io);
startWebSocketStream(io);

app.use(require('./middleware/errorHandler'));

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`Velox live on port ${PORT}`));
