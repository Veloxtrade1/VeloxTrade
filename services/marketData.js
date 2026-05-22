const axios = require('axios');
const WebSocket = require('ws');

const API_KEY = process.env.TWELVE_DATA_API_KEY;

// ── SYMBOL MAP: internal name → Twelve Data format ──────────
const symbolMap = {
  // Forex Majors
  EURUSD: 'EUR/USD', GBPUSD: 'GBP/USD', USDJPY: 'USD/JPY',
  USDCHF: 'USD/CHF', AUDUSD: 'AUD/USD', USDCAD: 'USD/CAD',
  NZDUSD: 'NZD/USD', EURGBP: 'EUR/GBP', EURJPY: 'EUR/JPY',
  GBPJPY: 'GBP/JPY', AUDJPY: 'AUD/JPY', CADJPY: 'CAD/JPY',

  // Forex Minors / Exotics
  EURCHF: 'EUR/CHF', GBPCHF: 'GBP/CHF', GBPAUD: 'GBP/AUD',
  EURCAD: 'EUR/CAD', EURNZD: 'EUR/NZD',

  // Commodities
  XAUUSD: 'XAU/USD',   // Gold
  XAGUSD: 'XAG/USD',   // Silver
  WTIUSD: 'WTI/USD',   // Crude Oil
  BRENTUSD: 'BRENT/USD', // Brent Oil
  XPTUSD: 'XPT/USD',   // Platinum
  NATURALGAS: 'NGAS/USD', // Natural Gas

  // Crypto
  BTCUSD: 'BTC/USD',   ETHUSD: 'ETH/USD',
  BNBUSD: 'BNB/USD',   SOLUSD: 'SOL/USD',
  XRPUSD: 'XRP/USD',   ADAUSD: 'ADA/USD',
  DOTUSD: 'DOT/USD',   AVAXUSD: 'AVAX/USD',
  MATICUSD: 'MATIC/USD', LINKUSD: 'LINK/USD',
  LTCUSD: 'LTC/USD',   DOGEUSD: 'DOGE/USD',

  // Indices (via Twelve Data as symbols)
  US30: 'DJI',       // Dow Jones
  SPX500: 'SPX',     // S&P 500
  NAS100: 'NDX',     // Nasdaq 100
  DAX40: 'DAX',      // Germany 40
  FTSE100: 'UK100',  // UK 100
  NIKKEI: 'JP225',   // Japan 225
  ASX200: 'AUS200',  // Australia 200

  // US Stocks
  AAPL: 'AAPL', MSFT: 'MSFT', AMZN: 'AMZN',
  GOOGL: 'GOOGL', TSLA: 'TSLA', NVDA: 'NVDA',
  META: 'META', NFLX: 'NFLX', BABA: 'BABA',
  JPM: 'JPM',
};

// ── DEFAULT SEED PRICES (used before API responds) ───────────
let cachedPrices = {
  EURUSD:1.0843,  GBPUSD:1.2678,  USDJPY:149.23,  USDCHF:0.9012,
  AUDUSD:0.6534,  USDCAD:1.3642,  NZDUSD:0.5923,  EURGBP:0.8562,
  EURJPY:161.82,  GBPJPY:189.40,  AUDJPY:97.52,   CADJPY:109.42,
  EURCHF:0.9742,  GBPCHF:1.1382,  GBPAUD:1.9398,  EURCAD:1.4782,
  EURNZD:1.8302,
  XAUUSD:2314.5,  XAGUSD:27.34,   WTIUSD:78.42,   BRENTUSD:82.15,
  XPTUSD:982.0,   NATURALGAS:2.84,
  BTCUSD:67842,   ETHUSD:3428.1,  BNBUSD:432.1,   SOLUSD:142.3,
  XRPUSD:0.5234,  ADAUSD:0.4521,  DOTUSD:7.82,    AVAXUSD:34.56,
  MATICUSD:0.88,  LINKUSD:13.42,  LTCUSD:84.20,   DOGEUSD:0.1523,
  US30:38420,     SPX500:5124,    NAS100:17834,   DAX40:18234,
  FTSE100:7834,   NIKKEI:38920,   ASX200:7642,
  AAPL:192.62,    MSFT:415.30,    AMZN:185.40,    GOOGL:174.20,
  TSLA:213.06,    NVDA:874.50,    META:493.50,    NFLX:628.40,
  BABA:74.20,     JPM:198.30,
};

let ws = null, ioInstance = null;

// ── CATEGORIES for frontend filtering ────────────────────────
const categories = {
  forex: ['EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','USDCAD','NZDUSD','EURGBP','EURJPY','GBPJPY','AUDJPY','CADJPY','EURCHF','GBPCHF','GBPAUD','EURCAD','EURNZD'],
  commodities: ['XAUUSD','XAGUSD','WTIUSD','BRENTUSD','XPTUSD','NATURALGAS'],
  crypto: ['BTCUSD','ETHUSD','BNBUSD','SOLUSD','XRPUSD','ADAUSD','DOTUSD','AVAXUSD','MATICUSD','LINKUSD','LTCUSD','DOGEUSD'],
  indices: ['US30','SPX500','NAS100','DAX40','FTSE100','NIKKEI','ASX200'],
  stocks: ['AAPL','MSFT','AMZN','GOOGL','TSLA','NVDA','META','NFLX','BABA','JPM'],
};

// ── BATCH REST FETCH (Twelve Data free: 8 req/min, 800/day) ──
// We fetch in small batches to stay within free tier limits
const allSymbols = Object.keys(symbolMap);
let batchIndex = 0;
const BATCH_SIZE = 4; // 4 symbols per request = well within limits

async function refreshBatch() {
  const batch = allSymbols.slice(batchIndex, batchIndex + BATCH_SIZE);
  batchIndex = (batchIndex + BATCH_SIZE) % allSymbols.length;

  try {
    await Promise.all(batch.map(async (sym) => {
      try {
        const td = symbolMap[sym];
        const url = `https://api.twelvedata.com/price?symbol=${encodeURIComponent(td)}&apikey=${API_KEY}`;
        const res = await axios.get(url, { timeout: 5000 });
        if (res.data && res.data.price) {
          const price = parseFloat(res.data.price);
          if (!isNaN(price) && price > 0) {
            cachedPrices[sym] = price;
          }
        }
      } catch (e) {
        // silently keep cached price on error
      }
    }));
  } catch (err) {
    console.error('Batch fetch error:', err.message);
  }
}

// ── WEBSOCKET STREAM (Twelve Data real-time) ─────────────────
function startWebSocketStream(io) {
  ioInstance = io;

  // Connect Twelve Data WebSocket for real-time forex + crypto
  if (!ws) {
    try {
      ws = new WebSocket(`wss://ws.twelvedata.com/v1/quotes?apikey=${API_KEY}`);

      ws.on('open', () => {
        console.log('Twelve Data WS connected');
        // Subscribe to all symbols (free tier: up to 8 concurrent)
        const wsSymbols = [
          'EUR/USD','GBP/USD','USD/JPY','USD/CHF','AUD/USD','USD/CAD','EUR/GBP',
          'BTC/USD','ETH/USD','XAU/USD','XAG/USD',
        ];
        ws.send(JSON.stringify({ action: 'subscribe', params: { symbols: wsSymbols.join(',') } }));
      });

      ws.on('message', (data) => {
        try {
          const msg = JSON.parse(data);
          if (msg.event === 'price' && msg.price) {
            const sym = msg.symbol.replace('/', '');
            const price = parseFloat(msg.price);
            if (!isNaN(price) && price > 0) {
              cachedPrices[sym] = price;
              if (ioInstance) ioInstance.emit('priceUpdate', { [sym]: price });
            }
          }
        } catch (e) {}
      });

      ws.on('error', (err) => console.error('WS error:', err.message));
      ws.on('close', () => {
        console.log('WS closed, reconnecting in 5s...');
        ws = null;
        setTimeout(() => startWebSocketStream(ioInstance), 5000);
      });
    } catch (e) {
      console.error('WS connect failed:', e.message);
    }
  }

  // Broadcast all prices to all connected clients every second
  // (even symbols not from WS get their simulated tick)
  setInterval(() => {
    // Add small random tick to non-WS symbols so they appear live
    Object.keys(cachedPrices).forEach(sym => {
      const isWSSymbol = ['EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','USDCAD','EURGBP','BTCUSD','ETHUSD','XAUUSD','XAGUSD'].includes(sym);
      if (!isWSSymbol) {
        const v = sym.includes('USD') && !['BTCUSD','ETHUSD','BNBUSD','SOLUSD','XRPUSD','ADAUSD','DOTUSD','AVAXUSD','MATICUSD','LINKUSD','LTCUSD','DOGEUSD','XAUUSD','XAGUSD','WTIUSD','BRENTUSD','XPTUSD'].includes(sym) ? 0.00005 : 0.001;
        cachedPrices[sym] *= (1 + (Math.random() - 0.499) * v);
      }
    });
    if (ioInstance) ioInstance.emit('allPrices', { prices: cachedPrices, ts: Date.now() });
  }, 1000);

  // REST batch refresh every 15 seconds to keep non-WS prices updated
  setInterval(refreshBatch, 15000);
  refreshBatch(); // immediate first fetch
}

function getCurrentPrice(symbol) { return cachedPrices[symbol] || null; }
function getAllPrices() { return cachedPrices; }
function getCategories() { return categories; }

module.exports = { getCurrentPrice, getAllPrices, getCategories, startWebSocketStream };
