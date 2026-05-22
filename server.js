require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
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
