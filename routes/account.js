const express = require('express');
const auth = require('../middleware/auth');
const User = require('../models/User');
const Position = require('../models/Position');
const Order = require('../models/Order');
const Transaction = require('../models/Transaction');
const QRCode = require('qrcode');
const router = express.Router();

router.get('/me', auth, async (req, res) => {
  const user = await User.findById(req.user.id).select('-password');
  res.json(user);
});

router.get('/positions', auth, async (req, res) => {
  const positions = await Position.find({ userId: req.user.id });
  res.json(positions);
});

router.get('/orders', auth, async (req, res) => {
  const orders = await Order.find({ userId: req.user.id }).sort('-createdAt');
  res.json(orders);
});

router.get('/deposit-address', auth, async (req, res) => {
  const btcAddr = process.env.CRYPTO_BTC_ADDRESS;
  const usdtAddr = process.env.CRYPTO_USDT_ADDRESS;
  const btcQR = await QRCode.toDataURL(btcAddr);
  const usdtQR = await QRCode.toDataURL(usdtAddr);
  res.json({ btc: btcAddr, usdt: usdtAddr, btcQR, usdtQR });
});

router.post('/confirm-deposit', auth, async (req, res) => {
  const { txHash, amount, currency } = req.body;
  const transaction = new Transaction({
    userId: req.user.id,
    type: 'deposit',
    amount,
    currency,
    txHash,
    status: 'completed'
  });
  await transaction.save();
  await User.findByIdAndUpdate(req.user.id, { $inc: { balance: amount } });
  res.json({ msg: 'Deposit confirmed', balance: (await User.findById(req.user.id)).balance });
});

module.exports = router;
