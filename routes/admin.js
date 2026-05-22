const express = require('express');
const User = require('../models/User');
const Transaction = require('../models/Transaction');
const router = express.Router();

const adminAuth = (req, res, next) => {
  const key = req.headers['admin-key'];
  if (key !== process.env.ADMIN_SECRET_KEY) return res.status(403).json({ msg: 'Forbidden' });
  next();
};

router.get('/users', adminAuth, async (req, res) => {
  const users = await User.find().select('-password');
  res.json(users);
});

router.post('/user/:id/balance', adminAuth, async (req, res) => {
  const { amount } = req.body;
  const user = await User.findByIdAndUpdate(req.params.id, { $inc: { balance: amount } }, { new: true });
  res.json({ balance: user.balance });
});

router.get('/pending-deposits', adminAuth, async (req, res) => {
  const pending = await Transaction.find({ type: 'deposit', status: 'pending' }).populate('userId');
  res.json(pending);
});

router.post('/confirm-tx/:id', adminAuth, async (req, res) => {
  const tx = await Transaction.findById(req.params.id);
  if (!tx) return res.status(404).json({ msg: 'Not found' });
  tx.status = 'completed';
  await tx.save();
  await User.findByIdAndUpdate(tx.userId, { $inc: { balance: tx.amount } });
  res.json({ msg: 'Confirmed' });
});

module.exports = router;
