const express = require('express');
const auth = require('../middleware/auth');
const User = require('../models/User');
const Order = require('../models/Order');
const Position = require('../models/Position');
const { getCurrentPrice } = require('../services/marketData');
const router = express.Router();

router.post('/order', auth, async (req, res) => {
  const { symbol, side, quantity } = req.body;
  if (!symbol || !side || !quantity || quantity <= 0) return res.status(400).json({ msg: 'Invalid order' });
  const price = getCurrentPrice(symbol);
  if (!price) return res.status(400).json({ msg: 'Price not available' });

  const session = await User.startSession();
  session.startTransaction();
  try {
    const user = await User.findById(req.user.id).session(session);
    const notional = quantity * price;

    if (side === 'buy') {
      if (user.balance < notional) throw new Error('Insufficient balance');
      user.balance -= notional;
      await user.save({ session });
      let pos = await Position.findOne({ userId: user.id, symbol }).session(session);
      if (pos) {
        pos.quantity += quantity;
        pos.avgPrice = ((pos.avgPrice * (pos.quantity - quantity)) + (price * quantity)) / pos.quantity;
        await pos.save({ session });
      } else {
        pos = new Position({ userId: user.id, symbol, quantity, avgPrice: price });
        await pos.save({ session });
      }
    } else if (side === 'sell') {
      let pos = await Position.findOne({ userId: user.id, symbol }).session(session);
      if (!pos || pos.quantity < quantity) throw new Error('Not enough position');
      pos.quantity -= quantity;
      if (pos.quantity === 0) await pos.deleteOne({ session });
      else await pos.save({ session });
      user.balance += notional;
      await user.save({ session });
    }

    const order = new Order({ userId: user.id, symbol, side, quantity, price });
    await order.save({ session });
    await session.commitTransaction();
    res.json({ msg: 'Order executed', balance: user.balance });
  } catch (err) {
    await session.abortTransaction();
    res.status(400).json({ msg: err.message });
  } finally { session.endSession(); }
});

module.exports = router;
