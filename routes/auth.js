const express = require('express');
const jwt = require('jsonwebtoken');
const User = require('../models/User');
const router = express.Router();

router.post('/register', async (req, res) => {
  try {
    const { email, password, fullName, country } = req.body;
    if (await User.findOne({ email })) return res.status(400).json({ msg: 'User exists' });
    const allowed = ['Pakistan','Bangladesh','Sri Lanka','Nepal','Bhutan','Maldives'];
    if (!allowed.includes(country)) return res.status(400).json({ msg: 'Country not supported' });
    const user = new User({ email, password, fullName, country });
    await user.save();
    const token = jwt.sign({ user: { id: user.id } }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: user.id, email, balance: user.balance, fullName, country, kycStatus: user.kycStatus } });
  } catch (err) { res.status(500).json({ msg: err.message }); }
});

router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email });
    if (!user) return res.status(400).json({ msg: 'Invalid credentials' });
    const match = await user.comparePassword(password);
    if (!match) return res.status(400).json({ msg: 'Invalid credentials' });
    const token = jwt.sign({ user: { id: user.id } }, process.env.JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: user.id, email, balance: user.balance, fullName: user.fullName, country: user.country, kycStatus: user.kycStatus } });
  } catch (err) { res.status(500).json({ msg: err.message }); }
});

module.exports = router;
