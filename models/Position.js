const mongoose = require('mongoose');
const PositionSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  symbol: { type: String, required: true },
  quantity: { type: Number, required: true },
  avgPrice: { type: Number, required: true },
  createdAt: { type: Date, default: Date.now }
});
module.exports = mongoose.model('Position', PositionSchema);
