const mongoose = require('mongoose');
const TransactionSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  type: { type: String, enum: ['deposit','withdrawal'], required: true },
  amount: { type: Number, required: true },
  currency: { type: String, default: 'USD' },
  method: { type: String, default: 'crypto' },
  status: { type: String, enum: ['pending','completed','failed'], default: 'pending' },
  txHash: String,
  createdAt: { type: Date, default: Date.now }
});
module.exports = mongoose.model('Transaction', TransactionSchema);
