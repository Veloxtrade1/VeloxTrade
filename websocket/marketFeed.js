const { getAllPrices, getCategories } = require('../services/marketData');

module.exports = (io) => {
  io.on('connection', (socket) => {
    // Send ALL prices immediately on connect
    socket.emit('initPrices', {
      prices: getAllPrices(),
      categories: getCategories(),
    });

    socket.on('disconnect', () => {});
  });
};
