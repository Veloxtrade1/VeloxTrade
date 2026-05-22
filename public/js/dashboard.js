const token = localStorage.getItem('token'); if (!token) window.location.href = '/';
const socket = io();
let currentPrices = {};
socket.on('initPrices', (prices) => { currentPrices = prices; updatePriceDisplay(); });
socket.on('priceUpdate', (prices) => { Object.assign(currentPrices, prices); updatePriceDisplay(); });
function updatePriceDisplay() { const sym = document.getElementById('symbolSelect').value; const span = document.getElementById('currentPrice'); if (span && currentPrices[sym]) span.innerText = currentPrices[sym].toFixed(sym === 'BTCUSD' ? 2 : 5); }
async function fetchUser() { const res = await fetch('/api/account/me', { headers:{'x-auth-token':token} }); const user = await res.json(); document.getElementById('userEmail').innerText = user.email; document.getElementById('balance').innerText = user.balance.toFixed(2); }
async function fetchPositions() { const res = await fetch('/api/account/positions', { headers:{'x-auth-token':token} }); const positions = await res.json(); document.getElementById('positionsList').innerHTML = positions.map(p => `<li>${p.symbol} | ${p.quantity} lots @ ${p.avgPrice}</li>`).join(''); }
async function fetchOrders() { const res = await fetch('/api/account/orders', { headers:{'x-auth-token':token} }); const orders = await res.json(); document.getElementById('ordersList').innerHTML = orders.map(o => `<li>${o.side.toUpperCase()} ${o.quantity} ${o.symbol} @ ${o.price}</li>`).join(''); }
async function placeOrder(side) { const symbol = document.getElementById('symbolSelect').value; const quantity = parseFloat(document.getElementById('quantity').value); if (!quantity || quantity<=0) return alert('Enter quantity'); const res = await fetch('/api/trading/order', { method:'POST', headers:{'Content-Type':'application/json','x-auth-token':token}, body:JSON.stringify({ symbol, side, quantity }) }); const data = await res.json(); if (res.ok) { document.getElementById('tradeMsg').innerHTML = `<span style="color:green;">Order filled! Balance: $${data.balance.toFixed(2)}</span>`; document.getElementById('balance').innerText = data.balance.toFixed(2); fetchPositions(); fetchOrders(); } else alert(data.msg); }
document.getElementById('buyBtn')?.addEventListener('click',()=>placeOrder('buy')); document.getElementById('sellBtn')?.addEventListener('click',()=>placeOrder('sell'));
document.getElementById('depositBtn')?.addEventListener('click', async()=>{ const res = await fetch('/api/account/deposit-address', { headers:{'x-auth-token':token} }); const data = await res.json(); document.getElementById('btcAddr').innerText = data.btc; document.getElementById('usdtAddr').innerText = data.usdt; document.getElementById('btcQR').src = data.btcQR; document.getElementById('usdtQR').src = data.usdtQR; document.getElementById('depositModal').style.display = 'block'; });
document.getElementById('logoutBtn')?.addEventListener('click',()=>{ localStorage.removeItem('token'); location.href='/'; });
document.getElementById('symbolSelect')?.addEventListener('change', updatePriceDisplay);
window.copyText = (id) => { const text = document.getElementById(id).innerText; navigator.clipboard.writeText(text); alert('Copied'); };
window.closeModal = () => document.getElementById('depositModal').style.display = 'none';
fetchUser(); fetchPositions(); fetchOrders();
