const socket = io();
socket.on('initPrices', (prices) => {
  const container = document.getElementById('liveTicker');
  if (container) container.innerHTML = Object.entries(prices).map(([s,p]) => `<div><span>${s}</span><span>${p.toFixed(5)}</span></div>`).join('');
});
socket.on('priceUpdate', (prices) => {
  const container = document.getElementById('liveTicker');
  if (container) Object.entries(prices).forEach(([s,p]) => { const divs = container.children; for(let div of divs) if(div.innerText.startsWith(s)) div.innerHTML = `<span>${s}</span><span>${p.toFixed(5)}</span>`; });
});
document.getElementById('loginBtn')?.addEventListener('click', async () => { const email = prompt('Email:'); const pwd = prompt('Password:'); const res = await fetch('/api/auth/login', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({email,password:pwd}) }); const data = await res.json(); if(data.token){ localStorage.setItem('token',data.token); location.href='/dashboard.html'; } else alert('Login failed: ' + data.msg); });
document.getElementById('signupBtn')?.addEventListener('click', async () => { const email = prompt('Email:'); const pwd = prompt('Password:'); const name = prompt('Full name:'); const country = prompt('Country (Pakistan/Bangladesh/Sri Lanka/Nepal/Bhutan/Maldives):'); const res = await fetch('/api/auth/register', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({email,password:pwd,fullName:name,country}) }); const data = await res.json(); if(data.token){ localStorage.setItem('token',data.token); location.href='/dashboard.html'; } else alert('Registration error: ' + data.msg); });
document.getElementById('heroSignupBtn')?.addEventListener('click', () => document.getElementById('signupBtn').click());
