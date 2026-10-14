// Counter multiplayer server (Render-ready): serves counter.html and relays game state over WebSocket.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  fs.readFile(path.join(__dirname, 'counter.html'), (err, data) => {
    if (err) { res.writeHead(500); res.end('counter.html not found'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });
const players = new Map();
let nextId = 1;

const scoreMsg = () => {
  const team = { T: 0, CT: 0 };
  for (const p of players.values()) team[p.team] += p.k;
  return { t: 'scores', list: [...players.values()].map(p => ({ id: p.id, k: p.k, d: p.d })), team };
};
const broadcast = (msg, except) => {
  const s = JSON.stringify(msg);
  for (const p of players.values()) if (p !== except && p.ws.readyState === 1) p.ws.send(s);
};
const pub = p => ({ id: p.id, name: p.name, team: p.team, k: p.k, d: p.d });

wss.on('connection', ws => {
  let me = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'ping') return;
    if (m.t === 'join' && !me) {
      let ct = 0, t = 0;
      for (const p of players.values()) p.team === 'CT' ? ct++ : t++;
      const team = ct <= t ? 'CT' : 'T';
      me = { id: nextId++, name: String(m.name || 'Player').slice(0, 16), team, k: 0, d: 0, ws };
      players.set(me.id, me);
      const team_scores = scoreMsg().team;
      ws.send(JSON.stringify({ t: 'welcome', id: me.id, team, team_scores, players: [...players.values()].map(pub) }));
      broadcast({ t: 'join', p: pub(me) }, me);
      broadcast(scoreMsg());
      return;
    }
    if (!me) return;
    switch (m.t) {
      case 's':
      case 'shot':
        m.id = me.id; broadcast(m, me); break;
      case 'hit': {
        const target = players.get(m.to);
        if (target && target !== me && target.team !== me.team && target.ws.readyState === 1) {
          const dmg = Math.max(0, Math.min(150, +m.dmg || 0));
          target.ws.send(JSON.stringify({ t: 'dmg', from: me.id, dmg, hs: m.hs ? 1 : 0, w: m.w | 0 }));
        }
        break;
      }
      case 'died': {
        me.d++;
        const killer = players.get(m.killer);
        if (killer && killer !== me) killer.k++;
        broadcast({ t: 'kill', killer: m.killer, victim: me.id, w: m.w | 0, hs: m.hs ? 1 : 0 });
        broadcast(scoreMsg());
        break;
      }
      case 'respawn':
        broadcast({ t: 'resp', id: me.id }, me); break;
    }
  });

  ws.on('close', () => {
    if (!me) return;
    players.delete(me.id);
    broadcast({ t: 'leave', id: me.id });
    broadcast(scoreMsg());
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log('Counter server listening on ' + PORT));
