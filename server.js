/**
 * Munjez Collaboration Server
 * ───────────────────────────
 * Yjs WebSocket server — handles:
 *   • CRDT sync (graph XML + strokes) via y-websocket
 *   • Presence channel (cursors, user names, colors) via a lightweight
 *     JSON broadcast layer on the PRESENCE connection
 *   • Owner-based kick (temporary) and ban (room-scoped, until server restart)
 */

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

let setupWSConnection;
try {
  ({ setupWSConnection } = require('y-websocket/bin/utils'));
} catch {
  console.error('[FATAL] y-websocket/bin/utils not found. Run: npm install y-websocket@2.0.4');
  process.exit(1);
}

const PORT = process.env.PORT || 1234;

// ── Room state ────────────────────────────────────────────────────────────────
// rooms    : Map<roomName, Map<clientId, { ws, user }>>
// roomMeta : Map<roomName, { ownerId: string, bannedIds: Set<string> }>
const rooms    = new Map();
const roomMeta = new Map();

function getRoomClients(room) {
  if (!rooms.has(room)) rooms.set(room, new Map());
  return rooms.get(room);
}

function getRoomMeta(room) {
  if (!roomMeta.has(room)) roomMeta.set(room, { ownerId: null, bannedIds: new Set() });
  return roomMeta.get(room);
}

function broadcastPresence(room, excludeId) {
  const clients = getRoomClients(room);
  const meta    = getRoomMeta(room);
  const users   = [];
  for (const [id, { user }] of clients) users.push({ id, ...user, isOwner: id === meta.ownerId });
  const msg = JSON.stringify({ type: 'presence', users, ownerId: meta.ownerId });
  for (const [id, { ws }] of clients) {
    if (id !== excludeId && ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

// بيبعت presence للكل بما فيهم الـ sender نفسه (للـ snapshot الأولي)
function sendPresenceTo(ws, room) {
  const clients = getRoomClients(room);
  const meta    = getRoomMeta(room);
  const users   = [];
  for (const [id, { user }] of clients) users.push({ id, ...user, isOwner: id === meta.ownerId });
  if (ws.readyState === WebSocket.OPEN)
    ws.send(JSON.stringify({ type: 'presence', users, ownerId: meta.ownerId }));
}

function broadcastCursor(room, fromId, cursorData) {
  const clients = getRoomClients(room);
  const msg = JSON.stringify({ type: 'cursor', id: fromId, ...cursorData });
  for (const [id, { ws }] of clients) {
    if (id !== fromId && ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

// ── HTTP server ───────────────────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));

  } else if (req.url.startsWith('/join/')) {
    const roomCode = req.url.slice('/join/'.length).split('?')[0].trim();
    if (!roomCode) { res.writeHead(400); res.end('Missing room code'); return; }
    const deepLink = `munjez://join/${roomCode}`;
    const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Opening Munjez…</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{min-height:100dvh;display:flex;align-items:center;justify-content:center;
         background:#0b1020;font-family:system-ui,sans-serif;color:#e2e8f0;text-align:center;padding:24px}
    .card{background:#131929;border:1px solid #1e2d45;border-radius:20px;padding:40px 32px;max-width:400px;width:100%}
    h1{font-size:22px;font-weight:700;margin-bottom:8px}
    p{color:#71717a;font-size:14px;line-height:1.6;margin-bottom:24px}
    .btn{display:inline-block;background:#6366f1;color:#fff;border-radius:12px;
         padding:12px 28px;font-size:15px;font-weight:600;text-decoration:none;
         box-shadow:0 4px 16px #6366f155}
    .code{font-family:monospace;font-size:12px;color:#6366f1;margin-top:20px;
          background:#6366f111;border-radius:8px;padding:8px 14px;word-break:break-all}
  </style>
  <script>
    window.location.href = ${JSON.stringify(deepLink)};
    setTimeout(() => { document.getElementById('fallback').style.display = 'block'; }, 2000);
  </script>
</head>
<body>
  <div class="card">
    <h1>🚀 Opening Munjez…</h1>
    <p>You'll be taken to the shared whiteboard automatically.</p>
    <div id="fallback" style="display:none">
      <p>If the app didn't open, make sure <strong>Munjez</strong> is installed, then tap the button below.</p>
      <a class="btn" href="${deepLink}">Open in Munjez</a>
      <div class="code">${roomCode}</div>
    </div>
  </div>
</body>
</html>`;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  } else {
    res.writeHead(404); res.end();
  }
});

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req) => {
  const url        = new URL(req.url, 'ws://localhost');
  const isPresence = url.pathname.startsWith('/presence/');

  const room = isPresence
    ? url.pathname.replace(/^\/presence\//, '').replace(/^\/+/, '') || 'default'
    : url.pathname.replace(/^\/+/, '') || 'default';

  const params    = url.searchParams;
  const clientId  = params.get('clientId') || Math.random().toString(36).slice(2);
  const userName  = (params.get('name')  || 'Anonymous').slice(0, 40);
  const userColor = (params.get('color') || '#6366f1').slice(0, 20);

  // ── Yjs CRDT connection ────────────────────────────────────────────────────
  if (!isPresence) {
    setupWSConnection(ws, req, { docName: room, gc: true });
    return;
  }

  // ── Presence connection ────────────────────────────────────────────────────
  const meta    = getRoomMeta(room);
  const clients = getRoomClients(room);

  // ── Ban check — رفض الاتصال لو الـ client متحزر ────────────────────────────
  if (meta.bannedIds.has(clientId)) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'banned', reason: 'You have been banned from this room.' }));
      ws.close(1008, 'banned');
    }
    return;
  }

  // ── أول واحد يدخل الروم = owner ────────────────────────────────────────────
  if (!meta.ownerId) meta.ownerId = clientId;

  // استبدل أي connection قديم لنفس الـ clientId
  const existing = clients.get(clientId);
  if (existing && existing.ws.readyState === WebSocket.OPEN) {
    existing.ws.close(1000, 'replaced by new connection');
  }

  clients.set(clientId, {
    ws,
    user: { name: userName, color: userColor, cursor: null, joinedAt: Date.now() },
  });

  // ابعت الـ snapshot الكامل للجديد (بما فيه ownerId)
  sendPresenceTo(ws, room);
  // وابعت للباقيين إنه انضم
  broadcastPresence(room, clientId);

  // ── Message handler ────────────────────────────────────────────────────────
  ws.on('message', (data) => {
    const str = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : null;
    if (!str || !str.startsWith('{')) return;
    let msg;
    try { msg = JSON.parse(str); } catch { return; }

    switch (msg.type) {

      case 'cursor': {
        const client = clients.get(clientId);
        if (client) client.user.cursor = { x: msg.x, y: msg.y };
        broadcastCursor(room, clientId, {
          x: msg.x, y: msg.y,
          name: userName, color: userColor,
          tool: msg.tool, penColor: msg.penColor,
        });
        break;
      }

      // ── Kick: طرد مؤقت — يقدر يرجع ──────────────────────────────────────
      case 'kick': {
        if (clientId !== meta.ownerId) break; // بس الـ owner
        const targetId = msg.targetId;
        const target   = clients.get(targetId);
        if (!target) break;
        if (target.ws.readyState === WebSocket.OPEN) {
          target.ws.send(JSON.stringify({ type: 'kicked', reason: 'You have been removed from this room.' }));
          target.ws.close(1008, 'kicked');
        }
        clients.delete(targetId);
        broadcastPresence(room, null);
        console.log(`[kick] owner ${clientId} kicked ${targetId} from room ${room}`);
        break;
      }

      // ── Ban: حظر دائم من الروم دي ────────────────────────────────────────
      case 'ban': {
        if (clientId !== meta.ownerId) break; // بس الـ owner
        const targetId = msg.targetId;
        meta.bannedIds.add(targetId);
        const target = clients.get(targetId);
        if (target) {
          if (target.ws.readyState === WebSocket.OPEN) {
            target.ws.send(JSON.stringify({ type: 'banned', reason: 'You have been banned from this room.' }));
            target.ws.close(1008, 'banned');
          }
          clients.delete(targetId);
          broadcastPresence(room, null);
        }
        console.log(`[ban] owner ${clientId} banned ${targetId} from room ${room}`);
        break;
      }

      case 'ping':
        if (ws.readyState === WebSocket.OPEN)
          ws.send(JSON.stringify({ type: 'pong' }));
        break;

      default: break;
    }
  });

  ws.on('close', () => {
    const current = clients.get(clientId);
    if (current && current.ws === ws) {
      clients.delete(clientId);
      // لو الـ owner خرج — أول واحد في الروم يبقى owner الجديد
      if (meta.ownerId === clientId) {
        const next = clients.keys().next().value;
        meta.ownerId = next || null;
      }
      if (clients.size === 0) {
        rooms.delete(room);
        roomMeta.delete(room);
      } else {
        broadcastPresence(room, clientId);
      }
    }
  });

  ws.on('error', (err) => {
    console.error(`[ws:presence] client ${clientId} error:`, err.message);
  });
});

httpServer.listen(PORT, () => {
  console.log(`✅ Munjez collab server running on ws://localhost:${PORT}`);
  console.log(`   Health: http://localhost:${PORT}/health`);
});
