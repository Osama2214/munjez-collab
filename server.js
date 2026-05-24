/**
 * Munjez Collaboration Server
 * ───────────────────────────
 * Yjs WebSocket server — handles:
 *   • CRDT sync (graph XML + strokes) via y-websocket
 *   • Presence channel (cursors, user names, colors) via a lightweight
 *     JSON broadcast layer on the PRESENCE connection
 *
 * Deploy free on Railway / Render / Fly.io:
 *   railway up   (or)   render deploy
 *
 * Local dev:
 *   node server.js        → ws://localhost:1234
 */

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

// ── y-websocket compat shim ──────────────────────────────────────────────────
// v2.x → bin/utils.cjs  |  v3.x removed bin/ entirely (setupWSConnection gone)
// Pin to 2.x in package.json; this shim is a safety net only.
let setupWSConnection;
try {
  ({ setupWSConnection } = require('y-websocket/bin/utils'));
} catch {
  console.error(
    '[FATAL] y-websocket/bin/utils not found.\n' +
    '  This server requires y-websocket@2.x.\n' +
    '  Run:  npm install y-websocket@2.0.4'
  );
  process.exit(1);
}
// ─────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 1234;

// ── Presence tracking ────────────────────────────────────────────────────────
// rooms: Map<roomName, Map<clientId, { ws, user }>>
// ONLY presence connections are tracked here.
// Yjs CRDT connections are managed entirely by setupWSConnection — they are
// intentionally NOT added to this map to prevent phantom disconnect/rejoin
// events when the Yjs socket reconnects after a network blip.
const rooms = new Map();

function getRoomClients(room) {
  if (!rooms.has(room)) rooms.set(room, new Map());
  return rooms.get(room);
}

function broadcastPresence(room, excludeId) {
  const clients = getRoomClients(room);
  const users = [];
  for (const [id, { user }] of clients) users.push({ id, ...user });
  const msg = JSON.stringify({ type: 'presence', users });
  for (const [id, { ws }] of clients) {
    if (id !== excludeId && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function broadcastCursor(room, fromId, cursorData) {
  const clients = getRoomClients(room);
  const msg = JSON.stringify({ type: 'cursor', id: fromId, ...cursorData });
  for (const [id, { ws }] of clients) {
    if (id !== fromId && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

// ── HTTP server (health check for Railway/Render) ────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));

  } else if (req.url.startsWith('/join/')) {
    // Deep-link redirect — munjez:// deep link for the installed app.
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
    setTimeout(() => {
      document.getElementById('fallback').style.display = 'block';
    }, 2000);
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

// ── WebSocket server ─────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'ws://localhost');
  const isPresence = url.pathname.startsWith('/presence/');

  const room = isPresence
    ? url.pathname.replace(/^\/presence\//, '').replace(/^\/+/, '') || 'default'
    : url.pathname.replace(/^\/+/, '') || 'default';

  const params    = url.searchParams;
  const clientId  = params.get('clientId') || Math.random().toString(36).slice(2);
  const userName  = (params.get('name')  || 'Anonymous').slice(0, 40);
  const userColor = (params.get('color') || '#6366f1').slice(0, 20);

  // ── Yjs CRDT connection (/room) ────────────────────────────────────────────
  // Handed entirely to setupWSConnection. Not tracked in rooms Map at all.
  // This prevents phantom leave/join events when the Yjs socket auto-reconnects
  // after a network blip (which happens independently of the presence socket).
  if (!isPresence) {
    setupWSConnection(ws, req, { docName: room, gc: true });
    return; // ← nothing else to do for this connection
  }

  // ── Presence connection (/presence/room) ──────────────────────────────────
  // All cursor, user-list, and ping/pong logic lives here.

  const clients = getRoomClients(room);

  // If this clientId already has an open presence socket (e.g. page reload
  // before the old socket's close event fires), gracefully close the old one
  // so the rooms Map stays consistent.
  const existing = clients.get(clientId);
  if (existing && existing.ws.readyState === WebSocket.OPEN) {
    existing.ws.close(1000, 'replaced by new connection');
  }

  clients.set(clientId, {
    ws,
    user: { name: userName, color: userColor, cursor: null, joinedAt: Date.now() },
  });

  // Send the new client the full current presence snapshot (including itself)
  const allUsers = [];
  for (const [id, { user }] of clients) allUsers.push({ id, ...user });
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'presence', users: allUsers }));
  }

  // Tell everyone else someone joined
  broadcastPresence(room, clientId);

  // Message handler: cursor updates + heartbeat pings
  ws.on('message', (data) => {
    // Only handle text frames (JSON). Ignore anything else.
    const str = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data.toString('utf8') : null;
    if (!str || !str.startsWith('{')) return;

    let msg;
    try { msg = JSON.parse(str); } catch { return; }

    switch (msg.type) {
      case 'cursor': {
        const client = clients.get(clientId);
        if (client) client.user.cursor = { x: msg.x, y: msg.y };
        broadcastCursor(room, clientId, { x: msg.x, y: msg.y, name: userName, color: userColor });
        break;
      }
      case 'ping':
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'pong' }));
        }
        break;
      default:
        break;
    }
  });

  ws.on('close', () => {
    // Only remove if this is still the current socket for this clientId.
    // (Avoids removing a replacement socket registered during reconnect.)
    const current = clients.get(clientId);
    if (current && current.ws === ws) {
      clients.delete(clientId);
      if (clients.size === 0) {
        rooms.delete(room);
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