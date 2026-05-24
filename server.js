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
  <title>Join Whiteboard — Munjez</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
  <style>
    *, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }

    :root {
      --bg: #080c18;
      --surface: #0f1526;
      --border: #1c2640;
      --accent: #6366f1;
      --accent-glow: #6366f133;
      --accent-soft: #6366f118;
      --text: #e8eaf0;
      --muted: #5a6480;
      --muted2: #8892aa;
      --success: #10b981;
      --radius: 18px;
    }

    body {
      min-height: 100dvh;
      display: flex;
      align-items: center;
      justify-content: center;
      background: var(--bg);
      font-family: 'Inter', system-ui, sans-serif;
      color: var(--text);
      padding: 20px;
      overflow: hidden;
    }

    /* Background grid */
    body::before {
      content: '';
      position: fixed;
      inset: 0;
      background-image:
        linear-gradient(var(--border) 1px, transparent 1px),
        linear-gradient(90deg, var(--border) 1px, transparent 1px);
      background-size: 40px 40px;
      opacity: 0.35;
      pointer-events: none;
    }

    /* Radial glow behind card */
    body::after {
      content: '';
      position: fixed;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      width: 600px;
      height: 600px;
      background: radial-gradient(circle, #6366f114 0%, transparent 70%);
      pointer-events: none;
    }

    .card {
      position: relative;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 36px 32px 32px;
      max-width: 420px;
      width: 100%;
      box-shadow: 0 32px 80px rgba(0,0,0,0.6), 0 0 0 1px #ffffff06 inset;
      animation: slideUp 0.4s cubic-bezier(0.16,1,0.3,1) both;
    }

    @keyframes slideUp {
      from { opacity: 0; transform: translateY(20px) scale(0.97); }
      to   { opacity: 1; transform: translateY(0)   scale(1); }
    }

    /* Top accent line */
    .card::before {
      content: '';
      position: absolute;
      top: 0; left: 32px; right: 32px;
      height: 1px;
      background: linear-gradient(90deg, transparent, var(--accent), transparent);
    }

    .logo-row {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 28px;
    }

    .logo-icon {
      width: 44px;
      height: 44px;
      border-radius: 13px;
      background: var(--accent);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 4px 20px var(--accent-glow);
      flex-shrink: 0;
    }

    .logo-icon svg { display: block; }

    .logo-text { line-height: 1; }
    .logo-text .app-name { font-size: 17px; font-weight: 700; letter-spacing: -0.02em; }
    .logo-text .app-sub  { font-size: 12px; color: var(--muted2); margin-top: 3px; }

    .divider {
      height: 1px;
      background: var(--border);
      margin-bottom: 24px;
    }

    /* Status pill */
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      background: var(--accent-soft);
      border: 1px solid #6366f128;
      border-radius: 100px;
      padding: 5px 12px;
      font-size: 12px;
      font-weight: 600;
      color: #a5b4fc;
      margin-bottom: 20px;
    }

    .status-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--accent);
      animation: pulse 1.8s ease-in-out infinite;
      box-shadow: 0 0 6px var(--accent);
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50%       { opacity: 0.5; transform: scale(0.8); }
    }

    h1 {
      font-size: 22px;
      font-weight: 700;
      letter-spacing: -0.03em;
      line-height: 1.2;
      margin-bottom: 8px;
    }

    .subtitle {
      font-size: 13px;
      color: var(--muted2);
      line-height: 1.6;
      margin-bottom: 24px;
    }

    /* Room code chip */
    .room-chip {
      display: flex;
      align-items: center;
      gap: 10px;
      background: #6366f10d;
      border: 1.5px solid #6366f128;
      border-radius: 12px;
      padding: 11px 14px;
      margin-bottom: 20px;
    }

    .room-chip-icon { color: var(--accent); flex-shrink: 0; }

    .room-chip-label {
      font-size: 10px;
      font-weight: 700;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .room-chip-code {
      font-family: 'JetBrains Mono', 'Fira Code', monospace;
      font-size: 13px;
      font-weight: 700;
      color: #a5b4fc;
      letter-spacing: 0.03em;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    /* Open button */
    .btn {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      width: 100%;
      background: var(--accent);
      color: #fff;
      text-decoration: none;
      border-radius: 13px;
      padding: 14px 0;
      font-size: 15px;
      font-weight: 700;
      letter-spacing: -0.01em;
      box-shadow: 0 4px 20px var(--accent-glow);
      transition: opacity 0.15s, transform 0.15s;
      margin-bottom: 14px;
    }

    .btn:hover  { opacity: 0.88; transform: translateY(-1px); }
    .btn:active { opacity: 1;    transform: translateY(0); }

    .hint {
      font-size: 12px;
      color: var(--muted);
      text-align: center;
      line-height: 1.6;
    }

    .hint strong { color: var(--muted2); font-weight: 600; }

    /* Spinner shown while redirecting */
    #auto-msg {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      font-size: 13px;
      color: var(--muted2);
      margin-bottom: 16px;
    }

    .spinner {
      width: 14px; height: 14px;
      border: 2px solid var(--border);
      border-top-color: var(--accent);
      border-radius: 50%;
      animation: spin 0.7s linear infinite;
      flex-shrink: 0;
    }

    @keyframes spin { to { transform: rotate(360deg); } }

    #fallback { display: none; }
  </style>
  <script>
    window.location.href = ${JSON.stringify(deepLink)};
    setTimeout(() => {
      document.getElementById('auto-msg').style.display = 'none';
      document.getElementById('fallback').style.display = 'block';
    }, 2000);
  </script>
</head>
<body>
  <div class="card">

    <div class="logo-row">
      <div class="logo-icon">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <rect x="3" y="3" width="18" height="18" rx="3"/>
          <path d="M8 12h8M12 8v8"/>
        </svg>
      </div>
      <div class="logo-text">
        <div class="app-name">Munjez</div>
        <div class="app-sub">Collaborative Whiteboard</div>
      </div>
    </div>

    <div class="divider"></div>

    <div class="status-pill">
      <span class="status-dot"></span>
      You've been invited
    </div>

    <h1>Join the Whiteboard</h1>
    <p class="subtitle">Someone shared a board with you. Open the app to start collaborating in real time.</p>

    <div class="room-chip">
      <span class="room-chip-icon">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
          <rect x="2" y="7" width="20" height="14" rx="2"/>
          <path d="M16 7V5a2 2 0 00-2-2h-4a2 2 0 00-2 2v2"/>
        </svg>
      </span>
      <div style="flex:1;overflow:hidden">
        <div class="room-chip-label">Room Code</div>
        <div class="room-chip-code" title="${roomCode}">${roomCode}</div>
      </div>
    </div>

    <div id="auto-msg">
      <span class="spinner"></span>
      Opening Munjez automatically…
    </div>

    <div id="fallback">
      <a class="btn" href="${deepLink}">
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4"/>
          <polyline points="10 17 15 12 10 7"/>
          <line x1="15" y1="12" x2="3" y2="12"/>
        </svg>
        Open in Munjez
      </a>
      <p class="hint">App didn't open? Make sure <strong>Munjez</strong> is installed on your device.</p>
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
