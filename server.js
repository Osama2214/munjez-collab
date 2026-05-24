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
const fs   = require('fs');
const path = require('path');
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
  if (req.url === '/icon.webp') {
    const iconPath = path.join(__dirname, 'icon.webp');
    fs.readFile(iconPath, (err, data) => {
      if (err) { res.writeHead(404); res.end(); return; }
      res.writeHead(200, { 'Content-Type': 'image/webp', 'Cache-Control': 'public,max-age=86400' });
      res.end(data);
    });

  } else if (req.url === '/health') {
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
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet"/>
  <style>
    *,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
    :root{
      --bg:#070c18;--surface:#0c1223;--card:#101827;
      --border:#1a2d4a;--border-h:rgba(124,58,237,.5);
      --primary:#7c3aed;--primary-h:#6d28d9;--primary-glow:rgba(124,58,237,.35);
      --accent:#a78bfa;--text:#e2e8f0;--muted:#7c8fa6;--dim:#3a4a60;
      --green:#10b981;--r:14px;
    }
    html{scroll-behavior:smooth}
    body{
      font-family:'Inter',-apple-system,BlinkMacSystemFont,system-ui,sans-serif;
      background:var(--bg);color:var(--text);
      min-height:100dvh;display:flex;align-items:center;justify-content:center;
      padding:24px;overflow:hidden;position:relative;
    }

    /* ── CANVAS particles ── */
    #particles{position:fixed;inset:0;z-index:0;pointer-events:none;opacity:.55}

    /* ── MOUSE GLOW ── */
    .mouse-glow{
      position:fixed;inset:0;z-index:1;pointer-events:none;
      background:radial-gradient(600px circle at var(--mx,50%) var(--my,50%),rgba(124,58,237,.07),transparent 40%);
    }

    /* ── AMBIENT ORBS ── */
    .orb{position:fixed;border-radius:50%;filter:blur(80px);pointer-events:none;animation:float-orb 8s ease-in-out infinite}
    .orb1{width:500px;height:500px;background:rgba(124,58,237,.11);top:-150px;left:-150px;animation-delay:0s}
    .orb2{width:350px;height:350px;background:rgba(99,102,241,.09);bottom:-100px;right:-100px;animation-delay:-3s}
    .orb3{width:250px;height:250px;background:rgba(168,85,247,.07);top:50%;left:65%;animation-delay:-5s}
    @keyframes float-orb{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(20px,-20px) scale(1.05)}}

    /* ── DOT GRID ── */
    .dot-grid{
      position:fixed;inset:0;z-index:0;pointer-events:none;
      background-image:radial-gradient(circle,rgba(255,255,255,.03) 1px,transparent 1px);
      background-size:28px 28px;
      mask-image:radial-gradient(ellipse 80% 80% at 50% 50%,black 0%,transparent 100%);
    }

    /* ── CARD ── */
    .card{
      position:relative;z-index:10;
      background:rgba(12,18,35,.9);
      border:1px solid var(--border);
      border-radius:20px;
      padding:40px 36px 36px;
      max-width:440px;width:100%;
      box-shadow:0 40px 100px rgba(0,0,0,.7),0 0 0 1px rgba(255,255,255,.04) inset;
      backdrop-filter:blur(24px);
      animation:slide-up .6s cubic-bezier(0.16,1,0.3,1) both;
    }
    @keyframes slide-up{from{opacity:0;transform:translateY(28px) scale(0.96)}to{opacity:1;transform:none}}

    /* top accent line */
    .card::before{
      content:'';position:absolute;top:0;left:36px;right:36px;height:1px;
      background:linear-gradient(90deg,transparent,var(--primary),transparent);
    }

    /* ── LOGO ROW ── */
    .logo-row{display:flex;align-items:center;gap:12px;margin-bottom:28px;animation:fade-up .7s ease both;animation-delay:.1s}
    .logo-icon{
      width:46px;height:46px;border-radius:13px;flex-shrink:0;
      box-shadow:0 0 28px var(--primary-glow);
      overflow:hidden;
    }
    .logo-text .app-name{font-size:17px;font-weight:800;letter-spacing:-.3px;line-height:1}
    .logo-text .app-sub{font-size:12px;color:var(--muted);margin-top:3px}

    /* ── DIVIDER ── */
    .divider{height:1px;background:var(--border);margin-bottom:28px}

    /* ── BADGE ── */
    .badge{
      display:inline-flex;align-items:center;gap:8px;
      background:rgba(124,58,237,.1);border:1px solid rgba(124,58,237,.3);
      color:var(--accent);padding:6px 16px;border-radius:100px;
      font-size:12px;font-weight:600;margin-bottom:20px;
      animation:fade-up .7s ease both;animation-delay:.2s;
    }
    .badge-dot{width:6px;height:6px;background:var(--accent);border-radius:50%;animation:blink 2.2s infinite;box-shadow:0 0 6px var(--accent)}
    @keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}

    /* ── HEADING ── */
    h1{
      font-size:28px;font-weight:900;letter-spacing:-.6px;line-height:1.15;
      margin-bottom:10px;
      background:linear-gradient(140deg,#c4b5fd 0%,#8b5cf6 40%,#a78bfa 100%);
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
      animation:fade-up .7s ease both;animation-delay:.25s;
    }
    .subtitle{
      font-size:14px;color:var(--muted);line-height:1.7;margin-bottom:28px;
      animation:fade-up .7s ease both;animation-delay:.3s;
    }

    /* ── ROOM CHIP ── */
    .room-chip{
      display:flex;align-items:center;gap:12px;
      background:rgba(124,58,237,.07);
      border:1.5px solid rgba(124,58,237,.22);
      border-radius:12px;padding:13px 16px;margin-bottom:24px;
      transition:border-color .2s,background .2s;
      animation:fade-up .7s ease both;animation-delay:.35s;
    }
    .room-chip:hover{border-color:rgba(124,58,237,.45);background:rgba(124,58,237,.12)}
    .room-chip-icon{color:var(--accent);flex-shrink:0}
    .room-chip-label{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.8px}
    .room-chip-code{
      font-family:'JetBrains Mono','Fira Code',monospace;
      font-size:14px;font-weight:700;color:var(--accent);
      letter-spacing:.03em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
      margin-top:2px;
    }

    /* ── AUTO MSG ── */
    #auto-msg{
      display:flex;align-items:center;justify-content:center;gap:9px;
      font-size:13px;color:var(--muted);margin-bottom:18px;
      animation:fade-up .7s ease both;animation-delay:.4s;
    }
    .spinner{
      width:15px;height:15px;
      border:2px solid var(--border);border-top-color:var(--primary);
      border-radius:50%;animation:spin .75s linear infinite;flex-shrink:0;
    }
    @keyframes spin{to{transform:rotate(360deg)}}

    /* ── FALLBACK ── */
    #fallback{display:none;animation:fade-up .4s ease both}

    /* ── BUTTON ── */
    .btn{
      display:flex;align-items:center;justify-content:center;gap:9px;
      width:100%;background:var(--primary);color:#fff;
      text-decoration:none;border-radius:12px;padding:15px 0;
      font-size:15px;font-weight:700;letter-spacing:-.01em;
      box-shadow:0 0 48px var(--primary-glow);
      transition:background .2s,transform .2s,box-shadow .2s;
      position:relative;overflow:hidden;margin-bottom:16px;
    }
    .btn::before{
      content:'';position:absolute;inset:0;
      background:linear-gradient(135deg,rgba(255,255,255,.15) 0%,transparent 60%);
      opacity:0;transition:opacity .2s;
    }
    .btn:hover{background:var(--primary-h);transform:translateY(-2px);box-shadow:0 0 72px rgba(124,58,237,.6)}
    .btn:hover::before{opacity:1}
    .btn:active{transform:none}
    .btn svg{animation:bounce-dl 1.2s ease-in-out infinite}
    @keyframes bounce-dl{0%,100%{transform:translateY(0)}50%{transform:translateY(3px)}}

    /* ── HINT ── */
    .hint{font-size:12px;color:var(--dim);text-align:center;line-height:1.7}
    .hint strong{color:var(--muted);font-weight:600}

    /* ── FOOTER NOTE ── */
    .footer-note{
      margin-top:24px;padding-top:20px;border-top:1px solid var(--border);
      display:flex;align-items:center;justify-content:center;gap:8px;
      font-size:11.5px;color:var(--dim);
    }
    .footer-note span{width:5px;height:5px;background:var(--green);border-radius:50%;display:inline-block;box-shadow:0 0 6px var(--green)}

    @keyframes fade-up{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:none}}
  </style>
  <script>
    setTimeout(() => { window.location.href = ${JSON.stringify(deepLink)}; }, 800);
    setTimeout(() => {
      const autoMsg  = document.getElementById('auto-msg');
      const fallback = document.getElementById('fallback');
      if (autoMsg)  autoMsg.style.display  = 'none';
      if (fallback) fallback.style.display = 'block';
    }, 2500);
  </script>
</head>
<body>
  <div class="mouse-glow" id="mglow"></div>
  <div class="dot-grid"></div>
  <div class="orb orb1"></div>
  <div class="orb orb2"></div>
  <div class="orb orb3"></div>
  <canvas id="particles"></canvas>

  <div class="card">
    <div class="logo-row">
      <div class="logo-icon">
        <img src="/icon.webp" width="46" height="46" alt="Munjez" style="border-radius:13px;display:block;object-fit:cover"/>
      </div>
      <div class="logo-text">
        <div class="app-name">Munjez</div>
        <div class="app-sub">Collaborative Whiteboard</div>
      </div>
    </div>

    <div class="divider"></div>

    <div class="badge">
      <span class="badge-dot"></span>
      You've been invited
    </div>

    <h1>Join the Whiteboard</h1>
    <p class="subtitle">Someone shared a board with you. Open the app to start collaborating in real time.</p>

    <div class="room-chip">
      <span class="room-chip-icon">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
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

    <div class="footer-note">
      <span></span>
      Secure real-time collaboration
    </div>
  </div>

  <script>
    // ── Mouse glow ──
    var mglow = document.getElementById('mglow');
    document.addEventListener('mousemove', function(e){
      mglow.style.setProperty('--mx', e.clientX + 'px');
      mglow.style.setProperty('--my', e.clientY + 'px');
    });

    // ── Particles ──
    (function(){
      var canvas = document.getElementById('particles');
      var ctx = canvas.getContext('2d');
      var W, H, pts = [];
      function resize(){
        W = canvas.width  = window.innerWidth;
        H = canvas.height = window.innerHeight;
      }
      resize();
      window.addEventListener('resize', resize);
      for(var i=0;i<55;i++){
        pts.push({
          x:Math.random()*1000,y:Math.random()*1000,
          vx:(Math.random()-.5)*.3,vy:(Math.random()-.5)*.3,
          r:Math.random()*1.6+.4,
          a:Math.random()
        });
      }
      function draw(){
        ctx.clearRect(0,0,W,H);
        for(var i=0;i<pts.length;i++){
          var p=pts[i];
          p.x+=p.vx; p.y+=p.vy;
          if(p.x<0)p.x=W; if(p.x>W)p.x=0;
          if(p.y<0)p.y=H; if(p.y>H)p.y=0;
          ctx.beginPath();
          ctx.arc(p.x,p.y,p.r,0,Math.PI*2);
          ctx.fillStyle='rgba(167,139,250,'+p.a+')';
          ctx.fill();
        }
        requestAnimationFrame(draw);
      }
      draw();
    })();
  </script>
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
