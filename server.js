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
// rooms      : Map<roomName, Map<clientId, { ws, user }>>
// roomMeta   : Map<roomName, { ownerId: string, bannedIds: Set<string>, kickedIds: Set<string> }>
// roomBans   : Map<roomName, Set<clientId>>  ← بتفضل حتى لو الروم اتفرغت
const rooms    = new Map();
const roomMeta = new Map();
const roomBans = new Map(); // persistent bans — مش بتتمسح لما الروم تتفرغ

function getRoomClients(room) {
  if (!rooms.has(room)) rooms.set(room, new Map());
  return rooms.get(room);
}

function getRoomMeta(room) {
  if (!roomMeta.has(room)) {
    // استرجع الـ bans المحفوظة لو موجودة
    const existingBans = roomBans.get(room) || new Set();
    roomMeta.set(room, { ownerId: null, bannedIds: existingBans, kickedIds: new Set() });
  }
  return roomMeta.get(room);
}

function broadcastPresence(room) {
  const clients = getRoomClients(room);
  const meta    = getRoomMeta(room);
  const users   = [];
  for (const [id, { user }] of clients) users.push({ id, ...user, isOwner: id === meta.ownerId });
  const msg = JSON.stringify({ type: 'presence', users, ownerId: meta.ownerId });
  for (const [, { ws }] of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
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

// ── Shared boards storage (in-memory) ────────────────────────────────────────
const sharedBoards = new Map(); // id → { board, graphXml, strokes, createdAt }

function generateShareId() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    const MAX = 50 * 1024 * 1024; // 50 MB limit
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX) { req.destroy(); reject(new Error('Payload too large')); return; }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

// ── Viewer HTML builder ──────────────────────────────────────────────────────
function buildViewerHtml(boardId, boardName, bgColor) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>${boardName} — Munjez</title>
  <link rel="icon" href="/favicon.ico"/>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet"/>
  <style>
    *,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
    :root{--bg:#070c18;--surface:#0c1223;--card:#101827;--border:#1a2d4a;--primary:#7c3aed;--accent:#a78bfa;--text:#e2e8f0;--muted:#7c8fa6}
    html,body{width:100%;height:100%;overflow:hidden;font-family:'Inter',system-ui,sans-serif;background:var(--bg);color:var(--text)}

    /* ── Top bar ── */
    .topbar{
      position:fixed;top:0;left:0;right:0;z-index:100;
      display:flex;align-items:center;gap:12px;
      padding:10px 20px;
      background:rgba(12,18,35,.85);backdrop-filter:blur(16px);
      border-bottom:1px solid var(--border);
    }
    .topbar-logo{width:28px;height:28px;border-radius:7px}
    .topbar-name{font-size:14px;font-weight:700;color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .topbar-badge{font-size:11px;color:var(--muted);background:rgba(124,58,237,.12);border:1px solid rgba(124,58,237,.25);border-radius:100px;padding:3px 12px;font-weight:600}
    .topbar-buttons{display:flex;gap:10px}
    .topbar-btn{
      display:flex;align-items:center;gap:6px;
      border:none;border-radius:8px;
      padding:7px 16px;font-size:12px;font-weight:700;cursor:pointer;
      transition:all .25s ease;text-decoration:none;
    }
    .topbar-btn-open{
      background:var(--primary);color:#fff;
    }
    .topbar-btn-open:hover{background:#6d28d9}
    .topbar-btn-download{
      background:rgba(124,58,237,.12);border:1px solid rgba(124,58,237,.35);
      color:#c4b5fd;
      position:relative;overflow:hidden;
      box-shadow:0 0 14px rgba(124,58,237,.1);
    }
    .topbar-btn-download::before{
      content:'';position:absolute;top:0;left:-100%;width:100%;height:100%;
      background:linear-gradient(90deg,transparent,rgba(167,139,250,.2),transparent);
      animation:btn-shimmer 2.5s ease-in-out infinite;
    }
    @keyframes btn-shimmer{0%{left:-100%}50%,100%{left:100%}}
    .topbar-btn-download svg{animation:bounce-dl 1.5s ease-in-out infinite}
    @keyframes bounce-dl{0%,100%{transform:translateY(0)}50%{transform:translateY(3px)}}
    .topbar-btn-download:hover{
      background:rgba(124,58,237,.2);border-color:rgba(167,139,250,.5);color:#e0d4ff;
      box-shadow:0 0 20px rgba(124,58,237,.2);
    }

    /* ── Board area ── */
    .board-area{position:absolute;top:48px;left:0;right:0;bottom:0;overflow:hidden;cursor:grab;background:var(--board-bg,#ffffff);touch-action:none}
    .board-area.grabbing{cursor:grabbing}
    #graph-container{position:absolute;top:0;left:0;width:100%;height:100%;overflow:hidden}
    #strokes-canvas{position:absolute;top:0;left:0;pointer-events:none}

    /* ── Loading ── */
    .loading{
      position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;z-index:50;
      background:var(--bg);transition:opacity .3s;
    }
    .loading.hide{opacity:0;pointer-events:none}
    .loading .spinner{width:32px;height:32px;border:3px solid var(--border);border-top-color:var(--primary);border-radius:50%;animation:spin .7s linear infinite}
    @keyframes spin{to{transform:rotate(360deg)}}
    .loading p{font-size:13px;color:var(--muted)}

    /* ── Error ── */
    .error-msg{
      position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:0;z-index:50;
      animation:fade-in .5s ease both;
    }
    @keyframes fade-in{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
    .error-icon{
      margin-bottom:20px;animation:error-bounce 2s ease-in-out infinite;
    }
    @keyframes error-bounce{0%,100%{transform:translateY(0)}50%{transform:translateY(-10px)}}
    .error-icon svg{color:var(--accent);opacity:.8}
    .error-msg h2{font-size:20px;font-weight:800;color:var(--text);margin-bottom:8px}
    .error-msg p{font-size:13px;color:var(--muted);margin-bottom:24px}
    .error-home{
      display:inline-flex;align-items:center;gap:6px;
      background:var(--primary);color:#fff;border:none;border-radius:8px;
      padding:9px 20px;font-size:13px;font-weight:700;cursor:pointer;
      text-decoration:none;transition:background .2s;
    }
    .error-home svg{animation:bounce-dl 1.5s ease-in-out infinite}
    }
    .error-home:hover{background:#6d28d9}

    /* ── Zoom controls ── */
    .zoom-controls{
      position:fixed;bottom:16px;right:16px;z-index:100;
      display:flex;gap:6px;align-items:center;
      background:rgba(12,18,35,.85);backdrop-filter:blur(12px);
      border:1px solid var(--border);border-radius:10px;padding:4px;
    }
    .zoom-btn{
      width:32px;height:32px;border:none;border-radius:7px;
      background:transparent;color:var(--text);font-size:16px;font-weight:700;
      cursor:pointer;display:flex;align-items:center;justify-content:center;
      transition:background .15s;
    }
    .zoom-btn:hover{background:rgba(124,58,237,.2)}
    .zoom-label{font-size:11px;color:var(--muted);min-width:42px;text-align:center;font-weight:600}

    /* ── Mobile responsive ── */
    @media(max-width:600px){
      .topbar{padding:8px 12px;gap:8px}
      .topbar-logo{width:24px;height:24px;border-radius:6px}
      .topbar-name{font-size:12px}
      .topbar-btn{padding:6px 10px;font-size:11px;gap:4px;border-radius:6px}
      .topbar-btn svg{width:12px;height:12px}
      .board-area{top:42px}
      .zoom-controls{bottom:10px;right:10px;padding:3px}
      .zoom-btn{width:28px;height:28px;font-size:14px}
      .zoom-label{font-size:10px;min-width:36px}
      .error-msg h2{font-size:17px}
      .error-msg p{font-size:12px}
      .error-icon svg{width:36px;height:36px}
      .error-home{padding:8px 16px;font-size:12px}
    }
    @media(max-width:400px){
      .topbar-btn span{display:none}
      .topbar-btn{padding:7px 8px}
      .topbar-btn svg{width:16px;height:16px}
    }
  </style>
</head>
<body>
  <div class="topbar">
    <img class="topbar-logo" src="/icon.webp" alt="Munjez"/>
    <span class="topbar-name" id="board-name">${boardName.replace(/</g,'&lt;')}</span>
    <button class="topbar-btn topbar-btn-open" id="app-btn" onclick="openInApp()">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
      <span>Open in App</span>
    </button>
    <a class="topbar-btn topbar-btn-download" href="https://munjez-website.vercel.app/" target="_blank" rel="noopener">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
      <span>Download Munjez</span>
    </a>
  </div>

  <div class="loading" id="loading">
    <div class="spinner"></div>
    <p>Loading board…</p>
  </div>

  <div class="board-area" id="board-area" style="--board-bg:${bgColor}">
    <div id="graph-container"></div>
    <canvas id="strokes-canvas"></canvas>
  </div>

  <div class="zoom-controls">
    <button class="zoom-btn" onclick="zoomOut()">−</button>
    <span class="zoom-label" id="zoom-label">100%</span>
    <button class="zoom-btn" onclick="zoomIn()">+</button>
    <button class="zoom-btn" onclick="zoomReset()" title="Reset">⟲</button>
  </div>

  <script type="module">
    const BOARD_ID = ${JSON.stringify(boardId)};
    const BG_COLOR = ${JSON.stringify(bgColor)};

    // ── Open in app (defined early so it works even if fetch fails) ──
    window.openInApp = () => {
      window.location.href = 'munjez://import/' + BOARD_ID;
    };

    // ── State ──
    let zoom = 1, panX = 0, panY = 0;
    let boardData = null;
    let isDragging = false, dragStartX = 0, dragStartY = 0, panStartX = 0, panStartY = 0;

    const boardArea = document.getElementById('board-area');
    const graphContainer = document.getElementById('graph-container');
    const strokesCanvas = document.getElementById('strokes-canvas');
    const loadingEl = document.getElementById('loading');
    const zoomLabel = document.getElementById('zoom-label');

    // ── Fetch board data ──
    try {
      const resp = await fetch('/api/boards/' + BOARD_ID);
      if (!resp.ok) throw new Error('Board not found');
      boardData = await resp.json();
    } catch (e) {
      loadingEl.innerHTML = '<div class="error-msg"><div class="error-icon"><svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 3v12"/><circle cx="12" cy="20" r="1" fill="currentColor" stroke="none"/></svg></div><h2>Board not found</h2><p>This board may have been removed or the link is invalid.</p><a class="error-home" href="https://munjez-website.vercel.app/" target="_blank" rel="noopener"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>Get Munjez</a></div>';
      throw e;
    }

    // ── Render graph shapes via pre-captured SVG ──
    graphContainer.style.background = BG_COLOR;
    if (boardData.graphSvg) {
      graphContainer.innerHTML = boardData.graphSvg;
      const svg = graphContainer.querySelector('svg');
      if (svg) {
        svg.removeAttribute('viewBox');
        svg.style.width = svg.getAttribute('width') ? svg.getAttribute('width') + 'px' : 'auto';
        svg.style.height = svg.getAttribute('height') ? svg.getAttribute('height') + 'px' : 'auto';
        svg.style.position = 'absolute';
        svg.style.top = '0';
        svg.style.left = '0';
        svg.style.overflow = 'visible';
        svg.style.maxWidth = 'none';
        svg.style.maxHeight = 'none';
      }
    }

    // ── Render strokes ──
    function resizeCanvas() {
      strokesCanvas.width = boardArea.clientWidth;
      strokesCanvas.height = boardArea.clientHeight;
    }
    resizeCanvas();
    window.addEventListener('resize', () => { resizeCanvas(); drawStrokes(); });

    function drawStrokes() {
      const ctx = strokesCanvas.getContext('2d');
      if (!ctx || !boardData?.strokes) return;
      ctx.clearRect(0, 0, strokesCanvas.width, strokesCanvas.height);
      ctx.save();
      ctx.translate(panX, panY);
      ctx.scale(zoom, zoom);

      for (const s of boardData.strokes) {
        if (s.tool !== 'pen' && s.tool !== 'eraser' && s.tool !== 'line') continue;

        if (s.tool === 'line' && s.points && s.points.length >= 2) {
          const p1 = s.points[0], p2 = s.points[s.points.length - 1];
          const dx = p2.x - p1.x, dy = p2.y - p1.y;
          const angle = Math.atan2(dy, dx);
          const hw = 12, ha = Math.PI / 6;
          ctx.save();
          ctx.strokeStyle = s.color; ctx.fillStyle = s.color;
          ctx.lineWidth = s.width; ctx.lineCap = 'round';
          ctx.beginPath(); ctx.moveTo(p1.x, p1.y); ctx.lineTo(p2.x, p2.y); ctx.stroke();
          ctx.beginPath();
          ctx.moveTo(p2.x, p2.y);
          ctx.lineTo(p2.x - hw * Math.cos(angle - ha), p2.y - hw * Math.sin(angle - ha));
          ctx.lineTo(p2.x - hw * Math.cos(angle + ha), p2.y - hw * Math.sin(angle + ha));
          ctx.closePath(); ctx.fill();
          ctx.restore();
          continue;
        }
        if (s.tool === 'line') continue;

        ctx.save();
        ctx.globalAlpha = s.opacity || 1;
        ctx.strokeStyle = s.color; ctx.fillStyle = s.color;
        ctx.lineWidth = s.width; ctx.lineCap = 'round'; ctx.lineJoin = 'round';

        if (s.tool === 'eraser') { ctx.globalCompositeOperation = 'destination-out'; ctx.globalAlpha = 1; }

        const pt = s.penType;
        if (pt === 'highlighter') {
          ctx.globalCompositeOperation = s.tool === 'eraser' ? 'destination-out' : 'multiply';
          ctx.globalAlpha = (s.opacity || 1) * 0.35;
        }

        if (!s.points || s.points.length === 0) { ctx.restore(); continue; }
        if (s.points.length < 2) {
          ctx.beginPath(); ctx.arc(s.points[0].x, s.points[0].y, s.width / 2, 0, Math.PI * 2); ctx.fill();
        } else {
          ctx.beginPath(); ctx.moveTo(s.points[0].x, s.points[0].y);
          for (let i = 1; i < s.points.length - 1; i++) {
            ctx.quadraticCurveTo(s.points[i].x, s.points[i].y,
              (s.points[i].x + s.points[i+1].x) / 2, (s.points[i].y + s.points[i+1].y) / 2);
          }
          ctx.lineTo(s.points[s.points.length - 1].x, s.points[s.points.length - 1].y);
          ctx.stroke();
        }
        ctx.restore();
      }
      ctx.restore();
    }
    drawStrokes();

    // ── Apply transform ──
    function applyTransform() {
      graphContainer.style.transform = 'translate(' + panX + 'px,' + panY + 'px) scale(' + zoom + ')';
      graphContainer.style.transformOrigin = '0 0';
      drawStrokes();
      zoomLabel.textContent = Math.round(zoom * 100) + '%';
    }

    // ── Pan (drag) ──
    boardArea.addEventListener('pointerdown', (e) => {
      isDragging = true; dragStartX = e.clientX; dragStartY = e.clientY;
      panStartX = panX; panStartY = panY;
      boardArea.classList.add('grabbing');
      boardArea.setPointerCapture(e.pointerId);
    });
    boardArea.addEventListener('pointermove', (e) => {
      if (!isDragging) return;
      panX = panStartX + (e.clientX - dragStartX);
      panY = panStartY + (e.clientY - dragStartY);
      applyTransform();
    });
    boardArea.addEventListener('pointerup', () => { isDragging = false; boardArea.classList.remove('grabbing'); });

    // ── Pinch-to-zoom (touch) ──
    let lastTouchDist = 0;
    let lastTouchMid = { x: 0, y: 0 };
    boardArea.addEventListener('touchstart', (e) => {
      if (e.touches.length === 2) {
        e.preventDefault();
        const dx = e.touches[1].clientX - e.touches[0].clientX;
        const dy = e.touches[1].clientY - e.touches[0].clientY;
        lastTouchDist = Math.hypot(dx, dy);
        lastTouchMid = { x: (e.touches[0].clientX + e.touches[1].clientX) / 2, y: (e.touches[0].clientY + e.touches[1].clientY) / 2 };
      }
    }, { passive: false });
    boardArea.addEventListener('touchmove', (e) => {
      if (e.touches.length === 2) {
        e.preventDefault();
        const dx = e.touches[1].clientX - e.touches[0].clientX;
        const dy = e.touches[1].clientY - e.touches[0].clientY;
        const dist = Math.hypot(dx, dy);
        if (lastTouchDist > 0) {
          const rect = boardArea.getBoundingClientRect();
          const mid = { x: (e.touches[0].clientX + e.touches[1].clientX) / 2, y: (e.touches[0].clientY + e.touches[1].clientY) / 2 };
          const mx = mid.x - rect.left, my = mid.y - rect.top;
          const factor = dist / lastTouchDist;
          const newZoom = Math.max(0.1, Math.min(5, zoom * factor));
          panX = mx - (mx - panX) * (newZoom / zoom) + (mid.x - lastTouchMid.x);
          panY = my - (my - panY) * (newZoom / zoom) + (mid.y - lastTouchMid.y);
          zoom = newZoom;
          lastTouchMid = mid;
          applyTransform();
        }
        lastTouchDist = dist;
      }
    }, { passive: false });
    boardArea.addEventListener('touchend', () => { lastTouchDist = 0; });

    // ── Zoom (wheel) ──
    boardArea.addEventListener('wheel', (e) => {
      e.preventDefault();
      const rect = boardArea.getBoundingClientRect();
      const mx = e.clientX - rect.left, my = e.clientY - rect.top;
      const factor = e.deltaY < 0 ? 1.1 : 0.9;
      const newZoom = Math.max(0.1, Math.min(5, zoom * factor));
      panX = mx - (mx - panX) * (newZoom / zoom);
      panY = my - (my - panY) * (newZoom / zoom);
      zoom = newZoom;
      applyTransform();
    }, { passive: false });

    // ── Zoom buttons ──
    window.zoomIn = () => { zoom = Math.min(5, zoom * 1.2); applyTransform(); };
    window.zoomOut = () => { zoom = Math.max(0.1, zoom / 1.2); applyTransform(); };
    window.zoomReset = () => { zoom = 1; panX = 0; panY = 0; applyTransform(); };

    // ── Done loading ──
    applyTransform();
    loadingEl.classList.add('hide');
  </script>
</body>
</html>`;
}

// ── HTTP server ───────────────────────────────────────────────────────────────
const httpServer = http.createServer(async (req, res) => {
  if (req.url === '/favicon.ico') {
    const iconPath = path.join(__dirname, 'icon.webp');
    fs.readFile(iconPath, (err, data) => {
      if (err) { res.writeHead(404); res.end(); return; }
      res.writeHead(200, { 'Content-Type': 'image/webp', 'Cache-Control': 'public,max-age=86400' });
      res.end(data);
    });

  } else if (req.url === '/icon.webp') {
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
    @keyframes bounce-dl{0%,100%{transform:translateX(0)}50%{transform:translateX(4px)}}

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
  // ── POST /api/boards — publish a board ────────────────────────────────────
  } else if (req.method === 'POST' && req.url === '/api/boards') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    try {
      const body = await parseBody(req);
      if (!body || (!body.graphXml && (!body.strokes || body.strokes.length === 0))) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Empty board — nothing to share' }));
        return;
      }
      const id = generateShareId();
      sharedBoards.set(id, {
        board: body.board || {},
        graphXml: body.graphXml || '',
        graphSvg: body.graphSvg || '',
        strokes: body.strokes || [],
        createdAt: Date.now(),
      });
      console.log(`[share] board published: ${id} (${body.board?.name || 'untitled'})`);
      const baseUrl = `https://${req.headers.host || 'munjez-collab-production.up.railway.app'}`;
      res.writeHead(201, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ id, url: `${baseUrl}/view/${id}` }));
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message || 'Invalid request' }));
    }

  // ── OPTIONS /api/boards — CORS preflight ─────────────────────────────────
  } else if (req.method === 'OPTIONS' && req.url === '/api/boards') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.writeHead(204);
    res.end();

  // ── GET /api/boards/:id — retrieve board data ───────────────────────────
  } else if (req.method === 'GET' && req.url.startsWith('/api/boards/')) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    const id = req.url.slice('/api/boards/'.length).split('?')[0].trim();
    const data = sharedBoards.get(id);
    if (!data) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Board not found' }));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));

  // ── GET /view/:id — web viewer ───────────────────────────────────────────
  } else if (req.url.startsWith('/view/')) {
    const id = req.url.slice('/view/'.length).split('?')[0].trim();
    if (!id) { res.writeHead(400); res.end('Missing board ID'); return; }
    const data = sharedBoards.get(id);
    const boardName = data?.board?.name || 'Shared Board';
    const bgColor = data?.board?.bgColor || '#ffffff';
    const html = buildViewerHtml(id, boardName, bgColor);
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
    // منع الـ kicked/banned users من الـ Yjs connection كمان
    const meta = getRoomMeta(room);
    if (meta.bannedIds.has(clientId)) {
      ws.close(1008, 'banned');
      return;
    }
    if (meta.kickedIds.has(clientId)) {
      ws.close(1008, 'kicked');
      return;
    }
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

  // ── أول واحد يدخل الروم = owner — بس مش لو اتـ kick قبل كده ────────────────
  if (!meta.ownerId && !meta.kickedIds.has(clientId)) meta.ownerId = clientId;

  // لو فضل الروم من غير owner (مثلاً الـ kicked user رجع وهو لوحده)
  // وفيه حد تاني — خليه owner
  if (!meta.ownerId) {
    for (const [id] of clients) {
      if (!meta.kickedIds.has(id)) {
        meta.ownerId = id;
        break;
      }
    }
  }

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
  broadcastPresence(room);

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
          laserPoints: msg.laserPoints,
        });
        break;
      }

      // ── Kick: طرد مؤقت — يقدر يرجع ──────────────────────────────────────
      case 'kick': {
        if (clientId !== meta.ownerId) break; // بس الـ owner
        const targetId = msg.targetId;
        const target   = clients.get(targetId);
        if (!target) break;
        // سجّل الـ kick عشان نمنع الـ reconnect فوراً
        meta.kickedIds.add(targetId);
        // شيل الـ user من الروم فوراً وابعت presence محدث للكل
        clients.delete(targetId);
        broadcastPresence(room);
        // بلّغ الـ owner إن الـ kick اتنفذ
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'kick_confirmed', targetId }));
        }
        // بلّغ الـ target — delay بسيط عشان الرسالة توصله قبل الـ close
        if (target.ws.readyState === WebSocket.OPEN) {
          target.ws.send(JSON.stringify({ type: 'kicked', reason: 'You have been removed from this room.' }));
          setTimeout(() => {
            if (target.ws.readyState === WebSocket.OPEN) target.ws.close(1008, 'kicked');
          }, 300);
        }
        // اتأكد إن الـ kickedIds بتتمسح بعد 60 ثانية — الـ kick مؤقت
        setTimeout(() => {
          meta.kickedIds.delete(targetId);
        }, 60_000);
        console.log(`[kick] owner ${clientId} kicked ${targetId} from room ${room}`);
        break;
      }

      // ── Ban: حظر دائم من الروم دي ────────────────────────────────────────
      case 'ban': {
        if (clientId !== meta.ownerId) break; // بس الـ owner
        const targetId = msg.targetId;
        meta.bannedIds.add(targetId);
        // احفظ الـ ban في الـ persistent store فوراً
        if (!roomBans.has(room)) roomBans.set(room, new Set());
        roomBans.get(room).add(targetId);
        const target = clients.get(targetId);
        if (target) {
          // شيل الـ user من الروم فوراً وابعت presence محدث للكل
          clients.delete(targetId);
          broadcastPresence(room);
          // بلّغ الـ owner إن الـ ban اتنفذ
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ban_confirmed', targetId }));
          }
          // بلّغ الـ target — delay بسيط عشان الرسالة توصله قبل الـ close
          if (target.ws.readyState === WebSocket.OPEN) {
            target.ws.send(JSON.stringify({ type: 'banned', reason: 'You have been banned from this room.' }));
            setTimeout(() => {
              if (target.ws.readyState === WebSocket.OPEN) target.ws.close(1008, 'banned');
            }, 300);
          }
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
      // لو الـ owner خرج — أول حد في الروم مش اتـ kick يبقى owner الجديد
      if (meta.ownerId === clientId) {
        let next = null;
        for (const [id] of clients) {
          if (!meta.kickedIds.has(id)) { next = id; break; }
        }
        meta.ownerId = next || null;
      }
      if (clients.size === 0) {
        // احفظ الـ bans قبل ما تمسح الـ meta — عشان لو حد حاول يدخل تاني
        const currentBans = meta.bannedIds;
        if (currentBans.size > 0) {
          roomBans.set(room, currentBans);
        } else {
          roomBans.delete(room);
        }
        rooms.delete(room);
        roomMeta.delete(room);
      } else {
        // Notify remaining peers to remove this user's cursor immediately
        const leaveMsg = JSON.stringify({ type: 'leave', id: clientId });
        for (const [, { ws: peerWs }] of clients) {
          if (peerWs.readyState === WebSocket.OPEN) peerWs.send(leaveMsg);
        }
        broadcastPresence(room);
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
