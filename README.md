# Munjez Collaboration Server (`munjez-collab-server`)

The official real-time synchronization backend for the **Munjez Collaborative Whiteboard**. Built on Node.js using standard WebSockets (`ws`) and Yjs, this server handles conflict-free collaborative editing (CRDT), user presence, canvas drawing synchronization, and public whiteboard web sharing.

---

## 🚀 Key Features

### 1. Collaborative Whiteboard CRDT Sync
*   Powered by [Yjs](https://github.com/yjs/yjs) and `y-websocket` to sync nodes, graph XML, and drawing canvas strokes.
*   Enables multiple users to draw simultaneously without overlap conflict or latency issues.

### 2. Live Presence & Cursors
*   Shares real-time cursor coordinates (`x`, `y`), current drawing tools, pen colors, and active laser pointer tracking points.
*   Broadcasts joined/left participants, user nicknames, custom avatars, and identification colors.

### 3. Room Moderation (Kick & Ban)
*   **Dynamic Ownership:** The first client to join a room code is designated as the Room Owner. Ownership is safely transferred if the host disconnects.
*   **Kick Action:** Owners can temporarily kick a client (prevents reconnecting for 60 seconds).
*   **Ban Action:** Owners can ban client IDs permanently from a room (until server restart).

### 4. Board Archival & Standalone Viewers
*   **Share API:** Whiteboards can be published to the server, generating a unique shared ID.
*   **Public Viewer (`/view/:id`):** Serves a high-performance web-based reader to render board diagrams, XML shapes, and vector strokes directly in the browser (supports zoom, pan, grid overlay, and particles).

### 5. Application Deep Linking (`/join/:roomCode`)
*   Provides landing pages that trigger device deep links (`munjez://join/<roomCode>`) to automatically launch the native desktop/mobile application.

---

## 📂 Project Structure

*   [server.js](file:///D:/munjez/collab-serve/server.js) — Core HTTP & WebSocket router, collaboration logic, moderation rules, and HTML page builders.
*   [package.json](file:///D:/munjez/collab-serve/package.json) — Node.js scripts and framework dependencies.
*   [railway.toml](file:///D:/munjez/collab-serve/railway.toml) / [render.yaml](file:///D:/munjez/collab-serve/render.yaml) — Cloud deployment blueprints.

---

## 🌐 API & Connection Endpoints

### HTTP REST API

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Server health check (returns room count). |
| `GET` | `/join/:roomCode` | Generates redirect webpage to launch desktop app. |
| `GET` | `/view/:id` | Renders a public read-only viewer for the shared board. |
| `GET` | `/api/boards/:id` | API to retrieve JSON data of a published whiteboard. |
| `POST` | `/api/boards` | Publishes whiteboard data (XML + strokes) and returns a view link. |

### WebSockets

*   **CRDT Synchronization (Yjs):** `ws://<host>/<roomName>`
*   **Presence & Cursor Channel:** `ws://<host>/presence/<roomName>?clientId=<id>&name=<username>&color=<hexColor>`

---

## 🛠️ Installation & Setup

1.  Install dependencies:
    ```bash
    npm install
    ```
2.  Start the development server (uses `node --watch`):
    ```bash
    npm run dev
    ```
3.  Start in production:
    ```bash
    npm start
    ```

*Note: The server defaults to port `1234` unless a `PORT` environment variable is specified.*

---

## 📦 Cloud Deployment

*   **Railway:** Ready for deployment via the included [railway.toml](file:///D:/munjez/collab-serve/railway.toml).
*   **Render:** Deploy instantly using the web service setup from [render.yaml](file:///D:/munjez/collab-serve/render.yaml).

---

## 🔗 Useful Links

*   **Official Project Website:** [Munjez Website](https://munjez-website.vercel.app/)
*   **Developer Portfolio:** [Osama Portfolio](https://osama-portfolio-six.vercel.app/)
