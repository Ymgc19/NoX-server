// =========================================================
//  NoX Online Battle - WebSocket Relay Server (PoC)
//  -------------------------------------------------------
//  ・ルーム作成/参加 (6文字コード)
//  ・ピア間メッセージ中継 (action, snapshot, deck提出)
//  ・サーバー権威の入口を意識: future-work として action 検証ロジック
//    を server 側に持たせる前提で API を整理。今は relay。
//  -------------------------------------------------------
//  Run:
//      cd server && npm install && npm start
//  Env:
//      PORT (default 4567)
// =========================================================
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "4567", 10);
const ROOM_CODE_LEN = 6;
const HEARTBEAT_MS = 25_000;

const rooms = new Map(); // code -> { host, guest, decks:{0,1}, names:{0,1}, startedAt }

function randomRoomCode() {
  // 紛らわしい 0/O/1/I/L を除いた 30 文字から 6 桁
  const chars = "23456789ABCDEFGHJKMNPQRSTUVWXYZ";
  let code;
  do {
    code = "";
    for (let i = 0; i < ROOM_CODE_LEN; i++) {
      code += chars[Math.floor(Math.random() * chars.length)];
    }
  } while (rooms.has(code));
  return code;
}

function send(ws, type, payload) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  try {
    ws.send(JSON.stringify({ type, ...(payload || {}) }));
  } catch (e) {
    console.error("send error:", e);
  }
}

function broadcastToRoom(room, type, payload, exceptWs) {
  for (const role of ["host", "guest"]) {
    const peer = room[role];
    if (peer && peer !== exceptWs) send(peer, type, payload);
  }
}

function shuffle(arr) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function cleanupRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  // 残ったピアに peer_left を送る
  broadcastToRoom(room, "peer_left", { code });
  rooms.delete(code);
  console.log(`[room] ${code} closed`);
}

const wss = new WebSocketServer({ port: PORT });

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.roomCode = null;
  ws.role = null;
  console.log(`[conn] client connected (total=${wss.clients.size})`);

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (e) { send(ws, "error", { message: "invalid JSON" }); return; }
    handleMessage(ws, msg);
  });

  ws.on("close", () => {
    console.log(`[conn] client disconnected`);
    if (ws.roomCode) {
      const room = rooms.get(ws.roomCode);
      if (room) {
        if (room.host === ws) room.host = null;
        if (room.guest === ws) room.guest = null;
        // PoC: ピアが落ちたらルーム破棄
        cleanupRoom(ws.roomCode);
      }
    }
  });
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case "ping":
      send(ws, "pong", { t: msg.t });
      return;

    case "create_room": {
      if (ws.roomCode) { send(ws, "error", { message: "既にルームに参加しています" }); return; }
      const code = randomRoomCode();
      rooms.set(code, { host: ws, guest: null, decks: {}, names: {}, startedAt: null });
      ws.roomCode = code;
      ws.role = "host";
      send(ws, "room_created", { code, role: "host" });
      console.log(`[room] ${code} created by host`);
      return;
    }

    case "join_room": {
      const code = String(msg.code || "").toUpperCase();
      if (!code) { send(ws, "error", { message: "ルームコードが空です" }); return; }
      const room = rooms.get(code);
      if (!room) { send(ws, "error", { message: `ルーム ${code} が見つかりません` }); return; }
      if (room.guest) { send(ws, "error", { message: "そのルームは満員です" }); return; }
      if (room.host === ws) { send(ws, "error", { message: "自分のルームには入れません" }); return; }
      room.guest = ws;
      ws.roomCode = code;
      ws.role = "guest";
      send(ws, "room_joined", { code, role: "guest" });
      send(room.host, "peer_joined", { code });
      console.log(`[room] ${code} guest joined`);
      return;
    }

    case "leave_room": {
      if (!ws.roomCode) return;
      const room = rooms.get(ws.roomCode);
      if (room) cleanupRoom(ws.roomCode);
      ws.roomCode = null;
      ws.role = null;
      return;
    }

    case "submit_deck": {
      // クライアント自身のデッキ (id配列, 25枚) + プレイヤー名を提出
      if (!ws.roomCode) { send(ws, "error", { message: "ルームに参加していません" }); return; }
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      const idx = ws.role === "host" ? 0 : 1;
      const ids = Array.isArray(msg.deck) ? msg.deck.slice() : null;
      if (!ids || ids.length === 0) { send(ws, "error", { message: "デッキが空です" }); return; }
      room.decks[idx] = ids;
      // プレイヤー名 (max 16 文字) を保存
      const submittedName = String(msg.username || "").trim().slice(0, 16);
      room.names[idx] = submittedName || `プレイヤー${idx + 1}`;
      send(ws, "deck_submitted", { idx });

      // 両者揃ったらマッチ開始: シャッフル + 先攻ランダム + 名前 + 双方に送信
      if (room.decks[0] && room.decks[1] && !room.startedAt) {
        room.startedAt = Date.now();
        const firstPlayer = Math.random() < 0.5 ? 0 : 1;
        const shuffled = [shuffle(room.decks[0]), shuffle(room.decks[1])];
        const payload = {
          deckP1: shuffled[0],
          deckP2: shuffled[1],
          firstPlayer,
          startedAt: room.startedAt,
          nameP1: room.names[0],
          nameP2: room.names[1],
        };
        send(room.host,  "match_start", { ...payload, yourPlayer: 0 });
        send(room.guest, "match_start", { ...payload, yourPlayer: 1 });
        console.log(`[room] ${ws.roomCode} match started: ${room.names[0]} vs ${room.names[1]} (first=P${firstPlayer + 1})`);
      }
      return;
    }

    case "action":
    case "snapshot":
    case "combat": {
      // ピアへ中継 (PoC: サーバーは検証せず素通し。将来はここでルール検証)
      if (!ws.roomCode) return;
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      broadcastToRoom(room, msg.type, { ...msg, from: ws.role }, ws);
      return;
    }

    case "chat": {
      if (!ws.roomCode) return;
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      broadcastToRoom(room, "chat", { from: ws.role, text: String(msg.text || "").slice(0, 200) }, ws);
      return;
    }

    default:
      send(ws, "error", { message: `unknown message type: ${msg.type}` });
  }
}

// ハートビート
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, HEARTBEAT_MS);

console.log(`NoX online server listening on ws://localhost:${PORT}`);
console.log(`(set env PORT to change)`);
