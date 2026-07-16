// =========================================================
//  NoX Online Battle - WebSocket Relay Server (PoC)
//  -------------------------------------------------------
//  ・ルーム作成/参加 (6文字コード)
//  ・ピア間メッセージ中継 (action, snapshot, deck提出)
//  ・サーバー権威の入口を意識: future-work として action 検証ロジック
//    を server 側に持たせる前提で API を整理．今は relay．
//  -------------------------------------------------------
//  Run:
//      cd server && npm install && npm start
//  Env:
//      PORT (default 4567)
// =========================================================
const { WebSocketServer } = require("ws");
const fs = require("fs");
const path = require("path");

const PORT = parseInt(process.env.PORT || "4567", 10);
const ROOM_CODE_LEN = 6;
const HEARTBEAT_MS = 25_000;

const rooms = new Map(); // code -> { host, guest, decks:{0,1}, names:{0,1}, startedAt, rank }

// ==================== ランクマッチ: 勝ち点・戦績の永続化 ====================
// users: { userId: { name, points, wins, losses } }
// 初期勝ち点 10．
// 勝ち点はポット方式: 対戦開始時点の各自の勝ち点×RANK_ALPHA を両者が供出して
// 合算し (ポット)，勝者が総取りする．勝者は自分の供出分が戻るため実質増減は
//   勝者: +(敗者の供出分) / 敗者: -(敗者の供出分)
// となりゼロサム．例: 10pt vs 10pt → 5+5=10 のポットを勝者が取り，勝者15pt/敗者5pt．
// 勝ち点は小数を許容する (丸めは表示側で行い，小数第3位以下を丸める)．
// 引き分け・不成立時は供出分がそのまま返る (増減なし)．
// データ保存先: 環境変数 DATA_DIR を設定するとそこに保存する．
// Render では Persistent Disk をマウントしたパス (例: /var/data) を指定すれば
// デプロイ・再起動後もデータが消えない．未設定時は従来通りサーバディレクトリ (揮発)．
const DATA_DIR = process.env.DATA_DIR || __dirname;
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
const RANK_FILE = path.join(DATA_DIR, "rank_stats.json");
const RANK_START_POINTS = 10;
const RANK_ALPHA = 0.5; // 移動額の計算に使う勝ち点の割合 (0-1)
// 対戦開始時点の勝ち点から各プレイヤーの拠出分を計算する (小数のまま保持し丸めない)
function stakeOf(points) {
  return RANK_ALPHA * Math.max(0, points);
}
let rankDb = { users: {} };
try {
  if (fs.existsSync(RANK_FILE)) rankDb = JSON.parse(fs.readFileSync(RANK_FILE, "utf8"));
  if (!rankDb.users) rankDb.users = {};
} catch (e) { console.error("rank_stats.json load error:", e); rankDb = { users: {} }; }

function saveRankDb() {
  try { fs.writeFileSync(RANK_FILE, JSON.stringify(rankDb, null, 2)); }
  catch (e) { console.error("rank_stats.json save error:", e); }
}

// ==================== 試合ログ (運営のデータ分析用) ====================
// ランクマッチ1試合ごとに1行のJSONを追記する (JSONL形式)．
// /admin/export でまとめてエクスポートできる．
const MATCH_LOG_FILE = path.join(DATA_DIR, "match_log.jsonl");
function appendMatchLog(entry) {
  try { fs.appendFileSync(MATCH_LOG_FILE, JSON.stringify(entry) + "\n"); }
  catch (e) { console.error("match_log append error:", e); }
}
function readMatchLog() {
  try {
    if (!fs.existsSync(MATCH_LOG_FILE)) return [];
    return fs.readFileSync(MATCH_LOG_FILE, "utf8")
      .split("\n").filter(Boolean)
      .map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean);
  } catch (e) { return []; }
}
function getRankUser(userId, name) {
  if (!userId) return null;
  if (!rankDb.users[userId]) {
    rankDb.users[userId] = { name: name || "", points: RANK_START_POINTS, wins: 0, losses: 0 };
  }
  if (name) rankDb.users[userId].name = name;
  return rankDb.users[userId];
}
function winRateOf(u) {
  const games = (u.wins || 0) + (u.losses || 0);
  return games > 0 ? u.wins / games : 0;
}
function publicStats(u) {
  return { points: u.points, wins: u.wins, losses: u.losses, winRate: winRateOf(u) };
}

// ==================== アカウント (Twitter風: @ユーザー名 + NoX ID) ====================
// accounts: { "NoXxxxxxxxxx": { username, createdAt, updatedAt, data } }
//   username: "@"なしの英数字 (照合は大文字小文字を無視)
//   data:     クライアントの userData 全体 (ログイン時の引き継ぎ用スナップショット)
const ACCOUNTS_FILE = path.join(DATA_DIR, "accounts.json");
let accountsDb = { accounts: {} };
try {
  if (fs.existsSync(ACCOUNTS_FILE)) accountsDb = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8"));
  if (!accountsDb.accounts) accountsDb.accounts = {};
} catch (e) { console.error("accounts.json load error:", e); accountsDb = { accounts: {} }; }
function saveAccounts() {
  try { fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accountsDb)); }
  catch (e) { console.error("accounts.json save error:", e); }
}
// NoX + 9桁のユニーク英数字 (紛らわしい 0/O/1/I/L を除外)
function genNoxId() {
  const cs = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
  let id;
  do {
    id = "NoX" + Array.from({ length: 9 }, () => cs[Math.floor(Math.random() * cs.length)]).join("");
  } while (accountsDb.accounts[id]);
  return id;
}
function normUsername(u) {
  return String(u || "").trim().replace(/^@+/, "");
}
function accountAuth(noxId, username) {
  const acc = accountsDb.accounts[String(noxId || "")];
  if (!acc) return null;
  if (acc.username.toLowerCase() !== normUsername(username).toLowerCase()) return null;
  return acc;
}

// マッチングキュー: { ws, userId, name, deck }
const rankQueue = [];
function removeFromQueue(ws) {
  const i = rankQueue.findIndex(q => q.ws === ws);
  if (i >= 0) rankQueue.splice(i, 1);
}
function tryRankMatch() {
  while (rankQueue.length >= 2) {
    const a = rankQueue.shift();
    const bIdx = rankQueue.findIndex(q => q.ws !== a.ws && q.userId !== a.userId);
    if (bIdx < 0) { rankQueue.unshift(a); return; } // 相手候補なし (同一ユーザーのみ)
    const b = rankQueue.splice(bIdx, 1)[0];
    if (!a.ws || a.ws.readyState !== a.ws.OPEN) { rankQueue.unshift(b); continue; }
    if (!b.ws || b.ws.readyState !== b.ws.OPEN) { rankQueue.unshift(a); continue; }

    // 内部ルームを作って即マッチ開始 (デッキはキュー参加時に提出済み)
    const code = randomRoomCode();
    const uA = getRankUser(a.userId, a.name);
    const uB = getRankUser(b.userId, b.name);
    const room = {
      host: a.ws, guest: b.ws,
      decks: { 0: a.deck, 1: b.deck },
      names: { 0: a.name || "プレイヤー1", 1: b.name || "プレイヤー2" },
      startedAt: Date.now(),
      rank: {
        userIds: [a.userId, b.userId],
        statsAtStart: [publicStats(uA), publicStats(uB)],
        stakes: [stakeOf(uA.points), stakeOf(uB.points)], // 開始時点の供出額 (ポット方式)
        resultDone: false,
      },
    };
    rooms.set(code, room);
    a.ws.roomCode = code; a.ws.role = "host";
    b.ws.roomCode = code; b.ws.role = "guest";

    const firstPlayer = Math.random() < 0.5 ? 0 : 1;
    const shuffled = [shuffle(room.decks[0]), shuffle(room.decks[1])];
    const base = {
      deckP1: shuffled[0], deckP2: shuffled[1],
      firstPlayer, startedAt: room.startedAt,
      nameP1: room.names[0], nameP2: room.names[1],
      rank: true,
    };
    send(a.ws, "match_start", { ...base, yourPlayer: 0, you: publicStats(uA), opp: publicStats(uB) });
    send(b.ws, "match_start", { ...base, yourPlayer: 1, you: publicStats(uB), opp: publicStats(uA) });
    console.log(`[rank] ${code} match: ${room.names[0]}(${(winRateOf(uA) * 100).toFixed(0)}%) vs ${room.names[1]}(${(winRateOf(uB) * 100).toFixed(0)}%)`);
  }
}

// 勝敗確定 → 勝ち点を計算して両者へ通知
function resolveRankResult(room, winnerIdx) {
  if (!room.rank || room.rank.resultDone) return;
  room.rank.resultDone = true;
  const [uidA, uidB] = room.rank.userIds;
  const uA = getRankUser(uidA), uB = getRankUser(uidB);
  let deltas = [0, 0];
  if (winnerIdx === 0 || winnerIdx === 1) {
    const winner = winnerIdx === 0 ? uA : uB;
    const loser  = winnerIdx === 0 ? uB : uA;
    // ポット方式: 開始時に両者が勝ち点×RANK_ALPHA を供出し (合算=ポット)，勝者が総取り．
    // 勝者は自分の供出分が返ってくるため，実質増減は敗者の供出分のみ (小数のまま計算)．
    const loserStake = room.rank.stakes[winnerIdx === 0 ? 1 : 0];
    winner.wins += 1;
    loser.losses += 1;
    winner.points = winner.points + loserStake;
    loser.points = Math.max(0, loser.points - loserStake);
    deltas = winnerIdx === 0 ? [loserStake, -loserStake] : [-loserStake, loserStake];
  }
  saveRankDb();
  // 運営の分析用に試合ログを追記
  appendMatchLog({
    ts: Date.now(),
    startedAt: room.startedAt,
    mode: "rank",
    winnerIdx, // 0 | 1 | -1 (draw)
    users: [uidA, uidB].map((uid, i) => ({
      userId: uid,
      name: (rankDb.users[uid] && rankDb.users[uid].name) || "",
      deck: room.decks[i] || null, // 使用デッキ (カードID配列)
      statsAtStart: room.rank.statsAtStart[i],
      delta: deltas[i],
      pointsAfter: (i === 0 ? uA : uB).points,
    })),
  });
  const payload = (idx, u) => ({
    delta: deltas[idx],
    points: u.points, wins: u.wins, losses: u.losses, winRate: winRateOf(u),
    winnerIdx,
    oppWinRateAtStart: room.rank.statsAtStart[1 - idx].winRate,
  });
  send(room.host,  "rank_update", payload(0, uA));
  send(room.guest, "rank_update", payload(1, uB));
  console.log(`[rank] result winner=P${winnerIdx + 1} deltas=${deltas.join("/")}`);
}

// ==================== 日刊 逢魔時の禍々マガジン (ログインボーナス誌面) ====================
//   star:    注目度アップ中のプレイヤー = 期間内の勝ち点純増が最大の人 (タイブレークは勝利数)
//            (クライアント指定の期間で試合ログから集計)
//   popular: 人気カードランキング = 全プレイヤーのデッキで最も広く採用されているカード Top5
//            (アカウント同期データの全デッキ編成を横断し，採用プレイヤー数ベース．
//             タイブレークは総採用枚数．n = 採用プレイヤー数として返す)
// データが無い場合は null / 空配列．
function buildMagazineData(msg) {
  const num = (v, d) => (Number.isFinite(v) ? v : d);
  const starFrom = num(msg.starFrom, 0), starTo = num(msg.starTo, Date.now());
  const log = readMatchLog().filter(e => e && e.mode === "rank" && Array.isArray(e.users));
  // 注目株: 勝敗に関わらず全参加者の勝ち点増減を合算し，純増が最大の人を選ぶ
  const byUser = {};
  for (const e of log) {
    if (!(e.ts >= starFrom && e.ts < starTo)) continue;
    for (let i = 0; i < e.users.length; i++) {
      const u = e.users[i];
      if (!u) continue;
      const k = u.userId || u.name || "?";
      if (!byUser[k]) byUser[k] = { name: "", wins: 0, gained: 0 };
      byUser[k].gained += num(u.delta, 0);
      if (e.winnerIdx === i) byUser[k].wins += 1;
      if (u.name) byUser[k].name = u.name;
    }
  }
  const star = Object.values(byUser)
    .filter(u => u.name && u.gained > 0)
    .sort((a, b) => b.gained - a.gained || b.wins - a.wins)[0] || null;
  // 人気カードランキング: 全プレイヤーのデッキ編成を横断して採用状況を集計
  const byCard = {}; // cardId -> { users: 採用プレイヤー数, copies: 総採用枚数 }
  for (const acc of Object.values(accountsDb.accounts)) {
    const d = acc && acc.data;
    if (!d) continue;
    // このプレイヤーの全デッキ (現行デッキ + 保存スロット) の採用枚数を数える
    const copies = {};
    const addDeck = (arr) => {
      if (!Array.isArray(arr)) return;
      for (const id of arr) {
        if (typeof id === "string") copies[id] = (copies[id] || 0) + 1;
      }
    };
    addDeck(d.currentDeck);
    if (Array.isArray(d.decks)) for (const slot of d.decks) addDeck(slot && slot.cards);
    for (const [id, n] of Object.entries(copies)) {
      if (!byCard[id]) byCard[id] = { users: 0, copies: 0 };
      byCard[id].users += 1;
      byCard[id].copies += n;
    }
  }
  const popular = Object.entries(byCard)
    .map(([id, v]) => ({ id, n: v.users, copies: v.copies }))
    .sort((a, b) => b.n - a.n || b.copies - a.copies)
    .slice(0, 5);
  return { star: star ? { name: star.name, wins: star.wins, gained: star.gained } : null, popular };
}

// 全ユーザー分布 (ホームの STATS ブロック用) — 勝ち点ベースのヒストグラム
function buildRankStats(userId) {
  const all = Object.values(rankDb.users)
    .map(u => ({ games: u.wins + u.losses, points: u.points, winRate: winRateOf(u) }))
    .filter(u => u.games >= 1);
  // 勝ち点の分布: 0 〜 max を 10 ビンに分割 (binSize はキリの良い値に切り上げ)
  const maxP = Math.max(20, ...all.map(u => u.points));
  const binSize = Math.max(2, Math.ceil((maxP + 1) / 10 / 2) * 2);
  const bins = new Array(10).fill(0);
  for (const u of all) bins[Math.min(9, Math.floor(u.points / binSize))]++;
  const me = rankDb.users[userId];
  let topPercent = null;
  if (me && all.length > 0) {
    const above = all.filter(u => u.points > me.points).length;
    topPercent = Math.max(1, Math.round(((above + 1) / all.length) * 100));
  }
  return {
    histType: "points",
    resetEpoch: rankDb.resetEpoch || 0, // 運営リセットの世代 (クライアントはこれを見て手元の戦績を初期化する)
    histogram: bins,
    binSize,
    totalUsers: all.length,
    you: me ? { ...publicStats(me), topPercent } : null,
  };
}

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

// ==================== 運営者用 HTTP API ====================
// 環境変数 ADMIN_KEY を設定すると有効化される (未設定なら全拒否 = 安全側)．
//   エクスポート: GET /admin/export?key=ADMIN_KEY
//     → { exportedAt, users: {userId: {name, points, wins, losses, winRate}}, matches: [...] }
//   リセット:    GET /admin/reset?key=ADMIN_KEY&what=stats|log|all
const http = require("http");
const ADMIN_KEY = process.env.ADMIN_KEY || null;

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const sendJson = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(obj, null, 2));
  };

  if (url.pathname === "/admin/export" || url.pathname === "/admin/reset" || url.pathname === "/admin/import") {
    if (!ADMIN_KEY) { sendJson(403, { error: "ADMIN_KEY が未設定のため管理APIは無効です (環境変数 ADMIN_KEY を設定してください)" }); return; }
    if (url.searchParams.get("key") !== ADMIN_KEY) { sendJson(403, { error: "invalid key" }); return; }

    // バックアップ (export したJSON) からの復元: デプロイでデータが消えた後に使う
    //   curl -X POST ".../admin/import?key=KEY" -H "Content-Type: application/json" --data-binary @nox_export.json
    if (url.pathname === "/admin/import") {
      if (req.method !== "POST") { sendJson(405, { error: "POST でJSONを送信してください" }); return; }
      let body = "";
      let size = 0;
      req.on("data", chunk => {
        size += chunk.length;
        if (size > 20 * 1024 * 1024) { req.destroy(); return; } // 20MB上限
        body += chunk;
      });
      req.on("end", () => {
        try {
          const data = JSON.parse(body);
          let userCount = 0, matchCount = 0;
          if (data.users && typeof data.users === "object") {
            const users = {};
            for (const [id, u] of Object.entries(data.users)) {
              users[id] = {
                name: String(u.name || ""),
                points: Number.isFinite(u.points) ? u.points : RANK_START_POINTS,
                wins: Number.isFinite(u.wins) ? u.wins : 0,
                losses: Number.isFinite(u.losses) ? u.losses : 0,
              };
              userCount++;
            }
            rankDb = { users };
            saveRankDb();
          }
          if (Array.isArray(data.matches)) {
            fs.writeFileSync(MATCH_LOG_FILE, data.matches.map(m => JSON.stringify(m)).join("\n") + (data.matches.length ? "\n" : ""));
            matchCount = data.matches.length;
          }
          console.log(`[admin] import: users=${userCount} matches=${matchCount}`);
          sendJson(200, { ok: true, importedUsers: userCount, importedMatches: matchCount });
        } catch (e) {
          sendJson(400, { error: "JSONの解析に失敗: " + (e.message || e) });
        }
      });
      return;
    }

    if (url.pathname === "/admin/export") {
      const users = {};
      for (const [id, u] of Object.entries(rankDb.users)) {
        users[id] = { name: u.name, points: u.points, wins: u.wins, losses: u.losses, winRate: winRateOf(u) };
      }
      const payload = {
        exportedAt: new Date().toISOString(),
        totalUsers: Object.keys(users).length,
        users,
        matches: readMatchLog(),
      };
      // &download=1 を付けるとブラウザがJSONファイルとして保存する (コピペ不要)
      if (url.searchParams.get("download")) {
        const fname = `nox_export_${new Date().toISOString().slice(0, 10)}.json`;
        res.writeHead(200, {
          "Content-Type": "application/json; charset=utf-8",
          "Content-Disposition": `attachment; filename="${fname}"`,
        });
        res.end(JSON.stringify(payload, null, 2));
      } else {
        sendJson(200, payload);
      }
      console.log("[admin] export served");
      return;
    }

    // /admin/reset
    const what = url.searchParams.get("what") || "stats";
    if (what === "stats" || what === "all") {
      // resetEpoch を進めると，各クライアントは次回接続時に手元の勝ち点・戦績・
      // 折れ線グラフ (rank.history) を初期化する．クライアントミラーからの
      // 自動復元もこの世代より古いデータは拒否される．
      rankDb = { users: {}, resetEpoch: Date.now() };
      saveRankDb();
    }
    if (what === "log" || what === "all") {
      try { fs.writeFileSync(MATCH_LOG_FILE, ""); } catch (e) {}
    }
    console.log(`[admin] reset: ${what}`);
    sendJson(200, { ok: true, reset: what });
    return;
  }

  // ヘルスチェック
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("NoX online server");
});

const wss = new WebSocketServer({ server: httpServer });
httpServer.listen(PORT);

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
    removeFromQueue(ws); // ランクマッチキューから除外
    if (ws.roomCode) {
      const room = rooms.get(ws.roomCode);
      if (room) {
        // 対戦中の切断 (アプリを閉じた等) は切断側の強制敗北．
        // ランクマッチなら勝ち点も確定させてから残ったピアに通知する．
        if (room.rank && room.startedAt && !room.rank.resultDone) {
          const winnerIdx = (room.host === ws) ? 1 : 0; // 切断していない側が勝者
          console.log(`[rank] disconnect forfeit: P${winnerIdx === 0 ? 2 : 1} left → P${winnerIdx + 1} wins`);
          resolveRankResult(room, winnerIdx);
        }
        if (room.host === ws) room.host = null;
        if (room.guest === ws) room.guest = null;
        // PoC: ピアが落ちたらルーム破棄 (残ったピアには peer_left が届き，クライアント側で勝利処理)
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
    case "resync_request": // 同期切れ検知時の再送要求 (クライアント側で最新状態を持つ方が再送する)
    case "combat": {
      // ピアへ中継 (PoC: サーバーは検証せず素通し．将来はここでルール検証)
      if (!ws.roomCode) return;
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      // 通常マッチ (ルーム/フレンド戦) の対戦ログ: 終了通知を検知して記録．
      // ランク戦は resolveRankResult 側で (勝ち点付きで) 記録するためここでは除外．
      if (msg.type === "combat" && !room.rank && !room.matchLogged && room.startedAt) {
        let winnerIdx = null;
        if (msg.kind === "game_end" && (msg.winner === 0 || msg.winner === 1 || msg.winner === -1)) {
          winnerIdx = msg.winner;
        } else if (msg.kind === "surrender") {
          winnerIdx = ws.role === "host" ? 1 : 0; // 投了の送信者の相手が勝者
        }
        if (winnerIdx !== null) {
          room.matchLogged = true;
          appendMatchLog({
            ts: Date.now(),
            startedAt: room.startedAt,
            mode: "casual",
            winnerIdx, // 0 | 1 | -1 (draw)
            reason: String(msg.reason || (msg.kind === "surrender" ? "投了" : "")).slice(0, 120),
            users: [0, 1].map(i => ({
              name: room.names[i] || "",
              deck: room.decks[i] || null, // 使用デッキ (カードID配列)
            })),
          });
        }
      }
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

    // ===== ランクマッチ =====
    case "rank_queue": {
      if (ws.roomCode) { send(ws, "error", { message: "既にルームに参加しています" }); return; }
      const userId = String(msg.userId || "").slice(0, 64);
      const deck = Array.isArray(msg.deck) ? msg.deck.slice(0, 30) : null;
      if (!userId) { send(ws, "error", { message: "userIdがありません" }); return; }
      if (!deck || deck.length === 0) { send(ws, "error", { message: "デッキが空です" }); return; }
      removeFromQueue(ws); // 二重キュー防止
      const name = String(msg.username || "").trim().slice(0, 16);
      getRankUser(userId, name);
      saveRankDb();
      rankQueue.push({ ws, userId, name, deck });
      send(ws, "rank_queued", { queueSize: rankQueue.length });
      console.log(`[rank] queued: ${name || userId} (queue=${rankQueue.length})`);
      tryRankMatch();
      return;
    }

    case "rank_cancel": {
      removeFromQueue(ws);
      send(ws, "rank_cancelled", {});
      return;
    }

    case "rank_result": {
      // 勝敗報告 (両クライアントから届くが最初の1件のみ処理)
      if (!ws.roomCode) return;
      const room = rooms.get(ws.roomCode);
      if (!room || !room.rank) return;
      const w = msg.winner; // 0 | 1 | -1 (draw)
      if (w !== 0 && w !== 1 && w !== -1) return;
      resolveRankResult(room, w);
      return;
    }

    case "rank_stats_request": {
      const userId = String(msg.userId || "").slice(0, 64);
      send(ws, "rank_stats", buildRankStats(userId));
      return;
    }

    // ===== ログインボーナス誌面 (前日の勝ち頭 + 人気カード番付) =====
    case "magazine_request": {
      send(ws, "magazine_data", buildMagazineData(msg));
      return;
    }

    // ===== アカウント登録: @ユーザー名 → NoX ID を発行 =====
    case "account_register": {
      const username = normUsername(msg.username);
      if (!/^[A-Za-z0-9]{1,15}$/.test(username)) {
        send(ws, "account_error", { op: "register", message: "ユーザー名は英数字1〜15文字で入力してください" });
        return;
      }
      // ユーザー名はユーザー間でユニーク (大文字小文字を区別しない)
      const taken = Object.values(accountsDb.accounts)
        .some(a => a.username.toLowerCase() === username.toLowerCase());
      if (taken) {
        send(ws, "account_error", { op: "register", code: "name_taken", message: `@${username} は既に使用されています．別のユーザー名を入力してください` });
        return;
      }
      const noxId = genNoxId();
      accountsDb.accounts[noxId] = { username, createdAt: Date.now(), updatedAt: null, data: null };
      saveAccounts();
      send(ws, "account_registered", { noxId, username });
      console.log(`[account] register: @${username} → ${noxId}`);
      return;
    }

    // ===== ログイン: ユーザー名 + NoX ID の組で照合し，保存済みデータを返す =====
    case "account_login": {
      const acc = accountAuth(msg.noxId, msg.username);
      if (!acc) {
        send(ws, "account_error", { op: "login", message: "ユーザー名とIDの組み合わせが見つかりません" });
        return;
      }
      send(ws, "account_login_ok", {
        noxId: String(msg.noxId),
        username: acc.username,
        data: acc.data || null,
        updatedAt: acc.updatedAt,
      });
      console.log(`[account] login: @${acc.username} (${msg.noxId})`);
      return;
    }

    // ===== データ同期: クライアントの userData をアカウントに保存 (引き継ぎ用) =====
    case "account_sync": {
      const acc = accountAuth(msg.noxId, msg.username);
      if (!acc) return; // 認証不一致は黙殺
      try {
        const raw = JSON.stringify(msg.data || null);
        if (raw.length > 500_000) { send(ws, "account_error", { op: "sync", message: "データが大きすぎます" }); return; }
        acc.data = msg.data || null;
        acc.updatedAt = Date.now();
        saveAccounts();
        // ==== 勝ち点の自動復元 ====
        // サーバリセット (再デプロイ等) で rank_stats.json が消えても，各クライアントは
        // 自分の勝ち点・戦績のミラーを userData.rank に保持している．サーバ側に記録が
        // 無いユーザーが同期してきたら，そのミラーから再登録する．これによりプレイヤーが
        // ログインするたびに分布が自然に復元されていく．
        // (既にサーバ記録があるユーザーには適用しない = サーバが常に正)
        try {
          const d = msg.data;
          const uid = d && (d.userId || d.noxId);
          // 運営リセットより古い世代のミラーからは復元しない (リセットの巻き戻し防止)
          const epochOk = (Number(d && d.rankResetEpoch) || 0) >= (rankDb.resetEpoch || 0);
          if (uid && d && d.rank && !rankDb.users[uid] && epochOk) {
            const clamp = (v, min, max, dflt) => (Number.isFinite(v) ? Math.max(min, Math.min(max, v)) : dflt);
            const wins = Math.round(clamp(d.rank.wins, 0, 1000000, 0));
            const losses = Math.round(clamp(d.rank.losses, 0, 1000000, 0));
            if (wins + losses > 0 || Number.isFinite(d.rank.points)) {
              rankDb.users[uid] = {
                name: String(d.displayName || d.username || "").replace(/^@+/, "").slice(0, 30),
                points: clamp(d.rank.points, 0, 1000000, RANK_START_POINTS),
                wins, losses,
              };
              saveRankDb();
              console.log(`[rank] クライアント同期から勝ち点を復元: ${uid} (${rankDb.users[uid].points}pt / ${wins}勝${losses}敗)`);
            }
          }
        } catch (e) { /* 復元失敗は無視 (通常の同期は成立済み) */ }
        send(ws, "account_synced", { updatedAt: acc.updatedAt });
      } catch (e) { /* ignore */ }
      return;
    }

    // ===== フレンド: 指定ユーザー群の戦績 + 推移 (試合ログから再構成) =====
    case "friends_stats_request": {
      const ids = Array.isArray(msg.userIds) ? msg.userIds.slice(0, 50).map(x => String(x).slice(0, 64)) : [];
      const log = readMatchLog();
      const friends = ids.map(id => {
        const u = rankDb.users[id];
        if (!u) return { userId: id, found: false };
        // 試合ログからこのユーザーの推移 [{ts, pts, wr}] を再構成 (直近30戦)
        const trend = [];
        for (const m of log) {
          const me = (m.users || []).find(x => x.userId === id);
          if (!me || !me.statsAtStart) continue;
          const before = me.statsAtStart;
          const games = (before.wins || 0) + (before.losses || 0);
          const won = m.winnerIdx !== -1 && (me.delta || 0) > 0;
          const lost = m.winnerIdx !== -1 && (me.delta || 0) < 0;
          const winsAfter = (before.wins || 0) + (won ? 1 : 0);
          const gamesAfter = games + (won || lost ? 1 : 0);
          trend.push({
            ts: m.ts,
            pts: me.pointsAfter,
            wr: gamesAfter > 0 ? winsAfter / gamesAfter : 0,
          });
        }
        return {
          userId: id, found: true,
          name: u.name || "", points: u.points,
          wins: u.wins, losses: u.losses, winRate: winRateOf(u),
          trend: trend.slice(-30),
        };
      });
      send(ws, "friends_stats", { friends });
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
