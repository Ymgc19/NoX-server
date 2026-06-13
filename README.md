# NoX Online Battle Server (PoC)

ローカル WebSocket リレーサーバー．NoX のオンライン対戦 PoC 用．

## セットアップ

```bash
cd server
npm install
npm start
```

デフォルトで `ws://localhost:4567` で待ち受け．
ポート変更は環境変数で:
```bash
PORT=8080 npm start
```

## 動作確認

1. サーバーを起動
2. 別のブラウザウィンドウ(または異なるブラウザ)で `Nox.html` を 2 枚開く
3. 一方で「vs オンライン」→「ルーム作成」→ 6 桁コードが表示される
4. もう一方で「vs オンライン」→「ルーム参加」→ コード入力
5. 両者揃うとマッチ開始 → 同じ盤面で対戦

## メッセージプロトコル

すべて JSON で `{type, ...}`．

### Client → Server

| type | payload | 用途 |
|---|---|---|
| `create_room` | - | 新規ルーム作成 |
| `join_room` | `{code}` | コードで参加 |
| `leave_room` | - | 離脱 |
| `submit_deck` | `{deck: [cardId...]}` | デッキ提出(25枚) |
| `action` | `{action: {...}}` | プレイヤー操作中継 |
| `snapshot` | `{state: {...}}` | 状態スナップショット中継 |
| `chat` | `{text}` | チャット |
| `ping` | `{t}` | レイテンシ測定 |

### Server → Client

| type | payload |
|---|---|
| `room_created` | `{code, role:"host"}` |
| `room_joined` | `{code, role:"guest"}` |
| `peer_joined` | `{code}` |
| `peer_left` | `{code}` |
| `deck_submitted` | `{idx}` |
| `match_start` | `{deckP1, deckP2, firstPlayer, yourPlayer, startedAt}` |
| `action` | `{action, from:"host"\|"guest"}` |
| `snapshot` | `{state, from}` |
| `chat` | `{from, text}` |
| `pong` | `{t}` |
| `error` | `{message}` |

## アーキテクチャ方針

現状: **サーバーは純粋リレー**．ルール検証は各クライアントが自走．

将来: サーバーがゲームロジックを保持して `action` を検証 → `snapshot` を返す **サーバー権威**モデルに移行．プロトコルは互換のまま，サーバー実装を差し替えるだけで済むように設計．

## 制限事項 (PoC)

- 切断時はルーム破棄 (再接続なし)
- 永続化なし (再起動で全ルーム消滅)
- マッチング無し (コード共有のみ)
- アニメは各クライアントローカル，軽微なズレあり
