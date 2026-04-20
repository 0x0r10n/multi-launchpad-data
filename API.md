# multi-launchpad-data — Integration Reference

## Overview

Two data delivery channels run in parallel:

| Channel | URL | Best For |
|---------|-----|----------|
| WebSocket (Socket.io) | `ws://<host>/` | Real-time launches, price ticks, live trade feed |
| REST API | `http://<host>/api/` | Snapshots, history, wallet analysis, pagination |

The WebSocket uses Socket.io rooms (not raw WebSocket frames). Use the Socket.io client library.

---

## WebSocket

### Connecting

```js
import { io } from "socket.io-client";

const socket = io("http://localhost:3000");

socket.on("connect",    () => console.log("connected:", socket.id));
socket.on("disconnect", () => console.log("disconnected"));
```

### Subscribing to rooms

There are two subscription patterns depending on the room type:

**Pattern A — `subscribe` message** (broadcast rooms, no snapshot on join):
```js
socket.emit("subscribe", "new");
socket.emit("subscribe", "graduating");
socket.emit("subscribe", "graduated");
socket.emit("subscribe", "trending");
```

**Pattern B — `join` / `leave`** (per-token rooms, snapshot sent on join):
```js
socket.emit("join",  `token:${mint}`);
socket.emit("join",  `chart:${mint}`);
socket.emit("join",  `transactions:${mint}`);

socket.emit("leave", `token:${mint}`);   // always leave when navigating away
```

---

### Room Reference

| Room | Pattern | Description |
|------|---------|-------------|
| `new` | subscribe | Every new token launch across all platforms |
| `graduating` | subscribe | Tokens that just crossed 80% bonding curve |
| `graduated` | subscribe | Tokens whose bonding curve completed |
| `trending` | subscribe | Top 50 tokens by volume (1m/5m/30m/1h), refreshed every 30 s |
| `token:{mint}` | join/leave | All state updates for a single token (price, risk, metadata) |
| `chart:{mint}` | join/leave | Price ticks only for a single token |
| `transactions:{mint}` | join/leave | Live swap feed — every buy/sell with exact SOL + token amounts |

---

### Standard token payload (WS message envelope)

All `subscribe`-pattern rooms emit a `message` event with this shape:

```json
{
  "type": "message",
  "room": "new",
  "data": {
    "token": {
      "mint": "...",
      "name": "Frog Coin",
      "symbol": "FROG",
      "image": "https://...",
      "description": "...",
      "socials": { "twitter": "", "telegram": "", "website": "" },
      "platform": "pump",
      "creator": "CreatorWallet...",
      "createdAt": 1713600000000
    },
    "pools": [
      {
        "pairAddress": "...",
        "platform": "pump",
        "type": "bonding-curve",
        "priceNative": 0.0000000012,
        "priceUsd": 0.000042,
        "liquidity": { "sol": 24.3, "usd": 3645 },
        "marketCapUsd": 42000,
        "curvePercentage": 34.2,
        "complete": false,
        "reserves": { "virtualSol": 30.0, "virtualToken": 714285714.2, "realSol": 24.3 }
      }
    ],
    "events": {
      "1m":  { "priceChangePercentage": 0 },
      "5m":  { "priceChangePercentage": 2.4 },
      "30m": { "priceChangePercentage": 12.3 },
      "1h":  { "priceChangePercentage": -5.0 }
    },
    "risk": {
      "snipers":  { "count": 3, "percentage": 4.2 },
      "insiders": { "count": 1, "percentage": 1.1 },
      "top10":    18.5,
      "dev":      { "holdings": 0, "percentage": 0 }
    },
    "graduation": { "status": "new" },
    "priceHistory": [
      { "time": 1713600000000, "price": 0.0000000012, "price_usd": 0.000042 }
    ],
    "meta": {
      "dataQuality": "complete",
      "hasPrice": true,
      "hasRisk": true,
      "hasImage": true,
      "richPayloadDelay": 420
    }
  }
}
```

`meta.dataQuality`:
- `"skeleton"` — just launched, name/symbol only, no price yet
- `"partial"` — has price, risk data still arriving
- `"complete"` — full data including top10 holders and risk scan

---

### `new` room — New token launches

```js
socket.emit("subscribe", "new");

socket.on("message", (msg) => {
  if (msg.room !== "new") return;
  const { token, pools, meta } = msg.data;
  console.log(`[${meta.dataQuality}] ${token.symbol} on ${token.platform}`);
  if (pools[0]) console.log(`  price: $${pools[0].priceUsd}`);
});
```

Fires for every new token detected across pump.fun, Moonshot, Bags, LetsBonk, LaunchLab.

---

### `graduating` / `graduated` rooms

```js
socket.emit("subscribe", "graduating");
socket.emit("subscribe", "graduated");

socket.on("message", (msg) => {
  if (msg.room === "graduating") {
    const { token, pools } = msg.data;
    console.log(`${token.symbol} is ${pools[0].curvePercentage.toFixed(1)}% full`);
  }
  if (msg.room === "graduated") {
    console.log(`${msg.data.token.symbol} graduated!`);
  }
});
```

---

### `trending` room

Refreshed every 30 seconds. Each update is a full replacement of the top-50 list.

```js
socket.emit("subscribe", "trending");

socket.on("message", (msg) => {
  if (msg.room !== "trending") return;
  const tokens = msg.data; // array of top-50 token payloads
  renderTrendingList(tokens);
});
```

---

### `token:{mint}` room — Per-token updates

Snapshot sent on join. Live `update` events arrive as token state changes (price, risk, curve).

```js
socket.emit("join", `token:${mint}`);

// Snapshot on join
socket.on("snapshot", (envelope) => {
  if (envelope.room === `token:${mint}`) {
    renderTokenPage(envelope.data);
  }
});

// Live updates
socket.on("update", (envelope) => {
  if (envelope.room === `token:${mint}`) {
    updateTokenPage(envelope.data);
  }
});

socket.on("trade", (event) => {
  // Fast-path trade event on token:{mint} — arrives before full swap parse.
  // Contains: { mint, type, sol, maker, slot }
  // Use to update buy/sell counters immediately; wait for "tx" for exact token amounts.
  if (event.mint === mint) incrementTradeCounter(event.type);
});

// Cleanup
function closeTokenPage() {
  socket.emit("leave", `token:${mint}`);
}
```

---

### `chart:{mint}` room — Price ticks only

Lower bandwidth than `token:{mint}`. Use this for chart updates on a token page.

```js
socket.emit("join", `chart:${mint}`);

socket.on("tick", (tick) => {
  // { mint, time, price, price_usd }
  if (tick.mint === mint) appendChartPoint(tick);
});
```

---

### `transactions:{mint}` room — Live Swap Feed

The highest-resolution data available. Every buy/sell emits a `tx` event with exact on-chain amounts derived from pre/post token balance deltas.

**Join / leave:**
```js
socket.emit("join",  `transactions:${mint}`);
socket.emit("leave", `transactions:${mint}`);
```

**On join — snapshot of the last 100 swaps:**
```json
{
  "type": "snapshot",
  "room": "transactions:Abc123...",
  "data": {
    "mint": "Abc123...",
    "count": 47,
    "history": [
      {
        "signature":      "TxSig1234...",
        "timestamp":      1713600000000,
        "slot":           312045678,
        "type":           "buy",
        "sol":            0.042000000,
        "tokens":         5000000.0,
        "maker":          "WalletAddr...",
        "priorityFeeSOL": 0.000025000
      }
    ]
  }
}
```

**Live `tx` event:**
```json
{
  "signature":      "FullTxSignature...",
  "timestamp":      1713600000000,
  "slot":           312045678,
  "type":           "buy",
  "sol":            0.042000000,
  "tokens":         5000000.0,
  "decimals":       6,
  "maker":          "WalletAddr...",
  "priorityFeeSOL": 0.000025000,
  "mint":           "Abc123..."
}
```

**Field reference:**

| Field | Type | Description |
|-------|------|-------------|
| `signature` | string | Full Solana transaction signature |
| `timestamp` | number | Unix ms at server receipt |
| `slot` | number | Solana slot — use as stable ordering key |
| `type` | `"buy"` \| `"sell"` | Swap direction |
| `sol` | number | Exact SOL sent (buy) or received (sell), 9 decimal places |
| `tokens` | number | Exact token amount, respects token decimals |
| `decimals` | number | Token decimal places (snapshot only includes in `tx` event) |
| `maker` | string | Wallet that signed the transaction |
| `priorityFeeSOL` | number | Compute budget priority fee in SOL |

**Full trade modal example:**

```js
let currentMint = null;

function openTradeModal(mint) {
  if (currentMint) socket.emit("leave", `transactions:${currentMint}`);
  currentMint = mint;
  socket.emit("join", `transactions:${mint}`);
}

function closeTradeModal() {
  if (currentMint) socket.emit("leave", `transactions:${currentMint}`);
  currentMint = null;
}

// Pre-populate the table with the last 100 trades
socket.on("snapshot", (envelope) => {
  if (envelope.room !== `transactions:${currentMint}`) return;
  const { history } = envelope.data;
  renderTradeTable(history);     // history is newest-first
});

// Prepend each new swap as it arrives
socket.on("tx", (event) => {
  if (event.mint !== currentMint) return;
  prependTradeRow({
    side:      event.type,
    sol:       event.sol,
    tokens:    event.tokens,
    wallet:    event.maker,
    sig:       event.signature,
    time:      event.timestamp,
    fee:       event.priorityFeeSOL,
  });
});
```

> **Latency note:** The `tx` event arrives ~1 ms after a faster `trade` event on `token:{mint}`. Use `trade` for instant buy/sell counters; use `tx` for the full amounts table.

Also available via REST: `GET /api/token/:mint/trades` — see below.

---

## REST API

Base URL: `http://<host>/api`

All responses include `"success": true` on success or `"success": false, "error": "..."` on failure.

---

### Token API

#### `GET /api/tokens`

Paginated token list.

| Query param | Type | Default | Notes |
|-------------|------|---------|-------|
| `platform` | string | — | `pump`, `moon`, `bags`, `letsbonk`, `launchlab` |
| `status` | string | — | `new`, `graduating`, `graduated` |
| `sort` | string | `created` | `created`, `volume`, `mcap` |
| `minMarketCap` | number | 0 | USD floor |
| `minVolume` | number | 0 | All-time SOL floor |
| `page` | number | 1 | 1-based |
| `limit` | number | 50 | Max 100 |

```
GET /api/tokens?platform=pump&sort=volume&limit=20
```

```json
{
  "success": true,
  "tokens": [
    {
      "mint": "...",
      "name": "Frog Coin",
      "symbol": "FROG",
      "platform": "pump",
      "priceUsd": 0.000042,
      "marketCapUsd": 42000,
      "volumeUsd": 8100,
      "curvePercentage": 34.2,
      "createdAt": 1713600000000
    }
  ],
  "total": 48,
  "page": 1,
  "limit": 20,
  "hasMore": true
}
```

---

#### `GET /api/token/:mint`

Full token payload — identical shape to WebSocket messages.

```
GET /api/token/So11111111111111111111111111111111111111112
```

Response shape: same as the `data` field in the WS message envelope above.

---

#### `GET /api/token/:mint/metadata`

Lightweight identity only (no price data). Fast single Redis lookup.

```json
{
  "success": true,
  "mint": "...",
  "name": "Frog Coin",
  "symbol": "FROG",
  "image": "https://...",
  "description": "...",
  "socials": { "twitter": "", "telegram": "", "website": "" },
  "platform": "pump",
  "creator": "...",
  "createdAt": 1713600000000,
  "isMayhemMode": false,
  "isCashbackEnabled": false
}
```

---

#### `GET /api/token/:mint/holders`

Top token holders. **Requires `?refresh=true` on first call** to fetch from chain.
Subsequent calls within 60 s return cached data.

```
GET /api/token/:mint/holders?refresh=true
```

```json
{
  "success": true,
  "mint": "...",
  "holders": [
    {
      "rank": 1,
      "address": "...",
      "uiAmount": 120000000,
      "percentage": 12.0,
      "isBondingCurve": true,
      "isCreator": false
    }
  ],
  "totalAccounts": 8,
  "fetchedAt": 1713600000000
}
```

If not cached and `?refresh=true` is not passed, returns HTTP 202:
```json
{ "success": false, "error": "Holder data not yet cached. Retry with ?refresh=true to fetch from chain." }
```

---

#### `GET /api/token/:mint/pairs`

Bonding curve pool data.

```json
{
  "success": true,
  "mint": "...",
  "pairs": [{
    "pairAddress": "...",
    "platform": "pump",
    "type": "bonding-curve",
    "priceNative": 0.0000000012,
    "priceUsd": 0.000042,
    "liquidity": { "sol": 24.3, "usd": 3645 },
    "marketCapUsd": 42000,
    "curvePercentage": 34.2,
    "complete": false,
    "reserves": { "virtualSol": 30.0, "virtualToken": 714285714.2, "realSol": 24.3 }
  }]
}
```

---

#### `GET /api/token/:mint/trades`

Rich swap history with exact SOL + token amounts. Populated ~1 ms after each on-chain swap. Returns newest trades first.

| Query param | Type | Default | Max | Notes |
|-------------|------|---------|-----|-------|
| `limit` | number | 50 | 500 | Entries per page |
| `before` | string | — | — | Signature from previous page's `nextCursor` |

**First page:**
```
GET /api/token/:mint/trades?limit=50
```

**Next page (cursor pagination):**
```
GET /api/token/:mint/trades?limit=50&before=<nextCursor>
```

```json
{
  "success": true,
  "data": [
    {
      "signature":      "TxSig1234...",
      "timestamp":      1713600000000,
      "slot":           312045678,
      "type":           "buy",
      "sol":            0.042000000,
      "tokens":         5000000.0,
      "maker":          "WalletAddr...",
      "priorityFeeSOL": 0.000025000
    }
  ],
  "nextCursor": "TxSigLastEntry..."
}
```

When `nextCursor` is `null`, you have reached the oldest available trade (500 entry cap).

**Full pagination example:**
```js
async function loadAllTrades(mint) {
  const trades = [];
  let cursor = null;

  do {
    const url = cursor
      ? `/api/token/${mint}/trades?limit=50&before=${cursor}`
      : `/api/token/${mint}/trades?limit=50`;
    const { data, nextCursor } = await fetch(url).then(r => r.json());
    trades.push(...data);
    cursor = nextCursor;
  } while (cursor);

  return trades;
}
```

---

#### `GET /api/token/:mint/history`

Raw price ticks.

| Query param | Type | Default | Max |
|-------------|------|---------|-----|
| `limit` | number | 300 | 1000 |

```json
{
  "success": true,
  "mint": "...",
  "count": 147,
  "history": [
    { "time": 1713600000000, "price": 0.0000000012, "price_usd": 0.000042 }
  ]
}
```

---

### Price API

#### `GET /api/price/:mint`

Current price + all timeframe % changes + volume.

```json
{
  "success": true,
  "mint": "...",
  "price": { "native": 0.0000000012, "usd": 0.000042 },
  "marketCapUsd": 42000,
  "liquidity": { "native": 24.3, "usd": 3645 },
  "priceChange": {
    "1m": 2.4, "5m": -1.1, "15m": 8.7,
    "30m": 12.3, "1h": -5.0, "4h": 0, "24h": 0
  },
  "volume": { "24h": 8100, "all": 8100 },
  "updatedAt": 1713600000000
}
```

---

#### `GET /api/price/:mint/history`

OHLCV candlesticks.

| Query param | Type | Default | Options |
|-------------|------|---------|---------|
| `interval` | string | `5m` | `1m`, `5m`, `15m`, `30m`, `1h`, `4h` |
| `limit` | number | 100 | Max 500 |

```json
{
  "success": true,
  "mint": "...",
  "interval": "5m",
  "candles": [
    {
      "time": 1713600000000,
      "open": 0.000040,
      "high": 0.000045,
      "low":  0.000038,
      "close": 0.000042,
      "volume": 1.42
    }
  ]
}
```

---

#### `POST /api/price/multiple`

Batch price lookup. Up to 50 mints per request.

```json
{ "mints": ["mint1...", "mint2...", "mint3..."] }
```

```json
{
  "success": true,
  "prices": {
    "mint1...": {
      "name": "Frog Coin",
      "symbol": "FROG",
      "priceUsd": 0.000042,
      "priceNative": 0.0000000012,
      "marketCapUsd": 42000,
      "change24h": 0,
      "liquidityUsd": 3645
    }
  },
  "count": 1,
  "requested": 3
}
```

---

### Wallet API

Rate limit: 60 requests/minute per IP.

#### `GET /api/wallet/:wallet/net-worth`

SPL token holdings cross-referenced with indexed prices.
**Requires `?refresh=true` on first call.** Cached 30 s.

```
GET /api/wallet/WalletAddress.../net-worth?refresh=true
```

```json
{
  "success": true,
  "wallet": "...",
  "solBalance": 4.82,
  "totalUsd": 1240.50,
  "holdings": [
    {
      "mint": "...",
      "address": "...",
      "name": "Frog Coin",
      "symbol": "FROG",
      "amount": 5000000,
      "priceUsd": 0.000042,
      "valueUsd": 210.0
    }
  ],
  "unindexed": 3,
  "fetchedAt": 1713600000000
}
```

`unindexed` = number of token accounts whose mint is not in this system's index.

---

#### `GET /api/wallet/:wallet/transactions`

Tokens launched by this wallet (CREATE events), newest first.

| Query param | Type | Default |
|-------------|------|---------|
| `limit` | number | 50 (max 100) |
| `cursor` | string | — (createdAt ms, for pagination) |

```json
{
  "success": true,
  "wallet": "...",
  "stats": { "total_launched": 12, "total_migrated": 3 },
  "transactions": [
    {
      "type": "CREATE",
      "mint": "...",
      "name": "Frog Coin",
      "symbol": "FROG",
      "platform": "pump",
      "createdAt": 1713600000000,
      "currentPriceUsd": 0.000042,
      "marketCapUsd": 42000,
      "status": "new"
    }
  ],
  "hasMore": false,
  "cursor": null
}
```

---

#### `GET /api/wallet/:wallet/history`

All launched tokens with current performance metrics.

```json
{
  "success": true,
  "wallet": "...",
  "stats": { "total_launched": 12, "total_migrated": 3 },
  "tokens": [
    {
      "mint": "...",
      "name": "Frog Coin",
      "symbol": "FROG",
      "platform": "pump",
      "createdAt": 1713600000000,
      "priceUsd": 0.000042,
      "marketCapUsd": 42000,
      "graduationStatus": "new",
      "curvePercentage": 34.2,
      "change1h": -2.1,
      "change24h": 0
    }
  ],
  "fetchedAt": 1713600000000
}
```

---

### System

#### `GET /api/stats`

```json
{
  "success": true,
  "status": "running",
  "tokensInRedis": 8432,
  "uptime": "3621s",
  "timestamp": "2026-04-20T14:22:01.000Z"
}
```

---

## Complete Integration Example

A token page with all data sources wired together:

```js
import { io } from "socket.io-client";

const BASE = "http://localhost:3000";
const socket = io(BASE);

let activeMint = null;

// ── Open a token page ────────────────────────────────────────────────────────
async function openTokenPage(mint) {
  // Clean up previous page subscriptions
  if (activeMint) {
    socket.emit("leave", `token:${activeMint}`);
    socket.emit("leave", `chart:${activeMint}`);
    socket.emit("leave", `transactions:${activeMint}`);
  }
  activeMint = mint;

  // 1. REST snapshot for instant render (no flash of empty state)
  const payload = await fetch(`${BASE}/api/token/${mint}`).then(r => r.json());
  renderTokenPage(payload);

  // 2. Load trade history (first page)
  await loadTrades(mint);

  // 3. Subscribe to live updates
  socket.emit("join", `token:${mint}`);
  socket.emit("join", `chart:${mint}`);
  socket.emit("join", `transactions:${mint}`);
}

// ── Trade history with infinite scroll ──────────────────────────────────────
let tradeCursor = null;
let tradeLoadingMore = false;

async function loadTrades(mint, cursor = null) {
  const url = cursor
    ? `${BASE}/api/token/${mint}/trades?limit=50&before=${cursor}`
    : `${BASE}/api/token/${mint}/trades?limit=50`;
  const res  = await fetch(url).then(r => r.json());
  if (!res.success) return;
  appendTrades(res.data);
  tradeCursor = res.nextCursor;
}

async function loadMoreTrades() {
  if (!tradeCursor || tradeLoadingMore) return;
  tradeLoadingMore = true;
  await loadTrades(activeMint, tradeCursor);
  tradeLoadingMore = false;
}

// ── Live WebSocket events ────────────────────────────────────────────────────

// Snapshot on join: pre-populates the trade table (last 100 trades, newest first)
socket.on("snapshot", (envelope) => {
  if (envelope.room === `transactions:${activeMint}`) {
    renderTradeTable(envelope.data.history);
  }
});

// Live trade (arrives ~1ms after raw "trade" event, includes exact token amounts)
socket.on("tx", (event) => {
  if (event.mint !== activeMint) return;
  prependTradeRow({
    sig:       event.signature,
    time:      event.timestamp,
    side:      event.type,        // "buy" | "sell"
    sol:       event.sol,
    tokens:    event.tokens,
    wallet:    event.maker,
    fee:       event.priorityFeeSOL,
  });
});

// Token state update (price, curve, risk)
socket.on("update", (envelope) => {
  if (envelope.room === `token:${activeMint}`) {
    updatePriceDisplay(envelope.data);
  }
});

// Chart price tick
socket.on("tick", (tick) => {
  if (tick.mint === activeMint) appendChartCandle(tick);
});

// Fast buy/sell counter update (before tx parse is complete)
socket.on("trade", (event) => {
  if (event.mint === activeMint) updateBuySellCounter(event.type);
});

// ── Subscribe to new launches (e.g. a live feed page) ───────────────────────
socket.emit("subscribe", "new");

socket.on("message", (msg) => {
  if (msg.room === "new") {
    const { token, pools, meta } = msg.data;
    if (meta.dataQuality === "complete") {
      addToLaunchFeed({ token, price: pools[0]?.priceUsd });
    }
  }
  if (msg.room === "trending") {
    renderTrendingList(msg.data);
  }
  if (msg.room === "graduating") {
    markTokenGraduating(msg.data.token.mint);
  }
});

socket.emit("subscribe", "trending");
socket.emit("subscribe", "graduating");
```

---

## Error Responses

All errors return `success: false` with an HTTP status code:

| Status | Meaning |
|--------|---------|
| 400 | Bad request (invalid address, missing body) |
| 202 | Data not yet cached — retry with `?refresh=true` |
| 404 | Token/wallet not found in index |
| 429 | Rate limit exceeded |
| 500 | Internal server error |

```json
{ "success": false, "error": "Token not found" }
```

---

## Rate Limits

| Endpoint group | Limit |
|----------------|-------|
| Token, Price, System | 300 req/min per IP |
| Wallet endpoints | 60 req/min per IP |

Exceeding limits returns HTTP 429:
```json
{ "error": "Rate limit exceeded", "retryAfterMs": 60000 }
```
