# Data Source Integration Guide – Real-Time Token Indexer

> For frontend and backend developers integrating real-time Solana token data into **caesarx.trade**.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Connection Details](#2-connection-details)
3. [Available WebSocket Rooms](#3-available-websocket-rooms)
4. [Message Format](#4-message-format)
5. [Integration Examples](#5-integration-examples)
6. [Best Practices](#6-best-practices)
7. [Current Limitations](#7-current-limitations)

---

## 1. Overview

This service is a real-time, multi-launchpad Solana token indexer. It listens to raw on-chain transaction events via a **Yellowstone gRPC stream** (Chainstack), parses and enriches token data, stores state in **Redis**, and streams live updates to connected clients via **Socket.io**.

All token payloads follow a **unified schema** — regardless of which launchpad launched the token, consumers receive the same data shape with no per-platform handling required.

**Key advantages:**

- Live data from block production via Yellowstone gRPC at `PROCESSED` commitment
- 100% on-chain metadata resolution via Metaplex PDA — no third-party token APIs
- Snapshot-on-join: every room sends its current state immediately when a client subscribes
- Unified schema across all supported launchpads
- SOL price via Jupiter; no other external price oracles

### Supported Launchpads

| Platform | ID | Program ID |
|---|---|---|
| Pump.fun | `pump` | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| Moon.it | `moon` | `MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG` |
| Bags.fm | `bags` | `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` |
| Meteora DBC | `meteora` | `dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN` |
| LetsBonk | `letsbonk` | `FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1` |
| Raydium LaunchLab | `launchlab` | `LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj` |

> Bags.fm and Meteora DBC share the same on-chain program. The indexer distinguishes them by inspecting the token mint suffix (`BAGS` → `bags`, otherwise `meteora`).

---

## 2. Connection Details

| Transport | URL |
|---|---|
| WebSocket (Socket.io) | `ws://80.190.80.155:3000` |
| REST Base URL | `http://80.190.80.155:3000` |

The WebSocket server uses the **Socket.io v4** protocol. Use the official Socket.io client — raw WebSocket clients are not compatible.

**Install the client:**

```bash
npm install socket.io-client
# or
yarn add socket.io-client
```

**Connect:**

```ts
import { io } from "socket.io-client";

const socket = io("ws://80.190.80.155:3000", {
  transports: ["websocket"],
  reconnection: true,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 10000,
  reconnectionAttempts: Infinity,
});

socket.on("connect", () => {
  console.log("Connected:", socket.id);
});

socket.on("disconnect", (reason) => {
  console.warn("Disconnected:", reason);
});
```

---

## 3. Available WebSocket Rooms

Join a room by emitting `join` with the room name after connecting. Rooms send a `snapshot` event immediately upon joining with their current state.

| Room | Description | Snapshot Contents |
|---|---|---|
| `new` | All new token launches across every supported platform | Last 50 tokens (full payload each) |
| `graduating` | Tokens whose bonding curve is ≥ 80% filled | All currently graduating tokens |
| `graduated` | Tokens whose bonding curve has completed | All graduated tokens |
| `trending` | Periodic volume-based rankings by time window | Top 50 tokens per window (1m / 5m / 30m / 1h) |
| `token:{mint}` | All state updates for a single token | Full payload for that token |
| `chart:{mint}` | Live price ticks for a single token | Last 500 price ticks |

> The `new` room is joined automatically on connection. Explicitly joining it is harmless.

### Events emitted per room

| Event | Fired on rooms | Description |
|---|---|---|
| `snapshot` | All | Sent once on join; contains current room state |
| `message` | `new`, `graduating`, `graduated`, `token:{mint}` | New token or full state update |
| `update` | `new`, `token:{mint}` | Incremental update to an existing token |
| `trade` | `token:{mint}` | Lightweight raw trade event |
| `chart` | `chart:{mint}` | Single new price tick |

---

## 4. Message Format

### 4.1 Envelope

Every event except `trade` is wrapped in an envelope:

```json
{
  "type": "message | update | snapshot | chart | trending",
  "room": "new | graduating | graduated | trending | token:{mint} | chart:{mint}",
  "data": {}
}
```

For `message`, `update`, and `snapshot` events, `data` is a **Full Token Payload** (see below).
For `trending` see [Trending Payload](#44-trending-payload). For `chart` see [Chart Tick Payload](#45-chart-tick-payload).

---

### 4.2 Full Token Payload

```jsonc
{
  // Static token metadata
  "token": {
    "name": "Froggo",
    "symbol": "FROG",
    "mint": "G7B...xyz",             // Base58 mint address
    "uri": "https://...",            // Off-chain metadata URI (IPFS or HTTP)
    "decimals": 6,
    "description": "The frog token",
    "image": "https://cf-ipfs.com/ipfs/Qm...",
    "hasFileMetaData": true,         // false if metadata fetch has not yet resolved
    "createdOn": "https://pump.fun/coin/G7B...xyz",
    "strictSocials": {
      "twitter": "https://x.com/froggosol",
      "telegram": "https://t.me/froggosol",
      "website": "https://froggo.io" // Empty string if not provided
    },
    "creation": {
      "creator": "Ab1...def",
      "created_tx": "3kX...sig",
      "created_time": 1711699200     // Unix seconds
    }
  },

  // Pool / bonding curve state (always one entry)
  "pools": [
    {
      "poolId": "7aB...curve",       // Bonding curve PDA address
      "liquidity": { "quote": 12.5, "usd": 1875.0 },
      "price": { "quote": 0.00000052, "usd": 0.0000728 },
      "tokenSupply": 1000000000000000,
      "lpBurn": 100,
      "tokenAddress": "G7B...xyz",
      "marketCap": { "quote": 520.0, "usd": 78000.0 },
      "decimals": 6,
      "security": {
        "freezeAuthority": null,     // null = renounced
        "mintAuthority": null
      },
      "quoteToken": "So11111111111111111111111111111111111111112",
      "market": "pump",              // Platform ID
      "deployer": "Ab1...def",
      "lastUpdated": 1711699260000,  // Milliseconds
      "createdAt": 1711699200000,    // Milliseconds
      "txns": {
        "buys": 142,
        "sells": 37,
        "total": 179,
        "volume": 45.2,              // All-time SOL volume
        "volume24h": 28.7            // SOL volume in past 24h
      },
      "curvePercentage": 14.7,       // 0–100; how full the bonding curve is
      "creation": {
        "creator": "Ab1...def",
        "created_tx": "3kX...sig",
        "created_time": 1711699200000 // Milliseconds
      }
    }
  ],

  // Price change percentages over rolling windows
  "events": {
    "1m":  { "priceChangePercentage": 4.21 },
    "5m":  { "priceChangePercentage": -1.87 },
    "15m": { "priceChangePercentage": 12.44 },
    "30m": { "priceChangePercentage": 8.91 },
    "1h":  { "priceChangePercentage": 33.10 },
    "4h":  { "priceChangePercentage": -5.02 },
    "24h": { "priceChangePercentage": 102.50 }
  },

  // On-chain holder risk analysis
  "risk": {
    "snipers": {
      "count": 3,
      "totalBalance": 42000000000000,
      "totalPercentage": 4.2,
      "wallets": [
        {
          "wallet": "Sn1...per",
          "percentage": 2.1,
          "amount": 21000000000000,
          "isTimeBasedSniper": true  // Bought within 20s of launch
        }
      ]
    },
    "insiders": {
      "count": 5,
      "totalBalance": 25000000000000,
      "totalPercentage": 2.5,
      "wallets": [
        { "wallet": "In1...der", "percentage": 0.8, "amount": 8000000000000 }
      ]
    },
    "top10": 28.4,                   // % of supply held by top 10 wallets
    "dev": {
      "percentage": 1.1,
      "amount": 11000000000000,
      "stats": {
        "total_launched": 14,
        "total_migrated": 2
      }
    }
  },

  // Lifecycle status derived from curvePercentage
  "graduation": {
    "status": "new | active | graduating | graduated"
  },

  // Creator launch history (mirrors risk.dev.stats)
  "dev_stats": {
    "total_launched": 14,
    "total_migrated": 2
  },

  // Recent price ticks (last 50); full history via chart:{mint} or REST
  "priceHistory": [
    { "time": 1711699200123, "price": 0.00000052, "price_usd": 0.0000728 }
  ]
}
```

**Graduation status thresholds:**

| Status | Condition |
|---|---|
| `new` | `curvePercentage` < 50% |
| `active` | 50% ≤ `curvePercentage` < 80% |
| `graduating` | `curvePercentage` ≥ 80% |
| `graduated` | On-chain `complete` flag is `true` |

**Graduation targets by platform:**

| Platform | Target |
|---|---|
| Pump.fun | 85 SOL in real reserves |
| All others | 100 SOL in virtual reserves |

**Sniper / Insider thresholds:**

| Type | Condition |
|---|---|
| Time-based sniper | Any buy within first 20 seconds of token creation |
| Percentage sniper | Holds ≥ 1% of supply |
| Insider | Holds 0.5%–1.0% of supply (not creator, not sniper) |

---

### 4.3 Timestamp Notes

> `token.creation.created_time` is in **Unix seconds**.
> All other timestamps (`pools[0].createdAt`, `pools[0].lastUpdated`, `priceHistory[].time`) are in **milliseconds**.

---

### 4.4 Trending Payload

```jsonc
{
  "type": "trending",
  "room": "trending",
  "data": {
    "1m":  [ /* full token payload */, ... ],  // Top 50 by 1m volume
    "5m":  [ /* full token payload */, ... ],  // Top 50 by 5m volume
    "30m": [ /* full token payload */, ... ],  // Top 50 by 30m volume
    "1h":  [ /* full token payload */, ... ]   // Top 50 by 1h volume
  }
}
```

Rankings are recalculated every 30 seconds.

---

### 4.5 Chart Tick Payload

**On join** — snapshot with full history (up to 500 ticks):

```jsonc
{
  "type": "snapshot",
  "room": "chart:G7B...xyz",
  "data": {
    "mint": "G7B...xyz",
    "priceHistory": [
      { "time": 1711699200000, "price": 0.00000048, "price_usd": 0.0000672 }
    ]
  }
}
```

**On each trade** — single appended tick:

```jsonc
{
  "type": "chart",
  "room": "chart:G7B...xyz",
  "data": {
    "mint": "G7B...xyz",
    "tick": {
      "time": 1711699260123,
      "price": 0.00000055,
      "price_usd": 0.0000770
    }
  }
}
```

---

## 5. Integration Examples

### 5.1 Basic Connection and Joining Rooms

```ts
import { io } from "socket.io-client";

const socket = io("ws://80.190.80.155:3000", {
  transports: ["websocket"],
  reconnection: true,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 10000,
  reconnectionAttempts: Infinity,
});

socket.on("connect", () => {
  console.log("Connected:", socket.id);

  // new is auto-joined; explicitly join others
  socket.emit("join", "graduating");
  socket.emit("join", "graduated");
  socket.emit("join", "trending");
});
```

---

### 5.2 Listening to New Token Launches

```ts
// Fires once on join with the last 50 tokens
socket.on("snapshot", (envelope) => {
  if (envelope.room === "new") {
    renderTokenList(envelope.data); // envelope.data is TokenPayload[]
  }
});

// Fires for every token that launches after connecting
socket.on("message", (envelope) => {
  if (envelope.room === "new") {
    prependToken(envelope.data);
  }
});

// Fires when an existing token in the new list updates
socket.on("update", (envelope) => {
  if (envelope.room === "new") {
    upsertToken(envelope.data.token.mint, envelope.data);
  }
});
```

---

### 5.3 Graduating and Graduated Token Feeds

These two rooms work identically to `new` but are pre-filtered by graduation status. Use them to power a "graduating soon" watchlist or a post-graduation feed without client-side filtering.

**Graduating room** — tokens whose bonding curve is ≥ 80% filled:

```ts
socket.emit("join", "graduating");

// Snapshot fires on join with all currently graduating tokens
socket.on("snapshot", (envelope) => {
  if (envelope.room === "graduating") {
    // envelope.data is TokenPayload[]
    // Each token has graduation.status === "graduating"
    // and curvePercentage >= 80
    renderGraduatingList(envelope.data);
  }
});

// A token enters this room when its curvePercentage crosses 80%
socket.on("message", (envelope) => {
  if (envelope.room === "graduating") {
    addToGraduatingList(envelope.data);
  }
});

// Curve % or price updates on a token already in the list
socket.on("update", (envelope) => {
  if (envelope.room === "graduating") {
    upsertGraduatingToken(envelope.data.token.mint, envelope.data);
  }
});
```

**Graduated room** — tokens whose on-chain `complete` flag is `true`:

```ts
socket.emit("join", "graduated");

// Snapshot fires on join with all graduated tokens the indexer has seen
socket.on("snapshot", (envelope) => {
  if (envelope.room === "graduated") {
    // graduation.status === "graduated" on all entries
    renderGraduatedFeed(envelope.data);
  }
});

// A token enters this room the moment the on-chain complete flag is detected
socket.on("message", (envelope) => {
  if (envelope.room === "graduated") {
    const token = envelope.data;

    // Useful fields at graduation time:
    // token.pools[0].curvePercentage  → should be ~100
    // token.pools[0].txns.volume      → total SOL raised
    // token.risk.dev.stats            → creator history
    prependGraduatedToken(token);
  }
});
```

**Handling a token moving across rooms**

A token progresses through rooms as its curve fills. If you are subscribed to all three rooms simultaneously, you will see the same token appear in `new`, then `graduating`, then `graduated`. Deduplicate by `token.mint` and update in place:

```ts
const allTokens = new Map<string, TokenPayload>();

function handleEnvelope(envelope: any) {
  const rooms = ["new", "graduating", "graduated"];
  if (!rooms.includes(envelope.room)) return;

  const payload: TokenPayload = envelope.data;
  const mint = payload.token.mint;

  // Update status and re-render regardless of which room triggered it
  allTokens.set(mint, payload);
  updateTokenCard(mint, payload);
}

socket.on("snapshot", (envelope) => {
  if (["new", "graduating", "graduated"].includes(envelope.room)) {
    for (const t of envelope.data) allTokens.set(t.token.mint, t);
    renderAll([...allTokens.values()]);
  }
});

socket.on("message", handleEnvelope);
socket.on("update", handleEnvelope);
```

---

### 5.4 Token Detail Page — `token:{mint}` and `chart:{mint}`

These two rooms are always used together on a token detail page. `token:{mint}` delivers full state updates (price, risk, metadata, graduation status). `chart:{mint}` delivers a price history snapshot on join and then a single tick after every trade.

**Subscribe when the user opens a token page, unsubscribe when they leave:**

```ts
let currentMint: string | null = null;

function openTokenPage(mint: string) {
  // Unsubscribe from the previous token's rooms
  if (currentMint) {
    socket.emit("leave", `token:${currentMint}`);
    socket.emit("leave", `chart:${currentMint}`);
  }

  currentMint = mint;
  socket.emit("join", `token:${mint}`);
  socket.emit("join", `chart:${mint}`);
}

function closeTokenPage() {
  if (!currentMint) return;
  socket.emit("leave", `token:${currentMint}`);
  socket.emit("leave", `chart:${currentMint}`);
  currentMint = null;
}
```

**Handle the snapshot (fires immediately on join):**

```ts
socket.on("snapshot", (envelope) => {
  // Full token state — use this to populate the entire detail page at once
  if (envelope.room === `token:${currentMint}`) {
    const payload = envelope.data;

    renderHeader(payload.token);
    renderPoolStats(payload.pools[0]);
    renderPriceChanges(payload.events);
    renderRiskPanel(payload.risk);
    renderGraduationBar(payload.pools[0].curvePercentage, payload.graduation.status);
    renderDevStats(payload.dev_stats);
  }

  // Up to 500 historical price ticks — seed the chart before live ticks arrive
  if (envelope.room === `chart:${currentMint}`) {
    initChart(envelope.data.priceHistory);
  }
});
```

**Handle ongoing updates:**

```ts
// Full state refresh — re-render the whole page (price, risk, curve all may have changed)
socket.on("message", (envelope) => {
  if (envelope.room === `token:${currentMint}`) {
    const payload = envelope.data;
    renderPoolStats(payload.pools[0]);
    renderPriceChanges(payload.events);
    renderRiskPanel(payload.risk);
    renderGraduationBar(payload.pools[0].curvePercentage, payload.graduation.status);
  }
});

// Lightweight trade event — cheaper than a full message; use for the activity feed only
socket.on("trade", (event) => {
  // Shape: { mint, signature, type: "buy" | "sell", solAmount, maker }
  if (event.mint === currentMint) {
    appendToActivityFeed({
      side: event.type,
      sol: event.solAmount,
      wallet: event.maker,
      sig: event.signature,
    });
  }
});

// Single price tick — append to chart without re-fetching history
socket.on("chart", (envelope) => {
  if (envelope.room === `chart:${currentMint}`) {
    appendChartTick(envelope.data.tick);
  }
});
```

**Risk data note:** Risk fields (`snipers`, `insiders`, `top10`, `dev`) may be empty for up to 60 seconds after the token launches. Render a loading state and update it when the first `message` event arrives with a populated `risk` object.

```ts
function renderRiskPanel(risk: Risk) {
  if (!risk || risk.snipers.count === 0 && risk.insiders.count === 0 && risk.top10 === 0) {
    showRiskSkeleton();
    return;
  }
  showRiskData(risk);
}
```

---

### 5.5 Trending Feed — `trending`

The `trending` room emits pre-sorted lists of the top 50 tokens by volume across four time windows: `1m`, `5m`, `30m`, and `1h`. The full list for all windows is sent in a single payload — switching windows is a local state change, not a new subscription.

**Subscribe and render:**

```ts
socket.emit("join", "trending");

let trending: Record<"1m" | "5m" | "30m" | "1h", TokenPayload[]> = {
  "1m": [], "5m": [], "30m": [], "1h": [],
};
let activeWindow: "1m" | "5m" | "30m" | "1h" = "1m";

// Snapshot fires on join — populate all four windows immediately
socket.on("snapshot", (envelope) => {
  if (envelope.room === "trending") {
    trending = envelope.data;
    renderTrendingTable(trending[activeWindow]);
  }
});

// Full refresh every ~30 seconds — replace, do not append
socket.on("message", (envelope) => {
  if (envelope.room === "trending") {
    trending = envelope.data;
    renderTrendingTable(trending[activeWindow]);
  }
});
```

**Switch time windows without re-subscribing:**

```ts
function setTrendingWindow(window: "1m" | "5m" | "30m" | "1h") {
  activeWindow = window;
  // Data is already in memory — no round trip needed
  renderTrendingTable(trending[window]);
}
```

**Rendering a trending row:**

```ts
function renderTrendingTable(tokens: TokenPayload[]) {
  const rows = tokens.map((t, index) => ({
    rank: index + 1,
    name: t.token.name,
    symbol: t.token.symbol,
    image: t.token.image,
    platform: t.pools[0].market,
    price: t.pools[0].price.usd,
    priceChange: t.events[activeWindow].priceChangePercentage,
    volume24h: t.pools[0].txns.volume24h,
    marketCap: t.pools[0].marketCap.usd,
    curvePercentage: t.pools[0].curvePercentage,
    mint: t.token.mint,
  }));

  mountTable(rows);
}
```

---

### 5.6 Live Price Chart — `chart:{mint}`

The `chart:{mint}` room exists solely to stream price ticks. It is lighter than `token:{mint}` — it does not carry full token state, only `{ time, price, price_usd }` per tick.

**Seed and stream using [lightweight-charts](https://github.com/tradingview/lightweight-charts):**

```ts
import { createChart, IChartApi, ISeriesApi } from "lightweight-charts";

let chart: IChartApi | null = null;
let series: ISeriesApi<"Line"> | null = null;

function mountChart(container: HTMLElement) {
  chart = createChart(container, { width: container.clientWidth, height: 400 });
  series = chart.addLineSeries({ color: "#7c3aed" });
}

// Snapshot: seed with up to 500 historical ticks
socket.on("snapshot", (envelope) => {
  if (envelope.room === `chart:${currentMint}` && series) {
    const points = envelope.data.priceHistory.map((t: PriceTick) => ({
      time: Math.floor(t.time / 1000) as UTCTimestamp, // convert ms → seconds
      value: t.price_usd,
    }));
    series.setData(points);
  }
});

// Live tick: append without replacing
socket.on("chart", (envelope) => {
  if (envelope.room === `chart:${currentMint}` && series) {
    const { tick } = envelope.data;
    series.update({
      time: Math.floor(tick.time / 1000) as UTCTimestamp,
      value: tick.price_usd,
    });
  }
});
```

**Switching between SOL and USD price:**

```ts
type PriceMode = "usd" | "sol";
let priceMode: PriceMode = "usd";

function setPriceMode(mode: PriceMode) {
  priceMode = mode;

  if (!series || !currentMint) return;

  // Re-fetch history from REST and re-seed (snapshot would require a leave/re-join)
  fetch(`http://80.190.80.155:3000/api/token/${currentMint}/history`)
    .then(r => r.json())
    .then(({ history }) => {
      const points = history.map((t: PriceTick) => ({
        time: Math.floor(t.time / 1000) as UTCTimestamp,
        value: mode === "usd" ? t.price_usd : t.price,
      }));
      series!.setData(points);
    });
}

// Also update the live tick handler to respect mode
socket.on("chart", (envelope) => {
  if (envelope.room === `chart:${currentMint}` && series) {
    const { tick } = envelope.data;
    series.update({
      time: Math.floor(tick.time / 1000) as UTCTimestamp,
      value: priceMode === "usd" ? tick.price_usd : tick.price,
    });
  }
});
```

---

### 5.7 Using REST for Initial Load

REST is best for server-side rendering or prefetching before the WebSocket is ready. Switch to WebSocket for all live updates.

```ts
// Latest 50 tokens — same shape as the `new` room snapshot
const latest = await fetch("http://80.190.80.155:3000/new").then(r => r.json());

// Full payload for a single token
const token = await fetch(`http://80.190.80.155:3000/api/token/${mint}`).then(r => r.json());

// Extended price history for chart seeding
const history = await fetch(`http://80.190.80.155:3000/api/token/${mint}/history`).then(r => r.json());
// { mint: string, count: number, history: PriceTick[] }

// Service health
const stats = await fetch("http://80.190.80.155:3000/api/stats").then(r => r.json());
// { status: "ok", tokensInRedis: number, uptime: number, timestamp: number }
```

### REST Endpoint Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/new` | Last 50 tokens (full payload each) |
| `GET` | `/api/token/:mint` | Full payload for a single token |
| `GET` | `/api/token/:mint/history` | Extended price history |
| `GET` | `/api/stats` | Service health and token count |

---

## 6. Best Practices

### Re-join rooms after reconnect

Socket.io reconnects automatically, but the server does not persist room membership across reconnections. Re-join every room your client needs inside the `connect` handler.

```ts
const activeRooms = new Set<string>();

socket.on("connect", () => {
  for (const room of activeRooms) {
    socket.emit("join", room);
  }
});

function joinRoom(room: string) {
  activeRooms.add(room);
  socket.emit("join", room);
}

function leaveRoom(room: string) {
  activeRooms.delete(room);
  socket.emit("leave", room);
}
```

### Treat snapshots as ground truth

When a `snapshot` fires, replace your local state for that room entirely — do not merge it into a stale list.

```ts
socket.on("snapshot", (envelope) => {
  if (envelope.room === "new") {
    tokenMap.clear();
    for (const t of envelope.data) tokenMap.set(t.token.mint, t);
    render();
  }
});
```

### Deduplicate by mint

Both `message` and `update` events can arrive for the same token. Key your local state by `token.mint` and upsert on every event.

```ts
const tokenMap = new Map<string, TokenPayload>();

function upsertToken(payload: TokenPayload) {
  tokenMap.set(payload.token.mint, payload);
  render([...tokenMap.values()]);
}
```

### Leave rooms you no longer need

When a user navigates away from a token detail page, leave the corresponding `token:{mint}` and `chart:{mint}` rooms to avoid accumulating unused event listeners and server-side room memberships.

### Do not use `volume` as a 24h metric

`pools[0].txns.volume` is **all-time** volume. Use `pools[0].txns.volume24h` for 24h figures. For trending volume use the pre-sorted `trending` room arrays.

### Expect metadata and risk data to arrive after launch

Images, descriptions, and socials are resolved asynchronously after token creation. Risk analysis has a minimum 60-second cooldown per token. Listen for `update` events and design your UI to handle temporarily empty fields gracefully.

---

## 7. Current Limitations

| Area | Current State |
|---|---|
| **Commitment level** | `PROCESSED` — data arrives as fast as possible but rare chain forks may cause transient incorrect values |
| **Risk analysis latency** | Minimum 60-second cooldown per token; fresh tokens will show empty risk fields for up to a minute |
| **Metadata availability** | `image`, `description`, and socials are fetched asynchronously — may lag a few seconds behind the launch event |
| **Chart depth** | Maximum 500 price ticks stored per token; no long-term OHLCV candle aggregation |
| **24h volume accuracy** | Only counts trades observed since the indexer indexed the token; may undercount on older tokens |
| **Multi-pool support** | `pools` always contains one entry; post-graduation AMM pools are not yet tracked |
| **Graduated room backfill** | The `graduated` snapshot only includes tokens the indexer has seen; tokens that graduated before indexer startup are absent |
| **Risk RPC dependency** | Holder analysis requires Solana RPC calls; congestion during peak network load can delay or skip analysis cycles |
