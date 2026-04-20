# multi-launchpad-data — Integration Reference

## Overview

Two data delivery channels run in parallel:

| Channel | URL | Best For |
|---------|-----|----------|
| WebSocket | `ws://<host>/` | Real-time token launches & updates |
| REST API | `http://<host>/api/` | Queries, backlogs, wallet analysis |

---

## WebSocket

### Connecting

```js
const ws = new WebSocket("ws://localhost:3000");
```

### Subscribing to rooms

Send a JSON subscription message after connecting:

```json
{ "type": "subscribe", "room": "new" }
```

Available rooms:

| Room | Description |
|------|-------------|
| `new` | Every new token launch across all platforms |
| `update` | Curve/price updates for existing tokens |
| `graduating` | Tokens that just crossed 80% bonding curve |

### Message shape

Every WS message has the same envelope:

```json
{
  "type": "message",
  "room": "new",
  "data": {
    "token": { ... },
    "pools": [ { ... } ],
    "events": { "1m": { "priceChangePercentage": 0 }, ... },
    "risk": { "snipers": { ... }, "insiders": { ... }, "top10": 0, "dev": { ... } },
    "graduation": { "status": "new" },
    "priceHistory": [ { "time": 1713600000000, "price": 0.000012, "price_usd": 0.0018 } ],
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
- `"skeleton"` — just launched, name/symbol only
- `"partial"` — has price, risk data still arriving
- `"complete"` — full data including top10 holders and risk scan

### Example (Node.js)

```js
import WebSocket from "ws";

const ws = new WebSocket("ws://localhost:3000");

ws.on("open", () => {
  ws.send(JSON.stringify({ type: "subscribe", room: "new" }));
});

ws.on("message", (raw) => {
  const msg = JSON.parse(raw.toString());
  if (msg.type !== "message") return;
  const { token, pools, risk, meta } = msg.data;
  console.log(`[${meta.dataQuality}] ${token.symbol} — $${pools[0].price.usd}`);
});
```

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

If not cached and `refresh=true` not passed, returns HTTP 202:
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
    { "time": 1713600000000, "open": 0.000040, "high": 0.000045, "low": 0.000038, "close": 0.000042, "volume": 1.42 }
  ]
}
```

---

#### `POST /api/price/multiple`

Batch price lookup. Up to 50 mints per request.

```json
// Request body
{ "mints": ["mint1...", "mint2...", "mint3..."] }
```

```json
{
  "success": true,
  "prices": {
    "mint1...": { "name": "Frog Coin", "symbol": "FROG", "priceUsd": 0.000042, "priceNative": 0.0000000012, "marketCapUsd": 42000, "change24h": 0, "liquidityUsd": 3645 }
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
GET /api/wallet/CaesarxWalletAddress.../net-worth?refresh=true
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

`unindexed` = number of token accounts with no price in this system.

---

#### `GET /api/wallet/:wallet/transactions`

Tokens launched by this wallet (CREATE events).

| Query param | Type | Default |
|-------------|------|---------|
| `limit` | number | 50 (max 100) |
| `cursor` | string | — (createdAt ms for pagination) |

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

Track record: all launched tokens with current performance.

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
