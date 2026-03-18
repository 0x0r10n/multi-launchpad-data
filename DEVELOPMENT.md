# Pump.fun Real-Time Token Indexer

> Ultra-low-latency Solana token detection and enrichment pipeline for Pump.fun, using Yellowstone gRPC.

## Architecture

```
Yellowstone gRPC (PROCESSED)
        │
        ▼
  yellowstone-manager.ts  ──▶  Detects new launches + parses trades
        │
        ├──▶  index.ts (WebSocket broadcast: skeleton)
        │
        └──▶  Background enrichment (parallel, instant):
              ├── enricher.ts       → image, desc, socials (Pump.fun API primary)
              ├── curve-tracker.ts  → liquidity, price, mcap, curvePercentage
              ├── risk-analyzer.ts  → top10, snipers, insiders, dev holdings
              ├── price-tracker.ts  → price ticks, 1m/5m/15m/30m/1h/4h/24h events
              └── dev-stats.ts      → creator's total launches + migrations
                    │
                    ▼
              Redis PUBSUB → index.ts → WebSocket broadcast (full update)
```

## Source Files

| File | Purpose | Size |
|------|---------|------|
| `src/index.ts` | Express API + Socket.IO + payload builder + PUBSUB listener | 10.5 KB |
| `src/yellowstone-manager.ts` | Yellowstone gRPC stream, tx parsing, event emission | 10.7 KB |
| `src/enricher.ts` | Token metadata: image, description, socials | 5.7 KB |
| `src/curve-tracker.ts` | Bonding curve data: price, liquidity, mcap, graduation | 6.0 KB |
| `src/risk-analyzer.ts` | Wallet risk: top10 holders, snipers, insiders, dev stats | 6.5 KB |
| `src/price-tracker.ts` | Price ticks on every trade + rolling price change events | 3.4 KB |
| `src/dev-stats.ts` | Creator history: total tokens launched + migrated | 3.6 KB |

## Data Flow

### 1. Token Detection (< 100ms)
- Yellowstone gRPC streams all Pump.fun transactions at `PROCESSED` commitment
- `yellowstone-manager.ts` detects `InitializeMint2` instructions → new token
- Anchor event data parsed from logs for name, symbol, URI, creator, curvePDA
- Skeleton payload broadcast immediately via WebSocket `message` event

### 2. Background Enrichment (100-800ms, all parallel)
All modules fire concurrently, no sequential queues:

- **Enricher**: Fetches from `https://frontend-api-v3.pump.fun/coins/{mint}` (primary, ~200ms) or IPFS metadata (fallback, ~500ms)
- **Curve Tracker**: Reads bonding curve account for real-time price/liquidity
- **Risk Analyzer**: `getTokenLargestAccounts` + 15 parallel `getParsedAccountInfo` calls
- **Price Tracker**: Records price tick to Redis list
- **Dev Stats**: Scans creator's token history

### 3. Redis PUBSUB Broadcasting
Each background module calls `redis.publish("token-updates", mint)` after updating Redis. The PUBSUB listener in `index.ts` picks this up, rebuilds the full payload (including priceHistory), and broadcasts via WebSocket `update` event.

### 4. Trade Processing (every trade, instant)
On each buy/sell:
- Trade counts (buys/sells/total) incremented
- Volume accumulated (SOL)
- Trade logged to `trades:{mint}` sorted set (for volume24h)
- Curve data refreshed
- Price tick recorded + events recalculated
- Risk analysis triggered (first trade only, with in-flight dedup)

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/new` | Latest 50 tokens with full payload (parallelized, ~200ms) |
| GET | `/api/token/:mint` | Single token full payload with price history |
| GET | `/api/token/:mint/history` | Price history for charts |

## WebSocket Events

| Room | Event | Trigger | Data |
|------|-------|---------|------|
| `new` | `message` | New token detected | Skeleton payload |
| `new` | `update` | Background data populated | Full payload |
| `token:{mint}` | `trade` | Every buy/sell | `{mint, signature, type, solAmount}` |
| `token:{mint}` | `update` | Curve/risk/price refresh | Full payload |

## Full Payload Structure

```json
{
  "type": "message",
  "room": "new",
  "data": {
    "token": {
      "name": "Token Name",
      "symbol": "SYM",
      "mint": "...",
      "uri": "https://...",
      "decimals": 6,
      "description": "...",
      "image": "https://...",
      "hasFileMetaData": true,
      "createdOn": "https://pump.fun",
      "strictSocials": {
        "twitter": "",
        "telegram": "",
        "website": ""
      },
      "creation": {
        "creator": "...",
        "created_tx": "...",
        "created_time": 1773863710
      }
    },
    "pools": [{
      "poolId": "curvePDA",
      "liquidity": { "quote": 30.0, "usd": 2700.0 },
      "price": { "quote": 2.8e-08, "usd": 2.5e-06 },
      "tokenSupply": 1000000000000000,
      "lpBurn": 100,
      "tokenAddress": "...",
      "marketCap": { "quote": 28.0, "usd": 2520.0 },
      "decimals": 6,
      "security": { "freezeAuthority": null, "mintAuthority": null },
      "quoteToken": "So11111111111111111111111111111111111111112",
      "market": "pumpfun",
      "deployer": "...",
      "lastUpdated": 1773863720000,
      "createdAt": 1773863710000,
      "txns": {
        "buys": 94,
        "sells": 117,
        "total": 211,
        "volume": 12236.0,
        "volume24h": 12236.0
      },
      "curvePercentage": 3.83,
      "creation": { "creator": "...", "created_tx": "...", "created_time": 1773863710000 }
    }],
    "events": {
      "1m":  { "priceChangePercentage": -34.39 },
      "5m":  { "priceChangePercentage": -19.16 },
      "15m": { "priceChangePercentage": -19.16 },
      "30m": { "priceChangePercentage": -19.16 },
      "1h":  { "priceChangePercentage": -19.16 },
      "4h":  { "priceChangePercentage": -19.16 },
      "24h": { "priceChangePercentage": -19.16 }
    },
    "risk": {
      "snipers": {
        "count": 2,
        "totalBalance": 196259020,
        "totalPercentage": 9.81,
        "wallets": [{ "wallet": "...", "percentage": 5.0, "amount": 100000000 }]
      },
      "insiders": {
        "count": 0,
        "totalBalance": 0,
        "totalPercentage": 0,
        "wallets": []
      },
      "top10": 21.17,
      "dev": {
        "percentage": 11.36,
        "amount": 227062500,
        "stats": { "total_launched": 4, "total_migrated": 0 }
      }
    },
    "graduation": { "status": "new" },
    "dev_stats": { "total_launched": 4, "total_migrated": 0 },
    "priceHistory": [
      { "time": 1773863710000, "price": 2.8e-08, "price_usd": 2.5e-06 }
    ]
  }
}
```

## External APIs

| API | Purpose | Cache |
|-----|---------|-------|
| `https://frontend-api-v3.pump.fun/coins/{mint}` | Primary metadata (image, desc, socials) | None (instant) |
| `https://frontend-api-v3.pump.fun/sol-price` | SOL/USD price (primary) | 30s |
| `https://lite-api.jup.ag/price/v3?ids=So111...` | SOL/USD price (fallback) | 30s |
| IPFS gateways (cf-ipfs, ipfs.io, pinata) | Metadata fallback | None |

## Background Loops

| Loop | Interval | Purpose |
|------|----------|---------|
| Curve Refresh | 30s | Re-fetch bonding curve for top 20 non-graduated tokens |
| Price Snapshots | 30s | Record price ticks for top 50 tokens (between trades) |
| Risk Analysis | 60s | Re-analyze risk for top 20 tokens |
| Enrichment Sweep | 15s | Retry un-enriched tokens (missing image) |

## Redis Schema

| Key | Type | Description |
|-----|------|-------------|
| `token:{mint}` | Hash | All token fields (name, price, risk, etc.) |
| `tokens:latest` | Sorted Set | Score=createdAt, for listing latest tokens |
| `price:{mint}` | List | JSON tick objects `{time, price, price_usd}`, last 500 |
| `trades:{mint}` | Sorted Set | Score=timestamp, for volume24h calculation |
| `token-updates` | PUBSUB Channel | Mint addresses to trigger WebSocket broadcasts |

## Environment Variables

```env
REDIS_URL=redis://localhost:6379
SOLANA_RPC=https://your-rpc-endpoint
YELLOWSTONE_ENDPOINT=https://your-yellowstone-grpc
YELLOWSTONE_TOKEN=your-auth-token
```

## Running

```bash
npm start          # Start the indexer (tsx src/index.ts)
```

Server starts on port 3000. Connects to Yellowstone gRPC and begins streaming Pump.fun transactions immediately.

## Performance

| Metric | Value |
|--------|-------|
| Token detection latency | < 100ms from on-chain |
| Full enrichment | 200-800ms (all parallel) |
| `/new` endpoint | ~200ms for 50 tokens |
| WebSocket broadcast | Instant on every data change |
| Redis memory (400 tokens) | ~9 MB |
| Throughput | 3000+ txs/min sustained |

## Graduation Status Flow

```
new (< 50% curve) → active (≥ 50%) → graduating (≥ 80%) → graduated (complete)
```

## Volume & Price

- **Volume** and **volume24h** are in **USD** (SOL amount × SOL price)
- **SOL price** fetched from Pump.fun API (primary) with Jupiter fallback
- **Price events** (1m through 24h) recalculated on every trade, not just periodically
- **Price ticks** stored per-trade in a Redis list (last 500 ticks per token)
