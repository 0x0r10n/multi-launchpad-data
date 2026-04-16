# Last Update — Full Session Fix Log
**Date:** 2026-04-15  
**Scope:** Full audit + optimisation of the entire multi-launchpad-data indexer against the grpc-geyser-tutorial reference implementation.

---

## Current State (end of session)

- **Running:** 5 Geyser streams (1 tx + 4 account), 800 curve PDAs tracked simultaneously
- **Platforms indexed:** pump, moon, bags, meteora, letsbonk, launchlab
- **All known bugs fixed**, all unnecessary RPCs removed, all Redis hotpaths pipelined
- **Dev stats bootstrap** working (historical on-chain counts, 2 RPCs per creator)
- **Price change events** O(1) per trade (reference-price anchors, no list scans)
- **Confirmed live:** launches, rich payloads, enrichment, dev stats all flowing correctly

---

## Fix 1 — `shared.ts`: `readU64` precision (bigint)

**File:** `src/launchpads/shared.ts`  
**Problem:** `readU64` returned `number`. JS number is safe only up to 2^53 (~9×10^15). Solana u64s go up to 2^64 — virtualTokenReserves on a fresh curve is ~1×10^15, already near the limit. Price calculations were silently losing precision.  
**Fix:** Return `bigint` instead of `number`.
```typescript
// Before
export function readU64(buf: Buffer, offset: number): number {
  return buf.readUInt32LE(offset) + buf.readUInt32LE(offset + 4) * 0x100000000;
}

// After
export function readU64(buf: Buffer, offset: number): bigint {
  const lo = BigInt(buf.readUInt32LE(offset));
  const hi = BigInt(buf.readUInt32LE(offset + 4));
  return (hi << 32n) | lo;
}
```

---

## Fix 2 — `types.ts`: `CurveState` interface all fields → bigint

**File:** `src/launchpads/types.ts`  
**Problem:** `CurveState` had reserve fields typed as `number`, inconsistent with the bigint fix above.  
**Fix:** All four reserve fields changed to `bigint`.
```typescript
export interface CurveState {
  virtualTokenReserves: bigint;
  virtualSolReserves:   bigint;
  realTokenReserves:    bigint;
  realSolReserves:      bigint;
  complete:             boolean;
  curvePercentage:      number;
}
```

---

## Fix 3 — All 5 parsers: bigint cascade + swap detection precision

**Files:** `src/launchpads/pump.ts`, `moon.ts`, `bags.ts`, `launchlab.ts`, `letsbonk.ts`

### pump.ts
- Removed `LAMPORTS = 1_000_000_000` and float `GRADUATION_TARGET = 85`
- Added `GRADUATION_TARGET_LAMPORTS = 85n * 1_000_000_000n`
- `curvePercentage` now: `Math.min(100, Number(realSolReserves * 100n / GRADUATION_TARGET_LAMPORTS))`
- Removed `BigInt()` casts (readU64 already returns bigint)

### moon.ts, bags.ts, launchlab.ts, letsbonk.ts
- Swap detection tightened: `l.includes("Buy")` → `l.includes("Instruction: Buy")` (prevents false positives from log lines that merely mention the word "Buy")
- `realTokenReserves = 0n`, `realSolReserves` default `0n` (not `0`)
- Removed `BigInt()` casts

---

## Fix 4 — `curve-tracker.ts`: bigint arithmetic boundary + isStartup guard

**File:** `src/curve-tracker.ts`

### Bigint → Number conversion
Added `N`-suffix variables at the arithmetic boundary in `buildCurveUpdate` so all floating-point price math uses `number` (no bigint arithmetic):
```typescript
const virtualTokenReservesN = Number(decoded.virtualTokenReserves);
const virtualSolReservesN   = Number(decoded.virtualSolReserves);
const realSolReservesN      = Number(decoded.realSolReserves);
```
Return object uses `virtualTokenReservesN.toString()`, etc.

### isStartup guard
Added `isStartup = false` parameter to `processCurveAccountUpdate`. Skips `redis.publish("token-updates", mint)` during startup state seeding — prevents spurious WebSocket updates being broadcast to all clients every time the indexer restarts.

### SOL price fallback order
Jupiter API first (stable public endpoint), pump.fun internal API as fallback (undocumented, may change).

---

## Fix 5 — `yellowstone-manager.ts`: 6 fixes

**File:** `src/yellowstone-manager.ts`

### 5a — Ping was wiping subscriptions every 10s
**Root cause:** Every `stream.write()` is a full replacement of that subscription type in proto3. The old keepalive sent `{ ping, accounts: {}, transactions: {} }` — the empty maps serialised identically to absent fields, wiping both the account and transaction subscriptions every 10 seconds.  
**Fix:** Merged ping into `sendFullSubscription(includePing = false)`. The keepalive now calls `sendFullSubscription(true)` — one write that carries both the ping and the full subscription state.

### 5b — `accountsDataSlice` bandwidth reduction
Added `accountsDataSlice: [{ offset: "0", length: "50" }]` to account stream subscriptions. All parsers read ≤ 48 bytes. Cuts Geyser account update bandwidth ~4× vs. receiving full account data.  
**Note:** Values must be strings (`"0"`, `"50"`) — the Rust/NAPI binding rejects JS numbers with `code=StringExpected`.

### 5c — write_version deduplication
Added `private lastWriteVersion = new Map<string, bigint>()`. On every account update, skips processing if `writeVersion <= lastWriteVersion[pubkey]`. Geyser commonly delivers the same account snapshot twice on reconnects — without this, each duplicate triggers a full curve decode + Redis write + broadcast.

### 5d — New-launch signature deduplication
```typescript
const isNew = await redis.set(`dedup:${signature}`, "1", "EX", 120, "NX");
if (!isNew) return;
```
Prevents double-launch and double creator counter increment when Yellowstone delivers the same create transaction twice.

### 5e — Slot-based sniper detection
Replaced wall-clock `Date.now() - createdAt <= 20_000` with on-chain slot delta:
```typescript
const slotDiff = Number(data.slot) - parseInt(launchSlotStr);
if (slotDiff >= 0 && slotDiff <= 50) { /* sniper */ }
```
50 slots ≈ 20 seconds. Wall clock was unreliable because it was anchored to indexer wall time, not on-chain time — restarts, latency spikes, and clock drift all caused missed or false snipers.

### 5f — `meta.fee` for SOL delta
```typescript
const fee = Number(meta?.fee ?? 5000);
```
Replaces hardcoded 5000 lamport base fee. Priority fees are 10×–200× the base during high-activity periods — without this, buy/sell SOL amounts were significantly overstated.

---

## Fix 6 — `risk-analyzer.ts`: removed `getTokenSupply` RPC

**File:** `src/risk-analyzer.ts`  
**Problem:** `analyzeRisk` was calling `connection.getTokenSupply(mintPubkey)` on every full risk scan. Supply is always `1,000,000,000 × 10^6 = 1×10^15` for every indexed token — it never changes.  
**Fix:** Removed `getTokenSupply` RPC entirely. `totalSupplyRaw` now uses the `RAW_SUPPLY` constant directly, saving 1 RPC per full risk analysis.

---

## Fix 7 — `enricher.ts`: pipeline the enrichment sweep

**File:** `src/enricher.ts` — `startEnrichmentSweep()`  
**Problem:** The 15-second retry sweep called `await redis.hmget(...)` in a for-loop — 50 sequential Redis round-trips.  
**Fix:** Single pipeline:
```typescript
const pipeline = redis.pipeline();
for (const mint of mints) pipeline.hmget(`token:${mint}`, "image", "name", "symbol");
const results = await pipeline.exec();
```
50 round-trips → 1.

---

## Fix 8 — `price-tracker.ts`: O(1) reference-price anchors

**File:** `src/price-tracker.ts` — `calcPriceEvents()`  
**Problem:** On every trade, read the entire price tick list (`lrange price:{mint} 0 -1`, up to 400 entries), then scan it 7 times for 7 intervals. O(N) per trade.  
**Fix:** Reference-price anchor approach. For each interval `I`, store two fields in the token hash:
- `priceRef_{I}` — price at the start of the current window
- `priceRefTs_{I}` — when that anchor was captured

On each trade: one `hmget` (15 fields, 1 RTT) → compute 7 percentage changes inline → one `hset`. The anchor rolls forward once it ages past `I`.

`startPriceTracker` was also sequential (`await recordPrice(mint)` in a loop). Fixed to pipeline the `complete` check then `Promise.all` the active mints.

---

## Fix 9 — `index.ts`: 3 fixes

**File:** `src/index.ts`

### 9a — Missing WebSocket `leave` handler
The `/doc` endpoint told clients to `socket.emit("leave", room)` but there was no server-side handler — clients accumulated in rooms forever with no way to leave.
```typescript
socket.on("leave", (room: string) => {
  socket.leave(room);
});
```

### 9b — `getTokensByStatus` pipeline
Replaced `Promise.all(matchingMints.map(m => redis.hgetall(...)))` with a pipeline. N parallel hgetall calls → 1 round-trip.

### 9c — `calculateTrendingTokens` pipeline
Same fix for the trending token payload fetch. Also bumped `zrevrange` scan from 200 → 500 tokens to get more accurate volume rankings now that we track more tokens.

---

## Fix 10 — `dev-stats.ts`: full rewrite

**File:** `src/dev-stats.ts`  
**Problem (old version):**
- Wrote to `dev_stats:{creator}` keys — different namespace from the rest of the codebase which uses `creator:{address}`
- Used `getParsedTransaction` in a for-loop — up to 100 sequential RPCs per creator
- Loose log matching (`includes('create')`, `includes('migrate')`) caused false positives
- Never imported anywhere — effectively dead code

**Fix:** Complete rewrite.
- **2 RPCs total** per creator: `getSignaturesForAddress` (1) + `getTransactions` batch (1 per 256-chunk)
- Uses precise `CREATE_SIGNALS` and `MIGRATE_SIGNALS` arrays matching the same strings the live parsers use
- Writes to `creator:{address}` (same namespace as `hincrby` in yellowstone-manager)
- `Math.max(bootstrap, live)` merge — never downgrades live-tracked counts
- `bootstrapAt` timestamp in the `creator:` hash prevents re-scanning within 24h
- `bootstrapDevStatsIfNeeded(maker)` called from yellowstone-manager on every new launch (fire-and-forget)

---

## Fix 11 — `yellowstone-manager.ts`: exponential backoff

**File:** `src/yellowstone-manager.ts` — `handleReconnect()`  
**Problem:** Fixed 5-second reconnect delay. If Geyser is down, hammers the endpoint at constant rate.  
**Fix:**
```typescript
const delay = Math.min(5000 * Math.pow(2, this.reconnectAttempts), 60_000);
// 5s → 10s → 20s → 40s → 60s (cap)
```
`reconnectAttempts` resets to 0 on successful connection.

---

## Fix 12 — `yellowstone-manager.ts`: gRPC error code handling

**File:** `src/yellowstone-manager.ts`  
**Problem:** All stream errors triggered a reconnect, including permanent auth errors (UNAUTHENTICATED, PERMISSION_DENIED) that reconnecting can never fix.  
**Fix:** Check gRPC status code before reconnecting:
```typescript
if (code === 16 || code === 7) {
  console.error("[Yellowstone] ⛔ Auth error — check CHAINSTACK_GEYSER_TOKEN. Not reconnecting.");
  return;
}
```

---

## Fix 13 — Multi-stream architecture (5 streams, 800 PDAs)

**File:** `src/yellowstone-manager.ts` — full refactor  
**Trigger:** User upgraded Chainstack plan to 5 streams / 250 accounts.  
**Before:** 1 stream handling both transactions and curve account subscriptions, 50 PDA limit.

**Architecture:**
```
Stream 0 (txStream)      — launchpad transactions only (all 6 programs)
Stream 1 (accountStream) — curve PDAs 0-199
Stream 2 (accountStream) — curve PDAs 200-399
Stream 3 (accountStream) — curve PDAs 400-599
Stream 4 (accountStream) — curve PDAs 600-799
```

**Key implementation points:**
- All 5 streams opened on the same `Client` connection (one TCP channel to Chainstack)
- `addCurvePDA` distributes across account streams using least-loaded assignment
- `removeCurvePDA` searches all 4 maps to find and remove the PDA from the correct stream
- Each stream has its own 20ms debounce timer for batching rapid subscription changes
- Each stream has its own 10-second keepalive interval (all 5 streams pinged independently)
- `restoreCurveSubscriptions` loads up to 800 tokens from Redis on startup
- `txStream` requires `accountsDataSlice: []` (empty array) — proto field must be present even when unused
- Geyser server hard cap is 200 pubkeys per subscription (not 250 as plan states — plan limit may count differently)
- `processAccountUpdate` searches all 4 maps to find the curve info for an incoming update

**Result:** 800 curve PDAs tracked simultaneously (was 50, 16× increase). Transaction monitoring fully isolated from account update bandwidth — pump.fun tx volume cannot starve curve price updates.

---

## Scan limits updated across codebase

| File | Was | Now | Reason |
|------|-----|-----|--------|
| `index.ts` — `getTokensByStatus` | 200 | 500 | More tokens tracked |
| `index.ts` — `calculateTrendingTokens` | 200 | 500 | More accurate volume rankings |
| `enricher.ts` — sweep | 50 | 200 | More tokens need enrichment |
| `price-tracker.ts` — snapshot | 50 | 200 | More tokens need price ticks |
| `yellowstone-manager.ts` — restore | 50 | 800 | Matches new PDA capacity |

---

## Files touched in this session

| File | Type of change |
|------|---------------|
| `src/launchpads/shared.ts` | readU64 → bigint |
| `src/launchpads/types.ts` | CurveState fields → bigint |
| `src/launchpads/pump.ts` | bigint graduation, swap signal fix |
| `src/launchpads/moon.ts` | bigint defaults, swap signal fix |
| `src/launchpads/bags.ts` | bigint defaults, swap signal fix |
| `src/launchpads/launchlab.ts` | bigint defaults, swap signal fix |
| `src/launchpads/letsbonk.ts` | bigint defaults, swap signal fix |
| `src/curve-tracker.ts` | bigint boundary, isStartup guard, SOL price order |
| `src/yellowstone-manager.ts` | ping fix, accountsDataSlice, write_version dedup, sig dedup, slot sniper, meta.fee, backoff, auth error check, full multi-stream rewrite |
| `src/risk-analyzer.ts` | Removed getTokenSupply RPC, totalSupplyRaw constant |
| `src/enricher.ts` | Pipeline sweep hmget, scan limit 50→200 |
| `src/price-tracker.ts` | O(1) reference-price anchors, parallel snapshot loop, scan limit 50→200 |
| `src/index.ts` | isStartup pass-through, slot sniper, leave handler, pipeline hgetall, scan limits 200→500 |
| `src/dev-stats.ts` | Full rewrite (batch RPCs, correct namespace, Math.max merge) |

---

## What is NOT changed (working as-is)

- `src/launchpads/index.ts` — parser registry, detectParser, resolveMeteoraId all correct
- `src/price-utils.ts` — addPriceTick with trim-on-overflow is fine
- Express REST endpoints — `/new`, `/api/token/:mint`, `/api/stats`, `/doc` all correct
- Socket.io room structure — all rooms, snapshot-on-join, two-stage broadcast all correct
- Redis key namespaces — `token:`, `creator:`, `snipers_set:`, `trades:`, `price:`, `payload:`, `dedup:`, `meta-cache:` all consistent
