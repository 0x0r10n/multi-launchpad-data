// src/global-tracker.ts — On-demand tracking for ANY Solana SPL token
// When a user requests chart data for a token we don't already track,
// this module discovers its Raydium pool, registers it with Yellowstone
// for real-time Geyser account pushes, and seeds initial price data.
//
// Lifecycle:
//   1. watchToken(mint) → pool discovery → Geyser subscription → initial price seed
//   2. Geyser pushes curve-update events → curve-tracker processes them (same as launchpad tokens)
//   3. Cleanup: tokens with no chart room subscribers for 48h get unwatched
//
// Capacity: max 200 global tokens (protects Geyser stream slots + RPC budget)
// Eviction: LRU — oldest lastAccess is removed when at capacity

import { YellowstoneManager } from "./yellowstone-manager";
import { findRaydiumPool, PoolInfo } from "./pool-resolver";
import { decodeRaydiumPoolWithVaults, parseRaydiumPoolInfo } from "./launchpads/raydium";
import { pollPoolTrades, recordGlobalTrades } from "./trade-poller";
import { addPriceTick } from "./price-utils";
import { calcPriceEvents } from "./price-tracker";
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

const redis      = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const MAX_GLOBAL_TOKENS  = 200;
const CLEANUP_INTERVAL   = 300_000; // 5 min
const IDLE_TIMEOUT       = 48 * 3600 * 1000; // 48h

interface WatchEntry {
  mint: string;
  pool: PoolInfo;
  lastAccess: number;
}

class GlobalTracker {
  private yellowstone: YellowstoneManager | null = null;
  private watched = new Map<string, WatchEntry>();
  private discovering = new Set<string>(); // prevent duplicate discovery
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private tradePollTimer: ReturnType<typeof setInterval> | null = null;

  /**
   * Must be called once after YellowstoneManager is created.
   */
  init(ym: YellowstoneManager) {
    this.yellowstone = ym;

    // Start periodic cleanup (every 5 min)
    this.cleanupTimer = setInterval(() => this.cleanup(), CLEANUP_INTERVAL);

    // Start periodic price poll (every 10s) — Raydium reserves change on every swap
    // but Geyser pushes on the pool account don't contain reserves (they're in vault SPL accounts).
    // So we poll vault balances periodically to keep prices fresh.
    this.pollTimer = setInterval(() => this.pollAllWatched(), 10_000);

    // Start periodic trade poll (every 15s) — fetch Raydium swap transactions via RPC
    // to populate trades:{mint} sorted set for candlestick volume data.
    this.tradePollTimer = setInterval(() => this.pollAllTrades(), 15_000);

    // Restore previously-watched global tokens from Redis
    this.restoreFromRedis().catch(e =>
      console.error("[GlobalTracker] Restore failed:", e.message)
    );
  }

  /**
   * Start tracking a token. Idempotent — no-op if already tracked.
   * Returns immediately; discovery happens async.
   */
  async watchToken(mint: string): Promise<"tracking" | "discovering" | "failed"> {
    // Already watching via Geyser?
    if (this.watched.has(mint)) {
      this.watched.get(mint)!.lastAccess = Date.now();
      await redis.set(`global:lastAccess:${mint}`, Date.now().toString());
      return "tracking";
    }

    // Already a launchpad token? Just touch the access time.
    const existingPlatform = await redis.hget(`token:${mint}`, "platform");
    if (existingPlatform && existingPlatform !== "raydium") {
      return "tracking"; // launchpad token — already fully tracked
    }

    // Already in discovery?
    if (this.discovering.has(mint)) return "discovering";

    // Start async discovery
    this.discovering.add(mint);
    this.discover(mint).catch(e => {
      console.error(`[GlobalTracker] Discovery failed for ${mint.slice(0, 12)}:`, e.message);
      this.discovering.delete(mint);
    });

    return "discovering";
  }

  /**
   * Stop tracking a token. Frees Geyser stream slot.
   */
  async unwatchToken(mint: string): Promise<boolean> {
    const entry = this.watched.get(mint);
    if (!entry) return false;

    this.yellowstone?.removeCurvePDA(entry.pool.poolAddress);
    this.watched.delete(mint);
    await redis.srem("global:watched", mint);
    await redis.del(`global:lastAccess:${mint}`);
    console.log(`[GlobalTracker] Unwatched ${mint.slice(0, 12)}`);
    return true;
  }

  /**
   * Touch — update lastAccess when a user joins a chart room.
   */
  touch(mint: string) {
    const entry = this.watched.get(mint);
    if (entry) {
      entry.lastAccess = Date.now();
      redis.set(`global:lastAccess:${mint}`, Date.now().toString()).catch(() => {});
    }
  }

  isWatched(mint: string): boolean {
    return this.watched.has(mint);
  }

  get watchedCount(): number {
    return this.watched.size;
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private async discover(mint: string) {
    const pool = await findRaydiumPool(mint);
    this.discovering.delete(mint);

    if (!pool) {
      console.warn(`[GlobalTracker] No pool found for ${mint.slice(0, 12)}`);
      return;
    }

    // Evict if at capacity (LRU)
    if (this.watched.size >= MAX_GLOBAL_TOKENS) {
      this.evictLRU();
    }

    // Fetch on-chain metadata (name/symbol) via Metaplex if we don't have it
    const existingName = await redis.hget(`token:${mint}`, "name");
    if (!existingName) {
      await this.seedTokenMetadata(mint);
    }

    // Register with Yellowstone for real-time Geyser pushes
    this.yellowstone?.addCurvePDA(pool.poolAddress, mint, "raydium", true);

    // Seed initial price from current pool state
    await this.seedInitialPrice(mint, pool);

    // Record in memory + Redis
    const entry: WatchEntry = { mint, pool, lastAccess: Date.now() };
    this.watched.set(mint, entry);
    await redis.sadd("global:watched", mint);
    await redis.set(`global:lastAccess:${mint}`, Date.now().toString());

    console.log(`[GlobalTracker] Now tracking ${mint.slice(0, 12)} → pool ${pool.poolAddress.slice(0, 12)}`);
  }

  private async seedInitialPrice(mint: string, pool: PoolInfo) {
    try {
      // Read the pool account data
      const poolAccountInfo = await connection.getAccountInfo(new PublicKey(pool.poolAddress));
      if (!poolAccountInfo?.data) return;

      const rpcUrl = process.env.SOLANA_RPC!;

      // Decode pool + fetch vault balances via RPC
      const decoded = await decodeRaydiumPoolWithVaults(
        Buffer.from(poolAccountInfo.data),
        rpcUrl,
      );
      if (!decoded) return;

      // Get SOL price for USD conversion
      const solPriceRes = await fetch("https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112");
      const solPriceJson: any = await solPriceRes.json();
      const solPrice = solPriceJson["So11111111111111111111111111111111111111112"]?.usdPrice ?? 140;

      // Get pool info for decimals
      const poolInfo = parseRaydiumPoolInfo(Buffer.from(poolAccountInfo.data));
      const tokenDecimals = poolInfo?.coinDecimals ?? 6;

      // Compute price from reserves
      const LAMPORTS_PER_SOL = 1_000_000_000;
      const virtualSolN   = Number(decoded.virtualSolReserves);
      const virtualTokenN = Number(decoded.virtualTokenReserves);
      const priceInSol = virtualTokenN > 0
        ? (virtualSolN / LAMPORTS_PER_SOL) / (virtualTokenN / Math.pow(10, tokenDecimals))
        : 0;

      if (priceInSol <= 0) return;

      const liquiditySol = virtualSolN / LAMPORTS_PER_SOL;
      const priceUsd     = priceInSol * solPrice;

      const update: Record<string, string> = {
        mint,
        platform:         "raydium",
        curvePDA:         pool.poolAddress,
        isGlobal:         "true",
        priceQuote:       priceInSol.toString(),
        priceUsd:         priceUsd.toString(),
        liquidity:        liquiditySol.toFixed(4),
        liquidityUsd:     (liquiditySol * solPrice).toFixed(2),
        marketCapQuote:   "0",
        marketCapUsd:     "0",
        curvePercentage:  "100",
        complete:         "true",
        graduationStatus: "graduated",
      };

      await redis.hset(`token:${mint}`, update);

      // Record first price tick
      await addPriceTick(mint, { time: Date.now(), price: priceInSol, price_usd: priceUsd });
      await calcPriceEvents(mint);
      await redis.publish("token-updates", mint);

      console.log(`[GlobalTracker] Seeded price for ${mint.slice(0, 12)}: ${priceUsd.toFixed(8)} USD`);
    } catch (e: any) {
      console.error(`[GlobalTracker] seedInitialPrice failed for ${mint.slice(0, 12)}:`, e.message);
    }
  }

  private async seedTokenMetadata(mint: string) {
    try {
      // Derive the Metaplex metadata PDA
      const METADATA_PROGRAM = new PublicKey("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
      const [metadataPDA] = PublicKey.findProgramAddressSync(
        [Buffer.from("metadata"), METADATA_PROGRAM.toBuffer(), new PublicKey(mint).toBuffer()],
        METADATA_PROGRAM,
      );

      const accountInfo = await connection.getAccountInfo(metadataPDA);
      if (!accountInfo?.data) return;

      // Metaplex Metadata V1 layout: skip first byte (key), then:
      // [1:33] updateAuthority, [33:65] mint
      // [65:69] name length (u32 LE), [69:69+nameLen] name
      // then symbol length + symbol, then uri length + uri
      const data = accountInfo.data;
      let offset = 65;

      const nameLen = data.readUInt32LE(offset); offset += 4;
      const name = data.slice(offset, offset + nameLen).toString("utf8").replace(/\0/g, "").trim();
      offset += nameLen;

      const symbolLen = data.readUInt32LE(offset); offset += 4;
      const symbol = data.slice(offset, offset + symbolLen).toString("utf8").replace(/\0/g, "").trim();
      offset += symbolLen;

      const uriLen = data.readUInt32LE(offset); offset += 4;
      const uri = data.slice(offset, offset + uriLen).toString("utf8").replace(/\0/g, "").trim();

      if (name || symbol) {
        await redis.hset(`token:${mint}`, {
          name:   name   || "Unknown",
          symbol: symbol || "???",
          uri:    uri    || "",
        });
      }
    } catch (e: any) {
      // Not critical — metadata will just show "Unknown"
      console.warn(`[GlobalTracker] Metadata fetch failed for ${mint.slice(0, 12)}:`, e.message);
    }
  }

  private evictLRU() {
    let oldest: WatchEntry | null = null;
    for (const entry of this.watched.values()) {
      if (!oldest || entry.lastAccess < oldest.lastAccess) {
        oldest = entry;
      }
    }
    if (oldest) {
      console.log(`[GlobalTracker] Evicting LRU: ${oldest.mint.slice(0, 12)} (idle ${Math.round((Date.now() - oldest.lastAccess) / 60000)}m)`);
      this.unwatchToken(oldest.mint).catch(() => {});
    }
  }

  private async cleanup() {
    const now = Date.now();
    const toRemove: string[] = [];
    for (const [mint, entry] of this.watched) {
      if (now - entry.lastAccess > IDLE_TIMEOUT) {
        toRemove.push(mint);
      }
    }
    for (const mint of toRemove) {
      await this.unwatchToken(mint);
    }
    if (toRemove.length > 0) {
      console.log(`[GlobalTracker] Cleanup: removed ${toRemove.length} idle tokens, ${this.watched.size} remaining`);
    }
  }

  private async restoreFromRedis() {
    const mints = await redis.smembers("global:watched");
    if (mints.length === 0) return;

    let restored = 0;
    for (const mint of mints) {
      const poolData = await redis.hgetall(`pool:${mint}`);
      if (!poolData?.poolAddress) {
        await redis.srem("global:watched", mint);
        continue;
      }

      const pool: PoolInfo = {
        poolAddress: poolData.poolAddress,
        poolType:    "raydium-amm-v4",
        programId:   poolData.programId || "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        baseMint:    poolData.baseMint  || mint,
        quoteMint:   poolData.quoteMint || "So11111111111111111111111111111111111111112",
      };

      const lastAccess = parseInt(await redis.get(`global:lastAccess:${mint}`) || "0") || Date.now();

      // Skip if idle too long
      if (Date.now() - lastAccess > IDLE_TIMEOUT) {
        await redis.srem("global:watched", mint);
        continue;
      }

      this.watched.set(mint, { mint, pool, lastAccess });
      this.yellowstone?.addCurvePDA(pool.poolAddress, mint, "raydium", false);
      restored++;
    }

    if (restored > 0) {
      console.log(`[GlobalTracker] Restored ${restored} global token subscriptions from Redis`);
    }
  }

  /**
   * Poll vault balances for all watched Raydium tokens to keep prices fresh.
   * Raydium pool accounts don't store reserves inline — they're in separate
   * SPL token vault accounts. So we batch-read vaults every 10s.
   */
  private async pollAllWatched() {
    if (this.watched.size === 0) return;

    const rpcUrl = process.env.SOLANA_RPC!;
    const entries = [...this.watched.values()];

    // Get SOL price once per poll cycle
    let solPrice = 140;
    try {
      const res = await fetch("https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112");
      const json: any = await res.json();
      solPrice = json["So11111111111111111111111111111111111111112"]?.usdPrice ?? 140;
    } catch {}

    // Process in batches of 20 to avoid overwhelming RPC
    const BATCH_SIZE = 20;
    for (let i = 0; i < entries.length; i += BATCH_SIZE) {
      const batch = entries.slice(i, i + BATCH_SIZE);
      await Promise.allSettled(batch.map(entry => this.pollSingleToken(entry, solPrice, rpcUrl)));
    }
  }

  private async pollSingleToken(entry: WatchEntry, solPrice: number, rpcUrl: string) {
    try {
      const poolAccountInfo = await connection.getAccountInfo(new PublicKey(entry.pool.poolAddress));
      if (!poolAccountInfo?.data) return;

      const decoded = await decodeRaydiumPoolWithVaults(
        Buffer.from(poolAccountInfo.data),
        rpcUrl,
      );
      if (!decoded) return;

      const poolInfo = parseRaydiumPoolInfo(Buffer.from(poolAccountInfo.data));
      const tokenDecimals = poolInfo?.coinDecimals ?? 6;

      const LAMPORTS_PER_SOL = 1_000_000_000;
      const virtualSolN   = Number(decoded.virtualSolReserves);
      const virtualTokenN = Number(decoded.virtualTokenReserves);
      const priceInSol = virtualTokenN > 0
        ? (virtualSolN / LAMPORTS_PER_SOL) / (virtualTokenN / Math.pow(10, tokenDecimals))
        : 0;

      if (priceInSol <= 0) return;

      const liquiditySol = virtualSolN / LAMPORTS_PER_SOL;
      const priceUsd     = priceInSol * solPrice;

      await redis.hset(`token:${entry.mint}`, {
        priceQuote:   priceInSol.toString(),
        priceUsd:     priceUsd.toString(),
        liquidity:    liquiditySol.toFixed(4),
        liquidityUsd: (liquiditySol * solPrice).toFixed(2),
      });

      // Record price tick + recalculate rolling changes
      await addPriceTick(entry.mint, { time: Date.now(), price: priceInSol, price_usd: priceUsd });
      await calcPriceEvents(entry.mint);
      await redis.publish("token-updates", entry.mint);
    } catch {
      // Silent — individual poll failures are expected under RPC pressure
    }
  }

  /**
   * Poll Raydium swap transactions for all watched tokens to populate volume data.
   * Uses getSignaturesForAddress + getParsedTransactions via RPC.
   * Runs every 15s — offset from the 10s price poll to spread RPC load.
   */
  private async pollAllTrades() {
    if (this.watched.size === 0) return;

    const entries = [...this.watched.values()];

    // Process in batches of 10 — each token does 1 getSignatures + up to 4 getParsedTx calls
    const BATCH_SIZE = 10;
    for (let i = 0; i < entries.length; i += BATCH_SIZE) {
      const batch = entries.slice(i, i + BATCH_SIZE);
      await Promise.allSettled(batch.map(async (entry) => {
        try {
          const trades = await pollPoolTrades(entry.pool.poolAddress, entry.mint);
          if (trades.length > 0) {
            await recordGlobalTrades(entry.mint, trades);
          }
        } catch {
          // Silent — individual failures expected
        }
      }));
    }
  }
}

export const globalTracker = new GlobalTracker();
