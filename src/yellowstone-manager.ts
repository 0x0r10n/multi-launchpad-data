// src/yellowstone-manager.ts
import Client from "@triton-one/yellowstone-grpc";
import { SubscribeUpdate, CommitmentLevel } from "@triton-one/yellowstone-grpc";
import bs58 from "bs58";
import { EventEmitter } from "events";
import Redis from "ioredis";
import "dotenv/config";

import { detectParser, resolveMeteoraId, LAUNCHPAD_PROGRAM_IDS } from "./launchpads";
import { bootstrapDevStatsIfNeeded } from "./dev-stats";
import { parseSwap } from "./swap-parser";

// Known quote/stablecoin tokens that appear in postTokenBalances but are never new launch mints
const QUOTE_TOKENS = new Set([
  "So11111111111111111111111111111111111111112", // WSOL
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwPc",  // USDT
  "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB",  // USD1 (World Liberty Fi — LetsBonk pair)
]);

const redis = new Redis(process.env.REDIS_URL!);

// ── Stream budget ─────────────────────────────────────────────────────────────
// 1 stream  → transactions only (all 6 launchpad programs)
// 4 streams → curve accounts only (250 PDAs each = 1,000 total)
// Total = 5 streams, matching the Chainstack plan limit.
const ACCOUNT_STREAM_COUNT   = 4;
const ACCOUNTS_PER_STREAM    = 200; // Geyser server hard cap per subscription
const TOTAL_CURVE_CAPACITY   = ACCOUNT_STREAM_COUNT * ACCOUNTS_PER_STREAM; // 800

export class YellowstoneManager extends EventEmitter {
  // One dedicated stream for launchpad transaction monitoring
  private txStream: any;

  // Four dedicated streams for curve account updates — indexed 0-3
  private accountStreams: any[]                                        = [];
  private accountMaps:   Map<string, { mint: string; platform: string }>[] = [];
  private accountDebounceTimers: (ReturnType<typeof setTimeout> | null)[] = [];

  private reconnecting     = false;
  private reconnectAttempts = 0;
  private msgCount = 0;
  private txCount  = 0;

  // Keepalive interval handles — tracked so they can be cleared on reconnect
  private txKeepaliveInterval: ReturnType<typeof setInterval> | null = null;
  private accountKeepaliveIntervals: (ReturnType<typeof setInterval> | null)[] = [];

  // write_version dedup: skip stale/duplicate account pushes on reconnects (global across all streams)
  private lastWriteVersion = new Map<string, bigint>();

  async start() {
    if (this.reconnecting) return;
    console.log("[Yellowstone] Connecting...");
    const client = new Client(
      process.env.CHAINSTACK_GEYSER_URL!,
      process.env.CHAINSTACK_GEYSER_TOKEN!,
      undefined,
    );

    try {
      await client.connect();
      this.reconnectAttempts = 0;

      // ── 1. Transaction stream ──────────────────────────────────────────────
      this.txStream = await client.subscribe();
      // Attach error/close handlers immediately — before any write — so h2 resets
      // that fire synchronously during subscribe() don't become unhandled rejections.
      this.txStream.on("error", (e: any) => this.handleStreamError(e, "txStream"));
      this.txStream.on("close", () => { console.log("[Yellowstone] txStream closed."); this.reconnect(); });
      this.setupTxStream();
      this.sendTxSubscription();

      // ── 2. Account streams (one per slot in the pool) ──────────────────────
      this.accountStreams = [];
      this.accountMaps   = [];
      this.accountDebounceTimers = [];
      this.accountKeepaliveIntervals = [];
      for (let i = 0; i < ACCOUNT_STREAM_COUNT; i++) {
        if (this.reconnecting) break; // abort if a stream already failed mid-loop
        const stream = await client.subscribe();
        stream.on("error", (e: any) => this.handleStreamError(e, `accountStream[${i}]`));
        stream.on("close", () => { console.log(`[Yellowstone] accountStream[${i}] closed.`); this.reconnect(); });
        this.accountStreams.push(stream);
        this.accountMaps.push(new Map());
        this.accountDebounceTimers.push(null);
        this.accountKeepaliveIntervals.push(null);
        this.setupAccountStream(i);
      }

      console.log(`[Yellowstone] ${1 + ACCOUNT_STREAM_COUNT} streams open (1 tx + ${ACCOUNT_STREAM_COUNT} account, capacity=${TOTAL_CURVE_CAPACITY} PDAs).`);
      await this.restoreCurveSubscriptions();
    } catch (err: any) {
      console.error("[Yellowstone] Connect fail:", err.message);
      this.reconnect();
    }
  }

  // ── Public API ────────────────────────────────────────────────────────────

  private addRoundRobinIdx = 0;

  // immediate=true: send the subscription write right now (used on new launches so Geyser
  // starts watching the curve before the 20ms debounce window expires).
  public addCurvePDA(curvePDA: string, mint: string, platform: string, immediate = false) {
    // Already tracked — just ensure the account map entry is fresh
    for (let i = 0; i < this.accountMaps.length; i++) {
      if (this.accountMaps[i].has(curvePDA)) {
        this.accountMaps[i].set(curvePDA, { mint, platform });
        return; // already subscribed, no re-send needed
      }
    }

    // Prefer streams that still have free slots; when all are full, round-robin
    // so new tokens distribute evenly across streams instead of always hitting stream 0.
    let targetIdx = -1;
    for (let i = 0; i < this.accountMaps.length; i++) {
      if (this.accountMaps[i].size < ACCOUNTS_PER_STREAM) {
        targetIdx = i;
        break;
      }
    }
    if (targetIdx === -1) {
      // All full — round-robin across streams
      this.addRoundRobinIdx = (this.addRoundRobinIdx + 1) % ACCOUNT_STREAM_COUNT;
      targetIdx = this.addRoundRobinIdx;
    }

    const map = this.accountMaps[targetIdx];
    // At capacity: evict oldest entry (Map preserves insertion order)
    if (map.size >= ACCOUNTS_PER_STREAM) {
      const oldest = map.keys().next().value;
      if (oldest) map.delete(oldest);
    }
    map.set(curvePDA, { mint, platform });

    if (immediate) {
      // Cancel any pending debounce so this write goes out immediately
      if (this.accountDebounceTimers[targetIdx]) {
        clearTimeout(this.accountDebounceTimers[targetIdx]!);
        this.accountDebounceTimers[targetIdx] = null;
      }
      this.sendAccountSubscription(targetIdx);
    } else {
      this.scheduleAccountUpdate(targetIdx);
    }
  }

  public removeCurvePDA(curvePDA: string) {
    for (let i = 0; i < this.accountMaps.length; i++) {
      if (this.accountMaps[i].has(curvePDA)) {
        this.accountMaps[i].delete(curvePDA);
        this.scheduleAccountUpdate(i);
        return;
      }
    }
  }

  // ── Subscription writers ──────────────────────────────────────────────────

  // Transaction stream: watches all launchpad programs, no account filter
  private sendTxSubscription(includePing = false) {
    if (!this.txStream || this.reconnecting) return;
    const req: any = {
      accounts: {},
      slots: {},
      transactions: {
        launchpads: {
          vote: false,
          failed: false,
          accountInclude: LAUNCHPAD_PROGRAM_IDS,
          accountExclude: [],
          accountRequired: [],
        },
      },
      transactionsStatus: {},
      entry: {},
      blocks: {},
      blocksMeta: {},
      commitment: CommitmentLevel.PROCESSED,
      // accountsDataSlice must be present even on the tx stream (proto requires the field)
      accountsDataSlice: [],
      ...(includePing ? { ping: { id: 1 } } : {}),
    };
    try {
      this.txStream.write(req);
    } catch (err: any) {
      console.error("[Yellowstone] txStream.write failed:", err?.message || err);
    }
  }

  // Account stream i: watches its slice of curve PDAs, no transaction filter
  private sendAccountSubscription(idx: number, includePing = false) {
    const stream = this.accountStreams[idx];
    if (!stream || this.reconnecting) return;
    const pdas = [...this.accountMaps[idx].keys()];
    const req: any = {
      // Omit account label entirely when empty — empty account list = "subscribe to ALL" which is forbidden
      accounts: pdas.length > 0
        ? { "curve-pdas": { account: pdas, owner: [], filters: [] } }
        : {},
      slots: {},
      transactions: {},
      transactionsStatus: {},
      entry: {},
      blocks: {},
      blocksMeta: {},
      commitment: CommitmentLevel.PROCESSED,
      // 320 bytes covers the deepest parser read:
      //   Meteora DBC VirtualPool: is_migrated @ 305, quote_reserve @ 240
      //   LaunchLab VirtualPool:   totalFundRaisingB @ 69
      //   Pump.fun BondingCurve:   complete @ 48
      accountsDataSlice: pdas.length > 0 ? [{ offset: "0", length: "320" }] : [],
      ...(includePing ? { ping: { id: 1 } } : {}),
    };
    try {
      stream.write(req);
      if (!includePing) {
        console.log(`[Yellowstone] Account stream ${idx}: ${pdas.length} PDAs`);
      }
    } catch (err: any) {
      console.error(`[Yellowstone] accountStream[${idx}].write failed:`, err?.message || err);
    }
  }

  // ── Debounced account update scheduling ──────────────────────────────────

  private scheduleAccountUpdate(idx: number) {
    if (this.accountDebounceTimers[idx]) clearTimeout(this.accountDebounceTimers[idx]!);
    // 20ms debounce — batches burst launches into a single subscription write
    this.accountDebounceTimers[idx] = setTimeout(() => {
      this.accountDebounceTimers[idx] = null;
      this.sendAccountSubscription(idx);
    }, 20);
  }

  // ── Restore subscriptions from Redis on startup / reconnect ──────────────

  private async restoreCurveSubscriptions() {
    try {
      // Load up to TOTAL_CURVE_CAPACITY most recent non-graduated tokens
      const mints = await redis.zrevrange("tokens:latest", 0, TOTAL_CURVE_CAPACITY - 1);
      if (!mints.length) {
        // Still need to push empty account subscriptions so the streams are "open"
        for (let i = 0; i < ACCOUNT_STREAM_COUNT; i++) this.sendAccountSubscription(i);
        return;
      }

      const pipeline = redis.pipeline();
      for (const m of mints) pipeline.hmget(`token:${m}`, "curvePDA", "platform", "complete");
      const results = await pipeline.exec();

      for (let i = 0; i < mints.length; i++) {
        const [pda, platform, complete] = (results?.[i]?.[1] as string[]) || [];
        if (pda && platform && complete !== "true") {
          this.addCurvePDA(pda, mints[i], platform);
        }
      }

      // Flush all account streams (addCurvePDA schedules debounced writes; flush now)
      for (let i = 0; i < ACCOUNT_STREAM_COUNT; i++) {
        if (this.accountDebounceTimers[i]) {
          clearTimeout(this.accountDebounceTimers[i]!);
          this.accountDebounceTimers[i] = null;
        }
        this.sendAccountSubscription(i);
      }

      const total = this.accountMaps.reduce((s, m) => s + m.size, 0);
      console.log(`[Yellowstone] Restored ${total} curve subscriptions across ${ACCOUNT_STREAM_COUNT} streams.`);
    } catch (e: any) {
      console.error("[Yellowstone] Failed to restore curve subs:", e.message);
    }
  }

  // ── Stream setup ──────────────────────────────────────────────────────────

  private setupTxStream() {
    // Keepalive every 10s — clear any previous interval first to prevent accumulation
    if (this.txKeepaliveInterval) clearInterval(this.txKeepaliveInterval);
    this.txKeepaliveInterval = setInterval(() => {
      if (this.txStream && !this.reconnecting) this.sendTxSubscription(true);
    }, 10_000);

    // Data handler only — error/close handlers registered in start() before any write
    this.txStream.on("data", (u: SubscribeUpdate) => {
      this.msgCount++;
      if ((u as any).pong) return;
      if (this.msgCount % 200 === 0) {
        console.log(`[Yellowstone] #${this.msgCount} | txs=${this.txCount}`);
      }
      if (u.transaction) {
        this.txCount++;
        this.processTx(u.transaction).catch(() => {});
      }
    });
  }

  private setupAccountStream(idx: number) {
    // Keepalive every 10s — clear any previous interval first to prevent accumulation
    if (this.accountKeepaliveIntervals[idx]) clearInterval(this.accountKeepaliveIntervals[idx]!);
    this.accountKeepaliveIntervals[idx] = setInterval(() => {
      if (this.accountStreams[idx] && !this.reconnecting) {
        this.sendAccountSubscription(idx, true);
      }
    }, 10_000);

    // Data handler only — error/close handlers registered in start() before any write
    this.accountStreams[idx].on("data", (u: SubscribeUpdate) => {
      if ((u as any).pong) return;
      if ((u as any).account) {
        this.processAccountUpdate((u as any).account).catch(() => {});
      }
    });
  }

  private handleStreamError(e: any, label: string) {
    const code = e?.code ?? e?.status ?? "unknown";
    console.error(`[Yellowstone] ${label} error (code=${code}):`, e.message);
    if (code === 16 || code === 7) {
      console.error("[Yellowstone] ⛔ Auth error — check CHAINSTACK_GEYSER_TOKEN. Not reconnecting.");
      return;
    }
    this.handleReconnect();
  }

  // ── Reconnection ──────────────────────────────────────────────────────────

  // Public so the global unhandledRejection handler can call it safely — it
  // tears down all streams, clears keepalive intervals, then reconnects with
  // exponential backoff. Calling reconnect() when already reconnecting is safe.
  public reconnect() {
    if (this.reconnecting) return;
    this.reconnecting = true;

    // Clear keepalive intervals before destroying streams to prevent stale writes
    if (this.txKeepaliveInterval) { clearInterval(this.txKeepaliveInterval); this.txKeepaliveInterval = null; }
    for (const iv of this.accountKeepaliveIntervals) { if (iv) clearInterval(iv); }
    this.accountKeepaliveIntervals = [];

    // Destroy streams so Chainstack releases concurrent slot counts immediately.
    // Without this, old h2 streams linger until TCP teardown and the reconnect's
    // 5 new streams push us over the plan limit → "resource exhausted" cascade.
    try { this.txStream?.destroy(); } catch {}
    for (const s of this.accountStreams) { try { s?.destroy(); } catch {} }
    this.txStream = null;
    this.accountStreams = [];

    // Exponential backoff: 5s, 10s, 20s, 40s … capped at 60s
    const delay = Math.min(5000 * Math.pow(2, this.reconnectAttempts), 60_000);
    this.reconnectAttempts++;
    console.log(`[Yellowstone] Reconnecting in ${delay / 1000}s (attempt ${this.reconnectAttempts})...`);
    setTimeout(() => { this.reconnecting = false; this.start(); }, delay);
  }

  private handleReconnect() { this.reconnect(); }

  // ── Geyser account update handler ────────────────────────────────────────

  private async processAccountUpdate(wrapper: any) {
    if (!wrapper?.account?.pubkey) return;
    const pubkey = bs58.encode(wrapper.account.pubkey);

    // Find which map this PDA belongs to
    let curveInfo: { mint: string; platform: string } | undefined;
    for (const map of this.accountMaps) {
      curveInfo = map.get(pubkey);
      if (curveInfo) break;
    }
    if (!curveInfo) return;

    // write_version deduplication — skip stale or duplicate pushes (common on reconnects)
    const wv = BigInt(wrapper.account.writeVersion ?? 0);
    if (wv > 0n) {
      const last = this.lastWriteVersion.get(pubkey) ?? 0n;
      if (wv <= last) return;
      this.lastWriteVersion.set(pubkey, wv);
      // Evict oldest entries when the map exceeds total curve capacity to prevent unbounded growth
      if (this.lastWriteVersion.size > TOTAL_CURVE_CAPACITY + 200) {
        const oldest = this.lastWriteVersion.keys().next().value;
        if (oldest) this.lastWriteVersion.delete(oldest);
      }
    }

    const data = Buffer.from(wrapper.account.data);
    if (data.length < 8) return;

    // txnSignature is null/empty for Geyser's initial subscription snapshot (current account
    // state sent immediately on subscribe). A non-empty signature means a tx triggered it.
    const isStartup = !wrapper.account.txnSignature || wrapper.account.txnSignature.length === 0;

    this.emit("curve-update", {
      curvePDA: pubkey,
      mint: curveInfo.mint,
      platform: curveInfo.platform,
      data,
      isStartup,
    });
  }

  // ── Transaction processing (unchanged) ───────────────────────────────────

  private async processTx(wrapper: any) {
    const txInfo  = wrapper.transaction;
    if (!txInfo) return;

    const signature = txInfo.signature ? bs58.encode(txInfo.signature) : "unknown";
    const meta      = txInfo.meta || {};
    const message   = txInfo.transaction?.message;
    const logs: string[] = meta.logMessages || [];

    const parser = detectParser(meta, message);
    if (!parser) return;

    const mint = this.extractMint(meta);
    if (!mint) return;

    const platformId = parser.id === "meteora"
      ? resolveMeteoraId(mint)
      : parser.id;

    const maker = message?.accountKeys?.[0]
      ? bs58.encode(message.accountKeys[0])
      : "unknown";

    // ── Create ────────────────────────────────────────────────────────────
    if (parser.isCreate(logs, message, meta)) {
      if (signature !== "unknown") {
        const isNew = await redis.set(`dedup:${signature}`, "1", "EX", 120, "NX");
        if (!isNew) return;
      }

      const parsed = parser.parseMetadata(logs, message, meta);
      if (parser.strictMetadata && (!parsed.name || !parsed.symbol)) return;

      const curvePDA = parser.deriveCurvePDA(mint, message, meta);
      if (!curvePDA) {
        console.error(`[CRITICAL] No curvePDA for mint=${mint.slice(0, 12)} platform=${platformId} — skipping rich path`);
        const tokenDataSkeleton: Record<string, string> = {
          mint,
          creator: maker,
          curvePDA: "",
          platform: platformId,
          name: parsed.name || "",
          symbol: parsed.symbol || "",
          uri: parsed.uri || "",
          createdAt: Date.now().toString(),
          createdTx: signature,
          slot: wrapper.slot?.toString() || "0",
          dataQuality: "skeleton"
        };
        this.emit("new-launch", { ...tokenDataSkeleton, signature });
        return;
      }

      console.log(`[PDA Debug] platform=${platformId} mint=${mint.slice(0, 12)} pda=${curvePDA.slice(0, 12)}`);
      const slot     = wrapper.slot?.toString() || "0";

      console.log(`[Yellowstone] 🚀 NEW (${platformId}): ${parsed.name || ""} ($${parsed.symbol || ""}) | ${mint.slice(0, 12)} | sig=${signature.slice(0, 8)}`);

      const tokenData: Record<string, string> = {
        mint,
        creator:           maker,
        curvePDA,
        platform:          platformId,
        name:              parsed.name   || "",
        symbol:            parsed.symbol || "",
        uri:               parsed.uri    || "",
        isMayhemMode:      parsed.isMayhemMode      ? "true" : "false",
        isCashbackEnabled: parsed.isCashbackEnabled ? "true" : "false",
        decimals:          "6",
        createdAt:         Date.now().toString(),
        createdTx:         signature,
        slot,
      };

      this.emit("new-launch", { ...tokenData, signature });
      Promise.all([
        redis.hset(`token:${mint}`, tokenData),
        redis.zadd("tokens:latest", Date.now(), mint),
        redis.hincrby(`creator:${maker}`, "launched", 1),
      ]).catch(() => {});
      bootstrapDevStatsIfNeeded(maker);
      return;
    }

    // ── Migration ─────────────────────────────────────────────────────────
    if (this.isMigrationTx(logs, platformId)) {
      const exists = await redis.exists(`token:${mint}`);
      if (exists) {
        const tokenData = await redis.hgetall(`token:${mint}`);
        if (tokenData && tokenData.complete !== "true") {
          await redis.hset(`token:${mint}`, { complete: "true", graduationStatus: "graduated" });
          if (tokenData.creator) {
            redis.hincrby(`creator:${tokenData.creator}`, "migrated", 1).catch(() => {});
          }
          if (tokenData.curvePDA) this.removeCurvePDA(tokenData.curvePDA);
          await redis.publish("token-updates", mint);
          console.log(`[Yellowstone] 🎓 GRADUATED (${platformId}): ${mint.slice(0, 12)}`);
        }
      }
      return;
    }

    // ── Swap ──────────────────────────────────────────────────────────────
    const swapType = parser.detectSwap(logs, meta);
    if (!swapType) return;

    const exists = await redis.exists(`token:${mint}`);
    if (!exists) return;

    const solAmount = this.calcSolDelta(meta);
    const now = Date.now();

    const pipe = redis.pipeline();
    if (swapType === "buy") pipe.hincrby(`token:${mint}`, "buys",  1);
    else                     pipe.hincrby(`token:${mint}`, "sells", 1);
    pipe.hincrby(`token:${mint}`, "totalTxns", 1);
    pipe.hincrbyfloat(`token:${mint}`, "volume", solAmount);
    pipe.zadd(`trades:${mint}`, now, `${swapType}:${solAmount}:${now}`);
    pipe.zremrangebyscore(`trades:${mint}`, 0, now - 86_400_000);
    await pipe.exec();

    this.emit("trade", { mint, signature, type: swapType, solAmount, maker, slot: wrapper.slot });

    // Fire-and-forget: precise swap extraction runs after the hot path completes.
    // Uses directional token-delta logic (buy = max-positive, sell = max-negative ATA)
    // which correctly handles ALTs, inner CPIs, and versioned transactions.
    setImmediate(() => {
      try {
        const swap = parseSwap(wrapper, mint, swapType, maker);
        this.emit("swap", swap);
      } catch (e: any) {
        console.error("[SwapParser] error:", e.message);
      }
    });
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private isMigrationTx(logs: string[], platformId: string): boolean {
    if (platformId === "pump") {
      return logs.some(l => l.includes("Instruction: Migrate"));
    }
    if (platformId === "launchlab" || platformId === "letsbonk") {
      return logs.some(l =>
        l.includes("MigrateToAmm") ||
        l.includes("migrate_to_amm") ||
        l.includes("Instruction: MigrateFunds") ||
        l.includes("Instruction: Migrate"),
      );
    }
    return false;
  }

  private extractMint(meta: any): string {
    for (const b of (meta?.postTokenBalances || [])) {
      if (b.mint && !QUOTE_TOKENS.has(b.mint)) return b.mint;
    }
    return "";
  }

  private calcSolDelta(meta: any): number {
    const pre  = meta?.preBalances;
    const post = meta?.postBalances;
    if (!pre || !post || pre.length === 0) return 0;
    const fee   = Number(meta?.fee ?? 5000);
    const delta = Math.abs(Number(post[0]) - Number(pre[0]));
    return Math.max(0, (delta - fee) / 1_000_000_000);
  }
}
