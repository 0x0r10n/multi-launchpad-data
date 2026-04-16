// src/yellowstone-manager.ts
import Client from "@triton-one/yellowstone-grpc";
import { SubscribeUpdate, CommitmentLevel } from "@triton-one/yellowstone-grpc";
import bs58 from "bs58";
import { EventEmitter } from "events";
import Redis from "ioredis";
import "dotenv/config";

import { detectParser, resolveMeteoraId, LAUNCHPAD_PROGRAM_IDS } from "./launchpads";
import { bootstrapDevStatsIfNeeded } from "./dev-stats";

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
      this.txStream.on("error", (e: any) => this.handleStreamError(e, "txStream"));
      this.txStream.on("close", () => { console.log("[Yellowstone] txStream closed."); this.handleReconnect(); });
      this.setupTxStream();
      this.sendTxSubscription();

      // ── 2. Account streams (one per slot in the pool) ──────────────────────
      this.accountStreams = [];
      this.accountMaps   = [];
      this.accountDebounceTimers = [];
      for (let i = 0; i < ACCOUNT_STREAM_COUNT; i++) {
        const stream = await client.subscribe();
        // Attach error/close handlers immediately — before any write — so h2 resets
        // that fire synchronously during subscribe() don't become unhandled rejections.
        stream.on("error", (e: any) => this.handleStreamError(e, `accountStream[${i}]`));
        stream.on("close", () => { console.log(`[Yellowstone] accountStream[${i}] closed.`); this.handleReconnect(); });
        this.accountStreams.push(stream);
        this.accountMaps.push(new Map());
        this.accountDebounceTimers.push(null);
        this.setupAccountStream(i);
      }

      console.log(`[Yellowstone] ${1 + ACCOUNT_STREAM_COUNT} streams open (1 tx + ${ACCOUNT_STREAM_COUNT} account, capacity=${TOTAL_CURVE_CAPACITY} PDAs).`);
      await this.restoreCurveSubscriptions();
    } catch (err: any) {
      console.error("[Yellowstone] Connect fail:", err.message);
      this.handleReconnect();
    }
  }

  // ── Public API ────────────────────────────────────────────────────────────

  private addRoundRobinIdx = 0;

  public addCurvePDA(curvePDA: string, mint: string, platform: string) {
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
    this.scheduleAccountUpdate(targetIdx);
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
      // Only request the first 50 bytes — all parsers read ≤ 48 bytes (~4× bandwidth reduction)
      accountsDataSlice: pdas.length > 0 ? [{ offset: "0", length: "50" }] : [],
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
    // Keepalive every 10s — piggybacks on the subscription write to avoid wiping state
    setInterval(() => {
      if (this.txStream && !this.reconnecting) this.sendTxSubscription(true);
    }, 10_000);

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

    this.txStream.on("error", (e: any) => this.handleStreamError(e, "txStream"));
    this.txStream.on("close", () => {
      console.log("[Yellowstone] txStream closed.");
      this.handleReconnect();
    });
  }

  private setupAccountStream(idx: number) {
    // Keepalive every 10s — must ping each account stream independently
    setInterval(() => {
      if (this.accountStreams[idx] && !this.reconnecting) {
        this.sendAccountSubscription(idx, true);
      }
    }, 10_000);

    this.accountStreams[idx].on("data", (u: SubscribeUpdate) => {
      if ((u as any).pong) return;
      if ((u as any).account) {
        this.processAccountUpdate((u as any).account).catch(() => {});
      }
    });

    this.accountStreams[idx].on("error", (e: any) => this.handleStreamError(e, `accountStream[${idx}]`));
    this.accountStreams[idx].on("close", () => {
      console.log(`[Yellowstone] accountStream[${idx}] closed.`);
      this.handleReconnect();
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

  private handleReconnect() {
    if (this.reconnecting) return;
    this.reconnecting = true;
    // Exponential backoff: 5s, 10s, 20s, 40s … capped at 60s
    const delay = Math.min(5000 * Math.pow(2, this.reconnectAttempts), 60_000);
    this.reconnectAttempts++;
    console.log(`[Yellowstone] Reconnecting in ${delay / 1000}s (attempt ${this.reconnectAttempts})...`);
    // Clear stale stream references so setup methods don't fire on dead objects
    this.txStream = null;
    this.accountStreams = [];
    setTimeout(() => { this.reconnecting = false; this.start(); }, delay);
  }

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
    }

    const data = Buffer.from(wrapper.account.data);
    if (data.length < 8) return;

    this.emit("curve-update", {
      curvePDA: pubkey,
      mint: curveInfo.mint,
      platform: curveInfo.platform,
      data,
      isStartup: !!(wrapper.isStartup),
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

      const curvePDA = parser.deriveCurvePDA(mint);
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
