// src/yellowstone-manager.ts
import Client from "@triton-one/yellowstone-grpc";
import { SubscribeUpdate, CommitmentLevel } from "@triton-one/yellowstone-grpc";
import bs58 from "bs58";
import { EventEmitter } from "events";
import Redis from "ioredis";
import "dotenv/config";

import { detectParser, resolveMeteoraId, LAUNCHPAD_PROGRAM_IDS, MOON_EXCLUDE_WALLETS } from "./launchpads";

// Known quote/stablecoin tokens that appear in postTokenBalances but are never new launch mints
const QUOTE_TOKENS = new Set([
  "So11111111111111111111111111111111111111112", // WSOL
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwPc",  // USDT
  "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB",  // USD1 (World Liberty Fi — LetsBonk pair)
]);

const redis = new Redis(process.env.REDIS_URL!);

export class YellowstoneManager extends EventEmitter {
  private stream: any;
  private reconnecting = false;
  private msgCount = 0;
  private txCount  = 0;

  // curvePDA (base58) → { mint, platform } — drives Geyser account subscriptions
  private curveAccounts = new Map<string, { mint: string; platform: string }>();
  private subDebounceTimer: ReturnType<typeof setTimeout> | null = null;

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
      this.stream = await client.subscribe();
      console.log("[Yellowstone] Stream opened.");
      this.setupStream();
      // Immediate write: subscribe to launchpad transactions right away (with 0 PDAs).
      // restoreCurveSubscriptions will call sendFullSubscription again once PDAs are loaded,
      // which updates the accounts filter. Two writes on startup is fine and correct.
      this.sendFullSubscription();
      this.restoreCurveSubscriptions().catch(() => {});
    } catch (err: any) {
      console.error("[Yellowstone] Connect fail:", err.message);
      this.handleReconnect();
    }
  }

  // ── Public API for managing curve PDA subscriptions ─────────────────────────

  public addCurvePDA(curvePDA: string, mint: string, platform: string) {
    // Chainstack limit: 50 accounts per stream subscription.
    // When at capacity, evict the oldest entry (Map preserves insertion order).
    if (!this.curveAccounts.has(curvePDA) && this.curveAccounts.size >= 50) {
      const oldest = this.curveAccounts.keys().next().value;
      if (oldest) this.curveAccounts.delete(oldest);
    }
    this.curveAccounts.set(curvePDA, { mint, platform });
    this.scheduleSubscriptionUpdate();
  }

  public removeCurvePDA(curvePDA: string) {
    this.curveAccounts.delete(curvePDA);
    this.scheduleSubscriptionUpdate();
  }

  // ── Private subscription management ─────────────────────────────────────────

  private scheduleSubscriptionUpdate() {
    if (this.subDebounceTimer) clearTimeout(this.subDebounceTimer);
    // 20ms debounce — batches rapid launches while minimizing Geyser registration lag
    this.subDebounceTimer = setTimeout(() => {
      this.subDebounceTimer = null;
      this.sendFullSubscription();
    }, 20);
  }

  // ── Single combined subscription write ───────────────────────────────────────
  // CRITICAL: every stream.write() is a full replacement of that subscription type.
  // An empty transactions:{} wipes the launchpad tx subscription. We must include
  // BOTH the transaction filter AND the account filter in every write.
  private sendFullSubscription() {
    if (!this.stream || this.reconnecting) return;
    const pdas = [...this.curveAccounts.keys()];
    const req: any = {
      // Empty account list with no owner/filters = "subscribe to ALL accounts" → not allowed.
      // When there are no PDAs to track, omit the curve-pdas label entirely (accounts: {}).
      accounts: pdas.length > 0
        ? { "curve-pdas": { account: pdas, owner: [], filters: [] } }
        : {},
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
      accountsDataSlice: [],
    };
    try {
      this.stream.write(req);
      console.log(`[Yellowstone] Subscription updated: ${LAUNCHPAD_PROGRAM_IDS.length} programs + ${pdas.length} curve PDAs`);
    } catch (_) {}
  }

  // On reconnect, restore curve subscriptions from Redis for non-graduated tokens
  private async restoreCurveSubscriptions() {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 49); // max 50 — Chainstack account sub limit
      if (!mints.length) return;
      const pipeline = redis.pipeline();
      for (const m of mints) pipeline.hmget(`token:${m}`, "curvePDA", "platform", "complete");
      const results = await pipeline.exec();
      for (let i = 0; i < mints.length; i++) {
        const [pda, platform, complete] = (results?.[i]?.[1] as string[]) || [];
        if (pda && platform && complete !== "true") {
          this.curveAccounts.set(pda, { mint: mints[i], platform });
        }
      }
      // Always call sendFullSubscription — includes both launchpad tx filter + restored PDAs
      this.sendFullSubscription();
      console.log(`[Yellowstone] Restored ${this.curveAccounts.size} curve subscriptions.`);
    } catch (e: any) {
      console.error("[Yellowstone] Failed to restore curve subs:", e.message);
    }
  }

  private handleReconnect() {
    if (this.reconnecting) return;
    this.reconnecting = true;
    console.log("[Yellowstone] Reconnecting in 5s...");
    setTimeout(() => { this.reconnecting = false; this.start(); }, 5000);
  }

  private setupStream() {
    const pingReq: any = {
      ping: { id: 1 },
      accounts: {}, slots: {}, transactions: {}, transactionsStatus: {},
      blocks: {}, blocksMeta: {}, entry: {}, accountsDataSlice: [],
    };
    setInterval(() => {
      if (this.stream && !this.reconnecting) {
        try { this.stream.write(pingReq); } catch (_) {}
      }
    }, 10_000);

    this.stream.on("data",  (u: SubscribeUpdate) => {
      this.msgCount++;
      if ((u as any).pong) return;
      this.handleUpdate(u);
    });
    this.stream.on("error", (e: any) => {
      console.error("[Yellowstone] Error:", e.message);
      this.handleReconnect();
    });
    this.stream.on("close", () => {
      console.log("[Yellowstone] Stream closed.");
      this.handleReconnect();
    });
  }

  private handleUpdate(u: SubscribeUpdate) {
    if (this.msgCount % 200 === 0) {
      console.log(`[Yellowstone] #${this.msgCount} | txs=${this.txCount}`);
    }
    if (u.transaction) {
      this.txCount++;
      this.processTx(u.transaction).catch(() => {});
    }
    if ((u as any).account) {
      this.processAccountUpdate((u as any).account).catch(() => {});
    }
  }

  // ── Geyser account update handler ───────────────────────────────────────────
  private async processAccountUpdate(wrapper: any) {
    if (!wrapper?.account?.pubkey) return;
    const pubkey = bs58.encode(wrapper.account.pubkey);
    const curveInfo = this.curveAccounts.get(pubkey);
    if (!curveInfo) return;

    const data = Buffer.from(wrapper.account.data);
    if (data.length < 8) return; // Too short to be valid curve data

    this.emit("curve-update", {
      curvePDA: pubkey,
      mint: curveInfo.mint,
      platform: curveInfo.platform,
      data,
    });
  }

  private async processTx(wrapper: any) {
    const txInfo  = wrapper.transaction;
    if (!txInfo) return;

    const signature = txInfo.signature ? bs58.encode(txInfo.signature) : "unknown";
    const meta      = txInfo.meta || {};
    const message   = txInfo.transaction?.message;
    const logs: string[] = meta.logMessages || [];

    // ── Platform detection via registry ───────────────────────────────────────
    const parser = detectParser(meta, message);
    if (!parser) return;

    // ── Mint extraction (universal — reads postTokenBalances) ─────────────────
    const mint = this.extractMint(meta);
    if (!mint) return;

    // ── Meteora DBC label resolution (bags vs meteora) ─────────────────────────
    const platformId = parser.id === "meteora"
      ? resolveMeteoraId(mint)
      : parser.id;

    const maker = message?.accountKeys?.[0]
      ? bs58.encode(message.accountKeys[0])
      : "unknown";

    // ── Create detection ───────────────────────────────────────────────────────
    if (parser.isCreate(logs, message, meta)) {
      const parsed = parser.parseMetadata(logs, message, meta);

      if (parser.strictMetadata && (!parsed.name || !parsed.symbol)) return;

      const name   = parsed.name   || "";
      const symbol = parsed.symbol || "";
      const finalPlatformId = platformId;
      const curvePDA = parser.deriveCurvePDA(mint);
      const slot     = wrapper.slot?.toString() || "0";

      console.log(`[Yellowstone] 🚀 NEW (${finalPlatformId}): ${name} ($${symbol}) | ${mint.slice(0, 12)} | sig=${signature.slice(0, 8)}`);

      const tokenData: Record<string, string> = {
        mint,
        creator:   maker,
        curvePDA,
        platform:  finalPlatformId,
        name,
        symbol,
        uri:       parsed.uri || "",
        decimals:  "6",
        createdAt: Date.now().toString(),
        createdTx: signature,
        slot,
      };

      // Emit first — index.ts handler starts immediately without waiting for Redis
      this.emit("new-launch", { ...tokenData, signature });
      // Persist core token data + increment creator launch counter in parallel
      Promise.all([
        redis.hset(`token:${mint}`, tokenData),
        redis.zadd("tokens:latest", Date.now(), mint),
        redis.hincrby(`creator:${maker}`, "launched", 1),
      ]).catch(() => {});
      return; // A create tx is never also a swap
    }

    // ── Migration / graduation detection ──────────────────────────────────────
    // Belt-and-suspenders: Geyser curve account updates already detect completion via
    // the complete flag, but migration transactions may emit distinct signals before
    // (or instead of) an account update arriving. Marking complete here ensures the
    // graduation is reflected even if the curve account is zeroed/deleted on migration.
    if (this.isMigrationTx(logs, parser.id)) {
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
          console.log(`[Yellowstone] 🎓 GRADUATED (${parser.id}): ${mint.slice(0, 12)}`);
        }
      }
      return; // migration is not also a swap
    }

    // ── Swap detection ─────────────────────────────────────────────────────────
    const swapType = parser.detectSwap(logs, meta);
    if (!swapType) return;

    const exists = await redis.exists(`token:${mint}`);
    if (!exists) return; // Only track tokens we've indexed

    const solAmount = this.calcSolDelta(meta);
    const now = Date.now();

    // Pipeline all 6 counter writes into a single Redis round-trip
    const pipe = redis.pipeline();
    if (swapType === "buy") pipe.hincrby(`token:${mint}`, "buys",  1);
    else                     pipe.hincrby(`token:${mint}`, "sells", 1);
    pipe.hincrby(`token:${mint}`, "totalTxns", 1);
    pipe.hincrbyfloat(`token:${mint}`, "volume", solAmount);
    pipe.zadd(`trades:${mint}`, now, `${swapType}:${solAmount}:${now}`);
    pipe.zremrangebyscore(`trades:${mint}`, 0, now - 86_400_000);
    await pipe.exec();

    this.emit("trade", { mint, signature, type: swapType, solAmount, maker });
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  // ── Migration signal detection ───────────────────────────────────────────────
  // Returns true if the transaction looks like a bonding-curve → DEX migration.
  // Uses platform-specific log signals to minimize false positives.
  // Pump.fun:   "Instruction: Migrate" within the pump program context
  // LaunchLab:  "migrate" or "MigrateToAmm" in LaunchLab program logs
  // Moon.it:    graduation happens via curve fill, not a separate migrate tx
  // Meteora:    graduation happens via curve fill, not a separate migrate tx
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
    // Moon/Bags/Meteora: graduation is detected via curvePercentage in account updates
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
    const delta = Math.abs(Number(post[0]) - Number(pre[0]));
    return Math.max(0, (delta - 5000) / 1_000_000_000);
  }
}
