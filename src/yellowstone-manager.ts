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
      this.subscribeToLaunchpads();
    } catch (err: any) {
      console.error("[Yellowstone] Connect fail:", err.message);
      this.handleReconnect();
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

  private subscribeToLaunchpads() {
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
      accountsDataSlice: [],
    };
    this.stream.write(req);
    console.log(`[Yellowstone] Subscribed to ${LAUNCHPAD_PROGRAM_IDS.length} launchpad program IDs.`);
  }

  private handleUpdate(u: SubscribeUpdate) {
    if (this.msgCount % 200 === 0) {
      console.log(`[Yellowstone] #${this.msgCount} | txs=${this.txCount}`);
    }
    if (u.transaction) {
      this.txCount++;
      this.processTx(u.transaction).catch(() => {});
    }
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
    // detectParser() returns MeteoraParser (id="meteora") for all Meteora DBC txs.
    // Symbol isn't known yet at this point — resolveMeteoraId is called again after
    // parseMetadata() if needed, passing the parsed symbol and logs for fuller detection.
    const platformId = parser.id === "meteora"
      ? resolveMeteoraId(mint)
      : parser.id;

    const maker = message?.accountKeys?.[0]
      ? bs58.encode(message.accountKeys[0])
      : "unknown";

    // ── Create detection ───────────────────────────────────────────────────────
    if (parser.isCreate(logs, message, meta)) {
      const parsed = parser.parseMetadata(logs, message, meta);

      // Strict platforms (Pump.fun) require name+symbol in the tx itself
      if (parser.strictMetadata && (!parsed.name || !parsed.symbol)) return;

      // Store raw parsed values (empty string if absent) — the payload builder applies
      // display defaults ("Unknown Token" / "???"), and the enricher checks !existing.name
      // to detect missing metadata. Storing "Unknown Token" would block that patch.
      const name   = parsed.name   || "";
      const symbol = parsed.symbol || "";

      // Re-resolve Meteora label now that we have the parsed symbol
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
      // Persist in background (both writes in parallel, non-blocking)
      Promise.all([
        redis.hset(`token:${mint}`, tokenData),
        redis.zadd("tokens:latest", Date.now(), mint),
      ]).catch(() => {});
      return; // A create tx is never also a swap
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
