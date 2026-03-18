// src/yellowstone-manager.ts — FIXED for correct Yellowstone gRPC nesting
import Client from "@triton-one/yellowstone-grpc";
import { SubscribeUpdate, CommitmentLevel } from "@triton-one/yellowstone-grpc";
import { PublicKey } from "@solana/web3.js";
import bs58 from "bs58";
import { EventEmitter } from "events";
import Redis from "ioredis";
import "dotenv/config";

const PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const redis = new Redis(process.env.REDIS_URL!);

export class YellowstoneManager extends EventEmitter {
  private stream: any;
  private reconnecting = false;
  private msgCount = 0;
  private txCount = 0;

  async start() {
    if (this.reconnecting) return;
    console.log("[Yellowstone] Connecting...");
    const client = new Client(
      process.env.CHAINSTACK_GEYSER_URL!,
      process.env.CHAINSTACK_GEYSER_TOKEN!,
      undefined // 3rd arg required by this library version
    );

    try {
      await client.connect();
      this.stream = await client.subscribe();
      console.log("[Yellowstone] Stream opened.");
      this.setupStream();
      this.subscribeToPumpFun();
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
    // Keep-alive ping (must include all required fields)
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

    this.stream.on("data", (u: SubscribeUpdate) => {
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

  private subscribeToPumpFun() {
    const req: any = {
      accounts: {},
      slots: {},
      transactions: {
        pump: {
          vote: false,
          failed: false,
          accountInclude: [PUMP_FUN],
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
    console.log("[Yellowstone] Subscribed to Pump.fun (PROCESSED).");
  }

  // ===== Yellowstone gRPC structure =====
  //
  // SubscribeUpdate.transaction (what we receive as `wrapper`) is:
  //   { transaction: { signature, isVote, transaction: { message }, meta, index }, slot }
  //
  // So: signature  = wrapper.transaction.signature
  //     meta       = wrapper.transaction.meta
  //     message    = wrapper.transaction.transaction.message

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
    const txInfo = wrapper.transaction; // { signature, meta, transaction: { message } }
    if (!txInfo) return;

    const signature = txInfo.signature ? bs58.encode(txInfo.signature) : "unknown";
    const meta = txInfo.meta || {};
    const message = txInfo.transaction?.message;
    const logs: string[] = meta.logMessages || [];



    // --- Extract mint ---
    const mint = this.extractMint(meta);
    if (!mint) return;

    // --- Detect Create: only emit if we can parse name/symbol from event log ---
    const parsed = this.parseCreateInstruction(message, meta);
    if (parsed.name && parsed.symbol) {
      const creator = message?.accountKeys?.[0]
        ? bs58.encode(message.accountKeys[0])
        : "unknown";
      const curvePDA = this.deriveCurvePDA(mint);
      const slot = wrapper.slot?.toString() || "0";

      console.log(`[Yellowstone] 🚀 NEW TOKEN: ${parsed.name} ($${parsed.symbol}) | ${mint.slice(0,12)} | sig=${signature.slice(0,8)}`);

      const tokenData: Record<string, string> = {
        mint, creator, curvePDA,
        platform: "pump",
        name: parsed.name,
        symbol: parsed.symbol,
        uri: parsed.uri || "",
        decimals: "6",
        createdAt: Date.now().toString(),
        createdTx: signature,
        slot,
      };

      await redis.hset(`token:${mint}`, tokenData);
      await redis.zadd("tokens:latest", Date.now(), mint);

      this.emit("new-launch", { ...tokenData, signature });
    }

    // --- Detect Swap (Buy/Sell) ---
    const swapType = this.detectSwapType(logs);
    if (swapType) {
      // Calculate SOL amount from balance changes
      const solAmount = this.calcSolDelta(meta);

      // Only track trades for tokens we've indexed
      const exists = await redis.exists(`token:${mint}`);
      if (exists) {
        // Increment counters in Redis
        if (swapType === "buy") {
          await redis.hincrby(`token:${mint}`, "buys", 1);
        } else {
          await redis.hincrby(`token:${mint}`, "sells", 1);
        }
        await redis.hincrby(`token:${mint}`, "totalTxns", 1);
        await redis.hincrbyfloat(`token:${mint}`, "volume", solAmount);

        // Log trade for volume24h calculation (sorted set: score=timestamp, value=type:amount)
        const now = Date.now();
        await redis.zadd(`trades:${mint}`, now, `${swapType}:${solAmount}:${now}`);
        // Trim trades older than 24h
        await redis.zremrangebyscore(`trades:${mint}`, 0, now - 86_400_000);

        this.emit("trade", { mint, signature, type: swapType, solAmount });
      }
    }
  }

  // ========== HELPERS ==========

  private extractMint(meta: any): string {
    const balances = meta?.postTokenBalances || [];
    for (const b of balances) {
      if (b.mint && b.mint !== "So11111111111111111111111111111111111111112") {
        return b.mint;
      }
    }
    return "";
  }

  private isCreate(logs: string[], message: any, meta?: any): boolean {
    // Check logs first (fastest)
    if (logs.some(l => l.includes("Program log: Instruction: Create"))) return true;

    const CREATE_DISC = "181ec828051c0777";

    // Check top-level instructions
    if (message?.instructions) {
      for (const ix of message.instructions) {
        const d = Buffer.from(ix.data || []);
        if (d.length >= 8 && d.slice(0, 8).toString("hex") === CREATE_DISC) return true;
      }
    }

    // Check inner instructions (CPI — where Pump.fun Create actually lives)
    if (meta?.innerInstructions) {
      for (const inner of meta.innerInstructions) {
        for (const ix of (inner.instructions || [])) {
          const d = Buffer.from(ix.data || []);
          if (d.length >= 8 && d.slice(0, 8).toString("hex") === CREATE_DISC) return true;
        }
      }
    }

    return false;
  }

  private detectSwapType(logs: string[]): "buy" | "sell" | null {
    for (const l of logs) {
      if (l.includes("Program log: Instruction: Buy")) return "buy";
      if (l.includes("Program log: Instruction: Sell")) return "sell";
    }
    return null;
  }

  // Calculate SOL delta from pre/post balances (signer = index 0)
  private calcSolDelta(meta: any): number {
    const pre = meta?.preBalances;
    const post = meta?.postBalances;
    if (!pre || !post || pre.length === 0) return 0;

    // The signer (index 0) spends SOL on buys, receives SOL on sells
    // We want the absolute SOL amount moved
    const delta = Math.abs(Number(post[0]) - Number(pre[0]));
    // Convert lamports to SOL, subtract fee (~5000 lamports)
    return Math.max(0, (delta - 5000) / 1_000_000_000);
  }

  private deriveCurvePDA(mint: string): string {
    try {
      const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
        new PublicKey(PUMP_FUN)
      );
      return pda.toBase58();
    } catch { return ""; }
  }

  // Parse name/symbol/uri from Pump.fun Create event
  // Strategy 1: Check "Program data:" log entries (Anchor event format, base64-encoded)
  // Strategy 2: Scan inner instruction data
  private parseCreateInstruction(message: any, meta?: any): { name: string; symbol: string; uri: string } {
    const empty = { name: "", symbol: "", uri: "" };

    // Strategy 1: Parse from "Program data:" log messages (most reliable)
    const logs: string[] = meta?.logMessages || [];
    for (const log of logs) {
      if (!log.startsWith("Program data: ")) continue;
      const b64 = log.slice("Program data: ".length);
      try {
        const data = Buffer.from(b64, "base64");
        if (data.length < 20) continue;
        const result = this.tryParseNameSymbolUri(data);
        if (result) return result;
      } catch { continue; }
    }

    // Strategy 2: Scan ALL instructions (top-level + inner)
    const allIx: any[] = [];
    if (message?.instructions) allIx.push(...message.instructions);
    if (meta?.innerInstructions) {
      for (const inner of meta.innerInstructions) {
        if (inner.instructions) allIx.push(...inner.instructions);
      }
    }
    for (const ix of allIx) {
      const data = Buffer.from(ix.data || []);
      if (data.length < 20) continue;
      const result = this.tryParseNameSymbolUri(data);
      if (result) return result;
    }

    return empty;
  }

  // Try to parse [8-byte disc] [4+name] [4+symbol] [4+uri] from buffer
  private tryParseNameSymbolUri(data: Buffer): { name: string; symbol: string; uri: string } | null {
    try {
      let offset = 8;
      const readStr = (): string => {
        if (offset + 4 > data.length) return "";
        const len = data.readUInt32LE(offset); offset += 4;
        if (len > 500 || len === 0 || offset + len > data.length) return "";
        const s = data.slice(offset, offset + len).toString("utf-8"); offset += len;
        return s;
      };
      const name = readStr();
      const symbol = readStr();
      const uri = readStr();
      // Sanity: name and symbol must be printable
      if (name && /^[\x20-\x7E]+$/.test(name) && symbol && /^[\x20-\x7E]+$/.test(symbol)) {
        return { name, symbol, uri };
      }
    } catch {}
    return null;
  }
}
