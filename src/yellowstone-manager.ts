// src/yellowstone-manager.ts — FIXED for correct Yellowstone gRPC nesting
import Client from "@triton-one/yellowstone-grpc";
import { SubscribeUpdate, CommitmentLevel } from "@triton-one/yellowstone-grpc";
import { PublicKey } from "@solana/web3.js";
import bs58 from "bs58";
import { EventEmitter } from "events";
import Redis from "ioredis";
import "dotenv/config";

const PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const MOONSHOT = "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"; // legacy
const MOON_IT = "Moonit1111111111111111111111111111111111111"; // current moon.it
const BAGS = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN";
const LETSBONK = "FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1";
const LAUNCHLAB = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";
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

  private subscribeToLaunchpads() {
    const req: any = {
      accounts: {},
      slots: {},
      transactions: {
        launchpads: {
          vote: false,
          failed: false,
          accountInclude: [PUMP_FUN, MOONSHOT, MOON_IT, BAGS, LETSBONK, LAUNCHLAB],
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
    console.log("[Yellowstone] Subscribed to 6 launchpads (Pump.fun + Moon.it + Bags + LetsBonk + LaunchLab).");
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



    // --- Detect Platform ---
    const platform = this.getPlatform(meta, message);
    if (!platform) return;

    // --- Extract mint ---
    const mint = this.extractMint(meta);
    if (!mint) return;

    // --- Extract Maker/Signer ---
    const maker = message?.accountKeys?.[0] 
      ? bs58.encode(message.accountKeys[0]) 
      : "unknown";

    // --- Detect Create ---
    const isCreate = this.isCreate(platform, logs, message, meta);
    if (isCreate) {
      const parsed = this.parseCreateInstruction(message, meta);
      const name = parsed.name || "Unknown Token";
      const symbol = parsed.symbol || "???";
      
      // If Pump.fun, we strictly require name/symbol to be parsed. Moonshot can pass with defaults until enriched.
      if (platform === "pump" && (!parsed.name || !parsed.symbol)) {
        return;
      }

      const creator = message?.accountKeys?.[0]
        ? bs58.encode(message.accountKeys[0])
        : "unknown";
      const curvePDA = this.deriveCurvePDA(platform, mint);
      const slot = wrapper.slot?.toString() || "0";

      console.log(`[Yellowstone] 🚀 NEW TOKEN (${platform}): ${name} ($${symbol}) | ${mint.slice(0,12)} | sig=${signature.slice(0,8)}`);

      const tokenData: Record<string, string> = {
        mint, creator, curvePDA,
        platform,
        name,
        symbol,
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
    const swapType = this.detectSwapType(platform, logs, meta);
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

        this.emit("trade", { mint, signature, type: swapType, solAmount, maker });
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

  private getPlatform(meta: any, message: any): "pump" | "moon" | "bags" | "letsbonk" | "launchlab" | null {
    if (!meta || !message) return null;
    const logStr = (meta.logMessages || []).join(" ");
    if (logStr.includes(PUMP_FUN)) return "pump";
    if (logStr.includes(MOONSHOT) || logStr.includes(MOON_IT)) return "moon";
    if (logStr.includes(BAGS)) return "bags";
    if (logStr.includes(LETSBONK)) return "letsbonk";
    if (logStr.includes(LAUNCHLAB)) return "launchlab";
    
    // Fallback: check program IDs in instructions
    const accountKeys = message.accountKeys || [];
    for (const key of accountKeys) {
      const b58 = bs58.encode(key);
      if (b58 === PUMP_FUN) return "pump";
      if (b58 === MOONSHOT || b58 === MOON_IT) return "moon";
      if (b58 === BAGS) return "bags";
      if (b58 === LETSBONK) return "letsbonk";
      if (b58 === LAUNCHLAB) return "launchlab";
    }
    return null;
  }

  private isCreate(platform: string, logs: string[], message: any, meta?: any): boolean {
    if (platform === "pump") {
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

      // Check inner instructions (CPI)
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

    if (platform === "moon") {
      // Moonshot create is identified by discriminator `2a9a1c1e0f0a1b2c` or specific logs
      const MOON_CREATE_DISC = "2a9a1c1e0f0a1b2c";
      
      if (logs.some(l => l.toLowerCase().includes("tokenmint"))) return true;
      
      if (message?.instructions) {
        for (const ix of message.instructions) {
          const d = Buffer.from(ix.data || []);
          if (d.length >= 8 && d.slice(0, 8).toString("hex") === MOON_CREATE_DISC) return true;
        }
      }
      if (meta?.innerInstructions) {
        for (const inner of meta.innerInstructions) {
          for (const ix of (inner.instructions || [])) {
            const d = Buffer.from(ix.data || []);
            if (d.length >= 8 && d.slice(0, 8).toString("hex") === MOON_CREATE_DISC) return true;
          }
        }
      }
      return false;
    }

    if (platform === "bags") {
      const isBagsCreate = logs.some(l => 
        l.includes("InitializeVirtualPool") || 
        l.includes("initialize_virtual_pool") || 
        l.includes("DBC: New Pool") || 
        l.includes("Meteora DBC: Initialize")
      );
      if (isBagsCreate) return true;
      return false;
    }

    if (platform === "letsbonk") {
      // LetsBonk uses similar Anchor patterns to Pump.fun
      if (logs.some(l => l.includes("Program log: Instruction: Create"))) return true;

      // Check instruction discriminator in top-level + inner instructions
      const allIx: any[] = [];
      if (message?.instructions) allIx.push(...message.instructions);
      if (meta?.innerInstructions) {
        for (const inner of meta.innerInstructions) {
          if (inner.instructions) allIx.push(...inner.instructions);
        }
      }
      for (const ix of allIx) {
        const d = Buffer.from(ix.data || []);
        if (d.length >= 8) {
          // Check for InitializeMint2 (SPL token creation) as a signal
          if (d[0] === 20) return true; // InitializeMint2 discriminator
        }
      }
      return false;
    }

    if (platform === "launchlab") {
      // Raydium LaunchLab uses Anchor-style logs
      if (logs.some(l => l.includes("Program log: Instruction: Create") || l.includes("InitializePool") || l.includes("LaunchPool"))) return true;

      // Check for InitializeMint2 in inner instructions
      const allIx: any[] = [];
      if (message?.instructions) allIx.push(...message.instructions);
      if (meta?.innerInstructions) {
        for (const inner of meta.innerInstructions) {
          if (inner.instructions) allIx.push(...inner.instructions);
        }
      }
      for (const ix of allIx) {
        const d = Buffer.from(ix.data || []);
        if (d.length >= 1 && d[0] === 20) return true;
      }
      return false;
    }

    return false;
  }

  private detectSwapType(platform: string, logs: string[], meta: any): "buy" | "sell" | null {
    if (platform === "pump") {
      for (const l of logs) {
        if (l.includes("Program log: Instruction: Buy")) return "buy";
        if (l.includes("Program log: Instruction: Sell")) return "sell";
      }
    }
    
    if (platform === "moon") {
      for (const l of logs) {
        if (l.includes("Buy") || l.includes("buy")) return "buy";
        if (l.includes("Sell") || l.includes("sell")) return "sell";
      }
      // Fallback: check SOL balances if logs dont explicitly say Buy/Sell
      const pre = meta?.preBalances;
      const post = meta?.postBalances;
      if (pre && post && pre.length > 0) {
        const delta = Number(post[0]) - Number(pre[0]);
        if (delta < -10000) return "buy";  // Spent SOL
        if (delta > 10000) return "sell";  // Gained SOL
      }
    }

    if (platform === "bags") {
      for (const l of logs) {
        if (l.includes("Buy") || l.toLowerCase().includes("buy")) return "buy";
        if (l.includes("Sell") || l.toLowerCase().includes("sell")) return "sell";
      }
      const pre = meta?.preBalances;
      const post = meta?.postBalances;
      if (pre && post && pre.length > 0) {
        const delta = Number(post[0]) - Number(pre[0]);
        if (delta < -10000) return "buy";
        if (delta > 10000) return "sell";
      }
    }

    if (platform === "letsbonk") {
      for (const l of logs) {
        if (l.includes("Program log: Instruction: Buy")) return "buy";
        if (l.includes("Program log: Instruction: Sell")) return "sell";
      }
      // Fallback: SOL balance delta
      const pre = meta?.preBalances;
      const post = meta?.postBalances;
      if (pre && post && pre.length > 0) {
        const delta = Number(post[0]) - Number(pre[0]);
        if (delta < -10000) return "buy";
        if (delta > 10000) return "sell";
      }
    }

    if (platform === "launchlab") {
      for (const l of logs) {
        if (l.includes("Buy") || l.includes("buy")) return "buy";
        if (l.includes("Sell") || l.includes("sell")) return "sell";
      }
      const pre = meta?.preBalances;
      const post = meta?.postBalances;
      if (pre && post && pre.length > 0) {
        const delta = Number(post[0]) - Number(pre[0]);
        if (delta < -10000) return "buy";
        if (delta > 10000) return "sell";
      }
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

  private deriveCurvePDA(platform: string, mint: string): string {
    try {
      if (platform === "pump") {
        const [pda] = PublicKey.findProgramAddressSync(
          [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
          new PublicKey(PUMP_FUN)
        );
        return pda.toBase58();
      }
      if (platform === "moon") {
        // Moon.it / Moonshot bonding curve derivation (try MOON_IT first)
        const programId = MOON_IT;
        const [pda] = PublicKey.findProgramAddressSync(
          [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
          new PublicKey(programId)
        );
        return pda.toBase58();
      }
      if (platform === "bags") {
        const [pda] = PublicKey.findProgramAddressSync(
          [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
          new PublicKey(BAGS)
        );
        return pda.toBase58();
      }
      if (platform === "letsbonk") {
        const [pda] = PublicKey.findProgramAddressSync(
          [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
          new PublicKey(LETSBONK)
        );
        return pda.toBase58();
      }
      if (platform === "launchlab") {
        // Raydium LaunchLab uses "launch-pool" seed
        const [pda] = PublicKey.findProgramAddressSync(
          [Buffer.from("launch-pool"), new PublicKey(mint).toBuffer()],
          new PublicKey(LAUNCHLAB)
        );
        return pda.toBase58();
      }
    } catch {}
    return "";
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
