// src/curve-tracker.ts — Bonding curve data: liquidity, price, marketCap, curvePercentage
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

const redis = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const PUMP_FUN = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");

// Pump.fun bonding curve layout (after 8-byte discriminator):
// virtualTokenReserves: u64 (8 bytes)
// virtualSolReserves: u64 (8 bytes)
// realTokenReserves: u64 (8 bytes)
// realSolReserves: u64 (8 bytes)
// tokenTotalSupply: u64 (8 bytes)
// complete: bool (1 byte)

const LAMPORTS_PER_SOL = 1_000_000_000;
const TOKEN_DECIMALS = 6;
const TOTAL_SUPPLY = 1_000_000_000; // 1B tokens
const BONDING_CURVE_TARGET_SOL = 85; // ~85 SOL to graduate

export function queueCurveUpdate(mint: string) {
  updateCurveData(mint).catch(e => {});
}

async function updateCurveData(mint: string) {
  const tokenData = await redis.hgetall(`token:${mint}`);
  if (!tokenData?.curvePDA) return;

  const curvePDA = new PublicKey(tokenData.curvePDA);
  const accountInfo = await connection.getAccountInfo(curvePDA);
  if (!accountInfo || !accountInfo.data) return;

  const data = accountInfo.data;
  if (data.length < 49) return; // min size: 8 disc + 5*8 + 1

  // Parse bonding curve fields (little-endian u64)
  const virtualTokenReserves = readU64(data, 8);
  const virtualSolReserves = readU64(data, 16);
  const realTokenReserves = readU64(data, 24);
  const realSolReserves = readU64(data, 32);
  const tokenTotalSupply = readU64(data, 40);
  const complete = data[48] === 1;

  // Calculate price (SOL per token)
  const priceInSol = virtualSolReserves > 0 && virtualTokenReserves > 0
    ? (virtualSolReserves / LAMPORTS_PER_SOL) / (virtualTokenReserves / Math.pow(10, TOKEN_DECIMALS))
    : 0;

  // Liquidity = virtual SOL reserves (what's available to trade against)
  const liquiditySol = virtualSolReserves / LAMPORTS_PER_SOL;

  // Market cap = price * total supply
  const marketCapSol = priceInSol * TOTAL_SUPPLY;

  // Curve percentage: real SOL deposited / 85 SOL target
  const realSol = realSolReserves / LAMPORTS_PER_SOL;
  const curvePercentage = Math.min(100, (realSol / BONDING_CURVE_TARGET_SOL) * 100);

  // Volume 24h: sum trade volumes from last 24 hours
  const now = Date.now();
  const cutoff24h = now - 86_400_000;
  const volume24h = await calcVolume24h(mint, cutoff24h);

  // Get SOL price for USD conversion (cached)
  const solPrice = await getSolPrice();

  // Graduation status
  let gradStatus = "new";
  if (complete) gradStatus = "graduated";
  else if (curvePercentage >= 80) gradStatus = "graduating";
  else if (curvePercentage >= 50) gradStatus = "active";

  const update: Record<string, string> = {
    priceQuote: priceInSol.toString(),
    priceUsd: (priceInSol * solPrice).toString(),
    liquidity: liquiditySol.toFixed(4),
    liquidityUsd: (liquiditySol * solPrice).toFixed(2),
    marketCapQuote: marketCapSol.toFixed(4),
    marketCapUsd: (marketCapSol * solPrice).toFixed(2),
    curvePercentage: curvePercentage.toFixed(2),
    virtualTokenReserves: virtualTokenReserves.toString(),
    virtualSolReserves: virtualSolReserves.toString(),
    realSolReserves: realSolReserves.toString(),
    complete: complete ? "true" : "false",
    volumeUsd: (parseFloat(tokenData.volume || "0") * solPrice).toFixed(2),
    volume24h: volume24h.toFixed(6),
    volume24hUsd: (volume24h * solPrice).toFixed(2),
    graduationStatus: gradStatus,
  };

  await redis.hset(`token:${mint}`, update);
  await redis.publish("token-updates", mint);
}

// Calculate 24h volume from the trade log sorted set
async function calcVolume24h(mint: string, cutoff: number): Promise<number> {
  const entries = await redis.zrangebyscore(`trades:${mint}`, cutoff, "+inf");
  let total = 0;
  for (const e of entries) {
    const sol = parseFloat(e.split(":")[1] || "0");
    total += sol;
  }
  return total;
}

// Read a u64 (little-endian) from buffer at offset
function readU64(buf: Buffer, offset: number): number {
  // Use BigInt for accuracy then convert (JS number is fine for these ranges)
  const lo = buf.readUInt32LE(offset);
  const hi = buf.readUInt32LE(offset + 4);
  return hi * 0x100000000 + lo;
}

// Cache SOL price, refresh every 30 seconds
let cachedSolPrice = 0;
let solPriceLastFetch = 0;

async function getSolPrice(): Promise<number> {
  if (Date.now() - solPriceLastFetch < 30_000 && cachedSolPrice > 0) {
    return cachedSolPrice;
  }

  try {
    // Primary: Pump.fun
    const res = await fetch("https://frontend-api-v3.pump.fun/sol-price");
    if (res.ok) {
      const json: any = await res.json();
      if (json.solPrice) {
        cachedSolPrice = json.solPrice;
        solPriceLastFetch = Date.now();
        return cachedSolPrice;
      }
    }
    throw new Error("Pump.fun price fetch failed");
  } catch (err) {
    try {
      // Fallback: Jupiter
      const res = await fetch("https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112");
      const json: any = await res.json();
      const price = json["So11111111111111111111111111111111111111112"]?.usdPrice;
      if (price) {
        cachedSolPrice = price;
        solPriceLastFetch = Date.now();
        return cachedSolPrice;
      }
    } catch {
      // Final fallback if both fail, keep last cached price
    }
  }

  return cachedSolPrice || 140;
}

export function startCurveRefreshLoop() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 19); // latest 20
      for (const mint of mints) {
        const token = await redis.hgetall(`token:${mint}`);
        if (token?.complete === "true") continue; // skip graduated
        queueCurveUpdate(mint);
      }
    } catch (e: any) {
      console.error("[CurveTracker] Refresh error:", e.message);
    }
  }, 30_000);
}
