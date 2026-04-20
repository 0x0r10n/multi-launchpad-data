// src/curve-tracker.ts — Bonding curve data: liquidity, price, marketCap, curvePercentage
// Ongoing curve updates are pushed via Geyser account subscriptions.
// fetchAndDecodeCurve() is a ONE-TIME call used only for the initial launch broadcast.
import { Connection, PublicKey, AccountInfo } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

import { getParser, CurveState } from "./launchpads";

const connection          = new Connection(process.env.SOLANA_RPC!, "confirmed");
// Processed-commitment connection for fast probes — matches Geyser's commitment level.
// 'confirmed' adds 300-400ms of slot finality wait, which was causing the fast probe
// to timeout at 250ms. 'processed' returns the account as soon as the validator sees it.
const connectionProcessed = new Connection(process.env.SOLANA_RPC!, "processed");

const redis      = new Redis(process.env.REDIS_URL!);

const LAMPORTS_PER_SOL = 1_000_000_000;
const TOKEN_DECIMALS   = 6;
const TOTAL_SUPPLY     = 1_000_000_000;

// ── Volume 24h with in-memory cache (10s TTL) ────────────────────────────────
const vol24hCache = new Map<string, { value: number; ts: number }>();

async function calcVolume24h(mint: string, cutoff: number): Promise<number> {
  const cached = vol24hCache.get(mint);
  if (cached && Date.now() - cached.ts < 10_000) return cached.value;

  const entries = await redis.zrangebyscore(`trades:${mint}`, cutoff, "+inf");
  let total = 0;
  for (const e of entries) {
    const sol = parseFloat(e.split(":")[1] || "0");
    total += isNaN(sol) ? 0 : sol;
  }
  vol24hCache.set(mint, { value: total, ts: Date.now() });
  return total;
}

// ── Curve decoding: delegated entirely to the launchpad parser registry ───────
function decodeCurveAccount(data: Buffer, platform: string, mint?: string): CurveState | null {
  const result = getParser(platform)?.parseCurveData(data) ?? null;
  if (!result) {
    const hexDump = data.slice(0, Math.min(data.length, 80)).toString("hex");
    const mintTag = mint ? ` mint=${mint.slice(0, 12)}` : "";
    // For Meteora DBC/Bags: also dump bytes 220-260 so reserves at offset 232 are visible
    const extraDump = (platform === "meteora" || platform === "bags") && data.length >= 220
      ? ` hex[220:260]=${data.slice(220, Math.min(data.length, 260)).toString("hex")}`
      : "";
    console.warn(`[ZERO CURVE DATA] platform=${platform}${mintTag} dataLen=${data.length} hex[0:80]=${hexDump}${extraDump}`);
  }
  return result;
}

// ── Build Redis update object from decoded curve ─────────────────────────────
async function buildCurveUpdate(
  mint: string,
  decoded: CurveState,
  tokenData: Record<string, string>,
  solPrice: number,
): Promise<Record<string, string>> {
  const { complete, curvePercentage } = decoded;
  // Convert bigint reserves to number for floating-point price arithmetic
  const virtualTokenReservesN = Number(decoded.virtualTokenReserves);
  const virtualSolReservesN   = Number(decoded.virtualSolReserves);
  const realSolReservesN      = Number(decoded.realSolReserves);

  const priceInSol = virtualSolReservesN > 0 && virtualTokenReservesN > 0
    ? (virtualSolReservesN / LAMPORTS_PER_SOL) / (virtualTokenReservesN / Math.pow(10, TOKEN_DECIMALS))
    : 0;

  if (priceInSol <= 0) {
    const p = tokenData.platform || "unknown";
    console.error(`[CRITICAL] Zero price in curve update — mint=${mint.slice(0, 12)} platform=${p} vSol=${virtualSolReservesN} vToken=${virtualTokenReservesN}`);
  }

  const liquiditySol = virtualSolReservesN / LAMPORTS_PER_SOL;
  const marketCapSol = priceInSol * TOTAL_SUPPLY;
  const volume24h    = await calcVolume24h(mint, Date.now() - 86_400_000);

  let gradStatus = "new";
  if (complete)                    gradStatus = "graduated";
  else if (curvePercentage >= 80)  gradStatus = "graduating";

  return {
    priceQuote:           priceInSol.toString(),
    priceUsd:             (priceInSol * solPrice).toString(),
    liquidity:            liquiditySol.toFixed(4),
    liquidityUsd:         (liquiditySol * solPrice).toFixed(2),
    marketCapQuote:       marketCapSol.toFixed(4),
    marketCapUsd:         (marketCapSol * solPrice).toFixed(2),
    curvePercentage:      curvePercentage.toFixed(2),
    virtualTokenReserves: virtualTokenReservesN.toString(),
    virtualSolReserves:   virtualSolReservesN.toString(),
    realSolReserves:      realSolReservesN.toString(),
    complete:             complete ? "true" : "false",
    volumeUsd:            (parseFloat(tokenData.volume || "0") * solPrice).toFixed(2),
    volume24h:            volume24h.toFixed(6),
    volume24hUsd:         (volume24h * solPrice).toFixed(2),
    graduationStatus:     gradStatus,
  };
}

// ── RPC curve probe with configurable timeout ─────────────────────────────────
// Used in two roles:
//   - Fast probe (150ms) at launch, racing against Geyser first snapshot
//   - Fallback (800ms default) if Geyser never delivers
// New token has no trade history — passes empty tokenData (volume = 0).
export async function fetchAndDecodeCurve(
  mint: string,
  curvePDA: string,
  platform: string,
  timeoutMs = 800,
  commitment: "processed" | "confirmed" = "processed",
): Promise<Record<string, string> | null> {
  const conn = commitment === "processed" ? connectionProcessed : connection;
  try {
    const rpc = (async () => {
      let accountInfo: AccountInfo<Buffer> | null = null;
      let solPrice = 0;
      
      // Retry up to 3 times for new accounts (RPC indexing lag)
      for (let attempt = 0; attempt < 3; attempt++) {
        const [info, price] = await Promise.all([
          conn.getAccountInfo(new PublicKey(curvePDA), commitment),
          getSolPrice(),
        ]);
        accountInfo = info;
        solPrice = price;
        if (accountInfo) break;
        if (attempt < 2 && commitment === "processed") await new Promise(r => setTimeout(r, 150));
        else break; // don't retry confirmed
      }

      if (!accountInfo) {
        console.warn(`[CurveTracker] account not found after retries — platform=${platform} curvePDA=${curvePDA.slice(0, 12)} mint=${mint.slice(0, 12)} commitment=${commitment}`);
        return null;
      }
      if (!accountInfo.data) return null;
      const decoded = decodeCurveAccount(Buffer.from(accountInfo.data), platform, mint);
      if (!decoded) return null;
      return await buildCurveUpdate(mint, decoded, {}, solPrice);
    })();
    const timeout = new Promise<null>(r => setTimeout(() => r(null), timeoutMs));
    return await Promise.race([rpc, timeout]);
  } catch {
    return null;
  }
}

// ── Geyser-pushed curve update (replaces all periodic RPC-based curve fetching) ─
// Returns the update fields so callers can check completion state without an extra Redis read.
// Handles the race where the token hasn't been written to Redis yet (first push at launch):
// in that case we still decode and return the fields — the new-launch handler persists them.
export async function processCurveAccountUpdate(
  mint: string,
  data: Buffer,
  platform: string,
  isStartup = false,
): Promise<Record<string, string> | null> {
  const decoded = decodeCurveAccount(data, platform, mint);
  if (!decoded) return null;

  const [tokenData, solPrice] = await Promise.all([
    redis.hgetall(`token:${mint}`),
    getSolPrice(),
  ]);

  // Use empty tokenData if token isn't in Redis yet (first push race) — volume will be 0 which is correct
  const td = (tokenData?.mint ? tokenData : {}) as Record<string, string>;
  const update = await buildCurveUpdate(mint, decoded, td, solPrice);

  // Graduation transition — only when token exists in Redis
  if (td.creator && update.complete === "true" && td.complete !== "true") {
    redis.hincrby(`creator:${td.creator}`, "migrated", 1).catch(() => {});
  }

  if (tokenData?.mint) {
    await redis.hset(`token:${mint}`, update);
    if (!isStartup) {
      await redis.publish("token-updates", mint);
    }
  }
  return update;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

let cachedSolPrice = 0;
let solPriceLastFetch = 0;

async function getSolPrice(): Promise<number> {
  if (Date.now() - solPriceLastFetch < 30_000 && cachedSolPrice > 0) return cachedSolPrice;

  // Jupiter first — public, stable API
  try {
    const res = await fetch("https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112");
    const json: any = await res.json();
    const price = json["So11111111111111111111111111111111111111112"]?.usdPrice;
    if (price) {
      cachedSolPrice = price;
      solPriceLastFetch = Date.now();
      return cachedSolPrice;
    }
  } catch {}

  // Fallback: pump.fun internal API (undocumented — may change without notice)
  try {
    const res = await fetch("https://frontend-api-v3.pump.fun/sol-price");
    if (res.ok) {
      const json: any = await res.json();
      if (json.solPrice) {
        cachedSolPrice = json.solPrice;
        solPriceLastFetch = Date.now();
        return cachedSolPrice;
      }
    }
  } catch {}

  return cachedSolPrice || 140;
}
