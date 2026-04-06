// src/curve-tracker.ts — Bonding curve data: liquidity, price, marketCap, curvePercentage
// Ongoing curve updates are pushed via Geyser account subscriptions.
// fetchAndDecodeCurve() is a ONE-TIME call used only for the initial launch broadcast.
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

import { getParser, CurveState } from "./launchpads";

const connection = new Connection(process.env.SOLANA_RPC!, "processed");

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
function decodeCurveAccount(data: Buffer, platform: string): CurveState | null {
  return getParser(platform)?.parseCurveData(data) ?? null;
}

// ── Build Redis update object from decoded curve ─────────────────────────────
async function buildCurveUpdate(
  mint: string,
  decoded: CurveState,
  tokenData: Record<string, string>,
  solPrice: number,
): Promise<Record<string, string>> {
  const { virtualTokenReserves, virtualSolReserves, realSolReserves, complete, curvePercentage } = decoded;

  const priceInSol = virtualSolReserves > 0 && virtualTokenReserves > 0
    ? (virtualSolReserves / LAMPORTS_PER_SOL) / (virtualTokenReserves / Math.pow(10, TOKEN_DECIMALS))
    : 0;

  const liquiditySol = virtualSolReserves / LAMPORTS_PER_SOL;
  const marketCapSol = priceInSol * TOTAL_SUPPLY;
  const volume24h    = await calcVolume24h(mint, Date.now() - 86_400_000);

  let gradStatus = "new";
  if (complete)              gradStatus = "graduated";
  else if (curvePercentage >= 80) gradStatus = "graduating";
  else if (curvePercentage >= 50) gradStatus = "active";

  return {
    priceQuote:           priceInSol.toString(),
    priceUsd:             (priceInSol * solPrice).toString(),
    liquidity:            liquiditySol.toFixed(4),
    liquidityUsd:         (liquiditySol * solPrice).toFixed(2),
    marketCapQuote:       marketCapSol.toFixed(4),
    marketCapUsd:         (marketCapSol * solPrice).toFixed(2),
    curvePercentage:      curvePercentage.toFixed(2),
    virtualTokenReserves: virtualTokenReserves.toString(),
    virtualSolReserves:   virtualSolReserves.toString(),
    realSolReserves:      realSolReserves.toString(),
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
): Promise<Record<string, string> | null> {
  try {
    const rpc = (async () => {
      const [accountInfo, solPrice] = await Promise.all([
        connection.getAccountInfo(new PublicKey(curvePDA), "processed"),
        getSolPrice(),
      ]);
      if (!accountInfo?.data) return null;
      const decoded = decodeCurveAccount(Buffer.from(accountInfo.data), platform);
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
): Promise<Record<string, string> | null> {
  const decoded = decodeCurveAccount(data, platform);
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
    await redis.publish("token-updates", mint);
  }
  return update;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

let cachedSolPrice = 0;
let solPriceLastFetch = 0;

async function getSolPrice(): Promise<number> {
  if (Date.now() - solPriceLastFetch < 30_000 && cachedSolPrice > 0) return cachedSolPrice;

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

  return cachedSolPrice || 140;
}
