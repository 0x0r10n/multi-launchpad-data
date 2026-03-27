// src/curve-tracker.ts — Bonding curve data: liquidity, price, marketCap, curvePercentage
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

import { getParser, CurveState } from "./launchpads";

const redis      = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const LAMPORTS_PER_SOL = 1_000_000_000;
const TOKEN_DECIMALS   = 6;
const TOTAL_SUPPLY     = 1_000_000_000;

// ── Debounce map: prevents RPC hammering on hot tokens ──────────────────────
const pendingCurveUpdates = new Map<string, ReturnType<typeof setTimeout>>();
const DEBOUNCE_MS = 300;

export function queueCurveUpdate(mint: string) {
  const existing = pendingCurveUpdates.get(mint);
  if (existing) clearTimeout(existing);
  const t = setTimeout(() => {
    pendingCurveUpdates.delete(mint);
    updateCurveData(mint).catch(() => {});
  }, DEBOUNCE_MS);
  pendingCurveUpdates.set(mint, t);
}

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
// Each parser owns its own layout — no if/else chains here.
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

// ── Inline fetch for new-launch handler (no debounce, no queue) ──────────────
// Fetches and decodes the bonding curve immediately so the first broadcast
// includes price/liquidity/curvePercentage instead of zeros.
export async function fetchAndDecodeCurve(
  mint: string,
  curvePDA: string,
  platform: string,
): Promise<Record<string, string> | null> {
  try {
    // "processed" matches Yellowstone's commitment level — account is live by the time we ask
    const [accountInfo, solPrice] = await Promise.all([
      connection.getAccountInfo(new PublicKey(curvePDA), "processed"),
      getSolPrice(),
    ]);
    if (!accountInfo?.data) return null;

    const decoded = decodeCurveAccount(Buffer.from(accountInfo.data), platform);
    if (!decoded) return null;

    // New token has no trade history yet — pass empty tokenData (volume = 0)
    return await buildCurveUpdate(mint, decoded, {}, solPrice);
  } catch {
    return null;
  }
}

// ── Single-token update (trade-triggered, after debounce) ────────────────────
async function updateCurveData(mint: string) {
  const tokenData = await redis.hgetall(`token:${mint}`);
  if (!tokenData?.curvePDA) return;

  const accountInfo = await connection.getAccountInfo(new PublicKey(tokenData.curvePDA));
  if (!accountInfo?.data) return;

  const platform = tokenData.platform || "pump";
  const decoded = decodeCurveAccount(Buffer.from(accountInfo.data), platform);
  if (!decoded) return;

  const solPrice = await getSolPrice();
  const update = await buildCurveUpdate(mint, decoded, tokenData, solPrice);
  await redis.hset(`token:${mint}`, update);
  await redis.publish("token-updates", mint);
}

// ── Batch update for the sweep loop: 1 RPC call for all curve accounts ────────
async function batchUpdateCurves(tokens: { mint: string; tokenData: Record<string, string> }[]) {
  const curveKeys = tokens.map(t => new PublicKey(t.tokenData.curvePDA));

  // Single RPC call + SOL price fetch in parallel
  const [accountInfos, solPrice] = await Promise.all([
    connection.getMultipleAccountsInfo(curveKeys),
    getSolPrice(),
  ]);

  await Promise.all(tokens.map(async ({ mint, tokenData }, i) => {
    const info = accountInfos[i];
    if (!info?.data) return;

    const platform = tokenData.platform || "pump";
    const decoded = decodeCurveAccount(Buffer.from(info.data), platform);
    if (!decoded) return;

    const update = await buildCurveUpdate(mint, decoded, tokenData, solPrice);
    await redis.hset(`token:${mint}`, update);
    await redis.publish("token-updates", mint);
  }));
}

// ── Sweep loop: batch-refresh active tokens every 30s ────────────────────────
export function startCurveRefreshLoop() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 19);
      const tokenDatas = await Promise.all(mints.map(m => redis.hgetall(`token:${m}`)));

      const active = mints
        .map((mint, i) => ({ mint, tokenData: tokenDatas[i] }))
        .filter(({ tokenData }) => tokenData?.curvePDA && tokenData.complete !== "true");

      if (active.length > 0) await batchUpdateCurves(active);
    } catch (e: any) {
      console.error("[CurveTracker] Refresh error:", e.message);
    }
  }, 30_000);
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
