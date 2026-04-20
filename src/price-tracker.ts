// src/price-tracker.ts — Price ticks on every trade + rolling price change events
import Redis from "ioredis";
import "dotenv/config";
import { addPriceTick } from "./price-utils";

const redis = new Redis(process.env.REDIS_URL!);

// Intervals in milliseconds
const INTERVALS: Record<string, number> = {
  "1m":  60_000,
  "5m":  300_000,
  "15m": 900_000,
  "30m": 1_800_000,
  "1h":  3_600_000,
  "4h":  14_400_000,
  "24h": 86_400_000,
};

// Store a price tick on every trade (called from index.ts trade handler)
export async function recordPrice(mint: string) {
  const [priceQuote, priceUsd] = await redis.hmget(`token:${mint}`, "priceQuote", "priceUsd");
  if (!priceQuote) return;

  const pq = parseFloat(priceQuote);
  const pu = parseFloat(priceUsd || "0");
  if (pq <= 0) return;

  const now = Date.now();

  // Store tick in a Redis list for fast retrieval (newest at end)
  await addPriceTick(mint, {
    time: now,
    price: pq,
    price_usd: pu,
  });

  // Also recalculate price change events immediately on this trade
  await calcPriceEvents(mint);
}

// Calculate price change percentages for all intervals.
// Uses a per-interval reference-price anchor stored in the token hash — O(1) per trade
// instead of reading the full price tick list.
// priceRef_{interval}   = price_usd at the start of the current window
// priceRefTs_{interval} = epoch ms when that anchor was captured
// The anchor rolls forward once it ages past the interval so the window slides naturally.
export async function calcPriceEvents(mint: string) {
  // Single round-trip: current price + all 7 reference anchors (15 fields total)
  const fields: string[] = ["priceUsd"];
  for (const interval of Object.keys(INTERVALS)) {
    fields.push(`priceRef_${interval}`, `priceRefTs_${interval}`);
  }
  const values = await redis.hmget(`token:${mint}`, ...fields);

  const priceStr = values[0];
  if (!priceStr) return;
  const currentPrice = parseFloat(priceStr);
  if (currentPrice <= 0) return;

  const now = Date.now();
  const updates: Record<string, string> = {};

  let idx = 1;
  for (const [interval, ms] of Object.entries(INTERVALS)) {
    const refPrice = values[idx]     ? parseFloat(values[idx]!)     : 0;
    const refTs    = values[idx + 1] ? parseInt(values[idx + 1]!)   : 0;
    idx += 2;

    if (!refTs || !refPrice) {
      // First trade for this token — set anchor, no change yet
      updates[`priceRef_${interval}`]   = currentPrice.toString();
      updates[`priceRefTs_${interval}`] = now.toString();
      updates[interval] = "0";
      continue;
    }

    const pctChange = ((currentPrice - refPrice) / refPrice) * 100;
    updates[interval] = pctChange.toFixed(2);

    // Roll the anchor forward once the current reference is older than the interval
    if (now - refTs >= ms) {
      updates[`priceRef_${interval}`]   = currentPrice.toString();
      updates[`priceRefTs_${interval}`] = now.toString();
    }
  }

  if (Object.keys(updates).length > 0) {
    await redis.hset(`token:${mint}`, updates);
  }
}

// Get price history for a token (for charts & payload)
export async function getPriceHistory(mint: string, limit: number = 100): Promise<Array<{ time: number; price: number; price_usd: number }>> {
  const raw = await redis.lrange(`price:${mint}`, -limit, -1);
  return raw.map(r => {
    const t = JSON.parse(r);
    return { time: t.time, price: t.price, price_usd: t.price_usd || 0 };
  });
}

// Periodic price snapshot loop — runs every 30s to catch tokens between trades
export function startPriceTracker() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 199);
      // Pipeline the complete check, then run all recordPrice calls in parallel
      const pipeline = redis.pipeline();
      for (const mint of mints) pipeline.hget(`token:${mint}`, "complete");
      const results = await pipeline.exec();
      const activeMints = mints.filter((_, i) => (results?.[i]?.[1] as string) !== "true");
      await Promise.all(activeMints.map(mint => recordPrice(mint)));
    } catch (e: any) {
      console.error("[PriceTracker] Error:", e.message);
    }
  }, 30_000);

  console.log("[PriceTracker] Started — snapshots every 30s for top 200 active tokens");
}

