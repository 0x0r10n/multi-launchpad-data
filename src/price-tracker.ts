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

// Calculate price change percentages for all intervals
export async function calcPriceEvents(mint: string) {
  const priceStr = await redis.hget(`token:${mint}`, "priceUsd");
  if (!priceStr) return;

  const currentPrice = parseFloat(priceStr);
  if (currentPrice <= 0) return;

  // Get all ticks from the list
  const raw = await redis.lrange(`price:${mint}`, 0, -1);
  if (raw.length === 0) return;

  const ticks = raw.map(r => JSON.parse(r));
  const now = Date.now();
  const updates: Record<string, string> = {};

  for (const [interval, ms] of Object.entries(INTERVALS)) {
    const targetTime = now - ms;

    // Find the oldest tick that is still within this interval
    // (the first tick whose timestamp >= targetTime)
    let oldTick = null;
    for (const t of ticks) {
      if (t.time >= targetTime) {
        oldTick = t;
        break;
      }
    }

    if (oldTick) {
      const oldPrice = oldTick.price_usd || 0;
      if (oldPrice > 0) {
        const pctChange = ((currentPrice - oldPrice) / oldPrice) * 100;
        updates[interval] = pctChange.toFixed(2);
      }
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
      const mints = await redis.zrevrange("tokens:latest", 0, 49);
      for (const mint of mints) {
        const complete = await redis.hget(`token:${mint}`, "complete");
        if (complete === "true") continue;

        await recordPrice(mint);
      }
    } catch (e: any) {
      console.error("[PriceTracker] Error:", e.message);
    }
  }, 30_000);

  console.log("[PriceTracker] Started — snapshots every 30s for top 50 tokens");
}

