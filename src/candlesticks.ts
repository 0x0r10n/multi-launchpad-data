// src/candlesticks.ts — OHLCV candle aggregation from raw price ticks + trade volume
// Matches the Moralis/Axiom candlestick response schema exactly.
import Redis from "ioredis";
import "dotenv/config";

const redis = new Redis(process.env.REDIS_URL!);

export interface Ohlcv {
  timestamp: string;
  open:   number;
  close:  number;
  high:   number;
  low:    number;
  volume: number;
  trades: number;
}

// Timeframe string → interval in seconds
const TIMEFRAME_SEC: Record<string, number> = {
  "1s":   1,
  "10s":  10,
  "30s":  30,
  "1min": 60,
  "5min": 300,
  "10min":600,
  "30min":1800,
  "1h":   3600,
  "4h":   14400,
  "12h":  43200,
  "1d":   86400,
  "1w":   604800,
  "1M":   2592000,
};

/**
 * Build OHLCV candlesticks for a token.
 *
 * Data sources:
 *   - price:{mint} (Redis List) — each entry is JSON with { time, price, price_usd }
 *   - trades:{mint} (Redis Sorted Set, score=epoch-ms) — each entry is "sig:solAmount:type"
 *
 * @param mint      Token mint address
 * @param timeframe One of the TIMEFRAME_SEC keys (e.g. "1min", "5min", "1h")
 * @param fromSec   Start time in unix seconds (inclusive)
 * @param toSec     End time in unix seconds (inclusive)
 * @param currency  "usd" or "native" (SOL)
 * @param limit     Max candles to return (capped at 1000)
 */
export async function getCandlesticks(
  mint: string,
  timeframe: string,
  fromSec: number,
  toSec: number,
  currency: "usd" | "native",
  limit: number = 100,
): Promise<Ohlcv[]> {
  const intervalSec = TIMEFRAME_SEC[timeframe] ?? 60;
  const fromMs = fromSec * 1000;
  const toMs   = toSec * 1000;

  // ── 1. Fetch raw price ticks ─────────────────────────────────────────────
  const ticksRaw = await redis.lrange(`price:${mint}`, 0, -1);
  if (ticksRaw.length === 0) return [];

  // Parse and filter to the requested time window
  const ticks: Array<{ time: number; price: number; price_usd: number }> = [];
  for (const raw of ticksRaw) {
    try {
      const t = JSON.parse(raw);
      // time field is epoch-ms (from price-tracker.ts recordPrice → Date.now())
      if (t.time >= fromMs && t.time <= toMs) {
        ticks.push({ time: t.time, price: t.price ?? 0, price_usd: t.price_usd ?? 0 });
      }
    } catch { /* skip malformed */ }
  }
  if (ticks.length === 0) return [];

  // ── 2. Fetch trade volume in the same window ────────────────────────────
  // trades:{mint} entries are "type:solAmount:timestamp", scored by epoch-ms
  const tradeEntriesWithScores = await redis.zrangebyscore(
    `trades:${mint}`, fromMs, toMs, "WITHSCORES",
  );
  const tradesByPeriod = new Map<number, { volume: number; count: number }>();
  for (let i = 0; i < tradeEntriesWithScores.length; i += 2) {
    const entry = tradeEntriesWithScores[i];
    const scoreMs = parseInt(tradeEntriesWithScores[i + 1]);
    const periodStart = Math.floor((scoreMs / 1000) / intervalSec) * intervalSec;

    const solAmount = parseFloat(entry.split(":")[1] || "0");
    const existing = tradesByPeriod.get(periodStart) ?? { volume: 0, count: 0 };
    existing.volume += isNaN(solAmount) ? 0 : solAmount;
    existing.count += 1;
    tradesByPeriod.set(periodStart, existing);
  }

  // ── 3. Group price ticks into candle periods ────────────────────────────
  const groups = new Map<number, typeof ticks>();
  for (const t of ticks) {
    const periodStart = Math.floor((t.time / 1000) / intervalSec) * intervalSec;
    let arr = groups.get(periodStart);
    if (!arr) { arr = []; groups.set(periodStart, arr); }
    arr.push(t);
  }

  // ── 4. Build candles ───────────────────────────────────────────────────
  const periods = [...groups.keys()].sort((a, b) => a - b);
  const result: Ohlcv[] = [];

  for (const period of periods) {
    const group = groups.get(period)!.sort((a, b) => a.time - b.time);
    const getPrice = currency === "usd"
      ? (t: typeof ticks[0]) => t.price_usd
      : (t: typeof ticks[0]) => t.price;

    const open  = getPrice(group[0]);
    const close = getPrice(group[group.length - 1]);
    let high = open;
    let low  = open;

    for (const t of group) {
      const p = getPrice(t);
      if (p > high) high = p;
      if (p > 0 && (low <= 0 || p < low)) low = p;
    }

    const tradeData = tradesByPeriod.get(period);

    result.push({
      timestamp: new Date(period * 1000).toISOString(),
      open,
      close,
      high,
      low,
      volume: tradeData?.volume ?? 0,
      trades: tradeData?.count ?? group.length,
    });
  }

  // Return the last `limit` candles (newest)
  return result.slice(-limit);
}
