import Redis from 'ioredis';
import 'dotenv/config';

const redis = new Redis(process.env.REDIS_URL!);

const MAX_PRICE_TICKS = 300;
const TRIM_BUFFER = 400;   // only trim when we exceed this (reduces operations dramatically)

export async function addPriceTick(mint: string, tickData: any): Promise<void> {
  const key = `price:${mint}`;

  try {
    // Push new tick (newest at the end with RPUSH)
    const newLength = await redis.rpush(key, JSON.stringify(tickData));

    // Only trim when we hit the buffer threshold (very efficient)
    if (newLength > TRIM_BUFFER) {
      await redis.ltrim(key, -MAX_PRICE_TICKS, -1);
      console.log(`[Price Cap] ${mint} trimmed to ${MAX_PRICE_TICKS} ticks (was ${newLength})`);
    }
  } catch (err: any) {
    console.error(`[Price Cap] Error for ${mint}:`, err.message);
  }
}
