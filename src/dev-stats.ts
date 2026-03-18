import redis from './redis';
import { Connection, PublicKey } from '@solana/web3.js';
import dotenv from 'dotenv';

dotenv.config();

const RPC = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const connection = new Connection(RPC, 'confirmed');

export const devStatsCache = new Map<string, {launched: number, migrated: number}>(); // in-memory fast path

export async function updateDevStats(creator: string, action: 'launched' | 'migrated', io?: any) {
  const key = creator;
  let stats = devStatsCache.get(key) || { launched: 0, migrated: 0 };

  // If not in cache, fallback to Redis check
  if (!devStatsCache.has(key)) {
    const redisStats = await redis.hgetall(`dev_stats:${creator}`);
    if (redisStats && Object.keys(redisStats).length > 0) {
      stats = { 
        launched: parseInt(redisStats.total_launched || '0'), 
        migrated: parseInt(redisStats.total_migrated || '0') 
      };
    }
  }

  if (action === 'launched') stats.launched++;
  else stats.migrated++;

  devStatsCache.set(key, stats);

  // Persist to Redis
  await redis.hmset(`dev_stats:${creator}`, {
    creator,
    total_launched: stats.launched.toString(),
    total_migrated: stats.migrated.toString(),
    last_updated: Date.now().toString()
  });

  // Broadcast to specific wallet room
  if (io) {
    const payload = { type: 'dev-stats', wallet: creator, data: stats };
    io.to(`dev-stats:${creator}`).emit('message', payload);
    io.emit('message', payload); 
  }
}

export async function bootstrapDevStats(creator: string) {
  // 1. Idempotent Guard: Check for recent update (24 hour cooldown)
  const lastUpdated = await redis.hget(`dev_stats:${creator}`, 'last_updated');

  if (lastUpdated && Date.now() - parseInt(lastUpdated) < 86400000) {
    if (devStatsCache.has(creator)) return devStatsCache.get(creator)!;
    const redisStats = await redis.hgetall(`dev_stats:${creator}`);
    if (redisStats && Object.keys(redisStats).length > 0) {
      const result = { 
        launched: parseInt(redisStats.total_launched || '0'), 
        migrated: parseInt(redisStats.total_migrated || '0') 
      };
      devStatsCache.set(creator, result);
      return result;
    }
  }

  try {
    // 2. Standard RPC Bootstrap (Helius-free)
    const sigs = await connection.getSignaturesForAddress(new PublicKey(creator), { limit: 100 });
    
    let launched = 0, migrated = 0;

    for (const sigInfo of sigs) {
      const tx = await connection.getParsedTransaction(sigInfo.signature, { maxSupportedTransactionVersion: 0, commitment: 'confirmed' });
      if (!tx || !tx.meta) continue;

      const logs = (tx.meta.logMessages || []).join(' ').toLowerCase();
      
      const isCreate = 
        logs.includes('initialize') || 
        logs.includes('create') || 
        logs.includes('mint');

      const isComplete = 
        logs.includes('complete') || 
        logs.includes('graduated') || 
        logs.includes('migrate') ||
        (logs.includes('pool created') && logs.includes('raydium'));

      if (isCreate) launched++;
      if (isComplete) migrated++;
    }

    await redis.hmset(`dev_stats:${creator}`, {
      creator,
      total_launched: launched.toString(),
      total_migrated: migrated.toString(),
      last_updated: Date.now().toString()
    });

    const stats = { launched, migrated };
    devStatsCache.set(creator, stats);
    return stats;
  } catch (err: any) {
    console.error(`[DevStats] Bootstrap failed for ${creator}:`, err.message);
    return { launched: 0, migrated: 0 };
  }
}

