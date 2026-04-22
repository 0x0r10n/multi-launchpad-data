// src/pool-resolver.ts — Find Raydium AMM V4 pool for any SPL token via pure RPC
// No third-party APIs. Uses getProgramAccounts with memcmp filters to match
// the baseMint (offset 400) or quoteMint (offset 432) in the 752-byte AMM layout.
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");
const redis      = new Redis(process.env.REDIS_URL!);

const RAYDIUM_V4 = new PublicKey("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
const WSOL       = "So11111111111111111111111111111111111111112";

export interface PoolInfo {
  poolAddress: string;
  poolType:    "raydium-amm-v4";
  programId:   string;
  baseMint:    string;
  quoteMint:   string;
}

/**
 * Finds the Raydium AMM V4 pool for a given mint.
 *
 * Strategy:
 *   1. Check Redis cache `pool:{mint}` first (saves an RPC call on re-watch)
 *   2. Query getProgramAccounts with baseMint filter (offset 400, 32 bytes)
 *   3. If no result, query with quoteMint filter (offset 432, 32 bytes)
 *   4. Among results, prefer pools paired with WSOL (highest liquidity)
 *   5. Cache the result in Redis for 24h
 *
 * Returns null if no pool exists on-chain.
 */
export async function findRaydiumPool(mint: string): Promise<PoolInfo | null> {
  // ── Redis cache ──────────────────────────────────────────────────────────
  const cached = await redis.hgetall(`pool:${mint}`);
  if (cached?.poolAddress) {
    return {
      poolAddress: cached.poolAddress,
      poolType:    "raydium-amm-v4",
      programId:   cached.programId || RAYDIUM_V4.toBase58(),
      baseMint:    cached.baseMint  || mint,
      quoteMint:   cached.quoteMint || WSOL,
    };
  }

  try {
    // ── Search as baseMint (offset 400) ──────────────────────────────────
    let accounts = await connection.getProgramAccounts(RAYDIUM_V4, {
      filters: [
        { dataSize: 752 },
        { memcmp: { offset: 400, bytes: mint } },
      ],
      // Only need the pubkey + enough data to read quoteMint (offset 432, 32 bytes)
      dataSlice: { offset: 432, length: 32 },
    });

    // Prefer WSOL-paired pools
    let best = pickBestPool(accounts, mint, "base");

    if (!best) {
      // ── Search as quoteMint (offset 432) ───────────────────────────────
      accounts = await connection.getProgramAccounts(RAYDIUM_V4, {
        filters: [
          { dataSize: 752 },
          { memcmp: { offset: 432, bytes: mint } },
        ],
        dataSlice: { offset: 400, length: 32 },
      });
      best = pickBestPool(accounts, mint, "quote");
    }

    if (!best) return null;

    // ── Cache for 24h ────────────────────────────────────────────────────
    const info: PoolInfo = {
      poolAddress: best.poolAddress,
      poolType:    "raydium-amm-v4",
      programId:   RAYDIUM_V4.toBase58(),
      baseMint:    best.baseMint,
      quoteMint:   best.quoteMint,
    };
    await redis.hset(`pool:${mint}`, {
      poolAddress: info.poolAddress,
      poolType:    info.poolType,
      programId:   info.programId,
      baseMint:    info.baseMint,
      quoteMint:   info.quoteMint,
    });
    await redis.expire(`pool:${mint}`, 86400);

    console.log(`[PoolResolver] Found pool for ${mint.slice(0, 12)}: ${info.poolAddress.slice(0, 12)} (${info.poolType})`);
    return info;

  } catch (err: any) {
    console.error(`[PoolResolver] RPC error for ${mint.slice(0, 12)}:`, err.message);
    return null;
  }
}

function pickBestPool(
  accounts: ReadonlyArray<{ pubkey: PublicKey; account: { data: Buffer } }>,
  mint: string,
  role: "base" | "quote",
): { poolAddress: string; baseMint: string; quoteMint: string } | null {
  if (accounts.length === 0) return null;

  // Each account's dataSlice is the 32-byte counterpart mint
  for (const acc of accounts) {
    const counterMint = new PublicKey(acc.account.data.slice(0, 32)).toBase58();
    // Prefer WSOL pairing
    if (counterMint === WSOL) {
      return {
        poolAddress: acc.pubkey.toBase58(),
        baseMint:  role === "base" ? mint : counterMint,
        quoteMint: role === "base" ? counterMint : mint,
      };
    }
  }

  // No WSOL pool — take the first result
  const acc = accounts[0];
  const counterMint = new PublicKey(acc.account.data.slice(0, 32)).toBase58();
  return {
    poolAddress: acc.pubkey.toBase58(),
    baseMint:  role === "base" ? mint : counterMint,
    quoteMint: role === "base" ? counterMint : mint,
  };
}
