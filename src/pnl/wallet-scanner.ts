// src/pnl/wallet-scanner.ts — Fetch and parse wallet trade history from Solana RPC
// Scans a wallet's transaction history to extract all token swap events
// across all supported platforms (Pump, Moon, Meteora, Raydium, etc.)
//
// Strategy:
//   1. getSignaturesForAddress — fetch recent signatures for the wallet
//   2. getParsedTransactions — batch fetch parsed tx data
//   3. Extract token balance deltas + SOL balance deltas to identify swaps
//   4. Normalize into NormalizedTrade events for the PnL engine
//
// Uses parsed transactions so we can read token mint + decimals directly
// from the pre/postTokenBalances arrays without manual deserialization.

import { Connection, PublicKey, ParsedTransactionWithMeta } from "@solana/web3.js";
import { NormalizedTrade } from "./types";
import Redis from "ioredis";
import "dotenv/config";

const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");
const redis      = new Redis(process.env.REDIS_URL!);

const WSOL = "So11111111111111111111111111111111111111112";

// Known DEX programs whose transactions are swap-like
const SWAP_PROGRAMS = new Set([
  "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium AMM V4
  "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  // Pump.fun
  "MoonCVVNZFSYkqNXP6bxHLPl6A4JInA9ccjfmqr2wnb",  // Moon.it (Moonshot)
  "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",  // Meteora DLMM
  "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",  // Meteora DBC
  "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",  // LaunchLab
  "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",  // Jupiter V6
  "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  // Orca Whirlpool
]);

/**
 * Scan a wallet's recent transaction history and extract normalized trade events.
 * Caches results in Redis to avoid re-scanning.
 *
 * @param wallet  Base58 wallet address
 * @param maxTxs  Maximum number of transactions to scan (default 200)
 * @returns Array of NormalizedTrade events, newest first
 */
export async function scanWalletTrades(
  wallet: string,
  maxTxs: number = 200,
): Promise<NormalizedTrade[]> {
  // Check if we have cached trades
  const cacheKey = `wallet:trades:${wallet}`;
  const cached = await redis.get(cacheKey);
  if (cached) {
    return JSON.parse(cached);
  }

  const trades: NormalizedTrade[] = [];

  try {
    // Fetch signatures in chunks
    let allSignatures: string[] = [];
    let before: string | undefined;

    while (allSignatures.length < maxTxs) {
      const batch = await connection.getSignaturesForAddress(
        new PublicKey(wallet),
        { limit: Math.min(100, maxTxs - allSignatures.length), before },
      );

      if (batch.length === 0) break;

      const valid = batch.filter(s => !s.err);
      allSignatures.push(...valid.map(s => s.signature));
      before = batch[batch.length - 1].signature;

      // If we got fewer than requested, we've reached the end
      if (batch.length < 100) break;
    }

    if (allSignatures.length === 0) {
      await redis.setex(cacheKey, 60, "[]"); // cache empty result briefly
      return [];
    }

    // Fetch SOL/USD price for USD conversion
    let solPrice = 140;
    try {
      const res = await fetch("https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112");
      const json: any = await res.json();
      solPrice = json["So11111111111111111111111111111111111111112"]?.usdPrice ?? 140;
    } catch {}

    // Batch fetch parsed transactions (5 at a time)
    const BATCH = 5;
    for (let i = 0; i < allSignatures.length; i += BATCH) {
      const batch = allSignatures.slice(i, i + BATCH);
      const txResults = await connection.getParsedTransactions(batch, {
        maxSupportedTransactionVersion: 0,
      });

      for (let j = 0; j < txResults.length; j++) {
        const tx = txResults[j];
        if (!tx?.meta || tx.meta.err) continue;

        const extracted = extractTradesFromParsedTx(tx, wallet, batch[j], solPrice);
        trades.push(...extracted);
      }
    }

    // Sort newest first
    trades.sort((a, b) => b.timestamp - a.timestamp);

    // Cache for 2 minutes (wallet PnL data is semi-live)
    await redis.setex(cacheKey, 120, JSON.stringify(trades));

  } catch (e: any) {
    console.error(`[WalletScanner] Scan failed for ${wallet.slice(0, 12)}:`, e.message);
  }

  return trades;
}

/**
 * Extract all token swap events from a single parsed transaction.
 *
 * For each token mint involved:
 *   - Compute the wallet's token balance change (post - pre)
 *   - Compute the wallet's SOL balance change (pre - post, adjusted for fees)
 *   - If both are non-zero with opposite signs → it's a swap
 *   - Positive token delta + negative SOL delta = buy
 *   - Negative token delta + positive SOL delta = sell
 */
function extractTradesFromParsedTx(
  tx: ParsedTransactionWithMeta,
  wallet: string,
  signature: string,
  solPrice: number,
): NormalizedTrade[] {
  const trades: NormalizedTrade[] = [];
  const meta = tx.meta!;

  // Check if any swap program is involved
  const accountKeys = tx.transaction.message.accountKeys.map(k =>
    typeof k === "string" ? k : k.pubkey.toBase58()
  );
  const hasSwapProgram = accountKeys.some(k => SWAP_PROGRAMS.has(k));
  if (!hasSwapProgram) return trades;

  // Find the wallet's signer index
  const signerIndex = accountKeys.indexOf(wallet);
  if (signerIndex < 0) return trades; // wallet not a signer in this tx

  // Compute SOL delta for the signer
  const preBalances  = meta.preBalances || [];
  const postBalances = meta.postBalances || [];
  const fee = meta.fee || 5000;
  const solDeltaLamports = postBalances[signerIndex] - preBalances[signerIndex] + fee;
  // solDeltaLamports: positive = gained SOL (sell), negative = spent SOL (buy)

  // Build a map of mint → token delta for this wallet
  const tokenDeltas = new Map<string, { delta: number; decimals: number }>();
  const preTokenBalances  = meta.preTokenBalances || [];
  const postTokenBalances = meta.postTokenBalances || [];

  // Index pre-balances by account index
  const preByAccount = new Map<number, { mint: string; amount: string; decimals: number }>();
  for (const b of preTokenBalances) {
    if (b.owner === wallet) {
      preByAccount.set(b.accountIndex, {
        mint: b.mint,
        amount: b.uiTokenAmount?.amount || "0",
        decimals: b.uiTokenAmount?.decimals || 0,
      });
    }
  }

  // Index post-balances by account index
  const postByAccount = new Map<number, { mint: string; amount: string; decimals: number }>();
  for (const b of postTokenBalances) {
    if (b.owner === wallet) {
      postByAccount.set(b.accountIndex, {
        mint: b.mint,
        amount: b.uiTokenAmount?.amount || "0",
        decimals: b.uiTokenAmount?.decimals || 0,
      });
    }
  }

  // Merge all account indices
  const allIndices = new Set([...preByAccount.keys(), ...postByAccount.keys()]);
  for (const idx of allIndices) {
    const pre  = preByAccount.get(idx);
    const post = postByAccount.get(idx);
    const mint = post?.mint || pre?.mint;
    if (!mint || mint === WSOL) continue; // skip wrapped SOL

    const preAmount  = BigInt(pre?.amount || "0");
    const postAmount = BigInt(post?.amount || "0");
    const delta = Number(postAmount - preAmount);
    const decimals = post?.decimals || pre?.decimals || 6;

    if (delta === 0) continue;

    const existing = tokenDeltas.get(mint);
    if (existing) {
      existing.delta += delta;
    } else {
      tokenDeltas.set(mint, { delta, decimals });
    }
  }

  // Each non-zero token delta paired with SOL delta = a swap event
  const timestamp = tx.blockTime ? tx.blockTime * 1000 : Date.now();

  for (const [mint, { delta, decimals }] of tokenDeltas) {
    if (delta === 0) continue;

    const tokenAmountUI = Math.abs(delta) / Math.pow(10, decimals);
    const solAmountUI   = Math.abs(solDeltaLamports) / 1_000_000_000;

    // Direction: positive token delta = buy (got tokens), negative = sell (lost tokens)
    const type: "buy" | "sell" = delta > 0 ? "buy" : "sell";

    // Sanity check: buy should spend SOL (negative delta), sell should gain SOL
    // If signs don't match the expected pattern, skip (might be a transfer, not a swap)
    if (type === "buy" && solDeltaLamports > 0) continue;
    if (type === "sell" && solDeltaLamports < 0) continue;

    trades.push({
      signature,
      mint,
      type,
      solAmount: solAmountUI,
      tokenAmount: tokenAmountUI,
      priceUsd: solAmountUI * solPrice,
      timestamp,
    });
  }

  return trades;
}

/**
 * Invalidate the cached trades for a wallet.
 * Call this when we detect a new trade for this wallet in real-time.
 */
export async function invalidateWalletCache(wallet: string) {
  await redis.del(`wallet:trades:${wallet}`);
  await redis.del(`wallet:pnl:${wallet}`);
}
