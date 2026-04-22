// src/trade-poller.ts — RPC-based trade polling for global (Raydium) tokens
// Since our Geyser gRPC streams are maxed out (5/5), we poll recent transactions
// on Raydium pool addresses via getSignaturesForAddress + getParsedTransaction
// to extract swap volume data for candlestick charts.
//
// Strategy:
//   - Every 15s, iterate all globally-watched tokens
//   - For each pool, fetch recent signatures since last poll
//   - Parse each transaction to extract SOL amount and direction (buy/sell)
//   - Write to trades:{mint} sorted set (same format as launchpad trades)
//
// RPC budget: ~3 calls per token per cycle (signatures + batch parsed txs)
// At 200 tokens max, that's ~600 calls/15s = 40 rps — well within RPC limits.

import { Connection, PublicKey, ParsedTransactionWithMeta } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

const redis      = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const RAYDIUM_V4_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const WSOL               = "So11111111111111111111111111111111111111112";

// Track the last signature we've seen per pool to avoid re-processing
const lastSignature = new Map<string, string>();

interface TradeRecord {
  signature: string;
  type:      "buy" | "sell";
  solAmount: number;
  timestamp: number; // epoch-ms
}

/**
 * Poll recent Raydium swaps for a single pool.
 * Returns the trades found since the last poll.
 */
export async function pollPoolTrades(
  poolAddress: string,
  mint: string,
): Promise<TradeRecord[]> {
  const trades: TradeRecord[] = [];

  try {
    // Fetch recent signatures — limit 20 per cycle (most pools don't do 20 swaps in 15s)
    const opts: any = { limit: 20 };
    const lastSig = lastSignature.get(poolAddress);
    if (lastSig) opts.until = lastSig; // only fetch newer than last seen

    const signatures = await connection.getSignaturesForAddress(
      new PublicKey(poolAddress),
      opts,
    );

    if (signatures.length === 0) return trades;

    // Store the newest signature for next poll
    lastSignature.set(poolAddress, signatures[0].signature);

    // Filter out failed txs and get parsed transactions in batch
    const validSigs = signatures
      .filter(s => !s.err)
      .map(s => s.signature);

    if (validSigs.length === 0) return trades;

    // Batch fetch parsed transactions (max 5 at a time to stay under RPC limits)
    const BATCH = 5;
    for (let i = 0; i < validSigs.length; i += BATCH) {
      const batch = validSigs.slice(i, i + BATCH);
      const txResults = await connection.getParsedTransactions(batch, {
        maxSupportedTransactionVersion: 0,
      });

      for (let j = 0; j < txResults.length; j++) {
        const tx = txResults[j];
        if (!tx?.meta || tx.meta.err) continue;

        const trade = extractSwapFromParsedTx(tx, mint, batch[j]);
        if (trade) trades.push(trade);
      }
    }
  } catch (e: any) {
    // Silent — individual poll failures are expected
    if (!e.message?.includes("429")) {
      console.warn(`[TradePoller] Poll failed for pool ${poolAddress.slice(0, 12)}:`, e.message);
    }
  }

  return trades;
}

/**
 * Extract a swap event from a parsed Raydium transaction.
 *
 * For Raydium AMM V4 swaps, we detect direction by looking at the signer's
 * SOL balance change (preBalances[0] - postBalances[0]):
 *   - Positive delta (SOL decreased) = buy (spent SOL to get tokens)
 *   - Negative delta (SOL increased) = sell (sold tokens to get SOL)
 *
 * SOL amount is the absolute value of the signer's lamport change minus fees.
 */
function extractSwapFromParsedTx(
  tx: ParsedTransactionWithMeta,
  mint: string,
  signature: string,
): TradeRecord | null {
  const meta = tx.meta!;

  // Check if this tx actually involves the Raydium program
  const accountKeys = tx.transaction.message.accountKeys.map(k =>
    typeof k === "string" ? k : k.pubkey.toBase58()
  );
  if (!accountKeys.some(k => k === RAYDIUM_V4_PROGRAM)) return null;

  // Check if the token mint is involved in this tx (via token balances)
  const postTokenBalances = meta.postTokenBalances || [];
  const hasOurMint = postTokenBalances.some(b => b.mint === mint);
  if (!hasOurMint) return null;

  // Signer's SOL delta
  const preBalances  = meta.preBalances;
  const postBalances = meta.postBalances;
  if (!preBalances?.length || !postBalances?.length) return null;

  const fee = meta.fee || 5000;
  const signerDelta = preBalances[0] - postBalances[0]; // positive = SOL spent

  // Net SOL (excluding fees)
  const netLamports = Math.abs(signerDelta) - fee;
  if (netLamports <= 0) return null;

  const solAmount = netLamports / 1_000_000_000;

  // Direction: if signer spent SOL, they bought tokens; if gained SOL, they sold
  const type: "buy" | "sell" = signerDelta > 0 ? "buy" : "sell";

  // Timestamp from blockTime (seconds → ms)
  const timestamp = tx.blockTime ? tx.blockTime * 1000 : Date.now();

  return { signature, type, solAmount, timestamp };
}

/**
 * Record a batch of trades into Redis (same format as launchpad trades).
 * Format: "type:solAmount:timestamp" scored by timestamp.
 */
export async function recordGlobalTrades(mint: string, trades: TradeRecord[]) {
  if (trades.length === 0) return;

  const now = Date.now();

  // Use individual ZADD NX to detect genuinely new trades before incrementing counters.
  // This prevents double-counting if the same trades are polled again after a restart.
  const pipe = redis.pipeline();
  const members: string[] = [];

  for (const trade of trades) {
    const member = `${trade.type}:${trade.solAmount}:${trade.timestamp}`;
    members.push(member);
    // NX = only add if member doesn't already exist; returns 1 if added, 0 if exists
    pipe.zadd(`trades:${mint}`, "NX", trade.timestamp, member);
  }

  const results = await pipe.exec();

  // Now increment counters only for genuinely new trades
  const counterPipe = redis.pipeline();
  let newCount = 0;

  for (let i = 0; i < trades.length; i++) {
    const wasAdded = results?.[i]?.[1] === 1;
    if (wasAdded) {
      newCount++;
      const trade = trades[i];
      if (trade.type === "buy") counterPipe.hincrby(`token:${mint}`, "buys", 1);
      else                       counterPipe.hincrby(`token:${mint}`, "sells", 1);
      counterPipe.hincrby(`token:${mint}`, "totalTxns", 1);
      counterPipe.hincrbyfloat(`token:${mint}`, "volume", trade.solAmount);
    }
  }

  // Trim trades older than 24h
  counterPipe.zremrangebyscore(`trades:${mint}`, 0, now - 86_400_000);
  if (newCount > 0) await counterPipe.exec();
}
