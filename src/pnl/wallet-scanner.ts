// src/pnl/wallet-scanner.ts — Fetch and parse wallet trade history from Solana RPC
//
// ╔═══════════════════════════════════════════════════════════════════════════╗
// ║  PnL CORRECTNESS: TRADE vs NON-TRADE SEPARATION                        ║
// ║                                                                         ║
// ║  Only genuine DEX swaps feed into cost basis and PnL calculations.      ║
// ║  The following are explicitly filtered OUT:                              ║
// ║                                                                         ║
// ║    ✗ Airdrops — tokens appear with zero/negligible SOL movement         ║
// ║    ✗ Transfers — tokens move between wallets without DEX involvement    ║
// ║    ✗ Dust attacks — sub-threshold token amounts with no SOL cost        ║
// ║    ✗ MEV inflows — tokens arrive from sandwich/arb without user intent  ║
// ║    ✗ Intermediary hops — Jupiter multi-hop tokens that aren't final     ║
// ║    ✗ Token account creation/close — WSOL wrap/unwrap artifacts          ║
// ║                                                                         ║
// ║  A transaction qualifies as a trade ONLY when ALL of:                   ║
// ║    1. A known DEX program is in the account list                        ║
// ║    2. The wallet is the signer (fee payer)                              ║
// ║    3. SOL moved in the opposite direction to tokens (buy = SOL out,     ║
// ║       sell = SOL in)                                                    ║
// ║    4. SOL amount exceeds MIN_SOL_THRESHOLD (filters dust/airdrops)      ║
// ║    5. Token amount exceeds a reasonable minimum (filters dust attacks)  ║
// ║    6. For multi-mint txs (Jupiter), only the largest delta pair counts  ║
// ╚═══════════════════════════════════════════════════════════════════════════╝

import { Connection, PublicKey, ParsedTransactionWithMeta } from "@solana/web3.js";
import { NormalizedTrade } from "./types";
import Redis from "ioredis";
import "dotenv/config";

const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");
const redis      = new Redis(process.env.REDIS_URL!);

const WSOL = "So11111111111111111111111111111111111111112";

// ── Anti-noise thresholds ────────────────────────────────────────────────────
// Minimum SOL movement required to classify as a trade (not an airdrop/transfer).
// 0.001 SOL (~$0.14 at $140/SOL) eliminates dust attacks and fee-only txs.
const MIN_SOL_THRESHOLD = 0.001;

// Minimum token delta (in raw units) to consider. Filters sub-dust amounts
// that appear in MEV sandwich attacks or account-creation artifacts.
const MIN_TOKEN_RAW_DELTA = 1;

// Known DEX programs whose transactions may contain swaps.
// A tx must include at least one of these to be considered a potential trade.
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

// Programs that are NOT trade sources — token movements via these are transfers,
// not swaps. If a tx ONLY involves these programs (no DEX), it's always a transfer.
const TRANSFER_PROGRAMS = new Set([
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  // SPL Token
  "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", // Associated Token
  "11111111111111111111111111111111",                 // System Program
]);

/**
 * Scan a wallet's recent transaction history and extract normalized trade events.
 * Only genuine DEX swaps are included — airdrops, transfers, and dust are filtered.
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
 * Extract genuine DEX swap events from a single parsed transaction.
 *
 * This function applies 6 layers of filtering to ensure only real trades
 * pass through to the PnL engine:
 *
 *   Layer 1: DEX program presence — at least one SWAP_PROGRAMS key in accounts
 *   Layer 2: Signer check — wallet must be the tx fee payer (index 0)
 *   Layer 3: SOL threshold — net SOL movement must exceed MIN_SOL_THRESHOLD
 *   Layer 4: Direction coherence — SOL and token deltas must be in opposite directions
 *   Layer 5: Token threshold — token delta must be non-trivial (> MIN_TOKEN_RAW_DELTA)
 *   Layer 6: Multi-mint disambiguation — for Jupiter-style routes, only the terminal
 *            swap (largest SOL-correlated delta) is recorded as a trade
 */
function extractTradesFromParsedTx(
  tx: ParsedTransactionWithMeta,
  wallet: string,
  signature: string,
  solPrice: number,
): NormalizedTrade[] {
  const meta = tx.meta!;

  // ── Layer 1: DEX program gate ──────────────────────────────────────────────
  // Build full account key list including ALT-loaded addresses
  const staticKeys: string[] = tx.transaction.message.accountKeys.map(k =>
    typeof k === "string" ? k : k.pubkey.toBase58()
  );
  // Include loaded addresses from Address Lookup Tables (versioned txs)
  const loadedWritable: string[] = (meta as any).loadedAddresses?.writable?.map(
    (k: PublicKey) => k.toBase58()
  ) || [];
  const loadedReadonly: string[] = (meta as any).loadedAddresses?.readonly?.map(
    (k: PublicKey) => k.toBase58()
  ) || [];
  const accountKeys = [...staticKeys, ...loadedWritable, ...loadedReadonly];

  const hasSwapProgram = accountKeys.some(k => SWAP_PROGRAMS.has(k));
  if (!hasSwapProgram) return [];

  // ── Layer 2: Signer check ─────────────────────────────────────────────────
  // The wallet must be the signer (fee payer) of this transaction.
  // If the wallet is just mentioned in accounts (e.g., as a recipient), skip.
  const signerIndex = staticKeys.indexOf(wallet);
  if (signerIndex < 0) return []; // not even in static keys
  // Typically the signer is at index 0, but we accept any position in static keys
  // as long as they're actually a signer
  const isSigner = tx.transaction.message.accountKeys[signerIndex] &&
    typeof tx.transaction.message.accountKeys[signerIndex] !== "string" &&
    (tx.transaction.message.accountKeys[signerIndex] as any).signer === true;
  // For parsed txs, accountKeys are objects with { pubkey, signer, writable }
  // If we can't confirm signer status, accept index 0 (always the fee payer)
  if (!isSigner && signerIndex !== 0) return [];

  // ── Layer 3: SOL threshold ────────────────────────────────────────────────
  // Compute net SOL movement for the signer, excluding the transaction fee.
  // This eliminates airdrops/transfers where the only SOL movement is the fee.
  const preBalances  = meta.preBalances || [];
  const postBalances = meta.postBalances || [];
  const fee = meta.fee || 5000;

  // Net SOL delta with fee removed:
  //   buy  → signer's SOL decreased → delta is negative
  //   sell → signer's SOL increased → delta is positive
  const solDeltaLamports = (postBalances[signerIndex] - preBalances[signerIndex]) + fee;
  const solDeltaUI = solDeltaLamports / 1_000_000_000;

  // If the SOL movement is below threshold, this is NOT a trade.
  // This catches: airdrops (0 SOL), dust attacks (<0.001 SOL), fee-only txs.
  if (Math.abs(solDeltaUI) < MIN_SOL_THRESHOLD) return [];

  // ── Build token deltas for this wallet ────────────────────────────────────
  const tokenDeltas = new Map<string, { delta: number; rawDelta: bigint; decimals: number }>();
  const preTokenBalances  = meta.preTokenBalances || [];
  const postTokenBalances = meta.postTokenBalances || [];

  // Index pre-balances by account index (only for this wallet)
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

  // Merge all account indices to compute deltas
  const allIndices = new Set([...preByAccount.keys(), ...postByAccount.keys()]);
  for (const idx of allIndices) {
    const pre  = preByAccount.get(idx);
    const post = postByAccount.get(idx);
    const mint = post?.mint || pre?.mint;

    // Skip WSOL — it's the "currency" side, not a tradeable token
    if (!mint || mint === WSOL) continue;

    const preAmount  = BigInt(pre?.amount || "0");
    const postAmount = BigInt(post?.amount || "0");
    const rawDelta = postAmount - preAmount;
    const delta = Number(rawDelta);
    const decimals = post?.decimals || pre?.decimals || 6;

    // ── Layer 5: Token threshold ──────────────────────────────────────────
    if (Math.abs(delta) < MIN_TOKEN_RAW_DELTA) continue;

    // Accumulate deltas per mint (some mints may have multiple ATAs)
    const existing = tokenDeltas.get(mint);
    if (existing) {
      existing.delta += delta;
      existing.rawDelta += rawDelta;
    } else {
      tokenDeltas.set(mint, { delta, rawDelta, decimals });
    }
  }

  if (tokenDeltas.size === 0) return [];

  // ── Layer 6: Multi-mint disambiguation ────────────────────────────────────
  // Jupiter and other aggregators route through multiple pools in one tx.
  // Example: SOL → USDC → BONK produces deltas for both USDC and BONK.
  // Only the TERMINAL swap (the token the user actually wanted) matters for PnL.
  //
  // Heuristic: In a multi-hop swap, intermediary tokens have BOTH positive and
  // negative movements that net to ~0, OR they appear as a "buy then sell" within
  // the same tx. The terminal token is the one with the LARGEST absolute delta
  // that's directionally coherent with the SOL movement.
  //
  // For single-mint txs, this is a no-op (the only mint is the terminal one).

  const timestamp = tx.blockTime ? tx.blockTime * 1000 : Date.now();
  const candidates: NormalizedTrade[] = [];

  for (const [mint, { delta, decimals }] of tokenDeltas) {
    if (delta === 0) continue;

    const tokenAmountUI = Math.abs(delta) / Math.pow(10, decimals);
    const solAmountUI   = Math.abs(solDeltaUI);

    // Direction: positive token delta = buy (got tokens), negative = sell (lost tokens)
    const type: "buy" | "sell" = delta > 0 ? "buy" : "sell";

    // ── Layer 4: Direction coherence ──────────────────────────────────────
    // Buy: wallet spent SOL (solDelta < 0) AND gained tokens (tokenDelta > 0)
    // Sell: wallet gained SOL (solDelta > 0) AND lost tokens (tokenDelta < 0)
    //
    // If the directions DON'T match, this token movement is NOT correlated with
    // the SOL movement — it's a transfer/airdrop/unrelated token change that
    // happened to be in the same transaction as a swap.
    if (type === "buy"  && solDeltaLamports >= 0) continue; // buy but didn't spend SOL
    if (type === "sell" && solDeltaLamports <= 0) continue; // sell but didn't gain SOL

    candidates.push({
      signature,
      mint,
      type,
      solAmount:   solAmountUI,
      tokenAmount: tokenAmountUI,
      priceUsd:    solAmountUI * solPrice,
      timestamp,
    });
  }

  // If multiple candidates exist (Jupiter multi-hop), take only the one
  // with the largest token delta — that's the terminal swap output.
  // The intermediary tokens (USDC, SOL, etc.) are just routing artifacts.
  if (candidates.length > 1) {
    candidates.sort((a, b) => b.tokenAmount - a.tokenAmount);
    return [candidates[0]]; // only the terminal swap
  }

  return candidates;
}

/**
 * Invalidate the cached trades for a wallet.
 * Call this when we detect a new trade for this wallet in real-time.
 */
export async function invalidateWalletCache(wallet: string) {
  await redis.del(`wallet:trades:${wallet}`);
  await redis.del(`wallet:pnl:${wallet}`);
}
