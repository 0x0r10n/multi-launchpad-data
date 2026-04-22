// src/launchpads/raydium.ts — Raydium AMM V4 pool decoder
// Unlike bonding curve parsers, Raydium AMM pools do NOT store reserves inline.
// The pool account stores vault Pubkeys (coin_vault, pc_vault) and the actual
// reserves live in separate SPL Token accounts.
//
// However, for Geyser-push decoding we receive the POOL account data, not the
// vault accounts. So parseCurveData cannot compute price from this account alone.
//
// Instead, the global-tracker uses fetchAndDecodeCurve() which calls this parser,
// and we use a DIFFERENT strategy: read the coin_decimals, pc_decimals, and the
// vault pubkeys from the pool data, then fetch vault balances via getTokenAccountBalance.
//
// For the parseCurveData interface (called from processCurveAccountUpdate when Geyser
// pushes a pool account update), we return a sentinel CurveState that signals
// "re-fetch needed" — the caller then does an RPC read of the vault accounts.

import { PublicKey, Connection } from "@solana/web3.js";
import { LaunchpadParser, CurveState } from "./types";
import { readU64 } from "./shared";
import "dotenv/config";

export const RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

// Raydium AMM V4 AmmInfo layout offsets (from state.rs)
// Total size: 752 bytes
const OFFSET = {
  status:          0,   // u64
  nonce:           8,   // u64
  coin_decimals:   32,  // u64
  pc_decimals:     40,  // u64
  coin_vault:      336, // Pubkey (32 bytes) — SPL Token account holding base tokens
  pc_vault:        368, // Pubkey (32 bytes) — SPL Token account holding quote tokens
  coin_vault_mint: 400, // Pubkey (32 bytes) — baseMint
  pc_vault_mint:   432, // Pubkey (32 bytes) — quoteMint
  lp_mint:         464, // Pubkey (32 bytes)
} as const;

const WSOL = "So11111111111111111111111111111111111111112";

/**
 * Decode the Raydium AMM V4 pool account and fetch vault balances via RPC.
 * This is used by the global-tracker's initial price seed and by curve-tracker's
 * processCurveAccountUpdate path.
 *
 * Returns { virtualTokenReserves, virtualSolReserves, ... } in the same CurveState
 * shape as bonding curve parsers, so curve-tracker can compute price identically.
 */
export async function decodeRaydiumPoolWithVaults(
  data: Buffer,
  conn: Connection,
): Promise<CurveState | null> {
  if (data.length < 752) return null;

  const status = readU64(data, OFFSET.status);
  // Accept SwapOnly (6) and WaitingTrade (7) — both have valid reserves
  if (status < 6n && status !== 1n) return null;

  const coinDecimals = Number(readU64(data, OFFSET.coin_decimals));
  const pcDecimals   = Number(readU64(data, OFFSET.pc_decimals));

  const coinVault = new PublicKey(data.slice(OFFSET.coin_vault, OFFSET.coin_vault + 32));
  const pcVault   = new PublicKey(data.slice(OFFSET.pc_vault, OFFSET.pc_vault + 32));
  const quoteMint = new PublicKey(data.slice(OFFSET.pc_vault_mint, OFFSET.pc_vault_mint + 32)).toBase58();

  // Fetch vault balances via RPC
  const [coinBalance, pcBalance] = await Promise.all([
    conn.getTokenAccountBalance(coinVault),
    conn.getTokenAccountBalance(pcVault),
  ]);

  const coinAmount = BigInt(coinBalance.value.amount);
  const pcAmount   = BigInt(pcBalance.value.amount);

  if (coinAmount === 0n || pcAmount === 0n) return null;

  // Determine which side is SOL
  const isQuoteSol = quoteMint === WSOL;

  const virtualTokenReserves = isQuoteSol ? coinAmount : pcAmount;
  const virtualSolReserves   = isQuoteSol ? pcAmount   : coinAmount;

  return {
    virtualTokenReserves,
    virtualSolReserves,
    realTokenReserves: virtualTokenReserves,
    realSolReserves:   virtualSolReserves,
    complete: true,
    curvePercentage: 100,
  };
}

/**
 * LaunchpadParser interface for Raydium AMM V4.
 *
 * parseCurveData returns null because the pool account doesn't contain reserves inline.
 * Use decodeRaydiumPoolWithVaults() instead for actual price computation.
 */
export const RaydiumAmmParser: LaunchpadParser = {
  id: "raydium",
  programId: RAYDIUM_AMM_V4_PROGRAM_ID,
  strictMetadata: false,

  isCreate() { return false; },
  detectSwap() { return null; },
  parseMetadata() { return { name: "", symbol: "", uri: "" }; },
  deriveCurvePDA() { return ""; },

  parseCurveData(data: Buffer): CurveState | null {
    // Raydium AMM pool data does NOT contain reserves inline — they live in
    // separate vault token accounts. We can't compute price from this buffer alone.
    //
    // When Geyser pushes a pool account update for a raydium-tracked token,
    // the curve-tracker will get null here, which triggers the global-tracker's
    // fallback RPC path to re-read vault balances.
    //
    // This is acceptable because Geyser account pushes on the pool happen on
    // config changes (rare), not on every swap. For Raydium, the vault accounts
    // change on every swap — but we're subscribing to the POOL account (which
    // has the vault pubkeys), not the vault accounts themselves.
    //
    // The global-tracker handles this by running a periodic poll loop instead.
    return null;
  },
};

/**
 * Extract basic pool info from raw account data without RPC calls.
 * Used for metadata extraction and pool identification.
 */
export function parseRaydiumPoolInfo(data: Buffer): {
  baseMint: string;
  quoteMint: string;
  coinVault: string;
  pcVault: string;
  coinDecimals: number;
  pcDecimals: number;
  status: bigint;
} | null {
  if (data.length < 752) return null;
  return {
    baseMint:     new PublicKey(data.slice(OFFSET.coin_vault_mint, OFFSET.coin_vault_mint + 32)).toBase58(),
    quoteMint:    new PublicKey(data.slice(OFFSET.pc_vault_mint, OFFSET.pc_vault_mint + 32)).toBase58(),
    coinVault:    new PublicKey(data.slice(OFFSET.coin_vault, OFFSET.coin_vault + 32)).toBase58(),
    pcVault:      new PublicKey(data.slice(OFFSET.pc_vault, OFFSET.pc_vault + 32)).toBase58(),
    coinDecimals: Number(readU64(data, OFFSET.coin_decimals)),
    pcDecimals:   Number(readU64(data, OFFSET.pc_decimals)),
    status:       readU64(data, OFFSET.status),
  };
}
