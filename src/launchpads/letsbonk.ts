// src/launchpads/letsbonk.ts — LetsBonk parser
//
// LetsBonk launches through the Raydium LaunchLab program (LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj).
// The definitive fingerprint is the presence of the LetsBonk PlatformConfig account
// (FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1) in the transaction's account list.
// detectParser() in index.ts checks for this account BEFORE the generic LaunchLab path.
//
// programId is set to the config account (not the LaunchLab program) so that
// LAUNCHPAD_PROGRAM_IDS includes it and Yellowstone subscribes to txs that reference it.
// deriveCurvePDA uses the LaunchLab program with the "launch-pool" seed — same as LaunchLab.

import { PublicKey } from "@solana/web3.js";
import { LaunchpadParser, CurveState } from "./types";
import { extractMetadataFromTx, detectSwapFromDelta, extractPoolFromCreateTx, readU64, parseNameSymbolUri, collectInstructions } from "./shared";

/** LetsBonk PlatformConfig account — the on-chain fingerprint used to distinguish
 *  LetsBonk txs from generic Raydium LaunchLab txs. */
export const LETSBONK_CONFIG_ACCOUNT = "FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1";

const LAUNCHLAB_PROGRAM_ID   = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";
const GRADUATION_TARGET_LAMPORTS = 100_000_000_000n; // 100 SOL
const MAX_SOL_RESERVE = 10_000n * 1_000_000_000n; // guard SOL-only; token reserves can legitimately hit 10^15

export const LetsBonkParser: LaunchpadParser = {
  id:             "letsbonk",
  // Set to config account so LAUNCHPAD_PROGRAM_IDS includes it for Yellowstone subscription.
  // detectParser() uses the account-presence check, not this field, for actual detection.
  programId:      LETSBONK_CONFIG_ACCOUNT,
  strictMetadata: false,

  isCreate(logs, _message, _meta) {
    // ONLY check — initialize_v2 is the exact creation instruction Raydium LaunchLab emits.
    // All fallbacks ("Create", "InitializePool", "LaunchPool") have been removed: they match
    // migrations, buys, and historical txs, causing false positives on old tokens.
    return logs.some(l => l.includes("Instruction: InitializeV2"));
  },

  detectSwap(logs, meta) {
    for (const l of logs) {
      if (l.includes("Instruction: Buy")  || l.includes("Instruction: buy"))  return "buy";
      if (l.includes("Instruction: Sell") || l.includes("Instruction: sell")) return "sell";
    }
    return detectSwapFromDelta(meta);
  },

  parseMetadata(logs, message, meta) {
    // initialize_v2 layout: [8-byte Anchor disc][u8 decimals][u32+name][u32+symbol][u32+uri]
    // Name/symbol are at offset 9, not 8. Also try offset 1 for Metaplex inner instructions
    // (CreateMetadataAccountV3 uses a 1-byte discriminator).
    for (const ix of collectInstructions(message, meta)) {
      const d = Buffer.from(ix.data || []);
      const result = parseNameSymbolUri(d, 9) ?? parseNameSymbolUri(d, 1);
      if (result) return result;
    }
    return extractMetadataFromTx(logs, message, meta, LAUNCHLAB_PROGRAM_ID);
  },

  deriveCurvePDA(mint, message?, meta?) {
    // Primary: extract pool account from InitializeV2 instruction accounts.
    if (message && meta) {
      // Same Raydium LaunchLab InitializeV2 layout as LaunchLab — pool PDA at index 5.
      const extracted = extractPoolFromCreateTx(LAUNCHLAB_PROGRAM_ID, mint, "LetsBonk", 5, message, meta);
      if (extracted) return extracted;
    }

    // Seed fallback (used for REST refreshes or if tx extraction fails)
    try {
      const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("launch-pool"), new PublicKey(mint).toBuffer()],
        new PublicKey(LAUNCHLAB_PROGRAM_ID),
      );
      return pda.toBase58();
    } catch { return ""; }
  },

  parseCurveData(data): CurveState | null {
    // Same VirtualPool layout as LaunchLab — LetsBonk uses the same Raydium LaunchLab program.
    // See launchlab.ts for full field-offset reference.
    if (data.length < 77) return null;
    const virtualTokenReserves = readU64(data, 37);
    const virtualSolReserves   = readU64(data, 45);
    const realTokenReserves    = readU64(data, 53);
    const realSolReserves      = readU64(data, 61);
    const totalFundRaisingB    = readU64(data, 69);
    if (virtualSolReserves === 0n || virtualTokenReserves === 0n) return null;
    if (virtualSolReserves > MAX_SOL_RESERVE) return null;
    const complete        = data[17] !== 0;
    const denominator     = totalFundRaisingB > 0n ? totalFundRaisingB : GRADUATION_TARGET_LAMPORTS;
    const curvePercentage = Math.min(100, Number(realSolReserves * 100n / denominator));
    return { virtualTokenReserves, virtualSolReserves, realTokenReserves, realSolReserves, complete, curvePercentage };
  },
};
