// src/launchpads/launchlab.ts — Raydium LaunchLab parser

import { PublicKey } from "@solana/web3.js";
import { LaunchpadParser } from "./types";
import { extractMetadataFromTx, detectSwapFromDelta, extractPoolFromCreateTx, readU64, parseNameSymbolUri, collectInstructions } from "./shared";

export const LAUNCHLAB_PROGRAM_ID = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";
const PROGRAM_ID = LAUNCHLAB_PROGRAM_ID;
const GRADUATION_TARGET_LAMPORTS = 100_000_000_000n; // 100 SOL
const MAX_SOL_RESERVE = 10_000n * 1_000_000_000n; // guard SOL-only; token reserves can legitimately hit 10^15

export const LaunchLabParser: LaunchpadParser = {
  id:             "launchlab",
  programId:      PROGRAM_ID,
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
    return extractMetadataFromTx(logs, message, meta, PROGRAM_ID);
  },

  deriveCurvePDA(mint, message?, meta?) {
    // Primary: extract pool account from InitializeV2 instruction accounts.
    if (message && meta) {
      // LaunchLab InitializeV2 account layout (verified from on-chain tx):
      //   [0] payer  [1] creator  [2] globalConfig  [3] platformConfig  [4] authority  [5] poolState  [6] baseMint ...
      const extracted = extractPoolFromCreateTx(PROGRAM_ID, mint, "LaunchLab", 5, message, meta);
      if (extracted) return extracted;
    }

    // Seed fallback (used for REST refreshes or if tx extraction fails)
    try {
      const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("launch-pool"), new PublicKey(mint).toBuffer()],
        new PublicKey(PROGRAM_ID),
      );
      return pda.toBase58();
    } catch { return ""; }
  },

  parseCurveData(data) {
    // Raydium LaunchLab VirtualPool account layout (Borsh-packed, from SDK layout.ts):
    // [0:8]   discriminator (unnamed u64)
    // [8:16]  epoch (u64)
    // [16]    bump (u8)
    // [17]    status (u8) — 0=active, non-zero=migrated
    // [18]    mintDecimalsA, [19] mintDecimalsB, [20] migrateType
    // [21:29] supply (u64)
    // [29:37] totalSellA (u64)
    // [37:45] virtualA (u64) ← virtualTokenReserves
    // [45:53] virtualB (u64) ← virtualSolReserves
    // [53:61] realA (u64)    ← realTokenReserves
    // [61:69] realB (u64)    ← realSolReserves
    // [69:77] totalFundRaisingB (u64) ← graduation target in lamports
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
