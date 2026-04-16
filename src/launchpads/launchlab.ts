// src/launchpads/launchlab.ts — Raydium LaunchLab parser

import { PublicKey } from "@solana/web3.js";
import { LaunchpadParser, CurveState } from "./types";
import { extractMetadataFromTx, detectSwapFromDelta, readU64, parseNameSymbolUri, collectInstructions } from "./shared";

export const LAUNCHLAB_PROGRAM_ID = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";
const PROGRAM_ID = LAUNCHLAB_PROGRAM_ID;
const GRADUATION_TARGET_LAMPORTS = 100_000_000_000n; // 100 SOL

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
    return extractMetadataFromTx(logs, message, meta);
  },

  deriveCurvePDA(mint) {
    try {
      // LaunchLab uses "launch-pool" — different from every other platform's "bonding-curve"
      const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("launch-pool"), new PublicKey(mint).toBuffer()],
        new PublicKey(PROGRAM_ID),
      );
      return pda.toBase58();
    } catch { return ""; }
  },

  parseCurveData(data) {
    // Raydium LaunchLab layout — virtualSolReserves is at offset 24, NOT 16
    // This is a known difference from all other platforms.
    // virtualTokenReserves u64 @8
    // (gap / other field)  u64 @16
    // virtualSolReserves   u64 @24   ← shifted
    // realSolReserves      u64 @32   (if data.length >= 40)
    if (data.length < 32) return null;
    const virtualTokenReserves = readU64(data, 8);
    const virtualSolReserves   = readU64(data, 24);
    const realTokenReserves    = 0n;
    const realSolReserves      = data.length >= 40 ? readU64(data, 32) : 0n;
    const curvePercentage      = Math.min(100, Number(virtualSolReserves * 100n / GRADUATION_TARGET_LAMPORTS));
    const complete             = curvePercentage >= 100;
    return { virtualTokenReserves, virtualSolReserves, realTokenReserves, realSolReserves, complete, curvePercentage };
  },
};
