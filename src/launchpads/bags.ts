// src/launchpads/bags.ts — Meteora DBC parser (covers both bags.fm and generic Meteora DBC tokens)
//
// Detection is two-stage:
//   1. Program ID match → this parser is returned by detectParser() with id "meteora"
//   2. Mint suffix check in yellowstone-manager → if mint ends with "BAGS" the platform
//      field stored in Redis is overridden to "bags"; otherwise it stays "meteora"
//
// Both labels ("bags" and "meteora") share the same curve layout, PDA seed, and detection
// logic — only the id differs. Both are registered in the BY_ID map so getParser() works
// for either label when curve-tracker or risk-analyzer looks up a stored token.

import { LaunchpadParser, CurveState } from "./types";
import { extractMetadataFromTx, extractPoolFromCreateTx, detectSwapFromDelta, readU64 } from "./shared";

export const METEORA_DBC_PROGRAM_ID = "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN";

const GRADUATION_TARGET_LAMPORTS = 100_000_000_000n; // 100 SOL
const MAX_SOL_RESERVE = 10_000n * 1_000_000_000n; // guard SOL-only; token reserves can legitimately hit 10^15

// Meteora DBC emits these specific log strings on pool initialization.
// Log-only — Meteora DBC does not expose a standard Anchor discriminator.
const CREATE_LOG_SIGNALS = [
  "InitializeVirtualPool",
  "initialize_virtual_pool",
  "DBC: New Pool",
  "Meteora DBC: Initialize",
];

// Shared implementation — id is set per-export below
const meteora: Omit<LaunchpadParser, "id"> = {
  programId:      METEORA_DBC_PROGRAM_ID,
  strictMetadata: false,

  isCreate(logs, _message, _meta) {
    return logs.some(l => CREATE_LOG_SIGNALS.some(sig => l.includes(sig)));
  },

  detectSwap(logs, meta) {
    for (const l of logs) {
      if (l.includes("Instruction: Buy")  || l.includes("Instruction: buy"))  return "buy";
      if (l.includes("Instruction: Sell") || l.includes("Instruction: sell")) return "sell";
    }
    return detectSwapFromDelta(meta);
  },

  parseMetadata(logs, message, meta) {
    return extractMetadataFromTx(logs, message, meta, METEORA_DBC_PROGRAM_ID);
  },

  deriveCurvePDA(mint, message?, meta?) {
    // Meteora DBC VirtualPool PDA requires config key — not derivable from mint alone.
    // Use the bulletproof extraction logic with preferred index 5.
    if (message && meta) {
      const extracted = extractPoolFromCreateTx(
        METEORA_DBC_PROGRAM_ID, mint, "MeteoraDBC", 5, message, meta,
      );
      if (extracted) return extracted;
    }
    return ""; // tx context required; no seed fallback possible
  },

  parseCurveData(data): CurveState | null {
    // Meteora DBC VirtualPool account layout (Borsh-packed):
    // base_reserve (token) @ 232, quote_reserve (SOL) @ 240
    if (data.length < 248) return null;
    const virtualTokenReserves = readU64(data, 232);
    const virtualSolReserves   = readU64(data, 240);

    if (virtualSolReserves === 0n || virtualTokenReserves === 0n) return null;
    if (virtualSolReserves > MAX_SOL_RESERVE) return null;
    const complete        = data.length >= 306 ? data[305] !== 0 : false;
    const curvePercentage = Math.min(100, Number(virtualSolReserves * 100n / GRADUATION_TARGET_LAMPORTS));
    return { virtualTokenReserves, virtualSolReserves, realTokenReserves: 0n, realSolReserves: virtualSolReserves, complete, curvePercentage };
  },
};

/** Default parser returned by detectParser() — platform id resolved to "bags" or "meteora" later */
export const MeteoraParser: LaunchpadParser = { id: "meteora", ...meteora };

/** Alias registered in BY_ID so getParser("bags") resolves for stored tokens */
export const BagsParser: LaunchpadParser    = { id: "bags",    ...meteora };
