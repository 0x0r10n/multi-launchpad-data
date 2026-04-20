// src/launchpads/moon.ts — Moon.it (+ legacy Moonshot) parser

import { LaunchpadParser, CurveState } from "./types";
import { collectInstructions, extractMetadataFromTx, detectSwapFromDelta, extractPoolFromCreateTx, readU64 } from "./shared";

export const MOON_PROGRAM_ID     = "Moonit1111111111111111111111111111111111111";
export const MOONSHOT_PROGRAM_ID = "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"; // legacy — still in use

const GRADUATION_TARGET_LAMPORTS = 100_000_000_000n; // 100 SOL in lamports
const MAX_SOL_RESERVE = 10_000n * 1_000_000_000n; // guard SOL-only; token reserves can legitimately hit 10^15

// Moon.it excludes these treasury/fee accounts from holder analysis
export const MOON_EXCLUDE_WALLETS = [MOON_PROGRAM_ID, MOONSHOT_PROGRAM_ID];

export const MoonParser: LaunchpadParser = {
  id:             "moon",
  programId:      MOON_PROGRAM_ID,
  strictMetadata: false, // metadata often absent from tx — enricher fills it in

  isCreate(logs, message, meta) {
    // PRIMARY: Moon.it emits "tokenMint" in logs on creation — specific to this platform
    if (logs.some(l => l.toLowerCase().includes("tokenmint"))) return true;
    // SECONDARY: discriminator scan
    // NOTE: "2a9a1c1e0f0a1b2c" is the reported Moon.it create discriminator but is
    // unverified against on-chain ABI — treated as supplementary only.
    for (const ix of collectInstructions(message, meta)) {
      const d = Buffer.from(ix.data || []);
      if (d.length >= 8 && d.slice(0, 8).toString("hex") === "2a9a1c1e0f0a1b2c") return true;
    }
    return false;
  },

  detectSwap(logs, meta) {
    // Anchor to "Instruction:" to avoid matching arbitrary log content containing "Buy"/"Sell"
    for (const l of logs) {
      if (l.includes("Instruction: Buy")  || l.includes("Instruction: buy"))  return "buy";
      if (l.includes("Instruction: Sell") || l.includes("Instruction: sell")) return "sell";
    }
    return detectSwapFromDelta(meta);
  },

  parseMetadata(logs, message, meta) {
    return extractMetadataFromTx(logs, message, meta, MOON_PROGRAM_ID);
  },

  deriveCurvePDA(mint, message?, meta?) {
    // For new launches, try to extract from tx first for 100% certainty.
    if (message && meta) {
      // Index 1 is unverified for Moon.it — debug dump enabled so live logs reveal correct index.
      const extracted = extractPoolFromCreateTx(MOON_PROGRAM_ID, mint, "moon", 1, message, meta)
                     || extractPoolFromCreateTx(MOONSHOT_PROGRAM_ID, mint, "moon", 1, message, meta);
      if (extracted) return extracted;
    }

    // No reliable seed derivation known for Moon.it — if tx extraction fails, return ""
    // and let the critical-error log in yellowstone-manager surface it.
    return "";
  },

  parseCurveData(data) {
    // Moon.it uses a shorter layout — only virtual reserves are guaranteed present
    // virtualTokenReserves u64 @8
    // virtualSolReserves   u64 @16
    // realTokenReserves    u64 @24  (if data.length >= 32)
    // realSolReserves      u64 @32  (if data.length >= 40)
    if (data.length < 24) return null;
    const virtualTokenReserves = readU64(data, 8);
    const virtualSolReserves   = readU64(data, 16);
    if (virtualSolReserves === 0n || virtualTokenReserves === 0n) return null;
    if (virtualSolReserves > MAX_SOL_RESERVE) return null;
    const realTokenReserves    = data.length >= 32 ? readU64(data, 24) : 0n;
    const realSolReserves      = data.length >= 40 ? readU64(data, 32) : 0n;
    const curvePercentage      = Math.min(100, Number(virtualSolReserves * 100n / GRADUATION_TARGET_LAMPORTS));
    const complete             = curvePercentage >= 100;
    return { virtualTokenReserves, virtualSolReserves, realTokenReserves, realSolReserves, complete, curvePercentage };
  },
};
