// src/launchpads/moon.ts — Moon.it (+ legacy Moonshot) parser

import { PublicKey } from "@solana/web3.js";
import { LaunchpadParser, CurveState } from "./types";
import { collectInstructions, extractMetadataFromTx, detectSwapFromDelta, readU64 } from "./shared";

export const MOON_PROGRAM_ID     = "Moonit1111111111111111111111111111111111111";
export const MOONSHOT_PROGRAM_ID = "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"; // legacy — still in use

const GRADUATION_TARGET_LAMPORTS = 100_000_000_000n; // 100 SOL in lamports

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
    // Moon.it logs are not as explicit as Pump.fun — use loose matching + delta fallback
    for (const l of logs) {
      if (l.includes("Buy")  || l.includes("buy"))  return "buy";
      if (l.includes("Sell") || l.includes("sell")) return "sell";
    }
    return detectSwapFromDelta(meta);
  },

  parseMetadata(logs, message, meta) {
    return extractMetadataFromTx(logs, message, meta);
  },

  deriveCurvePDA(mint) {
    try {
      const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
        new PublicKey(MOON_PROGRAM_ID),
      );
      return pda.toBase58();
    } catch { return ""; }
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
    const realTokenReserves    = data.length >= 32 ? readU64(data, 24) : 0;
    const realSolReserves      = data.length >= 40 ? readU64(data, 32) : 0;
    const curvePercentage      = Math.min(100, Number(BigInt(virtualSolReserves) * 100n / GRADUATION_TARGET_LAMPORTS));
    const complete             = curvePercentage >= 100;
    return { virtualTokenReserves, virtualSolReserves, realTokenReserves, realSolReserves, complete, curvePercentage };
  },
};
