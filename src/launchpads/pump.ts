// src/launchpads/pump.ts — Pump.fun parser

import { PublicKey } from "@solana/web3.js";
import { LaunchpadParser, CurveState } from "./types";
import { collectInstructions, extractMetadataFromTx, readU64 } from "./shared";

const PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

// Anchor discriminator for the "create" instruction: sha256("global:create")[0..8]
// Stable across Pump.fun deployments — prefer over log string.
const CREATE_DISC = "181ec828051c0777";

const GRADUATION_TARGET_LAMPORTS = 85n * 1_000_000_000n; // 85 SOL in lamports

export const PumpParser: LaunchpadParser = {
  id:             "pump",
  programId:      PROGRAM_ID,
  strictMetadata: true, // Pump.fun always embeds name+symbol in the tx — skip if absent

  isCreate(logs, message, meta) {
    // PRIMARY: Anchor discriminator in any instruction (immune to log format changes)
    for (const ix of collectInstructions(message, meta)) {
      const d = Buffer.from(ix.data || []);
      if (d.length >= 8 && d.slice(0, 8).toString("hex") === CREATE_DISC) return true;
    }
    // SECONDARY: log message (readable but mutable)
    return logs.some(l => l.includes("Program log: Instruction: Create"));
  },

  detectSwap(logs, _meta) {
    // Pump.fun logs are explicit — no need for balance delta fallback
    for (const l of logs) {
      if (l.includes("Program log: Instruction: Buy"))  return "buy";
      if (l.includes("Program log: Instruction: Sell")) return "sell";
    }
    return null;
  },

  parseMetadata(logs, message, meta) {
    return extractMetadataFromTx(logs, message, meta);
  },

  deriveCurvePDA(mint) {
    try {
      const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("bonding-curve"), new PublicKey(mint).toBuffer()],
        new PublicKey(PROGRAM_ID),
      );
      return pda.toBase58();
    } catch { return ""; }
  },

  parseCurveData(data) {
    // Layout after 8-byte discriminator:
    // virtualTokenReserves u64 @8
    // virtualSolReserves   u64 @16
    // realTokenReserves    u64 @24
    // realSolReserves      u64 @32
    // tokenTotalSupply     u64 @40
    // complete             bool @48
    if (data.length < 49) return null;
    const virtualTokenReserves = readU64(data, 8);
    const virtualSolReserves   = readU64(data, 16);
    const realTokenReserves    = readU64(data, 24);
    const realSolReserves      = readU64(data, 32);
    const complete             = data[48] === 1;
    const curvePercentage      = Math.min(100, Number(realSolReserves * 100n / GRADUATION_TARGET_LAMPORTS));
    return { virtualTokenReserves, virtualSolReserves, realTokenReserves, realSolReserves, complete, curvePercentage };
  },
};
