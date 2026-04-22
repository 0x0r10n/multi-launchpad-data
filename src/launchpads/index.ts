// src/launchpads/index.ts — Registry and platform detection

import bs58 from "bs58";
import { LaunchpadParser } from "./types";
import { PumpParser }                                          from "./pump";
import { MoonParser, MOONSHOT_PROGRAM_ID, MOON_EXCLUDE_WALLETS } from "./moon";
import { MeteoraParser, BagsParser, METEORA_DBC_PROGRAM_ID }  from "./bags";
import { LetsBonkParser, LETSBONK_CONFIG_ACCOUNT }            from "./letsbonk";
import { LaunchLabParser, LAUNCHLAB_PROGRAM_ID }              from "./launchlab";
import { RaydiumAmmParser, RAYDIUM_AMM_V4_PROGRAM_ID }        from "./raydium";

export type { LaunchpadParser, TokenMetadata, CurveState } from "./types";
export { PumpParser }                                            from "./pump";
export { MoonParser, MOONSHOT_PROGRAM_ID, MOON_EXCLUDE_WALLETS } from "./moon";
export { MeteoraParser, BagsParser, METEORA_DBC_PROGRAM_ID }    from "./bags";
export { LetsBonkParser, LETSBONK_CONFIG_ACCOUNT }               from "./letsbonk";
export { LaunchLabParser, LAUNCHLAB_PROGRAM_ID }                 from "./launchlab";
export { RaydiumAmmParser, RAYDIUM_AMM_V4_PROGRAM_ID }           from "./raydium";

// Parsers used for program-ID-based detection (Pump, Moon, Meteora).
// LaunchLab and LetsBonk share the same program ID and are handled separately in
// detectParser() where LetsBonk is always checked FIRST via the config account.
// LetsBonkParser is included here so its programId (the config account) is captured
// in LAUNCHPAD_PROGRAM_IDS for Yellowstone subscription.
const PARSERS: LaunchpadParser[] = [
  PumpParser,
  MoonParser,
  MeteoraParser,   // id="meteora" — label may be overridden to "bags" by caller
  LetsBonkParser,  // programId = config account — included for Yellowstone subscription only
  LaunchLabParser,
  RaydiumAmmParser,
];

// id → parser
// "bags" and "meteora" both resolve to the same implementation so curve-tracker and
// risk-analyzer can look up either label stored in Redis.
const BY_ID = new Map<string, LaunchpadParser>(PARSERS.map(p => [p.id, p]));
BY_ID.set("bags", BagsParser); // register the "bags" alias explicitly

// programId → parser for non-LaunchLab platforms (Pump, Moon, Meteora, LetsBonk config account).
// LaunchLab is intentionally excluded from this map — it shares a program ID with LetsBonk
// and is handled by the explicit hasLaunchLab path in detectParser() below.
const BY_PROGRAM = new Map<string, LaunchpadParser>([
  [PumpParser.programId,     PumpParser],
  [MoonParser.programId,     MoonParser],
  [MeteoraParser.programId,  MeteoraParser],
  [MOONSHOT_PROGRAM_ID,      MoonParser],        // Moon.it legacy address
  [LETSBONK_CONFIG_ACCOUNT,  LetsBonkParser],    // config account fallback (account key scan)
]);

/**
 * All program/account addresses for the Yellowstone accountInclude filter.
 * Includes both the LaunchLab program and the LetsBonk config account so Yellowstone
 * delivers transactions that reference either.
 */
export const LAUNCHPAD_PROGRAM_IDS: string[] = [
  ...new Set([
    ...PARSERS.map(p => p.programId),
    MOONSHOT_PROGRAM_ID,
    LAUNCHLAB_PROGRAM_ID, // explicit — LetsBonkParser.programId is the config account, not the program
  ]),
];

/**
 * Detect which launchpad a transaction belongs to.
 *
 * Detection order (critical for correctness and speed):
 *   1. LetsBonk — LaunchLab program present AND LetsBonk PlatformConfig in accounts
 *   2. LaunchLab (generic) — LaunchLab program present, no LetsBonk config account
 *   3. All other parsers via log-string program ID scan
 *   4. Account key fallback (Pump, Moon, Meteora)
 *
 * LetsBonk must be checked before generic LaunchLab because both use the same program.
 */
export function detectParser(meta: any, message: any): LaunchpadParser | null {
  const logStr     = (meta?.logMessages || []).join(" ");
  // Include ALT-loaded addresses (versioned txs) so LetsBonk config account is found
  // even when it lives in an Address Lookup Table rather than the static key list.
  const staticKeys: string[] = (message?.accountKeys || []).map((k: any) => bs58.encode(k));
  const loadedWritable: string[] = (meta?.loadedWritableAddresses || []).map((k: any) => bs58.encode(k));
  const loadedReadonly: string[] = (meta?.loadedReadonlyAddresses || []).map((k: any) => bs58.encode(k));
  const accountKeys = [...staticKeys, ...loadedWritable, ...loadedReadonly];

  // LetsBonk / LaunchLab — checked first since they share a program ID
  const hasLaunchLab =
    logStr.includes(LAUNCHLAB_PROGRAM_ID) ||
    accountKeys.includes(LAUNCHLAB_PROGRAM_ID);

  if (hasLaunchLab) {
    // LetsBonk: PlatformConfig account present → definitive fingerprint
    return accountKeys.includes(LETSBONK_CONFIG_ACCOUNT) ? LetsBonkParser : LaunchLabParser;
  }

  // All other platforms: log-string scan (fast — single pass over already-joined string)
  for (const [programId, parser] of BY_PROGRAM) {
    if (logStr.includes(programId)) return parser;
  }

  // Account key fallback (catches txs where program ID doesn't appear in logs)
  for (const key of accountKeys) {
    const parser = BY_PROGRAM.get(key);
    if (parser) return parser;
  }

  return null;
}

/**
 * Resolve the final platform id for a Meteora DBC token.
 * Detection is mint-address only — Bags.fm uses vanity mints ending with "BAGS".
 * Symbol and log content are intentionally excluded: they are mutable and unreliable.
 */
export function resolveMeteoraId(mint: string): "bags" | "meteora" {
  return mint.toUpperCase().endsWith("BAGS") ? "bags" : "meteora";
}

/** Look up a parser by its stored platform id ("pump", "moon", "bags", "meteora", …) */
export function getParser(id: string): LaunchpadParser | undefined {
  return BY_ID.get(id);
}
