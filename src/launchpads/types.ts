// src/launchpads/types.ts

export interface TokenMetadata {
  name:               string;
  symbol:             string;
  uri:                string;
  isMayhemMode?:      boolean;
  isCashbackEnabled?: boolean;
}

export interface CurveState {
  virtualTokenReserves: bigint;
  virtualSolReserves:   bigint;
  realTokenReserves:    bigint;
  realSolReserves:      bigint;
  complete:             boolean;
  curvePercentage:      number;
}

export interface LaunchpadParser {
  /** Short id stored in Redis token hash as "platform" field */
  readonly id: string;
  /** Primary on-chain program address */
  readonly programId: string;
  /**
   * If true, the first broadcast is skipped when name+symbol are absent from the tx.
   * Pump.fun always embeds metadata; others may not.
   */
  readonly strictMetadata: boolean;

  /**
   * Returns true if this transaction is a token creation event.
   * Implementations must list signals from most to least reliable.
   */
  isCreate(logs: string[], message: any, meta: any): boolean;

  /**
   * Returns "buy" | "sell" | null.
   * Should prefer explicit log signals over balance delta.
   */
  detectSwap(logs: string[], meta: any): "buy" | "sell" | null;

  /**
   * Extracts name/symbol/uri from transaction data.
   * Returns empty strings on failure — enricher will fill them in.
   */
  parseMetadata(logs: string[], message: any, meta: any): TokenMetadata;

  /**
   * Derives the bonding curve account PDA for a mint.
   * Returns "" on failure — caller must check.
   */
  deriveCurvePDA(mint: string): string;

  /**
   * Decodes raw bonding curve account data.
   * Returns null if data is too short or layout unrecognized.
   */
  parseCurveData(data: Buffer): CurveState | null;
}
