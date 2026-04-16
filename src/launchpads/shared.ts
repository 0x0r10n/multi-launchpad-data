// src/launchpads/shared.ts — Utilities shared across all launchpad parsers

import { TokenMetadata } from "./types";

// ── Instruction collection ────────────────────────────────────────────────────

/** Flatten top-level + all inner instructions into one array */
export function collectInstructions(message: any, meta: any): any[] {
  const all: any[] = [];
  if (message?.instructions) all.push(...message.instructions);
  if (meta?.innerInstructions) {
    for (const inner of meta.innerInstructions) {
      if (inner.instructions) all.push(...inner.instructions);
    }
  }
  return all;
}

// ── Metadata parsing ──────────────────────────────────────────────────────────

/**
 * Try to parse [8-byte disc][u32+name][u32+symbol][u32+uri] from a Borsh buffer.
 * offset defaults to 8 to skip the Anchor discriminator.
 * Rejects strings containing non-printable ASCII (binary noise guard).
 */
export function parseNameSymbolUri(data: Buffer, offset = 8): TokenMetadata | null {
  try {
    let pos = offset;
    const readStr = (): string | null => {
      if (pos + 4 > data.length) return null;
      const len = data.readUInt32LE(pos); pos += 4;
      if (len === 0 || len > 500 || pos + len > data.length) return null;
      const s = data.slice(pos, pos + len).toString("utf-8"); pos += len;
      return /^[\x20-\x7E]+$/.test(s) ? s : null;
    };
    const name   = readStr();
    const symbol = readStr();
    const uri    = readStr();
    if (!name || !symbol) return null;

    let isMayhemMode = false;
    let isCashbackEnabled = false;

    // Pump.fun CreateV2 / Mayhem logic: Try to jump ahead to flags if buffer is large enough
    // Layout after uri: [32 mint][32 bc][32 user][32 creator][8 ts][32 reserves][32 tokenpg][1 mayhem][1 cashback]
    const EXTRA_PUMP_FIELDS_LEN = (32 * 5) + 8 + 32 + 2; 
    if (pos + EXTRA_PUMP_FIELDS_LEN <= data.length) {
      const skipToFlags = pos + (32 * 5) + 8 + 32;
      isMayhemMode = data[skipToFlags] === 1;
      isCashbackEnabled = data[skipToFlags + 1] === 1;
    }

    return { name, symbol, uri: uri || "", isMayhemMode, isCashbackEnabled };
  } catch {}
  return null;
}

/** Decode a base64 "Program data:" log entry and try to parse metadata from it */
export function parseFromBase64(b64: string): TokenMetadata | null {
  try {
    const data = Buffer.from(b64, "base64");
    if (data.length < 20) return null;
    return parseNameSymbolUri(data);
  } catch { return null; }
}

/**
 * Universal metadata extraction strategy used by all parsers:
 * 1. "Program data:" base64 log entries  (Anchor event — most structured)
 * 2. Raw instruction data scan           (Borsh-encoded instruction args)
 * Returns empty strings when nothing is found — enricher handles the rest.
 */
export function extractMetadataFromTx(logs: string[], message: any, meta: any): TokenMetadata {
  for (const log of logs) {
    if (!log.startsWith("Program data: ")) continue;
    const result = parseFromBase64(log.slice("Program data: ".length));
    if (result) return result;
  }
  for (const ix of collectInstructions(message, meta)) {
    const result = parseNameSymbolUri(Buffer.from(ix.data || []));
    if (result) return result;
  }
  return { name: "", symbol: "", uri: "" };
}

// ── Swap detection fallback ────────────────────────────────────────────────────

/**
 * Infer buy/sell from signer (index 0) SOL balance delta.
 * 10,000 lamport threshold filters out fee-only interactions.
 * Use only when explicit log signals are absent.
 */
export function detectSwapFromDelta(meta: any): "buy" | "sell" | null {
  const pre  = meta?.preBalances;
  const post = meta?.postBalances;
  if (!pre || !post || pre.length === 0) return null;
  const delta = Number(post[0]) - Number(pre[0]);
  if (delta < -10_000) return "buy";
  if (delta >  10_000) return "sell";
  return null;
}

// ── Binary helpers ────────────────────────────────────────────────────────────

/** Read a little-endian u64 from buffer at offset (returns bigint — safe for full u64 range) */
export function readU64(buf: Buffer, offset: number): bigint {
  const lo = BigInt(buf.readUInt32LE(offset));
  const hi = BigInt(buf.readUInt32LE(offset + 4));
  return (hi << 32n) | lo;
}
