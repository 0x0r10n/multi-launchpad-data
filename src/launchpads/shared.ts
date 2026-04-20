// src/launchpads/shared.ts — Utilities shared across all launchpad parsers

import bs58 from "bs58";
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

/**
 * Extract the pool/curve PDA from a create transaction.
 *
 * Strategy (in order of preference):
 *   1. Top-level instructions first — inner CPIs have different account layouts.
 *   2. The instruction must reference `programId` AND include `mint` as one of its
 *      accounts — this uniquely identifies the create instruction even when the same
 *      program appears in multiple inner instructions.
 *   3. Return the account at `preferredIndex` within that instruction's account list.
 *   4. Fall back to inner instructions with the same filter.
 *
 * Logs all accounts so callers can verify the correct index from live output.
 * Returns "" on failure — caller must fall back to seed derivation.
 */
export function extractPoolFromCreateTx(
  programId: string,
  mint: string,
  label: string,
  preferredIndex: number = 3,
  message: any,
  meta: any,
  debugAccounts: boolean = false
): string {
  if (!message) return "";

  const rawKeys: Uint8Array[] = message?.accountKeys || [];
  const loadedWritable: Uint8Array[] = meta?.loadedWritableAddresses || [];
  const loadedReadonly: Uint8Array[] = meta?.loadedReadonlyAddresses || [];
  const accountKeys = [...rawKeys, ...loadedWritable, ...loadedReadonly].map((k: any) => bs58.encode(k));

  // First pass: top-level instructions (most reliable for create)
  const topLevel: any[] = message?.instructions || [];
  for (const ix of topLevel) {
    if (accountKeys[ix.programIdIndex] !== programId) continue;
    const accs = Array.from((ix.accounts as Uint8Array) || []);
    if (accs.some(i => accountKeys[i] === mint)) {
      const accounts = accs.map(i => accountKeys[i]);
      if (debugAccounts) {
        console.log(`[Extract ${label}] accounts dump for mint=${mint.slice(0, 12)}:`);
        accounts.forEach((a, i) => console.log(`  [${i}] ${a}`));
      }
      const candidate = accounts[preferredIndex];
      if (candidate && candidate.length > 40) {
        console.log(`[Extract ${label}] SUCCESS — poolPDA from top-level ix, index=${preferredIndex} → ${candidate.slice(0, 12)}`);
        return candidate;
      }
      console.warn(`[Extract ${label}] index=${preferredIndex} empty/missing — accounts.length=${accounts.length}`);
    }
  }

  // Second pass: inner instructions (for complex CPIs)
  const innerGroups: any[] = meta?.innerInstructions || [];
  for (const group of innerGroups) {
    for (const ix of (group.instructions || [])) {
      const pid = ix.programIdIndex != null ? accountKeys[ix.programIdIndex] : (ix.programId ? bs58.encode(ix.programId) : "");
      if (pid !== programId) continue;

      const accs = Array.from((ix.accounts as Uint8Array) || []);
      if (accs.some(i => accountKeys[i] === mint)) {
        const accounts = accs.map(i => accountKeys[i]);
        if (debugAccounts) {
          console.log(`[Extract ${label}] inner accounts dump for mint=${mint.slice(0, 12)}:`);
          accounts.forEach((a, i) => console.log(`  [${i}] ${a}`));
        }
        const candidate = accounts[preferredIndex];
        if (candidate && candidate.length > 40) {
          console.log(`[Extract ${label}] SUCCESS — poolPDA from inner ix, index=${preferredIndex} → ${candidate.slice(0, 12)}`);
          return candidate;
        }
      }
    }
  }

  console.warn(`[Extract ${label}] FAILED to find pool PDA — mint=${mint.slice(0, 12)}`);
  return "";
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
 *
 * When `programId` is supplied the log scan is fenced to entries emitted inside that
 * program's execution context. This prevents Jito tip and other bundled programs from
 * having their fee/config placeholder strings accidentally parsed as token name/symbol.
 *
 * Returns empty strings when nothing is found — enricher handles the rest.
 */
export function extractMetadataFromTx(logs: string[], message: any, meta: any, programId?: string): TokenMetadata {
  if (programId) {
    // Context-fenced scan — only read "Program data:" within this program's invoke/success span
    let inCtx = false;
    for (const log of logs) {
      if (log.includes(`Program ${programId} invoke`)) { inCtx = true; continue; }
      if (log.includes(`Program ${programId} success`) || log.includes(`Program ${programId} failed`)) { inCtx = false; continue; }
      if (inCtx && log.startsWith("Program data: ")) {
        const result = parseFromBase64(log.slice("Program data: ".length));
        if (result) return result;
      }
    }

    // Instruction fallback — only scan instructions belonging to the target program.
    // Scanning all instructions risks matching fee/config structs from Jito or other CPIs.
    const rawKeys: Uint8Array[] = message?.accountKeys || [];
    const loadedWritable: Uint8Array[] = meta?.loadedWritableAddresses || [];
    const loadedReadonly: Uint8Array[] = meta?.loadedReadonlyAddresses || [];
    const accountKeys = [...rawKeys, ...loadedWritable, ...loadedReadonly].map((k: any) => bs58.encode(k));
    for (const ix of collectInstructions(message, meta)) {
      const pid = ix.programIdIndex != null ? accountKeys[ix.programIdIndex] : "";
      if (pid !== programId) continue;
      const result = parseNameSymbolUri(Buffer.from(ix.data || []));
      if (result) return result;
    }
  } else {
    for (const log of logs) {
      if (!log.startsWith("Program data: ")) continue;
      const result = parseFromBase64(log.slice("Program data: ".length));
      if (result) return result;
    }
    for (const ix of collectInstructions(message, meta)) {
      const result = parseNameSymbolUri(Buffer.from(ix.data || []));
      if (result) return result;
    }
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
