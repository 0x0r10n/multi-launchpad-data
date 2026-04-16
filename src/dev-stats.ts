// src/dev-stats.ts — Historical on-chain dev stat bootstrap
//
// The live incremental system (yellowstone-manager / curve-tracker) tracks launched/migrated
// in real time via hincrby on creator:{address}. But that only covers events seen since the
// indexer started — a creator who launched 20 tokens last month would show launched=0.
//
// This module fills in the historical gap using two RPC calls:
//   1. getSignaturesForAddress  — fetch up to 200 recent signatures  (1 RPC)
//   2. getTransactions (batch)  — fetch all those txs at once         (1 RPC per 256-chunk)
//
// Counts are merged with max(bootstrap, live) so live tracking is never downgraded.
// A bootstrapAt timestamp in creator:{address} prevents re-scanning within 24h.

import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";
import { LAUNCHPAD_PROGRAM_IDS } from "./launchpads";

const redis      = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const BOOTSTRAP_COOLDOWN_MS = 24 * 60 * 60 * 1000; // 24 h
const SIGNATURE_LIMIT       = 200;                  // max signatures to scan per creator
const BATCH_SIZE            = 256;                  // getTransactions max per call

// ── Create signals — same precise strings used by each parser's isCreate() ───
// These are Anchor instruction logs emitted by the launchpad programs themselves.
const CREATE_SIGNALS = [
  "Instruction: Create",        // Pump.fun
  "Instruction: InitializeV2",  // LaunchLab / LetsBonk
  "Instruction: Initialize",    // Moon.it / Bags.fm / Meteora DBC
];

// ── Migration signals — same strings used by yellowstone-manager isMigrationTx ─
const MIGRATE_SIGNALS = [
  "Instruction: Migrate",       // Pump.fun + LaunchLab fallback
  "MigrateToAmm",               // LaunchLab / LetsBonk
  "migrate_to_amm",
  "Instruction: MigrateFunds",
];

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Fire-and-forget bootstrap — safe to call on every new-launch.
 * Does nothing if the creator was already bootstrapped within 24h.
 */
export function bootstrapDevStatsIfNeeded(creator: string): void {
  redis.hget(`creator:${creator}`, "bootstrapAt")
    .then(ts => {
      if (ts && Date.now() - parseInt(ts) < BOOTSTRAP_COOLDOWN_MS) return;
      _bootstrap(creator).catch(() => {});
    })
    .catch(() => {});
}

// ── Internal ──────────────────────────────────────────────────────────────────

async function _bootstrap(creator: string): Promise<void> {
  // 1. Get recent signatures for the creator wallet (1 RPC)
  const sigInfos = await connection
    .getSignaturesForAddress(new PublicKey(creator), { limit: SIGNATURE_LIMIT })
    .catch(() => null);

  if (!sigInfos?.length) {
    await redis.hset(`creator:${creator}`, { bootstrapAt: Date.now().toString() });
    return;
  }

  // Only bother fetching successful transactions
  const signatures = sigInfos.filter(s => !s.err).map(s => s.signature);
  if (!signatures.length) {
    await redis.hset(`creator:${creator}`, { bootstrapAt: Date.now().toString() });
    return;
  }

  // 2. Batch fetch transactions — BATCH_SIZE per call (1–2 RPCs for 200 sigs)
  const allTxs: any[] = [];
  for (let i = 0; i < signatures.length; i += BATCH_SIZE) {
    const chunk = signatures.slice(i, i + BATCH_SIZE);
    const txs = await connection
      .getTransactions(chunk, { maxSupportedTransactionVersion: 0, commitment: "confirmed" })
      .catch(() => null);
    if (txs) allTxs.push(...txs);
  }

  let launched = 0;
  let migrated = 0;

  for (const tx of allTxs) {
    if (!tx?.meta?.logMessages) continue;
    const logs: string[] = tx.meta.logMessages;
    const logStr = logs.join(" ");

    // Skip transactions that don't touch any known launchpad program
    if (!LAUNCHPAD_PROGRAM_IDS.some(id => logStr.includes(id))) continue;

    if (logs.some(l => CREATE_SIGNALS.some(s => l.includes(s)))) {
      launched++;
    } else if (logs.some(l => MIGRATE_SIGNALS.some(s => l.includes(s)))) {
      migrated++;
    }
  }

  // Merge with live-tracked counts — never downgrade what the live system already knows
  const existing = await redis.hgetall(`creator:${creator}`).catch(() => ({} as Record<string, string>));
  const liveLaunched = parseInt(existing.launched || "0");
  const liveMigrated = parseInt(existing.migrated || "0");

  await redis.hset(`creator:${creator}`, {
    launched:    Math.max(launched, liveLaunched).toString(),
    migrated:    Math.max(migrated, liveMigrated).toString(),
    bootstrapAt: Date.now().toString(),
  });

  console.log(
    `[DevStats] ${creator.slice(0, 8)} | launched=${Math.max(launched, liveLaunched)} migrated=${Math.max(migrated, liveMigrated)}` +
    ` (scanned ${allTxs.length} txs, found ${launched}L ${migrated}M on-chain)`,
  );
}
