// src/risk-analyzer.ts — Snipers, insiders, dev stats, top10 holder concentration
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";
import { MOON_EXCLUDE_WALLETS } from "./launchpads";

const redis = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

// ATA derivation — used in quick scan to find dev holdings without extra RPC
const TOKEN_PROGRAM_ID          = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const ASSOCIATED_TOKEN_PROGRAM_ID = new PublicKey("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bF6");

function getAssociatedTokenAddress(mint: PublicKey, owner: PublicKey): PublicKey {
  return PublicKey.findProgramAddressSync(
    [owner.toBuffer(), TOKEN_PROGRAM_ID.toBuffer(), mint.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM_ID,
  )[0];
}

const TOKEN_TOTAL_SUPPLY = 1_000_000_000;
const DECIMALS = 6;
const RAW_SUPPLY = TOKEN_TOTAL_SUPPLY * Math.pow(10, DECIMALS);
const inFlight = new Set<string>();

// 5-minute cooldown — risk data doesn't change that fast between analyses
const RISK_COOLDOWN_MS = 5 * 60_000;

// ── Shared holder computation ─────────────────────────────────────────────────
// Core of both the inline fast path and the t=2s fallback.
// Returns null if RPC doesn't respond within timeoutMs.
async function computeHolderFields(
  mint: string,
  creator: string,
  curvePDA: string,
  platform: string,
  timeoutMs: number,
): Promise<{ top10: string; devPercentage: string } | null> {
  const mintPubkey = new PublicKey(mint);
  const excludeWallets = platform === "moon" ? MOON_EXCLUDE_WALLETS : [];

  const result = await Promise.race([
    connection.getTokenLargestAccounts(mintPubkey).catch(() => null),
    new Promise<null>(r => setTimeout(() => r(null), timeoutMs)),
  ]);
  if (!result?.value?.length) return null;

  const holders = result.value
    .filter(h => {
      const addr = h.address.toBase58();
      return addr !== curvePDA && !excludeWallets.includes(addr);
    })
    .slice(0, 10);

  const top10Raw = holders.reduce((s, h) => s + Number(h.amount), 0);
  const top10Pct = Math.min(100, (top10Raw / RAW_SUPPLY) * 100);

  let devPercentage = 0;
  if (creator) {
    try {
      const creatorAta = getAssociatedTokenAddress(mintPubkey, new PublicKey(creator));
      const devHolder = result.value.find(h => h.address.toBase58() === creatorAta.toBase58());
      if (devHolder) devPercentage = Math.min(100, (Number(devHolder.amount) / RAW_SUPPLY) * 100);
    } catch {}
  }

  return { top10: top10Pct.toFixed(2), devPercentage: devPercentage.toFixed(2) };
}

// ── Inline holder snapshot for the new-launch hot path ───────────────────────
// Started at t=0 in parallel with the curve race. If it resolves in time,
// top10 + dev% appear in the same broadcast as price (~100-500ms after launch).
// Does NOT write to Redis — the caller merges and persists.
export function startHolderSnapshot(
  mint: string,
  creator: string,
  curvePDA: string,
  platform: string,
): Promise<{ top10: string; devPercentage: string } | null> {
  // 480ms internal timeout: slightly more breathing room without pushing worst-case too far.
  // Combined with the 450ms grace window in index.ts, total holder budget = 480-930ms.
  return computeHolderFields(mint, creator, curvePDA, platform, 480);
}

// ── Scheduled quick risk scan (t=2s fallback) ─────────────────────────────────
// Runs only if riskQuickScanAt is not yet set — idempotent by design.
// Adds sniper count + dev stats (Redis reads) on top of the holder fields.
export function queueInitialQuickRisk(mint: string) {
  initialQuickRisk(mint).catch(() => {});
}

async function initialQuickRisk(mint: string) {
  const alreadyScanned = await redis.hget(`token:${mint}`, "riskQuickScanAt");
  if (alreadyScanned) return; // inline snapshot already handled this

  const tokenData = await redis.hgetall(`token:${mint}`);
  if (!tokenData?.mint) return;

  const holderFields = await computeHolderFields(
    mint, tokenData.creator || "", tokenData.curvePDA || "", tokenData.platform || "pump", 3000,
  );
  if (!holderFields) return;

  const [knownSnipers, creatorStats] = await Promise.all([
    redis.smembers(`snipers_set:${mint}`),
    tokenData.creator ? redis.hgetall(`creator:${tokenData.creator}`) : Promise.resolve({} as Record<string, string>),
  ]);

  const update: Record<string, string> = {
    top10:           holderFields.top10,
    devPercentage:   holderFields.devPercentage,
    devStats:        JSON.stringify({
      total_launched: parseInt(creatorStats.launched || "0"),
      total_migrated: parseInt(creatorStats.migrated || "0"),
    }),
    snipersCount:    knownSnipers.length.toString(),
    riskQuickScanAt: Date.now().toString(),
  };

  await redis.hset(`token:${mint}`, update);
  console.log(`[Risk] ⚡ Quick scan ${tokenData.name} | top10≈${parseFloat(holderFields.top10).toFixed(1)}% dev≈${parseFloat(holderFields.devPercentage).toFixed(1)}% snipers=${knownSnipers.length}`);
  await redis.publish("token-updates", mint);
}

export function queueRiskAnalysis(mint: string) {
  if (inFlight.has(mint)) return;
  inFlight.add(mint);
  analyzeRisk(mint).catch(() => {}).finally(() => inFlight.delete(mint));
}

async function analyzeRisk(mint: string) {
  const tokenData = await redis.hgetall(`token:${mint}`);
  if (!tokenData?.mint) return;

  const lastAnalysis = parseInt(tokenData.riskAnalyzedAt || "0");
  if (Date.now() - lastAnalysis < RISK_COOLDOWN_MS) return;

  const mintPubkey = new PublicKey(mint);
  const creator = tokenData.creator || "";

  let largestAccounts;
  try {
    largestAccounts = await connection.getTokenLargestAccounts(mintPubkey);
  } catch {
    return;
  }

  if (!largestAccounts?.value?.length) return;

  const holders = largestAccounts.value;
  const holdersToResolve = holders.slice(0, 15);

  // ── Batch resolve token account owners ──────────────────────────────────────
  // Supply is always RAW_SUPPLY (1B × 10^6) for all indexed tokens — no RPC needed.
  const tokenAccountPubkeys = holdersToResolve.map(h => h.address);
  const accountInfos = await connection.getMultipleAccountsInfo(tokenAccountPubkeys);

  const resolvedHolders = holdersToResolve.map((holder, i) => {
    const data = accountInfos[i]?.data;
    let owner = "";
    if (data && data.length >= 64) {
      owner = new PublicKey(data.slice(32, 64)).toBase58();
    }
    return {
      address: holder.address.toBase58(),
      owner,
      rawAmount: Number(holder.amount),
    };
  });

  // ── Filter out bonding curve and platform treasury accounts ───────────────
  const curvePDA = tokenData.curvePDA || "";
  const platform = tokenData.platform || "pump";

  const excludeWallets = platform === "moon" ? MOON_EXCLUDE_WALLETS : [];

  const validHolders = resolvedHolders.filter(h =>
    h.owner !== curvePDA &&
    h.address !== curvePDA &&
    !excludeWallets.includes(h.owner) &&
    !excludeWallets.includes(h.address),
  );

  // ── Top 10 concentration ──────────────────────────────────────────────────
  const top10RawAmount = validHolders.slice(0, 10).reduce((sum, h) => sum + h.rawAmount, 0);

  const totalSupplyRaw = RAW_SUPPLY;

  const top10Pct = totalSupplyRaw > 0
    ? Math.min(Math.max((top10RawAmount / totalSupplyRaw) * 100, 0), 100)
    : 0;

  const top10Wallets = validHolders.slice(0, 10).map(holder => ({
    address: holder.address,
    owner: holder.owner,
    amount: holder.rawAmount / Math.pow(10, DECIMALS),
    percentage: parseFloat(((holder.rawAmount / totalSupplyRaw) * 100).toFixed(2)),
  }));

  // ── Dev holdings ──────────────────────────────────────────────────────────
  let devPercentage = 0;
  let devAmount = 0;
  if (creator) {
    const devSlot = top10Wallets.find(w => w.owner === creator);
    if (devSlot) { devPercentage = devSlot.percentage; devAmount = devSlot.amount; }
  }

  // ── Snipers & insiders ────────────────────────────────────────────────────
  const knownSnipers = await redis.smembers(`snipers_set:${mint}`);

  const sniperWallets: any[] = [];
  const insiderWallets: any[] = [];

  for (const w of top10Wallets) {
    if (w.owner === creator) continue;
    if (w.percentage < 0.2) continue;

    const walletAddress = w.owner || w.address;
    const isKnownSniper = knownSnipers.includes(walletAddress);

    if (isKnownSniper || w.percentage >= 1.0) {
      sniperWallets.push({ wallet: walletAddress, percentage: w.percentage, amount: w.amount, isTimeBasedSniper: isKnownSniper });
    } else if (w.percentage >= 0.5) {
      insiderWallets.push({ wallet: walletAddress, percentage: w.percentage, amount: w.amount });
    }
  }

  const sniperTotalPct = sniperWallets.reduce((s, w) => s + w.percentage, 0);
  const sniperTotalBal = sniperWallets.reduce((s, w) => s + w.amount, 0);
  const insiderTotalPct = insiderWallets.reduce((s, w) => s + w.percentage, 0);
  const insiderTotalBal = insiderWallets.reduce((s, w) => s + w.amount, 0);

  // ── Dev stats: O(1) lookup via creator index (no full-table scan) ─────────
  // Creator stats are maintained incrementally in yellowstone-manager and curve-tracker
  // via hincrby on creator:{address} — no need to scan all tokens.
  let devStats = { total_launched: 0, total_migrated: 0 };
  if (creator) {
    const stats = await redis.hgetall(`creator:${creator}`);
    devStats = {
      total_launched: parseInt(stats.launched || "0"),
      total_migrated: parseInt(stats.migrated || "0"),
    };
  }

  // ── Write to Redis ────────────────────────────────────────────────────────
  const update: Record<string, string> = {
    top10:             top10Pct.toFixed(2),
    snipers:           JSON.stringify(sniperWallets),
    snipersCount:      sniperWallets.length.toString(),
    snipersTotalPct:   sniperTotalPct.toFixed(2),
    snipersTotalBal:   sniperTotalBal.toFixed(0),
    insiders:          JSON.stringify(insiderWallets),
    insidersCount:     insiderWallets.length.toString(),
    insidersTotalPct:  insiderTotalPct.toFixed(2),
    insidersTotalBal:  insiderTotalBal.toFixed(0),
    devPercentage:     devPercentage.toFixed(2),
    devAmount:         devAmount.toFixed(0),
    devStats:          JSON.stringify(devStats),
    riskAnalyzedAt:    Date.now().toString(),
  };

  await redis.hset(`token:${mint}`, update);
  console.log(`[Risk] ✅ ${tokenData.name} | top10=${top10Pct.toFixed(1)}% snipers=${sniperWallets.length} (${sniperTotalPct.toFixed(1)}%) dev=${devPercentage.toFixed(1)}%`);
  await redis.publish("token-updates", mint);
}
