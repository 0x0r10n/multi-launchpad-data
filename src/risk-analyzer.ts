// src/risk-analyzer.ts — Snipers, insiders, dev stats, top10 holder concentration
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";
import { MOON_EXCLUDE_WALLETS } from "./launchpads";

const redis = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const TOKEN_TOTAL_SUPPLY = 1_000_000_000;
const DECIMALS = 6;
const RAW_SUPPLY = TOKEN_TOTAL_SUPPLY * Math.pow(10, DECIMALS);
const inFlight = new Set<string>();

export function queueRiskAnalysis(mint: string) {
  if (inFlight.has(mint)) return;
  inFlight.add(mint);
  analyzeRisk(mint).catch(() => {}).finally(() => inFlight.delete(mint));
}

async function analyzeRisk(mint: string) {
  const tokenData = await redis.hgetall(`token:${mint}`);
  if (!tokenData?.mint) return;

  const lastAnalysis = parseInt(tokenData.riskAnalyzedAt || "0");
  if (Date.now() - lastAnalysis < 60_000) return;

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

  // ── Batch resolve token account owners + token supply in parallel ───────────
  // SPL token account layout: mint[0..32] | owner[32..64] | amount[64..72] | ...
  const tokenAccountPubkeys = holdersToResolve.map(h => h.address);
  const [accountInfos, supplyInfo] = await Promise.all([
    connection.getMultipleAccountsInfo(tokenAccountPubkeys),
    connection.getTokenSupply(mintPubkey).catch(() => null),
  ]);

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

  const totalSupplyRaw = supplyInfo ? Number(supplyInfo.value.amount) : RAW_SUPPLY;

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

  // ── Dev stats: pipeline all creator+complete lookups in one roundtrip ───────
  let devStats = { total_launched: 0, total_migrated: 0 };
  if (creator) {
    const allMints = await redis.zrange("tokens:latest", 0, -1);
    if (allMints.length > 0) {
      const pipeline = redis.pipeline();
      for (const m of allMints) pipeline.hmget(`token:${m}`, "creator", "complete");
      const results = await pipeline.exec();

      let launched = 0, migrated = 0;
      for (const r of results || []) {
        const [c, complete] = (r?.[1] as [string, string]) || [];
        if (c === creator) {
          launched++;
          if (complete === "true") migrated++;
        }
      }
      devStats = { total_launched: launched, total_migrated: migrated };
    }
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

export function startRiskAnalysisLoop() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 49);
      for (const mint of mints) queueRiskAnalysis(mint);
    } catch (e: any) {
      console.error("[Risk] Loop error:", e.message);
    }
  }, 30_000);

  console.log("[Risk] Started — analysis every 30s for top 50 tokens");
}
