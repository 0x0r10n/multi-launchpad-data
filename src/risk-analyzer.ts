// src/risk-analyzer.ts — Snipers, insiders, dev stats, top10 holder concentration
import { Connection, PublicKey } from "@solana/web3.js";
import Redis from "ioredis";
import "dotenv/config";

const redis = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

const TOKEN_TOTAL_SUPPLY = 1_000_000_000; // 1B tokens (6 decimals = 1e15 raw)
const DECIMALS = 6;
const RAW_SUPPLY = TOKEN_TOTAL_SUPPLY * Math.pow(10, DECIMALS);
const inFlight = new Set<string>();

export function queueRiskAnalysis(mint: string) {
  if (inFlight.has(mint)) return;
  inFlight.add(mint);
  analyzeRisk(mint).catch(e => {}).finally(() => inFlight.delete(mint));
}

async function analyzeRisk(mint: string) {
  const tokenData = await redis.hgetall(`token:${mint}`);
  if (!tokenData?.mint) return;

  // Skip if analyzed recently (within 60s)
  const lastAnalysis = parseInt(tokenData.riskAnalyzedAt || "0");
  if (Date.now() - lastAnalysis < 60_000) return;

  const mintPubkey = new PublicKey(mint);
  const creator = tokenData.creator || "";

  // Get largest token holders
  let largestAccounts;
  try {
    largestAccounts = await connection.getTokenLargestAccounts(mintPubkey);
  } catch {
    return; // RPC error, skip
  }

  if (!largestAccounts?.value?.length) return;

  const holders = largestAccounts.value;

  // ===== TOP 10 HOLDER CONCENTRATION =====
  // Resolve the owners for the top 15 largest accounts so we can filter out bonding curve
  // Perform the 15 RPC calls in PARALLEL to make it instantaneous
  const holdersToResolve = holders.slice(0, 15);
  
  const resolvedHolders = await Promise.all(
    holdersToResolve.map(async (holder) => {
      let owner = "";
      try {
        const accInfo = await connection.getParsedAccountInfo(holder.address);
        const parsed = (accInfo?.value?.data as any)?.parsed;
        owner = parsed?.info?.owner || "";
      } catch {}

      return {
        address: holder.address.toBase58(),
        owner,
        rawAmount: Number(holder.amount)
      };
    })
  );

  const curvePDA = tokenData.curvePDA || "";
  const platform = tokenData.platform || "pump";

  const excludeWallets = platform === "moon" 
    ? ["MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG", "Moonit1111111111111111111111111111111111111"] // Moon.it / Moonshot fee/treasury
    : [];

  // Filter out the bonding curve and any platform-specific treasury wallets
  const validHolders = resolvedHolders.filter(h => 
    h.owner !== curvePDA && 
    h.address !== curvePDA &&
    !excludeWallets.includes(h.owner) &&
    !excludeWallets.includes(h.address)
  );

  
  const top10RawAmount = validHolders
    .slice(0, 10)
    .reduce((sum, h) => sum + h.rawAmount, 0);

  let totalSupplyRaw = RAW_SUPPLY;
  try {
    const supplyInfo = await connection.getTokenSupply(mintPubkey);
    totalSupplyRaw = Number(supplyInfo.value.amount);
  } catch (e) {}

  let top10Pct = 0;
  if (totalSupplyRaw > 0) {
    top10Pct = Math.min(Math.max((top10RawAmount / totalSupplyRaw) * 100, 0), 100);
  }

  // Build the top 10 wallets array for dev & sniper checks
  const top10Wallets: any[] = [];
  for (let i = 0; i < Math.min(10, validHolders.length); i++) {
    const holder = validHolders[i];
    const percentage = (holder.rawAmount / totalSupplyRaw) * 100;

    top10Wallets.push({
      address: holder.address,
      owner: holder.owner,
      amount: holder.rawAmount / Math.pow(10, DECIMALS),
      percentage: parseFloat(percentage.toFixed(2)),
    });
  }

  // ===== DEV HOLDINGS =====
  let devPercentage = 0;
  let devAmount = 0;
  if (creator) {
    for (const w of top10Wallets) {
      if (w.owner === creator) {
        devPercentage = w.percentage;
        devAmount = w.amount;
        break;
      }
    }
  }

  // Fetch time-based snipers logged during the first 20s of trading
  const knownSnipers = await redis.smembers(`snipers_set:${mint}`);

  const sniperWallets: any[] = [];
  const insiderWallets: any[] = [];

  for (const w of top10Wallets) {
    if (w.owner === creator) continue; // Skip dev (counted separately)
    if (w.percentage < 0.2) continue; // Skip dust

    const walletAddress = w.owner || w.address;
    const isKnownSniper = knownSnipers.includes(walletAddress);

    // Snipers: Time-based snipers OR massive holders (>1% of supply)
    if (isKnownSniper || w.percentage >= 1.0) {
      sniperWallets.push({
        wallet: walletAddress,
        percentage: w.percentage,
        amount: w.amount,
        isTimeBasedSniper: isKnownSniper
      });
    }
    // Insiders: moderate holders (0.5% - 1%)
    else if (w.percentage >= 0.5) {
      insiderWallets.push({
        wallet: walletAddress,
        percentage: w.percentage,
        amount: w.amount,
      });
    }
  }

  const sniperTotalPct = sniperWallets.reduce((s, w) => s + w.percentage, 0);
  const sniperTotalBal = sniperWallets.reduce((s, w) => s + w.amount, 0);
  const insiderTotalPct = insiderWallets.reduce((s, w) => s + w.percentage, 0);
  const insiderTotalBal = insiderWallets.reduce((s, w) => s + w.amount, 0);

  // ===== DEV STATS (how many tokens this creator has launched) =====
  let devStats = { total_launched: 0, total_migrated: 0 };
  if (creator) {
    // Count how many tokens this creator has in our db
    const allMints = await redis.zrange("tokens:latest", 0, -1);
    let launched = 0;
    let migrated = 0;
    for (const m of allMints) {
      const c = await redis.hget(`token:${m}`, "creator");
      if (c === creator) {
        launched++;
        const complete = await redis.hget(`token:${m}`, "complete");
        if (complete === "true") migrated++;
      }
    }
    devStats = { total_launched: launched, total_migrated: migrated };
  }

  // Store in Redis
  const update: Record<string, string> = {
    top10: top10Pct.toFixed(2),
    snipers: JSON.stringify(sniperWallets),
    snipersCount: sniperWallets.length.toString(),
    snipersTotalPct: sniperTotalPct.toFixed(2),
    snipersTotalBal: sniperTotalBal.toFixed(0),
    insiders: JSON.stringify(insiderWallets),
    insidersCount: insiderWallets.length.toString(),
    insidersTotalPct: insiderTotalPct.toFixed(2),
    insidersTotalBal: insiderTotalBal.toFixed(0),
    devPercentage: devPercentage.toFixed(2),
    devAmount: devAmount.toFixed(0),
    devStats: JSON.stringify(devStats),
    riskAnalyzedAt: Date.now().toString(),
  };

  await redis.hset(`token:${mint}`, update);
  console.log(`[Risk] ✅ ${tokenData.name} | top10=${top10Pct.toFixed(1)}% snipers=${sniperWallets.length} (${sniperTotalPct.toFixed(1)}%) dev=${devPercentage.toFixed(1)}%`);
  await redis.publish("token-updates", mint);
}

export function startRiskAnalysisLoop() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 19);
      for (const mint of mints) {
        queueRiskAnalysis(mint);
      }
    } catch (e: any) {
      console.error("[Risk] Loop error:", e.message);
    }
  }, 60_000);

  console.log("[Risk] Started — analysis every 60s for top 20 tokens");
}
