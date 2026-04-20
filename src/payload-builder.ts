// src/payload-builder.ts — Shared payload assembly used by WS broadcaster and REST router
import Redis from "ioredis";
import "dotenv/config";
import { getPriceHistory } from "./price-tracker";

const redis = new Redis(process.env.REDIS_URL!);

export function safeParse(json: string | undefined, fallback: any): any {
  if (!json) return fallback;
  try { return JSON.parse(json); } catch { return fallback; }
}

/**
 * Build the canonical token envelope used by every WebSocket and REST response.
 *
 * forceRebuild=true  → skip payload cache (REST endpoints always want fresh data)
 * forceRebuild=false → use 10s Redis cache (WS pubsub path, avoids rebuilding on rapid bursts)
 *
 * priceHistoryOverride=[] skips the Redis lrange (new-launch broadcast where history is empty).
 */
export async function buildTokenPayload(
  d: Record<string, string>,
  forceRebuild = false,
  priceHistoryOverride?: any[],
): Promise<any> {
  const mint = d.mint || "";

  if (!forceRebuild && mint) {
    const cached = await redis.get(`payload:${mint}`);
    if (cached) return JSON.parse(cached);
  }

  const createdAt = parseInt(d.createdAt || Date.now().toString());
  const platform  = d.platform || "pump";

  const priceHistory = priceHistoryOverride !== undefined
    ? priceHistoryOverride
    : (mint ? await getPriceHistory(mint, 50) : []);

  const createdOnUrl =
    platform === "moon"      ? "https://moon.it" :
    platform === "bags"      ? "https://bags.fm" :
    platform === "letsbonk"  ? "https://letsbonk.fun" :
    platform === "launchlab" ? "https://raydium.io/launchlab" :
    platform === "meteora"   ? "https://meteora.ag" :
                               "https://pump.fun";

  const payload = {
    type: "message",
    room: "new",
    data: {
      token: {
        name:              d.name    || "Unknown Token",
        symbol:            d.symbol  || "???",
        mint,
        uri:               d.uri     || "",
        decimals:          parseInt(d.decimals || "6"),
        description:       d.description || "",
        image:             d.image   || "",
        hasFileMetaData:   !!(d.uri),
        isMayhemMode:      d.isMayhemMode      === "true",
        isCashbackEnabled: d.isCashbackEnabled === "true",
        createdOn: createdOnUrl,
        strictSocials: {
          twitter:  d.twitter  || "",
          telegram: d.telegram || "",
          website:  d.website  || "",
        },
        creation: {
          creator:      d.creator  || "",
          created_tx:   d.createdTx || "",
          created_time: Math.floor(createdAt / 1000),
        },
      },
      pools: [{
        poolId:   d.curvePDA || "",
        liquidity: {
          quote: parseFloat(d.liquidity    || "0"),
          usd:   parseFloat(d.liquidityUsd || "0"),
        },
        price: {
          quote: parseFloat(d.priceQuote || "0"),
          usd:   parseFloat(d.priceUsd   || "0"),
        },
        tokenSupply: 1_000_000_000_000_000,
        lpBurn:      100,
        tokenAddress: mint,
        marketCap: {
          quote: parseFloat(d.marketCapQuote || "0"),
          usd:   parseFloat(d.marketCapUsd   || "0"),
        },
        decimals: parseInt(d.decimals || "6"),
        security: { freezeAuthority: null, mintAuthority: null },
        quoteToken:    "So11111111111111111111111111111111111111112",
        market:        platform === "pump" ? "pumpfun" : platform,
        deployer:      d.creator || "",
        lastUpdated:   Date.now(),
        createdAt,
        txns: {
          buys:      parseInt(d.buys      || "0"),
          sells:     parseInt(d.sells     || "0"),
          total:     parseInt(d.totalTxns || "0"),
          volume:    parseFloat(d.volumeUsd    || "0"),
          volume24h: parseFloat(d.volume24hUsd || "0"),
        },
        curvePercentage: parseFloat(d.curvePercentage || "0"),
        creation: {
          creator:      d.creator  || "",
          created_tx:   d.createdTx || "",
          created_time: createdAt,
        },
      }],
      events: {
        "1m":  { priceChangePercentage: parseFloat(d["1m"]  || "0") },
        "5m":  { priceChangePercentage: parseFloat(d["5m"]  || "0") },
        "15m": { priceChangePercentage: parseFloat(d["15m"] || "0") },
        "30m": { priceChangePercentage: parseFloat(d["30m"] || "0") },
        "1h":  { priceChangePercentage: parseFloat(d["1h"]  || "0") },
        "4h":  { priceChangePercentage: parseFloat(d["4h"]  || "0") },
        "24h": { priceChangePercentage: parseFloat(d["24h"] || "0") },
      },
      risk: {
        snipers: {
          count:           parseInt(d.snipersCount   || "0"),
          totalBalance:    parseFloat(d.snipersTotalBal || "0"),
          totalPercentage: parseFloat(d.snipersTotalPct || "0"),
          wallets:         safeParse(d.snipers, []),
        },
        insiders: {
          count:           parseInt(d.insidersCount   || "0"),
          totalBalance:    parseFloat(d.insidersTotalBal || "0"),
          totalPercentage: parseFloat(d.insidersTotalPct || "0"),
          wallets:         safeParse(d.insiders, []),
        },
        top10: parseFloat(d.top10 || "0"),
        dev: {
          percentage: parseFloat(d.devPercentage || "0"),
          amount:     parseFloat(d.devAmount     || "0"),
          stats:      safeParse(d.devStats, { total_launched: 0, total_migrated: 0 }),
        },
      },
      graduation: {
        status: d.graduationStatus || (parseFloat(d.curvePercentage || "0") >= 80 ? "graduating" : "new"),
      },
      dev_stats: safeParse(d.devStats, { total_launched: 0, total_migrated: 0 }),
      priceHistory,
      meta: (() => {
        const hasPrice     = parseFloat(d.priceUsd || "0") > 0;
        const hasBasicRisk = !!d.devStats;
        const hasTop10     = d.top10 !== undefined && d.top10 !== "";
        const hasRisk      = !!(d.riskQuickScanAt || d.riskAnalyzedAt);
        const hasImage     = !!(d.image && d.image !== "");
        const dataQuality: "complete" | "partial" | "skeleton" =
          hasPrice && hasTop10 ? "complete" : hasPrice || hasBasicRisk ? "partial" : "skeleton";
        return {
          dataQuality,
          hasPrice,
          hasBasicRisk,
          hasTop10,
          hasRisk,
          hasImage,
          expectedRichBy:   parseInt(d.createdAt || "0") + 450,
          richPayloadDelay: d.richPayloadMs ? parseInt(d.richPayloadMs) : null,
        };
      })(),
    },
  };

  if (mint) await redis.setex(`payload:${mint}`, 10, JSON.stringify(payload));
  return payload;
}
