// src/enricher.ts — Instant metadata enrichment via Pump.fun API + IPFS fallback
import Redis from "ioredis";
import "dotenv/config";

const redis = new Redis(process.env.REDIS_URL!);

const IPFS_GATEWAYS = [
  "https://cf-ipfs.com/ipfs/",
  "https://ipfs.io/ipfs/",
  "https://gateway.pinata.cloud/ipfs/",
];

export function queueEnrichment(mint: string) {
  enrichToken(mint).catch(e => console.error(`[Enricher] Failed ${mint.slice(0, 8)}: ${e.message}`));
}

async function enrichToken(mint: string) {
  const existing = await redis.hgetall(`token:${mint}`);
  if (!existing?.mint) return;

  // Skip if already enriched
  if (existing.image && existing.image !== "") return;

  // === PRIMARY: Pump.fun API (instant, reliable) ===
  let enriched = false;
  
  if (!existing.platform || existing.platform === "pump") {
    try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);

    const res = await fetch(`https://frontend-api-v3.pump.fun/coins/${mint}`, {
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (res.ok) {
      const data: any = await res.json();
      const update: Record<string, string> = {};

      if (data.image_uri) update.image = data.image_uri;
      if (data.description) update.description = data.description;
      if (data.twitter) update.twitter = data.twitter;
      if (data.telegram) update.telegram = data.telegram;
      if (data.website) update.website = data.website;

      if (Object.keys(update).length > 0) {
        await redis.hset(`token:${mint}`, update);
        enriched = true;
        const hasSocials = !!(update.twitter || update.telegram || update.website);
        console.log(`[Enricher] ✅ ${existing.name} ($${existing.symbol}) | pump.fun API | image=${!!update.image} desc=${!!update.description} socials=${hasSocials}`);
        await redis.publish("token-updates", mint);
        return;
      }
    }
  } catch {
    // Pump.fun API failed, fall through to IPFS
  }
  }

  // === FALLBACK: Fetch from token URI (IPFS/HTTP) ===
  if (!enriched && existing.uri) {
    const metadata = await fetchMetadata(existing.uri);
    if (metadata) {
      const update: Record<string, string> = {};

      if (metadata.image) update.image = resolveIpfsUrl(metadata.image);
      if (metadata.description) update.description = metadata.description;

      // Parse socials
      if (metadata.twitter) update.twitter = metadata.twitter;
      if (metadata.telegram) update.telegram = metadata.telegram;
      if (metadata.website) update.website = metadata.website;
      if (metadata.extensions) {
        if (metadata.extensions.twitter) update.twitter = metadata.extensions.twitter;
        if (metadata.extensions.telegram) update.telegram = metadata.extensions.telegram;
        if (metadata.extensions.website) update.website = metadata.extensions.website;
      }
      if (metadata.properties?.links) {
        const links = metadata.properties.links;
        if (links.twitter) update.twitter = links.twitter;
        if (links.telegram) update.telegram = links.telegram;
        if (links.website) update.website = links.website;
      }

      if (Object.keys(update).length > 0) {
        await redis.hset(`token:${mint}`, update);
        const hasSocials = !!(update.twitter || update.telegram || update.website);
        console.log(`[Enricher] ✅ ${existing.name} ($${existing.symbol}) | IPFS fallback | image=${!!update.image} desc=${!!update.description} socials=${hasSocials}`);
        await redis.publish("token-updates", mint);
      }
    }
  }
}

// Retry enrichment for tokens that are still missing data
export function startEnrichmentSweep() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 49);
      let retried = 0;
      for (const mint of mints) {
        const image = await redis.hget(`token:${mint}`, "image");
        if (!image || image === "") {
          enrichToken(mint).catch(() => {});
          retried++;
        }
      }
      if (retried > 0) {
        console.log(`[Enricher] 🔄 Retrying ${retried} un-enriched tokens`);
      }
    } catch (e: any) {
      console.error("[Enricher] Sweep error:", e.message);
    }
  }, 15_000); // Every 15 seconds
}

async function fetchMetadata(uri: string): Promise<any | null> {
  const url = resolveIpfsUrl(uri);

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { "Accept": "application/json" },
    });
    clearTimeout(timeout);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    // Try alternate IPFS gateways
    if (uri.includes("ipfs")) {
      const hash = extractIpfsHash(uri);
      if (hash) {
        for (const gw of IPFS_GATEWAYS) {
          try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 5000);
            const res = await fetch(gw + hash, {
              signal: controller.signal,
              headers: { "Accept": "application/json" },
            });
            clearTimeout(timeout);
            if (res.ok) return await res.json();
          } catch { continue; }
        }
      }
    }
    return null;
  }
}

function resolveIpfsUrl(uri: string): string {
  if (uri.startsWith("ipfs://")) {
    return "https://cf-ipfs.com/ipfs/" + uri.slice(7);
  }
  return uri;
}

function extractIpfsHash(uri: string): string | null {
  const match = uri.match(/\/ipfs\/([A-Za-z0-9]+)/);
  if (match) return match[1];
  if (uri.startsWith("ipfs://")) return uri.slice(7);
  return null;
}
