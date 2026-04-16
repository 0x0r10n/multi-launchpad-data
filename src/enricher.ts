// src/enricher.ts — 100% on-chain enrichment via Metaplex PDA + off-chain URI fetch
import Redis from "ioredis";
import "dotenv/config";
import { createUmi } from "@metaplex-foundation/umi-bundle-defaults";
import { fetchMetadata as fetchOnChainMetadata, findMetadataPda } from "@metaplex-foundation/mpl-token-metadata";
import { publicKey } from "@metaplex-foundation/umi";

const redis = new Redis(process.env.REDIS_URL!);
const umi = createUmi(process.env.SOLANA_RPC!);

const IPFS_GATEWAYS = [
  "https://cf-ipfs.com/ipfs/",
  "https://cloudflare-ipfs.com/ipfs/",
  "https://ipfs.io/ipfs/",
  "https://gateway.pinata.cloud/ipfs/",
  "https://nftstorage.link/ipfs/",
  "https://ipfs.filebase.io/ipfs/",
];

// Arweave gateway for ar:// URIs (common for Pump.fun tokens)
const ARWEAVE_GATEWAY = "https://arweave.net/";

export function queueEnrichment(mint: string) {
  enrichToken(mint).catch(e => console.error(`[Enricher] Failed ${mint.slice(0, 8)}: ${e.message}`));
}

async function enrichToken(mint: string) {
  const existing = await redis.hgetall(`token:${mint}`);
  if (!existing?.mint) return;

  if (existing.image && existing.image !== "") return;

  // === ON-CHAIN: Get URI ===
  let uri = existing.uri || "";

  if (!uri) {
    // URI wasn't parsed from the tx (common for Moon/Bags/LaunchLab) — fetch Metaplex PDA.
    // Pump.fun and LetsBonk always embed URI in the tx, so this branch is skipped for them.
    try {
      const metadataPda = findMetadataPda(umi, { mint: publicKey(mint) });
      const onChain = await fetchOnChainMetadata(umi, metadataPda);

      if (onChain.uri) {
        uri = onChain.uri.replace(/\0/g, "").trim();
      }

      // Opportunistically fix missing name/symbol from on-chain data
      const patch: Record<string, string> = {};
      const nameMissing   = !existing.name   || existing.name   === "Unknown Token";
      const symbolMissing = !existing.symbol || existing.symbol === "???";
      if (nameMissing   && onChain.name)   patch.name   = onChain.name.replace(/\0/g, "").trim();
      if (symbolMissing && onChain.symbol) patch.symbol = onChain.symbol.replace(/\0/g, "").trim();
      if (Object.keys(patch).length > 0) await redis.hset(`token:${mint}`, patch);
    } catch {
      // RPC unavailable or account not found — nothing to do
    }
  }

  if (!uri) return;

  // === OFF-CHAIN: Fetch JSON from token URI (IPFS/HTTP) ===
  const metadata = await fetchOffChainMetadata(uri);
  if (!metadata) return;

  const update: Record<string, string> = {};

  if (metadata.image)       update.image       = resolveIpfsUrl(metadata.image);
  if (metadata.description) update.description = metadata.description;
  if (metadata.twitter)     update.twitter     = metadata.twitter;
  if (metadata.telegram)    update.telegram    = metadata.telegram;
  if (metadata.website)     update.website     = metadata.website;

  if (metadata.extensions) {
    if (metadata.extensions.twitter)  update.twitter  = metadata.extensions.twitter;
    if (metadata.extensions.telegram) update.telegram = metadata.extensions.telegram;
    if (metadata.extensions.website)  update.website  = metadata.extensions.website;
  }
  if (metadata.properties?.links) {
    const links = metadata.properties.links;
    if (links.twitter)  update.twitter  = links.twitter;
    if (links.telegram) update.telegram = links.telegram;
    if (links.website)  update.website  = links.website;
  }

  if (Object.keys(update).length > 0) {
    await redis.hset(`token:${mint}`, update);
    const hasSocials = !!(update.twitter || update.telegram || update.website);
    console.log(`[Enricher] ✅ ${existing.name} ($${existing.symbol}) | image=${!!update.image} desc=${!!update.description} socials=${hasSocials}`);
    await redis.publish("token-updates", mint);
  }
}

// Retry enrichment for tokens still missing images every 15s
export function startEnrichmentSweep() {
  setInterval(async () => {
    try {
      const mints = await redis.zrevrange("tokens:latest", 0, 199);
      let retried = 0;
      // Pipeline all 50 hmget calls into a single round-trip
      const pipeline = redis.pipeline();
      for (const mint of mints) pipeline.hmget(`token:${mint}`, "image", "name", "symbol");
      const results = await pipeline.exec();
      for (let i = 0; i < mints.length; i++) {
        const [image, name, symbol] = (results?.[i]?.[1] as string[] | null) ?? [];
        if (!image || image === "" || !name || name === "" || name === "Unknown Token" || !symbol || symbol === "" || symbol === "???") {
          enrichToken(mints[i]).catch(() => {});
          retried++;
        }
      }
      if (retried > 0) console.log(`[Enricher] 🔄 Retrying ${retried} un-enriched tokens`);
    } catch (e: any) {
      console.error("[Enricher] Sweep error:", e.message);
    }
  }, 15_000);
}

async function fetchOffChainMetadata(uri: string): Promise<any | null> {
  // Check Redis cache first — 24h TTL so re-launches or refreshes are instant
  const cacheKey = `meta-cache:${uri.slice(0, 200)}`;
  try {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
  } catch {}

  let metadata: any | null = null;

  const isIpfs = uri.startsWith("ipfs://") || uri.includes("/ipfs/");

  if (isIpfs) {
    // Race all IPFS gateways in parallel — no sequential primary attempt.
    // Promise.any takes the first success; gateways that fail are ignored.
    const hash = extractIpfsHash(uri);
    if (hash) {
      metadata = await Promise.any(
        IPFS_GATEWAYS.map(async gw => {
          const ctrl = new AbortController();
          const t = setTimeout(() => ctrl.abort(), 4000);
          try {
            const res = await fetch(gw + hash, { signal: ctrl.signal, headers: { "Accept": "application/json" } });
            clearTimeout(t);
            if (!res.ok) throw new Error("not ok");
            return await res.json();
          } catch (e) {
            clearTimeout(t);
            throw e;
          }
        }),
      ).catch(() => null);
    }
  } else {
    // Direct URL (HTTPS, Arweave via resolveIpfsUrl, pump.fun servers, etc.)
    const url = resolveIpfsUrl(uri);
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 3000);
      const res = await fetch(url, { signal: ctrl.signal, headers: { "Accept": "application/json" } });
      clearTimeout(t);
      if (res.ok) metadata = await res.json();
    } catch {}
  }

  if (metadata) {
    redis.setex(cacheKey, 86400, JSON.stringify(metadata)).catch(() => {});
  }
  return metadata;
}

function resolveIpfsUrl(uri: string): string {
  if (uri.startsWith("ipfs://")) return "https://cf-ipfs.com/ipfs/" + uri.slice(7);
  if (uri.startsWith("ar://"))   return ARWEAVE_GATEWAY + uri.slice(5);
  return uri;
}

function extractIpfsHash(uri: string): string | null {
  const match = uri.match(/\/ipfs\/([A-Za-z0-9]+)/);
  if (match) return match[1];
  if (uri.startsWith("ipfs://")) return uri.slice(7);
  return null;
}
