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
  "https://ipfs.io/ipfs/",
  "https://gateway.pinata.cloud/ipfs/",
];

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
      const mints = await redis.zrevrange("tokens:latest", 0, 49);
      let retried = 0;
      for (const mint of mints) {
        const [image, name, symbol] = await redis.hmget(`token:${mint}`, "image", "name", "symbol");
        if (!image || image === "" || name === "Unknown Token" || symbol === "???") {
          enrichToken(mint).catch(() => {});
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
  const url = resolveIpfsUrl(uri);

  // Try the resolved primary URL first
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 4000);
    const res = await fetch(url, { signal: ctrl.signal, headers: { "Accept": "application/json" } });
    clearTimeout(t);
    if (res.ok) return await res.json();
  } catch {}

  // IPFS fallback: race all gateways in parallel — take the first to respond successfully.
  // Sequential retries are the old bottleneck (3× 5s timeouts = up to 15s). Parallel = one wait.
  if (uri.includes("ipfs")) {
    const hash = extractIpfsHash(uri);
    if (hash) {
      return await Promise.any(
        IPFS_GATEWAYS.map(async gw => {
          const ctrl = new AbortController();
          const t = setTimeout(() => ctrl.abort(), 5000);
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
  }

  return null;
}

function resolveIpfsUrl(uri: string): string {
  if (uri.startsWith("ipfs://")) return "https://cf-ipfs.com/ipfs/" + uri.slice(7);
  return uri;
}

function extractIpfsHash(uri: string): string | null {
  const match = uri.match(/\/ipfs\/([A-Za-z0-9]+)/);
  if (match) return match[1];
  if (uri.startsWith("ipfs://")) return uri.slice(7);
  return null;
}
