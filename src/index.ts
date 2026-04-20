// src/index.ts — WebSocket broadcaster + /new endpoint + Live Broadcasting
import express from "express";
import { Server } from "socket.io";
import { YellowstoneManager } from "./yellowstone-manager";
import { queueEnrichment, startEnrichmentSweep } from "./enricher";
import { processCurveAccountUpdate, fetchAndDecodeCurve } from "./curve-tracker";
import { startPriceTracker, getPriceHistory, recordPrice } from "./price-tracker";
import { queueRiskAnalysis, queueInitialQuickRisk, startHolderSnapshot } from "./risk-analyzer";
import { buildTokenPayload } from "./payload-builder";
import { router as apiRouter } from "./routes/api";
import Redis from "ioredis";
import "dotenv/config";
import cors from "cors";

const app = express();
app.use(cors());
app.use(express.json());
app.use("/api", apiRouter);

const server = app.listen(3000, () => console.log("→ API: http://localhost:3000"));
const io = new Server(server, { cors: { origin: "*" } });

const redis = new Redis(process.env.REDIS_URL!);
const subscriber = new Redis(process.env.REDIS_URL!);
const yellowstone = new YellowstoneManager();

// Pending first-curve resolvers: mint → resolve fn.
// Registered synchronously on new-launch so Geyser delivery is never missed.
const pendingFirstCurve = new Map<string, (fields: Record<string, string> | null) => void>();

// In-memory buffer for rich payload latency telemetry (ms from launch to rich broadcast).
// Flushed every 5 minutes with p50/p95/p99 — no per-token Redis overhead.
const richDelayBuffer: number[] = [];
// Tracks how many rich broadcasts included top10 via the inline holder path.
let top10InlineCount = 0;
// Count of launches where all curve probes returned null/zero → rich broadcast was blocked.
// Target: <0.5% of launches. Flushed every 5 minutes with other telemetry.
let zeroPriceBlockedCount = 0;

// Race two curve-data promises — first non-null result wins.
// If both resolve null, returns null (skeleton stays until Geyser pushes later).
function raceCurveData(
  a: Promise<Record<string, string> | null>,
  b: Promise<Record<string, string> | null>,
): Promise<Record<string, string> | null> {
  return new Promise(resolve => {
    let resolved: Record<string, string> | null = null;
    let remaining = 2;
    function onResult(v: Record<string, string> | null) {
      remaining--;
      if (v !== null && resolved === null) { resolved = v; resolve(v); }
      else if (remaining === 0 && resolved === null) resolve(null);
    }
    a.then(onResult).catch(() => onResult(null));
    b.then(onResult).catch(() => onResult(null));
  });
}

yellowstone.start().catch((err) => console.error("Yellowstone start failed:", err));

startPriceTracker();
startEnrichmentSweep();

// ========== WEBSOCKET ROOMS ==========
io.on("connection", (socket) => {
  // Auto-join "new" room — client gets future new-launch events immediately
  socket.join("new");

  // Send a snapshot of the latest 50 tokens immediately so the client has data
  // without waiting for the next launch or having to call /new via REST.
  redis.zrevrange("tokens:latest", 0, 49).then(async mints => {
    if (!mints.length) return;
    const pipeline = redis.pipeline();
    for (const mint of mints) pipeline.hgetall(`token:${mint}`);
    const results = await pipeline.exec();
    const tokenDatas = results?.map(r => r?.[1] as Record<string, string> | null) ?? [];
    const payloads = await Promise.all(
      tokenDatas.filter(d => d?.mint).map(d => buildTokenPayload(d!))
    );
    socket.emit("snapshot", { type: "snapshot", room: "new", data: payloads });
  }).catch(() => {});

  socket.on("leave", (room: string) => {
    socket.leave(room);
  });

  socket.on("join", async (room: string) => {
    socket.join(room);

    // Send current state immediately so clients don't wait for the next broadcast cycle
    try {
      if (room === "graduating") {
        const tokens = await getTokensByStatus("graduating");
        socket.emit("snapshot", { type: "snapshot", room: "graduating", data: tokens });

      } else if (room === "graduated") {
        const tokens = await getTokensByStatus("graduated");
        socket.emit("snapshot", { type: "snapshot", room: "graduated", data: tokens });

      } else if (room === "trending") {
        const trendingList = await calculateTrendingTokens();
        socket.emit("snapshot", { type: "snapshot", room: "trending", data: trendingList });

      } else if (room.startsWith("chart:")) {
        const mint = room.slice(6);
        if (mint) {
          const history = await getPriceHistory(mint, 500);
          socket.emit("snapshot", {
            type: "snapshot",
            room,
            data: { mint, priceHistory: history },
          });
        }

      } else if (room.startsWith("token:")) {
        const mint = room.slice(6);
        if (mint) {
          const d = await redis.hgetall(`token:${mint}`);
          if (d?.mint) {
            const payload = await buildTokenPayload(d, true);
            socket.emit("snapshot", { type: "snapshot", room, data: payload });
          }
        }

      } else if (room.startsWith("transactions:")) {
        const mint = room.slice("transactions:".length);
        if (mint) {
          // Newest 100 entries, slot-ordered descending (highest slot first)
          const raw = await redis.zrevrange(`txs:${mint}`, 0, 99, "WITHSCORES");
          const history: any[] = [];
          const sigs: string[] = [];
          for (let i = 0; i < raw.length; i += 2) {
            try {
              const entry = JSON.parse(raw[i]);
              entry.slot = parseInt(raw[i + 1]);
              history.push(entry);
              sigs.push(entry.signature);
            } catch {}
          }
          // Batch-fetch timestamps from secondary index
          if (sigs.length > 0) {
            const metas = await redis.hmget(`txs_meta:${mint}`, ...sigs);
            for (let i = 0; i < history.length; i++) {
              try {
                const m = metas[i] ? JSON.parse(metas[i]!) : null;
                history[i].timestamp = m?.ts ?? null;
              } catch { history[i].timestamp = null; }
            }
          }
          socket.emit("snapshot", {
            type: "snapshot",
            room,
            data: { mint, count: history.length, history },
          });
        }
      }
    } catch (e: any) {
      console.error(`[WS] Snapshot failed for room ${room}:`, e.message);
    }
  });
});

// === ROOM BROADCAST HELPERS ===
function broadcastNewToken(payload: any) {
  io.to("new").emit("message", payload);
}

function broadcastGraduating(payload: any) {
  io.to("graduating").emit("message", payload);
}

function broadcastGraduated(payload: any) {
  io.to("graduated").emit("message", payload);
}

function broadcastTokenUpdate(mint: string, payload: any) {
  io.to(`token:${mint}`).emit("message", payload);
}

function broadcastTrending(trendingData: Record<string, any[]>) {
  io.to("trending").emit("message", {
    type: "trending",
    room: "trending",
    data: trendingData,  // { "1m": [...], "5m": [...], "30m": [...], "1h": [...] }
  });
}

// Dedicated chart room — emits only the latest tick (full history served via REST)
async function broadcastChartTick(mint: string) {
  try {
    const [priceQuote, priceUsd] = await redis.hmget(`token:${mint}`, "priceQuote", "priceUsd");
    if (!priceQuote) return;
    io.to(`chart:${mint}`).emit("message", {
      type: "chart",
      room: `chart:${mint}`,
      data: {
        mint,
        tick: { time: Date.now(), price: parseFloat(priceQuote), price_usd: parseFloat(priceUsd || "0") },
      },
    });
  } catch {}
}

// ========== EVENT HANDLERS ==========

// ── Geyser curve account update handler ─────────────────────────────────────
yellowstone.on("curve-update", async (event: any) => {
  const fields = await processCurveAccountUpdate(event.mint, event.data, event.platform, event.isStartup);

  // Resolve pending first-curve race if the new-launch handler is still waiting
  const resolver = pendingFirstCurve.get(event.mint);
  if (resolver && fields) {
    pendingFirstCurve.delete(event.mint);
    resolver(fields);
  }

  if (fields?.complete === "true" && event.curvePDA) {
    yellowstone.removeCurvePDA(event.curvePDA);
  }

  // Record price tick and push chart update AFTER Geyser confirms the post-trade price.
  // Doing this here (not in the trade handler) ensures the tick always uses the real
  // on-chain price, not the stale pre-trade value that was in Redis when the tx fired.
  if (!event.isStartup && fields && parseFloat(fields.priceUsd || "0") > 0) {
    recordPrice(event.mint).catch(() => {});
    broadcastChartTick(event.mint).catch(() => {});
  }
});

yellowstone.on("new-launch", async (data: any) => {
  const platform = data.platform || "pump";

  // ── t=0 SYNC: Register Geyser listener before any await ─────────────────────
  // Must be synchronous so we never miss a Geyser snapshot that arrives
  // while we're awaiting the skeleton build or curve race below.
  // 800ms window: Geyser account subscriptions take 400-800ms to propagate for
  // brand-new accounts. The old 300ms window was too tight and caused 100% misses.
  const geyserFirstCurve: Promise<Record<string, string> | null> = data.curvePDA
    ? new Promise(resolve => {
        pendingFirstCurve.set(data.mint, resolve);
        setTimeout(() => { if (pendingFirstCurve.delete(data.mint)) resolve(null); }, 800);
      })
    : Promise.resolve(null);

  // ── t=0 ASYNC: Fire all parallel work immediately ────────────────────────────
  // immediate=true: Geyser subscription goes out NOW, not after 20ms debounce
  if (data.curvePDA) yellowstone.addCurvePDA(data.curvePDA, data.mint, platform, true);

  // RPC probe with 800ms timeout — Chainstack confirmed commitment averages 400-600ms.
  // The old 250ms timeout was causing near-100% timeouts.
  const rpcProbe = data.curvePDA
    ? fetchAndDecodeCurve(data.mint, data.curvePDA, platform, 800)
    : Promise.resolve(null);

  // Holder snapshot: getTokenLargestAccounts (700ms internal timeout)
  let holderFields: { top10: string; devPercentage: string } | null = null;
  const holderPromise = data.curvePDA
    ? startHolderSnapshot(data.mint, data.creator || "", data.curvePDA, platform)
        .then(f => { holderFields = f; return f; })
    : Promise.resolve(null);

  // Redis reads start immediately — both done in ~1ms
  const sniperPromise  = redis.smembers(`snipers_set:${data.mint}`);
  const creatorPromise = data.creator
    ? redis.hgetall(`creator:${data.creator}`)
    : Promise.resolve({} as Record<string, string>);

  // ── Skeleton broadcast (t≈5ms) ──────────────────────────────────────────────
  const skeletonPayload = await buildTokenPayload(data, false, []);
  broadcastNewToken(skeletonPayload);
  broadcastTokenUpdate(data.mint, skeletonPayload);
  console.log(`[BROADCAST] 🚀 ${data.name} ($${data.symbol}) | ${data.mint.slice(0, 12)}...`);

  // ── Fast curve race: RPC (800ms) vs Geyser (800ms) ──────────────────────────
  let curveFields: Record<string, string> | null = null;
  if (data.curvePDA) {
    curveFields = await raceCurveData(rpcProbe, geyserFirstCurve);
    pendingFirstCurve.delete(data.mint);
  }

  // Validate using priceQuote (SOL-denominated) — computed directly from on-chain reserves,
  // independent of SOL/USD oracle. More reliable than priceUsd for zero-check.
  let curveValid = !!(curveFields && parseFloat(curveFields.priceQuote || "0") > 0);

  // ── Blocking RPC fallback (2s) — only fires if fast race missed ─────────────
  // This catches cases where Chainstack confirmed/processed takes >800ms.
  if (!curveValid && data.curvePDA) {
    console.log(`[BROADCAST] ⏳ Fast race missed for ${data.name} — trying 2s RPC fallback`);
    // Try processed commitment first (faster)
    const fallbackFields = await fetchAndDecodeCurve(data.mint, data.curvePDA, platform, 2000, "processed");
    if (fallbackFields && parseFloat(fallbackFields.priceQuote || "0") > 0) {
      curveFields = fallbackFields;
      curveValid = true;
    } else {
      // IF THAT FAILS: try one last time with CONFIRMED commitment.
      // Sometimes 'processed' is missing some accounts that 'confirmed' has (weird RPC behavior).
      console.log(`[BROADCAST] ⚠️ 2s processed fallback missed for ${data.name} — final attempt with confirmed commitment`);
      const finalFallback = await fetchAndDecodeCurve(data.mint, data.curvePDA, platform, 3000, "confirmed");
      if (finalFallback && parseFloat(finalFallback.priceQuote || "0") > 0) {
        curveFields = finalFallback;
        curveValid = true;
      }
    }
  }

  // ── Wait for parallel risk reads (always started at t=0) ────────────────────
  const [earlyBuyers, creatorHash] = await Promise.all([sniperPromise, creatorPromise]);

  // Wait for holder snapshot when we have price (or even without — it's already running)
  if (!holderFields) {
    holderFields = await Promise.race([
      holderPromise,
      new Promise<null>(r => setTimeout(() => r(null), curveValid ? 450 : 100)),
    ]);
  }

  const richPayloadMs = Date.now() - parseInt(data.createdAt || "0");
  richDelayBuffer.push(richPayloadMs);
  if (holderFields) top10InlineCount++;

  const richFields: Record<string, string> = {
    snipersCount: earlyBuyers.length.toString(),
    devStats:     JSON.stringify({
      total_launched: parseInt(creatorHash.launched || "0"),
      total_migrated: parseInt(creatorHash.migrated || "0"),
    }),
    richPayloadMs: richPayloadMs.toString(),
    ...(holderFields
      ? {
          top10:           holderFields.top10,
          devPercentage:   holderFields.devPercentage,
          riskQuickScanAt: Date.now().toString(),
        }
      : {}),
  };

  // ── CRITICAL: Never broadcast a "rich" payload with zero price ──────────────
  // If curve data is still missing after all probes, persist only risk data to
  // Redis (silent write) and do NOT broadcast a second payload. The Geyser
  // pubsub handler (processCurveAccountUpdate → token-updates → subscriber)
  // will trigger a full rich broadcast when the real curve data arrives.
  if (!curveValid) {
    zeroPriceBlockedCount++;
    // Persist risk fields silently — no broadcast with zeros
    redis.hset(`token:${data.mint}`, richFields).catch(() => {});
    console.log(
      `[BROADCAST] ⏸️ ${data.name} | price pending (skeleton live)` +
      ` | top10=${holderFields ? holderFields.top10 + "%" : "pending"}` +
      ` | dev=${creatorHash.launched || "0"} snipers=${earlyBuyers.length}` +
      ` | t=${richPayloadMs}ms — waiting for Geyser`,
    );
  } else {
    // Rich broadcast — real price, liquidity, mcap all non-zero
    const broadcastFields = { ...curveFields!, ...richFields };
    Object.assign(data, broadcastFields);
    redis.hset(`token:${data.mint}`, broadcastFields).catch(() => {});
    await redis.del(`payload:${data.mint}`);
    const secondPayload = await buildTokenPayload(data, true, []);
    broadcastNewToken(secondPayload);
    broadcastTokenUpdate(data.mint, secondPayload);
    console.log(
      `[BROADCAST] 💰 ${data.name}` +
      ` | price=${parseFloat(data.priceUsd || "0").toFixed(8)}` +
      ` | mcap=$${parseFloat(data.marketCapUsd || "0").toFixed(0)}` +
      ` | curve=${parseFloat(data.curvePercentage || "0").toFixed(1)}%` +
      ` | top10=${holderFields ? holderFields.top10 + "%" : "pending"}` +
      ` | dev=${creatorHash.launched || "0"} snipers=${earlyBuyers.length}` +
      ` | t=${richPayloadMs}ms`,
    );
  }

  // ── Background enrichment (IPFS/Arweave — non-blocking) ─────────────────────
  queueEnrichment(data.mint);

  // ── t=2s fallback: only runs if riskQuickScanAt not yet set ──────────────────
  setTimeout(() => queueInitialQuickRisk(data.mint), 2_000);

  // ── t=15s: full risk — owner resolution, insider detection ───────────────────
  setTimeout(() => queueRiskAnalysis(data.mint), 15_000);
});

yellowstone.on("trade", async (data: any) => {
  // Emit the raw trade event immediately — client uses this to update buy/sell counters
  // and trade list without waiting for the next curve-update broadcast.
  io.to(`token:${data.mint}`).emit("trade", {
    mint: data.mint,
    signature: data.signature,
    type: data.type,
    solAmount: data.solAmount,
    maker: data.maker,
  });

  // recordPrice + broadcastChartTick are called in the curve-update handler after Geyser
  // confirms the post-trade price — guarantees chart ticks always use the real on-chain value.

  try {
    if (data.type === "buy" && data.maker && data.slot != null) {
      const launchSlotStr = await redis.hget(`token:${data.mint}`, "slot");
      if (launchSlotStr) {
        // Use slot delta instead of wall clock — immune to indexer latency and restarts.
        // One slot ≈ 400ms, so 50 slots ≈ 20s.
        const slotDiff = Number(data.slot) - parseInt(launchSlotStr);
        if (slotDiff >= 0 && slotDiff <= 50) {
          await redis.sadd(`snipers_set:${data.mint}`, data.maker);
          await redis.expire(`snipers_set:${data.mint}`, 43_200);
        }
      }
    }
  } catch (err) {}

  // Trigger risk analysis if not yet analyzed
  const analyzed = await redis.hget(`token:${data.mint}`, "riskAnalyzedAt");
  if (!analyzed) queueRiskAnalysis(data.mint);
});

yellowstone.on("swap", async (data: any) => {
  if (!data.mint || !data.success) return;

  const now  = Date.now();
  const pipe = redis.pipeline();

  // txs:{mint}: score = slot for stable ordering + natural signature dedup.
  // Member is deterministic (no timestamp) so ZADD silently deduplicates Geyser re-deliveries.
  // Capped at 500 entries (trim when > 550 to batch the cleanup).
  const priorityFeeSOL = parseFloat((data.priorityFeeLamports / 1e9).toFixed(9));
  const txEntry = JSON.stringify({
    signature:      data.signature,
    type:           data.type,
    sol:            data.solAmount,
    tokens:         data.tokenAmount,
    maker:          data.maker,
    priorityFeeSOL,
  });
  pipe.zadd(`txs:${data.mint}`, data.slot, txEntry);
  pipe.zremrangebyrank(`txs:${data.mint}`, 0, -552);   // keep newest 551; trim fires only after 551st entry
  pipe.expire(`txs:${data.mint}`, 86_400);
  // txs_meta:{mint}: sig → {slot, ts} for signature-based cursor + timestamp enrichment
  pipe.hset(`txs_meta:${data.mint}`, data.signature, JSON.stringify({ slot: data.slot, ts: now }));
  pipe.expire(`txs_meta:${data.mint}`, 86_400);

  // trades_full:{mint}: time-scored for OHLCV volume calculations (separate from txs)
  pipe.zadd(
    `trades_full:${data.mint}`, now,
    `${data.type}:${data.solAmount.toFixed(9)}:${data.tokenAmount.toFixed(data.decimals)}:${data.signature.slice(0, 8)}`,
  );
  pipe.zremrangebyscore(`trades_full:${data.mint}`, 0, now - 86_400_000);
  pipe.expire(`trades_full:${data.mint}`, 86_400);

  // Dust buy filter: remove wallets that paid < 0.001 SOL from the sniper set
  if (data.type === "buy" && data.maker && data.solAmount < 0.001) {
    pipe.srem(`snipers_set:${data.mint}`, data.maker);
  }

  await pipe.exec().catch(() => {});

  const txPayload = {
    signature:      data.signature,
    timestamp:      now,
    slot:           data.slot,
    type:           data.type,
    sol:            data.solAmount,
    tokens:         data.tokenAmount,
    decimals:       data.decimals,
    maker:          data.maker,
    priorityFeeSOL,
    mint:           data.mint,
  };

  io.to(`transactions:${data.mint}`).emit("tx", txPayload);
  io.to(`token:${data.mint}`).emit("trade-enriched", txPayload);
});

// ========== BACKGROUND UPDATES (PUBSUB) ==========
subscriber.subscribe("token-updates");
subscriber.on("message", async (channel, mint) => {
  if (channel === "token-updates") {
    try {
      const d = await redis.hgetall(`token:${mint}`);
      if (!d || !d.mint) return;

      // Bust the payload cache so the next build is always fresh
      await redis.del(`payload:${mint}`);

      const payload = await buildTokenPayload(d);
      const status = payload.data.graduation.status;

      // Broadcast to specific token room (REQUIRED for all updates)
      broadcastTokenUpdate(mint, { ...payload, type: "update" });

      // Broadcast to main "new" stream as an update
      io.to("new").emit("update", payload);

      // Graduation-specific rooms
      if (status === "graduating") {
        broadcastGraduating(payload);
      } else if (status === "graduated") {
        broadcastGraduated(payload);
      }
    } catch (e) {
      console.error("[Broadcast] Failed sending update for", mint);
    }
  }
});

// ========== HELPERS ==========

// Scan tokens:latest and return full payloads for a given graduationStatus.
// Uses a Redis pipeline so N tokens = 1 roundtrip for the status filter.
async function getTokensByStatus(status: string): Promise<any[]> {
  const mints = await redis.zrevrange("tokens:latest", 0, 499);
  if (!mints.length) return [];

  const pipeline = redis.pipeline();
  for (const mint of mints) pipeline.hget(`token:${mint}`, "graduationStatus");
  const results = await pipeline.exec();

  const matchingMints = mints.filter((_, i) => results?.[i]?.[1] === status);
  if (!matchingMints.length) return [];

  // Pipeline all hgetall calls into a single round-trip
  const hashPipeline = redis.pipeline();
  for (const mint of matchingMints) hashPipeline.hgetall(`token:${mint}`);
  const hashResults = await hashPipeline.exec();
  const tokenDatas = hashResults?.map(r => r?.[1] as Record<string, string> | null) ?? [];
  const payloads = await Promise.all(
    tokenDatas.filter(d => d?.mint).map(d => buildTokenPayload(d!)),
  );
  return payloads;
}

// ========== RICH PAYLOAD LATENCY TELEMETRY ==========
// Logs p50/p95/p99 of the time from launch detection to rich broadcast.
// Buffer is in-memory and flushed every 5 minutes — zero per-token overhead.
setInterval(() => {
  const n = richDelayBuffer.length;
  if (n === 0) return;
  const sorted = [...richDelayBuffer].sort((a, b) => a - b);
  const p = (pct: number) => sorted[Math.min(Math.floor(n * pct), n - 1)];
  const top10Pct      = ((top10InlineCount / n) * 100).toFixed(1);
  const blockedPct    = ((zeroPriceBlockedCount / n) * 100).toFixed(1);
  console.log(
    `[Telemetry] richPayloadDelay | n=${n} p50=${p(0.50)}ms p95=${p(0.95)}ms p99=${p(0.99)}ms` +
    ` | top10Inline=${top10Pct}% zeroPriceBlocked=${zeroPriceBlockedCount} (${blockedPct}%)`,
  );
  richDelayBuffer.length = 0;
  top10InlineCount = 0;
  zeroPriceBlockedCount = 0;
}, 5 * 60_000);

// ========== TRENDING JOB ==========
// Runs every 30s — 1m window needs more frequent refreshes than 60s
setInterval(async () => {
  try {
    const trendingData = await calculateTrendingTokens();
    broadcastTrending(trendingData);
  } catch (e: any) {
    console.error("[Trending] Job failed:", e.message);
  }
}, 30_000);

const TRENDING_WINDOWS: Record<string, number> = {
  "1m":  60_000,
  "5m":  300_000,
  "30m": 1_800_000,
  "1h":  3_600_000,
};

// Returns { "1m": [...top50], "5m": [...top50], "30m": [...top50], "1h": [...top50] }
// One pipeline call to fetch all trade data, then volume is computed in-memory per window.
async function calculateTrendingTokens(): Promise<Record<string, any[]>> {
  const empty = { "1m": [], "5m": [], "30m": [], "1h": [] };
  const mints = await redis.zrevrange("tokens:latest", 0, 499);
  if (!mints.length) return empty;

  const now = Date.now();
  const cutoff1h = now - 3_600_000; // fetch trades up to 1h — covers all 4 windows

  // Single pipeline roundtrip: get all trades within 1h for every mint
  const pipeline = redis.pipeline();
  for (const mint of mints) {
    pipeline.zrangebyscore(`trades:${mint}`, cutoff1h, "+inf");
  }
  const pipelineResults = await pipeline.exec();

  // Compute per-window volume for each mint from the in-memory trade entries
  // Trade member format: "buy|sell:{solAmount}:{timestamp}"
  const mintVolumes: Record<string, Record<string, number>> = {};

  for (let i = 0; i < mints.length; i++) {
    const mint = mints[i];
    const entries: string[] = (pipelineResults?.[i]?.[1] as string[]) || [];
    const vol: Record<string, number> = { "1m": 0, "5m": 0, "30m": 0, "1h": 0 };

    for (const entry of entries) {
      const parts = entry.split(":");
      const sol = parseFloat(parts[1] || "0");
      const ts  = parseInt(parts[2]  || "0");
      if (!sol || !ts) continue;

      const age = now - ts;
      if (age <= 60_000)    vol["1m"]  += sol;
      if (age <= 300_000)   vol["5m"]  += sol;
      if (age <= 1_800_000) vol["30m"] += sol;
      vol["1h"] += sol;
    }

    mintVolumes[mint] = vol;
  }

  // Rank mints per window and take top 50
  const windowRankings: Record<string, string[]> = {};
  const neededMints = new Set<string>();

  for (const window of Object.keys(TRENDING_WINDOWS)) {
    const ranked = mints
      .filter(m => (mintVolumes[m]?.[window] || 0) > 0)
      .sort((a, b) => (mintVolumes[b][window] || 0) - (mintVolumes[a][window] || 0))
      .slice(0, 50);
    windowRankings[window] = ranked;
    for (const m of ranked) neededMints.add(m);
  }

  // Fetch token data for all unique mints that appear in any window — deduplicated.
  // Pipeline all hgetall calls into a single round-trip (up to 200 mints).
  const uniqueMints = [...neededMints];
  const trendPipeline = redis.pipeline();
  for (const mint of uniqueMints) trendPipeline.hgetall(`token:${mint}`);
  const trendResults = await trendPipeline.exec();
  const uniqueTokenDatas = trendResults?.map(r => r?.[1] as Record<string, string> | null) ?? [];

  const payloadMap = new Map<string, any>();
  await Promise.all(uniqueMints.map(async (m, i) => {
    const d = uniqueTokenDatas[i];
    if (d?.mint) payloadMap.set(m, await buildTokenPayload(d));
  }));

  // Assemble final result
  const result: Record<string, any[]> = {};
  for (const window of Object.keys(TRENDING_WINDOWS)) {
    result[window] = windowRankings[window]
      .map(m => payloadMap.get(m))
      .filter(Boolean);
  }
  return result;
}

// ========== API ENDPOINTS ==========

// GET /new — JSON list of latest tokens with full payload structure
app.get("/new", async (_req, res) => {
  try {
    const mints = await redis.zrevrange("tokens:latest", 0, 49);
    
    // Fetch all token data in parallel
    const hashes = await Promise.all(mints.map(m => redis.hgetall(`token:${m}`)));
    const validHashes = hashes.filter(d => d && d.mint);
    
    // Build all payloads in parallel (REST always bypasses cache for freshness)
    const tokens = await Promise.all(validHashes.map(d => buildTokenPayload(d, true)));

    res.json(tokens);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// GET /doc — full integration reference in JSON
app.get("/doc", (_req, res) => {
  res.json({
    overview: {
      description: "Real-time multi-launchpad Solana token indexer. Streams live token launches, trades, price updates, risk analysis, and trending data via Socket.io WebSocket.",
      websocket_url: "ws://localhost:3000",
      rest_base_url: "http://localhost:3000",
      client_library: "socket.io-client (v4)",
      supported_platforms: ["pump", "moon", "bags", "meteora", "letsbonk", "launchlab"],
    },

    connection: {
      install: "npm install socket.io-client",
      example: `
import { io } from "socket.io-client";

const socket = io("ws://localhost:3000", {
  transports: ["websocket"],
  reconnection: true,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 10000,
  reconnectionAttempts: Infinity,
});

socket.on("connect", () => {
  // re-join all rooms here so they restore after reconnect
});

socket.on("disconnect", (reason) => {
  console.warn("disconnected:", reason);
});`.trim(),
    },

    payload_shape: {
      note: "Every token payload — regardless of room or event — has this shape. Access via .data on message/update events, or .data on each array item in snapshots.",
      fields: {
        "data.token": {
          name: "string — token name",
          symbol: "string — ticker",
          mint: "string — base58 mint address",
          uri: "string — off-chain metadata URI",
          decimals: "number — always 6",
          description: "string — from metadata (empty until enriched)",
          image: "string — resolved image URL (empty until IPFS resolves)",
          hasFileMetaData: "boolean — true if URI exists",
          isMayhemMode: "boolean — true if Pump.fun Mayhem mode is active",
          isCashbackEnabled: "boolean — true if trading cashback is enabled",
          createdOn: "string — launchpad URL",
          strictSocials: { twitter: "string", telegram: "string", website: "string" },
          creation: { creator: "string — wallet", created_tx: "string — signature", created_time: "number — unix seconds" },
        },
        "data.pools[0]": {
          poolId: "string — bonding curve PDA",
          liquidity: { quote: "number — SOL", usd: "number — USD" },
          price: { quote: "number — price in SOL", usd: "number — price in USD" },
          marketCap: { quote: "number — SOL", usd: "number — USD" },
          curvePercentage: "number — 0-100, how full the bonding curve is",
          txns: { buys: "number", sells: "number", total: "number", volume: "number — all-time SOL", volume24h: "number — 24h SOL" },
          market: "string — platform ID (pumpfun / moon / bags / meteora / letsbonk / launchlab)",
          deployer: "string — creator wallet",
          createdAt: "number — ms timestamp",
          lastUpdated: "number — ms timestamp",
        },
        "data.events": {
          "1m":  { priceChangePercentage: "number" },
          "5m":  { priceChangePercentage: "number" },
          "15m": { priceChangePercentage: "number" },
          "30m": { priceChangePercentage: "number" },
          "1h":  { priceChangePercentage: "number" },
          "4h":  { priceChangePercentage: "number" },
          "24h": { priceChangePercentage: "number" },
        },
        "data.risk": {
          top10: "number — % of supply held by top 10 wallets",
          snipers: { count: "number", totalPercentage: "number", totalBalance: "number", wallets: "array — { wallet, percentage, amount, isTimeBasedSniper }" },
          insiders: { count: "number", totalPercentage: "number", totalBalance: "number", wallets: "array — { wallet, percentage, amount }" },
          dev: { percentage: "number", amount: "number", stats: { total_launched: "number", total_migrated: "number" } },
        },
        "data.graduation": {
          status: "string — 'new' | 'graduating' | 'graduated'",
        },
        "data.dev_stats": {
          total_launched: "number — total tokens this wallet has launched",
          total_migrated: "number — total tokens this wallet has graduated",
        },
        "data.priceHistory": "array — [{ time: ms, price: SOL, price_usd: USD }] — last 50 ticks (up to 500 via chart room or REST)",
        "data.meta": {
          hasPrice: "boolean — price/liquidity populated (arrives ~100-500ms after launch)",
          hasBasicRisk: "boolean — dev stats + sniper count populated",
          hasTop10: "boolean — top10 concentration + dev% populated",
          hasRisk: "boolean — full risk scan complete",
          hasImage: "boolean — IPFS/Arweave image resolved",
          expectedRichBy: "number — ms timestamp by which hasPrice should be true",
          richPayloadDelay: "number | null — actual ms from launch to rich broadcast",
        },
      },
    },

    rooms: {
      new: {
        description: "Every new token launch across all platforms. Auto-joined on connect — no need to emit join.",
        auto_joined: true,
        events: {
          snapshot: {
            trigger: "Fires once immediately on connect",
            data_shape: "array of token envelopes",
            access_pattern: "envelope.data[i].data.token / envelope.data[i].data.pools[0]",
            example: `
socket.on("snapshot", (envelope) => {
  if (envelope.room === "new") {
    envelope.data.forEach((item) => {
      const token = item.data.token;
      const pool  = item.data.pools[0];
      const meta  = item.data.meta;
      console.log(token.name, pool.price.usd, pool.marketCap.usd);
    });
  }
});`.trim(),
          },
          message: {
            trigger: "Fires for every new token detected after connect. Two messages per token: skeleton (~5ms, meta.hasPrice=false) then rich (~100-500ms, meta.hasPrice=true)",
            data_shape: "single token envelope",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
            example: `
socket.on("message", (envelope) => {
  if (envelope.room === "new") {
    const token = envelope.data.token;
    const pool  = envelope.data.pools[0];
    const meta  = envelope.data.meta;

    if (!meta.hasPrice) {
      // First broadcast — skeleton only, price not yet available
      renderSkeletonCard(token);
    } else {
      // Rich broadcast — price, top10, dev stats all included
      renderFullCard(token, pool, envelope.data.risk);
    }
  }
});`.trim(),
          },
          update: {
            trigger: "Fires when an existing token updates (price change, risk analysis, metadata resolved)",
            data_shape: "single token envelope",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
            example: `
socket.on("update", (envelope) => {
  if (envelope.room === "new") {
    const mint = envelope.data.token.mint;
    upsertTokenCard(mint, envelope.data);
  }
});`.trim(),
          },
        },
        full_example: `
import { io } from "socket.io-client";

const socket = io("ws://localhost:3000", { transports: ["websocket"] });
const tokens = new Map();

socket.on("snapshot", (envelope) => {
  if (envelope.room === "new") {
    envelope.data.forEach((item) => {
      tokens.set(item.data.token.mint, item.data);
    });
    renderList([...tokens.values()]);
  }
});

socket.on("message", (envelope) => {
  if (envelope.room === "new") {
    tokens.set(envelope.data.token.mint, envelope.data);
    renderList([...tokens.values()]);
  }
});

socket.on("update", (envelope) => {
  if (envelope.room === "new") {
    tokens.set(envelope.data.token.mint, envelope.data);
    renderList([...tokens.values()]);
  }
});`.trim(),
      },

      graduating: {
        description: "Tokens whose bonding curve is >= 80% filled. Snapshot on join, message when a token enters.",
        auto_joined: false,
        join: `socket.emit("join", "graduating")`,
        events: {
          snapshot: {
            trigger: "Fires once on join with all currently graduating tokens",
            data_shape: "array of token envelopes",
            access_pattern: "envelope.data[i].data.token / envelope.data[i].data.pools[0]",
          },
          message: {
            trigger: "Fires when a token's curvePercentage crosses 80%",
            data_shape: "single token envelope",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
          },
          update: {
            trigger: "Fires when a graduating token's price or curve% updates",
            data_shape: "single token envelope",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
          },
        },
        example: `
socket.emit("join", "graduating");

socket.on("snapshot", (envelope) => {
  if (envelope.room === "graduating") {
    envelope.data.forEach((item) => {
      const token = item.data.token;
      const pool  = item.data.pools[0];
      // pool.curvePercentage >= 80 here
      addToGraduatingList(token, pool);
    });
  }
});

socket.on("message", (envelope) => {
  if (envelope.room === "graduating") {
    const token = envelope.data.token;
    const pool  = envelope.data.pools[0];
    addToGraduatingList(token, pool);
  }
});`.trim(),
      },

      graduated: {
        description: "Tokens whose bonding curve has completed and migrated to a DEX. Snapshot on join, message when graduation is detected.",
        auto_joined: false,
        join: `socket.emit("join", "graduated")`,
        events: {
          snapshot: {
            trigger: "Fires once on join with all graduated tokens the indexer has seen",
            data_shape: "array of token envelopes",
            access_pattern: "envelope.data[i].data.token / envelope.data[i].data.pools[0]",
          },
          message: {
            trigger: "Fires the moment a token's on-chain complete flag is detected",
            data_shape: "single token envelope",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
          },
        },
        example: `
socket.emit("join", "graduated");

socket.on("snapshot", (envelope) => {
  if (envelope.room === "graduated") {
    envelope.data.forEach((item) => {
      const token = item.data.token;
      const pool  = item.data.pools[0];
      // pool.curvePercentage ~100, graduation.status === "graduated"
      addToGraduatedFeed(token, pool);
    });
  }
});

socket.on("message", (envelope) => {
  if (envelope.room === "graduated") {
    const token = envelope.data.token;
    const pool  = envelope.data.pools[0];
    prependToGraduatedFeed(token, pool);
  }
});`.trim(),
      },

      trending: {
        description: "Top 50 tokens by volume for four time windows: 1m, 5m, 30m, 1h. Full list refreshed every 30 seconds. Switching windows is a local state change — no re-subscribe needed.",
        auto_joined: false,
        join: `socket.emit("join", "trending")`,
        windows: ["1m", "5m", "30m", "1h"],
        events: {
          snapshot: {
            trigger: "Fires once on join with current rankings for all four windows",
            data_shape: "object with keys 1m/5m/30m/1h, each an array of token envelopes",
            access_pattern: "envelope.data['1m'][i].data.token / envelope.data['1m'][i].data.pools[0]",
          },
          message: {
            trigger: "Fires every ~30 seconds with a full refresh of all windows — replace, do not merge",
            data_shape: "same as snapshot",
            access_pattern: "same as snapshot",
          },
        },
        example: `
socket.emit("join", "trending");

let trending = { "1m": [], "5m": [], "30m": [], "1h": [] };
let activeWindow = "1m";

function parseTrending(data) {
  const result = {};
  for (const window of ["1m", "5m", "30m", "1h"]) {
    result[window] = (data[window] || []).map((item) => ({
      token:  item.data.token,
      pool:   item.data.pools[0],
      events: item.data.events,
      risk:   item.data.risk,
      meta:   item.data.meta,
    }));
  }
  return result;
}

socket.on("snapshot", (envelope) => {
  if (envelope.room === "trending") {
    trending = parseTrending(envelope.data);
    renderTrendingTable(trending[activeWindow]);
  }
});

socket.on("message", (envelope) => {
  if (envelope.room === "trending") {
    trending = parseTrending(envelope.data);
    renderTrendingTable(trending[activeWindow]);
  }
});

// Switch window without re-subscribing
function setWindow(window) {
  activeWindow = window;
  renderTrendingTable(trending[window]);
}`.trim(),
      },

      "token:{mint}": {
        description: "All state updates for a single token. Use on a token detail page. Join when the page opens, leave when it closes.",
        auto_joined: false,
        join: `socket.emit("join", "token:{mint}")`,
        leave: `socket.emit("leave", "token:{mint}")`,
        events: {
          snapshot: {
            trigger: "Fires once on join with the full current state of the token",
            data_shape: "single token payload (not wrapped in an extra envelope)",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
          },
          message: {
            trigger: "Fires on any state change: price update, risk analysis, metadata resolved, graduation",
            data_shape: "single token payload",
            access_pattern: "envelope.data.token / envelope.data.pools[0]",
          },
          trade: {
            trigger: "Fires on every buy or sell — lightweight, no full payload",
            data_shape: "{ mint, signature, type: 'buy'|'sell', solAmount, maker }",
            access_pattern: "event.type / event.solAmount / event.maker",
          },
        },
        example: `
let currentMint = null;

function openTokenPage(mint) {
  if (currentMint) {
    socket.emit("leave", \`token:\${currentMint}\`);
    socket.emit("leave", \`chart:\${currentMint}\`);
  }
  currentMint = mint;
  socket.emit("join", \`token:\${mint}\`);
  socket.emit("join", \`chart:\${mint}\`);
}

socket.on("snapshot", (envelope) => {
  if (envelope.room === \`token:\${currentMint}\`) {
    const { token, pools, events, risk, graduation, dev_stats, meta } = envelope.data;
    renderHeader(token);
    renderPoolStats(pools[0]);
    renderPriceChanges(events);
    renderRiskPanel(risk, meta);
    renderGraduationBar(pools[0].curvePercentage, graduation.status);
    renderDevStats(dev_stats);
  }
});

socket.on("message", (envelope) => {
  if (envelope.room === \`token:\${currentMint}\`) {
    const { pools, events, risk, graduation, meta } = envelope.data;
    renderPoolStats(pools[0]);
    renderPriceChanges(events);
    renderRiskPanel(risk, meta);
    renderGraduationBar(pools[0].curvePercentage, graduation.status);
  }
});

socket.on("trade", (event) => {
  if (event.mint === currentMint) {
    appendTradeRow({
      side:   event.type,       // "buy" | "sell"
      sol:    event.solAmount,
      wallet: event.maker,
      sig:    event.signature,
    });
  }
});`.trim(),
      },

      "chart:{mint}": {
        description: "Price ticks only for a single token. Snapshot delivers up to 500 historical ticks. Each trade appends one new tick. Always used alongside token:{mint}.",
        auto_joined: false,
        join: `socket.emit("join", "chart:{mint}")`,
        leave: `socket.emit("leave", "chart:{mint}")`,
        events: {
          snapshot: {
            trigger: "Fires once on join with price history",
            data_shape: "{ mint, priceHistory: [{ time, price, price_usd }] }",
            access_pattern: "envelope.data.priceHistory",
          },
          chart: {
            trigger: "Fires after every trade with a single new tick",
            data_shape: "{ mint, tick: { time, price, price_usd } }",
            access_pattern: "envelope.data.tick",
          },
        },
        example: `
socket.on("snapshot", (envelope) => {
  if (envelope.room === \`chart:\${currentMint}\`) {
    const history = envelope.data.priceHistory;
    // history[i] = { time: ms, price: SOL, price_usd: USD }
    initChart(history);
  }
});

socket.on("chart", (envelope) => {
  if (envelope.room === \`chart:\${currentMint}\`) {
    const tick = envelope.data.tick;
    // tick = { time: ms, price: SOL, price_usd: USD }
    appendChartTick(tick);
  }
});`.trim(),
      },
    },

    access_pattern_summary: {
      note: "The nesting differs between snapshots (arrays) and single-item events. This is the complete reference:",
      table: [
        { room: "new",            event: "snapshot", access: "envelope.data[i].data.token  /  envelope.data[i].data.pools[0]" },
        { room: "new",            event: "message",  access: "envelope.data.token  /  envelope.data.pools[0]" },
        { room: "new",            event: "update",   access: "envelope.data.token  /  envelope.data.pools[0]" },
        { room: "graduating",     event: "snapshot", access: "envelope.data[i].data.token  /  envelope.data[i].data.pools[0]" },
        { room: "graduating",     event: "message",  access: "envelope.data.token  /  envelope.data.pools[0]" },
        { room: "graduated",      event: "snapshot", access: "envelope.data[i].data.token  /  envelope.data[i].data.pools[0]" },
        { room: "graduated",      event: "message",  access: "envelope.data.token  /  envelope.data.pools[0]" },
        { room: "trending",       event: "snapshot", access: "envelope.data['1m'][i].data.token  /  envelope.data['1m'][i].data.pools[0]" },
        { room: "trending",       event: "message",  access: "envelope.data['1m'][i].data.token  /  envelope.data['1m'][i].data.pools[0]" },
        { room: "token:{mint}",   event: "snapshot", access: "envelope.data.token  /  envelope.data.pools[0]" },
        { room: "token:{mint}",   event: "message",  access: "envelope.data.token  /  envelope.data.pools[0]" },
        { room: "token:{mint}",   event: "trade",    access: "event.type  /  event.solAmount  /  event.maker  /  event.signature" },
        { room: "chart:{mint}",   event: "snapshot", access: "envelope.data.priceHistory" },
        { room: "chart:{mint}",   event: "chart",    access: "envelope.data.tick" },
      ],
    },

    rest_endpoints: {
      "GET /new": "Last 50 tokens. Same payload shape as the new room snapshot — each item is { type, room, data } so access via item.data.token",
      "GET /api/token/:mint": "Full payload for a single token. Access directly as response.data.token",
      "GET /api/token/:mint/history": "Extended price history. Response: { mint, count, history: [{ time, price, price_usd }] }",
      "GET /api/stats": "Service health. Response: { status, tokensInRedis, uptime, timestamp }",
      "GET /doc": "This document",
    },

    meta_flags: {
      description: "Use meta flags to drive progressive loading states instead of hard-coding timers or checking if fields are zero.",
      timeline: {
        "~5ms":      "Skeleton broadcast — name/symbol/creator/mayhem/cashback. meta.hasPrice=false",
        "~100-500ms": "Rich broadcast — price, top10, devPercentage, devStats, snipersCount. meta.hasPrice=true, meta.hasTop10=true",
        "~15s":      "Full risk — snipers/insiders arrays with resolved wallet owners. meta.hasRisk=true",
        "~1-10s":    "Image resolved — IPFS/Arweave metadata. meta.hasImage=true",
      },
      example: `
function renderRiskPanel(risk, meta) {
  if (!meta.hasTop10) {
    showSkeleton();        // holder scan still pending
    return;
  }
  if (!meta.hasRisk) {
    showPartialRisk({      // top10 + dev% available, full snipers/insiders not yet
      top10:         risk.top10,
      devPercentage: risk.dev.percentage,
    });
    return;
  }
  showFullRisk(risk);      // everything available
}

function renderImage(token, meta) {
  if (!meta.hasImage) {
    showImagePlaceholder();
  } else {
    showImage(token.image);
  }
}

// Show "data delayed" if rich payload never arrived
function checkDelay(meta) {
  if (!meta.hasPrice && Date.now() > meta.expectedRichBy + 2000) {
    showDelayedBanner();
  }
}`.trim(),
    },

    reconnect_pattern: {
      description: "Socket.io reconnects automatically but does NOT restore room memberships. Re-join all rooms inside the connect handler.",
      example: `
const activeRooms = new Set();

socket.on("connect", () => {
  for (const room of activeRooms) {
    socket.emit("join", room);
  }
});

function joinRoom(room) {
  activeRooms.add(room);
  socket.emit("join", room);
}

function leaveRoom(room) {
  activeRooms.delete(room);
  socket.emit("leave", room);
}`.trim(),
    },
  });
});



// ── Global error safety net ───────────────────────────────────────────────────
// Node.js v15+ crashes on unhandled promise rejections by default.
// Geyser h2 transport resets (INTERNAL_ERROR from Chainstack) can leak past the
// stream 'error' event handler as raw thrown exceptions. Catch them here so the
// process survives and the built-in reconnect logic handles recovery.
process.on("unhandledRejection", (reason: any) => {
  console.error("[Process] Unhandled rejection:", reason?.message || reason);
  // If it looks like a Geyser transport error, trigger a reconnect
  const msg = String(reason?.message || reason || "");
  if (msg.includes("h2 protocol") || msg.includes("transport error") || msg.includes("gRPC") || msg.includes("exhausted")) {
    console.error("[Process] Geyser transport error — triggering reconnect via manager");
    yellowstone.reconnect(); // reconnect() tears down old streams first, unlike start()
  }
});

process.on("uncaughtException", (err: Error) => {
  console.error("[Process] Uncaught exception:", err.message);
  // Don't exit — let the reconnect logic recover
});

console.log("✅ sol-indexer started — Pump.fun live via Yellowstone");
