// src/index.ts — Full Payload Builder + /new endpoint + Live Broadcasting
import express from "express";
import { Server } from "socket.io";
import { YellowstoneManager } from "./yellowstone-manager";
import { queueEnrichment, startEnrichmentSweep } from "./enricher";
import { processCurveAccountUpdate, fetchAndDecodeCurve } from "./curve-tracker";
import { startPriceTracker, getPriceHistory, recordPrice } from "./price-tracker";
import { queueRiskAnalysis, queueInitialQuickRisk, startHolderSnapshot } from "./risk-analyzer";
import Redis from "ioredis";
import "dotenv/config";
import cors from "cors";

const app = express();
app.use(cors());
app.use(express.json());

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
  const fields = await processCurveAccountUpdate(event.mint, event.data, event.platform);

  // Resolve pending first-curve race if the new-launch handler is still waiting
  const resolver = pendingFirstCurve.get(event.mint);
  if (resolver && fields) {
    pendingFirstCurve.delete(event.mint);
    resolver(fields);
  }

  if (fields?.complete === "true" && event.curvePDA) {
    yellowstone.removeCurvePDA(event.curvePDA);
  }
});

yellowstone.on("new-launch", async (data: any) => {
  const platform = data.platform || "pump";

  // ── t=0 SYNC: Register Geyser listener before any await ─────────────────────
  // Must be synchronous so we never miss a Geyser snapshot that arrives
  // while we're awaiting the skeleton build or curve race below.
  const geyserFirstCurve: Promise<Record<string, string> | null> = data.curvePDA
    ? new Promise(resolve => {
        pendingFirstCurve.set(data.mint, resolve);
        setTimeout(() => { if (pendingFirstCurve.delete(data.mint)) resolve(null); }, 800);
      })
    : Promise.resolve(null);

  // ── t=0 ASYNC: Fire all parallel work immediately ────────────────────────────
  // These three tasks start before the skeleton broadcast. By the time the curve
  // race resolves (~100-400ms), the Redis reads are always done (~1ms each) and
  // the holder fetch is often done too (~150-400ms RPC).
  if (data.curvePDA) yellowstone.addCurvePDA(data.curvePDA, data.mint, platform);

  const rpcProbe = data.curvePDA
    ? fetchAndDecodeCurve(data.mint, data.curvePDA, platform, 100)
    : Promise.resolve(null);

  // Holder snapshot: getTokenLargestAccounts (500ms internal timeout)
  // Used for top10 concentration + ATA-derived dev% in the second broadcast.
  let holderFields: { top10: string; devPercentage: string } | null = null;
  const holderPromise = data.curvePDA
    ? startHolderSnapshot(data.mint, data.creator || "", data.curvePDA, platform)
        .then(f => { holderFields = f; return f; })
    : Promise.resolve(null);

  // Redis reads start immediately — both done in ~1ms, well before curve race resolves
  const sniperPromise  = redis.smembers(`snipers_set:${data.mint}`);
  const creatorPromise = data.creator
    ? redis.hgetall(`creator:${data.creator}`)
    : Promise.resolve({} as Record<string, string>);

  // ── Skeleton broadcast (t≈5ms — after one Redis get for payload cache) ───────
  const skeletonPayload = await buildTokenPayload(data, false, []);
  broadcastNewToken(skeletonPayload);
  broadcastTokenUpdate(data.mint, skeletonPayload);
  console.log(`[BROADCAST] 🚀 ${data.name} ($${data.symbol}) | ${data.mint.slice(0, 12)}...`);

  // ── Curve race: RPC probe (100ms) vs Geyser first snapshot (800ms safety) ────
  if (data.curvePDA) {
    const curveFields = await raceCurveData(rpcProbe, geyserFirstCurve);
    pendingFirstCurve.delete(data.mint);

    if (curveFields) {
      Object.assign(data, curveFields);

      // Redis reads are long done by now (~1ms each, started at t=0)
      const [earlyBuyers, creatorHash] = await Promise.all([sniperPromise, creatorPromise]);

      // Give holder snapshot up to 450ms extra after curve resolves.
      // holderPromise started at t=0, so total budget = curve_race_time + 450ms.
      // Internal timeout (480ms) is the binding constraint in all cases.
      if (!holderFields) {
        holderFields = await Promise.race([
          holderPromise,
          new Promise<null>(r => setTimeout(() => r(null), 450)),
        ]);
      }

      // Build one combined rich update: price + curve + dev context + top10 (if ready)
      const richPayloadMs = Date.now() - parseInt(data.createdAt || "0");
      richDelayBuffer.push(richPayloadMs);
      if (holderFields) top10InlineCount++;
      const richFields: Record<string, string> = {
        snipersCount:  earlyBuyers.length.toString(),
        devStats:      JSON.stringify({
          total_launched: parseInt(creatorHash.launched || "0"),
          total_migrated: parseInt(creatorHash.migrated || "0"),
        }),
        richPayloadMs: richPayloadMs.toString(), // telemetry: ms from launch to rich broadcast
        ...(holderFields
          ? {
              top10:           holderFields.top10,
              devPercentage:   holderFields.devPercentage,
              riskQuickScanAt: Date.now().toString(), // marks quick scan done → t=2s skips
            }
          : {}),
      };
      Object.assign(data, richFields);

      redis.hset(`token:${data.mint}`, { ...curveFields, ...richFields }).catch(() => {});
      const richPayload = await buildTokenPayload(data, true, []);
      broadcastNewToken(richPayload);
      broadcastTokenUpdate(data.mint, richPayload);
      console.log(
        `[BROADCAST] 💰 ${data.name} | price=${parseFloat(data.priceUsd || "0").toFixed(8)}` +
        ` | top10=${holderFields ? holderFields.top10 + "%" : "pending"}` +
        ` | dev_launches=${creatorHash.launched || "0"} snipers=${earlyBuyers.length}`,
      );
    }
    // No curve data: Geyser will deliver eventually via the pubsub → broadcast path
  }

  // ── Background enrichment (IPFS/Arweave — non-blocking) ─────────────────────
  queueEnrichment(data.mint);

  // ── t=2s fallback: only runs if riskQuickScanAt not yet set ──────────────────
  // Covers: holderPromise timed out, or curveFields never arrived above.
  setTimeout(() => queueInitialQuickRisk(data.mint), 2_000);

  // ── t=15s: full risk — owner resolution, insider detection ───────────────────
  setTimeout(() => queueRiskAnalysis(data.mint), 15_000);
});

yellowstone.on("trade", async (data: any) => {
  // We don't build full payload here to save RPC/Redis usage
  // Just emit the trade event to the specific token room
  io.to(`token:${data.mint}`).emit("trade", {
    mint: data.mint, signature: data.signature,
    type: data.type, solAmount: data.solAmount
  });
  
  // Curve data is pushed by Geyser when the curve account changes — no RPC call needed.
  // Just record the price tick (reads cached priceQuote from Redis, no RPC).
  recordPrice(data.mint);
  broadcastChartTick(data.mint);

  try {
    const createdAt = await redis.hget(`token:${data.mint}`, "createdAt");
    if (createdAt && data.type === "buy" && data.maker) {
      const ageMs = Date.now() - parseInt(createdAt);
      // Sniper logic: any buy within first 20 seconds is a sniper
      // (Using 20s as Moon.it and Pump.fun have varying immediate launch activity)
      if (ageMs <= 20_000) {
        await redis.sadd(`snipers_set:${data.mint}`, data.maker);
        // Expiry of 12h for Redis memory hygiene
        await redis.expire(`snipers_set:${data.mint}`, 43_200); 
      }
    }
  } catch (err) {}

  // Trigger risk analysis if not yet analyzed
  const analyzed = await redis.hget(`token:${data.mint}`, "riskAnalyzedAt");
  if (!analyzed) queueRiskAnalysis(data.mint);
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
  const mints = await redis.zrevrange("tokens:latest", 0, 199);
  if (!mints.length) return [];

  const pipeline = redis.pipeline();
  for (const mint of mints) pipeline.hget(`token:${mint}`, "graduationStatus");
  const results = await pipeline.exec();

  const matchingMints = mints.filter((_, i) => results?.[i]?.[1] === status);
  if (!matchingMints.length) return [];

  const tokenDatas = await Promise.all(matchingMints.map(m => redis.hgetall(`token:${m}`)));
  const payloads = await Promise.all(
    tokenDatas.filter(d => d?.mint).map(d => buildTokenPayload(d)),
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
  const top10Pct = ((top10InlineCount / n) * 100).toFixed(1);
  console.log(`[Telemetry] richPayloadDelay | n=${n} p50=${p(0.50)}ms p95=${p(0.95)}ms p99=${p(0.99)}ms | top10Inline=${top10Pct}%`);
  richDelayBuffer.length = 0;
  top10InlineCount = 0;
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
  const mints = await redis.zrevrange("tokens:latest", 0, 199);
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

  // Fetch token data for all unique mints that appear in any window — deduplicated
  const uniqueMints = [...neededMints];
  const tokenDatas = await Promise.all(uniqueMints.map(m => redis.hgetall(`token:${m}`)));

  const payloadMap = new Map<string, any>();
  await Promise.all(uniqueMints.map(async (m, i) => {
    const d = tokenDatas[i];
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

// GET /api/token/:mint — single token full payload with price history
app.get("/api/token/:mint", async (req, res) => {
  try {
    const d = await redis.hgetall(`token:${req.params.mint}`);
    if (!d || Object.keys(d).length === 0) {
      return res.status(404).json({ error: "Token not found" });
    }
    const payload = await buildTokenPayload(d, true);
    res.json(payload);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/token/:mint/history — price history for charts
app.get("/api/token/:mint/history", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 200;
    const history = await getPriceHistory(req.params.mint, limit);
    res.json({ mint: req.params.mint, count: history.length, history });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/stats — live stream statistics
app.get("/api/stats", async (_req, res) => {
  const tokenCount = await redis.zcard("tokens:latest");
  res.json({
    status: "running",
    tokensInRedis: tokenCount,
    uptime: process.uptime().toFixed(0) + "s",
    timestamp: new Date().toISOString(),
  });
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
        "~5ms":      "Skeleton broadcast — name/symbol/creator. meta.hasPrice=false",
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



// ========== PAYLOAD BUILDER ==========

// Short-lived payload cache (2s TTL) — avoids rebuilding on rapid successive pubsub messages.
// The pubsub handler busts this before calling buildTokenPayload, so workers always get fresh data.
// REST endpoints always bypass the cache by passing forceRebuild=true.
async function buildTokenPayload(
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
  const platform = d.platform || "pump";

  // Use provided history (e.g. [] for new tokens) or fetch from Redis
  const priceHistory = priceHistoryOverride !== undefined
    ? priceHistoryOverride
    : (mint ? await getPriceHistory(mint, 50) : []);

  const payload = {
    type: "message",
    room: "new",
    data: {
      token: {
        name: d.name || "Unknown Token",
        symbol: d.symbol || "???",
        mint,
        uri: d.uri || "",
        decimals: parseInt(d.decimals || "6"),
        description: d.description || "",
        image: d.image || "",
        hasFileMetaData: !!(d.uri),
        isMayhemMode: d.isMayhemMode === "true",
        isCashbackEnabled: d.isCashbackEnabled === "true",
        createdOn: platform === "moon" ? "https://moon.it" : platform === "bags" ? "https://bags.fm" : platform === "letsbonk" ? "https://letsbonk.fun" : platform === "launchlab" ? "https://raydium.io/launchlab" : "https://pump.fun",
        strictSocials: {
          twitter: d.twitter || "",
          telegram: d.telegram || "",
          website: d.website || ""
        },
        creation: {
          creator: d.creator || "",
          created_tx: d.createdTx || "",
          created_time: Math.floor(createdAt / 1000)
        }
      },
      pools: [{
        poolId: d.curvePDA || "",
        liquidity: {
          quote: parseFloat(d.liquidity || "0"),
          usd: parseFloat(d.liquidityUsd || "0")
        },
        price: {
          quote: parseFloat(d.priceQuote || "0"),
          usd: parseFloat(d.priceUsd || "0")
        },
        tokenSupply: 1000000000000000,
        lpBurn: 100,
        tokenAddress: mint,
        marketCap: {
          quote: parseFloat(d.marketCapQuote || "0"),
          usd: parseFloat(d.marketCapUsd || "0")
        },
        decimals: parseInt(d.decimals || "6"),
        security: {
          freezeAuthority: null,
          mintAuthority: null
        },
        quoteToken: "So11111111111111111111111111111111111111112",
        market: platform === "pump" ? "pumpfun" : platform,
        deployer: d.creator || "",
        lastUpdated: Date.now(),
        createdAt: createdAt,
        txns: {
          buys: parseInt(d.buys || "0"),
          sells: parseInt(d.sells || "0"),
          total: parseInt(d.totalTxns || "0"),
          volume: parseFloat(d.volumeUsd || "0"),
          volume24h: parseFloat(d.volume24hUsd || "0")
        },
        curvePercentage: parseFloat(d.curvePercentage || "0"),
        creation: {
          creator: d.creator || "",
          created_tx: d.createdTx || "",
          created_time: createdAt
        }
      }],
      events: {
        "1m": { priceChangePercentage: parseFloat(d["1m"] || "0") },
        "5m": { priceChangePercentage: parseFloat(d["5m"] || "0") },
        "15m": { priceChangePercentage: parseFloat(d["15m"] || "0") },
        "30m": { priceChangePercentage: parseFloat(d["30m"] || "0") },
        "1h": { priceChangePercentage: parseFloat(d["1h"] || "0") },
        "4h": { priceChangePercentage: parseFloat(d["4h"] || "0") },
        "24h": { priceChangePercentage: parseFloat(d["24h"] || "0") }
      },
      risk: {
        snipers: {
          count: parseInt(d.snipersCount || "0"),
          totalBalance: parseFloat(d.snipersTotalBal || "0"),
          totalPercentage: parseFloat(d.snipersTotalPct || "0"),
          wallets: safeParse(d.snipers, [])
        },
        insiders: {
          count: parseInt(d.insidersCount || "0"),
          totalBalance: parseFloat(d.insidersTotalBal || "0"),
          totalPercentage: parseFloat(d.insidersTotalPct || "0"),
          wallets: safeParse(d.insiders, [])
        },
        top10: parseFloat(d.top10 || "0"),
        dev: {
          percentage: parseFloat(d.devPercentage || "0"),
          amount: parseFloat(d.devAmount || "0"),
          stats: safeParse(d.devStats, { total_launched: 0, total_migrated: 0 })
        }
      },
      graduation: {
        status: d.graduationStatus || (parseFloat(d.curvePercentage || "0") >= 80 ? "graduating" : "new")
      },
      dev_stats: safeParse(d.devStats, { total_launched: 0, total_migrated: 0 }),
      priceHistory,
      meta: {
        // Completeness flags — frontend uses for progressive loading states.
        // hasPrice:      price/liquidity populated (~100-400ms after launch)
        // hasBasicRisk:  dev history + sniper count in same broadcast as price
        // hasTop10:      top10 concentration + ATA dev% computed (inline or t=2s fallback)
        // hasRisk:       full quick scan complete (superset of hasTop10)
        // hasImage:      IPFS/Arweave metadata resolved (~500ms–3s)
        // expectedRichBy: unix ms by which rich payload (hasPrice+hasTop10) should have arrived.
        //                 Anchored to createdAt so it stays meaningful on re-fetches and REST calls.
        //                 Frontend: if Date.now() > expectedRichBy && !hasPrice → show "data delayed".
        hasPrice:        parseFloat(d.priceUsd || "0") > 0,
        hasBasicRisk:    !!d.devStats,
        hasTop10:        "top10" in d,
        hasRisk:         !!(d.riskQuickScanAt || d.riskAnalyzedAt),
        hasImage:        !!(d.image && d.image !== ""),
        expectedRichBy:  parseInt(d.createdAt || "0") + 450,
        richPayloadDelay: d.richPayloadMs ? parseInt(d.richPayloadMs) : null,
      }
    }
  };

  // Cache for 10 seconds — busted by the pubsub handler before each worker update.
  // Longer TTL helps trending calc and repeated WebSocket snapshot requests.
  if (mint) await redis.setex(`payload:${mint}`, 10, JSON.stringify(payload));
  return payload;
}

function safeParse(json: string | undefined, fallback: any): any {
  if (!json) return fallback;
  try { return JSON.parse(json); } catch { return fallback; }
}

console.log("✅ sol-indexer started — Pump.fun live via Yellowstone");
