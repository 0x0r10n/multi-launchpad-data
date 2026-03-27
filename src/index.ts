// src/index.ts — Full Payload Builder + /new endpoint + Live Broadcasting
import express from "express";
import { Server } from "socket.io";
import { YellowstoneManager } from "./yellowstone-manager";
import { queueEnrichment, startEnrichmentSweep } from "./enricher";
import { queueCurveUpdate, startCurveRefreshLoop, fetchAndDecodeCurve } from "./curve-tracker";
import { startPriceTracker, getPriceHistory, recordPrice, calcPriceEvents } from "./price-tracker";
import { queueRiskAnalysis, startRiskAnalysisLoop } from "./risk-analyzer";
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

yellowstone.start().catch((err) => console.error("Yellowstone start failed:", err));

// Start periodic curve refresh for active tokens
startCurveRefreshLoop();
startPriceTracker();
startRiskAnalysisLoop();
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

yellowstone.on("new-launch", async (data: any) => {
  // Fetch bonding curve inline — price/liquidity/curvePercentage in the first broadcast.
  // Falls back gracefully (fields stay 0) if the account isn't readable yet.
  if (data.curvePDA) {
    const curveFields = await fetchAndDecodeCurve(data.mint, data.curvePDA, data.platform || "pump");
    if (curveFields) {
      Object.assign(data, curveFields);
      // Persist curve fields async — don't block the broadcast
      redis.hset(`token:${data.mint}`, curveFields).catch(() => {});
    }
  }

  // New tokens have no price history — pass [] directly to skip the Redis lrange call
  const payload = await buildTokenPayload(data, false, []);
  broadcastNewToken(payload);
  broadcastTokenUpdate(data.mint, payload);

  console.log(`[BROADCAST] 🚀 ${data.name} ($${data.symbol}) | price=${parseFloat(data.priceUsd || "0").toFixed(8)} | ${data.mint.slice(0, 12)}...`);

  // Background: image/socials (IPFS — slow, non-blocking) and risk (RPC-heavy)
  queueEnrichment(data.mint);
  queueRiskAnalysis(data.mint);
  // Note: curve already fetched inline above. Trade events drive future curve updates.
});

yellowstone.on("trade", async (data: any) => {
  // We don't build full payload here to save RPC/Redis usage
  // Just emit the trade event to the specific token room
  io.to(`token:${data.mint}`).emit("trade", {
    mint: data.mint, signature: data.signature,
    type: data.type, solAmount: data.solAmount
  });
  
  // Refresh curve data, record price tick, and recalc events on every trade
  // These will trigger a PUBSUB message which then broadcasts the full payload update
  queueCurveUpdate(data.mint);
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

// GET / — simple HTML dashboard
app.get("/", async (_req, res) => {
  const mints = await redis.zrevrange("tokens:latest", 0, 19);
  const tokens: any[] = [];
  for (const mint of mints) {
    const data = await redis.hgetall(`token:${mint}`);
    if (data?.mint) tokens.push(data);
  }

  const rows = tokens.map(t =>
    `<tr>
      <td><a href="/api/token/${t.mint}" style="color:#0f0">${t.mint?.slice(0,16)}...</a></td>
      <td>${t.name || 'Unknown'}</td>
      <td>${t.symbol || '???'}</td>
      <td>${t.creator?.slice(0,12)}...</td>
      <td>${t.createdAt ? new Date(parseInt(t.createdAt)).toLocaleTimeString() : '-'}</td>
    </tr>`
  ).join("");

  const total = await redis.zcard("tokens:latest");

  res.send(`
    <html>
    <head><title>Pump.fun Live Tracker</title>
    <style>
      body { font-family: monospace; background: #111; color: #0f0; padding: 20px; }
      h1 { color: #0ff; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #333; padding: 8px; text-align: left; }
      th { background: #222; color: #0ff; }
      tr:hover { background: #1a1a1a; }
      .stats { color: #ff0; margin-bottom: 20px; }
      a { text-decoration: none; }
    </style>
    </head>
    <body>
      <h1>🚀 Pump.fun Live Tracker</h1>
      <div class="stats">Total Tokens: ${total} | Showing latest 20 | <a href="/new" style="color:#0ff">/new (JSON)</a> | <a href="/api/stats" style="color:#0ff">/api/stats</a></div>
      <table>
        <tr><th>Mint</th><th>Name</th><th>Symbol</th><th>Creator</th><th>Time</th></tr>
        ${rows}
      </table>
      <script>setTimeout(() => location.reload(), 5000);</script>
    </body>
    </html>
  `);
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
      priceHistory
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
