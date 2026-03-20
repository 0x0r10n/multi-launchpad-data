// src/index.ts — Full Payload Builder + /new endpoint + Live Broadcasting
import express from "express";
import { Server } from "socket.io";
import { YellowstoneManager } from "./yellowstone-manager";
import { queueEnrichment, startEnrichmentSweep } from "./enricher";
import { queueCurveUpdate, startCurveRefreshLoop } from "./curve-tracker";
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
  socket.on("join", (room: string) => {
    socket.join(room);
    console.log(`[WS] Client joined room: ${room}`);
  });
  // Auto-join "new" room
  socket.join("new");
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

function broadcastTrending(trendingList: any) {
  io.to("trending").emit("message", {
    type: "trending",
    room: "trending",
    data: trendingList
  });
}

// Dedicated chart room — sends ONLY price history (lightweight for chart UIs)
async function broadcastChartData(mint: string) {
  try {
    const history = await getPriceHistory(mint, 500);
    io.to(`chart:${mint}`).emit("message", {
      type: "chart",
      room: `chart:${mint}`,
      data: { mint, priceHistory: history }
    });
  } catch {}
}

// ========== EVENT HANDLERS ==========

yellowstone.on("new-launch", async (data: any) => {
  const payload = await buildTokenPayload(data);
  
  broadcastNewToken(payload);           // "new" room
  broadcastTokenUpdate(data.mint, payload); // individual token room
  
  console.log(`[BROADCAST] 🚀 ${data.name} ($${data.symbol}) | ${data.mint.slice(0, 12)}...`);

  // Queue background enrichment (metadata from IPFS + bonding curve data)
  queueEnrichment(data.mint);
  queueCurveUpdate(data.mint);
  queueRiskAnalysis(data.mint);
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
  broadcastChartData(data.mint);  // lightweight chart-only update

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

// ========== TRENDING JOB ==========
setInterval(async () => {
  try {
    const trendingList = await calculateTrendingTokens();
    broadcastTrending(trendingList);
  } catch (e: any) {
    console.error("[Trending] Job failed:", e.message);
  }
}, 60000);

async function calculateTrendingTokens() {
  const mints = await redis.zrevrange("tokens:latest", 0, 100);
  const payloads = await Promise.all(mints.map(async (mint) => {
    const data = await redis.hgetall(`token:${mint}`);
    if (!data || !data.mint) return null;
    return await buildTokenPayload(data);
  }));
  
  const valid = payloads.filter(p => p !== null);
  // Sort by volume24h (normalized to USD if possible)
  valid.sort((a: any, b: any) => {
    const volA = a.data.pools[0]?.txns?.volume24h || 0;
    const volB = b.data.pools[0]?.txns?.volume24h || 0;
    return volB - volA;
  });
  
  return valid.slice(0, 20);
}

// ========== API ENDPOINTS ==========

// GET /new — JSON list of latest tokens with full payload structure
app.get("/new", async (_req, res) => {
  try {
    const mints = await redis.zrevrange("tokens:latest", 0, 49);
    
    // Fetch all token data in parallel
    const hashes = await Promise.all(mints.map(m => redis.hgetall(`token:${m}`)));
    const validHashes = hashes.filter(d => d && d.mint);
    
    // Build all payloads in parallel
    const tokens = await Promise.all(validHashes.map(d => buildTokenPayload(d)));

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
    const payload = await buildTokenPayload(d);
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

async function buildTokenPayload(d: Record<string, string>): Promise<any> {
  const createdAt = parseInt(d.createdAt || Date.now().toString());
  const mint = d.mint || "";
  const platform = d.platform || "pump";

  // Fetch price history inline so every broadcast includes it
  const priceHistory = mint ? await getPriceHistory(mint, 50) : [];

  return {
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
}

function safeParse(json: string | undefined, fallback: any): any {
  if (!json) return fallback;
  try { return JSON.parse(json); } catch { return fallback; }
}

console.log("✅ sol-indexer started — Pump.fun live via Yellowstone");
