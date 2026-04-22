// src/routes/api.ts — Moralis-style REST API: Token / Price / Wallet endpoints
// Mounted at /api in src/index.ts.  100% additive — no Geyser/WS code touched.
import { Router, Request, Response } from "express";
import Redis from "ioredis";
import "dotenv/config";
import { Connection, PublicKey } from "@solana/web3.js";
import { AccountLayout, TOKEN_PROGRAM_ID } from "@solana/spl-token";

import { getPriceHistory } from "../price-tracker";
import { buildTokenPayload }  from "../payload-builder";
import { getCandlesticks } from "../candlesticks";
import { globalTracker } from "../global-tracker";
import { computeWalletPnl, getTokenPnl } from "../pnl/pnl-engine";

const redis      = new Redis(process.env.REDIS_URL!);
const connection = new Connection(process.env.SOLANA_RPC!, "confirmed");

export const router = Router();

// ── In-memory sliding-window rate limiter ─────────────────────────────────────
// Key = "<ip>:<windowMs>", value = { count, resetAt }.
// Single-instance only — good enough for this deployment topology.
const rlMap = new Map<string, { count: number; resetAt: number }>();

function checkRateLimit(ip: string, limit: number, windowMs = 60_000): boolean {
  const now  = Date.now();
  const key  = `${ip}|${limit}`;
  const slot = rlMap.get(key) ?? { count: 0, resetAt: now + windowMs };
  if (now > slot.resetAt) { slot.count = 0; slot.resetAt = now + windowMs; }
  slot.count++;
  rlMap.set(key, slot);
  return slot.count <= limit;
}

// Evict expired entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of rlMap) if (now > v.resetAt) rlMap.delete(k);
}, 300_000);

// Middleware factories
const rl = (limit: number) => (req: Request, res: Response, next: Function) => {
  if (!checkRateLimit(req.ip ?? "unknown", limit)) {
    return res.status(429).json({ error: "Rate limit exceeded", retryAfterMs: 60_000 });
  }
  next();
};

// ── Redis response cache helper ───────────────────────────────────────────────
async function withCache<T>(key: string, ttlSeconds: number, fn: () => Promise<T>): Promise<T> {
  try {
    const hit = await redis.get(key);
    if (hit) return JSON.parse(hit) as T;
  } catch {}
  const result = await fn();
  if (result != null) redis.setex(key, ttlSeconds, JSON.stringify(result)).catch(() => {});
  return result;
}

// ── Async error wrapper — never crashes the server ────────────────────────────
function wrap(fn: (req: Request, res: Response) => Promise<any>) {
  return (req: Request, res: Response): void => {
    fn(req, res).catch(e => {
      console.error(`[API] ${req.method} ${req.path}:`, e.message);
      const status = (e as any).status ?? 500;
      if (!res.headersSent)
        res.status(status).json({ success: false, error: e.message ?? "Internal server error" });
    });
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// TOKEN API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/tokens
 * Paginated token list with optional filters.
 *
 * Query params:
 *   platform     string   pump | moon | bags | meteora | letsbonk | launchlab
 *   status       string   new | graduating | graduated
 *   sort         string   created (default) | volume | mcap
 *   minMarketCap number   USD floor
 *   minVolume    number   All-time SOL volume floor
 *   page         number   1-based (default 1)
 *   limit        number   max 200 (default 50)
 */
router.get("/tokens", rl(300), wrap(async (req, res) => {
  const limit    = Math.min(parseInt(req.query.limit    as string) || 50,  100);
  const page     = Math.max(parseInt(req.query.page     as string) || 1,   1);
  const platform = req.query.platform as string | undefined;
  const status   = req.query.status   as string | undefined;
  const sort     = (req.query.sort    as string) || "created";
  const minMcap  = parseFloat(req.query.minMarketCap as string) || 0;
  const minVol   = parseFloat(req.query.minVolume    as string) || 0;

  // Pull enough mints to satisfy filters + the requested page (up to 500 total)
  const fetchCount = Math.min(page * limit + 300, 500);
  const mints = await redis.zrevrange("tokens:latest", 0, fetchCount - 1);
  if (!mints.length) return res.json({ success: true, tokens: [], total: 0, page, limit, hasMore: false });

  // One pipeline round-trip to get filter/sort fields for every mint
  const pipe = redis.pipeline();
  for (const m of mints)
    pipe.hmget(`token:${m}`, "platform", "graduationStatus", "marketCapUsd", "volumeUsd",
                              "priceUsd", "name", "symbol", "curvePercentage", "createdAt", "image");
  const rows = await pipe.exec();

  type Row = {
    mint: string; platform: string; status: string;
    marketCapUsd: number; volumeUsd: number; priceUsd: number;
    name: string; symbol: string; curvePercentage: number; createdAt: number; image: string;
  };

  const filtered: Row[] = [];
  for (let i = 0; i < mints.length; i++) {
    const f = rows?.[i]?.[1] as (string | null)[] | null;
    if (!f || !f[5]) continue; // skip tokens with no name yet

    const [tp, st, mcap, vol, price, name, symbol, curve, cat, img] = f;
    const tmcap = parseFloat(mcap  || "0");
    const tvol  = parseFloat(vol   || "0");

    if (platform && (tp || "pump") !== platform) continue;
    if (status   && (st || "new")  !== status)   continue;
    if (minMcap  && tmcap < minMcap) continue;
    if (minVol   && tvol  < minVol)  continue;

    filtered.push({
      mint:            mints[i],
      platform:        tp    || "pump",
      status:          st    || "new",
      marketCapUsd:    tmcap,
      volumeUsd:       tvol,
      priceUsd:        parseFloat(price || "0"),
      name:            name   || "",
      symbol:          symbol || "",
      curvePercentage: parseFloat(curve || "0"),
      createdAt:       parseInt(cat || "0"),
      image:           img || "",
    });
  }

  if (sort === "volume") filtered.sort((a, b) => b.volumeUsd   - a.volumeUsd);
  else if (sort === "mcap") filtered.sort((a, b) => b.marketCapUsd - a.marketCapUsd);
  // "created" → already in zrevrange order

  const total     = filtered.length;
  const paginated = filtered.slice((page - 1) * limit, page * limit);

  res.set("Cache-Control", "public, max-age=5");
  res.json({ success: true, tokens: paginated, total, page, limit, hasMore: page * limit < total });
}));

/**
 * GET /api/token/:mint
 * Full canonical payload — same shape as WebSocket messages.
 */
router.get("/token/:mint", rl(300), wrap(async (req, res) => {
  const mint   = req.params.mint as string;
  const result = await withCache(`rest:token:${mint}`, 8, async () => {
    const d = await redis.hgetall(`token:${mint}`);
    if (!d?.mint) return null;
    return buildTokenPayload(d, true);
  });
  if (!result) return res.status(404).json({ success: false, error: "Token not found" });
  res.set("Cache-Control", "public, max-age=8");
  res.json(result);
}));

/**
 * GET /api/token/:mint/metadata
 * Lightweight identity fields only. No price data. Very fast (single hmget).
 */
router.get("/token/:mint/metadata", rl(300), wrap(async (req, res) => {
  const { mint } = req.params;
  const f = await redis.hmget(
    `token:${mint}`,
    "name", "symbol", "uri", "image", "description",
    "twitter", "telegram", "website", "platform", "createdAt",
    "isMayhemMode", "isCashbackEnabled", "creator",
  );
  if (!f[0]) return res.status(404).json({ success: false, error: "Token not found" });
  const [name, symbol, uri, image, desc, tw, tg, web, plat, cat, mayhem, cashback, creator] = f;

  res.set("Cache-Control", "public, max-age=30");
  res.json({
    success:           true,
    mint,
    name:              name     || "Unknown Token",
    symbol:            symbol   || "???",
    uri:               uri      || "",
    image:             image    || "",
    description:       desc     || "",
    socials:           { twitter: tw || "", telegram: tg || "", website: web || "" },
    platform:          plat     || "pump",
    creator:           creator  || "",
    createdAt:         parseInt(cat || "0"),
    isMayhemMode:      mayhem   === "true",
    isCashbackEnabled: cashback === "true",
  });
}));

/**
 * GET /api/token/:mint/holders
 * Top token holders via RPC. Cached 60 s per mint.
 * Returns up to 20 largest accounts with % of supply.
 */
router.get("/token/:mint/holders", rl(60), wrap(async (req, res) => {
  const mint         = req.params.mint as string;
  const forceRefresh = req.query.refresh === "true";

  const cached = await redis.get(`holders:${mint}`);
  if (cached) {
    res.set("Cache-Control", "public, max-age=60");
    return res.json(JSON.parse(cached));
  }

  if (!forceRefresh) {
    return res.status(202).json({
      success: false,
      error: "Holder data not yet cached. Retry with ?refresh=true to fetch from chain.",
      mint,
    });
  }

  const [curvePDA, creator] = await redis.hmget(`token:${mint}`, "curvePDA", "creator");
  if (!curvePDA && !creator)
    return res.status(404).json({ success: false, error: "Token not found" });

  const accounts = await connection.getTokenLargestAccounts(new PublicKey(mint), "confirmed");
  const SUPPLY   = 1_000_000_000;

  const holders = accounts.value.map((a, i) => ({
    rank:           i + 1,
    address:        a.address.toBase58(),
    uiAmount:       a.uiAmount ?? 0,
    percentage:     parseFloat((((a.uiAmount ?? 0) / SUPPLY) * 100).toFixed(2)),
    isBondingCurve: a.address.toBase58() === curvePDA,
    isCreator:      a.address.toBase58() === creator,
  }));

  const result = { success: true, mint, holders, totalAccounts: holders.length, fetchedAt: Date.now() };
  await redis.setex(`holders:${mint}`, 60, JSON.stringify(result));

  res.set("Cache-Control", "public, max-age=60");
  res.json(result);
}));

/**
 * GET /api/token/:mint/pairs
 * Bonding-curve pool data (price, liquidity, curve %, reserves).
 */
router.get("/token/:mint/pairs", rl(300), wrap(async (req, res) => {
  const { mint } = req.params;
  const f = await redis.hmget(
    `token:${mint}`,
    "curvePDA", "platform", "priceQuote", "priceUsd",
    "liquidity", "liquidityUsd", "marketCapUsd", "curvePercentage",
    "complete", "createdAt", "creator",
    "virtualSolReserves", "virtualTokenReserves", "realSolReserves",
  );
  if (!f[0] && !f[10]) return res.status(404).json({ success: false, error: "Token not found" });
  const [curvePDA, plat, pq, pu, liq, liqUsd, mcap, curve, complete, cat, creator, vSol, vToken, rSol] = f;

  res.set("Cache-Control", "public, max-age=8");
  res.json({
    success: true,
    mint,
    pairs: [{
      pairAddress:     curvePDA || "",
      platform:        plat     || "pump",
      type:            "bonding-curve",
      priceNative:     parseFloat(pq   || "0"),
      priceUsd:        parseFloat(pu   || "0"),
      liquidity:       { sol: parseFloat(liq    || "0"), usd: parseFloat(liqUsd || "0") },
      marketCapUsd:    parseFloat(mcap  || "0"),
      curvePercentage: parseFloat(curve || "0"),
      complete:        complete  === "true",
      reserves: {
        virtualSol:    parseFloat(vSol   || "0") / 1e9,
        virtualToken:  parseFloat(vToken || "0") / 1e6,
        realSol:       parseFloat(rSol   || "0") / 1e9,
      },
      creator:         creator  || "",
      createdAt:       parseInt(cat || "0"),
    }],
  });
}));

/**
 * GET /api/token/:mint/trades
 * Live swap history with exact SOL + token amounts, precise priority fees.
 * Scores are Solana slot numbers — naturally ordered and deduplicated by signature.
 *
 * Query:
 *   limit   number  default 50, max 500
 *   before  string  signature from previous page's nextCursor (exclusive, cursor pagination)
 *
 * Cursor usage:
 *   First page:  GET /api/token/:mint/trades?limit=50
 *   Next page:   GET /api/token/:mint/trades?limit=50&before=<nextCursor>
 */
router.get("/token/:mint/trades", rl(300), wrap(async (req, res) => {
  const mint   = req.params.mint as string;
  const limit  = Math.min(parseInt(req.query.limit as string) || 50, 500);
  const before = req.query.before as string | undefined;

  // Resolve signature cursor → slot for ZREVRANGEBYSCORE
  let upperBound = "+inf";
  if (before) {
    const metaRaw = await redis.hget(`txs_meta:${mint}`, before);
    if (!metaRaw) {
      return res.json({ success: true, data: [], nextCursor: null });
    }
    const { slot } = JSON.parse(metaRaw);
    upperBound = `(${slot}`;
  }

  const raw = await redis.zrevrangebyscore(
    `txs:${mint}`, upperBound, "-inf", "WITHSCORES", "LIMIT", 0, limit,
  );

  const data: any[] = [];
  const sigs: string[] = [];
  for (let i = 0; i < raw.length; i += 2) {
    try {
      const entry = JSON.parse(raw[i]);
      entry.slot = parseInt(raw[i + 1]);
      data.push(entry);
      sigs.push(entry.signature);
    } catch {}
  }

  // Batch-fetch timestamps
  if (sigs.length > 0) {
    const metas = await redis.hmget(`txs_meta:${mint}`, ...sigs);
    for (let i = 0; i < data.length; i++) {
      try {
        const m = metas[i] ? JSON.parse(metas[i]!) : null;
        data[i].timestamp = m?.ts ?? null;
      } catch { data[i].timestamp = null; }
    }
  }

  const nextCursor = data.length === limit ? (data[data.length - 1]?.signature ?? null) : null;

  res.set("Cache-Control", "public, max-age=3");
  res.json({ success: true, data, nextCursor });
}));

/**
 * GET /api/token/:mint/history
 * Raw price ticks (time, price in SOL, price in USD).
 * Query: limit (default 300, max 1000)
 */
router.get("/token/:mint/history", rl(300), wrap(async (req, res) => {
  const limit   = Math.min(parseInt(req.query.limit as string) || 300, 1000);
  const history = await getPriceHistory(req.params.mint as string, limit);
  res.set("Cache-Control", "public, max-age=5");
  res.json({ success: true, mint: req.params.mint, count: history.length, history });
}));

// ─────────────────────────────────────────────────────────────────────────────
// PRICE API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/price/:mint
 * Current price + 7-interval change % + 24h volume.
 */
router.get("/price/:mint", rl(300), wrap(async (req, res) => {
  const mint = req.params.mint as string;
  const f = await redis.hmget(
    `token:${mint}`,
    "priceQuote", "priceUsd", "marketCapUsd",
    "liquidity", "liquidityUsd",
    "1m", "5m", "15m", "30m", "1h", "4h", "24h",
    "volumeUsd", "volume24hUsd",
  );
  if (!f[0]) {
    // Auto-trigger global tracking for unknown tokens — returns 202 while warming up
    const status = await globalTracker.watchToken(mint);
    return res.status(202).json({
      success: true,
      status: status === "tracking" ? "warming_up" : status,
      mint,
      message: "Token tracking initiated. Price data will be available within 5-10 seconds.",
    });
  }
  const [pq, pu, mcap, liq, liqUsd, c1m, c5m, c15m, c30m, c1h, c4h, c24h, volAll, vol24h] = f;

  res.set("Cache-Control", "public, max-age=5");
  res.json({
    success:      true,
    mint,
    price:       { native: parseFloat(pq || "0"), usd: parseFloat(pu || "0") },
    marketCapUsd: parseFloat(mcap   || "0"),
    liquidity:   { native: parseFloat(liq || "0"), usd: parseFloat(liqUsd || "0") },
    priceChange: {
      "1m":  parseFloat(c1m  || "0"),
      "5m":  parseFloat(c5m  || "0"),
      "15m": parseFloat(c15m || "0"),
      "30m": parseFloat(c30m || "0"),
      "1h":  parseFloat(c1h  || "0"),
      "4h":  parseFloat(c4h  || "0"),
      "24h": parseFloat(c24h || "0"),
    },
    volume: { "24h": parseFloat(vol24h || "0"), all: parseFloat(volAll || "0") },
    updatedAt: Date.now(),
  });
}));

/**
 * GET /api/price/:mint/history
 * OHLCV candlestick data built from stored price ticks + trade volume.
 *
 * Query:
 *   interval  string  1m | 5m | 15m | 30m | 1h | 4h  (default 5m)
 *   limit     number  max 500 candles (default 100)
 */
router.get("/price/:mint/history", rl(300), wrap(async (req, res) => {
  const mint        = req.params.mint as string;
  const interval    = (req.query.interval as string) || "5m";
  const limit       = Math.min(parseInt(req.query.limit as string) || 100, 500);
  const INTERVAL_MS: Record<string, number> = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000,
  };
  const bucketMs = INTERVAL_MS[interval] ?? 300_000;

  const cacheKey = `ohlcv:${mint}:${interval}:${limit}`;
  const result   = await withCache(cacheKey, 10, async () => {
    const cutoff = Date.now() - bucketMs * limit;
    const [ticks, trades] = await Promise.all([
      getPriceHistory(mint, 1000),
      redis.zrangebyscore(`trades:${mint}`, cutoff, "+inf"),
    ]);
    if (!ticks.length) return { mint, interval, candles: [] };

    type Bucket = { o: number; h: number; l: number; c: number; v: number };
    const buckets = new Map<number, Bucket>();

    // Build OHLC from price ticks
    for (const tick of ticks) {
      if (tick.time < cutoff) continue;
      const ts = Math.floor(tick.time / bucketMs) * bucketMs;
      const b  = buckets.get(ts);
      const p  = tick.price_usd;
      if (!b) {
        buckets.set(ts, { o: p, h: p, l: p, c: p, v: 0 });
      } else {
        if (p > b.h) b.h = p;
        if (p < b.l) b.l = p;
        b.c = p;
      }
    }

    // Add volume from trade sorted set
    for (const entry of trades) {
      const parts = entry.split(":");
      const sol   = parseFloat(parts[1] || "0");
      const ts    = parseInt(parts[2]   || "0");
      if (!sol || !ts) continue;
      const bucket = Math.floor(ts / bucketMs) * bucketMs;
      const b = buckets.get(bucket);
      if (b) b.v += sol;
    }

    const candles = [...buckets.entries()]
      .sort(([a], [b]) => a - b)
      .slice(-limit)
      .map(([time, b]) => ({
        time,
        open:   b.o,
        high:   b.h,
        low:    b.l,
        close:  b.c,
        volume: parseFloat(b.v.toFixed(4)),
      }));

    return { mint, interval, candles };
  });

  res.set("Cache-Control", "public, max-age=10");
  res.json({ success: true, ...result });
}));

/**
 * POST /api/price/multiple
 * Batch price lookup for up to 100 mints. Single pipeline round-trip.
 *
 * Body: { mints: string[] }
 */
router.post("/price/multiple", rl(300), wrap(async (req, res) => {
  const { mints } = req.body as { mints?: string[] };
  if (!Array.isArray(mints) || mints.length === 0)
    return res.status(400).json({ success: false, error: "Body must contain a non-empty 'mints' array" });

  const bounded = mints.slice(0, 50);
  const pipe    = redis.pipeline();
  for (const m of bounded)
    pipe.hmget(`token:${m}`, "priceUsd", "priceQuote", "marketCapUsd", "24h", "liquidityUsd", "name", "symbol");
  const results = await pipe.exec();

  const prices: Record<string, any> = {};
  for (let i = 0; i < bounded.length; i++) {
    const f = results?.[i]?.[1] as (string | null)[] | null;
    if (!f?.[0]) continue;
    const [pu, pq, mcap, c24h, liqUsd, name, symbol] = f;
    prices[bounded[i]] = {
      name:         name   || "",
      symbol:       symbol || "",
      priceUsd:     parseFloat(pu    || "0"),
      priceNative:  parseFloat(pq    || "0"),
      marketCapUsd: parseFloat(mcap  || "0"),
      change24h:    parseFloat(c24h  || "0"),
      liquidityUsd: parseFloat(liqUsd || "0"),
    };
  }

  res.set("Cache-Control", "public, max-age=5");
  res.json({ success: true, prices, count: Object.keys(prices).length, requested: bounded.length });
}));

// ─────────────────────────────────────────────────────────────────────────────
// WALLET API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/wallet/:wallet/net-worth
 * Portfolio value in our indexed token universe.
 * Fetches all SPL token accounts via RPC, cross-references with Redis for prices.
 * Cached 30 s per wallet.
 *
 * Response:
 *   solBalance       SOL balance
 *   totalUsd         total value of indexed holdings in USD
 *   holdings[]       tokens we have price data for
 *   unindexed        count of token accounts with no price in our system
 */
router.get("/wallet/:wallet/net-worth", rl(60), wrap(async (req, res) => {
  const wallet       = req.params.wallet as string;
  const forceRefresh = req.query.refresh === "true";

  const cached = await redis.get(`wallet-worth:${wallet}`);
  if (cached) {
    res.set("Cache-Control", "private, max-age=30");
    return res.json(JSON.parse(cached));
  }

  if (!forceRefresh) {
    return res.status(202).json({
      success: false,
      error: "Portfolio data not yet cached. Retry with ?refresh=true to fetch from chain.",
      wallet,
    });
  }

  let pubkey: PublicKey;
  try { pubkey = new PublicKey(wallet); }
  catch { return res.status(400).json({ success: false, error: "Invalid wallet address" }); }

  const timeout = new Promise<never>((_, rej) => setTimeout(() => rej(new Error("RPC timeout")), 8_000));

  const [tokenAccounts, solLamports] = await Promise.race([
    Promise.all([
      connection.getTokenAccountsByOwner(pubkey, { programId: TOKEN_PROGRAM_ID }),
      connection.getBalance(pubkey),
    ]),
    timeout,
  ]);

  const raw: { mint: string; amount: number; address: string }[] = [];
  for (const { pubkey: addr, account } of tokenAccounts.value) {
    const data   = AccountLayout.decode(account.data);
    const amount = Number(data.amount);
    if (amount === 0) continue;
    raw.push({ mint: data.mint.toBase58(), amount, address: addr.toBase58() });
  }

  let totalUsd  = 0;
  let unindexed = 0;
  const holdings: any[] = [];

  if (raw.length) {
    const pipe = redis.pipeline();
    for (const r of raw) pipe.hmget(`token:${r.mint}`, "priceUsd", "name", "symbol", "decimals");
    const pipeResults = await pipe.exec();

    for (let i = 0; i < raw.length; i++) {
      const f = pipeResults?.[i]?.[1] as (string | null)[] | null;
      if (!f?.[0]) { unindexed++; continue; }
      const [priceUsdStr, name, symbol, decimalsStr] = f;
      const decimals = parseInt(decimalsStr || "6");
      const uiAmount = raw[i].amount / Math.pow(10, decimals);
      const priceUsd = parseFloat(priceUsdStr || "0");
      const valueUsd = priceUsd * uiAmount;
      totalUsd += valueUsd;
      holdings.push({
        mint:     raw[i].mint,
        address:  raw[i].address,
        name:     name   || "Unknown Token",
        symbol:   symbol || "???",
        amount:   parseFloat(uiAmount.toFixed(decimals)),
        priceUsd,
        valueUsd: parseFloat(valueUsd.toFixed(4)),
      });
    }
    holdings.sort((a, b) => b.valueUsd - a.valueUsd);
  }

  const result = {
    success:    true,
    wallet,
    solBalance: solLamports / 1e9,
    totalUsd:   parseFloat(totalUsd.toFixed(4)),
    holdings,
    unindexed,
    fetchedAt:  Date.now(),
  };
  await redis.setex(`wallet-worth:${wallet}`, 30, JSON.stringify(result));

  res.set("Cache-Control", "private, max-age=30");
  res.json(result);
}));

/**
 * GET /api/wallet/:wallet/transactions
 * Returns token-launch and sniper activity for the wallet within our indexed universe.
 *
 * Data sources:
 *   creator:{wallet}  → total_launched / total_migrated counts
 *   tokens:latest     → scanned for tokens where creator === wallet
 *   snipers_set:{mint}→ scanned for mints where this wallet appears as an early buyer
 *
 * Query:
 *   limit  number  max 100 (default 50)
 *   cursor string  createdAt ms value — return tokens older than this for pagination
 */
router.get("/wallet/:wallet/transactions", rl(60), wrap(async (req, res) => {
  const { wallet }  = req.params;
  const limit       = Math.min(parseInt(req.query.limit as string) || 50, 100);
  const cursorMs    = parseInt(req.query.cursor as string) || Date.now();

  const result = await withCache(`wallet-txns:${wallet}:${cursorMs}:${limit}`, 15, async () => {
    // Fetch creator aggregate stats
    const creatorHash = await redis.hgetall(`creator:${wallet}`);
    const stats = {
      total_launched: parseInt(creatorHash?.launched  || "0"),
      total_migrated: parseInt(creatorHash?.migrated  || "0"),
    };

    // Scan tokens:latest for mints created by this wallet (pipeline — 1 round-trip)
    const mints = await redis.zrevrangebyscore("tokens:latest", cursorMs, 0, "LIMIT", 0, 500);
    if (!mints.length) {
      return { wallet, stats, transactions: [], hasMore: false, cursor: null };
    }

    const pipe = redis.pipeline();
    for (const m of mints) pipe.hmget(`token:${m}`, "creator", "name", "symbol", "createdAt", "platform", "priceUsd", "marketCapUsd", "graduationStatus");
    const rows = await pipe.exec();

    const transactions: any[] = [];

    for (let i = 0; i < mints.length; i++) {
      const f = rows?.[i]?.[1] as (string | null)[] | null;
      if (!f) continue;
      const [creator, name, symbol, cat, plat, priceUsd, mcap, gradStatus] = f;
      if (creator !== wallet) continue;

      transactions.push({
        type:           "CREATE",
        mint:           mints[i],
        name:           name   || "Unknown Token",
        symbol:         symbol || "???",
        platform:       plat   || "pump",
        createdAt:      parseInt(cat || "0"),
        currentPriceUsd: parseFloat(priceUsd || "0"),
        marketCapUsd:   parseFloat(mcap || "0"),
        status:         gradStatus || "new",
      });

      if (transactions.length >= limit) break;
    }

    // Sort newest-first
    transactions.sort((a, b) => b.createdAt - a.createdAt);
    const last   = transactions[transactions.length - 1];
    const cursor = transactions.length >= limit && last ? last.createdAt.toString() : null;

    return { wallet, stats, transactions, hasMore: !!cursor, cursor };
  });

  res.set("Cache-Control", "private, max-age=15");
  res.json({ success: true, ...result });
}));

/**
 * GET /api/wallet/:wallet/history
 * Performance of tokens launched by this wallet over time.
 * Returns each launched token with current price, % change, and graduation status.
 *
 * Useful for "dev track record" display on the Caesarx terminal.
 */
router.get("/wallet/:wallet/history", rl(60), wrap(async (req, res) => {
  const { wallet } = req.params;

  const result = await withCache(`wallet-history:${wallet}`, 30, async () => {
    const creatorHash = await redis.hgetall(`creator:${wallet}`);
    const stats = {
      total_launched: parseInt(creatorHash?.launched || "0"),
      total_migrated: parseInt(creatorHash?.migrated || "0"),
    };

    if (stats.total_launched === 0)
      return { wallet, stats, tokens: [], fetchedAt: Date.now() };

    // Scan up to 200 recent tokens to find those from this wallet
    const mints = await redis.zrevrange("tokens:latest", 0, 499);
    const pipe  = redis.pipeline();
    for (const m of mints)
      pipe.hmget(`token:${m}`, "creator", "name", "symbol", "createdAt", "platform",
                                "priceUsd", "marketCapUsd", "graduationStatus",
                                "curvePercentage", "1h", "24h");
    const rows = await pipe.exec();

    const tokens: any[] = [];
    for (let i = 0; i < mints.length; i++) {
      const f = rows?.[i]?.[1] as (string | null)[] | null;
      if (!f || f[0] !== wallet) continue;
      const [, name, symbol, cat, plat, pu, mcap, gs, curve, c1h, c24h] = f;
      tokens.push({
        mint:            mints[i],
        name:            name   || "Unknown Token",
        symbol:          symbol || "???",
        platform:        plat   || "pump",
        createdAt:       parseInt(cat || "0"),
        priceUsd:        parseFloat(pu    || "0"),
        marketCapUsd:    parseFloat(mcap  || "0"),
        graduationStatus: gs || "new",
        curvePercentage: parseFloat(curve || "0"),
        change1h:        parseFloat(c1h   || "0"),
        change24h:       parseFloat(c24h  || "0"),
      });
    }

    tokens.sort((a, b) => b.createdAt - a.createdAt);
    return { wallet, stats, tokens, fetchedAt: Date.now() };
  });

  res.set("Cache-Control", "private, max-age=30");
  res.json({ success: true, ...result });
}));

// ─────────────────────────────────────────────────────────────────────────────
// SYSTEM
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/stats
 * Service health: token count, uptime, connected streams.
 */
router.get("/stats", rl(300), wrap(async (_req, res) => {
  const tokenCount = await redis.zcard("tokens:latest");
  res.json({
    success:       true,
    status:        "running",
    tokensInRedis: tokenCount,
    uptime:        process.uptime().toFixed(0) + "s",
    timestamp:     new Date().toISOString(),
  });
}));

// ═══════════════════════════════════════════════════════════════════════════════
// WALLET PNL API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/wallet/:wallet/pnl
 * Full wallet PnL: summary + all token positions.
 *
 * Computes weighted-average cost basis from on-chain trade history.
 * First request triggers an RPC scan (~3-8s), subsequent requests are cached (2min).
 *
 * Response: WalletPnlResponse { wallet, summary, tokens, updatedAt }
 */
router.get("/wallet/:wallet/pnl", rl(60), wrap(async (req, res) => {
  const wallet = req.params.wallet as string;

  try { new PublicKey(wallet); }
  catch { return res.status(400).json({ success: false, error: "Invalid wallet address" }); }

  try {
    const result = await computeWalletPnl(wallet);
    return res.json({ success: true, ...result });
  } catch (e: any) {
    console.error(`[PnL] Computation failed for ${wallet.slice(0, 12)}:`, e.message);
    return res.status(500).json({ success: false, error: "PnL computation failed" });
  }
}));

/**
 * GET /api/wallet/:wallet/pnl/:mint
 * PnL for a specific token position.
 *
 * Returns the per-token PnL object or 404 if no trades found for that mint.
 */
router.get("/wallet/:wallet/pnl/:mint", rl(60), wrap(async (req, res) => {
  const wallet = req.params.wallet as string;
  const mint   = req.params.mint as string;

  try { new PublicKey(wallet); }
  catch { return res.status(400).json({ success: false, error: "Invalid wallet address" }); }

  try { new PublicKey(mint); }
  catch { return res.status(400).json({ success: false, error: "Invalid mint address" }); }

  try {
    const tokenPnl = await getTokenPnl(wallet, mint);
    if (!tokenPnl) {
      return res.status(404).json({ success: false, error: "No trades found for this token" });
    }
    return res.json({ success: true, wallet, ...tokenPnl });
  } catch (e: any) {
    console.error(`[PnL] Token PnL failed for ${wallet.slice(0, 12)}/${mint.slice(0, 12)}:`, e.message);
    return res.status(500).json({ success: false, error: "PnL computation failed" });
  }
}));

/**
 * POST /api/price/:mint/watch
 * Start tracking any token globally. Idempotent — no-op if already tracked.
 */
router.post("/price/:mint/watch", rl(60), wrap(async (req, res) => {
  const mint = req.params.mint as string;
  if (!mint || mint.length < 32) {
    return res.status(400).json({ success: false, error: "Invalid mint address" });
  }

  const status = await globalTracker.watchToken(mint);
  const since = await redis.hget(`token:${mint}`, "createdAt");

  res.json({
    success: true,
    status,
    mint,
    since: since ? parseInt(since) : null,
    watchedCount: globalTracker.watchedCount,
  });
}));

/**
 * DELETE /api/price/:mint/watch
 * Stop tracking a globally-watched token. Frees Geyser capacity.
 */
router.delete("/price/:mint/watch", rl(60), wrap(async (req, res) => {
  const mint = req.params.mint as string;
  const removed = await globalTracker.unwatchToken(mint);

  res.json({
    success: true,
    removed,
    mint,
    watchedCount: globalTracker.watchedCount,
  });
}));

/**
 * GET /api/global/status
 * Returns the current state of the global tracker.
 */
router.get("/global/status", rl(300), wrap(async (_req, res) => {
  res.json({
    success: true,
    watchedCount: globalTracker.watchedCount,
    maxCapacity: 200,
    uptime: process.uptime().toFixed(0) + "s",
  });
}));

/**
 * GET /api/price/:address/candlesticks
 * Returns OHLCV candles matching the Axiom/Moralis API schema.
 */
router.get("/price/:address/candlesticks", rl(100), wrap(async (req, res) => {
  const address = req.params.address as string;
  const cursor = req.query.cursor as string | undefined;
  const timeframe = (req.query.timeframe as string) || "1min";
  const currency = (req.query.currency as string) || "usd";
  const limit = Math.min(Math.max(parseInt(req.query.limit as string) || 100, 1), 1000);
  
  let fromDate = req.query.fromDate as string;
  let toDate = req.query.toDate as string;

  // Convert to unix seconds
  let fromSec = 0;
  let toSec = Math.floor(Date.now() / 1000);

  if (fromDate) {
    if (/^\d+$/.test(fromDate)) {
      fromSec = parseInt(fromDate);
    } else {
      fromSec = Math.floor(new Date(fromDate).getTime() / 1000);
    }
  } else {
    // Default to 1 day ago if not provided (per schema defaults roughly)
    fromSec = toSec - 86400;
  }

  if (toDate) {
    if (/^\d+$/.test(toDate)) {
      toSec = parseInt(toDate);
    } else {
      toSec = Math.floor(new Date(toDate).getTime() / 1000);
    }
  }

  // Ensure tracking is active for this token (no-op if already tracking)
  globalTracker.watchToken(address).catch(console.error);

  const candles = await getCandlesticks(address, timeframe, fromSec, toSec, currency as "usd" | "native", limit);

  return res.json({
    cursor: null,
    page: 1,
    pairAddress: address, // Or use the actual pool address if we want to fetch it
    tokenAddress: address,
    timeframe,
    currency,
    result: candles,
  });
}));
