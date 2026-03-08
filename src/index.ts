// src/index.ts
import { Connection, PublicKey } from '@solana/web3.js';
import WebSocket, { Server as SocketServer } from 'ws';
import sqlite3 from 'sqlite3';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import express, { Request, Response } from 'express';
import http from 'http';
import { Server } from 'socket.io';
const cors = require('cors');
const bs58 = require('bs58');

dotenv.config();

// BigInt serialization fix for Socket.io
(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};


process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.message);
});

const RPC = process.env.SOLANA_RPC || process.env.RPC_ENDPOINT || 'https://api.mainnet-beta.solana.com';
const WSS_ENDPOINTS = [
  process.env.SOLANA_WSS,
  process.env.WSS_ENDPOINT,
  process.env.WSS_ENDPOINT_BACKUP,
].filter(Boolean) as string[];

for (let i = 2; i <= 20; i++) {
  const key = `WSS_ENDPOINT_BACKUP_${i}`;
  if (process.env[key]) WSS_ENDPOINTS.push(process.env[key] as string);
}
WSS_ENDPOINTS.push('wss://api.mainnet-beta.solana.com');

const UNIQUE_WSS = [...new Set(WSS_ENDPOINTS.filter(Boolean))];
const RPC_ENDPOINTS = UNIQUE_WSS.map(w => w.replace('wss://', 'https://'));

// Rate limit queue for RPC calls with Load Balancing
class RPCQueue {
  private queue: ((conn: Connection) => Promise<any>)[] = [];
  private running = 0;
  private maxConcurrent = 50; // Increased to allow better utilization of multiple endpoints
  private connections: { conn: Connection, lastUsed: number, reqCount: number, windowStart: number }[] = [];
  private connIndex = 0;

  constructor() {
    this.connections = RPC_ENDPOINTS.map(rpc => ({
      conn: new Connection(rpc, 'confirmed'),
      lastUsed: 0,
      reqCount: 0,
      windowStart: Date.now()
    }));
  }

  get nextConn() {
    const now = Date.now();
    // Try to find a connection that hasn't hit the 25 req/sec limit
    for (let i = 0; i < this.connections.length; i++) {
      const idx = (this.connIndex + i) % this.connections.length;
      const c = this.connections[idx];

      // Reset window every second
      if (now - c.windowStart > 1000) {
        c.reqCount = 0;
        c.windowStart = now;
      }

      if (c.reqCount < 25) {
        c.reqCount++;
        c.lastUsed = now;
        this.connIndex = (idx + 1) % this.connections.length;
        return c.conn;
      }
    }
    return null; // All connections are rate-limited
  }

  async add<T>(fn: (conn: Connection) => Promise<T>, retries = 5): Promise<T> {
    return new Promise((resolve, reject) => {
      this.queue.push(async (conn: Connection) => {
        try {
          const res = await fn(conn);
          resolve(res);
        } catch (e: any) {
          const msg = e.message || '';
          const isRateLimit = msg.includes('429') || msg.includes('Too Many Requests');
          const isNotFoundError = msg.includes('could not find mint') || msg.includes('AccountNotFound');

          if ((isRateLimit || isNotFoundError) && retries > 0) {
            const wait = isRateLimit
              ? Math.pow(2, 6 - retries) * 500 + Math.random() * 1000
              : 1000 + (5 - retries) * 1000; // Slower retry for not found

            console.log(`⚠️ ${isRateLimit ? 'Rate limit hit' : 'Mint not found'}, retrying in ${Math.round(wait)}ms... (${retries} retries left)`);
            setTimeout(() => {
              this.add(fn, retries - 1).then(resolve).catch(reject);
            }, wait);
          } else {
            reject(e);
          }
        }
      });
      this.process();
    });
  }

  private async process() {
    if (this.running >= this.maxConcurrent || this.queue.length === 0) return;

    const conn = this.nextConn;
    if (!conn) {
      // All connections are full, wait a bit and try again
      setTimeout(() => this.process(), 50);
      return;
    }

    this.running++;
    const fn = this.queue.shift();
    if (fn) {
      fn(conn).finally(() => {
        this.running--;
        this.process();
      });
    } else {
      this.running--;
      this.process();
    }
  }
}

const rpcQueue = new RPCQueue();


let wssIndex = 0;
let currentWss = UNIQUE_WSS[wssIndex];
let currentRpc = RPC_ENDPOINTS[wssIndex];
const PORT = Number(process.env.PORT) || 3000;

console.log(`📡 Endpoints initialized: ${UNIQUE_WSS.length} (Primary: ${currentWss.slice(0, 35)}...)`);

import { detectLaunchpad, PUMP_PROGRAM_ID, MOONSHOT_PROGRAM_ID, RAYDIUM_LAUNCHLAB_PROGRAM, METDBC_PROGRAM, BAGS_FEE_PROGRAM } from './launchpad-detectors';
const DAOS_FUN_PROGRAM = new PublicKey('4FqThZWv3QKWkSyXCDmATpWkpEiCHq5yhkdGWpSEDAZM');
const RAYDIUM_AMM_PROGRAM = new PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

const SYSTEM_PROGRAM_ID = new PublicKey('11111111111111111111111111111111');
const METAPLEX_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');

const WATCH_PROGRAMS = [
  PUMP_PROGRAM_ID,
  MOONSHOT_PROGRAM_ID,
  RAYDIUM_LAUNCHLAB_PROGRAM,
  METDBC_PROGRAM,
  BAGS_FEE_PROGRAM,
  new PublicKey('BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv'),
  DAOS_FUN_PROGRAM,
  RAYDIUM_AMM_PROGRAM,
  METAPLEX_PROGRAM_ID // 🛰️ Satellite Subscription
];

// Global Metadata Cache (The Pre-Indexer)
const metadataRegistry = new Map<string, { name: string, symbol: string, uri: string }>();
// Cleanup registry periodically to save memory
setInterval(() => metadataRegistry.clear(), 300000); // 5 min

async function getMetadataWithWait(mint: string, retries = 10, delay = 600) {
  // ⚡ Check Registry First
  const cached = metadataRegistry.get(mint);
  if (cached && cached.name !== 'Unknown') return cached;

  const metadataPDA = PublicKey.findProgramAddressSync(
    [Buffer.from('metadata'), METAPLEX_PROGRAM_ID.toBuffer(), new PublicKey(mint).toBuffer()],
    METAPLEX_PROGRAM_ID
  )[0];

  for (let i = 0; i < retries; i++) {
    try {
      const account = await rpcQueue.add((c) => c.getAccountInfo(metadataPDA));
      if (account && account.data.length > 65) {
        const parsed = parseMetaplexMetadata(account.data);
        if (parsed.name && parsed.name !== 'Unknown' && parsed.name.length > 0) {
          metadataRegistry.set(mint, parsed); // Cache it!
          return parsed;
        }
      }
    } catch (e) { }
    await new Promise(r => setTimeout(r, delay));
  }
  return null;
}
const DEBUG_MULTI_LAUNCHPAD = false; // protection checklist step 3

let connection = new Connection(currentRpc, { commitment: 'confirmed' });

// Global cache for real-time trade monitoring
const monitoredMints = new Set<string>();
const processedSignatures = new Set<string>();
setInterval(() => processedSignatures.clear(), 60000); // Clear every minute

// Database initialization
const db = new sqlite3.Database('./mainnet.db', (err) => {
  if (err) console.error('❌ SQLite error:', err);
  else {
    console.log('✅ SQLite DB ready');
    // Load existing mints into cache
    db.all('SELECT coinMint FROM tokens ORDER BY created_at DESC LIMIT 100', (err, rows: any[]) => {
      if (!err && rows) rows.forEach(r => monitoredMints.add(r.coinMint));
    });
  }
});

db.serialize(() => {
  db.run('PRAGMA journal_mode = WAL');
  db.run(`
    CREATE TABLE IF NOT EXISTS tokens (
      coinMint TEXT PRIMARY KEY,
      dev TEXT,
      name TEXT,
      ticker TEXT,
      imageUrl TEXT,
      creationTime INTEGER,
      numHolders INTEGER DEFAULT 0,
      description TEXT,
      marketCap REAL DEFAULT 0,
      volume REAL DEFAULT 0,
      currentMarketPrice REAL DEFAULT 0,
      bondingCurveProgress REAL DEFAULT 0,
      sniperCount INTEGER DEFAULT 0,
      graduationDate INTEGER,
      allTimeHighMarketCap REAL DEFAULT 0,
      poolAddress TEXT,
      twitter TEXT,
      telegram TEXT,
      website TEXT,
      hasTwitter INTEGER DEFAULT 0,
      hasTelegram INTEGER DEFAULT 0,
      hasWebsite INTEGER DEFAULT 0,
      hasSocial INTEGER DEFAULT 0,
      devHoldingsPercentage REAL DEFAULT 0,
      buyTransactions INTEGER DEFAULT 0,
      sellTransactions INTEGER DEFAULT 0,
      transactions INTEGER DEFAULT 0,
      sniperOwnedPercentage REAL DEFAULT 0,
      top10holderspercentage REAL DEFAULT 0,
      tokenProgram TEXT DEFAULT "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      program TEXT DEFAULT "pump",
      platform TEXT DEFAULT "pump",
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      -- Internal fields
      uri TEXT,
      signature TEXT,
      block_time_iso TEXT,
      virtual_sol REAL,
      virtual_token REAL,
      real_sol REAL,
      real_token REAL,
      is_complete INTEGER DEFAULT 0,
      sniperWallets TEXT DEFAULT "[]",
      insiderWallets TEXT DEFAULT "[]",
      sniperTotalBalance REAL DEFAULT 0,
      insiderTotalBalance REAL DEFAULT 0,
      insiderOwnedPercentage REAL DEFAULT 0,
      lastSignature TEXT,
      graduation_status TEXT DEFAULT "new",
      graduation_timestamp INTEGER
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS price_history (
      coinMint TEXT,
      price REAL,
      volume REAL DEFAULT 0,
      timestamp INTEGER
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS idx_price_mint_time ON price_history(coinMint, timestamp)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_tokens_creation ON tokens(creationTime)`);
  db.run(`ALTER TABLE price_history ADD COLUMN volume REAL DEFAULT 0`, (err) => { });

  // Migration for sniperWallets and lastSignature if needed
  db.run(`ALTER TABLE tokens ADD COLUMN insiderWallets TEXT DEFAULT "[]"`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN sniperTotalBalance REAL DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN insiderTotalBalance REAL DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN insiderOwnedPercentage REAL DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN lastSignature TEXT`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN graduation_status TEXT DEFAULT "new"`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN hasFileMetaData INTEGER DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN graduation_timestamp INTEGER`, (err) => { });
  db.run(`ALTER TABLE tokens RENAME COLUMN topHoldersPercentage TO top10holderspercentage`, (err) => { });

  // Dev Migration Stats Table
  db.run(`
    CREATE TABLE IF NOT EXISTS dev_stats (
      creator TEXT PRIMARY KEY,
      total_launched INTEGER DEFAULT 0,
      total_migrated INTEGER DEFAULT 0,
      last_full_scan INTEGER,
      last_updated INTEGER
    )
  `);
  db.run(`ALTER TABLE dev_stats ADD COLUMN last_full_scan INTEGER`, (err) => { });


});

// App & Socket.io Broadcasting
const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Socket.io Production-Grade Room System
io.on('connection', (socket) => {
  console.log(`[SOCKET] Client ${socket.id} connected`);

  // 1. Join General Rooms (Overview)
  socket.on('join', (room: string) => {
    if (typeof room === 'string') {
      socket.join(room);
      console.log(`[SOCKET] Client joined general room: ${room}`);
    }
  });

  // 2. Subscribe to specific token (Detailed updates)
  socket.on('subscribe-token', (mint: string) => {
    if (!mint) return;
    socket.join(`token:${mint}`);
    socket.join(`fees:${mint}`);
    socket.join(`price-by-token:${mint}`);
    socket.join(`chart-data:${mint}`);
    socket.join(`holders:${mint}`);
    socket.join(`snipers:${mint}`);
    socket.join(`insiders:${mint}`);
    socket.join(`dex:${mint}`);
    console.log(`[SOCKET] Subscribed to token rooms for mint: ${mint}`);
  });

  // 3. Subscribe to dev stats for a wallet
  socket.on('subscribe-dev', async (wallet: string) => {
    if (!wallet) return;
    socket.join(`dev-stats:${wallet}`);
    console.log(`[SOCKET] Subscribed to dev-stats for wallet: ${wallet}`);

    // Send initial stats on join
    const stats = await getFullDevStats(wallet);
    socket.emit('message', { type: 'dev-stats', wallet, data: stats });
  });

  // 4. Subscribe to a specific wallet's transactions
  socket.on('subscribe-wallet', (wallet: string) => {
    if (!wallet) return;
    socket.join(`wallet:${wallet}`);
    console.log(`[SOCKET] Client subscribed to wallet: ${wallet}`);
  });

  // 5. Subscribe to a specific pool room
  socket.on('subscribe-pool', (pool: string) => {
    if (!pool) return;
    socket.join(`pool:${pool}`);
    socket.join(`price-by-pool:${pool}`);
    console.log(`[SOCKET] Client subscribed to pool: ${pool}`);
  });

  socket.on('disconnect', () => {
    console.log(`[SOCKET] Client ${socket.id} disconnected`);
  });
});

// Core Broadcast Functions
function broadcastNewToken(tokenData: any) {
  const payload = { type: 'new-token', data: tokenData };
  io.to('latest-tokens').emit('message', payload);
  if (tokenData.bondingCurveProgress >= 80) {
    io.to('graduating').emit('message', payload);
  }
}

function broadcastGraduating(tokenData: any) {
  io.to('graduating').emit('message', { type: 'graduating-update', data: tokenData });
}

function broadcastGraduated(tokenData: any) {
  io.to('graduated').emit('message', { type: 'graduated-update', data: tokenData });
}

function broadcastPriceUpdate(tokenMint: string, priceUpdate: any, poolAddress?: string) {
  io.to(`price-by-token:${tokenMint}`).emit('message', { type: 'price-update', mint: tokenMint, data: priceUpdate });
  if (poolAddress) {
    io.to(`price-by-pool:${poolAddress}`).emit('message', { type: 'price-update', pool: poolAddress, data: priceUpdate });
  }
  // Also send to general room for overview table compatibility if needed
  io.to('latest-tokens').emit('message', { type: 'price-update', mint: tokenMint, data: priceUpdate });
}

function broadcastChartData(tokenMint: string, candleData: any, poolAddress?: string) {
  io.to(`chart-data:${tokenMint}`).emit('message', { type: 'chart-update', mint: tokenMint, data: candleData });
  if (poolAddress) {
    io.to(`chart-data:${poolAddress}`).emit('message', { type: 'chart-update', pool: poolAddress, data: candleData });
  }
}

function broadcastTransaction(txData: any, poolAddress?: string) {
  io.to(`token:${txData.mint}`).emit('message', { type: 'transaction', data: txData });
  if (poolAddress) {
    io.to(`pool:${poolAddress}`).emit('message', { type: 'transaction', data: txData });
  }
}

function broadcastHolders(tokenMint: string, holdersData: any) {
  io.to(`holders:${tokenMint}`).emit('message', { type: 'holders-update', mint: tokenMint, data: holdersData });
}

function broadcastSniperUpdate(tokenMint: string, sniperData: any) {
  io.to(`snipers:${tokenMint}`).emit('message', { type: 'sniper-update', mint: tokenMint, data: sniperData });
}

function broadcastInsiderUpdate(tokenMint: string, insiderData: any) {
  io.to(`insiders:${tokenMint}`).emit('message', { type: 'insider-update', mint: tokenMint, data: insiderData });
}

function broadcastDevHoldings(tokenMint: string, devData: any) {
  io.to(`token:${tokenMint}`).emit('message', { type: 'dev-holdings', mint: tokenMint, data: devData });
}

function broadcastTop10(tokenMint: string, top10Data: any) {
  io.to(`holders:${tokenMint}`).emit('message', { type: 'top10-update', mint: tokenMint, data: top10Data });
}

function broadcastWalletTransactions(wallet: string, txData: any) {
  // Use generic wallet room
  io.to(`wallet:${wallet}`).emit('message', { type: 'wallet-tx', wallet, data: txData });
}

function broadcastStatistics(stats: any) {
  io.to('token-statistics-total').emit('message', { type: 'global-stats', data: stats });
}

function broadcastDevUpdate(creator: string, updatedStats: any) {
  io.to(`dev-stats:${creator}`).emit('message', {
    type: 'dev-stats',
    wallet: creator,
    data: updatedStats
  });
}

function broadcastFees(mint: string, feeData: any) {
  io.to(`fees:${mint}`).emit('message', { type: 'fee-update', mint, data: feeData });
}

/**
 * DEX Data Feature - Ready for integration
 * This function broadcasts DEX Paid status and Boost counts to the dexroom and token-specific dex rooms.
 */
function broadcastDexInfo(mint: string, dexInfo: { dex_paid: boolean; active_boosts: number; banner_url?: string }) {
  const payload = {
    type: 'dex_info',
    mint,
    dex_paid: dexInfo.dex_paid,
    active_boosts: dexInfo.active_boosts,
    banner_url: dexInfo.banner_url || null,
    timestamp: Date.now(),
    // bonus: show golden badge if >=500
    show_golden: (dexInfo.active_boosts || 0) >= 500
  };
  io.to(`dex:${mint}`).emit('message', payload);
  io.to('dexroom').emit('message', payload);
}

export async function getFullDevStats(creator: string): Promise<{
  total_launched: number;
  total_migrated: number;
}> {
  console.log(`[DevStats] Pulsing live on-chain data for creator: ${creator}`);

  let launched = 0;
  let migrated = 0;

  try {
    const signatures = await rpcQueue.add((c) => c.getSignaturesForAddress(
      new PublicKey(creator),
      { limit: 1000 }
    ));

    for (const sigInfo of signatures) {
      try {
        const tx = await rpcQueue.add((c) => c.getParsedTransaction(sigInfo.signature, {
          maxSupportedTransactionVersion: 0,
          commitment: 'confirmed'
        }));

        if (!tx || !tx.meta || !tx.meta.logMessages) continue;

        const logs = tx.meta.logMessages;
        const isCreate = logs.some(l =>
          l.includes('Instruction: Create') ||
          l.includes('Instruction: CreateConfig') ||
          l.includes('initialize_virtual_pool')
        );

        const isComplete = logs.some(l =>
          l.includes('PoolIsCompleted') ||
          l.includes('MigrationDammV2') ||
          l.includes('graduated')
        );

        if (isCreate) launched++;
        if (isComplete) migrated++;
      } catch (innerErr) { }
    }
  } catch (e: any) {
    console.error(`[DevStats] Live Scan Error: ${e.message}`);
  }

  return { total_launched: launched, total_migrated: migrated };
}

async function broadcast(payload: any) {
  if (!payload || !payload.name || payload.name === 'Unknown' || payload.name === 'Pending Metadata' || payload.name === '?' || payload.name === '') {
    return; // 🛡️ Zero Tolerance for Placeholders
  }
  try {
    if (cachedSolPrice === 0) await getSolPrice();
    const fullPayload = await formatFullPayload(payload, cachedSolPrice);

    // Always broadcast to latest tokens
    broadcastNewToken(fullPayload);

    // Graduation status broadcasts
    if (payload.graduation_status === 'imminent') {
      broadcastGraduating(fullPayload);
    } else if (payload.graduation_status === 'graduated') {
      broadcastGraduated(fullPayload);
    }

    // Update aggregate stats
    const stats = {
      totalTokens: monitoredMints.size,
      solPrice: cachedSolPrice,
      timestamp: Date.now()
    };
    broadcastStatistics(stats);

  } catch (err) {
    console.error("❌ Broadcast Error:", err);
  }
}

async function getPriceChange(mint: string, seconds: number, currentPrice: number, launchPrice?: number): Promise<number> {
  return new Promise((resolve) => {
    const targetTime = Date.now() - (seconds * 1000);
    db.get('SELECT price FROM price_history WHERE coinMint = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1',
      [mint, targetTime], (err, row: any) => {
        if (!err && row && row.price > 0) {
          resolve(((currentPrice - row.price) / row.price) * 100);
        } else if (launchPrice && launchPrice > 0) {
          // If no history, assume change since launch
          resolve(((currentPrice - launchPrice) / launchPrice) * 100);
        } else {
          resolve(0);
        }
      });
  });
}

const tradeQueues: Record<string, Promise<void>> = {};

async function enqueueTrade(mint: string, trade: any, signature?: string) {
  if (!tradeQueues[mint]) tradeQueues[mint] = Promise.resolve();
  tradeQueues[mint] = tradeQueues[mint].then(() => handleTrade(trade, signature));
  return tradeQueues[mint];
}

async function formatFullPayload(row: any, solPrice: number) {
  const liquiditySol = row.real_sol || 0;

  // Fetch Dev Stats
  const devStats: any = await new Promise((resolve) => {
    db.get('SELECT total_launched, total_migrated FROM dev_stats WHERE creator = ?', [row.dev], (err, stats: any) => {
      if (err || !stats) resolve({ total_launched: 0, total_migrated: 0 });
      else resolve(stats);
    });
  });


  // Fetch price history for OHLCV
  const priceHistory: any[] = await new Promise((resolve) => {
    // Get last 200 ticks to ensure enough data for various candle intervals
    db.all('SELECT price, volume, timestamp FROM price_history WHERE coinMint = ? ORDER BY timestamp DESC LIMIT 200',
      [row.coinMint], (err, rows: any[]) => {
        if (err || !rows) resolve([]);
        else resolve(rows.map(r => ({
          price_usd: r.price,
          volume_sol: r.volume,
          timestamp: r.timestamp
        })).reverse());
      });
  });

  const sniperWallets = JSON.parse(row.sniperWallets || '[]');
  const priceUsd = row.currentMarketPrice || 0;
  const launchPrice = (30 / 1073000000) * solPrice;

  // Parallelize price change lookups
  const [p1m, p5m, p15m, p30m, p1h, p4h, p24h] = await Promise.all([
    getPriceChange(row.coinMint, 60, priceUsd, launchPrice),
    getPriceChange(row.coinMint, 300, priceUsd, launchPrice),
    getPriceChange(row.coinMint, 900, priceUsd, launchPrice),
    getPriceChange(row.coinMint, 1800, priceUsd, launchPrice),
    getPriceChange(row.coinMint, 3600, priceUsd, launchPrice),
    getPriceChange(row.coinMint, 14400, priceUsd, launchPrice),
    getPriceChange(row.coinMint, 86400, priceUsd, launchPrice)
  ]);

  const events = {
    "1m": { priceChangePercentage: p1m },
    "5m": { priceChangePercentage: p5m },
    "15m": { priceChangePercentage: p15m },
    "30m": { priceChangePercentage: p30m },
    "1h": { priceChangePercentage: p1h },
    "4h": { priceChangePercentage: p4h },
    "24h": { priceChangePercentage: p24h }
  };

  // console.log(`[Broadcast DEBUG] Price history length for ${row.coinMint.slice(0, 8)}: ${priceHistory.length}`);

  return {
    type: "message",
    room: "New",
    data: {
      token: {
        name: row.name,
        symbol: row.ticker,
        mint: row.coinMint,
        uri: row.uri || "",
        decimals: 6,
        description: row.description || "",
        image: row.imageUrl || "",
        hasFileMetaData: true,
        createdOn: row.platform === 'moonshot' ? "https://moonshot.cc" :
          row.platform === 'letsbonk' ? "https://letsbonk.fun" :
            "https://pump.fun",
        strictSocials: {
          twitter: row.twitter || "",
          telegram: row.telegram || "",
          website: row.website || ""
        },
        creation: {
          creator: row.dev,
          created_tx: row.signature,
          created_time: Math.floor(row.creationTime / 1000)
        }
      },
      pools: [
        {
          poolId: row.poolAddress || "",
          liquidity: {
            quote: liquiditySol,
            usd: liquiditySol * solPrice
          },
          price: {
            quote: priceUsd / solPrice,
            usd: priceUsd
          },
          tokenSupply: 1000000000000000,
          lpBurn: 0,
          tokenAddress: row.coinMint,
          marketCap: {
            quote: row.marketCap / solPrice,
            usd: row.marketCap
          },
          decimals: 6,
          security: {
            freezeAuthority: null,
            mintAuthority: null
          },
          quoteToken: "So11111111111111111111111111111111111111112",
          market: row.platform || "pump",
          deployer: row.dev,
          lastUpdated: Date.now(),
          createdAt: row.creationTime,
          txns: {
            buys: row.buyTransactions || 0,
            sells: row.sellTransactions || 0,
            total: row.transactions || 0,
            volume: row.volume * solPrice,
            volume24h: 0
          },
          curvePercentage: row.bondingCurveProgress || 0
        }
      ],
      events,
      risk: {
        snipers: {
          count: row.sniperCount || 0,
          totalBalance: row.sniperTotalBalance || 0,
          totalPercentage: row.sniperOwnedPercentage || 0,
          wallets: sniperWallets
        },
        insiders: {
          count: JSON.parse(row.insiderWallets || '[]').length,
          totalBalance: row.insiderTotalBalance || 0,
          totalPercentage: row.insiderOwnedPercentage || 0,
          wallets: JSON.parse(row.insiderWallets || '[]')
        },
        top10: row.top10holderspercentage || 0,
        dev: {
          percentage: row.devHoldingsPercentage || 0,
          amount: (row.devHoldingsPercentage / 100) * 1000000000,
          stats: devStats
        }
      },
      graduation: {
        status: row.graduation_status || "new",
        timestamp: row.graduation_timestamp || null
      },
      dev_stats: {
        total_launched: devStats.total_launched,
        total_migrated: devStats.total_migrated
      },
      priceHistory
    }
  };
}




// API Route to fetch initial tokens
app.get('/api/tokens', async (req, res) => {
  console.log(`[API] GET /api/tokens - limit: ${req.query.limit}`);
  const limit = Math.min(Number(req.query.limit) || 50, 100);

  if (cachedSolPrice === 0) await getSolPrice();

  db.all('SELECT * FROM tokens ORDER BY creationTime DESC LIMIT ?', [limit], async (err, rows: any[]) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: 'Database error' });
    }

    try {
      const payloads = await Promise.all(rows.map(row => formatFullPayload(row, cachedSolPrice)));
      res.json(payloads);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: 'Formatting error' });
    }
  });
});

// New /api/mints endpoint with filtering
app.get('/api/mints', async (req, res) => {
  try {
    const { launchpad, limit = 50, offset = 0, sort = 'desc' } = req.query;
    const queryParams: any[] = [];
    let sql = `SELECT * FROM tokens WHERE 1=1`;

    if (launchpad && launchpad !== 'all') {
      sql += ` AND platform = ?`;
      queryParams.push(launchpad);
    }

    sql += ` ORDER BY creationTime ${sort === 'asc' ? 'ASC' : 'DESC'}`;
    sql += ` LIMIT ? OFFSET ?`;
    queryParams.push(Number(limit), Number(offset));

    if (cachedSolPrice === 0) await getSolPrice();

    db.all(sql, queryParams, async (err, rows: any[]) => {
      if (err) {
        console.error(err);
        return res.status(500).json({ error: 'Database error' });
      }

      try {
        const payloads = await Promise.all(rows.map(row => formatFullPayload(row, cachedSolPrice)));
        res.json({
          status: 'success',
          data: payloads,
          total: rows.length,
          filters: { launchpad, limit, offset, sort }
        });
      } catch (e: any) {
        console.error(e);
        res.status(500).json({ status: 'error', message: e.message });
      }
    });
  } catch (e: any) {
    console.error('Error in /api/mints:', e);
    res.status(500).json({ status: 'error', message: e.message });
  }
});
// Test Route
app.get('/api/test/create-letsbonk', (req, res) => {
  const fakeToken = {
    coinMint: "BonkTest" + Date.now(),
    name: "LetsBonk Test",
    ticker: "BONKTEST",
    imageUrl: "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263/logo.png",
    marketCap: 15000,
    volume: 100,
    bondingCurveProgress: 25,
    platform: 'letsbonk',
    creationTime: Date.now() / 1000,
    hasTwitter: 1,
    hasTelegram: 1,
    hasWebsite: 1,
    devHoldingsPercentage: 2.5,
    sniperOwnedPercentage: 5.0,
    top10holderspercentage: 15.0
  };

  // Save to DB (mock) or just broadcast
  // Ideally save to DB so history works
  db.run(`INSERT INTO tokens (coinMint, name, ticker, imageUrl, marketCap, volume, bondingCurveProgress, platform, creationTime, hasTwitter, hasTelegram, hasWebsite, devHoldingsPercentage, sniperOwnedPercentage, top10holderspercentage) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [fakeToken.coinMint, fakeToken.name, fakeToken.ticker, fakeToken.imageUrl, fakeToken.marketCap, fakeToken.volume, fakeToken.bondingCurveProgress, fakeToken.platform, fakeToken.creationTime, 1, 1, 1, 2.5, 5.0, 15.0],
    (err) => {
      if (err) return res.status(500).json({ error: err.message });
      broadcast(fakeToken);
      res.json({ success: true, mint: fakeToken.coinMint });
    }
  );
});

// API Endpoints
app.get('/', async (req: Request, res: Response) => {
  const solPrice = await getSolPrice();
  const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>PumpFun Tracker Dashboard</title>
      <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&family=JetBrains+Mono&display=swap" rel="stylesheet">
      <style>
        :root {
          --bg: #09090b;
          --card-bg: #141416;
          --accent: #22c55e;
          --text: #fafafa;
          --text-dim: #a1a1aa;
          --border: #27272a;
        }
        body {
          background: var(--bg);
          color: var(--text);
          font-family: 'Outfit', sans-serif;
          margin: 0;
          display: flex;
          align-items: center;
          justify-content: center;
          min-height: 100vh;
        }
        .dashboard {
          background: var(--card-bg);
          border: 1px solid var(--border);
          border-radius: 24px;
          padding: 40px;
          width: 90%;
          max-width: 500px;
          box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        }
        .header {
          display: flex;
          align-items: center;
          gap: 12px;
          margin-bottom: 32px;
        }
        .status-dot {
          width: 12px;
          height: 12px;
          background: var(--accent);
          border-radius: 50%;
          box-shadow: 0 0 12px var(--accent);
          animation: pulse 2s infinite;
        }
        h1 { margin: 0; font-size: 24px; font-weight: 600; letter-spacing: -0.5px; }
        .stats-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 16px;
          margin-bottom: 32px;
        }
        .stat-card {
          background: rgba(255,255,255,0.02);
          border: 1px solid var(--border);
          padding: 20px;
          border-radius: 16px;
        }
        .stat-label { font-size: 12px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
        .stat-value { font-family: 'JetBrains Mono', monospace; font-size: 20px; font-weight: 600; color: var(--accent); }
        .endpoints {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }
        .endpoint-link {
          display: flex;
          justify-content: space-between;
          padding: 14px 20px;
          background: rgba(255,255,255,0.03);
          border: 1px solid var(--border);
          border-radius: 12px;
          color: var(--text-dim);
          text-decoration: none;
          transition: all 0.2s;
          font-family: 'JetBrains Mono', monospace;
          font-size: 13px;
        }
        .endpoint-link:hover {
          border-color: var(--accent);
          color: var(--text);
          background: rgba(34, 197, 94, 0.05);
        }
        @keyframes pulse {
          0% { transform: scale(1); opacity: 1; }
          50% { transform: scale(1.2); opacity: 0.7; }
          100% { transform: scale(1); opacity: 1; }
        }
      </style>
    </head>
    <body>
      <div class="dashboard">
        <div class="header">
          <div class="status-dot"></div>
          <h1>System Operational</h1>
        </div>
        <div class="stats-grid">
          <div class="stat-card">
            <div class="stat-label">SOL Price</div>
            <div class="stat-value">$${solPrice.toFixed(2)}</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">Monitored</div>
            <div class="stat-value">${monitoredMints.size}</div>
          </div>
        </div>
        <div class="endpoints">
          <a href="/status" class="endpoint-link"><span>/status</span> <span>→</span></a>
          <a href="/tokens" class="endpoint-link"><span>/tokens</span> <span>→</span></a>
          <a href="/imminent-tokens" class="endpoint-link"><span>/imminent</span> <span>→</span></a>
          <a href="/imminent" class="endpoint-link"><span>/imminent-dashboard</span> <span>→</span></a>
        </div>
      </div>
    </body>
    </html>
  `;
  res.send(html);
});

app.get('/imminent', async (req: Request, res: Response) => {
  const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Imminent Graduation | PumpFun Engine</title>
      <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&family=JetBrains+Mono&display=swap" rel="stylesheet">
      <style>
        :root {
          --bg: #09090b;
          --card-bg: #141416;
          --accent: #22c55e;
          --warning: #f59e0b;
          --text: #fafafa;
          --text-dim: #a1a1aa;
          --border: #27272a;
        }
        body {
          background: var(--bg);
          color: var(--text);
          font-family: 'Outfit', sans-serif;
          margin: 0;
          padding: 40px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 40px; }
        h1 { margin: 0; font-size: 32px; font-weight: 600; letter-spacing: -1px; }
        .badge { background: rgba(34, 197, 94, 0.1); color: var(--accent); padding: 6px 12px; border-radius: 99px; font-size: 13px; font-weight: 600; }
        
        table { width: 100%; border-collapse: collapse; margin-top: 20px; background: var(--card-bg); border-radius: 16px; overflow: hidden; border: 1px solid var(--border); }
        th { text-align: left; padding: 20px; font-size: 13px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; border-bottom: 1px solid var(--border); }
        td { padding: 20px; border-bottom: 1px solid var(--border); font-family: 'JetBrains Mono', monospace; font-size: 14px; }
        tr:last-child td { border-bottom: none; }
        
        .token-cell { display: flex; align-items: center; gap: 12px; }
        .token-icon { width: 32px; height: 32px; border-radius: 50%; background: #27272a; }
        .token-info { display: flex; flex-direction: column; }
        .token-name { font-weight: 600; font-family: 'Outfit', sans-serif; }
        .token-symbol { font-size: 12px; color: var(--text-dim); }
        
        .progress-bar-container { width: 100px; height: 6px; background: rgba(255,255,255,0.05); border-radius: 3px; overflow: hidden; }
        .progress-bar-fill { height: 100%; background: var(--accent); transition: width 0.3s ease; }
        .imminent { color: var(--warning); }
        .graduated { color: var(--accent); }
        
        .metric { display: flex; flex-direction: column; gap: 4px; }
        .metric-val { font-weight: 600; }
        .metric-label { font-size: 11px; color: var(--text-dim); }

        #imminent-list tr { transition: background 0.2s; }
        #imminent-list tr:hover { background: rgba(255,255,255,0.02); }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <div>
            <h1>Imminent Graduation</h1>
            <p style="color: var(--text-dim); margin-top: 8px;">Tokens nearing the bonding curve threshold (80%+)</p>
          </div>
          <div id="connection-status" class="badge">📡 Live Feed Connecting...</div>
        </div>

        <table>
          <thead>
            <tr>
              <th>Token</th>
              <th>Progress</th>
              <th>Market Cap</th>
              <th>Volume</th>
              <th>Risk Scan</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="imminent-list">
            <!-- Live updates will inject here -->
          </tbody>
        </table>
      </div>

      <script src="/socket.io/socket.io.js"></script>
      <script>
        const socket = io();
        const imminentList = document.getElementById('imminent-list');
        const statusBadge = document.getElementById('connection-status');
        const tokens = new Map();

        socket.on('connect', () => {
          socket.emit('join', 'graduating');
          socket.emit('join', 'latest-tokens');
          statusBadge.textContent = '🟢 Live Connection Active';
        });

        socket.on('disconnect', () => {
          statusBadge.textContent = '🔴 Connection Lost';
        });

        socket.on('message', (payload) => {
          if (payload.type === 'graduating' || payload.type === 'new-token') {
            const data = payload.data.data || payload.data;
            updateRow(data);
          }
        });

        function updateRow(item) {
          const token = item.token || item;
          const pool = item.pools ? item.pools[0] : item;
          const risk = item.risk || {};
          
          const rowId = 'row-' + (token.mint || item.coinMint);
          let row = document.getElementById(rowId);
          
          const progress = pool.curvePercentage || item.bondingCurveProgress || 0;
          const mc = pool.marketCap?.usd || item.marketCap || 0;
          const vol = pool.txns?.volume || item.volume || 0;
          const sniperPct = risk.snipers?.totalPercentage || item.sniperOwnedPercentage || 0;
          const devPct = risk.dev?.percentage || item.devHoldingsPercentage || 0;
          const top10 = risk.top10 || item.top10holderspercentage || 0;
          
          const html = '<td>' +
              '<div class="token-cell">' +
                '<img class="token-icon" src="' + (token.image || '') + '" onerror="this.style.opacity=0">' +
                '<div class="token-info">' +
                  '<span class="token-name">' + token.name + '</span>' +
                  '<span class="token-symbol">' + token.symbol + '</span>' +
                '</div>' +
              '</div>' +
            '</td>' +
            '<td>' +
              '<div class="metric">' +
                '<span class="metric-val">' + progress.toFixed(1) + '%</span>' +
                '<div class="progress-bar-container">' +
                  '<div class="progress-bar-fill" style="width: ' + progress + '%"></div>' +
                '</div>' +
              '</div>' +
            '</td>' +
            '<td>$' + (mc / 1000).toFixed(1) + 'k</td>' +
            '<td>$' + vol.toFixed(2) + ' SOL</td>' +
            '<td>' +
              '<div class="metric">' +
                '<span class="metric-val" style="color: ' + (sniperPct > 10 ? '#ef4444' : 'inherit') + '">' + sniperPct.toFixed(1) + '% Sniper</span>' +
                '<span class="metric-label">' + top10.toFixed(1) + '% Top 10 | ' + devPct.toFixed(1) + '% Dev</span>' +
              '</div>' +
            '</td>' +
            '<td class="' + (item.graduation?.status === 'graduated' ? 'graduated' : 'imminent') + '">' +
              (item.graduation?.status || 'imminent').toUpperCase() +
            '</td>';

          if (!row) {
            row = document.createElement('tr');
            row.id = rowId;
            imminentList.prepend(row);
          }
          row.innerHTML = html;

          // Simple sort
          const rows = Array.from(imminentList.querySelectorAll('tr'));
          rows.sort((a, b) => {
            const pA = parseFloat(a.querySelector('.metric-val')?.textContent || '0');
            const pB = parseFloat(b.querySelector('.metric-val')?.textContent || '0');
            return pB - pA;
          });
          rows.forEach(r => imminentList.appendChild(r));
        }
      </script>
    </body>
    </html>
  `;
  res.send(html);
});

app.get('/status', async (req: Request, res: Response) => {
  try {
    const slot = await rpcQueue.add((c) => c.getSlot()).catch(() => 0);
    res.json({ ok: true, slot, connections: io.sockets.sockets.size, monitored: monitoredMints.size });
  } catch (e) {
    res.json({ ok: false, error: (e as Error).message });
  }
});


app.get('/tokens', async (req: Request, res: Response) => {
  // Use /api/tokens instead
  res.redirect('/api/tokens');
});


app.get('/imminent-tokens', async (req: Request, res: Response) => {
  const solPrice = await getSolPrice();
  db.all('SELECT * FROM tokens WHERE graduation_status = "imminent" AND is_complete = 0 ORDER BY bondingCurveProgress DESC LIMIT 50', async (err: any, rows: any[]) => {
    if (err) return res.status(500).json({ error: err.message });
    const tasks = rows.map(r => formatFullPayload(r, solPrice));
    res.json(await Promise.all(tasks));
  });
});

app.get('/pump-payload', async (req: Request, res: Response) => {
  const mint = req.query.mint as string;
  if (!mint) return res.status(400).json({ error: 'Missing mint' });
  const solPrice = await getSolPrice();
  db.get('SELECT * FROM tokens WHERE coinMint = ?', [mint], async (err: any, row: any) => {
    if (!row) return res.status(404).json({ error: 'Token not found' });
    res.json(await formatFullPayload(row, solPrice));
  });
});

// Helpers
let cachedSolPrice = 0;
let lastSolPriceFetch = 0;
async function getSolPrice(): Promise<number> {
  if (Date.now() - lastSolPriceFetch < 30000 && cachedSolPrice > 0) return cachedSolPrice;

  const sources = [
    {
      url: 'https://frontend-api-v3.pump.fun/sol-price',
      parse: (json: any) => json?.solPrice ? Number(json.solPrice) : null
    },
    {
      url: 'https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112',
      parse: (json: any) => json?.data?.['So11111111111111111111111111111111111111112']?.price ? Number(json.data['So11111111111111111111111111111111111111112'].price) : null
    },
    {
      url: 'https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT',
      parse: (json: any) => json?.price ? Number(json.price) : null
    }
  ];

  for (const source of sources) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 8000);

      const res = await fetch(source.url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'application/json',
          'Cache-Control': 'no-cache'
        },
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (res.ok) {
        const json = await res.json() as any;
        const price = source.parse(json);
        if (price) {
          cachedSolPrice = price;
          lastSolPriceFetch = Date.now();
          return cachedSolPrice;
        }
      }
    } catch (e) {
      // Continue to next source
    }
  }

  console.error('⚠️ All SOL Price sources failed, using cache.');
  return cachedSolPrice;
}

async function getBondingCurveInfo(mint: string, platform: string = 'pump', poolAddress?: string) {
  try {
    const mintKey = new PublicKey(mint);
    let pda: PublicKey;

    if (poolAddress && poolAddress !== 'unknown') {
      pda = new PublicKey(poolAddress);
    } else if (platform === 'pump') {
      [pda] = PublicKey.findProgramAddressSync([Buffer.from('bonding-curve'), mintKey.toBuffer()], PUMP_PROGRAM_ID);
    } else if (platform === 'moonshot') {
      [pda] = PublicKey.findProgramAddressSync([Buffer.from('token'), mintKey.toBuffer()], MOONSHOT_PROGRAM_ID);
    } else {
      return null;
    }

    const info = await rpcQueue.add((c) => c.getAccountInfo(pda)).catch(() => null);
    if (!info || !info.data || info.data.length < 40) return null;

    // Standard Layout (Pump/Moonshot use similar for virtual reserves)
    const data = info.data.slice(8);
    const vToken = Number(data.readBigUInt64LE(0)) / 1e6;
    const vSol = Number(data.readBigUInt64LE(8)) / 1e9;
    const rToken = Number(data.readBigUInt64LE(16)) / 1e6;
    const rSol = Number(data.readBigUInt64LE(24)) / 1e9;
    const isComplete = data.length > 40 ? data.readUInt8(40) === 1 : false;

    const priceQuote = vSol / vToken;
    const solPrice = await getSolPrice();
    const priceUsd = priceQuote * solPrice;

    // Targets (Mainnet standards)
    let targetSol = 85;
    if (platform === 'moonshot') targetSol = 100;
    if (platform === 'bags') targetSol = 50; // Heuristic for BAGS

    // Virtual Progress: (Real SOL / Target SOL) * 100
    // Pump starts with 30 SOL liquidity usually
    const currentSol = platform === 'pump' ? (vSol - 30) : rSol;
    const curvePercentage = Math.min(100, (currentSol / targetSol) * 100);

    return {
      pda: pda.toString(),
      vSol, vToken, rSol, rToken,
      isComplete, priceQuote, priceUsd, curvePercentage, solPrice
    };
  } catch (e) { return null; }
}

export async function checkFundingSource(wallet: string, creator: string): Promise<boolean> {
  if (!wallet || !creator || wallet === creator) return false;
  try {
    const signatures = await rpcQueue.add((c) => c.getSignaturesForAddress(new PublicKey(wallet), { limit: 10 }));
    if (!signatures || signatures.length === 0) return false;

    // Look for a transfer from creator to this wallet in their recent history
    for (const sig of signatures) {
      const tx = await rpcQueue.add((c) => c.getParsedTransaction(sig.signature, { maxSupportedTransactionVersion: 0, commitment: 'confirmed' }));
      if (tx && tx.meta) {
        const accountKeys = tx.transaction.message.accountKeys.map(k => k.pubkey.toBase58());
        const creatorIndex = accountKeys.indexOf(creator);
        const walletIndex = accountKeys.indexOf(wallet);

        if (creatorIndex !== -1 && walletIndex !== -1) {
          // Check for net gain in wallet from creator's pre/post balances
          const creatorDiff = (tx.meta.postBalances[creatorIndex] - tx.meta.preBalances[creatorIndex]) / 1e9;
          const walletDiff = (tx.meta.postBalances[walletIndex] - tx.meta.preBalances[walletIndex]) / 1e9;
          if (creatorDiff < 0 && walletDiff > 0) {
            console.log(`🔗 Detected funding connection: Dev (${creator.slice(0, 5)}) -> Wallet (${wallet.slice(0, 5)})`);
            return true;
          }
        }
      }
    }
  } catch (e) { }
  return false;
}

export async function detectInsiders(mint: string, creator: string = 'unknown', launchTx: string = 'unknown', pool: string = 'unknown'): Promise<{
  insiders: string[];
  snipers: string[];
}> {
  const insiders: Set<string> = new Set();
  const snipers: Set<string> = new Set();
  const earlyBuyers: Set<string> = new Set();

  try {
    const sigs = await rpcQueue.add((c) => c.getSignaturesForAddress(new PublicKey(mint), { limit: 20 }));
    if (!sigs) return { insiders: [], snipers: [] };

    for (const sigInfo of sigs) {
      const tx = await rpcQueue.add((c) => c.getParsedTransaction(sigInfo.signature, { maxSupportedTransactionVersion: 0, commitment: 'confirmed' }));
      if (!tx || !tx.meta) continue;

      const accs = tx.transaction.message.accountKeys.map(k => k.pubkey.toBase58());
      for (let i = 0; i < accs.length; i++) {
        const addr = accs[i];
        if (addr === creator || addr === mint || addr === pool || addr === PUMP_PROGRAM_ID.toBase58()) continue;

        // Buying if post balance > pre balance
        if ((tx.meta.postBalances[i] - tx.meta.preBalances[i]) > 0) {
          // Check if this wallet is another PDA or program (skip)
          if (tx.transaction.message.accountKeys[i].signer) {
            earlyBuyers.add(addr);
            // If they are in the launch tx itself, they are immediate insiders
            if (sigInfo.signature === launchTx) insiders.add(addr);
          }
        }
      }
    }

    // Secondary Cross-Reference: Funding Source (Deep Insider Detection)
    for (const wallet of earlyBuyers) {
      const isFundedByDev = await checkFundingSource(wallet, creator);
      if (isFundedByDev) {
        insiders.add(wallet);
      } else {
        snipers.add(wallet); // Early but not funded (likely a bot)
      }
    }

  } catch (e) { }

  return {
    insiders: Array.from(insiders),
    snipers: Array.from(snipers)
  };
}

function parseMetaplexMetadata(data: Buffer, isInstruction = false) {
  try {
    // Borsh layout:
    // Account: 1 (key) + 32 (auth) + 32 (mint) = 65
    // Instruction: 1 (disc) = 1
    let offset = isInstruction ? 1 : 65;

    if (data.length < offset + 4) return { name: 'Unknown', symbol: '?', uri: '' };

    const nameLen = data.readUInt32LE(offset); offset += 4;
    const name = data.slice(offset, offset + nameLen).toString('utf8').replace(/\0/g, '').trim(); offset += nameLen;

    const symbolLen = data.readUInt32LE(offset); offset += 4;
    const symbol = data.slice(offset, offset + symbolLen).toString('utf8').replace(/\0/g, '').trim(); offset += symbolLen;

    const uriLen = data.readUInt32LE(offset); offset += 4;
    const uri = data.slice(offset, offset + uriLen).toString('utf8').replace(/\0/g, '').trim();

    return { name, symbol, uri };
  } catch (e) {
    return { name: 'Unknown', symbol: '?', uri: '' };
  }
}

async function fetchAndSaveMetadata(mint: string, context?: any, launchpad: string = 'pump') {
  try {
    if (monitoredMints.has(mint)) return;

    // 1. Check DB for duplicates
    const exists = await new Promise((resolve) => {
      db.get('SELECT 1 FROM tokens WHERE coinMint = ?', [mint], (err, row) => resolve(!!row));
    });
    if (exists) {
      monitoredMints.add(mint);
      return;
    }

    // 2. Initial Meta (Try cached first)
    let name = context?.name || 'Unknown';
    let symbol = context?.symbol || context?.ticker || '?';
    let uri = context?.uri || '';
    let description = '';
    let image = '';
    let twitter = '';
    let telegram = '';
    let website = '';

    const cached = metadataRegistry.get(mint);
    if (cached && cached.name !== 'Unknown') {
      name = cached.name;
      symbol = cached.symbol;
      if (cached.uri) uri = cached.uri;
      console.log(`🛰️ Registry Resolve: ${name} (${mint.slice(0, 8)})`);
    }

    // 3. On-Chain Metaplex Fallback (Reserved for Meteora/Bags as requested)
    const useMetaplex = ['meteora', 'bags'].includes(launchpad);
    if (useMetaplex && (name === 'Unknown' || name === 'Pending Metadata' || !uri)) {
      for (let attempt = 0; attempt < 10; attempt++) {
        const reg = metadataRegistry.get(mint);
        if (reg && reg.name !== 'Unknown') {
          name = reg.name; symbol = reg.symbol; uri = reg.uri || uri;
          break;
        }

        try {
          const metadataPDA = PublicKey.findProgramAddressSync(
            [Buffer.from('metadata'), METAPLEX_PROGRAM_ID.toBuffer(), new PublicKey(mint).toBuffer()],
            METAPLEX_PROGRAM_ID
          )[0];

          const account = await rpcQueue.add((c) => c.getAccountInfo(metadataPDA));
          if (account && account.data.length > 65) {
            const chainMeta = parseMetaplexMetadata(account.data);
            name = chainMeta.name || name;
            symbol = chainMeta.symbol || symbol;
            uri = chainMeta.uri || uri;
            console.log(`✅ On-chain Metaplex resolved for ${mint.slice(0, 8)}: ${name}`);
            break;
          }
        } catch (e) { }
        await new Promise(r => setTimeout(r, 2000));
      }
    }

    // 4. Off-Chain JSON
    if (uri && uri.startsWith('http')) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000);
        const res = await fetch(uri, { signal: controller.signal });
        clearTimeout(timeoutId);
        if (res.ok) {
          const json = await res.json() as any;
          name = json.name || name;
          symbol = json.symbol || symbol;
          description = json.description || '';
          image = json.image || json.image_url || '';
          twitter = json.twitter || json.extensions?.twitter || json.links?.twitter || '';
          telegram = json.telegram || json.extensions?.telegram || json.links?.telegram || '';
          website = json.website || json.external_url || json.links?.website || '';
          console.log(`📄 JSON Metadata fetched for ${mint.slice(0, 8)}`);
        }
      } catch (e) { }
    }

    // 5. Analytics
    let top10holderspercentage = 0, numHolders = 0, devHoldingsPercentage = 0, sniperOwnedPercentage = 0;
    try {
      const mintPubKey = new PublicKey(mint);
      const [holders, supply]: [any, any] = await Promise.all([
        rpcQueue.add((c) => c.getTokenLargestAccounts(mintPubKey)),
        rpcQueue.add((c) => c.getTokenSupply(mintPubKey))
      ]);
      const total = supply.value.uiAmount || 0;
      numHolders = holders.value.length;

      if (total > 0) {
        const accs = await rpcQueue.add((c) => c.getMultipleAccountsInfo(holders.value.map((h: any) => h.address))).catch(() => []);
        const ownerBalances: Record<string, number> = {};
        const snipersRaw = context?.sniperWallets;
        const snipers = Array.isArray(snipersRaw) ? snipersRaw : JSON.parse(snipersRaw || '[]');
        const dev = context?.creator;

        for (let i = 0; i < accs.length; i++) {
          const acc = accs[i];
          if (acc && acc.data.length >= 64) {
            const owner = new PublicKey(acc.data.slice(32, 64)).toString();
            const amount = Number(acc.data.readBigUInt64LE(64)) / 1e6;
            ownerBalances[owner] = (ownerBalances[owner] || 0) + amount;
            const pct = (amount / total) * 100;
            if (owner === dev) devHoldingsPercentage += pct;
            if (snipers.includes(owner)) sniperOwnedPercentage += pct;
          }
        }

        const sortedOwnersRaw = Object.entries(ownerBalances).sort((a, b: any) => b[1] - a[1]);
        const uniqueOwners = sortedOwnersRaw.map(([o]) => new PublicKey(o));
        const ownerInfos = await rpcQueue.add((c) => c.getMultipleAccountsInfo(uniqueOwners)).catch(() => []);

        let validOwners = 0, top10OwnersSum = 0;
        for (let i = 0; i < sortedOwnersRaw.length; i++) {
          const info = ownerInfos[i];
          if (info && info.owner.equals(SYSTEM_PROGRAM_ID)) {
            top10OwnersSum += sortedOwnersRaw[i][1];
            validOwners++;
            if (validOwners === 10) break;
          }
        }
        top10holderspercentage = (top10OwnersSum / total) * 100;
      }
    } catch { }

    const curve = await getBondingCurveInfo(mint, launchpad, context?.bonding_curve);
    const solPrice = await getSolPrice();
    const marketCap = (curve?.priceUsd || (30 / 1073000000) * solPrice) * 1000000000;
    let creationTime = context?.timestamp ? Number(context.timestamp) * 1000 : Date.now();
    const { insiders, snipers } = await detectInsiders(mint, context?.creator, context?.signature, curve?.pda);
    const insiderWallets = insiders;
    const sniperWallets = snipers;

    const row: any = {
      coinMint: mint,
      dev: context?.creator || null,
      name: name,
      ticker: symbol,
      imageUrl: image,
      creationTime: creationTime,
      numHolders: numHolders,
      description: description,
      marketCap: marketCap,
      volume: 0,
      currentMarketPrice: curve?.priceUsd || (30 / 1073000000) * solPrice,
      bondingCurveProgress: curve?.curvePercentage || 0,
      sniperCount: 0,
      poolAddress: curve?.pda || context?.bonding_curve || null,
      twitter: twitter,
      telegram: telegram,
      website: website,
      hasTwitter: twitter ? 1 : 0,
      hasTelegram: telegram ? 1 : 0,
      hasWebsite: website ? 1 : 0,
      hasSocial: (twitter || telegram || website) ? 1 : 0,
      devHoldingsPercentage,
      buyTransactions: 0,
      sellTransactions: 0,
      transactions: 0,
      sniperOwnedPercentage,
      top10holderspercentage,
      tokenProgram: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      program: launchpad,
      platform: launchpad,
      uri: uri,
      signature: context?.signature || null,
      block_time_iso: context?.block_time || new Date().toISOString(),
      virtual_sol: curve?.vSol || 30.000000001,
      virtual_token: curve?.vToken || 1073000000,
      real_sol: curve?.rSol || 0,
      real_token: curve?.rToken || 793100000,
      is_complete: 0,
      sniperWallets: JSON.stringify(sniperWallets),
      insiderWallets: JSON.stringify(insiderWallets),
      sniperTotalBalance: 0,
      insiderTotalBalance: 0,
      insiderOwnedPercentage: 0,
      graduation_status: "new",
      hasFileMetaData: uri ? 1 : 0
    };

    db.run(`INSERT OR REPLACE INTO tokens(
        coinMint, dev, name, ticker, imageUrl, creationTime, numHolders, description,
        marketCap, volume, currentMarketPrice, bondingCurveProgress, sniperCount,
        poolAddress, twitter, telegram, website,
        hasTwitter, hasTelegram, hasWebsite, hasSocial, devHoldingsPercentage,
        sniperOwnedPercentage, top10holderspercentage, tokenProgram, program, platform,
        uri, signature, block_time_iso, virtual_sol, virtual_token, real_sol, real_token, is_complete, sniperWallets,
        insiderWallets, sniperTotalBalance, insiderTotalBalance, insiderOwnedPercentage, graduation_status, hasFileMetaData
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        row.coinMint, row.dev, row.name, row.ticker, row.imageUrl, row.creationTime,
        row.numHolders, row.description, row.marketCap, row.volume, row.currentMarketPrice,
        row.bondingCurveProgress, row.sniperCount, row.poolAddress, row.twitter, row.telegram, row.website,
        row.hasTwitter, row.hasTelegram, row.hasWebsite, row.hasSocial,
        row.devHoldingsPercentage, row.sniperOwnedPercentage, row.top10holderspercentage,
        row.tokenProgram, row.program, row.platform,
        row.uri, row.signature, row.block_time_iso, row.virtual_sol, row.virtual_token,
        row.real_sol, row.real_token, row.is_complete, row.sniperWallets,
        row.insiderWallets, row.sniperTotalBalance, row.insiderTotalBalance, row.insiderOwnedPercentage,
        row.graduation_status, row.hasFileMetaData
      ], (err) => {
        if (err) {
          console.error('❌ DB Insert Error:', err);
        } else {
          console.log(`[SAVE] ${launchpad.toUpperCase()} token saved: ${mint}`);
          monitoredMints.add(mint);
          db.run(`INSERT INTO price_history(coinMint, price, volume, timestamp) VALUES(?, ?, ?, ?)`,
            [mint, row.currentMarketPrice, 0, (creationTime as any)]);

          // Dev Migration Stats (Full Scan if needed)
          (async () => {
            const stats: any = await new Promise((resolve) => {
              db.get('SELECT * FROM dev_stats WHERE creator = ?', [row.dev], (err, s) => resolve(s));
            });

            if (!stats || !stats.last_full_scan || Date.now() - stats.last_full_scan > 24 * 60 * 60 * 1000) {
              const devStats = await getFullDevStats(row.dev);
              db.run(`
                INSERT OR REPLACE INTO dev_stats (creator, total_launched, total_migrated, last_full_scan, last_updated)
                VALUES (?, ?, ?, ?, ?)
              `, [row.dev, devStats.total_launched, devStats.total_migrated, Date.now(), Date.now()], () => {
                broadcastDevUpdate(row.dev, devStats);
                broadcast(row);
              });
            } else {
              db.run(`
                UPDATE dev_stats SET total_launched = total_launched + 1, last_updated = ? WHERE creator = ?
              `, [Date.now(), row.dev], () => {
                db.get('SELECT total_launched, total_migrated FROM dev_stats WHERE creator = ?', [row.dev], (err, s: any) => {
                  if (!err && s) broadcastDevUpdate(row.dev, s);
                  broadcast(row);
                });
              });
            }
          })();
          console.log(`✅ Token successfully broadcasted: ${mint.slice(0, 8)}`);
        }
      });
  } catch (e) {
    console.error('❌ Error processing new mint:', e);
  }
}

// WebSocket Listener
function connectWS() {
  console.log(`📡 Connecting to Solana WSS: ${currentWss.split('.com/')[0]}...`);
  const ws = new WebSocket(currentWss);
  let reconnecting = false;

  ws.on('open', () => {
    console.log('✅ Connected to Solana Mainnet');
    // Subscribe separately for maximum compatibility with some RPC providers
    const programs = WATCH_PROGRAMS.map(p => p.toBase58());

    programs.forEach((id, index) => {
      ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: index + 1,
        method: 'logsSubscribe',
        params: [
          { mentions: [id] },
          { commitment: 'confirmed' }
        ]
      }));
    });
    console.log(`✅ Subscribed to ${programs.length} programs separately`);
  });

  ws.on('message', async (data: string) => {
    try {
      const msg = JSON.parse(data);
      if (msg.method === 'logsNotification') {
        const { logs, signature } = msg.params.result.value;

        // 🛡️ Fix: Dedup Signatures (WebSocket spams same tx for multiple program mentions)
        if (processedSignatures.has(signature)) return;
        processedSignatures.add(signature);

        // Find all invoked programs
        const activePrograms = new Set<string>();
        for (const log of logs) {
          if (log.includes('invoke [')) {
            const parts = log.trim().split(' ');
            const id = parts[1];
            if (id && id.length >= 32 && id.length <= 44) activePrograms.add(id);
          }
        }

        // --- 🛰️ Pre-Indexer (Metaplex Intercept) ---
        const isMetaplexMentioned = activePrograms.has(METAPLEX_PROGRAM_ID.toBase58());
        if (isMetaplexMentioned) {
          console.log(`🛰️ Metaplex activity: ${signature.slice(0, 8)}`);
          const isCreateMeta = logs.some((l: string) =>
            l.includes('CreateMetadataAccount') ||
            l.includes('Create Metadata Account') ||
            l.includes('Instruction: Create')
          );
          if (isCreateMeta) {
            console.log(`🚀 Found METAPLEX CREATE in ${signature.slice(0, 8)}`);
            (async () => {
              try {
                await new Promise(r => setTimeout(r, 800)); // ⏳ Mini delay for RPC consistency
                const tx = await rpcQueue.add((c) => c.getParsedTransaction(signature, { maxSupportedTransactionVersion: 0, commitment: 'confirmed' }));
                if (tx && tx.transaction.message) {
                  const allIxs: any[] = [...tx.transaction.message.instructions];
                  if (tx.meta?.innerInstructions) {
                    tx.meta.innerInstructions.forEach(inner => allIxs.push(...inner.instructions));
                  }

                  for (const ix of allIxs) {
                    if (ix.programId.toBase58() === METAPLEX_PROGRAM_ID.toBase58() && ix.data && ix.accounts) {
                      const mint = ix.accounts[1]?.toBase58?.() || ix.accounts[1]?.toString?.();
                      if (mint) {
                        // Try both base64 and base58 (Solana uses b58 for raw data in messages usually)
                        const data = ix.data.length > 100 ? Buffer.from(ix.data, 'base64') : bs58.decode(ix.data);
                        const parsed = parseMetaplexMetadata(Buffer.from(data), true);
                        if (parsed.name && parsed.name !== 'Unknown') {
                          metadataRegistry.set(mint, parsed);
                          console.log(`🛰️ Registry Pre-Indexed: ${parsed.name} (${mint.slice(0, 8)})`);
                        }
                      }
                    }
                  }
                }
              } catch (e) { }
            })();
          }
        }

        // --- Heavy Detection (Bags.fm / Meteora DBC) ---
        const BAGS_V1 = 'BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv';
        const isBagsProgramInvolved = activePrograms.has(BAGS_FEE_PROGRAM.toBase58()) || activePrograms.has(BAGS_V1);
        const isMeteoraInvolved = activePrograms.has(METDBC_PROGRAM.toBase58());

        if (isBagsProgramInvolved || isMeteoraInvolved) {
          const hasLaunchKeywords = logs.some((l: string) =>
            l.includes('Instruction: Create') ||
            l.includes('Instruction: CreateConfig') ||
            l.includes('Instruction: Initialize') ||
            l.includes('initialize') ||
            l.includes('Instruction: InitializeVirtualPool')
          );

          if (hasLaunchKeywords) {
            (async () => {
              try {
                const tx = await rpcQueue.add((c) => c.getParsedTransaction(signature, {
                  maxSupportedTransactionVersion: 0,
                  commitment: 'confirmed'
                }));

                if (tx && tx.transaction.message) {
                  const allAccounts = tx.transaction.message.accountKeys.map(k => k.pubkey.toBase58());

                  // 1. Bags.fm Check
                  if (allAccounts.includes(BAGS_V1) || allAccounts.includes(BAGS_FEE_PROGRAM.toBase58())) {
                    let mint = allAccounts.find(a => a.endsWith('BAGS')) || 'unknown';
                    let pool = 'unknown';
                    let creator = allAccounts[0];

                    const bagsIx = (tx.transaction.message.instructions as any[]).find(ix =>
                      ix.programId.toBase58() === BAGS_FEE_PROGRAM.toBase58() || ix.programId.toBase58() === BAGS_V1
                    ) || (tx.transaction.message.instructions as any[]).find(ix => ix.programId.toBase58() === '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

                    if (bagsIx && bagsIx.accounts) {
                      const ixAccs = bagsIx.accounts.map((a: any) => a.toBase58 ? a.toBase58() : a.toString());
                      if (mint === 'unknown') mint = ixAccs.find((a: string) => a.endsWith('pump')) || ixAccs[6] || ixAccs[ixAccs.length - 3] || 'unknown';
                      pool = ixAccs[ixAccs.length - 1] === BAGS_FEE_PROGRAM.toBase58() ? ixAccs[ixAccs.length - 2] : ixAccs[5];
                    }

                    if (mint !== 'unknown' && mint.length >= 32) {
                      const parsed = {
                        name: "Unknown", symbol: "BAGS", uri: "",
                        mint, bonding_curve: pool, creator, signature, launchpad: 'bags'
                      };

                      // 1. Check Pre-Indexer Cache first (Instant)
                      const cached = metadataRegistry.get(mint);
                      if (cached) {
                        parsed.name = cached.name;
                        parsed.symbol = cached.symbol;
                        parsed.uri = cached.uri;
                        console.log(`⚡ Instant resolve from registry: ${parsed.name} (${mint.slice(0, 8)})`);
                        fetchAndSaveMetadata(mint, parsed, 'bags');
                      } else {
                        // 2. Fallback to wait-and-resolve
                        getMetadataWithWait(mint).then(chainMeta => {
                          if (chainMeta) {
                            parsed.name = chainMeta.name;
                            parsed.symbol = chainMeta.symbol;
                            parsed.uri = chainMeta.uri;
                            fetchAndSaveMetadata(mint, parsed, 'bags');
                          } else {
                            fetchAndSaveMetadata(mint, parsed, 'bags');
                          }
                        });
                      }
                    }
                  } else if (allAccounts.includes(METDBC_PROGRAM.toBase58())) {
                    // 2. Meteora DBC Check - Production Indices
                    const meteoraIx = (tx.transaction.message.instructions as any[]).find(ix => ix.programId.toBase58() === METDBC_PROGRAM.toBase58());
                    if (meteoraIx && meteoraIx.accounts) {
                      const accounts = meteoraIx.accounts.map((a: any) => a.toBase58 ? a.toBase58() : a.toString());
                      const creator = accounts[2] || 'unknown';
                      const mint = accounts[3] || 'unknown';
                      const pool = accounts[4] || 'unknown'; // Using index 4 as the correct pool/bonding curve address

                      if (mint !== 'unknown' && mint.length >= 32) {
                        // 🛡️ Filter Base Tokens (SOL, USDC, etc)
                        const BASE_TOKENS = ['So11111111111111111111111111111111111111112', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaDCSTjG3L69G9v9YTyV2ZP6SS8uHLL758686'];
                        if (BASE_TOKENS.includes(mint)) return;

                        const parsed = { name: "", symbol: "", uri: "", mint, bonding_curve: pool, creator, signature };

                        // 1. Check Pre-Indexer Cache first (Instant)
                        const cached = metadataRegistry.get(mint);
                        if (cached) {
                          parsed.name = cached.name;
                          parsed.symbol = cached.symbol;
                          parsed.uri = cached.uri;
                          console.log(`⚡ Instant resolve (Meteora) from registry: ${parsed.name} (${mint.slice(0, 8)})`);
                          fetchAndSaveMetadata(mint, parsed, 'meteora');
                        } else {
                          // 2. Fallback to wait-and-resolve
                          getMetadataWithWait(mint).then(chainMeta => {
                            if (chainMeta) {
                              parsed.name = chainMeta.name;
                              parsed.symbol = chainMeta.symbol;
                              parsed.uri = chainMeta.uri;
                              fetchAndSaveMetadata(mint, parsed, 'meteora');
                            } else {
                              fetchAndSaveMetadata(mint, parsed, 'meteora');
                            }
                          });
                        }
                      }
                    }
                  }
                }
              } catch (e: any) { }
            })();
          }
        }

        // --- LetsBonk / Raydium Launchpad Debug ---
        if (activePrograms.has(RAYDIUM_LAUNCHLAB_PROGRAM.toBase58())) {
          for (const log of logs) {
            if (log.includes('Program data: ')) {
              const buffer = Buffer.from(log.split('Program data: ')[1], 'base64');
              console.log(`[LetsBonk] Event Detected | Sig: ${signature.slice(0, 8)}`);
            }
          }
        }

        // --- Standard Event Based Detection (Pump, Moonshot) ---
        const programsToCheck = activePrograms.size > 0 ? Array.from(activePrograms) : WATCH_PROGRAMS.map(p => p.toBase58());
        for (const log of logs) {
          if (!log.includes('Program data: ')) continue;
          const dataBase64 = log.split('Program data: ')[1];
          if (!dataBase64) continue;
          const buffer = Buffer.from(dataBase64, 'base64');

          for (const activeId of programsToCheck) {
            const detected = detectLaunchpad(activeId, buffer);
            if (detected) {
              if (detected.type === 'create') {
                const event: any = detected.parsed;
                console.log(`🚀 New ${detected.launchpad} Token: `, signature);
                fetchAndSaveMetadata(event.mint, {
                  creator: event.user, signature, uri: event.uri, bonding_curve: event.bonding_curve, timestamp: Date.now() / 1000
                }, detected.launchpad);
              } else if (detected.type === 'trade') {
                const trade = detected.parsed;
                enqueueTrade(trade.mint, trade, signature);
              }
            }
          }
        }
      }
    } catch (err) {
      console.error('❌ WebSocket Message Error:', err);
    }
  });

  ws.on('error', (e: any) => console.error('❌ WS Error:', e.message));
  ws.on('close', () => {
    if (!reconnecting) {
      reconnecting = true;
      wssIndex = (wssIndex + 1) % WSS_ENDPOINTS.length;
      currentWss = WSS_ENDPOINTS[wssIndex];
      currentRpc = RPC_ENDPOINTS[wssIndex];
      connection = new Connection(currentRpc, { commitment: 'confirmed' });
      console.log(`🔌 Connection Lost.Rotating to index ${wssIndex + 1}/${WSS_ENDPOINTS.length} in 3s...`);
      setTimeout(connectWS, 3000);
    }
  });
}


// Debounce map for holder updates
const holderUpdateTimeouts = new Map<string, NodeJS.Timeout>();

async function updateHolderStats(mint: string) {
  if (holderUpdateTimeouts.has(mint)) return; // Already scheduled

  const timeout = setTimeout(async () => {
    holderUpdateTimeouts.delete(mint);
    db.get('SELECT dev, sniperWallets, insiderWallets FROM tokens WHERE coinMint = ?', [mint], async (err, row: any) => {
      try {
        if (err || !row) return;

        const mintPubKey = new PublicKey(mint);
        const [holders, supply] = await Promise.all([
          rpcQueue.add((c) => c.getTokenLargestAccounts(mintPubKey)),
          rpcQueue.add((c) => c.getTokenSupply(mintPubKey))
        ]);

        const total = supply.value.uiAmount || 0;
        if (total <= 0) return;

        const snipers = JSON.parse(row.sniperWallets || '[]');
        const insiders = JSON.parse(row.insiderWallets || '[]');

        const balanceMap = new Map<string, number>();
        holders.value.forEach(h => balanceMap.set(h.address.toString(), h.uiAmount || 0));

        const accs = await rpcQueue.add((c) => c.getMultipleAccountsInfo(holders.value.map(h => h.address))).catch(() => null);
        if (!accs) return;

        let devPct = 0, sniperPct = 0, sniperBal = 0, insiderPct = 0, insiderBal = 0;
        const ownerBalances: Record<string, number> = {};

        for (let i = 0; i < accs.length; i++) {
          const acc = accs[i];
          if (!acc || acc.data.length < 64) continue;
          const addr = holders.value[i].address.toString();
          const amount = balanceMap.get(addr) || 0;
          const owner = new PublicKey(acc.data.slice(32, 64)).toString();
          ownerBalances[owner] = (ownerBalances[owner] || 0) + amount;

          if (owner === row.dev) devPct += (amount / total) * 100;
          if (snipers.includes(owner)) { sniperBal += amount; sniperPct += (amount / total) * 100; }
          if (insiders.includes(owner)) { insiderBal += amount; insiderPct += (amount / total) * 100; }
        }

        const sortedOwnersRaw = Object.entries(ownerBalances).sort((a, b) => b[1] - a[1]);
        const uniqueOwnerPubkeys = sortedOwnersRaw.slice(0, 20).map(([o]) => new PublicKey(o));
        const ownerInfos = await rpcQueue.add((c) => c.getMultipleAccountsInfo(uniqueOwnerPubkeys)).catch(() => null);

        let top10OwnersSum = 0, validOwnersCount = 0;
        if (ownerInfos) {
          for (let j = 0; j < ownerInfos.length; j++) {
            const info = ownerInfos[j];
            if (info && info.owner.equals(SYSTEM_PROGRAM_ID)) {
              top10OwnersSum += ownerBalances[uniqueOwnerPubkeys[j].toString()];
              if (++validOwnersCount === 10) break;
            }
          }
        }

        db.run(`UPDATE tokens SET numHolders=?, devHoldingsPercentage=?, sniperOwnedPercentage=?, sniperTotalBalance=?, insiderOwnedPercentage=?, insiderTotalBalance=?, top10holderspercentage=? WHERE coinMint=?`,
          [holders.value.length, devPct, sniperPct, sniperBal, insiderPct, insiderBal, (top10OwnersSum / total) * 100, mint],
          (err) => {
            if (!err) {
              db.get('SELECT * FROM tokens WHERE coinMint = ?', [mint], (err, updatedRow) => {
                if (!err && updatedRow) {
                  broadcast(updatedRow);
                  // Specialized broadcasts
                  broadcastHolders(mint, { count: holders.value.length, top10: (top10OwnersSum / total) * 100 });
                  broadcastDevHoldings(mint, { percentage: devPct });
                  broadcastSniperUpdate(mint, { percentage: sniperPct, wallets: snipers });
                  broadcastInsiderUpdate(mint, { percentage: insiderPct, wallets: insiders });
                }
              });
            }
          });
      } catch (e: any) {
        console.error(`❌ Holder Update Async Error (${mint}):`, e.message);
      }
    });
  }, 5000); // 5s debounce

  holderUpdateTimeouts.set(mint, timeout);
}

async function handleTrade(trade: any, signature?: string) {
  const solVol = Number(trade.solAmount) / 1e9;
  db.get('SELECT dev, sniperWallets, insiderWallets, buyTransactions, sellTransactions, volume, creationTime, lastSignature, platform, poolAddress FROM tokens WHERE coinMint = ?', [trade.mint], async (err, row: any) => {
    try {
      if (err || !row) return;
      if (signature && signature === row.lastSignature) return;

      const snipers = JSON.parse(row.sniperWallets || '[]');
      const insiders = JSON.parse(row.insiderWallets || '[]');
      let { buyTransactions, sellTransactions, volume } = row;
      if (trade.isBuy) buyTransactions++; else sellTransactions++;
      const totalTx = buyTransactions + sellTransactions;
      const newVol = volume + solVol;

      if (trade.isBuy && (Number(trade.timestamp) * 1000 - row.creationTime) < 15000) {
        if (!snipers.includes(trade.user) && !insiders.includes(trade.user) && trade.user !== row.dev) {
          // Identify if they are a real insider or just a fast sniper
          const isFundedByDev = await checkFundingSource(trade.user, row.dev);
          if (isFundedByDev) {
            insiders.push(trade.user);
            console.log(`🕵️ Real-time Insider Detected: ${trade.user.slice(0, 8)}`);
          } else {
            snipers.push(trade.user);
            console.log(`🤖 Real-time Sniper Detected: ${trade.user.slice(0, 8)}`);
          }
          updateHolderStats(trade.mint).catch(() => { });
        }
      }

      // 🛰️ Real-time On-Chain Verification (Price & Fees)
      let livePriceUsd = (Number(trade.vSol) / Number(trade.vToken)) * 0.001 * (cachedSolPrice || 160);
      let realFeeSol = solVol * 0.01; // Default

      if (signature) {
        try {
          const detailedTx = await rpcQueue.add((c) => c.getParsedTransaction(signature, { maxSupportedTransactionVersion: 0, commitment: 'confirmed' }));
          if (detailedTx && detailedTx.meta) {
            // Find Treasury/Platform fee by identifying balance changes in global fee accounts
            const feeAccounts = [
              'Fee8ZDkZRpxU86gY7d72r8sFm9sWw4nS1T2V6XU5C', // Pump.fun Fee
              '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium (Proxy)
              'C98A6qPvS4DVs3vF79hWv9C8sNWHf4pQf3YyXG6t9k4D' // Moonshot Fee
            ];

            let totalTreasuryGain = 0;
            const accountKeys = detailedTx.transaction.message.accountKeys.map(k => k.pubkey.toBase58());
            for (let i = 0; i < accountKeys.length; i++) {
              if (feeAccounts.includes(accountKeys[i])) {
                const gain = (detailedTx.meta.postBalances[i] - detailedTx.meta.preBalances[i]) / 1e9;
                if (gain > 0) totalTreasuryGain += gain;
              }
            }
            if (totalTreasuryGain > 0) realFeeSol = totalTreasuryGain;
          }
        } catch (e) { }
      }

      const progress = Math.min(100, ((Number(trade.vSol) / 1e9 - (row.platform === 'moonshot' ? 0 : 30)) / (row.platform === 'moonshot' ? 100 : 85)) * 100);

      db.run(`UPDATE tokens SET buyTransactions=?, sellTransactions=?, transactions=?, volume=?, sniperWallets=?, insiderWallets=?, sniperCount=?, currentMarketPrice=?, marketCap=?, bondingCurveProgress=?, graduation_status=?, lastSignature=? WHERE coinMint=?`,
        [buyTransactions, sellTransactions, totalTx, newVol, JSON.stringify(snipers), JSON.stringify(insiders), snipers.length, livePriceUsd, livePriceUsd * 1e9, progress, progress >= 80 ? 'imminent' : 'new', signature || row.lastSignature, trade.mint],
        (err) => {
          if (!err) {
            db.run(`INSERT INTO price_history (coinMint, price, volume, timestamp) VALUES (?, ?, ?, ?)`,
              [trade.mint, livePriceUsd, solVol, Date.now()]);

            db.get('SELECT * FROM tokens WHERE coinMint = ?', [trade.mint], (err, updatedRow) => {
              if (!err && updatedRow) {
                broadcast(updatedRow);
                broadcastPriceUpdate(trade.mint, { price: livePriceUsd, timestamp: Date.now() }, row.poolAddress);
                broadcastFees(trade.mint, {
                  mint: trade.mint,
                  total_fees_sol: realFeeSol,
                  timestamp: Date.now(),
                  tx_signature: signature
                });
                broadcastTransaction({
                  mint: trade.mint,
                  signature,
                  user: trade.user,
                  isBuy: trade.isBuy,
                  solAmount: solVol,
                  tokenAmount: trade.tokenAmount,
                  timestamp: Date.now()
                }, row.poolAddress);
                broadcastWalletTransactions(trade.user, {
                  mint: trade.mint,
                  signature,
                  isBuy: trade.isBuy,
                  solAmount: solVol,
                  timestamp: Date.now()
                });

                // Update aggregate stats on every trade with real mainnet data
                const stats = {
                  totalTokens: monitoredMints.size,
                  solPrice: cachedSolPrice,
                  totalVolume24h: 0,
                  timestamp: Date.now()
                };

                // Get real total volume from DB asynchronously
                db.get('SELECT SUM(volume) as total FROM tokens', (err, result: any) => {
                  if (!err && result) {
                    stats.totalVolume24h = result.total || 0;
                    broadcastStatistics(stats);
                  }
                });

                // Fetch recent history for chart update
                db.all('SELECT price, volume, timestamp FROM price_history WHERE coinMint = ? ORDER BY timestamp DESC LIMIT 1', [trade.mint], (err, history) => {
                  if (!err && history.length > 0) {
                    broadcastChartData(trade.mint, history[0], row.poolAddress);
                  }
                });
              }
            });
          }
        }
      );
    } catch (e) { console.error(`❌ Trade Handler Error (${trade.mint}):`, e); }
  });
}

async function poll() {
  db.all('SELECT * FROM tokens WHERE is_complete = 0 ORDER BY created_at DESC LIMIT 20', async (err: any, rows: any[]) => {
    if (err || !rows) return;
    for (const row of rows) {
      try {
        const curve = await getBondingCurveInfo(row.coinMint, row.platform || 'pump', row.poolAddress);
        if (curve && curve.pda) {
          const sigs = await rpcQueue.add((c) => c.getSignaturesForAddress(new PublicKey(curve.pda), { until: row.lastSignature || undefined, limit: 10 }));
          if (sigs.length > 0) {
            for (const s of sigs.reverse()) {
              const tx = await rpcQueue.add((c) => c.getTransaction(s.signature, { maxSupportedTransactionVersion: 0 }));
              if (tx?.meta?.logMessages) {
                for (const log of tx.meta.logMessages) {
                  if (log.includes('Program data: ')) {
                    const detected = detectLaunchpad(row.platform === 'moonshot' ? MOONSHOT_PROGRAM_ID.toBase58() : PUMP_PROGRAM_ID.toBase58(), Buffer.from(log.split('Program data: ')[1], 'base64'));
                    if (detected && detected.type === 'trade') await enqueueTrade(detected.parsed.mint, detected.parsed, s.signature);
                  }
                }
              }
            }
          }
          const mc = curve.priceUsd * 1e9;
          const newGraduationStatus = curve.isComplete ? 'graduated' : (curve.curvePercentage >= 80 ? 'imminent' : (row.graduation_status || 'new'));

          db.run(`UPDATE tokens SET virtual_sol=?, virtual_token=?, real_sol=?, real_token=?, is_complete=?, bondingCurveProgress=?, currentMarketPrice=?, marketCap=?, allTimeHighMarketCap=?, graduation_status=? WHERE coinMint=?`,
            [curve.vSol, curve.vToken, curve.rSol, curve.rToken, curve.isComplete ? 1 : 0, curve.curvePercentage, curve.priceUsd, mc, Math.max(row.allTimeHighMarketCap || 0, mc), newGraduationStatus, row.coinMint],
            () => {
              if (curve.isComplete && row.graduation_status !== 'graduated') {
                (async () => {
                  const stats: any = await new Promise((resolve) => {
                    db.get('SELECT * FROM dev_stats WHERE creator = ?', [row.dev], (err, s) => resolve(s));
                  });

                  if (!stats || !stats.last_full_scan || Date.now() - (stats.last_full_scan || 0) > 24 * 60 * 60 * 1000) {
                    const devStats = await getFullDevStats(row.dev);
                    db.run(`
                      INSERT OR REPLACE INTO dev_stats (creator, total_launched, total_migrated, last_full_scan, last_updated)
                      VALUES (?, ?, ?, ?, ?)
                    `, [row.dev, devStats.total_launched, devStats.total_migrated, Date.now(), Date.now()], () => {
                      broadcastDevUpdate(row.dev, devStats);
                    });
                  } else {
                    db.run(`
                      UPDATE dev_stats SET total_migrated = total_migrated + 1, last_updated = ? WHERE creator = ?
                    `, [Date.now(), row.dev], () => {
                      db.get('SELECT total_launched, total_migrated FROM dev_stats WHERE creator = ?', [row.dev], (err, s: any) => {
                        if (!err && s) broadcastDevUpdate(row.dev, s);
                      });
                    });
                  }
                })();
              }
              updateHolderStats(row.coinMint).catch(() => { })
            }
          );
        }
      } catch (e) { }
      await new Promise(r => setTimeout(r, 500));
    }
  });
}

console.log('⏳ Initializing Bot...');
getSolPrice();
connectWS();
setInterval(poll, 45000);

// 🛠 Maintenance: Background Meta Refresh
setInterval(async () => {
  try {
    db.all('SELECT coinMint, platform, uri FROM tokens WHERE name = "Pending Metadata" OR name = "Unknown" OR name = "" OR name IS NULL ORDER BY rowid DESC LIMIT 50', async (err, rows: any[]) => {
      if (err || !rows) return;
      for (const token of rows) {
        let meta: any = { name: '', symbol: '' };
        let currentUri = token.uri;

        try {
          const metadataPDA = PublicKey.findProgramAddressSync(
            [Buffer.from('metadata'), METAPLEX_PROGRAM_ID.toBuffer(), new PublicKey(token.coinMint).toBuffer()],
            METAPLEX_PROGRAM_ID
          )[0];
          const metadataAccount = await rpcQueue.add((c) => c.getAccountInfo(metadataPDA));
          if (metadataAccount && metadataAccount.data.length > 65) {
            const metadata = parseMetaplexMetadata(metadataAccount.data);
            if (metadata.name && metadata.name !== 'Unknown') {
              meta.name = metadata.name;
              meta.symbol = metadata.symbol;
              if (metadata.uri && metadata.uri.startsWith('http')) currentUri = metadata.uri;
            }
          }
        } catch (e) { }

        if ((!meta.name || meta.name === '') && currentUri && currentUri.startsWith('http')) {
          try {
            const res = await fetch(currentUri);
            if (res.ok) {
              const json = await res.json() as any;
              meta.name = json.name || meta.name;
              meta.symbol = json.symbol || meta.symbol;
            }
          } catch (e) { }
        }

        if (meta.name && meta.name !== '' && meta.name !== 'Unknown' && meta.name !== 'Pending Metadata') {
          db.run('UPDATE tokens SET name=?, ticker=?, imageUrl=?, description=?, twitter=?, telegram=?, website=? WHERE coinMint=?',
            [meta.name, meta.symbol, meta.image || meta.image_url || '', meta.description || '', meta.twitter || '', meta.telegram || '', meta.website || '', token.coinMint]);
          console.log(`✅ RESOLVED: ${token.coinMint.slice(0, 8)} -> ${meta.name}`);

          db.get('SELECT * FROM tokens WHERE coinMint = ?', [token.coinMint], (err, row) => {
            if (row) broadcast(row);
          });
        }
      }
    });
  } catch (e) { }
}, 4000);


server.listen(PORT, '0.0.0.0', () => console.log(`🚀 API: http://localhost:${PORT}`));
