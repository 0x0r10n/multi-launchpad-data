// src/index.ts
import { Connection, PublicKey } from '@solana/web3.js';
import WebSocket, { Server as SocketServer } from 'ws';
import sqlite3 from 'sqlite3';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import express, { Request, Response } from 'express';
import http from 'http';

dotenv.config();

const RPC = process.env.SOLANA_RPC || 'https://api.mainnet-beta.solana.com';
const WSS_ENDPOINTS = [
  process.env.SOLANA_WSS || 'wss://api.mainnet-beta.solana.com',
  process.env.WSS_ENDPOINT_BACKUP,
  process.env.WSS_ENDPOINT_BACKUP_2
].filter(Boolean) as string[];

let wssIndex = 0;
let currentWss = WSS_ENDPOINTS[0];
const PORT = Number(process.env.PORT) || 3000;
const PUMP_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const CREATE_EVENT_DISCRIMINATOR = '1b72a94ddeeb6376';
const TRADE_EVENT_DISCRIMINATOR = 'bddb7fd34ee661ee';
const SYSTEM_PROGRAM_ID = new PublicKey('11111111111111111111111111111111');

const connection = new Connection(RPC, { commitment: 'confirmed' });

// Global cache for real-time trade monitoring
const monitoredMints = new Set<string>();

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
      timestamp INTEGER
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS idx_price_mint_time ON price_history(coinMint, timestamp)`);

  // Migration for sniperWallets and lastSignature if needed
  db.run(`ALTER TABLE tokens ADD COLUMN insiderWallets TEXT DEFAULT "[]"`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN sniperTotalBalance REAL DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN insiderTotalBalance REAL DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN insiderOwnedPercentage REAL DEFAULT 0`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN lastSignature TEXT`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN graduation_status TEXT DEFAULT "new"`, (err) => { });
  db.run(`ALTER TABLE tokens ADD COLUMN graduation_timestamp INTEGER`, (err) => { });
  db.run(`ALTER TABLE tokens RENAME COLUMN topHoldersPercentage TO top10holderspercentage`, (err) => { });
});

// App & WebSocket Broadcasting
const app = express();
const server = http.createServer(app);
const broadcastWs = new SocketServer({ server, path: '/subscribe-newmints' });
const graduatingWs = new SocketServer({ server, path: '/subscribe-graduating' });

async function broadcastToRoom(wsServer: SocketServer, payload: any) {
  if (cachedSolPrice === 0) await getSolPrice();
  const formatted = await formatFullPayload(payload, cachedSolPrice);
  const msg = JSON.stringify(formatted);
  wsServer.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
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
        createdOn: "https://pump.fun",
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
          market: row.platform || "pumpfun",
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
          amount: (row.devHoldingsPercentage / 100) * 1000000000
        }
      },
      graduation: {
        status: row.graduation_status || "new",
        timestamp: row.graduation_timestamp || null
      }
    }
  };
}

// Keeping original broadcast for backward compatibility, but using multi-room logic
async function broadcast(payload: any) {
  broadcastToRoom(broadcastWs, payload);
  if (payload.graduation_status === 'imminent' || payload.graduation_status === 'graduated') {
    broadcastToRoom(graduatingWs, payload);
  }
}

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
          <a href="/subscribe-graduating" class="endpoint-link"><span>/ws-graduating</span> <span>→</span></a>
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

      <script>
        const ws = new WebSocket(\`ws://\${window.location.host}/subscribe-graduating\`);
        const imminentList = document.getElementById('imminent-list');
        const statusBadge = document.getElementById('connection-status');
        const tokens = new Map();

        ws.onopen = () => {
          statusBadge.textContent = '🟢 Live Connection Active';
          statusBadge.style.background = 'rgba(34, 197, 94, 0.1)';
          statusBadge.style.color = '#22c55e';
          // Initial fetch
          fetch('/imminent-tokens').then(res => res.json()).then(data => {
            data.forEach(t => updateRow(t));
          });
        };

        ws.onmessage = (event) => {
          const payload = JSON.parse(event.data);
          updateRow(payload);
        };

        function updateRow(token) {
          const rowId = \`row-\${token.mint}\`;
          let row = document.getElementById(rowId);
          
          const progress = token.curveProgress || 0;
          const mc = token.marketCap || 0;
          const vol = token.volume || 0;
          const sniperPct = token.risk?.sniperOwned || 0;
          const devPct = token.risk?.devOwned || 0;
          const top10 = token.risk?.top10Owned || 0;
          
          const html = \`
            <td>
              <div class="token-cell">
                <img class="token-icon" src="\${token.image || ''}" onerror="this.style.opacity=0">
                <div class="token-info">
                  <span class="token-name">\${token.name}</span>
                  <span class="token-symbol">\${token.symbol}</span>
                </div>
              </div>
            </td>
            <td>
              <div class="metric">
                <span class="metric-val">\${progress.toFixed(1)}%</span>
                <div class="progress-bar-container">
                  <div class="progress-bar-fill" style="width: \${progress}%"></div>
                </div>
              </div>
            </td>
            <td>$\${(mc/1000).toFixed(1)}k</td>
            <td>$\${vol.toFixed(2)} SOL</td>
            <td>
              <div class="metric">
                <span class="metric-val" style="color: \${sniperPct > 10 ? '#ef4444' : 'inherit'}">\${sniperPct.toFixed(1)}% Sniper</span>
                <span class="metric-label">\${top10.toFixed(1)}% Top 10 | \${devPct.toFixed(1)}% Dev</span>
              </div>
            </td>
            <td class="\${token.graduation?.status === 'graduated' ? 'graduated' : 'imminent'}">
              \${token.graduation?.status.toUpperCase()}
            </td>
          \`;

          if (!row) {
            row = document.createElement('tr');
            row.id = rowId;
            imminentList.prepend(row);
          }
          row.innerHTML = html;
          
          // Sort by progress
          const rows = Array.from(imminentList.querySelectorAll('tr'));
          rows.sort((a, b) => {
            const pA = parseFloat(a.querySelector('.metric-val').textContent);
            const pB = parseFloat(b.querySelector('.metric-val').textContent);
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
    const slot = await connection.getSlot();
    res.json({ ok: true, slot, connections: broadcastWs.clients.size, monitored: monitoredMints.size });
  } catch (e) {
    res.json({ ok: false, error: (e as Error).message });
  }
});

app.get('/tokens', async (req: Request, res: Response) => {
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const solPrice = await getSolPrice();
  db.all('SELECT * FROM tokens ORDER BY created_at DESC LIMIT ?', [limit], async (err: any, rows: any[]) => {
    if (err) return res.status(500).json({ error: err.message });
    const tasks = rows.map(r => formatFullPayload(r, solPrice));
    res.json(await Promise.all(tasks));
  });
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

  while (true) {
    try {
      // Primary source: Pump.fun
      const res = await fetch('https://frontend-api-v3.pump.fun/sol-price');
      if (res.ok) {
        const json = await res.json() as any;
        if (json?.solPrice) {
          cachedSolPrice = Number(json.solPrice);
          lastSolPriceFetch = Date.now();
          return cachedSolPrice;
        }
      }

      // Secondary source: Jupiter Lite V3 (Solana Native)
      const jupRes = await fetch('https://lite-api.jup.ag/price/v3?ids=So11111111111111111111111111111111111111112');
      if (jupRes.ok) {
        const jJson = await jupRes.json() as any;
        const price = jJson?.data?.['So11111111111111111111111111111111111111112']?.price;
        if (price) {
          cachedSolPrice = Number(price);
          lastSolPriceFetch = Date.now();
          return cachedSolPrice;
        }
      }
    } catch (e) {
      console.error('⚠️ SOL Price Fetch Failed, retrying...', (e as Error).message);
    }

    // If both fail, wait 2s and retry - we never return a fallback
    await new Promise(r => setTimeout(r, 2000));
  }
}

async function getBondingCurveInfo(mint: string) {
  try {
    const mintKey = new PublicKey(mint);
    const [pda] = PublicKey.findProgramAddressSync([Buffer.from('bonding-curve'), mintKey.toBuffer()], PUMP_PROGRAM_ID);
    const info = await connection.getAccountInfo(pda);
    if (!info) return null;

    const data = info.data.slice(8);
    const vToken = Number(data.readBigUInt64LE(0)) / 1e6;
    const vSol = Number(data.readBigUInt64LE(8)) / 1e9;
    const rToken = Number(data.readBigUInt64LE(16)) / 1e6;
    const rSol = Number(data.readBigUInt64LE(24)) / 1e9;
    const isComplete = data.readUInt8(40) === 1;

    const priceQuote = vSol / vToken;
    const solPrice = await getSolPrice();
    const curvePercentage = Math.min(100, (rSol / 85) * 100);

    return { pda: pda.toString(), vSol, vToken, rSol, rToken, isComplete, priceQuote, priceUsd: priceQuote * solPrice, curvePercentage, solPrice };
  } catch { return null; }
}

async function fetchAndSaveMetadata(mint: string, context?: any) {
  try {
    const uri = context?.uri;
    if (!uri) return;

    let meta: any = { name: 'Unknown', symbol: '?' };
    let success = false;
    for (let i = 0; i < 3; i++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout
        const res = await fetch(uri, { signal: controller.signal });
        clearTimeout(timeoutId);
        if (res.ok) {
          meta = await res.json();
          success = true;
          break;
        }
      } catch (e) { }
      await new Promise(r => setTimeout(r, 1000));
    }

    let top10holderspercentage = 0;
    let numHolders = 0;
    let devHoldingsPercentage = 0;
    let sniperOwnedPercentage = 0;
    try {
      const mintPubKey = new PublicKey(mint);
      const holders = await connection.getTokenLargestAccounts(mintPubKey);
      const supply = await connection.getTokenSupply(mintPubKey);
      const total = supply.value.uiAmount || 0;
      numHolders = holders.value.length;

      if (total > 0) {
        const accs = await connection.getMultipleAccountsInfo(holders.value.map(h => h.address)).catch(() => []);
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

        const sortedOwnersRaw = Object.entries(ownerBalances).sort((a, b) => b[1] - a[1]);
        const uniqueOwners = sortedOwnersRaw.map(([o]) => new PublicKey(o));
        const ownerInfos = await connection.getMultipleAccountsInfo(uniqueOwners).catch(() => []);

        let validOwners = 0;
        let top10OwnersSum = 0;
        for (let i = 0; i < sortedOwnersRaw.length; i++) {
          const info = ownerInfos[i];
          // A "wallet" (EOA) is owned by System Program. Programs/PDAs are not.
          if (info && info.owner.equals(SYSTEM_PROGRAM_ID)) {
            top10OwnersSum += sortedOwnersRaw[i][1];
            validOwners++;
            if (validOwners === 10) break;
          }
        }
        top10holderspercentage = (top10OwnersSum / total) * 100;

        if (devHoldingsPercentage > 0) console.log(`💎 Dev Holdings for ${mint.slice(0, 8)}: ${devHoldingsPercentage.toFixed(2)}%`);
      }
    } catch { }

    const curve = await getBondingCurveInfo(mint);
    const solPrice = await getSolPrice();
    const marketCap = (curve?.priceUsd || (30 / 1073000000) * solPrice) * 1000000000;

    let creationTime = context?.timestamp ? Number(context.timestamp) * 1000 : Date.now();
    let insiderWallets: string[] = [];

    // Insider Detection - Check if dev funded other wallets in creation tx
    if (context?.signature) {
      try {
        const tx = await connection.getTransaction(context.signature, { maxSupportedTransactionVersion: 0 });
        if (tx?.meta?.postBalances && tx.transaction.message) {
          // Heuristic: check for SOL transfers from dev to other wallets
          // For simplicity, we can look at the account keys and pre/post balances
          const dev = context.creator;
          const accounts = tx.transaction.message.staticAccountKeys;
          for (let i = 0; i < accounts.length; i++) {
            const addr = accounts[i].toString();
            if (addr !== dev && (tx.meta.preBalances[i] < tx.meta.postBalances[i])) {
              // Wallet received SOL in the same transaction
              insiderWallets.push(addr);
            }
          }
        }
      } catch (err) { }
    }

    const row: any = {
      coinMint: mint,
      dev: context?.creator || null,
      name: meta.name || 'Unknown',
      ticker: meta.symbol || '?',
      imageUrl: meta.image || meta.image_url || '',
      creationTime: creationTime,
      numHolders: numHolders,
      description: meta.description || '',
      marketCap: marketCap,
      volume: 0,
      currentMarketPrice: curve?.priceUsd || (30 / 1073000000) * solPrice,
      bondingCurveProgress: curve?.curvePercentage || 0,
      sniperCount: 0,
      graduationDate: null,
      allTimeHighMarketCap: marketCap,
      poolAddress: curve?.pda || context?.bonding_curve || null,
      twitter: meta.twitter || meta.extensions?.twitter || null,
      telegram: meta.telegram || meta.extensions?.telegram || null,
      website: meta.website || meta.extensions?.website || null,
      hasTwitter: (meta.twitter || meta.extensions?.twitter) ? 1 : 0,
      hasTelegram: (meta.telegram || meta.extensions?.telegram) ? 1 : 0,
      hasWebsite: (meta.website || meta.extensions?.website) ? 1 : 0,
      hasSocial: (meta.twitter || meta.telegram || meta.website) ? 1 : 0,
      devHoldingsPercentage,
      buyTransactions: 0,
      sellTransactions: 0,
      transactions: 0,
      sniperOwnedPercentage,
      top10holderspercentage,
      tokenProgram: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      program: "pump",
      platform: "pump",
      uri: uri,
      signature: context?.signature || null,
      block_time_iso: context?.block_time || new Date().toISOString(),
      virtual_sol: curve?.vSol || 30.000000001,
      virtual_token: curve?.vToken || 1073000000,
      real_sol: curve?.rSol || 0,
      real_token: curve?.rToken || 793100000,
      is_complete: 0,
      sniperWallets: "[]",
      insiderWallets: JSON.stringify(insiderWallets),
      sniperTotalBalance: 0,
      insiderTotalBalance: 0,
      insiderOwnedPercentage: 0,
      graduation_status: "new"
    };

    db.run(`INSERT OR REPLACE INTO tokens (
      coinMint, dev, name, ticker, imageUrl, creationTime, numHolders, description,
      marketCap, volume, currentMarketPrice, bondingCurveProgress, sniperCount,
      graduationDate, allTimeHighMarketCap, poolAddress, twitter, telegram, website,
      hasTwitter, hasTelegram, hasWebsite, hasSocial, devHoldingsPercentage,
      buyTransactions, sellTransactions, transactions, sniperOwnedPercentage,
      top10holderspercentage, tokenProgram, program, platform,
      uri, signature, block_time_iso, virtual_sol, virtual_token, real_sol, real_token, is_complete, sniperWallets,
      insiderWallets, sniperTotalBalance, insiderTotalBalance, insiderOwnedPercentage, graduation_status
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        row.coinMint, row.dev, row.name, row.ticker, row.imageUrl, row.creationTime,
        row.numHolders, row.description, row.marketCap, row.volume, row.currentMarketPrice,
        row.bondingCurveProgress, row.sniperCount, row.graduationDate, row.allTimeHighMarketCap,
        row.poolAddress, row.twitter, row.telegram, row.website,
        row.hasTwitter, row.hasTelegram, row.hasWebsite, row.hasSocial,
        row.devHoldingsPercentage, row.buyTransactions, row.sellTransactions,
        row.transactions, row.sniperOwnedPercentage, row.top10holderspercentage,
        row.tokenProgram, row.program, row.platform,
        row.uri, row.signature, row.block_time_iso, row.virtual_sol, row.virtual_token,
        row.real_sol, row.real_token, row.is_complete, row.sniperWallets,
        row.insiderWallets, row.sniperTotalBalance, row.insiderTotalBalance, row.insiderOwnedPercentage,
        row.graduation_status
      ], (err) => {
        if (err) console.error('❌ DB Insert Error:', err);
        else {
          monitoredMints.add(mint);
          broadcast(row);
          console.log(`✅ Token saved: ${mint.slice(0, 8)}`);
        }
      }
    );
  } catch (e) { console.error('❌ Error processing new mint:', e); }
}

// WebSocket Listener
function connectWS() {
  console.log(`📡 Connecting to Solana WSS: ${currentWss.split('.com/')[0]}...`);
  const ws = new WebSocket(currentWss);
  let reconnecting = false;

  ws.on('open', () => {
    console.log('✅ Connected to Solana Mainnet');
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: [{ mentions: [PUMP_PROGRAM_ID.toString()] }, { commitment: 'confirmed' }] }));
  });

  ws.on('message', async (data: string) => {
    try {
      const msg = JSON.parse(data);
      if (msg.method === 'logsNotification') {
        const { logs, signature } = msg.params.result.value;
        for (const log of logs) {
          if (!log.includes('Program data: ')) continue;
          const buffer = Buffer.from(log.split('Program data: ')[1], 'base64');
          const disc = buffer.slice(0, 8).toString('hex');

          if (disc === CREATE_EVENT_DISCRIMINATOR) {
            console.log('🚀 New Pump.fun Token:', signature);
            const event = parseCreateEvent(buffer);
            await fetchAndSaveMetadata(event.mint, {
              creator: event.user, signature, uri: event.uri, bonding_curve: event.bonding_curve,
              timestamp: Date.now() / 1000 // Proxy if not found
            });
          }
          else if (disc === TRADE_EVENT_DISCRIMINATOR) {
            const trade = parseTradeEvent(buffer);
            // Allow processing even if not in cache yet if it's in this transaciton
            await enqueueTrade(trade.mint, trade, signature);
          }
        }
      }
    } catch (err) { console.error('❌ Trade Message Error:', err); }
  });

  ws.on('error', (e) => console.error('❌ WS Error:', e.message));
  ws.on('close', () => {
    if (!reconnecting) {
      reconnecting = true;
      wssIndex = (wssIndex + 1) % WSS_ENDPOINTS.length;
      currentWss = WSS_ENDPOINTS[wssIndex];
      console.log(`🔌 WS Closed. Rotating to endpoint ${wssIndex + 1}/${WSS_ENDPOINTS.length} in 3s...`);
      setTimeout(connectWS, 3000);
    }
  });
}

async function handleTrade(trade: any, signature?: string) {
  const solVol = Number(trade.solAmount) / 1e9;
  db.get('SELECT sniperWallets, buyTransactions, sellTransactions, volume, creationTime, lastSignature FROM tokens WHERE coinMint = ?', [trade.mint], async (err, row: any) => {
    if (err || !row) return;

    let { sniperWallets, buyTransactions, sellTransactions, volume, creationTime, lastSignature } = row;

    // Skip if already processed via poller
    if (signature && signature === lastSignature) return;

    const snipers = JSON.parse(sniperWallets || '[]');

    // Update TX Counts
    if (trade.isBuy) buyTransactions++; else sellTransactions++;
    console.log(`💎 Trade for ${trade.mint.slice(0, 8)}: ${trade.isBuy ? 'BUY' : 'SELL'} (${trade.solAmount} lamports)`);
    const totalTx = buyTransactions + sellTransactions;
    const newVol = volume + solVol;

    // Sniper Detection (Buy within first 15s of launch)
    const txTime = Number(trade.timestamp) * 1000;
    const age = txTime - creationTime;
    if (trade.isBuy && age < 15000 && !snipers.includes(trade.user)) {
      snipers.push(trade.user);
      console.log(`🎯 Sniper Detected for ${trade.mint.slice(0, 8)}: ${trade.user.slice(0, 8)} at ${age}ms`);
    }

    if (totalTx % 5 === 0) console.log(`🔄 Activity for ${trade.mint.slice(0, 8)}: ${totalTx} txs, ${newVol.toFixed(3)} SOL vol`);

    const vSol = Number(trade.vSol) / 1e9;
    const vToken = Number(trade.vToken) / 1e6;
    const priceQuote = vSol / vToken;
    const currentPriceUsd = priceQuote * (cachedSolPrice || await getSolPrice());
    const progress = Math.min(100, ((vSol - 30) / 85) * 100);

    let status = 'new';
    if (progress >= 80) status = 'imminent';

    db.run(`UPDATE tokens SET 
      buyTransactions=?, sellTransactions=?, transactions=?, volume=?, 
      sniperWallets=?, sniperCount=?, currentMarketPrice=?, marketCap=?, 
      bondingCurveProgress=?, graduation_status=?, lastSignature=? WHERE coinMint=?`,
      [
        buyTransactions, sellTransactions, totalTx, newVol,
        JSON.stringify(snipers), snipers.length, currentPriceUsd,
        currentPriceUsd * 1000000000, progress, status, signature || lastSignature, trade.mint
      ],
      (err) => {
        if (err) console.error('❌ DB Trade Update Error:', err);
        else {
          db.run('INSERT INTO price_history (coinMint, price, timestamp) VALUES (?, ?, ?)', [trade.mint, currentPriceUsd, Date.now()]);
          // Fetch the full updated row and broadcast
          db.get('SELECT * FROM tokens WHERE coinMint = ?', [trade.mint], (err, updatedRow) => {
            if (!err && updatedRow) broadcast(updatedRow);
          });
        }
      }
    );
  });
}

function parseCreateEvent(data: Buffer) {
  let offset = 8;
  const readStr = () => {
    const len = data.readUInt32LE(offset); offset += 4;
    const s = data.slice(offset, offset + len).toString(); offset += len;
    return s;
  };
  const name = readStr(); const symbol = readStr(); const uri = readStr();
  const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
  const bonding_curve = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
  const user = new PublicKey(data.slice(offset, offset + 32)).toString();
  return { name, symbol, uri, mint, bonding_curve, user };
}

function parseTradeEvent(data: Buffer) {
  let offset = 8;
  const mint = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
  const solAmount = data.readBigUInt64LE(offset); offset += 8;
  const tokenAmount = data.readBigUInt64LE(offset); offset += 8;
  const isBuy = data.readUInt8(offset) === 1; offset += 1;
  const user = new PublicKey(data.slice(offset, offset + 32)).toString(); offset += 32;
  const timestamp = data.readBigInt64LE(offset); offset += 8;
  const vSol = data.readBigUInt64LE(offset); offset += 8;
  const vToken = data.readBigUInt64LE(offset); offset += 8;
  return { mint, solAmount, tokenAmount, isBuy, user, timestamp, vSol, vToken };
}

// Background poller
async function poll() {
  db.all('SELECT * FROM tokens WHERE is_complete = 0 ORDER BY created_at DESC LIMIT 20', async (err: any, rows: any[]) => {
    if (err || !rows) return;
    for (const row of rows) {
      try {
        // 1. Transaction Backfill (Signatures Catch-up) - Moderate version
        const curve = await getBondingCurveInfo(row.coinMint);
        if (curve && curve.pda) {
          const sigs = await connection.getSignaturesForAddress(new PublicKey(curve.pda), {
            until: row.lastSignature || undefined,
            limit: 20
          });

          if (sigs.length > 0) {
            console.log(`🔄 Backfilling ${sigs.length} txs for ${row.coinMint.slice(0, 8)}...`);
            for (const s of sigs.reverse()) {
              const tx = await connection.getTransaction(s.signature, { maxSupportedTransactionVersion: 0 });
              if (tx?.meta?.logMessages) {
                for (const log of tx.meta.logMessages) {
                  if (log.includes('Program data: ')) {
                    const buffer = Buffer.from(log.split('Program data: ')[1], 'base64');
                    if (buffer.slice(0, 8).toString('hex') === TRADE_EVENT_DISCRIMINATOR) {
                      await enqueueTrade(parseTradeEvent(buffer).mint, parseTradeEvent(buffer), s.signature);
                    }
                  }
                }
              }
              await new Promise(r => setTimeout(r, 100)); // Delay to avoid burst 429
            }
          }

          // 2. Market Stats Update
          const currentMC = curve.priceUsd * 1000000000;
          const newAth = Math.max(row.allTimeHighMarketCap || 0, currentMC);

          let status = row.graduation_status || 'new';
          if (curve.isComplete) status = 'graduated';
          else if (curve.curvePercentage >= 80) status = 'imminent';

          db.run(`UPDATE tokens SET 
            virtual_sol=?, virtual_token=?, real_sol=?, real_token=?, 
            is_complete=?, bondingCurveProgress=?, currentMarketPrice=?, 
            marketCap=?, allTimeHighMarketCap=?, graduationDate=?, graduation_status=?, graduation_timestamp=?
            WHERE coinMint=?`,
            [
              curve.vSol, curve.vToken, curve.rSol, curve.rToken,
              curve.isComplete ? 1 : 0, curve.curvePercentage, curve.priceUsd,
              currentMC, newAth, (curve.isComplete && !row.graduationDate) ? Date.now() : row.graduationDate,
              status, (curve.isComplete && !row.graduation_timestamp) ? Date.now() : row.graduation_timestamp,
              row.coinMint
            ], (err) => {
              if (err) console.error('❌ DB Poll Update Error:', err);
              else {
                db.run('INSERT INTO price_history (coinMint, price, timestamp) VALUES (?, ?, ?)', [row.coinMint, curve.priceUsd, Date.now()]);
              }
            });

          // 3. Holder & Dev & Sniper Calculation
          const mintPubKey = new PublicKey(row.coinMint);
          const holders = await connection.getTokenLargestAccounts(mintPubKey);
          const supply = await connection.getTokenSupply(mintPubKey);
          const total = supply.value.uiAmount || 0;

          if (total > 0) {
            const accs = await connection.getMultipleAccountsInfo(holders.value.map(h => h.address)).catch(() => []);
            let devPct = 0;
            let sniperPct = 0;
            let sniperBal = 0;
            let insiderPct = 0;
            let insiderBal = 0;
            let totalTxCount = row.transactions || 0;
            const ownerBalances: Record<string, number> = {};
            const snipers = JSON.parse(row.sniperWallets || '[]');
            const insiders = JSON.parse(row.insiderWallets || '[]');

            for (let i = 0; i < accs.length; i++) {
              const acc = accs[i];
              if (acc && acc.data.length >= 64) {
                const owner = new PublicKey(acc.data.slice(32, 64)).toString();
                const amount = Number(acc.data.readBigUInt64LE(64)) / 1e6;
                ownerBalances[owner] = (ownerBalances[owner] || 0) + amount;

                if (owner === row.dev) devPct += (amount / total) * 100;
                if (snipers.includes(owner)) {
                  sniperBal += amount;
                  sniperPct += (amount / total) * 100;
                }
                if (insiders.includes(owner)) {
                  insiderBal += amount;
                  insiderPct += (amount / total) * 100;
                }
              }
            }

            const sortedOwnersRaw = Object.entries(ownerBalances).sort((a, b) => b[1] - a[1]);
            const uniqueOwnerPubkeys = sortedOwnersRaw.map(([o]) => new PublicKey(o));
            const ownerInfos = await connection.getMultipleAccountsInfo(uniqueOwnerPubkeys).catch(() => []);

            let validOwners = 0;
            let top10OwnersSum = 0;
            for (let j = 0; j < sortedOwnersRaw.length; j++) {
              const info = ownerInfos[j];
              if (info && info.owner.equals(SYSTEM_PROGRAM_ID)) {
                top10OwnersSum += sortedOwnersRaw[j][1];
                validOwners++;
                if (validOwners === 10) break;
              }
            }
            const top10holderspercentage = (top10OwnersSum / total) * 100;

            if (totalTxCount % 10 === 0 || devPct > 0 || sniperPct > 0) {
              const solPrice = await getSolPrice();
              console.log(`📈 ${row.coinMint.slice(0, 8)}: Vol $${(row.volume * solPrice).toFixed(2)}, Holders ${holders.value.length}, Dev ${devPct.toFixed(2)}%, Sniper ${sniperPct.toFixed(2)}%, Insider ${insiderPct.toFixed(2)}%`);
            }

            db.run(`UPDATE tokens SET numHolders=?, devHoldingsPercentage=?, sniperOwnedPercentage=?, sniperTotalBalance=?, insiderOwnedPercentage=?, insiderTotalBalance=?, top10holderspercentage=? WHERE coinMint=?`,
              [holders.value.length, devPct, sniperPct, sniperBal, insiderPct, insiderBal, top10holderspercentage, row.coinMint],
              (err) => {
                if (err) console.error('❌ DB Holder Update Error:', err);
                else {
                  // Fetch updated row and broadcast
                  db.get('SELECT * FROM tokens WHERE coinMint = ?', [row.coinMint], (err, updatedRow) => {
                    if (!err && updatedRow) broadcast(updatedRow);
                  });
                }
              });
          }
        }
      } catch (err) { }
      await new Promise(r => setTimeout(r, 1000)); // Rate limit throttle
    }
    // Refresh cache for WSS monitoring
    rows.forEach(r => monitoredMints.add(r.coinMint));
  });
}

console.log('⏳ Initializing SOL Price...');
getSolPrice().then(() => {
  console.log(`🎯 Initial SOL Price Locked: $${cachedSolPrice}`);
  connectWS();
  setInterval(poll, 60000);
  server.listen(PORT, '0.0.0.0', () => console.log(`🚀 API: http://localhost:${PORT}`));
});
