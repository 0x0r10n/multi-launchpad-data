// src/pnl/pnl-engine.ts — Weighted-average cost basis PnL computation engine
//
// Takes normalized trades and computes per-token PnL using the
// weighted average cost basis method:
//   - Each buy increases position and adjusts the average cost
//   - Each sell realizes PnL at (sell price - avg cost) × quantity
//   - Unrealized PnL uses current market price vs remaining cost basis
//
// Results are cached in Redis for instant API reads:
//   wallet:pnl:{wallet}         — full WalletPnlResponse JSON
//   wallet:pnl:{wallet}:{mint}  — individual TokenPnl JSON

import { NormalizedTrade, TokenPnl, WalletPnlSummary, WalletPnlResponse } from "./types";
import { scanWalletTrades } from "./wallet-scanner";
import Redis from "ioredis";
import "dotenv/config";

const redis = new Redis(process.env.REDIS_URL!);

/**
 * Compute full wallet PnL from trade history.
 *
 * Flow:
 *   1. Fetch normalized trades via wallet-scanner (cached or fresh)
 *   2. Group trades by mint
 *   3. For each mint, compute weighted-average cost basis PnL
 *   4. Fetch current prices from token:{mint} hash for unrealized PnL
 *   5. Aggregate into summary
 *   6. Cache in Redis for 2 minutes
 */
export async function computeWalletPnl(wallet: string): Promise<WalletPnlResponse> {
  // Check cache first
  const cached = await redis.get(`wallet:pnl:${wallet}`);
  if (cached) {
    return JSON.parse(cached);
  }

  // Fetch trade history
  const trades = await scanWalletTrades(wallet);

  // Group trades by mint, sorted by timestamp (oldest first for FIFO/avg processing)
  const tradesByMint = new Map<string, NormalizedTrade[]>();
  for (const trade of trades) {
    let arr = tradesByMint.get(trade.mint);
    if (!arr) { arr = []; tradesByMint.set(trade.mint, arr); }
    arr.push(trade);
  }

  // Sort each mint's trades oldest-first
  for (const arr of tradesByMint.values()) {
    arr.sort((a, b) => a.timestamp - b.timestamp);
  }

  // Fetch current prices for all mints in one pipeline
  const mints = [...tradesByMint.keys()];
  const pricePipe = redis.pipeline();
  for (const mint of mints) {
    pricePipe.hmget(`token:${mint}`, "priceUsd", "priceQuote");
  }
  const priceResults = await pricePipe.exec();

  const currentPrices = new Map<string, number>();
  for (let i = 0; i < mints.length; i++) {
    const [err, fields] = priceResults?.[i] || [null, null];
    const [priceUsd] = (fields as string[]) || [];
    currentPrices.set(mints[i], parseFloat(priceUsd || "0") || 0);
  }

  // Compute PnL for each token
  const tokens: Record<string, TokenPnl> = {};
  const summary: WalletPnlSummary = {
    realized: 0,
    unrealized: 0,
    total: 0,
    totalInvested: 0,
    currentValue: 0,
    tokenCount: 0,
    buyTransactions: 0,
    sellTransactions: 0,
    totalTransactions: 0,
  };

  for (const [mint, mintTrades] of tradesByMint) {
    const currentPrice = currentPrices.get(mint) || 0;
    const tokenPnl = computeTokenPnl(mint, mintTrades, currentPrice);
    tokens[mint] = tokenPnl;

    // Aggregate into summary
    summary.realized      += tokenPnl.realized;
    summary.unrealized    += tokenPnl.unrealized;
    summary.total         += tokenPnl.total;
    summary.totalInvested += tokenPnl.total_invested;
    summary.currentValue  += tokenPnl.current_value;
    summary.tokenCount++;
    summary.buyTransactions  += tokenPnl.buy_transactions;
    summary.sellTransactions += tokenPnl.sell_transactions;
    summary.totalTransactions += tokenPnl.total_transactions;
  }

  const response: WalletPnlResponse = {
    wallet,
    summary,
    tokens,
    updatedAt: Date.now(),
  };

  // Cache for 2 minutes
  await redis.setex(`wallet:pnl:${wallet}`, 120, JSON.stringify(response));

  // Also cache individual token PnLs for the token-specific endpoint
  const tokenPipe = redis.pipeline();
  for (const [mint, tokenPnl] of Object.entries(tokens)) {
    tokenPipe.setex(`wallet:pnl:${wallet}:${mint}`, 120, JSON.stringify(tokenPnl));
  }
  await tokenPipe.exec();

  return response;
}

/**
 * Get PnL for a specific token. Tries token-specific cache first,
 * falls back to full wallet computation.
 */
export async function getTokenPnl(wallet: string, mint: string): Promise<TokenPnl | null> {
  // Try token-specific cache
  const cached = await redis.get(`wallet:pnl:${wallet}:${mint}`);
  if (cached) return JSON.parse(cached);

  // Compute full wallet PnL (this will populate token caches)
  const full = await computeWalletPnl(wallet);
  return full.tokens[mint] || null;
}

/**
 * Compute PnL for a single token using weighted-average cost basis.
 *
 * Weighted average: when you buy at different prices, the cost basis
 * is the total cost / total units. When you sell, realized PnL is
 * (sell proceeds - avg_cost × units_sold).
 */
function computeTokenPnl(
  mint: string,
  trades: NormalizedTrade[],
  currentPriceUsd: number,
): TokenPnl {
  let holding = 0;           // current token balance
  let held = 0;              // total tokens ever bought
  let sold = 0;              // total tokens ever sold
  let soldUsd = 0;           // total USD received from sells
  let totalInvested = 0;     // total USD spent on buys
  let costBasis = 0;         // total USD cost of currently held tokens
  let realized = 0;          // total realized PnL (USD)
  let totalSoldSol = 0;      // total SOL received from sells

  let firstBuyTime = 0;
  let lastBuyTime = 0;
  let lastSellTime = 0;
  let lastTradeTime = 0;
  let buyTxns = 0;
  let sellTxns = 0;

  for (const trade of trades) {
    lastTradeTime = trade.timestamp;

    if (trade.type === "buy") {
      buyTxns++;
      if (firstBuyTime === 0) firstBuyTime = trade.timestamp;
      lastBuyTime = trade.timestamp;

      // Add to position
      holding += trade.tokenAmount;
      held    += trade.tokenAmount;
      totalInvested += trade.priceUsd;
      costBasis     += trade.priceUsd;

    } else { // sell
      sellTxns++;
      lastSellTime = trade.timestamp;

      // Compute weighted average cost per token
      const avgCostPerToken = holding > 0 ? costBasis / holding : 0;

      // Realized PnL = sell proceeds - cost of sold tokens
      const sellProceeds  = trade.priceUsd;
      const costOfSold    = avgCostPerToken * trade.tokenAmount;
      const tradeRealized = sellProceeds - costOfSold;

      realized += tradeRealized;

      // Reduce position
      holding   -= trade.tokenAmount;
      sold      += trade.tokenAmount;
      soldUsd   += trade.priceUsd;
      totalSoldSol += trade.solAmount;
      costBasis -= costOfSold;

      // Clamp to zero in case of floating-point drift
      if (holding < 0) holding = 0;
      if (costBasis < 0) costBasis = 0;
    }
  }

  // Unrealized PnL = current market value - remaining cost basis
  const currentValue = holding * currentPriceUsd;
  const unrealized   = currentValue - costBasis;

  // Average buy price per token
  const avgBuyAmount = held > 0 ? totalInvested / held : 0;

  return {
    mint,
    holding,
    held,
    sold,
    sold_usd:           soldUsd,
    realized,
    unrealized,
    total:              realized + unrealized,
    total_sold:         totalSoldSol,
    total_invested:     totalInvested,
    average_buy_amount: avgBuyAmount,
    current_value:      currentValue,
    cost_basis:         costBasis,
    first_buy_time:     firstBuyTime,
    last_buy_time:      lastBuyTime,
    last_sell_time:     lastSellTime,
    last_trade_time:    lastTradeTime,
    buy_transactions:   buyTxns,
    sell_transactions:  sellTxns,
    total_transactions: buyTxns + sellTxns,
  };
}
