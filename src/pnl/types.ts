// src/pnl/types.ts — Wallet PnL data types

export interface TokenPnl {
  mint:               string;
  holding:            number;  // current token balance still open
  held:               number;  // total tokens bought historically
  sold:               number;  // total tokens sold historically
  sold_usd:           number;  // cumulative USD proceeds from sells
  realized:           number;  // USD profit/loss from closed trades
  unrealized:         number;  // current market value minus remaining cost basis
  total:              number;  // realized + unrealized
  total_sold:         number;  // total SOL received from sells
  total_invested:     number;  // cumulative USD spent on buys
  average_buy_amount: number;  // weighted average buy price per token (USD)
  current_value:      number;  // holding × current token price (USD)
  cost_basis:         number;  // remaining cost basis of held tokens (USD)
  first_buy_time:     number;  // epoch-ms
  last_buy_time:      number;  // epoch-ms
  last_sell_time:     number;  // epoch-ms  (0 if never sold)
  last_trade_time:    number;  // epoch-ms
  buy_transactions:   number;
  sell_transactions:  number;
  total_transactions: number;
}

export interface WalletPnlSummary {
  realized:           number;
  unrealized:         number;
  total:              number;
  totalInvested:      number;
  currentValue:       number;
  tokenCount:         number;
  buyTransactions:    number;
  sellTransactions:   number;
  totalTransactions:  number;
}

export interface WalletPnlResponse {
  wallet:    string;
  summary:   WalletPnlSummary;
  tokens:    Record<string, TokenPnl>;
  updatedAt: number;
}

/** A single normalized trade event extracted from on-chain transactions */
export interface NormalizedTrade {
  signature:   string;
  mint:        string;
  type:        "buy" | "sell";
  solAmount:   number;  // SOL paid (buy) or received (sell)
  tokenAmount: number;  // token units received (buy) or sent (sell)
  priceUsd:    number;  // USD price at trade time (solAmount * solPriceAtTime)
  timestamp:   number;  // epoch-ms from blockTime
}
