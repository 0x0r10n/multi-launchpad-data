# Solana Token Tracker - System Documentation

## 🚀 Overview
This application monitors the Solana blockchain for new token launches across multiple launchpads and tracks their trading activity, holder stats, and bonding curve progress in real-time.

## 🛠 Supported Launchpads
- **Pump.fun**: Native event-based detection.
- **Moonshot**: Native event-based detection.
- **Raydium (LaunchLab)**: Native event-based detection.
- **Meteora (DBC)**: Instruction-based detection + Keyword monitoring.
- **Bags.fm**: Instruction-based detection + Keyword monitoring + Account scanning.
- **LetsBonk**: Event-based detection.
- **Daos.fun**: Program-based detection.

## 🔍 Core Mechanisms

### 1. Launchpad Detection (`src/index.ts`)
The system uses a hybrid detection strategy:
- **Event Monitoring**: For launchpads like Pump.fun and Moonshot, it parses `Program data` logs using predefined discriminators.
- **Instruction Monitoring**: For Bags.fm and Meteora, it monitors program invocations and specific keywords (e.g., `CreateConfig`, `InitializeVirtualPool`).
- **Signature Deduplication**: A local cache (`processedSignatures`) prevents duplicate processing of transactions that involve multiple monitored programs. This significantly reduces RPC calls and avoids rate limiting.

### 2. Metadata Fetching (`fetchAndSaveMetadata`)
When a launch is detected, the system retrieves full token details:
- **Metaplex PDA Fallback**: If standard metadata is missing or "Pending", the system calculates the Metaplex Metadata PDA and performs up to 3 on-chain lookups.
- **JSON URI Fetching**: Fetches external metadata from URIs (IPFS, Arweave, etc.) with retry logic and timeouts.
- **Account Scanning**: For launchpads without clear event data (like Bags.fm), the system scans the transaction's account keys for "vanity" mint addresses (ending in `BAGS` or `pump`).

### 3. Holder & Risk Analysis (`updateHolderStats`)
- **Top 10 Holders**: Calculates what percentage of supply is held by the top 10 wallets (EOAs only, excluding system programs).
- **Sniper & Insider Detection**: 
    - **Snipers**: Wallets that buy within 15 seconds of launch.
    - **Insiders**: Wallets funded by the creator in the same transaction as the token launch.
- **Dev Holdings**: Tracks the percentage of supply held by the token creator.

### 4. WebSocket System
- **Real-time Broadcast**: New mints and price updates are broadcast to connected clients via WebSockets (`/subscribe-newmints`).
- **Graduation Tracking**: Tokens nearing the bonding curve threshold (80%+) are broadcast to `/subscribe-graduating`.

## 🗄 Database Schema (`mainnet.db`)
- **tokens**: Stores comprehensive token data, socials, risk metrics, and graduation status.
- **price_history**: Stores OHLCV-style price ticks for chart generation and 1m-24h price change calculations.

## ⚙️ environment Variables
- `SOLANA_RPC`: Solana RPC endpoint (Chainstack recommended).
- `WSS_ENDPOINT`: Solana WebSocket endpoint.
- `WSS_ENDPOINT_BACKUP_1-10`: Backup WebSocket endpoints for automatic rotation on failure.
- `PORT`: Server port (default: 3000).

---
*Documentation updated: Feb 25, 2026*
