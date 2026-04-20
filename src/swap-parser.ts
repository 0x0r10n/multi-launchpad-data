// src/swap-parser.ts — Precise per-swap extraction directly from Geyser raw data.
// Replaces the earlier full-transaction.ts approach with directional token delta logic.
import bs58 from "bs58";

const COMPUTE_BUDGET = "ComputeBudget111111111111111111111111111111";

export interface SwapEvent {
  signature:           string;
  mint:                string;
  maker:               string;
  type:                "buy" | "sell";
  solAmount:           number;   // exact SOL sent/received (positive)
  tokenAmount:         number;   // UI token amount (positive)
  decimals:            number;
  feeLamports:         number;
  priorityFeeLamports: number;
  slot:                number;
  success:             boolean;
}

// 30-second signature dedup — Geyser re-delivers txs on reconnect
const cache = new Map<string, { result: SwapEvent; expiresAt: number }>();
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of cache) if (now > v.expiresAt) cache.delete(k);
}, 30_000);

/**
 * Parse a Yellowstone wrapper into a precise SwapEvent.
 *
 * Directional token delta strategy:
 *   buy  → find account with the largest POSITIVE token delta (maker's ATA gained tokens)
 *   sell → find account with the largest NEGATIVE token delta (maker's ATA lost tokens)
 *
 * This avoids the ambiguity of "largest absolute delta" which accidentally matches the
 * curve's ATA on sells and gives the wrong account on inner-CPI platforms (LaunchLab,
 * Meteora DBC, LetsBonk). Pre/post token balances reflect the final post-CPI state,
 * so we never need to trace individual inner instructions.
 *
 * @param wrapper  Raw Geyser SubscribeUpdate wrapper (wrapper.transaction = txInfo)
 * @param mint     Already-detected mint (from postTokenBalances in extractMint)
 * @param swapType Already-detected "buy" | "sell" (from parser.detectSwap)
 * @param maker    Account index 0 of the transaction (signer wallet)
 */
export function parseSwap(
  wrapper:  any,
  mint:     string,
  swapType: "buy" | "sell",
  maker:    string,
): SwapEvent {
  const txInfo = wrapper.transaction;
  const sig    = txInfo?.signature ? bs58.encode(txInfo.signature) : "unknown";

  const hit = cache.get(sig);
  if (hit && Date.now() < hit.expiresAt) return hit.result;

  const meta    = txInfo?.meta    || {};
  const message = txInfo?.transaction?.message;

  // Resolve full account key list: static + ALT-loaded addresses
  const rawKeys: Uint8Array[] = message?.accountKeys            || [];
  const loadedW: Uint8Array[] = meta?.loadedWritableAddresses   || [];
  const loadedR: Uint8Array[] = meta?.loadedReadonlyAddresses   || [];
  const accountKeys = [...rawKeys, ...loadedW, ...loadedR].map((k: any) => bs58.encode(k));

  // ── SOL delta ─────────────────────────────────────────────────────────────
  const pre  = (meta.preBalances  || []) as number[];
  const post = (meta.postBalances || []) as number[];
  const feeLamports = Number(meta.fee ?? 5_000);

  // signer's lamport change (positive = sent SOL, i.e. a buy)
  const makerDeltaLamports = Number(pre[0] ?? 0) - Number(post[0] ?? 0);
  // Net SOL paid to the protocol, excluding the base tx fee
  const solAmount = Math.max(0, makerDeltaLamports - feeLamports) / 1_000_000_000;

  // ── Priority fee ──────────────────────────────────────────────────────────
  // ComputeBudget SetComputeUnitPrice: discriminator 0x03, then 8-byte LE u64 microlamports/CU
  let priorityMicroLamports = 0;
  for (const ix of (message?.instructions || [])) {
    const pid = ix.programIdIndex != null ? accountKeys[ix.programIdIndex] : "";
    if (pid !== COMPUTE_BUDGET) continue;
    const data = Buffer.from(ix.data || []);
    if (data.length >= 9 && data[0] === 0x03) {
      priorityMicroLamports = data.readUInt32LE(1) + data.readUInt32LE(5) * 0x100000000;
    }
  }
  const computeUnits        = Number(meta.computeUnitsConsumed ?? 0);
  const priorityFeeLamports = Math.floor((priorityMicroLamports * computeUnits) / 1_000_000);

  // ── Token amount ──────────────────────────────────────────────────────────
  const preTB  = (meta.preTokenBalances  || []) as any[];
  const postTB = (meta.postTokenBalances || []) as any[];

  const preMap = new Map<number, number>();
  for (const b of preTB) {
    if (b.mint === mint)
      preMap.set(b.accountIndex, parseFloat(b.uiTokenAmount?.uiAmount ?? "0"));
  }

  let tokenAmount = 0;
  let decimals    = 6;
  let bestDelta   = 0; // sentinel: for buy seek max-positive, for sell seek max-negative

  for (const b of postTB) {
    if (b.mint !== mint) continue;
    const postAmt = parseFloat(b.uiTokenAmount?.uiAmount ?? "0");
    const preAmt  = preMap.get(b.accountIndex) ?? 0;
    const delta   = postAmt - preAmt;

    if (swapType === "buy"  && delta > bestDelta) {
      bestDelta   = delta;
      tokenAmount = delta;
      decimals    = b.uiTokenAmount?.decimals ?? 6;
    } else if (swapType === "sell" && delta < bestDelta) {
      bestDelta   = delta;
      tokenAmount = Math.abs(delta);
      decimals    = b.uiTokenAmount?.decimals ?? 6;
    }
  }

  const result: SwapEvent = {
    signature:           sig,
    mint,
    maker,
    type:                swapType,
    solAmount:           parseFloat(solAmount.toFixed(9)),
    tokenAmount:         parseFloat(tokenAmount.toFixed(decimals)),
    decimals,
    feeLamports,
    priorityFeeLamports,
    slot:                Number(wrapper.slot ?? 0),
    success:             !(meta.err),
  };

  cache.set(sig, { result, expiresAt: Date.now() + 30_000 });
  return result;
}
