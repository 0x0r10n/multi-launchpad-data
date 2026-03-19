import { Connection, PublicKey } from "@solana/web3.js";
import fetch from "node-fetch";

const connection = new Connection("https://api.mainnet-beta.solana.com");
const MOONSHOT = new PublicKey("MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG");

// Read a u64 (little-endian) from buffer at offset
function readU64(buf: Buffer, offset: number): number {
  const lo = buf.readUInt32LE(offset);
  const hi = buf.readUInt32LE(offset + 4);
  return hi * 0x100000000 + lo;
}

async function main() {
    const res = await fetch("https://api.dexscreener.com/latest/dex/search?q=moonshot");
    const data = await res.json() as any;
    const pair = data.pairs?.find((p: any) => p.dexId === "moonshot" || p.url.includes("moonshot") || p.labels?.includes("moonshot"));
    
    if (!pair) {
        console.log("No pair found on DexScreener. Using hardcoded MOONSHOT mint");
        // We need a known moonshot token. Example: MOODENG? No, moodeng is pump.
        // Let's rely on finding one.
        return;
    }
    const mintStr = pair.baseToken.address;
    console.log("Mint:", mintStr);

    const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("bonding-curve"), new PublicKey(mintStr).toBuffer()],
        MOONSHOT
    );
    console.log("Curve PDA:", pda.toBase58());

    const accountInfo = await connection.getAccountInfo(pda);
    if (!accountInfo) {
        console.log("No account info found for Curve PDA.");
        return;
    }

    const d = accountInfo.data;
    console.log("Data length:", d.length);
    
    // Parse using our curve-tracker moonshot logic
    if (d.length >= 24) {
      const virtualTokenReserves = readU64(d, 8);
      const virtualSolReserves = readU64(d, 16);
      let realTokenReserves = 0;
      let realSolReserves = 0;
      if (d.length >= 40) {
        realTokenReserves = readU64(d, 24);
        realSolReserves = readU64(d, 32);
      }
      const curvePercentage = Math.min(100, Number((BigInt(virtualSolReserves) * BigInt(100)) / BigInt(100_000_000_000)));

      console.log({
        virtualTokenReserves,
        virtualSolReserves,
        realTokenReserves,
        realSolReserves,
        curvePercentage
      });
    } else {
      console.log("Data too small to parse");
    }
}
main().catch(console.error);
