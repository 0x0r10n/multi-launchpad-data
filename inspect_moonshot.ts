import { Connection, PublicKey } from "@solana/web3.js";
import fetch from "node-fetch";

const connection = new Connection("https://api.mainnet-beta.solana.com");
const MOONSHOT = new PublicKey("MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG");

async function main() {
    // 1. Fetch a recent Moonshot token from DexScreener
    const res = await fetch("https://api.dexscreener.com/latest/dex/search?q=moonshot");
    const data = await res.json() as any;
    const pair = data.pairs?.find((p: any) => p.dexId === "moonshot" || p.url.includes("moonshot") || p.labels?.includes("moonshot"));
    
    if (!pair) { console.log("No pair found"); return; }
    const mintStr = pair.baseToken.address;
    console.log("Mint:", mintStr);

    // 2. Derive PDA
    const [pda] = PublicKey.findProgramAddressSync(
        [Buffer.from("bonding-curve"), new PublicKey(mintStr).toBuffer()],
        MOONSHOT
    );
    console.log("Curve PDA:", pda.toBase58());

    // 3. Get Account info
    const info = await connection.getAccountInfo(pda);
    if (!info) {
        // Maybe derivation is different? Let's try dex screener pair address?
        console.log("No account info for curve PDA. Pair address:", pair.pairAddress);
        const pairInfo = await connection.getAccountInfo(new PublicKey(pair.pairAddress));
        if (pairInfo) {
            console.log("Pair data length:", pairInfo.data.length);
            console.log("Pair hex:", pairInfo.data.toString("hex"));
        }
        return;
    }
    
    const d = info.data;
    console.log("Length:", d.length);
    console.log("Data hex:", d.toString("hex").substring(0, 128) + "...");

    // Try reading some common offsets
    // u64 = 8 bytes. Let's dump the first 10 u64s
    for (let i = 0; i < Math.min(d.length - 8, 80); i += 8) {
        console.log(`Offset ${i}: ${d.readBigUInt64LE(i)}`);
    }
}
main().catch(console.error);
