import Redis from "ioredis";

const redis = new Redis("redis://localhost:6379");
async function main() {
    const keys = await redis.keys("token:*");
    let found = 0;
    for (const k of keys) {
        const platform = await redis.hget(k, "platform");
        if (platform === "moonshot") {
            const data = await redis.hgetall(k);
            console.log("Found:", data);
            found++;
        }
    }
    console.log("Total moonshot tokens found:", found);
    process.exit(0);
}
main();
