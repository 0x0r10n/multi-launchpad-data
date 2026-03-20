module.exports = {
  apps: [{
    name: "sol-indexer",
    script: "src/index.ts",
    interpreter: "tsx",
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: "2G",
    kill_timeout: 10000,
    env: {
      NODE_ENV: "production",
      PORT: "3000",
      REDIS_URL: process.env.REDIS_URL,
      SOLANA_RPC: process.env.SOLANA_RPC,
      YELLOWSTONE_ENDPOINT: process.env.YELLOWSTONE_ENDPOINT,
      YELLOWSTONE_TOKEN: process.env.YELLOWSTONE_TOKEN
    },
    error_file: "./logs/pm2-error.log",
    out_file: "./logs/pm2-out.log",
    log_date_format: "YYYY-MM-DD HH:mm:ss",
    merge_logs: true
  }]
};
