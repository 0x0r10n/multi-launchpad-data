module.exports = {
  apps: [{
    name: "sol-indexer",
    script: "start.sh",
    interpreter: "bash",
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: "2G",
    kill_timeout: 10000,
    restart_delay: 5000,
    env: {
      NODE_ENV: "production",
      PORT: "3000",
      FORCE_COLOR: "1",
    },
    error_file: "./logs/pm2-error.log",
    out_file: "./logs/pm2-out.log",
    log_date_format: "YYYY-MM-DD HH:mm:ss",
    merge_logs: true
  }]
};
