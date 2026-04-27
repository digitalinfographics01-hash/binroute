/**
 * PM2 ecosystem config for BinRoute.
 *
 * Two processes:
 *   binroute        — data platform (imports, analytics, UI) on port 3001
 *   binroute-router — AI routing engine (shadow decisions) on port 3002
 *
 * Usage:
 *   pm2 start ecosystem.config.js
 *   pm2 restart binroute          (safe — router stays up)
 *   pm2 restart binroute-router   (safe — data platform stays up)
 */
module.exports = {
  apps: [
    {
      name: 'binroute',
      script: 'server.js',
      cwd: '/opt/binroute',
      env: {
        PORT: 3001,
        NODE_ENV: 'production',
      },
      max_memory_restart: '512M',
      restart_delay: 2000,
    },
    {
      name: 'binroute-router',
      script: 'router.js',
      cwd: '/opt/binroute',
      env: {
        ROUTER_PORT: 3002,
        NODE_ENV: 'production',
      },
      max_memory_restart: '256M',
      restart_delay: 1000,
    },
  ],
};
