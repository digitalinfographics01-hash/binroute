module.exports = {
  apps: [
    {
      name: "task-inbox-worker",
      script: "worker_loop.py",
      interpreter: "python3",
      cwd: __dirname,
      autorestart: true,
    },
    {
      name: "task-inbox-dashboard",
      script: "dashboard.py",
      interpreter: "python3",
      cwd: __dirname,
      autorestart: true,
    },
  ],
};
