module.exports = {
  apps: [
    {
      name: "tracker-api",
      script: "api.py",
      interpreter: "/root/judgment-download-tracker/.venv/bin/python",
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        PYTHONUNBUFFERED: "1",
        API_HOST: "0.0.0.0",
      },
    },
  ],
};
