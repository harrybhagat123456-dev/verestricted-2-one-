#!/bin/bash
# Start script for Heroku web dyno
# ONLY runs Flask for health checks — the bot runs on the worker dyno only

echo "Starting Flask web server (health check only)..."
echo "Telegram bot runs on the worker dyno, NOT here."

# Start Flask — this is ONLY for Heroku health checks
python3 app.py
