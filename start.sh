#!/bin/bash
# Start script for Render / Heroku web dyno.
#
# main.py now starts the Flask health-check server in a daemon thread
# BEFORE loading Telegram plugins. This ensures Render's 60-second
# health check passes (~2s to bind) while plugins load in the background.
#
# Single process: Flask (health check) + Pyrogram bot (commands)
# This is required for Render web services — a worker-only dyno would
# fail Render's deploy-time TCP probe.

echo "Starting bot (main.py — runs Flask health check + Telegram bot in one process)..."
exec python3 main.py
