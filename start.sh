#!/usr/bin/env bash
# Optional start script - Railway will run the Procfile command or Docker CMD by default.
# This script is handy for local testing.
set -euo pipefail
echo "Starting bot..."
python dhan_websocket_bot.py
