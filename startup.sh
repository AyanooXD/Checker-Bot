#!/bin/bash
# ═══════════════════════════════════════════════════════════
# AYANO X BOT — Railway Startup Script
# Initializes required files before bot starts
# ═══════════════════════════════════════════════════════════

set -e

echo "🚀 Starting AYANO X Bot..."

# Create required empty files if they don't exist
mkdir -p /app/data

[ ! -f /app/data/users.json ]         && echo "{}"  > /app/data/users.json         && echo "Created users.json"
[ ! -f /app/data/codes.json ]         && echo "{}"  > /app/data/codes.json         && echo "Created codes.json"
[ ! -f /app/data/banned_users.json ]  && echo "{}"  > /app/data/banned_users.json  && echo "Created banned_users.json"
[ ! -f /app/data/sites.txt ]          && touch /app/data/sites.txt                  && echo "Created sites.txt"
[ ! -f /app/data/proxies.txt ]        && touch /app/data/proxies.txt                && echo "Created proxies.txt"
[ ! -f /app/data/premium_users.txt ]  && touch /app/data/premium_users.txt          && echo "Created premium_users.txt"
[ ! -f /app/data/verified_users.txt ] && touch /app/data/verified_users.txt         && echo "Created verified_users.txt"

echo "✅ Files ready. Starting bot..."

# Start the bot with unbuffered output (needed for Railway logs)
exec python3 -u bot.py
