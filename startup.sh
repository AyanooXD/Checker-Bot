#!/bin/bash
# ═══════════════════════════════════════════════════════════
# AYANO X BOT — Railway Startup Script
# Initializes required files before bot starts
# ═══════════════════════════════════════════════════════════

set -e

echo "🚀 Starting AYANO X Bot..."

# Create required empty files if they don't exist
[ ! -f users.json ]         && echo "{}"  > users.json         && echo "Created users.json"
[ ! -f codes.json ]         && echo "{}"  > codes.json         && echo "Created codes.json"
[ ! -f banned_users.json ]  && echo "{}"  > banned_users.json  && echo "Created banned_users.json"
[ ! -f sites.txt ]          && touch sites.txt                  && echo "Created sites.txt"
[ ! -f proxies.txt ]        && touch proxies.txt                && echo "Created proxies.txt"
[ ! -f premium_users.txt ]  && touch premium_users.txt          && echo "Created premium_users.txt"
[ ! -f verified_users.txt ] && touch verified_users.txt         && echo "Created verified_users.txt"

echo "✅ Files ready. Starting bot..."

# Start the bot with unbuffered output (needed for Railway logs)
exec python3 -u bot.py
