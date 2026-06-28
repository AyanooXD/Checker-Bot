╔══════════════════════════════════════════════════════════════════╗
║             AYANO X BOT — RAILWAY DEPLOYMENT GUIDE             ║
╚══════════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 1 — Upload Code to GitHub
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Option A (Recommended): GitHub
  1. Create new private GitHub repo
  2. Upload ALL files from this zip (excluding .session files)
  3. Connect repo to Railway

Option B: Railway CLI (direct upload)
  1. Install: npm install -g @railway/cli
  2. Login:   railway login
  3. Deploy:  railway init && railway up

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2 — Deploy on Railway
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Go to https://railway.app
  2. New Project → Deploy from GitHub repo
  3. Select your repo
  4. Railway auto-detects railway.toml ✅

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 3 — Set Environment Variables
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Go to: Railway Project → Variables tab → Add the following:

  ┌─────────────────────────┬──────────────────────────────────────┐
  │ Variable                │ Value                                │
  ├─────────────────────────┼──────────────────────────────────────┤
  │ API_ID                  │ 39027759                             │
  │ API_HASH                │ ea20df34f5f44c21c493eff664559ba3     │
  │ BOT_TOKEN               │ (your bot token from @BotFather)     │
  │ CHECKER_API_URL         │ https://autosh.up.railway.app/shopii │
  │ SITE_TEST_URL           │ https://autosh.up.railway.app/shopii │
  │ SHOPIFY_API_KEY         │ afuona_2026                          │
  │ RAZORPAY_API_URL        │ https://notfrrx-razorpay.up.railway  │
  │ RAZORPAY_MERCHANT_URL   │ https://razorpay.me/@mstechnomedia   │
  └─────────────────────────┴──────────────────────────────────────┘

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 4 — Add Persistent Volume (IMPORTANT!)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Railway filesystem resets on every deploy!
  Add a Volume to keep your data:

  1. Railway Project → Volumes → Add Volume
  2. Mount path: /app  (or wherever your bot files are)
  3. This saves: users.json, codes.json, sites.txt, proxies.txt

  WITHOUT volume: data is lost on every redeploy ⚠️

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 5 — Deploy
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Click Deploy
  2. Watch logs → should see "✅ Bot started successfully!"
  3. Test bot on Telegram: /start

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FILES INCLUDED IN THIS ZIP
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  bot.py           — Main bot code
  startup.sh       — Railway startup script (init files + start bot)
  railway.toml     — Railway configuration
  nixpacks.toml    — Build configuration (Python 3.12)
  Procfile         — Process definition (backup)
  requirements.txt — Python dependencies
  .env.example     — Environment variables reference

  Data files (will be auto-created on first run):
  users.json       — User plan data
  codes.json       — Redeem codes
  sites.txt        — Shopify sites list
  proxies.txt      — Proxy list
  verified_users.txt — Channel-verified users
  banned_users.json  — Banned users

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DO NOT UPLOAD (Railway will ignore, but keep them out):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  *.session        — Telegram session files (auto-regenerated)
  bot.pid          — Process ID file (auto-created)
  bot_output.log   — Log file

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TROUBLESHOOTING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ❌ Bot not responding → Check BOT_TOKEN env variable
  ❌ API errors        → Check CHECKER_API_URL env variable  
  ❌ Session error     → Delete *.session files from Volume, redeploy
  ❌ Data lost         → Add Railway Volume (Step 4)
  ❌ Import error      → pip install -r requirements.txt manually

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
