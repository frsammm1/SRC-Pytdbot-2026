"""
config.py  –  Centralised configuration and runtime state.

v7.0 changes (Pyrofork → TDLib / pytdbot[tdjson]>=0.10.1):
  • Pyrogram Client removed — bot + user clients are now pytdbot Client
    objects talking to TDLib via tdjson.
  • TDLib sessions are NOT string-based: a user's authorized TDLib
    database directory is archived (tar.gz → base64) and stored in Mongo in
    the SAME `session_string` field — the rest of the system (worker dynos,
    resume, /logout, /extract_string) works unchanged.
  • TDLib has no FloodWait exception — rate limits arrive as
    types.Error(code=429, message="Too Many Requests: retry after N").
    get_retry_after() parses them.
  • get_ptb_bot(): unchanged — the PTB (HTTP Bot API) path still lives for
    small files (<45MB) and high-frequency progress edits, exactly as
    before, since it uses a completely separate rate-limit pool.
"""

import os
import re
import logging
from dotenv import load_dotenv

load_dotenv()

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID", 0))
API_HASH  = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
ADMIN_ID  = int(os.environ.get("ADMIN_ID", 0))
PORT      = int(os.environ.get("PORT", 8080))

# ── HEROKU ────────────────────────────────────────────────────────────────────
HEROKU_API_TOKEN = os.environ.get("HEROKU_API_TOKEN")
HEROKU_APP_NAME  = os.environ.get("HEROKU_APP_NAME")
WORKER_DYNO_SIZE = os.environ.get("WORKER_DYNO_SIZE", "standard-1x")

# ── TDLIB ─────────────────────────────────────────────────────────────────────
# A single fixed encryption key so a TDLib database archived on one dyno can be
# restored and opened on ANY other dyno (login dyno → worker dyno → resume dyno).
TD_ENCRYPTION_KEY = os.environ.get(
    "TD_ENCRYPTION_KEY", f"src-pytdbot-{API_HASH or 'default'}-key"
)

# ── BOT IDENTITY (for cross-bot load balancing) ───────────────────────────────
BOT_ID   = os.environ.get("BOT_ID", "").strip()
BOT_TYPE = os.environ.get("BOT_TYPE", "P").strip().upper()
REGISTRY_REFRESH_INTERVAL = 300   # seconds (5 minutes)

# ── TRANSFER CORE ─────────────────────────────────────────────────────────────
CHUNK_SIZE   = 1 * 1024 * 1024   # 1 MB — legacy reference
REQUEST_SIZE = 512 * 1024         # 512 KB — kept for legacy reference
UPDATE_INTERVAL = 5               # seconds between general status edits
MAX_RETRIES     = 5
REQUEST_RETRIES = 10

# ── PROGRESS CALLBACK THROTTLE ────────────────────────────────────────────────
#
# TDLib fires updateFile events continuously while a file downloads/uploads.
# We forward them to progress edits throttled to 8 seconds, exactly like the
# Pyrogram 512KB-chunk callbacks were throttled before.
#
DOWNLOAD_PROGRESS_INTERVAL = 8   # seconds between download progress edits
UPLOAD_PROGRESS_INTERVAL   = 8   # seconds between upload progress edits

# ── PTB ROUTING ───────────────────────────────────────────────────────────────
#
# Files smaller than PTB_SMALL_FILE_LIMIT are sent via Telegram Bot API (PTB /
# HTTP). Bot API has its own separate rate-limit pool from MTProto/TDLib, so
# FloodWait there never affects main.py's TDLib command handlers.
#
# Files at or above this threshold are downloaded via the TDLib user client
# and re-uploaded via the TDLib bot client (MTProto).
#
# HARD PLATFORM CEILING — NOT configurable, not a bug:
#   A bot account can NEVER hold Telegram Premium, so Telegram enforces a hard
#   ~2 GiB per-file cap for bot uploads. SPLIT_FILE_THRESHOLD is hardcoded to
#   EXACTLY 2 GiB: any file <= 2 GiB goes through as ONE single file; only
#   files STRICTLY LARGER get chunked into parts.
#
PTB_SMALL_FILE_LIMIT  = 45  * 1024 * 1024    # 45 MB
SPLIT_FILE_THRESHOLD  = 2 * 1024 * 1024 * 1024   # exactly 2 GiB — hardcoded per request

# ── UPLOAD STALL TIMEOUT ───────────────────────────────────────────────────────
UPLOAD_TIMEOUT_FLOOR_SECONDS      = 600            # minimum timeout per attempt (10 min)
UPLOAD_MIN_THROUGHPUT_BYTES_PER_SEC = 200 * 1024    # assume ≥200 KB/s or it's genuinely stalled

def get_upload_timeout(file_size: int) -> float:
    """Generous, size-scaled timeout for a single upload attempt."""
    scaled = (file_size or 0) / UPLOAD_MIN_THROUGHPUT_BYTES_PER_SEC
    return max(UPLOAD_TIMEOUT_FLOOR_SECONDS, scaled)

# ── ZERO-PROGRESS PROTECTION (message range scan) ─────────────────────────────
ZERO_PROGRESS_RETRIES = 5
ZERO_PROGRESS_DELAYS  = [20, 40, 60, 90, 120]   # seconds per retry attempt

# ── INTER-FILE SLEEP ──────────────────────────────────────────────────────────
SLEEP_BETWEEN_FILES = 2    # seconds between each file transfer
SLEEP_EVERY_10      = 4    # extra pause every 10 files

# ── RELIABILITY / WATCHDOG ────────────────────────────────────────────────────
DYNO_STALE_THRESHOLD  = 150   # seconds — silent (crashed) dyno detection
WATCHDOG_INTERVAL     = 30    # seconds between watchdog sweeps
CLEANUP_INTERVAL      = 1800  # seconds between expired-subscription sweeps
RETRY_AFTER_FAILURE_SECONDS = 20   # first retry pause after a failure ("kuch pal")
STUCK_BACKOFF_MAX     = 1800  # cap (30 min) on backoff for a message that keeps
                               # failing — retried forever, never skipped

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── RUNTIME STATE ─────────────────────────────────────────────────────────────
active_sessions  = {}
global_stop_flag = False

# ── TDLIB ERROR HELPERS ───────────────────────────────────────────────────────

_RETRY_AFTER_RE = re.compile(r'retry after\s+(\d+)', re.IGNORECASE)

def is_error(res) -> bool:
    """True if a pytdbot call returned a TDLib Error instead of a result."""
    try:
        from pytdbot import types
        return isinstance(res, types.Error)
    except Exception:
        return False

def get_retry_after(err) -> int:
    """
    Extract the retry-after seconds from a TDLib 429 error
    ('Too Many Requests: retry after 35'). Returns 0 if not a 429.
    """
    if getattr(err, 'code', None) == 429:
        m = _RETRY_AFTER_RE.search(getattr(err, 'message', '') or '')
        if m:
            return int(m.group(1))
        return 20
    m = _RETRY_AFTER_RE.search(str(err))
    return int(m.group(1)) if m else 0

def err_text(err) -> str:
    """Compact human-readable text for a TDLib Error (or any exception)."""
    return f"{getattr(err, 'code', '')} {getattr(err, 'message', '')}".strip() or str(err)

def is_auth_error(err) -> bool:
    """True if the error means the user session/auth key is dead."""
    msg = (getattr(err, 'message', '') or str(err)).upper()
    code = getattr(err, 'code', 0)
    return (
        code == 401
        or 'AUTH_KEY_UNREGISTERED' in msg
        or 'SESSION_EXPIRED' in msg
        or 'USER_DEACTIVATED' in msg
        or 'AUTH_KEY_INVALID' in msg
    )

# ── PTB SINGLETON ─────────────────────────────────────────────────────────────
#
# One PTB Bot object per process, lazily created on first use.
# PTB uses HTTP — completely separate rate-limit infra from TDLib MTProto.
#
_ptb_bot_instance = None

async def get_ptb_bot():
    """
    Return the process-level PTB Bot singleton, initialising it on first call.
    Generous HTTPXRequest timeouts avoid the 'duplicate small file' bug.
    """
    global _ptb_bot_instance
    if _ptb_bot_instance is None:
        from telegram import Bot
        from telegram.request import HTTPXRequest
        request = HTTPXRequest(
            connect_timeout=60,
            read_timeout=300,
            write_timeout=300,
            pool_timeout=60,
        )
        _ptb_bot_instance = Bot(token=BOT_TOKEN, request=request)
        await _ptb_bot_instance.initialize()
        logger.info("⚡ PTB Bot singleton initialised")
    return _ptb_bot_instance
