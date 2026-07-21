# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

import os, re, time, asyncio, json, random, copy, gc, ctypes
from datetime import datetime
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import Message, PollOption, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import PollType, ParseMode
from pyrogram.errors import UserNotParticipant, ChatIdInvalid, PeerIdInvalid, ChannelPrivate, FloodWait


class AuthKeyUnregisteredError(Exception):
    """Raised when Telegram returns AUTH_KEY_UNREGISTERED — the session is dead.
    This is a FATAL error for the batch: the userbot session has been revoked
    by Telegram and ALL API calls will fail until the user re-logs in.
    The batch must stop immediately instead of failing on every single message.
    """
    pass


def _is_auth_key_error(error):
    """Check if an error indicates the userbot session auth key is invalid/revoked.
    
    Returns True for AUTH_KEY_UNREGISTERED and similar fatal session errors.
    These errors mean EVERY subsequent API call will also fail — there's no
    point in retrying individual messages. The user must re-login.
    """
    err_str = str(error).lower()
    return (
        "auth_key_unregistered" in err_str
        or "key is not registered" in err_str
        or "auth key" in err_str and "unregistered" in err_str
        or "authorization key" in err_str
        or "session_revoked" in err_str
        or "session_expired" in err_str
    )

from config import API_ID, API_HASH, LOG_GROUP, STRING, FORCE_SUB, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID
from utils.func import get_user_data, screenshot, thumbnail, get_video_metadata
from utils.func import get_user_data_key, process_text_with_rules, is_premium_user, is_auth_user, E, save_user_data
from shared_client import app as X
from plugins.settings import rename_file
from plugins.start import subscribe as sub
from plugins.pin_map import handle_pin_mirror, startup_pin, detect_pin_from_msg, verify_and_sync_pins
from plugins.upload_queue import verify_upload, batch_verify_uploads, count_sanity_check, BATCH_VERIFY_INTERVAL
from plugins.verify_and_resume import (
    clear_upload_status, save_batch_state, mark_batch_complete,
    load_batch_state, clear_batch_state,
    auto_resume_verify, get_failed_uploads,
    post_batch_verify, bulk_verify_dest, split_into_batches,
    batch_heartbeat, batch_checkpoint_heartbeat, startup_auto_resume_check,
    RESUME_LOOKBACK, BATCH_OVERLAP
)
from utils.custom_filters import login_in_progress
from utils.encrypt import dcs
from typing import Dict, Any, Optional

# PERMANENT: RAM monitoring at key batch lifecycle points
from utils.ram_monitor import log_ram
from plugins.simple_rewriter import SimpleRewriter


async def _flood_wait_stop(uid, wait_secs, user_chat_id=None):
    """Handle FloodWait by STOPPING the batch immediately.
    
    No auto-resume. Notifies the user with the FloodWait duration and
    a continue link so they can restart manually after the wait clears.
    Mappings are flushed to MongoDB so no data is lost.
    
    IMPORTANT: Cleans up ACTIVE_USERS and rate limiter state IMMEDIATELY
    so /batch command responds right away. The finally block in the batch
    function also calls remove_active_batch() (idempotent), but we do it
    here first so the user doesn't see "active task" if they try /batch.
    """
    from scheduler import scheduler

    duration = _format_duration(wait_secs)
    print(f"[FLOOD] uid={uid} — FloodWait {duration}, STOPPING batch (no auto-resume)")

    # ── IMMEDIATE CLEANUP: Remove from ACTIVE_USERS so /batch responds ──
    # This MUST happen before the notification attempt — if the notification
    # itself gets FloodWait, the user might try /batch immediately, and
    # without this cleanup, is_user_active() would still return True.
    await remove_active_batch(uid)
    batch_tasks.pop(uid, None)
    clear_cancel_flag(uid)
    Z.pop(uid, None)
    # Clear rate limiter state so next batch starts fresh
    _rate_limiter.clear()
    _download_rate_limiter.clear()
    print(f"[FLOOD] uid={uid} — ACTIVE_USERS cleaned up immediately, /batch should respond now")

    # Cancel any pending scheduler resume (safety cleanup)
    scheduler.unregister(uid)

    # Notify user — NO auto-resume, just tell them when they can restart.
    # Use safe_reply style: if FloodWait prevents notification, don't block.
    if user_chat_id:
        try:
            continue_hint = f"\n\nAfter {duration}, use /batch to continue from where you left off."
            
            # Try to send, but don't block if FloodWait is active
            try:
                await X.send_message(
                    user_chat_id,
                    f"🛑 **Batch stopped — Flood Wait {duration}**\n\n"
                    f"Telegram rate-limited the bot. The batch has been stopped to prevent further issues.\n"
                    f"All progress has been saved.{continue_hint}\n\n"
                    f"Use /stop to fully clean up, or wait {duration} and restart with /batch."
                )
            except FloodWait:
                # FloodWait on PM too — can't notify user right now.
                # ACTIVE_USERS is already cleaned up, so /batch will work.
                print(f"[FLOOD] Could not notify uid={uid} — FloodWait on PM too, skipping notification")
        except Exception as e:
            print(f"[FLOOD] Could not notify uid={uid}: {e}")


# ═══════════════════════════════════════════════════════════════
# BATCH SEND RATE — 6 messages per minute (10 seconds per msg)
# ═══════════════════════════════════════════════════════════════
# 6/min (10s delay). Bot makes send + edit buttons + answer topic
# = ~2-3 API calls per message, so effective API rate is ~12-18/min.
# This stays well under Telegram's ~20 sustained API calls/min limit.
#
# IMPORTANT: 5s delay (12/min) was causing repeated FloodWait errors
# because 12 msgs × 3 API calls = 36 calls/min exceeds Telegram's
# sustained limit. 10s delay is the minimum safe rate.
BATCH_SEND_RATE  = 6                         # messages per minute
BATCH_SEND_DELAY = 60.0 / BATCH_SEND_RATE    # 10 seconds between messages

# ═══════════════════════════════════════════════════════════════
# BATCH COOLDOWN — pause after every N messages to prevent
# sustained-rate FloodWait. Telegram tracks long-term patterns,
# not just per-minute rates. A periodic cooldown prevents the
# "slow burn" FloodWait that happens after 30-50+ messages.
#
# Two-tier cooldown:
#   SHORT: every 30 messages, pause 30s (quick breather)
#   LONG:  every 100 messages, pause 60s (full rest)
# Kept light so testing small batches doesn't feel like a freeze.
# The per-call rate limiter (3.33s/call) is the primary defence;
# cooldowns are just a safety net for very long runs.
# ═══════════════════════════════════════════════════════════════
BATCH_COOLDOWN_EVERY_SHORT    = 30   # short cooldown every 30 messages
BATCH_COOLDOWN_DURATION_SHORT = 30   # 30 seconds pause
BATCH_COOLDOWN_EVERY_LONG     = 100  # long cooldown every 100 messages
BATCH_COOLDOWN_DURATION_LONG  = 60   # 60 seconds pause

# ═══════════════════════════════════════════════════════════════
# FLOOD WAIT AUTO-RECOVERY
# Short FloodWaits (≤ AUTO_WAIT_MAX seconds) are waited out and
# retried automatically — no batch stop, no manual restart needed.
# Long FloodWaits (> AUTO_WAIT_MAX) stop the batch so the user
# isn't left waiting silently for minutes.
# ═══════════════════════════════════════════════════════════════
FLOOD_AUTO_WAIT_MAX = 120   # auto-wait FloodWaits ≤ 120 s
FLOOD_AUTO_WAIT_MAX_RETRIES = 3  # max auto-retry attempts per send call

async def _batch_cooldown_check(j, uid):
    """Check if a cooldown pause is needed after processing message j.
    
    Two-tier cooldown:
    - SHORT: Every 15 messages, pause 90 seconds
    - LONG: Every 50 messages, pause 180 seconds (overrides short)
    
    This prevents the "slow burn" FloodWait that Telegram applies when
    it detects sustained API activity over a long period, even if the
    per-minute rate is technically within limits.
    
    Checks the cancel flag so /stop still works during cooldown.
    """
    # Long cooldown takes priority (50 is divisible by... well, just check both)
    if BATCH_COOLDOWN_EVERY_LONG > 0 and (j + 1) % BATCH_COOLDOWN_EVERY_LONG == 0:
        duration = BATCH_COOLDOWN_DURATION_LONG
        tier = "LONG"
    elif BATCH_COOLDOWN_EVERY_SHORT > 0 and (j + 1) % BATCH_COOLDOWN_EVERY_SHORT == 0:
        duration = BATCH_COOLDOWN_DURATION_SHORT
        tier = "SHORT"
    else:
        return
    
    print(f"[COOLDOWN-{tier}] uid={uid} — pausing {duration}s after {j+1} messages")
    # Sleep in chunks so /stop can interrupt
    remaining = duration
    while remaining > 0:
        if should_cancel(uid):
            print(f"[COOLDOWN-{tier}] uid={uid} — cancelled during cooldown")
            return
        chunk = min(remaining, 5)
        await asyncio.sleep(chunk)
        remaining -= chunk
    print(f"[COOLDOWN-{tier}] uid={uid} — cooldown complete, resuming")

# ═══════════════════════════════════════════════════════════════
# IN-MEMORY BUTTON TRACKER — avoids get_messages() before edit
# When buttons are added to a message, we track them here so we
# don't need to fetch the message to read its current keyboard.
# Key: (chat_id, msg_id) → list of rows, each row = list of (label, url)
# ═══════════════════════════════════════════════════════════════
_button_tracker: Dict[tuple, list] = {}

def _track_buttons(chat_id, msg_id, rows):
    """Store button rows for a message in memory."""
    _button_tracker[(chat_id, msg_id)] = rows

def _get_tracked_buttons(chat_id, msg_id):
    """Retrieve tracked button rows for a message. Returns [] if none."""
    return _button_tracker.get((chat_id, msg_id), [])

def _clear_tracked_buttons(chat_id=None):
    """Clear tracked buttons. If chat_id given, only clear that chat's entries."""
    if chat_id is None:
        _button_tracker.clear()
    else:
        keys_to_remove = [k for k in _button_tracker if k[0] == chat_id]
        for k in keys_to_remove:
            del _button_tracker[k]

# ═══════════════════════════════════════════════════════════════
# GLOBAL RATE LIMITER — prevents FloodWait across ALL API calls
# Telegram allows ~20 sustained API calls/min to the same chat.
# The bot makes send + edit buttons + answer topic posts, so we
# need to rate-limit ALL calls, not just the main send.
# With 10s between messages and ~2-3 API calls per message, effective
# rate is ~12-18/min. We set the per-call interval to ~3.33s (18/min)
# to balance speed with FloodWait prevention.
# ═══════════════════════════════════════════════════════════════
API_RATE_LIMIT = 18  # max API calls per minute to same chat
API_CALL_INTERVAL = 60.0 / API_RATE_LIMIT  # ~3.33s between API calls

class _ChatRateLimiter:
    """Per-chat rate limiter. Ensures API calls to the same chat are spaced out.
    
    Uses PER-CHAT locks instead of a global lock. A global asyncio.Lock()
    blocked ALL coroutines (including command handlers) when the batch was
    sleeping inside the lock. Per-chat locks only block coroutines targeting
    the SAME chat, so command responses (which go to the user's PM, not the
    dest channel) are never blocked.
    """
    
    def __init__(self):
        self._last_call_time: Dict[int, float] = {}  # chat_id → last API call timestamp
        self._chat_locks: Dict[int, asyncio.Lock] = {}  # per-chat locks
    
    def _get_lock(self, chat_id: int) -> asyncio.Lock:
        """Get or create a lock for a specific chat."""
        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]
    
    async def acquire(self, chat_id: int):
        """Wait until it's safe to make an API call to this chat.
        
        Lock is RELEASED before sleeping so other chat_ids are never blocked.
        Even per-chat locks shouldn't be held during sleep — the lock is only
        used to atomically check/update the timestamp.
        """
        while True:
            lock = self._get_lock(chat_id)
            async with lock:
                now = time.time()
                last = self._last_call_time.get(chat_id, 0)
                elapsed = now - last
                if elapsed >= API_CALL_INTERVAL:
                    # Safe to proceed — reserve this time slot and return
                    self._last_call_time[chat_id] = now
                    return
                # Need to wait — calculate how long
                wait_time = API_CALL_INTERVAL - elapsed
            # Sleep OUTSIDE the lock — other coroutines (including command
            # handlers targeting different chat_ids) can run freely
            await asyncio.sleep(wait_time)
            # Loop back and re-check (another coroutine may have taken the slot)
    
    def clear(self, chat_id: int = None):
        """Clear rate limit state."""
        if chat_id is None:
            self._last_call_time.clear()
            self._chat_locks.clear()
        else:
            self._last_call_time.pop(chat_id, None)
            self._chat_locks.pop(chat_id, None)

# Global rate limiter instance
_rate_limiter = _ChatRateLimiter()

# ═══════════════════════════════════════════════════════════════
# DOWNLOAD RATE LIMITER — prevents FloodWait on download_media
# Telegram rate-limits file downloads separately from sends.
# Downloads are per-account/per-DC, not per-chat. A separate
# limiter ensures download_media calls are spaced out.
# Set to 10s between downloads (6/min) to prevent PDFs from
# downloading too fast and triggering FloodWait.
# ═══════════════════════════════════════════════════════════════
DL_RATE_LIMIT = 6  # max downloads per minute (6/min = 10s between)
DL_CALL_INTERVAL = 60.0 / DL_RATE_LIMIT  # 10.0s between downloads
DL_FLOOD_WAIT_THRESHOLD = 60  # auto-wait for download FloodWaits <= this many seconds

class _DownloadRateLimiter:
    """Rate limiter for download_media calls.
    
    File downloads are rate-limited by Telegram per-account/per-DC,
    not per-chat. Uses a simple lock + timestamp approach.
    
    IMPORTANT: The lock is RELEASED before sleeping, so other coroutines
    (including command handlers) are NEVER blocked. Only the timestamp
    check is inside the lock — the actual sleep happens outside it.
    """
    
    def __init__(self):
        self._last_download_time: float = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Wait until it's safe to make a download_media call.
        
        Lock is released BEFORE sleeping so command handlers never block.
        """
        while True:
            async with self._lock:
                now = time.time()
                elapsed = now - self._last_download_time
                if elapsed >= DL_CALL_INTERVAL:
                    # Safe to proceed — reserve this time slot and return
                    self._last_download_time = now
                    return
                # Need to wait — calculate how long
                wait_time = DL_CALL_INTERVAL - elapsed
            # Sleep OUTSIDE the lock — other coroutines can run freely
            await asyncio.sleep(wait_time)
            # Loop back and re-check (another coroutine may have taken the slot)
    
    def clear(self):
        """Clear rate limit state."""
        self._last_download_time = 0

# Global download rate limiter instance
_download_rate_limiter = _DownloadRateLimiter()

async def _rate_limited_call(coro_factory, chat_id, description="api_call"):
    """Execute an API call with rate limiting and FloodWait handling.
    
    Args:
        coro_factory: Callable that returns a coroutine (like flood_wait_retry expects)
        chat_id: The chat_id this call targets (for per-chat rate limiting)
        description: Description for logging
    
    Returns:
        The result of the API call
    
    Raises:
        FloodWait: Re-raised so batch can stop
        asyncio.CancelledError: Re-raised for /stop to work
    """
    await _rate_limiter.acquire(chat_id)
    try:
        if callable(coro_factory) and not asyncio.iscoroutine(coro_factory):
            coro = coro_factory()
        else:
            coro = coro_factory
        return await coro
    except FloodWait:
        raise  # Let batch handle it
    except asyncio.CancelledError:
        raise


async def _download_with_retry(client, message, file_name, progress=None, progress_args=None,
                                max_wait=DL_FLOOD_WAIT_THRESHOLD, max_retries=3):
    """Download a file with rate limiting and auto-wait for short FloodWaits.
    
    Unlike send operations where FloodWait immediately stops the batch,
    download FloodWaits are common and recoverable. This function:
    1. Rate-limits downloads via _download_rate_limiter (prevents FloodWait)
    2. Auto-waits for short FloodWaits (<= max_wait seconds) instead of stopping batch
    3. Re-raises long FloodWaits (> max_wait) so the batch stops cleanly
    
    Args:
        client: Pyrogram client to use for download
        message: Message object to download media from
        file_name: Path to save the downloaded file
        progress: Progress callback function
        progress_args: Args for progress callback
        max_wait: Max FloodWait seconds to auto-wait (default: 180s = 3 min)
        max_retries: Max retry attempts for FloodWait
    
    Returns:
        Downloaded file path, or None on failure
    
    Raises:
        FloodWait: If FloodWait > max_wait seconds (batch should stop)
        asyncio.CancelledError: If /stop is used
    """
    for attempt in range(max_retries):
        try:
            # Rate-limit before downloading
            await _download_rate_limiter.acquire()
            return await client.download_media(message, file_name=file_name,
                                                progress=progress, progress_args=progress_args)
        except FloodWait as e:
            wait_secs = e.value if hasattr(e, 'value') else 30
            if wait_secs <= max_wait:
                duration = _format_duration(wait_secs)
                print(f"[DL-FLOOD] download_media FloodWait {duration} — auto-waiting (attempt {attempt+1}/{max_retries}, threshold={max_wait}s)")
                await asyncio.sleep(wait_secs + 2)  # +2s buffer for Telegram's clock drift
                continue  # Retry after waiting
            else:
                # Too long to auto-wait — re-raise so batch stops
                duration = _format_duration(wait_secs)
                print(f"[DL-FLOOD] download_media FloodWait {duration} — EXCEEDS {max_wait}s threshold, stopping batch")
                raise
        except asyncio.CancelledError:
            raise  # /stop must work immediately
        except Exception:
            raise  # Other errors pass through
    
    # All retries exhausted
    print(f"[DL-FLOOD] download_media failed after {max_retries} FloodWait retries")
    return None


# ═══════════════════════════════════════════════════════════════
# MONGODB FLUSH STRATEGY — single write at batch end
# MongoDB's role is ONLY to store fetch maps and upload maps.
# During batch processing, ALL data is held in memory (msg_id_map)
# and flushed ONCE at the end. No per-message MongoDB writes.
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# EXPLANATION DEBUG LOG — writes to disk in real-time, survives restarts
# Use /explanlogs command to retrieve the log file
# ═══════════════════════════════════════════════════════════════
_EXPLAN_DEBUG_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'explan_debug.log')
_EXPLAN_DEBUG_MAX_LINES = 5000  # Keep last 5000 lines, auto-trim

def _edlog(msg):
    """Write to explanation debug log file (disk, real-time, survives restart).
    
    Every call appends a timestamped line to explan_debug.log.
    Also prints to stdout so it shows up in /logs too.
    Auto-trims to last _EXPLAN_DEBUG_MAX_LINES to prevent unbounded growth.
    """
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        # Always print to stdout (captured by /logs)
        print(line)
        # Also write to disk file
        with open(_EXPLAN_DEBUG_LOG, 'a', encoding='utf-8') as f:
            f.write(line + "\n")
        # Auto-trim: check line count every 100 writes
        if not hasattr(_edlog, '_count'):
            _edlog._count = 0
        _edlog._count += 1
        if _edlog._count % 100 == 0:
            try:
                with open(_EXPLAN_DEBUG_LOG, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                if len(lines) > _EXPLAN_DEBUG_MAX_LINES:
                    with open(_EXPLAN_DEBUG_LOG, 'w', encoding='utf-8') as f:
                        f.writelines(lines[-_EXPLAN_DEBUG_MAX_LINES:])
            except Exception:
                pass
    except Exception:
        pass  # Never let debug logging break the bot


def _extract_flood_wait_local(error):
    """Extract FloodWait seconds from an error (local copy for batch.py).
    
    Handles Pyrogram FloodWait, string patterns like 'FLOOD_WAIT_492', etc.
    """
    # Method 1: Pyrogram FloodWait with .value attribute
    try:
        if isinstance(error, FloodWait):
            return getattr(error, 'value', None)
    except Exception:
        pass
    
    # Method 2: Check for .seconds or .value attribute directly
    for attr in ('seconds', 'value'):
        val = getattr(error, attr, None)
        if isinstance(val, int) and val > 0:
            return val
    
    # Method 3: Parse error string for common FloodWait patterns
    error_str = str(error)
    patterns = [
        r'A wait of (\d+) seconds',
        r'FloodWait[:\s_]+(\d+)',
        r'flood.wait[:\s_]+(\d+)',
        r'wait of (\d+) second',
        r'Slowmode: wait (\d+)',
        r'try again in (\d+) second',
        r'FLOOD_WAIT[_\s]*(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, error_str, re.IGNORECASE)
        if match:
            return int(match.group(1))
    
    return None


# ═══════════════════════════════════════════════════════════════
# POLL — Native Quiz Poll + 💡 View Answer
# 💡 View Answer: Telegraph page (attached to poll at send time)
# 📖 View Explanation: DISABLED — removed per user request
# ═══════════════════════════════════════════════════════════════


# ── TELEGRAPH ANSWER PAGE — offline-proof answer reveal ──
_telegraph_token = None
_telegraph_token_index = 0  # Tracks how many tokens we've created (for rotation)

async def _get_telegraph_token():
    """Get or create Telegraph access token. Persists to file."""
    global _telegraph_token
    if _telegraph_token:
        return _telegraph_token
    # Try loading from file
    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'telegraph_token.txt')
    try:
        if os.path.exists(token_path):
            with open(token_path, 'r') as f:
                _telegraph_token = f.read().strip()
            if _telegraph_token:
                return _telegraph_token
    except Exception:
        pass
    # Create new account
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post('https://api.telegra.ph/createAccount', json={
                'short_name': 'QuizBot',
                'author_name': 'Answer Key'
            }) as resp:
                data = await resp.json()
                if data.get('ok'):
                    _telegraph_token = data['result']['access_token']
                    try:
                        with open(token_path, 'w') as f:
                            f.write(_telegraph_token)
                    except Exception:
                        pass
                    print(f"[TELEGRAPH] Account created, token saved")
    except Exception as e:
        print(f"[TELEGRAPH] Failed to create account: {e}")
    return _telegraph_token


async def _rotate_telegraph_token():
    """Rotate the Telegraph token by creating a brand new account.
    
    Called when Telegraph API returns FLOOD_WAIT_xxx.
    Telegraph rate-limits per access token — creating a new account
    gives us a fresh token with reset rate limits.
    
    Returns the new token, or None on failure.
    """
    global _telegraph_token, _telegraph_token_index
    _telegraph_token_index += 1
    
    print(f"[TELEGRAPH] 🔄 Rotating token (rotation #{_telegraph_token_index})...")
    
    # Create a new account with a unique short_name to avoid conflicts
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post('https://api.telegra.ph/createAccount', json={
                'short_name': f'QuizBot{_telegraph_token_index}',
                'author_name': 'Answer Key'
            }) as resp:
                data = await resp.json()
                if data.get('ok'):
                    old_token = _telegraph_token
                    _telegraph_token = data['result']['access_token']
                    # Save new token to file (overwrite old one)
                    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'telegraph_token.txt')
                    try:
                        with open(token_path, 'w') as f:
                            f.write(_telegraph_token)
                    except Exception:
                        pass
                    print(f"[TELEGRAPH] ✅ Token rotated successfully (old={old_token[:8] if old_token else 'None'}... "
                          f"new={_telegraph_token[:8]}...)")
                    return _telegraph_token
                else:
                    print(f"[TELEGRAPH] ❌ Token rotation FAILED: {data.get('error', 'unknown')}")
    except Exception as e:
        print(f"[TELEGRAPH] ❌ Token rotation error: {e}")
    return None


def _is_telegraph_flood_wait(response_data):
    """Check if Telegraph API response indicates a FloodWait error.
    
    Telegraph returns errors like:
    - {"ok": false, "error": "FLOOD_WAIT_1415"}
    - {"ok": false, "error": "FLOOD_WAIT_30"}
    
    Returns the wait seconds if FloodWait detected, None otherwise.
    """
    if not isinstance(response_data, dict):
        return None
    error_str = response_data.get('error', '')
    if not error_str:
        return None
    # Parse FLOOD_WAIT_XXX pattern
    fw_seconds = _extract_flood_wait_local(error_str)
    return fw_seconds

def _get_fallback_telegraph_token():
    """No fallback tokens — returns None."""
    return None

def _detect_image_type(file_path):
    """Detect actual image format from magic bytes. Returns (ext, mime) tuple."""
    try:
        with open(file_path, 'rb') as f:
            header = f.read(12)
        if header[:8] == b'\x89PNG\r\n\x1a\n':
            return ('.png', 'image/png')
        elif header[:3] == b'GIF':
            return ('.gif', 'image/gif')
        elif header[:4] == b'RIFF' and header[8:12] == b'WEBP':
            return ('.webp', 'image/webp')
        elif header[:2] == b'\xff\xd8':
            return ('.jpg', 'image/jpeg')
        # Default to jpeg — most common for Telegram photos
        return ('.jpg', 'image/jpeg')
    except Exception:
        return ('.jpg', 'image/jpeg')


async def _upload_telegraph_photo(client, photo_msg, watermark_text=None):
    """Download a photo from Telegram and upload to Telegraph. Returns Telegraph file URL or None.
    
    Handles FLOOD_WAIT by waiting and retrying (upload endpoint doesn't use tokens,
    so rotation doesn't help — we just wait out the FloodWait).
    
    Args:
        client: Pyrogram client to download from
        photo_msg: Message containing the photo
        watermark_text: Optional text to overlay as watermark before upload
    """
    msg_id = getattr(photo_msg, 'id', '???')
    actual_path = None
    
    # Download the file ONCE — retries use the same downloaded file
    try:
        import aiohttp
        download_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'downloads')
        os.makedirs(download_dir, exist_ok=True)
        download_base = os.path.join(download_dir, f'poll_img_{msg_id}')
        
        await _download_rate_limiter.acquire()
        actual_path = await client.download_media(photo_msg, file_name=download_base)
        if not actual_path or not os.path.exists(actual_path):
            print(f"[TELEGRAPH] Photo download FAILED for msg={msg_id}")
            return None
        
        ext, mime = _detect_image_type(actual_path)
        if not actual_path.lower().endswith(ext):
            new_path = actual_path + ext
            os.rename(actual_path, new_path)
            actual_path = new_path
        
        # ── Apply watermark to explanation image before upload ──
        try:
            wm = watermark_text or _DEFAULT_WATERMARK_TEXT
            if wm and wm.strip():
                _apply_watermark(actual_path, wm)
        except Exception as _wm_err:
            print(f"[TELEGRAPH] Watermark failed for msg={msg_id}: {_wm_err}")
        
        with open(actual_path, 'rb') as f:
            file_bytes = f.read()
    except Exception as e:
        print(f"[TELEGRAPH] Photo download/read error for msg={msg_id}: {e}")
        if actual_path:
            try: os.remove(actual_path)
            except: pass
        return None
    
    # Upload with FloodWait retry
    max_retries = 3
    try:
        for attempt in range(max_retries + 1):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': '*/*',
            }
            
            import aiohttp
            async with aiohttp.ClientSession(headers=headers) as session:
                data = aiohttp.FormData()
                data.add_field('file', file_bytes, filename=f'photo{ext}', content_type=mime)
                async with session.post('https://telegra.ph/upload?source=bugtracker', data=data) as resp:
                    result = json.loads(await resp.text())
                    
                    if isinstance(result, dict) and 'src' in result:
                        return f"https://telegra.ph{result['src']}"
                    elif isinstance(result, list) and result and isinstance(result[0], dict) and 'src' in result[0]:
                        return f"https://telegra.ph{result[0]['src']}"
                    else:
                        err = result.get('error', '') if isinstance(result, dict) else (result[0].get('error', '') if result else 'unknown')
                        # Check for FloodWait in upload response
                        fw_seconds = _extract_flood_wait_local(str(err))
                        if fw_seconds is not None and attempt < max_retries:
                            wait = min(fw_seconds + 5, 300)  # Cap at 5 minutes
                            print(f"[TELEGRAPH] Upload FLOOD_WAIT_{fw_seconds} for msg={msg_id} "
                                  f"(attempt {attempt+1}/{max_retries+1}) — waiting {wait}s")
                            await asyncio.sleep(wait)
                            continue  # Retry upload with same file bytes
                        print(f"[TELEGRAPH] Upload FAILED for msg={msg_id}: {err}")
                        return None
    except Exception as e:
        fw_seconds = _extract_flood_wait_local(e)
        if fw_seconds is not None:
            print(f"[TELEGRAPH] Upload FloodWait ({fw_seconds}s) for msg={msg_id} — max retries exceeded")
        else:
            print(f"[TELEGRAPH] Upload error for msg={msg_id}: {e}")
    finally:
        if actual_path:
            try: os.remove(actual_path)
            except: pass
    return None


# ═══════════════════════════════════════════════════════════════
# WATERMARK — diagonal tiled overlay on images and videos
# Covers source watermarks (like "AchieveCAPF") completely
# ═══════════════════════════════════════════════════════════════

_DEFAULT_WATERMARK_TEXT = "THE ENLIGHTER FROM HARRY"  # default watermark


def _apply_watermark(image_path, watermark_text):
    """Watermark DISABLED — returns image unchanged.
    Previously applied a bottom bar watermark; now skipped for speed.
    """
    return image_path  # Watermark removed — no processing

    # ── OLD WATERMARK CODE (disabled) ──
    if not watermark_text or not watermark_text.strip():
        return image_path  # nothing to apply

    try:
        from PIL import Image, ImageDraw, ImageFont

        img = Image.open(image_path).convert("RGBA")
        w, h = img.size

        # ── Font size proportional to image width ──
        font_size = max(16, min(72, int(w * 0.05)))

        # Try to load a decent font
        font = None
        font_paths = [
            '/usr/share/fonts/truetype/english/Carlito-Bold.ttf',
            '/usr/share/fonts/truetype/english/Tinos-Bold.ttf',
            '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',
            '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
            '/usr/share/fonts/truetype/freefont/FreeSansBold.ttf',
        ]
        for fp in font_paths:
            if os.path.exists(fp):
                try:
                    font = ImageFont.truetype(fp, font_size)
                    break
                except Exception:
                    continue
        if font is None:
            font = ImageFont.load_default()

        # ── Measure text for bar height ──
        tmp_draw = ImageDraw.Draw(Image.new("RGBA", (1, 1), (0, 0, 0, 0)))
        bbox = tmp_draw.textbbox((0, 0), watermark_text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]

        pad_y = int(font_size * 0.5)
        bar_h = text_h + pad_y * 2

        # ── Draw full-width semi-transparent dark bar at bottom ──
        overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        draw.rectangle([0, h - bar_h, w, h], fill=(0, 0, 0, 160))

        # ── Center the watermark text in the bar ──
        text_x = (w - text_w) // 2
        text_y = h - bar_h + pad_y
        draw.text((text_x, text_y), watermark_text, font=font, fill=(255, 255, 255, 230))

        # Composite watermark bar onto original
        result = Image.alpha_composite(img, overlay)

        # ── Save back ──
        ext = os.path.splitext(image_path)[1].lower()
        if ext in ('.jpg', '.jpeg'):
            result.convert("RGB").save(image_path, "JPEG", quality=92)
        elif ext == '.webp':
            result.save(image_path, "WEBP", quality=92)
        elif ext == '.png':
            result.save(image_path, "PNG")
        else:
            result.convert("RGB").save(image_path, "JPEG", quality=92)

        print(f"[WATERMARK] Applied bottom-bar '{watermark_text}' to {image_path}")
        return image_path

    except Exception as e:
        print(f"[WATERMARK] Failed to apply watermark to {image_path}: {e}")
        import traceback; traceback.print_exc()
        return image_path  # return original path — don't block the upload


async def _apply_video_watermark(video_path, watermark_text):
    """Watermark DISABLED — returns video unchanged.
    Previously applied ffmpeg drawtext watermark; now skipped for speed.
    """
    return video_path  # Watermark removed — no processing

    # ── OLD WATERMARK CODE (disabled) ──
    if not watermark_text or not watermark_text.strip():
        return video_path

    try:
        # Probe video dimensions
        probe_cmd = [
            'ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'csv=s=x:p=0', video_path
        ]
        proc = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        dims = stdout.decode().strip().split('x')
        v_w = int(dims[0]) if len(dims) == 2 and dims[0].isdigit() else 1280
        v_h = int(dims[1]) if len(dims) == 2 and dims[1].isdigit() else 720

        # Font size proportional to video width
        font_size = max(16, min(48, int(v_w * 0.04)))

        # Find a suitable font
        font_file = '/usr/share/fonts/truetype/english/Carlito-Bold.ttf'
        if not os.path.exists(font_file):
            font_file = '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf'
        if not os.path.exists(font_file):
            font_file = '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'

        # ── Single centered drawtext at the bottom ──
        margin = int(font_size * 0.3)
        border_w = int(font_size * 0.5)

        drawtext = (
            f"drawtext=fontfile={font_file}:text='{watermark_text}'"
            f":fontsize={font_size}:fontcolor=white@0.9"
            f":x=(w-text_w)/2:y=h-text_h-{margin}"
            f":box=1:boxcolor=black@0.65:boxborderw={border_w}"
        )

        # Output to temp file, then replace original
        tmp_path = video_path + '_wm.mp4'

        cmd = [
            'ffmpeg', '-y', '-i', video_path,
            '-vf', drawtext,
            '-c:a', 'copy',
            '-preset', 'fast',
            '-crf', '23',
            tmp_path
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)

        if proc.returncode == 0 and os.path.exists(tmp_path):
            os.replace(tmp_path, video_path)
            print(f"[WATERMARK] Applied bottom-bar '{watermark_text}' to video {video_path}")
            return video_path
        else:
            err = stderr.decode()[:300] if stderr else 'unknown'
            print(f"[WATERMARK] ffmpeg failed for {video_path}: {err}")
            if os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except: pass
            return video_path  # return original — don't block

    except asyncio.TimeoutError:
        print(f"[WATERMARK] ffmpeg timed out for {video_path}")
        return video_path
    except Exception as e:
        print(f"[WATERMARK] Video watermark error for {video_path}: {e}")
        return video_path  # return original — don't block


async def _apply_watermark_to_file(file_path, watermark_text, is_video=False, is_image=False):
    """Watermark DISABLED — returns file unchanged.
    Previously dispatched to image/video watermark; now skipped for speed.
    """
    return file_path  # Watermark removed — no processing

    # ── OLD WATERMARK CODE (disabled) ──
    if not watermark_text or not watermark_text.strip():
        return file_path

    ext = os.path.splitext(file_path)[1].lower()
    image_exts = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tiff'}
    video_exts = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.ogv'}

    if is_image or ext in image_exts:
        return _apply_watermark(file_path, watermark_text)
    elif is_video or ext in video_exts:
        return await _apply_video_watermark(file_path, watermark_text)
    else:
        # Documents, audio, etc — no watermark applied
        return file_path


def _text_to_telegraph_nodes(text):
    """Convert plain text to Telegraph Node objects (paragraphs with line breaks).
    
    Handles multi-line text by splitting on newlines and creating
    separate paragraph nodes for each line, with <br> for empty lines.
    """
    if not text:
        return []
    nodes = []
    lines = text.strip().split('\n')
    for line in lines:
        if line.strip():
            # Non-empty line → paragraph
            nodes.append({'tag': 'p', 'children': [line.strip()]})
        else:
            # Empty line → line break separator
            nodes.append({'tag': 'br'})
    return nodes


async def _create_answer_page(correct_letter, image_url=None, answer_text=None):
    """Create a Telegraph page revealing the correct answer. Returns URL or None.
    
    Handles FLOOD_WAIT by rotating the Telegraph token and retrying.
    Telegraph rate-limits per access token — when we hit FloodWait,
    we create a new account (fresh token with reset limits) and retry.
    """
    token = await _get_telegraph_token()
    if not token:
        print(f"[TELEGRAPH] No token — cannot create page")
        return None
    
    max_retries = 3  # Max token rotations before giving up
    for attempt in range(max_retries + 1):
        try:
            import aiohttp
            content_nodes = []
            if image_url:
                content_nodes.append({'tag': 'img', 'attrs': {'src': image_url}})
            if answer_text:
                content_nodes.extend(_text_to_telegraph_nodes(answer_text))
            content_nodes.append({
                'tag': 'h3',
                'children': ['Correct Answer: ', {'tag': 'b', 'children': [correct_letter]}]
            })
            
            async with aiohttp.ClientSession() as session:
                async with session.post('https://api.telegra.ph/createPage', json={
                    'access_token': token,
                    'title': 'Correct Answer',
                    'author_name': 'Answer Key',
                    'content': content_nodes
                }) as resp:
                    data = await resp.json()
                    if data.get('ok'):
                        url = data['result']['url']
                        print(f"[TELEGRAPH] Page created: {url}")
                        return url
                    else:
                        # Check if this is a FloodWait error
                        fw_seconds = _is_telegraph_flood_wait(data)
                        if fw_seconds is not None:
                            print(f"[TELEGRAPH] createPage FLOOD_WAIT_{fw_seconds} "
                                  f"(attempt {attempt+1}/{max_retries+1})")
                            if attempt < max_retries:
                                # Rotate token to get fresh rate limits
                                print(f"[TELEGRAPH] Rotating token to bypass FloodWait...")
                                new_token = await _rotate_telegraph_token()
                                if new_token:
                                    token = new_token
                                    # Don't wait the full FloodWait — new token has fresh limits
                                    # Just a small delay to avoid immediate re-trigger
                                    await asyncio.sleep(2)
                                    continue  # Retry with new token
                                else:
                                    # Token rotation failed — wait the FloodWait time and retry with same token
                                    wait = min(fw_seconds + 5, 300)  # Cap at 5 minutes
                                    print(f"[TELEGRAPH] Token rotation failed — waiting {wait}s and retrying with same token")
                                    await asyncio.sleep(wait)
                                    continue
                            else:
                                print(f"[TELEGRAPH] Max retries ({max_retries}) reached — giving up on this page")
                        else:
                            print(f"[TELEGRAPH] createPage FAILED: {data.get('error', 'unknown')}")
        except Exception as e:
            print(f"[TELEGRAPH] createPage error: {e}")
            # Check if the exception itself contains FloodWait info
            fw_seconds = _extract_flood_wait_local(e)
            if fw_seconds and attempt < max_retries:
                print(f"[TELEGRAPH] Exception FloodWait ({fw_seconds}s) — rotating token...")
                new_token = await _rotate_telegraph_token()
                if new_token:
                    token = new_token
                    await asyncio.sleep(2)
                    continue
            break  # Non-retryable error
    return None


# ═══════════════════════════════════════════════════════════════
# POLL MAP — DISABLED (📖 View Explanation button removed)
# Kept as stubs so explanation_listener.py imports don't break.
# ═══════════════════════════════════════════════════════════════

POLL_MAP = {}
_POLL_MAP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'poll_map.json')


def _save_poll_map():
    pass


def _load_poll_map():
    pass


# Load on import (no-op)
_load_poll_map()


async def _add_inline_button(bot_client, dest_chat_id, dest_msg_id, button_label, button_url, log_prefix="BTN"):
    """Append an inline button to an existing message in the dest channel.
    
    OPTIMIZED: Uses in-memory button tracker instead of get_messages() to read
    the current keyboard. This eliminates 1 API call per button add, halving
    the API cost of adding buttons.
    
    Falls back to get_messages() if the message is not in the tracker
    (e.g. first button on a message sent outside the current batch).
    
    Args:
        bot_client: Bot client
        dest_chat_id: Destination channel ID
        dest_msg_id: Message ID in dest channel
        button_label: Text for the new button (e.g. "📖 View Explanation")
        button_url: URL for the new button
        log_prefix: Prefix for log messages
    
    Returns:
        bool: True if button was added successfully
    """
    try:
        # ── FAST PATH: Use in-memory tracker (0 API calls to read keyboard) ──
        tracked_rows = _get_tracked_buttons(dest_chat_id, dest_msg_id)
        buttons = []
        already_exists = False
        
        if tracked_rows:
            # Reconstruct InlineKeyboardButton objects from tracked data
            for row_data in tracked_rows:
                new_row = []
                for (lbl, url) in row_data:
                    new_row.append(InlineKeyboardButton(lbl, url=url))
                    if lbl == button_label:
                        already_exists = True
                buttons.append(new_row)
            _edlog(f"[{log_prefix}] Using in-memory tracker: {len(buttons)} existing rows on msg {dest_msg_id}")
        else:
            # ── SLOW PATH: Fetch from Telegram (fallback for untracked messages) ──
            msg = await _rate_limited_call(
                lambda: bot_client.get_messages(dest_chat_id, dest_msg_id),
                dest_chat_id, f"get_messages_btn_{dest_msg_id}"
            )
            if not msg:
                _edlog(f"[{log_prefix}] ❌ Cannot find msg {dest_msg_id} in dest {dest_chat_id}")
                return False
            
            if msg.reply_markup and msg.reply_markup.inline_keyboard:
                for row in msg.reply_markup.inline_keyboard:
                    new_row = []
                    row_data = []  # Track for in-memory storage
                    for btn in row:
                        new_row.append(btn)
                        # Track this button for future calls
                        if hasattr(btn, 'url') and btn.url:
                            row_data.append((btn.text, btn.url))
                        elif hasattr(btn, 'callback_data') and btn.callback_data:
                            row_data.append((btn.text, f"callback:{btn.callback_data}"))
                        if hasattr(btn, 'text') and btn.text == button_label:
                            already_exists = True
                    buttons.append(new_row)
                # Store fetched rows in tracker for next time
                _track_buttons(dest_chat_id, dest_msg_id, 
                    [[(btn.text, btn.url) for btn in row if hasattr(btn, 'url') and btn.url]
                     for row in msg.reply_markup.inline_keyboard])
                _edlog(f"[{log_prefix}] Fetched & cached {len(buttons)} existing button rows on msg {dest_msg_id}")
            else:
                _edlog(f"[{log_prefix}] No existing keyboard on msg {dest_msg_id}")
        
        # Skip if button already exists (prevents duplicates from retries/race conditions)
        if already_exists:
            _edlog(f"[{log_prefix}] ⏭️ Button '{button_label}' already exists on msg {dest_msg_id} — skipping duplicate")
            return True  # Return True since the button IS there
        
        # Append new row with the button
        buttons.append([InlineKeyboardButton(button_label, url=button_url)])
        
        # Update in-memory tracker with the new button
        tracked_rows_copy = list(tracked_rows) if tracked_rows else []
        tracked_rows_copy.append([(button_label, button_url)])
        _track_buttons(dest_chat_id, dest_msg_id, tracked_rows_copy)
        
        new_keyboard = InlineKeyboardMarkup(buttons)
        await _rate_limited_call(
            lambda: bot_client.edit_message_reply_markup(
                chat_id=dest_chat_id,
                message_id=dest_msg_id,
                reply_markup=new_keyboard
            ),
            dest_chat_id, f"add_btn_{dest_msg_id}"
        )
        _edlog(f"[{log_prefix}] ✅ Added '{button_label}' button to msg {dest_msg_id} → {button_url}")
        return True
    except Exception as e:
        _edlog(f"[{log_prefix}] ❌ Error adding button: {e}")
        import traceback; traceback.print_exc()
        return False


async def _add_explanation_button(bot_client, dest_chat_id, dest_msg_id, explanation_url):
    """Append a 📖 View Explanation button to an existing message.
    
    Used to add the button on the PARENT message (question image A')
    linking to the explanation (C') in the dest channel.
    """
    return await _add_inline_button(bot_client, dest_chat_id, dest_msg_id, 
                                     "📖 View Explanation", explanation_url, 
                                     log_prefix="EXPL-BTN")



def register_poll(source_chat_id, source_poll_msg_id, dest_chat_id, dest_poll_msg_id):
    """DISABLED — 📖 View Explanation button removed. Kept as stub for import compatibility."""
    pass


def _get_reply_to_id(msg):
    """Extract the reply-to message ID from a Pyrofork Message object.
    
    In Pyrofork, the reply-to info may be stored in different attributes:
      - msg.reply_to_message_id  (most common, but can be None)
      - msg.reply_to.message_id  (alternative location)
      - msg.reply_to.reply_to_msg_id  (another alternative)
    
    This helper checks all three locations, matching the pattern used
    elsewhere in the codebase for poll reply detection.
    
    Args:
        msg: Pyrofork/pyrogram Message object
    
    Returns:
        int or None: The message ID this message replies to
    """
    return (msg.reply_to_message_id
            or getattr(getattr(msg, 'reply_to', None), 'message_id', None)
            or getattr(getattr(msg, 'reply_to', None), 'reply_to_msg_id', None))


async def _copy_explanation_to_dest(user_client, bot_client, source_chat, source_msg_id, dest_chat_id,
                                      poll_dest_chat_id=None, poll_dest_msg_id=None):
    """Copy the explanation message from source channel to dest channel.
    
    Uses a fetch-and-send approach instead of copy_message() — this works
    for PRIVATE channels where copy_message() fails because the sending client
    is not a member of the source channel.
    
    Strategy:
      1. Fetch the explanation message from source using userbot (has source access)
      2. Send it to destination using bot_client (has dest access), preserving format
      3. Set reply_to_message_id=poll_dest_msg_id so it appears as a reply to the poll
    
    No "Forwarded from" header — looks like an original message.
    
    After sending, adds a 🔙 Back to Question button on the explanation
    message that navigates back to the poll in the dest channel.
    
    Args:
        user_client: Userbot/client with source channel access
        bot_client: Bot/client with dest channel access
        source_chat: Source channel ID
        source_msg_id: Message ID of explanation in source channel
        dest_chat_id: Destination channel ID
        poll_dest_chat_id: Chat ID of the poll in dest (for 🔙 button, usually same as dest_chat_id)
        poll_dest_msg_id: Message ID of the poll in dest (for 🔙 button AND reply_to_message_id)
    
    Returns:
        tuple: (dest_msg_id, dest_link) on success
               (None, source_link) on failure (falls back to source link)
    """
    # ── Step 1: Fetch the explanation message from source using userbot ──
    expl_msg = None
    if user_client:
        try:
            if hasattr(user_client, 'is_connected') and not user_client.is_connected:
                _edlog(f"[COPY-EXPL] Userbot disconnected, skipping fetch")
            else:
                resolved_src = await resolve_chat(user_client, source_chat)
                expl_msg = await user_client.get_messages(resolved_src, source_msg_id)
                if expl_msg and getattr(expl_msg, 'empty', False):
                    _edlog(f"[COPY-EXPL] Fetched explanation msg {source_msg_id} but it's empty")
                    expl_msg = None
                else:
                    _edlog(f"[COPY-EXPL] Fetched explanation msg {source_msg_id} from source (text={'YES' if expl_msg and expl_msg.text else 'NO'} photo={'YES' if expl_msg and expl_msg.photo else 'NO'} media={'YES' if expl_msg and expl_msg.media else 'NO'})")
        except Exception as e:
            _edlog(f"[COPY-EXPL] Userbot fetch failed: {e}")
            expl_msg = None
    
    # ── Step 2: Send the explanation to destination using bot_client ──
    sent = None
    if expl_msg and bot_client:
        try:
            if hasattr(bot_client, 'is_connected') and not bot_client.is_connected:
                _edlog(f"[COPY-EXPL] Bot disconnected, skipping send")
            else:
                # Build send kwargs with reply_to_message_id for reply chain
                _send_kwargs = dict(chat_id=dest_chat_id)
                if poll_dest_msg_id:
                    _send_kwargs['reply_to_message_id'] = poll_dest_msg_id
                    _edlog(f"[COPY-EXPL] Will set reply_to_message_id={poll_dest_msg_id} on explanation")
                
                # Determine message type and send accordingly — same logic as send_direct()
                ft = expl_msg.text.markdown if hasattr(expl_msg.text, 'markdown') else (str(expl_msg.text) if expl_msg.text else None)
                if ft is None:
                    ft = expl_msg.caption.markdown if hasattr(expl_msg.caption, 'markdown') else (str(expl_msg.caption) if expl_msg.caption else None)
                
                if expl_msg.poll:
                    # Explanation is itself a poll (rare) — skip, can't nest polls
                    _edlog(f"[COPY-EXPL] Explanation is a poll — skipping (can't nest polls)")
                elif expl_msg.video:
                    _kw = dict(_send_kwargs, video=expl_msg.video.file_id, caption=ft,
                               duration=expl_msg.video.duration, width=expl_msg.video.width, height=expl_msg.video.height)
                    sent = await flood_wait_retry(_safe_markdown_send(bot_client.send_video, "copy_expl_video", **_kw), "copy_expl_video", dest_chat_id=dest_chat_id)
                elif expl_msg.photo:
                    _kw = dict(_send_kwargs, photo=expl_msg.photo.file_id, caption=ft)
                    sent = await flood_wait_retry(_safe_markdown_send(bot_client.send_photo, "copy_expl_photo", **_kw), "copy_expl_photo", dest_chat_id=dest_chat_id)
                elif expl_msg.animation:
                    _kw = dict(_send_kwargs, animation=expl_msg.animation.file_id, caption=ft)
                    sent = await flood_wait_retry(_safe_markdown_send(bot_client.send_animation, "copy_expl_animation", **_kw), "copy_expl_animation", dest_chat_id=dest_chat_id)
                elif expl_msg.document:
                    _kw = dict(_send_kwargs, document=expl_msg.document.file_id, caption=ft,
                               file_name=expl_msg.document.file_name)
                    sent = await flood_wait_retry(_safe_markdown_send(bot_client.send_document, "copy_expl_document", **_kw), "copy_expl_document", dest_chat_id=dest_chat_id)
                elif expl_msg.audio:
                    _kw = dict(_send_kwargs, audio=expl_msg.audio.file_id, caption=ft,
                               duration=expl_msg.audio.duration, performer=expl_msg.audio.performer,
                               title=expl_msg.audio.title)
                    sent = await flood_wait_retry(_safe_markdown_send(bot_client.send_audio, "copy_expl_audio", **_kw), "copy_expl_audio", dest_chat_id=dest_chat_id)
                elif expl_msg.voice:
                    _kw = dict(_send_kwargs, voice=expl_msg.voice.file_id, duration=expl_msg.voice.duration)
                    sent = await flood_wait_retry(bot_client.send_voice(**_kw), "copy_expl_voice", dest_chat_id=dest_chat_id)
                elif expl_msg.sticker:
                    _kw = dict(_send_kwargs, sticker=expl_msg.sticker.file_id)
                    sent = await flood_wait_retry(bot_client.send_sticker(**_kw), "copy_expl_sticker", dest_chat_id=dest_chat_id)
                elif expl_msg.video_note:
                    _kw = dict(_send_kwargs, video_note=expl_msg.video_note.file_id, duration=expl_msg.video_note.duration)
                    sent = await flood_wait_retry(bot_client.send_video_note(**_kw), "copy_expl_video_note", dest_chat_id=dest_chat_id)
                elif expl_msg.text:
                    # Text-only explanation
                    _kw = dict(_send_kwargs, text=ft)
                    sent = await flood_wait_retry(_safe_markdown_send(bot_client.send_message, "copy_expl_text", **_kw), "copy_expl_text", dest_chat_id=dest_chat_id)
                else:
                    _edlog(f"[COPY-EXPL] Unknown explanation message type — no text/media to send")
                
                if sent:
                    _edlog(f"[COPY-EXPL] ✅ Sent explanation to dest: msg_id={sent.id}")
        except Exception as e:
            _edlog(f"[COPY-EXPL] Bot send failed: {e}")
            sent = None
    
    # ── Step 3: If fetch-and-send failed, try copy_message as last resort ──
    # copy_message works when the client IS a member of both source and dest
    # (e.g., userbot that has joined the destination channel)
    if not sent:
        _copy_kwargs = dict(
            chat_id=dest_chat_id,
            from_chat_id=source_chat,
            message_id=source_msg_id,
        )
        if poll_dest_msg_id:
            _copy_kwargs['reply_to_message_id'] = poll_dest_msg_id
        
        # Try userbot first
        if user_client:
            try:
                if hasattr(user_client, 'is_connected') and not user_client.is_connected:
                    pass
                else:
                    if hasattr(user_client, 'copy_message'):
                        copied = await user_client.copy_message(**_copy_kwargs)
                        _edlog(f"[COPY-EXPL] Userbot copy_message fallback succeeded for msg {source_msg_id}")
                        if copied:
                            sent = copied
            except Exception as e:
                _edlog(f"[COPY-EXPL] Userbot copy_message fallback failed: {e}")
        
        # Try bot client
        if not sent and bot_client:
            try:
                if hasattr(bot_client, 'is_connected') and not bot_client.is_connected:
                    pass
                else:
                    if hasattr(bot_client, 'copy_message'):
                        copied = await bot_client.copy_message(**_copy_kwargs)
                        _edlog(f"[COPY-EXPL] Bot copy_message fallback succeeded for msg {source_msg_id}")
                        if copied:
                            sent = copied
            except Exception as e:
                _edlog(f"[COPY-EXPL] Bot copy_message fallback also failed: {e}")
    
    if sent:
        dest_msg_id = sent.id if hasattr(sent, 'id') else None
        if dest_msg_id:
            dest_link = _build_telegram_link(dest_chat_id, dest_msg_id)
            _edlog(f"[COPY-EXPL] ✅ Explanation in dest: msg_id={dest_msg_id} link={dest_link} reply_to={poll_dest_msg_id}")
            
            # ── 🔙 Back to Question: add on the EXPLANATION (C') → links back to POLL (B') ──
            if poll_dest_msg_id:
                try:
                    poll_chat = poll_dest_chat_id or dest_chat_id
                    poll_link = _build_telegram_link(poll_chat, poll_dest_msg_id)
                    if poll_link:
                        back_button_added = await _add_inline_button(
                            bot_client, dest_chat_id, dest_msg_id,
                            "🔙 Back to Question", poll_link,
                            log_prefix="BACK-BTN"
                        )
                        if back_button_added:
                            _edlog(f"[COPY-EXPL] ✅ 🔙 button added on expl {dest_msg_id} → poll {poll_dest_msg_id}")
                        else:
                            _edlog(f"[COPY-EXPL] ⚠️ Failed to add 🔙 button on expl {dest_msg_id}")
                except Exception as _back_e:
                    _edlog(f"[COPY-EXPL] 🔙 button error: {_back_e}")
            
            return dest_msg_id, dest_link
    
    # All methods failed — fall back to source link
    source_link = _build_telegram_link(source_chat, source_msg_id)
    _edlog(f"[COPY-EXPL] ⚠️ All methods failed, falling back to source link: {source_link}")
    return None, source_link


async def _get_correct_option(source_chat, source_msg_id, poll, user_client=None):
    """Get the correct option ID for a quiz poll.
    
    Problem: Pyrofork returns correct_option_id=None for quiz polls
    fetched from source channels because Telegram hides it until someone votes.
    
    Solution: Re-fetch using Pyrogram user client, then vote using the
    REAL option.data bytes (NOT b'\\x00') to trigger Telegram to reveal
    the correct answer. Then re-fetch to read it.
    
    KEY FIXES (learned from debug sessions):
    1. source_chat must be int() — Telethon/Pyrogram raw API treat strings as phone numbers
    2. Must use poll.options[0].data bytes, NOT b'\\x00' — OPTION_INVALID otherwise
    3. Must use Pyrogram user client (u), not Telethon — bot hasn't joined source channels
    
    Args:
        source_chat: Channel ID (string or int)
        source_msg_id: Message ID in source channel
        poll: Pyrogram Poll object
        user_client: Pyrogram user client (the 'u' param from process_msg)
    
    Returns: correct_option_id (int) or None
    """
    # Already have it — nothing to do
    if poll.correct_option_id is not None:
        print(f"[QUIZ] src_msg_id={source_msg_id} src_chat={source_chat} — Already have correct_option_id={poll.correct_option_id}")
        return poll.correct_option_id
    
    # Check if it's even a quiz (not a regular poll)
    poll_type = getattr(poll, 'type', None)
    if poll_type != PollType.QUIZ:
        print(f"[QUIZ] src_msg_id={source_msg_id} — Not a quiz (type={poll_type}) — no correct answer")
        return None
    
    # Get user client — try passed param first, then global userbot
    uc = user_client or get_Y()
    if not uc:
        print(f"[QUIZ] src_msg_id={source_msg_id} — No user client available — cannot reveal correct answer")
        return None
    
    # Resolve source_chat for API calls
    # - get_messages() accepts username strings directly (Pyrogram resolves internally)
    # - Raw API invoke() needs an InputChannel peer object
    # - Integer channel IDs may fail with PEER_ID_INVALID if not in Pyrogram's SQLite cache
    chat_ref = source_chat  # For get_messages (string or int)
    resolved_peer = None     # For raw invoke()
    try:
        source_chat = int(source_chat)
        # It's an integer — try resolve_peer to populate Pyrogram's cache
        try:
            resolved_peer = await uc.resolve_peer(source_chat)
        except Exception:
            pass
        chat_ref = source_chat
    except (ValueError, TypeError):
        print(f"[QUIZ] src_msg_id={source_msg_id} — source_chat '{source_chat}' is not an int — resolving username...")
        try:
            resolved_peer = await uc.resolve_peer(source_chat)
            print(f"[QUIZ] src_msg_id={source_msg_id} — Resolved username '{source_chat}' to peer {type(resolved_peer).__name__}")
        except Exception as e:
            print(f"[QUIZ] src_msg_id={source_msg_id} — Could not resolve username '{source_chat}': {e}")
            return None
    
    print(f"[QUIZ] src_msg_id={source_msg_id} src_chat={source_chat} chat_ref={chat_ref} — Quiz detected, uc={type(uc).__name__}")
    
    try:
        import pyrogram.raw as raw
        
        # ── STEP 1: Re-fetch via user client (might already reveal correct answer) ──
        print(f"[QUIZ] src_msg_id={source_msg_id} — Step 1: Re-fetching via user client (chat_ref={chat_ref})...")
        msg = await uc.get_messages(chat_ref, message_ids=source_msg_id)
        if not msg or not msg.poll:
            print(f"[QUIZ] src_msg_id={source_msg_id} — Re-fetch returned no poll")
            return None
        
        poll_obj = msg.poll
        q_preview = poll_obj.question[:50] if poll_obj.question else 'N/A'
        print(f"[QUIZ] src_msg_id={source_msg_id} — Re-fetched: question={q_preview}, correct_option_id={poll_obj.correct_option_id}, type={getattr(poll_obj, 'type', 'N/A')}, options={len(poll_obj.options)}")
        
        # Already revealed after re-fetch?
        if poll_obj.correct_option_id is not None:
            print(f"[QUIZ] src_msg_id={source_msg_id} — Re-fetch revealed correct_option_id={poll_obj.correct_option_id}")
            return poll_obj.correct_option_id
        
        # ── STEP 2: Vote using REAL option.data bytes to trigger reveal ──
        # CRITICAL: Each poll option has its own .data bytes — you CANNOT use b'\x00'
        # That causes OPTION_INVALID. Must use poll_obj.options[N].data
        if not poll_obj.options:
            print(f"[QUIZ] src_msg_id={source_msg_id} — Poll has no options — cannot vote")
            return None
        
        option_data = poll_obj.options[0].data
        print(f"[QUIZ] src_msg_id={source_msg_id} — Step 2: Voting with option 0 data bytes: {option_data}")
        
        # Use already-resolved peer, or resolve now
        peer = resolved_peer
        if not peer:
            try:
                peer = await uc.resolve_peer(chat_ref)
            except Exception as e:
                print(f"[QUIZ] src_msg_id={source_msg_id} — Could not resolve peer for voting: {e}")
                return None
        print(f"[QUIZ] src_msg_id={source_msg_id} — Using peer: {type(peer).__name__}")
        
        # Send vote — this reveals the correct answer for quizzes
        vote_result = await uc.invoke(
            raw.functions.messages.SendVote(
                peer=peer,
                msg_id=source_msg_id,
                options=[option_data]
            )
        )
        print(f"[QUIZ] src_msg_id={source_msg_id} — Vote result type: {type(vote_result).__name__}")
        
        await asyncio.sleep(1.5)
        
        # ── STEP 3: Re-fetch after voting — correct_option_id should now be visible ──
        print(f"[QUIZ] src_msg_id={source_msg_id} — Step 3: Re-fetching after vote...")
        msg2 = await uc.get_messages(chat_ref, message_ids=source_msg_id)
        if msg2 and msg2.poll:
            print(f"[QUIZ] src_msg_id={source_msg_id} — After vote: correct_option_id={msg2.poll.correct_option_id}")
            if msg2.poll.correct_option_id is not None:
                print(f"[QUIZ] src_msg_id={source_msg_id} — After voting, correct_option_id={msg2.poll.correct_option_id}")
                return msg2.poll.correct_option_id
        
        # ── STEP 4: Try reading correct answer from vote_result updates ──
        # Sometimes the vote response itself contains the revealed poll
        print(f"[QUIZ] src_msg_id={source_msg_id} — Step 4: Checking vote_result for correct answer...")
        if hasattr(vote_result, 'updates'):
            for update in vote_result.updates:
                update_type = type(update).__name__
                print(f"[QUIZ] src_msg_id={source_msg_id} — Update: {update_type}")
                # UpdateMessagePoll contains the poll with correct_option
                if hasattr(update, 'poll') and update.poll:
                    co = getattr(update.poll, 'correct_option', None)
                    print(f"[QUIZ] src_msg_id={source_msg_id} — UpdateMessagePoll.correct_option={co}")
                    if co is not None:
                        print(f"[QUIZ] src_msg_id={source_msg_id} — Found in vote result: correct_option={co}")
                        return co
        
        print(f"[QUIZ] src_msg_id={source_msg_id} — Vote succeeded but correct_option_id still not found")
        
    except Exception as e:
        import traceback
        print(f"[QUIZ] src_msg_id={source_msg_id} — EXCEPTION: {e}")
        traceback.print_exc()
    
    print(f"[QUIZ] src_msg_id={source_msg_id} src_chat={source_chat} — Could not reveal correct answer")
    return None


def _build_telegram_link(source_chat, message_id):
    """Build a clickable Telegram message link.
    
    Public channel (username):  https://t.me/channelname/123
    Private channel (-100xxx):  https://t.me/c/1234567/123
    
    Returns None if link can't be built.
    """
    if not message_id:
        return None
    
    chat_str = str(source_chat)
    
    # Public channel — source_chat is a username like "channelname"
    if not chat_str.lstrip('-').isdigit():
        url = f"https://t.me/{chat_str}/{message_id}"
        _edlog(f"[LINK] Built public link: {url}")
        return url
    
    # Private channel — source_chat is a numeric ID like -1001234567890
    chat_int = int(chat_str)
    if chat_int < 0:
        # Strip -100 prefix to get the ID used in t.me/c/ links
        # e.g. -1001234567890 → 1234567890
        # Only valid for supergroups/channels (IDs starting with -100)
        abs_str = str(abs(chat_int))
        if abs_str.startswith('100') and len(abs_str) > 5:
            # Supergroup/channel — strip the "100" prefix
            clean_id = abs_str[3:]
            url = f"https://t.me/c/{clean_id}/{message_id}"
            _edlog(f"[LINK] Built private link: tcid={source_chat} → clean_id={clean_id} → {url}")
            return url
        else:
            # Basic group (not -100xxx) — no valid t.me/c/ link exists
            _edlog(f"[LINK] Cannot build link — not a supergroup/channel ID: {source_chat}")
            return None
    
    # Supergroup with positive ID (rare)
    url = f"https://t.me/c/{chat_str}/{message_id}"
    _edlog(f"[LINK] Built positive ID link: {url}")
    return url


def build_inline_quiz(question, options, correct_option_id, reveal_url=None, question_url=None, explanation_url=None):
    """Build inline keyboard with 💡 View Answer button.
    
    - 💡 View Answer: Telegraph page (always added if reveal_url exists)
    - 📖 View Explanation: Added on PARENT (question image A') via _add_explanation_button()
      when explanation arrives or is found in cache. Points to C' (explanation in dest).
      NOT added on the poll itself — poll stays clean with only 💡+📺.
    """
    buttons = []
    
    # 💡 View Answer — Telegraph page, works offline, no bot needed
    if reveal_url:
        buttons.append([InlineKeyboardButton("💡 View Answer", url=reveal_url)])
    
    # Debug: log what buttons were built
    button_info = []
    if reveal_url:
        button_info.append(f"View Answer → {reveal_url[:60]}...")
    _edlog(f"[BUTTONS] Built {len(buttons)} buttons: {button_info}")
    
    keyboard = InlineKeyboardMarkup(buttons) if buttons else None
    return keyboard


# Register the callback handler for inline quiz buttons
@X.on_callback_query(filters.regex(r"^iq:"))
async def inline_quiz_callback(client, callback_query):
    """Handle inline quiz button taps — show correct/wrong popup."""
    try:
        data = callback_query.data  # iq:{correct_idx}:{tapped_idx}
        parts = data.split(":")
        if len(parts) != 3:
            await callback_query.answer("Invalid quiz data", show_alert=True)
            return
        
        correct_idx = int(parts[1])
        tapped_idx = int(parts[2])
        option_letter = chr(65 + tapped_idx) if tapped_idx < 26 else str(tapped_idx + 1)
        
        if correct_idx == -1:
            # Regular poll — no correct answer defined
            await callback_query.answer(f"📍 You selected: Option {option_letter}", show_alert=True)
        elif tapped_idx == correct_idx:
            correct_letter = chr(65 + correct_idx) if correct_idx < 26 else str(correct_idx + 1)
            await callback_query.answer(f"✅ Correct!\nAnswer: Option {correct_letter}", show_alert=True)
        else:
            correct_letter = chr(65 + correct_idx) if correct_idx < 26 else str(correct_idx + 1)
            await callback_query.answer(f"❌ Wrong!\nCorrect answer: Option {correct_letter}", show_alert=True)
    except Exception as e:
        print(f"Inline quiz callback error: {e}")
        await callback_query.answer("Error", show_alert=True)





# ═══════════════════════════════════════════════════════════════
# FLOOD WAIT AUTO-RETRY — NEVER gives up, NEVER skips a message.
# Sleeps for the required duration, then retries automatically.
# The batch resumes from where it left off after FloodWait ends.
# No cap — even 30+ min FloodWaits are waited out.
# ═══════════════════════════════════════════════════════════════
FLOOD_WAIT_MAX_RETRIES = 999  # Effectively unlimited — never give up

def _format_duration(secs):
    """Human-readable duration: '30m 15s', '1h 5m', '45s'"""
    if secs >= 3600:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    elif secs >= 60:
        return f"{secs // 60}m {secs % 60}s"
    else:
        return f"{secs}s"

def _ram_reclaim():
    """Force Python to return freed memory to the OS after large file operations.
    
    Without this, glibc's malloc keeps freed pages mapped in RSS forever.
    malloc_trim(0) forces glibc to return all free pages to the kernel.
    This is CRITICAL on Heroku where RAM is limited (1024MB).
    """
    try:
        import gc as _gc
        _before = 0
        try:
            with open('/proc/self/status') as _f:
                for _line in _f:
                    if _line.startswith('VmRSS:'):
                        _before = int(_line.split()[1]) / 1024  # MB
                        break
        except Exception:
            pass
        
        _gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass
        
        _after = 0
        try:
            with open('/proc/self/status') as _f:
                for _line in _f:
                    if _line.startswith('VmRSS:'):
                        _after = int(_line.split()[1]) / 1024  # MB
                        break
        except Exception:
            pass
        
        _freed = _before - _after
        if _freed > 5:  # Only log if significant
            print(f"[RAM-RECLAIM] RSS: {_before:.0f} → {_after:.0f} MB (freed {_freed:.0f} MB back to OS)")
    except Exception as e:
        pass  # Non-critical — never break the batch


async def _delete_after(message, delay=10):
    """Delete a message after a delay. Non-blocking — uses asyncio.create_task to schedule."""
    try:
        await asyncio.sleep(delay)
        await message.delete()
    except Exception:
        pass

async def _safe_markdown_send(send_func, description="send", **kwargs):
    """Send a message with markdown parse_mode, fallback to no parse_mode on error.
    
    This ensures clickable blue links ([text](url)) are preserved when sending
    captions/text that contain markdown-formatted links from .markdown property.
    If Pyrogram's markdown parser fails (e.g., unmatched brackets), falls back
    to sending as plain text — at least the content is delivered.
    
    Args:
        send_func: The async send function (e.g., c.send_video)
        description: Description for logging
        **kwargs: Arguments to pass to send_func (including parse_mode will be overridden)
    
    Returns:
        The sent message object
    """
    # First try with markdown parse_mode (preserves blue links)
    try:
        return await send_func(**kwargs, parse_mode=ParseMode.MARKDOWN)
    except Exception as md_err:
        _edlog(f"[MARKDOWN-FALLBACK] {description}: Markdown parse failed ({md_err}), retrying without parse_mode")
        # Fallback: send without parse_mode (links appear as plain text, but content is delivered)
        try:
            return await send_func(**kwargs)
        except Exception:
            raise


async def _safe_markdown_edit(edit_func, description="edit", **kwargs):
    """Edit a message with markdown parse_mode, fallback to no parse_mode on error.
    
    Same as _safe_markdown_send but for edit operations.
    """
    try:
        return await edit_func(**kwargs, parse_mode=ParseMode.MARKDOWN)
    except Exception as md_err:
        _edlog(f"[MARKDOWN-FALLBACK] {description}: Markdown parse failed ({md_err}), retrying without parse_mode")
        try:
            return await edit_func(**kwargs)
        except Exception:
            raise


async def resolve_peers_at_startup(user_client, bot_client, src_chat_id, dest_chat_id=None):
    """Resolve source chat peer on BOTH clients before batch starts.
    
    Prevents PEER_ID_INVALID errors during batch processing by ensuring
    both the user_client and bot have the source chat's peer cached.
    Must be called AFTER clients are connected but BEFORE any batch operations.
    
    KEY FIX: src_chat_id from E() is a STRING like '-1002563279588' for private
    channels. Pyrogram treats strings as usernames in get_chat()/resolve_peer(),
    causing PEER_ID_INVALID. We MUST convert to int before calling Pyrogram APIs.
    
    Recovery strategy:
    1. Normalize src_chat_id to int (if it's a numeric string)
    2. Try resolve_peer() first (lightweight — just caches access hash)
    3. If that fails, refresh dialogs (populates Pyrogram's internal peer cache)
    4. Retry resolve_peer() after dialog refresh
    5. Try get_chat() as final verification (validates actual access)
    6. NON-FATAL: Even if user_client fails, the batch may still work because
       resolve_peer can succeed later during actual API calls (get_messages, etc.)
       Only raise if BOTH clients fail to resolve.
    """
    # ── STEP 0: Normalize src_chat_id from string to int ──
    # E() returns '-1002563279588' as a STRING for private channels.
    # Pyrogram treats strings as usernames → PEER_ID_INVALID.
    # Must convert to int before any Pyrogram API call.
    resolved_src = src_chat_id
    if isinstance(src_chat_id, str) and src_chat_id.lstrip('-').isdigit():
        resolved_src = int(src_chat_id)
        print(f"[STARTUP] Normalized src_chat_id: '{src_chat_id}' → int {resolved_src}")
    
    # Similarly normalize dest_chat_id
    resolved_dest = dest_chat_id
    if dest_chat_id and isinstance(dest_chat_id, str) and str(dest_chat_id).lstrip('-').isdigit():
        resolved_dest = int(dest_chat_id)
    
    user_client_ok = False
    bot_client_ok = False
    
    for client_name, client in [("user_client", user_client), ("bot_client", bot_client)]:
        if not client:
            print(f"[STARTUP] {client_name}: Skipped (not available)")
            continue
        
        # Check if client is actually connected
        if hasattr(client, 'is_connected') and not client.is_connected:
            print(f"[STARTUP] {client_name}: Not connected — skipping")
            continue
        
        # ── Resolve source chat with multi-step recovery ──
        src_resolved = False
        
        # Step 1: Try resolve_peer (lightweight — just caches access hash)
        try:
            await client.resolve_peer(resolved_src)
            print(f"[STARTUP] {client_name}: resolve_peer OK for src_chat={resolved_src}")
            src_resolved = True
        except PeerIdInvalid:
            print(f"[STARTUP] {client_name}: resolve_peer PEER_ID_INVALID for src_chat={resolved_src} — trying dialog refresh...")
        except Exception as e:
            print(f"[STARTUP] {client_name}: resolve_peer failed for src_chat={resolved_src}: {e}")
        
        # Step 2: If resolve_peer failed, refresh dialogs and retry
        if not src_resolved:
            try:
                print(f"[STARTUP] {client_name}: Refreshing dialogs to populate peer cache...")
                async for _ in client.get_dialogs(limit=50):   # 50 is enough to cache recent peers
                    pass
                await client.resolve_peer(resolved_src)
                print(f"[STARTUP] {client_name}: resolve_peer OK after dialog refresh for src_chat={resolved_src}")
                src_resolved = True
            except PeerIdInvalid:
                print(f"[STARTUP] {client_name}: Still PEER_ID_INVALID after dialog refresh for src_chat={resolved_src}")
            except Exception as e:
                print(f"[STARTUP] {client_name}: resolve_peer failed after dialog refresh for src_chat={resolved_src}: {e}")
        
        # Step 3: Try get_chat as verification (also populates cache)
        if src_resolved:
            try:
                chat = await client.get_chat(resolved_src)
                print(f"[STARTUP] {client_name} ✅ resolved src_chat={resolved_src} → {getattr(chat, 'title', 'N/A')}")
            except Exception as e:
                print(f"[STARTUP] {client_name}: get_chat failed after resolve_peer OK for src_chat={resolved_src}: {e}")
                # resolve_peer succeeded — peer is cached, this is non-fatal
        else:
            # resolve_peer failed on both attempts — try get_chat directly as last resort
            try:
                chat = await client.get_chat(resolved_src)
                print(f"[STARTUP] {client_name} ✅ get_chat succeeded directly for src_chat={resolved_src} → {getattr(chat, 'title', 'N/A')}")
                src_resolved = True
            except Exception as e:
                print(f"[STARTUP] {client_name} ❌ ALL methods failed for src_chat={resolved_src}: {e}")
        
        if src_resolved:
            if client_name == "user_client":
                user_client_ok = True
            else:
                bot_client_ok = True
        
        # ── Resolve destination chat (if provided) ──
        if resolved_dest:
            try:
                await client.resolve_peer(resolved_dest)
                print(f"[STARTUP] {client_name}: dest_chat={resolved_dest} resolved OK")
            except Exception as e:
                print(f"[STARTUP] {client_name}: dest_chat={resolved_dest} resolve failed: {e}")
    
    # ── Final check: at least ONE client must be able to resolve the source ──
    if not user_client_ok and not bot_client_ok:
        # Neither client could resolve — this is fatal
        raise PeerIdInvalid(f"Neither user_client nor bot_client could resolve source channel {resolved_src}. "
                           f"Make sure the userbot is logged in and is a member of the source channel.")
    elif not user_client_ok:
        # User client failed but bot succeeded — warn but continue
        # Bot is usually NOT in the source channel, so this means user client
        # can't access it. But we don't raise — let the batch try and fail gracefully.
        print(f"[STARTUP] ⚠️ user_client could NOT resolve src_chat={resolved_src}, but bot_client did. "
              f"Batch may fail for restricted/private channel content.")
    # else: both or user_client succeeded — all good


async def flood_wait_retry(coro_factory, description="operation", max_retries=FLOOD_WAIT_MAX_RETRIES, dest_chat_id=None):
    """Execute an async operation with smart FloodWait handling.

    Short FloodWaits (≤ FLOOD_AUTO_WAIT_MAX seconds) are waited out and
    retried automatically — the batch keeps running without interruption.
    Long FloodWaits (> FLOOD_AUTO_WAIT_MAX) re-raise immediately so the
    batch stops cleanly and the user is notified.

    IMPORTANT: coro_factory must be a CALLABLE that returns a new coroutine
    each time (e.g., a lambda or partial). If you pass an already-created
    coroutine object, it can only be awaited ONCE — re-awaiting causes
    "cannot reuse already awaited coroutine" RuntimeError.

    If dest_chat_id is provided, the call is rate-limited via the global
    _rate_limiter to prevent FloodWait. This ensures EVERY API call to
    the destination channel is spaced out, not just the main send.

    Usage:
        # CORRECT — callable that creates a fresh coroutine each retry:
        await flood_wait_retry(lambda: c.send_video(...), "send_video", dest_chat_id=tcid)

        # WRONG — coroutine object, can only be awaited once:
        await flood_wait_retry(c.send_video(...), "send_video")  # BUG!
    """
    attempts = FLOOD_AUTO_WAIT_MAX_RETRIES
    for attempt in range(attempts):
        # Rate-limit before executing if dest_chat_id is provided
        if dest_chat_id is not None:
            await _rate_limiter.acquire(dest_chat_id)
        try:
            # Create fresh coroutine each attempt (MUST be callable for retry to work)
            if callable(coro_factory) and not asyncio.iscoroutine(coro_factory):
                coro = coro_factory()
            else:
                coro = coro_factory
            return await coro
        except FloodWait as e:
            wait_secs = e.value if hasattr(e, 'value') else 30
            duration = _format_duration(wait_secs)
            if wait_secs <= FLOOD_AUTO_WAIT_MAX and attempt < attempts - 1:
                # Short FloodWait — wait it out and retry automatically
                print(f"[FLOOD] {description}: FloodWait {duration} — auto-waiting then retrying "
                      f"(attempt {attempt+1}/{attempts})")
                await asyncio.sleep(wait_secs + 2)  # +2s buffer for Telegram's clock
                continue
            else:
                # Long FloodWait or retries exhausted — stop the batch
                print(f"[FLOOD] {description}: FloodWait {duration} — RE-RAISING (batch will stop)")
                raise
        except asyncio.CancelledError:
            raise  # Must always re-raise CancelledError for /stop to work
        except Exception:
            raise  # Non-FloodWait exceptions pass through immediately


async def safe_reply(message, text, **kwargs):
    """reply_text with FloodWait protection — silently fails during FloodWait.
    Used in command handlers where we can't reply during a global FloodWait."""
    try:
        return await message.reply_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[CMD-FLOOD] reply_text FloodWait {wait}s — suppressed")
        return None
    except Exception as e:
        print(f"[CMD-ERR] reply_text failed: {e}")
        return None


async def reply_with_wait(message, text, **kwargs):
    """reply_text that WAITS during FloodWait instead of silently failing.
    Use for IMPORTANT command responses that the user MUST see (e.g. /clearbatch).
    Waits up to 60s for FloodWait; gives up on longer waits or other errors."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await message.reply_text(text, **kwargs)
        except FloodWait as e:
            wait = e.value if hasattr(e, 'value') else 30
            if wait <= 60:
                print(f"[CMD-FLOOD] reply_text FloodWait {wait}s — waiting (attempt {attempt+1}/{max_retries})")
                await asyncio.sleep(wait + 1)
                continue  # Retry after waiting
            else:
                print(f"[CMD-FLOOD] reply_text FloodWait {wait}s — too long, giving up (uid={message.from_user.id if message.from_user else '?'})")
                return None
        except Exception as e:
            print(f"[CMD-ERR] reply_text failed: {e} (uid={message.from_user.id if message.from_user else '?'})")
            return None
    print(f"[CMD-ERR] reply_with_wait exhausted {max_retries} retries (uid={message.from_user.id if message.from_user else '?'})")
    return None

async def safe_edit(message, text, **kwargs):
    """edit_text with FloodWait protection.

    FloodWait from edit_text is SUPPRESSED — never re-raised.
    Status/progress message edits are NOT critical. If Telegram
    rate-limits them, we skip the edit silently and continue the batch.
    The batch must NEVER stop because of a status message edit.
    """
    if message is None:
        return None
    try:
        return await message.edit_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else 30
        print(f"[CMD-FLOOD] edit_text FloodWait {wait}s — suppressed, continuing batch")
        return None  # Skip the edit, never raise
    except Exception as e:
        print(f"[CMD-ERR] edit_text failed: {e}")
        return None


def get_Y():
    """Get the global userbot dynamically — NOT captured at import time.
    The old Y = __import__('shared_client').userbot was evaluated at import
    when userbot was still None, so it always returned None."""
    try:
        import shared_client
        return shared_client.userbot
    except Exception:
        return None

Z, P, UB, UC, emp = {}, {}, {}, {}, {}

# Force fresh start tracking — UIDs that did /clearbatch and should NOT
# resume from previous upload data. The batch startup checks this set
# and, if the UID is present, skips resume detection and msg_id_map loading.
# The UID is removed from this set after the batch starts.
_force_fresh_start_uids = set()

# TEST FEATURE: LRU client caches (replacing unbounded UB/UC dicts)
from utils.client_cache import user_bot_cache, user_client_cache

ACTIVE_USERS = {}
ACTIVE_USERS_FILE = "active_users.json"

# ═══════════════════════════════════════════════════════════════
# BATCH TASK TRACKING — enables immediate /stop via asyncio.Task.cancel()
# Without this, /stop only sets a flag that's checked at the top of each
# loop iteration. If a download is stuck (FloodWait), the flag is never
# checked and the batch appears to ignore /stop.
# ═══════════════════════════════════════════════════════════════
batch_tasks = {}  # uid (int) -> asyncio.Task

# fixed directory file_name problems 
def sanitize(filename):
    return re.sub(r'[<>:"/\\|?*\']', '_', filename).strip(" .")[:255]

def load_active_users():
    try:
        if os.path.exists(ACTIVE_USERS_FILE):
            with open(ACTIVE_USERS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception:
        return {}

async def save_active_users_to_file():
    try:
        with open(ACTIVE_USERS_FILE, 'w') as f:
            json.dump(ACTIVE_USERS, f)
    except Exception as e:
        print(f"Error saving active users: {e}")

async def add_active_batch(user_id: int, batch_info: Dict[str, Any]):
    ACTIVE_USERS[str(user_id)] = batch_info
    await save_active_users_to_file()

def is_user_active(user_id: int) -> bool:
    return str(user_id) in ACTIVE_USERS

async def update_batch_progress(user_id: int, current: int, success: int):
    if str(user_id) in ACTIVE_USERS:
        ACTIVE_USERS[str(user_id)]["current"] = current
        ACTIVE_USERS[str(user_id)]["success"] = success
        await save_active_users_to_file()


# ════════════════════════════════════════════════════════════
#  STARTUP AUTO-RESUME — called from main.py on bot startup
#
#  Finds all interrupted batches (crash, dyno restart, deploy)
#  and notifies affected users with resume instructions.
#
#  This is the function that main.py line 135 calls:
#      from plugins.batch import startup_auto_resume
#      await startup_auto_resume()
# ════════════════════════════════════════════════════════════

async def startup_auto_resume():
    """
    Called ONCE at bot startup. Checks for interrupted batches
    from the previous session and notifies affected users.
    
    How it works:
      1. Queries MongoDB for batches with status="in_progress" or
         checkpoints with status="running" and stale updated_at (> 2 min old)
      2. For each interrupted batch, sends a notification to the user
         with the batch details and instructions to resume
      3. Marks the batch state as "interrupted" so it's not picked up again
    
    Why notify instead of auto-start:
      - The batch runner (_batch_streaming) needs user session context
        (message object, user client, etc.) that doesn't exist on restart
      - The user may have intentionally stopped the batch
      - Starting automatically could conflict with other operations
    """
    try:
        interrupted = await startup_auto_resume_check()
    except Exception as e:
        print(f"[AUTO-RESUME] Failed to check for interrupted batches: {e}")
        return

    if not interrupted:
        print("[AUTO-RESUME] No interrupted batches found — all clean")
        return

    print(f"[AUTO-RESUME] Found {len(interrupted)} interrupted batch(es)")

    # Import here to avoid circular imports
    from shared_client import app as X

    for batch_info in interrupted:
        uid = batch_info.get("uid")
        source_channel = batch_info.get("source_channel", "?")
        start_msg_id = batch_info.get("start_msg_id", "?")
        total_count = batch_info.get("total_count", "?")
        last_uploaded = (
            batch_info.get("last_uploaded_src_id") or
            batch_info.get("last_completed_msg_id") or
            "?"
        )
        success_count = batch_info.get("success_count") or batch_info.get("total_completed") or "?"
        dest_channel = batch_info.get("dest_channel_id", "?")
        link_type = batch_info.get("link_type", "?")
        user_chat_id = batch_info.get("user_chat_id")

        # GUARD: Check if upload_maps still exist — if /clearbatch was used,
        # clean up stale batch_state/checkpoint and skip notification
        _upload_map_count = 0
        try:
            _upload_map_count = await upload_maps_collection.count_documents(
                {"user_id": uid, "source_channel": str(source_channel)}
            )
        except Exception:
            pass
        
        if _upload_map_count == 0:
            print(f"[AUTO-RESUME] uid={uid} source={source_channel} — "
                  f"No upload_maps found (clearbatch was used?). Cleaning up stale state.")
            try:
                await clear_batch_state(uid, str(source_channel))
                from plugins.verify_and_resume import batch_checkpoint_collection
                await batch_checkpoint_collection.delete_many(
                    {"user_id": uid, "source_channel": str(source_channel)}
                )
            except Exception:
                pass
            continue

        # Mark as interrupted so it's not picked up again on next restart
        try:
            await save_batch_state(
                user_id=uid,
                source_channel=source_channel,
                start_msg_id=int(start_msg_id) if str(start_msg_id).isdigit() else 0,
                total_count=int(total_count) if str(total_count).isdigit() else 0,
                dest_channel_id=dest_channel,
                link_type=link_type or "private",
            )
            # Update status to "interrupted"
            from plugins.verify_and_resume import batch_state_collection
            await batch_state_collection.update_one(
                {"user_id": uid, "source_channel": str(source_channel)},
                {"$set": {"status": "interrupted"}},
            )
        except Exception as e:
            print(f"[AUTO-RESUME] Failed to mark batch interrupted: {e}")

        # Notify the user
        if user_chat_id:
            try:
                await X.send_message(
                    user_chat_id,
                    f"⚠️ **Batch Interrupted**\n\n"
                    f"Your batch was interrupted (bot restart/crash).\n\n"
                    f"**Source:** `{source_channel}`\n"
                    f"**Range:** {start_msg_id} → {start_msg_id + total_count - 1 if str(total_count).isdigit() else '?'}\n"
                    f"**Last uploaded:** msg {last_uploaded}\n"
                    f"**Completed:** {success_count}/{total_count}\n"
                    f"**Destination:** `{dest_channel}`\n\n"
                    f"To resume, use:\n"
                    f"`/batch {source_channel} {last_uploaded} {total_count - (int(success_count) if str(success_count).isdigit() else 0)}`\n\n"
                    f"_The bot has saved your progress — no messages will be duplicated._",
                )
                print(f"[AUTO-RESUME] Notified uid={uid} about interrupted batch for source={source_channel}")
            except Exception as e:
                print(f"[AUTO-RESUME] Failed to notify uid={uid}: {e}")
        else:
            print(f"[AUTO-RESUME] No user_chat_id for uid={uid} — cannot notify about source={source_channel}")

async def request_batch_cancel(user_id: int):
    """Cancel a running batch — uses TRIPLE redundancy for guaranteed stop.
    
    Method 1: asyncio.Task.cancel() — IMMEDIATE stop at next await point.
    Method 2: Independent _CANCEL_FLAGS dict — survives remove_active_batch().
    Method 3: ACTIVE_USERS cancel_requested flag — legacy backup.
    
    The independent flag is critical because cancel_cmd calls remove_active_batch()
    which deletes the ACTIVE_USERS entry, making the old flag-based check useless.
    """
    cancelled = False
    
    # Method 1: Cancel the asyncio.Task directly (IMMEDIATE — works even during downloads)
    if user_id in batch_tasks:
        task = batch_tasks[user_id]
        if not task.done():
            task.cancel()
            cancelled = True
            print(f"[STOP] Cancelled asyncio.Task for uid={user_id}")
    
    # Method 2: Set the INDEPENDENT cancel flag (survives remove_active_batch deletion)
    _CANCEL_FLAGS[user_id] = True
    cancelled = True
    print(f"[STOP] Set _CANCEL_FLAGS for uid={user_id}")
    
    # Method 3: Set the ACTIVE_USERS flag (backup — checked at top of loop)
    if str(user_id) in ACTIVE_USERS:
        ACTIVE_USERS[str(user_id)]["cancel_requested"] = True
        await save_active_users_to_file()
        cancelled = True
    
    return cancelled

# Separate cancel flags dict — survives remove_active_batch() deletion.
# The old flag in ACTIVE_USERS was useless because cancel_cmd deletes
# the user from ACTIVE_USERS immediately after setting the flag.
_CANCEL_FLAGS: Dict[int, bool] = {}

def should_cancel(user_id: int) -> bool:
    # Check the independent cancel flag FIRST (most reliable)
    if _CANCEL_FLAGS.get(user_id, False):
        return True
    # Fallback: check ACTIVE_USERS flag (for legacy code paths)
    user_str = str(user_id)
    return user_str in ACTIVE_USERS and ACTIVE_USERS[user_str].get("cancel_requested", False)

def clear_cancel_flag(user_id: int):
    """Clear the cancel flag after batch cleanup. Called from finally blocks."""
    _CANCEL_FLAGS.pop(user_id, None)

async def remove_active_batch(user_id: int):
    if str(user_id) in ACTIVE_USERS:
        del ACTIVE_USERS[str(user_id)]
        await save_active_users_to_file()
    # Also unregister from FloodWaitScheduler (cancels any pending auto-resume)
    try:
        from scheduler import scheduler
        scheduler.unregister(user_id)
    except Exception:
        pass

def get_batch_info(user_id: int) -> Optional[Dict[str, Any]]:
    return ACTIVE_USERS.get(str(user_id))

ACTIVE_USERS = load_active_users()

# Clear stale active users on startup — these are from previous bot instances
# that may have crashed, and they would block users from starting new batches
if ACTIVE_USERS:
    print(f"Clearing {len(ACTIVE_USERS)} stale active batch entries from previous session...")
    ACTIVE_USERS = {}
    # Save empty state
    try:
        with open(ACTIVE_USERS_FILE, 'w') as f:
            json.dump({}, f)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════
# MongoDB-based upload maps — replaces filesystem-based reply_map.json
# Survives container restarts on Render, supports resume detection,
# incremental saves, and forward reference tracking.
# ═══════════════════════════════════════════════════════════════

# Legacy reply_map.json support (kept for one-time migration)
REPLY_MAP_FILE = "reply_map.json"

def load_reply_map():
    """Legacy: Load reply_map.json. Used only for one-time migration to MongoDB."""
    try:
        if os.path.exists(REPLY_MAP_FILE):
            with open(REPLY_MAP_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception:
        return {}

def save_reply_map(data):
    """Legacy: Save reply_map.json. Kept for backward compatibility only."""
    try:
        with open(REPLY_MAP_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving reply map: {e}")

def get_reply_map_key(uid, channel_id):
    """Legacy: Build reply_map key. Kept for migration only."""
    return f"{uid}_{channel_id}"

# MongoDB connection for upload maps
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME

_upload_mongo_client = AsyncIOMotorClient(MONGO_URI)
_upload_db = _upload_mongo_client[DB_NAME]
upload_maps_collection = _upload_db["upload_maps"]
pending_replies_collection = _upload_db["pending_replies"]
dependencies_collection = _upload_db["dependencies"]  # Method 1: poll → question image dependency index

# Mirror command collections (same DB, different structure)
mirror_state_collection = _upload_db["mirror_state"]
mirror_src_to_dst_collection = _upload_db["mirror_src_to_dst"]


def normalize_channel_id(channel_id) -> str:
    """Strip all prefixes to get the bare numeric channel ID.

    Handles every known format Telegram / MongoDB might store:
        "-1002563279588" → "2563279588"
        "1002563279588"  → "2563279588"
        "2563279588"     → "2563279588"
        -1002563279588   → "2563279588"  (int)
        2563279588       → "2563279588"  (int)
    """
    s = str(channel_id).strip()
    s = s.lstrip('-')
    if s.startswith('100') and len(s) > 5 and s[3:].isdigit():
        s = s[3:]
    return s

unresolved_links_collection = _upload_db["unresolved_links"]  # Link rewrite resume gap fix
pending_explanations_collection = _upload_db["pending_explanations"]  # 📖 Explanation button: poll awaiting explanation
mirrored_messages_index = _upload_db["mirrored_messages_index"]  # Smart Cache: tracks which dest messages contain source links

# 📺 Answer channel (forum topic): always post explanation + add button
# Supports formats:
#   ANSWER_CHANNEL_ID = "-100xxxxx" or "@channel"
#   ANSWER_TOPIC_ID   = "28646"                          (just topic ID)
#   OR combined: ANSWER_TOPIC_ID = "-1003745613477/28646" (group_id/topic_id)
#   If combined, it auto-fills ANSWER_CHANNEL_ID too.
ANSWER_CHANNEL_ID = os.environ.get("ANSWER_CHANNEL_ID", "")  # e.g. "-100xxxxx" or "@channel"
_raw_topic = os.environ.get("ANSWER_TOPIC_ID", "0") or "0"
if "/" in _raw_topic:
    # Combined format: "-1003745613477/28646" → split group_id and topic_id
    _parts = _raw_topic.split("/", 1)
    if not ANSWER_CHANNEL_ID:
        ANSWER_CHANNEL_ID = _parts[0].strip()
    try:
        ANSWER_TOPIC_ID = int(_parts[1].strip())
    except ValueError:
        print(f"[CONFIG] WARNING: ANSWER_TOPIC_ID topic part '{_parts[1]}' is not a number, defaulting to 0")
        ANSWER_TOPIC_ID = 0
else:
    try:
        ANSWER_TOPIC_ID = int(_raw_topic)
    except ValueError:
        print(f"[CONFIG] WARNING: ANSWER_TOPIC_ID '{_raw_topic}' is not a number, defaulting to 0")
        ANSWER_TOPIC_ID = 0

# ADDITIONAL_SOURCE_CHANNELS: Comma-separated list of source channel IDs
# whose msg_id_maps should ALSO be loaded for link rewriting.
# This enables cross-channel link rewriting when messages from the primary
# source channel contain links to OTHER source channels.
# Example: "-1002563279000,-1002563279001"
# These channels must have upload_map entries in MongoDB for the same user.
ADDITIONAL_SOURCE_CHANNELS = [
    ch.strip() for ch in os.environ.get("ADDITIONAL_SOURCE_CHANNELS", "").split(",")
    if ch.strip()
]

# Create indexes for efficient queries
try:
    import asyncio as _asyncio
    _loop = _asyncio.get_event_loop()
    _loop.create_task(dependencies_collection.create_index(
        [("user_id", 1), ("channel_id", 1), ("poll_src_id", 1)]
    ))
    _loop.create_task(unresolved_links_collection.create_index(
        [("user_id", 1), ("source_channel", 1), ("unresolved", 1)]
    ))
    _loop.create_task(unresolved_links_collection.create_index(
        [("unresolved", 1)],
        expireAfterSeconds=7 * 24 * 3600  # Auto-delete resolved after 7 days
    ))
    _loop.create_task(pending_explanations_collection.create_index(
        [("user_id", 1), ("source_channel", 1), ("poll_src_id", 1)],
        unique=True
    ))
    # Smart Cache compound index — enables lightning-fast /relink queries
    # Only scans messages with contains_old_links=True, skipping millions of others
    _loop.create_task(mirrored_messages_index.create_index(
        [("uid", 1), ("dst_chat_id", 1), ("contains_old_links", 1)]
    ))
    _loop.create_task(mirrored_messages_index.create_index(
        [("uid", 1), ("dst_chat_id", 1), ("dst_msg_id", 1)],
        unique=True
    ))
except Exception:
    pass

# ── Method 1 batch-side dependency recording ──
# If /fetch never built the dependency index, the batch records dependencies
# on-the-fly as it encounters polls. This makes the index self-populating.
# Keyed by uid — each running batch has its own flag and buffer.
_should_record_deps = {}     # uid → bool (True = /fetch didn't build deps, batch should record)
_pending_dep_batch = {}       # uid → list of dep dicts (buffered for bulk_write)
_DEP_BATCH_FLUSH_SIZE = 200  # Flush to MongoDB every N dependencies

async def _flush_dep_batch(uid, force=False):
    """Flush pending dependency records to MongoDB. Called every 200 deps and at batch end."""
    batch = _pending_dep_batch.get(uid, [])
    if not batch:
        return
    if not force and len(batch) < _DEP_BATCH_FLUSH_SIZE:
        return
    try:
        from pymongo import UpdateOne
        bulk_ops = []
        for dep in batch:
            bulk_ops.append(UpdateOne(
                {"user_id": dep["user_id"], "channel_id": dep["channel_id"], "question_src_id": dep["question_src_id"]},
                {"$set": dep},
                upsert=True
            ))
        await dependencies_collection.bulk_write(bulk_ops)
        print(f"[BATCH-DEP] Flushed {len(batch)} dependencies to MongoDB for uid={uid}")
        _pending_dep_batch[uid] = []
    except Exception as e:
        print(f"[BATCH-DEP] Failed to flush dependencies for uid={uid}: {e}")

_dep_recorded_count = {}    # uid → total dependencies recorded (including flushed)
_dep_last_log_time = {}     # uid → last log timestamp

def _record_poll_dependency(uid, channel_id, poll_src_id, question_src_id):
    """Record a poll→question image dependency (if batch-side recording is enabled).
    
    Only records if /fetch didn't already build the dependency index.
    Buffered and flushed in batches to avoid per-poll MongoDB writes.
    Logs progress every ~10 seconds.
    """
    if not _should_record_deps.get(uid):
        return
    dep = {
        "user_id": uid,
        "channel_id": str(channel_id),
        "question_src_id": question_src_id,
        "poll_src_id": poll_src_id,
    }
    if uid not in _pending_dep_batch:
        _pending_dep_batch[uid] = []
    _pending_dep_batch[uid].append(dep)
    
    # Track total count
    _dep_recorded_count[uid] = _dep_recorded_count.get(uid, 0) + 1
    
    # Log progress every ~10 seconds
    now = time.time()
    last = _dep_last_log_time.get(uid, 0)
    if now - last >= 10:
        _dep_last_log_time[uid] = now
        buffered = len(_pending_dep_batch.get(uid, []))
        total = _dep_recorded_count[uid]
        print(f"[BATCH-DEP] uid={uid} recorded={total} buffered={buffered} (flush at {_DEP_BATCH_FLUSH_SIZE})")
    
    # Flush if buffer is full
    if len(_pending_dep_batch[uid]) >= _DEP_BATCH_FLUSH_SIZE:
        # Schedule async flush (fire and forget via asyncio.create_task)
        try:
            asyncio.create_task(_flush_dep_batch(uid, force=True))
        except Exception:
            pass

def _clear_dep_recording_state(uid):
    """Clear dependency recording state for a user (called at batch end/cancel)."""
    _should_record_deps.pop(uid, None)
    _pending_dep_batch.pop(uid, None)
    _dep_recorded_count.pop(uid, None)
    _dep_last_log_time.pop(uid, None)

# Lazy migration flag
_upload_map_migration_done = False

async def _ensure_migration():
    """One-time migration: copy reply_map.json data to MongoDB."""
    global _upload_map_migration_done
    if _upload_map_migration_done:
        return
    _upload_map_migration_done = True

    old_map = load_reply_map()
    if not old_map:
        return

    migrated = 0
    for key, mappings in old_map.items():
        # key format: "{uid}_{channel_id}"
        parts = key.split("_", 1)
        if len(parts) != 2:
            continue
        uid_str, channel_id = parts
        try:
            uid = int(uid_str)
        except ValueError:
            continue

        if mappings:
            int_mappings = {int(k): v for k, v in mappings.items()}
            last_id = max(int_mappings.keys()) if int_mappings else 0
            await save_upload_map_incremental(uid, channel_id, None, int_mappings, last_id)
            migrated += 1

    if migrated > 0:
        print(f"[MIGRATE] Migrated {migrated} reply maps from JSON to MongoDB")
        # Rename old file to prevent re-migration
        try:
            os.rename(REPLY_MAP_FILE, REPLY_MAP_FILE + ".bak")
        except Exception:
            pass


async def load_upload_map(user_id, source_channel):
    """Load upload map from MongoDB. Returns (mappings_dict, last_uploaded_source_id, dest_channel).

    Tries ALL possible channel ID format variants because MongoDB might store
    the source_channel in a different format than what we're looking up with:
      - "-1002563279588" (full supergroup ID)
      - "2563279588" (clean channel ID)
      - "-1002563279588" → "2563279588" (strip -100 prefix)
      - "2563279588" → "-1002563279588" (add -100 prefix)
    """
    await _ensure_migration()
    source_str = str(source_channel)

    # Build ALL possible channel ID format variants
    channel_variants = set()
    channel_variants.add(source_str)  # as-is
    s = source_str.strip()
    channel_variants.add(s.lstrip("-"))  # no minus
    if not s.startswith("-100"):
        channel_variants.add(f"-100{s.lstrip('-')}")  # with -100 prefix
    if s.startswith("-100"):
        channel_variants.add(s[4:])  # strip -100 prefix
    # Also try without leading zeros in clean ID
    clean = s.lstrip('-')
    if clean.startswith('100'):
        channel_variants.add(clean[3:])  # strip 100 from clean ID

    # Try each variant until we find a match
    for variant in channel_variants:
        doc = await upload_maps_collection.find_one({
            "user_id": user_id,
            "source_channel": variant
        })
        if doc and "mappings" in doc and doc["mappings"]:
            if variant != source_str:
                _edlog(f"[MAP-VARIANT] uid={user_id} looked up '{source_str}' "
                       f"but found data under variant '{variant}' "
                       f"({len(doc['mappings'])} mappings)")
            return {int(k): v for k, v in doc["mappings"].items()}, doc.get("last_uploaded_source_id", 0), doc.get("dest_channel")

    return {}, 0, None


async def load_combined_msg_id_map(uid, source_channels=None, dest_channel_id=None):
    """Load src→dst mappings from ALL source channels the user has.

    This enables multi-source link rewriting: a message from channel A
    can contain links to channels B, C, etc. We need ALL their mappings
    to rewrite those cross-channel links.

    Args:
        uid: User ID
        source_channels: List of source channel identifiers to load.
                        If None, loads ALL source channels for this user.
        dest_channel_id: If provided, only load mappings for THIS destination
                        channel. Filters out unrelated source channels.

    Returns:
        combined_map: dict {src_msg_id: dst_msg_id} — flat map from ALL channels.
                      Note: If two source channels have the same message ID,
                      the last one loaded wins (rare edge case).
        channel_info: dict {channel_str: {"username": str|None, "numeric_id": int|None}}
                      — resolved channel metadata for building regex patterns.
    """
    combined_map = {}
    channel_info = {}

    if source_channels:
        # Load specific channels
        for src_ch in source_channels:
            ch_map, _, dest_ch = await load_upload_map(uid, str(src_ch))
            combined_map.update(ch_map)
            channel_info[str(src_ch)] = {
                "username": None,  # Will be resolved separately if needed
                "numeric_id": None,
            }
    else:
        # Load ALL source channels for this user, filtered by dest if specified
        await _ensure_migration()
        # Diagnostic: count total docs before filtering
        _total_upload_docs = await upload_maps_collection.count_documents({"user_id": uid})
        _edlog(f"[MULTI-SRC-MAP] upload_maps: total docs for uid={uid}: {_total_upload_docs}")
        cursor = upload_maps_collection.find({"user_id": uid})
        _channel_keys_logged = 0
        async for doc in cursor:
            src_ch = doc.get("source_channel", "")
            doc_dest = doc.get("dest_channel")
            _mappings_count = len(doc.get("mappings", {}))

            # Diagnostic: log every upload_map doc
            if _channel_keys_logged < 10:
                _edlog(f"[MAP-DB] uid={uid} source_channel='{src_ch}' "
                       f"dest_channel={doc_dest} mappings={_mappings_count}")
                _channel_keys_logged += 1

            # Filter by destination if specified
            if dest_channel_id and doc_dest:
                if normalize_channel_id(doc_dest) != normalize_channel_id(dest_channel_id):
                    _edlog(f"[MAP-DB] SKIPPED: dest mismatch — "
                           f"doc_dest={doc_dest}({normalize_channel_id(doc_dest)}) "
                           f"vs filter={dest_channel_id}({normalize_channel_id(dest_channel_id)})")
                    continue

            mappings = doc.get("mappings", {})
            if mappings:
                combined_map.update({int(k): v for k, v in mappings.items()})
                # Log the first 3 channels found with their key format and mapping count
                if _channel_keys_logged < 3:
                    _map_keys = sorted([int(k) for k in mappings.keys()])
                    _edlog(
                        f"[MAP-DB] uid={uid} source_channel='{src_ch}' "
                        f"mappings={len(mappings)} "
                        f"key_range={_map_keys[0] if _map_keys else 0}"
                        f"-{_map_keys[-1] if _map_keys else 0} "
                        f"sample={_map_keys[:3]}"
                    )
                    _channel_keys_logged += 1
            channel_info[src_ch] = {
                "username": None,
                "numeric_id": None,
                "dest_channel": doc.get("dest_channel"),
            }

    _edlog(f"[MULTI-SRC-MAP] Loaded {len(combined_map)} total mappings "
           f"from {len(channel_info)} source channel(s) [upload_maps]")
    _edlog(f"[MULTI-SRC-MAP] dest_channel_id filter={dest_channel_id} "
           f"normalize={normalize_channel_id(dest_channel_id) if dest_channel_id else None}")

    # ── Also load from /mirror command's mirror_src_to_dst collection ──
    # The /mirror command stores src→dst mappings in a different collection
    # than /batch. If the user mirrored via /mirror, the mappings won't be
    # in upload_maps but in mirror_src_to_dst. We need to load both.
    try:
        _mirror_count = 0
        # Filter by destination if specified
        mirror_query = {}
        if dest_channel_id:
            mirror_query["dst_chat_id"] = dest_channel_id
        # Diagnostic: also check total mirrors without filter
        _total_mirrors = await mirror_state_collection.count_documents({})
        _mirrors_with_dest = await mirror_state_collection.count_documents(mirror_query) if mirror_query else _total_mirrors
        _edlog(f"[MULTI-SRC-MAP] mirror_state: total={_total_mirrors} matching_dest={_mirrors_with_dest} query={mirror_query}")
        # Also log ALL mirror states for diagnostic
        async for _mdiag in mirror_state_collection.find({}):
            _edlog(f"[MULTI-SRC-MAP] mirror_state: id={_mdiag.get('mirror_id')} "
                   f"src={_mdiag.get('src_chat_id')} dst={_mdiag.get('dst_chat_id')} "
                   f"status={_mdiag.get('status')}")
        async for mstate in mirror_state_collection.find(mirror_query):
            mid = mstate.get("mirror_id", "")
            src_chat_id = mstate.get("src_chat_id")
            dst_chat_id = mstate.get("dst_chat_id")
            if not mid or not src_chat_id:
                continue

            # Load all src→dst mappings for this mirror_id
            m_cursor = mirror_src_to_dst_collection.find(
                {"mirror_id": mid, "status": "done"}
            )
            _m_map = {}
            async for mdoc in m_cursor:
                src_id = mdoc.get("src_msg_id")
                dst_id = mdoc.get("dst_msg_id")
                if src_id and dst_id:
                    _m_map[int(src_id)] = int(dst_id)

            if _m_map:
                combined_map.update(_m_map)
                _mirror_count += len(_m_map)

                # Add channel info for this source
                src_ch_str = str(src_chat_id)
                if src_ch_str not in channel_info:
                    channel_info[src_ch_str] = {
                        "username": None,
                        "numeric_id": src_chat_id if isinstance(src_chat_id, int) else None,
                        "dest_channel": dst_chat_id,
                    }
                _edlog(f"[MULTI-SRC-MAP] Mirror {mid}: src={src_chat_id} dst={dst_chat_id} "
                       f"mappings={len(_m_map)}")

        if _mirror_count:
            _edlog(f"[MULTI-SRC-MAP] Loaded {_mirror_count} additional mappings from /mirror collections")
    except Exception as _mirror_err:
        _edlog(f"[MULTI-SRC-MAP] Could not load mirror mappings: {_mirror_err}")

    # ── Also load src→dst mappings from relink fingerprints collection ──
    # The fingerprints collection stores {src_msg_id, dst_msg_id, source_channel, uid}
    # at UPLOAD TIME via checkpoint_with_fingerprint(). This is the RICHEST source
    # of src→dst mappings because it covers ALL messages ever mirrored, not just
    # the latest /batch run.
    #
    # CRITICAL FIX: Before this, combined_map only had upload_maps entries (narrow
    # range like 21268-21503). But links in destination messages point to source
    # IDs across the full range (6000-16000+). The fingerprints have this data!
    # Loading them into combined_map makes Strategy 1 (direct lookup) work for
    # the vast majority of cases, instead of relying on slow per-link DB queries.
    try:
        from plugins.relink import fingerprints_collection as _fpc
        _fp_count_total = await _fpc.count_documents({"uid": uid})
        _fp_loaded = 0
        _fp_overwritten = 0  # How many keys already existed in combined_map
        if _fp_count_total > 0:
            _fp_query = {"uid": uid}
            # If dest_channel_id is specified, try to filter fingerprints.
            # Fingerprints don't store dest_channel, but we can infer it:
            # all fingerprints for this user's source channels that map to
            # the destination channel's message ID range should be included.
            # For now, load ALL fingerprints for this user — the overlap is
            # acceptable (same src_msg_id mapping to same dst_msg_id).
            async for fp_doc in _fpc.find(_fp_query):
                src_id = fp_doc.get("src_msg_id")
                dst_id = fp_doc.get("dst_msg_id")
                if src_id is not None and dst_id is not None:
                    src_id = int(src_id)
                    dst_id = int(dst_id)
                    if src_id in combined_map:
                        _fp_overwritten += 1
                    combined_map[src_id] = dst_id
                    _fp_loaded += 1

                    # Also add channel_info from fingerprint if not already known
                    fp_ch = fp_doc.get("source_channel", "")
                    if fp_ch and fp_ch not in channel_info:
                        channel_info[fp_ch] = {
                            "username": None,
                            "numeric_id": None,
                            "dest_channel": None,
                        }

            _edlog(f"[MULTI-SRC-MAP] Loaded {_fp_loaded} src→dst mappings from "
                   f"fingerprints collection (total docs: {_fp_count_total}, "
                   f"overwrites: {_fp_overwritten})")
        else:
            _edlog(f"[MULTI-SRC-MAP] No fingerprints found for uid={uid}")
    except Exception as _fp_err:
        _edlog(f"[MULTI-SRC-MAP] Could not load fingerprint mappings: {_fp_err}")

    # ── Also load channel_info from /fetch command's fetch_maps collection ──
    # /fetch scans source channels and stores metadata. Even if there are no
    # src→dst mappings yet, the channel_info from fetch_maps lets /relink
    # classify links as source channel links.
    #
    # IMPORTANT: fetch_maps don't store destination channel info, so we can't
    # filter by dest. But we ONLY add channels that aren't already known from
    # upload_maps/mirror (which ARE filtered). This prevents loading unrelated
    # channels when we already have specific ones.
    try:
        from plugins.fetch import fetch_maps_collection as _fmc
        _fetch_channels = 0
        async for fdoc in _fmc.find({"user_id": uid}):
            ch_id = fdoc.get("channel_id", "")
            if ch_id and ch_id not in channel_info:
                # If we already have filtered channels from upload_maps/mirror,
                # only add fetch channels that could plausibly be a source for
                # this destination (we can't filter exactly, but skip if we
                # already have specific channels)
                channel_info[ch_id] = {
                    "username": None,
                    "numeric_id": None,
                    "dest_channel": None,
                }
                _fetch_channels += 1
        if _fetch_channels:
            _edlog(f"[MULTI-SRC-MAP] Loaded {_fetch_channels} source channel(s) from /fetch maps "
                   f"(channel_info only, no src→dst mappings)")
    except Exception as _fetch_err:
        _edlog(f"[MULTI-SRC-MAP] Could not load fetch_maps: {_fetch_err}")

    return combined_map, channel_info


async def build_multi_source_channels(uid, primary_channel, primary_username=None, primary_numeric_id=None, client=None):
    """Build the multi_source_channels list for a user.

    Loads ALL source channels the user has from upload_maps, plus resolves
    their usernames and numeric IDs using the Telegram API (if client provided).

    Also incorporates ADDITIONAL_SOURCE_CHANNELS from the env var — these are
    source channels whose msg_id_maps should be loaded for cross-channel
    link rewriting, even if they weren't auto-discovered from upload_maps.

    The primary channel (current batch's source) is always included first.

    Returns:
        multi_source_channels: list of dicts for rewrite functions
        combined_msg_id_map: combined src→dst map from ALL channels
    """
    # Load ALL source channels' maps from MongoDB (auto-discovery)
    combined_map, channel_info = await load_combined_msg_id_map(uid)

    # Also load maps from ADDITIONAL_SOURCE_CHANNELS env var
    _extra_channels_loaded = 0
    if ADDITIONAL_SOURCE_CHANNELS:
        for extra_ch in ADDITIONAL_SOURCE_CHANNELS:
            if extra_ch in channel_info:
                continue  # Already loaded from upload_maps
            extra_map, _, extra_dest = await load_upload_map(uid, str(extra_ch))
            if extra_map:
                combined_map.update(extra_map)
                channel_info[extra_ch] = {
                    "username": None,
                    "numeric_id": None,
                    "dest_channel": extra_dest,
                }
                _extra_channels_loaded += 1
        if _extra_channels_loaded:
            _edlog(f"[MULTI-SRC] Loaded {_extra_channels_loaded} additional source channels from env var "
                   f"({len(combined_map)} total mappings now)")

    # Build the multi_source_channels list
    multi_source_channels = []

    # Add primary channel first (already resolved)
    primary_entry = {
        "channel": str(primary_channel),
        "username": primary_username,
        "numeric_id": primary_numeric_id,
    }
    multi_source_channels.append(primary_entry)

    # Track which channels we've already added (by string)
    _added_channels = {str(primary_channel)}

    # Try to resolve other channels' usernames/IDs via client
    if client:
        for ch_str, ch_info in channel_info.items():
            if ch_str in _added_channels:
                continue  # Already added
            _added_channels.add(ch_str)
            ch_username = ch_info.get("username")
            ch_numeric_id = ch_info.get("numeric_id")
            ch_dest = ch_info.get("dest_channel")

            # Try resolving via Telegram API if we don't have username yet
            if not ch_username:
                try:
                    resolved_ch = ch_str
                    if ch_str.lstrip('-').isdigit():
                        resolved_ch = int(ch_str)
                    ch_chat = await client.get_chat(resolved_ch)
                    ch_username = getattr(ch_chat, 'username', None)
                    ch_numeric_id = getattr(ch_chat, 'id', ch_numeric_id)
                except Exception:
                    pass

            multi_source_channels.append({
                "channel": ch_str,
                "username": ch_username,
                "numeric_id": ch_numeric_id,
            })
    else:
        # No client — just add raw channel info
        for ch_str, ch_info in channel_info.items():
            if ch_str in _added_channels:
                continue
            _added_channels.add(ch_str)
            multi_source_channels.append({
                "channel": ch_str,
                "username": ch_info.get("username"),
                "numeric_id": ch_info.get("numeric_id"),
            })

    _edlog(f"[MULTI-SRC] Built multi_source_channels: {len(multi_source_channels)} channels, "
           f"{len(combined_map)} total mappings")
    return multi_source_channels, combined_map


async def save_upload_map_incremental(user_id, source_channel, dest_channel, new_mappings, last_uploaded_source_id):
    """Save upload mappings incrementally — merge new mappings into existing document."""
    # Convert to string keys for MongoDB
    str_mappings = {str(k): v for k, v in new_mappings.items()}

    update_ops = {
        "$set": {
            "last_uploaded_source_id": last_uploaded_source_id,
            "updated_at": datetime.now()
        },
        "$inc": {"total_uploaded": len(new_mappings)},
    }
    if dest_channel is not None:
        update_ops["$set"]["dest_channel"] = dest_channel

    await upload_maps_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        update_ops,
        upsert=True
    )

    # Merge mappings separately (MongoDB $set with dotted keys)
    if str_mappings:
        await upload_maps_collection.update_one(
            {"user_id": user_id, "source_channel": str(source_channel)},
            {"$set": {f"mappings.{k}": v for k, v in str_mappings.items()}}
        )


async def save_upload_map(user_id, source_channel, mappings):
    """Convenience wrapper — save a full mappings dict without needing dest_channel/last_id.

    Called by channel_clone.py as: save_upload_map(uid, source_channel, msg_id_map)
    where msg_id_map is {src_msg_id: dst_msg_id}.
    Derives last_uploaded_source_id from the max source key in the dict.
    """
    if not mappings:
        return
    last_id = max(int(k) for k in mappings.keys())
    await save_upload_map_incremental(user_id, source_channel, None, mappings, last_id)


async def get_upload_map_resume_info(user_id, source_channel):
    """Check if a partial upload exists for resume.
    Returns (last_uploaded_id, total_uploaded, dest_channel) or None."""
    await _ensure_migration()
    doc = await upload_maps_collection.find_one({
        "user_id": user_id,
        "source_channel": str(source_channel)
    })
    if doc:
        return doc.get("last_uploaded_source_id", 0), doc.get("total_uploaded", 0), doc.get("dest_channel")
    return None


async def add_pending_reply(user_id, source_channel, dest_channel, dest_msg_id, source_reply_to_id):
    """Track a forward reference — a message whose reply_to points to a not-yet-uploaded message."""
    await pending_replies_collection.insert_one({
        "user_id": user_id,
        "source_channel": str(source_channel),
        "dest_channel": dest_channel,
        "dest_msg_id": dest_msg_id,
        "source_reply_to_id": source_reply_to_id,
        "resolved": False,
        "created_at": datetime.now()
    })


async def resolve_pending_replies(user_id, source_channel, msg_id_map):
    """After batch completes, check pending forward references and try to resolve them."""
    pending = await pending_replies_collection.find({
        "user_id": user_id,
        "source_channel": str(source_channel),
        "resolved": False
    }).to_list(length=None)

    resolved_count = 0
    for item in pending:
        source_reply_to = item["source_reply_to_id"]
        if source_reply_to in msg_id_map:
            # The target message was uploaded later — we can now fix the reply
            # BUT Telegram API doesn't allow editing reply_to_message_id
            # So we just mark it as resolved and log it
            resolved_count += 1
            await pending_replies_collection.update_one(
                {"_id": item["_id"]},
                {"$set": {"resolved": True, "resolved_dest_id": msg_id_map[source_reply_to]}}
            )

    return resolved_count


async def delete_upload_map(user_id, source_channel=None):
    """Delete upload maps for a user."""
    query = {"user_id": user_id}
    if source_channel:
        query["source_channel"] = str(source_channel)
    result = await upload_maps_collection.delete_many(query)
    return result.deleted_count


# ════════════════════════════════════════════════════════════════════
#  UNRESOLVED LINKS TRACKING — Link Rewrite Resume Gap Fix
#
#  PROBLEM:
#    messages_needing_link_update is in-memory only.
#    If batch stops mid-way → list is lost → post-batch pass
#    never runs → those messages keep source channel links forever.
#
#  FIX:
#    When a message has unresolved links → write to MongoDB immediately.
#    On every batch start → query MongoDB for unresolved messages
#    → rewrite with complete msg_id_map → mark resolved.
#    Runs BEFORE new messages are processed → never skipped.
# ════════════════════════════════════════════════════════════════════

async def mark_needs_link_update(uid, source_channel, dst_chat_id, dst_msg_id, src_msg_id, unresolved_src_ids=None):
    """
    Called immediately when a message is uploaded but has
    unresolved links (src_msg_id not yet in msg_id_map).

    Writes to MongoDB so this survives crashes and stops.
    Replaces the old in-memory messages_needing_link_update.append().

    Now also tracks the SPECIFIC unresolved src_msg IDs as a list,
    so resolve_pending_link_rewrites can skip messages where none of
    the unresolved IDs have appeared in the map yet (breaks the
    FLOOD_WAIT + MESSAGE_NOT_MODIFIED death spiral).
    """
    try:
        update_ops = {
            "$set": {
                "src_msg_id": src_msg_id,
                "unresolved": True,
                "updated_at": datetime.now(),
            },
        }
        # Track specific unresolved src_msg IDs for smarter retry logic
        if unresolved_src_ids:
            update_ops["$addToSet"] = {
                "unresolved_src_ids": {"$each": [int(x) for x in unresolved_src_ids if x]},
            }
        elif src_msg_id:
            # Single ID fallback
            update_ops["$addToSet"] = {
                "unresolved_src_ids": int(src_msg_id),
            }
        await unresolved_links_collection.update_one(
            {
                "user_id": uid,
                "source_channel": str(source_channel),
                "dst_chat_id": dst_chat_id,
                "dst_msg_id": dst_msg_id,
            },
            update_ops,
            upsert=True,
        )
    except Exception as e:
        print(f"[LINK-REWRITE-DB] Failed to mark unresolved: {e}")


async def mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id):
    """Mark a destination message as having all links fully resolved."""
    try:
        await unresolved_links_collection.update_one(
            {
                "user_id": uid,
                "source_channel": str(source_channel),
                "dst_chat_id": dst_chat_id,
                "dst_msg_id": dst_msg_id,
            },
            {"$set": {
                "unresolved": False,
                "resolved_at": datetime.now(),
            }},
        )
    except Exception as e:
        print(f"[LINK-REWRITE-DB] Failed to mark resolved: {e}")


# ════════════════════════════════════════════════════════════════════
#  SMART CACHE — mirrored_messages_index
#
#  3-Step MongoDB Caching System for lightning-fast /relink:
#
#  Step 1: cache_message_for_relink() — Called at mirror time.
#    Extracts ALL source-channel links from the message text/entities
#    and stores a lightweight index document in mirrored_messages_index.
#    This is the "Smart Cache Write Hook".
#
#  Step 2: /relink queries mirrored_messages_index to find EXACTLY
#    which destination messages contain old source-channel links,
#    instead of scanning every message backwards.
#
#  Step 3: "Surgical Strike" editing — only edit the messages that
#    need fixing, then mark them as fixed in the index.
# ════════════════════════════════════════════════════════════════════

# Regex patterns for extracting source channel links from message text
_TME_PRIVATE_RE = re.compile(r'https?://t\.me/c/(\d+)/(\d+)(?:/(\d+))?', re.IGNORECASE)
_TME_PUBLIC_RE = re.compile(r'https?://t\.me/([a-zA-Z]\w{3,}[a-zA-Z0-9])/(\d+)(?:/(\d+))?', re.IGNORECASE)
_TG_RESOLVE_RE = re.compile(r'tg://resolve\?domain=(\w+)&post=(\d+)', re.IGNORECASE)
# Known non-channel t.me/ paths (skip these)
_TME_SKIP_PATHS = {'c', 'joinchat', '+', 'addstickers', 'bot', 'setlanguage',
                   'confirmphone', 'login', 'passport', 'faq', 'privacy'}


def _extract_source_links_from_message(text, entities, source_channel, source_channel_username=None,
                                        source_channel_id=None, multi_source_channels=None):
    """Extract ALL source-channel Telegram links from message text and entities.
    
    Returns a list of dicts, each containing:
        - url: The full URL string
        - src_msg_id: The message ID referenced in the source channel
        - channel_key: The channel identifier (clean ID or username)
        - link_type: 'private', 'public', or 'tg_resolve'
    
    This is called at MIRROR TIME to build the Smart Cache index.
    It extracts links BEFORE they are rewritten, so we know exactly
    which source channel links are in the message.
    """
    links = []
    seen_urls = set()  # Deduplicate
    
    # Build the set of source channel identifiers we care about
    # A link is a "source link" if it points to one of our source channels
    source_clean_ids = set()
    source_usernames = set()
    
    # Primary source channel
    src_clean = normalize_channel_id(str(source_channel))
    if src_clean:
        source_clean_ids.add(src_clean)
    if source_channel_username:
        source_usernames.add(source_channel_username.lower())
    if source_channel_id:
        sid_clean = normalize_channel_id(str(source_channel_id))
        if sid_clean:
            source_clean_ids.add(sid_clean)
    
    # Additional source channels
    if multi_source_channels:
        for ch_info in multi_source_channels:
            ch_str = ch_info.get("channel", "")
            ch_clean = normalize_channel_id(ch_str)
            if ch_clean:
                source_clean_ids.add(ch_clean)
            ch_uname = ch_info.get("username")
            if ch_uname:
                source_usernames.add(ch_uname.lower())
            ch_numeric = ch_info.get("numeric_id")
            if ch_numeric:
                nid_clean = normalize_channel_id(str(ch_numeric))
                if nid_clean:
                    source_clean_ids.add(nid_clean)
    
    # Helper to check if a link points to one of our source channels
    def _is_source_link(peer_id_str, username_str):
        if peer_id_str:
            return normalize_channel_id(peer_id_str) in source_clean_ids
        if username_str:
            return username_str.lower() in source_usernames
        return False
    
    # Extract from text (bare URLs)
    all_text = (text or "")
    
    # Private links: https://t.me/c/1234567890/52
    for m in _TME_PRIVATE_RE.finditer(all_text):
        url = m.group(0)
        if url in seen_urls:
            continue
        peer_id = m.group(1)
        msg_id = int(m.group(2))
        if _is_source_link(peer_id, None):
            seen_urls.add(url)
            links.append({
                "url": url,
                "src_msg_id": msg_id,
                "channel_key": normalize_channel_id(peer_id),
                "link_type": "private"
            })
    
    # Public links: https://t.me/channelname/52
    for m in _TME_PUBLIC_RE.finditer(all_text):
        url = m.group(0)
        if url in seen_urls:
            continue
        username = m.group(1)
        msg_id = int(m.group(2))
        if username.lower() in _TME_SKIP_PATHS:
            continue
        if _is_source_link(None, username):
            seen_urls.add(url)
            links.append({
                "url": url,
                "src_msg_id": msg_id,
                "channel_key": username.lower(),
                "link_type": "public"
            })
    
    # tg://resolve links
    for m in _TG_RESOLVE_RE.finditer(all_text):
        url = m.group(0)
        if url in seen_urls:
            continue
        username = m.group(1)
        msg_id = int(m.group(2))
        if _is_source_link(None, username):
            seen_urls.add(url)
            links.append({
                "url": url,
                "src_msg_id": msg_id,
                "channel_key": username.lower(),
                "link_type": "tg_resolve"
            })
    
    # Extract from entities (TEXT_LINK entities have URLs not in text)
    if entities:
        for ent in entities:
            ent_url = getattr(ent, 'url', None)
            if not ent_url or ('t.me' not in ent_url.lower() and 'tg://' not in ent_url.lower()):
                continue
            if ent_url in seen_urls:
                continue
            
            # Private link in entity
            priv_m = _TME_PRIVATE_RE.match(ent_url)
            if priv_m:
                peer_id = priv_m.group(1)
                msg_id = int(priv_m.group(2))
                if _is_source_link(peer_id, None):
                    seen_urls.add(ent_url)
                    links.append({
                        "url": ent_url,
                        "src_msg_id": msg_id,
                        "channel_key": normalize_channel_id(peer_id),
                        "link_type": "private"
                    })
                continue
            
            # Public link in entity
            pub_m = _TME_PUBLIC_RE.match(ent_url)
            if pub_m:
                username = pub_m.group(1)
                msg_id = int(pub_m.group(2))
                if username.lower() in _TME_SKIP_PATHS:
                    continue
                if _is_source_link(None, username):
                    seen_urls.add(ent_url)
                    links.append({
                        "url": ent_url,
                        "src_msg_id": msg_id,
                        "channel_key": username.lower(),
                        "link_type": "public"
                    })
                continue
            
            # tg://resolve in entity
            tg_m = _TG_RESOLVE_RE.match(ent_url)
            if tg_m:
                username = tg_m.group(1)
                msg_id = int(tg_m.group(2))
                if _is_source_link(None, username):
                    seen_urls.add(ent_url)
                    links.append({
                        "url": ent_url,
                        "src_msg_id": msg_id,
                        "channel_key": username.lower(),
                        "link_type": "tg_resolve"
                    })
    
    return links


async def cache_message_for_relink(uid, source_channel, dst_chat_id, dst_msg_id, src_msg_id,
                                    text, entities, source_channel_username=None,
                                    source_channel_id=None, multi_source_channels=None):
    """Smart Cache Write Hook — called at mirror time AFTER a message is sent.
    
    Extracts ALL source-channel links from the message text/entities and stores
    a lightweight index document in mirrored_messages_index.
    
    Key design decisions:
    1. This is called BEFORE link rewriting, so we capture the ORIGINAL source links
    2. If the message contains source-channel links, contains_old_links=True
    3. The links_to_resolve list stores the EXACT source links and their src_msg_ids
    4. When /relink runs, it queries ONLY messages with contains_old_links=True
    5. After successful relink, contains_old_links is set to False
    
    This eliminates the "blind ID scanning" bottleneck entirely:
    - No more backwards while-loop scanning millions of messages
    - No more FloodWait from scanning deleted/empty messages
    - MongoDB compound index makes the query instant
    
    Args:
        uid: User ID
        source_channel: Source channel identifier
        dst_chat_id: Destination chat ID
        dst_msg_id: Destination message ID (just sent)
        src_msg_id: Source message ID
        text: The message text/caption (BEFORE rewriting)
        entities: The message entities (BEFORE rewriting)
        source_channel_username: Source channel's public username
        source_channel_id: Source channel's numeric ID
        multi_source_channels: List of additional source channel dicts
    """
    try:
        # Extract source-channel links from the message
        source_links = _extract_source_links_from_message(
            text, entities, source_channel,
            source_channel_username=source_channel_username,
            source_channel_id=source_channel_id,
            multi_source_channels=multi_source_channels
        )
        
        if not source_links:
            # No source links in this message — no need to index
            # But we still store a lightweight entry so /relink knows it was processed
            await mirrored_messages_index.update_one(
                {"uid": uid, "dst_chat_id": dst_chat_id, "dst_msg_id": dst_msg_id},
                {"$set": {
                    "uid": uid,
                    "source_channel": str(source_channel),
                    "src_msg_id": src_msg_id,
                    "dst_chat_id": dst_chat_id,
                    "dst_msg_id": dst_msg_id,
                    "contains_old_links": False,
                    "links_to_resolve": [],
                    "last_updated": datetime.utcnow(),
                }},
                upsert=True,
            )
            return
        
        # Message contains source-channel links — index them
        # Determine which links are unresolved (src_msg_id not in current map)
        # We also store all link info so /relink can resolve them later
        link_docs = []
        unresolved_src_ids = []
        for link in source_links:
            link_docs.append({
                "url": link["url"],
                "src_msg_id": link["src_msg_id"],
                "channel_key": link["channel_key"],
                "link_type": link["link_type"],
            })
            unresolved_src_ids.append(link["src_msg_id"])
        
        await mirrored_messages_index.update_one(
            {"uid": uid, "dst_chat_id": dst_chat_id, "dst_msg_id": dst_msg_id},
            {"$set": {
                "uid": uid,
                "source_channel": str(source_channel),
                "src_msg_id": src_msg_id,
                "dst_chat_id": dst_chat_id,
                "dst_msg_id": dst_msg_id,
                "contains_old_links": True,
                "links_to_resolve": link_docs,
                "unresolved_src_ids": unresolved_src_ids,
                "last_updated": datetime.utcnow(),
            }},
            upsert=True,
        )
        
        _edlog(f"[SMART-CACHE] Indexed msg dst={dst_msg_id} src={src_msg_id} — "
               f"{len(source_links)} source links ({len(unresolved_src_ids)} unresolved)")
        
    except Exception as e:
        # NEVER let the cache hook break mirroring
        print(f"[SMART-CACHE] Failed to cache message for relink: {e}")


async def mark_message_links_fixed(uid, dst_chat_id, dst_msg_id):
    """Mark a destination message as having all links fixed in the Smart Cache.
    
    Called after successful /relink editing. Sets contains_old_links=False
    so future /relink runs skip this message entirely.
    """
    try:
        await mirrored_messages_index.update_one(
            {"uid": uid, "dst_chat_id": dst_chat_id, "dst_msg_id": dst_msg_id},
            {"$set": {
                "contains_old_links": False,
                "fixed_at": datetime.utcnow(),
            }},
        )
    except Exception as e:
        print(f"[SMART-CACHE] Failed to mark message fixed: {e}")


async def get_messages_needing_relink(uid, dst_chat_id, limit=0):
    """Smart Cache Query — Step 2 of the 3-Step system.
    
    Returns destination message IDs that contain unresolved source-channel links.
    Uses the mirrored_messages_index compound index for instant lookup.
    
    This REPLACES the old backwards-while-loop scan that fetched every message
    from the destination channel one by one (causing FloodWait, CHANNEL_INVALID, etc).
    
    Args:
        uid: User ID
        dst_chat_id: Destination channel ID
        limit: Maximum number of results (0 = no limit)
    
    Returns:
        List of dicts with dst_msg_id, links_to_resolve, and other metadata
    """
    try:
        query = {"uid": uid, "dst_chat_id": dst_chat_id, "contains_old_links": True}
        cursor = mirrored_messages_index.find(
            query,
            {"dst_msg_id": 1, "links_to_resolve": 1, "src_msg_id": 1,
             "source_channel": 1, "unresolved_src_ids": 1, "_id": 0}
        )
        if limit:
            cursor = cursor.limit(limit)
        results = await cursor.to_list(length=limit or None)
        return results
    except Exception as e:
        print(f"[SMART-CACHE] Query failed: {e}")
        return []


# ════════════════════════════════════════════════════════════════════
#  ANSWER TOPIC (Forum Group) — Always post explanation + add 📺 button to poll
#
#  FLOW:
#    1. Quiz poll sent → immediately look up explanation
#    2. Post explanation (image or text) to answer forum topic
#    3. Edit poll to add 📺 View Answer (Channel) button
#    4. Preserves existing 💡 View Answer (Telegraph) button
#
#  RESULT:
#    Telegraph OK   → 💡 View Answer (Telegraph) + 📺 View Answer (Channel)
#    Telegraph FAIL → 📺 View Answer (Channel) only
#
#  SETUP:
#    1. Create a group → Settings → Enable Topics/Forum
#    2. Create a topic called "Answers"
#    3. Add bot as admin
#    4. Set ANSWER_CHANNEL_ID = group ID  (e.g. -100XXXXXXXXXX)
#    5. Set ANSWER_TOPIC_ID  = topic thread_id (e.g. 123)
# ════════════════════════════════════════════════════════════════════

async def post_to_answer_topic(bot_client, user_client, source_channel, poll_src_id, correct_letter,
                                explanation_image_url=None, explanation_text=None, expl_msg_id=None, has_photo=False,
                                poll_dest_chat_id=None, poll_dest_msg_id=None):
    """
    Post explanation to a specific forum topic thread and return the t.me link.
    Accepts explanation data directly (from Telegraph section) to avoid redundant lookups.
    Falls back to cache lookup only if no data is passed.
    Bot must be admin in the forum group.
    """
    if not ANSWER_CHANNEL_ID:
        return None

    # If caller didn't pass explanation data, try cache lookup
    if not explanation_text and not explanation_image_url and not has_photo:
        try:
            from plugins.explanation_listener import get_explanation_lookup
            channel_expl = get_explanation_lookup(str(source_channel))
            entry = channel_expl.get(poll_src_id)
            if entry:
                explanation_text = entry.get("text")
                expl_msg_id = entry.get("explanation_msg_id")
                has_photo = entry.get("has_photo", False)
        except Exception as e:
            print(f"[ANSWER-TOPIC] Cache lookup failed: {e}")

        if not explanation_text and user_client:
            try:
                from plugins.explanation_listener import check_poll_builtin_explanation
                solution = await check_poll_builtin_explanation(user_client, source_channel, poll_src_id)
                if solution:
                    explanation_text = solution
            except Exception:
                pass

    # Build kwargs for topic posting
    topic_kwargs = {}
    if ANSWER_TOPIC_ID:
        topic_kwargs["message_thread_id"] = ANSWER_TOPIC_ID

    sent_msg = None

    # Post image if available — try in order:
    # 1) Direct file_id from source channel (best quality, no re-download)
    # 2) URL from Telegraph upload (already uploaded, usable as photo param)
    # Also handles VIDEO explanations (explanation message can be a video)
    if has_photo and expl_msg_id and user_client:
        try:
            resolved_src = await resolve_chat(user_client, source_channel)
            expl_msg = await user_client.get_messages(resolved_src, expl_msg_id)
            if expl_msg and expl_msg.photo:
                caption = f"✅ Answer: {correct_letter}"
                if explanation_text:
                    caption = f"✅ Answer: {correct_letter}\n\n{explanation_text}"
                await _rate_limiter.acquire(ANSWER_CHANNEL_ID)
                sent_msg = await bot_client.send_photo(
                    ANSWER_CHANNEL_ID,
                    photo=expl_msg.photo.file_id,
                    caption=caption,
                    **topic_kwargs
                )
                print(f"[ANSWER-TOPIC] Photo posted (file_id): msg_id={sent_msg.id} topic={ANSWER_TOPIC_ID}")
            elif expl_msg and expl_msg.video:
                # Video explanation — send as video
                caption = f"✅ Answer: {correct_letter}"
                if explanation_text:
                    caption = f"✅ Answer: {correct_letter}\n\n{explanation_text}"
                await _rate_limiter.acquire(ANSWER_CHANNEL_ID)
                sent_msg = await bot_client.send_video(
                    ANSWER_CHANNEL_ID,
                    video=expl_msg.video.file_id,
                    caption=caption,
                    duration=expl_msg.video.duration,
                    width=expl_msg.video.width,
                    height=expl_msg.video.height,
                    **topic_kwargs
                )
                print(f"[ANSWER-TOPIC] Video posted (file_id): msg_id={sent_msg.id} topic={ANSWER_TOPIC_ID}")
        except Exception as e:
            print(f"[ANSWER-TOPIC] Photo/Video post (file_id) failed: {e}")

    if not sent_msg and explanation_image_url:
        try:
            caption = f"✅ Answer: {correct_letter}"
            if explanation_text:
                caption = f"✅ Answer: {correct_letter}\n\n{explanation_text}"
            await _rate_limiter.acquire(ANSWER_CHANNEL_ID)
            sent_msg = await bot_client.send_photo(
                ANSWER_CHANNEL_ID,
                photo=explanation_image_url,
                caption=caption,
                **topic_kwargs
            )
            print(f"[ANSWER-TOPIC] Photo posted (URL): msg_id={sent_msg.id} topic={ANSWER_TOPIC_ID}")
        except Exception as e:
            print(f"[ANSWER-TOPIC] Photo post (URL) failed: {e}")

    # Post text if no image was sent
    if not sent_msg and explanation_text:
        try:
            await _rate_limiter.acquire(ANSWER_CHANNEL_ID)
            sent_msg = await bot_client.send_message(
                ANSWER_CHANNEL_ID,
                text=f"✅ Answer: {correct_letter}\n\n{explanation_text}",
                **topic_kwargs
            )
            print(f"[ANSWER-TOPIC] Text posted: msg_id={sent_msg.id} topic={ANSWER_TOPIC_ID}")
        except Exception as e:
            print(f"[ANSWER-TOPIC] Text post failed: {e}")

    # Post bare letter if nothing else worked
    if not sent_msg:
        try:
            await _rate_limiter.acquire(ANSWER_CHANNEL_ID)
            sent_msg = await bot_client.send_message(
                ANSWER_CHANNEL_ID,
                text=f"✅ Answer: {correct_letter}",
                **topic_kwargs
            )
            print(f"[ANSWER-TOPIC] Bare letter posted: msg_id={sent_msg.id} topic={ANSWER_TOPIC_ID}")
        except Exception as e:
            print(f"[ANSWER-TOPIC] Bare letter post failed: {e}")

    if not sent_msg:
        return None

    # ── 🔙 Back to Question: add on the answer topic message → links back to poll (B') ──
    if poll_dest_msg_id:
        try:
            poll_chat = poll_dest_chat_id  # Usually the same as the dest channel where the poll lives
            if poll_chat:
                poll_link = _build_telegram_link(poll_chat, poll_dest_msg_id)
                if poll_link:
                    await _add_inline_button(
                        bot_client, ANSWER_CHANNEL_ID, sent_msg.id,
                        "🔙 Back to Question", poll_link,
                        log_prefix="BACK-ANSWER"
                    )
                    print(f"[ANSWER-TOPIC] ✅ 🔙 button added on answer msg {sent_msg.id} → poll {poll_dest_msg_id}")
        except Exception as _back_e:
            print(f"[ANSWER-TOPIC] 🔙 button error: {_back_e}")

    # Build link to the answer message
    if str(ANSWER_CHANNEL_ID).startswith('@'):
        # Public group with username
        if ANSWER_TOPIC_ID:
            answer_url = f"https://t.me/{ANSWER_CHANNEL_ID.lstrip('@')}/{ANSWER_TOPIC_ID}/{sent_msg.id}"
        else:
            answer_url = f"https://t.me/{ANSWER_CHANNEL_ID.lstrip('@')}/{sent_msg.id}"
    else:
        # Private group — use /c/ format
        clean_id = str(ANSWER_CHANNEL_ID).lstrip('-')
        if clean_id.startswith('100'):
            clean_id = clean_id[3:]
        if ANSWER_TOPIC_ID:
            answer_url = f"https://t.me/c/{clean_id}/{ANSWER_TOPIC_ID}/{sent_msg.id}"
        else:
            answer_url = f"https://t.me/c/{clean_id}/{sent_msg.id}"

    print(f"[ANSWER-TOPIC] URL: {answer_url}")
    return answer_url


async def handle_answer_buttons(bot_client, user_client, source_channel, poll_src_id, correct_letter, dst_chat_id, dst_msg_id,
                                 explanation_image_url=None, explanation_text=None, expl_msg_id=None, has_photo=False):
    """
    After quiz poll is sent, ALWAYS post explanation to answer topic (forum)
    and add 📺 View Answer (Channel) button to the poll.
    Does NOT touch existing 💡 View Answer button.
    Accepts explanation data directly (from Telegraph section) to avoid redundant lookups.
    Also adds 🔙 Back to Question button on the answer topic message.
    """
    answer_url = await post_to_answer_topic(bot_client, user_client, source_channel, poll_src_id, correct_letter,
                                             explanation_image_url=explanation_image_url,
                                             explanation_text=explanation_text,
                                             expl_msg_id=expl_msg_id,
                                             has_photo=has_photo,
                                             poll_dest_chat_id=dst_chat_id,
                                             poll_dest_msg_id=dst_msg_id)

    if not answer_url:
        print(f"[ANSWER-TOPIC] No answer URL — skipping button add")
        return

    # Fetch existing keyboard — preserve existing buttons (💡 View Answer)
    # OPTIMIZED: Use in-memory button tracker first, fall back to get_messages()
    existing_rows = []
    already_has_tv = False  # Duplicate detection for 📺 View Answer
    
    tracked_rows = _get_tracked_buttons(dst_chat_id, dst_msg_id)
    if tracked_rows:
        # FAST PATH: Use in-memory tracker (0 API calls)
        for row_data in tracked_rows:
            row_btns = []
            for (lbl, url) in row_data:
                row_btns.append(InlineKeyboardButton(lbl, url=url))
                if lbl == "📺 View Answer":
                    already_has_tv = True
            existing_rows.append(row_btns)
        print(f"[ANSWER-TOPIC] Using in-memory tracker: {len(existing_rows)} rows on poll dst={dst_msg_id}")
    else:
        # SLOW PATH: Fetch from Telegram
        try:
            dst_msg = await _rate_limited_call(
                lambda: bot_client.get_messages(dst_chat_id, dst_msg_id),
                dst_chat_id, f"get_msg_answer_{dst_msg_id}"
            )
        except Exception as e:
            print(f"[ANSWER-TOPIC] Failed to fetch dst msg: {e}")
            return
        
        if dst_msg and dst_msg.reply_markup and hasattr(dst_msg.reply_markup, 'inline_keyboard'):
            tracked_row_data = []
            for row in dst_msg.reply_markup.inline_keyboard:
                row_btns = []
                row_data = []
                for btn in row:
                    if btn.url:
                        row_btns.append(InlineKeyboardButton(btn.text, url=btn.url))
                        row_data.append((btn.text, btn.url))
                        if hasattr(btn, 'text') and btn.text == "📺 View Answer":
                            already_has_tv = True
                    elif btn.callback_data:
                        row_btns.append(InlineKeyboardButton(btn.text, callback_data=btn.callback_data))
                existing_rows.append(row_btns)
                tracked_row_data.append(row_data)
            # Cache for future calls
            _track_buttons(dst_chat_id, dst_msg_id, tracked_row_data)

    # Skip if 📺 button already exists (prevents duplicates from retries/race conditions)
    if already_has_tv:
        print(f"[ANSWER-TOPIC] ⏭️ 📺 View Answer already exists on poll dst={dst_msg_id} — skipping duplicate")
        return

    # Append 📺 button as new row
    existing_rows.append([InlineKeyboardButton("📺 View Answer", url=answer_url)])
    
    # Update tracker
    tracked_rows_copy = list(tracked_rows) if tracked_rows else []
    tracked_rows_copy.append([("📺 View Answer", answer_url)])
    _track_buttons(dst_chat_id, dst_msg_id, tracked_rows_copy)

    try:
        await _rate_limited_call(
            lambda: bot_client.edit_message_reply_markup(
                chat_id=dst_chat_id,
                message_id=dst_msg_id,
                reply_markup=InlineKeyboardMarkup(existing_rows),
            ),
            dst_chat_id, f"add_tv_btn_{dst_msg_id}"
        )
        print(f"[ANSWER-TOPIC] ✅ Topic button added to poll dst={dst_msg_id} → {answer_url}")
    except Exception as e:
        print(f"[ANSWER-TOPIC] Failed to edit poll dst={dst_msg_id}: {e}")


async def resolve_pending_link_rewrites(
    bot_client, ubot, source_channel, dest_channel_id_int,
    dest_channel_username, source_channel_username, uid,
    source_channel_id=None, multi_source_channels=None,
    combined_msg_id_map=None,
):
    """
    ULTRA ROBUST link resolver — runs at the START of every batch/auto-sync
    AND periodically during monitoring.

    MULTI-SOURCE: Now loads msg_id_map from ALL source channels the user has,
    so cross-channel links (e.g. channel B links in channel A messages) are
    also resolved.

    Finds all messages with unresolved=True in MongoDB.
    Loads COMPLETE msg_id_map (ALL previous + current mappings from ALL channels).
    Rewrites links and edits destination messages.
    Marks resolved in MongoDB.

    Args:
        multi_source_channels: Optional list of dicts describing all source channels.
            If provided, loads combined map from ALL channels for cross-channel rewriting.
            Format: [{"channel": str, "username": str|None, "numeric_id": int|None}, ...]
        combined_msg_id_map: Optional pre-built combined map (avoids re-loading from MongoDB).
            If provided, skips the map loading step entirely.
    """
    try:
        cursor = unresolved_links_collection.find({
            "user_id": uid,
            "source_channel": str(source_channel),
            "unresolved": True,
        })
        unresolved = await cursor.to_list(length=None)
    except Exception as e:
        print(f"[LINK-REWRITE-RESUME] Failed to query unresolved: {e}")
        return

    if not unresolved:
        return  # No logging noise for the common case

    print(f"[LINK-REWRITE-RESUME] Found {len(unresolved)} messages with unresolved links — processing now")

    # Load COMPLETE msg_id_map from MongoDB
    # If a pre-built combined map was passed in, use it directly (avoids re-loading)
    if combined_msg_id_map is not None:
        complete_msg_id_map = combined_msg_id_map
        print(f"[LINK-REWRITE-RESUME] Using pre-built combined msg_id_map: {len(complete_msg_id_map)} mappings")
    elif multi_source_channels:
        # Load combined map from ALL source channels
        all_src_channels = [ch_info["channel"] for ch_info in multi_source_channels]
        complete_msg_id_map, _ = await load_combined_msg_id_map(uid, all_src_channels)
        print(f"[LINK-REWRITE-RESUME] Loaded COMBINED msg_id_map: {len(complete_msg_id_map)} mappings "
              f"from {len(multi_source_channels)} source channels")
    else:
        # Single-source mode (backward compatible)
        complete_msg_id_map, _, _ = await load_upload_map(uid, str(source_channel))
        print(f"[LINK-REWRITE-RESUME] Loaded msg_id_map: {len(complete_msg_id_map)} mappings (single-source)")

    # If source_channel_id wasn't provided but source_channel is numeric, derive it
    _src_ch_id = source_channel_id
    if not _src_ch_id:
        _src_str = str(source_channel)
        if _src_str.lstrip('-').isdigit():
            _src_ch_id = int(_src_str)

    resolved_count = 0
    still_pending = 0
    deleted_count = 0
    edit_fail_count = 0
    skipped_no_new_mappings = 0
    flood_wait_sleeping = False

    for item in unresolved:
        dst_chat_id = item["dst_chat_id"]
        dst_msg_id = item["dst_msg_id"]
        src_msg_id = item.get("src_msg_id")

        # ══════════════════════════════════════════════════════════════
        # SMART SKIP: Check if ANY of this message's unresolved src_msg IDs
        # are now in the map. If NONE are, skip the message entirely —
        # no point in editing since the content won't change.
        # This breaks the FLOOD_WAIT + MESSAGE_NOT_MODIFIED death spiral.
        # ══════════════════════════════════════════════════════════════
        stored_unresolved_ids = item.get("unresolved_src_ids", [])
        if not stored_unresolved_ids and src_msg_id:
            stored_unresolved_ids = [src_msg_id]

        if stored_unresolved_ids and complete_msg_id_map:
            # Check if ANY of the unresolved IDs are now in the map
            newly_resolvable = [sid for sid in stored_unresolved_ids
                                if int(sid) in complete_msg_id_map]
            if not newly_resolvable:
                # None of the previously-unresolved IDs are in the map yet.
                # Skip this message — editing it would produce identical content
                # → MESSAGE_NOT_MODIFIED → death spiral.
                skipped_no_new_mappings += 1
                continue

        # Fetch the current text of the destination message
        # FIX: Use ubot (user client) first for fetching — it has broader access.
        # bot_client can fail with CHANNEL_INVALID if it can't resolve the chat.
        # For EDITING, we still prefer bot_client since it's admin in the dest channel.
        dst_msg = None
        fetch_client = None
        for _client in [ubot, bot_client]:
            if not _client:
                continue
            try:
                dst_msg = await _client.get_messages(dst_chat_id, dst_msg_id)
                if dst_msg and not getattr(dst_msg, "empty", True):
                    fetch_client = _client
                    break
                # Message returned but empty/deleted — check next client
            except Exception as e:
                # FloodWait during get_messages — skip this message instead of sleeping
                fw_secs = _extract_flood_wait_local(e)
                if fw_secs:
                    print(f"[LINK-REWRITE-RESUME] FloodWait {fw_secs}s during get_messages — skipping dst={dst_msg_id}")
                    break
                else:
                    print(f"[LINK-REWRITE-RESUME] {type(_client).__name__} failed to fetch dst={dst_msg_id}: {e}")
                continue

        # If we got a message but it's empty/deleted → mark resolved to stop retrying
        if dst_msg and getattr(dst_msg, "empty", True):
            await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
            deleted_count += 1
            continue

        # For editing: prefer ubot (user client) — it posted most messages via batch.
        # Bot CANNOT edit messages it didn't send → MESSAGE_AUTHOR_REQUIRED.
        # Try ubot first, then bot_client (can edit its own messages).
        # Fall back to fetch_client if neither primary client works.
        client_to_use = fetch_client  # ubot is usually the fetch_client
        if client_to_use is None:
            client_to_use = bot_client if bot_client else None

        # Determine fallback client for MESSAGE_AUTHOR_REQUIRED retry
        # If client_to_use is ubot, fallback is bot_client (and vice versa)
        _edit_clients = [c for c in [fetch_client, bot_client] if c is not None and c is not client_to_use]
        fallback_edit_client = _edit_clients[0] if _edit_clients else None

        if not fetch_client:
            # Both clients failed to fetch this message.
            # Track how many times we've failed — if it keeps happening,
            # the message might be deleted. After 5 consecutive failures,
            # mark as resolved to stop retrying.
            _fail_count = item.get("fetch_fail_count", 0) + 1
            if _fail_count >= 5:
                print(f"[LINK-REWRITE-RESUME] dst={dst_msg_id} failed fetch {_fail_count}x — marking resolved (likely deleted)")
                await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
                deleted_count += 1
            else:
                try:
                    await unresolved_links_collection.update_one(
                        {"user_id": uid, "source_channel": str(source_channel),
                         "dst_chat_id": dst_chat_id, "dst_msg_id": dst_msg_id},
                        {"$set": {"fetch_fail_count": _fail_count, "updated_at": datetime.now()}},
                    )
                except Exception:
                    pass
                print(f"[LINK-REWRITE-RESUME] No client available to fetch/edit dst={dst_msg_id} (fail #{_fail_count}) — skipping")
            continue

        current_text = ""
        is_caption = False
        # Also capture entities for entity-level URL rewriting
        dst_entities = None
        if dst_msg.text:
            current_text = dst_msg.text.markdown if hasattr(dst_msg.text, 'markdown') else str(dst_msg.text)
            dst_entities = dst_msg.entities
        elif dst_msg.caption:
            current_text = dst_msg.caption.markdown if hasattr(dst_msg.caption, 'markdown') else str(dst_msg.caption)
            is_caption = True
            dst_entities = dst_msg.caption_entities

        if not current_text:
            await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
            resolved_count += 1
            continue

        # Rewrite links using complete map — MULTI-SOURCE for cross-channel link rewriting
        rewritten_text, still_has_unresolved = rewrite_telegram_links(
            current_text, source_channel, dest_channel_id_int,
            dest_channel_username, complete_msg_id_map,
            source_channel_username=source_channel_username,
            source_channel_id=_src_ch_id,
            multi_source_channels=multi_source_channels,
        )
        
        # ULTRA PRO MAX: Also rewrite entity URLs (blue clickable links)
        # Only rewrite text_link entities (which have a .url attribute).
        # Do NOT convert 'url' entities to 'text_link' — that modifies raw_text
        # and causes offset/length corruption. Instead, 'url' entities (bare links
        # in text) are handled by the markdown-level rewrite above.
        rewritten_entities = None
        if dst_entities:
            raw_dst_text = str(dst_msg.text) if dst_msg.text else (str(dst_msg.caption) if dst_msg.caption else '')
            rewritten_entities, ent_unresolved, _modified_raw = rewrite_entity_urls(
                dst_entities, source_channel, dest_channel_id_int,
                dest_channel_username, complete_msg_id_map,
                source_channel_username=source_channel_username,
                source_channel_id=_src_ch_id,
                raw_text=raw_dst_text,
                skip_url_entity_conversion=True,  # CRITICAL FIX: don't convert url→text_link (causes garbled text)
                multi_source_channels=multi_source_channels,
            )
            if ent_unresolved:
                still_has_unresolved = True

        # Prefer entity-level edit (preserves blue links) over markdown edit
        # CRITICAL: Always use ORIGINAL raw_dst_text (not _modified_raw) for entity edits
        # because skip_url_entity_conversion=True means _modified_raw == raw_dst_text
        edit_success = False
        if rewritten_entities:
            _entity_edit_text = raw_dst_text  # Use original raw text — entity URLs are in .url attributes
            try:
                if not is_caption:
                    await client_to_use.edit_message_text(
                        chat_id=dst_chat_id, message_id=dst_msg_id,
                        text=_entity_edit_text, entities=rewritten_entities
                    )
                else:
                    await client_to_use.edit_message_caption(
                        chat_id=dst_chat_id, message_id=dst_msg_id,
                        caption=_entity_edit_text, caption_entities=rewritten_entities
                    )
                _edlog(f"[LINK-REWRITE-RESUME] Entity-based edit for dst={dst_msg_id} — {len(rewritten_entities)} entities rewritten")
                # Also do markdown edit if text changed too (for bare URL text rewriting)
                if rewritten_text != current_text:
                    try:
                        if not is_caption:
                            await _safe_markdown_edit(
                                client_to_use.edit_message_text,
                                f"link_rewrite_resume_md_{dst_msg_id}",
                                chat_id=dst_chat_id, message_id=dst_msg_id,
                                text=rewritten_text
                            )
                        else:
                            await _safe_markdown_edit(
                                client_to_use.edit_message_caption,
                                f"link_rewrite_resume_md_{dst_msg_id}",
                                chat_id=dst_chat_id, message_id=dst_msg_id,
                                caption=rewritten_text
                            )
                    except Exception:
                        pass  # Entity edit already succeeded
                edit_success = True
            except FloodWait as fw:
                fw_secs = getattr(fw, 'value', 37)
                # FloodWait → skip this message instead of sleeping
                print(f"[LINK-REWRITE-RESUME] FloodWait {fw_secs}s during entity edit for dst={dst_msg_id} — skipping")
                edit_fail_count += 1
                continue
            except Exception as ent_err:
                # Check for FLOOD_WAIT on entity edit — sleep, then skip to next message
                _fw = _extract_flood_wait_local(ent_err)
                if _fw:
                    print(f"[LINK-REWRITE-RESUME] FloodWait {_fw}s on entity edit for dst={dst_msg_id} — skipping")
                    edit_fail_count += 1
                    continue  # Skip to next message — will retry this one next time
                # Check for permanent errors
                _err_str = str(ent_err)
                # ── MESSAGE_AUTHOR_REQUIRED → try fallback client ──
                if 'MESSAGE_AUTHOR_REQUIRED' in _err_str and fallback_edit_client is not None:
                    print(f"[LINK-REWRITE-RESUME] {type(client_to_use).__name__} not author of dst={dst_msg_id} — trying fallback {type(fallback_edit_client).__name__}")
                    try:
                        if not is_caption:
                            await fallback_edit_client.edit_message_text(
                                chat_id=dst_chat_id, message_id=dst_msg_id,
                                text=_entity_edit_text, entities=rewritten_entities
                            )
                        else:
                            await fallback_edit_client.edit_message_caption(
                                chat_id=dst_chat_id, message_id=dst_msg_id,
                                caption=_entity_edit_text, caption_entities=rewritten_entities
                            )
                        edit_success = True
                        _edlog(f"[LINK-REWRITE-RESUME] Fallback client entity edit succeeded for dst={dst_msg_id}")
                        # Continue to next message on success
                    except Exception as fb_err:
                        _fb_str = str(fb_err)
                        if 'MESSAGE_AUTHOR_REQUIRED' in _fb_str:
                            print(f"[LINK-REWRITE-RESUME] Both clients rejected: not author of dst={dst_msg_id}")
                        else:
                            _edlog(f"[LINK-REWRITE-RESUME] Fallback entity edit also failed for dst={dst_msg_id}: {fb_err}")
                if edit_success:
                    pass  # Already handled above
                elif any(code in _err_str for code in [
                    'MESSAGE_ID_INVALID', 'MessageNotModified', 'MESSAGE_NOT_MODIFIED',
                    'ChatAdminRequired', 'MESSAGE_EDIT_TIME_EXPIRED',
                ]):
                    print(f"[LINK-REWRITE-RESUME] Entity edit permanent error for dst={dst_msg_id}: {_err_str[:80]}")
                    await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
                    resolved_count += 1
                    continue
                elif not edit_success:
                    _edlog(f"[LINK-REWRITE-RESUME] Entity-based edit failed for dst={dst_msg_id}: {ent_err}, trying markdown")
                    # Fall through to markdown edit below
                    rewritten_entities = None  # Force markdown path
        
        if not rewritten_entities and rewritten_text != current_text:
            # Edit destination message with rewritten text
            edit_success = False
            # Try 1: Markdown edit (preserves blue links)
            try:
                if not is_caption:
                    await _safe_markdown_edit(
                        client_to_use.edit_message_text,
                        f"link_rewrite_resume_{dst_msg_id}",
                        chat_id=dst_chat_id, message_id=dst_msg_id,
                        text=rewritten_text
                    )
                else:
                    await _safe_markdown_edit(
                        client_to_use.edit_message_caption,
                        f"link_rewrite_resume_{dst_msg_id}",
                        chat_id=dst_chat_id, message_id=dst_msg_id,
                        caption=rewritten_text
                    )
                edit_success = True
            except FloodWait as fw:
                fw_secs = getattr(fw, 'value', 37)
                # FloodWait → skip this message instead of sleeping
                print(f"[LINK-REWRITE-RESUME] FloodWait {fw_secs}s during markdown edit for dst={dst_msg_id} — skipping")
                edit_fail_count += 1
                continue
            except Exception as e:
                err_str = str(e)
                # Permanent errors — mark resolved to stop retrying
                if any(code in err_str for code in [
                    'MESSAGE_ID_INVALID', 'MessageNotModified', 'MESSAGE_NOT_MODIFIED',
                    'ChatAdminRequired', 'MESSAGE_EDIT_TIME_EXPIRED', 'MESSAGE_DELETE_FAILED'
                ]):
                    print(f"[LINK-REWRITE-RESUME] Marking dst={dst_msg_id} as resolved (permanent error: {err_str[:80]})")
                    await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
                    resolved_count += 1
                    continue

                # Try 2: Plain text edit (no markdown — loses blue links but content is updated)
                # This handles cases where markdown parsing fails on complex text
                if 'Parse' in err_str or 'MARKDOWN' in err_str.upper() or 'Can' in err_str:
                    print(f"[LINK-REWRITE-RESUME] Markdown edit failed for dst={dst_msg_id}, trying plain text edit...")
                    try:
                        if not is_caption:
                            await client_to_use.edit_message_text(
                                chat_id=dst_chat_id, message_id=dst_msg_id,
                                text=rewritten_text
                            )
                        else:
                            await client_to_use.edit_message_caption(
                                chat_id=dst_chat_id, message_id=dst_msg_id,
                                caption=rewritten_text
                            )
                        edit_success = True
                        print(f"[LINK-REWRITE-RESUME] Plain text edit succeeded for dst={dst_msg_id} (blue links may be lost)")
                    except FloodWait as fw2:
                        fw_secs2 = getattr(fw2, 'value', 37)
                        # FloodWait → skip this message instead of sleeping
                        print(f"[LINK-REWRITE-RESUME] FloodWait {fw_secs2}s during plain text edit for dst={dst_msg_id} — skipping")
                        edit_fail_count += 1
                    except Exception as e2:
                        err_str2 = str(e2)
                        if any(code in err_str2 for code in [
                            'MESSAGE_ID_INVALID', 'MessageNotModified', 'MESSAGE_NOT_MODIFIED',
                            'ChatAdminRequired', 'MESSAGE_EDIT_TIME_EXPIRED'
                        ]):
                            await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
                            resolved_count += 1
                            continue
                        print(f"[LINK-REWRITE-RESUME] Plain text edit also failed for dst={dst_msg_id}: {e2}")
                        edit_fail_count += 1

                if not edit_success:
                    edit_fail_count += 1
                    continue

        # Rate-limit: delay between edits to avoid triggering FloodWait
        # 1.5s between edits ≈ 40 edits/min — safe for Telegram API
        if edit_success:
            await asyncio.sleep(1.5)

        if still_has_unresolved:
            still_pending += 1
            # Update the stored unresolved_src_ids so the SMART SKIP
            # logic works on the next cycle — only the remaining
            # unresolved IDs will be checked.
            remaining_unresolved = [sid for sid in stored_unresolved_ids
                                    if int(sid) not in complete_msg_id_map]
            try:
                await unresolved_links_collection.update_one(
                    {"user_id": uid, "source_channel": str(source_channel),
                     "dst_chat_id": dst_chat_id, "dst_msg_id": dst_msg_id},
                    {"$set": {
                        "unresolved_src_ids": remaining_unresolved,
                        "updated_at": datetime.now(),
                    }},
                )
            except Exception:
                pass
            print(f"[LINK-REWRITE-RESUME] dst={dst_msg_id} partially resolved "
                  f"({len(stored_unresolved_ids) - len(remaining_unresolved)} links fixed, "
                  f"{len(remaining_unresolved)} still pending)")
        else:
            await mark_links_resolved(uid, source_channel, dst_chat_id, dst_msg_id)
            resolved_count += 1
            print(f"[LINK-REWRITE-RESUME] ✅ dst={dst_msg_id} fully resolved")

        # Rate-limit: small delay between edits to avoid FLOOD_WAIT
        # Telegram allows ~30 edits per minute per chat; with 60 messages
        # at 0.5s each we stay well under the limit
        if len(unresolved) > 5:
            await asyncio.sleep(0.5)

    if resolved_count > 0 or still_pending > 0 or edit_fail_count > 0 or skipped_no_new_mappings > 0:
        print(
            f"[LINK-REWRITE-RESUME] Done — "
            f"resolved={resolved_count} still_pending={still_pending} "
            f"deleted={deleted_count} edit_failed={edit_fail_count} "
            f"skipped={skipped_no_new_mappings}"
        )


def _build_source_patterns(source_channel, source_channel_username=None, source_channel_id=None):
    """Build URL patterns for ONE source channel.

    Returns a list of pattern strings suitable for t.me/ URL matching.
    Each pattern captures 2 groups:
      Group 1 = base message ID (e.g. '123')
      Group 2 = thread/reply ID (e.g. '456') — may be None

    Also returns the clean_id and username for tg://resolve pattern.
    """
    source_str = str(source_channel)
    source_is_public = not source_str.lstrip('-').isdigit()

    _src_clean_id = None
    if not source_is_public:
        clean_id = source_str.lstrip('-')
        if clean_id.startswith('100'):
            clean_id = clean_id[3:]
        if clean_id:
            _src_clean_id = clean_id
    elif source_channel_id:
        clean_id = str(source_channel_id).lstrip('-')
        if clean_id.startswith('100'):
            clean_id = clean_id[3:]
        if clean_id:
            _src_clean_id = clean_id

    _src_username = source_channel_username
    if not _src_username and source_is_public:
        _src_username = source_str

    url_patterns = []
    if _src_clean_id:
        url_patterns.append(r'c/' + re.escape(_src_clean_id) + r'/(\d+)(?:/(\d+))?')
    if _src_username:
        url_patterns.append(re.escape(_src_username) + r'/(\d+)(?:/(\d+))?')

    return url_patterns, _src_clean_id, _src_username


def rewrite_telegram_links(text, source_channel, dest_channel_id, dest_channel_username, msg_id_map,
                           source_channel_username=None, source_channel_id=None,
                           multi_source_channels=None):
    """Rewrite Telegram message links from source channel(s) to destination channel.

    MULTI-SOURCE CAPABLE — handles links from MULTIPLE source channels in one pass.
    A single message can contain links to channels A, B, C — we rewrite ALL of them.

    When multi_source_channels is provided, it should be a list of dicts:
        [
            {"channel": "-1002563279588", "username": None, "numeric_id": -1002563279588},
            {"channel": "-1003900746078", "username": "other_chan", "numeric_id": -1003900746078},
            ...
        ]
    Each entry describes one source channel with its resolved username and numeric ID.

    When multi_source_channels is None or empty, falls back to single-source mode
    using source_channel/source_channel_username/source_channel_id.

    Handles ALL known Telegram link formats:
      1. Bare URLs:          https://t.me/c/1234567/123
      2. Markdown links:     [Click Here](https://t.me/c/1234567/123)
      3. Both public/private: t.me/username/123 or t.me/c/1234567/123
      4. Thread links:       https://t.me/c/1234567/123/456 (preserves thread ID 456)
      5. Query params:       https://t.me/c/1234567/123?single (preserved in match)
      6. Deep links:         tg://resolve?domain=username&post=123
      7. Short links:        t.me/username/123 (without https://)
      8. DUAL-FORMAT:        Matches BOTH private (c/XXX/) and public (username/) formats

    Args:
        text: The text/caption to process
        source_channel: Source channel identifier (primary, for backward compat)
        dest_channel_id: Destination channel ID (integer like -1001234567890). Must be valid.
        dest_channel_username: Destination channel username (string or None if private)
        msg_id_map: Dict mapping source_msg_id (int) -> dest_msg_id (int)
        source_channel_username: Source channel's public username (if known).
        source_channel_id: Source channel's numeric ID (int, e.g. -1001234567890).
        multi_source_channels: List of dicts for multi-source mode (see above).

    Returns:
        (rewritten_text, has_unresolved_links) tuple
    """
    if not text:
        return text, False

    # GUARD: If dest_channel_id is not a valid channel ID, skip rewriting entirely.
    if dest_channel_id is None:
        _edlog(f"[LINK-REWRITE] SKIP: dest_channel_id is None — cannot rewrite links")
        return text, False

    dest_id_str = str(dest_channel_id)
    if not dest_id_str.lstrip('-').isdigit():
        _edlog(f"[LINK-REWRITE] SKIP: dest_channel_id={dest_channel_id} is not a numeric ID")
        return text, False

    # Positive IDs are user chats, not channels — links would be broken
    if int(dest_channel_id) > 0:
        _edlog(f"[LINK-REWRITE] SKIP: dest_channel_id={dest_channel_id} is a user chat (positive), not a channel")
        return text, False

    has_unresolved = False
    rewrite_count = 0

    # ═══════════════════════════════════════════════════════════════
    # BUILD URL PATTERNS — MULTI-SOURCE (all source channels)
    # ═══════════════════════════════════════════════════════════════

    # Build the full URL prefix for constructing replacement links
    if dest_channel_username:
        dest_url_prefix = f'https://t.me/{dest_channel_username}/'
    else:
        clean_dest = str(dest_channel_id).lstrip('-')
        if clean_dest.startswith('100'):
            clean_dest = clean_dest[3:]
        dest_url_prefix = f'https://t.me/c/{clean_dest}/'

    def build_dest_url(dest_msg_id):
        return f'{dest_url_prefix}{dest_msg_id}'

    # Collect ALL source channel patterns
    # Each entry: (url_patterns_list, clean_id, username, channel_label)
    all_source_entries = []

    if multi_source_channels:
        # Multi-source mode: patterns from ALL channels
        for ch_info in multi_source_channels:
            ch = ch_info.get("channel", "")
            ch_username = ch_info.get("username")
            ch_numeric_id = ch_info.get("numeric_id")
            patterns, clean_id, username = _build_source_patterns(ch, ch_username, ch_numeric_id)
            if patterns or username:
                all_source_entries.append((patterns, clean_id, username, ch))
    else:
        # Single-source mode (backward compatible)
        patterns, clean_id, username = _build_source_patterns(
            source_channel, source_channel_username, source_channel_id
        )
        all_source_entries.append((patterns, clean_id, username, str(source_channel)))

    # Deduplicate patterns across channels (avoid applying same pattern twice)
    seen_url_pats = set()
    all_url_patterns = []       # (url_pat_str, channel_label)
    all_tg_resolve = []         # (username, channel_label)

    for patterns, clean_id, username, ch_label in all_source_entries:
        for pat in patterns:
            if pat not in seen_url_pats:
                seen_url_pats.add(pat)
                all_url_patterns.append((pat, ch_label))
        if username:
            tg_key = f"tg://{username}"
            if tg_key not in seen_url_pats:
                seen_url_pats.add(tg_key)
                all_tg_resolve.append((username, ch_label))

    if not all_url_patterns and not all_tg_resolve:
        _edlog(f"[LINK-REWRITE] SKIP: No URL patterns could be built from any source channel")
        return text, False

    _edlog(f"[LINK-REWRITE] multi_source={len(all_source_entries)} channels "
           f"url_patterns={len(all_url_patterns)} tg_resolve={len(all_tg_resolve)} "
           f"dest_id={dest_channel_id} dest_username={dest_channel_username} "
           f"map_size={len(msg_id_map)}")

    # ═══════════════════════════════════════════════════════════════
    # PASS 1: Process t.me/ URL patterns (both private and public formats)
    # ═══════════════════════════════════════════════════════════════
    result = text
    for url_pat, ch_label in all_url_patterns:
        # Pattern 1a: Markdown links [text](https://t.me/...)
        md_pattern = re.compile(
            r'\[([^\]]*)\]\(https?://t\.me/' + url_pat + r'(?:\?[^\)\s]*)?\)',
            re.IGNORECASE
        )

        def replace_md_link(match):
            nonlocal has_unresolved, rewrite_count
            label = match.group(1)
            base_msg_id = int(match.group(2))
            thread_id_str = match.group(3)
            dest_msg_id = msg_id_map.get(base_msg_id)

            if dest_msg_id:
                new_url = build_dest_url(dest_msg_id)
                if thread_id_str:
                    new_url = f'{new_url}/{thread_id_str}'
                rewrite_count += 1
                _edlog(f"[LINK-REWRITE] MD link: [{label[:30]}](src_msg={base_msg_id}) → dst_msg={dest_msg_id}")
                return f'[{label}]({new_url})'
            else:
                has_unresolved = True
                _edlog(f"[LINK-REWRITE] MD link unresolved: src_msg={base_msg_id}, keeping original")
                return match.group(0)

        result = md_pattern.sub(replace_md_link, result)

        # Pattern 1b: Bare URLs (not inside markdown [...](...) syntax)
        bare_pattern = re.compile(
            r'(?<!\]\()https?://t\.me/' + url_pat + r'(?:\?[^\s]*)?',
            re.IGNORECASE
        )

        def replace_bare_link(match):
            nonlocal has_unresolved, rewrite_count
            base_msg_id = int(match.group(1))
            thread_id_str = match.group(2)
            dest_msg_id = msg_id_map.get(base_msg_id)

            if dest_msg_id:
                new_url = build_dest_url(dest_msg_id)
                if thread_id_str:
                    new_url = f'{new_url}/{thread_id_str}'
                rewrite_count += 1
                _edlog(f"[LINK-REWRITE] Bare URL: src_msg={base_msg_id} → dst_msg={dest_msg_id}")
                return new_url
            else:
                has_unresolved = True
                _edlog(f"[LINK-REWRITE] Bare URL unresolved: src_msg={base_msg_id}, keeping original")
                return match.group(0)

        result = bare_pattern.sub(replace_bare_link, result)

    # ═══════════════════════════════════════════════════════════════
    # PASS 2: Handle tg://resolve deep links (tg://resolve?domain=username&post=123)
    # ═══════════════════════════════════════════════════════════════
    for tg_username, ch_label in all_tg_resolve:
        # Markdown links with tg:// URLs
        tg_md_pattern = re.compile(
            r'\[([^\]]*)\]\(tg://resolve\?domain=' + re.escape(tg_username) + r'&post=(\d+)(?:&[^)\s]*)?\)',
            re.IGNORECASE
        )

        def replace_tg_md_link(match):
            nonlocal has_unresolved, rewrite_count
            label = match.group(1)
            base_msg_id = int(match.group(2))
            dest_msg_id = msg_id_map.get(base_msg_id)

            if dest_msg_id:
                new_url = build_dest_url(dest_msg_id)
                rewrite_count += 1
                _edlog(f"[LINK-REWRITE] TG deep MD: [{label[:30]}](post={base_msg_id}) → dst_msg={dest_msg_id}")
                return f'[{label}]({new_url})'
            else:
                has_unresolved = True
                _edlog(f"[LINK-REWRITE] TG deep MD unresolved: post={base_msg_id}, keeping original")
                return match.group(0)

        result = tg_md_pattern.sub(replace_tg_md_link, result)

        # Bare tg:// URLs
        tg_bare_pattern = re.compile(
            r'(?<!\]\()tg://resolve\?domain=' + re.escape(tg_username) + r'&post=(\d+)(?:&[^\s]*)?',
            re.IGNORECASE
        )

        def replace_tg_bare_link(match):
            nonlocal has_unresolved, rewrite_count
            base_msg_id = int(match.group(1))
            dest_msg_id = msg_id_map.get(base_msg_id)

            if dest_msg_id:
                new_url = build_dest_url(dest_msg_id)
                rewrite_count += 1
                _edlog(f"[LINK-REWRITE] TG deep bare: post={base_msg_id} → dst_msg={dest_msg_id}")
                return new_url
            else:
                has_unresolved = True
                _edlog(f"[LINK-REWRITE] TG deep bare unresolved: post={base_msg_id}, keeping original")
                return match.group(0)

        result = tg_bare_pattern.sub(replace_tg_bare_link, result)

    _edlog(f"[LINK-REWRITE] SUMMARY: rewrote={rewrite_count} unresolved={has_unresolved} "
           f"map_size={len(msg_id_map)} text_len={len(result)}")

    return result, has_unresolved


def rewrite_entity_urls(entities, source_channel, dest_channel_id, dest_channel_username, msg_id_map,
                        source_channel_username=None, source_channel_id=None, raw_text=None,
                        skip_url_entity_conversion=False, multi_source_channels=None):
    """ULTRA PRO MAX: Rewrite URLs inside Telegram message ENTITIES.

    MULTI-SOURCE CAPABLE — handles entity URLs from MULTIPLE source channels.
    Same multi_source_channels format as rewrite_telegram_links.

    This is the CORE fix for "blue navigable links" — Telegram message entities
    are the real source of clickable links. When Pyrogram converts entities to
    markdown via .markdown property, some edge cases (nested entities, complex
    entity types) may not convert properly.

    By rewriting URLs directly in the entities BEFORE sending, we guarantee
    that EVERY blue clickable link points to the destination channel.

    Handles:
      - text_link entities: inline URL links (blue clickable text → URL)
      - url entities: bare URL text (extracted from raw_text at entity offset,
        then converted to text_link with rewritten URL)

    Args:
        entities: List of Pyrogram MessageEntity objects (from m.entities or m.caption_entities)
        source_channel: Source channel identifier (primary, for backward compat)
        dest_channel_id: Destination channel ID (integer)
        dest_channel_username: Destination channel username (string or None)
        msg_id_map: Dict mapping source_msg_id (int) -> dest_msg_id (int)
        source_channel_username: Source channel username (if known)
        source_channel_id: Source channel numeric ID (if known)
        raw_text: The raw text of the message (needed to extract URLs from 'url' type entities)
        skip_url_entity_conversion: If True, skip 'url' type entities (leave for markdown rewriter)
        multi_source_channels: List of dicts for multi-source mode (same format as rewrite_telegram_links)

    Returns:
        (entities, had_unresolved, modified_raw_text) — rewritten entities list, flag, and modified text
    """
    if not entities or not msg_id_map:
        return entities, False, raw_text

    # GUARD: If dest_channel_id is not valid, skip
    if dest_channel_id is None or int(dest_channel_id) > 0:
        return entities, False, raw_text

    # Build dest URL prefix
    if dest_channel_username:
        dest_url_prefix = f'https://t.me/{dest_channel_username}/'
    else:
        clean_dest = str(dest_channel_id).lstrip('-')
        if clean_dest.startswith('100'):
            clean_dest = clean_dest[3:]
        dest_url_prefix = f'https://t.me/c/{clean_dest}/'

    # Build source URL patterns for matching — MULTI-SOURCE
    _src_patterns = []

    if multi_source_channels:
        # Multi-source mode: build patterns from ALL channels
        for ch_info in multi_source_channels:
            ch = ch_info.get("channel", "")
            ch_username = ch_info.get("username")
            ch_numeric_id = ch_info.get("numeric_id")
            _, clean_id, username = _build_source_patterns(ch, ch_username, ch_numeric_id)
            if clean_id:
                _src_patterns.append(('private', re.compile(r'https?://t\.me/c/' + re.escape(clean_id) + r'/(\d+)(?:/(\d+))?', re.IGNORECASE)))
            if username:
                _src_patterns.append(('public', re.compile(r'https?://t\.me/' + re.escape(username) + r'/(\d+)(?:/(\d+))?', re.IGNORECASE)))
                _src_patterns.append(('public_tg', re.compile(r'tg://resolve\?domain=' + re.escape(username) + r'&post=(\d+)', re.IGNORECASE)))
    else:
        # Single-source mode (backward compatible)
        source_str = str(source_channel)
        source_is_public = not source_str.lstrip('-').isdigit()

        _src_clean_id = None
        if not source_is_public:
            clean_id = source_str.lstrip('-')
            if clean_id.startswith('100'):
                clean_id = clean_id[3:]
            if clean_id:
                _src_clean_id = clean_id
        elif source_channel_id:
            clean_id = str(source_channel_id).lstrip('-')
            if clean_id.startswith('100'):
                clean_id = clean_id[3:]
            if clean_id:
                _src_clean_id = clean_id

        _src_username = source_channel_username
        if not _src_username and source_is_public:
            _src_username = source_str

        if _src_clean_id:
            _src_patterns.append(('private', re.compile(r'https?://t\.me/c/' + re.escape(_src_clean_id) + r'/(\d+)(?:/(\d+))?', re.IGNORECASE)))
        if _src_username:
            _src_patterns.append(('public', re.compile(r'https?://t\.me/' + re.escape(_src_username) + r'/(\d+)(?:/(\d+))?', re.IGNORECASE)))
            _src_patterns.append(('public_tg', re.compile(r'tg://resolve\?domain=' + re.escape(_src_username) + r'&post=(\d+)', re.IGNORECASE)))

    if not _src_patterns:
        return entities, False, raw_text

    had_unresolved = False
    rewrite_count = 0
    new_entities = []

    for entity in entities:
        # Check if this entity has a URL that could be a Telegram link
        entity_type = getattr(entity, 'type', None)
        url = None

        if entity_type == 'text_link':
            url = getattr(entity, 'url', None)
        elif entity_type == 'url':
            if skip_url_entity_conversion:
                new_entities.append(entity)
                continue
            if raw_text:
                offset = getattr(entity, 'offset', 0)
                length = getattr(entity, 'length', 0)
                if offset is not None and length > 0 and offset + length <= len(raw_text):
                    url = raw_text[offset:offset + length]
                    _edlog(f"[ENTITY-REWRITE] Extracted 'url' entity text: '{url[:60]}'")
            if not url:
                new_entities.append(entity)
                continue
        else:
            new_entities.append(entity)
            continue

        if not url:
            new_entities.append(entity)
            continue

        # Try to match and rewrite the URL against ALL source channel patterns
        url_rewritten = False
        for pat_type, pattern in _src_patterns:
            match = pattern.search(url)
            if match:
                base_msg_id = int(match.group(1))
                thread_id_str = match.group(2) if len(match.groups()) > 1 else None
                dest_msg_id = msg_id_map.get(base_msg_id)

                if dest_msg_id:
                    new_url = f'{dest_url_prefix}{dest_msg_id}'
                    if thread_id_str:
                        new_url = f'{new_url}/{thread_id_str}'

                    new_url_full = pattern.sub(lambda m: new_url, url)

                    new_entity = copy.deepcopy(entity)
                    if hasattr(new_entity, 'url'):
                        new_entity.url = new_url_full

                    if entity_type == 'url' and raw_text:
                        new_entity.type = 'text_link'
                        new_entity.url = new_url_full
                        offset = getattr(entity, 'offset', 0)
                        length = getattr(entity, 'length', 0)
                        if offset is not None and length > 0 and offset + length <= len(raw_text):
                            raw_text = raw_text[:offset] + new_url_full + raw_text[offset + length:]
                            length_diff = len(new_url_full) - length
                            new_entity.length = len(new_url_full)
                            for prev_ent in new_entities:
                                prev_offset = getattr(prev_ent, 'offset', 0)
                                if prev_offset > offset:
                                    prev_ent.offset = prev_offset + length_diff

                    new_entities.append(new_entity)

                    rewrite_count += 1
                    _edlog(f"[ENTITY-REWRITE] {entity_type} URL: src_msg={base_msg_id} → dst_msg={dest_msg_id} | {url[:50]} → {new_url_full[:50]}")
                    url_rewritten = True
                    break
                else:
                    had_unresolved = True
                    _edlog(f"[ENTITY-REWRITE] {entity_type} URL unresolved: src_msg={base_msg_id} in map, keeping original")
                    break

        if not url_rewritten:
            new_entities.append(entity)

    if rewrite_count > 0:
        _edlog(f"[ENTITY-REWRITE] Rewrote {rewrite_count} entity URLs (multi_source={bool(multi_source_channels)})")

    return new_entities, had_unresolved, raw_text


async def upd_dlg(c):
    try:
        async for _ in c.get_dialogs(limit=100): pass
        return True
    except Exception as e:
        print(f'Failed to update dialogs: {e}')
        return False

async def resolve_chat(client, chat_id):
    """Resolve a chat peer before interacting with it.
    This ensures Pyrogram has the chat in its internal cache,
    preventing CHAT_ID_INVALID errors.
    Returns the resolved chat_id (int) or the original if resolution fails."""
    try:
        # If chat_id is a username string (public channel), resolve it
        if isinstance(chat_id, str) and not chat_id.lstrip('-').isdigit():
            # It's a username like "channelname" — resolve it
            chat = await client.get_chat(chat_id)
            return chat.id if chat else chat_id
        elif isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id = int(chat_id)
        
        if isinstance(chat_id, int):
            # Try resolve_peer to ensure Pyrogram knows this chat
            try:
                await client.resolve_peer(chat_id)
            except Exception:
                # If resolve_peer fails, try get_chat as fallback
                try:
                    chat = await client.get_chat(chat_id)
                    return chat.id if chat else chat_id
                except Exception:
                    pass
        return chat_id
    except Exception as e:
        print(f"resolve_chat warning: {e}")
        return chat_id


# fixed the old group of 2021-2022 extraction
async def get_msg(c, u, i, d, lt):
    """Fetch a single message from source channel with robust error handling.
    
    All exception paths now LOG the error instead of silently swallowing it.
    This helps diagnose why messages are being missed.
    """
    try:
        if lt == 'public':
            try:
                # Resolve the chat first to avoid CHAT_ID_INVALID
                resolved_i = await resolve_chat(c, i)
                
                if str(i).lower().endswith('bot'):
                    emp[i] = False
                    xm = await u.get_messages(resolved_i, d)
                    emp[i] = getattr(xm, "empty", False)
                    if not emp[i]:
                        emp[i] = True
                        print(f"Bot chat found successfully...")
                        return xm
                    
                if emp.get(i, True):
                    # Try with bot first
                    try:
                        resolved_i = await resolve_chat(c, i)
                        xm = await c.get_messages(resolved_i, d)
                        print(f"fetched by {c.me.username}")
                        emp[i] = getattr(xm, "empty", False)
                        if not emp[i]:
                            return xm
                    except ChannelPrivate as e:
                        print(f"[GET_MSG] Bot: ChannelPrivate — chat={i} msg={d}: {e}")
                        # Channel is private/deleted — bot can never access it
                    except (ChatIdInvalid, PeerIdInvalid) as e:
                        print(f"[GET_MSG] Bot can't access chat {i} msg {d}: {e}")
                    except Exception as e:
                        print(f"[GET_MSG] Bot fetch error chat={i} msg={d}: {e}")
                    
                    # If bot couldn't fetch, try userbot
                    if u:
                        try:
                            resolved_u = await resolve_chat(u, i)
                            xm = await u.get_messages(resolved_u, d)
                            if xm and not getattr(xm, "empty", False):
                                return xm
                        except ChannelPrivate as e:
                            print(f"[GET_MSG] Userbot: ChannelPrivate — chat={i} msg={d}: {e}")
                            # Even userbot can't access — might be kicked/banned
                            # Try joining the chat as last resort
                            print(f"[GET_MSG] Userbot can't access — trying join_chat...")
                            try:
                                await u.join_chat(i)
                                print(f"[GET_MSG] Userbot joined chat {i} successfully!")
                            except UserNotParticipant as jbe:
                                print(f"[GET_MSG] Userbot not participant, join failed: {jbe}")
                            except ChannelPrivate as cpe:
                                print(f"[GET_MSG] Userbot join failed — channel is private/banned: {cpe}")
                            except Exception as je:
                                print(f"[GET_MSG] Userbot join_chat failed: {je}")
                            try:
                                chat = await u.get_chat(f"@{i}" if not str(i).startswith('-') else i)
                                resolved_u = await resolve_chat(u, chat.id)
                                xm = await u.get_messages(resolved_u, d)
                                return xm
                            except ChannelPrivate as e2:
                                print(f"[GET_MSG] Userbot still ChannelPrivate after join: {e2}")
                            except Exception as e2:
                                print(f"[GET_MSG] Userbot fallback also failed chat={i} msg={d}: {e2}")
                        except (ChatIdInvalid, PeerIdInvalid):
                            # Try joining the chat first
                            print(f"[GET_MSG] PeerIdInvalid for chat={i} msg={d} — trying join_chat...")
                            try:
                                await u.join_chat(i)
                                print(f"[GET_MSG] Userbot joined chat {i} successfully!")
                            except UserNotParticipant as jbe:
                                print(f"[GET_MSG] Userbot not participant, join failed: {jbe}")
                            except ChannelPrivate as cpe:
                                print(f"[GET_MSG] Userbot join failed — channel is private/banned: {cpe}")
                            except Exception as je:
                                print(f"[GET_MSG] Userbot join_chat failed: {je}")
                            try:
                                chat = await u.get_chat(f"@{i}" if not str(i).startswith('-') else i)
                                resolved_u = await resolve_chat(u, chat.id)
                                xm = await u.get_messages(resolved_u, d)
                                return xm
                            except Exception as e2:
                                print(f"[GET_MSG] Userbot fallback also failed chat={i} msg={d}: {e2}")
                        except Exception as e:
                            print(f"[GET_MSG] Userbot fetch error chat={i} msg={d}: {e}")
                    
                    print(f"[GET_MSG] All attempts failed for public chat={i} msg={d}")
                    return None
                    
            except Exception as e:
                print(f'[GET_MSG] Error fetching public message chat={i} msg={d}: {e}')
                return None
        else:
            # Private channel
            if u:
                try:
                    # Resolve the chat for the userbot first
                    chat_id_int = int(i) if isinstance(i, str) and i.lstrip('-').isdigit() else i
                    
                    # Try to resolve the peer
                    try:
                        await u.resolve_peer(chat_id_int)
                    except Exception as e:
                        if _is_auth_key_error(e):
                            print(f"[GET_MSG] ⚠️ FATAL: AUTH_KEY_UNREGISTERED during resolve_peer — session is dead!")
                            raise AuthKeyUnregisteredError(f"Session revoked: {e}")
                        # Refresh dialogs and try again
                        print(f"[GET_MSG] resolve_peer failed for {chat_id_int}: {e} — refreshing dialogs...")
                        try:
                            async for _ in u.get_dialogs(limit=200): pass
                        except Exception as dg_e:
                            if _is_auth_key_error(dg_e):
                                print(f"[GET_MSG] ⚠️ FATAL: AUTH_KEY_UNREGISTERED during dialog refresh — session is dead!")
                                raise AuthKeyUnregisteredError(f"Session revoked: {dg_e}")
                        try:
                            await u.resolve_peer(chat_id_int)
                        except Exception as e2:
                            if _is_auth_key_error(e2):
                                raise AuthKeyUnregisteredError(f"Session revoked: {e2}")
                            print(f"[GET_MSG] resolve_peer still failed after dialog refresh for {chat_id_int}: {e2}")
                    
                    # ── Try get_messages with proper ID formats ──
                    # Telegram channel IDs use -100XXXXXXXXXX format.
                    # The old "dash format" (-XXXXXXXXX) was WRONG — it strips
                    # the 100 prefix creating an invalid ID that causes CHAT_ID_INVALID.
                    # We now only use valid Telegram ID formats.
                    
                    # Normalize chat_id to -100 format (standard for channels/supergroups)
                    if isinstance(i, str) and i.lstrip('-').isdigit():
                        chat_id_normalized = int(i)
                        # If it's a negative ID without -100 prefix, add it
                        if chat_id_normalized < 0 and not str(i).startswith('-100'):
                            chat_id_normalized = int(f"-100{abs(chat_id_normalized)}")
                    else:
                        chat_id_normalized = int(i) if isinstance(i, str) else i
                    
                    # Attempt 1: Use normalized -100 format with already-resolved peer
                    try:
                        result = await u.get_messages(chat_id_normalized, d)
                        if result and not getattr(result, "empty", False):
                            return result
                        elif result and getattr(result, "empty", False):
                            print(f"[GET_MSG] -100 format returned empty for chat={chat_id_normalized} msg={d} — message may not exist")
                        else:
                            print(f"[GET_MSG] -100 format returned None for chat={chat_id_normalized} msg={d}")
                    except ChannelPrivate as e:
                        print(f"[GET_MSG] ChannelPrivate — chat={chat_id_normalized} msg={d}: {e}")
                        # Userbot is NOT a member of this private channel — cannot access
                    except (ChatIdInvalid, PeerIdInvalid) as e:
                        print(f"[GET_MSG] -100 format failed chat={chat_id_normalized} msg={d}: {e}")
                    except Exception as e:
                        if _is_auth_key_error(e):
                            print(f"[GET_MSG] ⚠️ FATAL: AUTH_KEY_UNREGISTERED — session is dead! Stopping batch.")
                            raise AuthKeyUnregisteredError(f"Session revoked: {e}")
                        print(f"[GET_MSG] -100 format error chat={chat_id_normalized} msg={d}: {e}")
                    
                    # Attempt 2: Refresh dialogs and retry (peer cache might be stale)
                    try:
                        print(f"[GET_MSG] Refreshing dialogs for chat={chat_id_normalized} msg={d}...")
                        async for _ in u.get_dialogs(limit=200): pass
                        await u.resolve_peer(chat_id_normalized)
                        result = await u.get_messages(chat_id_normalized, d)
                        if result and not getattr(result, "empty", False):
                            return result
                        elif result and getattr(result, "empty", False):
                            print(f"[GET_MSG] Retry also returned empty for chat={chat_id_normalized} msg={d}")
                    except ChannelPrivate as e:
                        print(f"[GET_MSG] ChannelPrivate after dialog refresh — chat={chat_id_normalized} msg={d}: {e}")
                    except Exception as e:
                        if _is_auth_key_error(e):
                            print(f"[GET_MSG] ⚠️ FATAL: AUTH_KEY_UNREGISTERED — session is dead! Stopping batch.")
                            raise AuthKeyUnregisteredError(f"Session revoked: {e}")
                        print(f"[GET_MSG] Dialog refresh retry failed chat={chat_id_normalized} msg={d}: {e}")
                    
                    # Attempt 3: Try with bot client as last resort
                    if c:
                        try:
                            await c.resolve_peer(chat_id_normalized)
                            result = await c.get_messages(chat_id_normalized, d)
                            if result and not getattr(result, "empty", False):
                                print(f"[GET_MSG] Bot client succeeded for chat={chat_id_normalized} msg={d}")
                                return result
                        except Exception as e:
                            print(f"[GET_MSG] Bot client also failed chat={chat_id_normalized} msg={d}: {e}")
                    
                    print(f"[GET_MSG] All attempts failed for private chat={chat_id_normalized} msg={d}")
                    return None
                            
                except Exception as e:
                    print(f'[GET_MSG] Private channel error chat={i} msg={d}: {e}')
                    return None
            print(f"[GET_MSG] No user client for private chat={i} msg={d}")
            return None
    except Exception as e:
        print(f'[GET_MSG] Top-level error chat={i} msg={d}: {e}')
        return None


async def get_ubot(uid):
    bt = await get_user_data_key(uid, "bot_token", None)
    if not bt: return None
    # TEST: Check LRU cache first
    cached = user_bot_cache.get(uid)
    if cached: return cached
    # Fallback: check old UB dict
    if uid in UB: return UB.get(uid)
    try:
        bot = Client(f"user_{uid}", bot_token=bt, api_id=API_ID, api_hash=API_HASH, in_memory=True)
        await bot.start()
        UB[uid] = bot
        user_bot_cache.put(uid, bot)
        log_ram("ubot_created", extra_info={"uid": uid, "bot_cache_size": len(user_bot_cache)})
        return bot
    except Exception as e:
        print(f"Error starting bot for user {uid}: {e}")
        return None

async def get_uclient(uid):
    ud = await get_user_data(uid)
    ubot = UB.get(uid) if uid in UB else user_bot_cache.get(uid)
    # TEST: Check LRU cache first
    cached = user_client_cache.get(uid)
    if cached: return cached
    # Fallback: check old UC dict
    if uid in UC: return UC.get(uid)
    if not ud: return ubot if ubot else get_Y()
    xxx = ud.get('session_string')
    if xxx:
        try:
            ss = dcs(xxx)
            gg = Client(f'{uid}_client', api_id=API_ID, api_hash=API_HASH, device_model="v3saver", session_string=ss, in_memory=True)
            # Add timeout to prevent hanging forever on start
            await asyncio.wait_for(gg.start(), timeout=60)
            await upd_dlg(gg)
            UC[uid] = gg
            user_client_cache.put(uid, gg)
            log_ram("uclient_created", extra_info={"uid": uid, "uc_cache_size": len(user_client_cache)})
            return gg
        except asyncio.TimeoutError:
            print(f'User client start timed out for {uid}')
            return ubot if ubot else get_Y()
        except Exception as e:
            print(f'User client error: {e}')
            return ubot if ubot else get_Y()
    return get_Y()

async def prog(c, t, C, h, m, st, phase="Processing"):
    """Progress callback for download_media and send_video.
    
    Args:
        c: Current bytes transferred
        t: Total bytes
        C: Client instance (unused but required by Pyrogram callback signature)
        h: Chat ID for edit_message_text
        m: Message ID to edit
        st: Start time (for speed calculation)
        phase: Label — "Downloading" or "Uploading" — so user can tell which phase is active
    """
    global P
    p = c / t * 100
    interval = 10 if t >= 100 * 1024 * 1024 else 20 if t >= 50 * 1024 * 1024 else 30 if t >= 10 * 1024 * 1024 else 50
    step = int(p // interval) * interval
    if m not in P or P[m] != step or p >= 100:
        P[m] = step
        c_mb = c / (1024 * 1024)
        t_mb = t / (1024 * 1024)
        bar = '🟢' * int(p / 10) + '🔴' * (10 - int(p / 10))
        speed = c / (time.time() - st) / (1024 * 1024) if time.time() > st else 0
        eta = time.strftime('%M:%S', time.gmtime((t - c) / (speed * 1024 * 1024))) if speed > 0 else '00:00'
        await C.edit_message_text(h, m, f"__**{phase}...**__\n\n{bar}\n\n⚡**__Completed__**: {c_mb:.2f} MB / {t_mb:.2f} MB\n📊 **__Done__**: {p:.2f}%\n🚀 **__Speed__**: {speed:.2f} MB/s\n⏳ **__ETA__**: {eta}\n\n**__Powered by Team SPY__**")
        if p >= 100: P.pop(m, None)


async def _extract_poll_question_media(poll_msg, source_chat, user_client=None, send_client=None):
    """Extract the question image/media from a poll message using ALL available methods.
    
    In newer Telegram, polls can have EMBEDDED question images (not as a reply-to).
    Pyrofork doesn't parse these natively, so we must inspect the raw message.
    
    Returns: tuple (media_file_id, media_type) where media_type is 'photo'|'document'|'video'|'invert_media_forward'|None
    """
    media_file_id = None
    media_type = None
    
    # ── Method 0: Quick check — does the Pyrogram Message have photo/document even with poll? ──
    # In rare cases, Pyrofork might parse both poll AND photo for newer Telegram polls
    if getattr(poll_msg, 'photo', None):
        photo = poll_msg.photo
        media_file_id = photo.file_id if hasattr(photo, 'file_id') else (photo[-1].file_id if hasattr(photo, '__getitem__') else None)
        if media_file_id:
            media_type = 'photo'
            print(f"[POLL-IMG-EXTRACT] Found question photo directly on poll message (Pyrofork parsed it!)")
            return (media_file_id, media_type)
    if getattr(poll_msg, 'document', None):
        media_file_id = poll_msg.document.file_id
        media_type = 'document'
        print(f"[POLL-IMG-EXTRACT] Found question document directly on poll message")
        return (media_file_id, media_type)
    if getattr(poll_msg, 'video', None):
        media_file_id = poll_msg.video.file_id
        media_type = 'video'
        print(f"[POLL-IMG-EXTRACT] Found question video directly on poll message")
        return (media_file_id, media_type)
    
    # ── Method 1: Check raw message for embedded question media ──
    # In newer Telegram, polls with question images store the photo in the raw message
    # but Pyrofork's MessageMediaPoll doesn't parse it. We check the raw message directly.
    try:
        raw_msg = getattr(poll_msg, 'raw', None)
        if raw_msg:
            raw_media = getattr(raw_msg, 'media', None)
            invert_media = getattr(raw_msg, 'invert_media', False)
            grouped_id = getattr(raw_msg, 'grouped_id', None)
            
            print(f"[POLL-IMG-EXTRACT] raw message: invert_media={invert_media}, grouped_id={grouped_id}")
            
            if raw_media:
                # Check if the raw media has any photo/document attributes that Pyrofork missed
                # MessageMediaPoll only has 'poll' and 'results', but in newer layers the
                # poll question image might be stored alongside
                from pyrogram import raw as pyro_raw
                
                # Check for photo inside the raw media (newer Telegram API)
                raw_photo = getattr(raw_media, 'photo', None)
                if raw_photo:
                    if isinstance(raw_photo, pyro_raw.types.Photo):
                        # Extract file_id from the raw Photo object
                        # Photo has sizes attribute with various photo sizes
                        for size in (raw_photo.sizes or []):
                            if hasattr(size, 'file_id'):
                                media_file_id = size.file_id
                                media_type = 'photo'
                                print(f"[POLL-IMG-EXTRACT] Found embedded question photo in raw media, file_id={media_file_id[:40]}...")
                                break
                        if not media_file_id:
                            # Try accessing PhotoSize with location
                            for size in (raw_photo.sizes or []):
                                if hasattr(size, 'location'):
                                    media_file_id = raw_photo
                                    media_type = 'photo_raw'
                                    print(f"[POLL-IMG-EXTRACT] Found raw photo with location (will re-fetch)")
                                    break
                
                # Check for document inside the raw media
                raw_doc = getattr(raw_media, 'document', None)
                if raw_doc and isinstance(raw_doc, pyro_raw.types.Document) and not media_file_id:
                    media_file_id = raw_doc
                    media_type = 'document_raw'
                    print(f"[POLL-IMG-EXTRACT] Found embedded document in raw media")
                
                # IMPORTANT: Check for ANY extra attributes that Pyrofork doesn't know about
                # The newer Telegram API may add a 'media' field to MessageMediaPoll that
                # Pyrofork ignores. We check for unknown attributes.
                known_slots = {'poll', 'results'}
                actual_slots = set(getattr(raw_media, '__slots__', set()))
                extra_slots = actual_slots - known_slots
                if extra_slots:
                    print(f"[POLL-IMG-EXTRACT] Found EXTRA slots in MessageMediaPoll: {extra_slots}")
                    for slot in extra_slots:
                        try:
                            val = getattr(raw_media, slot, None)
                            if val is not None:
                                print(f"[POLL-IMG-EXTRACT]   {slot} = type={type(val).__name__}")
                                # If it's a Photo or Document, use it
                                if isinstance(val, pyro_raw.types.Photo):
                                    for size in (val.sizes or []):
                                        if hasattr(size, 'file_id'):
                                            media_file_id = size.file_id
                                            media_type = 'photo'
                                            print(f"[POLL-IMG-EXTRACT] Found question photo from extra slot '{slot}'")
                                            break
                                elif isinstance(val, pyro_raw.types.Document):
                                    media_file_id = val
                                    media_type = 'document_raw'
                                    print(f"[POLL-IMG-EXTRACT] Found question document from extra slot '{slot}'")
                        except Exception:
                            pass
                
                # Log diagnostic info about the raw media
                print(f"[POLL-IMG-EXTRACT] raw_media type={type(raw_media).__name__} (ID={hex(type(raw_media).ID)})")
                print(f"[POLL-IMG-EXTRACT] raw_media slots={list(getattr(raw_media, '__slots__', []))}")
                # Dump ALL slot values for diagnosis
                for slot in getattr(raw_media, '__slots__', []):
                    try:
                        val = getattr(raw_media, slot, None)
                        val_type = type(val).__name__ if val is not None else 'None'
                        val_len = len(val) if hasattr(val, '__len__') and val is not None else ''
                        print(f"[POLL-IMG-EXTRACT]   {slot}: type={val_type}{f' len={val_len}' if val_len else ''}, is_none={val is None}")
                    except Exception:
                        pass
                print(f"[POLL-IMG-EXTRACT] invert_media={invert_media}")
                
                # If invert_media is True and we didn't find any media yet,
                # mark this for forward-based handling
                if invert_media and not media_file_id:
                    media_file_id = 'invert_media'
                    media_type = 'invert_media_forward'
                    print(f"[POLL-IMG-EXTRACT] invert_media=True but no embedded media found — will try forward")
    except Exception as e:
        print(f"[POLL-IMG-EXTRACT] Error inspecting raw message: {e}")
        import traceback; traceback.print_exc()
    
    # ── Method 2: Re-fetch the message with user_client for complete data ──
    if not media_file_id and user_client:
        try:
            resolved_src = await resolve_chat(user_client, source_chat)
            # Re-fetch the poll message to get complete data
            fresh_msg = await user_client.get_messages(resolved_src, poll_msg.id)
            if fresh_msg:
                # Check if the fresh message has photo that the original didn't
                if getattr(fresh_msg, 'photo', None) and not getattr(poll_msg, 'photo', None):
                    photo = fresh_msg.photo
                    media_file_id = photo.file_id if hasattr(photo, 'file_id') else (photo[-1].file_id if hasattr(photo, '__getitem__') else None)
                    media_type = 'photo'
                    print(f"[POLL-IMG-EXTRACT] Found question photo from re-fetched message!")
                
                # Check raw message from fresh fetch
                if not media_file_id:
                    fresh_raw = getattr(fresh_msg, 'raw', None)
                    if fresh_raw:
                        fresh_raw_media = getattr(fresh_raw, 'media', None)
                        if fresh_raw_media:
                            # Same inspection as above but on the fresh message
                            from pyrogram import raw as pyro_raw
                            fresh_photo = getattr(fresh_raw_media, 'photo', None)
                            if fresh_photo and isinstance(fresh_photo, pyro_raw.types.Photo):
                                for size in (fresh_photo.sizes or []):
                                    if hasattr(size, 'file_id'):
                                        media_file_id = size.file_id
                                        media_type = 'photo'
                                        print(f"[POLL-IMG-EXTRACT] Found embedded question photo from fresh fetch!")
                                        break
                            # Log all attributes of the fresh raw media
                            print(f"[POLL-IMG-EXTRACT] fresh_raw_media type={type(fresh_raw_media).__name__}")
                            print(f"[POLL-IMG-EXTRACT] fresh_raw_media slots={list(getattr(fresh_raw_media, '__slots__', []))}")
                            # Dump all slot values for diagnosis
                            for slot in getattr(fresh_raw_media, '__slots__', []):
                                try:
                                    val = getattr(fresh_raw_media, slot, None)
                                    val_type = type(val).__name__ if val is not None else 'None'
                                    print(f"[POLL-IMG-EXTRACT]   {slot}: type={val_type}, is_none={val is None}")
                                except Exception:
                                    pass
        except ChannelPrivate as e:
            print(f"[POLL-IMG-EXTRACT] ChannelPrivate — user_client can't access source channel: {e}")
        except Exception as e:
            print(f"[POLL-IMG-EXTRACT] Error re-fetching message: {e}")
    
    # ── Method 3: Check grouped messages (album-style poll + image) ──
    if not media_file_id and user_client:
        try:
            grouped_id = getattr(poll_msg, 'media_group_id', None)
            if grouped_id:
                resolved_src = await resolve_chat(user_client, source_chat)
                # Fetch nearby messages to find the grouped image
                # get_media_groups returns all messages in the same group
                try:
                    grouped = await user_client.get_media_group(resolved_src, poll_msg.id)
                    for gmsg in grouped:
                        if gmsg.id != poll_msg.id:
                            if getattr(gmsg, 'photo', None):
                                photo = gmsg.photo
                                media_file_id = photo.file_id if hasattr(photo, 'file_id') else (photo[-1].file_id if hasattr(photo, '__getitem__') else None)
                                media_type = 'photo_grouped'
                                print(f"[POLL-IMG-EXTRACT] Found grouped question photo from msg_id={gmsg.id}")
                                break
                            elif getattr(gmsg, 'document', None):
                                media_file_id = gmsg.document.file_id
                                media_type = 'document_grouped'
                                print(f"[POLL-IMG-EXTRACT] Found grouped question document from msg_id={gmsg.id}")
                                break
                            elif getattr(gmsg, 'video', None):
                                media_file_id = gmsg.video.file_id
                                media_type = 'video_grouped'
                                print(f"[POLL-IMG-EXTRACT] Found grouped question video from msg_id={gmsg.id}")
                                break
                except ChannelPrivate as e:
                    print(f"[POLL-IMG-EXTRACT] get_media_group ChannelPrivate: {e}")
                except Exception as e:
                    print(f"[POLL-IMG-EXTRACT] get_media_group failed: {e}")
        except ChannelPrivate as e:
            print(f"[POLL-IMG-EXTRACT] ChannelPrivate checking grouped messages: {e}")
        except Exception as e:
            print(f"[POLL-IMG-EXTRACT] Error checking grouped messages: {e}")
    
    return (media_file_id, media_type)


async def _upload_question_image(send_client, source_chat, src_reply_id, dest_chat_id, user_client=None, topic_id=None, watermark_text=None):
    """Upload the question image that a poll replies to.
    
    PRIMARY APPROACH: Download media to disk, then re-upload as local file.
    This is more reliable than sending file_id directly because:
    - file_ids are session-specific and may not work cross-client
    - MEDIA_EMPTY / FILE_ID_INVALID errors are common with file_id approach
    
    Falls back to file_id direct if download fails.
    Returns the destination message ID of the uploaded image, or None.
    """
    # Build topic kwargs for forum topic support
    _topic_kw = {}
    if topic_id:
        _topic_kw['message_thread_id'] = topic_id
    
    # Build list of clients to try for fetching from source channel
    fetch_clients = []
    if user_client:
        fetch_clients.append(("user_client", user_client))
    if send_client and send_client != user_client:
        fetch_clients.append(("send_client", send_client))
    
    for client_name, fetch_c in fetch_clients:
        try:
            resolved_src = await resolve_chat(fetch_c, source_chat)
            reply_msg = await fetch_c.get_messages(resolved_src, src_reply_id)
            
            if not reply_msg:
                print(f"[POLL-IMG] {client_name}: reply_msg is None for src_reply_id={src_reply_id}")
                continue
            
            print(f"[POLL-IMG] {client_name}: reply_msg.id={reply_msg.id} has_photo={bool(reply_msg.photo)} has_document={bool(reply_msg.document)} has_video={bool(reply_msg.video)} has_sticker={bool(getattr(reply_msg, 'sticker', None))}")
            
            has_media = reply_msg.photo or reply_msg.document or reply_msg.video
            if not has_media:
                # ── Sticker ──
                if getattr(reply_msg, 'sticker', None):
                    q_sent = await flood_wait_retry(send_client.send_sticker(dest_chat_id, reply_msg.sticker.file_id, **_topic_kw), f"question_sticker_{client_name}")
                    if q_sent:
                        print(f"[POLL-IMG] Uploaded question sticker via {client_name}, dest_id={q_sent.id}")
                        return q_sent.id
                
                # ── Text-only reply (no media) — send the text as context ──
                elif reply_msg.text:
                    txt = reply_msg.text.markdown if hasattr(reply_msg.text, 'markdown') else (reply_msg.text or '')
                    if txt.strip():
                        q_sent = await flood_wait_retry(_safe_markdown_send(send_client.send_message, f"question_text_{client_name}",
                            chat_id=dest_chat_id, text=txt, **_topic_kw), f"question_text_{client_name}")
                        if q_sent:
                            print(f"[POLL-IMG] Uploaded question text via {client_name}, dest_id={q_sent.id}")
                            return q_sent.id
                
                else:
                    print(f"[POLL-IMG] {client_name}: reply msg {src_reply_id} has no sendable media/text (type={type(reply_msg).__name__})")
                    # Try next client
                continue
            
            # ── Fix 1: Download → re-upload (prevents MEDIA_EMPTY + PHOTO_EXT_INVALID) ──
            os.makedirs("downloads", exist_ok=True)
            _dl_base = f"downloads/qimg_{src_reply_id}.jpg"
            print(f"[POLL-IMG] {client_name}: Downloading src_reply_id={src_reply_id} for re-upload...")
            dl_path = None
            try:
                await _download_rate_limiter.acquire()
                dl_path = await fetch_c.download_media(reply_msg, file_name=_dl_base)
            except Exception as dl_err:
                print(f"[POLL-IMG] {client_name}: Download failed: {dl_err}")
            
            if dl_path and os.path.exists(dl_path):
                # ── Apply watermark to question image before re-upload ──
                try:
                    wm = watermark_text or _DEFAULT_WATERMARK_TEXT
                    if wm and wm.strip():
                        _is_img = bool(reply_msg.photo) or (bool(reply_msg.document) and getattr(reply_msg.document, 'mime_type', '').startswith('image/'))
                        _is_vid = bool(reply_msg.video)
                        dl_path = await _apply_watermark_to_file(dl_path, wm, is_video=_is_vid, is_image=_is_img)
                except Exception as _wm_err:
                    print(f"[POLL-IMG] Watermark failed for question img: {_wm_err}")
                
                file_size = os.path.getsize(dl_path)
                print(f"[POLL-IMG] {client_name}: Downloaded {file_size} bytes — re-uploading...")
                try:
                    if reply_msg.photo or (reply_msg.document and getattr(reply_msg.document, 'mime_type', '') and reply_msg.document.mime_type.startswith('image/')):
                        q_sent = await flood_wait_retry(send_client.send_photo(dest_chat_id, dl_path, **_topic_kw), f"question_image_dl_{client_name}")
                    elif reply_msg.document:
                        q_sent = await flood_wait_retry(send_client.send_document(dest_chat_id, dl_path, **_topic_kw), f"question_doc_dl_{client_name}")
                    elif reply_msg.video:
                        q_sent = await flood_wait_retry(send_client.send_video(dest_chat_id, dl_path, **_topic_kw), f"question_video_dl_{client_name}")
                    else:
                        q_sent = await flood_wait_retry(send_client.send_document(dest_chat_id, dl_path, **_topic_kw), f"question_generic_dl_{client_name}")
                    
                    if q_sent:
                        print(f"[POLL-IMG] {client_name}: Uploaded question image (download+re-upload), dest_id={q_sent.id}")
                        return q_sent.id
                    else:
                        print(f"[POLL-IMG] {client_name}: Re-upload returned None")
                except Exception as upload_err:
                    print(f"[POLL-IMG] {client_name}: Re-upload failed: {upload_err}")
                finally:
                    try: os.remove(dl_path)
                    except: pass
            else:
                print(f"[POLL-IMG] {client_name}: Download returned no path — trying file_id direct...")
                # Fallback: try file_id direct
                if reply_msg.photo:
                    photo_id = reply_msg.photo.file_id if hasattr(reply_msg.photo, 'file_id') else reply_msg.photo[-1].file_id
                    try:
                        q_sent = await flood_wait_retry(send_client.send_photo(dest_chat_id, photo_id, **_topic_kw), f"question_image_fid_{client_name}")
                        if q_sent:
                            print(f"[POLL-IMG] {client_name}: Uploaded via file_id fallback, dest_id={q_sent.id}")
                            return q_sent.id
                    except Exception as fid_err:
                        print(f"[POLL-IMG] {client_name}: file_id fallback also failed: {fid_err}")
                elif reply_msg.document:
                    doc = reply_msg.document
                    file_name = getattr(doc, 'file_name', '') or ''
                    try:
                        q_sent = await flood_wait_retry(send_client.send_document(dest_chat_id, doc.file_id, file_name=file_name, **_topic_kw), f"question_doc_fid_{client_name}")
                        if q_sent:
                            print(f"[POLL-IMG] {client_name}: Uploaded document via file_id fallback, dest_id={q_sent.id}")
                            return q_sent.id
                    except Exception as fid_err:
                        print(f"[POLL-IMG] {client_name}: Document file_id fallback also failed: {fid_err}")
                elif reply_msg.video:
                    vid = reply_msg.video
                    try:
                        q_sent = await flood_wait_retry(send_client.send_video(dest_chat_id, vid.file_id, duration=vid.duration, width=vid.width, height=vid.height, **_topic_kw), f"question_video_fid_{client_name}")
                        if q_sent:
                            print(f"[POLL-IMG] {client_name}: Uploaded video via file_id fallback, dest_id={q_sent.id}")
                            return q_sent.id
                    except Exception as fid_err:
                        print(f"[POLL-IMG] {client_name}: Video file_id fallback also failed: {fid_err}")
        
        except ChannelPrivate as e:
            print(f"[POLL-IMG] {client_name}: ChannelPrivate — can't access source: {e}")
            continue
        except Exception as e:
            print(f"[POLL-IMG] {client_name}: Failed to upload question image: {e}")
            continue
    
    # ── LAST RESORT: Try forwarding the entire message ──
    for client_name, fetch_c in fetch_clients:
        try:
            resolved_src = await resolve_chat(fetch_c, source_chat)
            fwd = await fetch_c.forward_messages(dest_chat_id, src_reply_id, resolved_src)
            if fwd:
                fwd_id = fwd.id if hasattr(fwd, 'id') else (fwd[0].id if isinstance(fwd, list) and fwd else None)
                if fwd_id:
                    print(f"[POLL-IMG] Forwarded question image via {client_name}, dest_id={fwd_id}")
                    return fwd_id
        except ChannelPrivate as e:
            print(f"[POLL-IMG] {client_name}: ChannelPrivate — can't access source: {e}")
        except (ChatIdInvalid, PeerIdInvalid) as e:
            print(f"[POLL-IMG] {client_name}: Can't resolve source chat: {e}")
        except Exception as e:
            print(f"[POLL-IMG] {client_name}: Forward fallback also failed: {e}")
            continue
    
    print(f"[POLL-IMG] ALL methods failed to upload question image for src_reply_id={src_reply_id}")
    return None


async def _upload_extracted_question_media(send_client, dest_chat_id, media_file_id, media_type, topic_id=None):
    """Upload extracted question media to the destination channel.
    
    Handles file_id strings, raw Photo objects, and raw Document objects.
    Returns the destination message ID, or None.
    """
    _topic_kw = {}
    if topic_id:
        _topic_kw['message_thread_id'] = topic_id
    
    try:
        if media_type == 'photo' and isinstance(media_file_id, str):
            q_sent = await flood_wait_retry(send_client.send_photo(dest_chat_id, media_file_id, **_topic_kw), "question_embedded_photo")
            if q_sent:
                print(f"[POLL-IMG] Uploaded embedded question photo, dest_id={q_sent.id}")
                return q_sent.id
        
        elif media_type == 'photo_raw':
            # Raw Photo object — need to download and re-upload
            from pyrogram import raw as pyro_raw
            if isinstance(media_file_id, pyro_raw.types.Photo):
                # Download the photo using any available client
                download_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'downloads')
                os.makedirs(download_dir, exist_ok=True)
                download_path = os.path.join(download_dir, f'poll_qimg_{media_file_id.id}')
                # We need a client to download — try using send_client
                await _download_rate_limiter.acquire()
                actual_path = await send_client.download_media(media_file_id, file_name=download_path)
                if actual_path and os.path.exists(actual_path):
                    # Apply watermark before re-upload
                    try:
                        _apply_watermark(actual_path, _DEFAULT_WATERMARK_TEXT)
                    except Exception:
                        pass
                    q_sent = await flood_wait_retry(send_client.send_photo(dest_chat_id, actual_path, **_topic_kw), "question_raw_photo")
                    try:
                        os.unlink(actual_path)
                    except Exception:
                        pass
                    if q_sent:
                        print(f"[POLL-IMG] Uploaded raw question photo, dest_id={q_sent.id}")
                        return q_sent.id
        
        elif media_type == 'document_raw':
            # Raw Document object
            from pyrogram import raw as pyro_raw
            if isinstance(media_file_id, pyro_raw.types.Document):
                # Get file_name from attributes
                file_name = None
                for attr in (media_file_id.attributes or []):
                    if isinstance(attr, pyro_raw.types.DocumentAttributeFilename):
                        file_name = attr.file_name
                        break
                download_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'downloads')
                os.makedirs(download_dir, exist_ok=True)
                download_path = os.path.join(download_dir, f'poll_qimg_doc_{media_file_id.id}')
                await _download_rate_limiter.acquire()
                actual_path = await send_client.download_media(media_file_id, file_name=download_path)
                if actual_path and os.path.exists(actual_path):
                    # Apply watermark to image documents before re-upload
                    _doc_mime = getattr(media_file_id, 'mime_type', '') or ''
                    if _doc_mime.startswith('image/'):
                        try:
                            _apply_watermark(actual_path, _DEFAULT_WATERMARK_TEXT)
                        except Exception:
                            pass
                    q_sent = await flood_wait_retry(
                        send_client.send_document(dest_chat_id, actual_path, file_name=file_name, **_topic_kw),
                        "question_raw_document"
                    )
                    try:
                        os.unlink(actual_path)
                    except Exception:
                        pass
                    if q_sent:
                        print(f"[POLL-IMG] Uploaded raw question document, dest_id={q_sent.id}")
                        return q_sent.id
        
        elif media_type in ('photo_grouped', 'video_grouped') and isinstance(media_file_id, str):
            if 'photo' in media_type:
                q_sent = await flood_wait_retry(send_client.send_photo(dest_chat_id, media_file_id, **_topic_kw), "question_grouped_photo")
            else:
                q_sent = await flood_wait_retry(send_client.send_video(dest_chat_id, media_file_id, **_topic_kw), "question_grouped_video")
            if q_sent:
                print(f"[POLL-IMG] Uploaded grouped question media ({media_type}), dest_id={q_sent.id}")
                return q_sent.id
        
        elif media_type == 'document_grouped' and isinstance(media_file_id, str):
            q_sent = await flood_wait_retry(send_client.send_document(dest_chat_id, media_file_id, **_topic_kw), "question_grouped_document")
            if q_sent:
                print(f"[POLL-IMG] Uploaded grouped question document, dest_id={q_sent.id}")
                return q_sent.id
        
        else:
            print(f"[POLL-IMG] Unknown media_type={media_type} or unsupported media_file_id type={type(media_file_id).__name__}")
    
    except Exception as e:
        print(f"[POLL-IMG] Failed to upload extracted question media: {e}")
        import traceback; traceback.print_exc()
    
    return None


async def _send_poll_with_embedded_image(client, dest_chat_id, poll_kwargs, media_file_id, media_type, user_client=None):
    """Send a poll with an embedded question image using the raw Telegram API.
    
    Pyrofork's send_poll() doesn't support the 'media' parameter in InputMediaPoll.
    This function constructs the raw API call directly to include the question image.
    
    Falls back to regular send_poll if raw API fails.
    Returns the sent Message object, or None.
    """
    from pyrogram import raw as pyro_raw
    
    try:
        # Step 1: Upload the media to get an InputMedia reference
        input_media = None
        
        if isinstance(media_file_id, str) and media_type == 'photo':
            # We have a file_id — need to re-fetch to get an InputPhoto
            # Use messages.uploadMedia or construct InputPhoto from file_id
            # Unfortunately, we can't directly convert file_id to InputPhoto
            # Instead, we upload the photo as a media message, get the raw photo, then use it
            print(f"[POLL-RAW] Attempting to send poll with embedded image via raw API...")
            
            # Upload the image to Telegram servers first (to get access_hash)
            # We send a temporary photo message, extract the InputMedia, then delete it
            try:
                temp_msg = await client.send_photo(dest_chat_id, media_file_id)
                if temp_msg:
                    # Get the raw message to extract InputMedia
                    raw_temp = getattr(temp_msg, 'raw', None)
                    if raw_temp and hasattr(raw_temp, 'media') and raw_temp.media:
                        raw_photo = getattr(raw_temp.media, 'photo', None)
                        if raw_photo and isinstance(raw_photo, pyro_raw.types.Photo):
                            input_media = pyro_raw.types.InputMediaPhoto(
                                id=pyro_raw.types.InputPhoto(
                                    id=raw_photo.id,
                                    access_hash=raw_photo.access_hash,
                                    file_reference=raw_photo.file_reference
                                ),
                                spoiler=False
                            )
                            print(f"[POLL-RAW] Extracted InputMediaPhoto from temp message")
                    # Delete the temp message
                    try:
                        await client.delete_messages(dest_chat_id, temp_msg.id)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[POLL-RAW] Failed to upload temp photo for InputMedia: {e}")
        
        if not input_media:
            print(f"[POLL-RAW] Could not construct InputMedia — falling back to regular send_poll")
            return None
        
        # Step 2: Construct the raw InputMediaPoll with media parameter
        poll = poll_kwargs.get('_raw_poll')  # We'll pass the raw Poll object separately
        if not poll:
            print(f"[POLL-RAW] No raw Poll object provided — falling back to regular send_poll")
            return None
        
        # Build PollAnswer objects
        from pyrogram.types import PollOption
        options = poll_kwargs.get('options', [])
        
        # Build raw Poll object
        q_text = poll_kwargs.get('question', '')
        q_entities_raw = poll_kwargs.get('question_entities', None)
        q_entities = []
        if q_entities_raw:
            for ent in q_entities_raw:
                try:
                    q_entities.append(await ent.write(client))
                except Exception:
                    pass
        
        is_quiz = poll_kwargs.get('type') == PollType.QUIZ
        is_anonymous = poll_kwargs.get('is_anonymous', True)
        allows_multiple = poll_kwargs.get('allows_multiple_answers', False)
        is_closed = poll_kwargs.get('is_closed', False)
        
        raw_poll = pyro_raw.types.Poll(
            id=client.rnd_id(),
            question=pyro_raw.types.TextWithEntities(text=q_text, entities=q_entities or []),
            answers=[
                await PollOption(text=opt.text, entities=getattr(opt, 'entities', None)).write(client, i)
                for i, opt in enumerate(options)
            ],
            closed=is_closed,
            public_voters=not is_anonymous,
            multiple_choice=allows_multiple,
            quiz=is_quiz,
        )
        
        correct_option_id = poll_kwargs.get('correct_option_id', None)
        correct_answers = [bytes([correct_option_id])] if correct_option_id is not None else None
        
        explanation = poll_kwargs.get('explanation', None)
        explanation_entities = poll_kwargs.get('explanation_entities', None) or []
        
        # Construct InputMediaPoll with media (the NEW constructor format)
        # We need to manually construct the bytes because Pyrofork doesn't have this constructor
        # Constructor ID for inputMediaPoll with media: 0x85564b30
        # inputMediaPoll#85564b30 poll:Poll correct_answers:Vector<bytes> solution:string solution_entities:Vector<MessageEntity> media:InputMedia = InputMedia;
        
        # Actually, let's try using the existing InputMediaPoll and adding media via raw invoke
        # We'll construct the RPC call manually
        
        input_media_poll = pyro_raw.types.InputMediaPoll(
            poll=raw_poll,
            correct_answers=correct_answers,
            solution=explanation,
            solution_entities=explanation_entities or []
        )
        
        # Build reply_to
        reply_to_msg_id = poll_kwargs.get('reply_to_message_id', None)
        reply_to = None
        if reply_to_msg_id:
            reply_to = pyro_raw.types.InputReplyToMessage(reply_to_msg_id=reply_to_msg_id)
        
        # Build reply_markup
        reply_markup = poll_kwargs.get('reply_markup', None)
        raw_reply_markup = await reply_markup.write(client) if reply_markup else None
        
        # Construct and send the raw RPC call
        # We use messages.SendMedia with the InputMediaPoll
        # The key: we need to manually inject the 'media' field into InputMediaPoll
        # Since Pyrofork's InputMediaPoll doesn't have 'media' slot, we'll
        # construct the raw bytes manually and use client.invoke()
        
        # ── Manual TL construction for inputMediaPoll#85564b30 ──
        import struct
        from pyrogram.raw.core import Int, Long, Vector, Bytes, String, TLObject
        
        b = bytearray()
        # Constructor ID: inputMediaPoll#85564b30
        b.extend(struct.pack('<i', 0x85564b30))
        
        # flags
        flags = 0
        if correct_answers is not None:
            flags |= (1 << 0)
        if explanation is not None:
            flags |= (1 << 1)
        if explanation_entities:
            flags |= (1 << 1)
        # media flag (bit 2)
        flags |= (1 << 2)  # media is present
        b.extend(struct.pack('<i', flags))
        
        # poll
        b.extend(raw_poll.write())
        
        # correct_answers (flags.0)
        if correct_answers is not None:
            b.extend(Vector(correct_answers, Bytes))
        
        # solution (flags.1)
        if explanation is not None:
            b.extend(String(explanation))
        
        # solution_entities (flags.1)
        if explanation_entities:
            b.extend(Vector(explanation_entities))
        
        # media (flags.2) — the question image
        b.extend(input_media.write())
        
        # Now we have the raw bytes for the InputMediaPoll with media
        # We need to send this using messages.SendMedia
        # But client.invoke() expects a TLObject, not raw bytes
        
        # Alternative approach: Use Pyrofork's raw invoke with a custom object
        # Let's monkey-patch by creating a temporary wrapper
        
        # Actually, the cleanest approach is to directly use messages.SendMedia
        # with our custom InputMediaPoll bytes injected
        
        # Let's use a different approach: construct the full SendMedia request
        # and replace the media bytes with our custom ones
        
        send_media_rpc = pyro_raw.functions.messages.SendMedia(
            peer=await client.resolve_peer(dest_chat_id),
            media=input_media_poll,  # This will use the old InputMediaPoll without media
            message="",
            random_id=client.rnd_id(),
            reply_to=reply_to,
            reply_markup=raw_reply_markup,
        )
        
        # Now serialize the entire request, then patch the media constructor
        full_bytes = send_media_rpc.write()
        
        # Find the old InputMediaPoll constructor ID (0xf94e5f1) in the bytes and replace
        old_id_bytes = struct.pack('<i', 0xf94e5f1)
        new_id_bytes = struct.pack('<i', 0x85564b30)
        
        # The old InputMediaPoll bytes don't include 'media', so we can't just swap IDs
        # We need to properly reconstruct the media portion
        # This approach is too fragile with byte manipulation
        
        # ── CLEANEST APPROACH: Use Telethon-style raw invoke ──
        # We'll create a custom function that properly constructs the request
        # with the new InputMediaPoll format including media
        
        print(f"[POLL-RAW] Byte-level patching is too fragile — using upload+reply fallback instead")
        return None
        
    except Exception as e:
        print(f"[POLL-RAW] Failed to send poll with embedded image: {e}")
        import traceback; traceback.print_exc()
        return None


async def pass1_preupload_missing_question_images(
    uid, source_channel, dest_channel_id_int, msg_id_map,
    start_msg_id, n, fetch_map, ubot, uc, lt, topic_id=None
):
    """Pass 1 — Pre-upload question images that are OUTSIDE the batch range.

    Before the main batch loop processes any message, this function scans
    all polls in the batch and checks if their question images (referenced
    via reply_to_message_id) exist in the msg_id_map. If a question image
    is outside the batch range AND not already uploaded, it fetches and
    uploads it NOW so the poll can find it later.

    Three checks per question image:
      1. Inside batch range? → Pass 2 will handle it (skip)
      2. Already in msg_id_map? → Previous batch uploaded it (skip)
      3. Neither? → FETCH + UPLOAD NOW, save mapping to msg_id_map

    Cost: 1 fetch + 1 upload ONLY for missing images. Zero cost otherwise.
    """
    if not fetch_map:
        print("[PASS-1] No fetch_map available — skipping pre-upload scan")
        return

    batch_id_set = set(range(start_msg_id, start_msg_id + n))
    already_present = 0
    pre_uploaded = 0
    failed = 0

    print(f"[PASS-1] Scanning {len(fetch_map)} messages for polls with missing question images...")

    for mid_key, msg_info in fetch_map.items():
        # Only care about polls
        if msg_info.get("media_type") != "poll":
            continue

        # Get the question image reference
        question_src_id = msg_info.get("reply_to")
        if not question_src_id:
            # Poll has no question image reference — skip
            continue

        question_src_id = int(question_src_id)

        # Check 1: Is question image inside the batch range?
        if question_src_id in batch_id_set:
            # It will be uploaded in Pass 2 naturally
            already_present += 1
            continue

        # Check 2: Was it already uploaded (in msg_id_map from previous batch)?
        if question_src_id in msg_id_map:
            # Already uploaded — destination ID exists
            already_present += 1
            continue

        # Question image is MISSING — outside batch AND never uploaded
        print(f"[PASS-1] poll={mid_key} question={question_src_id} "
              f"is OUTSIDE batch range and not in msg_id_map — pre-uploading now")

        result = await _fetch_and_upload_missing_question_image(
            question_src_id=question_src_id,
            source_channel=source_channel,
            dest_channel_id_int=dest_channel_id_int,
            ubot=ubot, uc=uc, lt=lt, topic_id=topic_id
        )

        if result and isinstance(result, tuple):
            q_src_id, q_dst_id = result
            # Add to msg_id_map so Pass 2 / process_msg can find it
            msg_id_map[q_src_id] = q_dst_id
            pre_uploaded += 1
            # Also save to MongoDB immediately so it survives crashes
            try:
                await save_upload_map_incremental(uid, str(source_channel), dest_channel_id_int, {q_src_id: q_dst_id}, q_src_id)
            except Exception as e:
                print(f"[PASS-1] MongoDB save failed for pre-uploaded question: {e}")
        else:
            failed += 1
            print(f"[PASS-1] ❌ Could not pre-upload question={question_src_id} "
                  f"for poll={mid_key} — poll will upload without question image")

    print(f"[PASS-1] ✅ Complete: already_present={already_present} "
          f"pre_uploaded={pre_uploaded} failed={failed}")


async def _fetch_and_upload_missing_question_image(
    question_src_id, source_channel, dest_channel_id_int, ubot, uc, lt, topic_id=None
):
    """Fetch one question image from source and upload it to destination.

    Tries user_client first, then bot client, then forward fallback.
    Returns (src_id, dest_id) tuple on success, False on failure.
    The caller adds the mapping to msg_id_map and MongoDB.
    """

    # Build topic kwargs for forum topic support
    _topic_kw = {}
    if topic_id:
        _topic_kw['message_thread_id'] = topic_id

    # Resolve source chat for API calls
    resolved_src = source_channel
    try:
        if isinstance(source_channel, str) and source_channel.lstrip('-').isdigit():
            resolved_src = int(source_channel)
    except Exception:
        pass

    # Try fetching the question image message
    msg = None
    for client_name, fetch_c in [("user_client", uc), ("bot", ubot)]:
        if not fetch_c:
            continue
        try:
            resolved_chat = await resolve_chat(fetch_c, source_channel)
            msg = await fetch_c.get_messages(resolved_chat, question_src_id)
            if msg and not getattr(msg, 'empty', False):
                break
            msg = None
        except ChannelPrivate:
            print(f"[PASS-1] {client_name}: ChannelPrivate — can't access source")
        except Exception as e:
            print(f"[PASS-1] {client_name}: Fetch failed src={question_src_id}: {e}")
            msg = None

    if not msg:
        print(f"[PASS-1] Cannot fetch question image src={question_src_id} from any client")
        return False

    # Upload to destination
    sent = None
    try:
        if msg.photo:
            sent = await flood_wait_retry(
                ubot.send_photo(dest_channel_id_int, photo=msg.photo.file_id, **_topic_kw),
                f"pass1_question_photo_{question_src_id}"
            )
        elif msg.document:
            # Check if it's an image document
            mime = getattr(msg.document, 'mime_type', '') or ''
            if mime.startswith('image/') or any(
                attr.file_name and attr.file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif'))
                for attr in getattr(msg.document, 'attributes', [])
                if hasattr(attr, 'file_name')
            ):
                sent = await flood_wait_retry(
                    ubot.send_document(dest_channel_id_int, document=msg.document.file_id, **_topic_kw),
                    f"pass1_question_doc_{question_src_id}"
                )
            else:
                # Non-image document — still upload, might be a PDF question image
                sent = await flood_wait_retry(
                    ubot.send_document(dest_channel_id_int, document=msg.document.file_id, **_topic_kw),
                    f"pass1_question_doc_{question_src_id}"
                )
        elif msg.sticker:
            sent = await flood_wait_retry(
                ubot.send_sticker(dest_channel_id_int, sticker=msg.sticker.file_id, **_topic_kw),
                f"pass1_question_sticker_{question_src_id}"
            )
        elif msg.video:
            sent = await flood_wait_retry(
                ubot.send_video(dest_channel_id_int, video=msg.video.file_id, **_topic_kw),
                f"pass1_question_video_{question_src_id}"
            )
        elif msg.text:
            # Text-only question — send as text message
            sent = await flood_wait_retry(
                ubot.send_message(dest_channel_id_int, text=msg.text, **_topic_kw),
                f"pass1_question_text_{question_src_id}"
            )
        else:
            print(f"[PASS-1] Question image src={question_src_id} has unsupported type — trying forward")
            # Last resort: forward the message
            for fwd_client in [uc, ubot]:
                if fwd_client:
                    try:
                        resolved_chat = await resolve_chat(fwd_client, source_channel)
                        fwd = await fwd_client.forward_messages(dest_channel_id_int, question_src_id, resolved_chat)
                        if fwd:
                            fwd_id = fwd.id if hasattr(fwd, 'id') else (fwd[0].id if isinstance(fwd, list) and fwd else None)
                            if fwd_id:
                                sent = type('_FakeSent', (), {'id': fwd_id})()
                                break
                    except Exception as e:
                        print(f"[PASS-1] Forward fallback failed: {e}")

    except FloodWait as e:
        wait_secs = e.value if hasattr(e, 'value') else 30
        # ANY FloodWait → re-raise so the batch stops
        print(f"[PASS-1] FloodWait {wait_secs}s on question upload — re-raising (batch will stop)")
        raise
    except Exception as e:
        print(f"[PASS-1] Upload failed for question src={question_src_id}: {e}")
        return False

    if not sent:
        print(f"[PASS-1] Upload returned None for question src={question_src_id}")
        return False

    dest_id = sent.id

    # Save to MongoDB so Pass 2 / process_msg can find it
    # We need the uid to save — but we don't have it here directly.
    # Instead, we add it to the caller's msg_id_map (passed by reference in pass1)
    # The caller will handle MongoDB saves via the normal flush mechanism.
    # For now, just print and let the caller add it.
    print(f"[PASS-1] ✅ Pre-uploaded question src={question_src_id} → dst={dest_id}")

    # Return the dest_id so the caller can add it to msg_id_map
    return (question_src_id, dest_id)


async def pass1_from_dependency_index(
    uid, source_channel, dest_channel_id_int, msg_id_map,
    start_msg_id, n, ubot, uc, lt, topic_id=None
):
    """Pass 1 using the pre-built dependency index (Method 1).

    Instead of scanning the entire fetch_map for polls (which can be huge),
    this queries the MongoDB dependencies collection directly — MongoDB does
    the heavy lifting, not Python.

    For each dependency found:
      1. Question image inside batch range? → Pass 2 will handle it (skip)
      2. Already in msg_id_map? → Previous batch uploaded it (skip)
      3. Missing? → FETCH + UPLOAD NOW, save mapping to msg_id_map

    Falls back to the old fetch_map scan if no dependencies exist in MongoDB
    (e.g., fetch map was built before Method 1 was implemented).

    Cost: Only queries dependencies for this user+channel.
    RAM: One dependency doc at a time from MongoDB cursor.
    """
    channel_str = str(source_channel)

    batch_start = start_msg_id
    batch_end = start_msg_id + n - 1

    # Query ONLY dependencies where the POLL is inside the batch range.
    # We only need to pre-upload question images for polls we're about to process.
    # Scanning all deps for the entire channel would be very slow for large channels.
    dep_query = {
        "user_id": uid,
        "channel_id": channel_str,
        "poll_src_id": {"$gte": batch_start, "$lte": batch_end},
    }

    dep_count = await dependencies_collection.count_documents(dep_query)

    if dep_count == 0:
        print(f"[PASS-1-DEP] No dependencies in batch range {batch_start}-{batch_end} for uid={uid} ch={channel_str} — "
              f"falling back to fetch_map scan")
        return None  # Signal caller to fall back to fetch_map scan

    print(f"[PASS-1-DEP] Found {dep_count} dependencies in batch range {batch_start}-{batch_end} for uid={uid} ch={channel_str}")

    batch_id_set = set(range(start_msg_id, start_msg_id + n))
    already_present = 0
    pre_uploaded = 0
    failed = 0

    # Query ONLY dependencies for polls inside this batch range
    cursor = dependencies_collection.find(dep_query)

    async for dep in cursor:
        question_src_id = dep["question_src_id"]
        poll_src_id = dep.get("poll_src_id", "?")

        # Ensure question_src_id is int
        try:
            question_src_id = int(question_src_id)
        except (ValueError, TypeError):
            continue

        # Check 1: Is question image inside the batch range?
        if question_src_id in batch_id_set:
            # It will be uploaded in Pass 2 naturally
            already_present += 1
            continue

        # Check 2: Was it already uploaded (in msg_id_map from previous batch)?
        if question_src_id in msg_id_map:
            already_present += 1
            continue

        # Question image is MISSING — outside batch AND never uploaded
        print(f"[PASS-1-DEP] poll={poll_src_id} question={question_src_id} "
              f"OUTSIDE batch range and not in msg_id_map — pre-uploading now")

        result = await _fetch_and_upload_missing_question_image(
            question_src_id=question_src_id,
            source_channel=source_channel,
            dest_channel_id_int=dest_channel_id_int,
            ubot=ubot, uc=uc, lt=lt, topic_id=topic_id
        )

        if result and isinstance(result, tuple):
            q_src_id, q_dst_id = result
            # Add to msg_id_map so Pass 2 / process_msg can find it
            msg_id_map[q_src_id] = q_dst_id
            pre_uploaded += 1
            # Also save to MongoDB immediately so it survives crashes
            try:
                await save_upload_map_incremental(uid, str(source_channel), dest_channel_id_int, {q_src_id: q_dst_id}, q_src_id)
            except Exception as e:
                print(f"[PASS-1-DEP] MongoDB save failed for pre-uploaded question: {e}")
        else:
            failed += 1
            print(f"[PASS-1-DEP] ❌ Could not pre-upload question={question_src_id} "
                  f"for poll={poll_src_id}")

    print(f"[PASS-1-DEP] ✅ Complete: already_present={already_present} "
          f"pre_uploaded={pre_uploaded} failed={failed}")

    return True  # Signal that dependency index was used (don't fall back)


async def send_direct(c, m, tcid, ft=None, rtmid=None, u=None, source_chat=None, uid=None, topic_id=None, reply_dest_for_button=None, link_rewrite_map=None, caption_entities=None):
    try:
        sent = None
        is_closed_poll = False
        # IMPORTANT: Check m.poll BEFORE m.media — in Pyrofork, poll messages have m.media=None
        # so the poll would be skipped if checked under if m.media:
        if m.poll:
            poll = m.poll
            is_quiz = poll.correct_option_id is not None or getattr(poll, 'type', None) == PollType.QUIZ
            options = [PollOption(text=opt.text, entities=opt.entities) for opt in poll.options]
            is_closed_poll = getattr(poll, 'is_closed', False)
            
            print(f"[DEBUG-POLL-DIRECT] ═══════ send_direct POLL START ═══════")
            print(f"[DEBUG-POLL-DIRECT] src_msg_id={m.id} source_chat={source_chat}")
            print(f"[DEBUG-POLL-DIRECT] question={poll.question[:80]}")
            print(f"[DEBUG-POLL-DIRECT] is_quiz={is_quiz} poll.type={getattr(poll, 'type', 'N/A')} correct_option_id={poll.correct_option_id}")
            print(f"[DEBUG-POLL-DIRECT] options_count={len(options)} is_closed={is_closed_poll}")
            print(f"[DEBUG-POLL-DIRECT] rtmid={rtmid} tcid={tcid}")
            print(f"[DEBUG-POLL-DIRECT] m.reply_to_message_id={m.reply_to_message_id}")
            print(f"[DEBUG-POLL-DIRECT] m.reply_to={getattr(m, 'reply_to', 'MISSING')}")
            if hasattr(m, 'reply_to') and m.reply_to:
                print(f"[DEBUG-POLL-DIRECT] m.reply_to.message_id={getattr(m.reply_to, 'message_id', 'N/A')}")
                print(f"[DEBUG-POLL-DIRECT] m.reply_to.reply_to_msg_id={getattr(m.reply_to, 'reply_to_msg_id', 'N/A')}")
            print(f"[DEBUG-POLL-DIRECT] m.invert_media={getattr(m, 'invert_media', 'N/A')} (True=poll has embedded question image)")
            print(f"[DEBUG-POLL-DIRECT] m.media_group_id={getattr(m, 'media_group_id', 'N/A')} (grouped=part of album with image)")
            print(f"[DEBUG-POLL-DIRECT] m.photo={'YES' if getattr(m, 'photo', None) else 'NO'} m.document={'YES' if getattr(m, 'document', None) else 'NO'}")
            print(f"[DEBUG-POLL-DIRECT] u (user_client)={'YES' if u else 'NO'}")
            
            # ── Record poll→question image dependency (Method 1 batch-side) ──
            if uid:
                _poll_reply_to = m.reply_to_message_id or getattr(getattr(m, 'reply_to', None), 'message_id', None) or getattr(getattr(m, 'reply_to', None), 'reply_to_msg_id', None)
                if _poll_reply_to:
                    _record_poll_dependency(uid, source_chat, m.id, _poll_reply_to)
            
            # ── SEND: 1) Upload question image  2) Inline Reveal Answer  3) Native Quiz Poll ──
            
            correct_id = poll.correct_option_id
            if is_quiz and correct_id is None:
                correct_id = await _get_correct_option(source_chat, m.id, poll, user_client=u)
            if correct_id is None:
                correct_id = -1
            print(f"[DEBUG-POLL-DIRECT] correct_id={correct_id} (after _get_correct_option)")
            
            # ── 1) Upload question image — ROBUST multi-method detection ──
            # In Telegram, poll question images can be:
            #   A) Embedded in the poll message (newer Telegram feature)
            #   B) A separate message that the poll replies to (traditional)
            #   C) Part of a grouped/album message set
            # Pyrofork doesn't support (A) natively, so we detect it ourselves.
            question_sent_id = rtmid  # default: already-uploaded question image
            
            # Extract src_reply_id early — used by Method A, B, and C for reply chain recording
            src_reply_id = m.reply_to_message_id or getattr(getattr(m, 'reply_to', None), 'message_id', None) or getattr(getattr(m, 'reply_to', None), 'reply_to_msg_id', None)
            
            # ── Method A: Try extracting embedded question media from the poll message ──
            embedded_media_id = None
            embedded_media_type = None
            if not rtmid:
                embedded_media_id, embedded_media_type = await _extract_poll_question_media(m, source_chat, user_client=u, send_client=c)
                if embedded_media_id:
                    print(f"[DEBUG-POLL-DIRECT] Found embedded question media: type={embedded_media_type}")
                    if embedded_media_type == 'invert_media_forward':
                        # invert_media detected — skip to Method C (forward)
                        print(f"[DEBUG-POLL-DIRECT] invert_media flag detected — will try forwarding (Method C)")
                    else:
                        # Upload the extracted media to destination
                        q_id = await _upload_extracted_question_media(c, tcid, embedded_media_id, embedded_media_type, topic_id=topic_id)
                        if q_id:
                            question_sent_id = q_id
                            print(f"[DEBUG-POLL-DIRECT] Embedded question media uploaded, dest_id={q_id}")
                            # ── REPLY CHAIN: Record embedded question image mapping so
                            # messages that reply to the question image can find it
                            if link_rewrite_map is not None and src_reply_id:
                                link_rewrite_map[src_reply_id] = q_id
                                print(f"[CHAIN-POLL] Recorded embedded question image: src={src_reply_id} → dst={q_id}")
                        else:
                            print(f"[DEBUG-POLL-DIRECT] Embedded question media upload FAILED")
            
            # ── Method B: Check if poll replies to a media message (traditional approach) ──
            if not rtmid and not question_sent_id:
                print(f"[DEBUG-POLL-DIRECT] src_reply_id={src_reply_id} (from reply_to detection)")
                if src_reply_id:
                    # Try uploading question image using the robust helper
                    # Falls back through: user_client → send_client → forward
                    _wm_qi = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT) if uid else _DEFAULT_WATERMARK_TEXT
                    q_id = await _upload_question_image(c, source_chat, src_reply_id, tcid, user_client=u, topic_id=topic_id, watermark_text=_wm_qi)
                    if q_id:
                        question_sent_id = q_id
                        # ── REPLY CHAIN: Record question image mapping so messages that
                        # reply to the question image (e.g. achiever analysis) can find it
                        if link_rewrite_map is not None:
                            link_rewrite_map[src_reply_id] = q_id
                            print(f"[CHAIN-POLL] Recorded question image: src={src_reply_id} → dst={q_id}")
                    else:
                        print(f"[DEBUG-POLL-DIRECT] Question image upload failed for src_reply_id={src_reply_id}")
                else:
                    print(f"[DEBUG-POLL-DIRECT] No src_reply_id found — poll has no reply-to reference")
            
            # ── Method C: Last resort — try forwarding the entire poll message ──
            # (This preserves the embedded image if nothing else worked)
            # Also triggered when invert_media=True was detected (embedded image that
            # Pyrofork can't extract, but forwarding preserves it)
            should_try_forward = (not rtmid and not question_sent_id) or embedded_media_type == 'invert_media_forward'
            if should_try_forward:
                # Try userbot first (more likely to have source channel access),
                # then fall back to bot client
                forward_clients = []
                if u:
                    forward_clients.append(("user_client", u))
                if c and c != u:
                    forward_clients.append(("bot_client", c))
                
                for client_name, fwd_c in forward_clients:
                    try:
                        resolved_src = await resolve_chat(fwd_c, source_chat)
                        fwd_msgs = await fwd_c.forward_messages(tcid, m.id, resolved_src)
                        if fwd_msgs:
                            fwd_id = fwd_msgs.id if hasattr(fwd_msgs, 'id') else (fwd_msgs[0].id if isinstance(fwd_msgs, list) and fwd_msgs else None)
                            if fwd_id:
                                print(f"[DEBUG-POLL-DIRECT] Forwarded poll with image (method C, {client_name}), dest_id={fwd_id}")
                                # ── REPLY CHAIN: Record forwarded poll as the question image
                                # mapping so messages that reply to the question image can find it
                                if link_rewrite_map is not None and src_reply_id:
                                    link_rewrite_map[src_reply_id] = fwd_id
                                    print(f"[CHAIN-POLL] Recorded forwarded poll as question image: src={src_reply_id} → dst={fwd_id}")
                                sent = fwd_msgs if not isinstance(fwd_msgs, list) else fwd_msgs[0]
                                _fwd_sent_id = sent.id if sent else fwd_id
                                # Add buttons to forwarded poll before returning
                                if is_quiz and correct_id is not None and correct_id != -1 and tcid:
                                    try:
                                        _answer_letter = chr(65 + correct_id)
                                        await handle_answer_buttons(c, u, source_chat, m.id, _answer_letter, tcid, _fwd_sent_id,
                                                                    explanation_image_url=None,
                                                                    explanation_text=None,
                                                                    expl_msg_id=None,
                                                                    has_photo=False)
                                    except Exception as _ans_e:
                                        print(f"[ANSWER-TOPIC] handle_answer_buttons (forward) failed (non-fatal): {_ans_e}")
                                # 📖 View Explanation: NOT copied here — explanation will be sent in
                                # natural order when the batch loop reaches it, and 📖/🔙 buttons
                                # will be added at that point via the reply_dest_for_button path.
                                return (True, _fwd_sent_id, is_closed_poll)
                    except ChannelPrivate as e:
                        print(f"[DEBUG-POLL-DIRECT] {client_name}: ChannelPrivate — can't access source: {e}")
                    except (ChatIdInvalid, PeerIdInvalid) as e:
                        print(f"[DEBUG-POLL-DIRECT] {client_name}: Can't resolve source chat: {e}")
                    except Exception as e:
                        print(f"[DEBUG-POLL-DIRECT] {client_name}: Forward fallback failed: {e}")
            
            if rtmid:
                print(f"[DEBUG-POLL-DIRECT] Skipping question image upload — rtmid={rtmid} already set")
            
            print(f"[DEBUG-POLL-DIRECT] question_sent_id={question_sent_id} (final reply_to target)")
            
            # ── 2) Create Telegraph answer page for offline reveal ──
            reveal_url = None
            explanation_text = None
            explanation_image_url = None
            entry = None
            if is_quiz and correct_id is not None and correct_id != -1:
                answer_letter = chr(65 + correct_id)
                
                # ── Look up explanation: Memory → Live scan → Built-in solution ──
                try:
                    from plugins.explanation_listener import get_explanation_lookup, get_explanation, find_explanation_batch, check_poll_builtin_explanation
                    
                    # STEP 0: Check CHANNEL_EXPLANATIONS (instant, 0 API calls)
                    # This is the FASTEST path — persistent dict loaded from JSON + watchers.
                    # Contains text, has_photo, photo_file_id, explanation_msg_id, kind.
                    channel_expl = get_explanation_lookup(source_chat)
                    entry = channel_expl.get(m.id)
                    
                    if entry:
                        explanation_text = entry.get("text")
                        print(f"[TELEGRAPH] src_msg_id={m.id} — CACHE HIT: kind={entry.get('kind')} text={'YES' if explanation_text else 'NO'} photo={'YES' if entry.get('has_photo') else 'NO'}")
                        _edlog(f"[DIRECT] src_msg_id={m.id} CACHE HIT: kind={entry.get('kind')} text={'YES' if explanation_text else 'NO'} photo={'YES' if entry.get('has_photo') else 'NO'} expl_msg_id={entry.get('explanation_msg_id')}")
                        # Upload photo/video to Telegraph if present
                        if entry.get("has_photo") and u:
                            try:
                                expl_msg_id = entry["explanation_msg_id"]
                                resolved_src = await resolve_chat(u, source_chat)
                                expl_msg = await u.get_messages(resolved_src, expl_msg_id)
                                _wm = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT) if uid else _DEFAULT_WATERMARK_TEXT
                                if expl_msg and expl_msg.photo:
                                    explanation_image_url = await _upload_telegraph_photo(u, expl_msg, watermark_text=_wm)
                                    print(f"[TELEGRAPH] src_msg_id={m.id} — Cache photo uploaded: {explanation_image_url}")
                                elif expl_msg and expl_msg.video:
                                    explanation_image_url = await _upload_telegraph_photo(u, expl_msg, watermark_text=_wm)
                                    print(f"[TELEGRAPH] src_msg_id={m.id} — Cache video frame uploaded: {explanation_image_url}")
                            except Exception as e:
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Cache photo/video upload error: {e}")
                    
                    # STEP 1: Sequential scan fallback — only in send_direct (non-batch)
                    # During batch (process_msg), this is skipped via _skip_explanation_scan.
                    # In send_direct (single-message), always scan — user can wait.
                    if (not explanation_text or not explanation_image_url) and u:
                        print(f"[TELEGRAPH] src_msg_id={m.id} — Trying sequential scan (text={'YES' if explanation_text else 'NO'} photo={'YES' if explanation_image_url else 'NO'})...")
                        _edlog(f"[DIRECT] src_msg_id={m.id} — Trying sequential scan")
                        try:
                            from plugins.explanation_listener import find_explanation_sequential
                            seq_result = await find_explanation_sequential(u, source_chat, m.id, scan_window=100)
                            if seq_result:
                                if not explanation_text and seq_result.get("text"):
                                    explanation_text = seq_result.get("text")
                                if not explanation_image_url and seq_result.get("photo_file_id"):
                                    # Fetch the explanation message to upload photo to Telegraph
                                    try:
                                        expl_msg_id = seq_result["explanation_msg_id"]
                                        resolved_src = await resolve_chat(u, source_chat)
                                        expl_msg = await u.get_messages(resolved_src, expl_msg_id)
                                        if expl_msg and (expl_msg.photo or expl_msg.video):
                                            explanation_image_url = await _upload_telegraph_photo(u, expl_msg)
                                            print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan photo uploaded: {explanation_image_url}")
                                    except Exception as e:
                                        print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan photo upload error: {e}")
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan result: text={'YES' if explanation_text else 'NO'} photo={'YES' if explanation_image_url else 'NO'}")
                                _edlog(f"[DIRECT] src_msg_id={m.id} SEQ: text={'YES' if explanation_text else 'NO'} photo={'YES' if explanation_image_url else 'NO'}")
                            else:
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan MISS")
                        except Exception as e:
                            print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan error: {e}")
                    
                    # STEP 2: Built-in poll solution fallback (last resort)
                    if not explanation_text and u:
                        print(f"[TELEGRAPH] src_msg_id={m.id} — Trying built-in poll solution...")
                        try:
                            builtin_solution = await check_poll_builtin_explanation(u, source_chat, m.id)
                            if builtin_solution:
                                explanation_text = builtin_solution
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Found built-in solution: {builtin_solution[:80]}...")
                        except Exception as e:
                            print(f"[TELEGRAPH] src_msg_id={m.id} — Built-in solution check error: {e}")
                            
                except Exception as e:
                    print(f"[TELEGRAPH] src_msg_id={m.id} — Error looking up explanation: {e}")
                
                print(f"[TELEGRAPH] src_msg_id={m.id} answer_letter={answer_letter}")
                reveal_url = await _create_answer_page(answer_letter, image_url=explanation_image_url, answer_text=explanation_text)
                print(f"[TELEGRAPH] src_msg_id={m.id} — Answer page URL: {reveal_url}")
            else:
                print(f"[DEBUG-POLL-DIRECT] Skipping Telegraph page — is_quiz={is_quiz} correct_id={correct_id}")
            
            # ── 2) Send native QUIZ poll with 💡 View Answer button ──
            _edlog(f"[DIRECT] src_msg_id={m.id} is_quiz={is_quiz} correct_id={correct_id} entry={'YES' if entry else 'NO'} source_chat={source_chat}")
            
            # Build keyboard with 💡 View Answer button (📖 added later after poll is sent)
            keyboard = build_inline_quiz(poll.question, options, correct_id, reveal_url=reveal_url)
            try:
                poll_kwargs = dict(
                    chat_id=tcid,
                    question=poll.question,
                    options=options,
                    type=PollType.QUIZ if is_quiz else PollType.REGULAR,
                    is_anonymous=getattr(poll, 'is_anonymous', True),
                    is_closed=False,
                    reply_to_message_id=question_sent_id
                )
                if is_quiz and correct_id is not None and correct_id != -1:
                    poll_kwargs['correct_option_id'] = correct_id
                    if explanation_text:
                        poll_kwargs['explanation'] = explanation_text[:200] if len(explanation_text) > 200 else explanation_text
                    elif explanation_image_url:
                        # Photo-only explanation — add hint to click View Answer button
                        poll_kwargs['explanation'] = "Tap 💡 View Answer for explanation"
                if topic_id:
                    poll_kwargs['message_thread_id'] = topic_id
                if getattr(poll, 'question_entities', None):
                    poll_kwargs['question_entities'] = poll.question_entities
                if getattr(poll, 'allows_multiple_answers', None):
                    poll_kwargs['allows_multiple_answers'] = poll.allows_multiple_answers
                # Attach buttons directly to the poll
                if keyboard:
                    poll_kwargs['reply_markup'] = keyboard
                print(f"[DEBUG-POLL-DIRECT] Sending native poll: type={'QUIZ' if is_quiz else 'REGULAR'} correct_option_id={poll_kwargs.get('correct_option_id')} has_explanation={'explanation' in poll_kwargs} has_buttons={'reply_markup' in poll_kwargs} reply_to={question_sent_id}")
                sent = await flood_wait_retry(c.send_poll(**poll_kwargs), "send_poll_direct", dest_chat_id=tcid)
                print(f"[DEBUG-POLL-DIRECT] Native poll sent: id={sent.id if sent else 'None'}")
            except Exception as e:
                print(f"[POLL] Native poll send failed (direct, full kwargs): {e}")
                import traceback; traceback.print_exc()
                # ── RETRY: Send poll with minimal kwargs ──
                try:
                    minimal_kwargs = dict(
                        chat_id=tcid,
                        question=poll.question,
                        options=options,
                        type=PollType.QUIZ if is_quiz else PollType.REGULAR,
                        is_anonymous=getattr(poll, 'is_anonymous', True),
                        is_closed=False,
                    )
                    if is_quiz and correct_id is not None and correct_id != -1:
                        minimal_kwargs['correct_option_id'] = correct_id
                    if question_sent_id:
                        minimal_kwargs['reply_to_message_id'] = question_sent_id
                    print(f"[POLL-RETRY-DIRECT] Retrying with minimal kwargs...")
                    sent = await flood_wait_retry(c.send_poll(**minimal_kwargs), "send_poll_direct_retry", dest_chat_id=tcid)
                    print(f"[POLL-RETRY-DIRECT] Minimal poll sent: id={sent.id if sent else 'None'}")
                except Exception as retry_e:
                    print(f"[POLL-RETRY-DIRECT] Minimal poll with reply_to also failed: {retry_e}")
                    # ── FINAL RETRY: Send poll WITHOUT reply_to_message_id ──
                    try:
                        final_kwargs = dict(
                            chat_id=tcid,
                            question=poll.question,
                            options=options,
                            type=PollType.QUIZ if is_quiz else PollType.REGULAR,
                            is_anonymous=getattr(poll, 'is_anonymous', True),
                            is_closed=False,
                        )
                        if is_quiz and correct_id is not None and correct_id != -1:
                            final_kwargs['correct_option_id'] = correct_id
                        print(f"[POLL-RETRY-DIRECT] Final attempt WITHOUT reply_to_message_id...")
                        sent = await flood_wait_retry(c.send_poll(**final_kwargs), "send_poll_direct_final", dest_chat_id=tcid)
                        print(f"[POLL-RETRY-DIRECT] Poll sent (no reply_to): id={sent.id if sent else 'None'}")
                    except Exception as final_e:
                        print(f"[POLL-RETRY-DIRECT] All poll send attempts failed: {final_e}")
                        sent = None
        elif m.video:
            _video_kwargs = dict(chat_id=tcid, video=m.video.file_id, caption=ft, duration=m.video.duration,
                width=m.video.width, height=m.video.height)
            if topic_id:
                _video_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _video_kwargs['reply_to_message_id'] = rtmid
            if caption_entities and ft:
                _video_kwargs['caption_entities'] = caption_entities
                sent = await flood_wait_retry(c.send_video(**_video_kwargs), "send_video_direct", dest_chat_id=tcid)
            else:
                sent = await flood_wait_retry(_safe_markdown_send(c.send_video, "send_video_direct",
                    **_video_kwargs), "send_video_direct", dest_chat_id=tcid)
        elif m.video_note:
            _vn_kwargs = dict(chat_id=tcid, video_note=m.video_note.file_id)
            if topic_id:
                _vn_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _vn_kwargs['reply_to_message_id'] = rtmid
            sent = await flood_wait_retry(c.send_video_note(**_vn_kwargs), "send_video_note", dest_chat_id=tcid)
        elif m.voice:
            _v_kwargs = dict(chat_id=tcid, voice=m.voice.file_id)
            if topic_id:
                _v_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _v_kwargs['reply_to_message_id'] = rtmid
            sent = await flood_wait_retry(c.send_voice(**_v_kwargs), "send_voice", dest_chat_id=tcid)
        elif m.sticker:
            _s_kwargs = dict(chat_id=tcid, sticker=m.sticker.file_id)
            if topic_id:
                _s_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _s_kwargs['reply_to_message_id'] = rtmid
            sent = await flood_wait_retry(c.send_sticker(**_s_kwargs), "send_sticker", dest_chat_id=tcid)
        elif m.audio:
            _a_kwargs = dict(chat_id=tcid, audio=m.audio.file_id, caption=ft, duration=m.audio.duration,
                performer=m.audio.performer, title=m.audio.title)
            if topic_id:
                _a_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _a_kwargs['reply_to_message_id'] = rtmid
            if caption_entities and ft:
                _a_kwargs['caption_entities'] = caption_entities
                sent = await flood_wait_retry(c.send_audio(**_a_kwargs), "send_audio_direct", dest_chat_id=tcid)
            else:
                sent = await flood_wait_retry(_safe_markdown_send(c.send_audio, "send_audio_direct",
                    **_a_kwargs), "send_audio_direct", dest_chat_id=tcid)
        elif m.photo:
            photo_id = m.photo.file_id if hasattr(m.photo, 'file_id') else m.photo[-1].file_id
            _p_kwargs = dict(chat_id=tcid, photo=photo_id, caption=ft)
            if topic_id:
                _p_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _p_kwargs['reply_to_message_id'] = rtmid
            if caption_entities and ft:
                _p_kwargs['caption_entities'] = caption_entities
                sent = await flood_wait_retry(c.send_photo(**_p_kwargs), "send_photo_direct", dest_chat_id=tcid)
            else:
                sent = await flood_wait_retry(_safe_markdown_send(c.send_photo, "send_photo_direct",
                    **_p_kwargs), "send_photo_direct", dest_chat_id=tcid)
        elif m.animation:
            _ani_kwargs = dict(chat_id=tcid, animation=m.animation.file_id, caption=ft)
            if topic_id:
                _ani_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _ani_kwargs['reply_to_message_id'] = rtmid
            if caption_entities and ft:
                _ani_kwargs['caption_entities'] = caption_entities
                sent = await flood_wait_retry(c.send_animation(**_ani_kwargs), "send_animation_direct", dest_chat_id=tcid)
            else:
                sent = await flood_wait_retry(_safe_markdown_send(c.send_animation, "send_animation_direct",
                    **_ani_kwargs), "send_animation_direct", dest_chat_id=tcid)
        elif m.document:
            _d_kwargs = dict(chat_id=tcid, document=m.document.file_id, caption=ft, file_name=m.document.file_name)
            if topic_id:
                _d_kwargs['message_thread_id'] = topic_id
            if rtmid:
                _d_kwargs['reply_to_message_id'] = rtmid
            if caption_entities and ft:
                _d_kwargs['caption_entities'] = caption_entities
                sent = await flood_wait_retry(c.send_document(**_d_kwargs), "send_document_direct", dest_chat_id=tcid)
            else:
                sent = await flood_wait_retry(_safe_markdown_send(c.send_document, "send_document_direct",
                    **_d_kwargs), "send_document_direct", dest_chat_id=tcid)
        else:
            # Unknown media type — can't send directly, return False
            # (process_msg will fall through to download+reupload path)
            print(f"[SEND-DIRECT] Unknown media type for msg {m.id} — send_direct cannot handle")
            return (False, None, False)
        # For poll messages: if sent is None, the poll failed to send
        if m.poll and sent is None:
            return (False, None, False)
        # 📺 ANSWER TOPIC: Always add topic button after quiz poll (send_direct path)
        if m.poll and is_quiz and sent and correct_id is not None and correct_id != -1 and tcid:
            try:
                _answer_letter = chr(65 + correct_id)
                await handle_answer_buttons(c, u, source_chat, m.id, _answer_letter, tcid, sent.id,
                                            explanation_image_url=explanation_image_url,
                                            explanation_text=explanation_text,
                                            expl_msg_id=entry.get("explanation_msg_id") if entry else None,
                                            has_photo=entry.get("has_photo", False) if entry else False)
            except Exception as _ans_e:
                print(f"[ANSWER-TOPIC] handle_answer_buttons (direct) failed (non-fatal): {_ans_e}")
        
        # ── 📖 View Explanation: NOT copied here — explanation will be sent in natural ──
        # order when the batch loop reaches it. When the explanation message is processed,
        # it will be sent as a reply to the poll (reply chain preserved) and both buttons
        # will be added at that point via the reply_dest_for_button path:
        #   📖 View Explanation on the POLL → links to the explanation
        #   🔙 Back to Question on the explanation → links back to the poll
        # Previously, copying the explanation immediately after the poll caused:
        #   1. Explanations uploaded out of order (right after poll, not 100 msgs later)
        #   2. Duplicate explanations when batch loop reached the same message again
        
        # ── 📖 View Explanation + 🔙 Back to Question: for non-poll messages that reply to a poll ──
        # When an explanation message is sent directly (not via _copy_explanation_to_dest),
        # add both buttons:
        #   📖 View Explanation on the POLL (B') → links to this explanation (C')
        #   🔙 Back to Question on this explanation (C') → links back to poll (B')
        # This handles the case where the explanation arrives during batch processing
        # and is sent via send_direct rather than being copied later.
        # NOTE: The message is also sent with reply_to_message_id=rtmid so it appears
        # as a REPLY to the poll in the destination channel.
        if not m.poll and rtmid:
            print(f"[REPLY-TO] ✅ src_msg_id={m.id} send_direct with reply_to_message_id={rtmid}")
        if not m.poll and reply_dest_for_button and sent and sent.id:
            try:
                _reply_url = _build_telegram_link(tcid, sent.id)
                if _reply_url:
                    # Add 📖 View Explanation on the poll → links to this explanation
                    await _add_explanation_button(c, tcid, reply_dest_for_button, _reply_url)
                    _edlog(f"[DIRECT] src_msg_id={m.id} — Adding 📖 on poll {reply_dest_for_button} → {_reply_url}")
                # Add 🔙 Back to Question on this explanation → links back to poll
                _poll_url = _build_telegram_link(tcid, reply_dest_for_button)
                if _poll_url:
                    back_added = await _add_inline_button(
                        c, tcid, sent.id,
                        "🔙 Back to Question", _poll_url,
                        log_prefix="BACK-BTN-DIRECT"
                    )
                    if back_added:
                        _edlog(f"[DIRECT] src_msg_id={m.id} ✅ 🔙 button added on explanation {sent.id} → poll {reply_dest_for_button}")
                    else:
                        _edlog(f"[DIRECT] src_msg_id={m.id} ⚠️ Failed to add 🔙 button on explanation {sent.id}")
            except Exception as _back_e:
                _edlog(f"[DIRECT] src_msg_id={m.id} 📖/🔙 button error: {_back_e}")
        
        return (True, sent.id if sent else None, is_closed_poll)
    except FloodWait as e:
        # FloodWait during direct send — stop the batch
        wait_secs = e.value if hasattr(e, 'value') else 30
        print(f"[FLOOD] send_direct: FloodWait {_format_duration(wait_secs)} — stopping batch")
        raise
    except asyncio.CancelledError:
        raise  # Must re-raise so /stop and /autooff work
    except Exception as e:
        print(f'Direct send error: {e}')
        return (False, None, False)

async def process_msg(c, u, m, d, lt, uid, i, reply_to_destination_id=None,
                      link_rewrite_map=None, dest_channel_id=None, dest_channel_username=None,
                      source_channel_username=None, source_channel_id=None,
                      multi_source_channels=None, rewriter=None,
                      _cached_tcid=None, _cached_topic_id=None, _cached_rtmid=None,
                      _cached_watermark=None, _cached_caption=None,
                      _cached_source_name=None, _skip_verify=False,
                      _skip_explanation_scan=False):
    sent_msg_id = None
    had_unresolved_links = False
    _rewriter = rewriter  # SimpleRewriter instance (takes priority over link_rewrite_map)
    
    # ═══════════════════════════════════════════════════════════════
    # SMART CACHE: Capture ORIGINAL text/entities BEFORE rewriting
    # This is needed for cache_message_for_relink() which runs AFTER
    # the message is sent. We capture now because link rewriting
    # modifies the text/entities in place.
    # ═══════════════════════════════════════════════════════════════
    _original_text = str(m.text) if m.text else (str(m.caption) if m.caption else '')
    _original_entities = m.entities if m.entities else (m.caption_entities if m.caption_entities else None)
    
    try:
        # ── Early cancel check: abort immediately if /stop was used ──
        # This catches the case where task.cancel() hasn't propagated yet
        # (e.g., we're inside process_msg which is awaited from the batch loop).
        if uid and should_cancel(uid):
            print(f"[STOP] Cancel flag detected inside process_msg for uid={uid} — aborting")
            raise asyncio.CancelledError()
        
        # ═══════════════════════════════════════════════════════════════
        # PERF: Use cached values from batch loop instead of per-message
        # MongoDB queries and resolve_peer() calls. This eliminates
        # ~3-5 API/DB round-trips per message (each 50-200ms).
        # ═══════════════════════════════════════════════════════════════
        if _cached_tcid is not None:
            # Use pre-cached values from batch loop (0 DB/API calls)
            tcid = _cached_tcid
            topic_id = _cached_topic_id
            rtmid = _cached_rtmid
        else:
            # Fallback: per-message lookup (slow, only for non-batch callers)
            cfg_chat = await get_user_data_key(d, 'chat_id', None)
            tcid = d
            rtmid = None
            topic_id = None
            if cfg_chat:
                if '/' in cfg_chat:
                    parts = cfg_chat.split('/', 1)
                    tcid = int(parts[0])
                    topic_id = int(parts[1]) if len(parts) > 1 else None
                    # Do NOT set rtmid = topic_id — reply_to is set per-message
                    # via reply_to_destination_id parameter, NOT from topic_id
                    rtmid = None
                else:
                    tcid = int(cfg_chat)
            
            # Resolve destination peer (only when not cached)
            try:
                await c.resolve_peer(tcid)
            except Exception as _rp_err:
                print(f"[DEST-RESOLVE] Failed to resolve dest peer {tcid}: {_rp_err}")
        
        # Use reply_to_destination_id as the reply_to target (rtmid).
        # Save the original reply_to_destination_id for 📖 View Explanation button on parent.
        # IMPORTANT: Do NOT overwrite topic_id with reply_to_destination_id!
        #   - topic_id = forum topic ID (from user config, e.g. "3" for channel/3)
        #   - reply_to_destination_id = dest message ID (e.g. 65653, the uploaded question image)
        # These are completely different things. Overwriting topic_id with a message ID
        # causes polls to be sent to the wrong thread or invisible in forum channels.
        _reply_to_dest_for_button = reply_to_destination_id
        if reply_to_destination_id is not None:
            rtmid = reply_to_destination_id
        
        # Debug: Log reply_to setup for non-poll messages that reply to something
        if not m.poll and reply_to_destination_id:
            _src_reply = _get_reply_to_id(m)
            if _src_reply:
                print(f"[REPLY-TO] src_msg_id={m.id} replies_to_src={_src_reply} → dest_reply_to={reply_to_destination_id}")
        
        # IMPORTANT: Check m.poll BEFORE m.media — in Pyrofork, poll messages have m.media=None
        # so the poll would be skipped if checked under if m.media:
        if m.poll:
            poll = m.poll
            is_quiz = poll.correct_option_id is not None or getattr(poll, 'type', None) == PollType.QUIZ
            options = [PollOption(text=opt.text, entities=opt.entities) for opt in poll.options]
            is_closed = getattr(poll, 'is_closed', False)
            
            print(f"[DEBUG-POLL-PROC] ═══════ process_msg POLL START ═══════")
            print(f"[DEBUG-POLL-PROC] src_msg_id={m.id} source_channel={i}")
            print(f"[DEBUG-POLL-PROC] question={poll.question[:80]}")
            print(f"[DEBUG-POLL-PROC] is_quiz={is_quiz} poll.type={getattr(poll, 'type', 'N/A')} correct_option_id={poll.correct_option_id}")
            print(f"[DEBUG-POLL-PROC] options_count={len(options)} is_closed={is_closed}")
            print(f"[DEBUG-POLL-PROC] rtmid={rtmid} tcid={tcid} reply_to_destination_id={reply_to_destination_id}")
            print(f"[DEBUG-POLL-PROC] m.reply_to_message_id={m.reply_to_message_id}")
            print(f"[DEBUG-POLL-PROC] m.reply_to={getattr(m, 'reply_to', 'MISSING')}")
            if hasattr(m, 'reply_to') and m.reply_to:
                print(f"[DEBUG-POLL-PROC] m.reply_to.message_id={getattr(m.reply_to, 'message_id', 'N/A')}")
                print(f"[DEBUG-POLL-PROC] m.reply_to.reply_to_msg_id={getattr(m.reply_to, 'reply_to_msg_id', 'N/A')}")
            print(f"[DEBUG-POLL-PROC] m.invert_media={getattr(m, 'invert_media', 'N/A')} (True=poll has embedded question image)")
            print(f"[DEBUG-POLL-PROC] m.media_group_id={getattr(m, 'media_group_id', 'N/A')} (grouped=part of album with image)")
            print(f"[DEBUG-POLL-PROC] m.photo={'YES' if getattr(m, 'photo', None) else 'NO'} m.document={'YES' if getattr(m, 'document', None) else 'NO'}")
            print(f"[DEBUG-POLL-PROC] u (user_client)={'YES' if u else 'NO'}")
            
            # ── Record poll→question image dependency (Method 1 batch-side) ──
            # If /fetch didn't build the dependency index, record it here so
            # the NEXT batch run can use it for instant Pass 1 lookup.
            _poll_reply_to = m.reply_to_message_id or getattr(getattr(m, 'reply_to', None), 'message_id', None) or getattr(getattr(m, 'reply_to', None), 'reply_to_msg_id', None)
            if _poll_reply_to:
                _record_poll_dependency(uid, i, m.id, _poll_reply_to)
            
            # ── SEND: 1) Upload question image  2) Inline Reveal Answer  3) Native Quiz Poll ──
            
            correct_id = poll.correct_option_id
            if is_quiz and correct_id is None:
                correct_id = await _get_correct_option(i, m.id, poll, user_client=u)
            if correct_id is None:
                correct_id = -1
            print(f"[DEBUG-POLL-PROC] correct_id={correct_id} (after _get_correct_option)")
            
            # ── 1) Upload question image — ROBUST multi-method detection ──
            # In Telegram, poll question images can be:
            #   A) Embedded in the poll message (newer Telegram feature)
            #   B) A separate message that the poll replies to (traditional)
            #   C) Part of a grouped/album message set
            # Pyrofork doesn't support (A) natively, so we detect it ourselves.
            question_sent_id = rtmid  # default: already-uploaded question image
            
            # Extract src_reply_id early — used by Method A, B, and C for reply chain recording
            src_reply_id = m.reply_to_message_id or getattr(getattr(m, 'reply_to', None), 'message_id', None) or getattr(getattr(m, 'reply_to', None), 'reply_to_msg_id', None)
            
            # ── Method A: Try extracting embedded question media from the poll message ──
            embedded_media_id = None
            embedded_media_type = None
            if not rtmid:
                embedded_media_id, embedded_media_type = await _extract_poll_question_media(m, i, user_client=u, send_client=c)
                if embedded_media_id:
                    print(f"[DEBUG-POLL-PROC] Found embedded question media: type={embedded_media_type}")
                    if embedded_media_type == 'invert_media_forward':
                        # invert_media detected — skip to Method C (forward)
                        print(f"[DEBUG-POLL-PROC] invert_media flag detected — will try forwarding (Method C)")
                    else:
                        # Upload the extracted media to destination
                        q_id = await _upload_extracted_question_media(c, tcid, embedded_media_id, embedded_media_type, topic_id=topic_id)
                        if q_id:
                            question_sent_id = q_id
                            print(f"[DEBUG-POLL-PROC] Embedded question media uploaded, dest_id={q_id}")
                            # ── REPLY CHAIN: Record embedded question image mapping so
                            # messages that reply to the question image can find it
                            if link_rewrite_map is not None and src_reply_id:
                                link_rewrite_map[src_reply_id] = q_id
                                print(f"[CHAIN-POLL] Recorded embedded question image: src={src_reply_id} → dst={q_id}")
                        else:
                            print(f"[DEBUG-POLL-PROC] Embedded question media upload FAILED")
            
            # ── Method B: Check if poll replies to a media message (traditional approach) ──
            if not rtmid and not question_sent_id:
                print(f"[DEBUG-POLL-PROC] src_reply_id={src_reply_id} (from reply_to detection)")
                if src_reply_id:
                    # Try uploading question image using the robust helper
                    # Falls back through: user_client → send_client → forward
                    _wm_qi = _cached_watermark if _cached_watermark is not None else (await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT) if uid else _DEFAULT_WATERMARK_TEXT)
                    q_id = await _upload_question_image(c, i, src_reply_id, tcid, user_client=u, topic_id=topic_id, watermark_text=_wm_qi)
                    if q_id:
                        question_sent_id = q_id
                        # ── REPLY CHAIN: Record question image mapping so messages that
                        # reply to the question image (e.g. achiever analysis) can find it
                        if link_rewrite_map is not None:
                            link_rewrite_map[src_reply_id] = q_id
                            print(f"[CHAIN-POLL] Recorded question image: src={src_reply_id} → dst={q_id}")
                    else:
                        print(f"[DEBUG-POLL-PROC] Question image upload failed for src_reply_id={src_reply_id}")
                else:
                    print(f"[DEBUG-POLL-PROC] No src_reply_id found — poll has no reply-to reference")
            
            # ── Method C: Last resort — try forwarding the entire poll message ──
            # (This preserves the embedded image if nothing else worked)
            # Also triggered when invert_media=True was detected (embedded image that
            # Pyrofork can't extract, but forwarding preserves it)
            should_try_forward = (not rtmid and not question_sent_id) or embedded_media_type == 'invert_media_forward'
            if should_try_forward:
                # Try userbot first (more likely to have source channel access),
                # then fall back to bot client
                forward_clients = []
                if u:
                    forward_clients.append(("user_client", u))
                if c and c != u:
                    forward_clients.append(("bot_client", c))
                
                for client_name, fwd_c in forward_clients:
                    try:
                        resolved_src = await resolve_chat(fwd_c, i)
                        fwd_msgs = await fwd_c.forward_messages(tcid, m.id, resolved_src)
                        if fwd_msgs:
                            fwd_id = fwd_msgs.id if hasattr(fwd_msgs, 'id') else (fwd_msgs[0].id if isinstance(fwd_msgs, list) and fwd_msgs else None)
                            if fwd_id:
                                print(f"[DEBUG-POLL-PROC] Forwarded poll with image (method C, {client_name}), dest_id={fwd_id}")
                                # ── REPLY CHAIN: Record forwarded poll as the question image
                                # mapping so messages that reply to the question image can find it
                                if link_rewrite_map is not None and src_reply_id:
                                    link_rewrite_map[src_reply_id] = fwd_id
                                    print(f"[CHAIN-POLL] Recorded forwarded poll as question image: src={src_reply_id} → dst={fwd_id}")
                                # The forwarded message already has the image — we're done
                                sent = fwd_msgs if not isinstance(fwd_msgs, list) else fwd_msgs[0]
                                label = 'Forwarded quiz + poll.' if is_quiz else 'Forwarded poll + poll.'
                                return (label, sent.id if sent else fwd_id, is_closed, False)
                    except ChannelPrivate as e:
                        print(f"[DEBUG-POLL-PROC] {client_name}: ChannelPrivate — can't access source: {e}")
                    except (ChatIdInvalid, PeerIdInvalid) as e:
                        print(f"[DEBUG-POLL-PROC] {client_name}: Can't resolve source chat: {e}")
                    except Exception as e:
                        print(f"[DEBUG-POLL-PROC] {client_name}: Forward fallback failed: {e}")
            
            if rtmid:
                print(f"[DEBUG-POLL-PROC] Skipping question image upload — rtmid={rtmid} already set")
            
            print(f"[DEBUG-POLL-PROC] question_sent_id={question_sent_id} (final reply_to target)")
            
            # ── 2) Create Telegraph answer page for offline reveal ──
            reveal_url = None
            explanation_text = None
            explanation_image_url = None
            entry = None
            if is_quiz and correct_id is not None and correct_id != -1:
                answer_letter = chr(65 + correct_id)
                
                # ── Look up explanation: Memory → Live scan → Built-in solution ──
                try:
                    from plugins.explanation_listener import get_explanation_lookup, get_explanation, find_explanation_batch, check_poll_builtin_explanation
                    
                    # STEP 0: Check CHANNEL_EXPLANATIONS (instant, 0 API calls)
                    channel_expl = get_explanation_lookup(i)
                    entry = channel_expl.get(m.id)
                    
                    if entry:
                        explanation_text = entry.get("text")
                        print(f"[TELEGRAPH] src_msg_id={m.id} — CACHE HIT: kind={entry.get('kind')} text={'YES' if explanation_text else 'NO'} photo={'YES' if entry.get('has_photo') else 'NO'}")
                        _edlog(f"[PROC] src_msg_id={m.id} CACHE HIT: kind={entry.get('kind')} text={'YES' if explanation_text else 'NO'} photo={'YES' if entry.get('has_photo') else 'NO'} expl_msg_id={entry.get('explanation_msg_id')}")
                        if entry.get("has_photo") and u:
                            try:
                                expl_msg_id = entry["explanation_msg_id"]
                                resolved_src = await resolve_chat(u, i)
                                expl_msg = await u.get_messages(resolved_src, expl_msg_id)
                                _wm = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT) if uid else _DEFAULT_WATERMARK_TEXT
                                if expl_msg and expl_msg.photo:
                                    explanation_image_url = await _upload_telegraph_photo(u, expl_msg, watermark_text=_wm)
                                    print(f"[TELEGRAPH] src_msg_id={m.id} — Cache photo uploaded: {explanation_image_url}")
                                elif expl_msg and expl_msg.video:
                                    # Video explanation — upload a frame as Telegraph photo
                                    explanation_image_url = await _upload_telegraph_photo(u, expl_msg, watermark_text=_wm)
                                    print(f"[TELEGRAPH] src_msg_id={m.id} — Cache video frame uploaded: {explanation_image_url}")
                            except Exception as e:
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Cache photo/video upload error: {e}")
                    
                    # STEP 1: Sequential scan fallback (SKIPPED during batch)
                    # Sequential scan triggers FloodWait (30-60s per poll) — catastrophic for batch.
                    # During batch: only use cache + built-in solution (fast, no FloodWait).
                    # During single-message: full scan is OK (user can wait).
                    if (not explanation_text or not explanation_image_url) and u and not _skip_explanation_scan:
                        print(f"[TELEGRAPH] src_msg_id={m.id} — Trying sequential scan (text={'YES' if explanation_text else 'NO'} photo={'YES' if explanation_image_url else 'NO'})...")
                        _edlog(f"[PROC] src_msg_id={m.id} — Trying sequential scan")
                        try:
                            from plugins.explanation_listener import find_explanation_sequential
                            seq_result = await find_explanation_sequential(u, i, m.id, scan_window=100)
                            if seq_result:
                                if not explanation_text and seq_result.get("text"):
                                    explanation_text = seq_result.get("text")
                                if not explanation_image_url and seq_result.get("photo_file_id"):
                                    # Fetch the explanation message to upload photo to Telegraph
                                    try:
                                        expl_msg_id = seq_result["explanation_msg_id"]
                                        resolved_src = await resolve_chat(u, i)
                                        expl_msg = await u.get_messages(resolved_src, expl_msg_id)
                                        if expl_msg and (expl_msg.photo or expl_msg.video):
                                            explanation_image_url = await _upload_telegraph_photo(u, expl_msg)
                                            print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan photo uploaded: {explanation_image_url}")
                                    except Exception as e:
                                        print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan photo upload error: {e}")
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan result: text={'YES' if explanation_text else 'NO'} photo={'YES' if explanation_image_url else 'NO'}")
                                _edlog(f"[PROC] src_msg_id={m.id} SEQ: text={'YES' if explanation_text else 'NO'} photo={'YES' if explanation_image_url else 'NO'}")
                            else:
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan MISS")
                        except Exception as e:
                            print(f"[TELEGRAPH] src_msg_id={m.id} — Sequential scan error: {e}")
                    
                    # STEP 2: Built-in poll solution fallback (last resort)
                    if not explanation_text and u:
                        print(f"[TELEGRAPH] src_msg_id={m.id} — Trying built-in poll solution...")
                        try:
                            builtin_solution = await check_poll_builtin_explanation(u, i, m.id)
                            if builtin_solution:
                                explanation_text = builtin_solution
                                print(f"[TELEGRAPH] src_msg_id={m.id} — Found built-in solution: {builtin_solution[:80]}...")
                        except Exception as e:
                            print(f"[TELEGRAPH] src_msg_id={m.id} — Built-in solution check error: {e}")
                            
                except Exception as e:
                    print(f"[TELEGRAPH] src_msg_id={m.id} — Error looking up explanation: {e}")
                
                print(f"[TELEGRAPH] src_msg_id={m.id} answer_letter={answer_letter}")
                reveal_url = await _create_answer_page(answer_letter, image_url=explanation_image_url, answer_text=explanation_text)
                print(f"[TELEGRAPH] src_msg_id={m.id} — Answer page URL: {reveal_url}")
            else:
                print(f"[DEBUG-POLL-PROC] Skipping Telegraph page — is_quiz={is_quiz} correct_id={correct_id}")
            
            # ── 2) Send native QUIZ poll with 💡 View Answer button ──
            _edlog(f"[PROC] src_msg_id={m.id} is_quiz={is_quiz} correct_id={correct_id} entry={'YES' if entry else 'NO'} source_ch={i}")
            
            # Build keyboard with 💡 View Answer button (📖 added later after poll is sent)
            keyboard = build_inline_quiz(poll.question, options, correct_id, reveal_url=reveal_url)
            try:
                poll_kwargs = dict(
                    chat_id=tcid,
                    question=poll.question,
                    options=options,
                    type=PollType.QUIZ if is_quiz else PollType.REGULAR,
                    is_anonymous=getattr(poll, 'is_anonymous', True),
                    is_closed=False,
                    reply_to_message_id=question_sent_id
                )
                if is_quiz and correct_id is not None and correct_id != -1:
                    poll_kwargs['correct_option_id'] = correct_id
                    if explanation_text:
                        poll_kwargs['explanation'] = explanation_text[:200] if len(explanation_text) > 200 else explanation_text
                    elif explanation_image_url:
                        # Photo-only explanation — add hint to click View Answer button
                        poll_kwargs['explanation'] = "Tap 💡 View Answer for explanation"
                if topic_id:
                    poll_kwargs['message_thread_id'] = topic_id
                if getattr(poll, 'question_entities', None):
                    poll_kwargs['question_entities'] = poll.question_entities
                if getattr(poll, 'allows_multiple_answers', None):
                    poll_kwargs['allows_multiple_answers'] = poll.allows_multiple_answers
                # Attach buttons directly to the poll
                if keyboard:
                    poll_kwargs['reply_markup'] = keyboard
                print(f"[DEBUG-POLL-PROC] Sending native poll: type={'QUIZ' if is_quiz else 'REGULAR'} correct_option_id={poll_kwargs.get('correct_option_id')} has_explanation={'explanation' in poll_kwargs} has_buttons={'reply_markup' in poll_kwargs} reply_to={question_sent_id} topic_id={topic_id}")
                sent = await flood_wait_retry(c.send_poll(**poll_kwargs), "send_poll_process", dest_chat_id=tcid)
                print(f"[DEBUG-POLL-PROC] Native poll sent: id={sent.id if sent else 'None'}")
            except Exception as e:
                print(f"[POLL] Native poll send failed (full kwargs): {e}")
                import traceback; traceback.print_exc()
                
                # ── RETRY: Send poll with minimal kwargs (strip problematic fields) ──
# Common failures: question_entities, reply_markup, explanation cause errors
                # in some Telegram API versions or when data is corrupted.
                try:
                    minimal_kwargs = dict(
                        chat_id=tcid,
                        question=poll.question,
                        options=options,
                        type=PollType.QUIZ if is_quiz else PollType.REGULAR,
                        is_anonymous=getattr(poll, 'is_anonymous', True),
                        is_closed=False,
                    )
                    if is_quiz and correct_id is not None and correct_id != -1:
                        minimal_kwargs['correct_option_id'] = correct_id
                    if question_sent_id:
                        minimal_kwargs['reply_to_message_id'] = question_sent_id
                    if topic_id:
                        minimal_kwargs['message_thread_id'] = topic_id
                    print(f"[POLL-RETRY] Retrying with minimal kwargs (no entities/buttons/explanation)...")
                    sent = await flood_wait_retry(c.send_poll(**minimal_kwargs), "send_poll_process_retry", dest_chat_id=tcid)
                    print(f"[POLL-RETRY] Minimal poll sent: id={sent.id if sent else 'None'}")
                except Exception as retry_e:
                    print(f"[POLL-RETRY] Minimal poll with reply_to also failed: {retry_e}")
                    # ── FINAL RETRY: Send poll WITHOUT reply_to_message_id ──
                    # The download→re-upload of question images returns a message ID,
                    # but Telegram may reject the poll's reply_to if the message is too
                    # new or there's a cross-reference issue. Poll without reply_to is
                    # better than no poll at all.
                    try:
                        final_kwargs = dict(
                            chat_id=tcid,
                            question=poll.question,
                            options=options,
                            type=PollType.QUIZ if is_quiz else PollType.REGULAR,
                            is_anonymous=getattr(poll, 'is_anonymous', True),
                            is_closed=False,
                        )
                        if is_quiz and correct_id is not None and correct_id != -1:
                            final_kwargs['correct_option_id'] = correct_id
                        if topic_id:
                            final_kwargs['message_thread_id'] = topic_id
                        print(f"[POLL-RETRY] Final attempt WITHOUT reply_to_message_id...")
                        sent = await flood_wait_retry(c.send_poll(**final_kwargs), "send_poll_process_final", dest_chat_id=tcid)
                        print(f"[POLL-RETRY] Poll sent (no reply_to): id={sent.id if sent else 'None'}")
                    except Exception as final_e:
                        print(f"[POLL-RETRY] All poll send attempts failed: {final_e}")
                        sent = None
            
            # ── 3) Post-poll: 📖 View Explanation button DISABLED ──
            
            if sent and sent.id:
                # 📺 ANSWER TOPIC: Always add topic button after quiz poll
                if is_quiz and correct_id is not None and correct_id != -1 and tcid:
                    try:
                        _answer_letter = chr(65 + correct_id)
                        await handle_answer_buttons(c, u, i, m.id, _answer_letter, tcid, sent.id,
                                                    explanation_image_url=explanation_image_url,
                                                    explanation_text=explanation_text,
                                                    expl_msg_id=entry.get("explanation_msg_id") if entry else None,
                                                    has_photo=entry.get("has_photo", False) if entry else False)
                    except Exception as _ans_e:
                        print(f"[ANSWER-TOPIC] handle_answer_buttons failed (non-fatal): {_ans_e}")
                
                # ── 📖 View Explanation: NOT copied here — explanation will be sent in ──
                # natural order when the batch loop reaches it. When the explanation message
                # is processed, it will be sent as a reply to the poll (reply chain preserved)
                # and both buttons will be added at that point via the reply_dest_for_button path:
                #   📖 View Explanation on the POLL → links to the explanation
                #   🔙 Back to Question on the explanation → links back to the poll
                # Previously, copying the explanation immediately after the poll caused:
                #   1. Explanations uploaded out of order (right after poll, not 100 msgs later)
                #   2. Duplicate explanations when batch loop reached the same message again
                
                label = 'Sent quiz + poll.' if is_quiz else 'Sent poll + poll.'
                print(f"[DEBUG-POLL-PROC] ═══════ process_msg POLL END — sent.id={sent.id} label={label} ═══════")
                return (label, sent.id, is_closed, False)
            else:
                # Poll send FAILED — return failure so outer loop doesn't count it as success
                print(f"[DEBUG-POLL-PROC] ═══════ process_msg POLL END — FAILED (sent=None) ═══════")
                return ('Failed (poll send).', None, False, False)
        
        # ── CHECK: Can we actually handle this media? ──
        # Messages with web_page previews, contacts, locations, etc. have m.media set
        # but no specific type (photo, video, document). send_direct fails for these,
        # and download_media also fails. We must detect this and fall through to text.
        _has_handleable_media = bool(
            m.media and (m.photo or m.video or m.document or m.audio or m.animation or
                         m.voice or m.video_note or m.sticker)
        )
        
        if _has_handleable_media:
            _media_type = type(m.media).__name__ if m.media else 'None'
            print(f"[PROC-MEDIA] src_msg_id={m.id} media_type={_media_type} has_photo={bool(m.photo)} has_video={bool(m.video)} has_document={bool(m.document)} has_caption={bool(m.caption)}")
            
            orig_text = m.caption.markdown if hasattr(m.caption, 'markdown') else (str(m.caption) if m.caption else '')
            proc_text = await process_text_with_rules(d, orig_text)
            user_cap = await get_user_data_key(d, 'caption', '')
            
            # Rewrite Telegram links in caption to point to destination channel
            if _rewriter is not None and proc_text:
                proc_text, _, _caption_unresolved = _rewriter.rewrite(proc_text, [])
                if _caption_unresolved:
                    had_unresolved_links = True
            elif link_rewrite_map is not None and proc_text:
                proc_text, had_unresolved = rewrite_telegram_links(
                    proc_text, i, dest_channel_id, dest_channel_username, link_rewrite_map,
                    source_channel_username=source_channel_username,
                    source_channel_id=source_channel_id,
                    multi_source_channels=multi_source_channels
                )
                if had_unresolved:
                    had_unresolved_links = True
            
            # ULTRA PRO MAX: Also rewrite caption_entities URLs (blue clickable links)
            rewritten_caption_entities = None
            raw_caption = str(m.caption) if m.caption else ''
            if _rewriter is not None and m.caption_entities:
                raw_caption, rewritten_caption_entities, _cap_unresolved = _rewriter.rewrite(raw_caption, m.caption_entities)
                if _cap_unresolved:
                    had_unresolved_links = True
            elif link_rewrite_map is not None and m.caption_entities:
                rewritten_caption_entities, cap_entity_unresolved, raw_caption = rewrite_entity_urls(
                    m.caption_entities, i, dest_channel_id, dest_channel_username, link_rewrite_map,
                    source_channel_username=source_channel_username,
                    source_channel_id=source_channel_id,
                    raw_text=raw_caption,
                    multi_source_channels=multi_source_channels
                )
                if cap_entity_unresolved:
                    had_unresolved_links = True
            
            # Get source channel name for caption
            source_name = ''
            try:
                resolved_i = await resolve_chat(c, i)
                chat = await c.get_chat(resolved_i)
                if chat and chat.title:
                    source_name = chat.title
            except (ChatIdInvalid, PeerIdInvalid):
                # Bot can't see this chat - try userbot
                try:
                    if u:
                        resolved_u = await resolve_chat(u, i)
                        chat = await u.get_chat(resolved_u)
                        if chat and chat.title:
                            source_name = chat.title
                except Exception:
                    pass
            except Exception:
                pass
            
            # Build final caption: original text + channel name + user custom caption
            if proc_text and source_name and user_cap:
                ft = f'{proc_text}\n\n__{source_name}__\n\n{user_cap}'
            elif proc_text and source_name:
                ft = f'{proc_text}\n\n__{source_name}__'
            elif proc_text and user_cap:
                ft = f'{proc_text}\n\n{user_cap}'
            elif user_cap and source_name:
                ft = f'__{source_name}__\n\n{user_cap}'
            elif source_name:
                ft = f'__{source_name}__'
            elif proc_text:
                ft = proc_text
            else:
                ft = user_cap
            
            # Enforce: "Extracted by" must always be followed by "HARRY"
            # AND remove "backup" word (case-insensitive) from caption
            # AND for VIDEOS only: remove markdown links [text](url) pointing to source channel
            # For non-videos (PDFs, photos, documents, audio): KEEP blue links — they're
            # navigation links that users need. Videos don't show captions well anyway.
            if ft:
                # Step 1: For VIDEOS only — remove inline Markdown links [text](url)
                # that point to the SOURCE channel. Links rewritten to destination are KEPT.
                # Videos show captions poorly (truncated), so blue links are useless there.
                if m.video:
                    _src_patterns_for_removal = []
                    _src_str = str(i)
                    if _src_str.lstrip('-').isdigit():
                        _clean_src = _src_str.lstrip('-')
                        if _clean_src.startswith('100'):
                            _clean_src = _clean_src[3:]
                        if _clean_src:
                            _src_patterns_for_removal.append(re.escape(f't.me/c/{_clean_src}/'))
                    else:
                        _src_patterns_for_removal.append(re.escape(f't.me/{_src_str}/'))
                    if source_channel_username:
                        _src_patterns_for_removal.append(re.escape(f't.me/{source_channel_username}/'))
                    
                    if _src_patterns_for_removal:
                        _src_pat = '|'.join(_src_patterns_for_removal)
                        ft = re.sub(r'\[(?:[^\]])*\]\([^)]*(?:' + _src_pat + r')[^)]*\)', '', ft)
                
                # Step 2: Remove the word "backup" (case-insensitive) from caption
                # Handles: "BACKUP", "Backup", "backup " etc.
                ft = re.sub(r'(?i)\bback\s*up\b', '', ft)
                # Step 3: Strip bold/italic markers around "Extracted by"
                ft = re.sub(r'(?i)(\*{1,2}|_{1,2})\s*Extracted by\s*(\*{1,2}|_{1,2})', 'Extracted by', ft)
                # Step 4: Replace "Extracted by" + everything until newline with "Extracted by HARRY"
                ft = re.sub(r'(?i)Extracted by\s*[^\n]*', 'Extracted by HARRY', ft)
                # Step 5: Clean any leftover @mentions on the next line
                ft = re.sub(r'(Extracted by HARRY)\s*\n?\s*@[^\s\n]+', r'\1', ft)
                # Step 6: Clean up spacing — remove double spaces and excessive blank lines
                ft = re.sub(r' {2,}', ' ', ft)
                ft = re.sub(r'\n{3,}', '\n\n', ft)
                # Strip trailing/leading whitespace from each line
                lines = [line.strip() for line in ft.split('\n')]
                ft = '\n'.join(lines).strip()
            
            # ── ENTITY-BASED CAPTION ──────────────────────────────────────────
            # Pyrogram's .markdown produces MarkdownV2 syntax (blockquotes → ">",
            # spoilers → "||", etc.).  Sending it with ParseMode.MARKDOWN (v1)
            # destroys blockquote formatting: ">" appears as literal text and
            # newlines collapse.  When entities are available, bypass markdown
            # entirely: send plain text + caption_entities.  Entities are the
            # native Telegram format and work for every type (blockquote, bold,
            # code, spoiler, text_link, …) without any parser.
            _final_entities = (
                rewritten_caption_entities
                if rewritten_caption_entities
                else (m.caption_entities if m.caption else None)
            )
            _use_entity_caption = bool(_final_entities)
            if _use_entity_caption:
                # Apply text-level substitutions to the raw plain-text caption
                # (no markdown markers to worry about here)
                _raw = raw_caption
                if _raw:
                    _raw = re.sub(r'(?i)\bback\s*up\b', '', _raw)
                    _raw = re.sub(r'(?i)Extracted by\s*[^\n]*', 'Extracted by HARRY', _raw)
                    _raw = re.sub(r'(Extracted by HARRY)\s*\n?\s*@[^\s\n]+', r'\1', _raw)
                    _raw = re.sub(r' {2,}', ' ', _raw)
                    _raw = re.sub(r'\n{3,}', '\n\n', _raw)
                    _raw = '\n'.join(line.strip() for line in _raw.split('\n')).strip()
                if user_cap:
                    _raw = ((_raw + '\n\n' + user_cap) if _raw else user_cap).strip()
                raw_caption_final = _raw
            else:
                raw_caption_final = None
            # Unified caption + entities for every send call below
            _send_caption = raw_caption_final if _use_entity_caption else (ft or None)
            _send_entities = _final_entities if _use_entity_caption else None
            # Guard against ENTITY_BOUNDS_INVALID: text substitutions above may
            # shorten the caption while entities still reference positions in the
            # original longer text.  Drop any entity whose offset+length exceeds
            # the final caption length so Telegram never rejects the send.
            if _send_entities and _send_caption:
                _cap_len = len(_send_caption)
                _send_entities = [
                    _e for _e in _send_entities
                    if getattr(_e, 'offset', 0) + getattr(_e, 'length', 0) <= _cap_len
                ] or None
            # ─────────────────────────────────────────────────────────────────

            if lt == 'public' and not emp.get(i, False):
                success, sent_id, is_closed_poll = await send_direct(
                    c, m, tcid, _send_caption, rtmid, u=u, source_chat=i,
                    uid=uid, topic_id=topic_id,
                    reply_dest_for_button=_reply_to_dest_for_button,
                    link_rewrite_map=link_rewrite_map,
                    caption_entities=_send_entities,
                )
                if success:
                    # 📖/🔙 buttons are already added inside send_direct() — no need to add again here
                    return ('Sent directly.', sent_id, is_closed_poll, False)
                else:
                    # send_direct failed — fall through to download+reupload path
                    print(f"[PROCESS_MSG] send_direct FAILED for msg={m.id} — falling back to download+reupload")
            
            # CHECK CANCEL before starting download — avoids wasted bandwidth if /stop was sent
            if should_cancel(uid):
                return ('Cancelled.', None, False, False)
            
            st = time.time()
            p = await c.send_message(d, 'Downloading...')

            # BUG FIX: Download to a simple numeric filename first to avoid
            # Unicode/special character issues with cv2 and ffmpeg.
            # The fancy filename is applied later by rename_file().
            # Previously, c_name used the original filename with emojis/special chars,
            # which caused cv2.VideoCapture and ffmpeg to fail with "No such file"
            # (error 2) on systems where Unicode paths aren't fully supported.
            c_name = None
            # Determine the correct extension for the temp download filename
            dl_ext = 'mp4'  # default
            if m.video:
                file_name = m.video.file_name
                if file_name and '.' in file_name:
                    dl_ext = file_name.rsplit('.', 1)[-1].lower()
                    if dl_ext not in {'mp4','mkv','avi','mov','wmv','flv','webm','m4v','3gp'}:
                        dl_ext = 'mp4'
            elif m.animation:
                file_name = m.animation.file_name
                dl_ext = 'mp4'
                if file_name and '.' in file_name:
                    dl_ext = file_name.rsplit('.', 1)[-1].lower()
                    if dl_ext not in {'mp4','gif','webm'}:
                        dl_ext = 'mp4'
            elif m.audio:
                file_name = m.audio.file_name
                dl_ext = 'mp3'
                if file_name and '.' in file_name:
                    dl_ext = file_name.rsplit('.', 1)[-1].lower()
                    if dl_ext not in {'mp3','wav','flac','aac','ogg','wma','m4a','opus'}:
                        dl_ext = 'mp3'
            elif m.document:
                file_name = m.document.file_name
                dl_ext = 'bin'
                if file_name and '.' in file_name:
                    dl_ext = file_name.rsplit('.', 1)[-1].lower()
            elif m.photo:
                dl_ext = 'jpg'
            # Use absolute path for download — prevents "no such file" errors from CWD changes
            # Use random suffix to avoid filename collisions when multiple downloads happen in same second
            download_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'downloads')
            os.makedirs(download_dir, exist_ok=True)
            c_name = os.path.join(download_dir, f"{int(time.time())}_{random.randint(1000,9999)}.{dl_ext}")
    
            f = await _download_with_retry(u, m, file_name=c_name, progress=prog, progress_args=(c, d, p.id, st, "Downloading"))
            
            if not f:
                await c.edit_message_text(d, p.id, 'Failed.')
                return ('Failed.', None, False, False)
            
            # Verify downloaded file actually exists before proceeding
            if not os.path.exists(f):
                print(f"[DL-BUG] download_media returned '{f}' but file does not exist!")
                await c.edit_message_text(d, p.id, 'Download failed — file not found on disk.')
                return ('Failed (file missing).', None, False, False)
            
            print(f"[DL] Downloaded: {f} ({os.path.getsize(f) / (1024*1024):.1f} MB)")
            
            # ── Cancel check after download (before upload) ──
            # If /stop was pressed during the download, don't waste time uploading
            if uid and should_cancel(uid):
                print(f"[STOP] Cancel detected after download for uid={uid} — cleaning up")
                if os.path.exists(f): os.remove(f)
                _ram_reclaim()
                raise asyncio.CancelledError()
            
            # ── Apply watermark to downloaded media before upload ──
            try:
                wm_text = _cached_watermark if _cached_watermark is not None else (await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT))
                if wm_text and wm_text.strip():
                    f = await _apply_watermark_to_file(f, wm_text, is_video=bool(m.video), is_image=bool(m.photo))
            except Exception as _wm_err:
                print(f"[WATERMARK] Failed for msg {m.id}: {_wm_err}")
            
            await c.edit_message_text(d, p.id, 'Renaming...')
            # Get the original filename from the Telegram message for rename_file
            # (we download to a safe numeric path, but need the original name for display)
            orig_name = None
            if m.video and m.video.file_name:
                orig_name = m.video.file_name
            elif m.animation and m.animation.file_name:
                orig_name = m.animation.file_name
            elif m.audio and m.audio.file_name:
                orig_name = m.audio.file_name
            elif m.document and m.document.file_name:
                orig_name = m.document.file_name
            
            # Compute the display filename for upload — NO on-disk rename.
            # rename_file() now returns just the display basename string.
            # The on-disk file stays at the safe numeric path (e.g. 1778744725.mp4)
            # so cv2/ffmpeg never see emoji/special-char paths.
            # Pyrogram's file_name= parameter sets the name shown in Telegram.
            display_name = None
            try:
                display_name = await rename_file(f, d, p, original_name=orig_name)
            except Exception as e:
                print(f"[RENAME] rename_file error: {e}")
                import traceback; traceback.print_exc()
            
            # Fallback: if rename_file failed, use original name or disk basename
            if not display_name:
                if orig_name:
                    display_name = orig_name
                else:
                    display_name = os.path.basename(f)
                print(f"[RENAME] Using fallback display_name: {display_name}")
            
            # Verify the on-disk file still exists (should never fail now since we don't rename)
            if not os.path.exists(f):
                print(f"[RENAME-BUG] downloaded file '{f}' does not exist!")
                await c.edit_message_text(d, p.id, 'Download failed — file not found.')
                return ('Failed (file missing).', None, False, False)
            
            fsize = os.path.getsize(f) / (1024 * 1024 * 1024)
            th = thumbnail(d)
            
            global_userbot = get_Y()
            if fsize > 2 and global_userbot:
                st = time.time()
                await c.edit_message_text(d, p.id, 'File is larger than 2GB. Using alternative method...')
                await upd_dlg(global_userbot)
                dur, h, w = 1, None, None
                try:
                    mtd = await get_video_metadata(f)
                    dur, h, w = mtd['duration'], mtd.get('width'), mtd.get('height')
                except Exception as e:
                    print(f"[VIDEO-META] get_video_metadata failed for large file {f}: {e}")
                try:
                    th = await screenshot(f, dur, d)
                except Exception as e:
                    print(f"[VIDEO-THUMB] screenshot failed for large file {f}: {e}")
                
                send_funcs = {'video': global_userbot.send_video, 'video_note': global_userbot.send_video_note, 
                            'voice': global_userbot.send_voice, 'audio': global_userbot.send_audio, 
                            'photo': global_userbot.send_photo, 'document': global_userbot.send_document}
                
                for mtype, func in send_funcs.items():
                    if f.endswith('.mp4'): mtype = 'video'
                    if getattr(m, mtype, None):
                        # Build kwargs — include parse_mode only when caption is present (markdown links)
                        _cap_val = ft if m.caption and mtype not in ['video_note', 'voice'] else None
                        _send_kwargs = dict(
                            chat_id=LOG_GROUP, document=f, file_name=display_name,
                            thumb=th if mtype == 'video' else None,
                            duration=dur if mtype == 'video' else None,
                            height=h if mtype == 'video' else None,
                            width=w if mtype == 'video' else None,
                            progress=prog, progress_args=(c, d, p.id, st, "Uploading"))
                        if _cap_val:
                            sent = await flood_wait_retry(_safe_markdown_send(func, f"large_file_{mtype}",
                                **_send_kwargs, caption=_cap_val), f"large_file_{mtype}")
                        else:
                            sent = await flood_wait_retry(func(**_send_kwargs), f"large_file_{mtype}")
                        sent_msg_id = sent.id
                        break
                else:
                    _cap_val = ft if m.caption else None
                    if _cap_val:
                        sent = await flood_wait_retry(_safe_markdown_send(global_userbot.send_document, "large_file_document",
                            chat_id=LOG_GROUP, document=f, file_name=display_name, thumb=th,
                            caption=_cap_val, progress=prog,
                            progress_args=(c, d, p.id, st, "Uploading")), "large_file_document")
                    else:
                        sent = await flood_wait_retry(global_userbot.send_document(LOG_GROUP, f, file_name=display_name, thumb=th,
                                                    progress=prog, progress_args=(c, d, p.id, st, "Uploading")), "large_file_document")
                    sent_msg_id = sent.id
                
                _copy_kwargs = dict(chat_id=d, from_chat_id=LOG_GROUP, message_ids=sent.id)
                if topic_id:
                    _copy_kwargs['message_thread_id'] = topic_id
                if _reply_to_dest_for_button:
                    _copy_kwargs['reply_to_message_id'] = _reply_to_dest_for_button
                msg = await flood_wait_retry(c.copy_message(**_copy_kwargs), "copy_large_file")
                sent_msg_id = msg.id
                os.remove(f)
                _ram_reclaim()  # Return freed download/upload buffers to OS
                await c.delete_messages(d, p.id)
                
                return ('Done (Large file).', sent_msg_id, False, had_unresolved_links)
            
            await c.edit_message_text(d, p.id, 'Uploading...')
            st = time.time()
            
            # CHECK CANCEL before starting upload — skip if /stop was sent during download
            if should_cancel(uid):
                if os.path.exists(f): os.remove(f)
                _ram_reclaim()  # Return freed download/upload buffers to OS
                await c.edit_message_text(d, p.id, 'Cancelled.')
                return ('Cancelled.', None, False, False)

            try:
                video_extensions = ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.ogv']
                audio_extensions = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus', '.aiff', '.ac3']
                file_ext = os.path.splitext(f)[1].lower()
                if m.video or (m.document and file_ext in video_extensions):
                    # Get video metadata — with fallback if cv2/ffmpeg fail
                    dur, h, w = 1, None, None
                    th = None
                    try:
                        mtd = await get_video_metadata(f)
                        dur = mtd['duration']
                        h = mtd.get('width')
                        w = mtd.get('height')
                    except Exception as e:
                        print(f"[VIDEO-META] get_video_metadata failed for {f}: {e}")
                    try:
                        th = await screenshot(f, dur, d)
                    except Exception as e:
                        print(f"[VIDEO-THUMB] screenshot failed for {f}: {e}")
                    # Build send_video kwargs — only include optional params if they have values
                    video_kwargs = dict(
                        chat_id=tcid,
                        video=f,
                        caption=_send_caption if m.caption else None,
                        file_name=display_name,
                        duration=dur if dur and dur > 1 else None,
                        progress=prog,
                        progress_args=(c, d, p.id, st, "Uploading"),
                    )
                    if topic_id:
                        video_kwargs['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        video_kwargs['reply_to_message_id'] = _reply_to_dest_for_button
                    if th and os.path.exists(th):
                        video_kwargs['thumb'] = th
                    if w and h:
                        video_kwargs['width'] = w
                        video_kwargs['height'] = h
                    if video_kwargs.get('caption'):
                        if _send_entities:
                            video_kwargs['caption_entities'] = _send_entities
                            sent = await flood_wait_retry(c.send_video(**video_kwargs), "upload_video", dest_chat_id=tcid)
                        else:
                            sent = await flood_wait_retry(_safe_markdown_send(c.send_video, "upload_video", **video_kwargs), "upload_video", dest_chat_id=tcid)
                    else:
                        sent = await flood_wait_retry(c.send_video(**video_kwargs), "upload_video", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.animation:
                    _ani_kw = dict(chat_id=tcid, animation=f, caption=_send_caption if m.caption else None)
                    if topic_id:
                        _ani_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _ani_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    if _ani_kw.get('caption'):
                        if _send_entities:
                            _ani_kw['caption_entities'] = _send_entities
                            sent = await flood_wait_retry(c.send_animation(**_ani_kw), "upload_animation", dest_chat_id=tcid)
                        else:
                            sent = await flood_wait_retry(_safe_markdown_send(c.send_animation, "upload_animation",
                                **_ani_kw), "upload_animation", dest_chat_id=tcid)
                    else:
                        sent = await flood_wait_retry(c.send_animation(**_ani_kw), "upload_animation", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.video_note:
                    _vn_kw = dict(chat_id=tcid, video_note=f, progress=prog, 
                                        progress_args=(c, d, p.id, st, "Uploading"))
                    if topic_id:
                        _vn_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _vn_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    sent = await flood_wait_retry(c.send_video_note(**_vn_kw), "upload_video_note", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.voice:
                    _vc_kw = dict(chat_id=tcid, voice=f, progress=prog, progress_args=(c, d, p.id, st, "Uploading"))
                    if topic_id:
                        _vc_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _vc_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    sent = await flood_wait_retry(c.send_voice(**_vc_kw), "upload_voice", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.sticker:
                    _st_kw = dict(chat_id=tcid, sticker=m.sticker.file_id)
                    if topic_id:
                        _st_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _st_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    sent = await flood_wait_retry(c.send_sticker(**_st_kw), "upload_sticker", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.audio or (m.document and file_ext in audio_extensions):
                    _cap_val = _send_caption if m.caption else None
                    _au_kw = dict(chat_id=tcid, audio=f,
                                file_name=display_name, thumb=th, progress=prog, progress_args=(c, d, p.id, st, "Uploading"))
                    if topic_id:
                        _au_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _au_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    if _cap_val:
                        _au_kw['caption'] = _cap_val
                        if _send_entities:
                            _au_kw['caption_entities'] = _send_entities
                            sent = await flood_wait_retry(c.send_audio(**_au_kw), "upload_audio", dest_chat_id=tcid)
                        else:
                            sent = await flood_wait_retry(_safe_markdown_send(c.send_audio, "upload_audio",
                                **_au_kw), "upload_audio", dest_chat_id=tcid)
                    else:
                        sent = await flood_wait_retry(c.send_audio(**_au_kw), "upload_audio", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.photo:
                    _cap_val = _send_caption if m.caption else None
                    _ph_kw = dict(chat_id=tcid, photo=f,
                                progress=prog, progress_args=(c, d, p.id, st, "Uploading"))
                    if topic_id:
                        _ph_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _ph_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    if _cap_val:
                        _ph_kw['caption'] = _cap_val
                        if _send_entities:
                            _ph_kw['caption_entities'] = _send_entities
                            sent = await flood_wait_retry(c.send_photo(**_ph_kw), "upload_photo", dest_chat_id=tcid)
                        else:
                            sent = await flood_wait_retry(_safe_markdown_send(c.send_photo, "upload_photo",
                                **_ph_kw), "upload_photo", dest_chat_id=tcid)
                    else:
                        sent = await flood_wait_retry(c.send_photo(**_ph_kw), "upload_photo", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                elif m.document:
                    _cap_val = _send_caption if m.caption else None
                    _doc_kw = dict(chat_id=tcid, document=f,
                                file_name=display_name, progress=prog, progress_args=(c, d, p.id, st, "Uploading"))
                    if topic_id:
                        _doc_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _doc_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    if _cap_val:
                        _doc_kw['caption'] = _cap_val
                        if _send_entities:
                            _doc_kw['caption_entities'] = _send_entities
                            sent = await flood_wait_retry(c.send_document(**_doc_kw), "upload_document", dest_chat_id=tcid)
                        else:
                            sent = await flood_wait_retry(_safe_markdown_send(c.send_document, "upload_document",
                                **_doc_kw), "upload_document", dest_chat_id=tcid)
                    else:
                        sent = await flood_wait_retry(c.send_document(**_doc_kw), "upload_document", dest_chat_id=tcid)
                    sent_msg_id = sent.id
                else:
                    _cap_val = _send_caption if m.caption else None
                    _fb_kw = dict(chat_id=tcid, document=f,
                                file_name=display_name, progress=prog, progress_args=(c, d, p.id, st, "Uploading"))
                    if topic_id:
                        _fb_kw['message_thread_id'] = topic_id
                    if _reply_to_dest_for_button:
                        _fb_kw['reply_to_message_id'] = _reply_to_dest_for_button
                    if _cap_val:
                        _fb_kw['caption'] = _cap_val
                        if _send_entities:
                            _fb_kw['caption_entities'] = _send_entities
                            sent = await flood_wait_retry(c.send_document(**_fb_kw), "upload_document_fallback", dest_chat_id=tcid)
                        else:
                            sent = await flood_wait_retry(_safe_markdown_send(c.send_document, "upload_document_fallback",
                                **_fb_kw), "upload_document_fallback", dest_chat_id=tcid)
                    else:
                        sent = await flood_wait_retry(c.send_document(**_fb_kw), "upload_document_fallback", dest_chat_id=tcid)
                    sent_msg_id = sent.id
            except FloodWait as e:
                # FloodWait during upload — stop the batch
                wait_secs = e.value if hasattr(e, 'value') else 30
                if os.path.exists(f): os.remove(f)
                _ram_reclaim()  # Return freed download/upload buffers to OS
                print(f"[FLOOD] process_msg upload: FloodWait {_format_duration(wait_secs)} — stopping batch")
                raise
            except Exception as e:
                await c.edit_message_text(d, p.id, f'Upload failed: {str(e)[:30]}')
                if os.path.exists(f): os.remove(f)
                _ram_reclaim()  # Return freed download/upload buffers to OS
                return ('Failed.', None, False, False)
            
            # Safety: sent could be None if send succeeded but returned no message object
            if not sent_msg_id:
                print(f"[PROC-MEDIA] src_msg_id={m.id} — sent_msg_id is None after upload!")
                if os.path.exists(f): os.remove(f)
                _ram_reclaim()
                return ('Failed (no sent_id).', None, False, False)
            
            os.remove(f)
            _ram_reclaim()  # Return freed download/upload buffers to OS
            await c.delete_messages(d, p.id)
            
            # Log reply_to confirmation for media uploads
            if _reply_to_dest_for_button:
                print(f"[REPLY-TO] ✅ src_msg_id={m.id} media sent as reply to dest_msg_id={_reply_to_dest_for_button}")
            
            # ── 📖 View Explanation: when explanation arrives, add on the POLL (B') ──
            # Explanation C replies to poll B. reply_to_dest_for_button = B' (poll in dest).
            # Add 📖 View Explanation on B' → C' (this explanation).
            if _reply_to_dest_for_button and sent_msg_id:
                try:
                    _reply_url = _build_telegram_link(tcid, sent_msg_id)
                    if _reply_url:
                        _edlog(f"[PROC-MEDIA-UPLOAD] src_msg_id={m.id} — Adding 📖 on poll {_reply_to_dest_for_button} → {_reply_url}")
                        await _add_explanation_button(c, tcid, _reply_to_dest_for_button, _reply_url)
                        # 🔙 Back to Question: add on the explanation (C') → links back to poll (B')
                        _poll_url = _build_telegram_link(tcid, _reply_to_dest_for_button)
                        if _poll_url:
                            await _add_inline_button(c, tcid, sent_msg_id, "🔙 Back to Question", _poll_url, log_prefix="BACK-BTN")
                except Exception as _rb_e:
                    _edlog(f"[PROC-MEDIA-UPLOAD] src_msg_id={m.id} — 📖/🔙 button error: {_rb_e}")
            
            return ('Done.', sent_msg_id, False, had_unresolved_links)
            
        elif m.text or (m.media and not _has_handleable_media):
            # Handles: 1) Pure text messages  2) Messages with unhandleable media (WebPage, Contact, Location)
            # that have text content but no downloadable media type
            _is_unhandleable_media = bool(m.media and not _has_handleable_media)
            if _is_unhandleable_media:
                _unhandleable_type = type(m.media).__name__ if m.media else 'Unknown'
                print(f"[PROC-TEXT-MEDIA] src_msg_id={m.id} — unhandleable media '{_unhandleable_type}' with text, sending as text (text_len={len(str(m.text or ''))})")
            else:
                print(f"[PROC-TEXT] src_msg_id={m.id} text_len={len(str(m.text))} has_entities={bool(m.entities)} reply_to={m.reply_to_message_id}")
            # PERF: Use cached source_name from batch loop (0 API calls)
            # Previously: 2-4 resolve_chat + get_chat API calls per text message
            # Now: 0 API calls — source_name is resolved once at batch start
            source_name = _cached_source_name if _cached_source_name is not None else ''
            if not source_name:
                # Fallback only if cache not provided
                try:
                    resolved_i = await resolve_chat(c, i)
                    chat = await c.get_chat(resolved_i)
                    if chat and chat.title:
                        source_name = chat.title
                except (ChatIdInvalid, PeerIdInvalid):
                    try:
                        if u:
                            resolved_u = await resolve_chat(u, i)
                            chat = await u.get_chat(resolved_u)
                            if chat and chat.title:
                                source_name = chat.title
                    except Exception:
                        pass
                except Exception:
                    pass
            
            # Build forwarded prefix if the message is forwarded
            fwd_prefix = ''
            if m.forward_origin:
                fwd = m.forward_origin
                try:
                    if hasattr(fwd, 'sender_user') and fwd.sender_user:
                        fwd_name = fwd.sender_user.first_name or ''
                        if fwd.sender_user.last_name:
                            fwd_name += f' {fwd.sender_user.last_name}'
                        fwd_prefix = f'Forwarded from {fwd_name}\n\n'
                    elif hasattr(fwd, 'chat') and fwd.chat:
                        fwd_prefix = f'Forwarded from {fwd.chat.title or fwd.chat.first_name or "Unknown"}\n\n'
                    elif hasattr(fwd, 'hidden_user_name') and fwd.hidden_user_name:
                        fwd_prefix = f'Forwarded from {fwd.hidden_user_name}\n\n'
                except Exception:
                    pass
            
            # ═══════════════════════════════════════════════════════════════
            # ULTRA PRO MAX LINK REWRITE — DUAL APPROACH
            # Method 1: Rewrite URLs in message ENTITIES (blue clickable links)
            # Method 2: Rewrite URLs in markdown TEXT (bare URLs + markdown links)
            # Both run to guarantee maximum coverage.
            # ═══════════════════════════════════════════════════════════════
            
            # Get raw text (no markdown conversion — preserves entity offsets)
            raw_text = str(m.text) if m.text else ''
            
            # ── Method 1: Rewrite entities (handles MessageEntityTextUrl — blue links) ──
            # Do this FIRST because it may modify raw_text for 'url' type entities
            rewritten_entities = None
            if _rewriter is not None and m.entities:
                raw_text, rewritten_entities, _ent_unresolved = _rewriter.rewrite(raw_text, m.entities)
                if _ent_unresolved:
                    had_unresolved_links = True
            elif link_rewrite_map is not None and m.entities:
                rewritten_entities, entity_unresolved, raw_text = rewrite_entity_urls(
                    m.entities, i, dest_channel_id, dest_channel_username, link_rewrite_map,
                    source_channel_username=source_channel_username,
                    source_channel_id=source_channel_id,
                    raw_text=raw_text,
                    multi_source_channels=multi_source_channels
                )
                if entity_unresolved:
                    had_unresolved_links = True
            
            # Now build final_text from (potentially modified) raw_text
            final_text = raw_text
            if fwd_prefix:
                final_text = fwd_prefix + final_text
            if source_name:
                final_text = final_text + f'\n\n{source_name}' if final_text else source_name
            
            # Adjust entity offsets for fwd_prefix + source_name additions
            if rewritten_entities and fwd_prefix:
                prefix_len = len(fwd_prefix)
                adjusted_entities = []
                for ent in rewritten_entities:
                    new_ent = copy.deepcopy(ent)
                    new_ent.offset = ent.offset + prefix_len
                    adjusted_entities.append(new_ent)
                rewritten_entities = adjusted_entities
            
            # ── Method 2: Rewrite markdown text (handles bare URLs + markdown links) ──
            # Build markdown version for fallback text rewriting
            # SAFE: m.text can be None when message enters via unhandleable-media path
            _raw_markdown = m.text.markdown if hasattr(m.text, 'markdown') else (str(m.text) if m.text else '')
            md_text = f'{fwd_prefix}{_raw_markdown}\n\n__{source_name}__' if source_name else f'{fwd_prefix}{_raw_markdown}'
            if _rewriter is not None:
                rewritten_md, _, _md_unresolved = _rewriter.rewrite(md_text, [])
                if _md_unresolved:
                    had_unresolved_links = True
            elif link_rewrite_map is not None:
                rewritten_md, md_unresolved = rewrite_telegram_links(
                    md_text, i, dest_channel_id, dest_channel_username, link_rewrite_map,
                    source_channel_username=source_channel_username,
                    source_channel_id=source_channel_id,
                    multi_source_channels=multi_source_channels
                )
                if md_unresolved:
                    had_unresolved_links = True
            
            # ── SEND: Try entity-based send first (preserves blue links perfectly),
            # then fallback to markdown send (rewrites text but may lose some entities) ──
            sent = None
            
            # Build reply_to for text messages — preserves reply chain (explanations replying to polls, etc.)
            _text_reply_to = reply_to_destination_id  # dest ID of the message this replies to
            
            if rewritten_entities:
                # PRIORITY: Send with raw text + rewritten entities — preserves ALL blue links
                _text_kwargs = dict(chat_id=tcid, text=final_text, entities=rewritten_entities)
                if topic_id:
                    _text_kwargs['message_thread_id'] = topic_id
                if _text_reply_to:
                    _text_kwargs['reply_to_message_id'] = _text_reply_to
                try:
                    sent = await flood_wait_retry(_safe_markdown_send(c.send_message, "send_text_entity",
                        **_text_kwargs), "send_text_entity", dest_chat_id=tcid)
                    _edlog(f"[SEND-TEXT] Sent with rewritten entities — {len(rewritten_entities)} entities preserved")
                except Exception as ent_err:
                    _edlog(f"[SEND-TEXT] Entity send failed: {ent_err}, falling back to markdown")
                    sent = None
            
            if not sent:
                # FALLBACK: Send with markdown text rewriting — may lose some blue links
                _text_kwargs = dict(chat_id=tcid, text=rewritten_md if (link_rewrite_map is not None or _rewriter is not None) else md_text)
                if topic_id:
                    _text_kwargs['message_thread_id'] = topic_id
                if _text_reply_to:
                    _text_kwargs['reply_to_message_id'] = _text_reply_to
                sent = await flood_wait_retry(_safe_markdown_send(c.send_message, "send_text",
                    **_text_kwargs), "send_text", dest_chat_id=tcid)
            
            if not sent:
                print(f"[PROC-TEXT] src_msg_id={m.id} — BOTH send paths failed, sending as plain text")
                _plain_kwargs = dict(chat_id=tcid, text=final_text or raw_text or md_text or ' ')
                if topic_id:
                    _plain_kwargs['message_thread_id'] = topic_id
                if _text_reply_to:
                    _plain_kwargs['reply_to_message_id'] = _text_reply_to
                try:
                    sent = await c.send_message(**_plain_kwargs)
                except Exception as _plain_err:
                    print(f"[PROC-TEXT] src_msg_id={m.id} — Even plain text send failed: {_plain_err}")
                    return ('Failed (text send).', None, False, False)
            
            sent_msg_id = sent.id if sent else None
            if not sent_msg_id:
                print(f"[PROC-TEXT] src_msg_id={m.id} — sent.id is None!")
                return ('Failed (no sent_id).', None, False, False)
            
            # Log reply_to confirmation
            if _text_reply_to:
                print(f"[REPLY-TO] ✅ src_msg_id={m.id} sent as reply to dest_msg_id={_text_reply_to}")
            
            # ── 📖 View Explanation: when explanation arrives, add on the POLL (B') ──
            # Explanation C replies to poll B. reply_to_dest_for_button = B' (poll in dest).
            # Add 📖 View Explanation on B' → C' (this explanation).
            if _reply_to_dest_for_button and sent_msg_id:
                try:
                    _reply_url = _build_telegram_link(tcid, sent_msg_id)
                    if _reply_url:
                        _edlog(f"[PROC-TEXT] src_msg_id={m.id} — Adding 📖 on poll {_reply_to_dest_for_button} → {_reply_url}")
                        await _add_explanation_button(c, tcid, _reply_to_dest_for_button, _reply_url)
                        # 🔙 Back to Question: add on the explanation (C') → links back to poll (B')
                        _poll_url = _build_telegram_link(tcid, _reply_to_dest_for_button)
                        if _poll_url:
                            await _add_inline_button(c, tcid, sent_msg_id, "🔙 Back to Question", _poll_url, log_prefix="BACK-BTN")
                except Exception as _rb_e:
                    _edlog(f"[PROC-TEXT] src_msg_id={m.id} — 📖/🔙 button error: {_rb_e}")
            
            return ('Sent.', sent_msg_id, False, had_unresolved_links)
        
        # Handle messages that have neither media nor text (e.g. service messages, etc.)
        # Also handles forwarded-only messages with no text content
        # NOTE: We do NOT attempt forward_messages() here — forwarding doesn't work
        # on restricted channels (the bot's entire purpose is to save restricted content).
        # Instead, we send a text description of what was forwarded.
        elif m.forward_origin and not m.text and not m.media:
            fwd = m.forward_origin
            fwd_info = 'Forwarded message (no viewable content)'
            try:
                if hasattr(fwd, 'sender_user') and fwd.sender_user:
                    fwd_info = f'Forwarded from {fwd.sender_user.first_name or "Unknown"} (no viewable content)'
                elif hasattr(fwd, 'chat') and fwd.chat:
                    fwd_info = f'Forwarded from {fwd.chat.title or "Unknown"} (no viewable content)'
            except Exception:
                pass
            # Send as text description — forwarding won't work on restricted channels
            _fwd_text_kwargs = dict(chat_id=tcid, text=fwd_info)
            if topic_id:
                _fwd_text_kwargs['message_thread_id'] = topic_id
            if reply_to_destination_id:
                _fwd_text_kwargs['reply_to_message_id'] = reply_to_destination_id
            try:
                sent = await c.send_message(**_fwd_text_kwargs)
                if sent:
                    print(f"[PROC-FWD] src_msg_id={m.id} — sent forward info as text, dst={sent.id}")
                    return (fwd_info, sent.id, False, False)
            except Exception as _fwd_e:
                print(f"[PROC-FWD] src_msg_id={m.id} — send text failed: {_fwd_e}")
            return (fwd_info, None, False, False)
        
        # Service messages, empty messages, or unhandled content — skip
        _skip_reason = 'no viewable content'
        _is_service = getattr(m, 'service', False)
        _is_empty = getattr(m, 'empty', False)
        if _is_service:
            _skip_reason = f'service message ({getattr(m, "service", "unknown")})'
        elif _is_empty:
            _skip_reason = 'empty message'
        elif not m.text and not m.media and not m.poll:
            _skip_reason = f'no text/media/poll (has attrs: {[a for a in dir(m) if not a.startswith("_") and getattr(m, a, None)][:5]})'
        print(f"[PROC-SKIP] src_msg_id={m.id} — Skipped: {_skip_reason}")
        return ('Skipped (no viewable content).', None, False, False)
    except asyncio.CancelledError:
        # CRITICAL: Re-raise CancelledError so /stop and /autooff actually work.
        # On Python 3.8, CancelledError is a subclass of Exception, so the
        # "except Exception" block below would catch and swallow it, making
        # task.cancel() ineffective. Always re-raise CancelledError.
        raise
    except FloodWait as e:
        # CRITICAL: Re-raise FloodWait so the batch loop stops immediately.
        # If we swallow it here, the batch continues sending messages that
        # will ALL fail, making the FloodWait ban longer and longer.
        wait_secs = e.value if hasattr(e, 'value') else 30
        print(f"[FLOOD] process_msg: FloodWait {_format_duration(wait_secs)} — RE-RAISING (batch will stop)")
        raise
    except Exception as e:
        err_str = str(e)
        if 'CHANNEL_INVALID' in err_str or 'CHANNEL_PRIVATE' in err_str:
            print(f'[DEST-ERROR] Destination channel {tcid} is INVALID — bot may not be admin or channel may be deleted: {e}')
            return (False, None, False, False)
        return (f'Error: {str(e)[:200]}', None, False, False)
        
# ═══════════════════════════════════════════════════════════════
# TEST FEATURE: STREAMING BATCH — uses /fetch map to process
# messages one-by-one instead of pre-fetching all into memory.
# Saves 50-100 MB RAM per batch.
# ═══════════════════════════════════════════════════════════════


async def _resume_batch(uid, i, s, n, lt, user_chat_id):
    """Resume entry point called by FloodWaitScheduler after FloodWait clears.

    Re-enters the batch processing. MongoDB last_uploaded_source_id ensures
    already-uploaded messages are automatically skipped — zero data loss.

    The function re-acquires client connections (they may have gone stale
    during the FloodWait sleep period) and re-enters _batch_streaming()
    or the original loop depending on whether a fetch_map is available.

    Args:
        uid: User ID
        i: Source channel identifier
        s: Start message ID (original start, not adjusted — resume detection handles skip)
        n: Total number of messages to process
        lt: Link type ('public' or 'private')
        user_chat_id: User's chat ID for sending status messages
    """
    print(f"[SCHEDULER-RESUME] uid={uid} — Re-entering batch for source={i}, start={s}, count={n}")

    # Re-acquire bot client
    ubot = await get_ubot(uid)
    if not ubot:
        ubot = X

    # Re-acquire user client
    try:
        uc = await asyncio.wait_for(get_uclient(uid), timeout=90)
    except asyncio.TimeoutError:
        print(f"[SCHEDULER-RESUME] uid={uid} — User client setup timed out on resume")
        try:
            await X.send_message(user_chat_id, "❌ Could not reconnect user client on resume. Use /batch to restart manually.")
        except Exception:
            pass
        Z.pop(uid, None)
        return

    if not uc:
        uc = get_Y()
        if not uc:
            print(f"[SCHEDULER-RESUME] uid={uid} — No user client available on resume")
            try:
                await X.send_message(user_chat_id, "❌ No user client available on resume. Use /login first, then /batch.")
            except Exception:
                pass
            Z.pop(uid, None)
            return

    # Check if user stopped during FloodWait
    if should_cancel(uid):
        print(f"[SCHEDULER-RESUME] uid={uid} — Cancel was requested during FloodWait, not resuming")
        await remove_active_batch(uid)
        batch_tasks.pop(uid, None)
        Z.pop(uid, None)
        return

    # Build a fake progress message (the old one may be stale after long FloodWait)
    pt = None
    try:
        pt = await X.send_message(user_chat_id, "⏳ Resuming batch...")
    except Exception:
        pass

    # Check for fetch map
    fetch_map = None
    try:
        from plugins.fetch import get_fetch_map
        start_msg_id_check = int(s)
        end_msg_id_check = start_msg_id_check + n - 1
        fetch_map = await get_fetch_map(uid, i, start_msg_id_check, end_msg_id_check)
    except Exception:
        fetch_map = None


    # Resolve peers on resume (dyno restart clears peer cache)
    try:
        await resolve_peers_at_startup(uc, ubot, i)
    except Exception as e:
        print(f"[SCHEDULER-RESUME] uid={uid} — Failed to resolve peers on resume: {e}")
        # Non-fatal: the batch may still work if the peer is cached from earlier operations

    # Re-enter the batch — resume detection in the batch code will skip
    # already-uploaded messages via MongoDB last_uploaded_source_id
    if fetch_map:
        print(f"[SCHEDULER-RESUME] uid={uid} — Resuming in streaming mode with fetch_map")
        # Build a minimal message-like object for _batch_streaming
        # It only needs m.chat.id for progress updates
        class _FakeMsg:
            def __init__(self, chat_id):
                self.chat = type('obj', (object,), {'id': chat_id})()
        fake_m = _FakeMsg(user_chat_id)

        if not pt:
            # Create progress message if we couldn't earlier
            try:
                pt = await X.send_message(user_chat_id, "⏳ Resuming batch (streaming)...")
            except Exception:
                # Can't even send a message — create a dummy pt
                class _DummyPT:
                    id = 0
                pt = _DummyPT()

        await _batch_streaming(X, fake_m, uid, i, s, n, lt, ubot, uc, pt, fetch_map)
    else:
        print(f"[SCHEDULER-RESUME] uid={uid} — Resuming in original mode (no fetch_map)")
        # For original mode, we need to re-enter the full batch setup
        # which includes pre-fetching. We'll call a simplified version.
        await _resume_batch_original(uid, i, s, n, lt, ubot, uc, user_chat_id)


async def _resume_batch_original(uid, i, s, n, lt, ubot, uc, user_chat_id):
    """Resume entry point for original (pre-fetch) mode batches.

    Re-fetches messages from where we left off and processes them.
    MongoDB resume detection handles skipping already-uploaded messages.
    """
    print(f"[SCHEDULER-RESUME] uid={uid} — Resuming original mode batch")

    start_msg_id = int(s)

    # Check if user stopped during FloodWait
    if should_cancel(uid):
        print(f"[SCHEDULER-RESUME] uid={uid} — Cancel requested, not resuming")
        await remove_active_batch(uid)
        batch_tasks.pop(uid, None)
        Z.pop(uid, None)
        return

    # Pre-fetch messages from start (resume detection will skip done ones)
    pt = None
    try:
        pt = await X.send_message(user_chat_id, "⏳ Resuming batch — re-fetching messages...")
    except Exception:
        class _DummyPT:
            id = 0
        pt = _DummyPT()

    message_ids = list(range(start_msg_id, start_msg_id + n))
    messages_data = []
    fetch_errors = 0

    # Determine which client can access the chat
    fetch_client = None
    resolved_fetch_chat = None

    if lt == 'public':
        try:
            resolved_fetch_chat = await resolve_chat(ubot, i)
            test_msg = await ubot.get_messages(resolved_fetch_chat, start_msg_id)
            if test_msg and not getattr(test_msg, 'empty', False):
                fetch_client = ubot
                emp[i] = False
        except Exception:
            pass

        if not fetch_client and uc:
            try:
                resolved_fetch_chat = await resolve_chat(uc, i)
                test_msg = await uc.get_messages(resolved_fetch_chat, start_msg_id)
                if test_msg and not getattr(test_msg, 'empty', False):
                    fetch_client = uc
            except Exception:
                pass

    elif lt == 'private' and uc:
        chat_id_int = int(i) if isinstance(i, str) and i.lstrip('-').isdigit() else i
        try:
            await uc.resolve_peer(chat_id_int)
            test_msg = await uc.get_messages(chat_id_int, start_msg_id)
            if test_msg and not getattr(test_msg, 'empty', False):
                fetch_client = uc
                resolved_fetch_chat = chat_id_int
        except Exception:
            pass

    # Fetch in chunks
    if fetch_client and resolved_fetch_chat is not None:
        for chunk_start in range(0, len(message_ids), 100):
            chunk_ids = message_ids[chunk_start:chunk_start + 100]
            try:
                results = await fetch_client.get_messages(resolved_fetch_chat, chunk_ids)
                if not isinstance(results, list):
                    results = [results]
                for msg in results:
                    if msg and not getattr(msg, 'empty', False):
                        messages_data.append((msg.id, msg))
                    else:
                        fetch_errors += 1
            except Exception as e:
                print(f'[SCHEDULER-RESUME] Chunk fetch error: {e}')
                # Check for fatal auth key error
                if _is_auth_key_error(e):
                    print(f"[SCHEDULER-RESUME] ⚠️ FATAL: AUTH_KEY_UNREGISTERED — session is dead!")
                    try:
                        await X.send_message(user_chat_id,
                            '🔴 **Session Expired**\n\n'
                            'Your Telegram session has been revoked.\n'
                            'Use `/logout` then `/login` to create a new session, then restart the batch.'
                        )
                    except Exception:
                        pass
                    Z.pop(uid, None)
                    return
                for mid in chunk_ids:
                    try:
                        msg = await get_msg(ubot, uc, i, mid, lt)
                        if msg:
                            messages_data.append((mid, msg))
                        else:
                            fetch_errors += 1
                    except AuthKeyUnregisteredError as ake:
                        print(f"[SCHEDULER-RESUME] ⚠️ FATAL: AUTH_KEY_UNREGISTERED in get_msg — stopping!")
                        try:
                            await X.send_message(user_chat_id,
                                '🔴 **Session Expired**\n\n'
                                'Your Telegram session has been revoked.\n'
                                'Use `/logout` then `/login` to create a new session, then restart the batch.'
                            )
                        except Exception:
                            pass
                        Z.pop(uid, None)
                        return
                    except Exception:
                        fetch_errors += 1
    else:
        # Fallback: one-by-one
        for j in range(n):
            mid = start_msg_id + j
            try:
                msg = await get_msg(ubot, uc, i, mid, lt)
                if msg:
                    messages_data.append((mid, msg))
                else:
                    fetch_errors += 1
            except AuthKeyUnregisteredError:
                print(f"[SCHEDULER-RESUME] ⚠️ FATAL: AUTH_KEY_UNREGISTERED in pre-fetch — stopping!")
                try:
                    await X.send_message(user_chat_id,
                        '🔴 **Session Expired**\n\n'
                        'Your Telegram session has been revoked.\n'
                        'Use `/logout` then `/login` to create a new session, then restart the batch.'
                    )
                except Exception:
                    pass
                Z.pop(uid, None)
                return
            except Exception:
                fetch_errors += 1

    if not messages_data:
        try:
            await X.send_message(user_chat_id, "❌ Could not re-fetch any messages on resume. Use /batch to restart.")
        except Exception:
            pass
        Z.pop(uid, None)
        return

    # Load upload map from MongoDB
    msg_id_map, last_uploaded_id, stored_dest_channel = await load_upload_map(uid, str(i))
    
    # IMPORTANT: After /clearbatch, last_uploaded_id is reset to 0 but old mappings
    # may still be in MongoDB. If last_uploaded_id == 0, this is a FRESH START —
    # don't use old mappings for skip detection, otherwise the batch skips all
    # previously-uploaded messages and success stays at 0.
    if last_uploaded_id == 0 and msg_id_map:
        print(f"[BATCH] Fresh start detected (last_uploaded_id=0) — clearing {len(msg_id_map)} old mappings from skip-detection map")
        msg_id_map = {}
    
    initial_msg_id_keys = set(msg_id_map.keys())  # Track pre-existing mappings to avoid double-counting on flush

    # Resolve destination channel
    dest_channel_id_int = None
    dest_channel_username = None
    try:
        cfg_chat = await get_user_data_key(str(user_chat_id), 'chat_id', None)
        if cfg_chat:
            if '/' in cfg_chat:
                dest_channel_id_int = int(cfg_chat.split('/')[0])
            else:
                dest_channel_id_int = int(cfg_chat)
        else:
            # No configured chat_id — fall back only if it's a channel (negative ID)
            _fallback_id = int(str(user_chat_id)) if str(user_chat_id).lstrip('-').isdigit() else None
            if _fallback_id and _fallback_id < 0:
                dest_channel_id_int = _fallback_id
            else:
                dest_channel_id_int = None
    except Exception:
        pass

    if dest_channel_id_int:
        try:
            dest_chat = await ubot.get_chat(dest_channel_id_int)
            dest_channel_username = getattr(dest_chat, 'username', None)
        except Exception:
            try:
                if uc:
                    dest_chat = await uc.get_chat(dest_channel_id_int)
                    dest_channel_username = getattr(dest_chat, 'username', None)
            except Exception:
                dest_channel_username = None

    # Pre-flight validation: Bot MUST be able to resolve destination channel
    if dest_channel_id_int:
        _dest_ok = False
        for _client_label, _client in [("ubot", ubot), ("user_client", uc)]:
            if not _client:
                continue
            try:
                await _client.resolve_peer(dest_channel_id_int)
                print(f"[DEST-VALIDATE] {_client_label}: dest channel {dest_channel_id_int} resolved OK")
                _dest_ok = True
                break
            except Exception as e:
                print(f"[DEST-VALIDATE] {_client_label}: dest channel {dest_channel_id_int} FAILED: {e}")
        
        if not _dest_ok:
            err_msg = (f"❌ Destination channel `{dest_channel_id_int}` is INVALID or bot lacks access!\n\n"
                       f"Please check:\n"
                       f"• The channel ID is correct (should start with -100)\n"
                       f"• The bot is added as admin in the destination channel\n"
                       f"• The channel hasn't been deleted\n\n"
                       f"Use /setchat to configure the correct destination channel.")
            await safe_edit(pt, err_msg)
            Z.pop(uid, None)
            return
    else:
        print(f"[DEST-VALIDATE] No dest_channel_id_int configured — user chat will be used as destination")

    # Resolve source channel username AND numeric ID for link rewriting
    # IMPORTANT: i from E() is a string like '-1001234567890' for private channels
    # or 'channelname' for public channels. We need BOTH the username and the
    # numeric ID to match ALL link formats in message text (private + public).
    source_channel_username = None
    source_channel_id_int = None  # Numeric ID for DUAL-FORMAT link matching
    try:
        src_chat_info = None
        # Convert string channel ID to int for Pyrogram API calls
        resolved_i = i
        try:
            if isinstance(i, str) and i.lstrip('-').isdigit():
                resolved_i = int(i)
                source_channel_id_int = resolved_i  # Already have numeric ID
        except Exception:
            pass
        if uc:
            try:
                src_chat_info = await uc.get_chat(resolved_i)
            except Exception:
                pass
        if not src_chat_info and ubot:
            try:
                src_chat_info = await ubot.get_chat(resolved_i)
            except Exception:
                pass
        if src_chat_info:
            source_channel_username = getattr(src_chat_info, 'username', None)
            # Get the numeric ID from chat info (important for public channels)
            if not source_channel_id_int:
                source_channel_id_int = getattr(src_chat_info, 'id', None)
            if source_channel_username:
                print(f"[LINK-REWRITE] Source channel username resolved: @{source_channel_username}")
            if source_channel_id_int:
                print(f"[LINK-REWRITE] Source channel numeric ID resolved: {source_channel_id_int}")
    except Exception:
        source_channel_username = None

    # ═══════════════════════════════════════════════════════════════
    # MULTI-SOURCE: Build list of ALL source channels for cross-channel
    # link rewriting. A message from channel A can have links to B, C.
    # ═══════════════════════════════════════════════════════════════
    _multi_src_channels = None
    _combined_msg_id_map = None
    try:
        _resolve_client = uc or ubot
        _multi_src_channels, _combined_msg_id_map = await build_multi_source_channels(
            uid, i,
            primary_username=source_channel_username,
            primary_numeric_id=source_channel_id_int,
            client=_resolve_client,
        )
        if _multi_src_channels:
            _ch_count = len(_multi_src_channels)
            if _ch_count > 1:
                print(f"[MULTI-SRC] Cross-channel rewriting enabled: {_ch_count} source channels")
            else:
                print(f"[MULTI-SRC] Single source channel — multi_source metadata still passed for URL pattern building")
        # IMPORTANT: Do NOT set _multi_src_channels=None when only 1 channel!
        # Even with 1 channel, the multi_source param carries username/numeric_id
        # metadata needed for URL pattern matching, and ensures the combined
        # msg_id_map from build_multi_source_channels() is used by
        # resolve_pending_link_rewrites.
    except Exception as e:
        print(f"[MULTI-SRC] Failed to build multi-source channels (non-fatal): {e}")
        _multi_src_channels = None

    # ── SIMPLEREWITER: Create and load all mappings ──
    _rewriter = None
    try:
        from plugins.simple_rewriter import SimpleRewriter as _SR
        _db = _upload_db
        if _db is not None and dest_channel_id_int:
            _rewriter = _SR(
                uid=uid,
                source_channel=str(i),
                dst_chat_id=dest_channel_id_int,
                db=_db,
                bot_client=X,
                ubot=ubot,
                source_channel_username=source_channel_username,
                dst_channel_username=dest_channel_username,
                dst_channel_id=dest_channel_id_int,
            )
            # Add multi-source channels
            if _multi_src_channels:
                for ch_info in _multi_src_channels:
                    ch = ch_info.get("channel", "")
                    ch_username = ch_info.get("username")
                    ch_numeric_id = ch_info.get("numeric_id")
                    if ch and ch != str(i):
                        _rewriter.add_source_channel(ch, ch_username, ch_numeric_id)
            await _rewriter.load()
            # Merge loaded mappings into msg_id_map (rewriter has more mappings)
            for _k, _v in _rewriter.map.items():
                if _k not in msg_id_map:
                    msg_id_map[_k] = _v
            print(f"[REWRITER] SimpleRewriter loaded {len(_rewriter.map)} mappings, {_rewriter._src_patterns.__len__()} source patterns")
        else:
            print(f"[REWRITER] Skipped — no MongoDB or no dest_channel_id_int")
    except Exception as _sr_err:
        print(f"[REWRITER] SimpleRewriter init failed (non-fatal): {_sr_err}")
        _rewriter = None

    # ═══════════════════════════════════════════════════════════════
    # RESOLVE PENDING LINK REWRITES from previous stopped batches
    # Runs BEFORE any new messages are processed — never skipped.
    # Finds all messages with unresolved links in MongoDB, rewrites them
    # with the complete msg_id_map, marks resolved. Crash-safe.
    # ═══════════════════════════════════════════════════════════════
    try:
        await resolve_pending_link_rewrites(
            bot_client=X, ubot=ubot, source_channel=i,
            dest_channel_id_int=dest_channel_id_int,
            dest_channel_username=dest_channel_username,
            source_channel_username=source_channel_username,
            uid=uid,
            source_channel_id=source_channel_id_int,
            multi_source_channels=_multi_src_channels,
            combined_msg_id_map=_combined_msg_id_map,
        )
    except Exception as e:
        print(f"[LINK-REWRITE-RESUME] Pre-batch resolve failed (non-fatal): {e}")

    # No longer needed — replaced by MongoDB-backed unresolved_links tracking
    messages_needing_link_update = []
    failed_links = []
    batch_start_time = time.time()
    last_progress_edit = 0
    success = 0

    # 📌 PIN MAP: Pre-scan ALL pinned messages BEFORE loop (1-2 API calls total)
    _pin_map = {}
    try:
        _pin_map = await startup_pin(user_client=uc, src_chat_id=i, fetch_map={})
        print(f"[PIN] Loaded pin_map with {len(_pin_map)} entries")
    except Exception as e:
        print(f"[PIN] Could not load pin_map: {e}")

    # Pre-load explanations into memory cache
    try:
        from plugins.explanation_listener import add_monitored_channel
        await add_monitored_channel(i, uid, client=uc or ubot)
    except Exception as e:
        print(f"[EXPLANATION] Could not pre-load explanations: {e}")

    # Ensure active batch entry exists
    if not is_user_active(uid):
        await add_active_batch(uid, {
            "total": n,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id if pt else 0
        })

    # Register as asyncio.Task
    current_task = asyncio.current_task()
    if current_task:
        batch_tasks[uid] = current_task

    # Register with FloodWaitScheduler for auto-resume on FloodWait
    from scheduler import scheduler
    scheduler.register(uid, resume_fn=_resume_batch,
        resume_kwargs=dict(uid=uid, i=i, s=s, n=n, lt=lt, user_chat_id=user_chat_id))

    # ═══════════════════════════════════════════════════════════════
    # PASS 1: DISABLED — pre-upload scan skipped to avoid slow full-channel queries.
    # Question images outside batch range will still be handled by the
    # download→re-upload approach inside process_msg() during Pass 2.
    # ═══════════════════════════════════════════════════════════════
    # PERF CACHE: Resolve per-message data ONCE before the loop starts.
    # This eliminates ~3-5 MongoDB/API round-trips per message that
    # were causing 0.02 msgs/min (50s/msg) instead of 30 msgs/min.
    # ═══════════════════════════════════════════════════════════════
    _cached_tcid = None
    _cached_topic_id = None
    _cached_rtmid = None
    _cached_watermark = None
    _cached_caption = None
    _cached_source_name = None

    try:
        cfg_chat = await get_user_data_key(str(user_chat_id), 'chat_id', None)
        tcid_cache = int(user_chat_id)
        topic_id_cache = None
        rtmid_cache = None
        if cfg_chat:
            if '/' in cfg_chat:
                parts = cfg_chat.split('/', 1)
                tcid_cache = int(parts[0])
                topic_id_cache = int(parts[1]) if len(parts) > 1 else None
                # IMPORTANT: Do NOT set rtmid_cache = topic_id_cache!
                # rtmid is used for reply_to_message_id — setting it to topic_id
                # makes ALL messages appear as "replies" to the topic root message.
                # Only set reply_to_message_id when there's an actual reply target
                # (handled by reply_to_destination_id in process_msg).
                rtmid_cache = None  # reply_to is set per-message via reply_to_destination_id
            else:
                tcid_cache = int(cfg_chat)
        _cached_tcid = tcid_cache
        _cached_topic_id = topic_id_cache
        _cached_rtmid = rtmid_cache
        # Resolve destination peer ONCE (not per message)
        try:
            await X.resolve_peer(_cached_tcid)
        except Exception as _rp_err:
            print(f"[DEST-RESOLVE] Failed to resolve dest peer {_cached_tcid}: {_rp_err}")
        print(f"[PERF-CACHE] Cached tcid={_cached_tcid}, topic_id={_cached_topic_id}")
    except Exception as _cache_err:
        print(f"[PERF-CACHE] tcid cache failed: {_cache_err}")

    try:
        _cached_watermark = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT)
        print(f"[PERF-CACHE] Cached watermark text")
    except Exception:
        _cached_watermark = _DEFAULT_WATERMARK_TEXT

    try:
        _cached_caption = await get_user_data_key(uid, 'caption_text', '')
        print(f"[PERF-CACHE] Cached caption text")
    except Exception:
        _cached_caption = ''

    # Cache source channel name (used by text messages) — resolve ONCE
    try:
        _resolve_client = uc or ubot or X
        _src_chat_info = None
        _resolved_i = i
        try:
            if isinstance(i, str) and i.lstrip('-').isdigit():
                _resolved_i = int(i)
        except Exception:
            pass
        try:
            _src_chat_info = await _resolve_client.get_chat(_resolved_i)
        except Exception:
            pass
        if _src_chat_info and getattr(_src_chat_info, 'title', None):
            _cached_source_name = _src_chat_info.title
            print(f"[PERF-CACHE] Cached source_name: {_cached_source_name}")
    except Exception:
        _cached_source_name = ''

    # ═══════════════════════════════════════════════════════════════
    dep_result = None
    print("[PASS-1] Pre-upload scan DISABLED — skipping")

    # ── Method 1 batch-side dependency recording ──
    # If /fetch didn't build the dependency index, the batch records deps
    # on-the-fly as it encounters polls. This makes the index self-populating
    # so the NEXT batch run can use the dependency index for instant Pass 1.
    if dep_result is None:
        _should_record_deps[uid] = True
        _pending_dep_batch[uid] = []
        print(f"[BATCH-DEP] /fetch didn't build deps — batch will record dependencies for uid={uid}")
    else:
        _should_record_deps[uid] = False
        print(f"[BATCH-DEP] /fetch already built deps — batch will NOT record dependencies for uid={uid}")

    j = 0
    try:
        for j in range(n):
            if should_cancel(uid):
                break

            await update_batch_progress(uid, j, success)

            mid = start_msg_id + j

            # Skip already-uploaded messages (resume detection)
            if not Z.get(uid, {}).get('force_fresh_start') and mid in msg_id_map:
                # Already uploaded — skip (log occasionally)
                if j % 100 == 0:
                    print(f"[BATCH-SKIP] msg_id={mid} — already uploaded (dest_id={msg_id_map[mid]}) — resuming from previous batch")
                continue

            # Find message from pre-fetched data
            src_msg = None
            for item in messages_data:
                if item[0] == mid:
                    src_msg = item[1]
                    break

            if not src_msg:
                # Message was not in pre-fetched data — try fetching on-demand
                print(f"[RESUME] Pre-fetch missed msg {mid} — fetching on-demand...")
                for retry_attempt in range(3):
                    try:
                        # Rate-limit source channel reads
                        _src_chat_int = int(i) if isinstance(i, str) and i.lstrip('-').isdigit() else hash(i)
                        await _rate_limiter.acquire(_src_chat_int)
                        src_msg = await get_msg(ubot, uc, i, mid, lt)
                        if src_msg:
                            break
                    except AuthKeyUnregisteredError:
                        print(f"[RESUME] ⚠️ FATAL: AUTH_KEY_UNREGISTERED — session is dead! Stopping batch.")
                        try:
                            await pt.edit(
                                '🔴 **Session Expired — Batch Stopped**\n\n'
                                'Your Telegram session has been revoked by Telegram.\n'
                                'Use `/logout` then `/login` to create a new session, then restart the batch.'
                            )
                        except Exception:
                            pass
                        await remove_active_batch(uid)
                        try:
                            from scheduler import scheduler
                            scheduler.unregister(uid)
                        except Exception:
                            pass
                        return
                    except Exception as e:
                        print(f"[RESUME] On-demand fetch attempt {retry_attempt+1}/3 failed for msg {mid}: {e}")
                        await asyncio.sleep(3)
                
                if not src_msg:
                    print(f"[BATCH-DEBUG] msg_id={mid} — get_msg returned None (not found or empty)")
                    if lt == 'private':
                        failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                    else:
                        failed_link = f"https://t.me/{i}/{mid}"
                    failed_links.append(f"{failed_link} — skipped (not fetched after 3 retries)")
                    continue
            
            # Debug: Log every fetched message type for visibility
            _msg_type = 'unknown'
            if src_msg.poll:
                _msg_type = 'poll'
            elif src_msg.photo:
                _msg_type = 'photo'
            elif src_msg.video:
                _msg_type = 'video'
            elif src_msg.document:
                _msg_type = 'document'
            elif src_msg.audio:
                _msg_type = 'audio'
            elif src_msg.animation:
                _msg_type = 'animation'
            elif src_msg.voice:
                _msg_type = 'voice'
            elif src_msg.video_note:
                _msg_type = 'video_note'
            elif src_msg.sticker:
                _msg_type = 'sticker'
            elif src_msg.text:
                _msg_type = 'text'
            elif src_msg.media:
                _msg_type = f'media({type(src_msg.media).__name__})'
            elif src_msg.service:
                _msg_type = 'service'
            elif src_msg.empty:
                _msg_type = 'empty'
            elif src_msg.forward_origin:
                _msg_type = 'forwarded(no_content)'
            _has_text = bool(src_msg.text)
            _has_caption = bool(getattr(src_msg, 'caption', None))
            _has_media = bool(src_msg.media)
            _src_reply_id = _get_reply_to_id(src_msg)
            print(f"[BATCH-DEBUG] msg_id={mid} type={_msg_type} text={_has_text} caption={_has_caption} media={_has_media} reply_to_msg_id={src_msg.reply_to_message_id} reply_to_robust={_src_reply_id}")

            # Preserve reply chain — use robust _get_reply_to_id() to handle
            # Pyrofork's different reply attribute locations
            reply_to_dest_id = None
            if _src_reply_id:
                reply_to_dest_id = msg_id_map.get(_src_reply_id)

            try:
                res, dest_id, _, had_unresolved = await process_msg(
                    X, uc, src_msg, str(user_chat_id), lt, uid, i,
                    reply_to_destination_id=reply_to_dest_id,
                    link_rewrite_map=msg_id_map,
                    dest_channel_id=dest_channel_id_int,
                    dest_channel_username=dest_channel_username,
                    source_channel_username=source_channel_username,
                    source_channel_id=source_channel_id_int,
                    multi_source_channels=_multi_src_channels,
                    rewriter=_rewriter,
                    _cached_tcid=_cached_tcid,
                    _cached_topic_id=_cached_topic_id,
                    _cached_rtmid=_cached_rtmid,
                    _cached_watermark=_cached_watermark,
                    _cached_caption=_cached_caption,
                    _cached_source_name=_cached_source_name,
                    _skip_verify=True,
                    _skip_explanation_scan=True
                )
                # Debug: Log process_msg result for non-poll messages
                if not src_msg.poll:
                    print(f"[BATCH-DEBUG] msg_id={mid} type={_msg_type} result='{res}' dest_id={dest_id}")
                if dest_id:
                    msg_id_map[mid] = dest_id
                    # ── SIMPLEREWITER: Record mapping ──
                    if _rewriter is not None:
                        try:
                            await _rewriter.record(mid, dest_id)
                        except Exception:
                            pass
                    # 🔑 FINGERPRINT: Store fingerprint for relink resolution (non-blocking)
                    # This enables the content-index strategy to find messages
                    # even when the direct mapping is missing.
                    try:
                        from plugins.relink import checkpoint_with_fingerprint
                        asyncio.create_task(checkpoint_with_fingerprint(uid, str(i), mid, dest_id, src_msg))
                    except Exception as _fp_err:
                        pass  # Never let fingerprint storage break mirroring
                    # 🗂️ SMART CACHE: Index source links for instant /relink queries
                    # Extract source links from ORIGINAL message (before rewriting)
                    # and store in mirrored_messages_index for surgical /relink
                    if dest_channel_id_int:
                        try:
                            _src_text = str(src_msg.text) if src_msg.text else (str(src_msg.caption) if src_msg.caption else '')
                            _src_ents = src_msg.entities if src_msg.entities else (src_msg.caption_entities if src_msg.caption_entities else None)
                            asyncio.create_task(cache_message_for_relink(
                                uid, str(i), dest_channel_id_int, dest_id, mid,
                                _src_text, _src_ents,
                                source_channel_username=source_channel_username,
                                source_channel_id=source_channel_id_int,
                                multi_source_channels=_multi_src_channels
                            ))
                        except Exception as _sc_err:
                            pass  # Never let Smart Cache break mirroring
                    if src_msg.reply_to_message_id and src_msg.reply_to_message_id not in msg_id_map:
                        if dest_channel_id_int:
                            asyncio.create_task(add_pending_reply(uid, str(i), dest_channel_id_int, dest_id, src_msg.reply_to_message_id))
                    if had_unresolved:
                        # Write to MongoDB (survives crashes/stops) instead of in-memory list
                        asyncio.create_task(mark_needs_link_update(uid, str(i), dest_channel_id_int, dest_id, mid))
                    # 📌 PIN: Non-blocking — fire and forget (pins are rare)
                    if dest_channel_id_int:
                        try:
                            asyncio.create_task(handle_pin_mirror(X, uc, i, mid, src_msg, dest_channel_id_int, dest_id, _pin_map))
                        except Exception as e:
                            print(f"[PIN] handle_pin_mirror failed for msg {mid}: {e}")
                    # ✅ VERIFY: DISABLED per-message — too slow (1 API call per msg).
                    # post_batch_verify() runs at batch end instead for bulk verification.
                    # if dest_channel_id_int:
                    #     try:
                    #         verified = await verify_upload(X, dest_channel_id_int, dest_id)
                    #     except Exception:
                    #         pass
                    # 🔗 AUTO-RELINK: Non-blocking — fire and forget via create_task
                    if dest_channel_id_int and dest_id:
                        try:
                            from plugins.relink import on_new_mirror_message
                            asyncio.create_task(on_new_mirror_message(uid, str(i), mid, dest_id, dest_channel_id_int))
                        except Exception as e:
                            pass  # Never let auto-relink break mirroring
                if 'Done' in res or 'Copied' in res or 'Sent' in res or 'Forwarded' in res or 'forwarded' in res:
                    success += 1
                else:
                    if lt == 'private':
                        failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                    else:
                        failed_link = f"https://t.me/{i}/{mid}"
                    failed_links.append(f"{failed_link} — {res}")
                
                # ── Check cancel flag after each message ──
                if should_cancel(uid):
                    print(f"[STOP] Cancel flag detected after msg {mid} — stopping resume batch")
                    raise asyncio.CancelledError()

            except FloodWait as e:
                # ── FloodWait: stop batch and notify user ──
                wait_secs = e.value if hasattr(e, 'value') else 30
                await _flood_wait_stop(uid, wait_secs, user_chat_id=user_chat_id)
                return

            except Exception as e:
                if lt == 'private':
                    failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                else:
                    failed_link = f"https://t.me/{i}/{mid}"
                failed_links.append(f"{failed_link} — Error: {str(e)[:80]}")

            # Update progress (throttled: every 50 msgs OR every 30s, prevents FloodWait)
            now = time.time()
            if (j + 1) % 50 == 0 or now - last_progress_edit >= 30 or j + 1 == n:
                elapsed = now - batch_start_time
                pct = min((j + 1) * 100 // n, 100)
                try:
                    await safe_edit(pt,
                        f'📦 **Batch Progress (Resumed)**\n\n'
                        f'{"🟢" * (pct // 10)}{"⚪" * (10 - pct // 10)}  **{pct}%**\n\n'
                        f'✅ Done: **{j+1}**/{n} | Success: **{success}**\n'
                        f'⏱️ Elapsed: {elapsed:.0f}s'
                    )
                    last_progress_edit = now
                except Exception:
                    pass

            await asyncio.sleep(BATCH_SEND_DELAY)
            
            # Cooldown pause every N messages to prevent sustained-rate FloodWait
            await _batch_cooldown_check(j, uid)
            
            # Post-sleep RAM reclaim: by now Pyrogram has released its internal session
            # buffers from the download/upload. malloc_trim can return these to the OS.
            _ram_reclaim()

        # ─── INCREMENTAL MAP SAVE for RESUME batch (every 100 msgs) ───
        if success > 0 and success % 100 == 0 and dest_channel_id_int and j + 1 < n:
            try:
                new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
                if new_mappings:
                    last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                    await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                    initial_msg_id_keys = set(msg_id_map.keys())
                    print(f"[BATCH-SAVE-RESUME] Incremental save at {success} msgs — {len(msg_id_map)} total mappings")
            except Exception as e:
                print(f"[BATCH-SAVE-RESUME] Incremental save failed: {e}")

        # Batch completion
        if j + 1 == n:
            # Single flush — write NEW mappings to MongoDB at once
            new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
            if new_mappings:
                try:
                    last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                    await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                    print(f"[BATCH-END] Flushed {len(new_mappings)} new mappings to MongoDB ({len(msg_id_map)} total)")
                except Exception as e:
                    print(f"[BATCH-END] Failed to flush mappings: {e}")
            
            # ── Flush any remaining batch-side dependency records ──
            try:
                await _flush_dep_batch(uid, force=True)
            except Exception as e:
                print(f"[BATCH-DEP] Final flush failed for uid={uid}: {e}")
            _clear_dep_recording_state(uid)
            
            last_msg_id = start_msg_id + n - 1
            if lt == 'private':
                channel_id_clean = str(i).replace('-100', '')
                last_link = f"https://t.me/c/{channel_id_clean}/{last_msg_id}"
            else:
                last_link = f"https://t.me/{i}/{last_msg_id}"
            
            # Mark batch complete in MongoDB
            try:
                await mark_batch_complete(uid, str(i))
            except Exception as e:
                print(f"[BATCH-STATE] Failed to mark batch complete: {e}")
            
            # Cancel heartbeat — batch is done
            try:
                _heartbeat_task.cancel()
            except Exception:
                pass
            
            # 📌 PIN SYNC: Ensure ALL source pins are pinned in dest
            # Use uc (user client) if available, else fall back to ubot
            # Bot doesn't need to be in source channel — user_client just needs
            # to be a member of the source channel to fetch pinned message IDs.
            _pin_client = uc or ubot
            if _pin_client and dest_channel_id_int and msg_id_map:
                try:
                    pin_result = await verify_and_sync_pins(
                        user_client=_pin_client, bot_client=X,
                        src_chat_id=i, dst_chat_id=dest_channel_id_int,
                        msg_id_map=msg_id_map,
                        uid=uid, source_channel=str(i),
                    )
                    print(f"[PIN-SYNC] Result: source_pins={pin_result.get('total_source_pins',0)} mapped={pin_result.get('mapped_pins',0)} newly_pinned={pin_result.get('newly_pinned',0)} failed={len(pin_result.get('failed_to_pin',[]))} not_in_map={len(pin_result.get('not_in_map',[]))}")
                    # If pins still failed after trying both user_client and bot_client,
                    # try with ubot as last resort (only if ubot wasn't already used as _pin_client)
                    if pin_result.get('failed_to_pin'):
                        # Try any client not already tried inside verify_and_sync_pins
                        # _pin_client and X were already tried; ubot is the remaining option
                        fallback_client = ubot if (ubot and ubot != _pin_client and ubot != X) else None
                        if fallback_client:
                            print(f"[PIN-SYNC] {len(pin_result['failed_to_pin'])} pins still failed — retrying with ubot...")
                            for src_id, reason in pin_result['failed_to_pin']:
                                dst_id = msg_id_map.get(src_id)
                                if dst_id:
                                    try:
                                        from plugins.pin_map import pin_in_destination
                                        await pin_in_destination(client=fallback_client, dst_chat_id=dest_channel_id_int, dst_msg_id=dst_id)
                                        print(f"[PIN-SYNC] ubot pinned src_msg={src_id} → dst_msg={dst_id}")
                                        # Also mark for link rewrite when ubot fallback pins
                                        try:
                                            await mark_needs_link_update(uid, str(i), dest_channel_id_int, dst_id, src_id)
                                        except Exception:
                                            pass
                                    except Exception as e2:
                                        print(f"[PIN-SYNC] ubot also failed to pin src_msg={src_id}: {e2}")
                        else:
                            print(f"[PIN-SYNC] {len(pin_result['failed_to_pin'])} pins failed and no additional client available for retry")
                except Exception as e:
                    print(f"[PIN-SYNC] verify_and_sync_pins failed: {e}")
            elif not _pin_client:
                print(f"[PIN-SYNC] SKIPPED — no user client available to fetch source pins")
            
            try:
                await X.send_message(
                    user_chat_id,
                    f'Batch Completed ✅ Success: {success}/{n}\n\n'
                    f'**Last message link:**\n`{last_link}`'
                )
            except Exception:
                pass

            if failed_links:
                try:
                    failed_file_path = f"failed_links_{uid}_{int(time.time())}.txt"
                    with open(failed_file_path, 'w', encoding='utf-8') as f:
                        f.write(f"Failed Links Report\n{'=' * 40}\n")
                        for idx, fl in enumerate(failed_links, 1):
                            f.write(f"{idx}. {fl}\n")
                    await X.send_document(user_chat_id, failed_file_path, caption=f'❌ Failed links ({len(failed_links)})')
                    os.remove(failed_file_path)
                except Exception:
                    pass
            
            # Post-batch link update pass — now uses MongoDB-backed tracking
            # Catches anything that became resolvable during this batch
            await resolve_pending_link_rewrites(
                bot_client=X, ubot=ubot, source_channel=i,
                dest_channel_id_int=dest_channel_id_int,
                dest_channel_username=dest_channel_username,
                source_channel_username=source_channel_username,
                uid=uid,
                source_channel_id=source_channel_id_int,
                multi_source_channels=_multi_src_channels,
                combined_msg_id_map=_combined_msg_id_map,
            )

    except asyncio.CancelledError:
        # Flush whatever new mappings we have before cleanup
        new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
        if new_mappings:
            try:
                last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                print(f"[STOP] Flushed {len(new_mappings)} new mappings on cancel (resume-original)")
            except Exception as e:
                print(f"[STOP] Failed to flush mappings on cancel: {e}")
        # Flush any remaining batch-side dependency records
        try:
            await _flush_dep_batch(uid, force=True)
        except Exception:
            pass
        _clear_dep_recording_state(uid)
        print(f"[SCHEDULER-RESUME] uid={uid} — Batch cancelled during resume")
    finally:
        # Cleanup: resolve pending replies
        try:
            resolved = await resolve_pending_replies(uid, str(i), msg_id_map)
        except Exception:
            pass
        await remove_active_batch(uid)
        batch_tasks.pop(uid, None)
        clear_cancel_flag(uid)
        Z.pop(uid, None)
        _rate_limiter.clear()
        _download_rate_limiter.clear()


async def _batch_streaming(c, m, uid, i, s, n, lt, ubot, uc, pt, fetch_map):
    """Process batch using pre-built fetch map — streams messages one-by-one.
    
    Instead of loading all Message objects into memory (50-100 MB burst),
    this fetches and processes one message at a time, using the fetch_map
    for reply chain info and media type hints.
    """
    log_ram("batch_streaming_start", extra_info={"uid": uid, "count": n})
    
    start_msg_id = int(s)
    success = 0
    failed_links = []
    batch_start_time = time.time()
    last_progress_edit = 0  # Throttle progress edits
    
    # Load upload map from MongoDB (survives restarts)
    msg_id_map, last_uploaded_id, stored_dest_channel = await load_upload_map(uid, str(i))
    
    # IMPORTANT: After /clearbatch, last_uploaded_id is reset to 0 but old mappings
    # may still be in MongoDB. If last_uploaded_id == 0, this is a FRESH START —
    # don't use old mappings for skip detection, otherwise the batch skips all
    # previously-uploaded messages and success stays at 0.
    if last_uploaded_id == 0 and msg_id_map:
        print(f"[BATCH] Fresh start detected (last_uploaded_id=0) — clearing {len(msg_id_map)} old mappings from skip-detection map")
        msg_id_map = {}
    
    # Count how many messages in this batch range will be skipped (including polls)
    if msg_id_map and fetch_map:
        _skip_count = 0
        _skip_polls = 0
        for _j in range(n):
            _mid = start_msg_id + _j
            if _mid in msg_id_map:
                _skip_count += 1
                _mi = fetch_map.get(str(_mid))
                if _mi and _mi.get("media_type") == "poll":
                    _skip_polls += 1
        if _skip_count > 0:
            print(f"[BATCH-RESUME] {_skip_count} messages already uploaded (including {_skip_polls} polls) — will be skipped. Use /clearbatch to re-upload everything.")
            if _skip_polls > 0:
                print(f"[BATCH-RESUME] ⚠️ {_skip_polls} polls will be SKIPPED because they're already in the upload map. If polls are missing from destination, use /clearbatch first.")
    
    initial_msg_id_keys = set(msg_id_map.keys())  # Track pre-existing mappings to avoid double-counting on flush
    # Save upload map after every message — guarantees zero data loss on crash.
    # With 15s delay between messages, the extra MongoDB write (~50 bytes) is negligible.
    
    # Resolve destination channel for link rewriting
    dest_channel_id_int = None
    dest_channel_username = None
    try:
        cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
        if cfg_chat:
            if '/' in cfg_chat:
                dest_channel_id_int = int(cfg_chat.split('/')[0])
            else:
                dest_channel_id_int = int(cfg_chat)
        else:
            # No configured chat_id — fall back only if it's a channel (negative ID)
            _fallback_id = int(str(m.chat.id)) if str(m.chat.id).lstrip('-').isdigit() else None
            if _fallback_id and _fallback_id < 0:
                dest_channel_id_int = _fallback_id
            else:
                dest_channel_id_int = None
    except Exception:
        pass
    
    if dest_channel_id_int:
        try:
            dest_chat = await ubot.get_chat(dest_channel_id_int)
            dest_channel_username = getattr(dest_chat, 'username', None)
        except Exception:
            # Bot can't access dest chat — try user client
            try:
                if uc:
                    dest_chat = await uc.get_chat(dest_channel_id_int)
                    dest_channel_username = getattr(dest_chat, 'username', None)
            except Exception:
                dest_channel_username = None
    
    # Pre-flight validation: Bot MUST be able to resolve destination channel
    if dest_channel_id_int:
        _dest_ok = False
        for _client_label, _client in [("ubot", ubot), ("user_client", uc)]:
            if not _client:
                continue
            try:
                await _client.resolve_peer(dest_channel_id_int)
                print(f"[DEST-VALIDATE] {_client_label}: dest channel {dest_channel_id_int} resolved OK")
                _dest_ok = True
                break
            except Exception as e:
                print(f"[DEST-VALIDATE] {_client_label}: dest channel {dest_channel_id_int} FAILED: {e}")
        
        if not _dest_ok:
            err_msg = (f"❌ Destination channel `{dest_channel_id_int}` is INVALID or bot lacks access!\n\n"
                       f"Please check:\n"
                       f"• The channel ID is correct (should start with -100)\n"
                       f"• The bot is added as admin in the destination channel\n"
                       f"• The channel hasn't been deleted\n\n"
                       f"Use /setchat to configure the correct destination channel.")
            await safe_edit(pt, err_msg)
            Z.pop(uid, None)
            return
    else:
        print(f"[DEST-VALIDATE] No dest_channel_id_int configured — user chat will be used as destination")
    
    # Resolve source channel username for link rewriting
    # IMPORTANT: i from E() is a string like '-1001234567890' for private channels
    # or 'channelname' for public channels. Pyrogram's get_chat() needs an integer
    # for private channels or a username string for public channels.
    source_channel_username = None
    source_channel_id_int = None  # Numeric ID for DUAL-FORMAT link matching
    try:
        src_chat_info = None
        # Convert string channel ID to int for Pyrogram API calls
        resolved_i = i
        try:
            if isinstance(i, str) and i.lstrip('-').isdigit():
                resolved_i = int(i)
                source_channel_id_int = resolved_i  # Already have numeric ID
        except Exception:
            pass
        if uc:
            try:
                src_chat_info = await uc.get_chat(resolved_i)
            except Exception:
                pass
        if not src_chat_info and ubot:
            try:
                src_chat_info = await ubot.get_chat(resolved_i)
            except Exception:
                pass
        if src_chat_info:
            source_channel_username = getattr(src_chat_info, 'username', None)
            # Get the numeric ID from chat info (important for public channels)
            if not source_channel_id_int:
                source_channel_id_int = getattr(src_chat_info, 'id', None)
            if source_channel_username:
                print(f"[LINK-REWRITE] Source channel username resolved: @{source_channel_username}")
            if source_channel_id_int:
                print(f"[LINK-REWRITE] Source channel numeric ID resolved: {source_channel_id_int}")
    except Exception:
        source_channel_username = None

    # ═══════════════════════════════════════════════════════════════
    # MULTI-SOURCE: Build list of ALL source channels for cross-channel
    # link rewriting. A message from channel A can have links to B, C.
    # ═══════════════════════════════════════════════════════════════
    _multi_src_channels = None
    _combined_msg_id_map = None
    try:
        _resolve_client = uc or ubot
        _multi_src_channels, _combined_msg_id_map = await build_multi_source_channels(
            uid, i,
            primary_username=source_channel_username,
            primary_numeric_id=source_channel_id_int,
            client=_resolve_client,
        )
        if _multi_src_channels:
            _ch_count = len(_multi_src_channels)
            if _ch_count > 1:
                print(f"[MULTI-SRC] Cross-channel rewriting enabled: {_ch_count} source channels")
            else:
                print(f"[MULTI-SRC] Single source channel — multi_source metadata still passed for URL pattern building")
        # IMPORTANT: Do NOT set _multi_src_channels=None when only 1 channel!
        # Even with 1 channel, the multi_source param carries username/numeric_id
        # metadata needed for URL pattern matching, and ensures the combined
        # msg_id_map from build_multi_source_channels() is used by
        # resolve_pending_link_rewrites.
    except Exception as e:
        print(f"[MULTI-SRC] Failed to build multi-source channels (non-fatal): {e}")
        _multi_src_channels = None

    # ── SIMPLEREWITER: Create and load all mappings ──
    _rewriter = None
    try:
        from plugins.simple_rewriter import SimpleRewriter as _SR
        _db = _upload_db
        if _db is not None and dest_channel_id_int:
            _rewriter = _SR(
                uid=uid,
                source_channel=str(i),
                dst_chat_id=dest_channel_id_int,
                db=_db,
                bot_client=X,
                ubot=ubot,
                source_channel_username=source_channel_username,
                dst_channel_username=dest_channel_username,
                dst_channel_id=dest_channel_id_int,
            )
            # Add multi-source channels
            if _multi_src_channels:
                for ch_info in _multi_src_channels:
                    ch = ch_info.get("channel", "")
                    ch_username = ch_info.get("username")
                    ch_numeric_id = ch_info.get("numeric_id")
                    if ch and ch != str(i):
                        _rewriter.add_source_channel(ch, ch_username, ch_numeric_id)
            await _rewriter.load()
            # Merge loaded mappings into msg_id_map (rewriter has more mappings)
            for _k, _v in _rewriter.map.items():
                if _k not in msg_id_map:
                    msg_id_map[_k] = _v
            print(f"[REWRITER] SimpleRewriter loaded {len(_rewriter.map)} mappings, {_rewriter._src_patterns.__len__()} source patterns")
        else:
            print(f"[REWRITER] Skipped — no MongoDB or no dest_channel_id_int")
    except Exception as _sr_err:
        print(f"[REWRITER] SimpleRewriter init failed (non-fatal): {_sr_err}")
        _rewriter = None

    # ─── PRE-RESOLVE SOURCE & DEST PEERS — prevents PEER_ID_INVALID ───
    # Pyrogram requires clients to "meet" a peer before using it.
    # Without this, resolve_peer() fails → get_messages() fails → PEER_ID_INVALID.
    # This MUST run before Pass 1 and any message processing.
    print(f"[PEER-RESOLVE] Pre-resolving source channel={resolved_i} and dest channel={dest_channel_id_int}...")
    for client_label, peer_client in [("user_client", uc), ("bot_client", ubot)]:
        if not peer_client:
            continue
        # Resolve source channel
        try:
            await peer_client.resolve_peer(resolved_i)
            print(f"[PEER-RESOLVE] {client_label}: source channel {resolved_i} resolved OK")
        except Exception as e:
            print(f"[PEER-RESOLVE] {client_label}: source channel {resolved_i} FAILED: {e}")
        # Resolve destination channel
        if dest_channel_id_int:
            try:
                await peer_client.resolve_peer(dest_channel_id_int)
                print(f"[PEER-RESOLVE] {client_label}: dest channel {dest_channel_id_int} resolved OK")
            except Exception as e:
                print(f"[PEER-RESOLVE] {client_label}: dest channel {dest_channel_id_int} FAILED: {e}")

    # ═══════════════════════════════════════════════════════════════
    # RESOLVE PENDING LINK REWRITES from previous stopped batches
    # Runs BEFORE any new messages are processed — never skipped.
    # ═══════════════════════════════════════════════════════════════
    try:
        await resolve_pending_link_rewrites(
            bot_client=X, ubot=ubot, source_channel=i,
            dest_channel_id_int=dest_channel_id_int,
            dest_channel_username=dest_channel_username,
            source_channel_username=source_channel_username,
            uid=uid,
            source_channel_id=source_channel_id_int,
            multi_source_channels=_multi_src_channels,
            combined_msg_id_map=_combined_msg_id_map,
        )
    except Exception as e:
        print(f"[LINK-REWRITE-RESUME] Pre-batch resolve failed (non-fatal): {e}")

    # No longer needed — replaced by MongoDB-backed unresolved_links tracking
    messages_needing_link_update = []
    
    # ─── MERGE FETCH_MAP DATA INTO CHANNEL_EXPLANATIONS ────────────
    # The fetch_map already knows which messages are polls and which
    # reply to polls. We merge this into the global CHANNEL_EXPLANATIONS
    # so it's available for instant lookup during batch processing.
    #
    # This is a SECONDARY data source — the primary source is the
    # persistent JSON state loaded at startup + real-time watchers.
    # The fetch_map fills gaps for channels that weren't monitored yet.
    try:
        from plugins.explanation_listener import add_known_poll, store_explanation, get_explanation_lookup, CHANNEL_EXPLANATIONS
        
        ch_str = str(i)
        ch_expl = get_explanation_lookup(ch_str)
        
        # Pass 1: Register all poll IDs from fetch_map
        for mid_key, msg_info in fetch_map.items():
            if msg_info.get("media_type") == "poll":
                await add_known_poll(ch_str, int(mid_key))
        
        # Pass 2: Index explanations (messages replying to known polls)
        # Only add if not already in the cache (cache takes priority)
        new_from_map = 0
        for mid_key, msg_info in fetch_map.items():
            reply_to = msg_info.get("reply_to")
            if not reply_to:
                continue
            reply_to_int = int(reply_to)
            # Skip if already cached (watcher or previous scan)
            if reply_to_int in ch_expl:
                continue
            # The reply_to MUST point to a known poll
            if msg_info.get("media_type") != "poll" and reply_to_int not in {int(k) for k, v in fetch_map.items() if v.get("media_type") == "poll"}:
                continue
            # Check if this message has explanation content
            media_type = msg_info.get("media_type")
            has_media = msg_info.get("has_media", False)
            is_candidate = (
                media_type == "photo" or
                (media_type is None and not has_media) or
                media_type in ("document", "sticker", "animation")
            )
            if is_candidate:
                ch_expl[reply_to_int] = {
                    "explanation_msg_id": int(mid_key),
                    "text": None,  # Text not stored in fetch_map — will be fetched on demand
                    "has_photo": media_type == "photo",
                    "photo_file_id": None,  # Not stored in fetch_map
                    "kind": "photo" if media_type == "photo" else "text",
                }
                new_from_map += 1
        
        if new_from_map > 0:
            print(f"[EXPLANATION-MERGE] Added {new_from_map} explanations from fetch_map to cache")
    except Exception as e:
        print(f"[EXPLANATION-MERGE] Error merging fetch_map data: {e}")
    
    # ─── PIN MAP: Pre-scan ALL pinned messages BEFORE loop (1-2 API calls total)
    _pin_map = {}
    try:
        _pin_map = await startup_pin(user_client=uc, src_chat_id=i, fetch_map=fetch_map or {})
        print(f"[PIN] Pin map ready: {len(_pin_map)} pinned messages")
    except Exception as e:
        print(f"[PIN] Could not build pin_map: {e}")
    
    await add_active_batch(uid, {
        "total": n,
        "current": 0,
        "success": 0,
        "cancel_requested": False,
        "progress_message_id": pt.id if pt else 0
    })
    
    # Register with FloodWaitScheduler for auto-resume on FloodWait
    from scheduler import scheduler
    try:
        _user_chat_id = m.chat.id
    except Exception:
        _user_chat_id = None
    scheduler.register(uid, resume_fn=_resume_batch,
        resume_kwargs=dict(uid=uid, i=i, s=s, n=n, lt=lt, user_chat_id=_user_chat_id))
    
    # Register this channel for explanation monitoring (real-time listener + incremental scan)
    try:
        from plugins.explanation_listener import add_monitored_channel
        await add_monitored_channel(i, uid, client=uc or ubot)
    except Exception as e:
        print(f"[EXPLANATION] Could not register channel for monitoring: {e}")
    
    # Save batch state to MongoDB for crash recovery
    try:
        await save_batch_state(uid, str(i), int(s), n, dest_channel_id_int, lt)
    except Exception as e:
        print(f"[BATCH-STATE] Failed to save batch state: {e}")
    
    # Start heartbeat — keeps batch_state.updated_at fresh every 30 seconds
    # so startup_auto_resume can detect stale/interrupted batches
    _heartbeat_task = asyncio.create_task(batch_checkpoint_heartbeat(uid, str(i)))
    
    # Register this batch as an asyncio.Task so /stop can cancel it immediately
    current_task = asyncio.current_task()
    if current_task:
        batch_tasks[uid] = current_task
    
    # ═══════════════════════════════════════════════════════════════
    # PASS 1: Pre-upload missing question images BEFORE batch loop
    # Method 1 (preferred): Query dependency index from MongoDB — instant
    # Fallback: Scan fetch_map for polls (slower, but works for old data)
    # ═══════════════════════════════════════════════════════════════
    # PASS 1: DISABLED — pre-upload scan skipped to avoid slow full-channel queries.
    # Question images outside batch range will still be handled by the
    # download→re-upload approach inside process_msg() during Pass 2.
    dep_result = None
    print("[PASS-1] Pre-upload scan DISABLED — skipping")
    
    # ── Method 1 batch-side dependency recording ──
    # If /fetch didn't build the dependency index, the batch records deps
    # on-the-fly as it encounters polls. This makes the index self-populating
    # so the NEXT batch run can use the dependency index for instant Pass 1.
    if dep_result is None:
        _should_record_deps[uid] = True
        _pending_dep_batch[uid] = []
        print(f"[BATCH-DEP] /fetch didn't build deps — batch will record dependencies for uid={uid}")
    else:
        _should_record_deps[uid] = False
        print(f"[BATCH-DEP] /fetch already built deps — batch will NOT record dependencies for uid={uid}")
    
    # ═══════════════════════════════════════════════════════════════
    # PERF CACHE: Resolve per-message data ONCE before the loop starts.
    # ═══════════════════════════════════════════════════════════════
    _s_cached_tcid = None
    _s_cached_topic_id = None
    _s_cached_rtmid = None
    _s_cached_watermark = None
    _s_cached_caption = None
    _s_cached_source_name = None

    try:
        cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
        tcid_cache = int(m.chat.id)
        topic_id_cache = None
        rtmid_cache = None
        if cfg_chat:
            if '/' in cfg_chat:
                parts = cfg_chat.split('/', 1)
                tcid_cache = int(parts[0])
                topic_id_cache = int(parts[1]) if len(parts) > 1 else None
                # Do NOT set rtmid_cache = topic_id_cache — reply_to is per-message
                rtmid_cache = None
            else:
                tcid_cache = int(cfg_chat)
        _s_cached_tcid = tcid_cache
        _s_cached_topic_id = topic_id_cache
        _s_cached_rtmid = rtmid_cache
        try:
            await X.resolve_peer(_s_cached_tcid)
        except Exception:
            pass
        print(f"[PERF-CACHE] Streaming: cached tcid={_s_cached_tcid}, topic_id={_s_cached_topic_id}")
    except Exception as _cache_err:
        print(f"[PERF-CACHE] Streaming tcid cache failed: {_cache_err}")

    try:
        _s_cached_watermark = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT)
        print(f"[PERF-CACHE] Streaming: cached watermark text")
    except Exception:
        _s_cached_watermark = _DEFAULT_WATERMARK_TEXT

    try:
        _s_cached_caption = await get_user_data_key(uid, 'caption_text', '')
        print(f"[PERF-CACHE] Streaming: cached caption text")
    except Exception:
        _s_cached_caption = ''

    try:
        _resolve_client = uc or ubot or X
        _src_chat_info = None
        _resolved_i = i
        try:
            if isinstance(i, str) and i.lstrip('-').isdigit():
                _resolved_i = int(i)
        except Exception:
            pass
        try:
            _src_chat_info = await _resolve_client.get_chat(_resolved_i)
        except Exception:
            pass
        if _src_chat_info and getattr(_src_chat_info, 'title', None):
            _s_cached_source_name = _src_chat_info.title
            print(f"[PERF-CACHE] Streaming: cached source_name={_s_cached_source_name}")
    except Exception:
        _s_cached_source_name = ''

    j = 0  # Track iteration for CancelledError handler
    try:
        for j in range(n):
            
            if should_cancel(uid):
                # Flush whatever new mappings we have on cancel
                new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
                if new_mappings:
                    try:
                        last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                        await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                        print(f"[STOP] Flushed {len(new_mappings)} new mappings on cancel (streaming)")
                    except Exception as e:
                        print(f"[STOP] Failed to flush mappings on cancel: {e}")
                current_msg_id = start_msg_id + j
                if lt == 'private':
                    channel_id_clean = str(i).replace('-100', '')
                    continue_link = f"https://t.me/c/{channel_id_clean}/{current_msg_id}"
                else:
                    continue_link = f"https://t.me/{i}/{current_msg_id}"
                
                await safe_edit(pt,
                    f'Cancelled at {j+1}/{n}. Success: {success}\n\n'
                    f'**Continue from here next time:**\n`{continue_link}`'
                )
                # Send failed links if any
                if failed_links:
                    try:
                        failed_file_path = f"failed_links_{uid}_{int(time.time())}.txt"
                        with open(failed_file_path, 'w', encoding='utf-8') as f:
                            f.write(f"Failed Links Report (Cancelled Batch)\n")
                            f.write(f"====================================\n")
                            f.write(f"Batch cancelled at {j+1}/{n}. Success: {success}\n")
                            f.write(f"Failed so far: {len(failed_links)}\n\n")
                            f.write(f"Links:\n------\n")
                            for idx, fl in enumerate(failed_links, 1):
                                f.write(f"{idx}. {fl}\n")
                        await m.reply_document(failed_file_path, caption=f'❌ Failed links so far ({len(failed_links)})')
                        os.remove(failed_file_path)
                    except Exception as e:
                        print(f"Failed to send failed links file on cancel: {e}")
                break
            
            await update_batch_progress(uid, j, success)
            
            mid = start_msg_id + j
            
            # Skip already-uploaded messages (resume detection)
            if not Z.get(uid, {}).get('force_fresh_start') and mid in msg_id_map:
                # Already uploaded — skip, no API call needed
                # Log every 100th skip to avoid spam, but always log poll skips
                _msg_info = fetch_map.get(str(mid)) if fetch_map else None
                _is_poll_skip = _msg_info and _msg_info.get("media_type") == "poll"
                if _is_poll_skip or j % 100 == 0:
                    _skip_type = "poll" if _is_poll_skip else (_msg_info.get("media_type", "unknown") if _msg_info else "unknown")
                    print(f"[BATCH-SKIP] msg_id={mid} — already uploaded (dest_id={msg_id_map[mid]}, type={_skip_type}) — resuming from previous batch")
                continue
            
            # Get reply_to from fetch map (lightweight — no need to fetch message first)
            reply_to_dest_id = None
            msg_info = fetch_map.get(str(mid))
            if msg_info and msg_info.get("reply_to"):
                reply_to_dest_id = msg_id_map.get(msg_info["reply_to"])
            
            # Fetch THIS ONE message on-demand (not pre-fetched)
            # After fetching, also check src_msg.reply_to_message_id — fetch_map only tracks
            # poll→question image deps, NOT explanation→poll or other reply chains.
            # Without this, explanation messages are sent WITHOUT reply_to the poll.
            # ROBUST: Retry up to 3 times for transient errors before giving up
            # RATE-LIMIT: Source channel reads count against Telegram's per-account
            # rate limit. We use the per-chat rate limiter with the source chat_id
            # to space out reads from the same source channel.
            src_msg = None
            fetch_attempts = 3
            for attempt in range(fetch_attempts):
                try:
                    # Rate-limit source channel reads
                    _src_chat_int = int(i) if isinstance(i, str) and i.lstrip('-').isdigit() else hash(i)
                    await _rate_limiter.acquire(_src_chat_int)
                    src_msg = await get_msg(ubot, uc, i, mid, lt)
                    break  # Success — exit retry loop
                except FloodWait as e:
                    wait_secs = e.value if hasattr(e, 'value') else 30
                    if wait_secs <= 60 and attempt < fetch_attempts - 1:
                        # Short FloodWait on source reads — auto-wait and retry
                        print(f"[FLOOD] Fetch FloodWait {wait_secs}s — auto-waiting (attempt {attempt+1}/{fetch_attempts})")
                        await asyncio.sleep(wait_secs + 2)
                        continue
                    # Long FloodWait or all retries exhausted → stop batch
                    print(f"[FLOOD] Fetch FloodWait {_format_duration(wait_secs)} — stopping batch")
                    await _flood_wait_stop(uid, wait_secs, user_chat_id=m.chat.id)
                    return
                except asyncio.CancelledError:
                    raise  # CRITICAL: Re-raise so /stop works immediately
                except AuthKeyUnregisteredError as e:
                    # FATAL: Session auth key is revoked — ALL messages will fail.
                    # Stop the batch immediately and tell user to re-login.
                    print(f"[BATCH] ⚠️ FATAL: AuthKeyUnregisteredError — stopping batch immediately!")
                    print(f"[BATCH] User must /login again to create a new session.")
                    try:
                        await pt.edit(
                            f'🔴 **Session Expired — Batch Stopped**\n\n'
                            f'Your Telegram session has been revoked by Telegram.\n'
                            f'Every message fetch fails because the auth key is no longer valid.\n\n'
                            f'**To fix this:**\n'
                            f'1. Use `/logout` to clear the old session\n'
                            f'2. Use `/login` to create a new session\n'
                            f'3. Start your batch again\n\n'
                            f'Processed {j+1}/{n} — Success: {success}'
                        )
                    except Exception:
                        pass
                    await remove_active_batch(uid)
                    try:
                        from scheduler import scheduler
                        scheduler.unregister(uid)
                    except Exception:
                        pass
                    return
                except Exception as e:
                    if attempt < fetch_attempts - 1:
                        # Retry after short delay
                        print(f"[FETCH] Attempt {attempt+1}/{fetch_attempts} failed for msg {mid}: {e} — retrying...")
                        await asyncio.sleep(3)
                        continue
                    else:
                        # All retries exhausted
                        if lt == 'private':
                            failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                        else:
                            failed_link = f"https://t.me/{i}/{mid}"
                        failed_links.append(f"{failed_link} — Error after {fetch_attempts} attempts: {str(e)[:60]}")
                        try: await pt.edit(f'Processing {j+1}/{n} — Success: {success} (fetch error)')
                        except: pass
                        break
            
            if not src_msg:
                # Message not found after retries — cannot forward from restricted channels
                # (forwarding is what this bot bypasses, so forward fallback is useless)
                if not failed_links or not any(str(mid) in fl for fl in failed_links[-3:]):
                    if lt == 'private':
                        failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                    else:
                        failed_link = f"https://t.me/{i}/{mid}"
                    failed_links.append(f"{failed_link} — not found (message could not be fetched)")
                print(f"[BATCH-DEBUG] msg_id={mid} — get_msg returned None, tracking as failed")
                try: await pt.edit(f'Processing {j+1}/{n} — Success: {success}')
                except: pass
                await asyncio.sleep(BATCH_SEND_DELAY)
                await _batch_cooldown_check(j, uid)
                continue
            
            # Debug: Log every fetched message type for visibility
            _msg_type = 'unknown'
            if src_msg.poll:
                _msg_type = 'poll'
            elif src_msg.photo:
                _msg_type = 'photo'
            elif src_msg.video:
                _msg_type = 'video'
            elif src_msg.document:
                _msg_type = 'document'
            elif src_msg.audio:
                _msg_type = 'audio'
            elif src_msg.animation:
                _msg_type = 'animation'
            elif src_msg.voice:
                _msg_type = 'voice'
            elif src_msg.video_note:
                _msg_type = 'video_note'
            elif src_msg.sticker:
                _msg_type = 'sticker'
            elif src_msg.text:
                _msg_type = 'text'
            elif src_msg.media:
                _msg_type = f'media({type(src_msg.media).__name__})'
            elif src_msg.service:
                _msg_type = 'service'
            elif src_msg.empty:
                _msg_type = 'empty'
            elif src_msg.forward_origin:
                _msg_type = 'forwarded(no_content)'
            _has_text = bool(src_msg.text)
            _has_caption = bool(getattr(src_msg, 'caption', None))
            _has_media = bool(src_msg.media)
            _src_reply_id_robust = _get_reply_to_id(src_msg)
            print(f"[BATCH-DEBUG] msg_id={mid} type={_msg_type} text={_has_text} caption={_has_caption} media={_has_media} reply_to_msg_id={src_msg.reply_to_message_id} reply_to_robust={_src_reply_id_robust}")
            
            # Handle "empty" messages — they exist in the channel but Pyrogram can't read content.
            # Cannot forward from restricted channels (that's the bot's whole purpose).
            # These are typically deleted messages or messages the userbot can't access.
            if getattr(src_msg, 'empty', False):
                print(f"[BATCH-DEBUG] msg_id={mid} — message is 'empty' in Pyrogram, tracking as failed")
                if not failed_links or not any(str(mid) in fl for fl in failed_links[-3:]):
                    if lt == 'private':
                        failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                    else:
                        failed_link = f"https://t.me/{i}/{mid}"
                    failed_links.append(f"{failed_link} — empty message (content not accessible)")
                await asyncio.sleep(BATCH_SEND_DELAY)
                await _batch_cooldown_check(j, uid)
                continue
            
            # After fetching src_msg, check its reply_to using robust _get_reply_to_id().
            # fetch_map only tracks poll→question image deps, NOT explanation→poll
            # or other reply chains. So we MUST also check src_msg.reply_to
            # to ensure explanation messages reply to their polls in the dest channel.
            # IMPORTANT: Use _get_reply_to_id() instead of src_msg.reply_to_message_id
            # because Pyrofork sometimes stores reply info in reply_to.reply_to_msg_id
            # instead of reply_to_message_id.
            if not reply_to_dest_id:
                if _src_reply_id_robust:
                    reply_to_dest_id = msg_id_map.get(_src_reply_id_robust)
            
            try:
                # CRITICAL FIX: Always use main bot X for sending to user's chat.
                # ubot/uc are for FETCHING (reading source channel), but X (the main bot)
                # must SEND messages to the destination chat because:
                # 1. ubot may be a custom bot that can't message the user's chat
                # 2. ubot may be None (user has no custom bot)
                # 3. The main bot X is the one the user is chatting with
                res, dest_id, _, had_unresolved = await process_msg(X, uc, src_msg, str(m.chat.id), lt, uid, i,
                    reply_to_destination_id=reply_to_dest_id,
                    link_rewrite_map=msg_id_map,
                    dest_channel_id=dest_channel_id_int,
                    dest_channel_username=dest_channel_username,
                    source_channel_username=source_channel_username,
                    source_channel_id=source_channel_id_int,
                    multi_source_channels=_multi_src_channels,
                    rewriter=_rewriter,
                    _cached_tcid=_s_cached_tcid,
                    _cached_topic_id=_s_cached_topic_id,
                    _cached_rtmid=_s_cached_rtmid,
                    _cached_watermark=_s_cached_watermark,
                    _cached_caption=_s_cached_caption,
                    _cached_source_name=_s_cached_source_name,
                    _skip_verify=True,
                    _skip_explanation_scan=True)
                # Debug: Log process_msg result for non-poll messages
                if not src_msg.poll:
                    print(f"[BATCH-DEBUG] msg_id={mid} type={_msg_type} result='{res}' dest_id={dest_id}")
                if dest_id:
                    msg_id_map[mid] = dest_id
                    # ── SIMPLEREWITER: Record mapping ──
                    if _rewriter is not None:
                        try:
                            await _rewriter.record(mid, dest_id)
                        except Exception:
                            pass
                    # 🔑 FINGERPRINT: Store fingerprint for relink resolution (non-blocking)
                    try:
                        from plugins.relink import checkpoint_with_fingerprint
                        asyncio.create_task(checkpoint_with_fingerprint(uid, str(i), mid, dest_id, src_msg))
                    except Exception:
                        pass
                    # 🗂️ SMART CACHE: Index source links for instant /relink queries
                    if dest_channel_id_int:
                        try:
                            _src_text = str(src_msg.text) if src_msg.text else (str(src_msg.caption) if src_msg.caption else '')
                            _src_ents = src_msg.entities if src_msg.entities else (src_msg.caption_entities if src_msg.caption_entities else None)
                            asyncio.create_task(cache_message_for_relink(
                                uid, str(i), dest_channel_id_int, dest_id, mid,
                                _src_text, _src_ents,
                                source_channel_username=source_channel_username,
                                source_channel_id=source_channel_id_int,
                                multi_source_channels=_multi_src_channels
                            ))
                        except Exception:
                            pass
                    
                    # Track forward references (reply_to not yet uploaded)
                    msg_info = fetch_map.get(str(mid))
                    if msg_info and msg_info.get("reply_to"):
                        reply_to_src = msg_info["reply_to"]
                        if reply_to_src not in msg_id_map or reply_to_src == mid:
                            # Forward reference — target message hasn't been uploaded yet
                            # (or self-reference which we skip)
                            if reply_to_src != mid and dest_channel_id_int:
                                asyncio.create_task(add_pending_reply(uid, str(i), dest_channel_id_int, dest_id, reply_to_src))
                    if had_unresolved:
                        # Write to MongoDB (survives crashes/stops) instead of in-memory list
                        asyncio.create_task(mark_needs_link_update(uid, str(i), dest_channel_id_int, dest_id, mid))
                    
                    # 📌 PIN: Non-blocking — fire and forget (pins are rare)
                    if dest_channel_id_int:
                        try:
                            asyncio.create_task(handle_pin_mirror(X, uc, i, mid, src_msg, dest_channel_id_int, dest_id, _pin_map))
                        except Exception as e:
                            print(f"[PIN] handle_pin_mirror failed for msg {mid}: {e}")
                    # ✅ VERIFY: DISABLED per-message — too slow (1 API call per msg)
                    # post_batch_verify() runs at batch end for bulk verification.
                    # 🔗 AUTO-RELINK: Non-blocking — fire and forget
                    if dest_channel_id_int and dest_id:
                        try:
                            from plugins.relink import on_new_mirror_message
                            asyncio.create_task(on_new_mirror_message(uid, str(i), mid, dest_id, dest_channel_id_int))
                        except Exception as e:
                            pass  # Never let auto-relink break mirroring
                if 'Done' in res or 'Copied' in res or 'Sent' in res or 'Forwarded' in res or 'forwarded' in res:
                    success += 1
                else:
                    if lt == 'private':
                        failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                    else:
                        failed_link = f"https://t.me/{i}/{mid}"
                    failed_links.append(f"{failed_link} — {res}")
                
                # ── Check cancel flag after each message ──
                if should_cancel(uid):
                    print(f"[STOP] Cancel flag detected after msg {mid} — stopping batch streaming")
                    raise asyncio.CancelledError()
                
                # ─── PERIODIC BATCH VERIFY (every 500 msgs) ───
                # Lightweight: 5 API calls per 500 msgs (vs 500 calls with old verify_upload)
                # Checks all dest IDs accumulated since last verify
                if dest_channel_id_int and success > 0 and success % BATCH_VERIFY_INTERVAL == 0:
                    try:
                        verify_start_id = start_msg_id + j - BATCH_VERIFY_INTERVAL + 1
                        print(f"[BATCH-VERIFY] Periodic check at {success} msgs — verifying last {BATCH_VERIFY_INTERVAL}...")
                        missing = await batch_verify_uploads(
                            bot_client=X,
                            dst_chat_id=dest_channel_id_int,
                            msg_id_map=msg_id_map,
                            batch_start_src_id=verify_start_id,
                        )
                        if missing:
                            print(f"[BATCH-VERIFY] ⚠️ {len(missing)} missing msgs at checkpoint: {missing[:10]}")
                            for missing_src in missing:
                                if lt == 'private':
                                    mlink = f"https://t.me/c/{str(i).replace('-100', '')}/{missing_src}"
                                else:
                                    mlink = f"https://t.me/{i}/{missing_src}"
                                if not any(str(missing_src) in fl for fl in failed_links):
                                    failed_links.append(f"{mlink} — BATCH VERIFY: missing in dest")
                    except Exception as e:
                        print(f"[BATCH-VERIFY] Periodic check failed: {e}")
                
                # ─── INCREMENTAL MAP SAVE (every 100 msgs) ───
                # Save upload map to MongoDB periodically so mappings survive crashes.
                # With 3s delay between messages, one MongoDB write every 100 msgs (~5 min) is negligible.
                if success > 0 and success % 100 == 0 and dest_channel_id_int:
                    try:
                        new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
                        if new_mappings:
                            last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                            await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                            # Update initial_msg_id_keys so we don't double-save old entries
                            initial_msg_id_keys = set(msg_id_map.keys())
                            print(f"[BATCH-SAVE] Incremental save at {success} msgs — {len(msg_id_map)} total mappings in map")
                    except Exception as e:
                        print(f"[BATCH-SAVE] Incremental save failed: {e}")
            except FloodWait as e:
                # ANY FloodWait during upload → stop batch immediately
                wait_secs = e.value if hasattr(e, 'value') else 30
                print(f"[FLOOD] Upload FloodWait {_format_duration(wait_secs)} — stopping batch")
                await _flood_wait_stop(uid, wait_secs, user_chat_id=m.chat.id)
                return
            except Exception as e:
                # CRITICAL: FloodWait must be caught here, NOT treated as regular error.
                # If FloodWait reaches here, it means the inner handlers didn't catch it.
                # This happens when download_media or process_msg raises FloodWait
                # that wasn't caught by the inner try/except.
                if isinstance(e, FloodWait):
                    wait_secs = e.value if hasattr(e, 'value') else 30
                    print(f"[FLOOD] Uncaught FloodWait {_format_duration(wait_secs)} at msg {mid} — stopping batch")
                    await _flood_wait_stop(uid, wait_secs, user_chat_id=m.chat.id)
                    return
                
                if lt == 'private':
                    failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                else:
                    failed_link = f"https://t.me/{i}/{mid}"
                failed_links.append(f"{failed_link} — Error: {str(e)[:80]}")
                print(f"[STREAMING] Error processing msg {mid}: {e}")
                try: await pt.edit(f'{j+1}/{n}: Error - {str(e)[:50]}')
                except: pass
            
            # Free memory immediately — we don't need this message anymore
            del src_msg
            
            # Reclaim RAM from Pyrogram's download/upload buffers after each message.
            # Without this, Python's heap holds freed memory forever and RSS climbs
            # ~60MB per video. malloc_trim(0) returns it to the OS instantly.
            _ram_reclaim()
            
            # RSS Guard: If RAM is too high, do aggressive cleanup
            try:
                _rss = 0
                with open('/proc/self/status') as _rf:
                    for _rl in _rf:
                        if _rl.startswith('VmRSS:'):
                            _rss = int(_rl.split()[1]) / 1024
                            break
                if _rss > 600:  # MB threshold
                    print(f"[RSS-GUARD] RSS={_rss:.0f}MB > 600MB — aggressive cleanup...")
                    for _rc in range(3):
                        gc.collect()
                        try:
                            ctypes.CDLL("libc.so.6").malloc_trim(0)
                        except Exception:
                            pass
                        await asyncio.sleep(5)
                    _rss_after = 0
                    try:
                        with open('/proc/self/status') as _rf:
                            for _rl in _rf:
                                if _rl.startswith('VmRSS:'):
                                    _rss_after = int(_rl.split()[1]) / 1024
                                    break
                    except Exception:
                        pass
                    _freed_mb = _rss - _rss_after
                    if _freed_mb > 50:
                        print(f"[RSS-GUARD] Recovered {_freed_mb:.0f}MB — continuing ({_rss:.0f} → {_rss_after:.0f}MB)")
                    else:
                        print(f"[RSS-GUARD] Cleanup only freed {_freed_mb:.0f}MB — RSS still {_rss_after:.0f}MB")
            except Exception:
                pass
            
            # Update progress with visual bar (throttled: every 50 msgs OR every 30s, prevents FloodWait)
            now = time.time()
            if (j + 1) % 50 == 0 or now - last_progress_edit >= 30 or j + 1 == n:
                elapsed = now - batch_start_time
                pct = min((j + 1) * 100 // n, 100)
                rate = (j + 1) / elapsed if elapsed > 0 else 0
                remaining = (n - j - 1) / rate if rate > 0 else 0
                filled = pct // 10
                bar = '🟢' * filled + '⚪' * (10 - filled)
                if remaining > 60:
                    eta_str = f'{int(remaining // 60)}m {int(remaining % 60)}s'
                else:
                    eta_str = f'{int(remaining)}s'
                try:
                    await safe_edit(pt,
                        f'📦 **Batch Progress**\n\n'
                        f'{bar}  **{pct}%**\n\n'
                        f'✅ Done: **{j+1}**/{n} | Success: **{success}**\n'
                        f'⚡ Rate: **{rate:.2f} msgs/min**\n'
                        f'⏳ ETA: **{eta_str}**\n'
                        f'⏱️ Elapsed: {elapsed:.0f}s'
                    )
                    last_progress_edit = now
                except Exception:
                    pass
            
            # Log RAM every 100 messages
            if (j + 1) % 100 == 0:
                log_ram("batch_streaming_progress", extra_info={"uid": uid, "progress": f"{j+1}/{n}", "success": success})
            
            await asyncio.sleep(BATCH_SEND_DELAY)
            
            # Cooldown pause every N messages to prevent sustained-rate FloodWait
            await _batch_cooldown_check(j, uid)
            
            # Post-sleep RAM reclaim: by now Pyrogram has released its internal session
            # buffers from the download/upload. malloc_trim can return these to the OS.
            _ram_reclaim()
        
        # Batch completion
        if j + 1 == n:
            # Single flush — write NEW mappings to MongoDB at once
            new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
            if new_mappings:
                try:
                    last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                    await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                    print(f"[BATCH-END] Flushed {len(new_mappings)} new mappings to MongoDB ({len(msg_id_map)} total)")
                except Exception as e:
                    print(f"[BATCH-END] Failed to flush mappings: {e}")
            
            # ── Flush any remaining batch-side dependency records ──
            try:
                await _flush_dep_batch(uid, force=True)
            except Exception as e:
                print(f"[BATCH-DEP] Final flush failed for uid={uid}: {e}")
            _clear_dep_recording_state(uid)
            
            last_msg_id = start_msg_id + n - 1
            if lt == 'private':
                channel_id_clean = str(i).replace('-100', '')
                last_link = f"https://t.me/c/{channel_id_clean}/{last_msg_id}"
            else:
                last_link = f"https://t.me/{i}/{last_msg_id}"
            
            # ─── POST-BATCH VERIFICATION (bulk, RAM-safe) ───
            # Full integrity check: missing, type, order, reply chain
            post_verify_issues = {}
            all_missing = []
            if dest_channel_id_int and msg_id_map:
                try:
                    print(f"[POST-VERIFY] Running full post-batch verification on {len(msg_id_map)} messages...")
                    post_verify_issues = await post_batch_verify(
                        client=X,
                        src_chat_id=i,
                        dst_chat_id=dest_channel_id_int,
                        src_to_dst=msg_id_map,
                        src_client=uc,
                    )
                    all_missing = post_verify_issues.get("missing", [])
                    if all_missing:
                        print(f"[POST-VERIFY] ⚠️ {len(all_missing)} missing messages: {all_missing[:20]}")
                        for missing_src in all_missing:
                            if lt == 'private':
                                mlink = f"https://t.me/c/{str(i).replace('-100', '')}/{missing_src}"
                            else:
                                mlink = f"https://t.me/{i}/{missing_src}"
                            if not any(str(missing_src) in fl for fl in failed_links):
                                failed_links.append(f"{mlink} — POST VERIFY: missing in dest")
                    else:
                        print(f"[POST-VERIFY] ✅ All {len(msg_id_map)} uploads verified!")
                    
                    # Log other issues
                    for issue_type in ["type_mismatch", "wrong_order", "reply_broken"]:
                        if post_verify_issues.get(issue_type):
                            print(f"[POST-VERIFY] {issue_type}: {len(post_verify_issues[issue_type])} issues")
                except Exception as e:
                    print(f"[POST-VERIFY] Verification failed: {e}")
            
            # ─── COUNT SANITY CHECK — 1 API call ───
            count_result = {"ok": True, "message": "Skipped (no dest channel)"}
            if dest_channel_id_int:
                try:
                    count_result = await count_sanity_check(
                        bot_client=X,
                        dst_chat_id=dest_channel_id_int,
                        expected_count=success,
                        src_chat_id=i,
                    )
                    print(f"[COUNT-CHECK] {count_result['message']}")
                except Exception as e:
                    print(f"[COUNT-CHECK] Failed: {e}")
            
            # Mark batch complete in MongoDB
            try:
                await mark_batch_complete(uid, str(i))
            except Exception as e:
                print(f"[BATCH-STATE] Failed to mark batch complete: {e}")
            
            # Cancel heartbeat — batch is done
            try:
                _heartbeat_task.cancel()
            except Exception:
                pass
            
            # 📌 PIN SYNC: Ensure ALL source pins are pinned in dest
            # Use uc (user client) if available, else fall back to ubot
            _pin_client = uc or ubot
            if _pin_client and dest_channel_id_int and msg_id_map:
                try:
                    pin_result = await verify_and_sync_pins(
                        user_client=_pin_client, bot_client=X,
                        src_chat_id=i, dst_chat_id=dest_channel_id_int,
                        msg_id_map=msg_id_map,
                        uid=uid, source_channel=str(i),
                    )
                    print(f"[PIN-SYNC] Result: source_pins={pin_result.get('total_source_pins',0)} mapped={pin_result.get('mapped_pins',0)} newly_pinned={pin_result.get('newly_pinned',0)} failed={len(pin_result.get('failed_to_pin',[]))} not_in_map={len(pin_result.get('not_in_map',[]))}")
                    # If pins still failed after trying both user_client and bot_client,
                    # try with ubot as last resort (only if ubot wasn't already used as _pin_client)
                    if pin_result.get('failed_to_pin'):
                        fallback_client = ubot if (ubot and ubot != _pin_client and ubot != X) else None
                        if fallback_client:
                            print(f"[PIN-SYNC] {len(pin_result['failed_to_pin'])} pins still failed — retrying with ubot...")
                            for src_id, reason in pin_result['failed_to_pin']:
                                dst_id = msg_id_map.get(src_id)
                                if dst_id:
                                    try:
                                        from plugins.pin_map import pin_in_destination
                                        await pin_in_destination(client=fallback_client, dst_chat_id=dest_channel_id_int, dst_msg_id=dst_id)
                                        print(f"[PIN-SYNC] ubot pinned src_msg={src_id} → dst_msg={dst_id}")
                                        try:
                                            await mark_needs_link_update(uid, str(i), dest_channel_id_int, dst_id, src_id)
                                        except Exception:
                                            pass
                                    except Exception as e2:
                                        print(f"[PIN-SYNC] ubot also failed to pin src_msg={src_id}: {e2}")
                        else:
                            print(f"[PIN-SYNC] {len(pin_result['failed_to_pin'])} pins failed and no additional client available for retry")
                except Exception as e:
                    print(f"[PIN-SYNC] verify_and_sync_pins failed: {e}")
            elif not _pin_client:
                print(f"[PIN-SYNC] SKIPPED — no user client available to fetch source pins")
            
            failed_count = n - success
            verify_info = ''
            if all_missing:
                verify_info = f'\n\n🔍 **Verify:** {len(all_missing)} missing in dest channel'
            elif dest_channel_id_int and msg_id_map:
                verify_info = f'\n\n🔍 **Verify:** ✅ All {len(msg_id_map)} uploads confirmed'
            count_info = f"\n📊 **Count check:** {count_result.get('message', 'N/A')}"
            
            completion_text = (
                f'Batch Completed ✅ Success: {success}/{n}\n\n'
                f'**Last message link:**\n`{last_link}`\n\n'
                f'Use this link next time to continue downloading from where you left off.'
                f'{verify_info}'
                f'{count_info}'
            )
            if failed_count > 0:
                completion_text += f'\n\n❌ Failed: {failed_count} — see attached file for details.'
            await safe_reply(m, completion_text)
            
            # Send failed links TXT file
            if failed_links:
                try:
                    failed_file_path = f"failed_links_{uid}_{int(time.time())}.txt"
                    with open(failed_file_path, 'w', encoding='utf-8') as f:
                        f.write(f"Failed Links Report\n")
                        f.write(f"==================\n")
                        f.write(f"Batch: {success}/{n} succeeded\n")
                        f.write(f"Failed: {len(failed_links)}\n\n")
                        f.write(f"Links:\n------\n")
                        for idx, fl in enumerate(failed_links, 1):
                            f.write(f"{idx}. {fl}\n")
                    await m.reply_document(failed_file_path, caption=f'❌ Failed links ({len(failed_links)})')
                    os.remove(failed_file_path)
                except Exception as e:
                    print(f"Failed to send failed links file: {e}")
        
        # Post-batch link update pass — now uses MongoDB-backed tracking
        # Catches anything that became resolvable during this batch
        await resolve_pending_link_rewrites(
            bot_client=X, ubot=ubot, source_channel=i,
            dest_channel_id_int=dest_channel_id_int,
            dest_channel_username=dest_channel_username,
            source_channel_username=source_channel_username,
            uid=uid,
            source_channel_id=source_channel_id_int,
            multi_source_channels=_multi_src_channels,
            combined_msg_id_map=_combined_msg_id_map,
        )
    
    except asyncio.CancelledError:
        # /stop was used — task.cancel() was called
        # Flush whatever new mappings we have before cleanup
        new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
        if new_mappings:
            try:
                last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                print(f"[STOP] Flushed {len(new_mappings)} new mappings on cancel (streaming)")
            except Exception as e:
                print(f"[STOP] Failed to flush mappings on cancel: {e}")
        # Flush any remaining batch-side dependency records
        try:
            await _flush_dep_batch(uid, force=True)
        except Exception:
            pass
        _clear_dep_recording_state(uid)
        print(f"[STOP] Batch streaming cancelled for uid={uid} at success={success}")
        try:
            current_msg_id = start_msg_id + j
            if lt == 'private':
                channel_id_clean = str(i).replace('-100', '')
                continue_link = f"https://t.me/c/{channel_id_clean}/{current_msg_id}"
            else:
                continue_link = f"https://t.me/{i}/{current_msg_id}"
            await safe_edit(pt,
                f'🛑 Stopped at {j+1}/{n}. Success: {success}\n\n'
                f'**Continue from here next time:**\n`{continue_link}`'
            )
        except Exception:
            pass
    finally:
        # Cleanup: resolve pending replies and remove active batch
        try:
            resolved = await resolve_pending_replies(uid, str(i), msg_id_map)
            if resolved > 0:
                print(f"[RESUME] Resolved {resolved} forward references after streaming batch")
        except Exception as e:
            print(f"[RESUME] Error resolving pending replies: {e}")
        
        await remove_active_batch(uid)
        batch_tasks.pop(uid, None)  # Clean up task reference
        clear_cancel_flag(uid)
        log_ram("batch_streaming_end", extra_info={"uid": uid, "success": success, "total": n})
        Z.pop(uid, None)
        _rate_limiter.clear()
        _download_rate_limiter.clear()


@X.on_message(filters.command(['batch', 'single']))
async def process_cmd(c, m):
    uid = m.from_user.id
    cmd = m.command[0]
    
    # Owner and auth users bypass all limits — Super Prime unlimited access
    if uid in OWNER_ID:
        pass  # Owner always has access
    elif await is_auth_user(uid):
        pass  # Auth users always have access
    elif FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await safe_reply(m, "This bot does not provide free servies, get subscription from OWNER")
        return
    
    if await sub(c, m) == 1: return
    # Use safe_reply (non-blocking) instead of reply_with_wait.
    # After a FloodWait, the bot's account may still be rate-limited,
    # and reply_with_wait blocks up to 60s trying to reply, making
    # the /batch command appear unresponsive. safe_reply fails silently.
    pro = await safe_reply(m, '⏳ Checking...')
    
    if is_user_active(uid):
        if pro:
            await safe_edit(pro, 'You have an active task. Use /stop to cancel it.')
        else:
            # pro is None (FloodWait prevented initial reply) — try direct reply
            await safe_reply(m, 'You have an active task. Use /stop to cancel it.')
        return
    
    # Check if user is in /fetch conversation
    try:
        from plugins.fetch import FETCH_STATE
        if uid in FETCH_STATE:
            await safe_edit(pro, 'You have a /fetch in progress. Send /cancelfetch first.')
            return
    except Exception:
        pass
    
    # Use user's custom bot if set, otherwise use the main bot
    ubot = await get_ubot(uid)
    if not ubot:
        ubot = X
    
    # ─── MODE SELECTION: Ask user to choose between Link mode and Clone mode ───
    mode_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 Link Mode (Normal)", callback_data=f"batch_mode_link_{uid}"),
        ],
        [
            InlineKeyboardButton("📁 Clone Channel Structure", callback_data=f"batch_mode_clone_{uid}"),
        ],
    ])
    mode_text = (
        '**Choose extraction mode:**\n\n'
        '🔗 **Link Mode** — Send start & end links, download flat to one channel/topic\n'
        '📁 **Clone Mode** — Copy source channel structure (creates forums/topics in destination if source has them)'
    )
    if pro:
        try:
            await safe_edit(pro, mode_text, reply_markup=mode_keyboard)
        except Exception:
            # If edit with keyboard fails, fall back to text-only
            await safe_edit(pro, mode_text)
            await safe_reply(m, mode_text, reply_markup=mode_keyboard)
    else:
        await safe_reply(m, mode_text, reply_markup=mode_keyboard)
    # Don't set Z[uid] step yet — wait for callback selection
    return

# ═══════════════════════════════════════════════════════════════
# MODE SELECTION CALLBACK — handles the inline button choice
# ═══════════════════════════════════════════════════════════════
@X.on_callback_query(filters.regex(r'^batch_mode_(link|clone)_(\d+)$'))
async def batch_mode_callback(c, cb):
    """Handle the mode selection callback from /batch."""
    uid = cb.from_user.id
    match = cb.data  # e.g. "batch_mode_link_123456"
    parts = match.split('_')
    # parts: ['batch', 'mode', 'link' or 'clone', 'uid']
    mode = parts[2]  # 'link' or 'clone'
    cb_uid = int(parts[3])

    if cb_uid != uid:
        await cb.answer("This is not your session.", show_alert=False)
        return

    try:
        await cb.answer()
    except Exception:
        pass

    if mode == 'link':
        # Original /batch flow — set step to 'start'
        Z[uid] = {'step': 'start'}
        await safe_reply(cb.message, '🔗 **Link Mode selected.**\n\nSend start link...')
    elif mode == 'clone':
        # Clone mode — redirect to channel_clone flow
        Z[uid] = {'step': 'clone_source', 'clone_mode': True}
        # Also set up CLONE_STATE for the clone plugin
        try:
            from plugins.channel_clone import CLONE_STATE
            CLONE_STATE[uid] = {'step': 'got_source_link', 'pro_message': cb.message}
        except ImportError:
            pass
        await safe_reply(cb.message,
            '📁 **Clone Mode selected.**\n\n'
            'Send the **source channel link** to clone its structure...\n\n'
            '_If the source has forums/topics, they will be created in your destination channel._'
        )


@X.on_message(filters.command(['cancel', 'stop']))
async def cancel_cmd(c, m):
    uid = m.from_user.id
    
    print(f"[STOP] /stop received from uid={uid} — is_active={is_user_active(uid)}, in_batch_tasks={uid in batch_tasks}")
    
    stopped_something = False
    
    if is_user_active(uid) or uid in batch_tasks:
        cancel_ok = await request_batch_cancel(uid)
        
        # ALWAYS force-clean up — the batch may be stale (file-persisted ACTIVE_USERS
        # entry from a crashed/restarted dyno, but no actual asyncio.Task running).
        await remove_active_batch(uid)
        batch_tasks.pop(uid, None)
        Z.pop(uid, None)
        clear_cancel_flag(uid)
        _rate_limiter.clear()
        _download_rate_limiter.clear()
        
        # Also clean up CLONE_STATE if user was in clone mode
        try:
            from plugins.channel_clone import CLONE_STATE
            CLONE_STATE.pop(uid, None)
        except Exception:
            pass
        
        stopped_something = True
        if cancel_ok:
            batch_msg = '🛑 Batch stopped immediately.'
        else:
            batch_msg = '🛑 Batch stopped (cleaned up stale state).'
        print(f"[STOP] uid={uid} — cancel_ok={cancel_ok}, batch stopped")
    else:
        # No active task — just clean up any stale state and set cancel flag anyway
        # (in case the batch is stuck in a weird state)
        _CANCEL_FLAGS[uid] = True
        if is_user_active(uid):
            await remove_active_batch(uid)
            batch_tasks.pop(uid, None)
            Z.pop(uid, None)
        # Also clean up CLONE_STATE
        try:
            from plugins.channel_clone import CLONE_STATE
            CLONE_STATE.pop(uid, None)
        except Exception:
            pass
        batch_msg = ''
        print(f"[STOP] uid={uid} — no active batch found, cancel flag set anyway as safety measure")
    
    # ── Stop auto-sync (uses cancel event + task.cancel() + MongoDB deactivate) ──
    auto_stopped = 0
    try:
        from plugins.auto import stop_auto_task, deactivate_auto_sync
        # stop_auto_task uses triple-redundancy: cancel event + task.cancel() + cleanup
        auto_stopped = stop_auto_task(uid)  # stops ALL auto-syncs for this user
        # Also deactivate in MongoDB (in case task was running in another dyno)
        count = await deactivate_auto_sync(uid)
        auto_stopped = max(auto_stopped, count)
        print(f"[STOP] uid={uid} — Stopped {auto_stopped} auto-sync(s)")
    except ImportError as e:
        print(f"[STOP] Cannot import auto module: {e}")
    except Exception as e:
        print(f"[STOP] Auto-sync cleanup error: {e}")
    
    # ── Build reply ──
    msgs = []
    if batch_msg:
        msgs.append(batch_msg)
    if auto_stopped:
        msgs.append(f"🛑 Stopped {auto_stopped} auto-sync(s).")
    
    if msgs:
        try:
            await reply_with_wait(m, '\n'.join(msgs))
        except Exception:
            # Don't let FloodWait on the reply block the /stop handler
            pass
    else:
        await reply_with_wait(m, 'No active batch or auto-sync found.')


@X.on_message(filters.command(['clearbatch', 'clear']))
async def clearbatch_cmd(c, m):
    """Delete batch state for the user — allows fully fresh start.
    
    DELETE upload_maps, pending_replies, unresolved_links, pending_explanations,
    dependencies, relink_fingerprints, mirrored_messages_index — all of these
    cause resume behavior or stale mapping data that breaks fresh starts.
    Preserve fetch maps, pin maps, answer keys (non-resume data).
    """
    # Support both /clearbatch and /clear batch
    cmd = m.command[0].lower()
    if cmd == 'clear' and (not m.command[1:] or m.command[1].lower() != 'batch'):
        return  # "/clear" without "batch" argument — not our command
    
    uid = m.from_user.id
    try:
        # 1) Cancel any running task
        task_cancelled = False
        if uid in batch_tasks:
            task = batch_tasks[uid]
            if not task.done():
                task.cancel()
                task_cancelled = True
                print(f"[CLEARBATCH] uid={uid} — Cancelled asyncio.Task")
            del batch_tasks[uid]
        
        # 1b) Clear Z[uid] session state (step/cid/sid/lt etc.)
        Z.pop(uid, None)
        clear_cancel_flag(uid)
        _rate_limiter.clear()
        _download_rate_limiter.clear()

        # 1c) Clone cleanup — cancel running clone and wipe its state + MongoDB job
        clone_cleared = False
        try:
            from plugins.channel_clone import CLONE_STATE, clone_jobs_collection as _cjc
            CLONE_STATE.pop(uid, None)
            cj_del = await _cjc.delete_many({"uid": uid})
            if cj_del.deleted_count:
                clone_cleared = True
                print(f"[CLEARBATCH] uid={uid} — Deleted {cj_del.deleted_count} clone job(s) from MongoDB")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Clone cleanup error: {e}")
        
        # 2) Unregister FloodWaitScheduler job & cancel any pending resume timer
        scheduler_cleared = False
        try:
            from scheduler import scheduler
            if scheduler.get_job(uid) is not None:
                scheduler.unregister(uid)
                scheduler_cleared = True
                print(f"[CLEARBATCH] uid={uid} — Unregistered FloodWaitScheduler job & cancelled resume timer")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not unregister scheduler: {e}")
        
        # 3) Remove active batch entry
        was_active = str(uid) in ACTIVE_USERS
        if was_active:
            del ACTIVE_USERS[str(uid)]
            await save_active_users_to_file()
            print(f"[CLEARBATCH] uid={uid} — Removed active batch entry")
        
        # 4) DELETE upload maps — clears mappings AND resume position
        maps_deleted = 0
        try:
            del_result = await upload_maps_collection.delete_many({"user_id": uid})
            maps_deleted = del_result.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {maps_deleted} upload map(s) (mappings + resume position)")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete upload maps: {e}")
        
        # 5) DELETE pending replies — stale reply chains from previous batch
        pending_deleted = 0
        try:
            pend_del = await pending_replies_collection.delete_many({"user_id": uid})
            pending_deleted = pend_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {pending_deleted} pending reply references")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete pending replies: {e}")
        
        # 6) Delete upload status (done/failed tracking only — NOT the mappings)
        status_deleted = 0
        try:
            status_deleted = await clear_upload_status(uid)
            print(f"[CLEARBATCH] uid={uid} — Deleted {status_deleted} upload status entries")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Failed to clear upload status: {e}")
        
        # 7) Delete batch state (crash recovery state only)
        state_deleted = 0
        try:
            state_deleted = await clear_batch_state(uid)
            print(f"[CLEARBATCH] uid={uid} — Deleted {state_deleted} batch state entries")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Failed to clear batch state: {e}")
        
        # 7a) Delete batch checkpoints — CRITICAL: prevents auto-resume on restart
        checkpoint_deleted = 0
        try:
            from plugins.verify_and_resume import batch_checkpoint_collection
            chk_del = await batch_checkpoint_collection.delete_many({"user_id": uid})
            checkpoint_deleted = chk_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {checkpoint_deleted} batch checkpoint(s)")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete batch checkpoints: {e}")
        
        # 7b) Delete unresolved links (stale link rewrite tracking)
        unresolved_deleted = 0
        try:
            unres_del = await unresolved_links_collection.delete_many({"user_id": uid})
            unresolved_deleted = unres_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {unresolved_deleted} unresolved link entries")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete unresolved links: {e}")
        
        # 7c) Delete pending explanations (stale poll explanation tracking)
        explanations_deleted = 0
        try:
            expl_del = await pending_explanations_collection.delete_many({"user_id": uid})
            explanations_deleted = expl_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {explanations_deleted} pending explanation entries")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete pending explanations: {e}")
        
        # 7d) Delete dependencies index (poll → question image tracking)
        deps_deleted = 0
        try:
            deps_del = await dependencies_collection.delete_many({"user_id": uid})
            deps_deleted = deps_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {deps_deleted} dependency entries")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete dependencies: {e}")
        
        # 8) DELETE relink fingerprints — stale after clearbatch causes wrong resume
        fp_deleted = 0
        try:
            from plugins.relink import fingerprints_collection as _fpc
            fp_del = await _fpc.delete_many({"uid": uid})
            fp_deleted = fp_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {fp_deleted} relink fingerprints")
        except ImportError:
            print(f"[CLEARBATCH] uid={uid} — relink plugin not available")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete fingerprints: {e}")
        
        # 9) DELETE mirrored messages index — SimpleRewriter loads this into msg_id_map
        #    which causes batch to SKIP messages as "already uploaded". MUST clear for fresh start.
        mirror_deleted = 0
        try:
            mirror_del = await mirrored_messages_index.delete_many({"uid": uid})
            mirror_deleted = mirror_del.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {mirror_deleted} mirrored message index entries")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not delete mirror index: {e}")
        
        # 10) Stop and delete auto-sync tasks
        auto_stopped = 0
        try:
            from plugins.auto import stop_auto_task, auto_sync_collection
            auto_stopped = stop_auto_task(uid)
            auto_del_result = await auto_sync_collection.delete_many({"user_id": uid})
            auto_stopped += auto_del_result.deleted_count
            print(f"[CLEARBATCH] uid={uid} — Deleted {auto_stopped} auto-sync entry/entries")
        except ImportError:
            pass
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Auto-sync cleanup error: {e}")
        
        # Build response — always show something
        parts = []
        if task_cancelled:
            parts.append("running task cancelled")
        if clone_cleared:
            parts.append("clone job cleared")
        if scheduler_cleared:
            parts.append("flood-wait auto-resume cancelled")
        if was_active:
            parts.append("active batch removed")
        if state_deleted:
            parts.append(f"{state_deleted} batch state cleared")
        if checkpoint_deleted:
            parts.append(f"{checkpoint_deleted} batch checkpoint(s) cleared")
        if status_deleted:
            parts.append(f"{status_deleted} upload status cleared")
        if auto_stopped:
            parts.append(f"{auto_stopped} auto-sync(s) stopped & deleted")
        
        # Add deleted items to response
        if maps_deleted:
            parts.append(f"{maps_deleted} upload map(s) deleted")
        if pending_deleted:
            parts.append(f"{pending_deleted} pending replies deleted")
        if unresolved_deleted:
            parts.append(f"{unresolved_deleted} unresolved links deleted")
        if explanations_deleted:
            parts.append(f"{explanations_deleted} pending explanations deleted")
        if deps_deleted:
            parts.append(f"{deps_deleted} dependencies deleted")
        if fp_deleted:
            parts.append(f"{fp_deleted} relink fingerprint(s) deleted")
        if mirror_deleted:
            parts.append(f"{mirror_deleted} mirror index entries deleted")
        
        if parts:
            cleared_line = f'🗑️ **Batch cleared — fully fresh start.**\n\n{" , ".join(parts)}'
        else:
            cleared_line = '🗑️ **No active batch state to clear.**'
        
        # 11) Set force_fresh_start flag — next /batch will start from scratch
        # This ensures that even if mirrored_messages_index or upload_maps have
        # residual data (from a previous clearbatch that preserved them), the
        # next batch will NOT resume from the middle. The flag is consumed
        # (deleted) when the next batch starts.
        _force_fresh_start_uids.add(uid)  # In-memory flag (immediate)
        try:
            from plugins.verify_and_resume import batch_state_collection as _bsc
            await _bsc.update_one(
                {"user_id": uid},
                {"$set": {"force_fresh_start": True, "updated_at": datetime.now()}},
                upsert=True
            )
            print(f"[CLEARBATCH] uid={uid} — Set force_fresh_start flag (in-memory + MongoDB)")
        except Exception as e:
            print(f"[CLEARBATCH] uid={uid} — Could not set force_fresh_start flag in MongoDB: {e}")
            # In-memory flag still works for this process lifetime
        
        await reply_with_wait(m,
            f'{cleared_line}'
            f'\n\n'
            f'Fetch maps, pin maps, answer keys are preserved.'
        )
    except Exception as e:
        import traceback
        print(f"[CLEARBATCH] uid={uid} — UNHANDLED ERROR: {e}")
        traceback.print_exc()
        try:
            await reply_with_wait(m, f'❌ /clearbatch error: {e}')
        except Exception:
            pass


# NOTE: /status is handled by stats.py — shows batch status if active, else login/premium status

@X.on_message(filters.command('setwatermark'))
async def setwatermark_cmd(c, m):
    """Set custom watermark text for images and videos.

    Usage:
        /setwatermark My Brand       → Sets watermark to "My Brand"
        /setwatermark                → Shows current watermark setting
        /setwatermark off            → Disables watermark
        /setwatermark reset          → Reset to default ("THE ENLIGHTER FROM HARRY")
    """
    uid = m.from_user.id
    args = m.text.split(" ", 1)
    
    # No args — show current setting
    if len(args) < 2 or not args[1].strip():
        current = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT)
        await safe_reply(m,
            f'🎨 **Watermark Setting**\n\n'
            f'Current watermark: `{current}`\n\n'
            f'**Commands:**\n'
            f'`/setwatermark Your Text` — Set custom watermark\n'
            f'`/setwatermark off` — Disable watermark\n'
            f'`/setwatermark reset` — Reset to default (`{_DEFAULT_WATERMARK_TEXT}`)\n\n'
            f'Watermark is applied to all photos and videos during batch processing.'
        )
        return
    
    text = args[1].strip()
    
    if text.lower() == 'off':
        await save_user_data(uid, 'watermark_text', '')
        await safe_reply(m, '🎨 Watermark **disabled**. Images and videos will be uploaded without watermark.')
    elif text.lower() == 'reset':
        await save_user_data(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT)
        await safe_reply(m, f'🎨 Watermark **reset** to default: `{_DEFAULT_WATERMARK_TEXT}`')
    else:
        # Validate length — keep it reasonable for overlay
        if len(text) > 40:
            await safe_reply(m, '❌ Watermark text too long (max 40 characters). Choose something shorter.')
            return
        await save_user_data(uid, 'watermark_text', text)
        await safe_reply(m, f'🎨 Watermark **set** to: `{text}`\n\nApplied to all photos and videos in future batches.')


@X.on_message(filters.text & filters.private & ~login_in_progress & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id',
    'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot',
    'auth', 'unauth', 'authusers', 'logs', 'fetch', 'cancelfetch', 'fetchmaps', 'clearfetch', 'answerkey', 'clearbatch', 'clear', 'status',
    'viewfetchmaps', 'viewanswerkey', 'clearanswerkey', 'settings', 'help', 'terms', 'plan',
    'auto', 'autooff', 'cancelauto', 'linkexplan', 'explans', 'transfer', 'rem', 'dl', 'adl', 'setwatermark', 'clone']))
async def text_handler(c, m):
    uid = m.from_user.id
    if uid not in Z:
        raise ContinuePropagation  # Pass to fetch.py's text_handler or other handlers
    s = Z[uid].get('step')

    # ═══════════════════════════════════════════════════════════════
    # CLONE MODE HANDLING — let channel_clone.py's registered handler process it
    # ═══════════════════════════════════════════════════════════════
    # IMPORTANT: Do NOT call clone_text_handler directly here.
    # channel_clone.py has its own @X.on_message handler registered for
    # the same filter. If we call it here AND let Pyrogram's handler chain
    # run it, the message gets processed TWICE — causing duplicate replies
    # and state corruption. Instead, raise ContinuePropagation so Pyrogram
    # passes the message to channel_clone.py's registered handler.
    if s in ('clone_source', 'clone_count', 'clone_running'):
        raise ContinuePropagation

    # Use user's custom bot if set, otherwise use the main bot
    x = await get_ubot(uid)
    if not x:
        x = X

    if s == 'start':
        L = m.text
        i, d, lt = E(L)
        if not i or not d:
            await safe_reply(m, 'Invalid link format.')
            Z.pop(uid, None)
            return
        Z[uid].update({'step': 'count', 'cid': i, 'sid': d, 'lt': lt})
        await safe_reply(m,
            '**How many files to download?**\n\n'
            '**Choose one:**\n'
            '1️⃣ **Number** — e.g. `5000` (download 5000 files)\n'
            '2️⃣ **Last link** — download from start to that link\n'
            '3️⃣ **all** — download ALL messages to the end'
        )

    elif s == 'start_single':
        L = m.text
        i, d, lt = E(L)
        if not i or not d:
            await safe_reply(m, 'Invalid link format.')
            Z.pop(uid, None)
            return

        Z[uid].update({'step': 'process_single', 'cid': i, 'sid': d, 'lt': lt})
        i, s, lt = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['lt']
        pt = await safe_reply(m, 'Processing...')
        
        # Use user's custom bot if set, otherwise use the main bot
        # CRITICAL FIX: Use get_ubot() which checks LRU cache first, not UB.get()
        ubot = await get_ubot(uid)
        if not ubot:
            ubot = X
        
        # Get user client with timeout protection
        try:
            uc = await asyncio.wait_for(get_uclient(uid), timeout=90)
        except asyncio.TimeoutError:
            await safe_edit(pt, 'User client setup timed out. Please try again or use /login first.')
            Z.pop(uid, None)
            return
        
        if not uc:
            # Try global userbot as fallback
            uc = get_Y()
            if not uc:
                await safe_edit(pt, 'Cannot proceed without user client. Use /login first.')
                Z.pop(uid, None)
                return
            
        if is_user_active(uid):
            await safe_edit(pt, 'Active task exists. Use /stop first.')
            Z.pop(uid, None)
            return

        # Read actual destination channel from user config BEFORE resolving
        _cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
        _dest_for_resolve = None
        if _cfg_chat:
            try:
                _dest_for_resolve = int(_cfg_chat.split('/')[0]) if '/' in _cfg_chat else int(_cfg_chat)
            except Exception:
                pass
        if not _dest_for_resolve:
            _dest_for_resolve = int(str(m.chat.id))  # fallback to user chat

        try:
            # Resolve source and destination peers on both clients
            # Normalize i from string to int for private channels
            _resolved_i = i
            if isinstance(i, str) and i.lstrip('-').isdigit():
                _resolved_i = int(i)
            for _cl_label, _cl in [("user_client", uc), ("bot_client", ubot)]:
                if not _cl:
                    continue
                try:
                    await _cl.resolve_peer(_resolved_i)
                except Exception:
                    pass
                try:
                    await _cl.resolve_peer(_dest_for_resolve)
                except Exception:
                    pass
        except Exception as e:
            print(f"[SINGLE] peer resolve failed (non-fatal): {e}")

        try:
            msg = await get_msg(ubot, uc, i, s, lt)
            if msg:
                # CRITICAL FIX: Always use main bot X for sending to user's chat
                res, _, _, _ = await process_msg(X, uc, msg, str(m.chat.id), lt, uid, i)
                await safe_edit(pt, f'1/1: {res}')
            else:
                await safe_edit(pt, 'Message not found. Make sure the link is correct and you have access to the channel.')
        except Exception as e:
            await safe_edit(pt, f'Error: {str(e)[:50]}')
        finally:
            Z.pop(uid, None)

    elif s == 'count':
        count = None
        input_text = m.text.strip().lower()
        
        # Check if user sent "all" — download ALL messages from start to end of channel
        if input_text == 'all':
            i_val = Z[uid].get('cid')
            start_d = Z[uid].get('sid')
            lt_val = Z[uid]['lt']
            # Try to get the latest message ID from the channel
            try:
                ubot_get = await get_ubot(uid)
                if not ubot_get:
                    ubot_get = X
                # Resolve the chat and get the last message to find the max message ID
                resolved_chat = await resolve_chat(ubot_get, i_val)
                # Get the last message in the channel
                async for last_msg in ubot_get.get_chat_history(resolved_chat, limit=1):
                    if last_msg and last_msg.id:
                        count = last_msg.id - start_d + 1
                        await safe_reply(m, f'📥 **ALL** messages selected: {count} messages (from msg {start_d} to {last_msg.id})')
                    break
                if not count:
                    await safe_reply(m, '❌ Could not determine the last message. Send a specific count or last link instead.')
                    return
            except Exception as e:
                print(f"[BATCH] Error getting channel last message: {e}")
                await safe_reply(m, f'❌ Could not read channel info. Send a specific count or last link instead.')
                return
        
        # Check if user sent a number
        elif m.text.isdigit():
            count = int(m.text)
        else:
            # Check if user sent a last link
            end_i, end_d, end_lt = E(m.text.strip())
            if end_i and end_d:
                # Verify the end link is from the same channel
                start_i = Z[uid].get('cid')
                start_d = Z[uid].get('sid')
                if str(end_i) != str(start_i):
                    await safe_reply(m, 'The last link must be from the same channel/group as the start link. Please try again.')
                    return
                if end_d < start_d:
                    await safe_reply(m, 'The last link message ID must be greater than or equal to the start link. Please try again.')
                    return
                count = end_d - start_d + 1
                await safe_reply(m, f'Calculated {count} files from start to end link.')
            else:
                await safe_reply(m, 'Please choose one:\n1️⃣ **Number** — e.g. `5000`\n2️⃣ **Last link** — send the end link\n3️⃣ **all** — download all messages')
                return
        
        # No limit for anyone — unlimited downloads
        # Removed maxlimit restriction

        # if count > maxlimit:
        #     await m.reply_text(f'Maximum limit is {maxlimit}.')
        #     return

        # ─── RESUME DETECTION ──────────────────────────────────────────
        # Check if user has a partial upload for this source channel.
        # IMPORTANT: We do NOT adjust the start position to skip ahead.
        # Instead, we always start from the original start and use
        # msg_id_map (loaded from MongoDB) to skip individually-uploaded
        # messages. This ensures FAILED messages in the middle are retried.
        
        # ── FRESH START CHECK ──
        # If the user did /clearbatch, we set a force_fresh_start flag in MongoDB
        # and add the UID to _force_fresh_start_uids. This overrides resume detection
        # and ensures a truly fresh start — no skipping of "already uploaded" messages.
        _force_fresh_start = uid in _force_fresh_start_uids
        if not _force_fresh_start:
            try:
                from plugins.verify_and_resume import batch_state_collection as _bsc_check
                bs_doc = await _bsc_check.find_one({"user_id": uid})
                if bs_doc and bs_doc.get("force_fresh_start"):
                    _force_fresh_start = True
                    # Consume the flag immediately — one-time use
                    await _bsc_check.update_one(
                        {"user_id": uid},
                        {"$unset": {"force_fresh_start": ""}}
                    )
                    print(f"[BATCH] uid={uid} — force_fresh_start flag detected in MongoDB — ignoring resume data")
            except Exception as e:
                print(f"[BATCH] uid={uid} — Could not check force_fresh_start flag: {e}")
        
        if _force_fresh_start:
            print(f"[BATCH] uid={uid} — Fresh start mode — NOT loading resume data")
            # Store in Z[uid] so _batch_streaming can check it for skip logic
            Z[uid]['force_fresh_start'] = True
            # Clean up the in-memory set flag (keep Z[uid] flag for batch loop)
            _force_fresh_start_uids.discard(uid)
        else:
            Z[uid].pop('force_fresh_start', None)  # Ensure no stale flag
        
        if not _force_fresh_start:
            try:
                i_resume = Z[uid].get('cid')
                s_resume = Z[uid].get('sid')
                resume_info = await get_upload_map_resume_info(uid, i_resume)
                if resume_info and resume_info[0] > 0:
                    last_uploaded = resume_info[0]
                    total_uploaded = resume_info[1]
                    start_msg_id_val = int(s_resume)
                    
                    if last_uploaded >= start_msg_id_val and last_uploaded < start_msg_id_val + count:
                        # User has previous upload — will resume by skipping already-uploaded msgs individually
                        # Do NOT adjust start/count — the batch loop will check msg_id_map per message
                        await safe_reply(m,
                            f'🔄 **Resume Detected!**\n\n'
                            f'📊 Found previous upload: **{total_uploaded}** messages already uploaded\n'
                            f'📍 Last uploaded source msg: **{last_uploaded}**\n'
                            f'✅ Starting from original position — already-uploaded messages will be skipped individually\n\n'
                            f'🔁 Failed messages will be **retried automatically**'
                        )
            except Exception as e:
                print(f"[RESUME] Resume detection error (non-fatal): {e}")
        else:
            # Fresh start — tell the user
            await safe_reply(m,
                '🆕 **Fresh Start** — all previous upload data cleared.\n'
                'Every message will be processed from the beginning.'
            )

        Z[uid].update({'step': 'process', 'did': str(m.chat.id), 'num': count})
        i, s, n, lt = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['num'], Z[uid]['lt']
        success = 0

        pt = await safe_reply(m, '⏳ Starting batch setup...')
        
        log_ram("batch_start", extra_info={"uid": uid, "count": n})
        
        # Step 1: Check for fetch map
        await safe_edit(pt, '⏳ Step 1/4: Checking fetch maps...')
        fetch_map = None
        try:
            from plugins.fetch import get_fetch_map
            start_msg_id_check = int(s)
            end_msg_id_check = start_msg_id_check + n - 1
            fetch_map = await get_fetch_map(uid, i, start_msg_id_check, end_msg_id_check)
            if fetch_map:
                print(f"[BATCH] Found fetch map for uid={uid} channel={i} — using STREAMING mode")
                log_ram("batch_fetch_map_found", extra_info={"uid": uid, "map_entries": len(fetch_map)})
        except Exception as e:
            print(f"[BATCH] Could not check fetch map: {e}")
            fetch_map = None
        
        # Step 2: Get user client
        await safe_edit(pt, f'⏳ Step 2/4: Connecting user client... {"(streaming mode)" if fetch_map else ""}')
        try:
            uc = await asyncio.wait_for(get_uclient(uid), timeout=90)
        except asyncio.TimeoutError:
            await safe_edit(pt, '❌ User client setup timed out. Please try again or use /login first.')
            Z.pop(uid, None)
            return
        
        # Step 3: Get bot client
        await safe_edit(pt, f'⏳ Step 3/4: Connecting bot client...')
        ubot = await get_ubot(uid)
        if not ubot:
            ubot = X
        
        if not uc:
            # If no user client, try using the global userbot as fallback
            uc = get_Y()
            if not uc:
                await safe_edit(pt, 'Missing user client. Use /login first to access channels.')
                Z.pop(uid, None)
                return
            else:
                await safe_edit(pt, 'Using global userbot (you may want to /login for private channels)...')
        
        if is_user_active(uid):
            await safe_edit(pt, 'Active task exists. Use /stop to cancel it first.')
            Z.pop(uid, None)
            return
        
        # ── Resolve source & destination chat peers on both clients ──
        # CRITICAL: This prevents CHANNEL_INVALID and PEER_ID_INVALID errors.
        # Must happen AFTER clients are connected but BEFORE any batch processing.
        _cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
        _dest_for_resolve = None
        if _cfg_chat:
            try:
                _dest_for_resolve = int(_cfg_chat.split('/')[0]) if '/' in _cfg_chat else int(_cfg_chat)
            except Exception:
                pass
        
        await safe_edit(pt, '\u23f3 Resolving chat peers...')
        try:
            await resolve_peers_at_startup(uc, ubot, i, _dest_for_resolve)
        except Exception as e:
            await safe_edit(pt, f'\u274c Failed to resolve source channel: {e}')
            Z.pop(uid, None)
            return
        
        # ═══════════════════════════════════════════════════════════════
        # STREAMING MODE (if fetch map exists)
        # Skip the heavy pre-fetch and process messages one-by-one
        # ═══════════════════════════════════════════════════════════════
        if fetch_map:
            await safe_edit(pt, f'✅ Step 4/4: Streaming {n} messages one-by-one (saves ~50-100MB RAM)')
            # Jump to streaming batch processing
            await _batch_streaming(c, m, uid, i, s, n, lt, ubot, uc, pt, fetch_map)
            return
        
        # ═══════════════════════════════════════════════════════════════
        # ORIGINAL MODE: Pre-fetch all messages (memory-heavy but reliable)
        # ═══════════════════════════════════════════════════════════════
        await safe_edit(pt, f'⏳ Step 4/4: Pre-fetching {n} messages (no fetch map)...')
        log_ram("batch_prefetch_start", extra_info={"uid": uid, "count": n})
        
        # Pre-fetch all messages — use chunk-based fetching (100 per API call)
        # instead of one-by-one which is 50x slower for large batches
        start_msg_id = int(s)
        end_msg_id = start_msg_id + n - 1
        message_ids = list(range(start_msg_id, end_msg_id + 1))
        messages_data = []  # List of (mid, msg)
        fetch_errors = 0
        
        await safe_edit(pt, f'Pre-fetching messages... (0/{n})')
        
        # Determine which client can access the chat for batch fetching
        fetch_client = None
        resolved_fetch_chat = None
        
        if lt == 'public':
            # For public channels, try bot first then userbot
            try:
                resolved_fetch_chat = await resolve_chat(ubot, i)
                test_msg = await ubot.get_messages(resolved_fetch_chat, start_msg_id)
                if test_msg and not getattr(test_msg, 'empty', False):
                    fetch_client = ubot
                    emp[i] = False
            except ChannelPrivate as e:
                print(f"[PREFETCH] Bot: ChannelPrivate for chat={i}: {e}")
            except Exception as e:
                print(f"[PREFETCH] Bot test fetch failed for chat={i}: {e}")
            
            if not fetch_client and uc:
                try:
                    resolved_fetch_chat = await resolve_chat(uc, i)
                    test_msg = await uc.get_messages(resolved_fetch_chat, start_msg_id)
                    if test_msg and not getattr(test_msg, 'empty', False):
                        fetch_client = uc
                except ChannelPrivate as e:
                    print(f"[PREFETCH] User client: ChannelPrivate for chat={i}: {e}")
                except Exception as e:
                    print(f"[PREFETCH] User client test fetch failed for chat={i}: {e}")
        
        elif lt == 'private' and uc:
            # For private channels, use userbot with resolved peer
            chat_id_int = int(i) if isinstance(i, str) and i.lstrip('-').isdigit() else i
            try:
                await uc.resolve_peer(chat_id_int)
                test_msg = await uc.get_messages(chat_id_int, start_msg_id)
                if test_msg and not getattr(test_msg, 'empty', False):
                    fetch_client = uc
                    resolved_fetch_chat = chat_id_int
            except ChannelPrivate as e:
                print(f"[PREFETCH] Private: ChannelPrivate for chat={chat_id_int}: {e}")
            except Exception as e:
                print(f"[PREFETCH] Private test fetch failed for chat={chat_id_int}: {e}")
            
            if not fetch_client:
                # Try with dialog refresh
                try:
                    async for _ in uc.get_dialogs(limit=200): pass
                except Exception as e:
                    print(f"[PREFETCH] Dialog refresh failed: {e}")
                try:
                    await uc.resolve_peer(chat_id_int)
                    test_msg = await uc.get_messages(chat_id_int, start_msg_id)
                    if test_msg and not getattr(test_msg, 'empty', False):
                        fetch_client = uc
                        resolved_fetch_chat = chat_id_int
                except ChannelPrivate as e:
                    print(f"[PREFETCH] Private after dialog refresh: ChannelPrivate for chat={chat_id_int}: {e}")
                except Exception as e:
                    print(f"[PREFETCH] Private after dialog refresh failed for chat={chat_id_int}: {e}")
        
        # Fetch in chunks of 100 (Telegram API limit per call)
        if fetch_client and resolved_fetch_chat is not None:
            for chunk_start in range(0, len(message_ids), 100):
                chunk_ids = message_ids[chunk_start:chunk_start + 100]
                try:
                    results = await fetch_client.get_messages(resolved_fetch_chat, chunk_ids)
                    if not isinstance(results, list):
                        results = [results]
                    for msg in results:
                        if msg and not getattr(msg, 'empty', False):
                            messages_data.append((msg.id, msg))
                        else:
                            fetch_errors += 1
                except Exception as e:
                    print(f'Batch chunk fetch error ({chunk_ids[0]}-{chunk_ids[-1]}): {e}')
                    # Check for fatal auth key error
                    if _is_auth_key_error(e):
                        print(f"[BATCH] ⚠️ FATAL: AUTH_KEY_UNREGISTERED — session is dead!")
                        try:
                            await pt.edit(
                                '🔴 **Session Expired — Batch Stopped**\n\n'
                                'Your Telegram session has been revoked by Telegram.\n'
                                'Use `/logout` then `/login` to create a new session, then restart the batch.'
                            )
                        except Exception:
                            pass
                        await remove_active_batch(uid)
                        try:
                            from scheduler import scheduler
                            scheduler.unregister(uid)
                        except Exception:
                            pass
                        return
                    # Fall back to individual fetch for this chunk
                    for mid in chunk_ids:
                        try:
                            msg = await get_msg(ubot, uc, i, mid, lt)
                            if msg:
                                messages_data.append((mid, msg))
                            else:
                                fetch_errors += 1
                        except AuthKeyUnregisteredError:
                            print(f"[BATCH] ⚠️ FATAL: AUTH_KEY_UNREGISTERED during pre-fetch — stopping!")
                            try:
                                await pt.edit(
                                    '🔴 **Session Expired — Batch Stopped**\n\n'
                                    'Your Telegram session has been revoked by Telegram.\n'
                                    'Use `/logout` then `/login` to create a new session, then restart the batch.'
                                )
                            except Exception:
                                pass
                            await remove_active_batch(uid)
                            try:
                                from scheduler import scheduler
                                scheduler.unregister(uid)
                            except Exception:
                                pass
                            return
                        except Exception:
                            fetch_errors += 1
                
                fetched_so_far = min(chunk_start + 100, len(message_ids))
                try:
                    await pt.edit(f'Pre-fetching messages... ({fetched_so_far}/{n}) found: {len(messages_data)}')
                except Exception:
                    pass
        else:
            # Fallback: one-by-one fetch using get_msg (slower but works for all cases)
            for j in range(n):
                mid = start_msg_id + j
                try:
                    msg = await get_msg(ubot, uc, i, mid, lt)
                    if msg:
                        messages_data.append((mid, msg))
                    else:
                        fetch_errors += 1
                except FloodWait as e:
                    wait_secs = e.value if hasattr(e, 'value') else 30
                    # ANY FloodWait → stop batch immediately
                    print(f'[FLOOD] Pre-fetch FloodWait {_format_duration(wait_secs)} — stopping batch')
                    await _flood_wait_stop(uid, wait_secs, user_chat_id=m.chat.id)
                    return
                except AuthKeyUnregisteredError:
                    print(f"[BATCH] ⚠️ FATAL: AUTH_KEY_UNREGISTERED during pre-fetch — stopping!")
                    try:
                        await pt.edit(
                            '🔴 **Session Expired — Batch Stopped**\n\n'
                            'Your Telegram session has been revoked by Telegram.\n'
                            'Use `/logout` then `/login` to create a new session, then restart the batch.'
                        )
                    except Exception:
                        pass
                    await remove_active_batch(uid)
                    try:
                        from scheduler import scheduler
                        scheduler.unregister(uid)
                    except Exception:
                        pass
                    return
                except Exception as e:
                    fetch_errors += 1
                    print(f'Pre-fetch error for msg {mid}: {e}')
                
                if (j + 1) % 5 == 0 or j + 1 == n:
                    try:
                        await pt.edit(f'Pre-fetching messages... ({j+1}/{n}) found: {len(messages_data)}')
                    except Exception:
                        pass
        
        log_ram("batch_prefetch_done", extra_info={"uid": uid, "messages": len(messages_data), "errors": fetch_errors})
        
        # Check if we got any messages at all
        if not messages_data:
            # Determine likely reason from fetch_errors count
            if fetch_errors == n:
                hint = (
                    f'Could not fetch ANY messages from the channel ({fetch_errors}/{n} failed).\n\n'
                    f'Possible reasons:\n'
                    f'• You are not a member of this channel — check "ChannelPrivate" in logs\n'
                    f'• The channel has been deleted or made private\n'
                    f'• Your userbot session may need /login to access private channels\n'
                    f'• The link may be incorrect\n\n'
                    f'Check server logs for [GET_MSG] or [PREFETCH] errors for details.'
                )
            else:
                hint = (
                    f'Could not fetch any messages from the channel.\n\n'
                    f'Make sure:\n'
                    f'• The link is correct\n'
                    f'• You have access to the channel\n'
                    f'• Use /login if it is a private channel\n\n'
                    f'Errors: {fetch_errors}/{n} messages could not be fetched.'
                )
            await safe_edit(pt, hint)
            Z.pop(uid, None)
            return
        
        # Load upload map from MongoDB (survives restarts)
        msg_id_map, last_uploaded_id, stored_dest_channel = await load_upload_map(uid, str(i))
        
        # IMPORTANT: After /clearbatch, last_uploaded_id is reset to 0 but old mappings
        # may still be in MongoDB. If last_uploaded_id == 0, this is a FRESH START —
        # don't use old mappings for skip detection.
        if last_uploaded_id == 0 and msg_id_map:
            print(f"[BATCH] Fresh start detected (last_uploaded_id=0) — clearing {len(msg_id_map)} old mappings from skip-detection map")
            msg_id_map = {}
        
        initial_msg_id_keys = set(msg_id_map.keys())  # Track pre-existing mappings to avoid double-counting on flush
        # Save upload map after every message — guarantees zero data loss on crash
        
        # Resolve destination channel info for link rewriting
        # This lets us rewrite Telegram links in messages to point to destination
        dest_channel_id_int = None
        dest_channel_username = None
        try:
            cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
            if cfg_chat:
                if '/' in cfg_chat:
                    dest_channel_id_int = int(cfg_chat.split('/')[0])
                else:
                    dest_channel_id_int = int(cfg_chat)
            else:
                # No configured chat_id — fall back to user's chat, but only if it's a channel (negative ID).
                # Positive IDs are user chats which can't have navigable links.
                _fallback_id = int(str(m.chat.id)) if str(m.chat.id).lstrip('-').isdigit() else None
                if _fallback_id and _fallback_id < 0:
                    dest_channel_id_int = _fallback_id
                else:
                    dest_channel_id_int = None
        except Exception:
            pass
        
        if dest_channel_id_int:
            try:
                dest_chat = await ubot.get_chat(dest_channel_id_int)
                dest_channel_username = getattr(dest_chat, 'username', None)
            except Exception:
                # Bot can't access dest chat — try user client
                try:
                    if uc:
                        dest_chat = await uc.get_chat(dest_channel_id_int)
                        dest_channel_username = getattr(dest_chat, 'username', None)
                except Exception:
                    dest_channel_username = None
        
        # Pre-flight validation: Bot MUST be able to resolve destination channel
        if dest_channel_id_int:
            _dest_ok = False
            for _client_label, _client in [("ubot", ubot), ("user_client", uc)]:
                if not _client:
                    continue
                try:
                    await _client.resolve_peer(dest_channel_id_int)
                    print(f"[DEST-VALIDATE] {_client_label}: dest channel {dest_channel_id_int} resolved OK")
                    _dest_ok = True
                    break
                except Exception as e:
                    print(f"[DEST-VALIDATE] {_client_label}: dest channel {dest_channel_id_int} FAILED: {e}")
            
            if not _dest_ok:
                err_msg = (f"❌ Destination channel `{dest_channel_id_int}` is INVALID or bot lacks access!\n\n"
                           f"Please check:\n"
                           f"• The channel ID is correct (should start with -100)\n"
                           f"• The bot is added as admin in the destination channel\n"
                           f"• The channel hasn't been deleted\n\n"
                           f"Use /setchat to configure the correct destination channel.")
                await safe_edit(pt, err_msg)
                Z.pop(uid, None)
                return
        else:
            print(f"[DEST-VALIDATE] No dest_channel_id_int configured — user chat will be used as destination")
        
        # Resolve source channel username for link rewriting
        # If source_channel (i) is an integer ID, messages may still contain
        # links using the public username format. We need both to rewrite all links.
        # IMPORTANT: i from E() is a string like '-1001234567890' for private channels.
        # Pyrogram's get_chat() needs an integer for private channels.
        source_channel_username = None
        source_channel_id_int = None  # Numeric ID for DUAL-FORMAT link matching
        try:
            src_chat_info = None
            # Convert string channel ID to int for Pyrogram API calls
            resolved_i = i
            try:
                if isinstance(i, str) and i.lstrip('-').isdigit():
                    resolved_i = int(i)
                    source_channel_id_int = resolved_i  # Already have numeric ID
            except Exception:
                pass
            if uc:
                try:
                    src_chat_info = await uc.get_chat(resolved_i)
                except Exception:
                    pass
            if not src_chat_info and ubot:
                try:
                    src_chat_info = await ubot.get_chat(resolved_i)
                except Exception:
                    pass
            if src_chat_info:
                source_channel_username = getattr(src_chat_info, 'username', None)
                # Get the numeric ID from chat info (important for public channels)
                if not source_channel_id_int:
                    source_channel_id_int = getattr(src_chat_info, 'id', None)
                if source_channel_username:
                    print(f"[LINK-REWRITE] Source channel username resolved: @{source_channel_username}")
                if source_channel_id_int:
                    print(f"[LINK-REWRITE] Source channel numeric ID resolved: {source_channel_id_int}")
        except Exception:
            source_channel_username = None
        
        # ═══════════════════════════════════════════════════════════════
        # MULTI-SOURCE: Build list of ALL source channels for cross-channel
        # link rewriting. A message from channel A can have links to B, C.
        # ═══════════════════════════════════════════════════════════════
        _multi_src_channels = None
        _combined_msg_id_map = None
        try:
            _resolve_client = uc or ubot
            _multi_src_channels, _combined_msg_id_map = await build_multi_source_channels(
                uid, i,
                primary_username=source_channel_username,
                primary_numeric_id=source_channel_id_int,
                client=_resolve_client,
            )
            if _multi_src_channels and len(_multi_src_channels) > 1:
                print(f"[MULTI-SRC] Cross-channel rewriting enabled: {len(_multi_src_channels)} source channels")
            else:
                _multi_src_channels = None  # Only 1 channel — no need for multi-source
        except Exception as e:
            print(f"[MULTI-SRC] Failed to build multi-source channels (non-fatal): {e}")
            _multi_src_channels = None

        # ── SIMPLEREWITER: Create and load all mappings ──
        _rewriter = None
        try:
            from plugins.simple_rewriter import SimpleRewriter as _SR
            _db = _upload_db
            if _db is not None and dest_channel_id_int:
                _rewriter = _SR(
                    uid=uid,
                    source_channel=str(i),
                    dst_chat_id=dest_channel_id_int,
                    db=_db,
                    bot_client=X,
                    ubot=ubot,
                    source_channel_username=source_channel_username,
                    dst_channel_username=dest_channel_username,
                    dst_channel_id=dest_channel_id_int,
                )
                # Add multi-source channels
                if _multi_src_channels:
                    for ch_info in _multi_src_channels:
                        ch = ch_info.get("channel", "")
                        ch_username = ch_info.get("username")
                        ch_numeric_id = ch_info.get("numeric_id")
                        if ch and ch != str(i):
                            _rewriter.add_source_channel(ch, ch_username, ch_numeric_id)
                await _rewriter.load()
                # Merge loaded mappings into msg_id_map (rewriter has more mappings)
                for _k, _v in _rewriter.map.items():
                    if _k not in msg_id_map:
                        msg_id_map[_k] = _v
                print(f"[REWRITER] SimpleRewriter loaded {len(_rewriter.map)} mappings, {_rewriter._src_patterns.__len__()} source patterns")
            else:
                print(f"[REWRITER] Skipped — no MongoDB or no dest_channel_id_int")
        except Exception as _sr_err:
            print(f"[REWRITER] SimpleRewriter init failed (non-fatal): {_sr_err}")
            _rewriter = None

        # ═══════════════════════════════════════════════════════════════
        # RESOLVE PENDING LINK REWRITES from previous stopped batches
        # Runs BEFORE any new messages are processed — never skipped.
        # ═══════════════════════════════════════════════════════════════
        try:
            await resolve_pending_link_rewrites(
                bot_client=X, ubot=ubot, source_channel=i,
                dest_channel_id_int=dest_channel_id_int,
                dest_channel_username=dest_channel_username,
                source_channel_username=source_channel_username,
                uid=uid,
                source_channel_id=source_channel_id_int,
                multi_source_channels=_multi_src_channels,
                combined_msg_id_map=_combined_msg_id_map,
            )
        except Exception as e:
            print(f"[LINK-REWRITE-RESUME] Pre-batch resolve failed (non-fatal): {e}")

        # No longer needed — replaced by MongoDB-backed unresolved_links tracking
        # Format: [(dest_channel_id, dest_msg_id), ...]
        messages_needing_link_update = []
        
        # Track failed links for end-of-batch TXT report
        failed_links = []
        batch_start_time = time.time()
        last_progress_edit = 0  # Throttle progress edits
        
        # ─── PIN MAP: Pre-scan ALL pinned messages BEFORE loop (1-2 API calls total)
        _pin_map = {}
        try:
            _pin_map = await startup_pin(user_client=uc, src_chat_id=i, fetch_map={})
            print(f"[PIN] Loaded pin_map with {len(_pin_map)} entries")
        except Exception as e:
            print(f"[PIN] Could not load pin_map: {e}")
        
        # Pre-load explanations into memory cache
        try:
            from plugins.explanation_listener import add_monitored_channel
            await add_monitored_channel(i, uid, client=uc or ubot)
        except Exception as e:
            print(f"[EXPLANATION] Could not pre-load explanations: {e}")
        
        await add_active_batch(uid, {
            "total": n,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": pt.id if pt else 0
            })
        
        # Register with FloodWaitScheduler for auto-resume on FloodWait
        from scheduler import scheduler
        try:
            _user_chat_id = m.chat.id
        except Exception:
            _user_chat_id = None
        scheduler.register(uid, resume_fn=_resume_batch,
            resume_kwargs=dict(uid=uid, i=i, s=s, n=n, lt=lt, user_chat_id=_user_chat_id))
        
        # Save batch state to MongoDB for crash recovery
        try:
            await save_batch_state(uid, str(i), int(s), n, dest_channel_id_int, lt)
        except Exception as e:
            print(f"[BATCH-STATE] Failed to save batch state: {e}")
        
        # Start heartbeat — keeps batch_state.updated_at fresh every 30 seconds
        _heartbeat_task2 = asyncio.create_task(batch_checkpoint_heartbeat(uid, str(i)))
        
        # Register this batch as an asyncio.Task so /stop can cancel it immediately
        current_task = asyncio.current_task()
        if current_task:
            batch_tasks[uid] = current_task
        
        # ═══════════════════════════════════════════════════════════════
        # PERF CACHE: Resolve per-message data ONCE before the loop starts.
        # ═══════════════════════════════════════════════════════════════
        _s_cached_tcid = None
        _s_cached_topic_id = None
        _s_cached_rtmid = None
        _s_cached_watermark = None
        _s_cached_caption = None
        _s_cached_source_name = None

        try:
            cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
            tcid_cache = int(m.chat.id)
            topic_id_cache = None
            rtmid_cache = None
            if cfg_chat:
                if '/' in cfg_chat:
                    parts = cfg_chat.split('/', 1)
                    tcid_cache = int(parts[0])
                    topic_id_cache = int(parts[1]) if len(parts) > 1 else None
                    # Do NOT set rtmid_cache = topic_id_cache — reply_to is per-message
                    rtmid_cache = None
                else:
                    tcid_cache = int(cfg_chat)
            _s_cached_tcid = tcid_cache
            _s_cached_topic_id = topic_id_cache
            _s_cached_rtmid = rtmid_cache
            try:
                await X.resolve_peer(_s_cached_tcid)
            except Exception:
                pass
            print(f"[PERF-CACHE] Original: cached tcid={_s_cached_tcid}, topic_id={_s_cached_topic_id}")
        except Exception as _cache_err:
            print(f"[PERF-CACHE] Original tcid cache failed: {_cache_err}")

        try:
            _s_cached_watermark = await get_user_data_key(uid, 'watermark_text', _DEFAULT_WATERMARK_TEXT)
            print(f"[PERF-CACHE] Original: cached watermark text")
        except Exception:
            _s_cached_watermark = _DEFAULT_WATERMARK_TEXT

        try:
            _s_cached_caption = await get_user_data_key(uid, 'caption_text', '')
            print(f"[PERF-CACHE] Original: cached caption text")
        except Exception:
            _s_cached_caption = ''

        try:
            _resolve_client = uc or ubot or X
            _src_chat_info = None
            _resolved_i = i
            try:
                if isinstance(i, str) and i.lstrip('-').isdigit():
                    _resolved_i = int(i)
            except Exception:
                pass
            try:
                _src_chat_info = await _resolve_client.get_chat(_resolved_i)
            except Exception:
                pass
            if _src_chat_info and getattr(_src_chat_info, 'title', None):
                _s_cached_source_name = _src_chat_info.title
                print(f"[PERF-CACHE] Original: cached source_name={_s_cached_source_name}")
        except Exception:
            _s_cached_source_name = ''

        j = 0  # Track iteration for CancelledError handler
        try:
            for j in range(n):
                
                if should_cancel(uid):
                    # Flush whatever new mappings we have on cancel
                    new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
                    if new_mappings:
                        try:
                            last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                            await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                            print(f"[STOP] Flushed {len(new_mappings)} new mappings on cancel (original)")
                        except Exception as e:
                            print(f"[STOP] Failed to flush mappings on cancel: {e}")
                    # Build link for the current position so user can continue later
                    current_msg_id = int(s) + j
                    if lt == 'private':
                        channel_id_clean = str(i).replace('-100', '')
                        continue_link = f"https://t.me/c/{channel_id_clean}/{current_msg_id}"
                    else:
                        continue_link = f"https://t.me/{i}/{current_msg_id}"
                    
                    await safe_edit(pt,
                        f'Cancelled at {j+1}/{n}. Success: {success}\n\n'
                        f'**Continue from here next time:**\n`{continue_link}`'
                    )
                    # Send failed links as TXT file if any exist before cancellation
                    if failed_links:
                        try:
                            failed_file_path = f"failed_links_{uid}_{int(time.time())}.txt"
                            with open(failed_file_path, 'w', encoding='utf-8') as f:
                                f.write(f"Failed Links Report (Cancelled Batch)\n")
                                f.write(f"====================================\n")
                                f.write(f"Batch cancelled at {j+1}/{n}. Success: {success}\n")
                                f.write(f"Failed so far: {len(failed_links)}\n\n")
                                f.write(f"Links:\n")
                                f.write(f"------\n")
                                for idx, fl in enumerate(failed_links, 1):
                                    f.write(f"{idx}. {fl}\n")
                            await m.reply_document(failed_file_path, caption=f'❌ Failed links so far ({len(failed_links)})')
                            os.remove(failed_file_path)
                        except Exception as e:
                            print(f"Failed to send failed links file on cancel: {e}")
                    break
                
                await update_batch_progress(uid, j, success)
                
                mid = int(s) + j
                
                # Skip already-uploaded messages (resume detection)
                if not Z.get(uid, {}).get('force_fresh_start') and mid in msg_id_map:
                    # Already uploaded — skip (log occasionally)
                    if j % 100 == 0:
                        print(f"[BATCH-SKIP] msg_id={mid} — already uploaded (dest_id={msg_id_map[mid]}) — resuming from previous batch")
                    continue
                
                # Find message from pre-fetched data (2-tuple: mid, msg)
                src_msg = None
                for item in messages_data:
                    if item[0] == mid:
                        src_msg = item[1]
                        break
                
                if not src_msg:
                    # Message was not fetched during pre-fetch — try fetching it now
                    print(f"[BATCH] Pre-fetch missed msg {mid} — fetching on-demand...")
                    for retry_attempt in range(3):
                        try:
                            # Rate-limit source channel reads
                            _src_chat_int = int(i) if isinstance(i, str) and i.lstrip('-').isdigit() else hash(i)
                            await _rate_limiter.acquire(_src_chat_int)
                            src_msg = await get_msg(ubot, uc, i, mid, lt)
                            if src_msg:
                                break
                        except AuthKeyUnregisteredError:
                            print(f"[BATCH] ⚠️ FATAL: AUTH_KEY_UNREGISTERED — session is dead! Stopping batch.")
                            try:
                                await pt.edit(
                                    '🔴 **Session Expired — Batch Stopped**\n\n'
                                    'Your Telegram session has been revoked by Telegram.\n'
                                    'Use `/logout` then `/login` to create a new session, then restart the batch.'
                                )
                            except Exception:
                                pass
                            await remove_active_batch(uid)
                            try:
                                from scheduler import scheduler
                                scheduler.unregister(uid)
                            except Exception:
                                pass
                            return
                        except Exception as e:
                            print(f"[BATCH] On-demand fetch attempt {retry_attempt+1}/3 failed for msg {mid}: {e}")
                            await asyncio.sleep(3)
                    
                    if not src_msg:
                        # Build the link for this failed message
                        print(f"[BATCH-DEBUG] msg_id={mid} — get_msg returned None (not found or empty)")
                        if lt == 'private':
                            failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                        else:
                            failed_link = f"https://t.me/{i}/{mid}"
                        failed_links.append(f"{failed_link} — skipped (not fetched after 3 retries)")
                        try:
                            await pt.edit(f'Processing {j+1}/{n} — Success: {success} (skipped unfetched)')
                        except Exception:
                            pass
                        continue
                
                # Debug: Log every fetched message type for visibility
                _msg_type = 'unknown'
                if src_msg.poll:
                    _msg_type = 'poll'
                elif src_msg.photo:
                    _msg_type = 'photo'
                elif src_msg.video:
                    _msg_type = 'video'
                elif src_msg.document:
                    _msg_type = 'document'
                elif src_msg.audio:
                    _msg_type = 'audio'
                elif src_msg.animation:
                    _msg_type = 'animation'
                elif src_msg.voice:
                    _msg_type = 'voice'
                elif src_msg.video_note:
                    _msg_type = 'video_note'
                elif src_msg.sticker:
                    _msg_type = 'sticker'
                elif src_msg.text:
                    _msg_type = 'text'
                elif src_msg.media:
                    _msg_type = f'media({type(src_msg.media).__name__})'
                elif src_msg.service:
                    _msg_type = 'service'
                elif src_msg.empty:
                    _msg_type = 'empty'
                elif src_msg.forward_origin:
                    _msg_type = 'forwarded(no_content)'
                _has_text = bool(src_msg.text)
                _has_caption = bool(getattr(src_msg, 'caption', None))
                _has_media = bool(src_msg.media)
                _src_reply_id = _get_reply_to_id(src_msg)
                print(f"[BATCH-DEBUG] msg_id={mid} type={_msg_type} text={_has_text} caption={_has_caption} media={_has_media} reply_to_msg_id={src_msg.reply_to_message_id} reply_to_robust={_src_reply_id}")

                # Preserve original reply chain — use robust _get_reply_to_id()
                # to handle Pyrofork's different reply attribute locations
                # Check both current batch range AND persistent map for cross-batch replies
                reply_to_dest_id = None
                if _src_reply_id:
                    reply_to_dest_id = msg_id_map.get(_src_reply_id)
                
                try:
                    # CRITICAL FIX: Always use main bot X for sending to user's chat
                    res, dest_id, _, had_unresolved = await process_msg(X, uc, src_msg, str(m.chat.id), lt, uid, i,
                        reply_to_destination_id=reply_to_dest_id,
                        link_rewrite_map=msg_id_map,
                        dest_channel_id=dest_channel_id_int,
                        dest_channel_username=dest_channel_username,
                        source_channel_username=source_channel_username,
                        source_channel_id=source_channel_id_int,
                        multi_source_channels=_multi_src_channels,
                        rewriter=_rewriter,
                        _cached_tcid=_s_cached_tcid,
                        _cached_topic_id=_s_cached_topic_id,
                        _cached_rtmid=_s_cached_rtmid,
                        _cached_watermark=_s_cached_watermark,
                        _cached_caption=_s_cached_caption,
                        _cached_source_name=_s_cached_source_name,
                        _skip_verify=True,
                        _skip_explanation_scan=True)
                    # Debug: Log process_msg result for non-poll messages
                    if not src_msg.poll:
                        print(f"[BATCH-DEBUG] msg_id={mid} type={_msg_type} result='{res}' dest_id={dest_id}")
                    if dest_id:
                        msg_id_map[mid] = dest_id
                        # ── SIMPLEREWITER: Record mapping ──
                        if _rewriter is not None:
                            try:
                                await _rewriter.record(mid, dest_id)
                            except Exception:
                                pass
                        # 🔑 FINGERPRINT: Store fingerprint for relink resolution (non-blocking)
                        try:
                            from plugins.relink import checkpoint_with_fingerprint
                            asyncio.create_task(checkpoint_with_fingerprint(uid, str(i), mid, dest_id, src_msg))
                        except Exception:
                            pass
                        # 🗂️ SMART CACHE: Index source links for instant /relink queries
                        if dest_channel_id_int:
                            try:
                                _src_text = str(src_msg.text) if src_msg.text else (str(src_msg.caption) if src_msg.caption else '')
                                _src_ents = src_msg.entities if src_msg.entities else (src_msg.caption_entities if src_msg.caption_entities else None)
                                asyncio.create_task(cache_message_for_relink(
                                    uid, str(i), dest_channel_id_int, dest_id, mid,
                                    _src_text, _src_ents,
                                    source_channel_username=source_channel_username,
                                    source_channel_id=source_channel_id_int,
                                    multi_source_channels=_multi_src_channels
                                ))
                            except Exception:
                                pass
                        
                        # Track forward references (reply_to not yet uploaded)
                        if src_msg.reply_to_message_id and src_msg.reply_to_message_id not in msg_id_map:
                            if dest_channel_id_int:
                                asyncio.create_task(add_pending_reply(uid, str(i), dest_channel_id_int, dest_id, src_msg.reply_to_message_id))
                        # Track messages with unresolved links — write to MongoDB (survives crashes/stops)
                        if had_unresolved:
                            asyncio.create_task(mark_needs_link_update(uid, str(i), dest_channel_id_int, dest_id, mid))
                        
                        # 📌 PIN: Non-blocking — fire and forget (pins are rare)
                        if dest_channel_id_int:
                            try:
                                asyncio.create_task(handle_pin_mirror(X, uc, i, mid, src_msg, dest_channel_id_int, dest_id, _pin_map))
                            except Exception as e:
                                print(f"[PIN] handle_pin_mirror failed for msg {mid}: {e}")
                        # ✅ VERIFY: DISABLED per-message — too slow (1 API call per msg)
                        # post_batch_verify() runs at batch end for bulk verification.
                        # 🔗 AUTO-RELINK: Non-blocking — fire and forget
                        if dest_channel_id_int and dest_id:
                            try:
                                from plugins.relink import on_new_mirror_message
                                asyncio.create_task(on_new_mirror_message(uid, str(i), mid, dest_id, dest_channel_id_int))
                            except Exception as e:
                                pass  # Never let auto-relink break mirroring
                    if 'Done' in res or 'Copied' in res or 'Sent' in res or 'Forwarded' in res or 'forwarded' in res:
                        success += 1
                    else:
                        # Message was processed but failed
                        if lt == 'private':
                            failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                        else:
                            failed_link = f"https://t.me/{i}/{mid}"
                        failed_links.append(f"{failed_link} — {res}")
                    
                    # ── Check cancel flag after each message ──
                    if should_cancel(uid):
                        print(f"[STOP] Cancel flag detected after msg {mid} — stopping batch (original)")
                        raise asyncio.CancelledError()
                    
                except FloodWait as e:
                    # ANY FloodWait during upload → stop batch immediately
                    wait_secs = e.value if hasattr(e, 'value') else 30
                    print(f"[FLOOD] Upload FloodWait {_format_duration(wait_secs)} — stopping batch")
                    await _flood_wait_stop(uid, wait_secs, user_chat_id=m.chat.id)
                    return
                except Exception as e:
                    # CRITICAL: FloodWait must be caught here, NOT treated as regular error.
                    if isinstance(e, FloodWait):
                        wait_secs = e.value if hasattr(e, 'value') else 30
                        print(f"[FLOOD] Uncaught FloodWait {_format_duration(wait_secs)} at msg {mid} — stopping batch")
                        await _flood_wait_stop(uid, wait_secs, user_chat_id=m.chat.id)
                        return
                    
                    # Exception during processing — track as failed
                    if lt == 'private':
                        failed_link = f"https://t.me/c/{str(i).replace('-100', '')}/{mid}"
                    else:
                        failed_link = f"https://t.me/{i}/{mid}"
                    failed_links.append(f"{failed_link} — Error: {str(e)[:80]}")
                    print(f"[BATCH] Error processing msg {mid}: {e}")
                    try: await pt.edit(f'{j+1}/{n}: Error - {str(e)[:50]}')
                    except: pass
                
                # Update progress with visual bar (throttled: every 50 msgs OR every 30s, prevents FloodWait)
                now = time.time()
                if (j + 1) % 50 == 0 or now - last_progress_edit >= 30 or j + 1 == n:
                    elapsed = now - batch_start_time
                    pct = min((j + 1) * 100 // n, 100)
                    rate = (j + 1) / elapsed if elapsed > 0 else 0
                    remaining = (n - j - 1) / rate if rate > 0 else 0
                    filled = pct // 10
                    bar = '🟢' * filled + '⚪' * (10 - filled)
                    if remaining > 60:
                        eta_str = f'{int(remaining // 60)}m {int(remaining % 60)}s'
                    else:
                        eta_str = f'{int(remaining)}s'
                    try:
                        await safe_edit(pt,
                            f'📦 **Batch Progress**\n\n'
                            f'{bar}  **{pct}%**\n\n'
                            f'✅ Done: **{j+1}**/{n} | Success: **{success}**\n'
                            f'⚡ Rate: **{rate:.2f} msgs/min**\n'
                            f'⏳ ETA: **{eta_str}**\n'
                            f'⏱️ Elapsed: {elapsed:.0f}s'
                        )
                        last_progress_edit = now
                    except Exception:
                        pass
                
                await asyncio.sleep(BATCH_SEND_DELAY)
                
                # Cooldown pause every N messages to prevent sustained-rate FloodWait
                await _batch_cooldown_check(j, uid)
                
                # Post-sleep RAM reclaim: by now Pyrogram has released its internal session
                # buffers from the download/upload. malloc_trim can return these to the OS.
                _ram_reclaim()
            
            # ─── INCREMENTAL MAP SAVE for ORIGINAL batch (every 100 msgs) ───
            if success > 0 and success % 100 == 0 and dest_channel_id_int and j + 1 < n:
                try:
                    new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
                    if new_mappings:
                        last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                        await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                        initial_msg_id_keys = set(msg_id_map.keys())
                        print(f"[BATCH-SAVE-ORIG] Incremental save at {success} msgs — {len(msg_id_map)} total mappings")
                except Exception as e:
                    print(f"[BATCH-SAVE-ORIG] Incremental save failed: {e}")
            
            if j+1 == n:
                # Single flush — write NEW mappings to MongoDB at once
                new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
                if new_mappings:
                    try:
                        last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                        await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                        print(f"[BATCH-END] Flushed {len(new_mappings)} new mappings to MongoDB ({len(msg_id_map)} total)")
                    except Exception as e:
                        print(f"[BATCH-END] Failed to flush mappings: {e}")
                
                # Build the last message link so user can continue next time
                last_msg_id = start_msg_id + n - 1
                if lt == 'private':
                    # Private: https://t.me/c/{id_without_-100}/{msg_id}
                    channel_id_clean = str(i).replace('-100', '')
                    last_link = f"https://t.me/c/{channel_id_clean}/{last_msg_id}"
                else:
                    # Public: https://t.me/{username}/{msg_id}
                    last_link = f"https://t.me/{i}/{last_msg_id}"
                
                # Mark batch complete in MongoDB
                try:
                    await mark_batch_complete(uid, str(i))
                except Exception as e:
                    print(f"[BATCH-STATE] Failed to mark batch complete: {e}")
                
                # Cancel heartbeat — batch is done
                try:
                    _heartbeat_task2.cancel()
                except Exception:
                    pass
                
                # Clear rate limiter state for this chat
                if dest_channel_id_int:
                    _rate_limiter.clear(dest_channel_id_int)
                _download_rate_limiter.clear()
                
                # 📌 PIN SYNC: Ensure ALL source pins are pinned in dest
                # Use uc (user client) if available, else fall back to ubot
                _pin_client = uc or ubot
                if _pin_client and dest_channel_id_int and msg_id_map:
                    try:
                        pin_result = await verify_and_sync_pins(
                            user_client=_pin_client, bot_client=X,
                            src_chat_id=i, dst_chat_id=dest_channel_id_int,
                            msg_id_map=msg_id_map,
                            uid=uid, source_channel=str(i),
                        )
                        print(f"[PIN-SYNC] Result: source_pins={pin_result.get('total_source_pins',0)} mapped={pin_result.get('mapped_pins',0)} newly_pinned={pin_result.get('newly_pinned',0)} failed={len(pin_result.get('failed_to_pin',[]))} not_in_map={len(pin_result.get('not_in_map',[]))}")
                        # If pins still failed after trying both user_client and bot_client,
                        # try with ubot as last resort (only if ubot wasn't already used as _pin_client)
                        if pin_result.get('failed_to_pin'):
                            fallback_client = ubot if (ubot and ubot != _pin_client and ubot != X) else None
                            if fallback_client:
                                print(f"[PIN-SYNC] {len(pin_result['failed_to_pin'])} pins still failed — retrying with ubot...")
                                for src_id, reason in pin_result['failed_to_pin']:
                                    dst_id = msg_id_map.get(src_id)
                                    if dst_id:
                                        try:
                                            from plugins.pin_map import pin_in_destination
                                            await pin_in_destination(client=fallback_client, dst_chat_id=dest_channel_id_int, dst_msg_id=dst_id)
                                            print(f"[PIN-SYNC] ubot pinned src_msg={src_id} → dst_msg={dst_id}")
                                            try:
                                                await mark_needs_link_update(uid, str(i), dest_channel_id_int, dst_id, src_id)
                                            except Exception:
                                                pass
                                        except Exception as e2:
                                            print(f"[PIN-SYNC] ubot also failed to pin src_msg={src_id}: {e2}")
                            else:
                                print(f"[PIN-SYNC] {len(pin_result['failed_to_pin'])} pins failed and no additional client available for retry")
                    except Exception as e:
                        print(f"[PIN-SYNC] verify_and_sync_pins failed: {e}")
                elif not _pin_client:
                    print(f"[PIN-SYNC] SKIPPED — no user client available to fetch source pins")
                
                failed_count = n - success
                completion_text = (
                    f'Batch Completed ✅ Success: {success}/{n}\n\n'
                    f'**Last message link:**\n`{last_link}`\n\n'
                    f'Use this link next time to continue downloading from where you left off.'
                )
                if failed_count > 0:
                    completion_text += f'\n\n❌ Failed: {failed_count} — see attached file for details.'
                await safe_reply(m, completion_text)
                
                # Send failed links as TXT file
                if failed_links:
                    try:
                        failed_file_path = f"failed_links_{uid}_{int(time.time())}.txt"
                        with open(failed_file_path, 'w', encoding='utf-8') as f:
                            f.write(f"Failed Links Report\n")
                            f.write(f"==================\n")
                            f.write(f"Batch: {success}/{n} succeeded\n")
                            f.write(f"Failed: {len(failed_links)}\n\n")
                            f.write(f"Links:\n")
                            f.write(f"------\n")
                            for idx, fl in enumerate(failed_links, 1):
                                f.write(f"{idx}. {fl}\n")
                        await m.reply_document(failed_file_path, caption=f'❌ Failed links ({len(failed_links)})')
                        os.remove(failed_file_path)
                    except Exception as e:
                        print(f"Failed to send failed links file: {e}")
                        # Fallback: send as text message if file send fails
                        try:
                            failed_text = '\n'.join(failed_links[:50])
                            await safe_reply(m, f'❌ Failed links:\n\n{failed_text}')
                        except Exception:
                            pass
            
            # Post-batch link update pass — now uses MongoDB-backed tracking
            # Catches anything that became resolvable during this batch
            await resolve_pending_link_rewrites(
                bot_client=X, ubot=ubot, source_channel=i,
                dest_channel_id_int=dest_channel_id_int,
                dest_channel_username=dest_channel_username,
                source_channel_username=source_channel_username,
                uid=uid,
                source_channel_id=source_channel_id_int,
                multi_source_channels=_multi_src_channels,
                combined_msg_id_map=_combined_msg_id_map,
            )
        
        except asyncio.CancelledError:
            # /stop was used — task.cancel() was called
            # Flush whatever new mappings we have before cleanup
            new_mappings = {k: v for k, v in msg_id_map.items() if k not in initial_msg_id_keys}
            if new_mappings:
                try:
                    last_src_id = max(msg_id_map.keys()) if msg_id_map else 0
                    await save_upload_map_incremental(uid, str(i), dest_channel_id_int, new_mappings, last_src_id)
                    print(f"[STOP] Flushed {len(new_mappings)} new mappings on cancel (original)")
                except Exception as e:
                    print(f"[STOP] Failed to flush mappings on cancel: {e}")
            print(f"[STOP] Batch (original) cancelled for uid={uid} at success={success}")
            
            # Clear rate limiter state
            if dest_channel_id_int:
                _rate_limiter.clear(dest_channel_id_int)
            _download_rate_limiter.clear()
            
            try:
                current_msg_id = int(s) + j
                if lt == 'private':
                    channel_id_clean = str(i).replace('-100', '')
                    continue_link = f"https://t.me/c/{channel_id_clean}/{current_msg_id}"
                else:
                    continue_link = f"https://t.me/{i}/{current_msg_id}"
                await safe_edit(pt,
                    f'🛑 Stopped at {j+1}/{n}. Success: {success}\n\n'
                    f'**Continue from here next time:**\n`{continue_link}`'
                )
            except Exception:
                pass
        finally:
            # Cleanup: resolve pending replies and remove active batch
            try:
                resolved = await resolve_pending_replies(uid, str(i), msg_id_map)
                if resolved > 0:
                    print(f"[RESUME] Resolved {resolved} forward references after regular batch")
            except Exception as e:
                print(f"[RESUME] Error resolving pending replies: {e}")
            
            await remove_active_batch(uid)
            batch_tasks.pop(uid, None)  # Clean up task reference
            clear_cancel_flag(uid)
            Z.pop(uid, None)
            _rate_limiter.clear()
            _download_rate_limiter.clear()


