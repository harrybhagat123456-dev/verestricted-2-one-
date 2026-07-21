# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

from telethon import TelegramClient
from telethon.sessions import StringSession
from config import API_ID, API_HASH, BOT_TOKEN, STRING, refresh_bot_token
from pyrogram import Client
import asyncio
import time
import sys
import re
import os
import glob as glob_module

# These will be set after start_client() is called
client = None
app = None
userbot = None


def _extract_flood_wait(error):
    """Extract FloodWait seconds from ANY error type.
    
    Handles:
    - Pyrogram FloodWait (has .value attribute)
    - Telethon FloodWaitError (has .seconds attribute)  
    - String patterns: "A wait of X seconds", "FloodWait: X", etc.
    - Any error with .seconds or .value int attribute
    """
    # Method 1: Pyrogram FloodWait with .value attribute
    try:
        from pyrogram.errors import FloodWait as PyroFloodWait
        if isinstance(error, PyroFloodWait):
            return getattr(error, 'value', None)
    except ImportError:
        pass
    
    # Method 2: Telethon FloodWaitError with .seconds attribute
    try:
        from telethon.errors import FloodWaitError as TeleFloodWait
        if isinstance(error, TeleFloodWait):
            return getattr(error, 'seconds', None)
    except ImportError:
        pass
    
    # Method 3: Check for .seconds or .value attribute directly
    for attr in ('seconds', 'value'):
        val = getattr(error, attr, None)
        if isinstance(val, int) and val > 0:
            return val
    
    # Method 4: Parse error string for common FloodWait patterns
    error_str = str(error)
    patterns = [
        r'A wait of (\d+) seconds',
        r'FloodWait[:\s]+(\d+)',
        r'flood.wait[:\s]+(\d+)',
        r'wait of (\d+) second',
        r'Slowmode: wait (\d+)',
        r'try again in (\d+) second',
    ]
    for pattern in patterns:
        match = re.search(pattern, error_str, re.IGNORECASE)
        if match:
            return int(match.group(1))
    
    return None


def _cleanup_stale_session_files():
    """Delete any .session files left on disk from previous runs.
    
    On Render's ephemeral filesystem, these shouldn't exist after a restart,
    but sometimes they persist. Stale session files cause re-authentication
    which triggers FloodWait.
    """
    try:
        for f in glob_module.glob("*.session"):
            try:
                os.remove(f)
                print(f"[CLEANUP] Deleted stale session file: {f}")
            except Exception:
                pass
    except Exception:
        pass


async def start_client():
    global client, app, userbot
    
    # Clean up any leftover .session files BEFORE starting
    _cleanup_stale_session_files()
    
    from utils.session_manager import (
        save_telethon_session, save_userbot_session,
        get_telethon_session, get_userbot_session
    )
    
    # ═══════════════════════════════════════════════════════════════
    # START TELETHON CLIENT — always fresh session for bots
    # ═══════════════════════════════════════════════════════════════
    # IMPORTANT: Bot sessions MUST always start with an empty StringSession.
    # Saved sessions cause ImportBotAuthorizationRequest failures when the
    # bot token changes (even slightly). Bots authenticate with bot_token
    # on EVERY start — reusing a saved session is unnecessary and causes
    # persistent "token is not valid" errors after token changes.
    # Only userbot sessions (STRING) benefit from persistence.
    print("Starting Telethon bot with bot_token (always fresh session)...")
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    telethon_started = False
    max_retries = 10  # More retries — never crash on FloodWait
    for attempt in range(max_retries):
        try:
            await client.start(bot_token=BOT_TOKEN)
            print("Telethon bot started successfully!")
            telethon_started = True
            break
        except Exception as e:
            err_str = str(e)
            
            # ── FATAL: bot token is genuinely invalid ──
            # If even a fresh session fails with "token is not valid",
            # the BOT_TOKEN env var itself is wrong — no retry will fix it.
            is_token_invalid = ("token is not valid" in err_str.lower()
                                or "ACCESS_TOKEN_INVALID" in err_str
                                or "bot access token is invalid" in err_str.lower())
            if is_token_invalid:
                print(f"[TELETHON] BOT_TOKEN appears to be INVALID: {e}")
                print("[TELETHON] Check your BOT_TOKEN environment variable!")
                # Don't waste retries on a bad token — break immediately
                client = None
                break
            
            # ── AUTH KEY INVALIDATED: session used from two IPs ──
            is_auth_key_error = ("authorization key" in err_str.lower()
                                 or "different IP" in err_str.lower()
                                 or "used under two" in err_str.lower())
            if is_auth_key_error:
                print(f"[TELETHON] Auth key invalidated: {e}")
                print("[TELETHON] Recreating client with fresh session...")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                client = TelegramClient(StringSession(), API_ID, API_HASH)
                try:
                    await client.start(bot_token=BOT_TOKEN)
                    print("[TELETHON] Fresh session created successfully!")
                    telethon_started = True
                    break
                except Exception as e2:
                    print(f"[TELETHON] Fresh session also failed: {e2}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(30 * (attempt + 1))
                    continue
            
            wait_seconds = _extract_flood_wait(e)
            if wait_seconds:
                if wait_seconds > 600:
                    print(f"Telethon FloodWait too long ({wait_seconds}s). Will wait it out...")
                    await asyncio.sleep(min(wait_seconds + 5, 700))
                    continue
                print(f"Telethon FloodWait: {wait_seconds}s. Sleeping... (attempt {attempt+1}/{max_retries})")
                await asyncio.sleep(wait_seconds + 5)
            else:
                print(f"Telethon start error (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(30 * (attempt + 1))
                else:
                    print("Telethon failed after max retries. Continuing WITHOUT Telethon client.")
                    client = None
    
    if not telethon_started and client is None:
        print("[WARN] Running without Telethon client — some features may be limited.")

    # ═══════════════════════════════════════════════════════════════
    # START PYROGRAM BOT — resilient with auto token refresh
    # ═══════════════════════════════════════════════════════════════
    # If BOT_TOKEN is invalid, the bot will NOT crash-loop.
    # Instead, it gracefully waits and periodically re-checks the env
    # for a new token. This prevents the Heroku/Render crash loop that
    # burns through dyno hours and triggers FloodWait.
    print("Starting Pyrogram bot with bot_token...")
    app = Client("pyrogrambot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, sleep_threshold=0)
    
    pyrogram_started = False
    max_retries = 10
    for attempt in range(max_retries):
        try:
            await app.start()
            print("Pyrogram bot started successfully!")
            pyrogram_started = True
            break
        except Exception as e:
            err_str = str(e)
            wait_seconds = _extract_flood_wait(e)
            if wait_seconds:
                print(f"Pyrogram bot FloodWait: {wait_seconds}s. Sleeping... (attempt {attempt+1}/{max_retries})")
                await asyncio.sleep(wait_seconds + 5)
            elif "ACCESS_TOKEN_INVALID" in err_str or "bot access token is invalid" in err_str.lower():
                # Bot token is INVALID — try to recover gracefully
                if attempt == 0:
                    print(f"[BOT] ACCESS_TOKEN_INVALID — clearing session and recreating client (attempt {attempt+1}/{max_retries})...")
                    try:
                        await app.stop()
                    except Exception:
                        pass
                    _cleanup_stale_session_files()
                    # Check if token has been updated in env
                    token_changed = refresh_bot_token()
                    app = Client(f"pyrogrambot_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, sleep_threshold=0)
                    await asyncio.sleep(3)
                    if token_changed:
                        print(f"[BOT] New token detected! Retrying with updated BOT_TOKEN...")
                    continue  # Try once more with fresh session
                elif attempt < 3:
                    # Retry a few more times — maybe Heroku is still propagating the env var
                    print(f"[BOT] ACCESS_TOKEN_INVALID persists (attempt {attempt+1}/{max_retries}). Waiting 30s before retry...")
                    refresh_bot_token()  # Check for token updates
                    try:
                        await app.stop()
                    except Exception:
                        pass
                    _cleanup_stale_session_files()
                    app = Client(f"pyrogrambot_{int(time.time())}", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, sleep_threshold=0)
                    await asyncio.sleep(30)
                    continue
                else:
                    # Token is definitely invalid — DON'T crash. Start a background watcher instead.
                    print(f"[BOT] ACCESS_TOKEN_INVALID PERSISTS — the BOT_TOKEN env var is INVALID!")
                    print(f"[BOT] Current BOT_TOKEN: {BOT_TOKEN[:10]}...{BOT_TOKEN[-5:] if len(BOT_TOKEN) > 15 else ''}")
                    print(f"[BOT] Bot will start in DEGRADED MODE and auto-recover when token is fixed.")
                    print(f"[BOT] Steps: 1) Message @BotFather on Telegram 2) /token 3) Revoke & copy new token 4) Update env var 5) Restart")
                    app = None  # Will be handled by main.py's retry loop
                    raise
            else:
                print(f"Pyrogram bot start error (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(30 * (attempt + 1))
                else:
                    print("Pyrogram bot failed after max retries! THIS IS FATAL — bot cannot run without Pyrogram.")
                    raise

    # ═══════════════════════════════════════════════════════════════
    # START USERBOT (STRING SESSION) — graceful failure, FloodWait resilient
    # ═══════════════════════════════════════════════════════════════
    if STRING:
        saved_ub_session = await get_userbot_session()
        
        max_retries = 10
        for attempt in range(max_retries):
            try:
                if saved_ub_session:
                    print("Using saved Userbot session from MongoDB...")
                    userbot = Client("4gbbot", api_id=API_ID, api_hash=API_HASH, session_string=saved_ub_session, in_memory=True, sleep_threshold=0)
                else:
                    print("Using Userbot session from env STRING...")
                    userbot = Client("4gbbot", api_id=API_ID, api_hash=API_HASH, session_string=STRING, in_memory=True, sleep_threshold=0)
                
                await userbot.start()
                # Save/update session in MongoDB
                try:
                    ub_session_string = await userbot.export_session_string()
                    await save_userbot_session(ub_session_string)
                except Exception:
                    pass
                print("Userbot started and session saved!")
                break
            except Exception as e:
                err_str = str(e)
                
                # ── AUTH KEY INVALIDATED: session used from two IPs, or key unregistered ──
                # AUTH_KEY_UNREGISTERED: Telegram revoked the auth key (session terminated,
                # password change, account settings change, etc.). The session is dead —
                # must delete and re-login. If env STRING is the same invalid session, user
                # must re-login via /login command.
                is_auth_key_error = (
                    "authorization key" in err_str.lower()
                    or "different IP" in err_str.lower()
                    or "used under two" in err_str.lower()
                    or "auth_key_unregistered" in err_str.lower()
                    or "key is not registered" in err_str.lower()
                )
                if is_auth_key_error:
                    print(f"[USERBOT] Session invalidated: {e}")
                    print("[USERBOT] Deleting stale session and trying fresh STRING from env...")
                    await save_userbot_session(None)
                    saved_ub_session = None
                    try:
                        if userbot:
                            await userbot.stop()
                    except Exception:
                        pass
                    userbot = Client("4gbbot", api_id=API_ID, api_hash=API_HASH, session_string=STRING, in_memory=True, sleep_threshold=0)
                    try:
                        await userbot.start()
                        ub_session_string = await userbot.export_session_string()
                        await save_userbot_session(ub_session_string)
                        print("[USERBOT] Fresh session from env STRING created and saved!")
                        break
                    except Exception as e2:
                        e2_str = str(e2)
                        is_still_auth_error = (
                            "auth_key_unregistered" in e2_str.lower()
                            or "key is not registered" in e2_str.lower()
                            or "authorization key" in e2_str.lower()
                        )
                        if is_still_auth_error:
                            print(f"[USERBOT] ⚠️ FATAL: Env STRING is ALSO invalid! User must /login again!")
                            print(f"[USERBOT] Both saved session AND env STRING are revoked by Telegram.")
                            print(f"[USERBOT] The userbot will NOT work until a new session is created via /login.")
                            userbot = None
                            break
                        print(f"[USERBOT] Fresh STRING session also failed: {e2}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(30 * (attempt + 1))
                        continue
                
                wait_seconds = _extract_flood_wait(e)
                if wait_seconds:
                    if wait_seconds > 600:
                        print(f"Userbot FloodWait too long ({wait_seconds}s). Skipping userbot — bot will continue without it.")
                        userbot = None
                        break
                    print(f"Userbot FloodWait: {wait_seconds}s. Sleeping... (attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(wait_seconds + 5)
                    # After FloodWait sleep, try with fresh STRING (not stale saved one)
                    if saved_ub_session and attempt == 0:
                        print("Saved userbot session may be stale. Trying fresh STRING from env...")
                        saved_ub_session = None
                    continue
                else:
                    print(f"Userbot start error (attempt {attempt+1}/{max_retries}): {e}")
                    # If saved session failed, try with fresh STRING
                    if saved_ub_session and attempt == 0:
                        print("Saved userbot session failed. Trying fresh STRING from env...")
                        saved_ub_session = None
                        continue
                    if attempt < max_retries - 1:
                        await asyncio.sleep(30 * (attempt + 1))
                    else:
                        print(f"Userbot failed after {max_retries} attempts. Continuing without userbot...")
                        userbot = None
    else:
        userbot = None
        print("No STRING configured. Running without userbot.")
    
    # Final cleanup of any .session files that may have been created
    _cleanup_stale_session_files()
    
    return client, app, userbot
