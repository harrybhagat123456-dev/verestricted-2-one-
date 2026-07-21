# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

import asyncio
from shared_client import start_client
import importlib
import os
import sys
from telethon import events

# ═══════════════════════════════════════════════════════════════
# STARTUP CLEANUP: Delete ALL stale .session files before anything.
# When the bot token changes, old session files cause auth errors.
# ═══════════════════════════════════════════════════════════════
import glob as _glob
for _sf in _glob.glob("*.session"):
    try:
        os.remove(_sf)
        print(f"[STARTUP-CLEANUP] Deleted stale session file: {_sf}")
    except Exception:
        pass
# Also remove pyrogrambot.session (used by Pyrogram if not in_memory)
for _sf in _glob.glob("pyrogrambot*.session"):
    try:
        os.remove(_sf)
        print(f"[STARTUP-CLEANUP] Deleted stale Pyrogram session: {_sf}")
    except Exception:
        pass

# Start log capture as early as possible so we don't miss any startup logs
from utils.log_buffer import start_log_capture
start_log_capture()
print("[MAIN] Log capture started — all stdout/stderr will be recorded for /logs command.")

# Surface pyrofork's internal logging to stdout so dispatcher exceptions are visible
import logging as _logging
_logging.basicConfig(
    level=_logging.WARNING,
    format="[PYRO-LOG] %(name)s %(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True,
)

# PERMANENT: RAM monitoring — log memory usage at startup
from utils.ram_monitor import log_ram, start_periodic_ram_log
log_ram("main_startup")

async def load_and_run_plugins():
    await start_client()
    plugin_dir = "plugins"
    plugins = [f[:-3] for f in os.listdir(plugin_dir) if f.endswith(".py") and f != "__init__.py"]

    # ═══════════════════════════════════════════════════════════════
    # PRIVACY GUARD: Owner + Auth users only
    # Must be registered BEFORE any plugin handlers.
    # Non-owner, non-auth users are silently dropped.
    # The bot appears invisible/dead to unauthorized users.
    #
    # IMPORTANT: Only blocks PRIVATE messages and callbacks.
    # Group/channel messages are NOT blocked because the bot needs
    # to read from source channels during batch/fetch operations.
    # The bot's own ID is auto-allowed to prevent self-blocking.
    # ═══════════════════════════════════════════════════════════════
    from shared_client import app, client
    from config import OWNER_ID
    from pyrogram import filters as pf, StopPropagation
    from pyrogram.errors import FloodWait
    from utils.func import is_auth_user

    # Get bot's own ID to prevent self-blocking
    bot_self_id = None
    try:
        bot_self_id = app.me.id
        print(f"[PRIVACY] Bot's own ID: {bot_self_id} — auto-allowed")
    except Exception:
        pass

    # Also get Telethon bot's own ID
    telethon_self_id = None
    if client:
        try:
            telethon_self_id = (await client.get_me()).id
            print(f"[PRIVACY] Telethon bot's own ID: {telethon_self_id} — auto-allowed")
        except Exception:
            pass

    async def is_allowed(user_id: int) -> bool:
        """Check if user is owner OR authorized user OR the bot itself."""
        # NEVER block the bot itself — prevents self-blocking that kills all interaction
        if bot_self_id and user_id == bot_self_id:
            return True
        if telethon_self_id and user_id == telethon_self_id:
            return True
        if user_id in OWNER_ID:
            return True
        return await is_auth_user(user_id)

    # --- Pyrogram bot: private message guard ONLY ---
    # This handler catches ONLY private messages from non-allowed users.
    # It does NOT block group/channel messages — the bot needs to read
    # from source channels during batch/fetch operations.
    # Uses group=-1 so it runs BEFORE all other handlers.
    @app.on_message(pf.private, group=-1)
    async def auth_guard(c, m):
        uid = m.from_user.id
        if not await is_allowed(uid):
            print(f"[PRIVACY] Blocked unauthorized private message from user {uid}")
            raise StopPropagation  # Silently drop — bot appears invisible
        # Diagnostic: log that an ALLOWED message passed the guard
        cmd_text = (m.text or m.caption or "")[:60]
        print(f"[PRIVACY-PASS] Allowed user {uid} — msg: {cmd_text}")

    @app.on_callback_query(group=-1)
    async def auth_callback_guard(c, cb):
        uid = cb.from_user.id
        if not await is_allowed(uid):
            print(f"[PRIVACY] Blocked unauthorized callback from user {uid}")
            await cb.answer()  # Acknowledge to prevent "loading" spinner
            raise StopPropagation
        print(f"[PRIVACY-PASS] Allowed callback from user {uid} — data: {(cb.data or '')[:50]}")

    # --- /login command + step handlers (group=-2, before all plugin handlers) ---
    @app.on_message(pf.command('login'), group=-2)
    async def login_handler_main(c, m):
        from plugins.login import login_command
        print(f"[LOGIN-MAIN] /login caught in main.py for user {m.from_user.id}")
        await login_command(c, m)

    from utils.custom_filters import login_in_progress as _login_in_progress
    @app.on_message(_login_in_progress & pf.text & pf.private & ~pf.command([
        'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id', 'pay',
        'redeem', 'gencode', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys',
        'setbot', 'rembot', 'mirror', 'mirrorstop', 'mirrorstatus', 'explanlogs'
    ]), group=-2)
    async def login_steps_handler_main(c, m):
        from plugins.login import handle_login_steps
        print(f"[LOGIN-MAIN] login step caught for user {m.from_user.id}: {(m.text or '')[:20]}")
        await handle_login_steps(c, m)

    # --- Direct /ping command on main app — proves command handling works on this app ---
    @app.on_message(pf.command("ping") & pf.private)
    async def ping_handler(c, m):
        print(f"[PING] /ping received from {m.from_user.id} — app_id={id(app)}")
        await m.reply_text(f"🏓 pong! (app_id={id(app)})")

    # --- Diagnostic probe at group=2 — confirms messages reach between guard and debug ---
    @app.on_message(pf.private, group=2)
    async def probe_group2(c, m):
        cmd_text = (m.text or m.caption or "")[:80]
        # Also test the command filter directly so we can see why it might not match
        try:
            cmd_filter = pf.command("start")
            result = await cmd_filter(c, m)
        except Exception as e:
            result = f"ERROR:{type(e).__name__}:{e}"
        me_username = getattr(getattr(c, 'me', None), 'username', 'NO_ME')
        # Log group=0 handler count + first handlers by name to find the culprit
        g0 = app.dispatcher.groups.get(0, [])
        g0_names = [getattr(getattr(h, 'callback', h), '__name__', str(type(h))) for h in g0[:10]]
        print(f"[PROBE-G2] app_id={id(app)} group0_handlers={len(g0)} first10={g0_names} msg='{cmd_text}' cmd_filter={result} m.cmd={getattr(m, 'command', 'N/A')}")

    # --- Global Pyrogram message debug logger (group=999, runs AFTER all handlers) ---
    # Logs ALL private messages so we can confirm Pyrogram is receiving updates
    @app.on_message(pf.private, group=999)
    async def debug_message_logger(c, m):
        cmd_text = (m.text or m.caption or "")[:80]
        print(f"[PYRO-DEBUG] app_id={id(app)} Private msg from {m.from_user.id}: {cmd_text}")

    # --- Telethon client: private message guard ONLY ---
    # CRITICAL FIX: Previously used `events.NewMessage(incoming=True)` which
    # caught ALL messages (groups, channels, private). This blocked the bot
    # from reading source channel messages during batch/fetch operations,
    # and could also block the bot's own forwarded messages.
    # Now ONLY blocks private messages — group/channel messages pass through.
    if client:
        @client.on(events.NewMessage(incoming=True, chats=None))
        async def telethon_auth_guard(event):
            # ONLY block private (DM) messages, NOT group/channel messages
            # The bot needs to read messages from source channels for batch/fetch
            if event.is_private:
                sender_id = event.sender_id
                if not await is_allowed(sender_id):
                    print(f"[PRIVACY] Blocked Telethon unauthorized private message from user {sender_id}")
                    raise events.StopPropagation
            # Group/channel messages are ALWAYS allowed — the bot needs them for batch processing

        @client.on(events.CallbackQuery)
        async def telethon_callback_guard(event):
            sender_id = event.sender_id
            if not await is_allowed(sender_id):
                print(f"[PRIVACY] Blocked Telethon unauthorized callback from user {sender_id}")
                await event.answer()
                raise events.StopPropagation

    if not OWNER_ID:
        print(f"[PRIVACY] ⚠️ WARNING: OWNER_ID is EMPTY! No owner is configured.")
        print(f"[PRIVACY] ⚠️ Set the OWNER_ID env var (space-separated Telegram user IDs) or the bot will be unresponsive!")
    print(f"[PRIVACY] Guard active. Owner: {OWNER_ID} + Auth users from DB can interact with the bot.")
    print(f"[PRIVACY] Bot self-ID: {bot_self_id}. Only PRIVATE messages are guarded — group/channel messages pass through.")

    # ── DIAGNOSTIC: Confirm app is live before loading plugins ──
    from shared_client import app as _app_live
    print(f"[MAIN] app before plugin load: {_app_live} (is_connected={getattr(_app_live, 'is_connected', 'N/A')})")

    for plugin in plugins:
        try:
            print(f"[MAIN] Loading plugin: {plugin}...")
            module = importlib.import_module(f"plugins.{plugin}")
            if hasattr(module, f"run_{plugin}_plugin"):
                print(f"[MAIN] Running {plugin} plugin init...")
                await getattr(module, f"run_{plugin}_plugin")()
                print(f"[MAIN] Plugin {plugin} initialized successfully")
            else:
                print(f"[MAIN] Plugin {plugin} loaded (no run_{plugin}_plugin function)")
        except Exception as e:
            print(f"[MAIN] ERROR loading plugin '{plugin}': {e}")
            import traceback
            traceback.print_exc()
            # IMPORTANT: For critical plugins like relink/batch, register a fallback handler
            if plugin == "relink":
                print(f"[MAIN] CRITICAL: relink plugin failed to load! Registering fallback handler...")
                try:
                    @app.on_message(pf.command("relink"))
                    async def relink_fallback_handler(c, m):
                        uid = m.from_user.id if m.from_user else None
                        if uid and uid in OWNER_ID:
                            await m.reply_text(
                                "/relink plugin failed to load!\n\n"
                                "Error details have been logged. Please check /logs.\n"
                                "Common fixes:\n"
                                "1. Restart the bot\n"
                                "2. Check if MongoDB is accessible\n"
                                "3. Check if all dependencies are installed"
                            )
                        elif uid:
                            await m.reply_text("Command unavailable. Contact admin.")
                    print("[MAIN] Fallback /relink handler registered (reports error to owner)")
                except Exception as fb_err:
                    print(f"[MAIN] Even fallback handler registration failed: {fb_err}")
            elif plugin == "batch":
                print(f"[MAIN] CRITICAL PLUGIN '{plugin}' FAILED TO LOAD - /{plugin} command will NOT work!")
                print(f"[MAIN] This is likely caused by an import error or dependency issue.")

    # Auto-setup bot menu on startup — OWNER ONLY (extreme privacy)
    try:
        from plugins.start import setup_bot_menu_owner_only
        await setup_bot_menu_owner_only()
    except Exception as e:
        print(f"Failed to setup bot menu: {e}")

    # AUTO-RESUME: Check for incomplete batches from previous session
    # Notifies affected users so they can /resumebatch
    try:
        from plugins.batch import startup_auto_resume
        print("[MAIN] Checking for incomplete batches from previous session...")
        await startup_auto_resume()
    except Exception as e:
        print(f"[MAIN] Auto-resume startup check failed (non-fatal): {e}")

    # AUTO-RESUME MIRRORS: Check for interrupted mirror sessions and auto-resume them
    try:
        from plugins.mirror import resume_interrupted_mirrors
        print("[MAIN] Checking for interrupted mirror sessions...")
        await resume_interrupted_mirrors()
    except Exception as e:
        print(f"[MAIN] Mirror auto-resume failed (non-fatal): {e}")

    # AUTO-RESUME CLONES: Notify users with interrupted clone jobs
    try:
        from plugins.channel_clone import startup_clone_resume_check
        print("[MAIN] Checking for interrupted clone jobs...")
        await startup_clone_resume_check()
    except Exception as e:
        print(f"[MAIN] Clone resume check failed (non-fatal): {e}")

    # AUTO-RESUME: Check for any queued auto-mirror items from relink sessions
    try:
        from plugins.relink import auto_mirror_queue_collection
        queued_count = await auto_mirror_queue_collection.count_documents({"status": "queued"})
        if queued_count > 0:
            print(f"[MAIN] Found {queued_count} queued auto-mirror items. These will be processed on next /batch.")
            # Notify owner about queued items
            try:
                from config import OWNER_ID
                for oid in OWNER_ID:
                    try:
                        await app.send_message(
                            oid,
                            f"📋 **Auto-Mirror Queue**\n\n"
                            f"{queued_count} message(s) queued for auto-mirror.\n"
                            f"Run `/relink backfill_missing` or start a `/batch` to process them."
                        )
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception as e:
        print(f"[MAIN] Auto-mirror queue check failed (non-fatal): {e}")

    # Pre-resolve FORCE_SUB chat to prevent CHAT_ID_INVALID on first user interaction
    try:
        from config import FORCE_SUB
        from shared_client import app
        if FORCE_SUB:
            await app.resolve_peer(FORCE_SUB)
            print(f"Pre-resolved FORCE_SUB chat: {FORCE_SUB}")
    except Exception as e:
        print(f"Could not pre-resolve FORCE_SUB chat: {e}")
        # Not fatal — subscribe() will handle gracefully
    
    # ═══════════════════════════════════════════════════════════════
    # DATA RESILIENCE: Ensure data survives bot token changes
    # ═══════════════════════════════════════════════════════════════
    try:
        from utils.data_resilience import run_startup_diagnostic, ensure_owner_has_auth, snapshot_data_counts
        
        # CRITICAL: Always ensure OWNER is in auth_users
        # Prevents "Total auth users: 0" after bot token changes
        await ensure_owner_has_auth()
        
        # Run diagnostic to check data health
        diagnostic = await run_startup_diagnostic()
        
        # Log quick snapshot
        snapshot = await snapshot_data_counts()
        print(f"[MAIN] Data snapshot: {snapshot}")
        
        # Warn if no data found (possible data loss from token change)
        if diagnostic.get("warnings"):
            for warning in diagnostic["warnings"]:
                print(f"[MAIN] ⚠️ DATA WARNING: {warning}")
    except Exception as e:
        print(f"[MAIN] Data resilience check failed (non-fatal): {e}")
    
    # PERMANENT: Start periodic RAM logging every 60 seconds
    log_ram("plugins_loaded")
    asyncio.ensure_future(start_periodic_ram_log(interval=60))

    # ═══════════════════════════════════════════════════════════════
    # STARTUP SELF-TEST + PERSISTENT STARTUP MESSAGE
    # 1. Checks if bot can SEND messages (FloodWait check)
    # 2. Sends a PERSISTENT "Bot Started" message to ALL owners
    #    (NOT auto-deleted — stays visible as deployment confirmation)
    # 3. Prints clear banners to stdout for Render's debug console
    # ═══════════════════════════════════════════════════════════════
    import time as _startup_time
    startup_ts = _startup_time.strftime("%Y-%m-%d %H:%M:%S UTC", _startup_time.gmtime())
    print("\n" + "=" * 70)
    print("[STARTUP] ═══════════════════════════════════════════════════")
    print(f"[STARTUP] Bot deployment started at: {startup_ts}")
    print(f"[STARTUP] OWNER_ID count: {len(OWNER_ID)}")
    print(f"[STARTUP] sleep_threshold=0 enforced on all clients (event-loop safe)")
    print("[STARTUP] ═══════════════════════════════════════════════════")
    print("=" * 70 + "\n")

    print("[SELF-TEST] Checking if bot can send messages...")
    flood_wait_until = None
    try:
        # Try sending to ALL owners — they each get a persistent startup message
        me = await app.get_me()
        bot_username = getattr(me, 'username', 'unknown')
        bot_id = getattr(me, 'id', 'unknown')
        bot_first = getattr(me, 'first_name', 'Bot')
        print(f"[SELF-TEST] get_me() OK — bot @{bot_username} (id={bot_id}, name={bot_first})")

        # Try a real send_message to verify FloodWait status
        try:
            # Build a rich, PERSISTENT startup notification
            startup_msg = (
                f"🟢 **BOT STARTED** — Deployment Confirmed\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🤖 **Bot:** @{bot_username} (ID: `{bot_id}`)\n"
                f"📅 **Started:** `{startup_ts}`\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ Telethon + Pyrogram clients initialized\n"
                f"✅ sleep_threshold=0 (event-loop safe, no blocking FloodWait sleeps)\n"
                f"✅ Two-tier cooldowns active\n"
                f"✅ Privacy guard active (owner + auth only)\n\n"
                f"**Ready to receive commands.** Try:\n"
                f"• `/start` — main menu\n"
                f"• `/ping` — health check\n"
                f"• `/status` — bot status\n"
                f"• `/clone` — clone channel with forums\n"
                f"• `/batch` — batch forward messages\n"
            )

            # Send to ALL owners — each gets the persistent startup message
            sent_count = 0
            for owner_id in OWNER_ID:
                try:
                    await app.send_message(owner_id, startup_msg)
                    sent_count += 1
                    print(f"[STARTUP] ✅ Persistent startup message sent to owner {owner_id}")
                except FloodWait as e_inner:
                    wait_secs = e_inner.value if hasattr(e_inner, 'value') else 30
                    import time as _time_inner
                    flood_wait_until = _time_inner.time() + wait_secs
                    hours_left = wait_secs / 3600
                    print(f"[STARTUP] ❌ FloodWait sending to owner {owner_id} — {wait_secs}s ({hours_left:.1f}h) remaining!")
                    print(f"[STARTUP] ❌ Bot CANNOT SEND responses until FloodWait expires (~{hours_left:.1f}h)")
                    break  # No point trying other owners — they'll all FloodWait
                except Exception as e_per_owner:
                    print(f"[STARTUP] ⚠️ Failed to send startup msg to owner {owner_id}: {e_per_owner}")

            if sent_count > 0 and not flood_wait_until:
                print(f"[SELF-TEST] ✅ send_message OK — sent startup notification to {sent_count} owner(s)")
                print(f"[SELF-TEST] ✅ Bot can send messages — commands will respond normally")

        except FloodWait as e:
            wait_secs = e.value if hasattr(e, 'value') else 30
            import time as _time
            flood_wait_until = _time.time() + wait_secs
            hours_left = wait_secs / 3600
            print(f"[SELF-TEST] ❌ FloodWait ACTIVE — {wait_secs}s ({hours_left:.1f}h) remaining!")
            print(f"[SELF-TEST] Bot can RECEIVE messages but CANNOT SEND responses until FloodWait expires.")
            print(f"[SELF-TEST] FloodWait expires in ~{hours_left:.1f} hours. Commands will appear unresponsive until then.")
        except Exception as e:
            print(f"[SELF-TEST] send_message failed (non-FloodWait): {e}")
    except Exception as e:
        print(f"[SELF-TEST] Self-test error: {e}")

    # Store FloodWait info globally for /status command
    if flood_wait_until:
        import time as _time
        from shared_client import app as _app
        _app._flood_wait_until = flood_wait_until
        print(f"\n[SELF-TEST] ⚠️ Bot is in FloodWait until {_time.strftime('%H:%M:%S UTC', _time.gmtime(flood_wait_until))}")
        print(f"[SELF-TEST] ⚠️ Owner must wait for FloodWait to clear before commands will respond.")
    else:
        print(f"\n[SELF-TEST] ✅ No FloodWait — bot should respond to commands normally")
        print(f"[STARTUP] ═══════════════════════════════════════════════════")
        print(f"[STARTUP] ✅ DEPLOYMENT COMPLETE — bot is live and ready!")
        print(f"[STARTUP] ═══════════════════════════════════════════════════\n")

async def main():
    """Main loop — NEVER exits. On crash, waits and retries forever.
    
    On Render free tier, if the process exits, the container restarts.
    This creates a crash loop: crash → restart → re-auth → FloodWait → crash.
    
    Instead, we NEVER exit. On any error, we sleep and retry.
    This prevents the crash loop and lets FloodWait sleep handle itself.
    
    Also catches RUNTIME crashes (not just startup) — if the bot dies
    mid-operation, it auto-recovers so inline quiz buttons keep working.
    
    On ACCESS_TOKEN_INVALID: The loop checks for token updates before
    retrying, so changing the token in Heroku/Render env vars and
    restarting the dyno should pick up the new token.
    """
    retry_count = 0
    max_retry_delay = 300  # Cap at 5 minutes between retries
    
    while True:
        try:
            # Before starting, check if BOT_TOKEN has been updated
            from config import refresh_bot_token
            refresh_bot_token()
            
            await load_and_run_plugins()
            print("[MAIN] Bot started successfully! Entering idle loop...")
            retry_count = 0  # Reset on successful start
            
            # Keep process alive forever — catch runtime crashes too
            while True:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            print("[MAIN] KeyboardInterrupt — shutting down.")
            break
            
        except Exception as e:
            retry_count += 1
            
            # Check if this is ACCESS_TOKEN_INVALID — refresh token before retry
            err_str = str(e)
            if "ACCESS_TOKEN_INVALID" in err_str or "bot access token is invalid" in err_str.lower():
                from config import refresh_bot_token, BOT_TOKEN
                token_changed = refresh_bot_token()
                if token_changed:
                    print(f"[MAIN] BOT_TOKEN was updated! New token: {BOT_TOKEN[:10]}... — retrying immediately")
                    retry_count = 0  # Reset retry count for new token
                    delay = 5
                else:
                    delay = min(60 * retry_count, max_retry_delay)
                    print(f"[MAIN] ACCESS_TOKEN_INVALID — token NOT changed. Current: {BOT_TOKEN[:10]}...")
                    print(f"[MAIN] To fix: 1) @BotFather → /token → Revoke 2) Set new BOT_TOKEN in Heroku 3) Restart dyno")
            else:
                delay = min(60 * retry_count, max_retry_delay)
            
            print(f"[MAIN] Runtime crash (attempt {retry_count}): {e}")
            print(f"[MAIN] Auto-restarting in {delay}s... (inline buttons need a live bot)")
            import traceback
            traceback.print_exc()

            # CRITICAL: Clear plugin + shared_client modules from sys.modules so they
            # re-import fresh on the next retry. Without this, plugins keep their
            # @app.on_message handlers bound to the OLD app object, so commands stop
            # working after any crash/retry cycle.
            import sys as _sys
            stale = [k for k in _sys.modules if k.startswith('plugins.') or k == 'shared_client']
            for _k in stale:
                _sys.modules.pop(_k, None)
            print(f"[MAIN] Cleared {len(stale)} stale modules from sys.modules for clean retry.")

            await asyncio.sleep(delay)

if __name__ == "__main__":
    import time as _boot_time
    boot_ts = _boot_time.strftime("%Y-%m-%d %H:%M:%S UTC", _boot_time.gmtime())
    print("\n" + "=" * 70)
    print("[BOOT] ============================================================")
    print("[BOOT]  BOT PROCESS BOOTING")
    print(f"[BOOT]  Timestamp: {boot_ts}")
    print("[BOOT]  Render debug console — all logs stream to stdout")
    print("[BOOT] ============================================================")
    print("=" * 70 + "\n")
    print("Starting clients ...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] KeyboardInterrupt — shutting down...")
    except Exception as _boot_err:
        import traceback as _tb
        print(f"\n[FATAL] Bot crashed at boot: {_boot_err}")
        _tb.print_exc()
        print("\n[FATAL] Render will auto-restart the container.")
