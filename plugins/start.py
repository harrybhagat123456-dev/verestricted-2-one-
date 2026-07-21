# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

import asyncio
import os
import time
from shared_client import app
from pyrogram import filters
from pyrogram.errors import UserNotParticipant, ChatIdInvalid, PeerIdInvalid, ChannelPrivate, FloodWait
from pyrogram.types import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands
from config import LOG_GROUP, OWNER_ID, FORCE_SUB
from utils.func import add_auth_user, remove_auth_user, is_auth_user, get_all_auth_users
from utils.log_buffer import log_buffer


# ─── FloodWait-safe helpers ──────────────────────────────────────────────────

async def safe_reply(message, text, **kwargs):
    """reply_text with FloodWait protection."""
    try:
        return await message.reply_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else 30
        hours_left = wait / 3600
        print(f"[FLOOD] reply_text FloodWait {wait}s ({hours_left:.1f}h) — CANNOT reply, bot appears unresponsive")
        return None
    except Exception as e:
        print(f"[ERR] reply_text failed: {e}")
        return None

async def safe_edit(message, text, **kwargs):
    """edit_text with FloodWait protection.
    
    Small FloodWait (<=60s): suppressed — command continues.
    Large FloodWait (>60s): also suppressed for UI commands (logs, etc).
    Only batch.py's safe_edit re-raises large FloodWait to stop batches.
    """
    if message is None:
        return None
    try:
        return await message.edit_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else 30
        hours_left = wait / 3600
        print(f"[FLOOD] edit_text FloodWait {wait}s ({hours_left:.1f}h) — suppressed")
        return None
    except Exception as e:
        print(f"[ERR] edit_text failed: {e}")
        return None

async def safe_send(client,  chat_id, text, **kwargs):
    """send_message with FloodWait protection."""
    try:
        return await client.send_message(chat_id, text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else 30
        hours_left = wait / 3600
        print(f"[FLOOD] send_message FloodWait {wait}s ({hours_left:.1f}h) — suppressed")
        return None
    except Exception as e:
        print(f"[ERR] send_message failed: {e}")
        return None


# Auto-set bot commands menu on startup
async def setup_bot_menu():
    """Set up the bot command menu automatically on startup.
    Clears old commands from ALL scopes first, then sets new ones."""
    try:
        # Step 1: Delete old commands from all scopes to remove stale/old commands
        from pyrogram.types import BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats, BotCommandScopeAllChatAdministrators, BotCommandScopeDefault
        
        for scope in [BotCommandScopeDefault(), BotCommandScopeAllPrivateChats(), 
                      BotCommandScopeAllGroupChats(), BotCommandScopeAllChatAdministrators()]:
            try:
                await app.delete_bot_commands(scope=scope)
            except Exception:
                pass
        
        print("Cleared old bot commands from all scopes.")
        
        # Step 2: Set new commands for private chats only
        new_commands = [
            BotCommand("start", "🚀 Start the bot"),
            BotCommand("batch", "🫠 Extract files in bulk"),
            BotCommand("clone", "📁 Clone channel structure (forums)"),
            BotCommand("single", "📥 Extract single file"),
            BotCommand("id", "🆔 Get Chat/User/Message ID"),
            BotCommand("login", "🔑 Login for private channels"),
            BotCommand("logout", "🚪 Logout from the bot"),
            BotCommand("settings", "⚙️ Personalize settings"),
            BotCommand("plan", "🗓️ Check premium plans"),
            BotCommand("pay", "💎 Pay for premium"),
            BotCommand("help", "❓ Help & commands"),
            BotCommand("cancel", "🚫 Cancel current process"),
            BotCommand("stop", "🛑 Stop batch process"),
        ]
        
        # Set commands for all private chats (where users interact with the bot)
        await app.set_bot_commands(
            new_commands,
            scope=BotCommandScopeAllPrivateChats()
        )
        
        # Also set as default scope (catch-all)
        await app.set_bot_commands(
            new_commands,
            scope=BotCommandScopeDefault()
        )
        
        # Step 3: Set the menu button to show commands
        await app.set_chat_menu_button(menu_button=MenuButtonCommands())
        print("Bot menu commands set successfully!")
    except Exception as e:
        print(f"Failed to set bot menu: {e}")

async def setup_bot_menu_owner_only():
    """PRIVACY: Set bot commands for owner (with auth commands) and auth users (without auth commands).
    Removes ALL public commands so the bot appears to have no commands
    to unauthorized users. Non-authorized users see a completely empty bot."""
    try:
        from pyrogram.types import BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats, BotCommandScopeAllChatAdministrators, BotCommandScopeDefault, BotCommandScopeChat
        from utils.func import get_all_auth_users
        
        # Step 1: Delete ALL commands from ALL public scopes — bot appears dead to unauthorized users
        for scope in [BotCommandScopeDefault(), BotCommandScopeAllPrivateChats(), 
                      BotCommandScopeAllGroupChats(), BotCommandScopeAllChatAdministrators()]:
            try:
                await app.delete_bot_commands(scope=scope)
            except Exception:
                pass
        
        print("[PRIVACY] Cleared ALL public bot commands — bot is invisible to unauthorized users.")
        
        # Step 1b: Set MINIMAL commands for group chats (so /relink etc. appear in autocomplete)
        # Auth checks in the handlers prevent unauthorized use — menu is just for discoverability.
        group_commands = [
            BotCommand("relink", "🔗 Fix broken links in mirrored chat"),
            BotCommand("batch", "🫠 Extract files in bulk"),
            BotCommand("status", "📊 Batch status + FloodWait info"),
            BotCommand("cancel", "🚫 Cancel current process"),
            BotCommand("id", "🆔 Get Chat/User/Message ID"),
            BotCommand("help", "❓ Help & commands"),
        ]
        try:
            await app.set_bot_commands(
                group_commands,
                scope=BotCommandScopeAllGroupChats()
            )
            print(f"[PRIVACY] Set group commands ({len(group_commands)} cmds) for autocomplete discoverability.")
        except Exception as e:
            print(f"[PRIVACY] Could not set group commands: {e}")
        
        # Step 2: Set commands for owner — includes ALL commands
        owner_commands = [
            BotCommand("start", "🚀 Start the bot"),
            BotCommand("batch", "🫠 Extract files in bulk"),
            BotCommand("clone", "📁 Clone channel structure (forums)"),
            BotCommand("single", "📥 Extract single file"),
            BotCommand("cancel", "🚫 Cancel current process"),
            BotCommand("stop", "🛑 Stop batch process"),
            BotCommand("clearbatch", "🗑️ Wipe all batch data"),
            BotCommand("status", "📊 Batch status + FloodWait info"),
            BotCommand("mirror", "🪞 Mirror channel to dest"),
            BotCommand("mirrorstop", "⏹️ Stop channel mirror"),
            BotCommand("mirrorstatus", "📊 Mirror progress"),
            BotCommand("login", "🔑 Login for private channels"),
            BotCommand("logout", "🚪 Logout from the bot"),
            BotCommand("id", "🆔 Get Chat/User/Message ID"),
            BotCommand("settings", "⚙️ Personalize settings"),
            BotCommand("pay", "💎 Pay for premium"),
            BotCommand("fetch", "🔍 Pre-scan messages for batch"),
            BotCommand("cancelfetch", "❌ Cancel /fetch scan"),
            BotCommand("fetchmaps", "📂 List your fetch maps"),
            BotCommand("viewfetchmaps", "📄 View fetch map as TXT"),
            BotCommand("clearfetch", "🗑️ Clear your fetch maps"),
            BotCommand("answerkey", "📝 Generate answer key"),
            BotCommand("viewanswerkey", "👁️ View saved answer key"),
            BotCommand("clearanswerkey", "🗑️ Delete answer keys"),
            BotCommand("linkexplan", "🔗 Link poll to explanation"),
            BotCommand("explans", "📋 View stored explanations"),
            BotCommand("explanlogs", "📋 Explanation debug log"),
            BotCommand("auto", "🔁 Auto-sync: fetch + upload"),
            BotCommand("autooff", "⏹️ Stop auto-sync"),
            BotCommand("cancelauto", "❌ Cancel /auto setup"),
            BotCommand("relink", "🔗 Fix broken links in mirrored chat"),
            BotCommand("dl", "🎬 Download video (YT/IG/FB)"),
            BotCommand("adl", "🎵 Download audio (MP3)"),
            BotCommand("setbot", "🤖 Set custom bot token"),
            BotCommand("rembot", "🚫 Remove custom bot token"),
            BotCommand("plan", "🗓️ Premium plans & pricing"),
            BotCommand("terms", "📜 Terms and conditions"),
            BotCommand("add", "➕ Add premium user (Owner)"),
            BotCommand("auth", "➕ Authorize a user (Owner)"),
            BotCommand("unauth", "➖ Remove user auth (Owner)"),
            BotCommand("authusers", "📋 List authorized users"),
            BotCommand("logs", "📋 Bot logs — last 1hr"),
            BotCommand("set", "🔧 Setup bot menu (Owner)"),
            BotCommand("transfer", "🔄 Transfer premium"),
            BotCommand("rem", "❌ Remove your premium"),
            BotCommand("help", "❓ Help & commands"),
        ]
        
        for owner_id in OWNER_ID:
            try:
                await app.set_bot_commands(
                    owner_commands,
                    scope=BotCommandScopeChat(chat_id=owner_id)
                )
                print(f"[PRIVACY] Set owner commands for {owner_id}")
            except Exception as e:
                print(f"[PRIVACY] Could not set commands for owner {owner_id}: {e}")
        
        # Step 3: Set commands for auth users — WITHOUT owner-only commands
        auth_user_commands = [
            BotCommand("start", "🚀 Start the bot"),
            BotCommand("batch", "🫠 Extract files in bulk"),
            BotCommand("clone", "📁 Clone channel structure (forums)"),
            BotCommand("single", "📥 Extract single file"),
            BotCommand("cancel", "🚫 Cancel current process"),
            BotCommand("stop", "🛑 Stop batch process"),
            BotCommand("clearbatch", "🗑️ Wipe all batch data"),
            BotCommand("status", "📊 Batch status + FloodWait info"),
            BotCommand("mirror", "🪞 Mirror channel to dest"),
            BotCommand("mirrorstop", "⏹️ Stop channel mirror"),
            BotCommand("mirrorstatus", "📊 Mirror progress"),
            BotCommand("login", "🔑 Login for private channels"),
            BotCommand("logout", "🚪 Logout from the bot"),
            BotCommand("id", "🆔 Get Chat/User/Message ID"),
            BotCommand("settings", "⚙️ Personalize settings"),
            BotCommand("pay", "💎 Pay for premium"),
            BotCommand("fetch", "🔍 Pre-scan messages for batch"),
            BotCommand("cancelfetch", "❌ Cancel /fetch scan"),
            BotCommand("fetchmaps", "📂 List your fetch maps"),
            BotCommand("viewfetchmaps", "📄 View fetch map as TXT"),
            BotCommand("clearfetch", "🗑️ Clear your fetch maps"),
            BotCommand("answerkey", "📝 Generate answer key"),
            BotCommand("viewanswerkey", "👁️ View saved answer key"),
            BotCommand("clearanswerkey", "🗑️ Delete answer keys"),
            BotCommand("linkexplan", "🔗 Link poll to explanation"),
            BotCommand("explans", "📋 View stored explanations"),
            BotCommand("explanlogs", "📋 Explanation debug log"),
            BotCommand("auto", "🔁 Auto-sync: fetch + upload"),
            BotCommand("autooff", "⏹️ Stop auto-sync"),
            BotCommand("cancelauto", "❌ Cancel /auto setup"),
            BotCommand("relink", "🔗 Fix broken links in mirrored chat"),
            BotCommand("dl", "🎬 Download video (YT/IG/FB)"),
            BotCommand("adl", "🎵 Download audio (MP3)"),
            BotCommand("setbot", "🤖 Set custom bot token"),
            BotCommand("rembot", "🚫 Remove custom bot token"),
            BotCommand("plan", "🗓️ Premium plans & pricing"),
            BotCommand("terms", "📜 Terms and conditions"),
            BotCommand("logs", "📋 Bot logs — last 1hr"),
            BotCommand("transfer", "🔄 Transfer premium"),
            BotCommand("rem", "❌ Remove your premium"),
            BotCommand("help", "❓ Help & commands"),
        ]
        
        auth_users = await get_all_auth_users()
        for user_doc in auth_users:
            auth_uid = user_doc.get("user_id")
            if auth_uid and auth_uid not in OWNER_ID:
                try:
                    await app.set_bot_commands(
                        auth_user_commands,
                        scope=BotCommandScopeChat(chat_id=auth_uid)
                    )
                    print(f"[PRIVACY] Set auth user commands for {auth_uid}")
                except Exception as e:
                    print(f"[PRIVACY] Could not set commands for auth user {auth_uid}: {e}")
        
        # Step 4: Set menu button for owner and auth users
        for owner_id in OWNER_ID:
            try:
                await app.set_chat_menu_button(
                    chat_id=owner_id,
                    menu_button=MenuButtonCommands()
                )
            except Exception:
                pass
        
        for user_doc in auth_users:
            auth_uid = user_doc.get("user_id")
            if auth_uid and auth_uid not in OWNER_ID:
                try:
                    await app.set_chat_menu_button(
                        chat_id=auth_uid,
                        menu_button=MenuButtonCommands()
                    )
                except Exception:
                    pass
        
        print(f"[PRIVACY] Bot menu commands set — Owner: {len(owner_commands)} cmds, Auth users: {len(auth_user_commands)} cmds, Total auth users: {len(auth_users)}")
    except Exception as e:
        print(f"Failed to set bot menu: {e}")

async def subscribe(app, message):
    # Owner always bypasses subscription check — Super Prime
    if message.from_user.id in OWNER_ID:
        return 0
    # Auth users also bypass subscription check
    if await is_auth_user(message.from_user.id):
        return 0
    
    if FORCE_SUB:
        try:
            # Resolve the FORCE_SUB chat peer first to prevent CHAT_ID_INVALID
            try:
                await app.resolve_peer(FORCE_SUB)
            except Exception:
                # If resolve_peer fails, try get_chat as fallback
                try:
                    await app.get_chat(FORCE_SUB)
                except Exception as e:
                    print(f"Cannot resolve FORCE_SUB chat {FORCE_SUB}: {e}")
                    # Skip subscription check if we can't resolve the chat
                    return 0
            
            user = await app.get_chat_member(FORCE_SUB, message.from_user.id)
            if str(user.status) == "ChatMemberStatus.BANNED":
                await safe_reply(message, "You are Banned. Contact -- Team SPY")
                return 1
        except UserNotParticipant:
            try:
                link = await app.export_chat_invite_link(FORCE_SUB)
                caption = f"Join our channel to use the bot"
                await message.reply_photo(photo="https://graph.org/file/d44f024a08ded19452152.jpg",caption=caption, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Join Now...", url=f"{link}")]]))
            except (ChatIdInvalid, PeerIdInvalid, ChannelPrivate) as e:
                print(f"Cannot access FORCE_SUB chat for invite link: {e}")
                # Skip subscription check if bot can't access the chat
                return 0
            return 1
        except (ChatIdInvalid, PeerIdInvalid) as e:
            print(f"CHAT_ID_INVALID/PEER_ID_INVALID for FORCE_SUB {FORCE_SUB}: {e}")
            # Skip subscription check — the bot likely hasn't seen this chat yet
            # This happens when the bot was just started and hasn't cached the chat
            return 0
        except Exception as ggn:
            print(f"Subscribe check error: {ggn}")
            # Don't block users with "Something Went Wrong" — skip the check
            return 0

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    """Welcome message — /start command."""
    uid = message.from_user.id
    print(f"[CMD] /start received from user {uid}")

    # Check force sub
    sub_result = await subscribe(client, message)
    if sub_result == 1:
        print(f"[CMD] /start — user {uid} needs to join channel first")
        return  # User needs to join channel first

    print(f"[CMD] /start — sending welcome to user {uid}...")
    result = await safe_reply(message,
        "**🚀 Welcome to the Bot!**\n\n"
        "I can extract and forward media from channels.\n\n"
        "**Use /help** to see all available commands."
    )
    if result is None:
        print(f"[CMD] /start — reply FAILED for user {uid} (possible FloodWait)")
    else:
        print(f"[CMD] /start — reply sent OK to user {uid}")


@app.on_message(filters.command("set"))
async def set_cmd(_, message):
    if message.from_user.id not in OWNER_ID:
        await safe_reply(message, "You are not authorized to use this command.")
        return
    
    await setup_bot_menu()
    await safe_reply(message, "✅ Commands menu configured successfully!")


@app.on_message(filters.command("id"))
async def id_command(client, message):
    """Send chat ID, user ID, and message ID"""
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else "Unknown"
    msg_id = message.id
    
    info_text = (
        f"📋 **IDs:**\n\n"
        f"💬 **Chat ID:** `{chat_id}`\n"
        f"👤 **User ID:** `{user_id}`\n"
        f"📩 **Message ID:** `{msg_id}`"
    )
    
    # If in a group/channel, also show the chat type and title
    if message.chat.type.value != "private":
        chat_title = message.chat.title or "Unknown"
        chat_type = message.chat.type.value
        info_text += (
            f"\n\n📌 **Chat Title:** {chat_title}\n"
            f"📁 **Chat Type:** {chat_type}"
        )
    
    # If the message is a reply, also show the replied message ID and its sender
    if message.reply_to_message:
        reply_msg_id = message.reply_to_message.id
        reply_user_id = message.reply_to_message.from_user.id if message.reply_to_message.from_user else "Unknown"
        info_text += (
            f"\n\n↩️ **Replied Message ID:** `{reply_msg_id}`\n"
            f"↩️ **Replied User ID:** `{reply_user_id}`"
        )
    
    # If forwarded, show forward info (using forward_origin instead of deprecated forward_date)
    if message.forward_origin:
        fwd_origin = message.forward_origin
        try:
            if hasattr(fwd_origin, 'sender_user') and fwd_origin.sender_user:
                info_text += f"\n📤 **Forwarded From User:** `{fwd_origin.sender_user.id}`"
            elif hasattr(fwd_origin, 'chat') and fwd_origin.chat:
                info_text += f"\n📤 **Forwarded From Chat:** `{fwd_origin.chat.id}`"
            elif hasattr(fwd_origin, 'date'):
                info_text += f"\n📤 **Forwarded on:** {fwd_origin.date.strftime('%d-%b-%Y %H:%M')}"
        except Exception:
            pass
    
    await safe_reply(message,info_text)


# ─── LOGS COMMAND (OWNER + AUTH USERS) ─────────────────────────────────────────────────

@app.on_message(filters.command("logs"))
async def logs_command(client, message):
    """Send bot logs as a TXT file. Owner + Auth users.
    
    Usage:
        /logs       — Last 1 hour (default)
        /logs all   — All buffered logs
        /logs 2     — Last 2 hours
    """
    uid = message.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return  # Silently ignore — unauthorized users
    
    # Parse optional hours argument
    args = message.text.split()
    hours = 1  # Default: last 1 hour
    if len(args) > 1:
        if args[1].lower() == 'all':
            hours = None  # All logs
        else:
            try:
                hours = float(args[1])
                if hours <= 0:
                    hours = 1  # Fallback to 1 hour
                if hours and hours > 24:
                    hours = 24.0
            except ValueError:
                hours = 1  # Invalid input → default 1 hour
    
    status_msg = await safe_reply(message, "📋 Collecting logs...")
    
    async def _update_status(text, **kwargs):
        """Edit status_msg if available, otherwise send a new message."""
        if status_msg:
            result = await safe_edit(status_msg, text, **kwargs)
            if result is not None:
                return result
        # Fallback: send as new message (status_msg was None or edit failed)
        try:
            return await message.reply_text(text, **kwargs)
        except Exception:
            return None
    
    try:
        # Debug: check if log capture is active
        is_capturing = log_buffer.is_capturing
        total_lines = log_buffer.line_count
        print(f"[LOGS] uid={uid} — is_capturing={is_capturing}, total_lines={total_lines}, hours={hours}")
        
        if total_lines == 0:
            await _update_status(
                "No logs captured yet.\n\n"
                f"Log capture active: {'Yes' if is_capturing else 'NO — this is the problem!'}\n\n"
                "Note: Log capture starts when the bot starts. If the bot was recently restarted, "
                "there may not be enough logs yet."
            )
            return
        
        # Write directly to file — avoids huge string in memory
        # Use absolute path to avoid working directory issues on Heroku
        import tempfile
        file_path = os.path.join(tempfile.gettempdir(), f"bot_logs_{int(time.time())}.txt")
        print(f"[LOGS] Writing to {file_path}...")
        
        # Run write_to_file in executor to avoid blocking event loop
        # (write_to_file holds a threading.Lock which can block if buffer is large)
        loop = asyncio.get_event_loop()
        lines_written, file_size = await loop.run_in_executor(
            None,  # Use default ThreadPoolExecutor
            log_buffer.write_to_file, file_path, hours
        )
        print(f"[LOGS] Written {lines_written} lines, {file_size} bytes")
        
        if lines_written == 0:
            await _update_status(
                f"No logs found for the last {hours} hour(s)." if hours else "No logs found."
            )
            try:
                os.remove(file_path)
            except Exception:
                pass
            return
        
        file_size_kb = file_size / 1024
        hours_label = f"last {hours}h" if hours else "all"
        
        try:
            await message.reply_document(
                file_path,
                caption=f"📋 Bot Logs — {hours_label}\n{lines_written} lines | {file_size_kb:.1f} KB | Buffer: {total_lines} total"
            )
        except FloodWait as e:
            wait = e.value if hasattr(e, 'value') else 30
            if wait <= 60:
                print(f"[LOGS] reply_document FloodWait {wait}s — waiting then retrying...")
                await asyncio.sleep(wait + 1)
                await message.reply_document(
                    file_path,
                    caption=f"📋 Bot Logs — {hours_label}\n{lines_written} lines | {file_size_kb:.1f} KB | Buffer: {total_lines} total"
                )
            else:
                await _update_status(f"❌ FloodWait {wait}s — cannot send logs file. Try again later.")
                try: os.remove(file_path)
                except: pass
                return
        os.remove(file_path)
        if status_msg:
            try: await status_msg.delete()
            except: pass
    
    except Exception as e:
        import traceback
        err_detail = f"❌ Failed to retrieve logs: {str(e)[:200]}"
        print(f"[LOGS] ERROR: {e}")
        traceback.print_exc()
        # Always try to inform the user, even if status_msg is None
        await _update_status(err_detail)


@app.on_message(filters.command("explanlogs"))
async def explanlogs_command(client, message):
    """Send explanation debug log as a TXT file. Owner/Auth only.
    
    Usage:
        /explanlogs       — Last 500 lines (default)
        /explanlogs 100   — Last 100 lines
        /explanlogs all   — All lines
        /explanlogs clear — Delete the log file
    """
    from utils.func import is_auth_user
    uid = message.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    # Parse argument
    args = message.text.split()
    tail_lines = 500  # Default: last 500 lines
    clear_mode = False
    
    if len(args) > 1:
        arg = args[1].lower()
        if arg == "clear":
            clear_mode = True
        elif arg == "all":
            tail_lines = None  # All lines
        else:
            try:
                tail_lines = int(arg)
                if tail_lines <= 0:
                    tail_lines = 500
            except ValueError:
                tail_lines = 500
    
    # Log file path (same as batch.py)
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'explan_debug.log')
    
    if clear_mode:
        try:
            if os.path.exists(log_path):
                os.remove(log_path)
                await safe_reply(message, "🗑️ Explanation debug log cleared.")
            else:
                await safe_reply(message, "No log file to clear.")
        except Exception as e:
            await safe_reply(message, f"❌ Failed to clear log: {e}")
        return
    
    if not os.path.exists(log_path):
        await safe_reply(message,
            "📋 No explanation debug log yet.\n\n"
            "The log is created automatically when polls with explanations are sent during /batch.\n"
            "Use `/explanlogs 500` to see last 500 lines."
        )
        return
    
    try:
        # Read the file
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        total_lines = len(lines)
        file_size = os.path.getsize(log_path)
        
        if total_lines == 0:
            await safe_reply(message, "📋 Explanation debug log is empty.")
            return
        
        # Apply tail limit
        if tail_lines and total_lines > tail_lines:
            lines = lines[-tail_lines:]
            showing = tail_lines
        else:
            showing = total_lines
        
        # Write to temp file for sending
        out_path = f"explan_logs_{int(time.time())}.txt"
        with open(out_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        file_size_kb = os.path.getsize(out_path) / 1024
        
        await message.reply_document(
            out_path,
            caption=f"📖 Explanation Debug Log\nShowing {showing}/{total_lines} lines | {file_size_kb:.1f} KB | Total: {file_size/1024:.1f} KB"
        )
        os.remove(out_path)
    
    except Exception as e:
        await safe_reply(message, f"❌ Failed to read explanation log: {str(e)[:200]}")


# ─── AUTH USER COMMANDS (OWNER ONLY) ────────────────────────────────────────────

@app.on_message(filters.command("auth"))
async def auth_command(client, message):
    """Add a user as authorized to use the bot. Owner only."""
    if message.from_user.id not in OWNER_ID:
        return  # Silently ignore — non-owners shouldn't know this exists
    
    args = message.text.split()
    if len(args) != 2:
        await safe_reply(message,
            "**Usage:** `/auth user_id`\n\n"
            "**Example:** `/auth 123456789`\n\n"
            "Adds the user so they can use the bot (batch, single, login, etc.)"
        )
        return
    
    try:
        target_id = int(args[1])
    except ValueError:
        await safe_reply(message, "Invalid user ID. Must be a number.")
        return
    
    if target_id in OWNER_ID:
        await safe_reply(message, "Owner is always authorized — no need to add.")
        return
    
    if await is_auth_user(target_id):
        await safe_reply(message, f"User `{target_id}` is already authorized.")
        return
    
    success = await add_auth_user(target_id, message.from_user.id)
    if success:
        await safe_reply(message,
            f"✅ User `{target_id}` has been authorized to use the bot."
        )
        # Set bot commands for the newly authorized user
        try:
            from pyrogram.types import BotCommand, BotCommandScopeChat, MenuButtonCommands
            auth_user_commands = [
                BotCommand("start", "🚀 Start the bot"),
                BotCommand("batch", "🫠 Extract files in bulk"),
                BotCommand("single", "📥 Extract single file"),
                BotCommand("cancel", "🚫 Cancel current process"),
                BotCommand("stop", "🛑 Stop batch process"),
                BotCommand("clearbatch", "🗑️ Wipe all batch data"),
                BotCommand("status", "📊 Batch status + FloodWait info"),
                BotCommand("mirror", "🪞 Mirror channel to dest"),
                BotCommand("mirrorstop", "⏹️ Stop channel mirror"),
                BotCommand("mirrorstatus", "📊 Mirror progress"),
                BotCommand("login", "🔑 Login for private channels"),
                BotCommand("logout", "🚪 Logout from the bot"),
                BotCommand("id", "🆔 Get Chat/User/Message ID"),
                BotCommand("settings", "⚙️ Personalize settings"),
                BotCommand("pay", "💎 Pay for premium"),
                BotCommand("fetch", "🔍 Pre-scan messages for batch"),
                BotCommand("cancelfetch", "❌ Cancel /fetch scan"),
                BotCommand("fetchmaps", "📂 List your fetch maps"),
                BotCommand("viewfetchmaps", "📄 View fetch map as TXT"),
                BotCommand("clearfetch", "🗑️ Clear your fetch maps"),
                BotCommand("answerkey", "📝 Generate answer key"),
                BotCommand("viewanswerkey", "👁️ View saved answer key"),
                BotCommand("clearanswerkey", "🗑️ Delete answer keys"),
                BotCommand("linkexplan", "🔗 Link poll to explanation"),
                BotCommand("explans", "📋 View stored explanations"),
                BotCommand("explanlogs", "📋 Explanation debug log"),
                BotCommand("auto", "🔁 Auto-sync: fetch + upload"),
                BotCommand("autooff", "⏹️ Stop auto-sync"),
                BotCommand("cancelauto", "❌ Cancel /auto setup"),
                BotCommand("relink", "🔗 Fix broken links in mirrored chat"),
                BotCommand("dl", "🎬 Download video (YT/IG/FB)"),
                BotCommand("adl", "🎵 Download audio (MP3)"),
                BotCommand("setbot", "🤖 Set custom bot token"),
                BotCommand("rembot", "🚫 Remove custom bot token"),
                BotCommand("plan", "🗓️ Premium plans & pricing"),
                BotCommand("terms", "📜 Terms and conditions"),
                BotCommand("logs", "📋 Bot logs — last 1hr"),
                BotCommand("transfer", "🔄 Transfer premium"),
                BotCommand("rem", "❌ Remove your premium"),
                BotCommand("help", "❓ Help & commands"),
            ]
            await app.set_bot_commands(
                auth_user_commands,
                scope=BotCommandScopeChat(chat_id=target_id)
            )
            await app.set_chat_menu_button(
                chat_id=target_id,
                menu_button=MenuButtonCommands()
            )
        except Exception as e:
            print(f"[PRIVACY] Could not set commands for new auth user {target_id}: {e}")
        # Notify the authorized user
        try:
            await safe_send(app, 
                target_id,
                "✅ You have been authorized to use this bot. You can now use /start, /batch, /single, /login, /settings and other commands."
            )
        except Exception as e:
            await safe_reply(message, f"Note: Could not notify the user (they may not have started the bot yet): {e}")
    else:
        await safe_reply(message, f"❌ Failed to authorize user `{target_id}`.")


@app.on_message(filters.command("unauth"))
async def unauth_command(client, message):
    """Remove a user's authorization. Owner only."""
    if message.from_user.id not in OWNER_ID:
        return  # Silently ignore
    
    args = message.text.split()
    if len(args) != 2:
        await safe_reply(message,
            "**Usage:** `/unauth user_id`\n\n"
            "**Example:** `/unauth 123456789`"
        )
        return
    
    try:
        target_id = int(args[1])
    except ValueError:
        await safe_reply(message, "Invalid user ID. Must be a number.")
        return
    
    if target_id in OWNER_ID:
        await safe_reply(message, "Cannot remove owner authorization.")
        return
    
    success = await remove_auth_user(target_id)
    if success:
        await safe_reply(message,
            f"✅ User `{target_id}` has been removed from authorized users."
        )
        # Remove bot commands for the unauthorized user
        try:
            from pyrogram.types import BotCommandScopeChat
            await app.delete_bot_commands(scope=BotCommandScopeChat(chat_id=target_id))
        except Exception:
            pass
        # Notify the unauthorized user
        try:
            await safe_send(app, 
                target_id,
                "⚠️ Your access to this bot has been revoked. Contact the owner if you think this is a mistake."
            )
        except Exception:
            pass
    else:
        await safe_reply(message, f"❌ User `{target_id}` was not in the authorized list.")


@app.on_message(filters.command("authusers"))
async def authusers_command(client, message):
    """List all authorized users. Owner only."""
    if message.from_user.id not in OWNER_ID:
        return  # Silently ignore
    
    auth_users = await get_all_auth_users()
    
    if not auth_users:
        await safe_reply(message, "No authorized users (besides the owner).")
        return
    
    lines = ["📋 **Authorized Users:**\n"]
    for i, user in enumerate(auth_users, 1):
        uid = user.get("user_id", "Unknown")
        added_by = user.get("added_by", "Unknown")
        added_at = user.get("added_at", None)
        date_str = added_at.strftime("%d-%b-%Y %H:%M") if added_at else "Unknown"
        lines.append(f"{i}. `{uid}` — Added by: `{added_by}` on {date_str}")
    
    lines.append(f"\n**Total:** {len(auth_users)} authorized user(s)")
    await safe_reply(message, "\n".join(lines))



help_pages = [
    # ── PAGE 1: Getting Started + Core ──
    (
        "🤖 **Team SPY Bot — All Commands (1/5)**\n\n"
        "**Getting Started:**\n\n"
        "1. **/start** — Welcome message and bot intro\n"
        "2. **/help** — Show all commands (this page!)\n"
        "3. **/batch** `<link> <count>` — Bulk extract messages from a channel\n"
        "4. **/single** `<link>` — Extract a single file/message\n"
        "5. **/cancel** or **/stop** — Cancel current running process\n"
        "6. **/clearbatch** — Wipe all batch data (no resume)\n"
        "7. **/status** — Show batch status + premium status\n"
        "8. **/login** — Log in with your Telegram account\n"
        "9. **/logout** — Terminate session and remove from DB\n"
        "10. **/id** — Get Chat ID, User ID, Message ID\n"
        "11. **/settings** — Set destination chat, caption, thumbnail, etc.\n"
        "12. **/pay** — Pay for premium via Telegram Stars\n\n"
    ),
    # ── PAGE 2: Mirror + Fetch + Auto ──
    (
        "🪞 **Mirror + Fetch + Auto (2/5)**\n\n"
        "13. **/mirror** — Mirror source channel to destination\n"
        "14. **/mirrorstop** `[id]` — Stop running mirror (omit id = stop all)\n"
        "15. **/mirrorstatus** — Check mirror progress\n"
        "16. **/fetch** — Pre-scan channel, store lightweight map in DB\n"
        "17. **/cancelfetch** — Cancel ongoing /fetch scan\n"
        "18. **/fetchmaps** — List all your stored fetch maps\n"
        "19. **/viewfetchmaps** — Download a fetch map as TXT\n"
        "20. **/clearfetch** — Delete fetch maps\n"
        "21. **/auto** — Auto-sync source channel to destination\n"
        "22. **/autooff** — Stop auto-sync monitoring\n"
        "23. **/cancelauto** — Cancel /auto setup conversation\n\n"
    ),
    # ── PAGE 3: Answer Key + Explanations + Downloads ──
    (
        "🔑 **Answer Key + Explanations + Downloads (3/5)**\n\n"
        "24. **/answerkey** — Generate answer key (.txt) from quiz/polls\n"
        "25. **/viewanswerkey** — View saved answer key by name/number\n"
        "26. **/clearanswerkey** — Delete saved answer keys\n"
        "27. **/linkexplan** — Manually link poll to its explanation\n"
        "28. **/explans** — View explanation count per channel\n"
        "29. **/explanlogs** `[count|all|clear]` — Explanation debug log\n"
        "30. **/dl** `<link>` — Download video (YT/IG/FB), auto-split >2GB\n"
        "31. **/adl** `<link>` — Download audio (MP3) from YT/IG\n\n"
    ),
    # ── PAGE 4: Relink (ALL subcommands) ──
    (
        "🔗 **Relink — Fix Broken Links (4/5)**\n\n"
        "32. **/relink** — Scan dest chat and fix all broken blue links\n"
        "33. **/relink status** — Show current session progress\n"
        "34. **/relink cancel** — Cancel running session (progress saved)\n"
        "35. **/relink retry** — Retry all previously failed edits\n"
        "36. **/relink backfill** — Build Smart Cache index (no editing)\n"
        "37. **/relink backfill mapping** — One-time backfill of missing src IDs\n"
        "38. **/relink backfill_missing** `IDs` — Re-mirror specific missing IDs\n"
        "    e.g. `/relink backfill_missing 19271,19444-19447`\n"
        "39. **/relink scan_dest** — Scan dest channel for source links\n"
        "40. **/relink --limit** `N` — Scan only last N messages\n"
        "41. **/relink --dry-run** — Preview changes without editing\n"
        "42. **/backfill_missing** `[IDs]` — Backfill missing src→dst mappings\n"
        "    Run during active batch when ubot is connected\n\n"
        "💡 Use /relink in the DESTINATION group. Bot must be admin.\n\n"
    ),
    # ── PAGE 5: Admin + Management + Workflows ──
    (
        "⚙️ **Admin + Management + Workflows (5/5)**\n\n"
        "43. **/setbot** `<token>` — Set custom bot token for uploads\n"
        "44. **/rembot** — Remove your custom bot token\n"
        "45. **/plan** — Check premium plans and pricing\n"
        "46. **/terms** — Terms and conditions\n"
        "47. **/add** `user_id dur unit` (Owner) — Add premium user\n"
        "48. **/auth** `userID` (Owner) — Authorize a user\n"
        "49. **/unauth** `userID` (Owner) — Remove user authorization\n"
        "50. **/authusers** (Owner) — List all authorized users\n"
        "51. **/logs** `[hours]` — Get bot logs as TXT file\n"
        "52. **/explanlogs** `[count|all|clear]` — Explanation debug log\n"
        "53. **/set** (Owner) — Setup bot command menu for all users\n"
        "54. **/transfer** — Transfer premium to another user\n"
        "55. **/rem** — Remove your premium status\n\n"
        "**📦 Quick Workflow:** /login → /settings → /fetch → /batch\n"
        "**🪞 Mirror Workflow:** /mirror → follow prompts → done!\n"
        "**🔗 Relink Workflow:** mirror first → /relink in dest group\n\n"
        "**__Powered by Team SPY__**"
    )
]
 

async def send_or_edit_help_page(_, message, page_number):
    if page_number < 0 or page_number >= len(help_pages):
        return
 
     
    prev_button = InlineKeyboardButton("◀️ Previous", callback_data=f"help_prev_{page_number}")
    next_button = InlineKeyboardButton("Next ▶️", callback_data=f"help_next_{page_number}")

     
    buttons = []
    if page_number > 0:
        buttons.append(prev_button)
    if page_number < len(help_pages) - 1:
        buttons.append(next_button)

     
    keyboard = InlineKeyboardMarkup([buttons])

     
    await message.delete()

     
    await safe_reply(message,
        help_pages[page_number],
        reply_markup=keyboard
    )


@app.on_message(filters.command("help"))
async def help(client, message):
    join = await subscribe(client, message)
    if join == 1:
        return
     
    await send_or_edit_help_page(client, message, 0)


@app.on_callback_query(filters.regex(r"help_(prev|next)_(\d+)"))
async def on_help_navigation(client, callback_query):
    action, page_number = callback_query.data.split("_")[1], int(callback_query.data.split("_")[2])
 
    if action == "prev":
        page_number -= 1
    elif action == "next":
        page_number += 1

    await send_or_edit_help_page(client, callback_query.message, page_number)
     
    await callback_query.answer()

 
@app.on_message(filters.command("terms") & filters.private)
async def terms(client, message):
    terms_text = (
        "> 📜 **Terms and Conditions** 📜\n\n"
        "✨ We are not responsible for user deeds, and we do not promote copyrighted content. If any user engages in such activities, it is solely their responsibility.\n"
        "✨ Upon purchase, we do not guarantee the uptime, downtime, or the validity of the plan. __Authorization and banning of users are at our discretion; we reserve the right to ban or authorize users at any time.__\n"
        "✨ Payment to us **__does not guarantee__** authorization for the /batch command. All decisions regarding authorization are made at our discretion and mood.\n"
    )
     
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📋 See Plans", callback_data="see_plan")],
            [InlineKeyboardButton("💬 Contact Now", url="https://t.me/kingofpatal")],
        ]
    )
    await safe_reply(message,terms_text, reply_markup=buttons)
 

@app.on_message(filters.command("plan") & filters.private)
async def plan(client, message):
    plan_text = (
        "> 💰 **Premium Price**:\n\n Starting from $2 or 200 INR accepted via **__Amazon Gift Card__** (terms and conditions apply).\n"
        "📥 **Download Limit**: Users can download up to 100,000 files in a single batch command.\n"
        "🛑 **Batch**: You will get two modes /bulk and /batch.\n"
        "   - Users are advised to wait for the process to automatically cancel before proceeding with any downloads or uploads.\n\n"
        "📜 **Terms and Conditions**: For further details and complete terms and conditions, please send /terms.\n"
    )
     
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📜 See Terms", callback_data="see_terms")],
            [InlineKeyboardButton("💬 Contact Now", url="https://t.me/kingofpatal")],
        ]
    )
    await safe_reply(message,plan_text, reply_markup=buttons)
 

@app.on_callback_query(filters.regex("see_plan"))
async def see_plan(client, callback_query):
    plan_text = (
        "> 💰**Premium Price**\n\n Starting from $2 or 200 INR accepted via **__Amazon Gift Card__** (terms and conditions apply).\n"
        "📥 **Download Limit**: Users can download up to 100,000 files in a single batch command.\n"
        "🛑 **Batch**: You will get two modes /bulk and /batch.\n"
        "   - Users are advised to wait for the process to automatically cancel before proceeding with any downloads or uploads.\n\n"
        "📜 **Terms and Conditions**: For further details and complete terms and conditions, please send /terms or click See Terms👇\n"
    )
     
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📜 See Terms", callback_data="see_terms")],
            [InlineKeyboardButton("💬 Contact Now", url="https://t.me/kingofpatal")],
        ]
    )
    await safe_edit(callback_query.message, plan_text, reply_markup=buttons)
 

@app.on_callback_query(filters.regex("see_terms"))
async def see_terms(client, callback_query):
    terms_text = (
        "> 📜 **Terms and Conditions** 📜\n\n"
        "✨ We are not responsible for user deeds, and we do not promote copyrighted content. If any user engages in such activities, it is solely their responsibility.\n"
        "✨ Upon purchase, we do not guarantee the uptime, downtime, or the validity of the plan. __Authorization and banning of users are at our discretion; we reserve the right to ban or authorize users at any time.__\n"
        "✨ Payment to us **__does not guarantee__** authorization for the /batch command. All decisions regarding authorization are made at our discretion and mood.\n"
    )
     
    buttons = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📋 See Plans", callback_data="see_plan")],
            [InlineKeyboardButton("💬 Contact Now", url="https://t.me/kingofpatal")],
        ]
    )
    await safe_edit(callback_query.message, terms_text, reply_markup=buttons)

