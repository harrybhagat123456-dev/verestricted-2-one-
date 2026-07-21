# Copyright (c) 2025 devgajan : https://github.com/devgajanin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

"""
AUTO command — Auto-sync source channel → destination channel.

Flow:
1. /auto → ask for source link
2. User sends source link → ask for destination channel ID (integer)
3. Bot checks existing fetch map for that channel
4. If fetch map doesn't cover all messages → auto-fetch remaining
5. Bot checks upload_map → skip already uploaded messages
6. Uploads all new/un-uploaded messages
7. Enters background monitoring loop — polls for new messages periodically
8. New messages are automatically fetched + uploaded

/autooff → stops the auto monitoring for that channel
/stop   → stops batch + auto-sync
/clearbatch → clears batch data + auto-sync data

Auto state is stored in MongoDB so it survives bot restarts.
"""

import os
import re
import time
import asyncio
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, ChatIdInvalid, PeerIdInvalid, ChannelPrivate
from shared_client import app as X
from config import OWNER_ID
from utils.func import E, is_auth_user
from utils.func import get_user_data, get_user_data_key, save_user_data
from utils.custom_filters import login_in_progress
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME


# ─── FloodWait-safe helpers ──────────────────────────────────────────────────

async def safe_reply(message, text, **kwargs):
    """reply_text with FloodWait protection."""
    try:
        return await message.reply_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[FLOOD] reply_text FloodWait {wait}s — suppressed")
        return None
    except Exception as e:
        print(f"[ERR] reply_text failed: {e}")
        return None

async def safe_edit(message, text, **kwargs):
    """edit_text with FloodWait protection."""
    try:
        return await message.edit_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[FLOOD] edit_text FloodWait {wait}s — suppressed")
        return None
    except Exception as e:
        print(f"[ERR] edit_text failed: {e}")
        return None

async def safe_send(client, chat_id, text, **kwargs):
    """send_message with FloodWait protection."""
    try:
        return await client.send_message(chat_id, text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[FLOOD] send_message FloodWait {wait}s — suppressed")
        return None
    except Exception as e:
        print(f"[ERR] send_message failed: {e}")
        return None


# MongoDB
_mongo_client = AsyncIOMotorClient(MONGO_URI)
_db = _mongo_client[DB_NAME]
auto_sync_collection = _db["auto_sync"]

# In-memory state for active auto tasks
AUTO_STATE = {}  # uid -> {'step': 'source'|'dest', ...}
AUTO_TASKS = {}  # (uid, source_channel) -> asyncio.Task  (multiple auto-syncs per user)

# ═══════════════════════════════════════════════════════════════
# CANCEL EVENTS — reliable auto-sync cancellation
#
# task.cancel() alone is unreliable because CancelledError can
# get swallowed by Pyrogram's internal try/except blocks or by
# generic "except Exception" in process_msg. The cancel event
# provides a secondary, guaranteed-stop mechanism that run_auto_sync
# checks at every loop iteration.
# ═══════════════════════════════════════════════════════════════
AUTO_CANCEL_EVENTS = {}  # (uid, source_channel) -> asyncio.Event


def _set_auto_cancel(uid, source_channel):
    """Signal that an auto-sync should stop immediately."""
    key = (uid, str(source_channel))
    if key in AUTO_CANCEL_EVENTS:
        AUTO_CANCEL_EVENTS[key].set()
    # Also set for all keys matching this uid (used by /stop all)
    for k in AUTO_CANCEL_EVENTS:
        if k[0] == uid:
            AUTO_CANCEL_EVENTS[k].set()


def _clear_auto_cancel(uid, source_channel):
    """Clear the cancel event (when starting a new auto-sync)."""
    key = (uid, str(source_channel))
    if key in AUTO_CANCEL_EVENTS:
        AUTO_CANCEL_EVENTS[key].clear()


def _is_auto_cancelled(uid, source_channel):
    """Check if auto-sync has been cancelled."""
    key = (uid, str(source_channel))
    if key in AUTO_CANCEL_EVENTS:
        return AUTO_CANCEL_EVENTS[key].is_set()
    return False


# ─── AUTO SYNC HELPERS ──────────────────────────────────────────────────────

async def save_auto_sync(user_id, source_channel, dest_channel, link_type, last_processed_id=0):
    """Save auto-sync config to MongoDB."""
    doc = {
        "user_id": user_id,
        "source_channel": str(source_channel),
        "dest_channel": int(dest_channel) if dest_channel else None,
        "link_type": link_type,
        "last_processed_id": last_processed_id,
        "active": True,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }
    await auto_sync_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {"$set": doc},
        upsert=True
    )


async def get_auto_sync(user_id, source_channel=None):
    """Get auto-sync config. If source_channel is None, get all for this user."""
    if source_channel:
        return await auto_sync_collection.find_one({
            "user_id": user_id,
            "source_channel": str(source_channel)
        })
    return await auto_sync_collection.find({"user_id": user_id}).to_list(length=None)


async def deactivate_auto_sync(user_id, source_channel=None):
    """Deactivate auto-sync. If source_channel=None, deactivate all."""
    query = {"user_id": user_id, "active": True}
    if source_channel:
        query["source_channel"] = str(source_channel)
    result = await auto_sync_collection.update_many(query, {"$set": {"active": False, "updated_at": datetime.now()}})
    return result.modified_count


async def update_auto_last_processed(user_id, source_channel, last_id):
    """Update the last processed message ID for auto-sync."""
    await auto_sync_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {"$set": {"last_processed_id": last_id, "updated_at": datetime.now()}}
    )


async def list_active_auto_syncs():
    """List all active auto-syncs (for background loop recovery)."""
    return await auto_sync_collection.find({"active": True}).to_list(length=None)


def stop_auto_task(uid, source_channel=None):
    """Stop an auto-sync task reliably.
    
    Uses THREE mechanisms to guarantee the task stops:
    1. Set cancel event (checked at every loop iteration in run_auto_sync)
    2. Call task.cancel() (raises CancelledError at next await)
    3. Deactivate in MongoDB (checked as fallback in run_auto_sync)
    
    IMPORTANT: We do NOT delete cancel events or AUTO_TASKS entries here.
    The cancel event must remain set so run_auto_sync sees it when it
    checks _is_auto_cancelled() after a long process_msg() call returns.
    The finally block in run_auto_sync handles cleanup when the task exits.
    
    Returns number of tasks found and cancelled.
    """
    stopped = 0
    
    if source_channel:
        # Stop specific auto-sync
        _set_auto_cancel(uid, source_channel)
        task_key = (uid, str(source_channel))
        if task_key in AUTO_TASKS:
            try:
                AUTO_TASKS[task_key].cancel()
            except Exception:
                pass
            stopped += 1
        # NOTE: Do NOT delete cancel event or AUTO_TASKS entry here!
        # The task may still be running inside process_msg() which can
        # swallow CancelledError. The cancel event must stay SET so
        # _is_auto_cancelled() returns True when checked after process_msg.
        # Cleanup happens in run_auto_sync's finally block.
    else:
        # Stop ALL auto-syncs for this user
        keys_to_remove = [k for k in AUTO_TASKS if k[0] == uid]
        for k in keys_to_remove:
            _set_auto_cancel(uid, k[1])
            try:
                AUTO_TASKS[k].cancel()
            except Exception:
                pass
            stopped += 1
        # Also set cancel events for any keys that might not have tasks
        # (e.g. task crashed but cancel event still exists)
        for k in AUTO_CANCEL_EVENTS:
            if k[0] == uid:
                AUTO_CANCEL_EVENTS[k].set()
        # NOTE: Do NOT delete cancel events or AUTO_TASKS entries!
        # Same reason as above — task may still be running.
    
    return stopped


# ─── /AUTO COMMAND ───────────────────────────────────────────────────────────

@X.on_message(filters.command("auto"))
async def auto_cmd(c, m):
    """Start auto-sync: fetch + upload + monitor a channel."""
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    if uid in AUTO_STATE:
        await safe_reply(m, "You already have an /auto setup in progress. Send /cancelauto to cancel.")
        return
    
    # Show existing active auto-syncs (but allow adding more)
    active = await get_auto_sync(uid)
    active_list = [a for a in active if a.get("active", False)] if active else []
    
    existing_info = ""
    if active_list:
        lines = [f"📋 You have {len(active_list)} active auto-sync(s):\n"]
        for idx, a in enumerate(active_list, 1):
            ch = a.get("source_channel", "?")
            dst = a.get("dest_channel", "?")
            last = a.get("last_processed_id", 0)
            lines.append(f"  {idx}. Source: `{ch}` → Dest: `{dst}` | Last: msg {last}")
        existing_info = "\n".join(lines) + "\n\n"
    
    AUTO_STATE[uid] = {'step': 'source'}
    
    await safe_reply(m,
        f"{existing_info}"
        "**🔄 /auto — Add Auto-Sync**\n\n"
        "Automatically syncs a source channel to a destination channel.\n\n"
        "**What it does:**\n"
        "1️⃣ Checks your fetch map for the channel\n"
        "2️⃣ Auto-fetches any missing messages\n"
        "3️⃣ Skips already uploaded messages\n"
        "4️⃣ Uploads all new messages\n"
        "5️⃣ **Monitors for new messages** — auto-uploads them as they appear\n\n"
        "**Step 1 of 2:** Send the **source channel link** (any message link from the channel)."
    )


# ─── /AUTOOFF COMMAND ────────────────────────────────────────────────────────

@X.on_message(filters.command("autooff"))
async def autooff_cmd(c, m):
    """Stop auto-sync. /autooff all → stop all. /autooff → show list to pick."""
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    # Check args: /autooff all → stop everything
    args = m.text.split(maxsplit=1)
    if len(args) > 1 and args[1].strip().lower() == 'all':
        stopped = stop_auto_task(uid)  # stops ALL for this user
        count = await deactivate_auto_sync(uid)
        total = max(stopped, count)
        if total > 0:
            await safe_reply(m, f"✅ Stopped all {total} auto-sync(s).")
        else:
            await safe_reply(m, "No active auto-syncs to stop.")
        return
    
    # No "all" — show list and let user pick
    active = await get_auto_sync(uid)
    active_list = [a for a in active if a.get("active", False)] if active else []
    
    if not active_list:
        await safe_reply(m, "No active auto-syncs to stop.")
        return
    
    if len(active_list) == 1:
        # Only one — just stop it
        source_ch = active_list[0]["source_channel"]
        stop_auto_task(uid, source_ch)
        await deactivate_auto_sync(uid, source_ch)
        await safe_reply(m, f"✅ Stopped auto-sync for `{source_ch}`.")
        return
    
    # Multiple — show numbered list
    lines = [f"📋 You have {len(active_list)} active auto-sync(s):\n"]
    for idx, a in enumerate(active_list, 1):
        ch = a.get("source_channel", "?")
        dst = a.get("dest_channel", "?")
        lines.append(f"  {idx}. `{ch}` → `{dst}`")
    lines.append("\nReply with the **number** to stop that one.")
    lines.append("Or send **/autooff all** to stop all.")
    
    # Store in AUTO_STATE for callback
    AUTO_STATE[uid] = {'step': 'autooff_pick', 'list': active_list}
    await safe_reply(m, "\n".join(lines))


# ─── /CANCELAUTO COMMAND ────────────────────────────────────────────────────

@X.on_message(filters.command("cancelauto"))
async def cancelauto_cmd(c, m):
    """Cancel an in-progress /auto setup conversation."""
    uid = m.from_user.id
    if uid in AUTO_STATE:
        del AUTO_STATE[uid]
        await safe_reply(m, "❌ /auto setup cancelled.")
    else:
        await safe_reply(m, "No /auto setup in progress.")


# ─── AUTO-SYNC MAIN LOGIC ───────────────────────────────────────────────────

async def run_auto_sync(uid, source_channel, dest_channel, link_type, notify_chat_id):
    """Background task: auto-fetch + auto-upload + monitor new messages.
    
    Steps:
    1. Check fetch map → auto-fetch missing messages
    2. Check upload_map → skip already uploaded
    3. Upload new messages
    4. Enter monitoring loop: check for new messages every 30s
    
    Cancellation is triple-redundant:
    - Cancel event (checked at every loop iteration — guaranteed fast stop)
    - task.cancel() (raises CancelledError at next await)
    - MongoDB active=False (checked periodically as fallback)
    """
    from plugins.batch import (
        get_ubot, get_uclient, get_Y, resolve_chat, get_msg,
        process_msg, load_upload_map, save_upload_map_incremental,
        flood_wait_retry, mark_needs_link_update,
        resolve_pending_link_rewrites
    )
    from plugins.fetch import (
        fetch_maps_collection, save_fetch_map, get_latest_fetch_map_end,
        run_fetch_scan
    )
    
    source_channel_str = str(source_channel)
    task_key = (uid, source_channel_str)
    print(f"[AUTO] Starting auto-sync for user={uid} channel={source_channel}")
    
    try:
        # Register this channel for explanation monitoring (real-time listener)
        try:
            from plugins.explanation_listener import add_monitored_channel
            await add_monitored_channel(source_channel, uid)
        except Exception as e:
            print(f"[EXPLANATION] Could not register channel for monitoring: {e}")
        
        # Get clients
        ubot = await get_ubot(uid)
        if not ubot:
            ubot = X
        
        uc = None
        try:
            uc = await asyncio.wait_for(get_uclient(uid), timeout=60)
        except asyncio.TimeoutError:
            pass
        
        if not uc:
            uc = get_Y()
        
        if not uc:
            try:
                await safe_send(X, notify_chat_id, "❌ Auto-sync failed: No user client. Use /login first.")
            except Exception:
                pass
            await deactivate_auto_sync(uid, source_channel)
            return
        
        resolved = await resolve_chat(uc, source_channel)
        
        # ── Resolve destination channel username for link rewriting ──
        dest_channel_username = None
        try:
            dest_chat = await ubot.get_chat(dest_channel)
            dest_channel_username = getattr(dest_chat, 'username', None)
        except Exception:
            try:
                if uc:
                    dest_chat = await uc.get_chat(dest_channel)
                    dest_channel_username = getattr(dest_chat, 'username', None)
            except Exception:
                dest_channel_username = None
        
        # Resolve source channel username AND numeric ID for link rewriting
        # DUAL-FORMAT: We need BOTH the username and numeric ID to match ALL
        # link formats (private t.me/c/XXX/ AND public t.me/username/)
        source_channel_username = None
        source_channel_id_int = None  # Numeric ID for DUAL-FORMAT link matching
        try:
            src_chat = await ubot.get_chat(resolved)
            source_channel_username = getattr(src_chat, 'username', None)
            source_channel_id_int = getattr(src_chat, 'id', None)
        except Exception:
            try:
                if uc:
                    src_chat = await uc.get_chat(resolved)
                    source_channel_username = getattr(src_chat, 'username', None)
                    source_channel_id_int = getattr(src_chat, 'id', None)
            except Exception:
                source_channel_username = None
        
        # If source_channel is already numeric, use it as source_channel_id_int
        if not source_channel_id_int:
            try:
                _src_str = str(source_channel)
                if _src_str.lstrip('-').isdigit():
                    source_channel_id_int = int(_src_str)
            except Exception:
                pass
        
        print(f"[AUTO] Link rewrite: dest_id={dest_channel} dest_username={dest_channel_username} src_username={source_channel_username} src_id={source_channel_id_int}")
        
        # ── MULTI-SOURCE: Build list of ALL source channels for cross-channel link rewriting ──
        _multi_src_channels = None
        # FIX #2: build_multi_source_channels returns 1 value (list), not 2 (list, dict)
        # FIX #6: Use MongoDict instead of combined_msg_id_map for RAM savings
        _auto_msg_id_map = None
        try:
            from plugins.batch import build_multi_source_channels as _build_msc
            _resolve_client = uc or ubot
            _multi_src_channels = await _build_msc(
                uid, source_channel,
                primary_username=source_channel_username,
                primary_numeric_id=source_channel_id_int,
                client=_resolve_client,
            )
            if _multi_src_channels:
                _ch_count = len(_multi_src_channels)
                if _ch_count > 1:
                    print(f"[AUTO-MULTI-SRC] Cross-channel rewriting enabled: {_ch_count} source channels")
                else:
                    print(f"[AUTO-MULTI-SRC] Single source channel — metadata still passed for URL pattern building")
            # Do NOT set _multi_src_channels=None when only 1 channel!
        except Exception as e:
            print(f"[AUTO-MULTI-SRC] Failed to build multi-source channels (non-fatal): {e}")
            _multi_src_channels = None
        
        # ── Resolve pending link rewrites from previous auto-sync runs ──
        try:
            # FIX #2/#6: No combined_msg_id_map — use None; resolve_pending_link_rewrites
            # will use MongoDict if msg_id_map is passed, or fall back to loading from DB
            await resolve_pending_link_rewrites(
                X, ubot, source_channel,
                dest_channel, dest_channel_username, source_channel_username, uid,
                source_channel_id=source_channel_id_int,
                multi_source_channels=_multi_src_channels,
                combined_msg_id_map=None,
            )
        except Exception as e:
            print(f"[AUTO] resolve_pending_link_rewrites failed: {e}")
        
        # ── STEP 1: Check fetch map & auto-fetch missing messages ──
        try:
            await safe_send(X, notify_chat_id, f"🔄 Auto-sync starting for channel `{source_channel}`...\n📋 Checking fetch map...")
        except Exception:
            pass
        
        existing_end, existing_count = await get_latest_fetch_map_end(uid, source_channel)
        
        # Get the latest message ID in the channel
        channel_last_id = None
        try:
            async for last_msg in uc.get_chat_history(resolved, limit=1):
                if last_msg and last_msg.id:
                    channel_last_id = last_msg.id
                break
        except Exception as e:
            print(f"[AUTO] Error getting channel last message: {e}")
            try:
                await safe_send(X, notify_chat_id, f"❌ Auto-sync failed: Cannot read channel. Error: {e}")
            except Exception:
                pass
            await deactivate_auto_sync(uid, source_channel)
            return
        
        if not channel_last_id:
            try:
                await safe_send(X, notify_chat_id, "❌ Auto-sync failed: Cannot determine channel's last message.")
            except Exception:
                pass
            await deactivate_auto_sync(uid, source_channel)
            return
        
        # Auto-fetch if needed
        if not existing_end:
            # No fetch map at all — fetch from message 1
            if _is_auto_cancelled(uid, source_channel):
                print(f"[AUTO] Cancelled before fetch for user={uid} channel={source_channel}")
                return
            try:
                await safe_send(X, notify_chat_id, f"📋 No fetch map found. Auto-fetching messages 1-{channel_last_id}...")
            except Exception:
                pass
            await _auto_fetch_range(uid, source_channel, link_type, 1, channel_last_id, uc, ubot, resolved, notify_chat_id)
        elif existing_end < channel_last_id:
            if _is_auto_cancelled(uid, source_channel):
                print(f"[AUTO] Cancelled before gap fetch for user={uid} channel={source_channel}")
                return
            # Fetch map exists but doesn't cover all — fetch the gap
            gap_start = existing_end + 1
            gap_count = channel_last_id - existing_end
            try:
                await safe_send(X, notify_chat_id, f"📋 Fetch map covers up to msg {existing_end}. Auto-fetching {gap_count} new messages ({gap_start}-{channel_last_id})...")
            except Exception:
                pass
            await _auto_fetch_range(uid, source_channel, link_type, gap_start, channel_last_id, uc, ubot, resolved, notify_chat_id)
        else:
            try:
                await safe_send(X, notify_chat_id, f"✅ Fetch map already covers all messages (up to msg {existing_end}).")
            except Exception:
                pass
        
        # ── STEP 2: Check upload_map & upload missing messages ──
        if _is_auto_cancelled(uid, source_channel):
            print(f"[AUTO] Cancelled before upload for user={uid} channel={source_channel}")
            return
        
        # FIX #6: Use MongoDict instead of load_upload_map() to save ~400MB RAM
        from utils.mongo_dict import MongoDict
        msg_id_map = MongoDict(uid=uid, source_channel=str(source_channel), max_cache=1000)
        await msg_id_map.aload_from_upload_maps(limit=500)
        last_uploaded_id = msg_id_map.last_src_id
        stored_dest = msg_id_map.stored_dest_channel
        
        # RAM FIX: Use aggregation pipeline to get ONLY message IDs from fetch_map.
        # Old code loaded the full 20K+ entry msg_map (~200MB) just to get keys.
        # Now we only get the keys — values stay in MongoDB until needed individually.
        from plugins.fetch import get_fetch_map_msg_ids_only
        all_msg_ids = sorted(await get_fetch_map_msg_ids_only(uid, source_channel))
        
        # Check which messages are already uploaded
        uploaded_ids = set()
        for mid in all_msg_ids:
            if mid in msg_id_map:  # Checks cache + pending (fast)
                uploaded_ids.add(mid)
            else:
                # Check MongoDB — needed because MongoDict only caches 1000 entries
                val = await msg_id_map.aget(mid)
                if val is not None:
                    uploaded_ids.add(mid)
        to_upload = [mid for mid in all_msg_ids if mid not in uploaded_ids]
        
        total_to_upload = len(to_upload)
        if total_to_upload > 0:
            try:
                await safe_send(X, notify_chat_id, f"📤 Uploading {total_to_upload} message(s) (skipping {len(uploaded_ids)} already uploaded)...")
            except Exception:
                pass
            
            uploaded_count = 0
            for mid in to_upload:
                # ── TRIPLE CANCEL CHECK ──
                if _is_auto_cancelled(uid, source_channel):
                    print(f"[AUTO] Cancel event set — stopping upload for user={uid} channel={source_channel}")
                    return
                
                # Check MongoDB active flag (fallback)
                auto_cfg = await get_auto_sync(uid, source_channel)
                if not auto_cfg or not auto_cfg.get("active", False):
                    print(f"[AUTO] Auto-sync deactivated (MongoDB) for user={uid} channel={source_channel}")
                    return
                
                # Fetch the message
                try:
                    src_msg = await get_msg(ubot, uc, source_channel, mid, link_type)
                except FloodWait as e:
                    wait_secs = e.value if hasattr(e, 'value') else 30
                    # ANY FloodWait → stop auto-sync immediately
                    print(f"[AUTO] FloodWait {wait_secs}s on fetch — stopping auto-sync")
                    break
                except asyncio.CancelledError:
                    print(f"[AUTO] CancelledError during fetch msg {mid}")
                    raise  # Re-raise so the task actually stops
                except Exception as e:
                    print(f"[AUTO] Error fetching msg {mid}: {e}")
                    continue
                
                if not src_msg:
                    continue
                
                # Process and upload
                try:
                    status, sent_msg_id, is_closed_poll, had_unresolved, _unresolved_ids = await process_msg(
                        X, uc, src_msg, str(uid), link_type, uid, source_channel,
                        link_rewrite_map=msg_id_map,
                        dest_channel_id=dest_channel,
                        dest_channel_username=dest_channel_username,
                        source_channel_username=source_channel_username,
                        source_channel_id=source_channel_id_int,
                        multi_source_channels=_multi_src_channels,
                    )
                    
                    if sent_msg_id:
                        msg_id_map[mid] = sent_msg_id
                        uploaded_count += 1
                        await save_upload_map_incremental(uid, str(source_channel), dest_channel, {mid: sent_msg_id}, mid)
                        await update_auto_last_processed(uid, source_channel, mid)
                        
                        # Track unresolved links for post-batch rewriting
                        if had_unresolved:
                            try:
                                await mark_needs_link_update(uid, str(source_channel), dest_channel, sent_msg_id, mid,
                                                              unresolved_src_ids=list(_unresolved_ids))
                            except Exception as e:
                                print(f"[AUTO] Failed to mark unresolved link: {e}")
                    
                except FloodWait as e:
                    wait_secs = e.value if hasattr(e, 'value') else 30
                    # ANY FloodWait → stop auto-sync immediately
                    print(f"[AUTO] FloodWait {wait_secs}s on upload — stopping auto-sync")
                    break
                except asyncio.CancelledError:
                    print(f"[AUTO] CancelledError during upload msg {mid}")
                    raise  # Re-raise so the task actually stops
                except Exception as e:
                    print(f"[AUTO] Error uploading msg {mid}: {e}")
                
                # Delay between uploads (same as batch) — with cancel check
                for _ in range(12 + (mid % 3)):
                    if _is_auto_cancelled(uid, source_channel):
                        return
                    await asyncio.sleep(1)
            
            try:
                await safe_send(X, notify_chat_id, f"✅ Uploaded {uploaded_count}/{total_to_upload} messages. Entering monitor mode...")
            except Exception:
                pass
            
            # ── RESOLVE PENDING LINK REWRITES after initial upload batch ──
            # Forward references created during the initial upload are now
            # resolvable because all messages have been uploaded.
            try:
                await resolve_pending_link_rewrites(
                    X, ubot, source_channel,
                    dest_channel, dest_channel_username, source_channel_username, uid,
                    source_channel_id=source_channel_id_int,
                    multi_source_channels=_multi_src_channels,
                    combined_msg_id_map=_combined_msg_id_map,
                )
            except Exception as e:
                print(f"[AUTO] Post-upload resolve_pending_link_rewrites failed: {e}")
        else:
            try:
                await safe_send(X, notify_chat_id, f"✅ All {len(uploaded_ids)} messages already uploaded. Entering monitor mode...")
            except Exception:
                pass
        
        # ── STEP 3: Monitor loop — check for new messages every 30s ──
        last_known_id = channel_last_id
        print(f"[AUTO] Entering monitor mode for user={uid} channel={source_channel} (last_id={last_known_id})")
        
        while True:
            # ── TRIPLE CANCEL CHECK ──
            if _is_auto_cancelled(uid, source_channel):
                print(f"[AUTO] Cancel event set — stopping monitor for user={uid} channel={source_channel}")
                try:
                    await safe_send(X, notify_chat_id, f"⏹️ Auto-sync stopped for channel `{source_channel}`.")
                except Exception:
                    pass
                return
            
            # Check MongoDB active flag (fallback)
            auto_cfg = await get_auto_sync(uid, source_channel)
            if not auto_cfg or not auto_cfg.get("active", False):
                print(f"[AUTO] Auto-sync stopped (MongoDB) for user={uid} channel={source_channel}")
                try:
                    await safe_send(X, notify_chat_id, f"⏹️ Auto-sync stopped for channel `{source_channel}`.")
                except Exception:
                    pass
                return
            
            # Check for new messages
            try:
                new_last_id = None
                async for last_msg in uc.get_chat_history(resolved, limit=1):
                    if last_msg and last_msg.id:
                        new_last_id = last_msg.id
                    break
                
                if new_last_id and new_last_id > last_known_id:
                    # New messages found!
                    new_count = new_last_id - last_known_id
                    print(f"[AUTO] {new_count} new message(s) detected for user={uid} channel={source_channel}")
                    
                    # Auto-fetch the new messages first
                    if not _is_auto_cancelled(uid, source_channel):
                        await _auto_fetch_range(uid, source_channel, link_type, last_known_id + 1, new_last_id, uc, ubot, resolved, notify_chat_id)
                    
                    # RAM FIX: No need to reload the full fetch_map (20K+ entries, ~200MB).
                    # The monitor loop iterates by message ID range, so we just need
                    # to check if each mid is already uploaded via MongoDict.
                    # Use MongoDict for O(1) lookups with MongoDB fallback instead of
                    # loading the entire upload_map into a Python dict.
                    from utils.mongo_dict import MongoDict
                    msg_id_map = MongoDict(uid=uid, source_channel=str(source_channel), max_cache=1000)
                    await msg_id_map.aload_from_upload_maps(limit=500)
                    
                    # Upload new messages
                    for mid in range(last_known_id + 1, new_last_id + 1):
                        # ── TRIPLE CANCEL CHECK ──
                        if _is_auto_cancelled(uid, source_channel):
                            return
                        
                        # Check MongoDB active flag
                        auto_cfg = await get_auto_sync(uid, source_channel)
                        if not auto_cfg or not auto_cfg.get("active", False):
                            return
                        
                        # Check if already uploaded (MongoDict: cache → pending → MongoDB)
                        val = await msg_id_map.aget(mid)
                        if val is not None:
                            continue
                        
                        # Fetch
                        try:
                            src_msg = await get_msg(ubot, uc, source_channel, mid, link_type)
                        except FloodWait as e:
                            wait_secs = e.value if hasattr(e, 'value') else 30
                            # ANY FloodWait → stop auto-sync immediately
                            print(f"[AUTO] Monitor: FloodWait {wait_secs}s on fetch — stopping auto-sync")
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            print(f"[AUTO] Monitor: Error fetching msg {mid}: {e}")
                            continue
                        
                        if not src_msg:
                            continue
                        
                        # Upload
                        try:
                            status, sent_msg_id, _, had_unresolved, _unresolved_ids = await process_msg(
                                X, uc, src_msg, str(uid), link_type, uid, source_channel,
                                link_rewrite_map=msg_id_map,
                                dest_channel_id=dest_channel,
                                dest_channel_username=dest_channel_username,
                                source_channel_username=source_channel_username,
                                source_channel_id=source_channel_id_int,
                                multi_source_channels=_multi_src_channels,
                            )
                            
                            if sent_msg_id:
                                msg_id_map[mid] = sent_msg_id
                                await save_upload_map_incremental(uid, str(source_channel), dest_channel, {mid: sent_msg_id}, mid)
                                
                                # Track unresolved links for post-batch rewriting
                                if had_unresolved:
                                    try:
                                        await mark_needs_link_update(uid, str(source_channel), dest_channel, sent_msg_id, mid,
                                                                      unresolved_src_ids=list(_unresolved_ids))
                                    except Exception as e:
                                        print(f"[AUTO] Monitor: Failed to mark unresolved link: {e}")
                        except FloodWait as e:
                            wait_secs = e.value if hasattr(e, 'value') else 30
                            # ANY FloodWait → stop auto-sync immediately
                            print(f"[AUTO] Monitor: FloodWait {wait_secs}s on upload — stopping auto-sync")
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            print(f"[AUTO] Monitor: Error uploading msg {mid}: {e}")
                        
                        # Delay between uploads — with cancel check
                        for _ in range(12 + (mid % 3)):
                            if _is_auto_cancelled(uid, source_channel):
                                return
                            await asyncio.sleep(1)
                    
                    last_known_id = new_last_id
                    await update_auto_last_processed(uid, source_channel, new_last_id)
                    
                    # ── RESOLVE PENDING LINK REWRITES after new uploads ──
                    # Forward references (msg A references msg B uploaded later)
                    # are only resolvable AFTER B is uploaded. This ensures
                    # ALL links get rewritten, not just backward references.
                    try:
                        await resolve_pending_link_rewrites(
                            X, ubot, source_channel,
                            dest_channel, dest_channel_username, source_channel_username, uid,
                            source_channel_id=source_channel_id_int,
                            multi_source_channels=_multi_src_channels,
                            combined_msg_id_map=_combined_msg_id_map,
                        )
                    except Exception as e:
                        print(f"[AUTO] Monitor: resolve_pending_link_rewrites failed: {e}")
            
            except asyncio.CancelledError:
                print(f"[AUTO] CancelledError in monitor loop for user={uid} channel={source_channel}")
                raise
            except Exception as e:
                print(f"[AUTO] Monitor error: {e}")
            
            # Wait before next check (30s) — with cancel check every 5s
            for _ in range(6):  # 6 × 5s = 30s total
                if _is_auto_cancelled(uid, source_channel):
                    return
                await asyncio.sleep(5)
    
    except asyncio.CancelledError:
        print(f"[AUTO] Task cancelled for user={uid} channel={source_channel}")
        # Don't re-raise — just exit cleanly
        return
    except Exception as e:
        print(f"[AUTO] Fatal error for user={uid} channel={source_channel}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up cancel event
        if task_key in AUTO_CANCEL_EVENTS:
            try:
                del AUTO_CANCEL_EVENTS[task_key]
            except Exception:
                pass
        # Remove from AUTO_TASKS if still there
        if task_key in AUTO_TASKS:
            try:
                del AUTO_TASKS[task_key]
            except Exception:
                pass
        print(f"[AUTO] Clean exit for user={uid} channel={source_channel}")


async def _auto_fetch_range(uid, channel_id, link_type, start_id, end_id, uc, ubot, resolved, notify_chat_id):
    """Auto-fetch a range of messages and store in fetch_map.
    
    This is a lightweight version of run_fetch_scan that doesn't need a message object.
    """
    from plugins.fetch import save_fetch_map
    
    msg_map = {}
    stats = {
        "total": 0,
        "video": 0, "photo": 0, "audio": 0, "document": 0,
        "text": 0, "sticker": 0, "poll": 0, "other": 0,
        "errors": 0
    }
    
    message_ids = list(range(start_id, end_id + 1))
    
    for chunk_start in range(0, len(message_ids), 100):
        # Cancel check
        if _is_auto_cancelled(uid, channel_id):
            break
        
        chunk_ids = message_ids[chunk_start:chunk_start + 100]
        
        try:
            messages = await uc.get_messages(resolved, chunk_ids)
            if messages and not isinstance(messages, list):
                messages = [messages]
            
            fetched_in_chunk = set()
            if messages:
                for msg in messages:
                    if not msg or getattr(msg, 'empty', False):
                        stats["errors"] += 1
                        continue
                    fetched_in_chunk.add(msg.id)
                    
                    mid = msg.id
                    has_media = bool(msg.media)
                    media_type = None
                    
                    if msg.poll:
                        media_type = "poll"
                    elif msg.video:
                        media_type = "video"
                    elif msg.video_note:
                        media_type = "video_note"
                    elif msg.voice:
                        media_type = "voice"
                    elif msg.sticker:
                        media_type = "sticker"
                    elif msg.audio:
                        media_type = "audio"
                    elif msg.photo:
                        media_type = "photo"
                    elif msg.document:
                        media_type = "document"
                    
                    msg_map[str(mid)] = {
                        "has_media": has_media,
                        "media_type": media_type,
                        "is_pinned": False
                    }
                    
                    if media_type in stats:
                        stats[media_type] += 1
                    elif has_media:
                        stats["other"] += 1
                    else:
                        stats["text"] += 1
                    stats["total"] += 1
            
        except FloodWait as e:
            wait_secs = e.value if hasattr(e, 'value') else 30
            # ANY FloodWait → stop auto-sync immediately
            print(f"[AUTO] FloodWait {wait_secs}s during fetch — stopping auto-sync")
            break
        except Exception as e:
            print(f"[AUTO] Fetch chunk error: {e}")
            stats["errors"] += len(chunk_ids)
        
        # Small delay between chunks
        await asyncio.sleep(0.5)
    
    # Save the fetch map
    if msg_map:
        await save_fetch_map(uid, channel_id, link_type, start_id, end_id, msg_map, stats)
        print(f"[AUTO] Saved fetch map: {start_id}-{end_id} ({len(msg_map)} messages)")


# ─── TEXT HANDLER FOR /AUTO CONVERSATION ─────────────────────────────────────

@X.on_message(
    filters.text & filters.private & ~login_in_progress & ~filters.command([
        'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id',
        'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt',
        'decrypt', 'keys', 'setbot', 'rembot', 'auth', 'unauth', 'authusers',
        'logs', 'fetch', 'cancelfetch', 'fetchmaps', 'clearfetch', 'answerkey',
        'clearanswerkey', 'viewanswerkey', 'viewfetchmaps', 'auto', 'autooff', 'cancelauto',
        'settings', 'help', 'terms', 'plan', 'status', 'clearbatch', 'linkexplan', 'explans', 'transfer', 'rem',
        'dl', 'adl', 'mirror', 'mirrorstop', 'mirrorstatus', 'explanlogs'
    ])
)
async def auto_text_handler(c, m):
    """Handle text input during /auto conversation."""
    uid = m.from_user.id
    
    if uid not in AUTO_STATE:
        from pyrogram import ContinuePropagation
        raise ContinuePropagation
    
    state = AUTO_STATE[uid]
    
    if state['step'] == 'source':
        # User sent source channel link
        L = m.text.strip()
        i, d, lt = E(L)
        if not i or not d:
            await safe_reply(m, 'Invalid link format. Send a valid Telegram message link.')
            del AUTO_STATE[uid]
            return
        
        # Save source info and move to dest step
        state['source_channel'] = i
        state['link_type'] = lt
        state['step'] = 'dest'
        
        # Show current dest as hint
        cfg_chat = await get_user_data_key(str(uid), 'chat_id', None)
        dest_hint = ""
        if cfg_chat:
            dest_hint = f"\n\n💡 Your saved destination: `{cfg_chat}`\nSend **same** to use it."
        
        await safe_reply(m,
            f"✅ **Source channel:** `{i}`\n\n"
            f"**Step 2 of 2:** Send the **destination channel ID** (integer).\n\n"
            f"Examples:\n"
            f"• `-1001234567890` (private channel)\n"
            f"• `-1001234567890/28646` (forum topic)\n"
            f"• `same` (use your saved destination){dest_hint}"
        )
    
    elif state['step'] == 'dest':
        # User sent destination channel ID
        L = m.text.strip()
        
        source_channel = state['source_channel']
        lt = state['link_type']
        
        # Parse destination: supports "-1001234567890" or "-1001234567890/28646" (forum topic)
        # We store the FULL string (with /topic) in user settings so process_msg
        # can extract both channel_id and message_thread_id.
        dest_chat_str = None  # Full string for settings (e.g. "-1003745613477/8809")
        dest_channel = None   # Integer channel ID only (for MongoDB, notifications)
        
        if L.lower() == 'same':
            cfg_chat = await get_user_data_key(str(uid), 'chat_id', None)
            if not cfg_chat:
                await safe_reply(m, "❌ No default destination set! Send a channel ID like `-1001234567890`.")
                return
            try:
                if '/' in cfg_chat:
                    dest_channel = int(cfg_chat.split('/')[0])
                else:
                    dest_channel = int(cfg_chat)
                dest_chat_str = cfg_chat  # Keep full format including topic
            except Exception:
                await safe_reply(m, "❌ Invalid default destination. Send a channel ID like `-1001234567890`.")
                del AUTO_STATE[uid]
                return
        else:
            # Parse destination channel ID directly (integer, NOT a link)
            try:
                if '/' in L:
                    dest_channel = int(L.split('/')[0])
                    dest_chat_str = L  # Keep full format: "-1003745613477/8809"
                else:
                    dest_channel = int(L)
                    dest_chat_str = L  # Just channel: "-1003745613477"
            except ValueError:
                await safe_reply(m,
                    '❌ Invalid channel ID. Send a **numeric ID** like:\n'
                    '• `-1001234567890`\n'
                    '• `-1001234567890/28646` (for forum topic)\n\n'
                    'Do NOT send a link — send the integer channel ID.'
                )
                return
        
        # Check if this source is already being auto-synced — stop old one first
        task_key = (uid, str(source_channel))
        if task_key in AUTO_TASKS:
            stop_auto_task(uid, source_channel)
        
        del AUTO_STATE[uid]
        
        # Save FULL destination string to user settings (so /batch, process_msg etc. also use it)
        # process_msg reads chat_id and parses "channel_id/topic_id" format to extract
        # both tcid (channel) and rtmid (message_thread_id for forum topics)
        try:
            await save_user_data(str(uid), 'chat_id', dest_chat_str)
        except Exception as e:
            print(f"[AUTO] Failed to save chat_id to settings: {e}")
        
        # Save auto-sync config to MongoDB (channel ID only for MongoDB)
        await save_auto_sync(uid, source_channel, dest_channel, lt, last_processed_id=0)
        
        # Create cancel event for this auto-sync
        AUTO_CANCEL_EVENTS[task_key] = asyncio.Event()
        _clear_auto_cancel(uid, source_channel)
        
        # Start the auto-sync background task
        task = asyncio.create_task(
            run_auto_sync(uid, source_channel, dest_channel, lt, notify_chat_id=m.chat.id)
        )
        AUTO_TASKS[task_key] = task
        
        # Count total active
        active_count = sum(1 for k in AUTO_TASKS if k[0] == uid)
        
        await safe_reply(m,
            f"✅ **Auto-sync started!**\n\n"
            f"📡 Source: `{source_channel}`\n"
            f"📤 Destination: `{dest_chat_str}`\n"
            f"📊 Total active: {active_count}\n\n"
            f"🔄 Bot will now:\n"
            f"• Auto-fetch missing messages\n"
            f"• Upload new messages\n"
            f"• Monitor for new posts & auto-upload\n\n"
            f"⏹️ Use **/autooff** or **/stop** to stop."
        )
    
    elif state['step'] == 'autooff_pick':
        # User picked a number from /autooff list
        L = m.text.strip()
        try:
            pick = int(L)
        except ValueError:
            await safe_reply(m, "Send a number from the list, or /cancelauto to cancel.")
            return
        
        active_list = state.get('list', [])
        if pick < 1 or pick > len(active_list):
            await safe_reply(m, f"Invalid number. Pick 1-{len(active_list)}.")
            return
        
        chosen = active_list[pick - 1]
        source_ch = chosen["source_channel"]
        
        stop_auto_task(uid, source_ch)
        await deactivate_auto_sync(uid, source_ch)
        del AUTO_STATE[uid]
        
        await safe_reply(m, f"✅ Stopped auto-sync for `{source_ch}`.")


# ─── RECOVERY: Restart auto-syncs on bot startup ────────────────────────────

async def recover_auto_syncs():
    """Check MongoDB for active auto-syncs and restart them.
    Called on bot startup to resume auto-syncs that were running before a crash/restart.
    """
    active = await list_active_auto_syncs()
    if not active:
        return
    
    print(f"[AUTO] Recovering {len(active)} auto-sync(s)...")
    
    for cfg in active:
        uid = cfg["user_id"]
        source = cfg["source_channel"]
        dest = cfg.get("dest_channel")
        lt = cfg.get("link_type", "private")
        last_id = cfg.get("last_processed_id", 0)
        
        # Don't start duplicate tasks
        task_key = (uid, str(source))
        if task_key in AUTO_TASKS:
            continue
        
        print(f"[AUTO] Restarting auto-sync for user={uid} channel={source} (last_id={last_id})")
        
        # Create cancel event
        AUTO_CANCEL_EVENTS[task_key] = asyncio.Event()
        
        task = asyncio.create_task(
            run_auto_sync(uid, source, dest, lt, notify_chat_id=uid)
        )
        AUTO_TASKS[task_key] = task


# Schedule recovery after bot starts
async def delayed_recovery():
    """Wait for bot to be fully started, then recover auto-syncs."""
    await asyncio.sleep(10)  # Give bot time to initialize
    await recover_auto_syncs()

asyncio.ensure_future(delayed_recovery())
