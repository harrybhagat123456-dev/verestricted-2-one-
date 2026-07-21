# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

"""
TEST FEATURE: /fetch command — Pre-scan messages and store lightweight map in MongoDB.

The /fetch command scans a range of messages in a channel and stores only
metadata (media type) — NOT the full Message objects.

When /batch runs, it checks for an existing fetch map and streams messages
one-by-one instead of pre-fetching all messages into memory (50-100 MB burst).

Memory savings: ~50-100 MB per batch (replaced with ~150 KB map in MongoDB).

Usage:
    /fetch <start_link>
    Bot: Send count or last link
    User: 5000 (or last link)
    Bot: Scans messages, stores map, reports stats

Limits:
    No limit — fetch ALL messages
    Chunk size: 5,000 messages per MongoDB document
"""

import os
import re
import time
import asyncio
import json
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, ChatIdInvalid, PeerIdInvalid, ChannelPrivate
from pyrogram import ContinuePropagation
from plugins.pin_map import fetch_all_pinned_ids
from shared_client import app as X
from config import OWNER_ID
from utils.func import E, is_auth_user
from utils.func import get_user_data, get_user_data_key
from utils.custom_filters import login_in_progress
from utils.ram_monitor import log_ram
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


# MongoDB collection for fetch maps
_mongo_client = AsyncIOMotorClient(MONGO_URI)
_db = _mongo_client[DB_NAME]
fetch_maps_collection = _db["fetch_maps"]
answer_keys_collection = _db["answer_keys"]
dependencies_collection = _db["dependencies"]  # Method 1: poll → question image dependency index

# Conversation state for /fetch command
FETCH_STATE = {}  # uid -> {'step': 'start'|'count', 'cid': ..., 'sid': ..., 'lt': ...}

# Limits
DEFAULT_FETCH_LIMIT = 100000  # No practical limit — fetch ALL messages
OWNER_FETCH_LIMIT = 100000
CHUNK_SIZE = 5000


# ─── FETCH MAP HELPERS ────────────────────────────────────────────────────────

async def save_fetch_map(user_id, channel_id, channel_type, start_msg_id, end_msg_id, msg_map, stats):
    """Save a fetch map chunk to MongoDB.
    
    Each chunk covers up to CHUNK_SIZE messages.
    Multiple chunks can exist for the same user+channel (contiguous ranges).
    """
    doc = {
        "user_id": user_id,
        "channel_id": str(channel_id),
        "channel_type": channel_type,
        "start_msg_id": start_msg_id,
        "end_msg_id": end_msg_id,
        "created_at": datetime.now(),
        "stats": stats,
        "msg_map": msg_map,  # dict: str(msg_id) -> {"has_media": bool, "media_type": str|null, "is_pinned": bool}
    }
    
    await fetch_maps_collection.update_one(
        {
            "user_id": user_id,
            "channel_id": str(channel_id),
            "start_msg_id": start_msg_id,
            "end_msg_id": end_msg_id,
        },
        {"$set": doc},
        upsert=True
    )
    
    print(f"[FETCH] Saved map for user={user_id} channel={channel_id} range={start_msg_id}-{end_msg_id} ({len(msg_map)} messages)")


async def get_fetch_map(user_id, channel_id, start_msg_id, end_msg_id):
    """Get a fetch map from MongoDB. Returns None if not found."""
    doc = await fetch_maps_collection.find_one({
        "user_id": user_id,
        "channel_id": str(channel_id),
        "start_msg_id": {"$lte": start_msg_id},
        "end_msg_id": {"$gte": end_msg_id},
    })
    
    if doc:
        return doc.get("msg_map", {})
    
    # Try finding overlapping chunks
    chunks = await fetch_maps_collection.find({
        "user_id": user_id,
        "channel_id": str(channel_id),
        "start_msg_id": {"$lte": end_msg_id},
        "end_msg_id": {"$gte": start_msg_id},
    }).to_list(length=None)
    
    if not chunks:
        return None
    
    # Merge chunks
    merged = {}
    for chunk in chunks:
        chunk_map = chunk.get("msg_map", {})
        merged.update(chunk_map)
    
    return merged if merged else None


async def get_latest_fetch_map_end(user_id, channel_id):
    """Find the highest end_msg_id across all fetch maps for a user+channel.
    Returns (end_msg_id, total_messages_in_map) or (None, 0) if no map exists.
    Used for smart merge — only fetch NEW messages after the last scanned point.
    If channel_id is None, searches across ALL channels.
    """
    query = {"user_id": user_id}
    if channel_id:
        query["channel_id"] = str(channel_id)
    
    chunks = await fetch_maps_collection.find(query).to_list(length=None)
    
    if not chunks:
        return None, 0
    
    max_end = 0
    total_msgs = 0
    for chunk in chunks:
        end_id = chunk.get("end_msg_id", 0)
        if end_id > max_end:
            max_end = end_id
        chunk_map = chunk.get("msg_map", {})
        total_msgs += len(chunk_map)
    
    return max_end if max_end > 0 else None, total_msgs


async def delete_fetch_map(user_id, channel_id=None):
    """Delete fetch maps for a user. If channel_id specified, only delete for that channel."""
    query = {"user_id": user_id}
    if channel_id:
        query["channel_id"] = str(channel_id)
    result = await fetch_maps_collection.delete_many(query)
    return result.deleted_count


async def list_fetch_maps(user_id):
    """List all fetch maps for a user."""
    cursor = fetch_maps_collection.find(
        {"user_id": user_id},
        {"channel_id": 1, "start_msg_id": 1, "end_msg_id": 1, "stats": 1, "created_at": 1}
    )
    return await cursor.to_list(length=None)


# ─── ANSWER KEY HELPERS ──────────────────────────────────────────────────────

async def save_answer_key(user_id, name, channel_id, content, total_questions, map_sources):
    """Save an answer key to MongoDB with a unique name."""
    doc = {
        "user_id": user_id,
        "name": name,
        "channel_id": str(channel_id),
        "content": content,
        "total_questions": total_questions,
        "map_sources": map_sources,
        "created_at": datetime.now()
    }
    await answer_keys_collection.update_one(
        {"user_id": user_id, "name": name},
        {"$set": doc},
        upsert=True
    )
    print(f"[ANSWERKEY] Saved key '{name}' for user={user_id} ({total_questions} questions)")


async def list_answer_keys(user_id):
    """List all answer keys for a user."""
    cursor = answer_keys_collection.find(
        {"user_id": user_id},
        {"name": 1, "channel_id": 1, "total_questions": 1, "created_at": 1}
    )
    return await cursor.to_list(length=None)


async def get_answer_key(user_id, name):
    """Get a specific answer key by name."""
    return await answer_keys_collection.find_one({"user_id": user_id, "name": name})


async def delete_answer_key(user_id, name=None):
    """Delete answer keys for a user. If name specified, only delete that one."""
    query = {"user_id": user_id}
    if name:
        query["name"] = name
    result = await answer_keys_collection.delete_many(query)
    return result.deleted_count


# ─── RESOLVE CHAT HELPER ─────────────────────────────────────────────────────

async def resolve_chat_for_fetch(client, chat_id):
    """Resolve a chat peer before interacting with it."""
    try:
        if isinstance(chat_id, str) and not chat_id.lstrip('-').isdigit():
            chat = await client.get_chat(chat_id)
            return chat.id if chat else chat_id
        elif isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id = int(chat_id)
        
        if isinstance(chat_id, int):
            try:
                await client.resolve_peer(chat_id)
            except Exception:
                try:
                    chat = await client.get_chat(chat_id)
                    return chat.id if chat else chat_id
                except Exception:
                    pass
        return chat_id
    except Exception as e:
        print(f"[FETCH] resolve_chat warning: {e}")
        return chat_id


# ─── /FETCH COMMAND ───────────────────────────────────────────────────────────

@X.on_message(filters.command("fetch"))
async def fetch_cmd(c, m):
    """Start the /fetch conversation — ask for start link."""
    uid = m.from_user.id
    
    # Only owner + auth users
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return  # Silently ignore
    
    if uid in FETCH_STATE:
        await safe_reply(m, "You already have a /fetch in progress. Send /cancelfetch to cancel.")
        return
    
    # Check if user has an active batch or is in batch conversation
    from plugins.batch import is_user_active, Z as batch_Z
    if is_user_active(uid):
        await safe_reply(m, "You have an active batch. Wait for it to finish or /stop it first.")
        return
    if uid in batch_Z:
        await safe_reply(m, "You have a /batch in progress. Send /cancel first.")
        return
    
    FETCH_STATE[uid] = {'step': 'start'}
    await safe_reply(m,
        "**📋 /fetch — Pre-scan Messages**\n\n"
        "Scans messages and stores a lightweight map (no downloads).\n"
        "Later, /batch uses this map to stream one-by-one instead of "
        "loading all into memory.\n\n"
        "**🔄 Smart Merge:** If a fetch map already exists for this channel, "
        "only NEW messages will be scanned and merged!\n\n"
        "**Send the start link** of the channel/group.\n\n"
        "**3 input options after link:**\n"
        "1️⃣ **Number** — e.g. `5000` (scan 5000 msgs)\n"
        "2️⃣ **Last link** — scan from start to that link\n"
        "3️⃣ **all** — scan ALL messages to the end"
    )


@X.on_message(filters.command("cancelfetch"))
async def cancel_fetch_cmd(c, m):
    """Cancel an in-progress /fetch."""
    uid = m.from_user.id
    if uid in FETCH_STATE:
        del FETCH_STATE[uid]
        await safe_reply(m, "✅ /fetch cancelled.")
    else:
        await safe_reply(m, "No /fetch in progress.")


# ─── /FETCHMAPS COMMAND ──────────────────────────────────────────────────────

@X.on_message(filters.command("fetchmaps"))
async def fetchmaps_cmd(c, m):
    """List all stored fetch maps for the user. Owner + auth users."""
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    maps = await list_fetch_maps(uid)
    
    if not maps:
        await safe_reply(m, "No fetch maps stored. Use /fetch to create one.")
        return
    
    lines = ["📋 **Your Fetch Maps:**\n"]
    for i, fm in enumerate(maps, 1):
        ch = fm.get("channel_id", "Unknown")
        start = fm.get("start_msg_id", "?")
        end = fm.get("end_msg_id", "?")
        stats = fm.get("stats", {})
        created = fm.get("created_at", None)
        date_str = created.strftime("%d-%b-%Y %H:%M") if created else "Unknown"
        total = stats.get("total", 0)
        lines.append(f"{i}. Channel: `{ch}` | Range: {start}-{end} | {total} msgs | {date_str}")
    
    lines.append(f"\n**Total:** {len(maps)} map(s)")
    await safe_reply(m, "\n".join(lines))


# ─── /VIEWFETCHMAPS COMMAND ─────────────────────────────────────────────────

VIEWFETCHMAPS_STATE = {}  # uid -> {'maps': [...]}

@X.on_message(filters.command("viewfetchmaps"))
async def viewfetchmaps_cmd(c, m):
    """Show fetch maps list, then user picks one to view as TXT file. Owner + auth users."""
    try:
        uid = m.from_user.id
        print(f"[VIEWFETCHMAPS] Command received from uid={uid}")
        if uid not in OWNER_ID and not await is_auth_user(uid):
            print(f"[VIEWFETCHMAPS] uid={uid} not authorized — ignoring")
            return
        
        maps = await list_fetch_maps(uid)
        print(f"[VIEWFETCHMAPS] Found {len(maps) if maps else 0} fetch maps for uid={uid}")
        
        if not maps:
            await safe_reply(m, "No fetch maps stored. Use /fetch to create one.")
            return
        
        lines = ["📋 **Your Fetch Maps:**\n"]
        for i, fm in enumerate(maps, 1):
            ch = fm.get("channel_id", "Unknown")
            start = fm.get("start_msg_id", "?")
            end = fm.get("end_msg_id", "?")
            stats = fm.get("stats", {})
            created = fm.get("created_at", None)
            date_str = created.strftime("%d-%b-%Y %H:%M") if created else "Unknown"
            total = stats.get("total", 0)
            lines.append(f"{i}. Channel: `{ch}` | Range: {start}-{end} | {total} msgs | {date_str}")
        
        lines.append(f"\n**Total:** {len(maps)} map(s)")
        lines.append("\nReply with:")
        lines.append("• **Number** (e.g. `1`) — view that map as TXT file")
        lines.append("• **cancel** — cancel")
        
        VIEWFETCHMAPS_STATE[uid] = {'maps': maps}
        await safe_reply(m, "\n".join(lines))
        print(f"[VIEWFETCHMAPS] Sent list to uid={uid}, waiting for selection")
    except Exception as e:
        import traceback
        print(f"[VIEWFETCHMAPS] ERROR: {e}")
        traceback.print_exc()
        try:
            await safe_reply(m, f"❌ Error: {e}")
        except Exception:
            pass


# ─── /CLEARFETCH COMMAND ─────────────────────────────────────────────────────

CLEARFETCH_STATE = {}  # uid -> waiting for selection

@X.on_message(filters.command("clearfetch"))
async def clearfetch_cmd(c, m):
    """Show fetch maps first, then ask which to delete. Owner + auth users."""
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    maps = await list_fetch_maps(uid)
    
    if not maps:
        await safe_reply(m, "No fetch maps to delete. Use /fetch to create one.")
        return
    
    lines = ["📋 **Your Fetch Maps:**\n"]
    for i, fm in enumerate(maps, 1):
        ch = fm.get("channel_id", "Unknown")
        start = fm.get("start_msg_id", "?")
        end = fm.get("end_msg_id", "?")
        stats = fm.get("stats", {})
        total = stats.get("total", 0)
        lines.append(f"{i}. Channel: `{ch}` | Range: {start}-{end} | {total} msgs")
    
    lines.append(f"\n**Total:** {len(maps)} map(s)")
    lines.append("\nReply with:")
    lines.append("• **Number** (e.g. `1`) — delete that specific map")
    lines.append("• **all** — delete all fetch maps")
    lines.append("• **cancel** — don't delete anything")
    
    CLEARFETCH_STATE[uid] = {'maps': maps}
    await safe_reply(m, "\n".join(lines))


# ─── /ANSWERKEY COMMAND ──────────────────────────────────────────────────────

ANSWERKEY_STATE = {}  # uid -> {'step': 'channel'|'name', 'channel_id': ..., ...}
CLEARANSWERKEY_STATE = {}  # uid -> {'keys': [...]}
VIEWANSWERKEY_STATE = {}  # uid -> {'keys': [...]}

@X.on_message(filters.command("answerkey"))
async def answerkey_cmd(c, m):
    """Generate answer key from fetched quiz/poll messages. Available to all auth users.
    
    How it works:
    1. User sends a channel link
    2. User names the answer key (unique name for later retrieval)
    3. Bot finds ALL fetch maps for that channel (Map1, Map2, Map3...)
    4. Merges all maps in order — uses already-fetched data
    5. Finds all poll/quiz message IDs from the merged map
    6. Fetches only the poll messages, reveals correct answers
    7. Generates .txt answer key with Q1, Q2... numbering
    8. Stores in MongoDB with the custom name for /viewanswerkey
    """
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    if uid in ANSWERKEY_STATE:
        del ANSWERKEY_STATE[uid]
    
    ANSWERKEY_STATE[uid] = {'step': 'channel'}
    await safe_reply(m,
        "**🔑 /answerkey — Generate Answer Key**\n\n"
        "Creates a .txt file with all quiz/poll questions and correct answers.\n\n"
        "**How it works:**\n"
        "• Finds ALL your fetch maps for the channel\n"
        "• Merges them in order (Map1 → Map2 → Map3...)\n"
        "• Uses already-fetched data to find polls instantly\n"
        "• Reveals correct answers for quizzes\n"
        "• **Saves with a custom name** for later viewing\n\n"
        "**Step 1:** Send any message link from the channel you want the answer key for."
    )


# ─── /CLEARANSWERKEY COMMAND ────────────────────────────────────────────────

@X.on_message(filters.command("clearanswerkey"))
async def clearanswerkey_cmd(c, m):
    """Show answer keys first, then ask which to delete. Owner + auth users."""
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    keys = await list_answer_keys(uid)
    
    if not keys:
        await safe_reply(m, "No answer keys stored. Use /answerkey to create one.")
        return
    
    lines = ["📋 **Your Answer Keys:**\n"]
    for i, ak in enumerate(keys, 1):
        name = ak.get("name", "Unknown")
        ch = ak.get("channel_id", "Unknown")
        total = ak.get("total_questions", 0)
        created = ak.get("created_at", None)
        date_str = created.strftime("%d-%b-%Y %H:%M") if created else "Unknown"
        lines.append(f"{i}. **{name}** | Channel: `{ch}` | {total} Qs | {date_str}")
    
    lines.append(f"\n**Total:** {len(keys)} key(s)")
    lines.append("\nReply with:")
    lines.append("• **Number** (e.g. `1`) — delete that specific key")
    lines.append("• **all** — delete all answer keys")
    lines.append("• **cancel** — don't delete anything")
    
    CLEARANSWERKEY_STATE[uid] = {'keys': keys}
    await safe_reply(m, "\n".join(lines))


# ─── /VIEWANSWERKEY COMMAND ─────────────────────────────────────────────────

@X.on_message(filters.command("viewanswerkey"))
async def viewanswerkey_cmd(c, m):
    """Show answer keys list, then user picks one to view. Owner + auth users."""
    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    keys = await list_answer_keys(uid)
    
    if not keys:
        await safe_reply(m, "No answer keys stored. Use /answerkey to create one.")
        return
    
    lines = ["📋 **Your Answer Keys:**\n"]
    for i, ak in enumerate(keys, 1):
        name = ak.get("name", "Unknown")
        ch = ak.get("channel_id", "Unknown")
        total = ak.get("total_questions", 0)
        lines.append(f"{i}. **{name}** | Channel: `{ch}` | {total} Qs")
    
    lines.append(f"\n**Total:** {len(keys)} key(s)")
    lines.append("\nReply with:")
    lines.append("• **Number** (e.g. `1`) — view that key")
    lines.append("• **Name** — type the exact name to view")
    lines.append("• **cancel** — cancel")
    
    VIEWANSWERKEY_STATE[uid] = {'keys': keys}
    await safe_reply(m, "\n".join(lines))


# ─── ANSWER KEY GENERATION ────────────────────────────────────────────────────

async def generate_answer_key(c, m, uid, channel_id, link_type, name="answer_key"):
    """Generate answer key .txt from quiz/poll messages using fetch maps.
    
    Flow:
    1. Find ALL fetch maps for this channel (Map1, Map2, Map3...)
    2. Merge all maps in order — uses already-fetched poll data
    3. Find all poll message IDs from the merged map
    4. If gaps exist between maps, warn user + auto-fetch gap messages
    5. For each poll: fetch it, reveal correct answer via _get_correct_option()
    6. Build .txt with Q1, Q2... format
    7. Save to MongoDB with the custom name for /viewanswerkey
    """
    pt = await safe_reply(m, "🔑 Looking for fetch maps...")
    
    # Get clients
    from plugins.batch import get_ubot, get_uclient, get_Y, resolve_chat
    from plugins.batch import _get_correct_option
    
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
        await safe_edit(pt, "❌ No user client available. Use /login first.")
        return
    
    # ── STEP 1: Find ALL fetch maps for this channel ──
    maps = await fetch_maps_collection.find({
        "user_id": uid,
        "channel_id": str(channel_id)
    }).sort("start_msg_id", 1).to_list(length=None)
    
    if not maps:
        await safe_edit(pt,
            "❌ No fetch maps found for this channel.\n\n"
            "Use /fetch first to scan the channel, then /answerkey to get answers."
        )
        return
    
    # ── STEP 2: Show maps found and merge them ──
    map_lines = ["📋 **Fetch maps found for this channel:**\n"]
    merged_map = {}  # str(msg_id) -> info dict
    map_ranges = []  # [(start, end), ...] for gap detection
    
    for i, fm in enumerate(maps, 1):
        start = fm.get("start_msg_id", 0)
        end = fm.get("end_msg_id", 0)
        stats = fm.get("stats", {})
        total = stats.get("total", 0)
        polls = stats.get("poll", 0)
        msg_map = fm.get("msg_map", {})
        
        map_lines.append(f"  Map {i}: msgs {start}-{end} | {total} msgs | {polls} polls")
        merged_map.update(msg_map)
        map_ranges.append((start, end))
    
    # ── STEP 3: Detect gaps between maps ──
    gaps = []
    for i in range(len(map_ranges) - 1):
        current_end = map_ranges[i][1]
        next_start = map_ranges[i + 1][0]
        if next_start > current_end + 1:
            gap_start = current_end + 1
            gap_end = next_start - 1
            gaps.append((gap_start, gap_end))
            map_lines.append(f"  ⚠️ Gap detected: msgs {gap_start}-{gap_end} (not fetched)")
    
    # Count polls from merged map
    poll_ids = []
    for mid_str, info in merged_map.items():
        if info.get("media_type") == "poll":
            poll_ids.append(int(mid_str))
    poll_ids.sort()
    
    total_msgs_in_map = len(merged_map)
    total_polls = len(poll_ids)
    
    map_lines.append(f"\n**Total:** {len(maps)} map(s) | {total_msgs_in_map} messages | {total_polls} polls")
    
    if gaps:
        gap_count = sum(g[1] - g[0] + 1 for g in gaps)
        map_lines.append(f"\n⚠️ **{len(gaps)} gap(s)** totaling {gap_count} unfetched messages")
        map_lines.append("🔄 Will auto-fetch gaps to find any hidden polls...")
    
    await safe_edit(pt, "\n".join(map_lines))
    await asyncio.sleep(1)
    
    # ── STEP 4: Auto-fetch gap messages to find hidden polls ──
    gap_poll_ids = []
    gap_fetched_ids = set()  # Track which gap messages were successfully fetched
    gap_failed_ids = []      # Track which gap messages failed
    if gaps:
        resolved = await resolve_chat(uc, channel_id)
        
        for gap_start, gap_end in gaps:
            gap_ids = list(range(gap_start, gap_end + 1))
            chunk_size = 100
            
            for chunk_start in range(0, len(gap_ids), chunk_size):
                chunk = gap_ids[chunk_start:chunk_start + chunk_size]
                try:
                    messages = await uc.get_messages(resolved, chunk)
                    if messages and not isinstance(messages, list):
                        messages = [messages]
                    
                    fetched_in_chunk = set()
                    if messages:
                        for msg in messages:
                            if msg and not getattr(msg, 'empty', False):
                                fetched_in_chunk.add(msg.id)
                                if msg.poll:
                                    gap_poll_ids.append(msg.id)
                    
                    # Track which IDs were fetched vs failed
                    for cid in chunk:
                        if cid in fetched_in_chunk:
                            gap_fetched_ids.add(cid)
                        else:
                            gap_failed_ids.append(cid)
                            
                except FloodWait as e:
                    wait_time = e.value if hasattr(e, 'value') else 30
                    print(f"[ANSWERKEY] Gap scan FloodWait: {wait_time}s — skipping chunk")
                    gap_failed_ids.extend(chunk)
                    continue
                except Exception as e:
                    print(f"[ANSWERKEY] Gap fetch error: {e}")
                    gap_failed_ids.extend(chunk)
        
        # Also update merged_map with gap-fetched data for skipped detection
        if gap_poll_ids:
            poll_ids.extend(gap_poll_ids)
            poll_ids.sort()
            total_polls = len(poll_ids)
            await safe_edit(pt, f"📋 Found {len(gap_poll_ids)} additional poll(s) in gaps.\n🔑 Total: {total_polls} polls to process...")
            await asyncio.sleep(1)
    
    if not poll_ids:
        await safe_edit(pt, "❌ No quiz/poll messages found in any fetch map for this channel.")
        return
    
    # ── STEP 5: Fetch each poll message and extract answer ──
    await safe_edit(pt, f"🔑 Processing {total_polls} poll(s)...")
    
    answer_entries = []
    question_num = 0
    resolved = await resolve_chat(uc, channel_id)
    
    # Calculate overall range for header
    overall_start = map_ranges[0][0]
    overall_end = map_ranges[-1][1]
    
    for poll_mid in poll_ids:
        try:
            msg = await uc.get_messages(resolved, poll_mid)
            if not msg or not msg.poll:
                continue
            
            poll = msg.poll
            question_num += 1
            
            # Build option list with letter prefixes
            options_text = []
            for idx, opt in enumerate(poll.options):
                letter = chr(65 + idx) if idx < 26 else str(idx + 1)
                options_text.append(f"  {letter}. {opt.text}")
            
            # Get correct answer
            correct_id = poll.correct_option_id
            
            # If quiz but correct_option_id is hidden, try to reveal it
            from pyrogram.enums import PollType
            is_quiz = correct_id is not None or getattr(poll, 'type', None) == PollType.QUIZ
            
            if is_quiz and correct_id is None:
                correct_id = await _get_correct_option(channel_id, poll_mid, poll, user_client=uc)
            
            if correct_id is not None and 0 <= correct_id < len(poll.options):
                correct_letter = chr(65 + correct_id) if correct_id < 26 else str(correct_id + 1)
                correct_text = poll.options[correct_id].text
                answer_line = f"✅ Answer: {correct_letter}. {correct_text}"
            elif is_quiz:
                answer_line = "⚠️ Correct answer could not be revealed"
            else:
                answer_line = "📊 Regular poll (no correct answer)"
            
            # Find which map this question belongs to for the label
            map_label = ""
            for i, (s, e) in enumerate(map_ranges):
                if s <= poll_mid <= e:
                    map_label = f" [Map {i+1}]"
                    break
                elif gaps:
                    for gs, ge in gaps:
                        if gs <= poll_mid <= ge:
                            map_label = " [Gap - auto-fetched]"
                            break
            
            entry = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Q{question_num}. {poll.question}{map_label}\n\n"
                + "\n".join(options_text) + "\n\n"
                + answer_line
            )
            answer_entries.append(entry)
            
            # Progress update every 5 questions
            if question_num % 5 == 0:
                try:
                    await safe_edit(pt, f"🔑 Processing... Q{question_num}/{total_polls}")
                except Exception:
                    pass
            
            # Small delay to avoid rate limits
            await asyncio.sleep(1.5)
            
        except FloodWait as e:
            wait_time = e.value if hasattr(e, 'value') else 30
            print(f"[ANSWERKEY] FloodWait {wait_time}s — skipping poll Q{question_num}")
            question_num += 1
            answer_entries.append(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\nQ{question_num}. [FloodWait — skipped]")
            continue
        except Exception as e:
            print(f"[ANSWERKEY] Error fetching poll {poll_mid}: {e}")
            question_num += 1
            answer_entries.append(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\nQ{question_num}. [Error fetching message {poll_mid}]")
    
    if not answer_entries:
        await safe_edit(pt, "❌ Could not extract any answers.")
        return
    
    # ── STEP 6: Build .txt file ──
    # Map summary for header
    map_summary_lines = []
    for i, (s, e) in enumerate(map_ranges):
        map_summary_lines.append(f"  Map {i+1}: Messages {s} - {e}")
    for i, (s, e) in enumerate(gaps):
        map_summary_lines.append(f"  Gap {i+1}: Messages {s} - {e} (auto-scanned)")
    
    header = (
        f"ANSWER KEY\n"
        f"Channel: {channel_id}\n"
        f"Overall Range: Message {overall_start} - {overall_end}\n"
        f"Total Questions: {len(answer_entries)}\n"
        f"Generated: {datetime.now().strftime('%d-%b-%Y %H:%M')}\n"
        f"\nData Sources:\n"
        + "\n".join(map_summary_lines) + "\n"
        + f"\n{'━' * 40}\n\n"
    )
    
    # Quick reference: just the answers
    quick_ref_lines = ["QUICK REFERENCE — Answers Only\n"]
    qnum = 0
    for entry in answer_entries:
        qnum += 1
        # Extract just the answer line
        for line in entry.split("\n"):
            if line.startswith("✅ Answer:"):
                quick_ref_lines.append(f"Q{qnum}: {line.replace('✅ Answer: ', '')}")
                break
            elif line.startswith("📊 Regular poll"):
                quick_ref_lines.append(f"Q{qnum}: Regular poll — no correct answer")
                break
            elif line.startswith("⚠️"):
                quick_ref_lines.append(f"Q{qnum}: Could not reveal answer")
                break
    
    # ── SKIPPED/UNFETCHED MESSAGES section ──
    skipped_lines = []
    # Find all message IDs in the overall range that are NOT in merged_map and NOT in gap_fetched_ids
    all_fetched = set(merged_map.keys())  # string keys
    all_fetched.update(str(mid) for mid in gap_fetched_ids)  # add gap-fetched IDs
    
    unfetched_in_range = []
    for mid in range(overall_start, overall_end + 1):
        if str(mid) not in all_fetched:
            unfetched_in_range.append(mid)
    
    if unfetched_in_range:
        skipped_lines.append(f"\n{'━' * 40}\nSKIPPED/UNFETCHED MESSAGES\n{'━' * 40}\n")
        skipped_lines.append(f"The following {len(unfetched_in_range)} message(s) were NOT fetched")
        skipped_lines.append(f"and may contain quiz/poll questions not included in this answer key.\n")
        skipped_lines.append(f"Use /fetch to scan these messages, then run /answerkey again.\n")
        
        # Group consecutive IDs into ranges for readability
        if unfetched_in_range:
            range_groups = []
            range_start = unfetched_in_range[0]
            range_end = unfetched_in_range[0]
            
            for mid in unfetched_in_range[1:]:
                if mid == range_end + 1:
                    range_end = mid
                else:
                    if range_start == range_end:
                        range_groups.append(f"  Message {range_start}")
                    else:
                        range_groups.append(f"  Messages {range_start} - {range_end}")
                    range_start = mid
                    range_end = mid
            
            # Don't forget the last group
            if range_start == range_end:
                range_groups.append(f"  Message {range_start}")
            else:
                range_groups.append(f"  Messages {range_start} - {range_end}")
            
            skipped_lines.extend(range_groups)
            
            # Also show which gap IDs failed to fetch
            if gap_failed_ids:
                gap_failed_ids.sort()
                skipped_lines.append(f"\n  ⚠️ Gap scan failures ({len(gap_failed_ids)} messages):")
                # Group failed IDs too
                fail_start = gap_failed_ids[0]
                fail_end = gap_failed_ids[0]
                for mid in gap_failed_ids[1:]:
                    if mid == fail_end + 1:
                        fail_end = mid
                    else:
                        if fail_start == fail_end:
                            skipped_lines.append(f"    Message {fail_start}")
                        else:
                            skipped_lines.append(f"    Messages {fail_start} - {fail_end}")
                        fail_start = mid
                        fail_end = mid
                if fail_start == fail_end:
                    skipped_lines.append(f"    Message {fail_start}")
                else:
                    skipped_lines.append(f"    Messages {fail_start} - {fail_end}")
    
    skipped_section = "\n".join(skipped_lines) if skipped_lines else ""
    
    file_content = (
        header 
        + "\n\n".join(answer_entries) 
        + f"\n\n{'━' * 40}\n\n" 
        + "\n".join(quick_ref_lines) 
        + f"\n\n{'━' * 40}"
        + skipped_section
        + f"\n\n{'━' * 40}\n"
    )
    
    # Save to file
    safe_name = re.sub(r'[^\w\-.]', '_', name)
    filename = f"{safe_name}.txt"
    filepath = f"/home/z/my-project/download/{filename}"
    os.makedirs("/home/z/my-project/download", exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(file_content)
    
    # Save to MongoDB for /viewanswerkey
    map_sources = [{"start": s, "end": e} for s, e in map_ranges]
    await save_answer_key(uid, name, channel_id, file_content, len(answer_entries), map_sources)
    
    unfetched_count = len(unfetched_in_range) if unfetched_in_range else 0
    caption = f"🔑 Answer Key: **{name}** — {len(answer_entries)} questions\nChannel: {channel_id}\nMaps used: {len(maps)} | Gaps auto-scanned: {len(gaps)}"
    if unfetched_count > 0:
        caption += f"\n⚠️ {unfetched_count} unfetched messages — see .txt for details"
    
    await safe_edit(pt,
        f"✅ Answer key **'{name}'** saved! {len(answer_entries)} questions processed."
        + (f" ({unfetched_count} messages skipped)" if unfetched_count else "")
        + "\n\n💡 Use /viewanswerkey to view it later\n💡 Use /clearanswerkey to delete it"
    )
    await m.reply_document(filepath, caption=caption)
    
    # Cleanup temp file
    try:
        os.remove(filepath)
    except Exception:
        pass


# ─── TEXT HANDLER FOR /FETCH, /CLEARFETCH, /ANSWERKEY CONVERSATIONS ──────────

@X.on_message(
    filters.text & filters.private & ~login_in_progress & ~filters.command([
        'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id',
        'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt',
        'decrypt', 'keys', 'setbot', 'rembot', 'auth', 'unauth', 'authusers',
        'logs', 'fetch', 'cancelfetch', 'fetchmaps', 'viewfetchmaps', 'clearfetch', 'answerkey', 'clearanswerkey', 'viewanswerkey',
        'settings', 'help', 'terms', 'plan', 'status', 'clearbatch', 'auto', 'autooff', 'cancelauto',
        'linkexplan', 'explans', 'transfer', 'rem', 'dl', 'adl'
    ])
)
async def fetch_text_handler(c, m):
    """Handle text input during /fetch, /clearfetch, /viewfetchmaps, /answerkey, /clearanswerkey, /viewanswerkey conversations.
    
    IMPORTANT: This runs BEFORE batch.py's text_handler because Python loads
    plugins alphabetically. 'fetch' < 'start' alphabetically but we need
    to handle fetch state here. We'll check states and only process
    if the user is in a conversation, otherwise fall through.
    """
    uid = m.from_user.id
    
    # ── HANDLE VIEWFETCHMAPS SELECTION ──
    if uid in VIEWFETCHMAPS_STATE:
        input_text = m.text.strip()
        print(f"[VIEWFETCHMAPS] uid={uid} replied with: '{input_text}'")
        maps = VIEWFETCHMAPS_STATE[uid].get('maps', [])
        del VIEWFETCHMAPS_STATE[uid]
        
        if input_text.lower() == 'cancel':
            await safe_reply(m, "❌ Cancelled.")
            return
        
        # Try matching by number
        fm = None
        try:
            idx = int(input_text) - 1
            if 0 <= idx < len(maps):
                fm = maps[idx]
        except ValueError:
            pass
        
        if not fm:
            await safe_reply(m, f"❌ Invalid number. Choose 1-{len(maps)}.")
            return
        
        try:
            # Fetch the full document (list_fetch_maps only returns metadata)
            ch = fm.get("channel_id", "Unknown")
            start = fm.get("start_msg_id", 0)
            end = fm.get("end_msg_id", 0)
            
            print(f"[VIEWFETCHMAPS] uid={uid} fetching full doc: channel={ch} range={start}-{end}")
            
            full_doc = await fetch_maps_collection.find_one({
                "user_id": uid,
                "channel_id": str(ch),
                "start_msg_id": start,
                "end_msg_id": end,
            })
            
            if not full_doc or not full_doc.get("msg_map"):
                await safe_reply(m, "❌ Fetch map content not found or empty.")
                return
            
            msg_map = full_doc["msg_map"]
            stats = full_doc.get("stats", {})
            created = full_doc.get("created_at", None)
            date_str = created.strftime("%d-%b-%Y %H:%M") if created else "Unknown"
            
            print(f"[VIEWFETCHMAPS] uid={uid} building TXT: {len(msg_map)} entries")
            
            # Build TXT content
            lines = []
            lines.append(f"Fetch Map — Channel: {ch}")
            lines.append(f"Range: {start} → {end}")
            lines.append(f"Created: {date_str}")
            lines.append(f"Total messages: {stats.get('total', len(msg_map))}")
            lines.append(f"Messages with media: {stats.get('with_media', '?')}")
            lines.append(f"Messages without media: {stats.get('without_media', '?')}")
            lines.append("")
            lines.append("=" * 70)
            lines.append(f"{'Msg ID':<12} {'Media':<8} {'Type':<16}")
            lines.append("=" * 70)
            
            # Sort by numeric message ID
            for msg_id in sorted(msg_map.keys(), key=lambda x: int(x)):
                info = msg_map[msg_id]
                has_media = "YES" if info.get("has_media") else "NO"
                media_type = info.get("media_type") or "—"
                lines.append(f"{msg_id:<12} {has_media:<8} {media_type:<16}")
            
            lines.append("=" * 70)
            lines.append(f"Total entries: {len(msg_map)}")
            
            content = "\n".join(lines)
            
            # Save to temp file and send
            safe_ch = re.sub(r'[^\w\-.]', '_', str(ch))
            filename = f"fetchmap_{safe_ch}_{start}_{end}.txt"
            filepath = f"/home/z/my-project/download/{filename}"
            os.makedirs("/home/z/my-project/download", exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"[VIEWFETCHMAPS] uid={uid} sending file: {filename} ({len(content)} bytes)")
            
            caption = (
                f"📋 Fetch Map: Channel `{ch}`\n"
                f"Range: {start}–{end} | {len(msg_map)} msgs\n"
                f"Created: {date_str}"
            )
            await m.reply_document(filepath, caption=caption)
            print(f"[VIEWFETCHMAPS] uid={uid} file sent successfully")
            
            # Cleanup temp file
            try:
                os.remove(filepath)
            except Exception:
                pass
        except Exception as e:
            import traceback
            print(f"[VIEWFETCHMAPS] ERROR in text handler: {e}")
            traceback.print_exc()
            try:
                await safe_reply(m, f"❌ Error generating fetch map file: {e}")
            except Exception:
                pass
        return
    
    # ── HANDLE CLEARFETCH SELECTION ──
    if uid in CLEARFETCH_STATE:
        input_text = m.text.strip().lower()
        maps = CLEARFETCH_STATE[uid].get('maps', [])
        del CLEARFETCH_STATE[uid]
        
        if input_text == 'cancel':
            await safe_reply(m, "❌ Cancelled. No fetch maps deleted.")
        elif input_text == 'all':
            deleted = await delete_fetch_map(uid)
            await safe_reply(m, f"✅ Deleted all {deleted} fetch map(s).")
        else:
            try:
                idx = int(input_text) - 1
                if 0 <= idx < len(maps):
                    fm = maps[idx]
                    ch = fm.get("channel_id")
                    start = fm.get("start_msg_id")
                    end = fm.get("end_msg_id")
                    deleted = await delete_fetch_map(uid, channel_id=ch)
                    await safe_reply(m, f"✅ Deleted fetch map for channel `{ch}` (range {start}-{end}).")
                else:
                    await safe_reply(m, f"❌ Invalid number. Choose 1-{len(maps)}.")
            except ValueError:
                await safe_reply(m, "❌ Invalid input. Send a number, 'all', or 'cancel'.")
        return
    
    # ── HANDLE CLEARANSWERKEY SELECTION ──
    if uid in CLEARANSWERKEY_STATE:
        input_text = m.text.strip().lower()
        keys = CLEARANSWERKEY_STATE[uid].get('keys', [])
        del CLEARANSWERKEY_STATE[uid]
        
        if input_text == 'cancel':
            await safe_reply(m, "❌ Cancelled. No answer keys deleted.")
        elif input_text == 'all':
            deleted = await delete_answer_key(uid)
            await safe_reply(m, f"✅ Deleted all {deleted} answer key(s).")
        else:
            try:
                idx = int(input_text) - 1
                if 0 <= idx < len(keys):
                    ak = keys[idx]
                    ak_name = ak.get("name")
                    deleted = await delete_answer_key(uid, name=ak_name)
                    await safe_reply(m, f"✅ Deleted answer key **'{ak_name}'**.")
                else:
                    await safe_reply(m, f"❌ Invalid number. Choose 1-{len(keys)}.")
            except ValueError:
                await safe_reply(m, "❌ Invalid input. Send a number, 'all', or 'cancel'.")
        return
    
    # ── HANDLE VIEWANSWERKEY SELECTION ──
    if uid in VIEWANSWERKEY_STATE:
        input_text = m.text.strip()
        keys = VIEWANSWERKEY_STATE[uid].get('keys', [])
        del VIEWANSWERKEY_STATE[uid]
        
        if input_text.lower() == 'cancel':
            await safe_reply(m, "❌ Cancelled.")
            return
        
        # Try matching by number first
        ak = None
        try:
            idx = int(input_text) - 1
            if 0 <= idx < len(keys):
                ak = keys[idx]
        except ValueError:
            # Try matching by exact name (case-insensitive)
            for k in keys:
                if k.get("name", "").lower() == input_text.lower():
                    ak = k
                    break
        
        if not ak:
            await safe_reply(m, "❌ Not found. Send a valid number or exact name.")
            return
        
        ak_name = ak.get("name", "answer_key")
        content = ak.get("content", "")
        total_q = ak.get("total_questions", 0)
        ch_id = ak.get("channel_id", "Unknown")
        created = ak.get("created_at", None)
        date_str = created.strftime("%d-%b-%Y %H:%M") if created else "Unknown"
        
        if not content:
            await safe_reply(m, "❌ Answer key content is empty or corrupted.")
            return
        
        # Save to temp file and send
        safe_name = re.sub(r'[^\w\-.]', '_', ak_name)
        filename = f"{safe_name}.txt"
        filepath = f"/home/z/my-project/download/{filename}"
        os.makedirs("/home/z/my-project/download", exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        caption = f"🔑 Answer Key: **{ak_name}**\n{total_q} questions | Channel: `{ch_id}`\nCreated: {date_str}"
        await m.reply_document(filepath, caption=caption)
        
        # Cleanup temp file
        try:
            os.remove(filepath)
        except Exception:
            pass
        return
    
    # ── HANDLE ANSWERKEY CONVERSATION ──
    if uid in ANSWERKEY_STATE:
        state = ANSWERKEY_STATE[uid]
        
        if state['step'] == 'channel':
            # User sent a channel link — extract channel_id
            L = m.text.strip()
            i, d, lt = E(L)
            if not i or not d:
                await safe_reply(m, 'Invalid link format. Send a valid Telegram message link.')
                del ANSWERKEY_STATE[uid]
                return
            
            # Move to name step
            ANSWERKEY_STATE[uid] = {'step': 'name', 'channel_id': i, 'link_type': lt}
            
            # Show existing answer key names so user doesn't duplicate
            existing_keys = await list_answer_keys(uid)
            existing_names = [ak.get("name", "") for ak in existing_keys]
            
            name_msg = "**📝 Step 2: Name your answer key**\n\nSend a unique name for this answer key."
            if existing_names:
                name_msg += "\n\n**Your existing keys:** " + ", ".join(f"`{n}`" for n in existing_names)
            name_msg += "\n\n⚠️ If the name already exists, it will be **overwritten**."
            
            await safe_reply(m,name_msg)
            return
        
        elif state['step'] == 'name':
            # User sent the name for the answer key
            name = m.text.strip()
            
            if not name or len(name) > 50:
                await safe_reply(m, "❌ Name must be 1-50 characters. Send a valid name.")
                return
            
            channel_id = state['channel_id']
            link_type = state['link_type']
            del ANSWERKEY_STATE[uid]
            
            # Run answer key generation with the custom name
            await generate_answer_key(c, m, uid, channel_id, link_type, name=name)
            return
    
    # ── HANDLE FETCH CONVERSATION ──
    if uid not in FETCH_STATE:
        raise ContinuePropagation  # Pass message to batch.py's text_handler
    
    state = FETCH_STATE[uid]
    
    if state['step'] == 'start':
        # User sent the start link
        L = m.text.strip()
        input_lower = L.lower()
        
        # Check if user typed "all" — auto-detect channel from existing maps
        if input_lower == 'all':
            existing_end, existing_count = await get_latest_fetch_map_end(uid, None)
            # Find any channel that has maps
            all_maps = await fetch_maps_collection.find({"user_id": uid}).to_list(length=None)
            if all_maps:
                # Find the channel with the latest end_msg_id
                best_channel = None
                best_end = 0
                for fm in all_maps:
                    fm_end = fm.get("end_msg_id", 0)
                    if fm_end > best_end:
                        best_end = fm_end
                        best_channel = fm.get("channel_id")
                
                if best_channel and best_end:
                    # Auto-resume from where we left off
                    FETCH_STATE[uid].update({'step': 'count', 'cid': best_channel, 'sid': best_end + 1, 'lt': 'private'})
                    await safe_reply(m,
                        f'✅ **Smart Merge Detected!**\n\n'
                        f'📋 Found existing map for channel `{best_channel}`\n'
                        f'📊 Last scanned up to message **{best_end}** ({existing_count} messages in map)\n\n'
                        f'Send **count** or **last link** to scan NEW messages only.\n'
                        f'Or type **all** to scan everything from msg {best_end + 1} to end.\n\n'
                        f'💡 Only new messages will be added — existing map is preserved!'
                    )
                    return
            await safe_reply(m, 'No existing fetch maps found. Send a start link from the channel first.')
            del FETCH_STATE[uid]
            return
        
        i, d, lt = E(L)
        if not i or not d:
            await safe_reply(m, 'Invalid link format. Send a valid Telegram message link.')
            del FETCH_STATE[uid]
            return  # Invalid link in fetch flow — don't propagate to batch
        
        # SMART MERGE: Check if a fetch map already exists for this channel
        existing_end, existing_count = await get_latest_fetch_map_end(uid, i)
        
        if existing_end and existing_end >= d:
            # Map already covers this start point — auto-adjust start to after the last scanned message
            new_start = existing_end + 1
            FETCH_STATE[uid].update({'step': 'count', 'cid': i, 'sid': new_start, 'original_sid': d, 'lt': lt})
            await safe_reply(m,
                f'✅ **Smart Merge Detected!**\n\n'
                f'📋 Found existing map for this channel\n'
                f'📊 Last scanned up to message **{existing_end}** ({existing_count} messages in map)\n\n'
                f'🔄 Start adjusted: msg {d} → msg **{new_start}** (only new messages)\n\n'
                f'**Choose one:**\n'
                f'1️⃣ **Number** — e.g. `500`\n'
                f'2️⃣ **Last link** — scan up to that link\n'
                f'3️⃣ **all** — scan all from msg {new_start} to end\n\n'
                f'💡 Existing map preserved + new messages merged!'
            )
        else:
            # No existing map or it doesn't cover this range — normal flow
            FETCH_STATE[uid].update({'step': 'count', 'cid': i, 'sid': d, 'original_sid': d, 'lt': lt})
            await safe_reply(m,
                f'✅ Start link received.\n\n'
                f'**Choose one:**\n'
                f'1️⃣ **Number** — e.g. `5000` (scan 5000 msgs)\n'
                f'2️⃣ **Last link** — scan from start to that link\n'
                f'3️⃣ **all** — scan ALL messages to the end\n\n'
                f'No limit — fetch as many as you need!'
            )
    
    elif state['step'] == 'count':
        # User sent count or last link
        i = state['cid']
        s = state['sid']
        lt = state['lt']
        max_limit = OWNER_FETCH_LIMIT if uid in OWNER_ID else DEFAULT_FETCH_LIMIT
        
        count = None
        input_text = m.text.strip().lower()
        
        # Check if user typed "all" — scan to the end of the channel
        if input_text == 'all':
            try:
                from plugins.batch import get_ubot, resolve_chat
                ubot_get = await get_ubot(uid)
                if not ubot_get:
                    ubot_get = X
                resolved_chat = await resolve_chat(ubot_get, i)
                async for last_msg in ubot_get.get_chat_history(resolved_chat, limit=1):
                    if last_msg and last_msg.id:
                        count = last_msg.id - s + 1
                        await safe_reply(m, f'📥 **ALL** messages selected: {count} messages (from msg {s} to {last_msg.id})')
                    break
                if not count:
                    await safe_reply(m, '❌ Could not determine the last message. Send a specific count or last link.')
                    return
            except Exception as e:
                print(f"[FETCH] Error getting channel last message for 'all': {e}")
                await safe_reply(m, '❌ Could not read channel info. Send a specific count or last link.')
                return
        
        # Check if user sent a number
        elif m.text.strip().isdigit():
            count = int(m.text.strip())
        else:
            end_i, end_d, end_lt = E(m.text.strip())
            if end_i and end_d:
                if str(end_i) != str(i):
                    await safe_reply(m, 'The last link must be from the same channel. Try again.')
                    return
                if end_d < s:
                    original_sid = state.get('original_sid', s)
                    if end_d >= original_sid and s > original_sid:
                        # Smart Merge adjusted start is past the user's end link
                        await safe_reply(m,
                            f'⚠️ **Already scanned past that point!**\n\n'
                            f'Your link is message **{end_d}**, but your existing map\n'
                            f'already covers up to message **{s - 1}** (Smart Merge).\n\n'
                            f'**Choose one:**\n'
                            f'1️⃣ **Number** — e.g. `500` (scan 500 msgs from msg {s})\n'
                            f'2️⃣ **Last link** — send a link AFTER message {s - 1}\n'
                            f'3️⃣ **all** — scan all remaining messages'
                        )
                    else:
                        await safe_reply(m,
                            f'⚠️ End message ID ({end_d}) must be greater than start ({s}).\n\n'
                            f'Your start link was message **{original_sid}**'
                            f'{f" (adjusted to {s} by Smart Merge)" if s != original_sid else ""}.\n\n'
                            f'Send a last link AFTER message {s}, or a number, or **all**.'
                        )
                    return
                count = end_d - s + 1
                await safe_reply(m, f'Calculated {count} messages from start to end link.')
            else:
                await safe_reply(m, 'Please choose one:\n1️⃣ **Number** — e.g. `5000`\n2️⃣ **Last link** — send the end link\n3️⃣ **all** — scan all messages')
                return
        
        # Clean up state — start the actual scanning
        del FETCH_STATE[uid]
        
        # Run the scan — new messages will be MERGED with existing map automatically
        await run_fetch_scan(c, m, uid, i, s, count, lt, max_limit)


async def run_fetch_scan(c, m, uid, channel_id, start_msg_id, count, link_type, max_limit):
    """Scan messages and build the lightweight fetch map.
    
    This reads messages one-by-one (or in small chunks) and extracts
    only metadata — NOT the full Message object.
    """
    log_ram("fetch_scan_start", extra_info={"uid": uid, "count": count})
    
    pt = await safe_reply(m, f'🔄 Pre-scanning {count} messages...\nStarting scan...')
    
    # Get the right client for fetching
    from plugins.batch import get_ubot, get_uclient, get_Y, emp, resolve_chat
    
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
        await safe_edit(pt, 'Cannot access channel — no user client available. Use /login first.')
        return
    
    # Build the lightweight map
    msg_map = {}  # str(msg_id) -> {"has_media": bool, "media_type": str, "reply_to": int|None, "is_pinned": bool}
    dep_batch = []  # Method 1: dependency index batch for bulk_write
    DEP_BATCH_SIZE = 200  # Flush dependencies every 200 messages
    unfetched_ids = []  # Track message IDs that could not be fetched
    stats = {
        "total": 0,
        "video": 0, "photo": 0, "audio": 0, "document": 0,
        "text": 0, "sticker": 0, "poll": 0, "other": 0,
        "has_reply": 0, "errors": 0,
        "dependencies": 0  # Method 1: count of poll→question dependencies recorded
    }
    
    scan_start_time = time.time()
    last_edit_time = 0  # Track last progress edit time to avoid MessageNotModified spam
    last_edit_text = ""  # Track last sent text to skip redundant edits
    chunks_done = 0
    total_chunks = (count + 99) // 100  # Total number of 100-msg chunks
    
    # Scan in chunks of 100 (Telegram API limit)
    start_id = int(start_msg_id)
    message_ids = list(range(start_id, start_id + count))
    
    for chunk_start in range(0, len(message_ids), 100):
        chunk_ids = message_ids[chunk_start:chunk_start + 100]
        chunks_done += 1
        
        try:
            # Try chunk fetch
            if link_type == 'private':
                chat_id_int = int(channel_id) if isinstance(channel_id, str) and channel_id.lstrip('-').isdigit() else channel_id
                try:
                    await uc.resolve_peer(chat_id_int)
                    messages = await uc.get_messages(chat_id_int, chunk_ids)
                except Exception:
                    messages = None
            else:
                resolved = await resolve_chat(ubot, channel_id)
                try:
                    messages = await ubot.get_messages(resolved, chunk_ids)
                except Exception:
                    if uc:
                        resolved_u = await resolve_chat(uc, channel_id)
                        messages = await uc.get_messages(resolved_u, chunk_ids)
                    else:
                        messages = None
            
            if messages and not isinstance(messages, list):
                messages = [messages]
            
            if messages:
                fetched_ids_in_chunk = set()
                for msg in messages:
                    if not msg or getattr(msg, 'empty', False):
                        stats["errors"] += 1
                        continue
                    fetched_ids_in_chunk.add(msg.id)
                    
                    mid = msg.id
                    has_media = bool(msg.media)
                    media_type = None
                    reply_to = None
                    
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
                    elif msg.text:
                        media_type = None
                    
                    # Extract reply_to for preserving reply chain in forwarded messages
                    # Robust: check multiple attribute locations because Pyrofork
                    # sometimes stores reply info in reply_to.message_id or
                    # reply_to.reply_to_msg_id instead of reply_to_message_id
                    raw_rtid = getattr(msg, 'reply_to_message_id', None)
                    if not raw_rtid:
                        reply_to_obj = getattr(msg, 'reply_to', None)
                        if reply_to_obj is not None:
                            raw_rtid = (getattr(reply_to_obj, 'reply_to_message_id', None)
                                        or getattr(reply_to_obj, 'message_id', None)
                                        or getattr(reply_to_obj, 'reply_to_msg_id', None))
                    reply_to = raw_rtid
                    
                    if reply_to:
                        stats["has_reply"] += 1
                    
                    # ─── METHOD 1: Dependency Index ─────────────────────
                    # If this is a poll with a reply_to (question image),
                    # record the dependency in MongoDB for instant Pass 1 lookup.
                    # Zero extra RAM — one tiny doc at a time, flushed in batches.
                    if media_type == "poll" and reply_to:
                        dep_batch.append({
                            "user_id": uid,
                            "channel_id": str(channel_id),
                            "question_src_id": reply_to,  # the image the poll needs
                            "poll_src_id": mid,            # the poll that needs it
                        })
                        stats["dependencies"] += 1
                        
                        # Flush dependencies in batches to avoid RAM buildup
                        if len(dep_batch) >= DEP_BATCH_SIZE:
                            try:
                                from pymongo import UpdateOne
                                bulk_ops = []
                                for dep in dep_batch:
                                    bulk_ops.append(UpdateOne(
                                        {"user_id": dep["user_id"], "channel_id": dep["channel_id"], "question_src_id": dep["question_src_id"]},
                                        {"$set": dep},
                                        upsert=True
                                    ))
                                await dependencies_collection.bulk_write(bulk_ops)
                                print(f"[FETCH-DEP] Flushed {len(dep_batch)} dependencies to MongoDB")
                            except Exception as dep_e:
                                print(f"[FETCH-DEP] Failed to flush dependencies: {dep_e}")
                            dep_batch = []
                    # ─── END METHOD 1 ────────────────────────────────────
                    
                    msg_map[str(mid)] = {
                        "has_media": has_media,
                        "media_type": media_type,
                        "reply_to": reply_to,
                        "is_pinned": False  # Will be updated after scan via proper API
                    }
                    
                    # Update stats
                    if media_type in stats:
                        stats[media_type] += 1
                    elif has_media:
                        stats["other"] += 1
                    else:
                        stats["text"] += 1
                    stats["total"] += 1
                
                # Track unfetched IDs in this chunk (requested but not returned or empty)
                for cid in chunk_ids:
                    if cid not in fetched_ids_in_chunk:
                        unfetched_ids.append(cid)
            else:
                # Entire chunk failed
                unfetched_ids.extend(chunk_ids)
                stats["errors"] += len(chunk_ids)
        
        except FloodWait as e:
            wait_time = e.value if hasattr(e, 'value') else 30
            print(f"[FETCH] FloodWait: {wait_time}s — skipping chunk ({chunk_ids[0]}-{chunk_ids[-1]})")
            unfetched_ids.extend(chunk_ids)
            stats["errors"] += len(chunk_ids)
            continue
        
        except Exception as e:
            print(f"[FETCH] Chunk error ({chunk_ids[0]}-{chunk_ids[-1]}): {e} — retrying after 5s...")
            await asyncio.sleep(5)
            # Retry once
            try:
                if link_type == 'private':
                    chat_id_int = int(channel_id) if isinstance(channel_id, str) and channel_id.lstrip('-').isdigit() else channel_id
                    try:
                        await uc.resolve_peer(chat_id_int)
                        messages = await uc.get_messages(chat_id_int, chunk_ids)
                    except Exception:
                        messages = None
                else:
                    resolved = await resolve_chat(ubot, channel_id)
                    messages = await ubot.get_messages(resolved, chunk_ids)
                
                if messages:
                    fetched_ids_in_chunk = set()
                    if not isinstance(messages, list):
                        messages = [messages]
                    for msg in messages:
                        if msg and not getattr(msg, 'empty', False):
                            mid = msg.id
                            fetched_ids_in_chunk.add(mid)
                            has_media = bool(getattr(msg, 'media', None))
                            media_type = None
                            if hasattr(msg, 'video') and msg.video: media_type = "video"
                            elif hasattr(msg, 'photo') and msg.photo: media_type = "photo"
                            elif hasattr(msg, 'document') and msg.document: media_type = "document"
                            elif hasattr(msg, 'poll') and msg.poll: media_type = "poll"
                            elif hasattr(msg, 'animation') and msg.animation: media_type = "animation"
                            elif hasattr(msg, 'voice') and msg.voice: media_type = "voice"
                            elif hasattr(msg, 'audio') and msg.audio: media_type = "audio"
                            elif hasattr(msg, 'sticker') and msg.sticker: media_type = "sticker"
                            reply_to = None
                            _raw_rtid = getattr(msg, 'reply_to_message_id', None)
                            if not _raw_rtid:
                                _rto = getattr(msg, 'reply_to', None)
                                if _rto is not None:
                                    _raw_rtid = (getattr(_rto, 'reply_to_message_id', None)
                                                 or getattr(_rto, 'message_id', None)
                                                 or getattr(_rto, 'reply_to_msg_id', None))
                            if _raw_rtid:
                                reply_to = str(_raw_rtid)
                            msg_map[str(mid)] = {
                                "has_media": has_media,
                                "media_type": media_type,
                                "reply_to": reply_to,
                                "is_pinned": False  # Will be updated after scan via proper API
                            }
                            if media_type in stats: stats[media_type] += 1
                            elif has_media: stats["other"] += 1
                            else: stats["text"] += 1
                            stats["total"] += 1
                    for cid in chunk_ids:
                        if cid not in fetched_ids_in_chunk:
                            unfetched_ids.append(cid)
                else:
                    unfetched_ids.extend(chunk_ids)
                    stats["errors"] += len(chunk_ids)
            except Exception as retry_e:
                print(f"[FETCH] Chunk retry also failed ({chunk_ids[0]}-{chunk_ids[-1]}): {retry_e}")
                unfetched_ids.extend(chunk_ids)
                stats["errors"] += len(chunk_ids)
        
        # ─── REAL-TIME PROGRESS UPDATE ────────────────────────────────
        # Update progress after every chunk (every ~100 msgs).
        # Throttle edits to max once per 3 seconds to avoid MessageNotModified spam.
        now = time.time()
        is_last_chunk = chunks_done >= total_chunks
        
        if is_last_chunk or (now - last_edit_time >= 3):
            elapsed = now - scan_start_time
            found = stats["total"]
            pct = min(found * 100 // count, 100) if count > 0 else 100
            rate = found / elapsed if elapsed > 0 else 0
            remaining = (count - found) / rate if rate > 0 else 0
            
            # Visual progress bar
            filled = pct // 10
            bar = '🟢' * filled + '⚪' * (10 - filled)
            
            # Format ETA nicely
            if remaining > 60:
                eta_str = f'{int(remaining // 60)}m {int(remaining % 60)}s'
            else:
                eta_str = f'{int(remaining)}s'
            
            progress_text = (
                f'🔄 **Pre-scanning messages**\n\n'
                f'{bar}  **{pct}%**\n\n'
                f'📊 Scanned: **{found}** / {count}\n'
                f'❌ Errors: {stats["errors"]}\n'
                f'⚡ Rate: **{rate:.0f} msgs/s**\n'
                f'⏳ ETA: **{eta_str}**\n'
                f'⏱️ Elapsed: {elapsed:.0f}s'
            )
            
            # Only edit if text actually changed (avoids MessageNotModified)
            if progress_text != last_edit_text:
                try:
                    await safe_edit(pt,progress_text)
                    last_edit_text = progress_text
                    last_edit_time = now
                except Exception:
                    pass
        
        # Small delay between chunks to avoid FloodWait
        await asyncio.sleep(0.5)
    
    scan_time = time.time() - scan_start_time
    
    # ─── METHOD 1: Flush remaining dependencies ───────────────────────
    if dep_batch:
        try:
            from pymongo import UpdateOne
            bulk_ops = []
            for dep in dep_batch:
                bulk_ops.append(UpdateOne(
                    {"user_id": dep["user_id"], "channel_id": dep["channel_id"], "question_src_id": dep["question_src_id"]},
                    {"$set": dep},
                    upsert=True
                ))
            await dependencies_collection.bulk_write(bulk_ops)
            print(f"[FETCH-DEP] Final flush: {len(dep_batch)} dependencies to MongoDB (total: {stats['dependencies']})")
        except Exception as dep_e:
            print(f"[FETCH-DEP] Final flush failed: {dep_e}")
        dep_batch = []
    # ─── END METHOD 1: Final flush ────────────────────────────────────
    
    # ─── DETECT PINNED MESSAGES ──────────────────────────────────────
    # Use Telegram's official API to get ALL pinned messages.
    # Cost: 1-2 API calls total — regardless of how many pins exist.
    # This replaces the broken service message scan that didn't work
    # in channels (service messages aren't returned by get_messages).
    pinned_count = 0
    try:
        if uc:
            pinned_ids = await fetch_all_pinned_ids(uc, channel_id)
            for pid in pinned_ids:
                pid_str = str(pid)
                if pid_str in msg_map:
                    msg_map[pid_str]["is_pinned"] = True
                    pinned_count += 1
                    print(f"[FETCH-PIN] ✅ Marked msg {pid} as pinned (from Telegram API)")
                else:
                    print(f"[FETCH-PIN] Pinned msg {pid} not in scan range — skipping")
        else:
            print(f"[FETCH-PIN] No user client available — skipping pin detection")
    except Exception as e:
        print(f"[FETCH-PIN] Could not check pinned messages via API: {e}")
    
    stats["pinned"] = pinned_count
    print(f"[FETCH-PIN] Total pinned messages detected: {pinned_count}")
    
    # Save to MongoDB in chunks of CHUNK_SIZE
    end_msg_id = start_id + count - 1
    
    # Split map into chunks if needed
    map_keys = sorted(msg_map.keys(), key=lambda x: int(x))
    
    for chunk_i in range(0, len(map_keys), CHUNK_SIZE):
        chunk_keys = map_keys[chunk_i:chunk_i + CHUNK_SIZE]
        chunk_map = {k: msg_map[k] for k in chunk_keys}
        
        chunk_start_id = int(chunk_keys[0])
        chunk_end_id = int(chunk_keys[-1])
        
        await save_fetch_map(uid, channel_id, link_type, chunk_start_id, chunk_end_id, chunk_map, stats)
    
    # Final summary
    log_ram("fetch_scan_end", extra_info={"uid": uid, "count": count, "map_size": f"{len(msg_map)} entries", "unfetched": len(unfetched_ids)})
    
    # Build final scan rate string safely
    if scan_time > 0 and stats["total"] > 0:
        rate_str = f'{stats["total"]/scan_time:.0f} msgs/s'
    else:
        rate_str = 'N/A'
    
    summary = (
        f'✅ **Scan Complete!**\n\n'
        f'📊 **Results:**\n'
        f'━━━━━━━━━━━━━━━\n'
        f'Total scanned: {stats["total"]}\n'
        f'Errors/skipped: {stats["errors"]}\n'
        f'Time: {scan_time:.1f}s ({rate_str})\n\n'
        f'📹 Videos: {stats["video"]} | 📷 Photos: {stats["photo"]}\n'
        f'📄 Documents: {stats["document"]} | 🎵 Audio: {stats["audio"]}\n'
        f'💬 Text: {stats["text"]} | 📊 Polls: {stats["poll"]}\n'
        f'🎨 Stickers: {stats["sticker"]} | 🔗 With replies: {stats["has_reply"]}\n'
        f'📌 Pinned: {stats.get("pinned", 0)} | 🔗 Dependencies: {stats.get("dependencies", 0)}\n'
    )
    
    summary += f'\n💾 Map saved to MongoDB ({len(msg_map)} entries)\n'
    
    # Register this channel for explanation monitoring (real-time listener)
    try:
        from plugins.explanation_listener import add_monitored_channel
        await add_monitored_channel(channel_id, uid, client=uc or ubot)
    except Exception as e:
        print(f"[EXPLANATION] Could not register channel for monitoring after /fetch: {e}")
    
    if unfetched_ids:
        summary += f'\n⚠️ Unfetched: {len(unfetched_ids)} — see attached file for details.'
    
    summary += f'\nUse /batch to process these messages (streaming mode).'
    
    await safe_edit(pt,summary)
    
    # Send unfetched messages as TXT file
    if unfetched_ids:
        try:
            unfetched_file_path = f"unfetched_{uid}_{int(time.time())}.txt"
            with open(unfetched_file_path, 'w', encoding='utf-8') as f:
                f.write(f"Unfetched Messages Report\n")
                f.write(f"========================\n")
                f.write(f"Channel: {channel_id}\n")
                f.write(f"Range requested: {start_id} to {start_id + count - 1} ({count} messages)\n")
                f.write(f"Successfully fetched: {stats['total']}\n")
                f.write(f"Unfetched: {len(unfetched_ids)}\n\n")
                f.write(f"Unfetched Links:\n")
                f.write(f"----------------\n")
                for idx, mid in enumerate(sorted(unfetched_ids), 1):
                    if link_type == 'private':
                        channel_id_clean = str(channel_id).replace('-100', '')
                        link = f"https://t.me/c/{channel_id_clean}/{mid}"
                    else:
                        link = f"https://t.me/{channel_id}/{mid}"
                    f.write(f"{idx}. {link}\n")
            await m.reply_document(unfetched_file_path, caption=f'⚠️ Unfetched messages ({len(unfetched_ids)})')
            os.remove(unfetched_file_path)
        except Exception as e:
            print(f"[FETCH] Failed to send unfetched file: {e}")
