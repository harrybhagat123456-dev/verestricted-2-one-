# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

# ╔══════════════════════════════════════════════════════════════════╗
# ║  CHANNEL CLONE PLUGIN — Forum-aware structure cloning           ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║                                                                  ║
# ║  Clones a source Telegram channel's STRUCTURE to a destination:  ║
# ║  - If source has forums/topics → creates matching topics in dest ║
# ║  - If source is a regular channel → flat copy (same as /batch)   ║
# ║  - Messages are placed in the correct topic based on source      ║
# ║  - General (no-topic) messages go to General in dest             ║
# ║                                                                  ║
# ║  TRIGGERED FROM: /batch → "Clone Channel Structure" option      ║
# ║                or /clone command directly                        ║
# ║                                                                  ║
# ║  FLOW:                                                           ║
# ║  1. Analyze source channel (is it a forum? get topic list)      ║
# ║  2. If destination is NOT a forum → ask user to make it one     ║
# ║  3. Create matching topics in destination                        ║
# ║  4. Stream messages from source, routing each to correct topic  ║
# ║                                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

import asyncio
import time
import re
from datetime import datetime
from typing import Dict, Optional, Tuple, List, Any
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup
)
from pyrogram.errors import (
    FloodWait, ChannelPrivate, ChatIdInvalid,
    PeerIdInvalid, UserNotParticipant, BadRequest
)
from pyrogram.raw.functions.channels import ToggleForum
from pyrogram.raw.types import InputChannel
from motor.motor_asyncio import AsyncIOMotorClient

from shared_client import app as X
from config import OWNER_ID, MONGO_DB as _CLONE_MONGO_URI, DB_NAME as _CLONE_DB_NAME
from utils.func import E, get_user_data_key, is_auth_user, is_premium_user


# ═══════════════════════════════════════════════════════════════
# CLONE STATE — per-user conversation state
# Key: uid (int) -> dict with step, source info, etc.
# ═══════════════════════════════════════════════════════════════
CLONE_STATE: Dict[int, dict] = {}

# ═══════════════════════════════════════════════════════════════
# CLONE JOBS — MongoDB persistence for resumable clones
# ═══════════════════════════════════════════════════════════════
_clone_mongo = AsyncIOMotorClient(_CLONE_MONGO_URI)
_clone_db = _clone_mongo[_CLONE_DB_NAME]
clone_jobs_collection = _clone_db["clone_jobs"]


async def _save_clone_job(uid: int, source_chat_id, source_link_type: str,
                          start_msg_id: int, message_count: int,
                          dest_chat_id: int, user_chat_id: int,
                          status: str = "running",
                          last_processed_msg_id: int = 0,
                          processed_count: int = 0,
                          success_count: int = 0):
    """Persist clone job state to MongoDB."""
    await clone_jobs_collection.update_one(
        {"uid": uid},
        {"$set": {
            "uid": uid,
            "source_chat_id": str(source_chat_id),
            "source_link_type": source_link_type,
            "start_msg_id": start_msg_id,
            "message_count": message_count,
            "dest_chat_id": dest_chat_id,
            "user_chat_id": user_chat_id,
            "status": status,
            "last_processed_msg_id": last_processed_msg_id,
            "processed_count": processed_count,
            "success_count": success_count,
            "updated_at": datetime.now(),
        }},
        upsert=True,
    )


async def _load_clone_job(uid: int) -> Optional[dict]:
    """Load the most recent clone job for a user."""
    return await clone_jobs_collection.find_one({"uid": uid})


async def _delete_clone_job(uid: int):
    """Remove clone job after successful completion."""
    await clone_jobs_collection.delete_many({"uid": uid})


async def startup_clone_resume_check():
    """Called at bot startup: notify users with interrupted clone jobs.

    Marks stale 'running' jobs as 'interrupted' and sends a DM so users
    know they can type /resumeclone to pick up where they left off.
    Resume safety: the upload_maps collection already tracks which messages
    were successfully sent, so re-running clone automatically skips them.
    """
    try:
        stale_cutoff = datetime.now().timestamp() - 120  # 2 min stale
        cursor = clone_jobs_collection.find({"status": "running"})
        async for job in cursor:
            updated = job.get("updated_at")
            if updated and updated.timestamp() > stale_cutoff:
                continue  # Still fresh — might be a running job on another process
            uid = job.get("uid")
            user_chat_id = job.get("user_chat_id", uid)
            source = job.get("source_chat_id", "?")
            done = job.get("processed_count", 0)
            total = job.get("message_count", 0)
            last_id = job.get("last_processed_msg_id", 0)
            # Mark as interrupted
            await clone_jobs_collection.update_one(
                {"uid": uid},
                {"$set": {"status": "interrupted", "updated_at": datetime.now()}},
            )
            # Notify user
            try:
                await X.send_message(
                    user_chat_id,
                    f"⚠️ **Clone Interrupted**\n\n"
                    f"Your channel clone was interrupted (bot restarted).\n\n"
                    f"**Source:** `{source}`\n"
                    f"**Progress:** {done}/{total} messages (last msg ID: {last_id})\n\n"
                    f"Send /resumeclone to pick up where it left off.\n"
                    f"Already-sent messages will be skipped automatically.",
                )
            except Exception as notify_err:
                print(f"[CLONE-RESUME] Failed to notify uid={uid}: {notify_err}")
    except Exception as e:
        print(f"[CLONE-RESUME] startup_clone_resume_check error: {e}")

# Rate limiter (reuse pattern from batch.py)
API_CALL_INTERVAL = 3.5  # seconds between API calls


class _CloneRateLimiter:
    """Simple rate limiter for clone API calls."""
    def __init__(self):
        self._last_time: float = 0
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.time()
                elapsed = now - self._last_time
                if elapsed >= API_CALL_INTERVAL:
                    self._last_time = now
                    return
                wait = API_CALL_INTERVAL - elapsed
            await asyncio.sleep(wait)

    def clear(self):
        self._last_time = 0


_clone_rate_limiter = _CloneRateLimiter()


# ═══════════════════════════════════════════════════════════════
# HELPERS — import from batch.py dynamically to avoid circular
# ═══════════════════════════════════════════════════════════════

def _get_batch_helpers():
    """Lazily import helpers from batch.py to avoid circular imports."""
    import plugins.batch as batch_mod
    return {
        'safe_reply': batch_mod.safe_reply,
        'safe_edit': batch_mod.safe_edit,
        'get_ubot': batch_mod.get_ubot,
        'get_uclient': batch_mod.get_uclient,
        'get_Y': batch_mod.get_Y,
        'resolve_chat': batch_mod.resolve_chat,
        'get_msg': batch_mod.get_msg,
        'process_msg': batch_mod.process_msg,
        'is_user_active': batch_mod.is_user_active,
        'add_active_batch': batch_mod.add_active_batch,
        'remove_active_batch': batch_mod.remove_active_batch,
        'should_cancel': batch_mod.should_cancel,
        'clear_cancel_flag': batch_mod.clear_cancel_flag,
        'cancel_cmd_batch': batch_mod.request_batch_cancel,
        'batch_tasks': batch_mod.batch_tasks,
        'Z': batch_mod.Z,
        'BATCH_SEND_DELAY': batch_mod.BATCH_SEND_DELAY,
        'BATCH_COOLDOWN_EVERY_SHORT': batch_mod.BATCH_COOLDOWN_EVERY_SHORT,
        'BATCH_COOLDOWN_DURATION_SHORT': batch_mod.BATCH_COOLDOWN_DURATION_SHORT,
        'BATCH_COOLDOWN_EVERY_LONG': batch_mod.BATCH_COOLDOWN_EVERY_LONG,
        'BATCH_COOLDOWN_DURATION_LONG': batch_mod.BATCH_COOLDOWN_DURATION_LONG,
        'batch_heartbeat': batch_mod.batch_heartbeat,
        'log_ram': batch_mod.log_ram,
        'load_upload_map': batch_mod.load_upload_map,
        'save_upload_map': batch_mod.save_upload_map,
        'get_upload_map_resume_info': batch_mod.get_upload_map_resume_info,
        'prog': batch_mod.prog,
        'upd_dlg': batch_mod.upd_dlg,
        'get_user_data': batch_mod.get_user_data_key,
        'rename_file': batch_mod.rename_file,
        'screenshot': batch_mod.screenshot,
        'thumbnail': batch_mod.thumbnail,
        'get_video_metadata': batch_mod.get_video_metadata,
        'E': batch_mod.E,
    }


# ═══════════════════════════════════════════════════════════════
# SOURCE CHANNEL ANALYSIS
# ═══════════════════════════════════════════════════════════════

async def _is_chat_forum(client: Client, chat_id, user_client: Client = None) -> bool:
    """Robustly detect whether a chat is a forum.

    pyrofork 2.3.x's `Chat` object does NOT expose `is_forum` as a public
    attribute, so `getattr(chat, 'is_forum', False)` always returns False.
    This helper uses three layered detection methods:

    1. `getattr(chat, 'is_forum', False)` — works for pyrogram forks that DO expose it.
    2. Probe `get_forum_topics(chat_id)` — if any topic is returned, it's a forum.
       This requires the client to be a participant with read access.
    3. Raw MTProto inspection: fetch the underlying `Channel` object via
       `client.resolve_peer(chat_id)` and read `channel.forum`.

    Returns True as soon as any method confirms forum status; False if all fail.
    """
    # Method 1: attribute check
    try:
        chat = await client.get_chat(chat_id)
        if getattr(chat, 'is_forum', False):
            return True
    except Exception as e:
        print(f"[CLONE-ISFORUM] get_chat failed on {chat_id}: {e}")

    # Method 2: probe get_forum_topics
    probe_clients = [client] + ([user_client] if user_client else [])
    for pc in probe_clients:
        if not pc or not hasattr(pc, 'get_forum_topics'):
            continue
        try:
            async for _t in pc.get_forum_topics(chat_id):
                return True  # at least one topic → it's a forum
            # Empty iterator could mean "not a forum" OR "forum with no topics"
            # — fall through to Method 3 to be sure.
            break
        except Exception as e:
            err_str = str(e).lower()
            # Errors like "TOPIC_CLOSED", "CHAT_NOT_FORUM" → definitely not a forum
            if 'not_forum' in err_str or 'not a forum' in err_str or 'chat_not_forum' in err_str:
                return False
            # Other errors (permission, flood) → try next method
            print(f"[CLONE-ISFORUM] get_forum_topics probe failed: {e}")
            continue

    # Method 3: raw MTProto inspection
    for pc in probe_clients:
        if not pc:
            continue
        try:
            peer = await pc.resolve_peer(chat_id)
            # Channel objects have a `forum` boolean flag
            if hasattr(peer, 'forum') and peer.forum:
                return True
            # Some builds wrap it differently
            if hasattr(peer, 'channel') and hasattr(peer.channel, 'forum'):
                return bool(peer.channel.forum)
        except Exception as e:
            print(f"[CLONE-ISFORUM] resolve_peer inspection failed: {e}")
            continue

    return False


async def _convert_to_forum(client: Client, chat_id) -> bool:
    """Convert a supergroup/channel to a forum using raw ToggleForum API.

    The client must be an admin with appropriate rights (admin + can_change_info).
    Bots can do this if they have those rights; user clients can always do it
    if they are admin/creator.

    Returns True on success, False on failure.
    """
    try:
        peer = await client.resolve_peer(chat_id)
        # ToggleForum requires an InputChannel. resolve_peer usually returns
        # an InputPeerChannel for supergroups/channels — convert if needed.
        if not isinstance(peer, InputChannel):
            if hasattr(peer, 'channel_id') and hasattr(peer, 'access_hash'):
                peer = InputChannel(
                    channel_id=peer.channel_id,
                    access_hash=peer.access_hash,
                )
            else:
                print(f"[CLONE-CONVERT] Cannot build InputChannel from peer {type(peer).__name__}")
                return False

        await client.invoke(ToggleForum(
            channel=peer,
            enabled=True,
            tabs=False,
        ))
        print(f"[CLONE-CONVERT] Successfully converted chat {chat_id} to a forum")
        return True
    except Exception as e:
        err_str = str(e).lower()
        # If already a forum, treat as success
        if 'already' in err_str and 'forum' in err_str:
            print(f"[CLONE-CONVERT] Chat {chat_id} is already a forum")
            return True
        print(f"[CLONE-CONVERT] Failed to convert {chat_id} to forum: {e}")
        return False


async def analyze_source_channel(client: Client, chat_id, user_client: Client = None):
    """Analyze the source channel to determine its structure.

    Returns:
        dict: {
            'is_forum': bool,
            'chat_title': str,
            'chat_id': int,
            'topics': [
                {
                    'topic_id': int,       # source topic ID (thread_id)
                    'topic_name': str,     # topic title
                    'topic_icon_color': int,  # icon color code
                    'message_count': int,  # approximate count
                    'first_msg_id': int,   # first message ID in topic
                    'last_msg_id': int,    # last message ID in topic
                    'is_general': bool,    # True if this is the General topic
                },
                ...
            ],
            'general_topic_id': int or None,
        }
    """
    result = {
        'is_forum': False,
        'chat_title': '',
        'chat_id': None,
        'topics': [],
        'general_topic_id': None,
    }

    # Get chat info
    try:
        if isinstance(chat_id, str) and not chat_id.lstrip('-').isdigit():
            resolved = await client.get_chat(chat_id)
            chat_id = resolved.id
        elif isinstance(chat_id, str):
            chat_id = int(chat_id)

        chat = await client.get_chat(chat_id)
        result['chat_id'] = chat.id
        result['chat_title'] = chat.title or ''
        result['is_forum'] = getattr(chat, 'is_forum', False) or False

        print(f"[CLONE-ANALYZE] Channel: {chat.title} (id={chat.id}), is_forum={result['is_forum']}")

        # Fallback: if is_forum came back False, probe get_forum_topics — some
        # Pyrofork builds don't populate the attribute even for forum supergroups.
        if not result['is_forum']:
            for _probe_client in ([client] + ([user_client] if user_client else [])):
                if not hasattr(_probe_client, 'get_forum_topics'):
                    continue
                try:
                    async for _t in _probe_client.get_forum_topics(chat_id):
                        result['is_forum'] = True
                        print(f"[CLONE-ANALYZE] is_forum was False but topics exist — treating as forum")
                        break
                    if result['is_forum']:
                        break
                except Exception:
                    pass  # Not a forum or permission denied — keep is_forum=False

        # Final fallback: raw MTProto inspection of the Channel object.
        # The raw `Channel` has a `forum` boolean flag we can read directly.
        if not result['is_forum']:
            try:
                peer = await client.resolve_peer(chat_id)
                if hasattr(peer, 'forum') and peer.forum:
                    result['is_forum'] = True
                    print(f"[CLONE-ANALYZE] is_forum detected via raw MTProto (peer.forum=True)")
                elif user_client:
                    peer_u = await user_client.resolve_peer(chat_id)
                    if hasattr(peer_u, 'forum') and peer_u.forum:
                        result['is_forum'] = True
                        print(f"[CLONE-ANALYZE] is_forum detected via raw MTProto (user_client peer.forum=True)")
            except Exception as _raw_err:
                print(f"[CLONE-ANALYZE] Raw MTProto forum check failed: {_raw_err}")
    except Exception as e:
        print(f"[CLONE-ANALYZE] Error getting chat info: {e}")
        # Try with user client
        if user_client:
            try:
                if isinstance(chat_id, str):
                    chat_id = int(chat_id) if chat_id.lstrip('-').isdigit() else chat_id
                chat = await user_client.get_chat(chat_id)
                result['chat_id'] = chat.id
                result['chat_title'] = chat.title or ''
                result['is_forum'] = getattr(chat, 'is_forum', False)
                print(f"[CLONE-ANALYZE] (user_client) Channel: {chat.title}, is_forum={result['is_forum']}")
            except Exception as e2:
                print(f"[CLONE-ANALYZE] User client also failed: {e2}")
                raise
        else:
            raise

    # If it's a forum, fetch all topics
    if result['is_forum']:
        topics = await _fetch_forum_topics(client, chat_id, user_client)
        result['topics'] = topics
        # Find General topic
        for t in topics:
            if t['is_general']:
                result['general_topic_id'] = t['topic_id']
                break
        print(f"[CLONE-ANALYZE] Found {len(topics)} topics")
        for t in topics:
            print(f"  - Topic '{t['topic_name']}' (id={t['topic_id']}, "
                  f"msgs={t['message_count']}, general={t['is_general']})")

    return result


async def _fetch_forum_topics(client: Client, chat_id, user_client: Client = None) -> list:
    """Fetch all forum topics from a channel.

    Uses Pyrogram's get_forum_topics or iterates through history to discover topics.
    Telegram's API requires the bot to be an admin with manage_topics permission.
    """
    topics = []
    fetch_client = client

    try:
        # Valid Telegram topic icon colors — 0 is not accepted by the API.
        # When icon_color is 0 or missing we store None so create_forum_topic
        # omits the parameter and lets Telegram pick a colour automatically.
        _VALID_COLORS = {0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F}

        def _safe_color(raw):
            """Return the colour int if valid, else None."""
            c = raw if isinstance(raw, int) else 0
            return c if c in _VALID_COLORS else None

        # Method 1: Use get_forum_topics if available (Pyrofork)
        if hasattr(fetch_client, 'get_forum_topics'):
            try:
                async for topic in fetch_client.get_forum_topics(chat_id):
                    t_info = {
                        'topic_id': topic.id,  # This is the thread_id / topic_id
                        'topic_name': topic.title or 'Untitled',
                        'topic_icon_color': _safe_color(getattr(topic, 'icon_color', 0)),
                        'message_count': getattr(topic, 'total_message_count', 0) or 0,
                        'first_msg_id': getattr(topic, 'top_message_id', 0) or 0,
                        'last_msg_id': getattr(topic, 'top_message_id', 0) or 0,
                        'is_general': getattr(topic, 'is_general', False),
                    }
                    topics.append(t_info)
            except Exception as e:
                print(f"[CLONE-TOPICS] get_forum_topics failed: {e}")
                # Fall through to Method 2

        # Method 2: If Method 1 returned nothing or failed, try raw API
        if not topics and user_client:
            try:
                print("[CLONE-TOPICS] Trying user client to fetch topics...")
                fetch_client = user_client
                if hasattr(fetch_client, 'get_forum_topics'):
                    async for topic in fetch_client.get_forum_topics(chat_id):
                        t_info = {
                            'topic_id': topic.id,
                            'topic_name': topic.title or 'Untitled',
                            'topic_icon_color': _safe_color(getattr(topic, 'icon_color', 0)),
                            'message_count': getattr(topic, 'total_message_count', 0) or 0,
                            'first_msg_id': getattr(topic, 'top_message_id', 0) or 0,
                            'last_msg_id': getattr(topic, 'top_message_id', 0) or 0,
                            'is_general': getattr(topic, 'is_general', False),
                        }
                        topics.append(t_info)
            except Exception as e:
                print(f"[CLONE-TOPICS] User client get_forum_topics also failed: {e}")

        # Method 3: Scan message history to discover topic IDs
        # This works even without manage_topics permission
        if not topics:
            print("[CLONE-TOPICS] Falling back to history scan to discover topics...")
            topics = await _discover_topics_from_history(fetch_client, chat_id, user_client)

        # Deduplicate by topic_id
        seen = set()
        unique_topics = []
        for t in topics:
            if t['topic_id'] not in seen:
                seen.add(t['topic_id'])
                unique_topics.append(t)
        topics = unique_topics

    except Exception as e:
        print(f"[CLONE-TOPICS] Error fetching topics: {e}")

    return topics


async def _discover_topics_from_history(client: Client, chat_id, user_client: Client = None) -> list:
    """Discover forum topics by scanning recent message history.

    Each message in a forum has a reply_to_message_id that points to the topic's
    first message. We collect these to build the topic map.
    """
    topic_map = {}  # topic_id -> info dict
    messages_scanned = 0
    max_scan = 500  # Scan last 500 messages to find topics

    scan_client = client
    try:
        # Scan recent messages
        async for msg in scan_client.get_chat_history(chat_id, limit=max_scan):
            messages_scanned += 1
            # In forums, each message has reply_to_message_id pointing to the topic's root
            thread_id = None
            if hasattr(msg, 'reply_to') and msg.reply_to:
                thread_id = getattr(msg.reply_to, 'forum_topic_id', None)
                if thread_id is None:
                    thread_id = getattr(msg.reply_to, 'message_id', None)
            elif hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id:
                thread_id = msg.reply_to_message_id

            if thread_id and thread_id not in topic_map:
                topic_map[thread_id] = {
                    'topic_id': thread_id,
                    'topic_name': f'Topic {thread_id}',  # Will be renamed when we fetch the root message
                    'topic_icon_color': 0,
                    'message_count': 1,
                    'first_msg_id': thread_id,
                    'last_msg_id': msg.id,
                    'is_general': (thread_id == 1),  # General topic usually has ID 1
                }
            elif thread_id and thread_id in topic_map:
                topic_map[thread_id]['message_count'] += 1
                if msg.id > topic_map[thread_id]['last_msg_id']:
                    topic_map[thread_id]['last_msg_id'] = msg.id
    except Exception as e:
        print(f"[CLONE-SCAN] History scan error on bot client: {e}")
        # Try with user client
        if user_client:
            try:
                async for msg in user_client.get_chat_history(chat_id, limit=max_scan):
                    thread_id = None
                    if hasattr(msg, 'reply_to') and msg.reply_to:
                        thread_id = getattr(msg.reply_to, 'forum_topic_id', None)
                        if thread_id is None:
                            thread_id = getattr(msg.reply_to, 'message_id', None)
                    elif hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id:
                        thread_id = msg.reply_to_message_id

                    if thread_id and thread_id not in topic_map:
                        topic_map[thread_id] = {
                            'topic_id': thread_id,
                            'topic_name': f'Topic {thread_id}',
                            'topic_icon_color': 0,
                            'message_count': 1,
                            'first_msg_id': thread_id,
                            'last_msg_id': msg.id,
                            'is_general': (thread_id == 1),
                        }
                    elif thread_id and thread_id in topic_map:
                        topic_map[thread_id]['message_count'] += 1
            except Exception as e2:
                print(f"[CLONE-SCAN] History scan error on user client: {e2}")

    # Try to get actual topic names by fetching the root (service) messages.
    # Topic root messages are service messages — their name lives in
    # msg.forum_topic_created.title (Pyrofork) or msg.action.title (Telethon-style).
    # They never have .text, so we check the service-message attributes directly.
    for topic_id, info in topic_map.items():
        try:
            root_msg = await scan_client.get_messages(chat_id, topic_id)
            if not root_msg:
                continue
            name = None
            # Pyrofork: Message.forum_topic_created is a ForumTopicCreated object
            ftc = getattr(root_msg, 'forum_topic_created', None)
            if ftc:
                name = getattr(ftc, 'title', None) or getattr(ftc, 'name', None)
            # Fallback: some builds expose it as Message.action
            if not name:
                action = getattr(root_msg, 'action', None)
                if action:
                    name = getattr(action, 'title', None)
            if name:
                info['topic_name'] = name[:128]
                print(f"[CLONE-SCAN] Resolved topic name for id={topic_id}: '{name}'")
        except Exception:
            pass

    print(f"[CLONE-SCAN] Scanned {messages_scanned} messages, found {len(topic_map)} topics")
    return list(topic_map.values())


# ═══════════════════════════════════════════════════════════════
# DESTINATION TOPIC CREATION
# ═══════════════════════════════════════════════════════════════

async def create_destination_topics(
    send_client: Client,
    dest_chat_id: int,
    source_topics: list,
    general_topic_id: int = None,
    progress_callback=None
) -> Dict[int, int]:
    """Create forum topics in the destination channel matching the source.

    Args:
        send_client: Client to use for creating topics (must be admin)
        dest_chat_id: Destination channel ID
        source_topics: List of topic info dicts from analyze_source_channel
        general_topic_id: Source General topic ID (we skip creating General)
        progress_callback: Optional async callback(topic_name, index, total)

    Returns:
        Dict mapping source_topic_id -> dest_topic_id
    """
    topic_mapping = {}  # source_topic_id -> dest_topic_id

    if not source_topics:
        return topic_mapping

    # ═══════════════════════════════════════════════════════════════
    # PRE-FLIGHT: Fetch existing topics in the destination channel.
    #
    # If a topic with the same name already exists in the destination,
    # we reuse it instead of creating a duplicate. This is critical for
    # /resumeclone and re-runs: previously created topics are kept,
    # only missing ones are created.
    # ═══════════════════════════════════════════════════════════════
    existing_dest_topics: Dict[str, int] = {}  # lowercase topic_name -> dest_topic_id
    try:
        existing_list = await _fetch_forum_topics(send_client, dest_chat_id, send_client)
        for _t in existing_list:
            _name = (_t.get('topic_name') or '').strip()
            if _name:
                existing_dest_topics[_name.lower()] = _t.get('topic_id')
        print(f"[CLONE-CREATE] Found {len(existing_dest_topics)} existing topics in destination "
              f"(will skip duplicates by name)")
    except Exception as _e:
        print(f"[CLONE-CREATE] Could not pre-fetch existing dest topics (will create all): {_e}")

    # Pre-flight check: verify the client can actually create topics by
    # probing with the first non-General topic. If this fails with a
    # permission error, we abort early instead of failing per-topic.
    non_general_topics = [t for t in source_topics if not t['is_general']]
    total = len(non_general_topics)
    created = 0
    reused = 0  # topics reused from destination (already existed)
    permission_denied = False

    for idx, topic in enumerate(source_topics):
        # Skip General topic — it's auto-created by Telegram
        if topic['is_general']:
            # Map General to General (topic_id 1 in most forums)
            topic_mapping[topic['topic_id']] = 1
            print(f"[CLONE-CREATE] Skipped General topic (id={topic['topic_id']}), mapping to dest topic 1")
            continue

        if permission_denied:
            # Already failed once on permission — don't keep trying.
            topic_mapping[topic['topic_id']] = None
            continue

        topic_name = topic['topic_name']
        # topic_icon_color is None when the source had an invalid/zero colour —
        # omit the parameter so Telegram picks one automatically.
        topic_color = topic.get('topic_icon_color')  # None means "let Telegram decide"

        # ═══════════════════════════════════════════════════════════════
        # DUPLICATE CHECK: If a topic with the same name already exists
        # in the destination, reuse it. This prevents the bot from
        # creating duplicate topics when /resumeclone or /clone is run
        # multiple times.
        # ═══════════════════════════════════════════════════════════════
        _existing_id = existing_dest_topics.get(topic_name.lower())
        if _existing_id:
            topic_mapping[topic['topic_id']] = _existing_id
            reused += 1
            print(f"[CLONE-CREATE] Reusing existing topic '{topic_name}' → dest_id={_existing_id} "
                  f"(source_id={topic['topic_id']}) — skipped creation")
            if progress_callback:
                try:
                    await progress_callback(topic_name, idx + 1, total)
                except Exception:
                    pass
            continue

        try:
            await _clone_rate_limiter.acquire()
            # create_forum_topic returns a ForumTopicCreated object.
            # Its .id IS the new topic's thread_id (the service message ID).
            create_kwargs = dict(chat_id=dest_chat_id, title=topic_name)
            if topic_color is not None:
                create_kwargs['icon_color'] = topic_color
            result = await send_client.create_forum_topic(**create_kwargs)
            # ForumTopicCreated.id is the topic's thread_id
            dest_topic_id = result.id if hasattr(result, 'id') else None
            if not dest_topic_id:
                # Should not happen with pyrofork 2.3.x, but be defensive
                dest_topic_id = getattr(result, 'message_id', None)

            if dest_topic_id:
                topic_mapping[topic['topic_id']] = dest_topic_id
                created += 1
                # Also add to existing_dest_topics so subsequent duplicates
                # in the same source are caught
                existing_dest_topics[topic_name.lower()] = dest_topic_id
                print(f"[CLONE-CREATE] Created topic '{topic_name}' → dest_id={dest_topic_id} "
                      f"(source_id={topic['topic_id']})")
            else:
                print(f"[CLONE-CREATE] Created topic '{topic_name}' but couldn't get dest_id")

        except FloodWait as e:
            wait = e.value if hasattr(e, 'value') else 30
            print(f"[CLONE-CREATE] FloodWait {wait}s while creating topic '{topic_name}'")
            await asyncio.sleep(wait + 2)
            # Retry once
            try:
                retry_kwargs = dict(chat_id=dest_chat_id, title=topic_name)
                if topic_color is not None:
                    retry_kwargs['icon_color'] = topic_color
                result = await send_client.create_forum_topic(**retry_kwargs)
                dest_topic_id = result.id if hasattr(result, 'id') else None
                if dest_topic_id:
                    topic_mapping[topic['topic_id']] = dest_topic_id
                    created += 1
                    existing_dest_topics[topic_name.lower()] = dest_topic_id
            except Exception as e2:
                print(f"[CLONE-CREATE] Retry failed for '{topic_name}': {e2}")
                # Map to None so we know it failed — messages will go to General
                topic_mapping[topic['topic_id']] = None

        except Exception as e:
            err_str = str(e).lower()
            print(f"[CLONE-CREATE] Failed to create topic '{topic_name}': {e}")
            # Detect permission errors and stop trying — every subsequent
            # call will fail the same way, so we save API quota and time.
            if (
                'not enough rights' in err_str
                or 'admin right needed' in err_str
                or 'forbidden' in err_str
                or 'chat_admin_rights_required' in err_str
                or 'user_not_mutual_contact' in err_str
                or 'manage_topics' in err_str
            ):
                print(f"[CLONE-CREATE] Permission error — aborting further topic creation")
                permission_denied = True
                topic_mapping[topic['topic_id']] = None
            else:
                topic_mapping[topic['topic_id']] = None

        if progress_callback:
            try:
                await progress_callback(topic_name, idx + 1, total)
            except Exception:
                pass

    print(f"[CLONE-CREATE] Created {created}/{total} topics in destination "
          f"({'permission denied — fell back to General for remaining' if permission_denied else 'ok'}, "
          f"reused {reused} existing)")
    return topic_mapping


# ═══════════════════════════════════════════════════════════════
# MESSAGE ROUTING — determine which topic a source message belongs to
# ═══════════════════════════════════════════════════════════════

def get_message_topic_id(msg: Message) -> Optional[int]:
    """Extract the forum topic ID from a message.

    In Telegram forums, every message has reply_to.forum_topic_id
    that indicates which topic it belongs to.

    Returns:
        int: The topic ID, or None if the message is not in a forum/topic.
    """
    if hasattr(msg, 'reply_to') and msg.reply_to:
        # Method 1: forum_topic_id (Pyrofork)
        fid = getattr(msg.reply_to, 'forum_topic_id', None)
        if fid:
            return fid

        # Method 2: For messages that reply to the topic's root message
        # The reply_to_message_id IS the topic_id for forum messages
        rtmid = getattr(msg.reply_to, 'message_id', None)
        if rtmid:
            return rtmid

    # Fallback: reply_to_message_id
    if hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id:
        return msg.reply_to_message_id

    return None


def is_topic_root_message(msg: Message) -> bool:
    """Check if a message is the root/first message of a forum topic.

    Topic root messages are service messages that created the topic.
    We should skip these since we create our own topics in the destination.
    """
    # A message is a topic root if:
    # 1. It has forum_topic_created attribute
    if hasattr(msg, 'forum_topic_created') and msg.forum_topic_created:
        return True
    # 2. It's a service message that's the first message in a topic
    # (We detect this by checking if its ID matches a topic ID we know about)
    return False


# ═══════════════════════════════════════════════════════════════
# MAIN CLONE ENGINE
# ═══════════════════════════════════════════════════════════════

async def run_clone(
    uid: int,
    source_chat_id,
    source_link_type: str,
    start_msg_id: int,
    message_count: int,
    dest_chat_id: int,
    ubot: Client,
    uc: Client,
    pt: Message,  # progress message to edit
    source_analysis: dict = None,
):
    """Execute the channel clone operation.

    This is the main entry point called from batch.py's text_handler
    when the user selects "Clone Channel Structure" mode.

    Args:
        uid: User ID
        source_chat_id: Source channel ID (string or int)
        source_link_type: 'public' or 'private'
        start_msg_id: First message ID to clone
        message_count: Number of messages to clone
        dest_chat_id: Destination channel ID (int)
        ubot: Bot client for sending
        uc: User client for reading source
        pt: Progress message (will be edited with status updates)
        source_analysis: Pre-computed analysis (or None to compute here)
    """
    from plugins.batch import (
        safe_edit, safe_reply, is_user_active, add_active_batch,
        remove_active_batch, should_cancel, process_msg, get_msg,
        resolve_chat, get_Y, BATCH_SEND_DELAY, batch_heartbeat,
        log_ram, load_upload_map, save_upload_map, _batch_cooldown_check,
        _rate_limiter, _download_rate_limiter, _download_with_retry,
        _format_duration, request_batch_cancel, batch_tasks,
    )

    # Initialise before try so except blocks can always reference it
    _user_chat_id = uid
    mid = start_msg_id  # last-processed msg ID, updated in loop
    j = 0               # loop counter
    success_count = 0
    _heartbeat_task = None  # background heartbeat task — started before message loop

    try:
        # ─── STEP 1: Analyze source channel ───
        await safe_edit(pt, '🔬 **Step 1/5: Analyzing source channel structure...**')
        log_ram("clone_start", extra_info={"uid": uid, "source": source_chat_id})

        analysis_client = uc if uc else ubot
        if not analysis_client:
            analysis_client = get_Y()

        if not source_analysis:
            source_analysis = await analyze_source_channel(
                analysis_client, source_chat_id, uc
            )

        is_forum = source_analysis.get('is_forum', False)
        source_topics = source_analysis.get('topics', [])
        general_topic_id = source_analysis.get('general_topic_id')

        if is_forum and source_topics:
            topic_summary = '\n'.join(
                f"  📁 {t['topic_name']} (~{t['message_count']} msgs)"
                for t in source_topics[:10]
            )
            if len(source_topics) > 10:
                topic_summary += f"\n  ... and {len(source_topics) - 10} more topics"
            await safe_edit(pt,
                f'✅ **Source is a FORUM** with {len(source_topics)} topics:\n\n'
                f'{topic_summary}\n\n'
                f'⏳ Proceeding to set up destination...'
            )
        else:
            await safe_edit(pt,
                'ℹ️ **Source is a regular channel** (no forums).\n\n'
                'Will clone as flat messages to destination.'
            )

        # ─── STEP 2: Verify destination ───
        await safe_edit(pt, '🔍 **Step 2/5: Checking destination channel...**')

        dest_chat_info = None
        dest_is_forum = False
        for cl_label, cl in [("bot", ubot), ("user_client", uc)]:
            if not cl:
                continue
            try:
                dest_chat_info = await cl.get_chat(dest_chat_id)
                break
            except Exception as e:
                print(f"[CLONE] {cl_label} can't access dest: {e}")

        if not dest_chat_info:
            await safe_edit(pt,
                f'❌ **Cannot access destination channel** `{dest_chat_id}`\n\n'
                f'Make sure:\n'
                f'• The bot/userbot is admin in the destination\n'
                f'• The channel ID is correct\n'
                f'• Use /settings to set the destination channel'
            )
            return

        dest_is_forum = await _is_chat_forum(ubot, dest_chat_id, uc)
        print(f"[CLONE] Destination chat {dest_chat_id} — detected as forum: {dest_is_forum}")

        # ── Determine which client can actually send to the destination ──
        # The bot client may not be a member/admin of the dest channel.
        # If resolve_peer fails, fall back to the user client as sender.
        _send_client = ubot
        try:
            await ubot.resolve_peer(dest_chat_id)
            print(f"[CLONE] Bot client can reach dest {dest_chat_id} — using bot as sender")
        except Exception as _rp_err:
            print(f"[CLONE] Bot cannot reach dest {dest_chat_id} ({_rp_err}) — falling back to user client for sending")
            _send_client = uc

        # ── If source is a forum but dest is NOT, try to auto-convert dest ──
        # pyrofork's Chat object doesn't expose is_forum reliably, so we use
        # _is_chat_forum() which probes via get_forum_topics + raw MTProto.
        # If dest is still not a forum, attempt conversion via ToggleForum raw API.
        if is_forum and not dest_is_forum:
            await safe_edit(pt,
                f'🔄 **Destination is not a forum yet.**\n\n'
                f'Destination: `{dest_chat_info.title}`\n\n'
                f'Attempting to **auto-convert** it to a forum so topics can be created...\n'
                f'(requires the bot/userbot to be admin with **Change Info** rights)'
            )

            conversion_ok = False
            # Try with the send client first (most likely to have admin rights)
            for try_client_label, try_client in [
                ("send_client", _send_client),
                ("user_client", uc),
                ("bot", ubot),
            ]:
                if not try_client:
                    continue
                print(f"[CLONE] Trying to convert dest to forum via {try_client_label}...")
                if await _convert_to_forum(try_client, dest_chat_id):
                    conversion_ok = True
                    break
                await asyncio.sleep(1)  # small delay between attempts

            if conversion_ok:
                dest_is_forum = True
                await safe_edit(pt,
                    f'✅ **Destination converted to a forum!**\n\n'
                    f'⏳ Proceeding to create topics...'
                )
                await asyncio.sleep(2)  # let Telegram propagate the change
            else:
                await safe_edit(pt,
                    '⚠️ **Could not auto-convert destination to a forum.**\n\n'
                    f'Destination: `{dest_chat_info.title}`\n\n'
                    'Options:\n'
                    '1. Convert destination to a forum manually (Telegram app → Edit → Topics → Turn On)\n'
                    '2. Make sure the bot/userbot is admin with **Change Info** rights, then retry\n'
                    '3. Continue anyway (all messages go to General, no topic separation)\n\n'
                    '⏳ Waiting 15s then continuing in flat mode...'
                )
                await asyncio.sleep(15)
                # User was warned — continue with flat mode
                is_forum = False  # Downgrade to flat mode

        # ─── STEP 3: Create topics in destination ───
        topic_mapping = {}  # source_topic_id -> dest_topic_id

        if is_forum and source_topics:
            await safe_edit(pt,
                f'🏗️ **Step 3/5: Creating {len(source_topics)} topics in destination...**'
            )

            async def topic_progress(name, idx, total):
                try:
                    await safe_edit(pt,
                        f'🏗️ **Creating topics...** ({idx}/{total})\n'
                        f'Latest: `{name}`'
                    )
                except Exception:
                    pass

            # Use the client that can reach the destination (bot or user fallback)
            topic_client = _send_client
            topic_mapping = await create_destination_topics(
                topic_client, dest_chat_id, source_topics,
                general_topic_id, topic_progress
            )

            created_count = sum(1 for v in topic_mapping.values() if v is not None)
            failed_count = sum(1 for v in topic_mapping.values() if v is None and v != 1)
            # Don't count General (mapped to 1) as failed
            total_to_create = sum(1 for t in source_topics if not t['is_general'])
            failed_count = max(0, total_to_create - created_count)
            if failed_count > 0:
                await safe_edit(pt,
                    f'⚠️ **Step 3/5: Created {created_count}/{total_to_create} topics**\n\n'
                    f'{failed_count} topic(s) could not be created (likely missing **Manage Topics** admin right).\n'
                    f'Messages from those topics will go to General.\n\n'
                    f'⏳ Starting message cloning...'
                )
            else:
                await safe_edit(pt,
                    f'✅ **Step 3/5: Created {created_count} topics** in destination\n\n'
                    f'_Existing topics with the same name were reused — no duplicates created._\n\n'
                    f'⏳ Starting message cloning...'
                )
        else:
            await safe_edit(pt, '✅ **Step 3/5: Skipped** (no forum topics to create)\n\n⏳ Starting message cloning...')

        # ─── STEP 4: Set up active batch tracking + persist job for resume ───
        await add_active_batch(uid, {
            'source': str(source_chat_id),
            'dest': str(dest_chat_id),
            'mode': 'clone',
            'current': 0,
            'success': 0,
            'total': message_count,
        })
        # Persist to MongoDB so resume survives restarts / session-file deletion
        _user_chat_id = getattr(pt, 'chat', None)
        _user_chat_id = _user_chat_id.id if _user_chat_id else uid
        await _save_clone_job(
            uid=uid,
            source_chat_id=source_chat_id,
            source_link_type=source_link_type,
            start_msg_id=start_msg_id,
            message_count=message_count,
            dest_chat_id=dest_chat_id,
            user_chat_id=_user_chat_id,
            status="running",
        )

        # ─── STEP 5: Clone messages ───
        await safe_edit(pt,
            f'🚀 **Step 4/5: Cloning {message_count} messages...**\n\n'
            f'0/{message_count} processed'
        )

        success_count = 0
        skip_count = 0
        error_count = 0
        start_time = time.time()

        end_msg_id = start_msg_id + message_count - 1

        # Load existing upload map for skip detection
        msg_id_map, last_uploaded_id, _ = await load_upload_map(uid, str(source_chat_id))

        # Start background heartbeat — keeps batch_state.updated_at fresh so
        # startup_auto_resume can detect a live clone vs a crashed one.
        # Must be create_task (not awaited) — it's a perpetual while-True loop.
        _heartbeat_task = asyncio.create_task(
            batch_heartbeat(uid, str(source_chat_id))
        )

        # Build a reverse lookup: source topic_id -> dest topic_id
        # Also build a per-message config override: which dest topic to send to
        for j in range(message_count):
            mid = start_msg_id + j

            # Cancel check
            if should_cancel(uid):
                print(f"[CLONE] Cancelled by user at msg {mid}")
                break

            # Progress update every 5 messages
            if (j + 1) % 5 == 0 or j + 1 == message_count:
                elapsed = time.time() - start_time
                speed = (j + 1) / elapsed if elapsed > 0 else 0
                eta = (message_count - j - 1) / speed if speed > 0 else 0
                eta_str = time.strftime('%M:%S', time.gmtime(eta))
                try:
                    await safe_edit(pt,
                        f'📦 **Cloning messages...**\n\n'
                        f'📊 {j + 1}/{message_count} ({(j+1)/message_count*100:.1f}%)\n'
                        f'✅ Success: {success_count} | ⏭️ Skipped: {skip_count} | ❌ Errors: {error_count}\n'
                        f'⚡ Speed: {speed:.1f} msg/s | ⏳ ETA: {eta_str}'
                    )
                except Exception:
                    pass

            # Skip if already uploaded
            if mid in msg_id_map:
                skip_count += 1
                continue

            # Fetch message from source
            try:
                msg = await get_msg(ubot, uc, source_chat_id, mid, source_link_type)
            except Exception as e:
                print(f"[CLONE] Error fetching msg {mid}: {e}")
                error_count += 1
                continue

            if not msg or getattr(msg, 'empty', False):
                skip_count += 1
                continue

            # Skip topic root messages (service messages that create topics)
            if is_topic_root_message(msg):
                print(f"[CLONE] Skipped topic root message {mid}")
                skip_count += 1
                continue

            # Determine destination topic ID
            dest_topic_id = None

            if is_forum:
                src_topic_id = get_message_topic_id(msg)
                if src_topic_id and src_topic_id in topic_mapping:
                    dest_topic_id = topic_mapping[src_topic_id]
                    if dest_topic_id is None:
                        # Topic creation failed — send to General (topic 1)
                        dest_topic_id = general_topic_id if general_topic_id else 1
                elif src_topic_id:
                    # Unknown topic — send to General
                    print(f"[CLONE] Unknown source topic {src_topic_id} for msg {mid} — sending to General")
                    dest_topic_id = 1
                # else: no topic info — message goes to General (no topic_id = General)

            # Process the message (same as batch.py's process_msg)
            try:
                await _clone_rate_limiter.acquire()

                # Pass pre-resolved tcid/topic_id directly so process_msg
                # never calls c.resolve_peer() per-message (which would fail
                # if the bot client can't access the destination channel).
                # _send_client is the bot OR user client, whichever reached dest.
                # IMPORTANT: pass d=str(uid) (user's chat) NOT str(dest_chat_id) —
                # process_msg sends "Downloading..." progress messages to d, and
                # sending those into the destination channel triggers FloodWait.
                _msg_topic_id = dest_topic_id if dest_topic_id and dest_topic_id != 1 else None
                result, sent_id, _, _ = await process_msg(
                    _send_client, uc, msg, str(uid), source_link_type, uid,
                    source_chat_id,
                    reply_to_destination_id=None,
                    _skip_verify=True,
                    _skip_explanation_scan=True,
                    _cached_tcid=dest_chat_id,
                    _cached_topic_id=_msg_topic_id,
                    _cached_rtmid=None,
                )

                if sent_id:
                    success_count += 1
                    msg_id_map[mid] = sent_id
                    # Incremental save every 20 messages (upload map + resume state)
                    if (j + 1) % 20 == 0:
                        await save_upload_map(uid, str(source_chat_id), msg_id_map)
                        await _save_clone_job(
                            uid=uid,
                            source_chat_id=source_chat_id,
                            source_link_type=source_link_type,
                            start_msg_id=start_msg_id,
                            message_count=message_count,
                            dest_chat_id=dest_chat_id,
                            user_chat_id=_user_chat_id,
                            status="running",
                            last_processed_msg_id=mid,
                            processed_count=j + 1,
                            success_count=success_count,
                        )
                else:
                    # process_msg returned but no sent_id — might be a skipped msg type
                    skip_count += 1

            except asyncio.CancelledError:
                print(f"[CLONE] Cancelled at msg {mid}")
                break
            except FloodWait as fw:
                wait_secs = getattr(fw, 'value', 30)
                print(f"[CLONE] FloodWait {wait_secs}s on msg {mid} — sleeping before continuing")
                await asyncio.sleep(wait_secs + 2)
                error_count += 1
                continue
            except Exception as e:
                error_count += 1
                print(f"[CLONE] Error processing msg {mid}: {e}")
                import traceback
                traceback.print_exc()
                continue

            # Rate limiting between messages — 10s gap to avoid FloodWait
            await asyncio.sleep(10)

            # Cooldown check (reuse batch.py's cooldown)
            try:
                await _batch_cooldown_check(j, uid)
            except Exception:
                pass

            # Update progress
            await add_active_batch(uid, {
                'source': str(source_chat_id),
                'dest': str(dest_chat_id),
                'mode': 'clone',
                'current': j + 1,
                'success': success_count,
                'total': message_count,
            })

        # ─── FINAL: Save upload map and report ───
        if msg_id_map:
            await save_upload_map(uid, str(source_chat_id), msg_id_map)

        # Mark job complete in MongoDB
        await _save_clone_job(
            uid=uid,
            source_chat_id=source_chat_id,
            source_link_type=source_link_type,
            start_msg_id=start_msg_id,
            message_count=message_count,
            dest_chat_id=dest_chat_id,
            user_chat_id=_user_chat_id,
            status="complete",
            last_processed_msg_id=start_msg_id + message_count - 1,
            processed_count=message_count,
            success_count=success_count,
        )

        elapsed = time.time() - start_time
        duration_str = time.strftime('%H:%M:%S', time.gmtime(elapsed))

        await safe_edit(pt,
            f'✅ **Clone Complete!**\n\n'
            f'📊 **Results:**\n'
            f'  ✅ Copied: {success_count}\n'
            f'  ⏭️ Skipped: {skip_count}\n'
            f'  ❌ Errors: {error_count}\n'
            f'  ⏱️ Duration: {duration_str}\n'
            f'  📁 Topics created: {sum(1 for v in topic_mapping.values() if v is not None)}\n\n'
            f'{"🔄 Use /resumeclone if any messages were missed." if error_count > 0 else ""}'
        )

        print(f"[CLONE] Done for uid={uid}: success={success_count}, skip={skip_count}, "
              f"error={error_count}, time={duration_str}")

    except asyncio.CancelledError:
        print(f"[CLONE] Task cancelled for uid={uid}")
        try:
            await _save_clone_job(
                uid=uid, source_chat_id=source_chat_id,
                source_link_type=source_link_type, start_msg_id=start_msg_id,
                message_count=message_count, dest_chat_id=dest_chat_id,
                user_chat_id=_user_chat_id, status="interrupted",
                last_processed_msg_id=mid,
                processed_count=j,
                success_count=success_count,
            )
        except Exception:
            pass
        try:
            await safe_edit(pt, '🛑 **Clone cancelled.**\n\nUse /resumeclone to continue from where it stopped.')
        except Exception:
            pass
    except Exception as e:
        print(f"[CLONE] Fatal error for uid={uid}: {e}")
        import traceback
        traceback.print_exc()
        try:
            await _save_clone_job(
                uid=uid, source_chat_id=source_chat_id,
                source_link_type=source_link_type, start_msg_id=start_msg_id,
                message_count=message_count, dest_chat_id=dest_chat_id,
                user_chat_id=_user_chat_id, status="interrupted",
                last_processed_msg_id=mid,
                processed_count=j,
                success_count=success_count,
            )
        except Exception:
            pass
        try:
            await safe_edit(pt, f'❌ **Clone failed:** {str(e)[:200]}\n\nUse /resumeclone to retry.')
        except Exception:
            pass
    finally:
        # Cleanup
        # Cancel the background heartbeat task
        if _heartbeat_task and not _heartbeat_task.done():
            _heartbeat_task.cancel()
            try:
                await _heartbeat_task
            except asyncio.CancelledError:
                pass
        await remove_active_batch(uid)
        batch_tasks.pop(uid, None)
        CLONE_STATE.pop(uid, None)
        _clone_rate_limiter.clear()
        try:
            clear_cancel_flag = _get_batch_helpers()['clear_cancel_flag']
            await clear_cancel_flag(uid)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
# /clone COMMAND — Direct entry point
# ═══════════════════════════════════════════════════════════════

@X.on_message(filters.command(['clone']))
async def clone_cmd(c, m):
    """Direct /clone command — shortcut to enter clone mode."""
    from plugins.start import subscribe as sub
    from plugins.batch import safe_reply, is_user_active

    uid = m.from_user.id

    # Auth check
    if uid in OWNER_ID:
        pass
    elif await is_auth_user(uid):
        pass
    elif not await is_premium_user(uid):
        await safe_reply(m, "This feature requires premium or owner access.")
        return

    if await sub(c, m) == 1:
        return

    if is_user_active(uid):
        await safe_reply(m, 'You have an active task. Use /stop to cancel it.')
        return

    pro = await safe_reply(m, '🔗 **Clone Mode** — Send a **message link** from the source channel (e.g. `https://t.me/c/1234567/1`) or just the channel link (e.g. `https://t.me/c/1234567`).')
    CLONE_STATE[uid] = {'step': 'got_source_link', 'pro_message': pro}

    # Also set Z[uid] so batch.py's text_handler doesn't interfere
    # We use a special step that only channel_clone.py handles
    from plugins.batch import Z
    Z[uid] = {'step': 'clone_source', 'clone_mode': True}


# ═══════════════════════════════════════════════════════════════
# /resumeclone COMMAND — Resume an interrupted clone
# ═══════════════════════════════════════════════════════════════

@X.on_message(filters.command(['resumeclone']))
async def resumeclone_cmd(c, m):
    """Resume an interrupted clone job from MongoDB state.

    Resume safety: the upload_maps collection tracks every successfully sent
    message, so re-running automatically skips already-uploaded messages.
    This works even if the session file was deleted — the skip logic is
    driven by MongoDB data, not in-memory state.
    """
    from plugins.batch import (
        safe_reply, is_user_active, remove_active_batch,
        batch_tasks, _CANCEL_FLAGS,
    )

    uid = m.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid) and not await is_premium_user(uid):
        await safe_reply(m, "This feature requires premium or owner access.")
        return

    # ═══════════════════════════════════════════════════════════════
    # STALE-STATE CLEANUP: If is_user_active() is True but there's no
    # actual asyncio.Task running (e.g. previous bot crashed mid-batch),
    # treat it as stale and clean up automatically instead of refusing.
    # This is the most common reason /resumeclone "doesn't respond" —
    # the user gets a refusal message that itself fails to send during
    # FloodWait, so they see silence.
    # ═══════════════════════════════════════════════════════════════
    if is_user_active(uid):
        _task = batch_tasks.get(uid)
        if _task is None or _task.done():
            # Stale entry — no live task. Clean up and proceed.
            print(f"[RESUMECLONE] uid={uid} had stale ACTIVE_USERS entry (task={_task}) — cleaning up")
            try:
                await remove_active_batch(uid)
            except Exception as _e:
                print(f"[RESUMECLONE] remove_active_batch error: {_e}")
            batch_tasks.pop(uid, None)
            _CANCEL_FLAGS.pop(uid, None)
        else:
            # Live task is actually running — refuse.
            await safe_reply(m, '⚠️ You already have an active task. Use /stop first.')
            return

    job = await _load_clone_job(uid)
    if not job:
        await safe_reply(m, '✅ No interrupted clone found. Start a new one with /clone.')
        return

    status = job.get('status', 'unknown')
    if status == 'complete':
        await safe_reply(m,
            f'✅ Your last clone is already marked **complete**.\n\n'
            f'Source: `{job.get("source_chat_id")}`\n'
            f'Use /clone to start a new one, or /clearbatch to reset.'
        )
        return

    source_chat_id = job['source_chat_id']
    source_link_type = job.get('source_link_type', 'private')
    start_msg_id = job['start_msg_id']
    message_count = job['message_count']
    dest_chat_id = job['dest_chat_id']
    last_id = job.get('last_processed_msg_id', start_msg_id)
    done = job.get('processed_count', 0)

    await safe_reply(m,
        f'🔄 **Resuming Clone**\n\n'
        f'**Source:** `{source_chat_id}`\n'
        f'**Range:** msg {start_msg_id} → {start_msg_id + message_count - 1}\n'
        f'**Last processed:** msg {last_id} ({done}/{message_count})\n\n'
        f'Already-sent messages will be skipped automatically...\n'
        f'_Existing topics in destination will be reused — no duplicates._'
    )
    await _execute_clone(c, m, uid, source_chat_id, start_msg_id,
                         source_link_type, message_count,
                         override_dest=dest_chat_id)


# ═══════════════════════════════════════════════════════════════
# CLONE TEXT HANDLER — handles conversation flow for /clone
# ═══════════════════════════════════════════════════════════════

@X.on_message(filters.text & filters.private & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id',
    'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys',
    'setbot', 'rembot', 'auth', 'unauth', 'authusers', 'logs', 'fetch', 'cancelfetch',
    'fetchmaps', 'clearfetch', 'answerkey', 'clearbatch', 'clear', 'status',
    'viewfetchmaps', 'viewanswerkey', 'clearanswerkey', 'settings', 'help', 'terms', 'plan',
    'auto', 'autooff', 'cancelauto', 'linkexplan', 'explans', 'transfer', 'rem', 'dl', 'adl',
    'setwatermark', 'clone', 'mirror', 'mirrorstop', 'mirrorstatus', 'relink', 'explanlogs',
]))
async def clone_text_handler(c, m):
    """Handle text messages for the /clone conversation flow.

    This handler checks if the user is in a clone flow and processes
    their input accordingly. Command exclusions prevent it from intercepting
    /start, /help, and other commands before their dedicated handlers run.
    """
    uid = m.from_user.id

    # Check both CLONE_STATE and Z for clone state
    from plugins.batch import Z as _Z
    is_clone_flow = uid in CLONE_STATE or (
        uid in _Z and _Z[uid].get('step', '').startswith('clone')
    )

    if not is_clone_flow:
        raise ContinuePropagation  # Not in clone flow — let other handlers handle it

    # Sync CLONE_STATE from Z if needed (when entering from /batch callback)
    if uid not in CLONE_STATE and uid in _Z:
        z_step = _Z[uid].get('step')
        if z_step == 'clone_source':
            CLONE_STATE[uid] = {'step': 'got_source_link', 'pro_message': m}

    if uid not in CLONE_STATE:
        raise ContinuePropagation

    state = CLONE_STATE[uid]
    step = state.get('step')

    from plugins.batch import (
        safe_reply, safe_edit, get_ubot, get_uclient, get_Y,
        is_user_active, add_active_batch, Z, E,
    )

    if step == 'got_source_link':
        # User sent the source link
        link = m.text.strip()
        i, d, lt = E(link)
        if not i or not d:
            await safe_reply(m, '❌ Invalid link format. Send a valid Telegram message link.')
            CLONE_STATE.pop(uid, None)
            Z.pop(uid, None)
            return

        CLONE_STATE[uid].update({
            'step': 'got_count',
            'cid': i,
            'sid': d,
            'lt': lt,
        })
        Z[uid].update({'step': 'clone_count', 'cid': i, 'sid': d, 'lt': lt})

        await safe_reply(m,
            '✅ Source link accepted.\n\n'
            '**How many messages to clone?**\n\n'
            '1️⃣ **Number** — e.g. `500`\n'
            '2️⃣ **Last link** — clone from start to that link\n'
            '3️⃣ **all** — clone ALL messages'
        )

    elif step == 'got_count':
        count = None
        input_text = m.text.strip().lower()
        cid = CLONE_STATE[uid]['cid']
        sid = CLONE_STATE[uid]['sid']
        lt = CLONE_STATE[uid]['lt']

        if input_text == 'all':
            try:
                uc_for_scan = await get_uclient(uid)
                ubot_for_scan = await get_ubot(uid) or X
                scan_client = uc_for_scan or ubot_for_scan or get_Y()
                if not scan_client:
                    await safe_reply(m, '❌ No client available. Use /login first.')
                    CLONE_STATE.pop(uid, None)
                    Z.pop(uid, None)
                    return

                resolved_chat = await scan_client.get_chat(
                    int(cid) if str(cid).lstrip('-').isdigit() else cid
                )
                # FIX: pass resolved_chat.id (int), not the Chat object —
                # Pyrogram serialises objects to username strings → USERNAME_INVALID
                async for last_msg in scan_client.get_chat_history(resolved_chat.id, limit=1):
                    if last_msg and last_msg.id:
                        count = last_msg.id - sid + 1
                    break
                if not count:
                    await safe_reply(m, '❌ Could not determine last message. Send a number or link.')
                    return
            except Exception as e:
                await safe_reply(m, f'❌ Error reading channel: {e}')
                CLONE_STATE.pop(uid, None)
                Z.pop(uid, None)
                return

        elif m.text.isdigit():
            count = int(m.text)
        else:
            end_i, end_d, end_lt = E(m.text.strip())
            if end_i and end_d:
                if str(end_i) != str(cid):
                    await safe_reply(m, 'Last link must be from the same channel.')
                    return
                if end_d < sid:
                    await safe_reply(m, 'The last link message ID must be greater than the start link.')
                    return
                count = end_d - sid + 1
            else:
                await safe_reply(m, 'Please send a number, last link, or "all".')
                return

        # Proceed to clone execution
        CLONE_STATE[uid].update({'step': 'cloning', 'num': count})
        Z[uid].update({'step': 'clone_running'})
        await _execute_clone(c, m, uid, cid, sid, lt, count)


async def _execute_clone(c, m, uid, source_chat_id, start_msg_id, link_type, count,
                        override_dest: int = None):
    """Set up clients and kick off the clone.

    Args:
        override_dest: If provided, use this destination instead of reading
                       from user settings. Used by /resumeclone.
    """
    from plugins.batch import (
        safe_reply, safe_edit, get_ubot, get_uclient, get_Y,
        is_user_active, resolve_peers_at_startup, resolve_chat,
    )

    pt = await safe_reply(m, '⏳ Setting up clone...')

    # Get clients
    try:
        uc = await asyncio.wait_for(get_uclient(uid), timeout=90)
    except asyncio.TimeoutError:
        await safe_edit(pt, '❌ User client timed out. Use /login first.')
        CLONE_STATE.pop(uid, None)
        from plugins.batch import Z
        Z.pop(uid, None)
        return

    ubot = await get_ubot(uid) or X
    if not uc:
        uc = get_Y()
        if not uc:
            await safe_edit(pt, '❌ No user client available. Use /login first.')
            CLONE_STATE.pop(uid, None)
            from plugins.batch import Z
            Z.pop(uid, None)
            return

    # Get destination (override_dest takes priority — used by /resumeclone)
    if override_dest:
        dest_chat_id = override_dest
    else:
        cfg_chat = await get_user_data_key(str(m.chat.id), 'chat_id', None)
        dest_chat_id = None
        if cfg_chat:
            try:
                dest_chat_id = int(cfg_chat.split('/')[0]) if '/' in cfg_chat else int(cfg_chat)
            except Exception:
                pass
        if not dest_chat_id:
            dest_chat_id = int(str(m.chat.id))

    # Resolve peers — non-fatal: warn and continue rather than abort.
    # USERNAME_INVALID / PEER_ID_INVALID on a fresh session is recoverable;
    # the actual get_messages calls will re-resolve peers on demand.
    # Root cause: E() returns public-channel usernames or numeric strings;
    # resolve_peers normalises them, but a brand-new session may not have
    # the peer cached yet — that resolves automatically once the first API
    # call (get_messages) populates the cache.
    try:
        from plugins.batch import resolve_peers_at_startup
        await resolve_peers_at_startup(uc, ubot, source_chat_id, dest_chat_id)
    except Exception as e:
        print(f"[CLONE] resolve_peers warning (non-fatal): {e}")
        await safe_edit(pt,
            f'⚠️ Peer pre-resolution warning (will retry during clone):\n'
            f'`{str(e)[:200]}`\n\nContinuing...'
        )
        await asyncio.sleep(3)

    # Run the clone as a tracked asyncio.Task stored in batch_tasks[uid].
    # This lets /stop's existing task.cancel() logic interrupt it immediately —
    # including breaking out of the 10-second inter-message sleep — instead of
    # waiting up to 10s for the should_cancel() flag to be noticed.
    clone_task = asyncio.create_task(run_clone(
        uid=uid,
        source_chat_id=source_chat_id,
        source_link_type=link_type,
        start_msg_id=start_msg_id,
        message_count=count,
        dest_chat_id=dest_chat_id,
        ubot=ubot,
        uc=uc,
        pt=pt,
    ))
    batch_tasks[uid] = clone_task
    try:
        await clone_task
    except asyncio.CancelledError:
        pass  # Cancelled by /stop or /clearbatch — run_clone's finally block handles cleanup
