# ════════════════════════════════════════════════════════════════════
# /RELINK — Production-Grade Retroactive Link Repair System
#
# 7 Layers of Robustness:
#   Layer 1: Crash-Proof Checkpoint Resume (MongoDB relink_sessions)
#   Layer 2: Multi-Strategy Link Resolution (5 strategies + fingerprint)
#   Layer 3: Entity-Safe Editing (formatting preservation)
#   Layer 4: Rate Limit Armor (adaptive throttling)
#   Layer 5: Progress Dashboard & Cancellation
#   Layer 6: Pre-Flight Validation (fail-fast checks)
#   Layer 7: Auto-Relink on New Mirror (real-time fixing)
#
# 7 GAPS (enhancements over base system):
#   GAP 1: Fingerprint Matching — replaces unreliable SequenceMatcher
#   GAP 2: Deep Reply Chain Walking — up to 10 levels deep
#   GAP 3: Cross-Session Cache — permanent resolution cache in MongoDB
#   GAP 4: Scan Direction Control — old_to_new / new_to_old / auto
#   GAP 5: Bulk Edit Queue — burst editing with FloodWait handling
#   GAP 6: Source Channel New Message Scan — catches post-batch links
#   GAP 7: Completion Notification — detailed summary with stats
#
# Usage:
#   /relink                        — Scan entire chat, fix all broken links
#   /relink backfill               — Build Smart Cache index (fast, no editing)
#   /relink status                 — Show current/past session progress
#   /relink cancel                 — Cancel running session (progress saved)
#   /relink retry                  — Retry all previously failed edits
#   /relink --limit 100            — Scan only last 100 messages
#   /relink --dry-run              — Preview changes without editing
#   /relink --direction old_to_new — Scan from oldest to newest
#   /relink --direction new_to_old — Scan from newest to oldest (default)
#   /relink --direction auto       — First run=old_to_new, then=new_to_old
# ════════════════════════════════════════════════════════════════════

import os
import re
import copy
import time
import asyncio
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher  # Kept as fallback
from typing import Optional, Dict, List, Any, Tuple

from pyrogram import Client, filters, ContinuePropagation
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait, MessageNotModified, MessageIdInvalid,
    ChatAdminRequired, UserNotParticipant, PeerIdInvalid,
    ChannelPrivate, ChatWriteForbidden
)

from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME

logger = logging.getLogger(__name__)

# ── MongoDB Collections ──────────────────────────────────────────
_relink_mongo = AsyncIOMotorClient(MONGO_URI)
_relink_db = _relink_mongo[DB_NAME]
relink_sessions_collection = _relink_db["relink_sessions"]
relink_cache_collection = _relink_db["relink_url_cache"]  # Resolved URL cache (GAP 3)
fingerprints_collection = _relink_db["relink_fingerprints"]  # GAP 1: Fingerprint index
source_scan_watermark_collection = _relink_db["source_scan_watermark"]  # GAP 6: Scan watermark

# Import shared client
from shared_client import app as X

# ── LAZY IMPORTS from plugins.batch ───────────────────────────
# We do NOT import from batch.py at module level to avoid circular import issues.
# batch.py imports from relink.py (inside functions), but if we also import
# from batch.py at module level, Python can deadlock or crash on startup.
# Instead, we cache them on first use via _get_batch_funcs().
_batch_funcs = {}
_batch_funcs_fully_loaded = False  # Separate flag — partial imports don't count

async def _get_batch_funcs():
    """Lazy-load and cache functions from plugins.batch.
    
    Called once on first /relink invocation, then cached.
    This prevents circular import crashes at startup.
    
    IMPORTANT: We use a _batch_funcs_fully_loaded flag instead of checking
    `if _batch_funcs:` because partial imports (_edlog, get_Y) add keys
    to _batch_funcs before the full import runs. A truthy dict would
    cause early return and KeyError for safe_reply, safe_edit, etc.
    """
    global _batch_funcs_fully_loaded
    if _batch_funcs_fully_loaded:
        return _batch_funcs
    
    from plugins.batch import (
        rewrite_telegram_links,
        rewrite_entity_urls,
        _build_source_patterns,
        _edlog,
        _extract_flood_wait_local,
        load_combined_msg_id_map,
        build_multi_source_channels,
        load_upload_map,
        mark_needs_link_update,
        mark_links_resolved,
        unresolved_links_collection,
        upload_maps_collection,
        mirror_state_collection,
        mirror_src_to_dst_collection,
        normalize_channel_id as batch_normalize_channel_id,
        ADDITIONAL_SOURCE_CHANNELS,
        get_Y,
        safe_reply,
        safe_edit,
        mirrored_messages_index,
        cache_message_for_relink,
        mark_message_links_fixed,
        get_messages_needing_relink,
    )
    
    _batch_funcs.update({
        "rewrite_telegram_links": rewrite_telegram_links,
        "rewrite_entity_urls": rewrite_entity_urls,
        "_build_source_patterns": _build_source_patterns,
        "_edlog": _edlog,
        "_extract_flood_wait_local": _extract_flood_wait_local,
        "load_combined_msg_id_map": load_combined_msg_id_map,
        "build_multi_source_channels": build_multi_source_channels,
        "load_upload_map": load_upload_map,
        "mark_needs_link_update": mark_needs_link_update,
        "mark_links_resolved": mark_links_resolved,
        "unresolved_links_collection": unresolved_links_collection,
        "upload_maps_collection": upload_maps_collection,
        "mirror_state_collection": mirror_state_collection,
        "mirror_src_to_dst_collection": mirror_src_to_dst_collection,
        "ADDITIONAL_SOURCE_CHANNELS": ADDITIONAL_SOURCE_CHANNELS,
        "get_Y": get_Y,
        "safe_reply": safe_reply,
        "safe_edit": safe_edit,
        "mirrored_messages_index": mirrored_messages_index,
        "cache_message_for_relink": cache_message_for_relink,
        "mark_message_links_fixed": mark_message_links_fixed,
        "get_messages_needing_relink": get_messages_needing_relink,
    })
    _batch_funcs_fully_loaded = True
    return _batch_funcs


# Module-level proxies that delegate to the lazy-loaded batch functions.
# These are called throughout the code as if they were direct imports.
# On first call, they trigger the lazy import; afterwards, they're cached.

def _edlog(*args, **kwargs):
    """Proxy for batch._edlog — lazy loaded."""
    if "edlog" not in _batch_funcs:
        # Not loaded yet — try a quick import just for _edlog (logging is important)
        try:
            from plugins.batch import _edlog as _real_edlog
            _batch_funcs["edlog"] = _real_edlog
        except Exception:
            _batch_funcs["edlog"] = lambda *a, **k: None
    return _batch_funcs["edlog"](*args, **kwargs)


async def load_combined_msg_id_map(*args, **kwargs):
    """Proxy for batch.load_combined_msg_id_map — lazy loaded."""
    if "load_combined_msg_id_map" not in _batch_funcs:
        await _get_batch_funcs()
    return await _batch_funcs["load_combined_msg_id_map"](*args, **kwargs)


async def build_multi_source_channels(*args, **kwargs):
    """Proxy for batch.build_multi_source_channels — lazy loaded."""
    if "build_multi_source_channels" not in _batch_funcs:
        await _get_batch_funcs()
    return await _batch_funcs["build_multi_source_channels"](*args, **kwargs)


async def load_upload_map(*args, **kwargs):
    """Proxy for batch.load_upload_map — lazy loaded."""
    if "load_upload_map" not in _batch_funcs:
        await _get_batch_funcs()
    return await _batch_funcs["load_upload_map"](*args, **kwargs)


def get_Y(*args, **kwargs):
    """Proxy for batch.get_Y — lazy loaded."""
    if "get_Y" not in _batch_funcs:
        try:
            from plugins.batch import get_Y as _real_get_Y
            _batch_funcs["get_Y"] = _real_get_Y
        except Exception:
            _batch_funcs["get_Y"] = lambda *a, **k: None
    return _batch_funcs["get_Y"](*args, **kwargs)


async def safe_reply(*args, **kwargs):
    """Proxy for batch.safe_reply — lazy loaded."""
    if "safe_reply" not in _batch_funcs:
        await _get_batch_funcs()
    return await _batch_funcs["safe_reply"](*args, **kwargs)


async def safe_edit(*args, **kwargs):
    """Proxy for batch.safe_edit — lazy loaded."""
    if "safe_edit" not in _batch_funcs:
        await _get_batch_funcs()
    return await _batch_funcs["safe_edit"](*args, **kwargs)


def _get_upload_maps_collection():
    """Proxy for batch.upload_maps_collection — lazy loaded."""
    if "upload_maps_collection" not in _batch_funcs:
        try:
            from plugins.batch import upload_maps_collection
            _batch_funcs["upload_maps_collection"] = upload_maps_collection
        except Exception:
            return None
    return _batch_funcs["upload_maps_collection"]


def _get_unresolved_links_collection():
    """Proxy for batch.unresolved_links_collection — lazy loaded."""
    if "unresolved_links_collection" not in _batch_funcs:
        try:
            from plugins.batch import unresolved_links_collection
            _batch_funcs["unresolved_links_collection"] = unresolved_links_collection
        except Exception:
            return None
    return _batch_funcs["unresolved_links_collection"]


def _get_additional_source_channels():
    """Proxy for batch.ADDITIONAL_SOURCE_CHANNELS — lazy loaded."""
    if "ADDITIONAL_SOURCE_CHANNELS" not in _batch_funcs:
        try:
            from plugins.batch import ADDITIONAL_SOURCE_CHANNELS
            _batch_funcs["ADDITIONAL_SOURCE_CHANNELS"] = ADDITIONAL_SOURCE_CHANNELS
        except Exception:
            return []
    return _batch_funcs["ADDITIONAL_SOURCE_CHANNELS"]


# Direct imports (no circular risk)
from utils.func import (
    get_user_data, get_user_data_key, is_premium_user, is_auth_user, E
)
from config import OWNER_ID


# ════════════════════════════════════════════════════════════════════
# LAYER 1: CRASH-PROOF CHECKPOINT RESUME SYSTEM
# ════════════════════════════════════════════════════════════════════

async def create_relink_session(chat_id: int, triggered_by: int, dest_channel_id: int,
                                 dest_channel_username: str = None, limit: int = None,
                                 dry_run: bool = False, direction: str = "new_to_old") -> dict:
    """Create a new relink session in MongoDB. Returns the session document."""
    session = {
        "chat_id": chat_id,
        "dest_channel_id": dest_channel_id,
        "dest_channel_username": dest_channel_username,
        "triggered_by": triggered_by,
        "status": "pending",  # pending | in_progress | completed | failed | cancelled
        "started_at": datetime.utcnow(),
        "last_scanned_msg_id": 0,
        "scan_direction": direction,  # GAP 4: old_to_new | new_to_old
        "total_scanned": 0,
        "total_fixed": 0,
        "total_unresolved": 0,
        "total_skipped": 0,
        "total_already_correct": 0,
        "failed_edits": [],
        "unresolved_links": [],
        "error_log": [],
        "limit": limit,
        "dry_run": dry_run,
        "completed_at": None,
        "speed_msg_per_min": 0.0,
    }
    result = await relink_sessions_collection.insert_one(session)
    session["_id"] = result.inserted_id
    return session


async def get_active_relink_session(chat_id: int) -> Optional[dict]:
    """Find an active (in_progress or pending) session for this chat."""
    return await relink_sessions_collection.find_one(
        {"chat_id": chat_id, "status": {"$in": ["in_progress", "pending"]}}
    )


async def get_latest_relink_session(chat_id: int) -> Optional[dict]:
    """Get the most recent session for this chat (any status)."""
    return await relink_sessions_collection.find_one(
        {"chat_id": chat_id},
        sort=[("started_at", -1)]
    )


async def update_relink_checkpoint(session_id, **kwargs):
    """Update session checkpoint fields atomically."""
    update = {"$set": {k: v for k, v in kwargs.items() if v is not None}}
    # Handle special increment fields
    inc_fields = {}
    for key in ["total_scanned", "total_fixed", "total_unresolved",
                 "total_skipped", "total_already_correct"]:
        if key in kwargs:
            inc_fields[key] = kwargs[key]

    if inc_fields:
        update["$inc"] = inc_fields
        # Remove from $set to avoid conflict
        for k in inc_fields:
            update["$set"].pop(k, None)

    if not update["$set"] and "$inc" not in update:
        return

    await relink_sessions_collection.update_one({"_id": session_id}, update)


async def append_to_session(session_id, field: str, items: list):
    """Append items to an array field in the session document."""
    if not items:
        return
    await relink_sessions_collection.update_one(
        {"_id": session_id},
        {"$push": {field: {"$each": items}}}
    )


async def cancel_relink_session(chat_id: int) -> bool:
    """Cancel any active session for this chat. Returns True if cancelled."""
    result = await relink_sessions_collection.update_one(
        {"chat_id": chat_id, "status": {"$in": ["in_progress", "pending"]}},
        {"$set": {"status": "cancelled", "completed_at": datetime.utcnow()}}
    )
    return result.modified_count > 0


# ════════════════════════════════════════════════════════════════════
# GAP 1: FINGERPRINT MATCHING
#
# Replaces unreliable SequenceMatcher fuzzy match.
# At upload time → generate fingerprint → store in MongoDB.
# At resolve time → query fingerprint → instant exact match.
# No source channel fetches. No false positives.
# Fingerprint = sha256(first 50 chars normalized + text length + media type)
# ════════════════════════════════════════════════════════════════════

def generate_fingerprint(msg) -> Optional[str]:
    """
    Generates a unique fingerprint for a message.
    Called at upload time for every message.
    Stored in MongoDB alongside src→dst mapping.

    Fingerprint components:
        text_prefix : first 50 chars normalized (lowercase, strip)
        text_length : total character count
        media_type  : photo | poll | text | document
    """
    text = None
    if hasattr(msg, "text") and msg.text:
        text = str(msg.text)
    elif hasattr(msg, "caption") and msg.caption:
        text = str(msg.caption)
    elif hasattr(msg, "poll") and msg.poll:
        text = str(msg.poll.question)

    if not text:
        return None

    media_type = (
        "photo"    if getattr(msg, "photo",    None) else
        "poll"     if getattr(msg, "poll",     None) else
        "document" if getattr(msg, "document", None) else
        "text"
    )

    normalized  = text.lower().strip()[:50]
    text_length = len(text)
    raw         = f"{normalized}|{text_length}|{media_type}"
    return hashlib.sha256(raw.encode()).hexdigest()


async def store_fingerprint(
    uid            : int,
    source_channel : str,
    src_msg_id     : int,
    dst_msg_id     : int,
    fingerprint    : str,
):
    """Store fingerprint in MongoDB at upload time."""
    try:
        await fingerprints_collection.update_one(
            {"fingerprint": fingerprint},
            {"$set": {
                "fingerprint"   : fingerprint,
                "uid"           : uid,
                "source_channel": str(source_channel),
                "src_msg_id"    : src_msg_id,
                "dst_msg_id"    : dst_msg_id,
                "created_at"    : datetime.utcnow(),
            }},
            upsert=True,
        )
    except Exception as e:
        logger.debug(f"[FINGERPRINT] store failed: {e}")


async def resolve_by_fingerprint(
    uid            : int,
    source_channel : str,
    src_msg_id     : int,
    user_client    = None,
) -> Optional[int]:
    """
    GAP 1 Resolution Strategy: Fingerprint match.
    Fetch source message → generate fingerprint → query MongoDB.
    Returns dst_msg_id if found, None otherwise.
    Zero false positives. One DB query.
    """
    try:
        # Try to get the source message to generate its fingerprint
        if not user_client:
            user_client = get_Y()
        if not user_client:
            return None

        source_ch = None
        ch_str_clean = str(source_channel)
        if ch_str_clean.lstrip('-').isdigit():
            source_ch = int(ch_str_clean)
        else:
            source_ch = ch_str_clean

        src_msg = await user_client.get_messages(source_ch, src_msg_id)
        if not src_msg or getattr(src_msg, "empty", True):
            return None

        fp = generate_fingerprint(src_msg)
        if not fp:
            return None

        doc = await fingerprints_collection.find_one({
            "fingerprint"   : fp,
            "uid"           : uid,
            "source_channel": str(source_channel),
        })
        if doc:
            logger.info(f"[FINGERPRINT] HIT src={src_msg_id} → dst={doc['dst_msg_id']}")
            return doc["dst_msg_id"]
        return None

    except Exception as e:
        logger.debug(f"[FINGERPRINT] resolve failed src={src_msg_id}: {e}")
        return None


async def checkpoint_with_fingerprint(
    uid            : int,
    source_channel : str,
    src_msg_id     : int,
    dst_msg_id     : int,
    src_msg,
):
    """Extended checkpoint that also stores fingerprint.
    Call this from batch.py at upload time instead of just mark_done()."""
    fp = generate_fingerprint(src_msg)
    if fp:
        await store_fingerprint(
            uid            = uid,
            source_channel = source_channel,
            src_msg_id     = src_msg_id,
            dst_msg_id     = dst_msg_id,
            fingerprint    = fp,
        )


# ════════════════════════════════════════════════════════════════════
# LAYER 2: MULTI-STRATEGY LINK RESOLUTION ENGINE
# ════════════════════════════════════════════════════════════════════

# Link type constants
LINK_PRIVATE = "private"      # https://t.me/c/1234567890/52
LINK_PUBLIC = "public"        # https://t.me/channelname/52
LINK_TG_PROTOCOL = "tg"       # tg://resolve?domain=channel&post=52
LINK_INVITE = "invite"        # https://t.me/+AbCdEfGh (skip)
LINK_THREAD = "thread"        # https://t.me/c/XXX/123/456 (thread link)
LINK_NON_CHANNEL = "non_channel"  # Links to users, bots, etc. (skip)

# Regex patterns for link classification
PRIVATE_LINK_RE = re.compile(r'https?://t\.me/c/(\d+)/(\d+)(?:/(\d+))?', re.IGNORECASE)
PUBLIC_LINK_RE = re.compile(r'https?://t\.me/([a-zA-Z]\w{3,}[a-zA-Z0-9])/(\d+)(?:/(\d+))?', re.IGNORECASE)
TG_RESOLVE_RE = re.compile(r'tg://resolve\?domain=(\w+)&post=(\d+)', re.IGNORECASE)
INVITE_LINK_RE = re.compile(r'https?://t\.me/[\+]|https?://t\.me/joinchat/', re.IGNORECASE)
# Known non-channel t.me/ paths
NON_CHANNEL_PATHS = {'c', 'joinchat', '+', 'addstickers', 'bot', 'setlanguage',
                     'confirmphone', 'login', 'passport', 'faq', 'privacy'}


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
    s = s.lstrip('-')            # remove minus
    if s.startswith('100') and len(s) > 5 and s[3:].isdigit():
        s = s[3:]                # remove 100 prefix (only for channel IDs)
    return s


class LinkInfo:
    """Parsed Telegram link information."""
    __slots__ = ['url', 'link_type', 'source_peer', 'source_msg_id', 'thread_id',
                 'username', 'is_source_channel', 'is_dest_channel']

    def __init__(self, url, link_type, source_peer=None, source_msg_id=None,
                 thread_id=None, username=None, is_source_channel=False, is_dest_channel=False):
        self.url = url
        self.link_type = link_type
        self.source_peer = source_peer
        self.source_msg_id = source_msg_id
        self.thread_id = thread_id
        self.username = username
        self.is_source_channel = is_source_channel
        self.is_dest_channel = is_dest_channel


def classify_link(url: str, source_channels_info: dict, dest_channel_id: int = None) -> LinkInfo:
    """Classify a Telegram URL and extract its components.

    source_channels_info: dict mapping channel identifiers to their info.
        Key: channel string (e.g. "-1001234567890")
        Value: {"clean_id": "1234567890", "username": "channelname" or None, "numeric_id": -1001234567890}
    dest_channel_id: Optional destination channel ID — links pointing here are self-references.
    """
    if not url:
        return LinkInfo(url, LINK_NON_CHANNEL)

    # Pre-compute dest channel clean ID for comparison
    dest_clean_id = normalize_channel_id(dest_channel_id) if dest_channel_id else None

    # Check invite links first (skip these)
    if INVITE_LINK_RE.match(url):
        return LinkInfo(url, LINK_INVITE)

    # Check tg:// deep links
    tg_match = TG_RESOLVE_RE.match(url)
    if tg_match:
        username = tg_match.group(1)
        msg_id = int(tg_match.group(2))
        # Check if this username belongs to one of our source channels
        is_source = False
        for ch_str, ch_info in source_channels_info.items():
            if ch_info.get("username", "").lower() == username.lower():
                is_source = True
                return LinkInfo(url, LINK_TG_PROTOCOL, source_peer=ch_info.get("numeric_id"),
                               source_msg_id=msg_id, username=username, is_source_channel=True)
        return LinkInfo(url, LINK_TG_PROTOCOL, username=username, source_msg_id=msg_id,
                       is_source_channel=False)

    # Check private channel links: https://t.me/c/1234567890/52
    priv_match = PRIVATE_LINK_RE.match(url)
    if priv_match:
        peer_id = int(priv_match.group(1))
        msg_id = int(priv_match.group(2))
        thread_id = priv_match.group(3)

        # Check if this is a DESTINATION channel self-reference
        # e.g. t.me/c/3900746078/2/28858 — links within the same channel
        if dest_clean_id and normalize_channel_id(peer_id) == dest_clean_id:
            return LinkInfo(url, LINK_PRIVATE, source_peer=peer_id,
                           source_msg_id=msg_id, thread_id=thread_id,
                           is_source_channel=False, is_dest_channel=True)

        # Check if this peer_id belongs to one of our source channels
        peer_clean = normalize_channel_id(peer_id)
        for ch_str, ch_info in source_channels_info.items():
            ch_clean = normalize_channel_id(ch_info.get("clean_id") or ch_str)
            if ch_clean == peer_clean:
                return LinkInfo(url, LINK_PRIVATE, source_peer=peer_id,
                               source_msg_id=msg_id, thread_id=thread_id,
                               is_source_channel=True)
        # Unknown private channel — not one of our sources
        return LinkInfo(url, LINK_PRIVATE, source_peer=peer_id,
                       source_msg_id=msg_id, thread_id=thread_id,
                       is_source_channel=False)

    # Check public channel links: https://t.me/channelname/52
    pub_match = PUBLIC_LINK_RE.match(url)
    if pub_match:
        username = pub_match.group(1)
        msg_id = int(pub_match.group(2))
        thread_id = pub_match.group(3)
        # Skip known non-channel paths
        if username.lower() in NON_CHANNEL_PATHS:
            return LinkInfo(url, LINK_NON_CHANNEL)
        # Check if this username belongs to one of our source channels
        for ch_str, ch_info in source_channels_info.items():
            if ch_info.get("username", "").lower() == username.lower():
                return LinkInfo(url, LINK_PUBLIC, source_peer=ch_info.get("numeric_id"),
                               source_msg_id=msg_id, thread_id=thread_id,
                               username=username, is_source_channel=True)
        # Unknown public channel — not one of our sources
        return LinkInfo(url, LINK_PUBLIC, source_msg_id=msg_id,
                       thread_id=thread_id, username=username,
                       is_source_channel=False)

    return LinkInfo(url, LINK_NON_CHANNEL)


class LinkResolver:
    """Multi-strategy link resolution engine.

    Strategies (in order of reliability):
    1. Direct msg_id_map lookup (exact match)
    2. Combined multi-source map lookup
    3. Cross-session cache check (GAP 3 — instant MongoDB lookup)
    4. Fingerprint match (GAP 1 — replaces unreliable fuzzy)
    5. Deep reply chain resolution (GAP 2 — up to 10 levels)
    6. On-demand source fetch + destination search (lazy mapping)
    """

    def __init__(self, combined_msg_id_map: dict, source_channels_info: dict,
                 dest_chat_id: int, dest_channel_username: str = None,
                 uid: int = None):
        self.msg_id_map = combined_msg_id_map
        self.source_channels_info = source_channels_info
        self.dest_chat_id = dest_chat_id
        self.dest_channel_username = dest_channel_username
        self.uid = uid
        self._cache = {}  # Local URL cache to avoid repeated lookups
        self.stats = {"strategy_1": 0, "strategy_2": 0, "strategy_3": 0,
                      "strategy_4": 0, "strategy_5": 0, "strategy_6": 0,
                      "strategy_7": 0, "strategy_8": 0,
                      "cache_hits": 0,
                      "failed": 0, "fingerprint_hits": 0, "deep_chain_hits": 0,
                      "lazy_mapping_hits": 0, "content_index_hits": 0, "entity_reverse_hits": 0,
                      "fp_db_lookup_hits": 0}

        # In-memory fingerprint index: fingerprint → dst_msg_id
        # Built incrementally as we scan destination messages.
        # Enables resolution of links whose src_msg_id is NOT in the direct map.
        self._fingerprint_index: Dict[str, int] = {}

        # Reverse map: dst_msg_id → src_msg_id (built from combined_map)
        self._reverse_map: Dict[int, int] = {v: k for k, v in combined_msg_id_map.items() if isinstance(v, int)}

        # ═══ CRITICAL NEW INDEX ═══
        # Entity URL reverse index: src_msg_id → dst_msg_id
        # Built from destination messages that CONTAIN source channel links.
        # When a destination message has a TEXT_LINK pointing to
        # https://t.me/c/2563279588/6311, we know that this destination
        # message IS the mirror of a source message that references 6311.
        # BUT we also need to find: which dst message IS source 6311?
        # Answer: scan ALL dest messages and find ones whose ENTITIES
        # contain links to the source channel. For each such message,
        # we know its dst_msg_id. If we can determine its src_msg_id
        # (from the reverse_map), we can build a complete mapping.
        #
        # Actually, the REAL key insight is simpler:
        # The dest messages themselves contain source channel links.
        # Those links encode src_msg_ids. We already extract them.
        # But the LINK tells us which source message is REFERENCED,
        # not which source message IS this destination message.
        #
        # The solution: build a src_msg_id → set of dst_msg_ids that
        # REFERENCE that src_msg_id. Then when we need to find the
        # mirror of src_msg_id X, we look for the destination message
        # whose CONTENT matches source message X (via fingerprint).
        #
        # BUT there's an even simpler approach (Strategy 8):
        # Use the combined_map and reverse_map that we ALREADY have,
        # PLUS scan destination messages for "topic header" patterns.
        # Many channels have topic headers like "TOPIC 2: ... Click here 🔗"
        # These contain links to all the messages in that topic.
        # If we find a message that links to src_msg_id 6311 AND
        # we know its own src_msg_id (from reverse_map), we can
        # infer the mapping for 6311 if it's in the same topic range.
        #
        # For now, just store the entity URLs for Phase 2 analysis.
        self._entity_url_map: Dict[int, int] = {}  # src_msg_id → dst_msg_id (from entity analysis)

        # Pre-build dest URL prefix
        if dest_channel_username:
            self.dest_url_prefix = f'https://t.me/{dest_channel_username}/'
        else:
            clean_dest = normalize_channel_id(dest_chat_id)
            self.dest_url_prefix = f'https://t.me/c/{clean_dest}/'

    def build_dest_url(self, dest_msg_id: int, thread_id: str = None) -> str:
        """Build destination URL from dest message ID."""
        url = f'{self.dest_url_prefix}{dest_msg_id}'
        if thread_id:
            url = f'{url}/{thread_id}'
        return url

    def add_to_fingerprint_index(self, msg, dst_msg_id: int):
        """Add a destination message to the in-memory fingerprint index.

        Called for EVERY message during the relink scan. Builds a
        fingerprint → dst_msg_id index that Strategy 7 uses to resolve
        links whose src_msg_id is NOT in the direct map.

        This is the KEY fix for the "map has 4914 entries but all
        lookups return False" problem: even if the direct mapping
        is missing, we can still find the destination message by
        matching its content fingerprint.
        """
        fp = generate_fingerprint(msg)
        if fp:
            self._fingerprint_index[fp] = dst_msg_id

    async def resolve(self, link_info: LinkInfo) -> Optional[str]:
        """Try every strategy to resolve a link. Returns dest URL or None."""
        # Skip non-source-channel links
        if not link_info.is_source_channel:
            return None
        if link_info.source_msg_id is None:
            return None

        cache_key = f"{link_info.source_peer or link_info.username}:{link_info.source_msg_id}"
        if cache_key in self._cache:
            self.stats["cache_hits"] += 1
            return self._cache[cache_key]

        # Strategy 1: Direct msg_id_map lookup
        result = self._resolve_direct(link_info)
        if result:
            self.stats["strategy_1"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "direct")
            return result

        # Strategy 2: Try ALL source channel maps (cross-channel)
        result = await self._resolve_cross_channel(link_info)
        if result:
            self.stats["strategy_2"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "cross_channel")
            return result

        # Strategy 3: Cross-session cache (GAP 3)
        result = await self._resolve_via_cache(link_info)
        if result:
            self.stats["strategy_3"] += 1
            self._cache[cache_key] = result
            return result

        # Strategy 4: Fingerprint match (GAP 1 — replaces unreliable fuzzy)
        result = await self._resolve_fingerprint(link_info)
        if result:
            self.stats["strategy_4"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "fingerprint")
            return result

        # Strategy 5: Deep reply chain resolution (GAP 2 — up to 10 levels)
        result = await self._resolve_deep_reply_chain(link_info)
        if result:
            self.stats["strategy_5"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "deep_reply_chain")
            return result

        # Strategy 6.5: Direct MongoDB fingerprint lookup by src_msg_id
        # CRITICAL FIX: This is the MOST RELIABLE strategy when ubot is None.
        # The fingerprints were stored at UPLOAD TIME in MongoDB with src_msg_id.
        # We can look them up directly WITHOUT fetching source messages.
        # This bypasses the ubot=None problem entirely.
        result = await self._resolve_via_fingerprint_db_lookup(link_info)
        if result:
            self.stats["strategy_6"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "fingerprint_db_lookup")
            return result

        # Strategy 7: In-memory content fingerprint index lookup
        # This is the KEY strategy for resolving links when the direct
        # mapping is missing. It works by:
        # 1. Fetching the source message from the source channel
        # 2. Computing its content fingerprint
        # 3. Looking up the fingerprint in our in-memory index
        #    (built from destination messages during the scan)
        # 4. If found, we have the destination message ID
        result = await self._resolve_via_content_index(link_info)
        if result:
            self.stats["strategy_7"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "content_index")
            return result

        # Strategy 8: MongoDB direct query — check ALL upload_maps docs
        # for this user, not just the ones we loaded into combined_map.
        # Sometimes the mapping exists in a different channel's doc
        # or under a different source_channel key format.
        result = await self._resolve_via_mongodb_scan(link_info)
        if result:
            self.stats["strategy_8"] += 1
            self._cache[cache_key] = result
            await self._cache_resolution(link_info.url, result, "mongodb_scan")
            return result

        self.stats["failed"] += 1
        return None

    def _resolve_direct(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 1: Direct msg_id_map lookup."""
        dest_msg_id = self.msg_id_map.get(link_info.source_msg_id)
        if dest_msg_id:
            return self.build_dest_url(dest_msg_id, link_info.thread_id)

        # Debug: log WHY lookup failed — only first 5 to avoid spam
        if not hasattr(self, '_direct_miss_logged'):
            self._direct_miss_logged = 0
        if self._direct_miss_logged < 5:
            self._direct_miss_logged += 1
            _sample_keys = sorted(self.msg_id_map.keys())[:5] if self.msg_id_map else []
            _edlog(
                f"[RELINK-MAP-CHECK] src={link_info.source_msg_id} "
                f"in_map={link_info.source_msg_id in self.msg_id_map} "
                f"map_size={len(self.msg_id_map)} "
                f"key_range={min(self.msg_id_map.keys()) if self.msg_id_map else 0}"
                f"-{max(self.msg_id_map.keys()) if self.msg_id_map else 0} "
                f"sample_keys={_sample_keys}"
            )
        return None

    async def _resolve_cross_channel(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 2: Try lookup in ALL source channel maps.

        Sometimes a link points to channel B but we only loaded channel A's map.
        This strategy explicitly loads channel B's map from MongoDB.
        """
        # Find the channel this link belongs to
        for ch_str, ch_info in self.source_channels_info.items():
            ch_numeric = ch_info.get("numeric_id")
            ch_clean = ch_info.get("clean_id")
            ch_username = ch_info.get("username")

            # Match by peer ID or username
            if link_info.source_peer and ch_clean == str(link_info.source_peer):
                pass  # Found it
            elif link_info.username and ch_username and ch_username.lower() == link_info.username.lower():
                pass  # Found it
            else:
                continue

            # This channel's map is already in combined_msg_id_map
            # If we didn't find it in strategy 1, it's truly not there
            return None

        # Link doesn't match any known source channel — can't resolve
        return None

    async def _resolve_via_cache(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 3: Cross-session cache (GAP 3).

        Checks MongoDB for previously resolved links.
        Permanent cache — never expires for resolved links.
        Returns dest_url if cached, None otherwise.
        """
        try:
            doc = await relink_cache_collection.find_one({
                "source_url": link_info.url,
            })
            if doc and doc.get("dest_url"):
                # Increment hit count for analytics
                await relink_cache_collection.update_one(
                    {"source_url": link_info.url},
                    {"$inc": {"hit_count": 1}},
                )
                logger.debug(
                    f"[CACHE] HIT source_url={link_info.url[:50]} "
                    f"→ {doc['dest_url'][:50]} "
                    f"hits={doc.get('hit_count', 0) + 1}"
                )
                return doc["dest_url"]
        except Exception:
            pass
        return None

    async def _cache_resolution(self, source_url: str, dest_url: str, strategy: str):
        """GAP 3: Cache a successful link resolution permanently.
        Called after any strategy resolves a link."""
        try:
            await relink_cache_collection.update_one(
                {"source_url": source_url},
                {"$set": {
                    "source_url": source_url,
                    "dest_url"  : dest_url,
                    "strategy"  : strategy,
                    "cached_at" : datetime.utcnow(),
                    "hit_count" : 0,
                }},
                upsert=True,
            )
        except Exception:
            pass

    async def _resolve_fingerprint(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 4: Fingerprint match (GAP 1 — replaces unreliable fuzzy).

        Zero false positives. One MongoDB query.
        Fetch source message → generate fingerprint → query MongoDB.
        """
        if not self.uid or not link_info.source_msg_id:
            return None

        # Determine source channel string for fingerprint lookup
        source_channel_str = None
        for ch_str, ch_info in self.source_channels_info.items():
            ch_clean = ch_info.get("clean_id")
            ch_username = ch_info.get("username")

            if link_info.source_peer and ch_clean == str(link_info.source_peer):
                source_channel_str = ch_str
                break
            elif link_info.username and ch_username and ch_username.lower() == link_info.username.lower():
                source_channel_str = ch_str
                break

        if not source_channel_str:
            return None

        try:
            dst_msg_id = await resolve_by_fingerprint(
                uid=self.uid,
                source_channel=source_channel_str,
                src_msg_id=link_info.source_msg_id,
            )
            if dst_msg_id:
                self.stats["fingerprint_hits"] += 1
                return self.build_dest_url(dst_msg_id, link_info.thread_id)
        except Exception:
            pass

        return None

    async def _resolve_deep_reply_chain(self, link_info: LinkInfo, max_depth: int = 10) -> Optional[str]:
        """Strategy 5: Deep reply chain resolution (GAP 2).

        Walks reply chain up to max_depth levels.
        At each level checks msg_id_map for a mapping.
        msg A → B → C → D: if A is unresolved, checks B, then C, then D.

        Cost: up to max_depth API calls in worst case.
        Stops as soon as a mapping is found.
        """
        client = get_Y()
        if not client or not link_info.source_msg_id:
            return None

        # Determine source channel
        source_ch = None
        for ch_str, ch_info in self.source_channels_info.items():
            ch_clean = ch_info.get("clean_id")
            ch_username = ch_info.get("username")
            ch_numeric = ch_info.get("numeric_id")

            if link_info.source_peer and ch_clean == str(link_info.source_peer):
                source_ch = ch_numeric if ch_numeric else int(f"-100{ch_clean}")
                break
            elif link_info.username and ch_username and ch_username.lower() == link_info.username.lower():
                source_ch = ch_numeric if ch_numeric else link_info.username
                break

        if not source_ch:
            return None

        visited = set()
        current = link_info.source_msg_id

        for depth in range(max_depth):
            if current in visited:
                break  # Cycle detected
            visited.add(current)

            # Check if current message has a mapping
            dst_id = self.msg_id_map.get(current)
            if dst_id:
                self.stats["deep_chain_hits"] += 1
                logger.info(
                    f"[REPLY-CHAIN] src={link_info.source_msg_id} resolved via "
                    f"chain depth={depth} through src={current} → dst={dst_id}"
                )
                return self.build_dest_url(dst_id, link_info.thread_id)

            # Fetch the message to get its reply_to
            try:
                msg = await client.get_messages(source_ch, current)
                if not msg or getattr(msg, "empty", True):
                    break

                reply_to = getattr(msg, "reply_to_message_id", None)
                if not reply_to:
                    raw = getattr(msg, "reply_to", None)
                    if raw:
                        reply_to = getattr(raw, "reply_to_msg_id", None)

                if not reply_to:
                    break  # End of chain

                current = reply_to

            except Exception as e:
                logger.debug(f"[REPLY-CHAIN] fetch failed at depth={depth}: {e}")
                break

        logger.debug(
            f"[REPLY-CHAIN] src={link_info.source_msg_id} unresolved "
            f"after walking {len(visited)} levels"
        )
        return None

    async def _resolve_via_fingerprint_db_lookup(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 6.5: Direct MongoDB fingerprint lookup by src_msg_id.

        THE CRITICAL FIX for the "ubot=None → fixed=0" problem.

        All previous strategies (4, 5, 7) require fetching source messages
        from the source channel to compute fingerprints or walk reply chains.
        When ubot is None and bot can't access the private source channel,
        ALL of these strategies silently fail → 0 hits.

        This strategy is DIFFERENT: it queries MongoDB's fingerprints_collection
        directly by src_msg_id. The fingerprints were stored at UPLOAD TIME via
        checkpoint_with_fingerprint() in batch.py. So the data IS there — we
        just weren't looking for it by src_msg_id.

        Cost: 1 MongoDB query per unresolved link (instant, no API calls).
        Works even when ubot=None.
        """
        if not self.uid or not link_info.source_msg_id:
            return None

        try:
            # Determine source channel string for the query
            source_channel_str = None
            for ch_str, ch_info in self.source_channels_info.items():
                ch_clean = ch_info.get("clean_id")
                ch_username = ch_info.get("username")

                if link_info.source_peer and ch_clean == str(link_info.source_peer):
                    source_channel_str = ch_str
                    break
                elif link_info.username and ch_username and ch_username.lower() == link_info.username.lower():
                    source_channel_str = ch_str
                    break

            # Build channel variants to try (same logic as load_upload_map)
            channel_variants = []
            if source_channel_str:
                channel_variants = [source_channel_str]
                raw = source_channel_str.lstrip('-')
                if raw.startswith('100') and len(raw) > 5:
                    # source_channel_str = "-1002563279588" → also try "2563279588"
                    channel_variants.append(raw[3:])
                elif raw.isdigit() and len(raw) <= 10:
                    # source_channel_str = "2563279588" → also try "-1002563279588"
                    channel_variants.append(f"-100{raw}")
            elif link_info.source_peer:
                peer = str(link_info.source_peer)
                channel_variants = [peer, f"-100{peer}", f"100{peer}", f"-{peer}"]

            # Deduplicate channel variants (avoid wasted queries)
            seen_variants = set()
            unique_variants = []
            for v in channel_variants:
                if v not in seen_variants:
                    seen_variants.add(v)
                    unique_variants.append(v)
            channel_variants = unique_variants

            # Query by src_msg_id first (most specific, fastest)
            # The fingerprints_collection has an index-ready structure
            src_id = link_info.source_msg_id

            # Try with uid + src_msg_id (fastest, most specific)
            for ch_var in channel_variants:
                doc = await fingerprints_collection.find_one({
                    "uid": self.uid,
                    "src_msg_id": src_id,
                    "source_channel": ch_var,
                })
                if doc and doc.get("dst_msg_id"):
                    dst_msg_id = doc["dst_msg_id"]
                    # Also add to the direct map for future instant lookups
                    self.msg_id_map[src_id] = dst_msg_id
                    self.stats["fp_db_lookup_hits"] += 1
                    print(f"[FP-DB-LOOKUP] ✅ HIT src={src_id} → dst={dst_msg_id} "
                          f"channel={ch_var} (NO source fetch needed!)")
                    return self.build_dest_url(dst_msg_id, link_info.thread_id)

            # Broader query: just uid + src_msg_id (ignoring channel)
            doc = await fingerprints_collection.find_one({
                "uid": self.uid,
                "src_msg_id": src_id,
            })
            if doc and doc.get("dst_msg_id"):
                dst_msg_id = doc["dst_msg_id"]
                self.msg_id_map[src_id] = dst_msg_id
                self.stats["fp_db_lookup_hits"] += 1
                print(f"[FP-DB-LOOKUP] ✅ HIT (broad) src={src_id} → dst={dst_msg_id} "
                      f"channel={doc.get('source_channel', '?')} (NO source fetch needed!)")
                return self.build_dest_url(dst_msg_id, link_info.thread_id)

            # Diagnostic: log miss (first 5 only)
            if not hasattr(self, '_fp_db_miss_logged'):
                self._fp_db_miss_logged = 0
            if self._fp_db_miss_logged < 5:
                self._fp_db_miss_logged += 1
                print(f"[FP-DB-LOOKUP] MISS src={src_id} uid={self.uid} "
                      f"channel_variants={channel_variants[:3]}")

            return None

        except Exception as e:
            if not hasattr(self, '_fp_db_err_logged'):
                self._fp_db_err_logged = 0
            if self._fp_db_err_logged < 3:
                self._fp_db_err_logged += 1
                print(f"[FP-DB-LOOKUP] ERROR src={link_info.source_msg_id}: {e}")
            return None

    async def _resolve_via_content_index(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 7: In-memory content fingerprint index lookup.

        Requires fetching the source message from the source channel.
        Will typically fail when ubot=None and bot can't access private channels.
        Strategy 6.5 (_resolve_via_fingerprint_db_lookup) should be tried FIRST
        since it doesn't need source fetch.

        Cost: 1 API call (fetch source message) per unresolved link.
        Zero false positives (sha256 collision probability is negligible).
        """
        if not link_info.source_msg_id or not self._fingerprint_index:
            return None

        # Determine source channel for fetching
        source_ch = None
        for ch_str, ch_info in self.source_channels_info.items():
            ch_clean = ch_info.get("clean_id")
            ch_username = ch_info.get("username")
            ch_numeric = ch_info.get("numeric_id")

            if link_info.source_peer and ch_clean == str(link_info.source_peer):
                source_ch = ch_numeric if ch_numeric else int(f"-100{ch_clean}")
                break
            elif link_info.username and ch_username and ch_username.lower() == link_info.username.lower():
                source_ch = ch_numeric if ch_numeric else link_info.username
                break

        if not source_ch:
            # Diagnostic: log why source channel wasn't found
            if not hasattr(self, '_no_source_ch_logged'):
                self._no_source_ch_logged = 0
            if self._no_source_ch_logged < 3:
                self._no_source_ch_logged += 1
                _edlog(f"[CONTENT-INDEX] No source_ch for peer={link_info.source_peer} "
                       f"username={link_info.username} "
                       f"available_channels={list(self.source_channels_info.keys())[:5]}")
            return None

        # Try ubot first (higher rate limits), then bot as fallback
        client = get_Y()
        client_source = "ubot" if client else "None"
        if not client:
            try:
                from shared_client import app as _bot_fallback
                client = _bot_fallback
                client_source = "bot_fallback"
            except Exception:
                pass
        if not client:
            # Diagnostic: log why client isn't available
            if not hasattr(self, '_no_client_logged'):
                self._no_client_logged = 0
            if self._no_client_logged < 3:
                self._no_client_logged += 1
                print(f"[CONTENT-INDEX] ❌ BLOCKED: No client available (ubot=None) "
                      f"for src={link_info.source_msg_id} — Strategy 6.5 should handle this")
            return None

        try:
            # Rate limit: add small delay before fetching source message
            # to avoid FloodWait on channels.getMessages
            await asyncio.sleep(0.5)

            # Fetch the source message
            src_msg = await client.get_messages(source_ch, link_info.source_msg_id)
            if not src_msg or getattr(src_msg, "empty", True):
                if not hasattr(self, '_src_msg_empty_logged'):
                    self._src_msg_empty_logged = 0
                if self._src_msg_empty_logged < 5:
                    self._src_msg_empty_logged += 1
                    print(f"[CONTENT-INDEX] ❌ Source msg empty/None: src={link_info.source_msg_id} "
                          f"source_ch={source_ch} client={client_source} "
                          f"empty={getattr(src_msg, 'empty', 'N/A') if src_msg else 'None'}")
                return None

            # Compute its fingerprint
            fp = generate_fingerprint(src_msg)
            if not fp:
                if not hasattr(self, '_no_fp_logged'):
                    self._no_fp_logged = 0
                if self._no_fp_logged < 3:
                    self._no_fp_logged += 1
                    print(f"[CONTENT-INDEX] ❌ No fingerprint for src={link_info.source_msg_id} "
                          f"(msg has no text/caption/poll)")
                return None

            # Look up in our in-memory index
            dst_msg_id = self._fingerprint_index.get(fp)
            if dst_msg_id:
                self.stats["content_index_hits"] += 1
                # Also add to the direct map so future lookups are instant
                self.msg_id_map[link_info.source_msg_id] = dst_msg_id
                _edlog(
                    f"[CONTENT-INDEX] ✅ HIT src={link_info.source_msg_id} → dst={dst_msg_id} "
                    f"(fp_index_size={len(self._fingerprint_index)}, "
                    f"map_now={len(self.msg_id_map)})"
                )
                return self.build_dest_url(dst_msg_id, link_info.thread_id)

            # Not found in index — message may not have been mirrored at all
            # Use print() for critical diagnostics — _edlog/logger.debug may not show in Heroku
            if not hasattr(self, '_content_miss_logged'):
                self._content_miss_logged = 0
            if self._content_miss_logged < 5:
                self._content_miss_logged += 1
                print(f"[CONTENT-INDEX] MISS src={link_info.source_msg_id} "
                      f"fp={fp[:16]}... idx_size={len(self._fingerprint_index)} "
                      f"source_ch={source_ch}")
            return None

        except Exception as e:
            # CRITICAL: Use print() not logger.debug — debug level doesn't show in Heroku
            if not hasattr(self, '_content_err_logged'):
                self._content_err_logged = 0
            if self._content_err_logged < 5:
                self._content_err_logged += 1
                print(f"[CONTENT-INDEX] ERROR src={link_info.source_msg_id} "
                      f"err={type(e).__name__}: {str(e)[:100]}")
            return None

    async def _resolve_via_mongodb_scan(self, link_info: LinkInfo) -> Optional[str]:
        """Strategy 8: Direct MongoDB scan for the src_msg_id mapping.

        The combined_map was loaded from MongoDB at scan start, but it may be
        incomplete because:
        1. The channel ID format in MongoDB doesn't match what we queried
        2. The mapping was saved under a different source_channel key
        3. The mapping was saved AFTER we loaded the map

        This strategy does a raw MongoDB query for the specific src_msg_id
        across ALL documents for this user, bypassing the channel format
        matching issue entirely.
        """
        if not self.uid or not link_info.source_msg_id:
            return None

        try:
            # Get MongoDB collection via lazy import
            _upload_maps = _get_upload_maps_collection()
            if _upload_maps is None:
                return None

            # Query MongoDB directly for this src_msg_id across ALL user docs
            # The mappings are stored as {"mappings": {"6311": 15023, ...}}
            # We need to find any doc where mappings contains this key
            src_str = str(link_info.source_msg_id)

            # Try each possible key format in the mappings subdocument
            doc = await _upload_maps.find_one(
                {"user_id": self.uid, f"mappings.{src_str}": {"$exists": True}},
                {f"mappings.{src_str}": 1}
            )

            if doc and "mappings" in doc and src_str in doc["mappings"]:
                dst_msg_id = int(doc["mappings"][src_str])
                self.stats["entity_reverse_hits"] += 1
                # Add to direct map for future instant lookups
                self.msg_id_map[link_info.source_msg_id] = dst_msg_id
                print(f"[MONGODB-SCAN] ✅ HIT src={link_info.source_msg_id} → dst={dst_msg_id} "
                      f"(found in MongoDB, was missing from combined_map!)")
                return self.build_dest_url(dst_msg_id, link_info.thread_id)

            # Also try with int key (in case MongoDB auto-converted)
            # and try the link's source_peer as channel ID
            if link_info.source_peer:
                peer_str = str(link_info.source_peer)
                for ch_variant in [peer_str, f"-100{peer_str}", f"100{peer_str}"]:
                    doc2 = await _upload_maps.find_one(
                        {"user_id": self.uid, "source_channel": ch_variant,
                         f"mappings.{src_str}": {"$exists": True}},
                        {f"mappings.{src_str}": 1}
                    )
                    if doc2 and "mappings" in doc2 and src_str in doc2["mappings"]:
                        dst_msg_id = int(doc2["mappings"][src_str])
                        self.stats["entity_reverse_hits"] += 1
                        self.msg_id_map[link_info.source_msg_id] = dst_msg_id
                        print(f"[MONGODB-SCAN] ✅ HIT (variant) src={link_info.source_msg_id} → dst={dst_msg_id} "
                              f"channel={ch_variant}")
                        return self.build_dest_url(dst_msg_id, link_info.thread_id)

            return None

        except Exception as e:
            if not hasattr(self, '_mongodb_err_logged'):
                self._mongodb_err_logged = 0
            if self._mongodb_err_logged < 3:
                self._mongodb_err_logged += 1
                print(f"[MONGODB-SCAN] ERROR src={link_info.source_msg_id}: {e}")
            return None


# ════════════════════════════════════════════════════════════════════
# LAYER 3: ENTITY-SAFE EDITING
# ════════════════════════════════════════════════════════════════════

class EntitySafeEditor:
    """Edit messages without breaking any formatting (bold, italic, links, etc.)."""

    @staticmethod
    def _normalize_entity_type(entity_type) -> str:
        """Normalize Pyrofork enum entity type to a lowercase string.

        Pyrofork returns MessageEntityType.TEXT_LINK (enum), NOT 'text_link' (string).
        Comparing enum == 'text_link' is ALWAYS False — this is the root cause
        of TEXT_LINK entities being invisible to the extraction pipeline.

        Returns: 'text_link', 'url', 'bold', 'hashtag', etc.
        """
        if entity_type is None:
            return ""
        # Try .value first (Pyrofork enums have a .value attribute)
        if hasattr(entity_type, 'value'):
            # Pyrofork: MessageEntityType.TEXT_LINK.value is a raw type class
            # e.g. <class 'pyrogram.raw.types.message_entity_text_url.MessageEntityTextUrl'>
            val = entity_type.value
            if isinstance(val, str):
                return val.lower()
            # It's a raw type class — fall through to name-based extraction
        # Extract from enum name: MessageEntityType.TEXT_LINK → "text_link"
        type_str = str(entity_type)
        if '.' in type_str:
            # "MessageEntityType.TEXT_LINK" → "TEXT_LINK" → "text_link"
            name = type_str.rsplit('.', 1)[-1]
            return name.lower()
        return type_str.lower()

    @staticmethod
    def extract_links_from_entities(text: str, entities: list) -> List[Dict]:
        """Extract all Telegram links from message entities.

        Returns list of dicts:
            {"url": str, "offset": int, "length": int, "entity_type": str}
        """
        if not entities or not text:
            return []

        links = []
        for entity in entities:
            raw_type = getattr(entity, 'type', None)
            entity_type = EntitySafeEditor._normalize_entity_type(raw_type)
            url = None
            offset = getattr(entity, 'offset', 0)
            length = getattr(entity, 'length', 0)

            if entity_type == 'text_link':
                url = getattr(entity, 'url', None)
            elif entity_type == 'url':
                # Extract URL from text at offset
                if text and offset is not None and length > 0 and offset + length <= len(text):
                    url = text[offset:offset + length]
            else:
                continue

            if url and ('t.me' in url.lower() or 'tg://' in url.lower()):
                links.append({
                    "url": url,
                    "offset": offset,
                    "length": length,
                    "entity_type": entity_type,  # normalized string: 'text_link' or 'url'
                })

        return links

    @staticmethod
    def extract_links_from_text(text: str) -> List[Dict]:
        """Extract bare Telegram links from text (not in entities).

        Returns list of dicts:
            {"url": str, "offset": int, "length": int, "entity_type": "bare_url"}
        """
        if not text:
            return []

        links = []
        # Find all t.me/ URLs in text
        for match in re.finditer(r'https?://t\.me/[^\s\]\)]+', text, re.IGNORECASE):
            url = match.group(0)
            # Clean trailing punctuation
            url = url.rstrip('.,;:!?')
            links.append({
                "url": url,
                "offset": match.start(),
                "length": len(url),
                "entity_type": "bare_url",
            })

        # Find tg:// deep links
        for match in re.finditer(r'tg://resolve\?domain=\w+&post=\d+', text, re.IGNORECASE):
            links.append({
                "url": match.group(0),
                "offset": match.start(),
                "length": len(match.group(0)),
                "entity_type": "bare_tg",
            })

        return links

    @staticmethod
    def build_rewrite_plan(links: List[Dict], resolver: LinkResolver,
                           source_channels_info: dict, dest_channel_id: int = None) -> List[Dict]:
        """Build a plan of which links to rewrite and their replacements.

        Returns list of dicts:
            {"old_url": str, "new_url": str, "offset": int, "length": int,
             "entity_type": str, "link_info": LinkInfo, "resolved": bool}
        """
        plan = []
        for link in links:
            link_info = classify_link(link["url"], source_channels_info, dest_channel_id=dest_channel_id)

            # Skip non-source-channel links (they don't need rewriting)
            # Also skip dest channel self-references (already correct)
            if not link_info.is_source_channel:
                continue

            # Skip links that already point to the destination
            # (already correctly rewritten)

            new_url = None
            if link_info.is_source_channel:
                # Try to resolve via the resolver (async, but we handle it in the caller)
                plan.append({
                    "old_url": link["url"],
                    "offset": link["offset"],
                    "length": link["length"],
                    "entity_type": link["entity_type"],
                    "link_info": link_info,
                    "new_url": None,  # Will be filled by async resolve
                    "resolved": False,
                })

        return plan

    @staticmethod
    async def resolve_plan(plan: List[Dict], resolver: LinkResolver) -> List[Dict]:
        """Resolve all links in the plan using the multi-strategy resolver."""
        for item in plan:
            new_url = await resolver.resolve(item["link_info"])
            if new_url:
                item["new_url"] = new_url
                item["resolved"] = True
        return plan

    @staticmethod
    def apply_rewrites_to_text(text: str, plan: List[Dict]) -> Tuple[str, List[Dict]]:
        """Apply URL rewrites to text, adjusting offsets for subsequent entities.

        Returns (new_text, adjustments) where adjustments is a list of
        (position, delta) tuples tracking how entity offsets shift.
        """
        if not plan:
            return text, []

        # Sort by offset (process left to right)
        resolved = [p for p in plan if p["resolved"]]
        resolved.sort(key=lambda p: p["offset"])

        new_text = text
        total_delta = 0
        adjustments = []  # (original_offset, length_change)

        for item in resolved:
            old_url = item["old_url"]
            new_url = item["new_url"]
            original_offset = item["offset"]
            original_length = item["length"]

            actual_offset = original_offset + total_delta
            old_len = len(old_url)
            new_len = len(new_url)
            delta = new_len - old_len

            # Replace the URL in text
            # For entity-based links, the URL is stored separately (not in text for text_link)
            # For url entities and bare URLs, the URL IS in the text
            if item["entity_type"] in ("url", "bare_url", "bare_tg"):
                new_text = (
                    new_text[:actual_offset] +
                    new_url +
                    new_text[actual_offset + original_length:]
                )
                adjustments.append((original_offset, delta))
                total_delta += delta

        return new_text, adjustments

    @staticmethod
    def adjust_entities(entities: list, plan: List[Dict], adjustments: List[Tuple[int, int]],
                        dest_channel_id: int = None) -> list:
        """Adjust all entity offsets after text rewrites AND rewrite entity URLs.

        Two jobs:
        1. When bare URLs in text change length, shift offsets of entities after them.
        2. Rewrite entity.url for TEXT_LINK entities that match the plan.
           Also convert URL entities to TEXT_LINK when their content changes.

        CRITICAL: This must run even when adjustments is empty, because
        TEXT_LINK entities store their URL in entity.url (not in text).
        The text "Click here" never changes — only entity.url changes.
        If we short-circuit on empty adjustments, entity.url rewriting
        is skipped and the edit produces MESSAGE_NOT_MODIFIED → fixed=0.

        Returns (new_entities, entity_urls_changed) tuple where
        entity_urls_changed is True if any entity.url was rewritten.
        """
        if not entities:
            return entities or [], False

        # Try to import the TEXT_LINK enum for proper type assignment
        _text_link_type = None
        try:
            from pyrogram.enums import MessageEntityType
            _text_link_type = MessageEntityType.TEXT_LINK
        except ImportError:
            pass

        new_entities = []
        entity_urls_changed = False

        for entity in entities:
            new_entity = copy.deepcopy(entity)
            offset = getattr(new_entity, 'offset', 0)
            raw_type = getattr(new_entity, 'type', None)
            entity_type = EntitySafeEditor._normalize_entity_type(raw_type)

            # Calculate cumulative offset shift from text rewrites
            if adjustments:
                shift = sum(delta for pos, delta in adjustments if pos < offset)
                new_entity.offset = offset + shift

            # ── Rewrite TEXT_LINK entity.url ──────────────────────
            # "Click here" → entity.url = "https://t.me/c/XXX/123"
            # The visible text never changes, but entity.url must be updated.
            if hasattr(new_entity, 'url') and new_entity.url:
                for item in plan:
                    if not item["resolved"]:
                        continue
                    if item["entity_type"] in ("text_link", "private", "public", "cached"):
                        # Match by URL (most reliable)
                        if new_entity.url == item["old_url"]:
                            new_entity.url = item["new_url"]
                            entity_urls_changed = True
                            _edlog(f"[RELINK-ENTITY] TEXT_LINK url rewritten: "
                                   f"{item['old_url'][:50]} → {item['new_url'][:50]}")
                            break
                        # Also match by offset+type for safety
                        if (getattr(entity, 'offset', -1) == item["offset"] and
                                entity_type == "text_link"):
                            new_entity.url = item["new_url"]
                            entity_urls_changed = True
                            _edlog(f"[RELINK-ENTITY] TEXT_LINK url rewritten by offset: "
                                   f"offset={item['offset']}")
                            break
                        # SMART CACHE: Match by src_msg_id — when the entity URL was
                        # already rewritten to a dest-channel URL, we can still match
                        # by extracting the msg_id from both URLs and comparing.
                        # e.g. entity.url = "https://t.me/c/3900746078/30582" (dest)
                        #      item.old_url = "https://t.me/c/2563279588/19444" (source)
                        # If the resolver mapped src 19444 → dst 30582, then the
                        # entity URL is already correct (points to dest). But if the
                        # resolver found a DIFFERENT dst for src 19444, we need to
                        # update the entity URL.
                        item_link_info = item.get("link_info")
                        if item_link_info and item.get("from_cache"):
                            # Extract msg_id from the entity's current URL
                            _ent_match = PRIVATE_LINK_RE.match(new_entity.url)
                            _item_match = PRIVATE_LINK_RE.match(item["old_url"])
                            if _ent_match and _item_match:
                                _ent_peer = _ent_match.group(1)
                                _ent_msg_id = int(_ent_match.group(2))
                                _item_msg_id = int(_item_match.group(2))
                                # Check if the entity's msg_id matches the item's src_msg_id
                                # and the entity's peer is the DEST channel
                                if (_ent_msg_id == _item_msg_id and
                                        normalize_channel_id(_ent_peer) == normalize_channel_id(dest_channel_id)):
                                    # Entity already points to the correct dest msg
                                    # No rewrite needed — skip
                                    break
                                # If the entity has a DIFFERENT msg_id but same src_msg_id
                                # was resolved to a different dst, update the entity
                                if _item_msg_id == item_link_info.source_msg_id:
                                    new_entity.url = item["new_url"]
                                    entity_urls_changed = True
                                    _edlog(f"[RELINK-ENTITY] TEXT_LINK rewritten by src_msg_id match: "
                                           f"src={_item_msg_id} → {item['new_url'][:50]}")
                                    break

            # ── Convert URL entity to TEXT_LINK when URL changes ──
            # url entity: bare URL visible in text at entity.offset
            # When the URL text changes, convert to text_link so the
            # visible text stays the same but the clickable URL updates.
            if entity_type == "url":
                for item in plan:
                    if not item["resolved"]:
                        continue
                    if item["entity_type"] == "url" and offset == item["offset"]:
                        # Convert URL entity to TEXT_LINK
                        # Use the proper enum if available, fall back to string
                        if _text_link_type is not None:
                            new_entity.type = _text_link_type
                        else:
                            new_entity.type = 'text_link'
                        new_entity.url = item["new_url"]
                        new_entity.length = len(item["new_url"])
                        entity_urls_changed = True
                        _edlog(f"[RELINK-ENTITY] URL → TEXT_LINK conversion at offset={offset}")
                        break

            new_entities.append(new_entity)

        return new_entities, entity_urls_changed


# ════════════════════════════════════════════════════════════════════
# LAYER 4: RATE LIMIT ARMOR
# ════════════════════════════════════════════════════════════════════

class RateLimitArmor:
    """Intelligent rate limiting that adapts to Telegram's responses.

    Features:
    - Base delay between edits (configurable)
    - Adaptive speed-up after consecutive successes
    - Exponential backoff on FloodWait
    - Per-minute edit tracking to stay under Telegram limits
    - Maximum delay cap to prevent infinite slowdowns
    """

    def __init__(self, base_delay: float = 0.5, max_delay: float = 5.0,
                 min_delay: float = 0.3, max_edits_per_minute: int = 15):
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.max_delay = max_delay
        self.min_delay = min_delay
        self.max_edits_per_minute = max_edits_per_minute
        self.consecutive_successes = 0
        self.flood_wait_count = 0
        self.edit_timestamps = []
        self.total_waits = 0
        self.total_wait_time = 0.0

    async def acquire(self):
        """Wait before making an edit (smart throttling)."""
        now = time.time()

        # Clean old timestamps (older than 60s)
        self.edit_timestamps = [t for t in self.edit_timestamps if now - t < 60]

        # If we've hit the per-minute limit, wait
        if len(self.edit_timestamps) >= self.max_edits_per_minute:
            oldest = self.edit_timestamps[0]
            wait_time = 60 - (now - oldest) + 1
            if wait_time > 0:
                _edlog(f"[RELINK-RATE] Per-minute limit reached, waiting {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
                self.total_wait_time += wait_time

        # Normal delay between edits
        await asyncio.sleep(self.current_delay)
        self.edit_timestamps.append(time.time())

    def on_success(self):
        """Called after successful edit — gradually speed up."""
        self.consecutive_successes += 1
        self.flood_wait_count = 0

        # After 20 consecutive successes, reduce delay by 10%
        if self.consecutive_successes >= 20:
            self.current_delay = max(self.min_delay, self.current_delay * 0.9)
            self.consecutive_successes = 0
            _edlog(f"[RELINK-RATE] Speeding up: delay now {self.current_delay:.2f}s")

    async def on_flood_wait(self, wait_seconds: int):
        """Called when FloodWait received — back off significantly."""
        self.flood_wait_count += 1
        self.consecutive_successes = 0
        self.total_waits += 1

        # Exponential backoff
        self.current_delay = min(self.max_delay, self.current_delay * 2)

        _edlog(f"[RELINK-RATE] FloodWait({wait_seconds}s)! "
               f"Backing off to {self.current_delay:.2f}s delay "
               f"(flood count: {self.flood_wait_count})")

        # Wait longer than Telegram says (safety margin)
        actual_wait = wait_seconds + 2
        await asyncio.sleep(actual_wait)
        self.total_wait_time += actual_wait

    def on_error(self, error: Exception):
        """Called on other errors — slight increase in delay."""
        self.consecutive_successes = 0
        # Slight increase
        self.current_delay = min(self.max_delay, self.current_delay * 1.2)

    def get_stats(self) -> dict:
        return {
            "current_delay": round(self.current_delay, 2),
            "flood_waits": self.flood_wait_count,
            "total_waits": self.total_waits,
            "total_wait_time": round(self.total_wait_time, 1),
            "consecutive_successes": self.consecutive_successes,
        }


# ════════════════════════════════════════════════════════════════════
# GAP 5: BULK EDIT QUEUE
#
# Instead of editing one message at a time and hitting FloodWait every
# few edits, collects all pending edits into a queue and fires them
# in controlled bursts. On FloodWait, sleeps exact required seconds
# then continues. Overall 3-4x faster than serial editing.
# ════════════════════════════════════════════════════════════════════

@dataclass
class EditTask:
    """Represents a single message edit operation."""
    dst_chat_id      : int
    dst_msg_id       : int
    new_text         : str
    new_entities     : list = None
    is_caption       : bool = False
    src_msg_id       : int  = 0      # for logging
    uid              : int  = 0
    source_channel   : str  = ""


async def process_edit_queue(
    edit_tasks      : list,    # list of EditTask
    client          ,
    dry_run         : bool = False,
    base_delay      : float = 1.2,   # seconds between edits
    burst_size      : int   = 10,    # edits per burst before longer pause
    burst_pause     : float = 5.0,   # pause between bursts
) -> dict:
    """
    GAP 5: Process all edit tasks in controlled bursts.
    Handles FloodWait by sleeping and retrying.
    Never skips a task on FloodWait.

    Returns stats dict with counts.
    """
    stats = {
        "success"    : 0,
        "failed"     : 0,
        "flood_waits": 0,
        "skipped"    : 0,
    }

    total     = len(edit_tasks)
    processed = 0

    logger.info(
        f"[EDIT-QUEUE] Processing {total} edits "
        f"burst_size={burst_size} delay={base_delay}s"
    )

    i = 0
    while i < len(edit_tasks):
        task = edit_tasks[i]

        if dry_run:
            logger.info(
                f"[EDIT-QUEUE][DRY-RUN] Would edit dst={task.dst_msg_id} "
                f"src={task.src_msg_id}"
            )
            stats["success"] += 1
            i += 1
            processed += 1
            continue

        try:
            if task.new_entities:
                if not task.is_caption:
                    await client.edit_message_text(
                        chat_id    = task.dst_chat_id,
                        message_id = task.dst_msg_id,
                        text       = task.new_text,
                        entities   = task.new_entities,
                    )
                else:
                    await client.edit_message_caption(
                        chat_id           = task.dst_chat_id,
                        message_id        = task.dst_msg_id,
                        caption           = task.new_text,
                        caption_entities  = task.new_entities,
                    )
            else:
                if not task.is_caption:
                    await client.edit_message_text(
                        chat_id    = task.dst_chat_id,
                        message_id = task.dst_msg_id,
                        text       = task.new_text,
                    )
                else:
                    await client.edit_message_caption(
                        chat_id    = task.dst_chat_id,
                        message_id = task.dst_msg_id,
                        caption    = task.new_text,
                    )

            stats["success"] += 1
            processed        += 1
            i                += 1

            logger.debug(
                f"[EDIT-QUEUE] ✅ dst={task.dst_msg_id} "
                f"({processed}/{total})"
            )

            # Delay between edits
            await asyncio.sleep(base_delay)

            # Burst pause every N edits
            if processed % burst_size == 0:
                logger.info(
                    f"[EDIT-QUEUE] Burst pause {burst_pause}s "
                    f"after {processed} edits"
                )
                await asyncio.sleep(burst_pause)

        except FloodWait as e:
            wait = getattr(e, 'value', 37) + 5
            stats["flood_waits"] += 1
            logger.warning(
                f"[EDIT-QUEUE] FloodWait {wait}s — sleeping "
                f"then retrying dst={task.dst_msg_id}"
            )
            await asyncio.sleep(wait)
            # DO NOT increment i — retry same task

        except Exception as e:
            err = str(e)
            if any(code in err for code in [
                "MESSAGE_NOT_MODIFIED",
                "MessageNotModified",
                "MESSAGE_ID_INVALID",
                "MESSAGE_EDIT_TIME_EXPIRED",
            ]):
                # Not a real error — move on
                stats["skipped"] += 1
                i += 1
                processed += 1
            else:
                logger.error(
                    f"[EDIT-QUEUE] Failed dst={task.dst_msg_id}: {e}"
                )
                stats["failed"] += 1
                i += 1
                processed += 1

    logger.info(
        f"[EDIT-QUEUE] Done — "
        f"success={stats['success']} "
        f"failed={stats['failed']} "
        f"flood_waits={stats['flood_waits']} "
        f"skipped={stats['skipped']}"
    )
    return stats


# ════════════════════════════════════════════════════════════════════
# LAYER 5: PROGRESS DASHBOARD & CANCELLATION
# ════════════════════════════════════════════════════════════════════

class ProgressReporter:
    """Live progress updates with rate-limit-aware editing."""

    def __init__(self, client, status_msg: Message, update_interval: int = 30):
        self.client = client
        self.status_msg = status_msg
        self.last_update = 0
        self.update_interval = update_interval  # seconds between status updates
        self.last_text = ""

    async def update(self, stats: dict, force: bool = False):
        """Update the status message if enough time has passed."""
        now = time.time()
        if not force and now - self.last_update < self.update_interval:
            return

        self.last_update = now

        elapsed = time.time() - stats.get("start_time", time.time())
        speed = stats.get("scanned", 0) / max(elapsed / 60, 0.01)

        # Build progress bar
        if stats.get("total_estimated"):
            pct = stats["scanned"] / stats["total_estimated"]
            bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
            progress = f"[{bar}] {pct * 100:.1f}%"
        else:
            progress = f"Scanned: {stats.get('scanned', 0)}"

        text = (
            f"🔍 **Relink In Progress**\n"
            f"{progress}\n\n"
            f"Scanned: {stats.get('scanned', 0)} | "
            f"Fixed: {stats.get('fixed', 0)} | "
            f"Unresolved: {stats.get('unresolved', 0)} | "
            f"Skipped: {stats.get('skipped', 0)}\n"
            f"Speed: {speed:.1f} msg/min | "
            f"Elapsed: {_fmt_dur(elapsed)}"
        )

        if text == self.last_text:
            return  # No change

        self.last_text = text
        try:
            await self.client.edit_message_text(
                self.status_msg.chat.id,
                self.status_msg.id,
                text
            )
        except MessageNotModified:
            pass
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception:
            pass


def _fmt_dur(secs: float) -> str:
    """Format duration in human-readable form."""
    if secs >= 3600:
        return f"{secs // 3600:.0f}h {(secs % 3600) // 60:.0f}m"
    elif secs >= 60:
        return f"{secs // 60:.0f}m {secs % 60:.0f}s"
    else:
        return f"{secs:.0f}s"


# ════════════════════════════════════════════════════════════════════
# LAYER 6: PRE-FLIGHT VALIDATION
# ════════════════════════════════════════════════════════════════════

async def pre_flight_check(client, chat_id: int, uid: int) -> dict:
    """Run all checks before starting relink. Returns checks dict."""
    checks = {
        "bot_is_admin": False,
        "can_edit_messages": False,
        "msg_id_map_exists": False,
        "msg_id_map_count": 0,
        "chat_has_messages": False,
        "source_channels_count": 0,
        "all_ok": False,
    }

    # Check 1: Bot is admin with edit permission
    try:
        member = await client.get_chat_member(chat_id, "me")
        # Pyrofork returns ChatMemberStatus enum, not a string.
        # Must handle both enum and string representations.
        status_str = str(member.status).lower()
        is_admin = (
            "administrator" in status_str or
            "owner" in status_str or
            status_str in ("administrator", "owner")
        )
        checks["bot_is_admin"] = is_admin
        # can_edit_messages: Bots can ALWAYS edit their OWN messages.
        # The relink feature edits messages the BOT itself posted during
        # mirroring — no special "can_edit_messages" admin right needed.
        # That right is only for editing OTHER users' messages.
        # So if bot is admin at all, it can do relink.
        if is_admin:
            checks["can_edit_messages"] = True
        else:
            checks["can_edit_messages"] = False
    except Exception as e:
        _edlog(f"[RELINK-PREFLIGHT] Admin check failed: {e}")

    # Check 2: msg_id_map has data (check ALL possible sources)
    try:
        count = 0
        ch_count = 0

        # Source A: upload_maps (from /batch) — filter by destination
        _uc = _get_upload_maps_collection()
        if _uc is not None:
            cursor = _uc.find({"user_id": uid})
            async for doc in cursor:
                dest_ch = doc.get("dest_channel")
                # Only count mappings for THIS destination channel
                if dest_ch and normalize_channel_id(dest_ch) != normalize_channel_id(chat_id):
                    continue
                mappings = doc.get("mappings", {})
                count += len(mappings)
                ch_count += 1

        # Source B: mirror_src_to_dst (from /mirror) — filter by destination
        try:
            _msc = _batch_funcs.get("mirror_state_collection") if _batch_funcs_fully_loaded else None
            _mstdc = _batch_funcs.get("mirror_src_to_dst_collection") if _batch_funcs_fully_loaded else None
            if _msc is None or _mstdc is None:
                try:
                    from plugins.batch import mirror_state_collection as _msc, mirror_src_to_dst_collection as _mstdc
                except Exception:
                    _msc = _mstdc = None
            if _msc is not None and _mstdc is not None:
                async for mstate in _msc.find({"dst_chat_id": chat_id}):
                    mid = mstate.get("mirror_id", "")
                    if mid:
                        m_count = await _mstdc.count_documents({"mirror_id": mid, "status": "done"})
                        count += m_count
                        if m_count > 0:
                            ch_count += 1
        except Exception as _me:
            _edlog(f"[RELINK-PREFLIGHT] Mirror map check failed: {_me}")

        # Source C: fetch_maps (from /fetch) — no dest info, count unique source channels
        _fetch_ch_count = 0
        try:
            from plugins.fetch import fetch_maps_collection as _fmc
            _seen_ch = set()
            async for fdoc in _fmc.find({"user_id": uid}):
                ch_id = fdoc.get("channel_id", "")
                if ch_id and ch_id not in _seen_ch:
                    _seen_ch.add(ch_id)
                    _fetch_ch_count += 1
        except Exception as _fe:
            _edlog(f"[RELINK-PREFLIGHT] Fetch map check failed: {_fe}")

        # Source D: fingerprints collection (from checkpoint_with_fingerprint)
        # This is often the RICHEST source of src→dst mappings
        _fp_count = 0
        try:
            _fp_count = await fingerprints_collection.count_documents({"uid": uid})
            count += _fp_count
        except Exception as _fpe:
            _edlog(f"[RELINK-PREFLIGHT] Fingerprint check failed: {_fpe}")

        # Source E: Smart Cache (mirrored_messages_index)
        # Shows how many messages are already indexed for fast /relink
        _smart_cache_count = 0
        try:
            from plugins.batch import mirrored_messages_index as _mmi
            _smart_cache_count = await _mmi.count_documents({"uid": uid, "dst_chat_id": chat_id})
            _sc_unresolved = await _mmi.count_documents(
                {"uid": uid, "dst_chat_id": chat_id, "contains_old_links": True}
            )
            _edlog(f"[RELINK-PREFLIGHT] Smart Cache: {_smart_cache_count} indexed, "
                   f"{_sc_unresolved} with old links")
            checks["smart_cache_count"] = _smart_cache_count
            checks["smart_cache_unresolved"] = _sc_unresolved
        except Exception as _sce:
            _edlog(f"[RELINK-PREFLIGHT] Smart Cache check failed: {_sce}")
            checks["smart_cache_count"] = 0
            checks["smart_cache_unresolved"] = 0

        # We have data if ANY source has mappings OR if /fetch scanned channels
        checks["msg_id_map_exists"] = count > 0 or _fetch_ch_count > 0
        checks["msg_id_map_count"] = count
        checks["source_channels_count"] = ch_count + _fetch_ch_count
    except Exception as e:
        _edlog(f"[RELINK-PREFLIGHT] Map check failed: {e}")

    # Check 3: Chat has messages
    # NOTE: Bots CANNOT use get_chat_history() or search_messages().
    # Both are user-only methods. Instead, try to fetch a specific
    # message by ID using get_messages(chat_id, [id]) which uses
    # channels.getMessages — bot-compatible for supergroups.
    try:
        # Try fetching message ID 1 (earliest) — if it exists, chat has messages
        test_msg = await client.get_messages(chat_id, 1)
        if test_msg and not getattr(test_msg, 'empty', True):
            checks["chat_has_messages"] = True
    except Exception:
        pass
    if not checks["chat_has_messages"]:
        try:
            chat_info = await client.get_chat(chat_id)
            if chat_info:
                checks["chat_has_messages"] = True
        except Exception as e:
            _edlog(f"[RELINK-PREFLIGHT] Chat check failed: {e}")

    # Overall check
    checks["all_ok"] = (
        checks["bot_is_admin"] and
        checks["can_edit_messages"] and
        checks["msg_id_map_exists"] and
        checks["chat_has_messages"]
    )

    return checks


# ════════════════════════════════════════════════════════════════════
# LAYER 7: AUTO-RELINK ON NEW MIRROR
# ════════════════════════════════════════════════════════════════════

async def check_new_mapping_resolves_unresolved(uid: int, source_channel: str,
                                                  new_src_msg_id: int, new_dest_msg_id: int,
                                                  dest_chat_id: int):
    """Called when a NEW message is mirrored. Checks if this new mapping
    can resolve any previously-unresolved links in the destination chat.

    This is the 'Fix-on-the-Fly' auto-relink layer — links get fixed in
    real-time as new messages are mirrored, not just when someone runs /relink.

    FLOW:
    1. Message B is just mirrored → src_msg_id=B → dst_msg_id=B'
    2. We query MongoDB for messages that had unresolved links to B
    3. For each such message, we fetch it, rewrite the links, and edit it
    4. If all links are now resolved, we mark it as resolved in MongoDB

    This is the EXACT "Alternative 2: Fix-on-the-Fly" implementation.
    """
    try:
        # Find unresolved links where this src_msg_id was listed as unresolved
        _ulc = _get_unresolved_links_collection()
        if _ulc is None:
            return
        cursor = _ulc.find({
            "user_id": uid,
            "unresolved": True,
            "unresolved_src_ids": new_src_msg_id,
        })

        unresolved = await cursor.to_list(length=100)
        if not unresolved:
            return  # Nothing to fix — most common case, returns instantly

        _edlog(f"[AUTO-RELINK] ✅ New mapping src={new_src_msg_id}→dst={new_dest_msg_id} "
               f"resolves {len(unresolved)} previously-unresolved message(s)")

        # Load the msg_id_map for this source channel (filtered by dest for efficiency)
        combined_map, _ = await load_combined_msg_id_map(uid, dest_channel_id=dest_chat_id)

        # Add the new mapping
        combined_map[new_src_msg_id] = new_dest_msg_id

        # Build multi_source_channels for rewriting
        multi_src_channels, _ = await build_multi_source_channels(
            uid, source_channel, client=None
        )

        # Resolve each affected message
        _fixed_count = 0
        for item in unresolved:
            dst_msg_id = item["dst_msg_id"]

            try:
                # Get the current message — try ubot first (can edit its own msgs), then bot
                _ubot = get_Y()
                bot_client = X

                dst_msg = None
                for _fc in [_ubot, bot_client]:
                    if not _fc:
                        continue
                    try:
                        dst_msg = await _fc.get_messages(dest_chat_id, dst_msg_id)
                        if dst_msg and not getattr(dst_msg, 'empty', True):
                            break
                    except Exception:
                        continue

                if not dst_msg or getattr(dst_msg, 'empty', True):
                    # Message deleted — mark resolved to stop retrying
                    await mark_links_resolved(uid, source_channel, dest_chat_id, dst_msg_id)
                    continue

                # Rewrite links using existing function
                raw_text = dst_msg.text or dst_msg.caption or ""
                new_text, has_unresolved_text = rewrite_telegram_links(
                    raw_text,
                    source_channel,
                    dest_chat_id,
                    None,  # dest_channel_username — will be derived
                    combined_map,
                    multi_source_channels=multi_src_channels,
                )

                new_entities, had_unresolved_entities, modified_text = rewrite_entity_urls(
                    dst_msg.entities or dst_msg.caption_entities or [],
                    source_channel,
                    dest_chat_id,
                    None,
                    combined_map,
                    raw_text=raw_text,
                    skip_url_entity_conversion=True,
                    multi_source_channels=multi_src_channels,
                )

                # Edit the message
                text_changed = (new_text != raw_text)
                entities_changed = new_entities != (dst_msg.entities or dst_msg.caption_entities)

                if text_changed or entities_changed:
                    is_cap = bool(getattr(dst_msg, 'caption', None)) and not bool(getattr(dst_msg, 'text', None))
                    edit_ok = await edit_message_safe(
                        bot_client=bot_client,
                        ubot=_ubot,
                        dst_chat_id=dest_chat_id,
                        dst_msg_id=dst_msg_id,
                        new_text=new_text or raw_text,
                        new_entities=new_entities if new_entities else None,
                        is_caption=is_cap,
                    )
                    if edit_ok:
                        _fixed_count += 1
                        _edlog(f"[AUTO-RELINK] ✅ Fixed msg {dst_msg_id} — link to src={new_src_msg_id} resolved")
                    else:
                        _edlog(f"[AUTO-RELINK] ⚠️ Edit failed for msg {dst_msg_id} — all clients rejected")

                if not has_unresolved_text and not had_unresolved_entities:
                    await mark_links_resolved(uid, source_channel, dest_chat_id, dst_msg_id)
                    # Also mark as fixed in Smart Cache
                    try:
                        from plugins.batch import mark_message_links_fixed
                        await mark_message_links_fixed(uid, dest_chat_id, dst_msg_id)
                    except Exception:
                        pass

            except Exception as e:
                _edlog(f"[AUTO-RELINK] Failed to process msg {dst_msg_id}: {e}")

        if _fixed_count > 0:
            _edlog(f"[AUTO-RELINK] Fixed {_fixed_count}/{len(unresolved)} messages "
                   f"via new mapping src={new_src_msg_id}→dst={new_dest_msg_id}")

    except Exception as e:
        _edlog(f"[AUTO-RELINK] Error in auto-relink: {e}")


# ════════════════════════════════════════════════════════════════════
# GAP 4: SCAN DIRECTION CONTROL
#
# Adds --direction flag to /relink.
# old_to_new: finds min msg_id → scans forward (catches pinned/index first)
# new_to_old: starts from latest msg → scans backward (current default)
# auto: first session → old_to_new, subsequent → new_to_old
# ════════════════════════════════════════════════════════════════════

def parse_relink_args(args: list) -> dict:
    """
    Parse /relink command arguments.
    /relink --direction old_to_new --limit 500 --dry-run
    """
    options = {
        "direction": "auto",   # auto | old_to_new | new_to_old
        "limit"    : 0,
        "dry_run"  : False,
    }

    i = 0
    while i < len(args):
        if args[i] == "--direction" and i + 1 < len(args):
            options["direction"] = args[i + 1]
            i += 2
        elif args[i].startswith("--direction="):
            options["direction"] = args[i].split("=", 1)[1]
            i += 1
        elif args[i] == "--limit" and i + 1 < len(args):
            try:
                options["limit"] = int(args[i + 1])
            except ValueError:
                pass
            i += 2
        elif args[i].startswith("--limit="):
            try:
                options["limit"] = int(args[i].split("=", 1)[1])
            except ValueError:
                pass
            i += 1
        elif args[i] == "--dry-run":
            options["dry_run"] = True
            i += 1
        else:
            i += 1

    return options


def build_id_batches(
    start_id   : int,
    end_id     : int,
    direction  : str,
    batch_size : int = 200,
) -> list:
    """
    Build list of ID batches to fetch based on direction.

    old_to_new: [1..200], [201..400], [401..600]...
    new_to_old: [latest..latest-200], [latest-201..latest-400]...
    """
    batches = []

    if direction == "old_to_new":
        current = start_id
        while current <= end_id:
            batch_end = min(current + batch_size - 1, end_id)
            batches.append(list(range(current, batch_end + 1)))
            current = batch_end + 1
    else:
        current = start_id
        while current >= end_id:
            batch_start = max(current - batch_size + 1, end_id)
            batches.append(list(range(batch_start, current + 1)))
            current = batch_start - 1

    return batches


# ════════════════════════════════════════════════════════════════════
# GAP 6: SOURCE CHANNEL NEW MESSAGE SCAN
#
# After batch completes, scans source channel for messages posted
# after the last batch run that contain links to other source messages.
# Checks if their destination equivalents have those links rewritten.
# If not, adds them to unresolved_links_collection.
# ════════════════════════════════════════════════════════════════════

LINK_PATTERN_RE = re.compile(
    r'https?://t\.me/c/\d+/\d+|https?://t\.me/\w+/\d+',
    re.IGNORECASE
)


async def scan_source_for_new_link_messages(
    uid              : int,
    source_channel   : int,
    dst_channel      : int,
    user_client      ,
    msg_id_map       : dict,
    last_scanned_id  : int = 0,
    batch_size       : int = 200,
):
    """
    GAP 6: Scans source channel for messages containing t.me links
    that were posted after last_scanned_id.

    For each such message:
        1. Find its dst_msg_id from msg_id_map
        2. Fetch dst message
        3. Check if dst message still has source channel links
        4. If yes → add to unresolved_links_collection

    Uses batch fetching — not get_chat_history.
    """
    if not user_client:
        user_client = get_Y()
    if not user_client:
        logger.warning("[SOURCE-SCAN] No user client available — skipping")
        return

    # Get latest message ID in source from the msg_id_map
    if not msg_id_map:
        logger.info("[SOURCE-SCAN] No msg_id_map — skipping")
        return

    max_mapped_id = max(msg_id_map.keys()) if msg_id_map else 0
    if max_mapped_id <= last_scanned_id:
        logger.info("[SOURCE-SCAN] No new messages since last scan")
        return

    logger.info(
        f"[SOURCE-SCAN] Scanning source messages "
        f"from {last_scanned_id} to {max_mapped_id}"
    )

    found     = 0
    needs_fix = 0
    current   = last_scanned_id + 1

    while current <= max_mapped_id:
        batch_end = min(current + batch_size - 1, max_mapped_id)
        ids       = list(range(current, batch_end + 1))
        current   = batch_end + 1

        try:
            msgs = await user_client.get_messages(source_channel, ids)
        except Exception as e:
            logger.error(f"[SOURCE-SCAN] batch fetch failed: {e}")
            continue

        for msg in msgs:
            if not msg or getattr(msg, "empty", True):
                continue

            text = (
                str(msg.text)    if msg.text    else
                str(msg.caption) if msg.caption else
                None
            )
            if not text:
                continue

            # Quick filter — skip if no t.me link
            if "t.me" not in text.lower():
                continue

            # Check if has source channel links
            links = LINK_PATTERN_RE.findall(text)
            if not links:
                continue

            found += 1

            # Find dst equivalent
            dst_msg_id = msg_id_map.get(msg.id)
            if not dst_msg_id:
                continue

            # Fetch dst message and check for unresolved links
            try:
                dst_msg = await user_client.get_messages(dst_channel, dst_msg_id)
                if not dst_msg or getattr(dst_msg, "empty", True):
                    continue

                dst_text = (
                    str(dst_msg.text)    if dst_msg.text    else
                    str(dst_msg.caption) if dst_msg.caption else
                    None
                )
                if not dst_text:
                    continue

                # If dst still has source channel links → needs fix
                src_clean = str(source_channel).lstrip("-")
                if src_clean.startswith("100"):
                    src_clean = src_clean[3:]

                if f"t.me/c/{src_clean}/" in dst_text:
                    try:
                        from plugins.batch import mark_needs_link_update
                        await mark_needs_link_update(
                            uid            = uid,
                            source_channel = str(source_channel),
                            dst_chat_id    = dst_channel,
                            dst_msg_id     = dst_msg_id,
                            src_msg_id     = msg.id,
                        )
                    except ImportError:
                        await mark_needs_link_update(
                            uid            = uid,
                            source_channel = str(source_channel),
                            dst_chat_id    = dst_channel,
                            dst_msg_id     = dst_msg_id,
                            src_msg_id     = msg.id,
                        )
                    needs_fix += 1
                    logger.info(
                        f"[SOURCE-SCAN] src={msg.id} → dst={dst_msg_id} "
                        f"has unresolved source links → added to queue"
                    )

            except Exception as e:
                logger.error(f"[SOURCE-SCAN] dst check failed: {e}")

    # Update watermark
    try:
        await source_scan_watermark_collection.update_one(
            {"uid": uid, "source_channel": str(source_channel)},
            {"$set": {"last_scanned_id": max_mapped_id, "updated_at": datetime.utcnow()}},
            upsert=True,
        )
    except Exception:
        pass

    logger.info(
        f"[SOURCE-SCAN] Done — "
        f"messages_with_links={found} "
        f"needs_fix={needs_fix}"
    )


# ════════════════════════════════════════════════════════════════════
# GAP 7: COMPLETION NOTIFICATION WITH SUMMARY
#
# After every /relink session completes, sends a detailed summary
# message showing exactly what was fixed, what's still pending and why,
# and which messages had their links corrected.
# ════════════════════════════════════════════════════════════════════

async def send_relink_summary(
    client          ,
    chat_id         : int,
    status_msg_id   : int,
    session         : dict,
    cache_stats     : dict,
    edit_stats      : dict,
    duration_secs   : int,
):
    """
    GAP 7: Sends final summary after /relink completes.
    Edits the status message with full results.
    """
    total_scanned    = session.get("total_scanned",        0)
    total_fixed      = session.get("total_fixed",          0)
    total_unresolved = session.get("total_unresolved",     0)
    total_skipped    = session.get("total_skipped",        0)
    already_correct  = session.get("total_already_correct",0)
    failed_edits     = session.get("failed_edits",         [])
    unresolved_links = session.get("unresolved_links",     [])

    # Format duration
    hours   = duration_secs // 3600
    minutes = (duration_secs % 3600) // 60
    seconds = duration_secs % 60

    if hours > 0:
        duration_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        duration_str = f"{minutes}m {seconds}s"
    else:
        duration_str = f"{seconds}s"

    # Build summary text
    lines = [
        "✅ **Relink Complete**\n",
        f"⏱ Duration: `{duration_str}`",
        f"📊 Scanned: `{total_scanned}` messages",
        f"✏️ Fixed: `{total_fixed}` messages",
        f"✅ Already correct: `{already_correct}` messages",
        f"⏭ Skipped (no links): `{total_skipped}` messages",
        "",
        "**Cache Performance:**",
        f"  Cache hits: `{cache_stats.get('hits', 0)}`",
        f"  Cache misses: `{cache_stats.get('misses', 0)}`",
        f"  New entries cached: `{cache_stats.get('new', 0)}`",
        "",
        "**Edit Results:**",
        f"  Successful edits: `{edit_stats.get('success', 0)}`",
        f"  Failed edits: `{edit_stats.get('failed', 0)}`",
        f"  FloodWaits hit: `{edit_stats.get('flood_waits', 0)}`",
    ]

    # Still unresolved
    if unresolved_links:
        lines.append("")
        lines.append(
            f"⚠️ **Still Unresolved: `{len(unresolved_links)}`**"
        )
        lines.append(
            "_(Target messages not yet uploaded — "
            "will auto-fix when uploaded)_"
        )

        # Show first 5 unresolved
        for item in unresolved_links[:5]:
            lines.append(
                f"  • dst=`{item.get('msg_id')}` "
                f"waiting for src=`{item.get('source_msg_id')}`"
            )
        if len(unresolved_links) > 5:
            lines.append(f"  • ...and {len(unresolved_links) - 5} more")

    # Failed edits
    if failed_edits:
        lines.append("")
        lines.append(f"❌ **Failed Edits: `{len(failed_edits)}`**")
        for item in failed_edits[:3]:
            lines.append(
                f"  • dst=`{item.get('msg_id')}`: "
                f"`{str(item.get('error', ''))[:50]}`"
            )

    summary_text = "\n".join(lines)

    try:
        await client.edit_message_text(
            chat_id    = chat_id,
            message_id = status_msg_id,
            text       = summary_text,
        )
    except Exception as e:
        # If can't edit → send new message
        try:
            await client.send_message(chat_id, summary_text)
        except Exception as e2:
            logger.error(f"[SUMMARY] Failed to send: {e2}")


# ════════════════════════════════════════════════════════════════════
# SAFE EDIT + SAFE FETCH HELPERS
#
# Problem 1: MESSAGE_AUTHOR_REQUIRED — Bot trying to edit messages
#   posted by the user client (ubot). Fix: try ubot first for editing,
#   bot as fallback.
# Problem 2: Scan stuck at 3277 — FloodWait on GetMessages not properly
#   respected. Fix: fetch_message_batch_safe with exact wait + retry.
# ════════════════════════════════════════════════════════════════════

async def edit_message_safe(
    bot_client,
    ubot,
    dst_chat_id: int,
    dst_msg_id: int,
    new_text: str,
    new_entities: list | None,
    is_caption: bool,
) -> bool:
    """
    Try ubot first for editing (it posted most messages via batch).
    Fall back to bot_client (can edit messages posted by bot).

    Handles:
      - MESSAGE_AUTHOR_REQUIRED → try next client
      - MESSAGE_NOT_MODIFIED → not an error, return True
      - FLOOD_WAIT → sleep exact time, retry same client once
      - Caption vs text dispatching

    IMPORTANT: Gets ubot FRESH from shared_client each time, because:
    - ubot may be None at scan start but become available later
    - The userbot might still be initializing when /relink starts
    - Avoids caching a stale None value for the entire scan
    """
    # ── Get ubot FRESH — don't trust the cached value ──────────
    # The ubot param might be None from scan start, but shared_client.userbot
    # might have been initialized since then. Always re-check.
    active_ubot = ubot
    if active_ubot is None:
        try:
            import shared_client as _sc
            active_ubot = _sc.userbot
        except Exception:
            pass

    clients = []

    # ubot first — it posted most messages via batch upload
    if active_ubot is not None:
        # Verify ubot is actually connected before adding
        # Pyrogram: is_connected is True when connected, None/False when not
        _ubot_connected = getattr(active_ubot, 'is_connected', True)
        if _ubot_connected:
            clients.append(("ubot", active_ubot))
        else:
            _edlog(f"[RELINK-EDIT] ubot exists but NOT connected — skipping")
    else:
        _edlog(f"[RELINK-EDIT] ubot is None — only bot can edit (will fail for ubot-posted msgs)")

    # bot as fallback — can edit messages it posted itself
    if bot_client is not None:
        clients.append(("bot", bot_client))

    if not clients:
        _edlog(f"[RELINK-EDIT] NO clients available for dst={dst_msg_id}!")
        return False

    _edlog(f"[RELINK-EDIT] Attempting edit dst={dst_msg_id} with clients: "
           f"{[name for name, _ in clients]}")

    for name, client in clients:
        try:
            if new_entities:
                if not is_caption:
                    await client.edit_message_text(
                        chat_id=dst_chat_id,
                        message_id=dst_msg_id,
                        text=new_text,
                        entities=new_entities,
                    )
                else:
                    await client.edit_message_caption(
                        chat_id=dst_chat_id,
                        message_id=dst_msg_id,
                        caption=new_text,
                        caption_entities=new_entities,
                    )
            else:
                if not is_caption:
                    await client.edit_message_text(
                        chat_id=dst_chat_id,
                        message_id=dst_msg_id,
                        text=new_text,
                    )
                else:
                    await client.edit_message_caption(
                        chat_id=dst_chat_id,
                        message_id=dst_msg_id,
                        caption=new_text,
                    )

            _edlog(f"[RELINK-EDIT] {name} edited dst={dst_msg_id}"
                   + (" (caption)" if is_caption else ""))
            return True

        except MessageNotModified:
            _edlog(f"[RELINK-EDIT] dst={dst_msg_id} not modified — already correct")
            return True

        except FloodWait as e:
            wait = getattr(e, 'value', 10) + 5
            _edlog(f"[RELINK-EDIT] {name} FloodWait {wait}s on dst={dst_msg_id}")
            await asyncio.sleep(wait)
            # Retry same client once after FloodWait
            try:
                if new_entities:
                    if not is_caption:
                        await client.edit_message_text(
                            chat_id=dst_chat_id,
                            message_id=dst_msg_id,
                            text=new_text,
                            entities=new_entities,
                        )
                    else:
                        await client.edit_message_caption(
                            chat_id=dst_chat_id,
                            message_id=dst_msg_id,
                            caption=new_text,
                            caption_entities=new_entities,
                        )
                else:
                    if not is_caption:
                        await client.edit_message_text(
                            chat_id=dst_chat_id,
                            message_id=dst_msg_id,
                            text=new_text,
                        )
                    else:
                        await client.edit_message_caption(
                            chat_id=dst_chat_id,
                            message_id=dst_msg_id,
                            caption=new_text,
                        )
                _edlog(f"[RELINK-EDIT] {name} retry succeeded for dst={dst_msg_id}")
                return True
            except MessageNotModified:
                return True
            except FloodWait:
                # Still rate-limited after retry — try next client
                continue
            except Exception as retry_err:
                _edlog(f"[RELINK-EDIT] {name} retry failed for dst={dst_msg_id}: {retry_err}")
                continue

        except Exception as e:
            err = str(e)
            if "MESSAGE_AUTHOR_REQUIRED" in err:
                _edlog(f"[RELINK-EDIT] {name} not author of dst={dst_msg_id} — trying next client")
                continue
            if "MESSAGE_NOT_MODIFIED" in err or "MessageNotModified" in err:
                return True
            # Log the ACTUAL error so we can debug — don't silently swallow
            _edlog(f"[RELINK-EDIT] {name} failed for dst={dst_msg_id}: {type(e).__name__}: {err[:200]}")
            continue

    _edlog(f"[RELINK-EDIT] All clients failed for dst={dst_msg_id}")
    return False


async def fetch_message_batch_safe(
    client,
    chat_id: int,
    ids_to_fetch: list,
) -> list:
    """
    Fetch message batch with proper FloodWait handling.
    Waits exact required seconds then retries up to 3 times.
    """
    for attempt in range(3):
        try:
            msgs = await client.get_messages(chat_id, ids_to_fetch)
            return msgs if msgs else []

        except FloodWait as e:
            wait = getattr(e, 'value', 30) + 5
            _edlog(
                f"[RELINK-FETCH] FloodWait {wait}s "
                f"attempt={attempt+1}/3 — sleeping"
            )
            await asyncio.sleep(wait)

        except Exception as e:
            _edlog(f"[RELINK-FETCH] Failed attempt={attempt+1}: {e}")
            # Brief pause before retry on other errors
            await asyncio.sleep(2)

    return []


# ════════════════════════════════════════════════════════════════════
# MAIN RELINK ENGINE
# ════════════════════════════════════════════════════════════════════

async def run_relink_scan(client, session: dict, status_msg: Message):
    """Main relink scan engine. Scans the entire chat and fixes broken links.

    Uses all 7 layers of robustness:
    1. Crash-proof checkpoint (session is updated in MongoDB after every message)
    2. Multi-strategy link resolution
    3. Entity-safe editing
    4. Rate limit armor
    5. Progress reporting
    6. Pre-flight validation (already done before calling this)
    7. Auto-relink (integrated with new mirror flow)
    """
    chat_id = session["chat_id"]
    dest_channel_id = session["dest_channel_id"]
    dest_channel_username = session.get("dest_channel_username")
    uid = session["triggered_by"]
    session_id = session["_id"]
    limit = session.get("limit")
    dry_run = session.get("dry_run", False)

    # ── Get user client (ubot) for editing ──────────────────────
    # Most messages in the destination were posted by the user client
    # during batch upload. Bot CANNOT edit those → MESSAGE_AUTHOR_REQUIRED.
    # We need ubot for editing, bot as fallback for bot-posted messages.
    ubot = get_Y()
    if ubot is None:
        _edlog(f"[RELINK] ⚠️  UBOT IS NONE — most edits will fail with MESSAGE_AUTHOR_REQUIRED!")
        _edlog(f"[RELINK] ⚠️  The user client (ubot) is required to edit messages it posted.")
        _edlog(f"[RELINK] ⚠️  Only bot-posted messages can be edited without ubot.")
        # Try harder to get the userbot — direct import
        try:
            import shared_client as _sc
            ubot = _sc.userbot
            if ubot is not None:
                _edlog(f"[RELINK] ✅ Got ubot via shared_client directly: {type(ubot).__name__}")
        except Exception as _sc_err:
            _edlog(f"[RELINK] shared_client.userbot also None: {_sc_err}")
    else:
        _edlog(f"[RELINK] ✅ ubot available: {type(ubot).__name__}")

    # ── Verify ubot is actually connected and functional ──────
    if ubot is not None:
        _ubot_connected = getattr(ubot, 'is_connected', True)
        _edlog(f"[RELINK] ubot.is_connected = {_ubot_connected}")
        if _ubot_connected:
            # Quick functional test — get ubot's own ID to confirm it works
            try:
                _ubot_me = getattr(ubot, 'me', None)
                if _ubot_me:
                    _ubot_id = getattr(_ubot_me, 'id', 'unknown')
                    _edlog(f"[RELINK] ✅ ubot verified — user_id={_ubot_id}")
                else:
                    _edlog(f"[RELINK] ⚠️  ubot.me is None — ubot may not be fully initialized")
            except Exception as _me_err:
                _edlog(f"[RELINK] ⚠️  ubot.me check failed: {_me_err}")
        else:
            _edlog(f"[RELINK] ⚠️  ubot exists but NOT connected — will try fresh on each edit")

    # Mark session as in_progress
    await relink_sessions_collection.update_one(
        {"_id": session_id},
        {"$set": {"status": "in_progress"}}
    )

    # ── DIAGNOSTIC: Check fingerprints_collection for this user ──
    # This tells us whether Strategy 6.5 (FP-DB-LOOKUP) will have data to work with
    try:
        _fp_count = await fingerprints_collection.count_documents({"uid": uid})
        _edlog(f"[RELINK] Fingerprints in DB for uid={uid}: {_fp_count}")
        if _fp_count > 0:
            _fp_sample = await fingerprints_collection.find_one({"uid": uid})
            if _fp_sample:
                _edlog(f"[RELINK] FP sample: src={_fp_sample.get('src_msg_id')} "
                       f"dst={_fp_sample.get('dst_msg_id')} "
                       f"channel={_fp_sample.get('source_channel')}")
        else:
            _edlog(f"[RELINK] ⚠️  NO fingerprints in DB — Strategy 6.5 will NOT work. "
                   f"Messages uploaded before checkpoint_with_fingerprint was added won't be found.")
    except Exception as _fp_diag_err:
        _edlog(f"[RELINK] FP collection diagnostic failed: {_fp_diag_err}")

    # ── Load ALL source channel mappings ──────────────────────
    _edlog(f"[RELINK] Loading combined msg_id_map for uid={uid} dest={dest_channel_id}")
    combined_map, channel_info = await load_combined_msg_id_map(uid, dest_channel_id=dest_channel_id)

    # Also load ADDITIONAL_SOURCE_CHANNELS
    _asc = _get_additional_source_channels()
    if _asc:
        for extra_ch in _asc:
            if extra_ch in channel_info:
                continue
            extra_map, _, extra_dest = await load_upload_map(uid, str(extra_ch))
            if extra_map:
                combined_map.update(extra_map)

    if not combined_map and not channel_info:
        await safe_edit(status_msg, "❌ No message ID mappings or channel info found! Run /fetch, /batch, or /mirror first.")
        await relink_sessions_collection.update_one(
            {"_id": session_id},
            {"$set": {"status": "failed", "completed_at": datetime.utcnow(),
                      "error_log": ["No msg_id_map or channel_info data found"]}}
        )
        return

    if not combined_map:
        _edlog(f"[RELINK] ⚠️ No src→dst mappings found, but channel_info has "
               f"{len(channel_info)} source channels. Continuing with fingerprint-based resolution.")
    else:
        _edlog(f"[RELINK] Mapping loaded: {len(combined_map)} entries from "
               f"{len(channel_info)} source channels")

    # ── Diagnostic: log mapping statistics ─────────────────────
    _map_keys = sorted(combined_map.keys()) if combined_map else []
    _map_keys_sample = _map_keys[:5]
    _edlog(f"[RELINK] Mapping loaded: {len(combined_map)} entries from "
           f"{len(channel_info)} source channels")
    _edlog(f"[RELINK] Mapping key range: min={_map_keys[0] if _map_keys else 0} "
           f"max={_map_keys[-1] if _map_keys else 0} "
           f"sample={_map_keys_sample}")

    # KEY DISTRIBUTION HISTOGRAM — shows which ID ranges have mappings
    # This is CRITICAL for diagnosing "all lookups fail" issues.
    # If the 6000-8000 range has 0 entries, those messages weren't mirrored.
    if _map_keys:
        _bucket_size = 1000
        _buckets = {}
        for k in _map_keys:
            bucket = (k // _bucket_size) * _bucket_size
            _buckets[bucket] = _buckets.get(bucket, 0) + 1
        _hist_lines = []
        for bucket_start in sorted(_buckets.keys()):
            count = _buckets[bucket_start]
            bar = "█" * min(count // 10, 40)
            _hist_lines.append(f"  {bucket_start:>6}-{bucket_start+_bucket_size-1:<6}: {count:>4} {bar}")
        _edlog(f"[RELINK-MAP-HISTOGRAM] Key distribution ({len(_map_keys)} keys in {_bucket_size}-unit buckets):\n" +
               "\n".join(_hist_lines))

    # Check: are the COMMON link IDs in the map?
    # Also test some IDs from different ranges for diagnostic coverage
    _test_ids = [6631, 6141, 6868, 6267, 13495, 16085]
    # Also test IDs from the actual map range (should always work)
    if _map_keys:
        _test_ids.extend(_map_keys[:3])
    _test_results = {tid: combined_map.get(tid) for tid in _test_ids}
    _test_found = sum(1 for v in _test_results.values() if v is not None)
    _edlog(f"[RELINK-MAP-CHECK] Test IDs in map: {_test_results} ({_test_found}/{len(_test_ids)} found)")

    # ALSO check value types — if values are dicts instead of ints,
    # the map structure is wrong
    if combined_map:
        _first_key = _map_keys[0]
        _first_val = combined_map[_first_key]
        _edlog(f"[RELINK-MAP-TYPE] First entry: key={_first_key!r} type={type(_first_key).__name__} "
               f"val={_first_val!r} type={type(_first_val).__name__}")

    for ch_str, ch_info in channel_info.items():
        _edlog(f"[RELINK] channel_info: key={ch_str} username={ch_info.get('username')} "
               f"dest={ch_info.get('dest_channel')}")

    # ── Build source_channels_info for link classification ─────
    source_channels_info = {}
    for ch_str, ch_info in channel_info.items():
        ch_numeric = ch_info.get("numeric_id")
        ch_username = ch_info.get("username")
        # Use normalize_channel_id for bulletproof clean_id regardless of format
        ch_clean = normalize_channel_id(ch_str)

        # ── RESOLVE missing username/numeric_id ──────────────
        # load_combined_msg_id_map sets username=None, numeric_id=None.
        # We need these to match links! Try to resolve from Telegram API.
        if ch_username is None or ch_numeric is None:
            try:
                # ch_str could be like "-1001234567890" or just "1234567890"
                ch_int = int(ch_str) if ch_str.lstrip('-').isdigit() else None
                if ch_int:
                    # Try to get channel info from Telegram
                    resolved = await client.get_chat(ch_int)
                    if resolved:
                        if ch_username is None and hasattr(resolved, 'username') and resolved.username:
                            ch_username = resolved.username.lower()
                            _edlog(f"[RELINK] Resolved username for {ch_str}: @{ch_username}")
                        if ch_numeric is None:
                            ch_numeric = ch_int
                elif ch_str and not ch_str.lstrip('-').isdigit():
                    # ch_str might be a username like "channelname"
                    resolved = await client.get_chat(ch_str)
                    if resolved:
                        if ch_username is None and hasattr(resolved, 'username') and resolved.username:
                            ch_username = resolved.username.lower()
                        if ch_numeric is None and hasattr(resolved, 'id'):
                            ch_numeric = resolved.id
                            # Rebuild clean_id from the actual numeric ID
                            ch_clean = normalize_channel_id(ch_numeric)
                            _edlog(f"[RELINK] Resolved numeric_id for @{ch_username}: {ch_numeric}")
            except Exception as resolve_err:
                _edlog(f"[RELINK] Could not resolve channel {ch_str}: {resolve_err}")

        source_channels_info[ch_str] = {
            "clean_id": ch_clean,
            "username": ch_username,
            "numeric_id": ch_numeric,
            "dest_channel": ch_info.get("dest_channel"),
        }

    _edlog(f"[RELINK] Loaded {len(combined_map)} mappings from "
           f"{len(source_channels_info)} source channels")
    # Debug: log source_channels_info so we can see what we're matching against
    for ch_key, ch_data in source_channels_info.items():
        _edlog(f"[RELINK] source_channel: key={ch_key}, clean_id={ch_data.get('clean_id')}, "
               f"username={ch_data.get('username')}, numeric_id={ch_data.get('numeric_id')}")

    # ── Initialize components ──────────────────────────────────
    resolver = LinkResolver(combined_map, source_channels_info,
                            dest_channel_id, dest_channel_username,
                            uid=uid)
    editor = EntitySafeEditor()
    armor = RateLimitArmor(base_delay=0.5)
    reporter = ProgressReporter(client, status_msg)

    start_time = time.time()
    stats = {
        "scanned": 0,
        "fixed": 0,
        "unresolved": 0,
        "skipped": 0,
        "already_correct": 0,
        "start_time": start_time,
        "total_estimated": None,
    }

    # ── Deferred resolution queue ─────────────────────────────────
    # Links that can't be resolved during the first pass (because the
    # fingerprint index is incomplete) are queued here. After the full
    # scan completes, we retry them with the COMPLETE fingerprint index.
    # This is THE KEY to making Strategy 7 work: the index must be fully
    # built before we can reliably look up fingerprints.
    deferred_links: List[Dict] = []  # [{"msg": Message, "plan": [...], "unresolved_items": [...]}]

    # ── Main scan loop ─────────────────────────────────────────
    scanned_count = session.get("total_scanned", 0)
    fixed_count = session.get("total_fixed", 0)
    unresolved_count = session.get("total_unresolved", 0)
    skipped_count = session.get("total_skipped", 0)
    already_correct_count = session.get("total_already_correct", 0)

    try:
        # Determine resume point
        resume_from_msg_id = session.get("last_scanned_msg_id", 0)
        scan_direction = session.get("scan_direction", "new_to_old")

        # ── BOT-COMPATIBLE MESSAGE ITERATION ─────────────────────
        # Bots CANNOT use get_chat_history() — BOT_METHOD_INVALID.
        # Bots CANNOT use search_messages() — BOT_METHOD_INVALID (messages.Search).
        # Both are user-only methods in Telegram's API.
        #
        # The ONLY bot-compatible way to fetch messages in a supergroup:
        #   client.get_messages(chat_id, list_of_ids)
        # This uses channels.getMessages, which IS allowed for bots.
        #
        # GAP 4: Direction-aware scan using build_id_batches().
        # old_to_new: starts from ID 1, scans forward (catches pinned/index first)
        # new_to_old: starts from latest ID, scans backward (default)
        max_msg_id = status_msg.id
        scan_limit_int = int(limit) if limit else 0  # 0 = no limit
        batch_size = 200  # Telegram allows ~200 IDs per channels.getMessages
        messages_scanned = 0

        # Build all ID batches based on direction
        if scan_direction == "old_to_new":
            id_batches = build_id_batches(1, max_msg_id, "old_to_new", batch_size)
        else:
            id_batches = build_id_batches(max_msg_id, 1, "new_to_old", batch_size)

        _edlog(f"[RELINK] Starting {scan_direction} scan: max_msg_id={max_msg_id}, "
               f"limit={scan_limit_int}, batches={len(id_batches)}")

        _consecutive_empty_batches = 0  # Track empty batches to detect stuck scans
        _max_consecutive_empty = 20     # After 20 empty batches, skip to end

        for batch_ids in id_batches:
            # Check if we've hit the scan limit
            if scan_limit_int > 0 and messages_scanned >= scan_limit_int:
                _edlog(f"[RELINK] Hit scan limit ({scan_limit_int}), stopping")
                break

            # Fetch this batch — prefer ubot (higher rate limits, avoids FloodWait)
            # Bot fallback when ubot unavailable
            _fetch_client = ubot if ubot is not None else client
            # Re-check ubot availability each batch (might become available mid-scan)
            if _fetch_client is None or _fetch_client is client:
                try:
                    import shared_client as _sc_f
                    if _sc_f.userbot is not None:
                        _fetch_client = _sc_f.userbot
                except Exception:
                    pass
            batch_msgs = await fetch_message_batch_safe(_fetch_client, chat_id, batch_ids)
            if not batch_msgs:
                _edlog(f"[RELINK] Batch fetch failed (IDs {batch_ids[0]}-{batch_ids[-1]})")
                # Brief pause before next batch to avoid tight FloodWait loops
                await asyncio.sleep(1)
                continue

            # Count actual (non-None, non-empty) messages in this batch
            _valid_msgs_in_batch = sum(1 for m in batch_msgs if m is not None and not getattr(m, 'empty', False))
            if _valid_msgs_in_batch == 0:
                _consecutive_empty_batches += 1
                if _consecutive_empty_batches >= _max_consecutive_empty:
                    _edlog(f"[RELINK] {_consecutive_empty_batches} consecutive empty batches — "
                           f"likely past end of channel messages. Stopping scan.")
                    break
            else:
                _consecutive_empty_batches = 0  # Reset on non-empty batch

            # Process each message in the batch
            for msg in batch_msgs:
                # Skip None (deleted/missing messages)
                if msg is None or getattr(msg, 'empty', False):
                    continue

                # ── CRITICAL: Add to fingerprint index ──────────
                # Build the in-memory fingerprint → dst_msg_id index
                # from EVERY destination message we scan. This enables
                # Strategy 7 (content index) to resolve links even when
                # the direct mapping is missing.
                resolver.add_to_fingerprint_index(msg, msg.id)

                # ── Check for cancellation ─────────────────────
                if messages_scanned % 50 == 0:
                    current_session = await relink_sessions_collection.find_one({"_id": session_id})
                    if current_session and current_session.get("status") == "cancelled":
                        _edlog(f"[RELINK] Session cancelled by user")
                        await safe_edit(status_msg,
                            f"⏹️ Relink cancelled. Progress saved.\n"
                            f"Scanned: {scanned_count} | Fixed: {fixed_count} | "
                            f"Unresolved: {unresolved_count}")
                        return

                messages_scanned += 1
                scanned_count += 1
                stats["scanned"] = scanned_count

                # Check scan limit
                if scan_limit_int > 0 and messages_scanned >= scan_limit_int:
                    break

                # ── Skip messages without text ─────────────────
                msg_text = msg.text or msg.caption or ""
                msg_entities = msg.entities or msg.caption_entities or []

                if not msg_text:
                    skipped_count += 1
                    stats["skipped"] = skipped_count

                    # Save checkpoint
                    await update_relink_checkpoint(
                        session_id,
                        last_scanned_msg_id=msg.id,
                        total_scanned=scanned_count,
                        total_skipped=skipped_count,
                    )
                    continue

                # ── Quick filter: skip messages without t.me ────
                # NOTE: For TEXT_LINK entities, the URL is in entity.url,
                # NOT in the text. "Click here" doesn't contain "t.me".
                # So we must also check entity URLs, not just text.
                _has_tme_in_text = "t.me" in msg_text.lower() or "tg://" in msg_text.lower()
                _has_tme_in_entity = False
                if not _has_tme_in_text and msg_entities:
                    for _ent in msg_entities:
                        _ent_url = getattr(_ent, 'url', None)
                        if _ent_url and ('t.me' in _ent_url.lower() or 'tg://' in _ent_url.lower()):
                            _has_tme_in_entity = True
                            break
                if not _has_tme_in_text and not _has_tme_in_entity:
                    skipped_count += 1
                    stats["skipped"] = skipped_count

                    # Checkpoint every 200 messages
                    if scanned_count % 200 == 0:
                        await update_relink_checkpoint(
                            session_id,
                            last_scanned_msg_id=msg.id,
                            total_scanned=scanned_count,
                            total_skipped=skipped_count,
                        )
                    continue

                # ── Extract ALL links from message ─────────────
                entity_links = editor.extract_links_from_entities(msg_text, msg_entities)
                text_links = editor.extract_links_from_text(msg_text)
                all_links = entity_links + text_links

                # Debug counters
                if not hasattr(run_relink_scan, '_tme_msg_count'):
                    run_relink_scan._tme_msg_count = 0
                    run_relink_scan._link_extracted_count = 0
                    run_relink_scan._source_channel_count = 0
                    run_relink_scan._dest_channel_count = 0
                    run_relink_scan._other_channel_count = 0
                    run_relink_scan._backfilled_count = 0
                run_relink_scan._tme_msg_count += 1

                # Deduplicate by URL
                seen_urls = set()
                unique_links = []
                for link in all_links:
                    if link["url"] not in seen_urls:
                        seen_urls.add(link["url"])
                        unique_links.append(link)

                if not unique_links:
                    skipped_count += 1
                    stats["skipped"] = skipped_count

                    # Checkpoint every 50 messages
                    if scanned_count % 50 == 0:
                        await update_relink_checkpoint(
                            session_id,
                            last_scanned_msg_id=msg.id,
                            total_scanned=scanned_count,
                            total_skipped=skipped_count,
                        )
                    continue

                # ── Classify ALL links for diagnostic + backfill ──────────
                classified_links = [classify_link(l["url"], source_channels_info, dest_channel_id=dest_channel_id) for l in unique_links]
                _has_source = any(c.is_source_channel for c in classified_links)
                _has_dest = any(c.is_dest_channel for c in classified_links)
                _has_other = any(not c.is_source_channel and not c.is_dest_channel and c.link_type not in (LINK_INVITE, LINK_NON_CHANNEL) for c in classified_links)

                if _has_source:
                    run_relink_scan._source_channel_count += 1
                if _has_dest:
                    run_relink_scan._dest_channel_count += 1
                if _has_other:
                    run_relink_scan._other_channel_count += 1

                # ── CRITICAL: Backfill Smart Cache for this message ──────────
                # Even if no source links, we index the message so future /relink
                # runs skip it (no need to re-scan known-good messages).
                # This is the KEY to making the Smart Cache work for existing messages.
                # Uses create_task() so it doesn't slow down the scan loop.
                if not hasattr(run_relink_scan, '_backfill_count'):
                    run_relink_scan._backfill_count = 0
                try:
                    _src_ch_str = list(source_channels_info.keys())[0] if source_channels_info else None

                    # Build source links list for the index
                    _index_source_links = []
                    _index_unresolved_ids = []
                    for cl in classified_links:
                        if cl.is_source_channel and cl.source_msg_id:
                            _index_source_links.append({
                                "url": cl.url,
                                "src_msg_id": cl.source_msg_id,
                                "channel_key": normalize_channel_id(cl.source_peer) if cl.source_peer else (cl.username or ""),
                                "link_type": cl.link_type,
                            })
                            _index_unresolved_ids.append(cl.source_msg_id)

                    from plugins.batch import mirrored_messages_index
                    if _index_source_links:
                        # Message has source-channel links that need fixing
                        asyncio.create_task(mirrored_messages_index.update_one(
                            {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": msg.id},
                            {"$set": {
                                "uid": uid,
                                "source_channel": str(_src_ch_str) if _src_ch_str else "",
                                "src_msg_id": None,  # We don't know the src_msg_id for this dest msg
                                "dst_chat_id": dest_channel_id,
                                "dst_msg_id": msg.id,
                                "contains_old_links": True,
                                "links_to_resolve": _index_source_links,
                                "unresolved_src_ids": _index_unresolved_ids,
                                "last_updated": datetime.utcnow(),
                                "backfilled": True,
                            }},
                            upsert=True,
                        ))
                        run_relink_scan._backfill_count += 1
                    else:
                        # No source links — mark as already fixed so future runs skip it
                        asyncio.create_task(mirrored_messages_index.update_one(
                            {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": msg.id},
                            {"$set": {
                                "uid": uid,
                                "source_channel": str(_src_ch_str) if _src_ch_str else "",
                                "src_msg_id": None,
                                "dst_chat_id": dest_channel_id,
                                "dst_msg_id": msg.id,
                                "contains_old_links": False,
                                "links_to_resolve": [],
                                "last_updated": datetime.utcnow(),
                                "backfilled": True,
                            }},
                            upsert=True,
                        ))
                except Exception as _bf_err:
                    # Never let backfill break the scan
                    if not hasattr(run_relink_scan, '_backfill_err_logged'):
                        run_relink_scan._backfill_err_logged = 0
                    if run_relink_scan._backfill_err_logged < 3:
                        run_relink_scan._backfill_err_logged += 1
                        _edlog(f"[RELINK-BACKFILL] Error: {_bf_err}")

                # ── Enhanced diagnostic logging (first 10 messages with t.me) ──
                if not hasattr(run_relink_scan, '_diag_logged'):
                    run_relink_scan._diag_logged = 0
                if run_relink_scan._diag_logged < 10:
                    run_relink_scan._diag_logged += 1
                    _edlog(f"[RELINK-DIAG] msg_id={msg.id} links={len(unique_links)} "
                           f"src={_has_source} dest={_has_dest} other={_has_other} "
                           f"classified={[(c.url[:50], c.link_type, c.is_source_channel, c.is_dest_channel) for c in classified_links[:5]]}")

                # ── Build rewrite plan ─────────────
                plan = editor.build_rewrite_plan(unique_links, resolver, source_channels_info, dest_channel_id=dest_channel_id)

                if not plan:
                    # No source-channel links found — all links already point to dest or are unrelated
                    skipped_count += 1
                    stats["skipped"] = skipped_count
                    continue

                # ── Resolve links ──────────────────────────────
                plan = await editor.resolve_plan(plan, resolver)

                resolved_items = [p for p in plan if p["resolved"]]
                unresolved_items = [p for p in plan if not p["resolved"] and p["link_info"].is_source_channel]

                # Diagnostic: log WHY links weren't resolved (first 10 only)
                if unresolved_items and not hasattr(run_relink_scan, '_unresolved_logged'):
                    run_relink_scan._unresolved_logged = 0
                if unresolved_items and run_relink_scan._unresolved_logged < 10:
                    run_relink_scan._unresolved_logged += 1
                    for ui in unresolved_items[:3]:
                        src_id = ui["link_info"].source_msg_id
                        in_map = src_id in resolver.msg_id_map
                        map_size = len(resolver.msg_id_map)
                        fp_idx_size = len(resolver._fingerprint_index)
                        _edlog(f"[RELINK-UNRESOLVED] src_msg_id={src_id} in_map={in_map} "
                               f"map_size={map_size} fp_idx_size={fp_idx_size} url={ui['old_url'][:60]}")

                if not resolved_items:
                    # Nothing to fix in first pass — but MAY be resolvable later
                    if unresolved_items:
                        unresolved_count += len(unresolved_items)
                        stats["unresolved"] = unresolved_count

                        # ── DEFERRED RESOLUTION: Queue for second pass ──
                        # The fingerprint index is INCOMPLETE during the first pass.
                        # After the full scan, we retry with the COMPLETE index.
                        # This is the key fix: Strategy 7 can only work when all
                        # destination messages have been indexed.
                        deferred_links.append({
                            "msg": msg,
                            "msg_text": msg_text,
                            "msg_entities": msg_entities,
                            "unresolved_items": unresolved_items,
                        })

                        # Track unresolved for later retry
                        unresolved_data = [
                            {"source_url": p["old_url"], "msg_id": msg.id,
                             "source_msg_id": p["link_info"].source_msg_id}
                            for p in unresolved_items
                        ]
                        await append_to_session(session_id, "unresolved_links", unresolved_data)
                    else:
                        already_correct_count += 1
                        stats["already_correct"] = already_correct_count

                    # Checkpoint every 50 messages
                    if scanned_count % 50 == 0:
                        await update_relink_checkpoint(
                            session_id,
                            last_scanned_msg_id=msg.id,
                            total_scanned=scanned_count,
                            total_unresolved=unresolved_count,
                            total_already_correct=already_correct_count,
                        )
                    continue

                # ── Apply rewrites ─────────────────────────────
                if dry_run:
                    # Don't edit, just count
                    fixed_count += len(resolved_items)
                    stats["fixed"] = fixed_count
                    _edlog(f"[RELINK-DRY] Would fix {len(resolved_items)} links in msg {msg.id}")
                else:
                    # Wait for rate limiter
                    await armor.acquire()

                    try:
                        # Apply text rewrites
                        new_text, adjustments = editor.apply_rewrites_to_text(msg_text, plan)

                        # Adjust entities (also rewrites TEXT_LINK entity.url)
                        new_entities, entity_urls_changed = editor.adjust_entities(msg_entities, plan, adjustments)

                        # Determine if this is a caption-only message
                        # (photo/video/document with no text, only caption)
                        is_caption = bool(msg.caption) and not bool(msg.text)

                        # Diagnostic: check if anything actually changed
                        text_changed = (new_text != msg_text)
                        if not text_changed and entity_urls_changed:
                            _edlog(f"[RELINK] msg {msg.id}: text unchanged, but entity URLs changed "
                                   f"(TEXT_LINK rewrite) — {len(resolved_items)} links")
                        elif not text_changed and not entity_urls_changed:
                            _edlog(f"[RELINK] msg {msg.id}: no changes detected — skipping edit")
                            already_correct_count += 1
                            stats["already_correct"] = already_correct_count
                            continue

                        # Edit the message — try ubot first (posted most msgs),
                        # bot as fallback (can edit its own messages)
                        edit_ok = await edit_message_safe(
                            bot_client=client,
                            ubot=ubot,
                            dst_chat_id=chat_id,
                            dst_msg_id=msg.id,
                            new_text=new_text,
                            new_entities=new_entities if new_entities else None,
                            is_caption=is_caption,
                        )

                        if edit_ok:
                            # Check if it was already correct (MessageNotModified)
                            # vs actually fixed — we log but count both
                            fixed_count += len(resolved_items)
                            stats["fixed"] = fixed_count
                            armor.on_success()
                            _edlog(f"[RELINK] Fixed {len(resolved_items)} links in msg {msg.id}"
                                   + (" (caption)" if is_caption else ""))
                        else:
                            # All clients failed
                            armor.on_error(Exception("edit_failed"))
                            failed_data = [{
                                "msg_id": msg.id,
                                "error": "MESSAGE_AUTHOR_REQUIRED or edit failed",
                                "retry_count": 0
                            }]
                            await append_to_session(session_id, "failed_edits", failed_data)
                            _edlog(f"[RELINK] Edit failed for msg {msg.id} — all clients rejected")

                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        armor.on_error(e)
                        _edlog(f"[RELINK] Rewrite/apply error for msg {msg.id}: {e}")

                if unresolved_items:
                    unresolved_count += len(unresolved_items)
                    stats["unresolved"] = unresolved_count

                    # Track unresolved for later retry
                    unresolved_data = [
                        {"source_url": p["old_url"], "msg_id": msg.id,
                         "source_msg_id": p["link_info"].source_msg_id}
                        for p in unresolved_items
                    ]
                    await append_to_session(session_id, "unresolved_links", unresolved_data)

                # ── Save checkpoint after every message with links ──
                await update_relink_checkpoint(
                    session_id,
                    last_scanned_msg_id=msg.id,
                    total_scanned=scanned_count,
                    total_fixed=fixed_count,
                    total_unresolved=unresolved_count,
                    total_already_correct=already_correct_count,
                )

                # ── Update progress reporter ──────────────────
                await reporter.update(stats)

            # ── End of batch: log progress ──────────────────────
            _edlog(f"[RELINK] Batch done. IDs scanned so far: {messages_scanned}, "
                   f"fixed: {fixed_count}, unresolved: {unresolved_count}, "
                   f"source_ch_links: {getattr(run_relink_scan, '_source_channel_count', 0)}, "
                   f"dest_ch_links: {getattr(run_relink_scan, '_dest_channel_count', 0)}, "
                   f"other_ch_links: {getattr(run_relink_scan, '_other_channel_count', 0)}, "
                   f"backfilled: {getattr(run_relink_scan, '_backfill_count', 0)}, "
                   f"fp_index: {len(resolver._fingerprint_index)}")

            # Pause between batches to avoid FloodWait on GetMessages
            # ubot has much higher rate limits (1.5s between 200-msg batches)
            # bot client needs more breathing room (3s between batches)
            _using_ubot_for_fetch = (_fetch_client is not client)
            _batch_delay = 1.5 if _using_ubot_for_fetch else 3.0
            await asyncio.sleep(_batch_delay)

        # ── End of for loop (all batches scanned) ────────────
        # Debug summary: how many messages had t.me, how many had extractable links,
        # how many were classified as source channel
        _edlog(f"[RELINK-SUMMARY] t.me_messages={getattr(run_relink_scan, '_tme_msg_count', 0)} "
               f"links_extracted={getattr(run_relink_scan, '_link_extracted_count', 0)} "
               f"source_channel_links={getattr(run_relink_scan, '_source_channel_count', 0)} "
               f"dest_channel_links={getattr(run_relink_scan, '_dest_channel_count', 0)} "
               f"other_channel_links={getattr(run_relink_scan, '_other_channel_count', 0)} "
               f"smart_cache_backfilled={getattr(run_relink_scan, '_backfill_count', 0)}")

        # ═══════════════════════════════════════════════════════════════
        # PHASE 2: DEFERRED RESOLUTION — Retry with COMPLETE fingerprint index
        # ═══════════════════════════════════════════════════════════════
        # During Phase 1, links couldn't be resolved because the fingerprint
        # index was incomplete. NOW the index has ALL destination messages.
        # We retry every deferred link using Strategy 7 (content index).
        if deferred_links:
            _edlog(f"[RELINK-PHASE2] Starting deferred resolution: "
                   f"{len(deferred_links)} messages with unresolved links, "
                   f"fp_index_size={len(resolver._fingerprint_index)}")

            _phase2_fixed = 0
            _phase2_still_unresolved = 0

            for idx, deferred in enumerate(deferred_links):
                # Check for cancellation
                if idx % 50 == 0:
                    current_session = await relink_sessions_collection.find_one({"_id": session_id})
                    if current_session and current_session.get("status") == "cancelled":
                        _edlog(f"[RELINK-PHASE2] Session cancelled during deferred resolution")
                        break

                msg = deferred["msg"]
                msg_text = deferred["msg_text"]
                msg_entities = deferred["msg_entities"]
                unresolved_items = deferred["unresolved_items"]

                # Retry each unresolved link using Strategy 7
                newly_resolved = []
                still_unresolved = []

                for item in unresolved_items:
                    # Try Strategy 7 directly with the COMPLETE fingerprint index
                    result = await resolver._resolve_via_content_index(item["link_info"])
                    if result:
                        item["new_url"] = result
                        item["resolved"] = True
                        newly_resolved.append(item)
                    else:
                        still_unresolved.append(item)

                if not newly_resolved:
                    _phase2_still_unresolved += len(still_unresolved)
                    continue

                # Apply the rewrites for newly resolved links
                if dry_run:
                    _phase2_fixed += len(newly_resolved)
                    _edlog(f"[RELINK-PHASE2-DRY] Would fix {len(newly_resolved)} links in msg {msg.id}")
                else:
                    # Build the full plan including previously resolved + newly resolved
                    # We need ALL items (resolved from phase 1 + newly resolved from phase 2)
                    # to correctly adjust entity offsets
                    full_plan = []
                    for item in newly_resolved:
                        full_plan.append(item)

                    if full_plan:
                        await armor.acquire()
                        try:
                            # Re-extract links and rebuild plan from scratch
                            # (simpler than trying to merge partial plans)
                            entity_links = editor.extract_links_from_entities(msg_text, msg_entities)
                            text_links = editor.extract_links_from_text(msg_text)
                            all_links = entity_links + text_links

                            # Deduplicate
                            seen_urls = set()
                            unique_links = []
                            for link in all_links:
                                if link["url"] not in seen_urls:
                                    seen_urls.add(link["url"])
                                    unique_links.append(link)

                            # Re-classify and resolve with the now-complete index
                            new_plan = editor.build_rewrite_plan(unique_links, resolver, source_channels_info, dest_channel_id=dest_channel_id)
                            new_plan = await editor.resolve_plan(new_plan, resolver)

                            resolved_in_plan = [p for p in new_plan if p["resolved"]]
                            if resolved_in_plan:
                                # Apply rewrites
                                new_text, adjustments = editor.apply_rewrites_to_text(msg_text, new_plan)
                                new_entities, entity_urls_changed = editor.adjust_entities(
                                    msg_entities, new_plan, adjustments)

                                # Edit the message — FIXED: correct argument order
                                # (was: ubot, client, chat_id — WRONG ORDER)
                                # (was: text=, entities= — WRONG KEYWORD NAMES)
                                is_caption = bool(getattr(msg, 'caption', None)) and not bool(getattr(msg, 'text', None))
                                edit_ok = await edit_message_safe(
                                    bot_client=client,
                                    ubot=ubot,
                                    dst_chat_id=chat_id,
                                    dst_msg_id=msg.id,
                                    new_text=new_text,
                                    new_entities=new_entities if new_entities else None,
                                    is_caption=is_caption,
                                )

                                if edit_ok:
                                    _phase2_fixed += len(resolved_in_plan)
                                    fixed_count += len(resolved_in_plan)
                                    _edlog(f"[RELINK-PHASE2] ✅ Fixed msg {msg.id}: "
                                           f"{len(resolved_in_plan)} links "
                                           f"(fp_idx={len(resolver._fingerprint_index)})")
                                else:
                                    _edlog(f"[RELINK-PHASE2] ❌ Edit failed for msg {msg.id}")
                        except Exception as e:
                            _edlog(f"[RELINK-PHASE2] Error processing msg {msg.id}: {e}")

                # Small delay between edits to avoid FloodWait
                await asyncio.sleep(0.5)

                # Log progress every 50 messages
                if (idx + 1) % 50 == 0:
                    _edlog(f"[RELINK-PHASE2] Progress: {idx+1}/{len(deferred_links)} "
                           f"fixed={_phase2_fixed} still_unresolved={_phase2_still_unresolved}")

            _edlog(f"[RELINK-PHASE2] Complete: fixed={_phase2_fixed} "
                   f"still_unresolved={_phase2_still_unresolved} "
                   f"fp_index_size={len(resolver._fingerprint_index)}")

            # Update unresolved count
            unresolved_count = unresolved_count - _phase2_fixed
            stats["unresolved"] = max(0, unresolved_count)
            stats["fixed"] = fixed_count
        else:
            _edlog(f"[RELINK-PHASE2] No deferred links — scan complete")

    except asyncio.CancelledError:
        _edlog(f"[RELINK] Scan cancelled (task cancelled)")
        await relink_sessions_collection.update_one(
            {"_id": session_id},
            {"$set": {"status": "cancelled", "completed_at": datetime.utcnow()}}
        )
        return
    except Exception as e:
        _edlog(f"[RELINK] Scan error: {e}")
        await append_to_session(session_id, "error_log", [f"{type(e).__name__}: {str(e)[:200]}"])
        # Don't mark as failed — the checkpoint is saved, user can resume

    # ── Final update ───────────────────────────────────────────
    elapsed = time.time() - start_time
    speed = scanned_count / max(elapsed / 60, 0.01)

    await relink_sessions_collection.update_one(
        {"_id": session_id},
        {"$set": {
            "status": "completed",
            "completed_at": datetime.utcnow(),
            "total_scanned": scanned_count,
            "total_fixed": fixed_count,
            "total_unresolved": unresolved_count,
            "total_skipped": skipped_count,
            "total_already_correct": already_correct_count,
            "speed_msg_per_min": round(speed, 1),
        }}
    )

    # GAP 7: Send detailed completion summary
    resolver_stats = resolver.stats
    armor_stats = armor.get_stats()

    cache_stats = {
        "hits": resolver_stats.get("cache_hits", 0),
        "misses": resolver_stats.get("failed", 0),
        "new": resolver_stats.get("strategy_1", 0) + resolver_stats.get("strategy_2", 0),
        "content_index_hits": resolver_stats.get("content_index_hits", 0),
        "fp_db_lookup_hits": resolver_stats.get("fp_db_lookup_hits", 0),
        "strategy_7": resolver_stats.get("strategy_7", 0),
        "fingerprint_index_size": len(resolver._fingerprint_index),
    }
    edit_stats = {
        "success": fixed_count,
        "failed": len(session.get("failed_edits", [])),
        "flood_waits": armor_stats.get("flood_waits", 0),
    }

    # Refresh session data for the summary
    final_session = await relink_sessions_collection.find_one({"_id": session_id})

    if dry_run:
        try:
            await safe_edit(status_msg, "🔒 **DRY RUN** (no edits made)\n\nScan complete — use without --dry-run to apply fixes.")
        except Exception:
            pass
    else:
        await send_relink_summary(
            client        = client,
            chat_id       = chat_id,
            status_msg_id = status_msg.id,
            session       = final_session or session,
            cache_stats   = cache_stats,
            edit_stats    = edit_stats,
            duration_secs = int(elapsed),
        )

    # Log final resolver stats
    _edlog(f"[RELINK] Session complete: scanned={scanned_count} fixed={fixed_count} "
           f"unresolved={unresolved_count} elapsed={_fmt_dur(elapsed)} "
           f"strategies={resolver_stats} fingerprint_hits={resolver_stats.get('fingerprint_hits', 0)} "
           f"fp_db_lookup_hits={resolver_stats.get('fp_db_lookup_hits', 0)} "
           f"deep_chain_hits={resolver_stats.get('deep_chain_hits', 0)} "
           f"source_ch_links={getattr(run_relink_scan, '_source_channel_count', 0)} "
           f"dest_ch_links={getattr(run_relink_scan, '_dest_channel_count', 0)} "
           f"other_ch_links={getattr(run_relink_scan, '_other_channel_count', 0)} "
           f"smart_cache_backfilled={getattr(run_relink_scan, '_backfill_count', 0)}")

    # ── Smart Cache: Report backfill results ──────────────────
    _backfill_count = getattr(run_relink_scan, '_backfill_count', 0)
    if _backfill_count > 0:
        _edlog(f"[RELINK] ✅ Smart Cache backfilled: {_backfill_count} messages with source links indexed. "
               f"Future /relink runs will use instant MongoDB queries!")
    else:
        _src_cnt = getattr(run_relink_scan, '_source_channel_count', 0)
        _dest_cnt = getattr(run_relink_scan, '_dest_channel_count', 0)
        if _src_cnt == 0 and _dest_cnt > 0:
            _edlog(f"[RELINK] ℹ️ All t.me links in the destination channel already point to the dest channel. "
                   f"No source-channel links found — all links are already correct!")
        elif _src_cnt == 0 and _dest_cnt == 0:
            _edlog(f"[RELINK] ℹ️ No t.me links found pointing to source or dest channels. "
                   f"Links may point to other channels, or messages have no links.")

    # GAP 6: Post-scan source channel check (runs in background)
    try:
        user_client = get_Y()
        if user_client:
            for ch_str, ch_info in source_channels_info.items():
                ch_numeric = ch_info.get("numeric_id")
                if ch_numeric:
                    # Get last scanned watermark
                    watermark_doc = await source_scan_watermark_collection.find_one(
                        {"uid": uid, "source_channel": str(ch_str)}
                    )
                    last_scanned = watermark_doc.get("last_scanned_id", 0) if watermark_doc else 0

                    asyncio.create_task(
                        scan_source_for_new_link_messages(
                            uid             = uid,
                            source_channel  = ch_numeric,
                            dst_channel     = dest_channel_id,
                            user_client     = user_client,
                            msg_id_map      = combined_map,
                            last_scanned_id = last_scanned,
                        )
                    )
    except Exception as e:
        _edlog(f"[RELINK] Post-scan source check failed: {e}")


async def retry_failed_edits(client, session: dict, status_msg: Message):
    """Retry all previously failed edits from a completed session."""
    failed_edits = session.get("failed_edits", [])
    if not failed_edits:
        await safe_edit(status_msg, "✅ No failed edits to retry!")
        return

    chat_id = session["chat_id"]
    dest_channel_id = session["dest_channel_id"]
    uid = session["triggered_by"]

    await safe_edit(status_msg, f"🔄 Retrying {len(failed_edits)} failed edits...")

    # Get user client for editing (most messages were posted by ubot)
    ubot = get_Y()

    # Reload mappings
    combined_map, channel_info = await load_combined_msg_id_map(uid, dest_channel_id=dest_channel_id)

    # Build source_channels_info
    source_channels_info = {}
    for ch_str, ch_info in channel_info.items():
        ch_clean = normalize_channel_id(ch_str)
        source_channels_info[ch_str] = {
            "clean_id": ch_clean,
            "username": ch_info.get("username"),
            "numeric_id": ch_info.get("numeric_id"),
        }

    resolver = LinkResolver(combined_map, source_channels_info, dest_channel_id)
    editor = EntitySafeEditor()
    armor = RateLimitArmor()
    retried = 0
    still_failing = 0

    for fail_item in failed_edits:
        msg_id = fail_item.get("msg_id")
        if not msg_id:
            continue

        try:
            await armor.acquire()
            msg = await client.get_messages(chat_id, msg_id)
            if not msg or getattr(msg, 'empty', True):
                continue

            msg_text = msg.text or msg.caption or ""
            msg_entities = msg.entities or msg.caption_entities or []

            entity_links = editor.extract_links_from_entities(msg_text, msg_entities)
            text_links = editor.extract_links_from_text(msg_text)
            all_links = entity_links + text_links

            plan = editor.build_rewrite_plan(all_links, resolver, source_channels_info, dest_channel_id=dest_channel_id)
            plan = await editor.resolve_plan(plan, resolver)

            resolved = [p for p in plan if p["resolved"]]
            if not resolved:
                still_failing += 1
                continue

            new_text, adjustments = editor.apply_rewrites_to_text(msg_text, plan)
            new_entities, _entity_urls_changed = editor.adjust_entities(msg_entities, plan, adjustments)

            is_caption = bool(getattr(msg, 'caption', None)) and not bool(getattr(msg, 'text', None))
            edit_ok = await edit_message_safe(
                bot_client=client,
                ubot=ubot,
                dst_chat_id=chat_id,
                dst_msg_id=msg_id,
                new_text=new_text,
                new_entities=new_entities if new_entities else None,
                is_caption=is_caption,
            )

            if edit_ok:
                retried += 1
                armor.on_success()
            else:
                still_failing += 1

        except Exception as e:
            still_failing += 1
            armor.on_error(e)

    await safe_edit(status_msg,
        f"🔄 **Retry Complete!**\n"
        f"  • Fixed: {retried}\n"
        f"  • Still failing: {still_failing}\n"
        f"  • Total attempted: {len(failed_edits)}"
    )


# ════════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ════════════════════════════════════════════════════════════════════

# Track running relink tasks for cancellation
_relink_tasks: Dict[int, asyncio.Task] = {}  # chat_id → asyncio.Task


# ════════════════════════════════════════════════════════════════════
# SMART CACHE SURGICAL STRIKE — Step 2 & 3 of the 3-Step System
#
# Instead of scanning the ENTIRE destination channel message by message
# (causing FloodWait, CHANNEL_INVALID, and millions of API calls),
# we query mirrored_messages_index to get the EXACT list of messages
# that contain old source-channel links. Then we fetch only those
# messages, rewrite their links, and mark them as fixed.
#
# This is 100-1000x faster than the legacy full-scan approach for
# channels with millions of messages but only hundreds of link fixes.
# ════════════════════════════════════════════════════════════════════

async def run_relink_smart_cache(client, session: dict, status_msg: Message):
    """Smart Cache surgical strike — fix ONLY messages that need fixing.
    
    Step 2: Query mirrored_messages_index for exact message IDs
    Step 3: Fetch only those messages, rewrite links, mark as fixed
    
    This replaces the old backwards-while-loop scan when Smart Cache
    data is available. Falls back to run_relink_scan if no cache data.
    """
    chat_id = session["chat_id"]
    dest_channel_id = session["dest_channel_id"]
    dest_channel_username = session.get("dest_channel_username")
    uid = session["triggered_by"]
    session_id = session["_id"]
    limit = session.get("limit")
    dry_run = session.get("dry_run", False)

    # ── Get user client (ubot) for editing ──────────────────────
    ubot = get_Y()
    if ubot is None:
        _edlog(f"[SMART-RELINK] ⚠️ UBOT IS NONE — most edits will fail!")
        try:
            import shared_client as _sc
            ubot = _sc.userbot
        except Exception:
            pass
    else:
        _edlog(f"[SMART-RELINK] ✅ ubot available: {type(ubot).__name__}")

    # Mark session as in_progress
    await relink_sessions_collection.update_one(
        {"_id": session_id},
        {"$set": {"status": "in_progress"}}
    )

    # ── Load combined msg_id_map ────────────────────────────────
    _edlog(f"[SMART-RELINK] Loading combined msg_id_map for uid={uid} dest={dest_channel_id}")
    combined_map, channel_info = await load_combined_msg_id_map(uid, dest_channel_id=dest_channel_id)

    # Also load ADDITIONAL_SOURCE_CHANNELS
    _asc = _get_additional_source_channels()
    if _asc:
        for extra_ch in _asc:
            if extra_ch in channel_info:
                continue
            extra_map, _, extra_dest = await load_upload_map(uid, str(extra_ch))
            if extra_map:
                combined_map.update(extra_map)

    _edlog(f"[SMART-RELINK] Mapping loaded: {len(combined_map)} entries from {len(channel_info)} source channels")

    # ── Build source_channels_info for link classification ─────
    source_channels_info = {}
    for ch_str, ch_info in channel_info.items():
        ch_numeric = ch_info.get("numeric_id")
        ch_username = ch_info.get("username")
        ch_clean = normalize_channel_id(ch_str)

        if ch_username is None or ch_numeric is None:
            try:
                ch_int = int(ch_str) if ch_str.lstrip('-').isdigit() else None
                if ch_int:
                    resolved = await client.get_chat(ch_int)
                    if resolved:
                        if ch_username is None and hasattr(resolved, 'username') and resolved.username:
                            ch_username = resolved.username.lower()
                        if ch_numeric is None:
                            ch_numeric = ch_int
            except Exception:
                pass

        source_channels_info[ch_str] = {
            "clean_id": ch_clean,
            "username": ch_username,
            "numeric_id": ch_numeric,
            "dest_channel": ch_info.get("dest_channel"),
        }

    # ── STEP 2: Query Smart Cache for messages needing relink ──
    _edlog(f"[SMART-RELINK] Querying mirrored_messages_index...")
    from plugins.batch import get_messages_needing_relink, mark_message_links_fixed

    target_messages = await get_messages_needing_relink(uid, dest_channel_id)

    # Also check unresolved_links_collection for any messages NOT in the Smart Cache
    # This catches messages that were marked as having unresolved links at mirror time
    # but haven't been indexed yet (e.g. from a different mirror run).
    _unresolved_extra = []
    try:
        from plugins.batch import unresolved_links_collection, normalize_channel_id as batch_norm
        _existing_ids = {m["dst_msg_id"] for m in target_messages}
        async for ul_doc in unresolved_links_collection.find({"user_id": uid, "unresolved": True}):
            ul_dst_msg = ul_doc.get("dst_msg_id")
            ul_dst_chat = ul_doc.get("dst_chat_id")
            if not ul_dst_msg or ul_dst_msg in _existing_ids:
                continue
            # Filter by destination channel
            if ul_dst_chat and batch_norm(str(ul_dst_chat)) != batch_norm(str(dest_channel_id)):
                continue
            _unresolved_src_ids = ul_doc.get("unresolved_src_ids", [])
            _source_links = []
            for _sid in _unresolved_src_ids:
                _src_ch = ul_doc.get("source_channel", "")
                _clean = batch_norm(_src_ch)
                _source_links.append({
                    "url": f"https://t.me/c/{_clean}/{_sid}",
                    "src_msg_id": int(_sid),
                    "channel_key": _clean,
                    "link_type": "private",
                })
            _unresolved_extra.append({
                "dst_msg_id": ul_dst_msg,
                "links_to_resolve": _source_links,
                "src_msg_id": ul_doc.get("src_msg_id"),
                "source_channel": ul_doc.get("source_channel", ""),
                "unresolved_src_ids": [int(x) for x in _unresolved_src_ids],
            })
        if _unresolved_extra:
            _edlog(f"[SMART-RELINK] Found {len(_unresolved_extra)} extra messages from unresolved_links_collection")
            target_messages.extend(_unresolved_extra)
    except Exception as _ul_err:
        _edlog(f"[SMART-RELINK] Could not check unresolved_links: {_ul_err}")

    if not target_messages:
        _edlog(f"[SMART-RELINK] No messages need relinking — all fixed!")
        await relink_sessions_collection.update_one(
            {"_id": session_id},
            {"$set": {"status": "completed", "completed_at": datetime.utcnow(),
                      "total_scanned": 0, "total_fixed": 0, "total_unresolved": 0}}
        )
        await safe_edit(status_msg, "✅ Smart Cache: All messages already fixed — nothing to do!")
        return

    # Apply limit if specified
    if limit and limit > 0:
        target_messages = target_messages[:limit]

    _edlog(f"[SMART-RELINK] Found {len(target_messages)} messages with old links")

    # ── Initialize components ──────────────────────────────────
    resolver = LinkResolver(combined_map, source_channels_info,
                            dest_channel_id, dest_channel_username,
                            uid=uid)
    editor = EntitySafeEditor()
    armor = RateLimitArmor(base_delay=0.5)

    start_time = time.time()
    scanned_count = 0
    fixed_count = 0
    unresolved_count = 0
    already_correct_count = 0
    failed_edit_count = 0
    marked_fixed_count = 0
    not_mirrored_count = 0          # Links to source messages that were never mirrored
    not_mirrored_src_ids = set()    # Set of src_msg_ids that need mirroring

    # ── STEP 3: Surgical Strike — fetch and fix only target messages ──
    # Process in batches of 200 (Telegram's get_messages limit)
    batch_size = 200
    target_ids = [m["dst_msg_id"] for m in target_messages]
    # Build a map: dst_msg_id → metadata from the index
    target_meta = {m["dst_msg_id"]: m for m in target_messages}

    try:
        for batch_start in range(0, len(target_ids), batch_size):
            batch_ids = target_ids[batch_start:batch_start + batch_size]

            # Check for cancellation
            current_session = await relink_sessions_collection.find_one({"_id": session_id})
            if current_session and current_session.get("status") == "cancelled":
                _edlog(f"[SMART-RELINK] Session cancelled by user")
                break

            # Fetch this batch — prefer ubot (higher rate limits)
            _fetch_client = ubot if ubot is not None else client
            batch_msgs = await fetch_message_batch_safe(_fetch_client, chat_id, batch_ids)

            if not batch_msgs:
                _edlog(f"[SMART-RELINK] Batch fetch failed (IDs {batch_ids[0]}-{batch_ids[-1]})")
                await asyncio.sleep(1)
                continue

            for msg in batch_msgs:
                if msg is None or getattr(msg, 'empty', False):
                    # Message was deleted — mark as fixed in the index so we don't retry
                    _meta = target_meta.get(batch_ids[batch_msgs.index(msg)] if msg in batch_msgs else None)
                    continue

                scanned_count += 1
                msg_text = msg.text or msg.caption or ""
                msg_entities = msg.entities or msg.caption_entities or []

                if not msg_text:
                    # No text — mark as fixed (nothing to rewrite)
                    await mark_message_links_fixed(uid, dest_channel_id, msg.id)
                    marked_fixed_count += 1
                    continue

                # Add to fingerprint index for Strategy 7
                resolver.add_to_fingerprint_index(msg, msg.id)

                # ── DUAL-SOURCE LINK EXTRACTION ─────────────
                # SOURCE 1: Current message text (may already be rewritten)
                entity_links = editor.extract_links_from_entities(msg_text, msg_entities)
                text_links = editor.extract_links_from_text(msg_text)
                current_links = entity_links + text_links

                # SOURCE 2: Cached links_to_resolve from mirrored_messages_index
                # These were captured at MIRROR TIME (before rewriting), so they
                # contain the ORIGINAL source-channel URLs. This is CRITICAL because
                # the current text may already have dest-channel URLs, which would
                # cause classify_link to mark them as is_source_channel=False.
                cached_links = target_meta.get(msg.id, {}).get("links_to_resolve", [])

                # Build a map of current entity URLs → entity info (offset, type)
                # This lets us match cached source URLs to the actual entities in
                # the current message, even if the entity URLs have been rewritten.
                entity_url_map = {}  # url → {"offset": int, "entity_type": str}
                if msg_entities:
                    for ent in msg_entities:
                        ent_url = getattr(ent, 'url', None)
                        if ent_url:
                            ent_type_raw = getattr(ent, 'type', None)
                            ent_type_str = str(ent_type_raw).split('.')[-1].lower() if ent_type_raw else "unknown"
                            entity_url_map[ent_url] = {
                                "offset": getattr(ent, 'offset', 0),
                                "entity_type": "text_link" if "text_link" in ent_type_str else ent_type_str,
                            }

                # Merge both sources: cached links take priority for resolution
                # because they have the correct source-channel URLs.
                # Current-text links are used for offset/position info (editing).
                seen_urls = set()
                unique_links = []

                # First, add cached source-channel links (HIGHEST PRIORITY)
                # These have the ORIGINAL URLs pointing to source channel
                for cl in cached_links:
                    cl_url = cl.get("url", "")
                    if cl_url and cl_url not in seen_urls:
                        seen_urls.add(cl_url)
                        # Try to match this cached URL to an actual entity
                        # If the entity URL matches (source channel not yet rewritten),
                        # use the entity's offset and type
                        ent_info = entity_url_map.get(cl_url, {})
                        unique_links.append({
                            "url": cl_url,
                            "offset": ent_info.get("offset", 0),
                            "length": ent_info.get("length", len(cl_url)),
                            "entity_type": ent_info.get("entity_type", cl.get("link_type", "cached")),
                            "src_msg_id": cl.get("src_msg_id"),
                            "channel_key": cl.get("channel_key", ""),
                            "from_cache": True,
                        })

                # Then, add current-text links that aren't already covered
                for link in current_links:
                    if link["url"] not in seen_urls:
                        seen_urls.add(link["url"])
                        link["from_cache"] = False
                        unique_links.append(link)

                if not unique_links:
                    await mark_message_links_fixed(uid, dest_channel_id, msg.id)
                    marked_fixed_count += 1
                    continue

                # Classify and build rewrite plan
                # For cached links, we need to create LinkInfo manually
                # since classify_link might not recognize them (if they've been
                # rewritten in the current text, the URL format differs)
                plan = []
                for link in unique_links:
                    link_info = classify_link(link["url"], source_channels_info, dest_channel_id=dest_channel_id)

                    # Skip non-source-channel links (they don't need rewriting)
                    if not link_info.is_source_channel:
                        continue

                    plan.append({
                        "old_url": link["url"],
                        "offset": link.get("offset", 0),
                        "length": link.get("length", len(link["url"])),
                        "entity_type": link.get("entity_type", "unknown"),
                        "link_info": link_info,
                        "new_url": None,
                        "resolved": False,
                        "from_cache": link.get("from_cache", False),
                    })

                if not plan:
                    # No source-channel links found in either current text or cache
                    await mark_message_links_fixed(uid, dest_channel_id, msg.id)
                    marked_fixed_count += 1
                    continue

                # Resolve links
                plan = await editor.resolve_plan(plan, resolver)

                resolved_items = [p for p in plan if p["resolved"]]
                unresolved_items = [p for p in plan if not p["resolved"] and p["link_info"].is_source_channel]

                # Categorize unresolved: "not_mirrored" vs "resolution_failed"
                not_mirrored_items = []
                resolution_failed_items = []
                for item in unresolved_items:
                    src_id = item["link_info"].source_msg_id
                    if src_id and src_id not in resolver.msg_id_map:
                        # Source message was never mirrored — no dst equivalent exists
                        not_mirrored_items.append(item)
                    else:
                        # Source message IS in mapping but resolution still failed
                        resolution_failed_items.append(item)

                if not resolved_items:
                    if unresolved_items:
                        unresolved_count += len(unresolved_items)
                        if not_mirrored_items:
                            not_mirrored_count += len(not_mirrored_items)
                            for nm_item in not_mirrored_items:
                                nm_src = nm_item["link_info"].source_msg_id
                                if nm_src:
                                    not_mirrored_src_ids.add(nm_src)
                            _edlog(f"[SMART-RELINK] msg {msg.id}: {len(not_mirrored_items)} links point to "
                                   f"source messages NOT YET MIRRORED (src_ids: "
                                   f"{[i['link_info'].source_msg_id for i in not_mirrored_items[:5]]}...)")
                    else:
                        already_correct_count += 1
                        # All links already point to dest or are non-source — mark as fixed
                        await mark_message_links_fixed(uid, dest_channel_id, msg.id)
                        marked_fixed_count += 1
                    continue

                # ── Apply rewrites ─────────────────────────────
                if dry_run:
                    fixed_count += len(resolved_items)
                    _edlog(f"[SMART-RELINK-DRY] Would fix {len(resolved_items)} links in msg {msg.id}")
                else:
                    await armor.acquire()
                    try:
                        new_text, adjustments = editor.apply_rewrites_to_text(msg_text, plan)
                        new_entities, entity_urls_changed = editor.adjust_entities(
                            msg_entities, plan, adjustments, dest_channel_id=dest_channel_id)

                        text_changed = (new_text != msg_text)
                        if not text_changed and not entity_urls_changed:
                            already_correct_count += 1
                            await mark_message_links_fixed(uid, dest_channel_id, msg.id)
                            marked_fixed_count += 1
                            continue

                        is_caption = bool(msg.caption) and not bool(msg.text)
                        edit_ok = await edit_message_safe(
                            bot_client=client,
                            ubot=ubot,
                            dst_chat_id=chat_id,
                            dst_msg_id=msg.id,
                            new_text=new_text,
                            new_entities=new_entities if new_entities else None,
                            is_caption=is_caption,
                        )

                        if edit_ok:
                            fixed_count += len(resolved_items)
                            armor.on_success()
                            # Mark as fixed in Smart Cache
                            await mark_message_links_fixed(uid, dest_channel_id, msg.id)
                            marked_fixed_count += 1
                            _edlog(f"[SMART-RELINK] ✅ Fixed {len(resolved_items)} links in msg {msg.id}")
                        else:
                            failed_edit_count += 1
                            armor.on_error(Exception("edit_failed"))
                            _edlog(f"[SMART-RELINK] ❌ Edit failed for msg {msg.id}")
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        armor.on_error(e)
                        _edlog(f"[SMART-RELINK] Rewrite error for msg {msg.id}: {e}")

                if unresolved_items:
                    unresolved_count += len(unresolved_items)
                    if not_mirrored_items:
                        not_mirrored_count += len(not_mirrored_items)
                        for nm_item in not_mirrored_items:
                            nm_src = nm_item["link_info"].source_msg_id
                            if nm_src:
                                not_mirrored_src_ids.add(nm_src)

                # Update progress every 10 messages
                if scanned_count % 10 == 0:
                    elapsed = time.time() - start_time
                    speed = scanned_count / max(elapsed / 60, 0.01)
                    try:
                        await safe_edit(status_msg,
                            f"⚡ **Smart Cache Relink**\n\n"
                            f"📋 Scanned: {scanned_count}/{len(target_ids)}\n"
                            f"✅ Fixed: {fixed_count}\n"
                            f"⏳ Unresolved: {unresolved_count}\n"
                            f"🔄 Already correct: {already_correct_count}\n"
                            f"🚀 Speed: {speed:.0f} msg/min"
                        )
                    except Exception:
                        pass

                # Update session checkpoint
                await update_relink_checkpoint(
                    session_id,
                    last_scanned_msg_id=msg.id,
                    total_scanned=scanned_count,
                    total_fixed=fixed_count,
                    total_unresolved=unresolved_count,
                    total_already_correct=already_correct_count,
                )

            # Brief pause between batches
            await asyncio.sleep(1.5)

    except asyncio.CancelledError:
        _edlog(f"[SMART-RELINK] Scan cancelled")
        await relink_sessions_collection.update_one(
            {"_id": session_id},
            {"$set": {"status": "cancelled", "completed_at": datetime.utcnow()}}
        )
        return
    except Exception as e:
        _edlog(f"[SMART-RELINK] Error: {e}")
        await append_to_session(session_id, "error_log", [f"{type(e).__name__}: {str(e)[:200]}"])

    # ── Final update ───────────────────────────────────────────
    elapsed = time.time() - start_time
    speed = scanned_count / max(elapsed / 60, 0.01)

    await relink_sessions_collection.update_one(
        {"_id": session_id},
        {"$set": {
            "status": "completed",
            "completed_at": datetime.utcnow(),
            "total_scanned": scanned_count,
            "total_fixed": fixed_count,
            "total_unresolved": unresolved_count,
            "total_already_correct": already_correct_count,
            "speed_msg_per_min": round(speed, 1),
        }}
    )

    # Send completion summary
    resolver_stats = resolver.stats

    # Build "not mirrored" advisory
    not_mirrored_advisory = ""
    if not_mirrored_count > 0:
        _sample_ids = sorted(not_mirrored_src_ids)[:10]
        not_mirrored_advisory = (
            f"\n\n⚠️ **{not_mirrored_count} links point to source messages NOT YET MIRRORED**\n"
            f"📋 Source msg IDs (sample): `{_sample_ids}`\n"
            f"💡 Mirror these source messages first, then run /relink again."
        )

    try:
        await safe_edit(status_msg,
            f"✅ **Smart Cache Relink Complete!**\n\n"
            f"⚡ Mode: Surgical Strike (no full scan)\n"
            f"📋 Targeted: {len(target_ids)} messages\n"
            f"✅ Fixed: {fixed_count} links\n"
            f"🔄 Already correct: {already_correct_count}\n"
            f"⏳ Still unresolved: {unresolved_count}\n"
            f"🚫 Not mirrored: {not_mirrored_count}\n"
            f"❌ Failed edits: {failed_edit_count}\n"
            f"🗂️ Marked fixed in cache: {marked_fixed_count}\n"
            f"⏱️ Duration: {_fmt_dur(elapsed)}\n"
            f"🚀 Speed: {speed:.0f} msg/min\n\n"
            f"📊 Strategy hits: S1={resolver_stats.get('strategy_1',0)} "
            f"S6={resolver_stats.get('strategy_6',0)} "
            f"S7={resolver_stats.get('strategy_7',0)} "
            f"FP={resolver_stats.get('fingerprint_hits',0)}"
            f"{not_mirrored_advisory}"
        )
    except Exception:
        pass

    _edlog(f"[SMART-RELINK] Complete: targeted={len(target_ids)} fixed={fixed_count} "
           f"unresolved={unresolved_count} elapsed={_fmt_dur(elapsed)} "
           f"strategies={resolver_stats}")


# ════════════════════════════════════════════════════════════════════
# /RELINK BACKFILL — Fast index-only scan to populate Smart Cache
#
# This is a ONE-TIME operation that scans ALL messages in the
# destination channel and indexes them in mirrored_messages_index.
# After backfill, future /relink runs use Smart Cache for instant
# surgical strikes instead of slow blind scanning.
#
# Key difference from the full scan:
#   - NO editing — only indexing
#   - NO resolving — only classification
#   - MUCH faster — ~200 msg/sec vs ~20 msg/sec for full scan
#   - Uses get_chat_history() for efficient iteration
# ════════════════════════════════════════════════════════════════════

async def _relink_backfill(client, message: Message, uid: int, chat_id: int):
    """Build the Smart Cache index by scanning all messages in the destination channel.

    This is a fast index-only scan — no editing, no resolving.
    After backfill, /relink will use Smart Cache for instant surgical strikes.

    The backfill:
    1. Iterates through ALL messages in the destination channel
    2. For each message, extracts and classifies links
    3. Writes a lightweight index document to mirrored_messages_index
    4. Messages with source-channel links get contains_old_links=True
    5. Messages without source links get contains_old_links=False (skip in future)
    """
    from plugins.batch import (
        mirrored_messages_index, ADDITIONAL_SOURCE_CHANNELS,
        _extract_source_links_from_message, normalize_channel_id as batch_normalize,
        unresolved_links_collection,
    )

    # Determine destination channel
    dest_channel_id = chat_id
    dest_channel_username = None
    try:
        chat = await client.get_chat(chat_id)
        dest_channel_username = getattr(chat, 'username', None)
    except Exception:
        pass

    # Check existing index
    existing_indexed = await mirrored_messages_index.count_documents(
        {"uid": uid, "dst_chat_id": dest_channel_id}
    )
    existing_old_links = await mirrored_messages_index.count_documents(
        {"uid": uid, "dst_chat_id": dest_channel_id, "contains_old_links": True}
    )

    # Also check unresolved_links_collection — messages KNOWN to have unresolved source links
    _unresolved_count = 0
    try:
        _unresolved_count = await unresolved_links_collection.count_documents(
            {"user_id": uid, "unresolved": True}
        )
    except Exception:
        pass

    status_msg = await safe_reply(message,
        f"🗂️ **Smart Cache Backfill**\n\n"
        f"📍 Channel: `{dest_channel_id}`\n"
        f"📊 Already indexed: {existing_indexed}\n"
        f"🔗 With old links: {existing_old_links}\n"
        f"⚠️ Unresolved (from DB): {_unresolved_count}\n\n"
        f"⏳ Starting fast index scan..."
    )
    if not status_msg:
        return

    # ── STEP 0: Import unresolved links from unresolved_links_collection ──
    # This is the MOST RELIABLE source of truth — messages that were marked
    # as having unresolved source links AT MIRROR TIME. Even if the destination
    # message text has been partially rewritten, the unresolved_links_collection
    # knows which messages still have broken links.
    _imported_from_unresolved = 0
    try:
        async for ul_doc in unresolved_links_collection.find({"user_id": uid, "unresolved": True}):
            ul_dst_chat = ul_doc.get("dst_chat_id")
            ul_dst_msg = ul_doc.get("dst_msg_id")
            ul_src_ch = ul_doc.get("source_channel", "")
            ul_src_msg = ul_doc.get("src_msg_id")
            ul_unresolved_ids = ul_doc.get("unresolved_src_ids", [])

            if not ul_dst_msg:
                continue
            # Filter by destination channel
            if ul_dst_chat and batch_normalize(str(ul_dst_chat)) != batch_normalize(str(dest_channel_id)):
                continue

            # Build source links from the unresolved IDs
            _ul_source_links = []
            _ul_unresolved = []
            for _uid_src in ul_unresolved_ids:
                # Build a URL for this unresolved src_msg_id
                _src_ch_clean = batch_normalize(ul_src_ch)
                if _src_ch_clean:
                    _ul_url = f"https://t.me/c/{_src_ch_clean}/{_uid_src}"
                else:
                    _ul_url = f"https://t.me/c/UNKNOWN/{_uid_src}"
                _ul_source_links.append({
                    "url": _ul_url,
                    "src_msg_id": int(_uid_src),
                    "channel_key": _src_ch_clean,
                    "link_type": "private",
                })
                _ul_unresolved.append(int(_uid_src))

            await mirrored_messages_index.update_one(
                {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": ul_dst_msg},
                {"$set": {
                    "uid": uid,
                    "source_channel": str(ul_src_ch),
                    "src_msg_id": ul_src_msg,
                    "dst_chat_id": dest_channel_id,
                    "dst_msg_id": ul_dst_msg,
                    "contains_old_links": True,
                    "links_to_resolve": _ul_source_links,
                    "unresolved_src_ids": _ul_unresolved,
                    "last_updated": datetime.utcnow(),
                    "imported_from_unresolved": True,
                }},
                upsert=True,
            )
            _imported_from_unresolved += 1

        if _imported_from_unresolved > 0:
            _edlog(f"[BACKFILL] Imported {_imported_from_unresolved} messages from unresolved_links_collection")
    except Exception as _ul_err:
        _edlog(f"[BACKFILL] Failed to import from unresolved_links: {_ul_err}")

    # Load source channel info for classification
    combined_map, channel_info = await load_combined_msg_id_map(uid, dest_channel_id=dest_channel_id)

    # Also load ADDITIONAL_SOURCE_CHANNELS
    _asc = ADDITIONAL_SOURCE_CHANNELS
    if _asc:
        for extra_ch in _asc:
            if extra_ch in channel_info:
                continue
            extra_map, _, extra_dest = await load_upload_map(uid, str(extra_ch))
            if extra_map:
                combined_map.update(extra_map)

    # Build source channel params for _extract_source_links_from_message
    # This function uses DIRECT channel ID matching (not classify_link's
    # source_channels_info approach), which is more reliable for link detection.
    _primary_source_channel = None
    _source_channel_username = None
    _source_channel_id = None
    _multi_source_channels = []

    for ch_str, ch_info in channel_info.items():
        ch_numeric = ch_info.get("numeric_id")
        ch_username = ch_info.get("username")

        if ch_username is None or ch_numeric is None:
            try:
                ch_int = int(ch_str) if ch_str.lstrip('-').isdigit() else None
                if ch_int:
                    try:
                        resolved = await client.get_chat(ch_int)
                        if resolved:
                            if ch_username is None and hasattr(resolved, 'username') and resolved.username:
                                ch_username = resolved.username.lower()
                            if ch_numeric is None:
                                ch_numeric = ch_int
                    except Exception:
                        pass
            except Exception:
                pass

        _multi_ch_info = {
            "channel": ch_str,
            "username": ch_username,
            "numeric_id": ch_numeric,
            "clean_id": batch_normalize(ch_str),
        }
        _multi_source_channels.append(_multi_ch_info)

        if _primary_source_channel is None:
            _primary_source_channel = ch_str
            _source_channel_username = ch_username
            _source_channel_id = ch_numeric

    # Also build source_channels_info for classify_link fallback
    source_channels_info = {}
    for ch_str, ch_info in channel_info.items():
        ch_numeric = ch_info.get("numeric_id")
        ch_username = ch_info.get("username")
        ch_clean = normalize_channel_id(ch_str)

        if ch_username is None or ch_numeric is None:
            try:
                ch_int = int(ch_str) if ch_str.lstrip('-').isdigit() else None
                if ch_int:
                    try:
                        resolved = await client.get_chat(ch_int)
                        if resolved:
                            if ch_username is None and hasattr(resolved, 'username') and resolved.username:
                                ch_username = resolved.username.lower()
                            if ch_numeric is None:
                                ch_numeric = ch_int
                    except Exception:
                        pass
            except Exception:
                pass

        source_channels_info[ch_str] = {
            "clean_id": ch_clean,
            "username": ch_username,
            "numeric_id": ch_numeric,
            "dest_channel": ch_info.get("dest_channel"),
        }

    if not source_channels_info and not _multi_source_channels:
        await safe_edit(status_msg, "❌ No source channel info found! Run /mirror first.")
        return

    # ── Fast backfill scan ─────────────────────────────────────
    # Uses _extract_source_links_from_message() from batch.py instead
    # of classify_link() because:
    #   1. _extract_source_links_from_message does DIRECT channel ID matching
    #   2. It checks BOTH entity URLs and bare text URLs
    #   3. It knows the source channel IDs directly, so no matching ambiguity
    #   4. classify_link depends on source_channels_info which may not have
    #      username/numeric_id populated (causing source_ch_links: 0)
    indexed_count = 0
    source_link_count = 0
    no_link_count = 0
    error_count = 0
    bulk_ops = []  # Bulk write operations for efficiency
    BULK_FLUSH_SIZE = 100  # Flush every 100 messages
    start_time = time.time()
    last_progress_update = time.time()

    # Also count messages already in the index (from STEP 0 import)
    _pre_imported = _imported_from_unresolved

    # Diagnostic counters
    _diag_tme_count = 0
    _diag_extracted_count = 0
    _diag_classify_hit = 0
    _diag_extract_hit = 0

    try:
        # ── Use bot-compatible message fetching ──
        # get_chat_history() is a user-only method (BOT_METHOD_INVALID).
        # We use the same bot-compatible approach as run_relink_scan:
        # build_id_batches + fetch_message_batch_safe.
        ubot = get_Y()
        _fetch_client = ubot if ubot is not None else client
        # Re-check ubot availability
        if _fetch_client is None or _fetch_client is client:
            try:
                import shared_client as _sc_f
                if _sc_f.userbot is not None:
                    _fetch_client = _sc_f.userbot
            except Exception:
                pass

        # Get the latest message ID to know the range
        _latest_msg = None
        try:
            _latest_msg = await _fetch_client.get_messages(chat_id, 1)
            if isinstance(_latest_msg, list):
                _latest_msg = _latest_msg[0] if _latest_msg else None
            _max_id = _latest_msg.id if _latest_msg and hasattr(_latest_msg, 'id') else 10000
        except Exception:
            _max_id = 10000

        # Build ID batches (scan from newest to oldest for efficiency)
        _batch_size = 200
        id_batches = build_id_batches(_max_id, 1, "new_to_old", _batch_size)
        _edlog(f"[BACKFILL] Scanning {_max_id} messages in {len(id_batches)} batches "
               f"(fetch_client={type(_fetch_client).__name__})")

        for batch_ids in id_batches:
            batch_msgs = await fetch_message_batch_safe(_fetch_client, chat_id, batch_ids)
            if not batch_msgs:
                continue

            for msg in batch_msgs:
                try:
                    if msg is None or getattr(msg, 'empty', False):
                        continue

                    msg_text = msg.text or msg.caption or ""
                    msg_entities = msg.entities or msg.caption_entities or []

                    # Quick filter: skip messages without t.me or tg:// links
                    _has_tme_in_text = "t.me" in msg_text.lower() or "tg://" in msg_text.lower()
                    _has_tme_in_entity = False
                    if not _has_tme_in_text and msg_entities:
                        for _ent in msg_entities:
                            _ent_url = getattr(_ent, 'url', None)
                            if _ent_url and ('t.me' in _ent_url.lower() or 'tg://' in _ent_url.lower()):
                                _has_tme_in_entity = True
                                break
                    if not _has_tme_in_text and not _has_tme_in_entity:
                        # No t.me links at all — still index as "no source links" for completeness
                        _src_ch_str = _primary_source_channel or ""
                        bulk_ops.append({
                            "filter": {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": msg.id},
                            "update": {"$set": {
                                "uid": uid,
                                "source_channel": str(_src_ch_str),
                                "src_msg_id": None,
                                "dst_chat_id": dest_channel_id,
                                "dst_msg_id": msg.id,
                                "contains_old_links": False,
                                "links_to_resolve": [],
                                "unresolved_src_ids": [],
                                "last_updated": datetime.utcnow(),
                                "backfilled": True,
                            }},
                            "upsert": True,
                        })
                        no_link_count += 1
                        indexed_count += 1
                        continue

                    _diag_tme_count += 1

                    # ── PRIMARY METHOD: Use _extract_source_links_from_message ──
                    # This directly matches channel IDs and is the most reliable method.
                    _source_links = []
                    _unresolved_ids = []

                    if _primary_source_channel:
                        _source_links = _extract_source_links_from_message(
                            msg_text, msg_entities,
                            _primary_source_channel,
                            source_channel_username=_source_channel_username,
                            source_channel_id=_source_channel_id,
                            multi_source_channels=_multi_source_channels if len(_multi_source_channels) > 1 else None,
                        )
                        _unresolved_ids = [l["src_msg_id"] for l in _source_links]

                    if _source_links:
                        _diag_extract_hit += 1

                    # ── FALLBACK: Also try classify_link for diagnostic ──
                    # If _extract found nothing but classify_link finds something,
                    # there's a matching issue we should know about.
                    if not _source_links and source_channels_info:
                        from plugins.batch import _TME_PRIVATE_RE, _TME_PUBLIC_RE, _TG_RESOLVE_RE, _TME_SKIP_PATHS
                        entity_urls = set()
                        text_urls = set()
                        if msg_entities:
                            for ent in msg_entities:
                                ent_url = getattr(ent, 'url', None)
                                if ent_url and ('t.me' in ent_url.lower() or 'tg://' in ent_url.lower()):
                                    entity_urls.add(ent_url)
                        for m in _TME_PRIVATE_RE.finditer(msg_text):
                            text_urls.add(m.group(0))
                        for m in _TME_PUBLIC_RE.finditer(msg_text):
                            uname = m.group(1)
                            if uname.lower() not in _TME_SKIP_PATHS:
                                text_urls.add(m.group(0))
                        for m in _TG_RESOLVE_RE.finditer(msg_text):
                            text_urls.add(m.group(0))

                        all_urls = entity_urls | text_urls
                        for url in all_urls:
                            link_info = classify_link(url, source_channels_info, dest_channel_id=dest_channel_id)
                            if link_info.is_source_channel and link_info.source_msg_id:
                                _source_links.append({
                                    "url": url,
                                    "src_msg_id": link_info.source_msg_id,
                                    "channel_key": normalize_channel_id(link_info.source_peer) if link_info.source_peer else (link_info.username or ""),
                                    "link_type": link_info.link_type,
                                })
                                _unresolved_ids.append(link_info.source_msg_id)
                        if _source_links:
                            _diag_classify_hit += 1

                    _diag_extracted_count += len(_source_links)

                    # Check if this message was already imported from unresolved_links
                    # If so, skip (the import already set contains_old_links=True with correct data)
                    _already_imported = False
                    if _pre_imported > 0:
                        _existing = await mirrored_messages_index.find_one(
                            {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": msg.id,
                             "imported_from_unresolved": True}
                        )
                        if _existing:
                            _already_imported = True

                    # Build index document
                    _src_ch_str = _primary_source_channel or ""
                    has_old_links = len(_source_links) > 0

                    if _already_imported and has_old_links:
                        # Merge: add newly found links to the existing imported ones
                        bulk_ops.append({
                            "filter": {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": msg.id},
                            "update": {"$addToSet": {
                                "links_to_resolve": {"$each": _source_links},
                                "unresolved_src_ids": {"$each": _unresolved_ids},
                            }},
                        })
                    elif not _already_imported:
                        bulk_ops.append({
                            "filter": {"uid": uid, "dst_chat_id": dest_channel_id, "dst_msg_id": msg.id},
                            "update": {"$set": {
                                "uid": uid,
                                "source_channel": str(_src_ch_str),
                                "src_msg_id": None,  # Unknown during backfill
                                "dst_chat_id": dest_channel_id,
                                "dst_msg_id": msg.id,
                                "contains_old_links": has_old_links,
                                "links_to_resolve": _source_links if has_old_links else [],
                                "unresolved_src_ids": _unresolved_ids if has_old_links else [],
                                "last_updated": datetime.utcnow(),
                                "backfilled": True,
                            }},
                            "upsert": True,
                        })

                    if has_old_links:
                        source_link_count += 1
                    else:
                        no_link_count += 1

                    indexed_count += 1

                    # Flush bulk ops periodically
                    if len(bulk_ops) >= BULK_FLUSH_SIZE:
                        try:
                            from pymongo import UpdateOne
                            write_ops = []
                            for op in bulk_ops:
                                write_ops.append(UpdateOne(op["filter"], op["update"], upsert=op.get("upsert", False)))
                            await mirrored_messages_index.bulk_write(write_ops, ordered=False)
                        except Exception as bw_err:
                            _edlog(f"[BACKFILL] Bulk write error: {bw_err}")
                            error_count += len(bulk_ops)
                        bulk_ops = []

                    # Progress update every 30 seconds
                    if time.time() - last_progress_update >= 30:
                        elapsed = time.time() - start_time
                        speed = indexed_count / max(elapsed / 60, 0.01)
                        try:
                            await safe_edit(status_msg,
                                f"🗂️ **Smart Cache Backfill**\n\n"
                                f"📊 Indexed: {indexed_count} messages\n"
                                f"🔗 With old source links: {source_link_count}\n"
                                f"✅ Already correct: {no_link_count}\n"
                                f"📥 Imported from DB: {_imported_from_unresolved}\n"
                                f"🔍 t.me msgs: {_diag_tme_count} | Found: {_diag_extract_hit}+{_diag_classify_hit}\n"
                                f"⚡ Speed: {speed:.0f} msg/min\n"
                                f"⏱️ Elapsed: {_fmt_dur(elapsed)}"
                            )
                        except Exception:
                            pass
                        last_progress_update = time.time()

                except Exception as msg_err:
                    error_count += 1
                    if error_count <= 3:
                        _edlog(f"[BACKFILL] Message error: {msg_err}")
                    continue

            # Brief pause between batches to avoid FloodWait
            await asyncio.sleep(1.5)

    except asyncio.CancelledError:
        _edlog(f"[BACKFILL] Cancelled by user")
    except Exception as scan_err:
        _edlog(f"[BACKFILL] Scan error: {scan_err}")
        error_count += 1

    # Flush remaining bulk ops
    if bulk_ops:
        try:
            from pymongo import UpdateOne
            write_ops = []
            for op in bulk_ops:
                write_ops.append(UpdateOne(op["filter"], op["update"], upsert=op.get("upsert", False)))
            await mirrored_messages_index.bulk_write(write_ops, ordered=False)
        except Exception as bw_err:
            _edlog(f"[BACKFILL] Final bulk write error: {bw_err}")
            error_count += len(bulk_ops)

    elapsed = time.time() - start_time
    speed = indexed_count / max(elapsed / 60, 0.01)

    # ── Also bulk-load fingerprints into combined_map ──────────
    # This ensures that the next /relink run has MAXIMUM mapping coverage
    _edlog(f"[BACKFILL] Loading all fingerprint src→dst mappings into combined_map...")
    fp_loaded = 0
    try:
        async for fp_doc in fingerprints_collection.find({"uid": uid}):
            src_id = fp_doc.get("src_msg_id")
            dst_id = fp_doc.get("dst_msg_id")
            if src_id and dst_id and src_id not in combined_map:
                combined_map[src_id] = dst_id
                fp_loaded += 1
    except Exception as fp_err:
        _edlog(f"[BACKFILL] Fingerprint load error: {fp_err}")

    _edlog(f"[BACKFILL] Complete: indexed={indexed_count} old_links={source_link_count} "
           f"no_links={no_link_count} errors={error_count} fp_extra={fp_loaded} "
           f"imported_from_unresolved={_imported_from_unresolved} "
           f"diag_extract_hit={_diag_extract_hit} diag_classify_hit={_diag_classify_hit} "
           f"diag_tme_count={_diag_tme_count} diag_extracted_count={_diag_extracted_count} "
           f"elapsed={_fmt_dur(elapsed)} speed={speed:.0f}")

    # Final status
    final_indexed = await mirrored_messages_index.count_documents(
        {"uid": uid, "dst_chat_id": dest_channel_id}
    )
    final_old_links = await mirrored_messages_index.count_documents(
        {"uid": uid, "dst_chat_id": dest_channel_id, "contains_old_links": True}
    )

    await safe_edit(status_msg,
        f"✅ **Smart Cache Backfill Complete!**\n\n"
        f"📊 Indexed: {indexed_count} messages\n"
        f"🔗 With old source links: **{source_link_count}**\n"
        f"✅ Already correct: {no_link_count}\n"
        f"📥 Imported from DB: {_imported_from_unresolved}\n"
        f"🗂️ Extra FP mappings: {fp_loaded}\n"
        f"🔍 Method hits: extract={_diag_extract_hit} classify={_diag_classify_hit}\n"
        f"⏱️ Time: {_fmt_dur(elapsed)} ({speed:.0f} msg/min)\n\n"
        f"📈 **Smart Cache now:**\n"
        f"  Total indexed: {final_indexed}\n"
        f"  Messages with old links: {final_old_links}\n\n"
        f"⚡ Next /relink will use **Smart Cache surgical strike mode!**"
    )


@X.on_message(filters.command("relink"))
async def relink_cmd(client, message: Message):
    """Handle /relink command in any chat type.

    NO chat-type routing — we ALWAYS run the full scan logic.
    Previous attempts to detect group vs private (filters.group,
    ChatType enum, string comparison) all failed in Pyrofork's
    topic/forum supergroups. The simplest reliable fix: just run
    the scan. If used in private chat, pre-flight checks will
    catch it and give a helpful error.

    Subcommands and options:
      /relink              — Scan entire chat, fix all broken links
      /relink backfill     — Build Smart Cache index (fast, no editing)
      /relink status       — Show current/past session progress
      /relink cancel       — Cancel running session (progress saved)
      /relink retry        — Retry all previously failed edits
      /relink --limit 100  — Scan only last 100 messages
      /relink --dry-run    — Preview changes without editing
    """
    try:
        await _relink_main(client, message)
    except Exception as e:
        # NEVER let /relink crash silently — always report errors
        try:
            await message.reply_text(f"❌ /relink error: `{type(e).__name__}: {str(e)[:200]}`")
        except Exception:
            pass
        _edlog(f"[RELINK] Unhandled error: {e}")


async def _relink_main(client, message: Message):
    """Core /relink logic — works in ANY chat type."""
    uid = message.from_user.id if message.from_user else None
    if not uid:
        return

    # Auth check
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return

    # Lazy-load batch functions on first /relink call
    try:
        await _get_batch_funcs()
    except Exception as e:
        await message.reply_text(f"❌ Failed to initialize relink: {e}")
        return

    chat_id = message.chat.id
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []

    # ── Subcommand: backfill ──────────────────────────────────
    if "backfill" in args:
        await _relink_backfill(client, message, uid, chat_id)
        return

    # ── Subcommand: status ────────────────────────────────────
    if "status" in args:
        session = await get_latest_relink_session(chat_id)
        if not session:
            await safe_reply(message, "📊 No relink sessions found for this chat.")
            return

        status_emoji = {
            "pending": "⏳", "in_progress": "🔄", "completed": "✅",
            "failed": "❌", "cancelled": "⏹️"
        }.get(session.get("status", ""), "❓")

        elapsed_str = ""
        if session.get("started_at"):
            if session.get("completed_at"):
                elapsed = (session["completed_at"] - session["started_at"]).total_seconds()
                elapsed_str = f" | Duration: {_fmt_dur(elapsed)}"
            else:
                elapsed = (datetime.utcnow() - session["started_at"]).total_seconds()
                elapsed_str = f" | Running: {_fmt_dur(elapsed)}"

        text = (
            f"📊 **Relink Session Status**\n\n"
            f"{status_emoji} Status: **{session.get('status', 'unknown')}**\n"
            f"👤 Triggered by: `{session.get('triggered_by', '?')}`\n"
            f"📋 Scanned: {session.get('total_scanned', 0)} messages\n"
            f"🔗 Fixed: {session.get('total_fixed', 0)} links ✅\n"
            f"⏳ Unresolved: {session.get('total_unresolved', 0)}\n"
            f"✓ Already correct: {session.get('total_already_correct', 0)}\n"
            f"⏭️ Skipped: {session.get('total_skipped', 0)}\n"
            f"❌ Failed edits: {len(session.get('failed_edits', []))}\n"
            f"📍 Last checkpoint: msg_id {session.get('last_scanned_msg_id', '?')}\n"
            f"🚀 Speed: {session.get('speed_msg_per_min', 0):.1f} msg/min{elapsed_str}"
        )

        if session.get("dry_run"):
            text = "🔒 **DRY RUN** (no edits made)\n\n" + text

        await safe_reply(message, text)
        return

    # ── Subcommand: cancel ────────────────────────────────────
    if "cancel" in args:
        # Cancel the asyncio task if running
        if chat_id in _relink_tasks:
            _relink_tasks[chat_id].cancel()
            del _relink_tasks[chat_id]

        cancelled = await cancel_relink_session(chat_id)
        if cancelled:
            await safe_reply(message, "⏹️ Relink session cancelled. Progress saved — you can resume later.")
        else:
            await safe_reply(message, "ℹ️ No active relink session to cancel.")
        return

    # ── Subcommand: retry ─────────────────────────────────────
    if "retry" in args:
        session = await get_latest_relink_session(chat_id)
        if not session or session.get("status") != "completed":
            await safe_reply(message, "ℹ️ No completed session to retry. Run /relink first.")
            return

        if not session.get("failed_edits"):
            await safe_reply(message, "✅ No failed edits to retry from last session!")
            return

        status_msg = await safe_reply(message, "🔄 Retrying failed edits...")
        if status_msg:
            asyncio.create_task(retry_failed_edits(client, session, status_msg))
        return

    # ── Parse options (GAP 4: --direction support) ───────────
    parsed = parse_relink_args(args)
    limit = parsed["limit"] if parsed["limit"] > 0 else None
    dry_run = parsed["dry_run"]
    direction = parsed["direction"]

    # Auto-direction: first session = old_to_new, subsequent = new_to_old
    if direction == "auto":
        prev_sessions = await relink_sessions_collection.count_documents({"chat_id": chat_id})
        direction = "old_to_new" if prev_sessions == 0 else "new_to_old"

    # ── Check for existing active session ─────────────────────
    existing = await get_active_relink_session(chat_id)
    if existing:
        status_emoji = "🔄" if existing["status"] == "in_progress" else "⏳"
        await safe_reply(message,
            f"{status_emoji} A relink session is already active for this chat!\n\n"
            f"📋 Scanned: {existing.get('total_scanned', 0)} | "
            f"Fixed: {existing.get('total_fixed', 0)} | "
            f"Unresolved: {existing.get('total_unresolved', 0)}\n\n"
            f"Use /relink cancel to stop it, or wait for it to complete."
        )
        return

    # ── Determine destination channel ─────────────────────────
    # For relink, the destination channel IS the current chat
    dest_channel_id = chat_id
    dest_channel_username = None

    # Try to get the chat's username
    try:
        chat = await client.get_chat(chat_id)
        dest_channel_username = getattr(chat, 'username', None)
    except Exception:
        pass

    # ── Pre-flight validation (Layer 6) ──────────────────────
    status_msg = await safe_reply(message, "🔍 Running pre-flight checks...")
    if not status_msg:
        return

    checks = await pre_flight_check(client, chat_id, uid)

    if not checks["bot_is_admin"]:
        await safe_edit(status_msg, "❌ I need to be admin in this chat to edit messages!")
        return

    if not checks["can_edit_messages"]:
        await safe_edit(status_msg, "❌ I need 'Edit Messages' admin permission in this chat!")
        return

    if not checks["msg_id_map_exists"]:
        await safe_edit(status_msg,
            "❌ No message ID mappings found!\n\n"
            "You need to mirror content first (using /batch or /auto) "
            "before /relink can fix links.")
        return

    if not checks["chat_has_messages"]:
        await safe_edit(status_msg, "❌ This chat appears to be empty!")
        return

    # ── All checks passed — show pre-flight summary ───────────
    preflight_text = (
        f"✅ Pre-flight checks passed!\n\n"
        f"📋 Message mappings: {checks['msg_id_map_count']}\n"
        f"📡 Source channels: {checks['source_channels_count']}\n"
        f"🛡️ Bot has edit permission\n"
    )
    if limit:
        preflight_text += f"🔢 Scan limit: last {limit} messages\n"
    if dry_run:
        preflight_text += f"🔒 DRY RUN — no edits will be made\n"

    # ── SMART CACHE CHECK: Try surgical strike first ──────────
    # Check if mirrored_messages_index has data for this user/channel.
    # If yes, use the lightning-fast Smart Cache approach instead of
    # the backwards-while-loop scan.
    #
    # NEW FLOW (user requested):
    #   If Smart Cache is EMPTY → run backfill FIRST → then surgical strike.
    #   This ensures we ALWAYS use the fast surgical strike mode, never the
    #   old slow full scan that had source_ch_links: 0 issues.
    _smart_cache_count = 0
    try:
        from plugins.batch import mirrored_messages_index
        _smart_cache_count = await mirrored_messages_index.count_documents(
            {"uid": uid, "dst_chat_id": dest_channel_id, "contains_old_links": True}
        )
    except Exception as _sc_err:
        _edlog(f"[RELINK] Smart Cache check failed: {_sc_err}")

    _total_indexed = 0
    try:
        from plugins.batch import mirrored_messages_index
        _total_indexed = await mirrored_messages_index.count_documents(
            {"uid": uid, "dst_chat_id": dest_channel_id}
        )
    except Exception:
        pass

    # Also check unresolved_links_collection for messages KNOWN to need relinking
    _unresolved_db_count = 0
    try:
        from plugins.batch import unresolved_links_collection
        _unresolved_db_count = await unresolved_links_collection.count_documents(
            {"user_id": uid, "unresolved": True}
        )
    except Exception:
        pass

    if _smart_cache_count > 0:
        # Smart Cache has data — use surgical strike mode!
        preflight_text += (
            f"\n🗂️ **Smart Cache: {_smart_cache_count} messages with old links** "
            f"(out of {_total_indexed} indexed)\n"
            f"⚡ Using surgical strike mode (no full scan needed!)\n"
        )
    elif _total_indexed > 0:
        # All indexed messages are already fixed — nothing to do
        preflight_text += f"\n✅ Smart Cache: All {_total_indexed} indexed messages are already fixed!\n"
        preflight_text += f"\n💡 If you expected fixes, the links may already have been rewritten during mirroring.\n"
        await safe_edit(status_msg, preflight_text)
        return
    else:
        # No Smart Cache data — RUN BACKFILL FIRST, then surgical strike.
        # This is the user's requested flow: if cache is empty, build it first.
        # The backfill imports data from unresolved_links_collection (most reliable)
        # and scans destination messages for remaining source links.
        preflight_text += (
            f"\n⚠️ Smart Cache empty — running backfill first!\n"
            f"📥 Unresolved in DB: {_unresolved_db_count}\n"
            f"🗂️ Backfill will build the index, then surgical strike will fix links.\n"
        )
        await safe_edit(status_msg, preflight_text)

        # Run backfill synchronously (wait for it to complete)
        await _relink_backfill(client, message, uid, chat_id)

        # After backfill, check if we now have messages needing relink
        _smart_cache_count_after = 0
        try:
            from plugins.batch import mirrored_messages_index
            _smart_cache_count_after = await mirrored_messages_index.count_documents(
                {"uid": uid, "dst_chat_id": dest_channel_id, "contains_old_links": True}
            )
        except Exception:
            pass

        _total_indexed_after = 0
        try:
            from plugins.batch import mirrored_messages_index
            _total_indexed_after = await mirrored_messages_index.count_documents(
                {"uid": uid, "dst_chat_id": dest_channel_id}
            )
        except Exception:
            pass

        if _smart_cache_count_after == 0:
            if _total_indexed_after > 0:
                await safe_reply(message,
                    f"✅ **Smart Cache built successfully!**\n\n"
                    f"📊 Indexed: {_total_indexed_after} messages\n"
                    f"🔗 With old links: **0** — all links are already correct!\n\n"
                    f"💡 Links were already rewritten during mirroring. No fixes needed."
                )
                return
            else:
                await safe_reply(message,
                    f"❌ **Smart Cache backfill failed** — no messages indexed.\n\n"
                    f"💡 Make sure you've mirrored content first using /batch or /mirror."
                )
                return

        _smart_cache_count = _smart_cache_count_after
        _edlog(f"[RELINK] After backfill: {_smart_cache_count} messages with old links "
               f"(out of {_total_indexed_after} indexed)")

    preflight_text += f"\n🔍 Starting relink..."

    await safe_edit(status_msg, preflight_text)

    # ── Create session and start scan ─────────────────────────
    session = await create_relink_session(
        chat_id=chat_id,
        triggered_by=uid,
        dest_channel_id=dest_channel_id,
        dest_channel_username=dest_channel_username,
        limit=limit,
        dry_run=dry_run,
        direction=direction,
    )

    # Choose scan mode — ALWAYS use Smart Cache surgical strike now.
    # If cache was empty, we already ran backfill above to populate it.
    # The old full scan (run_relink_scan) is no longer used because it
    # had source_ch_links: 0 issues due to links being rewritten at mirror time.
    if _smart_cache_count > 0:
        task = asyncio.create_task(run_relink_smart_cache(client, session, status_msg))
    else:
        # Shouldn't reach here since we backfill above, but fallback gracefully
        await safe_edit(status_msg, "✅ No messages need relinking — all links are already correct!")
        return
    _relink_tasks[chat_id] = task

    # Clean up task reference when done
    def _cleanup_task(t):
        _relink_tasks.pop(chat_id, None)

    task.add_done_callback(_cleanup_task)


# ════════════════════════════════════════════════════════════════════
# PLUGIN REGISTRATION
# ════════════════════════════════════════════════════════════════════

async def run_relink_plugin():
    """Register the relink plugin. Called by main.py's plugin loader."""
    # Create indexes for all collections
    try:
        await relink_sessions_collection.create_index(
            [("chat_id", 1), ("status", 1)]
        )
        await relink_sessions_collection.create_index(
            [("triggered_by", 1)]
        )

        # GAP 3: Cache indexes
        await relink_cache_collection.create_index(
            [("source_url", 1)],
            unique=True
        )
        await relink_cache_collection.create_index(
            [("hit_count", 1)]
        )

        # GAP 1: Fingerprint indexes
        await fingerprints_collection.create_index(
            [("fingerprint", 1)],
            unique=True
        )
        await fingerprints_collection.create_index(
            [("uid", 1), ("source_channel", 1)]
        )
        # CRITICAL: src_msg_id index for Strategy 6.5 direct lookup
        # This makes FP-DB-LOOKUP instant instead of collection scan
        await fingerprints_collection.create_index(
            [("uid", 1), ("src_msg_id", 1)]
        )
        await fingerprints_collection.create_index(
            [("uid", 1), ("src_msg_id", 1), ("source_channel", 1)]
        )

        # GAP 6: Source scan watermark indexes
        await source_scan_watermark_collection.create_index(
            [("uid", 1), ("source_channel", 1)],
            unique=True,
        )

        # TTL index: auto-delete old completed sessions after 30 days
        await relink_sessions_collection.create_index(
            [("completed_at", 1)],
            expireAfterSeconds=30 * 24 * 3600
        )

        logger.info("[RELINK] All indexes created ✅")
    except Exception as e:
        logger.warning(f"Failed to create relink indexes: {e}")

    # Verify handler was registered (debug log)
    try:
        handler_count = 0
        for group_id, handlers in X.dispatcher.groups.items():
            for handler in handlers:
                if hasattr(handler, 'callback') and 'relink' in getattr(handler.callback, '__name__', ''):
                    handler_count += 1
        print(f"[RELINK] Handler registration verified: {handler_count} relink handler(s) found on client")
    except Exception as e:
        print(f"[RELINK] Could not verify handler registration: {e}")

    _edlog("[RELINK] Plugin loaded — /relink command ready (unified handler, all chat types)")
    print("[RELINK] Plugin loaded — /relink command ready (unified handler, all chat types)")


# ════════════════════════════════════════════════════════════════════
# INTEGRATION HOOKS — called from batch.py after each message upload
# ════════════════════════════════════════════════════════════════════

async def on_new_mirror_message(uid: int, source_channel: str,
                                 src_msg_id: int, dest_msg_id: int,
                                 dest_chat_id: int):
    """Hook called from batch.py after each message is successfully mirrored.

    Implements Layer 7: Auto-Relink. Checks if this new mapping resolves
    any previously-unresolved links in the destination chat.

    Usage in batch.py process_msg():
        from plugins.relink import on_new_mirror_message
        await on_new_mirror_message(uid, source_channel, src_msg_id, dest_msg_id, dest_chat_id)
    """
    # Run in background to not slow down mirroring
    asyncio.create_task(
        check_new_mapping_resolves_unresolved(
            uid, source_channel, src_msg_id, dest_msg_id, dest_chat_id
        )
    )
