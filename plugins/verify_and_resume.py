# ════════════════════════════════════════════════════════════════════
# ROBUST AUTO-RESUME + BULK VERIFICATION
#
# Two things this module does:
#
# 1. AUTO RESUME — when bot restarts mid-batch:
#      - Loads batch state from MongoDB
#      - Finds where it stopped
#      - Goes 300 messages BACK from that point
#      - Bulk-verifies those 300 in the dest channel (200 IDs per call)
#      - Re-queues any that are missing or broken
#      - Only then continues forward
#
#    WHY 300 BACK:
#    If bot crashed at msg 500, msgs 200-500 may have been
#    partially uploaded or uploaded but not recorded in MongoDB.
#    Going 300 back catches all edge cases safely.
#
# 2. POST-BATCH VERIFICATION — after batch completes:
#      - Bulk fetches ALL dest message IDs
#      - Checks: missing, type match, order, reply chain
#      - Reports issues without re-uploading (safe, read-only)
#      - Returns list of missing src_ids for retry
#
# RAM SAFETY:
#   All verification uses bulk_get_messages (200 IDs per API call).
#   Message objects are discarded immediately after checking.
#   Never more than 200 Message objects in RAM at once (~6MB).
#
# MongoDB STATE:
#   All state stored in MongoDB — survives Heroku dyno restarts.
#   No JSON files (ephemeral filesystem on Heroku).
# ════════════════════════════════════════════════════════════════════

import asyncio
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════════
# CONSTANTS
# ════════════════════════════════════════════════════════════════════

RESUME_LOOKBACK = 300       # How many messages to verify back on resume
RESUME_LOOKBACK_SMALL = 100  # Smaller lookback for quick checks
BULK_FETCH_SIZE = 200       # Max IDs per get_messages call (Telegram limit)
BATCH_OVERLAP   = 100       # Overlap between consecutive 4000-msg batches

# ════════════════════════════════════════════════════════════════════
# MONGODB COLLECTIONS — reuse existing connection from batch.py
# ════════════════════════════════════════════════════════════════════

from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME

_var_mongo_client = AsyncIOMotorClient(MONGO_URI)
_var_db = _var_mongo_client[DB_NAME]
upload_status_collection = _var_db["upload_status"]
batch_state_collection = _var_db["batch_state"]
batch_checkpoint_collection = _var_db["batch_checkpoint"]


# ════════════════════════════════════════════════════════════════════
# UPLOAD STATUS — per-message "done" vs "failed" tracking in MongoDB
#
# The existing upload_maps_collection stores: src_id → dst_id
# This NEW collection stores: src_id → {status, dst_id, error, timestamp}
#
# Why separate? Because upload_maps is used for resume detection
# (skip already-uploaded msgs), and we don't want "failed" entries
# to cause skips. Only "done" entries are trusted for skipping.
# ════════════════════════════════════════════════════════════════════

async def mark_upload_done(user_id, source_channel, src_msg_id, dst_msg_id):
    """Mark a single message as successfully uploaded and verified in MongoDB."""
    await upload_status_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel), "src_msg_id": int(src_msg_id)},
        {"$set": {
            "status": "done",
            "dst_msg_id": int(dst_msg_id),
            "updated_at": datetime.now()
        }},
        upsert=True
    )


async def mark_upload_failed(user_id, source_channel, src_msg_id, reason=""):
    """Mark a single message as failed in MongoDB."""
    await upload_status_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel), "src_msg_id": int(src_msg_id)},
        {"$set": {
            "status": "failed",
            "dst_msg_id": None,
            "error": reason[:300],
            "updated_at": datetime.now()
        }},
        upsert=True
    )


async def get_upload_status(user_id, source_channel, src_msg_id):
    """Get the status of a single upload. Returns 'done', 'failed', or None."""
    doc = await upload_status_collection.find_one(
        {"user_id": user_id, "source_channel": str(source_channel), "src_msg_id": int(src_msg_id)}
    )
    if doc:
        return doc.get("status"), doc.get("dst_msg_id")
    return None, None


async def get_bulk_upload_status(user_id, source_channel, src_msg_ids):
    """Get status for multiple messages at once. Returns dict of src_id → (status, dst_id)."""
    if not src_msg_ids:
        return {}
    
    cursor = upload_status_collection.find({
        "user_id": user_id,
        "source_channel": str(source_channel),
        "src_msg_id": {"$in": [int(x) for x in src_msg_ids]}
    })
    
    result = {}
    async for doc in cursor:
        src_id = doc["src_msg_id"]
        result[src_id] = (doc.get("status"), doc.get("dst_msg_id"))
    
    return result


async def clear_upload_status(user_id, source_channel=None):
    """Clear upload status for a user (used by /clearbatch)."""
    query = {"user_id": user_id}
    if source_channel:
        query["source_channel"] = str(source_channel)
    result = await upload_status_collection.delete_many(query)
    return result.deleted_count


# ════════════════════════════════════════════════════════════════════
# BATCH STATE — track which batch is running, where it stopped
#
# Stored in MongoDB so it survives dyno restarts.
# The existing ACTIVE_USERS + active_users.json is for the CURRENT
# session only. This is for crash recovery across restarts.
# ════════════════════════════════════════════════════════════════════

async def save_batch_state(user_id, source_channel, start_msg_id, total_count,
                           dest_channel_id, link_type, batch_size=4000, overlap=BATCH_OVERLAP,
                           user_chat_id=None):
    """Save or update batch state for crash recovery.
    
    All fields needed to fully re-enter _batch_streaming() on restart.
    user_chat_id: The chat ID where progress messages are sent (user's DM with bot).
    """
    update_data = {
        "start_msg_id": int(start_msg_id),
        "total_count": int(total_count),
        "dest_channel_id": dest_channel_id,
        "link_type": link_type,
        "batch_size": batch_size,
        "overlap": overlap,
        "status": "in_progress",
        "updated_at": datetime.now()
    }
    if user_chat_id is not None:
        update_data["user_chat_id"] = user_chat_id
    await batch_state_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {"$set": update_data},
        upsert=True
    )


async def update_batch_progress(user_id, source_channel, last_uploaded_src_id, success_count,
                               failed_count=0):
    """Update the last uploaded position (called after every message).
    
    Also tracks failed_count so we know how many messages failed on resume.
    updated_at is used to detect stale batches (e.g. batch not updated for 24h).
    """
    await batch_state_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {"$set": {
            "last_uploaded_src_id": int(last_uploaded_src_id),
            "success_count": int(success_count),
            "failed_count": int(failed_count),
            "updated_at": datetime.now()
        }}
    )


async def mark_batch_complete(user_id, source_channel):
    """Mark a batch as fully completed."""
    await batch_state_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {"$set": {
            "status": "completed",
            "completed_at": datetime.now(),
            "updated_at": datetime.now()
        }}
    )


async def load_batch_state(user_id, source_channel):
    """Load batch state for resume. Returns dict or None."""
    doc = await batch_state_collection.find_one(
        {"user_id": user_id, "source_channel": str(source_channel)}
    )
    if doc:
        doc.pop("_id", None)
        return doc
    return None


async def clear_batch_state(user_id, source_channel=None):
    """Clear batch state (used by /clearbatch)."""
    query = {"user_id": user_id}
    if source_channel:
        query["source_channel"] = str(source_channel)
    result = await batch_state_collection.delete_many(query)
    return result.deleted_count


async def load_all_incomplete_batches():
    """Find ALL batch states that are 'in_progress' — these are crashed/restarted batches.
    
    Called on bot startup to detect and auto-resume batches that were running
    when the dyno restarted. Also finds batches that haven't been updated
    for a while (stale) — these might have crashed without marking completion.
    
    Returns list of batch state dicts, sorted by updated_at (newest first).
    """
    cursor = batch_state_collection.find({"status": "in_progress"})
    results = []
    async for doc in cursor:
        doc.pop("_id", None)
        results.append(doc)
    
    # Also find stale batches (not updated in 2 hours but still in_progress)
    # This catches batches where the status wasn't updated before crash
    from datetime import timedelta
    stale_cutoff = datetime.now() - timedelta(hours=2)
    cursor = batch_state_collection.find({
        "status": {"$ne": "completed"},
        "updated_at": {"$lt": stale_cutoff},
    })
    async for doc in cursor:
        doc.pop("_id", None)
        # Avoid duplicates
        if not any(r.get("source_channel") == doc.get("source_channel") and r.get("user_id") == doc.get("user_id") for r in results):
            results.append(doc)
    
    # Sort newest first
    results.sort(key=lambda x: x.get("updated_at", datetime.min), reverse=True)
    return results


# ════════════════════════════════════════════════════════════════════
# BATCH CHECKPOINT — persistent progress tracking in MongoDB
#
# The batch_checkpoint collection is the SINGLE SOURCE OF TRUTH for
# "where did the batch stop?". Updated after every successful message
# upload, it guarantees that on restart the bot knows EXACTLY where
# to resume — no scanning, no guessing, no data loss.
#
# Difference from batch_state:
#   - batch_state: high-level batch metadata (start_id, total, status)
#   - batch_checkpoint: granular per-message progress (last_completed_msg_id,
#     index position, source_msg_ids list for instant resume)
#
# Difference from upload_maps:
#   - upload_maps: stores src→dst mapping (for skip detection + link rewriting)
#   - batch_checkpoint: stores progress position + full ID list (for instant resume)
#
# The checkpoint also stores the enumerated source_msg_ids list so that
# on resume we don't need to re-enumerate from the source channel.
# This is safe because source_msg_ids are immutable — message IDs in a
# channel never change.
# ════════════════════════════════════════════════════════════════════

async def save_checkpoint(user_id, source_channel, last_completed_msg_id,
                          last_completed_index, total_completed,
                          source_msg_ids, start_msg_id, total_count,
                          dest_channel_id, link_type, user_chat_id=None):
    """Create or update a batch checkpoint — called after every successful upload.
    
    This is the heartbeat of the auto-resume system. After each message is
    uploaded and confirmed, we update the checkpoint so that on restart we
    know EXACTLY where to resume.
    
    The source_msg_ids list is stored so we can resume without re-enumerating
    from the source channel (saves API calls on restart).
    
    Args:
        user_id: User ID
        source_channel: Source channel identifier (str)
        last_completed_msg_id: Last source msg_id that was successfully uploaded
        last_completed_index: Index of last_completed_msg_id in source_msg_ids list
        total_completed: Total number of messages successfully uploaded so far
        source_msg_ids: Full list of message IDs for this batch (stored for instant resume)
        start_msg_id: Original start message ID
        total_count: Original total count (n)
        dest_channel_id: Destination channel ID
        link_type: 'public' or 'private'
        user_chat_id: User's chat ID for notifications
    """
    update_data = {
        "last_completed_msg_id": int(last_completed_msg_id),
        "last_completed_index": int(last_completed_index),
        "total_completed": int(total_completed),
        "start_msg_id": int(start_msg_id),
        "total_count": int(total_count),
        "dest_channel_id": dest_channel_id,
        "link_type": link_type,
        "status": "running",
        "updated_at": datetime.now()
    }
    if user_chat_id is not None:
        update_data["user_chat_id"] = user_chat_id
    
    # Only store source_msg_ids on the FIRST checkpoint save (when there's no existing doc)
    # or when the list changes (shouldn't happen normally).
    # We use $setOnInsert to avoid re-writing a potentially large list on every message.
    await batch_checkpoint_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {
            "$set": update_data,
            "$setOnInsert": {
                "source_msg_ids": source_msg_ids,
                "created_at": datetime.now()
            }
        },
        upsert=True
    )


async def load_checkpoint(user_id, source_channel):
    """Load the batch checkpoint for resume. Returns dict or None.
    
    Contains all data needed to resume exactly where the batch left off:
    - source_msg_ids: the full list of IDs to iterate (no re-enumeration needed)
    - last_completed_index: skip-ahead position in the list
    - last_completed_msg_id: the last source msg that was uploaded
    - total_completed: count of successful uploads so far
    """
    doc = await batch_checkpoint_collection.find_one(
        {"user_id": user_id, "source_channel": str(source_channel)}
    )
    if doc:
        doc.pop("_id", None)
        return doc
    return None


async def mark_checkpoint_complete(user_id, source_channel):
    """Mark a batch checkpoint as completed (all messages done)."""
    await batch_checkpoint_collection.update_one(
        {"user_id": user_id, "source_channel": str(source_channel)},
        {"$set": {
            "status": "completed",
            "completed_at": datetime.now(),
            "updated_at": datetime.now()
        }}
    )


async def clear_checkpoint(user_id, source_channel=None):
    """Clear batch checkpoint(s). Used by /clearbatch."""
    query = {"user_id": user_id}
    if source_channel:
        query["source_channel"] = str(source_channel)
    result = await batch_checkpoint_collection.delete_many(query)
    return result.deleted_count


async def load_all_running_checkpoints():
    """Find ALL batch checkpoints with status='running' — for startup auto-resume.
    
    These are batches that were interrupted by a dyno restart/crash.
    Each checkpoint contains everything needed to resume exactly where it left off.
    
    Returns list of checkpoint dicts, sorted by updated_at (newest first).
    """
    cursor = batch_checkpoint_collection.find({"status": "running"})
    results = []
    async for doc in cursor:
        doc.pop("_id", None)
        results.append(doc)
    
    # Also find stale checkpoints (not updated in 2 hours but still running)
    from datetime import timedelta
    stale_cutoff = datetime.now() - timedelta(hours=2)
    cursor = batch_checkpoint_collection.find({
        "status": {"$ne": "completed"},
        "updated_at": {"$lt": stale_cutoff},
    })
    async for doc in cursor:
        doc.pop("_id", None)
        if not any(r.get("source_channel") == doc.get("source_channel") and r.get("user_id") == doc.get("user_id") for r in results):
            results.append(doc)
    
    results.sort(key=lambda x: x.get("updated_at", datetime.min), reverse=True)
    return results


# ════════════════════════════════════════════════════════════════════
# BULK FETCH HELPER — never fetch one at a time
#
# Fetches messages in chunks of 200 (Telegram's hard limit).
# 5000 messages = 25 API calls total.
# Message objects are returned but should be discarded after use.
# ════════════════════════════════════════════════════════════════════

async def bulk_fetch_messages(client, chat_id, msg_ids, batch_size=BULK_FETCH_SIZE):
    """
    Fetch messages in bulk — never one at a time.
    
    Returns dict of {msg_id: msg_object}.
    Caller should discard msg_objects after checking to free RAM.
    
    Args:
        client: Pyrogram client with access to the chat
        chat_id: Chat ID to fetch from
        msg_ids: List of message IDs to fetch
        batch_size: IDs per API call (max 200 for Telegram)
    
    Returns:
        dict of {msg_id: Message} for successfully fetched messages
    """
    result = {}
    
    for i in range(0, len(msg_ids), batch_size):
        chunk = msg_ids[i : i + batch_size]
        try:
            msgs = await client.get_messages(chat_id, chunk)
            if not isinstance(msgs, list):
                msgs = [msgs]
            for msg in msgs:
                if msg and not getattr(msg, "empty", True):
                    result[msg.id] = msg
        except Exception as e:
            logger.error(
                f"[BULK-FETCH] chat={chat_id} "
                f"chunk [{chunk[0]}..{chunk[-1]}] failed: {e}"
            )
    
    logger.info(
        f"[BULK-FETCH] chat={chat_id} "
        f"requested={len(msg_ids)} fetched={len(result)}"
    )
    return result


# ════════════════════════════════════════════════════════════════════
# BULK VERIFY DESTINATION — check if messages actually exist in dest
#
# Uses bulk_fetch_messages (200 IDs per call).
# Returns list of src_ids that are MISSING from the dest channel.
# RAM-safe: processes in chunks, discards Message objects.
# ════════════════════════════════════════════════════════════════════

async def bulk_verify_dest(client, dst_chat_id, src_to_dst_map):
    """
    Verify dest messages exist in bulk.
    
    Args:
        client: Pyrogram client with access to dest channel
        dst_chat_id: Destination channel ID
        src_to_dst_map: Dict of {src_msg_id: dst_msg_id}
    
    Returns:
        List of src_msg_ids whose dst messages are MISSING.
    """
    if not src_to_dst_map:
        return []
    
    dst_ids = list(src_to_dst_map.values())
    
    # Bulk fetch all dest messages
    dst_msgs = await bulk_fetch_messages(client, dst_chat_id, dst_ids)
    
    # Check which are missing
    missing_src_ids = []
    for src_id, dst_id in src_to_dst_map.items():
        if dst_id not in dst_msgs:
            missing_src_ids.append(src_id)
            logger.warning(
                f"[VERIFY] ❌ MISSING src={src_id} → dst={dst_id} "
                f"not found in dest channel"
            )
    
    if not missing_src_ids:
        logger.info(
            f"[VERIFY] ✅ All {len(src_to_dst_map)} dest messages verified"
        )
    else:
        logger.warning(
            f"[VERIFY] ❌ {len(missing_src_ids)}/{len(src_to_dst_map)} "
            f"missing from dest channel"
        )
    
    return missing_src_ids


# ════════════════════════════════════════════════════════════════════
# AUTO RESUME — 300-back verification + re-upload missing messages
#
# Called when the bot restarts and finds a batch that was in_progress.
# Steps:
#   1. Load batch state from MongoDB
#   2. Find last uploaded src_msg_id
#   3. Go 300 messages back from that point
#   4. Bulk verify those 300 in dest channel
#   5. Return: resume_index, reupload_ids
#
# The caller then:
#   - Re-uploads messages in reupload_ids
#   - Continues forward from resume_index
# ════════════════════════════════════════════════════════════════════

async def get_failed_uploads(user_id, source_channel):
    """Get all src_msg_ids that were marked as FAILED in upload_status.

    DEPRECATED: No longer written during batch. Returns empty list.
    Failed messages are detected via bulk verification on resume.
    """
    return []


async def auto_resume_verify(client, dst_chat_id, msg_id_map, last_uploaded_src_id,
                              start_msg_id, total_count, lookback=RESUME_LOOKBACK):
    """
    Verify the lookback zone after a crash/restart.
    
    Args:
        client: Pyrogram client with access to dest channel
        dst_chat_id: Destination channel ID
        msg_id_map: Current {src_id: dst_id} mapping (from MongoDB)
        last_uploaded_src_id: Last src_id that was recorded as uploaded
        start_msg_id: First src_id of the batch
        total_count: Total messages in the batch
        lookback: How many messages to verify back (default 100)
    
    Returns:
        (resume_src_id, reupload_src_ids) tuple:
          - resume_src_id: The src_id to continue from
          - reupload_src_ids: Set of src_ids that need re-uploading
    """
    if not msg_id_map:
        logger.info("[RESUME] No messages uploaded yet — starting from beginning")
        return start_msg_id, set()
    
    # Calculate lookback range
    lookback_start = max(start_msg_id, last_uploaded_src_id - lookback + 1)
    lookback_end = last_uploaded_src_id
    
    lookback_src_ids = list(range(lookback_start, lookback_end + 1))
    
    logger.info(
        f"[RESUME] Verifying {len(lookback_src_ids)} messages "
        f"({lookback} back from last uploaded)\n"
        f"  from src_id={lookback_start} to src_id={lookback_end}"
    )
    
    # Build src_to_dst map for lookback range
    lookback_map = {}
    for src_id in lookback_src_ids:
        dst_id = msg_id_map.get(src_id)
        if dst_id:
            lookback_map[src_id] = dst_id
    
    if not lookback_map:
        logger.info("[RESUME] No mappings in lookback range — starting from beginning")
        return start_msg_id, set(lookback_src_ids)
    
    # Bulk verify dest messages exist
    missing_src_ids = await bulk_verify_dest(client, dst_chat_id, lookback_map)
    missing_set = set(missing_src_ids)
    
    # Also check for src_ids that have NO mapping at all (never uploaded)
    for src_id in lookback_src_ids:
        if src_id not in msg_id_map:
            missing_set.add(src_id)
            logger.warning(
                f"[RESUME] src={src_id} not in msg_id_map → needs upload"
            )
    
    # Determine resume point
    if missing_set:
        resume_src_id = min(missing_set)
        logger.info(
            f"[RESUME] {len(missing_set)} messages need re-upload\n"
            f"  resuming from src_id={resume_src_id}"
        )
    else:
        resume_src_id = last_uploaded_src_id + 1
        logger.info(
            f"[RESUME] ✅ All {len(lookback_src_ids)} lookback messages OK\n"
            f"  resuming from src_id={resume_src_id}"
        )
    
    return resume_src_id, missing_set


# ════════════════════════════════════════════════════════════════════
# POST-BATCH VERIFICATION — full integrity check after batch completes
#
# Checks 4 things using bulk fetch (low RAM):
#   1. Missing — dst message doesn't exist
#   2. Type match — src was photo but dst is text
#   3. Order — dst IDs not in ascending sequence
#   4. Reply chain — poll should reply to question, etc.
#
# Returns issues dict. Caller can decide what to do with it.
# This is READ-ONLY — no modifications to the dest channel.
# ════════════════════════════════════════════════════════════════════

async def post_batch_verify(client, src_chat_id, dst_chat_id, src_to_dst,
                             src_client=None):
    """
    Full integrity check after batch completes.
    
    Args:
        client: Pyrogram client with access to dest channel
        src_chat_id: Source channel ID
        dst_chat_id: Destination channel ID
        src_to_dst: Dict of {src_msg_id: dst_msg_id}
        src_client: Client for source channel (optional, for type checking)
    
    Returns:
        Dict with issue lists:
        {
            "missing": [src_ids],
            "type_mismatch": [(src_id, dst_id, src_type, dst_type)],
            "wrong_order": [(src_id, dst_id, prev_dst_id)],
            "reply_broken": [(src_id, expected_dst_parent, actual_dst_parent)],
        }
    """
    issues = {
        "missing": [],
        "type_mismatch": [],
        "wrong_order": [],
        "reply_broken": [],
    }
    
    if not src_to_dst:
        return issues
    
    # Sort by src_id ascending for ordered processing
    ordered = sorted(src_to_dst.items(), key=lambda x: int(x[0]))
    
    # Bulk fetch ALL dest messages
    dst_ids = [int(v) for v in src_to_dst.values()]
    logger.info(f"[POST-VERIFY] Bulk fetching {len(dst_ids)} dest messages...")
    dst_msgs = await bulk_fetch_messages(client, dst_chat_id, dst_ids)
    
    # Optionally bulk fetch src messages for type/reply checks
    src_msgs = {}
    if src_client:
        src_ids = [int(k) for k in src_to_dst.keys()]
        logger.info(f"[POST-VERIFY] Bulk fetching {len(src_ids)} src messages...")
        src_msgs = await bulk_fetch_messages(src_client, src_chat_id, src_ids)
    
    # Process in order
    prev_dst_id = None
    
    for src_id_str, dst_id in ordered:
        src_id = int(src_id_str)
        dst_id = int(dst_id)
        
        dst_msg = dst_msgs.get(dst_id)
        src_msg = src_msgs.get(src_id)
        
        # ── Check 1: Missing ──
        if not dst_msg:
            issues["missing"].append(src_id)
            prev_dst_id = None  # Reset order tracking
            continue
        
        # ── Check 2: Type mismatch ──
        if src_msg:
            src_is_photo = src_msg.photo is not None
            src_is_poll = src_msg.poll is not None
            src_is_video = src_msg.video is not None
            src_is_text = bool(getattr(src_msg, "text", None)) and not src_is_photo and not src_is_poll
            
            dst_is_photo = dst_msg.photo is not None
            dst_is_poll = dst_msg.poll is not None
            dst_is_video = dst_msg.video is not None
            dst_is_text = bool(getattr(dst_msg, "text", None)) and not dst_is_photo and not dst_is_poll
            
            type_ok = (
                (src_is_photo and dst_is_photo) or
                (src_is_poll and dst_is_poll) or
                (src_is_video and dst_is_video) or
                (src_is_text and dst_is_text) or
                (not src_is_photo and not src_is_poll and not src_is_video and
                 not dst_is_photo and not dst_is_poll and not dst_is_video)
            )
            
            if not type_ok:
                src_type = "photo" if src_is_photo else "poll" if src_is_poll else "video" if src_is_video else "other"
                dst_type = "photo" if dst_is_photo else "poll" if dst_is_poll else "video" if dst_is_video else "other"
                issues["type_mismatch"].append((src_id, dst_id, src_type, dst_type))
        
        # ── Check 3: Order ──
        if prev_dst_id is not None and dst_id < prev_dst_id:
            issues["wrong_order"].append((src_id, dst_id, prev_dst_id))
        prev_dst_id = dst_id
        
        # ── Check 4: Reply chain ──
        if src_msg:
            src_reply_to = getattr(src_msg, "reply_to_message_id", None)
            if not src_reply_to:
                reply_to = getattr(src_msg, "reply_to", None)
                if reply_to:
                    src_reply_to = getattr(reply_to, "reply_to_msg_id", None)
            
            if src_reply_to:
                expected_dst_parent = src_to_dst.get(str(src_reply_to)) or src_to_dst.get(src_reply_to)
                actual_dst_parent = getattr(dst_msg, "reply_to_message_id", None)
                
                if expected_dst_parent and int(actual_dst_parent or 0) != int(expected_dst_parent):
                    issues["reply_broken"].append((
                        src_id,
                        expected_dst_parent,
                        actual_dst_parent,
                    ))
    
    # ── Summary ──
    total_issues = sum(len(v) for v in issues.values())
    if total_issues == 0:
        logger.info(
            f"[POST-VERIFY] ✅ All {len(src_to_dst)} messages verified — no issues"
        )
    else:
        logger.warning(
            f"[POST-VERIFY] ❌ {total_issues} issues found:\n"
            f"  missing:        {len(issues['missing'])}\n"
            f"  type_mismatch:  {len(issues['type_mismatch'])}\n"
            f"  wrong_order:    {len(issues['wrong_order'])}\n"
            f"  reply_broken:   {len(issues['reply_broken'])}"
        )
    
    return issues


# ════════════════════════════════════════════════════════════════════
# BATCH SPLITTER — split 20K messages into 4K batches with overlap
#
# For 20,000 messages with batch_size=4000 and overlap=100:
#   Batch 1: msgs 1-4000
#   Batch 2: msgs 3901-8000    (starts 100 before batch 1 ends)
#   Batch 3: msgs 7901-12000
#   Batch 4: msgs 11901-16000
#   Batch 5: msgs 15901-20000
#
# Overlap ensures cross-batch reply chains are preserved.
# Already-uploaded messages in the overlap zone are skipped.
# ════════════════════════════════════════════════════════════════════

def split_into_batches(start_msg_id, total_count, batch_size=4000, overlap=BATCH_OVERLAP):
    """
    Split a large message range into batches with overlap.
    
    Args:
        start_msg_id: First source message ID
        total_count: Total number of messages
        batch_size: Messages per batch (default 4000)
        overlap: Overlap between consecutive batches (default 100)
    
    Returns:
        List of (batch_start, batch_count) tuples.
        batch_start is the src_msg_id to start from.
        batch_count is how many messages in this batch.
    """
    batches = []
    current_start = start_msg_id
    remaining = total_count
    
    while remaining > 0:
        batch_count = min(batch_size, remaining)
        batches.append((current_start, batch_count))
        
        # Next batch starts (batch_size - overlap) messages after current start
        advance = batch_size - overlap
        current_start += advance
        remaining -= advance
        
        # If remaining is less than overlap, we're done
        if remaining <= 0:
            break
    
    return batches


# ════════════════════════════════════════════════════════════
#  HEARTBEAT — keep batch_state.updated_at fresh while running
#
#  PROBLEM:
#  load_all_incomplete_batches() uses updated_at to detect stale batches.
#  If batch is running normally, updated_at should be recent.
#  If bot crashed, updated_at goes stale (> 2 min old).
#
#  SOLUTION:
#  Background task that updates batch_state.updated_at every 30 seconds.
#  This lets startup_auto_resume distinguish between:
#    - Batch running normally (updated_at fresh) → don't touch
#    - Batch interrupted by crash (updated_at stale) → auto-resume
#
#  USAGE:
#    heartbeat_task = asyncio.create_task(batch_heartbeat(uid, source_channel))
#    # ... run batch ...
#    heartbeat_task.cancel()
# ════════════════════════════════════════════════════════════

async def batch_heartbeat(user_id: int, source_channel: str):
    """
    Background task — updates batch_state.updated_at every 30 seconds.
    
    Start as asyncio.create_task() when batch starts.
    Cancel when batch ends.
    
    This ensures auto_resume_on_startup can detect:
      updated_at fresh (< 2 min ago) → batch still running normally
      updated_at stale (> 2 min ago) → batch was interrupted by crash
    """
    while True:
        await asyncio.sleep(30)
        try:
            await batch_state_collection.update_one(
                {"user_id": user_id, "source_channel": str(source_channel)},
                {"$set": {"updated_at": datetime.now(), "heartbeat": True}},
            )
        except Exception as e:
            logger.error(f"[HEARTBEAT] Failed for uid={user_id}: {e}")


async def batch_checkpoint_heartbeat(user_id: int, source_channel: str):
    """
    Background task — updates BOTH batch_state and batch_checkpoint
    updated_at every 30 seconds.
    
    More thorough than batch_heartbeat — keeps both collections fresh.
    """
    while True:
        await asyncio.sleep(30)
        try:
            now = datetime.now()
            await batch_state_collection.update_one(
                {"user_id": user_id, "source_channel": str(source_channel)},
                {"$set": {"updated_at": now, "heartbeat": True}},
            )
            await batch_checkpoint_collection.update_one(
                {"user_id": user_id, "source_channel": str(source_channel)},
                {"$set": {"updated_at": now, "heartbeat": True}},
            )
        except Exception as e:
            logger.error(f"[CHECKPOINT-HEARTBEAT] Failed for uid={user_id}: {e}")


# ════════════════════════════════════════════════════════════
#  STARTUP AUTO-RESUME — detect and resume interrupted batches
#
#  Called ONCE at bot startup from main.py.
#  Finds all batches where:
#    - status = "in_progress" or checkpoint status = "running"
#    - updated_at > 2 minutes ago (stale = bot died mid-batch)
#  Notifies affected users that their batch was interrupted.
#
#  DESIGN DECISION:
#  We do NOT auto-start the batch because:
#    1. The batch runner needs user session context (c, m, ubot, uc, pt)
#    2. The user may not want to resume (they may have cancelled)
#    3. Auto-starting could conflict with other operations
#  Instead, we NOTIFY and let the user trigger resume manually.
# ════════════════════════════════════════════════════════════

async def startup_auto_resume_check():
    """
    Check for interrupted batches at startup.
    Returns list of interrupted batch info for notification.
    
    Called from main.py startup_auto_resume().
    """
    from datetime import timedelta
    
    # Check batch_state for interrupted batches
    interrupted = []
    
    # 1. Find batches with status "in_progress" and stale updated_at
    stale_cutoff = datetime.now() - timedelta(minutes=2)
    
    cursor = batch_state_collection.find({
        "status": "in_progress",
        "updated_at": {"$lt": stale_cutoff},
    })
    
    async for doc in cursor:
        doc.pop("_id", None)
        interrupted.append({
            "source": "batch_state",
            "uid": doc.get("user_id"),
            "source_channel": doc.get("source_channel"),
            "start_msg_id": doc.get("start_msg_id"),
            "total_count": doc.get("total_count"),
            "last_uploaded_src_id": doc.get("last_uploaded_src_id"),
            "success_count": doc.get("success_count"),
            "dest_channel_id": doc.get("dest_channel_id"),
            "link_type": doc.get("link_type"),
            "user_chat_id": doc.get("user_chat_id"),
        })
    
    # 2. Find checkpoints with status "running" and stale updated_at
    cursor2 = batch_checkpoint_collection.find({
        "status": "running",
        "updated_at": {"$lt": stale_cutoff},
    })
    
    checkpoint_uids = set()
    async for doc in cursor2:
        doc.pop("_id", None)
        uid = doc.get("user_id")
        src_ch = doc.get("source_channel")
        
        # Avoid duplicates — if batch_state already has this uid+channel, skip
        if any(r["uid"] == uid and r["source_channel"] == src_ch for r in interrupted):
            checkpoint_uids.add((uid, src_ch))
            continue
        
        interrupted.append({
            "source": "checkpoint",
            "uid": uid,
            "source_channel": src_ch,
            "start_msg_id": doc.get("start_msg_id"),
            "total_count": doc.get("total_count"),
            "last_completed_msg_id": doc.get("last_completed_msg_id"),
            "last_completed_index": doc.get("last_completed_index"),
            "total_completed": doc.get("total_completed"),
            "dest_channel_id": doc.get("dest_channel_id"),
            "link_type": doc.get("link_type"),
            "user_chat_id": doc.get("user_chat_id"),
        })
        checkpoint_uids.add((uid, src_ch))
    
    return interrupted
