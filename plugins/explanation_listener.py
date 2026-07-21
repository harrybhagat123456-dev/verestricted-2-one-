# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

"""
Persistent explanation lookup for poll/quiz messages.

Architecture:
  RESTART
    └── load JSON from disk (instant)
          └── scan only messages newer than last known (fast)
                └── register watcher

  NEW POLL POSTED (in source channel)
    └── watcher fires → poll_ids.add(msg.id) → saved

  POLL FORWARDED TO DEST CHANNEL (by batch.py)
    └── poll sent with ONLY 💡 View Answer button
    └── 📖 View Explanation button DISABLED — removed per user request

  EXPLANATION POSTED (in source channel, replies to poll)
    └── watcher fires → reply_id in poll_ids? YES
          → store explanation in CHANNEL_EXPLANATIONS (used for Telegraph 💡 View Answer)
          → 📖 View Explanation button DISABLED — no copy to dest, no button added

  POLL ANSWER EVENT FIRES
    └── explanation_lookup.get(poll_id) → instant
          → 0 API calls, 0 scanning

The 📖 View Explanation button has been permanently removed.
Explanations are still stored for the 💡 View Answer Telegraph page.

Fallback chain for misses:
  1. CHANNEL_EXPLANATIONS lookup (instant, from JSON + watchers)
  2. find_explanation_batch() (parallel batch ID fetch [poll+1..poll+1500], ~0.6s)
  3. check_poll_builtin_explanation() (poll's built-in solution field)
"""

import os
import json
import asyncio
from datetime import datetime
from pyrogram.errors import FloodWait
from pyrogram import Client, filters
from pyrogram.types import Message
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME
from shared_client import app as X


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


# ─── Reply-to extraction helper ─────────────────────────────────────────────

def _get_reply_to_id(msg) -> int | None:
    """Reliably extract reply_to_msg_id from a Pyrogram message.

    Works across different Pyrogram/Pyrofork versions that may store
    the reply-to ID in different attributes.
    """
    # High-level attribute (most common)
    val = getattr(msg, "reply_to_message_id", None)
    if val:
        return val

    # Raw reply_to object
    reply_to = getattr(msg, "reply_to", None)
    if reply_to:
        for attr in ("reply_to_msg_id", "message_id", "reply_to_message_id"):
            val = getattr(reply_to, attr, None)
            if val:
                return val

    return None


# ─── MongoDB ─────────────────────────────────────────────────────────────────

_mongo_client = AsyncIOMotorClient(MONGO_URI)
_db = _mongo_client[DB_NAME]
poll_explanations_collection = _db["poll_explanations"]
monitored_channels_collection = _db["monitored_channels"]

# In-memory sets for fast lookup
MONITORED_CHANNELS = set()   # set of str channel_ids
KNOWN_POLLS = {}             # {str(channel_id): set(int(poll_msg_id))}


# ═══════════════════════════════════════════════════════════════
# PERSISTENT STATE — JSON file for instant restart + watcher updates
# ═══════════════════════════════════════════════════════════════

_STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'explanation_state.json'
)

# In-memory explanation lookup: {str(channel_id): {int(poll_msg_id): entry_dict}}
# entry_dict = {
#     "explanation_msg_id": int,
#     "text": str | None,
#     "has_photo": bool,
#     "photo_file_id": str | None,
#     "kind": str            # "photo" | "text" | "photo+text"
# }
CHANNEL_EXPLANATIONS = {}

# Last scanned message ID per channel — for incremental scan
LAST_SCANNED_MSG_ID = {}     # {str(channel_id): int}


def _save_state():
    """Persist explanation state to JSON file (synchronous — fast for small data).

    Called after every watcher update so state survives restarts.
    """
    try:
        state = {"channels": {}}
        for ch_id in MONITORED_CHANNELS:
            ch_polls = sorted(KNOWN_POLLS.get(ch_id, set()))
            ch_expl = {}
            for poll_id, entry in CHANNEL_EXPLANATIONS.get(ch_id, {}).items():
                ch_expl[str(poll_id)] = {
                    "explanation_msg_id": entry["explanation_msg_id"],
                    "text": entry.get("text"),
                    "has_photo": entry.get("has_photo", False),
                    "has_video": entry.get("has_video", False),
                    "photo_file_id": entry.get("photo_file_id"),
                    "kind": entry.get("kind", "text"),
                    "has_document": entry.get("has_document", False),
                }
            state["channels"][ch_id] = {
                "poll_ids": ch_polls,
                "explanation_lookup": ch_expl,
                "last_scanned_msg_id": LAST_SCANNED_MSG_ID.get(ch_id, 0),
            }
        with open(_STATE_FILE, 'w') as f:
            json.dump(state, f)
    except Exception as e:
        print(f"[EXPLANATION-STATE] Error saving state: {e}")


def _load_state():
    """Load explanation state from JSON file.

    Returns True if state was loaded, False if starting fresh.
    """
    global CHANNEL_EXPLANATIONS, LAST_SCANNED_MSG_ID
    try:
        if not os.path.exists(_STATE_FILE):
            print("[EXPLANATION-STATE] No state file — starting fresh")
            return False

        with open(_STATE_FILE, 'r') as f:
            state = json.load(f)

        channels = state.get("channels", {})
        for ch_id, ch_data in channels.items():
            MONITORED_CHANNELS.add(ch_id)
            KNOWN_POLLS[ch_id] = set(int(p) for p in ch_data.get("poll_ids", []))

            CHANNEL_EXPLANATIONS[ch_id] = {}
            for poll_id_str, entry in ch_data.get("explanation_lookup", {}).items():
                CHANNEL_EXPLANATIONS[ch_id][int(poll_id_str)] = {
                    "explanation_msg_id": entry["explanation_msg_id"],
                    "text": entry.get("text"),
                    "has_photo": entry.get("has_photo", False),
                    "has_video": entry.get("has_video", False),
                    "photo_file_id": entry.get("photo_file_id"),
                    "kind": entry.get("kind", "text"),
                    "has_document": entry.get("has_document", False),
                }

            LAST_SCANNED_MSG_ID[ch_id] = ch_data.get("last_scanned_msg_id", 0)

        total_polls = sum(len(p) for p in KNOWN_POLLS.values())
        total_expl = sum(len(e) for e in CHANNEL_EXPLANATIONS.values())
        print(f"[EXPLANATION-STATE] Loaded from disk: {len(channels)} channels, "
              f"{total_polls} polls, {total_expl} explanations")
        return True
    except Exception as e:
        print(f"[EXPLANATION-STATE] Error loading state: {e}")
        return False


def get_explanation_lookup(channel_id):
    """Get the explanation lookup dict for a channel.

    Called from batch.py for instant lookup — 0 API calls, 0 scanning.
    Returns {int(poll_msg_id): entry_dict}.
    """
    ch = str(channel_id)
    return CHANNEL_EXPLANATIONS.get(ch, {})


async def _resolve_channel_key(channel_id, client=None):
    """Return the canonical numeric chat id string when it can be resolved."""
    ch = str(channel_id)
    if not client:
        return ch

    try:
        from plugins.batch import resolve_chat
        resolved = await resolve_chat(client, channel_id)
        return str(resolved)
    except Exception as e:
        print(f"[EXPLANATION] Could not resolve channel key {channel_id}: {e}")
        return ch


def _merge_channel_state(source_key, dest_key):
    """Move cached poll/explanation state from an alias key to the canonical key."""
    source_key = str(source_key)
    dest_key = str(dest_key)
    if source_key == dest_key:
        return

    if source_key in KNOWN_POLLS:
        KNOWN_POLLS.setdefault(dest_key, set()).update(KNOWN_POLLS[source_key])
    if source_key in CHANNEL_EXPLANATIONS:
        CHANNEL_EXPLANATIONS.setdefault(dest_key, {}).update(CHANNEL_EXPLANATIONS[source_key])
    if source_key in LAST_SCANNED_MSG_ID:
        LAST_SCANNED_MSG_ID[dest_key] = max(
            LAST_SCANNED_MSG_ID.get(dest_key, 0),
            LAST_SCANNED_MSG_ID[source_key],
        )

    MONITORED_CHANNELS.discard(source_key)


def _is_known_or_pending_poll(channel_id, poll_msg_id):
    """A reply is relevant if it targets a known poll or a pending copied poll."""
    ch = str(channel_id)
    try:
        poll_id = int(poll_msg_id)
    except Exception:
        return False

    if poll_id in KNOWN_POLLS.get(ch, set()):
        return True

    try:
        from plugins.batch import POLL_MAP
        return poll_id in POLL_MAP.get(ch, {})
    except Exception:
        return False


async def _incremental_scan_channel(client, channel_id):
    """Scan only messages newer than last_scanned_msg_id for a channel.

    Two-pass approach (same as full scan but only for new messages):
      Pass 1: identify new polls -> add to KNOWN_POLLS
      Pass 2: identify new explanations -> add to CHANNEL_EXPLANATIONS

    Updates last_scanned_msg_id and saves state to JSON.
    """
    ch = str(channel_id)
    last_id = LAST_SCANNED_MSG_ID.get(ch, 0)

    if ch not in KNOWN_POLLS:
        KNOWN_POLLS[ch] = set()
    if ch not in CHANNEL_EXPLANATIONS:
        CHANNEL_EXPLANATIONS[ch] = {}

    poll_ids = KNOWN_POLLS[ch]
    explanation_lookup = CHANNEL_EXPLANATIONS[ch]

    try:
        resolved = None
        try:
            from plugins.batch import resolve_chat
            resolved = await resolve_chat(client, ch)
        except Exception:
            resolved = ch

        # Collect all messages newer than last_scanned_msg_id
        # get_chat_history returns newest-first, so we break when msg.id <= last_id
        new_msgs = []
        max_msg_id = last_id

        async for msg in client.get_chat_history(resolved, limit=999999):
            if not msg or getattr(msg, 'empty', False):
                continue
            if msg.id <= last_id:
                break
            new_msgs.append(msg)
            if msg.id > max_msg_id:
                max_msg_id = msg.id

        if not new_msgs:
            print(f"[EXPLANATION-SCAN] Channel {ch}: no new messages since {last_id}")
            return

        # Pass 1: collect new poll IDs
        new_poll_count = 0
        for msg in new_msgs:
            if msg.poll is not None:
                poll_ids.add(msg.id)
                new_poll_count += 1

        # Pass 2: index new explanations (messages that reply to known polls)
        new_expl_count = 0
        for msg in new_msgs:
            reply_id = _get_reply_to_id(msg)
            if not reply_id:
                continue
            if not _is_known_or_pending_poll(ch, reply_id):
                continue

            has_photo = msg.photo is not None
            has_video = msg.video is not None or getattr(msg, 'video_note', None) is not None
            has_document = msg.document is not None
            text = msg.text or msg.caption or None

            # Skip if no usable content at all
            if not has_photo and not has_video and not has_document and not text:
                continue

            # Determine kind
            if has_video:
                kind = "video"
            elif has_document:
                kind = "document"
            elif has_photo and text:
                kind = "photo+text"
            elif has_photo:
                kind = "photo"
            else:
                kind = "text"

            photo_file_id = msg.photo.file_id if msg.photo else None

            explanation_lookup[reply_id] = {
                "explanation_msg_id": msg.id,
                "text": text,
                "has_photo": has_photo or has_video,  # True if ANY visual media (photo, video, doc)
                "has_video": has_video,
                "photo_file_id": photo_file_id,
                "kind": kind,
                "has_document": has_document,
            }
            new_expl_count += 1

        LAST_SCANNED_MSG_ID[ch] = max_msg_id
        _save_state()

        print(f"[EXPLANATION-SCAN] Channel {ch}: {len(new_msgs)} new msgs "
              f"(since {last_id}), {new_poll_count} polls, {new_expl_count} explanations")

    except Exception as e:
        print(f"[EXPLANATION-SCAN] Error scanning channel {ch}: {e}")


# ═══════════════════════════════════════════════════════════════
# HELPERS — called from batch.py, fetch.py, auto.py
# ═══════════════════════════════════════════════════════════════

async def add_monitored_channel(channel_id, user_id=None, client=None):
    """Register a channel for explanation monitoring.

    Called when a user runs /fetch, /batch, or /auto on a channel.
    Loads known poll IDs and triggers incremental scan to catch up
    on any messages posted since the last scan.

    Args:
        channel_id: Channel ID (int or str)
        user_id: User who triggered the monitoring
        client: Pyrogram client for scanning (falls back to global userbot)
    """
    original_ch = str(channel_id)
    ch = await _resolve_channel_key(channel_id, client)
    _merge_channel_state(original_ch, ch)
    is_new = ch not in MONITORED_CHANNELS

    if not is_new:
        # Already monitored — but maybe new polls were added to fetch_map
        await _load_polls_for_channel(original_ch, store_channel_id=ch)
        # Still do an incremental scan to catch new messages
        scan_client = client or get_userbot()
        if scan_client and scan_client.is_connected:
            await _incremental_scan_channel(scan_client, ch)
        return

    MONITORED_CHANNELS.add(ch)

    # Load known polls for this channel from fetch_maps
    await _load_polls_for_channel(original_ch, store_channel_id=ch)

    # Ensure dicts exist
    if ch not in KNOWN_POLLS:
        KNOWN_POLLS[ch] = set()
    if ch not in CHANNEL_EXPLANATIONS:
        CHANNEL_EXPLANATIONS[ch] = {}

    # Incremental scan to catch up on messages since last scan
    scan_client = client or get_userbot()
    if scan_client and scan_client.is_connected:
        await _incremental_scan_channel(scan_client, ch)

    # Persist to MongoDB (for cross-instance awareness)
    await monitored_channels_collection.update_one(
        {"channel_id": ch},
        {"$set": {
            "channel_id": ch,
            "source_channel_id": original_ch,
            "user_id": user_id,
            "active": True,
            "updated_at": datetime.now()
        }},
        upsert=True
    )

    poll_count = len(KNOWN_POLLS.get(ch, set()))
    expl_count = len(CHANNEL_EXPLANATIONS.get(ch, {}))
    print(f"[EXPLANATION] Channel {ch} now monitored ({poll_count} polls, {expl_count} explanations)")


def get_userbot():
    """Get the global userbot dynamically."""
    try:
        import shared_client
        return shared_client.userbot
    except Exception:
        return None


async def _load_polls_for_channel(channel_id, store_channel_id=None):
    """Load known poll IDs from fetch_maps for a channel.

    The fetch_map already knows which messages are polls
    (media_type == "poll"). We use this as our "known polls" set
    so the listener can quickly check if a reply is to a poll.
    """
    lookup_ch = str(channel_id)
    ch = str(store_channel_id or channel_id)
    existing = KNOWN_POLLS.get(ch, set())

    try:
        from plugins.fetch import fetch_maps_collection
        lookup_ids = list(dict.fromkeys([lookup_ch, ch]))
        maps = await fetch_maps_collection.find(
            {"channel_id": {"$in": lookup_ids}}
        ).to_list(length=None)

        new_poll_ids = set()
        for fm in maps:
            msg_map = fm.get("msg_map", {})
            for mid_str, info in msg_map.items():
                if info.get("media_type") == "poll":
                    new_poll_ids.add(int(mid_str))

        # Merge with existing (don't lose polls added by live listener)
        if ch not in KNOWN_POLLS:
            KNOWN_POLLS[ch] = set()
        KNOWN_POLLS[ch].update(new_poll_ids)

        added = len(new_poll_ids) - len(existing & new_poll_ids)
        if added > 0:
            print(f"[EXPLANATION] Loaded {added} new poll IDs for channel {ch} (total: {len(KNOWN_POLLS[ch])})")
    except Exception as e:
        print(f"[EXPLANATION] Error loading polls for channel {ch}: {e}")
        if ch not in KNOWN_POLLS:
            KNOWN_POLLS[ch] = set()


async def remove_monitored_channel(channel_id):
    """Unregister a channel from explanation monitoring."""
    ch = str(channel_id)
    MONITORED_CHANNELS.discard(ch)
    KNOWN_POLLS.pop(ch, None)
    CHANNEL_EXPLANATIONS.pop(ch, None)
    LAST_SCANNED_MSG_ID.pop(ch, None)
    _save_state()

    await monitored_channels_collection.update_one(
        {"channel_id": ch},
        {"$set": {"active": False, "updated_at": datetime.now()}}
    )


async def get_explanation(channel_id, poll_msg_id):
    """Look up stored explanation for a poll. Returns dict or None.

    FIRST checks the in-memory CHANNEL_EXPLANATIONS (instant, 0 API calls).
    Falls back to MongoDB if not found in memory.
    """
    ch = str(channel_id)
    # Fast path: in-memory lookup
    entry = CHANNEL_EXPLANATIONS.get(ch, {}).get(int(poll_msg_id))
    if entry:
        return entry

    # Slow path: MongoDB
    return await poll_explanations_collection.find_one(
        {"channel_id": str(channel_id), "poll_msg_id": int(poll_msg_id)},
        {"_id": 0}
    )


async def check_poll_builtin_explanation(client, channel_id, poll_msg_id):
    """Check if the poll message has a built-in explanation via Telegram's PollResults.solution.

    Telegram quiz polls can have an explanation embedded directly in the poll
    via the `solution` field in PollResults. This is a last-resort fallback when
    neither the in-memory lookup nor the live scan found an explanation.

    Args:
        client: Pyrogram client with access to the channel
        channel_id: Source channel ID
        poll_msg_id: Message ID of the poll in the source channel

    Returns:
        str: The solution text if found, or None
    """
    try:
        import pyrogram.raw as raw

        # Resolve the peer for raw API call
        try:
            from plugins.batch import resolve_chat
            resolved = await resolve_chat(client, str(channel_id))
        except Exception:
            resolved = channel_id

        peer = await client.resolve_peer(resolved)

        result = await client.invoke(
            raw.functions.messages.GetPollResults(
                peer=peer,
                msg_id=poll_msg_id
            )
        )

        # Check for solution in the results
        solution = getattr(result, 'solution', None)
        if solution:
            print(f"[EXPLANATION-BUILTIN] Found built-in solution for poll {poll_msg_id}: {solution[:80]}...")
            return solution

        return None

    except Exception as e:
        print(f"[EXPLANATION-BUILTIN] Error checking built-in solution for poll {poll_msg_id}: {e}")
        return None


async def find_explanation_batch(client, channel_id, poll_msg_id,
                             scan_window=1500, batch_size=200, max_parallel=4):
    """Parallel batch ID fetch for messages that reply to poll_msg_id.

    Fetches specific message IDs [poll_msg_id+1 .. poll_msg_id+scan_window]
    using get_messages() in parallel batches, NOT get_chat_history backwards.

    Why this is better than get_chat_history:
      - poll_msg_id=25 in a chat with 8608 messages:
        OLD: get_chat_history scans 8583 messages backwards (slow, rate-limit heavy)
        NEW: get_messages([26..1525]) = 1500 IDs / 200 per batch = 8 batches
             4 parallel at a time = 2 rounds = ~0.6 seconds total

    On success, stores the result in CHANNEL_EXPLANATIONS + JSON for
    future instant lookups.

    Args:
        client: Pyrogram client with access to the channel
        channel_id: Source channel ID (int or str)
        poll_msg_id: Message ID of the poll in the source channel
        scan_window: How many msg IDs after poll to check (default 1500)
        batch_size: Telegram allows up to 200 per get_messages call
        max_parallel: Max simultaneous API calls (default 4)

    Returns:
        dict with keys: text, photo_file_id, explanation_msg_id, captured_by
        or None if not found
    """
    try:
        ch = str(channel_id)
        resolved = None
        try:
            from plugins.batch import resolve_chat
            resolved = await resolve_chat(client, ch)
        except Exception:
            resolved = ch  # Try raw ID

        start = poll_msg_id + 1
        end = poll_msg_id + scan_window + 1

        # Build all batch ID lists upfront
        all_batches = []
        for batch_start in range(start, end, batch_size):
            batch_ids = list(range(batch_start, min(batch_start + batch_size, end)))
            all_batches.append(batch_ids)

        total_ids = sum(len(b) for b in all_batches)
        num_rounds = -(-len(all_batches) // max_parallel)  # ceil division

        print(f"[EXPLANATION-BATCH] Parallel batch fetch for poll={poll_msg_id} "
              f"scanning [{start}..{end - 1}] "
              f"({len(all_batches)} batches, {total_ids} IDs, "
              f"{num_rounds} rounds of {max_parallel})")

        # ── Fetch one batch, return list of messages ──────────────
        async def _fetch_batch(batch_ids):
            try:
                msgs = await client.get_messages(resolved, batch_ids)
                # get_messages can return a single Message or a list
                if not isinstance(msgs, list):
                    msgs = [msgs]
                return msgs
            except Exception as e:
                print(f"[EXPLANATION-BATCH] Batch [{batch_ids[0]}..{batch_ids[-1]}] failed: {e}")
                return []

        # ── Fetch in parallel groups ──────────────────────────────
        all_messages = []

        for group_start in range(0, len(all_batches), max_parallel):
            group = all_batches[group_start : group_start + max_parallel]
            round_num = group_start // max_parallel + 1

            print(f"[EXPLANATION-BATCH] Round {round_num}/{num_rounds}: "
                  f"firing {len(group)} parallel calls "
                  f"[{group[0][0]}..{group[-1][-1]}]")

            # Fire all in group simultaneously
            results = await asyncio.gather(*[_fetch_batch(b) for b in group])

            for batch_result in results:
                all_messages.extend(batch_result)

        # ── Scan results for explanation ──────────────────────────
        for msg in all_messages:
            # get_messages returns None for non-existent IDs
            if not msg or getattr(msg, 'empty', True):
                continue

            # Check if this message replies to our poll
            reply_id = _get_reply_to_id(msg)

            # Direct match: this message replies directly to the poll
            direct_match = (reply_id == poll_msg_id)

            # Top-ID fallback: check reply_to_top_id
            top_id_match = False
            if not direct_match:
                reply_to_obj = getattr(msg, 'reply_to', None)
                if reply_to_obj:
                    top_id = getattr(reply_to_obj, 'reply_to_top_id', None)
                    if top_id == poll_msg_id:
                        top_id_match = True

            if direct_match or top_id_match:
                text = msg.text or msg.caption or None
                photo_file_id = msg.photo.file_id if msg.photo else None
                has_document = msg.document is not None
                match_type = "direct" if direct_match else "top_id_fallback"

                if text or photo_file_id or has_document:
                    print(f"[EXPLANATION-BATCH] Found explanation for poll {poll_msg_id}: "
                          f"msg_id={msg.id} match={match_type} "
                          f"text={'YES' if text else 'NO'} photo={'YES' if photo_file_id else 'NO'} "
                          f"doc={'YES' if has_document else 'NO'} "
                          f"gap={msg.id - poll_msg_id} "
                          f"(scanned {len(all_messages)} messages across {len(all_batches)} batches)")

                    # Store it for future instant lookups
                    await store_explanation(ch, poll_msg_id, msg.id,
                                           text=text, photo_file_id=photo_file_id,
                                           captured_by="batch_fetch",
                                           has_document=has_document)

                    return {
                        "text": text,
                        "photo_file_id": photo_file_id,
                        "explanation_msg_id": msg.id,
                        "has_document": has_document,
                        "captured_by": "batch_fetch"
                    }

        print(f"[EXPLANATION-BATCH] No explanation found for poll {poll_msg_id} "
              f"(scanned {len(all_messages)} messages across {len(all_batches)} batches "
              f"in [{start}..{end - 1}])")
        return None

    except Exception as e:
        print(f"[EXPLANATION-BATCH] Error batch-fetching for poll {poll_msg_id}: {e}")
        return None


async def find_explanation_sequential(client, channel_id, poll_msg_id,
                                       scan_window=100, batch_size=200):
    """Sequential (NOT parallel) batch ID fetch for messages that reply to poll_msg_id.

    This is the FloodWait-safe replacement for find_explanation_batch().
    Instead of firing 4 parallel API calls simultaneously (which triggers
    FloodWait), it fetches one batch at a time sequentially.

    The scan_window is intentionally small (100 by default) because
    explanations in Telegram channels typically appear within 5-20 messages
    after the poll. Scanning 100 messages is more than enough and costs
    only 1 API call (100 IDs < 200 per-call limit).

    If no explanation is found in the first window, a second pass expands
    to scan_window*3 (300 IDs) — still only 2 API calls total.

    On success, stores the result in CHANNEL_EXPLANATIONS + JSON for
    future instant lookups.

    Args:
        client: Pyrogram client with access to the channel
        channel_id: Source channel ID (int or str)
        poll_msg_id: Message ID of the poll in the source channel
        scan_window: How many msg IDs after poll to check (default 100)
        batch_size: Telegram allows up to 200 per get_messages call

    Returns:
        dict with keys: text, photo_file_id, explanation_msg_id, captured_by
        or None if not found
    """
    try:
        ch = str(channel_id)
        resolved = None
        try:
            from plugins.batch import resolve_chat
            resolved = await resolve_chat(client, ch)
        except Exception:
            resolved = ch  # Try raw ID

        # Pass 1: Scan first `scan_window` IDs after the poll
        for pass_num, window in enumerate([scan_window, scan_window * 3], 1):
            start = poll_msg_id + 1
            end = min(poll_msg_id + window + 1, poll_msg_id + scan_window * 3 + 1)

            # For pass 2, skip IDs already scanned in pass 1
            if pass_num == 2:
                start = poll_msg_id + scan_window + 1

            all_ids = list(range(start, end))

            # Fetch in batches of `batch_size`, ONE AT A TIME (sequential)
            for batch_start in range(0, len(all_ids), batch_size):
                batch_ids = all_ids[batch_start:batch_start + batch_size]
                if not batch_ids:
                    continue

                try:
                    msgs = await client.get_messages(resolved, batch_ids)
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                except FloodWait as e:
                    wait_time = e.value if hasattr(e, 'value') else 30
                    print(f"[EXPLANATION-SEQ] FloodWait {wait_time}s — sleeping then retrying...")
                    await asyncio.sleep(wait_time + 2)
                    # Retry once
                    try:
                        msgs = await client.get_messages(resolved, batch_ids)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                    except Exception:
                        continue
                except Exception as e:
                    print(f"[EXPLANATION-SEQ] Batch [{batch_ids[0]}..{batch_ids[-1]}] failed: {e}")
                    continue

                # Scan messages for explanation
                for msg in msgs:
                    if not msg or getattr(msg, 'empty', True):
                        continue

                    reply_id = _get_reply_to_id(msg)

                    # Direct match: this message replies directly to the poll
                    direct_match = (reply_id == poll_msg_id)

                    # Top-ID fallback: check reply_to_top_id
                    top_id_match = False
                    if not direct_match:
                        reply_to_obj = getattr(msg, 'reply_to', None)
                        if reply_to_obj:
                            top_id = getattr(reply_to_obj, 'reply_to_top_id', None)
                            if top_id == poll_msg_id:
                                top_id_match = True

                    if direct_match or top_id_match:
                        text = msg.text or msg.caption or None
                        photo_file_id = msg.photo.file_id if msg.photo else None
                        has_document = msg.document is not None
                        match_type = "direct" if direct_match else "top_id_fallback"

                        if text or photo_file_id or has_document:
                            print(f"[EXPLANATION-SEQ] Found explanation for poll {poll_msg_id}: "
                                  f"msg_id={msg.id} match={match_type} "
                                  f"text={'YES' if text else 'NO'} photo={'YES' if photo_file_id else 'NO'} "
                                  f"doc={'YES' if has_document else 'NO'} gap={msg.id - poll_msg_id}")

                            # Store it for future instant lookups
                            await store_explanation(ch, poll_msg_id, msg.id,
                                                   text=text, photo_file_id=photo_file_id,
                                                   captured_by="sequential_scan",
                                                   has_document=has_document)

                            return {
                                "text": text,
                                "photo_file_id": photo_file_id,
                                "explanation_msg_id": msg.id,
                                "has_document": has_document,
                                "captured_by": "sequential_scan"
                            }

            # Pass 1 done, no explanation found — continue to pass 2 (expanded window)
            if pass_num == 1:
                print(f"[EXPLANATION-SEQ] No explanation in first {scan_window} msgs for poll {poll_msg_id} — expanding to {scan_window * 3}...")

        print(f"[EXPLANATION-SEQ] No explanation found for poll {poll_msg_id} "
              f"(scanned up to {scan_window * 3} messages)")
        return None

    except Exception as e:
        print(f"[EXPLANATION-SEQ] Error scanning for poll {poll_msg_id}: {e}")
        return None


async def store_explanation(channel_id, poll_msg_id, explanation_msg_id,
                            text=None, photo_file_id=None, captured_by="bot",
                            has_document=False, has_video=False):
    """Store a poll -> explanation mapping in BOTH in-memory lookup AND MongoDB.

    The in-memory CHANNEL_EXPLANATIONS dict is the primary fast-lookup
    mechanism (0 API calls). MongoDB is the backup for cross-instance
    awareness and the /linkexplan /explans commands.

    Also persists to JSON so the state survives restarts.
    """
    ch = str(channel_id)
    has_photo = photo_file_id is not None or has_video

    # Determine kind based on content
    if has_video:
        kind = "video"
    elif has_document:
        kind = "document"
    elif has_photo and text:
        kind = "photo+text"
    elif has_photo:
        kind = "photo"
    else:
        kind = "text"

    # Update in-memory CHANNEL_EXPLANATIONS (primary fast lookup)
    if ch not in CHANNEL_EXPLANATIONS:
        CHANNEL_EXPLANATIONS[ch] = {}
    CHANNEL_EXPLANATIONS[ch][int(poll_msg_id)] = {
        "explanation_msg_id": int(explanation_msg_id),
        "text": text,
        "has_photo": has_photo,
        "has_video": has_video,
        "photo_file_id": photo_file_id,
        "kind": kind,
        "has_document": has_document,
    }

    # Persist to JSON (survives restart)
    _save_state()

    # Also store in MongoDB (backup + /linkexplan /explans commands)
    doc = {
        "channel_id": str(channel_id),
        "poll_msg_id": int(poll_msg_id),
        "explanation_msg_id": int(explanation_msg_id),
        "text": text,
        "photo_file_id": photo_file_id,
        "kind": kind,
        "has_document": has_document,
        "has_video": has_video,
        "captured_by": captured_by,
        "updated_at": datetime.now()
    }
    await poll_explanations_collection.update_one(
        {"channel_id": str(channel_id), "poll_msg_id": int(poll_msg_id)},
        {"$set": doc},
        upsert=True
    )
    text_preview = (text[:50] + "...") if text and len(text) > 50 else (text or "no text")
    has_photo_str = "📸" if photo_file_id else ""
    has_doc_str = "📄" if has_document else ""
    print(f"[EXPLANATION] Stored: ch={channel_id} poll={poll_msg_id} -> "
          f"expl={explanation_msg_id} [{captured_by}] {has_photo_str}{has_doc_str} kind={kind} \"{text_preview}\"")


async def add_known_poll(channel_id, poll_msg_id):
    """Add a poll ID to the known polls set for a channel.

    Called by the live listener when a new poll is detected,
    or from batch.py when polls are found during /fetch.
    Also persists state to JSON.
    """
    ch = str(channel_id)
    if ch not in KNOWN_POLLS:
        KNOWN_POLLS[ch] = set()
    KNOWN_POLLS[ch].add(int(poll_msg_id))
    _save_state()


# ═══════════════════════════════════════════════════════════════
# REAL-TIME LISTENER — on main bot X
# ═══════════════════════════════════════════════════════════════

@X.on_message(filters.channel & filters.reply, group=5)
async def _on_reply_in_channel(client, message):
    """Listen for replies in monitored channels — captures explanations.

    When a reply to a known poll is detected:
      1. Stores the explanation in CHANNEL_EXPLANATIONS + JSON + MongoDB
      2. Checks POLL_MAP for this poll's dest mapping
      3. If found: sends explanation to dest → builds t.me/c/ link → appends 📖 button to poll
    """
    chat_id = str(message.chat.id)

    # Quick check — is this channel monitored?
    if chat_id not in MONITORED_CHANNELS:
        return

    # Get the replied-to message ID
    replied_id = _get_reply_to_id(message)
    if replied_id is None:
        return

    # Is the replied-to message a known poll or a pending mapped poll?
    matched_poll_id = None

    if _is_known_or_pending_poll(chat_id, replied_id):
        matched_poll_id = replied_id
    else:
        # Top-ID fallback
        reply_to_obj = getattr(message, 'reply_to', None)
        if reply_to_obj:
            top_id = getattr(reply_to_obj, 'reply_to_top_id', None)
            if top_id and _is_known_or_pending_poll(chat_id, top_id):
                matched_poll_id = top_id

    if matched_poll_id is None:
        return  # Not a reply to a known poll — skip

    # Store the explanation (updates CHANNEL_EXPLANATIONS + JSON + MongoDB)
    text = message.text or message.caption or None
    photo_file_id = message.photo.file_id if message.photo else None
    has_document = message.document is not None

    await store_explanation(chat_id, matched_poll_id, message.id,
                           text=text, photo_file_id=photo_file_id,
                           captured_by="bot", has_document=has_document)

    # ── 📖 View Explanation button DISABLED — only store explanation, no copy/button ──


@X.on_message(filters.channel & filters.poll, group=5)
async def _on_new_poll_in_channel(client, message):
    """Listen for new polls in monitored channels — adds to KNOWN_POLLS + saves."""
    chat_id = str(message.chat.id)

    if chat_id not in MONITORED_CHANNELS:
        return

    await add_known_poll(chat_id, message.id)
    print(f"[EXPLANATION] New poll detected: ch={chat_id} msg_id={message.id}")


# ═══════════════════════════════════════════════════════════════
# USERBOT LISTENER — registered dynamically after userbot is ready
# ═══════════════════════════════════════════════════════════════

_userbot_handler_registered = False


async def _on_reply_userbot(client, message):
    """Same as _on_reply_in_channel but on the userbot.
    
    Uses the bot client for sending to dest channel (bot is member of dest).
    Uses the userbot for downloading from source channel (userbot has access).
    """
    chat_id = str(message.chat.id)
    if chat_id not in MONITORED_CHANNELS:
        return

    replied_id = _get_reply_to_id(message)
    if replied_id is None:
        return

    matched_poll_id = None

    if _is_known_or_pending_poll(chat_id, replied_id):
        matched_poll_id = replied_id
    else:
        reply_to_obj = getattr(message, 'reply_to', None)
        if reply_to_obj:
            top_id = getattr(reply_to_obj, 'reply_to_top_id', None)
            if top_id and _is_known_or_pending_poll(chat_id, top_id):
                matched_poll_id = top_id

    if matched_poll_id is None:
        return

    text = message.text or message.caption or None
    photo_file_id = message.photo.file_id if message.photo else None
    has_document = message.document is not None

    await store_explanation(chat_id, matched_poll_id, message.id,
                           text=text, photo_file_id=photo_file_id,
                           captured_by="userbot", has_document=has_document)

    # ── 📖 View Explanation button DISABLED — only store explanation, no copy/button ──



async def _on_new_poll_userbot(client, message):
    """Same as _on_new_poll_in_channel but on the userbot."""
    chat_id = str(message.chat.id)
    if chat_id not in MONITORED_CHANNELS:
        return

    await add_known_poll(chat_id, message.id)
    print(f"[EXPLANATION] New poll detected (userbot): ch={chat_id} msg_id={message.id}")


async def register_userbot_listener():
    """Register explanation listener on the global userbot."""
    global _userbot_handler_registered
    if _userbot_handler_registered:
        return

    try:
        import shared_client
        userbot = shared_client.userbot
        if not userbot:
            print("[EXPLANATION] No global userbot available — skipping userbot listener")
            return

        if not userbot.is_connected:
            print("[EXPLANATION] Userbot not connected yet — will retry later")
            return

        from pyrogram.handlers import MessageHandler

        userbot.add_handler(
            MessageHandler(_on_reply_userbot, filters.channel & filters.reply),
            group=5
        )
        userbot.add_handler(
            MessageHandler(_on_new_poll_userbot, filters.channel & filters.poll),
            group=5
        )

        _userbot_handler_registered = True
        print("[EXPLANATION] Registered userbot listener for explanations")
    except Exception as e:
        print(f"[EXPLANATION] Failed to register userbot listener: {e}")


# ═══════════════════════════════════════════════════════════════
# /LINKEXPLAN — Manual explanation linking for past polls
# ═══════════════════════════════════════════════════════════════

LINKEXPLAN_STATE = {}  # uid -> {'step': 'channel'|'poll'|'explan', ...}


@X.on_message(filters.command("linkexplan"))
async def linkexplan_cmd(c, m):
    """Manually link a poll to its explanation message."""
    uid = m.from_user.id
    from config import OWNER_ID
    from utils.func import is_auth_user
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return

    LINKEXPLAN_STATE[uid] = {'step': 'channel'}
    await safe_reply(m,
        "**🔗 /linkexplan — Manual Explanation Link**\n\n"
        "Links a poll message to its explanation (the reply).\n\n"
        "**Step 1:** Send any message link from the source channel."
    )


# ═══════════════════════════════════════════════════════════════
# /EXPLANS — View stored explanations for a channel
# ═══════════════════════════════════════════════════════════════

@X.on_message(filters.command("explans"))
async def explans_cmd(c, m):
    """View stored explanations for a channel."""
    uid = m.from_user.id
    from config import OWNER_ID
    from utils.func import is_auth_user
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return

    # Show both in-memory and MongoDB counts
    mem_total = sum(len(e) for e in CHANNEL_EXPLANATIONS.values())
    db_count = await poll_explanations_collection.count_documents({})
    channels = await poll_explanations_collection.distinct("channel_id")

    if mem_total == 0 and db_count == 0:
        await safe_reply(m,
            "📋 No explanations stored yet.\n\n"
            "Explanations are captured automatically when:\n"
            "• The bot/userbot is a member of the source channel\n"
            "• Someone replies to a poll in that channel\n\n"
            "Use /linkexplan to manually link past explanations."
        )
        return

    lines = [f"📋 **Stored Explanations:** {mem_total} in memory, {db_count} in DB\n"]
    for ch in channels:
        mem_ch = len(CHANNEL_EXPLANATIONS.get(ch, {}))
        db_ch = await poll_explanations_collection.count_documents({"channel_id": ch})
        lines.append(f"  Channel `{ch}`: {mem_ch} memory / {db_ch} DB")

    lines.append(f"\n💡 Use /linkexplan to add more manually")
    await safe_reply(m, "\n".join(lines))


# ═══════════════════════════════════════════════════════════════
# TEXT HANDLER for /linkexplan conversation
# ═══════════════════════════════════════════════════════════════

@X.on_message(
    filters.text & filters.private & ~filters.command([
        'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id',
        'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt',
        'decrypt', 'keys', 'setbot', 'rembot', 'auth', 'unauth', 'authusers',
        'logs', 'fetch', 'cancelfetch', 'fetchmaps', 'clearfetch', 'answerkey',
        'clearanswerkey', 'viewanswerkey', 'viewfetchmaps', 'auto', 'autooff',
        'cancelauto', 'settings', 'help', 'terms', 'plan', 'status', 'clearbatch',
        'linkexplan', 'explans', 'transfer', 'rem', 'dl', 'adl',
        'mirror', 'mirrorstop', 'mirrorstatus', 'explanlogs'
    ]),
    group=3
)
async def linkexplan_text_handler(c, m):
    """Handle text input during /linkexplan conversation."""
    uid = m.from_user.id

    if uid not in LINKEXPLAN_STATE:
        from pyrogram import ContinuePropagation
        raise ContinuePropagation

    state = LINKEXPLAN_STATE[uid]

    if state['step'] == 'channel':
        from utils.func import E
        L = m.text.strip()
        channel_id, _, lt = E(L)
        if not channel_id:
            await safe_reply(m, "Invalid link. Send a valid Telegram message link from the source channel.")
            return

        state['channel_id'] = channel_id
        state['link_type'] = lt
        state['step'] = 'poll'
        await safe_reply(m,
            "✅ Channel registered.\n\n"
            "**Step 2:** Send the **poll message link** (the quiz/poll question)."
        )

    elif state['step'] == 'poll':
        from utils.func import E
        L = m.text.strip()
        channel_id, msg_id, lt = E(L)
        if not channel_id or not msg_id:
            await safe_reply(m, "Invalid link. Send a valid Telegram message link for the poll.")
            return

        state['poll_msg_id'] = msg_id
        state['step'] = 'explan'
        await safe_reply(m,
            f"✅ Poll message ID: {msg_id}\n\n"
            "**Step 3:** Send the **explanation message link** "
            "(the message that replies to the poll with the answer/explanation)."
        )

    elif state['step'] == 'explan':
        from utils.func import E
        L = m.text.strip()
        channel_id, msg_id, lt = E(L)
        if not channel_id or not msg_id:
            await safe_reply(m, "Invalid link. Send a valid Telegram message link for the explanation.")
            del LINKEXPLAN_STATE[uid]
            return

        ch = state['channel_id']
        poll_msg_id = state['poll_msg_id']
        explanation_msg_id = msg_id

        # Try to fetch the explanation message to get its content
        text = None
        photo_file_id = None
        has_document = False

        from plugins.batch import get_Y, resolve_chat

        uc = get_Y()
        if uc:
            try:
                resolved = await resolve_chat(uc, ch)
                expl_msg = await uc.get_messages(resolved, explanation_msg_id)
                if expl_msg:
                    text = expl_msg.text or expl_msg.caption or None
                    photo_file_id = expl_msg.photo.file_id if expl_msg.photo else None
                    has_document = expl_msg.document is not None
            except Exception as e:
                print(f"[LINKEXPLAN] Error fetching explanation: {e}")

        # Store the explanation (updates CHANNEL_EXPLANATIONS + JSON + MongoDB)
        await store_explanation(ch, poll_msg_id, explanation_msg_id,
                               text=text, photo_file_id=photo_file_id,
                               captured_by="manual",
                               has_document=has_document)

        # Also add the poll to known polls
        await add_known_poll(ch, poll_msg_id)

        # Make sure this channel is monitored
        await add_monitored_channel(ch, uid)

        has_text = "✅" if text else "❌"
        has_photo = "✅" if photo_file_id else "❌"

        del LINKEXPLAN_STATE[uid]
        await safe_reply(m,
            f"✅ **Explanation linked!**\n\n"
            f"Channel: `{ch}`\n"
            f"Poll message: `{poll_msg_id}`\n"
            f"Explanation message: `{explanation_msg_id}`\n\n"
            f"Has text: {has_text}\n"
            f"Has photo: {has_photo}\n\n"
            f"This explanation is stored in the cache for the 💡 View Answer Telegraph page."
        )


# ═══════════════════════════════════════════════════════════════
# STARTUP — load JSON, incremental scan, register watcher
# ═══════════════════════════════════════════════════════════════

async def _startup():
    """Startup sequence:
    1. Load state from JSON (instant)
    2. Load monitored channels from MongoDB (for channels not in JSON)
    3. Incremental scan for each channel (fast — only new messages)
    4. Register userbot listener
    """
    try:
        # Step 1: Load state from JSON (instant)
        loaded = _load_state()

        # Step 2: Load monitored channels from MongoDB (supplements JSON)
        channels = await monitored_channels_collection.find(
            {"active": True}
        ).to_list(length=None)

        ub = get_userbot()
        for ch in channels:
            raw_ch_id = ch["channel_id"]
            ch_id = await _resolve_channel_key(raw_ch_id, ub)
            _merge_channel_state(raw_ch_id, ch_id)
            if ch_id not in MONITORED_CHANNELS:
                MONITORED_CHANNELS.add(ch_id)
                await _load_polls_for_channel(raw_ch_id, store_channel_id=ch_id)

        # Step 3: Incremental scan for each monitored channel
        if ub and ub.is_connected:
            for ch_id in list(MONITORED_CHANNELS):
                await _incremental_scan_channel(ub, ch_id)
        else:
            print("[EXPLANATION] Userbot not connected yet — skipping incremental scan")

        total_polls = sum(len(p) for p in KNOWN_POLLS.values())
        total_expl = sum(len(e) for e in CHANNEL_EXPLANATIONS.values())
        print(f"[EXPLANATION] Startup: {len(MONITORED_CHANNELS)} channels, "
              f"{total_polls} polls, {total_expl} explanations")

        # Step 4: Register userbot listener (with retry since userbot may start late)
        for attempt in range(5):
            await asyncio.sleep(5 * (attempt + 1))
            await register_userbot_listener()
            if _userbot_handler_registered:
                break
            print(f"[EXPLANATION] Userbot not ready yet, retry {attempt + 1}/5...")

    except Exception as e:
        print(f"[EXPLANATION] Startup error: {e}")


# Run startup after a short delay (let other modules initialize first)
asyncio.ensure_future(_startup())
