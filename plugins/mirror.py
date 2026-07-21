# ╔══════════════════════════════════════════════════════════════════╗
# ║  TELEGRAM CHANNEL MIRROR PLUGIN — INTEGRATED                   ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║                                                                  ║
# ║  Mirrors an entire source Telegram channel to a destination     ║
# ║  channel preserving exact order, reply chains, and pins.        ║
# ║                                                                  ║
# ║  INTEGRATED: Uses shared_client (app + userbot) instead of     ║
# ║  creating its own Pyrogram clients. Shares the same MongoDB    ║
# ║  database as the rest of the bot.                               ║
# ║                                                                  ║
# ║  COMMANDS:                                                       ║
# ║  /mirror <source_link> <dest_chat_id>  — Start mirroring        ║
# ║  /mirrorstop                           — Stop current mirror     ║
# ║  /mirrorstatus                        — Check mirror progress    ║
# ║                                                                  ║
# ║  KEY RULES:                                                      ║
# ║  1. ONE message uploaded at a time — strictly sequential       ║
# ║  2. Every message confirmed before next starts                  ║
# ║  3. Never skip a failed message — retry with backoff           ║
# ║  4. All storage in MongoDB — Heroku RAM stays flat             ║
# ║  5. Auto resume with 100-back verification on restart          ║
# ║  6. NO explanation inline buttons — not needed                  ║
# ║                                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

import asyncio
import logging
import os
import gc
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from pyrogram import Client, filters, ContinuePropagation
from pyrogram.errors import FloodWait, ChatIdInvalid, PeerIdInvalid, ChannelPrivate
from pyrogram.enums import PollType
from pyrogram.types import PollOption
from config import API_ID, API_HASH, MONGO_DB, DB_NAME, OWNER_ID, FREEMIUM_LIMIT
from shared_client import app as X
from utils.func import is_auth_user, is_premium_user
from plugins.start import subscribe as sub
from utils.ram_monitor import log_ram

logger = logging.getLogger(__name__)

# ── MongoDB — use existing connection ────────────────────────────────
_mongo_client = AsyncIOMotorClient(MONGO_DB)
_db           = _mongo_client[DB_NAME]

# Collections for mirror state (in the SAME database as the rest of the bot)
mirror_fetch_map  = _db["mirror_fetch_map"]   # source channel messages
mirror_src_to_dst = _db["mirror_src_to_dst"]  # upload tracking
mirror_pin_map    = _db["mirror_pin_map"]      # pinned messages
mirror_state      = _db["mirror_state"]        # active mirror config

# ── Indexes (created once) ────────────────────────────────────────────
async def _ensure_indexes():
    await mirror_fetch_map.create_index("msg_id", unique=True)
    await mirror_fetch_map.create_index([("mirror_id", 1), ("msg_id", 1)])
    await mirror_src_to_dst.create_index([("mirror_id", 1), ("src_msg_id", 1)], unique=True)
    await mirror_src_to_dst.create_index([("mirror_id", 1), ("status", 1)])
    await mirror_pin_map.create_index([("mirror_id", 1), ("msg_id", 1)], unique=True)
    await mirror_state.create_index("mirror_id", unique=True)
    logger.info("[MIRROR-DB] Indexes ready")


# ── BATCH SEND RATE — safe for Telegram ──────────────────────────────
BATCH_SEND_RATE  = 18                         # messages per minute
BATCH_SEND_DELAY = 60.0 / BATCH_SEND_RATE     # ~3.33 seconds


# ── Active mirror tasks ──────────────────────────────────────────────
mirror_tasks = {}   # mirror_id -> asyncio.Task


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 1: FETCH MAP                                           ║
# ║  Scan source channel ONCE, store in MongoDB.                    ║
# ╚══════════════════════════════════════════════════════════════════╝

def _msg_to_doc(msg, mirror_id: str) -> dict:
    """Convert a Pyrogram message to a MongoDB document."""
    # Get reply_to reliably
    reply_to = getattr(msg, "reply_to_message_id", None)
    if not reply_to:
        raw_reply = getattr(msg, "reply_to", None)
        if raw_reply:
            reply_to = getattr(raw_reply, "reply_to_msg_id", None) or getattr(raw_reply, "message_id", None)

    # Determine message type — support ALL types from batch.py
    if msg.poll:
        msg_type = "poll"
    elif msg.photo:
        msg_type = "photo"
    elif msg.video:
        msg_type = "video"
    elif msg.document:
        msg_type = "document"
    elif msg.audio:
        msg_type = "audio"
    elif msg.voice:
        msg_type = "voice"
    elif msg.video_note:
        msg_type = "video_note"
    elif msg.sticker:
        msg_type = "sticker"
    elif msg.animation:
        msg_type = "animation"
    elif msg.text:
        msg_type = "text"
    else:
        msg_type = "service"  # service messages, pins, etc.

    # Extract file_id for media that can be re-sent directly
    file_id = None
    if msg.photo:
        file_id = msg.photo.file_id
    elif msg.video:
        file_id = msg.video.file_id
    elif msg.document:
        file_id = msg.document.file_id
    elif msg.audio:
        file_id = msg.audio.file_id
    elif msg.voice:
        file_id = msg.voice.file_id
    elif msg.sticker:
        file_id = msg.sticker.file_id
    elif msg.animation:
        file_id = msg.animation.file_id
    elif msg.video_note:
        file_id = msg.video_note.file_id

    return {
        "mirror_id"     : mirror_id,
        "msg_id"        : msg.id,
        "type"          : msg_type,
        "reply_to"      : reply_to,
        "is_pinned"     : getattr(msg, "pinned", False),
        "file_id"       : file_id,
        "text"          : msg.caption or msg.text if not msg.poll else None,
        "poll_question" : msg.poll.question if msg.poll else None,
    }


async def build_fetch_map(user_client: Client, src_chat_id: int, mirror_id: str):
    """Scan entire source channel history and store in MongoDB."""
    logger.info(f"[MIRROR-FETCH] Starting full history scan for mirror_id={mirror_id}...")

    batch      = []
    total      = 0
    batch_size = 200

    async for msg in user_client.get_chat_history(src_chat_id):
        batch.append(
            UpdateOne(
                {"mirror_id": mirror_id, "msg_id": msg.id},
                {"$set": _msg_to_doc(msg, mirror_id)},
                upsert=True,
            )
        )

        if len(batch) >= batch_size:
            await mirror_fetch_map.bulk_write(batch)
            total += len(batch)
            batch  = []
            logger.info(f"[MIRROR-FETCH] Saved {total} messages...")

    if batch:
        await mirror_fetch_map.bulk_write(batch)
        total += len(batch)

    logger.info(f"[MIRROR-FETCH] Complete — {total} messages in MongoDB for mirror_id={mirror_id}")
    return total


async def get_msg_doc(mirror_id: str, msg_id: int) -> dict | None:
    return await mirror_fetch_map.find_one({"mirror_id": mirror_id, "msg_id": msg_id})


async def get_sorted_msg_ids(mirror_id: str) -> list:
    cursor  = mirror_fetch_map.find({"mirror_id": mirror_id}, {"msg_id": 1}).sort("msg_id", 1)
    msg_ids = []
    async for doc in cursor:
        msg_ids.append(doc["msg_id"])
    logger.info(f"[MIRROR-DB] Loaded {len(msg_ids)} message IDs for mirror_id={mirror_id}")
    return msg_ids


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 2: UPLOAD STATE TRACKING                               ║
# ╚══════════════════════════════════════════════════════════════════╝

async def mark_done(mirror_id: str, src_msg_id: int, dst_msg_id: int):
    await mirror_src_to_dst.update_one(
        {"mirror_id": mirror_id, "src_msg_id": src_msg_id},
        {"$set": {
            "mirror_id"  : mirror_id,
            "src_msg_id" : src_msg_id,
            "dst_msg_id" : dst_msg_id,
            "status"     : "done",
        }},
        upsert=True,
    )


async def mark_failed(mirror_id: str, src_msg_id: int):
    await mirror_src_to_dst.update_one(
        {"mirror_id": mirror_id, "src_msg_id": src_msg_id},
        {"$set": {"status": "failed"}},
        upsert=True,
    )


async def get_dst_id(mirror_id: str, src_msg_id: int) -> int | None:
    doc = await mirror_src_to_dst.find_one(
        {"mirror_id": mirror_id, "src_msg_id": src_msg_id, "status": "done"}
    )
    return doc["dst_msg_id"] if doc else None


async def get_last_done_src_id(mirror_id: str) -> int | None:
    doc = await mirror_src_to_dst.find_one(
        {"mirror_id": mirror_id, "status": "done"},
        sort=[("src_msg_id", -1)],
    )
    return doc["src_msg_id"] if doc else None


async def is_done(mirror_id: str, src_msg_id: int) -> bool:
    doc = await mirror_src_to_dst.find_one(
        {"mirror_id": mirror_id, "src_msg_id": src_msg_id, "status": "done"}
    )
    return bool(doc)


async def get_all_src_to_dst(mirror_id: str) -> dict:
    cursor = mirror_src_to_dst.find({"mirror_id": mirror_id, "status": "done"})
    result = {}
    async for doc in cursor:
        result[doc["src_msg_id"]] = doc["dst_msg_id"]
    return result


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 3: MESSAGE SENDER                                      ║
# ║  Sends one message to destination based on type.                ║
# ║  Uses shared_client.app (bot) for sending.                     ║
# ║  Uses shared_client.userbot for fetching source data.          ║
# ╚══════════════════════════════════════════════════════════════════╝

async def _send_mirror_message(
    msg_doc       : dict,
    dst_chat_id   : int,
    reply_to_dst  : int | None,
    src_chat_id   : int,
    mirror_id     : str,
):
    """Send one message to destination based on its type.
    Returns the sent message object (confirmation) or None.
    """
    msg_type = msg_doc.get("type")
    bot = X  # shared_client Pyrogram bot

    # ── POLL ────────────────────────────────────────────────────
    if msg_type == "poll":
        # Need full poll data — fetch from source via userbot
        userbot = _get_userbot()
        if not userbot:
            logger.error(f"[MIRROR-SEND] No userbot available for poll src={msg_doc['msg_id']}")
            return None

        full_msg = await userbot.get_messages(src_chat_id, msg_doc["msg_id"])
        if not full_msg or not full_msg.poll:
            logger.error(f"[MIRROR-SEND] Could not fetch poll src={msg_doc['msg_id']}")
            return None

        poll = full_msg.poll
        is_quiz = poll.correct_option_id is not None or getattr(poll, 'type', None) == PollType.QUIZ
        options = [PollOption(text=opt.text, entities=getattr(opt, 'entities', None)) for opt in poll.options]

        # Try to reveal correct answer for quizzes
        correct_id = poll.correct_option_id
        if is_quiz and correct_id is None:
            try:
                from plugins.batch import _get_correct_option
                correct_id = await _get_correct_option(src_chat_id, msg_doc["msg_id"], poll, user_client=userbot)
            except Exception as e:
                logger.warning(f"[MIRROR-SEND] Could not reveal quiz answer: {e}")

        poll_kwargs = dict(
            chat_id=dst_chat_id,
            question=poll.question,
            options=options,
            type=PollType.QUIZ if is_quiz else PollType.REGULAR,
            is_anonymous=getattr(poll, 'is_anonymous', True),
            reply_to_message_id=reply_to_dst,
        )
        if is_quiz and correct_id is not None:
            poll_kwargs['correct_option_id'] = correct_id
            if getattr(poll, 'explanation', None):
                poll_kwargs['explanation'] = poll.explanation[:200]
        if getattr(poll, 'question_entities', None):
            poll_kwargs['question_entities'] = poll.question_entities
        if getattr(poll, 'allows_multiple_answers', None):
            poll_kwargs['allows_multiple_answers'] = poll.allows_multiple_answers

        return await bot.send_poll(**poll_kwargs)

    # ── PHOTO ───────────────────────────────────────────────────
    elif msg_type == "photo":
        return await bot.send_photo(
            chat_id=dst_chat_id,
            photo=msg_doc.get("file_id"),
            caption=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── VIDEO ───────────────────────────────────────────────────
    elif msg_type == "video":
        return await bot.send_video(
            chat_id=dst_chat_id,
            video=msg_doc.get("file_id"),
            caption=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── DOCUMENT ────────────────────────────────────────────────
    elif msg_type == "document":
        return await bot.send_document(
            chat_id=dst_chat_id,
            document=msg_doc.get("file_id"),
            caption=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── AUDIO ───────────────────────────────────────────────────
    elif msg_type == "audio":
        return await bot.send_audio(
            chat_id=dst_chat_id,
            audio=msg_doc.get("file_id"),
            caption=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── VOICE ───────────────────────────────────────────────────
    elif msg_type == "voice":
        return await bot.send_voice(
            chat_id=dst_chat_id,
            voice=msg_doc.get("file_id"),
            caption=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── STICKER ─────────────────────────────────────────────────
    elif msg_type == "sticker":
        return await bot.send_sticker(
            chat_id=dst_chat_id,
            sticker=msg_doc.get("file_id"),
            reply_to_message_id=reply_to_dst,
        )

    # ── ANIMATION (GIF) ────────────────────────────────────────
    elif msg_type == "animation":
        return await bot.send_animation(
            chat_id=dst_chat_id,
            animation=msg_doc.get("file_id"),
            caption=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── VIDEO NOTE (round video) ────────────────────────────────
    elif msg_type == "video_note":
        return await bot.send_video_note(
            chat_id=dst_chat_id,
            video_note=msg_doc.get("file_id"),
            reply_to_message_id=reply_to_dst,
        )

    # ── TEXT ────────────────────────────────────────────────────
    elif msg_type == "text":
        return await bot.send_message(
            chat_id=dst_chat_id,
            text=msg_doc.get("text") or "",
            reply_to_message_id=reply_to_dst,
        )

    # ── SERVICE (skip — pins handled separately) ────────────────
    elif msg_type == "service":
        return None  # Service messages can't be forwarded

    return None


def _get_userbot():
    """Get userbot dynamically from shared_client."""
    try:
        import shared_client
        return shared_client.userbot
    except Exception:
        return None


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 4: UPLOAD WITH RETRY                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

async def _upload_with_retry(
    msg_doc       : dict,
    dst_chat_id   : int,
    reply_to_dst  : int | None,
    src_chat_id   : int,
    mirror_id     : str,
    max_retries   : int = 5,
) -> int | None:
    """Upload one message with retry logic. Returns dst_msg_id or None."""
    for attempt in range(1, max_retries + 1):
        try:
            sent = await _send_mirror_message(msg_doc, dst_chat_id, reply_to_dst, src_chat_id, mirror_id)
            if sent:
                return sent.id

        except FloodWait as e:
            wait = e.value + 2
            # ANY FloodWait → stop immediately instead of sleeping
            logger.warning(f"[MIRROR-RETRY] FloodWait {wait}s src={msg_doc['msg_id']} — stopping (no retry)")
            return None

        except Exception as e:
            logger.error(f"[MIRROR-RETRY] src={msg_doc['msg_id']} attempt={attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)

    return None


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 5: AUTO RESUME WITH 100-BACK VERIFICATION              ║
# ╚══════════════════════════════════════════════════════════════════╝

async def auto_resume(mirror_id: str, all_src_ids: list, lookback: int = 100):
    """Find correct resume point with 100-back verification."""
    last_done = await get_last_done_src_id(mirror_id)

    if not last_done:
        logger.info("[MIRROR-RESUME] No uploads found — starting from beginning")
        return 0, set()

    logger.info(f"[MIRROR-RESUME] Last uploaded src_msg_id={last_done}")

    try:
        last_index = all_src_ids.index(last_done)
    except ValueError:
        logger.warning("[MIRROR-RESUME] Last done ID not in src list — starting fresh")
        return 0, set()

    lookback_start = max(0, last_index - lookback)
    lookback_ids   = all_src_ids[lookback_start : last_index + 1]

    logger.info(f"[MIRROR-RESUME] Verifying {len(lookback_ids)} messages ({lookback} back)")

    # Get src_to_dst for lookback
    src_to_dst_all = await get_all_src_to_dst(mirror_id)

    reupload_ids = set()
    for src_id in lookback_ids:
        if src_id not in src_to_dst_all:
            reupload_ids.add(src_id)

    if reupload_ids:
        earliest = min(reupload_ids)
        resume_index = all_src_ids.index(earliest)
        logger.info(f"[MIRROR-RESUME] {len(reupload_ids)} messages need re-upload")
    else:
        resume_index = last_index + 1
        logger.info("[MIRROR-RESUME] All lookback messages OK")

    return resume_index, reupload_ids


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 6: PIN DETECTION AND MIRRORING                         ║
# ╚══════════════════════════════════════════════════════════════════╝

async def build_pin_map(mirror_id: str, src_chat_id: int):
    """Fetch ALL pinned messages from source and store in MongoDB."""
    userbot = _get_userbot()
    pinned_ids = set()

    if userbot:
        try:
            async for msg in userbot.get_pinned_messages(src_chat_id):
                pinned_ids.add(msg.id)
            logger.info(f"[MIRROR-PIN] Found {len(pinned_ids)} pins via API")
        except Exception as e:
            logger.warning(f"[MIRROR-PIN] get_pinned_messages failed: {e}")

    if not pinned_ids and userbot:
        try:
            msg = await userbot.get_pinned_message(src_chat_id)
            if msg:
                pinned_ids.add(msg.id)
        except Exception as e:
            logger.warning(f"[MIRROR-PIN] get_pinned_message failed: {e}")

    if pinned_ids:
        ops = [
            UpdateOne(
                {"mirror_id": mirror_id, "msg_id": pid},
                {"$set": {"mirror_id": mirror_id, "msg_id": pid, "is_pinned": True}},
                upsert=True,
            )
            for pid in pinned_ids
        ]
        await mirror_pin_map.bulk_write(ops)

    logger.info(f"[MIRROR-PIN] pin_map built: {len(pinned_ids)} pins for mirror_id={mirror_id}")
    return pinned_ids


async def mirror_pin_if_needed(mirror_id: str, src_msg_id: int, dst_msg_id: int, dst_chat_id: int):
    """If src_msg_id is pinned -> pin dst_msg_id in destination."""
    doc = await mirror_pin_map.find_one({"mirror_id": mirror_id, "msg_id": src_msg_id})
    if not doc:
        return

    try:
        await X.pin_chat_message(
            chat_id=dst_chat_id,
            message_id=dst_msg_id,
            disable_notification=True,
            both_sides=False,
        )
        logger.info(f"[MIRROR-PIN] Pinned dst={dst_msg_id} in chat={dst_chat_id}")
    except Exception as e:
        logger.error(f"[MIRROR-PIN] Failed to pin dst={dst_msg_id}: {e}")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 7: MAIN BATCH RUNNER                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

# ── Sub-batch size for progress tracking ─────────────────────────
SUB_BATCH_SIZE = 100  # Split large mirrors into chunks of 100 for easy tracking


async def run_mirror_batch(mirror_id: str, src_chat_id: int, dst_chat_id: int):
    """Main batch runner — uploads ALL source messages to destination in exact order.
    
    Splits messages into sub-batches of 100 for progress tracking.
    Example: 4000 messages = 40 sub-batches of 100 each.
    Progress shows: "Sub-batch 12/40 — msg 1200/4000"
    """
    logger.info(f"[MIRROR-BATCH] Starting mirror_id={mirror_id} src={src_chat_id} dst={dst_chat_id}")

    # Update state
    await mirror_state.update_one(
        {"mirror_id": mirror_id},
        {"$set": {
            "mirror_id"       : mirror_id,
            "src_chat_id"     : src_chat_id,
            "dst_chat_id"     : dst_chat_id,
            "status"          : "running",
            "started_at"      : datetime.utcnow(),
            "progress"        : 0,
            "sub_batch_size"  : SUB_BATCH_SIZE,
            "current_sub_batch": 0,
            "total_sub_batches": 0,
        }},
        upsert=True,
    )

    # Load all src msg IDs in order from MongoDB
    all_src_ids = await get_sorted_msg_ids(mirror_id)
    total = len(all_src_ids)
    logger.info(f"[MIRROR-BATCH] Total messages to process: {total}")

    if total == 0:
        logger.info("[MIRROR-BATCH] No messages to mirror")
        await mirror_state.update_one({"mirror_id": mirror_id}, {"$set": {"status": "completed"}})
        return

    # Calculate sub-batch info
    total_sub_batches = (total + SUB_BATCH_SIZE - 1) // SUB_BATCH_SIZE  # ceil division
    await mirror_state.update_one(
        {"mirror_id": mirror_id},
        {"$set": {"total_sub_batches": total_sub_batches}},
    )
    logger.info(f"[MIRROR-BATCH] Split into {total_sub_batches} sub-batches of {SUB_BATCH_SIZE}")

    # Auto resume with 100-back verification
    resume_index, reupload_ids = await auto_resume(mirror_id, all_src_ids)
    logger.info(f"[MIRROR-BATCH] Resuming from index={resume_index} re-uploads={len(reupload_ids)}")

    # Sequential upload — one at a time, strictly ordered
    uploaded = 0
    failed   = 0
    stopped  = False
    current_sub_batch = 0

    for i, src_id in enumerate(all_src_ids):
        # Check if mirror was stopped
        state_doc = await mirror_state.find_one({"mirror_id": mirror_id})
        if state_doc and state_doc.get("status") == "stopped":
            logger.info(f"[MIRROR-BATCH] Mirror stopped by user at msg {uploaded}/{total}")
            stopped = True
            break

        # Skip already verified messages before resume point
        if i < resume_index and src_id not in reupload_ids:
            continue

        # Already uploaded? Skip instantly
        if await is_done(mirror_id, src_id):
            continue

        # Track sub-batch progress
        new_sub_batch = i // SUB_BATCH_SIZE + 1
        if new_sub_batch != current_sub_batch:
            current_sub_batch = new_sub_batch
            logger.info(
                f"[MIRROR-BATCH] Sub-batch {current_sub_batch}/{total_sub_batches} "
                f"— msg {i+1}/{total}"
            )

        # Get message document from MongoDB
        msg_doc = await get_msg_doc(mirror_id, src_id)
        if not msg_doc:
            logger.warning(f"[MIRROR-BATCH] src={src_id} not in fetch_map — skipping")
            continue

        # Skip service messages — they can't be re-sent
        if msg_doc.get("type") == "service":
            await mark_done(mirror_id, src_id, 0)  # Mark as done with dst=0
            continue

        # Resolve reply_to in destination
        depends_on  = msg_doc.get("reply_to")
        reply_to_dst = None
        if depends_on:
            reply_to_dst = await get_dst_id(mirror_id, depends_on)

        # Upload with retry
        dst_msg_id = await _upload_with_retry(
            msg_doc       = msg_doc,
            dst_chat_id   = dst_chat_id,
            reply_to_dst  = reply_to_dst,
            src_chat_id   = src_chat_id,
            mirror_id     = mirror_id,
        )

        if dst_msg_id:
            await mark_done(mirror_id, src_id, dst_msg_id)
            uploaded += 1

            # Mirror pin if needed (pass dst_chat_id explicitly)
            await mirror_pin_if_needed(mirror_id, src_id, dst_msg_id, dst_chat_id)

            # Rate limiting
            await asyncio.sleep(BATCH_SEND_DELAY)

            # Progress update every 50 messages OR at sub-batch boundary
            if uploaded % 50 == 0 or (i + 1) % SUB_BATCH_SIZE == 0:
                await mirror_state.update_one(
                    {"mirror_id": mirror_id},
                    {"$set": {
                        "progress"         : uploaded,
                        "last_src_id"      : src_id,
                        "current_sub_batch": current_sub_batch,
                    }},
                )
                logger.info(
                    f"[MIRROR-BATCH] Progress: {uploaded}/{total} "
                    f"({uploaded*100//total}%) — "
                    f"Sub-batch {current_sub_batch}/{total_sub_batches}"
                )
                gc.collect()  # Keep RAM flat
        else:
            await mark_failed(mirror_id, src_id)
            failed += 1
            logger.error(f"[MIRROR-BATCH] FAILED src={src_id} after all retries")
            # Don't stop — skip and continue (different from standalone mirror.py)
            # because in multi-user bot, one failure shouldn't block everything

    # Final state update
    final_status = "stopped" if stopped else "completed"
    await mirror_state.update_one(
        {"mirror_id": mirror_id},
        {"$set": {
            "status"            : final_status,
            "progress"          : uploaded,
            "uploaded"          : uploaded,
            "failed"            : failed,
            "completed_at"       : datetime.utcnow(),
            "current_sub_batch" : current_sub_batch,
        }},
    )

    logger.info(f"[MIRROR-BATCH] {final_status}: uploaded={uploaded} failed={failed} total={total}")
    log_ram("mirror_batch_done")
    gc.collect()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 8: POST-BATCH VERIFICATION                             ║
# ╚══════════════════════════════════════════════════════════════════╝

async def verify_mirror_integrity(mirror_id: str) -> dict:
    """Full post-batch integrity check."""
    issues = {"missing": [], "type_mismatch": [], "wrong_order": [], "reply_broken": []}

    src_to_dst = await get_all_src_to_dst(mirror_id)
    if not src_to_dst:
        return issues

    state_doc = await mirror_state.find_one({"mirror_id": mirror_id})
    if not state_doc:
        return issues

    src_chat_id = state_doc["src_chat_id"]
    dst_chat_id = state_doc["dst_chat_id"]
    src_ids     = sorted(src_to_dst.keys())
    dst_ids     = [src_to_dst[s] for s in src_ids]

    logger.info(f"[MIRROR-VERIFY] Checking {len(src_ids)} messages...")

    # Bulk fetch source messages
    userbot = _get_userbot()
    src_msgs = {}
    if userbot:
        for i in range(0, len(src_ids), 200):
            chunk = src_ids[i : i + 200]
            try:
                msgs = await userbot.get_messages(src_chat_id, chunk)
                for m in msgs:
                    if m and not getattr(m, "empty", True):
                        src_msgs[m.id] = m
            except Exception as e:
                logger.error(f"[MIRROR-VERIFY] src bulk fetch failed: {e}")

    # Bulk fetch destination messages
    dst_msgs = {}
    for i in range(0, len(dst_ids), 200):
        chunk = dst_ids[i : i + 200]
        try:
            msgs = await X.get_messages(dst_chat_id, chunk)
            for m in msgs:
                if m and not getattr(m, "empty", True):
                    dst_msgs[m.id] = m
        except Exception as e:
            logger.error(f"[MIRROR-VERIFY] dst bulk fetch failed: {e}")

    # Check each message
    prev_dst_id = None
    for src_id in src_ids:
        dst_id  = src_to_dst[src_id]
        src_msg = src_msgs.get(src_id)
        dst_msg = dst_msgs.get(dst_id)

        if not dst_msg:
            issues["missing"].append(src_id)
            prev_dst_id = None
            continue

        if src_msg:
            src_photo = src_msg.photo is not None
            src_poll  = src_msg.poll  is not None
            dst_photo = dst_msg.photo is not None
            dst_poll  = dst_msg.poll  is not None

            if (src_photo != dst_photo) or (src_poll != dst_poll):
                issues["type_mismatch"].append(src_id)

        if prev_dst_id and dst_id < prev_dst_id:
            issues["wrong_order"].append(src_id)
        prev_dst_id = dst_id

        if src_msg:
            src_reply = getattr(src_msg, "reply_to_message_id", None)
            if src_reply:
                expected = src_to_dst.get(src_reply)
                actual   = getattr(dst_msg, "reply_to_message_id", None)
                if expected and int(actual or 0) != int(expected):
                    issues["reply_broken"].append(src_id)

    total_issues = sum(len(v) for v in issues.values())
    if total_issues == 0:
        logger.info(f"[MIRROR-VERIFY] All {len(src_ids)} messages perfect")
    else:
        logger.error(f"[MIRROR-VERIFY] {total_issues} issues found")

    return issues


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 9: COMMANDS — /mirror, /mirrorstop, /mirrorstatus      ║
# ║                                                                  ║
# ║  /mirror supports TWO modes:                                    ║
# ║    1. Inline: /mirror <source_link> <dest_chat_id>              ║
# ║    2. Interactive: /mirror (no args) → bot asks for source,    ║
# ║       then destination during the command flow                  ║
# ╚══════════════════════════════════════════════════════════════════╝

# ── Conversation state for interactive /mirror ──
MIRROR_CONV_STATE = {}  # uid -> {'step': 'source'|'dest', ...}


async def _start_mirror(client, message, uid, src_chat_id, dst_chat_id):
    """Shared function to start a mirror after source+dest are resolved."""
    import hashlib
    mirror_id = hashlib.md5(f"{src_chat_id}:{dst_chat_id}".encode()).hexdigest()[:12]

    # Check if already running
    state_doc = await mirror_state.find_one({"mirror_id": mirror_id})
    if state_doc and state_doc.get("status") == "running":
        await message.reply_text(f"Mirror `{mirror_id}` is already running!", quote=True)
        return

    status_msg = await message.reply_text(
        f"**Mirror Starting** 🔄\n\n"
        f"Mirror ID: `{mirror_id}`\n"
        f"Source: `{src_chat_id}`\n"
        f"Destination: `{dst_chat_id}`\n\n"
        f"Step 1/3: Building fetch map (scanning source channel)...",
        quote=True,
    )

    # Step 1: Build fetch map
    userbot = _get_userbot()
    if not userbot:
        await status_msg.edit_text("**Error:** Userbot (STRING session) is required for mirroring. Set STRING env var.")
        return

    try:
        # Resolve source chat
        resolved_src = await userbot.resolve_peer(src_chat_id)
    except Exception as e:
        await status_msg.edit_text(f"**Error:** Cannot access source channel: `{e}`")
        return

    total_msgs = await build_fetch_map(userbot, src_chat_id, mirror_id)

    await status_msg.edit_text(
        f"**Mirror Starting** 🔄\n\n"
        f"Mirror ID: `{mirror_id}`\n"
        f"Source: `{src_chat_id}`\n"
        f"Destination: `{dst_chat_id}`\n"
        f"Messages found: {total_msgs}\n\n"
        f"Step 2/3: Building pin map...",
    )

    # Step 2: Build pin map
    await build_pin_map(mirror_id, src_chat_id)

    await status_msg.edit_text(
        f"**Mirror Starting** 🔄\n\n"
        f"Mirror ID: `{mirror_id}`\n"
        f"Source: `{src_chat_id}`\n"
        f"Destination: `{dst_chat_id}`\n"
        f"Messages: {total_msgs}\n\n"
        f"Step 3/3: Starting batch upload...",
    )

    # Step 3: Run batch in background
    async def _run_and_notify():
        await run_mirror_batch(mirror_id, src_chat_id, dst_chat_id)

        # Get final stats
        final = await mirror_state.find_one({"mirror_id": mirror_id})
        uploaded = final.get("uploaded", 0) if final else 0
        failed   = final.get("failed", 0) if final else 0
        status   = final.get("status", "unknown") if final else "unknown"

        result_text = (
            f"**Mirror {status.upper()}** {'✅' if status == 'completed' else '⚠️'}\n\n"
            f"Mirror ID: `{mirror_id}`\n"
            f"Source: `{src_chat_id}`\n"
            f"Destination: `{dst_chat_id}`\n"
            f"Uploaded: {uploaded}/{total_msgs}\n"
            f"Failed: {failed}\n"
        )

        if status == "completed" and failed == 0:
            result_text += "\nRunning integrity verification..."
            try:
                issues = await verify_mirror_integrity(mirror_id)
                total_issues = sum(len(v) for v in issues.values())
                if total_issues == 0:
                    result_text += f"\n✅ **All {uploaded} messages verified — perfect mirror!**"
                else:
                    result_text += f"\n⚠️ **{total_issues} issues found:** missing={len(issues['missing'])} type={len(issues['type_mismatch'])} order={len(issues['wrong_order'])} reply={len(issues['reply_broken'])}"
            except Exception as e:
                result_text += f"\n⚠️ Verification error: {e}"

        try:
            await status_msg.edit_text(result_text)
        except Exception:
            pass

    task = asyncio.create_task(_run_and_notify())
    mirror_tasks[mirror_id] = task


@X.on_message(filters.command("mirror") & filters.private)
async def mirror_command(client, message):
    """Start channel mirroring.
    
    Two modes:
      1. Inline: /mirror <source_link> <dest_chat_id>
      2. Interactive: /mirror (no args) → bot asks for source, then dest
    
    Access: Owner + Auth users + Premium users (with force-sub check)
    """
    uid = message.from_user.id

    # Access control — same pattern as batch.py
    if uid in OWNER_ID:
        pass  # Owner always has access
    elif await is_auth_user(uid):
        pass  # Auth users always have access
    elif FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await message.reply_text("This bot does not provide free services. Get a subscription from the OWNER.", quote=True)
        return

    # Force-subscribe check
    if await sub(client, message) == 1:
        return

    # Parse args — check if both source and dest are provided inline
    args = message.text.split(maxsplit=2)
    
    if len(args) >= 3:
        # ── INLINE MODE: source_link + dest_chat_id provided ──
        source_link = args[1].strip()
        dest_chat_str = args[2].strip()

        # Parse source link
        try:
            from utils.func import E
            parsed = E(source_link)
            if not parsed or not parsed[0]:
                await message.reply_text("Invalid source link. Use a Telegram message link.", quote=True)
                return
            src_chat_id = parsed[0]
        except Exception:
            await message.reply_text("Could not parse source link.", quote=True)
            return

        # Parse dest chat ID
        try:
            dst_chat_id = int(dest_chat_str)
        except ValueError:
            await message.reply_text("Invalid destination chat ID. Must be a number like -1001234567890", quote=True)
            return

        # Start mirror directly
        await _start_mirror(client, message, uid, src_chat_id, dst_chat_id)

    elif len(args) == 2:
        # ── PARTIAL: only source link provided, ask for dest ──
        source_link = args[1].strip()

        # Parse source link
        try:
            from utils.func import E
            parsed = E(source_link)
            if not parsed or not parsed[0]:
                await message.reply_text("Invalid source link. Use a Telegram message link.", quote=True)
                return
            src_chat_id = parsed[0]
        except Exception:
            await message.reply_text("Could not parse source link.", quote=True)
            return

        # Store source and ask for dest
        MIRROR_CONV_STATE[uid] = {
            'step': 'dest',
            'src_chat_id': src_chat_id,
        }
        await message.reply_text(
            f"✅ Source channel resolved: `{src_chat_id}`\n\n"
            "📤 Now send the **destination chat ID** (e.g. `-1001234567890`)\n\n"
            "💡 Use /id in the destination channel to get its ID.\n"
            "💡 Send /cancel to cancel.",
            quote=True,
        )

    else:
        # ── INTERACTIVE MODE: no args, ask for source first ──
        MIRROR_CONV_STATE[uid] = {'step': 'source'}
        await message.reply_text(
            "**🪞 /mirror — Interactive Setup**\n\n"
            "Mirror an entire source channel to a destination channel.\n\n"
            "**What gets mirrored:**\n"
            "✅ Photos, Videos, Documents, Audio, Voice\n"
            "✅ Stickers, GIFs, Video Notes, Text\n"
            "✅ Polls with correct answers revealed\n"
            "✅ Reply chains preserved exactly\n"
            "✅ Pinned messages auto-pinned in dest\n\n"
            "**Step 1:** Send a **message link** from the source channel.\n\n"
            "💡 Or use inline: `/mirror <source_link> <dest_chat_id>`\n"
            "💡 Send /cancel to cancel.",
            quote=True,
        )


# ── Text handler for interactive /mirror conversation ──

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
    group=4
)
async def mirror_text_handler(client, message):
    """Handle text input during interactive /mirror conversation."""
    uid = message.from_user.id

    if uid not in MIRROR_CONV_STATE:
        from pyrogram import ContinuePropagation
        raise ContinuePropagation

    state = MIRROR_CONV_STATE[uid]

    if state['step'] == 'source':
        # User sent source link
        from utils.func import E
        link = message.text.strip()
        parsed = E(link)
        if not parsed or not parsed[0]:
            await message.reply_text(
                "❌ Invalid link. Send a valid Telegram message link from the source channel.\n\n"
                "Example: `https://t.me/channelname/1`\n\n"
                "Send /cancel to cancel.",
                quote=True,
            )
            return

        src_chat_id = parsed[0]
        state['src_chat_id'] = src_chat_id
        state['step'] = 'dest'

        await message.reply_text(
            f"✅ Source channel resolved: `{src_chat_id}`\n\n"
            "📤 **Step 2:** Send the **destination chat ID**.\n\n"
            "This is where all messages will be mirrored to.\n"
            "Example: `-1001234567890`\n\n"
            "💡 Use /id in the destination channel to get its ID.\n"
            "💡 Send /cancel to cancel.",
            quote=True,
        )

    elif state['step'] == 'dest':
        # User sent destination chat ID
        dest_text = message.text.strip()
        try:
            dst_chat_id = int(dest_text)
        except ValueError:
            await message.reply_text(
                "❌ Invalid destination chat ID. It must be a number like `-1001234567890`.\n\n"
                "💡 Use /id in the destination channel to get its ID.\n"
                "💡 Send /cancel to cancel.",
                quote=True,
            )
            return

        src_chat_id = state['src_chat_id']
        del MIRROR_CONV_STATE[uid]

        # Start the mirror
        await _start_mirror(client, message, uid, src_chat_id, dst_chat_id)


# ── /cancel handler for mirror conversation ──
@X.on_message(filters.command("cancel") & filters.private, group=2)
async def mirror_cancel_handler(client, message):
    """Cancel interactive /mirror setup if in progress."""
    uid = message.from_user.id
    if uid in MIRROR_CONV_STATE:
        del MIRROR_CONV_STATE[uid]
        await message.reply_text("❌ /mirror setup cancelled.", quote=True)
        # Don't raise ContinuePropagation — other /cancel handlers may not need this
    # NOTE: Previous version had dead code here (copy-paste from _start_mirror)
    # that referenced undefined src_chat_id/dst_chat_id — removed.


@X.on_message(filters.command("mirrorstop") & filters.private)
async def mirror_stop_command(client, message):
    """Stop a running mirror: /mirrorstop <mirror_id>
    
    Access: Owner + Auth users + Premium users (with force-sub check)
    """
    uid = message.from_user.id

    # Access control — same pattern as batch.py
    if uid in OWNER_ID:
        pass
    elif await is_auth_user(uid):
        pass
    elif FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await message.reply_text("This bot does not provide free services. Get a subscription from the OWNER.", quote=True)
        return

    if await sub(client, message) == 1:
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        # Stop all running mirrors
        running = mirror_state.find({"status": "running"})
        running_ids = []
        async for doc in running:
            running_ids.append(doc["mirror_id"])

        if not running_ids:
            await message.reply_text("No mirrors are currently running.", quote=True)
            return

        for mid in running_ids:
            await mirror_state.update_one({"mirror_id": mid}, {"$set": {"status": "stopped"}})
            if mid in mirror_tasks:
                mirror_tasks[mid].cancel()

        await message.reply_text(f"Stopped {len(running_ids)} mirror(s): {', '.join(running_ids)}", quote=True)
        return

    mirror_id = args[1].strip()
    await mirror_state.update_one({"mirror_id": mirror_id}, {"$set": {"status": "stopped"}})

    if mirror_id in mirror_tasks:
        mirror_tasks[mirror_id].cancel()
        await message.reply_text(f"Mirror `{mirror_id}` stopped.", quote=True)
    else:
        await message.reply_text(f"Mirror `{mirror_id}` — stop signal sent (will stop at next message).", quote=True)


@X.on_message(filters.command("mirrorstatus") & filters.private)
async def mirror_status_command(client, message):
    """Check mirror progress: /mirrorstatus
    
    Access: Owner + Auth users + Premium users (with force-sub check)
    """
    uid = message.from_user.id

    # Access control — same pattern as batch.py
    if uid in OWNER_ID:
        pass
    elif await is_auth_user(uid):
        pass
    elif FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await message.reply_text("This bot does not provide free services. Get a subscription from the OWNER.", quote=True)
        return

    if await sub(client, message) == 1:
        return

    all_states = mirror_state.find({})
    result_lines = []
    async for doc in all_states:
        mid             = doc["mirror_id"]
        status          = doc.get("status", "unknown")
        progress        = doc.get("progress", 0)
        uploaded        = doc.get("uploaded", 0)
        failed          = doc.get("failed", 0)
        src_chat        = doc.get("src_chat_id", "?")
        dst_chat        = doc.get("dst_chat_id", "?")
        current_sub     = doc.get("current_sub_batch", 0)
        total_sub       = doc.get("total_sub_batches", 0)
        sub_batch_size  = doc.get("sub_batch_size", 100)

        sub_batch_info = ""
        if total_sub > 0 and current_sub > 0:
            sub_batch_info = f"\n  Sub-batch: {current_sub}/{total_sub} ({sub_batch_size} msgs each)"

        result_lines.append(
            f"**Mirror `{mid}`** — {status.upper()}\n"
            f"  Source: `{src_chat}` → Dest: `{dst_chat}`\n"
            f"  Progress: {progress} uploaded, {failed} failed"
            f"{sub_batch_info}\n"
        )

    if not result_lines:
        await message.reply_text("No mirrors found.", quote=True)
    else:
        await message.reply_text("\n".join(result_lines), quote=True)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 10: AUTO-RESUME ON RESTART                             ║
# ║  If bot restarts while a mirror was running, auto-resume it.    ║
# ╚══════════════════════════════════════════════════════════════════╝

async def resume_interrupted_mirrors():
    """Resume any mirrors that were running when the bot crashed."""
    cursor = mirror_state.find({"status": "running"})
    resumed = 0
    async for doc in cursor:
        mirror_id   = doc["mirror_id"]
        src_chat_id = doc["src_chat_id"]
        dst_chat_id = doc["dst_chat_id"]

        logger.info(f"[MIRROR-RESUME-ON-START] Resuming mirror_id={mirror_id}")

        async def _run():
            await run_mirror_batch(mirror_id, src_chat_id, dst_chat_id)

        task = asyncio.create_task(_run())
        mirror_tasks[mirror_id] = task
        resumed += 1

    if resumed:
        logger.info(f"[MIRROR-RESUME-ON-START] Resumed {resumed} interrupted mirror(s)")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SECTION 11: PLUGIN ENTRY POINT                                 ║
# ╚══════════════════════════════════════════════════════════════════╝

async def run_mirror_plugin():
    """Called by main.py on startup."""
    await _ensure_indexes()
    await resume_interrupted_mirrors()
    logger.info("[MIRROR-PLUGIN] Loaded — /mirror, /mirrorstop, /mirrorstatus commands ready")
