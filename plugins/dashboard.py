# Copyright (c) 2025 devgagan : https://github.com/devgajanin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

"""
Dashboard Plugin — User Performance Dashboard for Poll/Quiz Users

Architecture:
  - Tracks every poll answer via raw Telegram updates (UpdateMessagePoll)
  - Assigns HQ-style IDs (HQ1, HQ2, HQ3...) to questions
  - Stores full explanations (text, image, video, photo) from dest channel
  - /dashboard command generates a permanent link to web dashboard
  - Web dashboard shows: score card, wrong questions with explanations,
    topic breakdown, difficulty analysis, streaks, leaderboard, trends

MongoDB Collections:
  - dashboard_polls: poll metadata (HQ ID, question, options, correct answer, topic, difficulty)
  - dashboard_user_answers: every user answer (user_id, HQ ID, selected, correct, is_correct, timestamp)
  - dashboard_question_explanations: explanations (text, images, videos, photos)
  - dashboard_users: user profiles (user_id, username, name, dashboard_secret)

Poll Answer Tracking:
  Uses Pyrogram raw update handler to capture UpdateMessagePoll events.
  For non-anonymous polls, Telegram includes PollAnswer data with user info.
  Polls are sent with is_anonymous=False to enable tracking.
"""

import os
import hashlib
import asyncio
import traceback
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from shared_client import app as X

# CRITICAL: First print — if you don't see this, the plugin is crashing during import
print("[DASHBOARD] Module loading...")

# ── Lazy MongoDB Connection ─────────────────────────────────────────
# Do NOT connect at module level — it can hang for 30+ seconds if MONGO_DB
# is misconfigured, and batch.py imports from this module at its import time,
# so a hang here cascades to block ALL plugin loading.
_mongo_client = None
_db = None
polls_col = None
answers_col = None
explanations_col = None
users_col = None

async def _init_mongo():
    """Initialize MongoDB connection lazily. Returns True on success."""
    global _mongo_client, _db, polls_col, answers_col, explanations_col, users_col

    if _db is not None:
        return True  # Already initialized

    try:
        from config import MONGO_DB as MONGO_URI, DB_NAME

        if not MONGO_URI:
            print("[DASHBOARD] ERROR: MONGO_DB env var is empty!")
            return False

        from motor.motor_asyncio import AsyncIOMotorClient

        _mongo_client = AsyncIOMotorClient(MONGO_URI)
        _db = _mongo_client[DB_NAME]

        polls_col = _db["dashboard_polls"]
        answers_col = _db["dashboard_user_answers"]
        explanations_col = _db["dashboard_question_explanations"]
        users_col = _db["dashboard_users"]

        print(f"[DASHBOARD] MongoDB connected to db={DB_NAME}")
        return True
    except Exception as e:
        print(f"[DASHBOARD] ERROR: Failed to connect to MongoDB: {e}")
        traceback.print_exc()
        return False


async def _ensure_mongo():
    """Ensure MongoDB is connected. Raise if not available."""
    if _db is None:
        ok = await _init_mongo()
        if not ok:
            raise RuntimeError("Dashboard MongoDB not available — check MONGO_DB env var")
    return True

# Dashboard base URL — set via env var DASHBOARD_URL
DASHBOARD_BASE_URL = os.getenv("DASHBOARD_URL", "https://yourdomain.com")
# Secret key for signing dashboard URLs
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "hq_dashboard_secret_key_2025")

# HQ ID prefix
HQ_PREFIX = os.getenv("HQ_PREFIX", "HQ")


# ── Indexes (created once) ──────────────────────────────────────────
async def _ensure_indexes():
    await _ensure_mongo()
    await polls_col.create_index("hq_id", unique=True)
    await polls_col.create_index([("dest_channel_id", 1), ("dest_msg_id", 1)], unique=True)
    await polls_col.create_index([("dest_channel_id", 1), ("topic", 1)])
    await polls_col.create_index("poll_id")
    await answers_col.create_index([("user_id", 1), ("hq_id", 1)], unique=True)
    await answers_col.create_index([("user_id", 1), ("is_correct", 1)])
    await answers_col.create_index([("user_id", 1), ("answered_at", -1)])
    await answers_col.create_index("hq_id")
    await explanations_col.create_index("hq_id", unique=True)
    await users_col.create_index("user_id", unique=True)
    await users_col.create_index("dashboard_secret", unique=True)
    print("[DASHBOARD-DB] Indexes ready")


# ── HQ ID Generation ────────────────────────────────────────────────
async def _get_next_hq_id():
    """Get the next HQ-style ID (HQ1, HQ2, HQ3...)."""
    await _ensure_mongo()
    last_poll = await polls_col.find_one(sort=[("hq_number", -1)])
    next_num = (last_poll["hq_number"] + 1) if last_poll else 1
    return f"{HQ_PREFIX}{next_num}", next_num


async def get_or_assign_hq_id(dest_channel_id, dest_msg_id, poll_data: dict):
    """Get existing HQ ID for a poll, or assign a new one.
    
    Called when a poll is sent to the dest channel (from batch.py).
    poll_data should contain:
      - question: str
      - options: list of str
      - correct_option_id: int or None
      - is_quiz: bool
      - poll_id: str (Telegram's internal poll ID)
      - topic: str or None
      - difficulty: str or None
      - source_channel_id: str or None
      - source_msg_id: int or None
    
    Returns the HQ ID string.
    """
    await _ensure_mongo()
    # Check if already assigned
    existing = await polls_col.find_one(
        {"dest_channel_id": dest_channel_id, "dest_msg_id": dest_msg_id}
    )
    if existing:
        return existing["hq_id"]
    
    # Also check by poll_id (Telegram's internal ID)
    poll_id = poll_data.get("poll_id")
    if poll_id:
        existing_by_poll = await polls_col.find_one({"poll_id": poll_id})
        if existing_by_poll:
            return existing_by_poll["hq_id"]
    
    # Assign new HQ ID
    hq_id, hq_number = await _get_next_hq_id()
    
    doc = {
        "hq_id": hq_id,
        "hq_number": hq_number,
        "dest_channel_id": dest_channel_id,
        "dest_msg_id": dest_msg_id,
        "poll_id": poll_id,
        "question": poll_data.get("question", ""),
        "options": poll_data.get("options", []),
        "correct_option_id": poll_data.get("correct_option_id"),
        "is_quiz": poll_data.get("is_quiz", False),
        "topic": poll_data.get("topic"),
        "difficulty": poll_data.get("difficulty"),
        "source_channel_id": poll_data.get("source_channel_id"),
        "source_msg_id": poll_data.get("source_msg_id"),
        "created_at": datetime.utcnow(),
    }
    
    try:
        await polls_col.update_one(
            {"dest_channel_id": dest_channel_id, "dest_msg_id": dest_msg_id},
            {"$set": doc},
            upsert=True,
        )
        print(f"[DASHBOARD] Assigned {hq_id} to poll in ch={dest_channel_id} msg={dest_msg_id}")
        
        # Try to link existing explanation from the source channel
        # The explanation_listener may have already stored an explanation
        # for this poll in the poll_explanations collection
        source_channel_id = poll_data.get("source_channel_id")
        source_msg_id = poll_data.get("source_msg_id")
        if source_channel_id and source_msg_id:
            try:
                existing_expl = await _db["poll_explanations"].find_one({
                    "channel_id": str(source_channel_id),
                    "poll_msg_id": int(source_msg_id),
                })
                if existing_expl:
                    await store_question_explanation(hq_id, {
                        "text": existing_expl.get("text"),
                        "images": [],
                        "videos": [],
                        "photos": [existing_expl["photo_file_id"]] if existing_expl.get("photo_file_id") and existing_expl.get("kind") not in ("video",) else [],
                        "kind": existing_expl.get("kind", "text"),
                        "dest_channel_message_id": existing_expl.get("explanation_msg_id"),
                    })
                    print(f"[DASHBOARD] Linked existing explanation from source to {hq_id}")
            except Exception as _link_err:
                print(f"[DASHBOARD] Failed to link explanation (non-fatal): {_link_err}")
    except Exception as e:
        print(f"[DASHBOARD] Error assigning HQ ID: {e}")
    
    return hq_id


# ── Explanation Storage ─────────────────────────────────────────────
async def store_question_explanation(hq_id: str, explanation_data: dict):
    """Store explanation for a question.
    
    explanation_data can contain:
      - text: str
      - images: list of URLs
      - videos: list of URLs  
      - photos: list of URLs
      - kind: str ("text" | "image" | "video" | "photo+text" | "rich")
      - dest_channel_message_id: int (link to message in dest channel)
    """
    await _ensure_mongo()
    doc = {
        "hq_id": hq_id,
        "text": explanation_data.get("text"),
        "images": explanation_data.get("images", []),
        "videos": explanation_data.get("videos", []),
        "photos": explanation_data.get("photos", []),
        "kind": explanation_data.get("kind", "text"),
        "dest_channel_message_id": explanation_data.get("dest_channel_message_id"),
        "updated_at": datetime.utcnow(),
    }
    
    await explanations_col.update_one(
        {"hq_id": hq_id},
        {"$set": doc},
        upsert=True,
    )
    print(f"[DASHBOARD] Stored explanation for {hq_id}: kind={doc['kind']}")


# ── User Answer Tracking ───────────────────────────────────────────
async def record_user_answer(user_id: int, username: str, first_name: str,
                              hq_id: str, selected_option: int,
                              correct_option: int | None, is_correct: bool,
                              response_time_seconds: float | None = None):
    """Record a user's answer to a poll question.
    
    Uses upsert so changing answer updates the existing record.
    """
    await _ensure_mongo()
    doc = {
        "user_id": user_id,
        "username": username,
        "first_name": first_name,
        "hq_id": hq_id,
        "selected_option": selected_option,
        "correct_option": correct_option,
        "is_correct": is_correct,
        "response_time_seconds": response_time_seconds,
        "answered_at": datetime.utcnow(),
    }
    
    await answers_col.update_one(
        {"user_id": user_id, "hq_id": hq_id},
        {"$set": doc},
        upsert=True,
    )
    
    # Also update user profile
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "last_active": datetime.utcnow(),
        }},
        upsert=True,
    )


# ── Raw Update Handler for Poll Answers ────────────────────────────
# In-memory option data mapping: {poll_id_hex: [option_data_bytes, ...]}
# Populated when we send polls, used to match PollAnswer option bytes to indices.
_poll_option_data_map = {}


def register_poll_option_data(poll_id_str: str, option_data_list: list):
    """Register the option.data bytes for a poll so we can match PollAnswer events.
    
    Called from batch.py after a poll is successfully sent.
    poll_id_str: hex string of the poll's internal ID
    option_data_list: list of bytes objects, one per option (from poll.options[N].data)
    """
    _poll_option_data_map[poll_id_str] = option_data_list
    # Keep the map bounded — remove old entries if it grows too large
    if len(_poll_option_data_map) > 10000:
        # Remove oldest half
        keys = list(_poll_option_data_map.keys())
        for k in keys[:5000]:
            del _poll_option_data_map[k]


def _match_option_index(poll_id_str: str, answer_option_bytes: bytes) -> int | None:
    """Match a PollAnswer's option bytes to an option index.
    
    Each poll option has unique .data bytes assigned by Telegram.
    When a user votes, PollAnswer.options contains those bytes.
    We match them to the stored option data to get the 0-based index.
    """
    stored = _poll_option_data_map.get(poll_id_str)
    if not stored:
        return None
    for idx, data_bytes in enumerate(stored):
        if data_bytes == answer_option_bytes:
            return idx
    return None


@X.on_raw_update()
async def on_raw_poll_update(client, update, users, chats):
    """Capture poll answer events via raw Telegram updates.
    
    When a user votes in a non-anonymous poll that the bot sent,
    Telegram sends an UpdateMessagePoll with PollAnswer data.
    We parse this to extract user ID and selected option, then
    record the answer in the dashboard.
    """
    try:
        # Skip if MongoDB not initialized yet
        if _db is None:
            return
        
        update_type = type(update).__name__
        
        # ── PollAnswer event: user voted in a non-anonymous poll ──
        # This is the PRIMARY answer tracking mechanism for the dashboard.
        # Pyrogram sends this as a raw Update with PollAnswer type.
        # It contains: poll_id, user_id, and options (list of option data bytes)
        
        if update_type == "UpdateMessagePoll":
            # Extract poll_id
            poll_id_bytes = getattr(update, 'poll_id', None)
            if not poll_id_bytes:
                return
            
            if isinstance(poll_id_bytes, bytes):
                poll_id_str = poll_id_bytes.hex()
            else:
                poll_id_str = str(poll_id_bytes)
            
            # Look up our tracked poll
            poll_doc = await polls_col.find_one({"poll_id": poll_id_str})
            if not poll_doc:
                return
            
            hq_id = poll_doc["hq_id"]
            correct_option = poll_doc.get("correct_option_id")
            
            # Try to get the poll results with voter info
            poll_obj = getattr(update, 'poll', None)
            if poll_obj:
                results = getattr(poll_obj, 'results', None)
                if results:
                    # Check for solution (built-in explanation)
                    solution = getattr(results, 'solution', None)
                    if solution and not await explanations_col.find_one({"hq_id": hq_id}):
                        # Store built-in explanation if we don't have one yet
                        await store_question_explanation(hq_id, {
                            "text": solution,
                            "kind": "text",
                        })
            
            # Process PollAnswer data from the update
            # In Pyrofork, UpdateMessagePoll sometimes wraps the PollAnswer
            poll_answer = getattr(update, 'poll_answer', None)
            if poll_answer:
                user_id = getattr(poll_answer, 'user_id', None)
                option_data_list = getattr(poll_answer, 'options', [])
                
                if user_id and option_data_list:
                    # Match option bytes to index
                    selected_idx = _match_option_index(poll_id_str, option_data_list[0]) if option_data_list else None
                    
                    if selected_idx is None:
                        # Fallback: if we can't match, use the first option as a guess
                        # This shouldn't happen if register_poll_option_data was called
                        return
                    
                    # Determine correctness
                    is_correct = (selected_idx == correct_option) if correct_option is not None else None
                    
                    # Get user details
                    user_info = users.get(user_id) if users else None
                    username = ""
                    first_name = ""
                    if user_info:
                        username = getattr(user_info, 'username', '') or ''
                        first_name = getattr(user_info, 'first_name', '') or ''
                    else:
                        # Try from our DB
                        user_doc = await users_col.find_one({"user_id": user_id})
                        if user_doc:
                            username = user_doc.get("username", "")
                            first_name = user_doc.get("first_name", "")
                    
                    # Record the answer
                    await record_user_answer(
                        user_id=user_id,
                        username=username,
                        first_name=first_name,
                        hq_id=hq_id,
                        selected_option=selected_idx,
                        correct_option=correct_option,
                        is_correct=is_correct if is_correct is not None else False,
                    )
                    print(f"[DASHBOARD] PollAnswer: user={user_id} hq={hq_id} selected={selected_idx} correct={correct_option} {'✅' if is_correct else '❌'}")
        
        elif update_type == "PollAnswer":
            # Direct PollAnswer event (some Pyrogram versions send this separately)
            poll_id_bytes = getattr(update, 'poll_id', None)
            if not poll_id_bytes:
                return
            
            if isinstance(poll_id_bytes, bytes):
                poll_id_str = poll_id_bytes.hex()
            else:
                poll_id_str = str(poll_id_bytes)
            
            # Look up our tracked poll
            poll_doc = await polls_col.find_one({"poll_id": poll_id_str})
            if not poll_doc:
                return
            
            hq_id = poll_doc["hq_id"]
            correct_option = poll_doc.get("correct_option_id")
            
            # Get user info
            user_id = getattr(update, 'user', None)
            if not user_id:
                user_id = getattr(update, 'user_id', None)
            if not user_id:
                return
            
            # If user is a User object, extract ID
            if hasattr(user_id, 'id'):
                user_obj = user_id
                user_id = user_obj.id
                username = getattr(user_obj, 'username', '') or ''
                first_name = getattr(user_obj, 'first_name', '') or ''
            else:
                # Get from users dict or DB
                user_info = users.get(user_id) if users else None
                if user_info:
                    username = getattr(user_info, 'username', '') or ''
                    first_name = getattr(user_info, 'first_name', '') or ''
                else:
                    user_doc = await users_col.find_one({"user_id": user_id})
                    username = user_doc.get("username", "") if user_doc else ""
                    first_name = user_doc.get("first_name", "") if user_doc else ""
            
            # Get selected options
            option_data_list = getattr(update, 'options', [])
            selected_idx = None
            
            if option_data_list:
                selected_idx = _match_option_index(poll_id_str, option_data_list[0])
            
            if selected_idx is None:
                return
            
            # Determine correctness
            is_correct = (selected_idx == correct_option) if correct_option is not None else None
            
            # Record the answer
            await record_user_answer(
                user_id=user_id,
                username=username,
                first_name=first_name,
                hq_id=hq_id,
                selected_option=selected_idx,
                correct_option=correct_option,
                is_correct=is_correct if is_correct is not None else False,
            )
            print(f"[DASHBOARD] PollAnswer direct: user={user_id} hq={hq_id} selected={selected_idx} correct={correct_option} {'✅' if is_correct else '❌'}")
    
    except Exception as e:
        # Don't let errors in update handling crash the bot
        print(f"[DASHBOARD] Raw update handler error: {e}")


# ── Inline Callback Handler for Quiz Answer Tracking ───────────────
# This is the PRIMARY answer tracking mechanism.
# When a user votes in a quiz poll, Telegram shows them if they're right/wrong.
# We add a callback button that captures the answer explicitly.

@X.on_callback_query(filters.regex(r"^hq_ans:"))
async def on_hq_answer_callback(client, callback_query):
    """Handle answer logging via inline button.
    
    Button data format: hq_ans:{hq_id}:{option_index}:{correct_index}
    Example: hq_ans:HQ12:2:1 (user selected option 2, correct is option 1)
    """
    try:
        data = callback_query.data
        parts = data.split(":")
        if len(parts) != 4:
            await callback_query.answer("Invalid data", show_alert=True)
            return
        
        hq_id = parts[1]
        selected_idx = int(parts[2])
        correct_idx = int(parts[3])
        
        user = callback_query.from_user
        user_id = user.id
        username = user.username or ""
        first_name = user.first_name or ""
        
        is_correct = (selected_idx == correct_idx) if correct_idx >= 0 else None
        
        # Record the answer
        await record_user_answer(
            user_id=user_id,
            username=username,
            first_name=first_name,
            hq_id=hq_id,
            selected_option=selected_idx,
            correct_option=correct_idx if correct_idx >= 0 else None,
            is_correct=is_correct if is_correct is not None else False,
        )
        
        # Show feedback
        if correct_idx < 0:
            # Regular poll (no correct answer)
            option_letter = chr(65 + selected_idx) if selected_idx < 26 else str(selected_idx + 1)
            await callback_query.answer(f"Answer logged: Option {option_letter}", show_alert=False)
        elif is_correct:
            correct_letter = chr(65 + correct_idx) if correct_idx < 26 else str(correct_idx + 1)
            await callback_query.answer(f"Correct! Answer: {correct_letter}", show_alert=False)
        else:
            correct_letter = chr(65 + correct_idx) if correct_idx < 26 else str(correct_idx + 1)
            selected_letter = chr(65 + selected_idx) if selected_idx < 26 else str(selected_idx + 1)
            await callback_query.answer(
                f"Wrong! You selected {selected_letter}, correct is {correct_letter}",
                show_alert=True
            )
    
    except Exception as e:
        print(f"[DASHBOARD] Error in hq_ans callback: {e}")
        try:
            await callback_query.answer("Error recording answer", show_alert=True)
        except:
            pass


# ── Dashboard URL Generation ───────────────────────────────────────
def _generate_dashboard_secret(user_id: int) -> str:
    """Generate a permanent secret for a user's dashboard URL."""
    raw = f"{user_id}:{DASHBOARD_SECRET}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _generate_dashboard_url(user_id: int) -> str:
    """Generate a permanent dashboard URL for a user."""
    secret = _generate_dashboard_secret(user_id)
    return f"{DASHBOARD_BASE_URL}/d/{secret}"


async def _ensure_user_secret(user_id: int, username: str, first_name: str):
    """Ensure user has a dashboard_secret stored in DB."""
    await _ensure_mongo()
    existing = await users_col.find_one({"user_id": user_id})
    if existing and existing.get("dashboard_secret"):
        return existing["dashboard_secret"]
    
    secret = _generate_dashboard_secret(user_id)
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "dashboard_secret": secret,
            "created_at": datetime.utcnow(),
        }},
        upsert=True,
    )
    return secret


# ── /dashboard Command ─────────────────────────────────────────────
@X.on_message(filters.command("dashboard") & filters.private)
async def dashboard_command(client, message):
    """Send permanent dashboard link to user.
    
    Access: Owner + Auth users + Premium users (with force-sub check)
    """
    uid = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or ""
    
    print(f"[DASHBOARD] /dashboard called by user_id={uid} username=@{username}")
    
    try:
        # Access control
        from config import OWNER_ID, FREEMIUM_LIMIT
        from utils.func import is_auth_user, is_premium_user
        from plugins.start import subscribe as sub
        
        if uid in OWNER_ID:
            pass
        elif await is_auth_user(uid):
            pass
        elif FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
            await message.reply_text("This bot does not provide free services. Get a subscription from the OWNER.", quote=True)
            return
        
        # Force-subscribe check
        if await sub(client, message) == 1:
            return
        
        # Ensure MongoDB is ready
        try:
            await _ensure_mongo()
        except RuntimeError as db_err:
            await message.reply_text(
                "Dashboard is currently unavailable. Please try again later.\n\n"
                f"_Error: Database not configured_",
                quote=True,
            )
            print(f"[DASHBOARD] MongoDB not available for uid={uid}: {db_err}")
            return
        
        # Generate permanent dashboard URL
        secret = await _ensure_user_secret(uid, username, first_name)
        dashboard_url = f"{DASHBOARD_BASE_URL}/d/{secret}"
        
        # Get quick stats
        total = await answers_col.count_documents({"user_id": uid})
        correct = await answers_col.count_documents({"user_id": uid, "is_correct": True})
        wrong = await answers_col.count_documents({"user_id": uid, "is_correct": False})
        accuracy = (correct * 100 // total) if total > 0 else 0
        
        # Get rank
        rank = 0
        try:
            pipeline = [
                {"$group": {"_id": "$user_id", "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}}, "total": {"$sum": 1}}},
                {"$addFields": {"accuracy": {"$cond": [{"$eq": ["$total", 0]}, 0, {"$multiply": [{"$divide": ["$correct", "$total"]}, 100]}]}}},
                {"$sort": {"correct": -1, "accuracy": -1}},
            ]
            all_users = await answers_col.aggregate(pipeline).to_list(length=None)
            rank = next((i + 1 for i, u in enumerate(all_users) if u["_id"] == uid), len(all_users) + 1)
        except Exception as rank_err:
            print(f"[DASHBOARD] Rank calculation failed (non-fatal): {rank_err}")
        
        print(f"[DASHBOARD] Sending dashboard link to uid={uid}: total={total} correct={correct} accuracy={accuracy}% rank=#{rank}")
        
        await message.reply_text(
            f"**Your Performance Dashboard**\n\n"
            f"Attempted: {total}\n"
            f"Correct: {correct}\n"
            f"Wrong: {wrong}\n"
            f"Accuracy: {accuracy}%\n"
            f"Rank: #{rank}\n\n"
            f"**Open Dashboard:**\n{dashboard_url}\n\n"
            f"_Bookmark this link -- it's permanent and always up to date._",
            quote=True,
            disable_web_page_preview=True,
        )
    
    except Exception as e:
        print(f"[DASHBOARD] ERROR in /dashboard command for uid={uid}: {e}")
        traceback.print_exc()
        try:
            await message.reply_text(
                "Something went wrong loading your dashboard. Please try again later.\n\n"
                f"_If this persists, contact the bot owner._",
                quote=True,
            )
        except:
            pass


# ── API Helper — verify dashboard secret ────────────────────────────
async def verify_dashboard_secret(secret: str) -> dict | None:
    """Verify a dashboard secret and return user data.
    
    Called by the Next.js dashboard API to authenticate users.
    """
    await _ensure_mongo()
    user = await users_col.find_one({"dashboard_secret": secret})
    if not user:
        return None
    return {
        "user_id": user["user_id"],
        "username": user.get("username", ""),
        "first_name": user.get("first_name", ""),
    }


# ── API Helper — get user stats ────────────────────────────────────
async def get_user_stats(user_id: int) -> dict:
    """Get comprehensive stats for a user. Called by dashboard API."""
    await _ensure_mongo()
    # Basic counts
    total = await answers_col.count_documents({"user_id": user_id})
    correct = await answers_col.count_documents({"user_id": user_id, "is_correct": True})
    wrong = await answers_col.count_documents({"user_id": user_id, "is_correct": False})
    accuracy = round((correct / total) * 100, 1) if total > 0 else 0
    
    # Average response time
    pipeline = [
        {"$match": {"user_id": user_id, "response_time_seconds": {"$ne": None}}},
        {"$group": {"_id": None, "avg_time": {"$avg": "$response_time_seconds"}}},
    ]
    result = await answers_col.aggregate(pipeline).to_list(length=1)
    avg_time = round(result[0]["avg_time"], 1) if result else None
    
    # Current streak
    recent_answers = await answers_col.find(
        {"user_id": user_id},
        {"is_correct": 1, "answered_at": 1}
    ).sort("answered_at", -1).to_list(length=100)
    
    current_streak = 0
    best_streak = 0
    temp_streak = 0
    for ans in recent_answers:
        if ans.get("is_correct"):
            temp_streak += 1
            if temp_streak > best_streak:
                best_streak = temp_streak
        else:
            if current_streak == 0:
                current_streak = temp_streak
            temp_streak = 0
    if current_streak == 0:
        current_streak = temp_streak
    if best_streak == 0:
        best_streak = current_streak
    
    # Rank
    all_users_pipeline = [
        {"$group": {"_id": "$user_id", "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}}, "total": {"$sum": 1}}},
        {"$addFields": {"accuracy": {"$cond": [{"$eq": ["$total", 0]}, 0, {"$multiply": [{"$divide": ["$correct", "$total"]}, 100]}]}}},
        {"$sort": {"correct": -1, "accuracy": -1}},
    ]
    all_users_list = await answers_col.aggregate(all_users_pipeline).to_list(length=None)
    rank = next((i + 1 for i, u in enumerate(all_users_list) if u["_id"] == user_id), len(all_users_list) + 1)
    total_users = len(all_users_list)
    
    return {
        "user_id": user_id,
        "total_attempted": total,
        "correct": correct,
        "wrong": wrong,
        "accuracy": accuracy,
        "avg_response_time": avg_time,
        "current_streak": current_streak,
        "best_streak": best_streak,
        "rank": rank,
        "total_users": total_users,
    }


# ── API Helper — get wrong questions ───────────────────────────────
async def get_wrong_questions(user_id: int) -> list:
    """Get list of wrong questions with explanations for a user."""
    await _ensure_mongo()
    pipeline = [
        {"$match": {"user_id": user_id, "is_correct": False}},
        {"$lookup": {"from": "dashboard_polls", "localField": "hq_id", "foreignField": "hq_id", "as": "poll"}},
        {"$unwind": {"path": "$poll", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "dashboard_question_explanations", "localField": "hq_id", "foreignField": "hq_id", "as": "explanation"}},
        {"$unwind": {"path": "$explanation", "preserveNullAndEmptyArrays": True}},
        {"$sort": {"answered_at": -1}},
    ]
    
    results = await answers_col.aggregate(pipeline).to_list(length=200)
    
    wrong_questions = []
    for r in results:
        poll = r.get("poll", {})
        explanation = r.get("explanation", {})
        
        options = poll.get("options", [])
        selected_idx = r.get("selected_option", -1)
        correct_idx = r.get("correct_option", -1)
        
        wrong_questions.append({
            "hq_id": r["hq_id"],
            "question": poll.get("question", "Unknown"),
            "options": options,
            "your_answer": options[selected_idx] if 0 <= selected_idx < len(options) else f"Option {selected_idx + 1}",
            "correct_answer": options[correct_idx] if 0 <= correct_idx < len(options) else f"Option {correct_idx + 1}",
            "explanation": {
                "text": explanation.get("text"),
                "images": explanation.get("images", []),
                "videos": explanation.get("videos", []),
                "photos": explanation.get("photos", []),
                "kind": explanation.get("kind", "text"),
            } if explanation else None,
            "answered_at": r.get("answered_at"),
        })
    
    return wrong_questions


# ── API Helper — get topic breakdown ───────────────────────────────
async def get_topic_breakdown(user_id: int) -> list:
    """Get accuracy breakdown by topic for a user."""
    await _ensure_mongo()
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$lookup": {"from": "dashboard_polls", "localField": "hq_id", "foreignField": "hq_id", "as": "poll"}},
        {"$unwind": {"path": "$poll", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {"$ifNull": ["$poll.topic", "Uncategorized"]},
            "total": {"$sum": 1},
            "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}},
        }},
        {"$addFields": {
            "accuracy": {"$cond": [{"$eq": ["$total", 0]}, 0, {"$multiply": [{"$divide": ["$correct", "$total"]}, 100]}]},
        }},
        {"$sort": {"total": -1}},
    ]
    
    results = await answers_col.aggregate(pipeline).to_list(length=50)
    return [
        {
            "topic": r["_id"],
            "total": r["total"],
            "correct": r["correct"],
            "wrong": r["total"] - r["correct"],
            "accuracy": round(r["accuracy"], 1),
        }
        for r in results
    ]


# ── API Helper — get leaderboard ──────────────────────────────────
async def get_leaderboard(limit: int = 50) -> list:
    """Get top users by performance."""
    await _ensure_mongo()
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "username": {"$first": "$username"},
            "first_name": {"$first": "$first_name"},
            "total": {"$sum": 1},
            "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}},
        }},
        {"$addFields": {
            "accuracy": {"$cond": [{"$eq": ["$total", 0]}, 0, {"$multiply": [{"$divide": ["$correct", "$total"]}, 100]}]},
        }},
        {"$sort": {"correct": -1, "accuracy": -1}},
        {"$limit": limit},
    ]
    
    results = await answers_col.aggregate(pipeline).to_list(length=limit)
    return [
        {
            "rank": i + 1,
            "user_id": r["_id"],
            "username": r.get("username", ""),
            "first_name": r.get("first_name", ""),
            "total": r["total"],
            "correct": r["correct"],
            "wrong": r["total"] - r["correct"],
            "accuracy": round(r["accuracy"], 1),
        }
        for i, r in enumerate(results)
    ]


# ── API Helper — get accuracy trend ────────────────────────────────
async def get_accuracy_trend(user_id: int, days: int = 30) -> list:
    """Get daily accuracy trend for a user."""
    await _ensure_mongo()
    from datetime import timedelta
    since = datetime.utcnow() - timedelta(days=days)
    
    pipeline = [
        {"$match": {"user_id": user_id, "answered_at": {"$gte": since}}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$answered_at"}},
            "total": {"$sum": 1},
            "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}},
        }},
        {"$addFields": {
            "accuracy": {"$cond": [{"$eq": ["$total", 0]}, 0, {"$multiply": [{"$divide": ["$correct", "$total"]}, 100]}]},
        }},
        {"$sort": {"_id": 1}},
    ]
    
    results = await answers_col.aggregate(pipeline).to_list(length=days)
    return [
        {
            "date": r["_id"],
            "total": r["total"],
            "correct": r["correct"],
            "accuracy": round(r["accuracy"], 1),
        }
        for r in results
    ]


# ── API Helper — get difficulty analysis ───────────────────────────
async def get_difficulty_analysis(user_id: int) -> list:
    """Get accuracy breakdown by difficulty level."""
    await _ensure_mongo()
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$lookup": {"from": "dashboard_polls", "localField": "hq_id", "foreignField": "hq_id", "as": "poll"}},
        {"$unwind": {"path": "$poll", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": {"$ifNull": ["$poll.difficulty", "Unknown"]},
            "total": {"$sum": 1},
            "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}},
        }},
        {"$addFields": {
            "accuracy": {"$cond": [{"$eq": ["$total", 0]}, 0, {"$multiply": [{"$divide": ["$correct", "$total"]}, 100]}]},
        }},
        {"$sort": {"total": -1}},
    ]
    
    results = await answers_col.aggregate(pipeline).to_list(length=10)
    return [
        {
            "difficulty": r["_id"],
            "total": r["total"],
            "correct": r["correct"],
            "wrong": r["total"] - r["correct"],
            "accuracy": round(r["accuracy"], 1),
        }
        for r in results
    ]


# ── API Helper — get weak areas ────────────────────────────────────
async def get_weak_areas(user_id: int, limit: int = 5) -> list:
    """Get weakest topics for a user (lowest accuracy)."""
    topic_breakdown = await get_topic_breakdown(user_id)
    # Sort by accuracy ascending (weakest first)
    topic_breakdown.sort(key=lambda x: x["accuracy"])
    return topic_breakdown[:limit]


# ── Plugin Runner ──────────────────────────────────────────────────
async def run_dashboard_plugin():
    """Initialize the dashboard plugin."""
    print("[DASHBOARD] run_dashboard_plugin() called")
    
    # Initialize MongoDB lazily — don't fail if unavailable
    ok = await _init_mongo()
    if not ok:
        print("[DASHBOARD] WARNING: MongoDB not available. /dashboard command will show error to users.")
        return
    
    # Create indexes
    try:
        await _ensure_indexes()
    except Exception as e:
        print(f"[DASHBOARD] WARNING: Index creation failed (non-fatal): {e}")
    
    print("[DASHBOARD] Plugin initialized — /dashboard command ready")
