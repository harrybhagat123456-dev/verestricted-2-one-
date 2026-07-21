# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

"""
Persistent Session Manager - Stores Telegram session strings in MongoDB
so that sessions survive container restarts/redeploys on Render.

Without this, every container restart wipes the .session files,
forcing re-authentication with Telegram → FloodWait → crash.
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
sessions_collection = db["bot_sessions"]

SESSION_KEY = "main_bot_sessions"

async def save_telethon_session(session_string):
    """Save or clear Telethon session string in MongoDB.
    Pass None to delete the session (e.g. when invalidated by two-IP conflict).
    """
    try:
        if session_string is None:
            # Delete the session field — session is permanently invalidated
            await sessions_collection.update_one(
                {"key": SESSION_KEY},
                {"$unset": {"telethon_session": ""}},
            )
            print("Telethon session CLEARED from MongoDB (was invalidated).")
        else:
            await sessions_collection.update_one(
                {"key": SESSION_KEY},
                {"$set": {"telethon_session": session_string}},
                upsert=True
            )
            print("Telethon session saved to MongoDB.")
    except Exception as e:
        print(f"Error saving Telethon session: {e}")

async def save_pyrogram_session(session_string):
    """Save Pyrogram bot session string to MongoDB (DEPRECATED - bot now always uses bot_token)"""
    # No longer saving pyrogram sessions - bot always authenticates with bot_token
    # to avoid ImportBotAuthorizationRequest FloodWait errors
    pass

async def save_userbot_session(session_string):
    """Save or clear userbot session string in MongoDB.
    Pass None to delete the session (e.g. when invalidated by two-IP conflict).
    """
    try:
        if session_string is None:
            await sessions_collection.update_one(
                {"key": SESSION_KEY},
                {"$unset": {"userbot_session": ""}},
            )
            print("Userbot session CLEARED from MongoDB (was invalidated).")
        else:
            await sessions_collection.update_one(
                {"key": SESSION_KEY},
                {"$set": {"userbot_session": session_string}},
                upsert=True
            )
            print("Userbot session saved to MongoDB.")
    except Exception as e:
        print(f"Error saving userbot session: {e}")

async def get_telethon_session():
    """Load Telethon session string from MongoDB"""
    try:
        doc = await sessions_collection.find_one({"key": SESSION_KEY})
        if doc and "telethon_session" in doc:
            print("Loaded Telethon session from MongoDB.")
            return doc["telethon_session"]
    except Exception as e:
        print(f"Error loading Telethon session: {e}")
    return None

async def get_pyrogram_session():
    """Load Pyrogram bot session string from MongoDB (DEPRECATED - bot now always uses bot_token)"""
    # No longer loading pyrogram sessions - bot always authenticates with bot_token
    # to avoid ImportBotAuthorizationRequest FloodWait errors
    return None

async def get_userbot_session():
    """Load userbot session string from MongoDB"""
    try:
        doc = await sessions_collection.find_one({"key": SESSION_KEY})
        if doc and "userbot_session" in doc:
            print("Loaded Userbot session from MongoDB.")
            return doc["userbot_session"]
    except Exception as e:
        print(f"Error loading userbot session: {e}")
    return None
