# Copyright (c) 2025 devgajan : https://github.com/devgajanin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.
#
# CONVERTED FROM TELETHON TO PYROGRAM — fixes /status, /transfer, /rem not working
# Owner-only commands are restricted to OWNER_ID only.

from datetime import timedelta, datetime
from shared_client import app
from pyrogram.errors import FloodWait
from pyrogram import filters
from utils.func import (
    get_premium_details, get_user_data,
    premium_users_collection, is_premium_user, is_auth_user
)
from config import OWNER_ID
import logging
logging.basicConfig(format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('teamspy')


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


# ─── /status COMMAND ───────────────────────────────────────────────────────────
@app.on_message(filters.command("status") & filters.private)
async def status_handler(client, message):
    """Handle /status command — shows batch status if active, otherwise login/premium status."""
    uid = message.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return  # Silently ignore unauthorized users
    
    # Check if batch is active first — defer to batch.py's logic
    from plugins.batch import is_user_active, get_batch_info, scheduler as batch_scheduler
    
    if batch_scheduler.is_paused(uid):
        remaining = batch_scheduler.time_remaining(uid)
        mins, secs = divmod(remaining, 60)
        job = batch_scheduler.get_job(uid)
        wait_secs = job.wait_seconds if job else 0
        wait_mins, wait_secs_part = divmod(wait_secs, 60)
        duration_str = f"{wait_mins}m {wait_secs_part}s" if wait_mins > 0 else f"{wait_secs}s"
        
        await safe_reply(message,
            f'⏳ **Batch paused — Flood Wait**\n\n'
            f'🕐 Original wait: **{duration_str}**\n'
            f'⏳ Remaining: **{mins}m {secs}s**\n\n'
            f'✅ Will resume **automatically** when the wait clears.\n'
            f'🛑 Use /stop to cancel permanently.'
        )
        return
    
    if is_user_active(uid):
        batch_info = get_batch_info(uid)
        if batch_info:
            total = batch_info.get('total', '?')
            current = batch_info.get('current', 0)
            success = batch_info.get('success', 0)
            await safe_reply(message,
                f'📦 **Batch in progress**\n\n'
                f'✅ Done: **{current}**/{total}\n'
                f'📊 Success: **{success}**\n\n'
                f'🛑 Use /stop to cancel.'
            )
        else:
            await safe_reply(message, '📦 Batch is running...')
        return
    
    # No active batch — show login/premium status
    user_data = await get_user_data(uid)
    
    session_active = False
    bot_active = False
    
    if user_data and "session_string" in user_data:
        session_active = True
    
    if user_data and "bot_token" in user_data:
        bot_active = True
    
    # Premium status check
    if uid in OWNER_ID:
        premium_status = "👑 Owner & Super Prime — Unlimited Access"
    elif await is_auth_user(uid):
        premium_status = "🔑 Authorized User — Full Access"
    else:
        premium_status = "❌ Not a premium member"
        premium_details = await get_premium_details(uid)
        if premium_details:
            expiry_utc = premium_details["subscription_end"]
            expiry_ist = expiry_utc + timedelta(hours=5, minutes=30)
            formatted_expiry = expiry_ist.strftime("%d-%b-%Y %I:%M:%S %p")
            premium_status = f"✅ Premium until {formatted_expiry} (IST)"
    
    await safe_reply(message,
        "**Your current status:**\n\n"
        f"**Login Status:** {'✅ Active' if session_active else '❌ Inactive'}\n"
        f"**Premium:** {premium_status}"
    )


# ─── /transfer COMMAND ─────────────────────────────────────────────────────────
@app.on_message(filters.command("transfer") & filters.private)
async def transfer_premium_handler(client, message):
    """Transfer premium subscription — premium users only."""
    uid = message.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return  # Silently ignore unauthorized users
    
    if not await is_premium_user(uid):
        await safe_reply(message, "❌ You don't have a premium subscription to transfer.")
        return
    
    args = message.text.split()
    if len(args) != 2:
        await safe_reply(message, '**Usage:** `/transfer user_id`\n\n**Example:** `/transfer 123456789`')
        return
    
    try:
        target_user_id = int(args[1])
    except ValueError:
        await safe_reply(message, '❌ Invalid user ID. Please provide a valid numeric user ID.')
        return
    
    if target_user_id == uid:
        await safe_reply(message, '❌ You cannot transfer premium to yourself.')
        return
    
    if await is_premium_user(target_user_id):
        await safe_reply(message, '❌ The target user already has a premium subscription.')
        return
    
    try:
        premium_details = await get_premium_details(uid)
        if not premium_details:
            await safe_reply(message, '❌ Error retrieving your premium details.')
            return
        
        target_name = 'Unknown'
        try:
            target_entity = await app.get_chat(target_user_id)
            target_name = getattr(target_entity, 'first_name', None) or getattr(target_entity, 'title', None) or 'Unknown'
        except Exception as e:
            logger.warning(f'Could not get target user name: {e}')
        
        now = datetime.now()
        expiry_date = premium_details['subscription_end']
        await premium_users_collection.update_one({'user_id': target_user_id},
            {'$set': {'user_id': target_user_id,
            'subscription_start': now, 'subscription_end': expiry_date,
            'expireAt': expiry_date, 'transferred_from': uid,
            'transferred_from_name': message.from_user.first_name or 'Unknown'}},
            upsert=True)
        await premium_users_collection.delete_one({'user_id': uid})
        expiry_ist = expiry_date + timedelta(hours=5, minutes=30)
        formatted_expiry = expiry_ist.strftime('%d-%b-%Y %I:%M:%S %p')
        
        await safe_reply(message,
            f'✅ Premium subscription successfully transferred to {target_name} ({target_user_id}). '
            f'Your premium access has been removed.'
        )
        try:
            await safe_send(app, target_user_id,
                f'🎁 You have received a premium subscription transfer from '
                f'{message.from_user.first_name or "Unknown"} ({uid}). '
                f'Your premium is valid until {formatted_expiry} (IST).'
            )
        except Exception as e:
            logger.error(f'Could not notify target user {target_user_id}: {e}')
        
        try:
            owner_id = OWNER_ID[0] if isinstance(OWNER_ID, list) else OWNER_ID
            await safe_send(app, owner_id,
                f'♻️ Premium Transfer: {message.from_user.first_name or "Unknown"} ({uid}) '
                f'has transferred their premium to {target_name} ({target_user_id}). '
                f'Expiry: {formatted_expiry}'
            )
        except Exception as e:
            logger.error(f'Could not notify owner about premium transfer: {e}')
        return
    except Exception as e:
        logger.error(f'Error transferring premium from {uid} to {target_user_id}: {e}')
        await safe_reply(message, f'❌ Error transferring premium: {str(e)}')
        return


# ─── /rem COMMAND (OWNER ONLY) ─────────────────────────────────────────────────
@app.on_message(filters.command("rem") & filters.private)
async def remove_premium_handler(client, message):
    """Remove premium subscription — OWNER ONLY."""
    uid = message.from_user.id
    if uid not in OWNER_ID:
        return  # Silently ignore — non-owners shouldn't know this exists
    
    args = message.text.split()
    if len(args) != 2:
        await safe_reply(message, '**Usage:** `/rem user_id`\n\n**Example:** `/rem 123456789`')
        return
    
    try:
        target_user_id = int(args[1])
    except ValueError:
        await safe_reply(message, '❌ Invalid user ID. Please provide a valid numeric user ID.')
        return
    
    if not await is_premium_user(target_user_id):
        await safe_reply(message, f'❌ User {target_user_id} does not have a premium subscription.')
        return
    
    try:
        target_name = 'Unknown'
        try:
            target_entity = await app.get_chat(target_user_id)
            target_name = getattr(target_entity, 'first_name', None) or getattr(target_entity, 'title', None) or 'Unknown'
        except Exception as e:
            logger.warning(f'Could not get target user name: {e}')
        
        result = await premium_users_collection.delete_one({'user_id': target_user_id})
        if result.deleted_count > 0:
            await safe_reply(message,
                f'✅ Premium subscription successfully removed from {target_name} ({target_user_id}).'
            )
            try:
                await safe_send(app, target_user_id,
                    '⚠️ Your premium subscription has been removed by the administrator.'
                )
            except Exception as e:
                logger.error(f'Could not notify user {target_user_id} about premium removal: {e}')
        else:
            await safe_reply(message, f'❌ Failed to remove premium from user {target_user_id}.')
        return
    except Exception as e:
        logger.error(f'Error removing premium from {target_user_id}: {e}')
        await safe_reply(message, f'❌ Error removing premium: {str(e)}')
        return
