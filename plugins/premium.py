# Copyright (c) 2025 Gagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.
#
# CONVERTED FROM TELETHON TO PYROGRAM — fixes /add not working
# Owner-only commands are restricted to OWNER_ID only.

from shared_client import app
from datetime import timedelta
from config import OWNER_ID
from utils.func import add_premium_user, is_auth_user
from pyrogram.errors import FloodWait
from pyrogram import filters
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton as IK, InlineKeyboardMarkup as IKM
from config import JOIN_LINK as JL, ADMIN_CONTACT as AC
import base64 as spy
from utils.func import a1, a2, a3, a4, a5, a7, a8, a9, a10, a11
from plugins.start import subscribe

from pyrogram.errors import FloodWait

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

async def safe_send(client,  chat_id, text, **kwargs):
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



# ─── /add COMMAND (OWNER ONLY) ─────────────────────────────────────────────────
@app.on_message(filters.command("add") & filters.private)
async def add_premium_handler(client, message):
    """Handle /add command to add premium users — OWNER ONLY."""
    uid = message.from_user.id
    if uid not in OWNER_ID:
        return  # Silently ignore — non-owners shouldn't know this exists
    
    text = message.text.strip()
    parts = text.split(' ')
    if len(parts) != 4:
        await safe_reply(message,
            "**Invalid format.** Use: `/add user_id duration_value duration_unit`\n\n"
            "**Example:** `/add 123456 1 week`"
        )
        return
    try:
        target_user_id = int(parts[1])
        duration_value = int(parts[2])
        duration_unit = parts[3].lower()
        valid_units = ['min', 'hours', 'days', 'weeks', 'month', 'year', 'decades']
        if duration_unit not in valid_units:
            await safe_reply(message,
                f"Invalid duration unit. Choose from: {', '.join(valid_units)}"
            )
            return
        success, result = await add_premium_user(target_user_id, duration_value, duration_unit)
        if success:
            expiry_utc = result
            expiry_ist = expiry_utc + timedelta(hours=5, minutes=30)
            formatted_expiry = expiry_ist.strftime('%d-%b-%Y %I:%M:%S %p')
            await safe_reply(message,
                f"✅ User {target_user_id} added as premium member\n"
                f"Subscription valid until: {formatted_expiry} (IST)"
            )
            try:
                await safe_send(app, target_user_id,
                    f"✅ You have been added as premium member\n"
                    f"**Validity upto**: {formatted_expiry} (IST)"
                )
            except Exception:
                pass
        else:
            await safe_reply(message, f'❌ Failed to add premium user: {result}')
    except ValueError:
        await safe_reply(message, 'Invalid user ID or duration value. Both must be integers.')
    except Exception as e:
        await safe_reply(message, f'Error: {str(e)}')


attr1 = spy.b64encode("photo".encode()).decode()
attr2 = spy.b64encode("file_id".encode()).decode()

@app.on_message(filters.command(spy.b64decode(a5.encode()).decode()))
async def start_handler(client, message):
    subscription_status = await subscribe(client, message)
    if subscription_status == 1:
        return

    b1 = spy.b64decode(a1).decode()
    b2 = int(spy.b64decode(a2).decode())
    b3 = spy.b64decode(a3).decode()
    b4 = spy.b64decode(a4).decode()
    b6 = spy.b64decode(a7).decode()
    b7 = spy.b64decode(a8).decode()
    b8 = spy.b64decode(a9).decode()
    b9 = spy.b64decode(a10).decode()
    b10 = spy.b64decode(a11).decode()

    tm = await getattr(app, b3)(b1, b2)

    pb = getattr(tm, spy.b64decode(attr1.encode()).decode())
    fd = getattr(pb, spy.b64decode(attr2.encode()).decode())

    kb = IKM([
        [IK(b7, url=JL)],
        [IK(b8, url=AC)]
    ])

    await getattr(message, b4)(
        fd,
        caption=b6,
        reply_markup=kb
    )
