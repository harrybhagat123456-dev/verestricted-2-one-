# Copyright (c) 2025 devgajan : https://github.com/devgajanin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.
#
# CONVERTED FROM TELETHON TO PYROGRAM — fixes /settings not working
# All handlers now use Pyrogram (app) instead of Telethon (client)
# because the Telethon auth guard was blocking these commands.

import re
import os
import asyncio
import string
import random
from shared_client import app
from pyrogram import filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait
from config import OWNER_ID
from utils.func import get_user_data_key, save_user_data, users_collection, is_auth_user


# ─── SAFE API HELPERS ──────────────────────────────────────────────────────────
# All reply_text / edit_text / send_message calls in settings handlers are
# protected against FloodWait. During a large FloodWait we CANNOT send any
# messages at all, so we silently fail and log instead of crashing.

async def safe_reply(message, text, **kwargs):
    """reply_text with FloodWait protection — silently fails during FloodWait."""
    try:
        return await message.reply_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[SETTINGS-FLOOD] reply_text FloodWait {wait}s — suppressed (cannot reply)")
        return None
    except Exception as e:
        print(f"[SETTINGS-ERR] reply_text failed: {e}")
        return None

async def safe_edit(message, text, **kwargs):
    """edit_text with FloodWait protection — silently fails during FloodWait."""
    try:
        return await message.edit_text(text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[SETTINGS-FLOOD] edit_text FloodWait {wait}s — suppressed (cannot edit)")
        return None
    except Exception as e:
        print(f"[SETTINGS-ERR] edit_text failed: {e}")
        return None

async def safe_send(client, chat_id, text, **kwargs):
    """send_message with FloodWait protection — silently fails during FloodWait."""
    try:
        return await client.send_message(chat_id, text, **kwargs)
    except FloodWait as e:
        wait = e.value if hasattr(e, 'value') else '?'
        print(f"[SETTINGS-FLOOD] send_message FloodWait {wait}s — suppressed (cannot send)")
        return None
    except Exception as e:
        print(f"[SETTINGS-ERR] send_message failed: {e}")
        return None

VIDEO_EXTENSIONS = {
    'mp4', 'mkv', 'avi', 'mov', 'wmv', 'flv', 'webm',
    'mpeg', 'mpg', '3gp'
}
SET_PIC = 'settings.jpg'
MESS = 'Customize settings for your files...'

active_conversations = {}

# ─── /settings COMMAND ─────────────────────────────────────────────────────────
@app.on_message(filters.command("settings") & filters.private)
async def settings_command(client, message):
    """Show settings menu — available to owner and auth users only."""
    uid = message.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return  # Silently ignore unauthorized users
    await send_settings_message(client, message.chat.id, uid)

async def send_settings_message(client, chat_id, user_id):
    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('📝 Set Chat ID', callback_data='setchat'),
            InlineKeyboardButton('🏷️ Set Rename Tag', callback_data='setrename')
        ],
        [
            InlineKeyboardButton('📋 Set Caption', callback_data='setcaption'),
            InlineKeyboardButton('🔄 Replace Words', callback_data='setreplacement')
        ],
        [
            InlineKeyboardButton('🗑️ Remove Words', callback_data='delete'),
            InlineKeyboardButton('🔄 Reset Settings', callback_data='reset')
        ],
        [
            InlineKeyboardButton('🔑 Session Login', callback_data='addsession'),
            InlineKeyboardButton('🚪 Logout', callback_data='logout')
        ],
        [
            InlineKeyboardButton('🖼️ Set Thumbnail', callback_data='setthumb'),
            InlineKeyboardButton('❌ Remove Thumbnail', callback_data='remthumb')
        ],
        [
            InlineKeyboardButton('🆘 Report Errors', url='https://t.me/team_spy_pro')
        ]
    ])
    await safe_send(client, chat_id, MESS, reply_markup=buttons)

# ─── CALLBACK QUERY HANDLER ────────────────────────────────────────────────────
SETTINGS_CALLBACKS = {'setchat', 'setrename', 'setcaption', 'setreplacement', 'addsession', 'delete', 'logout', 'reset', 'remthumb', 'setthumb'}

@app.on_callback_query(filters.regex("^(setchat|setrename|setcaption|setreplacement|addsession|delete|logout|reset|remthumb|setthumb)$"))
async def callback_query_handler(client, callback_query):
    """Handle settings button callbacks — owner + auth users only."""
    uid = callback_query.from_user.id
    if uid not in OWNER_ID and not await is_auth_user(uid):
        await callback_query.answer("Not authorized.", show_alert=True)
        return
    
    data = callback_query.data
    
    callback_actions = {
        'setchat': {
            'type': 'setchat',
            'message': """Send me the ID of that chat(with -100 prefix): 
__👉 **Note:** if you are using custom bot then your bot should be admin that chat if not then this bot should be admin.__
👉 __If you want to upload in topic group and in specific topic then pass chat id as **-100CHANNELID/TOPIC_ID** for example: **-1004783898/12**__"""
        },
        'setrename': {
            'type': 'setrename',
            'message': 'Send me the rename tag:'
        },
        'setcaption': {
            'type': 'setcaption',
            'message': 'Send me the caption:'
        },
        'setreplacement': {
            'type': 'setreplacement',
            'message': "Send me the replacement words in the format: 'WORD(s)' 'REPLACEWORD'"
        },
        'addsession': {
            'type': 'addsession',
            'message': 'Send Pyrogram V2 session string:'
        },
        'delete': {
            'type': 'deleteword',
            'message': 'Send words separated by space to delete them from caption/filename...'
        },
        'setthumb': {
            'type': 'setthumb',
            'message': 'Please send the photo you want to set as the thumbnail.'
        }
    }
    
    if data in callback_actions:
        action = callback_actions[data]
        await start_conversation(client, callback_query, uid, action['type'], action['message'])
    elif data == 'logout':
        result = await users_collection.update_one(
            {'user_id': uid},
            {'$unset': {'session_string': ''}}
        )
        if result.modified_count > 0:
            await safe_edit(callback_query.message, 'Logged out and deleted session successfully.')
        else:
            await safe_edit(callback_query.message, 'You are not logged in.')
    elif data == 'reset':
        try:
            await users_collection.update_one(
                {'user_id': uid},
                {'$unset': {
                    'delete_words': '',
                    'replacement_words': '',
                    'rename_tag': '',
                    'caption': '',
                    'chat_id': ''
                }}
            )
            thumbnail_path = f'{uid}.jpg'
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            await safe_edit(callback_query.message, '✅ All settings reset successfully. To logout, click /logout')
        except Exception as e:
            await safe_edit(callback_query.message, f'Error resetting settings: {e}')
    elif data == 'remthumb':
        try:
            os.remove(f'{uid}.jpg')
            await safe_edit(callback_query.message, 'Thumbnail removed successfully!')
        except FileNotFoundError:
            await safe_edit(callback_query.message, 'No thumbnail found to remove.')

async def start_conversation(client, callback_query, user_id, conv_type, prompt_message):
    if user_id in active_conversations:
        await safe_edit(callback_query.message, 'Previous conversation cancelled. Starting new one.')
        # Small delay before sending new prompt
        await asyncio.sleep(0.5)
    
    msg = await safe_reply(callback_query.message, f'{prompt_message}\n\n(Send /cancel to cancel this operation)')
    if msg:
        active_conversations[user_id] = {'type': conv_type, 'message_id': msg.id}
    else:
        # FloodWait — couldn't send prompt, but still track conversation type
        # so user's next message gets processed (they may type after FloodWait clears)
        active_conversations[user_id] = {'type': conv_type, 'message_id': None}

# ─── /cancel COMMAND ───────────────────────────────────────────────────────────
@app.on_message(filters.command("cancel") & filters.private)
async def cancel_conversation(client, message):
    uid = message.from_user.id
    if uid in active_conversations:
        await safe_reply(message, 'Cancelled enjoy baby...')
        del active_conversations[uid]
    else:
        # No settings conversation — pass to other /cancel handlers (batch, login)
        from pyrogram import ContinuePropagation
        raise ContinuePropagation

# ─── CONVERSATION INPUT HANDLER ────────────────────────────────────────────────
@app.on_message(filters.private & filters.text & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id',
    'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot',
    'auth', 'unauth', 'authusers', 'logs', 'fetch', 'cancelfetch', 'fetchmaps', 'clearfetch', 'answerkey',
    'clearbatch', 'status', 'viewfetchmaps', 'viewanswerkey', 'clearanswerkey', 'settings', 'help', 'terms', 'plan',
    'auto', 'autooff', 'cancelauto', 'linkexplan', 'explans', 'transfer', 'rem', 'dl', 'adl',
    'mirror', 'mirrorstop', 'mirrorstatus', 'explanlogs'
]), group=1)
async def handle_conversation_input(client, message):
    """Handle text input during an active settings conversation."""
    uid = message.from_user.id
    if uid not in active_conversations:
        from pyrogram import ContinuePropagation
        raise ContinuePropagation
    
    # Auth check
    if uid not in OWNER_ID and not await is_auth_user(uid):
        return
    
    conv_type = active_conversations[uid]['type']
    
    handlers = {
        'setchat': handle_setchat,
        'setrename': handle_setrename,
        'setcaption': handle_setcaption,
        'setreplacement': handle_setreplacement,
        'addsession': handle_addsession,
        'deleteword': handle_deleteword,
        'setthumb': handle_deleteword,  # Text input for setthumb is invalid, will be handled below
    }
    
    if conv_type == 'setthumb':
        # Thumbnail requires a photo, not text
        await safe_reply(message, '❌ Please send a photo, not text. Operation cancelled.')
        if uid in active_conversations:
            del active_conversations[uid]
        return
    
    if conv_type in handlers:
        await handlers[conv_type](client, message, uid)
    
    if uid in active_conversations:
        del active_conversations[uid]

# ─── PHOTO HANDLER FOR THUMBNAIL ───────────────────────────────────────────────
@app.on_message(filters.private & filters.photo, group=1)
async def handle_photo_input(client, message):
    """Handle photo input for setthumb conversation."""
    uid = message.from_user.id
    if uid not in active_conversations:
        return
    
    conv_type = active_conversations[uid]['type']
    if conv_type != 'setthumb':
        return
    
    await handle_setthumb(client, message, uid)
    
    if uid in active_conversations:
        del active_conversations[uid]

# ─── INDIVIDUAL SETTING HANDLERS ───────────────────────────────────────────────

async def handle_setchat(client, message, user_id):
    try:
        chat_id = message.text.strip()
        await save_user_data(user_id, 'chat_id', chat_id)
        await safe_reply(message, '✅ Chat ID set successfully!')
    except Exception as e:
        await safe_reply(message, f'❌ Error setting chat ID: {e}')

async def handle_setrename(client, message, user_id):
    rename_tag = message.text.strip()
    await save_user_data(user_id, 'rename_tag', rename_tag)
    await safe_reply(message, f'✅ Rename tag set to: {rename_tag}')

async def handle_setcaption(client, message, user_id):
    caption = message.text
    await save_user_data(user_id, 'caption', caption)
    await safe_reply(message, '✅ Caption set successfully!')

async def handle_setreplacement(client, message, user_id):
    match = re.match("'(.+)' '(.+)'", message.text)
    if not match:
        await safe_reply(message, "❌ Invalid format. Usage: 'WORD(s)' 'REPLACEWORD'")
    else:
        word, replace_word = match.groups()
        delete_words = await get_user_data_key(user_id, 'delete_words', [])
        if word in delete_words:
            await safe_reply(message, f"❌ The word '{word}' is in the delete list and cannot be replaced.")
        else:
            replacements = await get_user_data_key(user_id, 'replacement_words', {})
            replacements[word] = replace_word
            await save_user_data(user_id, 'replacement_words', replacements)
            await safe_reply(message, f"✅ Replacement saved: '{word}' will be replaced with '{replace_word}'")

async def handle_addsession(client, message, user_id):
    session_string = message.text.strip()
    await save_user_data(user_id, 'session_string', session_string)
    await safe_reply(message, '✅ Session string added successfully!')

async def handle_deleteword(client, message, user_id):
    words_to_delete = message.text.split()
    delete_words = await get_user_data_key(user_id, 'delete_words', [])
    delete_words = list(set(delete_words + words_to_delete))
    await save_user_data(user_id, 'delete_words', delete_words)
    await safe_reply(message, f"✅ Words added to delete list: {', '.join(words_to_delete)}")

async def handle_setthumb(client, message, user_id):
    if message.photo:
        temp_path = await client.download_media(message)
        try:
            thumb_path = f'{user_id}.jpg'
            if os.path.exists(thumb_path):
                os.remove(thumb_path)
            os.rename(temp_path, thumb_path)
            await safe_reply(message, '✅ Thumbnail saved successfully!')
        except Exception as e:
            await safe_reply(message, f'❌ Error saving thumbnail: {e}')
    else:
        await safe_reply(message, '❌ Please send a photo. Operation cancelled.')

def generate_random_name(length=7):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def sanitize(filename):
    """Remove invalid filesystem characters and emojis from filename.
    
    Strips characters that can cause issues with file operations,
    Telegram API, or system tools (cv2, ffmpeg):
    - Filesystem-invalid chars: < > : " / \\ | ? * '
    - Emoji and non-BMP Unicode characters
    - Control characters
    
    IMPORTANT: Preserves underscores, hyphens, and original spacing.
    """
    # Remove filesystem-invalid characters (replace with underscore)
    cleaned = re.sub(r'[<>:"/\\|?*\']', '_', filename)
    # Remove emoji and other non-text Unicode (keep letters, digits, underscores, hyphens, dots, spaces, common punctuation)
    # \w already includes letters, digits, and underscore
    # This regex removes: emoji, special symbols, zero-width chars, etc.
    cleaned = re.sub(r'[^\w\s\-\.\(\)\[\]\{\}&!,;=\+\@\#\$\%\^\~`\u0900-\u097F\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]', '', cleaned)
    # Collapse multiple SPACES only (not underscores or hyphens)
    cleaned = re.sub(r' {2,}', ' ', cleaned)
    return cleaned.strip(" .")[:255]


async def rename_file(file, sender, edit, original_name=None):
    """Compute a display filename based on user settings.
    
    file: The on-disk file path (may be a safe numeric name like '1778744725.mp4')
    sender: User ID for looking up rename settings
    edit: Progress message (unused in computation)
    original_name: The original filename from Telegram (e.g. m.video.file_name).
                   If provided, used as the base for rename instead of the disk filename.
    
    Returns: The display filename (basename only, e.g. 'My Video [Tag].mp4').
             IMPORTANT: The on-disk file is NOT renamed — the caller should pass the
             returned name as file_name= to Pyrogram upload methods to set the
             display name in Telegram without touching the filesystem.
             This avoids emoji/special-char issues with cv2/ffmpeg.
    """
    try:
        delete_words = await get_user_data_key(sender, 'delete_words', []) or []
        custom_rename_tag = await get_user_data_key(sender, 'rename_tag', '') or ''
        replacements = await get_user_data_key(sender, 'replacement_words', {}) or {}
        
        # If original_name is provided, use it as the base for renaming
        # Otherwise fall back to the on-disk filename
        name_source = original_name if original_name else os.path.basename(file)
        
        last_dot_index = name_source.rfind('.')
        if last_dot_index != -1 and last_dot_index != 0:
            ggn_ext = name_source[last_dot_index + 1:]
            name_without_ext = name_source[:last_dot_index]
            if ggn_ext.isalpha() and len(ggn_ext) <= 9:
                if ggn_ext.lower() in VIDEO_EXTENSIONS:
                    original_file_name = name_without_ext
                    file_extension = 'mp4'
                else:
                    original_file_name = name_without_ext
                    file_extension = ggn_ext
            else:
                # Extension is numeric (e.g. timestamp.123456) — treat as mp4
                original_file_name = name_without_ext
                file_extension = 'mp4'
        else:
            original_file_name = name_source
            file_extension = 'mp4'
        
        for word in delete_words:
            original_file_name = original_file_name.replace(word, '')
        
        for word, replace_word in replacements.items():
            original_file_name = original_file_name.replace(word, replace_word)
        
        # Always remove "backup" from filename (case-insensitive)
        original_file_name = re.sub(r'(?i)\bback\s*up\b', '', original_file_name)
        # Clean up any double spaces or trailing/leading spaces left by removals
        original_file_name = re.sub(r'\s+', ' ', original_file_name).strip()
        
        # Sanitize the filename part (remove invalid chars)
        original_file_name = sanitize(original_file_name)
        
        # Build new filename — only add rename tag if it's non-empty
        if custom_rename_tag:
            new_basename = f'{original_file_name} {custom_rename_tag}.{file_extension}'
        else:
            new_basename = f'{original_file_name}.{file_extension}'
        
        print(f"[RENAME] display_name='{new_basename}' (disk path unchanged: '{file}')")
        return new_basename
    except Exception as e:
        print(f"Rename error: {e}")
        import traceback
        traceback.print_exc()
        return os.path.basename(file)
