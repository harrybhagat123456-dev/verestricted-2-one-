# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import BadRequest, SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, MessageNotModified, FloodWait
import logging
import os
import asyncio
from config import API_HASH, API_ID
from shared_client import app as bot
from utils.func import save_user_session, get_user_data, remove_user_session, save_user_bot, remove_user_bot
from utils.encrypt import ecs, dcs
from plugins.batch import UB, UC
from utils.custom_filters import login_in_progress, set_user_step, get_user_step

from pyrogram.errors import FloodWait


def _is_auth_key_error(error_str):
    """Check if an error indicates the session/auth key is revoked."""
    e = error_str.lower()
    return (
        "authorization key" in e
        or "auth_key_unregistered" in e
        or "key is not registered" in e
        or "session revoked" in e
        or "session expired" in e
    )

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
model = "v3saver Team SPY"

STEP_PHONE = 1
STEP_CODE = 2
STEP_PASSWORD = 3
login_cache = {}

@bot.on_message(filters.command('login'))
async def login_command(client, message):
    print(f"[LOGIN-DBG] Handler entered for user {message.from_user.id}")
    user_id = message.from_user.id
    set_user_step(user_id, STEP_PHONE)
    login_cache.pop(user_id, None)
    try:
        await message.delete()
    except Exception as e:
        print(f"[LOGIN-DBG] delete failed (ok): {e}")
    print(f"[LOGIN-DBG] About to call safe_reply for user {user_id}")
    try:
        status_msg = await client.send_message(
            message.chat.id,
            "Please send your phone number with country code\nExample: `+12345678900`"
        )
        print(f"[LOGIN-DBG] send_message succeeded: {status_msg.id}")
    except Exception as e:
        print(f"[LOGIN-DBG] send_message FAILED: {e}")
        status_msg = None
    login_cache[user_id] = {'status_msg': status_msg}
    
    
@bot.on_message(filters.command("setbot"))
async def set_bot_token(C, m):
    user_id = m.from_user.id
    args = m.text.split(" ", 1)
    if user_id in UB:
        try:
            await UB[user_id].stop()
            if UB.get(user_id, None):
                del UB[user_id]  # Remove from dictionary
                
            try:
                if os.path.exists(f"user_{user_id}.session"):
                    os.remove(f"user_{user_id}.session")
            except Exception:
                pass
            
            print(f"Stopped and removed old bot for user {user_id}")
        except Exception as e:
            print(f"Error stopping old bot for user {user_id}: {e}")
            del UB[user_id]  # Remove from dictionary

    if len(args) < 2:
        await safe_reply(m, "⚠️ Please provide a bot token. Usage: `/setbot token`", quote=True)
        return

    bot_token = args[1].strip()
    await save_user_bot(user_id, bot_token)
    await safe_reply(m, "✅ Bot token saved successfully.", quote=True)
    
    
@bot.on_message(filters.command("rembot"))
async def rem_bot_token(C, m):
    user_id = m.from_user.id
    if user_id in UB:
        try:
            await UB[user_id].stop()
            
            if UB.get(user_id, None):
                del UB[user_id]  # Remove from dictionary # Remove from dictionary
            print(f"Stopped and removed old bot for user {user_id}")
            try:
                if os.path.exists(f"user_{user_id}.session"):
                    os.remove(f"user_{user_id}.session")
            except Exception:
                pass
        except Exception as e:
            print(f"Error stopping old bot for user {user_id}: {e}")
            if UB.get(user_id, None):
                del UB[user_id]  # Remove from dictionary  # Remove from dictionary
            try:
                if os.path.exists(f"user_{user_id}.session"):
                    os.remove(f"user_{user_id}.session")
            except Exception:
                pass
    await remove_user_bot(user_id)
    await safe_reply(m, "✅ Bot token removed successfully.", quote=True)

    
@bot.on_message(login_in_progress & filters.text & filters.private & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'id', 'pay',
    'redeem', 'gencode', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot',
    'mirror', 'mirrorstop', 'mirrorstatus', 'explanlogs']))
async def handle_login_steps(client, message):
    user_id = message.from_user.id
    text = message.text.strip()
    step = get_user_step(user_id)
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f'Could not delete message: {e}')
    status_msg = login_cache[user_id].get('status_msg')
    if not status_msg:
        status_msg = await safe_reply(message, 'Processing...')
        login_cache[user_id]['status_msg'] = status_msg
    try:
        if step == STEP_PHONE:
            if not text.startswith('+'):
                await edit_message_safely(status_msg,
                    '❌ Please provide a valid phone number starting with +')
                return
            await edit_message_safely(status_msg,
                '🔄 Processing phone number...')
            temp_client = Client(f'temp_{user_id}', api_id=API_ID, api_hash
                =API_HASH, device_model=model, in_memory=True, sleep_threshold=0)
            try:
                await temp_client.connect()
                sent_code = await temp_client.send_code(text)
                login_cache[user_id]['phone'] = text
                login_cache[user_id]['phone_code_hash'
                    ] = sent_code.phone_code_hash
                login_cache[user_id]['temp_client'] = temp_client
                set_user_step(user_id, STEP_CODE)
                await edit_message_safely(status_msg,
                    """✅ Verification code sent to your Telegram account.
                    
Please enter the code you received like 1 2 3 4 5 (i.e seperated by space):"""
                    )
            except BadRequest as e:
                await edit_message_safely(status_msg,
                    f"""❌ Error: {str(e)}
Please try again with /login.""")
                await temp_client.disconnect()
                set_user_step(user_id, None)
        elif step == STEP_CODE:
            code = text.replace(' ', '')
            phone = login_cache[user_id]['phone']
            phone_code_hash = login_cache[user_id]['phone_code_hash']
            temp_client = login_cache[user_id]['temp_client']
            try:
                await edit_message_safely(status_msg, '🔄 Verifying code...')
                await temp_client.sign_in(phone, phone_code_hash, code)
                session_string = await temp_client.export_session_string()
                encrypted_session = ecs(session_string)
                await save_user_session(user_id, encrypted_session)
                await temp_client.disconnect()
                temp_status_msg = login_cache[user_id]['status_msg']
                login_cache.pop(user_id, None)
                login_cache[user_id] = {'status_msg': temp_status_msg}
                await edit_message_safely(status_msg,
                    """✅ Logged in successfully!!"""
                    )
                set_user_step(user_id, None)
            except SessionPasswordNeeded:
                set_user_step(user_id, STEP_PASSWORD)
                await edit_message_safely(status_msg,
                    """🔒 Two-step verification is enabled.
Please enter your password:"""
                    )
            except (PhoneCodeInvalid, PhoneCodeExpired) as e:
                await edit_message_safely(status_msg,
                    f'❌ {str(e)}. Please try again with /login.')
                await temp_client.disconnect()
                login_cache.pop(user_id, None)
                set_user_step(user_id, None)
        elif step == STEP_PASSWORD:
            temp_client = login_cache[user_id]['temp_client']
            try:
                await edit_message_safely(status_msg, '🔄 Verifying password...'
                    )
                await temp_client.check_password(text)
                session_string = await temp_client.export_session_string()
                encrypted_session = ecs(session_string)
                await save_user_session(user_id, encrypted_session)
                await temp_client.disconnect()
                temp_status_msg = login_cache[user_id]['status_msg']
                login_cache.pop(user_id, None)
                login_cache[user_id] = {'status_msg': temp_status_msg}
                await edit_message_safely(status_msg,
                    """✅ Logged in successfully!!"""
                    )
                set_user_step(user_id, None)
            except BadRequest as e:
                await edit_message_safely(status_msg,
                    f"""❌ Incorrect password: {str(e)}
Please try again:""")
    except Exception as e:
        logger.error(f'Error in login flow: {str(e)}')
        await edit_message_safely(status_msg,
            f"""❌ An error occurred: {str(e)}
Please try again with /login.""")
        if user_id in login_cache and 'temp_client' in login_cache[user_id]:
            await login_cache[user_id]['temp_client'].disconnect()
        login_cache.pop(user_id, None)
        set_user_step(user_id, None)
async def edit_message_safely(message, text):
    """Helper function to edit message and handle errors"""
    try:
        await safe_edit(message,text)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f'Error editing message: {e}')
        
@bot.on_message(filters.command('cancel'))
async def cancel_command(client, message):
    user_id = message.from_user.id
    # Only handle /cancel if there's an active login in progress.
    # Otherwise, let other /cancel handlers (batch, mirror, settings) handle it.
    if not get_user_step(user_id):
        from pyrogram import ContinuePropagation
        raise ContinuePropagation
    await message.delete()
    status_msg = login_cache.get(user_id, {}).get('status_msg')
    if user_id in login_cache and 'temp_client' in login_cache[user_id]:
        await login_cache[user_id]['temp_client'].disconnect()
    login_cache.pop(user_id, None)
    set_user_step(user_id, None)
    if status_msg:
        await edit_message_safely(status_msg,
            '✅ Login process cancelled. Use /login to start again.')
    else:
        temp_msg = await safe_reply(message,
            '✅ Login process cancelled. Use /login to start again.')
        await temp_msg.delete(5)
        
@bot.on_message(filters.command('logout'))
async def logout_command(client, message):
    user_id = message.from_user.id
    try:
        await message.delete()
    except Exception:
        pass
    status_msg = await safe_reply(message, '🔄 Processing logout request...')
    try:
        session_data = await get_user_data(user_id)
        
        if not session_data or 'session_string' not in session_data:
            await edit_message_safely(status_msg,
                '❌ No active session found for your account.')
            return
        
        encss = session_data['session_string']
        session_string = dcs(encss)
        telegram_logout_ok = False
        temp_client = Client(f'temp_logout_{user_id}', api_id=API_ID,
            api_hash=API_HASH, session_string=session_string, in_memory=True, sleep_threshold=0)
        try:
            # Try to connect with a TIMEOUT — revoked sessions can hang forever
            await asyncio.wait_for(temp_client.connect(), timeout=30)
            # If connected, try to revoke the session on Telegram's side
            try:
                await temp_client.log_out()
                telegram_logout_ok = True
                await edit_message_safely(status_msg,
                    '✅ Telegram session terminated. Removing from database...')
            except Exception as e:
                err_str = str(e)
                if _is_auth_key_error(err_str):
                    # Session already revoked on Telegram's side — that's fine
                    telegram_logout_ok = True
                    await edit_message_safely(status_msg,
                        '⚠️ Session already revoked on Telegram. Removing from database...')
                else:
                    logger.error(f'Error calling log_out: {err_str}')
                    await edit_message_safely(status_msg,
                        f'⚠️ Error terminating Telegram session: {err_str}\nStill removing from database...')
        except asyncio.TimeoutError:
            logger.error(f'Logout: temp_client.connect() timed out (30s) — session likely revoked')
            await edit_message_safely(status_msg,
                '⚠️ Connection to Telegram timed out (session likely revoked). Removing from database...')
        except Exception as e:
            err_str = str(e)
            if _is_auth_key_error(err_str):
                # Session is already revoked — skip Telegram-side logout
                await edit_message_safely(status_msg,
                    '⚠️ Session already revoked on Telegram. Removing from database...')
            else:
                logger.error(f'Error connecting for logout: {err_str}')
                await edit_message_safely(status_msg,
                    f'⚠️ Error connecting to Telegram: {err_str}\nStill removing from database...')
        finally:
            try:
                await asyncio.wait_for(temp_client.disconnect(), timeout=10)
            except Exception:
                pass
        
        # Always remove from database regardless of Telegram-side result
        await remove_user_session(user_id)
        
        # Clean up in-memory state
        if UB.get(user_id, None):
            try:
                await UB[user_id].stop()
            except Exception:
                pass
            del UB[user_id]
        if UC.get(user_id, None):
            del UC[user_id]
        
        # Clean up session files
        try:
            if os.path.exists(f"{user_id}_client.session"):
                os.remove(f"{user_id}_client.session")
        except Exception:
            pass
        try:
            if os.path.exists(f"user_{user_id}.session"):
                os.remove(f"user_{user_id}.session")
        except Exception:
            pass
        
        # Also clear the userbot session in MongoDB (shared_client.py uses this)
        try:
            from utils.session_manager import save_userbot_session
            await save_userbot_session(None)
        except Exception:
            pass
        
        await edit_message_safely(status_msg,
            '✅ **Logged out successfully!**\n\n'
            'Use /login to create a new session.')
    except Exception as e:
        logger.error(f'Error in logout command: {str(e)}')
        # Still try to clean up even on unexpected errors
        try:
            await remove_user_session(user_id)
        except Exception:
            pass
        if UB.get(user_id, None):
            try:
                await UB[user_id].stop()
            except Exception:
                pass
            del UB[user_id]
        if UC.get(user_id, None):
            del UC[user_id]
        try:
            if os.path.exists(f"{user_id}_client.session"):
                os.remove(f"{user_id}_client.session")
        except Exception:
            pass
        try:
            if os.path.exists(f"user_{user_id}.session"):
                os.remove(f"user_{user_id}.session")
        except Exception:
            pass
        await edit_message_safely(status_msg,
            f'❌ Error during logout: {str(e)}\n\nSession data has been removed. Use /login to create a new session.')

