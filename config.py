# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

import os
from dotenv import load_dotenv
load_dotenv()

# ════════════════════════════════════════════════════════════════════════════════
# ░ CONFIGURATION SETTINGS
# ════════════════════════════════════════════════════════════════════════════════

# VPS --- FILL COOKIES 🍪 in """ ... """ 
INST_COOKIES = """
# write up here insta cookies
"""

YTUB_COOKIES = """
# write here yt cookies
"""

# ─── BOT / DATABASE CONFIG ──────────────────────────────────────────────────────
API_ID       = os.getenv("API_ID", "")
API_HASH     = os.getenv("API_HASH", "")
BOT_TOKEN    = os.getenv("BOT_TOKEN", "").strip()  # Strip whitespace — common copy-paste issue
MONGO_DB     = os.getenv("MONGO_DB", "")
DB_NAME      = os.getenv("DB_NAME", "telegram_downloader")

# Validate BOT_TOKEN format: must be "123456:ABC-DEF..." (numeric_id:hash)
if BOT_TOKEN:
    _bt_parts = BOT_TOKEN.split(':', 1)
    if len(_bt_parts) != 2 or not _bt_parts[0].isdigit():
        print(f"[CONFIG] WARNING: BOT_TOKEN format looks invalid! Expected '123456:ABC-DEF...', got '{BOT_TOKEN[:15]}...'")
        print(f"[CONFIG] This will cause ACCESS_TOKEN_INVALID errors. Please check your BOT_TOKEN env var.")
else:
    print("[CONFIG] WARNING: BOT_TOKEN is empty! Bot cannot start without a valid token.")

def refresh_bot_token():
    """Re-read BOT_TOKEN from environment (picks up changes without restart).
    
    On Heroku/Render, when you update an env var, a NEW dyno is spawned.
    But this function allows the retry loop to pick up a token change
    if the process is still running when the var changes (rare but possible).
    """
    global BOT_TOKEN
    new_token = os.getenv("BOT_TOKEN", "").strip()
    if new_token != BOT_TOKEN:
        print(f"[CONFIG] BOT_TOKEN changed! Old={BOT_TOKEN[:10]}... New={new_token[:10]}...")
        BOT_TOKEN = new_token
        return True  # Token changed
    return False  # No change

# ─── OWNER / CONTROL SETTINGS ───────────────────────────────────────────────────
# Parse OWNER_ID as a list of integers (space-separated).
# CRITICAL: If this is empty or unparseable, the bot's privacy guard will block
# EVERY private message — including the owner's — making the bot appear dead.
# We parse defensively and print a LOUD warning if something is wrong.
_owner_raw = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID = [int(x) for x in _owner_raw.split()] if _owner_raw else []
except ValueError as _owner_err:
    print(f"[CONFIG] ❌ FATAL: OWNER_ID contains non-numeric value: {_owner_err}")
    print(f"[CONFIG] ❌ OWNER_ID must be space-separated integers (e.g. '123456789 987654321')")
    print(f"[CONFIG] ❌ Got: '{_owner_raw}'")
    print(f"[CONFIG] ❌ Bot will start but BLOCK ALL private messages until fixed!")
    OWNER_ID = []

if not OWNER_ID:
    print("[CONFIG] ❌❌❌ FATAL: OWNER_ID is EMPTY!")
    print("[CONFIG] ❌ The bot's privacy guard will block EVERY private message.")
    print("[CONFIG] ❌ Set OWNER_ID env var on Render to your Telegram user ID (space-separated for multiple).")
    print("[CONFIG] ❌ Get your ID by messaging @userinfobot on Telegram.")
    print("[CONFIG] ❌ Essential commands (/ping, /diag, /start, /login) will still work to help you diagnose.")
else:
    print(f"[CONFIG] ✅ OWNER_ID loaded: {OWNER_ID} ({len(OWNER_ID)} owner(s))")

STRING       = os.getenv("STRING", None) or None  # optional session string
LOG_GROUP    = int(os.getenv("LOG_GROUP") or "0")  # Safe: empty string → 0
FORCE_SUB    = int(os.getenv("FORCE_SUB") or "0")  # Safe: empty string → 0

# ─── SECURITY KEYS ──────────────────────────────────────────────────────────────
MASTER_KEY   = os.getenv("MASTER_KEY", "gK8HzLfT9QpViJcYeB5wRa3DmN7P2xUq")  # session encryption
IV_KEY       = os.getenv("IV_KEY", "s7Yx5CpVmE3F")  # decryption key

# ─── COOKIES HANDLING ───────────────────────────────────────────────────────────
YT_COOKIES   = os.getenv("YT_COOKIES", YTUB_COOKIES)
INSTA_COOKIES = os.getenv("INSTA_COOKIES", INST_COOKIES)

# ─── USAGE LIMITS ───────────────────────────────────────────────────────────────
FREEMIUM_LIMIT = int(os.getenv("FREEMIUM_LIMIT") or "0")
PREMIUM_LIMIT  = int(os.getenv("PREMIUM_LIMIT") or "5000")

# ─── UI / LINKS ─────────────────────────────────────────────────────────────────
JOIN_LINK     = os.getenv("JOIN_LINK", "https://t.me/team_spy_pro")
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "https://t.me/username_of_admin")

# ════════════════════════════════════════════════════════════════════════════════
# ░ PREMIUM PLANS CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

P0 = {
    "d": {
        "s": int(os.getenv("PLAN_D_S") or 1),
        "du": int(os.getenv("PLAN_D_DU") or 1),
        "u": os.getenv("PLAN_D_U", "days"),
        "l": os.getenv("PLAN_D_L", "Daily"),
    },
    "w": {
        "s": int(os.getenv("PLAN_W_S") or 3),
        "du": int(os.getenv("PLAN_W_DU") or 1),
        "u": os.getenv("PLAN_W_U", "weeks"),
        "l": os.getenv("PLAN_W_L", "Weekly"),
    },
    "m": {
        "s": int(os.getenv("PLAN_M_S") or 5),
        "du": int(os.getenv("PLAN_M_DU") or 1),
        "u": os.getenv("PLAN_M_U", "month"),
        "l": os.getenv("PLAN_M_L", "Monthly"),
    },
}

# ════════════════════════════════════════════════════════════════════════════════
# ░ DEVGAGAN
# ════════════════════════════════════════════════════════════════════════════════
