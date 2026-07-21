# Save Restricted Content Bot v3

A Telegram bot that extracts restricted content from Telegram channels/groups and downloads media from YouTube, Instagram, and 100+ other sites via yt-dlp.

## Stack
- **Python 3.12** — async, Pyrogram (pyrofork) + Telethon dual-client
- **MongoDB** — user data, sessions, clone/batch resume state (Motor async driver)
- **yt-dlp** — media downloads from external sites

## Required secrets (set via Replit Secrets)
| Secret | Description |
|---|---|
| `API_ID` | Telegram API ID from my.telegram.org |
| `API_HASH` | Telegram API hash from my.telegram.org |
| `BOT_TOKEN` | Bot token from @BotFather |
| `MONGO_DB` | MongoDB connection string (e.g. `mongodb+srv://...`) |
| `OWNER_ID` | Your Telegram user ID (space-separated if multiple) |
| `STRING` | Optional Pyrogram session string for user client (/login flow is the alternative) |

## How to run
The workflow `Start application` runs `python main.py`. Start it after setting all required secrets.

## Key plugins
| Plugin | Purpose |
|---|---|
| `plugins/batch.py` | Core batch download/forward engine |
| `plugins/channel_clone.py` | Clone channel structure + forum topics |
| `plugins/mirror.py` | Auto-mirror sessions |
| `plugins/fetch.py` | Fetch/export message ranges |
| `plugins/login.py` | User client login via phone number |
| `plugins/ytdl.py` | yt-dlp downloads |

## Clone feature (`/clone`)
- Analyzes source channel structure (forum topics or flat)
- Creates matching topics in destination
- Streams messages with a **10-second delay** between each to avoid FloodWait
- Persists resume state to MongoDB — use `/resumeclone` after any interruption
- Bugs fixed: `add_active_batch`/`remove_active_batch` missing `await`, `batch_heartbeat` missing `source_channel` arg

## User preferences
