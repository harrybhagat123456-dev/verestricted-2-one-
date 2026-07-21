# ═══════════════════════════════════════════════════════════════════════
#  LINK VALIDATOR PLUGIN
#
#  Periodically scans destination channel messages, validates every
#  rewritten link, reports broken ones, and attempts auto‑heal.
#
#  Hook: on_startup  — starts periodic validation task
#  Hook: on_shutdown — cancels background task
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional, List

from pyrogram import Client, types
from pyrogram.errors import FloodWait, MessageIdInvalid, ChannelPrivate

from config import MONGO_DB as MONGO_URI, DB_NAME

logger = logging.getLogger(__name__)

from motor.motor_asyncio import AsyncIOMotorClient

_client = None
_db = None

def _get_db():
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(MONGO_URI)
        _db = _client[DB_NAME]
    return _db


class LinkValidatorPlugin:
    """
    Periodically scans all messages in the destination channel,
    validates every rewritten link, reports broken ones via Telegram,
    and attempts auto‑heal.
    """

    def __init__(self, report_chat_id: Optional[int] = None, scan_interval: int = 21600):
        """
        Args:
            report_chat_id: Chat ID to send validation reports to.
            scan_interval: Seconds between scans (default 6 hours).
        """
        self.db = _get_db()
        self.report_chat_id = report_chat_id
        self.scan_interval = scan_interval
        self.validation_collection = self.db.link_validation
        self.scan_task = None

    async def on_startup(self, rewriter):
        """Create indexes and start periodic validator."""
        try:
            await self.validation_collection.create_index("uid")
            await self.validation_collection.create_index("last_scan")
        except Exception:
            pass
        self.scan_task = asyncio.create_task(self._periodic_validator(rewriter))
        logger.info("[LINK-VALIDATOR] Plugin started")

    async def _periodic_validator(self, rewriter):
        """Run validation on schedule."""
        # Wait a bit before first scan to let batch settle
        await asyncio.sleep(300)  # 5 min initial delay
        while True:
            try:
                await self.validate_all_links(rewriter)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[LINK-VALIDATOR] Error: {e}")
            await asyncio.sleep(self.scan_interval)

    async def validate_all_links(self, rewriter):
        """
        Scan recent destination channel messages and check each link.
        Reports broken ones and attempts auto‑heal.
        """
        uid = rewriter.uid
        dst_chat = rewriter.dst_chat_id
        bot = rewriter.bot_client

        if not dst_chat or not bot:
            return

        broken_links = []
        checked = 0

        try:
            async for message in bot.get_chat_history(dst_chat, limit=500):
                if getattr(message, 'empty', True):
                    continue
                checked += 1
                links = self._extract_all_links(message)
                for link_info in links:
                    ok = await self._check_link(bot, link_info["chat_id"], link_info["message_id"])
                    if not ok:
                        broken_links.append(link_info)
        except FloodWait as e:
            logger.warning(f"[LINK-VALIDATOR] FloodWait during scan: {e.value}s")
            await asyncio.sleep(e.value + 2)
            return
        except Exception as e:
            logger.error(f"[LINK-VALIDATOR] Scan error: {e}")
            return

        # Report broken links
        if broken_links and self.report_chat_id:
            try:
                report = f"⚠️ **Link Validation Report**\n\n"
                report += f"Checked: `{checked}` messages\n"
                report += f"Broken: `{len(broken_links)}` links\n\n"
                for bl in broken_links[:10]:
                    report += f"• {bl['url']} — missing\n"
                if len(broken_links) > 10:
                    report += f"… and {len(broken_links) - 10} more"
                await bot.send_message(self.report_chat_id, report)
            except Exception as e:
                logger.debug(f"[LINK-VALIDATOR] Report send failed: {e}")

            # Attempt auto‑heal
            for bl in broken_links[:20]:  # limit to avoid rate limits
                try:
                    await self._auto_heal(rewriter, bl)
                except Exception:
                    pass
                await asyncio.sleep(1)  # rate limit padding

        # Update last scan
        try:
            await self.validation_collection.update_one(
                {"uid": uid},
                {"$set": {
                    "last_scan": datetime.utcnow(),
                    "broken_count": len(broken_links),
                    "checked_count": checked,
                }},
                upsert=True,
            )
        except Exception:
            pass

        logger.info(
            f"[LINK-VALIDATOR] Scan done: checked={checked}, broken={len(broken_links)}"
        )

    def _extract_all_links(self, message: types.Message) -> List[dict]:
        """Extract all t.me/c/... links from message text and entities."""
        pattern = re.compile(r'https?://t\.me/c/(\d+)/(\d+)', re.IGNORECASE)
        links = []
        seen = set()

        # Text
        text = message.text or message.caption or ""
        for match in pattern.finditer(text):
            key = (match.group(1), match.group(2))
            if key not in seen:
                seen.add(key)
                links.append({
                    "url": match.group(0),
                    "chat_id": int(match.group(1)),
                    "message_id": int(match.group(2)),
                })

        # Entities (text_link)
        entities = message.entities or message.caption_entities or []
        for ent in entities:
            url = getattr(ent, "url", "") or ""
            match = pattern.search(url)
            if match:
                key = (match.group(1), match.group(2))
                if key not in seen:
                    seen.add(key)
                    links.append({
                        "url": url,
                        "chat_id": int(match.group(1)),
                        "message_id": int(match.group(2)),
                    })

        return links

    async def _check_link(self, bot: Client, chat_id: int, msg_id: int) -> bool:
        """Return True if message exists and is accessible."""
        try:
            # Convert bare chat_id to full channel ID for get_messages
            full_chat_id = int(f"-100{chat_id}") if chat_id > 0 else chat_id
            msg = await bot.get_messages(full_chat_id, msg_id)
            return msg and not getattr(msg, "empty", True)
        except (MessageIdInvalid, ChannelPrivate):
            return False
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            return False
        except Exception:
            return False

    async def _auto_heal(self, rewriter, broken_link_info: dict):
        """Try to re‑mirror the missing source message."""
        uid = rewriter.uid
        dst_msg_id = broken_link_info["message_id"]
        # Search fingerprints for the mapping
        doc = await self.db.relink_fingerprints.find_one({"uid": uid, "dst_msg_id": dst_msg_id})
        if doc:
            src_msg_id = doc["src_msg_id"]
            await rewriter.handle_not_mirrored_manually(src_msg_id, dst_msg_id)

    async def on_shutdown(self, rewriter):
        """Cancel background scan task."""
        if self.scan_task:
            self.scan_task.cancel()
            try:
                await self.scan_task
            except asyncio.CancelledError:
                pass
