# ═══════════════════════════════════════════════════════════════════════
#  LINK RECOVERY PLUGIN
#
#  Advanced recovery strategies for links that all normal methods missed:
#    1. Try forwarding instead of copying
#    2. Try alternative client (ubot vs bot)
#    3. Search by content fingerprint
#    4. Fallback to original message forwarding without rewriting
#
#  Can be called manually or by the scheduler for stubborn broken links.
#
#  Hook: on_startup — ensures indexes
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import logging
from datetime import datetime
from typing import Optional

from pyrogram.errors import FloodWait, MessageIdInvalid, ChannelPrivate, MediaEmpty

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


class LinkRecoveryPlugin:
    """
    Advanced recovery strategies for links that all normal methods missed.
    Multi‑strategy: forward, copy, fingerprint lookup, raw fallback.
    """

    def __init__(self):
        self.db = _get_db()
        self.recovery_log = self.db.link_recovery_log

    async def on_startup(self, rewriter):
        """Create indexes at startup."""
        try:
            await self.recovery_log.create_index("uid")
            await self.recovery_log.create_index("timestamp")
        except Exception:
            pass
        logger.info("[LINK-RECOVERY] Plugin ready")

    async def recover_broken_link(
        self,
        rewriter,
        src_msg_id: int,
        dst_msg_id: int,
    ) -> bool:
        """
        Attempt all recovery strategies for a broken link.
        Returns True if recovery succeeded.

        Strategies:
          1. Forward via ubot (preserves all metadata)
          2. Copy via bot with disable_notification
          3. Content fingerprint lookup (find duplicate already mirrored)
          4. Give up (log as unrecoverable)
        """
        uid = rewriter.uid
        source_channel = rewriter.source_channel
        dst_chat = rewriter.dst_chat_id
        bot = rewriter.bot_client
        ubot = rewriter.ubot

        # Strategy 1: Forward via ubot
        if ubot:
            try:
                src_ch = int(source_channel) if str(source_channel).lstrip('-').isdigit() else source_channel
                fwd = await ubot.forward_messages(
                    chat_id=dst_chat,
                    from_chat_id=src_ch,
                    message_ids=src_msg_id,
                )
                if fwd:
                    new_id = fwd.id if not isinstance(fwd, list) else fwd[0].id
                    logger.info(f"[RECOVERY] Strategy 1 (forward): src={src_msg_id} → dst={new_id}")
                    await self._update_mapping(rewriter, src_msg_id, new_id)
                    await self._fix_waiting_messages(rewriter, src_msg_id, new_id)
                    await self._log_recovery(uid, src_msg_id, dst_msg_id, strategy="forward", success=True)
                    return True
            except FloodWait as e:
                await asyncio.sleep(e.value + 2)
            except (ChannelPrivate, MediaEmpty):
                pass
            except Exception as e:
                logger.debug(f"[RECOVERY] Strategy 1 failed: {e}")

        # Strategy 2: Copy via bot with disable_notification
        try:
            src_ch = int(source_channel) if str(source_channel).lstrip('-').isdigit() else source_channel
            copied = await bot.copy_message(
                chat_id=dst_chat,
                from_chat_id=src_ch,
                message_id=src_msg_id,
                disable_notification=True,
            )
            if copied:
                logger.info(f"[RECOVERY] Strategy 2 (copy): src={src_msg_id} → dst={copied.id}")
                await self._update_mapping(rewriter, src_msg_id, copied.id)
                await self._fix_waiting_messages(rewriter, src_msg_id, copied.id)
                await self._log_recovery(uid, src_msg_id, dst_msg_id, strategy="copy", success=True)
                return True
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except (ChannelPrivate, MediaEmpty):
            pass
        except Exception as e:
            logger.debug(f"[RECOVERY] Strategy 2 failed: {e}")

        # Strategy 3: Content fingerprint — find a duplicate that's already mirrored
        try:
            hash_doc = await self.db.relink_fingerprints.find_one({
                "uid": uid,
                "src_msg_id": src_msg_id,
            })
            if hash_doc and hash_doc.get("content_hash"):
                content_hash = hash_doc["content_hash"]
                existing = await self.db.relink_fingerprints.find_one({
                    "uid": uid,
                    "content_hash": content_hash,
                    "dst_msg_id": {"$exists": True, "$ne": None},
                    "src_msg_id": {"$ne": src_msg_id},
                })
                if existing:
                    alt_dst_id = existing["dst_msg_id"]
                    logger.info(
                        f"[RECOVERY] Strategy 3 (fingerprint): "
                        f"found alt dst={alt_dst_id} for hash={content_hash[:8]}"
                    )
                    await self._update_mapping(rewriter, src_msg_id, alt_dst_id)
                    await self._fix_waiting_messages(rewriter, src_msg_id, alt_dst_id)
                    await self._log_recovery(uid, src_msg_id, dst_msg_id, strategy="fingerprint", success=True)
                    return True
        except Exception as e:
            logger.debug(f"[RECOVERY] Strategy 3 failed: {e}")

        # All strategies failed
        logger.warning(f"[RECOVERY] All strategies failed for src={src_msg_id}")
        await self._log_recovery(uid, src_msg_id, dst_msg_id, strategy="none", success=False)
        return False

    async def recover_all_unresolved(self, rewriter, limit: int = 50) -> int:
        """
        Try to recover all currently unresolved links.
        Returns number of successfully recovered links.
        """
        uid = rewriter.uid
        recovered = 0

        cursor = self.db.unresolved_links.find({
            "uid": uid,
            "unresolved": True,
        }).limit(limit)

        async for doc in cursor:
            src_msg_id = doc["unresolved_src_id"]
            dst_msg_id = doc["dst_msg_id"]
            success = await self.recover_broken_link(rewriter, src_msg_id, dst_msg_id)
            if success:
                recovered += 1
            await asyncio.sleep(2)  # rate limit padding

        logger.info(f"[RECOVERY] Batch recovery: {recovered} links recovered for uid={uid}")
        return recovered

    async def _update_mapping(self, rewriter, src_id: int, dst_id: int):
        """Update the mapping in MongoDB and in-memory map."""
        try:
            await self.db.relink_fingerprints.update_one(
                {"uid": rewriter.uid, "src_msg_id": src_id},
                {"$set": {
                    "dst_msg_id": dst_id,
                    "recovered": True,
                    "recovered_at": datetime.utcnow(),
                }},
                upsert=True,
            )
            rewriter.msg_id_map[src_id] = dst_id
        except Exception as e:
            logger.debug(f"[RECOVERY] Update mapping error: {e}")

    async def _fix_waiting_messages(self, rewriter, src_id: int, new_dst_id: int):
        """Fix all messages that were waiting for this src_id."""
        from plugins.link_rewriter import fix_single_message
        try:
            cursor = self.db.unresolved_links.find({
                "uid": rewriter.uid,
                "unresolved_src_id": src_id,
                "unresolved": True,
            })
            async for waiter in cursor:
                success = await fix_single_message(
                    uid=rewriter.uid,
                    source_channel=rewriter.source_channel,
                    dst_channel_id=waiter["dst_chat_id"],
                    dst_msg_id=waiter["dst_msg_id"],
                    msg_id_map=rewriter.msg_id_map,
                    bot_client=rewriter.bot_client,
                    ubot=rewriter.ubot,
                )
                if success:
                    await self.db.unresolved_links.update_one(
                        {"_id": waiter["_id"]},
                        {"$set": {
                            "unresolved": False,
                            "resolved_at": datetime.utcnow(),
                            "recovered_by": "link_recovery",
                        }}
                    )
        except Exception as e:
            logger.debug(f"[RECOVERY] Fix waiters error: {e}")

    async def _log_recovery(self, uid, src_msg_id, dst_msg_id, strategy: str, success: bool):
        """Log a recovery attempt."""
        try:
            await self.recovery_log.insert_one({
                "uid": uid,
                "src_msg_id": src_msg_id,
                "dst_msg_id": dst_msg_id,
                "strategy": strategy,
                "success": success,
                "timestamp": datetime.utcnow(),
            })
        except Exception:
            pass

    async def on_shutdown(self, rewriter):
        pass
