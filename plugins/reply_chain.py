# ════════════════════════════════════════════════════════════
#  REPLY CHAIN PRESERVER
#
#  OBJECTIVE:
#  Every message sent to destination is recorded in a map.
#  Every message that replies to something looks up that map.
#  Reply chain is preserved for ALL message types forever.
#
#  HOW TO USE:
#  1. Create once at batch start: chain = ReplyChain(uid, source_channel, db)
#  2. Load at batch start: await chain.load()
#  3. Before every send: reply_to_dst = await chain.get_reply_to(src_msg)
#  4. After every send:  await chain.record(src_msg.id, sent.id)
# ════════════════════════════════════════════════════════════

import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ReplyChain:
    """
    Preserves reply chains for ALL message types.
    One dict. Two methods. Works forever.
    """

    def __init__(self, uid: int, source_channel: str, db):
        self.uid            = uid
        self.source_channel = str(source_channel)
        self.db             = db
        self.map            = {}   # src_msg_id → dst_msg_id

    # ── LOAD ─────────────────────────────────────────────────

    async def load(self):
        """
        Load ALL historical src→dst mappings at batch start.
        Tries all channel ID format variants.
        After this, map has every previously recorded mapping.
        """
        src = self.source_channel
        variants = list(set([
            src,
            src.lstrip("-"),
            f"-100{src.lstrip('-')[3:] if src.lstrip('-').startswith('100') else src.lstrip('-')}",
        ]))

        for variant in variants:
            try:
                doc = await self.db["upload_maps"].find_one({
                    "user_id"       : self.uid,
                    "source_channel": variant,
                })
                if doc and "mappings" in doc and doc["mappings"]:
                    for k, v in doc["mappings"].items():
                        self.map[int(k)] = int(v)
                    if self.map:
                        break
            except Exception as e:
                logger.debug(f"[CHAIN] load variant={variant} failed: {e}")

        logger.info(
            f"[CHAIN] Loaded {len(self.map)} mappings "
            f"range=[{min(self.map) if self.map else 'N/A'}"
            f"..{max(self.map) if self.map else 'N/A'}]"
        )

    # ── GET REPLY TO ─────────────────────────────────────────

    async def get_reply_to(self, src_msg) -> int | None:
        """
        Given a source message, return the destination msg_id
        it should reply to. Returns None if no reply needed
        or if the replied-to message was not found in map.

        Call this BEFORE every send.
        """
        # Get reply_to from source message
        src_reply_id = getattr(src_msg, "reply_to_message_id", None)
        if not src_reply_id:
            raw = getattr(src_msg, "reply_to", None)
            if raw:
                src_reply_id = getattr(raw, "reply_to_msg_id", None)

        if not src_reply_id:
            return None  # message does not reply to anything

        # Look up in map
        dst_reply_id = self.map.get(src_reply_id)

        if dst_reply_id:
            logger.debug(
                f"[CHAIN] src_reply={src_reply_id} → dst_reply={dst_reply_id} ✅"
            )
        else:
            logger.warning(
                f"[CHAIN] src_reply={src_reply_id} NOT in map "
                f"— will send without reply_to "
                f"(map_size={len(self.map)})"
            )

        return dst_reply_id

    # ── RECORD ───────────────────────────────────────────────

    async def record(self, src_msg_id: int, dst_msg_id: int):
        """
        Record a successful upload.
        Call this AFTER every successful send — ALL message types.

        Updates in-memory map immediately.
        Saves to MongoDB in background (never blocks the batch).
        """
        # Update in-memory map immediately
        self.map[src_msg_id] = dst_msg_id

        logger.debug(
            f"[CHAIN] Recorded src={src_msg_id} → dst={dst_msg_id} "
            f"map_size={len(self.map)}"
        )

        # Save to MongoDB in background
        asyncio.create_task(
            self._save(src_msg_id, dst_msg_id)
        )

    async def _save(self, src_msg_id: int, dst_msg_id: int):
        """Save mapping to MongoDB. Runs in background."""
        try:
            await self.db["upload_maps"].update_one(
                {
                    "user_id"       : self.uid,
                    "source_channel": self.source_channel,
                },
                {"$set": {f"mappings.{src_msg_id}": dst_msg_id}},
                upsert=True,
            )
        except Exception as e:
            logger.debug(f"[CHAIN] save failed src={src_msg_id}: {e}")
