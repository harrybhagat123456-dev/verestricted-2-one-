# ═══════════════════════════════════════════════════════════════════════
#  LINK SCHEDULER PLUGIN
#
#  Manages scheduled background tasks:
#    - Daily link validation
#    - Weekly analytics report
#    - Hourly unresolved re‑check
#
#  Uses pure asyncio scheduling (no APScheduler dependency).
#
#  Hook: on_startup  — starts scheduled tasks
#  Hook: on_shutdown — cancels all tasks
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import logging
from datetime import datetime

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


class LinkSchedulerPlugin:
    """
    Manages scheduled background tasks using asyncio (no APScheduler).
    Runs:
      - Hourly unresolved re‑check
      - Daily link validation (delegates to LinkValidatorPlugin)
      - Weekly analytics report (delegates to LinkAnalyticsPlugin)
    """

    def __init__(self, validator_plugin=None, analytics_plugin=None):
        """
        Args:
            validator_plugin: LinkValidatorPlugin instance (optional).
            analytics_plugin: LinkAnalyticsPlugin instance (optional).
        """
        self.db = _get_db()
        self.validator = validator_plugin
        self.analytics = analytics_plugin
        self._tasks: list = []

    async def on_startup(self, rewriter):
        """Start all scheduled tasks."""
        # Hourly unresolved re‑check
        t1 = asyncio.create_task(self._schedule_task(
            rewriter=rewriter,
            coro_func=self._recheck_unresolved,
            interval=3600,  # 1 hour
            name="recheck_unresolved",
            initial_delay=600,  # 10 min
        ))
        self._tasks.append(t1)

        # Daily link validation (if validator plugin available)
        if self.validator:
            t2 = asyncio.create_task(self._schedule_task(
                rewriter=rewriter,
                coro_func=self.validator.validate_all_links,
                interval=86400,  # 24 hours
                name="daily_validation",
                initial_delay=3600,  # 1 hour
            ))
            self._tasks.append(t2)

        # Weekly analytics report (if analytics plugin available)
        if self.analytics:
            t3 = asyncio.create_task(self._schedule_task(
                rewriter=rewriter,
                coro_func=lambda r: self.analytics.generate_report(r, days=7),
                interval=604800,  # 7 days
                name="weekly_report",
                initial_delay=7200,  # 2 hours
            ))
            self._tasks.append(t3)

        logger.info(
            f"[LINK-SCHEDULER] Started {len(self._tasks)} scheduled tasks "
            f"for uid={rewriter.uid}"
        )

    async def _schedule_task(self, rewriter, coro_func, interval: int, name: str, initial_delay: int = 0):
        """Generic scheduler: runs coro_func every `interval` seconds."""
        if initial_delay:
            await asyncio.sleep(initial_delay)
        while True:
            try:
                await coro_func(rewriter)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[LINK-SCHEDULER] {name} error: {e}")
            await asyncio.sleep(interval)

    async def _recheck_unresolved(self, rewriter):
        """Re‑attempt to resolve any unresolved links."""
        db = self.db
        uid = rewriter.uid
        fixed = 0

        try:
            cursor = db.unresolved_links.find({
                "uid": uid,
                "unresolved": True,
            })
            async for doc in cursor:
                src_id = doc["unresolved_src_id"]
                # Check if a mapping now exists
                mapping = await db.relink_fingerprints.find_one({
                    "uid": uid,
                    "src_msg_id": src_id,
                })
                if mapping and mapping.get("dst_msg_id"):
                    dst_id = mapping["dst_msg_id"]
                    # Update the in-memory map
                    rewriter.msg_id_map[src_id] = dst_id
                    # Fix the waiting message
                    from plugins.link_rewriter import fix_single_message
                    success = await fix_single_message(
                        uid=uid,
                        source_channel=rewriter.source_channel,
                        dst_channel_id=doc["dst_chat_id"],
                        dst_msg_id=doc["dst_msg_id"],
                        msg_id_map=rewriter.msg_id_map,
                        bot_client=rewriter.bot_client,
                        ubot=rewriter.ubot,
                    )
                    if success:
                        await db.unresolved_links.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {
                                "unresolved": False,
                                "resolved_at": datetime.utcnow(),
                                "resolved_by": "scheduler",
                            }}
                        )
                        fixed += 1
        except Exception as e:
            logger.debug(f"[LINK-SCHEDULER] recheck error: {e}")

        if fixed:
            logger.info(f"[LINK-SCHEDULER] Fixed {fixed} previously-unresolved links for uid={uid}")

    async def on_shutdown(self, rewriter):
        """Cancel all scheduled tasks."""
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        logger.info("[LINK-SCHEDULER] All tasks cancelled")
