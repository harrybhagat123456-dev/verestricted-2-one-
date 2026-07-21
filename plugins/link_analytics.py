# ═══════════════════════════════════════════════════════════════════════
#  LINK ANALYTICS PLUGIN
#
#  Tracks every rewrite, unresolved, auto‑mirror, and success rate.
#  Can send a detailed Telegram report on demand or at shutdown.
#
#  Hook: after_send  — records rewrite event
#  Hook: on_shutdown — sends final report
# ═══════════════════════════════════════════════════════════════════════

import logging
from datetime import datetime, timedelta
from collections import Counter

from motor.motor_asyncio import AsyncIOMotorClient

from config import MONGO_DB as MONGO_URI, DB_NAME

logger = logging.getLogger(__name__)

_client = None
_db = None

def _get_db():
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(MONGO_URI)
        _db = _client[DB_NAME]
    return _db


class LinkAnalyticsPlugin:
    """
    Tracks every rewrite, unresolved, auto‑mirror, and success rate.
    Can send a detailed Telegram report (text + stats) on demand.
    """

    def __init__(self, report_chat_id: int = None):
        self.db = _get_db()
        self.report_chat_id = report_chat_id
        self.analytics_collection = self.db.link_analytics

    async def on_startup(self, rewriter):
        """Create indexes at startup."""
        try:
            await self.analytics_collection.create_index("uid")
            await self.analytics_collection.create_index("timestamp")
        except Exception:
            pass
        logger.info("[LINK-ANALYTICS] Plugin ready")

    async def after_send(self, rewriter, src_msg_id, dst_msg_id, unresolved):
        """Record a rewrite event."""
        try:
            await self.analytics_collection.insert_one({
                "uid": rewriter.uid,
                "source_channel": rewriter.source_channel,
                "src_msg_id": src_msg_id,
                "dst_msg_id": dst_msg_id,
                "unresolved_count": len(unresolved),
                "timestamp": datetime.utcnow(),
                "type": "send",
            })
        except Exception as e:
            logger.debug(f"[LINK-ANALYTICS] Insert failed: {e}")

    async def generate_report(self, rewriter, days: int = 7):
        """Generate and send a detailed analytics report."""
        since = datetime.utcnow() - timedelta(days=days)
        uid = rewriter.uid

        cursor = self.analytics_collection.find({
            "uid": uid,
            "timestamp": {"$gte": since},
        })

        total = 0
        unresolved_total = 0
        daily_counts = Counter()

        async for doc in cursor:
            total += 1
            unresolved_total += doc.get("unresolved_count", 0)
            date_str = doc["timestamp"].strftime("%Y-%m-%d")
            daily_counts[date_str] += 1

        success_rate = ((total - unresolved_total) / total * 100) if total else 0

        # Get auto‑mirror stats
        try:
            mirror_done = await self.db.auto_mirror_queue.count_documents({
                "uid": uid,
                "status": "done",
            })
            mirror_pending = await self.db.auto_mirror_queue.count_documents({
                "uid": uid,
                "status": "pending",
            })
        except Exception:
            mirror_done = 0
            mirror_pending = 0

        # Get unresolved count
        try:
            unresolved_count = await self.db.unresolved_links.count_documents({
                "uid": uid,
                "unresolved": True,
            })
        except Exception:
            unresolved_count = 0

        report = (
            f"📊 **Link Analytics Report** (last {days} days)\n\n"
            f"• Total messages processed: `{total}`\n"
            f"• Broken links detected: `{unresolved_total}`\n"
            f"• Success rate: `{success_rate:.1f}%`\n"
            f"• Auto‑mirrored messages: `{mirror_done}`\n"
            f"• Pending mirrors: `{mirror_pending}`\n"
            f"• Still unresolved: `{unresolved_count}`\n"
        )

        if daily_counts:
            report += f"\n**Daily activity:**\n"
            for date, count in sorted(daily_counts.items())[-7:]:
                bar = "█" * min(count // 5, 20)
                report += f"  `{date}`: {count} {bar}\n"

        # Send report
        if self.report_chat_id and rewriter.bot_client:
            try:
                await rewriter.bot_client.send_message(self.report_chat_id, report)
            except Exception as e:
                logger.debug(f"[LINK-ANALYTICS] Report send failed: {e}")

        logger.info(f"[LINK-ANALYTICS] Report generated for uid={uid}: {total} msgs, {success_rate:.1f}% success")
        return report

    async def on_shutdown(self, rewriter):
        """Send final batch report."""
        try:
            await self.generate_report(rewriter, days=1)
        except Exception:
            pass
