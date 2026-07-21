# ═══════════════════════════════════════════════════════════════════════
#  DEPENDENCY GRAPH PLUGIN
#
#  Builds a dependency graph of messages based on internal links.
#  Sorts messages so that a message is sent AFTER all messages it
#  links to. This prevents "broken link on first send" situations.
#
#  Hook: before_batch — reorders messages via topological sort
#  Hook: after_send  — records resolved dependencies
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Set

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


class DependencyGraphPlugin:
    """
    Builds a dependency graph of messages based on internal links.
    Sorts messages so that a message is sent AFTER all messages it
    links to. This prevents "broken link on first send" situations.
    """

    def __init__(self):
        self.db = _get_db()
        self.collection = self.db.dependency_graph

    async def on_startup(self, rewriter):
        """Create indexes at startup."""
        try:
            await self.collection.create_index("uid")
            await self.collection.create_index("src_msg_id")
        except Exception:
            pass
        logger.info("[DEP-GRAPH] Plugin ready")

    async def before_batch(self, rewriter, messages: List) -> List:
        """
        Compute topological order of messages.
        Reorders the message list if dependencies exist within the batch.

        NOTE: This only works when messages are pre-loaded (not streaming).
        In streaming mode, this is a no-op — the after_send auto-fix
        handles unresolved links instead.
        """
        if not messages or len(messages) <= 1:
            return messages

        uid = rewriter.uid
        src_channel = rewriter.source_channel
        src_clean = rewriter.source_channel.lstrip('-')
        if src_clean.startswith('100') and len(src_clean) > 5 and src_clean[3:].isdigit():
            src_clean = src_clean[3:]

        # 1. Extract all links from the batch
        pattern = re.compile(
            r'https?://t\.me/c/' + re.escape(src_clean) + r'/(\d+)',
            re.IGNORECASE
        )
        links: Dict[int, Set[int]] = defaultdict(set)
        msg_ids = set()

        for msg in messages:
            msg_id = getattr(msg, 'id', None)
            if msg_id is None:
                continue
            msg_ids.add(msg_id)
            text = msg.text or msg.caption or ""
            if not text:
                continue
            for match in pattern.finditer(text):
                linked_id = int(match.group(1))
                links[msg_id].add(linked_id)

        if not links:
            return messages

        # 2. Build graph (only intra-batch dependencies matter)
        graph: Dict[int, Set[int]] = {mid: set() for mid in msg_ids}
        for src, targets in links.items():
            for t in targets:
                if t in graph and t != src:
                    graph[src].add(t)  # src depends on t

        # 3. Topological sort (Kahn's algorithm)
        # In-degree = number of messages that depend on this node
        in_degree: Dict[int, int] = {node: 0 for node in graph}
        for src, deps in graph.items():
            for dep in deps:
                in_degree[dep] = in_degree.get(dep, 0) + 1

        # Start with nodes that nothing depends on (highest in-degree resolved first)
        queue = [node for node, deg in in_degree.items() if deg == 0]
        sorted_nodes = []

        while queue:
            # Process nodes with no incoming edges first
            node = queue.pop(0)
            sorted_nodes.append(node)
            # For each node that this node depends on, reduce their in-degree
            for dep in graph.get(node, set()):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)

        # If cycle detected, fall back to original order
        if len(sorted_nodes) != len(msg_ids):
            logger.warning(
                f"[DEP-GRAPH] Cycle detected for uid={uid}, "
                f"sorted={len(sorted_nodes)}/{len(msg_ids)} — using original order"
            )
            return messages

        # Reverse: we want dependencies FIRST (they should be sent before dependents)
        sorted_nodes.reverse()

        # Reorder messages
        msg_by_id = {getattr(msg, 'id'): msg for msg in messages if hasattr(msg, 'id')}
        ordered_messages = [msg_by_id[mid] for mid in sorted_nodes if mid in msg_by_id]

        # Add any messages that weren't in the graph (no links)
        seen_ids = set(sorted_nodes)
        for msg in messages:
            mid = getattr(msg, 'id', None)
            if mid and mid not in seen_ids:
                ordered_messages.append(msg)

        logger.info(
            f"[DEP-GRAPH] Reordered {len(messages)} messages "
            f"to satisfy {len(links)} link dependencies"
        )
        return ordered_messages

    async def after_send(self, rewriter, src_msg_id, dst_msg_id, unresolved):
        """Record that this message is now resolved."""
        try:
            await self.collection.update_one(
                {"uid": rewriter.uid, "src_msg_id": src_msg_id},
                {"$set": {
                    "resolved": True,
                    "dst_msg_id": dst_msg_id,
                    "unresolved_count": len(unresolved),
                    "updated_at": datetime.utcnow(),
                }},
                upsert=True,
            )
        except Exception as e:
            logger.debug(f"[DEP-GRAPH] after_send error: {e}")

    async def on_shutdown(self, rewriter):
        pass
