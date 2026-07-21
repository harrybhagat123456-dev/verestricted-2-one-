# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                  LINK REWRITER PLUGIN – ULTRA EDITION                    ║
# ║                                                                          ║
# ║  1. On‑the‑fly rewriting BEFORE the message hits the destination.        ║
# ║  2. Full source‑to‑destination mapping from ALL collections.             ║
# ║  3. Automatic mirroring of missing linked messages (with flood control). ║
# ║  4. Self‑healing: once a missing message is mirrored, all links to it    ║
# ║     are edited in every message that waited for it.                      ║
# ║  5. Thread‑safe multi‑user/multi‑batch support.                          ║
# ║  6. LRU cache for complete maps (memory‑efficient).                      ║
# ║  7. Periodic background scanner for leftover broken links.               ║
# ║  8. Concurrency locking per destination message.                         ║
# ║  9. FloodWait‑aware batch auto‑mirror with exponential backoff.          ║
# ╚══════════════════════════════════════════════════════════════════════════╝

import asyncio
import copy
import logging
import re
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, types
from pyrogram.errors import (
    FloodWait,
    MessageNotModified,
    ChannelPrivate,
    MediaEmpty,
)

from config import MONGO_DB as MONGO_URI, DB_NAME

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────────────────────
# MongoDB connection & collections (lazy, thread‑safe)
# ────────────────────────────────────────────────────────────────
_client: Optional[AsyncIOMotorClient] = None
_db = None

def get_db():
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(MONGO_URI)
        _db = _client[DB_NAME]
    return _db

async def ensure_indexes():
    """Create indexes to speed up common queries. Call once at startup."""
    db = get_db()
    try:
        await db.upload_maps.create_index("user_id")
        await db.upload_maps.create_index("source_channel")
    except Exception:
        pass
    try:
        await db.relink_fingerprints.create_index("uid")
        await db.relink_fingerprints.create_index("src_msg_id")
    except Exception:
        pass
    try:
        await db.unresolved_links.create_index("uid")
        await db.unresolved_links.create_index("unresolved_src_id")
        await db.unresolved_links.create_index("dst_msg_id")
    except Exception:
        pass
    try:
        await db.auto_mirror_queue.create_index("uid")
        await db.auto_mirror_queue.create_index("status")
    except Exception:
        pass
    try:
        await db.mirrored_messages_index.create_index("uid")
        await db.mirrored_messages_index.create_index("dst_chat_id")
        await db.mirrored_messages_index.create_index("src_msg_id")
    except Exception:
        pass
    try:
        await db.relink_url_cache.create_index("uid")
        await db.relink_url_cache.create_index("src_msg_id")
    except Exception:
        pass
    logger.info("Indexes ensured.")


# ────────────────────────────────────────────────────────────────
# 1. Channel normalisation & URL builder (thread‑aware)
# ────────────────────────────────────────────────────────────────
def normalize_ch(channel_id) -> str:
    s = str(channel_id).strip().lstrip("-")
    if s.startswith("100") and len(s) > 5 and s[3:].isdigit():
        s = s[3:]
    return s


def build_dest_url(dst_chat_id: int, dst_msg_id: int, thread_id: Optional[int] = None) -> str:
    clean = normalize_ch(dst_chat_id)
    url = f"https://t.me/c/{clean}/{dst_msg_id}"
    if thread_id:
        url += f"?thread={thread_id}"
    return url


# ────────────────────────────────────────────────────────────────
# 2. Complete map loader (memory‑efficient with LRU)
# ────────────────────────────────────────────────────────────────
class MapCache:
    """LRU cache of complete maps per (uid, source_channel, dst_chat_id)."""
    def __init__(self, max_size=50):
        self.cache = OrderedDict()
        self.max_size = max_size

    def _key(self, uid, src, dst):
        return f"{uid}:{normalize_ch(src)}:{normalize_ch(dst)}"

    def get(self, uid, src, dst):
        key = self._key(uid, src, dst)
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def set(self, uid, src, dst, data: dict):
        key = self._key(uid, src, dst)
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
        self.cache[key] = data

    def clear(self):
        self.cache.clear()


_map_cache = MapCache(max_size=30)


async def load_complete_map(
    uid: int,
    source_channel: str,
    dst_chat_id: int,
    bypass_cache: bool = False,
) -> Dict[int, int]:
    """
    Load ALL src→dst mappings from MongoDB.
    Merged in priority:
        1. upload_maps
        2. relink_fingerprints
        3. mirrored_messages_index

    Also includes fallback by dest_channel when uid lookup returns 0
    (handles the case where a different user ran /batch).
    """
    if not bypass_cache:
        cached = _map_cache.get(uid, source_channel, dst_chat_id)
        if cached is not None:
            logger.debug(f"Using cached map ({len(cached)} entries)")
            return cached

    db = get_db()
    combined = {}
    src_clean = normalize_ch(source_channel)

    # --- Source 1: upload_maps ---
    variants = [str(source_channel), src_clean, f"-100{src_clean}", f"-{src_clean}"]
    _found_by_uid = False
    for variant in set(variants):
        try:
            doc = await db.upload_maps.find_one({
                "user_id": uid,
                "source_channel": variant,
            })
            if doc and "mappings" in doc:
                for k, v in doc["mappings"].items():
                    combined[int(k)] = int(v)
                if combined:
                    logger.info(f"[MAP-LOAD] upload_maps variant='{variant}' → {len(combined)} entries")
                    _found_by_uid = True
                    break
        except Exception as e:
            logger.debug(f"[MAP-LOAD] upload_maps variant={variant} error: {e}")

    # Fallback: search by dest_channel when uid returns nothing
    if not _found_by_uid and dst_chat_id:
        for _dc_try in [dst_chat_id, int(str(dst_chat_id).lstrip('-')), str(dst_chat_id)]:
            try:
                cursor = db.upload_maps.find({"dest_channel": _dc_try})
                async for doc in cursor:
                    mappings = doc.get("mappings", {})
                    if mappings:
                        _map_uid = doc.get("user_id", "?")
                        for k, v in mappings.items():
                            combined[int(k)] = int(v)
                        logger.info(
                            f"[MAP-LOAD] upload_maps fallback by dest_channel={_dc_try} "
                            f"→ {len(mappings)} entries (owned by uid={_map_uid})"
                        )
                if combined:
                    break
            except Exception as e:
                logger.debug(f"[MAP-LOAD] upload_maps dest_channel fallback error: {e}")

    # --- Source 2: fingerprints ---
    before = len(combined)
    try:
        async for doc in db.relink_fingerprints.find({"uid": uid}):
            src_id = doc.get("src_msg_id")
            dst_id = doc.get("dst_msg_id")
            if src_id and dst_id and src_id not in combined:
                combined[int(src_id)] = int(dst_id)
        # Also try fingerprints without uid filter if nothing found
        if len(combined) == before:
            async for doc in db.relink_fingerprints.find({"source_channel": str(source_channel)}):
                src_id = doc.get("src_msg_id")
                dst_id = doc.get("dst_msg_id")
                if src_id and dst_id and src_id not in combined:
                    combined[int(src_id)] = int(dst_id)
    except Exception as e:
        logger.debug(f"fingerprints scan: {e}")
    logger.info(f"fingerprints added {len(combined) - before} entries")

    # --- Source 3: mirrored_index ---
    before = len(combined)
    try:
        async for doc in db.mirrored_messages_index.find({
            "uid": uid,
            "dst_chat_id": dst_chat_id,
        }):
            src_id = doc.get("src_msg_id")
            dst_id = doc.get("dst_msg_id")
            if src_id and dst_id and src_id not in combined:
                combined[int(src_id)] = int(dst_id)
        # Also try without uid filter
        if len(combined) == before:
            async for doc in db.mirrored_messages_index.find({"dst_chat_id": dst_chat_id}):
                src_id = doc.get("src_msg_id")
                dst_id = doc.get("dst_msg_id")
                if src_id and dst_id and src_id not in combined:
                    combined[int(src_id)] = int(dst_id)
    except Exception as e:
        logger.debug(f"mirrored_index scan: {e}")
    logger.info(f"mirrored_index added {len(combined) - before} entries")

    combined_sorted = dict(sorted(combined.items()))
    logger.info(
        f"Complete map: {len(combined_sorted)} entries "
        f"(keys {min(combined_sorted) if combined_sorted else 'N/A'}"
        f"..{max(combined_sorted) if combined_sorted else 'N/A'})"
    )

    _map_cache.set(uid, source_channel, dst_chat_id, combined_sorted)
    return combined_sorted


# ────────────────────────────────────────────────────────────────
# 3. Link extraction (text, entities)
# ────────────────────────────────────────────────────────────────
def extract_source_links(
    text: Optional[str],
    entities: Optional[List],
    source_channel: str,
) -> List[dict]:
    """
    Returns list of link dicts:
    {
        "src_msg_id": int,
        "original_url": str,
        "location": "text" | "entity",
        "entity_index": int | None,
        "thread_id": int | None,
    }
    """
    src_clean = normalize_ch(source_channel)
    pattern = re.compile(
        r'https?://t\.me/c/' + re.escape(src_clean) + r'/(\d+)(?:\?thread=(\d+))?/?',
        re.IGNORECASE
    )

    found = []
    seen = set()

    # 1. Text
    if text:
        for m in pattern.finditer(text):
            sid = int(m.group(1))
            tid = m.group(2)
            if sid not in seen:
                seen.add(sid)
                found.append({
                    "src_msg_id": sid,
                    "original_url": m.group(0),
                    "location": "text",
                    "entity_index": None,
                    "thread_id": int(tid) if tid else None,
                })

    # 2. Entities
    if entities:
        for i, ent in enumerate(entities):
            etype = str(getattr(ent, "type", "")).lower()
            if "text_link" not in etype:
                continue
            url = getattr(ent, "url", "") or ""
            match = pattern.search(url)
            if match:
                sid = int(match.group(1))
                tid = match.group(2)
                if sid not in seen:
                    seen.add(sid)
                    found.append({
                        "src_msg_id": sid,
                        "original_url": url,
                        "location": "entity",
                        "entity_index": i,
                        "thread_id": int(tid) if tid else None,
                    })

    return found


# ────────────────────────────────────────────────────────────────
# 4. On‑the‑fly rewriting (text, entities)
# ────────────────────────────────────────────────────────────────
async def rewrite_message_links(
    text: Optional[str],
    entities: Optional[List],
    source_channel: str,
    dst_chat_id: int,
    msg_id_map: Dict[int, int],
) -> Tuple[Optional[str], Optional[List], List[int]]:
    """
    Rewrites ALL source channel links in text and entities.
    Returns:
        new_text, new_entities, unresolved_src_ids
    """
    src_clean = normalize_ch(source_channel)
    pattern = re.compile(
        r'(https?://t\.me/c/' + re.escape(src_clean) + r'/)(\d+)((?:\?thread=\d+)?)',
        re.IGNORECASE
    )

    unresolved = []
    new_text = text or ""

    def _replace_text(match):
        src_msg_id = int(match.group(2))
        thread_part = match.group(3) or ""
        dst_id = msg_id_map.get(src_msg_id)
        if dst_id:
            return build_dest_url(dst_chat_id, dst_id) + thread_part
        else:
            unresolved.append(src_msg_id)
            return match.group(0)

    if new_text:
        new_text = pattern.sub(_replace_text, new_text)

    # Entities
    new_entities = []
    if entities:
        for entity in entities:
            new_e = copy.deepcopy(entity)
            etype = str(getattr(entity, "type", "")).lower()
            if "text_link" in etype:
                url = getattr(entity, "url", "") or ""
                match = pattern.search(url)
                if match:
                    src_msg_id = int(match.group(2))
                    thread_part = match.group(3) or ""
                    dst_id = msg_id_map.get(src_msg_id)
                    if dst_id:
                        new_e.url = build_dest_url(dst_chat_id, dst_id) + thread_part
                    else:
                        unresolved.append(src_msg_id)
            new_entities.append(new_e)

    # Deduplicate unresolved
    unresolved = list(dict.fromkeys(unresolved))

    return new_text, new_entities, unresolved


# ────────────────────────────────────────────────────────────────
# 5. Saving / fixing unresolved links
# ────────────────────────────────────────────────────────────────
async def save_unresolved_links(
    uid: int,
    source_channel: str,
    dst_chat_id: int,
    dst_msg_id: int,
    src_msg_id: int,
    unresolved_ids: List[int],
):
    if not unresolved_ids:
        return
    db = get_db()
    ops = []
    for un_id in unresolved_ids:
        ops.append({
            "uid": uid,
            "source_channel": source_channel,
            "dst_chat_id": dst_chat_id,
            "dst_msg_id": dst_msg_id,
            "src_msg_id": src_msg_id,
            "unresolved_src_id": un_id,
            "unresolved": True,
            "created_at": datetime.utcnow(),
        })
    try:
        await db.unresolved_links.insert_many(ops, ordered=False)
        logger.info(f"[UNRESOLVED] Saved {len(ops)} links for dst={dst_msg_id}")
    except Exception as e:
        logger.debug(f"Save unresolved error: {e}")


# ────────────────────────────────────────────────────────────────
# 6. Auto‑fix on new mirror (with edit lock per dst_msg_id)
# ────────────────────────────────────────────────────────────────
_edit_locks: Dict[str, asyncio.Lock] = {}

def _lock_key(uid, dst_chat_id, dst_msg_id):
    return f"{uid}:{dst_chat_id}:{dst_msg_id}"


async def fix_single_message(
    uid: int,
    source_channel: str,
    dst_channel_id: int,
    dst_msg_id: int,
    msg_id_map: Dict[int, int],
    bot_client: Client,
    ubot: Optional[Client],
) -> bool:
    """Fetch & edit destination message, rewriting all broken links."""
    lock_key = _lock_key(uid, dst_channel_id, dst_msg_id)
    if lock_key not in _edit_locks:
        _edit_locks[lock_key] = asyncio.Lock()
    async with _edit_locks[lock_key]:
        # Fetch current state
        dst_msg = None
        for client in [ubot, bot_client]:
            if client is None:
                continue
            try:
                dst_msg = await client.get_messages(dst_channel_id, dst_msg_id)
                if dst_msg and not getattr(dst_msg, "empty", True):
                    break
            except Exception:
                continue

        if not dst_msg:
            return False

        text = dst_msg.text or ""
        caption = dst_msg.caption or ""
        is_caption = bool(dst_msg.caption) and not dst_msg.text
        current = caption if is_caption else text
        entities = dst_msg.caption_entities if is_caption else dst_msg.entities

        new_text, new_entities, still_unresolved = await rewrite_message_links(
            text=current,
            entities=entities,
            source_channel=source_channel,
            dst_chat_id=dst_channel_id,
            msg_id_map=msg_id_map,
        )

        if new_text == current and new_entities == entities:
            return True  # nothing changed

        # Try edit
        for client in [ubot, bot_client]:
            if client is None:
                continue
            try:
                kwargs = {
                    "chat_id": dst_channel_id,
                    "message_id": dst_msg_id,
                }
                if not is_caption:
                    kwargs["text"] = new_text
                    if new_entities:
                        kwargs["entities"] = new_entities
                    await client.edit_message_text(**kwargs)
                else:
                    kwargs["caption"] = new_text
                    if new_entities:
                        kwargs["caption_entities"] = new_entities
                    await client.edit_message_caption(**kwargs)
                logger.info(f"[FIX] Edited dst={dst_msg_id} via {client.__class__.__name__}")
                return True
            except MessageNotModified:
                return True
            except FloodWait as e:
                await asyncio.sleep(e.value + 2)
            except Exception as e:
                err = str(e)
                if "MESSAGE_AUTHOR_REQUIRED" in err:
                    continue
                logger.warning(f"[FIX] Edit failed ({client}): {e}")
        return False


async def on_message_mirrored(
    uid: int,
    source_channel: str,
    src_msg_id: int,
    dst_msg_id: int,
    dst_chat_id: int,
    msg_id_map: Dict[int, int],
    bot_client: Client,
    ubot: Optional[Client],
):
    """Called after every successful mirror. Updates map & fixes waiters."""
    msg_id_map[src_msg_id] = dst_msg_id
    db = get_db()

    try:
        cursor = db.unresolved_links.find({
            "uid": uid,
            "source_channel": str(source_channel),
            "unresolved_src_id": src_msg_id,
            "unresolved": True,
        })
        waiters = []
        async for doc in cursor:
            waiters.append(doc)
    except Exception as e:
        logger.debug(f"[AUTO-FIX] Query failed: {e}")
        return

    if not waiters:
        return

    logger.info(f"[AUTO-FIX] {len(waiters)} messages waiting for src={src_msg_id}")

    for item in waiters:
        success = await fix_single_message(
            uid=uid,
            source_channel=source_channel,
            dst_channel_id=item["dst_chat_id"],
            dst_msg_id=item["dst_msg_id"],
            msg_id_map=msg_id_map,
            bot_client=bot_client,
            ubot=ubot,
        )
        if success:
            await db.unresolved_links.update_one(
                {"_id": item["_id"]},
                {"$set": {"unresolved": False, "resolved_at": datetime.utcnow(), "resolved_via": "auto_fix"}}
            )


# ────────────────────────────────────────────────────────────────
# 7. Auto‑mirror missing messages (flood‑controlled)
# ────────────────────────────────────────────────────────────────
_mirror_semaphore = asyncio.Semaphore(3)  # max concurrent mirrors


async def mirror_single_message(
    src_msg: types.Message,
    dst_chat_id: int,
    bot_client: Client,
    ubot: Optional[Client],
    uid: int,
    source_channel: str,
) -> Optional[int]:
    """
    Mirror a single source message to destination.
    Returns destination message id or None.
    Uses forward first (fast, preserves metadata), falls back to copy_message.
    """
    async with _mirror_semaphore:
        for attempt in range(3):
            try:
                # Try forward first (fast, preserves metadata)
                if ubot:
                    try:
                        fwd = await ubot.forward_messages(
                            chat_id=dst_chat_id,
                            from_chat_id=source_channel,
                            message_ids=src_msg.id,
                        )
                        if fwd:
                            # forward_messages may return a list or single message
                            if isinstance(fwd, list):
                                return fwd[0].id
                            return fwd.id
                    except Exception:
                        pass  # fallback to copy

                # Fallback: copy_message via bot (no local download needed)
                try:
                    sent = await bot_client.copy_message(
                        chat_id=dst_chat_id,
                        from_chat_id=source_channel,
                        message_id=src_msg.id,
                    )
                    return sent.id
                except Exception:
                    pass

                # Last resort: text-only send
                if src_msg.text:
                    sent = await bot_client.send_message(
                        chat_id=dst_chat_id,
                        text=src_msg.text,
                        entities=src_msg.entities,
                        disable_web_page_preview=True,
                    )
                    return sent.id

                return None

            except FloodWait as e:
                await asyncio.sleep(e.value + 2)
            except (ChannelPrivate, MediaEmpty) as e:
                logger.warning(f"Cannot mirror {src_msg.id}: {e}")
                return None
            except Exception as e:
                logger.error(f"Mirror attempt {attempt+1} failed for {src_msg.id}: {e}")
                await asyncio.sleep(2 ** attempt)
    return None


async def handle_not_mirrored(
    uid: int,
    source_channel: str,
    src_msg_id: int,
    dst_chat_id: int,
    dst_msg_id: int,     # message that has broken link
    msg_id_map: Dict[int, int],
    bot_client: Client,
    ubot: Optional[Client],
) -> Optional[int]:
    """Mirror a source message if not already done. If ubot absent, queue."""
    db = get_db()
    if ubot is None:
        await db.auto_mirror_queue.update_one(
            {
                "uid": uid,
                "source_channel": source_channel,
                "src_msg_id": src_msg_id,
            },
            {"$set": {
                "uid": uid,
                "source_channel": source_channel,
                "src_msg_id": src_msg_id,
                "dst_chat_id": dst_chat_id,
                "broken_in_dst": dst_msg_id,
                "status": "pending",
                "queued_at": datetime.utcnow(),
            }},
            upsert=True,
        )
        logger.info(f"[AUTO-MIRROR] src={src_msg_id} queued (ubot unavailable)")
        return None

    # Fetch source message
    try:
        src_ch = int(source_channel) if str(source_channel).lstrip('-').isdigit() else source_channel
        src_msg = await ubot.get_messages(src_ch, src_msg_id)
        if not src_msg or getattr(src_msg, "empty", True):
            logger.info(f"src={src_msg_id} deleted/unavailable")
            return None
    except Exception as e:
        logger.error(f"Fetch src={src_msg_id} failed: {e}")
        return None

    new_dst_id = await mirror_single_message(
        src_msg=src_msg,
        dst_chat_id=dst_chat_id,
        bot_client=bot_client,
        ubot=ubot,
        uid=uid,
        source_channel=source_channel,
    )

    if new_dst_id:
        msg_id_map[src_msg_id] = new_dst_id
        await db.relink_fingerprints.update_one(
            {"uid": uid, "src_msg_id": src_msg_id},
            {"$set": {
                "uid": uid,
                "source_channel": source_channel,
                "src_msg_id": src_msg_id,
                "dst_msg_id": new_dst_id,
                "auto_mirrored": True,
            }},
            upsert=True,
        )
        logger.info(f"[AUTO-MIRROR] src={src_msg_id} → dst={new_dst_id}")
    return new_dst_id


async def process_auto_mirror_queue_at_start(
    uid: int,
    source_channel: str,
    dst_chat_id: int,
    msg_id_map: Dict[int, int],
    bot_client: Client,
    ubot: Optional[Client],
):
    """Process all pending mirror requests at batch start."""
    if ubot is None:
        logger.info("[QUEUE] ubot None — skipping queue processing")
        return
    db = get_db()
    try:
        cursor = db.auto_mirror_queue.find({
            "uid": uid,
            "source_channel": str(source_channel),
            "status": "pending",
        })
        pending = [doc async for doc in cursor]
    except Exception as e:
        logger.error(f"[QUEUE] Query failed: {e}")
        return

    if not pending:
        logger.info("[QUEUE] No pending auto-mirror items")
        return

    logger.info(f"[QUEUE] Processing {len(pending)} queued items")

    for item in pending:
        src_msg_id = item["src_msg_id"]
        broken_dst = item.get("broken_in_dst")

        new_dst_id = await handle_not_mirrored(
            uid=uid,
            source_channel=source_channel,
            src_msg_id=src_msg_id,
            dst_chat_id=item.get("dst_chat_id", dst_chat_id),
            dst_msg_id=broken_dst or 0,
            msg_id_map=msg_id_map,
            bot_client=bot_client,
            ubot=ubot,
        )
        if new_dst_id:
            # Fix the message that waited
            if broken_dst:
                await fix_single_message(
                    uid=uid,
                    source_channel=source_channel,
                    dst_channel_id=item["dst_chat_id"],
                    dst_msg_id=broken_dst,
                    msg_id_map=msg_id_map,
                    bot_client=bot_client,
                    ubot=ubot,
                )
            await db.auto_mirror_queue.update_one(
                {"_id": item["_id"]},
                {"$set": {"status": "done", "dst_msg_id": new_dst_id, "done_at": datetime.utcnow()}}
            )


# ────────────────────────────────────────────────────────────────
# 8. Periodic unresolved sweeper (optional background task)
# ────────────────────────────────────────────────────────────────
async def periodic_unresolved_sweeper(
    uid: int,
    source_channel: str,
    dst_chat_id: int,
    bot_client: Client,
    ubot: Optional[Client],
    interval: int = 3600,
):
    """Re‑check old unresolved links and fix them if target now mirrored."""
    db = get_db()
    while True:
        await asyncio.sleep(interval)
        try:
            cursor = db.unresolved_links.find({
                "uid": uid,
                "unresolved": True,
                "source_channel": str(source_channel),
            })
            async for doc in cursor:
                src_target = doc["unresolved_src_id"]
                existing = await db.relink_fingerprints.find_one({"uid": uid, "src_msg_id": src_target})
                if existing:
                    msg_id_map = await load_complete_map(uid, source_channel, dst_chat_id, bypass_cache=True)
                    await fix_single_message(
                        uid=uid,
                        source_channel=source_channel,
                        dst_channel_id=doc["dst_chat_id"],
                        dst_msg_id=doc["dst_msg_id"],
                        msg_id_map=msg_id_map,
                        bot_client=bot_client,
                        ubot=ubot,
                    )
                    await db.unresolved_links.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"unresolved": False, "resolved_at": datetime.utcnow(), "resolved_via": "periodic"}}
                    )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Periodic sweeper error: {e}")


# ────────────────────────────────────────────────────────────────
# 9. Plugin Manager
# ────────────────────────────────────────────────────────────────

class PluginManager:
    """Lightweight plugin loader for LinkRewriter hooks."""
    def __init__(self):
        self.plugins = []

    def register(self, plugin):
        self.plugins.append(plugin)

    async def on_startup(self, rewriter):
        for p in self.plugins:
            if hasattr(p, 'on_startup'):
                try:
                    await p.on_startup(rewriter)
                except Exception as e:
                    logger.warning(f"[PLUGIN] {p.__class__.__name__}.on_startup failed: {e}")

    async def before_batch(self, rewriter, messages):
        for p in self.plugins:
            if hasattr(p, 'before_batch'):
                try:
                    messages = await p.before_batch(rewriter, messages)
                except Exception as e:
                    logger.warning(f"[PLUGIN] {p.__class__.__name__}.before_batch failed: {e}")
        return messages

    async def after_send(self, rewriter, src_msg_id, dst_msg_id, unresolved):
        for p in self.plugins:
            if hasattr(p, 'after_send'):
                try:
                    await p.after_send(rewriter, src_msg_id, dst_msg_id, unresolved)
                except Exception as e:
                    logger.debug(f"[PLUGIN] {p.__class__.__name__}.after_send failed: {e}")

    async def on_shutdown(self, rewriter):
        for p in self.plugins:
            if hasattr(p, 'on_shutdown'):
                try:
                    await p.on_shutdown(rewriter)
                except Exception as e:
                    logger.debug(f"[PLUGIN] {p.__class__.__name__}.on_shutdown failed: {e}")


# ────────────────────────────────────────────────────────────────
# 10. Main LinkRewriter class (ULTRA interface)
# ────────────────────────────────────────────────────────────────
class LinkRewriter:
    """
    Instantiated once per batch session.
    Holds the complete map in memory and provides pre‑send rewriting
    plus post‑send self‑healing.
    """
    def __init__(
        self,
        uid: int,
        source_channel: str,
        dst_chat_id: int,
        bot_client: Client,
        ubot: Optional[Client],
        enable_periodic_sweep: bool = False,
        plugins: Optional[list] = None,
    ):
        self.uid = uid
        self.source_channel = str(source_channel)
        self.dst_chat_id = dst_chat_id
        self.bot_client = bot_client
        self.ubot = ubot
        self.msg_id_map: Dict[int, int] = {}
        self._sweep_task = None
        self._enable_sweep = enable_periodic_sweep
        # Plugin system
        self.plugin_manager = PluginManager()
        if plugins:
            for p in plugins:
                self.plugin_manager.register(p)

    async def startup(self):
        """Call ONCE at batch start. Loads map + processes queue + starts plugins."""
        try:
            await ensure_indexes()
        except Exception as e:
            logger.debug(f"ensure_indexes failed (non-fatal): {e}")

        self.msg_id_map = await load_complete_map(
            self.uid, self.source_channel, self.dst_chat_id
        )
        await process_auto_mirror_queue_at_start(
            uid=self.uid,
            source_channel=self.source_channel,
            dst_chat_id=self.dst_chat_id,
            msg_id_map=self.msg_id_map,
            bot_client=self.bot_client,
            ubot=self.ubot,
        )
        if self._enable_sweep:
            self._sweep_task = asyncio.create_task(
                periodic_unresolved_sweeper(
                    uid=self.uid,
                    source_channel=self.source_channel,
                    dst_chat_id=self.dst_chat_id,
                    bot_client=self.bot_client,
                    ubot=self.ubot,
                    interval=1800,  # every 30 minutes
                )
            )
        # Start plugins
        await self.plugin_manager.on_startup(self)
        logger.info(
            f"[LINK-REWRITER] Ready — map_size={len(self.msg_id_map)} "
            f"source={self.source_channel} "
            f"plugins={len(self.plugin_manager.plugins)}"
        )

    async def shutdown(self):
        """Cancel background tasks + shutdown plugins on batch end."""
        await self.plugin_manager.on_shutdown(self)
        if self._sweep_task:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass

    async def rewrite_before_send(
        self,
        text: Optional[str] = None,
        entities: Optional[List] = None,
    ) -> Tuple[Optional[str], Optional[List], List[int]]:
        """
        Call BEFORE sending.
        Returns (new_text, new_entities, unresolved_src_ids).
        """
        return await rewrite_message_links(
            text=text,
            entities=entities,
            source_channel=self.source_channel,
            dst_chat_id=self.dst_chat_id,
            msg_id_map=self.msg_id_map,
        )

    async def after_send(
        self,
        src_msg_id: int,
        dst_msg_id: int,
        unresolved_ids: List[int],
    ):
        """Call AFTER successful send. Updates map, fixes waiters, notifies plugins."""
        await on_message_mirrored(
            uid=self.uid,
            source_channel=self.source_channel,
            src_msg_id=src_msg_id,
            dst_msg_id=dst_msg_id,
            dst_chat_id=self.dst_chat_id,
            msg_id_map=self.msg_id_map,
            bot_client=self.bot_client,
            ubot=self.ubot,
        )
        if unresolved_ids:
            await save_unresolved_links(
                uid=self.uid,
                source_channel=self.source_channel,
                dst_chat_id=self.dst_chat_id,
                dst_msg_id=dst_msg_id,
                src_msg_id=src_msg_id,
                unresolved_ids=unresolved_ids,
            )
        # Notify plugins
        await self.plugin_manager.after_send(self, src_msg_id, dst_msg_id, unresolved_ids)

    async def handle_not_mirrored_manually(
        self,
        src_msg_id: int,
        dst_msg_id_with_broken_link: int,
    ) -> Optional[int]:
        """Public wrapper for auto‑mirroring a missing source message."""
        return await handle_not_mirrored(
            uid=self.uid,
            source_channel=self.source_channel,
            src_msg_id=src_msg_id,
            dst_chat_id=self.dst_chat_id,
            dst_msg_id=dst_msg_id_with_broken_link,
            msg_id_map=self.msg_id_map,
            bot_client=self.bot_client,
            ubot=self.ubot,
        )

    # Statistics
    async def get_stats(self) -> dict:
        db = get_db()
        unresolved_count = await db.unresolved_links.count_documents({
            "uid": self.uid,
            "unresolved": True,
            "source_channel": self.source_channel,
        })
        queue_count = await db.auto_mirror_queue.count_documents({
            "uid": self.uid,
            "source_channel": self.source_channel,
            "status": "pending",
        })
        return {
            "map_entries": len(self.msg_id_map),
            "unresolved_links": unresolved_count,
            "pending_mirrors": queue_count,
        }
