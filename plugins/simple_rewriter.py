# ╔══════════════════════════════════════════════════════════════════╗
# ║  SIMPLE ROBUST LINK REWRITER                                     ║
# ║                                                                  ║
# ║  HOW IT WORKS:                                                   ║
# ║  1. One dict: src_msg_id → dst_msg_id                            ║
# ║  2. Before send: rewrite text + entity URLs using dict           ║
# ║  3. After send: add new mapping to dict                          ║
# ║  4. Dict miss: mark unresolved, auto-fix when target is mirrored ║
# ║  5. Persist to MongoDB (fire-and-forget)                         ║
# ║                                                                  ║
# ║  MULTI-SOURCE: Supports cross-channel link rewriting.            ║
# ║  A message from channel A can contain links to channels B, C, D.║
# ║  All source channels' mappings are loaded into one combined map. ║
# ╚══════════════════════════════════════════════════════════════════╝

import asyncio
import copy
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def _clean_id(channel_id) -> str:
    """Any channel ID format → bare numeric string for t.me/c/ URLs."""
    s = str(channel_id).strip().lstrip("-")
    return s[3:] if s.startswith("100") else s


def _dest_url(dst_chat_id: int, dst_msg_id: int, dst_username: str = None) -> str:
    """Build destination URL. Uses public username if available, else private /c/ format."""
    if dst_username:
        return f"https://t.me/{dst_username}/{dst_msg_id}"
    return f"https://t.me/c/{_clean_id(dst_chat_id)}/{dst_msg_id}"


class SimpleRewriter:
    """
    Simple robust on-the-fly link rewriter.

    Replaces the legacy dual-function system (rewrite_telegram_links +
    rewrite_entity_urls) with ONE class, ONE dict, TWO hook points.

    Usage:
        r = SimpleRewriter(uid, source_channel, dst_chat_id, db, bot, ubot,
                           source_channel_username=username,
                           dst_channel_username=dst_username)
        await r.load()                           # once at batch start

        for src_msg in messages:
            text, ents, unresolved = r.rewrite(text, ents)  # before send
            sent = await bot.send_message(...)
            await r.record(src_msg.id, sent.id,     # after send
                           unresolved=unresolved,
                           dst_msg_id_of_unresolved=sent.id)

    MULTI-SOURCE:
        r.add_source_channel(channel_id, username=..., numeric_id=...)
        # All source channels' patterns are matched during rewrite()
    """

    def __init__(
        self,
        uid            : int,
        source_channel : str,
        dst_chat_id    : int,
        db             ,
        bot_client     ,
        ubot           ,
        source_channel_username : str = None,
        dst_channel_username    : str = None,
        dst_channel_id          : int = None,  # sometimes differs from dst_chat_id
        multi_source_channels   : list = None, # pre-built multi-source list
    ):
        self.uid            = uid
        self.src_channel    = str(source_channel)
        self.dst_chat_id    = dst_chat_id
        self.dst_channel_id = dst_channel_id or dst_chat_id
        self.dst_channel_username = dst_channel_username
        self.db             = db
        self.bot            = bot_client
        self.ubot           = ubot
        self.source_channel_username = source_channel_username

        # THE dict — src_msg_id → dst_msg_id
        self.map : dict = {}

        # Unresolved tracker — src_id → set(dst_msg_ids waiting for it)
        self._waiting : dict = {}   # src_id → set of dst_msg_ids
        self._fixing  : set  = set()  # dst_msg_ids currently being fixed (dedup)

        # ═══════════════════════════════════════════════════════
        # SOURCE PATTERNS — supports MULTI-SOURCE channels
        # Each entry: (pattern, label)
        # ═══════════════════════════════════════════════════════
        self._src_patterns = []  # list of (compiled_regex, channel_label)

        # Add primary source channel
        self._add_source_patterns(source_channel, source_channel_username)

        # Add multi-source channels if provided
        if multi_source_channels:
            for ch_info in multi_source_channels:
                ch = ch_info.get("channel", "")
                ch_username = ch_info.get("username")
                ch_numeric_id = ch_info.get("numeric_id")
                if ch and ch != str(source_channel):
                    self._add_source_patterns(ch, ch_username, ch_numeric_id)

        # Destination URL prefix for building replacement URLs
        self._dst_prefix = f"https://t.me/c/{_clean_id(self.dst_channel_id)}/"
        if dst_channel_username:
            self._dst_username_prefix = f"https://t.me/{dst_channel_username}/"
        else:
            self._dst_username_prefix = None

    def _add_source_patterns(self, channel: str, username: str = None, numeric_id=None):
        """Add URL-matching patterns for a source channel."""
        clean = _clean_id(channel)

        # Pattern 1: Private channel links — t.me/c/{clean_id}/{msg_id}
        if clean:
            pat = re.compile(
                r'(https?://t\.me/c/' + re.escape(clean) + r'/)(\d+)((?:/\d+)?)',
                re.IGNORECASE
            )
            self._src_patterns.append((pat, f'private:{clean}'))

        # Pattern 2: Public username links — t.me/{username}/{msg_id}
        ch_username = username
        if not ch_username and not str(channel).lstrip('-').isdigit():
            ch_username = str(channel)  # channel IS the username

        if ch_username:
            pat = re.compile(
                r'(https?://t\.me/' + re.escape(ch_username) + r'/)(\d+)((?:/\d+)?)',
                re.IGNORECASE
            )
            self._src_patterns.append((pat, f'public:{ch_username}'))

            # Pattern 3: tg://resolve deep links
            pat_tg = re.compile(
                r'(tg://resolve\?domain=' + re.escape(ch_username) + r'&post=)(\d+)((?:&[^)\s]*)?)',
                re.IGNORECASE
            )
            self._src_patterns.append((pat_tg, f'tg:{ch_username}'))

        # Also try numeric_id if different from channel string
        if numeric_id and str(numeric_id) != channel:
            clean2 = _clean_id(numeric_id)
            if clean2 and clean2 != clean:
                pat2 = re.compile(
                    r'(https?://t\.me/c/' + re.escape(clean2) + r'/)(\d+)((?:/\d+)?)',
                    re.IGNORECASE
                )
                self._src_patterns.append((pat2, f'private:{clean2}'))

        logger.debug(f"[REWRITER] Added source patterns for channel={channel} "
                     f"username={username} numeric_id={numeric_id} — "
                     f"total patterns: {len(self._src_patterns)}")

    def add_source_channel(self, channel: str, username: str = None, numeric_id=None):
        """Add an additional source channel for cross-channel link rewriting."""
        self._add_source_patterns(channel, username, numeric_id)

    # ── STEP 1: LOAD ─────────────────────────────────────────

    async def load(self):
        """
        Load ALL historical src→dst mappings into self.map at startup.
        Queries all MongoDB collections. Called ONCE before batch starts.
        After this, self.map has every mapping ever saved.
        """
        # Try all channel ID variants to handle format mismatches
        variants = list(set([
            self.src_channel,
            _clean_id(self.src_channel),
            f"-100{_clean_id(self.src_channel)}",
            f"-{_clean_id(self.src_channel)}",
        ]))

        # Source 1: upload_maps (main collection)
        for variant in variants:
            try:
                doc = await self.db["upload_maps"].find_one({
                    "user_id"       : self.uid,
                    "source_channel": variant,
                })
                if doc and "mappings" in doc:
                    for k, v in doc["mappings"].items():
                        try:
                            self.map[int(k)] = int(v)
                        except (ValueError, TypeError):
                            pass
                    if self.map:
                        break
            except Exception:
                continue

        loaded_so_far = len(self.map)

        # Source 2: fingerprints (backup)
        try:
            async for doc in self.db["relink_fingerprints"].find(
                {"uid": self.uid},
                {"src_msg_id": 1, "dst_msg_id": 1},
            ):
                src = doc.get("src_msg_id")
                dst = doc.get("dst_msg_id")
                if src and dst and src not in self.map:
                    try:
                        self.map[int(src)] = int(dst)
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

        # Source 3: mirrored_messages_index (supplementary)
        try:
            async for doc in self.db["mirrored_messages_index"].find(
                {"uid": self.uid, "dst_chat_id": self.dst_channel_id},
                {"src_msg_id": 1, "dst_msg_id": 1},
            ):
                src = doc.get("src_msg_id")
                dst = doc.get("dst_msg_id")
                if src and dst and src not in self.map:
                    try:
                        self.map[int(src)] = int(dst)
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

        # Source 4: Load mappings from multi-source channels
        # These are other channels the user has batched from
        for pat, label in self._src_patterns:
            # Extract the clean channel ID from the label
            if label.startswith('private:'):
                ch_clean = label.split(':')[1]
                ch_variants = [ch_clean, f"-100{ch_clean}", f"-{ch_clean}"]
                for ch_var in ch_variants:
                    if ch_var in variants:
                        continue  # Already loaded above
                    try:
                        doc = await self.db["upload_maps"].find_one({
                            "user_id"       : self.uid,
                            "source_channel": ch_var,
                        })
                        if doc and "mappings" in doc:
                            for k, v in doc["mappings"].items():
                                try:
                                    if int(k) not in self.map:
                                        self.map[int(k)] = int(v)
                                except (ValueError, TypeError):
                                    pass
                    except Exception:
                        continue

        print(
            f"[REWRITER-LOAD] Loaded {len(self.map)} mappings "
            f"(upload_maps={loaded_so_far}) "
            f"patterns={len(self._src_patterns)} "
            f"range=[{min(self.map) if self.map else 'N/A'}"
            f"..{max(self.map) if self.map else 'N/A'}]"
        )

    # ── STEP 2: REWRITE ──────────────────────────────────────

    def rewrite(
        self,
        text     : str,
        entities : list,
    ) -> tuple:
        """
        Rewrite all source channel links in text and entities.
        Sync — uses only self.map (no DB calls here).
        Returns (new_text, new_entities, unresolved_src_ids).

        Call this BEFORE every send.
        """
        unresolved = []
        new_text   = text or ""

        def _make_replacer(dst_prefix):
            """Create a replacement function for a given destination prefix."""
            def _replace(match):
                src_id   = int(match.group(2))
                thread   = match.group(3) or ""
                dst_id   = self.map.get(src_id)
                if dst_id:
                    return dst_prefix + str(dst_id) + thread
                else:
                    if src_id not in unresolved:
                        unresolved.append(src_id)
                    return match.group(0)
            return _replace

        # ── Rewrite bare URLs in text ─────────────────────────
        if new_text:
            for pattern, label in self._src_patterns:
                if label.startswith('tg:'):
                    # tg://resolve links — replace entire URL with t.me format
                    def _make_tg_replacer(dst_prefix):
                        def _replace_tg(match):
                            src_id = int(match.group(2))
                            dst_id = self.map.get(src_id)
                            if dst_id:
                                return dst_prefix + str(dst_id)
                            else:
                                if src_id not in unresolved:
                                    unresolved.append(src_id)
                                return match.group(0)
                        return _replace_tg
                    new_text = pattern.sub(_make_tg_replacer(self._dst_prefix), new_text)
                else:
                    new_text = pattern.sub(
                        _make_replacer(
                            self._dst_username_prefix or self._dst_prefix
                        ),
                        new_text,
                    )

        # ── Rewrite TEXT_LINK entity URLs ─────────────────────
        new_entities = []
        if entities:
            for entity in entities:
                new_e = copy.deepcopy(entity)
                etype = str(getattr(entity, "type", "")).lower()

                if "text_link" in etype:
                    url = getattr(entity, "url", "") or ""
                    url_changed = False
                    for pattern, label in self._src_patterns:
                        match = pattern.search(url)
                        if match:
                            src_id = int(match.group(2))
                            thread = match.group(3) if len(match.groups()) > 2 else ""
                            dst_id = self.map.get(src_id)
                            if dst_id:
                                if label.startswith('tg:'):
                                    new_url = self._dst_prefix + str(dst_id)
                                else:
                                    dst_prefix = self._dst_username_prefix or self._dst_prefix
                                    new_url = dst_prefix + str(dst_id) + (thread or "")
                                # Replace the matched portion in the URL
                                new_e.url = pattern.sub(
                                    lambda m, u=new_url: u, url
                                )
                                url_changed = True
                            else:
                                if src_id not in unresolved:
                                    unresolved.append(src_id)
                            break  # Only match first pattern

                elif etype == 'url':
                    # 'url' entities: the URL is embedded in the text itself
                    # We need raw_text to extract it — handled by caller
                    # For now, just pass through
                    pass

                new_entities.append(new_e)

        # ── Rewrite 'url' type entities ───────────────────────
        # These are bare URLs embedded in text. The text itself needs rewriting,
        # which we already did above. But we should also convert them to
        # text_link entities if the URL was rewritten.
        # This is handled by the text rewriter above + entity offset adjustments.

        if unresolved:
            print(
                f"[REWRITER-REWRITE] {len(unresolved)} unresolved src_ids: {unresolved[:5]}"
            )

        # Log how many links were rewritten (resolved)
        _resolved = len([1 for p, _ in self._src_patterns if p.search(text or '')]) if text else 0
        if _resolved and not unresolved:
            print(f"[REWRITER-REWRITE] All links resolved for text snippet: {(text or '')[:80]}")

        return new_text, new_entities, unresolved

    def rewrite_inline_keyboard(self, reply_markup):
        """
        Rewrite URLs inside inline keyboard buttons.
        Returns (new_reply_markup, unresolved_src_ids).
        """
        if not reply_markup:
            return reply_markup, []

        unresolved = []

        try:
            from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

            if not isinstance(reply_markup, InlineKeyboardMarkup):
                return reply_markup, []

            new_rows = []
            for row in reply_markup.inline_keyboard:
                new_buttons = []
                for button in row:
                    new_btn = copy.deepcopy(button)
                    url = getattr(new_btn, 'url', None)
                    if url:
                        for pattern, label in self._src_patterns:
                            match = pattern.search(url)
                            if match:
                                src_id = int(match.group(2))
                                thread = match.group(3) if len(match.groups()) > 2 else ""
                                dst_id = self.map.get(src_id)
                                if dst_id:
                                    dst_prefix = self._dst_username_prefix or self._dst_prefix
                                    new_url = dst_prefix + str(dst_id) + (thread or "")
                                    new_btn.url = pattern.sub(lambda m, u=new_url: u, url)
                                else:
                                    if src_id not in unresolved:
                                        unresolved.append(src_id)
                                break  # Only match first pattern
                    new_buttons.append(new_btn)
                new_rows.append(new_buttons)

            new_markup = InlineKeyboardMarkup(new_rows)
            return new_markup, unresolved

        except Exception as e:
            logger.debug(f"[REWRITER] rewrite_inline_keyboard failed: {e}")
            return reply_markup, []

    # ── STEP 3: RECORD ───────────────────────────────────────

    async def record(
        self,
        src_msg_id    : int,
        dst_msg_id    : int,
        unresolved    : list = None,
        dst_msg_id_of_unresolved : int = None,
    ):
        """
        Call AFTER every successful send.
        1. Adds src→dst to self.map
        2. Checks if any messages were waiting for this src_msg_id
        3. Fixes them immediately (auto-fix)
        4. Saves unresolved links for later
        5. Persists mapping to MongoDB

        dst_msg_id_of_unresolved: the dest message that has broken links
        """
        # Add to map
        self.map[src_msg_id] = dst_msg_id
        print(f"[REWRITER-RECORD] src={src_msg_id} → dst={dst_msg_id} | map_size={len(self.map)} | unresolved={unresolved} | waiting_keys={list(self._waiting.keys())[:5]}")

        # Check if any messages were waiting for this src_msg_id → AUTO-FIX
        waiting = self._waiting.pop(src_msg_id, set())
        for waiting_dst_id in waiting:
            if waiting_dst_id not in self._fixing:
                asyncio.create_task(
                    self._fix_message(waiting_dst_id)
                )
                print(f"[REWRITER-AUTOFIX] Triggered! dst={waiting_dst_id} was waiting for src={src_msg_id}")

        # Save new unresolved links — these messages will be fixed
        # when the target src_msg_id is eventually mirrored
        if unresolved and dst_msg_id_of_unresolved:
            for un_id in unresolved:
                if un_id not in self._waiting:
                    self._waiting[un_id] = set()
                self._waiting[un_id].add(dst_msg_id_of_unresolved)
            print(f"[REWRITER-WAITING] {len(unresolved)} unresolved src_ids registered | waiting_for={unresolved[:5]} from dst={dst_msg_id_of_unresolved}")

            # Persist to MongoDB
            asyncio.create_task(
                self._save_unresolved(
                    dst_msg_id  = dst_msg_id_of_unresolved,
                    src_msg_id  = src_msg_id,
                    unresolved  = unresolved,
                )
            )

        # Save mapping to MongoDB (fire and forget)
        asyncio.create_task(self._save_mapping(src_msg_id, dst_msg_id))

    # ── INTERNAL: FIX A WAITING MESSAGE ──────────────────────

    async def _fix_message(self, dst_msg_id: int):
        """
        Fetch a destination message and rewrite any remaining source links.
        Called when a previously-unresolved target is now in self.map.
        """
        if dst_msg_id in self._fixing:
            return  # dedup — already being fixed
        self._fixing.add(dst_msg_id)

        try:
            # Fetch message
            msg = None
            for client in [self.ubot, self.bot]:
                if client is None:
                    continue
                try:
                    msg = await client.get_messages(self.dst_chat_id, dst_msg_id)
                    if msg and not getattr(msg, "empty", True):
                        break
                except Exception:
                    continue

            if not msg:
                return

            text      = str(msg.text    or "")
            caption   = str(msg.caption or "")
            is_cap    = bool(msg.caption) and not bool(msg.text)
            current   = caption if is_cap else text
            entities  = (
                msg.caption_entities if is_cap else msg.entities
            ) or []

            new_text, new_entities, still_unresolved = self.rewrite(current, entities)

            if new_text == current:
                return   # nothing changed

            # Edit the message
            for client in [self.ubot, self.bot]:
                if client is None:
                    continue
                try:
                    if not is_cap:
                        await client.edit_message_text(
                            chat_id    = self.dst_chat_id,
                            message_id = dst_msg_id,
                            text       = new_text,
                            entities   = new_entities or None,
                        )
                    else:
                        await client.edit_message_caption(
                            chat_id          = self.dst_chat_id,
                            message_id       = dst_msg_id,
                            caption          = new_text,
                            caption_entities = new_entities or None,
                        )
                    logger.info(f"[REWRITER] Fixed dst={dst_msg_id}")
                    return

                except Exception as ex:
                    from pyrogram.errors import MessageNotModified, FloodWait
                    if isinstance(ex, MessageNotModified):
                        return
                    if isinstance(ex, FloodWait):
                        await asyncio.sleep(ex.value + 2)
                    elif "AUTHOR_REQUIRED" in str(ex):
                        continue
                    else:
                        logger.debug(f"[REWRITER] edit failed: {ex}")
                        continue

        finally:
            self._fixing.discard(dst_msg_id)

    # ── INTERNAL: SAVE TO MONGODB ─────────────────────────────

    async def _save_mapping(self, src_id: int, dst_id: int):
        """Save one src→dst mapping to all MongoDB collections."""
        try:
            await self.db["upload_maps"].update_one(
                {
                    "user_id"       : self.uid,
                    "source_channel": self.src_channel,
                },
                {"$set": {f"mappings.{src_id}": dst_id}},
                upsert=True,
            )
        except Exception as e:
            logger.debug(f"[REWRITER] save_mapping failed: {e}")

    async def _save_unresolved(
        self,
        dst_msg_id : int,
        src_msg_id : int,
        unresolved : list,
    ):
        """Save unresolved links so /relink can fix them later."""
        try:
            docs = [
                {
                    "uid"               : self.uid,
                    "source_channel"    : self.src_channel,
                    "dst_chat_id"       : self.dst_chat_id,
                    "dst_msg_id"        : dst_msg_id,
                    "src_msg_id"        : src_msg_id,
                    "unresolved_src_id" : un_id,
                    "unresolved"        : True,
                    "created_at"        : datetime.utcnow(),
                }
                for un_id in unresolved
            ]
            if docs:
                await self.db["unresolved_links"].insert_many(
                    docs, ordered=False
                )
        except Exception as e:
            logger.debug(f"[REWRITER] save_unresolved failed: {e}")

    # ── UTILITY: Get reply destination ────────────────────────

    def get_reply_dest(self, src_reply_to_id: int) -> int | None:
        """
        Look up the destination message ID for a source reply-to ID.
        Uses self.map (no DB call).
        Returns None if not found.
        """
        if src_reply_to_id is None:
            return None
        return self.map.get(src_reply_to_id)
