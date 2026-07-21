# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

"""
MongoDict — A dict-like interface backed by MongoDB with an in-process LRU cache.

Purpose:
  Replace the 19K+ entry plain-Python dicts (msg_id_map) that consume ~985MB RAM
  on Heroku with a lightweight LRU cache (default 1000 entries) backed by MongoDB.

Key design decisions:
  - Sync methods (get, __getitem__, __setitem__, __contains__, keys(), items(), len())
    operate on the LRU cache + pending_writes ONLY. This is safe because during
    batch processing every new mapping is written to cache + pending_writes first.
  - For lookups of older mappings that may have been evicted from cache, use
    `await aget(key)` which checks cache → pending → MongoDB.
  - `aflush()` writes all pending_writes to MongoDB and clears the pending list.
  - The "initial keys / new_mappings" flush pattern from the old code is replaced
    by pending_writes tracking — MongoDict already knows which entries are new.
"""

import logging
from collections import OrderedDict
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME

logger = logging.getLogger(__name__)

# Module-level MongoDB client (shared across MongoDict instances)
_mongo_client = AsyncIOMotorClient(MONGO_URI)
_db = _mongo_client[DB_NAME]
_upload_maps_collection = _db["upload_maps"]
_mirrored_messages_index = _db["mirrored_messages_index"]
_relink_fingerprints = _db["relink_fingerprints"]


class MongoDict:
    """Dict-like object backed by MongoDB with LRU cache.

    Supports the same interface patterns used in batch.py:
      - msg_id_map[mid] = dest_id     → __setitem__
      - msg_id_map.get(key)            → get (cache + pending only)
      - key in msg_id_map              → __contains__ (cache + pending only)
      - len(msg_id_map)                → __len__ (cache + pending only)
      - msg_id_map[key]                → __getitem__ (cache + pending only)
      - msg_id_map.items()             → items (cache + pending only)
      - msg_id_map.keys()              → keys (cache + pending only)

    For cross-process or evicted-entry lookups, use async methods:
      - await msg_id_map.aget(key)     → checks cache → pending → MongoDB
      - await msg_id_map.aflush()      → flush pending_writes to MongoDB
      - await msg_id_map.aload_from_upload_maps(limit=N) → pre-load recent mappings
    """

    def __init__(self, uid, source_channel, dest_channel_id=None, max_cache=1000):
        """
        Args:
            uid: User ID (int)
            source_channel: Source channel identifier (str), e.g. "-1002563279588"
            dest_channel_id: Destination channel ID (int or None)
            max_cache: Maximum number of entries in the LRU cache
        """
        self.uid = uid
        self.source_channel = str(source_channel)
        self.dest_channel_id = dest_channel_id
        self.max_cache = max_cache

        # LRU cache — OrderedDict for O(1) eviction
        self._cache = OrderedDict()  # {int_key: value}

        # Pending writes — entries not yet flushed to MongoDB
        self._pending_writes = {}  # {int_key: value}

        # Metadata loaded from MongoDB
        self._last_src_id = 0
        self._stored_dest_channel = None
        self._total_count = 0  # Total entries in MongoDB (approximate)
        self._metadata_loaded = False

    # ────────────────────────────────────────────────────────────────
    # Sync dict-like interface (operates on cache + pending only)
    # ────────────────────────────────────────────────────────────────

    def __setitem__(self, key, value):
        """Write a mapping. Stored in cache + pending_writes."""
        int_key = int(key)
        self._cache[int_key] = value
        self._pending_writes[int_key] = value
        # Move to end (most recently used)
        self._cache.move_to_end(int_key)
        # Evict oldest if over limit
        while len(self._cache) > self.max_cache:
            self._cache.popitem(last=False)
        # Track highest source ID
        if int_key > self._last_src_id:
            self._last_src_id = int_key

    def __getitem__(self, key):
        """Get a mapping. Checks cache then pending. Raises KeyError if missing."""
        int_key = int(key)
        if int_key in self._cache:
            self._cache.move_to_end(int_key)
            return self._cache[int_key]
        if int_key in self._pending_writes:
            return self._pending_writes[int_key]
        raise KeyError(int_key)

    def get(self, key, default=None):
        """Get a mapping with default. Checks cache then pending."""
        int_key = int(key)
        if int_key in self._cache:
            self._cache.move_to_end(int_key)
            return self._cache[int_key]
        if int_key in self._pending_writes:
            return self._pending_writes[int_key]
        return default

    def __contains__(self, key):
        """Check if key exists. Checks cache then pending."""
        int_key = int(key)
        return int_key in self._cache or int_key in self._pending_writes

    def __len__(self):
        """Return approximate total count: cache + pending (deduplicated).
        
        Note: This returns the count of unique keys in cache + pending_writes.
        During batch processing this is accurate because new entries are added
        to both cache and pending. For the full MongoDB count, use `await atotal_count()`.
        """
        # Merge keys from cache and pending (pending is a superset of new entries)
        all_keys = set(self._cache.keys()) | set(self._pending_writes.keys())
        return len(all_keys)

    @property
    def cache_size(self):
        """Number of entries currently in the LRU cache."""
        return len(self._cache)

    def _add_to_cache(self, key, value):
        """Add to LRU cache ONLY — does NOT add to pending_writes.
        
        FIX #8: Use this when loading data that ALREADY EXISTS in MongoDB
        to avoid duplicate pending writes (data would be re-written on flush).
        
        Also used by aload_from_upload_maps to populate cache from DB
        without marking entries as "new" writes.
        """
        int_key = int(key)
        self._cache[int_key] = value
        self._cache.move_to_end(int_key)
        while len(self._cache) > self.max_cache:
            self._cache.popitem(last=False)
        if int_key > self._last_src_id:
            self._last_src_id = int_key

    def keys(self):
        """Return all keys in cache + pending (deduplicated)."""
        return set(self._cache.keys()) | set(self._pending_writes.keys())

    def items(self):
        """Return all items in cache + pending (deduplicated, pending takes precedence)."""
        merged = {}
        # Cache entries first
        for k, v in self._cache.items():
            merged[k] = v
        # Pending entries override (they're newer)
        for k, v in self._pending_writes.items():
            merged[k] = v
        return merged.items()

    def values(self):
        """Return all values in cache + pending (deduplicated)."""
        merged = {}
        for k, v in self._cache.items():
            merged[k] = v
        for k, v in self._pending_writes.items():
            merged[k] = v
        return merged.values()

    # ────────────────────────────────────────────────────────────────
    # Properties
    # ────────────────────────────────────────────────────────────────

    @property
    def last_src_id(self):
        """The highest source message ID written so far."""
        return self._last_src_id

    @property
    def stored_dest_channel(self):
        """The destination channel stored in MongoDB metadata."""
        return self._stored_dest_channel

    @property
    def pending_count(self):
        """Number of pending writes not yet flushed to MongoDB."""
        return len(self._pending_writes)

    # ────────────────────────────────────────────────────────────────
    # Async methods (MongoDB access)
    # ────────────────────────────────────────────────────────────────

    async def aget(self, key, default=None):
        """Get a mapping with MongoDB fallback.
        
        Checks: cache → pending → upload_maps → mirrored_messages_index → fingerprints.
        Falls back to multiple MongoDB collections for maximum coverage.
        """
        int_key = int(key)
        # Check cache first
        if int_key in self._cache:
            self._cache.move_to_end(int_key)
            return self._cache[int_key]
        # Check pending
        if int_key in self._pending_writes:
            return self._pending_writes[int_key]
        
        # Check upload_maps (primary source)
        doc = await _upload_maps_collection.find_one(
            {"user_id": self.uid, "source_channel": self.source_channel},
            {f"mappings.{int_key}": 1}
        )
        if doc and "mappings" in doc and str(int_key) in doc["mappings"]:
            val = doc["mappings"][str(int_key)]
            self._cache[int_key] = val
            self._cache.move_to_end(int_key)
            while len(self._cache) > self.max_cache:
                self._cache.popitem(last=False)
            return val
        
        # Fallback: mirrored_messages_index (cross-channel mappings)
        idx_doc = await _mirrored_messages_index.find_one(
            {"uid": self.uid, "src_msg_id": int_key},
            {"dst_msg_id": 1, "_id": 0}
        )
        if idx_doc and idx_doc.get("dst_msg_id"):
            val = idx_doc["dst_msg_id"]
            self._cache[int_key] = val
            self._cache.move_to_end(int_key)
            while len(self._cache) > self.max_cache:
                self._cache.popitem(last=False)
            return val
        
        # Fallback: relink_fingerprints
        fp_doc = await _relink_fingerprints.find_one(
            {"uid": self.uid, "src_msg_id": int_key},
            {"dst_msg_id": 1, "_id": 0}
        )
        if fp_doc and fp_doc.get("dst_msg_id"):
            val = fp_doc["dst_msg_id"]
            self._cache[int_key] = val
            self._cache.move_to_end(int_key)
            while len(self._cache) > self.max_cache:
                self._cache.popitem(last=False)
            return val
        
        return default

    async def apreload_keys(self, keys):
        """Pre-load specific keys into cache from MongoDB.
        
        For each key, checks cache → pending → MongoDB (all fallback collections).
        Only queries MongoDB for keys not already in cache/pending.
        
        Args:
            keys: Iterable of keys (ints or strings) to pre-load.
        
        Returns:
            Number of new keys loaded from MongoDB.
        """
        if not keys:
            return 0
        
        # Filter to keys not already in cache or pending
        missing_keys = []
        for k in keys:
            int_k = int(k)
            if int_k not in self._cache and int_k not in self._pending_writes:
                missing_keys.append(int_k)
        
        if not missing_keys:
            return 0
        
        loaded = 0
        
        # Batch query: upload_maps — get all mappings at once
        source_str = getattr(self, '_resolved_channel_variant', self.source_channel)
        doc = await _upload_maps_collection.find_one(
            {"user_id": self.uid, "source_channel": source_str},
            {"mappings": 1}
        )
        if doc and "mappings" in doc:
            mappings = doc["mappings"]
            for int_k in missing_keys:
                str_k = str(int_k)
                if str_k in mappings:
                    self._cache[int_k] = mappings[str_k]
                    loaded += 1
        
        # For keys still not found, try mirrored_messages_index one-by-one
        still_missing = [k for k in missing_keys if k not in self._cache]
        if still_missing:
            for int_k in still_missing:
                idx_doc = await _mirrored_messages_index.find_one(
                    {"uid": self.uid, "src_msg_id": int_k},
                    {"dst_msg_id": 1, "_id": 0}
                )
                if idx_doc and idx_doc.get("dst_msg_id"):
                    self._cache[int_k] = idx_doc["dst_msg_id"]
                    loaded += 1
        
        # For keys still not found, try fingerprints one-by-one
        still_missing = [k for k in missing_keys if k not in self._cache]
        if still_missing:
            for int_k in still_missing:
                fp_doc = await _relink_fingerprints.find_one(
                    {"uid": self.uid, "src_msg_id": int_k},
                    {"dst_msg_id": 1, "_id": 0}
                )
                if fp_doc and fp_doc.get("dst_msg_id"):
                    self._cache[int_k] = fp_doc["dst_msg_id"]
                    loaded += 1
        
        # Rebuild LRU order and trim
        if loaded > 0:
            new_cache = OrderedDict()
            for k in sorted(self._cache.keys()):
                new_cache[k] = self._cache[k]
            self._cache = new_cache
            while len(self._cache) > self.max_cache:
                self._cache.popitem(last=False)
        
        if loaded > 0:
            logger.info(f"[MongoDict] uid={self.uid} ch={self.source_channel} "
                        f"pre-loaded {loaded}/{len(missing_keys)} requested keys from DB")
        
        return loaded

    async def aload_metadata(self):
        """Load metadata (last_uploaded_source_id, dest_channel, total count) from MongoDB.
        
        Tries ALL possible channel ID format variants, same as load_upload_map().
        """
        source_str = self.source_channel

        # Build ALL possible channel ID format variants
        channel_variants = set()
        channel_variants.add(source_str)
        s = source_str.strip()
        channel_variants.add(s.lstrip("-"))
        if not s.startswith("-100"):
            channel_variants.add(f"-100{s.lstrip('-')}")
        if s.startswith("-100"):
            channel_variants.add(s[4:])
        clean = s.lstrip('-')
        if clean.startswith('100'):
            channel_variants.add(clean[3:])

        for variant in channel_variants:
            doc = await _upload_maps_collection.find_one(
                {"user_id": self.uid, "source_channel": variant},
                {"mappings": 0}  # Exclude mappings blob — we only want metadata
            )
            if doc:
                self._last_src_id = doc.get("last_uploaded_source_id", 0)
                self._stored_dest_channel = doc.get("dest_channel")
                self._total_count = doc.get("total_uploaded", 0)
                self._metadata_loaded = True
                # Store the variant that matched for future queries
                if variant != source_str:
                    self._resolved_channel_variant = variant
                return

        # No document found — defaults
        self._last_src_id = 0
        self._stored_dest_channel = None
        self._total_count = 0
        self._metadata_loaded = True

    async def aload_from_upload_maps(self, limit=500):
        """Pre-load the most recent mappings from MongoDB into cache.
        
        Loads the N most recent entries (by key = source message ID, descending).
        Also loads metadata if not already loaded.
        """
        # Load metadata first if not loaded
        if not self._metadata_loaded:
            await self.aload_metadata()

        # Determine which channel variant to use
        source_str = getattr(self, '_resolved_channel_variant', self.source_channel)

        # Find the document
        doc = await _upload_maps_collection.find_one(
            {"user_id": self.uid, "source_channel": source_str}
        )

        if not doc or "mappings" not in doc:
            return

        mappings = doc.get("mappings", {})
        if not mappings:
            return

        # Sort by key (int) descending and take top N
        int_keys = sorted([int(k) for k in mappings.keys()], reverse=True)
        keys_to_load = int_keys[:limit]

        loaded = 0
        for k in keys_to_load:
            v = mappings[str(k)]
            # FIX #8: Use _add_to_cache instead of direct assignment
            # This avoids adding to pending_writes (data already in MongoDB)
            self._add_to_cache(k, v)
            loaded += 1

        # Cache is already sorted by _add_to_cache (move_to_end)
        # But let's rebuild for consistent ordering: lowest first, highest last
        new_cache = OrderedDict()
        for k in sorted(self._cache.keys()):
            new_cache[k] = self._cache[k]
        self._cache = new_cache

        # Trim cache if over limit
        while len(self._cache) > self.max_cache:
            self._cache.popitem(last=False)  # Remove oldest (lowest key)

        # Also load metadata from doc
        if "last_uploaded_source_id" in doc:
            self._last_src_id = doc["last_uploaded_source_id"]
        if "dest_channel" in doc:
            self._stored_dest_channel = doc["dest_channel"]

        logger.info(f"[MongoDict] uid={self.uid} ch={self.source_channel} "
                    f"pre-loaded {loaded} mappings into cache "
                    f"(total in DB: {len(mappings)}, cache_size={len(self._cache)})")

    async def aflush(self, dest_channel=None):
        """Flush all pending writes to MongoDB.
        
        Uses the same merge strategy as save_upload_map_incremental():
          - $set last_uploaded_source_id and dest_channel
          - $inc total_uploaded
          - $set individual mapping entries with dotted keys
        
        After flush, pending_writes are cleared.
        
        Args:
            dest_channel: Override dest_channel_id for this flush.
                         If None, uses self.dest_channel_id.
        """
        if not self._pending_writes:
            return 0

        dest_ch = dest_channel if dest_channel is not None else self.dest_channel_id
        new_mappings = dict(self._pending_writes)
        str_mappings = {str(k): v for k, v in new_mappings.items()}
        last_src_id = self._last_src_id

        # Update metadata
        update_ops = {
            "$set": {
                "last_uploaded_source_id": last_src_id,
                "updated_at": datetime.now()
            },
            "$inc": {"total_uploaded": len(new_mappings)},
        }
        if dest_ch is not None:
            update_ops["$set"]["dest_channel"] = dest_ch

        source_str = getattr(self, '_resolved_channel_variant', self.source_channel)

        await _upload_maps_collection.update_one(
            {"user_id": self.uid, "source_channel": source_str},
            update_ops,
            upsert=True
        )

        # Merge mappings separately (MongoDB $set with dotted keys)
        if str_mappings:
            await _upload_maps_collection.update_one(
                {"user_id": self.uid, "source_channel": source_str},
                {"$set": {f"mappings.{k}": v for k, v in str_mappings.items()}}
            )

        count = len(self._pending_writes)
        self._pending_writes.clear()
        logger.info(f"[MongoDict] uid={self.uid} ch={self.source_channel} "
                    f"flushed {count} pending writes to MongoDB")
        return count

    async def atotal_count(self):
        """Get the total number of mappings in MongoDB + pending."""
        source_str = getattr(self, '_resolved_channel_variant', self.source_channel)
        doc = await _upload_maps_collection.find_one(
            {"user_id": self.uid, "source_channel": source_str},
            {"mappings": 1}
        )
        db_count = len(doc.get("mappings", {})) if doc else 0
        # Add pending that aren't in DB yet
        pending_new = len(self._pending_writes)
        return db_count + pending_new

    def clear_pending(self):
        """Clear pending writes without flushing. Use with caution."""
        self._pending_writes.clear()

    def __bool__(self):
        """Truthiness: True if any entries exist in cache or pending."""
        return bool(self._cache) or bool(self._pending_writes)

    def __repr__(self):
        return (f"MongoDict(uid={self.uid}, ch={self.source_channel}, "
                f"cache={len(self._cache)}, pending={len(self._pending_writes)}, "
                f"last_src_id={self._last_src_id})")
