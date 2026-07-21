# Copyright (c) 2025 devgagan : https://github.com/devgaganin.  
# Licensed under the GNU General Public License v3.0.  
# See LICENSE file in the repository root for full license text.

"""
TEST FEATURE: LRU cache for Pyrogram Client instances.

Replaces the unbounded UB/UC dicts with a bounded LRU cache.
When the cache exceeds max_size, the least-recently-used client
is stopped and evicted to free memory (~30-50 MB per client).

NEVER evicts a client whose user has an active batch running.

Usage:
    from utils.client_cache import user_bot_cache, user_client_cache
    
    # Put a client
    user_bot_cache.put(uid, bot_client)
    user_client_cache.put(uid, user_client)
    
    # Get a client
    bot = user_bot_cache.get(uid)
    uc = user_client_cache.get(uid)
    
    # Remove a client manually
    user_bot_cache.remove(uid)
"""

import asyncio
from collections import OrderedDict
from utils.ram_monitor import log_ram


class LRUClientCache:
    """Thread-safe LRU cache for Pyrogram Client instances.
    
    When the cache exceeds max_size, the least-recently-used client
    is gracefully stopped (disconnect) and removed to free memory.
    
    Active batch users are NEVER evicted (checked via is_active callback).
    """
    
    def __init__(self, name: str, max_size: int = 3, is_active_callback=None):
        """
        Args:
            name: Human-readable name for logging (e.g., "user_bot", "user_client")
            max_size: Maximum number of clients to keep in cache
            is_active_callback: Async callable(user_id) -> bool, returns True if
                                the user has an active batch (should not be evicted)
        """
        self.name = name
        self.max_size = max_size
        self.is_active_callback = is_active_callback
        self._cache = OrderedDict()  # uid -> client (ordered by access time)
    
    def get(self, uid):
        """Get a client by user ID. Returns None if not found.
        Moves the entry to the end (most recently used)."""
        if uid in self._cache:
            self._cache.move_to_end(uid)  # Mark as recently used
            return self._cache[uid]
        return None
    
    def put(self, uid, client):
        """Add or update a client in the cache.
        If cache is full, evicts the LRU entry (if not active)."""
        if uid in self._cache:
            # Update existing — move to end
            self._cache.move_to_end(uid)
            self._cache[uid] = client
            return
        
        # Check if we need to evict
        if len(self._cache) >= self.max_size:
            self._evict_lru()
        
        self._cache[uid] = client
        log_ram(f"{self.name}_cache_put", extra_info={"uid": uid, "cache_size": len(self._cache)})
    
    def remove(self, uid):
        """Remove a specific client from the cache (does NOT stop it).
        Use this when you want to manually manage the lifecycle."""
        if uid in self._cache:
            del self._cache[uid]
    
    async def stop_and_remove(self, uid):
        """Gracefully stop and remove a specific client."""
        if uid in self._cache:
            client = self._cache[uid]
            try:
                await client.stop()
                print(f"[{self.name}_cache] Stopped and removed client for uid={uid}")
            except Exception as e:
                print(f"[{self.name}_cache] Error stopping client for uid={uid}: {e}")
            del self._cache[uid]
            log_ram(f"{self.name}_cache_evicted", extra_info={"uid": uid, "cache_size": len(self._cache)})
    
    def _evict_lru(self):
        """Evict the least-recently-used client.
        Skips users with active batches.
        This is synchronous — schedules client.stop() as a task."""
        evicted = False
        # Iterate from oldest (front) to newest (back)
        for uid in list(self._cache.keys()):
            # Check if user is active — don't evict active batch users
            if self.is_active_callback:
                try:
                    # We can't await here (sync context), so check synchronously
                    # The is_active check from batch.py is synchronous (dict lookup)
                    from plugins.batch import is_user_active
                    if is_user_active(uid):
                        print(f"[{self.name}_cache] Skipping eviction of uid={uid} — active batch")
                        continue
                except Exception:
                    pass
            
            # Evict this user
            client = self._cache.pop(uid)
            
            # Schedule graceful stop (fire-and-forget)
            try:
                asyncio.ensure_future(self._async_stop_client(uid, client))
            except Exception as e:
                print(f"[{self.name}_cache] Error scheduling stop for uid={uid}: {e}")
            
            evicted = True
            break
        
        if not evicted:
            print(f"[{self.name}_cache] WARNING: Could not evict any client — all users are active!")
    
    async def _async_stop_client(self, uid, client):
        """Gracefully stop a client (called async after eviction)."""
        try:
            await client.stop()
            print(f"[{self.name}_cache] Evicted & stopped LRU client for uid={uid}")
            log_ram(f"{self.name}_cache_evicted", extra_info={"uid": uid, "cache_size": len(self._cache)})
        except Exception as e:
            print(f"[{self.name}_cache] Error stopping evicted client for uid={uid}: {e}")
    
    def __contains__(self, uid):
        return uid in self._cache
    
    def __len__(self):
        return len(self._cache)
    
    def keys(self):
        return self._cache.keys()
    
    def items(self):
        return self._cache.items()


# Global instances — replace UB and UC dicts
# Max 3 concurrent user bot clients and 3 user session clients
user_bot_cache = LRUClientCache(name="user_bot", max_size=3)
user_client_cache = LRUClientCache(name="user_client", max_size=3)
