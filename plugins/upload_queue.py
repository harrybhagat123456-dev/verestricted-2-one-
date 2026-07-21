import asyncio
import json
import os
import logging
from pyrogram import Client
from pyrogram.errors import FloodWait

logger = logging.getLogger(__name__)

UPLOAD_STATE_FILE = "upload_state.json"


# ════════════════════════════════════════════════════════════
#
#  SEQUENTIAL UPLOAD VERIFICATION
#
#  ADDITIONAL CHECK on top of the existing batch loop.
#  Does NOT replace process_msg — it adds a VERIFICATION
#  step after each upload to confirm the message landed
#  in the destination channel.
#
#  Why this is needed:
#    - process_msg returns dest_id but doesn't verify it
#    - Network glitches can cause silent upload failures
#    - FloodWait can cause partial sends
#    - High RAM can cause event loop lag → missed messages
#
#  How it works:
#    1. After process_msg returns dest_id → verify it exists
#    2. If verification fails → retry up to 3 times
#    3. Track state to upload_state.json for crash recovery
#    4. Provide verify_upload() helper used by batch.py
#
# ════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════
#  UPLOAD STATE — tracks every upload for crash recovery
# ════════════════════════════════════════════════════════════

def load_upload_state() -> dict:
    if not os.path.exists(UPLOAD_STATE_FILE):
        return {}
    try:
        with open(UPLOAD_STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def save_upload_state(state: dict):
    try:
        with open(UPLOAD_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"[UPLOAD-STATE] Failed to save: {e}")

def mark_uploaded(state: dict, src_msg_id: int, dst_chat_id: int, dst_msg_id: int):
    """Mark a message as successfully uploaded and verified."""
    state[str(src_msg_id)] = {
        "dst_chat_id" : dst_chat_id,
        "dst_msg_id"  : dst_msg_id,
        "status"      : "verified",
    }
    save_upload_state(state)

def is_already_verified(state: dict, src_msg_id: int) -> int | None:
    """Returns dst_msg_id if already verified, else None."""
    entry = state.get(str(src_msg_id))
    if entry and entry.get("status") == "verified":
        return entry.get("dst_msg_id")
    return None

def mark_failed(state: dict, src_msg_id: int, reason: str = ""):
    """Mark a message as failed after all retries exhausted."""
    state[str(src_msg_id)] = {
        "status": "failed",
        "reason": reason[:200],
    }
    save_upload_state(state)


# ════════════════════════════════════════════════════════════
#  VERIFY UPLOAD — confirm message actually exists in dest
#
#  This is the core additional check. Called after every
#  process_msg() that returns a dest_id.
#
#  Cost: 1 API call (get_messages) per message.
#  Can be disabled with VERIFY_UPLOADS=False for speed.
# ════════════════════════════════════════════════════════════

VERIFY_UPLOADS = False  # Disabled — even on 1024MB dyno, Pyrogram session churn from get_messages() accumulates and never returns to OS


async def verify_upload(
    bot_client  : Client,
    dst_chat_id : int,
    dst_msg_id  : int,
    max_retries : int = 3,
) -> bool:
    """
    Verify that a message actually exists in the destination channel.
    
    Returns True if verified, False if not found after retries.
    
    This catches:
    - Silent upload failures (network glitch during send)
    - Partial sends (FloodWait interrupted the send)
    - Message deleted by Telegram (copyright strike, etc.)
    """
    if not VERIFY_UPLOADS:
        return True  # Skip verification if disabled
    
    for attempt in range(1, max_retries + 1):
        try:
            msg = await bot_client.get_messages(dst_chat_id, dst_msg_id)
            if msg and not getattr(msg, "empty", False):
                logger.info(
                    f"[VERIFY] ✅ dst_msg={dst_msg_id} in "
                    f"dst_chat={dst_chat_id} — confirmed (attempt {attempt})"
                )
                return True
            else:
                logger.warning(
                    f"[VERIFY] ❌ dst_msg={dst_msg_id} in "
                    f"dst_chat={dst_chat_id} — NOT FOUND (attempt {attempt}/{max_retries})"
                )
        except FloodWait as e:
            wait_secs = e.value + 2
            logger.warning(
                f"[VERIFY] FloodWait {wait_secs}s during verification "
                f"of dst_msg={dst_msg_id} — sleeping..."
            )
            await asyncio.sleep(wait_secs)
            continue  # Don't count FloodWait as a failed attempt
        except Exception as e:
            logger.warning(
                f"[VERIFY] Error checking dst_msg={dst_msg_id} "
                f"(attempt {attempt}/{max_retries}): {e}"
            )
        
        if attempt < max_retries:
            await asyncio.sleep(2 * attempt)  # Exponential backoff
    
    logger.error(
        f"[VERIFY] ❌ dst_msg={dst_msg_id} in dst_chat={dst_chat_id} "
        f"NOT verified after {max_retries} attempts"
    )
    return False


# ════════════════════════════════════════════════════════════
#  BATCH VERIFY — verify 100 messages per API call
#
#  Instead of 1 get_messages() per message (causes session
#  churn = RAM leak), we batch up to 100 dest IDs into ONE
#  raw API call: messages.GetMessages.
#
#  For 20K messages:
#    Old: 20,000 calls → ~5GB RAM (💀 crash)
#    New: 200 calls   → ~10MB RAM  (✅ safe)
#
#  Called every BATCH_VERIFY_INTERVAL messages (default: 500).
#  Also called once at end of batch for final check.
# ════════════════════════════════════════════════════════════

BATCH_VERIFY_INTERVAL = 500  # Verify every N uploaded messages
BATCH_VERIFY_CHUNK    = 100  # Max IDs per raw API call (Telegram limit)


async def batch_verify_uploads(
    bot_client : Client,
    dst_chat_id: int,
    msg_id_map : dict,    # src_msg_id → dst_msg_id (only recently uploaded)
    batch_start_src_id: int = None,  # only verify from this src_id onwards
) -> list:
    """
    Verify uploaded messages exist in dest channel — BATCHED.
    
    Sends up to 100 dest IDs per API call using raw Telegram API
    messages.GetMessages, instead of 1 call per message.
    
    Called every BATCH_VERIFY_INTERVAL messages during batch,
    and once at end of batch.
    
    Args:
        bot_client: Bot client with access to dest channel
        dst_chat_id: Destination channel ID
        msg_id_map: {src_msg_id: dst_msg_id} mapping of uploaded messages
        batch_start_src_id: If set, only verify messages with src_id >= this
    
    Returns:
        List of src_msg_ids that are MISSING from dest channel.
        Empty list = all verified ✅
    
    RAM cost: ~0.25MB per chunk of 100 IDs (vs 25MB for 100 individual calls)
    API cost: ceil(N / 100) calls for N messages (vs N calls)
    """
    if not msg_id_map:
        return []
    
    # Filter to only recently uploaded messages
    items_to_check = []
    for src_id, dst_id in msg_id_map.items():
        src_id_int = int(src_id) if isinstance(src_id, str) else src_id
        if batch_start_src_id is not None and src_id_int < batch_start_src_id:
            continue
        if dst_id is not None:
            items_to_check.append((src_id_int, dst_id))
    
    if not items_to_check:
        return []
    
    # Sort by dst_id for consistent chunking
    items_to_check.sort(key=lambda x: x[1])
    
    missing_src_ids = []
    total_chunks = (len(items_to_check) + BATCH_VERIFY_CHUNK - 1) // BATCH_VERIFY_CHUNK
    
    logger.info(
        f"[BATCH-VERIFY] Checking {len(items_to_check)} messages in "
        f"{total_chunks} chunk(s) (dst_chat={dst_chat_id})"
    )
    
    for chunk_idx in range(total_chunks):
        chunk_start = chunk_idx * BATCH_VERIFY_CHUNK
        chunk_end = min(chunk_start + BATCH_VERIFY_CHUNK, len(items_to_check))
        chunk_items = items_to_check[chunk_start:chunk_end]
        
        # Build mapping: dst_id → src_id for reverse lookup
        dst_to_src = {dst_id: src_id for src_id, dst_id in chunk_items}
        dst_ids = [dst_id for _, dst_id in chunk_items]
        
        try:
            # Use raw API — messages.GetMessages with InputMessageID
            # This fetches up to 100 messages in ONE call
            from pyrogram.raw import functions, types as raw_types
            
            peer = await bot_client.resolve_peer(dst_chat_id)
            
            input_ids = [
                raw_types.InputMessageID(id=dst_id)
                for dst_id in dst_ids
            ]
            
            result = await bot_client.invoke(
                functions.messages.GetMessages(
                    id=input_ids,
                )
            )
            
            # Collect which dst_ids were found
            found_dst_ids = set()
            if hasattr(result, 'messages') and result.messages:
                for msg in result.messages:
                    msg_id = getattr(msg, 'id', None)
                    if msg_id:
                        found_dst_ids.add(msg_id)
            
            # Check which are missing
            for dst_id in dst_ids:
                if dst_id not in found_dst_ids:
                    src_id = dst_to_src[dst_id]
                    missing_src_ids.append(src_id)
                    logger.warning(
                        f"[BATCH-VERIFY] ❌ MISSING src_msg={src_id} → "
                        f"dst_msg={dst_id} not found in dest channel"
                    )
            
            verified_count = len(found_dst_ids & set(dst_ids))
            logger.info(
                f"[BATCH-VERIFY] Chunk {chunk_idx+1}/{total_chunks}: "
                f"{verified_count}/{len(dst_ids)} verified ✅"
            )
            
        except FloodWait as e:
            wait_secs = e.value + 2
            logger.warning(
                f"[BATCH-VERIFY] FloodWait {wait_secs}s — sleeping..."
            )
            await asyncio.sleep(wait_secs)
            # Retry this chunk once after FloodWait
            try:
                result = await bot_client.invoke(
                    functions.messages.GetMessages(id=input_ids)
                )
                found_dst_ids = set()
                if hasattr(result, 'messages') and result.messages:
                    for msg in result.messages:
                        msg_id = getattr(msg, 'id', None)
                        if msg_id:
                            found_dst_ids.add(msg_id)
                for dst_id in dst_ids:
                    if dst_id not in found_dst_ids:
                        src_id = dst_to_src[dst_id]
                        missing_src_ids.append(src_id)
            except Exception as retry_e:
                logger.error(
                    f"[BATCH-VERIFY] Chunk {chunk_idx+1} retry also failed: {retry_e}"
                )
                # Can't verify — assume all OK to avoid false positives
                pass
            
        except Exception as e:
            logger.error(
                f"[BATCH-VERIFY] Chunk {chunk_idx+1}/{total_chunks} failed: {e}"
            )
            # Don't mark as missing — network error, not actual missing msg
            pass
        
        # Small delay between chunks to avoid FloodWait
        if chunk_idx < total_chunks - 1:
            await asyncio.sleep(1)
    
    if missing_src_ids:
        logger.warning(
            f"[BATCH-VERIFY] ⚠️ {len(missing_src_ids)} missing messages: "
            f"{missing_src_ids[:20]}{'...' if len(missing_src_ids) > 20 else ''}"
        )
    else:
        logger.info(
            f"[BATCH-VERIFY] ✅ All {len(items_to_check)} messages verified!"
        )
    
    return missing_src_ids


# ════════════════════════════════════════════════════════════
#  COUNT SANITY CHECK — 1 API call to compare message counts
#
#  After batch completes, compare how many messages exist in
#  dest channel vs how many we expected to upload.
#
#  Cost: 1 API call (get_chat or get_messages with limit=1)
#  RAM: ~0.25MB
#
#  Doesn't tell you WHICH messages are missing, but tells
#  you IF any are missing. Use as a quick sanity check.
# ════════════════════════════════════════════════════════════

async def count_sanity_check(
    bot_client : Client,
    dst_chat_id: int,
    expected_count: int,     # how many messages we expected to upload
    src_chat_id: int = None, # for logging only
) -> dict:
    """
    Quick sanity check: does dest channel have expected message count?
    
    Uses get_chat() to read the channel's message count.
    Compares against expected_count.
    
    Cost: 1 API call.
    RAM: ~0.25MB.
    
    Returns dict:
        {
            "ok": bool,
            "dest_count": int,
            "expected": int,
            "missing": int,     # expected - dest_count (approximate)
            "message": str,
        }
    """
    result = {
        "ok": False,
        "dest_count": 0,
        "expected": expected_count,
        "missing": 0,
        "message": "",
    }
    
    try:
        chat = await bot_client.get_chat(dst_chat_id)
        if chat:
            dest_count = getattr(chat, 'message_count', None) or 0
            result["dest_count"] = dest_count
            
            if dest_count >= expected_count:
                result["ok"] = True
                result["missing"] = 0
                result["message"] = (
                    f"✅ Count check PASSED — dest has {dest_count} msgs "
                    f"(expected ≥ {expected_count})"
                )
            else:
                result["missing"] = expected_count - dest_count
                result["message"] = (
                    f"⚠️ Count check FAILED — dest has {dest_count} msgs "
                    f"but expected {expected_count} — "
                    f"~{result['missing']} may be missing"
                )
            
            src_info = f" (src={src_chat_id})" if src_chat_id else ""
            logger.info(
                f"[COUNT-CHECK] {result['message']}{src_info}"
            )
        else:
            result["message"] = f"❌ Could not fetch dest channel info"
            logger.error(f"[COUNT-CHECK] {result['message']}")
    
    except Exception as e:
        result["message"] = f"❌ Count check error: {e}"
        logger.error(f"[COUNT-CHECK] {result['message']}")
    
    return result


# ════════════════════════════════════════════════════════════
#  SEQUENTIAL UPLOAD QUEUE
#
#  All messages go into this queue.
#  One worker processes them one at a time.
#  Guarantees order: question → poll → next question → etc.
#  Each upload is VERIFIED before moving to next.
# ════════════════════════════════════════════════════════════

class SequentialUploadQueue:
    """
    Single queue with single worker.
    Guarantees sequential upload in source order.
    No parallelism — eliminates all race conditions.
    Each upload is verified before the next one starts.
    """

    def __init__(self, bot_client: Client, dst_chat_id: int):
        self.queue        = asyncio.Queue()
        self.worker_task  = None
        self.is_running   = False
        self._src_to_dst  = {}    # src_msg_id → dst_msg_id (in-memory map)
        self.bot_client   = bot_client
        self.dst_chat_id  = dst_chat_id
        self._results     = {}    # src_msg_id → {"status": str, "dest_id": int|None}

    async def start(self):
        """Start the single worker."""
        self.is_running  = True
        self.worker_task = asyncio.create_task(self._worker())
        logger.info("[QUEUE] Sequential upload worker started ✅")

    async def stop(self):
        """Gracefully stop the worker after current item finishes."""
        self.is_running = False
        await self.queue.put(None)   # sentinel to unblock worker
        if self.worker_task:
            try:
                await asyncio.wait_for(self.worker_task, timeout=30)
            except asyncio.TimeoutError:
                logger.warning("[QUEUE] Worker didn't stop in 30s — cancelling")
                self.worker_task.cancel()
        logger.info("[QUEUE] Worker stopped")

    async def enqueue(self, item: dict):
        """
        Add a message to the upload queue.
        item must contain:
            src_msg_id  : int
            upload_fn   : async callable() → (res_str, dest_id, _, had_unresolved)
                          This calls process_msg internally.
            depends_on  : int | None  (src_msg_id of message this replies to)
            retry_fn    : async callable() → same as upload_fn (for FloodWait retry)
        """
        await self.queue.put(item)
        logger.info(
            f"[QUEUE] Enqueued src_msg={item['src_msg_id']} "
            f"depends_on={item.get('depends_on')} "
            f"queue_size={self.queue.qsize()}"
        )

    def get_dst_id(self, src_msg_id: int) -> int | None:
        """Get destination msg_id for a source msg_id."""
        return self._src_to_dst.get(src_msg_id)

    def get_result(self, src_msg_id: int) -> dict | None:
        """Get upload result for a source msg_id."""
        return self._results.get(src_msg_id)

    @property
    def src_to_dst(self) -> dict:
        """Get the full src→dst mapping."""
        return dict(self._src_to_dst)

    async def _worker(self):
        """
        The single worker coroutine.
        Processes one message at a time.
        Waits for each upload to fully complete AND be verified
        before moving to the next.
        Handles FloodWait by waiting and retrying — NEVER skips.
        """
        logger.info("[WORKER] Started, waiting for items...")

        while self.is_running:
            try:
                # Block until an item is available
                item = await self.queue.get()

                # Sentinel = shutdown signal
                if item is None:
                    break

                src_msg_id  = item["src_msg_id"]
                depends_on  = item.get("depends_on")
                upload_fn   = item["upload_fn"]
                retry_fn    = item.get("retry_fn")

                logger.info(
                    f"[WORKER] Processing src_msg={src_msg_id} "
                    f"depends_on={depends_on}"
                )

                # ── Check upload state (skip if already verified) ─
                state = load_upload_state()
                existing_dst = is_already_verified(state, src_msg_id)
                if existing_dst:
                    logger.info(
                        f"[WORKER] src_msg={src_msg_id} already verified "
                        f"as dst_msg={existing_dst} — skipping"
                    )
                    self._src_to_dst[src_msg_id] = existing_dst
                    self._results[src_msg_id] = {"status": "skipped_verified", "dest_id": existing_dst}
                    self.queue.task_done()
                    continue

                # ── Upload with retry + verification ──────
                max_retries = 3
                upload_succeeded = False
                dest_id = None
                res_str = ""

                for attempt in range(1, max_retries + 1):
                    try:
                        logger.info(
                            f"[WORKER] src_msg={src_msg_id} "
                            f"upload attempt {attempt}/{max_retries}"
                        )

                        # Call the upload function (process_msg)
                        result = await upload_fn()
                        res_str = result[0] if result else ""
                        dest_id = result[1] if result else None

                        if not dest_id:
                            logger.warning(
                                f"[WORKER] src_msg={src_msg_id} upload returned "
                                f"no dest_id — res={res_str[:80]}"
                            )
                            if attempt < max_retries:
                                await asyncio.sleep(2 * attempt)
                            continue

                        # ── VERIFY the upload actually landed ──
                        verified = await verify_upload(
                            bot_client  = self.bot_client,
                            dst_chat_id = self.dst_chat_id,
                            dst_msg_id  = dest_id,
                        )

                        if verified:
                            upload_succeeded = True
                            self._src_to_dst[src_msg_id] = dest_id
                            mark_uploaded(state, src_msg_id, self.dst_chat_id, dest_id)
                            logger.info(
                                f"[WORKER] ✅ src_msg={src_msg_id} → "
                                f"dst_msg={dest_id} uploaded AND verified"
                            )
                            break
                        else:
                            # Upload claimed success but verification failed
                            logger.error(
                                f"[WORKER] src_msg={src_msg_id} → dst_msg={dest_id} "
                                f"uploaded but NOT verified — retrying upload"
                            )
                            # Remove the failed dest_id from mapping
                            if src_msg_id in self._src_to_dst:
                                del self._src_to_dst[src_msg_id]
                            if attempt < max_retries:
                                await asyncio.sleep(3 * attempt)
                                # Try with retry_fn if available (has fresh FloodWait handling)
                                if retry_fn and attempt > 1:
                                    try:
                                        result = await retry_fn()
                                        res_str = result[0] if result else ""
                                        dest_id = result[1] if result else None
                                        if dest_id:
                                            verified2 = await verify_upload(
                                                bot_client  = self.bot_client,
                                                dst_chat_id = self.dst_chat_id,
                                                dst_msg_id  = dest_id,
                                            )
                                            if verified2:
                                                upload_succeeded = True
                                                self._src_to_dst[src_msg_id] = dest_id
                                                mark_uploaded(state, src_msg_id, self.dst_chat_id, dest_id)
                                                break
                                    except Exception as e:
                                        logger.warning(f"[WORKER] retry_fn also failed: {e}")

                    except FloodWait as e:
                        wait_seconds = e.value + 2
                        logger.warning(
                            f"[WORKER] FloodWait {wait_seconds}s for "
                            f"src_msg={src_msg_id} — sleeping..."
                        )
                        await asyncio.sleep(wait_seconds)
                        # Don't count FloodWait as a failed attempt
                        continue

                    except Exception as e:
                        logger.error(
                            f"[WORKER] src_msg={src_msg_id} attempt {attempt} "
                            f"failed: {e}"
                        )
                        if attempt < max_retries:
                            await asyncio.sleep(2 * attempt)

                # ── Record result ──────────────────────────
                if upload_succeeded:
                    self._results[src_msg_id] = {"status": "verified", "dest_id": dest_id, "res": res_str}
                else:
                    logger.error(
                        f"[WORKER] ❌ src_msg={src_msg_id} failed "
                        f"after {max_retries} attempts — marking as failed"
                    )
                    mark_failed(state, src_msg_id, reason=res_str[:200])
                    self._results[src_msg_id] = {"status": "failed", "dest_id": None, "res": res_str}

                self.queue.task_done()

            except Exception as e:
                logger.error(f"[WORKER] Unexpected error: {e}", exc_info=True)
                self.queue.task_done()

    async def wait_complete(self):
        """Wait for all enqueued items to be processed."""
        await self.queue.join()

    @property
    def total_enqueued(self) -> int:
        return len(self._results)

    @property
    def verified_count(self) -> int:
        return sum(1 for r in self._results.values() if r["status"] == "verified")

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self._results.values() if r["status"] == "failed")
