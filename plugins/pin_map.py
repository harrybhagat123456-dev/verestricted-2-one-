import asyncio
import json
import os
import logging
from pyrogram import Client
from pyrogram.enums import MessageServiceType
from pyrogram.errors import PeerIdInvalid, ChannelPrivate, FloodWait

logger = logging.getLogger(__name__)

PIN_MAP_FILE = "pin_map.json"


# ════════════════════════════════════════════════════════════
#
#  UNDERSTANDING WHY PREVIOUS APPROACH FAILED:
#
#  1. Service messages (MessageActionPinMessage) are NOT
#     returned by get_messages() in Telegram CHANNELS.
#     They exist in groups/supergroups but channels behave
#     differently — pins are stored server-side only.
#
#  2. check_pinned_batch() was called PER MESSAGE inside
#     the batch loop = 500 messages × 61 API calls = 30,500
#     redundant API calls causing frame exhaustion.
#
#  3. msg.pinned attribute only reflects the CURRENT pinned
#     message, not historical ones.
#
#  THE FIX:
#  Use Telegram's official API to get ALL pinned messages
#  for a channel in ONE call before the batch loop starts.
#  Store in pin_map. During loop = zero extra API calls.
#  Remove check_pinned_batch() from the per-message loop.
#
# ════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════
#  STORAGE
# ════════════════════════════════════════════════════════════

def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)

def save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ════════════════════════════════════════════════════════════
#  GET ALL PINNED MESSAGE IDs
#
#  WHY get_pinned_messages() ONLY RETURNS 1 OUT OF 30:
#
#  Pyrogram's get_pinned_messages() wraps the Telegram API
#  but only returns the LAST pinned message (singular).
#  It does NOT paginate through all pinned messages.
#
#  channels.GetFullChannel also returns only 1 pinned_msg_id.
#
#  THE FIX: Use raw Telegram API messages.Search with
#  InputMessagesFilterPinned. This is EXACTLY what Telegram
#  clients use when you tap "Pinned Messages" in a channel.
#  It returns ALL pinned messages with proper pagination.
#
#  Cost: 1 API call per 100 pinned messages (pagination).
#  For 30 pins = 1 call. For 200 pins = 2 calls. etc.
# ════════════════════════════════════════════════════════════

async def fetch_all_pinned_ids(
    user_client : Client,    # must be user client, bot gets PEER_ID_INVALID
    src_chat_id : int,
) -> list:
    """
    Fetches ALL currently pinned message IDs from source channel.

    PRIMARY METHOD: messages.Search with InputMessagesFilterPinned
    This is the SAME API call Telegram clients make when you open
    "Pinned Messages" in a channel. Returns ALL pins with pagination.

    Fallbacks (if primary fails):
        B. get_pinned_messages() — Pyrogram built-in (may return only 1)
        C. get_pinned_message() — single pin fallback
        D. raw API channels.getFullChannel — last resort

    Returns ORDERED list of pinned message IDs.
    The order is the same as shown in Telegram's "Pinned Messages" panel:
    most recently pinned message FIRST (index 0).
    Cost: ~1 API call per 100 pinned messages.
    """
    pinned_ids = []    # ordered list — most recently pinned first

    # ── PRIMARY: messages.Search with InputMessagesFilterPinned ──
    # This is what Telegram clients use internally to show the
    # "Pinned Messages" list. It returns ALL pinned messages
    # in the channel with proper pagination.
    try:
        from pyrogram.raw import functions, types as raw_types

        peer = await user_client.resolve_peer(src_chat_id)
        logger.info(
            f"[PIN-FETCH] Starting messages.Search with "
            f"InputMessagesFilterPinned for chat={src_chat_id}"
        )

        offset_id = 0
        page = 0
        while True:
            page += 1
            result = await user_client.invoke(
                functions.messages.Search(
                    peer=peer,
                    q="",
                    filter=raw_types.InputMessagesFilterPinned(),
                    min_date=0,
                    max_date=0,
                    offset_id=offset_id,
                    add_offset=0,
                    limit=100,       # max per page
                    min_id=0,
                    max_id=0,
                    hash=0,
                )
            )

            # Extract message IDs from the result (preserving API order)
            # messages.Search returns most recently pinned first
            msgs_found = 0
            if hasattr(result, 'messages') and result.messages:
                for msg in result.messages:
                    msg_id = getattr(msg, 'id', None)
                    if msg_id and msg_id not in pinned_ids:
                        pinned_ids.append(msg_id)
                        msgs_found += 1

            logger.info(
                f"[PIN-FETCH] Page {page}: found {msgs_found} pinned msgs "
                f"(total so far: {len(pinned_ids)})"
            )

            # Pagination: if we got fewer than limit, we're done
            if msgs_found < 100:
                break

            # Set offset_id to the last message ID for next page
            # messages.Search with pinned filter returns newest first
            if pinned_ids:
                offset_id = min(pinned_ids)  # oldest ID found so far
            else:
                break

            # Safety: avoid infinite loops
            if page > 50:  # 50 pages × 100 = 5000 pins max
                logger.warning(
                    f"[PIN-FETCH] Hit pagination limit (50 pages). "
                    f"Stopping with {len(pinned_ids)} pins found."
                )
                break

        logger.info(
            f"[PIN-FETCH] messages.Search found "
            f"{len(pinned_ids)} total pins in {page} page(s)"
        )

    except FloodWait as e:
        wait_secs = e.value if hasattr(e, 'value') else 30
        logger.warning(
            f"[PIN-FETCH] FloodWait {wait_secs}s during messages.Search"
        )
        # Don't wait here — let caller handle it. Keep what we have.
    except Exception as e:
        logger.warning(
            f"[PIN-FETCH] messages.Search failed: {type(e).__name__}: {e}"
        )

    # ── If primary worked, return immediately (no need for fallbacks) ──
    if pinned_ids:
        logger.info(
            f"[PIN-FETCH] Total pinned messages found: {len(pinned_ids)} "
            f"→ IDs (display order): {pinned_ids}"
        )
        return pinned_ids

    # ── Fallback B: get_pinned_messages() ─────────────────────
    # May return only 1, but better than nothing.
    try:
        fallback_ids = []
        async for msg in user_client.get_pinned_messages(src_chat_id):
            if msg.id not in fallback_ids:
                fallback_ids.append(msg.id)
                logger.info(f"[PIN-FETCH] Found pinned msg_id={msg.id} via get_pinned_messages (fallback)")
        pinned_ids.extend(fallback_ids)

        if pinned_ids:
            logger.info(
                f"[PIN-FETCH] get_pinned_messages fallback found "
                f"{len(pinned_ids)} pins"
            )
    except Exception as e:
        logger.warning(f"[PIN-FETCH] get_pinned_messages failed: {e}")

    # ── Fallback C: get_pinned_message (single pin) ─────────
    if not pinned_ids:
        try:
            msg = await user_client.get_pinned_message(src_chat_id)
            if msg and msg.id not in pinned_ids:
                pinned_ids.append(msg.id)
                logger.info(
                    f"[PIN-FETCH] Found pinned msg_id={msg.id} "
                    f"via get_pinned_message (fallback)"
                )
        except Exception as e:
            logger.warning(f"[PIN-FETCH] get_pinned_message failed: {e}")

    # ── Fallback D: raw API channels.getFullChannel ───────────
    if not pinned_ids:
        try:
            from pyrogram.raw import functions, types as raw_types
            peer   = await user_client.resolve_peer(src_chat_id)
            result = await user_client.invoke(
                functions.channels.GetFullChannel(channel=peer)
            )
            pinned_id = getattr(result.full_chat, "pinned_msg_id", None)
            if pinned_id and pinned_id not in pinned_ids:
                pinned_ids.append(pinned_id)
                logger.info(
                    f"[PIN-FETCH] Found pinned msg_id={pinned_id} "
                    f"via getFullChannel raw API (fallback)"
                )
        except Exception as e:
            logger.warning(f"[PIN-FETCH] getFullChannel failed: {e}")

    logger.info(
        f"[PIN-FETCH] Total pinned messages found: {len(pinned_ids)} "
        f"→ IDs (display order): {pinned_ids}"
    )
    return pinned_ids


# ════════════════════════════════════════════════════════════
#  METHOD 2: detect from msg object (zero API cost)
#
#  Called on every message DURING the batch loop.
#  Checks the msg object itself — no API call needed.
#  Updates pin_map if a pin is detected.
# ════════════════════════════════════════════════════════════

def detect_pin_from_msg(msg, pin_map: dict) -> bool:
    """
    Checks if `msg` itself indicates it is pinned.
    Zero API calls — uses only the message object.

    Two checks:
        1. msg.pinned == True (flag on the message object)
        2. msg is a service message of type PINNED_MESSAGE
           (rare in channels but works in supergroups)

    Updates pin_map if detected.
    Returns True if pinned, False otherwise.
    """
    pin_key = str(msg.id)

    # Check 1: msg.pinned flag
    if getattr(msg, "pinned", False):
        if pin_key not in pin_map:
            pin_map[pin_key] = True
            logger.info(
                f"[PIN-DETECT] msg={msg.id} has pinned=True flag"
            )
        return True

    # Check 2: service message type
    service = getattr(msg, "service", None)
    if service == MessageServiceType.PINNED_MESSAGE:
        # This is a pin notification service message
        # The reply_to tells us which msg was pinned
        pinned_id = getattr(msg, "reply_to_message_id", None)
        if pinned_id:
            pin_key_target = str(pinned_id)
            if pin_key_target not in pin_map:
                pin_map[pin_key_target] = True
                logger.info(
                    f"[PIN-DETECT] Service msg={msg.id} "
                    f"→ msg={pinned_id} is pinned"
                )
            return True

    return False


# ════════════════════════════════════════════════════════════
#  STARTUP: build complete pin_map BEFORE batch loop starts
#
#  This is the KEY fix. All pin detection happens here.
#  The batch loop never calls any API for pin detection.
# ════════════════════════════════════════════════════════════

async def startup_pin(
    user_client  : Client,
    src_chat_id  : int,
    fetch_map    : dict,     # your existing fetch map (msg_id → msg)
) -> dict:
    """
    Builds a complete pin_map BEFORE the batch loop starts.
    After this, the batch loop uses ZERO extra API calls
    for pin detection.

    Three sources merged (cheapest to most expensive):
        1. Disk cache from previous run     (0 API calls)
        2. fetch_map scan for pinned flags  (0 API calls)
        3. Telegram's official pinned API   (1-2 API calls)

    After startup_pin() completes, every pinned message
    in the source channel is in pin_map.
    """

    # ── Source 1: load from disk ──────────────────────────────
    pin_map = load_json(PIN_MAP_FILE)
    logger.info(
        f"[PIN-STARTUP] Loaded {len(pin_map)} entries from disk"
    )

    # ── Source 2: scan fetch_map (zero API) ───────────────────
    # fetch_map has all downloaded messages. Check msg.pinned
    # flag on each one. Free — no API calls.
    fetch_found = 0
    for msg_id, msg in fetch_map.items():
        if isinstance(msg, dict):
            # Dict-style fetch_map entry (from /fetch command)
            if msg.get("is_pinned"):
                if str(msg_id) not in pin_map:
                    pin_map[str(msg_id)] = True
                    fetch_found += 1
        elif getattr(msg, "pinned", False):
            if str(msg_id) not in pin_map:
                pin_map[str(msg_id)] = True
                fetch_found += 1

    logger.info(
        f"[PIN-STARTUP] fetch_map scan found "
        f"{fetch_found} additional pins"
    )

    # ── Source 3: Telegram official API (1-2 API calls) ───────
    # This gets ALL currently pinned messages from Telegram.
    # This is the most reliable method — do it every startup.
    api_found = 0
    if user_client:
        try:
            api_pinned_ids = await fetch_all_pinned_ids(user_client, src_chat_id)
            for pid in api_pinned_ids:
                if str(pid) not in pin_map:
                    pin_map[str(pid)] = True
                    api_found += 1
        except Exception as e:
            logger.warning(f"[PIN-STARTUP] API fetch failed: {e}")

    logger.info(
        f"[PIN-STARTUP] API found {api_found} additional pins"
    )

    # Save merged result to disk
    save_json(PIN_MAP_FILE, pin_map)

    logger.info(
        f"[PIN-STARTUP] ✅ pin_map ready: {len(pin_map)} total pins\n"
        f"  from fetch_map: {fetch_found}\n"
        f"  from API:       {api_found}\n"
        f"  pinned IDs:     {list(pin_map.keys())}"
    )

    return pin_map


# ════════════════════════════════════════════════════════════
#  PER-MESSAGE: handle pin mirror
#
#  Called for each message in the batch loop.
#  ZERO API calls — only uses pin_map cache.
#  detect_pin_from_msg() also runs free on the msg object.
#
#  check_pinned_batch() is REMOVED from here entirely.
#  It caused frame exhaustion and was the primary CPU/RAM issue.
# ════════════════════════════════════════════════════════════

async def handle_pin_mirror(
    bot_client  : Client,     # bot client for pinning in dest
    user_client : Client,     # user client (tried as fallback for pinning)
    src_chat_id : int,
    src_msg_id  : int,
    src_msg,                  # the actual source message object
    dst_chat_id : int,
    dst_msg_id  : int,
    pin_map     : dict,
):
    """
    Detect if a source message is pinned and PIN IT IMMEDIATELY in dest.

    ON-THE-FLY PINNING: When a pinned message is encountered during
    the batch loop, it is pinned in the destination channel RIGHT AWAY.
    This preserves the SEQUENCE — if msg #10 is pinned in source, it
    gets pinned in dest at the same position during the batch.

    Telegram uses LIFO for pin display — the LAST pin operation shows
    on top. Since we process messages in chronological order (oldest
    first), the most recently pinned message in the source is encountered
    LAST and pinned LAST → it ends up on top in dest → matches source.

    Detection order (all zero API cost):
        1. pin_map cache lookup           (built at startup)
        2. detect_pin_from_msg(src_msg)   (checks msg object)

    If the message IS pinned, we pin it in dest immediately using
    bot_client, falling back to user_client if bot_client fails.

    Returns: True if the message was pinned on-the-fly, False otherwise.
    """
    pin_key   = str(src_msg_id)
    is_pinned = False

    # ── Check 1: pin_map cache (built at startup) ─────────────
    if pin_key in pin_map:
        is_pinned = pin_map[pin_key]
        logger.info(
            f"[PIN-MIRROR] Cache HIT src_msg={src_msg_id} "
            f"→ is_pinned={is_pinned}"
        )

    # ── Check 2: detect from msg object (zero API) ────────────
    elif detect_pin_from_msg(src_msg, pin_map):
        is_pinned = True
        logger.info(
            f"[PIN-MIRROR] Detected via msg object src_msg={src_msg_id}"
        )
        save_json(PIN_MAP_FILE, pin_map)   # persist newly found pin

    else:
        logger.debug(
            f"[PIN-MIRROR] src_msg={src_msg_id} not pinned "
            f"(cache miss + msg check negative)"
        )
        return False

    # ── ON-THE-FLY PINNING ──────────────────────────────────
    # Pin the message in dest RIGHT NOW as it's encountered during the batch.
    # This preserves the exact sequence — pins happen in the same order
    # as the source channel because we process messages chronologically.
    if is_pinned and dst_msg_id:
        # Try bot_client first, then user_client as fallback
        clients_to_try = [bot_client]
        if user_client and user_client != bot_client:
            clients_to_try.append(user_client)

        for client in clients_to_try:
            try:
                await pin_in_destination(
                    client=client,
                    dst_chat_id=dst_chat_id,
                    dst_msg_id=dst_msg_id,
                )
                client_name = 'user_client' if client == user_client else 'bot_client'
                logger.info(
                    f"[PIN-MIRROR] ✅ On-the-fly pinned src_msg={src_msg_id} "
                    f"→ dst_msg={dst_msg_id} using {client_name}"
                )
                return True
            except Exception as e:
                client_name = 'user_client' if client == user_client else 'bot_client'
                logger.debug(
                    f"[PIN-MIRROR] Failed to pin src_msg={src_msg_id} "
                    f"→ dst_msg={dst_msg_id} with {client_name}: {e}"
                )
                continue

        # All clients failed — log but don't crash the batch
        logger.warning(
            f"[PIN-MIRROR] ❌ All clients failed to pin src_msg={src_msg_id} "
            f"→ dst_msg={dst_msg_id} on-the-fly. "
            f"verify_and_sync_pins() will retry after batch."
        )
        return False

    return False


# ════════════════════════════════════════════════════════════
#  PIN IN DESTINATION
# ════════════════════════════════════════════════════════════

async def pin_in_destination(
    client      : Client,
    dst_chat_id : int,
    dst_msg_id  : int,
):
    """Pins the message in destination channel. Silent pin.

    The client must be an admin in the destination channel with
    'pin_messages' permission. If the bot doesn't have this,
    try using the userbot (ubot) instead.

    Raises: The original exception on failure so callers can
    detect failure and apply fallback logic.
    """
    try:
        await client.pin_chat_message(
            chat_id              = dst_chat_id,
            message_id           = dst_msg_id,
            disable_notification = True,
            both_sides           = False,
        )
        logger.info(
            f"[PIN-DEST] ✅ Pinned dst_msg={dst_msg_id} "
            f"in dst_chat={dst_chat_id}"
        )
    except Exception as e:
        err_str = str(e)
        # Log specific reason for debugging
        if "CHAT_ADMIN_REQUIRED" in err_str or "admin" in err_str.lower():
            logger.error(
                f"[PIN-DEST] ❌ Client lacks admin rights to pin in dst_chat={dst_chat_id}. "
                f"Make sure a client is an admin with 'Pin Messages' permission. Error: {e}"
            )
        elif "MESSAGE_ID_INVALID" in err_str:
            logger.error(
                f"[PIN-DEST] ❌ Message dst_msg={dst_msg_id} not found in dst_chat={dst_chat_id}. "
                f"It may not have been sent yet. Error: {e}"
            )
        else:
            logger.error(
                f"[PIN-DEST] ❌ Failed to pin dst_msg={dst_msg_id} "
                f"in dst_chat={dst_chat_id}: {e}"
            )
        # CRITICAL: Re-raise so callers know the pin FAILED.
        # Previously this was swallowed, causing failed_to_pin to always
        # be empty and the ubot fallback to never trigger.
        raise


# ════════════════════════════════════════════════════════════
#  POST-BATCH: Verify ALL source pins are synced to dest
#
#  Called after batch completes. Fetches ALL pinned messages
#  from the source channel one more time, then checks each
#  one has a corresponding pin in the destination channel.
#  If not pinned yet, pins it.
#
#  Cost: 1-2 API calls to fetch source pins + N pin calls
#  (where N = number of pins not yet pinned in dest).
# ════════════════════════════════════════════════════════════

async def verify_and_sync_pins(
    user_client : Client,
    bot_client  : Client,
    src_chat_id : int,
    dst_chat_id : int,
    msg_id_map  : dict,    # src_msg_id (int) → dst_msg_id (int)
    uid         : int = None,           # user ID for mark_needs_link_update
    source_channel : str = None,        # source channel str for mark_needs_link_update
):
    """After batch completes, verify ALL source pins are pinned in dest.

    ON-THE-FLY PINNING FALLBACK: Since handle_pin_mirror() now pins
    on-the-fly during the batch, this function only catches MISSED pins
    (e.g., if pin_in_destination failed during the batch due to admin
    rights issues, or messages that were pinned AFTER they were already
    uploaded).

    It does NOT unpin and re-pin everything. Instead it:
      1. Fetches pinned IDs from source channel
      2. Fetches pinned IDs from dest channel (already pinned on-the-fly)
      3. Only pins messages that are pinned in source but NOT in dest
      4. Reports stats on what was done

    For the MISSED pins, we pin in REVERSE source display order so that
    LIFO behavior preserves the source channel's display order.

    Args:
        user_client: User client with source channel access
        bot_client: Bot client with dest channel access
        src_chat_id: Source channel ID
        dst_chat_id: Destination channel ID
        msg_id_map: Mapping of src_msg_id → dst_msg_id

    Returns:
        dict with stats:
            total_source_pins: int
            already_pinned: int (pins already in dest from on-the-fly)
            newly_pinned: int (missed pins that were applied now)
            failed_to_pin: list of (src_id, reason)
            not_in_map: list of src_ids that were pinned but not uploaded
    """
    result = {
        "total_source_pins": 0,
        "already_pinned": 0,
        "newly_pinned": 0,
        "failed_to_pin": [],
        "not_in_map": [],
    }

    # Build ordered list of clients to try for dest channel operations.
    dest_clients = []
    if user_client:
        dest_clients.append(user_client)
    if bot_client and bot_client not in dest_clients:
        dest_clients.append(bot_client)

    # Step 1: Fetch ALL pinned IDs from source (ORDERED: most recently pinned first)
    try:
        source_pinned_ids = await fetch_all_pinned_ids(user_client, src_chat_id)
    except Exception as e:
        logger.error(f"[PIN-SYNC] Failed to fetch source pinned IDs: {e}")
        return result

    result["total_source_pins"] = len(source_pinned_ids)
    logger.info(
        f"[PIN-SYNC] Source channel has {len(source_pinned_ids)} pinned messages "
        f"(display order): {source_pinned_ids}"
    )

    if not source_pinned_ids:
        return result

    # Step 2: Fetch already-pinned IDs from dest (these were pinned on-the-fly)
    dest_pinned_ids = []
    for client in dest_clients:
        try:
            dest_pinned_ids = await fetch_all_pinned_ids(client, dst_chat_id)
            if dest_pinned_ids is not None:
                logger.info(
                    f"[PIN-SYNC] Dest has {len(dest_pinned_ids)} already-pinned messages "
                    f"(pinned on-the-fly during batch)"
                )
                break
        except Exception as e:
            logger.warning(
                f"[PIN-SYNC] Could not fetch dest pinned IDs: {e}"
            )

    dest_pinned_set = set(dest_pinned_ids) if dest_pinned_ids else set()

    # Step 3: Build list of source pins that are NOT yet pinned in dest
    # These are the MISSED pins that need to be applied now.
    missed_pins = []  # list of (src_id, dst_id) in source display order
    for src_id in source_pinned_ids:
        dst_id = msg_id_map.get(src_id)
        if not dst_id:
            result["not_in_map"].append(src_id)
            logger.info(
                f"[PIN-SYNC] src_msg={src_id} is pinned but not in msg_id_map — skipping"
            )
            continue

        if dst_id in dest_pinned_set:
            # Already pinned on-the-fly during the batch ✅
            result["already_pinned"] += 1
            logger.info(
                f"[PIN-SYNC] src_msg={src_id} → dst_msg={dst_id} already pinned (on-the-fly) ✅"
            )
        else:
            # Missed — needs to be pinned now
            missed_pins.append((src_id, dst_id))

    if not missed_pins:
        logger.info(
            f"[PIN-SYNC] All {result['already_pinned']} pins already in dest "
            f"(on-the-fly pinning worked perfectly) ✅"
        )
        return result

    # Step 4: Pin MISSED messages in REVERSE source display order
    # Telegram uses LIFO — the last pin operation shows at the top.
    # We pin in REVERSE so the most recently pinned ends up on top.
    logger.info(
        f"[PIN-SYNC] {len(missed_pins)} pins missed on-the-fly, applying now..."
    )
    for src_id, dst_id in reversed(missed_pins):
        pinned = False
        last_error = None
        for client in dest_clients:
            try:
                await pin_in_destination(
                    client=client,
                    dst_chat_id=dst_chat_id,
                    dst_msg_id=dst_id,
                )
                result["newly_pinned"] += 1
                client_name = 'user_client' if client == user_client else 'bot_client'
                logger.info(
                    f"[PIN-SYNC] Pinned MISSED src_msg={src_id} → dst_msg={dst_id} "
                    f"using {client_name} "
                    f"(pin #{result['newly_pinned']} of {len(missed_pins)})"
                )
                # Mark pinned message for link rewrite
                if uid and source_channel:
                    try:
                        from plugins.batch import mark_needs_link_update
                        await mark_needs_link_update(
                            uid=uid,
                            source_channel=source_channel,
                            dst_chat_id=dst_chat_id,
                            dst_msg_id=dst_id,
                            src_msg_id=src_id,
                        )
                        logger.info(f"[PIN-SYNC] Marked pinned dst={dst_id} for link rewrite")
                    except Exception as mark_err:
                        logger.warning(f"[PIN-SYNC] Failed to mark pinned dst={dst_id} for link rewrite: {mark_err}")
                pinned = True
                break
            except Exception as e:
                last_error = e
                client_name = 'user_client' if client == user_client else 'bot_client'
                logger.debug(
                    f"[PIN-SYNC] Failed to pin src_msg={src_id} → dst_msg={dst_id} "
                    f"with {client_name}: {e}"
                )
                continue

        if not pinned:
            result["failed_to_pin"].append((src_id, str(last_error)))
            logger.error(
                f"[PIN-SYNC] ALL clients failed to pin src_msg={src_id} "
                f"→ dst_msg={dst_id}: {last_error}"
            )

        # Small delay between pins to avoid FloodWait
        await asyncio.sleep(0.5)

    logger.info(
        f"[PIN-SYNC] Done: source_pins={result['total_source_pins']} "
        f"already_pinned={result['already_pinned']} "
        f"newly_pinned={result['newly_pinned']} "
        f"failed={len(result['failed_to_pin'])} "
        f"not_in_map={len(result['not_in_map'])}"
    )

    return result


# ════════════════════════════════════════════════════════════
#  ONE-TIME FIX: Force re-link all pinned messages
#
#  Pinned messages were never added to unresolved_links_collection
#  because the pin code path never called mark_needs_link_update().
#  This function finds ALL currently pinned messages in the
#  destination channel, adds them to the collection, and then
#  runs resolve_pending_link_rewrites() to fix their links.
# ════════════════════════════════════════════════════════════

async def force_relink_all_pinned(
    bot_client      : Client,
    ubot            : Client,
    uid             : int,
    source_channel  : str,
    dst_chat_id     : int,
    src_chat_id     : int,
    dest_channel_username : str = None,
    source_channel_username : str = None,
):
    """
    One-time fix for already-pinned messages that were never added
    to unresolved_links_collection.

    Fetches ALL currently pinned messages in destination.
    Adds them to unresolved_links_collection.
    Then calls resolve_pending_link_rewrites() to fix them.
    """
    logger.info("[PIN-LINK] Force re-linking all pinned messages...")

    # Get all pinned messages in destination
    pinned_dst_msgs = []
    try:
        async for msg in bot_client.get_pinned_messages(dst_chat_id):
            pinned_dst_msgs.append(msg)
    except Exception as e:
        logger.error(f"[PIN-LINK] Failed to get pinned messages from bot_client: {e}")
        # Try ubot as fallback
        if ubot:
            try:
                async for msg in ubot.get_pinned_messages(dst_chat_id):
                    pinned_dst_msgs.append(msg)
            except Exception as e2:
                logger.error(f"[PIN-LINK] Failed to get pinned messages from ubot: {e2}")
                return

    logger.info(f"[PIN-LINK] Found {len(pinned_dst_msgs)} pinned messages in destination")

    if not pinned_dst_msgs:
        logger.info("[PIN-LINK] No pinned messages to fix")
        return

    # Load src_to_dst map to find src_msg_id for each dst_msg_id
    try:
        from plugins.batch import mark_needs_link_update, resolve_pending_link_rewrites, load_upload_map
    except ImportError as e:
        logger.error(f"[PIN-LINK] Cannot import batch functions: {e}")
        return

    # Load complete msg_id_map and build dst_to_src reverse map
    complete_map, _, _ = await load_upload_map(uid, str(source_channel))
    dst_to_src = {v: k for k, v in complete_map.items()}
    logger.info(f"[PIN-LINK] Loaded msg_id_map: {len(complete_map)} mappings, reverse map: {len(dst_to_src)} entries")

    # Mark each pinned message for link rewrite
    marked = 0
    for dst_msg in pinned_dst_msgs:
        dst_msg_id = dst_msg.id
        src_msg_id = dst_to_src.get(dst_msg_id)
        if not src_msg_id:
            logger.warning(f"[PIN-LINK] dst={dst_msg_id} has no src mapping — skipping")
            continue

        await mark_needs_link_update(
            uid=uid,
            source_channel=source_channel,
            dst_chat_id=dst_chat_id,
            dst_msg_id=dst_msg_id,
            src_msg_id=src_msg_id,
        )
        marked += 1

    logger.info(f"[PIN-LINK] Marked {marked} pinned messages for link rewrite")

    if marked == 0:
        logger.info("[PIN-LINK] No pinned messages could be mapped — nothing to fix")
        return

    # Now run the resolver — will fix all of them
    # Build multi-source channels for cross-channel link rewriting
    _multi_src_channels = None
    try:
        from plugins.batch import build_multi_source_channels as _build_msc
        _resolve_client = ubot
        _multi_src_channels, _ = await _build_msc(
            uid, source_channel,
            primary_username=source_channel_username,
            primary_numeric_id=src_chat_id,
            client=_resolve_client,
        )
        if _multi_src_channels and len(_multi_src_channels) <= 1:
            _multi_src_channels = None
    except Exception:
        _multi_src_channels = None

    await resolve_pending_link_rewrites(
        bot_client=bot_client,
        ubot=ubot,
        source_channel=source_channel,
        dest_channel_id_int=dst_chat_id,
        dest_channel_username=dest_channel_username,
        source_channel_username=source_channel_username,
        uid=uid,
        source_channel_id=src_chat_id,
        multi_source_channels=_multi_src_channels,
    )

    logger.info("[PIN-LINK] Force re-link complete")
