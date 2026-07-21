# ════════════════════════════════════════════════════════════════════
# DATA RESILIENCE — Survive bot token changes, DB migrations, etc.
#
# PROBLEM: When the bot token changes (e.g., @BotFather /revoke),
# the Heroku dyno restarts. If the MONGO_DB URI also changes (e.g.,
# new Heroku app), ALL stored data (fetch_maps, upload_maps, relink
# sessions, etc.) becomes invisible because the new DB is empty.
#
# Even if the MongoDB stays the same, some users report data "vanishing"
# after token changes. This module:
#   1. Diagnoses data health on startup
#   2. Detects orphaned data (exists under different user IDs)
#   3. Provides migration tools to re-associate data
#   4. Provides fallback lookup functions
#
# KEY INSIGHT: All data is keyed by user_id (human user's Telegram ID).
# This ID does NOT change when the bot token changes. So data should
# persist. If it doesn't, the cause is:
#   - MongoDB URI/DB_NAME changed
#   - Data was accidentally deleted
#   - Environment variable misconfiguration
# ════════════════════════════════════════════════════════════════════

import asyncio
import logging
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME, OWNER_ID

logger = logging.getLogger(__name__)

# MongoDB connection for diagnostics
_dr_mongo = AsyncIOMotorClient(MONGO_URI)
_dr_db = _dr_mongo[DB_NAME]

# Critical collections to check
CRITICAL_COLLECTIONS = {
    "fetch_maps": "Fetch maps (pre-scanned message metadata)",
    "upload_maps": "Upload maps (src→dst message ID mappings)",
    "relink_sessions": "Relink sessions (link repair progress)",
    "relink_url_cache": "Resolved URL cache (link rewriting)",
    "relink_fingerprints": "Fingerprint index (content matching)",
    "mirrored_messages_index": "Mirror index (src→dst at mirror time)",
    "auth_users": "Authorized users list",
    "users": "User sessions and settings",
}


async def run_startup_diagnostic():
    """Run data health diagnostic on bot startup.
    
    Checks:
    1. Which critical collections exist and have data
    2. How many documents each collection has
    3. Which user IDs own data (detects orphaned data)
    4. Whether OWNER_ID has data in critical collections
    5. Warns if OWNER_ID has NO data (possible data loss)
    
    Returns: dict with diagnostic results.
    """
    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "owner_ids": OWNER_ID,
        "db_name": DB_NAME,
        "collections": {},
        "warnings": [],
        "data_found_for": set(),
        "owner_has_data": False,
    }
    
    print(f"[DATA-RESILIENCE] Running startup diagnostic...")
    print(f"[DATA-RESILIENCE] DB_NAME={DB_NAME} OWNER_ID={OWNER_ID}")
    
    for coll_name, description in CRITICAL_COLLECTIONS.items():
        try:
            collection = _dr_db[coll_name]
            total_docs = await collection.count_documents({})
            
            # Get distinct user_ids in this collection
            distinct_uids = []
            if total_docs > 0:
                try:
                    distinct_uids = await collection.distinct("user_id")
                except Exception:
                    # Some collections might not have user_id field
                    try:
                        distinct_uids = await collection.distinct("uid")
                    except Exception:
                        distinct_uids = []
            
            coll_info = {
                "total_docs": total_docs,
                "description": description,
                "distinct_user_ids": distinct_uids,
                "has_data": total_docs > 0,
            }
            results["collections"][coll_name] = coll_info
            
            # Track which users have data
            for uid in distinct_uids:
                results["data_found_for"].add(uid)
            
            # Check if owner has data in this collection
            owner_in_coll = any(uid in OWNER_ID for uid in distinct_uids)
            if owner_in_coll:
                results["owner_has_data"] = True
            
            status = f"✅ {total_docs} docs" if total_docs > 0 else "❌ EMPTY"
            print(f"[DATA-RESILIENCE] {coll_name}: {status} | {description}")
            if distinct_uids:
                print(f"[DATA-RESILIENCE]   user_ids: {distinct_uids}")
                
        except Exception as e:
            results["collections"][coll_name] = {
                "error": str(e),
                "has_data": False,
            }
            print(f"[DATA-RESILIENCE] {coll_name}: ⚠️ ERROR: {e}")
    
    # Convert set to list for JSON serialization
    results["data_found_for"] = list(results["data_found_for"])
    
    # Generate warnings
    if not results["owner_has_data"] and results["data_found_for"]:
        non_owner_uids = [uid for uid in results["data_found_for"] if uid not in OWNER_ID]
        if non_owner_uids:
            warning = (
                f"⚠️ OWNER ({OWNER_ID}) has NO data, but data exists for user IDs: {non_owner_uids}. "
                f"This may indicate data was stored under a different user ID. "
                f"Use /claimdata to re-associate it."
            )
            results["warnings"].append(warning)
            print(f"[DATA-RESILIENCE] {warning}")
    
    # Check for empty critical collections
    empty_collections = [
        name for name, info in results["collections"].items()
        if not info.get("has_data", False)
    ]
    if empty_collections:
        warning = f"⚠️ Empty collections: {empty_collections}"
        results["warnings"].append(warning)
        print(f"[DATA-RESILIENCE] {warning}")
    
    # Summary
    total_docs = sum(
        info.get("total_docs", 0)
        for info in results["collections"].values()
    )
    print(f"[DATA-RESILIENCE] Summary: {total_docs} total docs across {len(CRITICAL_COLLECTIONS)} collections")
    if results["warnings"]:
        print(f"[DATA-RESILIENCE] ⚠️ {len(results['warnings'])} warning(s) detected!")
    
    return results


async def migrate_data_between_users(source_user_id: int, target_user_id: int, collections: list = None):
    """Migrate all data from source_user_id to target_user_id.
    
    This is used when data was stored under a wrong user ID and needs
    to be re-associated with the correct user.
    
    Args:
        source_user_id: The user ID that currently owns the data
        target_user_id: The user ID that should own the data
        collections: List of collection names to migrate. If None, migrates all.
    
    Returns: dict with migration results per collection.
    """
    if collections is None:
        collections = list(CRITICAL_COLLECTIONS.keys())
    
    results = {}
    print(f"[DATA-RESILIENCE] Migrating data from user_id={source_user_id} → user_id={target_user_id}")
    
    for coll_name in collections:
        try:
            collection = _dr_db[coll_name]
            
            # Count documents to migrate
            count = await collection.count_documents({"user_id": source_user_id})
            if count == 0:
                # Try with "uid" field instead
                count = await collection.count_documents({"uid": source_user_id})
                if count == 0:
                    results[coll_name] = {"migrated": 0, "skipped": True}
                    continue
                # Migrate using "uid" field
                result = await collection.update_many(
                    {"uid": source_user_id},
                    {"$set": {"uid": target_user_id}}
                )
                results[coll_name] = {"migrated": result.modified_count, "field": "uid"}
                print(f"[DATA-RESILIENCE] {coll_name}: migrated {result.modified_count} docs (uid field)")
            else:
                # Migrate using "user_id" field
                result = await collection.update_many(
                    {"user_id": source_user_id},
                    {"$set": {"user_id": target_user_id}}
                )
                results[coll_name] = {"migrated": result.modified_count, "field": "user_id"}
                print(f"[DATA-RESILIENCE] {coll_name}: migrated {result.modified_count} docs (user_id field)")
                
        except Exception as e:
            results[coll_name] = {"error": str(e)}
            print(f"[DATA-RESILIENCE] {coll_name}: ERROR: {e}")
    
    total_migrated = sum(r.get("migrated", 0) for r in results.values())
    print(f"[DATA-RESILIENCE] Migration complete: {total_migrated} total documents migrated")
    return results


async def get_all_user_ids_with_data():
    """Find ALL user IDs that have data in any critical collection.
    
    Returns: dict {user_id: {collection_name: doc_count}}
    """
    user_data_map = {}
    
    for coll_name in CRITICAL_COLLECTIONS.keys():
        try:
            collection = _dr_db[coll_name]
            distinct_uids = []
            try:
                distinct_uids = await collection.distinct("user_id")
            except Exception:
                try:
                    distinct_uids = await collection.distinct("uid")
                except Exception:
                    continue
            
            for uid in distinct_uids:
                if uid not in user_data_map:
                    user_data_map[uid] = {}
                count = await collection.count_documents(
                    {"user_id": uid} if "user_id" in await collection.index_information() else {"uid": uid}
                )
                # Fallback count
                if count == 0:
                    count = await collection.count_documents({"user_id": uid})
                if count == 0:
                    count = await collection.count_documents({"uid": uid})
                user_data_map[uid][coll_name] = count
                
        except Exception:
            continue
    
    return user_data_map


async def ensure_owner_has_auth():
    """Make sure the OWNER_ID is in the auth_users collection.
    
    This prevents the "Total auth users: 0" issue where the owner
    can't use the bot because auth_users is empty.
    """
    for owner_id in OWNER_ID:
        try:
            existing = await _dr_db["auth_users"].find_one({"user_id": owner_id})
            if not existing:
                await _dr_db["auth_users"].update_one(
                    {"user_id": owner_id},
                    {"$set": {
                        "user_id": owner_id,
                        "added_by": owner_id,
                        "added_at": datetime.utcnow(),
                        "auto_added": True,
                        "reason": "Owner auto-added by data_resilience on startup",
                    }},
                    upsert=True,
                )
                print(f"[DATA-RESILIENCE] ✅ Auto-added owner {owner_id} to auth_users")
            else:
                print(f"[DATA-RESILIENCE] Owner {owner_id} already in auth_users ✅")
        except Exception as e:
            print(f"[DATA-RESILIENCE] Failed to ensure owner {owner_id} in auth_users: {e}")


async def snapshot_data_counts():
    """Quick snapshot of data counts for logging.
    Returns a one-line summary string for startup logs.
    """
    parts = []
    for coll_name in ["fetch_maps", "upload_maps", "auth_users", "relink_fingerprints", "mirrored_messages_index"]:
        try:
            count = await _dr_db[coll_name].count_documents({})
            if count > 0:
                parts.append(f"{coll_name}={count}")
        except Exception:
            pass
    
    return " | ".join(parts) if parts else "NO DATA in critical collections"
