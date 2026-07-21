"""
Shared MongoDB Client — FIX #7

Replaces 10+ separate AsyncIOMotorClient() instances across the codebase
with a single shared connection pool. Each separate client creates:
  - Its own connection pool (default: 100 connections)
  - Background task for server monitoring
  - Internal caches and buffers

With 10 separate clients, that's potentially 1000 connections to MongoDB Atlas
(free tier: 500 max). This module provides a single client shared by all modules.

Usage:
    from utils.mongo_client import get_collection, get_db

    upload_maps = get_collection("upload_maps")
    db = get_db()
"""

from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_DB as MONGO_URI, DB_NAME

# ONE client shared across entire bot
# maxPoolSize=50 is sufficient for all concurrent operations
_client = AsyncIOMotorClient(
    MONGO_URI,
    maxPoolSize=50,
    serverSelectionTimeoutMS=5000,
)
_db = _client[DB_NAME]


def get_collection(name: str):
    """Get a collection from the shared client."""
    return _db[name]


def get_db():
    """Get the shared database."""
    return _db


def get_client():
    """Get the shared client (rarely needed)."""
    return _client
