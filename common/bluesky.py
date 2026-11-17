"""Bluesky / AT Protocol constants."""
from __future__ import annotations

POST_COLLECTION = "app.bsky.feed.post"
BSKY_MODERATION_DID = "did:plc:ar7c4by46qjdydhdevvrndac"   # Bluesky's own moderation service


def post_uri(did: str, rkey: str) -> str:
    return f"at://{did}/{POST_COLLECTION}/{rkey}"
