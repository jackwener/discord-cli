"""Discord REST API v10 client using httpx."""

from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import httpx

from .config import API_BASE, get_token

# Discord epoch: 2015-01-01T00:00:00Z
DISCORD_EPOCH = 1420070400000


def snowflake_to_datetime(snowflake: int | str) -> datetime:
    """Convert a Discord snowflake ID to a UTC datetime."""
    ms = (int(snowflake) >> 22) + DISCORD_EPOCH
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def datetime_to_snowflake(dt: datetime) -> int:
    """Convert a datetime to a Discord snowflake ID (for use as 'after' param)."""
    ms = int(dt.timestamp() * 1000) - DISCORD_EPOCH
    return ms << 22


@asynccontextmanager
async def get_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Async context manager for an authenticated httpx client."""
    token = get_token()
    async with httpx.AsyncClient(
        base_url=API_BASE,
        headers={
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/133.0.0.0 Safari/537.36"
            ),
            "sec-ch-ua": '"Chromium";v="133", "Not(A:Brand";v="99", "Google Chrome";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        },
        timeout=30.0,
    ) as client:
        yield client


async def _handle_rate_limit(response: httpx.Response) -> None:
    """Sleep if we hit a rate limit, with jitter."""
    if response.status_code == 429:
        data = response.json()
        retry_after = data.get("retry_after", 1.0)
        await asyncio.sleep(retry_after + random.uniform(0.5, 2.0))
    elif remaining := response.headers.get("X-RateLimit-Remaining"):
        if int(remaining) == 0:
            reset_after = float(response.headers.get("X-RateLimit-Reset-After", "1.0"))
            await asyncio.sleep(reset_after + random.uniform(0.2, 1.0))


async def _get(client: httpx.AsyncClient, path: str, **params: Any) -> Any:
    """GET request with rate limit handling and retry."""
    for attempt in range(3):
        response = await client.get(path, params=params)
        if response.status_code == 429:
            await _handle_rate_limit(response)
            continue
        await _handle_rate_limit(response)
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"Rate limited after 3 retries: {path}")


async def list_guilds(client: httpx.AsyncClient) -> list[dict]:
    """List all guilds (servers) the user has joined."""
    data = await _get(client, "/users/@me/guilds")
    return [
        {
            "id": g["id"],
            "name": g["name"],
            "icon": g.get("icon"),
            "owner": g.get("owner", False),
        }
        for g in data
    ]


async def resolve_guild_id(client: httpx.AsyncClient, guild: str) -> str | None:
    """Resolve a guild name or ID string to a guild ID.

    Returns the guild ID if found, or None if not.
    """
    if guild.isdigit():
        return guild
    guilds = await list_guilds(client)
    match = next(
        (g for g in guilds if guild.lower() in g["name"].lower()),
        None,
    )
    return match["id"] if match else None


async def list_channels(client: httpx.AsyncClient, guild_id: str) -> list[dict]:
    """List all text channels in a guild."""
    data = await _get(client, f"/guilds/{guild_id}/channels")
    # type 0 = text channel, 5 = announcement, 15 = forum
    text_types = {0, 5, 15}
    results = []
    for ch in data:
        if ch.get("type") in text_types:
            results.append(
                {
                    "id": ch["id"],
                    "name": ch["name"],
                    "type": ch.get("type", 0),
                    "position": ch.get("position", 0),
                    "parent_id": ch.get("parent_id"),
                    "topic": ch.get("topic"),
                }
            )
    return sorted(results, key=lambda x: x["position"])


async def fetch_messages(
    client: httpx.AsyncClient,
    channel_id: str,
    *,
    limit: int = 1000,
    after: str | None = None,
    before: str | None = None,
) -> list[dict]:
    """Fetch messages from a channel, handling pagination.

    Discord returns max 100 messages per request, so we paginate.
    Two modes:
      - after mode: fetch messages newer than `after` ID (incremental sync)
      - before/default mode: fetch messages older-ward (history fetch)
    """
    all_messages: list[dict] = []
    remaining = limit
    use_after = after is not None

    while remaining > 0:
        batch_limit = min(remaining, 100)
        params: dict[str, Any] = {"limit": batch_limit}

        if use_after:
            params["after"] = after
        elif before:
            params["before"] = before

        data = await _get(client, f"/channels/{channel_id}/messages", **params)

        if not data:
            break

        for msg in data:
            all_messages.append(_parse_message(msg, channel_id))

        remaining -= len(data)

        if len(data) < batch_limit:
            break

        # Discord always returns messages newest-first.
        if use_after:
            # 'after' mode: move cursor to the newest message we've seen
            after = data[0]["id"]
        else:
            # 'before'/default mode: move cursor to the oldest message we've seen
            before = data[-1]["id"]

        # Small delay with jitter to be nice
        await asyncio.sleep(random.uniform(0.3, 1.0))

    # Sort by timestamp ascending
    all_messages.sort(key=lambda m: m["msg_id"])
    return all_messages


def _parse_message(msg: dict, channel_id: str) -> dict:
    """Parse a raw Discord message into our standard format."""
    author = msg.get("author", {})
    ts_str = msg.get("timestamp", "")
    timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    # Build content: message text + any attachment URLs
    content_parts = []
    if msg.get("content"):
        content_parts.append(msg["content"])
    for att in msg.get("attachments", []):
        content_parts.append(f"[attachment: {att.get('filename', 'file')}]")
    for embed in msg.get("embeds", []):
        if title := embed.get("title"):
            content_parts.append(f"[embed: {title}]")

    return {
        "msg_id": msg["id"],
        "channel_id": channel_id,
        "sender_id": author.get("id"),
        "sender_name": author.get("global_name") or author.get("username") or "Unknown",
        "content": "\n".join(content_parts),
        "timestamp": timestamp,
    }


async def get_guild_info(client: httpx.AsyncClient, guild_id: str) -> dict | None:
    """Get detailed guild info."""
    try:
        data = await _get(client, f"/guilds/{guild_id}", with_counts="true")
        return {
            "id": data["id"],
            "name": data["name"],
            "description": data.get("description"),
            "member_count": data.get("approximate_member_count"),
            "online_count": data.get("approximate_presence_count"),
        }
    except Exception:
        return None


async def get_me(client: httpx.AsyncClient) -> dict:
    """Get current user info."""
    data = await _get(client, "/users/@me")
    created_at = snowflake_to_datetime(data["id"])
    return {
        "id": data["id"],
        "username": data.get("username", "?"),
        "global_name": data.get("global_name"),
        "email": data.get("email"),
        "phone": data.get("phone"),
        "mfa_enabled": data.get("mfa_enabled", False),
        "premium_type": data.get("premium_type", 0),
        "created_at": created_at.isoformat(),
    }


async def get_user(client: httpx.AsyncClient, user_id: str) -> dict | None:
    """Get a user's profile."""
    try:
        data = await _get(client, f"/users/{user_id}")
        return {
            "id": data["id"],
            "username": data.get("username"),
            "global_name": data.get("global_name"),
            "bot": data.get("bot", False),
            "created_at": snowflake_to_datetime(data["id"]).isoformat(),
        }
    except Exception:
        return None


async def search_guild_messages(
    client: httpx.AsyncClient,
    guild_id: str,
    query: str,
    *,
    channel_id: str | None = None,
    limit: int = 25,
) -> list[dict]:
    """Search messages in a guild using Discord's built-in search."""
    params: dict[str, Any] = {"content": query}
    if channel_id:
        params["channel_id"] = channel_id

    data = await _get(client, f"/guilds/{guild_id}/messages/search", **params)
    results = []
    for group in data.get("messages", []):
        for msg in group:
            if msg.get("hit"):
                results.append(_parse_message(msg, msg.get("channel_id", "")))
    return results[:limit]


async def list_members(
    client: httpx.AsyncClient,
    guild_id: str,
    limit: int = 100,
) -> list[dict]:
    """List members of a guild."""
    data = await _get(client, f"/guilds/{guild_id}/members", limit=min(limit, 1000))
    return [
        {
            "id": m["user"]["id"],
            "username": m["user"].get("username"),
            "global_name": m["user"].get("global_name"),
            "nick": m.get("nick"),
            "joined_at": m.get("joined_at"),
            "bot": m["user"].get("bot", False),
        }
        for m in data
    ]
