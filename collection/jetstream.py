"""Bluesky Jetstream client (v1 and v2 wire formats), used by prevalence mode."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import websockets

from common.time_utils import iso_to_us


@dataclass
class CommitEvent:
    operation: str            # "create" | "update" | "delete"
    collection: str
    did: str
    rkey: str
    record: dict[str, Any] | None
    time_us: int | None
    seq: int | None


def parse_event(msg: dict[str, Any]) -> CommitEvent | None:
    """Normalise a Jetstream v1 or v2 commit message. Returns None for anything else."""
    if msg.get("$type") == "message" and isinstance(msg.get("payload"), dict):  # v2
        p = msg["payload"]
        if str(p.get("$type", "")).rsplit("#", 1)[-1] != "commit":
            return None
        return CommitEvent(p.get("operation", ""), p.get("collection", ""), p.get("did", ""),
                           p.get("rkey", ""), p.get("record"), iso_to_us(p.get("time", "")),
                           p.get("seq"))
    if msg.get("kind") == "commit" and isinstance(msg.get("commit"), dict):  # v1
        c = msg["commit"]
        return CommitEvent(c.get("operation", ""), c.get("collection", ""), msg.get("did", ""),
                           c.get("rkey", ""), c.get("record"), msg.get("time_us"),
                           msg.get("cursor"))
    return None


def is_v2_url(url: str) -> bool:
    return "/xrpc/" in url


def with_cursor(url: str, cursor: int | None) -> str:
    """Return url with its cursor query parameter set (or unchanged if cursor is None)."""
    if cursor is None:
        return url
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "cursor"]
    query.append(("cursor", str(cursor)))
    return urlunsplit(parts._replace(query=urlencode(query)))


class StreamHandler(Protocol):
    """What run_jetstream needs from its consumer."""

    def handle_message(self, raw: str | bytes) -> None: ...
    def done(self) -> bool: ...
    def resume_url(self, base_url: str) -> str: ...
    def save_checkpoint(self) -> None: ...


async def run_jetstream(handler: StreamHandler, url: str, backoff_initial_s: float,
                        backoff_max_s: float) -> None:
    """Consume Jetstream until handler.done(), reconnecting with exponential backoff."""
    subprotocols = ["xrpc.v1.json"] if is_v2_url(url) else None
    backoff = backoff_initial_s
    while not handler.done():
        resume = handler.resume_url(url)
        print(f"[jetstream] connecting: {resume}", flush=True)
        try:
            async with websockets.connect(resume, subprotocols=subprotocols, open_timeout=20,
                                          ping_interval=20, ping_timeout=30,
                                          max_size=4 * 1024 * 1024) as ws:
                print("[jetstream] connected", flush=True)
                backoff = backoff_initial_s
                async for raw in ws:
                    handler.handle_message(raw)
                    if handler.done():
                        return
            print("[jetstream] server closed the connection", flush=True)
        except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as exc:
            print(f"[jetstream] connection error: {type(exc).__name__}: {exc}", flush=True)
        handler.save_checkpoint()
        delay = backoff * (0.5 + random.random() / 2)
        print(f"[jetstream] reconnecting in {delay:.1f}s", flush=True)
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, backoff_max_s)
