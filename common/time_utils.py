"""Timestamp helpers."""
from __future__ import annotations

import re
from datetime import datetime, timezone

_ISO_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_to_us(value: str) -> int | None:
    """Parse an ISO-8601 timestamp (any fraction length) into Unix microseconds."""
    m = _ISO_RE.match(value or "")
    if not m:
        return None
    base, frac, tz = m.groups()
    dt = datetime.fromisoformat(base + (tz if tz and tz != "Z" else "+00:00"))
    micros = int((frac or "0")[:6].ljust(6, "0"))
    return int(dt.timestamp()) * 1_000_000 + micros
