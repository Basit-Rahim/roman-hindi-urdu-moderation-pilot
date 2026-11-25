"""Jetstream v2 replay client: archive listing, snapshot planning and block decoding.

Replay reads Jetstream's sealed archive over HTTP instead of the live WebSocket.
Docs: https://bsky.network/docs/jetstream-replay. Format details checked against
real segments (Nov 2026):

- A segment file (.jss) is a 256-byte header, then blocks, then a footer.
  Each block is an 8-byte little-endian length followed by one zstd frame.
- A decompressed block is columnar: event_count, then fixed-width columns
  (seq, witnessed_at, indexed_at, kind, lengths), then variable columns
  (collections, dids, rkeys, revs, payloads). Payloads are DAG-CBOR records.
- kind: 1 create, 2 update, 3 delete, 4 identity, 5 account, 6 sync.
  kind 7 is undocumented; observed blocks hold one account's old records
  re-sent at once (a resync), so they are not new posts.
- Sealed segments can be rewritten later (seen Nov 2026: a segment shrank
  overnight, probably as deleted records were purged). Byte offsets and block
  numbers then shift; sequence numbers do not. The header checksum changes, so
  a rewrite can be detected by comparing it with the plan's `checksum`.

Calls need an API key (JETSTREAM_API_KEY), sent as a Bearer token. Downloads are
metered by compressed bytes; over quota the server answers 429 + Retry-After.

The decoder never returns DIDs, rkeys or revs: it skips those columns.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Iterator

import requests
import zstandard

XRPC = "/xrpc/network.bsky.jetstream."
HEADER_SIZE = 256
HEADER_FMT = "<4sQHIIIQQqqQQQQQ"            # packed, little-endian; see SegmentHeader
INDEX_ENTRY_FMT = "<QIIIQQqq"               # 52 bytes per block
INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FMT)

KIND_NAMES = {1: "create", 2: "update", 3: "delete", 4: "identity", 5: "account", 6: "sync",
              7: "resync"}


class ReplayError(Exception):
    """Permanent failure (bad key, missing segment, bad data). Not retried."""


class TransientError(Exception):
    """Retryable failure (network, 5xx, 429). retry_after_s is set for 429."""

    def __init__(self, msg: str, retry_after_s: float | None = None) -> None:
        super().__init__(msg)
        self.retry_after_s = retry_after_s


# --------------------------------------------------------------------------- decoding

@dataclass(frozen=True)
class SegmentHeader:
    checksum: str               # 16 hex chars; equals listSegments/planSnapshot `checksum`
    block_count: int
    event_count: int
    min_seq: int
    max_seq: int
    min_time_us: int
    max_time_us: int
    footer_offset: int
    block_index_offset: int


def parse_header(raw: bytes) -> SegmentHeader:
    if len(raw) < struct.calcsize(HEADER_FMT):
        raise ReplayError(f"segment header too short ({len(raw)} bytes)")
    (magic, checksum, _version, block_count, event_count, _dids, min_seq, max_seq,
     min_wat, max_wat, footer_off, _bloom, _block_bloom, _coll_idx, block_idx_off) = \
        struct.unpack_from(HEADER_FMT, raw, 0)
    if magic != b"jss0":
        raise ReplayError(f"not a Jetstream segment (magic {magic!r})")
    return SegmentHeader(f"{checksum:016x}", block_count, event_count, min_seq, max_seq,
                         min_wat, max_wat, footer_off, block_idx_off)


@dataclass(frozen=True)
class BlockIndexEntry:
    offset: int                 # file offset of the block's 8-byte length prefix
    compressed_size: int
    event_count: int
    min_seq: int
    max_seq: int
    min_time_us: int
    max_time_us: int


def parse_block_index(raw: bytes) -> list[BlockIndexEntry]:
    out = []
    for i in range(len(raw) // INDEX_ENTRY_SIZE):
        off, csize, _usize, n, smin, smax, tmin, tmax = struct.unpack_from(
            INDEX_ENTRY_FMT, raw, i * INDEX_ENTRY_SIZE)
        out.append(BlockIndexEntry(off, csize, n, smin, smax, tmin, tmax))
    return out


@dataclass(frozen=True)
class BlockEvent:
    """One archived event, without any account identifiers."""
    seq: int
    time_us: int                # witnessed_at (equals the live stream's time_us)
    kind: int
    collection: str
    payload: bytes              # DAG-CBOR record; empty for deletes


_DCTX = zstandard.ZstdDecompressor()


def decompress_frame(frame: bytes) -> bytes:
    return _DCTX.decompressobj().decompress(frame)


def iter_block_events(data: bytes) -> Iterator[BlockEvent]:
    """Decode a decompressed block. DIDs, rkeys and revs are skipped, never returned."""
    n = struct.unpack_from("<I", data, 0)[0]
    off = 4

    def fixed(fmt: str, size: int) -> tuple[int, ...]:
        nonlocal off
        values = struct.unpack_from(f"<{n}{fmt}", data, off)
        off += n * size
        return values

    seqs = fixed("Q", 8)
    times = fixed("q", 8)
    fixed("q", 8)                                   # indexed_at (always 0)
    kinds = data[off:off + n]
    off += n
    coll_lens = data[off:off + n]
    off += n
    did_lens = fixed("H", 2)
    rkey_lens = data[off:off + n]
    off += n
    rev_lens = data[off:off + n]
    off += n
    payload_lens = fixed("I", 4)

    collections = []
    for length in coll_lens:
        collections.append(data[off:off + length].decode("utf-8", "replace"))
        off += length
    off += sum(did_lens) + sum(rkey_lens) + sum(rev_lens)   # skip identifiers
    for i in range(n):
        length = payload_lens[i]
        yield BlockEvent(seqs[i], times[i], kinds[i], collections[i], data[off:off + length])
        off += length
    if off != len(data):
        raise ReplayError(f"block decode mismatch: used {off} of {len(data)} bytes")


# --------------------------------------------------------------------------- HTTP client

class ReplayClient:
    """Thin client for the replay endpoints. Raises TransientError for retryable failures."""

    def __init__(self, host: str, api_key: str, timeout_s: float = 120) -> None:
        if not api_key:
            raise ReplayError("JETSTREAM_API_KEY is not set (put it in .env)")
        self.host = host.rstrip("/")
        self.timeout_s = timeout_s
        self.bytes_downloaded = 0
        self.session = requests.Session()
        # requests drops Authorization on the redirect to the CDN host, which is what we want.
        self.session.headers.update({"Authorization": f"Bearer {api_key}",
                                     "User-Agent": "research-prevalence-counter",
                                     "Accept-Encoding": "identity"})

    def _check(self, r: requests.Response) -> requests.Response:
        if r.status_code in (200, 206):
            return r
        body = " ".join(r.text.split())[:120]       # one line, even for HTML error pages
        if r.status_code == 429:
            raw = r.headers.get("Retry-After")
            raise TransientError(f"429 {body}", float(raw) if raw and raw.isdigit() else None)
        if r.status_code >= 500:
            raise TransientError(f"HTTP {r.status_code} {body}")
        if r.status_code == 401:
            raise ReplayError("401: the API key was rejected (check JETSTREAM_API_KEY)")
        raise ReplayError(f"HTTP {r.status_code} {body}")

    def _request(self, method: str, name: str, **kw: Any) -> requests.Response:
        try:
            r = self.session.request(method, self.host + XRPC + name, timeout=self.timeout_s, **kw)
        except requests.RequestException as exc:
            raise TransientError(f"{type(exc).__name__}: {exc}") from exc
        return self._check(r)

    def list_segments(self) -> list[dict[str, Any]]:
        segments: list[dict[str, Any]] = []
        cursor = None
        while True:
            params: dict[str, Any] = {"limit": 1000}
            if cursor:
                params["cursor"] = cursor
            out = self._request("GET", "listSegments", params=params).json()
            segments += out.get("segments", [])
            cursor = out.get("cursor")
            if not cursor or not out.get("segments"):
                return segments

    def plan(self, collections: list[str], after_seq: int, before_seq: int) -> tuple[list[dict], dict]:
        """planSnapshot, all pages. Returns (segments, last stats)."""
        body: dict[str, Any] = {"kinds": ["commit"], "collections": collections,
                                "afterSeq": after_seq, "beforeSeq": before_seq}
        segments: list[dict] = []
        while True:
            out = self._request("POST", "planSnapshot", json=body).json()
            segments += out["segments"]
            if out["plannedThroughSeq"] >= out["sealedTipSeq"] or not out["segments"]:
                return segments, out.get("stats", {})
            body["afterSeq"] = out["plannedThroughSeq"]
            body["beforeSeq"] = out["sealedTipSeq"]

    def _range(self, name: str, start: int, end: int) -> bytes:
        r = self._request("GET", "getSegment", params={"name": name},
                          headers={"Range": f"bytes={start}-{end}"})
        if r.status_code != 206:
            raise ReplayError(f"range request on {name} returned {r.status_code}")
        self.bytes_downloaded += len(r.content)
        return r.content

    def segment_header(self, name: str) -> SegmentHeader:
        return parse_header(self._range(name, 0, HEADER_SIZE - 1))

    def block_index(self, name: str, header: SegmentHeader) -> list[BlockIndexEntry]:
        size = header.block_count * INDEX_ENTRY_SIZE
        return parse_block_index(self._range(name, header.block_index_offset,
                                             header.block_index_offset + size - 1))

    def stream_blocks(self, name: str, start: int, end: int) -> Iterator[tuple[int, bytes]]:
        """Stream frames from file offset `start` up to `end` (exclusive).

        Yields (offset of the next block, zstd frame). Resume by passing that offset.
        """
        if start >= end:
            return
        try:
            r = self.session.get(self.host + XRPC + "getSegment", params={"name": name},
                                 headers={"Range": f"bytes={start}-{end - 1}"},
                                 timeout=self.timeout_s, stream=True)
        except requests.RequestException as exc:
            raise TransientError(f"{type(exc).__name__}: {exc}") from exc
        with r:
            self._check(r)
            if r.status_code != 206:
                raise ReplayError(f"range request on {name} returned {r.status_code}")
            pos = start
            while pos < end:
                length = struct.unpack("<Q", self._read_exact(r, 8))[0]
                frame = self._read_exact(r, length)
                pos += 8 + length
                yield pos, frame

    def _read_exact(self, r: requests.Response, n: int) -> bytes:
        chunks, got = [], 0
        try:
            while got < n:
                chunk = r.raw.read(n - got)
                if not chunk:
                    raise TransientError("connection closed mid-block")
                chunks.append(chunk)
                got += len(chunk)
        except (requests.RequestException, OSError) as exc:
            raise TransientError(f"{type(exc).__name__}: {exc}") from exc
        except Exception as exc:                    # urllib3 protocol / read-timeout errors
            if isinstance(exc, TransientError):
                raise
            raise TransientError(f"{type(exc).__name__}: {exc}") from exc
        self.bytes_downloaded += n
        return b"".join(chunks)

    def get_block(self, name: str, index: int) -> bytes:
        r = self._request("GET", "getBlock", params={"segment": name, "blockIndex": index})
        self.bytes_downloaded += len(r.content)
        return r.content
