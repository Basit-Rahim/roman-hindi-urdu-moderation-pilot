"""Step 1 of the pilot: collect candidate Romanized Hindi-Urdu posts from Bluesky.

Source A (default): Bluesky Jetstream live stream (no authentication).
Source B (optional): app.bsky.feed.searchPosts with an App Password.

Usage:
    python collect.py                    # run until target_count posts are stored
    python collect.py --test-minutes 5   # short test run into data/raw/test/
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
import websockets
import yaml
from dotenv import load_dotenv

from filters import KeywordMatcher, detect_script, load_keywords

ROOT = Path(__file__).resolve().parent
POST_COLLECTION = "app.bsky.feed.post"
APP_PASSWORD_RE = re.compile(r"^[a-z0-9]{4}(-[a-z0-9]{4}){3}$")


# --------------------------------------------------------------------------- settings

@dataclass
class Settings:
    target_count: int = 10_000
    min_matches: int = 2
    keywords_file: Path = ROOT / "config/keywords.txt"
    output_dir: Path = ROOT / "data/raw"
    progress_every: int = 100
    heartbeat_s: float = 60
    jetstream_enabled: bool = True
    jetstream_url: str = (
        "wss://jetstream.us-east.bsky.network/xrpc/"
        "network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post"
    )
    checkpoint_interval_s: float = 5
    cursor_rewind_s: float = 5
    backoff_initial_s: float = 1
    backoff_max_s: float = 60
    record_deletes: str = "collected"
    search_enabled: bool = False
    search_host: str = "https://bsky.social"
    search_queries_file: Path = ROOT / "config/search_queries.txt"
    search_page_limit: int = 100
    search_max_pages: int = 0          # 0 = paginate fully
    search_sort: str = "latest"
    search_delay_s: float = 1
    search_max_retries: int = 5


def _env_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings(path: Path) -> Settings:
    """Read settings.yaml, then apply environment-variable overrides."""
    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    raw = raw or {}
    js: dict[str, Any] = raw.get("jetstream") or {}
    se: dict[str, Any] = raw.get("search") or {}
    s = Settings()
    s.target_count = int(raw.get("target_count", s.target_count))
    s.min_matches = int(raw.get("min_matches", s.min_matches))
    s.keywords_file = ROOT / raw.get("keywords_file", "config/keywords.txt")
    s.output_dir = ROOT / raw.get("output_dir", "data/raw")
    s.progress_every = int(raw.get("progress_every", s.progress_every))
    s.heartbeat_s = float(raw.get("heartbeat_s", s.heartbeat_s))
    s.jetstream_enabled = bool(js.get("enabled", s.jetstream_enabled))
    s.jetstream_url = str(js.get("url", s.jetstream_url))
    s.checkpoint_interval_s = float(js.get("checkpoint_interval_s", s.checkpoint_interval_s))
    s.cursor_rewind_s = float(js.get("cursor_rewind_s", s.cursor_rewind_s))
    s.backoff_initial_s = float(js.get("backoff_initial_s", s.backoff_initial_s))
    s.backoff_max_s = float(js.get("backoff_max_s", s.backoff_max_s))
    s.record_deletes = str(js.get("record_deletes", s.record_deletes))
    s.search_enabled = bool(se.get("enabled", s.search_enabled))
    s.search_host = str(se.get("host", s.search_host)).rstrip("/")
    s.search_queries_file = ROOT / se.get("queries_file", "config/search_queries.txt")
    s.search_page_limit = int(se.get("page_limit", s.search_page_limit))
    s.search_max_pages = int(se.get("max_pages") or 0)
    s.search_sort = str(se.get("sort", s.search_sort))
    s.search_delay_s = float(se.get("delay_s", s.search_delay_s))
    s.search_max_retries = int(se.get("max_retries", s.search_max_retries))

    env = os.environ
    if env.get("TARGET_COUNT"):
        s.target_count = int(env["TARGET_COUNT"])
    if env.get("MIN_MATCHES"):
        s.min_matches = int(env["MIN_MATCHES"])
    if env.get("JETSTREAM_URL"):
        s.jetstream_url = env["JETSTREAM_URL"]
    if env.get("JETSTREAM_ENABLED"):
        s.jetstream_enabled = _env_bool(env["JETSTREAM_ENABLED"])
    if env.get("SEARCH_ENABLED"):
        s.search_enabled = _env_bool(env["SEARCH_ENABLED"])
    if s.record_deletes not in {"collected", "all"}:
        raise ValueError("jetstream.record_deletes must be 'collected' or 'all'")
    return s


# --------------------------------------------------------------------------- helpers

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


_ISO_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?$")


def iso_to_us(value: str) -> int | None:
    """Parse an ISO-8601 timestamp (any fraction length) into Unix microseconds."""
    m = _ISO_RE.match(value or "")
    if not m:
        return None
    base, frac, tz = m.groups()
    dt = datetime.fromisoformat(base + (tz if tz and tz != "Z" else "+00:00"))
    micros = int((frac or "0")[:6].ljust(6, "0"))
    return int(dt.timestamp()) * 1_000_000 + micros


def post_uri(did: str, rkey: str) -> str:
    return f"at://{did}/{POST_COLLECTION}/{rkey}"


def append_jsonl(fh: Any, obj: dict[str, Any]) -> None:
    fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
    fh.flush()


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue  # tolerate a half-written last line after a crash


def build_post(
    *, did: str, rkey: str, record: dict[str, Any], source: str,
    matcher: KeywordMatcher, min_matches: int,
) -> dict[str, Any] | None:
    """Return the stored row for a post record, or None if it fails the filter."""
    if record.get("$type", POST_COLLECTION) != POST_COLLECTION:
        return None
    text = record.get("text") or ""
    matched = matcher.find(text)
    if len(matched) < min_matches:
        return None
    return {
        "uri": post_uri(did, rkey),
        "did": did,
        "rkey": rkey,
        "text": text,
        "langs": record.get("langs"),  # raw value; None if the field is missing
        "created_at": record.get("createdAt"),
        "collected_at": utc_now_iso(),
        "is_reply": "reply" in record and record["reply"] is not None,
        "matched_keywords": matched,
        "match_count": len(matched),
        "script": detect_script(text),
        "source": source,
    }


# --------------------------------------------------------------------------- Jetstream events

@dataclass
class CommitEvent:
    operation: str            # "create" | "update" | "delete"
    collection: str
    did: str
    rkey: str
    record: dict[str, Any] | None
    time_us: int | None
    seq: int | None


@dataclass
class AccountEvent:
    did: str
    active: bool
    status: str | None
    time_us: int | None
    seq: int | None


def parse_event(msg: dict[str, Any]) -> CommitEvent | AccountEvent | None:
    """Normalise a Jetstream v1 or v2 message. Returns None for anything else."""
    if msg.get("$type") == "message" and isinstance(msg.get("payload"), dict):  # v2
        p = msg["payload"]
        kind = str(p.get("$type", "")).rsplit("#", 1)[-1]
        time_us = iso_to_us(p.get("time", ""))
        seq = p.get("seq")
        if kind == "commit":
            return CommitEvent(p.get("operation", ""), p.get("collection", ""), p.get("did", ""),
                               p.get("rkey", ""), p.get("record"), time_us, seq)
        if kind == "account":
            return AccountEvent(p.get("did", ""), bool(p.get("active", True)), p.get("status"),
                                time_us, seq)
        return None
    kind = msg.get("kind")  # v1
    time_us = msg.get("time_us")
    seq = msg.get("cursor")
    if kind == "commit" and isinstance(msg.get("commit"), dict):
        c = msg["commit"]
        return CommitEvent(c.get("operation", ""), c.get("collection", ""), msg.get("did", ""),
                           c.get("rkey", ""), c.get("record"), time_us, seq)
    if kind == "account" and isinstance(msg.get("account"), dict):
        a = msg["account"]
        return AccountEvent(a.get("did", msg.get("did", "")), bool(a.get("active", True)),
                            a.get("status"), time_us, seq)
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


# --------------------------------------------------------------------------- collector state

@dataclass
class Stats:
    started: float = field(default_factory=time.monotonic)
    scanned: int = 0           # post create events inspected
    kept_session: int = 0
    duplicates: int = 0
    deletes_logged: int = 0

    def per_hour(self, n: int) -> float:
        hours = max(time.monotonic() - self.started, 1e-9) / 3600
        return n / hours


class Collector:
    """Holds output files, dedupe sets, cursor and counters."""

    def __init__(self, settings: Settings, out_dir: Path, examples_to_keep: int = 0) -> None:
        self.s = settings
        self.matcher = KeywordMatcher(load_keywords(settings.keywords_file))
        out_dir.mkdir(parents=True, exist_ok=True)
        self.posts_path = out_dir / "posts.jsonl"
        self.deleted_path = out_dir / "deleted.jsonl"
        self.checkpoint_path = out_dir / "checkpoint.json"
        self.uris: set[str] = set()
        self.dids: set[str] = set()
        for row in read_jsonl(self.posts_path):
            self.uris.add(row["uri"])
            self.dids.add(row["did"])
        self.logged_deletes = {r.get("uri") or r.get("did") for r in read_jsonl(self.deleted_path)}
        self.cursor = self._load_checkpoint()
        self.stats = Stats()
        self.examples: list[dict[str, Any]] = []   # in memory only, for the test summary
        self.examples_to_keep = examples_to_keep
        self._posts_fh = self.posts_path.open("a", encoding="utf-8")
        self._deleted_fh = self.deleted_path.open("a", encoding="utf-8")
        self._last_checkpoint = time.monotonic()
        self._last_heartbeat = time.monotonic()

    # -- lifecycle
    def close(self) -> None:
        self.save_checkpoint()
        self._posts_fh.close()
        self._deleted_fh.close()

    @property
    def total(self) -> int:
        return len(self.uris)

    def done(self) -> bool:
        return self.total >= self.s.target_count

    # -- checkpoint
    def _load_checkpoint(self) -> dict[str, Any]:
        try:
            return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_checkpoint(self) -> None:
        if not self.cursor:
            return
        tmp = self.checkpoint_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({**self.cursor, "saved_at": utc_now_iso()}), encoding="utf-8")
        os.replace(tmp, self.checkpoint_path)
        self._last_checkpoint = time.monotonic()

    def resume_url(self, base_url: str) -> str:
        """Base URL plus the cursor to resume from (seq for v2, time_us for v1)."""
        if is_v2_url(base_url) and self.cursor.get("seq") is not None:
            return with_cursor(base_url, int(self.cursor["seq"]))  # inclusive; dedupe handles it
        if self.cursor.get("time_us") is not None:
            rewind = int(self.s.cursor_rewind_s * 1_000_000)
            return with_cursor(base_url, int(self.cursor["time_us"]) - rewind)
        return base_url

    # -- storing
    def store(self, row: dict[str, Any]) -> bool:
        if row["uri"] in self.uris:
            self.stats.duplicates += 1
            return False
        append_jsonl(self._posts_fh, row)
        self.uris.add(row["uri"])
        self.dids.add(row["did"])
        self.stats.kept_session += 1
        if len(self.examples) < self.examples_to_keep:
            self.examples.append(row)
        if self.stats.kept_session % self.s.progress_every == 0:
            self.print_progress()
        return True

    def log_post_delete(self, did: str, rkey: str, time_us: int | None) -> None:
        uri = post_uri(did, rkey)
        if uri in self.logged_deletes:
            return
        if self.s.record_deletes == "collected" and uri not in self.uris:
            return
        append_jsonl(self._deleted_fh, {"type": "post", "uri": uri, "did": did,
                                        "event_time_us": time_us, "logged_at": utc_now_iso()})
        self.logged_deletes.add(uri)
        self.stats.deletes_logged += 1

    def log_account_gone(self, ev: AccountEvent) -> None:
        """Log deactivated/deleted/taken-down accounts whose posts we hold."""
        if ev.active or ev.did not in self.dids or ev.did in self.logged_deletes:
            return
        append_jsonl(self._deleted_fh, {"type": "account", "did": ev.did, "status": ev.status,
                                        "event_time_us": ev.time_us, "logged_at": utc_now_iso()})
        self.logged_deletes.add(ev.did)
        self.stats.deletes_logged += 1

    # -- progress
    def print_progress(self, label: str = "progress") -> None:
        st = self.stats
        print(f"[{label}] kept total={self.total} (this run {st.kept_session}) | "
              f"scanned={st.scanned:,} | {st.per_hour(st.kept_session):,.0f} kept/h, "
              f"{st.per_hour(st.scanned):,.0f} scanned/h | deletes logged={st.deletes_logged}",
              flush=True)
        self._last_heartbeat = time.monotonic()

    # -- Jetstream
    def handle_message(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        ev = parse_event(msg)
        if ev is None:
            if "error" in msg or str(msg.get("$type", "")).endswith("error"):
                print(f"[jetstream] server message: {str(msg)[:200]}", flush=True)
            return
        if isinstance(ev, CommitEvent) and ev.collection == POST_COLLECTION:
            if ev.operation == "create" and ev.record:
                self.stats.scanned += 1
                row = build_post(did=ev.did, rkey=ev.rkey, record=ev.record, source="jetstream",
                                 matcher=self.matcher, min_matches=self.s.min_matches)
                if row:
                    self.store(row)
            elif ev.operation == "delete":
                self.log_post_delete(ev.did, ev.rkey, ev.time_us)
        elif isinstance(ev, AccountEvent):
            self.log_account_gone(ev)
        # Cursor advances only after the event has been fully handled and written.
        if ev.time_us is not None or ev.seq is not None:
            self.cursor = {"seq": ev.seq, "time_us": ev.time_us}
        now = time.monotonic()
        if now - self._last_checkpoint >= self.s.checkpoint_interval_s:
            self.save_checkpoint()
        if now - self._last_heartbeat >= self.s.heartbeat_s:
            self.print_progress("status")


class StreamHandler(Protocol):
    """Anything run_jetstream can drive (Collector, prevalence.Prevalence)."""
    s: Settings

    def handle_message(self, raw: str | bytes) -> None: ...
    def done(self) -> bool: ...
    def resume_url(self, base_url: str) -> str: ...
    def save_checkpoint(self) -> None: ...


async def run_jetstream(c: StreamHandler) -> None:
    """Consume Jetstream until c.done(), reconnecting with exponential backoff."""
    base = c.s.jetstream_url
    subprotocols = ["xrpc.v1.json"] if is_v2_url(base) else None
    backoff = c.s.backoff_initial_s
    while not c.done():
        url = c.resume_url(base)
        print(f"[jetstream] connecting: {url}", flush=True)
        try:
            async with websockets.connect(url, subprotocols=subprotocols, open_timeout=20,
                                          ping_interval=20, ping_timeout=30,
                                          max_size=4 * 1024 * 1024) as ws:
                print("[jetstream] connected", flush=True)
                backoff = c.s.backoff_initial_s
                async for raw in ws:
                    c.handle_message(raw)
                    if c.done():
                        return
            print("[jetstream] server closed the connection", flush=True)
        except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as exc:
            print(f"[jetstream] connection error: {type(exc).__name__}: {exc}", flush=True)
        c.save_checkpoint()
        delay = backoff * (0.5 + random.random() / 2)
        print(f"[jetstream] reconnecting in {delay:.1f}s", flush=True)
        await asyncio.sleep(delay)
        backoff = min(backoff * 2, c.s.backoff_max_s)


# --------------------------------------------------------------------------- Source B: search

class SearchClient:
    """Minimal authenticated client for app.bsky.feed.searchPosts on bsky.social."""

    def __init__(self, s: Settings, handle: str, app_password: str) -> None:
        self.s = s
        self.handle = handle
        self.app_password = app_password
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "research-pilot-collector/0.1"
        self.token: str | None = None

    def login(self) -> None:
        r = self.http.post(f"{self.s.search_host}/xrpc/com.atproto.server.createSession",
                           json={"identifier": self.handle, "password": self.app_password},
                           timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"login failed ({r.status_code}): {r.text[:200]}")
        self.token = r.json()["accessJwt"]

    def _get(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        delay = self.s.search_delay_s
        for attempt in range(self.s.search_max_retries + 1):
            r = self.http.get(f"{self.s.search_host}/xrpc/{method}", params=params,
                              headers={"Authorization": f"Bearer {self.token}"}, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 400 and "ExpiredToken" in r.text:
                self.login()
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = _retry_after(r) or delay
                print(f"[search] HTTP {r.status_code}; retry {attempt + 1} in {wait:.0f}s",
                      flush=True)
                time.sleep(wait)
                delay = min(delay * 2, 300)
                continue
            raise RuntimeError(f"{method} failed ({r.status_code}): {r.text[:200]}")
        raise RuntimeError(f"{method}: giving up after {self.s.search_max_retries} retries")

    def search(self, query: str) -> Iterator[list[dict[str, Any]]]:
        """Yield result pages until the API stops returning a cursor.

        search_max_pages <= 0 means no page limit (paginate fully).
        """
        cursor: str | None = None
        pages = 0
        seen_cursors: set[str] = set()
        while self.s.search_max_pages <= 0 or pages < self.s.search_max_pages:
            params: dict[str, Any] = {"q": query, "limit": self.s.search_page_limit,
                                      "sort": self.s.search_sort}
            if cursor:
                params["cursor"] = cursor
            data = self._get("app.bsky.feed.searchPosts", params)
            pages += 1
            posts = data.get("posts", [])
            yield posts
            cursor = data.get("cursor")
            time.sleep(self.s.search_delay_s)
            if not cursor or not posts or cursor in seen_cursors:
                return
            seen_cursors.add(cursor)


def _retry_after(r: requests.Response) -> float | None:
    if r.headers.get("Retry-After", "").isdigit():
        return float(r.headers["Retry-After"])
    reset = r.headers.get("RateLimit-Reset") or r.headers.get("ratelimit-reset")
    if reset and reset.isdigit():
        return max(float(reset) - time.time(), 1.0)
    return None


def load_queries(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [q.strip() for q in lines if q.strip() and not q.strip().startswith("#")]


@dataclass
class QueryReport:
    query: str
    pages: int = 0
    found: int = 0        # posts returned by the API
    passed: int = 0       # passed the keyword filter
    kept_new: int = 0     # passed and not already stored
    complete: bool = False

    def line(self) -> str:
        state = "complete" if self.complete else "stopped early"
        return (f"[search] '{self.query}': pages={self.pages} found={self.found} "
                f"passed filter={self.passed} new kept={self.kept_new} ({state})")


def run_search(c: Collector, deadline: float | None = None) -> list[QueryReport]:
    """Run every query in the queries file; stop at the target or at the deadline."""
    handle = os.environ.get("BSKY_HANDLE", "").strip()
    password = os.environ.get("BSKY_APP_PASSWORD", "").strip()
    if not handle or not password:
        raise SystemExit("Search enabled but BSKY_HANDLE / BSKY_APP_PASSWORD are not set in .env")
    if not APP_PASSWORD_RE.match(password):
        raise SystemExit("BSKY_APP_PASSWORD does not look like an App Password (xxxx-xxxx-xxxx-xxxx). "
                         "Create one in Bluesky settings; never use your main password.")
    client = SearchClient(c.s, handle, password)
    client.login()
    print(f"[search] logged in to {c.s.search_host}", flush=True)
    reports: list[QueryReport] = []
    for query in load_queries(c.s.search_queries_file):
        rep = QueryReport(query)
        reports.append(rep)
        print(f"[search] running '{query}'", flush=True)
        for page in client.search(query):
            rep.pages += 1
            for view in page:
                rep.found += 1
                c.stats.scanned += 1
                uri = view.get("uri", "")
                row = build_post(did=view.get("author", {}).get("did", ""),
                                 rkey=uri.rsplit("/", 1)[-1], record=view.get("record", {}),
                                 source="search", matcher=c.matcher,
                                 min_matches=c.s.min_matches)
                if row:
                    rep.passed += 1
                    rep.kept_new += c.store(row)
            if c.done() or (deadline is not None and time.monotonic() >= deadline):
                print(rep.line(), flush=True)
                return reports
        rep.complete = True
        print(rep.line(), flush=True)
    return reports


# --------------------------------------------------------------------------- main

def print_examples(c: Collector, n: int) -> None:
    print(f"\n=== {min(n, len(c.examples))} example kept posts (terminal only, not saved) ===")
    for row in c.examples[:n]:
        text = " ".join(row["text"].split())
        print(f"- langs={row['langs']} script={row['script']} "
              f"keywords={row['matched_keywords']}\n  {text[:280]}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--settings", type=Path, default=ROOT / "config/settings.yaml")
    p.add_argument("--test-minutes", type=float, default=None,
                   help="test mode: run for N minutes, write to data/raw/test/, show examples")
    p.add_argument("--examples", type=int, default=5, help="examples shown at the end of test mode")
    return p.parse_args(argv)


async def _run_for(c: Collector, seconds: float | None) -> None:
    if seconds is None:
        await run_jetstream(c)
        return
    try:
        await asyncio.wait_for(run_jetstream(c), timeout=seconds)
    except asyncio.TimeoutError:
        print(f"[test] time limit of {seconds / 60:.1f} min reached", flush=True)


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Windows consoles
    load_dotenv(ROOT / ".env")
    args = parse_args(argv)
    s = load_settings(args.settings)
    test_mode = args.test_minutes is not None
    out_dir = s.output_dir / "test" if test_mode else s.output_dir
    c = Collector(s, out_dir, examples_to_keep=args.examples if test_mode else 0)
    print(f"Output: {c.posts_path} (already stored: {c.total}) | target={s.target_count} | "
          f"min distinct keywords={s.min_matches} | keywords={len(load_keywords(s.keywords_file))}",
          flush=True)
    budget_s = args.test_minutes * 60 if test_mode else None
    deadline = time.monotonic() + budget_s if budget_s is not None else None
    reports: list[QueryReport] = []
    try:
        if s.search_enabled and not c.done():
            reports = run_search(c, deadline)
        remaining = None if deadline is None else deadline - time.monotonic()
        if s.jetstream_enabled and not c.done() and (remaining is None or remaining > 0):
            asyncio.run(_run_for(c, remaining))
    except KeyboardInterrupt:
        print("\n[stopped by user]")
    finally:
        c.close()
        c.print_progress("final")
        if reports:
            print_search_summary(reports)
        if c.done():
            print(f"Target of {s.target_count} posts reached.")
        if test_mode:
            print_examples(c, args.examples)


def print_search_summary(reports: list[QueryReport]) -> None:
    print("\n=== search summary ===")
    for rep in reports:
        print("  " + rep.line().removeprefix("[search] "))
    print(f"  queries run: {len(reports)} ({sum(r.complete for r in reports)} complete) | "
          f"found={sum(r.found for r in reports)} passed filter={sum(r.passed for r in reports)} "
          f"new kept={sum(r.kept_new for r in reports)}")


if __name__ == "__main__":
    main()
