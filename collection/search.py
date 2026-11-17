"""Step 1: collect candidate Romanized Hindi-Urdu posts with Bluesky keyword search.

Runs app.bsky.feed.searchPosts for every query in config/search_queries.txt,
paginating fully, keeps posts with >= min_matches distinct keywords, and
appends them (deduplicated by uri) to data/raw/posts.jsonl.

Usage:
    python -m collection.search                    # run until target_count posts are stored
    python -m collection.search --test-minutes 10  # test run into data/raw/test/
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

from common.bluesky import POST_COLLECTION, post_uri
from common.config import DEFAULT_SETTINGS_PATH, ROOT, SearchSettings, Settings, load_settings
from common.files import append_jsonl, read_jsonl
from common.filters import KeywordMatcher, detect_script, load_keywords, record_has_link
from common.time_utils import utc_now_iso

APP_PASSWORD_RE = re.compile(r"^[a-z0-9]{4}(-[a-z0-9]{4}){3}$")


# --------------------------------------------------------------------------- post rows

def build_post(
    *, did: str, rkey: str, record: dict[str, Any], source: str,
    matcher: KeywordMatcher, min_matches: int,
    indexed_at: str | None = None, labels: list[dict[str, Any]] | None = None,
    author_labels: list[dict[str, Any]] | None = None,
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
        "created_at": record.get("createdAt"),      # set by the posting client
        "indexed_at": indexed_at,                   # set by the AppView
        "collected_at": utc_now_iso(),
        "is_reply": "reply" in record and record["reply"] is not None,
        "matched_keywords": matched,
        "match_count": len(matched),
        "script": detect_script(text),
        "has_link": record_has_link(record),
        "labels": labels,                # post labels from the API
        "author_labels": author_labels,  # account-level labels on the author
        "source": source,
    }


@dataclass
class Stats:
    started: float = field(default_factory=time.monotonic)
    scanned: int = 0           # posts returned by search and inspected
    kept_session: int = 0

    def per_hour(self, n: int) -> float:
        hours = max(time.monotonic() - self.started, 1e-9) / 3600
        return n / hours


class PostStore:
    """Appends kept posts to posts.jsonl, deduplicated by uri."""

    def __init__(self, s: Settings, out_dir: Path, examples_to_keep: int = 0) -> None:
        self.target_count = s.search.target_count
        self.progress_every = s.search.progress_every
        out_dir.mkdir(parents=True, exist_ok=True)
        self.posts_path = out_dir / "posts.jsonl"
        self.uris: set[str] = {row["uri"] for row in read_jsonl(self.posts_path)}
        self.stats = Stats()
        self.examples: list[dict[str, Any]] = []   # in memory only, for the test summary
        self.examples_to_keep = examples_to_keep
        self._fh = self.posts_path.open("a", encoding="utf-8")

    def close(self) -> None:
        self._fh.close()

    @property
    def total(self) -> int:
        return len(self.uris)

    def done(self) -> bool:
        return self.total >= self.target_count

    def store(self, row: dict[str, Any]) -> bool:
        if row["uri"] in self.uris:
            return False
        append_jsonl(self._fh, row)
        self.uris.add(row["uri"])
        self.stats.kept_session += 1
        if len(self.examples) < self.examples_to_keep:
            self.examples.append(row)
        if self.stats.kept_session % self.progress_every == 0:
            self.print_progress()
        return True

    def print_progress(self, label: str = "progress") -> None:
        st = self.stats
        print(f"[{label}] kept total={self.total} (this run {st.kept_session}) | "
              f"scanned={st.scanned:,} | {st.per_hour(st.kept_session):,.0f} kept/h, "
              f"{st.per_hour(st.scanned):,.0f} scanned/h", flush=True)


# --------------------------------------------------------------------------- API client

class SearchClient:
    """Minimal authenticated client for app.bsky.feed.searchPosts."""

    def __init__(self, s: SearchSettings, handle: str, app_password: str) -> None:
        self.s = s
        self.handle = handle
        self.app_password = app_password
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "research-pilot-collector/0.1"
        self.token: str | None = None

    def login(self) -> None:
        r = self.http.post(f"{self.s.host}/xrpc/com.atproto.server.createSession",
                           json={"identifier": self.handle, "password": self.app_password},
                           timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"login failed ({r.status_code}): {r.text[:200]}")
        self.token = r.json()["accessJwt"]

    def _get(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        delay = self.s.delay_s
        for attempt in range(self.s.max_retries + 1):
            headers = {"Authorization": f"Bearer {self.token}"}
            if self.s.accept_labelers:
                headers["atproto-accept-labelers"] = self.s.accept_labelers
            r = self.http.get(f"{self.s.host}/xrpc/{method}", params=params,
                              headers=headers, timeout=30)
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
        raise RuntimeError(f"{method}: giving up after {self.s.max_retries} retries")

    def search(self, query: str) -> Iterator[list[dict[str, Any]]]:
        """Yield result pages until the API stops returning a cursor.

        max_pages <= 0 means no page limit (paginate fully).
        """
        cursor: str | None = None
        pages = 0
        seen_cursors: set[str] = set()
        while self.s.max_pages <= 0 or pages < self.s.max_pages:
            params: dict[str, Any] = {"q": query, "limit": self.s.page_limit, "sort": self.s.sort}
            if cursor:
                params["cursor"] = cursor
            data = self._get("app.bsky.feed.searchPosts", params)
            pages += 1
            posts = data.get("posts", [])
            yield posts
            cursor = data.get("cursor")
            time.sleep(self.s.delay_s)
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


# --------------------------------------------------------------------------- run

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
        return (f"'{self.query}': pages={self.pages} found={self.found} "
                f"passed filter={self.passed} new kept={self.kept_new} ({state})")


def run_search(store: PostStore, s: Settings, deadline: float | None = None) -> list[QueryReport]:
    """Run every query in the queries file; stop at the target or at the deadline."""
    handle = os.environ.get("BSKY_HANDLE", "").strip()
    password = os.environ.get("BSKY_APP_PASSWORD", "").strip()
    if not handle or not password:
        raise SystemExit("BSKY_HANDLE / BSKY_APP_PASSWORD are not set in .env")
    if not APP_PASSWORD_RE.match(password):
        raise SystemExit("BSKY_APP_PASSWORD does not look like an App Password (xxxx-xxxx-xxxx-xxxx). "
                         "Create one in Bluesky settings; never use your main password.")
    matcher = KeywordMatcher(load_keywords(s.keywords_file))
    client = SearchClient(s.search, handle, password)
    client.login()
    print(f"[search] logged in to {s.search.host}", flush=True)
    reports: list[QueryReport] = []
    for query in load_queries(s.search.queries_file):
        rep = QueryReport(query)
        reports.append(rep)
        print(f"[search] running '{query}'", flush=True)
        for page in client.search(query):
            rep.pages += 1
            for view in page:
                rep.found += 1
                store.stats.scanned += 1
                uri = view.get("uri", "")
                author = view.get("author", {})
                row = build_post(did=author.get("did", ""),
                                 rkey=uri.rsplit("/", 1)[-1], record=view.get("record", {}),
                                 source="search", matcher=matcher,
                                 min_matches=s.min_matches,
                                 indexed_at=view.get("indexedAt"),
                                 labels=view.get("labels") or [],
                                 author_labels=author.get("labels") or [])
                if row:
                    rep.passed += 1
                    rep.kept_new += store.store(row)
            if store.done() or (deadline is not None and time.monotonic() >= deadline):
                print(f"[search] {rep.line()}", flush=True)
                return reports
        rep.complete = True
        print(f"[search] {rep.line()}", flush=True)
    return reports


def print_search_summary(reports: list[QueryReport]) -> None:
    print("\n=== search summary ===")
    for rep in reports:
        print("  " + rep.line())
    print(f"  queries run: {len(reports)} ({sum(r.complete for r in reports)} complete) | "
          f"found={sum(r.found for r in reports)} passed filter={sum(r.passed for r in reports)} "
          f"new kept={sum(r.kept_new for r in reports)}")


def print_examples(store: PostStore, n: int) -> None:
    print(f"\n=== {min(n, len(store.examples))} example kept posts (terminal only, not saved) ===")
    for row in store.examples[:n]:
        text = " ".join(row["text"].split())
        print(f"- langs={row['langs']} script={row['script']} "
              f"keywords={row['matched_keywords']}\n  {text[:280]}")


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Windows consoles
    load_dotenv(ROOT / ".env")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--settings", type=Path, default=DEFAULT_SETTINGS_PATH)
    ap.add_argument("--test-minutes", type=float, default=None,
                    help="test mode: run for N minutes, write to data/raw/test/, show examples")
    ap.add_argument("--examples", type=int, default=5, help="examples shown at the end of test mode")
    args = ap.parse_args(argv)
    s = load_settings(args.settings)
    test_mode = args.test_minutes is not None
    out_dir = s.search.output_dir / "test" if test_mode else s.search.output_dir
    store = PostStore(s, out_dir, examples_to_keep=args.examples if test_mode else 0)
    print(f"Output: {store.posts_path} (already stored: {store.total}) | "
          f"target={s.search.target_count} | min distinct keywords={s.min_matches} | "
          f"keywords={len(load_keywords(s.keywords_file))}", flush=True)
    deadline = time.monotonic() + args.test_minutes * 60 if test_mode else None
    reports: list[QueryReport] = []
    try:
        if not store.done():
            reports = run_search(store, s, deadline)
    except KeyboardInterrupt:
        print("\n[stopped by user]")
    finally:
        store.close()
        store.print_progress("final")
        if reports:
            print_search_summary(reports)
        if store.done():
            print(f"Target of {s.search.target_count} posts reached.")
        if test_mode:
            print_examples(store, args.examples)


if __name__ == "__main__":
    main()
