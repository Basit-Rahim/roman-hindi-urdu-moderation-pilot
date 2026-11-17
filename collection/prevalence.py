"""Jetstream prevalence mode: how common are Romanized Hindi-Urdu posts on Bluesky?

Counts only. No post text, URIs, DIDs or handles are saved. Output (one JSON
file, rewritten every save_interval_s):

    totals / hourly[<UTC hour>]:
        scanned            post create events seen
        matched            posts with >= min_matches distinct keywords
        matched_1plus      posts with >= 1 keyword (for comparison)
        scanned_langs      declared `langs` value of all scanned posts
        matched_langs      declared `langs` value of matched posts
        matched_scripts    script of matched posts
        matched_keywords   keyword frequencies among matched posts

The run is resumable: if the process stops, start it again and it continues
from the saved cursor until the original end time. Use --fresh to start over.

Usage:
    python -m collection.prevalence                # 24 h (prevalence.hours) or resume
    python -m collection.prevalence --hours 0.05   # 3-minute smoke test
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from collection.jetstream import CommitEvent, is_v2_url, parse_event, run_jetstream, with_cursor
from common.bluesky import POST_COLLECTION
from common.config import DEFAULT_SETTINGS_PATH, Settings, load_settings
from common.files import write_json
from common.filters import KeywordMatcher, detect_script, load_keywords
from common.time_utils import utc_now_iso

COUNTER_KEYS = ("scanned_langs", "matched_langs", "matched_scripts", "matched_keywords")


def empty_counts() -> dict[str, Any]:
    return {"scanned": 0, "matched": 0, "matched_1plus": 0, **{k: {} for k in COUNTER_KEYS}}


def langs_key(langs: Any) -> str:
    if langs is None:
        return "(missing)"
    if not langs:
        return "(empty)"
    return ",".join(str(x) for x in langs)


def bump(counts: dict[str, Any], key: str, value: str) -> None:
    counts[key][value] = counts[key].get(value, 0) + 1


def hour_key(time_us: int | None) -> str:
    ts = time_us / 1e6 if time_us else time.time()
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:00Z")


def count_post(counts: dict[str, Any], record: dict[str, Any], matcher: KeywordMatcher,
               min_matches: int) -> None:
    """Add one post to a counts dict. Only aggregates are kept."""
    text = record.get("text") or ""
    langs = langs_key(record.get("langs"))
    found = matcher.find(text)
    counts["scanned"] += 1
    bump(counts, "scanned_langs", langs)
    if found:
        counts["matched_1plus"] += 1
    if len(found) >= min_matches:
        counts["matched"] += 1
        bump(counts, "matched_langs", langs)
        bump(counts, "matched_scripts", detect_script(text))
        for kw in found:
            bump(counts, "matched_keywords", kw)


class Prevalence:
    """Jetstream handler that keeps counts only. Driven by jetstream.run_jetstream."""

    def __init__(self, s: Settings, out: Path, hours: float, fresh: bool) -> None:
        self.min_matches = s.min_matches
        self.save_interval_s = s.prevalence.save_interval_s
        self.heartbeat_s = s.prevalence.heartbeat_s
        self.out = out
        keywords = load_keywords(s.keywords_file)
        self.matcher = KeywordMatcher(keywords)
        state = None if fresh else self._load()
        if state and not state.get("finished"):
            print(f"[prevalence] resuming run started {state['started_at']}", flush=True)
            state["resumes"] = state.get("resumes", 0) + 1
            self.state = state
        else:
            now = time.time()
            self.state = {
                "started_at": utc_now_iso(),
                "ends_at_epoch": now + hours * 3600,
                "ends_at": datetime.fromtimestamp(now + hours * 3600, timezone.utc)
                .isoformat(timespec="seconds").replace("+00:00", "Z"),
                "finished": False,
                "jetstream_url": s.prevalence.jetstream_url,
                "min_matches": s.min_matches,
                "keywords": keywords,
                "resumes": 0,
                "connections": 0,
                "first_event_time_us": None,
                "last_event_time_us": None,
                "cursor": {},
                "totals": empty_counts(),
                "hourly": {},
            }
        out.parent.mkdir(parents=True, exist_ok=True)
        self._last_save = time.monotonic()
        self._last_beat = time.monotonic()
        self._session_start = time.monotonic()
        self._session_scanned = 0

    # -- persistence
    def _load(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.out.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def save_checkpoint(self) -> None:
        self.state["saved_at"] = utc_now_iso()
        write_json(self.out, self.state)
        self._last_save = time.monotonic()

    def finish(self) -> None:
        if self.done():
            self.state["finished"] = True
        self.save_checkpoint()

    # -- StreamHandler interface
    def remaining_s(self) -> float:
        return self.state["ends_at_epoch"] - time.time()

    def done(self) -> bool:
        return self.remaining_s() <= 0

    def resume_url(self, base_url: str) -> str:
        self.state["connections"] += 1
        cur = self.state["cursor"]
        if is_v2_url(base_url) and cur.get("seq") is not None:
            return with_cursor(base_url, int(cur["seq"]))
        if cur.get("time_us") is not None:
            return with_cursor(base_url, int(cur["time_us"]))
        return base_url

    def _is_replay(self, ev: CommitEvent) -> bool:
        cur = self.state["cursor"]
        if ev.seq is not None and cur.get("seq") is not None:
            return ev.seq <= cur["seq"]
        if ev.time_us is not None and cur.get("time_us") is not None:
            return ev.time_us <= cur["time_us"]
        return False

    def handle_message(self, raw: str | bytes) -> None:
        try:
            ev = parse_event(json.loads(raw))
        except json.JSONDecodeError:
            return
        if ev is None or self._is_replay(ev):
            return
        if ev.collection == POST_COLLECTION and ev.operation == "create" and ev.record:
            hourly = self.state["hourly"].setdefault(hour_key(ev.time_us), empty_counts())
            for counts in (self.state["totals"], hourly):
                count_post(counts, ev.record, self.matcher, self.min_matches)
            self._session_scanned += 1
        if ev.time_us is not None:
            self.state["first_event_time_us"] = self.state["first_event_time_us"] or ev.time_us
            self.state["last_event_time_us"] = ev.time_us
        self.state["cursor"] = {"seq": ev.seq, "time_us": ev.time_us}
        now = time.monotonic()
        if now - self._last_save >= self.save_interval_s:
            self.save_checkpoint()
        if now - self._last_beat >= self.heartbeat_s:
            self.print_status()

    def print_status(self, label: str = "status") -> None:
        t = self.state["totals"]
        rate = self._session_scanned / max(time.monotonic() - self._session_start, 1e-9) * 3600
        share = 100 * t["matched"] / max(t["scanned"], 1)
        print(f"[{label}] scanned={t['scanned']:,} matched={t['matched']} ({share:.4f}%) "
              f">=1 keyword={t['matched_1plus']:,} | {rate:,.0f} scanned/h | "
              f"{max(self.remaining_s(), 0) / 3600:.2f} h left", flush=True)
        self._last_beat = time.monotonic()


async def _run(p: Prevalence, s: Settings) -> None:
    try:
        await asyncio.wait_for(
            run_jetstream(p, s.prevalence.jetstream_url, s.prevalence.backoff_initial_s,
                          s.prevalence.backoff_max_s),
            timeout=max(p.remaining_s(), 0))
    except asyncio.TimeoutError:
        pass


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--settings", type=Path, default=DEFAULT_SETTINGS_PATH)
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--output", type=Path, default=None)
    ap.add_argument("--fresh", action="store_true", help="ignore any saved run and start over")
    args = ap.parse_args(argv)
    s = load_settings(args.settings)
    hours = args.hours if args.hours is not None else s.prevalence.hours
    out = args.output or s.prevalence.output
    p = Prevalence(s, out, hours, args.fresh)
    print(f"[prevalence] output={out} ends_at={p.state['ends_at']} "
          f"min_matches={s.min_matches} url={s.prevalence.jetstream_url}", flush=True)
    try:
        asyncio.run(_run(p, s))
    except KeyboardInterrupt:
        print("\n[stopped by user; run again to resume]")
    finally:
        p.finish()
        p.print_status("final" if p.state["finished"] else "saved")
        if p.state["finished"]:
            t = p.state["totals"]
            print("matched langs:", Counter(t["matched_langs"]).most_common(10))
            print("matched scripts:", t["matched_scripts"])


if __name__ == "__main__":
    main()
