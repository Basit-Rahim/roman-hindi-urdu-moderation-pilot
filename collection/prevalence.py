"""Prevalence mode: how common are Romanized Hindi-Urdu posts on Bluesky, and how are they tagged?

Counts only. No post text, URIs, DIDs or handles are saved.

Two ways to cover a time window:

    live    (default) listen to Jetstream's live stream for prevalence.hours.
            Output: prevalence.output (data/prevalence/prevalence.json), buckets per UTC hour.
    replay  (--since/--until) read the same events from Jetstream's archive for a past
            window, then stop. Needs JETSTREAM_API_KEY in .env.
            Output: prevalence.replay_output (data/prevalence/prevalence_7d.json),
            buckets per UTC day, plus a coverage section.

Counts kept in totals and in every bucket:
    scanned               post create events seen
    matched               posts with >= min_matches distinct keywords
    matched_1plus         posts with >= 1 keyword (for comparison)
    scanned_langs         declared `langs` value of all scanned posts
    scanned_scripts       script of all scanned posts
    scanned_langs_script  langs x script for all scanned posts
    matched_langs         declared `langs` value of matched posts
    matched_scripts       script of matched posts
    matched_langs_script  langs x script for matched posts
    matched_keywords      keyword frequencies among matched posts

Both modes are resumable: run the same command again and it continues from the
saved cursor (live) or archive position (replay). Use --fresh to start over.

Usage:
    python -m collection.prevalence                                  # live, 24 h, or resume
    python -m collection.prevalence --hours 0.05                     # live 3-minute smoke test
    python -m collection.prevalence --since 2026-11-17 --until 2026-11-24   # replay 7 days
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import cbor2
from dotenv import load_dotenv

from collection.jetstream import CommitEvent, is_v2_url, parse_event, run_jetstream, with_cursor
from collection.jetstream_replay import (HEADER_SIZE, KIND_NAMES, ReplayClient, ReplayError,
                                         TransientError, decompress_frame, iter_block_events)
from common.bluesky import POST_COLLECTION
from common.config import DEFAULT_SETTINGS_PATH, ROOT, Settings, load_settings
from common.files import write_json
from common.filters import KeywordMatcher, detect_script, load_keywords
from common.time_utils import utc_now_iso

COUNTER_KEYS = ("scanned_langs", "scanned_scripts", "matched_langs", "matched_scripts",
                "matched_keywords")
TABLE_KEYS = ("scanned_langs_script", "matched_langs_script")
DAY_US = 86_400_000_000


# --------------------------------------------------------------------------- counting

def empty_counts() -> dict[str, Any]:
    return {"scanned": 0, "matched": 0, "matched_1plus": 0,
            **{k: {} for k in COUNTER_KEYS}, **{k: {} for k in TABLE_KEYS}}


def langs_key(langs: Any) -> str:
    if langs is None:
        return "(missing)"
    if not langs:
        return "(empty)"
    return ",".join(str(x) for x in langs)


def bump(counts: dict[str, Any], key: str, value: str) -> None:
    c = counts.setdefault(key, {})
    c[value] = c.get(value, 0) + 1


def bump_table(counts: dict[str, Any], key: str, row: str, col: str) -> None:
    r = counts.setdefault(key, {}).setdefault(row, {})
    r[col] = r.get(col, 0) + 1


def classify_post(record: dict[str, Any], matcher: KeywordMatcher) -> tuple[str, str, list[str]]:
    """(langs key, script, distinct keywords found) for one post record."""
    text = record.get("text") or ""
    if not isinstance(text, str):
        text = ""
    return langs_key(record.get("langs")), detect_script(text), matcher.find(text)


def count_post(counts: dict[str, Any], record: dict[str, Any], matcher: KeywordMatcher,
               min_matches: int) -> None:
    """Add one post to a counts dict. Only aggregates are kept."""
    add_post(counts, *classify_post(record, matcher), min_matches)


def add_post(counts: dict[str, Any], langs: str, script: str, found: list[str],
             min_matches: int) -> None:
    counts["scanned"] += 1
    bump(counts, "scanned_langs", langs)
    bump(counts, "scanned_scripts", script)
    bump_table(counts, "scanned_langs_script", langs, script)
    if found:
        counts["matched_1plus"] += 1
    if len(found) >= min_matches:
        counts["matched"] += 1
        bump(counts, "matched_langs", langs)
        bump(counts, "matched_scripts", script)
        bump_table(counts, "matched_langs_script", langs, script)
        for kw in found:
            bump(counts, "matched_keywords", kw)


def hour_key(time_us: int | None) -> str:
    ts = time_us / 1e6 if time_us else time.time()
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:00Z")


def day_key(time_us: int) -> str:
    return datetime.fromtimestamp(time_us // DAY_US * 86_400, timezone.utc).strftime("%Y-%m-%d")


def us_to_iso(time_us: int | None) -> str | None:
    if time_us is None:
        return None
    return (datetime.fromtimestamp(time_us / 1e6, timezone.utc)
            .isoformat(timespec="seconds").replace("+00:00", "Z"))


def parse_utc(value: str) -> int:
    """'2026-11-17' or '2026-11-17T06:00[:00][Z]' (always UTC) -> unix microseconds."""
    v = value.strip().rstrip("Z").replace(" ", "T")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return int(dt.replace(tzinfo=timezone.utc).timestamp()) * 1_000_000


# --------------------------------------------------------------------------- summaries

def lang_codes(key: str) -> set[str]:
    """Primary language subtags of a langs key: 'en-US,hi' -> {'en', 'hi'}."""
    if key in ("(missing)", "(empty)"):
        return set()
    return {part.strip().split("-")[0].lower() for part in key.split(",") if part.strip()}


def scripts_for_tag(table: dict[str, dict[str, int]], code: str) -> dict[str, int]:
    """Script counts of posts whose langs include `code` (alone or with other tags)."""
    out: Counter[str] = Counter()
    for key, scripts in table.items():
        if code in lang_codes(key):
            out.update(scripts)
    return dict(out.most_common())


def tag_groups(langs: dict[str, int]) -> dict[str, int]:
    """Matched-post langs grouped: English only / Hindi or Urdu / missing or empty / other."""
    out = {"en_only": 0, "hi_or_ur": 0, "missing_or_empty": 0, "other": 0}
    for key, n in langs.items():
        codes = lang_codes(key)
        if not codes:
            out["missing_or_empty"] += n
        elif codes & {"hi", "ur"}:
            out["hi_or_ur"] += n
        elif codes == {"en"}:
            out["en_only"] += n
        else:
            out["other"] += n
    return out


def summarize(counts: dict[str, Any]) -> dict[str, Any]:
    scanned = counts["scanned"]
    table = counts.get("scanned_langs_script", {})
    return {
        "scanned": scanned,
        "matched": counts["matched"],
        "matched_per_100k": round(counts["matched"] / scanned * 100_000, 3) if scanned else None,
        "matched_1plus": counts["matched_1plus"],
        "matched_langs_grouped": tag_groups(counts.get("matched_langs", {})),
        "tagged_hi_by_script": scripts_for_tag(table, "hi"),
        "tagged_ur_by_script": scripts_for_tag(table, "ur"),
        "all_scripts": dict(Counter(counts.get("scanned_scripts", {})).most_common()),
    }


def print_summary(counts: dict[str, Any], daily: dict[str, dict[str, Any]] | None = None) -> None:
    s = summarize(counts)
    print(f"scanned        {s['scanned']:,}")
    print(f"matched        {s['matched']:,}  ({s['matched_per_100k']} per 100,000)")
    print(f">=1 keyword    {s['matched_1plus']:,}")
    print("matched langs  ", Counter(counts.get("matched_langs", {})).most_common(12))
    print("matched grouped", s["matched_langs_grouped"])
    print("matched scripts", counts.get("matched_scripts", {}))
    print("tagged hi, by script:", s["tagged_hi_by_script"])
    print("tagged ur, by script:", s["tagged_ur_by_script"])
    if daily:
        print(f"{'day':<12}{'scanned':>12}{'matched':>9}{'per 100k':>10}")
        for day in sorted(daily):
            d = daily[day]
            rate = d["matched"] / d["scanned"] * 100_000 if d["scanned"] else 0
            print(f"{day:<12}{d['scanned']:>12,}{d['matched']:>9}{rate:>10.2f}")


# --------------------------------------------------------------------------- live mode

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


def run_live(s: Settings, args: argparse.Namespace) -> None:
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
            print_summary(p.state["totals"])


# --------------------------------------------------------------------------- replay mode

def resolve_window(segments: list[dict[str, Any]], since_us: int, until_us: int) -> tuple[int, int]:
    """Map a UTC window onto archive sequence bounds (afterSeq, beforeSeq].

    Uses each segment's witnessed-at range from listSegments. Events are still
    filtered by their own time while counting, so the bounds only need to cover
    the window, not match it exactly.
    """
    if not segments:
        raise ReplayError("the archive lists no segments")
    if since_us >= until_us:
        raise ReplayError("--since must be before --until")
    start, tip = segments[0]["minWitnessedAt"], segments[-1]["maxWitnessedAt"]
    if since_us < start:
        raise ReplayError(f"--since is before the archive start ({us_to_iso(start)})")
    if until_us > tip:
        raise ReplayError(f"--until is after the sealed archive tip ({us_to_iso(tip)}); "
                          "choose an earlier end")
    before = [s for s in segments if s["maxWitnessedAt"] < since_us]
    after_seq = before[-1]["maxSeq"] if before else 0
    later = [s for s in segments if s["minWitnessedAt"] >= until_us]
    before_seq = later[0]["minSeq"] - 1 if later else segments[-1]["maxSeq"]
    return after_seq, before_seq


class Gaps:
    """Tracks gaps longer than a threshold between consecutive in-window events."""

    def __init__(self, threshold_us: int, gaps: list[dict[str, Any]], last_us: int | None) -> None:
        self.threshold_us = threshold_us
        self.gaps = gaps
        self.last_us = last_us

    def see(self, t_us: int) -> None:
        if self.last_us is not None and t_us - self.last_us > self.threshold_us:
            self.add(self.last_us, t_us)
        if self.last_us is None or t_us > self.last_us:
            self.last_us = t_us

    def add(self, start_us: int, end_us: int) -> None:
        self.gaps.append({"start": us_to_iso(start_us), "end": us_to_iso(end_us),
                          "length_s": round((end_us - start_us) / 1e6, 1)})


# Position inside the plan. `seq` is the last archive sequence number whose block
# has been fully counted; it is the real resume point, because the archive may
# rewrite a segment and shift its byte offsets and block numbers. `offset`/`block`
# are shortcuts that are used only while the segment's checksum is unchanged.
NEW_POSITION = {"segment": 0, "checksum": None, "offset": None, "end": None, "block": None}


class StalePlan(Exception):
    """A planned segment was rewritten in the archive; plan the rest again."""


def plan_entries(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keep = ("name", "mode", "checksum", "minSeq", "maxSeq", "blocks")
    return [{k: seg[k] for k in keep if k in seg} for seg in plan]


class ReplayPrevalence:
    """Counts posts in a past UTC window from Jetstream's archive. Counts only."""

    def __init__(self, s: Settings, out: Path, since_us: int, until_us: int, fresh: bool,
                 client: ReplayClient) -> None:
        self.s = s
        self.out = out
        self.client = client
        self.since_us, self.until_us = since_us, until_us
        self.min_matches = s.min_matches
        keywords = load_keywords(s.keywords_file)
        self.matcher = KeywordMatcher(keywords)
        state = None if fresh else self._load()
        window = {"since": us_to_iso(since_us), "until": us_to_iso(until_us)}
        if state is not None:
            if state.get("mode") != "replay" or state.get("window") != window:
                raise SystemExit(f"{out} already holds a different run. Use --fresh to replace it "
                                 "or --output for a new file.")
            self.resumed = not state.get("finished")
            if self.resumed:
                state["coverage"]["resumes"] += 1
                state["coverage"].setdefault("replans", 0)
                print(f"[replay] resuming from {state['coverage']['last_event']}", flush=True)
            self.state = state
        else:
            self.resumed = False
            self.state = {
                "mode": "replay",
                "window": window,
                "started_at": utc_now_iso(),
                "finished": False,
                "host": client.host,
                "min_matches": s.min_matches,
                "keywords": keywords,
                "coverage": {
                    "first_event_time_us": None, "last_event_time_us": None,
                    "first_event": None, "last_event": None,
                    "events_in_window": 0, "events_by_kind": {},
                    "post_creates": 0, "post_updates": 0, "post_deletes": 0,
                    "resync_post_records_skipped": 0, "record_decode_errors": 0,
                    "events_outside_window": 0,
                    "gap_threshold_s": s.prevalence.gap_threshold_s, "gaps": [],
                    "reconnects": 0, "rate_limited": 0, "resumes": 0, "replans": 0,
                    "bytes_downloaded": 0, "segments_planned": 0, "segments_done": 0,
                },
                "summary": None,
                "totals": empty_counts(),
                "daily": {},
                "replay": {"after_seq": None, "before_seq": None, "plan": [], "plan_stats": {},
                           "position": dict(NEW_POSITION, seq=None)},
            }
        cov = self.state["coverage"]
        self.gaps = Gaps(int(cov["gap_threshold_s"] * 1e6), cov["gaps"], cov["last_event_time_us"])
        self._day_cache: dict[int, str] = {}
        self._bytes_at_save = 0
        self._last_save = self._last_beat = self._start = time.monotonic()
        self._events_session = 0

    # -- persistence
    def _load(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.out.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def save(self) -> None:
        cov = self.state["coverage"]
        cov["bytes_downloaded"] += self.client.bytes_downloaded - self._bytes_at_save
        self._bytes_at_save = self.client.bytes_downloaded
        cov["first_event"] = us_to_iso(cov["first_event_time_us"])
        cov["last_event"] = us_to_iso(cov["last_event_time_us"])
        self.state["saved_at"] = utc_now_iso()
        self.out.parent.mkdir(parents=True, exist_ok=True)
        write_json(self.out, self.state)
        self._last_save = time.monotonic()

    def _tick(self) -> None:
        now = time.monotonic()
        if now - self._last_save >= self.s.prevalence.save_interval_s:
            self.save()
        if now - self._last_beat >= self.s.prevalence.heartbeat_s:
            self.print_status()

    # -- planning
    def prepare(self) -> None:
        rp = self.state["replay"]
        if rp["plan"]:
            return
        print("[replay] listing archive segments", flush=True)
        segments = self.client.list_segments()
        rp["after_seq"], rp["before_seq"] = resolve_window(segments, self.since_us, self.until_us)
        rp["position"]["seq"] = rp["after_seq"]
        plan, stats = self.client.plan([POST_COLLECTION], rp["after_seq"], rp["before_seq"])
        rp["plan"] = plan_entries(plan)
        rp["plan_stats"] = stats
        self.state["coverage"]["segments_planned"] = len(plan)
        self.save()
        print(f"[replay] plan: {len(plan)} segments "
              f"({sum(1 for p in plan if p['mode'] == 'segment')} whole), "
              f"seq ({rp['after_seq']}, {rp['before_seq']}]", flush=True)

    def replan(self, reason: str) -> None:
        """Plan the rest of the window again, from the last fully counted sequence number."""
        rp = self.state["replay"]
        pos = rp["position"]
        if pos.get("seq") is None:
            pos["seq"] = self._legacy_seq()
        plan, stats = self.client.plan([POST_COLLECTION], pos["seq"], rp["before_seq"])
        rp["plan"] = plan_entries(plan)
        rp["plan_stats"] = stats
        pos.update(NEW_POSITION)
        self.state["coverage"]["replans"] += 1
        self.save()
        print(f"[replay] {reason}: planned again from seq {pos['seq']}: "
              f"{len(plan)} segments left", flush=True)

    def _legacy_seq(self) -> int:
        """Resume point for files saved before `seq` was recorded (one-off migration).

        Finds the block holding the last counted event (by its time) and takes the
        highest sequence number at or before that time.
        """
        rp = self.state["replay"]
        last_t = self.state["coverage"]["last_event_time_us"]
        if last_t is None:
            return rp["after_seq"]
        name = rp["plan"][rp["position"]["segment"]]["name"]
        header = self.client.segment_header(name)
        index = self.client.block_index(name, header)
        holding = [b for b in index if b.min_time_us <= last_t]
        if not holding:
            return rp["after_seq"]
        block = holding[-1]
        _, frame = next(self.client.stream_blocks(name, block.offset,
                                                  block.offset + 8 + block.compressed_size))
        seqs = [e.seq for e in iter_block_events(decompress_frame(frame)) if e.time_us <= last_t]
        return max(seqs) if seqs else block.min_seq - 1

    # -- counting
    def _day(self, t_us: int) -> str:
        d = t_us // DAY_US
        key = self._day_cache.get(d)
        if key is None:
            key = self._day_cache[d] = day_key(t_us)
        return key

    def process_frame(self, frame: bytes) -> None:
        """Count one block. Events at or below position.seq were counted before; skip them."""
        cov = self.state["coverage"]
        pos = self.state["replay"]["position"]
        done_seq = pos["seq"]
        kinds = cov["events_by_kind"]
        since, until = self.since_us, self.until_us
        events = list(iter_block_events(decompress_frame(frame)))
        for ev in events:
            if ev.seq <= done_seq:
                continue
            t = ev.time_us
            if t < since or t >= until:
                cov["events_outside_window"] += 1
                continue
            cov["events_in_window"] += 1
            self._events_session += 1
            kname = KIND_NAMES.get(ev.kind, str(ev.kind))
            kinds[kname] = kinds.get(kname, 0) + 1
            self.gaps.see(t)
            if cov["first_event_time_us"] is None or t < cov["first_event_time_us"]:
                cov["first_event_time_us"] = t
            if cov["last_event_time_us"] is None or t > cov["last_event_time_us"]:
                cov["last_event_time_us"] = t
            if ev.collection != POST_COLLECTION:
                continue
            if ev.kind == 1:
                try:
                    record = cbor2.loads(ev.payload)
                except Exception:
                    cov["record_decode_errors"] += 1
                    continue
                if not isinstance(record, dict):
                    cov["record_decode_errors"] += 1
                    continue
                cov["post_creates"] += 1
                daily = self.state["daily"].setdefault(self._day(t), empty_counts())
                post = classify_post(record, self.matcher)
                add_post(self.state["totals"], *post, self.min_matches)
                add_post(daily, *post, self.min_matches)
            elif ev.kind == 2:
                cov["post_updates"] += 1
            elif ev.kind == 3:
                cov["post_deletes"] += 1
            elif ev.kind == 7:
                cov["resync_post_records_skipped"] += 1
        if events:
            pos["seq"] = max(done_seq, events[-1].seq)

    # -- archive walk
    def _check_segment(self, seg: dict[str, Any], pos: dict[str, Any]):
        """Fetch the header; raise StalePlan if the segment changed since it was planned.

        Also drops the byte/block shortcut if it was saved against another version.
        """
        header = self.client.segment_header(seg["name"])
        if seg.get("checksum") and header.checksum != seg["checksum"]:
            raise StalePlan(f"segment {seg['name']} was rewritten in the archive")
        if pos["checksum"] != header.checksum:
            pos.update(checksum=header.checksum, offset=None, end=None, block=None)
        return header

    def _segment_bounds(self, name: str, header) -> tuple[int, int]:
        """File byte range [start, end) of the blocks that may hold uncounted in-window events."""
        start, end = HEADER_SIZE, header.footer_offset
        done_seq = self.state["replay"]["position"]["seq"]
        if (header.min_time_us < self.since_us or header.max_time_us >= self.until_us
                or header.min_seq <= done_seq):
            index = self.client.block_index(name, header)
            todo = [b for b in index if b.max_time_us >= self.since_us
                    and b.min_time_us < self.until_us and b.max_seq > done_seq]
            if not todo:
                return end, end
            start = todo[0].offset
            end = todo[-1].offset + 8 + todo[-1].compressed_size
        return start, end

    def _walk_segment(self, seg: dict[str, Any], pos: dict[str, Any]) -> None:
        header = self._check_segment(seg, pos)
        if pos["offset"] is None:
            pos["offset"], pos["end"] = self._segment_bounds(seg["name"], header)
        for next_offset, frame in self.client.stream_blocks(seg["name"], pos["offset"], pos["end"]):
            self.process_frame(frame)
            pos["offset"] = next_offset
            self._tick()

    def _walk_blocks(self, seg: dict[str, Any], pos: dict[str, Any]) -> None:
        self._check_segment(seg, pos)
        indices = [i for r in seg["blocks"] for i in range(r["first"], r["last"] + 1)]
        if pos["block"] is None:
            pos["block"] = 0
        while pos["block"] < len(indices):
            self.process_frame(self.client.get_block(seg["name"], indices[pos["block"]]))
            pos["block"] += 1
            self._tick()

    def run(self) -> None:
        self.prepare()
        rp = self.state["replay"]
        pos = rp["position"]
        p = self.s.prevalence
        failures = 0
        if self.resumed:
            self.replan("resumed run")
        while pos["segment"] < len(rp["plan"]):
            seg = rp["plan"][pos["segment"]]
            try:
                if seg["mode"] == "blocks":
                    self._walk_blocks(seg, pos)
                else:
                    self._walk_segment(seg, pos)
            except StalePlan as exc:
                self.replan(str(exc))
                continue
            except TransientError as exc:
                failures += 1
                cov = self.state["coverage"]
                cov["reconnects"] += 1
                if exc.retry_after_s is not None:
                    cov["rate_limited"] += 1
                    delay = exc.retry_after_s
                else:
                    delay = min(p.backoff_initial_s * 2 ** (failures - 1), p.backoff_max_s)
                    delay *= random.uniform(0.8, 1.2)
                print(f"[replay] {exc}; retrying in {delay:.1f}s", flush=True)
                self.save()
                time.sleep(delay)
                continue
            failures = 0
            pos.update(NEW_POSITION, segment=pos["segment"] + 1)
            self.state["coverage"]["segments_done"] += 1
        self._finish()

    def _finish(self) -> None:
        cov = self.state["coverage"]
        threshold = self.gaps.threshold_us
        first, last = cov["first_event_time_us"], cov["last_event_time_us"]
        if first is None:
            self.gaps.add(self.since_us, self.until_us)
        else:
            if first - self.since_us > threshold:
                self.gaps.gaps.insert(0, {"start": us_to_iso(self.since_us), "end": us_to_iso(first),
                                          "length_s": round((first - self.since_us) / 1e6, 1)})
            if self.until_us - last > threshold:
                self.gaps.add(last, self.until_us)
        self.state["finished"] = True
        self.state["finished_at"] = utc_now_iso()
        self.state["summary"] = summarize(self.state["totals"])
        self.save()

    def print_status(self, label: str = "status") -> None:
        t = self.state["totals"]
        cov = self.state["coverage"]
        pos = self.state["replay"]["position"]
        reached = cov["last_event_time_us"] or self.since_us
        done = (reached - self.since_us) / (self.until_us - self.since_us)
        elapsed = max(time.monotonic() - self._start, 1e-9)
        mb = self.client.bytes_downloaded / 1e6
        planned = len(self.state["replay"]["plan"])
        print(f"[{label}] reached {us_to_iso(reached)} ({done:.1%}) | segment "
              f"{min(pos['segment'] + 1, planned)}/{planned} | scanned={t['scanned']:,} "
              f"matched={t['matched']} >=1 keyword={t['matched_1plus']:,} | "
              f"{mb / elapsed:.1f} MB/s, {self._events_session / elapsed:,.0f} events/s",
              flush=True)
        self._last_beat = time.monotonic()


def run_replay(s: Settings, args: argparse.Namespace) -> None:
    load_dotenv(ROOT / ".env")
    since_us = parse_utc(args.since)
    until_us = parse_utc(args.until) if args.until else \
        int(time.time()) // 86_400 * 86_400 * 1_000_000          # start of today, UTC
    out = (args.output or s.prevalence.replay_output).resolve()
    if out == s.prevalence.output.resolve():
        raise SystemExit(f"refusing to write replay output over the live-run file {out}")
    client = ReplayClient(s.prevalence.replay_host, os.environ.get("JETSTREAM_API_KEY", "").strip())
    p = ReplayPrevalence(s, out, since_us, until_us, args.fresh, client)
    print(f"[replay] window {p.state['window']['since']} -> {p.state['window']['until']} "
          f"host={client.host} output={out}", flush=True)
    if p.state["finished"]:
        print("[replay] this window is already finished; use --fresh to redo it")
    else:
        try:
            p.run()
        except KeyboardInterrupt:
            p.save()
            print("\n[stopped by user; run the same command again to resume]")
        except ReplayError as exc:
            p.save()
            raise SystemExit(f"[replay] stopped: {exc}")
        p.print_status("final" if p.state["finished"] else "saved")
    if p.state["finished"]:
        cov = p.state["coverage"]
        print(f"coverage: {cov['first_event']} -> {cov['last_event']}, "
              f"{cov['events_in_window']:,} events, {len(cov['gaps'])} gaps "
              f"> {cov['gap_threshold_s']:.0f}s, {cov['reconnects']} reconnects, "
              f"{cov['bytes_downloaded'] / 1e9:.2f} GB")
        print_summary(p.state["totals"], p.state["daily"])


# --------------------------------------------------------------------------- CLI

def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--settings", type=Path, default=DEFAULT_SETTINGS_PATH)
    ap.add_argument("--hours", type=float, default=None, help="live mode: run length")
    ap.add_argument("--since", help="replay mode: window start, UTC (e.g. 2026-11-17)")
    ap.add_argument("--until", help="replay mode: window end, UTC, exclusive "
                                    "(default: start of today)")
    ap.add_argument("--output", type=Path, default=None)
    ap.add_argument("--fresh", action="store_true", help="ignore any saved run and start over")
    args = ap.parse_args(argv)
    s = load_settings(args.settings)
    if args.until and not args.since:
        ap.error("--until needs --since")
    if args.since:
        run_replay(s, args)
    else:
        run_live(s, args)


if __name__ == "__main__":
    main()
