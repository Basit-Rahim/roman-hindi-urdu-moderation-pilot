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
    python prevalence.py                # 24 h (prevalence.hours in settings.yaml)
    python prevalence.py --hours 0.05   # 3-minute smoke test
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from collect import (ROOT, POST_COLLECTION, CommitEvent, Settings, is_v2_url, load_settings,
                     parse_event, run_jetstream, utc_now_iso, with_cursor)
from filters import KeywordMatcher, detect_script, load_keywords

COUNTER_KEYS = ("scanned_langs", "matched_langs", "matched_scripts", "matched_keywords")


def empty_counts() -> dict[str, Any]:
    return {"scanned": 0, "matched": 0, "matched_1plus": 0, **{k: {} for k in COUNTER_KEYS}}


def langs_key(langs: Any) -> str:
    if langs is None:
        return "(missing)"
    if not langs:
        return "(empty)"
    return ",".join(str(x) for x in langs)


def bump(counts: dict[str, Any], key: str, value: str, n: int = 1) -> None:
    counts[key][value] = counts[key].get(value, 0) + n


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
    """Jetstream handler that keeps counts only. Driven by collect.run_jetstream."""

    def __init__(self, s: Settings, out: Path, hours: float, save_interval_s: float,
                 fresh: bool) -> None:
        self.s = s
        self.out = out
        self.save_interval_s = save_interval_s
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
                "jetstream_url": s.jetstream_url,
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
        tmp = self.out.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=1, sort_keys=False), encoding="utf-8")
        os.replace(tmp, self.out)
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
        if not isinstance(ev, CommitEvent) or self._is_replay(ev):
            return
        if ev.collection == POST_COLLECTION and ev.operation == "create" and ev.record:
            hourly = self.state["hourly"].setdefault(hour_key(ev.time_us), empty_counts())
            for counts in (self.state["totals"], hourly):
                count_post(counts, ev.record, self.matcher, self.s.min_matches)
            self._session_scanned += 1
        if ev.time_us is not None:
            self.state["first_event_time_us"] = self.state["first_event_time_us"] or ev.time_us
            self.state["last_event_time_us"] = ev.time_us
        self.state["cursor"] = {"seq": ev.seq, "time_us": ev.time_us}
        now = time.monotonic()
        if now - self._last_save >= self.save_interval_s:
            self.save_checkpoint()
        if now - self._last_beat >= self.s.heartbeat_s:
            self.print_status()

    def print_status(self, label: str = "status") -> None:
        t = self.state["totals"]
        rate = self._session_scanned / max(time.monotonic() - self._session_start, 1e-9) * 3600
        share = 100 * t["matched"] / max(t["scanned"], 1)
        print(f"[{label}] scanned={t['scanned']:,} matched={t['matched']} ({share:.4f}%) "
              f">=1 keyword={t['matched_1plus']:,} | {rate:,.0f} scanned/h | "
              f"{max(self.remaining_s(), 0) / 3600:.2f} h left", flush=True)
        self._last_beat = time.monotonic()


async def _run(p: Prevalence) -> None:
    try:
        await asyncio.wait_for(run_jetstream(p), timeout=max(p.remaining_s(), 0))
    except asyncio.TimeoutError:
        pass


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--settings", type=Path, default=ROOT / "config/settings.yaml")
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--output", type=Path, default=None)
    ap.add_argument("--fresh", action="store_true", help="ignore any saved run and start over")
    args = ap.parse_args(argv)
    s = load_settings(args.settings)
    raw = yaml.safe_load(args.settings.read_text(encoding="utf-8")) or {}
    cfg = raw.get("prevalence") or {}
    hours = args.hours if args.hours is not None else float(cfg.get("hours", 24))
    out = args.output or ROOT / cfg.get("output", "data/prevalence/prevalence.json")
    p = Prevalence(s, out, hours, float(cfg.get("save_interval_s", 60)), args.fresh)
    print(f"[prevalence] output={out} ends_at={p.state['ends_at']} "
          f"min_matches={s.min_matches} url={s.jetstream_url}", flush=True)
    try:
        asyncio.run(_run(p))
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
