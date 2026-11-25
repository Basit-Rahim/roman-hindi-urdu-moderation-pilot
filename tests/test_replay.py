"""Replay prevalence: block decoding, script classification, per-day buckets, coverage, resume.

The archive is faked in memory with real .jss framing (header, zstd blocks, block
index), so these tests exercise the same decoder and walk as a real run.
"""
from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path
from typing import Any, Iterator

import cbor2
import pytest
import zstandard

from collection.jetstream_replay import (HEADER_FMT, HEADER_SIZE, INDEX_ENTRY_FMT, ReplayError,
                                         TransientError, decompress_frame, iter_block_events,
                                         parse_block_index, parse_header)
from collection.prevalence import (ReplayPrevalence, day_key, lang_codes, parse_utc,
                                   resolve_window, scripts_for_tag, summarize, tag_groups)
from common.config import Settings
from common.filters import _letter_script, detect_script

POST = "app.bsky.feed.post"
LIKE = "app.bsky.feed.like"
SINCE = parse_utc("2026-11-17")
UNTIL = parse_utc("2026-11-19")
DAY = 86_400_000_000
MIN = 60_000_000


# --------------------------------------------------------------------------- script classifier

@pytest.mark.parametrize("text, script", [
    ("Yaar kya hai", "latin"),
    ("क्या हाल है", "devanagari"),
    ("کیا حال ہے", "arabic"),
    ("kya हाल hai", "mixed"),
    ("کیا hai", "mixed"),
    ("こんにちは 😂 123", "other"),
    ("", "other"),
    ("Ḍhābā façade", "latin"),
])
def test_detect_script(text: str, script: str) -> None:
    assert detect_script(text) == script


def test_fast_classifier_matches_per_character_rule() -> None:
    def slow(text: str) -> str:
        present = {s for s in map(_letter_script, text) if s}
        return "mixed" if len(present) > 1 else (present.pop() if present else "other")
    samples = ["abc", "क", "ا", "ے", "०", "٠", "ﷺ", "ḍ",
               "ß", "ɐ", "ꣲ", "\U0001e900", "aकا", "̀"]
    for text in samples:
        assert detect_script(text) == slow(text), text


# --------------------------------------------------------------------------- bucketing, tags

def test_day_key_splits_at_utc_midnight() -> None:
    assert day_key(SINCE + DAY - 1) == "2026-11-17"
    assert day_key(SINCE + DAY) == "2026-11-18"


def test_parse_utc_accepts_dates_and_times() -> None:
    assert parse_utc("2026-11-17T00:00:00Z") == parse_utc("2026-11-17") == SINCE
    assert parse_utc("2026-11-17T01:00") == SINCE + 60 * MIN


def test_lang_tag_helpers() -> None:
    assert lang_codes("en-US,hi") == {"en", "hi"} and lang_codes("(missing)") == set()
    table = {"hi": {"devanagari": 5, "latin": 1}, "en,hi": {"latin": 2}, "ur": {"arabic": 3},
             "en": {"latin": 9}}
    assert scripts_for_tag(table, "hi") == {"devanagari": 5, "latin": 3}
    assert scripts_for_tag(table, "ur") == {"arabic": 3}
    assert tag_groups({"en": 3, "en-US": 1, "hi,en": 2, "(missing)": 4, "tr": 1}) == \
        {"en_only": 4, "hi_or_ur": 2, "missing_or_empty": 4, "other": 1}


def test_resolve_window() -> None:
    segs = [{"minSeq": 1, "maxSeq": 100, "minWitnessedAt": SINCE - DAY, "maxWitnessedAt": SINCE - 1},
            {"minSeq": 101, "maxSeq": 200, "minWitnessedAt": SINCE, "maxWitnessedAt": UNTIL - 1},
            {"minSeq": 201, "maxSeq": 300, "minWitnessedAt": UNTIL, "maxWitnessedAt": UNTIL + DAY}]
    assert resolve_window(segs, SINCE, UNTIL) == (100, 200)
    with pytest.raises(ReplayError):
        resolve_window(segs, SINCE - 2 * DAY, UNTIL)            # before archive start
    with pytest.raises(ReplayError):
        resolve_window(segs, SINCE, UNTIL + 2 * DAY)            # after sealed tip


# --------------------------------------------------------------------------- fake archive

def encode_block(events: list[tuple[int, int, int, str, bytes]]) -> bytes:
    """events: (seq, time_us, kind, collection, payload). Returns the zstd frame."""
    n = len(events)
    seqs, times, kinds, colls, payloads = zip(*events)
    dids = [f"did:plc:user{i % 3}".encode() for i in range(n)]
    rkeys = [f"rk{s}".encode() for s in seqs]
    revs = [b"3abcdefghijkl"] * n
    colls_b = [c.encode() for c in colls]
    raw = b"".join([
        struct.pack("<I", n), struct.pack(f"<{n}Q", *seqs), struct.pack(f"<{n}q", *times),
        struct.pack(f"<{n}q", *[0] * n), bytes(kinds), bytes(len(c) for c in colls_b),
        struct.pack(f"<{n}H", *map(len, dids)), bytes(map(len, rkeys)), bytes(map(len, revs)),
        struct.pack(f"<{n}I", *map(len, payloads)),
        *colls_b, *dids, *rkeys, *revs, *payloads])
    return zstandard.ZstdCompressor().compress(raw)


def encode_segment(blocks: list[list[tuple[int, int, int, str, bytes]]]) -> bytes:
    body, index, pos = b"", b"", HEADER_SIZE
    for events in blocks:
        frame = encode_block(events)
        index += struct.pack(INDEX_ENTRY_FMT, pos, len(frame), 0, len(events), events[0][0],
                             events[-1][0], events[0][1], events[-1][1])
        body += struct.pack("<Q", len(frame)) + frame
        pos += 8 + len(frame)
    all_events = [e for b in blocks for e in b]
    checksum = zlib.crc32(body)                       # changes whenever the content does
    header = struct.pack(HEADER_FMT, b"jss0", checksum, 1, len(blocks), len(all_events), 3,
                         all_events[0][0], all_events[-1][0], all_events[0][1], all_events[-1][1],
                         pos, pos, pos, pos, pos)
    return header.ljust(HEADER_SIZE, b"\0") + body + index


def post(text: str, langs: list[str] | None = None) -> bytes:
    rec: dict[str, Any] = {"$type": POST, "text": text, "createdAt": "2026-11-17T00:00:00Z"}
    if langs is not None:
        rec["langs"] = langs
    return cbor2.dumps(rec)


class FakeClient:
    """In-memory stand-in for ReplayClient. Can fail once at the Nth frame served."""

    host = "fake://archive"

    def __init__(self, files: dict[str, bytes], fail_at: int | None = None,
                 exc: BaseException | None = None,
                 rewrite_on_fail: dict[str, bytes] | None = None) -> None:
        self.files = dict(files)
        self.bytes_downloaded = 0
        self.served = 0
        self.fail_at, self.exc = fail_at, exc
        self.rewrite_on_fail = rewrite_on_fail      # archive rewrites segments as the error hits

    def _serve(self) -> None:
        if self.fail_at is not None and self.served == self.fail_at:
            self.fail_at = None
            self.files.update(self.rewrite_on_fail or {})
            raise self.exc
        self.served += 1

    def list_segments(self) -> list[dict[str, Any]]:
        out = []
        for name, data in self.files.items():
            h = parse_header(data)
            out.append({"name": name, "minSeq": h.min_seq, "maxSeq": h.max_seq,
                        "checksum": h.checksum,
                        "minWitnessedAt": h.min_time_us, "maxWitnessedAt": h.max_time_us})
        return out

    def plan(self, collections: list[str], after_seq: int, before_seq: int):
        segs = [s for s in self.list_segments() if s["maxSeq"] > after_seq and s["minSeq"] <= before_seq]
        plan = [{"name": s["name"], "mode": "segment", "checksum": s["checksum"],
                 "minSeq": s["minSeq"], "maxSeq": s["maxSeq"]} for s in segs]
        plan[-1]["mode"] = "blocks"                     # exercise both download modes
        plan[-1]["blocks"] = [{"first": 0, "last": parse_header(self.files[plan[-1]["name"]]).block_count - 1}]
        return plan, {"segmentsMatched": len(plan)}

    def segment_header(self, name: str):
        return parse_header(self.files[name])

    def block_index(self, name: str, header):
        data = self.files[name]
        return parse_block_index(data[header.block_index_offset:])

    def stream_blocks(self, name: str, start: int, end: int) -> Iterator[tuple[int, bytes]]:
        data, pos = self.files[name], start
        while pos < end:
            self._serve()
            length = struct.unpack_from("<Q", data, pos)[0]
            frame = data[pos + 8:pos + 8 + length]
            pos += 8 + length
            self.bytes_downloaded += 8 + length
            yield pos, frame

    def get_block(self, name: str, index: int) -> bytes:
        self._serve()
        h = parse_header(self.files[name])
        entry = self.block_index(name, h)[index]
        data = self.files[name]
        self.bytes_downloaded += entry.compressed_size
        return data[entry.offset + 8:entry.offset + 8 + entry.compressed_size]


def make_archive(purge_like: bool = False) -> dict[str, bytes]:
    """Four segments around a two-day window (17-18 Nov), with known contents.

    purge_like=True gives the same archive after a rewrite that dropped one
    early 'like' record: later byte offsets shift and seg_b's checksum changes,
    but no post is affected, so post counts must not change.
    """
    t = SINCE
    before = [[(1, t - 10 * MIN, 1, POST, post("kya hai yaar", ["en"])),
               (2, t - 5 * MIN, 1, POST, post("hello", ["en"]))]]
    day1 = [
        [(3, t - MIN, 1, POST, post("too early kya hai", ["en"])),            # outside window
         (4, t + MIN, 1, POST, post("Yaar kya hai", ["en"])),                 # match, latin, en
         (5, t + 2 * MIN, 1, LIKE, b"\xa0"),                                  # not a post
         (6, t + 3 * MIN, 1, POST, post("क्या हाल है", ["hi"]))],             # devanagari, hi
        [(7, t + 4 * MIN, 1, POST, post("کیا حال ہے", ["ur"])),               # arabic, ur
         (8, t + 5 * MIN, 3, POST, b""),                                      # delete
         (9, t + 5 * MIN, 7, POST, post("old resync kya hai", ["en"])),       # resync, skipped
         # 20-minute silence -> gap
         (10, t + 25 * MIN, 1, POST, post("nahi pata bhai")),                 # match, langs missing
         (11, t + 26 * MIN, 1, POST, post("hai", ["it"]))],                   # 1 keyword only
    ]
    t2 = t + DAY
    day2 = [
        [(12, t2 - 1, 1, POST, post("abhi nahi", ["en-US"])),                 # day 1, last µs
         (13, t2, 1, POST, post("kuch nahi hai", ["hi", "en"])),             # day 2 starts
         (14, t2 + MIN, 4, "", b"\xa0")],                                    # identity event
        [(15, t2 + 2 * MIN, 1, POST, post("plain english post", []))],
        [(16, UNTIL - MIN, 1, POST, post("kya baat hai", ["en"])),
         (17, UNTIL, 1, POST, post("after the window kya hai", ["en"]))],    # outside window
    ]
    after = [[(18, UNTIL + 10 * MIN, 1, POST, post("later", ["en"]))]]
    if purge_like:
        day1[0] = [e for e in day1[0] if e[0] != 5]             # the like, seq 5
    return {"seg_a.jss": encode_segment(before), "seg_b.jss": encode_segment(day1),
            "seg_c.jss": encode_segment(day2), "seg_d.jss": encode_segment(after)}


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    kw = tmp_path / "keywords.txt"
    kw.write_text("hai\nkya\nyaar\nnahi\npata\nbhai\nabhi\nkuch\nbaat\n", encoding="utf-8")
    s = Settings()
    s.keywords_file = kw
    s.prevalence.save_interval_s = 0          # checkpoint after every block
    s.prevalence.heartbeat_s = 1e9
    s.prevalence.backoff_initial_s = 0
    s.prevalence.backoff_max_s = 0
    return s


def run(settings: Settings, out: Path, client: FakeClient, fresh: bool = False) -> dict[str, Any]:
    ReplayPrevalence(settings, out, SINCE, UNTIL, fresh, client).run()
    return json.loads(out.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- decoding

def test_block_round_trip_skips_identifiers() -> None:
    frame = encode_block([(5, 100, 1, POST, post("kya hai")), (6, 101, 3, POST, b"")])
    events = list(iter_block_events(decompress_frame(frame)))
    assert [(e.seq, e.time_us, e.kind, e.collection) for e in events] == \
        [(5, 100, 1, POST), (6, 101, 3, POST)]
    assert cbor2.loads(events[0].payload)["text"] == "kya hai" and events[1].payload == b""
    assert not hasattr(events[0], "did")


# --------------------------------------------------------------------------- full replay

def test_replay_counts_days_scripts_and_coverage(settings: Settings, tmp_path: Path) -> None:
    out = tmp_path / "p7.json"
    state = run(settings, out, FakeClient(make_archive()))
    assert state["finished"] and state["window"] == {"since": "2026-11-17T00:00:00Z",
                                                     "until": "2026-11-19T00:00:00Z"}
    t = state["totals"]
    assert (t["scanned"], t["matched"], t["matched_1plus"]) == (9, 5, 6)
    assert t["scanned_scripts"] == {"latin": 7, "devanagari": 1, "arabic": 1}
    assert t["scanned_langs_script"]["hi"] == {"devanagari": 1}
    assert t["scanned_langs_script"]["ur"] == {"arabic": 1}
    assert t["matched_langs_script"] == {"en": {"latin": 2}, "(missing)": {"latin": 1},
                                         "en-US": {"latin": 1}, "hi,en": {"latin": 1}}

    d = state["daily"]
    assert sorted(d) == ["2026-11-17", "2026-11-18"]
    assert (d["2026-11-17"]["scanned"], d["2026-11-17"]["matched"]) == (6, 3)
    assert (d["2026-11-18"]["scanned"], d["2026-11-18"]["matched"]) == (3, 2)

    cov = state["coverage"]
    assert cov["first_event"] == "2026-11-17T00:01:00Z"
    assert cov["last_event"] == "2026-11-18T23:59:00Z"
    assert cov["post_creates"] == 9 and cov["post_deletes"] == 1
    assert cov["resync_post_records_skipped"] == 1
    assert cov["events_outside_window"] == 2
    assert cov["events_by_kind"] == {"create": 10, "delete": 1, "resync": 1, "identity": 1}
    # 20-minute silence, then the two long quiet stretches; edges are within 1 minute
    assert [g["length_s"] for g in cov["gaps"]] == [1200.0, 84840.0, 86220.0]
    assert cov["gaps"][0] == {"start": "2026-11-17T00:05:00Z", "end": "2026-11-17T00:25:00Z",
                              "length_s": 1200.0}
    assert state["summary"]["tagged_hi_by_script"] == {"devanagari": 1, "latin": 1}

    text = out.read_text(encoding="utf-8")
    for secret in ("Yaar kya hai", "did:plc:", "rk4", "at://"):
        assert secret not in text


def test_resume_after_crash_gives_same_counts(settings: Settings, tmp_path: Path) -> None:
    clean = run(settings, tmp_path / "clean.json", FakeClient(make_archive()))
    for fail_at in range(5):                          # crash before each of the 5 blocks
        out = tmp_path / f"crash{fail_at}.json"
        crashing = FakeClient(make_archive(), fail_at=fail_at, exc=RuntimeError("power cut"))
        with pytest.raises(RuntimeError):
            run(settings, out, crashing)
        saved = json.loads(out.read_text(encoding="utf-8"))
        assert not saved["finished"]
        resumed = run(settings, out, FakeClient(make_archive()))
        assert resumed["coverage"]["resumes"] == 1
        for key in ("totals", "daily", "summary"):
            assert resumed[key] == clean[key], (fail_at, key)
        for key in ("events_in_window", "post_creates", "gaps", "events_by_kind"):
            assert resumed["coverage"][key] == clean["coverage"][key], (fail_at, key)


def same_counts(a: dict[str, Any], b: dict[str, Any]) -> None:
    for key in ("totals", "daily", "summary"):
        assert a[key] == b[key], key
    for key in ("events_in_window", "post_creates", "gaps", "events_by_kind"):
        assert a["coverage"][key] == b["coverage"][key], key


def test_resume_after_archive_rewrite(settings: Settings, tmp_path: Path) -> None:
    """Crash after seg_b's first block; overnight the archive rewrites seg_b (offsets shift)."""
    clean = run(settings, tmp_path / "clean.json", FakeClient(make_archive()))
    out = tmp_path / "p7.json"
    with pytest.raises(RuntimeError):
        run(settings, out, FakeClient(make_archive(), fail_at=1, exc=RuntimeError("stop")))
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["replay"]["position"]["offset"] is not None    # a now-stale byte offset
    resumed = run(settings, out, FakeClient(make_archive(purge_like=True)))
    same_counts(resumed, clean)
    assert resumed["coverage"]["replans"] == 1


def test_rewrite_during_run_triggers_replan(settings: Settings, tmp_path: Path) -> None:
    clean = run(settings, tmp_path / "clean.json", FakeClient(make_archive()))
    client = FakeClient(make_archive(), fail_at=1, exc=TransientError("connection closed"),
                        rewrite_on_fail=make_archive(purge_like=True))
    state = run(settings, tmp_path / "p7.json", client)
    same_counts(state, clean)
    assert state["coverage"]["replans"] == 1 and state["coverage"]["reconnects"] == 1


def test_legacy_checkpoint_without_seq_resumes(settings: Settings, tmp_path: Path) -> None:
    """Files saved before `seq` was recorded resume from the last counted event's time."""
    clean = run(settings, tmp_path / "clean.json", FakeClient(make_archive()))
    out = tmp_path / "p7.json"
    with pytest.raises(RuntimeError):
        run(settings, out, FakeClient(make_archive(), fail_at=1, exc=RuntimeError("stop")))
    saved = json.loads(out.read_text(encoding="utf-8"))
    pos = saved["replay"]["position"]
    for key in ("seq", "checksum"):
        pos.pop(key)
    saved["coverage"].pop("replans")
    out.write_text(json.dumps(saved), encoding="utf-8")
    resumed = run(settings, out, FakeClient(make_archive(purge_like=True)))
    same_counts(resumed, clean)


def test_transient_error_is_retried_without_double_counting(settings: Settings, tmp_path: Path) -> None:
    clean = run(settings, tmp_path / "clean.json", FakeClient(make_archive()))
    flaky = FakeClient(make_archive(), fail_at=3, exc=TransientError("429", retry_after_s=0))
    state = run(settings, tmp_path / "flaky.json", flaky)
    assert state["totals"] == clean["totals"]
    assert (state["coverage"]["reconnects"], state["coverage"]["rate_limited"]) == (1, 1)


def test_other_run_in_output_is_not_overwritten(settings: Settings, tmp_path: Path) -> None:
    out = tmp_path / "prevalence.json"
    out.write_text(json.dumps({"started_at": "x", "finished": True}), encoding="utf-8")
    with pytest.raises(SystemExit):
        ReplayPrevalence(settings, out, SINCE, UNTIL, False, FakeClient(make_archive()))
    assert json.loads(out.read_text(encoding="utf-8")) == {"started_at": "x", "finished": True}


def test_summary_rate_per_100k() -> None:
    counts = {"scanned": 200_000, "matched": 3, "matched_1plus": 10, "matched_langs": {"en": 3},
              "scanned_langs_script": {}, "scanned_scripts": {}}
    assert summarize(counts)["matched_per_100k"] == 1.5
