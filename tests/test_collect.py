from collect import (AccountEvent, CommitEvent, build_post, iso_to_us, parse_event,
                     with_cursor)
from export_public import public_rows
from filters import KeywordMatcher

RECORD = {"$type": "app.bsky.feed.post", "text": "Yaar kya hai", "langs": ["en"],
          "createdAt": "2026-11-17T10:43:33.659Z"}


def test_parse_v1_create() -> None:
    msg = {"did": "did:plc:a", "time_us": 1794912217052828, "cursor": 27276127740,
           "kind": "commit", "commit": {"operation": "create", "collection": "app.bsky.feed.post",
                                        "rkey": "r1", "record": RECORD}}
    ev = parse_event(msg)
    assert isinstance(ev, CommitEvent)
    assert (ev.operation, ev.did, ev.rkey, ev.time_us, ev.seq) == (
        "create", "did:plc:a", "r1", 1794912217052828, 27276127740)


def test_parse_v2_delete() -> None:
    msg = {"$type": "message", "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit", "collection": "app.bsky.feed.post",
        "did": "did:plc:b", "operation": "delete", "rkey": "r2", "seq": 27276129920,
        "time": "2026-11-17T10:43:45.152911Z"}}
    ev = parse_event(msg)
    assert isinstance(ev, CommitEvent)
    assert ev.operation == "delete" and ev.record is None and ev.seq == 27276129920
    assert ev.time_us == iso_to_us("2026-11-17T10:43:45.152911Z")


def test_parse_v2_account() -> None:
    msg = {"$type": "message", "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#account", "did": "did:plc:c",
        "active": False, "status": "deleted", "seq": 5, "time": "2026-11-17T10:00:00Z"}}
    ev = parse_event(msg)
    assert isinstance(ev, AccountEvent) and not ev.active and ev.status == "deleted"


def test_iso_to_us_handles_any_fraction() -> None:
    assert iso_to_us("1970-01-01T00:00:01Z") == 1_000_000
    assert iso_to_us("1970-01-01T00:00:01.5Z") == 1_500_000
    assert iso_to_us("1970-01-01T00:00:01.123456789Z") == 1_123_456
    assert iso_to_us("garbage") is None


def test_with_cursor_replaces_existing() -> None:
    url = "wss://h/subscribe?wantedCollections=app.bsky.feed.post&cursor=1"
    assert with_cursor(url, 42).endswith("wantedCollections=app.bsky.feed.post&cursor=42")
    assert with_cursor(url, None) == url


def test_build_post_fields() -> None:
    m = KeywordMatcher(["yaar", "kya", "hai"])
    row = build_post(did="did:plc:a", rkey="r1", record=RECORD, source="jetstream",
                     matcher=m, min_matches=2)
    assert row is not None
    assert row["uri"] == "at://did:plc:a/app.bsky.feed.post/r1"
    assert row["matched_keywords"] == ["yaar", "kya", "hai"] and row["match_count"] == 3
    assert row["is_reply"] is False and row["script"] == "latin" and row["langs"] == ["en"]
    no_langs = {k: v for k, v in RECORD.items() if k != "langs"}
    assert build_post(did="d", rkey="r", record=no_langs, source="search", matcher=m,
                      min_matches=2)["langs"] is None
    assert build_post(did="d", rkey="r", record={"text": "hai"}, source="jetstream",
                      matcher=m, min_matches=2) is None


def test_public_rows_drop_text_and_deleted() -> None:
    rows = [
        {"uri": "at://a/p/1", "did": "a", "text": "secret", "langs": ["en"], "script": "latin",
         "match_count": 2, "created_at": "t"},
        {"uri": "at://a/p/2", "did": "a", "text": "x", "langs": None, "script": "latin",
         "match_count": 2, "created_at": "t"},
        {"uri": "at://b/p/3", "did": "b", "text": "y", "langs": [], "script": "latin",
         "match_count": 2, "created_at": "t"},
    ]
    out = public_rows(rows, deleted_uris={"at://a/p/2"}, gone_dids={"b"})
    assert out == [{"uri": "at://a/p/1", "langs": "en", "script": "latin", "match_count": 2,
                    "created_at": "t"}]


def test_prevalence_counts_only_aggregates() -> None:
    from prevalence import count_post, empty_counts
    m = KeywordMatcher(["yaar", "kya", "hai"])
    counts = empty_counts()
    count_post(counts, RECORD, m, 2)                                 # match, langs en
    count_post(counts, {"text": "hai there"}, m, 2)                  # 1 keyword, langs missing
    count_post(counts, {"text": "hello", "langs": []}, m, 2)         # no keyword
    assert (counts["scanned"], counts["matched"], counts["matched_1plus"]) == (3, 1, 2)
    assert counts["scanned_langs"] == {"en": 1, "(missing)": 1, "(empty)": 1}
    assert counts["matched_langs"] == {"en": 1} and counts["matched_scripts"] == {"latin": 1}
    assert "Yaar kya hai" not in str(counts)                         # no text stored
