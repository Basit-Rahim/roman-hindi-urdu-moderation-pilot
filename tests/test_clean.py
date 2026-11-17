from collection.clean import cap_per_account, clean, drop_duplicate_texts
from common.filters import record_has_link, text_has_link


def row(i: int, did: str, text: str) -> dict:
    return {"uri": f"at://{did}/p/{i}", "did": did, "text": text}


def test_duplicate_texts_normalized_keep_first() -> None:
    rows = [row(1, "a", "Kya  hai yaar"), row(2, "b", "kya hai\nYAAR "), row(3, "c", "other")]
    assert [r["uri"] for r in drop_duplicate_texts(rows)] == ["at://a/p/1", "at://c/p/3"]


def test_cap_is_deterministic_and_preserves_order() -> None:
    rows = [row(i, "spam", f"t{i}") for i in range(20)] + [row(99, "x", "solo")]
    first = cap_per_account(rows, 5, seed=42)
    assert first == cap_per_account(rows, 5, seed=42)
    assert sum(r["did"] == "spam" for r in first) == 5 and first[-1]["did"] == "x"
    assert [r["uri"] for r in first] == sorted((r["uri"] for r in first), key=lambda u: [r["uri"] for r in rows].index(u))
    assert cap_per_account(rows, 5, seed=1) != first


def test_clean_report_and_has_link() -> None:
    rows = [row(1, "a", "see example.com/x"), row(2, "a", "SEE example.com/x"), row(3, "b", "no link here")]
    out, report = clean(rows, cap=5, seed=0)
    assert report == {"input": 3, "duplicate_texts_removed": 1, "removed_by_account_cap": 0, "output": 2}
    assert [r["has_link"] for r in out] == [True, False]


def test_link_detection() -> None:
    assert text_has_link("https://x.co") and text_has_link("okfilmyduniya.blogspot.com/2026/11/hot-...")
    assert not text_has_link("kya hai. yaar") and not text_has_link("e.g. this")
    assert record_has_link({"text": "", "embed": {"$type": "app.bsky.embed.external"}})
    assert record_has_link({"text": "", "facets": [{"features": [{"$type": "app.bsky.richtext.facet#link"}]}]})
    assert not record_has_link({"text": "kya hai"})
