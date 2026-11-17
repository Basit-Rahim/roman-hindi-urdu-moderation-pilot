from pathlib import Path

import pytest

from common.filters import KeywordMatcher, detect_script, load_keywords

KEYWORDS = ["hai", "nahi", "kya", "yaar", "bhai", "acha", "tha"]


@pytest.fixture
def matcher() -> KeywordMatcher:
    return KeywordMatcher(KEYWORDS)


# ---------------------------------------------------------------- whole-word matching

@pytest.mark.parametrize("text", ["chai", "haircut", "Thailand", "nahiii", "bhaijaan", "kyaaa"])
def test_substrings_inside_words_do_not_match(matcher: KeywordMatcher, text: str) -> None:
    assert matcher.find(text) == []


@pytest.mark.parametrize("text, expected", [
    ("hai", ["hai"]),
    ("HAI", ["hai"]),
    ("Kya?", ["kya"]),
    ("#yaar", ["yaar"]),
    ("kya-hai", ["kya", "hai"]),
    ("kya_hai", ["kya", "hai"]),
    ("(nahi)", ["nahi"]),
    ("bhai😂", ["bhai"]),
])
def test_whole_words_match_at_punctuation_and_case(matcher, text, expected) -> None:
    assert matcher.find(text) == expected


def test_accented_letters_are_part_of_the_word(matcher: KeywordMatcher) -> None:
    assert matcher.find("haí taí") == []


def test_returns_distinct_keywords_in_order(matcher: KeywordMatcher) -> None:
    assert matcher.find("Kya hai yaar, kya HAI") == ["kya", "hai", "yaar"]


# ---------------------------------------------------------------- 2-keyword rule

def test_single_keyword_is_rejected(matcher: KeywordMatcher) -> None:
    # Italian "hai" alone must not pass.
    assert not matcher.is_match("Hai visto il film?", min_matches=2)


def test_repeated_single_keyword_is_rejected(matcher: KeywordMatcher) -> None:
    assert not matcher.is_match("hai hai hai hai", min_matches=2)


def test_two_distinct_keywords_pass(matcher: KeywordMatcher) -> None:
    assert matcher.is_match("Yaar ye kya hai", min_matches=2)


def test_min_matches_is_configurable(matcher: KeywordMatcher) -> None:
    text = "kya hai"
    assert matcher.is_match(text, min_matches=1)
    assert matcher.is_match(text, min_matches=2)
    assert not matcher.is_match(text, min_matches=3)


def test_empty_keyword_list_raises() -> None:
    with pytest.raises(ValueError):
        KeywordMatcher(["", "  "])


def test_load_keywords_skips_comments_and_dupes(tmp_path: Path) -> None:
    f = tmp_path / "kw.txt"
    f.write_text("# comment\nHai\n\nnahi\nhai\n", encoding="utf-8")
    assert load_keywords(f) == ["hai", "nahi"]


def test_shipped_keyword_file_loads() -> None:
    words = load_keywords(Path(__file__).resolve().parents[1] / "config/keywords.txt")
    assert "hai" in words and "dost" in words and len(words) == 53


# ---------------------------------------------------------------- script detection

@pytest.mark.parametrize("text, expected", [
    ("Yaar kya hai", "latin"),
    ("Kyá baat hai 😂 123!", "latin"),
    ("یار کیا بات ہے", "arabic"),
    ("यार क्या बात है", "devanagari"),
    ("kya baat hai یار", "mixed"),
    ("kya baat hai यार", "mixed"),
    ("یار यार", "mixed"),
    ("😂 123 !!!", "other"),
    ("", "other"),
])
def test_detect_script(text: str, expected: str) -> None:
    assert detect_script(text) == expected
