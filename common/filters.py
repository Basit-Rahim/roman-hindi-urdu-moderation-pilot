"""Client-side filtering: keyword matching, script detection and link detection."""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable

def load_keywords(path: Path) -> list[str]:
    """Read one keyword per line; skip blanks and '#' comments; lowercase and dedupe."""
    seen: dict[str, None] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        word = line.strip().lower()
        if word and not word.startswith("#"):
            seen[word] = None
    return list(seen)


class KeywordMatcher:
    """Finds distinct whole-word, case-insensitive keyword matches in text.

    A "word" boundary is any non-letter/non-digit character (or text edge), so
    'hai' matches in 'Hai!', '#hai' and 'kya-hai' but not in 'chai' or 'haircut'.
    """

    def __init__(self, keywords: Iterable[str]) -> None:
        words = sorted({k.lower() for k in keywords if k.strip()}, key=len, reverse=True)
        if not words:
            raise ValueError("keyword list is empty")
        alternation = "|".join(re.escape(w) for w in words)
        # [^\W_] = a Unicode letter or digit. Underscore counts as a separator.
        self._pattern = re.compile(rf"(?<![^\W_])(?:{alternation})(?![^\W_])", re.IGNORECASE)

    def find(self, text: str) -> list[str]:
        """Distinct matched keywords (lowercased), in order of first appearance."""
        found: dict[str, None] = {}
        for m in self._pattern.finditer(text or ""):
            found[m.group(0).lower()] = None
        return list(found)

    def is_match(self, text: str, min_matches: int = 2) -> bool:
        return len(self.find(text)) >= min_matches


def _letter_script(ch: str) -> str | None:
    if not ch.isalpha():
        return None
    name = unicodedata.name(ch, "")
    if name.startswith("LATIN"):
        return "latin"
    if name.startswith("ARABIC"):
        return "arabic"
    if name.startswith("DEVANAGARI"):
        return "devanagari"
    return None


def detect_script(text: str) -> str:
    """Classify text by the scripts of its letters.

    Returns 'latin', 'arabic' or 'devanagari' if exactly one of these scripts
    occurs, 'mixed' if two or more occur, and 'other' if none occurs (e.g. only
    emoji, digits or another script; this cannot happen for kept posts, which
    always contain Latin keywords). Punctuation, digits, emoji and letters of
    other scripts are ignored.
    """
    present = {s for s in map(_letter_script, text or "") if s}
    if len(present) > 1:
        return "mixed"
    if len(present) == 1:
        return present.pop()
    return "other"


# --------------------------------------------------------------------------- links

LINK_RE = re.compile(r"https?://\S+|www\.\S+|\b(?:[a-z0-9-]+\.)+[a-z]{2,}/\S*", re.IGNORECASE)


def text_has_link(text: str) -> bool:
    """URL-like text: http(s)://, www., or domain.tld/path (Bluesky shortens link text)."""
    return bool(LINK_RE.search(text or ""))


def record_has_link(record: dict[str, Any]) -> bool:
    """True if the post links out: a link facet, an external embed, or a URL in the text."""
    for facet in record.get("facets") or []:
        for feature in facet.get("features") or []:
            if str(feature.get("$type", "")).endswith("#link"):
                return True
    embed = record.get("embed") or {}
    for e in (embed, embed.get("media") or {}):
        if str(e.get("$type", "")).startswith("app.bsky.embed.external"):
            return True
    return text_has_link(record.get("text") or "")
