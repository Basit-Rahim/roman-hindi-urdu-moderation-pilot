"""Client-side filtering: keyword matching, script detection and link detection."""
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
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


SCRIPTS = ("latin", "arabic", "devanagari")


def _letter_script(ch: str) -> str | None:
    """Script of one character: a letter whose Unicode name starts with the script name."""
    if not ch.isalpha():
        return None
    name = unicodedata.name(ch, "")
    for script in SCRIPTS:
        if name.startswith(script.upper()):
            return script
    return None


@lru_cache(maxsize=1)
def _script_patterns() -> tuple[tuple[str, re.Pattern[str]], ...]:
    """One character-class regex per script, built once from _letter_script.

    Scanning all code points takes about a second, but each detect_script call
    is then a few regex searches instead of a Python loop over characters,
    which matters when classifying millions of posts.
    """
    ranges: dict[str, list[list[int]]] = {s: [] for s in SCRIPTS}
    for cp in range(0x110000):
        if 0xD800 <= cp <= 0xDFFF:
            continue
        script = _letter_script(chr(cp))
        if script:
            spans = ranges[script]
            if spans and spans[-1][1] == cp - 1:
                spans[-1][1] = cp
            else:
                spans.append([cp, cp])
    esc = lambda cp: f"\\U{cp:08x}"
    return tuple((s, re.compile("[" + "".join(f"{esc(a)}-{esc(b)}" for a, b in ranges[s]) + "]"))
                 for s in SCRIPTS)


def detect_script(text: str) -> str:
    """Classify text by the scripts of its letters.

    Returns 'latin', 'arabic' or 'devanagari' if exactly one of these scripts
    occurs, 'mixed' if two or more occur, and 'other' if none occurs (e.g. only
    emoji, digits or another script such as Japanese). Punctuation, digits,
    emoji and letters of other scripts are ignored.
    """
    text = text or ""
    present = [s for s, pattern in _script_patterns() if pattern.search(text)]
    if len(present) > 1:
        return "mixed"
    return present[0] if present else "other"


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
