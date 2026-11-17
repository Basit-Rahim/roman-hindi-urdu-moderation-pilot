"""Print summary statistics for the raw collected posts."""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from collect import read_jsonl

ROOT = Path(__file__).resolve().parent
MAIN_LANGS = ("en", "ur", "hi")


def langs_bucket(langs: Any) -> str:
    """Group a post's raw `langs` value: missing, empty, en/ur/hi, multi or other."""
    if langs is None:
        return "(missing)"
    if not langs:
        return "(empty list)"
    if len(langs) > 1:
        return "multiple: " + ",".join(langs)
    tag = str(langs[0])
    base = tag.split("-")[0].lower()
    return tag if base in MAIN_LANGS else f"other ({tag})"


def print_counter(title: str, counter: Counter[str], total: int, top: int | None = None) -> None:
    print(f"\n{title}")
    for key, n in counter.most_common(top):
        print(f"  {key:<28} {n:>7}  {100 * n / max(total, 1):5.1f}%")


def summarize(rows: list[dict[str, Any]]) -> None:
    total = len(rows)
    print(f"Total posts: {total}")
    if not total:
        return
    print_counter("Posts by source:", Counter(r.get("source", "?") for r in rows), total)
    print_counter("Posts by script:", Counter(r.get("script", "?") for r in rows), total)
    print_counter("Posts by langs value:", Counter(langs_bucket(r.get("langs")) for r in rows),
                  total, top=25)
    tags: Counter[str] = Counter()
    for r in rows:
        tags.update(set(r.get("langs") or []))
    print_counter("Posts declaring each language tag (a post can declare several):", tags,
                  total, top=15)
    replies = sum(1 for r in rows if r.get("is_reply"))
    print(f"\nReplies: {replies} ({100 * replies / total:.1f}%)")
    print_counter("Match count distribution:", Counter(str(r.get("match_count")) for r in rows),
                  total)
    kw: Counter[str] = Counter()
    for r in rows:
        kw.update(r.get("matched_keywords") or [])
    print_counter("Top 20 matched keywords (posts containing each):", kw, total, top=20)


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=ROOT / "data/raw/posts.jsonl")
    args = p.parse_args(argv)
    if not args.input.exists():
        raise SystemExit(f"No file at {args.input}")
    rows = list(read_jsonl(args.input))
    deleted = list(read_jsonl(args.input.with_name("deleted.jsonl")))
    print(f"File: {args.input}")
    summarize(rows)
    if deleted:
        print(f"\nDeletion records logged: {len(deleted)} "
              f"({Counter(d.get('type') for d in deleted)})")


if __name__ == "__main__":
    main()
