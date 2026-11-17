"""Print summary statistics for the raw collected posts."""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from collect import BSKY_MODERATION_DID, read_jsonl

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
    items = counter.most_common(top)
    width = max([28, *(len(str(k)) for k, _ in items)])
    for key, n in items:
        print(f"  {key:<{width}} {n:>7}  {100 * n / max(total, 1):5.1f}%")


def summarize(rows: list[dict[str, Any]]) -> None:
    total = len(rows)
    print(f"Total posts: {total}")
    if not total:
        return
    by_did = Counter(r.get("did") for r in rows)
    top5 = by_did.most_common(5)
    print(f"Accounts: {len(by_did)} | top-5 accounts hold {sum(n for _, n in top5)} posts "
          f"({100 * sum(n for _, n in top5) / total:.1f}%): {[n for _, n in top5]}")
    links = [r.get("has_link") for r in rows if r.get("has_link") is not None]
    if links:
        print(f"Posts with a link: {sum(links)} ({100 * sum(links) / total:.1f}%)")
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
    print_label_counts(rows, total)


def label_source(src: str, author_did: str) -> str:
    if src == BSKY_MODERATION_DID:
        return "bluesky-moderation"
    if src == author_did:
        return "self-label"
    return "other labeler"


def label_counter(rows: list[dict[str, Any]], field: str) -> tuple[Counter[str], int]:
    """Posts carrying each (label value, source); negated labels are skipped."""
    counts: Counter[str] = Counter()
    available = 0
    for r in rows:
        labels = r.get(field)
        if labels is None:          # not collected for this row (e.g. Jetstream)
            continue
        available += 1
        counts.update({f"{lab.get('val')} [{label_source(lab.get('src', ''), r.get('did', ''))}]"
                       for lab in labels if not lab.get("neg")})
    return counts, available


def print_label_counts(rows: list[dict[str, Any]], total: int) -> None:
    for field, title in (("labels", "Post moderation labels"),
                         ("author_labels", "Author (account) labels")):
        counts, available = label_counter(rows, field)
        if not available:
            print(f"\n{title}: not available in this file")
            continue
        with_any = sum(1 for r in rows if any(not x.get("neg") for x in r.get(field) or []))
        print_counter(f"{title} — {with_any} of {available} posts have at least one "
                      f"(posts per label value):", counts, available)


def main(argv: list[str] | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=ROOT / "data/raw/posts.jsonl")
    p.add_argument("--clean", action="store_true", help="use data/raw/posts_clean.jsonl")
    args = p.parse_args(argv)
    if args.clean:
        args.input = ROOT / "data/raw/posts_clean.jsonl"
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
