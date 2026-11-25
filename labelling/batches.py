"""Batches for labelling posts by hand (Claude Code labels them itself).

Batches are consecutive slices of BATCH_SIZE posts of posts_clean.jsonl in
file order. Posts whose uri is already in the labels file are skipped, so
labelling resumes where it stopped.

    python -m labelling.batches status
    python -m labelling.batches show 3          # print batch 3's unlabelled posts
    python -m labelling.batches add 3 roman_hu mostly_en ...   # one label per shown post, in order
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

POSTS = Path("data/raw/posts_clean.jsonl")
LABELS = Path("data/labels/llm_labels.jsonl")
BATCH_SIZE = 25
VALID = {"roman_hu", "mostly_en", "other", "unclear"}
LABELLER = "Claude Code (claude-opus-5-5)"
PROMPT_VERSION = "v1"


def load_posts() -> list[dict]:
    with POSTS.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def labelled_uris() -> set[str]:
    if not LABELS.exists():
        return set()
    with LABELS.open(encoding="utf-8") as f:
        return {json.loads(line)["uri"] for line in f if line.strip()}


def batch(posts: list[dict], n: int) -> list[dict]:
    if n < 1 or (n - 1) * BATCH_SIZE >= len(posts):
        sys.exit(f"batch {n} does not exist (1-{-(-len(posts) // BATCH_SIZE)})")
    return posts[(n - 1) * BATCH_SIZE : n * BATCH_SIZE]


def pending(posts: list[dict], n: int) -> list[dict]:
    done = labelled_uris()
    return [p for p in batch(posts, n) if p["uri"] not in done]


def cmd_status(posts: list[dict]) -> None:
    done = labelled_uris()
    total = -(-len(posts) // BATCH_SIZE)
    first_open = next(
        (n for n in range(1, total + 1) if any(p["uri"] not in done for p in batch(posts, n))),
        None,
    )
    print(f"{len(done)} of {len(posts)} posts labelled; {total} batches")
    print("all batches done" if first_open is None else f"next batch: {first_open}")


def cmd_show(posts: list[dict], n: int) -> None:
    for i, p in enumerate(pending(posts, n), 1):
        text = " ".join(p["text"].split())
        print(f"[{i}] {text}")


def cmd_add(posts: list[dict], n: int, labels: list[str]) -> None:
    todo = pending(posts, n)
    bad = [x for x in labels if x not in VALID]
    if bad:
        sys.exit(f"invalid labels: {bad}")
    if len(labels) != len(todo):
        sys.exit(f"batch {n} has {len(todo)} unlabelled posts but {len(labels)} labels were given")
    today = dt.date.today().isoformat()
    LABELS.parent.mkdir(parents=True, exist_ok=True)
    with LABELS.open("a", encoding="utf-8") as f:
        for p, label in zip(todo, labels):
            row = {
                "uri": p["uri"],
                "label": label,
                "labeller": LABELLER,
                "date": today,
                "prompt_version": PROMPT_VERSION,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"batch {n}: wrote {len(todo)} labels")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    s = sub.add_parser("show")
    s.add_argument("batch", type=int)
    a = sub.add_parser("add")
    a.add_argument("batch", type=int)
    a.add_argument("labels", nargs="+")
    args = ap.parse_args()

    posts = load_posts()
    if args.cmd == "status":
        cmd_status(posts)
    elif args.cmd == "show":
        cmd_show(posts, args.batch)
    else:
        cmd_add(posts, args.batch, args.labels)


if __name__ == "__main__":
    main()
