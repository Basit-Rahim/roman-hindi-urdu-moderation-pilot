"""Make a cleaned copy of the raw posts: data/raw/posts_clean.jsonl.

The original file is never modified. Steps, in order:
1. Drop exact duplicate texts (case-folded, whitespace collapsed); keep the first.
2. Cap each account at max_per_account posts (random pick, fixed seed).
3. Ensure every row has has_link (true/false).
Rows keep their original order.
"""
from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from collect import ROOT, read_jsonl, text_has_link


def normalize_text(text: str) -> str:
    return " ".join((text or "").casefold().split())


def drop_duplicate_texts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        key = normalize_text(r.get("text", ""))
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def cap_per_account(rows: list[dict[str, Any]], cap: int, seed: int) -> list[dict[str, Any]]:
    """Keep at most `cap` randomly chosen rows per DID; deterministic for a given seed."""
    by_did: dict[str, list[int]] = defaultdict(list)
    for i, r in enumerate(rows):
        by_did[r["did"]].append(i)
    rng = random.Random(seed)
    keep: set[int] = set()
    for did in sorted(by_did):                      # sorted -> stable RNG consumption
        idx = by_did[did]
        keep.update(idx if len(idx) <= cap else rng.sample(idx, cap))
    return [r for i, r in enumerate(rows) if i in keep]


def add_has_link(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for r in rows:
        if r.get("has_link") is None:               # older rows: fall back to the text
            r["has_link"] = text_has_link(r.get("text", ""))
    return rows


def clean(rows: list[dict[str, Any]], cap: int, seed: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    uniq = drop_duplicate_texts(rows)
    capped = cap_per_account(uniq, cap, seed)
    report = {"input": len(rows), "duplicate_texts_removed": len(rows) - len(uniq),
              "removed_by_account_cap": len(uniq) - len(capped), "output": len(capped)}
    return add_has_link(capped), report


def main(argv: list[str] | None = None) -> None:
    cfg = (yaml.safe_load((ROOT / "config/settings.yaml").read_text(encoding="utf-8")) or {}).get("clean") or {}
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--input", type=Path, default=ROOT / cfg.get("input", "data/raw/posts.jsonl"))
    ap.add_argument("--output", type=Path, default=ROOT / cfg.get("output", "data/raw/posts_clean.jsonl"))
    ap.add_argument("--max-per-account", type=int, default=int(cfg.get("max_per_account", 5)))
    ap.add_argument("--seed", type=int, default=int(cfg.get("seed", 42)))
    args = ap.parse_args(argv)
    if args.input.resolve() == args.output.resolve():
        raise SystemExit("--output must differ from --input (the original is kept)")
    rows, report = clean(list(read_jsonl(args.input)), args.max_per_account, args.seed)
    tmp = args.output.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(args.output)
    print(f"{args.input.name} -> {args.output.name}: {report} "
          f"(max_per_account={args.max_per_account}, seed={args.seed})")


if __name__ == "__main__":
    main()
