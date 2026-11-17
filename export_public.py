"""Write a text-free public ID file: data/public/posts_ids.csv.

Columns: uri, langs, script, match_count, created_at. No text, no handles.
Posts listed in deleted.jsonl (deleted posts, or deactivated/deleted accounts)
are removed.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Iterable

from collect import read_jsonl

ROOT = Path(__file__).resolve().parent
COLUMNS = ("uri", "langs", "script", "match_count", "created_at")


def load_removals(deleted_path: Path) -> tuple[set[str], set[str]]:
    """Return (deleted post URIs, DIDs of accounts that are gone)."""
    uris: set[str] = set()
    dids: set[str] = set()
    for rec in read_jsonl(deleted_path):
        if rec.get("type") == "account" and rec.get("did"):
            dids.add(rec["did"])
        elif rec.get("uri"):
            uris.add(rec["uri"])
    return uris, dids


def public_rows(rows: Iterable[dict[str, Any]], deleted_uris: set[str],
                gone_dids: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        uri = r["uri"]
        if uri in seen or uri in deleted_uris or r.get("did") in gone_dids:
            continue
        seen.add(uri)
        langs = r.get("langs")
        out.append({
            "uri": uri,
            "langs": "" if langs is None else ";".join(langs),  # empty = not declared
            "script": r.get("script"),
            "match_count": r.get("match_count"),
            "created_at": r.get("created_at"),
        })
    return out


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=ROOT / "data/raw/posts.jsonl")
    p.add_argument("--output", type=Path, default=ROOT / "data/public/posts_ids.csv")
    args = p.parse_args(argv)
    deleted_uris, gone_dids = load_removals(args.input.with_name("deleted.jsonl"))
    rows = list(read_jsonl(args.input))
    out = public_rows(rows, deleted_uris, gone_dids)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(out)
    print(f"Wrote {len(out)} rows to {args.output} "
          f"(removed {len(rows) - len(out)} deleted/duplicate posts)")


if __name__ == "__main__":
    main()
