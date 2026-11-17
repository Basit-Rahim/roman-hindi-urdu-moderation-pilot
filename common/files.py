"""JSON / JSONL file helpers."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Iterator, TextIO


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield one dict per line; skips blank lines and a half-written last line."""
    if not path.exists():
        return
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def append_jsonl(fh: TextIO, obj: dict[str, Any]) -> None:
    fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
    fh.flush()


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    """Write rows to path atomically (via a .tmp file)."""
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(path)


def write_json(path: Path, obj: Any) -> None:
    """Write obj as indented JSON atomically (via a .tmp file)."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=False), encoding="utf-8")
    os.replace(tmp, path)
