"""Settings: config/settings.yaml, plus TARGET_COUNT / MIN_MATCHES env overrides."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

import yaml

from common.bluesky import BSKY_MODERATION_DID

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SETTINGS_PATH = ROOT / "config/settings.yaml"


@dataclass
class SearchSettings:
    target_count: int = 10_000
    output_dir: Path = ROOT / "data/raw"
    progress_every: int = 100
    host: str = "https://bsky.social"
    queries_file: Path = ROOT / "config/search_queries.txt"
    page_limit: int = 100
    max_pages: int = 0                       # 0 = paginate fully
    sort: str = "latest"
    delay_s: float = 1
    max_retries: int = 5
    accept_labelers: str = BSKY_MODERATION_DID


@dataclass
class PrevalenceSettings:
    hours: float = 24
    output: Path = ROOT / "data/prevalence/prevalence.json"
    save_interval_s: float = 60
    heartbeat_s: float = 60
    jetstream_url: str = (
        "wss://jetstream.us-east.bsky.network/xrpc/"
        "network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post"
    )
    backoff_initial_s: float = 1
    backoff_max_s: float = 60
    # Replay mode (--since/--until): Jetstream v2 archive over HTTP
    replay_host: str = "https://jetstream.us-east.bsky.network"
    replay_output: Path = ROOT / "data/prevalence/prevalence_7d.json"
    gap_threshold_s: float = 300


@dataclass
class CleanSettings:
    input: Path = ROOT / "data/raw/posts.jsonl"
    output: Path = ROOT / "data/raw/posts_clean.jsonl"
    max_per_account: int = 5
    seed: int = 42


@dataclass
class Settings:
    min_matches: int = 2
    keywords_file: Path = ROOT / "config/keywords.txt"
    search: SearchSettings = field(default_factory=SearchSettings)
    prevalence: PrevalenceSettings = field(default_factory=PrevalenceSettings)
    clean: CleanSettings = field(default_factory=CleanSettings)


def _fill(obj: Any, raw: dict[str, Any]) -> None:
    """Copy values from a YAML mapping onto a settings dataclass.

    Values are converted to the type of the default; paths are relative to ROOT.
    Nested sections are handled by the caller.
    """
    for f in fields(obj):
        current = getattr(obj, f.name)
        if is_dataclass(current) or raw.get(f.name) is None:
            continue
        value = raw[f.name]
        setattr(obj, f.name, ROOT / value if isinstance(current, Path) else type(current)(value))


def load_settings(path: Path = DEFAULT_SETTINGS_PATH) -> Settings:
    raw: dict[str, Any] = (yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else None) or {}
    s = Settings()
    _fill(s, raw)
    _fill(s.search, raw.get("search") or {})
    _fill(s.prevalence, raw.get("prevalence") or {})
    _fill(s.clean, raw.get("clean") or {})
    s.search.host = s.search.host.rstrip("/")
    if os.environ.get("TARGET_COUNT"):
        s.search.target_count = int(os.environ["TARGET_COUNT"])
    if os.environ.get("MIN_MATCHES"):
        s.min_matches = int(os.environ["MIN_MATCHES"])
    return s
