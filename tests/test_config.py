from pathlib import Path

from common.config import ROOT, load_settings


def test_shipped_settings_load_into_sections() -> None:
    s = load_settings()
    assert s.min_matches == 2 and s.keywords_file == ROOT / "config/keywords.txt"
    assert s.search.target_count == 10_000 and s.search.max_pages == 0
    assert s.search.output_dir == ROOT / "data/raw"
    assert s.prevalence.output == ROOT / "data/prevalence/prevalence.json"
    assert s.prevalence.jetstream_url.startswith("wss://jetstream.us-east.bsky.network/xrpc/")
    assert (s.clean.max_per_account, s.clean.seed) == (5, 42)


def test_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("TARGET_COUNT", "123")
    monkeypatch.setenv("MIN_MATCHES", "3")
    s = load_settings()
    assert s.search.target_count == 123 and s.min_matches == 3


def test_missing_file_gives_defaults(tmp_path: Path) -> None:
    s = load_settings(tmp_path / "nope.yaml")
    assert s.search.host == "https://bsky.social" and s.clean.seed == 42
