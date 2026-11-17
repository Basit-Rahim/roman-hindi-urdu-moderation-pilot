# Romanized Hindi–Urdu moderation pilot

Question: *Do platforms and language-ID tools mistake Romanized
Hindi–Urdu posts for English?*

 **step 1: collecting candidate posts from
Bluesky**


## Repository layout

```
config/       settings.yaml, keywords.txt, search_queries.txt

collection/   step 1 scripts: search, prevalence, clean, stats, export_public
              (jetstream.py is the stream client used by prevalence)

common/       shared code: config loading, keyword/script/link filters,
              file and time helpers, Bluesky constants

labelling/    (empty)

analysis/     (empty)

tests/        unit tests (pytest)

data/         outputs; data/raw/ is git-ignored

```

Every script runs as a module from the repository root, e.g.
`python -m collection.search`, and supports `--help`.

## Setup

Requires Python 3.11 or newer.

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows; on macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # then add BSKY_HANDLE and an App Password
pytest                          # unit tests
```

## Pipeline

Run these in order from the repository root:

| # | Step | Command | Output |
|---|---|---|---|
| 1 | Collect posts by keyword search | `python -m collection.search` | `data/raw/posts.jsonl` |
| 2 | Clean (dedupe texts, cap per account) | `python -m collection.clean` | `data/raw/posts_clean.jsonl` |
| 3 | Summary statistics | `python -m collection.stats --clean` | terminal |
| 4 | Text-free public ID list | `python -m collection.export_public` | `data/public/posts_ids.csv` |

The baseline runs separately, alongside the pipeline:

| Step | Command | Output |
|---|---|---|
| Prevalence on the live stream (24 h, counts only) | `python -m collection.prevalence` | `data/prevalence/prevalence.json` |

Useful variants:

```bash
python -m collection.search --test-minutes 10   # test run into data/raw/test/, prints 5 examples
python -m collection.stats                      # stats for the raw file
python -m collection.stats --input data/raw/test/posts.jsonl
python -m collection.prevalence --hours 0.05    # 3-minute prevalence smoke test (new run)
```

## Step 1: Data collection

### Keyword search (`collection.search`)

The search step runs `app.bsky.feed.searchPosts` (`sort=latest`) on
bsky.social once for each query in `config/search_queries.txt`. Each query is
a pair of keywords, such as `nahi hai`. Each query is paginated fully until
the API stops returning a cursor (`search.max_pages: 0`). Posts that pass the
keyword filter are appended to `data/raw/posts.jsonl`. Posts are deduplicated
by `uri`, so re-runs only add new posts. The run stops at
`search.target_count`. At the end, a per-query summary lists pages, posts
found, posts that passed the filter, and new posts kept.

Bluesky search matches the words of a query in any order, so `nahi pata`
returns the same posts as `pata nahi`. Wrap a query in double quotes to
search for an exact phrase.

1. Create an **App Password** in Bluesky under Settings → Privacy and
   security → App passwords. **Never use your main password.** The collector
   refuses any password that is not in App Password format
   (`xxxx-xxxx-xxxx-xxxx`).
2. Put `BSKY_HANDLE` and `BSKY_APP_PASSWORD` in `.env`.

There is a pause between requests (`search.delay_s`). On HTTP 429 or 5xx,
the collector honours `Retry-After` and `RateLimit-Reset`, or otherwise
backs off exponentially.

### Moderation labels

Search requests send `atproto-accept-labelers: did:plc:ar7c4by46qjdydhdevvrndac`
(Bluesky's moderation service; set `search.accept_labelers`). Testing showed
that the AppView applies this labeler **by default** even without the header:
the response header `atproto-content-labelers` names it, and the labels
returned are the same. The `labels` array mixes several sources, which
`collection.stats` reports separately:

- **self-labels**: `src` is the author's DID, e.g. `porn` or `graphic-media`
- **Bluesky moderation**: `src` is `did:plc:ar7c4by46qjdydhdevvrndac`
- **any other labeler** named in the header

Posts that Bluesky has taken down never appear in search results, so they
cannot be counted here.

### Filtering (`common/filters.py`)

- Keywords are read from `config/keywords.txt`, one per line. The file is
  editable, and `#` starts a comment.
- Matching is **whole-word** and **case-insensitive**. Letters and digits
  count as word characters. Everything else is a boundary, including
  punctuation, `#`, `_`, `-` and emoji. For example, `hai` matches `Hai!` and
  `#hai`, but not `chai` or `haircut`.
- A post is kept only if it contains at least `min_matches` **distinct**
  keywords (default 2). Writing `hai hai hai` counts as one keyword. The rule
  exists because single words such as *hai* also appear in Italian and
  Indonesian.
- `script` is set from the letters in the text:
  - `latin`, `arabic` or `devanagari` when only one of these scripts is present
  - `mixed` when two or more are present
  - `other` when none is present, which cannot happen for kept posts

  All scripts are kept. `latin` is the main target.
- `has_link` is true if the post has a link facet, an external embed, or
  URL-like text.

### Cleaning (`collection.clean`)

The cleaning step writes `data/raw/posts_clean.jsonl` and never changes
`posts.jsonl`. It applies these steps in order:

1. Drop exact duplicate texts after case-folding and collapsing whitespace.
   The first copy is kept.
2. Cap each account at `clean.max_per_account` posts (default 5). The posts
   kept are a random pick with a fixed `clean.seed` (default 42), so the
   result is reproducible.
3. Make sure every row has `has_link`.

### Prevalence mode (`collection.prevalence`, counts only)

The prevalence step reads Bluesky's public Jetstream live stream for 24 hours
(`prevalence.hours`) and applies the same keyword filter. It saves **only
aggregate counts** to `data/prevalence/prevalence.json`, in totals and per
UTC hour:

- posts scanned
- posts matched (2 or more distinct keywords)
- posts with 1 or more keywords
- the `langs` values of all scanned posts and of matched posts
- the scripts of matched posts
- keyword frequencies among matched posts

No text, URIs, DIDs or handles are saved. The file is rewritten every 60
seconds (`prevalence.save_interval_s`).

Jetstream v2 is used by default, as the
[docs](https://bsky.network/docs/jetstream) recommend. v1 also works; set
`prevalence.jetstream_url`. On a dropped connection, the stream reconnects
with exponential backoff (1 s, doubling to 60 s, with jitter).

**Resuming.** If the process stops, run the same command again from the
repository root:

```bash
python -m collection.prevalence
```

It finds the unfinished run in `data/prevalence/prevalence.json`, continues
from the saved cursor, and still ends at the original end time. Any
`--hours` value is ignored when resuming. Use `--fresh` to discard the saved
run and start over.

To keep it running after the terminal closes on Windows, start it detached:

```powershell
Start-Process .venv\Scripts\python.exe -ArgumentList "-u","-m","collection.prevalence" -WindowStyle Hidden -RedirectStandardOutput data\prevalence\prevalence.log -RedirectStandardError data\prevalence\prevalence.err.log
```

The machine must stay awake for the whole run. Gaps longer than Jetstream's
replay window are lost; `connections` and `resumes` in the file show whether
this happened.

### What is stored

`data/raw/posts.jsonl` holds one JSON object per line:

| field | meaning |
|---|---|
| `uri`, `did`, `rkey` | `at://did/app.bsky.feed.post/rkey`, plus its parts |
| `text` | post text |
| `langs` | raw `langs` value from the record (`null` if the field is missing) |
| `created_at` | `createdAt` from the record (set by the client, so it is not trusted) |
| `indexed_at` | `indexedAt` from the AppView post view |
| `collected_at` | UTC time the collector stored the post |
| `is_reply` | whether the record has a `reply` field |
| `matched_keywords`, `match_count` | distinct keywords found |
| `script` | `latin` / `arabic` / `devanagari` / `mixed` |
| `has_link` | link facet, external embed, or URL-like text |
| `labels` | the post's `labels` array from the API |
| `author_labels` | the author's account-level `labels` array |
| `source` | `search` |

`data/public/posts_ids.csv` holds only `uri, langs, script, match_count,
created_at`. It has no text and no handles. `langs` values are joined with
`;`, and an empty cell means no language was declared. If
`data/raw/deleted.jsonl` exists, the posts and accounts it lists are removed.
It holds lines like `{"type": "post", "uri": ...}` or
`{"type": "account", "did": ...}`.

### Privacy rules

- `data/raw/` contains post text and **must never be committed**. It is
  listed in `.gitignore`. Check `git status` before every commit.
- `.env` holds credentials and is also ignored. `.env.example` lists only
  variable names.
- Use only a Bluesky **App Password**, never the main account password.
- Only `collection.export_public` output may be shared. It has no text, but
  each `uri` contains the author's DID, which can be resolved to their
  account. Treat the file as pseudonymous, not anonymous.
- Deleted posts are not tracked automatically. The export removes only the
  posts and accounts listed in `data/raw/deleted.jsonl`.
- Example post text is shown only in the terminal during test runs. It is
  never written to public files.

### Configuration

`config/settings.yaml` holds all settings, in sections `search`,
`prevalence` and `clean`, plus the shared `min_matches` and `keywords_file`.
Two environment variables override it: `TARGET_COUNT`
(`search.target_count`) and `MIN_MATCHES`.
