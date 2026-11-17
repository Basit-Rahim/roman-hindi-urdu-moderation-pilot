# Romanized Hindi–Urdu moderation pilot

Research question: *Do platforms and language-ID tools mistake Romanized
Hindi–Urdu posts for English?*

This repository currently covers **only step 1: collecting candidate posts
from Bluesky**. It contains no labelling, translation or moderation steps.

## Step 1: Data collection

### Setup

Requires Python 3.11 or newer.

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows; on macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # only needed for Source B or for overrides
pytest                          # unit tests
```

### Running

```bash
python collect.py                    # collect until TARGET_COUNT posts are stored
python collect.py --test-minutes 5   # test run: writes to data/raw/test/, prints 5 examples
python stats.py                      # summary of data/raw/posts.jsonl
python stats.py --input data/raw/test/posts.jsonl
python export_public.py              # text-free ID list -> data/public/posts_ids.csv
```

Stop with Ctrl+C at any time. The cursor is saved to
`data/raw/checkpoint.json` every 5 seconds and on exit. The next run resumes
from it. Stored posts are deduplicated by `uri`, so replayed events are safe.

### Sources

**A. Jetstream (default, no login).** The collector subscribes to
`app.bsky.feed.post` on Bluesky's public Jetstream WebSocket. Jetstream v2
(`/xrpc/network.bsky.jetstream.subscribeEvents?collections=...`) is the
default because the [docs](https://bsky.network/docs/jetstream) recommend it.
v1 (`/subscribe?wantedCollections=...`) also works. Set
`jetstream.url` in `config/settings.yaml` or `JETSTREAM_URL` in `.env`. Both
event formats are parsed.

- Post **create** events are filtered and stored.
- Post **delete** events are logged in `deleted.jsonl` when they refer to a
  post we stored. Set `record_deletes: all` to log every deletion on the
  network, which makes a very large file.
- Account events that mark an account as no longer active (deleted,
  deactivated, taken down) are also logged if we hold posts from that account.
- The checkpoint stores both `seq` (the v2 cursor) and `time_us` (the v1
  cursor). On v1, the collector resumes 5 seconds before the saved `time_us`.
- If the connection drops, the collector reconnects with exponential backoff
  (1 s, doubling to a maximum of 60 s, with jitter).

**B. Keyword search (optional, off by default).** This source runs
`app.bsky.feed.searchPosts` on bsky.social once for each query in
`config/search_queries.txt`.

1. Create an **App Password** in Bluesky under Settings → Privacy and
   security → App passwords. **Never use your main password.** The collector
   refuses any password that is not in App Password format
   (`xxxx-xxxx-xxxx-xxxx`).
2. Put `BSKY_HANDLE` and `BSKY_APP_PASSWORD` in `.env`.
3. Set `search.enabled: true` in `settings.yaml`, or `SEARCH_ENABLED=true` in
   `.env`.

There is a pause between requests (`delay_s`). On HTTP 429 or 5xx, the
collector honours `Retry-After` and `RateLimit-Reset`, or otherwise backs off
exponentially. When both sources are enabled, search runs first and Jetstream
runs after it.

### Filtering

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

### What is stored

`data/raw/posts.jsonl` holds one JSON object per line:

| field | meaning |
|---|---|
| `uri`, `did`, `rkey` | `at://did/app.bsky.feed.post/rkey`, plus its parts |
| `text` | post text |
| `langs` | raw `langs` value from the record (`null` if the field is missing) |
| `created_at` | `createdAt` from the record (set by the client, so it is not trusted) |
| `collected_at` | UTC time the collector stored the post |
| `is_reply` | whether the record has a `reply` field |
| `matched_keywords`, `match_count` | distinct keywords found |
| `script` | `latin` / `arabic` / `devanagari` / `mixed` |
| `source` | `jetstream` or `search` |

`data/raw/deleted.jsonl` holds deletion records:
`{"type": "post", "uri": ...}` or `{"type": "account", "did": ..., "status": ...}`.

`data/public/posts_ids.csv` holds only `uri, langs, script, match_count,
created_at`. It has no text and no handles, and deleted posts and accounts
are removed. `langs` values are joined with `;`, and an empty cell means no
language was declared.

### Privacy rules

- `data/raw/` contains post text and **must never be committed**. It is
  listed in `.gitignore`. Check `git status` before every commit.
- `.env` holds credentials and is also ignored. `.env.example` lists only
  variable names.
- Use only a Bluesky **App Password**, never the main account password.
- Only `export_public.py` output may be shared. It has no text, but each
  `uri` contains the author's DID, which can be resolved to their account.
  Treat the file as pseudonymous, not anonymous.
- Before any analysis or sharing, re-run `export_public.py` so deletions are
  applied. Deletions that happen while the collector is **not** running are
  not seen. A later step should re-check stored URIs against the network
  before publication.
- Example post text is shown only in the terminal during test runs. It is
  never written to public files.

### Configuration

`config/settings.yaml` holds all settings. These environment variables
override it: `TARGET_COUNT`, `MIN_MATCHES`, `JETSTREAM_URL`,
`JETSTREAM_ENABLED` and `SEARCH_ENABLED`.
