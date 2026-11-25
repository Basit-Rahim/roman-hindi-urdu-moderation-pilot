# Romanized Hindi–Urdu moderation pilot

Question: *Do platforms and language-ID tools mistake Romanized
Hindi–Urdu posts for English?*

 **step 1: collecting candidate posts from
Bluesky**


## Repository layout

```
config/       settings.yaml, keywords.txt, search_queries.txt

collection/   step 1 scripts: search, prevalence, clean, stats, export_public
              (jetstream.py is the live stream client and jetstream_replay.py
              the archive client used by prevalence)

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
| Prevalence from the archive (7 days, counts only) | `python -m collection.prevalence --since 2026-11-17 --until 2026-11-24` | `data/prevalence/prevalence_7d.json` |

Useful variants:

```bash
python -m collection.search --test-minutes 10   # test run into data/raw/test/, prints 5 examples
python -m collection.stats                      # stats for the raw file
python -m collection.stats --input data/raw/test/posts.jsonl
python -m collection.prevalence --hours 0.05    # 3-minute prevalence smoke test (new run)
python -m collection.prevalence --since 2026-11-20T00:00 --until 2026-11-20T00:20 --output data/prevalence/smoke.json
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
- the scripts of all scanned posts and of matched posts
- `langs` × script tables for all scanned posts and for matched posts
- keyword frequencies among matched posts

The `langs` × script table (`scanned_langs_script`) makes it possible to
compare posts tagged `hi`/`ur` written in Devanagari or Arabic script with
Romanised posts tagged `en`. Script is detected as described under
[Filtering](#filtering-commonfilterspy).

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

### Prevalence from the archive (`--since` / `--until`, counts only)

With `--since` (and optionally `--until`, default: start of today, UTC), the
prevalence step reads a past window from Jetstream v2's archive instead of
the live stream, then stops. The keyword filter and counts are the same as in
live mode, kept in totals and **per UTC day**. Output goes to
`data/prevalence/prevalence_7d.json` (`prevalence.replay_output`); the step
refuses to write over the 24-hour live file.

```bash
python -m collection.prevalence --since 2026-11-17 --until 2026-11-24
```

Times are UTC; `--until` is exclusive. It needs `JETSTREAM_API_KEY` in `.env`
(create one at <https://bsky.network/account#api-keys-section-heading>). The
key is sent only to the Jetstream host and is never printed or saved.

**Method** (`collection/jetstream_replay.py`, following the
[replay docs](https://bsky.network/docs/jetstream-replay)):

1. `listSegments` lists the archive's sealed segments with their sequence and
   time ranges. The window is mapped onto a sequence range. The archive on
   `jetstream.us-east.bsky.network` started on 2 Oct 2026 when checked.
2. `planSnapshot`, filtered to `app.bsky.feed.post` commits, says which
   segments to download whole (`getSegment`) and which blocks to fetch
   (`getBlock`). Posts are in almost every block, so most segments come
   whole; the filter saves little.
3. Each segment is streamed block by block (zstd frames, columnar layout).
   For segments that straddle the window edges, the block index in the
   segment footer is used to skip blocks outside the window.
4. Each event is checked against the window by its `witnessed_at` time
   (the live stream's `time_us`). Post creates (`kind` 1) are decoded from
   DAG-CBOR and counted. Days are UTC days of `witnessed_at`, not the
   client-set `createdAt`.
5. Undocumented `kind` 7 events are skipped and counted under
   `resync_post_records_skipped`. Sampled blocks of this kind held one
   account's older posts (dated months earlier) re-sent at once, so they are
   not new posts.

Downloads are metered by compressed bytes. On HTTP 429 the step waits for
`Retry-After`; on network errors and 5xx it backs off (1 s doubling to 60 s)
and continues from the last finished block.

**Resuming.** The last fully counted archive sequence number is saved with
the counts every 60 seconds, in the same file write. Run the same command
again to continue; `--fresh` starts over. Another run's file at the output
path is never overwritten without `--fresh`.

The archive can rewrite sealed segments after the fact. One segment shrank
by about 169 KB overnight, probably as deleted records were purged, which
shifted every later byte offset and block number. So a resumed run plans
the rest of the window again from the saved sequence number, which does not
change, and skips any event at or below it. Before reading each segment,
its header checksum is compared with the plan; on a mismatch the rest of
the window is planned again (`coverage.replans`). One consequence: posts
deleted before a segment is read are not in the archive, so they are not
counted.

**Coverage.** The `coverage` section records the first and last event time,
events in the window (by kind), post creates, updates and deletes,
`resync_post_records_skipped`, every gap between consecutive events longer
than 5 minutes (`prevalence.gap_threshold_s`) with start, end and length,
reconnects, HTTP 429 waits, resumes and bytes downloaded. `summary` holds the
headline figures, including the scripts of posts tagged `hi` and `ur`.

**Run of 17–23 Nov 2026** (`data/prevalence/prevalence_7d.json`):

- Window covered: 2026-11-17T00:00:00Z to 2026-11-23T23:59:59Z, 151
  segments, 229,897,866 events, 24.0 GB downloaded.
- Coverage gaps: none longer than 5 minutes. 88 reconnects and 34 HTTP 429
  waits, all recovered. The run was stopped and resumed twice; two segments
  were rewritten by the archive during the run and were planned again
  (`replans: 2`).
- Skipped: 2,584,377 post records in `kind` 7 resync events.

| | count |
|---|---|
| posts scanned | 23,214,223 |
| matched (2+ keywords) | 254 (1.09 per 100,000) |
| 1+ keyword | 10,220 |

Language tags of the 254 matches: missing 106, `en` only 97, includes
`hi` or `ur` 32, other 19 (mostly `vi`, 15). Scripts: latin 234, mixed 20.

Posts tagged `hi` (all 23.2M scanned): 1,350, of which 239 Devanagari only,
252 latin, 838 mixed, 21 other. Posts tagged `ur`: 215, of which 23 Arabic
script only, 65 latin, 127 mixed.

| UTC day | scanned | matched | per 100k |
|---|---|---|---|
| 2026-11-17 | 3,448,516 | 31 | 0.90 |
| 2026-11-18 | 3,425,960 | 32 | 0.93 |
| 2026-11-19 | 3,467,799 | 33 | 0.95 |
| 2026-11-20 | 3,409,525 | 34 | 1.00 |
| 2026-11-21 | 3,103,275 | 32 | 1.03 |
| 2026-11-22 | 3,049,687 | 38 | 1.25 |
| 2026-11-23 | 3,309,461 | 54 | 1.63 |

**Privacy.** Post records and account identifiers exist only in memory while
a block is processed. The block decoder steps over the DID, rkey and rev columns
using only their lengths and never returns them. Nothing is written to disk except aggregate counts,
coverage times and archive positions (segment names and sequence numbers).

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
- `.env` holds credentials (`BSKY_APP_PASSWORD`, `JETSTREAM_API_KEY`) and is
  also ignored. `.env.example` lists only variable names.
- Prevalence files (live and archive) hold aggregate counts only: no text,
  URIs, DIDs or handles.
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
