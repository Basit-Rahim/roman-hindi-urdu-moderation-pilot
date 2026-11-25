# 7-day prevalence of Romanised Hindi–Urdu on Bluesky (counts only)

Source: Jetstream replay archive, all `app.bsky.feed.post` creates. Match = 2+ distinct keywords, whole words, any case.

## 1. Coverage

- Window (UTC): 2026-11-17T00:00:00Z to 2026-11-23T23:59:59Z (requested 2026-11-17T00:00:00Z to 2026-11-24T00:00:00Z, end exclusive)
- Posts scanned: 23,214,223
- Gaps longer than 5 minutes: none

## 2. Romanised Hindi–Urdu matches

- Matched (2+ keywords): 254 = 1.09 per 100,000 posts
- 1+ keyword: 10,220 = 44.0 per 100,000 posts

## 3. Per day

| UTC day | scanned | matched | per 100,000 |
|---|---:|---:|---:|
| 2026-11-17 | 3,448,516 | 31 | 0.90 |
| 2026-11-18 | 3,425,960 | 32 | 0.93 |
| 2026-11-19 | 3,467,799 | 33 | 0.95 |
| 2026-11-20 | 3,409,525 | 34 | 1.00 |
| 2026-11-21 | 3,103,275 | 32 | 1.03 |
| 2026-11-22 | 3,049,687 | 38 | 1.25 |
| 2026-11-23 | 3,309,461 | 54 | 1.63 |

## 4. Langs tags on the 254 matched posts

| tag group | count | % |
|---|---:|---:|
| en only | 97 | 38.2% |
| no tag | 106 | 41.7% |
| any hi/ur tag | 32 | 12.6% |
| other | 19 | 7.5% |

en only or no tag: 203 of 254 = 79.9% (95% Wilson CI 74.6%–84.4%)

## 5. Native script vs Romanised (all scanned posts)

- Devanagari-script posts: 1,532; tagged hi: 239 (15.6%)
- Arabic-script posts: 32,364; tagged ur: 23 (0.1%). Rough comparison: Arabic script also covers Arabic, Persian and other languages.
- Romanised matches: 254; tagged hi or ur: 32 (12.6%)

Script = only that script's letters in the text; posts mixing scripts are not counted in the Devanagari or Arabic rows.

## 6. 24-hour run vs 7-day run

24h live run (2026-11-17 14:15 to 2026-11-18 14:15 UTC): 0.86 per 100,000 (31 of 3,622,051); 7-day run: 1.09 per 100,000 (254 of 23,214,223).
