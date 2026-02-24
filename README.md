# GitHubAPIFlow

Paginated ingestion pipeline using the **GitHub REST API** to collect repositories related to data engineering, stored in a lightweight **Medallion architecture** (raw → bronze → silver → gold).

---

## Architecture

```
GitHub Search API (paginated)
        ↓
   [Extract]  ← checkpoint.json (last_page_processed)
        ↓
   [Raw]     data/raw/yyyy-mm-dd/page_X.json  (exact API JSON)
        ↓
   [Bronze]  data/bronze/yyyy-mm-dd/repositories.parquet  (normalized schema)
        ↓
   [Silver]  data/silver/yyyy-mm-dd/year=.../month=.../  (deduped, schema, partitioned)
        ↓
   [Gold]    data/gold/yyyy-mm-dd/*.parquet  (aggregations)
```

- **Raw**: Immutable copy of API responses; one JSON file per page per day.
- **Bronze**: Normalized, single-schema Parquet (repo_id, repo_name, owner, description, language, stars, forks, created_at, updated_at).
- **Silver**: Deduplicated by `repo_id`, schema enforced, `ingestion_timestamp` added, partitioned by repository `created_at` year/month.
- **Gold**: Simple aggregations: repositories by language, by stars range, by year (created_at).

---

## Pagination strategy

- **Endpoint**: `GET /search/repositories?q=...&per_page=100&page=N`
- **Parameters**: `page` (1-based) and `per_page=100` (GitHub max 100).
- **Stop condition**: Stop when the API returns **no items** for a page, fewer than `per_page` items, or when reaching **page 10** (GitHub returns at most **1000** search results; page 11 returns 422).
- **Checkpoint**: After each successful page fetch and raw write, `checkpoint.json` is updated with `last_page_processed` (and optional `run_date`). On the next run, the pipeline resumes from `last_page_processed + 1`.
- **Idempotency**: Re-running the pipeline continues from the checkpoint; already-written raw files for that page/date are overwritten if you re-fetch the same page (same run date). Bronze/Silver/Gold are overwritten for the same run date. So “run once per day” is naturally idempotent for that day; “run multiple times per day” will append new pages and then reprocess the whole day’s raw into bronze/silver/gold.

---

## Checkpoint logic

- **File**: `checkpoint.json` in the project root.
- **Fields**: `last_page_processed` (int), optionally `run_date` (str, yyyy-mm-dd).
- **Load**: At startup, load checkpoint; first page to fetch = `last_page_processed + 1`.
- **Save**: After each page is written to raw, save `last_page_processed = page` (and current run date if desired).
- **Reset**: Delete `checkpoint.json` to start pagination from page 1 again.

---

## Rate limits and retries

- **Rate limits**: GitHub allows 10 requests/min unauthenticated, 5,000/hour with a personal access token. The client respects `403` and `X-RateLimit-Remaining` / `X-RateLimit-Reset` and backs off when rate limited.
- **Retry**: Up to 5 attempts per request with **exponential backoff** (2s, 4s, 8s, …). On 403 with `X-RateLimit-Reset`, the client may wait until the reset time before retrying.

---

## Medallion design

| Layer   | Format  | Content |
|--------|--------|---------|
| Raw    | JSON   | Exact API response per page (`items` + `total_count`) |
| Bronze | Parquet| One table: normalized repo fields (no partitioning) |
| Silver | Parquet| Deduped by `repo_id`, schema + `ingestion_timestamp`, partitioned by `year`/`month` of `created_at` |
| Gold   | Parquet| Aggregation tables: by language, by stars range, by year |

---

## Cumulative silver and gold

Each pipeline run also updates **cumulative** layers so data accumulates until you stop:

- **Cumulative silver**: `data/silver/cumulative/repositories.parquet` — each run merges that day’s bronze into this file; duplicates are deduplicated by `repo_id` (latest `updated_at` kept).
- **Cumulative gold**: `data/gold/cumulative/` — same outputs as daily gold (repos_by_*.parquet, repositories.csv, profile.json), built from cumulative silver.

Daily outputs (e.g. `data/gold/2026-02-24/`) are still written; cumulative is updated in addition. To count repos in cumulative gold: `python count_gold_repos.py cumulative`.

---

## Project structure

```
GitHubAPIFlow/
├── .env.example
├── config/
│   └── search_queries.yaml  # Search query, per_page, sort, order (edit to add more repos)
├── checkpoint.json          # Created at runtime
├── data/
│   ├── raw/yyyy-mm-dd/      # page_1.json, page_2.json, ...
│   ├── bronze/yyyy-mm-dd/   # repositories.parquet
│   ├── silver/yyyy-mm-dd/   # year=.../month=.../
│   └── gold/yyyy-mm-dd/     # repos_by_*.parquet
├── requirements.txt
├── run_pipeline.py          # Entry point
├── README.md
└── src/
    ├── config.py            # Env vars, paths, loads search from YAML
    ├── logging_config.py
    ├── extract/
    │   ├── github_client.py # API + pagination + retry
    │   └── checkpoint.py
    ├── raw/
    │   └── writer.py
    ├── bronze/
    │   ├── transform.py
    │   └── writer.py
    ├── silver/
    │   ├── schema.py
    │   └── writer.py
    └── gold/
        └── writer.py
```

---

## Setup and run

1. **Clone and install**
   ```bash
   cd GitHubAPIFlow
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   pip install -r requirements.txt
   ```

2. **Optional: GitHub token** (recommended for higher rate limits)
   - Copy `.env.example` to `.env`.
   - Set `GITHUB_TOKEN` to a personal access token (no scope needed for public search).

3. **Run pipeline**
   ```bash
   python run_pipeline.py
   ```
   Logs go to console and `pipeline.log`.

4. **Reset and re-run from page 1**
   - Delete `checkpoint.json`, then run again.

---

## Search query

The pipeline loads the search from **`config/search_queries.yaml`**. Edit that file to change what repositories are ingested (no code changes):

- **search_query**: GitHub search string (max 5 OR/AND/NOT operators to avoid 422).
- **per_page**, **max_search_pages**: Pagination (default 100 and 10 = 1000 results).
- **sort**, **order**: e.g. `stars` / `desc` (see [GitHub API](https://docs.github.com/en/rest/search/search#search-repositories)).

If the YAML file is missing or invalid, defaults in code are used.
