# GitHubAPIFlow

Paginated ingestion pipeline using the **GitHub REST API** to collect repositories related to data engineering, stored in a lightweight **Medallion architecture** (raw → bronze → silver → gold).

> 🚧 **This project is currently in progress and actively evolving.** The current state already reflects production-oriented design decisions.

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
   [Gold]    data/gold/yyyy-mm-dd/  (aggregations, repositories.csv, profile.json) + data/gold/top_repositories.csv (ranking)
```

- **Raw**: Immutable copy of API responses; one JSON file per page per day.
- **Bronze**: Normalized, single-schema Parquet (repo_id, repo_name, owner, description, language, stars, forks, created_at, updated_at).
- **Silver**: Deduplicated by `repo_id` (latest `updated_at` kept), schema enforced, `watermark_hash` (row-version key) and `ingestion_timestamp` added, partitioned by repository `created_at` year/month.
- **Gold**: Aggregations (by language, stars range, year), `repositories.csv` with links, `top_repositories.csv` ranked by score (0–100), and profiling (`profile.json`).

---

## Pagination strategy

- **Endpoint**: `GET /search/repositories?q=...&per_page=100&page=N`
- **Parameters**: `page` (1-based) and `per_page=100` (GitHub max 100).
- **Stop condition**: Stop when the API returns **no items** for a page, fewer than `per_page` items, or when reaching **page 10** (GitHub returns at most **1000** search results; page 11 returns 422).
- **Checkpoint**: After each successful page fetch and raw write, `checkpoint.json` is updated with `last_page_processed` (and optional `run_date`). On the next run, the pipeline resumes from `last_page_processed + 1`.
- **Idempotency**: Re-running the pipeline continues from the checkpoint; already-written raw files for that page/date are overwritten if you re-fetch the same page (same run date). Bronze/Silver/Gold are overwritten for the same run date. So “run once per day” is naturally idempotent for that day; “run multiple times per day” will append new pages and then reprocess the whole day’s raw into bronze/silver/gold.

---

- **Pipeline order**: Extract (paginated) → Raw → Bronze → Silver → Daily gold → Merge into cumulative silver → Cumulative gold → Ranking (`top_repositories.csv`). If a run fails mid-way, the next run resumes from the last checkpoint; partial outputs already written are left as-is.

---

## Checkpoint logic

- **File**: `checkpoint.json` in the project root.
- **Fields**: `last_page_processed` (int), optionally `run_date` (str, yyyy-mm-dd).
- **Load**: At startup, load checkpoint; first page to fetch = `last_page_processed + 1`.
- **Save**: After each page is written to raw, save `last_page_processed = page` (and current run date if desired).
- **Reset**: Delete `checkpoint.json` to start pagination from page 1 again.

---

## Rate limits and retries

- **Rate limits**: **Search API** — 10 requests/min unauthenticated, 30/min with token. **Core API** — 60/hour unauthenticated, 5,000/hour with token. The client respects `403` and `X-RateLimit-Remaining` / `X-RateLimit-Reset` and backs off when rate limited.
- **Retry**: Up to 5 attempts per request with **exponential backoff** (2s, 4s, 8s, …). On 403 with `X-RateLimit-Reset`, the client may wait until the reset time before retrying.

---

## Rate limit monitoring

The pipeline includes production-style **rate limit monitoring** so you can observe usage and avoid unnecessary 403s.

**GitHub API limits**

- **Search**: 10 requests/min unauthenticated, 30/min with token.
- **Core**: 60 requests/hour unauthenticated, 5,000/hour with token.

Each response includes `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` (Unix timestamp). Exceeding the limit returns `403` and blocks until the reset time.

**Monitoring approach**

- After **every** API request, the client calls `src.utils.rate_limit` to:
  1. **Log** limit, remaining, and reset time (human-readable UTC) at INFO.
  2. **Append** a JSON line to `logs/rate_limit_metrics.jsonl` (timestamp, limit, remaining, reset_time) for later analysis.
  3. **Optionally pause**: if `remaining < 5`, the process sleeps until the reset time, then continues.

**Auto-pause strategy**

- When `X-RateLimit-Remaining` drops below 5, the pipeline logs a warning and sleeps until `X-RateLimit-Reset` before making the next request. That avoids hitting 403 and keeps runs predictable. The existing 403 retry (wait then retry) still applies if a limit is hit despite this.

**Why this matters in production**

- **Observability**: Structured logs and a JSONL metrics file give a clear history of rate limit usage per run.
- **Stability**: Proactive pause reduces 403s and avoids backoff storms when running many pages or multiple jobs.
- **Capacity planning**: You can inspect `logs/rate_limit_metrics.jsonl` to see how close you get to the limit and tune batch size or schedule.

The `logs/` directory is created automatically on first use and is listed in `.gitignore`.

---

## Medallion design

| Layer   | Format  | Content |
|--------|--------|---------|
| Raw    | JSON   | Exact API response per page (`items` + `total_count`) |
| Bronze | Parquet| One table: normalized repo fields (no partitioning) |
| Silver | Parquet| Deduped by `repo_id`, schema + `watermark_hash` + `ingestion_timestamp`, partitioned by `year`/`month` of `created_at` |
| Gold   | Parquet| Aggregation tables: by language, by stars range, by year |

---

## Cumulative silver and gold

Each pipeline run also updates **cumulative** layers so data accumulates until you stop:

- **Cumulative silver**: `data/silver/cumulative/repositories.parquet` — each run merges that day’s bronze into this file; duplicates are deduplicated by `repo_id` (latest `updated_at` kept).
- **Cumulative gold**: `data/gold/cumulative/` — same outputs as daily gold (repos_by_*.parquet, repositories.csv, profile.json), built from cumulative silver.

Daily outputs (e.g. `data/gold/2026-02-24/`) are still written; cumulative is updated in addition.

---

## Data profiling

Each run writes **profile.json** next to the layer output:

- **Bronze**: `data/bronze/yyyy-mm-dd/profile.json` — row count, null counts, numeric stats (min/max/mean), value counts for language/owner (top N).
- **Silver**: `data/silver/yyyy-mm-dd/profile.json` — same structure, on deduplicated silver.
- **Gold**: `data/gold/yyyy-mm-dd/profile.json` and `data/gold/cumulative/profile.json` — row counts and repo_count sum/min/max per aggregation table.

Use these for quick data-quality checks and column distributions without loading Parquet.

---

## Gold layer – Repository ranking

The gold layer produces **`data/gold/top_repositories.csv`**: a ranked list of repositories by a composite **score** that combines popularity and recency. This simulates a business relevance metric for prioritization and analytics.

**Why the score was created**

- Raw star/fork counts alone do not reflect how *current* a repo is. A repo with many stars but no updates in years may be less relevant than a recently maintained one.
- A single score (popularity + recency) supports sorting, filtering, and “top N” use cases without duplicating logic.

**How the formula works**

- **Raw score** = `stars × 0.6` + `forks × 0.3` + `recency_factor × 0.1`
- **score** is then **min-max normalized to 0–100**: the best repo in the dataset gets 100, the worst gets 0 (same relative order as raw).
- **recency_factor** = `1 / (days_since_update + 1)`:
  - Recently updated repos get a value close to 1.
  - Older repos get a smaller value (e.g. 1 year ≈ 1/366).
  - The `+ 1` avoids division by zero and caps the factor at 1.

Weights (0.6, 0.3, 0.1) emphasize stars, then forks, then recency. They can be tuned in `src/gold/ranking.py`.

**Why recency matters in analytics**

- Stale repos may have outdated docs, unfixed security issues, or incompatible dependencies.
- Recency helps distinguish actively maintained projects from abandoned ones, improving recommendation and discovery.

**Output**

- Rows are sorted by **score** descending with a **ranking** column (1-based).
- Columns: `repo_id`, `name`, `repo_url`, `stars`, `forks`, `updated_at`, `language`, `recency_factor`, `score`, `ranking`, plus enrichment columns (empty until `run_llm_enrichment.py` runs): `readme_quality_score`, `uses_cloud_services`, `stack_mentioned`, `llm_summary`, `llm_scored_at`.
- Built from cumulative silver in `src/gold/ranking.py`; run after each pipeline execution.

---

## LLM enrichment pipeline (optional)

A **second pipeline** enriches `top_repositories.csv` with LLM-derived scores: README quality, cloud usage (AWS/GCP/Azure), and stack mentioned. It is separate from the main ingestion; run it after gold ranking.

**Flow (single file)**

1. The main pipeline writes **`data/gold/top_repositories.csv`** with enrichment columns present but empty.
2. **`run_llm_enrichment.py`** reads that same file, selects up to **`--limit`** repos without `llm_scored_at` (by ranking order).
3. For each: fetch README via GitHub API, call OpenAI to score, then set the enrichment columns and `llm_scored_at`.
4. Writes back to **`data/gold/top_repositories.csv`** (enrichment columns filled in place for processed rows).

**Usage**

```bash
python run_llm_enrichment.py --limit 10
python run_llm_enrichment.py --limit 50 --model gpt-4o-mini
```

**Environment**

- **`OPENAI_API_KEY`** in `.env` (see `.env.example`). If unset, the script runs but skips LLM calls (default scores only).
- GitHub token in `.env` is used for README fetches (same rate limits as main pipeline).

**Enriched columns**

- `readme_quality_score` (1–10)
- `uses_cloud_services` (e.g. "AWS, GCP" or "None")
- `stack_mentioned` (e.g. "Python, Spark, dbt")
- `llm_summary` (LLM-generated summary; max 6000 characters)
- `llm_scored_at` (ISO timestamp)

**Idempotency and cost**

- Only repos without `llm_scored_at` are processed; re-runs enrich the next N unscored repos.
- Rough cost (OpenAI): on the order of ~$0.30–1 for ~1000 repos with a small model; use `--limit` to control how many are scored per run.

---

## Final Gold Schema

Gold layer outputs and their schemas (column names and purpose):

| Output | Format | Columns |
|--------|--------|--------|
| **repositories.csv** | CSV | `repo_url`, `repo_id`, `repo_name`, `owner`, `description`, `language`, `stars`, `forks`, `created_at`, `updated_at`, `watermark_hash` |
| **repos_by_language.parquet** | Parquet | `language`, `repo_count` |
| **repos_by_stars_range.parquet** | Parquet | `stars_range`, `repo_count` |
| **repos_by_year.parquet** | Parquet | `created_year`, `repo_count` |
| **top_repositories.csv** | CSV | `repo_id`, `name`, `repo_url`, `stars`, `forks`, `updated_at`, `language`, `recency_factor`, `score`, `ranking`, plus enrichment (empty until run): `readme_quality_score`, `uses_cloud_services`, `stack_mentioned`, `llm_summary`, `llm_scored_at` |

**Locations**

- **Daily:** `data/gold/yyyy-mm-dd/` — repositories.csv, repos_by_*.parquet, profile.json.
- **Cumulative:** `data/gold/cumulative/` — same files, built from cumulative silver.
- **Ranking:** `data/gold/top_repositories.csv` — single file at gold root; built from cumulative silver (score 0–100). Includes empty enrichment columns; `run_llm_enrichment.py` fills them in place.

---

## Silver schema and watermark hash

Silver tables include a **`watermark_hash`** column: a SHA-256 hex hash of `repo_id` + `updated_at`. It uniquely identifies each (repo, version) row and is used for:

- **Row-version identity** — Same hash ⇒ same logical snapshot of a repo; different `updated_at` ⇒ different hash.
- **CDC / idempotent merge** — Downstream systems can use it as a merge key or to detect “already seen” rows.
- **Auditing** — Compare hashes across runs or tables to see if a row changed.

Computed in the silver layer (see `src/silver/schema.py` and `src/silver/writer.py`); also present in gold `repositories.csv` when built from silver.

---

## Project structure

```
GitHubAPIFlow/
├── .env.example
├── config/
│   └── search_queries.yaml  # Search query, per_page, sort, order (edit to add more repos)
├── checkpoint.json          # Created at runtime
├── logs/
│   └── rate_limit_metrics.jsonl  # Created at runtime (rate limit metrics)
├── data/
│   ├── raw/yyyy-mm-dd/      # page_1.json, page_2.json, ...
│   ├── bronze/yyyy-mm-dd/   # repositories.parquet
│   ├── silver/yyyy-mm-dd/   # year=.../month=.../
│   └── gold/yyyy-mm-dd/     # repos_by_*.parquet; top_repositories.csv at data/gold/
├── requirements.txt
├── run_pipeline.py          # Entry point (ingestion + medallion + ranking)
├── run_llm_enrichment.py    # Optional: LLM enrichment of top_repositories
├── count_gold_repos.py      # Count repos (today or cumulative)
├── README.md
└── src/
    ├── config.py            # Env vars, paths, loads search from YAML
    ├── logging_config.py
    ├── enrich/              # README fetch + LLM scoring (optional pipeline)
    │   ├── readme_fetcher.py
    │   ├── llm_scorer.py
    │   └── runner.py
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
    ├── gold/
    │   ├── ranking.py   # Score, recency_factor, top_repositories.csv
    │   └── writer.py
    └── utils/
        └── rate_limit.py   # Rate limit logging, metrics, auto-pause
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

4. **Useful commands**
   - Count repos in gold: `python count_gold_repos.py` (today) or `python count_gold_repos.py cumulative`
   - Enrich top repos with LLM: `python run_llm_enrichment.py --limit 10` (requires `OPENAI_API_KEY` in `.env`)
   - Reset and re-run from page 1: delete `checkpoint.json`, then run the pipeline again.

---

## Search query

The pipeline loads the search from **`config/search_queries.yaml`**. Edit that file to change what repositories are ingested (no code changes):

- **search_query**: GitHub search string (max 5 OR/AND/NOT operators to avoid 422).
- **per_page**, **max_search_pages**: Pagination (default 100 and 10 = 1000 results).
- **sort**, **order**: e.g. `stars` / `desc` (see [GitHub API](https://docs.github.com/en/rest/search/search#search-repositories)).

If the YAML file is missing or invalid, defaults in code are used.
