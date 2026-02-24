"""
Enrichment runner: load gold top repos, select unscored up to limit, fetch README + LLM score, save enriched CSV.
Idempotent and resumable; only repos without llm_scored_at are processed.
"""
import logging
from pathlib import Path

import pandas as pd

from src.config import GOLD_TOP_REPOS_ENRICHED_PATH, GOLD_TOP_REPOS_PATH
from src.enrich.llm_scorer import score_readme, utc_now_iso
from src.enrich.readme_fetcher import fetch_readme, parse_owner_repo

logger = logging.getLogger(__name__)

LLM_COLUMNS = [
    "readme_quality_score",
    "uses_cloud_services",
    "stack_mentioned",
    "llm_summary",
    "llm_scored_at",
]


def _ensure_llm_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in LLM_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col == "llm_scored_at" else pd.NA
    return df


def load_base_and_enriched() -> pd.DataFrame:
    """
    Load base top_repositories and merge with existing enriched data if present.
    Result has all base columns plus LLM columns (empty for never-scored repos).
    """
    if not GOLD_TOP_REPOS_PATH.exists():
        logger.warning("Base gold file not found: %s", GOLD_TOP_REPOS_PATH)
        return pd.DataFrame()

    base = pd.read_csv(GOLD_TOP_REPOS_PATH)
    base = _ensure_llm_columns(base.copy())

    if GOLD_TOP_REPOS_ENRICHED_PATH.exists():
        try:
            enriched = pd.read_csv(GOLD_TOP_REPOS_ENRICHED_PATH)
            # Keep only repo_id + LLM columns from enriched to avoid duplicating base columns
            enrich_cols = [c for c in LLM_COLUMNS if c in enriched.columns]
            if enrich_cols:
                subset = enriched[["repo_id"] + enrich_cols].drop_duplicates(subset=["repo_id"], keep="last")
                base = base.drop(columns=[c for c in LLM_COLUMNS if c in base.columns], errors="ignore")
                base = base.merge(subset, on="repo_id", how="left")
        except Exception as e:
            logger.warning("Could not load enriched file, starting fresh: %s", e)
    base = _ensure_llm_columns(base)
    return base


def is_scored(row: pd.Series) -> bool:
    val = row.get("llm_scored_at")
    if pd.isna(val):
        return False
    return str(val).strip() != ""


def run_enrichment(limit: int = 10, model: str = "gpt-4o-mini") -> int:
    """
    Enrich up to `limit` repos that do not yet have llm_scored_at. Returns number enriched.
    """
    df = load_base_and_enriched()
    if df.empty:
        return 0

    unscored = df[~df.apply(is_scored, axis=1)].sort_values("ranking").head(limit)
    if unscored.empty:
        logger.info("No unscored repos remaining.")
        return 0

    logger.info("Enriching %s repos (limit=%s).", len(unscored), limit)
    enriched_count = 0
    for _, row in unscored.iterrows():
        repo_id = row["repo_id"]
        name = row.get("name", "")
        repo_url = row.get("repo_url", "")
        language = row.get("language", "")
        owner, repo = parse_owner_repo(str(repo_url))
        readme = fetch_readme(owner, repo)
        result = score_readme(name, language, readme, model=model)
        result["llm_scored_at"] = utc_now_iso()
        for col in LLM_COLUMNS:
            df.loc[df["repo_id"] == repo_id, col] = result.get(col, "")
        enriched_count += 1
        logger.info("Enriched repo_id=%s %s (quality=%s).", repo_id, name, result.get("readme_quality_score"))

    if enriched_count > 0:
        GOLD_TOP_REPOS_ENRICHED_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(GOLD_TOP_REPOS_ENRICHED_PATH, index=False)
        logger.info("Saved %s to %s.", GOLD_TOP_REPOS_ENRICHED_PATH.name, GOLD_TOP_REPOS_ENRICHED_PATH.parent)

    return enriched_count
