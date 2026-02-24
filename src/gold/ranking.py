"""
Gold layer: repository ranking by a composite score (popularity + recency).
Reads silver data, computes score = stars*0.6 + forks*0.3 + recency_factor*0.1, sorts, and saves top_repositories.csv.
"""
import logging
from datetime import timezone
from pathlib import Path

import pandas as pd

from src.config import CUMULATIVE_SILVER_DIR, GOLD_DIR

# Score weights: popularity (stars, forks) and recency
STARS_WEIGHT = 0.6
FORKS_WEIGHT = 0.3
RECENCY_WEIGHT = 0.1

REQUIRED_COLUMNS = ["repo_id", "repo_name", "stars", "forks", "updated_at", "language"]

logger = logging.getLogger(__name__)


def _read_silver_for_ranking(silver_path: Path) -> pd.DataFrame:
    """
    Read silver dataset (single Parquet or directory of Parquet files).
    Returns a DataFrame with at least repo_id, repo_name, stars, forks, updated_at, language.
    """
    if not silver_path.exists():
        return pd.DataFrame()

    path = silver_path / "repositories.parquet"
    if path.exists():
        df = pd.read_parquet(path)
    else:
        parquet_files = sorted(silver_path.rglob("*.parquet"))
        if not parquet_files:
            return pd.DataFrame()
        df = pd.read_parquet(parquet_files)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            logger.warning("Silver missing column %s; ranking may be incomplete.", col)
    return df


def _compute_recency_factor(updated_at: pd.Series) -> pd.Series:
    """
    recency_factor = 1 / (days_since_update + 1).
    Recently updated repos get higher value; older repos get lower. No division by zero.
    """
    today = pd.Timestamp.now(tz=timezone.utc).normalize()
    updated = pd.to_datetime(updated_at, errors="coerce", utc=True)
    days_since = (today - updated).dt.days
    days_since = days_since.fillna(365).clip(lower=0)
    return 1.0 / (days_since + 1)


def _compute_score_raw(df: pd.DataFrame) -> pd.Series:
    """
    Raw score = stars * 0.6 + forks * 0.3 + recency_factor * 0.1.
    Uses numeric coercion and fillna(0) for missing stars/forks.
    """
    stars = pd.to_numeric(df["stars"], errors="coerce").fillna(0).clip(lower=0)
    forks = pd.to_numeric(df["forks"], errors="coerce").fillna(0).clip(lower=0)
    recency = _compute_recency_factor(df["updated_at"])
    return stars * STARS_WEIGHT + forks * FORKS_WEIGHT + recency * RECENCY_WEIGHT


def _build_repo_url(owner: str, repo_name: str) -> str:
    """Build GitHub repository URL from owner and repo_name."""
    o = str(owner).strip() if pd.notna(owner) else ""
    r = str(repo_name).strip() if pd.notna(repo_name) else ""
    if not o or not r:
        return ""
    return f"https://github.com/{o}/{r}"


def _normalize_score_0_100(series: pd.Series) -> pd.Series:
    """
    Min-max normalize to 0-100. Best repo in dataset = 100, worst = 0.
    If all values are equal, return 100 for all.
    """
    lo, hi = series.min(), series.max()
    if pd.isna(lo) or pd.isna(hi) or hi <= lo:
        return pd.Series(100.0, index=series.index)
    return ((series - lo) / (hi - lo) * 100.0).round(2)


def build_ranking(silver_path: Path | None = None, output_path: Path | None = None) -> Path:
    """
    Read silver dataset, compute ranking score, sort by score descending, add ranking column, save to CSV.

    Score formula: raw = stars*0.6 + forks*0.3 + recency_factor*0.1; then score is min-max
    normalized to 0-100 (best repo = 100, worst = 0). recency_factor = 1 / (days_since_update + 1).

    Output columns: repo_id, name, repo_url, stars, forks, updated_at, language, recency_factor, score, ranking.
    score is in range 0-100.
    Saves to data/gold/top_repositories.csv by default.
    """
    silver_path = silver_path or CUMULATIVE_SILVER_DIR
    output_path = output_path or (GOLD_DIR / "top_repositories.csv")

    df = _read_silver_for_ranking(silver_path)
    if df.empty:
        logger.warning("No silver data for ranking; writing empty top_repositories.csv")
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        empty_cols = [
            "repo_id", "name", "repo_url", "stars", "forks", "updated_at", "language",
            "recency_factor", "score", "ranking",
        ]
        pd.DataFrame(columns=empty_cols).to_csv(output_path, index=False, encoding="utf-8")
        return output_path

    # Ensure required columns; use repo_name as name
    df = df.copy()
    if "repo_name" in df.columns and "name" not in df.columns:
        df["name"] = df["repo_name"]
    if "owner" in df.columns and "repo_name" in df.columns:
        df["repo_url"] = df.apply(
            lambda row: _build_repo_url(row.get("owner"), row.get("repo_name")),
            axis=1,
        )
    elif "repo_url" not in df.columns:
        df["repo_url"] = ""

    df["recency_factor"] = _compute_recency_factor(df["updated_at"])
    df["_raw_score"] = _compute_score_raw(df)
    df = df.sort_values("_raw_score", ascending=False).reset_index(drop=True)
    df["score"] = _normalize_score_0_100(df["_raw_score"])
    df = df.drop(columns=["_raw_score"])
    df["ranking"] = range(1, len(df) + 1)

    out_cols = [
        "repo_id", "name", "repo_url", "stars", "forks", "updated_at", "language",
        "recency_factor", "score", "ranking",
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    df[out_cols].to_csv(output_path, index=False, encoding="utf-8")

    logger.info("Ranking: processed %s records; saved to %s", len(df), output_path)

    top5 = df.head(5)
    for _, row in top5.iterrows():
        logger.info(
            "Top repo #%s: %s (score=%s/100, stars=%s, forks=%s)",
            int(row["ranking"]),
            row.get("name", row.get("repo_name", "?")),
            float(row["score"]),
            row.get("stars", "?"),
            row.get("forks", "?"),
        )

    return output_path
