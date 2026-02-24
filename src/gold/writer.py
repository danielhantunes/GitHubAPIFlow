"""Gold layer: aggregations — by language, stars range, year."""
import logging
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import CUMULATIVE_GOLD_DIR, CUMULATIVE_SILVER_DIR, GOLD_DIR, SILVER_DIR
from src.profiling import profile_gold

logger = logging.getLogger(__name__)


def count_gold_repositories(run_date: str | date | None = None, cumulative: bool = False) -> int:
    """
    Return the total number of repositories stored in the gold layer.
    If cumulative=True or run_date='cumulative', uses cumulative gold; else uses run_date folder.
    """
    if cumulative or (isinstance(run_date, str) and run_date == "cumulative"):
        path = CUMULATIVE_GOLD_DIR / "repos_by_language.parquet"
    else:
        d = run_date if isinstance(run_date, str) else (run_date or date.today()).isoformat()
        path = GOLD_DIR / d / "repos_by_language.parquet"
    if not path.exists():
        return 0
    df = pd.read_parquet(path)
    if df.empty or "repo_count" not in df.columns:
        return 0
    return int(df["repo_count"].sum())


def _read_silver_partitions(silver_base: Path) -> pd.DataFrame:
    """Read all partition directories under silver_base into one DataFrame.
    Only reads .parquet files so profile.json and other non-parquet files are ignored.
    """
    if not silver_base.exists():
        return pd.DataFrame()
    parquet_files = sorted(silver_base.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()
    return pd.read_parquet(parquet_files)


def _stars_range(stars: int) -> str:
    """Bucket stars into ranges."""
    if pd.isna(stars) or stars < 0:
        return "unknown"
    if stars < 10:
        return "0-9"
    if stars < 100:
        return "10-99"
    if stars < 1000:
        return "100-999"
    if stars < 10000:
        return "1000-9999"
    return "10000+"


def _build_repo_url(owner: str, repo_name: str) -> str:
    """Build GitHub repository URL from owner and repo_name."""
    o = str(owner).strip() if pd.notna(owner) else ""
    r = str(repo_name).strip() if pd.notna(repo_name) else ""
    if not o or not r:
        return ""
    return f"https://github.com/{o}/{r}"


def silver_to_gold(run_date: str | None = None) -> Path:
    """
    Read silver (for run_date or latest), build aggregations, write gold Parquet files.
    Outputs:
      - repos_by_language.parquet
      - repos_by_stars_range.parquet
      - repos_by_year.parquet
      - repositories.csv (repository list with links)
    """
    from datetime import date
    d = date.fromisoformat(run_date) if run_date else date.today()
    date_str = d.isoformat()
    silver_path = SILVER_DIR / date_str
    df = _read_silver_partitions(silver_path)
    if df.empty:
        logger.warning("No silver data for %s; writing empty gold.", date_str)
    out_dir = GOLD_DIR / date_str
    by_lang, by_stars, by_year = _build_gold_from_silver_df(df, out_dir, f"Gold {date_str}")
    profile_gold(by_lang, by_stars, by_year, date_str, out_dir)
    return out_dir


def _build_gold_from_silver_df(
    df: pd.DataFrame, out_dir: Path, run_label: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build gold Parquet files and CSV from a silver DataFrame. Returns (by_lang, by_stars, by_year)."""
    if df.empty:
        df = pd.DataFrame(columns=[
            "repo_id", "repo_name", "owner", "description", "language",
            "stars", "forks", "created_at", "updated_at", "watermark_hash", "ingestion_timestamp",
        ])
    else:
        df = df.drop_duplicates(subset=["repo_id"], keep="first")

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_cols = [
        "repo_url", "repo_id", "repo_name", "owner", "description",
        "language", "stars", "forks", "created_at", "updated_at", "watermark_hash",
    ]
    if not df.empty and "owner" in df.columns and "repo_name" in df.columns:
        repos_df = df.copy()
        repos_df["repo_url"] = repos_df.apply(
            lambda row: _build_repo_url(row.get("owner"), row.get("repo_name")),
            axis=1,
        )
        cols = [c for c in csv_cols if c in repos_df.columns]
        repos_df[cols].to_csv(out_dir / "repositories.csv", index=False, encoding="utf-8")
        logger.info("%s: repositories.csv (%s rows)", run_label, len(repos_df))
    else:
        pd.DataFrame(columns=csv_cols).to_csv(out_dir / "repositories.csv", index=False, encoding="utf-8")

    by_lang = (
        df.groupby("language", dropna=False)
        .agg(repo_count=("repo_id", "count"))
        .reset_index()
    )
    by_lang.to_parquet(out_dir / "repos_by_language.parquet", index=False)
    stars_series = pd.to_numeric(df["stars"], errors="coerce").fillna(-1).astype("int64")
    df_stars = df.assign(stars_range=stars_series.map(_stars_range))
    by_stars = (
        df_stars.groupby("stars_range")
        .agg(repo_count=("repo_id", "count"))
        .reset_index()
    )
    by_stars.to_parquet(out_dir / "repos_by_stars_range.parquet", index=False)
    created = pd.to_datetime(df["created_at"], errors="coerce")
    df_year = df.assign(created_year=created.dt.year.fillna(0).astype("int32"))
    by_year = (
        df_year.groupby("created_year")
        .agg(repo_count=("repo_id", "count"))
        .reset_index()
    )
    by_year.to_parquet(out_dir / "repos_by_year.parquet", index=False)
    logger.info("%s: gold written (%s repos)", run_label, len(df))
    return by_lang, by_stars, by_year


def build_cumulative_gold() -> Path:
    """
    Read cumulative silver and build cumulative gold (aggregations + repositories.csv).
    Call after merge_bronze_into_cumulative_silver.
    """
    cumulative_path = CUMULATIVE_SILVER_DIR / "repositories.parquet"
    if not cumulative_path.exists():
        CUMULATIVE_GOLD_DIR.mkdir(parents=True, exist_ok=True)
        empty = pd.DataFrame(columns=[
            "repo_id", "repo_name", "owner", "description", "language",
            "stars", "forks", "created_at", "updated_at", "watermark_hash", "ingestion_timestamp",
        ])
        by_lang, by_stars, by_year = _build_gold_from_silver_df(
            empty, CUMULATIVE_GOLD_DIR, "Cumulative gold"
        )
        profile_gold(by_lang, by_stars, by_year, "cumulative", CUMULATIVE_GOLD_DIR)
        return CUMULATIVE_GOLD_DIR

    df = pd.read_parquet(cumulative_path)
    by_lang, by_stars, by_year = _build_gold_from_silver_df(
        df, CUMULATIVE_GOLD_DIR, "Cumulative gold"
    )
    profile_gold(by_lang, by_stars, by_year, "cumulative", CUMULATIVE_GOLD_DIR)
    return CUMULATIVE_GOLD_DIR
