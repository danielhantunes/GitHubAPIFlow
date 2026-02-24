"""Silver layer: dedupe by repo_id, enforce schema, add watermark_hash and ingestion_timestamp, partition by year/month."""
import hashlib
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.config import BRONZE_DIR, CUMULATIVE_SILVER_DIR, SILVER_DIR
from src.silver.schema import enforce_schema
from src.profiling import profile_silver

logger = logging.getLogger(__name__)


def _compute_watermark_hash(repo_id: int | str, updated_at: str) -> str:
    """Deterministic hash of repo_id + updated_at for row-version identity (CDC, dedup keys)."""
    raw = f"{repo_id}_{updated_at or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _add_watermark_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Add watermark_hash column from repo_id and updated_at."""
    if df.empty:
        df["watermark_hash"] = pd.Series(dtype="string")
        return df
    df = df.copy()
    df["watermark_hash"] = df.apply(
        lambda row: _compute_watermark_hash(row.get("repo_id"), row.get("updated_at")),
        axis=1,
    )
    return df


def bronze_to_silver(run_date: date | None = None) -> Path:
    """
    Read bronze Parquet for run_date, dedupe by repo_id, enforce schema,
    add ingestion_timestamp, write Parquet partitioned by year/month (of created_at).
    """
    d = run_date or date.today()
    date_str = d.isoformat()
    bronze_path = BRONZE_DIR / date_str / "repositories.parquet"
    if not bronze_path.exists():
        logger.warning("Bronze file not found: %s", bronze_path)
        out_dir = SILVER_DIR / date_str
        out_dir.mkdir(parents=True, exist_ok=True)
        from src.silver.schema import SILVER_COLUMNS
        empty = pd.DataFrame(columns=SILVER_COLUMNS + ["year", "month"])
        (out_dir / "year=0" / "month=0").mkdir(parents=True, exist_ok=True)
        empty.to_parquet(out_dir / "year=0" / "month=0" / "data.parquet", index=False)
        return out_dir

    df = pd.read_parquet(bronze_path)
    # Deduplicate by repo_id, keeping the row with latest updated_at
    df["_updated_at_parsed"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df = df.sort_values("_updated_at_parsed", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["repo_id"], keep="first")
    df = df.drop(columns=["_updated_at_parsed"], errors="ignore")
    df["ingestion_timestamp"] = datetime.utcnow().isoformat() + "Z"
    df = _add_watermark_hash(df)
    df = enforce_schema(df)

    # Partition by year/month from created_at
    created = pd.to_datetime(df["created_at"], errors="coerce")
    df["year"] = created.dt.year.fillna(0).astype("int32")
    df["month"] = created.dt.month.fillna(0).astype("int32")
    out_dir = SILVER_DIR / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir, index=False, partition_cols=["year", "month"])
    logger.info("Silver written: %s (%s rows)", out_dir, len(df))
    # Profile before dropping partition cols for stats (use same df, cols still present)
    profile_silver(df, date_str, out_dir)
    return out_dir


def merge_bronze_into_cumulative_silver(run_date: date | None = None) -> Path:
    """
    Merge this run's bronze into cumulative silver (dedupe by repo_id, keep latest updated_at).
    Cumulative silver is a single Parquet file; each run adds/updates repos.
    """
    d = run_date or date.today()
    date_str = d.isoformat()
    bronze_path = BRONZE_DIR / date_str / "repositories.parquet"
    if not bronze_path.exists():
        logger.warning("Bronze file not found for %s; skipping cumulative silver merge.", date_str)
        return CUMULATIVE_SILVER_DIR

    new_df = pd.read_parquet(bronze_path)
    if new_df.empty:
        logger.info("Bronze empty for %s; cumulative silver unchanged.", date_str)
        return CUMULATIVE_SILVER_DIR

    cumulative_path = CUMULATIVE_SILVER_DIR / "repositories.parquet"
    if cumulative_path.exists():
        existing = pd.read_parquet(cumulative_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined["_updated_at_parsed"] = pd.to_datetime(combined["updated_at"], errors="coerce")
    combined = combined.sort_values("_updated_at_parsed", ascending=False, na_position="last")
    combined = combined.drop_duplicates(subset=["repo_id"], keep="first")
    combined = combined.drop(columns=["_updated_at_parsed"], errors="ignore")
    combined["ingestion_timestamp"] = datetime.utcnow().isoformat() + "Z"
    combined = _add_watermark_hash(combined)
    combined = enforce_schema(combined)

    CUMULATIVE_SILVER_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(cumulative_path, index=False)
    logger.info("Cumulative silver updated: %s (%s repos)", cumulative_path, len(combined))
    return CUMULATIVE_SILVER_DIR
