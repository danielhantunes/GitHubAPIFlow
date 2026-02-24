"""Data profiling: row counts, nulls, numeric stats, and value counts for medallion layers."""
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Top N values to include for categorical columns
PROFILE_TOP_N = 10


def profile_dataframe(
    df: pd.DataFrame,
    layer_name: str,
    categorical_columns: list[str] | None = None,
    top_n: int = PROFILE_TOP_N,
) -> dict[str, Any]:
    """
    Build a data profile for a single DataFrame.

    Returns a dict with: row_count, column_null_counts, numeric_stats (min/max/mean),
    and value_counts for specified categorical columns (top N).
    """
    if df.empty:
        return {
            "layer": layer_name,
            "row_count": 0,
            "column_null_counts": {},
            "numeric_stats": {},
            "value_counts": {},
        }

    profile: dict[str, Any] = {
        "layer": layer_name,
        "row_count": int(len(df)),
        "column_null_counts": df.isna().sum().astype(int).to_dict(),
        "numeric_stats": {},
        "value_counts": {},
    }

    # Numeric columns: min, max, mean
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    for col in numeric_cols:
        s = df[col].dropna()
        if len(s) > 0:
            profile["numeric_stats"][col] = {
                "min": float(s.min()),
                "max": float(s.max()),
                "mean": float(s.mean()),
            }

    # Categorical / object columns: value_counts (top N)
    cats = categorical_columns or []
    for col in cats:
        if col not in df.columns:
            continue
        vc = df[col].fillna("__null__").value_counts().head(top_n)
        profile["value_counts"][col] = vc.astype(int).to_dict()

    return profile


def write_profile(profile: dict[str, Any], out_path: Path) -> None:
    """Write profile dict to JSON file (with serializable values)."""
    def _serialize(obj: Any) -> Any:
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        if isinstance(obj, dict):
            return {k: _serialize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_serialize(x) for x in obj]
        return str(obj)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(_serialize(profile), f, indent=2)
    logger.info("Profile written: %s", out_path)


def profile_bronze(df: pd.DataFrame, run_date: str, out_dir: Path) -> Path:
    """Profile bronze repositories table; write profile.json to out_dir."""
    categorical = ["language", "owner"]
    profile = profile_dataframe(df, "bronze", categorical_columns=categorical)
    profile["run_date"] = run_date
    out_path = out_dir / "profile.json"
    write_profile(profile, out_path)
    return out_path


def profile_silver(df: pd.DataFrame, run_date: str, out_dir: Path) -> Path:
    """Profile silver repositories table (deduped); write profile.json to out_dir."""
    categorical = ["language", "owner"]
    profile = profile_dataframe(df, "silver", categorical_columns=categorical)
    profile["run_date"] = run_date
    out_path = out_dir / "profile.json"
    write_profile(profile, out_path)
    return out_path


def profile_gold(
    by_language: pd.DataFrame,
    by_stars_range: pd.DataFrame,
    by_year: pd.DataFrame,
    run_date: str,
    out_dir: Path,
) -> Path:
    """Profile gold aggregation tables; write profile.json to out_dir."""
    profile: dict[str, Any] = {
        "layer": "gold",
        "run_date": run_date,
        "tables": {},
    }
    for name, df in [
        ("repos_by_language", by_language),
        ("repos_by_stars_range", by_stars_range),
        ("repos_by_year", by_year),
    ]:
        profile["tables"][name] = {
            "row_count": int(len(df)),
            "columns": df.columns.tolist(),
        }
        if not df.empty and "repo_count" in df.columns:
            profile["tables"][name]["repo_count_sum"] = int(df["repo_count"].sum())
            profile["tables"][name]["repo_count_min"] = int(df["repo_count"].min())
            profile["tables"][name]["repo_count_max"] = int(df["repo_count"].max())
    out_path = out_dir / "profile.json"
    write_profile(profile, out_path)
    return out_path
