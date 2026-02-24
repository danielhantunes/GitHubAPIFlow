"""Silver schema: column types and required columns."""
import pandas as pd

SILVER_COLUMNS = [
    "repo_id",
    "repo_name",
    "owner",
    "description",
    "language",
    "stars",
    "forks",
    "created_at",
    "updated_at",
    "ingestion_timestamp",
]

# Coerce types for schema enforcement
DTYPE_MAP = {
    "repo_id": "Int64",
    "repo_name": "string",
    "owner": "string",
    "description": "string",
    "language": "string",
    "stars": "Int64",
    "forks": "Int64",
    "created_at": "string",
    "updated_at": "string",
    "ingestion_timestamp": "string",
}


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Select and cast columns to silver schema."""
    for col in SILVER_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[SILVER_COLUMNS].copy()
    for col, dtype in DTYPE_MAP.items():
        df[col] = df[col].astype(dtype)
    return df
