"""Bronze layer: convert raw JSON directory to single Parquet file."""
import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd

from src.config import BRONZE_DIR, RAW_DIR
from src.bronze.transform import raw_item_to_row
from src.profiling import profile_bronze

logger = logging.getLogger(__name__)


def raw_to_bronze(run_date: date | None = None) -> Path:
    """
    Read all raw page_*.json for the run date, normalize to bronze schema, write Parquet.
    Output: data/bronze/yyyy-mm-dd/repositories.parquet
    """
    d = run_date or date.today()
    date_str = d.isoformat()
    raw_path = RAW_DIR / date_str
    out_dir = BRONZE_DIR / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "repositories.parquet"

    rows: list[dict] = []
    for path in sorted(raw_path.glob("page_*.json")):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items") or []
        for item in items:
            rows.append(raw_item_to_row(item))

    if not rows:
        logger.warning("No raw items for %s; writing empty Parquet.", date_str)
    df = pd.DataFrame(rows)
    df.to_parquet(out_file, index=False)
    logger.info("Bronze written: %s (%s rows)", out_file, len(df))
    profile_bronze(df, date_str, out_dir)
    return out_file
