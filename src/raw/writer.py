"""Raw layer: persist API response JSON as-is."""
import json
import logging
from datetime import date
from pathlib import Path

from src.config import RAW_DIR

logger = logging.getLogger(__name__)


def get_raw_dir_for_date(run_date: date | None = None) -> Path:
    """Return raw directory for date: data/raw/yyyy-mm-dd."""
    d = run_date or date.today()
    path = RAW_DIR / d.isoformat()
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_raw_page(page: int, payload: dict, run_date: date | None = None) -> Path:
    """
    Save raw API response for one page.
    Writes to data/raw/yyyy-mm-dd/page_X.json.
    Returns path to written file.
    """
    raw_dir = get_raw_dir_for_date(run_date)
    path = raw_dir / f"page_{page}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Raw written: %s", path)
    return path
