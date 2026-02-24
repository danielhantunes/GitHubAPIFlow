"""Checkpoint persistence for idempotent pagination."""
import json
import logging
from pathlib import Path
from typing import Any

from src.config import CHECKPOINT_FILE

logger = logging.getLogger(__name__)


def load_checkpoint() -> dict[str, Any]:
    """Load checkpoint from JSON file. Returns empty dict if missing or invalid."""
    if not CHECKPOINT_FILE.exists():
        return {}
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Checkpoint load failed: %s. Starting fresh.", e)
        return {}


def save_checkpoint(last_page_processed: int, run_date: str | None = None) -> None:
    """Persist last_page_processed (and optional run_date) to checkpoint file."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {"last_page_processed": last_page_processed}
    if run_date:
        data["run_date"] = run_date
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info("Checkpoint saved: last_page_processed=%s", last_page_processed)
