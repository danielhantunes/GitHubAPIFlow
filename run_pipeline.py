"""
Main entry point: paginated GitHub ingestion and medallion pipeline (raw → bronze → silver → gold).
Idempotent: resumes from checkpoint; stops when API returns no items.
"""
import sys
from pathlib import Path
from datetime import date

# Ensure project root is on path when run from any cwd
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import MAX_SEARCH_PAGES, PER_PAGE, PROJECT_ROOT
from src.logging_config import setup_logging
from src.extract import fetch_repositories_page, load_checkpoint, save_checkpoint
from src.raw import write_raw_page
from src.bronze import raw_to_bronze
from src.silver import bronze_to_silver, merge_bronze_into_cumulative_silver
from src.gold import build_cumulative_gold, build_ranking, silver_to_gold

import logging
logger = logging.getLogger(__name__)


def run_ingestion(run_date: date | None = None) -> None:
    """Run full pipeline: extract (with pagination + checkpoint) → raw → bronze → silver → gold."""
    run_date = run_date or date.today()
    run_date_str = run_date.isoformat()

    # --- Extract + Raw (paginated, checkpointed) ---
    checkpoint = load_checkpoint()
    last_page = checkpoint.get("last_page_processed", 0)
    start_page = last_page + 1
    logger.info("Starting/resuming from page %s (checkpoint last_page=%s)", start_page, last_page)

    page = start_page
    while page <= MAX_SEARCH_PAGES:
        items, total_count = fetch_repositories_page(page)
        if not items:
            logger.info("No items for page %s; stopping pagination.", page)
            break
        payload = {"items": items, "total_count": total_count}
        write_raw_page(page, payload, run_date=run_date)
        save_checkpoint(page, run_date_str)
        if len(items) < PER_PAGE:
            logger.info("Last page (%s) had %s items; stopping.", page, len(items))
            break
        page += 1
    else:
        logger.info("Reached max search pages (%s); stopping.", MAX_SEARCH_PAGES)

    # --- Medallion: bronze → silver → gold (daily) ---
    raw_to_bronze(run_date=run_date)
    bronze_to_silver(run_date=run_date)
    silver_to_gold(run_date=run_date_str)

    # --- Cumulative: merge into cumulative silver, then rebuild cumulative gold ---
    merge_bronze_into_cumulative_silver(run_date=run_date)
    build_cumulative_gold()
    build_ranking()


def main() -> None:
    setup_logging()
    run_ingestion()


if __name__ == "__main__":
    main()
