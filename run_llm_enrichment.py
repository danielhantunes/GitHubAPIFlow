"""
LLM enrichment pipeline: score top repositories from gold using README + OpenAI.
Only enriches repos that do not yet have llm_scored_at (idempotent, resumable).

Usage:
  python run_llm_enrichment.py --limit 10
  python run_llm_enrichment.py --limit 50 --model gpt-4o-mini
"""
import argparse
import sys

from dotenv import load_dotenv

load_dotenv()

from src.enrich.runner import run_enrichment
from src.logging_config import configure_logging


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Enrich gold top repositories with LLM scores (README quality, cloud, stack).")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max number of unscored repos to enrich this run (default: 10)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o-mini",
        help="OpenAI model for scoring (default: gpt-4o-mini)",
    )
    args = parser.parse_args()
    if args.limit < 1:
        print("--limit must be >= 1", file=sys.stderr)
        return 1
    n = run_enrichment(limit=args.limit, model=args.model)
    print(f"Enriched {n} repositories.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
