"""Environment and pipeline configuration."""
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

# Paths (relative to project root)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
SEARCH_QUERIES_YAML = CONFIG_DIR / "search_queries.yaml"

# GitHub API
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_API_BASE_URL = os.getenv("GITHUB_API_BASE_URL", "https://api.github.com").rstrip("/")

# Search config: load from YAML with fallbacks
_default_query = (
    '"data pipeline" OR "data engineering" OR "etl pipeline" '
    'OR "medallion architecture" language:python'
)
_search_config: dict = {}
if SEARCH_QUERIES_YAML.exists():
    try:
        with open(SEARCH_QUERIES_YAML, encoding="utf-8") as f:
            _search_config = yaml.safe_load(f) or {}
    except (yaml.YAMLError, OSError):
        pass

SEARCH_QUERY = _search_config.get("search_query", _default_query).strip()
PER_PAGE = int(_search_config.get("per_page", 100))
MAX_SEARCH_PAGES = int(_search_config.get("max_search_pages", 10))
SEARCH_SORT = _search_config.get("sort", "stars")
SEARCH_ORDER = _search_config.get("order", "desc")
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
# Cumulative layers: merge each run into one dataset until pipeline is stopped
CUMULATIVE_SILVER_DIR = DATA_DIR / "silver" / "cumulative"
CUMULATIVE_GOLD_DIR = DATA_DIR / "gold" / "cumulative"
CHECKPOINT_FILE = PROJECT_ROOT / "checkpoint.json"

# LLM enrichment: single file (pipeline writes empty enrichment cols; run_llm_enrichment fills in place)
GOLD_TOP_REPOS_PATH = GOLD_DIR / "top_repositories.csv"

# Retry
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
