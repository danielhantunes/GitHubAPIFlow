"""GitHub Search API client with pagination, rate-limit handling, and retry."""
import logging
import time
from urllib.parse import urlencode

import requests

from src.config import (
    GITHUB_API_BASE_URL,
    GITHUB_TOKEN,
    INITIAL_BACKOFF_SECONDS,
    MAX_RETRIES,
    PER_PAGE,
    SEARCH_ORDER,
    SEARCH_QUERY,
    SEARCH_SORT,
)
from src.utils.rate_limit import process_response

logger = logging.getLogger(__name__)


def _build_search_url(page: int) -> str:
    """Build search/repositories URL with query and pagination."""
    params = {
        "q": SEARCH_QUERY,
        "per_page": PER_PAGE,
        "page": page,
        "sort": SEARCH_SORT,
        "order": SEARCH_ORDER,
    }
    return f"{GITHUB_API_BASE_URL}/search/repositories?{urlencode(params)}"


def _get_headers() -> dict[str, str]:
    """Request headers; include token if set for higher rate limit."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "GitHubAPIFlow-DataIngestion/1.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def fetch_repositories_page(page: int) -> tuple[list[dict], int | None]:
    """
    Fetch one page of repository search results.

    Returns:
        (items, total_count_or_none). total_count is from API response;
        None if not available. items is empty list on error or no results.
    """
    url = _build_search_url(page)
    headers = _get_headers()
    total_count: int | None = None
    backoff = INITIAL_BACKOFF_SECONDS

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            process_response(resp)

            if resp.status_code == 403:
                reset_at = resp.headers.get("X-RateLimit-Reset")
                if reset_at:
                    wait = max(60, int(reset_at) - int(time.time()))
                    logger.warning("Rate limited. Waiting %s seconds.", wait)
                    time.sleep(wait)
                    backoff = INITIAL_BACKOFF_SECONDS
                    continue
                # No reset header: exponential backoff
                logger.warning("Rate limited (403). Backoff %s s (attempt %s).", backoff, attempt + 1)
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 422:
                # GitHub search only returns first 1000 results; page 11+ is invalid
                logger.info(
                    "Search limit reached (422). GitHub returns at most 1000 results; stopping at page %s.",
                    page,
                )
                return ([], None)

            resp.raise_for_status()
            data = resp.json()
            items = data.get("items") or []
            if "total_count" in data:
                total_count = data["total_count"]
            return (items, total_count)

        except requests.RequestException as e:
            logger.warning("Request failed (attempt %s/%s): %s", attempt + 1, MAX_RETRIES, e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
            else:
                logger.error("Max retries exceeded.")
                return ([], None)

    return ([], None)
