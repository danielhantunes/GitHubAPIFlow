"""
Fetch repository README content via GitHub API.
Uses /repos/{owner}/{repo}/readme with raw Accept to avoid base64 decoding.
"""
import logging
from urllib.parse import urlparse

import requests

from src.config import GITHUB_API_BASE_URL, GITHUB_TOKEN
from src.utils.rate_limit import process_response

logger = logging.getLogger(__name__)


def parse_owner_repo(repo_url: str) -> tuple[str, str]:
    """
    Parse owner and repo name from a GitHub repo URL.
    e.g. https://github.com/airbytehq/airbyte -> ('airbytehq', 'airbyte')
    """
    path = urlparse(repo_url).path or ""
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return "", ""


def fetch_readme(owner: str, repo: str, max_chars: int = 12000) -> str | None:
    """
    Fetch README content for a repository. Returns raw text or None if missing/error.
    Truncates to max_chars to control LLM token usage.
    """
    if not owner or not repo:
        return None
    url = f"{GITHUB_API_BASE_URL}/repos/{owner}/{repo}/readme"
    headers = {
        "Accept": "application/vnd.github.raw",
        "User-Agent": "GitHubAPIFlow-Enrichment/1.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        process_response(resp)
        resp.raise_for_status()
        text = resp.text
        if text and len(text) > max_chars:
            text = text[:max_chars] + "\n\n[... truncated for length ...]"
        return text or None
    except requests.RequestException as e:
        logger.warning("README fetch failed for %s/%s: %s", owner, repo, e)
        return None
