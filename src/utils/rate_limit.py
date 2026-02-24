"""
GitHub API rate limit monitoring: structured logging, auto-pause when near exhaustion, metrics persistence.
"""
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import PROJECT_ROOT

LOGS_DIR = PROJECT_ROOT / "logs"
RATE_LIMIT_METRICS_FILE = LOGS_DIR / "rate_limit_metrics.jsonl"

# Threshold below which we pause until reset
REMAINING_THRESHOLD = 5

logger = logging.getLogger(__name__)


def log_rate_limit(response: Any) -> tuple[int | None, int | None, str | None]:
    """
    Read X-RateLimit-* headers from the GitHub API response, log structured info, return (limit, remaining, reset_time).
    reset_time is ISO format string (UTC); None if header missing.
    """
    limit_h = response.headers.get("X-RateLimit-Limit")
    remaining_h = response.headers.get("X-RateLimit-Remaining")
    reset_h = response.headers.get("X-RateLimit-Reset")

    limit = int(limit_h) if limit_h is not None and str(limit_h).strip() != "" else None
    remaining = int(remaining_h) if remaining_h is not None and str(remaining_h).strip() != "" else None
    reset_ts = int(reset_h) if reset_h is not None and str(reset_h).strip() != "" else None

    reset_time_str: str | None = None
    if reset_ts is not None:
        try:
            dt = datetime.fromtimestamp(reset_ts, tz=timezone.utc)
            reset_time_str = dt.isoformat()
        except (ValueError, OSError):
            reset_time_str = None

    if limit is not None and remaining is not None:
        logger.info(
            "Rate limit: limit=%s remaining=%s reset=%s",
            limit,
            remaining,
            reset_time_str or reset_ts,
        )
    elif limit is None and remaining is None:
        logger.debug("Rate limit headers not present in response")

    return (limit, remaining, reset_time_str)


def handle_rate_limit(remaining: int | None, reset_timestamp: int | None) -> None:
    """
    If remaining < REMAINING_THRESHOLD, sleep until reset time to avoid 403.
    Logs a warning before sleeping.
    """
    if remaining is None or reset_timestamp is None:
        return
    if remaining >= REMAINING_THRESHOLD:
        return

    now = int(time.time())
    sleep_seconds = max(0, reset_timestamp - now)
    if sleep_seconds <= 0:
        return

    reset_iso = datetime.fromtimestamp(reset_timestamp, tz=timezone.utc).isoformat()
    logger.warning(
        "Rate limit low (remaining=%s). Pausing %s s until reset at %s.",
        remaining,
        sleep_seconds,
        reset_iso,
    )
    time.sleep(sleep_seconds)


def save_rate_limit_metrics(
    limit: int | None,
    remaining: int | None,
    reset_time: str | None,
) -> None:
    """
    Append one JSON line to logs/rate_limit_metrics.jsonl.
    Creates logs directory if it does not exist.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "limit": limit,
        "remaining": remaining,
        "reset_time": reset_time,
    }
    with open(RATE_LIMIT_METRICS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def process_response(response: Any) -> None:
    """
    Run full monitoring after an API request: log, persist metrics, and pause if near limit.
    Call this after every GitHub API request.
    """
    limit, remaining, reset_time = log_rate_limit(response)
    save_rate_limit_metrics(limit, remaining, reset_time)

    reset_ts: int | None = None
    if response.headers.get("X-RateLimit-Reset"):
        try:
            reset_ts = int(response.headers.get("X-RateLimit-Reset"))
        except (TypeError, ValueError):
            pass
    handle_rate_limit(remaining, reset_ts)
