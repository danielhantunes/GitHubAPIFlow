"""Bronze: normalize raw API item to structured row."""
from typing import Any


def raw_item_to_row(item: dict[str, Any]) -> dict[str, Any]:
    """Map one repository item from API to normalized bronze schema."""
    owner = item.get("owner") or {}
    return {
        "repo_id": item.get("id"),
        "repo_name": item.get("name"),
        "owner": owner.get("login") if isinstance(owner, dict) else None,
        "description": item.get("description"),
        "language": item.get("language"),
        "stars": item.get("stargazers_count"),
        "forks": item.get("forks_count"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
    }
