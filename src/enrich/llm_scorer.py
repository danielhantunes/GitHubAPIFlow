"""
Score a repository README via LLM: readme quality, cloud usage, stack mentioned.
Returns structured dict for enrichment columns.
"""
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Env (loaded in run_llm_enrichment; openai not required at import time)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

MAX_SUMMARY_CHARS = 6000


SYSTEM_PROMPT = """You are a data-engineering analyst. Score the given README and respond with valid JSON only, no markdown.
Output exactly this structure:
{
  "readme_quality_score": <1-10 integer, 10=excellent docs>,
  "uses_cloud_services": "<comma-separated list or 'None'>",
  "stack_mentioned": "<comma-separated list or 'None'>",
  "summary": "<one short sentence>"
}
Consider: clarity, installation/usage instructions, cloud (AWS/GCP/Azure), tech stack (Python, Spark, dbt, etc.), data-engineering practices."""

USER_PROMPT_TEMPLATE = """Repository: {repo_name}
Language: {language}

README content:
---
{readme}
---
Respond with JSON only."""


def score_readme(
    repo_name: str,
    language: str,
    readme_text: str | None,
    model: str = "gpt-4o-mini",
) -> dict:
    """
    Call OpenAI to score the README. Returns dict with readme_quality_score, uses_cloud_services, stack_mentioned, summary.
    If no API key or error, returns defaults and empty summary.
    """
    defaults = {
        "readme_quality_score": 0,
        "uses_cloud_services": "None",
        "stack_mentioned": "None",
        "summary": "",
    }
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY not set; skipping LLM score")
        return defaults
    if not readme_text or not readme_text.strip():
        return {**defaults, "summary": "No README"}

    try:
        from openai import OpenAI

        client = OpenAI(api_key=OPENAI_API_KEY)
        user_content = USER_PROMPT_TEMPLATE.format(
            repo_name=repo_name,
            language=language or "Unknown",
            readme=readme_text.strip()[:10000],
        )
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.2,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Strip markdown code block if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        data = json.loads(raw)
        return {
            "readme_quality_score": int(data.get("readme_quality_score", 0)),
            "uses_cloud_services": str(data.get("uses_cloud_services", "None")).strip() or "None",
            "stack_mentioned": str(data.get("stack_mentioned", "None")).strip() or "None",
            "summary": str(data.get("summary", "")).strip()[:MAX_SUMMARY_CHARS],
        }
    except Exception as e:
        logger.warning("LLM score failed for %s: %s", repo_name, e)
        return {**defaults, "summary": f"Error: {e!s}"[:200]}


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
