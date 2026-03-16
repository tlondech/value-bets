"""
NewsAPI + rule-based sentence extraction for high-EV bet context.
Fetches recent articles per team, filters for relevance, then surfaces
the top injury/suspension sentences with no external API or LLM needed.
"""

import logging
import re
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

from constants import NEWS_DAYS_BACK_DEFAULT, NEWS_FETCH_SIZE

_NEWSAPI_URL = "https://newsapi.org/v2/everything"

# Substring keywords — "injur" matches injury/injured/injuries, "suspend" matches suspended/suspension
_KEYWORDS = {
    "miss", "misses", "missing", "out", "doubt", "doubtful",
    "suspend", "suspended", "suspension", "injur", "injury",
    "injured", "injuries", "unavailable", "ruled out", "return",
    "fitness", "knock", "absence", "absent", "sidelined",
}

# 1. Compile regex ONCE with word boundaries (\b) to prevent "out" matching "without"
_KW_PATTERN = re.compile(r'\b(?:' + '|'.join(_KEYWORDS) + r')\b', re.IGNORECASE)


def _fetch_articles(team: str, api_key: str, days_back: int) -> list[dict]:
    """Fetches recent news articles for a single team from NewsAPI."""
    from_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    params = {
        "q": f'"{team}" AND (injury OR injuries OR suspended OR suspension OR "team news")',
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": NEWS_FETCH_SIZE,
        "from": from_date,
        "apiKey": api_key,
    }
    try:
        resp = requests.get(_NEWSAPI_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("articles", [])
    except Exception as exc:
        logger.warning("team_news: NewsAPI request failed for '%s': %s", team, exc)
        return []


def _filter_relevant(articles: list[dict], team: str) -> list[dict]:
    """Keeps only articles where title or description mentions the team name."""
    team_lower = team.lower()
    return [
        a for a in articles
        if team_lower in (a.get("title") or "").lower()
        or team_lower in (a.get("description") or "").lower()
    ]


def _extract_key_sentences(articles: list[dict], team: str, opponent: str) -> str:
    """
    Scans article titles and descriptions for injury/suspension sentences using word boundaries.
    Only surfaces sentences where the team name is the subject: when both team and opponent
    names appear, the team name must come first (e.g. "Fiorentina news: Kean out vs Cremonese"
    is valid for Fiorentina but skipped for Cremonese). Deduplicates and strips truncation artifacts.
    """
    team_lower     = team.lower()
    opponent_lower = opponent.lower()
    raw_sentences: list[str] = []

    for a in articles:
        title = (a.get("title") or "").strip()
        desc = (a.get("description") or "").strip()

        # Force a period between title and desc so the splitter doesn't merge them
        text = f"{title}. {desc}"
        raw_sentences.extend(re.split(r"(?<=[.!?])\s+", text))

    scored: list[tuple[int, str]] = []

    for sentence in raw_sentences:
        sentence = sentence.strip()

        # Skip empty strings and NewsAPI truncation garbage
        if not sentence or sentence.endswith("chars]"):
            continue

        s_lower = sentence.lower()

        # Require the team name to appear in the sentence
        if team_lower not in s_lower:
            continue

        # When both names appear, only keep the sentence if the team name comes
        # first — this makes "Fiorentina news: Kean out vs Cremonese" valid for
        # Fiorentina but invalid for Cremonese (where opponent appears first).
        if opponent_lower in s_lower and s_lower.index(opponent_lower) < s_lower.index(team_lower):
            continue

        # Count actual word boundary matches
        matches = _KW_PATTERN.findall(sentence)
        if matches:
            scored.append((len(matches), sentence))

    if not scored:
        return "No notable absences reported."

    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)

    seen: set[str] = set()
    top: list[str] = []

    for _, sentence in scored:
        # 4. Normalize the string for deduplication (removes punctuation so similar sentences are caught)
        normalized = re.sub(r'[^\w\s]', '', sentence.lower())

        if normalized not in seen:
            seen.add(normalized)
            # Strip redundant "{Team} [team news]: " prefix and clean up ellipsis
            clean_sentence = re.sub(
                rf'^{re.escape(team)}\s*(?:team\s+news\s*)?[:\-]\s*',
                '', sentence, flags=re.IGNORECASE,
            ).replace("…", "").strip()
            top.append(clean_sentence)

        if len(top) == 2:
            break

    return " ".join(top)


def fetch_team_news(
    home_team: str,
    away_team: str,
    news_api_key: str,
    days_back: int = NEWS_DAYS_BACK_DEFAULT,
) -> dict | None:
    """
    Fetches and extracts pre-match team news for both teams.

    Returns {home_summary, away_summary, fetched_at} or None if news_api_key is missing.
    Non-fatal: individual failures fall back to "No notable absences reported."
    """
    if not news_api_key:
        return None

    home_articles = _filter_relevant(_fetch_articles(home_team, news_api_key, days_back), home_team)
    away_articles = _filter_relevant(_fetch_articles(away_team, news_api_key, days_back), away_team)

    return {
        "home_summary": _extract_key_sentences(home_articles, home_team, away_team),
        "away_summary": _extract_key_sentences(away_articles, away_team, home_team),
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
    }
