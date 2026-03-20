"""ESPN injury enrichment for high-EV signal context.

Fetches structured injury data from ESPN's free public API — no API key required.
Replaces the previous NewsAPI-based implementation.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _format_espn_injuries(injuries: list[dict]) -> str:
    """Formats up to 3 ESPN injury objects into a readable summary string."""
    if not injuries:
        return "No notable absences reported."
    parts = []
    for inj in injuries[:3]:
        name = inj.get("athlete", {}).get("displayName", "Unknown")
        status = inj.get("status", "Unknown")
        injury_type = inj.get("type", {}).get("description", "")
        label = f"{name} ({status}{': ' + injury_type if injury_type else ''})"
        parts.append(label)
    return "; ".join(parts)


def _fetch_espn_injuries(
    home_team: str,
    away_team: str,
    sport_type: str,
    league_key: str,
) -> dict | None:
    """Fetches ESPN injury data for both teams. Returns None on any failure."""
    try:
        from extractors.espn_injuries_client import ESPNInjuriesClient
        client = ESPNInjuriesClient()
        home_injuries = client.fetch_team_injuries(home_team, sport_type, league_key)
        away_injuries = client.fetch_team_injuries(away_team, sport_type, league_key)
        return {
            "home_summary": _format_espn_injuries(home_injuries),
            "away_summary": _format_espn_injuries(away_injuries),
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "source":       "espn",
        }
    except Exception as exc:
        logger.warning("team_news: ESPN injury fetch failed: %s", exc)
        return None


def fetch_team_news(
    home_team: str,
    away_team: str,
    sport_type: str = "football",
    league_key: str = "epl",
) -> dict | None:
    """
    Fetches injury context for both teams via ESPN's public injury API.

    Returns {home_summary, away_summary, fetched_at, source} or None on failure.
    Non-fatal.
    """
    return _fetch_espn_injuries(home_team, away_team, sport_type, league_key)
