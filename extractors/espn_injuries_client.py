"""ESPN public API client for team injury data.

Free endpoint — no API key required. Uses the same base URL as ESPNClient.
"""

import logging
import time

import requests

from constants import ESPN_API_BASE_URL

logger = logging.getLogger(__name__)
_TIMEOUT = 30
_RATE_LIMIT_SECONDS = 0.3


class ESPNInjuriesClient:
    """Fetches team injury reports from ESPN (free, no authentication required)."""

    def __init__(self) -> None:
        self._team_id_cache: dict[tuple[str, str, str], int] = {}

    def _get(self, path: str, params: dict | None = None) -> dict:
        """HTTP GET against the ESPN API. Returns parsed JSON or empty dict on failure."""
        url = f"{ESPN_API_BASE_URL}/{path}"
        try:
            time.sleep(_RATE_LIMIT_SECONDS)
            r = requests.get(url, params=params or {}, timeout=_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            logger.warning("ESPN injuries fetch failed (%s): %s", path, exc)
            return {}

    def _get_team_id(self, team_name: str, espn_sport: str, espn_league: str) -> int | None:
        """Returns the ESPN team ID for a given display name. Results are cached per run."""
        cache_key = (espn_sport, espn_league, team_name.lower())
        if cache_key in self._team_id_cache:
            return self._team_id_cache[cache_key]

        data = self._get(f"{espn_sport}/{espn_league}/teams", {"limit": 200})
        sports = data.get("sports", [])
        teams_list: list[dict] = []
        if sports:
            leagues = sports[0].get("leagues", [])
            if leagues:
                teams_list = leagues[0].get("teams", [])

        for entry in teams_list:
            t = entry.get("team", {})
            if t.get("displayName", "").lower() == team_name.lower():
                team_id = int(t["id"])
                self._team_id_cache[cache_key] = team_id
                return team_id

        logger.debug(
            "ESPN: no team ID found for '%s' in %s/%s", team_name, espn_sport, espn_league
        )
        return None

    def fetch_team_injuries(
        self, team_name: str, sport_type: str, league_key: str
    ) -> list[dict]:
        """Returns the ESPN injury list for a team.

        Returns [] if the league is unsupported, the team is not found, or the request fails.
        """
        espn_sport, espn_league = _resolve_espn_league(sport_type, league_key)
        if not espn_league:
            return []

        team_id = self._get_team_id(team_name, espn_sport, espn_league)
        if not team_id:
            return []

        data = self._get(f"{espn_sport}/{espn_league}/teams/{team_id}/injuries")
        return data.get("injuries", [])


def _resolve_espn_league(sport_type: str, league_key: str) -> tuple[str, str]:
    """Returns (espn_sport_slug, espn_league_slug) for a given sport_type and league_key."""
    if sport_type == "football":
        from extractors.espn_soccer_client import ESPNSoccerClient
        return "soccer", ESPNSoccerClient.LEAGUE_MAP.get(league_key, "")
    if sport_type == "basketball":
        return "basketball", "nba"
    return "", ""
