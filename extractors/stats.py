import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"


class FootballAPIError(Exception):
    pass


class FootballAPIClient:
    def __init__(self, api_key: str, league_id: int, season: int):
        self.api_key = api_key
        self.league_id = league_id
        self.season = season

    def _get_headers(self) -> dict:
        return {"x-apisports-key": self.api_key}

    def _get(self, endpoint: str, params: dict) -> dict:
        url = f"{BASE_URL}/{endpoint}"
        response = requests.get(url, headers=self._get_headers(), params=params, timeout=15)
        if response.status_code == 401:
            raise FootballAPIError("Invalid API key for API-Football.")
        if not response.ok:
            raise FootballAPIError(
                f"API-Football returned {response.status_code}: {response.text}"
            )
        data = response.json()
        errors = data.get("errors", {})
        if errors:
            raise FootballAPIError(f"API-Football errors: {errors}")
        return data

    def fetch_team_list(self) -> list[dict]:
        """
        Returns all teams in the configured league/season.
        Each entry: {"team_id": int, "team_name": str}
        """
        data = self._get("teams", {"league": self.league_id, "season": self.season})
        teams = []
        for item in data.get("response", []):
            team = item.get("team", {})
            teams.append({"team_id": team["id"], "team_name": team["name"]})
        logger.info("Fetched %d teams for league %d season %d.", len(teams), self.league_id, self.season)
        return teams

    def fetch_fixtures(self, status: str = "FT") -> list[dict]:
        """
        Returns all finished fixtures for the configured league/season.
        Each entry: {fixture_id, fixture_date, home_team_id, away_team_id, home_goals, away_goals}
        """
        data = self._get(
            "fixtures",
            {"league": self.league_id, "season": self.season, "status": status},
        )
        fixtures = []
        for item in data.get("response", []):
            fixture = item.get("fixture", {})
            teams = item.get("teams", {})
            goals = item.get("goals", {})

            date_str = fixture.get("date", "")
            try:
                fixture_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                logger.warning("Could not parse fixture date '%s', skipping.", date_str)
                continue

            home_goals = goals.get("home")
            away_goals = goals.get("away")
            if home_goals is None or away_goals is None:
                continue

            fixtures.append({
                "fixture_id": fixture["id"],
                "fixture_date": fixture_date,
                "home_team_id": teams["home"]["id"],
                "away_team_id": teams["away"]["id"],
                "home_goals": home_goals,
                "away_goals": away_goals,
            })

        logger.info("Fetched %d finished fixtures.", len(fixtures))
        return fixtures

    def fetch_fixture_xg(self, fixture_id: int, home_team_id: int, away_team_id: int) -> dict | None:
        """
        Returns {"home_xg": float, "away_xg": float} from the fixtures/statistics endpoint.
        Returns None if xG is unavailable for this fixture.
        """
        data = self._get("fixtures/statistics", {"fixture": fixture_id})
        xg: dict[int, float] = {}
        for team_stats in data.get("response", []):
            tid = team_stats["team"]["id"]
            for stat in team_stats.get("statistics", []):
                if stat.get("type") == "Expected Goals":
                    val = stat.get("value")
                    if val not in (None, "N/A"):
                        xg[tid] = float(val)
        home = xg.get(home_team_id)
        away = xg.get(away_team_id)
        if home is None or away is None:
            return None
        return {"home_xg": home, "away_xg": away}

    def fetch_team_statistics(self, team_id: int) -> dict | None:
        """
        Returns season aggregate stats for a team.
        Returns None if no data is available.
        """
        data = self._get(
            "teams/statistics",
            {"league": self.league_id, "season": self.season, "team": team_id},
        )
        resp = data.get("response")
        if not resp:
            logger.warning("No statistics found for team %d.", team_id)
            return None

        goals_for = resp.get("goals", {}).get("for", {}).get("total", {}).get("total", 0)
        goals_against = resp.get("goals", {}).get("against", {}).get("total", {}).get("total", 0)
        played = resp.get("fixtures", {}).get("played", {}).get("total", 0)

        return {
            "team_id": team_id,
            "played": played,
            "goals_scored": goals_for,
            "goals_conceded": goals_against,
        }
