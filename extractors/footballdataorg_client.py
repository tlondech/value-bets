import json
import logging
import time
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class FootballDataOrgError(Exception):
    pass


class FootballDataOrgClient:
    """
    Fetches finished fixtures from the football-data.org REST API.
    Requires a free API key (X-Auth-Token header).
    No xG available — features.py falls back to actual goals gracefully.
    """

    BASE_URL = "https://api.football-data.org/v4"
    # Class-level rate limiter: free tier allows 10 req/min → 1 req per 6s
    _last_request_time: float = 0.0
    _min_interval: float = 6.5

    def _throttle(self) -> None:
        elapsed = time.monotonic() - FootballDataOrgClient._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        FootballDataOrgClient._last_request_time = time.monotonic()

    def __init__(self, competition: str, season: int, api_key: str):
        self.competition = competition  # e.g. "CL"
        self.season = season            # start year, e.g. 2024
        self.api_key = api_key

    def fetch_fixtures(self) -> list[dict]:
        """
        Returns all finished fixtures for the configured competition/season.
        Each entry: {fixture_id, fixture_date, home_team, away_team,
                     home_goals, away_goals, home_xg, away_xg}
        """
        url = (
            f"{self.BASE_URL}/competitions/{self.competition}/matches"
            f"?status=FINISHED&season={self.season}"
        )
        req = urllib.request.Request(url, headers={"X-Auth-Token": self.api_key})
        self._throttle()
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read())
        except Exception as e:
            raise FootballDataOrgError(f"Failed to fetch {url}: {e}") from e

        fixtures = []
        for m in data.get("matches", []):
            score = m.get("score", {}).get("fullTime", {})
            home_goals = score.get("home")
            away_goals = score.get("away")
            if home_goals is None or away_goals is None:
                continue
            try:
                dt = datetime.fromisoformat(m["utcDate"].replace("Z", "+00:00"))
            except Exception:
                continue
            home = m["homeTeam"]["name"]
            away = m["awayTeam"]["name"]
            date_str = dt.strftime("%Y-%m-%d")
            fid = f"{self.competition}_{self.season}_{home}_{away}_{date_str}"
            fixtures.append({
                "fixture_id":   fid,
                "fixture_date": dt,
                "home_team":    home,
                "away_team":    away,
                "home_goals":   int(home_goals),
                "away_goals":   int(away_goals),
                "home_xg":      None,
                "away_xg":      None,
            })

        logger.debug(
            "football-data.org: fetched %d finished fixtures for %s %s.",
            len(fixtures), self.competition, self.season,
        )
        return fixtures

    def fetch_team_list(self) -> list[str]:
        """Returns sorted unique team names in this competition/season."""
        fixtures = self.fetch_fixtures()
        names: set[str] = set()
        for f in fixtures:
            names.add(f["home_team"])
            names.add(f["away_team"])
        return sorted(names)

    _STAGE_LABELS: dict[str, str] = {
        "REGULAR_SEASON": "",        # use matchweek
        "LEAGUE_PHASE":   "",        # use matchweek
        "GROUP_STAGE":    "Group Stage",
        "ROUND_OF_16":    "Round of 16",
        "LAST_16":        "Round of 16",
        "QUARTER_FINALS": "Quarter-finals",
        "SEMI_FINALS":    "Semi-finals",
        "FINAL":          "Final",
        "3RD_PLACE":      "3rd Place",
    }

    def fetch_stage_map(
        self, name_map: dict, league_key: str
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        """
        Returns (stage_map, crest_map, raw_stage_map) for SCHEDULED/TIMED matches in this competition/season.
        stage_map:     {"{home_canonical}|{away_canonical}": stage_label}
        crest_map:     {canonical_name: crest_url}
        raw_stage_map: {"{home_canonical}|{away_canonical}": raw_stage_key}  (e.g. "ROUND_OF_16")
        Non-fatal — returns ({}, {}, {}) on any error.
        """
        from models.features import resolve_team_name

        url = (
            f"{self.BASE_URL}/competitions/{self.competition}/matches"
            f"?status=SCHEDULED,TIMED&season={self.season}"
        )
        req = urllib.request.Request(url, headers={"X-Auth-Token": self.api_key})
        self._throttle()
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read())
        except Exception as e:
            logger.warning("fetch_stage_map: failed to fetch %s: %s", url, e)
            return {}, {}, {}

        result: dict[str, str] = {}
        crest_map: dict[str, str] = {}
        raw_stage_map: dict[str, str] = {}
        for m in data.get("matches", []):
            raw_stage = m.get("stage", "")
            matchweek = m.get("matchday")
            home_team = m.get("homeTeam") or {}
            away_team = m.get("awayTeam") or {}

            # Skip TBD knock-out matches where team names aren't decided yet
            home_name = home_team.get("name") or ""
            away_name = away_team.get("name") or ""
            if not home_name or not away_name:
                continue

            # Try full name first, then shortName as fallback
            home_c = (
                resolve_team_name(home_name, name_map, league_key)
                or resolve_team_name(home_team.get("shortName") or "", name_map, league_key)
            )
            away_c = (
                resolve_team_name(away_name, name_map, league_key)
                or resolve_team_name(away_team.get("shortName") or "", name_map, league_key)
            )
            if home_c is None or away_c is None:
                continue

            # Collect crest URLs as a byproduct (no extra API call)
            if home_c and (crest := home_team.get("crest")):
                crest_map[home_c] = crest
            if away_c and (crest := away_team.get("crest")):
                crest_map[away_c] = crest

            label = self._STAGE_LABELS.get(raw_stage)
            if label is None:
                # Unknown stage — pretty-print it
                label = raw_stage.replace("_", " ").title()
            if not label and matchweek:
                label = f"Matchweek {matchweek}"
            elif not label:
                continue

            result[f"{home_c}|{away_c}"] = label
            raw_stage_map[f"{home_c}|{away_c}"] = raw_stage

        logger.debug(
            "fetch_stage_map: %d stage entries, %d crests for %s %s.",
            len(result), len(crest_map), self.competition, self.season,
        )
        return result, crest_map, raw_stage_map
