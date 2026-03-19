"""ESPN public API client for football (soccer) fixture data.

Parses completed events into the standard fixture schema used by the pipeline.

# Check here to add more leagues:
# https://github.com/pseudo-r/Public-ESPN-API/blob/main/docs/sports/soccer.md#leagues--competitions
"""

import logging
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone

from extractors.base import MatchData
from extractors.espn_client import ESPNClient

logger = logging.getLogger(__name__)


class ESPNSoccerClient(ESPNClient):
    SPORT = "soccer"

    LEAGUE_MAP: dict[str, str] = {
        "epl":        "eng.1",
        "laliga":     "esp.1",
        "bundesliga": "ger.1",
        "seriea":     "ita.1",
        "ligue1":     "fra.1",
        "ucl":        "uefa.champions",
    }

    def fetch_recent_results(self, days_back: int = 7) -> list[MatchData]:
        """Returns completed fixtures from the last N days as MatchData."""
        today = datetime.now(timezone.utc).date()
        return [_fixture_to_match_data(f) for f in self.fetch_fixtures(today - timedelta(days=days_back), today)]

    def fetch_upcoming_matches(self, days_ahead: int = 7) -> list[MatchData]:
        """Returns scheduled (not yet completed) fixtures from ESPN."""
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=days_ahead)
        league_keys = list(self.LEAGUE_MAP.keys())
        matches: list[MatchData] = []
        seen: set[str] = set()

        for league_key in league_keys:
            espn_slug = self.LEAGUE_MAP[league_key]
            events = self.fetch_scoreboard(self.SPORT, espn_slug, today, end)
            for event in events:
                competitions = event.get("competitions") or []
                if not competitions:
                    continue
                comp = competitions[0]
                if comp.get("status", {}).get("type", {}).get("completed"):
                    continue  # only upcoming
                competitors = comp.get("competitors") or []
                home_c = next((c for c in competitors if c.get("homeAway") == "home"), None)
                away_c = next((c for c in competitors if c.get("homeAway") == "away"), None)
                if not home_c or not away_c:
                    continue
                home_team = (home_c.get("team") or {}).get("displayName")
                away_team = (away_c.get("team") or {}).get("displayName")
                if not home_team or not away_team:
                    continue
                raw_date = event.get("date", "")
                try:
                    kickoff = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    continue
                if kickoff.tzinfo is None:
                    kickoff = kickoff.replace(tzinfo=timezone.utc)
                season = (event.get("season") or {}).get("year") or today.year
                fixture_id = f"espn_{season}_{home_team}_{away_team}_{kickoff.strftime('%Y%m%d')}"
                if fixture_id in seen:
                    continue
                seen.add(fixture_id)
                matches.append(MatchData(
                    fixture_id=fixture_id,
                    sport="football",
                    league_key=league_key,
                    kickoff=kickoff,
                    home_team=home_team,
                    away_team=away_team,
                    completed=False,
                ))
        return matches

    def fetch_fixtures(
        self,
        start_date: date,
        end_date: date,
        leagues: Iterable[str] | None = None,
    ) -> list[dict]:
        """
        Fetches completed fixtures across mapped leagues for the given date range.

        Each returned dict contains:
            fixture_id, fixture_date, home_team, away_team,
            home_goals, away_goals, home_xg, away_xg,
            home_logo, away_logo, league_key

        fixture_id:  "espn_{season}_{home_team}_{away_team}_{YYYYMMDD}"
        home_xg / away_xg: float from ESPN expectedGoals statistic, or None.
        home_logo / away_logo: ESPN CDN URL (e.g. https://a.espncdn.com/...), or None.
        league_key:  the LEAGUE_MAP key (e.g. "epl").
        """
        league_keys = list(leagues) if leagues is not None else list(self.LEAGUE_MAP.keys())
        seen: set[str] = set()
        fixtures: list[dict] = []

        for league_key in league_keys:
            espn_slug = self.LEAGUE_MAP.get(league_key)
            if espn_slug is None:
                logger.warning("ESPNSoccerClient: unknown league key %r — skipping.", league_key)
                continue

            events = self.fetch_scoreboard(self.SPORT, espn_slug, start_date, end_date)
            league_count = 0

            for event in events:
                competitions = event.get("competitions") or []
                if not competitions:
                    continue

                comp = competitions[0]
                if not comp.get("status", {}).get("type", {}).get("completed"):
                    continue

                competitors = comp.get("competitors") or []
                home_c = next((c for c in competitors if c.get("homeAway") == "home"), None)
                away_c = next((c for c in competitors if c.get("homeAway") == "away"), None)
                if home_c is None or away_c is None:
                    continue

                home_team_data = home_c.get("team") or {}
                away_team_data = away_c.get("team") or {}
                home_team = home_team_data.get("displayName")
                away_team = away_team_data.get("displayName")
                if not home_team or not away_team:
                    continue

                try:
                    home_goals = int(home_c["score"])
                    away_goals = int(away_c["score"])
                except (KeyError, ValueError, TypeError):
                    continue

                season = (event.get("season") or {}).get("year") or start_date.year
                raw_date = event.get("date", "")
                try:
                    fixture_date = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    continue

                date_str = fixture_date.strftime("%Y%m%d")
                fixture_id = f"espn_{season}_{home_team}_{away_team}_{date_str}"

                if fixture_id in seen:
                    continue
                seen.add(fixture_id)

                if fixture_date.tzinfo is None:
                    fixture_date = fixture_date.replace(tzinfo=timezone.utc)

                fixtures.append({
                    "fixture_id":   fixture_id,
                    "fixture_date": fixture_date,
                    "home_team":    home_team,
                    "away_team":    away_team,
                    "home_goals":   home_goals,
                    "away_goals":   away_goals,
                    "home_xg":      _extract_xg(home_c),
                    "away_xg":      _extract_xg(away_c),
                    "home_logo":    home_team_data.get("logo"),
                    "away_logo":    away_team_data.get("logo"),
                    "league_key":   league_key,
                })
                league_count += 1

            logger.info(
                "ESPNSoccerClient: %s (%s) → %d completed fixtures.",
                league_key, espn_slug, league_count,
            )

        xg_count = sum(1 for f in fixtures if f["home_xg"] is not None)
        logger.info(
            "ESPNSoccerClient: %d total fixtures fetched (%d with xG).",
            len(fixtures), xg_count,
        )
        return fixtures


def _fixture_to_match_data(f: dict) -> MatchData:
    """Converts a raw fetch_fixtures() dict to a MatchData instance."""
    return MatchData(
        fixture_id=f["fixture_id"],
        sport="football",
        league_key=f["league_key"],
        kickoff=f["fixture_date"],
        home_team=f["home_team"],
        away_team=f["away_team"],
        home_score=f["home_goals"],
        away_score=f["away_goals"],
        completed=True,
        metadata={
            "home_xg":   f.get("home_xg"),
            "away_xg":   f.get("away_xg"),
            "home_logo": f.get("home_logo"),
            "away_logo": f.get("away_logo"),
        },
    )


def _extract_xg(competitor: dict) -> float | None:
    """Returns the expectedGoals float from the ESPN statistics array, or None."""
    for stat in competitor.get("statistics") or []:
        if stat.get("name") == "expectedGoals":
            try:
                return float(stat["displayValue"])
            except (KeyError, ValueError, TypeError):
                return None
    return None
