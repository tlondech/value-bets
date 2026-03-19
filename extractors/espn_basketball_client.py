"""ESPN public API client for basketball (NBA) match results.

Fetches completed NBA games and normalizes them to MatchData
(home_score / away_score = points scored).
"""

import logging
from datetime import datetime, timedelta, timezone

from extractors.base import MatchData
from extractors.espn_client import ESPNClient

logger = logging.getLogger(__name__)

# ESPN season type → stage label (post-season rounds are keyed by competition type id)
_SEASON_TYPE_STAGE: dict[int, str] = {
    5: "Play-In",
}
_PLAYOFF_COMP_TYPE_STAGE: dict[str, str] = {
    "14": "R1",
    "15": "Semis",
    "16": "Conf. Finals",
    "17": "Finals",
}


def _nba_stage_label(event: dict) -> str | None:
    """Derives a human-readable stage label from an ESPN event dict, or None for regular season."""
    season_type = event.get("season", {}).get("type")
    if season_type in _SEASON_TYPE_STAGE:
        return _SEASON_TYPE_STAGE[season_type]
    if season_type == 3:  # post-season — use competition type id for round
        comp_type_id = str(
            (event.get("competitions") or [{}])[0].get("type", {}).get("id", "")
        )
        return _PLAYOFF_COMP_TYPE_STAGE.get(comp_type_id)
    return None


class ESPNBasketballClient(ESPNClient):
    SPORT = "basketball"

    LEAGUE_MAP: dict[str, str] = {
        "nba": "nba",
    }

    def fetch_recent_results(self, days_back: int = 7) -> list[MatchData]:
        """
        Returns completed NBA games from the last `days_back` days as MatchData.
        home_score / away_score = points scored.
        Non-fatal — returns [] on failure.
        """
        results: list[MatchData] = []
        seen: set[tuple] = set()

        for league_key, espn_slug in self.LEAGUE_MAP.items():
            events = self._fetch_scoreboard_recent(self.SPORT, espn_slug, days_back)

            for event in events:
                comps = event.get("competitions")
                if not comps:
                    continue
                comp = comps[0]
                if not comp.get("status", {}).get("type", {}).get("completed"):
                    continue

                competitors = comp.get("competitors", [])
                home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                if not home or not away:
                    continue

                try:
                    home_pts = int(home["score"])
                    away_pts = int(away["score"])
                except (KeyError, ValueError, TypeError):
                    continue

                match_date = datetime.strptime(event["date"][:10], "%Y-%m-%d")
                kickoff = match_date.replace(tzinfo=timezone.utc)
                home_team = (home.get("team") or {}).get("displayName")
                away_team = (away.get("team") or {}).get("displayName")
                if not home_team or not away_team:
                    continue

                key = (kickoff.date(), home_team, away_team)
                if key in seen:
                    continue
                seen.add(key)

                date_str = kickoff.strftime("%Y%m%d")
                results.append(MatchData(
                    fixture_id=f"espn_nba_{home_team}_{away_team}_{date_str}",
                    sport="basketball",
                    league_key=league_key,
                    kickoff=kickoff,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=home_pts,
                    away_score=away_pts,
                    completed=True,
                ))

        logger.debug(
            "ESPNBasketballClient: %d completed game(s) over %d days",
            len(results), days_back,
        )
        return results

    def fetch_upcoming_matches(self, days_ahead: int = 7) -> list[MatchData]:
        """Returns scheduled (not yet completed) NBA games from ESPN."""
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=days_ahead)
        matches: list[MatchData] = []
        seen: set[tuple] = set()

        for league_key, espn_slug in self.LEAGUE_MAP.items():
            events = self.fetch_scoreboard(self.SPORT, espn_slug, today, end)
            for event in events:
                comps = event.get("competitions")
                if not comps:
                    continue
                comp = comps[0]
                if comp.get("status", {}).get("type", {}).get("completed"):
                    continue
                competitors = comp.get("competitors", [])
                home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                if not home or not away:
                    continue
                home_team = (home.get("team") or {}).get("displayName")
                away_team = (away.get("team") or {}).get("displayName")
                if not home_team or not away_team:
                    continue
                raw_date = event.get("date", "")
                try:
                    kickoff = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    if kickoff.tzinfo is None:
                        kickoff = kickoff.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    continue
                key = (kickoff.date(), home_team, away_team)
                if key in seen:
                    continue
                seen.add(key)
                date_str = kickoff.strftime("%Y%m%d")
                stage = _nba_stage_label(event)
                matches.append(MatchData(
                    fixture_id=f"espn_nba_{home_team}_{away_team}_{date_str}",
                    sport="basketball",
                    league_key=league_key,
                    kickoff=kickoff,
                    home_team=home_team,
                    away_team=away_team,
                    completed=False,
                    metadata={"stage": stage} if stage else {},
                ))
        return matches
