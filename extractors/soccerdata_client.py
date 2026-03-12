import logging

import pandas as pd
import soccerdata as sd

logger = logging.getLogger(__name__)


class SoccerDataError(Exception):
    pass


class FBrefClient:
    def __init__(self, fbref_league_key: str, season: int):
        self.fbref_league_key = fbref_league_key
        self.season = season

    def fetch_fixtures(self) -> list[dict]:
        """
        Returns all finished fixtures for the configured league/season.
        Each entry: {fixture_id, fixture_date, home_team, away_team,
                     home_goals, away_goals, home_xg, away_xg}
        fixture_id is a deterministic string: "{league}_{season}_{home}_{away}_{date}"
        xG is included when FBref has it (most European leagues from ~2017+), None otherwise.
        """
        try:
            fbref = sd.FBref(leagues=[self.fbref_league_key], seasons=[self.season])
            schedule = fbref.read_schedule()
        except Exception as e:
            raise SoccerDataError(
                f"FBref scrape failed for {self.fbref_league_key} {self.season}: {e}"
            ) from e

        # Flatten multi-index columns if present
        if isinstance(schedule.columns, pd.MultiIndex):
            schedule.columns = [
                "_".join(str(c) for c in col).strip("_") if isinstance(col, tuple) else col
                for col in schedule.columns
            ]

        fixtures = []
        for _, row in schedule.iterrows():
            score = row.get("score") or row.get("Score")
            # Skip unplayed fixtures (score is NaN, None, or empty)
            if score is None or (isinstance(score, float) and pd.isna(score)) or str(score).strip() == "":
                continue

            try:
                score_str = str(score)
                # FBref uses en-dash (–) or regular hyphen
                sep = "–" if "–" in score_str else "-"
                parts = score_str.split(sep)
                home_goals = int(parts[0].strip())
                away_goals = int(parts[1].strip())
            except (ValueError, AttributeError, IndexError):
                continue

            date = row.get("date") or row.get("Date")
            home = row.get("home") or row.get("Home")
            away = row.get("away") or row.get("Away")
            if date is None or home is None or away is None:
                continue

            date_str = str(date)[:10]
            fid = f"{self.fbref_league_key}_{self.season}_{home}_{away}_{date_str}"

            # xG: FBref column names vary by soccerdata version
            home_xg = _safe_float(
                row.get("xg_home") or row.get("xGHome") or row.get("xg_Home")
                or row.get("home_xg") or row.get("expected_goals_home")
            )
            away_xg = _safe_float(
                row.get("xg_away") or row.get("xGAway") or row.get("xg_Away")
                or row.get("away_xg") or row.get("expected_goals_away")
            )

            fixtures.append({
                "fixture_id":   fid,
                "fixture_date": pd.Timestamp(date).to_pydatetime() if not isinstance(date, str) else date,
                "home_team":    str(home),
                "away_team":    str(away),
                "home_goals":   home_goals,
                "away_goals":   away_goals,
                "home_xg":      home_xg,
                "away_xg":      away_xg,
            })

        xg_count = sum(1 for f in fixtures if f["home_xg"] is not None)
        logger.info(
            "FBref: fetched %d finished fixtures for %s %s (%d with xG).",
            len(fixtures), self.fbref_league_key, self.season, xg_count,
        )
        return fixtures

    def fetch_team_list(self) -> list[str]:
        """Returns sorted unique team names in this league/season."""
        fixtures = self.fetch_fixtures()
        names: set[str] = set()
        for f in fixtures:
            names.add(f["home_team"])
            names.add(f["away_team"])
        return sorted(names)


def _safe_float(val) -> float | None:
    """Converts val to float, returning None for missing/NaN values."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None
