import io
import logging
import urllib.request

import pandas as pd

logger = logging.getLogger(__name__)


class FootballDataError(Exception):
    pass


class FootballDataClient:
    """
    Fetches finished fixtures from football-data.co.uk free CSV files.
    No auth, no scraping — plain HTTP GET of a CSV.

    xG columns (HxG/AxG) are available from ~2022-23 onward via Opta.
    Earlier seasons fall back to actual goals in the features pipeline.
    """

    BASE_URL = "https://www.football-data.co.uk/mmz4281"

    def __init__(self, fd_code: str, season: int):
        self.fd_code = fd_code
        self.season = season
        # Season code: last 2 digits of start year + last 2 digits of end year
        # e.g. season=2025 → "2526"
        self._season_str = f"{season % 100:02d}{(season + 1) % 100:02d}"

    def fetch_fixtures(self) -> list[dict]:
        """
        Returns all finished fixtures for the configured league/season.
        Each entry: {fixture_id, fixture_date, home_team, away_team,
                     home_goals, away_goals, home_xg, away_xg}
        fixture_id is a deterministic string: "{fd_code}_{season}_{home}_{away}_{date}"
        """
        url = f"{self.BASE_URL}/{self._season_str}/{self.fd_code}.csv"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                raw = r.read()
        except Exception as e:
            raise FootballDataError(f"Failed to download {url}: {e}") from e

        try:
            df = pd.read_csv(io.BytesIO(raw), encoding="utf-8", on_bad_lines="skip")
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(raw), encoding="latin-1", on_bad_lines="skip")

        fixtures = []
        for _, row in df.iterrows():
            fthg = row.get("FTHG")
            ftag = row.get("FTAG")
            if fthg is None or ftag is None:
                continue
            try:
                if pd.isna(fthg) or pd.isna(ftag):
                    continue  # unplayed match
            except (TypeError, ValueError):
                continue

            date = row.get("Date")
            home = row.get("HomeTeam")
            away = row.get("AwayTeam")
            try:
                if pd.isna(date) or pd.isna(home) or pd.isna(away):
                    continue
            except (TypeError, ValueError):
                continue

            try:
                fixture_date = pd.to_datetime(str(date), dayfirst=True)
            except Exception:
                continue

            date_str = fixture_date.strftime("%Y-%m-%d")
            fid = f"{self.fd_code}_{self.season}_{home}_{away}_{date_str}"

            fixtures.append({
                "fixture_id":   fid,
                "fixture_date": fixture_date.to_pydatetime(),
                "home_team":    str(home),
                "away_team":    str(away),
                "home_goals":   int(fthg),
                "away_goals":   int(ftag),
                "home_xg":      _safe_float(row.get("HxG")),
                "away_xg":      _safe_float(row.get("AxG")),
            })

        xg_count = sum(1 for f in fixtures if f["home_xg"] is not None)
        logger.debug(
            "football-data.co.uk: fetched %d finished fixtures for %s %s (%d with xG).",
            len(fixtures), self.fd_code, self.season, xg_count,
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
