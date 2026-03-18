"""
NBA historical game data client using the nba_api package.

Fetches team game logs from NBA Stats (stats.nba.com) for use in the
Gaussian efficiency model. No API key required, but the endpoint enforces
rate limiting — callers must sleep between requests.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

logger = logging.getLogger(__name__)

# NBA Stats API enforces ~1 req/sec; sleep between calls to avoid 429s
_REQUEST_SLEEP = 0.6
_REQUEST_TIMEOUT = 60  # stats.nba.com is slow from cloud IPs
_MAX_RETRIES = 3

# League ID for NBA
_NBA_LEAGUE_ID = "00"

# stats.nba.com blocks requests without browser-like headers (especially from CI)
_NBA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
    "Host": "stats.nba.com",
}


class NBADataClient:
    """
    Thin wrapper around nba_api endpoints.
    All methods return empty DataFrames / empty lists on failure (non-fatal).
    """

    def fetch_team_game_logs(
        self,
        season: str,
        include_playoffs: bool = True,
    ) -> pd.DataFrame:
        """
        Fetches all team game logs for the given season (e.g. '2024-25').

        Returns a DataFrame with columns:
            TEAM_ABBREVIATION, GAME_DATE (datetime.date), is_home (bool),
            PTS (int), OPP_PTS (int)

        Each game appears once per team perspective. Use is_home=True rows
        for home perspective and is_home=False for away perspective.
        """
        try:
            from nba_api.stats.endpoints import leaguegamefinder
        except ImportError:
            logger.error("nba_api is not installed. Run: pip install nba_api")
            return pd.DataFrame()

        all_frames = []
        season_types = ["Regular Season"]
        if include_playoffs:
            season_types.append("Playoffs")

        for season_type in season_types:
            for attempt in range(_MAX_RETRIES):
                try:
                    time.sleep(_REQUEST_SLEEP * (2 ** attempt))
                    finder = leaguegamefinder.LeagueGameFinder(
                        league_id_nullable=_NBA_LEAGUE_ID,
                        season_nullable=season,
                        season_type_nullable=season_type,
                        timeout=_REQUEST_TIMEOUT,
                        headers=_NBA_HEADERS,
                    )
                    df = finder.get_data_frames()[0]
                    if not df.empty:
                        all_frames.append(df)
                        logger.debug(
                            "NBA %s %s: %d game rows fetched",
                            season, season_type, len(df),
                        )
                    break
                except Exception as exc:
                    if attempt < _MAX_RETRIES - 1:
                        logger.debug(
                            "NBA fetch attempt %d/%d failed for %s %s, retrying: %s",
                            attempt + 1, _MAX_RETRIES, season, season_type, exc,
                        )
                    else:
                        logger.warning(
                            "NBA fetch failed for %s %s: %s", season, season_type, exc
                        )

        if not all_frames:
            return pd.DataFrame()

        raw = pd.concat(all_frames, ignore_index=True)
        return _parse_game_logs(raw)

    def fetch_recent_results(self, days_back: int = 7) -> list[dict]:
        """
        Returns completed NBA games from the last `days_back` days as a list of dicts:
            {home_team, away_team, home_pts, away_pts, game_date}

        Uses only the home-team perspective rows to produce one entry per game.
        team names are the full TEAM_NAME from the NBA API (e.g. "Los Angeles Lakers").
        """
        try:
            from nba_api.stats.endpoints import leaguegamefinder
        except ImportError:
            logger.error("nba_api is not installed.")
            return []

        now = datetime.now(timezone.utc)
        date_from = (now - timedelta(days=days_back)).strftime("%m/%d/%Y")
        date_to = now.strftime("%m/%d/%Y")

        df = None
        for attempt in range(_MAX_RETRIES):
            try:
                time.sleep(_REQUEST_SLEEP * (2 ** attempt))
                finder = leaguegamefinder.LeagueGameFinder(
                    league_id_nullable=_NBA_LEAGUE_ID,
                    date_from_nullable=date_from,
                    date_to_nullable=date_to,
                    timeout=_REQUEST_TIMEOUT,
                    headers=_NBA_HEADERS,
                )
                df = finder.get_data_frames()[0]
                break
            except Exception as exc:
                if attempt < _MAX_RETRIES - 1:
                    logger.debug(
                        "NBA recent results fetch attempt %d/%d failed, retrying: %s",
                        attempt + 1, _MAX_RETRIES, exc,
                    )
                else:
                    logger.warning("NBA recent results fetch failed: %s", exc)
        if df is None:
            return []

        if df.empty:
            return []

        parsed = _parse_game_logs(df)
        if parsed.empty:
            return []

        # Keep only the home perspective (one row per game)
        home_rows = parsed[parsed["is_home"]].copy()
        results = []
        for _, row in home_rows.iterrows():
            results.append({
                "home_team":  row["TEAM_NAME"],
                "away_team":  row["OPP_TEAM_NAME"],
                "home_pts":   int(row["PTS"]),
                "away_pts":   int(row["OPP_PTS"]),
                "game_date":  row["GAME_DATE"],
            })
        return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_game_logs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalises raw LeagueGameFinder output into a clean DataFrame.

    Added columns:
        is_home   — True if this row represents the home team
        OPP_PTS   — points scored by the opponent (derived from PLUS_MINUS)
        OPP_TEAM_NAME — full name of the opponent (extracted from MATCHUP)
        GAME_DATE — converted to datetime.date
    """
    if df.empty:
        return pd.DataFrame()

    required = {"TEAM_ABBREVIATION", "TEAM_NAME", "GAME_DATE", "MATCHUP", "PTS", "PLUS_MINUS"}
    missing = required - set(df.columns)
    if missing:
        logger.error("NBA game log DataFrame missing columns: %s", missing)
        return pd.DataFrame()

    df = df.copy()

    # is_home: MATCHUP is "ABBR vs. OPP" for home, "ABBR @ OPP" for away
    df["is_home"] = df["MATCHUP"].str.contains(r"\bvs\.", regex=True)

    # Opponent points: PTS - PLUS_MINUS  (PLUS_MINUS = PTS - OPP_PTS)
    df["OPP_PTS"] = (df["PTS"] - df["PLUS_MINUS"]).round().astype(int)

    # Opponent team name from MATCHUP: "LAL vs. BOS" → "BOS", "LAL @ BOS" → "BOS"
    df["OPP_ABBR"] = df["MATCHUP"].str.split(r"vs\.|@").str[-1].str.strip()

    # Build abbr → full name lookup from this same DataFrame
    abbr_to_name = (
        df[["TEAM_ABBREVIATION", "TEAM_NAME"]]
        .drop_duplicates()
        .set_index("TEAM_ABBREVIATION")["TEAM_NAME"]
        .to_dict()
    )
    df["OPP_TEAM_NAME"] = df["OPP_ABBR"].map(abbr_to_name).fillna(df["OPP_ABBR"])

    # Normalise GAME_DATE to datetime.date
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"]).dt.date

    # Keep only the columns we need downstream
    keep = ["TEAM_ABBREVIATION", "TEAM_NAME", "GAME_DATE", "is_home", "PTS", "OPP_PTS", "OPP_TEAM_NAME"]
    return df[keep].reset_index(drop=True)
