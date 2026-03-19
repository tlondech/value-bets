"""
Fetches historical tennis match data from Jeff Sackmann's GitHub repositories:
  https://github.com/JeffSackmann/tennis_atp
  https://github.com/JeffSackmann/tennis_wta

Returns cleaned DataFrames suitable for Elo rating computation.
"""
import io
import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SACKMANN_ATP = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
SACKMANN_WTA = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"

_REQUIRED_COLS = ["winner_name", "loser_name", "tourney_date"]


class TennisDataClient:
    def fetch_atp_matches(self, years: list[int]) -> pd.DataFrame:
        return self._fetch(SACKMANN_ATP, "atp_matches", years)

    def fetch_wta_matches(self, years: list[int]) -> pd.DataFrame:
        return self._fetch(SACKMANN_WTA, "wta_matches", years)

    def _fetch(self, base: str, prefix: str, years: list[int]) -> pd.DataFrame:
        dfs = []
        for year in years:
            url = f"{base}/{prefix}_{year}.csv"
            try:
                r = requests.get(url, timeout=30)
                if r.ok:
                    df = pd.read_csv(io.StringIO(r.text), low_memory=False)
                    dfs.append(df)
                    logger.debug("Fetched %d rows from %s", len(df), url)
                else:
                    logger.debug("No data at %s (HTTP %d)", url, r.status_code)
            except Exception as e:
                logger.warning("Failed to fetch %s: %s", url, e)

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)

        # Drop qualifier rounds (round starts with "Q") and walkovers
        if "round" in df.columns:
            df = df[~df["round"].str.startswith("Q", na=False)]

        df = df.dropna(subset=_REQUIRED_COLS)
        df["tourney_date"] = pd.to_datetime(df["tourney_date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["tourney_date"])
        df = df.sort_values("tourney_date").reset_index(drop=True)

        logger.debug("Tennis data: %d completed matches across %d year(s)", len(df), len(years))
        return df
