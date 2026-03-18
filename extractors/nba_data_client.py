"""
NBA historical game data client.

Uses the ESPN public API (no key required, works from any IP including CI).
Falls back to an on-disk CSV cache written after every successful fetch.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd

logger = logging.getLogger(__name__)

# Cache — committed to the repo so runs without network access still have data
_CACHE_PATH = Path(__file__).parent.parent / "data" / "nba_game_logs_cache.csv"

# ESPN public API base — no key, no IP restrictions
_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"


class NBADataClient:
    """
    Fetches team game logs and recent results via ESPN.
    All methods return empty DataFrames / lists on total failure (non-fatal).
    """

    # ── Season game logs (used for ratings) ───────────────────────

    def fetch_team_game_logs(
        self,
        season: str,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with columns:
            TEAM_ABBREVIATION, TEAM_NAME, GAME_DATE (datetime.date),
            is_home (bool), PTS (int), OPP_PTS (int), OPP_TEAM_NAME (str)

        Each game appears once per team perspective.
        Fetches the previous completed season and the current season so that
        teams with limited current-season data still have a full ratings history.
        """
        prev_season = _previous_season(season)
        logger.debug("NBA fetch_team_game_logs: season=%s (also fetching %s)", season, prev_season)

        prev_df = _fetch_from_espn(prev_season)
        curr_df = _fetch_from_espn(season)

        if not curr_df.empty or not prev_df.empty:
            parts = [df for df in (prev_df, curr_df) if not df.empty]
            result = pd.concat(parts, ignore_index=True)
            _save_cache(result, season)
            return result

        return _load_cache(season)

    # ── Recent results (used for settlement) ──────────────────────

    def fetch_recent_results(self, days_back: int = 7) -> list[dict]:
        """
        Returns completed NBA games from the last `days_back` days as:
            [{home_team, away_team, home_pts, away_pts, game_date}, ...]
        """
        logger.debug("NBA fetch_recent_results: days_back=%d", days_back)
        return _fetch_recent_from_espn(days_back)


# ---------------------------------------------------------------------------
# ESPN helpers
# ---------------------------------------------------------------------------

def _espn_season_year(season: str) -> int:
    """'2024-25' → 2025"""
    return int(season.split("-")[0]) + 1


def _previous_season(season: str) -> str:
    """'2025-26' → '2024-25'"""
    start = int(season.split("-")[0])
    return f"{start - 1}-{start:02d}"


def _parse_score(raw) -> int:
    """Handle ESPN score fields that vary by endpoint: string, int, or dict."""
    if isinstance(raw, (int, float)):
        return int(raw)
    if isinstance(raw, str):
        return int(raw)
    if isinstance(raw, dict):
        val = raw.get("value") or raw.get("displayValue")
        if val is None:
            raise ValueError(f"No value/displayValue in score dict: {raw!r}")
        return int(val)
    raise ValueError(f"Unexpected score type: {type(raw).__name__} {raw!r}")


def _fetch_from_espn(season: str) -> pd.DataFrame:
    """
    Fetches full-season game logs from ESPN's scoreboard endpoint using monthly
    date-range requests (9 requests total). No API key required.
    """
    import calendar as cal

    season_year = _espn_season_year(season)
    start_year  = season_year - 1
    now         = datetime.now(timezone.utc)

    # NBA season: Oct of start_year through Jun of season_year
    months = (
        [(start_year, m) for m in range(10, 13)] +
        [(season_year, m) for m in range(1, 7)]
    )
    logger.debug("ESPN _fetch_from_espn: season_year=%d, %d months planned", season_year, len(months))

    all_rows: list[dict] = []
    seen: set[tuple]     = set()

    for year, month in months:
        if datetime(year, month, 1, tzinfo=timezone.utc) > now:
            logger.debug("ESPN skipping future month %d-%02d", year, month)
            break

        last_day   = cal.monthrange(year, month)[1]
        date_range = f"{year}{month:02d}01-{year}{month:02d}{last_day:02d}"

        try:
            time.sleep(0.3)
            r = requests.get(
                f"{_ESPN_BASE}/scoreboard",
                params={"dates": date_range, "limit": 500},
                timeout=15,
            )
            r.raise_for_status()
            events = r.json().get("events", [])
        except Exception as exc:
            logger.warning("ESPN scoreboard fetch failed for %s: %s", date_range, exc)
            continue

        completed = 0
        skipped   = 0

        for event in events:
            comps = event.get("competitions")
            if not comps:
                continue
            comp = comps[0]
            if not comp.get("status", {}).get("type", {}).get("completed"):
                skipped += 1
                continue

            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c["homeAway"] == "home"), None)
            away = next((c for c in competitors if c["homeAway"] == "away"), None)
            if not home or not away:
                continue

            try:
                home_pts = _parse_score(home["score"])
                away_pts = _parse_score(away["score"])
            except (KeyError, ValueError, TypeError) as exc:
                logger.debug("ESPN bad-score: home=%s away=%s err=%s", home.get("score"), away.get("score"), exc)
                skipped += 1
                continue

            game_date = datetime.strptime(event["date"][:10], "%Y-%m-%d").date()
            home_abbr = home["team"].get("abbreviation")
            away_abbr = away["team"].get("abbreviation")
            if not home_abbr or not away_abbr:
                skipped += 1
                continue
            home_name = home["team"].get("displayName", home_abbr)
            away_name = away["team"].get("displayName", away_abbr)

            key = (game_date, home_abbr, away_abbr)
            if key in seen:
                continue
            seen.add(key)
            completed += 1

            # One row per team perspective (home + away)
            for is_home, abbr, name, pts, opp_pts, opp_name in [
                (True,  home_abbr, home_name, home_pts, away_pts, away_name),
                (False, away_abbr, away_name, away_pts, home_pts, home_name),
            ]:
                all_rows.append({
                    "TEAM_ABBREVIATION": abbr,
                    "TEAM_NAME":         name,
                    "GAME_DATE":         game_date,
                    "is_home":           is_home,
                    "PTS":               pts,
                    "OPP_PTS":           opp_pts,
                    "OPP_TEAM_NAME":     opp_name,
                })

        logger.debug(
            "ESPN scoreboard %s: %d events, %d completed, %d skipped",
            date_range, len(events), completed, skipped,
        )

    if not all_rows:
        logger.warning("ESPN returned no game rows for season %s (season_year=%d)", season, season_year)
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).reset_index(drop=True)
    logger.info("NBA ESPN: %d game rows fetched for season %s", len(df), season)
    return df


def _fetch_recent_from_espn(days_back: int) -> list[dict]:
    """Fetches completed games from the last `days_back` days via ESPN scoreboard."""
    now    = datetime.now(timezone.utc)
    dates  = [(now - timedelta(days=i)).strftime("%Y%m%d") for i in range(days_back)]
    result = []
    seen: set[tuple] = set()

    logger.debug("ESPN _fetch_recent_from_espn: querying %d dates (%s … %s)", len(dates), dates[-1], dates[0])

    for d in dates:
        try:
            r = requests.get(
                f"{_ESPN_BASE}/scoreboard",
                params={"dates": d, "limit": 50},
                timeout=15,
            )
            r.raise_for_status()
            events = r.json().get("events", [])
        except Exception as exc:
            logger.debug("ESPN scoreboard fetch failed for %s: %s", d, exc)
            continue

        day_count = 0
        for event in events:
            comps = event.get("competitions")
            if not comps:
                continue
            comp = comps[0]
            if not comp.get("status", {}).get("type", {}).get("completed"):
                continue

            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c["homeAway"] == "home"), None)
            away = next((c for c in competitors if c["homeAway"] == "away"), None)
            if not home or not away:
                continue

            try:
                home_pts = int(home["score"])
                away_pts = int(away["score"])
            except (KeyError, ValueError, TypeError):
                continue

            game_date = datetime.strptime(event["date"][:10], "%Y-%m-%d").date()
            home_team = home["team"]["displayName"]
            away_team = away["team"]["displayName"]
            key       = (game_date, home_team, away_team)
            if key in seen:
                continue
            seen.add(key)
            day_count += 1

            result.append({
                "home_team": home_team,
                "away_team": away_team,
                "home_pts":  home_pts,
                "away_pts":  away_pts,
                "game_date": game_date,
            })

        logger.debug("ESPN scoreboard %s: %d completed games", d, day_count)

    logger.debug("ESPN recent results total: %d games over %d days", len(result), days_back)
    return result


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _save_cache(df: pd.DataFrame, season: str) -> None:
    try:
        out = df.copy()
        out["_season"] = season
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(_CACHE_PATH, index=False)
        logger.debug("NBA game log cache saved (%d rows, season %s)", len(df), season)
    except Exception as exc:
        logger.warning("NBA cache save failed: %s", exc)


def _load_cache(season: str) -> pd.DataFrame:
    if not _CACHE_PATH.exists():
        logger.warning("NBA ESPN fetch failed and no cache found at %s", _CACHE_PATH)
        return pd.DataFrame()
    try:
        df            = pd.read_csv(_CACHE_PATH)
        cached_season = df["_season"].iloc[0] if "_season" in df.columns else "unknown"
        df            = df.drop(columns=["_season"], errors="ignore")
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"]).dt.date
        df["is_home"]   = df["is_home"].astype(bool)
        logger.debug("NBA cache loaded: %d rows, cached season=%s", len(df), cached_season)
        if cached_season != season:
            logger.warning(
                "NBA ESPN fetch failed — cache is for %s, current season is %s",
                cached_season, season,
            )
        else:
            logger.warning("NBA ESPN fetch failed — using cached game logs for %s", season)
        return df
    except Exception as exc:
        logger.warning("NBA cache load failed: %s", exc)
        return pd.DataFrame()
