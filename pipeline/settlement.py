"""
Settlement helpers — ESPN primary source, season-long fixtures as fill-in.

Public entry point: settle_all_sports()
"""

import logging
from dataclasses import replace
from datetime import date, timedelta

from extractors.base import MatchData
from extractors.espn_soccer_client import ESPNSoccerClient
from models.features import resolve_team_name

logger = logging.getLogger(__name__)


def _fetch_org_settlement_fixtures(
    leagues: list,
    cfg,
    name_map: dict,
) -> list[MatchData]:
    """
    Fetches recent completed fixtures from ESPN for settlement use.
    Covers the last 14 days to ensure freshly finished matches are captured.
    Team names are pre-resolved to canonical form.
    Returns [] on total failure.
    """
    football_leagues = [lg for lg in leagues if lg.sport_type == "football"]
    if not football_leagues:
        return []

    today = date.today()
    start = today - timedelta(days=14)
    league_keys = [lg.key for lg in football_leagues]

    try:
        raw = ESPNSoccerClient().fetch_fixtures(start, today, leagues=league_keys)
    except Exception as e:
        logger.warning("ESPN settlement fetch failed: %s", e)
        return []

    results: list[MatchData] = []
    for f in raw:
        lk = f.get("league_key", "")
        home_c = resolve_team_name(f["home_team"], name_map, lk) or f["home_team"]
        away_c = resolve_team_name(f["away_team"], name_map, lk) or f["away_team"]
        from extractors.espn_soccer_client import _fixture_to_match_data
        m = _fixture_to_match_data(f)
        results.append(replace(m, home_team=home_c, away_team=away_c))

    logger.debug("_fetch_org_settlement_fixtures: %d fixtures from ESPN.", len(results))
    return results


def _merge_settlement_fixtures(
    all_raw_fixtures: list[dict],
    espn_settle: list[MatchData],
    name_map: dict,
) -> list[MatchData]:
    """
    Merges season-long ESPN fixture dicts with recent ESPN settlement MatchData.
    Recent settlement entries take precedence (fresher).
    Dedup key: (canonical_home, canonical_away, YYYY-MM-DD).
    Returns list[MatchData].
    """
    from extractors.espn_soccer_client import _fixture_to_match_data

    def _date_str(dt) -> str:
        if hasattr(dt, "strftime"):
            return dt.strftime("%Y-%m-%d")
        return str(dt)[:10]

    # Index recent settlement entries (already pre-resolved to canonical)
    settle_index: dict[tuple, MatchData] = {}
    for m in espn_settle:
        key = (m.home_team, m.away_team, _date_str(m.kickoff))
        settle_index[key] = m

    # Fill from season-long fixtures where not already covered
    fill_ins: list[MatchData] = []
    for f in all_raw_fixtures:
        lk = f.get("league_key", "")
        home_c = resolve_team_name(f["home_team"], name_map, lk) or f["home_team"]
        away_c = resolve_team_name(f["away_team"], name_map, lk) or f["away_team"]
        key = (home_c, away_c, _date_str(f["fixture_date"]))
        if key not in settle_index:
            m = _fixture_to_match_data(f)
            fill_ins.append(replace(m, home_team=home_c, away_team=away_c))

    return list(settle_index.values()) + fill_ins


def settle_all_sports(
    supabase,
    cfg,
    all_raw_fixtures: list[dict],
    name_map: dict,
    force_fetch: bool,
) -> None:
    """
    Settles past signals across all sport types.

    Replaces the inline _settle_all() in main.py as the single settlement entry point.
    """
    from db.supabase import (
        backfill_tennis_scores,
        settle_nba_supabase_signals,
        settle_supabase_signals,
        settle_tennis_supabase_signals,
    )

    espn_settle = _fetch_org_settlement_fixtures(cfg.enabled_leagues, cfg, name_map) if force_fetch else []
    football_fixtures = _merge_settlement_fixtures(all_raw_fixtures, espn_settle, name_map)
    settle_supabase_signals(supabase, football_fixtures, name_map)

    if any(lg.sport_type == "tennis" for lg in cfg.enabled_leagues):
        settle_tennis_supabase_signals(supabase)
        backfill_tennis_scores(supabase)

    nba_keys = [lg.key for lg in cfg.enabled_leagues if lg.sport_type == "basketball"]
    if nba_keys:
        settle_nba_supabase_signals(supabase, nba_keys, name_map)
