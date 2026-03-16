"""
Settlement helpers — dual-source (football-data.org supplements .co.uk).
"""

import logging

from config import _current_season
from extractors.footballdataorg_client import FootballDataOrgClient, FootballDataOrgError
from models.features import resolve_team_name

logger = logging.getLogger(__name__)


def _fetch_org_settlement_fixtures(
    leagues: list,
    cfg,
    name_map: dict,
) -> list[dict]:
    """
    Fetches finished fixtures from football-data.org for settlement use only.
    Only called in force=True path. Returns [] on total failure.
    Skips leagues whose fdo_enrich_code is None (UCL already covered via fdo_code,
    World Cup has no .org source). Team names are pre-resolved to canonical form.
    """
    if not cfg.fdo_api_key:
        return []

    season = _current_season()
    results: list[dict] = []

    for league in leagues:
        settle_code = league.fdo_enrich_code
        if not settle_code:
            continue
        try:
            client = FootballDataOrgClient(settle_code, season, cfg.fdo_api_key)
            fixtures = client.fetch_fixtures()
        except FootballDataOrgError as e:
            logger.warning(
                "[%s] .org settlement fetch failed (will fall back to .co.uk): %s",
                league.key, e,
            )
            continue

        for f in fixtures:
            home_c = resolve_team_name(f["home_team"], name_map, league.key)
            away_c = resolve_team_name(f["away_team"], name_map, league.key)
            if not home_c or not away_c:
                continue
            results.append({**f, "home_team": home_c, "away_team": away_c, "league_key": league.key})

    logger.debug("_fetch_org_settlement_fixtures: %d fixtures across %d leagues.", len(results), len(leagues))
    return results


def _merge_settlement_fixtures(
    couk_fixtures: list[dict],
    org_fixtures: list[dict],
    name_map: dict,
) -> list[dict]:
    """
    Merges .co.uk and .org fixture lists for settlement.
    .org entries take precedence (near real-time). .co.uk fills gaps.
    Dedup key: (canonical_home, canonical_away, YYYY-MM-DD) — timezone-safe.
    """
    def _date_str(dt) -> str:
        if hasattr(dt, "strftime"):
            return dt.strftime("%Y-%m-%d")
        return str(dt)[:10]

    # Index .org entries (already canonical)
    org_index: dict[tuple, dict] = {}
    for f in org_fixtures:
        key = (f["home_team"], f["away_team"], _date_str(f["fixture_date"]))
        org_index[key] = f

    # Fill in .co.uk entries not covered by .org
    fill_ins: list[dict] = []
    for f in couk_fixtures:
        lk = f.get("league_key", "")
        home_c = resolve_team_name(f["home_team"], name_map, lk) or f["home_team"]
        away_c = resolve_team_name(f["away_team"], name_map, lk) or f["away_team"]
        key = (home_c, away_c, _date_str(f["fixture_date"]))
        if key not in org_index:
            fill_ins.append(f)

    return list(org_index.values()) + fill_ins
