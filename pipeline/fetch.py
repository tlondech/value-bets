"""
Data fetching for a single league.

Hits The Odds API and ESPN on every call; no local caching.
"""

import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from config import LeagueConfig
from extractors.espn_soccer_client import ESPNSoccerClient
from extractors.odds import OddsAPIClient

logger = logging.getLogger(__name__)


def fetch_league_data(
    league: LeagueConfig,
    cfg,
    name_map: dict,
    season: int,
    dry_run: bool = False,
) -> tuple[list[dict], list[dict], dict[str, str], dict[str, str], Optional[OddsAPIClient]]:
    """
    Fetches data for one league from The Odds API and ESPN.

    Returns (upcoming_events, raw_fixtures, stage_map, crest_map, odds_client).
    Returns ([], [], {}, {}, None) on any unrecoverable failure.
    """
    stage_map: dict[str, str] = {}
    crest_map: dict[str, str] = {}

    # Fetch odds from API (basketball and football also need spreads)
    extra_markets = ["spreads"] if league.sport_type in ("basketball", "football") else []
    odds_client = OddsAPIClient(
        api_key=cfg.odds_api_key,
        sport=league.odds_sport,
        region=cfg.odds_region,
        bookmaker=cfg.odds_bookmaker,
        market=cfg.odds_market,
        odds_format=cfg.odds_format,
        totals_bookmakers=cfg.odds_totals_bookmakers,
        extra_markets=extra_markets,
    )
    upcoming_events = odds_client.fetch_upcoming_odds()

    # Tennis and basketball: no fixture history needed — return early
    if league.sport_type in ("tennis", "basketball"):
        if dry_run:
            if not upcoming_events:
                logger.info(
                    "[DRY-RUN] %-20s  sport=%-35s  → no upcoming matches on Winamax",
                    league.display_name, league.odds_sport,
                )
            else:
                logger.info(
                    "[DRY-RUN] %-20s  sport=%-35s  → %d match(es) found on Winamax",
                    league.display_name, league.odds_sport, len(upcoming_events),
                )
        return upcoming_events, [], {}, {}, odds_client

    if dry_run:
        if not upcoming_events:
            logger.info(
                "[DRY-RUN] %-20s  sport=%-35s  → no upcoming matches on Winamax",
                league.display_name, league.odds_sport,
            )
        else:
            logger.info(
                "[DRY-RUN] %-20s  sport=%-35s  → %d match(es) found on Winamax",
                league.display_name, league.odds_sport, len(upcoming_events),
            )
        return upcoming_events, [], {}, {}, odds_client

    if not upcoming_events:
        logger.debug("[%s] No upcoming matches with Winamax odds — skipping.", league.key)
        return [], [], {}, {}, None

    # For UCL, stamp the competition stage on events
    if league.key == "ucl":
        espn_stage = ESPNSoccerClient()
        ucl_upcoming = espn_stage.fetch_upcoming_matches(leagues=["ucl"])
        current_stage = next(
            (m.metadata["stage"] for m in ucl_upcoming if m.metadata.get("stage")), None,
        )
        if current_stage:
            for event in upcoming_events:
                if not event.get("stage"):
                    event["stage"] = current_stage

    # Fetch finished fixtures from ESPN
    espn = ESPNSoccerClient()
    season_start = date(season, 7, 1)
    raw_fixtures = espn.fetch_fixtures(season_start, date.today(), leagues=[league.key])

    if not raw_fixtures:
        logger.warning(
            "[%s] No finished fixtures found — season may not have started yet. Skipping.",
            league.key,
        )
        return [], [], {}, {}, None

    # Auto-resolve unmapped Winamax names against ESPN names for this league
    from models.features import auto_patch_name_map, resolve_team_name
    winamax_names = {ev["home_team"] for ev in upcoming_events} | {ev["away_team"] for ev in upcoming_events}
    espn_names    = {f["home_team"] for f in raw_fixtures} | {f["away_team"] for f in raw_fixtures}
    auto_patch_name_map(league.key, winamax_names, espn_names, name_map, cfg.team_map_path)

    # Build crest map from ESPN logo URLs and persist to JSON
    p = Path(cfg.football_crest_map_path)
    crest_map = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    for f in raw_fixtures:
        for team_key, logo_key in (("home_team", "home_logo"), ("away_team", "away_logo")):
            logo = f.get(logo_key)
            if not logo:
                continue
            canonical = resolve_team_name(f[team_key], name_map, league.key) or f[team_key]
            crest_map[canonical] = logo
    p.write_text(json.dumps(crest_map, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.debug("[%s] Football crest map updated: %d entries.", league.key, len(crest_map))

    # Build stage_map for UCL (both directions to cover Leg 2 reversals)
    if league.key == "ucl":
        from models.features import resolve_team_name
        for f in raw_fixtures:
            stage = f.get("stage")
            if not stage:
                continue
            home_c = resolve_team_name(f["home_team"], name_map, league.key)
            away_c = resolve_team_name(f["away_team"], name_map, league.key)
            if home_c and away_c:
                stage_map[f"{home_c}|{away_c}"] = stage
                stage_map[f"{away_c}|{home_c}"] = stage

    return upcoming_events, raw_fixtures, stage_map, crest_map, odds_client
