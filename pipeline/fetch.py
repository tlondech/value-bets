"""
Data fetching and loading for a single league.

In force_fetch mode: hits The Odds API and ESPN, then persists everything to SQLite.
In cached mode: loads from SQLite only.
"""

import json
import logging
from datetime import date
from pathlib import Path

from sqlalchemy.orm import Session

from config import LeagueConfig
from db.queries import (
    load_raw_fixtures_from_db,
    load_upcoming_events_from_db,
    upsert_fixtures,
    upsert_match,
    upsert_odds,
)
from extractors.espn_soccer_client import ESPNSoccerClient
from extractors.odds import OddsAPIClient
from pipeline.helpers import is_live

logger = logging.getLogger(__name__)


def fetch_league_data(
    league: LeagueConfig,
    cfg,
    engine,
    name_map: dict,
    force_fetch: bool,
    season: int,
    dry_run: bool = False,
) -> tuple[list[dict], list[dict], dict[str, str], dict[str, str], OddsAPIClient | None]:
    """
    Fetches or loads data for one league.

    Returns (upcoming_events, raw_fixtures, stage_map, crest_map, odds_client).
    Returns ([], [], {}, {}, None) on any unrecoverable failure.
    odds_client is non-None only on a force_fetch run (used to log API quota).
    """
    stage_map: dict[str, str] = {}
    crest_map: dict[str, str] = {}
    odds_client = None

    if force_fetch:
        # Phase 1: Fetch odds from API
        # Basketball needs the spreads market in addition to h2h + totals
        extra_markets = ["spreads"] if league.sport_type == "basketball" else []
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

        # Tennis and basketball leagues: no fixture history needed — return early after odds fetch
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
                    for ev in upcoming_events:
                        logger.info("    · %s vs %s  (%s)", ev["home_team"], ev["away_team"], ev["commence_time"].date())
                return [], [], {}, {}, odds_client
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
                for ev in upcoming_events:
                    logger.info("    · %s vs %s  (%s)", ev["home_team"], ev["away_team"], ev["commence_time"].date())
            return [], [], {}, {}, odds_client

        if not upcoming_events:
            logger.debug("[%s] No upcoming matches with Winamax odds — skipping.", league.key)
            return [], [], {}, {}, None

        # Phase 2: Upsert matches + odds (skip odds for live matches)
        with Session(engine) as session:
            for event in upcoming_events:
                upsert_match(session, event, league.key)
                if not is_live(event["commence_time"]):
                    upsert_odds(session, event)
            session.commit()

        # Phase 3: Fetch finished fixtures from ESPN
        espn = ESPNSoccerClient()
        season_start = date(season, 7, 1)
        raw_fixtures = espn.fetch_fixtures(season_start, date.today(), leagues=[league.key])

        if not raw_fixtures:
            logger.warning(
                "[%s] No finished fixtures found — season may not have started yet. Skipping.",
                league.key,
            )
            return [], [], {}, {}, None

        with Session(engine) as session:
            upsert_fixtures(session, raw_fixtures, league.key, season)
            session.commit()

        # Also fetch prior season for multi-season H2H context (non-fatal)
        prior_season = season - 1
        try:
            prior_fixtures = espn.fetch_fixtures(
                date(prior_season, 7, 1), date(season, 6, 30), leagues=[league.key],
            )
            if prior_fixtures:
                with Session(engine) as session:
                    upsert_fixtures(session, prior_fixtures, league.key, prior_season)
                    session.commit()
                logger.debug("[%s] Fetched %d prior-season fixtures for H2H.", league.key, len(prior_fixtures))
        except Exception as e:
            logger.debug("[%s] Could not fetch prior-season fixtures (%d): %s", league.key, prior_season, e)

        # Build crest map from ESPN logo URLs and persist to JSON
        from models.features import resolve_team_name
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

    else:
        # Load from DB — no API calls
        p = Path(cfg.football_crest_map_path)
        crest_map = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        with Session(engine) as session:
            upcoming_events = load_upcoming_events_from_db(session, league.key)
            raw_fixtures = load_raw_fixtures_from_db(session, league.key, season)

        if not upcoming_events:
            logger.debug("[%s] No upcoming matches in DB — skipping.", league.key)
            return [], [], {}, {}, None

        if not raw_fixtures:
            logger.warning(
                "[%s] No finished fixtures in DB — run with --fetch to fetch from API. Skipping.",
                league.key,
            )
            return [], [], {}, {}, None

    return upcoming_events, raw_fixtures, stage_map, crest_map, odds_client
