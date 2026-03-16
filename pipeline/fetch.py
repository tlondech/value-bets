"""
Data fetching and loading for a single league.

In force_fetch mode: hits The Odds API, football-data.org, and football-data.co.uk,
then persists everything to SQLite.
In cached mode: loads from SQLite only.
"""

import json
import logging
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
from extractors.footballdata_client import FootballDataClient, FootballDataError
from extractors.footballdataorg_client import FootballDataOrgClient, FootballDataOrgError
from extractors.odds import OddsAPIClient
from models.features import resolve_team_name
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
        odds_client = OddsAPIClient(
            api_key=cfg.odds_api_key,
            sport=league.odds_sport,
            region=cfg.odds_region,
            bookmaker=cfg.odds_bookmaker,
            market=cfg.odds_market,
            odds_format=cfg.odds_format,
            totals_bookmakers=cfg.odds_totals_bookmakers,
        )
        upcoming_events = odds_client.fetch_upcoming_odds()

        # Tennis leagues: no fixture history needed — return early after odds fetch
        if league.sport_type == "tennis":
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

        # Phase 1b: Fetch stage/matchweek enrichment (non-fatal)
        enrich_code = league.fdo_code or league.fdo_enrich_code
        if enrich_code and cfg.fdo_api_key:
            fdo_enrich = FootballDataOrgClient(enrich_code, season, cfg.fdo_api_key)
            try:
                stage_map, crest_map, _ = fdo_enrich.fetch_stage_map(name_map, league.key)
                logger.debug("[%s] Stage map: %d entries, %d crests.", league.key, len(stage_map), len(crest_map))
                # Persist crests so no-force runs can use them
                if crest_map:
                    p = Path(cfg.football_crest_map_path)
                    existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
                    existing.update(crest_map)
                    p.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                logger.warning("[%s] Stage enrichment failed: %s", league.display_name, e)

        # Apply stage to events before upsert so it's persisted in SQLite
        # and available on subsequent non-force runs via load_upcoming_events_from_db.
        if stage_map:
            for event in upcoming_events:
                home_c = resolve_team_name(event["home_team"], name_map, league.key)
                away_c = resolve_team_name(event["away_team"], name_map, league.key)
                if home_c and away_c:
                    stage = stage_map.get(f"{home_c}|{away_c}")
                    if stage:
                        event["stage"] = stage

        # Phase 2: Upsert matches + odds (skip odds for live matches)
        with Session(engine) as session:
            for event in upcoming_events:
                upsert_match(session, event, league.key)
                if not is_live(event["commence_time"]):
                    upsert_odds(session, event)
            session.commit()

        # Phase 3: Fetch finished fixtures from API
        try:
            if league.fd_code is not None:
                raw_fixtures = FootballDataClient(league.fd_code, season).fetch_fixtures()
            else:
                raw_fixtures = FootballDataOrgClient(league.fdo_code, season, cfg.fdo_api_key).fetch_fixtures()  # type: ignore[arg-type]
        except (FootballDataError, FootballDataOrgError) as e:
            logger.error("[%s] Failed to fetch fixtures: %s — skipping league.", league.display_name, e)
            return [], [], {}, {}, None

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
            if league.fd_code is not None:
                prior_fixtures = FootballDataClient(league.fd_code, prior_season).fetch_fixtures()
            else:
                prior_fixtures = FootballDataOrgClient(league.fdo_code, prior_season, cfg.fdo_api_key).fetch_fixtures()  # type: ignore[arg-type]
            if prior_fixtures:
                with Session(engine) as session:
                    upsert_fixtures(session, prior_fixtures, league.key, prior_season)
                    session.commit()
                logger.debug("[%s] Fetched %d prior-season fixtures for H2H.", league.key, len(prior_fixtures))
        except Exception as e:
            logger.debug("[%s] Could not fetch prior-season fixtures (%d): %s", league.key, prior_season, e)

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
