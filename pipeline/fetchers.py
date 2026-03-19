"""Sport-specific fetch strategies.

Each concrete class handles the data-gathering phase for one sport and returns
a ``FetchResult``.  The ``FETCHERS`` registry maps ``LeagueConfig.sport_type``
strings to the appropriate singleton.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Protocol

from config import LeagueConfig, _current_season
from extractors.espn_basketball_client import ESPNBasketballClient
from extractors.espn_tennis_client import ESPNTennisClient
from pipeline.evaluate import build_features
from pipeline.fetch import fetch_league_data
from pipeline.helpers import build_leg2_map

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data contract
# ---------------------------------------------------------------------------

@dataclass
class FetchResult:
    """Uniform return type from any LeagueFetcher.fetch() call."""
    upcoming_events: list[dict] = field(default_factory=list)
    raw_fixtures:    list[dict] = field(default_factory=list)
    stage_map:       dict       = field(default_factory=dict)
    crest_map:       dict       = field(default_factory=dict)
    round_map:       dict | None = None   # tennis only
    features:        dict       = field(default_factory=dict)  # football only
    odds_client:     object | None = None


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

class LeagueFetcher(Protocol):
    """Contract for sport-specific data fetch and preparation strategies."""

    def fetch(
        self,
        league: LeagueConfig,
        cfg,
        engine,
        name_map: dict,
        force_fetch: bool,
        dry_run: bool,
    ) -> FetchResult: ...


# ---------------------------------------------------------------------------
# Concrete fetchers
# ---------------------------------------------------------------------------

class FootballFetcher:
    """Fetches ESPN fixture history, builds features and leg-2 context."""

    def fetch(
        self,
        league: LeagueConfig,
        cfg,
        engine,
        name_map: dict,
        force_fetch: bool,
        dry_run: bool,
    ) -> FetchResult:
        season = league.season_override if league.season_override is not None else _current_season()
        logger.debug(
            "--- League: %s (key=%s, season=%d) ---",
            league.display_name, league.key, season,
        )
        upcoming_events, raw_fixtures, stage_map, crest_map, odds_client = fetch_league_data(
            league, cfg, engine, name_map,
            force_fetch=force_fetch, season=season, dry_run=dry_run,
        )

        if dry_run or not upcoming_events:
            return FetchResult(
                upcoming_events=upcoming_events,
                raw_fixtures=raw_fixtures,
                stage_map=stage_map,
                crest_map=crest_map,
                odds_client=odds_client,
            )

        leg2_map = build_leg2_map(upcoming_events, raw_fixtures, name_map, league.key)
        if leg2_map:
            logger.info("[%s] Detected %d Leg 2 fixture(s).", league.display_name, len(leg2_map))

        features = build_features(raw_fixtures, engine, name_map, league, cfg, season)
        features["leg2_map"] = leg2_map

        return FetchResult(
            upcoming_events=upcoming_events,
            raw_fixtures=raw_fixtures,
            stage_map=stage_map,
            crest_map=crest_map,
            features=features,
            odds_client=odds_client,
        )


class TennisFetcher:
    """Fetches live odds for tennis; builds round map once per run."""

    _round_map_cache: dict | None = None  # class-level: shared across all league iterations

    def fetch(
        self,
        league: LeagueConfig,
        cfg,
        engine,
        name_map: dict,
        force_fetch: bool,
        dry_run: bool,
    ) -> FetchResult:
        season = league.season_override if league.season_override is not None else _current_season()
        effective_force_fetch = True if dry_run else force_fetch
        upcoming_events, _, _, _, odds_client = fetch_league_data(
            league, cfg, engine, name_map,
            force_fetch=effective_force_fetch, season=season, dry_run=dry_run,
        )

        if dry_run:
            return FetchResult(upcoming_events=upcoming_events, odds_client=odds_client)

        if TennisFetcher._round_map_cache is None:
            TennisFetcher._round_map_cache = _build_tennis_round_map()

        return FetchResult(
            upcoming_events=upcoming_events,
            round_map=TennisFetcher._round_map_cache,
            odds_client=odds_client,
        )


class NBAFetcher:
    """Fetches live NBA odds; builds stage map once per run."""

    def fetch(
        self,
        league: LeagueConfig,
        cfg,
        engine,
        name_map: dict,
        force_fetch: bool,
        dry_run: bool,
    ) -> FetchResult:
        upcoming_events, _, _, _, odds_client = fetch_league_data(
            league, cfg, engine, name_map,
            force_fetch=True, season=0, dry_run=dry_run,
        )

        if dry_run:
            return FetchResult(upcoming_events=upcoming_events, odds_client=odds_client)

        stage_map = _build_nba_stage_map()
        return FetchResult(
            upcoming_events=upcoming_events,
            stage_map=stage_map,
            odds_client=odds_client,
        )


# ---------------------------------------------------------------------------
# Stage/round map builders
# ---------------------------------------------------------------------------

def _build_tennis_round_map() -> dict:
    """Fetches ESPN upcoming tennis matches; returns {frozenset({p1, p2}): compact_round}."""
    try:
        matches = ESPNTennisClient().fetch_upcoming_matches(days_ahead=14)
        return {
            frozenset({m.home_team.lower(), m.away_team.lower()}): m.metadata["round"]
            for m in matches
            if m.metadata.get("round")
        }
    except Exception as e:
        logger.debug("Tennis round map fetch failed (non-fatal): %s", e)
        return {}


def _build_nba_stage_map() -> dict:
    """Fetches ESPN upcoming NBA games; returns {frozenset({home, away}): stage_label}."""
    try:
        matches = ESPNBasketballClient().fetch_upcoming_matches(days_ahead=21)
        return {
            frozenset({m.home_team.lower(), m.away_team.lower()}): m.metadata["stage"]
            for m in matches
            if m.metadata.get("stage")
        }
    except Exception as e:
        logger.debug("NBA stage map fetch failed (non-fatal): %s", e)
        return {}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

FETCHERS: dict[str, LeagueFetcher] = {
    "football":   FootballFetcher(),
    "tennis":     TennisFetcher(),
    "basketball": NBAFetcher(),
}
