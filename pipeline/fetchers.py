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
    seed_map:        dict | None = None   # tennis only; {frozenset({p1, p2}): {"home": int|None, "away": int|None}}
    short_name_map:  dict | None = None   # tennis only; {full_name: short_name}
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
        name_map: dict,
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
        name_map: dict,
        dry_run: bool,
    ) -> FetchResult:
        season = league.season_override if league.season_override is not None else _current_season()
        logger.debug(
            "--- League: %s (key=%s, season=%d) ---",
            league.display_name, league.key, season,
        )
        upcoming_events, raw_fixtures, stage_map, crest_map, odds_client = fetch_league_data(
            league, cfg, name_map, season=season, dry_run=dry_run,
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

        features = build_features(raw_fixtures, name_map, league, cfg)
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
    """Fetches live odds for tennis; builds round and seed maps once per run."""

    _round_map_cache:      dict | None = None  # class-level: shared across all league iterations
    _seed_map_cache:       dict | None = None
    _short_name_map_cache: dict | None = None

    def fetch(
        self,
        league: LeagueConfig,
        cfg,
        name_map: dict,
        dry_run: bool,
    ) -> FetchResult:
        season = league.season_override if league.season_override is not None else _current_season()
        upcoming_events, _, _, _, odds_client = fetch_league_data(
            league, cfg, name_map, season=season, dry_run=dry_run,
        )

        if dry_run:
            return FetchResult(upcoming_events=upcoming_events, odds_client=odds_client)

        if TennisFetcher._round_map_cache is None:
            TennisFetcher._round_map_cache, TennisFetcher._seed_map_cache, TennisFetcher._short_name_map_cache = _build_tennis_maps()

        return FetchResult(
            upcoming_events=upcoming_events,
            round_map=TennisFetcher._round_map_cache,
            seed_map=TennisFetcher._seed_map_cache,
            short_name_map=TennisFetcher._short_name_map_cache,
            odds_client=odds_client,
        )


class NBAFetcher:
    """Fetches live NBA odds; builds stage map once per run."""

    def fetch(
        self,
        league: LeagueConfig,
        cfg,
        name_map: dict,
        dry_run: bool,
    ) -> FetchResult:
        upcoming_events, _, _, _, odds_client = fetch_league_data(
            league, cfg, name_map, season=0, dry_run=dry_run,
        )

        if dry_run:
            return FetchResult(upcoming_events=upcoming_events, odds_client=odds_client)

        stage_map, short_name_map = _build_nba_maps()
        return FetchResult(
            upcoming_events=upcoming_events,
            stage_map=stage_map,
            short_name_map=short_name_map,
            odds_client=odds_client,
        )


# ---------------------------------------------------------------------------
# Stage/round map builders
# ---------------------------------------------------------------------------

def _build_tennis_maps() -> tuple[dict, dict, dict]:
    """Fetches ESPN upcoming tennis matches; returns (round_map, seed_map, short_name_map).

    round_map:       {frozenset({p1, p2}): compact_round}
    seed_map:        {frozenset({p1, p2}): {"home": int|None, "away": int|None}}
    short_name_map:  {full_name: short_name}  (e.g. "Carlos Alcaraz" → "C. Alcaraz")
    """
    try:
        matches = ESPNTennisClient().fetch_upcoming_matches(days_ahead=14)
        round_map = {
            frozenset({m.home_team.lower(), m.away_team.lower()}): m.metadata["round"]
            for m in matches
            if m.metadata.get("round")
        }
        seed_map = {
            frozenset({m.home_team.lower(), m.away_team.lower()}): {
                "home": m.metadata.get("home_seed"),
                "away": m.metadata.get("away_seed"),
            }
            for m in matches
            if m.metadata.get("home_seed") is not None or m.metadata.get("away_seed") is not None
        }
        short_name_map: dict[str, str] = {}
        for m in matches:
            if m.metadata.get("home_short_name"):
                short_name_map[m.home_team] = m.metadata["home_short_name"]
            if m.metadata.get("away_short_name"):
                short_name_map[m.away_team] = m.metadata["away_short_name"]
        return round_map, seed_map, short_name_map
    except Exception as e:
        logger.debug("Tennis maps fetch failed (non-fatal): %s", e)
        return {}, {}, {}


def _build_nba_maps() -> tuple[dict, dict]:
    """Fetches ESPN upcoming NBA games; returns (stage_map, short_name_map).

    stage_map:      {frozenset({home, away}): stage_label}
    short_name_map: {full_name: short_name}  (e.g. "Charlotte Hornets" → "Hornets")
    """
    try:
        matches = ESPNBasketballClient().fetch_upcoming_matches(days_ahead=21)
        stage_map = {
            frozenset({m.home_team.lower(), m.away_team.lower()}): m.metadata["stage"]
            for m in matches
            if m.metadata.get("stage")
        }
        short_name_map: dict[str, str] = {}
        for m in matches:
            if m.metadata.get("home_short_name"):
                short_name_map[m.home_team] = m.metadata["home_short_name"]
            if m.metadata.get("away_short_name"):
                short_name_map[m.away_team] = m.metadata["away_short_name"]
        return stage_map, short_name_map
    except Exception as e:
        logger.debug("NBA maps fetch failed (non-fatal): %s", e)
        return {}, {}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

FETCHERS: dict[str, LeagueFetcher] = {
    "football":   FootballFetcher(),
    "tennis":     TennisFetcher(),
    "basketball": NBAFetcher(),
}
