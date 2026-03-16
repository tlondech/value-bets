"""
Pipeline package — per-league orchestration and settlement helpers.
"""

import logging

from config import LeagueConfig, _current_season
from models.tennis_model import evaluate_tennis_match
from pipeline.evaluate import build_features, enrich_with_news, evaluate_matches
from pipeline.fetch import fetch_league_data
from pipeline.helpers import build_leg2_map
from pipeline.settlement import _fetch_org_settlement_fixtures, _merge_settlement_fixtures

logger = logging.getLogger(__name__)

__all__ = [
    "run_league_pipeline",
    "_fetch_org_settlement_fixtures",
    "_merge_settlement_fixtures",
]

_SURFACE_KEYWORDS = {
    "Clay": ["clay", "roland", "french", "monte", "madrid", "rome", "barcelona"],
    "Grass": ["grass", "wimbledon", "queens", "halle", "eastbourne", "hertogenbosch"],
}


def _infer_surface(display_name: str) -> str:
    lower = display_name.lower()
    for surface, keywords in _SURFACE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return surface
    return "Hard"  # default


def _evaluate_tennis_league(
    upcoming_events: list[dict],
    league: LeagueConfig,
    cfg,
) -> list[dict]:
    is_wta = league.odds_sport.startswith("tennis_wta_")
    ratings = cfg.wta_elo if is_wta else cfg.atp_elo
    if not ratings:
        logger.warning("[%s] No Elo ratings available — skipping.", league.display_name)
        return []

    surface = _infer_surface(league.display_name)
    logger.debug("[%s] Inferred surface: %s", league.display_name, surface)

    value_bets = []
    for event in upcoming_events:
        player1 = event["home_team"]
        player2 = event["away_team"]
        bets = evaluate_tennis_match(
            player1=player1,
            player2=player2,
            surface=surface,
            p1_odds=event["home_odds"],
            p2_odds=event["away_odds"],
            ratings=ratings,
            ev_threshold=cfg.ev_threshold,
        )
        if bets:
            value_bets.append({
                "league_key":  league.key,
                "league_name": league.display_name,
                "home_team":   player1,
                "away_team":   player2,
                "kickoff":     event["commence_time"].isoformat(),
                "bets":        sorted(bets, key=lambda b: b["ev"], reverse=True),
            })
    return value_bets


def run_league_pipeline(
    league: LeagueConfig,
    cfg,
    engine,
    name_map: dict,
    force_fetch: bool = False,
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Runs the full extraction → evaluation pipeline for one league.
    Returns (value_bets, raw_fixtures). Both lists are empty on any recoverable failure.
    """
    if (
        not dry_run
        and league.fd_code is None
        and league.fdo_code is None
        and league.sport_type == "football"
    ):
        logger.debug("[%s] No data source configured — skipping.", league.key)
        return [], []

    season = league.season_override if league.season_override is not None else _current_season()
    logger.debug(
        "--- League: %s (key=%s, season=%d) ---",
        league.display_name, league.key, season,
    )

    # Tennis leagues always force-fetch (no DB caching)
    effective_force_fetch = True if (dry_run or league.sport_type == "tennis") else force_fetch

    upcoming_events, raw_fixtures, stage_map, crest_map, odds_client = fetch_league_data(
        league, cfg, engine, name_map, force_fetch=effective_force_fetch, season=season, dry_run=dry_run,
    )

    if league.sport_type == "tennis":
        if dry_run:
            return [], []
        value_bets = _evaluate_tennis_league(upcoming_events, league, cfg)
        n_bets = sum(len(m["bets"]) for m in value_bets)
        quota = odds_client.quota_remaining if odds_client is not None else None
        logger.info(
            "%-14s  %2d upcoming  tennis (Elo)  → %2d value bets   API quota: %s",
            f"[{league.display_name}]",
            len(upcoming_events), n_bets,
            quota if quota is not None else "—",
        )
        return value_bets, []

    if dry_run:
        return [], []
    if not upcoming_events:
        return [], []

    leg2_map = build_leg2_map(upcoming_events, raw_fixtures, name_map, league.key)
    if leg2_map:
        logger.info("[%s] Detected %d Leg 2 fixture(s).", league.display_name, len(leg2_map))

    features = build_features(raw_fixtures, engine, name_map, league, cfg, season)
    features["leg2_map"] = leg2_map

    match_bets, n_skipped = evaluate_matches(
        upcoming_events, league, cfg, name_map, stage_map, crest_map, features,
    )

    value_bets = [m for m in match_bets.values() if m["bets"]]
    for m in value_bets:
        m["bets"].sort(key=lambda b: b["ev"], reverse=True)

    enrich_with_news(value_bets, cfg)

    n_bets = sum(len(m["bets"]) for m in value_bets)
    skipped_note = f"  ⚠ {n_skipped} matches skipped" if n_skipped else ""
    quota = odds_client.quota_remaining if odds_client is not None else None
    logger.info(
        "%-14s  %2d upcoming  %3d past fixtures  → %2d value bets   API quota: %s%s",
        f"[{league.display_name}]",
        len(upcoming_events), len(raw_fixtures), n_bets,
        quota if quota is not None else "—",
        skipped_note,
    )

    for f in raw_fixtures:
        f["league_key"] = league.key
    return value_bets, raw_fixtures
