"""
Pipeline package — per-league orchestration and settlement helpers.
"""

import logging

from config import LeagueConfig
from models.sport_evaluators import EVALUATORS
from pipeline.fetchers import FETCHERS
from pipeline.settlement import _fetch_org_settlement_fixtures, _merge_settlement_fixtures, settle_all_sports

logger = logging.getLogger(__name__)

__all__ = [
    "run_league_pipeline",
    "settle_all_sports",
    "_fetch_org_settlement_fixtures",
    "_merge_settlement_fixtures",
]


def run_league_pipeline(
    league: LeagueConfig,
    cfg,
    name_map: dict,
    dry_run: bool = False,
) -> tuple[list[dict], list[dict], int, list]:
    """
    Runs the full extraction → evaluation pipeline for one league.
    Returns (signals, raw_fixtures, n_upcoming, dry_run_events).
    All lists are empty and n_upcoming is 0 on any recoverable failure.
    """
    fetcher = FETCHERS.get(league.sport_type)
    evaluator = EVALUATORS.get(league.sport_type)
    if fetcher is None or evaluator is None:
        logger.warning("[%s] Unknown sport_type %r — skipping.", league.key, league.sport_type)
        return [], [], 0, []

    result = fetcher.fetch(league, cfg, name_map, dry_run)

    n_upcoming = len(result.upcoming_events)
    quota = getattr(result.odds_client, "quota_remaining", None)
    quota_str = f"  quota: {quota}" if quota is not None else ""

    if quota_str:
        logger.debug("  %s%s", league.display_name, quota_str)

    if dry_run or not result.upcoming_events:
        return [], [], n_upcoming, result.upcoming_events if dry_run else []

    signals = evaluator.evaluate(
        result.upcoming_events, league, cfg, name_map,
        raw_fixtures=result.raw_fixtures,
        stage_map=result.stage_map,
        crest_map=result.crest_map,
        features=result.features,
        round_map=result.round_map,
        seed_map=result.seed_map,
        short_name_map=result.short_name_map,
    )

    for f in result.raw_fixtures:
        f["league_key"] = league.key

    return signals, result.raw_fixtures, n_upcoming, []
