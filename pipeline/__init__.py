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
    engine,
    name_map: dict,
    force_fetch: bool = False,
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Runs the full extraction → evaluation pipeline for one league.
    Returns (signals, raw_fixtures). Both lists are empty on any recoverable failure.
    """
    fetcher = FETCHERS.get(league.sport_type)
    evaluator = EVALUATORS.get(league.sport_type)
    if fetcher is None or evaluator is None:
        logger.warning("[%s] Unknown sport_type %r — skipping.", league.key, league.sport_type)
        return [], []

    result = fetcher.fetch(league, cfg, engine, name_map, force_fetch, dry_run)

    n_upcoming = len(result.upcoming_events)
    quota = getattr(result.odds_client, "quota_remaining", None)
    quota_str = f"  quota: {quota}" if quota is not None else ""

    if dry_run or not result.upcoming_events:
        logger.info("  %-26s  [FETCH]    %2d matches%s", league.display_name, n_upcoming, quota_str)
        return [], []

    logger.info("  %-26s  [FETCH]    %2d matches%s", league.display_name, n_upcoming, quota_str)

    signals = evaluator.evaluate(
        result.upcoming_events, league, cfg, name_map,
        raw_fixtures=result.raw_fixtures,
        stage_map=result.stage_map,
        crest_map=result.crest_map,
        features=result.features,
        round_map=result.round_map,
    )

    n_signals = sum(len(m.get("signals", [])) for m in signals)
    logger.info("  %-26s  [EVALUATE] %2d signals", league.display_name, n_signals)

    for f in result.raw_fixtures:
        f["league_key"] = league.key

    return signals, result.raw_fixtures
