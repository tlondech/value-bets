"""
Pipeline package — per-league orchestration and settlement helpers.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config import LeagueConfig, _current_season
from models.nba_model import evaluate_nba_match
from models.tennis_model import evaluate_tennis_match
from pipeline.evaluate import build_features, enrich_with_news, evaluate_matches
from pipeline.fetch import fetch_league_data
from pipeline.helpers import build_leg2_map, is_live
from pipeline.settlement import _fetch_org_settlement_fixtures, _merge_settlement_fixtures

logger = logging.getLogger(__name__)

__all__ = [
    "run_league_pipeline",
    "_fetch_org_settlement_fixtures",
    "_merge_settlement_fixtures",
]

# ---------------------------------------------------------------------------
# NBA helpers
# ---------------------------------------------------------------------------

def _load_nba_crest_map(path: str) -> dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not k.startswith("_")}


def _resolve_nba_team(winamax_name: str, name_map: dict) -> str | None:
    """Maps a Winamax NBA team display name to an nba_api abbreviation."""
    return name_map.get("nba", {}).get(winamax_name)


def _evaluate_nba_league(
    upcoming_events: list[dict],
    league: LeagueConfig,
    cfg,
    name_map: dict,
) -> list[dict]:
    """
    Evaluates upcoming NBA games using the Gaussian efficiency model.
    Mirrors _evaluate_tennis_league() in structure.
    """
    ratings = cfg.nba_ratings
    crest_map = _load_nba_crest_map(cfg.nba_crest_map_path)
    if not ratings:
        logger.warning("[NBA] No team ratings available — skipping NBA evaluation.")
        return []

    now = datetime.now(timezone.utc)
    started = [e for e in upcoming_events if is_live(e["commence_time"], window_hours=league.live_window_hours)]
    upcoming_events = [e for e in upcoming_events if not is_live(e["commence_time"], window_hours=league.live_window_hours) and e["commence_time"] > now]
    if started:
        logger.info(
            "[NBA] Skipping %d game(s) currently in progress (live window: %.1fh).",
            len(started), league.live_window_hours,
        )

    value_bets = []
    n_unmapped = 0
    for event in upcoming_events:
        home_winamax = event["home_team"]
        away_winamax = event["away_team"]

        home_abbr = _resolve_nba_team(home_winamax, name_map)
        away_abbr = _resolve_nba_team(away_winamax, name_map)
        if not home_abbr or not away_abbr:
            logger.debug(
                "[NBA] Skipping %s vs %s — unmapped team name(s) (check team_name_map.json[\"nba\"]).",
                home_winamax, away_winamax,
            )
            n_unmapped += 1
            continue

        home_r = ratings.get(home_abbr)
        away_r = ratings.get(away_abbr)
        if not home_r or not away_r:
            logger.debug(
                "[NBA] Skipping %s (%s) vs %s (%s) — no ratings data.",
                home_winamax, home_abbr, away_winamax, away_abbr,
            )
            continue

        game_date = event["commence_time"].date()
        home_rest_days: int | None = None
        away_rest_days: int | None = None
        if last := home_r.get("last_game_date"):
            home_rest_days = (game_date - last).days
        if last := away_r.get("last_game_date"):
            away_rest_days = (game_date - last).days

        bets = evaluate_nba_match(
            home_team=home_winamax,
            away_team=away_winamax,
            home_ratings=home_r,
            away_ratings=away_r,
            home_odds=event.get("home_odds"),
            away_odds=event.get("away_odds"),
            over_odds=event.get("over_odds"),
            under_odds=event.get("under_odds"),
            totals_line=event.get("totals_line"),
            spread_home_point=event.get("spread_home_point"),
            spread_home_odds=event.get("spread_home_odds"),
            spread_away_odds=event.get("spread_away_odds"),
            ev_threshold=cfg.ev_threshold,
            max_prob_ratio=cfg.max_prob_ratio,
            min_games=cfg.nba_min_games,
            home_advantage=cfg.nba_home_advantage,
            spread_std=cfg.nba_spread_std,
            total_std=cfg.nba_total_std,
            home_rest_days=home_rest_days,
            away_rest_days=away_rest_days,
        )

        if not bets:
            continue

        # Market-group filtering: keep only the highest-EV bet per market group
        groups: dict[str, dict] = {}
        for b in bets:
            grp = b.get("market_group", b["outcome"])
            if grp not in groups or b["ev"] > groups[grp]["ev"]:
                groups[grp] = b
        filtered_bets = sorted(groups.values(), key=lambda b: b["ev"], reverse=True)
        # Remove internal market_group key before persisting
        for b in filtered_bets:
            b.pop("market_group", None)

        value_bets.append({
            "league_key":      league.key,
            "league_name":     league.display_name,
            "home_team":       home_winamax,
            "away_team":       away_winamax,
            "home_canonical":  home_abbr,
            "away_canonical":  away_abbr,
            "home_crest":      crest_map.get(home_abbr),
            "away_crest":      crest_map.get(away_abbr),
            "home_form":       home_r.get("form"),
            "away_form":       away_r.get("form"),
            "home_rest_days":  home_rest_days,
            "away_rest_days":  away_rest_days,
            "kickoff":         event["commence_time"].isoformat(),
            "sport":           "basketball",
            "bookmaker_link":  event.get("bookmaker_link"),
            "bets":            filtered_bets,
        })

    if n_unmapped:
        logger.warning(
            "[NBA] %d event(s) skipped due to unmapped team names. "
            "Add entries to data/team_name_map.json[\"nba\"].",
            n_unmapped,
        )
    return value_bets

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
    p = Path(cfg.tennis_crest_map_path)
    crest_map = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    if not ratings:
        logger.warning("[%s] No Elo ratings available — skipping.", league.display_name)
        return []

    surface = _infer_surface(league.display_name)
    logger.debug("[%s] Inferred surface: %s", league.display_name, surface)

    now = datetime.now(timezone.utc)
    started = [e for e in upcoming_events if e["commence_time"] <= now]
    upcoming_events = [e for e in upcoming_events if e["commence_time"] > now]
    if started:
        logger.info(
            "[%s] Skipping %d tennis match(es) whose scheduled start has passed.",
            league.display_name, len(started),
        )

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
            max_prob_ratio=cfg.tennis_max_prob_ratio,
            min_matches=cfg.tennis_min_matches,
        )
        # home_win and away_win are mutually exclusive — keep only the highest EV
        if bets:
            bets = [max(bets, key=lambda b: b["ev"])]
            value_bets.append({
                "league_key":      league.key,
                "league_name":     league.display_name,
                "home_team":       player1,
                "away_team":       player2,
                "home_canonical":  player1,
                "away_canonical":  player2,
                "home_crest":      crest_map.get(player1),
                "away_crest":      crest_map.get(player2),
                "surface":         surface,
                "kickoff":         event["commence_time"].isoformat(),
                "sport":           "tennis",
                "bookmaker_link":  event.get("bookmaker_link"),
                "bets":            sorted(bets, key=lambda b: b["ev"], reverse=True),
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
    if league.sport_type == "basketball":
        # season is not used for basketball (no fixture DB); pass 0 as a placeholder
        upcoming_events, _, _, _, odds_client = fetch_league_data(
            league, cfg, engine, name_map, force_fetch=True, season=0, dry_run=dry_run,
        )
        if dry_run:
            return [], []
        value_bets = _evaluate_nba_league(upcoming_events, league, cfg, name_map)
        n_bets = sum(len(m["bets"]) for m in value_bets)
        quota = odds_client.quota_remaining if odds_client is not None else None
        logger.info(
            "%-14s  %2d upcoming  basketball (Gaussian)  → %2d value bets   API quota: %s",
            f"[{league.display_name}]",
            len(upcoming_events), n_bets,
            quota if quota is not None else "—",
        )
        return value_bets, []

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
