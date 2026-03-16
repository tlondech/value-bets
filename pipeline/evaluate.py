"""
Feature building, match evaluation, and team news enrichment.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from config import LeagueConfig
from constants import (
    DIXON_COLES_MIN_FIXTURES,
    DIXON_COLES_XI,
    TEAM_NEWS_CUTOFF_HOURS,
    UCL_PROB_RATIO_CAP,
)
from db.queries import load_all_fixtures_df, load_h2h_fixtures_df
from models.evaluator import evaluate_match
from models.features import (
    build_fixtures_dataframe,
    build_poisson_inputs,
    build_poisson_inputs_dc,
    compute_form,
    compute_league_averages,
    compute_standings,
    fit_dixon_coles,
    resolve_team_name,
)
from pipeline.helpers import get_outcome_label, is_live

logger = logging.getLogger(__name__)


def build_features(
    raw_fixtures: list[dict],
    engine,
    name_map: dict,
    league: LeagueConfig,
    cfg,
    season: int,
) -> dict:
    """
    Builds feature inputs for the evaluation phase.

    Returns a dict with keys:
      dc_params, fixtures_df, league_avgs, all_fixtures_df, h2h_fixtures_df,
      universal_names, rankings, total_matchdays, form_map
    """
    fixtures_df = build_fixtures_dataframe(raw_fixtures)
    league_avgs = compute_league_averages(fixtures_df)
    universal_names = name_map.get("universal_names", {})
    all_fixtures_df = load_all_fixtures_df(engine, universal_names)
    with Session(engine) as session:
        h2h_fixtures_df = load_h2h_fixtures_df(session, league.key, season)
    logger.debug(
        "[%s] League averages — home goals: %.2f | away goals: %.2f",
        league.key, league_avgs["avg_home_goals"], league_avgs["avg_away_goals"],
    )

    dc_params = fit_dixon_coles(fixtures_df, xi=DIXON_COLES_XI, min_fixtures=DIXON_COLES_MIN_FIXTURES)
    if dc_params is not None:
        logger.info(
            "[%s] Dixon-Coles fit: %d teams, %d fixtures, ρ=%.4f, γ=%.4f",
            league.display_name, len(dc_params["attack"]), dc_params["n_fixtures"],
            dc_params["rho"], dc_params["gamma"],
        )
    else:
        logger.warning(
            "[%s] Not enough fixtures for Dixon-Coles (<%d) — using rolling-window fallback.",
            league.display_name, DIXON_COLES_MIN_FIXTURES,
        )

    rankings: dict[str, int] = {}
    total_matchdays: int | None = None
    form_map: dict[str, list[str]] = {}
    if league.fdo_enrich_code:
        standings = compute_standings(raw_fixtures)
        rankings = standings["rankings"]
        total_matchdays = standings["total_matchdays"]
        form_map = compute_form(raw_fixtures)
        logger.debug(
            "[%s] Standings computed: %d teams, total_matchdays=%s.",
            league.key, len(rankings), total_matchdays,
        )

    return {
        "dc_params": dc_params,
        "fixtures_df": fixtures_df,
        "league_avgs": league_avgs,
        "all_fixtures_df": all_fixtures_df,
        "h2h_fixtures_df": h2h_fixtures_df,
        "universal_names": universal_names,
        "rankings": rankings,
        "total_matchdays": total_matchdays,
        "form_map": form_map,
    }


def evaluate_matches(
    upcoming_events: list[dict],
    league: LeagueConfig,
    cfg,
    name_map: dict,
    stage_map: dict[str, str],
    crest_map: dict[str, str],
    features: dict,
) -> tuple[dict[tuple, dict], int]:
    """
    Runs the EV evaluation loop over all non-live upcoming events.

    Returns match_bets dict keyed by (home_winamax, away_winamax, kickoff_iso).
    """
    rankings = features["rankings"]
    form_map = features["form_map"]
    dc_params = features["dc_params"]
    fixtures_df = features["fixtures_df"]
    league_avgs = features["league_avgs"]
    all_fixtures_df = features["all_fixtures_df"]
    h2h_fixtures_df = features["h2h_fixtures_df"]
    universal_names = features["universal_names"]

    live_count = sum(1 for e in upcoming_events if is_live(e["commence_time"]))
    if live_count:
        logger.info("[%s] Skipping %d live match(es).", league.display_name, live_count)
    upcoming_events = [e for e in upcoming_events if not is_live(e["commence_time"])]

    # leg2_map is built before this call and passed via features or separately;
    # import here to avoid circular dependency at module level
    from pipeline.helpers import build_leg2_map
    leg2_map = features.get("leg2_map", {})

    match_bets: dict[tuple, dict] = {}
    n_skipped = 0
    for event in upcoming_events:
        home_winamax = event["home_team"]
        away_winamax = event["away_team"]

        home_canonical = resolve_team_name(home_winamax, name_map, league.key)
        away_canonical = resolve_team_name(away_winamax, name_map, league.key)

        if home_canonical is None or away_canonical is None:
            logger.debug("[%s] Skipping %s vs %s (unmapped team name).", league.key, home_winamax, away_winamax)
            n_skipped += 1
            continue

        event["stage"] = stage_map.get(f"{home_canonical}|{away_canonical}") or event.get("stage")
        event["home_rank"] = rankings.get(home_canonical)
        event["away_rank"] = rankings.get(away_canonical)
        event["home_form"] = form_map.get(home_canonical)
        event["away_form"] = form_map.get(away_canonical)
        event["home_crest"] = crest_map.get(home_canonical)
        event["away_crest"] = crest_map.get(away_canonical)

        home_universal = universal_names.get(home_canonical, home_canonical)
        away_universal = universal_names.get(away_canonical, away_canonical)

        leg2_context = leg2_map.get((home_canonical, away_canonical))

        poisson_inputs = None
        if dc_params is not None:
            poisson_inputs = build_poisson_inputs_dc(
                home_canonical, away_canonical,
                dc_params=dc_params,
                match_date=event["commence_time"],
                all_fixtures_df=all_fixtures_df,
                h2h_fixtures_df=h2h_fixtures_df,
                home_universal=home_universal,
                away_universal=away_universal,
                leg2_context=leg2_context,
            )
        if poisson_inputs is None:
            poisson_inputs = build_poisson_inputs(
                home_canonical, away_canonical, fixtures_df, league_avgs, cfg.rolling_window,
                match_date=event["commence_time"],
                all_fixtures_df=all_fixtures_df,
                h2h_fixtures_df=h2h_fixtures_df,
                home_universal=home_universal,
                away_universal=away_universal,
                leg2_context=leg2_context,
            )

        if poisson_inputs is None:
            logger.debug(
                "[%s] Skipping %s vs %s (insufficient fixture history).",
                league.key, home_winamax, away_winamax,
            )
            n_skipped += 1
            continue

        result = evaluate_match(
            home_lambda=poisson_inputs["home_lambda"],
            away_lambda=poisson_inputs["away_lambda"],
            home_odds=event["home_odds"],
            draw_odds=event["draw_odds"],
            away_odds=event["away_odds"],
            ev_threshold=cfg.ev_threshold,
            max_goals=cfg.poisson_max_goals,
            over_odds=event.get("over_odds"),
            under_odds=event.get("under_odds"),
            totals_line=event.get("totals_line"),
            rho=dc_params["rho"] if dc_params is not None else 0.0,
        )

        if not result["value_bets"]:
            continue

        over_key  = result["over_key"]
        under_key = result["under_key"]
        outcome_map = {
            "home_win": (result["home_win_prob"], event["home_odds"],          result["home_ev"]),
            "draw":     (result["draw_prob"],     event["draw_odds"],          result["draw_ev"]),
            "away_win": (result["away_win_prob"], event["away_odds"],          result["away_ev"]),
            over_key:   (result[over_key + "_prob"],  event.get("over_odds"),  result[over_key + "_ev"]),
            under_key:  (result[under_key + "_prob"], event.get("under_odds"), result[under_key + "_ev"]),
        }
        kickoff_iso = event["commence_time"].isoformat()
        key = (home_winamax, away_winamax, kickoff_iso)
        if key not in match_bets:
            match_bets[key] = {
                "league_key":    league.key,
                "league_name":   league.display_name,
                "home_team":     home_winamax,
                "away_team":     away_winamax,
                "kickoff":       kickoff_iso,
                "kickoff_local": event["commence_time"].astimezone().strftime("%H:%M"),
                "stage":         event.get("stage"),
                "home_rank":     event.get("home_rank"),
                "away_rank":     event.get("away_rank"),
                "home_form":     event.get("home_form"),
                "away_form":     event.get("away_form"),
                "home_crest":    event.get("home_crest"),
                "away_crest":    event.get("away_crest"),
                "h2h_used":       poisson_inputs.get("h2h_used", False),
                "home_rest_days": poisson_inputs.get("home_rest_days"),
                "away_rest_days": poisson_inputs.get("away_rest_days"),
                "is_second_leg":  bool(leg2_context),
                "leg1_result":    leg2_context["leg1_result"] if leg2_context else None,
                "agg_home":       leg2_context["agg_home"]    if leg2_context else None,
                "agg_away":       leg2_context["agg_away"]    if leg2_context else None,
                "home_canonical": home_canonical,
                "away_canonical": away_canonical,
                "bets":          [],
            }

        _market_groups = [
            {"home_win", "draw", "away_win"},
            {"over_2_5", "under_2_5"},
        ]
        ratio_cap = UCL_PROB_RATIO_CAP if league.key == "ucl" else cfg.max_prob_ratio
        filtered_value_bets = []
        for group in _market_groups:
            candidates = [
                o for o in result["value_bets"]
                if o in group
                and outcome_map[o][1] is not None
                and outcome_map[o][0] * outcome_map[o][1] <= ratio_cap
            ]
            if candidates:
                filtered_value_bets.append(max(candidates, key=lambda o: outcome_map[o][2]))

        for outcome in filtered_value_bets:
            true_prob, odds, ev = outcome_map[outcome]
            if odds is None:
                continue
            match_bets[key]["bets"].append({
                "outcome":       outcome,
                "outcome_label": get_outcome_label(outcome),
                "odds":          odds,
                "true_prob":     round(true_prob, 4),
                "ev":            round(ev, 4),
            })

    if n_skipped:
        logger.debug("[%s] %d match(es) skipped during evaluation.", league.key, n_skipped)

    return match_bets, n_skipped


def enrich_with_news(value_bets: list[dict], cfg) -> None:
    """
    Fetches team news for high-EV matches within 24h of kickoff.
    Mutates value_bets in place.
    """
    if not cfg.news_api_key:
        return

    from constants import EV_NEWS_THRESHOLD, NEWS_DAYS_BACK_DEFAULT
    from extractors.team_news import fetch_team_news

    now_utc = datetime.now(timezone.utc)
    for match in value_bets:
        if not any(b["ev"] >= EV_NEWS_THRESHOLD for b in match["bets"]):
            continue
        kickoff = datetime.fromisoformat(match["kickoff"])
        if kickoff.tzinfo is None:
            kickoff = kickoff.replace(tzinfo=timezone.utc)
        hours_until_kickoff = (kickoff - now_utc).total_seconds() / 3600
        if hours_until_kickoff <= TEAM_NEWS_CUTOFF_HOURS:
            days_back = max(
                match.get("home_rest_days") or NEWS_DAYS_BACK_DEFAULT,
                match.get("away_rest_days") or NEWS_DAYS_BACK_DEFAULT,
            )
            match["team_news"] = fetch_team_news(
                match["home_team"], match["away_team"], cfg.news_api_key,
                days_back=days_back,
            )
            logger.info(
                "[%s] Fetched team news for %s vs %s (EV %.0f%%, kickoff in %.1fh)",
                match["league_name"],
                match["home_team"], match["away_team"],
                max(b["ev"] for b in match["bets"]) * 100,
                hours_until_kickoff,
            )
