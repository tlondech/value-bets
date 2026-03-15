"""
Per-league pipeline orchestration and settlement helpers.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from config import LeagueConfig, _current_season
from db.schema import Match
from db.queries import (
    load_all_fixtures_df,
    load_h2h_fixtures_df,
    load_raw_fixtures_from_db,
    load_upcoming_events_from_db,
    upsert_fixtures,
    upsert_match,
    upsert_odds,
)
from extractors.footballdata_client import FootballDataClient, FootballDataError
from extractors.footballdataorg_client import FootballDataOrgClient, FootballDataOrgError
from extractors.odds import OddsAPIClient
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

logger = logging.getLogger(__name__)

_OUTCOME_LABELS = {
    "home_win":  "Home Win",
    "draw":      "Draw",
    "away_win":  "Away Win",
    "over_2_5":  "Over 2.5",
    "under_2_5": "Under 2.5",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_live(commence_time: datetime, window_hours: float = 2.5) -> bool:
    """Return True if the match is currently in progress (kicked off but not yet finished)."""
    now = datetime.now(timezone.utc)
    return commence_time <= now < commence_time + timedelta(hours=window_hours)


def build_leg2_map(
    upcoming_events: list[dict],
    raw_fixtures: list[dict],
    name_map: dict,
    league_key: str,
) -> dict[tuple, dict]:
    """
    Returns {(home_canonical, away_canonical): leg2_context} for UCL Leg 2 fixtures.

    A match is Leg 2 when a finished fixture exists between the same two teams with
    reversed home/away roles (i.e. Leg 1). No stage filter is applied — in the current
    UCL league-phase format each team faces each opponent only once, so a reversed
    finished fixture unambiguously signals a knockout second leg regardless of the
    stage label returned by the API.

    Aggregate going into Leg 2:
      agg_home = leg1.away_goals  (Leg 2 home team was away in Leg 1)
      agg_away = leg1.home_goals  (Leg 2 away team was home in Leg 1)
    """
    if league_key != "ucl":
        return {}

    # Index finished fixtures by (home_canonical, away_canonical) for O(1) lookup
    finished_index: dict[tuple, dict] = {}
    for f in raw_fixtures:
        home_c = resolve_team_name(f["home_team"], name_map, league_key)
        away_c = resolve_team_name(f["away_team"], name_map, league_key)
        if home_c and away_c:
            finished_index[(home_c, away_c)] = f

    leg2_map: dict[tuple, dict] = {}
    for event in upcoming_events:
        home_c = resolve_team_name(event["home_team"], name_map, league_key)
        away_c = resolve_team_name(event["away_team"], name_map, league_key)
        if not home_c or not away_c:
            logger.debug(
                "build_leg2_map: skipping '%s' vs '%s' — name resolution failed (home_c=%r, away_c=%r)",
                event["home_team"], event["away_team"], home_c, away_c,
            )
            continue
        # Leg 2 home team was AWAY in Leg 1 → look for reversed fixture
        leg1 = finished_index.get((away_c, home_c))
        if leg1 is None:
            continue
        agg_home = leg1["away_goals"]   # Leg 2 home team's Leg 1 goals (scored as away)
        agg_away = leg1["home_goals"]   # Leg 2 away team's Leg 1 goals (scored as home)
        leg2_map[(home_c, away_c)] = {
            "is_second_leg": True,
            "leg1_result": {
                "home_team": away_c,
                "away_team": home_c,
                "home_goals": leg1["home_goals"],
                "away_goals": leg1["away_goals"],
            },
            "agg_home": agg_home,
            "agg_away": agg_away,
            "agg_diff": agg_home - agg_away,
        }

    return leg2_map


# ---------------------------------------------------------------------------
# Per-league pipeline
# ---------------------------------------------------------------------------

def run_league_pipeline(
    league: LeagueConfig,
    cfg,
    engine,
    name_map: dict,
    force_fetch: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Runs the full extraction → evaluation pipeline for one league.
    Returns (value_bets, raw_fixtures). Both lists are empty on any recoverable failure.
    """
    if league.fd_code is None and league.fdo_code is None:
        logger.debug("[%s] No data source configured — skipping.", league.key)
        return [], []

    season = league.season_override if league.season_override is not None else _current_season()
    logger.debug(
        "--- League: %s (key=%s, season=%d) ---",
        league.display_name, league.key, season,
    )

    stage_map: dict[str, str] = {}
    crest_map: dict[str, str] = {}
    rankings: dict[str, int] = {}
    total_matchdays: int | None = None
    form_map: dict[str, list[str]] = {}
    odds_client = None

    # Auto-bypass cache when a match is scheduled today (fresh odds always needed)
    if not force_fetch:
        today_utc = datetime.now(timezone.utc).date()
        day_start = datetime.combine(today_utc, datetime.min.time())
        day_end   = datetime.combine(today_utc, datetime.max.time())
        with Session(engine) as _s:
            match_today = _s.query(Match).filter(
                Match.league == league.key,
                Match.status == "upcoming",
                Match.match_date >= day_start,
                Match.match_date <= day_end,
            ).first()
        if match_today:
            logger.info("[%s] Match today detected — bypassing cache.", league.display_name)
            force_fetch = True

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

        if not upcoming_events:
            logger.debug("[%s] No upcoming matches with Winamax odds — skipping.", league.key)
            return [], []

        # Phase 1b: Fetch stage/matchweek enrichment (non-fatal)
        enrich_code = league.fdo_code or league.fdo_enrich_code
        if enrich_code and cfg.fdo_api_key:
            fdo_enrich = FootballDataOrgClient(enrich_code, season, cfg.fdo_api_key)
            try:
                stage_map, crest_map, _ = fdo_enrich.fetch_stage_map(name_map, league.key)
                logger.debug("[%s] Stage map: %d entries, %d crests.", league.key, len(stage_map), len(crest_map))
                # Persist crests so no-force runs can use them
                if crest_map:
                    p = Path(cfg.crest_map_path)
                    existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
                    existing.update(crest_map)
                    p.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                logger.warning("[%s] Stage enrichment failed: %s", league.display_name, e)

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
            return [], []

        if not raw_fixtures:
            logger.warning(
                "[%s] No finished fixtures found — season may not have started yet. Skipping.",
                league.key,
            )
            return [], []

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
        p = Path(cfg.crest_map_path)
        crest_map = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        with Session(engine) as session:
            upcoming_events = load_upcoming_events_from_db(session, league.key)
            raw_fixtures = load_raw_fixtures_from_db(session, league.key, season)

        if not upcoming_events:
            logger.debug("[%s] No upcoming matches in DB — skipping.", league.key)
            return [], []

        if not raw_fixtures:
            logger.warning(
                "[%s] No finished fixtures in DB — run with --fetch to fetch from API. Skipping.",
                league.key,
            )
            return [], []

    # Build Leg 2 aggregate context map (UCL knockout only; no-op for all other leagues)
    leg2_map = build_leg2_map(upcoming_events, raw_fixtures, name_map, league.key)
    if leg2_map:
        logger.info("[%s] Detected %d Leg 2 fixture(s).", league.display_name, len(leg2_map))

    # Compute standings and form from fixture data (domestic leagues only; no extra API call)
    if league.fdo_enrich_code:
        standings = compute_standings(raw_fixtures)
        rankings = standings["rankings"]
        total_matchdays = standings["total_matchdays"]
        form_map = compute_form(raw_fixtures)
        logger.debug("[%s] Standings computed: %d teams, total_matchdays=%s.", league.key, len(rankings), total_matchdays)
    # Augment stage labels with total matchweek count
    if total_matchdays:
        stage_map = {
            k: (f"{v} / {total_matchdays}" if v.startswith("Matchday ") else v)
            for k, v in stage_map.items()
        }

    # Phase 4: Build features
    fixtures_df = build_fixtures_dataframe(raw_fixtures)
    league_avgs = compute_league_averages(fixtures_df)
    universal_names = name_map.get("universal_names", {})
    all_fixtures_df = load_all_fixtures_df(engine, universal_names)  # cross-league, for rest days
    with Session(engine) as session:
        h2h_fixtures_df = load_h2h_fixtures_df(session, league.key, season)  # last 3 seasons, for H2H
    logger.debug(
        "[%s] League averages — home goals: %.2f | away goals: %.2f",
        league.key, league_avgs["avg_home_goals"], league_avgs["avg_away_goals"],
    )

    # Fit Dixon-Coles MLE model (falls back to rolling-window if insufficient data)
    dc_params = fit_dixon_coles(fixtures_df, xi=0.0065, min_fixtures=10)
    if dc_params is not None:
        logger.info(
            "[%s] Dixon-Coles fit: %d teams, %d fixtures, ρ=%.4f, γ=%.4f",
            league.display_name, len(dc_params["attack"]), dc_params["n_fixtures"],
            dc_params["rho"], dc_params["gamma"],
        )
    else:
        logger.warning(
            "[%s] Not enough fixtures for Dixon-Coles (<10) — using rolling-window fallback.",
            league.display_name,
        )

    # Phase 5: Evaluate each match (skip live matches)
    live_count = sum(1 for e in upcoming_events if is_live(e["commence_time"]))
    if live_count:
        logger.info("[%s] Skipping %d live match(es).", league.display_name, live_count)
    upcoming_events = [e for e in upcoming_events if not is_live(e["commence_time"])]

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

        # Attach stage and rankings from enrichment maps
        event["stage"] = stage_map.get(f"{home_canonical}|{away_canonical}") or event.get("stage")
        event["home_rank"] = rankings.get(home_canonical)
        event["away_rank"] = rankings.get(away_canonical)
        event["home_form"] = form_map.get(home_canonical)
        event["away_form"] = form_map.get(away_canonical)
        event["home_crest"] = crest_map.get(home_canonical)
        event["away_crest"] = crest_map.get(away_canonical)

        # Normalize to universal canonical for cross-league rest-day lookup
        home_universal = universal_names.get(home_canonical, home_canonical)
        away_universal = universal_names.get(away_canonical, away_canonical)

        # Look up Leg 2 aggregate context (None for all non-UCL-knockout matches)
        leg2_context = leg2_map.get((home_canonical, away_canonical))

        # Try Dixon-Coles first; fall back to rolling-window if team is unknown
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
            over_2_5_odds=event.get("over_2_5_odds"),
            under_2_5_odds=event.get("under_2_5_odds"),
            rho=dc_params["rho"] if dc_params is not None else 0.0,
        )

        if not result["value_bets"]:
            continue

        outcome_map = {
            "home_win":  (result["home_win_prob"],  event["home_odds"],           result["home_ev"]),
            "draw":      (result["draw_prob"],      event["draw_odds"],           result["draw_ev"]),
            "away_win":  (result["away_win_prob"],  event["away_odds"],           result["away_ev"]),
            "over_2_5":  (result["over_2_5_prob"],  event.get("over_2_5_odds"),   result["over_2_5_ev"]),
            "under_2_5": (result["under_2_5_prob"], event.get("under_2_5_odds"),  result["under_2_5_ev"]),
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
        for outcome in result["value_bets"]:
            true_prob, odds, ev = outcome_map[outcome]
            if odds is None:
                continue
            match_bets[key]["bets"].append({
                "outcome":       outcome,
                "outcome_label": _OUTCOME_LABELS[outcome],
                "odds":          odds,
                "true_prob":     round(true_prob, 4),
                "ev":            round(ev, 4),
            })

    value_bets = [m for m in match_bets.values() if m["bets"]]
    for m in value_bets:
        m["bets"].sort(key=lambda b: b["ev"], reverse=True)

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
    # Tag fixtures with their league so settle_supabase_bets can canonicalize names
    for f in raw_fixtures:
        f["league_key"] = league.key
    return value_bets, raw_fixtures


# ---------------------------------------------------------------------------
# Settlement helpers — dual-source (football-data.org supplements .co.uk)
# ---------------------------------------------------------------------------

def _fetch_org_settlement_fixtures(
    leagues: list,
    cfg,
    name_map: dict,
) -> list[dict]:
    """
    Fetches finished fixtures from football-data.org for settlement use only.
    Only called in force=True path. Returns [] on total failure.
    Skips leagues whose fdo_enrich_code is None (UCL already covered via fdo_code,
    World Cup has no .org source). Team names are pre-resolved to canonical form.
    """
    if not cfg.fdo_api_key:
        return []

    season = _current_season()
    results: list[dict] = []

    for league in leagues:
        settle_code = league.fdo_enrich_code
        if not settle_code:
            continue
        try:
            client = FootballDataOrgClient(settle_code, season, cfg.fdo_api_key)
            fixtures = client.fetch_fixtures()
        except FootballDataOrgError as e:
            logger.warning(
                "[%s] .org settlement fetch failed (will fall back to .co.uk): %s",
                league.key, e,
            )
            continue

        for f in fixtures:
            home_c = resolve_team_name(f["home_team"], name_map, league.key)
            away_c = resolve_team_name(f["away_team"], name_map, league.key)
            if not home_c or not away_c:
                continue
            results.append({**f, "home_team": home_c, "away_team": away_c, "league_key": league.key})

    logger.debug("_fetch_org_settlement_fixtures: %d fixtures across %d leagues.", len(results), len(leagues))
    return results


def _merge_settlement_fixtures(
    couk_fixtures: list[dict],
    org_fixtures: list[dict],
    name_map: dict,
) -> list[dict]:
    """
    Merges .co.uk and .org fixture lists for settlement.
    .org entries take precedence (near real-time). .co.uk fills gaps.
    Dedup key: (canonical_home, canonical_away, YYYY-MM-DD) — timezone-safe.
    """
    def _date_str(dt) -> str:
        if hasattr(dt, "strftime"):
            return dt.strftime("%Y-%m-%d")
        return str(dt)[:10]

    # Index .org entries (already canonical)
    org_index: dict[tuple, dict] = {}
    for f in org_fixtures:
        key = (f["home_team"], f["away_team"], _date_str(f["fixture_date"]))
        org_index[key] = f

    # Fill in .co.uk entries not covered by .org
    fill_ins: list[dict] = []
    for f in couk_fixtures:
        lk = f.get("league_key", "")
        home_c = resolve_team_name(f["home_team"], name_map, lk) or f["home_team"]
        away_c = resolve_team_name(f["away_team"], name_map, lk) or f["away_team"]
        key = (home_c, away_c, _date_str(f["fixture_date"]))
        if key not in org_index:
            fill_ins.append(f)

    return list(org_index.values()) + fill_ins
