"""
Betting Recommendation Engine — Main Orchestrator
Run: python main.py

Pipeline (per enabled league):
  1. Load config + init DB
  2. Fetch upcoming Winamax odds (The Odds API)
  3. Upsert matches + odds into SQLite
  4. Fetch finished fixtures + xG (football-data.co.uk CSV)
  5. Upsert fixtures into SQLite
  6. Build Poisson features per match
  7. Calculate Expected Value → collect value bets
  8. Merge all leagues, write report JSON + open index.html in browser
"""

import argparse
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from config import LeagueConfig, _current_season, load_config
from db.schema import BetHistory, Fixture, Match, Odds, init_db
from extractors.odds import OddsAPIClient
from extractors.footballdata_client import FootballDataClient, FootballDataError
from extractors.footballdataorg_client import FootballDataOrgClient, FootballDataOrgError
from models.evaluator import evaluate_match
from models.features import (
    build_fixtures_dataframe,
    build_poisson_inputs,
    build_poisson_inputs_dc,
    compute_form,
    compute_league_averages,
    compute_standings,
    fit_dixon_coles,
    load_team_name_map,
    resolve_team_name,
)
from notifications.reporter import open_report, write_report_json

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

class _ColoredFormatter(logging.Formatter):
    _COLORS = {
        logging.DEBUG:    "\033[90m",   # dim gray
        logging.INFO:     "\033[0m",    # default
        logging.WARNING:  "\033[93m",   # bright yellow
        logging.ERROR:    "\033[91m",   # bright red
        logging.CRITICAL: "\033[95m",   # magenta
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self._COLORS.get(record.levelno, "")
        return f"{color}{super().format(record)}{self._RESET}"


_FMT  = "%(asctime)s [%(levelname)s]  %(message)s"
_DATE = "%H:%M:%S"

os.makedirs("logs", exist_ok=True)

_file_handler = logging.FileHandler("logs/run.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE))

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_ColoredFormatter(_FMT, datefmt=_DATE))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logger = logging.getLogger(__name__)

_OUTCOME_LABELS = {
    "home_win":  "Home Win",
    "draw":      "Draw",
    "away_win":  "Away Win",
    "over_2_5":  "Over 2.5",
    "under_2_5": "Under 2.5",
    "btts_yes":  "BTTS Yes",
    "btts_no":   "BTTS No",
}

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def upsert_match(session: Session, event: dict, league_key: str) -> None:
    existing = session.get(Match, event["match_id"])
    if existing is None:
        session.add(Match(
            match_id=event["match_id"],
            home_team=event["home_team"],
            away_team=event["away_team"],
            match_date=event["commence_time"],
            league=league_key,
            status="upcoming",
            stage=event.get("stage"),
        ))
    elif event.get("stage") and existing.stage != event["stage"]:
        existing.stage = event["stage"]


def upsert_odds(session: Session, event: dict) -> None:
    session.query(Odds).filter(
        Odds.match_id == event["match_id"],
        Odds.bookmaker == event["bookmaker"],
        Odds.market == "h2h",
    ).delete()
    session.add(Odds(
        match_id=event["match_id"],
        bookmaker=event["bookmaker"],
        market="h2h",
        home_odds=event["home_odds"],
        draw_odds=event["draw_odds"],
        away_odds=event["away_odds"],
        fetched_at=datetime.now(tz=timezone.utc),
    ))
    if event.get("over_2_5_odds") is not None or event.get("under_2_5_odds") is not None:
        session.query(Odds).filter(
            Odds.match_id == event["match_id"],
            Odds.market == "totals",
        ).delete()
        session.add(Odds(
            match_id=event["match_id"],
            bookmaker=event["bookmaker"],
            market="totals",
            home_odds=event.get("over_2_5_odds"),
            away_odds=event.get("under_2_5_odds"),
            fetched_at=datetime.now(tz=timezone.utc),
        ))
    if event.get("btts_yes_odds") is not None or event.get("btts_no_odds") is not None:
        session.query(Odds).filter(
            Odds.match_id == event["match_id"],
            Odds.market == "bts",
        ).delete()
        session.add(Odds(
            match_id=event["match_id"],
            bookmaker=event["bookmaker"],
            market="bts",
            home_odds=event.get("btts_yes_odds"),
            away_odds=event.get("btts_no_odds"),
            fetched_at=datetime.now(tz=timezone.utc),
        ))


def upsert_fixtures(session: Session, raw_fixtures: list[dict], league_key: str, season: int) -> None:
    existing_ids = {fid for (fid,) in session.query(Fixture.fixture_id).all()}
    new_fixtures = [f for f in raw_fixtures if f["fixture_id"] not in existing_ids]
    for f in new_fixtures:
        session.add(Fixture(
            fixture_id=f["fixture_id"],
            league_id=league_key,
            season=season,
            fixture_date=f["fixture_date"],
            home_team=f["home_team"],
            away_team=f["away_team"],
            home_goals=f["home_goals"],
            away_goals=f["away_goals"],
            home_xg=f.get("home_xg"),
            away_xg=f.get("away_xg"),
        ))
    logger.debug("[DB] Inserted %d new fixtures.", len(new_fixtures))


def load_upcoming_events_from_db(session: Session, league_key: str) -> list[dict]:
    """Loads upcoming matches with stored odds from the DB, returning the same format as fetch_upcoming_odds()."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    matches = session.query(Match).filter(
        Match.league == league_key,
        Match.status == "upcoming",
        Match.match_date > now,
    ).all()
    events = []
    for match in matches:
        h2h = session.query(Odds).filter(
            Odds.match_id == match.match_id,
            Odds.market == "h2h",
        ).order_by(Odds.fetched_at.desc()).first()
        if h2h is None:
            continue
        totals = session.query(Odds).filter(
            Odds.match_id == match.match_id,
            Odds.market == "totals",
        ).order_by(Odds.fetched_at.desc()).first()
        bts = session.query(Odds).filter(
            Odds.match_id == match.match_id,
            Odds.market == "bts",
        ).order_by(Odds.fetched_at.desc()).first()
        events.append({
            "match_id": match.match_id,
            "home_team": match.home_team,
            "away_team": match.away_team,
            "commence_time": match.match_date.replace(tzinfo=timezone.utc),
            "home_odds": h2h.home_odds,
            "draw_odds": h2h.draw_odds,
            "away_odds": h2h.away_odds,
            "over_2_5_odds": totals.home_odds if totals else None,
            "under_2_5_odds": totals.away_odds if totals else None,
            "btts_yes_odds": bts.home_odds if bts else None,
            "btts_no_odds": bts.away_odds if bts else None,
            "bookmaker": h2h.bookmaker,
            "stage": match.stage,
        })
    return events


def load_raw_fixtures_from_db(session: Session, league_key: str, season: int) -> list[dict]:
    """Loads finished fixtures for a league/season from the DB, returning the same format as fetch_fixtures()."""
    rows = session.query(Fixture).filter(
        Fixture.league_id == league_key,
        Fixture.season == season,
    ).all()
    return [{
        "fixture_id": r.fixture_id,
        "fixture_date": r.fixture_date,
        "home_team": r.home_team,
        "away_team": r.away_team,
        "home_goals": r.home_goals,
        "away_goals": r.away_goals,
        "home_xg": r.home_xg,
        "away_xg": r.away_xg,
    } for r in rows]


# ---------------------------------------------------------------------------
# Per-league pipeline
# ---------------------------------------------------------------------------


def build_leg2_map(
    upcoming_events: list[dict],
    raw_fixtures: list[dict],
    raw_stage_map: dict[str, str],
    name_map: dict,
    league_key: str,
) -> dict[tuple, dict]:
    """
    Returns {(home_canonical, away_canonical): leg2_context} for UCL Leg 2 fixtures.

    A match is Leg 2 when:
      1. Its raw stage is in UCL_KNOCKOUT_STAGES
      2. A finished fixture exists between the same two teams with reversed home/away roles

    Aggregate going into Leg 2:
      agg_home = leg1.away_goals  (Leg 2 home team was away in Leg 1)
      agg_away = leg1.home_goals  (Leg 2 away team was home in Leg 1)
    """
    from config import UCL_KNOCKOUT_STAGES

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
        raw_stage = raw_stage_map.get(f"{home_c}|{away_c}", "")
        if raw_stage not in UCL_KNOCKOUT_STAGES:
            logger.debug(
                "build_leg2_map: skipping %s vs %s — stage %r not in UCL_KNOCKOUT_STAGES (raw_stage_map has %d entries)",
                home_c, away_c, raw_stage, len(raw_stage_map),
            )
            continue
        # Leg 2 home team was AWAY in Leg 1 → look for reversed fixture
        leg1 = finished_index.get((away_c, home_c))
        if leg1 is None:
            logger.debug(
                "build_leg2_map: skipping %s vs %s — leg 1 (%s vs %s) not found in finished_index (%d entries)",
                home_c, away_c, away_c, home_c, len(finished_index),
            )
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

def load_all_fixtures_df(engine, universal_names: dict | None = None):
    """Loads all finished fixtures from every league in the DB as a DataFrame.

    If universal_names is provided, team names are normalized to a common canonical
    form so that cross-league rest-day lookups work (e.g. 'Liverpool FC' → 'Liverpool').
    """
    import pandas as pd
    with Session(engine) as session:
        rows = session.query(Fixture).all()
    if not rows:
        return pd.DataFrame(columns=["fixture_date", "home_team", "away_team",
                                     "home_goals", "away_goals", "home_goals_eff", "away_goals_eff"])
    norm = universal_names or {}
    df = pd.DataFrame([{
        "fixture_date": r.fixture_date,
        "home_team": norm.get(r.home_team, r.home_team),
        "away_team": norm.get(r.away_team, r.away_team),
        "home_goals": r.home_goals,
        "away_goals": r.away_goals,
        "home_xg": r.home_xg,
        "away_xg": r.away_xg,
    } for r in rows])
    df["fixture_date"] = pd.to_datetime(df["fixture_date"], utc=True)
    df["home_goals_eff"] = df["home_xg"].where(df["home_xg"].notna(), df["home_goals"])
    df["away_goals_eff"] = df["away_xg"].where(df["away_xg"].notna(), df["away_goals"])
    return df


def run_league_pipeline(
    league: LeagueConfig,
    cfg,
    engine,
    name_map: dict,
    force: bool = False,
) -> list[dict]:
    """
    Runs the full extraction → evaluation pipeline for one league.
    Returns a (possibly empty) list of value bet dicts.
    Any recoverable failure logs an error and returns [].
    """
    if league.fd_code is None and league.fdo_code is None:
        logger.debug("[%s] No data source configured — skipping.", league.key)
        return []

    season = league.season_override if league.season_override is not None else _current_season()
    logger.debug(
        "--- League: %s (key=%s, season=%d) ---",
        league.display_name, league.key, season,
    )

    stage_map: dict[str, str] = {}
    crest_map: dict[str, str] = {}
    raw_stage_map: dict[str, str] = {}
    rankings: dict[str, int] = {}
    total_matchdays: int | None = None
    form_map: dict[str, list[str]] = {}
    odds_client = None

    if force:
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
            return []

        # Optional: fetch BTTS odds per-event (costs 1 quota unit per match)
        if cfg.odds_btts_bookmakers:
            event_ids = [e["match_id"] for e in upcoming_events]
            btts_map = odds_client.fetch_btts_odds(league.odds_sport, event_ids, cfg.odds_btts_bookmakers)
            for event in upcoming_events:
                yes_odds, no_odds = btts_map.get(event["match_id"], (None, None))
                event["btts_yes_odds"] = yes_odds
                event["btts_no_odds"] = no_odds

        # Phase 1b: Fetch stage/matchday enrichment (non-fatal)
        enrich_code = league.fdo_code or league.fdo_enrich_code
        if enrich_code and cfg.fdo_api_key:
            fdo_enrich = FootballDataOrgClient(enrich_code, season, cfg.fdo_api_key)
            try:
                stage_map, crest_map, raw_stage_map = fdo_enrich.fetch_stage_map(name_map, league.key)
                logger.debug("[%s] Stage map: %d entries, %d crests.", league.key, len(stage_map), len(crest_map))
                # Persist crests so no-force runs can use them
                if crest_map:
                    p = Path(cfg.crest_map_path)
                    existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
                    existing.update(crest_map)
                    p.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                logger.warning("[%s] Stage enrichment failed: %s", league.display_name, e)

        # Phase 2: Upsert matches + odds
        with Session(engine) as session:
            for event in upcoming_events:
                upsert_match(session, event, league.key)
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
            return []

        if not raw_fixtures:
            logger.warning(
                "[%s] No finished fixtures found — season may not have started yet. Skipping.",
                league.key,
            )
            return []

        with Session(engine) as session:
            upsert_fixtures(session, raw_fixtures, league.key, season)
            session.commit()
    else:
        # Load from DB — no API calls
        p = Path(cfg.crest_map_path)
        crest_map = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        with Session(engine) as session:
            upcoming_events = load_upcoming_events_from_db(session, league.key)
            raw_fixtures = load_raw_fixtures_from_db(session, league.key, season)

        if not upcoming_events:
            logger.debug("[%s] No upcoming matches in DB — skipping.", league.key)
            return []

        if not raw_fixtures:
            logger.warning(
                "[%s] No finished fixtures in DB — run with --force to fetch from API. Skipping.",
                league.key,
            )
            return []

    # Build Leg 2 aggregate context map (UCL knockout only; no-op for all other leagues)
    leg2_map = build_leg2_map(upcoming_events, raw_fixtures, raw_stage_map, name_map, league.key)
    if leg2_map:
        logger.info("[%s] Detected %d Leg 2 fixture(s).", league.display_name, len(leg2_map))

    # Compute standings and form from fixture data (domestic leagues only; no extra API call)
    if league.fdo_enrich_code:
        standings = compute_standings(raw_fixtures)
        rankings = standings["rankings"]
        total_matchdays = standings["total_matchdays"]
        form_map = compute_form(raw_fixtures)
        logger.debug("[%s] Standings computed: %d teams, total_matchdays=%s.", league.key, len(rankings), total_matchdays)
    # Augment stage labels with total matchday count
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

    # Phase 5: Evaluate each match
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
        event["stage"] = stage_map.get(f"{home_canonical}|{away_canonical}")
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
                home_universal=home_universal,
                away_universal=away_universal,
                leg2_context=leg2_context,
            )
        if poisson_inputs is None:
            poisson_inputs = build_poisson_inputs(
                home_canonical, away_canonical, fixtures_df, league_avgs, cfg.rolling_window,
                match_date=event["commence_time"],
                all_fixtures_df=all_fixtures_df,
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
            btts_yes_odds=event.get("btts_yes_odds"),
            btts_no_odds=event.get("btts_no_odds"),
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
            "btts_yes":  (result["btts_yes_prob"],  event.get("btts_yes_odds"),   result["btts_yes_ev"]),
            "btts_no":   (result["btts_no_prob"],   event.get("btts_no_odds"),    result["btts_no_ev"]),
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
                "btts_yes_prob":  round(result["btts_yes_prob"], 4),
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
    return value_bets


# ---------------------------------------------------------------------------
# Bet history helpers
# ---------------------------------------------------------------------------

def save_bets_to_history(session, match_bets_list: list[dict], recorded_date: str) -> int:
    """
    Persists each recommended bet in match_bets_list to bet_history.
    Skips duplicates (same kickoff + teams + outcome already exists).
    Returns the number of newly inserted rows.
    """
    inserted = 0
    for m in match_bets_list:
        kickoff_dt = datetime.fromisoformat(m["kickoff"]).replace(tzinfo=None)
        home_c = m.get("home_canonical")
        away_c = m.get("away_canonical")
        for b in m["bets"]:
            exists = session.query(BetHistory).filter_by(
                kickoff=kickoff_dt,
                home_team=m["home_team"],
                away_team=m["away_team"],
                outcome=b["outcome"],
            ).first()
            if exists:
                if not exists.settled:
                    exists.odds      = b["odds"]
                    exists.true_prob = b["true_prob"]
                    exists.ev        = b["ev"]
                continue
            session.add(BetHistory(
                recorded_date=recorded_date,
                league_key=m["league_key"],
                league_name=m["league_name"],
                home_team=m["home_team"],
                away_team=m["away_team"],
                home_canonical=home_c,
                away_canonical=away_c,
                kickoff=kickoff_dt,
                stage=m.get("stage"),
                outcome=b["outcome"],
                outcome_label=b["outcome_label"],
                odds=b["odds"],
                true_prob=b["true_prob"],
                ev=b["ev"],
            ))
            inserted += 1
    return inserted


def settle_bets(session) -> int:
    """
    Resolves unsettled bets whose kickoff is in the past by matching
    results from the Fixture table. Returns the number of newly settled bets.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC for SQLite comparison
    unsettled = session.query(BetHistory).filter(
        BetHistory.settled == False,  # noqa: E712
        BetHistory.kickoff < now,
    ).all()

    settled_count = 0
    for bet in unsettled:
        fixture = session.query(Fixture).filter(
            Fixture.home_team == bet.home_canonical,
            Fixture.away_team == bet.away_canonical,
            Fixture.fixture_date >= bet.kickoff - timedelta(days=1),
            Fixture.fixture_date <= bet.kickoff + timedelta(days=1),
        ).first()

        if fixture is None:
            continue  # result not yet available — will retry on next run

        hg, ag = fixture.home_goals, fixture.away_goals
        won = {
            "home_win":  hg > ag,
            "draw":      hg == ag,
            "away_win":  ag > hg,
            "over_2_5":  hg + ag > 2,
            "under_2_5": hg + ag <= 2,
            "btts_yes":  hg > 0 and ag > 0,
            "btts_no":   hg == 0 or ag == 0,
        }.get(bet.outcome, False)

        bet.settled = True
        bet.result = "won" if won else "lost"
        bet.actual_home_goals = hg
        bet.actual_away_goals = ag
        bet.settled_at = now
        settled_count += 1

    return settled_count


def load_bet_history(session) -> list[dict]:
    """Returns all bet history rows as dicts, ordered newest first."""
    rows = session.query(BetHistory).order_by(BetHistory.kickoff.desc()).all()
    return [
        {
            "recorded_date":     r.recorded_date,
            "league_name":       r.league_name,
            "home_team":         r.home_team,
            "away_team":         r.away_team,
            "kickoff":           r.kickoff.isoformat(),
            "stage":             r.stage,
            "outcome_label":     r.outcome_label,
            "odds":              r.odds,
            "true_prob":         r.true_prob,
            "ev":                r.ev,
            "settled":           r.settled,
            "result":            r.result,
            "actual_home_goals": r.actual_home_goals,
            "actual_away_goals": r.actual_away_goals,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_pipeline(force: bool = False) -> None:
    t0 = time.monotonic()
    cfg = load_config()
    engine = init_db(cfg.db_path)
    name_map = load_team_name_map(cfg.team_map_path)

    from config import LEAGUES as _ALL_LEAGUES
    n_skipped_leagues = len(_ALL_LEAGUES) - len(cfg.enabled_leagues)
    suffix = f"  (+ {n_skipped_leagues} skipped)" if n_skipped_leagues else ""
    logger.info("Leagues: %s%s", ", ".join(lg.display_name for lg in cfg.enabled_leagues), suffix)

    # Settle any past bets before running today's pipeline (results may now be available)
    with Session(engine) as session:
        n_settled = settle_bets(session)
        session.commit()
    if n_settled:
        logger.info("Settled %d past bet(s).", n_settled)

    all_value_bets: list[dict] = []
    for league in cfg.enabled_leagues:
        league_bets = run_league_pipeline(league, cfg, engine, name_map, force=force)
        all_value_bets.extend(league_bets)

    all_value_bets.sort(key=lambda x: x["kickoff"])
    total_bets = sum(len(m["bets"]) for m in all_value_bets)
    logger.info(
        "Total: %d value bets across %d matches  (%.1f sec)",
        total_bets, len(all_value_bets), time.monotonic() - t0,
    )

    # Persist today's recommendations and load full history for the report
    with Session(engine) as session:
        n_new = save_bets_to_history(session, all_value_bets, date.today().isoformat())
        session.commit()
        history = load_bet_history(session)
    if n_new:
        logger.info("Saved %d new bet record(s) to history.", n_new)

    write_report_json(all_value_bets, history, cfg.report_json_path)
    open_report(cfg.report_html_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Betting Recommendation Engine")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if already run today")
    parser.add_argument("--fetch", action="store_true", help="Always fetch fresh data from external APIs (use in CI / scheduled runs)")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG-level logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("══ Betting Engine ══  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        run_pipeline(force=args.force or args.fetch)
    except Exception as e:
        logger.exception("Unhandled error in pipeline: %s", e)
        raise


if __name__ == "__main__":
    main()
