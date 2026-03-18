"""
SQLite DB read/write helpers.
All functions that interact directly with the local SQLite database via SQLAlchemy.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy.orm import Session

from constants import FIXTURE_DATE_TOLERANCE_DAYS, H2H_LOOKBACK_SEASONS
from db.schema import SignalHistory, Fixture, Match, Odds

logger = logging.getLogger(__name__)


def _settle_totals(outcome: str, hg: int, ag: int) -> bool | None:
    """Parse a dynamic totals outcome string and determine if it won.

    Examples: "over_2_5" → line=2.5, threshold=2 → won if hg+ag > 2
              "over_3_25" → line=3.25, threshold=3 → won if hg+ag > 3
    Returns None if the outcome string is not a totals signal.
    """
    if not outcome.startswith(("over_", "under_")):
        return None
    prefix, line_str = outcome.split("_", 1)
    parts = line_str.split("_")
    line = float(f"{parts[0]}.{''.join(parts[1:])}") if len(parts) > 1 else float(parts[0])
    threshold = int(line)
    return (hg + ag > threshold) if prefix == "over" else (hg + ag <= threshold)


# ---------------------------------------------------------------------------
# Upsert helpers
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
    if event.get("over_odds") is not None or event.get("under_odds") is not None:
        session.query(Odds).filter(
            Odds.match_id == event["match_id"],
            Odds.market == "totals",
        ).delete()
        session.add(Odds(
            match_id=event["match_id"],
            bookmaker=event["bookmaker"],
            market="totals",
            home_odds=event.get("over_odds"),
            away_odds=event.get("under_odds"),
            totals_line=event.get("totals_line"),
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


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

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
        events.append({
            "match_id": match.match_id,
            "home_team": match.home_team,
            "away_team": match.away_team,
            "commence_time": match.match_date.replace(tzinfo=timezone.utc),
            "home_odds": h2h.home_odds,
            "draw_odds": h2h.draw_odds,
            "away_odds": h2h.away_odds,
            "over_odds":   totals.home_odds if totals else None,
            "under_odds":  totals.away_odds if totals else None,
            "totals_line": totals.totals_line if totals else None,
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


def load_h2h_fixtures_df(session: Session, league_key: str, current_season: int, n_seasons: int = H2H_LOOKBACK_SEASONS):
    """Loads finished fixtures for a league across the last n_seasons (including current) for H2H lookups."""
    seasons = [current_season - i for i in range(n_seasons)]
    rows = session.query(Fixture).filter(
        Fixture.league_id == league_key,
        Fixture.season.in_(seasons),
    ).all()
    if not rows:
        return pd.DataFrame(columns=["fixture_date", "home_team", "away_team",
                                     "home_goals", "away_goals", "home_goals_eff", "away_goals_eff"])
    df = pd.DataFrame([{
        "fixture_date": r.fixture_date,
        "home_team": r.home_team,
        "away_team": r.away_team,
        "home_goals": r.home_goals,
        "away_goals": r.away_goals,
        "home_xg": r.home_xg,
        "away_xg": r.away_xg,
    } for r in rows])
    df["fixture_date"] = pd.to_datetime(df["fixture_date"], utc=True)
    df["home_goals_eff"] = df["home_xg"].where(df["home_xg"].notna(), df["home_goals"])
    df["away_goals_eff"] = df["away_xg"].where(df["away_xg"].notna(), df["away_goals"])
    return df


def load_all_fixtures_df(engine, universal_names: dict | None = None):
    """Loads all finished fixtures from every league in the DB as a DataFrame.

    If universal_names is provided, team names are normalized to a common canonical
    form so that cross-league rest-day lookups work (e.g. 'Liverpool FC' → 'Liverpool').
    """
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


# ---------------------------------------------------------------------------
# Signal history helpers
# ---------------------------------------------------------------------------

def prune_stale_signals(
    session,
    all_signals: list[dict],
    processed_league_keys: set[str],
) -> int:
    """
    Deletes unsettled future signals for processed leagues whose outcome is no
    longer in the current detected set (e.g. filtered by the ratio cap or
    market-group deduplication). Scoped to processed leagues only so partial
    runs don't wipe signals from leagues not evaluated this time.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    current_keys: set[tuple] = set()
    for m in all_signals:
        kickoff_dt = datetime.fromisoformat(m["kickoff"]).replace(tzinfo=None)
        for s in m["signals"]:
            current_keys.add((kickoff_dt, m["home_team"], m["away_team"], s["outcome"]))

    unsettled = (
        session.query(SignalHistory)
        .filter(
            SignalHistory.settled == False,
            SignalHistory.kickoff > now,
            SignalHistory.league_key.in_(processed_league_keys),
        )
        .all()
    )

    pruned = 0
    for row in unsettled:
        if (row.kickoff, row.home_team, row.away_team, row.outcome) not in current_keys:
            session.delete(row)
            pruned += 1

    return pruned


def save_signals_to_history(session, match_signals_list: list[dict], recorded_date: str) -> int:
    """
    Persists each detected signal in match_signals_list to signal_history.
    Skips duplicates (same kickoff + teams + outcome already exists).
    Returns the number of newly inserted rows.
    """
    inserted = 0
    for m in match_signals_list:
        kickoff_dt = datetime.fromisoformat(m["kickoff"]).replace(tzinfo=None)
        home_c = m.get("home_canonical")
        away_c = m.get("away_canonical")
        for s in m["signals"]:
            exists = session.query(SignalHistory).filter_by(
                kickoff=kickoff_dt,
                home_team=m["home_team"],
                away_team=m["away_team"],
                outcome=s["outcome"],
            ).first()
            if exists:
                if not exists.settled:
                    exists.odds          = s["odds"]
                    exists.true_prob     = s["true_prob"]
                    exists.ev            = s["ev"]
                    exists.home_rank     = m.get("home_rank")
                    exists.away_rank     = m.get("away_rank")
                    exists.home_form     = json.dumps(m["home_form"]) if m.get("home_form") is not None else None
                    exists.away_form     = json.dumps(m["away_form"]) if m.get("away_form") is not None else None
                    exists.home_crest    = m.get("home_crest")
                    exists.away_crest    = m.get("away_crest")
                    exists.home_rest_days = m.get("home_rest_days")
                    exists.away_rest_days = m.get("away_rest_days")
                    exists.h2h_used      = m.get("h2h_used")
                    exists.is_second_leg = m.get("is_second_leg")
                    exists.agg_home        = m.get("agg_home")
                    exists.agg_away        = m.get("agg_away")
                    exists.leg1_result     = json.dumps(m["leg1_result"]) if m.get("leg1_result") is not None else None
                    exists.bookmaker_link  = m.get("bookmaker_link")
                continue
            session.add(SignalHistory(
                recorded_date=recorded_date,
                league_key=m["league_key"],
                league_name=m["league_name"],
                home_team=m["home_team"],
                away_team=m["away_team"],
                home_canonical=home_c,
                away_canonical=away_c,
                kickoff=kickoff_dt,
                stage=m.get("stage"),
                outcome=s["outcome"],
                outcome_label=s["outcome_label"],
                odds=s["odds"],
                true_prob=s["true_prob"],
                ev=s["ev"],
                home_rank=m.get("home_rank"),
                away_rank=m.get("away_rank"),
                home_form=json.dumps(m["home_form"]) if m.get("home_form") is not None else None,
                away_form=json.dumps(m["away_form"]) if m.get("away_form") is not None else None,
                home_crest=m.get("home_crest"),
                away_crest=m.get("away_crest"),
                home_rest_days=m.get("home_rest_days"),
                away_rest_days=m.get("away_rest_days"),
                h2h_used=m.get("h2h_used"),
                is_second_leg=m.get("is_second_leg"),
                agg_home=m.get("agg_home"),
                agg_away=m.get("agg_away"),
                leg1_result=json.dumps(m["leg1_result"]) if m.get("leg1_result") is not None else None,
                bookmaker_link=m.get("bookmaker_link"),
            ))
            inserted += 1
    return inserted


def settle_signals(session) -> int:
    """
    Resolves unsettled signals whose kickoff is in the past by matching
    results from the Fixture table. Returns the number of newly settled signals.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC for SQLite comparison
    unsettled = session.query(SignalHistory).filter(
        SignalHistory.settled == False,  # noqa: E712
        SignalHistory.kickoff < now,
    ).all()

    settled_count = 0
    for row in unsettled:
        fixture = session.query(Fixture).filter(
            Fixture.home_team == row.home_canonical,
            Fixture.away_team == row.away_canonical,
            Fixture.fixture_date >= row.kickoff - timedelta(days=FIXTURE_DATE_TOLERANCE_DAYS),
            Fixture.fixture_date <= row.kickoff + timedelta(days=FIXTURE_DATE_TOLERANCE_DAYS),
        ).first()

        if fixture is None:
            continue  # result not yet available — will retry on next run

        hg, ag = fixture.home_goals, fixture.away_goals
        won = {
            "home_win": hg > ag,
            "draw":     hg == ag,
            "away_win": ag > hg,
        }.get(row.outcome)
        if won is None:
            won = _settle_totals(row.outcome, hg, ag) or False

        row.settled = True
        row.result = "won" if won else "lost"
        row.actual_home_goals = hg
        row.actual_away_goals = ag
        row.settled_at = now
        settled_count += 1

    return settled_count


def load_signal_history(session) -> list[dict]:
    """Returns all signal history rows as dicts, ordered newest first."""
    rows = session.query(SignalHistory).order_by(SignalHistory.kickoff.desc()).all()
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
