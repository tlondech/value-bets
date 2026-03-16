"""
Shared helpers and constants used across the pipeline.
"""

from datetime import datetime, timedelta, timezone

from constants import LIVE_MATCH_WINDOW_HOURS
from models.features import resolve_team_name

logger_name = __name__

import logging
logger = logging.getLogger(__name__)

_STATIC_LABELS = {
    "home_win": "Home Win",
    "draw":     "Draw",
    "away_win": "Away Win",
}


def get_outcome_label(outcome: str) -> str:
    """Return a human-readable label for a bet outcome key.

    Static outcomes (home_win, draw, away_win) use a lookup table.
    Totals outcomes encode a normalised half-integer line: "over_2_5" → "Over 2.5".
    """
    if outcome in _STATIC_LABELS:
        return _STATIC_LABELS[outcome]
    if outcome.startswith(("over_", "under_")):
        prefix, line_str = outcome.split("_", 1)
        parts = line_str.split("_")
        line = f"{parts[0]}.{''.join(parts[1:])}" if len(parts) > 1 else parts[0]
        return f"{'Over' if prefix == 'over' else 'Under'} {line}"
    return outcome


def is_live(commence_time: datetime, window_hours: float = LIVE_MATCH_WINDOW_HOURS) -> bool:
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
