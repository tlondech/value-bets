"""
Shared helpers and constants used across the pipeline.
"""

from datetime import datetime, timezone

from models.features import resolve_team_name

logger_name = __name__

import logging
logger = logging.getLogger(__name__)

def get_outcome_label(
    outcome: str,
    home_name: str | None = None,
    away_name: str | None = None,
) -> str:
    """Return a human-readable label for a signal outcome key.

    When home_name/away_name are provided, win outcomes use the team name
    (e.g. "Arsenal Win") instead of the generic "Home Win"/"Away Win".
    Draw and totals labels are unaffected.
    """
    if outcome == "home_win":
        return f"{home_name} Win" if home_name else "Home Win"
    if outcome == "away_win":
        return f"{away_name} Win" if away_name else "Away Win"
    if outcome == "draw":
        return "Draw"
    if outcome.startswith(("over_", "under_")):
        prefix, line_str = outcome.split("_", 1)
        parts = line_str.split("_")
        line = f"{parts[0]}.{''.join(parts[1:])}" if len(parts) > 1 else parts[0]
        return f"{'Over' if prefix == 'over' else 'Under'} {line}"
    if outcome.startswith("spread_home_") or outcome.startswith("spread_away_"):
        side = "home" if outcome.startswith("spread_home_") else "away"
        encoded = outcome[len("spread_home_"):] if side == "home" else outcome[len("spread_away_"):]
        sign = -1 if encoded.startswith("m") else 1
        line = sign * float(encoded[1:].replace("_", "."))
        team = home_name if side == "home" else away_name
        return f"{team} {line:+.1f}" if team else f"{'Home' if side == 'home' else 'Away'} {line:+.1f}"
    return outcome


def is_live(commence_time: datetime) -> bool:
    """Return True if the match has already started (kickoff is in the past)."""
    return commence_time <= datetime.now(timezone.utc)


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
