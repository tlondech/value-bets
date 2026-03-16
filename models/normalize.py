from __future__ import annotations

from typing import Any


def normalize_match_data(raw_data: dict[str, Any], sport: str) -> dict[str, Any]:
    """
    Normalize raw API match data into a unified structure across sports.

    Returns:
        {
            "sport":         str,
            "home_team":     str,
            "away_team":     str,
            "home_score":    float | None,
            "away_score":    float | None,
            "score_display": str | None,   # human-readable, e.g. "6-4, 7-6"
            "has_draw":      bool,
            "result":        "home" | "away" | "draw" | None,
            "handicap_line": float | None, # basketball spreads; None for others
        }
    """
    sport = sport.lower()

    match sport:
        case "football":
            return _normalize_football(raw_data)
        case "basketball":
            return _normalize_basketball(raw_data)
        case "tennis":
            return _normalize_tennis(raw_data)
        case _:
            raise ValueError(f"Unsupported sport: {sport!r}")


# ---------------------------------------------------------------------------
# Football
# ---------------------------------------------------------------------------

def _normalize_football(raw: dict) -> dict:
    home = raw.get("home_score")
    away = raw.get("away_score")

    result = None
    if home is not None and away is not None:
        home, away = int(home), int(away)
        if home > away:
            result = "home"
        elif away > home:
            result = "away"
        else:
            result = "draw"

    return {
        "sport":         "football",
        "home_team":     raw.get("home_team") or raw.get("homeTeam", ""),
        "away_team":     raw.get("away_team") or raw.get("awayTeam", ""),
        "home_score":    float(home) if home is not None else None,
        "away_score":    float(away) if away is not None else None,
        "score_display": f"{home}-{away}" if home is not None else None,
        "has_draw":      True,
        "result":        result,
        "handicap_line": None,
    }


# ---------------------------------------------------------------------------
# Basketball
# ---------------------------------------------------------------------------

def _normalize_basketball(raw: dict) -> dict:
    # Scores may arrive as "112-108" or separate fields
    home, away = _parse_score_field(
        raw.get("score"),
        raw.get("home_score"),
        raw.get("away_score"),
    )

    result = None
    if home is not None and away is not None:
        # Basketball has no draw; OT always produces a winner
        result = "home" if home > away else "away"

    # Spread stored as a signed float, e.g. -5.5 means home favoured by 5.5
    handicap_raw = raw.get("spread") or raw.get("handicap") or raw.get("line")
    handicap_line = float(handicap_raw) if handicap_raw is not None else None

    return {
        "sport":         "basketball",
        "home_team":     raw.get("home_team") or raw.get("homeTeam", ""),
        "away_team":     raw.get("away_team") or raw.get("awayTeam", ""),
        "home_score":    float(home) if home is not None else None,
        "away_score":    float(away) if away is not None else None,
        "score_display": f"{int(home)}-{int(away)}" if home is not None else None,
        "has_draw":      False,
        "result":        result,
        "handicap_line": handicap_line,
    }


# ---------------------------------------------------------------------------
# Tennis
# ---------------------------------------------------------------------------

def _normalize_tennis(raw: dict) -> dict:
    """
    Tennis scores arrive as set strings, e.g. "6-4, 7-6" or ["6-4", "7-6"].
    home_score / away_score represent sets won, not points.
    """
    sets_raw = raw.get("score") or raw.get("sets") or raw.get("result")

    home_sets = away_sets = None
    score_display = None

    if sets_raw:
        if isinstance(sets_raw, list):
            score_display = ", ".join(sets_raw)
            set_list = sets_raw
        else:
            score_display = str(sets_raw)
            set_list = [s.strip() for s in score_display.split(",")]

        home_sets = away_sets = 0
        for s in set_list:
            # Strip tiebreak notation, e.g. "7-6(4)" → "7-6"
            base = s.split("(")[0].strip()
            parts = base.split("-")
            if len(parts) == 2 and all(p.isdigit() for p in parts):
                h, a = int(parts[0]), int(parts[1])
                if h > a:
                    home_sets += 1
                elif a > h:
                    away_sets += 1

    result = None
    if home_sets is not None and away_sets is not None:
        result = "home" if home_sets > away_sets else "away"

    return {
        "sport":         "tennis",
        "home_team":     raw.get("player1") or raw.get("home_team", ""),
        "away_team":     raw.get("player2") or raw.get("away_team", ""),
        "home_score":    float(home_sets) if home_sets is not None else None,
        "away_score":    float(away_sets) if away_sets is not None else None,
        "score_display": score_display,
        "has_draw":      False,
        "result":        result,
        "handicap_line": None,
    }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_score_field(
    combined: str | None,
    home_raw: Any,
    away_raw: Any,
) -> tuple[float | None, float | None]:
    """Parse a '112-108' string or fall back to separate home/away fields."""
    if combined:
        parts = str(combined).split("-")
        if len(parts) == 2:
            try:
                return float(parts[0]), float(parts[1])
            except ValueError:
                pass
    if home_raw is not None and away_raw is not None:
        try:
            return float(home_raw), float(away_raw)
        except (TypeError, ValueError):
            pass
    return None, None
