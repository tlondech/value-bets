"""
Surface-adjusted Elo model for tennis.

Elo ratings are computed from Jeff Sackmann historical match data and blended
across overall + surface-specific pools. Win probability is then compared to
Winamax odds using the existing calculate_ev function to find +EV bets.
"""
import logging

import pandas as pd

from constants import EV_THRESHOLD
from models.evaluator import calculate_ev

logger = logging.getLogger(__name__)

INITIAL_ELO = 1500.0
SURFACES = ("Hard", "Clay", "Grass")
# K-factor by tournament level: Grand Slams carry more weight than ATP 250s
K_BY_LEVEL = {"G": 32, "M": 28, "A": 24, "D": 20, "F": 20}
# Blend: 60% surface-specific Elo + 40% overall Elo
SURFACE_WEIGHT = 0.6


def compute_elo_ratings(matches: pd.DataFrame) -> dict[str, dict[str, float]]:
    """
    Computes per-player Elo ratings from a sorted DataFrame of completed matches.

    Returns {player_name: {"overall": float, "Hard": float, "Clay": float, "Grass": float}}
    """
    ratings: dict[str, dict] = {}

    def _init(player: str) -> None:
        if player not in ratings:
            ratings[player] = {s: INITIAL_ELO for s in ("overall", *SURFACES)}

    for _, row in matches.iterrows():
        w = str(row["winner_name"])
        l = str(row["loser_name"])
        surface = str(row.get("surface", "Hard"))
        level = str(row.get("tourney_level", "D"))
        K = K_BY_LEVEL.get(level, 20)

        _init(w)
        _init(l)

        surface_key = surface if surface in SURFACES else None
        for key in ("overall", surface_key):
            if key is None:
                continue
            ew = ratings[w][key]
            el = ratings[l][key]
            expected = 1.0 / (1.0 + 10.0 ** ((el - ew) / 400.0))
            ratings[w][key] += K * (1.0 - expected)
            ratings[l][key] += K * (0.0 - (1.0 - expected))

    return ratings


def blended_elo(ratings: dict, player: str, surface: str) -> float:
    overall = ratings[player]["overall"]
    s_elo = ratings[player].get(surface, overall)
    return SURFACE_WEIGHT * s_elo + (1.0 - SURFACE_WEIGHT) * overall


def evaluate_tennis_match(
    player1: str,
    player2: str,
    surface: str,
    p1_odds: float,
    p2_odds: float,
    ratings: dict[str, dict],
    ev_threshold: float = EV_THRESHOLD,
) -> list[dict]:
    """
    Returns a list of value bets for one match.
    Returns [] if either player has no Elo history.
    """
    if player1 not in ratings or player2 not in ratings:
        logger.debug(
            "No Elo history — skipping %s vs %s (missing: %s)",
            player1, player2,
            ", ".join(p for p in (player1, player2) if p not in ratings),
        )
        return []

    elo1 = blended_elo(ratings, player1, surface)
    elo2 = blended_elo(ratings, player2, surface)
    p1_wins = 1.0 / (1.0 + 10.0 ** ((elo2 - elo1) / 400.0))
    p2_wins = 1.0 - p1_wins

    bets = []
    for outcome, true_prob, odds, player in (
        ("home_win", p1_wins, p1_odds, player1),
        ("away_win", p2_wins, p2_odds, player2),
    ):
        ev = calculate_ev(true_prob, odds)
        if ev >= ev_threshold:
            bets.append({
                "outcome":       outcome,
                "outcome_label": f"{player} Win",
                "odds":          odds,
                "true_prob":     round(true_prob, 6),
                "ev":            round(ev, 6),
            })
    return bets
