"""
Gaussian efficiency model for NBA basketball.

Team offensive / defensive ratings are derived from a rolling window of recent
game logs. A Normal distribution over the point differential (and total) is
used to derive win, over/under, and spread cover probabilities.

Model formula
-------------
  league_avg   = mean of all recent points-scored values across all teams

  home_expected = home_attack_home + league_avg - away_defense + HOME_ADV
  away_expected = away_attack_away + league_avg - home_defense

  spread_mu     = home_expected - away_expected
  total_mu      = home_expected + away_expected

where:
  home_attack_home  — home team's rolling avg pts scored in home games
  away_attack_away  — away team's rolling avg pts scored in away games
  home_defense      — home team's rolling avg pts allowed (all games)
  away_defense      — away team's rolling avg pts allowed (all games)
  HOME_ADV          — home court advantage (~3.5 pts)
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import norm

from constants import EV_THRESHOLD, NBA_BACK_TO_BACK_DAYS, NBA_BACK_TO_BACK_PENALTY
from models.evaluator import calculate_ev

logger = logging.getLogger(__name__)

# Default league average (fallback when insufficient data)
_DEFAULT_LEAGUE_AVG = 113.0
# Minimum number of home/away games required to use the split rating
_MIN_SPLIT_GAMES = 3
# Outcome key encoding for spread lines (e.g. -5.5 → "m5_5", +5.5 → "p5_5")
_NEG_PREFIX = "m"
_POS_PREFIX = "p"


def _encode_line(point: float) -> str:
    """Encodes a decimal line into a safe string key, e.g. -5.5 → 'm5_5'."""
    prefix = _NEG_PREFIX if point < 0 else _POS_PREFIX
    return prefix + str(abs(point)).replace(".", "_")


def compute_nba_ratings(
    games_df: pd.DataFrame,
    rolling_window: int = 10,
) -> dict[str, dict]:
    """
    Computes per-team offensive and defensive ratings from recent game logs.

    Parameters
    ----------
    games_df : DataFrame from NBADataClient.fetch_team_game_logs()
    rolling_window : number of most recent games to use for each team

    Returns
    -------
    {
      team_abbr: {
        "attack":       float,   # rolling avg points scored (all games)
        "defense":      float,   # rolling avg points allowed (all games)
        "home_attack":  float,   # rolling avg points scored in home games
        "away_attack":  float,   # rolling avg points scored in away games
        "n_games":      int,
        "n_home_games": int,
        "n_away_games": int,
      }
    }
    """
    if games_df.empty:
        return {}

    ratings: dict[str, dict] = {}
    all_pts: list[float] = []

    for abbr, group in games_df.groupby("TEAM_ABBREVIATION"):
        team_games = group.sort_values("GAME_DATE")
        pts:     np.ndarray = np.asarray(team_games["PTS"],     dtype=float)
        opp_pts: np.ndarray = np.asarray(team_games["OPP_PTS"], dtype=float)

        home_mask: np.ndarray = np.asarray(team_games["is_home"], dtype=bool)
        home_pts: np.ndarray  = np.asarray(pts[home_mask], dtype=float)
        away_pts: np.ndarray  = np.asarray(pts[~home_mask], dtype=float)

        # Rolling window: most recent N games
        recent_pts     = pts[-rolling_window:]
        recent_opp_pts = opp_pts[-rolling_window:]
        recent_home    = home_pts[-rolling_window:]
        recent_away    = away_pts[-rolling_window:]

        all_pts.extend(recent_pts.tolist())

        attack       = float(np.mean(recent_pts))     if len(recent_pts)  > 0 else _DEFAULT_LEAGUE_AVG
        defense      = float(np.mean(recent_opp_pts)) if len(recent_opp_pts) > 0 else _DEFAULT_LEAGUE_AVG
        home_attack  = float(np.mean(recent_home))    if len(recent_home) >= _MIN_SPLIT_GAMES else attack
        away_attack  = float(np.mean(recent_away))    if len(recent_away) >= _MIN_SPLIT_GAMES else attack

        # Recent form: last 5 results as W/L (no draws in basketball)
        form_window = min(5, len(pts))
        form = [
            "W" if p > o else "L"
            for p, o in zip(pts[-form_window:], opp_pts[-form_window:])
        ]

        ratings[str(abbr)] = {
            "attack":          attack,
            "defense":         defense,
            "home_attack":     home_attack,
            "away_attack":     away_attack,
            "n_games":         len(team_games),
            "n_home_games":    int(home_mask.sum()),
            "n_away_games":    int((~home_mask).sum()),
            "form":            form,
            "last_game_date":  team_games["GAME_DATE"].iloc[-1],
        }

    league_avg = float(np.mean(all_pts)) if all_pts else _DEFAULT_LEAGUE_AVG
    # Attach league_avg to each entry so predict_game can use it
    for r in ratings.values():
        r["league_avg"] = league_avg

    logger.debug(
        "NBA ratings: %d teams, league_avg=%.1f pts",
        len(ratings), league_avg,
    )
    return ratings


def predict_game(
    home_ratings: dict,
    away_ratings: dict,
    home_advantage: float = 3.5,
    spread_std: float = 15.5,
    total_std: float = 19.0,
    home_rest_days: int | None = None,
    away_rest_days: int | None = None,
) -> dict:
    """
    Predicts expected scores and score-distribution parameters for one matchup.

    Returns
    -------
    {
      "home_expected": float,
      "away_expected": float,
      "spread_mu":     float,   # home - away expected point differential
      "total_mu":      float,   # expected total points
      "spread_std":    float,
      "total_std":     float,
    }
    """
    league_avg = home_ratings.get("league_avg", _DEFAULT_LEAGUE_AVG)

    home_expected = (
        home_ratings["home_attack"]
        + league_avg
        - away_ratings["defense"]
        + home_advantage
    )
    away_expected = (
        away_ratings["away_attack"]
        + league_avg
        - home_ratings["defense"]
    )

    if home_rest_days is not None and home_rest_days <= NBA_BACK_TO_BACK_DAYS:
        home_expected -= NBA_BACK_TO_BACK_PENALTY
        logger.debug("Home team on B2B (%d rest day(s)) → home_expected reduced by %.1f", home_rest_days, NBA_BACK_TO_BACK_PENALTY)
    if away_rest_days is not None and away_rest_days <= NBA_BACK_TO_BACK_DAYS:
        away_expected -= NBA_BACK_TO_BACK_PENALTY
        logger.debug("Away team on B2B (%d rest day(s)) → away_expected reduced by %.1f", away_rest_days, NBA_BACK_TO_BACK_PENALTY)

    return {
        "home_expected": home_expected,
        "away_expected": away_expected,
        "spread_mu":     home_expected - away_expected,
        "total_mu":      home_expected + away_expected,
        "spread_std":    spread_std,
        "total_std":     total_std,
    }


def evaluate_nba_match(
    home_team: str,
    away_team: str,
    home_ratings: dict,
    away_ratings: dict,
    home_odds: float | None,
    away_odds: float | None,
    over_odds: float | None,
    under_odds: float | None,
    totals_line: float | None,
    spread_home_point: float | None,
    spread_home_odds: float | None,
    spread_away_odds: float | None,
    ev_threshold: float = EV_THRESHOLD,
    max_prob_ratio: float = 1.3,
    min_games: int = 10,
    home_advantage: float = 3.5,
    spread_std: float = 15.5,
    total_std: float = 19.0,
    home_rest_days: int | None = None,
    away_rest_days: int | None = None,
) -> list[dict]:
    """
    Evaluates a single NBA matchup and returns a list of value bet dicts.

    Returns [] when either team has insufficient game history.
    Each bet dict mirrors the structure used by tennis / football:
        {outcome, outcome_label, odds, true_prob, ev}
    """
    h_n = home_ratings.get("n_games", 0)
    a_n = away_ratings.get("n_games", 0)
    if h_n < min_games or a_n < min_games:
        logger.debug(
            "Skipping %s vs %s — insufficient game history (%d, %d < min %d)",
            home_team, away_team, h_n, a_n, min_games,
        )
        return []

    pred = predict_game(
        home_ratings, away_ratings,
        home_advantage=home_advantage,
        spread_std=spread_std,
        total_std=total_std,
        home_rest_days=home_rest_days,
        away_rest_days=away_rest_days,
    )

    s_mu  = pred["spread_mu"]
    s_std = pred["spread_std"]
    t_mu  = pred["total_mu"]
    t_std = pred["total_std"]

    bets = []

    # --- Moneyline (h2h) ---
    if home_odds and away_odds:
        p_home = float(norm.sf(0, loc=s_mu, scale=s_std))
        p_away = 1.0 - p_home

        for outcome, true_prob, odds, label in (
            ("home_win", p_home, home_odds, "Home Win"),
            ("away_win", p_away, away_odds, "Away Win"),
        ):
            ev = calculate_ev(true_prob, odds)
            if ev >= ev_threshold and true_prob * odds <= max_prob_ratio:
                bets.append({
                    "outcome":       outcome,
                    "outcome_label": label,
                    "odds":          odds,
                    "true_prob":     round(true_prob, 6),
                    "ev":            round(ev, 6),
                    "market_group":  "h2h",
                })

    # --- Over / Under ---
    if over_odds and under_odds and totals_line is not None:
        p_over  = float(norm.sf(totals_line, loc=t_mu, scale=t_std))
        p_under = 1.0 - p_over

        # Whole-number lines aren't available in the app; we always bet the
        # nearest half-point: Over 234.0 → Over 233.5, Under 234.0 → Under 234.5.
        over_line  = (totals_line - 0.5) if totals_line == int(totals_line) else totals_line
        under_line = (totals_line + 0.5) if totals_line == int(totals_line) else totals_line
        over_key   = str(over_line).replace(".", "_")
        under_key  = str(under_line).replace(".", "_")
        for outcome, true_prob, odds, label in (
            (f"over_{over_key}",   p_over,  over_odds,  f"Over {over_line}"),
            (f"under_{under_key}", p_under, under_odds, f"Under {under_line}"),
        ):
            ev = calculate_ev(true_prob, odds)
            if ev >= ev_threshold and true_prob * odds <= max_prob_ratio:
                bets.append({
                    "outcome":       outcome,
                    "outcome_label": label,
                    "odds":          odds,
                    "true_prob":     round(true_prob, 6),
                    "ev":            round(ev, 6),
                    "market_group":  "totals",
                })

    # --- Spread / Handicap ---
    if spread_home_point is not None and spread_home_odds and spread_away_odds:
        # spread_home_point: e.g. -5.5 means home is favoured by 5.5
        # P(home covers) = P(home - away > 5.5) = P(spread_dist > 5.5)
        threshold = -spread_home_point  # 5.5 when spread_home_point is -5.5
        p_home_covers = float(norm.sf(threshold, loc=s_mu, scale=s_std))
        p_away_covers = 1.0 - p_home_covers

        line_enc = _encode_line(spread_home_point)
        away_line_enc = _encode_line(-spread_home_point)
        home_label = f"{home_team} {spread_home_point:+.1f}"
        away_label = f"{away_team} {-spread_home_point:+.1f}"

        for outcome, true_prob, odds, label in (
            (f"spread_home_{line_enc}", p_home_covers, spread_home_odds, home_label),
            (f"spread_away_{away_line_enc}", p_away_covers, spread_away_odds, away_label),
        ):
            ev = calculate_ev(true_prob, odds)
            if ev >= ev_threshold and true_prob * odds <= max_prob_ratio:
                bets.append({
                    "outcome":       outcome,
                    "outcome_label": label,
                    "odds":          odds,
                    "true_prob":     round(true_prob, 6),
                    "ev":            round(ev, 6),
                    "market_group":  "spreads",
                })

    return bets
