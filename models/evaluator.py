import numpy as np
from scipy.stats import poisson

from constants import DIXON_COLES_RHO_FLOOR, EV_THRESHOLD

def build_score_matrix(
    home_lambda: float,
    away_lambda: float,
    max_goals: int = 8,
    rho: float = 0.0,
) -> np.ndarray:
    """
    Builds a (max_goals+1) x (max_goals+1) matrix where
    matrix[i][j] = P(home scores i goals) * P(away scores j goals).

    When rho != 0 (fitted by Dixon-Coles MLE), applies the low-score
    correction to the 0-0, 1-0, 0-1, and 1-1 cells, then re-normalises.
    """
    home_probs = np.array([poisson.pmf(i, home_lambda) for i in range(max_goals + 1)])
    away_probs = np.array([poisson.pmf(j, away_lambda) for j in range(max_goals + 1)])
    matrix = np.outer(home_probs, away_probs)
    if rho != 0.0:
        lam1, lam2 = home_lambda, away_lambda
        matrix[0, 0] *= max(1.0 - lam1 * lam2 * rho, DIXON_COLES_RHO_FLOOR)
        matrix[1, 0] *= max(1.0 + lam2 * rho,         DIXON_COLES_RHO_FLOOR)
        matrix[0, 1] *= max(1.0 + lam1 * rho,         DIXON_COLES_RHO_FLOOR)
        matrix[1, 1] *= max(1.0 - rho,                 DIXON_COLES_RHO_FLOOR)
        matrix /= matrix.sum()
    return matrix


def calculate_match_probabilities(score_matrix: np.ndarray) -> dict:
    """
    Sums regions of the score matrix to get match outcome probabilities.
    Normalizes so the three outcomes sum to exactly 1.0.
    """
    home_win = float(np.sum(np.tril(score_matrix, k=-1)))
    draw = float(np.sum(np.diag(score_matrix)))
    away_win = float(np.sum(np.triu(score_matrix, k=1)))

    total = home_win + draw + away_win
    if total == 0:
        return {"home_win": 1 / 3, "draw": 1 / 3, "away_win": 1 / 3}

    return {
        "home_win": home_win / total,
        "draw": draw / total,
        "away_win": away_win / total,
    }


def calculate_over_under_probs(score_matrix: np.ndarray, line: float = 2.5) -> dict:
    """P(total goals > line) and P(total goals <= line)."""
    threshold = int(line)  # for 2.5 → 2; "over" means total >= 3
    mask = np.array(
        [[i + j > threshold for j in range(score_matrix.shape[1])]
         for i in range(score_matrix.shape[0])]
    )
    over = float(score_matrix[mask].sum())
    return {"over": over, "under": 1.0 - over}


def calculate_ev(true_probability: float, decimal_odds: float) -> float:
    """
    EV = (true_probability * decimal_odds) - 1
    Positive EV means the bet has a positive expected return.
    """
    return (true_probability * decimal_odds) - 1.0


def _fmt_line(line: float) -> str:
    """Normalise a totals line to its canonical half-integer outcome key.

    Uses int(line) + 0.5 so that 2.25, 2.5, and 2.75 all become '2_5',
    and 3.25/3.5/3.75 all become '3_5'. This matches how Winamax labels the market.
    """
    canonical = int(line) + 0.5
    return str(canonical).replace(".", "_")


def evaluate_match(
    home_lambda: float,
    away_lambda: float,
    home_odds: float,
    draw_odds: float | None,
    away_odds: float,
    ev_threshold: float = EV_THRESHOLD,
    max_goals: int = 8,
    over_odds: float | None = None,
    under_odds: float | None = None,
    totals_line: float | None = None,
    rho: float = 0.0,
) -> dict:
    """
    End-to-end evaluation for one match.
    Returns probabilities, EVs, and a list of outcomes that exceed the EV threshold.
    """
    score_matrix = build_score_matrix(home_lambda, away_lambda, max_goals, rho=rho)
    probs = calculate_match_probabilities(score_matrix)
    ou = calculate_over_under_probs(score_matrix, line=totals_line or 2.5)

    line_key = _fmt_line(totals_line) if totals_line is not None else "2_5"
    over_key  = f"over_{line_key}"
    under_key = f"under_{line_key}"

    # For draw-excluded (binary) markets, derive away prob as 1 - home so the
    # two sides are guaranteed complementary and sum to exactly 1.0.
    if draw_odds is None:
        binary_total = probs["home_win"] + probs["away_win"]
        home_prob = probs["home_win"] / binary_total if binary_total > 0 else 0.5
        away_prob = 1.0 - home_prob
    else:
        home_prob = probs["home_win"]
        away_prob = probs["away_win"]

    home_ev  = calculate_ev(home_prob, home_odds)
    away_ev  = calculate_ev(away_prob, away_odds)
    draw_ev  = calculate_ev(probs["draw"], draw_odds) if draw_odds is not None else None
    over_ev  = calculate_ev(ou["over"],  over_odds)  if over_odds  is not None else None
    under_ev = calculate_ev(ou["under"], under_odds) if under_odds is not None else None

    value_bets = []
    if home_ev >= ev_threshold:
        value_bets.append("home_win")
    if away_ev >= ev_threshold:
        value_bets.append("away_win")
    if draw_ev is not None and draw_ev >= ev_threshold:
        value_bets.append("draw")
    if over_ev is not None and over_ev >= ev_threshold:
        value_bets.append(over_key)
    if under_ev is not None and under_ev >= ev_threshold:
        value_bets.append(under_key)

    return {
        "home_win_prob": home_prob,
        "draw_prob":     probs["draw"],
        "away_win_prob": away_prob,
        over_key + "_prob":  ou["over"],
        under_key + "_prob": ou["under"],
        "home_ev":  home_ev,
        "draw_ev":  draw_ev,
        "away_ev":  away_ev,
        over_key + "_ev":  over_ev,
        under_key + "_ev": under_ev,
        "value_bets": value_bets,
        "over_key":   over_key,
        "under_key":  under_key,
    }
