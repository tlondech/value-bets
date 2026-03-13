import numpy as np
from scipy.stats import poisson


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
        matrix[0, 0] *= max(1.0 - lam1 * lam2 * rho, 1e-10)
        matrix[1, 0] *= max(1.0 + lam2 * rho,         1e-10)
        matrix[0, 1] *= max(1.0 + lam1 * rho,         1e-10)
        matrix[1, 1] *= max(1.0 - rho,                 1e-10)
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


def calculate_btts_prob(score_matrix: np.ndarray) -> float:
    """P(both teams score ≥ 1 goal). Pure Poisson → home/away goals are independent."""
    p_home_0 = float(score_matrix[0, :].sum())
    p_away_0 = float(score_matrix[:, 0].sum())
    return (1.0 - p_home_0) * (1.0 - p_away_0)


def calculate_ev(true_probability: float, decimal_odds: float) -> float:
    """
    EV = (true_probability * decimal_odds) - 1
    Positive EV means the bet has a positive expected return.
    """
    return (true_probability * decimal_odds) - 1.0


def evaluate_match(
    home_lambda: float,
    away_lambda: float,
    home_odds: float,
    draw_odds: float | None,
    away_odds: float,
    ev_threshold: float = 0.05,
    max_goals: int = 8,
    over_2_5_odds: float | None = None,
    under_2_5_odds: float | None = None,
    btts_yes_odds: float | None = None,
    btts_no_odds: float | None = None,
    rho: float = 0.0,
) -> dict:
    """
    End-to-end evaluation for one match.
    Returns probabilities, EVs, and a list of outcomes that exceed the EV threshold.
    """
    score_matrix = build_score_matrix(home_lambda, away_lambda, max_goals, rho=rho)
    probs = calculate_match_probabilities(score_matrix)
    ou = calculate_over_under_probs(score_matrix)
    btts_yes = calculate_btts_prob(score_matrix)

    home_ev = calculate_ev(probs["home_win"], home_odds)
    away_ev = calculate_ev(probs["away_win"], away_odds)
    draw_ev = calculate_ev(probs["draw"], draw_odds) if draw_odds is not None else None
    over_2_5_ev  = calculate_ev(ou["over"],  over_2_5_odds)  if over_2_5_odds  is not None else None
    under_2_5_ev = calculate_ev(ou["under"], under_2_5_odds) if under_2_5_odds is not None else None
    btts_yes_ev  = calculate_ev(btts_yes,          btts_yes_odds) if btts_yes_odds is not None else None
    btts_no_ev   = calculate_ev(1.0 - btts_yes,    btts_no_odds)  if btts_no_odds  is not None else None

    value_bets = []
    if home_ev >= ev_threshold:
        value_bets.append("home_win")
    if away_ev >= ev_threshold:
        value_bets.append("away_win")
    if draw_ev is not None and draw_ev >= ev_threshold:
        value_bets.append("draw")
    if over_2_5_ev is not None and over_2_5_ev >= ev_threshold:
        value_bets.append("over_2_5")
    if under_2_5_ev is not None and under_2_5_ev >= ev_threshold:
        value_bets.append("under_2_5")
    if btts_yes_ev is not None and btts_yes_ev >= ev_threshold:
        value_bets.append("btts_yes")
    if btts_no_ev is not None and btts_no_ev >= ev_threshold:
        value_bets.append("btts_no")

    return {
        "home_win_prob":  probs["home_win"],
        "draw_prob":      probs["draw"],
        "away_win_prob":  probs["away_win"],
        "over_2_5_prob":  ou["over"],
        "under_2_5_prob": ou["under"],
        "btts_yes_prob":  btts_yes,
        "btts_no_prob":   1.0 - btts_yes,
        "home_ev":        home_ev,
        "draw_ev":        draw_ev,
        "away_ev":        away_ev,
        "over_2_5_ev":    over_2_5_ev,
        "under_2_5_ev":   under_2_5_ev,
        "btts_yes_ev":    btts_yes_ev,
        "btts_no_ev":     btts_no_ev,
        "value_bets":     value_bets,
    }
