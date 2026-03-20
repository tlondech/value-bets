import difflib
import json
import logging
import math
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from constants import (
    DEFAULT_AVG_AWAY_GOALS,
    DEFAULT_AVG_HOME_GOALS,
    DIXON_COLES_ATTACK_DEFENSE_BOUND,
    DIXON_COLES_FTOL,
    DIXON_COLES_GAMMA_BOUNDS,
    DIXON_COLES_INIT_GAMMA,
    DIXON_COLES_INIT_RHO,
    DIXON_COLES_MAX_ITER,
    DIXON_COLES_MIN_FIXTURES,
    DIXON_COLES_L2_REG,
    DIXON_COLES_RHO_BOUNDS,
    DIXON_COLES_RHO_FLOOR,
    DIXON_COLES_XI,
    EXP_DECAY_WEIGHT,
    FATIGUE_FACTOR,
    FATIGUE_THRESHOLD_DAYS,
    H2H_BLEND_WEIGHT,
    H2H_MIN_FIXTURES,
    LAMBDA_MAX,
    LAMBDA_MIN,
    MIN_TEAM_FIXTURES,
)

logger = logging.getLogger(__name__)

_warned_unmapped: set[tuple[str, str]] = set()


def _apply_agg_adjustment(
    home_lambda: float,
    away_lambda: float,
    leg2_context: dict | None,
) -> tuple[float, float]:
    """
    Adjusts lambdas for a UCL Leg 2 match based on aggregate score going in.

    agg_diff = agg_home - agg_away:
      > 0  → Leg 2 home team leads; away team must attack
      < 0  → Leg 2 home team trails; home team must attack
      = 0  → Tied; no adjustment
    """
    if leg2_context is None:
        return home_lambda, away_lambda

    from config import AGG_ATTACK_BOOST, AGG_DEFEND_FACTOR, AGG_MIN_MULT

    agg_diff = leg2_context["agg_diff"]
    if agg_diff == 0:
        return home_lambda, away_lambda

    if agg_diff < 0:
        # Home team is behind — must attack; away team can defend
        gap = abs(agg_diff)
        home_lambda *= 1 + AGG_ATTACK_BOOST * gap
        away_lambda *= max(AGG_MIN_MULT, 1 - AGG_DEFEND_FACTOR * gap)
    else:
        # Away team is behind — must attack; home team can defend
        away_lambda *= 1 + AGG_ATTACK_BOOST * agg_diff
        home_lambda *= max(AGG_MIN_MULT, 1 - AGG_DEFEND_FACTOR * agg_diff)

    return home_lambda, away_lambda


def load_team_name_map(path: str) -> dict[str, dict[str, str]]:
    """
    Loads the Winamax→API-Football name mapping from JSON.
    Returns a nested dict keyed by league_key.
    Top-level keys starting with '_' (metadata) are ignored.
    Raises ValueError on duplicate keys so CI catches copy-paste errors.
    """
    def _raise_on_duplicates(pairs):
        result = {}
        for k, v in pairs:
            if k in result:
                raise ValueError(f"Duplicate key in team_name_map.json: {k!r}")
            result[k] = v
        return result

    with open(path, encoding="utf-8") as f:
        raw = json.load(f, object_pairs_hook=_raise_on_duplicates)
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def auto_patch_name_map(
    league_key: str,
    winamax_names: set[str],
    espn_names: set[str],
    name_map: dict,
    map_path: str,
    threshold: float = 0.85,
) -> int:
    """Fuzzy-match unmapped Winamax names against known ESPN names for a league.

    For each Winamax name that has no entry in name_map[league_key], attempts a
    fuzzy match against ``espn_names``.  Matches with a ratio >= ``threshold`` are
    written back to the JSON file and into the live ``name_map`` dict so the
    current run can use them immediately.

    Returns the number of new mappings added.
    """
    league_dict = name_map.setdefault(league_key, {})
    new_entries: dict[str, str] = {}

    for winamax_name in winamax_names:
        if winamax_name in league_dict:
            continue
        matches = difflib.get_close_matches(winamax_name, espn_names, n=1, cutoff=threshold)
        if not matches:
            continue
        espn_name = matches[0]
        if espn_name == winamax_name:
            continue  # identity — no mapping needed
        new_entries[winamax_name] = espn_name
        logger.info(
            "[name-map] Auto-resolved '%s' → '%s' (league '%s')",
            winamax_name, espn_name, league_key,
        )

    if not new_entries:
        return 0

    league_dict.update(new_entries)

    with open(map_path, encoding="utf-8") as f:
        raw = json.load(f)
    raw.setdefault(league_key, {}).update(new_entries)
    raw["_meta"]["last_updated"] = __import__("datetime").date.today().isoformat()
    with open(map_path, "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2, ensure_ascii=False)

    return len(new_entries)


def resolve_team_name(winamax_name: str, name_map: dict, league_key: str) -> str | None:
    """
    Returns the API-Football canonical name for a Winamax team name within a given league.
    Returns None and logs a warning if no mapping is found.
    """
    league_dict = name_map.get(league_key, {})
    canonical = league_dict.get(winamax_name)
    if canonical is None:
        key = (league_key, winamax_name)
        if key not in _warned_unmapped:
            _warned_unmapped.add(key)
            logger.warning(
                "No mapping: '%s' in '%s' — add to data/team_name_map.json",
                winamax_name, league_key,
            )
    return canonical


def build_fixtures_dataframe(raw_fixtures: list[dict]) -> pd.DataFrame:
    """
    Converts fixtures into a flat DataFrame.
    Derives home_goals_eff / away_goals_eff: uses xG when present, falls back to actual goals.
    Sorted ascending by date.
    """
    if not raw_fixtures:
        return pd.DataFrame(columns=[
            "fixture_date", "home_team", "away_team",
            "home_goals", "away_goals", "home_goals_eff", "away_goals_eff",
        ])

    df = pd.DataFrame(raw_fixtures)
    df["fixture_date"] = pd.to_datetime(df["fixture_date"], utc=True)
    df = df.sort_values("fixture_date").reset_index(drop=True)

    # Use xG when available, fall back to actual goals
    home_xg = df["home_xg"] if "home_xg" in df.columns else pd.Series(dtype=float)
    away_xg = df["away_xg"] if "away_xg" in df.columns else pd.Series(dtype=float)
    df["home_goals_eff"] = home_xg.where(home_xg.notna(), df["home_goals"])
    df["away_goals_eff"] = away_xg.where(away_xg.notna(), df["away_goals"])
    return df


def compute_standings(raw_fixtures: list[dict]) -> dict:
    """
    Derives league table from finished fixtures.
    Returns {"rankings": {team_name: position}, "total_matchdays": int | None}.
    team_name matches the canonical names in raw_fixtures (home_team/away_team).
    """
    from collections import defaultdict

    stats: dict[str, dict] = defaultdict(lambda: {"pts": 0, "gf": 0, "ga": 0})
    teams: set[str] = set()

    for f in raw_fixtures:
        h, a = f["home_team"], f["away_team"]
        hg, ag = int(f["home_goals"]), int(f["away_goals"])
        teams.update([h, a])
        stats[h]["gf"] += hg; stats[h]["ga"] += ag
        stats[a]["gf"] += ag; stats[a]["ga"] += hg
        if hg > ag:
            stats[h]["pts"] += 3
        elif ag > hg:
            stats[a]["pts"] += 3
        else:
            stats[h]["pts"] += 1; stats[a]["pts"] += 1

    n_teams = len(teams)
    total_matchdays = (n_teams - 1) * 2 if n_teams > 1 else None

    ranked = sorted(
        teams,
        key=lambda t: (stats[t]["pts"], stats[t]["gf"] - stats[t]["ga"], stats[t]["gf"]),
        reverse=True,
    )
    return {
        "rankings": {team: pos + 1 for pos, team in enumerate(ranked)},
        "total_matchdays": total_matchdays,
    }


def compute_form(raw_fixtures: list[dict], n: int = 5) -> dict[str, list[str]]:
    """
    Returns the last n results per team as ["W", "D", "L"] lists (oldest → newest).
    Keys match canonical team names in raw_fixtures.
    """
    from collections import defaultdict

    team_matches: dict[str, list[tuple]] = defaultdict(list)
    for f in raw_fixtures:
        h, a = f["home_team"], f["away_team"]
        hg, ag = int(f["home_goals"]), int(f["away_goals"])
        date = f["fixture_date"]
        if hg > ag:
            h_res, a_res = "W", "L"
        elif ag > hg:
            h_res, a_res = "L", "W"
        else:
            h_res = a_res = "D"
        team_matches[h].append((date, h_res))
        team_matches[a].append((date, a_res))

    return {
        team: [r for _, r in sorted(matches, key=lambda x: x[0])[-n:]]
        for team, matches in team_matches.items()
    }


def compute_league_averages(fixtures_df: pd.DataFrame) -> dict:
    """
    Computes league-wide average goals per match (home and away separately).
    """
    if fixtures_df.empty:
        return {
            "avg_home_goals": DEFAULT_AVG_HOME_GOALS,
            "avg_away_goals": DEFAULT_AVG_AWAY_GOALS,
            "avg_total_goals": DEFAULT_AVG_HOME_GOALS + DEFAULT_AVG_AWAY_GOALS,
        }

    avg_home = fixtures_df["home_goals_eff"].mean()
    avg_away = fixtures_df["away_goals_eff"].mean()
    return {
        "avg_home_goals": avg_home,
        "avg_away_goals": avg_away,
        "avg_total_goals": avg_home + avg_away,
    }


def _exp_weighted_mean(series: pd.Series, decay: float = EXP_DECAY_WEIGHT) -> float:
    """
    Exponentially-decayed weighted mean.
    Most recent row = weight 1.0, second most recent = decay, etc.
    Emphasises last 2–3 matches without discarding older ones entirely.
    """
    n = len(series)
    if n == 0:
        return 0.0
    weights = [decay ** i for i in range(n - 1, -1, -1)]
    return float(np.average(series.to_numpy(dtype=float), weights=weights))


def compute_team_attack_defense(
    fixtures_df: pd.DataFrame,
    team_name: str,
    rolling_window: int = 5,
) -> dict | None:
    """
    Computes rolling attack/defense averages for a team using the last
    `rolling_window` home matches and last `rolling_window` away matches.
    Values are exponentially weighted so more recent matches carry more weight.

    Returns:
        {
            "home_attack": float,   weighted avg goals scored at home
            "home_defense": float,  weighted avg goals conceded at home
            "away_attack": float,   weighted avg goals scored away
            "away_defense": float,  weighted avg goals conceded away
        }
    Returns None if the team has fewer than 3 home or away matches (insufficient data).
    """
    home_matches = fixtures_df[fixtures_df["home_team"] == team_name].tail(rolling_window)
    away_matches = fixtures_df[fixtures_df["away_team"] == team_name].tail(rolling_window)

    if len(home_matches) < MIN_TEAM_FIXTURES or len(away_matches) < MIN_TEAM_FIXTURES:
        logger.debug(
            "Team '%s' has insufficient fixture history (%d home, %d away). Skipping.",
            team_name, len(home_matches), len(away_matches),
        )
        return None

    return {
        "home_attack":  _exp_weighted_mean(home_matches["home_goals_eff"]),
        "home_defense": _exp_weighted_mean(home_matches["away_goals_eff"]),
        "away_attack":  _exp_weighted_mean(away_matches["away_goals_eff"]),
        "away_defense": _exp_weighted_mean(away_matches["home_goals_eff"]),
    }


def compute_h2h_stats(
    fixtures_df: pd.DataFrame,
    home_team: str,
    away_team: str,
) -> dict | None:
    """
    Computes attack/defense averages derived only from past meetings between
    these two teams (regardless of which side was home/away).

    Returns the same shape as compute_team_attack_defense, but from H2H fixtures only.
    Returns None if fewer than _H2H_MIN_FIXTURES meetings exist.
    """
    h2h = fixtures_df[
        ((fixtures_df["home_team"] == home_team) & (fixtures_df["away_team"] == away_team)) |
        ((fixtures_df["home_team"] == away_team) & (fixtures_df["away_team"] == home_team))
    ]
    if len(h2h) < H2H_MIN_FIXTURES:
        return None

    # Matches where home_team played at home
    as_home = h2h[h2h["home_team"] == home_team]
    # Matches where home_team played away
    as_away = h2h[h2h["away_team"] == home_team]

    if as_home.empty or as_away.empty:
        # All H2H fixtures on one side — not enough directional data
        return None

    return {
        "home_attack":  _exp_weighted_mean(as_home["home_goals_eff"]),
        "home_defense": _exp_weighted_mean(as_home["away_goals_eff"]),
        "away_attack":  _exp_weighted_mean(as_away["away_goals_eff"]),
        "away_defense": _exp_weighted_mean(as_away["home_goals_eff"]),
    }


def compute_rest_days(
    fixtures_df: pd.DataFrame,
    team: str,
    before_date: datetime,
) -> int | None:
    """
    Returns the number of days since the team's most recent finished match
    before `before_date`. Returns None if no prior fixture is found.
    """
    past = fixtures_df[
        (fixtures_df["home_team"] == team) | (fixtures_df["away_team"] == team)
    ]
    past = past[past["fixture_date"] < before_date]
    if past.empty:
        return None
    last = past["fixture_date"].max()
    # both are timezone-aware (UTC)
    delta = before_date - last
    return delta.days


def build_poisson_inputs(
    home_team: str,
    away_team: str,
    fixtures_df: pd.DataFrame,
    league_avgs: dict,
    rolling_window: int = 5,
    match_date: datetime | None = None,
    all_fixtures_df: pd.DataFrame | None = None,
    h2h_fixtures_df: pd.DataFrame | None = None,
    home_universal: str | None = None,
    away_universal: str | None = None,
    leg2_context: dict | None = None,
) -> dict | None:
    """
    Calculates the expected goals (lambda) for each team using the Dixon-Coles
    attack/defense rating method, enhanced with:
      - Exponential recency weighting on the rolling window
      - H2H blend (30%) when ≥ 3 head-to-head fixtures exist
      - Fatigue multiplier when a team has < 4 days rest

    home_λ = (home_attack / league_avg_home) * (away_defense / league_avg_away) * league_avg_home
    away_λ = (away_attack / league_avg_away) * (home_defense / league_avg_home) * league_avg_away

    Returns a dict with home_lambda, away_lambda, h2h_used, home_rest_days, away_rest_days,
    or None if data is insufficient.
    """
    home_stats = compute_team_attack_defense(fixtures_df, home_team, rolling_window)
    away_stats = compute_team_attack_defense(fixtures_df, away_team, rolling_window)

    if home_stats is None or away_stats is None:
        return None

    avg_h = league_avgs["avg_home_goals"]
    avg_a = league_avgs["avg_away_goals"]

    # Avoid division by zero on new seasons with no data
    if avg_h == 0 or avg_a == 0:
        logger.warning("League averages are zero — not enough fixtures played yet.")
        return None

    home_lambda = (home_stats["home_attack"] / avg_h) * (away_stats["away_defense"] / avg_a) * avg_h
    away_lambda = (away_stats["away_attack"] / avg_a) * (home_stats["home_defense"] / avg_h) * avg_a

    # H2H blend — use multi-season data if available, else fall back to current season
    h2h_used = False
    h2h = compute_h2h_stats(h2h_fixtures_df if h2h_fixtures_df is not None else fixtures_df, home_team, away_team)
    if h2h is not None:
        h2h_home_lambda = (h2h["home_attack"] / avg_h) * (h2h["away_defense"] / avg_a) * avg_h
        h2h_away_lambda = (h2h["away_attack"] / avg_a) * (h2h["home_defense"] / avg_h) * avg_a
        home_lambda = (1 - H2H_BLEND_WEIGHT) * home_lambda + H2H_BLEND_WEIGHT * h2h_home_lambda
        away_lambda = (1 - H2H_BLEND_WEIGHT) * away_lambda + H2H_BLEND_WEIGHT * h2h_away_lambda
        h2h_used = True
        logger.debug(
            "H2H blend applied for %s vs %s (h2h_λ home=%.2f away=%.2f)",
            home_team, away_team, h2h_home_lambda, h2h_away_lambda,
        )

    # Rest days / fatigue
    home_rest_days: int | None = None
    away_rest_days: int | None = None
    if match_date is not None:
        # Ensure match_date is timezone-aware for comparison with UTC fixture_date
        if match_date.tzinfo is None:
            match_date = match_date.replace(tzinfo=timezone.utc)
        rest_df = all_fixtures_df if all_fixtures_df is not None else fixtures_df
        home_lookup = home_universal if home_universal is not None else home_team
        away_lookup = away_universal if away_universal is not None else away_team
        home_rest_days = compute_rest_days(rest_df, home_lookup, match_date)
        away_rest_days = compute_rest_days(rest_df, away_lookup, match_date)
        if home_rest_days is not None and home_rest_days < FATIGUE_THRESHOLD_DAYS:
            away_lambda *= FATIGUE_FACTOR
            logger.debug(
                "%s fatigued (%dd rest) → away_λ nudged to %.2f",
                home_team, home_rest_days, away_lambda,
            )
        if away_rest_days is not None and away_rest_days < FATIGUE_THRESHOLD_DAYS:
            home_lambda *= FATIGUE_FACTOR
            logger.debug(
                "%s fatigued (%dd rest) → home_λ nudged to %.2f",
                away_team, away_rest_days, home_lambda,
            )

    # Aggregate adjustment (UCL Leg 2 only; no-op when leg2_context is None)
    home_lambda, away_lambda = _apply_agg_adjustment(home_lambda, away_lambda, leg2_context)
    if leg2_context:
        logger.debug(
            "Leg 2 agg adjustment (rolling): agg_diff=%+d → home_λ=%.2f away_λ=%.2f",
            leg2_context["agg_diff"], home_lambda, away_lambda,
        )

    # Clamp to reasonable range to prevent degenerate Poisson inputs
    home_lambda = max(LAMBDA_MIN, min(home_lambda, LAMBDA_MAX))
    away_lambda = max(LAMBDA_MIN, min(away_lambda, LAMBDA_MAX))

    return {
        "home_lambda":    home_lambda,
        "away_lambda":    away_lambda,
        "h2h_used":       h2h_used,
        "home_rest_days": home_rest_days,
        "away_rest_days": away_rest_days,
    }


def fit_dixon_coles(
    fixtures_df: pd.DataFrame,
    xi: float = DIXON_COLES_XI,
    min_fixtures: int = DIXON_COLES_MIN_FIXTURES,
) -> dict | None:
    """
    Fits the Dixon-Coles (1997) model via Maximum Likelihood Estimation.

    Simultaneously estimates for every team:
      - α (attack strength)
      - β (defence weakness)
    Plus global parameters:
      - γ (home advantage)
      - ρ (low-score correction)

    Lambda formula:
      home_λ = exp(α_home + β_away + γ)
      away_λ = exp(α_away + β_home)

    Returns a dict with keys "attack", "defense", "gamma", "rho", "n_fixtures",
    or None if fewer than min_fixtures are available.
    """
    if len(fixtures_df) < min_fixtures:
        return None

    teams = sorted(set(fixtures_df["home_team"]) | set(fixtures_df["away_team"]))
    n = len(teams)
    if n < 2:
        return None
    team_idx = {t: i for i, t in enumerate(teams)}

    # Time-decay weights: most recent fixture gets weight 1.0
    reference_date = fixtures_df["fixture_date"].max()
    rows = []
    for _, r in fixtures_df.iterrows():
        days_ago = (reference_date - r["fixture_date"]).days
        w = math.exp(-xi * days_ago)
        rows.append((
            team_idx[r["home_team"]],
            team_idx[r["away_team"]],
            int(r["home_goals"]),        # integer actual goals for τ correction
            int(r["away_goals"]),
            float(r["home_goals_eff"]),  # xG (or actual) for Poisson likelihood
            float(r["away_goals_eff"]),
            w,
        ))

    def _tau(x: int, y: int, lam1: float, lam2: float, rho: float) -> float:
        if   x == 0 and y == 0: return 1.0 - lam1 * lam2 * rho
        elif x == 1 and y == 0: return 1.0 + lam2 * rho
        elif x == 0 and y == 1: return 1.0 + lam1 * rho
        elif x == 1 and y == 1: return 1.0 - rho
        else:                   return 1.0

    def _unpack(params):
        # α: teams[0] fixed at 0 (identifiability); remaining n-1 are free
        alpha = [0.0] + list(params[:n - 1])
        beta  = list(params[n - 1: 2 * n - 1])
        gamma = float(params[2 * n - 1])
        rho   = float(params[2 * n])
        return alpha, beta, gamma, rho

    def _neg_log_likelihood(params):
        alpha, beta, gamma, rho = _unpack(params)
        total = 0.0
        for hi, ai, x_int, y_int, x_eff, y_eff, w in rows:
            lam1 = math.exp(alpha[hi] + beta[ai] + gamma)
            lam2 = math.exp(alpha[ai] + beta[hi])
            tau_val = max(_tau(x_int, y_int, lam1, lam2, rho), DIXON_COLES_RHO_FLOOR)
            ll = (
                math.log(tau_val)
                + x_eff * math.log(lam1) - lam1 - math.lgamma(x_eff + 1)
                + y_eff * math.log(lam2) - lam2 - math.lgamma(y_eff + 1)
            )
            total += w * ll
        # L2 penalty on attack/defense params (alpha[0] is fixed at 0, skip it)
        l2 = DIXON_COLES_L2_REG * (
            sum(a ** 2 for a in alpha[1:]) + sum(b ** 2 for b in beta)
        )
        return -total + l2

    # Parameter vector: α₁…α_{n-1}, β₀…β_{n-1}, γ, ρ  (length = 2n+1)
    x0 = np.zeros(2 * n + 1)
    x0[2 * n - 1] = DIXON_COLES_INIT_GAMMA
    x0[2 * n]     = DIXON_COLES_INIT_RHO

    _ab = DIXON_COLES_ATTACK_DEFENSE_BOUND
    bounds = (
        [(-_ab, _ab)] * (n - 1)              # α (team 0 is fixed at 0)
        + [(-_ab, _ab)] * n                  # β
        + [DIXON_COLES_GAMMA_BOUNDS]          # γ
        + [DIXON_COLES_RHO_BOUNDS]            # ρ
    )

    result = minimize(
        _neg_log_likelihood,
        x0,
        method="L-BFGS-B",
        bounds=bounds,
        options={"maxiter": DIXON_COLES_MAX_ITER, "maxfun": DIXON_COLES_MAX_ITER * 20, "ftol": DIXON_COLES_FTOL},
    )
    if not result.success:
        logger.warning("Dixon-Coles optimisation did not fully converge: %s", result.message)

    alpha_arr, beta_arr, gamma, rho = _unpack(result.x)
    return {
        "attack":     {teams[i]: alpha_arr[i] for i in range(n)},
        "defense":    {teams[i]: beta_arr[i]  for i in range(n)},
        "gamma":      gamma,
        "rho":        rho,
        "n_fixtures": len(rows),
    }


def build_poisson_inputs_dc(
    home_team: str,
    away_team: str,
    dc_params: dict,
    match_date: datetime | None = None,
    all_fixtures_df: pd.DataFrame | None = None,
    h2h_fixtures_df: pd.DataFrame | None = None,
    home_universal: str | None = None,
    away_universal: str | None = None,
    leg2_context: dict | None = None,
) -> dict | None:
    """
    Computes expected goals (λ) from pre-fitted Dixon-Coles parameters.

    Returns the same dict shape as build_poisson_inputs(), or None if either
    team is absent from the fitted parameters (e.g. a promoted/unknown team).
    """
    if home_team not in dc_params["attack"] or away_team not in dc_params["attack"]:
        logger.debug(
            "DC params missing team(s): %s / %s — will fall back to rolling-window.",
            home_team, away_team,
        )
        return None

    alpha = dc_params["attack"]
    beta  = dc_params["defense"]
    gamma = dc_params["gamma"]

    home_lambda = math.exp(alpha[home_team] + beta[away_team] + gamma)
    away_lambda = math.exp(alpha[away_team] + beta[home_team])

    # H2H blend (same logic as rolling-window path)
    h2h_used = False
    if h2h_fixtures_df is not None:
        avg_h = float(h2h_fixtures_df["home_goals_eff"].mean()) if len(h2h_fixtures_df) else 0.0
        avg_a = float(h2h_fixtures_df["away_goals_eff"].mean()) if len(h2h_fixtures_df) else 0.0
        if avg_h > 0 and avg_a > 0:
            h2h = compute_h2h_stats(h2h_fixtures_df, home_team, away_team)
            if h2h is not None:
                h2h_home_lambda = (h2h["home_attack"] / avg_h) * (h2h["away_defense"] / avg_a) * avg_h
                h2h_away_lambda = (h2h["away_attack"] / avg_a) * (h2h["home_defense"] / avg_h) * avg_a
                home_lambda = (1 - H2H_BLEND_WEIGHT) * home_lambda + H2H_BLEND_WEIGHT * h2h_home_lambda
                away_lambda = (1 - H2H_BLEND_WEIGHT) * away_lambda + H2H_BLEND_WEIGHT * h2h_away_lambda
                h2h_used = True
                logger.debug(
                    "H2H blend applied (DC) for %s vs %s (h2h_λ home=%.2f away=%.2f)",
                    home_team, away_team, h2h_home_lambda, h2h_away_lambda,
                )

    # Fatigue adjustment (identical logic to build_poisson_inputs)
    home_rest_days: int | None = None
    away_rest_days: int | None = None
    if match_date is not None:
        if match_date.tzinfo is None:
            match_date = match_date.replace(tzinfo=timezone.utc)
        rest_df = all_fixtures_df if all_fixtures_df is not None else pd.DataFrame()
        home_lookup = home_universal if home_universal is not None else home_team
        away_lookup = away_universal if away_universal is not None else away_team
        home_rest_days = compute_rest_days(rest_df, home_lookup, match_date)
        away_rest_days = compute_rest_days(rest_df, away_lookup, match_date)
        if home_rest_days is not None and home_rest_days < FATIGUE_THRESHOLD_DAYS:
            away_lambda *= FATIGUE_FACTOR
            logger.debug(
                "%s fatigued (%dd rest) → away_λ nudged to %.2f",
                home_team, home_rest_days, away_lambda,
            )
        if away_rest_days is not None and away_rest_days < FATIGUE_THRESHOLD_DAYS:
            home_lambda *= FATIGUE_FACTOR
            logger.debug(
                "%s fatigued (%dd rest) → home_λ nudged to %.2f",
                away_team, away_rest_days, home_lambda,
            )

    # Aggregate adjustment (UCL Leg 2 only; no-op when leg2_context is None)
    home_lambda, away_lambda = _apply_agg_adjustment(home_lambda, away_lambda, leg2_context)
    if leg2_context:
        logger.debug(
            "Leg 2 agg adjustment (DC): agg_diff=%+d → home_λ=%.2f away_λ=%.2f",
            leg2_context["agg_diff"], home_lambda, away_lambda,
        )

    home_lambda = max(LAMBDA_MIN, min(home_lambda, LAMBDA_MAX))
    away_lambda = max(LAMBDA_MIN, min(away_lambda, LAMBDA_MAX))

    return {
        "home_lambda":    home_lambda,
        "away_lambda":    away_lambda,
        "h2h_used":       h2h_used,
        "home_rest_days": home_rest_days,
        "away_rest_days": away_rest_days,
    }
