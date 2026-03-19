"""
Surface-adjusted Elo model for tennis.

Elo ratings are computed from Jeff Sackmann historical match data and blended
across overall + surface-specific pools. Win probability is then compared to
Winamax odds using the existing calculate_ev function to find +EV signals.
"""
import logging

import pandas as pd

from constants import EV_THRESHOLD
from models.evaluator import calculate_ev

logger = logging.getLogger(__name__)

# IOC 3-letter country code → ISO 3166-1 alpha-2 (for flag CDN)
_IOC_TO_ISO2: dict[str, str] = {
    "AFG": "af", "ALB": "al", "ALG": "dz", "AND": "ad", "ANG": "ao",
    "ARG": "ar", "ARM": "am", "AUS": "au", "AUT": "at", "AZE": "az",
    "BAH": "bs", "BAN": "bd", "BAR": "bb", "BEL": "be", "BEN": "bj",
    "BER": "bm", "BIH": "ba", "BLR": "by", "BOL": "bo", "BRA": "br",
    "BRN": "bh", "BUL": "bg", "CAN": "ca", "CHI": "cl", "CHN": "cn",
    "CIV": "ci", "CMR": "cm", "COL": "co", "CRC": "cr", "CRO": "hr",
    "CYP": "cy", "CZE": "cz", "DEN": "dk", "DOM": "do", "ECU": "ec",
    "EGY": "eg", "ESP": "es", "EST": "ee", "ETH": "et", "FIN": "fi",
    "FRA": "fr", "GBR": "gb", "GEO": "ge", "GER": "de", "GHA": "gh",
    "GRE": "gr", "GTM": "gt", "HKG": "hk", "HUN": "hu", "INA": "id",
    "IND": "in", "IRI": "ir", "IRL": "ie", "ISL": "is", "ISR": "il",
    "ITA": "it", "JAM": "jm", "JOR": "jo", "JPN": "jp", "KAZ": "kz",
    "KEN": "ke", "KGZ": "kg", "KOR": "kr", "KUW": "kw", "LAT": "lv",
    "LBA": "ly", "LIE": "li", "LTU": "lt", "LUX": "lu", "MAR": "ma",
    "MDA": "md", "MEX": "mx", "MKD": "mk", "MNE": "me", "MON": "mc",
    "MRI": "mu", "NED": "nl", "NOR": "no", "NZL": "nz", "PAK": "pk",
    "PAR": "py", "PER": "pe", "PHI": "ph", "POL": "pl", "POR": "pt",
    "PUR": "pr", "QAT": "qa", "ROU": "ro", "RSA": "za", "RUS": "ru",
    "SAU": "sa", "SEN": "sn", "SLO": "si", "SRB": "rs", "SRI": "lk",
    "SUI": "ch", "SVK": "sk", "SWE": "se", "THA": "th", "TPE": "tw",
    "TTO": "tt", "TUN": "tn", "TUR": "tr", "UAE": "ae", "UGA": "ug",
    "UKR": "ua", "URU": "uy", "USA": "us", "UZB": "uz", "VEN": "ve",
    "VIE": "vn", "ZIM": "zw",
}

FLAGCDN_BASE = "https://flagcdn.com/w40"

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
            ratings[player]["n_matches"] = 0

    for _, row in matches.iterrows():
        w = str(row["winner_name"])
        l = str(row["loser_name"])
        surface = str(row.get("surface", "Hard"))
        level = str(row.get("tourney_level", "D"))
        K = K_BY_LEVEL.get(level, 20)

        _init(w)
        _init(l)
        ratings[w]["n_matches"] += 1
        ratings[l]["n_matches"] += 1

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
    max_prob_ratio: float = 1.3,
    min_matches: int = 10,
) -> list[dict]:
    """
    Returns a list of signals for one match.
    Returns [] if either player has no Elo history or too few matches.
    """
    if player1 not in ratings or player2 not in ratings:
        logger.debug(
            "No Elo history — skipping %s vs %s (missing: %s)",
            player1, player2,
            ", ".join(p for p in (player1, player2) if p not in ratings),
        )
        return []

    p1_n = ratings[player1].get("n_matches", 0)
    p2_n = ratings[player2].get("n_matches", 0)
    if p1_n < min_matches or p2_n < min_matches:
        logger.debug(
            "Skipping %s vs %s — insufficient match history (%d, %d < min %d)",
            player1, player2, p1_n, p2_n, min_matches,
        )
        return []

    elo1 = blended_elo(ratings, player1, surface)
    elo2 = blended_elo(ratings, player2, surface)
    p1_wins = 1.0 / (1.0 + 10.0 ** ((elo2 - elo1) / 400.0))
    p2_wins = 1.0 - p1_wins

    signals = []
    for outcome, true_prob, odds, label in (
        ("home_win", p1_wins, p1_odds, "Player 1 Win"),
        ("away_win", p2_wins, p2_odds, "Player 2 Win"),
    ):
        ev = calculate_ev(true_prob, odds)
        if ev >= ev_threshold and true_prob * odds <= max_prob_ratio:
            signals.append({
                "outcome":       outcome,
                "outcome_label": label,
                "odds":          odds,
                "true_prob":     round(true_prob, 6),
                "ev":            round(ev, 6),
            })
    return signals


def build_player_country_map(matches: pd.DataFrame) -> dict[str, str]:
    """
    Returns {player_name: flag_url} derived from winner_ioc/loser_ioc columns.
    Uses the most recently seen IOC code for each player.
    Returns an empty dict if country columns are absent.
    """
    if matches.empty or "winner_ioc" not in matches.columns:
        return {}

    country: dict[str, str] = {}
    for _, row in matches.iterrows():
        for name_col, ioc_col in (("winner_name", "winner_ioc"), ("loser_name", "loser_ioc")):
            name = str(row.get(name_col, ""))
            ioc = str(row.get(ioc_col, ""))
            if name and ioc and ioc != "nan":
                country[name] = ioc

    result = {}
    for player, ioc in country.items():
        iso2 = _IOC_TO_ISO2.get(ioc)
        if iso2:
            result[player] = f"{FLAGCDN_BASE}/{iso2}.png"
    return result
