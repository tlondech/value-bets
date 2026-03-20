import os
from dataclasses import dataclass, field
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def _current_season() -> int:
    now = datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def _current_nba_season() -> str:
    """Returns the current NBA season string, e.g. '2024-25'.
    NBA season starts in October; July–September is off-season.
    """
    now = datetime.now()
    # Season year is the year the season starts (October)
    if now.month >= 10:
        start_year = now.year
    elif now.month <= 6:
        start_year = now.year - 1
    else:
        # July–September: off-season; return the upcoming season
        start_year = now.year
    return f"{start_year}-{str(start_year + 1)[2:]}"


# UCL 2nd-leg aggregate adjustment multipliers
AGG_ATTACK_BOOST  = 0.15   # lambda boost per goal deficit for trailing team
AGG_DEFEND_FACTOR = 0.05   # lambda penalty per goal lead for leading team
AGG_MIN_MULT      = 0.85   # floor multiplier for the leading team's attack lambda


@dataclass
class LeagueConfig:
    key: str                           # slug used in team_name_map and DB (e.g. "epl")
    display_name: str                  # shown in HTML report badge (e.g. "Premier League")
    odds_sport: str                    # The Odds API sport key
    season_override: int | None = None # set only for competitions with non-standard seasons
    sport_type: str = "football"       # "football" | "tennis" | "basketball"


LEAGUES: list[LeagueConfig] = [
    LeagueConfig("ligue1",          "Ligue 1",                   "soccer_france_ligue_one"),
    LeagueConfig("ligue2",          "Ligue 2",                   "soccer_france_ligue_two"),
    LeagueConfig("coupedefrance",    "Coupe de France",           "soccer_france_coupe_de_france"),
    LeagueConfig("epl",             "Premier League",            "soccer_epl"),
    LeagueConfig("facup",           "FA Cup",                    "soccer_fa_cup"),
    LeagueConfig("eflcup",          "EFL Cup",                   "soccer_england_efl_cup"),
    LeagueConfig("laliga",          "La Liga",                   "soccer_spain_la_liga"),
    LeagueConfig("copadelrey",      "Copa del Rey",              "soccer_spain_copa_del_rey"),
    LeagueConfig("bundesliga",      "Bundesliga",                "soccer_germany_bundesliga"),
    LeagueConfig("dfbpokal",        "DFB-Pokal",                 "soccer_germany_dfb_pokal"),
    LeagueConfig("seriea",          "Serie A",                   "soccer_italy_serie_a"),
    LeagueConfig("coppaditalia",    "Coppa Italia",              "soccer_italy_coppa_italia"),
    LeagueConfig("ucl",             "Champions League",          "soccer_uefa_champs_league"),
    LeagueConfig("uel",             "Europa League",             "soccer_uefa_europa_league"),
    LeagueConfig("uecl",            "Conference League",         "soccer_uefa_europa_conference_league"),
    LeagueConfig("uefanations",     "UEFA Nations League",       "soccer_uefa_nations_league"),
    LeagueConfig("euroqual",        "Euro Qualification",        "soccer_uefa_euro_qualification"),
    LeagueConfig("worldcup",        "World Cup",                 "soccer_fifa_world_cup",                    season_override=2026),
    LeagueConfig("wcqualeurope",    "WC Qualifiers Europe",      "soccer_fifa_world_cup_qualifiers_europe"),
    LeagueConfig("nba", "NBA", "basketball_nba", sport_type="basketball"),
]

_LEAGUES_BY_KEY: dict[str, LeagueConfig] = {lg.key: lg for lg in LEAGUES}


@dataclass
class Config:
    # API credentials
    odds_api_key: str

    # Leagues to process in this run
    enabled_leagues: list[LeagueConfig] = field(default_factory=lambda: list(LEAGUES))

    # The Odds API shared settings (apply to all leagues)
    odds_region: str = "eu"
    odds_bookmaker: str = "winamax_fr"
    odds_market: str = "h2h"
    odds_format: str = "decimal"
    odds_totals_bookmakers: str = ""   # extra bookmakers for O/U 2.5 fallback (e.g. "pinnacle")

    # Tennis Elo ratings — computed once per run in main.py and shared across all tennis leagues
    atp_elo: dict = field(default_factory=dict)
    wta_elo: dict = field(default_factory=dict)

    # NBA team ratings — computed once per run in main.py
    nba_ratings: dict = field(default_factory=dict)
    nba_min_games: int = 10          # minimum games required for a team to generate signals
    nba_home_advantage: float = 3.5  # home court advantage in points
    nba_spread_std: float = 15.5     # std dev of point differential (Normal dist)
    nba_total_std: float = 19.0      # std dev of total points (Normal dist)

    # Model settings
    ev_threshold: float = 0.05
    poisson_max_goals: int = 8
    rolling_window: int = 5
    max_prob_ratio: float = 1.3        # max model_prob / implied_prob; UCL uses 1.4
    tennis_max_prob_ratio: float = 1.5 # looser cap for tennis Elo (less data history than football)
    tennis_min_matches: int = 10       # min historical matches required for a player to generate signals

    # Paths
    db_path: str = "data/signals.db"
    team_map_path: str = "data/team_name_map.json"
    football_crest_map_path: str = "data/football_crest_map.json"
    tennis_crest_map_path: str = "data/tennis_crest_map.json"
    nba_crest_map_path: str = "data/nba_crest_map.json"
    report_html_path: str = "index.html"
    log_dir: str = "logs"


def load_config() -> Config:
    if not os.getenv("THE_ODDS_API_KEY"):
        raise ValueError(
            "Missing required environment variable: THE_ODDS_API_KEY\n"
            "Copy .env.example to .env and fill in your API key."
        )

    # Parse ENABLED_LEAGUES — comma-separated keys, empty = all leagues
    enabled_keys_raw = os.getenv("ENABLED_LEAGUES", "")
    if enabled_keys_raw.strip():
        keys = {k.strip() for k in enabled_keys_raw.split(",")}
        enabled = [lg for lg in LEAGUES if lg.key in keys]
        unknown = keys - {lg.key for lg in LEAGUES}
        if unknown:
            raise ValueError(
                f"Unknown league key(s) in ENABLED_LEAGUES: {unknown}. "
                f"Valid keys: {[lg.key for lg in LEAGUES]}"
            )
        if not enabled:
            raise ValueError(
                f"ENABLED_LEAGUES='{enabled_keys_raw}' matched no known league keys. "
                f"Valid keys: {[lg.key for lg in LEAGUES]}"
            )
    else:
        enabled = list(LEAGUES)

    return Config(
        odds_api_key=os.environ["THE_ODDS_API_KEY"],
        enabled_leagues=enabled,
        ev_threshold=float(os.getenv("EV_THRESHOLD", "0.05")),
        rolling_window=int(os.getenv("ROLLING_WINDOW", "5")),
        poisson_max_goals=int(os.getenv("POISSON_MAX_GOALS", "8")),
        odds_totals_bookmakers=os.getenv("ODDS_TOTALS_BOOKMAKERS", ""),
        tennis_max_prob_ratio=float(os.getenv("TENNIS_MAX_PROB_RATIO", "1.5")),
        tennis_min_matches=int(os.getenv("TENNIS_MIN_MATCHES", "10")),
        nba_min_games=int(os.getenv("NBA_MIN_GAMES", "10")),
        nba_home_advantage=float(os.getenv("NBA_HOME_ADVANTAGE", "3.5")),
        nba_spread_std=float(os.getenv("NBA_SPREAD_STD", "15.5")),
        nba_total_std=float(os.getenv("NBA_TOTAL_STD", "19.0")),
    )
