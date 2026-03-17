"""
Betting Recommendation Engine — Main Orchestrator
Run: python main.py

Pipeline (per enabled league):
  1. Load config + init DB
  2. Fetch upcoming Winamax odds (The Odds API)
  3. Upsert matches + odds into SQLite
  4. Fetch finished fixtures + xG (football-data.co.uk CSV)
  5. Upsert fixtures into SQLite
  6. Build Poisson features per match
  7. Calculate Expected Value → collect value bets
  8. Merge all leagues, push value bets to Supabase bet_history table
"""

import argparse
import json
import logging
import os
import time
import webbrowser
from datetime import date, datetime
from pathlib import Path

from sqlalchemy.orm import Session

from config import LEAGUES as _ALL_LEAGUES
from config import _current_nba_season, load_config
from constants import LOCAL_REPORT_URL
from db.queries import prune_stale_bets, save_bets_to_history
from db.schema import init_db
from db.supabase import (
    get_supabase_client,
    prune_stale_supabase_bets,
    push_bets_to_supabase,
    settle_nba_supabase_bets,
    settle_supabase_bets,
    settle_tennis_supabase_bets,
)
from extractors.nba_data_client import NBADataClient
from extractors.odds import fetch_active_tennis_leagues
from extractors.tennis_data_client import TennisDataClient
from models.features import load_team_name_map
from models.nba_model import compute_nba_ratings
from models.tennis_model import build_player_country_map, compute_elo_ratings
from pipeline import _fetch_org_settlement_fixtures, _merge_settlement_fixtures, run_league_pipeline

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

class _ColoredFormatter(logging.Formatter):
    _COLORS = {
        logging.DEBUG:    "\033[90m",   # dim gray
        logging.INFO:     "\033[0m",    # default
        logging.WARNING:  "\033[93m",   # bright yellow
        logging.ERROR:    "\033[91m",   # bright red
        logging.CRITICAL: "\033[95m",   # magenta
    }
    _RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self._COLORS.get(record.levelno, "")
        return f"{color}{super().format(record)}{self._RESET}"


_FMT  = "%(asctime)s [%(levelname)s]  %(message)s"
_DATE = "%H:%M:%S"

os.makedirs("logs", exist_ok=True)

_file_handler = logging.FileHandler("logs/run.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE))

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_ColoredFormatter(_FMT, datefmt=_DATE))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pre-computation helpers (one per sport type)
# ---------------------------------------------------------------------------

def _init_tennis(cfg) -> None:
    """Discovers active ATP/WTA tournaments, computes Elo ratings, persists flag URLs."""
    tennis_leagues = fetch_active_tennis_leagues(cfg.odds_api_key)
    if not tennis_leagues:
        return

    cfg.enabled_leagues.extend(tennis_leagues)
    logger.debug("Tennis: %d active tournament(s) discovered", len(tennis_leagues))

    try:
        current_year = datetime.now().year
        years = list(range(current_year - 4, current_year + 1))
        client = TennisDataClient()
        atp_matches = client.fetch_atp_matches(years)
        wta_matches = client.fetch_wta_matches(years)
        cfg.atp_elo = compute_elo_ratings(atp_matches)
        cfg.wta_elo = compute_elo_ratings(wta_matches)
        logger.debug(
            "Tennis Elo: %d ATP players, %d WTA players rated",
            len(cfg.atp_elo), len(cfg.wta_elo),
        )
        country_map = {
            **build_player_country_map(atp_matches),
            **build_player_country_map(wta_matches),
        }
        if country_map:
            p = Path(cfg.tennis_crest_map_path)
            existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
            merged = {**country_map, **existing}  # existing entries win (allow manual overrides)
            p.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.debug("Tennis: %d player flag URL(s) merged into %s", len(country_map), cfg.tennis_crest_map_path)
    except Exception as e:
        logger.warning("Tennis Elo computation failed — tennis leagues will be skipped: %s", e)


def _init_nba(cfg) -> None:
    """Fetches NBA game logs for the current season and computes team efficiency ratings."""
    nba_leagues = [lg for lg in cfg.enabled_leagues if lg.sport_type == "basketball"]
    if not nba_leagues:
        return

    if 7 <= datetime.now().month <= 9:
        logger.info("NBA: off-season (July–September) — skipping ratings computation.")
        return

    try:
        nba_season = _current_nba_season()
        rolling_window = cfg.rolling_window * 2  # more games needed for basketball
        games_df = NBADataClient().fetch_team_game_logs(nba_season)
        cfg.nba_ratings = compute_nba_ratings(games_df, rolling_window=rolling_window)
        logger.debug(
            "NBA: %d team(s) rated for season %s (rolling=%d)",
            len(cfg.nba_ratings), nba_season, rolling_window,
        )
    except Exception as e:
        logger.warning("NBA ratings computation failed — NBA will be skipped: %s", e)


# ---------------------------------------------------------------------------
# Logging summary
# ---------------------------------------------------------------------------

def _log_enabled_leagues(cfg) -> None:
    football_leagues    = [lg for lg in cfg.enabled_leagues if lg.sport_type == "football"]
    tennis_tournaments  = [lg for lg in cfg.enabled_leagues if lg.sport_type == "tennis"]
    basketball_leagues  = [lg for lg in cfg.enabled_leagues if lg.sport_type == "basketball"]
    all_football        = [lg for lg in _ALL_LEAGUES if lg.sport_type == "football"]
    n_skipped           = len(all_football) - len(football_leagues)
    suffix = f"  (+ {n_skipped} skipped)" if n_skipped else ""
    logger.info("Football: %s%s", ", ".join(lg.display_name for lg in football_leagues), suffix)
    if tennis_tournaments:
        logger.info("Tennis: %s", ", ".join(lg.display_name for lg in tennis_tournaments))
    if basketball_leagues:
        logger.info("Basketball: %s", ", ".join(lg.display_name for lg in basketball_leagues))


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

def _settle_all(supabase, cfg, all_raw_fixtures: list[dict], name_map: dict, force_fetch: bool) -> None:
    """Settles past bets across all sport types."""
    org_settle = _fetch_org_settlement_fixtures(cfg.enabled_leagues, cfg, name_map) if force_fetch else []
    settlement_fixtures = _merge_settlement_fixtures(all_raw_fixtures, org_settle, name_map)
    settle_supabase_bets(supabase, settlement_fixtures, name_map)

    tennis_keys = [lg.key for lg in cfg.enabled_leagues if lg.sport_type == "tennis"]
    if tennis_keys:
        settle_tennis_supabase_bets(supabase, tennis_keys)

    nba_keys = [lg.key for lg in cfg.enabled_leagues if lg.sport_type == "basketball"]
    if nba_keys:
        settle_nba_supabase_bets(supabase, nba_keys, name_map)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _persist(supabase, engine, all_value_bets: list[dict], processed_league_keys: set[str]) -> None:
    """Saves value bets to local SQLite and Supabase, pruning stale entries."""
    today = date.today().isoformat()

    with Session(engine) as session:
        n_pruned = prune_stale_bets(session, all_value_bets, processed_league_keys)
        n_new = save_bets_to_history(session, all_value_bets, today)
        session.commit()
    if n_pruned:
        logger.info("Pruned %d stale bet record(s) from local DB.", n_pruned)
    if n_new:
        logger.info("Saved %d new bet record(s) to local DB.", n_new)

    prune_stale_supabase_bets(supabase, all_value_bets, processed_league_keys)
    push_bets_to_supabase(supabase, all_value_bets, today)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(force_fetch: bool = False, dry_run: bool = False) -> None:
    t0 = time.monotonic()
    cfg = load_config()
    engine = init_db(cfg.db_path)
    name_map = load_team_name_map(cfg.team_map_path)

    _init_tennis(cfg)
    _init_nba(cfg)
    _log_enabled_leagues(cfg)

    supabase = get_supabase_client()

    all_value_bets: list[dict] = []
    all_raw_fixtures: list[dict] = []
    for league in cfg.enabled_leagues:
        league_bets, raw_fixtures = run_league_pipeline(
            league, cfg, engine, name_map, force_fetch=force_fetch, dry_run=dry_run,
        )
        all_value_bets.extend(league_bets)
        all_raw_fixtures.extend(raw_fixtures)

    if dry_run:
        return

    all_value_bets.sort(key=lambda x: x["kickoff"])
    total_bets = sum(len(m["bets"]) for m in all_value_bets)
    logger.info(
        "Total: %d value bets across %d matches  (%.1f sec)",
        total_bets, len(all_value_bets), time.monotonic() - t0,
    )

    _settle_all(supabase, cfg, all_raw_fixtures, name_map, force_fetch)
    _persist(supabase, engine, all_value_bets, {lg.key for lg in cfg.enabled_leagues})

    webbrowser.open(LOCAL_REPORT_URL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Betting Recommendation Engine")
    parser.add_argument("--fetch", action="store_true", help="Always fetch fresh data from external APIs (use in CI / scheduled runs)")
    parser.add_argument("--dry-run", action="store_true", help="Check Odds API coverage per league without writing to DB or running the model")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG-level logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("══ Betting Engine ══  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        run_pipeline(force_fetch=args.fetch, dry_run=args.dry_run)
    except Exception as e:
        logger.exception("Unhandled error in pipeline: %s", e)
        raise


if __name__ == "__main__":
    main()
