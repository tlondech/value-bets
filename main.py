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
import logging
import os
import time
import webbrowser
from datetime import date, datetime

from sqlalchemy.orm import Session

from config import load_config
from constants import LOCAL_REPORT_URL
from db.schema import init_db
from db.queries import prune_stale_bets, save_bets_to_history
from db.supabase import get_supabase_client, prune_stale_supabase_bets, push_bets_to_supabase, settle_supabase_bets
from extractors.odds import fetch_active_tennis_leagues
from extractors.tennis_data_client import TennisDataClient
from models.features import load_team_name_map
from models.tennis_model import compute_elo_ratings
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
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(force_fetch: bool = False, dry_run: bool = False) -> None:
    t0 = time.monotonic()
    cfg = load_config()
    engine = init_db(cfg.db_path)
    name_map = load_team_name_map(cfg.team_map_path)

    # Discover active tennis tournaments and pre-compute Elo ratings
    tennis_leagues = fetch_active_tennis_leagues(cfg.odds_api_key)
    if tennis_leagues:
        cfg.enabled_leagues.extend(tennis_leagues)
        logger.debug("Tennis: %d active tournament(s) discovered", len(tennis_leagues))
        try:
            current_year = datetime.now().year
            years = list(range(current_year - 4, current_year + 1))
            client = TennisDataClient()
            cfg.atp_elo = compute_elo_ratings(client.fetch_atp_matches(years))
            cfg.wta_elo = compute_elo_ratings(client.fetch_wta_matches(years))
            logger.debug(
                "Tennis Elo: %d ATP players, %d WTA players rated",
                len(cfg.atp_elo), len(cfg.wta_elo),
            )
        except Exception as e:
            logger.warning("Tennis Elo computation failed — tennis leagues will be skipped: %s", e)

    from config import LEAGUES as _ALL_LEAGUES
    football_leagues = [lg for lg in cfg.enabled_leagues if lg.sport_type == "football"]
    tennis_tournaments = [lg for lg in cfg.enabled_leagues if lg.sport_type == "tennis"]
    n_skipped_leagues = len(_ALL_LEAGUES) - len(football_leagues)
    suffix = f"  (+ {n_skipped_leagues} skipped)" if n_skipped_leagues else ""
    logger.info("Leagues: %s%s", ", ".join(lg.display_name for lg in football_leagues), suffix)
    if tennis_tournaments:
        logger.info("Tournaments: %s", ", ".join(lg.display_name for lg in tennis_tournaments))

    supabase = get_supabase_client()

    all_value_bets: list[dict] = []
    all_raw_fixtures: list[dict] = []
    for league in cfg.enabled_leagues:
        league_bets, raw_fixtures = run_league_pipeline(league, cfg, engine, name_map, force_fetch=force_fetch, dry_run=dry_run)
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

    # Settle past bets against Supabase (works in CI — no local SQLite needed)
    # Supplement .co.uk fixtures with near-real-time .org results for faster settlement.
    org_settle = _fetch_org_settlement_fixtures(cfg.enabled_leagues, cfg, name_map) if force_fetch else []
    settlement_fixtures = _merge_settlement_fixtures(all_raw_fixtures, org_settle, name_map)
    settle_supabase_bets(supabase, settlement_fixtures, name_map)

    processed_league_keys = {lg.key for lg in cfg.enabled_leagues}

    # Persist today's recommendations to local SQLite (used by settle_bets)
    with Session(engine) as session:
        n_pruned = prune_stale_bets(session, all_value_bets, processed_league_keys)
        n_new = save_bets_to_history(session, all_value_bets, date.today().isoformat())
        session.commit()
    if n_pruned:
        logger.info("Pruned %d stale bet record(s) from local DB.", n_pruned)
    if n_new:
        logger.info("Saved %d new bet record(s) to local DB.", n_new)

    # Push today's value bets to Supabase
    prune_stale_supabase_bets(supabase, all_value_bets, processed_league_keys)
    push_bets_to_supabase(supabase, all_value_bets, date.today().isoformat())

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
