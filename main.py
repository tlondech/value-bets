"""
Signal Arena — Main Orchestrator
Run: python main.py

Pipeline (per enabled league):
  1. Fetch upcoming Winamax odds (The Odds API)
  2. Fetch finished fixtures from ESPN (football only)
  3. Build Poisson features per match
  4. Calculate Expected Value → collect signals
  5. Merge all leagues, push signals to Supabase signal_history table
"""

import argparse
import json
import logging
import os
import time
import webbrowser
from datetime import date, datetime
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from rich.tree import Tree

from config import _current_nba_season, load_config
from constants import LOCAL_REPORT_URL
from db.supabase import (
    get_supabase_client,
    prune_stale_supabase_signals,
    push_signals_to_supabase,
)
from extractors.basketball_data_client import BasketballDataClient
from extractors.espn_tennis_client import ESPNTennisClient
from extractors.odds import fetch_active_tennis_leagues
from extractors.tennis_sackmann_client import TennisDataClient
from models.features import load_team_name_map
from models.nba_model import compute_nba_ratings
from models.tennis_model import build_player_country_map, compute_elo_ratings
from pipeline import run_league_pipeline, settle_all_sports

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

console = Console()

_FMT  = "%(asctime)s [%(levelname)s]  %(message)s"
_DATE = "%H:%M:%S"

_SPORT_LABEL = {
    "football":   "FOOTBALL",
    "tennis":     "TENNIS",
    "basketball": "NBA",
}

os.makedirs("logs", exist_ok=True)

_file_handler = logging.FileHandler("logs/run.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE))

_rich_handler = RichHandler(
    console=console,
    show_path=False,
    rich_tracebacks=True,
    log_time_format="[%H:%M:%S]",
)
_rich_handler.setFormatter(logging.Formatter("%(message)s"))

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _rich_handler])
for _noisy in ("httpx", "httpcore", "h2", "hpack"):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)
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

        # Fetch ESPN flag URLs — these override flagcdn.com entries (higher quality, same CDN)
        espn_flags: dict[str, str] = {}
        try:
            for r in ESPNTennisClient().fetch_recent_results(days_back=30):
                if r.metadata.get("home_flag"):
                    espn_flags[r.home_team] = r.metadata["home_flag"]
                if r.metadata.get("away_flag"):
                    espn_flags[r.away_team] = r.metadata["away_flag"]
        except Exception as e:
            logger.debug("ESPN tennis flag fetch failed (non-fatal): %s", e)

        if country_map or espn_flags:
            p = Path(cfg.tennis_crest_map_path)
            existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
            merged = {**existing, **country_map, **espn_flags}  # ESPN flags > flagcdn > stale
            p.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.debug("Tennis: %d flag URL(s) merged into %s (%d from ESPN)", len(merged), cfg.tennis_crest_map_path, len(espn_flags))
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
        games_df = BasketballDataClient().fetch_team_game_logs(nba_season)
        cfg.nba_ratings = compute_nba_ratings(games_df, rolling_window=rolling_window)
        logger.debug(
            "NBA: %d team(s) rated for season %s (rolling=%d)",
            len(cfg.nba_ratings), nba_season, rolling_window,
        )
    except Exception as e:
        logger.warning("NBA ratings computation failed — NBA will be skipped: %s", e)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _persist(supabase, all_signals: list[dict], processed_league_keys: set[str]) -> None:
    """Pushes signals to Supabase, pruning stale entries."""
    today = date.today().isoformat()
    prune_stale_supabase_signals(supabase, all_signals, processed_league_keys)
    push_signals_to_supabase(supabase, all_signals, today)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(force_fetch: bool = False, dry_run: bool = False) -> None:
    t0 = time.monotonic()
    cfg = load_config()
    name_map = load_team_name_map(cfg.team_map_path)

    # ── INIT ──────────────────────────────────────────────────────────────────
    logger.info("── INIT ──")
    _init_tennis(cfg)
    _init_nba(cfg)

    supabase = get_supabase_client()

    # Group leagues by sport type, preserving config order
    leagues_by_sport: dict[str, list] = {}
    for league in cfg.enabled_leagues:
        leagues_by_sport.setdefault(league.sport_type, []).append(league)

    all_signals: list[dict] = []
    all_raw_fixtures: list[dict] = []
    signals_by_sport: dict[str, int] = {}

    for sport_type, leagues in leagues_by_sport.items():
        label = _SPORT_LABEL.get(sport_type, sport_type.upper())
        logger.info("")
        logger.info("── %s  (%d) ──", label, len(leagues))

        sport_tree = Tree(f"[bold]{label}[/bold]  ({len(leagues)} league{'s' if len(leagues) != 1 else ''})")
        dry_run_rows: list[tuple[str, str, str]] = []
        sport_signals = 0

        for league in leagues:
            league_signals, raw_fixtures, n_matches, dry_events = run_league_pipeline(
                league, cfg, name_map, dry_run=dry_run,
            )
            all_signals.extend(league_signals)
            all_raw_fixtures.extend(raw_fixtures)
            n_sig = sum(len(m["signals"]) for m in league_signals)
            sport_signals += n_sig

            leaf = f"{league.display_name:<26}  [cyan][FETCH][/cyan] {n_matches:2d} matches"
            if not dry_run:
                leaf += f"  [green][EVALUATE][/green] {n_sig:2d} signals"
            sport_tree.add(leaf)

            for ev in dry_events:
                dry_run_rows.append((
                    str(ev["commence_time"].date()),
                    ev["home_team"],
                    ev["away_team"],
                ))

        console.print(sport_tree)

        if dry_run and dry_run_rows:
            tbl = Table(
                title=f"{label} — Upcoming Matches",
                show_header=True,
                header_style="bold",
            )
            tbl.add_column("Date", style="dim cyan")
            tbl.add_column("Home")
            tbl.add_column("Away")
            for date_str, home, away in dry_run_rows:
                tbl.add_row(date_str, home, away)
            console.print(tbl)

        signals_by_sport[sport_type] = sport_signals

    if dry_run:
        return

    # ── SETTLEMENT ────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("── SETTLEMENT ──")
    settle_all_sports(supabase, cfg, all_raw_fixtures, name_map, force_fetch)

    # ── PERSIST ───────────────────────────────────────────────────────────────
    logger.info("")
    logger.info("── PERSIST ──")
    all_signals.sort(key=lambda x: x["kickoff"])
    _persist(supabase, all_signals, {lg.key for lg in cfg.enabled_leagues})

    # ── DONE ──────────────────────────────────────────────────────────────────
    total_signals = sum(len(m["signals"]) for m in all_signals)
    parts = [
        f"{_SPORT_LABEL.get(s, s.upper())}: {n}"
        for s, n in signals_by_sport.items()
    ]
    logger.info("")
    logger.info(
        "── DONE ──  %s  →  %d total  (%.1f sec)",
        "  ·  ".join(parts), total_signals, time.monotonic() - t0,
    )

    webbrowser.open(LOCAL_REPORT_URL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Signal Arena")
    parser.add_argument("--fetch", action="store_true", help="Always fetch fresh data from external APIs (use in CI / scheduled runs)")
    parser.add_argument("--dry-run", action="store_true", help="Check Odds API coverage per league without writing to DB or running the model")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG-level logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("════════════════════════════════════════")
    logger.info("  Signal Arena  ·  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("════════════════════════════════════════")
    try:
        run_pipeline(force_fetch=args.fetch, dry_run=args.dry_run)
    except Exception as e:
        logger.exception("Unhandled error in pipeline: %s", e)
        raise


if __name__ == "__main__":
    main()
