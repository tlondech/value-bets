"""Sport-specific evaluator strategies.

Each concrete class encapsulates the evaluation logic for one sport and exposes
a uniform ``evaluate()`` method.  The ``EVALUATORS`` registry maps
``LeagueConfig.sport_type`` strings to the appropriate singleton.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Protocol, runtime_checkable

from config import LeagueConfig
from models.nba_model import evaluate_basketball_match
from models.tennis_model import evaluate_tennis_match
from pipeline.evaluate import enrich_with_news, evaluate_matches
from pipeline.helpers import is_live

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class SportEvaluator(Protocol):
    """Uniform interface for all sport evaluation strategies."""

    def evaluate(
        self,
        upcoming_events: list[dict],
        league: LeagueConfig,
        cfg,
        name_map: dict,
        **kwargs,
    ) -> list[dict]:
        """Returns a flat list of match signal dicts, each containing a ``signals`` key."""
        ...


# ---------------------------------------------------------------------------
# Tennis helpers
# ---------------------------------------------------------------------------

_SURFACE_KEYWORDS: dict[str, list[str]] = {
    "Clay": ["clay", "roland", "french", "monte", "madrid", "rome", "barcelona"],
    "Grass": ["grass", "wimbledon", "queens", "halle", "eastbourne", "hertogenbosch"],
}


def _infer_surface(display_name: str) -> str:
    lower = display_name.lower()
    for surface, keywords in _SURFACE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return surface
    return "Hard"


# ---------------------------------------------------------------------------
# NBA helpers
# ---------------------------------------------------------------------------

def _load_nba_crest_map(path: str) -> dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not k.startswith("_")}


def _resolve_nba_team(winamax_name: str, name_map: dict) -> str | None:
    return name_map.get("nba", {}).get(winamax_name)


# ---------------------------------------------------------------------------
# Concrete evaluators
# ---------------------------------------------------------------------------

class FootballEvaluator:
    """Poisson/Dixon-Coles evaluation for football leagues."""

    def evaluate(
        self,
        upcoming_events: list[dict],
        league: LeagueConfig,
        cfg,
        name_map: dict,
        *,
        raw_fixtures: list[dict] | None = None,
        stage_map: dict | None = None,
        crest_map: dict | None = None,
        features: dict | None = None,
        **_ignored,
    ) -> list[dict]:
        match_signals, n_skipped = evaluate_matches(
            upcoming_events, league, cfg, name_map,
            stage_map or {}, crest_map or {}, features or {},
        )

        signals = [m for m in match_signals.values() if m["signals"]]
        for m in signals:
            m["signals"].sort(key=lambda b: b["ev"], reverse=True)

        enrich_with_news(signals, cfg)

        if n_skipped:
            logger.warning(
                "[%s] %d match(es) skipped during evaluation.",
                league.display_name, n_skipped,
            )
        return signals


class TennisEvaluator:
    """Surface-adjusted Elo evaluation for tennis leagues."""

    def evaluate(
        self,
        upcoming_events: list[dict],
        league: LeagueConfig,
        cfg,
        name_map: dict,
        *,
        round_map: dict | None = None,
        **_ignored,
    ) -> list[dict]:
        is_wta = league.odds_sport.startswith("tennis_wta_")
        ratings = cfg.wta_elo if is_wta else cfg.atp_elo
        p = Path(cfg.tennis_crest_map_path)
        crest_map = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        if not ratings:
            logger.warning("[%s] No Elo ratings available — skipping.", league.display_name)
            return []

        surface = _infer_surface(league.display_name)
        logger.debug("[%s] Inferred surface: %s", league.display_name, surface)

        started = [e for e in upcoming_events if is_live(e["commence_time"])]
        upcoming_events = [e for e in upcoming_events if not is_live(e["commence_time"])]
        if started:
            logger.info(
                "[%s] Skipping %d tennis match(es) that have already started.",
                league.display_name, len(started),
            )

        signals = []
        for event in upcoming_events:
            player1 = event["home_team"]
            player2 = event["away_team"]
            raw_signals = evaluate_tennis_match(
                player1=player1,
                player2=player2,
                surface=surface,
                p1_odds=event["home_odds"],
                p2_odds=event["away_odds"],
                ratings=ratings,
                ev_threshold=cfg.ev_threshold,
                max_prob_ratio=cfg.tennis_max_prob_ratio,
                min_matches=cfg.tennis_min_matches,
            )
            if raw_signals:
                raw_signals = [max(raw_signals, key=lambda s: s["ev"])]
                stage = None
                if round_map:
                    stage = round_map.get(frozenset({player1.lower(), player2.lower()}))
                signals.append({
                    "league_key":     league.key,
                    "league_name":    league.display_name,
                    "home_team":      player1,
                    "away_team":      player2,
                    "home_canonical": player1,
                    "away_canonical": player2,
                    "home_crest":     crest_map.get(player1),
                    "away_crest":     crest_map.get(player2),
                    "surface":        surface,
                    "stage":          stage,
                    "kickoff":        event["commence_time"].isoformat(),
                    "sport":          "tennis",
                    "bookmaker_link": event.get("bookmaker_link"),
                    "signals":        sorted(raw_signals, key=lambda s: s["ev"], reverse=True),
                })
        return signals


class NBAEvaluator:
    """Gaussian efficiency model evaluation for NBA."""

    def evaluate(
        self,
        upcoming_events: list[dict],
        league: LeagueConfig,
        cfg,
        name_map: dict,
        *,
        stage_map: dict | None = None,
        **_ignored,
    ) -> list[dict]:
        ratings = cfg.nba_ratings
        crest_map = _load_nba_crest_map(cfg.nba_crest_map_path)
        if not ratings:
            logger.warning("[NBA] No team ratings available — skipping NBA evaluation.")
            return []

        started = [e for e in upcoming_events if is_live(e["commence_time"])]
        upcoming_events = [e for e in upcoming_events if not is_live(e["commence_time"])]
        if started:
            logger.info("[NBA] Skipping %d game(s) that have already started.", len(started))

        signals = []
        n_unmapped = 0
        for event in upcoming_events:
            home_winamax = event["home_team"]
            away_winamax = event["away_team"]

            home_abbr = _resolve_nba_team(home_winamax, name_map)
            away_abbr = _resolve_nba_team(away_winamax, name_map)
            if not home_abbr or not away_abbr:
                logger.debug(
                    "[NBA] Skipping %s vs %s — unmapped team name(s) "
                    "(check team_name_map.json[\"nba\"]).",
                    home_winamax, away_winamax,
                )
                n_unmapped += 1
                continue

            home_r = ratings.get(home_abbr)
            away_r = ratings.get(away_abbr)
            if not home_r or not away_r:
                logger.debug(
                    "[NBA] Skipping %s (%s) vs %s (%s) — no ratings data.",
                    home_winamax, home_abbr, away_winamax, away_abbr,
                )
                continue

            game_date = event["commence_time"].date()
            home_rest_days: int | None = None
            away_rest_days: int | None = None
            if last := home_r.get("last_game_date"):
                home_rest_days = (game_date - last).days
            if last := away_r.get("last_game_date"):
                away_rest_days = (game_date - last).days

            stage = None
            if stage_map:
                stage = stage_map.get(frozenset({home_winamax.lower(), away_winamax.lower()}))

            raw_signals = evaluate_basketball_match(
                home_team=home_winamax,
                away_team=away_winamax,
                home_ratings=home_r,
                away_ratings=away_r,
                home_odds=event.get("home_odds"),
                away_odds=event.get("away_odds"),
                over_odds=event.get("over_odds"),
                under_odds=event.get("under_odds"),
                totals_line=event.get("totals_line"),
                spread_home_point=event.get("spread_home_point"),
                spread_home_odds=event.get("spread_home_odds"),
                spread_away_odds=event.get("spread_away_odds"),
                ev_threshold=cfg.ev_threshold,
                max_prob_ratio=cfg.max_prob_ratio,
                min_games=cfg.nba_min_games,
                home_advantage=cfg.nba_home_advantage,
                spread_std=cfg.nba_spread_std,
                total_std=cfg.nba_total_std,
                home_rest_days=home_rest_days,
                away_rest_days=away_rest_days,
            )
            if not raw_signals:
                continue

            # Market-group filtering: keep only the highest-EV signal per group
            groups: dict[str, dict] = {}
            for b in raw_signals:
                grp = b.get("market_group", b["outcome"])
                if grp not in groups or b["ev"] > groups[grp]["ev"]:
                    groups[grp] = b
            filtered_signals = sorted(groups.values(), key=lambda b: b["ev"], reverse=True)
            for b in filtered_signals:
                b.pop("market_group", None)

            signals.append({
                "league_key":     league.key,
                "league_name":    league.display_name,
                "home_team":      home_winamax,
                "away_team":      away_winamax,
                "home_canonical": home_abbr,
                "away_canonical": away_abbr,
                "home_crest":     crest_map.get(home_abbr),
                "away_crest":     crest_map.get(away_abbr),
                "home_form":      home_r.get("form"),
                "away_form":      away_r.get("form"),
                "home_rest_days": home_rest_days,
                "away_rest_days": away_rest_days,
                "stage":          stage,
                "kickoff":        event["commence_time"].isoformat(),
                "sport":          "basketball",
                "bookmaker_link": event.get("bookmaker_link"),
                "signals":        filtered_signals,
            })

        if n_unmapped:
            logger.warning(
                "[NBA] %d event(s) skipped due to unmapped team names. "
                "Add entries to data/team_name_map.json[\"nba\"].",
                n_unmapped,
            )

        enrich_with_news(signals, cfg)
        return signals


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

EVALUATORS: dict[str, SportEvaluator] = {
    "football":   FootballEvaluator(),
    "tennis":     TennisEvaluator(),
    "basketball": NBAEvaluator(),
}
