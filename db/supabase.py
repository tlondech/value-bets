"""
Supabase client and remote persistence operations.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import cast

from supabase import create_client, Client

from models.features import resolve_team_name

logger = logging.getLogger(__name__)


def get_supabase_client() -> Client:
    """Creates a Supabase client from environment variables."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_ANON_KEY must be set in your .env file."
        )
    return create_client(url, key)


def settle_supabase_bets(supabase: Client, all_raw_fixtures: list[dict], name_map: dict | None = None) -> int:
    """
    Settles bets directly against Supabase — works in CI where no local SQLite exists.

    1. Fetches unsettled bets from Supabase whose kickoff is in the past.
    2. Matches them against raw_fixtures (already fetched from football-data.co.uk).
    3. Evaluates each outcome and upserts the result back to Supabase.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        resp = (
            supabase.table("bet_history")
            .select("kickoff,home_team,away_team,home_canonical,away_canonical,league_key,outcome")
            .eq("settled", False)
            .lt("kickoff", now_iso)
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled bets from Supabase: %s", exc)
        return 0

    unsettled = cast(list[dict], resp.data or [])
    if not unsettled:
        logger.debug("No unsettled past bets found in Supabase.")
        return 0

    # Build fixture index keyed by canonical names so it aligns with bet["home_canonical"].
    # Each fixture is tagged with league_key by run_league_pipeline; resolve_team_name maps
    # the raw CSV/API name to its canonical form. Falls back to raw name if unresolvable.
    fixture_index: dict[tuple, dict] = {}
    for f in all_raw_fixtures:
        lk = f.get("league_key", "")
        if name_map and lk:
            home_c = resolve_team_name(f["home_team"], name_map, lk) or f["home_team"]
            away_c = resolve_team_name(f["away_team"], name_map, lk) or f["away_team"]
        else:
            home_c, away_c = f["home_team"], f["away_team"]
        fixture_index[(home_c, away_c)] = f

    rows_to_update = []
    settled_at = datetime.now(timezone.utc).isoformat()
    for bet in unsettled:
        # Use stored canonical; fall back to resolving home_team when canonical is null
        home_key = bet.get("home_canonical")
        away_key = bet.get("away_canonical")
        if not home_key or not away_key:
            lk = bet.get("league_key", "")
            if name_map and lk:
                home_key = resolve_team_name(bet.get("home_team", ""), name_map, lk) or bet.get("home_team")
                away_key = resolve_team_name(bet.get("away_team", ""), name_map, lk) or bet.get("away_team")
            else:
                home_key, away_key = bet.get("home_team"), bet.get("away_team")
        fixture = fixture_index.get((home_key, away_key))
        if fixture is None:
            continue

        # Date guard: fixture must be within ±1 day of kickoff
        kickoff_dt = datetime.fromisoformat(bet["kickoff"].replace("Z", "+00:00"))
        fixture_dt = fixture["fixture_date"]
        if not isinstance(fixture_dt, datetime):
            fixture_dt = datetime.fromisoformat(str(fixture_dt))
        if fixture_dt.tzinfo is None:
            fixture_dt = fixture_dt.replace(tzinfo=timezone.utc)
        if abs((fixture_dt - kickoff_dt).total_seconds()) > 86400:
            continue

        hg, ag = fixture["home_goals"], fixture["away_goals"]
        won = {
            "home_win":  hg > ag,
            "draw":      hg == ag,
            "away_win":  ag > hg,
            "over_2_5":  hg + ag > 2,
            "under_2_5": hg + ag <= 2,
        }.get(bet["outcome"], False)

        rows_to_update.append({
            "kickoff":           bet["kickoff"],
            "home_team":         bet["home_team"],
            "away_team":         bet["away_team"],
            "outcome":           bet["outcome"],
            "settled":           True,
            "result":            "won" if won else "lost",
            "actual_home_goals": hg,
            "actual_away_goals": ag,
            "settled_at":        settled_at,
        })

    if not rows_to_update:
        logger.debug("No fixture matches found for unsettled bets.")
        return 0

    count = 0
    for row in rows_to_update:
        try:
            supabase.table("bet_history").update({
                "settled":           row["settled"],
                "result":            row["result"],
                "actual_home_goals": row["actual_home_goals"],
                "actual_away_goals": row["actual_away_goals"],
                "settled_at":        row["settled_at"],
            }).eq("kickoff", row["kickoff"]).eq("home_team", row["home_team"]).eq("away_team", row["away_team"]).eq("outcome", row["outcome"]).execute()
            count += 1
        except Exception as exc:
            logger.error(
                "Failed to settle bet %s vs %s (%s) @ %s: %s",
                row["home_team"], row["away_team"], row["outcome"], row["kickoff"], exc,
            )
    if count:
        logger.info("Settled %d bet(s) via Supabase.", count)
    return count


def prune_stale_supabase_bets(
    supabase: Client,
    all_value_bets: list[dict],
    processed_league_keys: set[str],
) -> int:
    """
    Deletes unsettled future bets from Supabase for processed leagues whose
    outcome is no longer in the current recommended set.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        resp = (
            supabase.table("bet_history")
            .select("id,kickoff,home_team,away_team,outcome,league_key")
            .eq("settled", False)
            .gt("kickoff", now_iso)
            .in_("league_key", list(processed_league_keys))
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled bets for pruning: %s", exc)
        return 0

    existing: list[dict] = cast(list[dict], resp.data or [])
    if not existing:
        return 0

    def _utc_prefix(iso: str) -> str:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    current_keys: set[tuple] = set()
    for m in all_value_bets:
        kickoff_utc = _utc_prefix(m["kickoff"])
        for b in m["bets"]:
            current_keys.add((kickoff_utc, m["home_team"], m["away_team"], b["outcome"]))

    stale_ids = [
        row["id"]
        for row in existing
        if (_utc_prefix(row["kickoff"]), row["home_team"], row["away_team"], row["outcome"])
        not in current_keys
    ]

    if not stale_ids:
        return 0

    try:
        supabase.table("bet_history").delete().in_("id", stale_ids).execute()
        logger.info("Pruned %d stale bet(s) from Supabase.", len(stale_ids))
    except Exception as exc:
        logger.error("Failed to prune stale bets from Supabase: %s", exc)
        return 0

    return len(stale_ids)


def push_bets_to_supabase(
    supabase: Client,
    value_bets: list[dict],
    recorded_date: str,
) -> int:
    """
    Upserts today's value bets into the Supabase `bet_history` table.
    Uses the unique constraint (kickoff, home_team, away_team, outcome)
    to skip duplicates. Returns the number of rows upserted.
    """
    rows = []
    for m in value_bets:
        for b in m["bets"]:
            rows.append({
                "recorded_date":  recorded_date,
                "league_key":     m["league_key"],
                "league_name":    m["league_name"],
                "home_team":      m["home_team"],
                "away_team":      m["away_team"],
                "home_canonical": m.get("home_canonical"),
                "away_canonical": m.get("away_canonical"),
                "kickoff":        m["kickoff"],  # ISO 8601 → Supabase parses as TIMESTAMPTZ
                "stage":          m.get("stage"),
                "outcome":        b["outcome"],
                "outcome_label":  b["outcome_label"],
                "odds":           b["odds"],
                "true_prob":      b["true_prob"],
                "ev":             b["ev"],
                "home_rank":      m.get("home_rank"),
                "away_rank":      m.get("away_rank"),
                "home_form":      m.get("home_form"),
                "away_form":      m.get("away_form"),
                "home_crest":     m.get("home_crest"),
                "away_crest":     m.get("away_crest"),
                "home_rest_days": m.get("home_rest_days"),
                "away_rest_days": m.get("away_rest_days"),
                "h2h_used":       m.get("h2h_used"),
                "is_second_leg":  m.get("is_second_leg"),
                "agg_home":       m.get("agg_home"),
                "agg_away":       m.get("agg_away"),
                "leg1_result":    m.get("leg1_result"),
            })

    if not rows:
        logger.info("No value bets to push to Supabase.")
        return 0

    try:
        response = (
            supabase.table("bet_history")
            .upsert(rows, on_conflict="kickoff,home_team,away_team,outcome")
            .execute()
        )
        count = len(response.data) if response.data else len(rows)
        logger.info("Pushed %d bet row(s) to Supabase.", count)
        return count
    except Exception as exc:
        logger.error("Failed to push bets to Supabase: %s", exc)
        raise
