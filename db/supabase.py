"""
Supabase client and remote persistence operations.
"""

import logging
import os
from datetime import datetime, timezone
from typing import cast

from supabase import create_client, Client

from constants import FIXTURE_DATE_TOLERANCE_SECONDS
from extractors.tennisdatauk_client import fetch_tennis_results
from models.features import resolve_team_name

logger = logging.getLogger(__name__)


def _settle_totals(outcome: str, hg: int, ag: int) -> bool | None:
    """Parse a dynamic totals outcome string and determine if it won.

    When the line is a whole number the actual bet placed is on the nearest
    available half-point: over_234_0 → over_233_5, under_234_0 → under_234_5.
    The adjusted line is then settled with the standard half-point logic.

    Examples:
        "over_2_5"    → line=2.5  → won if hg+ag > 2
        "under_3_5"   → line=3.5  → won if hg+ag <= 3
        "over_234_0"  → line=233.5 → won if hg+ag > 233  (i.e. >= 234)
        "under_234_0" → line=234.5 → won if hg+ag <= 234
    Returns None if the outcome string is not a totals bet.
    """
    if not outcome.startswith(("over_", "under_")):
        return None
    prefix, line_str = outcome.split("_", 1)
    parts = line_str.split("_")
    line = float(f"{parts[0]}.{''.join(parts[1:])}") if len(parts) > 1 else float(parts[0])
    if line == int(line):
        line = (line - 0.5) if prefix == "over" else (line + 0.5)
    threshold = int(line)
    return (hg + ag > threshold) if prefix == "over" else (hg + ag <= threshold)


# ---------------------------------------------------------------------------
# Pure stateless helpers
# ---------------------------------------------------------------------------

def _utc_prefix(iso: str) -> str:
    """Normalises an ISO-8601 timestamp to a UTC second-precision string for keying."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _last_name(name: str) -> str:
    """Last word of a player name, lowercased — used as a loose match key."""
    return name.strip().split()[-1].lower() if name.strip() else ""


# ---------------------------------------------------------------------------
# Supabase-coupled private helpers
# ---------------------------------------------------------------------------

_SETTLE_KEYS = frozenset({
    "settled", "result", "settled_at", "actual_home_goals", "actual_away_goals"
})


def _write_settled_bets(supabase: Client, rows: list[dict], sport: str) -> int:
    """
    Persists evaluated settlement rows to bet_history.

    Includes only keys present in the row (football rows carry actual_home_goals /
    actual_away_goals; tennis rows do not — the key-filter handles both without branching).
    Returns the number of rows successfully written.
    """
    count = 0
    for row in rows:
        payload = {k: row[k] for k in _SETTLE_KEYS if k in row}
        try:
            (
                supabase.table("bet_history")
                .update(payload)
                .eq("kickoff",   row["kickoff"])
                .eq("home_team", row["home_team"])
                .eq("away_team", row["away_team"])
                .eq("outcome",   row["outcome"])
                .execute()
            )
            count += 1
        except Exception as exc:
            logger.error(
                "Failed to settle %s bet %s vs %s (%s) @ %s: %s",
                sport, row["home_team"], row["away_team"],
                row["outcome"], row["kickoff"], exc,
            )
    if count:
        logger.info("Settled %d %s bet(s).", count, sport)
    return count



# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

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
            .eq("sport", "football")
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
        if abs((fixture_dt - kickoff_dt).total_seconds()) > FIXTURE_DATE_TOLERANCE_SECONDS:
            continue

        hg, ag = fixture["home_goals"], fixture["away_goals"]
        outcome = bet["outcome"]
        won = {"home_win": hg > ag, "draw": hg == ag, "away_win": ag > hg}.get(outcome)
        if won is None:
            won = _settle_totals(outcome, hg, ag) or False

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

    return _write_settled_bets(supabase, rows_to_update, "football via Supabase")


def settle_tennis_supabase_bets(supabase: Client, active_tennis_league_keys: list[str]) -> int:
    """
    Settles unsettled tennis bets in Supabase using tennis-data.co.uk CSV results.

    For each active tennis league, fetches completed match results and matches them
    against unsettled bets by player name and date proximity.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    year    = datetime.now(timezone.utc).year

    try:
        resp = (
            supabase.table("bet_history")
            .select("id,kickoff,home_team,away_team,outcome,league_key")
            .eq("settled", False)
            .eq("sport", "tennis")
            .lt("kickoff", now_iso)
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled tennis bets: %s", exc)
        return 0

    unsettled = cast(list[dict], resp.data or [])
    if not unsettled:
        logger.debug("No unsettled past tennis bets found.")
        return 0

    # Build results index per league key: {(winner_last, loser_last, date_str): full_result}
    results_by_league: dict[str, list[dict]] = {}
    for lk in active_tennis_league_keys:
        results_by_league[lk] = fetch_tennis_results(lk, year)

    rows_to_update = []
    settled_at = datetime.now(timezone.utc).isoformat()

    for bet in unsettled:
        lk      = bet.get("league_key", "")
        results = results_by_league.get(lk, [])
        if not results:
            continue

        kickoff_dt = datetime.fromisoformat(bet["kickoff"].replace("Z", "+00:00"))
        home = bet["home_team"]
        away = bet["away_team"]

        matched = None
        for r in results:
            # Date guard: within ±2 days (tennis matches sometimes span midnight)
            if abs((r["match_date"].replace(tzinfo=timezone.utc) - kickoff_dt).total_seconds()) > 2 * 86400:
                continue
            # Match by last name (handles minor name format differences)
            players = {_last_name(r["winner"]), _last_name(r["loser"])}
            if _last_name(home) in players and _last_name(away) in players:
                matched = r
                break

        if matched is None:
            continue

        won = (
            (bet["outcome"] == "home_win" and _last_name(matched["winner"]) == _last_name(home)) or
            (bet["outcome"] == "away_win" and _last_name(matched["winner"]) == _last_name(away))
        )
        rows_to_update.append({
            "id":         bet["id"],
            "home_team":  home,
            "away_team":  away,
            "outcome":    bet["outcome"],
            "kickoff":    bet["kickoff"],
            "settled":    True,
            "result":     "won" if won else "lost",
            "settled_at": settled_at,
        })

    if not rows_to_update:
        return 0

    return _write_settled_bets(supabase, rows_to_update, "tennis via tennis-data.co.uk")


def settle_nba_supabase_bets(
    supabase: Client,
    nba_league_keys: list[str],
    name_map: dict[str, dict[str, str]] | None = None,
) -> int:
    """
    Settles unsettled NBA bets in Supabase using nba_api game results.

    Uses team_name_map.json["nba"] to normalise both sides to abbreviations
    (e.g. "LA Clippers" and "Los Angeles Clippers" both → "LAC"), so matching
    is robust to display-name differences between the odds source and nba_api.

    Only attempts settlement for games that started more than NBA_LIVE_MATCH_WINDOW_HOURS
    ago to ensure the game has actually finished (overtime, TV timeouts, etc.).
    """
    from datetime import timedelta
    from constants import NBA_LIVE_MATCH_WINDOW_HOURS
    from extractors.nba_data_client import NBADataClient

    now = datetime.now(timezone.utc)
    # Only consider bets for games that have had enough time to finish
    cutoff_iso = (now - timedelta(hours=NBA_LIVE_MATCH_WINDOW_HOURS)).isoformat()

    try:
        resp = (
            supabase.table("bet_history")
            .select("id,kickoff,home_team,away_team,outcome,league_key")
            .eq("settled", False)
            .eq("sport", "basketball")
            .in_("league_key", nba_league_keys)
            .lt("kickoff", cutoff_iso)
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled NBA bets: %s", exc)
        return 0

    unsettled = cast(list[dict], resp.data or [])
    if not unsettled:
        logger.debug("No unsettled past NBA bets found.")
        return 0

    try:
        results = NBADataClient().fetch_recent_results(days_back=7)
    except Exception as exc:
        logger.warning("NBA results fetch failed — skipping settlement: %s", exc)
        return 0

    if not results:
        logger.debug("No recent NBA results found for settlement.")
        return 0

    # Build abbreviation lookup from team_name_map["nba"] so that both the
    # odds-source name ("Los Angeles Clippers") and the nba_api name ("LA Clippers")
    # resolve to the same abbreviation ("LAC") for matching.
    nba_abbr: dict[str, str] = {}
    if name_map:
        nba_abbr = {k.lower(): v for k, v in name_map.get("nba", {}).items()
                    if not k.startswith("_")}

    def _to_abbr(name: str) -> str:
        return nba_abbr.get(name.lower(), name.lower())

    # Index results by (home_abbr, away_abbr, game_date)
    results_index: dict[tuple, dict] = {}
    for r in results:
        key = (_to_abbr(r["home_team"]), _to_abbr(r["away_team"]), r["game_date"])
        results_index[key] = r

    rows_to_update = []
    settled_at = now.isoformat()

    for bet in unsettled:
        kickoff_dt = datetime.fromisoformat(bet["kickoff"].replace("Z", "+00:00"))
        home_abbr = _to_abbr(bet["home_team"])
        away_abbr = _to_abbr(bet["away_team"])

        # Try to match within ±1 day of kickoff
        matched = None
        for delta_days in (0, 1, -1):
            candidate_date = (kickoff_dt + timedelta(days=delta_days)).date()
            result = results_index.get((home_abbr, away_abbr, candidate_date))
            if result:
                matched = result
                break

        if matched is None:
            continue

        home_pts = matched["home_pts"]
        away_pts = matched["away_pts"]
        outcome  = bet["outcome"]

        # Moneyline
        if outcome == "home_win":
            won = home_pts > away_pts
        elif outcome == "away_win":
            won = away_pts > home_pts
        # Totals (e.g. "over_220_5" or "under_220_5")
        elif outcome.startswith(("over_", "under_")):
            won = _settle_totals(outcome, home_pts, away_pts)
            won = won if won is not None else False
        # Spreads (e.g. "spread_home_m5_5" or "spread_away_p5_5")
        elif outcome.startswith("spread_home_") or outcome.startswith("spread_away_"):
            won = _settle_nba_spread(outcome, home_pts, away_pts)
        else:
            logger.debug("[NBA] Unknown outcome key '%s' — skipping.", outcome)
            continue

        rows_to_update.append({
            "id":        bet["id"],
            "home_team": bet["home_team"],
            "away_team": bet["away_team"],
            "outcome":   outcome,
            "kickoff":   bet["kickoff"],
            "settled":   True,
            "result":    "won" if won else "lost",
            "actual_home_goals": home_pts,
            "actual_away_goals": away_pts,
            "settled_at": settled_at,
        })

    if not rows_to_update:
        return 0

    return _write_settled_bets(supabase, rows_to_update, "NBA")


def _settle_nba_spread(outcome: str, home_pts: int, away_pts: int) -> bool:
    """
    Determines if a spread/handicap bet won.

    Outcome encoding:
        "spread_home_m5_5"  → home covers -5.5  → home must win by > 5.5
        "spread_home_p3_5"  → home covers +3.5  → home wins or loses by < 3.5
        "spread_away_p5_5"  → away covers +5.5  → away wins or loses by < 5.5
        "spread_away_m3_5"  → away covers -3.5  → away wins by > 3.5
    """
    diff = home_pts - away_pts  # positive = home wins

    if outcome.startswith("spread_home_"):
        line_str = outcome[len("spread_home_"):]
        threshold = _decode_spread_line(line_str)
        # Home covers if actual spread > threshold (e.g. threshold=5.5 means home wins by 5.5+)
        return diff > threshold

    if outcome.startswith("spread_away_"):
        line_str = outcome[len("spread_away_"):]
        threshold = _decode_spread_line(line_str)
        # Away covers if away_pts - home_pts > threshold
        return (-diff) > threshold

    return False


def _decode_spread_line(encoded: str) -> float:
    """Decodes an encoded spread line string back to a float.

    Examples: "m5_5" → 5.5,  "p3_5" → 3.5,  "m10_0" → 10.0
    The prefix 'm' means the original line was negative (home favoured).
    """
    if encoded.startswith("m"):
        encoded = encoded[1:]
    elif encoded.startswith("p"):
        encoded = encoded[1:]
    parts = encoded.split("_")
    return float(f"{parts[0]}.{''.join(parts[1:])}") if len(parts) > 1 else float(parts[0])


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
        return len(stale_ids)
    except Exception as exc:
        logger.error("Failed to prune stale bets from Supabase: %s", exc)
        return 0


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
                "surface":        m.get("surface"),
                "home_rest_days": m.get("home_rest_days"),
                "away_rest_days": m.get("away_rest_days"),
                "h2h_used":       m.get("h2h_used"),
                "is_second_leg":  m.get("is_second_leg"),
                "agg_home":       m.get("agg_home"),
                "agg_away":       m.get("agg_away"),
                "leg1_result":    m.get("leg1_result"),
                "team_news":      m.get("team_news"),
                "sport":          m.get("sport", "football"),
                "bookmaker_link": m.get("bookmaker_link"),
            })

    if not rows:
        logger.info("No value bets to push to Supabase.")
        return 0

    # Before upserting, delete any unsettled rows for the same match that may
    # have a stale kickoff (e.g. match was rescheduled since last run).
    # Uses 1 SELECT + at most 1 DELETE instead of one DELETE per row.
    # Also used below to distinguish created vs updated rows in the log.
    existing_keys: set[tuple] = set()
    try:
        league_keys = list({r["league_key"] for r in rows})
        existing = (
            supabase.table("bet_history")
            .select("id,home_team,away_team,league_key,outcome,kickoff")
            .in_("league_key", league_keys)
            .eq("settled", False)
            .execute()
        )
        current_kickoffs: dict[tuple, str] = {
            (r["home_team"], r["away_team"], r["league_key"], r["outcome"]): r["kickoff"]
            for r in rows
        }
        existing_rows: list[dict] = cast(list[dict], existing.data or [])
        stale_ids = [
            row["id"]
            for row in existing_rows
            if (row["home_team"], row["away_team"], row["league_key"], row["outcome"]) in current_kickoffs
            and row["kickoff"] != current_kickoffs[(row["home_team"], row["away_team"], row["league_key"], row["outcome"])]
        ]
        if stale_ids:
            supabase.table("bet_history").delete().in_("id", stale_ids).execute()
            logger.info("Deleted %d stale bet row(s) before upsert.", len(stale_ids))
        stale_id_set = set(stale_ids)
        existing_keys = {
            (row["home_team"], row["away_team"], row["league_key"], row["outcome"], row["kickoff"])
            for row in existing_rows
            if row["id"] not in stale_id_set
        }
    except Exception as exc:
        logger.warning("Failed to clean up stale bets before upsert: %s", exc)

    try:
        response = (
            supabase.table("bet_history")
            .upsert(rows, on_conflict="kickoff,home_team,away_team,outcome")
            .execute()
        )
        count = len(response.data) if response.data else len(rows)
        n_updated = sum(
            1 for r in rows
            if (r["home_team"], r["away_team"], r["league_key"], r["outcome"], r["kickoff"]) in existing_keys
        )
        n_created = count - n_updated
        logger.info("Pushed %d bet row(s) to Supabase (%d new, %d updated).", count, n_created, n_updated)
        return count
    except Exception as exc:
        logger.error("Failed to push bets to Supabase: %s", exc)
        raise
