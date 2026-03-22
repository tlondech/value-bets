"""
Supabase client and remote persistence operations.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import cast

from supabase import create_client, Client

from constants import FIXTURE_DATE_TOLERANCE_SECONDS
from models.features import resolve_team_name

logger = logging.getLogger(__name__)


def _settle_totals(outcome: str, hg: int, ag: int) -> bool | None:
    """Parse a dynamic totals outcome string and determine if it won.

    When the line is a whole number the actual signal placed is on the nearest
    available half-point: over_234_0 → over_233_5, under_234_0 → under_234_5.
    The adjusted line is then settled with the standard half-point logic.

    Examples:
        "over_2_5"    → line=2.5  → won if hg+ag > 2
        "under_3_5"   → line=3.5  → won if hg+ag <= 3
        "over_234_0"  → line=233.5 → won if hg+ag > 233  (i.e. >= 234)
        "under_234_0" → line=234.5 → won if hg+ag <= 234
    Returns None if the outcome string is not a totals signal.
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


def _name_matches(a: str, b: str) -> bool:
    """True if two player name strings refer to the same person.

    Handles standard last-name comparison and reversed name order (e.g.
    "Shuai Zhang" vs "Zhang Shuai" used by ESPN for Chinese players).
    """
    if not a.strip() or not b.strip():
        return False
    if _last_name(a) == _last_name(b):
        return True
    # Reversed order: check if any word in a appears in b's words
    return bool(set(a.strip().lower().split()) & set(b.strip().lower().split()))


def _tennis_sets(matched, home: str) -> tuple[int | None, int | None]:
    """Returns (home_sets, away_sets) from a MatchData or dict, or (None, None) on failure.

    Score format is "w-l" per set with winner's games first (e.g. "6-2 1-6 6-1").
    Accepts both MatchData (score in metadata["score"]) and legacy dicts.
    """
    from extractors.base import MatchData as _MatchData
    if isinstance(matched, _MatchData):
        score = matched.metadata.get("score")
        home_name = matched.home_team
    else:
        score = matched.get("score")
        home_name = matched.get("home_team", "")
    if not score:
        return None, None
    try:
        parsed = [s.split("(")[0].split("-") for s in score.split()]
        winner_sets = sum(1 for w, l in parsed if int(w) > int(l))
        loser_sets  = len(parsed) - winner_sets
        home_winner = _name_matches(home_name, home)
        return (winner_sets, loser_sets) if home_winner else (loser_sets, winner_sets)
    except (ValueError, IndexError):
        return None, None


# ---------------------------------------------------------------------------
# Supabase-coupled private helpers
# ---------------------------------------------------------------------------

_SETTLE_KEYS = frozenset({
    "settled", "result", "settled_at", "actual_home_score", "actual_away_score", "score_detail"
})


def _write_settled_signals(supabase: Client, rows: list[dict], sport: str) -> int:
    """
    Persists evaluated settlement rows to signal_history.

    Includes only keys present in the row (football rows carry actual_home_score /
    actual_away_score; tennis rows do not — the key-filter handles both without branching).
    Returns the number of rows successfully written.
    """
    count = 0
    for row in rows:
        payload = {k: row[k] for k in _SETTLE_KEYS if k in row}
        try:
            (
                supabase.table("signal_history")
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
                "Failed to settle %s signal %s vs %s (%s) @ %s: %s",
                sport, row["home_team"], row["away_team"],
                row["outcome"], row["kickoff"], exc,
            )
    if count:
        logger.info("Settled %d %s signal(s).", count, sport)
    return count



# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_supabase_client() -> Client:
    """Creates a Supabase client from environment variables.

    Prefers SUPABASE_SERVICE_KEY (bypasses RLS — required after enabling RLS on
    signal_history).  Falls back to SUPABASE_ANON_KEY for local dev without a
    service key.
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_ANON_KEY must be set in your .env file."
        )
    return create_client(url, key)


def settle_supabase_signals(supabase: Client, all_fixtures, name_map: dict | None = None) -> int:
    """
    Settles signals directly against Supabase — works in CI where no local SQLite exists.

    Accepts list[MatchData] (from pipeline/settlement.py) or legacy list[dict].
    1. Fetches unsettled signals from Supabase whose kickoff is in the past.
    2. Matches them against fixtures by canonical team name + date.
    3. Evaluates each outcome and upserts the result back to Supabase.
    """
    from extractors.base import MatchData as _MatchData

    try:
        resp = (
            supabase.table("signal_history")
            .select("kickoff,home_team,away_team,home_canonical,away_canonical,league_key,outcome")
            .eq("settled", False)
            .eq("sport", "football")
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled signals from Supabase: %s", exc)
        return 0

    unsettled = cast(list[dict], resp.data or [])
    if not unsettled:
        logger.debug("No unsettled past signals found in Supabase.")
        return 0

    # Normalise fixtures to a uniform (home_team, away_team, kickoff_dt, home_score, away_score) tuple.
    # MatchData objects carry pre-resolved canonical names; raw dicts need resolve_team_name().
    fixture_index: dict[tuple, tuple[datetime, int | None, int | None]] = {}
    for f in all_fixtures:
        if isinstance(f, _MatchData):
            home_c, away_c = f.home_team, f.away_team
            fixture_index[(home_c, away_c)] = (f.kickoff, f.home_score, f.away_score)
        else:
            lk = f.get("league_key", "")
            home_c = (resolve_team_name(f["home_team"], name_map, lk) or f["home_team"]) if name_map and lk else f["home_team"]
            away_c = (resolve_team_name(f["away_team"], name_map, lk) or f["away_team"]) if name_map and lk else f["away_team"]
            fixture_dt = f["fixture_date"]
            if not isinstance(fixture_dt, datetime):
                fixture_dt = datetime.fromisoformat(str(fixture_dt))
            if fixture_dt.tzinfo is None:
                fixture_dt = fixture_dt.replace(tzinfo=timezone.utc)
            fixture_index[(home_c, away_c)] = (fixture_dt, f["home_goals"], f["away_goals"])

    rows_to_update = []
    settled_at = datetime.now(timezone.utc).isoformat()
    for row in unsettled:
        # Use stored canonical; fall back to resolving home_team when canonical is null
        home_key = row.get("home_canonical")
        away_key = row.get("away_canonical")
        if not home_key or not away_key:
            lk = row.get("league_key", "")
            if name_map and lk:
                home_key = resolve_team_name(row.get("home_team", ""), name_map, lk) or row.get("home_team")
                away_key = resolve_team_name(row.get("away_team", ""), name_map, lk) or row.get("away_team")
            else:
                home_key, away_key = row.get("home_team"), row.get("away_team")

        entry = fixture_index.get((home_key, away_key))
        if entry is None:
            continue

        fixture_dt, hg, ag = entry
        if hg is None or ag is None:
            continue

        # Date guard: fixture must be within ±1 day of kickoff
        kickoff_dt = datetime.fromisoformat(row["kickoff"].replace("Z", "+00:00"))
        if abs((fixture_dt - kickoff_dt).total_seconds()) > FIXTURE_DATE_TOLERANCE_SECONDS:
            continue

        outcome = row["outcome"]
        won = _settle_outcome(outcome, hg, ag, "football")
        if won is None:
            continue

        rows_to_update.append({
            "kickoff":           row["kickoff"],
            "home_team":         row["home_team"],
            "away_team":         row["away_team"],
            "outcome":           row["outcome"],
            "settled":           True,
            "result":            "hit" if won else "miss",
            "actual_home_score": hg,
            "actual_away_score": ag,
            "settled_at":        settled_at,
        })

    if not rows_to_update:
        logger.debug("No fixture matches found for unsettled signals.")
        return 0

    return _write_settled_signals(supabase, rows_to_update, "football via Supabase")


def settle_tennis_supabase_signals(supabase: Client) -> int:
    """
    Settles unsettled tennis signals in Supabase using ESPN scoreboard results.

    Fetches the last 14 days of completed ATP + WTA matches in two requests,
    then matches them against unsettled signals by player name and date proximity.
    """
    from extractors.espn_tennis_client import ESPNTennisClient

    try:
        resp = (
            supabase.table("signal_history")
            .select("id,kickoff,home_team,away_team,outcome,league_key")
            .eq("settled", False)
            .eq("sport", "tennis")
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled tennis signals: %s", exc)
        return 0

    unsettled = cast(list[dict], resp.data or [])
    if not unsettled:
        logger.debug("No unsettled past tennis signals found.")
        return 0

    all_results = ESPNTennisClient().fetch_recent_results(days_back=14)

    rows_to_update = []
    unmatched = []
    settled_at = datetime.now(timezone.utc).isoformat()

    for row in unsettled:
        kickoff_dt = datetime.fromisoformat(row["kickoff"].replace("Z", "+00:00"))
        home = row["home_team"]
        away = row["away_team"]

        matched = None
        for r in all_results:
            # Date guard: within ±2 days (tennis matches sometimes span midnight)
            r_kickoff = r.kickoff if r.kickoff.tzinfo else r.kickoff.replace(tzinfo=timezone.utc)
            if abs((r_kickoff - kickoff_dt).total_seconds()) > 2 * 86400:
                continue
            # Match by name (handles minor format differences and reversed order)
            if (_name_matches(home, r.home_team) or _name_matches(home, r.away_team)) and \
               (_name_matches(away, r.home_team) or _name_matches(away, r.away_team)):
                matched = r
                break

        if matched is None:
            unmatched.append(row)
            continue

        won = (
            (row["outcome"] == "home_win" and _name_matches(matched.home_team, home)) or
            (row["outcome"] == "away_win" and _name_matches(matched.home_team, away))
        )

        home_sets, away_sets = _tennis_sets(matched, home)

        update: dict = {
            "id":         row["id"],
            "home_team":  home,
            "away_team":  away,
            "outcome":    row["outcome"],
            "kickoff":    row["kickoff"],
            "settled":    True,
            "result":     "hit" if won else "miss",
            "settled_at": settled_at,
        }
        if home_sets is not None:
            update["actual_home_score"] = home_sets
            update["actual_away_score"] = away_sets
        score_str = matched.metadata.get("score")
        if score_str:
            update["score_detail"] = score_str
        rows_to_update.append(update)

    # Fallback: settle remaining unmatched signals via tennis-data.co.uk
    if unmatched:
        from extractors.tennisdatauk_client import fetch_tennis_results
        current_year = datetime.now(timezone.utc).year
        # Cache co.uk results per league_key to avoid duplicate fetches
        couk_cache: dict[str, list[dict]] = {}
        for row in unmatched:
            lk = row.get("league_key", "")
            if lk not in couk_cache:
                couk_cache[lk] = fetch_tennis_results(lk, current_year)
            couk_results = couk_cache[lk]

            kickoff_dt = datetime.fromisoformat(row["kickoff"].replace("Z", "+00:00"))
            home = row["home_team"]
            away = row["away_team"]

            matched_couk = None
            for r in couk_results:
                match_dt = r["match_date"]
                if not match_dt.tzinfo:
                    match_dt = match_dt.replace(tzinfo=timezone.utc)
                if abs((match_dt - kickoff_dt).total_seconds()) > 2 * 86400:
                    continue
                if (_name_matches(home, r["winner"]) or _name_matches(home, r["loser"])) and \
                   (_name_matches(away, r["winner"]) or _name_matches(away, r["loser"])):
                    matched_couk = r
                    break

            if matched_couk is None:
                continue

            won = (
                (row["outcome"] == "home_win" and _name_matches(matched_couk["winner"], home)) or
                (row["outcome"] == "away_win" and _name_matches(matched_couk["winner"], away))
            )
            rows_to_update.append({
                "id":         row["id"],
                "home_team":  home,
                "away_team":  away,
                "outcome":    row["outcome"],
                "kickoff":    row["kickoff"],
                "settled":    True,
                "result":     "hit" if won else "miss",
                "settled_at": settled_at,
            })

    if not rows_to_update:
        return 0

    return _write_settled_signals(supabase, rows_to_update, "tennis")


def backfill_tennis_scores(supabase: Client) -> int:
    """
    Backfills actual_home_score / actual_away_score / score_detail for already-settled
    tennis signals that are missing either set scores or the per-set breakdown string
    (e.g. settled before score tracking was added).
    """
    from extractors.espn_tennis_client import ESPNTennisClient

    try:
        # Fetch rows missing set scores OR missing score_detail
        resp = (
            supabase.table("signal_history")
            .select("id,kickoff,home_team,away_team,outcome,actual_home_score")
            .eq("settled", True)
            .eq("sport", "tennis")
            .or_("actual_home_score.is.null,score_detail.is.null")
            .execute()
        )
    except Exception as exc:
        logger.error("backfill_tennis_scores: fetch failed: %s", exc)
        return 0

    rows = cast(list[dict], resp.data or [])
    if not rows:
        logger.debug("backfill_tennis_scores: nothing to backfill.")
        return 0

    all_results = ESPNTennisClient().fetch_recent_results(days_back=14)
    count = 0

    for row in rows:
        kickoff_dt = datetime.fromisoformat(row["kickoff"].replace("Z", "+00:00"))
        home, away = row["home_team"], row["away_team"]

        matched = next(
            (
                r for r in all_results
                if abs(((r.kickoff if r.kickoff.tzinfo else r.kickoff.replace(tzinfo=timezone.utc)) - kickoff_dt).total_seconds()) <= 2 * 86400
                and _last_name(home) in {_last_name(r.home_team), _last_name(r.away_team)}
                and _last_name(away) in {_last_name(r.home_team), _last_name(r.away_team)}
            ),
            None,
        )
        if matched is None:
            continue

        score_str = matched.metadata.get("score")
        payload: dict = {}

        if row.get("actual_home_score") is None:
            home_sets, away_sets = _tennis_sets(matched, home)
            if home_sets is None:
                continue
            payload["actual_home_score"] = home_sets
            payload["actual_away_score"] = away_sets

        if score_str:
            payload["score_detail"] = score_str

        if not payload:
            continue

        try:
            supabase.table("signal_history").update(payload).eq("id", row["id"]).execute()
            count += 1
        except Exception as exc:
            logger.error("backfill_tennis_scores: update failed for id=%s: %s", row["id"], exc)

    if count:
        logger.info("backfill_tennis_scores: updated scores for %d signal(s).", count)
    return count


def settle_nba_supabase_signals(
    supabase: Client,
    nba_league_keys: list[str],
    name_map: dict[str, dict[str, str]] | None = None,
) -> int:
    """
    Settles unsettled NBA signals in Supabase using ESPN game results.

    Uses team_name_map.json["nba"] to normalise both sides to abbreviations
    (e.g. "LA Clippers" and "Los Angeles Clippers" both → "LAC"), so matching
    is robust to display-name differences between the odds source and ESPN.

    Settlement relies on ESPN only returning completed=True results, so no kickoff
    time guard is needed — unfinished games simply won't match.
    """
    from extractors.basketball_data_client import BasketballDataClient

    now = datetime.now(timezone.utc)
    try:
        resp = (
            supabase.table("signal_history")
            .select("id,kickoff,home_team,away_team,outcome,league_key")
            .eq("settled", False)
            .eq("sport", "basketball")
            .in_("league_key", nba_league_keys)
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled NBA signals: %s", exc)
        return 0

    unsettled = cast(list[dict], resp.data or [])
    if not unsettled:
        logger.debug("No unsettled NBA signals found.")
        return 0

    try:
        results = BasketballDataClient().fetch_recent_results(days_back=7)
    except Exception as exc:
        logger.warning("NBA results fetch failed — skipping settlement: %s", exc)
        return 0

    if not results:
        logger.debug("No recent NBA results found for settlement.")
        return 0

    # Build abbreviation lookup from team_name_map["nba"] so that both the
    # odds-source name ("Los Angeles Clippers") and the ESPN name ("LA Clippers")
    # resolve to the same abbreviation ("LAC") for matching.
    nba_abbr: dict[str, str] = {}
    if name_map:
        nba_abbr = {k.lower(): v for k, v in name_map.get("nba", {}).items()
                    if not k.startswith("_")}

    def _to_abbr(name: str) -> str:
        return nba_abbr.get(name.lower(), name.lower())

    # Index results by (home_abbr, away_abbr, game_date)  — r is now MatchData
    from extractors.base import MatchData as _MatchData
    results_index: dict[tuple, _MatchData] = {}
    for r in results:
        key = (_to_abbr(r.home_team), _to_abbr(r.away_team), r.kickoff.date())
        results_index[key] = r

    rows_to_update = []
    settled_at = now.isoformat()

    for row in unsettled:
        kickoff_dt = datetime.fromisoformat(row["kickoff"].replace("Z", "+00:00"))
        home_abbr = _to_abbr(row["home_team"])
        away_abbr = _to_abbr(row["away_team"])

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

        if matched.home_score is None or matched.away_score is None:
            continue
        home_pts = matched.home_score
        away_pts = matched.away_score
        outcome  = row["outcome"]

        won = _settle_outcome(outcome, home_pts, away_pts, "NBA")
        if won is None:
            continue

        rows_to_update.append({
            "id":        row["id"],
            "home_team": row["home_team"],
            "away_team": row["away_team"],
            "outcome":   outcome,
            "kickoff":   row["kickoff"],
            "settled":   True,
            "result":    "hit" if won else "miss",
            "actual_home_score": home_pts,
            "actual_away_score": away_pts,
            "settled_at": settled_at,
        })

    if not rows_to_update:
        return 0

    return _write_settled_signals(supabase, rows_to_update, "NBA")


def _settle_outcome(outcome: str, home_score: int, away_score: int, sport: str) -> bool | None:
    """Evaluates a single outcome against a final score.

    Returns True (win), False (loss), or None if the outcome key is unrecognised
    (caller should skip the row).
    """
    if outcome in ("home_win", "draw", "away_win"):
        return {"home_win": home_score > away_score, "draw": home_score == away_score, "away_win": away_score > home_score}[outcome]
    if outcome.startswith(("over_", "under_")):
        won = _settle_totals(outcome, home_score, away_score)
        return won if won is not None else False
    if outcome.startswith(("spread_home_", "spread_away_")):
        return _settle_spread(outcome, home_score, away_score)
    logger.debug("[%s] Unknown outcome key '%s' — skipping.", sport, outcome)
    return None


def _settle_spread(outcome: str, home_score: int, away_score: int) -> bool:
    """
    Determines if a spread/handicap signal won.

    Outcome encoding:
        "spread_home_m5_5"  → home covers -5.5  → home must win by > 5.5
        "spread_home_p3_5"  → home covers +3.5  → home wins or loses by < 3.5
        "spread_away_p5_5"  → away covers +5.5  → away wins or loses by < 5.5
        "spread_away_m3_5"  → away covers -3.5  → away wins by > 3.5
    """
    diff = home_score - away_score  # positive = home wins

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


def backfill_outcome_labels(supabase: Client) -> int:
    """
    Rewrites outcome_label for all home_win / away_win rows to use the actual
    team / player name (e.g. "Arsenal Win", "J. Ostapenko Win") instead of the
    old generic labels ("Home Win", "Away Win", "Player 1 Win", "Player 2 Win").

    Rows whose outcome_label already ends with " Win" and doesn't start with
    "Home" or "Away" or "Player" are left untouched.
    """
    _STALE = {"Home Win", "Away Win", "Player 1 Win", "Player 2 Win"}
    try:
        resp = (
            supabase.table("signal_history")
            .select("id,outcome,outcome_label,home_team,away_team")
            .in_("outcome", ["home_win", "away_win"])
            .execute()
        )
    except Exception as exc:
        logger.error("backfill_outcome_labels: fetch failed: %s", exc)
        return 0

    rows = cast(list[dict], resp.data or [])
    updated = 0
    for row in rows:
        current = row.get("outcome_label") or ""
        if current not in _STALE:
            continue
        new_label = (
            f"{row['home_team']} Win" if row["outcome"] == "home_win"
            else f"{row['away_team']} Win"
        )
        if new_label == current:
            continue
        try:
            supabase.table("signal_history").update({"outcome_label": new_label}).eq("id", row["id"]).execute()
            updated += 1
        except Exception as exc:
            logger.warning("backfill_outcome_labels: failed to update id=%s: %s", row["id"], exc)

    logger.info("backfill_outcome_labels: updated %d / %d rows.", updated, len(rows))
    return updated


def prune_stale_supabase_signals(
    supabase: Client,
    all_signals: list[dict],
    processed_league_keys: set[str],
) -> int:
    """
    Deletes unsettled future signals from Supabase for processed leagues whose
    outcome is no longer in the current detected set.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        resp = (
            supabase.table("signal_history")
            .select("id,kickoff,home_team,away_team,outcome,league_key")
            .eq("settled", False)
            .gt("kickoff", now_iso)
            .in_("league_key", list(processed_league_keys))
            .execute()
        )
    except Exception as exc:
        logger.error("Failed to fetch unsettled signals for pruning: %s", exc)
        return 0

    existing: list[dict] = cast(list[dict], resp.data or [])

    current_keys: set[tuple] = set()
    for m in all_signals:
        kickoff_utc = _utc_prefix(m["kickoff"])
        for s in m["signals"]:
            current_keys.add((kickoff_utc, m["home_team"], m["away_team"], s["outcome"]))

    stale_ids = [
        row["id"]
        for row in existing
        if (_utc_prefix(row["kickoff"]), row["home_team"], row["away_team"], row["outcome"])
        not in current_keys
    ]

    if not stale_ids:
        return 0

    try:
        supabase.table("signal_history").delete().in_("id", stale_ids).execute()
        logger.info("Pruned %d stale signal(s) from Supabase.", len(stale_ids))
        return len(stale_ids)
    except Exception as exc:
        logger.error("Failed to prune stale signals from Supabase: %s", exc)
        return 0


def push_signals_to_supabase(
    supabase: Client,
    signals: list[dict],
    recorded_date: str,
) -> int:
    """
    Upserts today's signals into the Supabase `signal_history` table.
    Uses the unique constraint (kickoff, home_team, away_team, outcome)
    to skip duplicates. Returns the number of rows upserted.
    """
    rows = []
    for m in signals:
        for s in m["signals"]:
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
                "outcome":        s["outcome"],
                "outcome_label":  s["outcome_label"],
                "odds":           s["odds"],
                "true_prob":      s["true_prob"],
                "ev":             s["ev"],
                "home_rank":       m.get("home_rank"),
                "away_rank":       m.get("away_rank"),
                "home_seed":       m.get("home_seed"),
                "away_seed":       m.get("away_seed"),
                "home_short_name": m.get("home_short_name"),
                "away_short_name": m.get("away_short_name"),
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
                "sport":          m.get("sport", "football"),
                "bookmaker_link": m.get("bookmaker_link"),
            })

    if not rows:
        logger.info("No signals to push to Supabase.")
        return 0

    # Before upserting, delete any unsettled rows for the same match that may
    # have a stale kickoff (e.g. match was rescheduled since last run).
    # Uses 1 SELECT + at most 1 DELETE instead of one DELETE per row.
    # Also used below to distinguish created vs updated rows in the log.
    existing_keys: set[tuple] = set()
    try:
        league_keys = list({r["league_key"] for r in rows})
        existing = (
            supabase.table("signal_history")  # noqa: duplicate table ref — intentional
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
            supabase.table("signal_history").delete().in_("id", stale_ids).execute()
            logger.info("Deleted %d stale signal row(s) before upsert.", len(stale_ids))
        stale_id_set = set(stale_ids)
        existing_keys = {
            (row["home_team"], row["away_team"], row["league_key"], row["outcome"], row["kickoff"])
            for row in existing_rows
            if row["id"] not in stale_id_set
        }
    except Exception as exc:
        logger.warning("Failed to clean up stale signals before upsert: %s", exc)

    try:
        response = (
            supabase.table("signal_history")
            .upsert(rows, on_conflict="kickoff,home_team,away_team,outcome")
            .execute()
        )
        count = len(response.data) if response.data else len(rows)
        n_updated = sum(
            1 for r in rows
            if (r["home_team"], r["away_team"], r["league_key"], r["outcome"], r["kickoff"]) in existing_keys
        )
        n_created = count - n_updated
        logger.info("Pushed %d signal row(s) to Supabase (%d new, %d updated).", count, n_created, n_updated)
        return count
    except Exception as exc:
        logger.error("Failed to push signals to Supabase: %s", exc)
        raise
