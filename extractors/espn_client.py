"""Shared ESPN public API client.

Thin HTTP transport for all ESPN scoreboard requests.
No API key required. No caching — that is a caller concern.
Used by NBA and tennis extractors.
"""

import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"
_RATE_LIMIT_SECONDS = 0.3
_TIMEOUT = 30


class ESPNClient:
    """
    Thin HTTP wrapper around the ESPN public scoreboard API.
    All methods return empty lists on failure (non-fatal).
    """

    def fetch_scoreboard(
        self,
        sport: str,
        league: str,
        start_date: date,
        end_date: date,
        limit: int = 500,
    ) -> list[dict]:
        """
        Fetches raw events from the ESPN scoreboard for a date range.

        GET {BASE_URL}/{sport}/{league}/scoreboard?dates=YYYYMMDD-YYYYMMDD

        Returns the raw ``events`` array from the ESPN JSON response, or []
        on any network / parse failure.
        """
        date_range = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
        url = f"{_BASE_URL}/{sport}/{league}/scoreboard"
        try:
            time.sleep(_RATE_LIMIT_SECONDS)
            r = requests.get(
                url,
                params={"dates": date_range, "limit": limit},
                timeout=_TIMEOUT,
            )
            r.raise_for_status()
            return r.json().get("events", [])
        except Exception as exc:
            logger.warning(
                "ESPN scoreboard fetch failed (%s/%s %s): %s",
                sport, league, date_range, exc,
            )
            return []

    def fetch_recent_results(
        self,
        sport: str,
        league: str,
        days_back: int = 7,
    ) -> list[dict]:
        """Convenience wrapper: fetches events from today-N through today."""
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=days_back)
        return self.fetch_scoreboard(sport, league, start, today)

    def fetch_tennis_recent_results(self, days_back: int = 14) -> list[dict]:
        """
        Fetches completed ATP + WTA matches for the last N days.

        Returns a list of:
            {"winner": str, "loser": str, "match_date": datetime, "league_slug": str}

        Player names come from ESPN's ``displayName`` field.
        Returns [] for any league that fails (non-fatal).
        """
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=days_back)
        results: list[dict] = []

        for league_slug in ("atp", "wta"):
            events = self.fetch_scoreboard("tennis", league_slug, start, today)
            if not events:
                logger.debug("ESPN tennis: no events returned for %s", league_slug)
                continue

            logger.debug(
                "ESPN tennis %s: %d event(s) returned, first keys: %s",
                league_slug, len(events), list(events[0].keys()),
            )

            for event in events:
                # Tennis scoreboard nests competitions inside groupings[]
                groupings = event.get("groupings") or []
                comps = [
                    comp
                    for g in groupings
                    for comp in (g.get("competitions") or [])
                ] or event.get("competitions") or []

                for comp in comps:
                    if not comp.get("status", {}).get("type", {}).get("completed"):
                        continue

                    competitors = comp.get("competitors", [])
                    if len(competitors) != 2:
                        continue

                    winner_name: str | None = None
                    loser_name:  str | None = None

                    for c in competitors:
                        # Tennis competitors use "athlete"; team sports use "team"
                        entity = c.get("athlete") or c.get("team") or {}
                        name = (
                            entity.get("displayName")
                            or entity.get("fullName")
                            or entity.get("name")
                        )
                        if not name:
                            continue
                        if c.get("winner"):
                            winner_name = name
                        else:
                            loser_name = name

                    # Fallback: if no explicit winner flag, use set-score comparison
                    if (winner_name is None or loser_name is None) and len(competitors) == 2:
                        c0, c1 = competitors[0], competitors[1]
                        try:
                            s0 = int(c0.get("score") or 0)
                            s1 = int(c1.get("score") or 0)
                            e0 = c0.get("athlete") or c0.get("team") or {}
                            e1 = c1.get("athlete") or c1.get("team") or {}
                            n0 = e0.get("displayName") or e0.get("fullName") or e0.get("name")
                            n1 = e1.get("displayName") or e1.get("fullName") or e1.get("name")
                            if n0 and n1 and s0 != s1:
                                winner_name, loser_name = (n0, n1) if s0 > s1 else (n1, n0)
                        except (ValueError, TypeError):
                            pass

                    if not winner_name or not loser_name:
                        continue

                    # Use competition date (actual match time), fall back to event date
                    raw_date = comp.get("date") or event.get("date", "")
                    try:
                        match_date = datetime.strptime(raw_date[:10], "%Y-%m-%d")
                    except (KeyError, ValueError):
                        continue

                    # Build set score string from winner's linescores vs loser's
                    score: str | None = None
                    try:
                        winner_c = next(c for c in competitors if c.get("winner"))
                        loser_c  = next(c for c in competitors if not c.get("winner"))
                        w_sets = winner_c.get("linescores") or []
                        l_sets = loser_c.get("linescores") or []
                        if w_sets and len(w_sets) == len(l_sets):
                            parts = []
                            for ws, ls in zip(w_sets, l_sets):
                                wv = int(ws.get("value", 0))
                                lv = int(ls.get("value", 0))
                                tb = ws.get("tiebreak") or ls.get("tiebreak")
                                parts.append(f"{wv}-{lv}({tb})" if tb is not None else f"{wv}-{lv}")
                            score = " ".join(parts)
                    except (StopIteration, TypeError, ValueError):
                        pass

                    results.append({
                        "winner":      winner_name,
                        "loser":       loser_name,
                        "score":       score,
                        "match_date":  match_date,
                        "league_slug": league_slug,
                    })

        # Deduplicate: ESPN serves the same matches under both atp and wta slugs
        seen: set[tuple] = set()
        unique: list[dict] = []
        for r in results:
            key = (r["winner"], r["loser"], r["match_date"].date())
            if key not in seen:
                seen.add(key)
                unique.append(r)

        logger.debug(
            "ESPN tennis recent results: %d completed matches over %d days (ATP+WTA, %d dupes removed)",
            len(unique), days_back, len(results) - len(unique),
        )
        return unique
