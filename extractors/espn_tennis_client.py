"""ESPN public API client for tennis match results.

Fetches completed ATP and WTA matches and normalizes them to MatchData
(winner → home_team, loser → away_team; sets won → home_score / away_score).
"""

import logging
from datetime import datetime, timedelta, timezone

from extractors.base import MatchData
from extractors.espn_client import ESPNClient

logger = logging.getLogger(__name__)


class ESPNTennisClient(ESPNClient):
    SPORT = "tennis"

    LEAGUE_MAP: dict[str, str] = {
        "atp": "atp",
        "wta": "wta",
    }

    def fetch_recent_results(self, days_back: int = 14) -> list[MatchData]:
        """
        Fetches completed ATP + WTA matches for the last N days.

        winner → home_team, loser → away_team.
        home_score / away_score = sets won by winner / loser.
        metadata["score"] = set-score string (e.g. "6-2 3-6 6-1").
        Deduplication removes cross-slug duplicates.
        Non-fatal — returns [] on failure.
        """
        results: list[MatchData] = []

        for league_slug in self.LEAGUE_MAP.values():
            events = self._fetch_scoreboard_recent(self.SPORT, league_slug, days_back)
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
                    winner_flag: str | None = None
                    loser_flag:  str | None = None

                    for c in competitors:
                        entity = c.get("athlete") or c.get("team") or {}
                        name = (
                            entity.get("displayName")
                            or entity.get("fullName")
                            or entity.get("name")
                        )
                        if not name:
                            continue
                        flag_href = (entity.get("flag") or {}).get("href")
                        if c.get("winner"):
                            winner_name = name
                            winner_flag = flag_href
                        else:
                            loser_name = name
                            loser_flag = flag_href

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
                        match_date = match_date.replace(tzinfo=timezone.utc)
                    except (KeyError, ValueError):
                        continue

                    date_str = match_date.strftime("%Y%m%d")
                    fixture_id = f"espn_tennis_{league_slug}_{winner_name}_{loser_name}_{date_str}"

                    # Build set score string and count sets won per player
                    score: str | None = None
                    winner_sets = 0
                    loser_sets = 0
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
                                if wv > lv:
                                    winner_sets += 1
                                else:
                                    loser_sets += 1
                            score = " ".join(parts)
                    except (StopIteration, TypeError, ValueError):
                        pass

                    results.append(MatchData(
                        fixture_id=fixture_id,
                        sport="tennis",
                        league_key=league_slug,
                        kickoff=match_date,
                        home_team=winner_name,
                        away_team=loser_name,
                        home_score=winner_sets,
                        away_score=loser_sets,
                        completed=True,
                        metadata={"score": score, "home_flag": winner_flag, "away_flag": loser_flag},
                    ))

        # Deduplicate: ESPN serves the same matches under both atp and wta slugs
        seen: set[tuple] = set()
        unique: list[MatchData] = []
        for r in results:
            key = (r.home_team, r.away_team, r.kickoff.date())
            if key not in seen:
                seen.add(key)
                unique.append(r)

        logger.debug(
            "ESPN tennis recent results: %d completed matches over %d days (ATP+WTA, %d dupes removed)",
            len(unique), days_back, len(results) - len(unique),
        )
        return unique

    def fetch_upcoming_matches(self, days_ahead: int = 7) -> list[MatchData]:
        """Returns scheduled (not yet completed) tennis matches from ESPN."""
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=days_ahead)
        matches: list[MatchData] = []
        seen: set[tuple] = set()

        for league_slug in self.LEAGUE_MAP.values():
            events = self.fetch_scoreboard(self.SPORT, league_slug, today, end)
            for event in events:
                groupings = event.get("groupings") or []
                comps = [
                    comp
                    for g in groupings
                    for comp in (g.get("competitions") or [])
                ] or event.get("competitions") or []

                for comp in comps:
                    if comp.get("status", {}).get("type", {}).get("completed"):
                        continue
                    competitors = comp.get("competitors", [])
                    if len(competitors) != 2:
                        continue
                    names = []
                    for c in competitors:
                        entity = c.get("athlete") or c.get("team") or {}
                        name = entity.get("displayName") or entity.get("fullName") or entity.get("name")
                        if name:
                            names.append(name)
                    if len(names) != 2:
                        continue
                    raw_date = comp.get("date") or event.get("date", "")
                    try:
                        kickoff = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                        if kickoff.tzinfo is None:
                            kickoff = kickoff.replace(tzinfo=timezone.utc)
                    except (ValueError, AttributeError):
                        continue
                    key = (names[0], names[1], kickoff.date())
                    if key in seen:
                        continue
                    seen.add(key)
                    date_str = kickoff.strftime("%Y%m%d")
                    round_raw = (comp.get("round") or {}).get("displayName")
                    round_compact = _compact_round(round_raw) if round_raw else None
                    matches.append(MatchData(
                        fixture_id=f"espn_tennis_{league_slug}_{names[0]}_{names[1]}_{date_str}",
                        sport="tennis",
                        league_key=league_slug,
                        kickoff=kickoff,
                        home_team=names[0],
                        away_team=names[1],
                        completed=False,
                        metadata={"round": round_compact},
                    ))
        return matches


def _compact_round(display_name: str) -> str:
    """Converts ESPN round display names to compact badge format.

    Examples:
        "Final"                  → "Final"
        "Semifinals"             → "SF"
        "Quarterfinals"          → "QF"
        "Round of 16"            → "R16"
        "3rd Round"              → "R3"
        "Qualifying 2nd Round"   → "Q2"
    """
    dn = display_name.lower().strip()
    if dn in ("final", "finals"):
        return "Final"
    if "semifinal" in dn:
        return "SF"
    if "quarterfinal" in dn:
        return "QF"
    import re
    m = re.search(r"round of (\d+)", dn)
    if m:
        return f"R{m.group(1)}"
    if dn.startswith("qualifying"):
        m = re.search(r"(\d+)", dn)
        return f"Q{m.group(1)}" if m else "Q"
    m = re.search(r"(\d+)", dn)
    if m:
        return f"R{m.group(1)}"
    return display_name
