import logging
from datetime import datetime, timezone

import requests

from constants import ODDS_API_QUOTA_CRITICAL, ODDS_API_TIMEOUT

# Imported lazily inside fetch_active_tennis_leagues to avoid circular imports
# (config imports from constants; odds is imported by config indirectly)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.the-odds-api.com"


class OddsAPIError(Exception):
    pass


def _pick_best_totals_line(outcomes: list[dict]) -> tuple[float | None, float | None, float | None]:
    """Return (line, over_price, under_price) for the totals line with the least vig.

    Groups outcomes by point value, then picks the line where both Over and Under
    are present and 1/over + 1/under is smallest (most efficient market).
    """
    lines: dict[float, dict] = {}
    for o in outcomes:
        pt = o.get("point")
        name = o.get("name")
        price = o.get("price")
        if pt is None or name not in ("Over", "Under") or not price:
            continue
        lines.setdefault(pt, {})[name] = price

    best_line, best_over, best_under = None, None, None
    best_vig = float("inf")
    for pt, sides in lines.items():
        if "Over" in sides and "Under" in sides:
            vig = 1.0 / sides["Over"] + 1.0 / sides["Under"]
            if vig < best_vig:
                best_vig = vig
                best_line, best_over, best_under = pt, sides["Over"], sides["Under"]
    return best_line, best_over, best_under


def _parse_spread_outcomes(
    outcomes: list[dict],
    home_team: str,
    away_team: str,
) -> dict | None:
    """
    Parses a spreads market outcomes list into {home_point, home_odds, away_odds}.

    Returns None if both home and away entries are not found.
    The home_point is the point handicap for the home team
    (e.g. -5.5 means home is favoured by 5.5 points).
    """
    home_point = None
    home_odds  = None
    away_odds  = None
    for outcome in outcomes:
        name  = outcome.get("name", "")
        price = outcome.get("price")
        point = outcome.get("point")
        if price is None or point is None:
            continue
        if name == home_team:
            home_point = point
            home_odds  = price
        elif name == away_team:
            away_odds = price
    if home_point is None or home_odds is None or away_odds is None:
        return None
    return {"home_point": home_point, "home_odds": home_odds, "away_odds": away_odds}


class OddsAPIClient:
    def __init__(
        self,
        api_key: str,
        sport: str,
        region: str,
        bookmaker: str,
        market: str,
        odds_format: str = "decimal",
        totals_bookmakers: str = "",
        extra_markets: list[str] | None = None,
    ):
        self.api_key = api_key
        self.sport = sport
        self.region = region
        self.bookmaker = bookmaker
        self.market = market
        self.odds_format = odds_format
        self.totals_bookmakers = totals_bookmakers
        self.extra_markets: list[str] = extra_markets or []
        self._quota_remaining: int | None = None

    @property
    def quota_remaining(self) -> int | None:
        return self._quota_remaining

    def fetch_upcoming_odds(self) -> list[dict]:
        """
        Fetches upcoming match odds from The Odds API filtered to the configured bookmaker.
        Returns a list of normalized event dicts.
        """
        url = f"{BASE_URL}/v4/sports/{self.sport}/odds/"
        # Build the bookmakers set: primary bookmaker + any totals fallbacks
        bk_set = {b.strip() for b in self.bookmaker.split(",") if b.strip()}
        for b in self.totals_bookmakers.split(","):
            if b.strip():
                bk_set.add(b.strip())
        base_markets = ["h2h", "totals"] + self.extra_markets
        params = {
            "apiKey": self.api_key,
            "regions": self.region,
            "markets": ",".join(base_markets),
            "bookmakers": ",".join(sorted(bk_set)),
            "oddsFormat": self.odds_format,
            "dateFormat": "iso",
            "includeSids": "true",
        }

        response = requests.get(url, params=params, timeout=ODDS_API_TIMEOUT)

        quota_header = response.headers.get("x-requests-remaining")
        if quota_header is not None:
            self._quota_remaining = int(quota_header)
            logger.debug("The Odds API quota remaining: %s", self._quota_remaining)
            if self._quota_remaining < ODDS_API_QUOTA_CRITICAL:
                raise OddsAPIError(
                    f"Quota critically low ({self._quota_remaining} requests remaining). "
                    "Aborting to preserve credits."
                )

        if response.status_code == 401:
            raise OddsAPIError("Invalid API key for The Odds API.")
        if response.status_code == 422:
            raise OddsAPIError(
                f"Invalid sport key '{self.sport}'. "
                "Check ODDS_SPORT in your .env (use 'soccer_france_ligue_one', not 'ligue1')."
            )
        if not response.ok:
            raise OddsAPIError(
                f"The Odds API returned {response.status_code}: {response.text}"
            )

        raw_events = response.json()
        logger.debug("Fetched %d upcoming matches from The Odds API.", len(raw_events))

        results = []
        for event in raw_events:
            parsed = self._parse_event(event)
            if parsed:
                results.append(parsed)
        return results

    def _parse_event(self, raw_event: dict) -> dict | None:
        """
        Flattens a single raw event into a normalized dict.
        Returns None if the bookmaker posted no odds for this event.
        """
        match_id = raw_event["id"]
        home_team = raw_event["home_team"]
        away_team = raw_event["away_team"]
        commence_time = datetime.fromisoformat(
            raw_event["commence_time"].replace("Z", "+00:00")
        ).astimezone(timezone.utc)

        bookmakers = raw_event.get("bookmakers", [])
        if not bookmakers:
            logger.warning("No bookmaker data for match %s (%s vs %s)", match_id, home_team, away_team)
            return None

        # Build a lookup by bookmaker key; primary bookmaker must be present for h2h
        bk_map = {bk["key"]: bk for bk in bookmakers}
        primary_bk = bk_map.get(self.bookmaker)
        if not primary_bk:
            logger.debug("Skipping %s vs %s — no odds from %s", home_team, away_team, self.bookmaker)
            return None
        markets = {m["key"]: m for m in primary_bk.get("markets", [])}
        h2h = markets.get("h2h")
        if not h2h:
            logger.debug("Skipping %s vs %s — no h2h market from %s", home_team, away_team, self.bookmaker)
            return None

        # outcomes list is UNORDERED — match by name, never by index
        home_odds = None
        draw_odds = None
        away_odds = None
        for outcome in h2h["outcomes"]:
            name = outcome["name"]
            price = outcome["price"]
            if name == home_team:
                home_odds = price
            elif name == away_team:
                away_odds = price
            elif name.lower() == "draw":
                draw_odds = price

        if home_odds is None or away_odds is None:
            logger.warning(
                "Could not find home/away odds for %s vs %s (match %s)",
                home_team, away_team, match_id,
            )
            return None

        # Parse totals market — pick the line with the least vig (most liquid).
        # Scan primary bookmaker first, then fallback bookmakers.
        totals_line = None
        over_odds = None
        under_odds = None
        ordered_bks = (
            [bk_map[self.bookmaker]] if self.bookmaker in bk_map else []
        ) + [bk_map[k] for k in bk_map if k != self.bookmaker]
        for bk_entry in ordered_bks:
            entry_markets = {m["key"]: m for m in bk_entry.get("markets", [])}
            totals = entry_markets.get("totals")
            if not totals:
                continue
            line, o_price, u_price = _pick_best_totals_line(totals.get("outcomes", []))
            if line is not None:
                totals_line = line
                over_odds = o_price
                under_odds = u_price
                break

        # Parse spreads market (basketball only; None for football/tennis)
        spread_home_point = None
        spread_home_odds = None
        spread_away_odds = None
        spreads_market = markets.get("spreads")
        if spreads_market:
            spread_result = _parse_spread_outcomes(
                spreads_market.get("outcomes", []), home_team, away_team
            )
            if spread_result:
                spread_home_point = spread_result["home_point"]
                spread_home_odds  = spread_result["home_odds"]
                spread_away_odds  = spread_result["away_odds"]

        sid = primary_bk.get("sid")
        bookmaker_link = f"https://www.winamax.fr/paris-sportifs/match/{sid}" if sid else None

        return {
            "match_id":          match_id,
            "home_team":         home_team,
            "away_team":         away_team,
            "commence_time":     commence_time,
            "home_odds":         home_odds,
            "draw_odds":         draw_odds,
            "away_odds":         away_odds,
            "totals_line":       totals_line,
            "over_odds":         over_odds,
            "under_odds":        under_odds,
            "spread_home_point": spread_home_point,
            "spread_home_odds":  spread_home_odds,
            "spread_away_odds":  spread_away_odds,
            "bookmaker":         primary_bk["key"],
            "bookmaker_link":    bookmaker_link,
        }


def fetch_active_tennis_leagues(api_key: str) -> list:
    """
    Fetches currently active ATP/WTA sport keys from The Odds API /v4/sports
    and returns a list of LeagueConfig objects ready to be added to cfg.enabled_leagues.

    Only currently active tournaments are returned (all=false filters out inactive sports).
    """
    from config import LeagueConfig  # local import to avoid circular dependency

    url = f"{BASE_URL}/v4/sports"
    try:
        response = requests.get(
            url,
            params={"apiKey": api_key, "all": "false"},
            timeout=ODDS_API_TIMEOUT,
        )
    except Exception as e:
        logger.warning("Could not fetch active tennis leagues: %s", e)
        return []

    if not response.ok:
        logger.warning("fetch_active_tennis_leagues: HTTP %d", response.status_code)
        return []

    leagues = []
    for sport in response.json():
        key = sport["key"]
        if key.startswith("tennis_atp_") or key.startswith("tennis_wta_"):
            leagues.append(LeagueConfig(
                key=key,
                display_name=sport["title"],
                odds_sport=key,
                sport_type="tennis",
            ))

    logger.debug("fetch_active_tennis_leagues: %d tournament(s) found", len(leagues))
    return leagues
