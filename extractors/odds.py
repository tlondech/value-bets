import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.the-odds-api.com"


class OddsAPIError(Exception):
    pass


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
    ):
        self.api_key = api_key
        self.sport = sport
        self.region = region
        self.bookmaker = bookmaker
        self.market = market
        self.odds_format = odds_format
        self.totals_bookmakers = totals_bookmakers
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
        params = {
            "apiKey": self.api_key,
            "regions": self.region,
            "markets": "h2h,totals",
            "bookmakers": ",".join(sorted(bk_set)),
            "oddsFormat": self.odds_format,
            "dateFormat": "iso",
        }

        response = requests.get(url, params=params, timeout=15)

        quota_header = response.headers.get("x-requests-remaining")
        if quota_header is not None:
            self._quota_remaining = int(quota_header)
            logger.debug("The Odds API quota remaining: %s", self._quota_remaining)
            if self._quota_remaining < 10:
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
        primary_bk = bk_map.get(self.bookmaker) or bookmakers[0]
        markets = {m["key"]: m for m in primary_bk.get("markets", [])}
        h2h = markets.get("h2h")
        if not h2h:
            logger.warning("No h2h market for match %s", match_id)
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

        # Parse totals market for the 2.5 goals line.
        # Scan primary bookmaker first, then fallback bookmakers until both sides are found.
        over_2_5_odds = None
        under_2_5_odds = None
        ordered_bks = (
            [bk_map[self.bookmaker]] if self.bookmaker in bk_map else []
        ) + [bk_map[k] for k in bk_map if k != self.bookmaker]
        for bk_entry in ordered_bks:
            entry_markets = {m["key"]: m for m in bk_entry.get("markets", [])}
            totals = entry_markets.get("totals")
            if not totals:
                continue
            for outcome in totals.get("outcomes", []):
                if abs(outcome.get("point", 0) - 2.5) < 0.01:
                    if outcome["name"] == "Over" and over_2_5_odds is None:
                        over_2_5_odds = outcome["price"]
                    elif outcome["name"] == "Under" and under_2_5_odds is None:
                        under_2_5_odds = outcome["price"]
            if over_2_5_odds is not None and under_2_5_odds is not None:
                break

        # Parse bts (both teams to score) market.
        btts_yes_odds = None
        btts_no_odds = None
        for bk_entry in ordered_bks:
            entry_markets = {m["key"]: m for m in bk_entry.get("markets", [])}
            bts = entry_markets.get("btts")
            if not bts:
                continue
            for outcome in bts.get("outcomes", []):
                if outcome["name"] == "Yes" and btts_yes_odds is None:
                    btts_yes_odds = outcome["price"]
                elif outcome["name"] == "No" and btts_no_odds is None:
                    btts_no_odds = outcome["price"]
            if btts_yes_odds is not None and btts_no_odds is not None:
                break

        return {
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "commence_time": commence_time,
            "home_odds": home_odds,
            "draw_odds": draw_odds,
            "away_odds": away_odds,
            "over_2_5_odds": over_2_5_odds,
            "under_2_5_odds": under_2_5_odds,
            "btts_yes_odds": btts_yes_odds,
            "btts_no_odds": btts_no_odds,
            "bookmaker": primary_bk["key"],
        }

    def fetch_btts_odds(
        self,
        sport: str,
        event_ids: list[str],
        btts_bookmakers: str,
    ) -> dict[str, tuple[float | None, float | None]]:
        """
        Fetches BTTS (both teams to score) odds for a list of event IDs using the
        per-event endpoint (the only endpoint that supports the `btts` market).

        Returns {event_id: (btts_yes_odds, btts_no_odds)}.
        Each call costs 1 API quota unit, so this is called only when
        ODDS_BTTS_BOOKMAKERS is configured.
        """
        bk_list = [b.strip() for b in btts_bookmakers.split(",") if b.strip()]
        if not bk_list:
            return {}

        results: dict[str, tuple[float | None, float | None]] = {}
        for event_id in event_ids:
            url = f"{BASE_URL}/v4/sports/{sport}/events/{event_id}/odds/"
            params = {
                "apiKey": self.api_key,
                "regions": self.region,
                "markets": "btts",
                "bookmakers": ",".join(bk_list),
                "oddsFormat": self.odds_format,
            }
            try:
                response = requests.get(url, params=params, timeout=15)
                quota_header = response.headers.get("x-requests-remaining")
                if quota_header is not None:
                    self._quota_remaining = int(quota_header)
                    if self._quota_remaining < 10:
                        logger.warning("Quota critically low (%s); stopping BTTS fetch.", self._quota_remaining)
                        break
                if not response.ok:
                    logger.debug("BTTS fetch for %s returned %s — skipping.", event_id, response.status_code)
                    results[event_id] = (None, None)
                    continue
                data = response.json()
                yes_odds = None
                no_odds = None
                for bk in data.get("bookmakers", []):
                    for market in bk.get("markets", []):
                        if market["key"] != "btts":
                            continue
                        for outcome in market.get("outcomes", []):
                            if outcome["name"] == "Yes" and yes_odds is None:
                                yes_odds = outcome["price"]
                            elif outcome["name"] == "No" and no_odds is None:
                                no_odds = outcome["price"]
                    if yes_odds is not None and no_odds is not None:
                        break
                results[event_id] = (yes_odds, no_odds)
            except Exception as exc:
                logger.debug("BTTS fetch error for %s: %s", event_id, exc)
                results[event_id] = (None, None)
        return results
