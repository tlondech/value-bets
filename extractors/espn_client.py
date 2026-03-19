"""Shared ESPN public API client.

Thin HTTP transport for all ESPN scoreboard requests.
No API key required. No caching — that is a caller concern.
Subclass this to build sport-specific ESPN clients.
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import date, timedelta, timezone
from datetime import datetime as _datetime

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"
_RATE_LIMIT_SECONDS = 0.3
_TIMEOUT = 30


class ESPNClient(ABC):
    """
    Abstract ESPN client. Each sport subclass defines LEAGUE_MAP and implements fetch_recent_results().

    fetch_scoreboard() is the concrete HTTP transport — do not override.
    """

    SPORT: str = ""                  # e.g. "soccer", "basketball", "tennis"
    LEAGUE_MAP: dict[str, str] = {}  # league_key → ESPN slug; override in each subclass

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

    @abstractmethod
    def fetch_recent_results(self, days_back: int = 7) -> list[dict]:
        """
        Returns completed matches from the last N days.

        Uniform schema across all sports:
            {home_team, away_team, home_score, away_score, match_date, league_key}

        Tennis maps winner → home_team, loser → away_team.
        Non-fatal — returns [] on failure.
        """
        ...

    # ------------------------------------------------------------------
    # Convenience helper — available to all subclasses
    # ------------------------------------------------------------------

    def _fetch_scoreboard_recent(
        self,
        sport: str,
        league: str,
        days_back: int,
    ) -> list[dict]:
        """Fetches scoreboard events from today-N through today."""
        today = _datetime.now(timezone.utc).date()
        start = today - timedelta(days=days_back)
        return self.fetch_scoreboard(sport, league, start, today)
