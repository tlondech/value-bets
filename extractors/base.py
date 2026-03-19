"""Universal match schema and extractor protocol for all sport data sources.

Any ESPN sport client must satisfy the SportsExtractor Protocol:
  - fetch_recent_results(days_back) → list[MatchData]   (completed matches, for settlement)
  - fetch_upcoming_matches(days_ahead) → list[MatchData] (scheduled matches, for discovery)

OddsAPIClient is NOT a SportsExtractor — it is the separate, authoritative source
of upcoming match odds for EV evaluation and does not return MatchData.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


@dataclass
class MatchData:
    """Universal match representation across all sports.

    Field semantics per sport:
        home_score / away_score:
            football   → goals
            basketball → points
            tennis     → sets won (winner → home_team)
        home_team / away_team:
            tennis     → winner / loser
        metadata:
            football   → {"home_xg": float|None, "away_xg": float|None,
                          "home_logo": str|None, "away_logo": str|None}
            tennis     → {"score": "6-2 3-6 6-1"}  (set-score string for settlement)
    """

    fixture_id: str
    sport:      str            # 'football' | 'basketball' | 'tennis'
    league_key: str            # 'epl', 'nba', 'atp', …
    kickoff:    datetime       # UTC-aware
    home_team:  str            # winner in tennis
    away_team:  str            # loser in tennis

    home_score: int | None = None   # goals | pts | sets won
    away_score: int | None = None
    completed:  bool = False
    metadata:   dict = field(default_factory=dict)

    def to_settlement_dict(self) -> dict:
        """Returns a dict ready for a Supabase signal_history UPDATE.

        Maps Python field names to the exact DB column names:
            home_score → actual_home_score   (goals / pts / sets — all the same column)
            away_score → actual_away_score
        """
        return {
            "actual_home_score": self.home_score,
            "actual_away_score": self.away_score,
            "settled":           self.completed,
        }


class SportsExtractor(Protocol):
    """Contract that every ESPN sport client must satisfy.

    Static type checkers (Pylance / mypy) will flag any client that is missing
    one of these methods or returns the wrong type.
    """

    SPORT:      str
    LEAGUE_MAP: dict[str, str]

    def fetch_recent_results(self, days_back: int = 7) -> list[MatchData]:
        """Returns completed matches from the last N days."""
        ...

    def fetch_upcoming_matches(self, days_ahead: int = 7) -> list[MatchData]:
        """Returns scheduled (not yet completed) matches from ESPN."""
        ...
