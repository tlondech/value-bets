import os
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, String, Text, UniqueConstraint,
    create_engine, text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Match(Base):
    __tablename__ = "matches"

    match_id = Column(String, primary_key=True)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    match_date = Column(DateTime, nullable=False)
    league = Column(String, nullable=False)
    status = Column(String, default="upcoming")  # upcoming | finished
    stage = Column(String, nullable=True)         # e.g. "Matchday 28", "Round of 16"
    home_goals = Column(Integer, nullable=True)
    away_goals = Column(Integer, nullable=True)


class Odds(Base):
    __tablename__ = "odds"

    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String, nullable=False)
    bookmaker = Column(String, nullable=False)
    market = Column(String, nullable=False)
    home_odds = Column(Float, nullable=True)
    draw_odds = Column(Float, nullable=True)
    away_odds = Column(Float, nullable=True)
    totals_line = Column(Float, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)


class Fixture(Base):
    """Stores individual finished match results for rolling-window calculations."""
    __tablename__ = "fixtures"

    fixture_id: Mapped[str] = mapped_column(String, primary_key=True)
    league_id: Mapped[str] = mapped_column(String, nullable=False)
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    fixture_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    home_team: Mapped[str] = mapped_column(String, nullable=False)
    away_team: Mapped[str] = mapped_column(String, nullable=False)
    home_goals: Mapped[int] = mapped_column(Integer, nullable=False)
    away_goals: Mapped[int] = mapped_column(Integer, nullable=False)
    home_xg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    away_xg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class BetHistory(Base):
    """Persistent record of every recommended bet and its eventual outcome."""
    __tablename__ = "bet_history"

    id                = Column(Integer,  primary_key=True, autoincrement=True)
    recorded_date     = Column(String,   nullable=False)   # YYYY-MM-DD
    league_key        = Column(String,   nullable=False)
    league_name       = Column(String,   nullable=False)
    home_team         = Column(String,   nullable=False)   # Winamax display name
    away_team         = Column(String,   nullable=False)
    home_canonical    = Column(String,   nullable=True)    # for result lookup
    away_canonical    = Column(String,   nullable=True)
    kickoff           = Column(DateTime, nullable=False)
    stage             = Column(String,   nullable=True)
    outcome           = Column(String,   nullable=False)   # "home_win" | "draw" | "away_win" | "over_2_5" | "under_2_5"
    outcome_label     = Column(String,   nullable=False)
    odds              = Column(Float,    nullable=False)
    true_prob         = Column(Float,    nullable=False)
    ev                = Column(Float,    nullable=False)
    settled           = Column(Boolean,  default=False)
    result            = Column(String,   nullable=True)    # "won" | "lost"
    actual_home_goals = Column(Integer,  nullable=True)
    actual_away_goals = Column(Integer,  nullable=True)
    settled_at        = Column(DateTime, nullable=True)
    home_rank         = Column(Integer,  nullable=True)
    away_rank         = Column(Integer,  nullable=True)
    home_form         = Column(Text,     nullable=True)    # JSON array
    away_form         = Column(Text,     nullable=True)    # JSON array
    home_crest        = Column(String,   nullable=True)
    away_crest        = Column(String,   nullable=True)
    home_rest_days    = Column(Integer,  nullable=True)
    away_rest_days    = Column(Integer,  nullable=True)
    h2h_used          = Column(Boolean,  nullable=True)
    is_second_leg     = Column(Boolean,  nullable=True)
    agg_home          = Column(Integer,  nullable=True)
    agg_away          = Column(Integer,  nullable=True)
    leg1_result       = Column(Text,     nullable=True)    # JSON object

    __table_args__ = (
        UniqueConstraint("kickoff", "home_team", "away_team", "outcome",
                         name="uq_bet_history"),
    )


def init_db(db_path: str):
    """Creates all tables if they don't exist. Returns the SQLAlchemy engine."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        for col_ddl in [
            "ALTER TABLE matches ADD COLUMN stage TEXT",
            "ALTER TABLE bet_history ADD COLUMN home_rank INTEGER",
            "ALTER TABLE bet_history ADD COLUMN away_rank INTEGER",
            "ALTER TABLE bet_history ADD COLUMN home_form TEXT",
            "ALTER TABLE bet_history ADD COLUMN away_form TEXT",
            "ALTER TABLE bet_history ADD COLUMN home_crest TEXT",
            "ALTER TABLE bet_history ADD COLUMN away_crest TEXT",
            "ALTER TABLE bet_history ADD COLUMN home_rest_days INTEGER",
            "ALTER TABLE bet_history ADD COLUMN away_rest_days INTEGER",
            "ALTER TABLE bet_history ADD COLUMN h2h_used BOOLEAN",
            "ALTER TABLE bet_history ADD COLUMN is_second_leg BOOLEAN",
            "ALTER TABLE bet_history ADD COLUMN agg_home INTEGER",
            "ALTER TABLE bet_history ADD COLUMN agg_away INTEGER",
            "ALTER TABLE bet_history ADD COLUMN leg1_result TEXT",
            "ALTER TABLE odds ADD COLUMN totals_line REAL",
        ]:
            try:
                conn.execute(text(col_ddl))
                conn.commit()
            except Exception:
                pass  # column already exists
    return engine
