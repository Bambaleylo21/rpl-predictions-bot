from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, String, UniqueConstraint, ForeignKey, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True, nullable=False)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    photo_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class Tournament(Base):
    __tablename__ = "tournaments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(16), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    round_min: Mapped[int] = mapped_column(Integer, nullable=False)
    round_max: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active", index=True)
    visible_in_miniapp: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    join_open: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    predict_open: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    planned_matches_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class UserTournament(Base):
    __tablename__ = "user_tournaments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    tournament_id: Mapped[int] = mapped_column(ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False, index=True)
    display_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    bonus_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    bonus_winner: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    bonus_scorer: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("tg_user_id", "tournament_id", name="uq_user_tournaments_user_tournament"),
    )


class Match(Base):
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tournament_id: Mapped[int] = mapped_column(
        ForeignKey("tournaments.id", ondelete="RESTRICT"),
        nullable=False,
        default=1,
        server_default="1",
        index=True,
    )
    round_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    home_team: Mapped[str] = mapped_column(String(64), nullable=False)
    away_team: Mapped[str] = mapped_column(String(64), nullable=False)
    group_label: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)

    kickoff_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    home_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    away_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Источник матча:
    # - "manual" — добавлен админом /admin_add_match
    # - "apisport" — подтянут из API-Sport.ru
    source: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="manual",
        server_default="manual",
        index=True,
    )

    # внешний id матча из API (для API-Sport.ru используем match id)
    api_fixture_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    is_placeholder: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    pred_home: Mapped[int] = mapped_column(Integer, nullable=False)
    pred_away: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("match_id", "tg_user_id", name="uq_predictions_match_user"),
    )


class Point(Base):
    __tablename__ = "points"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    points: Mapped[int] = mapped_column(Integer, nullable=False)
    category: Mapped[str] = mapped_column(String(16), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("match_id", "tg_user_id", name="uq_points_match_user"),
    )


class Setting(Base):
    """Простая таблица key/value для настроек турнира (окно турнира и т.п.)."""
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(256), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class LongtermPrediction(Base):
    __tablename__ = "longterm_predictions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tournament_id: Mapped[int] = mapped_column(ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    pick_type: Mapped[str] = mapped_column(String(16), nullable=False, index=True)  # winner | scorer
    pick_value: Mapped[str] = mapped_column(String(128), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("tournament_id", "tg_user_id", "pick_type", name="uq_longterm_predictions_tour_user_type"),
    )


class Season(Base):
    __tablename__ = "seasons"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1", index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class Stage(Base):
    __tablename__ = "stages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("seasons.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(32), nullable=False)
    stage_order: Mapped[int] = mapped_column(Integer, nullable=False)
    round_min: Mapped[int] = mapped_column(Integer, nullable=False)
    round_max: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0", index=True)
    is_completed: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0", index=True)
    promote_count: Mapped[int] = mapped_column(Integer, nullable=False, default=2, server_default="2")
    relegate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=2, server_default="2")

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("season_id", "stage_order", name="uq_stages_season_order"),
    )


class League(Base):
    __tablename__ = "leagues"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("seasons.id", ondelete="CASCADE"), nullable=False, index=True)
    code: Mapped[str] = mapped_column(String(16), nullable=False)
    name: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1", index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("season_id", "code", name="uq_leagues_season_code"),
    )


class LeagueParticipant(Base):
    __tablename__ = "league_participants"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("seasons.id", ondelete="CASCADE"), nullable=False, index=True)
    stage_id: Mapped[int] = mapped_column(ForeignKey("stages.id", ondelete="CASCADE"), nullable=False, index=True)
    league_id: Mapped[int] = mapped_column(ForeignKey("leagues.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    display_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1", index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("stage_id", "tg_user_id", name="uq_league_participants_stage_user"),
    )


class LeagueMovement(Base):
    __tablename__ = "league_movements"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    season_id: Mapped[int] = mapped_column(ForeignKey("seasons.id", ondelete="CASCADE"), nullable=False, index=True)
    from_stage_id: Mapped[int] = mapped_column(ForeignKey("stages.id", ondelete="CASCADE"), nullable=False, index=True)
    to_stage_id: Mapped[int] = mapped_column(ForeignKey("stages.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    from_league_id: Mapped[int] = mapped_column(ForeignKey("leagues.id", ondelete="CASCADE"), nullable=False, index=True)
    to_league_id: Mapped[int] = mapped_column(ForeignKey("leagues.id", ondelete="CASCADE"), nullable=False, index=True)
    reason: Mapped[str] = mapped_column(String(32), nullable=False, default="stage_transition", server_default="stage_transition")

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class Duel(Base):
    __tablename__ = "duels"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tournament_id: Mapped[int] = mapped_column(ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False, index=True)
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id", ondelete="CASCADE"), nullable=False, index=True)

    challenger_tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    opponent_tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    pair_low_tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    pair_high_tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    challenger_pred_home: Mapped[int] = mapped_column(Integer, nullable=False)
    challenger_pred_away: Mapped[int] = mapped_column(Integer, nullable=False)
    opponent_pred_home: Mapped[int | None] = mapped_column(Integer, nullable=True)
    opponent_pred_away: Mapped[int | None] = mapped_column(Integer, nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending", index=True)
    winner_tg_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    outcome: Mapped[str | None] = mapped_column(String(16), nullable=True)  # challenger_win | opponent_win | draw

    risk_multiplier_bp: Mapped[int] = mapped_column(Integer, nullable=False, default=100, server_default="100")
    elo_delta_challenger: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    elo_delta_opponent: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )
    responded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("match_id", "pair_low_tg_user_id", "pair_high_tg_user_id", name="uq_duels_match_pair"),
    )


class DuelElo(Base):
    __tablename__ = "duel_elo"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tournament_id: Mapped[int] = mapped_column(ForeignKey("tournaments.id", ondelete="CASCADE"), nullable=False, index=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    rating: Mapped[int] = mapped_column(Integer, nullable=False, default=1000, server_default="1000")
    duels_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    draws: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("tournament_id", "tg_user_id", name="uq_duel_elo_tournament_user"),
    )
