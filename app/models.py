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

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        server_default=func.now(),
    )


class Match(Base):
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    round_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    home_team: Mapped[str] = mapped_column(String(64), nullable=False)
    away_team: Mapped[str] = mapped_column(String(64), nullable=False)

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