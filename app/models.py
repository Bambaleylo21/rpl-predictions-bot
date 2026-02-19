from datetime import datetime

from sqlalchemy import BigInteger, String, DateTime, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True, nullable=False)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Match(Base):
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    round_number: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    home_team: Mapped[str] = mapped_column(String(80), nullable=False)
    away_team: Mapped[str] = mapped_column(String(80), nullable=False)
    kickoff_time: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)

    home_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    away_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Связь с матчем
    match_id: Mapped[int] = mapped_column(ForeignKey("matches.id"), index=True, nullable=False)

    # Telegram user id (храним прямо его, так проще для бота)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)

    pred_home: Mapped[int] = mapped_column(Integer, nullable=False)
    pred_away: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    # Один пользователь может сделать только один прогноз на один матч
    __table_args__ = (
        UniqueConstraint("match_id", "tg_user_id", name="uq_prediction_match_user"),
    )