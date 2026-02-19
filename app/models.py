from datetime import datetime

from sqlalchemy import BigInteger, String, DateTime, Integer
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
    """
    Матч РПЛ (пока упрощённо).
    Позже добавим match_id из API, статусы, туры и т.д.
    """
    __tablename__ = "matches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Номер тура (1,2,3...)
    round_number: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

    home_team: Mapped[str] = mapped_column(String(80), nullable=False)
    away_team: Mapped[str] = mapped_column(String(80), nullable=False)

    kickoff_time: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)

    # Результат (пока может быть пустым, пока матч не сыгран)
    home_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    away_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # created_at для отладки/истории
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)