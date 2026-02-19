import os

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Base  # Base и модели уже у тебя в app/models.py


def _make_async_db_url(raw_url: str) -> str:
    """
    Render обычно даёт DATABASE_URL вида:
    - postgres://...
    - postgresql://...

    Для SQLAlchemy async нужен драйвер asyncpg:
    - postgresql+asyncpg://...
    """
    url = raw_url.strip()

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return url


DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    ASYNC_DB_URL = _make_async_db_url(DATABASE_URL)

    # Для Render по External URL обычно нужен SSL.
    # asyncpg принимает ssl=True (использует безопасное соединение).
    engine = create_async_engine(
        ASYNC_DB_URL,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": True},
    )
else:
    # fallback на локальный sqlite (на всякий случай)
    # В Render использовать не надо — там и была проблема "пропадают матчи".
    engine = create_async_engine(
        "sqlite+aiosqlite:///./bot.db",
        echo=False,
    )

SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


async def init_db() -> None:
    """
    Создаёт таблицы в базе, если их ещё нет.
    ВАЖНО: после перехода на Postgres это нужно выполнить хотя бы 1 раз.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)