import os
import ssl

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Base


def _make_async_db_url(raw_url: str) -> str:
    url = raw_url.strip()

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return url


DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    ASYNC_DB_URL = _make_async_db_url(DATABASE_URL)

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    engine = create_async_engine(
        ASYNC_DB_URL,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": ssl_ctx},
    )
else:
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
    1) создаём таблицы, если их нет
    2) добавляем колонку users.full_name (для старых БД)
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Postgres: добавим колонку, если её не было раньше
        if DATABASE_URL:
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name VARCHAR(128);"))