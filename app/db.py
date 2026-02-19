import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.models import Base


def _make_async_db_url(url: str) -> str:
    # Render часто даёт DATABASE_URL вида postgresql://...
    # SQLAlchemy asyncpg хочет postgresql+asyncpg://...
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    return url


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db")
ASYNC_DATABASE_URL = _make_async_db_url(DATABASE_URL)

# Если ты добавлял sslmode=require — оно обычно уже в DATABASE_URL.
engine = create_async_engine(ASYNC_DATABASE_URL, echo=False, future=True)

SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _apply_postgres_schema_fixes(conn) -> None:
    """
    Мини-миграции без Alembic.

    Чиним created_at дефолты (чтобы Postgres не падал на NOT NULL)
    + добавляем поле api_fixture_id для синхронизации матчей с внешним API.
    """
    if not str(engine.url).startswith("postgresql+asyncpg://"):
        return

    statements = [
        # users
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE users ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE users SET created_at = NOW() WHERE created_at IS NULL",

        # matches
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE matches ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE matches SET created_at = NOW() WHERE created_at IS NULL",

        # NEW: внешний id матча из API (API-Football fixture id)
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS api_fixture_id BIGINT",
        "CREATE INDEX IF NOT EXISTS ix_matches_api_fixture_id ON matches (api_fixture_id)",

        # predictions (ВАЖНО)
        "ALTER TABLE predictions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE predictions ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE predictions SET created_at = NOW() WHERE created_at IS NULL",

        # points
        "ALTER TABLE points ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE points ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE points SET created_at = NOW() WHERE created_at IS NULL",
    ]

    for sql in statements:
        try:
            await conn.execute(text(sql))
        except Exception as e:
            # не валим бот, если какая-то команда не применима
            print("MIGRATION SKIP:", sql, "ERR:", repr(e))


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _apply_postgres_schema_fixes(conn)