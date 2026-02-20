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

engine = create_async_engine(ASYNC_DATABASE_URL, echo=False, future=True)

SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _apply_postgres_schema_fixes(conn) -> None:
    """
    Мини-миграции без Alembic.

    Чиним created_at дефолты (чтобы Postgres не падал на NOT NULL)
    + добавляем поля для синхронизации матчей с внешним API (API-Sport.ru).
    """
    if not str(engine.url).startswith("postgresql+asyncpg://"):
        return

    statements = [
        # tournaments
        "CREATE TABLE IF NOT EXISTS tournaments (id SERIAL PRIMARY KEY, code VARCHAR(16) UNIQUE NOT NULL, name VARCHAR(64) NOT NULL, round_min INTEGER NOT NULL, round_max INTEGER NOT NULL, is_active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP NOT NULL DEFAULT NOW())",
        "CREATE INDEX IF NOT EXISTS ix_tournaments_code ON tournaments (code)",
        "INSERT INTO tournaments (code, name, round_min, round_max, is_active) VALUES ('RPL', 'Russian Premier League', 19, 30, 1) ON CONFLICT (code) DO NOTHING",
        "INSERT INTO tournaments (code, name, round_min, round_max, is_active) VALUES ('EPL', 'English Premier League', 27, 38, 1) ON CONFLICT (code) DO NOTHING",

        # user_tournaments
        "CREATE TABLE IF NOT EXISTS user_tournaments (id SERIAL PRIMARY KEY, tg_user_id BIGINT NOT NULL, tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE, created_at TIMESTAMP NOT NULL DEFAULT NOW(), CONSTRAINT uq_user_tournaments_user_tournament UNIQUE (tg_user_id, tournament_id))",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tg_user_id ON user_tournaments (tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tournament_id ON user_tournaments (tournament_id)",

        # users
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE users ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE users SET created_at = NOW() WHERE created_at IS NULL",

        # matches
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE matches ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE matches SET created_at = NOW() WHERE created_at IS NULL",
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS tournament_id INTEGER",
        "UPDATE matches SET tournament_id = (SELECT id FROM tournaments WHERE code = 'RPL' LIMIT 1) WHERE tournament_id IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_matches_tournament_id ON matches (tournament_id)",

        # внешний id матча из API (для API-Sport.ru используем match id)
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS api_fixture_id BIGINT",
        "CREATE INDEX IF NOT EXISTS ix_matches_api_fixture_id ON matches (api_fixture_id)",

        # источник матча: manual / apisport
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS source VARCHAR(16) NOT NULL DEFAULT 'manual'",
        "ALTER TABLE matches ALTER COLUMN source SET DEFAULT 'manual'",
        "UPDATE matches SET source = 'manual' WHERE source IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_matches_source ON matches (source)",

        # predictions
        "ALTER TABLE predictions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE predictions ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE predictions SET created_at = NOW() WHERE created_at IS NULL",

        # points
        "ALTER TABLE points ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE points ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE points SET created_at = NOW() WHERE created_at IS NULL",

        # settings (на всякий — не валимся, даже если create_all уже сделает)
        "CREATE TABLE IF NOT EXISTS settings (key VARCHAR(64) PRIMARY KEY, value VARCHAR(256) NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT NOW())",
        "INSERT INTO user_tournaments (tg_user_id, tournament_id) SELECT u.tg_user_id, t.id FROM users u CROSS JOIN tournaments t WHERE t.code = 'RPL' ON CONFLICT (tg_user_id, tournament_id) DO NOTHING",
    ]

    for sql in statements:
        try:
            await conn.execute(text(sql))
        except Exception as e:
            print("MIGRATION SKIP:", sql, "ERR:", repr(e))


async def _apply_sqlite_schema_fixes(conn) -> None:
    if not str(engine.url).startswith("sqlite+aiosqlite://"):
        return

    statements = [
        # tournaments
        "CREATE TABLE IF NOT EXISTS tournaments (id INTEGER PRIMARY KEY AUTOINCREMENT, code VARCHAR(16) UNIQUE NOT NULL, name VARCHAR(64) NOT NULL, round_min INTEGER NOT NULL, round_max INTEGER NOT NULL, is_active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE INDEX IF NOT EXISTS ix_tournaments_code ON tournaments (code)",
        "INSERT OR IGNORE INTO tournaments (code, name, round_min, round_max, is_active) VALUES ('RPL', 'Russian Premier League', 19, 30, 1)",
        "INSERT OR IGNORE INTO tournaments (code, name, round_min, round_max, is_active) VALUES ('EPL', 'English Premier League', 27, 38, 1)",

        # user_tournaments
        "CREATE TABLE IF NOT EXISTS user_tournaments (id INTEGER PRIMARY KEY AUTOINCREMENT, tg_user_id BIGINT NOT NULL, tournament_id INTEGER NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT uq_user_tournaments_user_tournament UNIQUE (tg_user_id, tournament_id))",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tg_user_id ON user_tournaments (tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tournament_id ON user_tournaments (tournament_id)",

        # matches.tournament_id
        "ALTER TABLE matches ADD COLUMN tournament_id INTEGER",
        "UPDATE matches SET tournament_id = (SELECT id FROM tournaments WHERE code = 'RPL' LIMIT 1) WHERE tournament_id IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_matches_tournament_id ON matches (tournament_id)",

        # backfill memberships for existing users into RPL
        "INSERT OR IGNORE INTO user_tournaments (tg_user_id, tournament_id) SELECT u.tg_user_id, t.id FROM users u CROSS JOIN tournaments t WHERE t.code = 'RPL'",
    ]

    for sql in statements:
        try:
            await conn.execute(text(sql))
        except Exception as e:
            print("MIGRATION SKIP:", sql, "ERR:", repr(e))


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _apply_postgres_schema_fixes(conn)
        await _apply_sqlite_schema_fixes(conn)
