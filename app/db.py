import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

from app.models import Base

load_dotenv()


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
        "CREATE TABLE IF NOT EXISTS tournaments (id SERIAL PRIMARY KEY, code VARCHAR(16) UNIQUE NOT NULL, name VARCHAR(64) NOT NULL, round_min INTEGER NOT NULL, round_max INTEGER NOT NULL, planned_matches_total INTEGER NOT NULL DEFAULT 0, is_active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP NOT NULL DEFAULT NOW())",
        "CREATE INDEX IF NOT EXISTS ix_tournaments_code ON tournaments (code)",
        "ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS planned_matches_total INTEGER NOT NULL DEFAULT 0",
        "INSERT INTO tournaments (code, name, round_min, round_max, is_active) VALUES ('RPL', 'РПЛ', 19, 30, 1) ON CONFLICT (code) DO NOTHING",
        "UPDATE tournaments SET name='РПЛ', round_min=19, round_max=30, is_active=1 WHERE code='RPL'",
        "UPDATE tournaments SET planned_matches_total = 104 WHERE code='WC2026'",
        "DELETE FROM tournaments WHERE code='EPL'",

        # user_tournaments
        "CREATE TABLE IF NOT EXISTS user_tournaments (id SERIAL PRIMARY KEY, tg_user_id BIGINT NOT NULL, tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE, display_name VARCHAR(64), created_at TIMESTAMP NOT NULL DEFAULT NOW(), CONSTRAINT uq_user_tournaments_user_tournament UNIQUE (tg_user_id, tournament_id))",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tg_user_id ON user_tournaments (tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tournament_id ON user_tournaments (tournament_id)",
        "ALTER TABLE user_tournaments ADD COLUMN IF NOT EXISTS display_name VARCHAR(64)",
        "ALTER TABLE user_tournaments ADD COLUMN IF NOT EXISTS bonus_points INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE user_tournaments ADD COLUMN IF NOT EXISTS bonus_winner INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE user_tournaments ADD COLUMN IF NOT EXISTS bonus_scorer INTEGER NOT NULL DEFAULT 0",

        # users
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name VARCHAR(64)",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_url VARCHAR(512)",
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
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS group_label VARCHAR(32)",
        "CREATE INDEX IF NOT EXISTS ix_matches_group_label ON matches (group_label)",
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS is_placeholder INTEGER NOT NULL DEFAULT 0",
        "UPDATE matches SET is_placeholder = 0 WHERE is_placeholder IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_matches_is_placeholder ON matches (is_placeholder)",

        # источник матча: manual / apisport
        "ALTER TABLE matches ADD COLUMN IF NOT EXISTS source VARCHAR(16) NOT NULL DEFAULT 'manual'",
        "ALTER TABLE matches ALTER COLUMN source SET DEFAULT 'manual'",
        "UPDATE matches SET source = 'manual' WHERE source IS NULL",
        "CREATE INDEX IF NOT EXISTS ix_matches_source ON matches (source)",

        # predictions
        "ALTER TABLE predictions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE predictions ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE predictions SET created_at = NOW() WHERE created_at IS NULL",
        "ALTER TABLE predictions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE predictions ALTER COLUMN updated_at SET DEFAULT NOW()",
        "UPDATE predictions SET updated_at = created_at WHERE updated_at IS NULL",

        # points
        "ALTER TABLE points ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW()",
        "ALTER TABLE points ALTER COLUMN created_at SET DEFAULT NOW()",
        "UPDATE points SET created_at = NOW() WHERE created_at IS NULL",

        # settings (на всякий — не валимся, даже если create_all уже сделает)
        "CREATE TABLE IF NOT EXISTS settings (key VARCHAR(64) PRIMARY KEY, value VARCHAR(256) NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT NOW())",
        # duels
        "CREATE TABLE IF NOT EXISTS duels (id SERIAL PRIMARY KEY, tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE, match_id INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE, challenger_tg_user_id BIGINT NOT NULL, opponent_tg_user_id BIGINT NOT NULL, pair_low_tg_user_id BIGINT NOT NULL, pair_high_tg_user_id BIGINT NOT NULL, challenger_pred_home INTEGER NOT NULL, challenger_pred_away INTEGER NOT NULL, opponent_pred_home INTEGER, opponent_pred_away INTEGER, status VARCHAR(16) NOT NULL DEFAULT 'pending', winner_tg_user_id BIGINT, outcome VARCHAR(16), risk_multiplier_bp INTEGER NOT NULL DEFAULT 100, elo_delta_challenger INTEGER NOT NULL DEFAULT 0, elo_delta_opponent INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT NOW(), responded_at TIMESTAMP NULL, resolved_at TIMESTAMP NULL, CONSTRAINT uq_duels_match_pair UNIQUE (match_id, pair_low_tg_user_id, pair_high_tg_user_id))",
        "CREATE INDEX IF NOT EXISTS ix_duels_tournament_id ON duels (tournament_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_match_id ON duels (match_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_challenger_tg_user_id ON duels (challenger_tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_opponent_tg_user_id ON duels (opponent_tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_status ON duels (status)",
        "CREATE INDEX IF NOT EXISTS ix_duels_winner_tg_user_id ON duels (winner_tg_user_id)",

        # duel elo
        "CREATE TABLE IF NOT EXISTS duel_elo (id SERIAL PRIMARY KEY, tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE, tg_user_id BIGINT NOT NULL, rating INTEGER NOT NULL DEFAULT 1000, duels_total INTEGER NOT NULL DEFAULT 0, wins INTEGER NOT NULL DEFAULT 0, losses INTEGER NOT NULL DEFAULT 0, draws INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW(), CONSTRAINT uq_duel_elo_tournament_user UNIQUE (tournament_id, tg_user_id))",
        "CREATE INDEX IF NOT EXISTS ix_duel_elo_tournament_id ON duel_elo (tournament_id)",
        "CREATE INDEX IF NOT EXISTS ix_duel_elo_tg_user_id ON duel_elo (tg_user_id)",

        # manual-only режим: удаляем API-матчи
        "DELETE FROM points WHERE match_id IN (SELECT id FROM matches WHERE source <> 'manual' OR api_fixture_id IS NOT NULL)",
        "DELETE FROM predictions WHERE match_id IN (SELECT id FROM matches WHERE source <> 'manual' OR api_fixture_id IS NOT NULL)",
        "DELETE FROM matches WHERE source <> 'manual' OR api_fixture_id IS NOT NULL",
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
        "CREATE TABLE IF NOT EXISTS tournaments (id INTEGER PRIMARY KEY AUTOINCREMENT, code VARCHAR(16) UNIQUE NOT NULL, name VARCHAR(64) NOT NULL, round_min INTEGER NOT NULL, round_max INTEGER NOT NULL, planned_matches_total INTEGER NOT NULL DEFAULT 0, is_active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE INDEX IF NOT EXISTS ix_tournaments_code ON tournaments (code)",
        "ALTER TABLE tournaments ADD COLUMN planned_matches_total INTEGER NOT NULL DEFAULT 0",
        "INSERT OR IGNORE INTO tournaments (code, name, round_min, round_max, is_active) VALUES ('RPL', 'РПЛ', 19, 30, 1)",
        "UPDATE tournaments SET name='РПЛ', round_min=19, round_max=30, is_active=1 WHERE code='RPL'",
        "UPDATE tournaments SET planned_matches_total = 104 WHERE code='WC2026'",
        "DELETE FROM tournaments WHERE code='EPL'",

        # user_tournaments
        "CREATE TABLE IF NOT EXISTS user_tournaments (id INTEGER PRIMARY KEY AUTOINCREMENT, tg_user_id BIGINT NOT NULL, tournament_id INTEGER NOT NULL, display_name VARCHAR(64), created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT uq_user_tournaments_user_tournament UNIQUE (tg_user_id, tournament_id))",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tg_user_id ON user_tournaments (tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_user_tournaments_tournament_id ON user_tournaments (tournament_id)",
        "ALTER TABLE user_tournaments ADD COLUMN display_name VARCHAR(64)",
        "ALTER TABLE user_tournaments ADD COLUMN bonus_points INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE user_tournaments ADD COLUMN bonus_winner INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE user_tournaments ADD COLUMN bonus_scorer INTEGER NOT NULL DEFAULT 0",

        # matches.tournament_id
        "ALTER TABLE matches ADD COLUMN tournament_id INTEGER",
        "ALTER TABLE matches ADD COLUMN group_label VARCHAR(32)",
        "ALTER TABLE matches ADD COLUMN is_placeholder INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE users ADD COLUMN display_name VARCHAR(64)",
        "ALTER TABLE users ADD COLUMN photo_url VARCHAR(512)",
        "UPDATE matches SET tournament_id = (SELECT id FROM tournaments WHERE code = 'RPL' LIMIT 1) WHERE tournament_id IS NULL",
        "UPDATE matches SET is_placeholder = COALESCE(is_placeholder, 0)",
        "CREATE INDEX IF NOT EXISTS ix_matches_tournament_id ON matches (tournament_id)",
        "CREATE INDEX IF NOT EXISTS ix_matches_group_label ON matches (group_label)",
        "CREATE INDEX IF NOT EXISTS ix_matches_is_placeholder ON matches (is_placeholder)",

        # duels
        "CREATE TABLE IF NOT EXISTS duels (id INTEGER PRIMARY KEY AUTOINCREMENT, tournament_id INTEGER NOT NULL, match_id INTEGER NOT NULL, challenger_tg_user_id BIGINT NOT NULL, opponent_tg_user_id BIGINT NOT NULL, pair_low_tg_user_id BIGINT NOT NULL, pair_high_tg_user_id BIGINT NOT NULL, challenger_pred_home INTEGER NOT NULL, challenger_pred_away INTEGER NOT NULL, opponent_pred_home INTEGER, opponent_pred_away INTEGER, status VARCHAR(16) NOT NULL DEFAULT 'pending', winner_tg_user_id BIGINT, outcome VARCHAR(16), risk_multiplier_bp INTEGER NOT NULL DEFAULT 100, elo_delta_challenger INTEGER NOT NULL DEFAULT 0, elo_delta_opponent INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, responded_at TIMESTAMP NULL, resolved_at TIMESTAMP NULL, CONSTRAINT uq_duels_match_pair UNIQUE (match_id, pair_low_tg_user_id, pair_high_tg_user_id))",
        "CREATE INDEX IF NOT EXISTS ix_duels_tournament_id ON duels (tournament_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_match_id ON duels (match_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_challenger_tg_user_id ON duels (challenger_tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_opponent_tg_user_id ON duels (opponent_tg_user_id)",
        "CREATE INDEX IF NOT EXISTS ix_duels_status ON duels (status)",
        "CREATE INDEX IF NOT EXISTS ix_duels_winner_tg_user_id ON duels (winner_tg_user_id)",
        # duel elo
        "CREATE TABLE IF NOT EXISTS duel_elo (id INTEGER PRIMARY KEY AUTOINCREMENT, tournament_id INTEGER NOT NULL, tg_user_id BIGINT NOT NULL, rating INTEGER NOT NULL DEFAULT 1000, duels_total INTEGER NOT NULL DEFAULT 0, wins INTEGER NOT NULL DEFAULT 0, losses INTEGER NOT NULL DEFAULT 0, draws INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT uq_duel_elo_tournament_user UNIQUE (tournament_id, tg_user_id))",
        "CREATE INDEX IF NOT EXISTS ix_duel_elo_tournament_id ON duel_elo (tournament_id)",
        "CREATE INDEX IF NOT EXISTS ix_duel_elo_tg_user_id ON duel_elo (tg_user_id)",
        # predictions.updated_at
        "ALTER TABLE predictions ADD COLUMN updated_at TIMESTAMP",
        "UPDATE predictions SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)",
        # manual-only режим: удаляем API-матчи
        "DELETE FROM points WHERE match_id IN (SELECT id FROM matches WHERE source <> 'manual' OR api_fixture_id IS NOT NULL)",
        "DELETE FROM predictions WHERE match_id IN (SELECT id FROM matches WHERE source <> 'manual' OR api_fixture_id IS NOT NULL)",
        "DELETE FROM matches WHERE source <> 'manual' OR api_fixture_id IS NOT NULL",
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
