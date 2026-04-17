from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import League, LeagueParticipant, Season, Setting, Stage, Tournament, User, UserTournament

DEFAULT_SEASON_NAME = "РПЛ 2026/27"
DEFAULT_STAGE_1_NAME = "Осенний этап"
DEFAULT_STAGE_2_NAME = "Весенний этап"
ENROLL_OPEN_KEY = "SEASON_ENROLL_OPEN"


@dataclass
class FoundationSummary:
    season_id: int
    season_name: str
    stage1_id: int
    stage2_id: int
    high_league_id: int
    low_league_id: int


async def _get_or_create_setting(session: AsyncSession, key: str, default_value: str) -> Setting:
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is None:
        row = Setting(key=key, value=default_value)
        session.add(row)
    return row


async def set_enrollment_open(session: AsyncSession, is_open: bool) -> None:
    obj = await _get_or_create_setting(session, ENROLL_OPEN_KEY, "0")
    obj.value = "1" if is_open else "0"


async def is_enrollment_open(session: AsyncSession) -> bool:
    q = await session.execute(select(Setting).where(Setting.key == ENROLL_OPEN_KEY))
    row = q.scalar_one_or_none()
    return bool(row and row.value == "1")


async def get_active_season(session: AsyncSession) -> Season | None:
    q = await session.execute(select(Season).where(Season.is_active == 1).order_by(Season.id.desc()))
    return q.scalars().first()


async def get_active_stage(session: AsyncSession, season_id: int) -> Stage | None:
    q = await session.execute(
        select(Stage)
        .where(Stage.season_id == season_id, Stage.is_active == 1)
        .order_by(Stage.stage_order.asc())
    )
    return q.scalars().first()


async def ensure_rpl_tournament(session: AsyncSession) -> Tournament:
    q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
    t = q.scalar_one_or_none()
    if t is None:
        t = Tournament(code="RPL", name="РПЛ", round_min=1, round_max=30, is_active=1)
        session.add(t)
        await session.flush()
    return t


async def setup_new_season_foundation(
    session: AsyncSession,
    season_name: str,
    stage_1_round_min: int = 1,
    stage_1_round_max: int = 17,
    stage_2_round_min: int = 18,
    stage_2_round_max: int = 30,
) -> FoundationSummary:
    # Выключаем старые сезоны/этапы.
    await session.execute(delete(LeagueMovement))
    await session.execute(delete(LeagueParticipant))
    await session.execute(delete(League))
    await session.execute(delete(Stage))
    await session.execute(delete(Season))

    # Оставляем только один активный сезон с двумя этапами.
    season = Season(name=season_name, is_active=1)
    session.add(season)
    await session.flush()

    stage1 = Stage(
        season_id=season.id,
        name=DEFAULT_STAGE_1_NAME,
        stage_order=1,
        round_min=stage_1_round_min,
        round_max=stage_1_round_max,
        is_active=1,
        is_completed=0,
        promote_count=2,
        relegate_count=2,
    )
    stage2 = Stage(
        season_id=season.id,
        name=DEFAULT_STAGE_2_NAME,
        stage_order=2,
        round_min=stage_2_round_min,
        round_max=stage_2_round_max,
        is_active=0,
        is_completed=0,
        promote_count=2,
        relegate_count=2,
    )
    session.add_all([stage1, stage2])
    await session.flush()

    high = League(season_id=season.id, code="HIGH", name="Высшая лига", is_active=1)
    low = League(season_id=season.id, code="LOW", name="Низшая лига", is_active=1)
    session.add_all([high, low])
    await session.flush()

    # Синхронизируем окно RPL для совместимости с текущей логикой матчей.
    rpl = await ensure_rpl_tournament(session)
    rpl.name = "РПЛ"
    rpl.round_min = stage_1_round_min
    rpl.round_max = stage_2_round_max
    rpl.is_active = 1

    await set_enrollment_open(session, False)

    return FoundationSummary(
        season_id=int(season.id),
        season_name=str(season.name),
        stage1_id=int(stage1.id),
        stage2_id=int(stage2.id),
        high_league_id=int(high.id),
        low_league_id=int(low.id),
    )


async def set_active_season_name(session: AsyncSession, season_name: str) -> Season | None:
    season = await get_active_season(session)
    if season is None:
        return None
    season.name = season_name
    return season


async def assign_user_to_active_stage_league(
    session: AsyncSession,
    tg_user_id: int,
    league_code: str,
) -> tuple[str, str] | None:
    season = await get_active_season(session)
    if season is None:
        return None

    stage = await get_active_stage(session, season.id)
    if stage is None:
        return None

    league_q = await session.execute(
        select(League).where(League.season_id == season.id, League.code == league_code.upper(), League.is_active == 1)
    )
    league = league_q.scalar_one_or_none()
    if league is None:
        return None

    user_q = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
    user = user_q.scalar_one_or_none()

    rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
    rpl = rpl_q.scalar_one_or_none()

    display_name: str | None = None
    if rpl is not None:
        ut_q = await session.execute(
            select(UserTournament).where(UserTournament.tg_user_id == tg_user_id, UserTournament.tournament_id == rpl.id)
        )
        ut = ut_q.scalar_one_or_none()
        if ut is None:
            ut = UserTournament(tg_user_id=tg_user_id, tournament_id=rpl.id, display_name=None)
            session.add(ut)
            await session.flush()
        display_name = ut.display_name

    if not display_name and user is not None:
        display_name = user.display_name or user.full_name or (f"@{user.username}" if user.username else None)

    if not display_name:
        display_name = str(tg_user_id)

    row_q = await session.execute(
        select(LeagueParticipant).where(LeagueParticipant.stage_id == stage.id, LeagueParticipant.tg_user_id == tg_user_id)
    )
    row = row_q.scalar_one_or_none()
    if row is None:
        row = LeagueParticipant(
            season_id=season.id,
            stage_id=stage.id,
            league_id=league.id,
            tg_user_id=tg_user_id,
            display_name=display_name,
            is_active=1,
        )
        session.add(row)
    else:
        row.league_id = league.id
        row.display_name = display_name
        row.is_active = 1

    return display_name, league.name


# Import placed at end to avoid circular import in type checking/runtime ordering.
from app.models import LeagueMovement  # noqa: E402
