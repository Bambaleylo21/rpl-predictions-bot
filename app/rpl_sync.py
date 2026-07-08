from __future__ import annotations

import asyncio
import logging
import os

from sqlalchemy import select

from app.db import SessionLocal
from app.duel_notify import send_duel_finished_pushes
from app.duels import finalize_duels_for_match
from app.football_api import fetch_league_fixtures
from app.models import Match, Tournament

logger = logging.getLogger(__name__)

RPL_LEAGUE_ID = int(os.getenv("FOOTBALL_RPL_LEAGUE_ID", "235"))
RPL_SEASON = int(os.getenv("FOOTBALL_RPL_SEASON", "2026"))
SYNC_INTERVAL_SEC = int(os.getenv("FOOTBALL_SYNC_INTERVAL_SEC", "600"))


async def _get_rpl_tournament(session) -> Tournament | None:
    q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
    return q.scalar_one_or_none()


async def sync_rpl_once(bot, session_factory=SessionLocal) -> dict:
    """
    Тянет расписание/результаты РПЛ из API-Football (лига FOOTBALL_RPL_LEAGUE_ID,
    сезон FOOTBALL_RPL_SEASON) и приводит таблицу matches в актуальное состояние:
      - создаёт новые матчи (source="apisport"), если их ещё нет;
      - обновляет расписание (тур/время/команды) для ранее созданных auto-матчей;
      - как только матч завершился по данным API — сохраняет счёт и запускает
        тот же пересчёт очков/дуэлей/ачивок, что и ручная команда /admin_set_result.
    Матчи с source="manual" (добавленные админом руками) не трогает.
    """
    stats = {
        "fetched": 0,
        "created": 0,
        "updated_schedule": 0,
        "results_applied": 0,
        "skipped_no_round": 0,
    }

    fixtures = await fetch_league_fixtures(RPL_LEAGUE_ID, RPL_SEASON)
    stats["fetched"] = len(fixtures)
    if not fixtures:
        return stats

    async with session_factory() as session:
        tournament = await _get_rpl_tournament(session)
        if tournament is None:
            logger.warning("[rpl_sync] RPL tournament not found in DB, skipping sync")
            return stats

        for fx in fixtures:
            if fx.round_number is None:
                stats["skipped_no_round"] += 1
                continue

            q = await session.execute(
                select(Match).where(
                    Match.tournament_id == tournament.id,
                    Match.api_fixture_id == fx.fixture_id,
                )
            )
            match = q.scalar_one_or_none()

            if match is None:
                match = Match(
                    tournament_id=tournament.id,
                    round_number=fx.round_number,
                    home_team=fx.home_team,
                    away_team=fx.away_team,
                    kickoff_time=fx.kickoff_msk,
                    source="apisport",
                    api_fixture_id=fx.fixture_id,
                    is_placeholder=0,
                )
                session.add(match)
                await session.flush()
                stats["created"] += 1
            elif match.source == "apisport":
                changed = False
                if match.round_number != fx.round_number:
                    match.round_number = fx.round_number
                    changed = True
                if match.home_team != fx.home_team:
                    match.home_team = fx.home_team
                    changed = True
                if match.away_team != fx.away_team:
                    match.away_team = fx.away_team
                    changed = True
                if match.kickoff_time != fx.kickoff_msk:
                    match.kickoff_time = fx.kickoff_msk
                    changed = True
                if changed:
                    stats["updated_schedule"] += 1
            else:
                # Матч когда-то добавлен вручную админом — не переписываем его.
                continue

            await session.commit()

            if (
                fx.is_finished
                and fx.home_score is not None
                and fx.away_score is not None
                and (match.home_score != fx.home_score or match.away_score != fx.away_score)
            ):
                match.home_score = fx.home_score
                match.away_score = fx.away_score
                await session.commit()

                # Локальный импорт: recalc_points_for_match_in_session определена
                # прямо в handlers_admin.py, тянуть весь модуль на старте не нужно.
                from app.handlers_admin import recalc_points_for_match_in_session

                await recalc_points_for_match_in_session(session, match.id)
                duel_events = await finalize_duels_for_match(session, int(match.id))
                if duel_events and bot is not None:
                    await send_duel_finished_pushes(bot, session, events=duel_events)
                if bot is not None:
                    from app.miniapp_api import send_new_achievement_pushes

                    await send_new_achievement_pushes(bot, session, tournament_id=int(tournament.id))
                await session.commit()
                stats["results_applied"] += 1

    return stats


async def run_rpl_sync_loop(bot, session_factory=SessionLocal) -> None:
    logger.info(
        "[rpl_sync] loop started, league=%s season=%s interval=%ss",
        RPL_LEAGUE_ID,
        RPL_SEASON,
        SYNC_INTERVAL_SEC,
    )
    while True:
        try:
            stats = await sync_rpl_once(bot, session_factory)
            if stats.get("fetched"):
                logger.info("[rpl_sync] %s", stats)
        except Exception:
            logger.exception("[rpl_sync] loop iteration failed")

        await asyncio.sleep(SYNC_INTERVAL_SEC)
