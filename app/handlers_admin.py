from __future__ import annotations

from datetime import datetime, timezone
import os

from aiogram import Dispatcher, types
from aiogram.filters import Command

from sqlalchemy import select, func

from app.config import load_admin_ids
from app.db import SessionLocal
from app.models import Match, Prediction, Point, User
from app.scoring import calculate_points
from app.rpl_api import fetch_rpl_round  # <-- используем удобный wrapper

ADMIN_IDS = load_admin_ids()


async def recalc_points_for_match(match_id: int) -> int:
    updates = 0

    async with SessionLocal() as session:
        res_match = await session.execute(select(Match).where(Match.id == match_id))
        match = res_match.scalar_one_or_none()
        if match is None:
            return 0

        if match.home_score is None or match.away_score is None:
            return 0

        res_preds = await session.execute(select(Prediction).where(Prediction.match_id == match_id))
        preds = res_preds.scalars().all()

        for p in preds:
            calc = calculate_points(p.pred_home, p.pred_away, match.home_score, match.away_score)

            res_point = await session.execute(
                select(Point).where(Point.match_id == match_id, Point.tg_user_id == p.tg_user_id)
            )
            point = res_point.scalar_one_or_none()

            if point is None:
                session.add(
                    Point(
                        match_id=match_id,
                        tg_user_id=p.tg_user_id,
                        points=calc.points,
                        category=calc.category,
                    )
                )
            else:
                point.points = calc.points
                point.category = calc.category

            updates += 1

        await session.commit()

    return updates


def _db_mode_text() -> str:
    if os.getenv("DATABASE_URL"):
        return "Postgres (DATABASE_URL)"
    return "SQLite fallback (⚠️ так быть не должно на Render)"


def register_admin_handlers(dp: Dispatcher) -> None:
    @dp.message(Command("admin_add_match"))
    async def cmd_admin_add_match(message: types.Message):
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔️ У вас нет прав на эту команду.")
            return

        raw = message.text.replace("/admin_add_match", "", 1).strip()

        if "|" not in raw:
            await message.answer(
                "Неверный формат.\n"
                "Пример:\n"
                "/admin_add_match 1 | Zenit | Spartak | 2026-03-01 18:30"
            )
            return

        parts = [p.strip() for p in raw.split("|")]
        if len(parts) != 4:
            await message.answer(
                "Неверный формат. Нужно 4 части через | \n"
                "Пример:\n"
                "/admin_add_match 1 | Zenit | Spartak | 2026-03-01 18:30"
            )
            return

        round_str, home_team, away_team, dt_str = parts

        try:
            round_number = int(round_str)
        except ValueError:
            await message.answer("Тур должен быть числом. Пример: 1")
            return

        try:
            kickoff_time = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError:
            await message.answer("Дата/время должны быть в формате YYYY-MM-DD HH:MM (например 2026-03-01 18:30)")
            return

        async with SessionLocal() as session:
            session.add(
                Match(
                    round_number=round_number,
                    home_team=home_team,
                    away_team=away_team,
                    kickoff_time=kickoff_time,
                )
            )
            await session.commit()

        await message.answer(
            f"✅ Матч добавлен:\n"
            f"Тур {round_number}: {home_team} — {away_team}\n"
            f"Начало: {kickoff_time.strftime('%Y-%m-%d %H:%M')}"
        )

    @dp.message(Command("admin_set_result"))
    async def cmd_admin_set_result(message: types.Message):
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔️ У вас нет прав на эту команду.")
            return

        parts = message.text.strip().split()
        if len(parts) != 3:
            await message.answer("Неверный формат. Пример: /admin_set_result 1 2:1")
            return

        try:
            match_id = int(parts[1])
        except ValueError:
            await message.answer("match_id должен быть числом. Пример: /admin_set_result 1 2:1")
            return

        score_str = parts[2].strip().replace("-", ":")
        if ":" not in score_str:
            await message.answer("Счёт должен быть в формате 2:1")
            return

        try:
            home_s, away_s = score_str.split(":")
            home_score = int(home_s)
            away_score = int(away_s)
        except ValueError:
            await message.answer("Счёт должен быть числом, пример: 2:1")
            return

        async with SessionLocal() as session:
            result = await session.execute(select(Match).where(Match.id == match_id))
            match = result.scalar_one_or_none()

            if match is None:
                await message.answer(f"Матч с id={match_id} не найден.")
                return

            match.home_score = home_score
            match.away_score = away_score
            await session.commit()

        updates = await recalc_points_for_match(match_id)

        await message.answer(
            f"✅ Результат сохранён для матча #{match_id}: {home_score}:{away_score}\n"
            f"🧮 Начислений пересчитано: {updates}"
        )

    @dp.message(Command("admin_recalc"))
    async def cmd_admin_recalc(message: types.Message):
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔️ У вас нет прав на эту команду.")
            return

        updates = 0

        async with SessionLocal() as session:
            res_matches = await session.execute(
                select(Match).where(Match.home_score.is_not(None), Match.away_score.is_not(None))
            )
            matches = res_matches.scalars().all()

            for m in matches:
                res_preds = await session.execute(select(Prediction).where(Prediction.match_id == m.id))
                preds = res_preds.scalars().all()

                for p in preds:
                    calc = calculate_points(p.pred_home, p.pred_away, m.home_score, m.away_score)

                    res_point = await session.execute(
                        select(Point).where(Point.match_id == m.id, Point.tg_user_id == p.tg_user_id)
                    )
                    point = res_point.scalar_one_or_none()

                    if point is None:
                        session.add(
                            Point(
                                match_id=m.id,
                                tg_user_id=p.tg_user_id,
                                points=calc.points,
                                category=calc.category,
                            )
                        )
                    else:
                        point.points = calc.points
                        point.category = calc.category

                    updates += 1

            await session.commit()

        await message.answer(f"✅ Пересчитано начислений: {updates}")

    @dp.message(Command("admin_health"))
    async def cmd_admin_health(message: types.Message):
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔️ У вас нет прав на эту команду.")
            return

        async with SessionLocal() as session:
            users_cnt = int((await session.execute(select(func.count(User.id)))).scalar_one() or 0)
            matches_cnt = int((await session.execute(select(func.count(Match.id)))).scalar_one() or 0)
            preds_cnt = int((await session.execute(select(func.count(Prediction.id)))).scalar_one() or 0)
            points_cnt = int((await session.execute(select(func.count(Point.id)))).scalar_one() or 0)

            played_cnt = int(
                (await session.execute(
                    select(func.count(Match.id)).where(Match.home_score.is_not(None), Match.away_score.is_not(None))
                )).scalar_one() or 0
            )

            active_users_cnt = int(
                (await session.execute(select(func.count(func.distinct(Prediction.tg_user_id))))).scalar_one() or 0
            )

        text = (
            "🩺 admin_health\n"
            f"DB: {_db_mode_text()}\n"
            f"users (registered): {users_cnt}\n"
            f"users (active): {active_users_cnt}\n"
            f"matches: {matches_cnt}\n"
            f"played matches: {played_cnt}\n"
            f"predictions: {preds_cnt}\n"
            f"points: {points_cnt}"
        )
        await message.answer(text)

    @dp.message(Command("admin_api_debug"))
    async def cmd_admin_api_debug(message: types.Message):
        """
        Показывает, какой season/year и какие названия туров реально видит API.
        """
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔️ У вас нет прав на эту команду.")
            return

        try:
            _, dbg = await fetch_rpl_round(1)
        except Exception:
            await message.answer("⚠️ Ошибка обращения к API. Смотри логи Render.")
            raise

        text = (
            "🔎 API debug\n"
            f"league_id: {dbg.get('league_id')}\n"
            f"season_year: {dbg.get('season_year')}\n"
            f"rounds_count: {dbg.get('rounds_count')}\n"
            f"target_round_for_1: {dbg.get('target_round')}\n"
            f"rounds_head: {dbg.get('rounds_head')}\n"
            f"rounds_tail: {dbg.get('rounds_tail')}\n"
        )
        await message.answer(text)

    @dp.message(Command("admin_sync_round"))
    async def cmd_admin_sync_round(message: types.Message):
        """
        /admin_sync_round N
        Подтягивает матчи тура N из API-Football и upsert'ит в matches по api_fixture_id.
        """
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔️ У вас нет прав на эту команду.")
            return

        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer("Неверный формат. Пример: /admin_sync_round 1")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer("N должен быть числом. Пример: /admin_sync_round 1")
            return

        try:
            fixtures, dbg = await fetch_rpl_round(round_number)
        except Exception:
            await message.answer("⚠️ Не смог получить матчи из API. Детали смотри в логах Render.")
            raise

        if not fixtures:
            await message.answer(
                "Матчи тура не найдены (API вернул пусто).\n"
                f"season_year: {dbg.get('season_year')}\n"
                f"target_round: {dbg.get('target_round')}\n"
                f"rounds_head: {dbg.get('rounds_head')}\n"
                "Сделай /admin_api_debug и пришли сюда ответ — скажу точно, что не так."
            )
            return

        created = 0
        updated = 0

        async with SessionLocal() as session:
            for fx in fixtures:
                res = await session.execute(select(Match).where(Match.api_fixture_id == fx.api_fixture_id))
                existing = res.scalar_one_or_none()

                kickoff_utc_naive = fx.start_time_utc.astimezone(timezone.utc).replace(tzinfo=None)

                if existing is None:
                    created += 1
                    session.add(
                        Match(
                            round_number=round_number,
                            home_team=fx.home_team,
                            away_team=fx.away_team,
                            kickoff_time=kickoff_utc_naive,
                            api_fixture_id=fx.api_fixture_id,
                        )
                    )
                else:
                    updated += 1
                    existing.round_number = round_number
                    existing.home_team = fx.home_team
                    existing.away_team = fx.away_team
                    existing.kickoff_time = kickoff_utc_naive

            await session.commit()

        await message.answer(
            "✅ Синхронизация завершена.\n"
            f"Тур: {round_number}\n"
            f"API round: {fixtures[0].round_str}\n"
            f"Матчей из API: {len(fixtures)}\n"
            f"Создано: {created}\n"
            f"Обновлено: {updated}"
        )