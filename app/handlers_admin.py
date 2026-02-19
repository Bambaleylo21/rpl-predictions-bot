from datetime import datetime

from aiogram import Dispatcher, types
from aiogram.filters import Command

from sqlalchemy import select

from app.config import load_admin_ids
from app.db import SessionLocal
from app.models import Match, Prediction, Point
from app.scoring import calculate_points

ADMIN_IDS = load_admin_ids()


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

        score_str = parts[2].strip()
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

        await message.answer(f"✅ Результат сохранён для матча #{match_id}: {home_score}:{away_score}")

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