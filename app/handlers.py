from datetime import datetime

from aiogram import Dispatcher, types
from aiogram.filters import CommandStart, Command

from sqlalchemy import select

from app.db import SessionLocal
from app.models import User, Match, Prediction

ADMIN_IDS = {210477579}


def register_handlers(dp: Dispatcher) -> None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        tg_user_id = message.from_user.id
        username = message.from_user.username

        async with SessionLocal() as session:
            result = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
            user = result.scalar_one_or_none()

            if user is None:
                session.add(User(tg_user_id=tg_user_id, username=username))
                await session.commit()

        await message.answer(
            "Привет! Я живой 🙂\n\n"
            "Основные команды:\n"
            "/round 1 — показать матчи тура\n"
            "/predict <match_id> <счет> — сделать прогноз (пример: /predict 1 2:0)\n"
            "/help — помощь"
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        text = (
            "📌 Команды:\n"
            "/start — начать\n"
            "/help — помощь\n"
            "/ping — проверка\n"
            "/round N — матчи тура (пример: /round 1)\n"
            "/predict <match_id> <счет> — прогноз (пример: /predict 1 2:0)\n\n"
            "Админ:\n"
            "/admin_add_match — добавить матч\n"
            "/admin_set_result — поставить результат (пример: /admin_set_result 1 2:1)\n"
        )
        await message.answer(text)

    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅")

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

    @dp.message(Command("round"))
    async def cmd_round(message: types.Message):
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer("Неверный формат. Пример: /round 1")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer("Номер тура должен быть числом. Пример: /round 1")
            return

        async with SessionLocal() as session:
            result = await session.execute(
                select(Match)
                .where(Match.round_number == round_number)
                .order_by(Match.kickoff_time.asc())
            )
            matches = result.scalars().all()

        if not matches:
            await message.answer(f"В туре {round_number} пока нет матчей.")
            return

        lines = [f"📅 Тур {round_number}:"]
        for m in matches:
            score = ""
            if m.home_score is not None and m.away_score is not None:
                score = f" | итог: {m.home_score}:{m.away_score}"
            lines.append(
                f"#{m.id} — {m.home_team} — {m.away_team} | {m.kickoff_time.strftime('%Y-%m-%d %H:%M')}{score}"
            )

        await message.answer("\n".join(lines))

    @dp.message(Command("predict"))
    async def cmd_predict(message: types.Message):
        # Формат: /predict 1 2:0
        parts = message.text.strip().split()
        if len(parts) != 3:
            await message.answer("Неверный формат. Пример: /predict 1 2:0")
            return

        try:
            match_id = int(parts[1])
        except ValueError:
            await message.answer("match_id должен быть числом. Пример: /predict 1 2:0")
            return

        score_str = parts[2].strip()
        if ":" not in score_str:
            await message.answer("Счёт должен быть в формате 2:0")
            return

        try:
            h, a = score_str.split(":")
            pred_home = int(h)
            pred_away = int(a)
        except ValueError:
            await message.answer("Счёт должен быть числом. Пример: 2:0")
            return

        tg_user_id = message.from_user.id

        async with SessionLocal() as session:
            # Проверим, что матч существует
            result = await session.execute(select(Match).where(Match.id == match_id))
            match = result.scalar_one_or_none()
            if match is None:
                await message.answer(f"Матч с id={match_id} не найден. Посмотри /round 1")
                return

            # Есть ли уже прогноз?
            result = await session.execute(
                select(Prediction).where(
                    Prediction.match_id == match_id,
                    Prediction.tg_user_id == tg_user_id,
                )
            )
            pred = result.scalar_one_or_none()

            if pred is None:
                session.add(
                    Prediction(
                        match_id=match_id,
                        tg_user_id=tg_user_id,
                        pred_home=pred_home,
                        pred_away=pred_away,
                    )
                )
            else:
                pred.pred_home = pred_home
                pred.pred_away = pred_away

            await session.commit()

        await message.answer(f"✅ Прогноз сохранён для матча #{match_id}: {pred_home}:{pred_away}")