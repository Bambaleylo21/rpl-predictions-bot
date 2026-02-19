from aiogram import Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import select

from app.db import SessionLocal
from app.models import User, Match, Prediction, Point
from app.stats import build_stats_text
from app.my_predictions import build_my_round_text


class PredictRoundStates(StatesGroup):
    waiting_for_predictions_block = State()


def register_user_handlers(dp: Dispatcher) -> None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        tg_user_id = message.from_user.id
        username = message.from_user.username  # без "@", может быть None
        full_name = message.from_user.full_name

        async with SessionLocal() as session:
            result = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
            user = result.scalar_one_or_none()

            if user is None:
                session.add(User(tg_user_id=tg_user_id, username=username))
            else:
                # обновляем username, если он появился или изменился
                user.username = username

            await session.commit()

        # Диагностика: что Telegram реально прислал (потом уберём)
        await message.answer(
            "Привет! Я живой 🙂\n\n"
            f"🔎 Диагностика:\n"
            f"tg_user_id: {tg_user_id}\n"
            f"from_user.username: {username}\n"
            f"from_user.full_name: {full_name}\n\n"
            "Команды:\n"
            "/round 1 — матчи тура\n"
            "/predict 1 2:0 — прогноз на матч\n"
            "/predict_round 1 — прогнозы на тур одним сообщением\n"
            "/my 1 — мои прогнозы на тур\n"
            "/table — таблица лидеров\n"
            "/stats — подробная статистика\n"
            "/whoami — что бот видит\n"
            "/help — помощь"
        )
    @dp.message(Command("fix_username"))
    async def cmd_fix_username(message: types.Message):
        tg_user_id = message.from_user.id
        username = message.from_user.username

        async with SessionLocal() as session:
            result = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
            user = result.scalar_one_or_none()

            if user is None:
                session.add(User(tg_user_id=tg_user_id, username=username))
            else:
                user.username = username

            await session.commit()

        await message.answer(f"✅ Записал в БД username={username} для tg_user_id={tg_user_id}")


    @dp.message(Command("whoami"))
    async def cmd_whoami(message: types.Message):
        tg_user_id = message.from_user.id
        username = message.from_user.username
        full_name = message.from_user.full_name

        async with SessionLocal() as session:
            result = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
            user = result.scalar_one_or_none()

        db_username = None
        if user is not None:
            db_username = user.username

        await message.answer(
            "👤 whoami\n"
            f"tg_user_id: {tg_user_id}\n"
            f"from_user.username: {username}\n"
            f"from_user.full_name: {full_name}\n"
            f"DB users.username: {db_username}\n"
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        text = (
            "📌 Команды:\n"
            "/start — начать\n"
            "/help — помощь\n"
            "/ping — проверка\n"
            "/round N — матчи тура (пример: /round 1)\n"
            "/predict <match_id> <счет> — прогноз (пример: /predict 1 2:0)\n"
            "/predict_round N — прогнозы на тур одним сообщением (пример: /predict_round 1)\n"
            "/my N — мои прогнозы на тур (пример: /my 1)\n"
            "/table — таблица лидеров\n"
            "/stats — подробная статистика\n"
            "/whoami — что бот видит\n\n"
            "Админ:\n"
            "/admin_add_match — добавить матч\n"
            "/admin_set_result — поставить результат\n"
            "/admin_recalc — пересчитать очки\n"
        )
        await message.answer(text)

    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅")

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
            # матч существует?
            result = await session.execute(select(Match).where(Match.id == match_id))
            match = result.scalar_one_or_none()
            if match is None:
                await message.answer(f"Матч с id={match_id} не найден. Посмотри /round 1")
                return

            # upsert прогноз
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

    @dp.message(Command("predict_round"))
    async def cmd_predict_round(message: types.Message, state: FSMContext):
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer("Неверный формат. Пример: /predict_round 1")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer("Номер тура должен быть числом. Пример: /predict_round 1")
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

        await state.update_data(round_number=round_number)
        await state.set_state(PredictRoundStates.waiting_for_predictions_block)

        lines = [f"📝 Ввод прогнозов на тур {round_number} одним сообщением."]
        lines.append("Отправь следующим сообщением строки в формате:")
        lines.append("match_id счет")
        lines.append("Пример:")
        lines.append("1 2:0")
        lines.append("2 1:1")
        lines.append("")
        lines.append("Матчи тура:")
        for m in matches:
            lines.append(f"#{m.id} {m.home_team} — {m.away_team} ({m.kickoff_time.strftime('%Y-%m-%d %H:%M')})")

        await message.answer("\n".join(lines))

    @dp.message(PredictRoundStates.waiting_for_predictions_block)
    async def handle_predictions_block(message: types.Message, state: FSMContext):
        data = await state.get_data()
        round_number = data.get("round_number")

        if round_number is None:
            await message.answer("Что-то пошло не так. Повтори /predict_round 1")
            await state.clear()
            return

        async with SessionLocal() as session:
            res = await session.execute(select(Match).where(Match.round_number == round_number))
            matches = res.scalars().all()
        allowed_match_ids = {m.id for m in matches}

        lines = [ln.strip() for ln in message.text.splitlines() if ln.strip()]

        saved = 0
        errors = 0
        error_lines: list[str] = []

        tg_user_id = message.from_user.id

        async with SessionLocal() as session:
            for ln in lines:
                parts = ln.split()
                if len(parts) != 2:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (нужно: match_id счет)")
                    continue

                match_id_str, score_str = parts
                try:
                    match_id = int(match_id_str)
                except ValueError:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (match_id должен быть числом)")
                    continue

                if match_id not in allowed_match_ids:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (match_id не из тура {round_number})")
                    continue

                if ":" not in score_str:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (счёт должен быть 2:0)")
                    continue

                try:
                    h, a = score_str.split(":")
                    pred_home = int(h)
                    pred_away = int(a)
                except ValueError:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (счёт должен быть числом, пример 2:0)")
                    continue

                res_pred = await session.execute(
                    select(Prediction).where(
                        Prediction.match_id == match_id,
                        Prediction.tg_user_id == tg_user_id,
                    )
                )
                pred = res_pred.scalar_one_or_none()

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

                saved += 1

            await session.commit()

        await state.clear()

        reply = [f"✅ Сохранено прогнозов: {saved}"]
        if errors:
            reply.append(f"⚠️ Ошибок: {errors}")
            reply.append("Проблемные строки:")
            reply.extend(error_lines[:10])
            if len(error_lines) > 10:
                reply.append("…(ещё есть ошибки, показываю первые 10)")

        await message.answer("\n".join(reply))

    @dp.message(Command("my"))
    async def cmd_my(message: types.Message):
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer("Неверный формат. Пример: /my 1")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer("Номер тура должен быть числом. Пример: /my 1")
            return

        tg_user_id = message.from_user.id
        text = await build_my_round_text(tg_user_id=tg_user_id, round_number=round_number)
        await message.answer(text)

    @dp.message(Command("table"))
    async def cmd_table(message: types.Message):
        async with SessionLocal() as session:
            res_users = await session.execute(select(User))
            users = res_users.scalars().all()

            res_points = await session.execute(select(Point))
            points_rows = res_points.scalars().all()

        stats = {}
        for u in users:
            name = u.username if u.username else str(u.tg_user_id)
            stats[u.tg_user_id] = {"name": name, "total": 0, "exact": 0, "diff": 0, "outcome": 0}

        for r in points_rows:
            if r.tg_user_id not in stats:
                stats[r.tg_user_id] = {"name": str(r.tg_user_id), "total": 0, "exact": 0, "diff": 0, "outcome": 0}

            stats[r.tg_user_id]["total"] += int(r.points)
            if r.category == "exact":
                stats[r.tg_user_id]["exact"] += 1
            elif r.category == "diff":
                stats[r.tg_user_id]["diff"] += 1
            elif r.category == "outcome":
                stats[r.tg_user_id]["outcome"] += 1

        rows = list(stats.values())
        rows.sort(key=lambda x: (x["total"], x["exact"], x["diff"], x["outcome"]), reverse=True)

        if not rows:
            await message.answer("Пока нет данных для таблицы.")
            return

        lines = ["🏆 Таблица лидеров:"]
        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")

        await message.answer("\n".join(lines))

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        text = await build_stats_text()
        await message.answer(text)