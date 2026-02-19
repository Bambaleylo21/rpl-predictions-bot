from aiogram import Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import select, func

from datetime import datetime
from zoneinfo import ZoneInfo

from app.db import SessionLocal
from app.models import User, Match, Prediction, Point
from app.stats import build_stats_text
from app.my_predictions import build_my_round_text


class PredictRoundStates(StatesGroup):
    waiting_for_predictions_block = State()


MSK_TZ = ZoneInfo("Europe/Moscow")


async def ensure_user(session, message: types.Message) -> None:
    """
    Гарантирует, что пользователь есть в БД, и что username актуален.
    """
    if not message.from_user:
        return

    tg_user_id = message.from_user.id
    username = message.from_user.username  # без "@", может быть None

    result = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
    user = result.scalar_one_or_none()

    if user is None:
        session.add(User(tg_user_id=tg_user_id, username=username))
    else:
        user.username = username

    await session.commit()


def now_msk_naive() -> datetime:
    """
    Возвращает текущее время как naive datetime в МСК.
    Предположение: kickoff_time в БД хранится как naive datetime в МСК
    (ты вводишь даты матчей в МСК).
    """
    return datetime.now(MSK_TZ).replace(tzinfo=None)


def normalize_score(score_str: str) -> str:
    """
    Нормализуем ввод счета: принимаем 2:0 и 2-0
    """
    return score_str.strip().replace("-", ":")


def parse_score(score_str: str) -> tuple[int, int] | None:
    """
    Парсим счет вида 2:0 (после normalize_score)
    """
    if ":" not in score_str:
        return None
    try:
        h, a = score_str.split(":")
        home = int(h)
        away = int(a)
    except Exception:
        return None
    if home < 0 or away < 0:
        return None
    return home, away


def match_status_icon(match: Match, now: datetime) -> str:
    """
    ✅ есть итог
    🔒 матч начался, прогноз закрыт
    🟢 матч не начался, прогноз открыт
    """
    if match.home_score is not None and match.away_score is not None:
        return "✅"
    if match.kickoff_time <= now:
        return "🔒"
    return "🟢"


async def build_leaderboard_for_round(round_number: int) -> list[dict]:
    """
    Возвращает список строк-лидеров за тур:
    [{"tg_user_id":..., "name":..., "total":..., "exact":..., "diff":..., "outcome":...}, ...]
    """
    async with SessionLocal() as session:
        res_users = await session.execute(select(User))
        users = res_users.scalars().all()

        # points только за выбранный тур (через join с Match)
        res_points = await session.execute(
            select(Point, Match)
            .join(Match, Point.match_id == Match.id)
            .where(Match.round_number == round_number)
        )
        point_match_rows = res_points.all()

    # подготовим словарь пользователей (чтобы имена были)
    stats: dict[int, dict] = {}
    for u in users:
        name = u.username if u.username else str(u.tg_user_id)
        stats[u.tg_user_id] = {"tg_user_id": u.tg_user_id, "name": name, "total": 0, "exact": 0, "diff": 0, "outcome": 0}

    # наполняем по очкам
    for p, _m in point_match_rows:
        if p.tg_user_id not in stats:
            stats[p.tg_user_id] = {"tg_user_id": p.tg_user_id, "name": str(p.tg_user_id), "total": 0, "exact": 0, "diff": 0, "outcome": 0}

        stats[p.tg_user_id]["total"] += int(p.points)
        if p.category == "exact":
            stats[p.tg_user_id]["exact"] += 1
        elif p.category == "diff":
            stats[p.tg_user_id]["diff"] += 1
        elif p.category == "outcome":
            stats[p.tg_user_id]["outcome"] += 1

    rows = list(stats.values())

    # покажем только тех, кто реально участвовал в туре (есть хотя бы один Point)
    rows = [r for r in rows if r["total"] > 0 or r["exact"] > 0 or r["diff"] > 0 or r["outcome"] > 0]

    rows.sort(key=lambda x: (x["total"], x["exact"], x["diff"], x["outcome"]), reverse=True)
    return rows


async def get_round_total_points_for_user(tg_user_id: int, round_number: int) -> int:
    async with SessionLocal() as session:
        res = await session.execute(
            select(func.coalesce(func.sum(Point.points), 0))
            .join(Match, Point.match_id == Match.id)
            .where(Match.round_number == round_number, Point.tg_user_id == tg_user_id)
        )
        total = res.scalar_one()
    return int(total or 0)


async def get_matches_played_stats() -> tuple[int, int]:
    """
    Возвращает (played, total) по всем матчам в базе.
    played = где есть итог (home_score и away_score не None)
    """
    async with SessionLocal() as session:
        total_res = await session.execute(select(func.count(Match.id)))
        total = int(total_res.scalar_one() or 0)

        played_res = await session.execute(
            select(func.count(Match.id)).where(Match.home_score.is_not(None), Match.away_score.is_not(None))
        )
        played = int(played_res.scalar_one() or 0)

    return played, total


async def get_best_player_of_last_played_round() -> tuple[int, int] | None:
    """
    Возвращает (round_number, tg_user_id) лучшего игрока последнего тура, где есть хоть один сыгранный матч.
    Если нет сыгранных матчей — None.
    """
    async with SessionLocal() as session:
        last_round_res = await session.execute(
            select(func.max(Match.round_number))
            .where(Match.home_score.is_not(None), Match.away_score.is_not(None))
        )
        last_round = last_round_res.scalar_one()
        if last_round is None:
            return None

        # топ по сумме очков за этот тур
        top_res = await session.execute(
            select(Point.tg_user_id, func.coalesce(func.sum(Point.points), 0).label("s"))
            .join(Match, Point.match_id == Match.id)
            .where(Match.round_number == last_round)
            .group_by(Point.tg_user_id)
            .order_by(func.coalesce(func.sum(Point.points), 0).desc())
            .limit(1)
        )
        row = top_res.first()
        if not row:
            return None

        tg_user_id = int(row[0])
        return int(last_round), tg_user_id


async def get_user_display_name(tg_user_id: int) -> str:
    async with SessionLocal() as session:
        res = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
        u = res.scalar_one_or_none()
    if u and u.username:
        return u.username
    return str(tg_user_id)


def register_user_handlers(dp: Dispatcher) -> None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        async with SessionLocal() as session:
            await ensure_user(session, message)

        await message.answer(
            "Привет! Я бот турнира прогнозов РПЛ ⚽️\n\n"
            "⏰ Время матчей и дедлайны — по Москве (МСК).\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n\n"
            "Команды:\n"
            "/round 1 — матчи тура\n"
            "/predict 1 2:0 — прогноз на матч\n"
            "/predict_round 1 — прогнозы на тур одним сообщением\n"
            "/my 1 — мои прогнозы на тур\n"
            "/table — общая таблица\n"
            "/table_round 1 — таблица за тур\n"
            "/stats — подробная статистика\n"
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
            "/predict <match_id> <счет> — прогноз (пример: /predict 1 2:0)\n"
            "/predict_round N — прогнозы на тур одним сообщением (пример: /predict_round 1)\n"
            "/my N — мои прогнозы на тур (пример: /my 1)\n"
            "/table — общая таблица лидеров\n"
            "/table_round N — таблица лидеров за тур\n"
            "/stats — подробная статистика\n\n"
            "Правила:\n"
            "⏰ Время матчей и дедлайны — по Москве (МСК).\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n"
            "✅ Можно вводить счет как 2:0 или 2-0.\n\n"
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

        now = now_msk_naive()

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

        lines = [f"📅 Тур {round_number} (МСК):"]
        lines.append("Легенда: 🟢 прогноз открыт · 🔒 прогноз закрыт · ✅ есть итог")
        for m in matches:
            icon = match_status_icon(m, now)

            extra = ""
            if m.home_score is not None and m.away_score is not None:
                extra = f" | итог: {m.home_score}:{m.away_score}"
            elif m.kickoff_time > now:
                delta = m.kickoff_time - now
                minutes = int(delta.total_seconds() // 60)
                if minutes >= 60:
                    extra = f" | старт через ~{minutes // 60}ч {minutes % 60}м"
                else:
                    extra = f" | старт через ~{minutes}м"
            else:
                extra = " | матч начался"

            lines.append(
                f"{icon} #{m.id} — {m.home_team} — {m.away_team} | {m.kickoff_time.strftime('%Y-%m-%d %H:%M')} МСК{extra}"
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

        score_str = normalize_score(parts[2])
        parsed = parse_score(score_str)
        if parsed is None:
            await message.answer("Счёт должен быть в формате 2:0 (или 2-0)")
            return

        pred_home, pred_away = parsed
        tg_user_id = message.from_user.id
        now = now_msk_naive()

        async with SessionLocal() as session:
            await ensure_user(session, message)

            result = await session.execute(select(Match).where(Match.id == match_id))
            match = result.scalar_one_or_none()
            if match is None:
                await message.answer(f"Матч с id={match_id} не найден. Посмотри /round 1")
                return

            if match.kickoff_time <= now:
                await message.answer(
                    "⛔️ Матч уже начался. Ставить/менять прогноз нельзя.\n"
                    f"Начало: {match.kickoff_time.strftime('%Y-%m-%d %H:%M')} МСК"
                )
                return

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
            await ensure_user(session, message)

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

        lines = [f"📝 Ввод прогнозов на тур {round_number} (МСК) одним сообщением."]
        lines.append("✅ Можно вводить счет как 2:0 или 2-0.")
        lines.append("⛔️ После начала матча прогноз поставить/изменить нельзя (такие строки будут пропущены).")
        lines.append("")
        lines.append("Отправь следующим сообщением строки в формате:")
        lines.append("match_id счет")
        lines.append("Пример:")
        lines.append("1 2:0")
        lines.append("2 1-1")
        lines.append("")
        lines.append("Матчи тура:")
        now = now_msk_naive()
        for m in matches:
            icon = match_status_icon(m, now)
            lines.append(f"{icon} #{m.id} {m.home_team} — {m.away_team} ({m.kickoff_time.strftime('%Y-%m-%d %H:%M')} МСК)")

        await message.answer("\n".join(lines))

    @dp.message(PredictRoundStates.waiting_for_predictions_block)
    async def handle_predictions_block(message: types.Message, state: FSMContext):
        data = await state.get_data()
        round_number = data.get("round_number")

        if round_number is None:
            await message.answer("Что-то пошло не так. Повтори /predict_round 1")
            await state.clear()
            return

        now = now_msk_naive()

        async with SessionLocal() as session:
            await ensure_user(session, message)

            res = await session.execute(select(Match).where(Match.round_number == round_number))
            matches = res.scalars().all()

            match_by_id = {m.id: m for m in matches}
            allowed_match_ids = set(match_by_id.keys())

            lines = [ln.strip() for ln in message.text.splitlines() if ln.strip()]

            saved = 0
            errors = 0
            skipped = 0
            skipped_details: list[str] = []
            error_lines: list[str] = []

            tg_user_id = message.from_user.id

            for ln in lines:
                parts = ln.split()
                if len(parts) != 2:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (нужно: match_id счет)")
                    continue

                match_id_str, score_str_raw = parts
                match_id_str = match_id_str.lstrip("#")

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

                score_str = normalize_score(score_str_raw)
                parsed = parse_score(score_str)
                if parsed is None:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (счёт должен быть 2:0 или 2-0)")
                    continue

                pred_home, pred_away = parsed

                m = match_by_id.get(match_id)
                if m is None:
                    errors += 1
                    error_lines.append(f"❌ '{ln}' (матч не найден)")
                    continue

                if m.kickoff_time <= now:
                    skipped += 1
                    skipped_details.append(f"🔒 #{m.id} {m.home_team}—{m.away_team} ({m.kickoff_time.strftime('%Y-%m-%d %H:%M')} МСК)")
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
        if skipped:
            reply.append(f"⛔️ Пропущено (матч уже начался): {skipped}")
            reply.append("Пропущенные матчи:")
            reply.extend(skipped_details[:10])
            if len(skipped_details) > 10:
                reply.append("…(ещё есть пропущенные, показываю первые 10)")
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

        async with SessionLocal() as session:
            await ensure_user(session, message)

        tg_user_id = message.from_user.id
        text = await build_my_round_text(tg_user_id=tg_user_id, round_number=round_number)

        total = await get_round_total_points_for_user(tg_user_id=tg_user_id, round_number=round_number)
        # добавим итог за тур, если в тексте вообще есть матчи
        if text and "пока нет матчей" not in text.lower():
            text = f"{text}\n\nИтого за тур: {total} очк."

        await message.answer(text)

    @dp.message(Command("table"))
    async def cmd_table(message: types.Message):
        played, total = await get_matches_played_stats()

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

        lines = ["🏆 Таблица лидеров (общая):"]
        lines.append(f"Матчей сыграно: {played} / {total}")
        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")

        await message.answer("\n".join(lines))

    @dp.message(Command("table_round"))
    async def cmd_table_round(message: types.Message):
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer("Неверный формат. Пример: /table_round 1")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer("Номер тура должен быть числом. Пример: /table_round 1")
            return

        rows = await build_leaderboard_for_round(round_number)

        if not rows:
            await message.answer(f"Пока нет данных для таблицы тура {round_number}.")
            return

        lines = [f"🏁 Таблица тура {round_number}:"]
        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")

        await message.answer("\n".join(lines))

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        text = await build_stats_text()

        best = await get_best_player_of_last_played_round()
        if best is not None:
            round_number, tg_user_id = best
            name = await get_user_display_name(tg_user_id)
            # посчитаем очки этого человека за тур
            total = await get_round_total_points_for_user(tg_user_id=tg_user_id, round_number=round_number)

            extra = (
                "\n\n🏅 Лучший игрок последнего сыгранного тура:\n"
                f"Тур {round_number}: {name} — {total} очк."
            )
            text = f"{text}{extra}"

        await message.answer(text)