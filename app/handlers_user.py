from aiogram import Dispatcher, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import select, func

from datetime import datetime, timedelta

from app.db import SessionLocal
from app.models import User, Match, Prediction, Point
from app.stats import build_stats_text
from app.my_predictions import build_my_round_text


class PredictRoundStates(StatesGroup):
    waiting_for_predictions_block = State()


# Надёжно для любого сервера: МСК = UTC+3 (без tzdata)
def now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


def format_user_name(username: str | None, full_name: str | None, tg_user_id: int) -> str:
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return str(tg_user_id)


# Telegram ограничивает длину одного сообщения (примерно 4096 символов).
# Если текст длиннее — Telegram вернёт ошибку "Bad Request: text is too long".
MAX_TELEGRAM_TEXT = 3800


def _split_text_for_telegram(text: str, max_len: int = MAX_TELEGRAM_TEXT) -> list[str]:
    text = (text or "").strip()
    if not text:
        return [""]
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    buf: list[str] = []
    buf_len = 0

    # Режем по строкам, стараясь не ломать форматирование
    for line in text.split("\n"):
        # +1 за символ переноса строки (если он будет добавлен)
        add_len = len(line) + (1 if buf else 0)

        if buf_len + add_len <= max_len:
            if buf:
                buf.append(line)
                buf_len += add_len
            else:
                buf = [line]
                buf_len = len(line)
            continue

        # Если текущий буфер не пуст — закрываем его
        if buf:
            chunks.append("\n".join(buf).strip())
            buf = []
            buf_len = 0

        # Если одна строка сама по себе слишком длинная — режем её по символам
        if len(line) > max_len:
            start = 0
            while start < len(line):
                chunks.append(line[start:start + max_len])
                start += max_len
        else:
            buf = [line]
            buf_len = len(line)

    if buf:
        chunks.append("\n".join(buf).strip())

    return [c for c in chunks if c]


async def send_long(message: types.Message, text: str) -> None:
    for chunk in _split_text_for_telegram(text):
        await message.answer(chunk)


async def upsert_user_from_message(session, message: types.Message):
    tg_user_id = message.from_user.id
    username = message.from_user.username
    full_name = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip() or None

    existing = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
    user = existing.scalar_one_or_none()

    if user is None:
        user = User(tg_user_id=tg_user_id, username=username, full_name=full_name)
        session.add(user)
    else:
        user.username = username
        user.full_name = full_name

    await session.commit()


def normalize_score(s: str) -> str:
    s = s.strip()
    s = s.replace("-", ":")
    return s


def parse_score(s: str) -> tuple[int, int] | None:
    if ":" not in s:
        return None
    a, b = s.split(":", 1)
    try:
        return int(a), int(b)
    except ValueError:
        return None


def match_status_icon(match: Match, now: datetime) -> str:
    # ✅ если есть итог
    if match.home_score is not None and match.away_score is not None:
        return "✅"
    # 🔒 если матч начался/прошёл и итога нет
    if match.kickoff_time <= now:
        return "🔒"
    # 🟢 если прогноз открыт
    return "🟢"


async def round_has_matches(round_number: int) -> bool:
    async with SessionLocal() as session:
        result = await session.execute(select(func.count(Match.id)).where(Match.round_number == round_number))
        cnt = result.scalar_one()
        return cnt > 0


async def get_round_total_points_for_user(tg_user_id: int, round_number: int) -> int:
    async with SessionLocal() as session:
        q = await session.execute(
            select(func.coalesce(func.sum(Point.points), 0))
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(Point.tg_user_id == tg_user_id, Match.round_number == round_number)
        )
        return int(q.scalar_one())


async def get_matches_played_stats() -> tuple[int, int]:
    async with SessionLocal() as session:
        total_q = await session.execute(select(func.count(Match.id)))
        total = int(total_q.scalar_one())

        played_q = await session.execute(
            select(func.count(Match.id)).where(Match.home_score.isnot(None), Match.away_score.isnot(None))
        )
        played = int(played_q.scalar_one())

    return played, total


async def build_overall_leaderboard() -> tuple[list[dict], int]:
    async with SessionLocal() as session:
        # Только участники, у которых есть хотя бы 1 прогноз (по сути — есть points или predictions)
        participants_q = await session.execute(select(func.count(func.distinct(Prediction.tg_user_id))))
        participants = int(participants_q.scalar_one())

        q = await session.execute(
            select(
                User.tg_user_id,
                User.username,
                User.full_name,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(Point.exact), 0).label("exact"),
                func.coalesce(func.sum(Point.diff), 0).label("diff"),
                func.coalesce(func.sum(Point.outcome), 0).label("outcome"),
            )
            .select_from(User)
            .join(Prediction, Prediction.tg_user_id == User.tg_user_id)
            .outerjoin(Point, Point.tg_user_id == User.tg_user_id)
            .group_by(User.tg_user_id)
            .order_by(func.coalesce(func.sum(Point.points), 0).desc())
        )

        rows = []
        for tg_user_id, username, full_name, total, exact, diff, outcome in q.all():
            rows.append(
                {
                    "tg_user_id": tg_user_id,
                    "name": format_user_name(username, full_name, tg_user_id),
                    "total": int(total),
                    "exact": int(exact),
                    "diff": int(diff),
                    "outcome": int(outcome),
                }
            )

        return rows, participants


async def build_round_leaderboard(round_number: int) -> tuple[list[dict], int]:
    async with SessionLocal() as session:
        participants_q = await session.execute(
            select(func.count(func.distinct(Prediction.tg_user_id)))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(Match.round_number == round_number)
        )
        participants = int(participants_q.scalar_one())

        q = await session.execute(
            select(
                User.tg_user_id,
                User.username,
                User.full_name,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(Point.exact), 0).label("exact"),
                func.coalesce(func.sum(Point.diff), 0).label("diff"),
                func.coalesce(func.sum(Point.outcome), 0).label("outcome"),
            )
            .select_from(User)
            .join(Prediction, Prediction.tg_user_id == User.tg_user_id)
            .join(Match, Match.id == Prediction.match_id)
            .outerjoin(Point, (Point.tg_user_id == User.tg_user_id) & (Point.match_id == Match.id))
            .where(Match.round_number == round_number)
            .group_by(User.tg_user_id)
            .order_by(func.coalesce(func.sum(Point.points), 0).desc())
        )

        rows = []
        for tg_user_id, username, full_name, total, exact, diff, outcome in q.all():
            rows.append(
                {
                    "tg_user_id": tg_user_id,
                    "name": format_user_name(username, full_name, tg_user_id),
                    "total": int(total),
                    "exact": int(exact),
                    "diff": int(diff),
                    "outcome": int(outcome),
                }
            )

        return rows, participants


def register_user_handlers(dp: Dispatcher):

    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        await message.answer(
            "Привет! Это бот турнира прогнозов РПЛ.\n\n"
            "Основные команды:\n"
            "/join — вступить в турнир\n"
            "/round N — матчи тура\n"
            "/predict <match_id> <счёт> — прогноз на матч\n"
            "/predict_round N — прогнозы на тур (пакетом)\n"
            "/my N — мои прогнозы на тур\n"
            "/table — общая таблица лидеров\n"
            "/table_round N — таблица лидеров тура\n"
            "/stats — статистика\n"
            "/ping — проверка связи\n\n"
            "Очки:\n"
            "🎯 точный счёт — 4\n"
            "📏 разница + исход — 2\n"
            "✅ только исход — 1\n"
            "❌ мимо — 0\n\n"
            "Время матчей и дедлайны — по Москве (МСК).\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n"
            "✅ Можно вводить счет как 2:0 или 2-0."
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        await message.answer(
            "Команды:\n"
            "/join — вступить в турнир\n"
            "/round N — матчи тура\n"
            "/predict <match_id> <счёт> — прогноз на матч\n"
            "/predict_round N — прогнозы на тур (пакетом)\n"
            "/my N — мои прогнозы на тур\n"
            "/table — общая таблица лидеров\n"
            "/table_round N — таблица лидеров тура\n"
            "/stats — статистика\n"
            "/ping — проверка связи\n"
        )

    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅")

    @dp.message(Command("join"))
    async def cmd_join(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
        await message.answer("✅ Ты в турнире! Теперь можешь делать прогнозы.")

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
                select(Match).where(Match.round_number == round_number).order_by(Match.kickoff_time.asc())
            )
            matches = result.scalars().all()

        if not matches:
            await message.answer(f"В туре {round_number} пока нет матчей.")
            return

        lines = [f"📅 Тур {round_number} (МСК):", "Легенда: 🟢 прогноз открыт · 🔒 прогноз закрыт · ✅ есть итог"]
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

        await send_long(message, "\n".join(lines))

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
            await upsert_user_from_message(session, message)

            match_q = await session.execute(select(Match).where(Match.id == match_id))
            match = match_q.scalar_one_or_none()
            if match is None:
                await message.answer("Матч не найден.")
                return

            if match.kickoff_time <= now:
                await message.answer("🔒 Прогнозы на этот матч уже закрыты (матч начался).")
                return

            pred_q = await session.execute(
                select(Prediction).where(Prediction.tg_user_id == tg_user_id, Prediction.match_id == match_id)
            )
            pred = pred_q.scalar_one_or_none()

            if pred is None:
                pred = Prediction(
                    tg_user_id=tg_user_id,
                    match_id=match_id,
                    pred_home=pred_home,
                    pred_away=pred_away,
                )
                session.add(pred)
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

        now = now_msk_naive()

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)

            q = await session.execute(
                select(Match).where(Match.round_number == round_number).order_by(Match.kickoff_time.asc())
            )
            matches = q.scalars().all()

        if not matches:
            await message.answer(f"В туре {round_number} пока нет матчей.")
            return

        open_matches = [m for m in matches if m.kickoff_time > now]
        if not open_matches:
            await message.answer("Все матчи тура уже закрыты. Нечего прогнозировать.")
            return

        lines = [
            f"🧾 Ввод прогнозов на тур {round_number}.\n"
            "Отправь одним сообщением прогнозы в формате:\n"
            "match_id счет\n"
            "Пример:\n"
            "1 2:0\n2 1:1\n\n"
            "Открытые матчи:"
        ]
        for m in open_matches:
            icon = match_status_icon(m, now)
            lines.append(f"{icon} #{m.id} {m.home_team} — {m.away_team} ({m.kickoff_time.strftime('%Y-%m-%d %H:%M')} МСК)")

        await state.set_state(PredictRoundStates.waiting_for_predictions_block)
        await state.update_data(round_number=round_number)

        await send_long(message, "\n".join(lines))

    @dp.message(PredictRoundStates.waiting_for_predictions_block)
    async def handle_predictions_block(message: types.Message, state: FSMContext):
        data = await state.get_data()
        round_number = data.get("round_number")
        if not round_number:
            await state.clear()
            await message.answer("⚠️ Сессия ввода сброшена. Начни заново: /predict_round N")
            return

        tg_user_id = message.from_user.id
        now = now_msk_naive()

        lines = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
        if not lines:
            await message.answer("Пусто. Пришли строки формата: match_id счет")
            return

        saved = 0
        skipped = 0
        errors = 0

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)

            for line in lines:
                parts = line.replace("-", ":").split()
                if len(parts) != 2:
                    errors += 1
                    continue
                try:
                    match_id = int(parts[0])
                except ValueError:
                    errors += 1
                    continue

                parsed = parse_score(parts[1])
                if parsed is None:
                    errors += 1
                    continue
                pred_home, pred_away = parsed

                match_q = await session.execute(select(Match).where(Match.id == match_id, Match.round_number == round_number))
                match = match_q.scalar_one_or_none()
                if match is None:
                    skipped += 1
                    continue

                if match.kickoff_time <= now:
                    skipped += 1
                    continue

                pred_q = await session.execute(
                    select(Prediction).where(Prediction.tg_user_id == tg_user_id, Prediction.match_id == match_id)
                )
                pred = pred_q.scalar_one_or_none()
                if pred is None:
                    session.add(
                        Prediction(
                            tg_user_id=tg_user_id,
                            match_id=match_id,
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
        await message.answer(f"✅ Готово. Сохранено: {saved}. Пропущено (закрыто/не найдено): {skipped}. Ошибок формата: {errors}.")

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
            await upsert_user_from_message(session, message)

        tg_user_id = message.from_user.id
        text = await build_my_round_text(tg_user_id=tg_user_id, round_number=round_number)

        if await round_has_matches(round_number):
            total = await get_round_total_points_for_user(tg_user_id=tg_user_id, round_number=round_number)
            text = f"{text}\n\nИтого за тур: {total} очк."

        await send_long(message, text)

    @dp.message(Command("table"))
    async def cmd_table(message: types.Message):
        played, total = await get_matches_played_stats()
        rows, participants = await build_overall_leaderboard()

        if not rows:
            await message.answer("Пока нет участников с прогнозами. Сделай первый прогноз через /predict или /predict_round.")
            return

        lines = ["🏆 Таблица лидеров (общая):"]
        lines.append(f"Участников с прогнозами: {participants}")
        lines.append(f"Матчей сыграно: {played} / {total}")

        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")

        await send_long(message, "\n".join(lines))

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

        rows, participants = await build_round_leaderboard(round_number)
        if not rows:
            await message.answer("Пока нет прогнозов на этот тур.")
            return

        lines = [f"🏁 Таблица тура {round_number}:"]
        lines.append(f"Участников с прогнозами в туре: {participants}")

        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")

        await send_long(message, "\n".join(lines))

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        text = await build_stats_text()
        await send_long(message, text)