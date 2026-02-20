from aiogram import Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import case, func, select

from datetime import datetime, timedelta

from app.db import SessionLocal
from app.models import User, Match, Prediction, Point
from app.stats import build_stats_text
from app.my_predictions import build_my_round_text
from app.tournament import ROUND_DEFAULT, ROUND_MAX, ROUND_MIN, is_tournament_round


class PredictRoundStates(StatesGroup):
    waiting_for_predictions_block = State()


def build_main_menu_keyboard(default_round: int = ROUND_DEFAULT) -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="/join"), types.KeyboardButton(text=f"/round {default_round}")],
            [types.KeyboardButton(text="🎯 Поставить прогноз")],
            [types.KeyboardButton(text=f"/predict_round {default_round}"), types.KeyboardButton(text=f"/my {default_round}")],
            [types.KeyboardButton(text="/table"), types.KeyboardButton(text="/stats")],
            [types.KeyboardButton(text="📘 Правила")],
            [types.KeyboardButton(text="/help")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие из меню ниже",
    )


# Надёжно для любого сервера: МСК = UTC+3 (без tzdata)
def now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


async def get_current_round_default() -> int:
    """
    Автовыбор "текущего тура" по расписанию:
    - если сейчас до окончания тура X -> тур X
    - если все туры прошли -> последний доступный тур
    - если матчей ещё нет -> ROUND_DEFAULT
    """
    async with SessionLocal() as session:
        q = await session.execute(
            select(
                Match.round_number,
                func.min(Match.kickoff_time).label("starts_at"),
                func.max(Match.kickoff_time).label("ends_at"),
            )
            .where(
                Match.round_number >= ROUND_MIN,
                Match.round_number <= ROUND_MAX,
                Match.source == "manual",
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        rows = q.all()

    if not rows:
        return ROUND_DEFAULT

    now = now_msk_naive()
    for round_number, _starts_at, ends_at in rows:
        if now <= ends_at:
            return int(round_number)

    return int(rows[-1][0])


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
        result = await session.execute(
            select(func.count(Match.id)).where(Match.round_number == round_number, Match.source == "manual")
        )
        cnt = result.scalar_one()
        return cnt > 0


async def get_round_total_points_for_user(tg_user_id: int, round_number: int) -> int:
    async with SessionLocal() as session:
        q = await session.execute(
            select(func.coalesce(func.sum(Point.points), 0))
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(Point.tg_user_id == tg_user_id, Match.round_number == round_number, Match.source == "manual")
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

        participants_subq = (
            select(Prediction.tg_user_id.label("tg_user_id"))
            .distinct()
            .subquery()
        )

        q = await session.execute(
            select(
                User.tg_user_id,
                User.username,
                User.full_name,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
            )
            .select_from(participants_subq)
            .join(User, User.tg_user_id == participants_subq.c.tg_user_id)
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
            .where(Match.round_number == round_number, Match.source == "manual")
        )
        participants = int(participants_q.scalar_one())

        q = await session.execute(
            select(
                User.tg_user_id,
                User.username,
                User.full_name,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
            )
            .select_from(User)
            .join(Prediction, Prediction.tg_user_id == User.tg_user_id)
            .join(Match, Match.id == Prediction.match_id)
            .outerjoin(Point, (Point.tg_user_id == User.tg_user_id) & (Point.match_id == Match.id))
            .where(Match.round_number == round_number, Match.source == "manual")
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
    @dp.message(F.text == "📘 Правила")
    async def quick_rules(message: types.Message):
        await message.answer(
            "📘 Короткие правила турнира\n\n"
            "Туры турнира: 19..30.\n"
            "Очки:\n"
            "🎯 точный счёт — 4\n"
            "📏 разница + исход — 2\n"
            "✅ только исход — 1\n"
            "❌ мимо — 0\n\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n"
            "🕒 Время матчей и дедлайны — по Москве (МСК)."
        )

    @dp.message(F.text == "🎯 Поставить прогноз")
    async def quick_predict_hint(message: types.Message):
        default_round = await get_current_round_default()
        await message.answer(
            f"Открой матчи: /round {default_round}\n"
            "Отправь прогноз:\n"
            "/predict <match_id> 2:1\n\n"
            "Пример: /predict 1 2:1"
        )

    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        default_round = await get_current_round_default()
        await message.answer(
            "🏆 Добро пожаловать в бот прогнозов РПЛ.\n\n"
            "Как начать (3 шага):\n"
            "1) Нажми /join\n"
            f"2) Открой матчи тура: /round {default_round}\n"
            "3) Поставь прогноз: /predict <match_id> <счёт>\n\n"
            f"Текущий тур для старта: {default_round}\n"
            "Можно использовать кнопки снизу — так проще и быстрее.\n\n"
            "Очки:\n"
            "🎯 точный счёт — 4\n"
            "📏 разница + исход — 2\n"
            "✅ только исход — 1\n"
            "❌ мимо — 0\n\n"
            "Время матчей и дедлайны — по Москве (МСК).\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n"
            "✅ Можно вводить счет как 2:0 или 2-0.",
            reply_markup=build_main_menu_keyboard(default_round=default_round),
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        default_round = await get_current_round_default()
        await message.answer(
            "📌 Команды:\n"
            "/join - присоединиться к турниру\n"
            "/round N - матчи тура\n"
            "/predict <match_id> <счёт> - прогноз\n"
            "/predict_round N - прогнозы на тур\n"
            "/my N - мои прогнозы на тур\n"
            "/table - общая таблица лидеров\n"
            "/table_round N - таблица лидеров за тур\n"
            "/stats - подробная статистика\n"
            "/ping - проверка\n\n"
            f"Сейчас для старта: тур {default_round}"
        )

    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅")

    @dp.message(Command("join"))
    async def cmd_join(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
        await message.answer("✅ Ты в турнире.")

    @dp.message(Command("round"))
    async def cmd_round(message: types.Message):
        default_round = await get_current_round_default()
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Неверный формат. Пример: /round {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура должен быть числом. Пример: /round {default_round}")
            return

        if not is_tournament_round(round_number):
            await message.answer(f"Можно использовать только туры {ROUND_MIN}..{ROUND_MAX}. Пример: /round {default_round}")
            return

        now = now_msk_naive()

        async with SessionLocal() as session:
            result = await session.execute(
                select(Match).where(Match.round_number == round_number, Match.source == "manual").order_by(Match.kickoff_time.asc())
            )
            matches = result.scalars().all()

        if not matches:
            await message.answer(f"В туре {round_number} пока нет матчей.")
            return

        lines = [f"📅 Тур {round_number} (МСК)"]
        for m in matches:
            icon = match_status_icon(m, now)
            score = ""
            if m.home_score is not None and m.away_score is not None:
                score = f" | {m.home_score}:{m.away_score}"

            lines.append(
                f"{icon} #{m.id} {m.home_team} — {m.away_team} | {m.kickoff_time.strftime('%d.%m %H:%M')}{score}"
            )
        lines.append("")
        lines.append("🟢 прогноз открыт · 🔒 прогноз закрыт · ✅ есть итог")

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

        await message.answer(f"✅ Прогноз #{match_id}: {pred_home}:{pred_away}")

    @dp.message(Command("predict_round"))
    async def cmd_predict_round(message: types.Message, state: FSMContext):
        default_round = await get_current_round_default()
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Неверный формат. Пример: /predict_round {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура должен быть числом. Пример: /predict_round {default_round}")
            return

        if not is_tournament_round(round_number):
            await message.answer(
                f"Можно использовать только туры {ROUND_MIN}..{ROUND_MAX}. Пример: /predict_round {default_round}"
            )
            return

        now = now_msk_naive()

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)

            q = await session.execute(
                select(Match).where(Match.round_number == round_number, Match.source == "manual").order_by(Match.kickoff_time.asc())
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

                match_q = await session.execute(
                    select(Match).where(Match.id == match_id, Match.round_number == round_number, Match.source == "manual")
                )
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
        await message.answer(f"✅ Сохранено: {saved} | Пропущено: {skipped} | Ошибок: {errors}")

    @dp.message(Command("my"))
    async def cmd_my(message: types.Message):
        default_round = await get_current_round_default()
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Неверный формат. Пример: /my {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура должен быть числом. Пример: /my {default_round}")
            return

        if not is_tournament_round(round_number):
            await message.answer(f"Можно использовать только туры {ROUND_MIN}..{ROUND_MAX}. Пример: /my {default_round}")
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
        default_round = await get_current_round_default()
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Неверный формат. Пример: /table_round {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура должен быть числом. Пример: /table_round {default_round}")
            return

        if not is_tournament_round(round_number):
            await message.answer(
                f"Можно использовать только туры {ROUND_MIN}..{ROUND_MAX}. Пример: /table_round {default_round}"
            )
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
