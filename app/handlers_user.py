from aiogram import Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import case, func, select

from datetime import datetime, timedelta

from app.config import load_admin_ids
from app.db import SessionLocal
from app.models import Match, Point, Prediction, Tournament, User, UserTournament
from app.stats import build_stats_text
from app.my_predictions import build_my_round_text

ADMIN_IDS = load_admin_ids()


class PredictRoundStates(StatesGroup):
    waiting_for_predictions_block = State()
    waiting_for_single_match_score = State()
    waiting_for_display_name = State()


DEFAULT_TOURNAMENT_CODE = "RPL"


def _selected_tournament_key(tg_user_id: int) -> str:
    return f"USER_SELECTED_TOURNAMENT_{tg_user_id}"


def build_main_menu_keyboard(default_round: int) -> types.ReplyKeyboardMarkup:
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="🇷🇺 РПЛ"), types.KeyboardButton(text="🇬🇧 АПЛ")],
            [types.KeyboardButton(text="✅ Вступить в турнир"), types.KeyboardButton(text="📅 Матчи тура")],
            [types.KeyboardButton(text="🎯 Поставить прогноз")],
            [types.KeyboardButton(text="🗂 Мои прогнозы"), types.KeyboardButton(text="🏆 Общая таблица")],
            [types.KeyboardButton(text="👤 Мой профиль"), types.KeyboardButton(text="📊 Статистика")],
            [types.KeyboardButton(text="❓ Помощь"), types.KeyboardButton(text="📘 Правила")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие из меню ниже",
    )


# Надёжно для любого сервера: МСК = UTC+3 (без tzdata)
def now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


async def get_tournament_by_code(session, code: str) -> Tournament | None:
    q = await session.execute(select(Tournament).where(Tournament.code == code))
    return q.scalar_one_or_none()


async def ensure_user_membership(
    session,
    tg_user_id: int,
    tournament_id: int,
    display_name: str | None = None,
) -> UserTournament:
    q = await session.execute(
        select(UserTournament).where(
            UserTournament.tg_user_id == tg_user_id,
            UserTournament.tournament_id == tournament_id,
        )
    )
    row = q.scalar_one_or_none()
    if row is None:
        row = UserTournament(tg_user_id=tg_user_id, tournament_id=tournament_id, display_name=display_name)
        session.add(row)
    elif display_name is not None:
        row.display_name = display_name
    return row


async def is_user_in_tournament(session, tg_user_id: int, tournament_id: int) -> bool:
    q = await session.execute(
        select(UserTournament).where(
            UserTournament.tg_user_id == tg_user_id,
            UserTournament.tournament_id == tournament_id,
        )
    )
    return q.scalar_one_or_none() is not None


async def get_selected_tournament_for_user(session, tg_user_id: int) -> Tournament:
    from app.models import Setting  # local import to avoid circular usage patterns

    key = _selected_tournament_key(tg_user_id)
    st_q = await session.execute(select(Setting).where(Setting.key == key))
    st = st_q.scalar_one_or_none()
    code = (st.value if st else DEFAULT_TOURNAMENT_CODE).upper()

    t = await get_tournament_by_code(session, code)
    if t is None:
        t = await get_tournament_by_code(session, DEFAULT_TOURNAMENT_CODE)
    if t is None:
        # fallback safety for corrupted DB
        t = Tournament(code=DEFAULT_TOURNAMENT_CODE, name="Russian Premier League", round_min=19, round_max=30, is_active=1)
        session.add(t)
        await session.commit()
        await session.refresh(t)
    return t


async def set_selected_tournament_for_user(session, tg_user_id: int, tournament_code: str) -> Tournament | None:
    from app.models import Setting  # local import

    t = await get_tournament_by_code(session, tournament_code.upper())
    if t is None:
        return None

    key = _selected_tournament_key(tg_user_id)
    st_q = await session.execute(select(Setting).where(Setting.key == key))
    st = st_q.scalar_one_or_none()
    if st is None:
        session.add(Setting(key=key, value=t.code))
    else:
        st.value = t.code
    await session.commit()
    return t


async def notify_admins_new_join(
    bot,
    tg_user_id: int,
    username: str | None,
    display_name: str,
    tournament_name: str,
) -> None:
    if not ADMIN_IDS:
        return
    login = f"@{username}" if username else str(tg_user_id)
    text = (
        "👤 Новый участник турнира\n"
        f"Имя: {display_name}\n"
        f"Логин: {login}\n"
        f"Турнир: {tournament_name}"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text)
        except Exception:
            continue


async def get_current_round_default(tournament_id: int, round_min: int, round_max: int) -> int:
    """
    Автовыбор "текущего тура" по расписанию в рамках выбранного турнира.
    """
    async with SessionLocal() as session:
        q = await session.execute(
            select(
                Match.round_number,
                func.max(Match.kickoff_time).label("ends_at"),
            )
            .where(
                Match.tournament_id == tournament_id,
                Match.round_number >= round_min,
                Match.round_number <= round_max,
                Match.source == "manual",
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        rows = q.all()

    if not rows:
        return round_min

    now = now_msk_naive()
    for round_number, ends_at in rows:
        if now <= ends_at:
            return int(round_number)
    return int(rows[-1][0])


def build_open_matches_inline_keyboard(matches: list[Match]) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    current_row: list[types.InlineKeyboardButton] = []
    for m in matches:
        btn = types.InlineKeyboardButton(
            text=f"{m.home_team} — {m.away_team}",
            callback_data=f"pick_match:{m.id}",
        )
        current_row.append(btn)
        if len(current_row) == 1:
            rows.append(current_row)
            current_row = []

    if current_row:
        rows.append(current_row)

    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def build_round_history_keyboard(round_min: int, round_max: int) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    row: list[types.InlineKeyboardButton] = []
    for r in range(round_min, round_max + 1):
        row.append(types.InlineKeyboardButton(text=str(r), callback_data=f"history_round:{r}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def build_quick_nav_keyboard(kind: str) -> types.InlineKeyboardMarkup:
    if kind == "after_predict":
        rows = [
            [
                types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data="qnav:my"),
                types.InlineKeyboardButton(text="📅 Матчи тура", callback_data="qnav:round"),
            ],
            [types.InlineKeyboardButton(text="🎯 Ещё прогноз", callback_data="qnav:predict")],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_table":
        rows = [
            [
                types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict"),
                types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data="qnav:my"),
            ]
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_my":
        rows = [
            [
                types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict"),
                types.InlineKeyboardButton(text="📅 Матчи тура", callback_data="qnav:round"),
            ],
            [types.InlineKeyboardButton(text="🏆 Общая таблица", callback_data="qnav:table")],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_info":
        rows = [
            [
                types.InlineKeyboardButton(text="📅 Матчи тура", callback_data="qnav:round"),
                types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict"),
            ],
            [types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data="qnav:my")],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    return types.InlineKeyboardMarkup(inline_keyboard=[])


def format_user_name(
    tournament_display_name: str | None,
    user_display_name: str | None,
    username: str | None,
    full_name: str | None,
    tg_user_id: int,
) -> str:
    if tournament_display_name:
        return tournament_display_name
    if user_display_name:
        return user_display_name
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return str(tg_user_id)


def normalize_display_name(raw: str) -> str | None:
    name = " ".join((raw or "").strip().split())
    if len(name) < 2 or len(name) > 24:
        return None
    return name


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


async def round_has_matches(round_number: int, tournament_id: int) -> bool:
    async with SessionLocal() as session:
        result = await session.execute(
            select(func.count(Match.id)).where(
                Match.round_number == round_number,
                Match.source == "manual",
                Match.tournament_id == tournament_id,
            )
        )
        cnt = result.scalar_one()
        return cnt > 0


async def get_round_total_points_for_user(tg_user_id: int, round_number: int, tournament_id: int) -> int:
    async with SessionLocal() as session:
        q = await session.execute(
            select(func.coalesce(func.sum(Point.points), 0))
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(
                Point.tg_user_id == tg_user_id,
                Match.round_number == round_number,
                Match.source == "manual",
                Match.tournament_id == tournament_id,
            )
        )
        return int(q.scalar_one())


async def get_matches_played_stats(tournament_id: int) -> tuple[int, int]:
    async with SessionLocal() as session:
        total_q = await session.execute(
            select(func.count(Match.id)).where(Match.source == "manual", Match.tournament_id == tournament_id)
        )
        total = int(total_q.scalar_one())

        played_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.home_score.isnot(None),
                Match.away_score.isnot(None),
                Match.source == "manual",
                Match.tournament_id == tournament_id,
            )
        )
        played = int(played_q.scalar_one())

    return played, total


async def build_overall_leaderboard(tournament_id: int) -> tuple[list[dict], int]:
    async with SessionLocal() as session:
        # Только участники, у которых есть хотя бы 1 прогноз (по сути — есть points или predictions)
        participants_q = await session.execute(
            select(func.count(func.distinct(Prediction.tg_user_id)))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(Match.tournament_id == tournament_id, Match.source == "manual")
        )
        participants = int(participants_q.scalar_one())

        participants_subq = (
            select(Prediction.tg_user_id.label("tg_user_id"))
            .join(Match, Match.id == Prediction.match_id)
            .where(Match.tournament_id == tournament_id, Match.source == "manual")
            .distinct()
            .subquery()
        )

        q = await session.execute(
            select(
                User.tg_user_id,
                UserTournament.display_name,
                User.display_name,
                User.username,
                User.full_name,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
            )
            .select_from(participants_subq)
            .join(User, User.tg_user_id == participants_subq.c.tg_user_id)
            .outerjoin(
                UserTournament,
                (UserTournament.tg_user_id == User.tg_user_id) & (UserTournament.tournament_id == tournament_id),
            )
            .outerjoin(Point, Point.tg_user_id == User.tg_user_id)
            .outerjoin(Match, Match.id == Point.match_id)
            .where((Match.id.is_(None)) | ((Match.tournament_id == tournament_id) & (Match.source == "manual")))
            .group_by(User.tg_user_id, UserTournament.display_name, User.display_name, User.username, User.full_name)
            .order_by(func.coalesce(func.sum(Point.points), 0).desc())
        )

        rows = []
        for tg_user_id, tournament_display_name, user_display_name, username, full_name, total, exact, diff, outcome in q.all():
            rows.append(
                {
                    "tg_user_id": tg_user_id,
                    "name": format_user_name(tournament_display_name, user_display_name, username, full_name, tg_user_id),
                    "total": int(total),
                    "exact": int(exact),
                    "diff": int(diff),
                    "outcome": int(outcome),
                }
            )

        return rows, participants


async def build_round_leaderboard(round_number: int, tournament_id: int) -> tuple[list[dict], int]:
    async with SessionLocal() as session:
        participants_q = await session.execute(
            select(func.count(func.distinct(Prediction.tg_user_id)))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(Match.round_number == round_number, Match.source == "manual", Match.tournament_id == tournament_id)
        )
        participants = int(participants_q.scalar_one())

        q = await session.execute(
            select(
                User.tg_user_id,
                UserTournament.display_name,
                User.display_name,
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
            .outerjoin(
                UserTournament,
                (UserTournament.tg_user_id == User.tg_user_id) & (UserTournament.tournament_id == tournament_id),
            )
            .outerjoin(Point, (Point.tg_user_id == User.tg_user_id) & (Point.match_id == Match.id))
            .where(Match.round_number == round_number, Match.source == "manual", Match.tournament_id == tournament_id)
            .group_by(User.tg_user_id, UserTournament.display_name, User.display_name, User.username, User.full_name)
            .order_by(func.coalesce(func.sum(Point.points), 0).desc())
        )

        rows = []
        for tg_user_id, tournament_display_name, user_display_name, username, full_name, total, exact, diff, outcome in q.all():
            rows.append(
                {
                    "tg_user_id": tg_user_id,
                    "name": format_user_name(tournament_display_name, user_display_name, username, full_name, tg_user_id),
                    "total": int(total),
                    "exact": int(exact),
                    "diff": int(diff),
                    "outcome": int(outcome),
                }
            )

        return rows, participants


async def build_round_matches_text(round_number: int, tournament_id: int, tournament_name: str, now: datetime | None = None) -> str:
    if now is None:
        now = now_msk_naive()

    async with SessionLocal() as session:
        result = await session.execute(
            select(Match)
            .where(
                Match.round_number == round_number,
                Match.source == "manual",
                Match.tournament_id == tournament_id,
            )
            .order_by(Match.kickoff_time.asc())
        )
        matches = result.scalars().all()

    if not matches:
        return (
            f"В туре {round_number} пока нет матчей.\n"
            "Проверь соседний тур или загляни позже — расписание может обновиться."
        )

    lines = [f"📅 {tournament_name} · Тур {round_number} (МСК)"]
    for m in matches:
        icon = match_status_icon(m, now)
        score = ""
        if m.home_score is not None and m.away_score is not None:
            score = f" | {m.home_score}:{m.away_score}"
        lines.append(f"{icon} {m.home_team} — {m.away_team} | {m.kickoff_time.strftime('%d.%m %H:%M')}{score}")
    lines.append("")
    lines.append("🟢 прогноз открыт · 🔒 прогноз закрыт · ✅ есть итог")
    return "\n".join(lines)


async def build_profile_text(tg_user_id: int, tournament_id: int, tournament_name: str) -> str:
    async with SessionLocal() as session:
        user_q = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
        user = user_q.scalar_one_or_none()
        if user is None:
            return "Похоже, ты ещё не в турнире. Нажми «✅ Вступить в турнир», и поехали."

        rank_q = await session.execute(
            select(
                User.tg_user_id,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
            )
            .select_from(User)
            .outerjoin(Point, Point.tg_user_id == User.tg_user_id)
            .outerjoin(Match, Match.id == Point.match_id)
            .where((Match.id.is_(None)) | ((Match.source == "manual") & (Match.tournament_id == tournament_id)))
            .group_by(User.tg_user_id)
            .order_by(func.coalesce(func.sum(Point.points), 0).desc(), User.tg_user_id.asc())
        )
        ranking = rank_q.all()

        place = None
        total = exact = diff = outcome = 0
        for i, row in enumerate(ranking, start=1):
            if int(row[0]) == tg_user_id:
                place = i
                total = int(row[1] or 0)
                exact = int(row[2] or 0)
                diff = int(row[3] or 0)
                outcome = int(row[4] or 0)
                break
        if place is None:
            place = len(ranking) + 1

        preds_q = await session.execute(
            select(func.count(Prediction.id))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(Prediction.tg_user_id == tg_user_id, Match.source == "manual", Match.tournament_id == tournament_id)
        )
        preds_count = int(preds_q.scalar_one() or 0)

        rounds_q = await session.execute(
            select(
                Match.round_number,
                func.coalesce(func.sum(Point.points), 0).label("pts"),
            )
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(Point.tg_user_id == tg_user_id, Match.source == "manual", Match.tournament_id == tournament_id)
            .group_by(Match.round_number)
            .order_by(Match.round_number.desc())
        )
        rounds = rounds_q.all()

        streak_q = await session.execute(
            select(Match.kickoff_time, Point.points)
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(
                Point.tg_user_id == tg_user_id,
                Match.source == "manual",
                Match.tournament_id == tournament_id,
            )
            .order_by(Match.kickoff_time.asc(), Match.id.asc())
        )
        streak_rows = streak_q.all()

        ut_q = await session.execute(
            select(UserTournament).where(
                UserTournament.tg_user_id == tg_user_id,
                UserTournament.tournament_id == tournament_id,
            )
        )
        ut = ut_q.scalar_one_or_none()

    avg_per_round = round((total / len(rounds)), 2) if rounds else 0.0
    form = " | ".join([f"Т{int(r[0])}:{int(r[1])}" for r in rounds[:3]]) if rounds else "нет данных"
    tournament_display_name = ut.display_name if ut is not None else None
    name = format_user_name(tournament_display_name, user.display_name, user.username, user.full_name, tg_user_id)

    current_streak = 0
    best_streak = 0
    for _kickoff_time, pts in streak_rows:
        if int(pts or 0) > 0:
            current_streak += 1
            if current_streak > best_streak:
                best_streak = current_streak
        else:
            current_streak = 0

    return (
        f"👤 Профиль: {name}\n"
        f"Турнир: {tournament_name}\n"
        f"Место в общем зачёте: {place}\n"
        f"Очки: {total}\n"
        f"Прогнозов: {preds_count}\n"
        f"🎯{exact} | 📏{diff} | ✅{outcome}\n"
        f"🔥 Текущая серия (матчи с очками): {current_streak}\n"
        f"🏅 Лучшая серия: {best_streak}\n"
        f"Средние очки за тур: {avg_per_round}\n"
        f"Форма (последние туры): {form}\n\n"
        "Хочешь подняться выше? Открой «📅 Матчи тура» и добавь свежие прогнозы."
    )


async def build_mvp_round_text(round_number: int, tournament_id: int, tournament_name: str) -> str:
    rows, participants = await build_round_leaderboard(round_number, tournament_id=tournament_id)
    if not rows:
        return (
            f"В туре {round_number} пока нет данных для MVP.\n"
            "Как только появятся результаты и очки, сразу покажу лучших."
        )
    best = rows[0]["total"]
    winners = [r for r in rows if r["total"] == best]
    lines = [f"🏅 MVP тура {round_number} ({tournament_name})", ""]
    lines.append(f"Лучший результат тура: {best} очк.")
    if len(winners) == 1:
        lines.append(f"MVP: {winners[0]['name']}")
    else:
        lines.append("MVP разделили:")
        for w in winners[:3]:
            lines.append(f"• {w['name']}")
    lines.append("")
    lines.append("Топ-3 тура:")
    for i, r in enumerate(rows[:3], start=1):
        lines.append(f"{i}. {r['name']} — {r['total']} очк.")
    lines.append(f"Участников в туре: {participants}")
    lines.append("")
    lines.append("Хочешь попасть сюда? Жми «🎯 Поставить прогноз».")
    return "\n".join(lines)


async def build_round_tops_text(round_number: int, tournament_id: int, tournament_name: str) -> str:
    rows, participants = await build_round_leaderboard(round_number, tournament_id=tournament_id)
    if not rows:
        return (
            f"В туре {round_number} пока нет данных для топов.\n"
            "Сначала нужны прогнозы и результаты матчей."
        )

    def top_by(key: str) -> list[dict]:
        mx = max(int(r[key]) for r in rows)
        return [r for r in rows if int(r[key]) == mx and mx > 0]

    exact_top = top_by("exact")
    diff_top = top_by("diff")
    outcome_top = top_by("outcome")

    def names(items: list[dict]) -> str:
        return ", ".join(i["name"] for i in items[:3]) if items else "—"

    breakthrough_line = "—"
    prev_rows, _prev_participants = await build_round_leaderboard(round_number - 1, tournament_id=tournament_id)
    prev_map = {int(r["tg_user_id"]): int(r["total"]) for r in prev_rows}
    deltas: list[tuple[int, int, str]] = []  # (delta, total, name)
    for r in rows:
        uid = int(r["tg_user_id"])
        if uid in prev_map:
            delta = int(r["total"]) - int(prev_map[uid])
            deltas.append((delta, int(r["total"]), r["name"]))
    if deltas:
        deltas.sort(key=lambda x: (x[0], x[1]), reverse=True)
        best_delta, best_total, best_name = deltas[0]
        if best_delta > 0:
            breakthrough_line = f"{best_name} — +{best_delta} к прошлому туру ({best_total} очк.)"
        else:
            breakthrough_line = f"{rows[0]['name']} — {rows[0]['total']} очк. (лучший результат тура)"
    elif rows:
        breakthrough_line = f"{rows[0]['name']} — {rows[0]['total']} очк. (лучший результат тура)"

    lines = [f"⭐ Топы тура {round_number} ({tournament_name})", f"Участников: {participants}", ""]
    lines.append(f"🎯 Снайпер тура: {names(exact_top)}")
    lines.append(f"📏 Мастер разницы: {names(diff_top)}")
    lines.append(f"✅ Король исходов: {names(outcome_top)}")
    lines.append(f"🚀 Прорыв тура: {breakthrough_line}")
    lines.append("")
    lines.append("Хочешь залететь в топы? Вперёд в «🎯 Поставить прогноз».")
    return "\n".join(lines)


async def build_round_digest_text(round_number: int, tournament_id: int, tournament_name: str) -> str:
    rows, participants = await build_round_leaderboard(round_number, tournament_id=tournament_id)
    if not rows:
        return (
            f"В туре {round_number} пока нет данных для итогов.\n"
            "Как только появятся результаты и очки, соберу красивую сводку."
        )

    def top_by(key: str) -> list[dict]:
        mx = max(int(r[key]) for r in rows)
        return [r for r in rows if int(r[key]) == mx and mx > 0]

    exact_top = top_by("exact")
    diff_top = top_by("diff")
    outcome_top = top_by("outcome")

    def names(items: list[dict]) -> str:
        return ", ".join(i["name"] for i in items[:3]) if items else "—"

    best = int(rows[0]["total"])
    mvp_names = ", ".join(r["name"] for r in rows if int(r["total"]) == best)
    prev_rows, _ = await build_round_leaderboard(round_number - 1, tournament_id=tournament_id)
    prev_map = {int(r["tg_user_id"]): int(r["total"]) for r in prev_rows}
    breakthrough_line = f"{rows[0]['name']} — {rows[0]['total']} очк."
    deltas: list[tuple[int, int, str]] = []
    for r in rows:
        uid = int(r["tg_user_id"])
        if uid in prev_map:
            delta = int(r["total"]) - int(prev_map[uid])
            deltas.append((delta, int(r["total"]), r["name"]))
    if deltas:
        deltas.sort(key=lambda x: (x[0], x[1]), reverse=True)
        d, t, n = deltas[0]
        if d > 0:
            breakthrough_line = f"{n} — +{d} к прошлому туру ({t} очк.)"

    lines = [f"🏁 Итоги тура {round_number} ({tournament_name})", ""]
    lines.append(f"🏅 MVP: {mvp_names} — {best} очк.")
    lines.append(f"🎯 Топ точных: {names(exact_top)}")
    lines.append(f"📏 Топ разницы: {names(diff_top)}")
    lines.append(f"✅ Топ исходов: {names(outcome_top)}")
    lines.append(f"🚀 Прорыв тура: {breakthrough_line}")
    lines.append("")
    lines.append(f"Участников в туре: {participants}")
    lines.append("Следующий тур открыт. Время ставить прогнозы: «🎯 Поставить прогноз».")
    return "\n".join(lines)


def register_user_handlers(dp: Dispatcher):
    async def _get_user_tournament_context(tg_user_id: int) -> tuple[Tournament, int]:
        async with SessionLocal() as session:
            tournament = await get_selected_tournament_for_user(session, tg_user_id)
        default_round = await get_current_round_default(
            tournament_id=tournament.id,
            round_min=tournament.round_min,
            round_max=tournament.round_max,
        )
        return tournament, default_round

    def _round_in_tournament(round_number: int, tournament: Tournament) -> bool:
        return tournament.round_min <= round_number <= tournament.round_max

    async def _require_membership_or_hint(message: types.Message, tournament: Tournament) -> bool:
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, message.from_user.id, tournament.id)
        if ok:
            return True
        await message.answer(
            f"Ты пока не в турнире {tournament.name}.\n"
            "Нажми «✅ Вступить в турнир» — и можно сразу ставить прогнозы."
        )
        return False

    @dp.message(F.text == "🇷🇺 РПЛ")
    async def btn_switch_rpl(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            t = await set_selected_tournament_for_user(session, message.from_user.id, "RPL")
        if t is None:
            await message.answer("Турнир РПЛ не найден в базе.")
            return
        default_round = await get_current_round_default(t.id, t.round_min, t.round_max)
        await message.answer(f"Переключено на турнир: {t.name}\nТекущий тур: {default_round}")

    @dp.message(F.text == "🇬🇧 АПЛ")
    async def btn_switch_epl(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            t = await set_selected_tournament_for_user(session, message.from_user.id, "EPL")
        if t is None:
            await message.answer("Турнир АПЛ не найден в базе.")
            return
        default_round = await get_current_round_default(t.id, t.round_min, t.round_max)
        await message.answer(f"Переключено на турнир: {t.name}\nТекущий тур: {default_round}")

    async def _send_help_text(message: types.Message) -> None:
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            "❓ Помощь\n\n"
            f"Сейчас ты в турнире: {tournament.name}\n"
            f"Диапазон туров: {tournament.round_min}..{tournament.round_max}\n\n"
            "Самый удобный путь — кнопки внизу:\n"
            "✅ Вступить в турнир\n"
            "📅 Матчи тура\n"
            "🎯 Поставить прогноз\n"
            "🗂 Мои прогнозы\n"
            "🏆 Общая таблица\n"
            "📊 Статистика\n"
            "👤 Мой профиль\n"
            "📘 Правила\n\n"
            "Дополнительно командами:\n"
            "/round N\n"
            "/my N\n"
            "/table_round N\n"
            "/history\n"
            "/mvp_round N\n"
            "/tops_round N\n\n"
            "/round_digest N\n\n"
            f"Стартовый тур сейчас: {default_round}\n"
            "Если что-то не получается, просто напиши команду ещё раз — подскажу формат."
        )

    async def _open_predict_round(message: types.Message, state: FSMContext, round_number: int, tournament: Tournament) -> None:
        now = now_msk_naive()
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            q = await session.execute(
                select(Match)
                .where(
                    Match.round_number == round_number,
                    Match.source == "manual",
                    Match.tournament_id == tournament.id,
                )
                .order_by(Match.kickoff_time.asc())
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
            "ID счёт\n"
            "Пример:\n"
            "1 2:0\n2 1:1\n\n"
            "Открытые матчи:"
        ]
        for m in open_matches:
            icon = match_status_icon(m, now)
            lines.append(f"{icon} ID {m.id}: {m.home_team} — {m.away_team} ({m.kickoff_time.strftime('%Y-%m-%d %H:%M')} МСК)")

        await state.set_state(PredictRoundStates.waiting_for_predictions_block)
        await state.update_data(round_number=round_number)
        await send_long(message, "\n".join(lines))

    async def _request_display_name_for_join(message: types.Message, state: FSMContext, tournament: Tournament) -> None:
        await state.set_state(PredictRoundStates.waiting_for_display_name)
        await state.update_data(join_tournament_id=tournament.id, join_tournament_name=tournament.name)
        await message.answer(
            f"Вступление в {tournament.name}.\n"
            "Введи имя для таблицы (2-24 символа).\n"
            "Пример: Роман"
        )

    async def _send_default_round_text(target: types.Message, tg_user_id: int) -> None:
        tournament, default_round = await _get_user_tournament_context(tg_user_id)
        await send_long(
            target,
            await build_round_matches_text(default_round, tournament_id=tournament.id, tournament_name=tournament.name),
        )

    async def _send_default_my_text(target: types.Message, tg_user_id: int) -> None:
        tournament, default_round = await _get_user_tournament_context(tg_user_id)
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, tg_user_id, tournament.id)
        if not ok:
            await target.answer(
                f"Сначала зайди в турнир {tournament.name} кнопкой «✅ Вступить в турнир»,"
                " и сразу сможем показать твои прогнозы."
            )
            return

        text = await build_my_round_text(tg_user_id=tg_user_id, round_number=default_round, tournament_id=tournament.id)
        if await round_has_matches(default_round, tournament_id=tournament.id):
            total = await get_round_total_points_for_user(
                tg_user_id=tg_user_id, round_number=default_round, tournament_id=tournament.id
            )
            text = (
                f"{text}\n\n"
                f"Итого за тур сейчас: {total} очк.\n"
                "Хочешь добить оставшиеся матчи? Жми «🎯 Поставить прогноз»."
            )
        await send_long(target, text)
        await target.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_my"))

    async def _send_quick_predict_picker(target: types.Message, tg_user_id: int) -> None:
        tournament, default_round = await _get_user_tournament_context(tg_user_id)
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, tg_user_id, tournament.id)
            if not ok:
                await target.answer(
                    f"Сначала зайди в турнир {tournament.name} кнопкой «✅ Вступить в турнир»,"
                    " и сразу сможем сохранить прогноз."
                )
                return

            now = now_msk_naive()
            q = await session.execute(
                select(Match)
                .where(
                    Match.round_number == default_round,
                    Match.source == "manual",
                    Match.tournament_id == tournament.id,
                    Match.kickoff_time > now,
                )
                .order_by(Match.kickoff_time.asc())
            )
            open_matches = q.scalars().all()

        if not open_matches:
            await target.answer(
                f"На тур {default_round} открытых матчей уже нет.\n"
                f"Загляни в следующий: /round {default_round + 1}"
            )
            return

        await target.answer(
            f"Выбери матч тура {default_round}, затем просто отправь счёт (например: 2:1).",
            reply_markup=build_open_matches_inline_keyboard(open_matches),
        )

    @dp.callback_query(F.data.startswith("qnav:"))
    async def on_quick_nav(callback: types.CallbackQuery, state: FSMContext):
        data = callback.data or ""
        action = data.split(":", 1)[1] if ":" in data else ""
        if action == "my":
            await _send_default_my_text(callback.message, callback.from_user.id)
        elif action == "round":
            await _send_default_round_text(callback.message, callback.from_user.id)
        elif action == "predict":
            await _send_quick_predict_picker(callback.message, callback.from_user.id)
        elif action == "table":
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            played, total = await get_matches_played_stats(tournament_id=tournament.id)
            rows, participants = await build_overall_leaderboard(tournament_id=tournament.id)
            if not rows:
                await callback.message.answer(
                    "Пока в таблице пусто — ещё нет прогнозов.\n"
                    "Можешь открыть сезон первым через «🎯 Поставить прогноз»."
                )
            else:
                lines = [f"🏆 {tournament.name} · Таблица лидеров"]
                lines.append(f"Участников с прогнозами: {participants}")
                lines.append(f"Матчей сыграно: {played} / {total}")
                for i, r in enumerate(rows[:20], start=1):
                    lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")
                lines.append("")
                lines.append("Хочешь проверить свои ставки? Жми «🗂 Мои прогнозы».")
                await send_long(callback.message, "\n".join(lines))
                await callback.message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))
        await callback.answer()

    @dp.message(F.text == "✅ Вступить в турнир")
    async def btn_join(message: types.Message, state: FSMContext):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
        await _request_display_name_for_join(message, state, tournament)

    @dp.message(F.text == "📅 Матчи тура")
    async def btn_round(message: types.Message):
        await _send_default_round_text(message, message.from_user.id)

    @dp.message(F.text == "🗂 Мои прогнозы")
    async def btn_my(message: types.Message):
        await _send_default_my_text(message, message.from_user.id)

    @dp.message(F.text == "🏆 Общая таблица")
    async def btn_table(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        played, total = await get_matches_played_stats(tournament_id=tournament.id)
        rows, participants = await build_overall_leaderboard(tournament_id=tournament.id)
        if not rows:
            await message.answer(
                "Пока в таблице пусто — никто ещё не поставил прогнозы.\n"
                "Ты можешь открыть гонку первым: «🎯 Поставить прогноз»."
            )
            return
        lines = [f"🏆 {tournament.name} · Таблица лидеров", f"Участников с прогнозами: {participants}", f"Матчей сыграно: {played} / {total}"]
        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")
        lines.append("")
        lines.append("Нужен следующий шаг? Открой «🗂 Мои прогнозы» или поставь новый через «🎯 Поставить прогноз».")
        await send_long(message, "\n".join(lines))
        await message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))

    @dp.message(F.text == "📊 Статистика")
    async def btn_stats(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        await send_long(message, await build_stats_text(tournament_id=tournament.id))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text == "👤 Мой профиль")
    async def btn_profile(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        if not await _require_membership_or_hint(message, tournament):
            return
        await message.answer(await build_profile_text(message.from_user.id, tournament_id=tournament.id, tournament_name=tournament.name))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text == "🗓 История туров")
    async def btn_history(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            f"🗂 История туров · {tournament.name}\nВыбери тур — покажу матчи и статус прогнозов.",
            reply_markup=build_round_history_keyboard(tournament.round_min, tournament.round_max),
        )

    @dp.message(F.text == "🥇 MVP тура")
    async def btn_mvp(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(await build_mvp_round_text(default_round, tournament_id=tournament.id, tournament_name=tournament.name))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text == "⭐ Топы тура")
    async def btn_tops(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(await build_round_tops_text(default_round, tournament_id=tournament.id, tournament_name=tournament.name))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text == "❓ Помощь")
    async def btn_help(message: types.Message):
        await _send_help_text(message)

    @dp.message(Command("history"))
    async def cmd_history(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            f"🗂 История туров · {tournament.name}\nВыбери тур — покажу матчи и статус прогнозов.",
            reply_markup=build_round_history_keyboard(tournament.round_min, tournament.round_max),
        )

    @dp.callback_query(F.data.startswith("history_round:"))
    async def on_history_round(callback: types.CallbackQuery):
        data = callback.data or ""
        try:
            round_number = int(data.split(":", 1)[1])
        except Exception:
            await callback.answer("Не получилось выбрать тур, попробуй ещё раз.", show_alert=True)
            return
        tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer("Этот тур вне диапазона выбранного турнира.", show_alert=True)
            return
        text = await build_round_matches_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name)
        await callback.message.answer(text)
        await callback.message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))
        await callback.answer()

    @dp.message(Command("profile"))
    async def cmd_profile(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        if not await _require_membership_or_hint(message, tournament):
            return
        text = await build_profile_text(message.from_user.id, tournament_id=tournament.id, tournament_name=tournament.name)
        await message.answer(text)
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(Command("mvp_round"))
    async def cmd_mvp_round(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        parts = (message.text or "").strip().split()
        if len(parts) == 1:
            round_number = default_round
        elif len(parts) == 2:
            try:
                round_number = int(parts[1])
            except ValueError:
                await message.answer(f"Номер тура должен быть числом. Пример: /mvp_round {default_round}")
                return
        else:
            await message.answer(f"Формат: /mvp_round {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"Можно использовать только туры {tournament.round_min}..{tournament.round_max}. Пример: /mvp_round {default_round}"
            )
            return

        await message.answer(await build_mvp_round_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(Command("tops_round"))
    async def cmd_tops_round(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        parts = (message.text or "").strip().split()
        if len(parts) == 1:
            round_number = default_round
        elif len(parts) == 2:
            try:
                round_number = int(parts[1])
            except ValueError:
                await message.answer(f"Номер тура должен быть числом. Пример: /tops_round {default_round}")
                return
        else:
            await message.answer(f"Формат: /tops_round {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"Можно использовать только туры {tournament.round_min}..{tournament.round_max}. Пример: /tops_round {default_round}"
            )
            return

        await message.answer(await build_round_tops_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(Command("round_digest"))
    async def cmd_round_digest(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        parts = (message.text or "").strip().split()
        if len(parts) == 1:
            round_number = default_round
        elif len(parts) == 2:
            try:
                round_number = int(parts[1])
            except ValueError:
                await message.answer(f"Номер тура нужен числом. Пример: /round_digest {default_round}")
                return
        else:
            await message.answer(f"Формат: /round_digest {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"В этом турнире доступны только туры {tournament.round_min}..{tournament.round_max}.\n"
                f"Попробуй: /round_digest {default_round}"
            )
            return

        await message.answer(await build_round_digest_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name))
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text == "📘 Правила")
    async def quick_rules(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            "📘 Правила турнира (коротко)\n\n"
            f"Турнир: {tournament.name}\n"
            f"Туры: {tournament.round_min}..{tournament.round_max}\n"
            "Очки:\n"
            "🎯 точный счёт — 4\n"
            "📏 разница + исход — 2\n"
            "✅ только исход — 1\n"
            "❌ мимо — 0\n\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n"
            "🕒 Время матчей и дедлайны — по Москве (МСК).\n\n"
            "Дальше проще всего так: «📅 Матчи тура» → «🎯 Поставить прогноз»."
        )
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text == "🎯 Поставить прогноз")
    async def quick_predict_hint(message: types.Message):
        await _send_quick_predict_picker(message, message.from_user.id)

    @dp.callback_query(F.data.startswith("pick_match:"))
    async def on_pick_match(callback: types.CallbackQuery, state: FSMContext):
        data = callback.data or ""
        try:
            match_id = int(data.split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать матч", show_alert=True)
            return

        now = now_msk_naive()
        async with SessionLocal() as session:
            tournament = await get_selected_tournament_for_user(session, callback.from_user.id)
            q = await session.execute(
                select(Match).where(
                    Match.id == match_id,
                    Match.source == "manual",
                    Match.tournament_id == tournament.id,
                )
            )
            match = q.scalar_one_or_none()

        if match is None:
            await callback.answer("Матч не найден", show_alert=True)
            return

        if match.kickoff_time <= now:
            await callback.answer("Прогноз уже закрыт", show_alert=True)
            return

        await state.set_state(PredictRoundStates.waiting_for_single_match_score)
        await state.update_data(single_match_id=match.id)
        await callback.message.answer(
            f"Матч выбран: {match.home_team} — {match.away_team}\n"
            "Отправь только счёт: 2:1"
        )
        await callback.answer()

    @dp.message(PredictRoundStates.waiting_for_single_match_score)
    async def on_single_match_score(message: types.Message, state: FSMContext):
        data = await state.get_data()
        match_id = data.get("single_match_id")
        if not match_id:
            await state.clear()
            await message.answer("Похоже, сессия сбилась. Нажми «🎯 Поставить прогноз» и попробуй ещё раз.")
            return

        parsed = parse_score(normalize_score(message.text or ""))
        if parsed is None:
            await message.answer("Не смог прочитать счёт. Отправь только формат `2:1`.")
            return
        pred_home, pred_away = parsed

        tg_user_id = message.from_user.id
        now = now_msk_naive()
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)

            q = await session.execute(
                select(Match).where(Match.id == int(match_id), Match.source == "manual", Match.tournament_id == tournament.id)
            )
            match = q.scalar_one_or_none()
            if match is None:
                await state.clear()
                await message.answer("Не нашёл этот матч. Нажми «🎯 Поставить прогноз» и выбери его из списка.")
                return

            if match.kickoff_time <= now:
                await state.clear()
                await message.answer("🔒 На этот матч прогноз уже закрыт. Можно выбрать другой открытый матч.")
                return

            pred_q = await session.execute(
                select(Prediction).where(Prediction.tg_user_id == tg_user_id, Prediction.match_id == match.id)
            )
            pred = pred_q.scalar_one_or_none()
            if pred is None:
                session.add(
                    Prediction(
                        tg_user_id=tg_user_id,
                        match_id=match.id,
                        pred_home=pred_home,
                        pred_away=pred_away,
                    )
                )
            else:
                pred.pred_home = pred_home
                pred.pred_away = pred_away

            await session.commit()

        await state.clear()
        await message.answer(
            f"✅ Прогноз: {match.home_team} — {match.away_team} | {pred_home}:{pred_away}",
            reply_markup=build_quick_nav_keyboard("after_predict"),
        )

    @dp.message(PredictRoundStates.waiting_for_display_name)
    async def on_display_name_input(message: types.Message, state: FSMContext):
        display_name = normalize_display_name(message.text or "")
        if display_name is None:
            await message.answer("Имя должно быть длиной 2-24 символа. Попробуй ещё раз.")
            return

        data = await state.get_data()
        tournament_id = int(data.get("join_tournament_id") or 0)
        tournament_name = str(data.get("join_tournament_name") or "")
        new_join = False

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)

            user_q = await session.execute(select(User).where(User.tg_user_id == message.from_user.id))
            user = user_q.scalar_one_or_none()
            if user is None:
                await state.clear()
                await message.answer("Не удалось обновить профиль. Попробуй /join ещё раз.")
                return

            tournament = None
            if tournament_id > 0:
                t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
                tournament = t_q.scalar_one_or_none()
            if tournament is None:
                tournament = await get_selected_tournament_for_user(session, message.from_user.id)

            exists_before = await is_user_in_tournament(session, message.from_user.id, tournament.id)
            await ensure_user_membership(
                session,
                message.from_user.id,
                tournament.id,
                display_name=display_name,
            )
            new_join = not exists_before
            await session.commit()

        await state.clear()
        t_name = tournament_name or tournament.name
        await message.answer(
            f"✅ Ты в турнире: {t_name}\n"
            f"Имя в таблице: {display_name}"
        )
        if new_join:
            await notify_admins_new_join(
                bot=message.bot,
                tg_user_id=message.from_user.id,
                username=message.from_user.username,
                display_name=display_name,
                tournament_name=t_name,
            )

    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            "🏆 Добро пожаловать в бот прогнозов РПЛ и АПЛ.\n\n"
            "Как начать (3 шага):\n"
            "1) Выбери турнир кнопкой: 🇷🇺 РПЛ или 🇬🇧 АПЛ\n"
            "2) Нажми «✅ Вступить в турнир» и введи имя для таблицы\n"
            "3) Открой «📅 Матчи тура» и поставь прогноз через «🎯 Поставить прогноз»\n\n"
            f"Сейчас выбран турнир: {tournament.name}\n"
            f"Текущий тур: {default_round}\n\n"
            "Очки:\n"
            "🎯 точный счёт — 4\n"
            "📏 разница + исход — 2\n"
            "✅ только исход — 1\n"
            "❌ мимо — 0\n\n"
            "Важно:\n"
            "🕒 Время матчей и дедлайны — по Москве (МСК).\n"
            "⛔️ После начала матча прогноз ставить/менять нельзя.\n"
            "✅ Можно вводить счет как 2:0 или 2-0.",
            reply_markup=build_main_menu_keyboard(default_round=default_round),
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        await _send_help_text(message)

    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅ На связи!")

    @dp.message(Command("chatid"))
    async def cmd_chatid(message: types.Message):
        chat = message.chat
        title = getattr(chat, "title", None) or getattr(chat, "full_name", None) or "private"
        await message.answer(
            f"chat_id: {chat.id}\n"
            f"type: {chat.type}\n"
            f"title: {title}"
        )

    @dp.message(Command("join"))
    async def cmd_join(message: types.Message, state: FSMContext):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
        await _request_display_name_for_join(message, state, tournament)

    @dp.message(Command("round"))
    async def cmd_round(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Чуть не так. Попробуй формат: /round {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура нужен числом. Пример: /round {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"В этом турнире доступны только туры {tournament.round_min}..{tournament.round_max}.\n"
                f"Попробуй: /round {default_round}"
            )
            return

        await send_long(message, await build_round_matches_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name))

    @dp.message(Command("predict"))
    async def cmd_predict(message: types.Message):
        parts = message.text.strip().split()
        if len(parts) != 3:
            await message.answer("Почти! Формат такой: /predict 1 2:0")
            return

        try:
            match_id = int(parts[1])
        except ValueError:
            await message.answer("ID матча должен быть числом. Пример: /predict 1 2:0")
            return

        score_str = normalize_score(parts[2])
        parsed = parse_score(score_str)
        if parsed is None:
            await message.answer("Счёт нужен в формате 2:0 (или 2-0).")
            return

        pred_home, pred_away = parsed
        tg_user_id = message.from_user.id
        now = now_msk_naive()

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
            if not await is_user_in_tournament(session, message.from_user.id, tournament.id):
                await message.answer(
                    f"Сначала зайди в турнир {tournament.name} кнопкой «✅ Вступить в турнир»,"
                    " и сразу сможем сохранить прогноз."
                )
                return

            match_q = await session.execute(select(Match).where(Match.id == match_id, Match.tournament_id == tournament.id))
            match = match_q.scalar_one_or_none()
            if match is None:
                await message.answer("Не нашёл такой матч в выбранном турнире. Проверь ID через «📅 Матчи тура».")
                return

            if match.kickoff_time <= now:
                await message.answer("🔒 На этот матч уже поздно: игра началась. Выбери другой открытый матч.")
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

        await message.answer(f"✅ Прогноз: {match.home_team} — {match.away_team} | {pred_home}:{pred_away}")
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_predict"))

    @dp.message(Command("predict_round"))
    async def cmd_predict_round(message: types.Message, state: FSMContext):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        if not await _require_membership_or_hint(message, tournament):
            return
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Для тура нужен формат: /predict_round {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура нужен числом. Пример: /predict_round {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"В этом турнире доступны только туры {tournament.round_min}..{tournament.round_max}.\n"
                f"Попробуй: /predict_round {default_round}"
            )
            return

        await _open_predict_round(message, state, round_number, tournament)

    @dp.message(PredictRoundStates.waiting_for_predictions_block)
    async def handle_predictions_block(message: types.Message, state: FSMContext):
        data = await state.get_data()
        round_number = data.get("round_number")
        if not round_number:
            await state.clear()
            await message.answer("⚠️ Сессия сбилась. Начни заново: /predict_round N")
            return

        tg_user_id = message.from_user.id
        now = now_msk_naive()

        lines = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
        if not lines:
            await message.answer("Сообщение пустое. Пришли строки в формате: `ID счёт`.")
            return

        saved = 0
        skipped = 0
        errors = 0

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)

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
                    select(Match).where(
                        Match.id == match_id,
                        Match.round_number == round_number,
                        Match.source == "manual",
                        Match.tournament_id == tournament.id,
                    )
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
        await message.answer(
            f"✅ Готово! Сохранено: {saved} | Пропущено: {skipped} | Ошибок: {errors}\n"
            "Проверить всё можно через «🗂 Мои прогнозы»."
        )
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_predict"))

    @dp.message(Command("my"))
    async def cmd_my(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        if not await _require_membership_or_hint(message, tournament):
            return
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Для просмотра тура используй формат: /my {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура нужен числом. Пример: /my {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"В этом турнире доступны только туры {tournament.round_min}..{tournament.round_max}.\n"
                f"Попробуй: /my {default_round}"
            )
            return

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)

        tg_user_id = message.from_user.id
        text = await build_my_round_text(tg_user_id=tg_user_id, round_number=round_number, tournament_id=tournament.id)

        if await round_has_matches(round_number, tournament_id=tournament.id):
            total = await get_round_total_points_for_user(
                tg_user_id=tg_user_id, round_number=round_number, tournament_id=tournament.id
            )
            text = (
                f"{text}\n\n"
                f"Итого за тур сейчас: {total} очк.\n"
                "Если хочешь, можно сразу продолжить через «🎯 Поставить прогноз»."
            )

        await send_long(message, text)
        await message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_my"))

    @dp.message(Command("table"))
    async def cmd_table(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        played, total = await get_matches_played_stats(tournament_id=tournament.id)
        rows, participants = await build_overall_leaderboard(tournament_id=tournament.id)

        if not rows:
            await message.answer(
                "Пока в таблице пусто — ещё нет прогнозов.\n"
                "Можешь открыть сезон первым через «🎯 Поставить прогноз»."
            )
            return

        lines = [f"🏆 {tournament.name} · Таблица лидеров"]
        lines.append(f"Участников с прогнозами: {participants}")
        lines.append(f"Матчей сыграно: {played} / {total}")

        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")
        lines.append("")
        lines.append("Хочешь проверить свои ставки? Жми «🗂 Мои прогнозы».")

        await send_long(message, "\n".join(lines))
        await message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))

    @dp.message(Command("table_round"))
    async def cmd_table_round(message: types.Message):
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        parts = message.text.strip().split()
        if len(parts) != 2:
            await message.answer(f"Для таблицы тура используй формат: /table_round {default_round}")
            return

        try:
            round_number = int(parts[1])
        except ValueError:
            await message.answer(f"Номер тура нужен числом. Пример: /table_round {default_round}")
            return

        if not _round_in_tournament(round_number, tournament):
            await message.answer(
                f"В этом турнире доступны только туры {tournament.round_min}..{tournament.round_max}.\n"
                f"Попробуй: /table_round {default_round}"
            )
            return

        rows, participants = await build_round_leaderboard(round_number, tournament_id=tournament.id)
        if not rows:
            await message.answer("На этот тур пока нет прогнозов. Можно стать первым 😉")
            return

        lines = [f"🏁 {tournament.name} · Таблица тура {round_number}:"]
        lines.append(f"Участников с прогнозами в туре: {participants}")

        for i, r in enumerate(rows[:20], start=1):
            lines.append(f"{i}. {r['name']} — {r['total']} очк. | 🎯{r['exact']} | 📏{r['diff']} | ✅{r['outcome']}")
        lines.append("")
        lines.append("Хочешь ворваться выше? Открой «🎯 Поставить прогноз».")

        await send_long(message, "\n".join(lines))
        await message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        text = await build_stats_text(tournament_id=tournament.id)
        await send_long(message, text)
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))
