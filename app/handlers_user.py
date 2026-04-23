from aiogram import Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import case, func, select

from datetime import datetime, timedelta
import os
import re
from urllib.parse import urlencode

from app.config import load_admin_ids
from app.db import SessionLocal
from app.display import display_round_name, display_team_name, display_tournament_name
from app.duels import respond_duel
from app.league_table import build_active_stage_league_table, get_user_stage_scope
from app.models import League, LeagueMovement, Match, Point, Prediction, Setting, Stage, Tournament, User, UserTournament
from app.season_setup import is_enrollment_open
from app.stats import build_stats_text
from app.my_predictions import build_my_round_text
from app.audience import unmark_user_blocked

ADMIN_IDS = load_admin_ids()


class PredictRoundStates(StatesGroup):
    waiting_for_predictions_block = State()
    waiting_for_single_match_score = State()
    waiting_for_display_name = State()


DEFAULT_TOURNAMENT_CODE = "RPL"
DEFAULT_RPL_ROUND_MIN = 1
DEFAULT_RPL_ROUND_MAX = 30
WC_TOURNAMENT_CODE = "WC2026"
DEFAULT_WC_ROUND_MIN = 1
DEFAULT_WC_ROUND_MAX = 64
TOURNAMENT_SELECTED_KEY_PREFIX = "TOURNAMENT_SELECTED_U"
MINIAPP_WEB_URL = os.getenv("MINIAPP_WEB_URL", "https://rpl-predictions-bot-mini-app.onrender.com").strip()


def build_main_menu_keyboard(
    default_round: int,
    is_joined: bool,
    join_cta_text: str = "✅ Вступить в турнир",
) -> types.ReplyKeyboardRemove:
    # Режим "только Mini App": полностью скрываем reply-клавиатуру.
    return types.ReplyKeyboardRemove()


def build_start_join_wc_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="ВСТУПИТЬ В ТУРНИР", callback_data="start_join_wc")]
        ]
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


def _left_tournament_key(tournament_id: int, tg_user_id: int) -> str:
    return f"LEFT_T{int(tournament_id)}_U{int(tg_user_id)}"


def _left_tournament_name_key(tournament_id: int, tg_user_id: int) -> str:
    return f"LEFT_NAME_T{int(tournament_id)}_U{int(tg_user_id)}"


async def _get_setting(session, key: str) -> str | None:
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    return row.value if row is not None else None


async def _set_setting(session, key: str, value: str) -> None:
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is None:
        session.add(Setting(key=key, value=value))
    else:
        row.value = value


async def _delete_setting(session, key: str) -> None:
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is not None:
        await session.delete(row)


async def _get_join_cta_text(session, tg_user_id: int, tournament_id: int) -> str:
    left_flag = await _get_setting(session, _left_tournament_key(tournament_id, tg_user_id))
    if (left_flag or "").strip() == "1":
        return "🔄 Вернуться в турнир"
    return "✅ Вступить в турнир"


def _selected_tournament_key(tg_user_id: int) -> str:
    return f"{TOURNAMENT_SELECTED_KEY_PREFIX}{int(tg_user_id)}"


async def _ensure_tournament(
    session,
    code: str,
    name: str,
    round_min: int,
    round_max: int,
) -> Tournament:
    q = await session.execute(select(Tournament).where(Tournament.code == code))
    t = q.scalar_one_or_none()
    if t is None:
        t = Tournament(code=code, name=name, round_min=round_min, round_max=round_max, is_active=1)
        session.add(t)
        await session.flush()
    return t


async def ensure_default_tournaments(session) -> tuple[Tournament, Tournament]:
    rpl = await _ensure_tournament(
        session=session,
        code=DEFAULT_TOURNAMENT_CODE,
        name="РПЛ",
        round_min=DEFAULT_RPL_ROUND_MIN,
        round_max=DEFAULT_RPL_ROUND_MAX,
    )
    wc = await _ensure_tournament(
        session=session,
        code=WC_TOURNAMENT_CODE,
        name="ЧМ 2026",
        round_min=DEFAULT_WC_ROUND_MIN,
        round_max=DEFAULT_WC_ROUND_MAX,
    )
    return rpl, wc


async def get_available_tournaments(session) -> list[Tournament]:
    await ensure_default_tournaments(session)
    q = await session.execute(
        select(Tournament)
        .where(Tournament.is_active == 1)
        .order_by(case((Tournament.code == DEFAULT_TOURNAMENT_CODE, 0), else_=1), Tournament.code.asc())
    )
    return list(q.scalars().all())


async def set_selected_tournament_for_user(session, tg_user_id: int, tournament_code: str) -> None:
    code = (tournament_code or "").strip().upper()
    await _set_setting(session, _selected_tournament_key(tg_user_id), code)


async def get_selected_tournament_for_user(session, tg_user_id: int) -> Tournament:
    await ensure_default_tournaments(session)
    selected_code = (await _get_setting(session, _selected_tournament_key(tg_user_id)) or "").strip().upper()
    if not selected_code:
        selected_code = DEFAULT_TOURNAMENT_CODE

    q = await session.execute(
        select(Tournament).where(Tournament.code == selected_code, Tournament.is_active == 1).limit(1)
    )
    t = q.scalar_one_or_none()
    if t is None:
        fallback_q = await session.execute(
            select(Tournament)
            .where(Tournament.is_active == 1)
            .order_by(case((Tournament.code == DEFAULT_TOURNAMENT_CODE, 0), else_=1), Tournament.code.asc())
            .limit(1)
        )
        t = fallback_q.scalar_one_or_none()
        if t is None:
            # safety fallback
            t = await _ensure_tournament(
                session=session,
                code=DEFAULT_TOURNAMENT_CODE,
                name="РПЛ",
                round_min=DEFAULT_RPL_ROUND_MIN,
                round_max=DEFAULT_RPL_ROUND_MAX,
            )
        await set_selected_tournament_for_user(session, tg_user_id, t.code)

    if (t.code or "").upper() == DEFAULT_TOURNAMENT_CODE and (
        int(t.round_min) != DEFAULT_RPL_ROUND_MIN or int(t.round_max) != DEFAULT_RPL_ROUND_MAX
    ):
        t.round_min = DEFAULT_RPL_ROUND_MIN
        t.round_max = DEFAULT_RPL_ROUND_MAX
        await session.commit()
        await session.refresh(t)
    return t


def get_effective_round_window(tournament: Tournament) -> tuple[int, int]:
    if (tournament.code or "").upper() == DEFAULT_TOURNAMENT_CODE:
        return DEFAULT_RPL_ROUND_MIN, DEFAULT_RPL_ROUND_MAX
    lo = int(tournament.round_min)
    hi = int(tournament.round_max)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


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


def _truncate_button_text(text: str, max_len: int = 64) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def build_open_matches_inline_keyboard(
    matches: list[Match],
    with_kickoff: bool = False,
    footer_rows: list[list[types.InlineKeyboardButton]] | None = None,
) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    for m in matches:
        if with_kickoff:
            label = (
                f"{display_team_name(m.home_team)} — {display_team_name(m.away_team)} "
                f"| {m.kickoff_time.strftime('%d.%m %H:%M')}"
            )
        else:
            label = f"{display_team_name(m.home_team)} — {display_team_name(m.away_team)}"
        btn = types.InlineKeyboardButton(
            text=_truncate_button_text(label),
            callback_data=f"pick_match:{m.id}",
        )
        rows.append([btn])

    if footer_rows:
        rows.extend(footer_rows)

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


def build_round_picker_inline(prefix: str, round_min: int, round_max: int) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    row: list[types.InlineKeyboardButton] = []
    for r in range(round_min, round_max + 1):
        row.append(types.InlineKeyboardButton(text=str(r), callback_data=f"{prefix}:{r}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def build_tournament_picker_inline(
    tournaments: list[Tournament],
    selected_code: str | None = None,
) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    selected = (selected_code or "").strip().upper()
    for t in tournaments:
        code = (t.code or "").strip().upper()
        marker = "⭐ " if code == selected else ""
        title = f"{marker}{display_tournament_name(t.name)} ({code})"
        rows.append([types.InlineKeyboardButton(text=title, callback_data=f"pick_tournament:{code}")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def build_stats_followup_keyboard() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="🥇 MVP тура", callback_data="qnav:mvp_pick"),
                types.InlineKeyboardButton(text="⭐ Топы тура", callback_data="qnav:tops_pick"),
            ],
            [types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict")],
        ]
    )


def build_quick_nav_keyboard(kind: str) -> types.InlineKeyboardMarkup:
    if kind == "after_predict":
        rows = [
            [
                types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data="qnav:my"),
                types.InlineKeyboardButton(text="🏆 Общая таблица", callback_data="qnav:table"),
            ],
            [types.InlineKeyboardButton(text="🎯 Ещё прогноз", callback_data="qnav:predict")],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_predict_done":
        rows = [
            [
                types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data="qnav:my"),
                types.InlineKeyboardButton(text="🏆 Общая таблица", callback_data="qnav:table"),
            ],
            [types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict")],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_table":
        rows = [
            [
                types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict"),
                types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data="qnav:my"),
            ],
            [types.InlineKeyboardButton(text="📅 Таблица по турам", callback_data="qnav:table_pick")],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_my":
        rows = [
            [
                types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict"),
                types.InlineKeyboardButton(text="📚 Выбрать тур", callback_data="qnav:my_pick"),
            ],
        ]
        return types.InlineKeyboardMarkup(inline_keyboard=rows)

    if kind == "after_info":
        rows = [
            [
                types.InlineKeyboardButton(text="🎯 Поставить прогноз", callback_data="qnav:predict"),
                types.InlineKeyboardButton(text="🏆 Общая таблица", callback_data="qnav:table"),
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


def _format_leaderboard_row(i: int, row: dict) -> str:
    return (
        f"{i}. {row.get('name', '')} — {int(row.get('total', 0))} очк. | "
        f"🎯{int(row.get('exact', 0))} | 📏{int(row.get('diff', 0))} | ✅{int(row.get('outcome', 0))}"
    )


def _overall_table_story_line(rows: list[dict], played: int, total: int) -> str:
    if not rows:
        return "Гонка ещё не началась."
    if played == 0:
        return "Гонка только начинается — каждый матч может перевернуть таблицу 👀"
    leader_pts = int(rows[0]["total"])
    second_pts = int(rows[1]["total"]) if len(rows) > 1 else 0
    gap = leader_pts - second_pts
    if total > 0 and played >= total:
        return "Все матчи сыграны — финальная развязка перед тобой 🏁"
    if gap == 0:
        return "Плотнейшая борьба наверху — лидер определяется по дополнительным показателям 🔥"
    if gap <= 2:
        return "Борьба за лидерство очень плотная — один удачный матч всё меняет ⚡"
    return f"У лидера небольшой отрыв ({gap} очк.), но он совсем не безопасный 👀"


def _round_table_story_line(rows: list[dict], played: int, total: int) -> str:
    if not rows:
        return "Пока нет прогнозов на этот тур."
    if total == 0:
        return "Матчи тура ещё не загружены."
    if played == 0:
        return "Тур только стартует — борьба за MVP впереди 🔥"
    if played < total:
        return "MVP тура ещё в игре — несколько матчей могут всё поменять 🔥"
    return "Тур завершён — итоговая раскладка тура зафиксирована ✅"


def _build_overall_user_summary(rows: list[dict], current_user_id: int, top_visible: int = 5) -> str | None:
    if not rows:
        return None
    me_idx = None
    for idx, row in enumerate(rows, start=1):
        if int(row.get("tg_user_id", 0)) == int(current_user_id):
            me_idx = idx
            me = row
            break
    if me_idx is None:
        return None

    leader_pts = int(rows[0]["total"])
    my_pts = int(me["total"])
    gap_to_leader = max(leader_pts - my_pts, 0)

    if me_idx == 1:
        return "Ты сейчас на 1 месте 👑\nЗадача — удержать лидерство в следующем матче."

    if me_idx <= top_visible:
        return f"Ты сейчас: {me_idx} место, отставание от лидера — {gap_to_leader} очк."

    top_n = min(top_visible, len(rows))
    top_cut_pts = int(rows[top_n - 1]["total"])
    gap_to_top = max(top_cut_pts - my_pts, 0)
    return (
        f"Твоя позиция: {me_idx} место — {my_pts} очк.\n"
        f"До топ-{top_n}: {gap_to_top} очк. Всё ещё очень реально 💪"
    )


def _build_overall_table_lines(
    tournament_name: str,
    rows: list[dict],
    participants: int,
    played: int,
    total: int,
    current_user_id: int,
    limit: int = 20,
) -> list[str]:
    lines = [f"🏆 {tournament_name} · Таблица лидеров"]
    lines.append(f"Участников с прогнозами: {participants}")
    lines.append(f"Матчей сыграно: {played} / {total}")
    lines.append("")
    lines.append(_overall_table_story_line(rows, played, total))
    lines.append("")
    for i, r in enumerate(rows[:limit], start=1):
        lines.append(_format_leaderboard_row(i, r))
    lines.append("")
    me_line = _build_overall_user_summary(rows, current_user_id=current_user_id)
    if me_line:
        lines.append(me_line)
    else:
        lines.append("Хочешь проверить свои ставки? Жми «🗂 Мои прогнозы».")
    return lines


def _build_stage_league_table_lines(
    season_name: str,
    stage_name: str,
    league_name: str,
    rows: list[dict],
    participants: int,
    played: int,
    total: int,
    current_user_id: int,
    limit: int = 20,
) -> list[str]:
    lines = [f"🏆 {league_name} · {stage_name}"]
    lines.append(f"Сезон: {season_name}")
    lines.append(f"Участников в лиге: {participants}")
    lines.append(f"Матчей сыграно: {played} / {total}")
    lines.append("")
    lines.append(_overall_table_story_line(rows, played, total))
    lines.append("")
    for i, r in enumerate(rows[:limit], start=1):
        lines.append(_format_leaderboard_row(i, r))
    lines.append("")
    me_line = _build_overall_user_summary(rows, current_user_id=current_user_id)
    if me_line:
        lines.append(me_line)
    return lines


def _build_round_table_lines(
    tournament_name: str,
    round_number: int,
    rows: list[dict],
    participants: int,
    played: int,
    total: int,
    limit: int = 20,
) -> list[str]:
    lines = [f"🏁 {tournament_name} · Таблица тура {round_number}"]
    lines.append(f"Участников с прогнозами в туре: {participants}")
    lines.append("")
    lines.append(_round_table_story_line(rows, played, total))
    lines.append("")
    for i, r in enumerate(rows[:limit], start=1):
        lines.append(_format_leaderboard_row(i, r))
    lines.append("")
    lines.append("Хочешь ворваться выше? Открой «🎯 Поставить прогноз».")
    return lines


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

    # Если пользователь снова пишет боту, значит он не в blocked-состоянии.
    await unmark_user_blocked(session, tg_user_id)
    await session.commit()


async def upsert_user_from_callback(session, callback: types.CallbackQuery):
    tg_user_id = callback.from_user.id
    username = callback.from_user.username
    full_name = f"{callback.from_user.first_name or ''} {callback.from_user.last_name or ''}".strip() or None

    existing = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
    user = existing.scalar_one_or_none()

    if user is None:
        user = User(tg_user_id=tg_user_id, username=username, full_name=full_name)
        session.add(user)
    else:
        user.username = username
        user.full_name = full_name

    # Если пользователь нажал кнопку у бота, значит он не в blocked-состоянии.
    await unmark_user_blocked(session, tg_user_id)
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


def normalize_team_token(s: str) -> str:
    s = (s or "").strip().lower().replace("ё", "е")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def resolve_match_by_team_names(home_raw: str, away_raw: str, matches: list[Match]) -> Match | None:
    home_norm = normalize_team_token(home_raw)
    away_norm = normalize_team_token(away_raw)
    if not home_norm or not away_norm:
        return None

    found: list[Match] = []
    for m in matches:
        home_aliases = {
            normalize_team_token(m.home_team),
            normalize_team_token(display_team_name(m.home_team)),
        }
        away_aliases = {
            normalize_team_token(m.away_team),
            normalize_team_token(display_team_name(m.away_team)),
        }
        if home_norm in home_aliases and away_norm in away_aliases:
            found.append(m)

    if len(found) != 1:
        return None
    return found[0]


def parse_bulk_prediction_line(line: str, open_matches: list[Match]) -> tuple[Match | None, int | None, int | None]:
    txt = (line or "").strip()
    if not txt:
        return None, None, None

    # Формат: "1 2:1" или "1. 2-1"
    m = re.match(r"^\s*(\d+)\.?\s+(\d+)\s*[:\-]\s*(\d+)\s*$", txt)
    if m:
        idx = int(m.group(1))
        pred_home = int(m.group(2))
        pred_away = int(m.group(3))
        if 1 <= idx <= len(open_matches):
            return open_matches[idx - 1], pred_home, pred_away
        # Обратная совместимость: если ввели ID, а не номер.
        by_id = {int(mm.id): mm for mm in open_matches}
        match = by_id.get(idx)
        if match is not None:
            return match, pred_home, pred_away
        return None, None, None

    # Формат: "Ростов 2-1 Балтика"
    m = re.match(r"^\s*(.+?)\s+(\d+)\s*[:\-]\s*(\d+)\s+(.+?)\s*$", txt)
    if m:
        home_raw = m.group(1).strip()
        pred_home = int(m.group(2))
        pred_away = int(m.group(3))
        away_raw = m.group(4).strip()
        match = resolve_match_by_team_names(home_raw, away_raw, open_matches)
        if match is not None:
            return match, pred_home, pred_away
        return None, None, None

    # Формат: "Ростов - Балтика 2:1"
    m = re.match(r"^\s*(.+?)\s*[-—]\s*(.+?)\s+(\d+)\s*[:\-]\s*(\d+)\s*$", txt)
    if m:
        home_raw = m.group(1).strip()
        away_raw = m.group(2).strip()
        pred_home = int(m.group(3))
        pred_away = int(m.group(4))
        match = resolve_match_by_team_names(home_raw, away_raw, open_matches)
        if match is not None:
            return match, pred_home, pred_away
        return None, None, None

    return None, None, None


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
                Match.tournament_id == tournament_id,
            )
        )
        return int(q.scalar_one())


async def get_matches_played_stats(
    tournament_id: int,
    round_min: int | None = None,
    round_max: int | None = None,
) -> tuple[int, int]:
    async with SessionLocal() as session:
        total_stmt = select(func.count(Match.id)).where(Match.tournament_id == tournament_id)
        if round_min is not None:
            total_stmt = total_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            total_stmt = total_stmt.where(Match.round_number <= round_max)
        total_q = await session.execute(total_stmt)
        total = int(total_q.scalar_one())

        played_stmt = select(func.count(Match.id)).where(
            Match.home_score.isnot(None),
            Match.away_score.isnot(None),
            Match.tournament_id == tournament_id,
        )
        if round_min is not None:
            played_stmt = played_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            played_stmt = played_stmt.where(Match.round_number <= round_max)
        played_q = await session.execute(played_stmt)
        played = int(played_q.scalar_one())

    return played, total


async def get_round_matches_played_stats(round_number: int, tournament_id: int) -> tuple[int, int]:
    async with SessionLocal() as session:
        total_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.round_number == round_number,
                Match.tournament_id == tournament_id,
            )
        )
        total = int(total_q.scalar_one() or 0)

        played_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.round_number == round_number,
                Match.tournament_id == tournament_id,
                Match.home_score.isnot(None),
                Match.away_score.isnot(None),
            )
        )
        played = int(played_q.scalar_one() or 0)
    return played, total


async def build_overall_leaderboard(
    tournament_id: int,
    round_min: int | None = None,
    round_max: int | None = None,
) -> tuple[list[dict], int]:
    async with SessionLocal() as session:
        # Только участники, у которых есть хотя бы 1 прогноз (по сути — есть points или predictions)
        participants_stmt = (
            select(func.count(func.distinct(Prediction.tg_user_id)))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(Match.tournament_id == tournament_id)
        )
        if round_min is not None:
            participants_stmt = participants_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            participants_stmt = participants_stmt.where(Match.round_number <= round_max)
        participants_q = await session.execute(participants_stmt)
        participants = int(participants_q.scalar_one())

        participants_subq_stmt = (
            select(Prediction.tg_user_id.label("tg_user_id"))
            .join(Match, Match.id == Prediction.match_id)
            .where(Match.tournament_id == tournament_id)
        )
        if round_min is not None:
            participants_subq_stmt = participants_subq_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            participants_subq_stmt = participants_subq_stmt.where(Match.round_number <= round_max)
        participants_subq = participants_subq_stmt.distinct().subquery()

        tournament_points_stmt = (
            select(
                Point.tg_user_id.label("tg_user_id"),
                Point.points.label("points"),
                Point.category.label("category"),
            )
            .join(Match, Match.id == Point.match_id)
            .where(Match.tournament_id == tournament_id)
        )
        if round_min is not None:
            tournament_points_stmt = tournament_points_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            tournament_points_stmt = tournament_points_stmt.where(Match.round_number <= round_max)
        tournament_points_subq = tournament_points_stmt.subquery()

        q = await session.execute(
            select(
                User.tg_user_id,
                UserTournament.display_name,
                UserTournament.bonus_points,
                User.display_name,
                User.username,
                User.full_name,
                func.coalesce(func.sum(tournament_points_subq.c.points), 0).label("total"),
                func.coalesce(func.sum(case((tournament_points_subq.c.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((tournament_points_subq.c.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((tournament_points_subq.c.category == "outcome", 1), else_=0)), 0).label("outcome"),
            )
            .select_from(participants_subq)
            .join(User, User.tg_user_id == participants_subq.c.tg_user_id)
            .outerjoin(
                UserTournament,
                (UserTournament.tg_user_id == User.tg_user_id) & (UserTournament.tournament_id == tournament_id),
            )
            .outerjoin(tournament_points_subq, tournament_points_subq.c.tg_user_id == User.tg_user_id)
            .group_by(
                User.tg_user_id,
                UserTournament.display_name,
                UserTournament.bonus_points,
                User.display_name,
                User.username,
                User.full_name,
            )
            .order_by(
                (func.coalesce(func.sum(tournament_points_subq.c.points), 0) + func.coalesce(UserTournament.bonus_points, 0)).desc(),
                User.tg_user_id.asc(),
            )
        )

        rows = []
        for (
            tg_user_id,
            tournament_display_name,
            bonus_points,
            user_display_name,
            username,
            full_name,
            total,
            exact,
            diff,
            outcome,
        ) in q.all():
            rows.append(
                {
                    "tg_user_id": tg_user_id,
                    "name": format_user_name(tournament_display_name, user_display_name, username, full_name, tg_user_id),
                    "total": int(total) + int(bonus_points or 0),
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
            .where(Match.round_number == round_number, Match.tournament_id == tournament_id)
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
            .where(Match.round_number == round_number, Match.tournament_id == tournament_id)
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

    tournament_code = ""
    async with SessionLocal() as session:
        tq = await session.execute(select(Tournament.code).where(Tournament.id == tournament_id))
        tournament_code = str(tq.scalar_one_or_none() or "")

    lines = [f"📅 {tournament_name} · {display_round_name(tournament_code, round_number)} (МСК)"]
    for m in matches:
        icon = match_status_icon(m, now)
        score = ""
        if m.home_score is not None and m.away_score is not None:
            score = f" | {m.home_score}:{m.away_score}"
        grp = f"[{m.group_label}] " if (m.group_label or "").strip() else ""
        lines.append(
            f"{icon} {grp}{display_team_name(m.home_team)} — {display_team_name(m.away_team)} "
            f"| {m.kickoff_time.strftime('%d.%m %H:%M')}{score}"
        )
    lines.append("")
    lines.append("🟢 прогноз открыт · 🔒 прогноз закрыт · ✅ есть итог")
    return "\n".join(lines)


async def build_profile_text(
    tg_user_id: int,
    tournament_id: int,
    tournament_name: str,
    round_min: int | None = None,
    round_max: int | None = None,
) -> str:
    scope = await get_user_stage_scope(tg_user_id)
    if scope is not None:
        round_min = scope.stage_round_min
        round_max = scope.stage_round_max

    async with SessionLocal() as session:
        user_q = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
        user = user_q.scalar_one_or_none()
        if user is None:
            return "Похоже, ты ещё не в турнире. Нажми «✅ Вступить в турнир», и поехали."

        preds_stmt = (
            select(func.count(Prediction.id))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(Prediction.tg_user_id == tg_user_id, Match.tournament_id == tournament_id)
        )
        if round_min is not None:
            preds_stmt = preds_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            preds_stmt = preds_stmt.where(Match.round_number <= round_max)
        preds_q = await session.execute(preds_stmt)
        preds_count = int(preds_q.scalar_one() or 0)

        rounds_stmt = (
            select(
                Match.round_number,
                func.coalesce(func.sum(Point.points), 0).label("pts"),
            )
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(Point.tg_user_id == tg_user_id, Match.tournament_id == tournament_id)
            .group_by(Match.round_number)
            .order_by(Match.round_number.desc())
        )
        if round_min is not None:
            rounds_stmt = rounds_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            rounds_stmt = rounds_stmt.where(Match.round_number <= round_max)
        rounds_q = await session.execute(rounds_stmt)
        rounds = rounds_q.all()

        streak_stmt = (
            select(Match.kickoff_time, Point.points)
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(
                Point.tg_user_id == tg_user_id,
                Match.tournament_id == tournament_id,
            )
            .order_by(Match.kickoff_time.asc(), Match.id.asc())
        )
        if round_min is not None:
            streak_stmt = streak_stmt.where(Match.round_number >= round_min)
        if round_max is not None:
            streak_stmt = streak_stmt.where(Match.round_number <= round_max)
        streak_q = await session.execute(streak_stmt)
        streak_rows = streak_q.all()

        ut_q = await session.execute(
            select(UserTournament).where(
                UserTournament.tg_user_id == tg_user_id,
                UserTournament.tournament_id == tournament_id,
            )
        )
        ut = ut_q.scalar_one_or_none()

    leaderboard_rows, meta = await build_active_stage_league_table(tg_user_id)

    place = "—"
    total = exact = diff = outcome = 0
    if meta is not None:
        for i, row in enumerate(leaderboard_rows, start=1):
            if int(row["tg_user_id"]) == int(tg_user_id):
                place = i
                total = int(row["total"])
                exact = int(row["exact"])
                diff = int(row["diff"])
                outcome = int(row["outcome"])
                break

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

    if place == 1:
        profile_status = "👑 Лидер гонки"
        profile_hint = "Ты впереди. Главное сейчас — удержать темп и не дать соперникам приблизиться."
    elif total > 0 and current_streak >= 3:
        profile_status = "🟢 В огне"
        profile_hint = "Серия с очками идёт отлично — можно замахнуться на серьёзный рывок."
    elif total > 0:
        profile_status = "🟡 Разогрев"
        profile_hint = "Ты уже нащупал ритм — следующий точный счёт может резко поднять тебя выше."
    elif preds_count > 0:
        profile_status = "⚪ Стартуем"
        profile_hint = "Прогнозы уже стоят. Ждём первые результаты — и таблица оживёт."
    else:
        profile_status = "🆕 Вход в игру"
        profile_hint = "Сделай первый прогноз через «🎯 Поставить прогноз» — и сразу включишься в гонку."

    stage_line = "нет данных"
    league_line = "нет данных"
    if scope is not None:
        stage_line = f"{scope.stage_name} (туры {scope.stage_round_min}-{scope.stage_round_max})"
        league_line = scope.league_name

    return (
        f"👤 Профиль: {name}\n"
        f"Лига: {league_line}\n"
        f"Этап: {stage_line}\n"
        f"Место в лиге: {place}\n"
        f"Очки этапа: {total}\n"
        f"Прогнозов: {preds_count}\n"
        f"🎯{exact} | 📏{diff} | ✅{outcome}\n"
        f"🔥 Текущая серия (матчи с очками): {current_streak}\n"
        f"🏅 Лучшая серия: {best_streak}\n"
        f"Средние очки за тур: {avg_per_round}\n"
        f"Форма (последние туры): {form}\n\n"
        f"Статус: {profile_status}\n"
        f"{profile_hint}\n\n"
        "Хочешь подняться выше? Открой «🎯 Поставить прогноз» и добавь свежие прогнозы."
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


async def get_round_prediction_progress_for_user(
    tg_user_id: int,
    round_number: int,
    tournament_id: int,
) -> tuple[int, int, int]:
    async with SessionLocal() as session:
        now = now_msk_naive()
        matches_q = await session.execute(
            select(Match.id, Match.kickoff_time).where(
                Match.round_number == round_number,
                Match.tournament_id == tournament_id,
            )
        )
        rows = [(int(mid), kickoff) for mid, kickoff in matches_q.all()]
        all_match_ids = [mid for mid, _kickoff in rows]
        if not all_match_ids:
            return 0, 0, 0

        open_match_ids = [mid for mid, kickoff in rows if kickoff > now]

        preds_q = await session.execute(
            select(Prediction.match_id).where(
                Prediction.tg_user_id == tg_user_id,
                Prediction.match_id.in_(all_match_ids),
            )
        )
        predicted_match_ids = {int(x[0]) for x in preds_q.all()}

        predicted_open = sum(1 for mid in open_match_ids if mid in predicted_match_ids)
        total_open = len(open_match_ids)
        missed_closed = sum(1 for mid, kickoff in rows if kickoff <= now and mid not in predicted_match_ids)
        return predicted_open, total_open, missed_closed


async def build_my_moves_text(tg_user_id: int, limit: int = 8) -> str:
    async with SessionLocal() as session:
        q = await session.execute(
            select(LeagueMovement)
            .where(LeagueMovement.tg_user_id == tg_user_id)
            .order_by(LeagueMovement.id.desc())
            .limit(limit)
        )
        moves = q.scalars().all()
        if not moves:
            return "Пока нет переходов между лигами."

        stage_ids = {int(m.from_stage_id) for m in moves} | {int(m.to_stage_id) for m in moves}
        league_ids = {int(m.from_league_id) for m in moves} | {int(m.to_league_id) for m in moves}
        stages_q = await session.execute(select(Stage.id, Stage.name).where(Stage.id.in_(stage_ids)))
        leagues_q = await session.execute(select(League.id, League.name).where(League.id.in_(league_ids)))
        stage_map = {int(sid): str(name) for sid, name in stages_q.all()}
        league_map = {int(lid): str(name) for lid, name in leagues_q.all()}

    lines = ["🔁 Мои переходы между лигами:"]
    for m in moves:
        reason = str(m.reason or "")
        icon = "⬆️" if reason == "promotion" else "⬇️" if reason == "relegation" else "➡️"
        from_league = league_map.get(int(m.from_league_id), str(m.from_league_id))
        to_league = league_map.get(int(m.to_league_id), str(m.to_league_id))
        from_stage = stage_map.get(int(m.from_stage_id), str(m.from_stage_id))
        to_stage = stage_map.get(int(m.to_stage_id), str(m.to_stage_id))
        lines.append(f"{icon} {from_league} -> {to_league} ({from_stage} -> {to_stage})")
    return "\n".join(lines)


def register_user_handlers(dp: Dispatcher):
    async def _get_user_tournament_context(tg_user_id: int) -> tuple[Tournament, int]:
        async with SessionLocal() as session:
            tournament = await get_selected_tournament_for_user(session, tg_user_id)
        tournament.name = display_tournament_name(tournament.name)
        eff_round_min, eff_round_max = get_effective_round_window(tournament)
        # Дальше в UI/запросах используем уже безопасное окно туров.
        tournament.round_min = eff_round_min
        tournament.round_max = eff_round_max
        default_round = await get_current_round_default(
            tournament_id=tournament.id,
            round_min=eff_round_min,
            round_max=eff_round_max,
        )
        return tournament, default_round

    def _round_in_tournament(round_number: int, tournament: Tournament) -> bool:
        return tournament.round_min <= round_number <= tournament.round_max

    def _my_round_followup_line(predicted_open: int, total_open: int, missed_closed: int) -> str:
        if total_open <= 0:
            if missed_closed > 0:
                return (
                    f"Все оставшиеся матчи уже закрыты.\n"
                    f"Пропущено закрытых матчей: {missed_closed}."
                )
            return "Как только появятся матчи, можно будет сразу поставить прогноз."
        left = max(total_open - predicted_open, 0)
        if left == 0:
            if missed_closed > 0:
                return (
                    "Все доступные матчи тура заполнены ✅\n"
                    f"Пропущено закрытых матчей: {missed_closed}."
                )
            return "Все доступные матчи тура заполнены ✅\nОстаётся ждать результатов."
        extra = f"\nПропущено закрытых матчей: {missed_closed}." if missed_closed > 0 else ""
        return (
            f"Осталось поставить ещё {left} доступн(ых) матч(а/ей).\n"
            f"Можешь быстро добить через «🎯 Поставить прогноз».{extra}"
        )

    async def _build_predict_saved_message(
        tg_user_id: int,
        tournament_id: int,
        round_number: int,
        home_team: str,
        away_team: str,
        pred_home: int,
        pred_away: int,
    ) -> tuple[str, str]:
        predicted_open, total_open, missed_closed = await get_round_prediction_progress_for_user(
            tg_user_id=tg_user_id,
            round_number=round_number,
            tournament_id=tournament_id,
        )
        left = max(total_open - predicted_open, 0)
        if left == 0 and total_open > 0:
            missed_line = f"\nПропущено закрытых матчей: {missed_closed}." if missed_closed > 0 else ""
            text = (
                f"✅ Прогноз принят: {home_team} — {away_team} | {pred_home}:{pred_away}\n\n"
                f"Готово! Ты закрыл все доступные матчи тура {round_number} ✅\n"
                f"Теперь ждём результаты и считаем очки.{missed_line}"
            )
            return text, "after_predict_done"

        missed_line = f"\nЗакрытых пропусков сейчас: {missed_closed}." if missed_closed > 0 else ""
        text = (
            f"✅ Прогноз принят: {home_team} — {away_team} | {pred_home}:{pred_away}\n\n"
            f"Осталось поставить ещё {left} доступн(ых) матч(а/ей) в туре {round_number}.\n"
            f"Успеешь добить сейчас? 👇{missed_line}"
        )
        return text, "after_predict"

    async def _require_membership_or_hint(message: types.Message, tournament: Tournament) -> bool:
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, message.from_user.id, tournament.id)
        if ok:
            return True
        async with SessionLocal() as session:
            join_cta_text = await _get_join_cta_text(session, message.from_user.id, tournament.id)
        _tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            f"Ты пока не в турнире {tournament.name}.\n"
            "Нажми «✅ Вступить в турнир» — после этого можно сразу ставить прогнозы.",
            reply_markup=build_main_menu_keyboard(
                default_round=default_round,
                is_joined=False,
                join_cta_text=join_cta_text,
            ),
        )
        return False

    async def _send_help_text(message: types.Message) -> None:
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            "❓ Помощь\n\n"
            f"Сейчас ты в турнире: {tournament.name}\n"
            f"Диапазон туров: {tournament.round_min}..{tournament.round_max}\n\n"
            "Если впервые:\n"
            "1) ✅ Вступить в турнир\n"
            "2) Открой «🎯 Поставить прогноз»\n"
            "3) Поставь прогноз через «🎯 Поставить прогноз»\n\n"
            "Самый удобный путь — кнопки внизу:\n"
            "✅ Вступить в турнир\n"
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

    async def _send_bulk_predict_prompt(
        target: types.Message,
        state: FSMContext,
        tg_user_id: int,
        tournament: Tournament,
        round_number: int,
    ) -> None:
        now = now_msk_naive()
        tournament_name = display_tournament_name(tournament.name)
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, tg_user_id, tournament.id)
            if not ok:
                join_cta_text = await _get_join_cta_text(session, tg_user_id, tournament.id)
                await target.answer(
                    f"Сначала вступи в турнир {tournament_name} кнопкой «✅ Вступить в турнир»,"
                    " и сразу сможем сохранить прогноз.",
                    reply_markup=build_main_menu_keyboard(
                        default_round=round_number,
                        is_joined=False,
                        join_cta_text=join_cta_text,
                    ),
                )
                return

            q = await session.execute(
                select(Match)
                .where(
                    Match.round_number == round_number,
                    Match.tournament_id == tournament.id,
                    Match.kickoff_time > now,
                )
                .order_by(Match.kickoff_time.asc())
            )
            open_matches = q.scalars().all()

        if not open_matches:
            await target.answer("В этом туре не осталось открытых матчей для прогноза.")
            return

        lines = [
            f"⚡ Проставить всё за тур\nТурнир: {tournament_name}\nТур: {round_number}\n",
            "Открытые матчи (только доступные для прогноза):",
        ]
        for i, m in enumerate(open_matches, start=1):
            lines.append(
                f"{i}) {display_team_name(m.home_team)} — {display_team_name(m.away_team)} | {m.kickoff_time.strftime('%d.%m %H:%M')}"
            )
        lines.extend(
            [
                "",
                "Отправь прогнозы одним сообщением, каждый матч с новой строки.",
                "Поддерживаемые форматы:",
                "1 2:1",
                "1. 2-1",
                "Ростов 2-1 Балтика",
                "Ростов - Балтика 2:1",
            ]
        )

        await state.set_state(PredictRoundStates.waiting_for_predictions_block)
        await state.update_data(round_number=round_number)
        await send_long(target, "\n".join(lines))

    async def _request_display_name_for_join(
        target: types.Message | types.CallbackQuery,
        state: FSMContext,
        tournament: Tournament,
    ) -> None:
        message = target.message if isinstance(target, types.CallbackQuery) else target
        if message is None:
            return
        await state.set_state(PredictRoundStates.waiting_for_display_name)
        await state.update_data(join_tournament_id=tournament.id, join_tournament_name=tournament.name)
        await message.answer(
            f"Вступление в {tournament.name}.\n"
            "Введи имя для таблицы (2-24 символа).\n"
            "Пример: Роман"
        )

    async def _ensure_enrollment_open_for_join(target: types.Message | types.CallbackQuery) -> bool:
        async with SessionLocal() as session:
            opened = await is_enrollment_open(session)
        if opened:
            return True
        if isinstance(target, types.CallbackQuery):
            if target.message is not None:
                await target.message.answer(
                    "🔒 Набор участников сейчас закрыт.\n"
                    "Дождись открытия набора от администратора."
                )
            return False
        await target.answer(
            "🔒 Набор участников сейчас закрыт.\n"
            "Дождись открытия набора от администратора."
        )
        return False

    async def _send_default_round_text(target: types.Message, tg_user_id: int) -> None:
        tournament, default_round = await _get_user_tournament_context(tg_user_id)
        await _send_round_predict_picker(
            target=target,
            tg_user_id=tg_user_id,
            tournament=tournament,
            round_number=default_round,
        )

    async def _send_round_predict_picker(
        target: types.Message,
        tg_user_id: int,
        tournament: Tournament,
        round_number: int,
    ) -> None:
        tournament_name = display_tournament_name(tournament.name)
        current_round = await get_current_round_default(
            tournament_id=tournament.id,
            round_min=tournament.round_min,
            round_max=tournament.round_max,
        )
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, tg_user_id, tournament.id)
            if not ok:
                join_cta_text = await _get_join_cta_text(session, tg_user_id, tournament.id)
                await target.answer(
                    f"Сначала вступи в турнир {tournament_name} кнопкой «✅ Вступить в турнир»,"
                    " и сразу сможем сохранить прогноз.",
                    reply_markup=build_main_menu_keyboard(
                        default_round=round_number,
                        is_joined=False,
                        join_cta_text=join_cta_text,
                    ),
                )
                return

            now = now_msk_naive()
            q = await session.execute(
                select(Match)
                .where(
                    Match.round_number == round_number,
                    Match.tournament_id == tournament.id,
                    Match.kickoff_time > now,
                )
                .order_by(Match.kickoff_time.asc())
            )
            open_matches = q.scalars().all()

            if not open_matches:
                await target.answer(
                    f"На тур {round_number} открытых матчей уже нет.\n"
                    "Можно посмотреть статусы через «🗂 Мои прогнозы»."
                )
                return

            open_ids = [m.id for m in open_matches]
            preds_q = await session.execute(
                select(Prediction.match_id).where(
                    Prediction.tg_user_id == tg_user_id,
                    Prediction.match_id.in_(open_ids),
                )
            )
            predicted_ids = {int(r[0]) for r in preds_q.all()}

        remaining = [m for m in open_matches if m.id not in predicted_ids]
        total_open = len(open_matches)
        left = len(remaining)

        if left == 0:
            await target.answer(
                f"Турнир: {tournament_name}\n"
                f"Тур: {round_number}\n"
                "Все доступные матчи уже заполнены ✅"
            )
            footer_rows = [
                [types.InlineKeyboardButton(text="⚡ Проставить всё за тур", callback_data=f"predict_bulk:{round_number}")],
                [types.InlineKeyboardButton(text="📚 Выбрать другой тур", callback_data=f"predict_rounds:{round_number}")],
                [types.InlineKeyboardButton(text="✏️ Изменить прогноз", callback_data=f"predict_edit:{round_number}")],
            ]
            if round_number != current_round:
                footer_rows.append(
                    [types.InlineKeyboardButton(text=f"🔙 Текущий тур ({current_round})", callback_data=f"predict_pick_round:{current_round}")]
                )
            await target.answer("Быстрые действия:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=footer_rows))
            return

        footer_rows = [
            [types.InlineKeyboardButton(text="⚡ Проставить всё за тур", callback_data=f"predict_bulk:{round_number}")],
            [types.InlineKeyboardButton(text="📚 Выбрать другой тур", callback_data=f"predict_rounds:{round_number}")],
            [types.InlineKeyboardButton(text="✏️ Изменить прогноз", callback_data=f"predict_edit:{round_number}")],
        ]
        if round_number != current_round:
            footer_rows.append(
                [types.InlineKeyboardButton(text=f"🔙 Текущий тур ({current_round})", callback_data=f"predict_pick_round:{current_round}")]
            )

        await target.answer(
            f"Турнир: {tournament_name}\n"
            f"Тур: {round_number}\n"
            f"Без прогноза осталось: {left} из {total_open}\n"
            "Выбери матч:",
            reply_markup=build_open_matches_inline_keyboard(remaining, with_kickoff=True, footer_rows=footer_rows),
        )

    async def _send_predict_rounds_picker(target: types.Message, tg_user_id: int, selected_round: int | None = None) -> None:
        tournament, current_round = await _get_user_tournament_context(tg_user_id)
        if selected_round is None:
            selected_round = current_round
        tournament_name = display_tournament_name(tournament.name)
        now = now_msk_naive()

        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, tg_user_id, tournament.id)
            if not ok:
                join_cta_text = await _get_join_cta_text(session, tg_user_id, tournament.id)
                await target.answer(
                    f"Сначала вступи в турнир {tournament_name} кнопкой «✅ Вступить в турнир»,"
                    " и сразу сможем сохранить прогноз.",
                    reply_markup=build_main_menu_keyboard(
                        default_round=selected_round,
                        is_joined=False,
                        join_cta_text=join_cta_text,
                    ),
                )
                return

            q = await session.execute(
                select(
                    Match.round_number,
                    func.count(Match.id).label("open_cnt"),
                )
                .where(
                    Match.tournament_id == tournament.id,
                    Match.round_number >= tournament.round_min,
                    Match.round_number <= tournament.round_max,
                    Match.kickoff_time > now,
                )
                .group_by(Match.round_number)
                .order_by(Match.round_number.asc())
            )
            round_rows = [(int(r), int(c)) for r, c in q.all()]

        if not round_rows:
            await target.answer("Сейчас нет открытых матчей для прогноза.")
            return

        rows: list[list[types.InlineKeyboardButton]] = []
        for rnd, open_cnt in round_rows:
            label = f"Тур {rnd} · открыто {open_cnt}"
            if rnd == current_round:
                label = f"⭐ {label} (текущий)"
            rows.append([types.InlineKeyboardButton(text=label, callback_data=f"predict_pick_round:{rnd}")])

        if selected_round != current_round:
            rows.append(
                [types.InlineKeyboardButton(text=f"🔙 Текущий тур ({current_round})", callback_data=f"predict_pick_round:{current_round}")]
            )

        await target.answer(
            f"Турнир: {tournament_name}\n"
            f"Выбери тур для прогноза:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows),
        )

    async def _send_round_edit_picker(
        target: types.Message,
        tg_user_id: int,
        tournament: Tournament,
        round_number: int,
    ) -> None:
        tournament_name = display_tournament_name(tournament.name)
        now = now_msk_naive()

        async with SessionLocal() as session:
            q = await session.execute(
                select(Match)
                .where(
                    Match.round_number == round_number,
                    Match.tournament_id == tournament.id,
                    Match.kickoff_time > now,
                )
                .order_by(Match.kickoff_time.asc())
            )
            open_matches = q.scalars().all()
            if not open_matches:
                await target.answer("В этом туре нет открытых матчей для редактирования.")
                return

            open_ids = [m.id for m in open_matches]
            preds_q = await session.execute(
                select(Prediction).where(
                    Prediction.tg_user_id == tg_user_id,
                    Prediction.match_id.in_(open_ids),
                )
            )
            preds = preds_q.scalars().all()
            preds_map = {int(p.match_id): p for p in preds}
            predicted_ids = set(preds_map.keys())

        editable = [m for m in open_matches if m.id in predicted_ids]
        if not editable:
            await target.answer(
                f"Турнир: {tournament_name}\n"
                f"Тур: {round_number}\n"
                "Пока нет открытых матчей с твоим прогнозом для редактирования."
            )
            return

        rows: list[list[types.InlineKeyboardButton]] = []
        for m in editable:
            p = preds_map.get(int(m.id))
            if p is None:
                continue
            label = (
                f"{display_team_name(m.home_team)} {p.pred_home}-{p.pred_away} {display_team_name(m.away_team)} "
                f"| {m.kickoff_time.strftime('%d.%m %H:%M')}"
            )
            rows.append(
                [
                    types.InlineKeyboardButton(
                        text=_truncate_button_text(label),
                        callback_data=f"pick_match:{m.id}",
                    )
                ]
            )

        await target.answer(
            f"Турнир: {tournament_name}\n"
            f"Тур: {round_number}\n"
            "Выбери матч для изменения прогноза:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=rows),
        )

    async def _send_my_round_text(
        target: types.Message,
        tg_user_id: int,
        tournament: Tournament,
        round_number: int,
    ) -> None:
        text = await build_my_round_text(tg_user_id=tg_user_id, round_number=round_number, tournament_id=tournament.id)
        await send_long(target, text)
        await target.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_my"))

    async def _send_default_my_text(target: types.Message, tg_user_id: int) -> None:
        tournament, default_round = await _get_user_tournament_context(tg_user_id)
        async with SessionLocal() as session:
            ok = await is_user_in_tournament(session, tg_user_id, tournament.id)
            join_cta_text = await _get_join_cta_text(session, tg_user_id, tournament.id)
        if not ok:
            await target.answer(
                f"Сначала вступи в турнир {tournament.name} кнопкой «✅ Вступить в турнир»,"
                " и сразу сможем показать твои прогнозы.",
                reply_markup=build_main_menu_keyboard(
                    default_round=default_round,
                    is_joined=False,
                    join_cta_text=join_cta_text,
                ),
            )
            return
        await _send_my_round_text(target, tg_user_id, tournament=tournament, round_number=default_round)

    async def _send_quick_predict_picker(target: types.Message, tg_user_id: int) -> None:
        tournament, default_round = await _get_user_tournament_context(tg_user_id)
        await _send_round_predict_picker(
            target=target,
            tg_user_id=tg_user_id,
            tournament=tournament,
            round_number=default_round,
        )

    @dp.callback_query(F.data.startswith("qnav:"))
    async def on_quick_nav(callback: types.CallbackQuery, state: FSMContext):
        data = callback.data or ""
        action = data.split(":", 1)[1] if ":" in data else ""
        if action == "my":
            await _send_default_my_text(callback.message, callback.from_user.id)
        elif action == "my_pick":
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            await callback.message.answer(
                f"Выбери тур для просмотра прогнозов ({display_tournament_name(tournament.name)}):",
                reply_markup=build_round_picker_inline("qnav_my_round", tournament.round_min, tournament.round_max),
            )
        elif action == "round":
            await _send_default_round_text(callback.message, callback.from_user.id)
        elif action == "predict":
            await _send_quick_predict_picker(callback.message, callback.from_user.id)
        elif action == "table":
            rows, meta = await build_active_stage_league_table(callback.from_user.id)
            if meta is None:
                await callback.message.answer("Сезон/этап пока не инициализирован. Обратись к администратору.")
                await callback.answer()
                return
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            played, total = await get_matches_played_stats(
                tournament_id=tournament.id,
                round_min=meta.stage_round_min,
                round_max=meta.stage_round_max,
            )
            if not rows:
                await callback.message.answer(
                    f"В лиге «{meta.league_name}» пока нет участников активного этапа."
                )
            else:
                lines = _build_stage_league_table_lines(
                    season_name=meta.season_name,
                    stage_name=meta.stage_name,
                    league_name=meta.league_name,
                    rows=rows,
                    participants=meta.participants,
                    played=played,
                    total=total,
                    current_user_id=callback.from_user.id,
                    limit=20,
                )
                await send_long(callback.message, "\n".join(lines))
                await callback.message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))
        elif action == "table_pick":
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            await callback.message.answer(
                f"Выбери тур для таблицы ({display_tournament_name(tournament.name)}):",
                reply_markup=build_round_picker_inline("qnav_table_round", tournament.round_min, tournament.round_max),
            )
        elif action == "mvp_pick":
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            await callback.message.answer(
                f"Выбери тур для MVP ({tournament.name}):",
                reply_markup=build_round_picker_inline("qnav_mvp_round", tournament.round_min, tournament.round_max),
            )
        elif action == "tops_pick":
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            await callback.message.answer(
                f"Выбери тур для топов ({tournament.name}):",
                reply_markup=build_round_picker_inline("qnav_tops_round", tournament.round_min, tournament.round_max),
            )
        elif action == "stats_full":
            tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
            scope = await get_user_stage_scope(callback.from_user.id)
            if scope is None:
                await send_long(callback.message, await build_stats_text(tournament_id=tournament.id))
            else:
                await send_long(
                    callback.message,
                    await build_stats_text(
                        tournament_id=tournament.id,
                        round_min=scope.stage_round_min,
                        round_max=scope.stage_round_max,
                        allowed_user_ids=set(scope.member_ids),
                        title=f"📊 Статистика · {scope.league_name} · {scope.stage_name}",
                    ),
                )
            await callback.message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))
        await callback.answer()

    @dp.callback_query(F.data.startswith("qnav_my_round:"))
    async def on_qnav_my_round(callback: types.CallbackQuery):
        try:
            round_number = int((callback.data or "").split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return

        tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer("Этот тур недоступен", show_alert=True)
            return

        await _send_my_round_text(
            callback.message,
            callback.from_user.id,
            tournament=tournament,
            round_number=round_number,
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("qnav_table_round:"))
    async def on_qnav_table_round(callback: types.CallbackQuery):
        try:
            round_number = int((callback.data or "").split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return

        tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer("Этот тур недоступен", show_alert=True)
            return

        rows, participants = await build_round_leaderboard(round_number, tournament_id=tournament.id)
        if not rows:
            await callback.message.answer("На этот тур пока нет прогнозов. Можно стать первым 😉")
            await callback.answer()
            return

        played, total = await get_round_matches_played_stats(round_number=round_number, tournament_id=tournament.id)
        lines = _build_round_table_lines(
            tournament_name=tournament.name,
            round_number=round_number,
            rows=rows,
            participants=participants,
            played=played,
            total=total,
            limit=20,
        )
        await send_long(callback.message, "\n".join(lines))
        await callback.message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))
        await callback.answer()

    @dp.callback_query(F.data.startswith("qnav_mvp_round:"))
    async def on_qnav_mvp_round(callback: types.CallbackQuery):
        try:
            round_number = int((callback.data or "").split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return
        tournament, default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer(
                f"Можно использовать только туры {tournament.round_min}..{tournament.round_max}.",
                show_alert=True,
            )
            return
        await callback.message.answer(
            await build_mvp_round_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name)
        )
        await callback.message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))
        await callback.answer()

    @dp.callback_query(F.data.startswith("qnav_tops_round:"))
    async def on_qnav_tops_round(callback: types.CallbackQuery):
        try:
            round_number = int((callback.data or "").split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return
        tournament, default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer(
                f"Можно использовать только туры {tournament.round_min}..{tournament.round_max}.",
                show_alert=True,
            )
            return
        await callback.message.answer(
            await build_round_tops_text(round_number, tournament_id=tournament.id, tournament_name=tournament.name)
        )
        await callback.message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))
        await callback.answer()

    @dp.message(F.text == "✅ Вступить в турнир")
    async def btn_join(message: types.Message, state: FSMContext):
        if not await _ensure_enrollment_open_for_join(message):
            return
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
        await _request_display_name_for_join(message, state, tournament)

    @dp.message(F.text == "🔄 Вернуться в турнир")
    async def btn_rejoin(message: types.Message, state: FSMContext):
        if not await _ensure_enrollment_open_for_join(message):
            return
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
            if await is_user_in_tournament(session, message.from_user.id, tournament.id):
                await message.answer("Ты уже участвуешь в турнире.")
                return

            saved_name = await _get_setting(session, _left_tournament_name_key(tournament.id, message.from_user.id))
            if saved_name:
                await ensure_user_membership(
                    session,
                    message.from_user.id,
                    tournament.id,
                    display_name=saved_name,
                )
                await _delete_setting(session, _left_tournament_key(tournament.id, message.from_user.id))
                await session.commit()
                tournament, default_round = await _get_user_tournament_context(message.from_user.id)
                await message.answer(
                    f"✅ Возвращение в турнир подтверждено.\nИмя в таблице: {saved_name}"
                )
                await message.answer(
                    f"Готово. Текущий тур: {default_round}",
                    reply_markup=build_main_menu_keyboard(default_round=default_round, is_joined=True),
                )
                return

        async with SessionLocal() as session:
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
        await message.answer("Не нашёл прошлое имя в турнире. Давай быстро заново укажем имя.")
        await _request_display_name_for_join(message, state, tournament)

    @dp.message(F.text == "🚪 Покинуть турнир")
    async def btn_leave(message: types.Message):
        async with SessionLocal() as session:
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
            if not await is_user_in_tournament(session, message.from_user.id, tournament.id):
                join_cta_text = await _get_join_cta_text(session, message.from_user.id, tournament.id)
                _tournament, default_round = await _get_user_tournament_context(message.from_user.id)
                await message.answer(
                    "Ты сейчас не участвуешь в турнире.",
                    reply_markup=build_main_menu_keyboard(
                        default_round=default_round,
                        is_joined=False,
                        join_cta_text=join_cta_text,
                    ),
                )
                return

        kb = types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="✅ Да, покинуть", callback_data="leave_tournament:yes")],
                [types.InlineKeyboardButton(text="↩️ Отмена", callback_data="leave_tournament:no")],
            ]
        )
        await message.answer("Подтвердить выход из турнира?", reply_markup=kb)

    @dp.callback_query(F.data.startswith("leave_tournament:"))
    async def on_leave_tournament(callback: types.CallbackQuery):
        action = (callback.data or "").split(":", 1)[1] if ":" in (callback.data or "") else "no"
        if action != "yes":
            await callback.message.answer("Ок, участие в турнире оставили без изменений.")
            await callback.answer()
            return

        async with SessionLocal() as session:
            tournament = await get_selected_tournament_for_user(session, callback.from_user.id)
            ut_q = await session.execute(
                select(UserTournament).where(
                    UserTournament.tg_user_id == callback.from_user.id,
                    UserTournament.tournament_id == tournament.id,
                )
            )
            ut = ut_q.scalar_one_or_none()
            if ut is None:
                join_cta_text = await _get_join_cta_text(session, callback.from_user.id, tournament.id)
                _tournament, default_round = await _get_user_tournament_context(callback.from_user.id)
                await callback.message.answer(
                    "Ты уже не участвуешь в турнире.",
                    reply_markup=build_main_menu_keyboard(
                        default_round=default_round,
                        is_joined=False,
                        join_cta_text=join_cta_text,
                    ),
                )
                await callback.answer()
                return

            if (ut.display_name or "").strip():
                await _set_setting(
                    session,
                    _left_tournament_name_key(tournament.id, callback.from_user.id),
                    ut.display_name.strip(),
                )
            await _set_setting(session, _left_tournament_key(tournament.id, callback.from_user.id), "1")
            await session.delete(ut)
            await session.commit()

            join_cta_text = await _get_join_cta_text(session, callback.from_user.id, tournament.id)
            _tournament, default_round = await _get_user_tournament_context(callback.from_user.id)

        await callback.message.answer(
            "✅ Ты вышел из турнира.\nМожно вернуться в любой момент кнопкой «🔄 Вернуться в турнир».",
            reply_markup=build_main_menu_keyboard(
                default_round=default_round,
                is_joined=False,
                join_cta_text=join_cta_text,
            ),
        )
        await callback.answer()

    @dp.message(F.text == "📅 Матчи тура")
    async def btn_round(message: types.Message):
        await _send_default_round_text(message, message.from_user.id)

    @dp.message(F.text.regexp(r"(?i).*(мои|мой)\s+прогноз.*"))
    async def btn_my(message: types.Message):
        await _send_default_my_text(message, message.from_user.id)

    @dp.message(F.text.regexp(r"(?i).*общая\s+таблиц.*"))
    async def btn_table(message: types.Message):
        rows, meta = await build_active_stage_league_table(message.from_user.id)
        if meta is None:
            await message.answer("Сезон/этап пока не инициализирован. Обратись к администратору.")
            return
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        played, total = await get_matches_played_stats(
            tournament_id=tournament.id,
            round_min=meta.stage_round_min,
            round_max=meta.stage_round_max,
        )
        if not rows:
            await message.answer(
                f"В лиге «{meta.league_name}» пока нет участников активного этапа."
            )
            return
        lines = _build_stage_league_table_lines(
            season_name=meta.season_name,
            stage_name=meta.stage_name,
            league_name=meta.league_name,
            rows=rows,
            participants=meta.participants,
            played=played,
            total=total,
            current_user_id=message.from_user.id,
            limit=20,
        )
        await send_long(message, "\n".join(lines))
        await message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))

    @dp.message(F.text.regexp(r"(?i).*статистик.*"))
    async def btn_stats(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        scope = await get_user_stage_scope(message.from_user.id)
        if scope is None:
            await send_long(message, await build_stats_text(tournament_id=tournament.id))
        else:
            await send_long(
                message,
                await build_stats_text(
                    tournament_id=tournament.id,
                    round_min=scope.stage_round_min,
                    round_max=scope.stage_round_max,
                    allowed_user_ids=set(scope.member_ids),
                    title=f"📊 Статистика · {scope.league_name} · {scope.stage_name}",
                ),
            )
        await message.answer("Что дальше?", reply_markup=build_stats_followup_keyboard())

    @dp.message(F.text == "🏁 Выбрать турнир")
    async def btn_pick_tournament(message: types.Message):
        async with SessionLocal() as session:
            tournaments = await get_available_tournaments(session)
            selected = await get_selected_tournament_for_user(session, message.from_user.id)
        await message.answer(
            "🏁 Выбери турнир:",
            reply_markup=build_tournament_picker_inline(tournaments, selected_code=selected.code),
        )

    @dp.message(Command("tournament"))
    async def cmd_tournament(message: types.Message):
        async with SessionLocal() as session:
            tournaments = await get_available_tournaments(session)
            selected = await get_selected_tournament_for_user(session, message.from_user.id)
        await message.answer(
            "🏁 Выбери турнир:",
            reply_markup=build_tournament_picker_inline(tournaments, selected_code=selected.code),
        )

    @dp.callback_query(F.data.startswith("pick_tournament:"))
    async def on_pick_tournament(callback: types.CallbackQuery):
        data = callback.data or ""
        code = (data.split(":", 1)[1] if ":" in data else "").strip().upper()
        if not code:
            await callback.answer("Не удалось выбрать турнир", show_alert=True)
            return

        async with SessionLocal() as session:
            tournaments = await get_available_tournaments(session)
            by_code = {(t.code or "").strip().upper(): t for t in tournaments}
            picked = by_code.get(code)
            if picked is None:
                await callback.answer("Турнир недоступен", show_alert=True)
                return
            await set_selected_tournament_for_user(session, callback.from_user.id, code)
            await session.commit()

            is_joined = await is_user_in_tournament(session, callback.from_user.id, picked.id)
            join_cta_text = await _get_join_cta_text(session, callback.from_user.id, picked.id)

        _tournament, default_round = await _get_user_tournament_context(callback.from_user.id)
        picked_name = display_tournament_name(picked.name)
        await callback.message.answer(
            f"✅ Выбран турнир: {picked_name}\nТекущий тур по умолчанию: {default_round}",
            reply_markup=build_main_menu_keyboard(
                default_round=default_round,
                is_joined=is_joined,
                join_cta_text=join_cta_text,
            ),
        )
        await callback.answer()

    @dp.message(F.text.regexp(r"(?i).*мой\s+профил.*"))
    async def btn_profile(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        if not await _require_membership_or_hint(message, tournament):
            return
        await message.answer(
            await build_profile_text(
                message.from_user.id,
                tournament_id=tournament.id,
                tournament_name=tournament.name,
                round_min=tournament.round_min,
                round_max=tournament.round_max,
            )
        )
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
        text = await build_profile_text(
            message.from_user.id,
            tournament_id=tournament.id,
            tournament_name=tournament.name,
            round_min=tournament.round_min,
            round_max=tournament.round_max,
        )
        await message.answer(text)
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(Command("my_moves"))
    async def cmd_my_moves(message: types.Message):
        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
        await message.answer(await build_my_moves_text(message.from_user.id))
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
            "Дальше проще всего так: «🎯 Поставить прогноз»."
        )
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard("after_info"))

    @dp.message(F.text.regexp(r"(?i).*(поставить|сделать)\s+прогноз.*"))
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
            f"Матч выбран: {display_team_name(match.home_team)} — {display_team_name(match.away_team)}\n"
            "Отправь только счёт: 2:1"
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("predict_rounds:"))
    async def on_predict_rounds_picker(callback: types.CallbackQuery):
        data = callback.data or ""
        try:
            selected_round = int(data.split(":", 1)[1])
        except Exception:
            selected_round = None
        await _send_predict_rounds_picker(callback.message, callback.from_user.id, selected_round=selected_round)
        await callback.answer()

    @dp.callback_query(F.data.startswith("predict_pick_round:"))
    async def on_predict_pick_round(callback: types.CallbackQuery):
        data = callback.data or ""
        try:
            round_number = int(data.split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return

        tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer("Этот тур недоступен", show_alert=True)
            return

        await _send_round_predict_picker(
            target=callback.message,
            tg_user_id=callback.from_user.id,
            tournament=tournament,
            round_number=round_number,
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("predict_edit:"))
    async def on_predict_edit(callback: types.CallbackQuery):
        data = callback.data or ""
        try:
            round_number = int(data.split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return

        tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer("Этот тур недоступен", show_alert=True)
            return

        await _send_round_edit_picker(
            target=callback.message,
            tg_user_id=callback.from_user.id,
            tournament=tournament,
            round_number=round_number,
        )
        await callback.answer()

    @dp.callback_query(F.data.startswith("predict_bulk:"))
    async def on_predict_bulk(callback: types.CallbackQuery, state: FSMContext):
        data = callback.data or ""
        try:
            round_number = int(data.split(":", 1)[1])
        except Exception:
            await callback.answer("Не удалось выбрать тур", show_alert=True)
            return

        tournament, _default_round = await _get_user_tournament_context(callback.from_user.id)
        if not _round_in_tournament(round_number, tournament):
            await callback.answer("Этот тур недоступен", show_alert=True)
            return

        await _send_bulk_predict_prompt(
            target=callback.message,
            state=state,
            tg_user_id=callback.from_user.id,
            tournament=tournament,
            round_number=round_number,
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
                select(Match).where(Match.id == int(match_id), Match.tournament_id == tournament.id)
            )
            match = q.scalar_one_or_none()
            if match is None:
                await state.clear()
                await message.answer("Не нашёл этот матч. Нажми «🎯 Поставить прогноз» и выбери его из списка.")
                return

            if match.kickoff_time <= now:
                await state.clear()
                await message.answer(
                    "⛔️ Этот матч уже начался, прогноз закрыт.\n\n"
                    "Открой «🎯 Поставить прогноз» — покажу, что ещё доступно для прогноза."
                )
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
                pred.updated_at = datetime.utcnow()

            await session.commit()

        await state.clear()
        await message.answer(
            f"✅ Ставка принята: {display_team_name(match.home_team)} — {display_team_name(match.away_team)} | {pred_home}:{pred_away}"
        )
        await _send_round_predict_picker(
            target=message,
            tg_user_id=tg_user_id,
            tournament=tournament,
            round_number=match.round_number,
        )

    @dp.message(PredictRoundStates.waiting_for_display_name)
    async def on_display_name_input(message: types.Message, state: FSMContext):
        display_name = normalize_display_name(message.text or "")
        if display_name is None:
            await message.answer("Имя должно быть длиной 2-24 символа. Попробуй ещё раз.")
            return

        async with SessionLocal() as check_session:
            enrollment_open = await is_enrollment_open(check_session)
        if not enrollment_open:
            await state.clear()
            await message.answer(
                "🔒 Набор уже закрыт, сохранить вступление сейчас нельзя.\n"
                "Дождись следующего открытия набора."
            )
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
            await _set_setting(
                session,
                _left_tournament_name_key(tournament.id, message.from_user.id),
                display_name,
            )
            await _delete_setting(session, _left_tournament_key(tournament.id, message.from_user.id))
            new_join = not exists_before
            await session.commit()

        await state.clear()
        t_name = tournament_name or tournament.name
        await message.answer(
            f"✅ Ты в турнире: {t_name}\n"
            f"Имя в таблице: {display_name}"
        )
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        await message.answer(
            f"Готово. Текущий тур: {default_round}",
            reply_markup=build_main_menu_keyboard(default_round=default_round, is_joined=True),
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
    async def cmd_start(message: types.Message, command: CommandObject):
        # Принудительно снимаем старую reply-клавиатуру у всех пользователей.
        await message.answer("Обновляю интерфейс…", reply_markup=types.ReplyKeyboardRemove())

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            await set_selected_tournament_for_user(session, message.from_user.id, WC_TOURNAMENT_CODE)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
            is_joined = await is_user_in_tournament(session, message.from_user.id, tournament.id)
            await session.commit()

        start_arg = (command.args or "").strip().lower()
        deep_join_wc = start_arg in {"join_wc2026", "join_wc", "wc2026", "wc", "join"}
        if deep_join_wc and not is_joined:
            if not await _ensure_enrollment_open_for_join(message):
                return
            await _request_display_name_for_join(message, state, tournament)
            return

        text = (
            "⚽  Вот это тебя корёжит! А всё потому что тебя ещё нет в новом турнире прогнозов World Cup 2026!\n"
            "Ставишь по двоичной системе или читаешь футбол, как открытую книгу? Скоро узнаем.\n\n"
            "Нажимай кнопку ниже и вступай в турнир!"
        )
        if is_joined:
            await message.answer("Ты уже участвуешь в турнире WC 2026 ✅", reply_markup=types.ReplyKeyboardRemove())
            return
        await message.answer(text, reply_markup=build_start_join_wc_keyboard())

    @dp.callback_query(F.data == "start_join_wc")
    async def cb_start_join_wc(callback: types.CallbackQuery, state: FSMContext):
        if not await _ensure_enrollment_open_for_join(callback):
            await callback.answer()
            return
        async with SessionLocal() as session:
            await upsert_user_from_callback(session, callback)
            await set_selected_tournament_for_user(session, callback.from_user.id, WC_TOURNAMENT_CODE)
            tournament = await get_selected_tournament_for_user(session, callback.from_user.id)
            await session.commit()
        await _request_display_name_for_join(callback, state, tournament)
        await callback.answer()

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
        if not await _ensure_enrollment_open_for_join(message):
            return
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
                join_cta_text = await _get_join_cta_text(session, message.from_user.id, tournament.id)
                await message.answer(
                    f"Сначала вступи в турнир {tournament.name} кнопкой «✅ Вступить в турнир»,"
                    " и сразу сможем сохранить прогноз.",
                    reply_markup=build_main_menu_keyboard(
                        default_round=int(tournament.round_min),
                        is_joined=False,
                        join_cta_text=join_cta_text,
                    ),
                )
                return

            match_q = await session.execute(select(Match).where(Match.id == match_id, Match.tournament_id == tournament.id))
            match = match_q.scalar_one_or_none()
            if match is None:
                await message.answer("Не нашёл такой матч в выбранном турнире. Проверь ID через «🎯 Поставить прогноз».")
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
                pred.updated_at = datetime.utcnow()

            await session.commit()

        confirm_text, nav_mode = await _build_predict_saved_message(
            tg_user_id=tg_user_id,
            tournament_id=tournament.id,
            round_number=match.round_number,
            home_team=match.home_team,
            away_team=match.away_team,
            pred_home=pred_home,
            pred_away=pred_away,
        )
        await message.answer(confirm_text)
        await message.answer("Что дальше?", reply_markup=build_quick_nav_keyboard(nav_mode))

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
        accepted_lines: list[str] = []

        async with SessionLocal() as session:
            await upsert_user_from_message(session, message)
            tournament = await get_selected_tournament_for_user(session, message.from_user.id)
            matches_q = await session.execute(
                select(Match)
                .where(
                    Match.round_number == round_number,
                    Match.tournament_id == tournament.id,
                    Match.kickoff_time > now,
                )
                .order_by(Match.kickoff_time.asc())
            )
            open_matches = matches_q.scalars().all()
            if not open_matches:
                await state.clear()
                await message.answer("В этом туре не осталось открытых матчей для прогноза.")
                return

            for line in lines:
                match, pred_home, pred_away = parse_bulk_prediction_line(line, open_matches=open_matches)
                if match is None or pred_home is None or pred_away is None:
                    errors += 1
                    continue

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
                    pred.updated_at = datetime.utcnow()

                saved += 1
                accepted_lines.append(
                    f"• {display_team_name(match.home_team)} {pred_home}-{pred_away} {display_team_name(match.away_team)}"
                )

            await session.commit()

        await state.clear()
        result_lines = [f"✅ Прогнозы приняты: {saved} | Пропущено: {skipped} | Ошибок: {errors}"]
        if accepted_lines:
            result_lines.append("")
            result_lines.append("Принятые прогнозы:")
            result_lines.extend(accepted_lines[:12])
            if len(accepted_lines) > 12:
                result_lines.append(f"... и ещё {len(accepted_lines) - 12}")
        await send_long(message, "\n".join(result_lines))
        await message.answer(
            "Быстрые действия:",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [types.InlineKeyboardButton(text="✏️ Изменить прогноз", callback_data=f"predict_edit:{round_number}")],
                    [types.InlineKeyboardButton(text="📚 Выбрать другой тур", callback_data=f"predict_rounds:{round_number}")],
                ]
            ),
        )

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
        await _send_my_round_text(message, message.from_user.id, tournament=tournament, round_number=round_number)

    @dp.message(Command("table"))
    async def cmd_table(message: types.Message):
        rows, meta = await build_active_stage_league_table(message.from_user.id)
        if meta is None:
            await message.answer("Сезон/этап пока не инициализирован. Обратись к администратору.")
            return
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        played, total = await get_matches_played_stats(
            tournament_id=tournament.id,
            round_min=meta.stage_round_min,
            round_max=meta.stage_round_max,
        )

        if not rows:
            await message.answer(
                f"В лиге «{meta.league_name}» пока нет участников активного этапа."
            )
            return

        lines = _build_stage_league_table_lines(
            season_name=meta.season_name,
            stage_name=meta.stage_name,
            league_name=meta.league_name,
            rows=rows,
            participants=meta.participants,
            played=played,
            total=total,
            current_user_id=message.from_user.id,
            limit=20,
        )

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
        played, total = await get_round_matches_played_stats(round_number=round_number, tournament_id=tournament.id)
        lines = _build_round_table_lines(
            tournament_name=tournament.name,
            round_number=round_number,
            rows=rows,
            participants=participants,
            played=played,
            total=total,
            limit=20,
        )

        await send_long(message, "\n".join(lines))
        await message.answer("Быстрые действия:", reply_markup=build_quick_nav_keyboard("after_table"))

    @dp.message(Command("stats"))
    async def cmd_stats(message: types.Message):
        tournament, _default_round = await _get_user_tournament_context(message.from_user.id)
        scope = await get_user_stage_scope(message.from_user.id)
        if scope is None:
            text = await build_stats_text(tournament_id=tournament.id)
        else:
            text = await build_stats_text(
                tournament_id=tournament.id,
                round_min=scope.stage_round_min,
                round_max=scope.stage_round_max,
                allowed_user_ids=set(scope.member_ids),
                title=f"📊 Статистика · {scope.league_name} · {scope.stage_name}",
            )
        await send_long(message, text)
        await message.answer("Что дальше?", reply_markup=build_stats_followup_keyboard())

    @dp.message(
        F.text
        & ~F.text.startswith("/")
        & ~F.text.regexp(r"^\s*/")
    )
    async def fallback_menu_text(message: types.Message):
        """
        Страховочный fallback только для неизвестного текста.
        Важные действия обрабатываются явными хендлерами кнопок/команд выше.
        """
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        async with SessionLocal() as session:
            is_joined = await is_user_in_tournament(session, message.from_user.id, tournament.id)
            join_cta_text = await _get_join_cta_text(session, message.from_user.id, tournament.id)
        await message.answer(
            "Не распознал действие. Нажми /start, если меню пропало, или выбери кнопку ниже.",
            reply_markup=build_main_menu_keyboard(
                default_round=default_round,
                is_joined=is_joined,
                join_cta_text=join_cta_text,
            ),
        )

    @dp.message(~F.text)
    async def fallback_any_message(message: types.Message):
        """
        Абсолютный fallback: если прилетело не-текстовое сообщение,
        всё равно отвечаем и не оставляем update в "not handled".
        """
        # В channel/private-channel бот часто не может отвечать -> CHANNEL_PRIVATE.
        # Просто игнорируем такие обновления без ошибок.
        if (getattr(message.chat, "type", None) or "") == "channel":
            return
        if message.text:
            return
        tournament, default_round = await _get_user_tournament_context(message.from_user.id)
        async with SessionLocal() as session:
            is_joined = await is_user_in_tournament(session, message.from_user.id, tournament.id)
            join_cta_text = await _get_join_cta_text(session, message.from_user.id, tournament.id)
        try:
            await message.answer(
                "Получил сообщение. Для действий используй кнопки меню ниже.",
                reply_markup=build_main_menu_keyboard(
                    default_round=default_round,
                    is_joined=is_joined,
                    join_cta_text=join_cta_text,
                ),
            )
        except Exception:
            # Тихий фейл для чатов, где бот не имеет права отправки.
            return

    @dp.callback_query(F.data.startswith("duel_accept:"))
    async def cb_duel_accept(callback: types.CallbackQuery):
        raw = (callback.data or "").split(":", 1)
        if len(raw) != 2:
            await callback.answer("Некорректная кнопка.")
            return
        try:
            duel_id = int(raw[1])
        except ValueError:
            await callback.answer("Некорректная дуэль.")
            return

        text = "Вызов отмечен. Открой 1х1 в Mini App и поставь свой прогноз, чтобы принять дуэль."
        kb = None
        if MINIAPP_WEB_URL:
            url = MINIAPP_WEB_URL
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{urlencode({'screen': 'duels', 'duel_id': int(duel_id)})}"
            kb = types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="Открыть 1х1", url=url)]]
            )
        await callback.message.answer(text, reply_markup=kb)
        await callback.answer("Открой 1х1 и сохрани свой счёт.")

    @dp.callback_query(F.data.startswith("duel_decline:"))
    async def cb_duel_decline(callback: types.CallbackQuery):
        raw = (callback.data or "").split(":", 1)
        if len(raw) != 2:
            await callback.answer("Некорректная кнопка.")
            return
        try:
            duel_id = int(raw[1])
        except ValueError:
            await callback.answer("Некорректная дуэль.")
            return

        try:
            async with SessionLocal() as session:
                duel = await respond_duel(
                    session,
                    duel_id=int(duel_id),
                    responder_tg_user_id=int(callback.from_user.id),
                    accept=False,
                )
                await session.commit()
            await callback.message.answer("Вызов отклонён.")
            try:
                await callback.bot.send_message(
                    chat_id=int(duel.challenger_tg_user_id),
                    text="Твой вызов в 1х1 отклонён соперником.",
                )
            except Exception:
                pass
            await callback.answer("Отклонено.")
        except Exception:
            await callback.answer("Не удалось отклонить вызов (возможно, он уже неактуален).", show_alert=True)

    @dp.callback_query()
    async def fallback_any_callback(callback: types.CallbackQuery):
        """
        Fallback для неизвестных callback_data.
        """
        await callback.answer("Эта кнопка устарела. Открой /start и попробуй ещё раз.", show_alert=False)
