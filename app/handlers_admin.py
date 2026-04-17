from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timedelta
from aiogram import Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import case, delete, select, func

from app.config import load_admin_ids
from app.db import SessionLocal
from app.display import display_team_name, display_tournament_name
from app.models import (
    League,
    LeagueMovement,
    LeagueParticipant,
    Match,
    Prediction,
    Point,
    Season,
    Setting,
    Stage,
    Tournament,
    User,
    UserTournament,
)
from app.league_table import build_active_stage_league_table
from app.scoring import calculate_points
from app.season_setup import (
    DEFAULT_SEASON_NAME,
    DEFAULT_STAGE_1_NAME,
    DEFAULT_STAGE_2_NAME,
    assign_user_to_active_stage_league,
    get_active_season,
    get_active_stage,
    is_enrollment_open,
    list_unassigned_enrolled_users,
    set_active_season_name,
    set_enrollment_open,
    setup_new_season_foundation,
)
from app.tournament import ROUND_DEFAULT, ROUND_MAX, ROUND_MIN, is_tournament_round
from app.audience import (
    extract_left_user_id,
    is_blocked_send_error,
    mark_user_blocked,
    LEFT_KEY_PATTERN,
)

ADMIN_IDS = load_admin_ids()
ROUND_DIGEST_CHAT_ID_RAW = os.getenv("ROUND_DIGEST_CHAT_ID", "").strip()
try:
    ROUND_DIGEST_CHAT_ID = int(ROUND_DIGEST_CHAT_ID_RAW) if ROUND_DIGEST_CHAT_ID_RAW else None
except ValueError:
    ROUND_DIGEST_CHAT_ID = None
# Публичный digest-чат отключён: итоги тура отправляются только участникам в ЛС.
ROUND_DIGEST_CHAT_ID = None

EXACT_HIT_PUSH_DELAY_RAW = os.getenv("EXACT_HIT_PUSH_DELAY_SEC", "0.12").strip()
try:
    EXACT_HIT_PUSH_DELAY_SEC = max(0.0, float(EXACT_HIT_PUSH_DELAY_RAW))
except ValueError:
    EXACT_HIT_PUSH_DELAY_SEC = 0.12

ENROLL_LIST_PAGE_SIZE = 8


class AdminSetResultStates(StatesGroup):
    waiting_for_score = State()


def _now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


def _is_admin(message_or_callback) -> bool:
    uid = message_or_callback.from_user.id
    return uid in ADMIN_IDS


def _admin_panel_keyboard() -> types.InlineKeyboardMarkup:
    rows = [
        [
            types.InlineKeyboardButton(text="📊 Статус", callback_data="admin_panel:status"),
            types.InlineKeyboardButton(text="📈 Прогресс тура", callback_data="admin_panel:progress"),
        ],
        [types.InlineKeyboardButton(text="🚫 Кто не поставил", callback_data="admin_panel:missing")],
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _parse_admin_kickoff_datetime(raw: str) -> datetime | None:
    """
    Надёжный парсинг даты/времени для /admin_add_match.
    Поддерживает:
    - YYYY-MM-DD HH:MM
    - YYYY-MM-DDTHH:MM
    - YYYY-MM-DD HH:MM:SS
    """
    s = (raw or "").strip()
    if not s:
        return None

    # Нормализуем частые "кривые" символы из мессенджеров/клавиатур.
    s = s.replace("—", "-").replace("–", "-").replace("−", "-")
    s = " ".join(s.split())
    if "T" in s:
        s = s.replace("T", " ")

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    # Последний шанс: fromisoformat (иногда принимает то, что strptime не берёт)
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


async def recalc_points_for_match_in_session(session, match_id: int) -> int:
    """Пересчитать очки за один матч (использует переданную DB-сессию)."""
    updates = 0

    res_match = await session.execute(select(Match).where(Match.id == match_id))
    match = res_match.scalar_one_or_none()
    if match is None:
        return 0

    if match.home_score is None or match.away_score is None:
        return 0

    res_preds = await session.execute(select(Prediction).where(Prediction.match_id == match_id))
    preds = res_preds.scalars().all()

    for p in preds:
        calc = calculate_points(
            pred_home=p.pred_home,
            pred_away=p.pred_away,
            real_home=match.home_score,
            real_away=match.away_score,
        )
        pts = calc.points
        cat = calc.category

        res_point = await session.execute(
            select(Point).where(Point.match_id == match_id, Point.tg_user_id == p.tg_user_id)
        )
        point = res_point.scalar_one_or_none()
        if point is None:
            session.add(Point(match_id=match_id, tg_user_id=p.tg_user_id, points=pts, category=cat))
            updates += 1
        else:
            if point.points != pts or point.category != cat:
                point.points = pts
                point.category = cat
                updates += 1

    await session.commit()
    return updates


async def recalc_points_for_match(match_id: int) -> int:
    """Пересчитать очки за один матч (открывает свою DB-сессию)."""
    async with SessionLocal() as session:
        return await recalc_points_for_match_in_session(session, match_id)


def _parse_score(score_str: str) -> tuple[int, int] | None:
    s = score_str.strip().replace("-", ":")
    if ":" not in s:
        return None
    a, b = s.split(":", 1)
    try:
        return int(a), int(b)
    except ValueError:
        return None


def _format_person_name(
    tg_user_id: int,
    tournament_display_name: str | None,
    user_display_name: str | None,
    username: str | None,
    full_name: str | None,
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


async def _set_setting(session, key: str, value: str) -> None:
    res = await session.execute(select(Setting).where(Setting.key == key))
    obj = res.scalar_one_or_none()
    if obj:
        obj.value = value
    else:
        session.add(Setting(key=key, value=value))
    await session.commit()


async def _get_setting(session, key: str) -> str | None:
    res = await session.execute(select(Setting).where(Setting.key == key))
    row = res.scalar_one_or_none()
    return row.value if row else None


async def _build_admin_match_result_live_update(match_id: int) -> tuple[str, bool]:
    async with SessionLocal() as session:
        q = await session.execute(select(Match).where(Match.id == match_id))
        match = q.scalar_one_or_none()
        if match is None:
            return "", False

        preds_q = await session.execute(
            select(
                Prediction.tg_user_id,
                Point.points,
                Point.category,
                UserTournament.display_name,
                User.display_name,
                User.username,
                User.full_name,
            )
            .select_from(Prediction)
            .outerjoin(
                Point,
                (Point.tg_user_id == Prediction.tg_user_id) & (Point.match_id == Prediction.match_id),
            )
            .outerjoin(
                UserTournament,
                (UserTournament.tg_user_id == Prediction.tg_user_id) & (UserTournament.tournament_id == match.tournament_id),
            )
            .outerjoin(User, User.tg_user_id == Prediction.tg_user_id)
            .where(Prediction.match_id == match_id)
            .order_by(Prediction.tg_user_id.asc())
        )
        pred_rows = preds_q.all()

        total_preds = len(pred_rows)
        winners_exact: list[str] = []
        winners_diff: list[str] = []
        winners_outcome: list[str] = []
        scored_any = 0

        for tg_user_id, points, category, tdn, udn, username, full_name in pred_rows:
            pts = int(points or 0)
            if pts > 0:
                scored_any += 1
            name = _format_person_name(int(tg_user_id), tdn, udn, username, full_name)
            cat = (category or "").strip()
            if cat == "exact":
                winners_exact.append(name)
            elif cat == "diff":
                winners_diff.append(name)
            elif cat == "outcome":
                winners_outcome.append(name)

        def _names(items: list[str]) -> str:
            return ", ".join(items[:5]) if items else "—"

        round_closed_q = await session.execute(
            select(func.count(Match.id))
            .where(
                Match.tournament_id == match.tournament_id,
                Match.round_number == match.round_number,
                Match.source == "manual",
                (Match.home_score.is_(None) | Match.away_score.is_(None)),
            )
        )
        remaining_without_result = int(round_closed_q.scalar_one() or 0)
        round_closed = remaining_without_result == 0

    if total_preds == 0:
        text = (
            "⚡ Апдейт по матчу:\n"
            "Прогнозов на этот матч не было."
        )
    elif scored_any == 0:
        text = (
            "⚡ Апдейт по матчу:\n"
            f"Участников с прогнозом: {total_preds}\n"
            "С очками за матч: 0\n\n"
            "Сегодня матч удивил всех 😅\n"
            "Никто не набрал очки за этот матч."
        )
    else:
        text = (
            "⚡ Апдейт по матчу:\n"
            f"Участников с прогнозом: {total_preds}\n"
            f"С очками за матч: {scored_any}\n\n"
            f"🎯 Точный счёт: {_names(winners_exact)}\n"
            f"📏 Разница + исход: {_names(winners_diff)}\n"
            f"✅ Только исход: {_names(winners_outcome)}"
        )

    return text, round_closed


async def _maybe_send_round_closed_summary(bot, tournament_id: int, round_number: int) -> None:
    """
    Шлём один сводный пуш после закрытия тура:
    - только один раз на тур (через settings key)
    - только когда у всех матчей тура есть итог
    """
    key = f"ROUND_SUMMARY_SENT_T{int(tournament_id)}_R{int(round_number)}"

    async with SessionLocal() as session:
        already_sent = await _get_setting(session, key)
        if already_sent:
            return

        counts_q = await session.execute(
            select(
                func.count(Match.id).label("total"),
                func.sum(
                    case(
                        (
                            (Match.home_score.isnot(None) & Match.away_score.isnot(None)),
                            1,
                        ),
                        else_=0,
                    )
                ).label("done"),
            ).where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
            )
        )
        total, done = counts_q.one()
        total = int(total or 0)
        done = int(done or 0)
        if total == 0 or done < total:
            return

        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        tournament = t_q.scalar_one_or_none()
        tournament_name = display_tournament_name(tournament.name) if tournament else f"турнир #{tournament_id}"

        members_q = await session.execute(
            select(UserTournament.tg_user_id).where(UserTournament.tournament_id == tournament_id)
        )
        member_ids = [int(x[0]) for x in members_q.all()]

        leaderboard_q = await session.execute(
            select(
                Prediction.tg_user_id,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
            )
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .outerjoin(
                Point,
                (Point.tg_user_id == Prediction.tg_user_id) & (Point.match_id == Prediction.match_id),
            )
            .where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
            )
            .group_by(Prediction.tg_user_id)
            .order_by(
                func.coalesce(func.sum(Point.points), 0).desc(),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).desc(),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).desc(),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).desc(),
                Prediction.tg_user_id.asc(),
            )
        )
        leaderboard_rows = leaderboard_q.all()
        places: dict[int, int] = {}
        stats: dict[int, tuple[int, int, int, int]] = {}
        for i, (tg_user_id, total_pts, exact, diff, outcome) in enumerate(leaderboard_rows, start=1):
            uid = int(tg_user_id)
            places[uid] = i
            stats[uid] = (
                int(total_pts or 0),
                int(exact or 0),
                int(diff or 0),
                int(outcome or 0),
            )
        participants = len(leaderboard_rows)

        exact_max = max((int(r[2] or 0) for r in leaderboard_rows), default=0)
        diff_max = max((int(r[3] or 0) for r in leaderboard_rows), default=0)
        outcome_max = max((int(r[4] or 0) for r in leaderboard_rows), default=0)

        exact_ids = [int(r[0]) for r in leaderboard_rows if int(r[2] or 0) == exact_max and exact_max > 0]
        diff_ids = [int(r[0]) for r in leaderboard_rows if int(r[3] or 0) == diff_max and diff_max > 0]
        outcome_ids = [int(r[0]) for r in leaderboard_rows if int(r[4] or 0) == outcome_max and outcome_max > 0]

        users_q = await session.execute(select(User.tg_user_id, User.display_name, User.full_name))
        users_map = {int(tg): (dn, fn) for tg, dn, fn in users_q.all()}
        ut_q = await session.execute(
            select(UserTournament.tg_user_id, UserTournament.display_name).where(
                UserTournament.tournament_id == tournament_id
            )
        )
        tournament_names = {int(tg): dn for tg, dn in ut_q.all() if dn}

        def pretty_name(tg_user_id: int) -> str:
            dn = tournament_names.get(int(tg_user_id))
            if dn:
                return dn
            user_dn, fn = users_map.get(int(tg_user_id), (None, None))
            if user_dn:
                return user_dn
            if fn:
                return fn
            return str(tg_user_id)

        exact_pretty = ", ".join(pretty_name(x) for x in exact_ids) if exact_ids else "—"
        diff_pretty = ", ".join(pretty_name(x) for x in diff_ids) if diff_ids else "—"
        outcome_pretty = ", ".join(pretty_name(x) for x in outcome_ids) if outcome_ids else "—"

        streak_rows_q = await session.execute(
            select(Point.tg_user_id, Match.kickoff_time, Point.points, Match.id)
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
            )
            .order_by(Point.tg_user_id.asc(), Match.kickoff_time.asc(), Match.id.asc())
        )
        streak_rows = streak_rows_q.all()
        streak_map: dict[int, tuple[int, int]] = {}
        curr_uid: int | None = None
        current_streak = 0
        best_streak = 0
        for tg_user_id, _kickoff, pts, _match_id in streak_rows:
            uid = int(tg_user_id)
            if curr_uid is None:
                curr_uid = uid
            if uid != curr_uid:
                streak_map[curr_uid] = (current_streak, best_streak)
                curr_uid = uid
                current_streak = 0
                best_streak = 0

            if int(pts or 0) > 0:
                current_streak += 1
                if current_streak > best_streak:
                    best_streak = current_streak
            else:
                current_streak = 0
        if curr_uid is not None:
            streak_map[curr_uid] = (current_streak, best_streak)

        for tg_user_id in member_ids:
            current_streak, best_streak = streak_map.get(int(tg_user_id), (0, 0))
            if tg_user_id in stats:
                total_pts, exact, diff, outcome = stats[tg_user_id]
                place = places[tg_user_id]
                place_mark = "🏆" if place == 1 else ("🥈" if place == 2 else ("🥉" if place == 3 else "📍"))
                if total_pts >= 10:
                    mood = "Ты просто в огне!"
                elif total_pts >= 6:
                    mood = "Очень сильный тур, так держать."
                elif total_pts >= 3:
                    mood = "Крепкий результат — хороший темп."
                elif total_pts > 0:
                    mood = "Плюс есть, продолжаем набирать."
                else:
                    mood = "Этот тур не зашёл, но всё можно вернуть в следующем."
                text = (
                    f"🏁 Тур {round_number} завершён ({tournament_name})\n\n"
                    f"{place_mark} Место в туре: {place}/{participants}\n"
                    f"📊 Очки за тур: {total_pts}\n"
                    f"🎯{exact} | 📏{diff} | ✅{outcome}\n"
                    f"🔥 Серия сейчас: {current_streak}\n"
                    f"🏅 Лучшая серия: {best_streak}\n\n"
                    f"{mood}\n"
                    "Следующий тур уже рядом — жми «🎯 Поставить прогноз»."
                )
            else:
                text = (
                    f"🏁 Тур {round_number} завершён ({tournament_name})\n\n"
                    "В этом туре у тебя не было прогнозов.\n"
                    f"🔥 Серия сейчас: {current_streak}\n"
                    f"🏅 Лучшая серия: {best_streak}\n\n"
                    "В следующем туре врываемся — жми «🎯 Поставить прогноз»."
                )

            try:
                await bot.send_message(chat_id=tg_user_id, text=text)
            except Exception as e:
                if is_blocked_send_error(e):
                    await mark_user_blocked(session, int(tg_user_id))
                continue

        if ROUND_DIGEST_CHAT_ID is not None:
            round_pred_stats_q = await session.execute(
                select(
                    func.count(Prediction.id).label("total_preds"),
                    func.coalesce(
                        func.sum(case((Point.points > 0, 1), else_=0)),
                        0,
                    ).label("hit_preds"),
                )
                .select_from(Prediction)
                .join(Match, Match.id == Prediction.match_id)
                .outerjoin(
                    Point,
                    (Point.tg_user_id == Prediction.tg_user_id) & (Point.match_id == Prediction.match_id),
                )
                .where(
                    Match.tournament_id == tournament_id,
                    Match.round_number == round_number,
                    Match.source == "manual",
                )
            )
            round_total_preds, round_hit_preds = round_pred_stats_q.one()
            round_total_preds = int(round_total_preds or 0)
            round_hit_preds = int(round_hit_preds or 0)
            round_acc = (round_hit_preds / round_total_preds * 100.0) if round_total_preds > 0 else 0.0

            season_pred_stats_q = await session.execute(
                select(
                    func.count(Prediction.id).label("total_preds"),
                    func.coalesce(
                        func.sum(case((Point.points > 0, 1), else_=0)),
                        0,
                    ).label("hit_preds"),
                )
                .select_from(Prediction)
                .join(Match, Match.id == Prediction.match_id)
                .outerjoin(
                    Point,
                    (Point.tg_user_id == Prediction.tg_user_id) & (Point.match_id == Prediction.match_id),
                )
                .where(
                    Match.tournament_id == tournament_id,
                    Match.source == "manual",
                    Match.home_score.isnot(None),
                    Match.away_score.isnot(None),
                )
            )
            season_total_preds, season_hit_preds = season_pred_stats_q.one()
            season_total_preds = int(season_total_preds or 0)
            season_hit_preds = int(season_hit_preds or 0)
            season_acc = (season_hit_preds / season_total_preds * 100.0) if season_total_preds > 0 else 0.0

            top3_lines: list[str] = []
            for i, (tg_user_id, total_pts, _exact, _diff, _outcome) in enumerate(leaderboard_rows[:3], start=1):
                top3_lines.append(f"{i}. {pretty_name(int(tg_user_id))} — {int(total_pts or 0)}")

            if leaderboard_rows:
                best_pts = int(leaderboard_rows[0][1] or 0)
                mvp_ids = [int(r[0]) for r in leaderboard_rows if int(r[1] or 0) == best_pts]
                mvp_text = ", ".join(pretty_name(uid) for uid in mvp_ids)
            else:
                best_pts = 0
                mvp_text = "—"

            public_lines = [f"🏁 Итоги тура {round_number} ({tournament_name})", ""]
            public_lines.append(f"🏅 MVP: {best_pts} — {mvp_text}")
            public_lines.append(f"🎯 Топ точных: {exact_max} — {exact_pretty}")
            public_lines.append(f"📏 Топ разницы: {diff_max} — {diff_pretty}")
            public_lines.append(f"✅ Топ исходов: {outcome_max} — {outcome_pretty}")
            if top3_lines:
                public_lines.append("")
                public_lines.append("Топ-3 тура:")
                public_lines.extend(top3_lines)
            public_lines.append("")
            public_lines.append(f"Участников в туре: {participants}")
            public_lines.append(
                f"Прогнозов с очками: {round_hit_preds}/{round_total_preds} ({round_acc:.0f}%)"
            )
            public_lines.append(f"Точность тура vs сезон: {round_acc:.0f}% vs {season_acc:.0f}%")
            try:
                await bot.send_message(chat_id=ROUND_DIGEST_CHAT_ID, text="\n".join(public_lines))
            except Exception:
                pass

        await _set_setting(session, key, "1")


def _rank_places_from_stats(stats_map: dict[int, tuple[int, int, int, int]]) -> dict[int, int]:
    rows = [
        (uid, vals[0], vals[1], vals[2], vals[3])
        for uid, vals in stats_map.items()
    ]
    rows.sort(key=lambda x: (-x[1], -x[2], -x[3], -x[4], x[0]))
    return {uid: idx for idx, (uid, *_rest) in enumerate(rows, start=1)}


async def _build_tournament_stats_map(session, tournament_id: int) -> dict[int, tuple[int, int, int, int]]:
    lb_q = await session.execute(
        select(
            Prediction.tg_user_id,
            func.coalesce(func.sum(Point.points), 0).label("total"),
            func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
            func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
            func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
        )
        .select_from(Prediction)
        .join(Match, Match.id == Prediction.match_id)
        .outerjoin(
            Point,
            (Point.tg_user_id == Prediction.tg_user_id) & (Point.match_id == Prediction.match_id),
        )
        .where(Match.tournament_id == tournament_id)
        .group_by(Prediction.tg_user_id)
    )
    out: dict[int, tuple[int, int, int, int]] = {}
    for uid, total, exact, diff, outcome in lb_q.all():
        out[int(uid)] = (int(total or 0), int(exact or 0), int(diff or 0), int(outcome or 0))
    return out


async def _maybe_send_exact_hit_pushes(bot, match_id: int) -> int:
    send_payloads: list[tuple[int, str, types.InlineKeyboardMarkup]] = []

    async with SessionLocal() as session:
        match_q = await session.execute(select(Match).where(Match.id == match_id))
        match = match_q.scalar_one_or_none()
        if match is None:
            return 0
        if match.home_score is None or match.away_score is None:
            return 0

        exact_q = await session.execute(
            select(Point.tg_user_id)
            .where(Point.match_id == match_id, Point.category == "exact")
            .order_by(Point.tg_user_id.asc())
        )
        exact_user_ids = [int(r[0]) for r in exact_q.all()]
        if not exact_user_ids:
            return 0

        keys_by_uid = {uid: f"EXACT_PUSH_SENT_M{int(match_id)}_U{uid}" for uid in exact_user_ids}
        sent_q = await session.execute(
            select(Setting.key).where(Setting.key.in_(list(keys_by_uid.values())))
        )
        already_sent = {str(r[0]) for r in sent_q.all()}
        target_user_ids = [uid for uid in exact_user_ids if keys_by_uid[uid] not in already_sent]
        if not target_user_ids:
            return 0

        after_stats = await _build_tournament_stats_map(session, match.tournament_id)
        before_stats = dict(after_stats)

        contrib_q = await session.execute(
            select(Point.tg_user_id, Point.points, Point.category).where(Point.match_id == match_id)
        )
        for uid_raw, pts_raw, cat_raw in contrib_q.all():
            uid = int(uid_raw)
            pts = int(pts_raw or 0)
            cat = str(cat_raw or "")
            total, exact, diff, outcome = before_stats.get(uid, (0, 0, 0, 0))
            total = max(total - pts, 0)
            if cat == "exact":
                exact = max(exact - 1, 0)
            elif cat == "diff":
                diff = max(diff - 1, 0)
            elif cat == "outcome":
                outcome = max(outcome - 1, 0)
            before_stats[uid] = (total, exact, diff, outcome)

        before_places = _rank_places_from_stats(before_stats)
        after_places = _rank_places_from_stats(after_stats)

        preds_q = await session.execute(
            select(Prediction.tg_user_id, Prediction.pred_home, Prediction.pred_away).where(
                Prediction.match_id == match_id,
                Prediction.tg_user_id.in_(target_user_ids),
            )
        )
        pred_map = {int(uid): (int(ph), int(pa)) for uid, ph, pa in preds_q.all()}

        for uid in target_user_ids:
            before_place = before_places.get(uid)
            after_place = after_places.get(uid)
            if before_place is not None and after_place is not None and before_place != after_place:
                icon = "⬆️" if after_place < before_place else "⬇️"
                pos_line = f"{icon} Подъём в таблице: {before_place} → {after_place} место"
                if after_place > before_place:
                    pos_line = f"{icon} Позиция в таблице: {before_place} → {after_place} место"
            elif after_place is not None:
                pos_line = f"Позиция в таблице: {after_place} место"
            else:
                pos_line = "Позиция в таблице: —"

            pred_pair = pred_map.get(uid, (match.home_score, match.away_score))
            score_line = f"{pred_pair[0]}-{pred_pair[1]}"
            text = (
                "🎯 Ты угадал точный счёт!\n"
                f"{display_team_name(match.home_team)} {score_line} {display_team_name(match.away_team)}\n"
                "+4 очка\n"
                f"{pos_line}"
            )
            kb = types.InlineKeyboardMarkup(
                inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏆 Общая таблица", callback_data="qnav:table")],
                    [types.InlineKeyboardButton(text="🗂 Мои прогнозы", callback_data=f"qnav_my_round:{match.round_number}")],
                ]
            )
            send_payloads.append((uid, text, kb))

            # Антидубль: помечаем отправку заранее.
            session.add(Setting(key=keys_by_uid[uid], value="1"))

        await session.commit()

    sent = 0
    for uid, text, kb in send_payloads:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=kb)
            sent += 1
        except Exception as e:
            # Тихий фейл: пользователь мог заблокировать бота или недоступен.
            if is_blocked_send_error(e):
                async with SessionLocal() as session:
                    await mark_user_blocked(session, int(uid))
                    await session.commit()
        if EXACT_HIT_PUSH_DELAY_SEC > 0:
            await asyncio.sleep(EXACT_HIT_PUSH_DELAY_SEC)
    return sent


def _enroll_page_bounds(total: int, page: int) -> tuple[int, int, int]:
    if total <= 0:
        return 1, 0, 0
    total_pages = (total + ENROLL_LIST_PAGE_SIZE - 1) // ENROLL_LIST_PAGE_SIZE
    page = max(1, min(page, total_pages))
    start = (page - 1) * ENROLL_LIST_PAGE_SIZE
    end = min(start + ENROLL_LIST_PAGE_SIZE, total)
    return page, start, end


def _admin_enroll_keyboard(rows: list, page: int, total_pages: int) -> types.InlineKeyboardMarkup:
    kb_rows: list[list[types.InlineKeyboardButton]] = []
    for item in rows:
        uid = int(item.tg_user_id)
        name = str(item.display_name)
        if len(name) > 22:
            name = name[:21].rstrip() + "…"
        kb_rows.append(
            [
                types.InlineKeyboardButton(text=f"⬆️ {name}", callback_data=f"admin_enroll_set:{uid}:HIGH:{page}"),
                types.InlineKeyboardButton(text=f"⬇️ {name}", callback_data=f"admin_enroll_set:{uid}:LOW:{page}"),
            ]
        )
    nav: list[types.InlineKeyboardButton] = []
    if page > 1:
        nav.append(types.InlineKeyboardButton(text="◀️", callback_data=f"admin_enroll_page:{page-1}"))
    nav.append(types.InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="admin_enroll_page:0"))
    if page < total_pages:
        nav.append(types.InlineKeyboardButton(text="▶️", callback_data=f"admin_enroll_page:{page+1}"))
    kb_rows.append(nav)
    return types.InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def _render_admin_enroll_list(target, page: int = 1) -> None:
    async with SessionLocal() as session:
        users = await list_unassigned_enrolled_users(session)
    total = len(users)
    if total == 0:
        await target.answer("✅ Все вступившие уже распределены по лигам.")
        return
    page, start, end = _enroll_page_bounds(total, page)
    page_rows = users[start:end]
    total_pages = (total + ENROLL_LIST_PAGE_SIZE - 1) // ENROLL_LIST_PAGE_SIZE
    text_lines = [
        f"🧩 Нераспределённые участники: {total}",
        f"Страница: {page}/{total_pages}",
        "",
        "Нажми ⬆️ или ⬇️ напротив имени:",
    ]
    await target.answer(
        "\n".join(text_lines),
        reply_markup=_admin_enroll_keyboard(page_rows, page=page, total_pages=total_pages),
    )


async def admin_enroll_list(message: types.Message):
    """/admin_enroll_list — список вступивших и нераспределённых по лигам."""
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return
    await _render_admin_enroll_list(message, page=1)


async def admin_enroll_page(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    try:
        page = int(data.split(":")[1])
    except Exception:
        await callback.answer("Ошибка страницы", show_alert=True)
        return
    if page == 0:
        await callback.answer()
        return
    await _render_admin_enroll_list(callback.message, page=page)
    await callback.answer()


async def admin_enroll_assign_from_list(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    try:
        _, uid_s, league_code, page_s = data.split(":")
        tg_user_id = int(uid_s)
        page = int(page_s)
    except Exception:
        await callback.answer("Ошибка назначения", show_alert=True)
        return
    league_code = league_code.upper().strip()
    if league_code not in {"HIGH", "LOW"}:
        await callback.answer("Неверная лига", show_alert=True)
        return

    async with SessionLocal() as session:
        result = await assign_user_to_active_stage_league(session, tg_user_id=tg_user_id, league_code=league_code)
        await session.commit()

    if result is None:
        await callback.message.answer("Не удалось назначить участника. Проверь /admin_season_init.")
        await callback.answer("Ошибка")
        return
    display_name, league_name = result
    await callback.message.answer(f"✅ {display_name} -> {league_name}")
    await _render_admin_enroll_list(callback.message, page=page)
    await callback.answer("Назначено")


async def admin_league_assign_name(message: types.Message):
    """
    /admin_league_assign_name <display_name> <HIGH|LOW>
    Пример: /admin_league_assign_name Слава HIGH
    """
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    raw = (message.text or "").strip()
    prefix = "/admin_league_assign_name"
    payload = raw[len(prefix):].strip() if raw.startswith(prefix) else ""
    if not payload:
        await message.answer("Формат: /admin_league_assign_name <имя в боте> <HIGH|LOW>")
        return

    try:
        name_part, league_code = payload.rsplit(maxsplit=1)
    except ValueError:
        await message.answer("Формат: /admin_league_assign_name <имя в боте> <HIGH|LOW>")
        return
    league_code = league_code.upper().strip()
    if league_code not in {"HIGH", "LOW"}:
        await message.answer("Лига должна быть HIGH или LOW.")
        return

    target_name = name_part.strip().strip("\"'").strip()
    if not target_name:
        await message.answer("Имя пустое. Пример: /admin_league_assign_name Слава HIGH")
        return

    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            await message.answer("Активный сезон не найден. Сначала /admin_season_init")
            return
        stage = await get_active_stage(session, season.id)
        if stage is None:
            await message.answer("Активный этап не найден. Сначала /admin_season_init")
            return
        rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = rpl_q.scalar_one_or_none()
        if rpl is None:
            await message.answer("Турнир RPL не найден.")
            return

        members_q = await session.execute(
            select(UserTournament.tg_user_id, UserTournament.display_name)
            .where(UserTournament.tournament_id == rpl.id)
            .order_by(UserTournament.tg_user_id.asc())
        )
        members = members_q.all()
        if not members:
            await message.answer("В турнире пока нет участников.")
            return

        member_ids = [int(x[0]) for x in members]
        users_q = await session.execute(
            select(User.tg_user_id, User.display_name, User.full_name, User.username).where(
                User.tg_user_id.in_(member_ids)
            )
        )
        user_map = {
            int(tg_id): {
                "display_name": u_display_name,
                "full_name": full_name,
                "username": username,
            }
            for tg_id, u_display_name, full_name, username in users_q.all()
        }

        candidates: list[tuple[int, str]] = []
        for tg_user_id_raw, ut_display_name in members:
            uid = int(tg_user_id_raw)
            u = user_map.get(uid, {})
            effective_name = (
                (ut_display_name or "").strip()
                or (str(u.get("display_name") or "").strip())
                or (str(u.get("full_name") or "").strip())
                or (f"@{u.get('username')}" if u.get("username") else "")
                or str(uid)
            )
            candidates.append((uid, effective_name))

        target_lc = target_name.lower()
        exact_rows = [(uid, nm) for uid, nm in candidates if nm.lower() == target_lc]
        if not exact_rows:
            exact_rows = [(uid, nm) for uid, nm in candidates if target_lc in nm.lower()]

        if not exact_rows:
            await message.answer("Совпадений по имени не найдено.")
            return

        if len(exact_rows) > 1:
            opts = "\n".join([f"• {name} (id: {uid})" for uid, name in exact_rows[:20]])
            await message.answer(
                "Найдено несколько совпадений. Уточни имя точнее или назначь по id:\n"
                f"{opts}"
            )
            return

        tg_user_id, _name = exact_rows[0]
        result = await assign_user_to_active_stage_league(session, tg_user_id=tg_user_id, league_code=league_code)
        await session.commit()
        if result is None:
            await message.answer("Не удалось назначить. Проверь /admin_season_init.")
            return
        display_name, league_name = result
        await message.answer(f"✅ {display_name} назначен(а) в «{league_name}».")


async def admin_season_init(message: types.Message):
    """
    /admin_season_init [Название сезона]
    Полный Week1-reset для новой структуры: сезоны/этапы/лиги.
    """
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    season_name = parts[1].strip() if len(parts) > 1 and parts[1].strip() else DEFAULT_SEASON_NAME

    async with SessionLocal() as session:
        summary = await setup_new_season_foundation(
            session=session,
            season_name=season_name,
            stage_1_round_min=1,
            stage_1_round_max=17,
            stage_2_round_min=18,
            stage_2_round_max=30,
        )
        await session.commit()

    await message.answer(
        "✅ Основа нового цикла создана.\n"
        f"Сезон: {summary.season_name}\n"
        "Этапы: 1-17 (Осенний), 18-30 (Весенний)\n"
        "Лиги: Высшая / Низшая\n"
        "Набор участников: закрыт"
    )


async def admin_set_season_name(message: types.Message):
    """/admin_set_season_name Новое название"""
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Формат: /admin_set_season_name РПЛ 2026/27")
        return

    async with SessionLocal() as session:
        season = await set_active_season_name(session, parts[1].strip())
        await session.commit()
        if season is None:
            await message.answer("Активный сезон не найден. Сначала запусти /admin_season_init")
            return

    await message.answer(f"✅ Название активного сезона обновлено: {parts[1].strip()}")


async def admin_enroll_open(message: types.Message):
    """/admin_enroll_open — открыть набор участников в сезон."""
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    async with SessionLocal() as session:
        await set_enrollment_open(session, True)
        await session.commit()
    await message.answer("🟢 Набор участников открыт.")


async def admin_enroll_close(message: types.Message):
    """/admin_enroll_close — закрыть набор участников в сезон."""
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    async with SessionLocal() as session:
        await set_enrollment_open(session, False)
        await session.commit()
    await message.answer("🔒 Набор участников закрыт.")


async def admin_league_assign(message: types.Message):
    """
    /admin_league_assign <tg_user_id> <HIGH|LOW>
    Пример: /admin_league_assign 210477579 HIGH
    """
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    raw = (message.text or "").strip()
    parts = raw.split()
    if len(parts) != 3:
        await message.answer("Формат: /admin_league_assign <tg_user_id> <HIGH|LOW>")
        return

    try:
        tg_user_id = int(parts[1])
    except ValueError:
        await message.answer("tg_user_id должен быть числом.")
        return

    league_code = parts[2].upper().strip()
    if league_code not in {"HIGH", "LOW"}:
        await message.answer("Лига должна быть HIGH или LOW.")
        return

    async with SessionLocal() as session:
        result = await assign_user_to_active_stage_league(session, tg_user_id=tg_user_id, league_code=league_code)
        await session.commit()
        if result is None:
            await message.answer("Не удалось назначить. Сначала запусти /admin_season_init.")
            return
        display_name, league_name = result
    await message.answer(f"✅ {display_name} назначен(а) в «{league_name}».")


async def admin_season_status(message: types.Message):
    """/admin_season_status — статус нового контура: сезон/этап/лиги/набор."""
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            await message.answer("Активный сезон не найден. Запусти /admin_season_init.")
            return

        stage = await get_active_stage(session, season.id)
        enroll_open = await is_enrollment_open(session)

        stages_q = await session.execute(
            select(Stage).where(Stage.season_id == season.id).order_by(Stage.stage_order.asc())
        )
        stages = stages_q.scalars().all()

        leagues_q = await session.execute(
            select(League).where(League.season_id == season.id, League.is_active == 1).order_by(League.code.asc())
        )
        leagues = leagues_q.scalars().all()
        league_ids = [int(x.id) for x in leagues]

        counts: dict[int, int] = {}
        joined_total = 0
        unassigned_total = 0
        if stage is not None and league_ids:
            c_q = await session.execute(
                select(LeagueParticipant.league_id, func.count(LeagueParticipant.id))
                .where(
                    LeagueParticipant.stage_id == stage.id,
                    LeagueParticipant.league_id.in_(league_ids),
                    LeagueParticipant.is_active == 1,
                )
                .group_by(LeagueParticipant.league_id)
            )
            counts = {int(league_id): int(cnt) for league_id, cnt in c_q.all()}

            rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
            rpl = rpl_q.scalar_one_or_none()
            if rpl is not None:
                joined_q = await session.execute(
                    select(func.count(UserTournament.id)).where(UserTournament.tournament_id == rpl.id)
                )
                joined_total = int(joined_q.scalar_one() or 0)

                assigned_ids_q = await session.execute(
                    select(LeagueParticipant.tg_user_id).where(
                        LeagueParticipant.stage_id == stage.id,
                        LeagueParticipant.is_active == 1,
                    )
                )
                assigned_ids = {int(x[0]) for x in assigned_ids_q.all()}

                member_ids_q = await session.execute(
                    select(UserTournament.tg_user_id).where(UserTournament.tournament_id == rpl.id)
                )
                member_ids = {int(x[0]) for x in member_ids_q.all()}
                unassigned_total = len(member_ids - assigned_ids)

    stage_lines = []
    for st in stages:
        marker = "⭐" if stage and int(st.id) == int(stage.id) else "•"
        stage_lines.append(f"{marker} {st.name}: туры {st.round_min}-{st.round_max}")

    league_lines = []
    for lg in leagues:
        cnt = counts.get(int(lg.id), 0)
        league_lines.append(f"• {lg.name}: {cnt} участ.")

    lines = [
        "🏁 Новый формат — Week1 статус",
        f"Сезон: {season.name}",
        f"Набор в сезон: {'ОТКРЫТ' if enroll_open else 'закрыт'}",
        "",
        "Этапы:",
        *stage_lines,
        "",
        "Лиги (в активном этапе):",
        *league_lines,
        "",
        f"Вступили в РПЛ: {joined_total}",
        f"Не распределены по лигам: {unassigned_total}",
    ]
    await message.answer("\n".join(lines))


def _derive_next_season_name(current_name: str) -> str:
    # "РПЛ 2026/27" -> "РПЛ 2027/28"
    m = re.search(r"(\d{4})\s*/\s*(\d{2,4})", current_name or "")
    if not m:
        return f"{(current_name or 'РПЛ').strip()} (новый сезон)"
    y1 = int(m.group(1))
    y2_raw = m.group(2)
    if len(y2_raw) == 2:
        century = (y1 // 100) * 100
        y2 = century + int(y2_raw)
    else:
        y2 = int(y2_raw)
    next_y1 = y1 + 1
    next_y2 = y2 + 1
    tail = str(next_y2 % 100).zfill(2)
    return re.sub(r"(\d{4})\s*/\s*(\d{2,4})", f"{next_y1}/{tail}", current_name, count=1)


async def admin_stage_finish(message: types.Message):
    """
    /admin_stage_finish
    Закрывает активный этап, делает 2↑/2↓, открывает следующий этап
    (или создаёт новый сезон и открывает его этап 1).
    """
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    # Рейтинг по активному этапу и лигам считаем тем же движком, что и таблицу пользователя.
    high_rows, high_meta = await build_active_stage_league_table(message.from_user.id, requested_league_code="HIGH")
    low_rows, low_meta = await build_active_stage_league_table(message.from_user.id, requested_league_code="LOW")

    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            await message.answer("Активный сезон не найден. Сначала /admin_season_init")
            return
        stage = await get_active_stage(session, season.id)
        if stage is None:
            await message.answer("Активный этап не найден. Сначала /admin_season_init")
            return
        if int(stage.is_completed or 0) == 1:
            await message.answer("Этот этап уже завершён.")
            return

        # Не закрываем этап, пока есть матчи без итогов в его окне.
        rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = rpl_q.scalar_one_or_none()
        if rpl is None:
            await message.answer("Турнир RPL не найден.")
            return
        pending_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == rpl.id,
                Match.round_number >= int(stage.round_min),
                Match.round_number <= int(stage.round_max),
                (Match.home_score.is_(None) | Match.away_score.is_(None)),
            )
        )
        pending = int(pending_q.scalar_one() or 0)
        if pending > 0:
            await message.answer(
                "Нельзя завершить этап: есть матчи без результатов.\n"
                f"Незаполненных матчей в окне этапа: {pending}"
            )
            return

        leagues_q = await session.execute(
            select(League).where(League.season_id == season.id, League.is_active == 1)
        )
        leagues = leagues_q.scalars().all()
        by_code = {str(x.code).upper(): x for x in leagues}
        high_league = by_code.get("HIGH")
        low_league = by_code.get("LOW")
        if high_league is None or low_league is None:
            await message.answer("Не найдены лиги HIGH/LOW в активном сезоне.")
            return

        move_up_count = min(int(stage.promote_count or 2), len(low_rows))
        move_down_count = min(int(stage.relegate_count or 2), len(high_rows))
        move_up_ids = [int(r["tg_user_id"]) for r in low_rows[:move_up_count]]
        move_down_ids = [int(r["tg_user_id"]) for r in high_rows[-move_down_count:]] if move_down_count > 0 else []

        # Текущий состав активного этапа.
        curr_rows_q = await session.execute(
            select(LeagueParticipant).where(
                LeagueParticipant.stage_id == stage.id,
                LeagueParticipant.is_active == 1,
            )
        )
        curr_rows = curr_rows_q.scalars().all()
        if not curr_rows:
            await message.answer("В активном этапе нет участников.")
            return

        next_stage_q = await session.execute(
            select(Stage)
            .where(Stage.season_id == season.id, Stage.stage_order == int(stage.stage_order) + 1)
            .order_by(Stage.id.asc())
        )
        next_stage = next_stage_q.scalars().first()

        target_season = season
        if next_stage is None:
            # Этап 2 закрыт -> запускаем новый сезон с этапами 1/2.
            season.is_active = 0
            new_name = _derive_next_season_name(str(season.name))
            target_season = Season(name=new_name, is_active=1)
            session.add(target_season)
            await session.flush()

            next_stage = Stage(
                season_id=target_season.id,
                name=DEFAULT_STAGE_1_NAME,
                stage_order=1,
                round_min=1,
                round_max=17,
                is_active=1,
                is_completed=0,
                promote_count=int(stage.promote_count or 2),
                relegate_count=int(stage.relegate_count or 2),
            )
            stage2 = Stage(
                season_id=target_season.id,
                name=DEFAULT_STAGE_2_NAME,
                stage_order=2,
                round_min=18,
                round_max=30,
                is_active=0,
                is_completed=0,
                promote_count=int(stage.promote_count or 2),
                relegate_count=int(stage.relegate_count or 2),
            )
            session.add_all([next_stage, stage2])
            await session.flush()

            new_high = League(season_id=target_season.id, code="HIGH", name="Высшая лига", is_active=1)
            new_low = League(season_id=target_season.id, code="LOW", name="Низшая лига", is_active=1)
            session.add_all([new_high, new_low])
            await session.flush()
            target_high_id = int(new_high.id)
            target_low_id = int(new_low.id)
        else:
            # Внутри текущего сезона: следующий этап уже есть.
            next_stage.is_active = 1
            target_high_id = int(high_league.id)
            target_low_id = int(low_league.id)

        # Закрываем активный этап.
        stage.is_active = 0
        stage.is_completed = 1

        # Перенос состава в next_stage с учётом обменов.
        moved_up_names: list[str] = []
        moved_down_names: list[str] = []
        move_up_set = set(move_up_ids)
        move_down_set = set(move_down_ids)

        for row in curr_rows:
            uid = int(row.tg_user_id)
            to_league_id = int(row.league_id)
            reason = "stay"
            if uid in move_up_set:
                to_league_id = target_high_id
                reason = "promotion"
                moved_up_names.append(str(row.display_name or uid))
            elif uid in move_down_set:
                to_league_id = target_low_id
                reason = "relegation"
                moved_down_names.append(str(row.display_name or uid))
            else:
                if int(row.league_id) == int(high_league.id):
                    to_league_id = target_high_id
                elif int(row.league_id) == int(low_league.id):
                    to_league_id = target_low_id

            existing_q = await session.execute(
                select(LeagueParticipant).where(
                    LeagueParticipant.stage_id == next_stage.id,
                    LeagueParticipant.tg_user_id == uid,
                )
            )
            existing = existing_q.scalar_one_or_none()
            if existing is None:
                session.add(
                    LeagueParticipant(
                        season_id=target_season.id,
                        stage_id=next_stage.id,
                        league_id=to_league_id,
                        tg_user_id=uid,
                        display_name=row.display_name,
                        is_active=1,
                    )
                )
            else:
                existing.league_id = to_league_id
                existing.display_name = row.display_name
                existing.is_active = 1

            if reason in {"promotion", "relegation"}:
                session.add(
                    LeagueMovement(
                        season_id=target_season.id,
                        from_stage_id=stage.id,
                        to_stage_id=next_stage.id,
                        tg_user_id=uid,
                        from_league_id=int(row.league_id),
                        to_league_id=to_league_id,
                        reason=reason,
                    )
                )

        await session.commit()

    up_text = ", ".join(moved_up_names) if moved_up_names else "—"
    down_text = ", ".join(moved_down_names) if moved_down_names else "—"
    await message.answer(
        "✅ Этап завершён.\n"
        f"Повышены: {up_text}\n"
        f"Понижены: {down_text}\n\n"
        "Текущий статус проверь через /admin_season_status"
    )


async def admin_stage_moves(message: types.Message):
    """/admin_stage_moves — показывает последний пакет переходов между лигами."""
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    async with SessionLocal() as session:
        last_q = await session.execute(select(LeagueMovement).order_by(LeagueMovement.id.desc()).limit(1))
        last = last_q.scalar_one_or_none()
        if last is None:
            await message.answer("Переходов пока нет.")
            return

        pack_q = await session.execute(
            select(LeagueMovement).where(
                LeagueMovement.from_stage_id == last.from_stage_id,
                LeagueMovement.to_stage_id == last.to_stage_id,
            )
        )
        pack = pack_q.scalars().all()
        if not pack:
            await message.answer("Переходов пока нет.")
            return

        user_ids = {int(x.tg_user_id) for x in pack}
        stage_ids = {int(last.from_stage_id), int(last.to_stage_id)}
        league_ids = {int(x.from_league_id) for x in pack} | {int(x.to_league_id) for x in pack}

        users_q = await session.execute(
            select(User.tg_user_id, UserTournament.display_name, User.display_name, User.username, User.full_name)
            .select_from(User)
            .outerjoin(UserTournament, UserTournament.tg_user_id == User.tg_user_id)
            .where(User.tg_user_id.in_(user_ids))
        )
        name_map = {}
        for uid, ut_name, u_name, username, full_name in users_q.all():
            name_map[int(uid)] = _format_person_name(int(uid), ut_name, u_name, username, full_name)

        stages_q = await session.execute(select(Stage.id, Stage.name).where(Stage.id.in_(stage_ids)))
        leagues_q = await session.execute(select(League.id, League.name).where(League.id.in_(league_ids)))
        stage_map = {int(i): str(n) for i, n in stages_q.all()}
        league_map = {int(i): str(n) for i, n in leagues_q.all()}

    ups = [name_map.get(int(x.tg_user_id), str(x.tg_user_id)) for x in pack if str(x.reason) == "promotion"]
    downs = [name_map.get(int(x.tg_user_id), str(x.tg_user_id)) for x in pack if str(x.reason) == "relegation"]
    from_stage = stage_map.get(int(last.from_stage_id), str(last.from_stage_id))
    to_stage = stage_map.get(int(last.to_stage_id), str(last.to_stage_id))
    lines = [
        "🔁 Последние переходы",
        f"{from_stage} -> {to_stage}",
        f"⬆️ Повышены: {', '.join(ups) if ups else '—'}",
        f"⬇️ Понижены: {', '.join(downs) if downs else '—'}",
    ]
    await message.answer("\n".join(lines))


def register_admin_handlers(dp: Dispatcher) -> None:
    dp.message.register(admin_panel, Command("admin_panel"))
    dp.message.register(admin_status, Command("admin_status"))
    dp.message.register(admin_round_progress, Command("admin_round_progress"))
    dp.message.register(admin_missing, Command("admin_missing"))
    dp.callback_query.register(admin_panel_click, F.data.startswith("admin_panel:"))
    dp.callback_query.register(admin_pick_tournament_for_progress, F.data.startswith("admin_progress_t:"))
    dp.callback_query.register(admin_pick_tournament_for_missing, F.data.startswith("admin_missing_t:"))
    dp.callback_query.register(admin_pick_round_for_progress, F.data.startswith("admin_progress_r:"))
    dp.callback_query.register(admin_pick_round_for_missing, F.data.startswith("admin_missing_r:"))

    dp.message.register(admin_add_match, Command("admin_add_match"))
    dp.message.register(admin_set_result, Command("admin_set_result"))
    dp.callback_query.register(admin_set_result_pick_round, F.data.startswith("admin_res_r:"))
    dp.callback_query.register(admin_set_result_pick_match, F.data.startswith("admin_res_m:"))
    dp.message.register(admin_set_result_score_input, AdminSetResultStates.waiting_for_score)
    dp.message.register(admin_recalc, Command("admin_recalc"))
    dp.message.register(admin_manual_only_cleanup, Command("admin_manual_only_cleanup"))
    dp.message.register(admin_health, Command("admin_health"))

    # Новое: управление окном турнира и удаление участников
    dp.message.register(admin_set_window, Command("admin_set_window"))
    dp.message.register(admin_remove_user, Command("admin_remove_user"))
    dp.message.register(admin_audience, Command("admin_audience"))
    dp.message.register(admin_audience_list, Command("admin_audience_list"))
    dp.message.register(admin_season_init, Command("admin_season_init"))
    dp.message.register(admin_set_season_name, Command("admin_set_season_name"))
    dp.message.register(admin_enroll_open, Command("admin_enroll_open"))
    dp.message.register(admin_enroll_close, Command("admin_enroll_close"))
    dp.message.register(admin_enroll_list, Command("admin_enroll_list"))
    dp.callback_query.register(admin_enroll_page, F.data.startswith("admin_enroll_page:"))
    dp.callback_query.register(admin_enroll_assign_from_list, F.data.startswith("admin_enroll_set:"))
    dp.message.register(admin_league_assign, Command("admin_league_assign"))
    dp.message.register(admin_league_assign_name, Command("admin_league_assign_name"))
    dp.message.register(admin_season_status, Command("admin_season_status"))
    dp.message.register(admin_stage_finish, Command("admin_stage_finish"))
    dp.message.register(admin_stage_moves, Command("admin_stage_moves"))


async def admin_add_match(message: types.Message):
    """
    /admin_add_match 19 | TeamA | TeamB | YYYY-MM-DD HH:MM
    Время — как и раньше в проекте: МСК считаем просто "как введено" (UTC+3 без zoneinfo).
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    text = (message.text or "").strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) != 4:
        await message.answer(f"Формат: /admin_add_match {ROUND_DEFAULT} | TeamA | TeamB | YYYY-MM-DD HH:MM")
        return

    try:
        round_number = int(parts[0].split(maxsplit=1)[1])
    except Exception:
        await message.answer(f"Не смог прочитать номер тура. Пример: /admin_add_match {ROUND_DEFAULT} | ...")
        return

    if not is_tournament_round(round_number):
        await message.answer(
            f"Можно добавлять матчи только для туров {ROUND_MIN}..{ROUND_MAX}. "
            f"Пример: /admin_add_match {ROUND_DEFAULT} | TeamA | TeamB | YYYY-MM-DD HH:MM"
        )
        return

    home = parts[1]
    away = parts[2]
    dt_str = parts[3]

    kickoff = _parse_admin_kickoff_datetime(dt_str)
    if kickoff is None:
        await message.answer("Не смог прочитать дату. Формат: YYYY-MM-DD HH:MM (пример: 2026-03-01 19:00)")
        return

    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = t_q.scalar_one_or_none()
        tournament_id = rpl.id if rpl is not None else 1

        m = Match(
            tournament_id=tournament_id,
            round_number=round_number,
            home_team=home,
            away_team=away,
            kickoff_time=kickoff,
            source="manual",
        )
        session.add(m)
        await session.commit()

        await message.answer(f"✅ Матч добавлен: #{m.id} | тур {round_number} | {home} — {away} | {dt_str} (МСК)")


async def admin_set_result(message: types.Message):
    """
    /admin_set_result <match_id> <score>
    score: 2:0 или 2-0
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    parts = (message.text or "").strip().split()
    if len(parts) == 1:
        await _admin_set_result_open_tournament_picker(message)
        return
    if len(parts) != 3:
        await message.answer(
            "Формат:\n"
            "1) /admin_set_result (кнопки выбора)\n"
            "2) /admin_set_result <match_id> <score> (пример: /admin_set_result 12 2:0)"
        )
        return

    try:
        match_id = int(parts[1])
    except ValueError:
        await message.answer("match_id должен быть числом.")
        return

    parsed = _parse_score(parts[2])
    if not parsed:
        await message.answer("Счёт должен быть формата 2:0 или 2-0")
        return
    home_score, away_score = parsed

    async with SessionLocal() as session:
        res = await session.execute(select(Match).where(Match.id == match_id))
        match = res.scalar_one_or_none()
        if not match:
            await message.answer("Матч не найден.")
            return

        match.home_score = home_score
        match.away_score = away_score
        await session.commit()

        updates = await recalc_points_for_match_in_session(session, match_id)

    sent_exact_pushes = await _maybe_send_exact_hit_pushes(message.bot, match.id)
    await message.answer(
        f"✅ Результат сохранён: {display_team_name(match.home_team)} — {display_team_name(match.away_team)} | {home_score}:{away_score}. "
        f"Пересчитано очков: {updates}"
    )
    if sent_exact_pushes > 0:
        await message.answer(f"📬 Пуш «точный счёт» отправлен: {sent_exact_pushes}")
    live_text, round_closed = await _build_admin_match_result_live_update(match.id)
    if live_text:
        await message.answer(live_text)
    if round_closed:
        await message.answer(
            f"🏁 Похоже, тур {match.round_number} закрыт полностью.\n"
            "Все матчи тура с результатами.\n\n"
            "Можно посмотреть:\n"
            f"• /mvp_round {match.round_number}\n"
            f"• /tops_round {match.round_number}\n"
            f"• /round_digest {match.round_number}"
        )
    await _maybe_send_round_closed_summary(message.bot, tournament_id=match.tournament_id, round_number=match.round_number)


async def _admin_set_result_open_tournament_picker(message: types.Message) -> None:
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        tournament = t_q.scalar_one_or_none()
        if tournament is None:
            await message.answer("Турнир RPL не найден.")
            return

        rounds_q = await session.execute(
            select(Match.round_number)
            .where(
                Match.tournament_id == tournament.id,
                Match.source == "manual",
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        round_numbers = [int(r[0]) for r in rounds_q.all()]

    if not round_numbers:
        await message.answer(f"В турнире {display_tournament_name(tournament.name)} нет матчей.")
        return

    rows = []
    for rnd in round_numbers:
        rows.append([types.InlineKeyboardButton(text=f"Тур {rnd}", callback_data=f"admin_res_r:{tournament.id}:{rnd}")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer(f"Турнир: {display_tournament_name(tournament.name)}\nВыбери тур:", reply_markup=kb)


async def admin_set_result_pick_tournament(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав", show_alert=True)
        return

    data = callback.data or ""
    try:
        tournament_id = int(data.split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка выбора турнира", show_alert=True)
        return

    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        tournament = t_q.scalar_one_or_none()
        if tournament is None:
            await callback.answer("Турнир не найден", show_alert=True)
            return

        rounds_q = await session.execute(
            select(Match.round_number)
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        round_numbers = [int(r[0]) for r in rounds_q.all()]

        if not round_numbers:
            await callback.message.answer(f"В турнире {display_tournament_name(tournament.name)} нет матчей.")
            await callback.answer()
            return

    rows = []
    for rnd in round_numbers:
        rows.append([types.InlineKeyboardButton(text=f"Тур {rnd}", callback_data=f"admin_res_r:{tournament_id}:{rnd}")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.answer(
        f"Турнир: {display_tournament_name(tournament.name)}\nВыбери тур:",
        reply_markup=kb,
    )
    await callback.answer()


async def admin_set_result_pick_round(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав", show_alert=True)
        return

    data = callback.data or ""
    try:
        _, payload = data.split(":", 1)
        tournament_id_s, round_s = payload.split(":", 1)
        tournament_id = int(tournament_id_s)
        round_number = int(round_s)
    except Exception:
        await callback.answer("Ошибка выбора тура", show_alert=True)
        return

    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        tournament = t_q.scalar_one_or_none()
        if tournament is None:
            await callback.answer("Турнир не найден", show_alert=True)
            return

        matches_q = await session.execute(
            select(Match)
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
                Match.round_number == round_number,
            )
            .order_by(Match.kickoff_time.asc(), Match.id.asc())
        )
        matches = matches_q.scalars().all()

    if not matches:
        await callback.message.answer("В этом туре нет матчей.")
        await callback.answer()
        return

    rows = []
    for m in matches:
        result = f"{m.home_score}:{m.away_score}" if m.home_score is not None and m.away_score is not None else "без итога"
        txt = (
            f"{display_team_name(m.home_team)} — {display_team_name(m.away_team)} "
            f"| {m.kickoff_time.strftime('%d.%m %H:%M')} | {result}"
        )
        rows.append([types.InlineKeyboardButton(text=txt, callback_data=f"admin_res_m:{m.id}")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.answer(
        f"Турнир: {display_tournament_name(tournament.name)}\nТур: {round_number}\nВыбери матч:",
        reply_markup=kb,
    )
    await callback.answer()


async def admin_set_result_pick_match(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет прав", show_alert=True)
        return

    data = callback.data or ""
    try:
        match_id = int(data.split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка выбора матча", show_alert=True)
        return

    async with SessionLocal() as session:
        q = await session.execute(select(Match).where(Match.id == match_id))
        match = q.scalar_one_or_none()
    if match is None:
        await callback.answer("Матч не найден", show_alert=True)
        return

    await state.set_state(AdminSetResultStates.waiting_for_score)
    await state.update_data(admin_result_match_id=match_id)
    await callback.message.answer(
        f"Матч: {display_team_name(match.home_team)} — {display_team_name(match.away_team)}\n"
        "Отправь только счёт: 2:1"
    )
    await callback.answer()


async def admin_set_result_score_input(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.clear()
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    data = await state.get_data()
    match_id = int(data.get("admin_result_match_id") or 0)
    if match_id <= 0:
        await state.clear()
        await message.answer("Сессия сброшена. Запусти /admin_set_result заново.")
        return

    parsed = _parse_score(message.text or "")
    if not parsed:
        await message.answer("Счёт должен быть формата 2:0 или 2-0")
        return
    home_score, away_score = parsed

    async with SessionLocal() as session:
        res = await session.execute(select(Match).where(Match.id == match_id))
        match = res.scalar_one_or_none()
        if not match:
            await state.clear()
            await message.answer("Матч не найден.")
            return

        match.home_score = home_score
        match.away_score = away_score
        await session.commit()

        updates = await recalc_points_for_match_in_session(session, match_id)

    await state.clear()
    sent_exact_pushes = await _maybe_send_exact_hit_pushes(message.bot, match.id)
    await message.answer(
        f"✅ Результат сохранён: {display_team_name(match.home_team)} — {display_team_name(match.away_team)} | {home_score}:{away_score}. "
        f"Пересчитано очков: {updates}"
    )
    if sent_exact_pushes > 0:
        await message.answer(f"📬 Пуш «точный счёт» отправлен: {sent_exact_pushes}")
    live_text, round_closed = await _build_admin_match_result_live_update(match.id)
    if live_text:
        await message.answer(live_text)
    if round_closed:
        await message.answer(
            f"🏁 Похоже, тур {match.round_number} закрыт полностью.\n"
            "Все матчи тура с результатами.\n\n"
            "Можно посмотреть:\n"
            f"• /mvp_round {match.round_number}\n"
            f"• /tops_round {match.round_number}\n"
            f"• /round_digest {match.round_number}"
        )
    await _maybe_send_round_closed_summary(message.bot, tournament_id=match.tournament_id, round_number=match.round_number)


async def admin_recalc(message: types.Message):
    """/admin_recalc — пересчитать всё"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    total_updates = 0
    async with SessionLocal() as session:
        res = await session.execute(select(Match))
        matches = res.scalars().all()

        for m in matches:
            if m.home_score is None or m.away_score is None:
                continue
            total_updates += await recalc_points_for_match_in_session(session, m.id)

    await message.answer(f"✅ Пересчёт завершён. Обновлений: {total_updates}")


async def admin_manual_only_cleanup(message: types.Message):
    """/admin_manual_only_cleanup — удалить всё API и оставить только ручной RPL"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    async with SessionLocal() as session:
        rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = rpl_q.scalar_one_or_none()
        if rpl is None:
            await message.answer("❌ Турнир RPL не найден. Операция остановлена.")
            return

        rpl.round_min = 19
        rpl.round_max = 30

        non_rpl_ids_q = await session.execute(select(Tournament.id).where(Tournament.id != rpl.id))
        non_rpl_ids = [int(x[0]) for x in non_rpl_ids_q.all()]

        bad_ids_subq = select(Match.id).where(
            (Match.source != "manual")
            | Match.api_fixture_id.isnot(None)
            | (Match.tournament_id != rpl.id)
        )
        del_points = await session.execute(delete(Point).where(Point.match_id.in_(bad_ids_subq)))
        del_preds = await session.execute(delete(Prediction).where(Prediction.match_id.in_(bad_ids_subq)))
        del_matches = await session.execute(delete(Match).where(Match.id.in_(bad_ids_subq)))

        del_ut_non_rpl = 0
        del_tournaments = 0
        if non_rpl_ids:
            del_ut_res = await session.execute(delete(UserTournament).where(UserTournament.tournament_id.in_(non_rpl_ids)))
            del_ut_non_rpl = int(del_ut_res.rowcount or 0)

            del_t_res = await session.execute(delete(Tournament).where(Tournament.id.in_(non_rpl_ids)))
            del_tournaments = int(del_t_res.rowcount or 0)

        rpl_member_ids_subq = select(UserTournament.tg_user_id).where(UserTournament.tournament_id == rpl.id)

        # Удаляем пользователей, не состоящих в RPL (включая следы их данных).
        del_user_points = await session.execute(delete(Point).where(Point.tg_user_id.not_in(rpl_member_ids_subq)))
        del_user_preds = await session.execute(delete(Prediction).where(Prediction.tg_user_id.not_in(rpl_member_ids_subq)))
        del_users = await session.execute(delete(User).where(User.tg_user_id.not_in(rpl_member_ids_subq)))

        # Чистим настройки выхода из не-RPL турниров.
        left_keys_q = await session.execute(select(Setting).where(Setting.key.like("LEFT_T%_U%")))
        removed_left_keys = 0
        for row in left_keys_q.scalars().all():
            key = str(row.key or "")
            if key.startswith(f"LEFT_T{rpl.id}_"):
                continue
            await session.delete(row)
            removed_left_keys += 1

        await session.commit()

        left_matches = int(
            (
                await session.execute(
                    select(func.count(Match.id)).where(
                        Match.tournament_id == rpl.id,
                    )
                )
            ).scalar_one()
            or 0
        )
        left_members = int(
            (
                await session.execute(
                    select(func.count(func.distinct(UserTournament.tg_user_id))).where(
                        UserTournament.tournament_id == rpl.id
                    )
                )
            ).scalar_one()
            or 0
        )

    await message.answer(
        "✅ Manual-only cleanup завершён.\n"
        f"Удалено матчей (не-RPL/API): {int(del_matches.rowcount or 0)}\n"
        f"Удалено прогнозов по матчам: {int(del_preds.rowcount or 0)}\n"
        f"Удалено очков по матчам: {int(del_points.rowcount or 0)}\n"
        f"Удалено участий в не-RPL: {del_ut_non_rpl}\n"
        f"Удалено турниров не-RPL: {del_tournaments}\n"
        f"Удалено прогнозов пользователей вне RPL: {int(del_user_preds.rowcount or 0)}\n"
        f"Удалено очков пользователей вне RPL: {int(del_user_points.rowcount or 0)}\n"
        f"Удалено пользователей вне RPL: {int(del_users.rowcount or 0)}\n"
        f"Удалено LEFT-настроек не-RPL: {removed_left_keys}\n"
        f"Осталось матчей в RPL: {left_matches}\n"
        f"Осталось участников в RPL: {left_members}\n"
        "Теперь бот полностью очищен от других турниров."
    )


async def admin_health(message: types.Message):
    """/admin_health — диагностика БД"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    async with SessionLocal() as session:
        users = (await session.execute(select(func.count(User.id)))).scalar() or 0
        matches = (await session.execute(select(func.count(Match.id)))).scalar() or 0
        preds = (await session.execute(select(func.count(Prediction.id)))).scalar() or 0
        points = (await session.execute(select(func.count(Point.id)))).scalar() or 0

    await message.answer(
        "🩺 DB health\n"
        f"users: {users}\n"
        f"matches: {matches}\n"
        f"predictions: {preds}\n"
        f"points: {points}"
    )


async def admin_set_window(message: types.Message):
    """
    /admin_set_window YYYY-MM-DD YYYY-MM-DD
    Сохраняем окно турнира в таблицу settings:
    - TOURNAMENT_START_DATE
    - TOURNAMENT_END_DATE
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    parts = (message.text or "").strip().split()
    if len(parts) != 3:
        await message.answer("Формат: /admin_set_window 2026-03-01 2026-05-31")
        return

    start_s = parts[1].strip()
    end_s = parts[2].strip()

    try:
        _ = datetime.fromisoformat(start_s).date()
        _ = datetime.fromisoformat(end_s).date()
    except Exception:
        await message.answer("Даты должны быть формата YYYY-MM-DD (пример: 2026-03-01)")
        return

    async with SessionLocal() as session:
        await _set_setting(session, "TOURNAMENT_START_DATE", start_s)
        await _set_setting(session, "TOURNAMENT_END_DATE", end_s)

    await message.answer(f"✅ Окно турнира установлено: {start_s} .. {end_s}")


async def admin_remove_user(message: types.Message):
    """
    /admin_remove_user <tg_user_id>
    Удаляет пользователя из users и чистит его predictions/points (по tg_user_id).
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    parts = (message.text or "").strip().split()
    if len(parts) != 2:
        await message.answer("Формат: /admin_remove_user 210477579")
        return

    try:
        tg_user_id = int(parts[1])
    except ValueError:
        await message.answer("tg_user_id должен быть числом.")
        return

    async with SessionLocal() as session:
        await session.execute(delete(Prediction).where(Prediction.tg_user_id == tg_user_id))
        await session.execute(delete(Point).where(Point.tg_user_id == tg_user_id))
        await session.execute(delete(UserTournament).where(UserTournament.tg_user_id == tg_user_id))
        await session.execute(delete(User).where(User.tg_user_id == tg_user_id))
        await session.commit()

    await message.answer(f"✅ Пользователь {tg_user_id} удалён (users + user_tournaments + predictions + points).")


async def admin_audience(message: types.Message):
    """/admin_audience — сводка аудитории бота"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    now = _now_msk_naive()

    async with SessionLocal() as session:
        rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = rpl_q.scalar_one_or_none()
        if rpl is None:
            await message.answer("Турнир RPL не найден.")
            return

        total_users_q = await session.execute(select(func.count(User.id)))
        total_users = int(total_users_q.scalar_one() or 0)

        all_user_ids_q = await session.execute(select(User.tg_user_id))
        all_user_ids = {int(x[0]) for x in all_user_ids_q.all()}

        any_members_q = await session.execute(select(func.distinct(UserTournament.tg_user_id)))
        any_member_ids = {int(x[0]) for x in any_members_q.all()}

        rpl_members_q = await session.execute(
            select(func.distinct(UserTournament.tg_user_id)).where(UserTournament.tournament_id == rpl.id)
        )
        rpl_member_ids = {int(x[0]) for x in rpl_members_q.all()}
        in_tournament_now = len(rpl_member_ids)

        never_joined_ids = all_user_ids - any_member_ids
        never_joined = len(never_joined_ids)

        blocked_q = await session.execute(
            select(Setting.key).where(Setting.key.like("BOT_BLOCKED_U%"))
        )
        blocked_user_ids: set[int] = set()
        for key_raw, in blocked_q.all():
            key = str(key_raw or "")
            suffix = key.replace("BOT_BLOCKED_U", "", 1).strip()
            if suffix.isdigit():
                blocked_user_ids.add(int(suffix))
        blocked_users = len(blocked_user_ids & all_user_ids)

        left_q = await session.execute(
            select(Setting.key).where(
                Setting.key.like(f"LEFT_T{int(rpl.id)}_U%"),
                Setting.value == "1",
            )
        )
        left_user_ids: set[int] = set()
        for key_raw, in left_q.all():
            uid = extract_left_user_id(str(key_raw or ""))
            if uid is not None:
                left_user_ids.add(uid)
        left_user_ids = left_user_ids & all_user_ids
        left_users = len(left_user_ids)

        outside_were_before_ids = left_user_ids - rpl_member_ids
        outside_were_before = len(outside_were_before_ids)

        rounds_q = await session.execute(
            select(
                Match.round_number,
                func.max(Match.kickoff_time).label("ends_at"),
            )
            .where(
                Match.tournament_id == rpl.id,
                Match.source == "manual",
                Match.round_number >= rpl.round_min,
                Match.round_number <= rpl.round_max,
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        round_rows = rounds_q.all()
        last_finished_round: int | None = None
        for r, ends_at in round_rows:
            if ends_at is not None and ends_at <= now:
                last_finished_round = int(r)

        season_preds_q = await session.execute(
            select(func.distinct(Prediction.tg_user_id))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Match.tournament_id == rpl.id,
                Match.source == "manual",
                Match.round_number >= rpl.round_min,
                Match.round_number <= rpl.round_max,
            )
        )
        season_pred_ids = {int(x[0]) for x in season_preds_q.all()}
        no_preds_ids = rpl_member_ids - season_pred_ids

        active_last_round_ids: set[int] = set()
        if last_finished_round is not None:
            active_q = await session.execute(
                select(func.distinct(Prediction.tg_user_id))
                .select_from(Prediction)
                .join(Match, Match.id == Prediction.match_id)
                .where(
                    Match.tournament_id == rpl.id,
                    Match.round_number == last_finished_round,
                )
            )
            active_last_round_ids = {int(x[0]) for x in active_q.all()} & rpl_member_ids

        sleeping_ids = (rpl_member_ids - active_last_round_ids) - no_preds_ids
        sleeping = len(sleeping_ids)
        with_preds_season = len(season_pred_ids & rpl_member_ids)
        active_now = len(active_last_round_ids)

    lines = [
        "👥 Аудитория бота",
        f"Всего пользователей: {total_users}",
        f"В турнире сейчас: {in_tournament_now}",
        f"Никогда не вступали: {never_joined}",
        f"Вне турнира (были раньше): {outside_were_before}",
        f"Активные (последний прошедший тур {last_finished_round if last_finished_round is not None else '—'}): {active_now}",
        f"Спящие (не активны в последнем прошедшем туре): {sleeping}",
        f"С прогнозами в сезоне: {with_preds_season}",
        f"Заблокировали бота (зафиксировано): {blocked_users}",
        f"Покинули турнир: {left_users}",
        "",
        "Примечание: «Заблокировали» считаем по недоставленным push-сообщениям.",
    ]
    await message.answer("\n".join(lines))


async def admin_audience_list(message: types.Message):
    """/admin_audience_list — именные списки аудитории по RPL"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return

    now = _now_msk_naive()

    async with SessionLocal() as session:
        rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = rpl_q.scalar_one_or_none()
        if rpl is None:
            await message.answer("Турнир RPL не найден.")
            return

        members_q = await session.execute(
            select(UserTournament.tg_user_id, UserTournament.display_name).where(UserTournament.tournament_id == rpl.id)
        )
        member_rows = members_q.all()
        rpl_member_ids = {int(uid) for uid, _dn in member_rows}
        dn_map = {int(uid): (dn or "").strip() for uid, dn in member_rows}

        users_q = await session.execute(
            select(User.tg_user_id, User.display_name, User.username, User.full_name).where(User.tg_user_id.in_(rpl_member_ids))
        )
        user_map = {int(uid): (udn, un, fn) for uid, udn, un, fn in users_q.all()}

        def name_of(uid: int) -> str:
            dn = dn_map.get(uid) or ""
            if dn:
                return dn
            udn, un, fn = user_map.get(uid, (None, None, None))
            if udn:
                return udn
            if un:
                return f"@{un}"
            if fn:
                return fn
            return str(uid)

        rounds_q = await session.execute(
            select(
                Match.round_number,
                func.max(Match.kickoff_time).label("ends_at"),
            )
            .where(
                Match.tournament_id == rpl.id,
                Match.source == "manual",
                Match.round_number >= rpl.round_min,
                Match.round_number <= rpl.round_max,
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        round_rows = rounds_q.all()
        last_finished_round: int | None = None
        for r, ends_at in round_rows:
            if ends_at is not None and ends_at <= now:
                last_finished_round = int(r)

        season_preds_q = await session.execute(
            select(func.distinct(Prediction.tg_user_id))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Match.tournament_id == rpl.id,
                Match.source == "manual",
                Match.round_number >= rpl.round_min,
                Match.round_number <= rpl.round_max,
            )
        )
        season_pred_ids = {int(x[0]) for x in season_preds_q.all()}
        no_preds_ids = rpl_member_ids - season_pred_ids

        active_last_round_ids: set[int] = set()
        if last_finished_round is not None:
            active_q = await session.execute(
                select(func.distinct(Prediction.tg_user_id))
                .select_from(Prediction)
                .join(Match, Match.id == Prediction.match_id)
                .where(
                    Match.tournament_id == rpl.id,
                    Match.round_number == last_finished_round,
                )
            )
            active_last_round_ids = {int(x[0]) for x in active_q.all()} & rpl_member_ids

        sleeping_ids = (rpl_member_ids - active_last_round_ids) - no_preds_ids

    def section(title: str, ids: set[int], limit: int = 50) -> list[str]:
        lines = [f"{title}: {len(ids)}"]
        if not ids:
            lines.append("—")
            return lines
        names = sorted((name_of(uid) for uid in ids), key=lambda x: x.lower())
        for i, name in enumerate(names[:limit], start=1):
            lines.append(f"{i}. {name}")
        if len(names) > limit:
            lines.append(f"... и ещё {len(names) - limit}")
        return lines

    out: list[str] = [
        f"👥 Аудитория RPL — списки",
        f"Последний прошедший тур: {last_finished_round if last_finished_round is not None else '—'}",
        "",
    ]
    out.extend(section("🟢 Активные (последний прошедший тур)", active_last_round_ids))
    out.append("")
    out.extend(section("😴 Спящие (не активны в последнем прошедшем туре)", sleeping_ids))
    out.append("")
    out.extend(section("🕳 В турнире без прогнозов в сезоне", no_preds_ids))
    await message.answer("\n".join(out))


async def _build_admin_status_text() -> str:
    now = _now_msk_naive()
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.is_active == 1).order_by(Tournament.code.asc()))
        tournaments = t_q.scalars().all()

        lines = ["🧭 Админ-статус", f"Время: {now.strftime('%d.%m %H:%M')} МСК", ""]
        for t in tournaments:
            members_q = await session.execute(
                select(func.count(func.distinct(Prediction.tg_user_id)))
                .select_from(Prediction)
                .join(Match, Match.id == Prediction.match_id)
                .where(
                    Match.tournament_id == t.id,
                    Match.source == "manual",
                    Match.round_number >= t.round_min,
                    Match.round_number <= t.round_max,
                )
            )
            members = int(members_q.scalar_one() or 0)

            rounds_q = await session.execute(
                select(
                    Match.round_number,
                    func.max(Match.kickoff_time).label("ends_at"),
                )
                .where(
                    Match.tournament_id == t.id,
                    Match.source == "manual",
                    Match.round_number >= t.round_min,
                    Match.round_number <= t.round_max,
                )
                .group_by(Match.round_number)
                .order_by(Match.round_number.asc())
            )
            round_rows = rounds_q.all()
            current_round = t.round_min
            for r, ends_at in round_rows:
                if now <= ends_at:
                    current_round = int(r)
                    break
            else:
                if round_rows:
                    current_round = int(round_rows[-1][0])

            open_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == t.id,
                    Match.source == "manual",
                    Match.round_number == current_round,
                    Match.kickoff_time > now,
                )
            )
            open_matches = int(open_q.scalar_one() or 0)

            no_result_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == t.id,
                    Match.source == "manual",
                    Match.round_number >= t.round_min,
                    Match.round_number <= t.round_max,
                    Match.home_score.is_(None),
                    Match.away_score.is_(None),
                )
            )
            no_result = int(no_result_q.scalar_one() or 0)

            lines.append(f"🏆 {t.name}")
            lines.append(f"Участников с прогнозами: {members}")
            lines.append(f"Текущий тур: {current_round}")
            lines.append(f"Открытых матчей в туре: {open_matches}")
            lines.append(f"Матчей без результата: {no_result}")
            lines.append("")

    return "\n".join(lines).strip()


async def _build_admin_progress_text(tournament_id: int, round_number: int) -> str:
    now = _now_msk_naive()
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        t = t_q.scalar_one_or_none()
        if t is None:
            return "Турнир не найден."
        if round_number < t.round_min or round_number > t.round_max:
            return f"Для {t.name} доступны туры только {t.round_min}..{t.round_max}."

        total_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
            )
        )
        total = int(total_q.scalar_one() or 0)
        if total == 0:
            return f"В туре {round_number} нет матчей ({t.name})."

        done_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
                Match.home_score.isnot(None),
                Match.away_score.isnot(None),
            )
        )
        done = int(done_q.scalar_one() or 0)

        open_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
                Match.kickoff_time > now,
            )
        )
        open_matches = int(open_q.scalar_one() or 0)

        members_q = await session.execute(
            select(UserTournament.tg_user_id).where(
                UserTournament.tournament_id == tournament_id,
                UserTournament.display_name.isnot(None),
            )
        )
        member_ids = [int(x[0]) for x in members_q.all()]
        members = len(member_ids)

        if members == 0:
            return (
                f"📈 Прогресс тура {round_number} ({t.name})\n"
                f"Матчи: {done}/{total} с результатом\n"
                f"Открытых матчей: {open_matches}\n"
                "Участников пока нет."
            )

        matches_q = await session.execute(
            select(Match.id).where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
            )
        )
        match_ids = [int(x[0]) for x in matches_q.all()]

        preds_q = await session.execute(
            select(Prediction.tg_user_id, Prediction.match_id).where(
                Prediction.match_id.in_(match_ids),
                Prediction.tg_user_id.in_(member_ids),
            )
        )
        pred_rows = preds_q.all()
        per_user_matches: dict[int, set[int]] = {}
        for uid in member_ids:
            per_user_matches[uid] = set()
        for uid, mid in pred_rows:
            per_user_matches[int(uid)].add(int(mid))

        with_any = sum(1 for uid in member_ids if len(per_user_matches[uid]) > 0)
        with_all = sum(1 for uid in member_ids if len(per_user_matches[uid]) >= len(match_ids))

    return (
        f"📈 Прогресс тура {round_number} ({t.name})\n"
        f"Матчи с результатом: {done}/{total}\n"
        f"Открытых матчей: {open_matches}\n"
        f"Участников в турнире: {members}\n"
        f"Поставили хотя бы 1 прогноз: {with_any}\n"
        f"Поставили все матчи тура: {with_all}"
    )


async def _build_admin_missing_text(tournament_id: int, round_number: int) -> str:
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        t = t_q.scalar_one_or_none()
        if t is None:
            return "Турнир не найден."
        if round_number < t.round_min or round_number > t.round_max:
            return f"Для {t.name} доступны туры только {t.round_min}..{t.round_max}."

        members_q = await session.execute(
            select(UserTournament.tg_user_id, UserTournament.display_name).where(
                UserTournament.tournament_id == tournament_id,
                UserTournament.display_name.isnot(None),
            )
        )
        members_rows = members_q.all()
        member_ids = [int(r[0]) for r in members_rows]
        display_map = {int(uid): dn for uid, dn in members_rows if dn}
        if not member_ids:
            return f"В турнире {t.name} пока нет участников."

        matches_q = await session.execute(
            select(Match.id).where(
                Match.tournament_id == tournament_id,
                Match.round_number == round_number,
                Match.source == "manual",
            )
        )
        match_ids = [int(x[0]) for x in matches_q.all()]
        if not match_ids:
            return f"В туре {round_number} нет матчей ({t.name})."

        preds_q = await session.execute(
            select(Prediction.tg_user_id, func.count(Prediction.id))
            .where(
                Prediction.tg_user_id.in_(member_ids),
                Prediction.match_id.in_(match_ids),
            )
            .group_by(Prediction.tg_user_id)
        )
        pred_count_map = {int(uid): int(cnt or 0) for uid, cnt in preds_q.all()}

        users_q = await session.execute(select(User.tg_user_id, User.username, User.full_name).where(User.tg_user_id.in_(member_ids)))
        users_map = {int(uid): (un, fn) for uid, un, fn in users_q.all()}

        missing_rows: list[tuple[int, str, int]] = []
        for uid in member_ids:
            cnt = pred_count_map.get(uid, 0)
            if cnt >= len(match_ids):
                continue
            dn = display_map.get(uid)
            if dn:
                name = dn
            else:
                un, fn = users_map.get(uid, (None, None))
                name = f"@{un}" if un else (fn or str(uid))
            missing_rows.append((uid, name, len(match_ids) - cnt))

    if not missing_rows:
        return f"✅ В туре {round_number} все участники {t.name} уже поставили прогнозы."

    missing_rows.sort(key=lambda x: (x[2], x[1]), reverse=True)
    lines = [f"🚫 Кто не поставил (или поставил не всё)\n{t.name} · тур {round_number}", ""]
    for i, (_uid, name, left) in enumerate(missing_rows[:50], start=1):
        lines.append(f"{i}. {name} — осталось матчей: {left}")
    lines.append("")
    lines.append(f"Всего участников с незавершёнными прогнозами: {len(missing_rows)}")
    return "\n".join(lines)


def _admin_pick_tournament_keyboard(prefix: str, tournaments: list[Tournament]) -> types.InlineKeyboardMarkup:
    rows = [
        [types.InlineKeyboardButton(text=t.name, callback_data=f"{prefix}:{t.id}")]
        for t in tournaments
    ]
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_pick_round_keyboard(prefix: str, tournament_id: int, rounds: list[int]) -> types.InlineKeyboardMarkup:
    row: list[types.InlineKeyboardButton] = []
    rows: list[list[types.InlineKeyboardButton]] = []
    for r in rounds:
        row.append(types.InlineKeyboardButton(text=str(r), callback_data=f"{prefix}:{tournament_id}:{r}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def admin_panel(message: types.Message):
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return
    await message.answer("🛠 Админ-панель\nВыбери действие:", reply_markup=_admin_panel_keyboard())


async def admin_status(message: types.Message):
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return
    await message.answer(await _build_admin_status_text(), reply_markup=_admin_panel_keyboard())


async def admin_round_progress(message: types.Message):
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) == 3:
        try:
            tournament_id = int(parts[1])
            round_number = int(parts[2])
        except ValueError:
            await message.answer("Формат: /admin_round_progress <tournament_id> <round_number>")
            return
        await message.answer(await _build_admin_progress_text(tournament_id, round_number), reply_markup=_admin_panel_keyboard())
        return

    async with SessionLocal() as session:
        q = await session.execute(select(Tournament).where(Tournament.is_active == 1).order_by(Tournament.code.asc()))
        tournaments = q.scalars().all()
    await message.answer(
        "Выбери турнир для прогресса тура:",
        reply_markup=_admin_pick_tournament_keyboard("admin_progress_t", tournaments),
    )


async def admin_missing(message: types.Message):
    if not _is_admin(message):
        await message.answer("⛔️ У вас нет прав на эту команду.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) == 3:
        try:
            tournament_id = int(parts[1])
            round_number = int(parts[2])
        except ValueError:
            await message.answer("Формат: /admin_missing <tournament_id> <round_number>")
            return
        await message.answer(await _build_admin_missing_text(tournament_id, round_number), reply_markup=_admin_panel_keyboard())
        return

    async with SessionLocal() as session:
        q = await session.execute(select(Tournament).where(Tournament.is_active == 1).order_by(Tournament.code.asc()))
        tournaments = q.scalars().all()
    await message.answer(
        "Выбери турнир для списка пропусков:",
        reply_markup=_admin_pick_tournament_keyboard("admin_missing_t", tournaments),
    )


async def admin_panel_click(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    action = data.split(":", 1)[1] if ":" in data else ""
    if action == "status":
        await callback.message.answer(await _build_admin_status_text(), reply_markup=_admin_panel_keyboard())
    elif action == "progress":
        async with SessionLocal() as session:
            q = await session.execute(select(Tournament).where(Tournament.is_active == 1).order_by(Tournament.code.asc()))
            tournaments = q.scalars().all()
        await callback.message.answer(
            "Выбери турнир для прогресса тура:",
            reply_markup=_admin_pick_tournament_keyboard("admin_progress_t", tournaments),
        )
    elif action == "missing":
        async with SessionLocal() as session:
            q = await session.execute(select(Tournament).where(Tournament.is_active == 1).order_by(Tournament.code.asc()))
            tournaments = q.scalars().all()
        await callback.message.answer(
            "Выбери турнир для списка пропусков:",
            reply_markup=_admin_pick_tournament_keyboard("admin_missing_t", tournaments),
        )
    await callback.answer()


async def admin_pick_tournament_for_progress(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    try:
        tournament_id = int(data.split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка выбора турнира", show_alert=True)
        return
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        t = t_q.scalar_one_or_none()
        if t is None:
            await callback.message.answer("Турнир не найден.")
            await callback.answer()
            return
        rounds_q = await session.execute(
            select(Match.round_number)
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
                Match.round_number >= t.round_min,
                Match.round_number <= t.round_max,
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        rounds = [int(x[0]) for x in rounds_q.all()]
    if not rounds:
        await callback.message.answer("Для этого турнира пока нет матчей.")
        await callback.answer()
        return
    await callback.message.answer(
        "Выбери тур:",
        reply_markup=_admin_pick_round_keyboard("admin_progress_r", tournament_id, rounds),
    )
    await callback.answer()


async def admin_pick_tournament_for_missing(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    try:
        tournament_id = int(data.split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка выбора турнира", show_alert=True)
        return
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        t = t_q.scalar_one_or_none()
        if t is None:
            await callback.message.answer("Турнир не найден.")
            await callback.answer()
            return
        rounds_q = await session.execute(
            select(Match.round_number)
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
                Match.round_number >= t.round_min,
                Match.round_number <= t.round_max,
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        rounds = [int(x[0]) for x in rounds_q.all()]
    if not rounds:
        await callback.message.answer("Для этого турнира пока нет матчей.")
        await callback.answer()
        return
    await callback.message.answer(
        "Выбери тур:",
        reply_markup=_admin_pick_round_keyboard("admin_missing_r", tournament_id, rounds),
    )
    await callback.answer()


async def admin_pick_round_for_progress(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    try:
        _prefix, tournament_id_s, round_number_s = data.split(":")
        tournament_id = int(tournament_id_s)
        round_number = int(round_number_s)
    except Exception:
        await callback.answer("Ошибка выбора тура", show_alert=True)
        return
    await callback.message.answer(await _build_admin_progress_text(tournament_id, round_number), reply_markup=_admin_panel_keyboard())
    await callback.answer()


async def admin_pick_round_for_missing(callback: types.CallbackQuery):
    if not _is_admin(callback):
        await callback.answer("Нет прав", show_alert=True)
        return
    data = callback.data or ""
    try:
        _prefix, tournament_id_s, round_number_s = data.split(":")
        tournament_id = int(tournament_id_s)
        round_number = int(round_number_s)
    except Exception:
        await callback.answer("Ошибка выбора тура", show_alert=True)
        return
    await callback.message.answer(await _build_admin_missing_text(tournament_id, round_number), reply_markup=_admin_panel_keyboard())
    await callback.answer()
