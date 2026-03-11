from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from aiogram import Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import case, delete, select, func

from app.config import load_admin_ids
from app.db import SessionLocal
from app.display import display_team_name, display_tournament_name
from app.models import Match, Prediction, Point, User, Setting, Tournament, UserTournament
from app.scoring import calculate_points
from app.tournament import ROUND_DEFAULT, ROUND_MAX, ROUND_MIN, is_tournament_round

ADMIN_IDS = load_admin_ids()
ROUND_DIGEST_CHAT_ID_RAW = os.getenv("ROUND_DIGEST_CHAT_ID", "").strip()
try:
    ROUND_DIGEST_CHAT_ID = int(ROUND_DIGEST_CHAT_ID_RAW) if ROUND_DIGEST_CHAT_ID_RAW else None
except ValueError:
    ROUND_DIGEST_CHAT_ID = None

EXACT_HIT_PUSH_DELAY_RAW = os.getenv("EXACT_HIT_PUSH_DELAY_SEC", "0.12").strip()
try:
    EXACT_HIT_PUSH_DELAY_SEC = max(0.0, float(EXACT_HIT_PUSH_DELAY_RAW))
except ValueError:
    EXACT_HIT_PUSH_DELAY_SEC = 0.12


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

        exact_names = ", ".join(
            [str(r[0]) for r in leaderboard_rows if int(r[2] or 0) == exact_max and exact_max > 0]
        )
        diff_names = ", ".join(
            [str(r[0]) for r in leaderboard_rows if int(r[3] or 0) == diff_max and diff_max > 0]
        )
        outcome_names = ", ".join(
            [str(r[0]) for r in leaderboard_rows if int(r[4] or 0) == outcome_max and outcome_max > 0]
        )

        users_q = await session.execute(select(User.tg_user_id, User.username, User.full_name))
        users_map = {int(tg): (un, fn) for tg, un, fn in users_q.all()}
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
            un, fn = users_map.get(int(tg_user_id), (None, None))
            if un:
                return f"@{un}"
            if fn:
                return fn
            return str(tg_user_id)

        exact_pretty = ", ".join(pretty_name(int(x)) for x in exact_names.split(", ") if x) if exact_names else "—"
        diff_pretty = ", ".join(pretty_name(int(x)) for x in diff_names.split(", ") if x) if diff_names else "—"
        outcome_pretty = ", ".join(pretty_name(int(x)) for x in outcome_names.split(", ") if x) if outcome_names else "—"

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
            except Exception:
                continue

        if ROUND_DIGEST_CHAT_ID is not None:
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
            public_lines.append(f"🏅 MVP: {mvp_text} — {best_pts} очк.")
            public_lines.append(f"🎯 Топ точных: {exact_pretty}")
            public_lines.append(f"📏 Топ разницы: {diff_pretty}")
            public_lines.append(f"✅ Топ исходов: {outcome_pretty}")
            if top3_lines:
                public_lines.append("")
                public_lines.append("Топ-3 тура:")
                public_lines.extend(top3_lines)
            public_lines.append("")
            public_lines.append(f"Участников в туре: {participants}")
            public_lines.append("Следующий тур открыт. Время ставить прогнозы: «🎯 Поставить прогноз».")
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
        except Exception:
            # Тихий фейл: пользователь мог заблокировать бота или недоступен.
            pass
        if EXACT_HIT_PUSH_DELAY_SEC > 0:
            await asyncio.sleep(EXACT_HIT_PUSH_DELAY_SEC)
    return sent


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
        if rpl is not None:
            rpl.round_min = 19
            rpl.round_max = 30

        await session.execute(delete(Tournament).where(Tournament.code != "RPL"))
        bad_ids_subq = select(Match.id).where(
            (Match.source != "manual") | Match.api_fixture_id.isnot(None) | (Match.tournament_id != (rpl.id if rpl else -1))
        )
        await session.execute(delete(Point).where(Point.match_id.in_(bad_ids_subq)))
        await session.execute(delete(Prediction).where(Prediction.match_id.in_(bad_ids_subq)))
        del_matches = await session.execute(delete(Match).where(Match.id.in_(bad_ids_subq)))
        await session.commit()

        left_matches = int((await session.execute(select(func.count(Match.id)))).scalar_one() or 0)

    await message.answer(
        "✅ Manual-only cleanup завершён.\n"
        f"Удалено матчей: {int(del_matches.rowcount or 0)}\n"
        f"Осталось матчей: {left_matches}\n"
        "Теперь бот работает только в ручном режиме RPL."
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
