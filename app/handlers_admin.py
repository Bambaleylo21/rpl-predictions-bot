from __future__ import annotations

import os
from datetime import datetime, timedelta
from aiogram import Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import case, delete, select, func

from app.config import load_admin_ids
from app.db import SessionLocal
from app.models import Match, Prediction, Point, User, Setting, Tournament, UserTournament
from app.scoring import calculate_points
from app.tournament import ROUND_DEFAULT, ROUND_MAX, ROUND_MIN, is_tournament_round

ADMIN_IDS = load_admin_ids()
ROUND_DIGEST_CHAT_ID_RAW = os.getenv("ROUND_DIGEST_CHAT_ID", "").strip()
try:
    ROUND_DIGEST_CHAT_ID = int(ROUND_DIGEST_CHAT_ID_RAW) if ROUND_DIGEST_CHAT_ID_RAW else None
except ValueError:
    ROUND_DIGEST_CHAT_ID = None


class AdminSetResultStates(StatesGroup):
    waiting_for_score = State()


def _now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


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
        tournament_name = tournament.name if tournament else f"турнир #{tournament_id}"

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


def register_admin_handlers(dp: Dispatcher) -> None:
    dp.message.register(admin_add_match, Command("admin_add_match"))
    dp.message.register(admin_set_result, Command("admin_set_result"))
    dp.callback_query.register(admin_set_result_pick_tournament, F.data.startswith("admin_res_t:"))
    dp.callback_query.register(admin_set_result_pick_match, F.data.startswith("admin_res_m:"))
    dp.message.register(admin_set_result_score_input, AdminSetResultStates.waiting_for_score)
    dp.message.register(admin_recalc, Command("admin_recalc"))
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

    await message.answer(
        f"✅ Результат сохранён: {match.home_team} — {match.away_team} | {home_score}:{away_score}. "
        f"Пересчитано очков: {updates}"
    )
    await _maybe_send_round_closed_summary(message.bot, tournament_id=match.tournament_id, round_number=match.round_number)


async def _admin_set_result_open_tournament_picker(message: types.Message) -> None:
    async with SessionLocal() as session:
        q = await session.execute(
            select(Tournament)
            .where(Tournament.is_active == 1)
            .order_by(Tournament.code.asc())
        )
        tournaments = q.scalars().all()

    if not tournaments:
        await message.answer("Нет активных турниров.")
        return

    rows = [
        [
            types.InlineKeyboardButton(
                text=t.name,
                callback_data=f"admin_res_t:{t.id}",
            )
        ]
        for t in tournaments
    ]
    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer("Выбери турнир для внесения результата:", reply_markup=kb)


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

    now = _now_msk_naive()
    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
        tournament = t_q.scalar_one_or_none()
        if tournament is None:
            await callback.answer("Турнир не найден", show_alert=True)
            return

        round_q = await session.execute(
            select(func.min(Match.round_number))
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
                Match.home_score.is_(None),
                Match.away_score.is_(None),
                Match.kickoff_time >= now,
            )
        )
        round_number = round_q.scalar_one_or_none()

        if round_number is None:
            round_q2 = await session.execute(
                select(func.min(Match.round_number))
                .where(
                    Match.tournament_id == tournament_id,
                    Match.source == "manual",
                    Match.home_score.is_(None),
                    Match.away_score.is_(None),
                )
            )
            round_number = round_q2.scalar_one_or_none()

        if round_number is None:
            await callback.message.answer(f"В турнире {tournament.name} нет матчей без результата.")
            await callback.answer()
            return

        matches_q = await session.execute(
            select(Match)
            .where(
                Match.tournament_id == tournament_id,
                Match.source == "manual",
                Match.round_number == int(round_number),
                Match.home_score.is_(None),
                Match.away_score.is_(None),
            )
            .order_by(Match.kickoff_time.asc(), Match.id.asc())
        )
        matches = matches_q.scalars().all()

    if not matches:
        await callback.message.answer("Матчи не найдены.")
        await callback.answer()
        return

    rows = []
    for m in matches:
        txt = f"{m.home_team} — {m.away_team} | {m.kickoff_time.strftime('%d.%m %H:%M')}"
        rows.append([types.InlineKeyboardButton(text=txt, callback_data=f"admin_res_m:{m.id}")])
    kb = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.answer(
        f"Турнир: {tournament.name}\nТур: {int(round_number)}\nВыбери матч:",
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
        f"Матч: {match.home_team} — {match.away_team}\n"
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
    await message.answer(
        f"✅ Результат сохранён: {match.home_team} — {match.away_team} | {home_score}:{away_score}. "
        f"Пересчитано очков: {updates}"
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
        await session.execute(delete(User).where(User.tg_user_id == tg_user_id))
        await session.commit()

    await message.answer(f"✅ Пользователь {tg_user_id} удалён (users + predictions + points).")
