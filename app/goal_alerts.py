from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from aiogram import Bot, types
from sqlalchemy import or_, select

from app.audience import is_blocked_send_error, mark_user_blocked
from app.db import SessionLocal
from app.display import display_round_name, display_team_name
from app.models import GoalAlertSubscription, Match, Tournament
from app.notify_prefs import should_send_notification

GoalSignature = tuple[str, int]

logger = logging.getLogger(__name__)

MINIAPP_WEB_URL = os.getenv("MINIAPP_WEB_URL", "https://rpl-predictions-bot-mini-app.onrender.com").strip()

# Как часто фоновый цикл просыпается и проверяет живые матчи РПЛ на новые голы.
# Само обращение к API-Football идёт через общий кэш fetch_fixture_events
# (app/match_center.py) с тем же TTL, что и у "живого" просмотра матч-центра
# (FOOTBALL_LIVE_TTL_SEC, по умолчанию 90с) — поэтому реальная частота запросов
# к внешнему API определяется этим TTL, а не интервалом сна цикла: часть
# "пробуждений" просто читает уже свежий кэш без нового запроса.
GOAL_ALERT_POLL_INTERVAL_SEC = int(os.getenv("GOAL_ALERT_POLL_INTERVAL_SEC", "60"))
LIVE_TTL_SECONDS = int(os.getenv("FOOTBALL_LIVE_TTL_SEC", "90"))

# Матч считаем "живым" (по времени кикоффа, МСК, как и везде в проекте) в
# течение этого окна после старта — совпадает с LIVE_WINDOW_MINUTES из
# match_center_current в app/miniapp_api.py.
LIVE_WINDOW_MINUTES = 130

# Только матчи с хотя бы одной активной подпиской вообще опрашиваются —
# на матчи без подписчиков фоновый цикл не тратит ни одного запроса к API.


def _now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


def _is_real_goal_event(event: dict[str, Any]) -> bool:
    """API-Football кладёт "Missed Penalty" под тем же type="Goal", что и
    настоящие голы — только по detail отличить непонятно проваленный пенальти
    от забитого. Фильтруем именно эти события."""
    event_type = str(event.get("type") or "").strip().lower()
    detail = str(event.get("detail") or "").strip().lower()
    return event_type == "goal" and detail != "missed penalty"


def sorted_goal_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    goals = [e for e in events if _is_real_goal_event(e)]
    goals.sort(key=lambda e: (int(e.get("minute") or 0), int(e.get("extra") or 0)))
    return goals


def _is_var_goal_disallowed_event(event: dict[str, Any]) -> bool:
    """ВАР отменяет гол отдельным событием type="Var" — API-Football не
    убирает исходное событие type="Goal" из ленты задним числом, поэтому
    без этой проверки отменённый гол выглядит как обычный забитый. detail
    обычно вида "Goal Disallowed - Offside"/"Goal Cancelled" и т.п."""
    event_type = str(event.get("type") or "").strip().lower()
    if event_type != "var":
        return False
    detail = str(event.get("detail") or "").strip().lower()
    return "goal" in detail and ("disallow" in detail or "cancel" in detail)


def confirmed_goal_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Голы, которые всё ещё в силе — без учёта отменённых ВАРом. Событие
    отмены сопоставляем с ближайшим по минуте голом той же команды (ВАР
    обычно логируется в ту же минуту или в течение пары минут после гола)."""
    goals = sorted_goal_events(events)
    var_cancels = [e for e in events if _is_var_goal_disallowed_event(e)]
    if not var_cancels:
        return goals

    cancelled_idx: set[int] = set()
    for cancel in var_cancels:
        cancel_team = str(cancel.get("team_name") or "")
        cancel_minute = int(cancel.get("minute") or 0)
        best_idx = None
        best_diff = None
        for idx, g in enumerate(goals):
            if idx in cancelled_idx:
                continue
            if str(g.get("team_name") or "") != cancel_team:
                continue
            diff = abs(int(g.get("minute") or 0) - cancel_minute)
            if diff <= 4 and (best_diff is None or diff < best_diff):
                best_idx = idx
                best_diff = diff
        if best_idx is not None:
            cancelled_idx.add(best_idx)

    return [g for idx, g in enumerate(goals) if idx not in cancelled_idx]


def _goal_signature(goal: dict[str, Any]) -> GoalSignature:
    # Специально НЕ включаем сюда extra/player_name: на практике API-Football
    # иногда дозаполняет/поправляет эти поля для уже отданного события гола
    # (например, автора гола) уже после того, как мы его увидели в первый
    # раз — из-за этого точная 4-элементная сигнатура "переставала совпадать
    # сама с собой" между двумя опросами: старая версия события выглядела
    # как будто пропала из ленты (мы принимали это за отмену ВАРом), а
    # "новая" версия того же гола выглядела как будто это другой, новый гол.
    # На проде это привело к тому, что один и тот же гол объявлялся заново
    # (с неверно завышенным счётом) и тут же ложно отменялся. Команда+минута
    # — устойчивая пара: два разных гола одной команды в одну и ту же минуту
    # матча практически невозможны, а вот дрейф остальных полей — нет.
    return (
        str(goal.get("team_name") or ""),
        int(goal.get("minute") or 0),
    )


def _load_goal_state(match: Match) -> tuple[list[GoalSignature], set[GoalSignature]]:
    raw = match.goal_alert_state
    if not raw:
        return [], set()
    try:
        data = json.loads(raw)
        # tuple(x[:2]) — на случай, если состояние ещё сохранено в старом
        # 4-элементном формате (team, minute, extra, player_name) до фикса
        # сигнатуры выше: обрезаем до (team, minute), чтобы уже накопленные
        # для текущих живых матчей данные не считались "новыми" целиком
        # заново после деплоя этого фикса.
        announced = [tuple(x[:2]) for x in (data.get("announced") or [])]
        cancelled = {tuple(x[:2]) for x in (data.get("cancelled") or [])}
        return announced, cancelled
    except Exception:
        logger.warning("[goal_alerts] failed to parse goal_alert_state for match_id=%s", match.id)
        return [], set()


def _save_goal_state(match: Match, announced: list[GoalSignature], cancelled: set[GoalSignature]) -> None:
    match.goal_alert_state = json.dumps(
        {"announced": [list(s) for s in announced], "cancelled": [list(s) for s in cancelled]}
    )


async def _safe_send(
    bot: Bot,
    session,
    *,
    chat_id: int,
    text: str,
    reply_markup: types.InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await bot.send_message(chat_id=int(chat_id), text=text, reply_markup=reply_markup)
    except Exception as exc:
        if is_blocked_send_error(exc):
            await mark_user_blocked(session, int(chat_id))
        else:
            logger.warning("[goal_alerts] send failed for %s: %s", chat_id, exc)


def _open_match_center_keyboard(match_id: int) -> types.InlineKeyboardMarkup | None:
    if not MINIAPP_WEB_URL:
        return None
    sep = "&" if "?" in MINIAPP_WEB_URL else "?"
    url = f"{MINIAPP_WEB_URL}{sep}{urlencode({'screen': 'matches', 't': 'RPL', 'match_id': int(match_id)})}"
    return types.InlineKeyboardMarkup(
        inline_keyboard=[[types.InlineKeyboardButton(text="Зайти в матч-центр", web_app=types.WebAppInfo(url=url))]]
    )


def _build_goal_alert_text(
    *,
    minute: int | None,
    extra: int | None,
    scoring_team_display: str,
    home_display: str,
    away_display: str,
    home_score: int,
    away_score: int,
    round_name: str,
) -> str:
    minute_str = str(int(minute or 0))
    if extra:
        minute_str = f"{minute_str}+{int(extra)}"
    return (
        f"⚽ {minute_str}' Гол! {scoring_team_display} забивает!\n\n"
        f"РПЛ · {round_name}\n"
        f"{home_display} {home_score}-{away_score} {away_display}"
    )


def _build_goal_cancelled_text(
    *,
    scoring_team_display: str,
    home_display: str,
    away_display: str,
    home_score: int,
    away_score: int,
    round_name: str,
) -> str:
    return (
        f"❌ Гол отменён после ВАР! {scoring_team_display} не забивает.\n\n"
        f"РПЛ · {round_name}\n"
        f"{home_display} {home_score}-{away_score} {away_display}"
    )


def _build_final_whistle_text(
    *,
    home_display: str,
    away_display: str,
    home_score: int,
    away_score: int,
    round_name: str,
) -> str:
    return (
        f"🏁 Матч завершён!\n\n"
        f"РПЛ · {round_name}\n"
        f"{home_display} {home_score}-{away_score} {away_display}"
    )


async def process_live_match_goals(bot: Bot, session, match: Match) -> int:
    """Проверяет один живой матч на новые голы и на отмены голов ВАРом,
    рассылает пуши подписчикам, у которых baseline_goal_count не больше
    порядкового номера этого гола в общем списке когда-либо объявленных
    голов матча (см. Match.goal_alert_state). Возвращает количество
    отправленных пушей (для логов)."""
    if not match.api_fixture_id:
        return 0

    subs = (
        await session.execute(
            select(GoalAlertSubscription).where(GoalAlertSubscription.match_id == int(match.id))
        )
    ).scalars().all()
    if not subs:
        return 0

    from app.match_center import fetch_fixture_events

    events = await fetch_fixture_events(int(match.api_fixture_id), ttl_seconds=LIVE_TTL_SECONDS)
    # goal_by_sig хранит последнюю (самую свежую) версию события гола по
    # сигнатуре — нужна только для отображения actual extra-времени в тексте
    # пуша, на саму дедупликацию/сопоставление уже не влияет (см. комментарий
    # в _goal_signature).
    goal_by_sig: dict[GoalSignature, dict[str, Any]] = {}
    for g in confirmed_goal_events(events):
        goal_by_sig[_goal_signature(g)] = g
    confirmed_sigs = list(goal_by_sig.keys())
    confirmed_set = set(confirmed_sigs)

    announced, cancelled = _load_goal_state(match)

    # Самоисцеление: если сигнатура раньше была ошибочно помечена отменённой
    # (например, из-за дрейфа старой 4-элементной сигнатуры до этого фикса),
    # а сейчас снова входит в подтверждённый список — снимаем отметку об
    # отмене. Настоящая VAR-отмена так себя никогда не ведёт: событие "Var" в
    # ленте API-Football не исчезает, поэтому по-настоящему отменённый гол не
    # может повторно попасть в confirmed_set на следующем опросе.
    resurrected = cancelled & confirmed_set
    if resurrected:
        cancelled -= resurrected
        logger.warning(
            "[goal_alerts] un-cancelled resurrected goal signatures for match_id=%s: %s",
            match.id,
            resurrected,
        )

    announced_set = set(announced)

    # Новые голы — сигнатуры из текущего подтверждённого списка, которых
    # раньше не было среди уже объявленных.
    new_sigs = [s for s in confirmed_sigs if s not in announced_set]
    if new_sigs:
        announced.extend(new_sigs)

    # Отменённые голы — были объявлены, ещё не помечены отменёнными, но
    # больше не входят в текущий подтверждённый список (ВАР отменил гол,
    # либо само событие пропало из ленты API-Football).
    newly_cancelled = [s for s in announced if s not in cancelled and s not in confirmed_set]

    if not new_sigs and not newly_cancelled:
        return 0

    round_name = display_round_name("RPL", int(match.round_number or 0))
    home_display = display_team_name(match.home_team)
    away_display = display_team_name(match.away_team)
    keyboard = _open_match_center_keyboard(int(match.id))
    home_team_raw = str(match.home_team or "")
    away_team_raw = str(match.away_team or "")

    def _running_score(upto: GoalSignature | None, exclude: set[GoalSignature]) -> tuple[int, int]:
        home = away = 0
        for s in announced:
            if s in exclude:
                continue
            if s[0] == home_team_raw:
                home += 1
            elif s[0] == away_team_raw:
                away += 1
            if upto is not None and s == upto:
                break
        return home, away

    sent_count = 0

    for sig in new_sigs:
        position = announced.index(sig)
        home_score, away_score = _running_score(sig, exclude=cancelled)
        goal = goal_by_sig.get(sig) or {}
        text = _build_goal_alert_text(
            minute=sig[1],
            extra=goal.get("extra"),
            scoring_team_display=display_team_name(sig[0]),
            home_display=home_display,
            away_display=away_display,
            home_score=home_score,
            away_score=away_score,
            round_name=round_name,
        )
        for sub in subs:
            if int(sub.baseline_goal_count or 0) > position:
                continue
            if not await should_send_notification(session, int(sub.tg_user_id), "goals"):
                continue
            await _safe_send(bot, session, chat_id=int(sub.tg_user_id), text=text, reply_markup=keyboard)
            sent_count += 1

    for sig in newly_cancelled:
        cancelled.add(sig)

    # Счёт для пушей об отмене считаем уже после того, как учтены все отмены
    # этого цикла — чтобы при нескольких одновременных отменах каждый пуш
    # показывал один и тот же, уже согласованный актуальный счёт.
    for sig in newly_cancelled:
        position = announced.index(sig)
        home_score, away_score = _running_score(None, exclude=cancelled)
        text = _build_goal_cancelled_text(
            scoring_team_display=display_team_name(sig[0]),
            home_display=home_display,
            away_display=away_display,
            home_score=home_score,
            away_score=away_score,
            round_name=round_name,
        )
        for sub in subs:
            if int(sub.baseline_goal_count or 0) > position:
                continue
            if not await should_send_notification(session, int(sub.tg_user_id), "goals"):
                continue
            await _safe_send(bot, session, chat_id=int(sub.tg_user_id), text=text, reply_markup=keyboard)
            sent_count += 1

    _save_goal_state(match, announced, cancelled)
    await session.commit()
    return sent_count


async def send_final_whistle_pushes(bot: Bot, session, match: Match) -> int:
    """Финальный пуш подписчикам голевых уведомлений с итоговым счётом.
    Вызывается из app/rpl_sync.py ровно в момент, когда матч помечается
    завершённым по данным того же fetch_league_fixtures — отдельного
    запроса к API-Football на статус матча не делает, бюджет не трогает.

    После финального пуша подписки на этот матч больше не нужны (голов
    в нём больше не будет) — удаляем их, заодно не давая таблице расти
    бесконечно."""
    subs = (
        await session.execute(
            select(GoalAlertSubscription).where(GoalAlertSubscription.match_id == int(match.id))
        )
    ).scalars().all()
    if not subs:
        return 0

    round_name = display_round_name("RPL", int(match.round_number or 0))
    home_display = display_team_name(match.home_team)
    away_display = display_team_name(match.away_team)
    text = _build_final_whistle_text(
        home_display=home_display,
        away_display=away_display,
        home_score=int(match.home_score or 0),
        away_score=int(match.away_score or 0),
        round_name=round_name,
    )
    keyboard = _open_match_center_keyboard(int(match.id))

    sent_count = 0
    for sub in subs:
        if not await should_send_notification(session, int(sub.tg_user_id), "goals"):
            continue
        await _safe_send(bot, session, chat_id=int(sub.tg_user_id), text=text, reply_markup=keyboard)
        sent_count += 1

    for sub in subs:
        await session.delete(sub)
    match.goal_alert_state = None
    await session.commit()
    return sent_count


async def _run_goal_alerts_once(bot: Bot, session_factory) -> int:
    now = _now_msk_naive()
    total_sent = 0
    async with session_factory() as session:
        rpl_tournament = (
            await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        ).scalar_one_or_none()
        if rpl_tournament is None:
            return 0

        sub_match_ids_rows = (
            await session.execute(select(GoalAlertSubscription.match_id).distinct())
        ).all()
        sub_match_ids = [int(r[0]) for r in sub_match_ids_rows]
        if not sub_match_ids:
            return 0

        matches = (
            await session.execute(
                select(Match).where(
                    Match.id.in_(sub_match_ids),
                    Match.tournament_id == int(rpl_tournament.id),
                    Match.api_fixture_id.is_not(None),
                    or_(Match.home_score.is_(None), Match.away_score.is_(None)),
                    Match.kickoff_time <= now,
                    Match.kickoff_time >= now - timedelta(minutes=LIVE_WINDOW_MINUTES),
                )
            )
        ).scalars().all()

        for match in matches:
            try:
                sent = await process_live_match_goals(bot, session, match)
                total_sent += sent
            except Exception:
                logger.exception("[goal_alerts] failed processing match_id=%s", match.id)

    return total_sent


async def run_goal_alerts_loop(bot: Bot, session_factory=SessionLocal) -> None:
    logger.info("[goal_alerts] loop started, interval=%ss", GOAL_ALERT_POLL_INTERVAL_SEC)
    while True:
        try:
            sent = await _run_goal_alerts_once(bot, session_factory)
            if sent:
                logger.info("[goal_alerts] pushes sent=%s", sent)
        except Exception:
            logger.exception("[goal_alerts] loop iteration failed")
        await asyncio.sleep(GOAL_ALERT_POLL_INTERVAL_SEC)
