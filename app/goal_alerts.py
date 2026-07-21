from __future__ import annotations

import asyncio
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
from app.models import GoalAlertSubscription, Match, Setting, Tournament
from app.notify_prefs import should_send_notification

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


def _notified_count_key(match_id: int) -> str:
    return f"GOAL_ALERT_NOTIFIED_COUNT_{int(match_id)}"


async def _get_setting_int(session, key: str, default: int = 0) -> int:
    row = (await session.execute(select(Setting).where(Setting.key == key))).scalar_one_or_none()
    if row is None:
        return default
    try:
        return int(row.value)
    except Exception:
        return default


async def _set_setting_int(session, key: str, value: int) -> None:
    row = (await session.execute(select(Setting).where(Setting.key == key))).scalar_one_or_none()
    if row is None:
        session.add(Setting(key=key, value=str(int(value))))
    else:
        row.value = str(int(value))


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


async def process_live_match_goals(bot: Bot, session, match: Match) -> int:
    """Проверяет один живой матч на новые голы и рассылает пуши подписчикам,
    у которых baseline_goal_count меньше порядкового номера этого гола.
    Возвращает количество отправленных пушей (для логов)."""
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
    goal_events = sorted_goal_events(events)
    total_goals = len(goal_events)

    already_notified = await _get_setting_int(session, _notified_count_key(match.id), default=0)
    if total_goals <= already_notified:
        return 0

    round_name = display_round_name("RPL", int(match.round_number or 0))
    home_display = display_team_name(match.home_team)
    away_display = display_team_name(match.away_team)

    # Прогоняем голы по порядку, считая счёт по ходу, чтобы у каждого пуша
    # была верная сиюминутная строка счёта (а не финальный счёт матча).
    running_home = 0
    running_away = 0
    sent_count = 0
    for idx, goal in enumerate(goal_events):
        scoring_team_raw = str(goal.get("team_name") or "")
        if scoring_team_raw == str(match.home_team or ""):
            running_home += 1
        elif scoring_team_raw == str(match.away_team or ""):
            running_away += 1
        else:
            # Неожиданное название команды (рассинхрон с данными матча) —
            # пропускаем подсчёт счёта для этого события, но не падаем.
            continue

        if idx < already_notified:
            continue

        text = _build_goal_alert_text(
            minute=goal.get("minute"),
            extra=goal.get("extra"),
            scoring_team_display=display_team_name(scoring_team_raw),
            home_display=home_display,
            away_display=away_display,
            home_score=running_home,
            away_score=running_away,
            round_name=round_name,
        )
        keyboard = _open_match_center_keyboard(int(match.id))

        for sub in subs:
            if int(sub.baseline_goal_count or 0) > idx:
                continue
            if not await should_send_notification(session, int(sub.tg_user_id), "goals"):
                continue
            await _safe_send(bot, session, chat_id=int(sub.tg_user_id), text=text, reply_markup=keyboard)
            sent_count += 1

    await _set_setting_int(session, _notified_count_key(match.id), total_goals)
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
