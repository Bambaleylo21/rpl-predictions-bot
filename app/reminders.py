from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlencode

from aiogram import types
from sqlalchemy import select

from app.audience import is_blocked_send_error, mark_user_blocked
from app.display import display_team_name
from app.models import Match, Prediction, Setting, Tournament, UserTournament

logger = logging.getLogger(__name__)
MINIAPP_WEB_URL = os.getenv("MINIAPP_WEB_URL", "https://rpl-predictions-bot-mini-app.onrender.com").strip()


def _now_msk_naive() -> datetime:
    return (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)


def _reminder_key(tournament_id: int, kickoff: datetime) -> str:
    return f"RMD30_T{tournament_id}_{kickoff.strftime('%Y%m%d%H%M')}"


async def _setting_exists(session, key: str) -> bool:
    q = await session.execute(select(Setting).where(Setting.key == key))
    return q.scalar_one_or_none() is not None


async def _mark_setting(session, key: str, value: str = "1") -> None:
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is None:
        session.add(Setting(key=key, value=value))
    else:
        row.value = value


def _build_reminder_text(kickoff: datetime, matches: list[Match]) -> str:
    lines = [
        "⏰ До начала матчей 30 минут",
        f"Старт: {kickoff.strftime('%d.%m %H:%M')} МСК",
        "",
    ]
    for m in matches:
        lines.append(f"• {m.home_team} — {m.away_team}")
    lines.append("")
    lines.append("Ставка: нажми «🎯 Поставить прогноз»")
    return "\n".join(lines)


def _build_reminder_keyboard(matches: list[Match], tournament_code: str | None = None) -> types.InlineKeyboardMarkup:
    params = {"screen": "matches"}
    code = (tournament_code or "").strip().upper()
    if code:
        params["t"] = code
    sep = "&" if "?" in MINIAPP_WEB_URL else "?"
    miniapp_url = f"{MINIAPP_WEB_URL}{sep}{urlencode(params)}"

    rows: list[list[types.InlineKeyboardButton]] = []
    for m in matches:
        label = (
            f"{display_team_name(m.home_team)} — {display_team_name(m.away_team)}"
            f" | {m.kickoff_time.strftime('%d.%m %H:%M')}"
        )
        rows.append([types.InlineKeyboardButton(text=label[:64], web_app=types.WebAppInfo(url=miniapp_url))])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def _process_reminders_once(bot, session_factory) -> int:
    sent_batches = 0
    now = _now_msk_naive()

    # Небольшое окно вокруг отметки "за 30 минут", чтобы не зависеть от дрейфа цикла.
    window_start = now + timedelta(minutes=29, seconds=30)
    window_end = now + timedelta(minutes=30, seconds=30)

    async with session_factory() as session:
        matches_q = await session.execute(
            select(Match)
            .where(
                Match.source == "manual",
                Match.kickoff_time >= window_start,
                Match.kickoff_time < window_end,
            )
            .order_by(Match.tournament_id.asc(), Match.kickoff_time.asc(), Match.id.asc())
        )
        matches = matches_q.scalars().all()
        if not matches:
            return 0

        by_group: dict[tuple[int, datetime], list[Match]] = defaultdict(list)
        for m in matches:
            by_group[(int(m.tournament_id), m.kickoff_time)].append(m)

        for (tournament_id, kickoff), kickoff_matches in by_group.items():
            key = _reminder_key(tournament_id, kickoff)
            if await _setting_exists(session, key):
                continue

            participants_q = await session.execute(
                select(UserTournament.tg_user_id).where(UserTournament.tournament_id == tournament_id)
            )
            user_ids = [int(x[0]) for x in participants_q.all()]
            if not user_ids:
                await _mark_setting(session, key, "1")
                await session.commit()
                sent_batches += 1
                continue

            kickoff_match_ids = [m.id for m in kickoff_matches]
            preds_q = await session.execute(
                select(Prediction.tg_user_id, Prediction.match_id).where(
                    Prediction.match_id.in_(kickoff_match_ids),
                    Prediction.tg_user_id.in_(user_ids),
                )
            )
            preds_rows = preds_q.all()

            user_predicted_ids: dict[int, set[int]] = defaultdict(set)
            for tg_user_id, match_id in preds_rows:
                user_predicted_ids[int(tg_user_id)].add(int(match_id))

            t_code_q = await session.execute(select(Tournament.code).where(Tournament.id == int(tournament_id)))
            tournament_code = t_code_q.scalar_one_or_none()

            for tg_user_id in user_ids:
                predicted_ids = user_predicted_ids.get(tg_user_id, set())
                missing_matches = [m for m in kickoff_matches if m.id not in predicted_ids]
                if not missing_matches:
                    continue

                text = _build_reminder_text(kickoff, missing_matches)
                kb = _build_reminder_keyboard(missing_matches, tournament_code=tournament_code)
                try:
                    await bot.send_message(chat_id=tg_user_id, text=text, reply_markup=kb)
                except Exception as e:
                    # Пользователь мог заблокировать бота и т.п.; продолжаем для остальных.
                    logger.exception("[reminder] send failed for user=%s", tg_user_id)
                    if is_blocked_send_error(e):
                        await mark_user_blocked(session, int(tg_user_id))

            await _mark_setting(session, key, "1")
            await session.commit()
            sent_batches += 1

    return sent_batches


async def run_match_reminders_loop(bot, session_factory) -> None:
    logger.info("[reminder] loop started")
    while True:
        try:
            sent = await _process_reminders_once(bot, session_factory)
            if sent:
                logger.info("[reminder] sent batches=%s", sent)
        except Exception:
            logger.exception("[reminder] loop iteration failed")

        await asyncio.sleep(30)
