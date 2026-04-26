from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlencode

from aiogram import Bot, types
from sqlalchemy import select

from app.audience import is_blocked_send_error, mark_user_blocked
from app.models import Duel, DuelElo, Match, UserTournament

logger = logging.getLogger(__name__)

MINIAPP_WEB_URL = os.getenv("MINIAPP_WEB_URL", "https://rpl-predictions-bot-mini-app.onrender.com").strip()


def _open_duels_keyboard(button_text: str = "Открыть 1х1", duel_id: int | None = None) -> types.InlineKeyboardMarkup | None:
    if not MINIAPP_WEB_URL:
        return None
    url = MINIAPP_WEB_URL
    if duel_id is not None and int(duel_id) > 0:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{urlencode({'screen': 'duels', 'duel_id': int(duel_id)})}"
    return types.InlineKeyboardMarkup(
        inline_keyboard=[[types.InlineKeyboardButton(text=button_text, web_app=types.WebAppInfo(url=url))]]
    )


def _challenge_keyboard(duel_id: int) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="Принять", callback_data=f"duel_accept:{int(duel_id)}"),
                types.InlineKeyboardButton(text="Отклонить", callback_data=f"duel_decline:{int(duel_id)}"),
            ]
        ]
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
            logger.warning("duel notify send failed for %s: %s", chat_id, exc)


async def _duel_context(session, duel_id: int) -> dict[str, Any] | None:
    row = (
        await session.execute(
            select(Duel, Match)
            .join(Match, Match.id == Duel.match_id)
            .where(Duel.id == int(duel_id))
        )
    ).first()
    if row is None:
        return None
    duel, match = row

    names = (
        await session.execute(
            select(UserTournament.tg_user_id, UserTournament.display_name).where(
                UserTournament.tournament_id == int(duel.tournament_id),
                UserTournament.tg_user_id.in_((int(duel.challenger_tg_user_id), int(duel.opponent_tg_user_id))),
            )
        )
    ).all()
    name_map = {int(uid): str(name or f"ID {int(uid)}") for uid, name in names}

    return {
        "duel": duel,
        "match": match,
        "challenger_name": name_map.get(int(duel.challenger_tg_user_id), f"ID {int(duel.challenger_tg_user_id)}"),
        "opponent_name": name_map.get(int(duel.opponent_tg_user_id), f"ID {int(duel.opponent_tg_user_id)}"),
    }


async def send_new_duel_challenge_push(bot: Bot, session, *, duel_id: int) -> None:
    ctx = await _duel_context(session, int(duel_id))
    if ctx is None:
        return
    duel: Duel = ctx["duel"]
    match: Match = ctx["match"]

    challenger_elo = (
        await session.execute(
            select(DuelElo.rating).where(
                DuelElo.tournament_id == int(duel.tournament_id),
                DuelElo.tg_user_id == int(duel.challenger_tg_user_id),
            )
        )
    ).scalar_one_or_none()
    challenger_rating = int(challenger_elo or 1000)

    text = (
        f"⚔️ Тебе бросил вызов {ctx['challenger_name']} ({challenger_rating})\n"
        f"Матч: {match.home_team} — {match.away_team}\n"
        f"Его прогноз: {int(duel.challenger_pred_home)}:{int(duel.challenger_pred_away)}"
    )
    await _safe_send(
        bot,
        session,
        chat_id=int(duel.opponent_tg_user_id),
        text=text,
        reply_markup=_challenge_keyboard(int(duel.id)),
    )


async def send_duel_accepted_push(bot: Bot, session, *, duel_id: int) -> None:
    ctx = await _duel_context(session, int(duel_id))
    if ctx is None:
        return
    duel: Duel = ctx["duel"]
    match: Match = ctx["match"]

    opponent_elo = (
        await session.execute(
            select(DuelElo.rating).where(
                DuelElo.tournament_id == int(duel.tournament_id),
                DuelElo.tg_user_id == int(duel.opponent_tg_user_id),
            )
        )
    ).scalar_one_or_none()
    opponent_rating = int(opponent_elo or 1000)

    text = (
        f"✅ {ctx['opponent_name']} ({opponent_rating}) принял твой вызов\n"
        f"Матч: {match.home_team} — {match.away_team}"
    )
    await _safe_send(
        bot,
        session,
        chat_id=int(duel.challenger_tg_user_id),
        text=text,
        reply_markup=_open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id)),
    )
    await _safe_send(
        bot,
        session,
        chat_id=int(duel.opponent_tg_user_id),
        text="✅ Вызов принят\nОткрой 1х1 в Mini App и поставь свой прогноз.",
        reply_markup=_open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id)),
    )


async def send_duel_declined_push(bot: Bot, session, *, duel_id: int) -> None:
    ctx = await _duel_context(session, int(duel_id))
    if ctx is None:
        return
    duel: Duel = ctx["duel"]
    match: Match = ctx["match"]
    text = (
        f"❌ {ctx['opponent_name']} отклонил твой вызов\n"
        f"Матч: {match.home_team} — {match.away_team}"
    )
    await _safe_send(
        bot,
        session,
        chat_id=int(duel.challenger_tg_user_id),
        text=text,
        reply_markup=_open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id)),
    )


async def send_duel_finished_pushes(bot: Bot, session, *, events: list[dict[str, Any]]) -> None:
    if not events:
        return

    for ev in events:
        duel_id = int(ev.get("duel_id") or 0)
        if duel_id <= 0:
            continue

        ctx = await _duel_context(session, duel_id)
        if ctx is None:
            continue
        duel: Duel = ctx["duel"]
        match: Match = ctx["match"]

        challenger_name = str(ctx["challenger_name"])
        opponent_name = str(ctx["opponent_name"])

        ch_new = int(ev.get("challenger_new_elo") or 1000)
        op_new = int(ev.get("opponent_new_elo") or 1000)
        ch_d = int(ev.get("challenger_delta") or 0)
        op_d = int(ev.get("opponent_delta") or 0)
        ch_old = int(ch_new - ch_d)
        op_old = int(op_new - op_d)
        ch_delta_text = f"{ch_d:+d}"
        op_delta_text = f"{op_d:+d}"

        ch_emoji = "📈" if ch_d > 0 else ("📉" if ch_d < 0 else "")
        op_emoji = "📈" if op_d > 0 else ("📉" if op_d < 0 else "")

        outcome = str(ev.get("outcome") or "")
        if outcome == "challenger_win":
            middle = f"{challenger_name} победил {opponent_name}"
        elif outcome == "opponent_win":
            middle = f"{opponent_name} победил {challenger_name}"
        else:
            middle = f"{challenger_name} и {opponent_name} сыграли вничью"

        pair_low = min(int(duel.challenger_tg_user_id), int(duel.opponent_tg_user_id))
        pair_high = max(int(duel.challenger_tg_user_id), int(duel.opponent_tg_user_id))
        h2h_rows = (
            await session.execute(
                select(Duel.challenger_tg_user_id, Duel.opponent_tg_user_id, Duel.outcome).where(
                    Duel.tournament_id == int(duel.tournament_id),
                    Duel.pair_low_tg_user_id == int(pair_low),
                    Duel.pair_high_tg_user_id == int(pair_high),
                    Duel.status == "finished",
                )
            )
        ).all()
        h2h_w = 0
        h2h_d = 0
        h2h_l = 0
        challenger_id = int(duel.challenger_tg_user_id)
        for row_ch_id, row_op_id, row_outcome in h2h_rows:
            row_ch_id = int(row_ch_id)
            row_op_id = int(row_op_id)
            row_outcome = str(row_outcome or "")
            if row_outcome == "draw":
                h2h_d += 1
            elif row_outcome == "challenger_win":
                if row_ch_id == challenger_id:
                    h2h_w += 1
                elif row_op_id == challenger_id:
                    h2h_l += 1
            elif row_outcome == "opponent_win":
                if row_op_id == challenger_id:
                    h2h_w += 1
                elif row_ch_id == challenger_id:
                    h2h_l += 1

        text = (
            "🏁 Дуэль завершилась!\n"
            f"Матч: {match.home_team} {int(match.home_score)}:{int(match.away_score)} {match.away_team}\n"
            f"Итог: {middle}\n\n"
            "Рейтинг:\n"
            f"{challenger_name}: {ch_old} → {ch_new}" + (f" {ch_emoji}" if ch_emoji else "") + f" ({ch_delta_text})\n"
            f"{opponent_name}: {op_old} → {op_new}" + (f" {op_emoji}" if op_emoji else "") + f" ({op_delta_text})\n\n"
            f"{challenger_name} {h2h_w}-{h2h_d}-{h2h_l} {opponent_name}"
        )
        kb = _open_duels_keyboard("Смотреть 1х1", duel_id=int(duel.id))

        await _safe_send(
            bot,
            session,
            chat_id=int(duel.challenger_tg_user_id),
            text=text,
            reply_markup=kb,
        )
        await _safe_send(
            bot,
            session,
            chat_id=int(duel.opponent_tg_user_id),
            text=text,
            reply_markup=kb,
        )
