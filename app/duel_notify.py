from __future__ import annotations

import logging
import os
from html import escape
from typing import Any
from urllib.parse import urlencode

from aiogram import Bot, types
from sqlalchemy import select

from app.audience import is_blocked_send_error, mark_user_blocked
from app.duels import diff_distance, get_duel_elo_rating_map, score_distance
from app.models import Duel, Match, UserTournament
from app.notify_prefs import should_send_notification

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
    parse_mode: str | None = None,
) -> None:
    try:
        await bot.send_message(chat_id=int(chat_id), text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as exc:
        if is_blocked_send_error(exc):
            await mark_user_blocked(session, int(chat_id))
        else:
            logger.warning("duel notify send failed for %s: %s", chat_id, exc)


def _html(value: Any) -> str:
    return escape(str(value), quote=False)


def _duel_result_line(
    *,
    outcome: str,
    challenger_name: str,
    opponent_name: str,
    challenger_points: int,
    opponent_points: int,
    challenger_pred_home: int,
    challenger_pred_away: int,
    opponent_pred_home: int,
    opponent_pred_away: int,
    real_home: int,
    real_away: int,
) -> str:
    if str(outcome) == "draw":
        if int(challenger_points) == 0 and int(opponent_points) == 0:
            ch_diff_error = diff_distance(challenger_pred_home, challenger_pred_away, real_home, real_away)
            op_diff_error = diff_distance(opponent_pred_home, opponent_pred_away, real_home, real_away)
            if ch_diff_error != op_diff_error:
                edge_name = challenger_name if ch_diff_error < op_diff_error else opponent_name
                return f"🤝 Ничья, но {_html(edge_name)} был ближе к разнице"

            ch_score_error = score_distance(challenger_pred_home, challenger_pred_away, real_home, real_away)
            op_score_error = score_distance(opponent_pred_home, opponent_pred_away, real_home, real_away)
            if ch_score_error != op_score_error:
                edge_name = challenger_name if ch_score_error < op_score_error else opponent_name
                return f"🤝 Ничья, но {_html(edge_name)} был ближе к счёту"

        return "🤝 Ничья. Прогнозы одинаково близки"

    challenger_won = str(outcome) == "challenger_win"
    winner_name = challenger_name if challenger_won else opponent_name
    winner_points = int(challenger_points if challenger_won else opponent_points)

    if int(challenger_points) != int(opponent_points):
        if winner_points >= 4:
            return f"🎯 {_html(winner_name)} победил точным счётом"
        if winner_points == 2:
            return f"📏 {_html(winner_name)} победил по разнице"
        if winner_points == 1:
            return f"✅ {_html(winner_name)} победил по исходу"

    ch_diff_error = diff_distance(challenger_pred_home, challenger_pred_away, real_home, real_away)
    op_diff_error = diff_distance(opponent_pred_home, opponent_pred_away, real_home, real_away)
    if ch_diff_error != op_diff_error:
        return f"📐 {_html(winner_name)} победил по близости к разнице"

    ch_score_error = score_distance(challenger_pred_home, challenger_pred_away, real_home, real_away)
    op_score_error = score_distance(opponent_pred_home, opponent_pred_away, real_home, real_away)
    if ch_score_error != op_score_error:
        return f"🔎 {_html(winner_name)} победил по близости к счёту"

    return f"🔎 {_html(winner_name)} победил по близости"


def _h2h_counts_for_user(h2h_rows: list[Any], recipient_tg_user_id: int) -> tuple[int, int, int]:
    wins = 0
    draws = 0
    losses = 0
    recipient_id = int(recipient_tg_user_id)
    for row_ch_id, row_op_id, row_outcome in h2h_rows:
        row_ch_id = int(row_ch_id)
        row_op_id = int(row_op_id)
        row_outcome = str(row_outcome or "")
        if row_outcome == "draw":
            draws += 1
        elif row_outcome == "challenger_win":
            if row_ch_id == recipient_id:
                wins += 1
            elif row_op_id == recipient_id:
                losses += 1
        elif row_outcome == "opponent_win":
            if row_op_id == recipient_id:
                wins += 1
            elif row_ch_id == recipient_id:
                losses += 1
    return wins, draws, losses


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

    rating_map = await get_duel_elo_rating_map(session, [int(duel.challenger_tg_user_id)])
    challenger_rating = int(rating_map.get(int(duel.challenger_tg_user_id), 1000))

    text = (
        f"⚔️ Тебе бросил вызов {ctx['challenger_name']} ({challenger_rating})\n"
        f"Матч: {match.home_team} — {match.away_team}\n"
        f"Его прогноз: {int(duel.challenger_pred_home)}:{int(duel.challenger_pred_away)}"
    )
    if await should_send_notification(session, int(duel.opponent_tg_user_id), "duels"):
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

    rating_map = await get_duel_elo_rating_map(session, [int(duel.opponent_tg_user_id)])
    opponent_rating = int(rating_map.get(int(duel.opponent_tg_user_id), 1000))

    text = (
        f"✅ {ctx['opponent_name']} ({opponent_rating}) принял твой вызов\n"
        f"Матч: {match.home_team} — {match.away_team}"
    )
    if await should_send_notification(session, int(duel.challenger_tg_user_id), "duels"):
        await _safe_send(
            bot,
            session,
            chat_id=int(duel.challenger_tg_user_id),
            text=text,
            reply_markup=_open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id)),
        )
    if await should_send_notification(session, int(duel.opponent_tg_user_id), "duels"):
        await _safe_send(
            bot,
            session,
            chat_id=int(duel.opponent_tg_user_id),
            text=(
                "✅ Вызов принят\n"
                f"Соперник: {ctx['challenger_name']}\n"
                f"Матч: {match.home_team} — {match.away_team}\n"
                "Открой 1х1 в Mini App и поставь свой прогноз."
            ),
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
    if await should_send_notification(session, int(duel.challenger_tg_user_id), "duels"):
        await _safe_send(
            bot,
            session,
            chat_id=int(duel.challenger_tg_user_id),
            text=text,
            reply_markup=_open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id)),
        )


async def send_duel_cancelled_push(bot: Bot, session, *, duel_id: int) -> None:
    ctx = await _duel_context(session, int(duel_id))
    if ctx is None:
        return
    duel: Duel = ctx["duel"]
    match: Match = ctx["match"]
    text = (
        "❌ Вызов 1х1 отменён\n"
        f"Матч: {match.home_team} — {match.away_team}\n"
        f"{ctx['challenger_name']} отменил вызов."
    )
    if await should_send_notification(session, int(duel.opponent_tg_user_id), "duels"):
        await _safe_send(
            bot,
            session,
            chat_id=int(duel.opponent_tg_user_id),
            text=text,
            reply_markup=_open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id)),
        )


async def send_duel_expired_pushes(bot: Bot, session, *, events: list[dict[str, Any]]) -> None:
    for ev in events:
        duel_id = int(ev.get("duel_id") or 0)
        if duel_id <= 0:
            continue
        ctx = await _duel_context(session, duel_id)
        if ctx is None:
            continue
        duel: Duel = ctx["duel"]
        match: Match = ctx["match"]
        keyboard = _open_duels_keyboard("Открыть 1х1", duel_id=int(duel.id))

        challenger_text = (
            "⌛ Вызов 1х1 истёк\n"
            f"Матч: {match.home_team} — {match.away_team}\n"
            f"{ctx['opponent_name']} не успел принять вызов в течение 3 часов."
        )
        opponent_text = (
            "⌛ Вызов 1х1 истёк\n"
            f"Матч: {match.home_team} — {match.away_team}\n"
            f"Вызов от {ctx['challenger_name']} больше неактуален."
        )

        if await should_send_notification(session, int(duel.challenger_tg_user_id), "duels"):
            await _safe_send(
                bot,
                session,
                chat_id=int(duel.challenger_tg_user_id),
                text=challenger_text,
                reply_markup=keyboard,
            )
        if await should_send_notification(session, int(duel.opponent_tg_user_id), "duels"):
            await _safe_send(
                bot,
                session,
                chat_id=int(duel.opponent_tg_user_id),
                text=opponent_text,
                reply_markup=keyboard,
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
        ch_pts = int(ev.get("challenger_points") or 0)
        op_pts = int(ev.get("opponent_points") or 0)
        match_home_score = int(match.home_score)
        match_away_score = int(match.away_score)
        ch_pred_home = int(duel.challenger_pred_home)
        ch_pred_away = int(duel.challenger_pred_away)
        op_pred_home = int(duel.opponent_pred_home or 0)
        op_pred_away = int(duel.opponent_pred_away or 0)
        result_line = _duel_result_line(
            outcome=outcome,
            challenger_name=challenger_name,
            opponent_name=opponent_name,
            challenger_points=ch_pts,
            opponent_points=op_pts,
            challenger_pred_home=ch_pred_home,
            challenger_pred_away=ch_pred_away,
            opponent_pred_home=op_pred_home,
            opponent_pred_away=op_pred_away,
            real_home=match_home_score,
            real_away=match_away_score,
        )

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
        kb = _open_duels_keyboard("Смотреть 1х1", duel_id=int(duel.id))

        def message_for(recipient_tg_user_id: int) -> str:
            h2h_w, h2h_d, h2h_l = _h2h_counts_for_user(h2h_rows, int(recipient_tg_user_id))
            return (
                "🏁 Дуэль завершилась!\n\n"
                f"<b>{_html(match.home_team)} {match_home_score}:{match_away_score} {_html(match.away_team)}</b>\n"
                "Прогнозы: "
                f"{_html(challenger_name)} {ch_pred_home}:{ch_pred_away} · "
                f"{_html(opponent_name)} {op_pred_home}:{op_pred_away}\n\n"
                f"{result_line}\n\n"
                f"{ch_emoji or '•'} {_html(challenger_name)}: {ch_old} → {ch_new} ({ch_delta_text})\n"
                f"{op_emoji or '•'} {_html(opponent_name)}: {op_old} → {op_new} ({op_delta_text})\n\n"
                f"Личные встречи: {h2h_w}-{h2h_d}-{h2h_l}"
            )

        if await should_send_notification(session, int(duel.challenger_tg_user_id), "duels"):
            await _safe_send(
                bot,
                session,
                chat_id=int(duel.challenger_tg_user_id),
                text=message_for(int(duel.challenger_tg_user_id)),
                reply_markup=kb,
                parse_mode="HTML",
            )
        if await should_send_notification(session, int(duel.opponent_tg_user_id), "duels"):
            await _safe_send(
                bot,
                session,
                chat_id=int(duel.opponent_tg_user_id),
                text=message_for(int(duel.opponent_tg_user_id)),
                reply_markup=kb,
                parse_mode="HTML",
            )
