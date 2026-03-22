from sqlalchemy import select

from app.db import SessionLocal
from app.display import display_team_name, display_tournament_name
from app.models import Match, Prediction, Point, Tournament


def _point_category_emoji(category: str | None, points: int | None) -> str:
    cat = (category or "").strip().lower()
    if cat == "exact":
        return "🎯"
    if cat == "diff":
        return "📏"
    if cat == "outcome":
        return "✅"
    # none / пусто / неизвестно — считаем промахом
    if int(points or 0) <= 0:
        return "❌"
    return "✅"


async def build_my_round_text(tg_user_id: int, round_number: int, tournament_id: int | None = None) -> str:
    async with SessionLocal() as session:
        q = select(Match).where(Match.round_number == round_number)
        tournament = None
        if tournament_id is not None:
            q = q.where(Match.tournament_id == tournament_id)
            t_q = await session.execute(select(Tournament).where(Tournament.id == tournament_id))
            tournament = t_q.scalar_one_or_none()
        res_matches = await session.execute(q.order_by(Match.kickoff_time.asc()))
        matches = res_matches.scalars().all()

        if not matches:
            return (
                f"В туре {round_number} пока нет матчей.\n"
                "Проверь соседний тур или загляни чуть позже."
            )

        # прогнозы пользователя на матчи тура
        match_ids = [m.id for m in matches]
        res_preds = await session.execute(
            select(Prediction).where(Prediction.tg_user_id == tg_user_id, Prediction.match_id.in_(match_ids))
        )
        preds = res_preds.scalars().all()
        preds_map = {p.match_id: p for p in preds}

        # очки пользователя по этим матчам (если уже пересчитано)
        res_points = await session.execute(
            select(Point).where(Point.tg_user_id == tg_user_id, Point.match_id.in_(match_ids))
        )
        pts = res_points.scalars().all()
        pts_map = {p.match_id: p for p in pts}

    tournament_name = display_tournament_name(tournament.name) if tournament is not None else "РПЛ"
    open_lines: list[str] = []
    closed_lines: list[str] = []
    closed_total = 0

    for m in matches:
        home = display_team_name(m.home_team)
        away = display_team_name(m.away_team)
        pred = preds_map.get(m.id)
        kickoff_txt = m.kickoff_time.strftime("%d.%m %H:%M")

        if m.home_score is None or m.away_score is None:
            suffix = "без прогноза"
            if pred is not None:
                suffix = f"прогноз: {pred.pred_home}:{pred.pred_away}"
            open_lines.append(f"{home} — {away} | {kickoff_txt} МСК | {suffix}")
            continue

        score_line = f"{home} {m.home_score}:{m.away_score} {away}"
        if pred is None:
            closed_lines.append(f"{score_line} | без прогноза")
            continue

        pt = pts_map.get(m.id)
        if pt is None:
            closed_lines.append(f"{score_line} | прогноз: {pred.pred_home}:{pred.pred_away}")
            continue

        pts_val = int(pt.points or 0)
        closed_total += pts_val
        emoji = _point_category_emoji(pt.category, pt.points)
        closed_lines.append(f"{score_line} | прогноз: {pred.pred_home}:{pred.pred_away} {emoji} {pts_val}")

    lines = ["🗂 Мои прогнозы", "", f"🗓 {tournament_name} • Тур {round_number}", ""]
    lines.append("🟢 Открытые")
    if open_lines:
        lines.extend(open_lines)
    else:
        lines.append("Нет открытых матчей.")

    lines.append("")
    lines.append("✅ Завершённые")
    if closed_lines:
        lines.extend(closed_lines)
    else:
        lines.append("Нет завершённых матчей.")

    lines.append("")
    lines.append(f"Итого за тур (по закрытым): {closed_total} очк.")
    return "\n".join(lines)
