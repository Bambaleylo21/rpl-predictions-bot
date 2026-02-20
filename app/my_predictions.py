from sqlalchemy import select

from app.db import SessionLocal
from app.models import Match, Prediction, Point


async def build_my_round_text(tg_user_id: int, round_number: int) -> str:
    async with SessionLocal() as session:
        res_matches = await session.execute(
            select(Match).where(Match.round_number == round_number, Match.source == "manual").order_by(Match.kickoff_time.asc())
        )
        matches = res_matches.scalars().all()

        if not matches:
            return f"В туре {round_number} пока нет матчей."

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

    lines = [f"🧾 Мои прогнозы — тур {round_number}:"]
    for m in matches:
        pred = preds_map.get(m.id)
        pred_txt = "—" if pred is None else f"{pred.pred_home}:{pred.pred_away}"

        result_txt = ""
        if m.home_score is not None and m.away_score is not None:
            result_txt = f" | итог {m.home_score}:{m.away_score}"
            pt = pts_map.get(m.id)
            if pt is not None:
                result_txt += f" | очки {pt.points} ({pt.category})"

        lines.append(f"#{m.id} {m.home_team} — {m.away_team} | прогноз: {pred_txt}{result_txt}")

    return "\n".join(lines)
