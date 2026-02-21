from collections import defaultdict
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Match, Point, User, UserTournament


async def build_stats_text(tournament_id: int | None = None) -> str:
    """
    Строит общий отчёт по всем участникам:
    - очки
    - кол-во exact/diff/outcome/none
    - проценты
    """
    async with SessionLocal() as session:
        res_users = await session.execute(select(User))
        users = res_users.scalars().all()
        user_tournament_rows = []
        if tournament_id is not None:
            ut_q = await session.execute(select(UserTournament).where(UserTournament.tournament_id == tournament_id))
            user_tournament_rows = ut_q.scalars().all()

        points_q = select(Point)
        if tournament_id is not None:
            points_q = (
                select(Point)
                .join(Match, Match.id == Point.match_id)
                .where(Match.tournament_id == tournament_id, Match.source == "manual")
            )

        res_points = await session.execute(points_q)
        points_rows = res_points.scalars().all()

    # Мапа tg_user_id -> имя
    tournament_names = {u.tg_user_id: u.display_name for u in user_tournament_rows if u.display_name}
    names = {}
    for u in users:
        if u.tg_user_id in tournament_names:
            names[u.tg_user_id] = tournament_names[u.tg_user_id]
        elif u.display_name:
            names[u.tg_user_id] = u.display_name
        elif u.username:
            names[u.tg_user_id] = f"@{u.username}"
        elif u.full_name:
            names[u.tg_user_id] = u.full_name
        else:
            names[u.tg_user_id] = str(u.tg_user_id)

    # Считаем по пользователям
    per_user = defaultdict(lambda: {"total": 0, "exact": 0, "diff": 0, "outcome": 0, "none": 0, "cnt": 0})

    for r in points_rows:
        pu = per_user[r.tg_user_id]
        pu["total"] += int(r.points)
        pu["cnt"] += 1
        if r.category in ("exact", "diff", "outcome", "none"):
            pu[r.category] += 1
        else:
            pu["none"] += 1

    if not per_user:
        return (
            "Пока нет статистики по очкам.\n"
            "Как только появятся результаты матчей, таблица здесь сразу оживёт."
        )

    # Формируем таблицу (топ-20)
    rows = []
    for tg_id, s in per_user.items():
        rows.append({
            "name": names.get(tg_id, str(tg_id)),
            **s
        })

    rows.sort(key=lambda x: (x["total"], x["exact"], x["diff"], x["outcome"]), reverse=True)

    lines = ["📊 Подробная статистика (топ-20):"]
    for i, r in enumerate(rows[:20], start=1):
        cnt = r["cnt"] or 1
        exact_pct = round(r["exact"] * 100 / cnt)
        diff_pct = round(r["diff"] * 100 / cnt)
        out_pct = round(r["outcome"] * 100 / cnt)
        none_pct = round(r["none"] * 100 / cnt)

        lines.append(
            f"{i}. {r['name']} — {r['total']} очк. "
            f"| 🎯{r['exact']} ({exact_pct}%) "
            f"| 📏{r['diff']} ({diff_pct}%) "
            f"| ✅{r['outcome']} ({out_pct}%) "
            f"| ❌{r['none']} ({none_pct}%) "
            f"| всего: {r['cnt']}"
        )

    return "\n".join(lines)
