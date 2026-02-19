from collections import defaultdict
from sqlalchemy import select

from app.db import SessionLocal
from app.models import User, Point


async def build_stats_text() -> str:
    """
    Строит общий отчёт по всем участникам:
    - очки
    - кол-во exact/diff/outcome/none
    - проценты
    """
    async with SessionLocal() as session:
        res_users = await session.execute(select(User))
        users = res_users.scalars().all()

        res_points = await session.execute(select(Point))
        points_rows = res_points.scalars().all()

    # Мапа tg_user_id -> имя
    names = {}
    for u in users:
        names[u.tg_user_id] = u.username if u.username else str(u.tg_user_id)

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
        return "Пока нет начислений (сначала нужно поставить результаты и сделать /admin_recalc)."

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