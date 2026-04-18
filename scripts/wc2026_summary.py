from __future__ import annotations

import asyncio

from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import Match, Tournament


async def main() -> None:
    async with SessionLocal() as session:
        tq = await session.execute(select(Tournament).where(Tournament.code == "WC2026").limit(1))
        t = tq.scalar_one_or_none()
        if t is None:
            print("WC2026 tournament not found")
            return

        total_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == int(t.id),
                Match.is_placeholder == 0,
            )
        )
        total = int(total_q.scalar_one() or 0)

        rows_q = await session.execute(
            select(Match.round_number, func.count(Match.id))
            .where(
                Match.tournament_id == int(t.id),
                Match.is_placeholder == 0,
            )
            .group_by(Match.round_number)
            .order_by(Match.round_number.asc())
        )
        rows = rows_q.all()

    print(f"WC2026 total matches: {total}")
    for rnd, cnt in rows:
        print(f"round {int(rnd)}: {int(cnt)}")


if __name__ == "__main__":
    asyncio.run(main())
