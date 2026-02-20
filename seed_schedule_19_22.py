import asyncio
from datetime import datetime

from sqlalchemy import delete, func, select

from app.db import SessionLocal, init_db
from app.models import Match, Point, Prediction, Tournament

SCHEDULE = {
    19: [
        ("2026-02-27 19:30", "Zenit", "Baltika"),
        ("2026-02-28 12:00", "Orenburg", "Akron"),
        ("2026-02-28 14:30", "Krasnodar", "Rostov"),
        ("2026-02-28 17:00", "Lokomotiv M", "Pari Nizhniy Novgorod"),
        ("2026-02-28 19:30", "Makhachkala D", "Rubin"),
        ("2026-03-01 13:00", "Moscow D", "Krylya Sovetov"),
        ("2026-03-01 16:30", "Sochi", "Spartak M"),
        ("2026-03-01 19:00", "Akhmat", "CSKA"),
    ],
    20: [
        ("2026-03-07 16:30", "Rostov", "Baltika"),
        ("2026-03-07 19:00", "Pari Nizhniy Novgorod", "Sochi"),
        ("2026-03-08 12:00", "Orenburg", "Zenit"),
        ("2026-03-08 14:30", "Krylya Sovetov", "Makhachkala D"),
        ("2026-03-08 17:00", "Rubin", "Krasnodar"),
        ("2026-03-08 19:30", "CSKA", "Moscow D"),
        ("2026-03-09 16:30", "Lokomotiv M", "Akhmat"),
        ("2026-03-09 19:00", "Spartak M", "Akron"),
    ],
    21: [
        ("2026-03-13 19:30", "Makhachkala D", "Orenburg"),
        ("2026-03-14 13:45", "Sochi", "Krasnodar"),
        ("2026-03-14 16:00", "Zenit", "Spartak M"),
        ("2026-03-14 18:15", "Rostov", "Moscow D"),
        ("2026-03-14 20:30", "Baltika", "CSKA"),
        ("2026-03-15 14:00", "Akron", "Akhmat"),
        ("2026-03-15 16:30", "Pari Nizhniy Novgorod", "Krylya Sovetov"),
        ("2026-03-15 19:00", "Rubin", "Lokomotiv M"),
    ],
    22: [
        ("2026-03-21 13:45", "Akhmat", "Rostov"),
        ("2026-03-21 16:00", "CSKA", "Makhachkala D"),
        ("2026-03-21 18:15", "Krasnodar", "Pari Nizhniy Novgorod"),
        ("2026-03-21 20:30", "Baltika", "Sochi"),
        ("2026-03-22 13:45", "Orenburg", "Spartak M"),
        ("2026-03-22 16:00", "Moscow D", "Zenit"),
        ("2026-03-22 16:30", "Krylya Sovetov", "Rubin"),
        ("2026-03-22 19:00", "Lokomotiv M", "Akron"),
    ],
}


def mkey(round_number: int, home: str, away: str, kickoff: datetime) -> tuple[int, str, str, datetime]:
    return (round_number, home.strip(), away.strip(), kickoff)


async def main() -> None:
    await init_db()

    planned = {}
    for rnd, matches in SCHEDULE.items():
        for dt_s, home, away in matches:
            dt = datetime.strptime(dt_s, "%Y-%m-%d %H:%M")
            planned[mkey(rnd, home, away, dt)] = (rnd, home, away, dt)

    inserted = 0
    updated = 0
    deleted_unused = 0

    async with SessionLocal() as session:
        t_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = t_q.scalar_one_or_none()
        if rpl is None:
            print("ERROR: RPL tournament not found")
            return

        existing_q = await session.execute(
            select(Match).where(Match.round_number >= 19, Match.round_number <= 22, Match.tournament_id == rpl.id)
        )
        existing = existing_q.scalars().all()

        # Remove extra rows only when they have no predictions/points.
        for row in existing:
            key = mkey(row.round_number, row.home_team, row.away_team, row.kickoff_time)
            if key in planned:
                continue

            preds_q = await session.execute(select(func.count(Prediction.id)).where(Prediction.match_id == row.id))
            points_q = await session.execute(select(func.count(Point.id)).where(Point.match_id == row.id))
            has_preds = int(preds_q.scalar_one() or 0) > 0
            has_points = int(points_q.scalar_one() or 0) > 0
            if has_preds or has_points:
                continue

            await session.execute(delete(Match).where(Match.id == row.id))
            deleted_unused += 1

        existing_q2 = await session.execute(
            select(Match).where(Match.round_number >= 19, Match.round_number <= 22, Match.tournament_id == rpl.id)
        )
        existing2 = existing_q2.scalars().all()
        by_full = {
            mkey(row.round_number, row.home_team, row.away_team, row.kickoff_time): row
            for row in existing2
        }
        by_pair = {
            (row.round_number, row.home_team.strip(), row.away_team.strip()): row
            for row in existing2
        }

        for key, (rnd, home, away, kickoff) in planned.items():
            if key in by_full:
                continue

            pair = (rnd, home.strip(), away.strip())
            row = by_pair.get(pair)
            if row is not None:
                changed = False
                if row.kickoff_time != kickoff:
                    row.kickoff_time = kickoff
                    changed = True
                if row.source != "manual":
                    row.source = "manual"
                    changed = True
                if row.tournament_id != rpl.id:
                    row.tournament_id = rpl.id
                    changed = True
                if changed:
                    updated += 1
                continue

            session.add(
                Match(
                    tournament_id=rpl.id,
                    round_number=rnd,
                    home_team=home,
                    away_team=away,
                    kickoff_time=kickoff,
                    source="manual",
                )
            )
            inserted += 1

        await session.commit()

        for rnd in (19, 20, 21, 22):
            cnt_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.round_number == rnd,
                    Match.source == "manual",
                    Match.tournament_id == rpl.id,
                )
            )
            print(f"ROUND_{rnd}_MANUAL={int(cnt_q.scalar_one() or 0)}")

    print(f"INSERTED={inserted}")
    print(f"UPDATED={updated}")
    print(f"DELETED_UNUSED_EXTRA={deleted_unused}")


if __name__ == "__main__":
    asyncio.run(main())
