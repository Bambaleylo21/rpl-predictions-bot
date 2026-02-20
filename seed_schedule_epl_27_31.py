import asyncio
from datetime import datetime

from sqlalchemy import delete, func, select

from app.db import SessionLocal, init_db
from app.models import Match, Point, Prediction, Tournament

SCHEDULE = {
    27: [
        ("2026-02-21 18:00", "Aston Villa", "Leeds United"),
        ("2026-02-21 18:00", "Chelsea", "Burnley"),
        ("2026-02-21 18:00", "Brentford", "Brighton & Hove Albion"),
        ("2026-02-21 20:30", "West Ham United", "Bournemouth"),
        ("2026-02-21 23:00", "Manchester City", "Newcastle United"),
        ("2026-02-22 17:00", "Nottingham Forest", "Liverpool"),
        ("2026-02-22 17:00", "Sunderland", "Fulham"),
        ("2026-02-22 17:00", "Crystal Palace", "Wolverhampton"),
        ("2026-02-22 19:30", "Tottenham Hotspur", "Arsenal"),
        ("2026-02-23 23:00", "Everton", "Manchester United"),
    ],
    28: [
        ("2026-02-27 23:00", "Wolverhampton", "Aston Villa"),
        ("2026-02-28 15:30", "Bournemouth", "Sunderland"),
        ("2026-02-28 18:00", "Newcastle United", "Everton"),
        ("2026-02-28 18:00", "Liverpool", "West Ham United"),
        ("2026-02-28 18:00", "Burnley", "Brentford"),
        ("2026-02-28 20:30", "Leeds United", "Manchester City"),
        ("2026-03-01 17:00", "Manchester United", "Crystal Palace"),
        ("2026-03-01 17:00", "Fulham", "Tottenham Hotspur"),
        ("2026-03-01 17:00", "Brighton & Hove Albion", "Nottingham Forest"),
        ("2026-03-01 19:30", "Arsenal", "Chelsea"),
    ],
    29: [
        ("2026-03-03 22:30", "Leeds United", "Sunderland"),
        ("2026-03-03 22:30", "Everton", "Burnley"),
        ("2026-03-03 22:30", "Bournemouth", "Brentford"),
        ("2026-03-03 23:15", "Wolverhampton", "Liverpool"),
        ("2026-03-04 22:30", "Fulham", "West Ham United"),
        ("2026-03-04 22:30", "Manchester City", "Nottingham Forest"),
        ("2026-03-04 22:30", "Brighton & Hove Albion", "Arsenal"),
        ("2026-03-04 22:30", "Aston Villa", "Chelsea"),
        ("2026-03-04 23:15", "Newcastle United", "Manchester United"),
        ("2026-03-05 23:00", "Tottenham Hotspur", "Crystal Palace"),
    ],
    30: [
        ("2026-03-14 15:30", "West Ham United", "Manchester City"),
        ("2026-03-14 18:00", "Sunderland", "Brighton & Hove Albion"),
        ("2026-03-14 18:00", "Burnley", "Bournemouth"),
        ("2026-03-14 18:00", "Crystal Palace", "Leeds United"),
        ("2026-03-14 20:30", "Chelsea", "Newcastle United"),
        ("2026-03-15 17:00", "Manchester United", "Aston Villa"),
        ("2026-03-15 17:00", "Arsenal", "Everton"),
        ("2026-03-15 17:00", "Nottingham Forest", "Fulham"),
        ("2026-03-15 19:30", "Liverpool", "Tottenham Hotspur"),
        ("2026-03-16 23:00", "Brentford", "Wolverhampton"),
    ],
    31: [
        ("2026-03-20 23:00", "Bournemouth", "Manchester United"),
        ("2026-03-21 15:30", "Brighton & Hove Albion", "Liverpool"),
        ("2026-03-21 18:00", "Fulham", "Burnley"),
        ("2026-03-21 18:00", "Manchester City", "Crystal Palace"),
        ("2026-03-21 20:30", "Everton", "Chelsea"),
        ("2026-03-21 23:00", "Leeds United", "Brentford"),
        ("2026-03-22 15:00", "Newcastle United", "Sunderland"),
        ("2026-03-22 17:15", "Tottenham Hotspur", "Nottingham Forest"),
        ("2026-03-22 17:15", "Aston Villa", "West Ham United"),
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
        t_q = await session.execute(select(Tournament).where(Tournament.code == "EPL"))
        epl = t_q.scalar_one_or_none()
        if epl is None:
            print("ERROR: EPL tournament not found")
            return

        existing_q = await session.execute(
            select(Match).where(Match.round_number >= 27, Match.round_number <= 31, Match.tournament_id == epl.id)
        )
        existing = existing_q.scalars().all()

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
            select(Match).where(Match.round_number >= 27, Match.round_number <= 31, Match.tournament_id == epl.id)
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
                if row.tournament_id != epl.id:
                    row.tournament_id = epl.id
                    changed = True
                if changed:
                    updated += 1
                continue

            session.add(
                Match(
                    tournament_id=epl.id,
                    round_number=rnd,
                    home_team=home,
                    away_team=away,
                    kickoff_time=kickoff,
                    source="manual",
                )
            )
            inserted += 1

        await session.commit()

        for rnd in (27, 28, 29, 30, 31):
            cnt_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.round_number == rnd,
                    Match.source == "manual",
                    Match.tournament_id == epl.id,
                )
            )
            print(f"EPL_ROUND_{rnd}_MANUAL={int(cnt_q.scalar_one() or 0)}")

    print(f"INSERTED={inserted}")
    print(f"UPDATED={updated}")
    print(f"DELETED_UNUSED_EXTRA={deleted_unused}")


if __name__ == "__main__":
    asyncio.run(main())
