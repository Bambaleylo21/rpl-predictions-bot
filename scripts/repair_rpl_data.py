from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, time

from sqlalchemy import and_, delete, func, or_, select

from app.db import SessionLocal
from app.models import Match, Point, Prediction, Setting, Tournament
from app.scoring import calculate_points

RPL_CODE = "RPL"
RPL_ROUND_MIN = 19
RPL_ROUND_MAX = 30


@dataclass
class Stats:
    matches: int
    played: int
    predictions: int
    points: int


async def _get_setting(session, key: str) -> str | None:
    row = (await session.execute(select(Setting).where(Setting.key == key))).scalar_one_or_none()
    return row.value if row else None


async def _read_window(session) -> tuple[datetime | None, datetime | None]:
    start_raw = await _get_setting(session, "TOURNAMENT_START_DATE")
    end_raw = await _get_setting(session, "TOURNAMENT_END_DATE")
    if not start_raw or not end_raw:
        return None, None

    try:
        start_dt = datetime.combine(datetime.fromisoformat(start_raw).date(), time.min)
        end_dt = datetime.combine(datetime.fromisoformat(end_raw).date(), time.max)
        return start_dt, end_dt
    except Exception:
        return None, None


def _keep_filter(rpl_id: int, start_dt: datetime | None, end_dt: datetime | None):
    cond = and_(
        Match.tournament_id == rpl_id,
        Match.round_number >= RPL_ROUND_MIN,
        Match.round_number <= RPL_ROUND_MAX,
    )
    if start_dt is not None and end_dt is not None:
        cond = and_(cond, Match.kickoff_time >= start_dt, Match.kickoff_time <= end_dt)
    return cond


async def _collect_stats(session, keep_cond) -> Stats:
    matches = int((await session.execute(select(func.count(Match.id)).where(keep_cond))).scalar_one() or 0)
    played = int(
        (
            await session.execute(
                select(func.count(Match.id)).where(
                    keep_cond, Match.home_score.isnot(None), Match.away_score.isnot(None)
                )
            )
        ).scalar_one()
        or 0
    )
    predictions = int(
        (
            await session.execute(
                select(func.count(Prediction.id)).join(Match, Match.id == Prediction.match_id).where(keep_cond)
            )
        ).scalar_one()
        or 0
    )
    points = int(
        (
            await session.execute(
                select(func.count(Point.id)).join(Match, Match.id == Point.match_id).where(keep_cond)
            )
        ).scalar_one()
        or 0
    )
    return Stats(matches=matches, played=played, predictions=predictions, points=points)


async def _dedupe_matches(session, keep_cond) -> int:
    q = await session.execute(
        select(
            Match.id,
            Match.api_fixture_id,
            Match.round_number,
            Match.home_team,
            Match.away_team,
            Match.kickoff_time,
        )
        .where(keep_cond)
        .order_by(Match.id.asc())
    )
    rows = q.all()
    keep_by_key: dict[tuple, int] = {}
    duplicates: list[tuple[int, int]] = []  # (dup_id, keep_id)

    for match_id, fixture_id, rnd, home, away, kickoff in rows:
        if fixture_id is not None:
            key = ("fx", int(fixture_id))
        else:
            key = ("sig", int(rnd), home or "", away or "", kickoff)
        keep_id = keep_by_key.get(key)
        if keep_id is None:
            keep_by_key[key] = int(match_id)
        else:
            duplicates.append((int(match_id), int(keep_id)))

    for dup_id, keep_id in duplicates:
        pred_rows = (
            await session.execute(select(Prediction).where(Prediction.match_id == dup_id).order_by(Prediction.id.asc()))
        ).scalars().all()
        for p in pred_rows:
            exists = (
                await session.execute(
                    select(Prediction.id).where(
                        Prediction.match_id == keep_id,
                        Prediction.tg_user_id == p.tg_user_id,
                    )
                )
            ).scalar_one_or_none()
            if exists is None:
                p.match_id = keep_id
            else:
                await session.delete(p)

        point_rows = (
            await session.execute(select(Point).where(Point.match_id == dup_id).order_by(Point.id.asc()))
        ).scalars().all()
        for pt in point_rows:
            exists = (
                await session.execute(
                    select(Point.id).where(
                        Point.match_id == keep_id,
                        Point.tg_user_id == pt.tg_user_id,
                    )
                )
            ).scalar_one_or_none()
            if exists is None:
                pt.match_id = keep_id
            else:
                await session.delete(pt)

        await session.execute(delete(Match).where(Match.id == dup_id))

    return len(duplicates)


async def _recalc_points(session, keep_cond) -> int:
    keep_ids_subq = select(Match.id).where(keep_cond)
    await session.execute(delete(Point).where(Point.match_id.in_(keep_ids_subq)))

    played_matches = (
        await session.execute(
            select(Match.id, Match.home_score, Match.away_score).where(
                keep_cond, Match.home_score.isnot(None), Match.away_score.isnot(None)
            )
        )
    ).all()

    inserts = 0
    for match_id, home_score, away_score in played_matches:
        preds = (
            await session.execute(select(Prediction).where(Prediction.match_id == int(match_id)).order_by(Prediction.id.asc()))
        ).scalars().all()
        for p in preds:
            calc = calculate_points(
                pred_home=int(p.pred_home),
                pred_away=int(p.pred_away),
                real_home=int(home_score),
                real_away=int(away_score),
            )
            session.add(
                Point(
                    match_id=int(match_id),
                    tg_user_id=int(p.tg_user_id),
                    points=int(calc.points),
                    category=str(calc.category),
                )
            )
            inserts += 1

    return inserts


async def run(apply_changes: bool) -> None:
    async with SessionLocal() as session:
        rpl = (await session.execute(select(Tournament).where(Tournament.code == RPL_CODE))).scalar_one_or_none()
        if rpl is None:
            raise RuntimeError("RPL tournament not found")

        # Нормализуем окно туров РПЛ.
        rpl.round_min = RPL_ROUND_MIN
        rpl.round_max = RPL_ROUND_MAX

        start_dt, end_dt = await _read_window(session)
        keep_cond = _keep_filter(int(rpl.id), start_dt, end_dt)

        before = await _collect_stats(session, keep_cond)
        total_all = int((await session.execute(select(func.count(Match.id)))).scalar_one() or 0)
        print(f"[before] all_matches={total_all} keep_matches={before.matches} keep_played={before.played} keep_preds={before.predictions} keep_points={before.points}")
        if start_dt is not None and end_dt is not None:
            print(f"[window] {start_dt.isoformat()} .. {end_dt.isoformat()}")
        else:
            print("[window] not set in settings, using only rounds 19..30")

        if not apply_changes:
            print("[dry-run] no changes applied")
            await session.rollback()
            return

        drop_cond = or_(
            Match.tournament_id != int(rpl.id),
            Match.round_number < RPL_ROUND_MIN,
            Match.round_number > RPL_ROUND_MAX,
        )
        if start_dt is not None and end_dt is not None:
            drop_cond = or_(
                drop_cond,
                Match.kickoff_time < start_dt,
                Match.kickoff_time > end_dt,
            )

        drop_ids_subq = select(Match.id).where(drop_cond)
        del_points = await session.execute(delete(Point).where(Point.match_id.in_(drop_ids_subq)))
        del_preds = await session.execute(delete(Prediction).where(Prediction.match_id.in_(drop_ids_subq)))
        del_matches = await session.execute(delete(Match).where(drop_cond))
        await session.commit()

        deduped = await _dedupe_matches(session, keep_cond)
        inserted_points = await _recalc_points(session, keep_cond)
        await session.commit()

        after = await _collect_stats(session, keep_cond)
        total_after = int((await session.execute(select(func.count(Match.id)))).scalar_one() or 0)

        print(
            f"[cleanup] deleted_matches={del_matches.rowcount or 0} deleted_predictions={del_preds.rowcount or 0} deleted_points={del_points.rowcount or 0}"
        )
        print(f"[dedupe] merged_matches={deduped}")
        print(f"[recalc] inserted_points={inserted_points}")
        print(f"[after] all_matches={total_after} keep_matches={after.matches} keep_played={after.played} keep_preds={after.predictions} keep_points={after.points}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair RPL data: keep only RPL rounds 19..30, dedupe, recalc points.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag script runs in dry-run mode.")
    args = parser.parse_args()
    asyncio.run(run(apply_changes=bool(args.apply)))


if __name__ == "__main__":
    main()
