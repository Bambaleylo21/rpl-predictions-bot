from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import case, func, select

from app.db import SessionLocal
from app.models import (
    League,
    LeagueParticipant,
    Match,
    Point,
    Prediction,
    Tournament,
    User,
    UserTournament,
)
from app.season_setup import get_active_season, get_active_stage


@dataclass
class LeagueTableMeta:
    season_name: str
    stage_name: str
    stage_round_min: int
    stage_round_max: int
    league_code: str
    league_name: str
    participants: int


@dataclass
class UserStageScope:
    season_id: int
    season_name: str
    stage_id: int
    stage_name: str
    stage_round_min: int
    stage_round_max: int
    league_id: int
    league_code: str
    league_name: str
    member_ids: set[int]


def _resolve_name(
    lp_display_name: str | None,
    ut_display_name: str | None,
    user_display_name: str | None,
    username: str | None,
    full_name: str | None,
    tg_user_id: int,
) -> str:
    if lp_display_name:
        return lp_display_name
    if ut_display_name:
        return ut_display_name
    if user_display_name:
        return user_display_name
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return str(tg_user_id)


async def get_user_active_league_name(tg_user_id: int) -> str | None:
    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            return None
        stage = await get_active_stage(session, season.id)
        if stage is None:
            return None

        row_q = await session.execute(
            select(League.name)
            .select_from(LeagueParticipant)
            .join(League, League.id == LeagueParticipant.league_id)
            .where(
                LeagueParticipant.stage_id == stage.id,
                LeagueParticipant.tg_user_id == tg_user_id,
                LeagueParticipant.is_active == 1,
            )
        )
        row = row_q.first()
        if not row:
            return None
        return str(row[0])


async def get_user_stage_scope(tg_user_id: int) -> UserStageScope | None:
    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            return None
        stage = await get_active_stage(session, season.id)
        if stage is None:
            return None

        mine_q = await session.execute(
            select(League.id, League.code, League.name)
            .select_from(LeagueParticipant)
            .join(League, League.id == LeagueParticipant.league_id)
            .where(
                LeagueParticipant.stage_id == stage.id,
                LeagueParticipant.tg_user_id == tg_user_id,
                LeagueParticipant.is_active == 1,
            )
        )
        mine = mine_q.first()
        if mine is None:
            return None
        league_id, league_code, league_name = mine

        members_q = await session.execute(
            select(LeagueParticipant.tg_user_id).where(
                LeagueParticipant.stage_id == stage.id,
                LeagueParticipant.league_id == int(league_id),
                LeagueParticipant.is_active == 1,
            )
        )
        member_ids = {int(x[0]) for x in members_q.all()}

        return UserStageScope(
            season_id=int(season.id),
            season_name=str(season.name),
            stage_id=int(stage.id),
            stage_name=str(stage.name),
            stage_round_min=int(stage.round_min),
            stage_round_max=int(stage.round_max),
            league_id=int(league_id),
            league_code=str(league_code),
            league_name=str(league_name),
            member_ids=member_ids,
        )


async def build_active_stage_league_table(
    tg_user_id: int,
    requested_league_code: str | None = None,
) -> tuple[list[dict], LeagueTableMeta | None]:
    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            return [], None

        stage = await get_active_stage(session, season.id)
        if stage is None:
            return [], None

        rpl_q = await session.execute(select(Tournament).where(Tournament.code == "RPL"))
        rpl = rpl_q.scalar_one_or_none()
        if rpl is None:
            return [], None

        leagues_q = await session.execute(
            select(League).where(League.season_id == season.id, League.is_active == 1)
        )
        leagues = leagues_q.scalars().all()
        if not leagues:
            return [], None

        by_code = {str(l.code).upper(): l for l in leagues}

        selected_league = None
        if requested_league_code:
            selected_league = by_code.get(requested_league_code.upper())

        if selected_league is None:
            mine_q = await session.execute(
                select(League)
                .join(LeagueParticipant, LeagueParticipant.league_id == League.id)
                .where(
                    LeagueParticipant.stage_id == stage.id,
                    LeagueParticipant.tg_user_id == tg_user_id,
                    LeagueParticipant.is_active == 1,
                )
            )
            selected_league = mine_q.scalar_one_or_none()

        if selected_league is None:
            selected_league = by_code.get("HIGH") or sorted(leagues, key=lambda x: int(x.id))[0]

        participants_q = await session.execute(
            select(
                LeagueParticipant.tg_user_id,
                LeagueParticipant.display_name,
                UserTournament.display_name,
                UserTournament.bonus_points,
                User.display_name,
                User.username,
                User.full_name,
            )
            .select_from(LeagueParticipant)
            .outerjoin(
                UserTournament,
                (UserTournament.tg_user_id == LeagueParticipant.tg_user_id)
                & (UserTournament.tournament_id == rpl.id),
            )
            .outerjoin(User, User.tg_user_id == LeagueParticipant.tg_user_id)
            .where(
                LeagueParticipant.stage_id == stage.id,
                LeagueParticipant.league_id == selected_league.id,
                LeagueParticipant.is_active == 1,
            )
            .order_by(LeagueParticipant.tg_user_id.asc())
        )
        participant_rows = participants_q.all()

        if not participant_rows:
            meta = LeagueTableMeta(
                season_name=str(season.name),
                stage_name=str(stage.name),
                stage_round_min=int(stage.round_min),
                stage_round_max=int(stage.round_max),
                league_code=str(selected_league.code),
                league_name=str(selected_league.name),
                participants=0,
            )
            return [], meta

        user_ids = [int(r[0]) for r in participant_rows]

        preds_q = await session.execute(
            select(
                Prediction.tg_user_id,
                func.count(Prediction.id).label("pred_total"),
                func.max(func.coalesce(Prediction.updated_at, Prediction.created_at)).label("last_pred_at"),
            )
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Prediction.tg_user_id.in_(user_ids),
                Match.tournament_id == rpl.id,
                Match.round_number >= stage.round_min,
                Match.round_number <= stage.round_max,
            )
            .group_by(Prediction.tg_user_id)
        )
        pred_map = {
            int(uid): (int(total or 0), last_pred_at)
            for uid, total, last_pred_at in preds_q.all()
        }

        now = datetime.utcnow()
        started_matches_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == rpl.id,
                Match.round_number >= stage.round_min,
                Match.round_number <= stage.round_max,
                Match.kickoff_time <= now,
            )
        )
        started_matches_total = int(started_matches_q.scalar_one() or 0)

        pred_started_q = await session.execute(
            select(Prediction.tg_user_id, func.count(Prediction.id))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Prediction.tg_user_id.in_(user_ids),
                Match.tournament_id == rpl.id,
                Match.round_number >= stage.round_min,
                Match.round_number <= stage.round_max,
                Match.kickoff_time <= now,
            )
            .group_by(Prediction.tg_user_id)
        )
        pred_started_map = {int(uid): int(cnt or 0) for uid, cnt in pred_started_q.all()}

        points_q = await session.execute(
            select(
                Point.tg_user_id,
                func.coalesce(func.sum(Point.points), 0).label("total"),
                func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
                func.coalesce(func.sum(case((Point.points > 0, 1), else_=0)), 0).label("hits"),
            )
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(
                Point.tg_user_id.in_(user_ids),
                Match.tournament_id == rpl.id,
                Match.round_number >= stage.round_min,
                Match.round_number <= stage.round_max,
            )
            .group_by(Point.tg_user_id)
        )
        points_map = {
            int(uid): {
                "total": int(total or 0),
                "exact": int(exact or 0),
                "diff": int(diff or 0),
                "outcome": int(outcome or 0),
                "hits": int(hits or 0),
            }
            for uid, total, exact, diff, outcome, hits in points_q.all()
        }

        rows: list[dict] = []
        for uid, lp_name, ut_name, ut_bonus, u_name, username, full_name in participant_rows:
            tgid = int(uid)
            pred_total, last_pred_at = pred_map.get(tgid, (0, None))
            pts = points_map.get(tgid, {})
            total = int(pts.get("total", 0)) + int(ut_bonus or 0)
            exact = int(pts.get("exact", 0))
            diff = int(pts.get("diff", 0))
            outcome = int(pts.get("outcome", 0))
            hits = int(pts.get("hits", 0))
            hit_rate = round((hits * 100.0 / pred_total), 2) if pred_total > 0 else 0.0
            pred_started = int(pred_started_map.get(tgid, 0))
            missed_matches = max(0, int(started_matches_total) - pred_started)
            rows.append(
                {
                    "tg_user_id": tgid,
                    "name": _resolve_name(lp_name, ut_name, u_name, username, full_name, tgid),
                    "total": total,
                    "exact": exact,
                    "diff": diff,
                    "outcome": outcome,
                    "pred_total": pred_total,
                    "hits": hits,
                    "hit_rate": hit_rate,
                    "missed_matches": missed_matches,
                    "last_pred_at": last_pred_at,
                }
            )

        def _sort_key(r: dict):
            last_at = r.get("last_pred_at")
            if not isinstance(last_at, datetime):
                last_at = datetime.max
            return (
                -int(r.get("total", 0)),
                -int(r.get("exact", 0)),
                -int(r.get("diff", 0)),
                -int(r.get("outcome", 0)),
                -float(r.get("hit_rate", 0.0)),
                int(r.get("pred_total", 0)),
                last_at,
                str(r.get("name", "")).lower(),
            )

        rows.sort(key=_sort_key)
        for i, row in enumerate(rows, start=1):
            row["place"] = i

        meta = LeagueTableMeta(
            season_name=str(season.name),
            stage_name=str(stage.name),
            stage_round_min=int(stage.round_min),
            stage_round_max=int(stage.round_max),
            league_code=str(selected_league.code),
            league_name=str(selected_league.name),
            participants=len(rows),
        )
        return rows, meta
