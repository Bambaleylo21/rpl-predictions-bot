from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import or_, select

from app.models import Duel, DuelElo, Match, UserTournament
from app.scoring import calculate_points

ELO_DEFAULT_RATING = 1000
ELO_K_FACTOR = 24


def outcome_sign(home: int, away: int) -> int:
    if home > away:
        return 1
    if home < away:
        return -1
    return 0


def risk_multiplier_bp(challenger_home: int, challenger_away: int, opponent_home: int, opponent_away: int) -> int:
    """
    Multiplier in basis points:
    - same outcome: 100
    - draw-vs-non-draw: 120
    - opposite outcomes: 140
    """
    c = outcome_sign(int(challenger_home), int(challenger_away))
    o = outcome_sign(int(opponent_home), int(opponent_away))
    if c == o:
        return 100
    if c == 0 or o == 0:
        return 120
    return 140


async def ensure_duel_elo(session, tournament_id: int, tg_user_id: int) -> DuelElo:
    row = (
        await session.execute(
            select(DuelElo).where(
                DuelElo.tournament_id == int(tournament_id),
                DuelElo.tg_user_id == int(tg_user_id),
            )
        )
    ).scalar_one_or_none()
    if row is not None:
        return row
    row = DuelElo(
        tournament_id=int(tournament_id),
        tg_user_id=int(tg_user_id),
        rating=ELO_DEFAULT_RATING,
        duels_total=0,
        wins=0,
        losses=0,
        draws=0,
    )
    session.add(row)
    await session.flush()
    return row


async def _is_joined(session, tournament_id: int, tg_user_id: int) -> bool:
    return (
        await session.execute(
            select(UserTournament.id).where(
                UserTournament.tournament_id == int(tournament_id),
                UserTournament.tg_user_id == int(tg_user_id),
            )
        )
    ).first() is not None


async def _display_name_map(session, tournament_id: int) -> dict[int, str]:
    rows = (
        await session.execute(
            select(UserTournament.tg_user_id, UserTournament.display_name).where(
                UserTournament.tournament_id == int(tournament_id)
            )
        )
    ).all()
    out: dict[int, str] = {}
    for tg_user_id, display_name in rows:
        out[int(tg_user_id)] = str(display_name or f"ID {int(tg_user_id)}")
    return out


async def list_duel_match_options(session, tournament_id: int, tg_user_id: int, limit: int = 200) -> list[dict[str, Any]]:
    now = datetime.utcnow()
    matches = (
        await session.execute(
            select(Match)
            .where(
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
                Match.home_score.is_(None),
                Match.away_score.is_(None),
                Match.kickoff_time > now,
            )
            .order_by(Match.kickoff_time.asc(), Match.id.asc())
            .limit(int(limit))
        )
    ).scalars().all()

    rows: list[dict[str, Any]] = []
    for m in matches:
        # 1 match = max 1 active duel per user
        blocked = (
            await session.execute(
                select(Duel.id).where(
                    Duel.match_id == int(m.id),
                    Duel.status.in_(("pending", "accepted")),
                    or_(
                        Duel.challenger_tg_user_id == int(tg_user_id),
                        Duel.opponent_tg_user_id == int(tg_user_id),
                    ),
                )
            )
        ).first() is not None
        rows.append(
            {
                "match_id": int(m.id),
                "home_team": str(m.home_team),
                "away_team": str(m.away_team),
                "group_label": m.group_label,
                "kickoff": m.kickoff_time.strftime("%d.%m %H:%M"),
                "blocked_for_user": bool(blocked),
            }
        )
    return rows


async def create_duel(
    session,
    *,
    tournament_id: int,
    challenger_tg_user_id: int,
    opponent_tg_user_id: int,
    match_id: int,
    challenger_pred_home: int,
    challenger_pred_away: int,
) -> Duel:
    if int(challenger_tg_user_id) == int(opponent_tg_user_id):
        raise ValueError("self_duel_not_allowed")
    if int(challenger_pred_home) < 0 or int(challenger_pred_away) < 0:
        raise ValueError("invalid_score")

    if not await _is_joined(session, int(tournament_id), int(challenger_tg_user_id)):
        raise ValueError("challenger_not_joined")
    if not await _is_joined(session, int(tournament_id), int(opponent_tg_user_id)):
        raise ValueError("opponent_not_joined")

    match = (
        await session.execute(
            select(Match).where(
                Match.id == int(match_id),
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
            )
        )
    ).scalar_one_or_none()
    if match is None:
        raise ValueError("match_not_found")
    if match.home_score is not None or match.away_score is not None:
        raise ValueError("match_already_finished")
    if match.kickoff_time <= datetime.utcnow():
        raise ValueError("match_locked")

    # User can't have 2 active duels on one match
    exists_for_users = (
        await session.execute(
            select(Duel.id).where(
                Duel.match_id == int(match_id),
                Duel.status.in_(("pending", "accepted")),
                or_(
                    Duel.challenger_tg_user_id.in_((int(challenger_tg_user_id), int(opponent_tg_user_id))),
                    Duel.opponent_tg_user_id.in_((int(challenger_tg_user_id), int(opponent_tg_user_id))),
                ),
            )
        )
    ).first()
    if exists_for_users is not None:
        raise ValueError("duel_already_exists_for_match")

    pair_low = min(int(challenger_tg_user_id), int(opponent_tg_user_id))
    pair_high = max(int(challenger_tg_user_id), int(opponent_tg_user_id))

    duel = Duel(
        tournament_id=int(tournament_id),
        match_id=int(match_id),
        challenger_tg_user_id=int(challenger_tg_user_id),
        opponent_tg_user_id=int(opponent_tg_user_id),
        pair_low_tg_user_id=int(pair_low),
        pair_high_tg_user_id=int(pair_high),
        challenger_pred_home=int(challenger_pred_home),
        challenger_pred_away=int(challenger_pred_away),
        status="pending",
    )
    session.add(duel)
    await session.flush()
    return duel


async def respond_duel(
    session,
    *,
    duel_id: int,
    responder_tg_user_id: int,
    accept: bool,
    pred_home: int | None = None,
    pred_away: int | None = None,
) -> Duel:
    duel = (await session.execute(select(Duel).where(Duel.id == int(duel_id)))).scalar_one_or_none()
    if duel is None:
        raise ValueError("duel_not_found")
    if int(duel.opponent_tg_user_id) != int(responder_tg_user_id):
        raise ValueError("not_duel_opponent")
    if str(duel.status) != "pending":
        raise ValueError("duel_not_pending")

    match = (await session.execute(select(Match).where(Match.id == int(duel.match_id)))).scalar_one_or_none()
    if match is None:
        raise ValueError("match_not_found")
    if match.home_score is not None or match.away_score is not None or match.kickoff_time <= datetime.utcnow():
        duel.status = "expired"
        duel.resolved_at = datetime.utcnow()
        duel.responded_at = duel.responded_at or datetime.utcnow()
        await session.flush()
        raise ValueError("duel_expired")

    now = datetime.utcnow()
    duel.responded_at = now
    if not accept:
        duel.status = "declined"
        duel.resolved_at = now
        await session.flush()
        return duel

    if pred_home is None or pred_away is None:
        raise ValueError("opponent_score_required")
    if int(pred_home) < 0 or int(pred_away) < 0:
        raise ValueError("invalid_score")
    if int(pred_home) == int(duel.challenger_pred_home) and int(pred_away) == int(duel.challenger_pred_away):
        raise ValueError("same_prediction_not_allowed")

    duel.opponent_pred_home = int(pred_home)
    duel.opponent_pred_away = int(pred_away)
    duel.risk_multiplier_bp = int(
        risk_multiplier_bp(
            int(duel.challenger_pred_home),
            int(duel.challenger_pred_away),
            int(pred_home),
            int(pred_away),
        )
    )
    duel.status = "accepted"
    await session.flush()
    return duel


async def get_duel_hub(session, *, tournament_id: int, tg_user_id: int) -> dict[str, Any]:
    await expire_stale_duels(session, int(tournament_id))
    name_map = await _display_name_map(session, int(tournament_id))

    elo = await ensure_duel_elo(session, int(tournament_id), int(tg_user_id))

    opponents_raw = (
        await session.execute(
            select(
                UserTournament.tg_user_id,
                UserTournament.display_name,
                DuelElo.rating,
            )
            .outerjoin(
                DuelElo,
                (DuelElo.tournament_id == UserTournament.tournament_id)
                & (DuelElo.tg_user_id == UserTournament.tg_user_id),
            )
            .where(
                UserTournament.tournament_id == int(tournament_id),
                UserTournament.tg_user_id != int(tg_user_id),
            )
            .order_by(UserTournament.display_name.asc().nullslast(), UserTournament.tg_user_id.asc())
        )
    ).all()
    opponents = [
        {
            "tg_user_id": int(uid),
            "display_name": str(dn or f"ID {int(uid)}"),
            "elo_rating": int(rating or ELO_DEFAULT_RATING),
        }
        for uid, dn, rating in opponents_raw
    ]

    match_options = await list_duel_match_options(session, int(tournament_id), int(tg_user_id), limit=200)

    elo_rows = (
        await session.execute(
            select(DuelElo.tg_user_id, DuelElo.rating).where(
                DuelElo.tournament_id == int(tournament_id)
            )
        )
    ).all()
    elo_map: dict[int, int] = {int(uid): int(rating or ELO_DEFAULT_RATING) for uid, rating in elo_rows}

    active_rows = (
        await session.execute(
            select(Duel, Match)
            .join(Match, Match.id == Duel.match_id)
            .where(
                Duel.tournament_id == int(tournament_id),
                Duel.status.in_(("pending", "accepted")),
                or_(
                    Duel.challenger_tg_user_id == int(tg_user_id),
                    Duel.opponent_tg_user_id == int(tg_user_id),
                ),
            )
            .order_by(Duel.created_at.desc(), Duel.id.desc())
        )
    ).all()

    finished_rows = (
        await session.execute(
            select(Duel, Match)
            .join(Match, Match.id == Duel.match_id)
            .where(
                Duel.tournament_id == int(tournament_id),
                Duel.status.in_(("finished", "declined", "expired")),
                or_(
                    Duel.challenger_tg_user_id == int(tg_user_id),
                    Duel.opponent_tg_user_id == int(tg_user_id),
                ),
            )
            .order_by(Duel.resolved_at.desc().nullslast(), Duel.id.desc())
            .limit(30)
        )
    ).all()

    def _duel_item(duel: Duel, match: Match) -> dict[str, Any]:
        return {
            "duel_id": int(duel.id),
            "status": str(duel.status),
            "match_id": int(match.id),
            "home_team": str(match.home_team),
            "away_team": str(match.away_team),
            "group_label": match.group_label,
            "kickoff": match.kickoff_time.strftime("%d.%m %H:%M"),
            "result": (
                f"{int(match.home_score)}:{int(match.away_score)}"
                if match.home_score is not None and match.away_score is not None
                else None
            ),
            "challenger_tg_user_id": int(duel.challenger_tg_user_id),
            "challenger_name": name_map.get(int(duel.challenger_tg_user_id), str(duel.challenger_tg_user_id)),
            "challenger_pred": f"{int(duel.challenger_pred_home)}:{int(duel.challenger_pred_away)}",
            "opponent_tg_user_id": int(duel.opponent_tg_user_id),
            "opponent_name": name_map.get(int(duel.opponent_tg_user_id), str(duel.opponent_tg_user_id)),
            "opponent_pred": (
                f"{int(duel.opponent_pred_home)}:{int(duel.opponent_pred_away)}"
                if duel.opponent_pred_home is not None and duel.opponent_pred_away is not None
                else None
            ),
            "risk_multiplier_bp": int(duel.risk_multiplier_bp or 100),
            "outcome": duel.outcome,
            "winner_tg_user_id": int(duel.winner_tg_user_id) if duel.winner_tg_user_id is not None else None,
            "elo_delta_challenger": int(duel.elo_delta_challenger or 0),
            "elo_delta_opponent": int(duel.elo_delta_opponent or 0),
            "challenger_rating": int(elo_map.get(int(duel.challenger_tg_user_id), ELO_DEFAULT_RATING)),
            "opponent_rating": int(elo_map.get(int(duel.opponent_tg_user_id), ELO_DEFAULT_RATING)),
        }

    return {
        "elo": {
            "rating": int(elo.rating or ELO_DEFAULT_RATING),
            "duels_total": int(elo.duels_total or 0),
            "wins": int(elo.wins or 0),
            "losses": int(elo.losses or 0),
            "draws": int(elo.draws or 0),
        },
        "match_options": match_options,
        "opponents": opponents,
        "active": [_duel_item(duel, match) for duel, match in active_rows],
        "finished": [_duel_item(duel, match) for duel, match in finished_rows],
    }


def _elo_delta(rating_self: int, rating_other: int, score_self: float, multiplier_bp: int) -> int:
    expected = 1.0 / (1.0 + 10 ** ((float(rating_other) - float(rating_self)) / 400.0))
    raw = ELO_K_FACTOR * (float(score_self) - expected)
    value = raw * (float(multiplier_bp) / 100.0)
    return int(round(value))


async def expire_stale_duels(session, tournament_id: int) -> int:
    now = datetime.utcnow()
    rows = (
        await session.execute(
            select(Duel, Match)
            .join(Match, Match.id == Duel.match_id)
            .where(
                Duel.tournament_id == int(tournament_id),
                Duel.status == "pending",
                or_(
                    Match.kickoff_time <= now,
                    Match.home_score.is_not(None),
                    Match.away_score.is_not(None),
                ),
            )
        )
    ).all()

    updates = 0
    for duel, _match in rows:
        duel.status = "expired"
        duel.resolved_at = now
        duel.responded_at = duel.responded_at or now
        updates += 1
    return updates


async def finalize_duels_for_match(session, match_id: int) -> list[dict[str, Any]]:
    """
    Finalize accepted duels for match and update Elo.
    Returns list of duel result payloads for notifications.
    """
    now = datetime.utcnow()
    match = (await session.execute(select(Match).where(Match.id == int(match_id)))).scalar_one_or_none()
    if match is None:
        return []

    # Expire pending duels for this match.
    pending_rows = (
        await session.execute(
            select(Duel).where(
                Duel.match_id == int(match_id),
                Duel.status == "pending",
            )
        )
    ).scalars().all()
    for duel in pending_rows:
        duel.status = "expired"
        duel.resolved_at = now
        duel.responded_at = duel.responded_at or now

    if match.home_score is None or match.away_score is None:
        return []

    duels = (
        await session.execute(
            select(Duel).where(
                Duel.match_id == int(match_id),
                Duel.status == "accepted",
            )
        )
    ).scalars().all()

    result_events: list[dict[str, Any]] = []
    for duel in duels:
        if duel.opponent_pred_home is None or duel.opponent_pred_away is None:
            duel.status = "expired"
            duel.resolved_at = now
            continue

        challenger_calc = calculate_points(
            pred_home=int(duel.challenger_pred_home),
            pred_away=int(duel.challenger_pred_away),
            real_home=int(match.home_score),
            real_away=int(match.away_score),
        )
        opponent_calc = calculate_points(
            pred_home=int(duel.opponent_pred_home),
            pred_away=int(duel.opponent_pred_away),
            real_home=int(match.home_score),
            real_away=int(match.away_score),
        )

        ch_pts = int(challenger_calc.points)
        op_pts = int(opponent_calc.points)
        if ch_pts > op_pts:
            outcome = "challenger_win"
            s_ch, s_op = 1.0, 0.0
            winner_id = int(duel.challenger_tg_user_id)
        elif op_pts > ch_pts:
            outcome = "opponent_win"
            s_ch, s_op = 0.0, 1.0
            winner_id = int(duel.opponent_tg_user_id)
        else:
            outcome = "draw"
            s_ch, s_op = 0.5, 0.5
            winner_id = None

        mult_bp = risk_multiplier_bp(
            int(duel.challenger_pred_home),
            int(duel.challenger_pred_away),
            int(duel.opponent_pred_home),
            int(duel.opponent_pred_away),
        )

        ch_elo = await ensure_duel_elo(session, int(duel.tournament_id), int(duel.challenger_tg_user_id))
        op_elo = await ensure_duel_elo(session, int(duel.tournament_id), int(duel.opponent_tg_user_id))

        old_ch = int(ch_elo.rating)
        old_op = int(op_elo.rating)

        d_ch = _elo_delta(old_ch, old_op, s_ch, mult_bp)
        d_op = _elo_delta(old_op, old_ch, s_op, mult_bp)

        # keep symmetric in integer space
        if (d_ch + d_op) != 0:
            d_op = -d_ch

        ch_elo.rating = int(old_ch + d_ch)
        op_elo.rating = int(old_op + d_op)
        ch_elo.duels_total = int(ch_elo.duels_total or 0) + 1
        op_elo.duels_total = int(op_elo.duels_total or 0) + 1
        ch_elo.updated_at = now
        op_elo.updated_at = now

        if outcome == "challenger_win":
            ch_elo.wins = int(ch_elo.wins or 0) + 1
            op_elo.losses = int(op_elo.losses or 0) + 1
        elif outcome == "opponent_win":
            op_elo.wins = int(op_elo.wins or 0) + 1
            ch_elo.losses = int(ch_elo.losses or 0) + 1
        else:
            ch_elo.draws = int(ch_elo.draws or 0) + 1
            op_elo.draws = int(op_elo.draws or 0) + 1

        duel.status = "finished"
        duel.outcome = outcome
        duel.winner_tg_user_id = winner_id
        duel.risk_multiplier_bp = int(mult_bp)
        duel.elo_delta_challenger = int(d_ch)
        duel.elo_delta_opponent = int(d_op)
        duel.resolved_at = now

        result_events.append(
            {
                "duel_id": int(duel.id),
                "match_id": int(match.id),
                "tournament_id": int(duel.tournament_id),
                "challenger_tg_user_id": int(duel.challenger_tg_user_id),
                "opponent_tg_user_id": int(duel.opponent_tg_user_id),
                "outcome": outcome,
                "winner_tg_user_id": winner_id,
                "match_result": f"{int(match.home_score)}:{int(match.away_score)}",
                "challenger_points": ch_pts,
                "opponent_points": op_pts,
                "challenger_new_elo": int(ch_elo.rating),
                "opponent_new_elo": int(op_elo.rating),
                "challenger_delta": int(d_ch),
                "opponent_delta": int(d_op),
            }
        )

    return result_events
