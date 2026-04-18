from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import parse_qs

from aiohttp import web
from sqlalchemy import case, func, select

from app.db import SessionLocal
from app.league_table import build_active_stage_league_table
from app.models import League, LeagueParticipant, Match, Point, Prediction, Stage, Tournament, User, UserTournament

logger = logging.getLogger(__name__)
MSK_TZ = timezone(timedelta(hours=3))


def _parse_init_data(init_data: str) -> dict[str, Any]:
    """
    Lightweight parser for Telegram WebApp initData.
    NOTE: signature validation will be added in the next step.
    """
    if not init_data:
        return {}
    parsed = parse_qs(init_data, keep_blank_values=True)
    out: dict[str, Any] = {}
    for key, values in parsed.items():
        out[key] = values[-1] if values else ""
    if "user" in out and isinstance(out["user"], str):
        try:
            out["user"] = json.loads(out["user"])
        except Exception:
            pass
    return out


def _extract_init_data(request: web.Request) -> str:
    # 1) Custom header from frontend fetch
    h = request.headers.get("X-Telegram-Init-Data", "").strip()
    if h:
        return h
    # 2) Bearer-style header
    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("tma "):
        return auth[4:].strip()
    # 3) Query param for manual testing
    q = request.query.get("init_data", "").strip()
    return q


def _verify_init_data_signature(init_data: str, bot_token: str) -> tuple[bool, str]:
    """
    Telegram Mini App signature check.
    Docs logic:
      secret = HMAC_SHA256(key="WebAppData", msg=bot_token)
      hash = HMAC_SHA256(key=secret, msg=data_check_string)
    """
    if not init_data:
        return False, "init_data is empty"
    if not bot_token:
        return False, "BOT_TOKEN is not configured"

    pairs = parse_qs(init_data, keep_blank_values=True)
    hash_values = pairs.get("hash", [])
    if not hash_values or not hash_values[-1]:
        return False, "hash is missing in init_data"
    received_hash = hash_values[-1]

    check_items: list[str] = []
    for key in sorted(pairs.keys()):
        if key == "hash":
            continue
        values = pairs.get(key, [])
        value = values[-1] if values else ""
        check_items.append(f"{key}={value}")
    data_check_string = "\n".join(check_items)

    secret_key = hmac.new(
        key=b"WebAppData",
        msg=bot_token.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    computed_hash = hmac.new(
        key=secret_key,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        return False, "hash mismatch"
    return True, "ok"


def _json_unauthorized(reason: str) -> web.Response:
    return web.json_response(
        {
            "ok": False,
            "error": "unauthorized",
            "reason": reason,
            "signature_checked": True,
        },
        status=401,
    )


def _extract_verified_user(request: web.Request) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, web.Response]:
    init_data_raw = _extract_init_data(request)
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    signature_ok, signature_reason = _verify_init_data_signature(init_data_raw, bot_token)
    if not signature_ok:
        return None, _json_unauthorized(signature_reason)

    payload = _parse_init_data(init_data_raw)
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
    return payload, user if isinstance(user, dict) else {}


async def health(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "miniapp-api"})


async def me(request: web.Request) -> web.Response:
    auth_result = _extract_verified_user(request)
    if auth_result[0] is None:
        return auth_result[1]
    payload, user = auth_result

    tg_user_id = user.get("id") if isinstance(user, dict) else None
    username = user.get("username") if isinstance(user, dict) else None
    first_name = user.get("first_name") if isinstance(user, dict) else None

    return web.json_response(
        {
            "ok": True,
            "in_telegram": bool(tg_user_id),
            "tg_user_id": tg_user_id,
            "username": username,
            "first_name": first_name,
            "auth_date": payload.get("auth_date"),
            "signature_checked": True,
            "trusted": True,
            "note": "Telegram initData signature is valid.",
        }
    )


async def profile(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None

        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        async with SessionLocal() as session:
            user_row = (
                await session.execute(
                    select(User).where(User.tg_user_id == int(tg_user_id))
                )
            ).scalar_one_or_none()

            if user_row is None:
                return web.json_response(
                    {
                        "ok": True,
                        "trusted": True,
                        "joined": False,
                        "tg_user_id": int(tg_user_id),
                        "display_name": user.get("first_name") or user.get("username") or f"id:{tg_user_id}",
                        "message": "Пользователь есть в Telegram, но ещё не вступил в турнир бота.",
                    }
                )

            stats_row = (
                await session.execute(
                    select(
                        func.count(Prediction.id).label("predictions_count"),
                        func.coalesce(func.sum(Point.points), 0).label("total_points"),
                        func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact_hits"),
                        func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff_hits"),
                        func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome_hits"),
                    )
                    .select_from(User)
                    .outerjoin(Prediction, Prediction.tg_user_id == User.tg_user_id)
                    .outerjoin(Point, Point.tg_user_id == User.tg_user_id)
                    .where(User.tg_user_id == int(tg_user_id))
                )
            ).one()

            league_row = (
                await session.execute(
                    select(
                        League.name.label("league_name"),
                        Stage.name.label("stage_name"),
                        Stage.round_min.label("round_min"),
                        Stage.round_max.label("round_max"),
                    )
                    .select_from(LeagueParticipant)
                    .join(League, League.id == LeagueParticipant.league_id)
                    .join(Stage, Stage.id == LeagueParticipant.stage_id)
                    .where(
                        LeagueParticipant.tg_user_id == int(tg_user_id),
                        LeagueParticipant.is_active == 1,
                        Stage.is_active == 1,
                    )
                    .order_by(LeagueParticipant.id.desc())
                    .limit(1)
                )
            ).one_or_none()

            display_name = (
                user_row.display_name
                or user_row.full_name
                or (f"@{user_row.username}" if user_row.username else None)
                or f"id:{tg_user_id}"
            )

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "joined": True,
                    "tg_user_id": int(tg_user_id),
                    "display_name": display_name,
                    "username": user_row.username,
                    "predictions_count": int(stats_row.predictions_count or 0),
                    "total_points": int(stats_row.total_points or 0),
                    "exact_hits": int(stats_row.exact_hits or 0),
                    "diff_hits": int(stats_row.diff_hits or 0),
                    "outcome_hits": int(stats_row.outcome_hits or 0),
                    "league_name": league_row.league_name if league_row else None,
                    "stage_name": league_row.stage_name if league_row else None,
                    "stage_round_min": int(league_row.round_min) if league_row and league_row.round_min is not None else None,
                    "stage_round_max": int(league_row.round_max) if league_row and league_row.round_max is not None else None,
                }
            )
    except Exception as e:
        logger.exception("miniapp profile error")
        return web.json_response(
            {
                "ok": False,
                "error": "profile_query_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


def _point_category_emoji(category: str | None, points: int | None) -> str:
    cat = (category or "").strip().lower()
    if cat == "exact":
        return "🎯"
    if cat == "diff":
        return "📏"
    if cat == "outcome":
        return "✅"
    if int(points or 0) <= 0:
        return "❌"
    return "✅"


async def predictions_current(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        async with SessionLocal() as session:
            tournament = (
                await session.execute(
                    select(Tournament).where(Tournament.code == "RPL").limit(1)
                )
            ).scalar_one_or_none()
            if tournament is None:
                return web.json_response({"ok": False, "error": "tournament_not_found"}, status=404)

            round_min = int(tournament.round_min)
            round_max = int(tournament.round_max)

            stage_row = (
                await session.execute(
                    select(
                        Stage.round_min.label("round_min"),
                        Stage.round_max.label("round_max"),
                        Stage.name.label("stage_name"),
                    )
                    .select_from(LeagueParticipant)
                    .join(Stage, Stage.id == LeagueParticipant.stage_id)
                    .where(
                        LeagueParticipant.tg_user_id == int(tg_user_id),
                        LeagueParticipant.is_active == 1,
                        Stage.is_active == 1,
                    )
                    .order_by(LeagueParticipant.id.desc())
                    .limit(1)
                )
            ).one_or_none()
            if stage_row is not None:
                round_min = int(stage_row.round_min)
                round_max = int(stage_row.round_max)

            rounds_rows = (
                await session.execute(
                    select(
                        Match.round_number.label("round_number"),
                        func.sum(case((Match.home_score.is_(None), 1), else_=0)).label("open_cnt"),
                    )
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number >= int(round_min),
                        Match.round_number <= int(round_max),
                    )
                    .group_by(Match.round_number)
                    .order_by(Match.round_number.asc())
                )
            ).all()

            if not rounds_rows:
                current_round = int(round_min)
            else:
                current_round = int(rounds_rows[-1].round_number)
                for r in rounds_rows:
                    if int(r.open_cnt or 0) > 0:
                        current_round = int(r.round_number)
                        break

            matches = (
                await session.execute(
                    select(Match)
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number == int(current_round),
                    )
                    .order_by(Match.kickoff_time.asc(), Match.id.asc())
                )
            ).scalars().all()

            match_ids = [int(m.id) for m in matches]
            preds_map: dict[int, Prediction] = {}
            points_map: dict[int, Point] = {}

            if match_ids:
                preds = (
                    await session.execute(
                        select(Prediction).where(
                            Prediction.tg_user_id == int(tg_user_id),
                            Prediction.match_id.in_(match_ids),
                        )
                    )
                ).scalars().all()
                preds_map = {int(p.match_id): p for p in preds}

                points = (
                    await session.execute(
                        select(Point).where(
                            Point.tg_user_id == int(tg_user_id),
                            Point.match_id.in_(match_ids),
                        )
                    )
                ).scalars().all()
                points_map = {int(p.match_id): p for p in points}

            items: list[dict[str, Any]] = []
            total_points = 0
            for m in matches:
                pred = preds_map.get(int(m.id))
                pt = points_map.get(int(m.id))
                points_val = int(pt.points or 0) if pt is not None else None
                if points_val is not None:
                    total_points += points_val

                is_closed = m.home_score is not None and m.away_score is not None
                items.append(
                    {
                        "match_id": int(m.id),
                        "home_team": m.home_team,
                        "away_team": m.away_team,
                        "kickoff": m.kickoff_time.strftime("%d.%m %H:%M"),
                        "status": "closed" if is_closed else "open",
                        "result": f"{m.home_score}:{m.away_score}" if is_closed else None,
                        "prediction": f"{pred.pred_home}:{pred.pred_away}" if pred is not None else None,
                        "points": points_val,
                        "category": pt.category if pt is not None else None,
                        "emoji": _point_category_emoji(pt.category if pt is not None else None, points_val),
                    }
                )

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "tournament": tournament.name,
                    "round_number": int(current_round),
                    "round_min": int(round_min),
                    "round_max": int(round_max),
                    "total_points_closed": int(total_points),
                    "items": items,
                }
            )
    except Exception as e:
        logger.exception("miniapp predictions_current error")
        return web.json_response(
            {
                "ok": False,
                "error": "predictions_query_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


def _now_msk_naive() -> datetime:
    return datetime.now(MSK_TZ).replace(tzinfo=None)


async def predict_current(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        now = _now_msk_naive()

        async with SessionLocal() as session:
            tournament = (
                await session.execute(select(Tournament).where(Tournament.code == "RPL").limit(1))
            ).scalar_one_or_none()
            if tournament is None:
                return web.json_response({"ok": False, "error": "tournament_not_found"}, status=404)

            in_tournament = (
                await session.execute(
                    select(UserTournament.id).where(
                        UserTournament.tg_user_id == int(tg_user_id),
                        UserTournament.tournament_id == int(tournament.id),
                    )
                )
            ).first() is not None
            if not in_tournament:
                return web.json_response(
                    {
                        "ok": True,
                        "trusted": True,
                        "joined": False,
                        "message": "Сначала вступи в турнир в боте, и затем ставь прогнозы в Mini App.",
                    }
                )

            round_min = int(tournament.round_min)
            round_max = int(tournament.round_max)
            stage_row = (
                await session.execute(
                    select(
                        Stage.round_min.label("round_min"),
                        Stage.round_max.label("round_max"),
                    )
                    .select_from(LeagueParticipant)
                    .join(Stage, Stage.id == LeagueParticipant.stage_id)
                    .where(
                        LeagueParticipant.tg_user_id == int(tg_user_id),
                        LeagueParticipant.is_active == 1,
                        Stage.is_active == 1,
                    )
                    .order_by(LeagueParticipant.id.desc())
                    .limit(1)
                )
            ).one_or_none()
            if stage_row is not None:
                round_min = int(stage_row.round_min)
                round_max = int(stage_row.round_max)

            rounds_rows = (
                await session.execute(
                    select(
                        Match.round_number.label("round_number"),
                        func.sum(case((Match.kickoff_time > now, 1), else_=0)).label("open_cnt"),
                    )
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number >= int(round_min),
                        Match.round_number <= int(round_max),
                    )
                    .group_by(Match.round_number)
                    .order_by(Match.round_number.asc())
                )
            ).all()

            current_round = int(round_min)
            for r in rounds_rows:
                if int(r.open_cnt or 0) > 0:
                    current_round = int(r.round_number)
                    break
            if rounds_rows and all(int(r.open_cnt or 0) <= 0 for r in rounds_rows):
                current_round = int(rounds_rows[-1].round_number)

            matches = (
                await session.execute(
                    select(Match)
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number == int(current_round),
                        Match.kickoff_time > now,
                    )
                    .order_by(Match.kickoff_time.asc(), Match.id.asc())
                )
            ).scalars().all()

            match_ids = [int(m.id) for m in matches]
            preds_map: dict[int, Prediction] = {}
            if match_ids:
                preds = (
                    await session.execute(
                        select(Prediction).where(
                            Prediction.tg_user_id == int(tg_user_id),
                            Prediction.match_id.in_(match_ids),
                        )
                    )
                ).scalars().all()
                preds_map = {int(p.match_id): p for p in preds}

            items: list[dict[str, Any]] = []
            for m in matches:
                pred = preds_map.get(int(m.id))
                items.append(
                    {
                        "match_id": int(m.id),
                        "home_team": m.home_team,
                        "away_team": m.away_team,
                        "kickoff": m.kickoff_time.strftime("%d.%m %H:%M"),
                        "prediction": f"{pred.pred_home}:{pred.pred_away}" if pred is not None else None,
                    }
                )

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "joined": True,
                    "tournament": tournament.name,
                    "round_number": int(current_round),
                    "round_min": int(round_min),
                    "round_max": int(round_max),
                    "items": items,
                }
            )
    except Exception as e:
        logger.exception("miniapp predict_current error")
        return web.json_response(
            {
                "ok": False,
                "error": "predict_current_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


async def predict_set(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        body = await request.json()
        match_id = int(body.get("match_id"))
        pred_home = int(body.get("pred_home"))
        pred_away = int(body.get("pred_away"))
        if pred_home < 0 or pred_away < 0:
            return web.json_response({"ok": False, "error": "invalid_score"}, status=400)

        now = _now_msk_naive()
        async with SessionLocal() as session:
            tournament = (
                await session.execute(select(Tournament).where(Tournament.code == "RPL").limit(1))
            ).scalar_one_or_none()
            if tournament is None:
                return web.json_response({"ok": False, "error": "tournament_not_found"}, status=404)

            in_tournament = (
                await session.execute(
                    select(UserTournament.id).where(
                        UserTournament.tg_user_id == int(tg_user_id),
                        UserTournament.tournament_id == int(tournament.id),
                    )
                )
            ).first() is not None
            if not in_tournament:
                return web.json_response({"ok": False, "error": "user_not_joined_tournament"}, status=403)

            match = (
                await session.execute(
                    select(Match).where(
                        Match.id == int(match_id),
                        Match.tournament_id == int(tournament.id),
                    )
                )
            ).scalar_one_or_none()
            if match is None:
                return web.json_response({"ok": False, "error": "match_not_found"}, status=404)
            if match.kickoff_time <= now:
                return web.json_response({"ok": False, "error": "match_locked"}, status=409)

            pred = (
                await session.execute(
                    select(Prediction).where(
                        Prediction.tg_user_id == int(tg_user_id),
                        Prediction.match_id == int(match.id),
                    )
                )
            ).scalar_one_or_none()
            if pred is None:
                pred = Prediction(
                    tg_user_id=int(tg_user_id),
                    match_id=int(match.id),
                    pred_home=int(pred_home),
                    pred_away=int(pred_away),
                )
                session.add(pred)
                action = "created"
            else:
                pred.pred_home = int(pred_home)
                pred.pred_away = int(pred_away)
                pred.updated_at = datetime.utcnow()
                action = "updated"

            await session.commit()

            return web.json_response(
                {
                    "ok": True,
                    "action": action,
                    "match_id": int(match.id),
                    "prediction": f"{int(pred_home)}:{int(pred_away)}",
                }
            )
    except (ValueError, TypeError):
        return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)
    except Exception as e:
        logger.exception("miniapp predict_set error")
        return web.json_response(
            {
                "ok": False,
                "error": "predict_set_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


async def table_current(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        rows, meta = await build_active_stage_league_table(int(tg_user_id))
        if meta is None:
            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "has_table": False,
                    "message": "Активная таблица пока не сформирована.",
                }
            )

        user_place = None
        for r in rows:
            if int(r.get("tg_user_id", 0)) == int(tg_user_id):
                user_place = int(r.get("place", 0))
                break

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "has_table": True,
                "season_name": meta.season_name,
                "stage_name": meta.stage_name,
                "stage_round_min": int(meta.stage_round_min),
                "stage_round_max": int(meta.stage_round_max),
                "league_name": meta.league_name,
                "participants": int(meta.participants),
                "user_place": user_place,
                "rows": [
                    {
                        "place": int(r.get("place", 0)),
                        "name": str(r.get("name", "")),
                        "total": int(r.get("total", 0)),
                        "exact": int(r.get("exact", 0)),
                        "diff": int(r.get("diff", 0)),
                        "outcome": int(r.get("outcome", 0)),
                        "pred_total": int(r.get("pred_total", 0)),
                        "hits": int(r.get("hits", 0)),
                        "hit_rate": float(r.get("hit_rate", 0.0)),
                    }
                    for r in rows
                ],
            }
        )
    except Exception as e:
        logger.exception("miniapp table_current error")
        return web.json_response(
            {
                "ok": False,
                "error": "table_query_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Telegram-Init-Data"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


def build_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_route("OPTIONS", "/{tail:.*}", lambda _request: web.Response(status=204))
    app.router.add_get("/healthz", health)
    app.router.add_get("/api/miniapp/me", me)
    app.router.add_get("/api/miniapp/profile", profile)
    app.router.add_get("/api/miniapp/predictions/current", predictions_current)
    app.router.add_get("/api/miniapp/table/current", table_current)
    app.router.add_get("/api/miniapp/predict/current", predict_current)
    app.router.add_post("/api/miniapp/predict/set", predict_set)
    return app


async def run_miniapp_api_forever() -> None:
    host = os.getenv("MINIAPP_API_HOST", "0.0.0.0")
    port_raw = os.getenv("MINIAPP_API_PORT", "8081")
    try:
        port = int(port_raw)
    except ValueError:
        port = 8081

    app = build_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    logger.info("[miniapp-api] started on %s:%s", host, port)

    # Keep task alive.
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_miniapp_api_forever())
