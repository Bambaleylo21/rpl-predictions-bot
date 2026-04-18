from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import asyncio
from typing import Any
from urllib.parse import parse_qs

from aiohttp import web
from sqlalchemy import case, func, select

from app.db import SessionLocal
from app.models import League, LeagueParticipant, Point, Prediction, Stage, User

logger = logging.getLogger(__name__)


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
