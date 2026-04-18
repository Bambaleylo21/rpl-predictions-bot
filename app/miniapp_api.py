from __future__ import annotations

import json
import logging
import os
import asyncio
from typing import Any
from urllib.parse import parse_qs

from aiohttp import web

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


async def health(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "miniapp-api"})


async def me(request: web.Request) -> web.Response:
    init_data_raw = _extract_init_data(request)
    payload = _parse_init_data(init_data_raw)
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}

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
            "signature_checked": False,
            "note": "Debug endpoint. Signature validation will be added next.",
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
