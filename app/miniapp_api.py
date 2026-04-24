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
from aiogram import Bot
from sqlalchemy import case, false, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.config import load_admin_ids, load_config
from app.db import SessionLocal, init_db
from app.duel_notify import send_duel_accepted_push, send_duel_finished_pushes, send_new_duel_challenge_push
from app.display import display_round_name
from app.duels import create_duel, finalize_duels_for_match, get_duel_hub, respond_duel
from app.league_table import build_active_stage_league_table
from app.models import Duel, DuelElo, League, LeagueParticipant, LongtermPrediction, Match, Point, Prediction, Setting, Stage, Tournament, User, UserTournament
from app.scoring import calculate_points

logger = logging.getLogger(__name__)
MSK_TZ = timezone(timedelta(hours=3))
DEFAULT_TOURNAMENT_CODE = "RPL"
WC_TOURNAMENT_CODE = "WC2026"
TOURNAMENT_SELECTED_KEY_PREFIX = "TOURNAMENT_SELECTED_U"
LONGTERM_TYPES = ("winner", "scorer")
ADMIN_IDS = load_admin_ids()
_NOTIFY_BOT: Bot | None = None
WC_TOP_SCORER_OPTIONS = [
    "Килиан Мбаппе",
    "Эрлинг Холанд",
    "Харри Кейн",
    "Джуд Беллингем",
    "Лионель Месси",
    "Лаутаро Мартинес",
    "Винисиус Жуниор",
    "Родриго",
    "Рафинья",
    "Эндрик",
    "Ламин Ямаль",
    "Альваро Мората",
    "Оярсабаль",
    "Роберт Левандовски",
    "Криштиану Роналду",
    "Бруну Фернандеш",
    "Рафаэл Леау",
    "Виктор Дьёкереш",
    "Антуан Гризманн",
    "Килиан Тюрам",
    "Рандаль Коло Муани",
    "Коуди Гакпо",
    "Мемфис Депай",
    "Расмус Хёйлунн",
    "Александер Исак",
    "Джонатан Дэвид",
    "Сон Хын Мин",
    "Такефуса Кубо",
    "Джулиан Альварес",
    "Флориан Вирц",
]

LEGACY_TROPHIES = [
    {"season": "2025/26", "title": "Лига Кайратовна", "format": "Группа ЛЧ", "place": 1, "username": "ImRus32"},
    {"season": "2025/26", "title": "Лига Кайратовна", "format": "Группа ЛЧ", "place": 2, "username": "kuznetsoff32"},
    {"season": "2025/26", "title": "Лига Кайратовна", "format": "Группа ЛЧ", "place": 3, "username": "artgorlin"},
    {"season": "2025", "title": "РПЛ - говно!", "format": "РПЛ (1-18 тур) · Высшая лига", "place": 1, "username": "ImRus32"},
    {"season": "2025", "title": "РПЛ - говно!", "format": "РПЛ (1-18 тур) · Низшая лига", "place": 1, "username": "kuznetsoff32"},
    {"season": "2025", "title": 'ТП "Ради денег"', "format": "Клубный ЧМ", "place": 1, "username": "kuznetsoff32"},
    {"season": "2025", "title": "Выносити Сити", "format": "ЛЧ плейофф", "place": 1, "username": "artgorlin"},
    {"season": "2025", "title": "Достоевский кап", "format": "РПЛ (19-30 тур) · Высшая лига", "place": 1, "username": "perepelkinSlava"},
    {"season": "2025", "title": "Достоевский кап", "format": "РПЛ (19-30 тур) · Низшая лига", "place": 1, "username": "ImRus32"},
    {"season": "2024", "title": "Под пивко", "format": "РПЛ (1-18 тур) · Высшая лига", "place": 1, "username": "perepelkinSlava"},
    {"season": "2024", "title": "Под пивко", "format": "РПЛ (1-18 тур) · Низшая лига", "place": 1, "username": "Efanchik"},
    {"season": "2024", "title": "Чемпионат Гейропы", "format": "ЧЕ 2022", "place": 1, "username": "nyaka19"},
    {"season": "2024", "title": "Последний турнир", "format": "РПЛ (19-30 тур)", "place": 1, "username": "ImRus32"},
    {"season": "2023/24", "title": "Лига Шампионьонов", "format": "ЛЧ", "place": 1, "username": "ImRus32"},
    {"season": "2023", "title": "Турнир имени Саши Доронина", "format": "РПЛ (19-30 тур)", "place": 1, "username": "THEALEX32"},
    {"season": "2023", "title": "Всё будет хорошо", "format": "РПЛ (1-18 тур)", "place": 1, "username": "marykozhanova"},
    {"season": "2022", "title": "КОТар прогноз", "format": "ЧМ 2022", "place": 1, "username": "THEALEX32"},
    {"season": "2022", "title": "eFan ID cup", "format": "РПЛ (18-30 тур)", "place": 1, "username": "THEALEX32"},
    {"season": "2022", "title": "Кожановый мяч", "format": "РПЛ (1-17 тур)", "place": 1, "username": "marykozhanova"},
    {"season": "2021", "title": "Чемпионат трёх матчей сборной России", "format": "ЧЕ 2021", "place": 1, "username": "THEALEX32"},
    {"season": "2021", "title": "Турнир Открытая Книга", "format": "РПЛ", "place": 1, "username": "artgorlin"},
    {"season": "2019", "title": "кубак памити арфаграфии Ивгения Реудского", "format": "РПЛ 1 часть", "place": 1, "username": "Efanchik"},
    {"season": "2018", "title": "No Criminality Cup", "format": "РПЛ 2 часть", "place": 1, "username": "perepelkinSlava"},
    {"season": "2017", "title": "Кубок Небесной Шлюхи", "format": "РПЛ 1 часть", "place": 1, "username": "THEALEX32"},
    {"season": "2017", "title": "Petooshock Trophy", "format": "РПЛ 2 часть", "place": 1, "username": "tsykun"},
    {"season": "2016", "title": "Усы Черчесова", "format": "РПЛ 1 часть", "place": 1, "username": "THEALEX32"},
    {"season": "2014", "title": "Ярошик Cup", "format": "РПЛ 2 часть", "place": 1, "username": "THEALEX32"},
    {"season": "2012", "title": "Чаша Бышовца", "format": "РПЛ 11-20 тур", "place": 1, "username": "Maks_032"},
    {"season": "2012", "title": "Кубок Бубнова", "format": "РПЛ 1-10 тур", "place": 1, "username": "Maks_032"},
]


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


def _get_notify_bot() -> Bot:
    global _NOTIFY_BOT
    if _NOTIFY_BOT is None:
        _NOTIFY_BOT = Bot(token=load_config())
    return _NOTIFY_BOT


def _selected_tournament_key(tg_user_id: int) -> str:
    return f"{TOURNAMENT_SELECTED_KEY_PREFIX}{int(tg_user_id)}"


def _normalize_username(value: str | None) -> str:
    s = (value or "").strip().lower()
    while s.startswith("@"):
        s = s[1:]
    return s


def _legacy_trophies_for_username(username: str | None) -> list[dict[str, Any]]:
    uname = _normalize_username(username)
    if not uname:
        return []
    rows: list[dict[str, Any]] = []
    for item in LEGACY_TROPHIES:
        if _normalize_username(str(item.get("username") or "")) != uname:
            continue
        rows.append(
            {
                "season": str(item.get("season") or ""),
                "title": str(item.get("title") or ""),
                "format": str(item.get("format") or ""),
                "place": int(item.get("place") or 0),
            }
        )
    return rows


async def _get_setting(session, key: str) -> str | None:
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    return row.value if row is not None else None


async def _set_setting(session, key: str, value: str) -> None:
    bind = session.get_bind()
    dialect_name = bind.dialect.name if bind is not None else ""

    if dialect_name == "postgresql":
        stmt = (
            pg_insert(Setting)
            .values(key=key, value=value)
            .on_conflict_do_update(
                index_elements=[Setting.key],
                set_={"value": value},
            )
        )
        await session.execute(stmt)
        return

    if dialect_name == "sqlite":
        stmt = (
            sqlite_insert(Setting)
            .values(key=key, value=value)
            .on_conflict_do_update(
                index_elements=[Setting.key],
                set_={"value": value},
            )
        )
        await session.execute(stmt)
        return

    # Fallback for other DBs.
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is None:
        session.add(Setting(key=key, value=value))
    else:
        row.value = value


async def _ensure_tournament(
    session,
    code: str,
    name: str,
    round_min: int,
    round_max: int,
) -> Tournament:
    q = await session.execute(select(Tournament).where(Tournament.code == code).limit(1))
    row = q.scalar_one_or_none()
    if row is None:
        row = Tournament(
            code=code,
            name=name,
            round_min=round_min,
            round_max=round_max,
            is_active=1,
        )
        session.add(row)
        await session.flush()
    return row


async def _ensure_default_tournaments(session) -> None:
    await _ensure_tournament(
        session=session,
        code=DEFAULT_TOURNAMENT_CODE,
        name="РПЛ",
        round_min=1,
        round_max=30,
    )
    await _ensure_tournament(
        session=session,
        code=WC_TOURNAMENT_CODE,
        name="ЧМ 2026",
        round_min=1,
        round_max=64,
    )


async def _get_active_tournaments(session) -> list[Tournament]:
    await _ensure_default_tournaments(session)
    q = await session.execute(
        select(Tournament)
        .where(Tournament.is_active == 1)
        .order_by(case((Tournament.code == DEFAULT_TOURNAMENT_CODE, 0), else_=1), Tournament.code.asc())
    )
    return list(q.scalars().all())


async def _resolve_tournament(session, tg_user_id: int, requested_code: str | None = None) -> Tournament:
    tournaments = await _get_active_tournaments(session)
    by_code = {(t.code or "").strip().upper(): t for t in tournaments}

    req = (requested_code or "").strip().upper()
    if req and req in by_code:
        await _set_setting(session, _selected_tournament_key(tg_user_id), req)
        return by_code[req]

    selected = (await _get_setting(session, _selected_tournament_key(tg_user_id)) or "").strip().upper()
    if selected and selected in by_code:
        return by_code[selected]

    fallback = by_code.get(DEFAULT_TOURNAMENT_CODE) or (tournaments[0] if tournaments else None)
    if fallback is None:
        fallback = await _ensure_tournament(
            session=session,
            code=DEFAULT_TOURNAMENT_CODE,
            name="РПЛ",
            round_min=1,
            round_max=30,
        )
    await _set_setting(session, _selected_tournament_key(tg_user_id), fallback.code)
    return fallback


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


def _extract_verified_admin_user(request: web.Request) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, web.Response]:
    auth_result = _extract_verified_user(request)
    if auth_result[0] is None:
        return auth_result
    payload, user = auth_result
    tg_user_id = user.get("id") if isinstance(user, dict) else None
    if not tg_user_id or int(tg_user_id) not in ADMIN_IDS:
        return None, web.json_response(
            {
                "ok": False,
                "error": "forbidden",
                "reason": "admin_required",
                "signature_checked": True,
            },
            status=403,
        )
    return payload, user


async def _upsert_user_from_webapp(session, user: dict[str, Any]) -> User | None:
    tg_user_id = user.get("id") if isinstance(user, dict) else None
    if not tg_user_id:
        return None
    username = user.get("username") if isinstance(user, dict) else None
    first_name = user.get("first_name") if isinstance(user, dict) else None
    last_name = user.get("last_name") if isinstance(user, dict) else None
    photo_url = user.get("photo_url") if isinstance(user, dict) else None
    full_name = f"{first_name or ''} {last_name or ''}".strip() or None

    row = (await session.execute(select(User).where(User.tg_user_id == int(tg_user_id)))).scalar_one_or_none()
    if row is None:
        row = User(
            tg_user_id=int(tg_user_id),
            username=username,
            full_name=full_name,
            photo_url=(str(photo_url).strip() or None) if photo_url else None,
        )
        session.add(row)
        await session.flush()
        return row

    row.username = username
    row.full_name = full_name
    if photo_url:
        row.photo_url = str(photo_url).strip() or row.photo_url
    return row


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

    selected_tournament_code = None
    selected_tournament_name = None
    if tg_user_id:
        async with SessionLocal() as session:
            await _upsert_user_from_webapp(session, user if isinstance(user, dict) else {})
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()
            selected_tournament_code = tournament.code
            selected_tournament_name = tournament.name

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
            "selected_tournament_code": selected_tournament_code,
            "selected_tournament_name": selected_tournament_name,
            "is_admin": bool(tg_user_id and int(tg_user_id) in ADMIN_IDS),
            "note": "Telegram initData signature is valid.",
        }
    )


def _parse_score_text(raw: str | None) -> tuple[int, int] | None:
    s = str(raw or "").strip().replace("-", ":")
    if ":" not in s:
        return None
    left, right = s.split(":", 1)
    if not left.isdigit() or not right.isdigit():
        return None
    return int(left), int(right)


async def _recalc_points_for_match_in_session(session, match_id: int) -> int:
    updates = 0
    match = (await session.execute(select(Match).where(Match.id == int(match_id)))).scalar_one_or_none()
    if match is None or match.home_score is None or match.away_score is None:
        return 0

    preds = (await session.execute(select(Prediction).where(Prediction.match_id == int(match_id)))).scalars().all()
    for p in preds:
        calc = calculate_points(
            pred_home=int(p.pred_home),
            pred_away=int(p.pred_away),
            real_home=int(match.home_score),
            real_away=int(match.away_score),
        )
        point = (
            await session.execute(
                select(Point).where(
                    Point.match_id == int(match_id),
                    Point.tg_user_id == int(p.tg_user_id),
                )
            )
        ).scalar_one_or_none()
        if point is None:
            session.add(
                Point(
                    match_id=int(match_id),
                    tg_user_id=int(p.tg_user_id),
                    points=int(calc.points),
                    category=str(calc.category),
                )
            )
            updates += 1
        elif int(point.points or 0) != int(calc.points) or str(point.category or "") != str(calc.category):
            point.points = int(calc.points)
            point.category = str(calc.category)
            updates += 1
    return updates


async def admin_rounds(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_admin_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = int(user.get("id"))

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, tg_user_id, requested_code=request.query.get("t"))
            rows = (
                await session.execute(
                    select(
                        Match.round_number,
                        func.count(Match.id).label("total_cnt"),
                        func.sum(case(((Match.home_score.is_(None)) | (Match.away_score.is_(None)), 1), else_=0)).label(
                            "without_result_cnt"
                        ),
                    )
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.is_placeholder == 0,
                    )
                    .group_by(Match.round_number)
                    .order_by(Match.round_number.asc())
                )
            ).all()
            await session.commit()

        rounds = []
        current_round = int(tournament.round_min)
        if rows:
            current_round = int(rows[-1].round_number)
            for r in rows:
                if int(r.without_result_cnt or 0) > 0:
                    current_round = int(r.round_number)
                    break
            for r in rows:
                rounds.append(
                    {
                        "round": int(r.round_number),
                        "round_name": display_round_name(tournament.code, int(r.round_number)),
                        "total": int(r.total_cnt or 0),
                        "without_result": int(r.without_result_cnt or 0),
                    }
                )

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "is_admin": True,
                "tournament_code": tournament.code,
                "tournament_name": tournament.name,
                "current_round": current_round,
                "rounds": rounds,
            }
        )
    except Exception as e:
        logger.exception("miniapp admin_rounds error")
        return web.json_response(
            {"ok": False, "error": "admin_rounds_failed", "reason": str(e), "signature_checked": True},
            status=500,
        )


async def admin_results_current(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_admin_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = int(user.get("id"))

        requested_round_raw = (request.query.get("round") or "").strip()
        requested_round = int(requested_round_raw) if requested_round_raw.isdigit() else None
        mode = (request.query.get("mode") or "open").strip().lower()
        if mode not in ("open", "all"):
            mode = "open"

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, tg_user_id, requested_code=request.query.get("t"))

            rounds_rows = (
                await session.execute(
                    select(
                        Match.round_number,
                        func.sum(case(((Match.home_score.is_(None)) | (Match.away_score.is_(None)), 1), else_=0)).label(
                            "without_result_cnt"
                        ),
                    )
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.is_placeholder == 0,
                    )
                    .group_by(Match.round_number)
                    .order_by(Match.round_number.asc())
                )
            ).all()

            current_round = int(tournament.round_min)
            if rounds_rows:
                current_round = int(rounds_rows[-1].round_number)
                for r in rounds_rows:
                    if int(r.without_result_cnt or 0) > 0:
                        current_round = int(r.round_number)
                        break
            if requested_round is not None:
                current_round = int(requested_round)

            filters = [
                Match.tournament_id == int(tournament.id),
                Match.round_number == int(current_round),
                Match.is_placeholder == 0,
            ]
            if mode == "open":
                filters.append((Match.home_score.is_(None)) | (Match.away_score.is_(None)))

            matches = (
                await session.execute(
                    select(Match)
                    .where(*filters)
                    .order_by(Match.kickoff_time.asc(), Match.id.asc())
                )
            ).scalars().all()

            round_total_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == int(tournament.id),
                    Match.round_number == int(current_round),
                    Match.is_placeholder == 0,
                )
            )
            round_total = int(round_total_q.scalar_one() or 0)
            without_result_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == int(tournament.id),
                    Match.round_number == int(current_round),
                    Match.is_placeholder == 0,
                    ((Match.home_score.is_(None)) | (Match.away_score.is_(None))),
                )
            )
            without_result = int(without_result_q.scalar_one() or 0)

            items = []
            for m in matches:
                preds_cnt_q = await session.execute(select(func.count(Prediction.id)).where(Prediction.match_id == int(m.id)))
                preds_cnt = int(preds_cnt_q.scalar_one() or 0)
                has_result = m.home_score is not None and m.away_score is not None
                items.append(
                    {
                        "match_id": int(m.id),
                        "home_team": str(m.home_team),
                        "away_team": str(m.away_team),
                        "group_label": m.group_label,
                        "kickoff": m.kickoff_time.strftime("%d.%m %H:%M"),
                        "has_result": bool(has_result),
                        "result": f"{int(m.home_score)}:{int(m.away_score)}" if has_result else None,
                        "predictions_count": preds_cnt,
                    }
                )

            await session.commit()

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "is_admin": True,
                "tournament_code": tournament.code,
                "tournament_name": tournament.name,
                "round_number": int(current_round),
                "round_name": display_round_name(tournament.code, int(current_round)),
                "mode": mode,
                "round_total": round_total,
                "without_result": without_result,
                "items": items,
            }
        )
    except Exception as e:
        logger.exception("miniapp admin_results_current error")
        return web.json_response(
            {"ok": False, "error": "admin_results_current_failed", "reason": str(e), "signature_checked": True},
            status=500,
        )


async def admin_result_set(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_admin_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = int(user.get("id"))

        body = await request.json()
        match_id = int(body.get("match_id") or 0)
        score = _parse_score_text(body.get("score"))
        if match_id <= 0 or score is None:
            return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)

        home_score, away_score = score
        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, tg_user_id, requested_code=request.query.get("t"))
            match = (
                await session.execute(
                    select(Match).where(
                        Match.id == int(match_id),
                        Match.tournament_id == int(tournament.id),
                        Match.is_placeholder == 0,
                    )
                )
            ).scalar_one_or_none()
            if match is None:
                return web.json_response({"ok": False, "error": "match_not_found"}, status=404)

            match.home_score = int(home_score)
            match.away_score = int(away_score)
            updates = await _recalc_points_for_match_in_session(session, int(match.id))
            duel_events = await finalize_duels_for_match(session, int(match.id))
            if duel_events:
                try:
                    await send_duel_finished_pushes(_get_notify_bot(), session, events=duel_events)
                except Exception:
                    logger.exception("miniapp admin_result_set duel notify failed")
            await session.commit()

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "is_admin": True,
                "match_id": int(match_id),
                "result": f"{int(home_score)}:{int(away_score)}",
                "updated_points": int(updates),
                "updated_duels": int(len(duel_events)),
            }
        )
    except (ValueError, TypeError):
        return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)
    except Exception as e:
        logger.exception("miniapp admin_result_set error")
        return web.json_response(
            {"ok": False, "error": "admin_result_set_failed", "reason": str(e), "signature_checked": True},
            status=500,
        )


async def admin_recalc_round(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_admin_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = int(user.get("id"))

        body = await request.json()
        round_number = int(body.get("round_number") or 0)
        if round_number <= 0:
            return web.json_response({"ok": False, "error": "round_required"}, status=400)

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, tg_user_id, requested_code=request.query.get("t"))
            matches = (
                await session.execute(
                    select(Match.id).where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number == int(round_number),
                        Match.is_placeholder == 0,
                        Match.home_score.is_not(None),
                        Match.away_score.is_not(None),
                    )
                )
            ).all()

            total_updates = 0
            for (mid,) in matches:
                total_updates += int(await _recalc_points_for_match_in_session(session, int(mid)))
            await session.commit()

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "is_admin": True,
                "round_number": int(round_number),
                "matches_recalced": int(len(matches)),
                "updated_points": int(total_updates),
            }
        )
    except (ValueError, TypeError):
        return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)
    except Exception as e:
        logger.exception("miniapp admin_recalc_round error")
        return web.json_response(
            {"ok": False, "error": "admin_recalc_round_failed", "reason": str(e), "signature_checked": True},
            status=500,
        )


async def tournaments_list(request: web.Request) -> web.Response:
    auth_result = _extract_verified_user(request)
    if auth_result[0] is None:
        return auth_result[1]
    _payload, user = auth_result
    tg_user_id = user.get("id") if isinstance(user, dict) else None
    if not tg_user_id:
        return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

    async with SessionLocal() as session:
        tournaments = await _get_active_tournaments(session)
        selected = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
        await session.commit()
        selected_code = (selected.code or "").strip().upper()

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "selected_tournament_code": selected.code,
                "items": [
                    {
                        "code": t.code,
                        "name": t.name,
                        "round_min": int(t.round_min),
                        "round_max": int(t.round_max),
                        "selected": (t.code or "").strip().upper() == selected_code,
                    }
                    for t in tournaments
                ],
            }
        )


async def tournament_select(request: web.Request) -> web.Response:
    auth_result = _extract_verified_user(request)
    if auth_result[0] is None:
        return auth_result[1]
    _payload, user = auth_result
    tg_user_id = user.get("id") if isinstance(user, dict) else None
    if not tg_user_id:
        return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

    try:
        body = await request.json()
    except Exception:
        body = {}
    requested_code = str(body.get("tournament_code") or "").strip().upper()
    if not requested_code:
        return web.json_response({"ok": False, "error": "tournament_code_required"}, status=400)

    async with SessionLocal() as session:
        tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=requested_code)
        await session.commit()
        if (tournament.code or "").strip().upper() != requested_code:
            return web.json_response({"ok": False, "error": "tournament_not_available"}, status=404)
        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "selected_tournament_code": tournament.code,
                "selected_tournament_name": tournament.name,
            }
        )


async def _build_profile_achievements(
    session,
    tournament_id: int,
    tg_user_id: int,
    missed_matches: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    achievements: list[dict[str, Any]] = []

    def _push(key: str, title: str, emoji: str, earned: bool, description: str) -> None:
        achievements.append(
            {
                "key": key,
                "title": title,
                "emoji": emoji,
                "earned": bool(earned),
                "taken_by_other": False,
                "description": description,
            }
        )

    def _push_unique(key: str, title: str, emoji: str, holders: set[int], description: str) -> None:
        earned = int(tg_user_id) in holders
        _push(
            key=key,
            title=title,
            emoji=emoji,
            earned=earned,
            description=description,
        )
        if holders and not earned:
            achievements[-1]["taken_by_other"] = True

    participant_rows = (
        await session.execute(
            select(UserTournament.tg_user_id).where(UserTournament.tournament_id == int(tournament_id))
        )
    ).all()
    participant_ids = [int(r[0]) for r in participant_rows if r[0] is not None]

    closed_match_rows = (
        await session.execute(
            select(Match.id, Match.round_number, Match.home_score, Match.away_score, Match.kickoff_time)
            .where(
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
                Match.home_score.is_not(None),
                Match.away_score.is_not(None),
            )
            .order_by(Match.kickoff_time.asc(), Match.id.asc())
        )
    ).all()
    closed_matches: list[dict[str, Any]] = [
        {
            "match_id": int(mid),
            "round_number": int(round_number or 0),
            "home_score": int(home_score or 0),
            "away_score": int(away_score or 0),
            "kickoff_time": kickoff_time,
        }
        for mid, round_number, home_score, away_score, kickoff_time in closed_match_rows
    ]
    closed_match_ids = [m["match_id"] for m in closed_matches]

    user_point_rows = (
        await session.execute(
            select(Point.match_id, Point.category, Point.points).where(
                Point.tg_user_id == int(tg_user_id),
                Point.match_id.in_(closed_match_ids) if closed_match_ids else false(),
            )
        )
    ).all()
    user_category_by_match = {int(mid): str(cat or "") for mid, cat, _pts in user_point_rows}
    user_points_by_match = {int(mid): int(pts or 0) for mid, _cat, pts in user_point_rows}

    # no_miss_tour: количество туров без пропусков.
    round_totals_rows = (
        await session.execute(
            select(Match.round_number, func.count(Match.id))
            .where(
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
            )
            .group_by(Match.round_number)
        )
    ).all()
    user_round_pred_rows = (
        await session.execute(
            select(Match.round_number, func.count(Prediction.id))
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Prediction.tg_user_id == int(tg_user_id),
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
            )
            .group_by(Match.round_number)
        )
    ).all()
    round_total_map = {int(r): int(c or 0) for r, c in round_totals_rows if r is not None}
    round_pred_map = {int(r): int(c or 0) for r, c in user_round_pred_rows if r is not None}
    round_closed_count_map: dict[int, int] = {}
    for m in closed_matches:
        rnd = int(m["round_number"] or 0)
        round_closed_count_map[rnd] = int(round_closed_count_map.get(rnd, 0)) + 1
    round_is_completed: dict[int, bool] = {
        int(rnd): int(total or 0) > 0 and int(round_closed_count_map.get(int(rnd), 0)) >= int(total or 0)
        for rnd, total in round_total_map.items()
    }
    sorted_rounds = sorted(round_total_map.keys())
    no_miss_round_count = 0
    for rnd in sorted_rounds:
        if not bool(round_is_completed.get(int(rnd), False)):
            continue
        total = int(round_total_map.get(rnd, 0))
        predicted = int(round_pred_map.get(rnd, 0))
        if total <= 0:
            continue
        if predicted >= total:
            no_miss_round_count += 1

    no_miss_specs = [
        (
            "no_miss_tour_streak_bronze",
            "Без пропусков",
            "🧱",
            bool(no_miss_round_count >= 1),
            "Проставил на все матчи в туре.",
        ),
        (
            "no_miss_tour_streak_silver",
            "Без пропусков",
            "🧱",
            bool(no_miss_round_count >= 2),
            "Проставил на все матчи в 2-х турах.",
        ),
        (
            "no_miss_tour_streak_gold",
            "Без пропусков",
            "🧱",
            bool(no_miss_round_count >= 3),
            "Проставил на все матчи в 3-х турах.",
        ),
    ]
    for key, title, emoji, earned, description in no_miss_specs:
        _push(
            key=key,
            title=title,
            emoji=emoji,
            earned=earned,
            description=description,
        )

    # scoring_match_streak: серия матчей подряд с очками > 0.
    current_scoring_streak = 0
    max_scoring_match_streak = 0
    for match in closed_matches:
        mid = int(match["match_id"])
        if int(user_points_by_match.get(mid, 0)) > 0:
            current_scoring_streak += 1
            max_scoring_match_streak = max(max_scoring_match_streak, current_scoring_streak)
        else:
            current_scoring_streak = 0

    scoring_specs = [
        ("scoring_match_streak_bronze", "Серия с очками", "⚡", 3),
        ("scoring_match_streak_silver", "Серия с очками", "⚡", 5),
        ("scoring_match_streak_gold", "Серия с очками", "⚡", 10),
    ]
    for key, title, emoji, target in scoring_specs:
        _push(
            key=key,
            title=title,
            emoji=emoji,
            earned=int(max_scoring_match_streak) >= int(target),
            description=f"Набрать очки в {target} матчах подряд.",
        )

    # duel_wins_total: всего побед в завершённых дуэлях 1x1.
    duel_wins_total = int(
        (
            await session.execute(
                select(func.count(Duel.id))
                .where(
                    Duel.tournament_id == int(tournament_id),
                    Duel.status == "finished",
                    Duel.winner_tg_user_id == int(tg_user_id),
                )
            )
        ).scalar_one()
        or 0
    )
    duel_specs = [
        ("duel_wins_total_bronze", "Мастер дуэли", "⚔️", 5),
        ("duel_wins_total_silver", "Мастер дуэли", "⚔️", 10),
        ("duel_wins_total_gold", "Мастер дуэли", "⚔️", 20),
    ]
    for key, title, emoji, target in duel_specs:
        _push(
            key=key,
            title=title,
            emoji=emoji,
            earned=int(duel_wins_total) >= int(target),
            description=f"Выиграть {target} дуэлей 1х1.",
        )

    # first / last exact in tournament.
    first_exact_row = (
        await session.execute(
            select(Prediction.tg_user_id)
            .select_from(Prediction)
            .join(
                Point,
                (Point.match_id == Prediction.match_id) & (Point.tg_user_id == Prediction.tg_user_id),
            )
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
                Point.category == "exact",
            )
            .order_by(
                Point.created_at.asc(),
                Match.kickoff_time.asc(),
                Prediction.created_at.asc(),
                Prediction.id.asc(),
            )
            .limit(1)
        )
    ).first()
    first_exact_holders = {int(first_exact_row[0])} if first_exact_row and first_exact_row[0] is not None else set()
    _push_unique(
        key="first_exact_tournament",
        title="Первый точный счёт в турнире",
        emoji="🎯",
        holders=first_exact_holders,
        description="Ты первый в турнире угадал точный счёт",
    )

    last_exact_holders: set[int] = set()
    if participant_ids:
        exact_rows = (
            await session.execute(
                select(
                    Point.tg_user_id,
                    Point.created_at,
                    Prediction.created_at,
                    Prediction.id,
                )
                .select_from(Point)
                .join(
                    Prediction,
                    (Prediction.match_id == Point.match_id) & (Prediction.tg_user_id == Point.tg_user_id),
                )
                .join(Match, Match.id == Point.match_id)
                .where(
                    Match.tournament_id == int(tournament_id),
                    Match.is_placeholder == 0,
                    Point.category == "exact",
                    Point.tg_user_id.in_(participant_ids),
                )
                .order_by(
                    Point.tg_user_id.asc(),
                    Point.created_at.asc(),
                    Prediction.created_at.asc(),
                    Prediction.id.asc(),
                )
            )
        ).all()
        first_exact_by_user: dict[int, tuple[Any, Any, int]] = {}
        for uid, point_created_at, pred_created_at, pred_id in exact_rows:
            iuid = int(uid)
            if iuid not in first_exact_by_user:
                first_exact_by_user[iuid] = (point_created_at, pred_created_at, int(pred_id or 0))
        if all(int(uid) in first_exact_by_user for uid in participant_ids):
            latest_uid = max(
                participant_ids,
                key=lambda uid: (
                    first_exact_by_user[int(uid)][0],
                    first_exact_by_user[int(uid)][1],
                    first_exact_by_user[int(uid)][2],
                ),
            )
            last_exact_holders = {int(latest_uid)}
    _push_unique(
        key="last_exact_tournament",
        title="Последний точный счёт в турнире",
        emoji="🕐",
        holders=last_exact_holders,
        description="Ты последним угадал точный счёт",
    )

    # first_leader_after_round1: лидер(ы) таблицы после завершения 1-го тура.
    round1_is_completed = bool(round_is_completed.get(1, False))
    round1_match_ids = [int(m["match_id"]) for m in closed_matches if int(m["round_number"]) == 1]
    round1_leaders: set[int] = set()
    if participant_ids and round1_match_ids and round1_is_completed:
        round1_points_rows = (
            await session.execute(
                select(Point.tg_user_id, func.sum(Point.points))
                .where(Point.match_id.in_(round1_match_ids))
                .group_by(Point.tg_user_id)
            )
        ).all()
        round1_points_map = {int(uid): int(pts or 0) for uid, pts in round1_points_rows}
        values = [int(round1_points_map.get(int(uid), 0)) for uid in participant_ids]
        if values:
            max_points = max(values)
            round1_leaders = {int(uid) for uid in participant_ids if int(round1_points_map.get(int(uid), 0)) == int(max_points)}
    _push_unique(
        key="first_leader_after_round1",
        title="Лидер после 1 тура",
        emoji="👑",
        holders=round1_leaders,
        description="Борис Николаевич одобряет победу в первом туре. Наливай",
    )

    # first_101_points: первый(е), кто достиг 100+ очков за турнир.
    first_101_holders: set[int] = set()
    if participant_ids and closed_match_ids:
        all_points_rows = (
            await session.execute(
                select(Point.match_id, Point.tg_user_id, Point.points).where(Point.match_id.in_(closed_match_ids))
            )
        ).all()
        points_by_pair: dict[tuple[int, int], int] = {
            (int(mid), int(uid)): int(pts or 0) for mid, uid, pts in all_points_rows
        }
        pred_rows = (
            await session.execute(
                select(Prediction.match_id, Prediction.tg_user_id, Prediction.created_at, Prediction.id).where(
                    Prediction.match_id.in_(closed_match_ids),
                    Prediction.tg_user_id.in_(participant_ids),
                )
            )
        ).all()
        pred_order_map: dict[tuple[int, int], tuple[Any, int]] = {
            (int(mid), int(uid)): (created_at, int(pred_id or 0)) for mid, uid, created_at, pred_id in pred_rows
        }
        cumulative: dict[int, int] = {int(uid): 0 for uid in participant_ids}
        for mid in closed_match_ids:
            reached_now: list[int] = []
            for uid in participant_ids:
                prev = int(cumulative.get(int(uid), 0))
                cumulative[int(uid)] = int(cumulative.get(int(uid), 0)) + int(points_by_pair.get((int(mid), int(uid)), 0))
                now_val = int(cumulative.get(int(uid), 0))
                if prev < 100 <= now_val:
                    reached_now.append(int(uid))
            if reached_now:
                winner_uid = min(
                    reached_now,
                    key=lambda uid: (
                        pred_order_map.get((int(mid), int(uid)), (None, 10**18))[0] is None,
                        pred_order_map.get((int(mid), int(uid)), (None, 10**18))[0],
                        pred_order_map.get((int(mid), int(uid)), (None, 10**18))[1],
                    ),
                )
                first_101_holders = {int(winner_uid)}
                break
    _push_unique(
        key="first_101_points",
        title="Поздравляем с юбилеем!",
        emoji="💯",
        holders=first_101_holders,
        description="Ты первым набрал 100 очков. Или 101...",
    )

    # group_stage_winner_after_round3: лидер(ы) после завершения 3-го тура.
    round3_is_completed = bool(round_is_completed.get(3, False))
    round3_match_ids = [int(m["match_id"]) for m in closed_matches if int(m["round_number"]) <= 3]
    round3_leaders: set[int] = set()
    if participant_ids and round3_match_ids and round3_is_completed:
        round3_points_rows = (
            await session.execute(
                select(Point.tg_user_id, func.sum(Point.points))
                .where(Point.match_id.in_(round3_match_ids))
                .group_by(Point.tg_user_id)
            )
        ).all()
        round3_points_map = {int(uid): int(pts or 0) for uid, pts in round3_points_rows}
        values = [int(round3_points_map.get(int(uid), 0)) for uid in participant_ids]
        if values:
            max_points = max(values)
            round3_leaders = {int(uid) for uid in participant_ids if int(round3_points_map.get(int(uid), 0)) == int(max_points)}
    _push_unique(
        key="group_stage_winner_after_round3",
        title="Победитель группового этапа",
        emoji="🏆",
        holders=round3_leaders,
        description="Ты выиграл групповой этап. Тед Лассо гордится тобой",
    )

    # Секретные ачивки.
    high_scoring_exact = any(
        int(user_category_by_match.get(int(m["match_id"]), "") == "exact")
        and (int(m["home_score"]) + int(m["away_score"]) >= 4)
        for m in closed_matches
    )
    _push(
        key="high_scoring_exact",
        title="Мастер многомяча",
        emoji="🔥",
        earned=bool(high_scoring_exact),
        description="Угадал точный счет в матче, где было забито 4+ гола",
    )

    only_scorer_rows = (
        await session.execute(
            select(
                Point.match_id,
                func.sum(case((Point.points > 0, 1), else_=0)).label("positive_count"),
                func.max(
                    case(
                        ((Point.tg_user_id == int(tg_user_id)) & (Point.points > 0), 1),
                        else_=0,
                    )
                ).label("my_positive"),
            )
            .select_from(Point)
            .join(Match, Match.id == Point.match_id)
            .where(
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
                Match.home_score.is_not(None),
                Match.away_score.is_not(None),
            )
            .group_by(Point.match_id)
            .having(
                func.sum(case((Point.points > 0, 1), else_=0)) == 1,
                func.max(case(((Point.tg_user_id == int(tg_user_id)) & (Point.points > 0), 1), else_=0)) == 1,
            )
            .limit(1)
        )
    ).first()
    _push(
        key="only_scorer_in_match",
        title="Единственный с очками",
        emoji="🧠",
        earned=only_scorer_rows is not None,
        description="Ты набрал очки в матче, в котором остальные получили 0",
    )

    # Fergie-time hit: прогноз за 5 минут до старта, который принес очки.
    fergie_rows = (
        await session.execute(
            select(Prediction.created_at, Prediction.updated_at, Match.kickoff_time, Point.points)
            .select_from(Prediction)
            .join(
                Point,
                (Point.match_id == Prediction.match_id) & (Point.tg_user_id == Prediction.tg_user_id),
            )
            .join(Match, Match.id == Prediction.match_id)
            .where(
                Prediction.tg_user_id == int(tg_user_id),
                Match.tournament_id == int(tournament_id),
                Match.is_placeholder == 0,
                Match.home_score.is_not(None),
                Match.away_score.is_not(None),
                Point.points > 0,
            )
            .order_by(Prediction.updated_at.desc(), Prediction.id.desc())
        )
    ).all()
    fergie_time_hit = False
    for created_at, updated_at, kickoff_time, points in fergie_rows:
        if int(points or 0) <= 0:
            continue
        ref_time = updated_at or created_at
        if ref_time is None or kickoff_time is None:
            continue
        delta_sec = (kickoff_time - ref_time).total_seconds()
        if 0 <= delta_sec <= 5 * 60:
            fergie_time_hit = True
            break
    _push(
        key="fergie_time_hit",
        title="Ферги тайм",
        emoji="⏱️",
        earned=fergie_time_hit,
        description="Ты сделал прогноз за 5 минут до начала матча и заработал очки",
    )

    progress_meta: dict[str, Any] = {
        "no_miss_tour_count": int(no_miss_round_count),
        "scoring_match_streak": int(max_scoring_match_streak),
        "duel_wins_total": int(duel_wins_total),
        "missed_matches": int(missed_matches),
    }
    return achievements, progress_meta


async def _build_profile_tournament_history(
    session,
    tg_user_id: int,
    current_tournament_id: int,
) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []

    user_tournaments_rows = (
        await session.execute(
            select(UserTournament.tournament_id).where(UserTournament.tg_user_id == int(tg_user_id))
        )
    ).all()
    tournament_ids = [int(r[0]) for r in user_tournaments_rows if r[0] is not None]
    if not tournament_ids:
        return history

    for tid in tournament_ids:
        if int(tid) == int(current_tournament_id):
            continue

        tournament = (
            await session.execute(select(Tournament).where(Tournament.id == int(tid)))
        ).scalar_one_or_none()
        if tournament is None:
            continue

        total_matches_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == int(tid),
                Match.is_placeholder == 0,
            )
        )
        total_matches = int(total_matches_q.scalar_one() or 0)
        if total_matches <= 0:
            continue

        played_matches_q = await session.execute(
            select(func.count(Match.id)).where(
                Match.tournament_id == int(tid),
                Match.is_placeholder == 0,
                Match.home_score.is_not(None),
                Match.away_score.is_not(None),
            )
        )
        played_matches = int(played_matches_q.scalar_one() or 0)

        # В историю попадают только завершённые турниры.
        if played_matches < total_matches:
            continue

        rows, participants = await _build_overall_table_rows(session, int(tid))
        my_row = next((r for r in rows if int(r.get("tg_user_id", 0)) == int(tg_user_id)), None)
        if my_row is None:
            continue

        history.append(
            {
                "tournament_code": tournament.code,
                "tournament_name": tournament.name,
                "place": int(my_row.get("place", 0)),
                "participants": int(participants or 0),
                "total_points": int(my_row.get("total", 0)),
                "exact": int(my_row.get("exact", 0)),
                "diff": int(my_row.get("diff", 0)),
                "outcome": int(my_row.get("outcome", 0)),
                "missed_matches": int(my_row.get("missed_matches", 0)),
                "hit_rate": float(my_row.get("hit_rate", 0.0)),
            }
        )

    history.sort(key=lambda x: str(x.get("tournament_name", "")), reverse=True)
    return history


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
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()
            await _upsert_user_from_webapp(session, user if isinstance(user, dict) else {})
            await session.commit()

            target_user_raw = (request.query.get("target_user_id") or "").strip()
            target_tg_user_id = int(tg_user_id)
            if target_user_raw.isdigit():
                candidate = int(target_user_raw)
                if candidate > 0:
                    target_tg_user_id = candidate

            # Просмотр чужого профиля разрешаем только для участника выбранного турнира.
            if int(target_tg_user_id) != int(tg_user_id):
                target_participant = (
                    await session.execute(
                        select(UserTournament).where(
                            UserTournament.tg_user_id == int(target_tg_user_id),
                            UserTournament.tournament_id == int(tournament.id),
                        )
                    )
                ).scalar_one_or_none()
                if target_participant is None:
                    target_tg_user_id = int(tg_user_id)

            user_row = (
                await session.execute(
                    select(User).where(User.tg_user_id == int(target_tg_user_id))
                )
            ).scalar_one_or_none()

            if user_row is None:
                return web.json_response(
                    {
                        "ok": True,
                        "trusted": True,
                        "joined": False,
                        "tournament_code": tournament.code,
                        "tournament_name": tournament.name,
                        "tg_user_id": int(target_tg_user_id),
                        "viewed_tg_user_id": int(target_tg_user_id),
                        "is_self_profile": int(target_tg_user_id) == int(tg_user_id),
                        "display_name": user.get("first_name") or user.get("username") or f"id:{target_tg_user_id}",
                        "photo_url": user.get("photo_url") if int(target_tg_user_id) == int(tg_user_id) else None,
                        "message": "Пользователь есть в Telegram, но ещё не вступил в турнир бота.",
                    }
                )

            user_tournament_row = (
                await session.execute(
                    select(UserTournament).where(
                        UserTournament.tg_user_id == int(target_tg_user_id),
                        UserTournament.tournament_id == int(tournament.id),
                    )
                )
            ).scalar_one_or_none()
            if user_tournament_row is None:
                return web.json_response(
                    {
                        "ok": True,
                        "trusted": True,
                        "joined": False,
                        "tournament_code": tournament.code,
                        "tournament_name": tournament.name,
                        "tg_user_id": int(target_tg_user_id),
                        "viewed_tg_user_id": int(target_tg_user_id),
                        "is_self_profile": int(target_tg_user_id) == int(tg_user_id),
                        "display_name": user.get("first_name") or user.get("username") or f"id:{target_tg_user_id}",
                        "photo_url": user_row.photo_url
                        or (user.get("photo_url") if int(target_tg_user_id) == int(tg_user_id) else None),
                        "message": f"Ты ещё не вступил в турнир «{tournament.name}».",
                    }
                )

            duel_rating_row = (
                await session.execute(
                    select(DuelElo.rating).where(
                        DuelElo.tournament_id == int(tournament.id),
                        DuelElo.tg_user_id == int(target_tg_user_id),
                    )
                )
            ).scalar_one_or_none()
            duel_rating = int(duel_rating_row or 1000)

            stats_row = (
                await session.execute(
                    select(
                        func.count(Prediction.id).label("predictions_count"),
                        func.coalesce(
                            func.sum(
                                case(
                                    (
                                        (Match.home_score.is_not(None)) & (Match.away_score.is_not(None)),
                                        1,
                                    ),
                                    else_=0,
                                )
                            ),
                            0,
                        ).label("resolved_predictions_count"),
                        func.coalesce(func.sum(Point.points), 0).label("total_points"),
                        func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact_hits"),
                        func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff_hits"),
                        func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome_hits"),
                    )
                    .select_from(User)
                    .outerjoin(Prediction, Prediction.tg_user_id == User.tg_user_id)
                    .outerjoin(Match, Match.id == Prediction.match_id)
                    .outerjoin(
                        Point,
                        (Point.tg_user_id == User.tg_user_id) & (Point.match_id == Prediction.match_id),
                    )
                    .where(User.tg_user_id == int(target_tg_user_id))
                    .where(Match.tournament_id == int(tournament.id))
                )
            ).one()

            now = _now_msk_naive()
            started_total_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == int(tournament.id),
                    Match.is_placeholder == 0,
                    Match.kickoff_time <= now,
                )
            )
            started_total = int(started_total_q.scalar_one() or 0)

            pred_started_q = await session.execute(
                select(func.count(Prediction.id))
                .select_from(Prediction)
                .join(Match, Match.id == Prediction.match_id)
                .where(
                    Prediction.tg_user_id == int(target_tg_user_id),
                    Match.tournament_id == int(tournament.id),
                    Match.is_placeholder == 0,
                    Match.kickoff_time <= now,
                )
            )
            pred_started = int(pred_started_q.scalar_one() or 0)
            missed_matches = max(0, started_total - pred_started)

            total_matches_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == int(tournament.id),
                    Match.is_placeholder == 0,
                )
            )
            total_matches = int(total_matches_q.scalar_one() or 0)

            played_matches_q = await session.execute(
                select(func.count(Match.id)).where(
                    Match.tournament_id == int(tournament.id),
                    Match.is_placeholder == 0,
                    Match.home_score.is_not(None),
                    Match.away_score.is_not(None),
                )
            )
            played_matches = int(played_matches_q.scalar_one() or 0)
            tournament_progress_pct = round((played_matches * 100.0 / total_matches), 1) if total_matches > 0 else 0.0

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
                        LeagueParticipant.tg_user_id == int(target_tg_user_id),
                        LeagueParticipant.is_active == 1,
                        Stage.is_active == 1,
                    )
                    .order_by(LeagueParticipant.id.desc())
                    .limit(1)
                )
            ).one_or_none()

            display_name = (
                user_tournament_row.display_name
                or user_row.display_name
                or user_row.full_name
                or (f"@{user_row.username}" if user_row.username else None)
                or f"id:{tg_user_id}"
            )

            predictions_count = int(stats_row.predictions_count or 0)
            resolved_predictions_count = int(stats_row.resolved_predictions_count or 0)
            exact_hits = int(stats_row.exact_hits or 0)
            diff_hits = int(stats_row.diff_hits or 0)
            outcome_hits = int(stats_row.outcome_hits or 0)
            hits_total = exact_hits + diff_hits + outcome_hits
            hit_rate = round((hits_total * 100.0 / resolved_predictions_count), 1) if resolved_predictions_count > 0 else 0.0
            tournament_code_upper = (tournament.code or "").strip().upper()
            achievements: list[dict[str, Any]] = []
            ach_meta: dict[str, Any] = {}
            achievements_earned = 0
            progress_candidates: list[dict[str, Any]] = []
            next_achievement = None

            if tournament_code_upper == WC_TOURNAMENT_CODE:
                achievements, ach_meta = await _build_profile_achievements(
                    session=session,
                    tournament_id=int(tournament.id),
                    tg_user_id=int(target_tg_user_id),
                    missed_matches=int(missed_matches),
                )
                achievements_earned = sum(1 for a in achievements if bool(a.get("earned")))

                def _add_progress_candidate(
                    *,
                    key: str,
                    title: str,
                    emoji: str,
                    current: int,
                    target: int,
                    positive_only: bool = True,
                ) -> None:
                    if current >= target:
                        return
                    if positive_only and current <= 0:
                        return
                    progress_candidates.append(
                        {
                            "key": key,
                            "title": title,
                            "emoji": emoji,
                            "current": int(current),
                            "target": int(target),
                            "left": int(target - current),
                        }
                    )

                _add_progress_candidate(
                    key="no_miss_tour_streak_bronze",
                    title="Без пропусков",
                    emoji="🧱",
                    current=int(ach_meta.get("no_miss_tour_count", 0)),
                    target=1,
                    positive_only=True,
                )
                _add_progress_candidate(
                    key="no_miss_tour_streak_silver",
                    title="Без пропусков",
                    emoji="🧱",
                    current=int(ach_meta.get("no_miss_tour_count", 0)),
                    target=2,
                    positive_only=True,
                )
                _add_progress_candidate(
                    key="no_miss_tour_streak_gold",
                    title="Без пропусков",
                    emoji="🧱",
                    current=int(ach_meta.get("no_miss_tour_count", 0)),
                    target=3,
                    positive_only=True,
                )
                _add_progress_candidate(
                    key="scoring_match_streak_bronze",
                    title="Серия с очками",
                    emoji="⚡",
                    current=int(ach_meta.get("scoring_match_streak", 0)),
                    target=3,
                    positive_only=True,
                )
                _add_progress_candidate(
                    key="scoring_match_streak_silver",
                    title="Серия с очками",
                    emoji="⚡",
                    current=int(ach_meta.get("scoring_match_streak", 0)),
                    target=5,
                    positive_only=True,
                )
                _add_progress_candidate(
                    key="scoring_match_streak_gold",
                    title="Серия с очками",
                    emoji="⚡",
                    current=int(ach_meta.get("scoring_match_streak", 0)),
                    target=10,
                    positive_only=True,
                )
                _add_progress_candidate(
                    key="duel_wins_total_bronze",
                    title="Мастер дуэли",
                    emoji="⚔️",
                    current=int(ach_meta.get("duel_wins_total", 0)),
                    target=5,
                    positive_only=False,
                )
                _add_progress_candidate(
                    key="duel_wins_total_silver",
                    title="Мастер дуэли",
                    emoji="⚔️",
                    current=int(ach_meta.get("duel_wins_total", 0)),
                    target=10,
                    positive_only=False,
                )
                _add_progress_candidate(
                    key="duel_wins_total_gold",
                    title="Мастер дуэли",
                    emoji="⚔️",
                    current=int(ach_meta.get("duel_wins_total", 0)),
                    target=20,
                    positive_only=False,
                )

                progress_candidates.sort(key=lambda x: (int(x["left"]), int(x["target"])))
                next_achievement = progress_candidates[0] if progress_candidates else None

            recent_form_rows = (
                await session.execute(
                    select(
                        Match.id,
                        Match.round_number,
                        Match.home_team,
                        Match.away_team,
                        Point.category,
                        Point.points,
                        Prediction.id.label("has_pred"),
                    )
                    .select_from(Match)
                    .outerjoin(
                        Prediction,
                        (Prediction.match_id == Match.id) & (Prediction.tg_user_id == int(target_tg_user_id)),
                    )
                    .outerjoin(
                        Point,
                        (Point.match_id == Match.id) & (Point.tg_user_id == int(target_tg_user_id)),
                    )
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.is_placeholder == 0,
                        Match.home_score.is_not(None),
                        Match.away_score.is_not(None),
                    )
                    .order_by(Match.kickoff_time.desc(), Match.id.desc())
                    .limit(5)
                )
            ).all()
            recent_form = []
            for _mid, round_number, home_team, away_team, category, pts, has_pred in recent_form_rows:
                if has_pred is None:
                    emoji = "⛔"
                    points_value = 0
                else:
                    emoji = _point_category_emoji(category, pts)
                    points_value = int(pts or 0)
                recent_form.append(
                    {
                        "round": int(round_number or 0),
                        "emoji": emoji,
                        "points": points_value,
                        "label": f"{home_team} — {away_team}",
                    }
                )

            # Статусы формы считаем по текущей "живой" серии от последнего завершённого матча.
            # Категории серии:
            # - fire: есть прогноз и очки > 0
            # - cold: есть прогноз, но очки == 0
            # - slow: прогноза нет
            form_streak_rows = (
                await session.execute(
                    select(
                        Match.id,
                        Point.points,
                        Prediction.id.label("has_pred"),
                    )
                    .select_from(Match)
                    .outerjoin(
                        Prediction,
                        (Prediction.match_id == Match.id) & (Prediction.tg_user_id == int(target_tg_user_id)),
                    )
                    .outerjoin(
                        Point,
                        (Point.match_id == Match.id) & (Point.tg_user_id == int(target_tg_user_id)),
                    )
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.is_placeholder == 0,
                        Match.home_score.is_not(None),
                        Match.away_score.is_not(None),
                    )
                    .order_by(Match.kickoff_time.desc(), Match.id.desc())
                )
            ).all()
            fire_streak = 0
            cold_streak = 0
            slow_streak = 0
            if form_streak_rows:
                first_has_pred = form_streak_rows[0].has_pred is not None
                first_points = int(form_streak_rows[0].points or 0) if first_has_pred else 0
                if not first_has_pred:
                    kind = "slow"
                elif first_points > 0:
                    kind = "fire"
                else:
                    kind = "cold"

                for _mid, pts, has_pred in form_streak_rows:
                    row_has_pred = has_pred is not None
                    row_points = int(pts or 0) if row_has_pred else 0
                    row_kind = "slow" if not row_has_pred else ("fire" if row_points > 0 else "cold")
                    if row_kind != kind:
                        break
                    if kind == "fire":
                        fire_streak += 1
                    elif kind == "cold":
                        cold_streak += 1
                    else:
                        slow_streak += 1

            form_statuses: list[str] = []
            if fire_streak >= 3:
                form_statuses.append("🔥 В огне")
            if cold_streak >= 3:
                form_statuses.append("❄️ Плохая форма")
            if slow_streak >= 3:
                form_statuses.append("⛔ Тормозишь")

            if (tournament.code or "").strip().upper() == DEFAULT_TOURNAMENT_CODE:
                table_rows, table_meta = await build_active_stage_league_table(int(target_tg_user_id))
                user_place = next(
                    (int(r.get("place", 0)) for r in table_rows if int(r.get("tg_user_id", 0)) == int(target_tg_user_id)),
                    None,
                ) if table_meta is not None else None
                participants = int(table_meta.participants) if table_meta is not None else 0
                rows_for_statuses = table_rows
            else:
                rows, participants = await _build_overall_table_rows(session, int(tournament.id))
                user_place = next((int(r["place"]) for r in rows if int(r["tg_user_id"]) == int(target_tg_user_id)), None)
                rows_for_statuses = rows

            live_statuses: list[str] = []
            if user_place == 1:
                live_statuses.append("Лидер таблицы")

            def _best_status(field: str, label: str) -> None:
                if not rows_for_statuses:
                    return
                max_value = max(int(r.get(field, 0)) for r in rows_for_statuses)
                if max_value <= 0:
                    return
                my_row = next(
                    (r for r in rows_for_statuses if int(r.get("tg_user_id", 0)) == int(target_tg_user_id)),
                    None,
                )
                if my_row is None:
                    return
                if int(my_row.get(field, 0)) == int(max_value):
                    live_statuses.append(label)

            _best_status("exact", "Лучший по 🎯 точным")
            _best_status("diff", "Лучший по 📏 разнице")
            _best_status("outcome", "Лучший по ✅ исходам")

            insights: list[str] = []

            def _add_insight(text: str) -> None:
                t = (text or "").strip()
                if not t:
                    return
                if t not in insights:
                    insights.append(t)

            if predictions_count == 0:
                _add_insight("Пока нет прогнозов. Поставь первый прогноз, чтобы запустить личную статистику.")
            elif resolved_predictions_count == 0:
                _add_insight("Прогнозы уже стоят. Ждём первые результаты матчей, чтобы посчитать форму и точность.")

            if resolved_predictions_count >= 4:
                if hit_rate >= 70.0:
                    _add_insight(f"Отличная точность: {hit_rate:.1f}%. Ты стабильно берёшь очки на дистанции.")
                elif hit_rate < 35.0:
                    _add_insight(f"Точность сейчас {hit_rate:.1f}%. Попробуй меньше рисковать с точным счётом.")

            if exact_hits >= 2:
                _add_insight(f"Точных счётов уже {exact_hits}. Это сильный буст по очкам в таблице.")
            if diff_hits >= 3:
                _add_insight(f"Угаданных разниц: {diff_hits}. Надёжная стратегия для стабильного набора.")
            if outcome_hits >= 3:
                _add_insight(f"Исходы заходят: {outcome_hits}. База работает, можно усиливать точными.")

            if started_total >= 3:
                if missed_matches == 0:
                    _add_insight("Пока без пропусков. Дисциплина даёт преимущество на длинной дистанции.")
                elif missed_matches >= 3:
                    _add_insight(f"Пропущено {missed_matches} матчей. Регулярные ставки могут быстро вернуть позиции.")
                else:
                    _add_insight(f"Есть пропуски ({missed_matches}). Закрывай туры полностью, чтобы не терять лёгкие очки.")

            if participants > 1 and user_place is not None:
                if user_place == 1:
                    _add_insight("Ты лидер таблицы. Удержать первое место обычно сложнее, чем взять его.")
                elif user_place <= 3:
                    _add_insight(f"Ты в топ-{user_place}. Один удачный тур может поднять на первое место.")
                else:
                    _add_insight(f"Текущее место: {user_place}/{participants}. Впереди ещё много очков в матче-апах.")

            if fire_streak >= 3:
                _add_insight(f"Серия с очками: {fire_streak}. Ты в хорошем темпе.")
            if cold_streak >= 3:
                _add_insight(f"Серия без очков: {cold_streak}. Можно выправить форму на следующем блоке матчей.")
            if slow_streak >= 3:
                _add_insight(f"Без прогнозов подряд: {slow_streak} матчей. Вернись в ритм, чтобы не выпадать из гонки.")

            if next_achievement is not None and int(next_achievement.get("left") or 0) <= 2:
                _add_insight(
                    f"До ачивки «{next_achievement.get('title', '')}» осталось {int(next_achievement.get('left') or 0)}."
                )

            if tournament_progress_pct <= 10.0 and predictions_count > 0:
                _add_insight("Турнир только начинается. Ранний отрыв легко создать за счёт дисциплины.")
            elif tournament_progress_pct >= 80.0:
                _add_insight("Финиш турнира близко. Сейчас особенно важен каждый матч.")

            if not insights:
                _add_insight("Поставь прогноз на ближайший матч — и здесь появятся персональные инсайты.")

            tournament_history = await _build_profile_tournament_history(
                session=session,
                tg_user_id=int(target_tg_user_id),
                current_tournament_id=int(tournament.id),
            )
            legacy_trophies = _legacy_trophies_for_username(user_row.username)

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "joined": True,
                    "tournament_code": tournament.code,
                    "tournament_name": tournament.name,
                    "tg_user_id": int(target_tg_user_id),
                    "viewed_tg_user_id": int(target_tg_user_id),
                    "is_self_profile": int(target_tg_user_id) == int(tg_user_id),
                    "display_name": display_name,
                    "username": user_row.username,
                    "photo_url": user_row.photo_url
                    or (user.get("photo_url") if int(target_tg_user_id) == int(tg_user_id) else None),
                    "predictions_count": predictions_count,
                    "total_points": int(stats_row.total_points or 0) + int(user_tournament_row.bonus_points or 0),
                    "exact_hits": exact_hits,
                    "diff_hits": diff_hits,
                    "outcome_hits": outcome_hits,
                    "hit_rate": hit_rate,
                    "missed_matches": int(missed_matches),
                    "duel_rating": int(duel_rating),
                    "place": user_place,
                    "participants": int(participants),
                    "played_matches": played_matches,
                    "total_matches": total_matches,
                    "tournament_progress_pct": tournament_progress_pct,
                    "achievements": achievements,
                    "achievements_earned": int(achievements_earned),
                    "achievements_total": int(len(achievements)),
                    "achievement_progress": {
                        "no_miss_tour_streak": int(ach_meta.get("no_miss_tour_count", 0)),
                        "scoring_match_streak": int(ach_meta.get("scoring_match_streak", 0)),
                        "duel_wins_total": int(ach_meta.get("duel_wins_total", 0)),
                    },
                    "next_achievement": next_achievement,
                    "insights": insights,
                    "recent_form": recent_form,
                    "live_statuses": live_statuses,
                    "form_statuses": form_statuses,
                    "tournament_history": tournament_history,
                    "legacy_trophies": legacy_trophies,
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
    return "❌"


async def _crowd_stats_for_matches(session, tournament_id: int, match_ids: list[int]) -> dict[int, dict[str, int]]:
    """
    Возвращает агрегированную статистику по прогнозам комьюнити:
    total/home/draw/away в процентах (0-100) + total count.
    Учитываем только активных участников выбранного турнира.
    """
    if not match_ids:
        return {}

    rows = (
        await session.execute(
            select(
                Prediction.match_id.label("match_id"),
                func.count(Prediction.id).label("total_cnt"),
                func.sum(case((Prediction.pred_home > Prediction.pred_away, 1), else_=0)).label("home_cnt"),
                func.sum(case((Prediction.pred_home == Prediction.pred_away, 1), else_=0)).label("draw_cnt"),
                func.sum(case((Prediction.pred_home < Prediction.pred_away, 1), else_=0)).label("away_cnt"),
            )
            .select_from(Prediction)
            .join(
                UserTournament,
                (UserTournament.tg_user_id == Prediction.tg_user_id)
                & (UserTournament.tournament_id == int(tournament_id)),
            )
            .where(Prediction.match_id.in_(match_ids))
            .group_by(Prediction.match_id)
        )
    ).all()

    out: dict[int, dict[str, int]] = {}
    for r in rows:
        total = int(r.total_cnt or 0)
        if total <= 0:
            continue
        home = int(r.home_cnt or 0)
        draw = int(r.draw_cnt or 0)
        away = int(r.away_cnt or 0)
        out[int(r.match_id)] = {
            "crowd_count": total,
            "crowd_home_pct": int(round((home * 100.0) / total)),
            "crowd_draw_pct": int(round((draw * 100.0) / total)),
            "crowd_away_pct": int(round((away * 100.0) / total)),
        }
    return out
    return "✅"


async def _build_overall_table_rows(
    session,
    tournament_id: int,
    round_number: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    participants_q = await session.execute(
        select(func.count(func.distinct(UserTournament.tg_user_id)))
        .select_from(UserTournament)
        .where(UserTournament.tournament_id == int(tournament_id))
    )
    participants = int(participants_q.scalar_one() or 0)

    round_filter = [Match.tournament_id == int(tournament_id)]
    if round_number is not None:
        round_filter.append(Match.round_number == int(round_number))

    points_subq = (
        select(
            Point.tg_user_id.label("tg_user_id"),
            Point.points.label("points"),
            Point.category.label("category"),
        )
        .join(Match, Match.id == Point.match_id)
        .where(*round_filter)
        .subquery()
    )
    pred_total_subq = (
        select(
            Prediction.tg_user_id.label("tg_user_id"),
            func.count(Prediction.id).label("pred_total"),
        )
        .select_from(Prediction)
        .join(Match, Match.id == Prediction.match_id)
        .where(*round_filter)
        .group_by(Prediction.tg_user_id)
        .subquery()
    )
    now = datetime.utcnow()
    started_total_q = await session.execute(
        select(func.count(Match.id)).where(
            *round_filter,
            Match.kickoff_time <= now,
        )
    )
    started_total = int(started_total_q.scalar_one() or 0)
    pred_started_subq = (
        select(
            Prediction.tg_user_id.label("tg_user_id"),
            func.count(Prediction.id).label("pred_started"),
        )
        .select_from(Prediction)
        .join(Match, Match.id == Prediction.match_id)
        .where(
            *round_filter,
            Match.kickoff_time <= now,
        )
        .group_by(Prediction.tg_user_id)
        .subquery()
    )

    q = await session.execute(
        select(
            User.tg_user_id,
            UserTournament.display_name,
            User.display_name,
            User.username,
            User.full_name,
            UserTournament.bonus_points,
            func.coalesce(func.sum(points_subq.c.points), 0).label("total"),
            func.coalesce(func.sum(case((points_subq.c.category == "exact", 1), else_=0)), 0).label("exact"),
            func.coalesce(func.sum(case((points_subq.c.category == "diff", 1), else_=0)), 0).label("diff"),
            func.coalesce(func.sum(case((points_subq.c.category == "outcome", 1), else_=0)), 0).label("outcome"),
            func.coalesce(pred_total_subq.c.pred_total, 0).label("pred_total"),
            func.coalesce(pred_started_subq.c.pred_started, 0).label("pred_started"),
        )
        .select_from(UserTournament)
        .join(User, User.tg_user_id == UserTournament.tg_user_id)
        .outerjoin(points_subq, points_subq.c.tg_user_id == User.tg_user_id)
        .outerjoin(pred_total_subq, pred_total_subq.c.tg_user_id == User.tg_user_id)
        .outerjoin(pred_started_subq, pred_started_subq.c.tg_user_id == User.tg_user_id)
        .where(UserTournament.tournament_id == int(tournament_id))
        .group_by(
            User.tg_user_id,
            UserTournament.display_name,
            User.display_name,
            User.username,
            User.full_name,
            UserTournament.bonus_points,
            pred_total_subq.c.pred_total,
            pred_started_subq.c.pred_started,
        )
        .order_by(
            (func.coalesce(func.sum(points_subq.c.points), 0) + func.coalesce(UserTournament.bonus_points, 0)).desc(),
            func.coalesce(func.sum(case((points_subq.c.category == "exact", 1), else_=0)), 0).desc(),
            func.coalesce(func.sum(case((points_subq.c.category == "diff", 1), else_=0)), 0).desc(),
            func.coalesce(func.sum(case((points_subq.c.category == "outcome", 1), else_=0)), 0).desc(),
            User.tg_user_id.asc(),
        )
    )

    rows: list[dict[str, Any]] = []
    place = 1
    for (
        tg_user_id,
        tournament_display_name,
        user_display_name,
        username,
        full_name,
        bonus_points,
        total,
        exact,
        diff,
        outcome,
        pred_total,
        pred_started,
    ) in q.all():
        name = (
            tournament_display_name
            or user_display_name
            or (f"@{username}" if username else None)
            or full_name
            or str(tg_user_id)
        )
        rows.append(
            {
                "place": int(place),
                "tg_user_id": int(tg_user_id),
                "name": str(name),
                "total": int(total or 0) + int(bonus_points or 0),
                "exact": int(exact or 0),
                "diff": int(diff or 0),
                "outcome": int(outcome or 0),
                "pred_total": int(pred_total or 0),
                "hits": int(exact or 0) + int(diff or 0) + int(outcome or 0),
                "hit_rate": round(
                    ((int(exact or 0) + int(diff or 0) + int(outcome or 0)) * 100.0 / int(pred_total or 0)),
                    2,
                )
                if int(pred_total or 0) > 0
                else 0.0,
                "missed_matches": max(0, int(started_total) - int(pred_started or 0)),
            }
        )
        place += 1
    return rows, participants


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
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()

            round_min = int(tournament.round_min)
            round_max = int(tournament.round_max)
            requested_round_raw = (request.query.get("round") or "").strip()
            requested_round = int(requested_round_raw) if requested_round_raw.isdigit() else None

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

            if requested_round is not None and int(round_min) <= int(requested_round) <= int(round_max):
                current_round = int(requested_round)

            matches = (
                await session.execute(
                    select(Match)
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number == int(current_round),
                        Match.is_placeholder == 0,
                    )
                    .order_by(Match.kickoff_time.asc(), Match.id.asc())
                )
            ).scalars().all()

            match_ids = [int(m.id) for m in matches]
            preds_map: dict[int, Prediction] = {}
            points_map: dict[int, Point] = {}
            crowd_map: dict[int, dict[str, int]] = {}

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
                crowd_map = await _crowd_stats_for_matches(session, int(tournament.id), match_ids)

            items: list[dict[str, Any]] = []
            total_points = 0
            for m in matches:
                pred = preds_map.get(int(m.id))
                pt = points_map.get(int(m.id))
                points_val = int(pt.points or 0) if pt is not None else None
                if points_val is not None:
                    total_points += points_val

                is_closed = m.home_score is not None and m.away_score is not None
                crowd = crowd_map.get(int(m.id), {})
                items.append(
                    {
                        "match_id": int(m.id),
                        "home_team": m.home_team,
                        "away_team": m.away_team,
                        "group_label": m.group_label,
                        "kickoff": m.kickoff_time.strftime("%d.%m %H:%M"),
                        "status": "closed" if is_closed else "open",
                        "result": f"{m.home_score}:{m.away_score}" if is_closed else None,
                        "prediction": f"{pred.pred_home}:{pred.pred_away}" if pred is not None else None,
                        "points": points_val,
                        "category": pt.category if pt is not None else None,
                        "emoji": _point_category_emoji(pt.category if pt is not None else None, points_val),
                        "crowd_count": int(crowd.get("crowd_count", 0)),
                        "crowd_home_pct": int(crowd.get("crowd_home_pct", 0)),
                        "crowd_draw_pct": int(crowd.get("crowd_draw_pct", 0)),
                        "crowd_away_pct": int(crowd.get("crowd_away_pct", 0)),
                    }
                )

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "tournament_code": tournament.code,
                    "tournament": tournament.name,
                    "round_name": display_round_name(tournament.code, current_round),
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


async def _wc_first_kickoff(session, tournament_id: int) -> datetime | None:
    q = await session.execute(
        select(func.min(Match.kickoff_time)).where(
            Match.tournament_id == int(tournament_id),
            Match.is_placeholder == 0,
        )
    )
    return q.scalar_one_or_none()


async def _wc_winner_options(session, tournament_id: int) -> list[str]:
    homes = await session.execute(
        select(Match.home_team)
        .where(
            Match.tournament_id == int(tournament_id),
            Match.is_placeholder == 0,
            Match.home_team.is_not(None),
        )
        .distinct()
    )
    aways = await session.execute(
        select(Match.away_team)
        .where(
            Match.tournament_id == int(tournament_id),
            Match.is_placeholder == 0,
            Match.away_team.is_not(None),
        )
        .distinct()
    )
    names = {str(x[0]).strip() for x in homes.all() if x and x[0]}
    names.update({str(x[0]).strip() for x in aways.all() if x and x[0]})
    return sorted([x for x in names if x], key=lambda s: s.lower())


async def _get_longterm_picks(session, tournament_id: int, tg_user_id: int) -> dict[str, str]:
    rows = (
        await session.execute(
            select(LongtermPrediction.pick_type, LongtermPrediction.pick_value).where(
                LongtermPrediction.tournament_id == int(tournament_id),
                LongtermPrediction.tg_user_id == int(tg_user_id),
                LongtermPrediction.pick_type.in_(LONGTERM_TYPES),
            )
        )
    ).all()
    out: dict[str, str] = {}
    for pick_type, pick_value in rows:
        out[str(pick_type)] = str(pick_value)
    return out


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
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()
            requested_round_raw = (request.query.get("round") or "").strip()
            requested_round = int(requested_round_raw) if requested_round_raw.isdigit() else None

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
                        Match.is_placeholder == 0,
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

            if requested_round is not None and int(round_min) <= int(requested_round) <= int(round_max):
                current_round = int(requested_round)

            matches = (
                await session.execute(
                    select(Match)
                    .where(
                        Match.tournament_id == int(tournament.id),
                        Match.round_number == int(current_round),
                        Match.kickoff_time > now,
                        Match.is_placeholder == 0,
                    )
                    .order_by(Match.kickoff_time.asc(), Match.id.asc())
                )
            ).scalars().all()

            match_ids = [int(m.id) for m in matches]
            preds_map: dict[int, Prediction] = {}
            crowd_map: dict[int, dict[str, int]] = {}
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
                crowd_map = await _crowd_stats_for_matches(session, int(tournament.id), match_ids)

            items: list[dict[str, Any]] = []
            for m in matches:
                pred = preds_map.get(int(m.id))
                crowd = crowd_map.get(int(m.id), {})
                items.append(
                    {
                        "match_id": int(m.id),
                        "home_team": m.home_team,
                        "away_team": m.away_team,
                        "group_label": m.group_label,
                        "kickoff": m.kickoff_time.strftime("%d.%m %H:%M"),
                        "prediction": f"{pred.pred_home}:{pred.pred_away}" if pred is not None else None,
                        "crowd_count": int(crowd.get("crowd_count", 0)),
                        "crowd_home_pct": int(crowd.get("crowd_home_pct", 0)),
                        "crowd_draw_pct": int(crowd.get("crowd_draw_pct", 0)),
                        "crowd_away_pct": int(crowd.get("crowd_away_pct", 0)),
                    }
                )

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "joined": True,
                    "tournament_code": tournament.code,
                    "tournament": tournament.name,
                    "round_name": display_round_name(tournament.code, current_round),
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
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()

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
                        Match.is_placeholder == 0,
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


async def longterm_current(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()

            enabled = (tournament.code or "").strip().upper() == WC_TOURNAMENT_CODE
            if not enabled:
                return web.json_response(
                    {
                        "ok": True,
                        "trusted": True,
                        "enabled": False,
                        "tournament_code": tournament.code,
                    }
                )

            deadline = await _wc_first_kickoff(session, int(tournament.id))
            now = _now_msk_naive()
            locked = bool(deadline is not None and now >= deadline)

            in_tournament = (
                await session.execute(
                    select(UserTournament.id).where(
                        UserTournament.tg_user_id == int(tg_user_id),
                        UserTournament.tournament_id == int(tournament.id),
                    )
                )
            ).first() is not None

            picks = await _get_longterm_picks(session, int(tournament.id), int(tg_user_id))
            winner_options = await _wc_winner_options(session, int(tournament.id))

            return web.json_response(
                {
                    "ok": True,
                    "trusted": True,
                    "enabled": True,
                    "joined": in_tournament,
                    "locked": locked,
                    "tournament_code": tournament.code,
                    "tournament_name": tournament.name,
                    "deadline_msk": deadline.strftime("%d.%m %H:%M") if deadline is not None else None,
                    "picks": {
                        "winner": picks.get("winner"),
                        "scorer": picks.get("scorer"),
                    },
                    "options": {
                        "winner": winner_options,
                        "scorer": WC_TOP_SCORER_OPTIONS,
                    },
                }
            )
    except Exception as e:
        logger.exception("miniapp longterm_current error")
        return web.json_response(
            {
                "ok": False,
                "error": "longterm_current_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


async def longterm_set(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        body = await request.json()
        pick_type = str(body.get("pick_type") or "").strip().lower()
        pick_value = str(body.get("pick_value") or "").strip()
        if pick_type not in LONGTERM_TYPES:
            return web.json_response({"ok": False, "error": "invalid_pick_type"}, status=400)
        if not pick_value:
            return web.json_response({"ok": False, "error": "pick_value_required"}, status=400)

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()

            if (tournament.code or "").strip().upper() != WC_TOURNAMENT_CODE:
                return web.json_response({"ok": False, "error": "longterm_not_enabled_for_tournament"}, status=400)

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

            deadline = await _wc_first_kickoff(session, int(tournament.id))
            now = _now_msk_naive()
            if deadline is not None and now >= deadline:
                return web.json_response({"ok": False, "error": "longterm_locked"}, status=409)

            if pick_type == "winner":
                allowed = await _wc_winner_options(session, int(tournament.id))
                if pick_value not in allowed:
                    return web.json_response({"ok": False, "error": "invalid_winner_value"}, status=400)
            else:
                if pick_value not in WC_TOP_SCORER_OPTIONS:
                    return web.json_response({"ok": False, "error": "invalid_scorer_value"}, status=400)

            row = (
                await session.execute(
                    select(LongtermPrediction).where(
                        LongtermPrediction.tournament_id == int(tournament.id),
                        LongtermPrediction.tg_user_id == int(tg_user_id),
                        LongtermPrediction.pick_type == pick_type,
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                row = LongtermPrediction(
                    tournament_id=int(tournament.id),
                    tg_user_id=int(tg_user_id),
                    pick_type=pick_type,
                    pick_value=pick_value,
                )
                session.add(row)
                action = "created"
            else:
                row.pick_value = pick_value
                row.updated_at = datetime.utcnow()
                action = "updated"

            await session.commit()
            return web.json_response(
                {
                    "ok": True,
                    "action": action,
                    "pick_type": pick_type,
                    "pick_value": pick_value,
                }
            )
    except (ValueError, TypeError):
        return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)
    except Exception as e:
        logger.exception("miniapp longterm_set error")
        return web.json_response(
            {
                "ok": False,
                "error": "longterm_set_failed",
                "reason": str(e),
                "signature_checked": True,
            },
            status=500,
        )


async def duels_current(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
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
                        "message": "Сначала вступи в турнир, чтобы открыть блок 1x1.",
                    }
                )

            hub = await get_duel_hub(session, tournament_id=int(tournament.id), tg_user_id=int(tg_user_id))
            await session.commit()

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "joined": True,
                "tournament_code": tournament.code,
                "tournament_name": tournament.name,
                **hub,
            }
        )
    except Exception as e:
        logger.exception("miniapp duels_current error")
        return web.json_response(
            {"ok": False, "error": "duels_current_failed", "reason": str(e), "signature_checked": True},
            status=500,
        )


async def duels_challenge(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        body = await request.json()
        match_id = int(body.get("match_id") or 0)
        opponent_tg_user_id = int(body.get("opponent_tg_user_id") or 0)
        pred_home = int(body.get("pred_home") or 0)
        pred_away = int(body.get("pred_away") or 0)
        if match_id <= 0 or opponent_tg_user_id <= 0:
            return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            duel = await create_duel(
                session,
                tournament_id=int(tournament.id),
                challenger_tg_user_id=int(tg_user_id),
                opponent_tg_user_id=int(opponent_tg_user_id),
                match_id=int(match_id),
                challenger_pred_home=int(pred_home),
                challenger_pred_away=int(pred_away),
            )
            try:
                await send_new_duel_challenge_push(_get_notify_bot(), session, duel_id=int(duel.id))
            except Exception:
                logger.exception("miniapp duels_challenge notify failed")
            await session.commit()

        return web.json_response(
            {
                "ok": True,
                "duel_id": int(duel.id),
                "status": str(duel.status),
            }
        )
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        logger.exception("miniapp duels_challenge error")
        return web.json_response(
            {"ok": False, "error": "duels_challenge_failed", "reason": str(e), "signature_checked": True},
            status=500,
        )


async def duels_respond(request: web.Request) -> web.Response:
    try:
        auth_result = _extract_verified_user(request)
        if auth_result[0] is None:
            return auth_result[1]
        _payload, user = auth_result
        tg_user_id = user.get("id") if isinstance(user, dict) else None
        if not tg_user_id:
            return web.json_response({"ok": False, "error": "user_not_found_in_init_data"}, status=400)

        body = await request.json()
        duel_id = int(body.get("duel_id") or 0)
        action = str(body.get("action") or "").strip().lower()
        if duel_id <= 0 or action not in ("accept", "decline"):
            return web.json_response({"ok": False, "error": "invalid_payload"}, status=400)
        pred_home = body.get("pred_home")
        pred_away = body.get("pred_away")

        async with SessionLocal() as session:
            duel = await respond_duel(
                session,
                duel_id=int(duel_id),
                responder_tg_user_id=int(tg_user_id),
                accept=action == "accept",
                pred_home=int(pred_home) if pred_home is not None else None,
                pred_away=int(pred_away) if pred_away is not None else None,
            )
            if action == "accept" and str(duel.status) == "accepted":
                try:
                    await send_duel_accepted_push(_get_notify_bot(), session, duel_id=int(duel.id))
                except Exception:
                    logger.exception("miniapp duels_respond notify failed")
            await session.commit()
        return web.json_response(
            {
                "ok": True,
                "duel_id": int(duel.id),
                "status": str(duel.status),
            }
        )
    except ValueError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)
    except Exception as e:
        logger.exception("miniapp duels_respond error")
        return web.json_response(
            {"ok": False, "error": "duels_respond_failed", "reason": str(e), "signature_checked": True},
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

        requested_round_raw = (request.query.get("round") or "").strip()
        requested_round = int(requested_round_raw) if requested_round_raw.isdigit() else None

        async with SessionLocal() as session:
            tournament = await _resolve_tournament(session, int(tg_user_id), requested_code=request.query.get("t"))
            await session.commit()

        if (tournament.code or "").strip().upper() == DEFAULT_TOURNAMENT_CODE:
            rows, meta = await build_active_stage_league_table(int(tg_user_id))
            if meta is None:
                return web.json_response(
                    {
                        "ok": True,
                        "trusted": True,
                        "tournament_code": tournament.code,
                        "tournament_name": tournament.name,
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
                    "tournament_code": tournament.code,
                    "tournament_name": tournament.name,
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
                            "tg_user_id": int(r.get("tg_user_id", 0)),
                            "place": int(r.get("place", 0)),
                            "name": str(r.get("name", "")),
                            "total": int(r.get("total", 0)),
                            "exact": int(r.get("exact", 0)),
                            "diff": int(r.get("diff", 0)),
                            "outcome": int(r.get("outcome", 0)),
                            "pred_total": int(r.get("pred_total", 0)),
                            "hits": int(r.get("hits", 0)),
                            "hit_rate": float(r.get("hit_rate", 0.0)),
                            "missed_matches": int(r.get("missed_matches", 0)),
                        }
                        for r in rows
                    ],
                }
            )

        if requested_round is not None and (
            int(requested_round) < int(tournament.round_min) or int(requested_round) > int(tournament.round_max)
        ):
            requested_round = None

        async with SessionLocal() as session:
            rows, participants = await _build_overall_table_rows(
                session,
                int(tournament.id),
                round_number=requested_round,
            )

        if requested_round is not None:
            stage_name = display_round_name(tournament.code, int(requested_round))
        else:
            stage_name = "Общая таблица"

        return web.json_response(
            {
                "ok": True,
                "trusted": True,
                "tournament_code": tournament.code,
                "tournament_name": tournament.name,
                "has_table": True,
                "season_name": tournament.name,
                "stage_name": stage_name,
                "stage_round_min": int(tournament.round_min),
                "stage_round_max": int(tournament.round_max),
                "league_name": "Общий зачёт",
                "participants": int(participants),
                "user_place": next((int(r["place"]) for r in rows if int(r["tg_user_id"]) == int(tg_user_id)), None),
                "rows": rows,
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
    app.router.add_get("/api/miniapp/tournaments", tournaments_list)
    app.router.add_post("/api/miniapp/tournament/select", tournament_select)
    app.router.add_get("/api/miniapp/profile", profile)
    app.router.add_get("/api/miniapp/predictions/current", predictions_current)
    app.router.add_get("/api/miniapp/table/current", table_current)
    app.router.add_get("/api/miniapp/predict/current", predict_current)
    app.router.add_post("/api/miniapp/predict/set", predict_set)
    app.router.add_get("/api/miniapp/longterm/current", longterm_current)
    app.router.add_post("/api/miniapp/longterm/set", longterm_set)
    app.router.add_get("/api/miniapp/duels/current", duels_current)
    app.router.add_post("/api/miniapp/duels/challenge", duels_challenge)
    app.router.add_post("/api/miniapp/duels/respond", duels_respond)
    app.router.add_get("/api/miniapp/admin/rounds", admin_rounds)
    app.router.add_get("/api/miniapp/admin/results/current", admin_results_current)
    app.router.add_post("/api/miniapp/admin/result/set", admin_result_set)
    app.router.add_post("/api/miniapp/admin/recalc_round", admin_recalc_round)
    return app


async def run_miniapp_api_forever() -> None:
    host = os.getenv("MINIAPP_API_HOST", "0.0.0.0")
    port_raw = os.getenv("MINIAPP_API_PORT", "8081")
    try:
        port = int(port_raw)
    except ValueError:
        port = 8081

    await init_db()
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
