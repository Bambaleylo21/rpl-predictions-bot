import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.apisport_client import ApiSportClient
from app.models import Match, Setting

logger = logging.getLogger(__name__)

UTC = timezone.utc
MSK = timezone(timedelta(hours=3))


def _to_naive_msk(dt: datetime) -> datetime:
    """
    Приводим любой datetime к МСК и убираем tzinfo.
    В БД храним наивное московское время, чтобы дедлайны прогнозов
    и отображение матчей были единообразными во всём боте.
    """
    if dt.tzinfo is None:
        # если tz нет — считаем, что это уже МСК
        return dt
    return dt.astimezone(MSK).replace(tzinfo=None)


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _parse_dt(value: Any) -> Optional[datetime]:
    """
    ISO-строка или epoch -> datetime (НАИВНЫЙ МСК, без tzinfo).
    """
    if value is None:
        return None

    # epoch seconds
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=UTC)
        return _to_naive_msk(dt)

    if isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            # если tz не дали — считаем, что это уже МСК
            if dt.tzinfo is None:
                return dt
            return _to_naive_msk(dt)
        except Exception:
            return None

    return None


def _extract_teams(m: dict[str, Any]) -> tuple[str, str]:
    home = ""
    away = ""

    teams = m.get("teams")
    if isinstance(teams, dict):
        h = teams.get("home")
        a = teams.get("away")
        if isinstance(h, dict):
            home = str(h.get("name") or h.get("title") or "")
        if isinstance(a, dict):
            away = str(a.get("name") or a.get("title") or "")

    if not home and isinstance(m.get("homeTeam"), dict):
        home = str(m["homeTeam"].get("name") or m["homeTeam"].get("title") or "")
    if not away and isinstance(m.get("awayTeam"), dict):
        away = str(m["awayTeam"].get("name") or m["awayTeam"].get("title") or "")

    return home.strip(), away.strip()


def _extract_round_number(m: dict[str, Any]) -> Optional[int]:
    for key in ("round", "roundNumber", "tour", "week", "matchday"):
        v = _safe_int(m.get(key))
        if v is not None:
            return v

    rr = m.get("tournamentRound") or m.get("roundInfo")
    if isinstance(rr, dict):
        for key in ("number", "round", "tour", "week", "matchday"):
            v = _safe_int(rr.get(key))
            if v is not None:
                return v

    return None


def _extract_kickoff_naive_msk(m: dict[str, Any]) -> Optional[datetime]:
    for key in ("dateTime", "kickoff", "startTime", "startAt", "scheduledAt", "date"):
        dt = _parse_dt(m.get(key))
        if dt:
            return dt
    return None


def _extract_status(m: dict[str, Any]) -> str:
    s = m.get("status")
    if isinstance(s, str):
        return s.lower().strip()
    if isinstance(s, dict):
        code = s.get("code") or s.get("slug") or s.get("status")
        if isinstance(code, str):
            return code.lower().strip()
    return ""


def _extract_score(m: dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
    for key in ("score", "result", "fullTime", "ftScore"):
        block = m.get(key)
        if isinstance(block, dict):
            h = block.get("home") or block.get("homeScore")
            a = block.get("away") or block.get("awayScore")
            hi = _safe_int(h)
            ai = _safe_int(a)
            if hi is not None and ai is not None:
                return hi, ai

    if "homeScore" in m and "awayScore" in m:
        return _safe_int(m.get("homeScore")), _safe_int(m.get("awayScore"))

    hs = m.get("homeScore")
    aws = m.get("awayScore")
    if isinstance(hs, dict) and isinstance(aws, dict):
        return _safe_int(hs.get("current")), _safe_int(aws.get("current"))

    return None, None


def _iter_matches(api_response: Any) -> Iterable[dict[str, Any]]:
    if isinstance(api_response, list):
        return api_response
    if isinstance(api_response, dict):
        for key in ("matches", "data", "items"):
            v = api_response.get(key)
            if isinstance(v, list):
                return v
    return []


@dataclass
class SyncConfig:
    tournament_id: int
    season_id: int
    fixtures_every_seconds: int = 21600  # 6h
    results_every_seconds: int = 120     # 2m
    lookback_days: int = 7
    lookahead_days: int = 30


def load_sync_config() -> SyncConfig:
    def need(name: str) -> str:
        v = os.getenv(name, "").strip()
        if not v:
            raise RuntimeError(f"{name} is not set")
        return v

    tournament_id = int(need("RPL_TOURNAMENT_ID"))
    season_id = int(need("RPL_SEASON_ID"))

    fixtures_every = int(os.getenv("SYNC_FIXTURES_EVERY_SECONDS", "21600"))
    results_every = int(os.getenv("SYNC_RESULTS_EVERY_SECONDS", "120"))
    lookback = int(os.getenv("SYNC_LOOKBACK_DAYS", "7"))
    lookahead = int(os.getenv("SYNC_LOOKAHEAD_DAYS", "30"))

    return SyncConfig(
        tournament_id=tournament_id,
        season_id=season_id,
        fixtures_every_seconds=fixtures_every,
        results_every_seconds=results_every,
        lookback_days=lookback,
        lookahead_days=lookahead,
    )


async def get_setting(session: AsyncSession, key: str) -> Optional[str]:
    res = await session.execute(select(Setting).where(Setting.key == key))
    obj = res.scalar_one_or_none()
    return obj.value if obj else None


async def get_tournament_window(session: AsyncSession) -> tuple[date, date]:
    s = await get_setting(session, "TOURNAMENT_START_DATE")
    e = await get_setting(session, "TOURNAMENT_END_DATE")

    if not s:
        s = os.getenv("TOURNAMENT_START_DATE", "").strip()
    if not e:
        e = os.getenv("TOURNAMENT_END_DATE", "").strip()

    if not s or not e:
        raise RuntimeError("Tournament window is not set: TOURNAMENT_START_DATE / TOURNAMENT_END_DATE")

    start_date = datetime.fromisoformat(s).date()
    end_date = datetime.fromisoformat(e).date()
    return start_date, end_date


async def upsert_fixtures_for_dates(
    session: AsyncSession,
    client: ApiSportClient,
    cfg: SyncConfig,
    day_from: date,
    day_to: date,
) -> int:
    inserted_or_updated = 0
    cur = day_from

    while cur <= day_to:
        payload = await client.list_matches(
            date=cur.isoformat(),
            tournamentId=cfg.tournament_id,
            seasonId=cfg.season_id,
        )

        for m in _iter_matches(payload):
            ext_id = _safe_int(m.get("matchId") or m.get("id"))
            if ext_id is None:
                continue

            home, away = _extract_teams(m)
            kickoff = _extract_kickoff_naive_msk(m)
            round_number = _extract_round_number(m)

            if round_number is None:
                continue

            if kickoff is None:
                # если API не дал время — ставим 00:00 UTC (без tzinfo)
                kickoff = datetime.combine(cur, datetime.min.time())

            q = await session.execute(select(Match).where(Match.api_fixture_id == ext_id))
            row = q.scalar_one_or_none()

            if row is None:
                row = Match(
                    round_number=round_number,
                    home_team=home,
                    away_team=away,
                    kickoff_time=kickoff,          # НАИВНЫЙ МСК
                    api_fixture_id=ext_id,
                    source="apisport",
                )
                session.add(row)
                inserted_or_updated += 1
            else:
                changed = False
                if row.source != "apisport":
                    row.source = "apisport"
                    changed = True
                if row.round_number != round_number:
                    row.round_number = round_number
                    changed = True
                if home and row.home_team != home:
                    row.home_team = home
                    changed = True
                if away and row.away_team != away:
                    row.away_team = away
                    changed = True
                if kickoff and row.kickoff_time != kickoff:
                    row.kickoff_time = kickoff
                    changed = True

                if changed:
                    inserted_or_updated += 1

        await session.commit()
        cur += timedelta(days=1)

    return inserted_or_updated


async def sync_finished_results(
    session: AsyncSession,
    client: ApiSportClient,
    cfg: SyncConfig,
    recalc_points_for_match_in_session,
) -> int:
    updated = 0

    start_date, end_date = await get_tournament_window(session)

    # Все границы времени делаем НАИВНЫМИ МСК
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    now_dt = (datetime.utcnow() + timedelta(hours=3)).replace(tzinfo=None)  # наивный МСК

    q = await session.execute(
        select(Match).where(
            Match.source == "apisport",
            Match.api_fixture_id.isnot(None),
            Match.kickoff_time >= start_dt,
            Match.kickoff_time <= end_dt,
            Match.kickoff_time < now_dt,
        )
    )
    matches = q.scalars().all()

    for dbm in matches:
        if dbm.home_score is not None and dbm.away_score is not None:
            continue

        ext_id = int(dbm.api_fixture_id)
        payload = await client.get_match(ext_id)

        status = _extract_status(payload)
        is_final = status in {"finished", "ended", "ft", "final", "completed"}

        h, a = _extract_score(payload)
        if is_final and h is not None and a is not None:
            dbm.home_score = h
            dbm.away_score = a
            await session.commit()

            await recalc_points_for_match_in_session(session, dbm.id)
            updated += 1

    return updated


async def run_sync_loops(session_factory, recalc_points_for_match_in_session) -> None:
    cfg = load_sync_config()
    client = ApiSportClient.from_env()

    async def fixtures_loop():
        while True:
            try:
                async with session_factory() as session:
                    start_date, end_date = await get_tournament_window(session)

                    today = (datetime.utcnow() + timedelta(hours=3)).date()
                    day_from = max(start_date, today - timedelta(days=cfg.lookback_days))
                    day_to = min(end_date, today + timedelta(days=cfg.lookahead_days))

                    n = await upsert_fixtures_for_dates(session, client, cfg, day_from, day_to)
                    logger.info(f"[apisport] fixtures sync ok: {n} inserted/updated ({day_from}..{day_to})")
            except Exception:
                logger.exception("[apisport] fixtures sync failed")

            await asyncio.sleep(cfg.fixtures_every_seconds)

    async def results_loop():
        while True:
            try:
                async with session_factory() as session:
                    n = await sync_finished_results(session, client, cfg, recalc_points_for_match_in_session)
                    if n:
                        logger.info(f"[apisport] results sync ok: {n} matches updated")
            except Exception:
                logger.exception("[apisport] results sync failed")

            await asyncio.sleep(cfg.results_every_seconds)

    asyncio.create_task(fixtures_loop())
    asyncio.create_task(results_loop())
