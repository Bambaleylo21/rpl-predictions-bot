from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import aiohttp

logger = logging.getLogger(__name__)

API_BASE_URL = "https://v3.football.api-sports.io"

# Статусы API-Football (fixture.status.short), см. документацию v3.
FINISHED_STATUSES = {"FT", "AET", "PEN"}


def _api_key() -> str:
    return os.getenv("FOOTBALL_API_KEY", "").strip()


@dataclass
class ApiFixture:
    fixture_id: int
    round_raw: str
    round_number: int | None
    kickoff_msk: datetime
    status_short: str
    home_team: str
    away_team: str
    home_score: int | None
    away_score: int | None

    @property
    def is_finished(self) -> bool:
        return self.status_short in FINISHED_STATUSES


def _parse_round_number(round_raw: str) -> int | None:
    if not round_raw:
        return None
    m = re.search(r"(\d+)\s*$", round_raw.strip())
    return int(m.group(1)) if m else None


def _utc_to_msk_naive(dt: datetime) -> datetime:
    """Приводит дату матча к naive-времени в МСК (UTC+3), как принято в проекте."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt + timedelta(hours=3)


async def fetch_league_fixtures(league_id: int, season: int) -> list[ApiFixture]:
    """
    Запрашивает у API-Football весь список матчей турнира за сезон
    (расписание + результаты уже сыгранных).
    """
    api_key = _api_key()
    if not api_key:
        logger.warning("[football_api] FOOTBALL_API_KEY is not set, skipping fetch")
        return []

    url = f"{API_BASE_URL}/fixtures"
    params = {"league": str(league_id), "season": str(season)}
    headers = {"x-apisports-key": api_key}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(
                        "[football_api] fixtures request failed: status=%s body=%s",
                        resp.status,
                        body[:500],
                    )
                    return []
                data = await resp.json()
    except Exception:
        logger.exception("[football_api] fixtures request raised an exception")
        return []

    errors = data.get("errors")
    if errors:
        logger.error("[football_api] API returned errors: %r", errors)

    out: list[ApiFixture] = []
    for item in data.get("response", []) or []:
        try:
            fixture = item.get("fixture") or {}
            league = item.get("league") or {}
            teams = item.get("teams") or {}
            goals = item.get("goals") or {}

            fixture_id = int(fixture.get("id"))

            date_str = fixture.get("date")
            if not date_str:
                continue
            kickoff_utc = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
            kickoff_msk = _utc_to_msk_naive(kickoff_utc)

            status_short = str(((fixture.get("status") or {}).get("short")) or "NS")
            round_raw = str(league.get("round") or "")
            round_number = _parse_round_number(round_raw)

            home_team = str((teams.get("home") or {}).get("name") or "").strip()
            away_team = str((teams.get("away") or {}).get("name") or "").strip()

            home_score = goals.get("home")
            away_score = goals.get("away")

            out.append(
                ApiFixture(
                    fixture_id=fixture_id,
                    round_raw=round_raw,
                    round_number=round_number,
                    kickoff_msk=kickoff_msk,
                    status_short=status_short,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=int(home_score) if home_score is not None else None,
                    away_score=int(away_score) if away_score is not None else None,
                )
            )
        except Exception:
            logger.exception("[football_api] failed to parse fixture item: %r", item)
            continue

    return out
