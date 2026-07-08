from __future__ import annotations

import logging
import os
import time
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

API_BASE_URL = "https://v3.football.api-sports.io"

# Простой in-memory кэш (endpoint + параметры -> (истекает_в, данные)).
# Матч-центр запрашивает данные по требованию (когда участник открыл конкретный
# матч), а не для всех матчей сразу в фоне — это ключевое отличие от rpl_sync.py
# и бережёт дневной лимит запросов. Кэш живёт в памяти процесса и не требует
# изменений схемы БД, поэтому не может затронуть данные других турниров.
_cache: dict[str, tuple[float, Any]] = {}


def _api_key() -> str:
    return os.getenv("FOOTBALL_API_KEY", "").strip()


async def _api_get(path: str, params: dict[str, str], cache_key: str, ttl_seconds: int) -> dict | None:
    now = time.time()
    cached = _cache.get(cache_key)
    if cached and cached[0] > now:
        return cached[1]

    api_key = _api_key()
    if not api_key:
        logger.warning("[match_center] FOOTBALL_API_KEY is not set, skipping %s", path)
        return None

    url = f"{API_BASE_URL}{path}"
    headers = {"x-apisports-key": api_key}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(
                        "[match_center] %s failed: status=%s body=%s", path, resp.status, body[:300]
                    )
                    return None
                data = await resp.json()
    except Exception:
        logger.exception("[match_center] %s request raised an exception", path)
        return None

    _cache[cache_key] = (now + ttl_seconds, data)
    return data


async def fetch_standings(league_id: int, season: int) -> list[dict[str, Any]]:
    """Официальная турнирная таблица РПЛ. Меняется редко — кэш на 3 часа."""
    data = await _api_get(
        "/standings",
        {"league": str(league_id), "season": str(season)},
        cache_key=f"standings:{league_id}:{season}",
        ttl_seconds=3 * 3600,
    )
    if not data:
        return []
    try:
        league = (data.get("response") or [{}])[0].get("league") or {}
        table = (league.get("standings") or [[]])[0]
    except Exception:
        logger.exception("[match_center] unexpected standings payload shape")
        return []

    out: list[dict[str, Any]] = []
    for row in table or []:
        team = row.get("team") or {}
        all_stats = row.get("all") or {}
        out.append(
            {
                "team_name": str(team.get("name") or ""),
                "team_id": team.get("id"),
                "rank": row.get("rank"),
                "points": row.get("points"),
                "played": all_stats.get("played"),
            }
        )
    return out


async def fetch_team_id_map(league_id: int, season: int) -> dict[str, int]:
    """Соответствие "имя команды из API" -> "id команды в API", нужно для запроса личных встреч."""
    data = await _api_get(
        "/teams",
        {"league": str(league_id), "season": str(season)},
        cache_key=f"teams:{league_id}:{season}",
        ttl_seconds=24 * 3600,
    )
    if not data:
        return {}
    out: dict[str, int] = {}
    for item in data.get("response", []) or []:
        team = item.get("team") or {}
        name = str(team.get("name") or "").strip()
        tid = team.get("id")
        if name and tid:
            out[name] = int(tid)
    return out


async def fetch_h2h(team1_id: int, team2_id: int, last: int = 5) -> list[dict[str, Any]]:
    """Последние личные встречи двух команд (по всем турнирам, не только РПЛ)."""
    data = await _api_get(
        "/fixtures/headtohead",
        {"h2h": f"{team1_id}-{team2_id}", "last": str(last)},
        cache_key=f"h2h:{team1_id}:{team2_id}:{last}",
        ttl_seconds=6 * 3600,
    )
    if not data:
        return []
    out: list[dict[str, Any]] = []
    for item in data.get("response", []) or []:
        fixture = item.get("fixture") or {}
        teams = item.get("teams") or {}
        goals = item.get("goals") or {}
        date_str = fixture.get("date")
        out.append(
            {
                "date": str(date_str)[:10] if date_str else None,
                "home_team": (teams.get("home") or {}).get("name"),
                "away_team": (teams.get("away") or {}).get("name"),
                "home_score": goals.get("home"),
                "away_score": goals.get("away"),
            }
        )
    return out


async def fetch_lineups(fixture_id: int) -> dict[str, Any] | None:
    """Стартовые составы по конкретному матчу. Появляются у API примерно за час до игры.

    Кэш короткий (10 минут), так как ближе к матчу состав может уточняться.
    """
    data = await _api_get(
        "/fixtures/lineups",
        {"fixture": str(fixture_id)},
        cache_key=f"lineups:{fixture_id}",
        ttl_seconds=10 * 60,
    )
    if not data:
        return None
    response = data.get("response") or []
    if not response:
        return None

    out: dict[str, Any] = {}
    for side in response:
        team = side.get("team") or {}
        name = str(team.get("name") or "")
        formation = side.get("formation")
        starters = [
            str((p.get("player") or {}).get("name") or "").strip()
            for p in side.get("startXI") or []
        ]
        out[name] = {"formation": formation, "starters": [s for s in starters if s]}
    return out or None


async def fetch_predictions(fixture_id: int) -> dict[str, Any] | None:
    """Алгоритмический прогноз API-Football по конкретному матчу (% на П1/Х/П2).

    Это не наш прогноз и не прогноз участников — просто статистическая оценка
    от поставщика данных. Кэш на 6 часов, обычно не меняется в течение дня.
    """
    data = await _api_get(
        "/predictions",
        {"fixture": str(fixture_id)},
        cache_key=f"predictions:{fixture_id}",
        ttl_seconds=6 * 3600,
    )
    if not data:
        return None
    response = data.get("response") or []
    if not response:
        return None
    try:
        predictions = (response[0] or {}).get("predictions") or {}
        percent = predictions.get("percent") or {}

        def _pct(raw: Any) -> int | None:
            if raw is None:
                return None
            s = str(raw).strip().rstrip("%")
            try:
                return int(round(float(s)))
            except (TypeError, ValueError):
                return None

        home_pct = _pct(percent.get("home"))
        draw_pct = _pct(percent.get("draw"))
        away_pct = _pct(percent.get("away"))
        if home_pct is None and draw_pct is None and away_pct is None:
            return None
        return {"home_pct": home_pct, "draw_pct": draw_pct, "away_pct": away_pct}
    except Exception:
        logger.exception("[match_center] unexpected predictions payload shape")
        return None


async def fetch_odds(fixture_id: int) -> dict[str, Any] | None:
    """Коэффициенты одного букмекера на исход матча (П1/Х/П2), просто как
    ещё один статистический ориентир — без ссылок на сами ставки.

    Кэш на 1 час: предматчевые коэффициенты могут немного двигаться.
    """
    data = await _api_get(
        "/odds",
        {"fixture": str(fixture_id)},
        cache_key=f"odds:{fixture_id}",
        ttl_seconds=3600,
    )
    if not data:
        return None
    response = data.get("response") or []
    if not response:
        return None
    try:
        bookmakers = (response[0] or {}).get("bookmakers") or []
        for bookmaker in bookmakers:
            for bet in bookmaker.get("bets") or []:
                bet_name = str(bet.get("name") or "").strip().lower()
                if bet_name not in ("match winner", "1x2"):
                    continue
                values = {str(v.get("value") or "").strip().lower(): v.get("odd") for v in bet.get("values") or []}
                home_odd = values.get("home")
                draw_odd = values.get("draw")
                away_odd = values.get("away")
                if home_odd or draw_odd or away_odd:
                    return {
                        "bookmaker": str(bookmaker.get("name") or ""),
                        "home_odd": home_odd,
                        "draw_odd": draw_odd,
                        "away_odd": away_odd,
                    }
        return None
    except Exception:
        logger.exception("[match_center] unexpected odds payload shape")
        return None
