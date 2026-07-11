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


async def fetch_injuries(fixture_id: int) -> list[dict[str, Any]]:
    """Травмы и дисквалификации игроков, которые пропускают конкретный матч.

    Кэш на 6 часов: список обычно уточняется в течение недели перед туром,
    но не меняется резко от минуты к минуте.
    """
    data = await _api_get(
        "/injuries",
        {"fixture": str(fixture_id)},
        cache_key=f"injuries:{fixture_id}",
        ttl_seconds=6 * 3600,
    )
    if not data:
        return []
    out: list[dict[str, Any]] = []
    for item in data.get("response", []) or []:
        player = item.get("player") or {}
        team = item.get("team") or {}
        name = str(player.get("name") or "").strip()
        if not name:
            continue
        out.append(
            {
                "player_name": name,
                "team_name": str(team.get("name") or ""),
                "type": player.get("type"),
                "reason": player.get("reason"),
            }
        )
    return out


async def fetch_fixture_events(fixture_id: int, ttl_seconds: int = 10 * 60) -> list[dict[str, Any]]:
    """Хронология событий матча: голы, карточки, замены.

    Кэш по умолчанию 10 минут; вызывающий код передаёт короткий TTL
    (например, 90 секунд), пока матч идёт вживую, чтобы события подтягивались
    почти в реальном времени, не тратя лишние запросы на уже завершённые матчи.
    """
    data = await _api_get(
        "/fixtures/events",
        {"fixture": str(fixture_id)},
        cache_key=f"events:{fixture_id}",
        ttl_seconds=ttl_seconds,
    )
    if not data:
        return []
    out: list[dict[str, Any]] = []
    for item in data.get("response", []) or []:
        time_info = item.get("time") or {}
        team = item.get("team") or {}
        player = item.get("player") or {}
        assist = item.get("assist") or {}
        out.append(
            {
                "minute": time_info.get("elapsed"),
                "extra": time_info.get("extra"),
                "team_name": str(team.get("name") or ""),
                "player_name": str(player.get("name") or ""),
                "assist_name": str(assist.get("name") or "").strip() or None,
                "type": item.get("type"),
                "detail": item.get("detail"),
            }
        )
    return out


async def fetch_league_coverage(league_id: int) -> list[dict[str, Any]]:
    """Технический дебаг-хелпер (не используется на боевых экранах): какие типы
    данных API-Football реально покрывает для этой лиги, по сезонам — coverage
    может отличаться от сезона к сезону, поэтому смотрим все сразу, включая
    прошлые. Кэш длинный (24ч) — справочная информация, не меняется на лету.
    """
    data = await _api_get(
        "/leagues",
        {"id": str(league_id)},
        cache_key=f"coverage:{league_id}",
        ttl_seconds=24 * 3600,
    )
    if not data:
        return []
    try:
        entries = (data.get("response") or [{}])[0].get("seasons") or []
    except Exception:
        logger.exception("[match_center] unexpected leagues payload shape")
        return []

    out: list[dict[str, Any]] = []
    for s in entries:
        cov = s.get("coverage") or {}
        fixtures_cov = cov.get("fixtures") or {}
        out.append(
            {
                "year": s.get("year"),
                "current": bool(s.get("current")),
                "coverage": {
                    "fixtures_events": bool(fixtures_cov.get("events")),
                    "fixtures_lineups": bool(fixtures_cov.get("lineups")),
                    "fixtures_statistics_fixtures": bool(fixtures_cov.get("statistics_fixtures")),
                    "fixtures_statistics_players": bool(fixtures_cov.get("statistics_players")),
                    "standings": bool(cov.get("standings")),
                    "players": bool(cov.get("players")),
                    "top_scorers": bool(cov.get("top_scorers")),
                    "top_assists": bool(cov.get("top_assists")),
                    "top_cards": bool(cov.get("top_cards")),
                    "injuries": bool(cov.get("injuries")),
                    "predictions": bool(cov.get("predictions")),
                    "odds": bool(cov.get("odds")),
                },
            }
        )
    out.sort(key=lambda x: int(x.get("year") or 0), reverse=True)
    return out


async def fetch_api_status() -> dict[str, Any] | None:
    """Технический дебаг-хелпер: инфо о нашей подписке (тариф, дневной лимит,
    сколько запросов уже использовано сегодня). Короткий кэш (5 минут) — это
    меняется в течение дня по мере использования.
    """
    data = await _api_get(
        "/status",
        {},
        cache_key="account_status",
        ttl_seconds=5 * 60,
    )
    if not data:
        return None
    try:
        resp = data.get("response") or {}
        subscription = resp.get("subscription") or {}
        requests_info = resp.get("requests") or {}
        return {
            "plan": subscription.get("plan"),
            "requests_current": requests_info.get("current"),
            "requests_limit_day": requests_info.get("limit_day"),
        }
    except Exception:
        logger.exception("[match_center] unexpected status payload shape")
        return None


async def fetch_fixture_statistics(fixture_id: int, ttl_seconds: int = 10 * 60) -> dict[str, dict[str, Any]] | None:
    """Статистика матча (владение мячом, удары, угловые и т.д.) по обеим командам.

    Кэш по умолчанию 10 минут; для матчей, идущих вживую, вызывающий код
    передаёт короткий TTL (например, 90 секунд), см. fetch_fixture_events.
    """
    data = await _api_get(
        "/fixtures/statistics",
        {"fixture": str(fixture_id)},
        cache_key=f"stats:{fixture_id}",
        ttl_seconds=ttl_seconds,
    )
    if not data:
        return None
    response = data.get("response") or []
    if not response:
        return None
    out: dict[str, dict[str, Any]] = {}
    for side in response:
        team = side.get("team") or {}
        name = str(team.get("name") or "")
        stats_map: dict[str, Any] = {}
        for stat in side.get("statistics") or []:
            stype = stat.get("type")
            if stype:
                stats_map[str(stype)] = stat.get("value")
        if name:
            out[name] = stats_map
    return out or None
