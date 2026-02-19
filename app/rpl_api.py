from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp


@dataclass
class RplFixture:
    api_fixture_id: int
    round_number: int
    round_str: str
    home_team: str
    away_team: str
    start_time_utc: datetime
    status_short: str
    home_goals: Optional[int]
    away_goals: Optional[int]


class ApiFootballClient:
    """
    API-Football (API-SPORTS) client.
    Base URL default: https://v3.football.api-sports.io
    Auth header: x-apisports-key
    """

    def __init__(self, api_key: str, base_url: str = "https://v3.football.api-sports.io") -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _headers(self) -> dict[str, str]:
        return {"x-apisports-key": self.api_key}

    async def _get(self, session: aiohttp.ClientSession, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with session.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def resolve_rpl_league_and_season(self, session: aiohttp.ClientSession) -> tuple[int, int]:
        """
        Resolve Russian Premier League league_id + current season year.
        """
        data = await self._get(session, "/leagues", params={"country": "Russia", "name": "Premier League"})
        resp = data.get("response") or []
        if not resp:
            raise RuntimeError("API-Football: не нашёл лигу Russia / Premier League")

        item = resp[0]
        league_id = int(item["league"]["id"])

        data2 = await self._get(session, "/leagues", params={"id": league_id, "current": "true"})
        resp2 = data2.get("response") or []
        seasons = (resp2[0].get("seasons") if resp2 else item.get("seasons")) or []

        current = None
        for s in seasons:
            if s.get("current") is True:
                current = s
                break
        if current is None and seasons:
            current = seasons[-1]

        if not current or "year" not in current:
            raise RuntimeError("API-Football: не смог определить текущий сезон (year)")

        season_year = int(current["year"])
        return league_id, season_year

    async def get_rounds(self, session: aiohttp.ClientSession, league_id: int, season_year: int) -> list[str]:
        data = await self._get(session, "/fixtures/rounds", params={"league": league_id, "season": season_year})
        return list(data.get("response") or [])

    @staticmethod
    def _match_round_number(round_str: str, round_number: int) -> bool:
        """
        Проверяем, что строка тура содержит нужный номер.
        Примеры round_str: "Regular Season - 1", "Regular Season - Round 1", "Matchday 1" и т.п.
        """
        if not round_str:
            return False
        return re.search(rf"\b{round_number}\b", round_str) is not None

    @staticmethod
    def find_round_string(rounds: list[str], round_number: int) -> Optional[str]:
        if not rounds:
            return None
        # 1) чаще всего "... - 1" в конце
        for r in rounds:
            if re.search(rf"[-\s]{round_number}\s*$", r):
                return r
        # 2) любой вариант, где есть число тура как отдельное слово
        for r in rounds:
            if ApiFootballClient._match_round_number(r, round_number):
                return r
        return None

    def _parse_fixture_item(self, it: dict[str, Any], round_number: int) -> Optional[RplFixture]:
        fx = it.get("fixture", {})
        teams = it.get("teams", {})
        goals = it.get("goals", {})
        league = it.get("league", {})

        api_fixture_id = fx.get("id")
        if api_fixture_id is None:
            return None

        date_str = fx.get("date")
        if not date_str:
            return None

        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        start_utc = dt.astimezone(timezone.utc)

        status_short = (fx.get("status") or {}).get("short") or ""
        home = (teams.get("home") or {}).get("name") or ""
        away = (teams.get("away") or {}).get("name") or ""
        home_goals = goals.get("home")
        away_goals = goals.get("away")
        round_str = league.get("round") or ""

        return RplFixture(
            api_fixture_id=int(api_fixture_id),
            round_number=round_number,
            round_str=round_str,
            home_team=home,
            away_team=away,
            start_time_utc=start_utc,
            status_short=status_short,
            home_goals=None if home_goals is None else int(home_goals),
            away_goals=None if away_goals is None else int(away_goals),
        )

    async def _fixtures_fallback_collect(
        self,
        session: aiohttp.ClientSession,
        league_id: int,
        season_year: int,
        *,
        last_n: int = 300,
        next_n: int = 300,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Фолбэк, когда /fixtures/rounds возвращает пусто:
        берём last и next матчи, собираем возможные round_str из league.round.
        """
        items: list[dict[str, Any]] = []

        # последние матчи (для ранних туров сезона)
        d_last = await self._get(session, "/fixtures", params={"league": league_id, "season": season_year, "last": last_n})
        items += list(d_last.get("response") or [])

        # ближайшие матчи (для будущих туров)
        d_next = await self._get(session, "/fixtures", params={"league": league_id, "season": season_year, "next": next_n})
        items += list(d_next.get("response") or [])

        # уникальные round strings из полученных матчей
        round_set = []
        seen = set()
        for it in items:
            r = (it.get("league") or {}).get("round")
            if r and r not in seen:
                seen.add(r)
                round_set.append(r)

        return items, round_set

    async def get_fixtures_by_round(
        self,
        session: aiohttp.ClientSession,
        league_id: int,
        season_year: int,
        round_number: int,
    ) -> tuple[list[RplFixture], dict[str, Any]]:
        rounds = await self.get_rounds(session, league_id, season_year)
        target_round = self.find_round_string(rounds, round_number)

        debug_info: dict[str, Any] = {
            "league_id": league_id,
            "season_year": season_year,
            "rounds_count": len(rounds),
            "target_round": target_round,
            "rounds_head": rounds[:5],
            "rounds_tail": rounds[-5:] if len(rounds) > 5 else rounds,
        }

        # Нормальный путь: rounds есть
        if target_round:
            data = await self._get(
                session,
                "/fixtures",
                params={"league": league_id, "season": season_year, "round": target_round},
            )
            resp = data.get("response") or []
            out: list[RplFixture] = []
            for it in resp:
                fx = self._parse_fixture_item(it, round_number)
                if fx:
                    fx.round_str = target_round
                    out.append(fx)
            out.sort(key=lambda x: x.start_time_utc)
            return out, debug_info

        # Фолбэк: rounds пустые или не нашли нужный round-string
        items, discovered_rounds = await self._fixtures_fallback_collect(session, league_id, season_year)
        debug_info["fallback_discovered_rounds_head"] = discovered_rounds[:10]
        debug_info["fallback_discovered_rounds_count"] = len(discovered_rounds)

        out2: list[RplFixture] = []
        for it in items:
            rstr = (it.get("league") or {}).get("round") or ""
            if not self._match_round_number(rstr, round_number):
                continue
            fx = self._parse_fixture_item(it, round_number)
            if fx:
                out2.append(fx)

        out2.sort(key=lambda x: x.start_time_utc)
        return out2, debug_info


async def fetch_rpl_round(round_number: int) -> tuple[list[RplFixture], dict[str, Any]]:
    api_key = os.getenv("FOOTBALL_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FOOTBALL_API_KEY не задан в env")

    base_url = os.getenv("FOOTBALL_API_BASE_URL", "https://v3.football.api-sports.io").strip()
    client = ApiFootballClient(api_key=api_key, base_url=base_url)

    async with aiohttp.ClientSession() as session:
        league_id, season_year = await client.resolve_rpl_league_and_season(session)
        return await client.get_fixtures_by_round(session, league_id, season_year, round_number)