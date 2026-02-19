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

    async def _get(self, session: aiohttp.ClientSession, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with session.get(
            url,
            headers=self._headers(),
            params=params or {},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            data = await r.json()
            errors = data.get("errors")
            if errors:
                raise RuntimeError(f"API-Football errors: {errors}")
            r.raise_for_status()
            return data

    async def get_status(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        return await self._get(session, "/status")

    @staticmethod
    def _is_russia_country(item: dict[str, Any]) -> bool:
        country = item.get("country") or {}
        name = (country.get("name") or "").lower()
        code = (country.get("code") or "").upper()
        return ("russia" in name) or (code == "RU")

    @staticmethod
    def _looks_like_rpl(item: dict[str, Any]) -> bool:
        league = item.get("league") or {}
        lname = (league.get("name") or "").lower()
        ltype = (league.get("type") or "").lower()
        return ("premier" in lname) and (ltype in ("league", "", None))

    async def resolve_rpl_league_and_season(self, session: aiohttp.ClientSession) -> tuple[int, int]:
        candidates: list[dict[str, Any]] = []

        for params in (
            {"country": "Russia"},
            {"search": "Russia"},
            {"search": "Premier"},
        ):
            try:
                d = await self._get(session, "/leagues", params=params)
                candidates += list(d.get("response") or [])
            except Exception:
                pass

        filtered = [it for it in candidates if self._is_russia_country(it) and self._looks_like_rpl(it)]

        if not filtered:
            rf_leagues = []
            for it in candidates:
                if self._is_russia_country(it):
                    league = it.get("league") or {}
                    rf_leagues.append(league.get("name"))
            raise RuntimeError(f"API-Football: не нашёл РПЛ. Russia leagues seen: {rf_leagues[:15]}")

        item = filtered[0]
        league_id = int((item.get("league") or {}).get("id"))

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
        if not round_str:
            return False
        return re.search(rf"\b{round_number}\b", round_str) is not None

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
        items: list[dict[str, Any]] = []

        d_last = await self._get(session, "/fixtures", params={"league": league_id, "season": season_year, "last": last_n})
        items += list(d_last.get("response") or [])

        d_next = await self._get(session, "/fixtures", params={"league": league_id, "season": season_year, "next": next_n})
        items += list(d_next.get("response") or [])

        round_list = []
        seen = set()
        for it in items:
            r = (it.get("league") or {}).get("round")
            if r and r not in seen:
                seen.add(r)
                round_list.append(r)
        return items, round_list

    async def get_fixtures_by_round(
        self,
        session: aiohttp.ClientSession,
        league_id: int,
        season_year: int,
        round_number: int,
    ) -> tuple[list[RplFixture], dict[str, Any]]:
        rounds = await self.get_rounds(session, league_id, season_year)

        debug_info: dict[str, Any] = {
            "league_id": league_id,
            "season_year": season_year,
            "rounds_count": len(rounds),
            "target_round": None,
            "rounds_head": rounds[:5],
            "rounds_tail": rounds[-5:] if len(rounds) > 5 else rounds,
        }

        if rounds:
            target = None
            for r in rounds:
                if self._match_round_number(r, round_number):
                    target = r
                    break
            debug_info["target_round"] = target

            if target:
                data = await self._get(session, "/fixtures", params={"league": league_id, "season": season_year, "round": target})
                resp = data.get("response") or []
                out = []
                for it in resp:
                    fx = self._parse_fixture_item(it, round_number)
                    if fx:
                        fx.round_str = target
                        out.append(fx)
                out.sort(key=lambda x: x.start_time_utc)
                return out, debug_info

        items, discovered_rounds = await self._fixtures_fallback_collect(session, league_id, season_year)
        debug_info["fallback_discovered_rounds_count"] = len(discovered_rounds)
        debug_info["fallback_discovered_rounds_head"] = discovered_rounds[:10]

        out2 = []
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


async def fetch_api_status() -> dict[str, Any]:
    """
    /status — проверить, что ключ рабочий, какой план/лимиты/использование.
    """
    api_key = os.getenv("FOOTBALL_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FOOTBALL_API_KEY не задан в env")

    base_url = os.getenv("FOOTBALL_API_BASE_URL", "https://v3.football.api-sports.io").strip()
    client = ApiFootballClient(api_key=api_key, base_url=base_url)

    async with aiohttp.ClientSession() as session:
        return await client.get_status(session)