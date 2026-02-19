# app/rpl_api.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp


@dataclass
class RplFixture:
    api_fixture_id: int
    round_number: int
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
        async with session.get(url, headers=self._headers(), params=params, timeout=aiohttp.ClientTimeout(total=30)) as r:
            r.raise_for_status()
            return await r.json()

    async def resolve_rpl_league_and_season(self, session: aiohttp.ClientSession) -> tuple[int, int]:
        """
        Resolve Russian Premier League league_id + current season year.
        We avoid hardcoding IDs.
        """
        data = await self._get(
            session,
            "/leagues",
            params={"country": "Russia", "name": "Premier League"},
        )
        resp = data.get("response") or []
        if not resp:
            raise RuntimeError("API-Football: не нашёл лигу Russia / Premier League")

        # choose best match
        # response item: {"league": {...}, "country": {...}, "seasons": [...]}
        item = resp[0]
        league_id = int(item["league"]["id"])

        # get current season
        data2 = await self._get(session, "/leagues", params={"id": league_id, "current": "true"})
        resp2 = data2.get("response") or []
        if not resp2:
            # fallback: use last season in the first response
            seasons = item.get("seasons") or []
        else:
            seasons = resp2[0].get("seasons") or []

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
        data = await self._get(
            session,
            "/fixtures/rounds",
            params={"league": league_id, "season": season_year},
        )
        return list(data.get("response") or [])

    @staticmethod
    def _extract_round_number(round_str: str) -> Optional[int]:
        """
        Typical round string example: "Regular Season - 1"
        We'll parse trailing integer.
        """
        if not round_str:
            return None
        parts = [p.strip() for p in round_str.split("-")]
        if not parts:
            return None
        try:
            return int(parts[-1])
        except Exception:
            return None

    async def get_fixtures_by_round(
        self,
        session: aiohttp.ClientSession,
        league_id: int,
        season_year: int,
        round_number: int,
    ) -> list[RplFixture]:
        rounds = await self.get_rounds(session, league_id, season_year)

        # Find matching round string
        target_round: Optional[str] = None
        for r in rounds:
            n = self._extract_round_number(r)
            if n == round_number:
                target_round = r
                break

        # Fallback: common pattern
        if target_round is None:
            target_round = f"Regular Season - {round_number}"

        data = await self._get(
            session,
            "/fixtures",
            params={"league": league_id, "season": season_year, "round": target_round},
        )

        resp = data.get("response") or []
        out: list[RplFixture] = []
        for it in resp:
            fx = it.get("fixture", {})
            teams = it.get("teams", {})
            goals = it.get("goals", {})

            api_fixture_id = int(fx["id"])
            date_str = fx.get("date")
            if not date_str:
                continue

            # API gives ISO with timezone, parse to aware -> UTC
            # Example: "2026-02-19T17:00:00+00:00"
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            start_utc = dt.astimezone(timezone.utc)

            status_short = (fx.get("status") or {}).get("short") or ""
            home = (teams.get("home") or {}).get("name") or ""
            away = (teams.get("away") or {}).get("name") or ""
            home_goals = goals.get("home")
            away_goals = goals.get("away")

            out.append(
                RplFixture(
                    api_fixture_id=api_fixture_id,
                    round_number=round_number,
                    home_team=home,
                    away_team=away,
                    start_time_utc=start_utc,
                    status_short=status_short,
                    home_goals=None if home_goals is None else int(home_goals),
                    away_goals=None if away_goals is None else int(away_goals),
                )
            )

        # Sort by kickoff
        out.sort(key=lambda x: x.start_time_utc)
        return out


async def fetch_rpl_round(round_number: int) -> list[RplFixture]:
    """
    Convenience wrapper using env vars:
    FOOTBALL_API_KEY (required)
    FOOTBALL_API_BASE_URL (optional)
    """
    api_key = os.getenv("FOOTBALL_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FOOTBALL_API_KEY не задан в env")

    base_url = os.getenv("FOOTBALL_API_BASE_URL", "https://v3.football.api-sports.io").strip()
    client = ApiFootballClient(api_key=api_key, base_url=base_url)

    async with aiohttp.ClientSession() as session:
        league_id, season_year = await client.resolve_rpl_league_and_season(session)
        return await client.get_fixtures_by_round(session, league_id, season_year, round_number)