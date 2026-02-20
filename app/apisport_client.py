import os
from typing import Any, Optional

import aiohttp


class ApiSportClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.api-sport.ru/v2",
        timeout_seconds: int = 20,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    @classmethod
    def from_env(cls) -> "ApiSportClient":
        api_key = os.getenv("APISPORT_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("APISPORT_API_KEY is not set")
        base_url = os.getenv("APISPORT_BASE_URL", "https://api.api-sport.ru/v2").strip()
        return cls(api_key=api_key, base_url=base_url)

    async def get_json(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Authorization": self.api_key}

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    raise RuntimeError(f"ApiSport HTTP {resp.status}: {text[:600]}")
                return await resp.json()

    async def list_matches(self, **filters: Any) -> Any:
        return await self.get_json("/football/matches", params=filters)

    async def get_match(self, match_id: int) -> Any:
        return await self.get_json(f"/football/matches/{match_id}")