from __future__ import annotations

TOURNAMENT_NAME_MAP: dict[str, str] = {
    "Russian Premier League": "РПЛ",
}

TEAM_NAME_MAP: dict[str, str] = {
    "Rostov": "Ростов",
    "Baltika": "Балтика",
    "Pari Nizhniy Novgorod": "Пари НН",
    "Sochi": "Сочи",
    "Orenburg": "Оренбург",
    "Zenit": "Зенит",
    "Krylya Sovetov": "Кр. Советов",
    "Makhachkala D": "Динамо Мхч",
    "Rubin": "Рубин",
    "Krasnodar": "Краснодар",
    "CSKA": "ЦСКА",
    "Moscow D": "Динамо Москва",
    "Lokomotiv M": "Локомотив",
    "Akhmat": "Ахмат",
    "Spartak M": "Спартак",
    "Akron": "Акрон",
}


def display_tournament_name(name: str | None) -> str:
    n = (name or "").strip()
    return TOURNAMENT_NAME_MAP.get(n, n or "РПЛ")


def display_team_name(name: str | None) -> str:
    n = (name or "").strip()
    return TEAM_NAME_MAP.get(n, n)
