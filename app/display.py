from __future__ import annotations

TOURNAMENT_NAME_MAP: dict[str, str] = {
    "Russian Premier League": "РПЛ",
    "World Cup 2026": "ЧМ 2026",
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


def display_round_name(tournament_code: str | None, round_number: int | None) -> str:
    code = (tournament_code or "").strip().upper()
    rn = int(round_number or 0)
    if code == "WC2026":
        wc_map = {
            1: "Тур 1",
            2: "Тур 2",
            3: "Тур 3",
            4: "1/16 финала",
            5: "1/8 финала",
            6: "Четвертьфинал",
            7: "Полуфинал",
            8: "Матч за 3-е место",
            9: "Финал",
        }
        return wc_map.get(rn, f"Раунд {rn}")
    return f"Тур {rn}"
