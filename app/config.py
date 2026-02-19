import os
from dotenv import load_dotenv


def load_config() -> str:
    """
    Оставляем как было: возвращает BOT_TOKEN.
    """
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("Не найден BOT_TOKEN. Проверьте файл .env и строку BOT_TOKEN=...")
    return token


def load_admin_ids() -> set[int]:
    """
    Читает ADMIN_IDS из .env.
    Форматы:
      ADMIN_IDS=210477579
      ADMIN_IDS=210477579,123456789
    """
    load_dotenv()
    raw = os.getenv("ADMIN_IDS", "").strip()
    if not raw:
        return set()

    ids: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            raise ValueError(f"ADMIN_IDS должен содержать только числа через запятую. Ошибка в: '{part}'")
    return ids


def load_football_api_key() -> str:
    """
    Читает ключ API-Football (API-SPORTS) из .env / Render env.

    Формат:
      FOOTBALL_API_KEY=xxxxxxxxxxxxxxxxxxxx

    Возвращает строку ключа. Если ключ не задан — выбрасывает ValueError.
    """
    load_dotenv()
    key = os.getenv("FOOTBALL_API_KEY")
    if not key or not key.strip():
        raise ValueError(
            "Не найден FOOTBALL_API_KEY. Добавьте переменную окружения FOOTBALL_API_KEY "
            "(в .env локально или в Render → Environment)."
        )
    return key.strip()


def load_football_api_base_url() -> str:
    """
    (Необязательно) Базовый URL API-Football.
    По умолчанию: https://v3.football.api-sports.io

    Можно переопределить:
      FOOTBALL_API_BASE_URL=https://v3.football.api-sports.io
    """
    load_dotenv()
    return os.getenv("FOOTBALL_API_BASE_URL", "https://v3.football.api-sports.io").strip()