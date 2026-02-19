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