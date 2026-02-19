import os
from dotenv import load_dotenv


def load_config() -> str:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("Не найден BOT_TOKEN. Проверьте файл .env и строку BOT_TOKEN=...")
    return token