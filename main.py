import asyncio
import os
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.db import SessionLocal, init_db
from app.handlers import register_handlers

try:
    from app.reminders import run_match_reminders_loop
except Exception:
    run_match_reminders_loop = None


logging.basicConfig(level=logging.INFO)


async def _setup_commands(bot: Bot) -> None:
    """
    Ставит меню команд (если у тебя есть app/bot_commands.py).
    Делает это безопасно: если что-то не так — бот не падает.
    """
    try:
        from app.bot_commands import set_bot_commands  # type: ignore
        await set_bot_commands(bot)  # type: ignore
        return
    except Exception:
        pass

    try:
        from app.bot_commands import bot_commands  # type: ignore
        res = bot_commands(bot)  # type: ignore
        if asyncio.iscoroutine(res):
            await res
        return
    except Exception:
        pass


def _mask_token(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return "<empty>"
    return f"{t[:6]}... (len={len(t)})"


async def main():
    # 1) Инициализация БД / авто-фиксы схемы
    await init_db()

    # 2) Токен
    bot_token = os.getenv("BOT_TOKEN", "")
    print("BOT_TOKEN detected:", _mask_token(bot_token), flush=True)

    if not bot_token.strip():
        raise RuntimeError("BOT_TOKEN is not set in environment variables")

    bot = Bot(token=bot_token.strip())
    dp = Dispatcher(storage=MemoryStorage())

    # 3) Подключаем твои хендлеры (твоя логика)
    register_handlers(dp)

    # 4) Меню команд
    await _setup_commands(bot)

    # 5) Фоновый цикл напоминаний за 30 минут до матчей
    if run_match_reminders_loop is not None:
        asyncio.create_task(run_match_reminders_loop(bot, SessionLocal))

    # 6) Polling (жёсткий ручной режим: без фоновой синхронизации API)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
