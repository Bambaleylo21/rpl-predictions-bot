import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.db import init_db
from app.handlers import register_handlers


async def _setup_commands(bot: Bot) -> None:
    """
    Универсально: поддерживаем разные названия функции в app/bot_commands.py
    - set_bot_commands(bot)
    - bot_commands(bot)
    Если файла/функции нет — просто пропускаем.
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


async def main():
    await init_db()

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in environment variables")

    bot = Bot(token=bot_token)
    dp = Dispatcher(storage=MemoryStorage())

    register_handlers(dp)

    await _setup_commands(bot)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())