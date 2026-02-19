import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.db import init_db
from app.handlers import register_handlers
from app.bot_commands import set_bot_commands


async def main():
    await init_db()

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in environment variables")

    bot = Bot(token=bot_token)
    dp = Dispatcher(storage=MemoryStorage())

    register_handlers(dp)
    await set_bot_commands(bot)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())