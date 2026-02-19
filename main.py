import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.bot_commands import get_bot_commands
from app.config import load_config
from app.handlers import register_handlers

from app.db import engine
from app.models import Base


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    # Создаём таблицы в базе, если их ещё нет
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    token = load_config()
    bot = Bot(token=token)

    await bot.set_my_commands(get_bot_commands())

    # ✅ включаем хранилище состояний (FSM) в памяти
    dp = Dispatcher(storage=MemoryStorage())
    register_handlers(dp)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())