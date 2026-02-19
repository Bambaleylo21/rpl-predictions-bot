import asyncio
import logging

from aiogram import Bot, Dispatcher

from app.bot_commands import get_bot_commands
from app.config import load_config
from app.handlers import register_handlers


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    token = load_config()
    bot = Bot(token=token)

    # Команды меню (кнопка / в Telegram)
    await bot.set_my_commands(get_bot_commands())

    dp = Dispatcher()
    register_handlers(dp)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())