from aiogram import Dispatcher, types
from aiogram.filters import CommandStart, Command


def register_handlers(dp: Dispatcher) -> None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        await message.answer(
            "Привет! Я живой 🙂\n"
            "Дальше будем принимать прогнозы на РПЛ.\n\n"
            "Набери /help, чтобы увидеть подсказку."
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        text = (
            "📌 Команды:\n"
            "/start — начать\n"
            "/help — помощь\n\n"
            "Что будет дальше:\n"
            "— выбор тура\n"
            "— ввод прогнозов на матчи\n"
            "— таблица лидеров и статистика\n"
        )
        await message.answer(text)
    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅")        