from aiogram import Dispatcher, types
from aiogram.filters import CommandStart, Command

from sqlalchemy import select

from app.db import SessionLocal
from app.models import User


def register_handlers(dp: Dispatcher) -> None:
    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        # 1) Сохраняем пользователя в базу, если его там ещё нет
        tg_user_id = message.from_user.id
        username = message.from_user.username  # может быть None

        async with SessionLocal() as session:
            result = await session.execute(select(User).where(User.tg_user_id == tg_user_id))
            user = result.scalar_one_or_none()

            if user is None:
                session.add(User(tg_user_id=tg_user_id, username=username))
                await session.commit()

        # 2) Ответ пользователю
        await message.answer(
            "Привет! Я живой 🙂\n"
            "Ты зарегистрирован(а) в турнире.\n\n"
            "Дальше будем принимать прогнозы на РПЛ.\n"
            "Набери /help, чтобы увидеть подсказку."
        )

    @dp.message(Command("help"))
    async def cmd_help(message: types.Message):
        text = (
            "📌 Команды:\n"
            "/start — начать\n"
            "/help — помощь\n"
            "/ping — проверка\n\n"
            "Что будет дальше:\n"
            "— выбор тура\n"
            "— ввод прогнозов на матчи\n"
            "— таблица лидеров и статистика\n"
        )
        await message.answer(text)

    @dp.message(Command("ping"))
    async def cmd_ping(message: types.Message):
        await message.answer("pong ✅")