import asyncio
import os
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message

from app.db import init_db, SessionLocal
from app.handlers import register_handlers
from app.handlers_admin import recalc_points_for_match_in_session

# Если файл apisport_sync.py у тебя есть — импорт сработает.
# Если ты его удалял — бот всё равно запустится, синк просто отключится.
try:
    from app.apisport_sync import run_sync_loops
except Exception:
    run_sync_loops = None


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

    # 3) ВАЖНО: временный отладочный логгер всех входящих сообщений
    # Чтобы в Render Logs было видно, что бот получает команды
    @dp.message()
    async def _debug_all_messages(message: Message):
        logging.info(
            "INCOMING: chat=%s user=%s text=%r",
            message.chat.id,
            message.from_user.id if message.from_user else None,
            message.text,
        )

    # 4) Подключаем твои хендлеры (твоя логика)
    register_handlers(dp)

    # 5) Меню команд
    await _setup_commands(bot)

    # 6) Синхронизация API-Sport (если есть ключ и модуль)
    # Сейчас ты удалил APISPORT_API_KEY — синк будет отключен.
    try:
        if run_sync_loops is None:
            raise RuntimeError("apisport sync module is not available")
        await run_sync_loops(SessionLocal, recalc_points_for_match_in_session)
    except Exception as e:
        print("APISPORT SYNC DISABLED:", repr(e), flush=True)

    # 7) Polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())