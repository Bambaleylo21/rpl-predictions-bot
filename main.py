import asyncio
import os
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from sqlalchemy import text

from app.db import SessionLocal, engine, init_db
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

    lock_conn = None
    try:
        # Защита от двойного polling на Render:
        # если второй инстанс стартует одновременно, он ждёт освобождения lock,
        # а не завершает процесс (чтобы Render не слал false-positive "Application exited early").
        if str(engine.url).startswith("postgresql+asyncpg://"):
            lock_conn = await engine.connect()
            lock_key = 8093666505  # стабильный ключ под этого бота
            while True:
                lock_q = await lock_conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key})
                got_lock = bool(lock_q.scalar())
                if got_lock:
                    break
                logging.warning("Another bot instance holds polling lock. Waiting 5s before retry...")
                await asyncio.sleep(5)
    except Exception as e:
        logging.error("Failed to acquire singleton polling lock: %r", e)
        if lock_conn is not None:
            try:
                await lock_conn.close()
            except Exception:
                pass
        await bot.session.close()
        return

    # 3) Подключаем твои хендлеры (твоя логика)
    register_handlers(dp)

    # 4) Меню команд
    await _setup_commands(bot)

    # 5) Фоновый цикл напоминаний за 30 минут до матчей
    if run_match_reminders_loop is not None:
        asyncio.create_task(run_match_reminders_loop(bot, SessionLocal))

    # 6) Polling (жёсткий ручной режим: без фоновой синхронизации API)
    try:
        # На всякий случай сбрасываем webhook-режим перед long polling.
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot)
    finally:
        if lock_conn is not None:
            try:
                await lock_conn.close()
            except Exception:
                pass
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
