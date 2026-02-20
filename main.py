import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from app.db import init_db, SessionLocal
from app.handlers import register_handlers
from app.handlers_admin import recalc_points_for_match_in_session
from app.apisport_sync import run_sync_loops


async def _setup_commands(bot: Bot) -> None:
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
    t = t.strip()
    if not t:
        return "<empty>"
    prefix = t[:6]
    return f"{prefix}... (len={len(t)})"


async def main():
    await init_db()

    bot_token = os.getenv("BOT_TOKEN", "")
    print("BOT_TOKEN detected:", _mask_token(bot_token), flush=True)

    if not bot_token.strip():
        raise RuntimeError("BOT_TOKEN is not set in environment variables")

    bot = Bot(token=bot_token.strip())
    dp = Dispatcher(storage=MemoryStorage())

    register_handlers(dp)
    await _setup_commands(bot)

    # sync (если ключа нет — отключится)
    try:
        await run_sync_loops(SessionLocal, recalc_points_for_match_in_session)
    except Exception as e:
        print("APISPORT SYNC DISABLED:", repr(e))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())