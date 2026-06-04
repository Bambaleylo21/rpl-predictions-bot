from aiogram import types
import os

from app.config import load_admin_ids


MINIAPP_WEB_URL = os.getenv("MINIAPP_WEB_URL", "https://rpl-predictions-bot-mini-app.onrender.com").strip()


async def set_bot_commands(bot) -> None:
    admin_ids = load_admin_ids()

    # Полностью скрываем меню команд в Telegram для всех scope.
    await bot.set_my_commands([], scope=types.BotCommandScopeDefault())
    await bot.set_my_commands([], scope=types.BotCommandScopeAllPrivateChats())
    await bot.set_my_commands([], scope=types.BotCommandScopeAllGroupChats())
    await bot.set_my_commands([], scope=types.BotCommandScopeAllChatAdministrators())

    # Чистим и персональные scope админов, чтобы не оставались старые команды.
    for admin_id in admin_ids:
        await bot.set_my_commands(
            [],
            scope=types.BotCommandScopeChat(chat_id=admin_id),
        )

    # Нижняя кнопка Telegram-бота теперь ведёт сразу в Mini App.
    # Если Telegram/API временно не примет кнопку, бот всё равно должен стартовать.
    if MINIAPP_WEB_URL:
        try:
            await bot.set_chat_menu_button(
                menu_button=types.MenuButtonWebApp(
                    text="Открыть Ванга-L",
                    web_app=types.WebAppInfo(url=MINIAPP_WEB_URL),
                )
            )
        except Exception:
            pass


async def bot_commands(bot) -> None:
    # Backward-compatible alias for older startup code paths.
    await set_bot_commands(bot)
