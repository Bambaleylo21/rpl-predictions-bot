from aiogram import types
from app.config import load_admin_ids


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


async def bot_commands(bot) -> None:
    # Backward-compatible alias for older startup code paths.
    await set_bot_commands(bot)
