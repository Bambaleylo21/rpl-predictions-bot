from aiogram import types
from app.config import load_admin_ids


def get_admin_bot_commands() -> list[types.BotCommand]:
    return [
        types.BotCommand(command="admin_panel", description="Админ: кнопочная панель"),
        types.BotCommand(command="admin_status", description="Админ: сводный статус"),
        types.BotCommand(command="admin_round_progress", description="Админ: прогресс тура"),
        types.BotCommand(command="admin_missing", description="Админ: кто не поставил"),
        types.BotCommand(command="admin_add_match", description="Админ: добавить матч"),
        types.BotCommand(command="admin_set_result", description="Админ: поставить результат"),
        types.BotCommand(command="admin_recalc", description="Админ: пересчитать очки"),
        types.BotCommand(command="admin_manual_only_cleanup", description="Админ: чистка ручного режима"),
        types.BotCommand(command="admin_health", description="Админ: здоровье БД"),
        types.BotCommand(command="admin_audience", description="Админ: аудитория бота"),
        types.BotCommand(command="admin_set_window", description="Админ: окно туров"),
        types.BotCommand(command="admin_remove_user", description="Админ: удалить участника"),
    ]


async def set_bot_commands(bot) -> None:
    admin_ids = load_admin_ids()

    # У обычных пользователей меню-команды скрыты полностью.
    await bot.set_my_commands([], scope=types.BotCommandScopeDefault())

    # Админам показываем только админские команды.
    admin_commands = get_admin_bot_commands()
    for admin_id in admin_ids:
        await bot.set_my_commands(
            admin_commands,
            scope=types.BotCommandScopeChat(chat_id=admin_id),
        )


async def bot_commands(bot) -> None:
    # Backward-compatible alias for older startup code paths.
    await set_bot_commands(bot)
