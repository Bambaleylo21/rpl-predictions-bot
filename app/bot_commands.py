from aiogram import types


def get_bot_commands() -> list[types.BotCommand]:
    return [
        types.BotCommand(command="start", description="Запустить бота"),
        types.BotCommand(command="help", description="Как пользоваться ботом"),
        types.BotCommand(command="ping", description="Проверка: бот жив?"),
    ]