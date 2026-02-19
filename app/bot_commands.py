from aiogram import types


def get_bot_commands() -> list[types.BotCommand]:
    return [
        types.BotCommand(command="start", description="Запустить бота"),
        types.BotCommand(command="help", description="Как пользоваться ботом"),
        types.BotCommand(command="ping", description="Проверка: бот жив?"),
        types.BotCommand(command="round", description="Матчи тура: /round 1"),
        types.BotCommand(command="predict", description="Сделать прогноз: /predict 1 2:0"),
        types.BotCommand(command="table", description="Таблица лидеров"),
        types.BotCommand(command="admin_add_match", description="Админ: добавить матч"),
        types.BotCommand(command="admin_set_result", description="Админ: поставить результат"),
        types.BotCommand(command="admin_recalc", description="Админ: пересчитать очки"),
    ]