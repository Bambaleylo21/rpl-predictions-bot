from aiogram import types


def get_bot_commands() -> list[types.BotCommand]:
    return [
        types.BotCommand(command="start", description="Запустить бота"),
        types.BotCommand(command="help", description="Как пользоваться ботом"),
        types.BotCommand(command="ping", description="Проверка: бот жив?"),
        types.BotCommand(command="round", description="Матчи тура: /round 19"),
        types.BotCommand(command="predict", description="Прогноз на матч: /predict 1 2:0"),
        types.BotCommand(command="predict_round", description="Прогнозы на тур: /predict_round 19"),
        types.BotCommand(command="table", description="Таблица лидеров"),
        types.BotCommand(command="table_round", description="Таблица тура: /table_round 19"),
        types.BotCommand(command="history", description="История туров (кнопки)"),
        types.BotCommand(command="profile", description="Мой профиль"),
        types.BotCommand(command="mvp_round", description="MVP тура: /mvp_round 19"),
        types.BotCommand(command="tops_round", description="Топы тура: /tops_round 19"),
        types.BotCommand(command="round_digest", description="Итоги тура: /round_digest 19"),
        types.BotCommand(command="stats", description="Подробная статистика"),
        types.BotCommand(command="admin_add_match", description="Админ: добавить матч"),
        types.BotCommand(command="admin_set_result", description="Админ: поставить результат"),
        types.BotCommand(command="admin_recalc", description="Админ: пересчитать очки"),
    ]


async def set_bot_commands(bot) -> None:
    await bot.set_my_commands(get_bot_commands())


async def bot_commands(bot) -> None:
    # Backward-compatible alias for older startup code paths.
    await set_bot_commands(bot)
