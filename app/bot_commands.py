from aiogram import types


def get_bot_commands() -> list[types.BotCommand]:
    return [
        types.BotCommand(command="start", description="Запустить бота"),
    ]


async def set_bot_commands(bot) -> None:
    await bot.set_my_commands(get_bot_commands())


async def bot_commands(bot) -> None:
    # Backward-compatible alias for older startup code paths.
    await set_bot_commands(bot)
