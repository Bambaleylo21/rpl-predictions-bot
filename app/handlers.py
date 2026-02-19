from aiogram import Dispatcher

from app.handlers_admin import register_admin_handlers
from app.handlers_user import register_user_handlers


def register_handlers(dp: Dispatcher) -> None:
    register_user_handlers(dp)
    register_admin_handlers(dp)