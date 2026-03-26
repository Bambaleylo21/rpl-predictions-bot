from __future__ import annotations

import re

from sqlalchemy import select

from app.models import Setting

BLOCKED_USER_KEY_PREFIX = "BOT_BLOCKED_U"
LEFT_KEY_PATTERN = "LEFT_T%_U%"


def blocked_user_key(tg_user_id: int) -> str:
    return f"{BLOCKED_USER_KEY_PREFIX}{int(tg_user_id)}"


def is_blocked_send_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    checks = (
        "bot was blocked by the user",
        "forbidden",
        "chat not found",
        "user is deactivated",
    )
    return any(c in msg for c in checks)


async def mark_user_blocked(session, tg_user_id: int) -> None:
    key = blocked_user_key(tg_user_id)
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is None:
        session.add(Setting(key=key, value="1"))
    else:
        row.value = "1"


async def unmark_user_blocked(session, tg_user_id: int) -> None:
    key = blocked_user_key(tg_user_id)
    q = await session.execute(select(Setting).where(Setting.key == key))
    row = q.scalar_one_or_none()
    if row is not None:
        await session.delete(row)


def extract_left_user_id(setting_key: str) -> int | None:
    # key format: LEFT_T<tournament_id>_U<tg_user_id>
    m = re.match(r"^LEFT_T\d+_U(\d+)$", setting_key or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None

