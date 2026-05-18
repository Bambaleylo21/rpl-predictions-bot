from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models import Setting

NOTIFY_TYPES = ("reminders", "duels", "achievements")
NOTIFY_ALL = "all"


def _key(tg_user_id: int, pref: str) -> str:
    return f"NOTIFY_U{int(tg_user_id)}_{pref.upper()}"


async def _get_raw_setting(session, key: str) -> str | None:
    row = (await session.execute(select(Setting).where(Setting.key == key))).scalar_one_or_none()
    return row.value if row is not None else None


async def _set_raw_setting(session, key: str, value: str) -> None:
    bind = session.get_bind()
    dialect_name = bind.dialect.name if bind is not None else ""

    if dialect_name == "postgresql":
        stmt = (
            pg_insert(Setting)
            .values(key=key, value=value)
            .on_conflict_do_update(
                index_elements=[Setting.key],
                set_={"value": value},
            )
        )
        await session.execute(stmt)
        return

    if dialect_name == "sqlite":
        stmt = (
            sqlite_insert(Setting)
            .values(key=key, value=value)
            .on_conflict_do_update(
                index_elements=[Setting.key],
                set_={"value": value},
            )
        )
        await session.execute(stmt)
        return

    row = (await session.execute(select(Setting).where(Setting.key == key))).scalar_one_or_none()
    if row is None:
        session.add(Setting(key=key, value=value))
    else:
        row.value = value


def _to_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in ("0", "false", "off", "no"):
        return False
    if normalized in ("1", "true", "on", "yes"):
        return True
    return default


async def get_user_notification_prefs(session, tg_user_id: int) -> dict[str, bool]:
    uid = int(tg_user_id)
    all_enabled = _to_bool(await _get_raw_setting(session, _key(uid, NOTIFY_ALL)), default=True)
    reminders_enabled = _to_bool(await _get_raw_setting(session, _key(uid, "reminders")), default=True)
    duels_enabled = _to_bool(await _get_raw_setting(session, _key(uid, "duels")), default=True)
    achievements_enabled = _to_bool(await _get_raw_setting(session, _key(uid, "achievements")), default=True)
    return {
        "all": all_enabled,
        "reminders": reminders_enabled,
        "duels": duels_enabled,
        "achievements": achievements_enabled,
    }


async def set_user_notification_pref(session, tg_user_id: int, pref: str, enabled: bool) -> bool:
    pref_norm = str(pref or "").strip().lower()
    if pref_norm not in (NOTIFY_ALL, *NOTIFY_TYPES):
        return False
    await _set_raw_setting(session, _key(int(tg_user_id), pref_norm), "1" if bool(enabled) else "0")
    return True


async def should_send_notification(session, tg_user_id: int, pref: str) -> bool:
    pref_norm = str(pref or "").strip().lower()
    if pref_norm not in NOTIFY_TYPES:
        return True
    prefs = await get_user_notification_prefs(session, int(tg_user_id))
    return bool(prefs.get("all", True)) and bool(prefs.get(pref_norm, True))

