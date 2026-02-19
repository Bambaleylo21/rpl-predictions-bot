from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# SQLite база — это просто файл в папке проекта
DATABASE_URL = "sqlite+aiosqlite:///./data.db"

# "Двигатель" подключения к базе
engine = create_async_engine(DATABASE_URL, echo=False)

# "Фабрика сессий" — так мы будем открывать соединение для запросов
SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncSession:
    """
    Возвращает новую сессию для работы с базой.
    Сессия = короткий "сеанс" общения с базой (прочитать/записать и закрыть).
    """
    return SessionLocal()