import './App.css'
import WebApp from '@twa-dev/sdk'

function App() {
  const tgUserId = WebApp.initDataUnsafe?.user?.id ?? null
  const tgUsername = WebApp.initDataUnsafe?.user?.username ?? null
  const inTelegram = tgUserId !== null

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="badge">РПЛ Mini App</div>
        <h1>Бот прогнозов</h1>
        <p>Первый экран. Дальше подключим живые данные из твоего бота.</p>
      </header>

      <section className="cards">
        <button className="card">
          <div className="card-title">🎯 Поставить прогноз</div>
          <div className="card-text">Выбрать тур и матчи</div>
        </button>

        <button className="card">
          <div className="card-title">🗂 Мои прогнозы</div>
          <div className="card-text">Текущий и другие туры</div>
        </button>

        <button className="card">
          <div className="card-title">🏆 Таблица</div>
          <div className="card-text">Лига и этап</div>
        </button>

        <button className="card">
          <div className="card-title">👤 Профиль</div>
          <div className="card-text">Статистика участника</div>
        </button>
      </section>

      <section className="cards" style={{ marginTop: 10 }}>
        <div className="card">
          <div className="card-title">🔐 Telegram-сессия</div>
          <div className="card-text">
            {inTelegram ? (
              <>
                Подключено: user_id <b>{tgUserId}</b>
                {tgUsername ? (
                  <>
                    {' '}
                    · @{tgUsername}
                  </>
                ) : null}
              </>
            ) : (
              'Открыто вне Telegram (это нормально для локальной проверки).'
            )}
          </div>
        </div>
      </section>

      <footer className="footer-note">
        Статус: макет готов. Следующий шаг: подключение API.
      </footer>
    </div>
  )
}

export default App
