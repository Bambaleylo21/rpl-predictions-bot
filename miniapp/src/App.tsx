import { useEffect, useState } from 'react'
import './App.css'
import WebApp from '@twa-dev/sdk'

type MeResponse = {
  ok: boolean
  in_telegram: boolean
  tg_user_id: number | null
  username: string | null
  first_name: string | null
  auth_date: string | null
  signature_checked: boolean
  note: string
}

function App() {
  const tgUserId = WebApp.initDataUnsafe?.user?.id ?? null
  const tgUsername = WebApp.initDataUnsafe?.user?.username ?? null
  const inTelegram = tgUserId !== null
  const [meData, setMeData] = useState<MeResponse | null>(null)
  const [apiError, setApiError] = useState<string | null>(null)

  useEffect(() => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = WebApp.initData || ''

    fetch(`${apiBase}/api/miniapp/me`, {
      headers: {
        'X-Telegram-Init-Data': initData,
      },
    })
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`)
        }
        const data = (await res.json()) as MeResponse
        setMeData(data)
        setApiError(null)
      })
      .catch((err) => {
        setApiError(String(err))
      })
  }, [])

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

      <section className="cards" style={{ marginTop: 10 }}>
        <div className="card">
          <div className="card-title">🧩 API /api/miniapp/me</div>
          <div className="card-text">
            {apiError ? (
              <>Ошибка API: {apiError}</>
            ) : meData ? (
              <>
                API ok: {String(meData.ok)} · in_telegram: {String(meData.in_telegram)}
                <br />
                tg_user_id: <b>{String(meData.tg_user_id)}</b>
              </>
            ) : (
              'Загружаю данные...'
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
