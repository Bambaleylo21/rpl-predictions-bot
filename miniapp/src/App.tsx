import { useEffect, useState } from 'react'
import './App.css'
import WebApp from '@twa-dev/sdk'

type MeResponse = {
  ok: boolean
  error?: string
  reason?: string
  in_telegram: boolean
  tg_user_id: number | null
  username: string | null
  first_name: string | null
  auth_date: string | null
  signature_checked: boolean
  trusted?: boolean
  note: string
}

type ProfileResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  joined?: boolean
  tg_user_id?: number
  display_name?: string
  username?: string | null
  predictions_count?: number
  total_points?: number
  exact_hits?: number
  diff_hits?: number
  outcome_hits?: number
  league_name?: string | null
  stage_name?: string | null
  stage_round_min?: number | null
  stage_round_max?: number | null
  message?: string
}

function App() {
  const [tgUserId, setTgUserId] = useState<number | null>(null)
  const [tgUsername, setTgUsername] = useState<string | null>(null)
  const [initDataLen, setInitDataLen] = useState<number>(0)
  const inTelegram = tgUserId !== null
  const [meData, setMeData] = useState<MeResponse | null>(null)
  const [apiError, setApiError] = useState<string | null>(null)
  const [profileData, setProfileData] = useState<ProfileResponse | null>(null)
  const [profileError, setProfileError] = useState<string | null>(null)

  useEffect(() => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    let attempts = 0
    const maxAttempts = 30 // ~3s

    const run = () => {
      const tgWebApp = (window as any).Telegram?.WebApp
      const initData = tgWebApp?.initData || WebApp.initData || ''
      const user = tgWebApp?.initDataUnsafe?.user ?? WebApp.initDataUnsafe?.user

      setInitDataLen(initData.length)
      setTgUserId(user?.id ?? null)
      setTgUsername(user?.username ?? null)

      if (!initData && attempts < maxAttempts) {
        attempts += 1
        setTimeout(run, 100)
        return
      }

      const headers = {
        'X-Telegram-Init-Data': initData,
      }

      fetch(`${apiBase}/api/miniapp/me`, { headers })
        .then(async (res) => {
          const data = (await res.json()) as MeResponse
          if (!res.ok) {
            throw new Error(data.reason || data.error || `HTTP ${res.status}`)
          }
          setMeData(data)
          setApiError(null)
        })
        .catch((err) => {
          setApiError(String(err))
        })

      fetch(`${apiBase}/api/miniapp/profile`, { headers })
        .then(async (res) => {
          const data = (await res.json()) as ProfileResponse
          if (!res.ok) {
            throw new Error(data.reason || data.error || `HTTP ${res.status}`)
          }
          setProfileData(data)
          setProfileError(null)
        })
        .catch((err) => {
          setProfileError(String(err))
        })
    }

    run()
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
              <>
                Открыто вне Telegram или initData ещё не пришёл.
                <br />
                initData length: <b>{initDataLen}</b>
              </>
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
                <br />
                signature_checked: <b>{String(meData.signature_checked)}</b>
              </>
            ) : (
              'Загружаю данные...'
            )}
          </div>
        </div>
      </section>

      <section className="cards" style={{ marginTop: 10 }}>
        <div className="card">
          <div className="card-title">👤 API /api/miniapp/profile</div>
          <div className="card-text">
            {profileError ? (
              <>Ошибка профиля: {profileError}</>
            ) : profileData ? (
              profileData.joined ? (
                <>
                  {profileData.display_name} · {profileData.total_points ?? 0} очк.
                  <br />
                  Прогнозов: <b>{profileData.predictions_count ?? 0}</b> · 🎯{profileData.exact_hits ?? 0} · 📏{profileData.diff_hits ?? 0} · ✅{profileData.outcome_hits ?? 0}
                  <br />
                  {profileData.league_name ? `${profileData.league_name}` : 'Лига: —'}
                  {profileData.stage_name ? ` · ${profileData.stage_name}` : ''}
                </>
              ) : (
                profileData.message || 'Пользователь пока не вступил в турнир.'
              )
            ) : (
              'Загружаю профиль...'
            )}
          </div>
        </div>
      </section>

      <footer className="footer-note">
        Статус: Mini App авторизован и получает профиль из API.
      </footer>
    </div>
  )
}

export default App
