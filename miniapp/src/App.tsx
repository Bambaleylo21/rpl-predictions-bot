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

type PredictionsResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  tournament?: string
  round_number?: number
  round_min?: number
  round_max?: number
  total_points_closed?: number
  items?: Array<{
    match_id: number
    home_team: string
    away_team: string
    kickoff: string
    status: 'open' | 'closed'
    result: string | null
    prediction: string | null
    points: number | null
    category: string | null
    emoji: string
  }>
}

type TableResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  has_table?: boolean
  message?: string
  season_name?: string
  stage_name?: string
  stage_round_min?: number
  stage_round_max?: number
  league_name?: string
  participants?: number
  user_place?: number | null
  rows?: Array<{
    place: number
    name: string
    total: number
    exact: number
    diff: number
    outcome: number
    pred_total: number
    hits: number
    hit_rate: number
  }>
}

type Screen = 'home' | 'profile' | 'predictions' | 'table'

function App() {
  const [screen, setScreen] = useState<Screen>('home')
  const [tgUserId, setTgUserId] = useState<number | null>(null)
  const [tgUsername, setTgUsername] = useState<string | null>(null)
  const [initDataLen, setInitDataLen] = useState<number>(0)
  const inTelegram = tgUserId !== null
  const [meData, setMeData] = useState<MeResponse | null>(null)
  const [apiError, setApiError] = useState<string | null>(null)
  const [profileData, setProfileData] = useState<ProfileResponse | null>(null)
  const [profileError, setProfileError] = useState<string | null>(null)
  const [predictionsData, setPredictionsData] = useState<PredictionsResponse | null>(null)
  const [predictionsError, setPredictionsError] = useState<string | null>(null)
  const [tableData, setTableData] = useState<TableResponse | null>(null)
  const [tableError, setTableError] = useState<string | null>(null)
  const showDebugPanels = import.meta.env.DEV || import.meta.env.VITE_DEBUG_PANELS === '1'

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

      fetch(`${apiBase}/api/miniapp/predictions/current`, { headers })
        .then(async (res) => {
          const data = (await res.json()) as PredictionsResponse
          if (!res.ok) {
            throw new Error(data.reason || data.error || `HTTP ${res.status}`)
          }
          setPredictionsData(data)
          setPredictionsError(null)
        })
        .catch((err) => {
          setPredictionsError(String(err))
        })

      fetch(`${apiBase}/api/miniapp/table/current`, { headers })
        .then(async (res) => {
          const data = (await res.json()) as TableResponse
          if (!res.ok) {
            throw new Error(data.reason || data.error || `HTTP ${res.status}`)
          }
          setTableData(data)
          setTableError(null)
        })
        .catch((err) => {
          setTableError(String(err))
        })
    }

    run()
  }, [])

  return (
    <div className="app-shell">
      {screen === 'home' ? (
        <>
          <header className="topbar">
            <div className="badge">РПЛ Mini App</div>
            <h1>Бот прогнозов</h1>
            <p>Новый интерфейс. Дальше подключаем рабочие экраны по шагам.</p>
          </header>

          <section className="cards">
            <button className="card">
              <div className="card-title">🎯 Поставить прогноз</div>
              <div className="card-text">Выбрать тур и матчи</div>
            </button>

            <button className="card" onClick={() => setScreen('predictions')}>
              <div className="card-title">🗂 Мои прогнозы</div>
              <div className="card-text">Текущий и другие туры</div>
            </button>

            <button className="card" onClick={() => setScreen('table')}>
              <div className="card-title">🏆 Таблица</div>
              <div className="card-text">Лига и этап</div>
            </button>

            <button className="card" onClick={() => setScreen('profile')}>
              <div className="card-title">👤 Профиль</div>
              <div className="card-text">Открыть личную статистику</div>
            </button>
          </section>
        </>
      ) : screen === 'profile' ? (
        <>
          <header className="topbar">
            <button className="back-btn" onClick={() => setScreen('home')}>
              ← Назад
            </button>
            <h1>👤 Мой профиль</h1>
            <p>Личные данные участника из защищённого API.</p>
          </header>

          <section className="cards">
            <div className="card">
              <div className="card-title">
                {profileData?.display_name || (tgUsername ? `@${tgUsername}` : `ID ${tgUserId ?? '—'}`)}
              </div>
              <div className="card-text">
                {profileError ? (
                  <>Ошибка загрузки профиля: {profileError}</>
                ) : !profileData ? (
                  'Загружаю профиль...'
                ) : profileData.joined ? (
                  <>
                    Очки: <b>{profileData.total_points ?? 0}</b>
                    <br />
                    Прогнозов: <b>{profileData.predictions_count ?? 0}</b>
                    <br />
                    🎯 Точный счёт: <b>{profileData.exact_hits ?? 0}</b> · 📏 Разница: <b>{profileData.diff_hits ?? 0}</b> · ✅ Исход: <b>{profileData.outcome_hits ?? 0}</b>
                    <br />
                    Лига: <b>{profileData.league_name || '—'}</b>
                    {profileData.stage_name ? (
                      <>
                        <br />
                        Этап: <b>{profileData.stage_name}</b>
                        {profileData.stage_round_min != null && profileData.stage_round_max != null ? (
                          <> (туры {profileData.stage_round_min}-{profileData.stage_round_max})</>
                        ) : null}
                      </>
                    ) : null}
                  </>
                ) : (
                  profileData.message || 'Пока нет активного участия в турнире.'
                )}
              </div>
            </div>
          </section>
        </>
      ) : screen === 'predictions' ? (
        <>
          <header className="topbar">
            <button className="back-btn" onClick={() => setScreen('home')}>
              ← Назад
            </button>
            <h1>🗂 Мои прогнозы</h1>
            <p>Текущий тур и твои ставки по матчам.</p>
          </header>

          <section className="cards">
            <div className="card">
              <div className="card-title">
                {predictionsData?.tournament || 'РПЛ'} · Тур {predictionsData?.round_number ?? '—'}
              </div>
              <div className="card-text">
                {predictionsError ? (
                  <>Ошибка загрузки прогнозов: {predictionsError}</>
                ) : !predictionsData ? (
                  'Загружаю прогнозы...'
                ) : (
                  <>
                    Итого по завершённым: <b>{predictionsData.total_points_closed ?? 0}</b> очк.
                    <br />
                    Матчей в туре: <b>{predictionsData.items?.length ?? 0}</b>
                  </>
                )}
              </div>
            </div>
          </section>

          <section className="cards" style={{ marginTop: 10 }}>
            {(predictionsData?.items || []).map((m) => (
              <div className="card" key={m.match_id}>
                <div className="card-title">
                  {m.home_team} — {m.away_team}
                </div>
                <div className="card-text">
                  {m.kickoff} МСК
                  <br />
                  {m.status === 'open' ? (
                    m.prediction ? (
                      <>Прогноз: <b>{m.prediction}</b> (матч ещё открыт)</>
                    ) : (
                      'Без прогноза (матч ещё открыт)'
                    )
                  ) : m.prediction ? (
                    <>
                      Итог: <b>{m.result}</b> · Прогноз: <b>{m.prediction}</b> · {m.emoji} {m.points ?? 0}
                    </>
                  ) : (
                    <>
                      Итог: <b>{m.result}</b> · Без прогноза
                    </>
                  )}
                </div>
              </div>
            ))}
          </section>
        </>
      ) : (
        <>
          <header className="topbar">
            <button className="back-btn" onClick={() => setScreen('home')}>
              ← Назад
            </button>
            <h1>🏆 Таблица</h1>
            <p>Текущая лига и позиции участников.</p>
          </header>

          <section className="cards">
            <div className="card">
              <div className="card-title">
                {tableData?.league_name || 'Лига'} {tableData?.stage_name ? `· ${tableData.stage_name}` : ''}
              </div>
              <div className="card-text">
                {tableError ? (
                  <>Ошибка загрузки таблицы: {tableError}</>
                ) : !tableData ? (
                  'Загружаю таблицу...'
                ) : tableData.has_table ? (
                  <>
                    Участников: <b>{tableData.participants ?? 0}</b>
                    <br />
                    {tableData.user_place ? (
                      <>
                        Твоё место: <b>{tableData.user_place}</b>
                      </>
                    ) : (
                      'Ты пока не в списке лиги этого этапа.'
                    )}
                  </>
                ) : (
                  tableData.message || 'Таблица пока не сформирована.'
                )}
              </div>
            </div>
          </section>

          <section className="cards" style={{ marginTop: 10 }}>
            {(tableData?.rows || []).map((r) => (
              <div className="card" key={`${r.place}-${r.name}`}>
                <div className="card-title">
                  {r.place}. {r.name} — {r.total} очк.
                </div>
                <div className="card-text">
                  🎯{r.exact} · 📏{r.diff} · ✅{r.outcome}
                </div>
              </div>
            ))}
          </section>
        </>
      )}

      {showDebugPanels ? (
        <>
          <section className="cards" style={{ marginTop: 10 }}>
            <div className="card">
              <div className="card-title">🔐 Telegram-сессия (debug)</div>
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
              <div className="card-title">🧩 API /api/miniapp/me (debug)</div>
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
        </>
      ) : null}

      <footer className="footer-note">Статус: Mini App авторизован и получает профиль из API.</footer>
    </div>
  )
}

export default App
