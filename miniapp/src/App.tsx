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
  selected_tournament_code?: string | null
  selected_tournament_name?: string | null
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
  round_name?: string
  round_number?: number
  round_min?: number
  round_max?: number
  total_points_closed?: number
  items?: Array<{
    match_id: number
    home_team: string
    away_team: string
    group_label?: string | null
    kickoff: string
    status: 'open' | 'closed'
    result: string | null
    prediction: string | null
    points: number | null
    category: string | null
    emoji: string
  }>
}

type PredictCurrentResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  joined?: boolean
  message?: string
  tournament?: string
  round_name?: string
  round_number?: number
  round_min?: number
  round_max?: number
  items?: Array<{
    match_id: number
    home_team: string
    away_team: string
    group_label?: string | null
    kickoff: string
    prediction: string | null
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

type TournamentsResponse = {
  ok: boolean
  trusted?: boolean
  selected_tournament_code?: string
  items?: Array<{
    code: string
    name: string
    round_min: number
    round_max: number
    selected: boolean
  }>
  error?: string
  reason?: string
}

type LongtermResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  enabled?: boolean
  joined?: boolean
  locked?: boolean
  deadline_msk?: string | null
  picks?: {
    winner?: string | null
    scorer?: string | null
  }
  options?: {
    winner?: string[]
    scorer?: string[]
  }
}

type Screen = 'predict' | 'profile' | 'predictions' | 'table'

function App() {
  const [screen, setScreen] = useState<Screen>('predict')
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
  const [predictData, setPredictData] = useState<PredictCurrentResponse | null>(null)
  const [predictError, setPredictError] = useState<string | null>(null)
  const [scoreInputs, setScoreInputs] = useState<Record<number, string>>({})
  const [savingMatchId, setSavingMatchId] = useState<number | null>(null)
  const [predictNotice, setPredictNotice] = useState<string | null>(null)
  const [tableData, setTableData] = useState<TableResponse | null>(null)
  const [tableError, setTableError] = useState<string | null>(null)
  const [longtermData, setLongtermData] = useState<LongtermResponse | null>(null)
  const [longtermError, setLongtermError] = useState<string | null>(null)
  const [longtermNotice, setLongtermNotice] = useState<string | null>(null)
  const [savingLongtermType, setSavingLongtermType] = useState<'winner' | 'scorer' | null>(null)
  const [winnerPickInput, setWinnerPickInput] = useState<string>('')
  const [scorerPickInput, setScorerPickInput] = useState<string>('')
  const [selectedTournamentCode, setSelectedTournamentCode] = useState<string>('WC2026')
  const [tournamentNotice, setTournamentNotice] = useState<string | null>(null)
  const [predictionsFilter, setPredictionsFilter] = useState<'open' | 'closed'>('open')
  const [stageTab, setStageTab] = useState<'1' | '2' | '3' | 'PO' | 'LT'>('1')
  const [playoffTab, setPlayoffTab] = useState<4 | 5 | 6 | 7 | 8 | 9>(4)

  const selectedRoundNumber =
    selectedTournamentCode === 'WC2026'
      ? (stageTab === 'PO' ? playoffTab : stageTab === 'LT' ? undefined : Number(stageTab))
      : undefined

  const formatScoreInput = (raw: string): string => {
    const digits = raw.replace(/\D/g, '').slice(0, 3)
    if (digits.length <= 1) return digits
    return `${digits.slice(0, 1)}-${digits.slice(1)}`
  }
  const showDebugPanels = import.meta.env.DEV || import.meta.env.VITE_DEBUG_PANELS === '1'

  const getInitData = () => {
    const tgWebApp = (window as any).Telegram?.WebApp
    return tgWebApp?.initData || WebApp.initData || ''
  }

  const loadPredictCurrent = async (
    apiBase: string,
    initData: string,
    tournamentCode: string,
    roundNumber?: number
  ) => {
    const headers = {
      'X-Telegram-Init-Data': initData,
    }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const rParam = roundNumber != null ? `&round=${encodeURIComponent(String(roundNumber))}` : ''
    const res = await fetch(`${apiBase}/api/miniapp/predict/current?t=${tParam}${rParam}`, { headers })
    const data = (await res.json()) as PredictCurrentResponse
    if (!res.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setPredictData(data)
    setPredictError(null)
    const nextInputs: Record<number, string> = {}
    for (const item of data.items || []) {
      nextInputs[item.match_id] = item.prediction || ''
    }
    setScoreInputs(nextInputs)
  }

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

      fetch(`${apiBase}/api/miniapp/tournaments`, { headers })
        .then(async (res) => {
          const data = (await res.json()) as TournamentsResponse
          if (!res.ok) {
            throw new Error(data.reason || data.error || `HTTP ${res.status}`)
          }
          const wcFirst = data.items?.find((x) => x.code === 'WC2026')?.code
          const selected = wcFirst || data.selected_tournament_code || data.items?.find((x) => x.selected)?.code || 'WC2026'
          setSelectedTournamentCode(selected)
        })
        .catch(() => {
          // silent: fallback to default tournament code
        })

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
    }

    run()
  }, [])

  useEffect(() => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData) {
      return
    }
    const headers = {
      'X-Telegram-Init-Data': initData,
    }
    const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')

    fetch(`${apiBase}/api/miniapp/profile?t=${tParam}`, { headers })
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

    const rParam = selectedRoundNumber != null ? `&round=${encodeURIComponent(String(selectedRoundNumber))}` : ''

    fetch(`${apiBase}/api/miniapp/predictions/current?t=${tParam}${rParam}`, { headers })
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

    loadPredictCurrent(apiBase, initData, selectedTournamentCode, selectedRoundNumber).catch((err) => {
      setPredictError(String(err))
    })

    fetch(`${apiBase}/api/miniapp/table/current?t=${tParam}`, { headers })
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

    fetch(`${apiBase}/api/miniapp/longterm/current?t=${tParam}`, { headers })
      .then(async (res) => {
        const data = (await res.json()) as LongtermResponse
        if (!res.ok) {
          throw new Error(data.reason || data.error || `HTTP ${res.status}`)
        }
        setLongtermData(data)
        setLongtermError(null)
        setWinnerPickInput(data.picks?.winner || '')
        setScorerPickInput(data.picks?.scorer || '')
      })
      .catch((err) => {
        setLongtermError(String(err))
      })
  }, [selectedTournamentCode, selectedRoundNumber])

  const selectTournament = async (code: string) => {
    if (!code || code === selectedTournamentCode) {
      return
    }
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    const headers = {
      'Content-Type': 'application/json',
      'X-Telegram-Init-Data': initData,
    }
    setTournamentNotice(null)
    try {
      const res = await fetch(`${apiBase}/api/miniapp/tournament/select`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ tournament_code: code }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; selected_tournament_code?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      const nextCode = data.selected_tournament_code || code
      setSelectedTournamentCode(nextCode)
      setTournamentNotice(`Выбран турнир: ${nextCode}`)
    } catch (err) {
      setTournamentNotice(`Ошибка выбора турнира: ${String(err)}`)
    }
  }

  const savePrediction = async (matchId: number) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    const raw = (scoreInputs[matchId] || '').trim().replace('-', ':')
    const m = raw.match(/^(\d+):(\d+)$/)
    if (!m) {
      setPredictNotice('Счёт введи в формате 2:1 или 2-1.')
      return
    }
    const predHome = Number(m[1])
    const predAway = Number(m[2])
    setSavingMatchId(matchId)
    setPredictNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/predict/set?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({
          match_id: matchId,
          pred_home: predHome,
          pred_away: predAway,
        }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; prediction?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setPredictNotice(`Ставка сохранена: ${data.prediction}`)
      await loadPredictCurrent(apiBase, initData, selectedTournamentCode, selectedRoundNumber)
    } catch (err) {
      setPredictNotice(`Ошибка сохранения: ${String(err)}`)
    } finally {
      setSavingMatchId(null)
    }
  }

  const saveLongtermPick = async (pickType: 'winner' | 'scorer') => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    const pickValue = (pickType === 'winner' ? winnerPickInput : scorerPickInput).trim()
    if (!pickValue) {
      setLongtermNotice('Выбери значение перед сохранением.')
      return
    }

    setSavingLongtermType(pickType)
    setLongtermNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/longterm/set?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({
          pick_type: pickType,
          pick_value: pickValue,
        }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; pick_value?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setLongtermNotice(`Сохранено: ${pickType === 'winner' ? 'Победитель' : 'Бомбардир'} — ${data.pick_value}`)

      const headers = { 'X-Telegram-Init-Data': initData }
      const reload = await fetch(`${apiBase}/api/miniapp/longterm/current?t=${tParam}`, { headers })
      const reloadData = (await reload.json()) as LongtermResponse
      if (reload.ok && reloadData.ok) {
        setLongtermData(reloadData)
      }
    } catch (err) {
      setLongtermNotice(`Ошибка сохранения: ${String(err)}`)
    } finally {
      setSavingLongtermType(null)
    }
  }

  const tabMeta: Record<Screen, { title: string; subtitle: string; icon: string }> = {
    profile: { title: 'Профиль', subtitle: 'Личная статистика участника', icon: '👤' },
    predict: { title: 'Сделать прогноз', subtitle: 'Открытые матчи текущего тура', icon: '🎯' },
    predictions: { title: 'Мои прогнозы', subtitle: 'Твои ставки по матчам', icon: '🗂' },
    table: { title: 'Таблица', subtitle: 'Позиции участников турнира', icon: '🏆' },
  }

  const tournamentButtons = [
    { code: 'WC2026', icon: '⚽', label: 'WC' },
    { code: 'RPL', icon: '🏆', label: 'РПЛ' },
  ]
  const showWcSelector = selectedTournamentCode === 'WC2026'
  const longtermLocked = Boolean(longtermData?.locked)

  const wcTopTabsBase: Array<{ key: '1' | '2' | '3' | 'PO'; label: string }> = [
    { key: '1', label: 'Тур 1' },
    { key: '2', label: 'Тур 2' },
    { key: '3', label: 'Тур 3' },
    { key: 'PO', label: 'Плей-офф' },
  ]
  const wcTopTabsPredict: Array<{ key: '1' | '2' | '3' | 'PO' | 'LT'; label: string }> =
    !longtermLocked ? [...wcTopTabsBase, { key: 'LT', label: 'Доп. прогнозы' }] : wcTopTabsBase
  const wcTopTabsPredictions: Array<{ key: '1' | '2' | '3' | 'PO' | 'LT'; label: string }> =
    longtermLocked ? [...wcTopTabsBase, { key: 'LT', label: 'Доп. прогнозы' }] : wcTopTabsBase
  const wcPlayoffTabs: Array<{ key: 4 | 5 | 6 | 7 | 8 | 9; label: string }> = [
    { key: 4, label: '1/16' },
    { key: 5, label: '1/8' },
    { key: 6, label: '1/4' },
    { key: 7, label: '1/2' },
    { key: 8, label: 'За 3-е' },
    { key: 9, label: 'Финал' },
  ]
  const allowLongtermTab =
    showWcSelector && ((screen === 'predict' && !longtermLocked) || (screen === 'predictions' && longtermLocked))

  useEffect(() => {
    if (stageTab === 'LT' && !allowLongtermTab) {
      setStageTab('1')
    }
  }, [allowLongtermTab, stageTab])

  return (
    <div className="app-shell">
      <header className="topbar sticky">
        <div className="topbar-row">
          <div>
            <div className="badge">РПЛ Mini App</div>
            <h1>{tabMeta[screen].icon} {tabMeta[screen].title}</h1>
            <p>{tabMeta[screen].subtitle}</p>
          </div>
          <div className="header-controls">
            <div className="tournament-icons">
              {tournamentButtons.map((t) => (
                <button
                  key={t.code}
                  className={`tournament-icon ${selectedTournamentCode === t.code ? 'is-active' : ''}`}
                  onClick={() => selectTournament(t.code)}
                  title={t.label}
                >
                  <span>{t.icon}</span>
                  <small>{t.label}</small>
                </button>
              ))}
            </div>
            {screen === 'predictions' && !(showWcSelector && stageTab === 'LT') ? (
              <div className="match-toggle">
                <button
                  className={`match-toggle-btn ${predictionsFilter === 'open' ? 'is-active' : ''}`}
                  onClick={() => setPredictionsFilter('open')}
                >
                  Активные
                </button>
                <button
                  className={`match-toggle-btn ${predictionsFilter === 'closed' ? 'is-active' : ''}`}
                  onClick={() => setPredictionsFilter('closed')}
                >
                  Завершённые
                </button>
              </div>
            ) : null}
          </div>
        </div>
        {tournamentNotice ? <div className="notice-line">{tournamentNotice}</div> : null}
      </header>

      <main className="content">
        {screen === 'predict' ? (
          <>
            {showWcSelector ? (
              <section className="cards">
                <div className="card card-static">
                  <div className="card-title">Этап турнира</div>
                  <div className="tournament-row">
                    {wcTopTabsPredict.map((tab) => (
                      <button
                        key={tab.key}
                        className={`tournament-chip ${stageTab === tab.key ? 'is-active' : ''}`}
                        onClick={() => setStageTab(tab.key)}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </div>
                  {stageTab === 'PO' ? (
                    <div className="tournament-row">
                      {wcPlayoffTabs.map((tab) => (
                        <button
                          key={tab.key}
                          className={`tournament-chip ${playoffTab === tab.key ? 'is-active' : ''}`}
                          onClick={() => setPlayoffTab(tab.key)}
                        >
                          {tab.label}
                        </button>
                      ))}
                    </div>
                  ) : null}
                </div>
              </section>
            ) : null}

            {showWcSelector && stageTab === 'LT' ? (
              <>
                <section className="cards">
                  <div className="card">
                    <div className="card-title">Доп. прогнозы ЧМ 2026</div>
                    <div className="card-text">
                      {longtermError ? (
                        <>Ошибка: {longtermError}</>
                      ) : !longtermData ? (
                        'Загружаю...'
                      ) : longtermData.joined === false ? (
                        'Сначала вступи в турнир, чтобы поставить доп. прогнозы.'
                      ) : (
                        <>
                          До старта первого матча:
                          <b> {longtermData.deadline_msk || '—'} МСК</b>
                          <br />
                          После дедлайна блок автоматически переедет в «Мои прогнозы».
                        </>
                      )}
                      {longtermNotice ? (
                        <>
                          <br />
                          {longtermNotice}
                        </>
                      ) : null}
                    </div>
                  </div>
                </section>

                <section className="cards space-top">
                  <div className="card">
                    <div className="card-title">🏆 Победитель ЧМ</div>
                    <div className="predict-row">
                      <select
                        className="score-input select-input"
                        value={winnerPickInput}
                        onChange={(e) => setWinnerPickInput(e.target.value)}
                      >
                        <option value="">Выбери команду</option>
                        {(longtermData?.options?.winner || []).map((name) => (
                          <option key={name} value={name}>
                            {name}
                          </option>
                        ))}
                      </select>
                      <button
                        className="save-btn"
                        onClick={() => saveLongtermPick('winner')}
                        disabled={savingLongtermType === 'winner' || longtermLocked || longtermData?.joined === false}
                      >
                        {savingLongtermType === 'winner' ? 'Сохраняю...' : 'Сохранить'}
                      </button>
                    </div>
                    <div className="card-text">
                      Текущий прогноз: <b>{longtermData?.picks?.winner || 'не выбран'}</b>
                    </div>
                  </div>

                  <div className="card">
                    <div className="card-title">⚽ Лучший бомбардир</div>
                    <div className="predict-row">
                      <select
                        className="score-input select-input"
                        value={scorerPickInput}
                        onChange={(e) => setScorerPickInput(e.target.value)}
                      >
                        <option value="">Выбери игрока</option>
                        {(longtermData?.options?.scorer || []).map((name) => (
                          <option key={name} value={name}>
                            {name}
                          </option>
                        ))}
                      </select>
                      <button
                        className="save-btn"
                        onClick={() => saveLongtermPick('scorer')}
                        disabled={savingLongtermType === 'scorer' || longtermLocked || longtermData?.joined === false}
                      >
                        {savingLongtermType === 'scorer' ? 'Сохраняю...' : 'Сохранить'}
                      </button>
                    </div>
                    <div className="card-text">
                      Текущий прогноз: <b>{longtermData?.picks?.scorer || 'не выбран'}</b>
                    </div>
                  </div>
                </section>
              </>
            ) : (
              <>
                <section className="cards">
                  <div className="card">
                    <div className="card-title">
                      {predictData?.tournament || selectedTournamentCode} · {predictData?.round_name || `Тур ${predictData?.round_number ?? '—'}`}
                    </div>
                    <div className="card-text">
                      {predictError ? (
                        <>Ошибка: {predictError}</>
                      ) : !predictData ? (
                        'Загружаю матчи...'
                      ) : predictData.joined === false ? (
                        predictData.message || 'Нужно вступить в турнир, чтобы ставить прогнозы.'
                      ) : (
                        <>Открытых матчей: <b>{predictData.items?.length ?? 0}</b></>
                      )}
                      {predictNotice ? (
                        <>
                          <br />
                          {predictNotice}
                        </>
                      ) : null}
                    </div>
                  </div>
                </section>

                <section className="cards space-top">
                  {(predictData?.items || []).map((m) => (
                    <div className="card" key={m.match_id}>
                      <div className="card-title">
                        {(m.group_label ? `[${m.group_label}] ` : '') + m.home_team} — {m.away_team}
                      </div>
                      <div className="card-text">
                        {m.kickoff} МСК
                        <br />
                        <div className="predict-row">
                          <input
                            className="score-input"
                            value={scoreInputs[m.match_id] || ''}
                            onChange={(e) =>
                              setScoreInputs((prev) => ({
                                ...prev,
                                [m.match_id]: formatScoreInput(e.target.value),
                              }))
                            }
                            placeholder="2-1"
                            inputMode="numeric"
                          />
                          <button
                            className="save-btn"
                            onClick={() => savePrediction(m.match_id)}
                            disabled={savingMatchId === m.match_id}
                          >
                            {savingMatchId === m.match_id ? 'Сохраняю...' : 'Сохранить'}
                          </button>
                        </div>
                        {m.prediction ? (
                          <>
                            Текущий прогноз: <b>{m.prediction}</b>
                          </>
                        ) : (
                          'Прогноз пока не поставлен.'
                        )}
                      </div>
                    </div>
                  ))}
                </section>
              </>
            )}
          </>
        ) : null}

        {screen === 'profile' ? (
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
        ) : null}

        {screen === 'predictions' ? (
          <>
            {showWcSelector ? (
              <section className="cards">
                <div className="card card-static">
                  <div className="card-title">Этап турнира</div>
                  <div className="tournament-row">
                    {wcTopTabsPredictions.map((tab) => (
                      <button
                        key={tab.key}
                        className={`tournament-chip ${stageTab === tab.key ? 'is-active' : ''}`}
                        onClick={() => setStageTab(tab.key)}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </div>
                  {stageTab === 'PO' ? (
                    <div className="tournament-row">
                      {wcPlayoffTabs.map((tab) => (
                        <button
                          key={tab.key}
                          className={`tournament-chip ${playoffTab === tab.key ? 'is-active' : ''}`}
                          onClick={() => setPlayoffTab(tab.key)}
                        >
                          {tab.label}
                        </button>
                      ))}
                    </div>
                  ) : null}
                </div>
              </section>
            ) : null}

            {showWcSelector && stageTab === 'LT' ? (
              <section className="cards">
                <div className="card">
                  <div className="card-title">Доп. прогнозы ЧМ 2026</div>
                  <div className="card-text">
                    {longtermError ? (
                      <>Ошибка: {longtermError}</>
                    ) : !longtermData ? (
                      'Загружаю...'
                    ) : (
                      <>
                        Дедлайн: <b>{longtermData.deadline_msk || '—'} МСК</b>
                        <br />
                        Победитель: <b>{longtermData.picks?.winner || 'не выбран'}</b>
                        <br />
                        Бомбардир: <b>{longtermData.picks?.scorer || 'не выбран'}</b>
                        <br />
                        После финала админ вручную начисляет по <b>5 очков</b> за каждый угаданный доп. прогноз.
                      </>
                    )}
                  </div>
                </div>
              </section>
            ) : (
              <>
                <section className="cards">
                  <div className="card">
                    <div className="card-title">
                      {predictionsData?.tournament || selectedTournamentCode} · {predictionsData?.round_name || `Тур ${predictionsData?.round_number ?? '—'}`}
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

                <section className="cards space-top">
                  {(predictionsData?.items || [])
                    .filter((m) => (predictionsFilter === 'open' ? m.status === 'open' : m.status === 'closed'))
                    .map((m) => (
                      <div className="card" key={m.match_id}>
                        <div className="card-title">
                          {(m.group_label ? `[${m.group_label}] ` : '') + m.home_team} — {m.away_team}
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
            )}
          </>
        ) : null}

        {screen === 'table' ? (
          <>
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

            <section className="cards space-top">
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
        ) : null}

        {showDebugPanels ? (
          <>
            <section className="cards space-top">
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

            <section className="cards space-top">
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
      </main>

      <nav className="bottom-tabs">
        <button className={`tab-btn ${screen === 'profile' ? 'is-active' : ''}`} onClick={() => setScreen('profile')}>
          👤 Профиль
        </button>
        <button className={`tab-btn ${screen === 'predict' ? 'is-active' : ''}`} onClick={() => setScreen('predict')}>
          🎯 Сделать прогноз
        </button>
        <button className={`tab-btn ${screen === 'predictions' ? 'is-active' : ''}`} onClick={() => setScreen('predictions')}>
          🗂 Мои прогнозы
        </button>
        <button className={`tab-btn ${screen === 'table' ? 'is-active' : ''}`} onClick={() => setScreen('table')}>
          🏆 Таблица
        </button>
      </nav>
    </div>
  )
}

export default App
