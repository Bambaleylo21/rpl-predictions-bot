import { useEffect, useMemo, useState } from 'react'
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
  tournament_name?: string
  tg_user_id?: number
  display_name?: string
  username?: string | null
  photo_url?: string | null
  predictions_count?: number
  total_points?: number
  exact_hits?: number
  diff_hits?: number
  outcome_hits?: number
  hit_rate?: number
  missed_matches?: number
  place?: number | null
  participants?: number
  played_matches?: number
  total_matches?: number
  tournament_progress_pct?: number
  achievements_earned?: number
  achievements_total?: number
  achievements?: Array<{
    key: string
    title: string
    emoji: string
    earned: boolean
    description?: string
  }>
  next_achievement?: {
    key: string
    title: string
    emoji: string
    current: number
    target: number
    left: number
  } | null
  recent_form?: Array<{
    round: number
    emoji: string
    points: number
    label: string
  }>
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
    missed_matches?: number
  }>
}
type TableRow = NonNullable<TableResponse['rows']>[number]

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

type Screen = 'predict' | 'profile' | 'table'

const ENGLAND_FLAG = String.fromCodePoint(
  0x1f3f4,
  0xe0067,
  0xe0062,
  0xe0065,
  0xe006e,
  0xe0067,
  0xe007f
)
const SCOTLAND_FLAG = String.fromCodePoint(
  0x1f3f4,
  0xe0067,
  0xe0062,
  0xe0073,
  0xe0063,
  0xe0074,
  0xe007f
)

const TEAM_FLAGS: Record<string, string> = {
  'Мексика': '🇲🇽',
  'ЮАР': '🇿🇦',
  'Южная Корея': '🇰🇷',
  'Чехия': '🇨🇿',
  'Канада': '🇨🇦',
  'Босния и Герцеговина': '🇧🇦',
  'США': '🇺🇸',
  'Парагвай': '🇵🇾',
  'Катар': '🇶🇦',
  'Швейцария': '🇨🇭',
  'Гаити': '🇭🇹',
  'Шотландия': SCOTLAND_FLAG,
  'Австралия': '🇦🇺',
  'Турция': '🇹🇷',
  'Германия': '🇩🇪',
  'Кюрасао': '🇨🇼',
  'Кабо-Верде': '🇨🇻',
  'Новая Зеландия': '🇳🇿',
  'Иордания': '🇯🇴',
  'ДР Конго': '🇨🇩',
  'ДРК': '🇨🇩',
  'Конго ДР': '🇨🇩',
  'Нидерланды': '🇳🇱',
  'Япония': '🇯🇵',
  "Кот-д'Ивуар": '🇨🇮',
  "Кот д'Ивуар": '🇨🇮',
  'Эквадор': '🇪🇨',
  'Швеция': '🇸🇪',
  'Тунис': '🇹🇳',
  'Аргентина': '🇦🇷',
  'Бразилия': '🇧🇷',
  'Англия': ENGLAND_FLAG,
  'Испания': '🇪🇸',
  'Франция': '🇫🇷',
  'Португалия': '🇵🇹',
  'Италия': '🇮🇹',
  'Бельгия': '🇧🇪',
  'Хорватия': '🇭🇷',
  'Сербия': '🇷🇸',
  'Польша': '🇵🇱',
  'Дания': '🇩🇰',
  'Норвегия': '🇳🇴',
  'Украина': '🇺🇦',
  'Марокко': '🇲🇦',
  'Алжир': '🇩🇿',
  'Египет': '🇪🇬',
  'Нигерия': '🇳🇬',
  'Камерун': '🇨🇲',
  'Сенегал': '🇸🇳',
  'Гана': '🇬🇭',
  'Ямайка': '🇯🇲',
  'Колумбия': '🇨🇴',
  'Уругвай': '🇺🇾',
  'Перу': '🇵🇪',
  'Чили': '🇨🇱',
  'Венесуэла': '🇻🇪',
  'Боливия': '🇧🇴',
  'Коста-Рика': '🇨🇷',
  'Панама': '🇵🇦',
  'Гондурас': '🇭🇳',
  'Сальвадор': '🇸🇻',
  'Иран': '🇮🇷',
  'Ирак': '🇮🇶',
  'Саудовская Аравия': '🇸🇦',
  'ОАЭ': '🇦🇪',
  'Узбекистан': '🇺🇿',
  'Казахстан': '🇰🇿',
  'Грузия': '🇬🇪',
  'Греция': '🇬🇷',
  'Румыния': '🇷🇴',
  'Венгрия': '🇭🇺',
  'Австрия': '🇦🇹',
  'Словакия': '🇸🇰',
  'Словения': '🇸🇮',
}

const normalizeTeamNameForLookup = (team: string): string =>
  (team || '')
    .trim()
    .replace(/[’`]/g, "'")
    .replace(/[‐‑–—]/g, '-')
    .replace(/\s+/g, ' ')

const teamWithFlag = (team: string): string => {
  const name = (team || '').trim()
  const normalized = normalizeTeamNameForLookup(name)
  const flag = TEAM_FLAGS[name]
  const fallbackFlag = TEAM_FLAGS[normalized]
  const found = flag || fallbackFlag
  return found ? `${found} ${name}` : name
}

function App() {
  const [screen, setScreen] = useState<Screen>('predict')
  const [tgUserId, setTgUserId] = useState<number | null>(null)
  const [tgUsername, setTgUsername] = useState<string | null>(null)
  const [tgPhotoUrl, setTgPhotoUrl] = useState<string | null>(null)
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
  const [tableRoundFilter, setTableRoundFilter] = useState<'ALL' | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9>('ALL')
  const [tableSortKey, setTableSortKey] = useState<'total' | 'exact' | 'diff' | 'outcome' | 'missed' | 'bonus'>('total')
  const [tableSortDir, setTableSortDir] = useState<'desc' | 'asc'>('desc')
  const [achievementsExpanded, setAchievementsExpanded] = useState<boolean>(false)

  const selectedRoundNumber =
    selectedTournamentCode === 'WC2026'
      ? (stageTab === 'PO' ? playoffTab : stageTab === 'LT' ? undefined : Number(stageTab))
      : undefined

  const formatScoreInput = (raw: string): string => {
    const digits = raw.replace(/\D/g, '').slice(0, 2)
    if (digits.length <= 1) return digits
    return `${digits.slice(0, 1)}:${digits.slice(1)}`
  }
  const normalizeScore = (raw: string): string => {
    const cleaned = (raw || '').trim().replace('-', ':')
    const m = cleaned.match(/^(\d+):(\d+)$/)
    if (!m) return ''
    return `${Number(m[1])}:${Number(m[2])}`
  }
  const showDebugPanels = import.meta.env.DEV || import.meta.env.VITE_DEBUG_PANELS === '1'

  useEffect(() => {
    let attempts = 0
    const maxAttempts = 8
    let timerId: ReturnType<typeof setTimeout> | null = null

    const expandNow = () => {
      try {
        WebApp.expand()
      } catch {
        // no-op outside Telegram
      }
    }

    try {
      WebApp.ready()
      expandNow()

      const loopExpand = () => {
        if (attempts >= maxAttempts) return
        attempts += 1
        expandNow()
        timerId = setTimeout(loopExpand, 220)
      }
      timerId = setTimeout(loopExpand, 120)

      const onViewportChanged = () => {
        expandNow()
      }
      ;(WebApp as any).onEvent?.('viewportChanged', onViewportChanged)

      return () => {
        if (timerId) clearTimeout(timerId)
        ;(WebApp as any).offEvent?.('viewportChanged', onViewportChanged)
      }
    } catch {
      // no-op outside Telegram
    }
  }, [])

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
      setTgPhotoUrl(user?.photo_url ?? null)

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

    const tableRoundParam =
      selectedTournamentCode === 'WC2026' && tableRoundFilter !== 'ALL'
        ? `&round=${encodeURIComponent(String(tableRoundFilter))}`
        : ''
    fetch(`${apiBase}/api/miniapp/table/current?t=${tParam}${tableRoundParam}`, { headers })
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
  }, [selectedTournamentCode, selectedRoundNumber, tableRoundFilter])

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
      setLongtermNotice(null)

      const headers = { 'X-Telegram-Init-Data': initData }
      const reload = await fetch(`${apiBase}/api/miniapp/longterm/current?t=${tParam}`, { headers })
      const reloadData = (await reload.json()) as LongtermResponse
      if (reload.ok && reloadData.ok) {
        setLongtermData(reloadData)
        setWinnerPickInput(reloadData.picks?.winner || '')
        setScorerPickInput(reloadData.picks?.scorer || '')
      }
    } catch (err) {
      setLongtermNotice(`Ошибка сохранения: ${String(err)}`)
    } finally {
      setSavingLongtermType(null)
    }
  }

  const tabMeta: Record<Screen, { title: string; subtitle: string; icon: string }> = {
    profile: { title: 'Профиль', subtitle: 'Личная статистика участника', icon: '👤' },
    predict: { title: 'Матчи', subtitle: 'Открытые и завершённые матчи в одном месте', icon: '⚽' },
    table: { title: 'Таблица', subtitle: 'Позиции участников турнира', icon: '🏆' },
  }
  const bottomTabs: Array<{ key: Screen; icon: string; label: string }> = [
    { key: 'profile', icon: '👤', label: 'Профиль' },
    { key: 'predict', icon: '⚽', label: 'Матчи' },
    { key: 'table', icon: '🏆', label: 'Таблица' },
  ]

  const tournamentButtons = [
    { code: 'WC2026', icon: '⚽', label: 'WC' },
    { code: 'RPL', icon: '🏆', label: 'РПЛ' },
  ]
  const showWcSelector = selectedTournamentCode === 'WC2026'
  const longtermLocked = Boolean(longtermData?.locked)
  const winnerCurrent = (longtermData?.picks?.winner || '').trim()
  const scorerCurrent = (longtermData?.picks?.scorer || '').trim()
  const winnerInputNormalized = winnerPickInput.trim()
  const scorerInputNormalized = scorerPickInput.trim()
  const winnerDirty = winnerInputNormalized !== winnerCurrent
  const scorerDirty = scorerInputNormalized !== scorerCurrent
  const winnerVisualState =
    savingLongtermType === 'winner'
      ? 'is-saving'
      : winnerDirty
        ? 'is-dirty'
        : winnerCurrent
          ? 'is-saved'
          : 'is-empty'
  const scorerVisualState =
    savingLongtermType === 'scorer'
      ? 'is-saving'
      : scorerDirty
        ? 'is-dirty'
        : scorerCurrent
          ? 'is-saved'
          : 'is-empty'

  const wcTopTabsBase: Array<{ key: '1' | '2' | '3' | 'PO'; label: string }> = [
    { key: '1', label: 'Тур 1' },
    { key: '2', label: 'Тур 2' },
    { key: '3', label: 'Тур 3' },
    { key: 'PO', label: 'Плей-офф' },
  ]
  const wcTopTabsMatches: Array<{ key: '1' | '2' | '3' | 'PO' | 'LT'; label: string }> =
    [...wcTopTabsBase, { key: 'LT', label: 'Доп. прогнозы' }]
  const wcPlayoffTabs: Array<{ key: 4 | 5 | 6 | 7 | 8 | 9; label: string }> = [
    { key: 4, label: '1/16' },
    { key: 5, label: '1/8' },
    { key: 6, label: '1/4' },
    { key: 7, label: '1/2' },
    { key: 8, label: 'За 3-е' },
    { key: 9, label: 'Финал' },
  ]
  const allowLongtermTab = showWcSelector

  const predictItems = predictData?.items || []
  const predictGroups = (() => {
    const grouped: Record<string, typeof predictItems> = {}
    for (const item of predictItems) {
      const dateKey = (item.kickoff || '').split(' ')[0] || '—'
      if (!grouped[dateKey]) grouped[dateKey] = []
      grouped[dateKey].push(item)
    }
    return Object.entries(grouped)
  })()
  const closedPredictionItems = (predictionsData?.items || []).filter((m) => m.status === 'closed')
  const closedPredictionGroups = (() => {
    const grouped: Record<string, typeof closedPredictionItems> = {}
    for (const item of closedPredictionItems) {
      const dateKey = (item.kickoff || '').split(' ')[0] || '—'
      if (!grouped[dateKey]) grouped[dateKey] = []
      grouped[dateKey].push(item)
    }
    return Object.entries(grouped)
  })()
  const openMatchesCount = predictData?.items?.length ?? 0
  const closedMatchesCount = closedPredictionItems.length

  useEffect(() => {
    if (stageTab === 'LT' && !allowLongtermTab) {
      setStageTab('1')
    }
  }, [allowLongtermTab, stageTab])

  useEffect(() => {
    if (selectedTournamentCode !== 'WC2026') {
      setTableRoundFilter('ALL')
    }
  }, [selectedTournamentCode])

  useEffect(() => {
    setAchievementsExpanded(false)
  }, [selectedTournamentCode])

  const tableRowsSorted = useMemo(() => {
    const rows = [...(tableData?.rows || [])]
    const dir = tableSortDir === 'asc' ? 1 : -1
    const getBonus = (r: TableRow) =>
      Math.max(0, (r.total ?? 0) - ((r.exact ?? 0) * 4 + (r.diff ?? 0) * 2 + (r.outcome ?? 0)))
    rows.sort((a, b) => {
      const av =
        tableSortKey === 'total'
          ? (a.total ?? 0)
          : tableSortKey === 'exact'
            ? (a.exact ?? 0)
            : tableSortKey === 'diff'
              ? (a.diff ?? 0)
              : tableSortKey === 'outcome'
                ? (a.outcome ?? 0)
                : tableSortKey === 'missed'
                  ? (a.missed_matches ?? 0)
                  : getBonus(a)
      const bv =
        tableSortKey === 'total'
          ? (b.total ?? 0)
          : tableSortKey === 'exact'
            ? (b.exact ?? 0)
            : tableSortKey === 'diff'
              ? (b.diff ?? 0)
              : tableSortKey === 'outcome'
                ? (b.outcome ?? 0)
                : tableSortKey === 'missed'
                  ? (b.missed_matches ?? 0)
                  : getBonus(b)
      if (av !== bv) return (av - bv) * dir
      return (a.place - b.place) * dir
    })
    return rows
  }, [tableData, tableSortDir, tableSortKey])

  const handleSortHeader = (key: 'total' | 'exact' | 'diff' | 'outcome' | 'missed' | 'bonus') => {
    if (tableSortKey === key) {
      setTableSortDir((prev) => (prev === 'desc' ? 'asc' : 'desc'))
      return
    }
    setTableSortKey(key)
    setTableSortDir(key === 'missed' ? 'asc' : 'desc')
  }

  const earnedAchievements = (profileData?.achievements || []).filter((a) => a.earned)
  const lockedAchievements = (profileData?.achievements || []).filter((a) => !a.earned)
  const visibleLockedAchievements = achievementsExpanded ? lockedAchievements : []

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
            {screen === 'predict' && !(showWcSelector && stageTab === 'LT') ? (
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
                    {wcTopTabsMatches.map((tab) => (
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
                    <div className="card-title">
                      Доп. прогнозы ЧМ 2026
                    </div>
                    <div className="card-text">
                      {longtermError ? (
                        <>Ошибка: {longtermError}</>
                      ) : !longtermData ? (
                        'Загружаю...'
                      ) : longtermLocked ? (
                        <>
                          Дедлайн: <b>{longtermData.deadline_msk || '—'} МСК</b>
                          <br />
                          Победитель: <b>{longtermData.picks?.winner || 'не выбран'}</b>
                          <br />
                          Бомбардир: <b>{longtermData.picks?.scorer || 'не выбран'}</b>
                          <br />
                          После финала админ вручную начисляет по <b>5 очков</b> за каждый угаданный доп. прогноз.
                        </>
                      ) : longtermData.joined === false ? (
                        'Сначала вступи в турнир, чтобы поставить доп. прогнозы.'
                      ) : !predictData ? (
                        'Загружаю...'
                      ) : (
                        <>
                          До старта первого матча:
                          <b> {longtermData.deadline_msk || '—'} МСК</b>
                          <br />
                          После дедлайна здесь будет только просмотр.
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

                {!longtermLocked ? (
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
                          className={`save-btn longterm-save-btn ${winnerVisualState}`}
                          onClick={() => saveLongtermPick('winner')}
                          disabled={savingLongtermType === 'winner' || longtermLocked || longtermData?.joined === false || !winnerDirty}
                        >
                          {savingLongtermType === 'winner' ? '…' : winnerVisualState === 'is-empty' ? '' : '✓'}
                        </button>
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
                          className={`save-btn longterm-save-btn ${scorerVisualState}`}
                          onClick={() => saveLongtermPick('scorer')}
                          disabled={savingLongtermType === 'scorer' || longtermLocked || longtermData?.joined === false || !scorerDirty}
                        >
                          {savingLongtermType === 'scorer' ? '…' : scorerVisualState === 'is-empty' ? '' : '✓'}
                        </button>
                      </div>
                    </div>
                  </section>
                ) : null}
              </>
            ) : (
              <>
                <section className="cards">
                  <div className="card">
                    <div className="card-title">
                      {predictData?.tournament || predictionsData?.tournament || selectedTournamentCode} · {predictData?.round_name || predictionsData?.round_name || `Тур ${predictData?.round_number ?? predictionsData?.round_number ?? '—'}`}
                    </div>
                    <div className="card-text">
                      {predictError || predictionsError ? (
                        <>
                          {predictError ? `Ошибка активных матчей: ${predictError}` : null}
                          {predictError && predictionsError ? <br /> : null}
                          {predictionsError ? `Ошибка завершённых матчей: ${predictionsError}` : null}
                        </>
                      ) : !predictData || !predictionsData ? (
                        'Загружаю матчи...'
                      ) : predictData.joined === false ? (
                        predictData.message || 'Нужно вступить в турнир, чтобы ставить прогнозы.'
                      ) : (
                        <>
                          Открытых матчей: <b>{openMatchesCount}</b> · Завершённых: <b>{closedMatchesCount}</b>
                          <br />
                          Очки по завершённым: <b>{predictionsData.total_points_closed ?? 0}</b>
                        </>
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
                  {predictionsFilter === 'open' ? (
                    <div className="card compact-list-card">
                      {predictGroups.length === 0 ? (
                        <div className="card-text">Открытых матчей нет.</div>
                      ) : (
                        predictGroups.map(([dateKey, matches]) => (
                          <div key={dateKey} className="day-group">
                            <div className="day-title">{dateKey}</div>
                            {matches.map((m) => (
                              <div className="compact-match" key={m.match_id}>
                                {(() => {
                                  const currentInput = normalizeScore(scoreInputs[m.match_id] || '')
                                  const savedInput = normalizeScore(m.prediction || '')
                                  const isDirty = currentInput !== savedInput
                                  const hasSaved = savedInput.length > 0
                                  const isSaving = savingMatchId === m.match_id
                                  const canSave = isDirty
                                  const saveVisualState = isSaving
                                    ? 'is-saving'
                                    : isDirty
                                      ? 'is-dirty'
                                      : hasSaved
                                        ? 'is-saved'
                                        : 'is-empty'
                                  return (
                                    <>
                                      <div className="compact-meta">
                                        {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                                        <span className="kickoff-small">{(m.kickoff || '').split(' ')[1] || ''} МСК</span>
                                      </div>
                                      <div className="compact-main">
                                        <span className="team-name team-left">{teamWithFlag(m.home_team)}</span>
                                        <input
                                          className="score-inline-input"
                                          value={scoreInputs[m.match_id] || ''}
                                          onChange={(e) =>
                                            setScoreInputs((prev) => ({
                                              ...prev,
                                              [m.match_id]: formatScoreInput(e.target.value),
                                            }))
                                          }
                                          placeholder="-:-"
                                          inputMode="numeric"
                                        />
                                        <span className="team-name team-right">{teamWithFlag(m.away_team)}</span>
                                        <button
                                          className={`save-btn compact-save-btn ${saveVisualState}`}
                                          onClick={() => savePrediction(m.match_id)}
                                          disabled={isSaving || !canSave}
                                        >
                                          {isSaving ? '…' : saveVisualState === 'is-empty' ? '' : '✓'}
                                        </button>
                                      </div>
                                    </>
                                  )
                                })()}
                              </div>
                            ))}
                          </div>
                        ))
                      )}
                    </div>
                  ) : closedPredictionGroups.length === 0 ? (
                    <div className="card">
                      <div className="card-text">Завершённых матчей пока нет.</div>
                    </div>
                  ) : (
                    <div className="card compact-list-card">
                      {closedPredictionGroups.map(([day, items]) => (
                        <div className="day-group" key={day}>
                          <div className="day-title">{day}</div>
                          {items.map((m) => (
                            <div className="compact-match" key={m.match_id}>
                              <div className="compact-meta">
                                {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                                <span className="kickoff-small">{(m.kickoff || '').split(' ')[1] || ''} МСК</span>
                              </div>
                              <div className="compact-main compact-main-result">
                                <span className="team-name team-left">{teamWithFlag(m.home_team)}</span>
                                <span className="score-inline-pill">{m.prediction || '-:-'}</span>
                                <span className="team-name team-right">{teamWithFlag(m.away_team)}</span>
                                <span className="result-badge">{m.prediction ? `${m.emoji} ${m.points ?? 0}` : '❌ 0'}</span>
                              </div>
                              <div className="compact-note">
                                Итог: {m.result || '—'}
                              </div>
                            </div>
                          ))}
                        </div>
                      ))}
                    </div>
                  )}
                </section>
              </>
            )}
          </>
        ) : null}

        {screen === 'profile' ? (
          <section className="cards">
            <div className="card">
              {profileError ? (
                <div className="card-text">Ошибка загрузки профиля: {profileError}</div>
              ) : !profileData ? (
                <div className="card-text">Загружаю профиль...</div>
              ) : profileData.joined ? (
                <>
                  <div className="profile-hero">
                    {profileData.photo_url || tgPhotoUrl ? (
                      <img
                        className="profile-avatar"
                        src={profileData.photo_url || tgPhotoUrl || ''}
                        alt="avatar"
                      />
                    ) : (
                      <div className="profile-avatar profile-avatar-fallback">
                        {(() => {
                          const name = (profileData.display_name || tgUsername || 'U').trim()
                          return name.slice(0, 2).toUpperCase()
                        })()}
                      </div>
                    )}
                    <div className="profile-hero-meta">
                      <div className="profile-name">
                        {profileData.display_name || (tgUsername ? `@${tgUsername}` : `ID ${tgUserId ?? '—'}`)}
                      </div>
                      <div className="profile-subline">
                        {profileData.tournament_name || 'Турнир'} · {profileData.league_name || 'Лига —'}
                      </div>
                      <div className="profile-subline">
                        Этап: {profileData.stage_name || '—'}
                        {profileData.stage_round_min != null && profileData.stage_round_max != null
                          ? ` (${profileData.stage_round_min}-${profileData.stage_round_max})`
                          : ''}
                      </div>
                    </div>
                  </div>

                  <div className="profile-kpi-grid">
                    <div className="profile-kpi">
                      <span>Очки</span>
                      <b>{profileData.total_points ?? 0}</b>
                    </div>
                    <div className="profile-kpi">
                      <span>Место</span>
                      <b>
                        {profileData.place != null
                          ? `${profileData.place}${profileData.participants ? `/${profileData.participants}` : ''}`
                          : '—'}
                      </b>
                    </div>
                    <div className="profile-kpi">
                      <span>Точность</span>
                      <b>{(profileData.hit_rate ?? 0).toFixed(1)}%</b>
                    </div>
                    <div className="profile-kpi">
                      <span>Пропуски</span>
                      <b>{profileData.missed_matches ?? 0}</b>
                    </div>
                  </div>

                  <div className="profile-hits-line">
                    🎯 <b>{profileData.exact_hits ?? 0}</b> · 📏 <b>{profileData.diff_hits ?? 0}</b> · ✅{' '}
                    <b>{profileData.outcome_hits ?? 0}</b> · Всего прогнозов: <b>{profileData.predictions_count ?? 0}</b>
                  </div>

                  <div className="profile-progress">
                    <div className="profile-progress-head">
                      <span>Прогресс турнира</span>
                      <b>{(profileData.tournament_progress_pct ?? 0).toFixed(1)}%</b>
                    </div>
                    <div className="profile-progress-bar">
                      <div
                        className="profile-progress-fill"
                        style={{ width: `${Math.max(0, Math.min(100, profileData.tournament_progress_pct ?? 0))}%` }}
                      />
                    </div>
                    <div className="profile-progress-meta">
                      Сыграно {profileData.played_matches ?? 0} из {profileData.total_matches ?? 0} матчей
                    </div>
                  </div>

                  {profileData.next_achievement ? (
                    <div className="profile-next-achievement">
                      <div className="profile-next-head">
                        <span>До следующей ачивки</span>
                      </div>
                      <div className="profile-next-title">
                        {profileData.next_achievement.emoji} {profileData.next_achievement.title}
                      </div>
                      <div className="profile-next-meta">
                        Прогресс: {profileData.next_achievement.current}/{profileData.next_achievement.target} · осталось {profileData.next_achievement.left}
                      </div>
                    </div>
                  ) : null}

                  {(profileData.recent_form || []).length > 0 ? (
                    <div className="profile-form">
                      <div className="profile-form-head">Форма (последние матчи)</div>
                      <div className="profile-form-list">
                        {(profileData.recent_form || []).map((item, idx) => (
                          <div className="profile-form-item" key={`${idx}-${item.round}-${item.label}`}>
                            <span className="profile-form-emoji">{item.emoji}</span>
                            <span className="profile-form-round">Т{item.round}</span>
                            <span className="profile-form-points">{item.points}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}

                  <div className="profile-achievements">
                    <div className="profile-achievements-head">
                      <span>Награды</span>
                      <b>
                        {profileData.achievements_earned ?? 0}/{profileData.achievements_total ?? 0}
                      </b>
                    </div>
                    <div className="profile-achievements-grid">
                      {earnedAchievements.map((a) => (
                        <div
                          key={a.key}
                          className={`profile-achievement ${a.earned ? 'is-earned' : 'is-locked'}`}
                          title={a.description || a.title}
                        >
                          <span className="profile-achievement-emoji">{a.emoji}</span>
                          <span className="profile-achievement-title">{a.title}</span>
                        </div>
                      ))}
                      {visibleLockedAchievements.map((a) => (
                        <div
                          key={a.key}
                          className="profile-achievement is-locked"
                          title={a.description || a.title}
                        >
                          <span className="profile-achievement-emoji">{a.emoji}</span>
                          <span className="profile-achievement-title">{a.title}</span>
                        </div>
                      ))}
                    </div>
                    {lockedAchievements.length > 0 ? (
                      <button
                        className="profile-achievements-toggle"
                        onClick={() => setAchievementsExpanded((v) => !v)}
                      >
                        {achievementsExpanded
                          ? 'Скрыть неактивные'
                          : `Показать все (${lockedAchievements.length} скрыто)`}
                      </button>
                    ) : null}
                  </div>
                </>
              ) : (
                <div className="card-text">{profileData.message || 'Пока нет активного участия в турнире.'}</div>
              )}
            </div>
          </section>
        ) : null}

        {screen === 'table' ? (
          <>
            {selectedTournamentCode === 'WC2026' ? (
              <section className="cards">
                <div className="card card-static">
                  <div className="card-title">Раунд таблицы</div>
                  <div className="tournament-row">
                    <button
                      className={`tournament-chip ${tableRoundFilter === 'ALL' ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter('ALL')}
                    >
                      Общая
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 1 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(1)}
                    >
                      Тур 1
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 2 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(2)}
                    >
                      Тур 2
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 3 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(3)}
                    >
                      Тур 3
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 4 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(4)}
                    >
                      1/16
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 5 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(5)}
                    >
                      1/8
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 6 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(6)}
                    >
                      1/4
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 7 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(7)}
                    >
                      1/2
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 8 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(8)}
                    >
                      За 3-е
                    </button>
                    <button
                      className={`tournament-chip ${tableRoundFilter === 9 ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter(9)}
                    >
                      Финал
                    </button>
                  </div>
                </div>
              </section>
            ) : null}

            <section className="cards space-top">
              {tableError ? (
                <div className="card">
                  <div className="card-text">Ошибка загрузки таблицы: {tableError}</div>
                </div>
              ) : !tableData ? (
                <div className="card">
                  <div className="card-text">Загружаю таблицу...</div>
                </div>
              ) : (tableData?.rows || []).length > 0 ? (
                <div className="card table-card">
                  <div className="table-grid table-grid-head">
                    <div className="col-place">#</div>
                    <div className="col-name">Имя</div>
                    <button className="col-sort-btn col-num" onClick={() => handleSortHeader('total')}>
                      Очк
                    </button>
                    <button className="col-sort-btn col-num" onClick={() => handleSortHeader('exact')}>
                      🎯
                    </button>
                    <button className="col-sort-btn col-num" onClick={() => handleSortHeader('diff')}>
                      📏
                    </button>
                    <button className="col-sort-btn col-num" onClick={() => handleSortHeader('outcome')}>
                      ✅
                    </button>
                    <button className="col-sort-btn col-num" onClick={() => handleSortHeader('missed')}>
                      ⛔
                    </button>
                    <button className="col-sort-btn col-num" onClick={() => handleSortHeader('bonus')}>
                      ⭐
                    </button>
                  </div>

                  {tableRowsSorted.map((r) => {
                    const basePoints = (r.exact * 4) + (r.diff * 2) + (r.outcome * 1)
                    const bonusPoints = Math.max(0, (r.total ?? 0) - basePoints)
                    return (
                      <div
                        className={`table-grid table-grid-row ${tableData?.user_place === r.place ? 'is-user' : ''}`}
                        key={`${r.place}-${r.name}`}
                      >
                        <div className="col-place">{r.place}</div>
                        <div className="col-name col-name-text">{r.name}</div>
                        <div className="col-num">{r.total}</div>
                        <div className="col-num">{r.exact}</div>
                        <div className="col-num">{r.diff}</div>
                        <div className="col-num">{r.outcome}</div>
                        <div className="col-num">{r.missed_matches ?? 0}</div>
                        <div className="col-num">{bonusPoints}</div>
                      </div>
                    )
                  })}
                </div>
              ) : (
                <div className="card">
                  <div className="card-text">
                    {tableData.message || 'Таблица ещё не сформирована. Ждём первые рассчитанные матчи.'}
                  </div>
                </div>
              )}
            </section>

            <section className="cards space-top">
              <div className="card">
                <div className="card-title">Расшифровка</div>
                <div className="card-text">
                  🎯 Точный счёт: 4 очка
                  <br />
                  📏 Разница мячей: 2 очка
                  <br />
                  ✅ Исход: 1 очко
                  <br />
                  ⛔ Пропущенные матчи: 0 очков
                  <br />
                  ⭐ Доп. прогнозы / бонусы: +5 за каждый угаданный доп. прогноз
                </div>
              </div>
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
        {bottomTabs.map((tab) => (
          <button
            key={tab.key}
            className={`tab-btn ${screen === tab.key ? 'is-active' : ''}`}
            onClick={() => setScreen(tab.key)}
            aria-label={tab.label}
          >
            <span className="tab-icon">{tab.icon}</span>
            <span className="tab-label">{tab.label}</span>
          </button>
        ))}
      </nav>
    </div>
  )
}

export default App
