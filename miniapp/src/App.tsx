import { useEffect, useMemo, useState } from 'react'
import './App.css'
import WebApp from '@twa-dev/sdk'
import wcActiveIcon from './assets/tournaments/wc-active.png'
import wcInactiveIcon from './assets/tournaments/wc-inactive.png'
import rplActiveIcon from './assets/tournaments/rpl-active.png'
import rplInactiveIcon from './assets/tournaments/rpl-inactive.png'

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
  is_admin?: boolean
  note: string
}

type ProfileResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  joined?: boolean
  tournament_code?: string
  tournament_name?: string
  tg_user_id?: number
  viewed_tg_user_id?: number
  is_self_profile?: boolean
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
  duel_rating?: number
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
    taken_by_other?: boolean
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
  insights?: string[]
  recent_form?: Array<{
    round: number
    emoji: string
    points: number
    label: string
  }>
  live_statuses?: string[]
  form_statuses?: string[]
  tournament_history?: Array<{
    tournament_code: string
    tournament_name: string
    place: number
    participants: number
    total_points: number
    exact: number
    diff: number
    outcome: number
    missed_matches: number
    hit_rate: number
  }>
  legacy_trophies?: Array<{
    season: string
    title: string
    format: string
    place: number
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
    crowd_count?: number
    crowd_home_pct?: number
    crowd_draw_pct?: number
    crowd_away_pct?: number
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
    crowd_count?: number
    crowd_home_pct?: number
    crowd_draw_pct?: number
    crowd_away_pct?: number
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
    tg_user_id?: number
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

type DuelItem = {
  duel_id: number
  status: 'pending' | 'accepted' | 'finished' | 'declined' | 'expired'
  match_id: number
  home_team: string
  away_team: string
  group_label?: string | null
  kickoff: string
  result?: string | null
  challenger_tg_user_id: number
  challenger_name: string
  challenger_pred: string
  opponent_tg_user_id: number
  opponent_name: string
  opponent_pred?: string | null
  risk_multiplier_bp: number
  outcome?: string | null
  winner_tg_user_id?: number | null
  elo_delta_challenger: number
  elo_delta_opponent: number
  challenger_rating?: number
  opponent_rating?: number
  h2h_wins?: number
  h2h_draws?: number
  h2h_losses?: number
}

type DuelsResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  joined?: boolean
  message?: string
  tournament_code?: string
  tournament_name?: string
  elo?: {
    rating: number
    duels_total: number
    wins: number
    losses: number
    draws: number
  }
  match_options?: Array<{
    match_id: number
    home_team: string
    away_team: string
    group_label?: string | null
    kickoff: string
    blocked_for_user?: boolean
  }>
  opponents?: Array<{
    tg_user_id: number
    display_name: string
    elo_rating?: number
  }>
  active?: DuelItem[]
  finished?: DuelItem[]
}

type AchievementItem = NonNullable<ProfileResponse['achievements']>[number]
type AchievementLevel = 'bronze' | 'silver' | 'gold' | null
type AchievementVisual = {
  iconUrl: string | null
  iconEmoji: string
  isSecretLocked: boolean
  displayTitle: string
  displayDescription: string
}
type AchievementWithVisual = AchievementItem & { visual: AchievementVisual }

type Screen = 'predict' | 'duels' | 'profile' | 'table' | 'admin'

type AdminRound = {
  round: number
  round_name: string
  total: number
  without_result: number
}

type AdminRoundsResponse = {
  ok: boolean
  error?: string
  reason?: string
  rounds?: AdminRound[]
  current_round?: number
}

type AdminResultItem = {
  match_id: number
  home_team: string
  away_team: string
  group_label?: string | null
  kickoff: string
  has_result: boolean
  result?: string | null
  predictions_count?: number
}

type AdminResultsCurrentResponse = {
  ok: boolean
  error?: string
  reason?: string
  round_number?: number
  round_name?: string
  mode?: 'open' | 'all'
  round_total?: number
  without_result?: number
  items?: AdminResultItem[]
}

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

const crowdText = (item: {
  crowd_count?: number
  crowd_home_pct?: number
  crowd_draw_pct?: number
  crowd_away_pct?: number
}): string | null => {
  const count = Number(item.crowd_count || 0)
  if (count < 4) return `0% · 0% · 0%`
  const h = Number(item.crowd_home_pct || 0)
  const d = Number(item.crowd_draw_pct || 0)
  const a = Number(item.crowd_away_pct || 0)
  return `${h}% · ${d}% · ${a}%`
}

const ACHIEVEMENT_ICON_MODULES = import.meta.glob('./assets/achievements/*.png', {
  eager: true,
  import: 'default',
}) as Record<string, string>

const SECRET_ACHIEVEMENT_KEYS = new Set<string>(['fergie_time_hit', 'high_scoring_exact', 'only_scorer_in_match'])

const normalizeAchievementKey = (value: string): string =>
  (value || '')
    .trim()
    .toLowerCase()
    .replace(/\.png$/i, '')
    .replace(/[^\w]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')

const ACHIEVEMENT_ICON_BY_KEY: Record<string, string> = Object.entries(ACHIEVEMENT_ICON_MODULES).reduce(
  (acc, [path, url]) => {
    const fileName = path.split('/').pop() || ''
    const normalized = normalizeAchievementKey(fileName)
    if (normalized) acc[normalized] = url
    return acc
  },
  {} as Record<string, string>
)

const parseAchievementLevel = (rawKey: string, title?: string): AchievementLevel => {
  const key = normalizeAchievementKey(rawKey)
  const titleNorm = normalizeAchievementKey(title || '')
  const levelTokens: AchievementLevel[] = ['bronze', 'silver', 'gold']
  for (const level of levelTokens) {
    if (!level) continue
    if (key.endsWith(`_${level}`) || titleNorm.includes(level)) {
      return level
    }
  }
  if (titleNorm.includes('бронз')) return 'bronze'
  if (titleNorm.includes('серебр')) return 'silver'
  if (titleNorm.includes('золот')) return 'gold'
  return null
}

const stripAchievementLevel = (rawKey: string): string => normalizeAchievementKey(rawKey).replace(/_(bronze|silver|gold)$/i, '')
const LEVEL_ACHIEVEMENT_BASES = new Set<string>(['no_miss_tour_streak', 'scoring_match_streak', 'duel_wins_total'])
const LEVEL_ORDER: Array<'bronze' | 'silver' | 'gold'> = ['bronze', 'silver', 'gold']

const getAchievementLevelGroupBase = (rawKey: string): string | null => {
  const key = normalizeAchievementKey(rawKey)
  const match = key.match(/^(.*)_(bronze|silver|gold)$/)
  if (!match) return null
  const base = stripAchievementLevel(key)
  return LEVEL_ACHIEVEMENT_BASES.has(base) ? base : null
}

const levelLabel = (level: 'bronze' | 'silver' | 'gold'): string => {
  if (level === 'bronze') return 'Уровень 1'
  if (level === 'silver') return 'Уровень 2'
  return 'Уровень 3'
}

const resolveAchievementIconUrl = (key: string, title: string | undefined, earned: boolean): string | null => {
  const normalizedKey = normalizeAchievementKey(key)
  const isSecret = SECRET_ACHIEVEMENT_KEYS.has(normalizedKey)
  if (isSecret && !earned) {
    return ACHIEVEMENT_ICON_BY_KEY['secret_locked'] || null
  }

  const level = parseAchievementLevel(normalizedKey, title)
  const baseKey = stripAchievementLevel(normalizedKey)
  const candidates = [
    normalizedKey,
    baseKey,
    level ? `${baseKey}_${level}` : '',
    level ? `${baseKey}${level}` : '',
  ].filter(Boolean)

  for (const candidate of candidates) {
    const found = ACHIEVEMENT_ICON_BY_KEY[normalizeAchievementKey(candidate)]
    if (found) return found
  }
  return null
}

const buildAchievementVisual = (achievement: AchievementItem): AchievementVisual => {
  const keyNorm = normalizeAchievementKey(achievement.key)
  const isSecretLocked = SECRET_ACHIEVEMENT_KEYS.has(keyNorm) && !achievement.earned
  return {
    iconUrl: resolveAchievementIconUrl(achievement.key, achievement.title, achievement.earned),
    iconEmoji: isSecretLocked ? '🔒' : achievement.emoji,
    isSecretLocked,
    displayTitle: isSecretLocked ? 'Секретная ачивка' : achievement.title,
    displayDescription: isSecretLocked
      ? 'Откроется после выполнения скрытого условия.'
      : achievement.description || achievement.title,
  }
}

function App() {
  const [screen, setScreen] = useState<Screen>('predict')
  const [duelFocusId, setDuelFocusId] = useState<number | null>(null)
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
  const [profileTargetUserId, setProfileTargetUserId] = useState<number | null>(null)
  const [stageTab, setStageTab] = useState<'1' | '2' | '3' | 'PO' | 'LT'>('1')
  const [playoffTab, setPlayoffTab] = useState<4 | 5 | 6 | 7 | 8 | 9>(4)
  const [tableRoundFilter, setTableRoundFilter] = useState<'ALL' | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9>('ALL')
  const [tableSortKey, setTableSortKey] = useState<'total' | 'exact' | 'diff' | 'outcome' | 'missed' | 'bonus'>('total')
  const [tableSortDir, setTableSortDir] = useState<'desc' | 'asc'>('desc')
  const [achievementsExpanded, setAchievementsExpanded] = useState<boolean>(false)
  const [achievementPreview, setAchievementPreview] = useState<AchievementWithVisual | null>(null)
  const [historyExpanded, setHistoryExpanded] = useState<boolean>(false)
  const [currentInsight, setCurrentInsight] = useState<string | null>(null)
  const [adminRounds, setAdminRounds] = useState<AdminRound[]>([])
  const [adminRound, setAdminRound] = useState<number | null>(null)
  const [adminMode, setAdminMode] = useState<'open' | 'all'>('open')
  const [adminResults, setAdminResults] = useState<AdminResultItem[]>([])
  const [adminRoundName, setAdminRoundName] = useState<string>('')
  const [adminRoundTotal, setAdminRoundTotal] = useState<number>(0)
  const [adminWithoutResult, setAdminWithoutResult] = useState<number>(0)
  const [adminError, setAdminError] = useState<string | null>(null)
  const [adminNotice, setAdminNotice] = useState<string | null>(null)
  const [adminSavingMatchId, setAdminSavingMatchId] = useState<number | null>(null)
  const [adminScoreInputs, setAdminScoreInputs] = useState<Record<number, string>>({})
  const [adminRecalcLoading, setAdminRecalcLoading] = useState<boolean>(false)
  const [duelsData, setDuelsData] = useState<DuelsResponse | null>(null)
  const [duelsError, setDuelsError] = useState<string | null>(null)
  const [duelsNotice, setDuelsNotice] = useState<string | null>(null)
  const [duelMatchId, setDuelMatchId] = useState<number>(0)
  const [duelOpponentId, setDuelOpponentId] = useState<number>(0)
  const [duelScoreInput, setDuelScoreInput] = useState<string>('')
  const [duelBusyId, setDuelBusyId] = useState<number | null>(null)
  const [duelsFilter, setDuelsFilter] = useState<'active' | 'finished'>('active')
  const [duelAcceptInputs, setDuelAcceptInputs] = useState<Record<number, string>>({})
  const [duelMatchPickerOpen, setDuelMatchPickerOpen] = useState<boolean>(false)
  const [duelOpponentPickerOpen, setDuelOpponentPickerOpen] = useState<boolean>(false)
  const [duelMatchSearch, setDuelMatchSearch] = useState<string>('')
  const [duelOpponentSearch, setDuelOpponentSearch] = useState<string>('')
  const [duelMatchVisibleCount, setDuelMatchVisibleCount] = useState<number>(20)

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
    if (screen !== 'duels') {
      setDuelMatchPickerOpen(false)
      setDuelOpponentPickerOpen(false)
    }
  }, [screen])

  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search || '')
      const startParamRaw =
        ((window as any).Telegram?.WebApp?.initDataUnsafe?.start_param as string | undefined) ||
        ((WebApp as any)?.initDataUnsafe?.start_param as string | undefined) ||
        ''
      let startParams = new URLSearchParams()
      if (startParamRaw) {
        try {
          // start_param expected like: "screen=duels&duel_id=12"
          startParams = new URLSearchParams(startParamRaw)
        } catch {
          startParams = new URLSearchParams()
        }
      }
      const getParam = (key: string): string =>
        params.get(key) || startParams.get(key) || ''
      const screenParam = getParam('screen').toLowerCase()
      if (screenParam === 'profile' || screenParam === 'matches' || screenParam === 'predict') {
        setScreen('predict')
      } else if (screenParam === 'table') {
        setScreen('table')
      } else if (screenParam === 'duels') {
        setScreen('duels')
      } else if (screenParam === 'admin') {
        setScreen('admin')
      }

      const duelRaw = getParam('duel_id')
      const duelId = Number(duelRaw)
      if (Number.isFinite(duelId) && duelId > 0) {
        setDuelFocusId(Math.trunc(duelId))
        setScreen('duels')
      }
    } catch {
      // no-op
    }
  }, [])

  useEffect(() => {
    let attempts = 0
    const maxAttempts = 8
    let timerId: ReturnType<typeof setTimeout> | null = null
    const appBg = '#0b1220'

    const expandNow = () => {
      try {
        WebApp.expand()
      } catch {
        // no-op outside Telegram
      }
    }

    try {
      WebApp.ready()
      try {
        WebApp.setBackgroundColor(appBg as any)
        WebApp.setHeaderColor(appBg as any)
        ;(WebApp as any).setBottomBarColor?.(appBg)
      } catch {
        // no-op for clients without these methods
      }
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

  const loadDuelsCurrent = async (apiBase: string, initData: string, tournamentCode: string) => {
    const headers = {
      'X-Telegram-Init-Data': initData,
    }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const res = await fetch(`${apiBase}/api/miniapp/duels/current?t=${tParam}`, { headers })
    const data = (await res.json()) as DuelsResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setDuelsData(data)
    setDuelsError(null)
    setDuelMatchVisibleCount(20)

    if (
      duelMatchId &&
      !(data.match_options || []).some((m) => Number(m.match_id) === Number(duelMatchId))
    ) {
      setDuelMatchId(0)
    }
    if (
      duelOpponentId &&
      !(data.opponents || []).some((u) => Number(u.tg_user_id) === Number(duelOpponentId))
    ) {
      setDuelOpponentId(0)
    }

    if (duelFocusId != null && duelFocusId > 0) {
      const inActive = (data.active || []).some((d) => Number(d.duel_id) === Number(duelFocusId))
      const inFinished = (data.finished || []).some((d) => Number(d.duel_id) === Number(duelFocusId))
      if (inActive) {
        setDuelsFilter('active')
      } else if (inFinished) {
        setDuelsFilter('finished')
      }
    }
  }

  useEffect(() => {
    if (screen !== 'duels' || duelFocusId == null || duelFocusId <= 0) return
    const t = setTimeout(() => {
      const el = document.getElementById(`duel-card-${duelFocusId}`)
      if (!el) return
      el.scrollIntoView({ block: 'center', behavior: 'smooth' })
    }, 80)
    return () => clearTimeout(t)
  }, [screen, duelFocusId, duelsFilter, duelsData])

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
    const targetParam =
      profileTargetUserId != null ? `&target_user_id=${encodeURIComponent(String(profileTargetUserId))}` : ''

    fetch(`${apiBase}/api/miniapp/profile?t=${tParam}${targetParam}`, { headers })
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
  }, [selectedTournamentCode, profileTargetUserId])

  useEffect(() => {
    setHistoryExpanded(false)
  }, [selectedTournamentCode, profileTargetUserId, profileData?.viewed_tg_user_id])

  useEffect(() => {
    setAchievementsExpanded(false)
    setAchievementPreview(null)
  }, [selectedTournamentCode, profileTargetUserId, profileData?.viewed_tg_user_id])

  useEffect(() => {
    if (screen === 'profile') return
    setAchievementPreview(null)
  }, [screen])

  useEffect(() => {
    if (!achievementPreview) return
    const onKeyDown = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') {
        setAchievementPreview(null)
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [achievementPreview])

  useEffect(() => {
    if (screen !== 'profile') return
    const pool = profileData?.insights || []
    if (!pool.length) {
      setCurrentInsight(null)
      return
    }
    const idx = Math.floor(Math.random() * pool.length)
    setCurrentInsight(pool[idx] || null)
  }, [
    screen,
    selectedTournamentCode,
    profileTargetUserId,
    profileData?.viewed_tg_user_id,
    profileData?.insights?.join('||'),
  ])

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

    loadDuelsCurrent(apiBase, initData, selectedTournamentCode).catch((err) => {
      setDuelsError(String(err))
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
      setProfileTargetUserId(null)
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

  const createDuelChallenge = async () => {
    const score = normalizeScore(duelScoreInput)
    if (!duelMatchId || !duelOpponentId || !score) {
      setDuelsNotice('Выбери матч, соперника и счёт в формате 2:1.')
      return
    }
    const [left, right] = score.split(':')
    const predHome = Number(left)
    const predAway = Number(right)

    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setDuelsNotice(null)
    setDuelBusyId(-1)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/duels/challenge?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({
          match_id: duelMatchId,
          opponent_tg_user_id: duelOpponentId,
          pred_home: predHome,
          pred_away: predAway,
        }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setDuelsNotice('Вызов отправлен.')
      setDuelScoreInput('')
      await loadDuelsCurrent(apiBase, initData, selectedTournamentCode)
    } catch (err) {
      setDuelsNotice(`Ошибка 1x1: ${String(err)}`)
    } finally {
      setDuelBusyId(null)
    }
  }

  const respondDuel = async (duelId: number, action: 'accept' | 'decline', score?: string) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setDuelBusyId(duelId)
    setDuelsNotice(null)
    try {
      const payload: Record<string, any> = { duel_id: duelId, action }
      if (action === 'accept') {
        const normalized = normalizeScore(score || '')
        if (!normalized) {
          throw new Error('Для принятия введи счёт 2:1')
        }
        const [l, r] = normalized.split(':')
        payload.pred_home = Number(l)
        payload.pred_away = Number(r)
      }

      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/duels/respond?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify(payload),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; status?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setDuelsNotice(action === 'accept' ? 'Вызов принят.' : 'Вызов отклонён.')
      await loadDuelsCurrent(apiBase, initData, selectedTournamentCode)
    } catch (err) {
      setDuelsNotice(`Ошибка 1x1: ${String(err)}`)
    } finally {
      setDuelBusyId(null)
    }
  }

  const loadAdminRounds = async (apiBase: string, initData: string, tournamentCode: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const res = await fetch(`${apiBase}/api/miniapp/admin/rounds?t=${tParam}`, { headers })
    const data = (await res.json()) as AdminRoundsResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    const rounds = data.rounds || []
    setAdminRounds(rounds)
    setAdminRound((prev) => prev ?? (data.current_round || rounds[0]?.round || null))
  }

  const loadAdminResults = async (
    apiBase: string,
    initData: string,
    tournamentCode: string,
    roundNumber: number,
    mode: 'open' | 'all'
  ) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const rParam = encodeURIComponent(String(roundNumber))
    const res = await fetch(`${apiBase}/api/miniapp/admin/results/current?t=${tParam}&round=${rParam}&mode=${mode}`, { headers })
    const data = (await res.json()) as AdminResultsCurrentResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    const items = data.items || []
    setAdminResults(items)
    setAdminRoundName(data.round_name || '')
    setAdminRoundTotal(data.round_total || 0)
    setAdminWithoutResult(data.without_result || 0)
    const nextInputs: Record<number, string> = {}
    for (const item of items) {
      nextInputs[item.match_id] = item.result || ''
    }
    setAdminScoreInputs(nextInputs)
  }

  const saveAdminResult = async (matchId: number) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    const score = normalizeScore(adminScoreInputs[matchId] || '')
    if (!score) {
      setAdminNotice('Введи счёт в формате 2:1 или 2-1')
      return
    }
    setAdminSavingMatchId(matchId)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/result/set?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ match_id: matchId, score }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; updated_points?: number }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(`Счёт сохранён. Обновлено очков: ${data.updated_points ?? 0}`)
      if (adminRound != null) {
        await loadAdminResults(apiBase, initData, selectedTournamentCode, adminRound, adminMode)
      }
    } catch (err) {
      setAdminNotice(`Ошибка сохранения: ${String(err)}`)
    } finally {
      setAdminSavingMatchId(null)
    }
  }

  const recalcAdminRound = async () => {
    if (adminRound == null) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminRecalcLoading(true)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/recalc_round?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ round_number: adminRound }),
      })
      const data = (await res.json()) as {
        ok?: boolean
        error?: string
        reason?: string
        matches_recalced?: number
        updated_points?: number
      }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(
        `Пересчёт завершён: матчей ${data.matches_recalced ?? 0}, обновлений очков ${data.updated_points ?? 0}`
      )
      await loadAdminResults(apiBase, initData, selectedTournamentCode, adminRound, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка пересчёта: ${String(err)}`)
    } finally {
      setAdminRecalcLoading(false)
    }
  }

  useEffect(() => {
    if (!meData?.is_admin) {
      setAdminRounds([])
      setAdminRound(null)
      setAdminResults([])
      setAdminError(null)
      setAdminNotice(null)
      if (screen === 'admin') {
        setScreen('predict')
      }
      return
    }

    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminRounds(apiBase, initData, selectedTournamentCode)
      .then(() => setAdminError(null))
      .catch((err) => {
        setAdminError(String(err))
        setAdminRounds([])
        setAdminRound(null)
      })
  }, [meData?.is_admin, selectedTournamentCode])

  useEffect(() => {
    if (!meData?.is_admin || adminRound == null) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminResults(apiBase, initData, selectedTournamentCode, adminRound, adminMode)
      .then(() => setAdminError(null))
      .catch((err) => {
        setAdminError(String(err))
        setAdminResults([])
      })
  }, [meData?.is_admin, selectedTournamentCode, adminRound, adminMode])

  const tabMeta: Record<Screen, { title: string; subtitle: string; icon: string }> = {
    profile: { title: 'Профиль', subtitle: 'Личная статистика участника', icon: '👤' },
    predict: { title: 'Матчи', subtitle: 'Открытые и завершённые матчи в одном месте', icon: '⚽' },
    duels: { title: '1x1', subtitle: 'Вызовы, споры и рейтинг Elo', icon: '⚔️' },
    table: { title: 'Таблица', subtitle: 'Позиции участников турнира', icon: '🏆' },
    admin: { title: 'Админ', subtitle: 'Внесение итогов и пересчёт очков', icon: '🛠️' },
  }
  const bottomTabs: Array<{ key: Screen; icon: string; label: string }> = [
    { key: 'profile', icon: '👤', label: 'Профиль' },
    { key: 'predict', icon: '⚽', label: 'Матчи' },
    { key: 'duels', icon: '⚔️', label: '1x1' },
    { key: 'table', icon: '🏆', label: 'Таблица' },
  ]
  if (meData?.is_admin) {
    bottomTabs.push({ key: 'admin', icon: '🛠️', label: 'Админ' })
  }

  const tournamentButtons = [
    { code: 'WC2026', label: 'WC', activeIcon: wcActiveIcon, inactiveIcon: wcInactiveIcon },
    { code: 'RPL', label: 'РПЛ', activeIcon: rplActiveIcon, inactiveIcon: rplInactiveIcon },
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

  const duelMatchOptions = duelsData?.match_options || []
  const duelOpponents = duelsData?.opponents || []
  const duelSelectedMatch = duelMatchOptions.find((m) => Number(m.match_id) === Number(duelMatchId)) || null
  const duelSelectedOpponent =
    duelOpponents.find((u) => Number(u.tg_user_id) === Number(duelOpponentId)) || null
  const duelMatchSearchNorm = duelMatchSearch.trim().toLowerCase()
  const duelOpponentSearchNorm = duelOpponentSearch.trim().toLowerCase()
  const duelFilteredMatches = duelMatchOptions.filter((m) => {
    if (!duelMatchSearchNorm) return true
    const hay = `${m.home_team} ${m.away_team} ${m.group_label || ''} ${m.kickoff}`.toLowerCase()
    return hay.includes(duelMatchSearchNorm)
  })
  const duelFilteredOpponents = duelOpponents.filter((u) => {
    if (!duelOpponentSearchNorm) return true
    return `${u.display_name} ${u.elo_rating || 1000}`.toLowerCase().includes(duelOpponentSearchNorm)
  })
  const duelVisibleMatches = duelFilteredMatches.slice(0, duelMatchVisibleCount)
  const duelCanShowMoreMatches = duelFilteredMatches.length > duelVisibleMatches.length
  const duelStatusText: Record<string, string> = {
    pending: 'Ожидает',
    accepted: 'Принята',
    finished: 'Завершена',
    declined: 'Отклонена',
    expired: 'Истекла',
  }

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
  const openPredictionCount = predictData?.items?.length || 0
  const closedPredictionCount = closedPredictionItems.length
  const closedPredictionGroups = (() => {
    const grouped: Record<string, typeof closedPredictionItems> = {}
    for (const item of closedPredictionItems) {
      const dateKey = (item.kickoff || '').split(' ')[0] || '—'
      if (!grouped[dateKey]) grouped[dateKey] = []
      grouped[dateKey].push(item)
    }
    return Object.entries(grouped)
  })()
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
      // Позиция в таблице всегда должна идти 1,2,3... при равных значениях сортировки.
      return a.place - b.place
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

  const allAchievements = profileData?.achievements || []
  const achievementsWithVisuals: AchievementWithVisual[] = allAchievements.map((a) => ({
    ...a,
    visual: buildAchievementVisual(a),
  }))
  const hasHiddenAchievements = achievementsWithVisuals.length > 3
  const visibleAchievements = achievementsExpanded ? achievementsWithVisuals : achievementsWithVisuals.slice(0, 3)
  const achievementPreviewVisual = achievementPreview?.visual || null
  const achievementPreviewGroupBase = achievementPreview ? getAchievementLevelGroupBase(achievementPreview.key) : null
  const achievementPreviewGroup: AchievementWithVisual[] | null =
    achievementPreviewGroupBase && achievementPreview
      ? LEVEL_ORDER.map((level) => {
          const key = `${achievementPreviewGroupBase}_${level}`
          const found = achievementsWithVisuals.find((a) => normalizeAchievementKey(a.key) === key)
          if (found) return found
          const fallback: AchievementItem = {
            key,
            title: levelLabel(level),
            emoji: achievementPreview.emoji,
            earned: false,
            taken_by_other: false,
            description: achievementPreview.description,
          }
          return {
            ...fallback,
            visual: buildAchievementVisual(fallback),
          }
        })
      : null
  const legacyTrophies = profileData?.legacy_trophies || []
  const tournamentHistory = profileData?.tournament_history || []
  const medalGold =
    legacyTrophies.filter((h) => h.place === 1).length +
    tournamentHistory.filter((h) => h.place === 1).length
  const medalSilver =
    legacyTrophies.filter((h) => h.place === 2).length +
    tournamentHistory.filter((h) => h.place === 2).length
  const medalBronze =
    legacyTrophies.filter((h) => h.place === 3).length +
    tournamentHistory.filter((h) => h.place === 3).length

  return (
    <div className="app-shell">
      <header className="topbar sticky">
        <div className="topbar-row">
          <div>
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
                  <img
                    src={selectedTournamentCode === t.code ? t.activeIcon : t.inactiveIcon}
                    alt={t.label}
                  />
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

      <main className={`content screen-${screen}`}>
        {screen === 'predict' ? (
          <>
            {showWcSelector ? (
              <section className="cards">
                <div className="card card-static segment-card">
                  <div className="card-title">Этап турнира</div>
                  <div className="segment-hint">Нажми, чтобы выбрать этап</div>
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

            {predictData?.joined !== false && (predictData || predictionsData) ? (
              <section className="cards space-top">
                <div className="card card-static matches-overview-card">
                  <div className="matches-overview-head">Сводка матчей</div>
                  <div className="matches-overview-row">
                    <span className="matches-overview-pill">
                      Активные: <b>{openPredictionCount}</b>
                    </span>
                    <span className="matches-overview-pill">
                      Завершённые: <b>{closedPredictionCount}</b>
                    </span>
                    {predictionsData?.total_points_closed != null ? (
                      <span className="matches-overview-pill">
                        Очки: <b>{predictionsData.total_points_closed}</b>
                      </span>
                    ) : null}
                  </div>
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
                {(predictError || predictionsError || predictNotice || (!predictData || !predictionsData) || (predictData?.joined === false)) ? (
                  <section className="cards">
                    <div className="card">
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
                        ) : null}
                        {predictNotice ? (
                          <>
                            <br />
                            {predictNotice}
                          </>
                        ) : null}
                      </div>
                    </div>
                  </section>
                ) : null}

                <section className="cards space-top">
                  {predictionsFilter === 'open' ? (
                    <div className="card compact-list-card">
                      {predictGroups.length === 0 ? (
                        <div className="card-text">Открытых матчей нет.</div>
                      ) : (
                        predictGroups.map(([dateKey, matches]) => (
                          <div key={dateKey} className="day-group day-group-inset">
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
                                      {crowdText(m) ? <div className="community-small">{crowdText(m)}</div> : null}
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
                        <div className="day-group day-group-inset" key={day}>
                          <div className="day-title">{day}</div>
                          {items.map((m) => (
                            <div className="compact-match" key={m.match_id}>
                              <div className="compact-meta">
                                {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                                <span className="kickoff-small">{(m.kickoff || '').split(' ')[1] || ''} МСК</span>
                              </div>
                              {crowdText(m) ? <div className="community-small">{crowdText(m)}</div> : null}
                              <div className="compact-main compact-main-result">
                                <span className="team-name team-left">{teamWithFlag(m.home_team)}</span>
                                <span className="score-inline-pill">{m.prediction || '-:-'}</span>
                                <span className="team-name team-right">{teamWithFlag(m.away_team)}</span>
                                <span className="result-badge">{m.prediction ? `${m.emoji} ${m.points ?? 0}` : '❌ 0'}</span>
                              </div>
                              <div className="compact-note compact-note-under-score">
                                <span className="compact-note-under-score-text">
                                  Итог: {m.result || '—'}
                                </span>
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
              {profileData?.joined && profileData.is_self_profile === false ? (
                <button
                  className="profile-back-btn"
                  onClick={() => {
                    setScreen('table')
                    setProfileTargetUserId(null)
                  }}
                >
                  ← К таблице
                </button>
              ) : null}
              {profileError ? (
                <div className="card-text">Ошибка загрузки профиля: {profileError}</div>
              ) : !profileData ? (
                <div className="card-text">Загружаю профиль...</div>
              ) : profileData.joined ? (
                <>
                  <div className="profile-hero">
                    {(() => {
                      const avatarSrc =
                        profileData.photo_url ||
                        (profileData.is_self_profile !== false ? tgPhotoUrl : null)
                      return avatarSrc ? (
                      <img
                        className={`profile-avatar ${profileData.place === 1 ? 'is-top-1' : ''}`}
                        src={avatarSrc}
                        alt="avatar"
                      />
                    ) : (
                      <div className={`profile-avatar profile-avatar-fallback ${profileData.place === 1 ? 'is-top-1' : ''}`}>
                        {(() => {
                          const name = (profileData.display_name || tgUsername || 'U').trim()
                          return name.slice(0, 2).toUpperCase()
                        })()}
                      </div>
                    )})()}
                    <div className="profile-hero-main">
                      <div className="profile-hero-meta">
                        <div className="profile-name">
                          {profileData.display_name || (tgUsername ? `@${tgUsername}` : `ID ${tgUserId ?? '—'}`)}
                        </div>
                        {profileData.tournament_code === 'WC2026' ? (
                          <div className="profile-subline">{profileData.tournament_name || 'Турнир'}</div>
                        ) : (
                          <>
                            <div className="profile-subline">
                              {profileData.tournament_name || 'Турнир'} · {profileData.league_name || 'Лига —'}
                            </div>
                            <div className="profile-subline">
                              Этап: {profileData.stage_name || '—'}
                              {profileData.stage_round_min != null && profileData.stage_round_max != null
                                ? ` (${profileData.stage_round_min}-${profileData.stage_round_max})`
                                : ''}
                            </div>
                          </>
                        )}
                      </div>
                      {(profileData.live_statuses || []).length > 0 || (profileData.form_statuses || []).length > 0 ? (
                        <div className="profile-inline-statuses">
                          {(profileData.live_statuses || []).slice(0, 2).map((s, idx) => (
                            <div className="profile-inline-status" key={`inline-live-${idx}-${s}`}>
                              {s}
                            </div>
                          ))}
                          {(profileData.form_statuses || []).slice(0, 1).map((s, idx) => (
                            <div className="profile-inline-status is-form" key={`inline-form-${idx}-${s}`}>
                              {s}
                            </div>
                          ))}
                        </div>
                      ) : null}
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
                      <span>Рейтинг</span>
                      <b>{profileData.duel_rating ?? 1000}</b>
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

                  {currentInsight ? (
                    <div className="profile-insight">
                      <div className="profile-insight-head">Инсайты</div>
                      <div className="profile-insight-text">{currentInsight}</div>
                    </div>
                  ) : null}

                  {(profileData.recent_form || []).length > 0 ? (
                    <div className="profile-form">
                      <div className="profile-form-head">Форма (последние матчи)</div>
                      <div className="profile-form-list">
                        {(profileData.recent_form || []).map((item, idx) => (
                          <div
                            className="profile-form-item"
                            key={`${idx}-${item.round}-${item.label}`}
                            title={item.label}
                          >
                            <span className="profile-form-emoji">{item.emoji}</span>
                            <span className="profile-form-points">{item.points}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}

                  <div className="profile-achievements">
                    <div className="profile-achievements-head-row">
                      <div className="profile-achievements-head">
                        <span>Ачивки</span>
                        <b>
                          {profileData.achievements_earned ?? 0}/{profileData.achievements_total ?? 0}
                        </b>
                      </div>
                      {hasHiddenAchievements ? (
                        <button
                          className="profile-history-toggle"
                          onClick={() => setAchievementsExpanded((v) => !v)}
                        >
                          {achievementsExpanded ? 'Скрыть' : 'Показать больше'}
                        </button>
                      ) : null}
                    </div>
                    {allAchievements.length > 0 ? (
                      <div className="profile-achievements-grid">
                        {visibleAchievements.map((a) => (
                          <button
                            key={a.key}
                            type="button"
                            className={`profile-achievement profile-achievement-btn ${
                              a.earned ? 'is-earned' : 'is-locked'
                            } ${a.taken_by_other ? 'is-taken' : ''} ${a.visual.isSecretLocked ? 'is-secret-locked' : ''}`}
                            title={a.visual.displayDescription}
                            onClick={() => setAchievementPreview(a)}
                          >
                            <span className="profile-achievement-emoji">
                              {a.visual.iconUrl ? (
                                <img src={a.visual.iconUrl} alt={a.visual.displayTitle} className="profile-achievement-icon" />
                              ) : (
                                a.visual.iconEmoji
                              )}
                            </span>
                          </button>
                        ))}
                      </div>
                    ) : (
                      <div className="card-text">Пока нет доступных ачивок.</div>
                    )}
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

                  <div className="profile-history">
                    <div className="profile-history-head-row">
                      <div className="profile-history-head">История турниров</div>
                      {(legacyTrophies.length > 0 || tournamentHistory.length > 0) ? (
                        <button
                          className="profile-history-toggle"
                          onClick={() => setHistoryExpanded((v) => !v)}
                        >
                          {historyExpanded ? 'Скрыть' : 'Показать'}
                        </button>
                      ) : null}
                    </div>

                    {(legacyTrophies.length > 0 || tournamentHistory.length > 0) ? (
                      <div className="profile-trophy-summary">
                        🥇 <b>{medalGold}</b> · 🥈 <b>{medalSilver}</b> · 🥉 <b>{medalBronze}</b>
                      </div>
                    ) : null}

                    {historyExpanded ? (
                      <>
                        {legacyTrophies.length > 0 ? (
                          <div className="profile-history-list">
                            {legacyTrophies.map((h, idx) => (
                              <div className="profile-history-item" key={`legacy-${idx}-${h.season}-${h.title}`}>
                                <div className="profile-history-title">
                                  {h.season} · {h.title}
                                </div>
                                <div className="profile-history-meta">
                                  {h.format} · <b>{h.place} место</b>
                                </div>
                              </div>
                            ))}
                          </div>
                        ) : null}

                        {tournamentHistory.length > 0 ? (
                          <div className="profile-history-list">
                            {tournamentHistory.map((h) => (
                              <div className="profile-history-item" key={`${h.tournament_code}-${h.tournament_name}`}>
                                <div className="profile-history-title">{h.tournament_name}</div>
                                <div className="profile-history-meta">
                                  Место: <b>{h.place}/{h.participants}</b> · Очки: <b>{h.total_points}</b>
                                </div>
                                <div className="profile-history-meta">
                                  🎯{h.exact} · 📏{h.diff} · ✅{h.outcome} · ⛔{h.missed_matches} · Точность {h.hit_rate.toFixed(1)}%
                                </div>
                              </div>
                            ))}
                          </div>
                        ) : null}
                      </>
                    ) : null}

                    {!historyExpanded &&
                    legacyTrophies.length === 0 &&
                    tournamentHistory.length === 0 ? (
                      <div className="card-text">Пока нет завершённых турниров в истории.</div>
                    ) : null}
                  </div>
                </>
              ) : (
                <div className="card-text">{profileData.message || 'Пока нет активного участия в турнире.'}</div>
              )}
            </div>
          </section>
        ) : null}

        {screen === 'duels' ? (
          <>
            <section className="cards">
              <div className="card">
                {duelsError ? (
                  <div className="card-text">Ошибка загрузки 1x1: {duelsError}</div>
                ) : !duelsData ? (
                  <div className="card-text">Загружаю блок 1x1...</div>
                ) : duelsData.joined === false ? (
                  <div className="card-text">{duelsData.message || 'Сначала вступи в турнир.'}</div>
                ) : (
                  <>
                    <div className="card-title">{profileData?.display_name || (tgUsername ? `@${tgUsername}` : 'Участник')}</div>
                    <div className="profile-hits-line">
                      Рейтинг: <b>{duelsData.elo?.rating ?? 1000}</b> · W <b>{duelsData.elo?.wins ?? 0}</b> · D <b>{duelsData.elo?.draws ?? 0}</b> · L{' '}
                      <b>{duelsData.elo?.losses ?? 0}</b> · всего <b>{duelsData.elo?.duels_total ?? 0}</b>
                    </div>

                    <div className="card-title" style={{ marginTop: 10 }}>Бросить вызов</div>
                    <div className="duel-picker-wrap">
                      <button
                        className={`duel-picker-btn ${duelMatchPickerOpen ? 'is-open' : ''}`}
                        onClick={() => {
                          setDuelMatchPickerOpen((v) => !v)
                          setDuelOpponentPickerOpen(false)
                        }}
                      >
                        <span className="duel-picker-label">Матч</span>
                        <span className="duel-picker-value">
                          {duelSelectedMatch
                            ? `${duelSelectedMatch.kickoff} · ${duelSelectedMatch.home_team} — ${duelSelectedMatch.away_team}`
                            : 'Выбрать матч'}
                        </span>
                        <span className={`duel-picker-chevron ${duelMatchPickerOpen ? 'is-open' : ''}`}>⌄</span>
                      </button>
                      {duelMatchPickerOpen ? (
                        <div className="duel-picker-panel">
                          <input
                            className="duel-picker-search"
                            value={duelMatchSearch}
                            onChange={(e) => {
                              setDuelMatchSearch(e.target.value)
                              setDuelMatchVisibleCount(20)
                            }}
                            placeholder="Поиск матча"
                          />
                          <div className="duel-picker-list">
                            {duelVisibleMatches.map((m) => (
                              <button
                                key={m.match_id}
                                className={`duel-picker-item ${duelMatchId === m.match_id ? 'is-selected' : ''} ${
                                  m.blocked_for_user ? 'is-disabled' : ''
                                }`}
                                onClick={() => {
                                  if (m.blocked_for_user) return
                                  setDuelMatchId(m.match_id)
                                  setDuelMatchPickerOpen(false)
                                }}
                                disabled={Boolean(m.blocked_for_user)}
                              >
                                <div className="duel-picker-item-top">
                                  <span>{m.kickoff} МСК</span>
                                  {m.group_label ? <span>[{m.group_label}]</span> : null}
                                </div>
                                <div className="duel-picker-item-title">
                                  {m.home_team} — {m.away_team}
                                </div>
                                {m.blocked_for_user ? (
                                  <div className="duel-picker-item-note">Уже есть активная дуэль на этот матч</div>
                                ) : null}
                              </button>
                            ))}
                          </div>
                          {duelCanShowMoreMatches ? (
                            <button
                              className="duel-picker-more"
                              onClick={() => setDuelMatchVisibleCount((v) => v + 20)}
                            >
                              Показать ещё
                            </button>
                          ) : null}
                        </div>
                      ) : null}
                    </div>

                    <div className="duel-picker-wrap">
                      <button
                        className={`duel-picker-btn ${duelOpponentPickerOpen ? 'is-open' : ''}`}
                        onClick={() => {
                          setDuelOpponentPickerOpen((v) => !v)
                          setDuelMatchPickerOpen(false)
                        }}
                      >
                        <span className="duel-picker-label">Соперник</span>
                        <span className="duel-picker-value">
                          {duelSelectedOpponent
                            ? `${duelSelectedOpponent.display_name} · ${duelSelectedOpponent.elo_rating || 1000}`
                            : 'Выбрать соперника'}
                        </span>
                        <span className={`duel-picker-chevron ${duelOpponentPickerOpen ? 'is-open' : ''}`}>⌄</span>
                      </button>
                      {duelOpponentPickerOpen ? (
                        <div className="duel-picker-panel">
                          <input
                            className="duel-picker-search"
                            value={duelOpponentSearch}
                            onChange={(e) => setDuelOpponentSearch(e.target.value)}
                            placeholder="Поиск соперника"
                          />
                          <div className="duel-picker-list">
                            {duelFilteredOpponents.map((u) => (
                              <button
                                key={u.tg_user_id}
                                className={`duel-picker-item ${duelOpponentId === u.tg_user_id ? 'is-selected' : ''}`}
                                onClick={() => {
                                  setDuelOpponentId(u.tg_user_id)
                                  setDuelOpponentPickerOpen(false)
                                }}
                              >
                                <div className="duel-picker-item-title">{u.display_name}</div>
                                <div className="duel-picker-item-note">Рейтинг: {u.elo_rating || 1000}</div>
                              </button>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </div>

                    <div className="predict-row" style={{ marginTop: 8 }}>
                      <input
                        className="score-input"
                        value={duelScoreInput}
                        onChange={(e) => setDuelScoreInput(formatScoreInput(e.target.value))}
                        placeholder="-:-"
                        inputMode="numeric"
                      />
                      <button
                        className={`save-btn ${normalizeScore(duelScoreInput) ? 'is-dirty' : 'is-empty'}`}
                        onClick={createDuelChallenge}
                        disabled={duelBusyId === -1}
                      >
                        {duelBusyId === -1 ? '…' : normalizeScore(duelScoreInput) ? '✓' : ''}
                      </button>
                    </div>
                    {duelsNotice ? <div className="card-text" style={{ marginTop: 10 }}>{duelsNotice}</div> : null}
                  </>
                )}
              </div>
            </section>

            {duelsData?.joined ? (
              <section className="cards space-top">
                <div className="card card-static">
                  <div className="match-toggle">
                    <button
                      className={`match-toggle-btn ${duelsFilter === 'active' ? 'is-active' : ''}`}
                      onClick={() => setDuelsFilter('active')}
                    >
                      Активные
                    </button>
                    <button
                      className={`match-toggle-btn ${duelsFilter === 'finished' ? 'is-active' : ''}`}
                      onClick={() => setDuelsFilter('finished')}
                    >
                      Завершённые
                    </button>
                  </div>
                </div>
              </section>
            ) : null}

            {duelsData?.joined ? (
              <section className="cards space-top">
                <div className="card compact-list-card">
                  {(duelsFilter === 'active' ? duelsData.active : duelsData.finished)?.length ? (
                    (duelsFilter === 'active' ? duelsData.active : duelsData.finished)!.map((d) => {
                      const isIncomingPending =
                        d.status === 'pending' && tgUserId != null && Number(d.opponent_tg_user_id) === Number(tgUserId)
                      return (
                        <div
                          className={`compact-match ${duelFocusId === d.duel_id ? 'is-focused' : ''}`}
                          key={`duel-${d.duel_id}`}
                          id={`duel-card-${d.duel_id}`}
                        >
                          <div className="compact-meta">
                            {d.group_label ? <span className="group-small">[{d.group_label}]</span> : <span className="group-small">—</span>}
                            <span className="kickoff-small">{d.kickoff} МСК</span>
                          </div>
                          {duelsFilter === 'finished' ? (
                            <div className="compact-main compact-main-result-only">
                              <span className="team-name team-left">{teamWithFlag(d.home_team)}</span>
                              <span className="score-inline-pill">{d.result || '-:-'}</span>
                              <span className="team-name team-right">{teamWithFlag(d.away_team)}</span>
                            </div>
                          ) : (
                            <div className="compact-main compact-main-duel">
                              <span className="team-name team-left">{teamWithFlag(d.home_team)}</span>
                              <span className="duel-vs-sep">—</span>
                              <span className="team-name team-right">{teamWithFlag(d.away_team)}</span>
                              <span className="result-badge duel-status-badge">{duelStatusText[d.status] || d.status}</span>
                            </div>
                          )}

                          <div className="duel-preds">
                            <div className="duel-pred-line">
                              <span className="duel-pred-text">
                                {duelsFilter === 'finished' ? (
                                  <>
                                    <b>{d.challenger_rating || 1000}</b>{' '}
                                    <span className={d.elo_delta_challenger >= 0 ? 'duel-delta-plus' : 'duel-delta-minus'}>
                                      ({d.elo_delta_challenger >= 0 ? `+${d.elo_delta_challenger}` : d.elo_delta_challenger})
                                    </span>{' '}
                                  </>
                                ) : (
                                  <>
                                    <b>{d.challenger_rating || 1000}</b>{' '}
                                  </>
                                )}
                                {d.challenger_name} <b>{d.challenger_pred}</b>
                              </span>
                            </div>
                            <div className="duel-pred-line">
                              <span className="duel-pred-text">
                                {duelsFilter === 'finished' ? (
                                  <>
                                    <b>{d.opponent_rating || 1000}</b>{' '}
                                    <span className={d.elo_delta_opponent >= 0 ? 'duel-delta-plus' : 'duel-delta-minus'}>
                                      ({d.elo_delta_opponent >= 0 ? `+${d.elo_delta_opponent}` : d.elo_delta_opponent})
                                    </span>{' '}
                                  </>
                                ) : (
                                  <>
                                    <b>{d.opponent_rating || 1000}</b>{' '}
                                  </>
                                )}
                                {d.opponent_name} <b>{d.opponent_pred || '—'}</b>
                              </span>
                            </div>
                            <div className="compact-note">
                              Личные встречи: <b>{d.h2h_wins || 0}-{d.h2h_draws || 0}-{d.h2h_losses || 0}</b> (W-D-L)
                            </div>
                          </div>

                          {isIncomingPending ? (
                            <div className="predict-row" style={{ marginTop: 8 }}>
                              <input
                                className="score-input"
                                value={duelAcceptInputs[d.duel_id] || ''}
                                onChange={(e) =>
                                  setDuelAcceptInputs((prev) => ({
                                    ...prev,
                                    [d.duel_id]: formatScoreInput(e.target.value),
                                  }))
                                }
                                placeholder="-:-"
                                inputMode="numeric"
                              />
                              <button
                                className="save-btn is-dirty"
                                onClick={() => respondDuel(d.duel_id, 'accept', duelAcceptInputs[d.duel_id] || '')}
                                disabled={duelBusyId === d.duel_id}
                              >
                                ✓
                              </button>
                            </div>
                          ) : null}
                        </div>
                      )
                    })
                  ) : (
                    <div className="card-text">
                      {duelsFilter === 'active' ? 'Активных дуэлей пока нет.' : 'Завершённых дуэлей пока нет.'}
                    </div>
                  )}
                </div>
              </section>
            ) : null}
          </>
        ) : null}

        {screen === 'table' ? (
          <>
            {selectedTournamentCode === 'WC2026' ? (
              <section className="cards">
                <div className="card card-static">
                  <div className="card-title">Раунд таблицы</div>
                  <div className="segment-hint">Нажми, чтобы выбрать раунд</div>
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
                        <div className="col-name col-name-text">
                          {r.tg_user_id ? (
                            <button
                              className="table-name-btn"
                              onClick={() => {
                                setProfileTargetUserId(r.tg_user_id || null)
                                setScreen('profile')
                              }}
                            >
                              {r.name}
                            </button>
                          ) : (
                            r.name
                          )}
                        </div>
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

        {screen === 'admin' ? (
          <>
            <section className="cards">
              <div className="card card-static">
                <div className="card-title">Тур для внесения итогов</div>
                <div className="segment-hint">Нажми, чтобы выбрать тур и матч</div>
                <div className="tournament-row">
                  {adminRounds.map((r) => (
                    <button
                      key={r.round}
                      className={`tournament-chip ${adminRound === r.round ? 'is-active' : ''}`}
                      onClick={() => setAdminRound(r.round)}
                    >
                      {r.round_name || `Тур ${r.round}`} · {r.without_result}/{r.total}
                    </button>
                  ))}
                </div>

                <div className="admin-top-row">
                  <div className="match-toggle">
                    <button
                      className={`match-toggle-btn ${adminMode === 'open' ? 'is-active' : ''}`}
                      onClick={() => setAdminMode('open')}
                    >
                      Без итогов
                    </button>
                    <button
                      className={`match-toggle-btn ${adminMode === 'all' ? 'is-active' : ''}`}
                      onClick={() => setAdminMode('all')}
                    >
                      Все
                    </button>
                  </div>

                  <button
                    className="admin-recalc-btn"
                    onClick={recalcAdminRound}
                    disabled={adminRecalcLoading || adminRound == null}
                  >
                    {adminRecalcLoading ? 'Пересчёт…' : 'Пересчитать тур'}
                  </button>
                </div>

                <div className="card-text">
                  {adminRoundName ? (
                    <>
                      Выбран: <b>{adminRoundName}</b> · матчей: <b>{adminRoundTotal}</b> · без итогов: <b>{adminWithoutResult}</b>
                    </>
                  ) : (
                    'Выбери тур.'
                  )}
                  {adminError ? (
                    <>
                      <br />
                      Ошибка: {adminError}
                    </>
                  ) : null}
                  {adminNotice ? (
                    <>
                      <br />
                      {adminNotice}
                    </>
                  ) : null}
                </div>
              </div>
            </section>

            <section className="cards space-top">
              <div className="card compact-list-card">
                {adminResults.length === 0 ? (
                  <div className="card-text">Матчей для показа нет.</div>
                ) : (
                  adminResults.map((m) => (
                    <div className="compact-match" key={m.match_id}>
                      <div className="compact-meta">
                        {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                        <span className="kickoff-small">{m.kickoff || '—'}</span>
                      </div>
                      <div className="compact-main admin-main">
                        <span className="team-name team-left">{teamWithFlag(m.home_team)}</span>
                        <input
                          className="score-inline-input"
                          value={adminScoreInputs[m.match_id] || ''}
                          onChange={(e) =>
                            setAdminScoreInputs((prev) => ({
                              ...prev,
                              [m.match_id]: formatScoreInput(e.target.value),
                            }))
                          }
                          placeholder="-:-"
                          inputMode="numeric"
                        />
                        <span className="team-name team-right">{teamWithFlag(m.away_team)}</span>
                        <button
                          className={`save-btn compact-save-btn ${
                            normalizeScore(adminScoreInputs[m.match_id] || '') ? 'is-dirty' : 'is-empty'
                          }`}
                          onClick={() => saveAdminResult(m.match_id)}
                          disabled={adminSavingMatchId === m.match_id}
                        >
                          {adminSavingMatchId === m.match_id ? '…' : '✓'}
                        </button>
                      </div>
                      <div className="compact-note">
                        Итог: <b>{m.result || 'не задан'}</b> · Прогнозов: <b>{m.predictions_count ?? 0}</b>
                      </div>
                    </div>
                  ))
                )}
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

        {achievementPreview && achievementPreviewVisual ? (
          <div className="achievement-modal-overlay" onClick={() => setAchievementPreview(null)}>
            <div className="achievement-modal-card" onClick={(ev) => ev.stopPropagation()}>
              <button
                type="button"
                className="achievement-modal-close"
                onClick={() => setAchievementPreview(null)}
                aria-label="Закрыть"
              >
                ✕
              </button>
              <div className="achievement-modal-icon-wrap">
                {achievementPreviewVisual.iconUrl ? (
                  <img
                    src={achievementPreviewVisual.iconUrl}
                    alt={achievementPreviewVisual.displayTitle}
                    className="achievement-modal-icon"
                  />
                ) : (
                  <span className="achievement-modal-emoji">{achievementPreviewVisual.iconEmoji}</span>
                )}
              </div>
              <div className="achievement-modal-title">{achievementPreviewVisual.displayTitle}</div>
              <div className="achievement-modal-description">{achievementPreviewVisual.displayDescription}</div>
              {achievementPreviewGroup ? (
                <div className="achievement-levels">
                  {achievementPreviewGroup.map((item, idx) => (
                    <div className={`achievement-level-item ${item.earned ? 'is-earned' : 'is-locked'}`} key={item.key}>
                      <div className="achievement-level-icon-wrap">
                        {item.visual.iconUrl ? (
                          <img src={item.visual.iconUrl} alt={item.visual.displayTitle} className="achievement-level-icon" />
                        ) : (
                          <span className="achievement-level-emoji">{item.visual.iconEmoji}</span>
                        )}
                      </div>
                      <div className="achievement-level-meta">
                        <div className="achievement-level-title">{levelLabel(LEVEL_ORDER[idx]!)}</div>
                        <div className="achievement-level-state">{item.earned ? 'Получена' : 'Не получена'}</div>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="achievement-modal-meta">{achievementPreview.earned ? 'Получена' : 'Не получена'}</div>
              )}
            </div>
          </div>
        ) : null}

        <footer className="footer-note">Статус: Mini App авторизован и получает профиль из API.</footer>
      </main>

      <nav
        className="bottom-tabs"
        style={{ gridTemplateColumns: `repeat(${bottomTabs.length}, minmax(0, 1fr))` }}
      >
        {bottomTabs.map((tab) => (
          <button
            key={tab.key}
            className={`tab-btn ${screen === tab.key ? 'is-active' : ''}`}
            onClick={() => {
              if (tab.key === 'profile') {
                setProfileTargetUserId(null)
              }
              setScreen(tab.key)
            }}
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
