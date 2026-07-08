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
  achievement_progress?: {
    no_miss_tour_streak?: number
    scoring_match_streak?: number
    duel_wins_total?: number
  }
  achievements?: Array<{
    key: string
    title: string
    emoji: string
    earned: boolean
    taken_by_other?: boolean
    taken_by_name?: string | null
    description?: string
    match_context?: {
      match_id: number
      home_team: string
      away_team: string
      result?: string | null
      prediction?: string | null
      points?: number | null
      total_after?: number | null
    } | null
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
    is_placeholder?: boolean
    locked?: boolean
    group_label?: string | null
    kickoff: string
    prediction: string | null
    crowd_count?: number
    crowd_home_pct?: number
    crowd_draw_pct?: number
    crowd_away_pct?: number
  }>
}

type MatchPredictionsResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  tournament_code?: string
  tournament?: string
  match_id?: number
  home_team?: string
  away_team?: string
  group_label?: string | null
  kickoff?: string
  status?: 'live' | 'closed'
  result?: string | null
  items?: Array<{
    tg_user_id: number
    name: string
    prediction: string | null
    points?: number | null
    category?: string | null
    emoji?: string | null
    is_me?: boolean
    place_before?: number | null
    place_after?: number | null
    place_delta?: number | null
  }>
}

type TableResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  table_mode?: 'regular' | 'longterm'
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
    bonus_points?: number
    exact: number
    diff: number
    outcome: number
    pred_total: number
    hits: number
    hit_rate: number
    missed_matches?: number
  }>
  rows_longterm?: Array<{
    tg_user_id?: number
    place: number
    name: string
    winner_pick?: string | null
    scorer_pick?: string | null
    longterm_points: number
  }>
}
type TableRow = NonNullable<TableResponse['rows']>[number]
type TableLongtermRow = NonNullable<TableResponse['rows_longterm']>[number]
type WcStageTab =
  | { type: 'stage'; key: '1' | '2' | '3' | 'LT'; label: string }
  | { type: 'playoff'; key: 4 | 5 | 6 | 7 | 8 | 9; label: string }
const MATCH_STAGE_ROUND_NUMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9] as const

type MatchStageAvailability = {
  round: number
  round_name?: string
  total: number
  real_total: number
  placeholders: number
  open: number
  closed: number
  completed: boolean
  latest_closed_kickoff: string
}

type MatchStagesResponse = {
  ok: boolean
  error?: string
  reason?: string
  trusted?: boolean
  tournament_code?: string
  tournament?: string
  stages?: MatchStageAvailability[]
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

type NotificationsPrefsResponse = {
  ok: boolean
  error?: string
  reason?: string
  all?: boolean
  reminders?: boolean
  duels?: boolean
  achievements?: boolean
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
  status: 'pending' | 'accepted' | 'finished' | 'declined' | 'expired' | 'cancelled'
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
  challenger_rating_before?: number | null
  opponent_rating_before?: number | null
  challenger_rating_after?: number | null
  opponent_rating_after?: number | null
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
  busy_opponents_by_match?: Record<string, number[]>
  opponents?: Array<{
    tg_user_id: number
    display_name: string
    elo_rating?: number
  }>
  leaderboard?: Array<{
    tg_user_id: number
    display_name: string
    rating: number
    duels_total: number
    wins: number
    losses: number
    draws: number
    place: number
  }>
  active?: DuelItem[]
  finished?: DuelItem[]
}

type AdminDuelsCurrentResponse = {
  ok: boolean
  error?: string
  reason?: string
  tournament_code?: string
  tournament_name?: string
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
  round_number?: number
  round_name?: string
  home_team: string
  away_team: string
  is_placeholder?: boolean
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
  scope?: 'all' | 'round'
  mode?: 'open' | 'all'
  round_total?: number
  without_result?: number
  items?: AdminResultItem[]
}

type AdminLongtermCurrentResponse = {
  ok: boolean
  error?: string
  reason?: string
  winner_actual?: string | null
  scorer_actual?: string | null
  participants?: number
  winner_awarded?: number
  scorer_awarded?: number
  options?: {
    winner?: string[]
    scorer?: string[]
  }
}

type AdminPlayoffSlotItem = {
  match_id: number
  round_number: number
  round_name: string
  kickoff: string
  home_team: string
  away_team: string
  is_placeholder: boolean
  is_filled: boolean
}

type AdminPlayoffSlotsCurrentResponse = {
  ok: boolean
  error?: string
  reason?: string
  items?: AdminPlayoffSlotItem[]
}

type AdminParticipantItem = {
  tg_user_id: number
  display_name: string
  bonus_points: number
  joined_at: string
}

type AdminParticipantsCurrentResponse = {
  ok: boolean
  error?: string
  reason?: string
  items?: AdminParticipantItem[]
}

type RplStageInfo = {
  id: number
  name: string
  round_min: number
  round_max: number
  is_active: boolean
  is_completed: boolean
}

type RplLeagueInfo = {
  id: number
  code: string
  name: string
}

type RplSeasonCounts = {
  total_members: number
  unassigned: number
  HIGH: number
  LOW: number
}

type AdminRplSeasonResponse = {
  ok: boolean
  error?: string
  reason?: string
  season: { id: number; name: string } | null
  stages: RplStageInfo[]
  leagues: RplLeagueInfo[]
  enrollment_open: boolean
  counts: RplSeasonCounts
}

type RplParticipantItem = {
  tg_user_id: number
  display_name: string
  league_code: string | null
}

type AdminRplParticipantsResponse = {
  ok: boolean
  error?: string
  reason?: string
  season_name?: string
  stage_name?: string
  items?: RplParticipantItem[]
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

const kickoffSortValue = (value?: string | null): number => {
  const match = String(value || '').match(/^(\d{2})\.(\d{2})(?:\s+(\d{2}):(\d{2}))?/)
  if (!match) return 0
  const [, day, month, hour = '00', minute = '00'] = match
  return Number(month) * 1000000 + Number(day) * 10000 + Number(hour) * 100 + Number(minute)
}

const teamOptionsWithFlags = Object.keys(TEAM_FLAGS).sort((a, b) => a.localeCompare(b, 'ru'))

const crowdText = (item: {
  crowd_count?: number
  crowd_home_pct?: number
  crowd_draw_pct?: number
  crowd_away_pct?: number
}): string | null => {
  const [h, d, a] = crowdPercentParts(item)
  return `${h} · ${d} · ${a}`
}

const crowdPercentParts = (item: {
  crowd_count?: number
  crowd_home_pct?: number
  crowd_draw_pct?: number
  crowd_away_pct?: number
}): [string, string, string] => {
  const count = Number(item.crowd_count || 0)
  if (count < 4) return ['0%', '0%', '0%']
  const h = Number(item.crowd_home_pct || 0)
  const d = Number(item.crowd_draw_pct || 0)
  const a = Number(item.crowd_away_pct || 0)
  return [`${h}%`, `${d}%`, `${a}%`]
}

const positionChangeLabel = (row: {
  place_after?: number | null
  place_delta?: number | null
}): { label: string; tone: 'up' | 'down' | 'same' } | null => {
  const delta = Number(row.place_delta ?? 0)
  if (delta > 0) return { label: `↑${delta}`, tone: 'up' }
  if (delta < 0) return { label: `↓${Math.abs(delta)}`, tone: 'down' }
  const placeAfter = Number(row.place_after || 0)
  return placeAfter > 0 ? { label: `•${placeAfter}`, tone: 'same' } : null
}

const intOrZero = (value: unknown): number => {
  const n = Number(value)
  if (!Number.isFinite(n)) return 0
  return Math.max(0, Math.trunc(n))
}

const ACHIEVEMENT_ICON_MODULES = import.meta.glob('./assets/achievements/*.png', {
  eager: true,
  import: 'default',
}) as Record<string, string>

const SECRET_ACHIEVEMENT_KEYS = new Set<string>(['fergie_time_hit', 'high_scoring_exact', 'only_scorer_in_match'])
const UNIQUE_ACHIEVEMENT_KEYS = new Set<string>([
  'first_exact_tournament',
  'last_exact_tournament',
  'first_leader_after_round1',
  'first_101_points',
  'group_stage_winner_after_round3',
])

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
const LEVEL_TARGETS_BY_BASE: Record<string, number[]> = {
  no_miss_tour_streak: [1, 2, 3],
  scoring_match_streak: [3, 5, 10],
  duel_wins_total: [5, 10, 20],
}
const LOCKED_ACHIEVEMENT_HINTS: Record<string, string> = {
  scoring_match_streak_bronze: 'Набери очки в 3 матчах подряд.',
  scoring_match_streak_silver: 'Набери очки в 5 матчах подряд.',
  scoring_match_streak_gold: 'Набери очки в 10 матчах подряд.',
  no_miss_tour_streak_bronze: 'Проставь прогнозы на все матчи одного тура.',
  no_miss_tour_streak_silver: 'Проставь прогнозы на все матчи в 3 турах.',
  no_miss_tour_streak_gold: 'Проставь прогнозы на все матчи турнира.',
  duel_wins_total_bronze: 'Выиграй 5 дуэлей 1x1.',
  duel_wins_total_silver: 'Выиграй 10 дуэлей 1x1.',
  duel_wins_total_gold: 'Выиграй 20 дуэлей 1x1.',
  first_exact_tournament: 'Стань первым участником турнира, кто угадает точный счёт.',
  last_exact_tournament: 'Стань последним участником, кто угадает точный счёт.',
  first_leader_after_round1: 'Займи 1 место после завершения 1 тура.',
  first_101_points: 'Стань первым участником турнира, кто наберёт 100+ очков.',
  group_stage_winner_after_round3: 'Займи 1 место после полного завершения 3 туров.',
}

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

const resolveAchievementIconUrl = (
  key: string,
  title: string | undefined,
  earned: boolean,
  revealLockedSecret = false
): string | null => {
  const normalizedKey = normalizeAchievementKey(key)
  const isSecret = SECRET_ACHIEVEMENT_KEYS.has(normalizedKey)
  if (isSecret && !earned && !revealLockedSecret) {
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
  const revealLockedSecret = Boolean(isSecretLocked && achievement.taken_by_other)
  return {
    iconUrl: resolveAchievementIconUrl(achievement.key, achievement.title, achievement.earned, revealLockedSecret),
    iconEmoji: isSecretLocked && !revealLockedSecret ? '🔒' : achievement.emoji,
    isSecretLocked,
    displayTitle: isSecretLocked ? 'Секретная ачивка' : achievement.title,
    displayDescription: isSecretLocked
      ? 'Откроется после выполнения скрытого условия.'
      : achievement.description || achievement.title,
  }
}

const isUniqueAchievement = (key: string): boolean => UNIQUE_ACHIEVEMENT_KEYS.has(normalizeAchievementKey(key))
const getLockedAchievementHintText = (achievement: AchievementItem): string => {
  const key = normalizeAchievementKey(achievement.key)
  return LOCKED_ACHIEVEMENT_HINTS[key] || achievement.description || 'Выполни условие этой ачивки в матчах турнира.'
}

const DUEL_RULES_SECTIONS: Array<{ title: string; lines: string[] }> = [
  {
    title: 'Что это',
    lines: ['1x1 - дуэль двух участников на одном матче. Это личное противостояние с отдельным рейтингом Elo.'],
  },
  {
    title: 'Как играть',
    lines: [
      '1. Выбери матч, соперника и свой прогноз.',
      '2. Соперник получает вызов и принимает или отклоняет его.',
      '3. Если вызов принят, соперник ставит прогноз на тот же матч.',
      '4. После расчёта счёта дуэль закрывается автоматически.',
    ],
  },
  {
    title: 'Как определяется победитель',
    lines: [
      'Очки за прогноз: 4 - точный счёт, 2 - исход + разница, 1 - только исход, 0 - мимо.',
      'Больше очков - победа. Равенство очков - ничья.',
    ],
  },
  {
    title: 'Как начисляется Elo',
    lines: [
      'База: 1000.',
      'После каждой завершённой дуэли рейтинг обновляется у обоих.',
      'Победа над более сильным соперником даёт больше рейтинга.',
      'При ничьей андердог может получить плюс, фаворит - небольшой минус.',
      'За поражение рейтинг снижается.',
      'Чем сильнее отличаются прогнозы, тем заметнее изменение рейтинга.',
    ],
  },
  {
    title: 'Ограничения',
    lines: [
      'На один матч у одной пары участников возможна только одна дуэль.',
      'Нельзя вызвать самого себя.',
      'Нельзя принять дуэль после старта матча.',
      'Просроченные/непринятые дуэли не попадают в завершённые.',
    ],
  },
]

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
  const [savingAllPredictions, setSavingAllPredictions] = useState<boolean>(false)
  const [predictNotice, setPredictNotice] = useState<string | null>(null)
  const [matchPredictionsSheet, setMatchPredictionsSheet] = useState<MatchPredictionsResponse | null>(null)
  const [matchPredictionsLoadingId, setMatchPredictionsLoadingId] = useState<number | null>(null)
  const [matchPredictionsError, setMatchPredictionsError] = useState<string | null>(null)
  const [tableData, setTableData] = useState<TableResponse | null>(null)
  const [tableError, setTableError] = useState<string | null>(null)
  const [longtermData, setLongtermData] = useState<LongtermResponse | null>(null)
  const [longtermError, setLongtermError] = useState<string | null>(null)
  const [longtermNotice, setLongtermNotice] = useState<string | null>(null)
  const [savingLongtermType, setSavingLongtermType] = useState<'winner' | 'scorer' | null>(null)
  const [savingAllLongterm, setSavingAllLongterm] = useState<boolean>(false)
  const [winnerPickInput, setWinnerPickInput] = useState<string>('')
  const [scorerPickInput, setScorerPickInput] = useState<string>('')
  const [winnerPickerOpen, setWinnerPickerOpen] = useState<boolean>(false)
  const [scorerPickerOpen, setScorerPickerOpen] = useState<boolean>(false)
  const [winnerSearch, setWinnerSearch] = useState<string>('')
  const [scorerSearch, setScorerSearch] = useState<string>('')
  const [selectedTournamentCode, setSelectedTournamentCode] = useState<string>('WC2026')
  const [tournamentNotice, setTournamentNotice] = useState<string | null>(null)
  const [predictionsFilter, setPredictionsFilter] = useState<'open' | 'closed'>('open')
  const [profileTargetUserId, setProfileTargetUserId] = useState<number | null>(null)
  const [stageTab, setStageTab] = useState<'1' | '2' | '3' | 'PO' | 'LT'>('1')
  const [playoffTab, setPlayoffTab] = useState<4 | 5 | 6 | 7 | 8 | 9>(4)
  const [matchStageAvailability, setMatchStageAvailability] = useState<Record<number, MatchStageAvailability>>({})
  const [matchStageAvailabilityReady, setMatchStageAvailabilityReady] = useState<boolean>(false)
  const [tableRoundFilter, setTableRoundFilter] = useState<'ALL' | 'LT' | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9>('ALL')
  const [tableSortKey, setTableSortKey] = useState<'total' | 'exact' | 'diff' | 'outcome' | 'missed' | 'bonus'>('total')
  const [tableSortDir, setTableSortDir] = useState<'desc' | 'asc'>('desc')
  const [achievementsExpanded, setAchievementsExpanded] = useState<boolean>(false)
  const [achievementPreview, setAchievementPreview] = useState<AchievementWithVisual | null>(null)
  const [historyExpanded, setHistoryExpanded] = useState<boolean>(false)
  const [notifModalOpen, setNotifModalOpen] = useState<boolean>(false)
  const [notifPrefs, setNotifPrefs] = useState<{ all: boolean; reminders: boolean; duels: boolean; achievements: boolean }>({
    all: true,
    reminders: true,
    duels: true,
    achievements: true,
  })
  const [notifSavingType, setNotifSavingType] = useState<'all' | 'reminders' | 'duels' | 'achievements' | null>(null)
  const [notifError, setNotifError] = useState<string | null>(null)
  const [currentInsight, setCurrentInsight] = useState<string | null>(null)
  const [adminRounds, setAdminRounds] = useState<AdminRound[]>([])
  const [adminRound, setAdminRound] = useState<number | null>(null)
  const [adminViewMode, setAdminViewMode] = useState<'matches' | 'playoff' | 'longterm' | 'participants' | 'duels' | 'rpl_season' | 'rpl_participants' | null>('matches')
  const [adminMode, setAdminMode] = useState<'open' | 'all'>('open')
  const [adminResults, setAdminResults] = useState<AdminResultItem[]>([])
  const [adminRoundName, setAdminRoundName] = useState<string>('')
  const [adminRoundTotal, setAdminRoundTotal] = useState<number>(0)
  const [adminWithoutResult, setAdminWithoutResult] = useState<number>(0)
  const [adminLongtermWinner, setAdminLongtermWinner] = useState<string>('')
  const [adminLongtermScorer, setAdminLongtermScorer] = useState<string>('')
  const [adminLongtermWinnerOptions, setAdminLongtermWinnerOptions] = useState<string[]>([])
  const [adminLongtermScorerOptions, setAdminLongtermScorerOptions] = useState<string[]>([])
  const [adminLongtermParticipants, setAdminLongtermParticipants] = useState<number>(0)
  const [adminLongtermWinnerAwarded, setAdminLongtermWinnerAwarded] = useState<number>(0)
  const [adminLongtermScorerAwarded, setAdminLongtermScorerAwarded] = useState<number>(0)
  const [adminLongtermSaving, setAdminLongtermSaving] = useState<boolean>(false)
  const [adminPlayoffInitLoading, setAdminPlayoffInitLoading] = useState<boolean>(false)
  const [adminPlayoffSavingMatchId, setAdminPlayoffSavingMatchId] = useState<number | null>(null)
  const [adminPlayoffTeamInputs, setAdminPlayoffTeamInputs] = useState<Record<number, { home_team: string; away_team: string }>>({})
  const [adminParticipants, setAdminParticipants] = useState<AdminParticipantItem[]>([])
  const [adminRemovingUserId, setAdminRemovingUserId] = useState<number | null>(null)
  const [adminRplSeason, setAdminRplSeason] = useState<AdminRplSeasonResponse | null>(null)
  const [adminRplParticipants, setAdminRplParticipants] = useState<RplParticipantItem[]>([])
  const [adminRplAssigningId, setAdminRplAssigningId] = useState<number | null>(null)
  const [adminRplSeasonNameInput, setAdminRplSeasonNameInput] = useState<string>('')
  const [adminRplStage1Min, setAdminRplStage1Min] = useState<string>('1')
  const [adminRplStage1Max, setAdminRplStage1Max] = useState<string>('17')
  const [adminRplStage2Min, setAdminRplStage2Min] = useState<string>('18')
  const [adminRplStage2Max, setAdminRplStage2Max] = useState<string>('30')
  const [adminRplConfirmInit, setAdminRplConfirmInit] = useState<boolean>(false)
  const [adminRplEnrollBusy, setAdminRplEnrollBusy] = useState<boolean>(false)
  const [adminRplInitBusy, setAdminRplInitBusy] = useState<boolean>(false)
  const [adminRplShowInit, setAdminRplShowInit] = useState<boolean>(false)
  const [adminDuels, setAdminDuels] = useState<AdminDuelsCurrentResponse | null>(null)
  const [adminDuelsFilter, setAdminDuelsFilter] = useState<'active' | 'finished'>('active')
  const [adminDuelCancelBusyId, setAdminDuelCancelBusyId] = useState<number | null>(null)
  const [adminError, setAdminError] = useState<string | null>(null)
  const [adminNotice, setAdminNotice] = useState<string | null>(null)
  const [adminSavingMatchId, setAdminSavingMatchId] = useState<number | null>(null)
  const [adminScoreInputs, setAdminScoreInputs] = useState<Record<number, string>>({})
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
  const [duelRulesOpen, setDuelRulesOpen] = useState<boolean>(false)
  const [duelLeaderboardOpen, setDuelLeaderboardOpen] = useState<boolean>(false)
  const [joinBusy, setJoinBusy] = useState<boolean>(false)
  const [joinNameInput, setJoinNameInput] = useState<string>('')
  const [joinNameTouched, setJoinNameTouched] = useState<boolean>(false)
  const [refreshTick, setRefreshTick] = useState<number>(0)

  const toggleAdminViewMode = (mode: 'matches' | 'playoff' | 'longterm' | 'participants' | 'duels' | 'rpl_season' | 'rpl_participants') => {
    setAdminViewMode((current) => (current === mode ? null : mode))
  }

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
  const duelErrorMessage = (error: string): string => {
    const key = (error || '').trim()
    if (key === 'duel_already_exists_for_match') {
      return 'На этот матч у тебя или соперника уже есть активная дуэль.'
    }
    if (key === 'same_prediction_not_allowed') {
      return 'Прогнозы в дуэли не должны совпадать.'
    }
    if (key === 'match_locked' || key === 'duel_expired') {
      return 'Матч уже начался, дуэль создать или принять нельзя.'
    }
    if (key === 'not_duel_challenger') {
      return 'Отменить вызов может только тот, кто его бросил.'
    }
    if (key === 'duel_not_pending') {
      return 'Этот вызов уже принят, отклонён или истёк.'
    }
    if (key === 'self_duel_not_allowed') {
      return 'Нельзя бросить вызов самому себе.'
    }
    return 'Не удалось выполнить действие в 1x1. Попробуй ещё раз.'
  }
  const showDebugPanels = import.meta.env.DEV || import.meta.env.VITE_DEBUG_PANELS === '1'

  useEffect(() => {
    if (screen !== 'duels') {
      setDuelMatchPickerOpen(false)
      setDuelOpponentPickerOpen(false)
    }
    if (screen !== 'predict') {
      setWinnerPickerOpen(false)
      setScorerPickerOpen(false)
    }
  }, [screen])

  useEffect(() => {
    if (stageTab !== 'LT') {
      setWinnerPickerOpen(false)
      setScorerPickerOpen(false)
    }
  }, [stageTab])

  useEffect(() => {
    if (joinNameTouched) return
    if (meData?.first_name) {
      setJoinNameInput(meData.first_name)
    }
  }, [meData?.first_name, joinNameTouched])

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
      if (screenParam === 'profile') {
        setScreen('profile')
      } else if (screenParam === 'matches' || screenParam === 'predict') {
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
    const maxAttempts = 16
    let timerId: ReturnType<typeof setTimeout> | null = null
    const appBg = '#0b1220'

    const expandNow = () => {
      try {
        WebApp.expand()
      } catch {
        // no-op outside Telegram
      }
      try {
        ;(WebApp as any).requestFullscreen?.()
      } catch {
        // no-op for clients without fullscreen support
      }
      try {
        ;(WebApp as any).disableVerticalSwipes?.()
      } catch {
        // no-op for clients without this method
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
        timerId = setTimeout(loopExpand, 260)
      }
      timerId = setTimeout(loopExpand, 120)

      const onViewportChanged = () => {
        expandNow()
      }
      const onFocus = () => {
        expandNow()
      }
      const onVisibilityChange = () => {
        if (document.visibilityState === 'visible') {
          expandNow()
        }
      }
      ;(WebApp as any).onEvent?.('viewportChanged', onViewportChanged)
      window.addEventListener('focus', onFocus)
      document.addEventListener('visibilitychange', onVisibilityChange)

      return () => {
        if (timerId) clearTimeout(timerId)
        ;(WebApp as any).offEvent?.('viewportChanged', onViewportChanged)
        window.removeEventListener('focus', onFocus)
        document.removeEventListener('visibilitychange', onVisibilityChange)
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
  }, [selectedTournamentCode, profileTargetUserId, refreshTick])

  useEffect(() => {
    const canManagePrefs = Boolean(profileData?.joined && profileData?.is_self_profile !== false)
    if (!canManagePrefs) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData) return
    const headers = { 'X-Telegram-Init-Data': initData }

    fetch(`${apiBase}/api/miniapp/notifications/current`, { headers })
      .then(async (res) => {
        const data = (await res.json()) as NotificationsPrefsResponse
        if (!res.ok || !data.ok) {
          throw new Error(data.reason || data.error || `HTTP ${res.status}`)
        }
        setNotifPrefs({
          all: Boolean(data.all ?? true),
          reminders: Boolean(data.reminders ?? true),
          duels: Boolean(data.duels ?? true),
          achievements: Boolean(data.achievements ?? true),
        })
        setNotifError(null)
      })
      .catch((err) => {
        setNotifError(String(err))
      })
  }, [profileData?.joined, profileData?.is_self_profile, selectedTournamentCode, refreshTick])

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
    if (screen === 'profile') return
    setNotifModalOpen(false)
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
    if (!notifModalOpen) return
    const onKeyDown = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') {
        setNotifModalOpen(false)
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => {
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [notifModalOpen])

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
  }, [selectedTournamentCode, selectedRoundNumber, tableRoundFilter, refreshTick])

  useEffect(() => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || selectedTournamentCode !== 'WC2026') {
      setMatchStageAvailability({})
      setMatchStageAvailabilityReady(true)
      return
    }
    const headers = {
      'X-Telegram-Init-Data': initData,
    }
    const tParam = encodeURIComponent(selectedTournamentCode)
    let cancelled = false
    setMatchStageAvailabilityReady(false)

    fetch(`${apiBase}/api/miniapp/matches/stages?t=${tParam}`, { headers })
      .then(async (res) => {
        const data = (await res.json()) as MatchStagesResponse
        if (!res.ok || !data.ok) {
          throw new Error(data.reason || data.error || 'match_stages_failed')
        }
        return data
      })
      .then((data) => {
        if (cancelled) return
        const nextAvailability = Object.fromEntries(
          (data.stages || []).map((stage) => [Number(stage.round), stage])
        ) as Record<number, MatchStageAvailability>
        setMatchStageAvailability(nextAvailability)
        setMatchStageAvailabilityReady(true)
      })
      .catch(() => {
        if (cancelled) return
        // Если обзор этапов не загрузился, оставляем старое поведение и показываем все вкладки.
        setMatchStageAvailability({})
        setMatchStageAvailabilityReady(true)
      })

    return () => {
      cancelled = true
    }
  }, [selectedTournamentCode, refreshTick])

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
    } catch (_err) {
      setTournamentNotice('Не удалось переключить турнир. Попробуй ещё раз.')
    }
  }

  const joinSelectedTournament = async () => {
    const joinTournamentCode = selectedTournamentCode || 'WC2026'
    const displayName = joinNameInput.trim().replace(/\s+/g, ' ')
    if (displayName.length < 2 || displayName.length > 24) {
      setTournamentNotice('Введи имя для таблицы — от 2 до 24 символов.')
      return
    }
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    const headers = {
      'Content-Type': 'application/json',
      'X-Telegram-Init-Data': initData,
    }
    setJoinBusy(true)
    setTournamentNotice(null)
    try {
      const res = await fetch(`${apiBase}/api/miniapp/tournament/join`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ tournament_code: joinTournamentCode, display_name: displayName }),
      })
      const data = (await res.json()) as {
        ok?: boolean
        error?: string
        reason?: string
        selected_tournament_code?: string
        selected_tournament_name?: string
      }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      if (data.selected_tournament_code && data.selected_tournament_code !== selectedTournamentCode) {
        setSelectedTournamentCode(data.selected_tournament_code)
      }
      setTournamentNotice(`Ты вступил в ${data.selected_tournament_name || data.selected_tournament_code || joinTournamentCode}.`)
      setRefreshTick((v) => v + 1)
    } catch (_err) {
      setTournamentNotice('Не удалось вступить в турнир. Попробуй ещё раз.')
    } finally {
      setJoinBusy(false)
    }
  }

  const updateNotifPref = async (
    type: 'all' | 'reminders' | 'duels' | 'achievements',
    enabled: boolean
  ) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData) return
    setNotifSavingType(type)
    setNotifError(null)
    try {
      const res = await fetch(`${apiBase}/api/miniapp/notifications/set`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ type, enabled }),
      })
      const data = (await res.json()) as NotificationsPrefsResponse
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setNotifPrefs({
        all: Boolean(data.all ?? true),
        reminders: Boolean(data.reminders ?? true),
        duels: Boolean(data.duels ?? true),
        achievements: Boolean(data.achievements ?? true),
      })
    } catch (err) {
      setNotifError(String(err))
    } finally {
      setNotifSavingType(null)
    }
  }

  const savePredictionRequest = async (
    apiBase: string,
    initData: string,
    tournamentCode: string,
    matchId: number,
    score: string
  ) => {
    const raw = (score || '').trim().replace('-', ':')
    const m = raw.match(/^(\d+):(\d+)$/)
    if (!m) {
      throw new Error('invalid_score')
    }
    const predHome = Number(m[1])
    const predAway = Number(m[2])
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
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
    return data.prediction || `${predHome}:${predAway}`
  }

  const savePrediction = async (matchId: number) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    const score = normalizeScore(scoreInputs[matchId] || '')
    if (!score) {
      setPredictNotice('Счёт введи в формате 2:1 или 2-1.')
      return
    }
    setSavingMatchId(matchId)
    setPredictNotice(null)
    try {
      const saved = await savePredictionRequest(apiBase, initData, selectedTournamentCode, matchId, score)
      setPredictNotice(`Прогноз сохранён: ${saved}`)
      await loadPredictCurrent(apiBase, initData, selectedTournamentCode, selectedRoundNumber)
    } catch (_err) {
      setPredictNotice('Не удалось сохранить прогноз. Попробуй ещё раз.')
    } finally {
      setSavingMatchId(null)
    }
  }

  const saveAllPredictions = async () => {
    const itemsToSave = (predictData?.items || [])
      .filter((item) => !item.is_placeholder && !item.locked)
      .map((item) => {
        const currentInput = normalizeScore(scoreInputs[item.match_id] || '')
        const savedInput = normalizeScore(item.prediction || '')
        return { item, currentInput, savedInput }
      })
      .filter(({ currentInput, savedInput }) => currentInput && currentInput !== savedInput)

    if (itemsToSave.length === 0) {
      setPredictNotice('Нет новых изменений для сохранения.')
      return
    }

    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setSavingAllPredictions(true)
    setSavingMatchId(null)
    setPredictNotice(null)
    try {
      for (const { item, currentInput } of itemsToSave) {
        await savePredictionRequest(apiBase, initData, selectedTournamentCode, item.match_id, currentInput)
      }
      setPredictNotice(`Сохранено прогнозов: ${itemsToSave.length}`)
      await loadPredictCurrent(apiBase, initData, selectedTournamentCode, selectedRoundNumber)
    } catch (_err) {
      setPredictNotice('Не удалось сохранить все прогнозы. Проверь счета и попробуй ещё раз.')
    } finally {
      setSavingAllPredictions(false)
    }
  }

  const openMatchPredictions = async (matchId: number) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData) return
    const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
    setMatchPredictionsLoadingId(matchId)
    setMatchPredictionsError(null)
    try {
      const res = await fetch(
        `${apiBase}/api/miniapp/match/predictions?t=${tParam}&match_id=${encodeURIComponent(String(matchId))}`,
        {
          headers: {
            'X-Telegram-Init-Data': initData,
          },
        }
      )
      const data = (await res.json()) as MatchPredictionsResponse
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setMatchPredictionsSheet(data)
    } catch (_err) {
      setMatchPredictionsError('Не удалось загрузить прогнозы матча. Попробуй ещё раз.')
    } finally {
      setMatchPredictionsLoadingId(null)
    }
  }

  const saveLongtermPickRequest = async (
    apiBase: string,
    initData: string,
    tournamentCode: string,
    pickType: 'winner' | 'scorer',
    pickValue: string
  ) => {
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
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
  }

  const reloadLongtermCurrent = async (apiBase: string, initData: string, tournamentCode: string) => {
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const headers = { 'X-Telegram-Init-Data': initData }
    const reload = await fetch(`${apiBase}/api/miniapp/longterm/current?t=${tParam}`, { headers })
    const reloadData = (await reload.json()) as LongtermResponse
    if (reload.ok && reloadData.ok) {
      setLongtermData(reloadData)
      setWinnerPickInput(reloadData.picks?.winner || '')
      setScorerPickInput(reloadData.picks?.scorer || '')
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
      await saveLongtermPickRequest(apiBase, initData, selectedTournamentCode, pickType, pickValue)
      setLongtermNotice(null)
      await reloadLongtermCurrent(apiBase, initData, selectedTournamentCode)
    } catch (_err) {
      setLongtermNotice('Не удалось сохранить доп. прогноз. Попробуй ещё раз.')
    } finally {
      setSavingLongtermType(null)
    }
  }

  const saveAllLongtermPicks = async () => {
    const itemsToSave: Array<{ pickType: 'winner' | 'scorer'; pickValue: string }> = []
    if (winnerDirty) {
      if (!winnerInputNormalized) {
        setLongtermNotice('Выбери победителя ЧМ перед сохранением.')
        return
      }
      itemsToSave.push({ pickType: 'winner', pickValue: winnerInputNormalized })
    }
    if (scorerDirty) {
      if (!scorerInputNormalized) {
        setLongtermNotice('Выбери бомбардира перед сохранением.')
        return
      }
      itemsToSave.push({ pickType: 'scorer', pickValue: scorerInputNormalized })
    }
    if (itemsToSave.length === 0) return

    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setSavingAllLongterm(true)
    setLongtermNotice(null)
    try {
      for (const item of itemsToSave) {
        await saveLongtermPickRequest(apiBase, initData, selectedTournamentCode, item.pickType, item.pickValue)
      }
      setLongtermNotice(`Сохранено доп. прогнозов: ${itemsToSave.length}`)
      await reloadLongtermCurrent(apiBase, initData, selectedTournamentCode)
    } catch (_err) {
      setLongtermNotice('Не удалось сохранить все доп. прогнозы. Попробуй ещё раз.')
    } finally {
      setSavingAllLongterm(false)
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
      setDuelsNotice(duelErrorMessage(err instanceof Error ? err.message : String(err)))
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
      setDuelsNotice(duelErrorMessage(err instanceof Error ? err.message : String(err)))
    } finally {
      setDuelBusyId(null)
    }
  }

  const cancelDuel = async (duelId: number) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setDuelBusyId(duelId)
    setDuelsNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/duels/cancel?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ duel_id: duelId }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; status?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setDuelsNotice('Вызов отменён.')
      await loadDuelsCurrent(apiBase, initData, selectedTournamentCode)
    } catch (err) {
      setDuelsNotice(duelErrorMessage(err instanceof Error ? err.message : String(err)))
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
    setAdminRound((prev) => {
      if (prev != null && rounds.some((r) => r.round === prev)) return prev
      return data.current_round || rounds[0]?.round || null
    })
  }

  const loadAdminResults = async (
    apiBase: string,
    initData: string,
    tournamentCode: string,
    roundNumber: number | null,
    mode: 'open' | 'all'
  ) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const rParam = roundNumber != null ? `&round=${encodeURIComponent(String(roundNumber))}` : ''
    const res = await fetch(`${apiBase}/api/miniapp/admin/results/current?t=${tParam}${rParam}&mode=${mode}`, { headers })
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
    const nextPlayoffInputs: Record<number, { home_team: string; away_team: string }> = {}
    for (const item of items) {
      nextInputs[item.match_id] = item.result || ''
      if (item.is_placeholder) {
        nextPlayoffInputs[item.match_id] = {
          home_team: item.home_team === '—' ? '' : item.home_team,
          away_team: item.away_team === '—' ? '' : item.away_team,
        }
      }
    }
    setAdminScoreInputs(nextInputs)
    setAdminPlayoffTeamInputs((prev) => ({ ...prev, ...nextPlayoffInputs }))
  }

  const loadAdminLongtermCurrent = async (apiBase: string, initData: string, tournamentCode: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const res = await fetch(`${apiBase}/api/miniapp/admin/longterm/current?t=${tParam}`, { headers })
    const data = (await res.json()) as AdminLongtermCurrentResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setAdminLongtermWinner(data.winner_actual || '')
    setAdminLongtermScorer(data.scorer_actual || '')
    setAdminLongtermWinnerOptions(data.options?.winner || [])
    setAdminLongtermScorerOptions(data.options?.scorer || [])
    setAdminLongtermParticipants(data.participants || 0)
    setAdminLongtermWinnerAwarded(data.winner_awarded || 0)
    setAdminLongtermScorerAwarded(data.scorer_awarded || 0)
  }

  const loadAdminPlayoffSlots = async (apiBase: string, initData: string, tournamentCode: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const res = await fetch(`${apiBase}/api/miniapp/admin/playoff_slots/current?t=${tParam}`, { headers })
    const data = (await res.json()) as AdminPlayoffSlotsCurrentResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    const items = data.items || []
    const nextInputs: Record<number, { home_team: string; away_team: string }> = {}
    for (const item of items) {
      nextInputs[item.match_id] = {
        home_team: item.home_team === '—' ? '' : item.home_team,
        away_team: item.away_team === '—' ? '' : item.away_team,
      }
    }
    setAdminPlayoffTeamInputs(nextInputs)
  }

  const loadAdminParticipants = async (apiBase: string, initData: string, tournamentCode: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const res = await fetch(`${apiBase}/api/miniapp/admin/participants/current?t=${tParam}`, { headers })
    const data = (await res.json()) as AdminParticipantsCurrentResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setAdminParticipants(data.items || [])
  }

  const loadAdminRplSeason = async (apiBase: string, initData: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const res = await fetch(`${apiBase}/api/miniapp/admin/rpl/season`, { headers })
    const data = (await res.json()) as AdminRplSeasonResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setAdminRplSeason(data)
    if (data.season?.name) setAdminRplSeasonNameInput(data.season.name)
    if (data.stages && data.stages.length >= 2) {
      setAdminRplStage1Min(String(data.stages[0].round_min))
      setAdminRplStage1Max(String(data.stages[0].round_max))
      setAdminRplStage2Min(String(data.stages[1].round_min))
      setAdminRplStage2Max(String(data.stages[1].round_max))
    }
  }

  const loadAdminRplParticipants = async (apiBase: string, initData: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const res = await fetch(`${apiBase}/api/miniapp/admin/rpl/participants`, { headers })
    const data = (await res.json()) as AdminRplParticipantsResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setAdminRplParticipants(data.items || [])
  }

  const submitRplSeasonInit = async () => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!adminRplConfirmInit) {
      setAdminNotice('Отметь подтверждение перед созданием сезона.')
      return
    }
    setAdminRplInitBusy(true)
    setAdminNotice(null)
    try {
      const res = await fetch(`${apiBase}/api/miniapp/admin/rpl/season/init`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Telegram-Init-Data': initData },
        body: JSON.stringify({
          season_name: adminRplSeasonNameInput.trim(),
          stage1_min: Number(adminRplStage1Min) || 1,
          stage1_max: Number(adminRplStage1Max) || 17,
          stage2_min: Number(adminRplStage2Min) || 18,
          stage2_max: Number(adminRplStage2Max) || 30,
          confirm: true,
        }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice('Сезон создан.')
      setAdminRplConfirmInit(false)
      await loadAdminRplSeason(apiBase, initData)
    } catch (err) {
      setAdminNotice(`Не удалось создать сезон: ${String(err)}`)
    } finally {
      setAdminRplInitBusy(false)
    }
  }

  const toggleRplEnrollment = async (open: boolean) => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminRplEnrollBusy(true)
    setAdminNotice(null)
    try {
      const res = await fetch(`${apiBase}/api/miniapp/admin/rpl/enroll`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Telegram-Init-Data': initData },
        body: JSON.stringify({ open }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      await loadAdminRplSeason(apiBase, initData)
    } catch (err) {
      setAdminNotice(`Не удалось изменить набор: ${String(err)}`)
    } finally {
      setAdminRplEnrollBusy(false)
    }
  }

  const assignRplParticipant = async (tgUserId: number, leagueCode: 'HIGH' | 'LOW') => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminRplAssigningId(tgUserId)
    setAdminNotice(null)
    try {
      const res = await fetch(`${apiBase}/api/miniapp/admin/rpl/participants/assign`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Telegram-Init-Data': initData },
        body: JSON.stringify({ tg_user_id: tgUserId, league_code: leagueCode }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      await loadAdminRplParticipants(apiBase, initData)
      await loadAdminRplSeason(apiBase, initData)
    } catch (err) {
      setAdminNotice(`Не удалось назначить лигу: ${String(err)}`)
    } finally {
      setAdminRplAssigningId(null)
    }
  }

  const loadAdminDuels = async (apiBase: string, initData: string, tournamentCode: string) => {
    const headers = { 'X-Telegram-Init-Data': initData }
    const tParam = encodeURIComponent(tournamentCode || 'RPL')
    const res = await fetch(`${apiBase}/api/miniapp/admin/duels/current?t=${tParam}`, { headers })
    const data = (await res.json()) as AdminDuelsCurrentResponse
    if (!res.ok || !data.ok) {
      throw new Error(data.reason || data.error || `HTTP ${res.status}`)
    }
    setAdminDuels(data)
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
      await loadAdminResults(apiBase, initData, selectedTournamentCode, null, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка сохранения: ${String(err)}`)
    } finally {
      setAdminSavingMatchId(null)
    }
  }

  const resetAdminResult = async (matchId: number) => {
    const confirmed = window.confirm('Сбросить итог этого матча? Начисленные очки по матчу будут удалены.')
    if (!confirmed) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminSavingMatchId(matchId)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/result/reset?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ match_id: matchId }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; deleted_points?: number }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(`Итог матча сброшен. Удалено очков: ${data.deleted_points ?? 0}`)
      await loadAdminResults(apiBase, initData, selectedTournamentCode, null, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка сброса: ${String(err)}`)
    } finally {
      setAdminSavingMatchId(null)
    }
  }

  const saveAdminLongterm = async () => {
    const winner = (adminLongtermWinner || '').trim()
    const scorer = (adminLongtermScorer || '').trim()
    if (!winner || !scorer) {
      setAdminNotice('Выбери победителя и бомбардира.')
      return
    }
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminLongtermSaving(true)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/longterm/set?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ winner, scorer }),
      })
      const data = (await res.json()) as {
        ok?: boolean
        error?: string
        reason?: string
        changed_participants?: number
        winner_awarded?: number
        scorer_awarded?: number
      }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(
        `Доп. прогнозы обновлены: изменилось ${data.changed_participants ?? 0}, +5 за победителя: ${data.winner_awarded ?? 0}, +5 за бомбардира: ${data.scorer_awarded ?? 0}`
      )
      await loadAdminLongtermCurrent(apiBase, initData, selectedTournamentCode)
    } catch (err) {
      setAdminNotice(`Ошибка сохранения доп. прогнозов: ${String(err)}`)
    } finally {
      setAdminLongtermSaving(false)
    }
  }

  const resetAdminLongterm = async () => {
    const confirmed = window.confirm('Сбросить факт доп. прогнозов? Это очистит бонусные очки у всех участников.')
    if (!confirmed) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminLongtermSaving(true)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/longterm/reset?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; reset_participants?: number }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(`Доп. прогнозы сброшены. Обновлено участников: ${data.reset_participants ?? 0}`)
      await loadAdminLongtermCurrent(apiBase, initData, selectedTournamentCode)
      await loadAdminResults(apiBase, initData, selectedTournamentCode, adminRound || 1, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка сброса доп. прогнозов: ${String(err)}`)
    } finally {
      setAdminLongtermSaving(false)
    }
  }

  const initAdminPlayoffSlots = async () => {
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminPlayoffInitLoading(true)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/playoff_slots/init?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({}),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string; created?: number; skipped?: number }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(`Шаблоны готовы: добавлено ${data.created ?? 0}, уже существовало ${data.skipped ?? 0}`)
      await loadAdminPlayoffSlots(apiBase, initData, selectedTournamentCode)
      await loadAdminResults(apiBase, initData, selectedTournamentCode, null, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка инициализации: ${String(err)}`)
    } finally {
      setAdminPlayoffInitLoading(false)
    }
  }

  const saveAdminPlayoffSlot = async (matchId: number) => {
    const teams = adminPlayoffTeamInputs[matchId] || { home_team: '', away_team: '' }
    const home_team = (teams.home_team || '').trim()
    const away_team = (teams.away_team || '').trim()
    if (!home_team || !away_team) {
      setAdminNotice('Заполни обе команды для пары.')
      return
    }
    if (home_team === away_team) {
      setAdminNotice('В паре должны быть разные команды.')
      return
    }
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminPlayoffSavingMatchId(matchId)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/playoff_slots/fill?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ match_id: matchId, home_team, away_team }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice('Пара сохранена.')
      await loadAdminPlayoffSlots(apiBase, initData, selectedTournamentCode)
      await loadAdminResults(apiBase, initData, selectedTournamentCode, null, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка сохранения пары: ${String(err)}`)
    } finally {
      setAdminPlayoffSavingMatchId(null)
    }
  }

  const clearAdminPlayoffSlot = async (matchId: number) => {
    const confirmed = window.confirm('Очистить слот? Команды, результат, прогнозы и очки по матчу будут удалены.')
    if (!confirmed) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminPlayoffSavingMatchId(matchId)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/playoff_slots/clear?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ match_id: matchId }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice('Слот очищен.')
      await loadAdminPlayoffSlots(apiBase, initData, selectedTournamentCode)
      await loadAdminResults(apiBase, initData, selectedTournamentCode, null, adminMode)
    } catch (err) {
      setAdminNotice(`Ошибка очистки слота: ${String(err)}`)
    } finally {
      setAdminPlayoffSavingMatchId(null)
    }
  }

  const removeAdminParticipant = async (targetTgUserId: number) => {
    const target = adminParticipants.find((u) => Number(u.tg_user_id) === Number(targetTgUserId))
    const label = target?.display_name || `ID ${targetTgUserId}`
    const confirmed = window.confirm(`Удалить ${label} из турнира ${selectedTournamentCode}?`)
    if (!confirmed) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminRemovingUserId(targetTgUserId)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/participant/remove?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ tg_user_id: targetTgUserId }),
      })
      const data = (await res.json()) as {
        ok?: boolean
        error?: string
        reason?: string
        deleted_predictions?: number
        deleted_points?: number
        deleted_duels?: number
      }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice(
        `Участник удалён из турнира. Прогнозы: ${data.deleted_predictions ?? 0}, очки: ${data.deleted_points ?? 0}, дуэли: ${data.deleted_duels ?? 0}`
      )
      await loadAdminParticipants(apiBase, initData, selectedTournamentCode)
    } catch (err) {
      setAdminNotice(`Ошибка удаления участника: ${String(err)}`)
    } finally {
      setAdminRemovingUserId(null)
    }
  }

  const cancelAdminDuel = async (duelId: number) => {
    const confirmed = window.confirm('Отменить активную дуэль 1x1? Elo не изменится, дуэль уйдёт из активных.')
    if (!confirmed) return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    setAdminDuelCancelBusyId(duelId)
    setAdminNotice(null)
    try {
      const tParam = encodeURIComponent(selectedTournamentCode || 'RPL')
      const res = await fetch(`${apiBase}/api/miniapp/admin/duels/cancel?t=${tParam}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Telegram-Init-Data': initData,
        },
        body: JSON.stringify({ duel_id: duelId }),
      })
      const data = (await res.json()) as { ok?: boolean; error?: string; reason?: string }
      if (!res.ok || !data.ok) {
        throw new Error(data.reason || data.error || `HTTP ${res.status}`)
      }
      setAdminNotice('Дуэль отменена.')
      await loadAdminDuels(apiBase, initData, selectedTournamentCode)
    } catch (err) {
      setAdminNotice(`Ошибка отмены дуэли: ${String(err)}`)
    } finally {
      setAdminDuelCancelBusyId(null)
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
    if (!meData?.is_admin) return
    if (adminViewMode !== 'matches') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminResults(apiBase, initData, selectedTournamentCode, null, adminMode)
      .then(() => setAdminError(null))
      .catch((err) => {
        setAdminError(String(err))
        setAdminResults([])
      })
  }, [meData?.is_admin, selectedTournamentCode, adminMode, adminViewMode])

  useEffect(() => {
    if (!meData?.is_admin) return
    if (adminViewMode !== 'longterm') return
    if (selectedTournamentCode !== 'WC2026') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminLongtermCurrent(apiBase, initData, selectedTournamentCode)
      .then(() => setAdminError(null))
      .catch((err) => setAdminError(String(err)))
  }, [meData?.is_admin, selectedTournamentCode, adminViewMode])

  useEffect(() => {
    if (!meData?.is_admin) return
    if (adminViewMode !== 'playoff') return
    if (selectedTournamentCode !== 'WC2026') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminPlayoffSlots(apiBase, initData, selectedTournamentCode)
      .then(() => setAdminError(null))
      .catch((err) => setAdminError(String(err)))
  }, [meData?.is_admin, selectedTournamentCode, adminViewMode])

  useEffect(() => {
    if (!meData?.is_admin) return
    if (adminViewMode !== 'participants') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminParticipants(apiBase, initData, selectedTournamentCode)
      .then(() => setAdminError(null))
      .catch((err) => {
        setAdminError(String(err))
        setAdminParticipants([])
      })
  }, [meData?.is_admin, selectedTournamentCode, adminViewMode])

  useEffect(() => {
    if (!meData?.is_admin) return
    if (adminViewMode !== 'duels') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData || !selectedTournamentCode) return

    loadAdminDuels(apiBase, initData, selectedTournamentCode)
      .then(() => setAdminError(null))
      .catch((err) => {
        setAdminError(String(err))
        setAdminDuels(null)
      })
  }, [meData?.is_admin, selectedTournamentCode, adminViewMode])

  useEffect(() => {
    if (!meData?.is_admin) return
    if (adminViewMode !== 'rpl_season') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData) return
    loadAdminRplSeason(apiBase, initData).catch((err) => setAdminError(String(err)))
  }, [meData?.is_admin, adminViewMode])

  useEffect(() => {
    if (!meData?.is_admin) return
    if (adminViewMode !== 'rpl_participants') return
    const apiBase = import.meta.env.VITE_API_BASE || 'http://localhost:8081'
    const initData = getInitData()
    if (!initData) return
    loadAdminRplParticipants(apiBase, initData).catch((err) => setAdminError(String(err)))
  }, [meData?.is_admin, adminViewMode])

  useEffect(() => {
    if (selectedTournamentCode !== 'WC2026' && (adminViewMode === 'longterm' || adminViewMode === 'playoff')) {
      setAdminViewMode('matches')
    }
    if (selectedTournamentCode !== 'RPL' && (adminViewMode === 'rpl_season' || adminViewMode === 'rpl_participants')) {
      setAdminViewMode('matches')
    }
  }, [selectedTournamentCode, adminViewMode])

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
  const dirtyLongtermPicksCount = (winnerDirty ? 1 : 0) + (scorerDirty ? 1 : 0)

  const duelMatchOptions = duelsData?.match_options || []
  const duelOpponents = duelsData?.opponents || []
  const duelLeaderboard = duelsData?.leaderboard || []
  const duelBusyOpponentsByMatch = duelsData?.busy_opponents_by_match || {}
  const duelSelectedMatch = duelMatchOptions.find((m) => Number(m.match_id) === Number(duelMatchId)) || null
  const duelSelectedOpponent =
    duelOpponents.find((u) => Number(u.tg_user_id) === Number(duelOpponentId)) || null
  const duelBusyOpponentIdsForMatch = new Set(
    (duelBusyOpponentsByMatch[String(duelMatchId)] || []).map((id) => Number(id))
  )
  const isDuelOpponentBusyForMatch = (tgUserIdValue: number) =>
    duelMatchId > 0 && duelBusyOpponentIdsForMatch.has(Number(tgUserIdValue))
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
    cancelled: 'Отменена',
  }

  const wcPlayoffTabs: Array<{ key: 4 | 5 | 6 | 7 | 8 | 9; label: string }> = [
    { key: 4, label: '1/16' },
    { key: 5, label: '1/8' },
    { key: 6, label: '1/4' },
    { key: 7, label: '1/2' },
    { key: 8, label: 'За 3-е' },
    { key: 9, label: 'Финал' },
  ]
  const wcStageTabsUnified: WcStageTab[] = [
    { type: 'stage', key: '1', label: 'Тур 1' },
    { type: 'stage', key: '2', label: 'Тур 2' },
    { type: 'stage', key: '3', label: 'Тур 3' },
    ...wcPlayoffTabs.map((tab) => ({ type: 'playoff' as const, key: tab.key, label: tab.label })),
    { type: 'stage', key: 'LT', label: 'Доп. прогнозы' },
  ]
  const selectedMatchStageRound = stageTab === 'PO' ? playoffTab : stageTab === 'LT' ? null : Number(stageTab)
  const getStageRound = (tab: WcStageTab) =>
    tab.type === 'playoff' ? Number(tab.key) : tab.key === 'LT' ? null : Number(tab.key)
  const hasStageAvailability = Object.keys(matchStageAvailability).length > 0
  const shouldShowMatchStageTab = (tab: WcStageTab) => {
    const round = getStageRound(tab)
    if (round == null) return true
    if (!hasStageAvailability) return true
    const info = matchStageAvailability[round]
    if (predictionsFilter === 'closed') return Boolean(info && info.closed > 0)
    return !info?.completed
  }
  const selectMatchStageRound = (round: number) => {
    if (round >= 4) {
      setStageTab('PO')
      setPlayoffTab(round as 4 | 5 | 6 | 7 | 8 | 9)
      return
    }
    setStageTab(String(round) as '1' | '2' | '3')
  }
  const latestClosedStageRound = hasStageAvailability
    ? MATCH_STAGE_ROUND_NUMBERS
        .filter((round) => (matchStageAvailability[round]?.closed || 0) > 0)
        .sort((a, b) => {
          const byKickoff =
            kickoffSortValue(matchStageAvailability[b]?.latest_closed_kickoff) -
            kickoffSortValue(matchStageAvailability[a]?.latest_closed_kickoff)
          return byKickoff !== 0 ? byKickoff : b - a
        })[0]
    : undefined
  const visibleWcStageTabs = wcStageTabsUnified.filter(shouldShowMatchStageTab)
  const groupStageTabs = visibleWcStageTabs.filter((tab) => tab.type === 'stage' && tab.key !== 'LT')
  const knockoutStageTabs = visibleWcStageTabs.filter((tab) => tab.type === 'playoff' || tab.key === 'LT')
  const matchStagesLoading = selectedTournamentCode === 'WC2026' && !matchStageAvailabilityReady
  const allowLongtermTab = showWcSelector
  const winnerSearchNorm = winnerSearch.trim().toLowerCase()
  const scorerSearchNorm = scorerSearch.trim().toLowerCase()
  const winnerOptions = longtermData?.options?.winner || []
  const scorerOptions = longtermData?.options?.scorer || []
  const winnerFilteredOptions = winnerOptions.filter((name) => name.toLowerCase().includes(winnerSearchNorm))
  const scorerFilteredOptions = scorerOptions.filter((name) => name.toLowerCase().includes(scorerSearchNorm))

  const predictItems = predictData?.items || []
  const dirtyPredictItemsCount = predictItems.filter((item) => {
    if (item.is_placeholder || item.locked) return false
    const currentInput = normalizeScore(scoreInputs[item.match_id] || '')
    const savedInput = normalizeScore(item.prediction || '')
    return Boolean(currentInput && currentInput !== savedInput)
  }).length
  const predictGroups = (() => {
    const grouped: Record<string, typeof predictItems> = {}
    for (const item of predictItems) {
      const dateKey = (item.kickoff || '').split(' ')[0] || '—'
      if (!grouped[dateKey]) grouped[dateKey] = []
      grouped[dateKey].push(item)
    }
    return Object.entries(grouped)
  })()
  const closedPredictionItems = (predictionsData?.items || [])
    .filter((m) => m.status === 'closed')
    .sort((a, b) => {
      const byKickoff = kickoffSortValue(b.kickoff) - kickoffSortValue(a.kickoff)
      if (byKickoff !== 0) return byKickoff
      return Number(b.match_id || 0) - Number(a.match_id || 0)
    })
  const openRealPredictionItems = predictItems.filter((m) => !m.is_placeholder)
  const openPredictionCount = openRealPredictionItems.length
  const closedPredictionCount = closedPredictionItems.length
  const placedPredictionCount =
    openRealPredictionItems.filter((m) => Boolean(normalizeScore(m.prediction || ''))).length +
    closedPredictionItems.filter((m) => Boolean(normalizeScore(m.prediction || ''))).length
  const overviewMatchesTotal = openPredictionCount + closedPredictionCount
  const overviewProgressPct = overviewMatchesTotal > 0 ? Math.round((placedPredictionCount / overviewMatchesTotal) * 100) : 0
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
    if (selectedTournamentCode !== 'WC2026' || !matchStageAvailabilityReady || !hasStageAvailability || stageTab === 'LT') return
    const currentRound = selectedMatchStageRound
    if (currentRound != null) {
      const currentInfo = matchStageAvailability[currentRound]
      const currentIsAvailable =
        predictionsFilter === 'closed'
          ? Boolean(currentInfo && currentInfo.closed > 0)
          : !currentInfo?.completed
      if (currentIsAvailable) return
    }

    const nextRound =
      predictionsFilter === 'closed'
        ? MATCH_STAGE_ROUND_NUMBERS
            .filter((round) => (matchStageAvailability[round]?.closed || 0) > 0)
            .sort((a, b) => {
              const byKickoff =
                kickoffSortValue(matchStageAvailability[b]?.latest_closed_kickoff) -
                kickoffSortValue(matchStageAvailability[a]?.latest_closed_kickoff)
              return byKickoff !== 0 ? byKickoff : b - a
            })[0]
        : MATCH_STAGE_ROUND_NUMBERS.find((round) => !matchStageAvailability[round]?.completed)

    if (!nextRound) return
    selectMatchStageRound(nextRound)
  }, [
    hasStageAvailability,
    matchStageAvailability,
    matchStageAvailabilityReady,
    predictionsFilter,
    selectedMatchStageRound,
    selectedTournamentCode,
    stageTab,
  ])

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
    if (tableData?.table_mode === 'longterm') {
      return []
    }
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

  const tableLongtermRows = useMemo<TableLongtermRow[]>(() => {
    return [...(tableData?.rows_longterm || [])]
  }, [tableData])

  const adminResultsWithSections = useMemo(() => {
    return adminResults.map((item, index) => {
      const sectionTitle = item.round_name || (item.round_number ? `Тур ${item.round_number}` : 'Матчи')
      const previous = adminResults[index - 1]
      const previousTitle = previous
        ? previous.round_name || (previous.round_number ? `Тур ${previous.round_number}` : 'Матчи')
        : ''
      return {
        item,
        sectionTitle,
        showSection: sectionTitle !== previousTitle,
      }
    })
  }, [adminResults])

  const handleSortHeader = (key: 'total' | 'exact' | 'diff' | 'outcome' | 'missed' | 'bonus') => {
    if (tableSortKey === key) {
      setTableSortDir((prev) => (prev === 'desc' ? 'asc' : 'desc'))
      return
    }
    setTableSortKey(key)
    setTableSortDir(key === 'missed' ? 'asc' : 'desc')
  }

  const renderAdminMatchesContent = () => (
    <div className="admin-inline-panel">
      <div className="segment-hint">Все матчи турнира одним списком, от ранних к поздним</div>

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

        {selectedTournamentCode === 'WC2026' ? (
          <button
            className="admin-secondary-btn"
            onClick={initAdminPlayoffSlots}
            disabled={adminPlayoffInitLoading}
          >
            {adminPlayoffInitLoading ? 'Создаю...' : 'Создать слоты плей-офф'}
          </button>
        ) : null}
      </div>
      <div className="card-text">
        {adminRoundTotal > 0 ? (
          <>
            Матчей: <b>{adminRoundTotal}</b> · без итогов: <b>{adminWithoutResult}</b>
          </>
        ) : (
          'Матчей для управления пока нет.'
        )}
      </div>

      <div className="compact-list-card admin-inline-list admin-matches-list">
        {adminResults.length === 0 ? (
          <div className="card-text">Матчей для показа нет.</div>
        ) : (
          adminResultsWithSections.map(({ item: m, sectionTitle, showSection }) => (
            <div className="admin-match-section-row" key={m.match_id}>
              {showSection ? <div className="admin-match-section-title">{sectionTitle}</div> : null}
              <div className="compact-match">
                <div className="compact-meta">
                  {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                  <span className="kickoff-small">{m.kickoff || '—'}</span>
                </div>
                {m.is_placeholder ? (
                  <>
                    <div className="admin-playoff-inline">
                      <select
                        className="admin-team-select"
                        value={adminPlayoffTeamInputs[m.match_id]?.home_team || ''}
                        onChange={(e) =>
                          setAdminPlayoffTeamInputs((prev) => ({
                            ...prev,
                            [m.match_id]: {
                              ...(prev[m.match_id] || { home_team: '', away_team: '' }),
                              home_team: e.target.value,
                            },
                          }))
                        }
                      >
                        <option value="">Команда A</option>
                        {teamOptionsWithFlags.map((team) => (
                          <option value={team} key={`home-${m.match_id}-${team}`}>
                            {teamWithFlag(team)}
                          </option>
                        ))}
                      </select>
                      <select
                        className="admin-team-select"
                        value={adminPlayoffTeamInputs[m.match_id]?.away_team || ''}
                        onChange={(e) =>
                          setAdminPlayoffTeamInputs((prev) => ({
                            ...prev,
                            [m.match_id]: {
                              ...(prev[m.match_id] || { home_team: '', away_team: '' }),
                              away_team: e.target.value,
                            },
                          }))
                        }
                      >
                        <option value="">Команда B</option>
                        {teamOptionsWithFlags.map((team) => (
                          <option value={team} key={`away-${m.match_id}-${team}`}>
                            {teamWithFlag(team)}
                          </option>
                        ))}
                      </select>
                      <button
                        className="save-btn admin-playoff-save-inline is-dirty"
                        onClick={() => saveAdminPlayoffSlot(m.match_id)}
                        disabled={adminPlayoffSavingMatchId === m.match_id}
                      >
                        {adminPlayoffSavingMatchId === m.match_id ? '...' : 'Сохранить пару'}
                      </button>
                    </div>
                    <div className="compact-note">
                      Пара ещё не заполнена. После сохранения матч появится пользователям.
                    </div>
                  </>
                ) : (
                  <>
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
                      <button
                        className="admin-reset-btn"
                        onClick={() => resetAdminResult(m.match_id)}
                        disabled={adminSavingMatchId === m.match_id || !m.result}
                      >
                        Сброс
                      </button>
                    </div>
                    <div className="compact-note">
                      Итог: <b>{m.result || 'не задан'}</b> · Прогнозов: <b>{m.predictions_count ?? 0}</b>
                      {selectedTournamentCode === 'WC2026' && Number(m.round_number || 0) >= 4 ? (
                        <>
                          {' · '}
                          <button
                            className="admin-note-action"
                            onClick={() => clearAdminPlayoffSlot(m.match_id)}
                            disabled={adminPlayoffSavingMatchId === m.match_id}
                          >
                            очистить пару
                          </button>
                        </>
                      ) : null}
                    </div>
                  </>
                )}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )

  const renderAdminLongtermContent = () => (
    <div className="admin-inline-panel">
      <div className="admin-longterm-summary">
        <div>
          <span>Участники</span>
          <b>{adminLongtermParticipants}</b>
        </div>
        <div>
          <span>Очки</span>
          <b>{adminLongtermWinnerAwarded + adminLongtermScorerAwarded}</b>
        </div>
        <div>
          <span>Факт</span>
          <b>{adminLongtermWinner && adminLongtermScorer ? 'задан' : 'не задан'}</b>
        </div>
      </div>
      <div className="card-text admin-longterm-note">Выбери фактические итоги и пересчитай бонусы +5.</div>
      <div className="admin-longterm-grid">
        <label className="admin-longterm-label">
          <span>Победитель турнира</span>
          <select
            className="duel-picker-search admin-longterm-select"
            value={adminLongtermWinner}
            onChange={(e) => setAdminLongtermWinner(e.target.value)}
          >
            <option value="">Выбрать команду</option>
            {adminLongtermWinnerOptions.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </label>
        <label className="admin-longterm-label">
          <span>Лучший бомбардир</span>
          <select
            className="duel-picker-search admin-longterm-select"
            value={adminLongtermScorer}
            onChange={(e) => setAdminLongtermScorer(e.target.value)}
          >
            <option value="">Выбрать игрока</option>
            {adminLongtermScorerOptions.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="admin-longterm-actions">
        <button
          className="admin-recalc-btn"
          onClick={saveAdminLongterm}
          disabled={adminLongtermSaving || !adminLongtermWinner || !adminLongtermScorer}
        >
          {adminLongtermSaving ? 'Сохраняю…' : 'Сохранить'}
        </button>
        <button
          className="admin-reset-btn"
          onClick={resetAdminLongterm}
          disabled={adminLongtermSaving}
        >
          Сбросить
        </button>
      </div>
    </div>
  )

  const renderAdminParticipantsContent = () => (
    <div className="admin-inline-panel">
      <div className="admin-participants-summary">
        <div>
          <span>Участники</span>
          <b>{adminParticipants.length}</b>
        </div>
        <div>
          <span>Бонусы</span>
          <b>{adminParticipants.reduce((sum, u) => sum + (u.bonus_points || 0), 0)}</b>
        </div>
        <div>
          <span>Действие</span>
          <b>удаление</b>
        </div>
      </div>
      <div className="compact-list-card admin-inline-list">
        {adminParticipants.length === 0 ? (
          <div className="card-text admin-empty-text">Участников в выбранном турнире пока нет.</div>
        ) : (
          adminParticipants.map((u) => (
            <div className="compact-match admin-participant-row" key={`admin-p-${u.tg_user_id}`}>
              <div className="admin-participant-main">
                <div className="admin-participant-name">
                  <span className="team-name team-left">{u.display_name}</span>
                  <span className="group-small">ID {u.tg_user_id}</span>
                </div>
                <div className="admin-participant-bonus">
                  <span>Бонус</span>
                  <b>{u.bonus_points || 0}</b>
                </div>
                <button
                  className="admin-reset-btn"
                  onClick={() => removeAdminParticipant(u.tg_user_id)}
                  disabled={adminRemovingUserId === u.tg_user_id}
                >
                  {adminRemovingUserId === u.tg_user_id ? '…' : 'Удалить'}
                </button>
              </div>
              <div className="compact-note">
                Вступил: <b>{u.joined_at || '—'}</b>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )

  const renderAdminRplSeasonContent = () => {
    const season = adminRplSeason?.season
    const stages = adminRplSeason?.stages || []
    const counts = adminRplSeason?.counts
    const enrollOpen = !!adminRplSeason?.enrollment_open
    const showInitForm = adminRplShowInit || !season

    return (
      <div className="admin-inline-panel">
        {season ? (
          <div className="admin-status-block">
            <div className="admin-status-row">
              <span>Сезон</span>
              <b>{season.name}</b>
            </div>
            <div className="admin-status-row">
              <span>Набор участников</span>
              <b className={`status-pill ${enrollOpen ? 'status-pill-open' : 'status-pill-closed'}`}>
                {enrollOpen ? 'Открыт' : 'Закрыт'}
              </b>
            </div>
            <div className="admin-status-row">
              <span>Вступило участников</span>
              <b>{counts?.total_members ?? 0}</b>
            </div>

            <div className="admin-status-divider" />

            {stages.map((s) => (
              <div className="admin-stage-row" key={`stage-${s.id}`}>
                <span className={`status-pill ${s.is_active ? 'status-pill-open' : 'status-pill-muted'}`}>
                  {s.is_active ? 'Сейчас' : 'Позже'}
                </span>
                <div className="admin-stage-info">
                  <b>{s.name}</b>
                  <small>Туры {s.round_min}–{s.round_max}</small>
                </div>
              </div>
            ))}

            <div className="admin-status-divider" />

            <div className="admin-status-row">
              <span>Высшая лига</span>
              <b>{counts?.HIGH ?? 0}</b>
            </div>
            <div className="admin-status-row">
              <span>Низшая лига</span>
              <b>{counts?.LOW ?? 0}</b>
            </div>
            <div className="admin-status-row">
              <span>Не распределены</span>
              <b>{counts?.unassigned ?? 0}</b>
            </div>

            <button
              type="button"
              className="admin-reset-btn admin-reset-btn-wide"
              onClick={() => toggleRplEnrollment(!enrollOpen)}
              disabled={adminRplEnrollBusy}
            >
              {adminRplEnrollBusy ? '…' : enrollOpen ? 'Закрыть набор' : 'Открыть набор'}
            </button>
          </div>
        ) : (
          <div className="card-text admin-empty-text">Сезон РПЛ ещё не создан.</div>
        )}

        {season ? (
          <button
            type="button"
            className="admin-danger-toggle"
            onClick={() => setAdminRplShowInit((v) => !v)}
          >
            {adminRplShowInit ? '▾ Скрыть пересоздание сезона' : '▸ Пересоздать сезон'}
          </button>
        ) : null}

        {showInitForm ? (
          <div className="admin-danger-zone">
            <div className="card-title">{season ? '⚠️ Пересоздать сезон' : 'Создать сезон'}</div>
            <div className="segment-hint">
              {season
                ? 'Это сотрёт текущие лиги, этапы и распределение участников по ним. Отменить нельзя.'
                : 'Задай при необходимости диапазоны туров — по умолчанию 1–17 (осень) и 18–30 (весна).'}
            </div>
            <input
              className="admin-text-input"
              placeholder="Название сезона, напр. РПЛ 2026/27"
              value={adminRplSeasonNameInput}
              onChange={(e) => setAdminRplSeasonNameInput(e.target.value)}
            />
            <div className="admin-range-row">
              <span>Осенний этап, туры</span>
              <input
                className="admin-number-input"
                value={adminRplStage1Min}
                onChange={(e) => setAdminRplStage1Min(e.target.value)}
              />
              <span>–</span>
              <input
                className="admin-number-input"
                value={adminRplStage1Max}
                onChange={(e) => setAdminRplStage1Max(e.target.value)}
              />
            </div>
            <div className="admin-range-row">
              <span>Весенний этап, туры</span>
              <input
                className="admin-number-input"
                value={adminRplStage2Min}
                onChange={(e) => setAdminRplStage2Min(e.target.value)}
              />
              <span>–</span>
              <input
                className="admin-number-input"
                value={adminRplStage2Max}
                onChange={(e) => setAdminRplStage2Max(e.target.value)}
              />
            </div>
            <label className="admin-confirm-row">
              <input
                type="checkbox"
                checked={adminRplConfirmInit}
                onChange={(e) => setAdminRplConfirmInit(e.target.checked)}
              />
              <span>Понимаю, что это сотрёт текущие лиги/этапы</span>
            </label>
            <button
              type="button"
              className="admin-reset-btn admin-reset-btn-wide"
              onClick={submitRplSeasonInit}
              disabled={adminRplInitBusy || !adminRplConfirmInit}
            >
              {adminRplInitBusy ? '…' : season ? 'Пересоздать сезон' : 'Создать сезон'}
            </button>
          </div>
        ) : null}
      </div>
    )
  }

  const renderAdminRplParticipantsContent = () => {
    const unassigned = adminRplParticipants.filter((p) => !p.league_code)
    const high = adminRplParticipants.filter((p) => p.league_code === 'HIGH')
    const low = adminRplParticipants.filter((p) => p.league_code === 'LOW')
    const renderRow = (p: RplParticipantItem) => (
      <div className="compact-match admin-participant-row" key={`rpl-p-${p.tg_user_id}`}>
        <div className="admin-participant-main">
          <div className="admin-participant-name">
            <span className="team-name team-left">{p.display_name}</span>
            <span className="group-small">
              ID {p.tg_user_id}
              {p.league_code ? ` · ${p.league_code === 'HIGH' ? 'Высшая' : 'Низшая'}` : ''}
            </span>
          </div>
          <button
            className="admin-reset-btn"
            onClick={() => assignRplParticipant(p.tg_user_id, 'HIGH')}
            disabled={adminRplAssigningId === p.tg_user_id || p.league_code === 'HIGH'}
          >
            {adminRplAssigningId === p.tg_user_id ? '…' : '→ Высшая'}
          </button>
          <button
            className="admin-reset-btn"
            onClick={() => assignRplParticipant(p.tg_user_id, 'LOW')}
            disabled={adminRplAssigningId === p.tg_user_id || p.league_code === 'LOW'}
          >
            {adminRplAssigningId === p.tg_user_id ? '…' : '→ Низшая'}
          </button>
        </div>
      </div>
    )
    return (
      <div className="admin-inline-panel">
        <div className="admin-participants-summary">
          <div>
            <span>Не распределены</span>
            <b>{unassigned.length}</b>
          </div>
          <div>
            <span>Высшая</span>
            <b>{high.length}</b>
          </div>
          <div>
            <span>Низшая</span>
            <b>{low.length}</b>
          </div>
        </div>
        <div className="compact-list-card admin-inline-list">
          {adminRplParticipants.length === 0 ? (
            <div className="card-text admin-empty-text">Пока никто не вступил в РПЛ.</div>
          ) : (
            <>
              {unassigned.map(renderRow)}
              {high.map(renderRow)}
              {low.map(renderRow)}
            </>
          )}
        </div>
      </div>
    )
  }

  const renderAdminDuelsContent = () => {
    const visibleAdminDuels = adminDuelsFilter === 'active' ? adminDuels?.active || [] : adminDuels?.finished || []
    return (
      <div className="admin-inline-panel">
        <div className="admin-duels-summary">
          <div>
            <span>Активные</span>
            <b>{adminDuels?.active?.length || 0}</b>
          </div>
          <div>
            <span>Завершённые</span>
            <b>{adminDuels?.finished?.length || 0}</b>
          </div>
          <div>
            <span>Всего</span>
            <b>{(adminDuels?.active?.length || 0) + (adminDuels?.finished?.length || 0)}</b>
          </div>
        </div>
        <div className="admin-duels-tabs">
          <button
            type="button"
            className={adminDuelsFilter === 'active' ? 'is-active' : ''}
            onClick={() => setAdminDuelsFilter('active')}
          >
            Активные
          </button>
          <button
            type="button"
            className={adminDuelsFilter === 'finished' ? 'is-active' : ''}
            onClick={() => setAdminDuelsFilter('finished')}
          >
            Завершённые
          </button>
        </div>
        <div className="admin-duels-list">
          {!adminDuels ? (
            <div className="card-text admin-empty-text">Загружаю дуэли…</div>
          ) : visibleAdminDuels.length ? (
            visibleAdminDuels.map((d) => {
              const isFinished = adminDuelsFilter === 'finished'
            const winnerName =
              d.outcome === 'draw'
                ? 'Ничья'
                : d.winner_tg_user_id === d.challenger_tg_user_id
                  ? d.challenger_name
                  : d.winner_tg_user_id === d.opponent_tg_user_id
                    ? d.opponent_name
                    : '—'
            return (
              <div className="admin-duel-card" key={`admin-duel-${d.duel_id}`}>
                <div className="admin-duel-card-top">
                  <div>
                    <div className="admin-duel-meta">
                      <span>{d.group_label ? `[${d.group_label}]` : '—'}</span>
                      <span>{d.kickoff} МСК</span>
                    </div>
                    <div className="admin-duel-match-line">
                      {teamWithFlag(d.home_team)} — {teamWithFlag(d.away_team)}
                    </div>
                  </div>
                  <span className={`admin-duel-state ${isFinished ? 'is-finished' : 'is-active'}`}>
                    {isFinished ? d.result || '-:-' : duelStatusText[d.status] || 'Активна'}
                  </span>
                </div>
                <div className="admin-duel-players">
                  <div className="admin-duel-player-line">
                    <span className="admin-duel-player-rating">{d.challenger_rating || 1000}</span>
                    <span className="admin-duel-player-name">{d.challenger_name}</span>
                    <b>{d.challenger_pred}</b>
                    {isFinished ? (
                        <span className={d.elo_delta_challenger >= 0 ? 'duel-delta-plus' : 'duel-delta-minus'}>
                          ({d.elo_delta_challenger >= 0 ? `+${d.elo_delta_challenger}` : d.elo_delta_challenger})
                        </span>
                    ) : (
                      <span />
                    )}
                  </div>
                  <div className="admin-duel-player-line">
                    <span className="admin-duel-player-rating">{d.opponent_rating || 1000}</span>
                    <span className="admin-duel-player-name">{d.opponent_name}</span>
                    <b>{d.opponent_pred || '—'}</b>
                    {isFinished ? (
                        <span className={d.elo_delta_opponent >= 0 ? 'duel-delta-plus' : 'duel-delta-minus'}>
                          ({d.elo_delta_opponent >= 0 ? `+${d.elo_delta_opponent}` : d.elo_delta_opponent})
                        </span>
                    ) : (
                      <span />
                    )}
                  </div>
                </div>
                <div className="admin-duel-footer">
                  {isFinished ? (
                    <span>
                      Итог: <b>{winnerName}</b>
                    </span>
                  ) : (
                    <>
                      <span>Принята · ждёт результат матча</span>
                      <button
                        type="button"
                        className="admin-duel-cancel-btn"
                        onClick={() => cancelAdminDuel(d.duel_id)}
                        disabled={adminDuelCancelBusyId === d.duel_id}
                      >
                        {adminDuelCancelBusyId === d.duel_id ? 'Отменяю…' : 'Отменить'}
                      </button>
                    </>
                  )}
                </div>
              </div>
            )
            })
          ) : (
            <div className="card-text admin-empty-text">
              {adminDuelsFilter === 'active' ? 'Активных дуэлей пока нет.' : 'Завершённых дуэлей пока нет.'}
            </div>
          )}
        </div>
      </div>
    )
  }

  const allAchievements = profileData?.achievements || []
  const achievementsWithVisuals: AchievementWithVisual[] = allAchievements.map((a) => ({
    ...a,
    visual: buildAchievementVisual(a),
  }))
  const earnedAchievements = achievementsWithVisuals.filter((a) => a.earned)
  const lockedAchievements = achievementsWithVisuals.filter((a) => !a.earned)
  const hasHiddenAchievements = earnedAchievements.length > 3 || lockedAchievements.length > 0
  const visibleAchievements = achievementsExpanded
    ? [...earnedAchievements, ...lockedAchievements]
    : (earnedAchievements.length > 0 ? earnedAchievements.slice(0, 3) : achievementsWithVisuals.slice(0, 3))
  const achievementPreviewVisual = achievementPreview?.visual || null
  const achievementPreviewIsUnique = achievementPreview ? isUniqueAchievement(achievementPreview.key) : false
  const achievementPreviewIsLockedHint =
    Boolean(achievementPreview && !achievementPreview.earned && !achievementPreview.visual.isSecretLocked)
  const achievementPreviewLockedHintText = achievementPreview
    ? getLockedAchievementHintText(achievementPreview)
    : ''
  const achievementPreviewGroupBase = achievementPreview ? getAchievementLevelGroupBase(achievementPreview.key) : null
  const achievementProgressByBase: Record<string, number> = {
    no_miss_tour_streak: intOrZero(profileData?.achievement_progress?.no_miss_tour_streak),
    scoring_match_streak: intOrZero(profileData?.achievement_progress?.scoring_match_streak),
    duel_wins_total: intOrZero(profileData?.achievement_progress?.duel_wins_total),
  }
  const achievementPreviewGroupCurrent =
    achievementPreviewGroupBase != null ? intOrZero(achievementProgressByBase[achievementPreviewGroupBase]) : 0
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
  const notificationsSummary = notifPrefs.all ? 'Включены' : 'Отключены'
  const medalGold =
    legacyTrophies.filter((h) => h.place === 1).length +
    tournamentHistory.filter((h) => h.place === 1).length
  const medalSilver =
    legacyTrophies.filter((h) => h.place === 2).length +
    tournamentHistory.filter((h) => h.place === 2).length
  const medalBronze =
    legacyTrophies.filter((h) => h.place === 3).length +
    tournamentHistory.filter((h) => h.place === 3).length
  const rplComingSoonEnabled = false
  const showRplComingSoon = rplComingSoonEnabled && screen !== 'admin' && selectedTournamentCode === 'RPL'
  const showJoinOnboarding =
    !showRplComingSoon && screen !== 'admin' && !profileTargetUserId && Boolean(profileData && profileData.joined === false)

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
                  onClick={() => {
                    setPredictionsFilter('closed')
                    if (latestClosedStageRound) {
                      selectMatchStageRound(latestClosedStageRound)
                    }
                  }}
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
        {showRplComingSoon ? (
          <section className="cards">
            <div className="card join-onboarding-card">
              <div className="card-title">РПЛ скоро стартует</div>
              <div className="card-text">
                Привет, дорогой друг. Мы еще готовим РПЛ к старту.
                <br />
                Новый сезон РПЛ 2026/27 начинается 24 июля 2026.
                <br />
                <br />
                Как обычно, играем в 2 этапа:
                <br />
                осенний — 1–17 туры (24 июля - 6 декабря),
                <br />
                весенний — 18–30 туры (февраль - 29 мая).
                <br />
                <br />
                И так же, как обычно, у нас будет две лиги - ВЫСШАЯ и НИЗШАЯ.
                <br />
                До запуска РПЛ активен турнир WC.
              </div>
              <button
                className="save-btn join-onboarding-btn is-dirty"
                onClick={() => selectTournament('WC2026')}
              >
                Перейти в WC
              </button>
            </div>
          </section>
        ) : null}

        {showJoinOnboarding ? (
          <section className="cards">
            <div className="card join-onboarding-card">
              <div className="card-title">Привет, дорогой друг</div>
              <div className="card-text">
                {selectedTournamentCode === 'RPL' ? (
                  <>
                    Это турнир прогнозов Российской Премьер-Лиги. Осенний этап (1–17 тур) и весенний (18–30 тур), Высшая и Низшая лиги с переходами между ними.
                    <br />
                    <br />
                    Результаты матчей подтягиваются автоматически. Вступай в турнир и знакомься со всеми разделами. Удачи!
                  </>
                ) : (
                  <>
                    Это турнир прогнозов Чемпионата мира по футболу. Тут всё как ты любишь: чтение открытых книг, туры на 0 очков и эмоциональные качели.
                    <br />
                    Поменялось лишь одно - место. Теперь мини-приложение в Telegram сделает твою (а особенно Ромину) жизнь проще и удобнее.
                    <br />
                    <br />
                    Тебя ждут быстрые автоматические расчёты матчей, подробная статистика, ачивки и битвы 1x1. Вступай в турнир и знакомься со всеми разделами. Удачи!
                  </>
                )}
              </div>
              <label className="join-name-label">
                <span>Имя для таблицы (2–24 символа)</span>
                <input
                  className="admin-text-input join-name-input"
                  value={joinNameInput}
                  onChange={(e) => {
                    setJoinNameTouched(true)
                    setJoinNameInput(e.target.value)
                  }}
                  placeholder="Например, Роман"
                  maxLength={24}
                />
              </label>
              <button
                className="save-btn join-onboarding-btn is-dirty"
                onClick={joinSelectedTournament}
                disabled={joinBusy || joinNameInput.trim().length < 2}
              >
                {joinBusy ? 'Вступаю…' : `Вступить в турнир ${selectedTournamentCode === 'RPL' ? 'РПЛ' : 'WC'}`}
              </button>
            </div>
          </section>
        ) : null}

        {!showRplComingSoon && !showJoinOnboarding && screen === 'predict' ? (
          <>
            {showWcSelector ? (
              <section className="cards">
                <div className="card card-static segment-card">
                  <div className="card-title">Этап турнира</div>
                  <div className="segment-hint">Нажми, чтобы выбрать этап</div>
                  {matchStagesLoading ? (
                    <div className="tournament-stage-loading" aria-live="polite">
                      Загружаю этапы турнира…
                    </div>
                  ) : (
                    <div className="tournament-stage-stack">
                      {[groupStageTabs, knockoutStageTabs].map((tabs, rowIdx) => (
                        <div className="tournament-row tournament-row-unified" key={`stage-row-${rowIdx}`}>
                          {tabs.map((tab) => {
                            const isActive =
                              tab.type === 'playoff'
                                ? stageTab === 'PO' && playoffTab === tab.key
                                : stageTab === tab.key
                            return (
                              <button
                                key={`${tab.type}-${tab.key}`}
                                className={`tournament-chip ${tab.type === 'playoff' ? 'is-playoff' : ''} ${
                                  tab.type === 'stage' && tab.key === 'LT' ? 'is-longterm' : ''
                                } ${isActive ? 'is-active' : ''}`}
                                onClick={() => {
                                  if (tab.type === 'playoff') {
                                    setStageTab('PO')
                                    setPlayoffTab(tab.key)
                                    return
                                  }
                                  setStageTab(tab.key)
                                }}
                              >
                                {tab.label}
                              </button>
                            )
                          })}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </section>
            ) : null}

            {stageTab !== 'LT' && predictData?.joined !== false && (predictData || predictionsData) ? (
              <section className="cards space-top">
                <div className="card card-static matches-overview-card">
                  <div className="matches-overview-top">
                    <span>Сводка</span>
                    <b>{placedPredictionCount}/{overviewMatchesTotal}</b>
                  </div>
                  <div className="matches-overview-progress" aria-hidden="true">
                    <span style={{ width: `${overviewProgressPct}%` }} />
                  </div>
                  <div className="matches-overview-text">
                    Проставлено <b>{placedPredictionCount}</b> из <b>{overviewMatchesTotal}</b>
                    <span> · </span>
                    Активные <b>{openPredictionCount}</b>
                    <span> · </span>
                    Завершённые <b>{closedPredictionCount}</b>
                    {predictionsData?.total_points_closed != null ? (
                      <>
                        <span> · </span>
                        Очки <b>{predictionsData.total_points_closed}</b>
                      </>
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
                        <>Не удалось загрузить доп. прогнозы. Попробуй обновить экран.</>
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
                  <section className="cards space-top longterm-cards">
                    <div className="card longterm-card">
                      <div className="card-title">🏆 Победитель ЧМ</div>
                      <div className="predict-row">
                        <div className="duel-picker-wrap longterm-picker-wrap">
                          <button
                            className={`duel-picker-btn ${winnerPickerOpen ? 'is-open' : ''}`}
                            onClick={() => {
                              setWinnerPickerOpen((v) => !v)
                              setScorerPickerOpen(false)
                            }}
                          >
                            <span className="duel-picker-value">{winnerPickInput ? teamWithFlag(winnerPickInput) : 'Выбери команду'}</span>
                            <span className={`duel-picker-chevron ${winnerPickerOpen ? 'is-open' : ''}`}>⌄</span>
                          </button>
                          {winnerPickerOpen ? (
                            <div className="duel-picker-panel">
                              <input
                                className="duel-picker-search"
                                value={winnerSearch}
                                onChange={(e) => setWinnerSearch(e.target.value)}
                                placeholder="Поиск команды"
                              />
                              <div className="duel-picker-list">
                                {winnerFilteredOptions.map((name) => (
                                  <button
                                    key={name}
                                    className={`duel-picker-item ${winnerPickInput === name ? 'is-selected' : ''}`}
                                    onClick={() => {
                                      setWinnerPickInput(name)
                                      setWinnerPickerOpen(false)
                                    }}
                                  >
                                    <div className="duel-picker-item-title">{teamWithFlag(name)}</div>
                                  </button>
                                ))}
                                {winnerFilteredOptions.length === 0 ? (
                                  <div className="longterm-picker-empty">Ничего не найдено</div>
                                ) : null}
                              </div>
                            </div>
                          ) : null}
                        </div>
                        <button
                          className={`save-btn longterm-save-btn ${winnerVisualState}`}
                          onClick={() => saveLongtermPick('winner')}
                          disabled={
                            savingAllLongterm ||
                            savingLongtermType === 'winner' ||
                            longtermLocked ||
                            longtermData?.joined === false ||
                            !winnerDirty
                          }
                        >
                          {savingLongtermType === 'winner'
                            ? 'Сохраняю...'
                            : winnerVisualState === 'is-saved'
                              ? 'Сохранено'
                              : 'Сохранить'}
                        </button>
                      </div>
                    </div>

                    <div className="card longterm-card">
                      <div className="card-title">⚽ Лучший бомбардир</div>
                      <div className="predict-row">
                        <div className="duel-picker-wrap longterm-picker-wrap">
                          <button
                            className={`duel-picker-btn ${scorerPickerOpen ? 'is-open' : ''}`}
                            onClick={() => {
                              setScorerPickerOpen((v) => !v)
                              setWinnerPickerOpen(false)
                            }}
                          >
                            <span className="duel-picker-value">{scorerPickInput || 'Выбери игрока'}</span>
                            <span className={`duel-picker-chevron ${scorerPickerOpen ? 'is-open' : ''}`}>⌄</span>
                          </button>
                          {scorerPickerOpen ? (
                            <div className="duel-picker-panel">
                              <input
                                className="duel-picker-search"
                                value={scorerSearch}
                                onChange={(e) => setScorerSearch(e.target.value)}
                                placeholder="Поиск игрока"
                              />
                              <div className="duel-picker-list">
                                {scorerFilteredOptions.map((name) => (
                                  <button
                                    key={name}
                                    className={`duel-picker-item ${scorerPickInput === name ? 'is-selected' : ''}`}
                                    onClick={() => {
                                      setScorerPickInput(name)
                                      setScorerPickerOpen(false)
                                    }}
                                  >
                                    <div className="duel-picker-item-title">{name}</div>
                                  </button>
                                ))}
                                {scorerFilteredOptions.length === 0 ? (
                                  <div className="longterm-picker-empty">Ничего не найдено</div>
                                ) : null}
                              </div>
                            </div>
                          ) : null}
                        </div>
                        <button
                          className={`save-btn longterm-save-btn ${scorerVisualState}`}
                          onClick={() => saveLongtermPick('scorer')}
                          disabled={
                            savingAllLongterm ||
                            savingLongtermType === 'scorer' ||
                            longtermLocked ||
                            longtermData?.joined === false ||
                            !scorerDirty
                          }
                        >
                          {savingLongtermType === 'scorer'
                            ? 'Сохраняю...'
                            : scorerVisualState === 'is-saved'
                              ? 'Сохранено'
                              : 'Сохранить'}
                        </button>
                      </div>
                    </div>
                    <button
                      className={`save-btn save-all-predictions-btn longterm-save-all-btn ${
                        dirtyLongtermPicksCount > 0 ? 'is-dirty' : 'is-saved'
                      }`}
                      onClick={saveAllLongtermPicks}
                      disabled={savingAllLongterm || savingLongtermType !== null || dirtyLongtermPicksCount === 0}
                    >
                      {savingAllLongterm
                        ? 'Сохраняю...'
                        : dirtyLongtermPicksCount > 0
                          ? `Сохранить всё (${dirtyLongtermPicksCount})`
                          : 'Все доп. прогнозы сохранены'}
                    </button>
                  </section>
                ) : null}
              </>
            ) : (
              <>
                {(predictError ||
                  predictionsError ||
                  predictNotice ||
                  matchPredictionsError ||
                  (!predictData || !predictionsData) ||
                  (predictData?.joined === false)) ? (
                  <section className="cards">
                    <div className="card">
                      <div className="card-text">
                        {predictError || predictionsError ? (
                          <>
                            Не удалось загрузить матчи. Попробуй обновить экран.
                            {showDebugPanels ? (
                              <>
                                <br />
                                <br />
                                Debug: {predictError || predictionsError}
                              </>
                            ) : null}
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
                        {matchPredictionsError ? (
                          <>
                            <br />
                            {matchPredictionsError}
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
                            {matches.map((m, matchIndex) => (
                              <div className="compact-match" key={m.match_id}>
                                {(() => {
                                  const showDateBadge = matchIndex === 0
                                  if (m.is_placeholder) {
                                    return (
                                      <>
                                        <div className="match-card-top">
                                          {showDateBadge ? <span className="day-title">{dateKey}</span> : <span />}
                                          {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                                          <span className="kickoff-small">{(m.kickoff || '').split(' ')[1] || ''} МСК</span>
                                        </div>
                                        <div className="compact-main compact-main-result">
                                          <span className="team-name team-left">{teamWithFlag(m.home_team)}</span>
                                          <span className="score-inline-pill">—</span>
                                          <span className="team-name team-right">{teamWithFlag(m.away_team)}</span>
                                        </div>
                                        <div className="match-card-bottom">
                                          <span>Пара определится позже</span>
                                        </div>
                                      </>
                                    )
                                  }
                                  const currentInput = normalizeScore(scoreInputs[m.match_id] || '')
                                  const savedInput = normalizeScore(m.prediction || '')
                                  const isDirty = currentInput !== savedInput
                                  const hasSaved = savedInput.length > 0
                                  const isLocked = Boolean(m.locked)
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
                                      <div className="match-card-top">
                                        {showDateBadge ? <span className="day-title">{dateKey}</span> : <span />}
                                        {m.group_label ? <span className="group-small">[{m.group_label}]</span> : <span className="group-small">—</span>}
                                        <span className="kickoff-small">{(m.kickoff || '').split(' ')[1] || ''} МСК</span>
                                      </div>
                                      <div className="compact-main compact-main-predict">
                                        <span className="team-name team-left">{teamWithFlag(m.home_team)}</span>
                                        {isLocked ? (
                                          <span className="score-inline-static">{savedInput || '-:-'}</span>
                                        ) : (
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
                                        )}
                                        <span className="team-name team-right">{teamWithFlag(m.away_team)}</span>
                                        <button
                                          className={`save-btn compact-save-btn prediction-save-btn ${
                                            isLocked ? 'is-locked' : saveVisualState
                                          }`}
                                          onClick={() => savePrediction(m.match_id)}
                                          disabled={isLocked || savingAllPredictions || isSaving || !canSave}
                                        >
                                          {isLocked
                                            ? 'Матч идёт'
                                            : isSaving
                                            ? '...'
                                            : saveVisualState === 'is-saved'
                                              ? 'Сохранено'
                                              : 'Сохранить'}
                                        </button>
                                      </div>
                                      {crowdText(m) || isLocked ? (
                                        <div className="match-card-bottom">
                                          {crowdText(m) ? (
                                            <span className="community-triplet">
                                              {crowdPercentParts(m).map((part, index) => (
                                                <span key={`${m.match_id}-crowd-${index}`}>{part}</span>
                                              ))}
                                            </span>
                                          ) : (
                                            <span />
                                          )}
                                          {isLocked ? (
                                            <button
                                              type="button"
                                              className="match-predictions-btn"
                                              onClick={() => openMatchPredictions(m.match_id)}
                                              disabled={matchPredictionsLoadingId === m.match_id}
                                            >
                                              {matchPredictionsLoadingId === m.match_id ? '...' : 'Прогнозы'}
                                            </button>
                                          ) : null}
                                        </div>
                                      ) : null}
                                    </>
                                  )
                                })()}
                              </div>
                            ))}
                          </div>
                        ))
                      )}
                      <button
                        className={`save-btn save-all-predictions-btn ${dirtyPredictItemsCount > 0 ? 'is-dirty' : 'is-saved'}`}
                        onClick={saveAllPredictions}
                        disabled={savingAllPredictions || dirtyPredictItemsCount === 0}
                      >
                        {savingAllPredictions
                          ? 'Сохраняю...'
                          : dirtyPredictItemsCount > 0
                            ? `Сохранить все матчи (${dirtyPredictItemsCount})`
                            : 'Все матчи сохранены'}
                      </button>
                    </div>
                  ) : closedPredictionGroups.length === 0 ? (
                    <div className="card">
                      <div className="card-text">Завершённых матчей пока нет.</div>
                    </div>
                  ) : (
                    <div className="card compact-list-card">
                      {closedPredictionGroups.map(([day, items]) => (
                        <div className="day-group day-group-inset" key={day}>
                          {items.map((m) => (
                            <div className="compact-match closed-match" key={m.match_id}>
                              <div className="closed-match-main">
                                <span className="team-name closed-team-name team-left">{teamWithFlag(m.home_team)}</span>
                                <span className="closed-result-score">{m.result || '—'}</span>
                                <span className="team-name closed-team-name team-right">{teamWithFlag(m.away_team)}</span>
                                <div className="closed-action-stack">
                                  <span className="closed-points-badge">
                                    {m.prediction
                                      ? `${m.emoji} ${Number(m.points ?? 0) > 0 ? '+' : ''}${m.points ?? 0}`
                                      : '❌ 0'}
                                  </span>
                                  <button
                                    type="button"
                                    className="match-predictions-link"
                                    onClick={() => openMatchPredictions(m.match_id)}
                                    disabled={matchPredictionsLoadingId === m.match_id}
                                  >
                                    {matchPredictionsLoadingId === m.match_id ? '...' : 'Прогнозы ›'}
                                  </button>
                                </div>
                                <div className="closed-own-prediction">
                                  мой прогноз: <b>{m.prediction || '—'}</b>
                                </div>
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

        {!showRplComingSoon && !showJoinOnboarding && screen === 'profile' ? (
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
                <div className="card-text">
                  Не удалось загрузить профиль. Попробуй снова через несколько секунд.
                  {showDebugPanels ? (
                    <>
                      <br />
                      <br />
                      Debug: {profileError}
                    </>
                  ) : null}
                </div>
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
                          {achievementsExpanded ? 'Скрыть' : 'Показать'}
                        </button>
                      ) : null}
                    </div>
                    {allAchievements.length > 0 ? (
                      <div className="profile-achievements-grid">
                        {visibleAchievements.map((a) => (
                          (() => {
                            const canOpen = a.earned || !a.visual.isSecretLocked
                            return (
                          <button
                            key={a.key}
                            type="button"
                            className={`profile-achievement profile-achievement-btn ${
                              a.earned ? 'is-earned' : 'is-locked'
                            } ${a.taken_by_other ? 'is-taken' : ''} ${a.visual.isSecretLocked ? 'is-secret-locked' : ''}`}
                            title={a.visual.displayDescription}
                            onClick={canOpen ? () => setAchievementPreview(a) : undefined}
                            disabled={!canOpen}
                            aria-disabled={!canOpen}
                          >
                            <span className="profile-achievement-emoji">
                              {a.visual.iconUrl ? (
                                <img src={a.visual.iconUrl} alt={a.visual.displayTitle} className="profile-achievement-icon" />
                              ) : (
                                a.visual.iconEmoji
                              )}
                            </span>
                            {a.taken_by_other && a.taken_by_name ? (
                              <span className="profile-achievement-owner">Получена: {a.taken_by_name}</span>
                            ) : null}
                          </button>
                            )
                          })()
                        ))}
                      </div>
                    ) : (
                      <div className="card-text">Пока нет доступных ачивок.</div>
                    )}
                  </div>

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

                  {profileData.is_self_profile !== false ? (
                    <div className="profile-notifications">
                      <button
                        type="button"
                        className="profile-notifications-row"
                        onClick={() => setNotifModalOpen(true)}
                      >
                        <span>Уведомления</span>
                        <b>{notificationsSummary}</b>
                      </button>
                    </div>
                  ) : null}
                </>
              ) : (
                <div className="card-text">{profileData.message || 'Пока нет активного участия в турнире.'}</div>
              )}
            </div>
          </section>
        ) : null}

        {!showRplComingSoon && !showJoinOnboarding && screen === 'duels' ? (
          <>
            <section className="cards">
              <div className="card">
                {duelsError ? (
                  <div className="card-text">
                    Не удалось загрузить раздел 1x1. Попробуй обновить экран.
                    {showDebugPanels ? (
                      <>
                        <br />
                        <br />
                        Debug: {duelsError}
                      </>
                    ) : null}
                  </div>
                ) : !duelsData ? (
                  <div className="card-text">Загружаю блок 1x1...</div>
                ) : duelsData.joined === false ? (
                  <div className="card-text">{duelsData.message || 'Сначала вступи в турнир.'}</div>
                ) : (
                  <>
                    <div className="duel-head-row">
                      <div className="card-title">{profileData?.display_name || (tgUsername ? `@${tgUsername}` : 'Участник')}</div>
                      <div className="duel-head-actions">
                        <button
                          type="button"
                          className="duel-rules-btn"
                          onClick={() => setDuelLeaderboardOpen(true)}
                        >
                          Таблица 1x1
                        </button>
                        <button
                          type="button"
                          className="duel-rules-btn"
                          onClick={() => setDuelRulesOpen(true)}
                        >
                          Правила
                        </button>
                      </div>
                    </div>
                    <div className="profile-hits-line">
                      Рейтинг: <b>{duelsData.elo?.rating ?? 1000}</b> · W <b>{duelsData.elo?.wins ?? 0}</b> · D <b>{duelsData.elo?.draws ?? 0}</b> · L{' '}
                      <b>{duelsData.elo?.losses ?? 0}</b> · всего <b>{duelsData.elo?.duels_total ?? 0}</b>
                    </div>

                    <div className="card-title duel-challenge-title">Бросить вызов</div>
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
                                  const nextBusyIds = new Set(
                                    (duelBusyOpponentsByMatch[String(m.match_id)] || []).map((id) => Number(id))
                                  )
                                  if (nextBusyIds.has(Number(duelOpponentId))) {
                                    setDuelOpponentId(0)
                                  }
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
                            {duelFilteredOpponents.map((u) => {
                              const opponentIsBusy = isDuelOpponentBusyForMatch(u.tg_user_id)
                              return (
                                <button
                                  key={u.tg_user_id}
                                  className={`duel-picker-item ${
                                    duelOpponentId === u.tg_user_id ? 'is-selected' : ''
                                  } ${opponentIsBusy ? 'is-disabled' : ''}`}
                                  onClick={() => {
                                    if (opponentIsBusy) return
                                    setDuelOpponentId(u.tg_user_id)
                                    setDuelOpponentPickerOpen(false)
                                  }}
                                  disabled={opponentIsBusy}
                                >
                                  <div className="duel-picker-item-title">{u.display_name}</div>
                                  <div className="duel-picker-item-note">
                                    {opponentIsBusy
                                      ? 'Игрок уже участвует в битве на этот матч'
                                      : `Рейтинг: ${u.elo_rating || 1000}`}
                                  </div>
                                </button>
                              )
                            })}
                          </div>
                        </div>
                      ) : null}
                    </div>

                    <div className="predict-row duel-predict-row">
                      <input
                        className="score-input"
                        value={duelScoreInput}
                        onChange={(e) => setDuelScoreInput(formatScoreInput(e.target.value))}
                        placeholder="-:-"
                        inputMode="numeric"
                      />
                      <button
                        className={`save-btn duel-challenge-submit-btn ${normalizeScore(duelScoreInput) ? 'is-dirty' : 'is-empty'}`}
                        onClick={createDuelChallenge}
                        disabled={duelBusyId === -1}
                      >
                        {duelBusyId === -1 ? 'Отправляю...' : 'Бросить вызов'}
                      </button>
                    </div>
                    {duelsNotice ? <div className="card-text duel-notice">{duelsNotice}</div> : null}
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
                      const isOutgoingPending =
                        d.status === 'pending' && tgUserId != null && Number(d.challenger_tg_user_id) === Number(tgUserId)
                      const acceptScore = normalizeScore(duelAcceptInputs[d.duel_id] || '')
                      return (
                        <div
                          className={`compact-match duel-card ${isIncomingPending ? 'is-incoming-pending' : ''} ${
                            duelFocusId === d.duel_id ? 'is-focused' : ''
                          }`}
                          key={`duel-${d.duel_id}`}
                          id={`duel-card-${d.duel_id}`}
                        >
                          <div className="compact-meta duel-card-meta">
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
                            <div className="duel-pred-line duel-player-line">
                              <span className="duel-player-main">
                                {duelsFilter === 'finished' ? (
                                  <>
                                    <span className="duel-player-rating">{d.challenger_rating || 1000}</span>{' '}
                                    <span className={d.elo_delta_challenger >= 0 ? 'duel-delta-plus' : 'duel-delta-minus'}>
                                      ({d.elo_delta_challenger >= 0 ? `+${d.elo_delta_challenger}` : d.elo_delta_challenger})
                                    </span>{' '}
                                  </>
                                ) : (
                                  <>
                                    <span className="duel-player-rating">{d.challenger_rating || 1000}</span>{' '}
                                  </>
                                )}
                                <span className="duel-player-name">{d.challenger_name}</span>
                              </span>
                              <b className="duel-player-pred">{d.challenger_pred}</b>
                            </div>
                            <div className="duel-pred-line duel-player-line">
                              <span className="duel-player-main">
                                {duelsFilter === 'finished' ? (
                                  <>
                                    <span className="duel-player-rating">{d.opponent_rating || 1000}</span>{' '}
                                    <span className={d.elo_delta_opponent >= 0 ? 'duel-delta-plus' : 'duel-delta-minus'}>
                                      ({d.elo_delta_opponent >= 0 ? `+${d.elo_delta_opponent}` : d.elo_delta_opponent})
                                    </span>{' '}
                                  </>
                                ) : (
                                  <>
                                    <span className="duel-player-rating">{d.opponent_rating || 1000}</span>{' '}
                                  </>
                                )}
                                <span className="duel-player-name">{d.opponent_name}</span>
                              </span>
                              <b className="duel-player-pred">{d.opponent_pred || '—'}</b>
                            </div>
                            <div className="compact-note">
                              Личные встречи · <b>{d.h2h_wins || 0}-{d.h2h_draws || 0}-{d.h2h_losses || 0}</b> (W-D-L)
                            </div>
                          </div>

                          {isIncomingPending ? (
                            <div className="predict-row duel-predict-row duel-accept-row">
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
                                className={`duel-accept-btn ${acceptScore ? 'is-ready' : 'is-empty'}`}
                                onClick={() => respondDuel(d.duel_id, 'accept', duelAcceptInputs[d.duel_id] || '')}
                                disabled={duelBusyId === d.duel_id}
                              >
                                {duelBusyId === d.duel_id ? 'Принимаю...' : 'Принять вызов'}
                              </button>
                            </div>
                          ) : null}
                          {isOutgoingPending ? (
                            <div className="duel-card-actions">
                              <button
                                className="duel-cancel-btn"
                                onClick={() => cancelDuel(d.duel_id)}
                                disabled={duelBusyId === d.duel_id}
                              >
                                {duelBusyId === d.duel_id ? 'Отменяю...' : 'Отменить вызов'}
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

        {!showRplComingSoon && !showJoinOnboarding && screen === 'table' ? (
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
                    <button
                      className={`tournament-chip ${tableRoundFilter === 'LT' ? 'is-active' : ''}`}
                      onClick={() => setTableRoundFilter('LT')}
                    >
                      Доп. прогнозы
                    </button>
                  </div>
                </div>
              </section>
            ) : null}

            <section className="cards space-top">
              {tableError ? (
                <div className="card">
                  <div className="card-text">
                    Не удалось загрузить таблицу. Попробуй обновить экран.
                    {showDebugPanels ? (
                      <>
                        <br />
                        <br />
                        Debug: {tableError}
                      </>
                    ) : null}
                  </div>
                </div>
              ) : !tableData ? (
                <div className="card">
                  <div className="card-text">Загружаю таблицу...</div>
                </div>
              ) : tableData?.table_mode === 'longterm' ? (
                <div className="card table-card">
                  <div className="table-grid-longterm table-grid-head">
                    <div className="col-place">#</div>
                    <div className="col-name">Имя</div>
                    <div className="col-name">🏆</div>
                    <div className="col-name">⚽</div>
                    <div className="col-num">⭐</div>
                  </div>
                  {tableLongtermRows.length > 0 ? (
                    tableLongtermRows.map((r) => (
                      <div
                        className={`table-grid-longterm table-grid-row ${tableData?.user_place === r.place ? 'is-user' : ''}`}
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
                        <div className="col-name col-name-text">{r.winner_pick || '—'}</div>
                        <div className="col-name col-name-text">{r.scorer_pick || '—'}</div>
                        <div className="col-num">{r.longterm_points ?? 0}</div>
                      </div>
                    ))
                  ) : (
                    <div className="card-text table-empty-note">Пока нет участников в таблице доп. прогнозов.</div>
                  )}
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
                    const bonusPoints = Math.max(0, Number(r.bonus_points ?? 0))
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

            {tableData?.table_mode !== 'longterm' ? (
              <section className="cards space-top">
                <div className="card">
                  <div className="card-title">Расшифровка</div>
                  <div className="card-text table-legend-text">
                    🎯 Точный счёт: 4 очка
                    <br />
                    📏 Разница мячей: 2 очка
                    <br />
                    ✅ Исход: 1 очко
                    <br />
                    ⛔ Пропущенные матчи: 0 очков
                    <br />
                    ⭐ Доп. прогнозы / бонусы: +5 за каждый угаданный доп. прогноз
                    <br />
                    <br />
                    Коэффициенты плей-офф
                    <br />
                    В плей-офф очки за матч умножаются на коэффициент стадии.
                    <br />
                    1/16, 1/8: x1
                    <br />
                    1/4, 1/2: x2
                    <br />
                    Финал, 3-е место: x3
                  </div>
                </div>
              </section>
            ) : null}
          </>
        ) : null}

        {screen === 'admin' ? (
          <>
            <section className="cards">
              <div className="card card-static">
                <div className="card-title">Панель управления</div>
                <div className="segment-hint">Открой нужный блок. Повторный тап свернёт его.</div>

                <div className="admin-accordion-list">
                  <button
                    className={`admin-accordion-head ${adminViewMode === 'matches' ? 'is-active' : ''}`}
                    onClick={() => toggleAdminViewMode('matches')}
                  >
                    <span>
                      <b>Матчи</b>
                      <small>
                        {adminRoundName
                          ? `${adminRoundName} · без итогов ${adminWithoutResult}`
                          : `раундов ${adminRounds.length}`}
                      </small>
                    </span>
                    <span className="admin-accordion-caret">{adminViewMode === 'matches' ? '⌃' : '⌄'}</span>
                  </button>
                  {adminViewMode === 'matches' ? renderAdminMatchesContent() : null}

                  {selectedTournamentCode === 'WC2026' ? (
                    <>
                      <button
                        className={`admin-accordion-head ${adminViewMode === 'longterm' ? 'is-active' : ''}`}
                        onClick={() => toggleAdminViewMode('longterm')}
                      >
                        <span>
                          <b>Доп. прогнозы</b>
                          <small>
                            участников {adminLongtermParticipants} · очки {adminLongtermWinnerAwarded + adminLongtermScorerAwarded}
                          </small>
                        </span>
                        <span className="admin-accordion-caret">{adminViewMode === 'longterm' ? '⌃' : '⌄'}</span>
                      </button>
                      {adminViewMode === 'longterm' ? renderAdminLongtermContent() : null}
                    </>
                  ) : null}

                  <button
                    className={`admin-accordion-head ${adminViewMode === 'participants' ? 'is-active' : ''}`}
                    onClick={() => toggleAdminViewMode('participants')}
                  >
                    <span>
                      <b>Участники</b>
                      <small>{adminParticipants.length ? `${adminParticipants.length} в турнире` : 'список для удаления'}</small>
                    </span>
                    <span className="admin-accordion-caret">{adminViewMode === 'participants' ? '⌃' : '⌄'}</span>
                  </button>
                  {adminViewMode === 'participants' ? renderAdminParticipantsContent() : null}

                  {selectedTournamentCode === 'RPL' ? (
                    <>
                      <button
                        className={`admin-accordion-head ${adminViewMode === 'rpl_season' ? 'is-active' : ''}`}
                        onClick={() => toggleAdminViewMode('rpl_season')}
                      >
                        <span>
                          <b>Сезон РПЛ</b>
                          <small>
                            {adminRplSeason?.season
                              ? `${adminRplSeason.season.name} · набор ${adminRplSeason.enrollment_open ? 'открыт' : 'закрыт'}`
                              : 'сезон ещё не создан'}
                          </small>
                        </span>
                        <span className="admin-accordion-caret">{adminViewMode === 'rpl_season' ? '⌃' : '⌄'}</span>
                      </button>
                      {adminViewMode === 'rpl_season' ? renderAdminRplSeasonContent() : null}

                      <button
                        className={`admin-accordion-head ${adminViewMode === 'rpl_participants' ? 'is-active' : ''}`}
                        onClick={() => toggleAdminViewMode('rpl_participants')}
                      >
                        <span>
                          <b>Участники и лиги</b>
                          <small>
                            {adminRplSeason?.counts
                              ? `не распределено ${adminRplSeason.counts.unassigned}`
                              : 'распределение по Высшей/Низшей'}
                          </small>
                        </span>
                        <span className="admin-accordion-caret">{adminViewMode === 'rpl_participants' ? '⌃' : '⌄'}</span>
                      </button>
                      {adminViewMode === 'rpl_participants' ? renderAdminRplParticipantsContent() : null}
                    </>
                  ) : null}

                  <button
                    className={`admin-accordion-head ${adminViewMode === 'duels' ? 'is-active' : ''}`}
                    onClick={() => toggleAdminViewMode('duels')}
                  >
                    <span>
                      <b>Битвы 1x1</b>
                      <small>активные и завершённые дуэли</small>
                    </span>
                    <span className="admin-accordion-caret">{adminViewMode === 'duels' ? '⌃' : '⌄'}</span>
                  </button>
                  {adminViewMode === 'duels' ? renderAdminDuelsContent() : null}
                </div>

                {adminError ? <div className="card-text admin-status-line">Ошибка: {adminError}</div> : null}
                {adminNotice ? <div className="card-text admin-status-line">{adminNotice}</div> : null}
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
              {!achievementPreviewIsLockedHint ? (
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
              ) : null}
              <div className="achievement-modal-title">{achievementPreviewVisual.displayTitle}</div>
              {achievementPreviewIsUnique && !achievementPreviewIsLockedHint ? (
                <div className="achievement-modal-unique">Уникальная ачивка</div>
              ) : null}
              <div className="achievement-modal-description">
                {achievementPreviewIsLockedHint ? achievementPreviewLockedHintText : achievementPreviewVisual.displayDescription}
              </div>
              {!achievementPreviewIsLockedHint && achievementPreview.match_context ? (
                <div className="achievement-match-context">
                  <div className="achievement-match-label">Матч</div>
                  <div className="achievement-match-title">
                    {achievementPreview.match_context.home_team} {achievementPreview.match_context.result || '-:-'}{' '}
                    {achievementPreview.match_context.away_team}
                  </div>
                  {achievementPreview.match_context.prediction ? (
                    <div className="achievement-match-line">
                      Твой прогноз: <b>{achievementPreview.match_context.prediction}</b>
                    </div>
                  ) : null}
                  {achievementPreview.match_context.points != null ? (
                    <div className="achievement-match-line">
                      Очки за матч: <b>{achievementPreview.match_context.points}</b>
                    </div>
                  ) : null}
                  {achievementPreview.match_context.total_after != null ? (
                    <div className="achievement-match-line">
                      После матча: <b>{achievementPreview.match_context.total_after}</b>
                    </div>
                  ) : null}
                </div>
              ) : null}
              {!achievementPreviewIsLockedHint && achievementPreviewGroup ? (
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
                        <div className="achievement-level-counter">
                          {`${Math.min(
                            achievementPreviewGroupCurrent,
                            LEVEL_TARGETS_BY_BASE[achievementPreviewGroupBase || '']?.[idx] || 0
                          )}/${LEVEL_TARGETS_BY_BASE[achievementPreviewGroupBase || '']?.[idx] || 0}`}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              ) : !achievementPreviewIsLockedHint ? (
                <div className="achievement-modal-meta">{achievementPreview.earned ? 'Получена' : 'Не получена'}</div>
              ) : null}
            </div>
          </div>
        ) : null}

        {notifModalOpen ? (
          <div className="notif-modal-overlay" onClick={() => setNotifModalOpen(false)}>
            <div className="notif-modal-card" onClick={(ev) => ev.stopPropagation()}>
              <button
                type="button"
                className="notif-modal-close"
                onClick={() => setNotifModalOpen(false)}
                aria-label="Закрыть"
              >
                ✕
              </button>
              <div className="notif-modal-title">Уведомления</div>
              <div className="notif-modal-list">
                {[
                  { key: 'all', label: 'Все уведомления' },
                  { key: 'reminders', label: 'Напоминания о матчах' },
                  { key: 'duels', label: 'События 1x1' },
                  { key: 'achievements', label: 'Новые ачивки' },
                ].map((item) => {
                  const key = item.key as 'all' | 'reminders' | 'duels' | 'achievements'
                  const enabled = Boolean(notifPrefs[key])
                  const disabled = notifSavingType !== null
                  return (
                    <button
                      key={item.key}
                      type="button"
                      className={`notif-switch-row ${enabled ? 'is-on' : 'is-off'} ${disabled ? 'is-disabled' : ''}`}
                      onClick={() => updateNotifPref(key, !enabled)}
                      disabled={disabled}
                    >
                      <span>{item.label}</span>
                      <span className="notif-switch-pill">{enabled ? 'Вкл' : 'Выкл'}</span>
                    </button>
                  )
                })}
              </div>
              {notifError && showDebugPanels ? <div className="notif-modal-error">Debug: {notifError}</div> : null}
            </div>
          </div>
        ) : null}

        {matchPredictionsSheet ? (
          <div className="match-predictions-sheet-overlay" onClick={() => setMatchPredictionsSheet(null)}>
            <div
              className={`match-predictions-sheet ${matchPredictionsSheet.status === 'closed' ? 'is-closed' : ''}`}
              onClick={(ev) => ev.stopPropagation()}
            >
              <div className="match-predictions-sheet-head">
                <div>
                  <div className="match-predictions-title">Прогнозы участников</div>
                  <div className="match-predictions-subtitle">
                    {teamWithFlag(matchPredictionsSheet.home_team || '')}{' '}
                    {matchPredictionsSheet.status === 'closed' && matchPredictionsSheet.result ? matchPredictionsSheet.result : '—'}{' '}
                    {teamWithFlag(matchPredictionsSheet.away_team || '')}
                  </div>
                </div>
                <button
                  type="button"
                  className="match-predictions-close"
                  onClick={() => setMatchPredictionsSheet(null)}
                  aria-label="Закрыть прогнозы матча"
                >
                  ✕
                </button>
              </div>
              <div className="match-predictions-list">
                {(matchPredictionsSheet.items || []).length ? (
                  (matchPredictionsSheet.items || []).map((row) => {
                    const pos = matchPredictionsSheet.status === 'closed' ? positionChangeLabel(row) : null
                    return (
                      <div
                        className={`match-prediction-row ${row.is_me ? 'is-me' : ''} ${row.prediction ? '' : 'is-missed'}`}
                        key={`match-prediction-${matchPredictionsSheet.match_id}-${row.tg_user_id}`}
                      >
                        {matchPredictionsSheet.status === 'closed' ? (
                          <div className={`match-prediction-position ${pos ? `is-${pos.tone}` : ''}`}>
                            {pos?.label || '—'}
                          </div>
                        ) : null}
                        <div className="match-prediction-name-line">
                          <span className="match-prediction-name">{row.name}</span>
                          {row.is_me ? <span className="match-prediction-me-badge">ты</span> : null}
                        </div>
                        <div className="match-prediction-score">{row.prediction || '—'}</div>
                        {matchPredictionsSheet.status === 'closed' ? (
                          <div className="match-prediction-result">
                            {row.prediction ? `${row.emoji || '❌'} ${Number(row.points ?? 0) > 0 ? '+' : ''}${row.points ?? 0}` : '—'}
                          </div>
                        ) : null}
                      </div>
                    )
                  })
                ) : (
                  <div className="match-predictions-empty">Прогнозов пока нет.</div>
                )}
              </div>
            </div>
          </div>
        ) : null}

        {duelRulesOpen ? (
          <div className="duel-rules-modal-overlay" onClick={() => setDuelRulesOpen(false)}>
            <div className="duel-rules-modal-card" onClick={(ev) => ev.stopPropagation()}>
              <button
                type="button"
                className="duel-rules-modal-close"
                onClick={() => setDuelRulesOpen(false)}
                aria-label="Закрыть"
              >
                ✕
              </button>
              <div className="duel-rules-modal-title">Правила 1x1</div>
              <div className="duel-rules-modal-content">
                {DUEL_RULES_SECTIONS.map((section) => (
                  <div className="duel-rules-section" key={section.title}>
                    <div className="duel-rules-section-title">{section.title}</div>
                    {section.lines.map((line) => (
                      <div className="duel-rules-section-line" key={`${section.title}-${line}`}>{line}</div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : null}

        {duelLeaderboardOpen ? (
          <div className="duel-rating-sheet-overlay" onClick={() => setDuelLeaderboardOpen(false)}>
            <div className="duel-rating-sheet" onClick={(ev) => ev.stopPropagation()}>
              <div className="duel-rating-sheet-head">
                <div>
                  <div className="duel-rating-title">Таблица 1x1</div>
                  <div className="duel-rating-subtitle">Сквозной Elo по всем турнирам</div>
                </div>
                <button
                  type="button"
                  className="duel-rating-close"
                  onClick={() => setDuelLeaderboardOpen(false)}
                  aria-label="Закрыть рейтинг 1x1"
                >
                  ✕
                </button>
              </div>
              <div className="duel-rating-list">
                {duelLeaderboard.length ? (
                  duelLeaderboard.map((row) => {
                    const isMe = tgUserId != null && Number(row.tg_user_id) === Number(tgUserId)
                    return (
                      <div className={`duel-rating-row ${isMe ? 'is-me' : ''}`} key={`duel-rating-${row.tg_user_id}`}>
                        <div className="duel-rating-place">{row.place}</div>
                        <div className="duel-rating-body">
                          <div className="duel-rating-name-line">
                            <span className="duel-rating-name">{row.display_name}</span>
                            {isMe ? <span className="duel-rating-me-badge">ты</span> : null}
                          </div>
                          <div className="duel-rating-record">
                            {row.duels_total} битв · W {row.wins} · D {row.draws} · L {row.losses}
                          </div>
                        </div>
                        <div className="duel-rating-score">
                          <b>{row.rating}</b>
                          <span>Elo</span>
                        </div>
                      </div>
                    )
                  })
                ) : (
                  <div className="duel-rating-empty">Рейтинг появится после первых дуэлей.</div>
                )}
              </div>
            </div>
          </div>
        ) : null}

        <footer className="footer-note">Удачи в прогнозах.</footer>
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
