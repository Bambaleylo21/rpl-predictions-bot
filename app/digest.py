from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass
from typing import Any

from sqlalchemy import case, func, select

from app.db import SessionLocal
from app.display import display_round_name, display_team_name
from app.league_table import _resolve_name, build_active_stage_league_table
from app.models import League, LeagueParticipant, Match, Point, Prediction, Stage, Tournament, User, UserTournament
from app.season_setup import get_active_season, get_active_stage

logger = logging.getLogger(__name__)

# Порядок и подписи лиг РПЛ — используются везде в дайджестах.
LEAGUE_ORDER: tuple[tuple[str, str], ...] = (("HIGH", "Высшая лига"), ("LOW", "Низшая лига"))

# Минимальная длина серии (в турах подряд), чтобы вообще упоминать её как инсайт —
# отсекаем шум вроде "2 тура подряд набирает 7+ очков".
MIN_STREAK_LENGTH = 3
# Порог очков за тур, который считаем "стабильно хорошим результатом" для инсайтов.
STRONG_ROUND_POINTS = 7
# Порог очков за тур, который считаем "антирекордным"/провальным туром.
WEAK_ROUND_POINTS = 1
# Сколько точных счётов в одном туре считаем поводом для отдельного инсайта
# (срабатывает при exact > этого значения, т.е. 4 и больше).
ROUND_EXACT_HIGHLIGHT_THRESHOLD = 3
# Минимальное число мест, на которое нужно подняться за тур, чтобы это
# считалось "самым большим рывком тура" (а не рядовым колебанием).
MIN_RISE_FOR_INSIGHT = 2
# Ниже этого числа сыгранных матчей в туре точностные инсайты (идеальный /
# провальный тур по попаданиям) не считаем — на 1-2 матчах это шум.
MIN_ROUND_SIZE_FOR_ACCURACY_INSIGHTS = 4
# Минимальное число сделанных прогнозов на конкретный матч, чтобы вообще
# имело смысл говорить "все угадали" / "никто не угадал" по этому матчу.
MIN_PREDICTORS_FOR_MATCH_INSIGHT = 3

# Статистические аномалии (тур целиком, личный результат тура, пропущенные
# матчи) считаем через отклонение от среднего в стандартных отклонениях —
# калибровка на реальных данных сезона (см. обсуждение с пользователем):
# 2 стандартных отклонения — редкое, действительно заметное отклонение.
ANOMALY_Z_THRESHOLD = 2.0
# Минимальный размер выборки (прошлых туров для истории игрока/лиги, или
# участников в лиге для сравнения между ними), прежде чем считать
# среднее/стдев статистически осмысленными — на 1-2 значениях любое число
# будет "аномалией".
MIN_SAMPLE_SIZE_FOR_ANOMALY = 5


@dataclass
class RoundSnapshot:
    round_number: int
    league_code: str
    league_name: str
    rows: list[dict[str, Any]]  # place/tg_user_id/name/total/exact/diff/outcome, уже отсортировано


async def _last_played_round(session, tournament_id: int, round_min: int, round_max: int) -> int | None:
    """Номер последнего тура, в котором уже есть хотя бы один сыгранный (с
    известным счётом) матч — используется как дефолтная граница для
    "текущего состояния" турнирного дайджеста и для перебора уже сыгранных
    туров при поиске рекордов/серий."""
    q = await session.execute(
        select(func.max(Match.round_number)).where(
            Match.tournament_id == int(tournament_id),
            Match.is_placeholder == 0,
            Match.round_number >= round_min,
            Match.round_number <= round_max,
            Match.home_score.is_not(None),
            Match.away_score.is_not(None),
        )
    )
    val = q.scalar_one_or_none()
    return int(val) if val is not None else None


async def _table_upto_round(
    session, rpl_tournament_id: int, season_id: int, stage: Stage, league: League, upto_round: int
) -> list[dict[str, Any]]:
    """Кумулятивная таблица лиги по очкам матчей с round_number <= upto_round.
    Та же логика тай-брейков, что и в build_active_stage_league_table, но с
    произвольной верхней границей тура — нужно для "место было / место
    стало" в дайджесте тура. upto_round < stage.round_min даёт пустую
    (ещё-не-начавшуюся) таблицу."""
    participant_rows = (
        await session.execute(
            select(
                LeagueParticipant.tg_user_id,
                LeagueParticipant.display_name,
                UserTournament.display_name,
                UserTournament.bonus_points,
                User.display_name,
                User.username,
                User.full_name,
            )
            .select_from(LeagueParticipant)
            .outerjoin(
                UserTournament,
                (UserTournament.tg_user_id == LeagueParticipant.tg_user_id)
                & (UserTournament.tournament_id == int(rpl_tournament_id)),
            )
            .outerjoin(User, User.tg_user_id == LeagueParticipant.tg_user_id)
            .where(
                LeagueParticipant.stage_id == int(stage.id),
                LeagueParticipant.league_id == int(league.id),
                LeagueParticipant.is_active == 1,
            )
        )
    ).all()
    if not participant_rows:
        return []

    user_ids = [int(r[0]) for r in participant_rows]
    points_map: dict[int, dict[str, int]] = {}
    if upto_round >= int(stage.round_min):
        points_rows = (
            await session.execute(
                select(
                    Point.tg_user_id,
                    func.coalesce(func.sum(Point.points), 0).label("total"),
                    func.coalesce(func.sum(case((Point.category == "exact", 1), else_=0)), 0).label("exact"),
                    func.coalesce(func.sum(case((Point.category == "diff", 1), else_=0)), 0).label("diff"),
                    func.coalesce(func.sum(case((Point.category == "outcome", 1), else_=0)), 0).label("outcome"),
                )
                .select_from(Point)
                .join(Match, Match.id == Point.match_id)
                .where(
                    Point.tg_user_id.in_(user_ids),
                    Match.tournament_id == int(rpl_tournament_id),
                    Match.season_id == int(season_id),
                    Match.round_number <= int(upto_round),
                )
                .group_by(Point.tg_user_id)
            )
        ).all()
        points_map = {
            int(uid): {"total": int(total or 0), "exact": int(exact or 0), "diff": int(diff or 0), "outcome": int(outcome or 0)}
            for uid, total, exact, diff, outcome in points_rows
        }

    rows: list[dict[str, Any]] = []
    for uid, lp_name, ut_name, ut_bonus, u_name, username, full_name in participant_rows:
        tgid = int(uid)
        pts = points_map.get(tgid, {})
        total = int(pts.get("total", 0)) + int(ut_bonus or 0)
        rows.append(
            {
                "tg_user_id": tgid,
                "name": _resolve_name(lp_name, ut_name, u_name, username, full_name, tgid),
                "total": total,
                "exact": int(pts.get("exact", 0)),
                "diff": int(pts.get("diff", 0)),
                "outcome": int(pts.get("outcome", 0)),
            }
        )
    rows.sort(key=lambda r: (-r["total"], -r["exact"], -r["diff"], -r["outcome"], r["tg_user_id"]))
    for i, r in enumerate(rows, start=1):
        r["place"] = i
    return rows


async def _round_snapshots(
    round_min: int, last_round: int, leagues: dict[str, League]
) -> list[RoundSnapshot]:
    """Прогоняет build_active_stage_league_table по каждому уже сыгранному
    туру и лиге — переиспользует ту же боевую логику ранжирования тура
    (с корректными тай-брейками по точным/разница/исходу), что и вкладка
    "Таблица" в мини-аппе, чтобы дайджест не расходился с тем, что видит
    пользователь в приложении."""
    out: list[RoundSnapshot] = []
    for round_number in range(round_min, last_round + 1):
        for code, label in LEAGUE_ORDER:
            if code not in leagues:
                continue
            rows, _meta = await build_active_stage_league_table(0, code, round_number=round_number)
            if rows:
                out.append(RoundSnapshot(round_number=round_number, league_code=code, league_name=label, rows=rows))
    return out


def _medal(place: int) -> str:
    return {1: "🥇", 2: "🥈", 3: "🥉"}.get(place, f"{place}.")


def _delta_arrow(before: int | None, after: int | None) -> str:
    if before is None or after is None:
        return ""
    delta = before - after
    if delta > 0:
        return f" (⬆{delta})"
    if delta < 0:
        return f" (⬇{abs(delta)})"
    return " (•)"


def _detect_current_streaks(snapshots: list[RoundSnapshot], as_of_round: int) -> list[str]:
    """Ищет серии (>= MIN_STREAK_LENGTH туров подряд, заканчивающиеся именно
    на as_of_round), которые ещё продолжаются на момент as_of_round: 1-е
    место в туре, последнее место в туре, "стабильно 7+ очков"."""
    by_user: dict[int, list[tuple[int, str, dict[str, Any], int]]] = {}
    league_sizes: dict[tuple[int, str], int] = {}
    for snap in snapshots:
        league_sizes[(snap.round_number, snap.league_code)] = len(snap.rows)
        for row in snap.rows:
            uid = int(row["tg_user_id"])
            by_user.setdefault(uid, []).append((snap.round_number, snap.league_code, row, len(snap.rows)))

    insights: list[str] = []
    for uid, entries in by_user.items():
        entries.sort(key=lambda e: e[0])
        if not entries or entries[-1][0] != as_of_round:
            continue
        name = entries[-1][2]["name"]

        def _walk_back(predicate) -> int:
            length = 0
            for round_number, _league_code, row, league_size in reversed(entries):
                if not predicate(row, league_size):
                    break
                length += 1
            return length

        first_len = _walk_back(lambda row, _size: int(row["place"]) == 1)
        last_len = _walk_back(lambda row, size: int(row["place"]) == size and size > 1)
        strong_len = _walk_back(lambda row, _size: int(row["total"]) >= STRONG_ROUND_POINTS)
        exact_len = _walk_back(lambda row, _size: int(row.get("exact", 0)) >= 1)

        if first_len >= MIN_STREAK_LENGTH:
            insights.append(f"🔥 {name} занимает 1-е место в туре уже {first_len} тура(ов) подряд.")
        if last_len >= MIN_STREAK_LENGTH:
            insights.append(f"🥶 {name} финиширует последним в туре {last_len} тура(ов) подряд.")
        if strong_len >= MIN_STREAK_LENGTH:
            insights.append(f"📈 {name} стабильно набирает {STRONG_ROUND_POINTS}+ очков — уже {strong_len} тура(ов) подряд.")
        if exact_len >= MIN_STREAK_LENGTH:
            insights.append(f"🎯 {name} угадывает точный счёт уже {exact_len} тура(ов) подряд.")
    return insights


async def _match_level_round_insights(
    session, rpl_tournament_id: int, season_id: int, round_number: int, finished_match_ids: list[int]
) -> list[str]:
    """Инсайты по конкретным матчам тура: матч, который угадали абсолютно
    все участники (по исходу и точнее), и матч, который не угадал никто —
    оба варианта интересны сами по себе, независимо от результатов туров
    участников. Считаем только по уже сыгранным матчам тура и только если
    прогнозов на матч набралось достаточно, чтобы не ловить шум на 1-2
    прогнозах."""
    if not finished_match_ids:
        return []

    rows = (
        await session.execute(
            select(
                Prediction.match_id,
                Match.home_team,
                Match.away_team,
                func.count(Prediction.id).label("total_preds"),
                func.coalesce(func.sum(case((Point.points > 0, 1), else_=0)), 0).label("hits"),
            )
            .select_from(Prediction)
            .join(Match, Match.id == Prediction.match_id)
            .outerjoin(
                Point,
                (Point.match_id == Prediction.match_id) & (Point.tg_user_id == Prediction.tg_user_id),
            )
            .where(Prediction.match_id.in_(finished_match_ids))
            .group_by(Prediction.match_id, Match.home_team, Match.away_team)
        )
    ).all()

    out: list[str] = []
    for match_id, home_team, away_team, total_preds, hits in rows:
        total_preds = int(total_preds or 0)
        hits = int(hits or 0)
        if total_preds < MIN_PREDICTORS_FOR_MATCH_INSIGHT:
            continue
        label = f"{display_team_name(home_team)} — {display_team_name(away_team)}"
        if hits == total_preds:
            out.append(f"✅ Матч {label} угадали все участники (по исходу).")
        elif hits == 0:
            out.append(f"🎲 Матч {label} не угадал ни один участник — полная неожиданность.")
    return out


async def build_round_digest_text(round_number: int) -> str | None:
    """Собирает текстовый дайджест одного тура РПЛ. Возвращает None, если
    активного сезона/этапа нет или указанный тур ещё не начинался."""
    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            return None
        stage = await get_active_stage(session, int(season.id))
        if stage is None:
            return None
        rpl = (await session.execute(select(Tournament).where(Tournament.code == "RPL"))).scalar_one_or_none()
        if rpl is None:
            return None
        if round_number < int(stage.round_min) or round_number > int(stage.round_max):
            return None

        league_rows = (
            await session.execute(select(League).where(League.season_id == season.id, League.is_active == 1))
        ).scalars().all()
        leagues = {str(l.code).upper(): l for l in league_rows}
        if not leagues:
            return None

        round_label = display_round_name("RPL", round_number)
        lines: list[str] = [f"📊 Итоги «{round_label}» — РПЛ", ""]

        match_rows = (
            await session.execute(
                select(Match.id, Match.home_score, Match.away_score)
                .where(
                    Match.tournament_id == rpl.id,
                    Match.season_id == season.id,
                    Match.round_number == round_number,
                    Match.is_placeholder == 0,
                )
            )
        ).all()
        finished_match_ids = [int(mid) for mid, hs, aws in match_rows if hs is not None and aws is not None]
        # Число сыгранных матчей тура — знаменатель для инсайтов про точность
        # (идеальный/провальный тур по попаданиям), не выводится напрямую.
        total_round_matches = len(finished_match_ids)

        best_overall: tuple[str, str, int] | None = None  # (league_name, участник, points)
        weak_results: list[str] = []
        insight_lines: list[str] = []
        biggest_rise: tuple[int, str, str, int, int] | None = None  # (delta, name, league_label, before, after)

        # История прошлых туров этого этапа — нужна для статистических
        # аномалий (личный результат сильно отличается от собственной нормы
        # игрока, либо тур целиком аномален для всей лиги). Считаем один раз
        # заранее и переиспользуем: тот же снапшот нужен и для серий ниже.
        all_snapshots = await _round_snapshots(int(stage.round_min), round_number, leagues)
        player_round_history: dict[int, list[int]] = {}
        league_round_avg_history: dict[str, list[float]] = {code: [] for code, _ in LEAGUE_ORDER}
        for snap in all_snapshots:
            if snap.round_number >= round_number or not snap.rows:
                continue
            league_round_avg_history.setdefault(snap.league_code, []).append(
                statistics.mean(int(r["total"]) for r in snap.rows)
            )
            for r in snap.rows:
                player_round_history.setdefault(int(r["tg_user_id"]), []).append(int(r["total"]))

        for code, label in LEAGUE_ORDER:
            league = leagues.get(code)
            if league is None:
                continue
            round_rows, _meta = await build_active_stage_league_table(0, code, round_number=round_number)
            if not round_rows:
                continue

            before_rows = await _table_upto_round(session, rpl.id, season.id, stage, league, round_number - 1)
            after_rows = await _table_upto_round(session, rpl.id, season.id, stage, league, round_number)
            before_place = {r["tg_user_id"]: r["place"] for r in before_rows}
            after_place = {r["tg_user_id"]: r["place"] for r in after_rows}

            top = round_rows[0]
            if int(top["total"]) > 0 and (best_overall is None or int(top["total"]) > best_overall[2]):
                best_overall = (label, top["name"], int(top["total"]))

            # Тур целиком аномален для всей лиги (не для одного игрока) —
            # сравниваем средний балл ЭТОГО тура с историей средних баллов
            # прошлых туров той же лиги.
            hist = league_round_avg_history.get(code, [])
            if len(hist) >= MIN_SAMPLE_SIZE_FOR_ANOMALY:
                hist_mean = statistics.mean(hist)
                hist_stdev = statistics.pstdev(hist)
                if hist_stdev > 0:
                    round_avg_now = statistics.mean(int(r["total"]) for r in round_rows)
                    z = (round_avg_now - hist_mean) / hist_stdev
                    if z <= -ANOMALY_Z_THRESHOLD:
                        insight_lines.append(
                            f"🌀 Тур получился аномально тяжёлым для всех в «{label.lower()}»: "
                            f"средний результат {round_avg_now:.1f} очк. против обычных {hist_mean:.1f}."
                        )
                    elif z >= ANOMALY_Z_THRESHOLD:
                        insight_lines.append(
                            f"🌀 Тур получился аномально лёгким для всех в «{label.lower()}»: "
                            f"средний результат {round_avg_now:.1f} очк. против обычных {hist_mean:.1f}."
                        )

            for r in round_rows:
                uid = int(r["tg_user_id"])
                total = int(r["total"])
                exact = int(r.get("exact", 0))
                hits = int(r.get("hits", 0))
                pred_total = int(r.get("pred_total", 0))

                if total <= WEAK_ROUND_POINTS:
                    weak_results.append(f"{r['name']} ({label.lower()}) — {total} очк.")

                if total_round_matches > 0 and pred_total >= total_round_matches and exact >= total_round_matches:
                    insight_lines.append(f"💯 Идеальный тур: {r['name']} угадал все {total_round_matches} матчей тура точно.")
                elif pred_total > 0 and total == 0:
                    insight_lines.append(f"🥶 Нулевой тур: {r['name']} не набрал ни одного очка в этом туре.")

                if exact > ROUND_EXACT_HIGHLIGHT_THRESHOLD:
                    insight_lines.append(f"🎯 {r['name']} угадал {exact} точных счёта в туре — впечатляющий результат.")

                if total_round_matches >= MIN_ROUND_SIZE_FOR_ACCURACY_INSIGHTS:
                    if hits >= total_round_matches - 1:
                        insight_lines.append(
                            f"✅ {r['name']} угадал исход в {hits} из {total_round_matches} матчей тура — отличная точность."
                        )
                    elif hits <= 1:
                        insight_lines.append(
                            f"📉 {r['name']} угадал исход всего в {hits} из {total_round_matches} матчей тура."
                        )

                # Личная аномалия: результат тура сильно отличается от
                # СОБСТВЕННОЙ нормы игрока (а не от группы) — нужна история
                # хотя бы за несколько прошлых туров, иначе среднее шумит.
                own_history = player_round_history.get(uid, [])
                if len(own_history) >= MIN_SAMPLE_SIZE_FOR_ANOMALY:
                    own_mean = statistics.mean(own_history)
                    own_stdev = statistics.pstdev(own_history)
                    if own_stdev > 0:
                        pz = (total - own_mean) / own_stdev
                        if pz <= -ANOMALY_Z_THRESHOLD:
                            insight_lines.append(
                                f"🔻 {r['name']} сыграл тур намного слабее своего обычного уровня — "
                                f"{total} очк. против обычных {own_mean:.1f}."
                            )
                        elif pz >= ANOMALY_Z_THRESHOLD:
                            insight_lines.append(
                                f"⚡ {r['name']} выдал тур намного сильнее своего обычного уровня — "
                                f"{total} очк. против обычных {own_mean:.1f}."
                            )

                delta = before_place.get(uid), after_place.get(uid)
                if delta[0] is not None and delta[1] is not None:
                    rise = int(delta[0]) - int(delta[1])
                    if rise >= MIN_RISE_FOR_INSIGHT and (biggest_rise is None or rise > biggest_rise[0]):
                        biggest_rise = (rise, r["name"], label, int(delta[0]), int(delta[1]))

        if biggest_rise:
            rise, name, league_label, before_p, after_p = biggest_rise
            insight_lines.append(
                f"🚀 Самый большой рывок тура: {name} поднялся на {rise} мест "
                f"(с {before_p}-го на {after_p}-е, {league_label.lower()})."
            )

        insight_lines.extend(
            await _match_level_round_insights(session, int(rpl.id), int(season.id), round_number, finished_match_ids)
        )

        if best_overall:
            league_label, name, pts = best_overall
            lines.append(f"⭐ Лучший результат тура: {name} — {pts} очк. ({league_label.lower()})")

        # Рекорд сезона: сравниваем лучший результат ЭТОГО тура с лучшими
        # результатами всех предыдущих туров (без текущего) — если этот тур
        # оказался сильнее всех прошлых, отмечаем как новый рекорд.
        if best_overall and round_number > int(stage.round_min):
            prev_snapshots = await _round_snapshots(int(stage.round_min), round_number - 1, leagues)
            prev_best = max((int(s.rows[0]["total"]) for s in prev_snapshots if s.rows), default=0)
            if best_overall[2] > prev_best:
                lines.append(f"🏅 Это новый рекорд сезона по очкам за тур! (прошлый максимум — {prev_best})")

        if weak_results:
            lines.append("😬 Провал тура: " + "; ".join(weak_results))

        # Серии (лидерство, последнее место, стабильно высокие очки, серии
        # точных счётов) считаем для любого запрошенного тура, не только для
        # последнего сыгранного — при просмотре истории это тоже интересно.
        # all_snapshots уже посчитан выше (использовался для аномалий).
        insight_lines.extend(_detect_current_streaks(all_snapshots, round_number))

        if insight_lines:
            lines.append("")
            lines.append("Инсайты:")
            lines.extend(insight_lines)

        return "\n".join(lines).strip() + "\n"


async def build_tournament_digest_text() -> str | None:
    """Собирает текстовый дайджест по турниру РПЛ в целом на текущий момент
    (не обязательно конец сезона — по факту сыгранных на сейчас туров)."""
    async with SessionLocal() as session:
        season = await get_active_season(session)
        if season is None:
            return None
        stage = await get_active_stage(session, int(season.id))
        if stage is None:
            return None
        rpl = (await session.execute(select(Tournament).where(Tournament.code == "RPL"))).scalar_one_or_none()
        if rpl is None:
            return None

        league_rows = (
            await session.execute(select(League).where(League.season_id == season.id, League.is_active == 1))
        ).scalars().all()
        leagues = {str(l.code).upper(): l for l in league_rows}
        if not leagues:
            return None

        last_round = await _last_played_round(session, int(rpl.id), int(stage.round_min), int(stage.round_max))
        if last_round is None:
            return None

        lines: list[str] = [
            f"🏆 Итоги турнира РПЛ — {stage.name}, сыграно {last_round} из {int(stage.round_max)} туров",
            "",
        ]

        league_totals: dict[str, tuple[int, int]] = {}  # league_code -> (sum_points, participants)
        best_hit_rate: tuple[str, float, int] | None = None  # (name, hit_rate, pred_total)
        insights: list[str] = []

        for code, label in LEAGUE_ORDER:
            league = leagues.get(code)
            if league is None:
                continue
            rows, meta = await build_active_stage_league_table(0, code, round_number=None)
            if not rows:
                continue
            lines.append(f"{label}:")
            for r in rows:
                lines.append(
                    f"{_medal(int(r['place']))} {r['name']} — {int(r['total'])} очк. "
                    f"(🎯{int(r['exact'])} 📏{int(r['diff'])} ✅{int(r['outcome'])})"
                )
            lines.append("")

            total_sum = sum(int(r["total"]) for r in rows)
            league_totals[code] = (total_sum, len(rows))

            for r in rows:
                pred_total = int(r.get("pred_total", 0))
                if pred_total < 3:
                    continue
                hit_rate = float(r.get("hit_rate", 0.0))
                if best_hit_rate is None or hit_rate > best_hit_rate[1]:
                    best_hit_rate = (r["name"], hit_rate, pred_total)

            # Аномалия пропущенных матчей: участник, у которого пропущенных
            # матчей за сезон заметно больше, чем у остальных в его лиге —
            # это и статистический выброс, и реальный сигнал, что человек
            # давно не заходит делать прогнозы.
            missed_vals = [int(r.get("missed_matches", 0)) for r in rows]
            if len(missed_vals) >= MIN_SAMPLE_SIZE_FOR_ANOMALY:
                mm_mean = statistics.mean(missed_vals)
                mm_stdev = statistics.pstdev(missed_vals)
                if mm_stdev > 0:
                    for r in rows:
                        missed = int(r.get("missed_matches", 0))
                        if missed < 3:
                            continue
                        z = (missed - mm_mean) / mm_stdev
                        if z >= ANOMALY_Z_THRESHOLD:
                            insights.append(
                                f"⛔ {r['name']} пропустил заметно больше матчей, чем остальные в «{label.lower()}», "
                                f"— {missed} против среднего {mm_mean:.1f}."
                            )

        if len(league_totals) == 2 and "HIGH" in league_totals and "LOW" in league_totals:
            high_sum, high_n = league_totals["HIGH"]
            low_sum, low_n = league_totals["LOW"]
            high_avg = round(high_sum / high_n, 1) if high_n else 0.0
            low_avg = round(low_sum / low_n, 1) if low_n else 0.0
            lines.append(
                "📊 Сравнение лиг: Высшая лига — "
                f"{high_sum} очк. в сумме (в среднем {high_avg} на участника), "
                f"Низшая — {low_sum} очк. в сумме (в среднем {low_avg} на участника)."
            )
            lines.append("")

        if best_hit_rate:
            name, hit_rate, pred_total = best_hit_rate
            lines.append(f"🎯 Точнее всех прогнозирует: {name} — {hit_rate:.0f}% попаданий ({pred_total} прогнозов).")

        # Рекорды сезона: лучший и худший результат тура за всё сыгранное время.
        snapshots = await _round_snapshots(int(stage.round_min), last_round, leagues)
        best_round: tuple[int, str, str, int] | None = None  # (round, league, name, points)
        worst_round: tuple[int, str, str, int] | None = None
        for snap in snapshots:
            if not snap.rows:
                continue
            top = snap.rows[0]
            if best_round is None or int(top["total"]) > best_round[3]:
                best_round = (snap.round_number, snap.league_name, top["name"], int(top["total"]))
            bottom = snap.rows[-1]
            if worst_round is None or int(bottom["total"]) < worst_round[3]:
                worst_round = (snap.round_number, snap.league_name, bottom["name"], int(bottom["total"]))

        if best_round or worst_round:
            lines.append("")
            lines.append("🏅 Рекорды сезона:")
            if best_round:
                rn, league_name, name, pts = best_round
                lines.append(f"Лучший тур: {name} ({league_name.lower()}) — {pts} очк. в туре {rn}")
            if worst_round:
                rn, league_name, name, pts = worst_round
                lines.append(f"Худший тур: {name} ({league_name.lower()}) — {pts} очк. в туре {rn}")

        insights.extend(_detect_current_streaks(snapshots, last_round))
        if insights:
            lines.append("")
            lines.append("Инсайты:")
            lines.extend(insights)

        return "\n".join(lines).strip() + "\n"
