from collections import defaultdict
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Match, Point, Prediction, User, UserTournament


MIN_PREDICTIONS_FOR_RATE = 5


def _build_name_map(users: list[User], user_tournament_rows: list[UserTournament]) -> dict[int, str]:
    tournament_names = {u.tg_user_id: u.display_name for u in user_tournament_rows if u.display_name}
    names: dict[int, str] = {}
    for u in users:
        if u.tg_user_id in tournament_names:
            names[u.tg_user_id] = tournament_names[u.tg_user_id]
        elif u.display_name:
            names[u.tg_user_id] = u.display_name
        elif u.username:
            names[u.tg_user_id] = f"@{u.username}"
        elif u.full_name:
            names[u.tg_user_id] = u.full_name
        else:
            names[u.tg_user_id] = str(u.tg_user_id)
    return names


def _format_names_with_optional_counts(names: dict[int, str], counts: dict[int, int]) -> str:
    if not counts:
        return "нет данных"
    rows = sorted(counts.items(), key=lambda kv: (-kv[1], names.get(kv[0], str(kv[0])).lower()))
    parts: list[str] = []
    for tg_id, cnt in rows:
        name = names.get(tg_id, str(tg_id))
        if cnt > 1:
            parts.append(f"{name} ({cnt})")
        else:
            parts.append(name)
    return ", ".join(parts)


def _format_best_metric(names: dict[int, str], per_user: dict[int, dict], key: str) -> str:
    if not per_user:
        return "нет данных"
    max_val = max(int(v.get(key, 0)) for v in per_user.values())
    winners = [uid for uid, v in per_user.items() if int(v.get(key, 0)) == max_val]
    winners_sorted = sorted(winners, key=lambda uid: names.get(uid, str(uid)).lower())
    return f"{', '.join(names.get(uid, str(uid)) for uid in winners_sorted)} — {max_val}"


def _format_rate_metric(
    names: dict[int, str],
    rows: list[tuple[int, int, int, int]],  # (tg_user_id, hits, total, pct)
    pick_max: bool,
) -> str:
    if not rows:
        return "нет данных"
    target = max(r[3] for r in rows) if pick_max else min(r[3] for r in rows)
    picked = [r for r in rows if r[3] == target]
    picked.sort(key=lambda r: names.get(r[0], str(r[0])).lower())
    return ", ".join(f"{names.get(uid, str(uid))} — {pct}% ({hits}/{total})" for uid, hits, total, pct in picked)


async def build_stats_text(tournament_id: int | None = None) -> str:
    async with SessionLocal() as session:
        res_users = await session.execute(select(User))
        users = res_users.scalars().all()
        user_tournament_rows = []
        if tournament_id is not None:
            ut_q = await session.execute(select(UserTournament).where(UserTournament.tournament_id == tournament_id))
            user_tournament_rows = ut_q.scalars().all()

        points_q = select(Point)
        if tournament_id is not None:
            points_q = (
                select(Point)
                .join(Match, Match.id == Point.match_id)
                .where(Match.tournament_id == tournament_id)
            )
        res_points = await session.execute(points_q)
        points_rows = res_points.scalars().all()
        matches_q = select(Match.id, Match.round_number, Match.home_score, Match.away_score)
        preds_q = select(Prediction.tg_user_id, Prediction.match_id)
        if tournament_id is not None:
            matches_q = matches_q.where(Match.tournament_id == tournament_id)
            preds_q = (
                select(Prediction.tg_user_id, Prediction.match_id)
                .join(Match, Match.id == Prediction.match_id)
                .where(Match.tournament_id == tournament_id)
            )
        matches_rows = (await session.execute(matches_q)).all()
        preds_rows = (await session.execute(preds_q)).all()

    names = _build_name_map(users, user_tournament_rows)

    per_user: dict[int, dict] = defaultdict(lambda: {"exact": 0, "diff": 0, "outcome": 0, "none": 0})
    points_by_user_match: dict[tuple[int, int], str] = {}
    points_by_user_round: dict[tuple[int, int], int] = defaultdict(int)
    match_round_map: dict[int, int] = {}
    round_totals: dict[int, dict[str, int]] = defaultdict(lambda: {"total": 0, "played": 0})

    for match_id, round_number, home_score, away_score in matches_rows:
        mid = int(match_id)
        rnd = int(round_number)
        match_round_map[mid] = rnd
        round_totals[rnd]["total"] += 1
        if home_score is not None and away_score is not None:
            round_totals[rnd]["played"] += 1

    for r in points_rows:
        uid = int(r.tg_user_id)
        cat = (r.category or "").strip().lower()
        if cat in ("exact", "diff", "outcome", "none"):
            per_user[uid][cat] += 1
        else:
            per_user[uid]["none"] += 1
        points_by_user_match[(uid, int(r.match_id))] = cat
        rnd = match_round_map.get(int(r.match_id))
        if rnd is not None:
            points_by_user_round[(uid, rnd)] += int(r.points or 0)

    if not per_user and not preds_rows:
        return (
            "Пока нет статистики по очкам.\n"
            "Как только появятся результаты матчей, таблица здесь сразу оживёт."
        )

    completed_rounds = [rnd for rnd, v in round_totals.items() if v["total"] > 0 and v["total"] == v["played"]]

    round_users: dict[int, set[int]] = defaultdict(set)
    resolved_pred_total_by_user: dict[int, int] = defaultdict(int)
    hits_by_user: dict[int, int] = defaultdict(int)

    for uid_raw, match_id_raw in preds_rows:
        uid = int(uid_raw)
        mid = int(match_id_raw)
        rnd = match_round_map.get(mid)
        if rnd is None:
            continue
        round_users[rnd].add(uid)
        rv = round_totals.get(rnd)
        if not rv or rv["played"] != rv["total"]:
            continue
        resolved_pred_total_by_user[uid] += 1
        cat = points_by_user_match.get((uid, mid), "none")
        if cat in ("exact", "diff", "outcome"):
            hits_by_user[uid] += 1

    best_in_round_count: dict[int, int] = defaultdict(int)
    worst_in_round_count: dict[int, int] = defaultdict(int)
    for rnd in completed_rounds:
        users_in_round = sorted(round_users.get(rnd, set()))
        if not users_in_round:
            continue
        scores = {uid: int(points_by_user_round.get((uid, rnd), 0)) for uid in users_in_round}
        best = max(scores.values())
        worst = min(scores.values())
        for uid, sc in scores.items():
            if sc == best:
                best_in_round_count[uid] += 1
            if sc == worst:
                worst_in_round_count[uid] += 1

    rate_rows: list[tuple[int, int, int, int]] = []
    for uid, total in resolved_pred_total_by_user.items():
        if int(total) < MIN_PREDICTIONS_FOR_RATE:
            continue
        hits = int(hits_by_user.get(uid, 0))
        pct = round(hits * 100 / total) if total > 0 else 0
        rate_rows.append((uid, hits, int(total), int(pct)))

    lines = ["📊 Статистика сезона:"]
    lines.append(f"Лучший в туре: {_format_names_with_optional_counts(names, best_in_round_count)}")
    lines.append(f"Худший в туре: {_format_names_with_optional_counts(names, worst_in_round_count)}")
    lines.append(f"Лучший по точным счётам: {_format_best_metric(names, per_user, 'exact')}")
    lines.append(f"Лучший по разнице: {_format_best_metric(names, per_user, 'diff')}")
    lines.append(f"Лучший по исходам: {_format_best_metric(names, per_user, 'outcome')}")
    lines.append(f"Лучший процент угаданных матчей: {_format_rate_metric(names, rate_rows, pick_max=True)}")
    lines.append(f"Худший процент угаданных матчей: {_format_rate_metric(names, rate_rows, pick_max=False)}")
    lines.append(f"Порог для процентов: минимум {MIN_PREDICTIONS_FOR_RATE} прогнозов на завершённые матчи.")
    return "\n".join(lines)
