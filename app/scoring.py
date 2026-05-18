from dataclasses import dataclass


@dataclass(frozen=True)
class ScoreResult:
    points: int
    category: str  # "exact" | "diff" | "outcome" | "none"


def _sign(x: int) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def calculate_points(pred_home: int, pred_away: int, real_home: int, real_away: int) -> ScoreResult:
    """
    Правила:
    - точный счёт: 4
    - угадан исход + разница (включая ничью): 2
    - угадан исход без разницы: 1
    - иначе: 0
    """

    # 1) Точный счёт
    if pred_home == real_home and pred_away == real_away:
        return ScoreResult(points=4, category="exact")

    pred_diff = pred_home - pred_away
    real_diff = real_home - real_away

    # 2) Исход + разница (включая ничью)
    # Разница совпала, и знак разницы совпал (в ничьей оба 0)
    if pred_diff == real_diff and _sign(pred_diff) == _sign(real_diff):
        return ScoreResult(points=2, category="diff")

    # 3) Только исход
    if _sign(pred_diff) == _sign(real_diff):
        return ScoreResult(points=1, category="outcome")

    # 4) Не угадал
    return ScoreResult(points=0, category="none")


def get_stage_points_multiplier(tournament_code: str | None, round_number: int | None) -> int:
    """
    Коэффициент стадии для начисления очков за матч.

    Сейчас применяется для ЧМ 2026:
    - Туры 1-3, 1/16, 1/8: x1
    - 1/4, 1/2: x2
    - Матч за 3-е место, финал: x3
    """
    code = (tournament_code or "").strip().upper()
    rn = int(round_number or 0)
    if code != "WC2026":
        return 1
    if rn in (6, 7):
        return 2
    if rn in (8, 9):
        return 3
    return 1
