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