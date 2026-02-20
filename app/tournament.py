ROUND_MIN = 19
ROUND_MAX = 30
ROUND_DEFAULT = ROUND_MIN


def is_tournament_round(round_number: int) -> bool:
    return ROUND_MIN <= round_number <= ROUND_MAX

