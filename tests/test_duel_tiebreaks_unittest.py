import unittest

from app.duels import (
    _elo_delta,
    combined_elo_multiplier_bp,
    duel_outcome_by_prediction_quality,
    duel_quality_multiplier_bp,
)


class DuelTiebreaksTest(unittest.TestCase):
    def test_regular_points_stay_primary(self):
        outcome, challenger_score, opponent_score = duel_outcome_by_prediction_quality(
            challenger_points=1,
            opponent_points=0,
            challenger_pred_home=1,
            challenger_pred_away=0,
            opponent_pred_home=2,
            opponent_pred_away=2,
            real_home=3,
            real_away=0,
        )

        self.assertEqual(outcome, "challenger_win")
        self.assertEqual((challenger_score, opponent_score), (1.0, 0.0))

    def test_score_closeness_breaks_equal_points(self):
        outcome, challenger_score, opponent_score = duel_outcome_by_prediction_quality(
            challenger_points=1,
            opponent_points=1,
            challenger_pred_home=1,
            challenger_pred_away=1,
            opponent_pred_home=7,
            opponent_pred_away=7,
            real_home=0,
            real_away=0,
        )

        self.assertEqual(outcome, "challenger_win")
        self.assertEqual((challenger_score, opponent_score), (1.0, 0.0))

    def test_opponent_can_win_by_goal_difference_closeness(self):
        outcome, challenger_score, opponent_score = duel_outcome_by_prediction_quality(
            challenger_points=1,
            opponent_points=1,
            challenger_pred_home=1,
            challenger_pred_away=0,
            opponent_pred_home=2,
            opponent_pred_away=0,
            real_home=3,
            real_away=0,
        )

        self.assertEqual(outcome, "opponent_win")
        self.assertEqual((challenger_score, opponent_score), (0.0, 1.0))

    def test_goal_difference_closeness_is_checked_before_score_closeness(self):
        outcome, challenger_score, opponent_score = duel_outcome_by_prediction_quality(
            challenger_points=0,
            opponent_points=0,
            challenger_pred_home=1,
            challenger_pred_away=1,
            opponent_pred_home=2,
            opponent_pred_away=1,
            real_home=2,
            real_away=3,
        )

        self.assertEqual(outcome, "challenger_win")
        self.assertEqual((challenger_score, opponent_score), (1.0, 0.0))

    def test_score_closeness_breaks_equal_goal_difference_distance(self):
        outcome, challenger_score, opponent_score = duel_outcome_by_prediction_quality(
            challenger_points=0,
            opponent_points=0,
            challenger_pred_home=1,
            challenger_pred_away=2,
            opponent_pred_home=3,
            opponent_pred_away=2,
            real_home=2,
            real_away=1,
        )

        self.assertEqual(outcome, "opponent_win")
        self.assertEqual((challenger_score, opponent_score), (0.0, 1.0))

    def test_full_tie_stays_draw(self):
        outcome, challenger_score, opponent_score = duel_outcome_by_prediction_quality(
            challenger_points=2,
            opponent_points=2,
            challenger_pred_home=1,
            challenger_pred_away=0,
            opponent_pred_home=3,
            opponent_pred_away=2,
            real_home=2,
            real_away=1,
        )

        self.assertEqual(outcome, "draw")
        self.assertEqual((challenger_score, opponent_score), (0.5, 0.5))

    def test_exact_score_win_has_stronger_quality_multiplier(self):
        exact_win = duel_quality_multiplier_bp("challenger_win", 4, 0)
        diff_win = duel_quality_multiplier_bp("challenger_win", 2, 0)
        outcome_win = duel_quality_multiplier_bp("challenger_win", 1, 0)

        self.assertGreater(exact_win, diff_win)
        self.assertGreater(diff_win, outcome_win)

    def test_closeness_win_has_minimal_quality_multiplier(self):
        self.assertEqual(duel_quality_multiplier_bp("challenger_win", 1, 1), 50)

    def test_combined_multiplier_keeps_risk_and_quality(self):
        self.assertEqual(combined_elo_multiplier_bp(140, 125), 175)
        self.assertEqual(combined_elo_multiplier_bp(100, 50), 50)

    def test_quality_multiplier_changes_elo_delta_size(self):
        base_delta = _elo_delta(1000, 1000, 1.0, combined_elo_multiplier_bp(100, 100))
        exact_delta = _elo_delta(1000, 1000, 1.0, combined_elo_multiplier_bp(100, 125))
        closeness_delta = _elo_delta(1000, 1000, 1.0, combined_elo_multiplier_bp(100, 50))

        self.assertGreater(exact_delta, base_delta)
        self.assertLess(closeness_delta, base_delta)

    def test_rating_gap_still_changes_elo_delta_size(self):
        underdog_win = _elo_delta(900, 1100, 1.0, combined_elo_multiplier_bp(100, 100))
        favorite_win = _elo_delta(1100, 900, 1.0, combined_elo_multiplier_bp(100, 100))

        self.assertGreater(underdog_win, favorite_win)


if __name__ == "__main__":
    unittest.main()
