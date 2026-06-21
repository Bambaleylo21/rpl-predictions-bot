import unittest

from app.duels import duel_outcome_by_prediction_quality


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

    def test_opponent_can_win_by_score_closeness(self):
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

    def test_goal_difference_closeness_breaks_equal_score_distance(self):
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


if __name__ == "__main__":
    unittest.main()
