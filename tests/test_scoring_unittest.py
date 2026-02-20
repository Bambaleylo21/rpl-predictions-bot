import unittest

from app.scoring import calculate_points


class TestScoring(unittest.TestCase):
    def test_exact_score(self):
        result = calculate_points(2, 1, 2, 1)
        self.assertEqual(result.points, 4)
        self.assertEqual(result.category, "exact")

    def test_diff_and_outcome(self):
        result = calculate_points(3, 1, 2, 0)
        self.assertEqual(result.points, 2)
        self.assertEqual(result.category, "diff")

    def test_outcome_only(self):
        result = calculate_points(2, 0, 1, 0)
        self.assertEqual(result.points, 1)
        self.assertEqual(result.category, "outcome")

    def test_miss(self):
        result = calculate_points(0, 2, 1, 0)
        self.assertEqual(result.points, 0)
        self.assertEqual(result.category, "none")


if __name__ == "__main__":
    unittest.main()
