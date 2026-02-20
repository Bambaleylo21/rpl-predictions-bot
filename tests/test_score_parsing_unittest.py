import unittest

from app.handlers_user import normalize_score, parse_score


class TestScoreParsing(unittest.TestCase):
    def test_colon_format(self):
        self.assertEqual(parse_score(normalize_score("2:1")), (2, 1))

    def test_dash_format(self):
        self.assertEqual(parse_score(normalize_score("2-1")), (2, 1))

    def test_spaces_are_ok(self):
        self.assertEqual(parse_score(normalize_score("  3-0  ")), (3, 0))

    def test_invalid_text(self):
        self.assertIsNone(parse_score(normalize_score("abc")))

    def test_invalid_separator(self):
        self.assertIsNone(parse_score(normalize_score("2/1")))


if __name__ == "__main__":
    unittest.main()
