import unittest
from datetime import datetime, timedelta

from app.handlers_user import match_status_icon
from app.models import Match


class TestMatchStatusIcon(unittest.TestCase):
    def test_finished_match(self):
        now = datetime(2026, 2, 20, 12, 0)
        m = Match(
            round_number=1,
            home_team="A",
            away_team="B",
            kickoff_time=now - timedelta(hours=2),
            home_score=1,
            away_score=0,
        )
        self.assertEqual(match_status_icon(m, now), "✅")

    def test_locked_match(self):
        now = datetime(2026, 2, 20, 12, 0)
        m = Match(
            round_number=1,
            home_team="A",
            away_team="B",
            kickoff_time=now - timedelta(minutes=10),
            home_score=None,
            away_score=None,
        )
        self.assertEqual(match_status_icon(m, now), "🔒")

    def test_open_match(self):
        now = datetime(2026, 2, 20, 12, 0)
        m = Match(
            round_number=1,
            home_team="A",
            away_team="B",
            kickoff_time=now + timedelta(minutes=30),
            home_score=None,
            away_score=None,
        )
        self.assertEqual(match_status_icon(m, now), "🟢")


if __name__ == "__main__":
    unittest.main()
