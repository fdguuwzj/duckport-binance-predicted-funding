from datetime import datetime, timezone
from decimal import Decimal
import unittest

import pandas as pd

from binance_predicted_funding.model import parse_premium_index, plan_snapshot


class ParseTests(unittest.TestCase):
    def test_keeps_one_cross_section_and_drops_dated_futures(self):
        source = int(datetime(2026, 9, 30, 4, 1, 20, tzinfo=timezone.utc).timestamp() * 1000)
        older = source - 60_000
        frame = parse_premium_index(
            [
                {"symbol": "ETHUSDC", "lastFundingRate": "-0.00010000", "interestRate": "0.00010000", "nextFundingTime": source + 3_600_000, "time": source},
                {"symbol": "BTCUSDT", "lastFundingRate": "0.00004244", "interestRate": "0.00010000", "nextFundingTime": source + 3_600_000, "time": source},
                {"symbol": "BTCUSDT_260327", "lastFundingRate": "0.0001", "nextFundingTime": source, "time": source},
                {"symbol": "OLDUSDT", "lastFundingRate": "0.01", "nextFundingTime": older, "time": older},
            ]
        )
        self.assertEqual(list(frame["symbol"]), ["BTCUSDT", "ETHUSDC"])
        self.assertEqual(frame.iloc[0]["open_time"], pd.Timestamp("2026-09-30 04:01:00", tz="UTC"))
        self.assertEqual(frame.iloc[0]["funding_rate"], Decimal("0.00004244"))

    def test_rejects_a_single_object_payload(self):
        with self.assertRaises(ValueError):
            parse_premium_index({"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "nextFundingTime": 1, "time": 60_000})


class PlanTests(unittest.TestCase):
    def test_waits_until_the_delay_and_does_not_replay_gaps(self):
        action, when = plan_snapshot(datetime(2026, 1, 1, 10, 0, 1, tzinfo=timezone.utc), None, 2)
        self.assertEqual(action, "sleep")
        self.assertEqual(when, pd.Timestamp("2026-01-01 10:00:02", tz="UTC"))

        action, bar = plan_snapshot(
            datetime(2026, 1, 1, 10, 5, 10, tzinfo=timezone.utc),
            pd.Timestamp("2026-01-01 10:01:00", tz="UTC"),
            2,
        )
        self.assertEqual(action, "capture")
        self.assertEqual(bar, pd.Timestamp("2026-01-01 10:05:00", tz="UTC"))


if __name__ == "__main__":
    unittest.main()
