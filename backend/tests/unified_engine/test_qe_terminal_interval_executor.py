"""Use installed Qlib, without changing provider calendars or global classes."""
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

try:
    from qlib.backtest.utils import TradeCalendarManager
except ModuleNotFoundError as exc:
    if exc.name != "qlib":
        raise
    raise unittest.SkipTest("Requires the QE worker Qlib environment") from exc
from qe_terminal_interval_executor import BoundedTerminalCalendar


def calendar(freq, dates, cls=BoundedTerminalCalendar, end="2026-09-30"):
    obj = cls.__new__(cls)
    obj.freq = freq
    obj._calendar = np.array([pd.Timestamp(d) for d in dates])
    obj.start_index = obj.trade_step = 0
    obj.end_time = pd.Timestamp(end) if end is not None else None
    return obj


def check_original_intervals_shift_and_terminal(freq, dates, end):
    bounded, original = calendar(freq, dates), calendar(freq, dates, TradeCalendarManager)
    for step in (0, 1):
        assert bounded.get_step_time(step) == original.get_step_time(step)
    assert bounded.get_step_time(2, shift=1) == original.get_step_time(2, shift=1)
    assert bounded.get_step_time(2) == (pd.Timestamp(dates[-1]), pd.Timestamp(end))
    with unittest.TestCase().assertRaises(IndexError):
        original.get_step_time(2)
    with unittest.TestCase().assertRaisesRegex(ValueError, "OUT_OF_RANGE"):
        bounded.get_step_time(3)
    with unittest.TestCase().assertRaisesRegex(ValueError, "OUT_OF_RANGE"):
        bounded.get_step_time(0, shift=1)


def check_no_fabricated_bar_or_other_frequency(freq, last):
    with unittest.TestCase().assertRaises(ValueError):
        calendar(freq, [last]).get_step_time()


class TerminalContractTests(unittest.TestCase):
    def test_intervals(self):
        for freq, dates, end in [("day", ["2026-09-28", "2026-09-29", "2026-09-30"], "2026-09-30 23:59:59"), ("1min", ["2026-09-30 14:58", "2026-09-30 14:59", "2026-09-30 15:00"], "2026-09-30 15:00:59")]:
            with self.subTest(freq=freq):
                check_original_intervals_shift_and_terminal(freq, dates, end)

    def test_rejected_boundaries(self):
        for freq, last in [("day", "2026-09-29"), ("5min", "2026-09-30 15:00"), ("1min", "2026-09-30 14:59"), ("day", "2026-10-01")]:
            with self.subTest(freq=freq, last=last):
                check_no_fabricated_bar_or_other_frequency(freq, last)

    def test_october_intervals_and_lookahead(self):
        dates = ["2026-09-30", "2026-10-08", "2026-10-29", "2026-10-30"]
        bounded = calendar("day", dates, end="2026-10-29")
        original = calendar("day", dates, TradeCalendarManager, end="2026-10-29")
        for step in range(3):
            self.assertEqual(bounded.get_step_time(step), original.get_step_time(step))
        self.assertEqual(bounded.get_step_time(3, shift=1), original.get_step_time(3, shift=1))
        with self.assertRaisesRegex(ValueError, "LOOKAHEAD"):
            bounded.get_step_time(3)
        with self.assertRaisesRegex(ValueError, "LOOKAHEAD"):
            bounded.get_step_time(2, shift=-1)

    def test_end_dates_are_instance_scoped(self):
        dates = ["2026-09-30", "2026-10-08", "2026-10-29", "2026-10-30"]
        september = calendar("day", dates)
        october = calendar("day", dates, end="2026-10-29")
        with self.assertRaisesRegex(ValueError, "LOOKAHEAD"):
            september.get_step_time(1)
        self.assertEqual(october.get_step_time(1)[0], pd.Timestamp("2026-10-08"))
        with self.assertRaisesRegex(ValueError, "LOOKAHEAD"):
            september.get_step_time(1)

    def test_october_physical_terminal_bar(self):
        for freq, last, end in [
            ("day", "2026-10-29", "2026-10-29 23:59:59"),
            ("1min", "2026-10-29 15:00:00", "2026-10-29 15:00:59"),
        ]:
            with self.subTest(freq=freq):
                bounded = calendar(freq, [last], end="2026-10-29")
                self.assertEqual(bounded.get_step_time(), (pd.Timestamp(last), pd.Timestamp(end)))

    def test_missing_end_is_rejected(self):
        for end in [None, pd.NaT]:
            with self.subTest(end=end), self.assertRaisesRegex(ValueError, "END_MISSING"):
                calendar("day", ["2026-10-29"], end=end).get_step_time()

    def test_partial_final_session_cannot_fabricate_close(self):
        for freq, last in [("1min", "2026-10-29 14:59"), ("5min", "2026-10-29 15:00")]:
            with self.subTest(freq=freq), self.assertRaisesRegex(ValueError, "FREQUENCY_INVALID"):
                calendar(freq, [last], end="2026-10-29").get_step_time()

    def test_only_unshifted_physical_terminal_can_be_extended(self):
        bounded = calendar("day", ["2026-10-29"], end="2026-10-29")
        with self.assertRaisesRegex(ValueError, "NOT_APPROVED_BAR"):
            bounded.get_step_time(1, shift=1)
