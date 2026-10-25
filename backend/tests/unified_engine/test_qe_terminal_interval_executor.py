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


def calendar(freq, dates, cls=BoundedTerminalCalendar):
    obj = cls.__new__(cls)
    obj.freq = freq
    obj._calendar = np.array([pd.Timestamp(d) for d in dates])
    obj.start_index = obj.trade_step = 0
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
