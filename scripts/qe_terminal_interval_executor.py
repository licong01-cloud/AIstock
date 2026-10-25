"""Instance-scoped final physical bar interval for the approved 125-day study."""
from __future__ import annotations

import pandas as pd
from qlib.backtest.executor import NestedExecutor, SimulatorExecutor
from qlib.backtest.utils import TradeCalendarManager


class BoundedTerminalCalendar(TradeCalendarManager):
    terminal_date = "2026-09-30"

    def get_step_time(self, trade_step=None, shift=0):
        step = self.trade_step if trade_step is None else trade_step
        index = self.start_index + step - shift
        if index < 0 or index >= len(self._calendar):
            raise ValueError("QE_TERMINAL_CALENDAR_OUT_OF_RANGE")
        start = pd.Timestamp(self._calendar[index])
        if start.date() > pd.Timestamp(self.terminal_date).date():
            raise ValueError("QE_TERMINAL_CALENDAR_LOOKAHEAD")
        if index < len(self._calendar) - 1:
            return super().get_step_time(trade_step, shift)
        if str(start.date()) != self.terminal_date or shift != 0:
            raise ValueError("QE_TERMINAL_CALENDAR_NOT_APPROVED_BAR")
        if self.freq in {"day", "1day"} and start == start.normalize():
            return start, start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        if self.freq in {"min", "1min"} and start.strftime("%H:%M:%S") == "15:00:00":
            return start, start + pd.Timedelta(minutes=1) - pd.Timedelta(seconds=1)
        raise ValueError("QE_TERMINAL_CALENDAR_FREQUENCY_INVALID")


class _TerminalExecutorMixin:
    def reset(self, *args, **kwargs):
        result = super().reset(*args, **kwargs)
        if self.level_infra.has("trade_calendar"):
            original = self.level_infra.get("trade_calendar")
            if not isinstance(original, BoundedTerminalCalendar):
                # Preserve every bit of Qlib's located/reset calendar state.
                # Only this executor's calendar instance is replaced.
                bounded = BoundedTerminalCalendar.__new__(BoundedTerminalCalendar)
                bounded.__dict__.update(original.__dict__)
                self.level_infra.reset_infra(trade_calendar=bounded)
        return result


class BoundedTerminalNestedExecutor(_TerminalExecutorMixin, NestedExecutor):
    pass


class BoundedTerminalSimulatorExecutor(_TerminalExecutorMixin, SimulatorExecutor):
    pass
