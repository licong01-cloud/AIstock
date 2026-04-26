"""Score-weighted strategy wrappers with explicit suspend_d signal filtering."""
from __future__ import annotations

try:
    from custom_strategy import ScoreWeightedTopkStrategy
except ImportError:
    from score_weighted_strategy import ScoreWeightedTopkStrategy
try:
    from custom_strategy import ScoreWeightedTopkStrategyV2
except ImportError:
    from score_weighted_strategy_v2 import ScoreWeightedTopkStrategyV2
from qe_suspend_filter import QESuspendFilter


class _SuspendFilterScoreWeightedMixin:
    """Filter the normalized daily score Series before score-weighted ranking."""

    def __init__(
        self,
        *args,
        filter_suspended_on_signal=False,
        suspend_filter_file=None,
        suspend_filter_strict=True,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._qe_suspend_filter = QESuspendFilter(
            enabled=filter_suspended_on_signal,
            suspend_filter_file=suspend_filter_file,
            strict=suspend_filter_strict,
        )
        self._qe_suspend_filter_trade_time = None

    def generate_trade_decision(self, execute_result=None):
        trade_step = self.trade_calendar.get_trade_step()
        trade_start_time, _trade_end_time = self.trade_calendar.get_step_time(trade_step)
        self._qe_suspend_filter_trade_time = trade_start_time
        return super().generate_trade_decision(execute_result)

    def _normalize_signal_scores(self, all_pred_scores, pred_end_time):
        scores = super()._normalize_signal_scores(all_pred_scores, pred_end_time)
        return self._qe_suspend_filter.filter_scores(
            scores,
            self._qe_suspend_filter_trade_time or pred_end_time,
        )


class SuspendFilterScoreWeightedTopkStrategy(
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategy,
):
    pass


class SuspendFilterScoreWeightedTopkStrategyV2(
    _SuspendFilterScoreWeightedMixin,
    ScoreWeightedTopkStrategyV2,
):
    pass
