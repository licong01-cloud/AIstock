from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import AutonomousEvolutionState, EvolutionVerdict, StopDecision


def _as_int(payload: dict[str, Any], key: str, default: int) -> int:
    value = payload.get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc


def _as_float(payload: dict[str, Any], key: str, default: float) -> float:
    value = payload.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be numeric") from exc


def evaluate_stop_conditions(state: AutonomousEvolutionState, verdict: EvolutionVerdict) -> StopDecision:
    if verdict.failure or verdict.data_gap:
        return StopDecision(True, "failed", verdict.reason or "verdict_failure_or_data_gap", "failure_or_data_gap")
    if state.last_observation and (state.last_observation.failure or state.last_observation.data_gap):
        return StopDecision(True, "failed", state.last_observation.failure_reason or "loop_observation_failure_or_data_gap", "failure_or_data_gap")
    conditions = state.stop_conditions
    if verdict.target_reached:
        return StopDecision(True, "stopped_target", verdict.reason or "target reached", "target_reached")
    metric_name = conditions.get("target_metric")
    if metric_name:
        metric_value = verdict.metrics.get(str(metric_name))
        threshold = conditions.get("target_threshold")
        if metric_value is not None and threshold is not None and float(metric_value) >= float(threshold):
            return StopDecision(True, "stopped_target", f"{metric_name} reached target", "target_reached")
    no_improve_limit = _as_int(conditions, "max_no_improve_rounds", 0)
    if no_improve_limit > 0 and state.consecutive_no_improve >= no_improve_limit:
        return StopDecision(True, "stopped_no_improve", f"{state.consecutive_no_improve} consecutive rounds without SOTA improvement", "no_improve")
    failure_limit = _as_int(conditions, "max_consecutive_failures", 0)
    if failure_limit > 0 and state.consecutive_failures >= failure_limit:
        return StopDecision(True, "failed", f"{state.consecutive_failures} consecutive failures", "consecutive_failures")
    return StopDecision.continue_running()


def evaluate_budget(state: AutonomousEvolutionState, *, now: datetime) -> StopDecision:
    budget = state.budget
    max_loops = _as_int(budget, "max_loops", 0)
    if max_loops > 0 and state.loops_completed >= max_loops:
        return StopDecision(True, "stopped_budget", f"max_loops reached: {state.loops_completed}/{max_loops}", "budget_max_loops")
    max_elapsed = _as_float(budget, "max_total_seconds", 0.0)
    elapsed = state.budget_usage(now).elapsed_seconds
    if max_elapsed > 0 and elapsed >= max_elapsed:
        return StopDecision(True, "stopped_budget", f"max_total_seconds reached: {elapsed:.3f}/{max_elapsed:.3f}", "budget_max_elapsed")
    max_gpu = _as_float(budget, "max_gpu_occupancy_pct", 0.0)
    if max_gpu > 0 and state.max_gpu_occupancy_pct is not None and state.max_gpu_occupancy_pct >= max_gpu:
        return StopDecision(True, "stopped_budget", f"max_gpu_occupancy_pct reached: {state.max_gpu_occupancy_pct:.3f}/{max_gpu:.3f}", "budget_max_gpu")
    return StopDecision.continue_running()
