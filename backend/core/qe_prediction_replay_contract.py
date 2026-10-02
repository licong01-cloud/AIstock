"""Neutral control-plane contract for QE prediction replay identity."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any


QE_PREDICTION_REPLAY_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def normalize_prediction_replay_contract(
    value: Mapping[str, Any] | None,
    *,
    context: str = "prediction_replay",
) -> dict[str, Any]:
    """Validate and normalize the shared prediction replay identity."""

    source = dict(value or {})
    enabled_value = source.get("prediction_replay", False)
    if not isinstance(enabled_value, bool):
        raise ValueError(f"{context}: prediction_replay must be a boolean")
    enabled = enabled_value
    source_task_value = source.get("prediction_source_task_id")
    source_task_id = source_task_value.strip() if isinstance(source_task_value, str) else ""
    source_loop_value = source.get("prediction_source_loop_index")
    expected_sha256_value = source.get("prediction_source_sha256")
    expected_sha256 = expected_sha256_value.strip().lower() if isinstance(expected_sha256_value, str) else ""

    if enabled and source.get("backtest_only") is True:
        raise ValueError(f"{context}: prediction_replay and backtest_only are mutually exclusive")
    if not enabled:
        if any(
            raw_value not in (None, "")
            for raw_value in (
                source_task_value,
                source_loop_value,
                expected_sha256_value,
            )
        ):
            raise ValueError(f"{context}: prediction source fields require prediction_replay=true")
        return {
            "prediction_replay": False,
            "prediction_source_task_id": None,
            "prediction_source_loop_index": None,
            "prediction_source_sha256": None,
        }

    if not source_task_id:
        raise ValueError(f"{context}: prediction_source_task_id must be a non-empty string")
    if isinstance(source_loop_value, bool) or source_loop_value in (None, ""):
        raise ValueError(f"{context}: prediction_source_loop_index is required")
    if isinstance(source_loop_value, int):
        source_loop_index = source_loop_value
    elif isinstance(source_loop_value, str) and re.fullmatch(r"[0-9]+", source_loop_value.strip()):
        source_loop_index = int(source_loop_value)
    else:
        raise ValueError(f"{context}: prediction_source_loop_index must be an integer")
    if source_loop_index < 1:
        raise ValueError(f"{context}: prediction_source_loop_index must be >= 1")
    if expected_sha256_value not in (None, "") and not isinstance(expected_sha256_value, str):
        raise ValueError(f"{context}: prediction_source_sha256 must be a hexadecimal string")
    if expected_sha256 and not QE_PREDICTION_REPLAY_SHA256_RE.fullmatch(expected_sha256):
        raise ValueError(f"{context}: prediction_source_sha256 must be 64 hex characters")
    return {
        "prediction_replay": True,
        "prediction_source_task_id": source_task_id,
        "prediction_source_loop_index": source_loop_index,
        "prediction_source_sha256": expected_sha256 or None,
    }
