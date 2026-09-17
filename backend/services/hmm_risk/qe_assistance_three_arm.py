"""Exact request and result contracts for the approved QE HMM three-arm replay."""

from __future__ import annotations

import copy
import hashlib
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.hmm_risk.qe_assistance_adapter import (
    EXPECTED_MODEL_CONTRACT,
    EXPECTED_SOURCE_FILE_SHA256,
)
from backend.services.hmm_risk.qe_assistance_transport import BINDING_PARAM

SCHEMA_VERSION = "hmm_risk_qe_assistance_three_arm_request_v1"
RESULT_SCHEMA_VERSION = "hmm_risk_qe_assistance_three_arm_result_v1"
SOURCE_TASK_ID = "qe_20260502_131502_9b54"
SOURCE_LOOP_INDEX = 2
TARGET_NODE_ID = "rdagent-node1"
WINDOW_START = "2024-07-02"
WINDOW_END = "2026-03-31"
FORMAL_DATA_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": WINDOW_END,
    "backtest_end": WINDOW_END,
}
ARMS = ("no_hmm", "legacy_static_hmm", "new_pit_hmm")

STATUS_BENEFIT = "BENEFIT_VS_NO_HMM_OBSERVED"
STATUS_SUPERIOR = "SUPERIOR_TO_OLD_HMM_OBSERVED"
STATUS_NO_INCREMENTAL = "NO_INCREMENTAL_BENEFIT"
STATUS_NEW_UNAVAILABLE = "NEW_ASSISTANCE_NOT_AVAILABLE"
REASON_INPUT = "hmm_risk_qe_assistance_three_arm_input_invalid"
REASON_IDENTITY = "hmm_risk_qe_assistance_three_arm_identity_mismatch"
REASON_RESULT = "hmm_risk_qe_assistance_three_arm_result_incomplete"

_HMM_FIELDS = {
    "enable_sector_hmm",
    "hmm_config_json",
    "hmm_model_version_id",
    "hmm_signal_preset",
    "hmm_signal_presets",
    "sector_hmm_model_path",
}

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class QEAssistanceThreeArmError(RuntimeError):
    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


def _sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _config(value: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise QEAssistanceThreeArmError(REASON_INPUT, f"{label} config must be an object")
    normalized = copy.deepcopy(dict(value))
    if not normalized.get("factor_list") or not normalized.get("model_id") or not normalized.get("strategy_id"):
        raise QEAssistanceThreeArmError(REASON_INPUT, f"{label} config is incomplete")
    return normalized


def _without_hmm(value: Mapping[str, Any]) -> dict[str, Any]:
    cleaned = copy.deepcopy(dict(value))
    for container_name in ("model_params", "strategy_params"):
        container = dict(cleaned.get(container_name) or {})
        for field in _HMM_FIELDS:
            container.pop(field, None)
        cleaned[container_name] = container
    for field in _HMM_FIELDS:
        cleaned.pop(field, None)
    return cleaned


def _common_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    cleaned = _without_hmm(value)
    for field in (
        "action_type",
        "backtest_only",
        "execution_node_id",
        "label",
        "model_source_loop_index",
        "model_source_task_id",
        "node_id",
        "source_label_horizon",
    ):
        cleaned.pop(field, None)
    return cleaned


def _loop_from_source(source: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    strategy_params = copy.deepcopy(dict(source.get("strategy_params") or {}))
    model_params = copy.deepcopy(dict(source.get("model_params") or {}))
    loop = {
        "label": label,
        "factor_keys": list(source["factor_list"]),
        "disable_alpha158": bool(source.get("disable_alpha158", model_params.get("disable_alpha158", False))),
        "model_id": str(source["model_id"]),
        "model_params": model_params,
        "strategy_id": str(source["strategy_id"]),
        "strategy_params": strategy_params,
        "execution_algo": source.get("execution_algo"),
        "execution_algo_params": copy.deepcopy(dict(source.get("execution_algo_params") or {})),
        "filter_suspended_on_signal": bool(source.get("filter_suspended_on_signal", False)),
        "suspend_filter_strict": bool(source.get("suspend_filter_strict", True)),
        "stock_pool": source.get("stock_pool"),
        "label_type": source.get("label_type"),
        "label_horizon": int(source.get("label_horizon") or 10),
        # Prediction replay uses only the frozen pred.pkl and the raw test
        # label/price panel.  Persist the approved common window explicitly;
        # an empty historical source split would otherwise inherit today's
        # moving defaults and silently extend the comparison beyond 2026-03-31.
        "data_split": copy.deepcopy(FORMAL_DATA_SPLIT),
        "prediction_replay": True,
        "prediction_source_task_id": SOURCE_TASK_ID,
        "prediction_source_loop_index": SOURCE_LOOP_INDEX,
        "prediction_source_sha256": EXPECTED_SOURCE_FILE_SHA256,
        "node_id": TARGET_NODE_ID,
    }
    unfilled_handler = model_params.get("unfilled_handler") or strategy_params.get("unfilled_handler")
    if unfilled_handler:
        loop["unfilled_handler"] = unfilled_handler
        loop["unfilled_handler_params"] = {
            "backup_depth": model_params.get("unfilled_backup_depth")
            or strategy_params.get("unfilled_backup_depth")
        }
    return loop


def build_three_arm_request(
    *,
    no_hmm_source: Mapping[str, Any],
    legacy_hmm_source: Mapping[str, Any],
    artifact_binding: Mapping[str, Any],
    task_name: str,
) -> dict[str, Any]:
    """Build one frozen pending custom-evo request without reading outcomes."""

    no_hmm = _config(no_hmm_source, label="no-HMM")
    legacy = _config(legacy_hmm_source, label="legacy-HMM")
    if _common_identity(no_hmm) != _common_identity(legacy):
        raise QEAssistanceThreeArmError(
            REASON_IDENTITY,
            "source no-HMM and legacy-HMM loops differ outside approved HMM fields",
        )

    no_loop = _loop_from_source(_without_hmm(no_hmm), label="QE-HMM-3ARM:no-HMM")
    legacy_loop = _loop_from_source(legacy, label="QE-HMM-3ARM:legacy-static")
    legacy_model_params = dict(legacy.get("model_params") or {})
    legacy_loop.update(
        {
            "enable_sector_hmm": True,
            "hmm_model_version_id": legacy_model_params.get("hmm_model_version_id"),
            "hmm_signal_preset": legacy_model_params.get("hmm_signal_preset"),
        }
    )
    if not legacy_loop["hmm_model_version_id"]:
        raise QEAssistanceThreeArmError(REASON_IDENTITY, "legacy source lacks HMM model identity")

    new_loop = _loop_from_source(_without_hmm(no_hmm), label="QE-HMM-3ARM:new-PIT-v1.6")
    new_strategy = dict(new_loop["strategy_params"])
    new_strategy[BINDING_PARAM] = copy.deepcopy(dict(artifact_binding))
    new_loop["strategy_params"] = new_strategy
    new_loop.update(
        {
            "enable_sector_hmm": True,
            "hmm_model_version_id": EXPECTED_MODEL_CONTRACT,
            "hmm_signal_preset": "qe_assistance_v1_6",
        }
    )
    body = {
        "schema_version": SCHEMA_VERSION,
        "task_name": str(task_name).strip(),
        "target_desc": "Frozen QE HMM assistance three-arm historical prediction replay",
        "loops": [no_loop, legacy_loop, new_loop],
        "node_id": TARGET_NODE_ID,
        "node_parallelism": {TARGET_NODE_ID: 1},
        "phase_pipeline_enabled": False,
        "engine_mode": "unified",
        "auto_start": False,
        "consumer_id": "qe_mainline",
        "created_by_type": "agent",
        "created_by_name": "hmm_qe_assistance_three_arm",
        "purpose": "validation",
        "frozen_identity": {
            "source_task_id": SOURCE_TASK_ID,
            "source_loop_index": SOURCE_LOOP_INDEX,
            "source_prediction_sha256": EXPECTED_SOURCE_FILE_SHA256,
            "window_start": WINDOW_START,
            "window_end": WINDOW_END,
            "arms": list(ARMS),
        },
    }
    if not body["task_name"]:
        raise QEAssistanceThreeArmError(REASON_INPUT, "task_name is required")
    api_request = {key: value for key, value in body.items() if key not in {"schema_version", "frozen_identity"}}
    return {**body, "request_sha256": _sha256(body), "api_request": api_request}


def compare_three_arm_results(arm_results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Apply the approved D5/D6 terminal state machine without selecting a model."""

    rows = {str(row.get("arm")): dict(row) for row in arm_results if isinstance(row, Mapping)}
    if set(rows) != set(ARMS) or len(rows) != len(ARMS):
        raise QEAssistanceThreeArmError(REASON_RESULT, "exactly one result for each approved arm is required")
    normalized: dict[str, dict[str, Any]] = {}
    for arm in ARMS:
        row = rows[arm]
        if row.get("status") != "completed":
            failure_reason = str(row.get("failure_reason") or "").strip()
            if arm == "new_pit_hmm" and row.get("status") == "failed" and failure_reason:
                normalized[arm] = {
                    "status": "failed",
                    "failure_reason": failure_reason,
                }
                continue
            raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm} is not completed")
        metrics = row.get("metrics")
        hashes = row.get("artifact_hashes")
        if not isinstance(metrics, Mapping) or not isinstance(hashes, Mapping):
            raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm} lacks metrics or artifact hashes")
        required_metrics = (
            "cost_after_annualized_return",
            "cost_after_compounded_return",
            "information_ratio",
            "max_drawdown",
            "average_turnover",
            "total_cost_drag",
            "average_cost_drag",
        )
        numeric: dict[str, float] = {}
        for field in required_metrics:
            value = metrics.get(field)
            if isinstance(value, bool):
                raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm}.{field} is invalid")
            try:
                number = float(value)
            except (TypeError, ValueError) as exc:
                raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm}.{field} is invalid") from exc
            if not math.isfinite(number):
                raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm}.{field} is non-finite")
            numeric[field] = number
        required_hashes = ("holdings_sha256", "orders_sha256", "fills_sha256")
        if any(_SHA256.fullmatch(str(hashes.get(field) or "")) is None for field in required_hashes):
            raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm} lacks durable business hashes")
        changed_counts: dict[str, int] = {}
        for field in ("holdings_changed_count", "orders_changed_count", "fills_changed_count"):
            value = row.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm}.{field} is invalid")
            changed_counts[field] = value
        completed_dates = row.get("completed_dates")
        first_trade_date = str(row.get("first_trade_date") or "")
        last_trade_date = str(row.get("last_trade_date") or "")
        if (
            isinstance(completed_dates, bool)
            or not isinstance(completed_dates, int)
            or completed_dates != 423
            or first_trade_date != WINDOW_START
            or last_trade_date != WINDOW_END
        ):
            raise QEAssistanceThreeArmError(REASON_RESULT, f"arm {arm} has invalid completed-date identity")
        normalized[arm] = {
            "status": "completed",
            "metrics": numeric,
            "artifact_hashes": {field: hashes[field] for field in required_hashes},
            **changed_counts,
            "completed_dates": completed_dates,
            "first_trade_date": first_trade_date,
            "last_trade_date": last_trade_date,
        }

    if normalized["new_pit_hmm"]["status"] == "failed":
        body = {
            "schema_version": RESULT_SCHEMA_VERSION,
            "status": STATUS_NEW_UNAVAILABLE,
            "primary_metric": "common_window_cost_after_compounded_return",
            "arms": normalized,
            "delta_new_vs_no_hmm": None,
            "delta_new_vs_legacy_static_hmm": None,
            "delta_legacy_static_hmm_vs_no_hmm": (
                normalized["legacy_static_hmm"]["metrics"]["cost_after_compounded_return"]
                - normalized["no_hmm"]["metrics"]["cost_after_compounded_return"]
            ),
            "benefit_vs_no_hmm_observed": False,
            "superior_to_old_hmm_observed": False,
            "superior_to_old_hmm_observation_code": None,
            "automatic_model_selection_performed": False,
            "automatic_runtime_activation_performed": False,
        }
        return {**body, "result_sha256": _sha256(body)}

    new_return = normalized["new_pit_hmm"]["metrics"]["cost_after_compounded_return"]
    no_return = normalized["no_hmm"]["metrics"]["cost_after_compounded_return"]
    old_return = normalized["legacy_static_hmm"]["metrics"]["cost_after_compounded_return"]
    delta_no = new_return - no_return
    delta_old = new_return - old_return
    delta_old_no = old_return - no_return
    benefit_vs_no_hmm = delta_no > 1e-12
    superior_to_old_hmm = delta_old > 1e-12
    if benefit_vs_no_hmm:
        status = STATUS_BENEFIT
    else:
        status = STATUS_NO_INCREMENTAL
    body = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "status": status,
        "primary_metric": "common_window_cost_after_compounded_return",
        "arms": normalized,
        "delta_new_vs_no_hmm": delta_no,
        "delta_new_vs_legacy_static_hmm": delta_old,
        "delta_legacy_static_hmm_vs_no_hmm": delta_old_no,
        "numeric_ties": {
            "new_vs_no_hmm": abs(delta_no) <= 1e-12,
            "new_vs_legacy_static_hmm": abs(delta_old) <= 1e-12,
            "legacy_static_hmm_vs_no_hmm": abs(delta_old_no) <= 1e-12,
        },
        "benefit_vs_no_hmm_observed": benefit_vs_no_hmm,
        "superior_to_old_hmm_observed": superior_to_old_hmm,
        "superior_to_old_hmm_observation_code": STATUS_SUPERIOR if superior_to_old_hmm else None,
        "automatic_model_selection_performed": False,
        "automatic_runtime_activation_performed": False,
    }
    return {**body, "result_sha256": _sha256(body)}
