"""P2-4 untouched-holdout acceptance for the frozen P2-3C candidate.

The module is deliberately offline-only.  It never fits or selects a model.
It validates one immutable P2-3C candidate, applies its frozen parameters to
one pre-registered holdout, evaluates the approved product metrics and
coverage contract, and writes only the artifacts permitted by the resulting
availability state.
"""

from __future__ import annotations

import json
import math
import os
import platform
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.hmm_risk.market_relative_jump_spike import (
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    DEVELOPMENT_TRADING_DAYS,
    HOLDOUT_END,
    HOLDOUT_START,
    HOLDOUT_TRADING_DAYS,
    MARKET_FEATURES,
    RELATIVE_FEATURES,
    Preprocessor,
    causal_states,
    classify_coverage,
    freeze_quintiles,
    newey_west_t,
    prepare_component,
    risk_metrics,
)
from backend.services.hmm_risk.market_relative_ridge_candidate import (
    LEVEL_SPECS,
    MINIMUM_EXTREME_COUNT,
    P2_3C_FEATURES,
    P2_3C_FIXED_JUMP_PENALTY,
    P2_3C_MARKET_COMPONENT_SCHEMA_VERSION,
    P2_3C_REPORT_SCHEMA_VERSION,
    P2_3C_COMPONENT_SCHEMA_VERSION,
    RidgeFit,
    STATE_FRACTION,
    STATE_TIE_TOLERANCE,
    _condition_component,
    predict_scores,
    project_daily_states,
)
from backend.services.hmm_risk.state_model_set import canonical_json_bytes, canonical_sha256, sha256_bytes

CONTRACT_VERSION = "C-011-P2-4-D1-D6"
ALGORITHM_VERSION = "hmm_risk_market_conditioned_ridge_holdout_v1"
REQUEST_SCHEMA_VERSION = "hmm_risk_p2_4_holdout_request_v1"
CHILD_SCHEMA_VERSION = "hmm_risk_p2_4_holdout_child_v1"
ACCEPTANCE_SCHEMA_VERSION = "hmm_risk_p2_4_holdout_acceptance_v1"
MODEL_SCHEMA_VERSION = "hmm_risk_market_conditioned_ridge_model_v1"
READY_SCHEMA_VERSION = "hmm_risk_market_conditioned_ridge_ready_v1"

EXPECTED_CANDIDATE_SHA256 = "792d4f6ac6b313961eaf5017a0a3ea4a3ebf96ab8364f4ff8518c182a68d17e3"
EXPECTED_CANDIDATE_PRODUCER = "8ca1b98db922489f91814b5d51aae1ab9c59fbd0"
EXPECTED_DEVELOPMENT_REQUEST_SHA256 = "4807125d24a9c01596f923122079c6d70dd48ff39522d3755c6ab0ad09ec6336"
EXPECTED_DEVELOPMENT_REQUEST_IDENTITY_SHA256 = "7cf7f7a7dd6ecbf8b3f63bd820250202ed7737b45ea1da0af5c3aa743c3d20f4"
EXPECTED_UNIVERSE_KEY = "shsz_st_pit_qe_dataset_qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2"
EXPECTED_UNIVERSE_RULE_VERSION = "st_pub_next_trade_restore_active_l_v1"
EXPECTED_SECURITY_IDENTITY_MANIFEST_PATH = "backend/services/hmm_risk/manifests/security_source_identity_v1.json"
EXPECTED_SECURITY_IDENTITY_MANIFEST_SHA256 = "24e0070fd97e00e5021eafc295426144b5b2eb3f7d76d4828aab18fe6d21358f"
EXPECTED_PROVIDER_ABSENCE_MANIFEST_PATH = "backend/services/hmm_risk/manifests/provider_absence_v1.json"
EXPECTED_PROVIDER_ABSENCE_MANIFEST_SHA256 = "717b899cbc5cebfa41f9ffe9d4fe32055f033bc93d1712d5da6a983a6a93e886"
EXPECTED_SOURCE_START = "2022-01-01"
EXPECTED_CIRC_MV_HISTORY_START = "2020-07-30"
EXPECTED_BENCHMARK_TS_CODE = "000300.SH"
OUTCOME_TAIL_TRADING_DAYS = 20
HORIZONS = (5, 10, 20)
PRIMARY_HORIZON = 10
RISK_THRESHOLD = -0.05
METRIC_COVERAGE_RATIO = 0.80

REASON_CANDIDATE = "hmm_risk_p2_4_candidate_identity_mismatch"
REASON_REQUEST = "hmm_risk_p2_4_request_identity_mismatch"
REASON_SOURCE = "hmm_risk_p2_4_holdout_source_identity_mismatch"
REASON_PREFLIGHT = "hmm_risk_p2_4_holdout_preflight_failed"
REASON_CAUSAL_STATE = "hmm_risk_p2_4_causal_state_failed"
REASON_SCORE = "hmm_risk_p2_4_score_state_non_finite"
REASON_METRIC = "hmm_risk_p2_4_metric_unavailable"
REASON_COVERAGE = "hmm_risk_p2_4_coverage_contract_failed"
REASON_REPRESENTATIVENESS = "hmm_risk_p2_4_representativeness_failed"
REASON_REPRODUCIBILITY = "hmm_risk_p2_4_fresh_process_reproducibility_failed"
REASON_COLLISION = "hmm_risk_p2_4_output_collision"
REASON_READBACK = "hmm_risk_p2_4_readback_mismatch"
REASON_UNEXPECTED = "hmm_risk_p2_4_unknown_execution_failure"


def expected_holdout_source(*, outcome_tail_end: date) -> dict[str, Any]:
    return {
        "source_start": EXPECTED_SOURCE_START,
        "source_end": outcome_tail_end.isoformat(),
        "circ_mv_history_start": EXPECTED_CIRC_MV_HISTORY_START,
        "universe_key": EXPECTED_UNIVERSE_KEY,
        "universe_rule_version": EXPECTED_UNIVERSE_RULE_VERSION,
        "benchmark_ts_code": EXPECTED_BENCHMARK_TS_CODE,
        "security_identity_manifest_path": EXPECTED_SECURITY_IDENTITY_MANIFEST_PATH,
        "security_identity_manifest_sha256": EXPECTED_SECURITY_IDENTITY_MANIFEST_SHA256,
        "provider_absence_manifest_path": EXPECTED_PROVIDER_ABSENCE_MANIFEST_PATH,
        "provider_absence_manifest_sha256": EXPECTED_PROVIDER_ABSENCE_MANIFEST_SHA256,
    }


class HoldoutAcceptanceError(RuntimeError):
    """Typed fail-closed P2-4 error."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        stage: str,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.stage = stage
        self.evidence = dict(evidence or {})


@dataclass(frozen=True)
class FrozenCandidate:
    report: dict[str, Any]
    market: dict[str, Any]
    levels: dict[str, dict[str, Any]]


def _fail(
    reason_code: str,
    message: str,
    *,
    stage: str,
    evidence: Mapping[str, Any] | None = None,
) -> HoldoutAcceptanceError:
    return HoldoutAcceptanceError(reason_code, message, stage=stage, evidence=evidence)


def _require_sha256(value: Any, field: str, *, reason: str = REASON_REQUEST) -> str:
    text = str(value or "")
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise _fail(reason, f"{field} must be lowercase SHA-256", stage="preflight")
    return text


def _require_canonical_receipt(value: Mapping[str, Any], *, label: str, reason: str = REASON_CANDIDATE) -> None:
    receipt = _require_sha256(value.get("receipt_sha256"), f"{label}.receipt_sha256", reason=reason)
    body = {key: item for key, item in value.items() if key != "receipt_sha256"}
    if canonical_sha256(body) != receipt:
        raise _fail(reason, f"{label} receipt hash is invalid", stage="preflight")


def _reject_duplicate_keys(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load_json(path: Path, *, reason: str, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_non_finite_constant,
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise _fail(
            reason,
            f"{label} cannot be read canonically",
            stage="preflight",
            evidence={"exception_type": type(exc).__name__},
        ) from exc
    if not isinstance(value, dict):
        raise _fail(reason, f"{label} must be an object", stage="preflight")
    return value


def load_request(path: Path) -> dict[str, Any]:
    return _load_json(path, reason=REASON_REQUEST, label="P2-4 request")


def load_written_artifact(path: Path, *, label: str, reason: str = REASON_READBACK) -> dict[str, Any]:
    return _load_json(path, reason=reason, label=label)


def _component(report: Mapping[str, Any], *, component: str, phase: str) -> dict[str, Any]:
    matches = [
        item
        for item in report.get("components", [])
        if isinstance(item, dict) and item.get("component") == component and item.get("phase") == phase
    ]
    if len(matches) != 1:
        raise _fail(REASON_CANDIDATE, f"candidate {component}/{phase} component is not unique", stage="preflight")
    _require_canonical_receipt(matches[0], label=f"candidate {component}/{phase}")
    return dict(matches[0])


def load_frozen_candidate(path: Path, *, expected_sha256: str = EXPECTED_CANDIDATE_SHA256) -> FrozenCandidate:
    report = _load_json(path, reason=REASON_CANDIDATE, label="P2-3C candidate")
    report_hash = _require_sha256(report.get("report_sha256"), "candidate.report_sha256", reason=REASON_CANDIDATE)
    body = {key: item for key, item in report.items() if key != "report_sha256"}
    if report_hash != canonical_sha256(body) or report_hash != _require_sha256(
        expected_sha256, "expected_candidate_sha256", reason=REASON_CANDIDATE
    ):
        raise _fail(REASON_CANDIDATE, "candidate canonical hash is invalid", stage="preflight")
    expected = {
        "schema_version": P2_3C_REPORT_SCHEMA_VERSION,
        "status": "P2_3C_CANDIDATE_FROZEN_PENDING_P2_4_HOLDOUT_ACCEPTANCE",
        "producer_commit": EXPECTED_CANDIDATE_PRODUCER,
        "completed_fit_count": 36,
        "planned_fit_count": 36,
        "candidate_attempt_index": 3,
        "holdout_accessed": False,
        "product_acceptance_performed": False,
        "model_write": False,
        "ready_write": False,
        "database_write": False,
        "runtime_action": False,
    }
    if any(report.get(key) != value for key, value in expected.items()):
        raise _fail(REASON_CANDIDATE, "candidate status or side-effect boundary is invalid", stage="preflight")
    request_identity = report.get("request_identity")
    if (
        not isinstance(request_identity, dict)
        or report.get("request_identity_sha256") != EXPECTED_DEVELOPMENT_REQUEST_IDENTITY_SHA256
        or canonical_sha256(request_identity) != EXPECTED_DEVELOPMENT_REQUEST_IDENTITY_SHA256
    ):
        raise _fail(REASON_CANDIDATE, "candidate development request identity is invalid", stage="preflight")
    components = report.get("components")
    hashes = report.get("component_receipt_sha256s")
    if not isinstance(components, list) or len(components) != 6 or not isinstance(hashes, list) or len(hashes) != 6:
        raise _fail(REASON_CANDIDATE, "candidate component closure is incomplete", stage="preflight")
    for index, item in enumerate(components):
        if not isinstance(item, dict):
            raise _fail(REASON_CANDIDATE, "candidate component is invalid", stage="preflight")
        _require_canonical_receipt(item, label=f"candidate component {index}")
        if hashes[index] != item["receipt_sha256"]:
            raise _fail(REASON_CANDIDATE, "candidate component hash list is invalid", stage="preflight")
    market = _component(report, component="market", phase="final-development")
    levels = {level: _component(report, component=level, phase="final-development") for level in ("L1", "L2")}
    if market.get("schema_version") != P2_3C_MARKET_COMPONENT_SCHEMA_VERSION:
        raise _fail(REASON_CANDIDATE, "candidate market schema is invalid", stage="preflight")
    for level, item in levels.items():
        if (
            item.get("schema_version") != P2_3C_COMPONENT_SCHEMA_VERSION
            or item.get("level") != level
            or item.get("selected_alpha") != 100.0
            or item.get("canonical_sector_count") != LEVEL_SPECS[level]["expected_sector_count"]
        ):
            raise _fail(REASON_CANDIDATE, f"candidate {level} identity is invalid", stage="preflight")
    return FrozenCandidate(report=report, market=market, levels=levels)


def validate_static_request(request: Mapping[str, Any], candidate: FrozenCandidate) -> dict[str, Any]:
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION or request.get("contract_version") != CONTRACT_VERSION:
        raise _fail(REASON_REQUEST, "P2-4 request schema or contract is invalid", stage="preflight")
    request_hash = _require_sha256(request.get("request_sha256"), "request.request_sha256", reason=REASON_REQUEST)
    if canonical_sha256({key: value for key, value in request.items() if key != "request_sha256"}) != request_hash:
        raise _fail(REASON_REQUEST, "P2-4 request canonical hash is invalid", stage="preflight")
    if request.get("candidate_report_sha256") != candidate.report["report_sha256"]:
        raise _fail(REASON_REQUEST, "request candidate hash does not match the frozen authority", stage="preflight")
    if request.get("candidate_producer_commit") != EXPECTED_CANDIDATE_PRODUCER:
        raise _fail(REASON_REQUEST, "request candidate producer is invalid", stage="preflight")
    if request.get("development_request_sha256") != EXPECTED_DEVELOPMENT_REQUEST_SHA256:
        raise _fail(REASON_REQUEST, "request development identity is invalid", stage="preflight")
    holdout = request.get("holdout_source")
    if not isinstance(holdout, dict):
        raise _fail(REASON_REQUEST, "holdout_source is missing", stage="preflight")
    required_hashes = (
        "dataset_manifest_sha256",
        "mapping_manifest_sha256",
        "calendar_manifest_sha256",
        "feature_formula_sha256",
        "security_identity_manifest_sha256",
        "provider_absence_manifest_sha256",
        "constituents_sha256",
        "state_date_set_sha256",
        "outcome_tail_date_set_sha256",
    )
    for field in required_hashes:
        _require_sha256(holdout.get(field), f"holdout_source.{field}", reason=REASON_REQUEST)
    if (
        holdout.get("state_start") != HOLDOUT_START.isoformat()
        or holdout.get("state_end") != HOLDOUT_END.isoformat()
        or holdout.get("state_trading_day_count") != HOLDOUT_TRADING_DAYS
        or holdout.get("outcome_tail_trading_day_count") != OUTCOME_TAIL_TRADING_DAYS
    ):
        raise _fail(REASON_REQUEST, "holdout date boundary is invalid", stage="preflight")
    source = holdout.get("source")
    if not isinstance(source, dict) or source == candidate.report.get("request_identity", {}).get("source"):
        raise _fail(REASON_REQUEST, "holdout source must be explicit and separate from development", stage="preflight")
    expected_source_policy = {
        "source_start": EXPECTED_SOURCE_START,
        "source_end": holdout.get("outcome_tail_end"),
        "circ_mv_history_start": EXPECTED_CIRC_MV_HISTORY_START,
        "universe_key": EXPECTED_UNIVERSE_KEY,
        "universe_rule_version": EXPECTED_UNIVERSE_RULE_VERSION,
        "benchmark_ts_code": EXPECTED_BENCHMARK_TS_CODE,
        "security_identity_manifest_path": EXPECTED_SECURITY_IDENTITY_MANIFEST_PATH,
        "security_identity_manifest_sha256": EXPECTED_SECURITY_IDENTITY_MANIFEST_SHA256,
        "provider_absence_manifest_path": EXPECTED_PROVIDER_ABSENCE_MANIFEST_PATH,
        "provider_absence_manifest_sha256": EXPECTED_PROVIDER_ABSENCE_MANIFEST_SHA256,
    }
    if any(source.get(key) != value for key, value in expected_source_policy.items()):
        raise _fail(
            REASON_REQUEST, "holdout source policy differs from the frozen development authority", stage="preflight"
        )
    if holdout.get("feature_formula_sha256") != candidate.report.get("feature_formula_sha256"):
        raise _fail(REASON_REQUEST, "holdout feature formula differs from the frozen candidate", stage="preflight")
    outputs = request.get("artifact_outputs")
    output_fields = (
        "acceptance_output",
        "acceptance_failure_output",
        "model_output",
        "ready_output",
        "child_1_output",
        "child_1_failure_output",
        "child_2_output",
        "child_2_failure_output",
    )
    if not isinstance(outputs, dict) or set(outputs) != set(output_fields):
        raise _fail(REASON_REQUEST, "artifact output identities are incomplete", stage="preflight")
    output_paths = [Path(str(outputs[field])) for field in output_fields]
    if any(not path.is_absolute() for path in output_paths) or len({str(path) for path in output_paths}) != len(
        output_paths
    ):
        raise _fail(REASON_REQUEST, "artifact output identities must be absolute and unique", stage="preflight")
    evaluation_body = {
        "contract_version": CONTRACT_VERSION,
        "candidate_report_sha256": candidate.report["report_sha256"],
        "holdout_state_date_set_sha256": holdout["state_date_set_sha256"],
    }
    expected_evaluation = canonical_sha256(evaluation_body)
    if request.get("holdout_evaluation_id") != expected_evaluation:
        raise _fail(REASON_REQUEST, "logical holdout evaluation identity is invalid", stage="preflight")
    body = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "candidate_report_sha256": candidate.report["report_sha256"],
        "holdout_evaluation_id": expected_evaluation,
        "holdout_source_sha256": canonical_sha256(holdout),
        "request_sha256": request_hash,
        "fit_count": 0,
        "selection_performed": False,
        "holdout_accessed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _preprocessor(payload: Mapping[str, Any], *, expected_features: Sequence[str], expected_hash: str) -> Preprocessor:
    if canonical_sha256(payload) != expected_hash or tuple(payload.get("feature_names") or ()) != tuple(
        expected_features
    ):
        raise _fail(REASON_CANDIDATE, "frozen preprocess identity is invalid", stage="preflight")
    try:
        preprocessor = Preprocessor(
            feature_names=tuple(str(value) for value in payload["feature_names"]),
            lower=tuple(float(value) for value in payload["lower"]),
            upper=tuple(float(value) for value in payload["upper"]),
            mean=tuple(float(value) for value in payload["mean"]),
            std=tuple(float(value) for value in payload["std"]),
            valid_row_count=int(payload["valid_row_count"]),
            valid_identity_sha256=_require_sha256(
                payload.get("valid_identity_sha256"), "preprocess.valid_identity_sha256", reason=REASON_CANDIDATE
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _fail(REASON_CANDIDATE, "frozen preprocess values are invalid", stage="preflight") from exc
    if preprocessor.payload() != dict(payload):
        raise _fail(REASON_CANDIDATE, "frozen preprocess payload is not canonical", stage="preflight")
    return preprocessor


def _ridge_fit(component: Mapping[str, Any]) -> RidgeFit:
    fit = component.get("fit")
    if not isinstance(fit, dict):
        raise _fail(REASON_CANDIDATE, "frozen Ridge fit is missing", stage="preflight")
    _require_canonical_receipt(fit, label=f"candidate {component.get('level')} fit")
    try:
        coefficient = np.asarray(fit["coefficient"], dtype="<f8")
        result = RidgeFit(
            alpha=float(fit["alpha"]),
            coefficient=coefficient,
            intercept=float(fit["intercept"]),
            row_count=int(fit["row_count"]),
            feature_count=int(fit["feature_count"]),
            training_identity_sha256=_require_sha256(
                fit.get("training_identity_sha256"), "fit.training_identity_sha256", reason=REASON_CANDIDATE
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise _fail(REASON_CANDIDATE, "frozen Ridge fit values are invalid", stage="preflight") from exc
    if (
        result.alpha != 100.0
        or result.feature_count != len(P2_3C_FEATURES)
        or coefficient.shape != (len(P2_3C_FEATURES),)
        or not np.isfinite(coefficient).all()
        or not math.isfinite(result.intercept)
        or fit.get("coefficient_sha256") != sha256_bytes(coefficient.tobytes())
    ):
        raise _fail(REASON_CANDIDATE, "frozen Ridge fit identity is invalid", stage="preflight")
    return result


def _holdout_dates(inputs: Mapping[str, Any]) -> tuple[tuple[date, ...], tuple[date, ...]]:
    calendar = tuple(inputs.get("trading_dates") or ())
    if not calendar or any(type(day) is not date for day in calendar) or calendar != tuple(sorted(set(calendar))):
        raise _fail(REASON_SOURCE, "holdout calendar is invalid", stage="source_preflight")
    state = tuple(day for day in calendar if HOLDOUT_START <= day <= HOLDOUT_END)
    positions = {day: index for index, day in enumerate(calendar)}
    if len(state) != HOLDOUT_TRADING_DAYS or state[0] != HOLDOUT_START or state[-1] != HOLDOUT_END:
        raise _fail(REASON_SOURCE, "holdout state date set is invalid", stage="source_preflight")
    tail_start = positions[HOLDOUT_END] + 1
    tail = calendar[tail_start : tail_start + OUTCOME_TAIL_TRADING_DAYS]
    if len(tail) != OUTCOME_TAIL_TRADING_DAYS:
        raise _fail(REASON_SOURCE, "holdout outcome-tail date set is invalid", stage="source_preflight")
    return state, tail


def _dates(inputs: Mapping[str, Any], request: Mapping[str, Any]) -> tuple[tuple[date, ...], tuple[date, ...]]:
    state, tail = _holdout_dates(inputs)
    holdout = request["holdout_source"]
    if (
        canonical_sha256([day.isoformat() for day in state]) != holdout["state_date_set_sha256"]
        or canonical_sha256([day.isoformat() for day in tail]) != holdout["outcome_tail_date_set_sha256"]
        or holdout.get("outcome_tail_end") != tail[-1].isoformat()
    ):
        raise _fail(REASON_SOURCE, "holdout state or outcome-tail date identity drifted", stage="source_preflight")
    return state, tail


def validate_loaded_source(
    inputs: Mapping[str, Any], request: Mapping[str, Any]
) -> tuple[tuple[date, ...], tuple[date, ...]]:
    holdout = request["holdout_source"]
    expected = {
        "dataset_manifest_sha256": canonical_sha256(inputs.get("dataset_manifest")),
        "mapping_manifest_sha256": canonical_sha256(inputs.get("mapping_manifest")),
        "calendar_manifest_sha256": canonical_sha256(inputs.get("dataset_manifest", {}).get("calendar_benchmark")),
        "feature_formula_sha256": canonical_sha256(
            {"L1": inputs.get("feature_definition"), "L2": inputs.get("l2_feature_definition")}
        ),
        "security_identity_manifest_sha256": canonical_sha256(inputs.get("security_identity_manifest")),
        "provider_absence_manifest_sha256": canonical_sha256(inputs.get("provider_absence_manifest")),
        "constituents_sha256": canonical_sha256(inputs.get("constituents")),
    }
    drift = {
        field: {"expected": holdout.get(field), "actual": value}
        for field, value in expected.items()
        if holdout.get(field) != value
    }
    if drift:
        raise _fail(
            REASON_SOURCE, "loaded holdout source identity drifted", stage="source_preflight", evidence={"drift": drift}
        )
    return _dates(inputs, request)


def build_holdout_request(
    inputs: Mapping[str, Any],
    candidate: FrozenCandidate,
    *,
    source: Mapping[str, Any],
    artifact_outputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Freeze the exact P2-4 source and output identities before evaluation."""

    state_dates, tail_dates = _holdout_dates(inputs)
    source_payload = dict(source)
    if source_payload.get("source_end") != tail_dates[-1].isoformat():
        raise _fail(
            REASON_SOURCE,
            "holdout source end does not equal the frozen outcome-tail end",
            stage="source_preflight",
        )
    required_mappings = {
        "dataset_manifest": inputs.get("dataset_manifest"),
        "mapping_manifest": inputs.get("mapping_manifest"),
        "feature_definition": inputs.get("feature_definition"),
        "l2_feature_definition": inputs.get("l2_feature_definition"),
        "security_identity_manifest": inputs.get("security_identity_manifest"),
        "provider_absence_manifest": inputs.get("provider_absence_manifest"),
        "constituents": inputs.get("constituents"),
    }
    invalid_components = sorted(
        name for name, value in required_mappings.items() if not isinstance(value, Mapping) or not value
    )
    dataset_manifest = required_mappings["dataset_manifest"]
    calendar_manifest = dataset_manifest.get("calendar_benchmark") if isinstance(dataset_manifest, Mapping) else None
    if invalid_components or not isinstance(calendar_manifest, Mapping) or not calendar_manifest:
        raise _fail(
            REASON_SOURCE,
            "holdout request source components are missing or invalid",
            stage="source_preflight",
            evidence={"invalid_components": invalid_components},
        )
    holdout_source = {
        "source": source_payload,
        "state_start": HOLDOUT_START.isoformat(),
        "state_end": HOLDOUT_END.isoformat(),
        "state_trading_day_count": HOLDOUT_TRADING_DAYS,
        "outcome_tail_end": tail_dates[-1].isoformat(),
        "outcome_tail_trading_day_count": OUTCOME_TAIL_TRADING_DAYS,
        "dataset_manifest_sha256": canonical_sha256(dataset_manifest),
        "mapping_manifest_sha256": canonical_sha256(required_mappings["mapping_manifest"]),
        "calendar_manifest_sha256": canonical_sha256(calendar_manifest),
        "feature_formula_sha256": canonical_sha256(
            {
                "L1": required_mappings["feature_definition"],
                "L2": required_mappings["l2_feature_definition"],
            }
        ),
        "security_identity_manifest_sha256": canonical_sha256(required_mappings["security_identity_manifest"]),
        "provider_absence_manifest_sha256": canonical_sha256(required_mappings["provider_absence_manifest"]),
        "constituents_sha256": canonical_sha256(required_mappings["constituents"]),
        "state_date_set_sha256": canonical_sha256([day.isoformat() for day in state_dates]),
        "outcome_tail_date_set_sha256": canonical_sha256([day.isoformat() for day in tail_dates]),
    }
    evaluation_id = canonical_sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "candidate_report_sha256": candidate.report["report_sha256"],
            "holdout_state_date_set_sha256": holdout_source["state_date_set_sha256"],
        }
    )
    body = {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "candidate_report_sha256": candidate.report["report_sha256"],
        "candidate_producer_commit": EXPECTED_CANDIDATE_PRODUCER,
        "development_request_sha256": EXPECTED_DEVELOPMENT_REQUEST_SHA256,
        "holdout_evaluation_id": evaluation_id,
        "holdout_source": holdout_source,
        "artifact_outputs": dict(artifact_outputs),
    }
    request = {**body, "request_sha256": canonical_sha256(body)}
    validate_static_request(request, candidate)
    validate_loaded_source(inputs, request)
    return request


def _market_states(
    panel: pd.DataFrame,
    calendar: Sequence[date],
    state_dates: Sequence[date],
    candidate: FrozenCandidate,
) -> tuple[dict[date, str], dict[str, Any]]:
    market = candidate.market
    preprocessor = _preprocessor(
        market["preprocess"], expected_features=MARKET_FEATURES, expected_hash=str(market["preprocess_sha256"])
    )
    component = prepare_component(
        panel,
        component="market",
        level="L2",
        feature_names=MARKET_FEATURES,
        calendar=calendar,
        start=HOLDOUT_START,
        end=HOLDOUT_END,
        expected_days=HOLDOUT_TRADING_DAYS,
        expected_sector_count=131,
        minimum_daily_count=118,
        relative=False,
        preprocessor=preprocessor,
    )
    centers = np.asarray(market.get("centers"), dtype="<f8")
    if centers.shape != (2, len(MARKET_FEATURES)) or not np.isfinite(centers).all():
        raise _fail(REASON_CANDIDATE, "frozen market centers are invalid", stage="preflight")
    if market.get("centers_sha256") != sha256_bytes(centers.tobytes()):
        raise _fail(REASON_CANDIDATE, "frozen market center hash is invalid", stage="preflight")
    mapping_raw = market.get("semantic_mapping")
    if not isinstance(mapping_raw, dict):
        raise _fail(REASON_CANDIDATE, "frozen market semantic mapping is invalid", stage="preflight")
    mapping = {int(key): str(value) for key, value in mapping_raw.items()}
    if set(mapping.values()) != {"risk_on", "risk_off"} or set(mapping) != {0, 1}:
        raise _fail(REASON_CANDIDATE, "frozen market semantic mapping is invalid", stage="preflight")
    if market.get("semantic_mapping_sha256") != canonical_sha256({str(k): v for k, v in sorted(mapping.items())}):
        raise _fail(REASON_CANDIDATE, "frozen market semantic mapping hash is invalid", stage="preflight")
    try:
        paths = causal_states(component, centers, P2_3C_FIXED_JUMP_PENALTY)
    except Exception as exc:
        raise _fail(
            REASON_CAUSAL_STATE,
            "causal market state inference failed",
            stage="market_state",
            evidence={"exception_type": type(exc).__name__},
        ) from exc
    rows: list[list[str]] = []
    output: dict[date, str] = {}
    for sequence, path in zip(component.sequences, paths, strict=True):
        for day, raw_state in zip(sequence.dates, path, strict=True):
            label = mapping.get(int(raw_state))
            if label is None or day in output:
                raise _fail(REASON_CAUSAL_STATE, "market state output is invalid", stage="market_state")
            output[day] = label
            rows.append([day.isoformat(), label])
    if set(output) != set(state_dates):
        raise _fail(REASON_CAUSAL_STATE, "market state coverage is incomplete", stage="market_state")
    body = {
        "arrival_cost_policy": "zero_at_each_segment_start_no_train_carry",
        "state_row_count": len(rows),
        "state_rows_sha256": canonical_sha256(rows),
        "state_date_set_sha256": canonical_sha256([day.isoformat() for day in sorted(output)]),
        "fit_count": 0,
    }
    return output, {**body, "receipt_sha256": canonical_sha256(body)}


def _level_states(
    panel: pd.DataFrame,
    calendar: Sequence[date],
    market_states: Mapping[date, str],
    *,
    level: str,
    candidate: FrozenCandidate,
) -> tuple[dict[tuple[str, date], float], dict[tuple[str, date], str], dict[str, Any]]:
    authority = candidate.levels[level]
    spec = LEVEL_SPECS[level]
    preprocessor = _preprocessor(
        authority["preprocess"], expected_features=RELATIVE_FEATURES, expected_hash=str(authority["preprocess_sha256"])
    )
    component = prepare_component(
        panel,
        component=level,
        level=level,
        feature_names=RELATIVE_FEATURES,
        calendar=calendar,
        start=HOLDOUT_START,
        end=HOLDOUT_END,
        expected_days=HOLDOUT_TRADING_DAYS,
        expected_sector_count=int(spec["expected_sector_count"]),
        minimum_daily_count=int(spec["minimum_daily_count"]),
        relative=True,
        preprocessor=preprocessor,
    )
    conditioned = _condition_component(component, market_states, fold="holdout", phase="holdout")
    fit = _ridge_fit(authority)
    scores, score_receipt = predict_scores(conditioned.component, fit)
    states, state_receipt = project_daily_states(
        scores,
        level=level,
        minimum_daily_count=int(spec["minimum_daily_count"]),
    )
    if any(not math.isfinite(value) for value in scores.values()):
        raise _fail(REASON_SCORE, f"{level} score is non-finite", stage="score")
    body = {
        "level": level,
        "canonical_sector_count": len(component.canonical_codes),
        "canonical_sector_sha256": canonical_sha256(list(component.canonical_codes)),
        "conditioned_receipt_sha256": conditioned.receipt["receipt_sha256"],
        "score_receipt": score_receipt,
        "state_receipt": state_receipt,
        "unavailable_items": list(component.unavailable_items),
        "unavailable_item_count": len(component.unavailable_items),
        "fit_count": 0,
        "selection_performed": False,
    }
    return scores, states, {**body, "receipt_sha256": canonical_sha256(body)}


def _panel_returns(panel: pd.DataFrame) -> dict[tuple[str, date], float]:
    if not isinstance(panel, pd.DataFrame) or not isinstance(panel.index, pd.MultiIndex) or "daily_return" not in panel:
        raise _fail(REASON_METRIC, "holdout return panel is invalid", stage="metric")
    frame = panel.reset_index().rename(columns={panel.index.names[1]: "sector_code"})
    result: dict[tuple[str, date], float] = {}
    for row in frame.itertuples(index=False):
        try:
            day_value = getattr(row, "trade_date")
            day = day_value.date() if isinstance(day_value, pd.Timestamp) else day_value
            value = float(getattr(row, "daily_return"))
        except (AttributeError, TypeError, ValueError):
            continue
        if type(day) is date and math.isfinite(value):
            key = (str(getattr(row, "sector_code")), day)
            if key in result:
                raise _fail(REASON_SOURCE, "holdout return identity is duplicated", stage="metric")
            result[key] = value
    return result


def _benchmark_returns(dataset_manifest: Mapping[str, Any]) -> dict[date, float]:
    manifest = dataset_manifest.get("calendar_benchmark")
    rows = manifest.get("benchmark_returns") if isinstance(manifest, dict) else None
    if not isinstance(rows, list):
        raise _fail(REASON_METRIC, "holdout benchmark returns are missing", stage="metric")
    output: dict[date, float] = {}
    for row in rows:
        try:
            day = date.fromisoformat(str(row[0]))
            value = float(row[1])
        except (IndexError, TypeError, ValueError) as exc:
            raise _fail(REASON_METRIC, "holdout benchmark row is invalid", stage="metric") from exc
        if day in output or not math.isfinite(value):
            raise _fail(REASON_METRIC, "holdout benchmark identity is invalid", stage="metric")
        output[day] = value
    return output


def _future_cumulative(values: Sequence[float]) -> float:
    product = 1.0
    for value in values:
        if not math.isfinite(value):
            raise ValueError("return is non-finite")
        product *= 1.0 + value
    result = product - 1.0
    if not math.isfinite(result):
        raise ValueError("cumulative return is non-finite")
    return result


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 5:
        return None
    left_rank = pd.Series(np.asarray(left, dtype=np.float64)).rank(method="average").to_numpy(dtype=np.float64)
    right_rank = pd.Series(np.asarray(right, dtype=np.float64)).rank(method="average").to_numpy(dtype=np.float64)
    if not np.isfinite(left_rank).all() or not np.isfinite(right_rank).all():
        return None
    if float(np.var(left_rank)) <= 0.0 or float(np.var(right_rank)) <= 0.0:
        return None
    value = float(np.corrcoef(left_rank, right_rank)[0, 1])
    return value if math.isfinite(value) else None


def _outcomes(
    panel: pd.DataFrame,
    benchmark: Mapping[date, float],
    calendar: Sequence[date],
    state_dates: Sequence[date],
) -> dict[int, dict[tuple[str, date], float]]:
    returns = _panel_returns(panel)
    codes = sorted({key[0] for key in returns})
    positions = {day: index for index, day in enumerate(calendar)}
    result: dict[int, dict[tuple[str, date], float]] = {horizon: {} for horizon in HORIZONS}
    for day in state_dates:
        position = positions[day]
        for horizon in HORIZONS:
            future = calendar[position + 1 : position + horizon + 1]
            if len(future) != horizon:
                continue
            try:
                benchmark_value = _future_cumulative([benchmark[item] for item in future])
            except (KeyError, ValueError):
                continue
            for code in codes:
                try:
                    sector_value = _future_cumulative([returns[(code, item)] for item in future])
                except (KeyError, ValueError):
                    continue
                result[horizon][(code, day)] = sector_value - benchmark_value
    return result


def _risk_path_outcomes(
    panel: pd.DataFrame,
    benchmark: Mapping[date, float],
    calendar: Sequence[date],
    state_dates: Sequence[date],
) -> dict[tuple[str, date], bool]:
    returns = _panel_returns(panel)
    codes = sorted({key[0] for key in returns})
    positions = {day: index for index, day in enumerate(calendar)}
    result: dict[tuple[str, date], bool] = {}
    for day in state_dates:
        position = positions[day]
        future = calendar[position + 1 : position + PRIMARY_HORIZON + 1]
        if len(future) != PRIMARY_HORIZON:
            continue
        for code in codes:
            minimum = math.inf
            try:
                for horizon in range(1, PRIMARY_HORIZON + 1):
                    sector = _future_cumulative([returns[(code, item)] for item in future[:horizon]])
                    market = _future_cumulative([benchmark[item] for item in future[:horizon]])
                    minimum = min(minimum, sector - market)
            except (KeyError, ValueError):
                continue
            if math.isfinite(minimum):
                result[(code, day)] = minimum <= RISK_THRESHOLD
    return result


def _warning_rows(
    states: Mapping[tuple[str, date], str], market_states: Mapping[date, str]
) -> dict[tuple[str, date], bool]:
    by_code: dict[str, list[tuple[date, str]]] = {}
    for (code, day), label in states.items():
        by_code.setdefault(code, []).append((day, label))
    output: dict[tuple[str, date], bool] = {}
    for code, values in by_code.items():
        previous: str | None = None
        for day, label in sorted(values):
            warning = label == "fading" and (
                previous is not None and previous != "fading" or market_states.get(day) == "risk_off"
            )
            output[(code, day)] = bool(warning)
            previous = label
    return output


def product_gate(
    *, level: str, primary: Mapping[str, Any], risk: Mapping[str, Any], quarters: Sequence[Mapping[str, Any]]
) -> dict[str, bool]:
    """Apply the approved D3 thresholds without substituting secondary metrics."""

    if level not in {"L1", "L2"}:
        raise _fail(REASON_METRIC, "product metric level is invalid", stage="metric")
    nw_ic = primary.get("rank_ic_newey_west")
    nw_spread = primary.get("spread_newey_west")
    if not isinstance(nw_ic, Mapping) or not isinstance(nw_spread, Mapping):
        return {"directional_metrics_passed": False, "risk_metrics_passed": False, "product_metrics_passed": False}
    quarter_positive = sum(
        bool(row.get("mean_rank_ic") is not None and float(row["mean_rank_ic"]) > 0.0) for row in quarters
    )
    quarter_floor = len(quarters) == 4 and all(
        row.get("mean_rank_ic") is not None and float(row["mean_rank_ic"]) >= -0.02 for row in quarters
    )
    if level == "L2":
        directional = bool(
            nw_ic.get("metric_valid")
            and float(nw_ic["mean"]) >= 0.02
            and float(nw_ic["t_stat"]) >= 1.96
            and nw_spread.get("metric_valid")
            and float(nw_spread["mean"]) >= 0.005
            and float(nw_spread["t_stat"]) >= 1.96
            and quarter_positive >= 3
            and quarter_floor
            and len(quarters) == 4
            and all(row.get("coverage_passed") is True for row in quarters)
        )
    else:
        directional = bool(
            nw_ic.get("metric_valid")
            and float(nw_ic["mean"]) > 0.0
            and nw_spread.get("metric_valid")
            and float(nw_spread["mean"]) > 0.0
        )
    risk_passed = bool(
        risk.get("metric_valid")
        and risk.get("precision_lift") is not None
        and float(risk["precision_lift"]) >= 0.10
        and risk.get("recall") is not None
        and float(risk["recall"]) >= 0.25
    )
    coverage_passed = primary.get("metric_coverage_passed") is True
    return {
        "directional_metrics_passed": directional,
        "risk_metrics_passed": risk_passed,
        "product_metrics_passed": bool(coverage_passed and directional and risk_passed),
    }


def product_metrics(
    scores: Mapping[tuple[str, date], float],
    states: Mapping[tuple[str, date], str],
    market_states: Mapping[date, str],
    outcomes: Mapping[int, Mapping[tuple[str, date], float]],
    risk_outcome_by_identity: Mapping[tuple[str, date], bool],
    state_dates: Sequence[date],
    *,
    level: str,
) -> dict[str, Any]:
    daily: dict[int, dict[str, Any]] = {}
    signal = {"fading": -1.0, "neutral": 0.0, "trending": 1.0}
    for horizon in HORIZONS:
        ic_rows: list[list[Any]] = []
        spread_rows: list[list[Any]] = []
        unavailable: list[dict[str, Any]] = []
        horizon_outcomes = outcomes[horizon]
        for day in state_dates:
            state_codes = {code for code, state_day in states if state_day == day}
            score_codes = {code for code, score_day in scores if score_day == day}
            outcome_codes = {code for code, outcome_day in horizon_outcomes if outcome_day == day}
            if state_codes != score_codes or state_codes != outcome_codes:
                unavailable.extend(
                    {
                        "trade_date": day.isoformat(),
                        "metric": metric,
                        "reason_code": REASON_METRIC,
                        "identity_mismatch": {
                            "state_count": len(state_codes),
                            "score_count": len(score_codes),
                            "outcome_count": len(outcome_codes),
                        },
                    }
                    for metric in ("rank_ic", "spread")
                )
                continue
            observations = sorted(
                (code, signal[label], float(horizon_outcomes[(code, day)]), label)
                for (code, state_day), label in states.items()
                if state_day == day and label in signal and (code, day) in horizon_outcomes and (code, day) in scores
            )
            ic = _spearman([item[1] for item in observations], [item[2] for item in observations])
            trending = [item[2] for item in observations if item[3] == "trending"]
            fading = [item[2] for item in observations if item[3] == "fading"]
            if ic is None:
                unavailable.append({"trade_date": day.isoformat(), "metric": "rank_ic", "reason_code": REASON_METRIC})
            else:
                ic_rows.append([day.isoformat(), ic])
            if len(trending) < 5 or len(fading) < 5:
                unavailable.append({"trade_date": day.isoformat(), "metric": "spread", "reason_code": REASON_METRIC})
            else:
                spread = math.fsum(trending) / len(trending) - math.fsum(fading) / len(fading)
                if not math.isfinite(spread):
                    unavailable.append(
                        {"trade_date": day.isoformat(), "metric": "spread", "reason_code": REASON_METRIC}
                    )
                else:
                    spread_rows.append([day.isoformat(), spread])
        eligible_dates = sorted({day for _, day in horizon_outcomes})
        required = math.ceil(METRIC_COVERAGE_RATIO * len(eligible_dates))
        daily[horizon] = {
            "horizon": horizon,
            "eligible_date_count": len(eligible_dates),
            "eligible_date_set_sha256": canonical_sha256([day.isoformat() for day in eligible_dates]),
            "required_date_count": required,
            "daily_rank_ic": ic_rows,
            "daily_rank_ic_sha256": canonical_sha256(ic_rows),
            "rank_ic_available_date_count": len(ic_rows),
            "daily_spread": spread_rows,
            "daily_spread_sha256": canonical_sha256(spread_rows),
            "spread_available_date_count": len(spread_rows),
            "unavailable": unavailable,
            "metric_coverage_passed": bool(eligible_dates)
            and len(ic_rows) >= required
            and len(spread_rows) >= required,
            "rank_ic_newey_west": newey_west_t([float(item[1]) for item in ic_rows], lag=horizon - 1),
            "spread_newey_west": newey_west_t([float(item[1]) for item in spread_rows], lag=horizon - 1),
        }
    primary = daily[PRIMARY_HORIZON]
    warnings = _warning_rows(states, market_states)
    risk_outcomes: list[bool] = []
    risk_predictions: list[bool] = []
    risk_rows: list[list[Any]] = []
    ten_day = outcomes[PRIMARY_HORIZON]
    expected_risk_identities = set(ten_day) & set(states)
    actual_risk_identities = set(risk_outcome_by_identity)
    if expected_risk_identities != actual_risk_identities:
        risk = {
            "metric_valid": False,
            "reason_code": REASON_METRIC,
            "expected_identity_count": len(expected_risk_identities),
            "actual_identity_count": len(actual_risk_identities),
        }
    else:
        for key in sorted(expected_risk_identities, key=lambda item: (item[1], item[0])):
            actual = bool(risk_outcome_by_identity[key])
            predicted = bool(warnings.get(key, False))
            risk_outcomes.append(actual)
            risk_predictions.append(predicted)
            risk_rows.append([key[1].isoformat(), key[0], actual, predicted])
        risk = risk_metrics(risk_outcomes, risk_predictions)
    by_sector: list[dict[str, Any]] = []
    for code in sorted({str(item[1]) for item in risk_rows}):
        rows = [item for item in risk_rows if item[1] == code]
        by_sector.append(
            {
                "sector_code": code,
                "row_count": len(rows),
                "metrics": risk_metrics([bool(item[2]) for item in rows], [bool(item[3]) for item in rows]),
            }
        )
    by_quarter: list[dict[str, Any]] = []
    for year, quarter in ((2025, 2), (2025, 3), (2025, 4), (2026, 1)):
        rows = [
            item
            for item in risk_rows
            if date.fromisoformat(str(item[0])).year == year
            and (date.fromisoformat(str(item[0])).month - 1) // 3 + 1 == quarter
        ]
        by_quarter.append(
            {
                "quarter": f"{year}-Q{quarter}",
                "row_count": len(rows),
                "metrics": risk_metrics([bool(item[2]) for item in rows], [bool(item[3]) for item in rows]),
            }
        )
    quarter_rows: list[dict[str, Any]] = []
    for year, quarter in ((2025, 2), (2025, 3), (2025, 4), (2026, 1)):
        dates = [day for day in state_dates if day.year == year and (day.month - 1) // 3 + 1 == quarter]
        eligible = [day for day in dates if any(key[1] == day for key in ten_day)]
        values = [
            float(item[1]) for item in primary["daily_rank_ic"] if date.fromisoformat(str(item[0])) in set(eligible)
        ]
        required = math.ceil(METRIC_COVERAGE_RATIO * len(eligible))
        quarter_rows.append(
            {
                "quarter": f"{year}-Q{quarter}",
                "eligible_date_count": len(eligible),
                "required_date_count": required,
                "available_date_count": len(values),
                "mean_rank_ic": math.fsum(values) / len(values) if values else None,
                "coverage_passed": bool(eligible) and len(values) >= required,
            }
        )
    quarter_positive = sum(bool(row["mean_rank_ic"] is not None and row["mean_rank_ic"] > 0.0) for row in quarter_rows)
    quarter_floor = all(row["mean_rank_ic"] is not None and row["mean_rank_ic"] >= -0.02 for row in quarter_rows)
    gate = product_gate(level=level, primary=primary, risk=risk, quarters=quarter_rows)
    body = {
        "schema_version": "hmm_risk_p2_4_level_product_metrics_v1",
        "level": level,
        "daily_metrics": {str(key): value for key, value in daily.items()},
        "quarter_metrics": quarter_rows,
        "quarter_positive_count": quarter_positive,
        "quarter_floor_passed": quarter_floor,
        "risk_metrics": risk,
        "risk_rows_sha256": canonical_sha256(risk_rows),
        "risk_by_sector": by_sector,
        "risk_by_sector_sha256": canonical_sha256(by_sector),
        "risk_by_quarter": by_quarter,
        "risk_by_quarter_sha256": canonical_sha256(by_quarter),
        "directional_metrics_passed": gate["directional_metrics_passed"],
        "risk_metrics_passed": gate["risk_metrics_passed"],
        "product_metrics_passed": gate["product_metrics_passed"],
        "reason_code": None if gate["product_metrics_passed"] else REASON_METRIC,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _hierarchy(constituents: Mapping[str, Any], *, l1_codes: Sequence[str], l2_codes: Sequence[str]) -> dict[str, str]:
    output: dict[str, str] = {}
    for parent in l1_codes:
        value = constituents.get(parent)
        children = value.get("l2_codes") if isinstance(value, dict) else None
        if not isinstance(children, list) or not children:
            raise _fail(REASON_REPRESENTATIVENESS, "L2 hierarchy is incomplete", stage="coverage")
        for child in children:
            code = str(child)
            if code in output:
                raise _fail(REASON_REPRESENTATIVENESS, "L2 hierarchy is duplicated", stage="coverage")
            output[code] = str(parent)
    if set(output) != set(l2_codes):
        raise _fail(REASON_REPRESENTATIVENESS, "L2 hierarchy does not match canonical sectors", stage="coverage")
    return output


def _quintile_rows(inputs: Mapping[str, Any]) -> list[dict[str, Any]]:
    c010 = inputs.get("c010_diagnostic")
    aggregate = c010.get("aggregate_evidence") if isinstance(c010, Mapping) else None
    receipts = aggregate.get("l2_domain_receipts") if isinstance(aggregate, Mapping) else None
    if not isinstance(receipts, list):
        raise _fail(REASON_REPRESENTATIVENESS, "development quintile receipts are missing", stage="coverage")
    rows: list[dict[str, Any]] = []
    for item in receipts:
        if not isinstance(item, Mapping):
            raise _fail(REASON_REPRESENTATIVENESS, "development quintile receipt is invalid", stage="coverage")
        identity = _require_sha256(item.get("entry_sha256"), "quintile.entry_sha256", reason=REASON_REPRESENTATIVENESS)
        if canonical_sha256({key: value for key, value in item.items() if key != "entry_sha256"}) != identity:
            raise _fail(REASON_REPRESENTATIVENESS, "development quintile receipt hash is invalid", stage="coverage")
        try:
            day = date.fromisoformat(str(item.get("trade_date") or ""))
        except ValueError as exc:
            raise _fail(REASON_REPRESENTATIVENESS, "development quintile date is invalid", stage="coverage") from exc
        if DEVELOPMENT_START <= day <= DEVELOPMENT_END:
            rows.append(
                {
                    "trade_date": day,
                    "sector_code": str(item.get("sector_code") or ""),
                    "price_expected_weight": item.get("price_expected_weight"),
                    "moneyflow_contributor_amount": item.get("moneyflow_contributor_amount"),
                }
            )
    return rows


def _coverage_evidence(
    *,
    state_dates: Sequence[date],
    l1_codes: Sequence[str],
    l2_codes: Sequence[str],
    l1_available: set[tuple[str, date]],
    l2_available: set[tuple[str, date]],
    hierarchy: Mapping[str, str],
    size_quintiles: Mapping[str, int],
    liquidity_quintiles: Mapping[str, int],
) -> dict[str, Any]:
    dates = tuple(state_dates)

    def sector_rows(codes: Sequence[str], available: set[tuple[str, date]]) -> list[dict[str, Any]]:
        return [
            {
                "sector_code": code,
                "available_date_count": (count := sum((code, day) in available for day in dates)),
                "date_denominator": len(dates),
                "availability_ratio": count / len(dates),
                "minimum_passed": count / len(dates) >= 0.80,
            }
            for code in codes
        ]

    def quintile_rows(groups: Mapping[str, int]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for group in range(5):
            members = [code for code in l2_codes if groups.get(code) == group]
            denominator = len(members) * len(dates)
            numerator = sum((code, day) in l2_available for code in members for day in dates)
            rows.append(
                {
                    "quintile": group,
                    "member_count": len(members),
                    "available_sector_date_count": numerator,
                    "sector_date_denominator": denominator,
                    "availability_ratio": numerator / denominator if denominator else None,
                    "minimum_passed": bool(denominator) and numerator / denominator >= 0.80,
                }
            )
        return rows

    daily = [
        {
            "trade_date": day.isoformat(),
            "l1_available_count": sum((code, day) in l1_available for code in l1_codes),
            "l1_denominator": len(l1_codes),
            "l2_available_count": sum((code, day) in l2_available for code in l2_codes),
            "l2_denominator": len(l2_codes),
        }
        for day in dates
    ]
    for row in daily:
        row["coverage_available_date"] = row["l1_available_count"] >= 28 and row["l2_available_count"] >= 118
    l1_sector = sector_rows(l1_codes, l1_available)
    l2_sector = sector_rows(l2_codes, l2_available)
    size = quintile_rows(size_quintiles)
    liquidity = quintile_rows(liquidity_quintiles)
    parents: list[dict[str, Any]] = []
    for parent in l1_codes:
        children = [code for code in l2_codes if hierarchy.get(code) == parent]
        covered = sum(any((child, day) in l2_available for child in children) for day in dates)
        parents.append(
            {
                "l1_code": parent,
                "child_count": len(children),
                "covered_date_count": covered,
                "date_denominator": len(dates),
                "coverage_ratio": covered / len(dates),
                "minimum_passed": bool(children) and covered / len(dates) >= 0.90,
            }
        )
    body = {
        "daily": daily,
        "daily_sha256": canonical_sha256(daily),
        "l1_sector": l1_sector,
        "l1_sector_sha256": canonical_sha256(l1_sector),
        "l2_sector": l2_sector,
        "l2_sector_sha256": canonical_sha256(l2_sector),
        "size_quintiles": size,
        "size_quintiles_sha256": canonical_sha256(size),
        "liquidity_quintiles": liquidity,
        "liquidity_quintiles_sha256": canonical_sha256(liquidity),
        "parents": parents,
        "parents_sha256": canonical_sha256(parents),
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def evaluate_child(
    inputs: Mapping[str, Any],
    request: Mapping[str, Any],
    candidate: FrozenCandidate,
    *,
    process_index: int,
    producer_commit: str,
) -> dict[str, Any]:
    """Evaluate one fresh-process P2-4 child with zero fit and zero selection."""

    validate_static_request(request, candidate)
    state_dates, tail_dates = validate_loaded_source(inputs, request)
    calendar = tuple(inputs["trading_dates"])
    market_states, market_receipt = _market_states(inputs["l2_panel"], calendar, state_dates, candidate)
    level_payloads: dict[str, dict[str, Any]] = {}
    product_passed = True
    available: dict[str, set[tuple[str, date]]] = {}
    codes: dict[str, tuple[str, ...]] = {}
    for level, panel_key in (("L1", "panel"), ("L2", "l2_panel")):
        scores, states, state_receipt = _level_states(
            inputs[panel_key], calendar, market_states, level=level, candidate=candidate
        )
        outcomes = _outcomes(
            inputs[panel_key],
            _benchmark_returns(inputs["dataset_manifest"]),
            calendar,
            state_dates,
        )
        risk_outcomes = _risk_path_outcomes(
            inputs[panel_key],
            _benchmark_returns(inputs["dataset_manifest"]),
            calendar,
            state_dates,
        )
        metrics = product_metrics(
            scores,
            states,
            market_states,
            outcomes,
            risk_outcomes,
            state_dates,
            level=level,
        )
        product_passed = product_passed and metrics["product_metrics_passed"] is True
        available[level] = set(states)
        codes[level] = tuple(sorted({code for code, _ in scores}))
        level_payloads[level] = {
            "state": state_receipt,
            "metrics": metrics,
            "score_rows_sha256": state_receipt["score_receipt"]["score_rows_sha256"],
            "state_rows_sha256": state_receipt["state_receipt"]["date_receipts_sha256"],
        }
    development_dates = tuple(day for day in calendar if DEVELOPMENT_START <= day <= DEVELOPMENT_END)
    quintiles = freeze_quintiles(
        _quintile_rows(inputs),
        canonical_codes=codes["L2"],
        development_dates=development_dates,
        expected_development_days=DEVELOPMENT_TRADING_DAYS,
        expected_sector_count=131,
    )
    hierarchy = _hierarchy(inputs["constituents"], l1_codes=codes["L1"], l2_codes=codes["L2"])
    coverage = classify_coverage(
        holdout_dates=state_dates,
        l1_codes=codes["L1"],
        l2_codes=codes["L2"],
        l1_available=available["L1"],
        l2_available=available["L2"],
        l2_to_l1=hierarchy,
        size_quintiles=quintiles["groups"]["size"],
        liquidity_quintiles=quintiles["groups"]["liquidity"],
        product_metrics_passed=product_passed,
    )
    coverage_evidence = _coverage_evidence(
        state_dates=state_dates,
        l1_codes=codes["L1"],
        l2_codes=codes["L2"],
        l1_available=available["L1"],
        l2_available=available["L2"],
        hierarchy=hierarchy,
        size_quintiles=quintiles["groups"]["size"],
        liquidity_quintiles=quintiles["groups"]["liquidity"],
    )
    coverage_body = {key: value for key, value in coverage.items() if key != "receipt_sha256"}
    coverage_body["evidence"] = coverage_evidence
    if coverage.get("status") == "NOT_AVAILABLE":
        source_reason = coverage.get("reason_code")
        reason = (
            REASON_REPRESENTATIVENESS if source_reason == "hmm_risk_jump_representativeness_failed" else REASON_COVERAGE
        )
        coverage_body = {**coverage_body, "reason_code": reason, "source_reason_code": source_reason}
    coverage = {**coverage_body, "receipt_sha256": canonical_sha256(coverage_body)}
    runtime = runtime_versions()
    reproducibility_payload = {
        "candidate_report_sha256": candidate.report["report_sha256"],
        "holdout_evaluation_id": request["holdout_evaluation_id"],
        "holdout_source_sha256": canonical_sha256(request["holdout_source"]),
        "state_date_set_sha256": canonical_sha256([day.isoformat() for day in state_dates]),
        "outcome_tail_date_set_sha256": canonical_sha256([day.isoformat() for day in tail_dates]),
        "market_receipt": market_receipt,
        "levels": level_payloads,
        "quintiles": quintiles,
        "hierarchy_sha256": canonical_sha256(hierarchy),
        "coverage": coverage,
        "runtime_versions": runtime,
        "fit_count": 0,
        "selection_performed": False,
        "database_write": False,
        "runtime_action": False,
    }
    body = {
        "schema_version": CHILD_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "status": "child_complete",
        "process_index": process_index,
        "producer_commit": producer_commit,
        "holdout_accessed": True,
        "product_acceptance_performed": True,
        "reproducibility_payload": reproducibility_payload,
        "reproducibility_payload_sha256": canonical_sha256(reproducibility_payload),
        "model_write": False,
        "ready_write": False,
    }
    return {**body, "report_sha256": canonical_sha256(body)}


def runtime_versions() -> dict[str, Any]:
    import scipy
    import sklearn
    from threadpoolctl import threadpool_info

    environment = {
        key: os.getenv(key)
        for key in (
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "VECLIB_MAXIMUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        )
    }
    pools = [
        {
            "internal_api": item.get("internal_api"),
            "num_threads": item.get("num_threads"),
            "version": item.get("version"),
        }
        for item in threadpool_info()
    ]
    if any(value != "1" for value in environment.values()) or any(item["num_threads"] != 1 for item in pools):
        raise _fail(REASON_REPRODUCIBILITY, "fresh process is not single-threaded", stage="runtime")
    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "scipy": scipy.__version__,
        "scikit_learn": sklearn.__version__,
        "environment": environment,
        "threadpools": pools,
    }


def close_children(
    first: Mapping[str, Any], second: Mapping[str, Any], *, request: Mapping[str, Any], producer_commit: str
) -> dict[str, Any]:
    expected_top_level = {
        "schema_version",
        "contract_version",
        "algorithm_version",
        "status",
        "process_index",
        "producer_commit",
        "holdout_accessed",
        "product_acceptance_performed",
        "reproducibility_payload",
        "reproducibility_payload_sha256",
        "model_write",
        "ready_write",
        "report_sha256",
    }
    expected_payload_keys = {
        "candidate_report_sha256",
        "holdout_evaluation_id",
        "holdout_source_sha256",
        "state_date_set_sha256",
        "outcome_tail_date_set_sha256",
        "market_receipt",
        "levels",
        "quintiles",
        "hierarchy_sha256",
        "coverage",
        "runtime_versions",
        "fit_count",
        "selection_performed",
        "database_write",
        "runtime_action",
    }
    for index, value in enumerate((first, second), start=1):
        if (
            set(value) != expected_top_level
            or value.get("schema_version") != CHILD_SCHEMA_VERSION
            or value.get("contract_version") != CONTRACT_VERSION
            or value.get("algorithm_version") != ALGORITHM_VERSION
            or value.get("status") != "child_complete"
            or value.get("process_index") != index
            or value.get("producer_commit") != producer_commit
            or value.get("holdout_accessed") is not True
            or value.get("product_acceptance_performed") is not True
            or value.get("model_write") is not False
            or value.get("ready_write") is not False
        ):
            raise _fail(REASON_REPRODUCIBILITY, "child identity is invalid", stage="parent_closure")
        report_hash = _require_sha256(value.get("report_sha256"), "child.report_sha256", reason=REASON_REPRODUCIBILITY)
        if canonical_sha256({key: item for key, item in value.items() if key != "report_sha256"}) != report_hash:
            raise _fail(REASON_REPRODUCIBILITY, "child report hash is invalid", stage="parent_closure")
        payload = value.get("reproducibility_payload")
        payload_hash = _require_sha256(
            value.get("reproducibility_payload_sha256"),
            "child.reproducibility_payload_sha256",
            reason=REASON_REPRODUCIBILITY,
        )
        if (
            not isinstance(payload, dict)
            or set(payload) != expected_payload_keys
            or canonical_sha256(payload) != payload_hash
        ):
            raise _fail(REASON_REPRODUCIBILITY, "child payload hash is invalid", stage="parent_closure")
        expected_payload = {
            "candidate_report_sha256": request.get("candidate_report_sha256"),
            "holdout_evaluation_id": request.get("holdout_evaluation_id"),
            "holdout_source_sha256": canonical_sha256(request.get("holdout_source")),
            "state_date_set_sha256": request.get("holdout_source", {}).get("state_date_set_sha256"),
            "outcome_tail_date_set_sha256": request.get("holdout_source", {}).get("outcome_tail_date_set_sha256"),
            "fit_count": 0,
            "selection_performed": False,
            "database_write": False,
            "runtime_action": False,
        }
        if any(payload.get(key) != expected for key, expected in expected_payload.items()):
            raise _fail(
                REASON_REPRODUCIBILITY,
                "child payload does not close over the parent authority",
                stage="parent_closure",
            )
        coverage = payload.get("coverage")
        levels = payload.get("levels")
        nested_receipts = [payload.get("market_receipt"), payload.get("quintiles"), coverage]
        if isinstance(levels, dict):
            nested_receipts.extend(
                item
                for level in ("L1", "L2")
                for item in (
                    levels.get(level, {}).get("state") if isinstance(levels.get(level), dict) else None,
                    levels.get(level, {}).get("metrics") if isinstance(levels.get(level), dict) else None,
                )
            )
        for receipt in nested_receipts:
            if not isinstance(receipt, dict):
                raise _fail(REASON_REPRODUCIBILITY, "child nested receipt is missing", stage="parent_closure")
            receipt_hash = _require_sha256(
                receipt.get("receipt_sha256"), "child nested receipt SHA-256", reason=REASON_REPRODUCIBILITY
            )
            if (
                canonical_sha256({key: value for key, value in receipt.items() if key != "receipt_sha256"})
                != receipt_hash
            ):
                raise _fail(REASON_REPRODUCIBILITY, "child nested receipt hash is invalid", stage="parent_closure")
        if not isinstance(coverage, dict) or coverage.get("status") not in {
            "FULL_READY",
            "COVERAGE_AVAILABLE",
            "NOT_AVAILABLE",
        }:
            raise _fail(REASON_REPRODUCIBILITY, "child coverage state is invalid", stage="parent_closure")
        metrics_passed = bool(
            isinstance(levels, dict)
            and all(
                isinstance(levels.get(level), dict)
                and isinstance(levels[level].get("metrics"), dict)
                and levels[level]["metrics"].get("product_metrics_passed") is True
                for level in ("L1", "L2")
            )
        )
        if coverage.get("product_metrics_passed") is not metrics_passed or (
            coverage.get("status") in {"FULL_READY", "COVERAGE_AVAILABLE"} and not metrics_passed
        ):
            raise _fail(REASON_REPRODUCIBILITY, "child coverage and product metrics conflict", stage="parent_closure")
    payload_hash = str(first.get("reproducibility_payload_sha256") or "")
    if payload_hash != second.get("reproducibility_payload_sha256") or first.get(
        "reproducibility_payload"
    ) != second.get("reproducibility_payload"):
        raise _fail(
            REASON_REPRODUCIBILITY, "fresh-process payloads are not bitwise canonical equal", stage="parent_closure"
        )
    payload = dict(first["reproducibility_payload"])
    coverage = payload["coverage"]
    status = str(coverage["status"])
    core = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "status": status,
        "producer_commit": producer_commit,
        "candidate_report_sha256": payload["candidate_report_sha256"],
        "holdout_evaluation_id": request["holdout_evaluation_id"],
        "holdout_source_sha256": payload["holdout_source_sha256"],
        "state_date_set_sha256": payload["state_date_set_sha256"],
        "outcome_tail_date_set_sha256": payload["outcome_tail_date_set_sha256"],
        "feature_formula_sha256": request["holdout_source"]["feature_formula_sha256"],
        "hierarchy_sha256": payload["hierarchy_sha256"],
        "quintiles_sha256": payload["quintiles"]["receipt_sha256"],
        "child_report_sha256s": [first["report_sha256"], second["report_sha256"]],
        "reproducibility_payload_sha256": payload_hash,
        "metrics": {level: payload["levels"][level]["metrics"] for level in ("L1", "L2")},
        "coverage": coverage,
        "holdout_accessed": True,
        "product_acceptance_performed": True,
        "fit_count": 0,
        "selection_performed": False,
        "model_write_required": status in {"FULL_READY", "COVERAGE_AVAILABLE"},
        "ready_write_required": status == "FULL_READY",
        "database_write": False,
        "runtime_action": False,
    }
    return {**core, "acceptance_core_sha256": canonical_sha256(core)}


def model_artifact(acceptance: Mapping[str, Any], candidate: FrozenCandidate) -> dict[str, Any]:
    if acceptance.get("status") not in {"FULL_READY", "COVERAGE_AVAILABLE"}:
        raise _fail(REASON_COVERAGE, "NOT_AVAILABLE acceptance cannot write a model", stage="writer")
    market = candidate.market
    levels = candidate.levels
    compact_market = {
        "schema_version": market["schema_version"],
        "fixed_jump_penalty": market["fixed_jump_penalty"],
        "fixed_seed": market["fixed_seed"],
        "preprocess": market["preprocess"],
        "preprocess_sha256": market["preprocess_sha256"],
        "centers": market["centers"],
        "centers_sha256": market["centers_sha256"],
        "semantic_mapping": market["semantic_mapping"],
        "semantic_mapping_sha256": market["semantic_mapping_sha256"],
    }
    compact_levels = {
        level: {
            "schema_version": levels[level]["schema_version"],
            "level": level,
            "selected_alpha": levels[level]["selected_alpha"],
            "canonical_sector_count": levels[level]["canonical_sector_count"],
            "canonical_sector_sha256": levels[level]["canonical_sector_sha256"],
            "preprocess": levels[level]["preprocess"],
            "preprocess_sha256": levels[level]["preprocess_sha256"],
            "fit": levels[level]["fit"],
            "interaction_contract": {
                "base_feature_names": list(RELATIVE_FEATURES),
                "conditioned_feature_names": list(P2_3C_FEATURES),
                "market_sign": {"risk_on": 1.0, "risk_off": -1.0},
            },
        }
        for level in ("L1", "L2")
    }
    model_body = {
        "schema_version": MODEL_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "candidate_report_sha256": candidate.report["report_sha256"],
        "acceptance_core_sha256": acceptance["acceptance_core_sha256"],
        "activation_requires_matching_final_acceptance": True,
        "availability_state": acceptance["status"],
        "source_identity": {
            "holdout_evaluation_id": acceptance["holdout_evaluation_id"],
            "holdout_source_sha256": acceptance["holdout_source_sha256"],
            "state_date_set_sha256": acceptance["state_date_set_sha256"],
            "outcome_tail_date_set_sha256": acceptance["outcome_tail_date_set_sha256"],
            "feature_formula_sha256": acceptance["feature_formula_sha256"],
            "hierarchy_sha256": acceptance["hierarchy_sha256"],
            "quintiles_sha256": acceptance["quintiles_sha256"],
        },
        "market": compact_market,
        "levels": compact_levels,
        "state_projection": {
            "method": "daily_cross_section_top_bottom_fraction",
            "state_fraction": STATE_FRACTION,
            "minimum_extreme_count": MINIMUM_EXTREME_COUNT,
            "tie_tolerance": STATE_TIE_TOLERANCE,
            "semantic_order": ["fading", "neutral", "trending"],
            "missing_policy": "typed_unavailable_no_neutral_fill",
        },
        "coverage": acceptance["coverage"],
        "ready": acceptance["status"] == "FULL_READY",
        "database_write": False,
        "runtime_action": False,
    }
    return {**model_body, "model_sha256": canonical_sha256(model_body)}


def ready_artifact(acceptance: Mapping[str, Any], model: Mapping[str, Any]) -> dict[str, Any]:
    if acceptance.get("status") != "FULL_READY" or model.get("ready") is not True:
        raise _fail(REASON_COVERAGE, "only FULL_READY may write a READY marker", stage="writer")
    body = {
        "schema_version": READY_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "acceptance_core_sha256": acceptance["acceptance_core_sha256"],
        "model_sha256": model["model_sha256"],
        "activation_requires_matching_final_acceptance": True,
        "ready": True,
        "scope": "offline_model_ready_for_p2_5_only",
        "runtime_activated": False,
    }
    return {**body, "ready_sha256": canonical_sha256(body)}


def finalize_acceptance(
    draft: Mapping[str, Any], *, model_sha256: str | None, ready_sha256: str | None
) -> dict[str, Any]:
    status = str(draft.get("status") or "")
    model_required = status in {"FULL_READY", "COVERAGE_AVAILABLE"}
    ready_required = status == "FULL_READY"
    if model_required != (model_sha256 is not None) or ready_required != (ready_sha256 is not None):
        raise _fail(REASON_READBACK, "acceptance artifact closure is incomplete", stage="writer")
    if model_sha256 is not None:
        _require_sha256(model_sha256, "model_sha256", reason=REASON_READBACK)
    if ready_sha256 is not None:
        _require_sha256(ready_sha256, "ready_sha256", reason=REASON_READBACK)
    core_hash = _require_sha256(draft.get("acceptance_core_sha256"), "acceptance_core_sha256", reason=REASON_READBACK)
    core = {key: value for key, value in draft.items() if key != "acceptance_core_sha256"}
    if canonical_sha256(core) != core_hash:
        raise _fail(REASON_READBACK, "acceptance core hash is invalid", stage="writer")
    body = {
        **core,
        "acceptance_core_sha256": core_hash,
        "finalized": True,
        "model_write": model_sha256 is not None,
        "model_sha256": model_sha256,
        "ready_write": ready_sha256 is not None,
        "ready_sha256": ready_sha256,
    }
    return {**body, "report_sha256": canonical_sha256(body)}


def validate_artifact_bundle(
    acceptance: Mapping[str, Any],
    *,
    model: Mapping[str, Any] | None,
    ready: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Revalidate the final acceptance/model/READY mutual closure after durable readback."""

    report_hash = _require_sha256(acceptance.get("report_sha256"), "acceptance.report_sha256", reason=REASON_READBACK)
    if canonical_sha256({key: value for key, value in acceptance.items() if key != "report_sha256"}) != report_hash:
        raise _fail(REASON_READBACK, "final acceptance hash is invalid", stage="writer")
    status = str(acceptance.get("status") or "")
    model_required = status in {"FULL_READY", "COVERAGE_AVAILABLE"}
    ready_required = status == "FULL_READY"
    if (
        acceptance.get("schema_version") != ACCEPTANCE_SCHEMA_VERSION
        or acceptance.get("contract_version") != CONTRACT_VERSION
        or acceptance.get("algorithm_version") != ALGORITHM_VERSION
        or status not in {"FULL_READY", "COVERAGE_AVAILABLE", "NOT_AVAILABLE"}
        or acceptance.get("finalized") is not True
        or acceptance.get("model_write") is not model_required
        or acceptance.get("ready_write") is not ready_required
        or (model is not None) is not model_required
        or (ready is not None) is not ready_required
    ):
        raise _fail(REASON_READBACK, "artifact bundle state is incomplete", stage="writer")
    if model is not None:
        model_hash = _require_sha256(model.get("model_sha256"), "model.model_sha256", reason=REASON_READBACK)
        expected_source_identity = {
            key: acceptance.get(key)
            for key in (
                "holdout_evaluation_id",
                "holdout_source_sha256",
                "state_date_set_sha256",
                "outcome_tail_date_set_sha256",
                "feature_formula_sha256",
                "hierarchy_sha256",
                "quintiles_sha256",
            )
        }
        if (
            canonical_sha256({key: value for key, value in model.items() if key != "model_sha256"}) != model_hash
            or model.get("schema_version") != MODEL_SCHEMA_VERSION
            or model.get("algorithm_version") != ALGORITHM_VERSION
            or acceptance.get("model_sha256") != model_hash
            or model.get("acceptance_core_sha256") != acceptance.get("acceptance_core_sha256")
            or model.get("candidate_report_sha256") != acceptance.get("candidate_report_sha256")
            or model.get("availability_state") != status
            or model.get("ready") is not ready_required
            or model.get("activation_requires_matching_final_acceptance") is not True
            or model.get("source_identity") != expected_source_identity
            or model.get("database_write") is not False
            or model.get("runtime_action") is not False
        ):
            raise _fail(REASON_READBACK, "model does not close over final acceptance", stage="writer")
    if ready is not None:
        ready_hash = _require_sha256(ready.get("ready_sha256"), "ready.ready_sha256", reason=REASON_READBACK)
        if (
            canonical_sha256({key: value for key, value in ready.items() if key != "ready_sha256"}) != ready_hash
            or ready.get("schema_version") != READY_SCHEMA_VERSION
            or ready.get("algorithm_version") != ALGORITHM_VERSION
            or acceptance.get("ready_sha256") != ready_hash
            or ready.get("acceptance_core_sha256") != acceptance.get("acceptance_core_sha256")
            or ready.get("model_sha256") != acceptance.get("model_sha256")
            or ready.get("ready") is not True
            or ready.get("activation_requires_matching_final_acceptance") is not True
            or ready.get("scope") != "offline_model_ready_for_p2_5_only"
            or ready.get("runtime_activated") is not False
        ):
            raise _fail(REASON_READBACK, "READY marker does not close over final acceptance", stage="writer")
    body = {
        "acceptance_report_sha256": report_hash,
        "model_sha256": acceptance.get("model_sha256"),
        "ready_sha256": acceptance.get("ready_sha256"),
        "status": status,
        "bundle_valid": True,
    }
    return {**body, "bundle_sha256": canonical_sha256(body)}


def _external(path: Path, repository_root: Path) -> Path:
    resolved = path.resolve()
    root = repository_root.resolve()
    if not resolved.is_absolute() or resolved == root or root in resolved.parents:
        raise _fail(REASON_REQUEST, "artifact output must be repository-external", stage="writer")
    return resolved


def preflight_output(path: Path, *, repository_root: Path) -> Path:
    output = _external(path, repository_root)
    if output.exists():
        raise _fail(REASON_COLLISION, "artifact output already exists", stage="preflight")
    return output


def validate_output_identity(
    request: Mapping[str, Any],
    *,
    acceptance_output: Path,
    model_output: Path,
    ready_output: Path,
    child_1_output: Path,
    child_2_output: Path,
    repository_root: Path,
) -> dict[str, str]:
    actual = {
        "acceptance_output": str(_external(acceptance_output, repository_root)),
        "acceptance_failure_output": str(
            _external(acceptance_output.with_name(f"{acceptance_output.stem}.failure.json"), repository_root)
        ),
        "model_output": str(_external(model_output, repository_root)),
        "ready_output": str(_external(ready_output, repository_root)),
        "child_1_output": str(_external(child_1_output, repository_root)),
        "child_1_failure_output": str(
            _external(child_1_output.with_name(f"{child_1_output.stem}.failure.json"), repository_root)
        ),
        "child_2_output": str(_external(child_2_output, repository_root)),
        "child_2_failure_output": str(
            _external(child_2_output.with_name(f"{child_2_output.stem}.failure.json"), repository_root)
        ),
    }
    if request.get("artifact_outputs") != actual:
        raise _fail(REASON_REQUEST, "CLI output paths differ from the canonical request", stage="preflight")
    return actual


def write_once(path: Path, value: Mapping[str, Any], *, repository_root: Path) -> Path:
    output = _external(path, repository_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(value) + b"\n"
    try:
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise _fail(REASON_COLLISION, "artifact output already exists", stage="writer") from exc
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        output.unlink(missing_ok=True)
        raise
    try:
        readback = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise _fail(REASON_READBACK, "artifact readback failed", stage="writer") from exc
    if readback != dict(value):
        raise _fail(REASON_READBACK, "artifact readback mismatch", stage="writer")
    return output


def failure_receipt(
    *,
    request: Mapping[str, Any],
    producer_commit: str,
    error: Exception,
    holdout_accessed: bool | None,
    product_acceptance_performed: bool | None = False,
    model_sha256: str | None = None,
    ready_sha256: str | None = None,
) -> dict[str, Any]:
    if isinstance(error, HoldoutAcceptanceError):
        reason = error.reason_code
        stage = error.stage
        evidence = error.evidence
    else:
        reason = REASON_UNEXPECTED
        stage = "unknown"
        evidence = {"exception_type": type(error).__name__, "error_message": str(error)}
    body = {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "contract_version": CONTRACT_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "status": "NOT_AVAILABLE",
        "producer_commit": producer_commit,
        "holdout_evaluation_id": request.get("holdout_evaluation_id"),
        "failure_reason_code": reason,
        "failure_stage": stage,
        "failure_evidence": evidence,
        "holdout_accessed": holdout_accessed,
        "product_acceptance_performed": product_acceptance_performed,
        "fit_count": 0,
        "selection_performed": False,
        "model_write": model_sha256 is not None,
        "model_sha256": model_sha256,
        "ready_write": ready_sha256 is not None,
        "ready_sha256": ready_sha256,
        "database_write": False,
        "runtime_action": False,
    }
    return {**body, "report_sha256": canonical_sha256(body)}
