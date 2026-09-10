from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.margin_information_set_contracts import (
    MARGIN_MVE_CALENDAR_SHA256,
    MARGIN_MVE_CALENDAR_SIZE,
    MARGIN_MVE_CURRENT_MARGIN_SHA256,
    MARGIN_MVE_CURRENT_MARGIN_SIZE,
    MARGIN_MVE_FEATURE_SCHEMA_HASH,
    MARGIN_MVE_SECONDARY_MARGIN_SHA256,
    MARGIN_MVE_SECONDARY_MARGIN_SIZE,
    MARGIN_MVE_SOURCE_FIELDS,
    FrozenMarginInformationSetRequestV1,
    build_margin_information_set_request,
    build_margin_source_request,
)
from backend.services.advisory_model_first.margin_information_set_pipeline import (
    _deliver_bundle,
    _publish_margin_source_bundle,
    _publish_mve_bundle,
    _read_margin_source_bundle,
    _read_margin_mve_bundle,
    _read_stable_candidate_state,
    _require_formal_environment,
    inspect_margin_information_set_bundle,
    inspect_margin_source_bundle,
    run_margin_information_set_mve,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _refs() -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_margin_generator_manifest",
        "n3_margin_generator_receipt",
        "n3_margin_n2b_manifest",
        "n3_margin_n2b_request",
        "n3_margin_n2b_outcomes",
        "n3_margin_n1_manifest",
        "n3_margin_n1_cpcv",
        "n3_margin_n1_regime_daily",
        "n3_margin_source_manifest",
        "n3_margin_source_receipt",
        "n3_margin_source_projection",
        "n3_margin_source_coverage",
        "n3_margin_cross_snapshot_parity",
        "n3_margin_candidate_state_snapshot",
    )
    return tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=f"/tmp/evidence-{index}",
            sha256=f"{index + 1:064x}",
            size_bytes=index + 1,
        )
        for index, role in enumerate(roles)
    )


def _request(tmp_path: Path) -> FrozenMarginInformationSetRequestV1:
    refs = _refs()
    source_dataset = "a" * 64
    route_dataset = "b" * 64
    split = "c" * 64
    source_identity = "d" * 64
    policy = "e" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "route_dataset_identity": route_dataset,
            "n1_split_policy_sha256": split,
            "source_identity_sha256": source_identity,
            "feature_schema_hash": MARGIN_MVE_FEATURE_SCHEMA_HASH,
            "policy_identity": policy,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    return build_margin_information_set_request(
        created_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        evidence_refs=refs,
        generator_bundle_path=(tmp_path / ("1" * 64)).as_posix(),
        generator_bundle_id="1" * 64,
        generator_request_sha256="2" * 64,
        generator_receipt_sha256="3" * 64,
        n2b_bundle_path=(tmp_path / ("4" * 64)).as_posix(),
        n2b_bundle_id="4" * 64,
        n2b_request_sha256="5" * 64,
        n2b_receipt_sha256="6" * 64,
        n1_bundle_path=(tmp_path / ("7" * 64)).as_posix(),
        n1_bundle_id="7" * 64,
        n1_request_sha256="8" * 64,
        n1_split_policy_sha256=split,
        source_bundle_path=(tmp_path / ("9" * 64)).as_posix(),
        source_bundle_id="9" * 64,
        source_request_sha256="0" * 64,
        source_receipt_sha256="1" * 64,
        source_identity_sha256=source_identity,
        source_dataset_identity=source_dataset,
        route_dataset_identity=route_dataset,
        dataset_identity=dataset,
        policy_identity=policy,
        registry_path=(tmp_path / "registry.jsonl").as_posix(),
        route_path=(tmp_path / "route.md").as_posix(),
        repository_root=tmp_path.as_posix(),
        repository_commit="f" * 40,
        output_root=(tmp_path / "output").as_posix(),
    )


def _publish(tmp_path: Path) -> tuple[FrozenMarginInformationSetRequestV1, Path]:
    request = _request(tmp_path)
    dates = pd.bdate_range("2025-01-02", periods=2)
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "instrument": ["000001.SZ", "000002.SZ"],
            "economic_net_excess_bps": [1.0, 2.0],
            "outcome_known": [True, True],
        }
    )
    oof = features.assign(
        parent_rank_pct=0.5,
        parent_comparator_oof_score=0.5,
        parent_comparator_oof_score_count=7,
        membership_oof_score=0.5,
        membership_oof_score_count=7,
        candidate_oof_score=0.5,
        candidate_oof_score_count=7,
    )
    folds = pd.DataFrame(
        {
            "trial_id": [
                "N3_MARGIN_PARENT_RIDGE_COMPARATOR_V1",
                "N3_MARGIN_MEMBERSHIP_CONTROL_V1",
                "N3_MARGIN_DYNAMICS_EXPANDED_V1",
            ],
            "path_id": ["path-0", "path-0", "path-0"],
            "train_row_count": [1, 1, 1],
            "validation_row_count": [1, 1, 1],
            "imputer_statistics_json": ["[0]", "[0]", "[0]"],
            "coefficient_json": ["[0]", "[0]", "[0]"],
            "intercept": [0.0, 0.0, 0.0],
        }
    )
    daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "parent_rank_ic": [0.0, 0.0],
            "parent_top5_net_excess_bps": [1.0, 1.0],
        }
    )
    summary = {
        "schema_version": "advisory_n3_margin_information_set_model_summary_v1",
        "request_sha256": request.request_sha256,
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selectable_trial_count": 1,
        "cumulative_candidate_index": 80,
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
        "selected_trial_id": None,
        "eligible": False,
        "support_sufficient": True,
        "evidence_class": "EXPLORATORY_NOT_SELECTED",
        "reason_codes": ["TEST_NEGATIVE"],
    }
    stability = {
        "schema_version": "advisory_n3_margin_information_set_stability_v1",
        "request_sha256": request.request_sha256,
        "rows": [],
        "late_half_start_date": "2025-01-02",
        "late_half_rank_ic_delta_mean": None,
        "late_half_top5_lift_mean_bps": None,
        "positive_joint_time_block_count": 0,
        "four_block_rule": "AT_LEAST_THREE_BLOCKS_HAVE_POSITIVE_RANKIC_DELTA_AND_TOP5_LIFT",
        "sealed_holdout_accessed": False,
    }
    stability["stability_sha256"] = canonical_json_sha256(stability)
    frontier = {
        "schema_version": "advisory_n3_margin_information_set_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": [],
        "selected_trial_id": None,
        "selected_trial_count": 0,
        "support_sufficient": True,
        "evidence_class": "EXPLORATORY_NOT_SELECTED",
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "CUMULATIVE_PARENT_AND_CURRENT_DUAL_CONTROL_LOWERS_SUPPORT_STABILITY_SELECT_ONCE",
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    bundle = _publish_mve_bundle(
        request=request,
        features=features,
        oof_scores=oof,
        fold_diagnostics=folds,
        daily_metrics=daily,
        model_summary=summary,
        stability=stability,
        frontier=frontier,
        elapsed_seconds=1.0,
    )
    return request, bundle


def _publish_source(tmp_path: Path) -> Path:
    candidate = tmp_path / "candidate"
    state_payload = {
        "status": "CANDIDATE_READY",
        "cutoff": "2026-08-31",
        "candidate_root": candidate.as_posix(),
        "production_writes": 0,
        "production_pointer_changes": 0,
        "updated_at": "2026-09-04T00:00:00Z",
        "components": {
            name: {"status": "PASS"}
            for name in ("daily_bin", "factor_h5_static", "index_context", "minute_bin", "suspend_d")
        },
        "validation": {"status": "PASS", "qe_multi_dataset_smoke": "PASS"},
    }
    state_bytes = json.dumps(state_payload, sort_keys=True, separators=(",", ":")).encode()
    state_sha256 = hashlib.sha256(state_bytes).hexdigest()
    request = build_margin_source_request(
        created_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        repository_root=tmp_path.as_posix(),
        repository_commit="f" * 40,
        n2b_bundle_path=(tmp_path / ("1" * 64)).as_posix(),
        n2b_bundle_id="1" * 64,
        n2b_outcomes_sha256="2" * 64,
        candidate_root=candidate.as_posix(),
        candidate_state_path=(candidate / "direct_monthly_state.json").as_posix(),
        candidate_state_sha256=state_sha256,
        candidate_state_size=len(state_bytes),
        candidate_state_updated_at=state_payload["updated_at"],
        current_margin_path=(candidate / "margin_detail.h5").as_posix(),
        current_margin_sha256=MARGIN_MVE_CURRENT_MARGIN_SHA256,
        current_margin_size=MARGIN_MVE_CURRENT_MARGIN_SIZE,
        secondary_margin_path=(tmp_path / "secondary-margin.h5").as_posix(),
        secondary_margin_sha256=MARGIN_MVE_SECONDARY_MARGIN_SHA256,
        secondary_margin_size=MARGIN_MVE_SECONDARY_MARGIN_SIZE,
        calendar_path=(candidate / "day.txt").as_posix(),
        calendar_sha256=MARGIN_MVE_CALENDAR_SHA256,
        calendar_size=MARGIN_MVE_CALENDAR_SIZE,
    )
    available_dates = pd.bdate_range("2024-07-04", "2026-02-02")
    positions = np.linspace(0, len(available_dates) - 1, 386, dtype=int)
    dates = available_dates[positions]
    quotient, remainder = divmod(1_710_301, len(dates))
    instrument_counts = [quotient + int(index < remainder) for index in range(len(dates))]
    coverage = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "source_date_d": dates - pd.offsets.BDay(1),
            "source_date_d1": dates - pd.offsets.BDay(2),
            "source_date_d5": dates - pd.offsets.BDay(6),
            "instrument_count": instrument_counts,
            "source_row_count": instrument_counts,
            "source_row_fraction": 1.0,
            "top20_source_row_count": 20,
            "top20_source_row_fraction": 1.0,
            "top50_source_row_count": 50,
            "top50_source_row_fraction": 1.0,
        }
    )
    projection = pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2024-07-03")],
            "instrument": ["000001.SZ"],
            **{name: np.asarray([1.0], dtype=np.float32) for name in MARGIN_MVE_SOURCE_FIELDS},
        }
    )
    parity = {
        "schema_version": "advisory_n3_margin_cross_snapshot_parity_v1",
        "source_quality": request.source_quality,
        "source_window_start": "2024-07-03",
        "source_window_end": "2026-01-30",
        "projection_common_key_count": 1,
        "common_key_count": 1,
        "current_only_key_count": 0,
        "secondary_only_key_count": 0,
        "value_drift_row_count": 0,
        "source_fields": list(MARGIN_MVE_SOURCE_FIELDS),
        "comparison_semantics": "FLOAT32_EXACT_WITH_PAIRED_NAN",
        "vintage_archive": False,
        "sealed_holdout_accessed": False,
    }
    return _publish_margin_source_bundle(
        output_root=tmp_path / "output",
        request=request,
        candidate_state_bytes=state_bytes,
        candidate_state_after_sha256=state_sha256,
        projection=projection,
        coverage=coverage,
        parity=parity,
        raw_quality={name: 1.0 for name in MARGIN_MVE_SOURCE_FIELDS},
        current_rows_read=1,
        secondary_rows_read=1,
        source_bytes_read=1,
        elapsed_seconds=1.0,
    )


def test_bundle_publish_inspect_and_delivery_are_restart_safe(tmp_path: Path) -> None:
    request, bundle = _publish(tmp_path)
    inspected = inspect_margin_information_set_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["selected_trial_count"] == 0
    assert inspected["next_task"] == "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN"
    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)
    assert first["registry"]["appended_count"] == 1
    assert second["registry"]["duplicate_noop_count"] == 1
    assert second["route"]["status"] == "EXACT_NOOP"


def test_source_bundle_publish_inspect_and_mutation_rejection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.services.advisory_model_first.margin_information_set_pipeline._peak_rss_bytes",
        lambda: 1,
    )
    bundle = _publish_source(tmp_path)
    inspected = inspect_margin_source_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["source_row_fraction"] == 1.0
    target = bundle / "cross_snapshot_parity.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["current_only_key_count"] = 1
    target.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _read_margin_source_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID"


@pytest.mark.parametrize("mutation", ["modify", "delete", "extra"])
def test_bundle_mutation_partial_and_extra_member_are_rejected(tmp_path: Path, mutation: str) -> None:
    _, bundle = _publish(tmp_path)
    if mutation == "modify":
        target = bundle / "model_summary.json"
        payload = json.loads(target.read_text(encoding="utf-8"))
        payload["reason_codes"] = []
        target.write_text(json.dumps(payload), encoding="utf-8")
    elif mutation == "delete":
        (bundle / "daily_metrics.parquet").unlink()
    else:
        (bundle / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _read_margin_mve_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID"


def test_candidate_state_non_object_has_typed_identity_failure(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    state = candidate / "direct_monthly_state.json"
    state.write_text("[]", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _read_stable_candidate_state(state, candidate)
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH"


def test_prepare_and_run_environment_gate_requires_rdagent_gpu(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONDA_DEFAULT_ENV", "not-rdagent-gpu")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _require_formal_environment()
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID"


def test_missing_request_file_has_typed_request_failure(tmp_path: Path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as caught:
        run_margin_information_set_mve(tmp_path / "missing-request.json")
    assert caught.value.reason_code == "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID"
