from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.minute_information_set_contracts import (
    MINUTE_MVE_RAW_ECONOMIC_FEATURES,
    build_minute_information_set_request,
)
from backend.services.advisory_model_first.minute_information_set_pipeline import (
    _deliver_bundle,
    _publish_bundle,
    _read_minute_bundle,
    _require_formal_environment,
    _validate_bound_sources,
    _validate_source_control_refs,
    fingerprint_minute_source,
    inspect_minute_information_set_bundle,
    run_minute_information_set_mve,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    EvidenceReferenceV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _refs(tmp_path: Path) -> tuple[EvidenceReferenceV1, ...]:
    roles = (
        "n3_minute_leg_manifest",
        "n3_minute_leg_receipt",
        "n3_minute_n2a_manifest",
        "n3_minute_n2a_request",
        "n3_minute_n2a_full_universe",
        "n3_minute_n1_manifest",
        "n3_minute_n1_cpcv",
        "n3_minute_n1_regime_daily",
        "n3_minute_source_spike_receipt",
        "n3_minute_source_meta",
        "n3_minute_source_calendar",
        "n3_minute_source_instruments",
    )
    refs: list[EvidenceReferenceV1] = []
    for index, role in enumerate(roles):
        path = tmp_path / f"evidence-{index}.bin"
        payload = f"evidence-{index}".encode()
        path.write_bytes(payload)
        refs.append(
            EvidenceReferenceV1(
                role=role,
                artifact_uri=path.as_posix(),
                sha256=hashlib.sha256(payload).hexdigest(),
                size_bytes=len(payload),
            )
        )
    return tuple(refs)


def _request(tmp_path: Path):
    refs = _refs(tmp_path)
    source_dataset = "a" * 64
    route_dataset = "b" * 64
    split = "c" * 64
    minute_content = "d" * 64
    dataset = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset,
            "route_dataset_identity": route_dataset,
            "n1_split_policy_sha256": split,
            "minute_source_content_sha256": minute_content,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    return build_minute_information_set_request(
        created_at=datetime(2026, 9, 3, tzinfo=timezone.utc),
        evidence_refs=refs,
        leg_bundle_path=(tmp_path / ("1" * 64)).as_posix(),
        leg_bundle_id="1" * 64,
        leg_request_sha256="2" * 64,
        leg_receipt_sha256="3" * 64,
        n2a_bundle_path=(tmp_path / ("4" * 64)).as_posix(),
        n2a_bundle_id="4" * 64,
        n2a_request_sha256="5" * 64,
        n2a_receipt_sha256="6" * 64,
        n1_bundle_path=(tmp_path / ("7" * 64)).as_posix(),
        n1_bundle_id="7" * 64,
        n1_request_sha256="8" * 64,
        n1_split_policy_sha256=split,
        source_spike_receipt_path=(tmp_path / "source-spike.json").as_posix(),
        source_spike_receipt_sha256="9" * 64,
        source_dataset_identity=source_dataset,
        route_dataset_identity=route_dataset,
        minute_source_content_sha256=minute_content,
        minute_source_file_count=2,
        dataset_identity=dataset,
        policy_identity="e" * 64,
        registry_path=(tmp_path / "registry.jsonl").as_posix(),
        route_path=(tmp_path / "route.md").as_posix(),
        repository_root=tmp_path.as_posix(),
        repository_commit="f" * 40,
        output_root=(tmp_path / "output").as_posix(),
    )


def _publish(tmp_path: Path) -> tuple[object, Path]:
    request = _request(tmp_path)
    dates = pd.bdate_range("2025-01-02", periods=2)
    instruments = [f"{index:06d}.SZ" for _ in dates for index in range(5)]
    features = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates.repeat(5),
            "instrument": instruments,
            "outcome_known": True,
            "economic_net_excess_bps": 1.0,
        }
    )
    features["minute_available"] = 1
    features["minute_coverage_fraction"] = 1.0
    for name in MINUTE_MVE_RAW_ECONOMIC_FEATURES:
        features[name] = 0.5
    oof = features[["decision_as_of_trade_date", "instrument"]].assign(
        parent_rank_pct=0.5,
        economic_net_excess_bps=1.0,
        outcome_known=True,
        comparator_oof_score=0.5,
        comparator_oof_score_count=7,
        candidate_oof_score=0.5,
        candidate_oof_score_count=7,
    )
    folds = pd.DataFrame(
        {
            "trial_id": ["N3_MINUTE_PARENT_RIDGE_COMPARATOR_V1", "N3_MINUTE_INFORMATION_EXPANDED_V1"],
            "path_id": ["path-0", "path-0"],
            "train_row_count": [5, 5],
            "validation_row_count": [5, 5],
            "imputer_statistics_json": ["[0]", "[0]"],
            "coefficient_json": ["[0]", "[0]"],
            "intercept": [0.0, 0.0],
        }
    )
    coverage = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "instrument_count": 5,
            "raw_calendar_slot_count": 240,
            "effective_calendar_slot_count": 240,
            "market_wide_empty_slot_count": 0,
            "market_wide_empty_slots": [[], []],
            "session_wide_single_bar_deficit": False,
            "complete_instrument_count": 5,
            "partial_instrument_count": 0,
            "whole_day_missing_instrument_count": 0,
            "normalized_complete_instrument_count": 5,
            "normalized_partial_instrument_count": 0,
            "available_fraction": 1.0,
            "mean_coverage_fraction": 1.0,
        }
    )
    daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "parent_rank_ic": [0.0, 0.0],
            "parent_top5_net_excess_bps": [1.0, 1.0],
            "parent_top5_evaluable": [True, True],
            "parent_top5_churn": [None, 0.0],
        }
    )
    inventory = pd.DataFrame(
        {
            "instrument": ["000001.SZ", "000001.SZ"],
            "field": ["open", "close"],
            "relative_path": ["features/000001.sz/open.1min.bin", "features/000001.sz/close.1min.bin"],
            "size_bytes": [4, 4],
            "mtime_ns_telemetry": [1, 1],
            "sha256": ["a" * 64, "b" * 64],
        }
    )
    summary = {
        "schema_version": "advisory_n3_minute_information_set_model_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": 2,
        "familywise_hypothesis_count": request.familywise_hypothesis_count,
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
        "selected_trial_id": None,
        "eligible": False,
        "reason_codes": ["TEST_NEGATIVE"],
    }
    frontier = {
        "schema_version": "advisory_n3_minute_information_set_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": [],
        "selected_trial_id": None,
        "selected_trial_count": 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "ALL_FOUR_FAMILYWISE_LOWERS_AND_DUAL_BASELINE_SUPPORT__SELECT_ONCE",
        "decision_use": "NAVIGATION_ONLY",
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    bundle = _publish_bundle(
        request=request,
        inventory=inventory,
        coverage_daily=coverage,
        features=features,
        oof_scores=oof,
        fold_diagnostics=folds,
        daily_metrics=daily,
        model_summary=summary,
        frontier=frontier,
        elapsed_seconds=1.0,
    )
    return request, bundle


def test_bundle_publish_inspect_and_delivery_are_restart_safe(tmp_path: Path) -> None:
    request, bundle = _publish(tmp_path)
    inspected = inspect_minute_information_set_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["selected_trial_count"] == 0
    assert inspected["next_task"] == "N3_QE_ALPHA_GENERATOR_MVE_DESIGN"
    first = _deliver_bundle(request=request, bundle_path=bundle)
    second = _deliver_bundle(request=request, bundle_path=bundle)
    assert first["registry"]["appended_count"] == 1
    assert second["registry"]["duplicate_noop_count"] == 1
    assert second["route"]["status"] == "EXACT_NOOP"


def test_bundle_mutation_is_rejected(tmp_path: Path) -> None:
    _, bundle = _publish(tmp_path)
    summary_path = bundle / "model_summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["reason_codes"] = []
    summary_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _read_minute_bundle(bundle)
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID"


def test_minute_source_fingerprint_is_content_bound(tmp_path: Path) -> None:
    provider = tmp_path / "provider"
    (provider / "calendars").mkdir(parents=True)
    (provider / "instruments").mkdir()
    (provider / "features" / "000001.sz").mkdir(parents=True)
    required = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
        "up_limit_price",
        "down_limit_price",
        "prev_close",
        "limit_up",
        "limit_down",
    ]
    (provider / "meta_export.json").write_text(
        json.dumps(
            {
                "snapshot_id": "qlib_minute_authoritative_full_candidate_20240102_20260630",
                "required_minute_fields": required,
            }
        ),
        encoding="utf-8",
    )
    (provider / "calendars" / "1min.txt").write_text("2025-01-02 09:30:00\n", encoding="utf-8")
    (provider / "instruments" / "all.txt").write_text(
        "000001.SZ\t2024-01-02 09:30:00\t2026-06-30 15:00:00\n", encoding="utf-8"
    )
    for field in ("open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down"):
        (provider / "features" / "000001.sz" / f"{field}.1min.bin").write_bytes(field.encode())
    inventory, first = fingerprint_minute_source(provider_path=provider, instruments=("000001.SZ",))
    assert len(inventory) == 8
    target = provider / "features" / "000001.sz" / "close.1min.bin"
    target.write_bytes(b"changed")
    _, second = fingerprint_minute_source(provider_path=provider, instruments=("000001.SZ",))
    assert first != second


def test_selected_leg_result_cannot_seed_minute_request() -> None:
    leg = {
        "receipt": SimpleNamespace(
            selected_trial_count=1,
            selected_trial_id="x",
            next_task="N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN",
            decision_use=DecisionUse.NAVIGATION_ONLY,
            sealed_holdout_accessed=False,
            deployable=False,
        ),
        "record": SimpleNamespace(
            experiment_id="ADVISORY-N3-LEG-DISAGREEMENT-LEARNABILITY-V1",
            policy_identity="p",
        ),
    }
    n2a = {
        "record": SimpleNamespace(
            experiment_id="ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",
            evaluated_trial_count=0,
            decision_use=DecisionUse.NAVIGATION_ONLY,
            policy_identity="p",
        )
    }
    n1 = {
        "request": SimpleNamespace(
            decision_date_start=pd.Timestamp("2024-07-04").date(), decision_date_end=pd.Timestamp("2026-02-02").date()
        ),
        "learnability": SimpleNamespace(sealed_holdout_accessed=False),
    }
    spike = {
        "source_ready": True,
        "model_training_performed": False,
        "target_or_label_columns_read": False,
        "sealed_holdout_accessed": False,
        "database_accessed": False,
        "network_accessed": False,
        "runtime_mutated": False,
        "snapshot_id": "qlib_minute_authoritative_full_candidate_20240102_20260630",
        "n2a_key_scope": {"row_count": 1_710_301, "manifest_interval_covered_rows": 1_710_301},
    }
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_bound_sources(leg=leg, n2a=n2a, n1=n1, spike=spike)
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID"


def test_source_control_files_must_match_source_readiness_receipt() -> None:
    refs = (
        SimpleNamespace(role="n3_minute_source_meta", sha256="a" * 64),
        SimpleNamespace(role="n3_minute_source_calendar", sha256="b" * 64),
        SimpleNamespace(role="n3_minute_source_instruments", sha256="c" * 64),
    )
    spike = {
        "source_hashes": {
            "meta_export_sha256": "a" * 64,
            "calendar_sha256": "b" * 64,
            "instrument_manifest_sha256": "c" * 64,
        }
    }
    _validate_source_control_refs(spike=spike, evidence_refs=refs)
    spike["source_hashes"]["calendar_sha256"] = "d" * 64
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _validate_source_control_refs(spike=spike, evidence_refs=refs)
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH"


def test_prepare_and_run_environment_gate_requires_rdagent_gpu(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONDA_DEFAULT_ENV", "not-rdagent-gpu")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _require_formal_environment()
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID"


def test_missing_request_file_has_typed_request_failure(tmp_path: Path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as caught:
        run_minute_information_set_mve(tmp_path / "missing-request.json")
    assert caught.value.reason_code == "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID"
