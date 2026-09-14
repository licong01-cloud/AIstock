from __future__ import annotations

from datetime import date, datetime, timezone
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256
from backend.services.advisory_model_first.prospective_price_contracts import (
    build_advisory_price_prospective_prediction_receipt,
    build_frozen_price_prospective_request,
)
from backend.services.advisory_model_first.prospective_price_evaluation import (
    AdvisoryPriceOutcomeSnapshot,
    AdvisoryPriceProspectiveEvaluationService,
    read_settlement_artifact,
)
from backend.services.advisory_model_first.prospective_price_evaluation_contracts import (
    AdvisoryPriceOutcomeRefreshAuditV1,
)
from backend.services.advisory_model_first.prospective_price_prediction import (
    AdvisoryPriceProspectivePredictionArtifact,
)


def _artifact() -> AdvisoryPriceProspectivePredictionArtifact:
    candidates = [
        {
            "symbol": "000001.SZ",
            "availability_status": "AVAILABLE",
            "decision_reference_price": 10.0,
            "calibrated_entry_price_range": {"low": 9.8, "mid": 10.0, "high": 10.2},
        },
        {
            "symbol": "000002.SZ",
            "availability_status": "UNAVAILABLE",
            "reason_code": "NORMAL_MISSING",
        },
    ]
    request = build_frozen_price_prospective_request(
        created_at=datetime(2026, 9, 14, 19, 0, tzinfo=timezone.utc),
        model_frozen_at=datetime(2026, 9, 14, 18, 0, tzinfo=timezone.utc),
        target_open_at=datetime(2026, 9, 15, 1, 30, tzinfo=timezone.utc),
        program_id="advp_test",
        binding_version_id="advb_test",
        list_version_id="list",
        review_run_id="review",
        selection_run_id="selection",
        candidate_count=2,
        candidate_symbols_sha256=canonical_json_sha256(("000001.SZ", "000002.SZ")),
        decision_as_of_trade_date=date(2026, 9, 14),
        target_trade_date=date(2026, 9, 15),
        package_id="package",
        manifest_sha256="1" * 64,
        style_profile_id="style",
        style_profile_hash="2" * 64,
        selection_runtime_semantics_hash="3" * 64,
        parent_bundle_id="4" * 64,
        parent_bundle_manifest_sha256="5" * 64,
        outcome_bundle_id="6" * 64,
        outcome_bundle_manifest_sha256="7" * 64,
        price_range_bundle_id="8" * 64,
        price_range_bundle_manifest_sha256="9" * 64,
        feature_schema_hash="a" * 64,
        review_policy_sha256="b" * 64,
        component_roles={"lstm": "role_lstm", "fund": "role_fund"},
        terminal_weights={"role_lstm": 0.6, "role_fund": 0.4},
    )
    prediction = {
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "target_trade_date": "2026-09-15",
        "generated_at": "2026-09-14T19:01:00+00:00",
        "price_envelope": {"candidates": candidates},
    }
    prediction_sha = canonical_json_sha256(prediction)
    bundle_id = canonical_json_sha256(
        {"request_sha256": request.request_sha256, "prediction_sha256": prediction_sha}
    )
    receipt = build_advisory_price_prospective_prediction_receipt(
        status="PUBLISHED",
        request_id=request.request_id,
        request_sha256=request.request_sha256,
        prediction_bundle_id=bundle_id,
        prediction_sha256=prediction_sha,
        manifest_sha256="c" * 64,
        decision_as_of_trade_date=date(2026, 9, 14),
        target_trade_date=date(2026, 9, 15),
        published_at=datetime(2026, 9, 14, 19, 1, tzinfo=timezone.utc),
        candidate_count=2,
        available_count=1,
        unavailable_count=1,
        elapsed_seconds=1.0,
        parent_bundle_id=request.parent_bundle_id,
        outcome_bundle_id=request.outcome_bundle_id,
        price_range_bundle_id=request.price_range_bundle_id,
    )
    return AdvisoryPriceProspectivePredictionArtifact(
        path=Path("unused"),
        request=request,
        prediction=prediction,
        manifest={},
        receipt=receipt,
    )


def _snapshot() -> AdvisoryPriceOutcomeSnapshot:
    audits = tuple(
        AdvisoryPriceOutcomeRefreshAuditV1(
            dataset=dataset,
            trade_date=date(2026, 9, 15),
            data_source="test",
            quality_status="ok" if dataset == "kline_daily_raw" else "empty_valid",
            row_count=100,
            refreshed_at=datetime(2026, 9, 15, 10, tzinfo=timezone.utc),
        )
        for dataset in ("kline_daily_raw", "suspend_d")
    )
    return AdvisoryPriceOutcomeSnapshot(
        refresh_audits=audits,  # type: ignore[arg-type]
        raw_open_by_symbol={"000001.SZ": 10.1},
        suspended_symbols=frozenset({"000002.SZ"}),
    )


def test_settle_publishes_immutable_outcome_and_preserves_suspension(tmp_path: Path) -> None:
    source = SimpleNamespace(calls=0)

    def load(**_kwargs):
        source.calls += 1
        return _snapshot()

    source.load = load
    service = AdvisoryPriceProspectiveEvaluationService(
        artifact_reader=lambda _path: _artifact(),
        outcome_source=source,
        now_provider=lambda: datetime(2026, 9, 15, 10, 1, tzinfo=timezone.utc),
    )
    first = service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    second = service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    assert first.status == "PUBLISHED"
    assert second.status == "ALREADY_MATERIALIZED"
    assert first.settlement_id == second.settlement_id
    assert source.calls == 1

    stored = read_settlement_artifact(
        tmp_path / "price_range_prospective_settlements" / first.request_id
    ).settlement
    assert stored.metrics.calibrated_coverage == 1.0
    assert stored.metrics.not_applicable_count == 1
    assert stored.metrics.model_unavailable_count == 1
    suspended = next(row for row in stored.candidates if row.symbol == "000002.SZ")
    assert suspended.market_outcome_status == "NOT_APPLICABLE"
    assert suspended.model_prediction_status == "UNAVAILABLE"
    assert suspended.actual_open is None
    assert stored.database_written is False
    assert stored.binding_activated is False
    assert stored.sealed_holdout_consumed is False


def test_settle_before_maturity_does_not_read_outcome_or_create_artifact(tmp_path: Path) -> None:
    source = SimpleNamespace(load=lambda **_kwargs: pytest.fail("outcome source must not be read"))
    service = AdvisoryPriceProspectiveEvaluationService(
        artifact_reader=lambda _path: _artifact(),
        outcome_source=source,
        now_provider=lambda: datetime(2026, 9, 15, 9, 59, tzinfo=timezone.utc),
    )
    with pytest.raises(AdvisoryModelFirstError) as captured:
        service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE"
    assert not (tmp_path / "price_range_prospective_settlements").exists()


def test_settlement_artifact_tampering_is_rejected(tmp_path: Path) -> None:
    source = SimpleNamespace(load=lambda **_kwargs: _snapshot())
    service = AdvisoryPriceProspectiveEvaluationService(
        artifact_reader=lambda _path: _artifact(),
        outcome_source=source,
        now_provider=lambda: datetime(2026, 9, 15, 10, 1, tzinfo=timezone.utc),
    )
    receipt = service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    root = tmp_path / "price_range_prospective_settlements" / receipt.request_id
    (root / "settlement.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as captured:
        read_settlement_artifact(root)
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT"


def test_exact_retry_rejects_a_different_prediction_identity(tmp_path: Path) -> None:
    original = _artifact()
    source = SimpleNamespace(load=lambda **_kwargs: _snapshot())
    current = {"artifact": original}
    service = AdvisoryPriceProspectiveEvaluationService(
        artifact_reader=lambda _path: current["artifact"],
        outcome_source=source,
        now_provider=lambda: datetime(2026, 9, 15, 10, 1, tzinfo=timezone.utc),
    )
    service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    drifted_receipt = original.receipt.model_copy(
        update={"prediction_bundle_id": "d" * 64, "prediction_sha256": "e" * 64}
    )
    current["artifact"] = replace(original, receipt=drifted_receipt)
    with pytest.raises(AdvisoryModelFirstError) as captured:
        service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ARTIFACT_CONFLICT"


def test_injected_outcome_snapshot_must_cover_the_exact_candidate_set(tmp_path: Path) -> None:
    snapshot = _snapshot()
    incomplete = replace(snapshot, suspended_symbols=frozenset())
    service = AdvisoryPriceProspectiveEvaluationService(
        artifact_reader=lambda _path: _artifact(),
        outcome_source=SimpleNamespace(load=lambda **_kwargs: incomplete),
        now_provider=lambda: datetime(2026, 9, 15, 10, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(AdvisoryModelFirstError) as captured:
        service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_IDENTITY_MISMATCH"


def test_unknown_model_availability_is_not_silently_treated_as_unavailable(tmp_path: Path) -> None:
    artifact = _artifact()
    prediction = deepcopy(artifact.prediction)
    prediction["price_envelope"]["candidates"][0]["availability_status"] = "BOGUS"
    drifted = replace(artifact, prediction=prediction)
    service = AdvisoryPriceProspectiveEvaluationService(
        artifact_reader=lambda _path: drifted,
        outcome_source=SimpleNamespace(load=lambda **_kwargs: _snapshot()),
        now_provider=lambda: datetime(2026, 9, 15, 10, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(AdvisoryModelFirstError) as captured:
        service.settle(prediction_artifact_path="unused", model_root=tmp_path)
    assert captured.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_INPUT_INVALID"
