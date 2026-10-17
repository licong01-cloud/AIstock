from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prospective_price_prediction import (
    AdvisoryPriceProspectivePredictionService,
    PROSPECTIVE_ROOT_NAME,
    _prospective_candidate_symbols,
    _require_next_trading_day,
    read_prospective_prediction_artifact,
)
from backend.services.advisory_model_first.prospective_price_contracts import (
    build_frozen_price_prospective_request,
    target_open_utc,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64
SHA_F = "f" * 64


def _request(symbols=("000001.SZ", "000002.SZ")):
    from backend.services.advisory_model_first.price_range_contracts import (
        canonical_json_sha256,
    )

    return build_frozen_price_prospective_request(
        created_at=datetime(2026, 9, 15, 8, 0, tzinfo=timezone.utc),
        model_frozen_at=datetime(2026, 9, 14, 18, 10, tzinfo=timezone.utc),
        target_open_at=target_open_utc(date(2026, 9, 16)),
        program_id="advp_program",
        binding_version_id="advb_binding",
        list_version_id="advl_list",
        review_run_id="advr_review",
        selection_run_id="sel_selection",
        candidate_count=len(symbols),
        candidate_symbols_sha256=canonical_json_sha256(tuple(sorted(symbols))),
        decision_as_of_trade_date=date(2026, 9, 15),
        target_trade_date=date(2026, 9, 16),
        package_id="pkg_alpha",
        manifest_sha256=SHA_A,
        style_profile_id="short_rebound_v1",
        style_profile_hash=SHA_B,
        selection_runtime_semantics_hash=SHA_C,
        parent_bundle_id=SHA_D,
        parent_bundle_manifest_sha256=SHA_E,
        outcome_bundle_id=SHA_F,
        outcome_bundle_manifest_sha256=SHA_A,
        price_range_bundle_id=SHA_B,
        price_range_bundle_manifest_sha256=SHA_C,
        feature_schema_hash=SHA_D,
        review_policy_sha256=SHA_E,
        component_roles={"lstm": "lstm_role", "fund": "fund_role"},
        terminal_weights={"lstm_role": 0.7, "fund_role": 0.3},
    )


class _ProgramService:
    def __init__(self) -> None:
        self.calls = 0

    def get_program(self, program_id):
        self.calls += 1
        assert program_id == "advp_program"
        return SimpleNamespace(program_id=program_id)


class _ShadowService:
    def __init__(self, symbols, *, poison=False) -> None:
        self.symbols = symbols
        self.poison = poison
        self.calls = []

    def model_shadow_for_forward(self, **kwargs):
        self.calls.append(kwargs)
        candidates = [
            {
                "symbol": symbol,
                "availability_status": "AVAILABLE",
                **({"realized_return": 0.1} if self.poison else {}),
            }
            for symbol in self.symbols
        ]
        return {
            "status": "EXPERIMENTAL_SHADOW",
            "bundle_id": SHA_D,
            "price_range": {
                "status": "EXPERIMENTAL_SHADOW",
                "availability_status": "AVAILABLE",
                "price_range_bundle_id": SHA_B,
                "candidates": candidates,
            },
        }


def _service(now, shadow):
    program_service = _ProgramService()
    service = AdvisoryPriceProspectivePredictionService(
        program_service=program_service,
        shadow_service_factory=lambda request: shadow,
        now_provider=lambda: now,
    )
    return service, program_service


def test_capture_publishes_immutable_prediction_without_side_effects(tmp_path: Path) -> None:
    request = _request()
    shadow = _ShadowService(("000001.SZ", "000002.SZ"))
    service, program_service = _service(datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc), shadow)

    receipt = service.capture(request=request, model_root=tmp_path)

    assert receipt.status == "PUBLISHED"
    assert receipt.available_count == 2
    assert receipt.realized_outcome_accessed is False
    assert receipt.binding_activated is False
    assert receipt.database_written is False
    assert receipt.sealed_holdout_consumed is False
    assert program_service.calls == 1
    assert shadow.calls[0]["list_version_id"] == "advl_list"
    target = tmp_path / PROSPECTIVE_ROOT_NAME / request.request_id
    assert {path.name for path in target.iterdir()} == {
        "manifest.json",
        "prediction.json",
        "receipt.json",
        "request.json",
    }
    prediction = json.loads((target / "prediction.json").read_text(encoding="utf-8"))
    assert "outcome" not in prediction
    assert prediction["realized_outcome_accessed"] is False


def test_public_artifact_reader_reuses_strict_publication_validation(tmp_path: Path) -> None:
    request = _request()
    service, _program = _service(
        datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc),
        _ShadowService(("000001.SZ", "000002.SZ")),
    )
    receipt = service.capture(request=request, model_root=tmp_path)

    artifact = read_prospective_prediction_artifact(
        tmp_path / PROSPECTIVE_ROOT_NAME / request.request_id
    )

    assert artifact.request == request
    assert artifact.receipt.prediction_bundle_id == receipt.prediction_bundle_id
    assert artifact.receipt.status == "ALREADY_MATERIALIZED"
    assert artifact.prediction["realized_outcome_accessed"] is False


def test_exact_retry_after_target_open_reads_existing_without_recompute(tmp_path: Path) -> None:
    request = _request()
    first_shadow = _ShadowService(("000001.SZ", "000002.SZ"))
    first, _program = _service(datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc), first_shadow)
    first.capture(request=request, model_root=tmp_path)
    second_shadow = _ShadowService(("999999.SZ",))
    second, second_program = _service(datetime(2026, 9, 16, 2, 0, tzinfo=timezone.utc), second_shadow)

    receipt = second.capture(request=request, model_root=tmp_path)

    assert receipt.status == "ALREADY_MATERIALIZED"
    assert second_program.calls == 0
    assert second_shadow.calls == []


def test_first_capture_at_or_after_target_open_is_rejected(tmp_path: Path) -> None:
    request = _request()
    service, _program = _service(
        datetime(2026, 9, 16, 1, 30, tzinfo=timezone.utc),
        _ShadowService(("000001.SZ", "000002.SZ")),
    )

    with pytest.raises(AdvisoryModelFirstError) as error:
        service.capture(request=request, model_root=tmp_path)

    assert error.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID"


def test_capture_rejects_candidate_drift_and_realized_result_poison(tmp_path: Path) -> None:
    request = _request()
    drifted, _program = _service(
        datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc),
        _ShadowService(("000001.SZ", "000003.SZ")),
    )
    with pytest.raises(AdvisoryModelFirstError) as error:
        drifted.capture(request=request, model_root=tmp_path)
    assert error.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH"

    poisoned, _program = _service(
        datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc),
        _ShadowService(("000001.SZ", "000002.SZ"), poison=True),
    )
    with pytest.raises(AdvisoryModelFirstError) as error:
        poisoned.capture(request=request, model_root=tmp_path)
    assert error.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ACCESS_FORBIDDEN"


def test_existing_artifact_tamper_fails_closed(tmp_path: Path) -> None:
    request = _request()
    service, _program = _service(
        datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc),
        _ShadowService(("000001.SZ", "000002.SZ")),
    )
    service.capture(request=request, model_root=tmp_path)
    target = tmp_path / PROSPECTIVE_ROOT_NAME / request.request_id / "prediction.json"
    target.write_text("{}\n", encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as error:
        service.capture(request=request, model_root=tmp_path)

    assert error.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT"


def test_existing_receipt_tamper_fails_closed(tmp_path: Path) -> None:
    request = _request()
    service, _program = _service(
        datetime(2026, 9, 15, 8, 5, tzinfo=timezone.utc),
        _ShadowService(("000001.SZ", "000002.SZ")),
    )
    service.capture(request=request, model_root=tmp_path)
    target = tmp_path / PROSPECTIVE_ROOT_NAME / request.request_id / "receipt.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["available_count"] = 1
    payload["unavailable_count"] = 1
    target.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as error:
        service.capture(request=request, model_root=tmp_path)

    assert error.value.reason_code == "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT"


def test_next_trading_day_check_uses_calendar_provider_contract() -> None:
    calls = []

    class Calendar:
        def list_trading_days(self, start_date, end_date):
            calls.append((start_date, end_date))
            return [start_date, end_date]

    _require_next_trading_day(
        Calendar(),
        decision_date=date(2026, 9, 15),
        target_trade_date=date(2026, 9, 16),
    )

    assert calls == [(date(2026, 9, 15), date(2026, 9, 16))]


def test_candidate_identity_uses_selection_top_group_not_all_list_items() -> None:
    selection_rows = [SimpleNamespace(symbol=f"00000{rank}.SZ", rank=rank) for rank in range(1, 4)]
    list_items = [{"symbol": f"0000{rank:02d}.SZ", "rank": rank, "action": "HOLD"} for rank in range(1, 53)]

    symbols = _prospective_candidate_symbols(
        selection_rows=selection_rows,
        list_items=list_items,
        list_version={"summary_json": {}},
        target_count=3,
    )

    assert symbols == ("000001.SZ", "000002.SZ", "000003.SZ")
