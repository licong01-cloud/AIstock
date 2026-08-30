from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import (
    authorize_research_window_access,
    freeze_default_research_windows,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (
    DecisionUse,
    ObjectiveContract,
    ResearchStudyType,
    ResearchWindowState,
    build_window_access_request,
)


def _sealed(contract):
    return next(item for item in contract.windows if item.state == ResearchWindowState.SEALED_UNCONSUMED)


def _request(contract, *, study_type, window, **overrides):
    values = {
        "contract_sha256": contract.contract_sha256,
        "study_type": study_type,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "decision_use": (
            DecisionUse.DIRECTION_GATE
            if study_type == ResearchStudyType.CONFIRMATION
            else DecisionUse.NAVIGATION_ONLY
        ),
        "dataset_identity": window.dataset_identity,
        "policy_identity": research_policy_identity(),
        "start_date": window.start_date,
        "end_date": window.end_date,
        "frontier_id": "frontier-1" if study_type == ResearchStudyType.CONFIRMATION else None,
        "candidate_id": "candidate-1" if study_type == ResearchStudyType.CONFIRMATION else None,
    }
    values.update(overrides)
    return build_window_access_request(**values)


@pytest.mark.parametrize(
    "study_type",
    (
        ResearchStudyType.ORACLE_DIAGNOSTIC,
        ResearchStudyType.LEARNABILITY_AUDIT,
        ResearchStudyType.EXPLORATORY_SCREEN,
        ResearchStudyType.CANDIDATE_MODEL,
        ResearchStudyType.ACTIVATION,
    ),
)
def test_non_confirmation_studies_cannot_overlap_sealed_holdout(study_type):
    contract = freeze_default_research_windows()
    request = _request(contract, study_type=study_type, window=_sealed(contract))

    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(contract=contract, request=request)

    assert captured.value.reason_code == "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED"


def test_declared_development_window_is_allowed_but_unknown_range_is_denied():
    contract = freeze_default_research_windows()
    development = next(item for item in contract.windows if item.window_id == "P0C_DEVELOPMENT_V1")
    request = _request(
        contract, study_type=ResearchStudyType.ORACLE_DIAGNOSTIC, window=development
    )

    result = authorize_research_window_access(contract=contract, request=request)
    assert result["status"] == "AUTHORIZED_DEVELOPMENT_ONLY"

    unknown = _request(
        contract,
        study_type=ResearchStudyType.ORACLE_DIAGNOSTIC,
        window=development,
        start_date="2027-01-01",
        end_date="2027-01-31",
    )
    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(contract=contract, request=unknown)
    assert captured.value.reason_code == "ADVISORY_RESEARCH_WINDOW_CONFLICT"


def test_activation_cannot_read_raw_development_window():
    contract = freeze_default_research_windows()
    development = next(item for item in contract.windows if item.window_id == "P0C_DEVELOPMENT_V1")
    request = _request(contract, study_type=ResearchStudyType.ACTIVATION, window=development)

    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(contract=contract, request=request)
    assert captured.value.reason_code == "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED"


def test_confirmation_consumes_exact_sealed_identity_once(tmp_path):
    contract = freeze_default_research_windows(artifact_root_uri=tmp_path)
    request = _request(
        contract, study_type=ResearchStudyType.CONFIRMATION, window=_sealed(contract)
    )
    receipt_path = tmp_path / "sealed_holdout_consumption_receipt.json"

    first = authorize_research_window_access(
        contract=contract, request=request, consume_receipt_path=receipt_path
    )
    assert first["status"] == "AUTHORIZED_SEALED_HOLDOUT_ONCE"
    assert receipt_path.is_file()

    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(
            contract=contract, request=request, consume_receipt_path=receipt_path
        )
    assert captured.value.reason_code == "ADVISORY_SEALED_HOLDOUT_ALREADY_CONSUMED"


def test_concurrent_confirmation_allows_exactly_one_consumer(tmp_path):
    contract = freeze_default_research_windows(artifact_root_uri=tmp_path)
    request = _request(
        contract, study_type=ResearchStudyType.CONFIRMATION, window=_sealed(contract)
    )
    receipt_path = tmp_path / "sealed_holdout_consumption_receipt.json"

    def consume() -> str:
        try:
            authorize_research_window_access(
                contract=contract,
                request=request,
                consume_receipt_path=receipt_path,
            )
            return "AUTHORIZED"
        except AdvisoryModelFirstError as exc:
            return exc.reason_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: consume(), range(2)))

    assert outcomes.count("AUTHORIZED") == 1
    assert outcomes.count("ADVISORY_SEALED_HOLDOUT_ALREADY_CONSUMED") == 1


def test_confirmation_cannot_bypass_consume_once_with_an_alternate_receipt_path(tmp_path):
    contract = freeze_default_research_windows(artifact_root_uri=tmp_path)
    request = _request(
        contract, study_type=ResearchStudyType.CONFIRMATION, window=_sealed(contract)
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(
            contract=contract,
            request=request,
            consume_receipt_path=tmp_path / "alternate.json",
        )
    assert captured.value.reason_code == "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED"


@pytest.mark.parametrize(
    "override",
    (
        {"dataset_identity": "different"},
        {"policy_identity": "f" * 64},
        {"start_date": "2026-09-01"},
        {"end_date": "2026-11-29"},
    ),
)
def test_confirmation_rejects_any_dataset_policy_or_date_reselection(tmp_path, override):
    contract = freeze_default_research_windows()
    request = _request(
        contract,
        study_type=ResearchStudyType.CONFIRMATION,
        window=_sealed(contract),
        **override,
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(
            contract=contract,
            request=request,
            consume_receipt_path=tmp_path / "consume.json",
        )
    assert captured.value.reason_code == "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED"


def test_confirmation_cannot_reuse_a_consumed_development_window(tmp_path):
    contract = freeze_default_research_windows()
    development = next(item for item in contract.windows if item.window_id == "P0C_DEVELOPMENT_V1")
    request = _request(
        contract, study_type=ResearchStudyType.CONFIRMATION, window=development
    )

    with pytest.raises(AdvisoryModelFirstError) as captured:
        authorize_research_window_access(
            contract=contract,
            request=request,
            consume_receipt_path=tmp_path / "consume.json",
        )
    assert captured.value.reason_code == "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED"
