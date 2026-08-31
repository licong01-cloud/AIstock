from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.routers import quantevolver as quantevolver_router
from backend.routers.quantevolver import GenerateConfigRequest, generate_config
from backend.routers.quantevolver_evolution import _normalize_qe_execution_algo_for_request
from backend.services.quantevolver import config_composer as config_composer_module
from backend.services.quantevolver.config_composer import ConfigComposer, SUPPORTED_QE_EXECUTION_ALGOS
from backend.services.quantevolver.execution_analyst import (
    project_execution_algorithm_catalog_row,
)
from backend.services.quantevolver.runtime_contract import build_qe_minute_runtime_contract
from backend.services.trading_core.execution_algo_retirement import (
    RETIRED_EXECUTION_ALGO_CODES,
    V25_EXECUTION_ALGO_RETIRED,
    ExecutionAlgoRetiredError,
)


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_qe_config_rejects_v25_before_catalog_or_workspace(monkeypatch: pytest.MonkeyPatch, algo_code: str) -> None:
    def unexpected_db_access():
        raise AssertionError("catalog DB must not be accessed for a retired algorithm")

    monkeypatch.setattr(config_composer_module, "get_conn", unexpected_db_access)
    with pytest.raises(ExecutionAlgoRetiredError):
        ConfigComposer._execution_algo_catalog_entry(algo_code)
    with pytest.raises(ExecutionAlgoRetiredError):
        ConfigComposer._normalize_execution_algo(algo_code)
    assert algo_code not in SUPPORTED_QE_EXECUTION_ALGOS


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_qe_runtime_contract_rejects_v25_without_fallback(algo_code: str) -> None:
    with pytest.raises(ExecutionAlgoRetiredError) as exc_info:
        build_qe_minute_runtime_contract(
            execution_algo=algo_code,
            execution_algo_params={"early_model_path": "must-not-be-read"},
            source="retirement_test",
            require_minute=True,
        )
    assert exc_info.value.context["reason_code"] == V25_EXECUTION_ALGO_RETIRED
    assert exc_info.value.context["fallback_used"] is False


def test_qe_request_boundary_returns_typed_retirement_reason() -> None:
    with pytest.raises(HTTPException) as exc_info:
        _normalize_qe_execution_algo_for_request("V25_TWO_STAGE", "execution_algo")
    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == V25_EXECUTION_ALGO_RETIRED


def test_qe_generate_rejects_before_catalog_database_access(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_catalog_access(*_args, **_kwargs):
        raise AssertionError("QE catalog must not be read before V25 retirement rejection")

    monkeypatch.setattr(quantevolver_router, "_validate_qe_catalog_refs", unexpected_catalog_access)
    with pytest.raises(HTTPException) as exc_info:
        generate_config(
            GenerateConfigRequest(
                factor_names=["factor_a"],
                custom_params={"execution_algo": "V25_TWO_STAGE"},
            )
        )
    assert exc_info.value.status_code == 422
    assert exc_info.value.detail["error_code"] == V25_EXECUTION_ALGO_RETIRED


def test_qe_positive_algorithms_are_unchanged() -> None:
    assert ConfigComposer._normalize_execution_algo("TWAP") == "TWAP"
    assert ConfigComposer._normalize_execution_algo("V24_PLAN") == "V24_PLAN"


def test_qe_catalog_projects_retired_v25_as_disabled_even_before_catalog_dml() -> None:
    row = project_execution_algorithm_catalog_row(
        {"algo_code": "V25_TWO_STAGE", "is_enabled": True}
    )
    assert row["catalog_is_enabled"] is True
    assert row["is_enabled"] is False
    assert row["retired"] is True
    assert row["selectable"] is False
    assert row["activatable"] is False
    assert row["retirement_reason_code"] == V25_EXECUTION_ALGO_RETIRED


def test_qe_catalog_projection_keeps_twap_enabled() -> None:
    row = project_execution_algorithm_catalog_row(
        {"algo_code": "TWAP", "is_enabled": True}
    )
    assert row["catalog_is_enabled"] is True
    assert row["is_enabled"] is True
    assert row["retired"] is False
    assert row["selectable"] is True
    assert row["activatable"] is True
