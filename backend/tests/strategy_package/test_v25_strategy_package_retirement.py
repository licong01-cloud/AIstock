from __future__ import annotations

import pytest

from backend.services.strategy_package.execution_policy import ValidatedExecutionPolicy
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.execution_algo_retirement import (
    RETIRED_EXECUTION_ALGO_CODES,
    V25_EXECUTION_ALGO_RETIRED,
    ExecutionAlgoRetiredError,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class _NoDatabaseRepository:
    def get(self, _package_id: str):
        raise AssertionError("repository must not be accessed before retirement rejection")


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_create_execution_policy_rejects_before_database_write(algo_code: str) -> None:
    service = StrategyPackageService(repository=_NoDatabaseRepository())
    with pytest.raises(ExecutionAlgoRetiredError) as exc_info:
        service.create_execution_policy(
            package_id="pkg_retired",
            policy_name="retired",
            policy_json={"algo_code": algo_code, "algo_config": {}},
            source_backtest_id="historical_backtest",
            source_backtest_status="SUCCEEDED",
        )
    assert exc_info.value.context["reason_code"] == V25_EXECUTION_ALGO_RETIRED
    assert exc_info.value.context["side_effect_started"] is False


@pytest.mark.parametrize("algo_code", sorted(RETIRED_EXECUTION_ALGO_CODES))
def test_paper_policy_validation_rejects_before_model_asset_access(algo_code: str) -> None:
    with pytest.raises(ExecutionAlgoRetiredError):
        StrategyPackageValidator().validate_execution_policy_for_paper(
            package_id="pkg_retired",
            policy_json={
                "algo_code": algo_code,
                "algo_config": {
                    "early_model_path": "must-not-be-read",
                    "late_model_path": "must-not-be-read",
                },
            },
        )


def test_historical_policy_model_remains_readable() -> None:
    policy = ValidatedExecutionPolicy(
        package_id="pkg_history",
        manifest_sha256="a" * 64,
        policy_name="historical V25 policy",
        policy_json={"algo_code": "V25_TWO_STAGE", "algo_config": {}},
        source_backtest_id="historical_backtest",
        source_backtest_status="SUCCEEDED",
    )
    assert policy.algo_code == "V25_TWO_STAGE"
    assert policy.policy_json["algo_code"] == "V25_TWO_STAGE"


def test_historical_v25_package_cannot_be_promoted_to_paper() -> None:
    repository = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest(algo_code="V25_TWO_STAGE").model_copy(
            update={"package_status": PackageStatus.BACKTEST_APPROVED, "manifest_sha256": None}
        )
    )
    repository.save_manifest(manifest)

    with pytest.raises(ExecutionAlgoRetiredError):
        StrategyPackageService(repository=repository).enable_paper(manifest.package_id)
    assert repository.get(manifest.package_id).package_status == PackageStatus.BACKTEST_APPROVED


def test_non_execution_v25_labels_are_not_rejected() -> None:
    StrategyPackageValidator().validate_execution_policy_for_paper(
        package_id="pkg_positive",
        policy_json={"algo_code": "TWAP", "algo_config": {"report_label": "V25_TWO_STAGE"}},
        instantiate_runtime=False,
        require_runtime_assets=False,
    )
