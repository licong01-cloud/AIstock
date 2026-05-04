from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Iterator

import pytest

from backend.services.strategy_package.models import AlphaMode, PackageStatus
from backend.services.strategy_package.qe_source_resolver import (
    QEExperimentSourceResolver,
    dict_record_conn,
)
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import UnsupportedFeatureError


def make_record(*, backtest_freq: str = "1min") -> dict:
    workspace = "rdagent_assets/qe_experiments/qe_20260416_082012"
    return {
        "experiment_id": "qe_unit_001",
        "experiment_name": "qe_unit_001",
        "status": "completed",
        "alpha_mode": "single",
        "qe_task_id": "qe_unit_001",
        "qe_loop_id": "Loop1",
        "factor_names": ["factor_a", "factor_b"],
        "model_id": "model_1",
        "strategy_id": "score_weighted_topk_v2",
        "data_split": {"test_start": "2024-01-01", "backtest_end": "2024-03-01"},
        "custom_params": {
            "topk": 50,
            "n_drop": 5,
            "stock_pool": "unit_pool",
            "backtest_freq": backtest_freq,
            "execution_algo": "TWAP",
            "execution_algo_params": {"split_count": 3},
        },
        "result_metrics": {
            "IC": 0.05,
            "Rank IC": 0.04,
            "ICIR": 1.2,
            "final_nav": 1.2,
            "n_trading_days": 30,
        },
        "workspace_path": workspace,
        "created_at": datetime.now(UTC),
        "completed_at": datetime.now(UTC),
    }


@contextmanager
def experiment_with_loop_conn(
    experiment_record: dict[str, Any],
    loop_record: dict[str, Any],
) -> Iterator[Any]:
    class _Cursor:
        def __init__(self) -> None:
            self._query = ""

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: object, *_args: object, **_kwargs: object) -> None:
            self._query = str(query)

        def fetchone(self) -> dict[str, Any] | None:
            if "FROM qe_evolution_loops" in self._query:
                return loop_record
            return experiment_record

    class _Conn:
        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self, *args: object, **kwargs: object) -> _Cursor:
            return _Cursor()

    yield _Conn()


def test_qe_single_experiment_builds_package() -> None:
    record = make_record()
    resolver = QEExperimentSourceResolver(conn_factory=lambda: dict_record_conn(record))

    manifest = resolver.build_from_experiment("qe_unit_001")

    assert manifest.alpha_mode == AlphaMode.SINGLE_ALPHA
    assert len(manifest.alpha_components) == 1
    assert manifest.alpha_components[0].factor_ids == ["factor_a", "factor_b"]
    assert manifest.minute_execution_policy.algo_code == "TWAP"
    assert manifest.minute_execution_policy.fallback_algo_code is None
    assert manifest.package_status == PackageStatus.BACKTEST_APPROVED
    assert {check.check_name for check in manifest.asset_checks} >= {
        "factor_names_present",
        "qe_task_loop_present",
        "backtest_metrics_present",
        "runtime_assets_api_only",
    }
    assert "workspace_exists" not in {check.check_name for check in manifest.asset_checks}
    assert "minute_runner_exists" not in {check.check_name for check in manifest.asset_checks}
    assert manifest.manifest_sha256
    StrategyPackageValidator().validate_for_paper_trading(manifest)


def test_qe_experiment_rejects_daily_backtest() -> None:
    record = make_record(backtest_freq="day")
    resolver = QEExperimentSourceResolver(conn_factory=lambda: dict_record_conn(record))

    with pytest.raises(UnsupportedFeatureError, match="daily backtest"):
        resolver.build_from_experiment("qe_unit_001")


def test_qe_experiment_enriches_runtime_contract_from_loop_config() -> None:
    record = make_record()
    record["custom_params"] = {
        "topk": 50,
        "n_drop": 5,
        "stock_pool": "unit_pool",
    }
    loop_record = {
        "config_json": {
            "execution_algo": "V25_TWO_STAGE",
            "execution_algo_params": {"early_model_path": "early.pt"},
            "model_params": {"topk": 50},
        },
        "task_execution_algo": None,
        "task_execution_algo_params": None,
    }
    resolver = QEExperimentSourceResolver(
        conn_factory=lambda: experiment_with_loop_conn(record, loop_record)
    )

    manifest = resolver.build_from_experiment("qe_unit_001")

    assert manifest.execution_policy.backtest_freq == "1min"
    assert manifest.minute_execution_policy.algo_code == "V25_TWO_STAGE"
    assert manifest.minute_execution_policy.algo_config == {"early_model_path": "early.pt"}
    assert manifest.strategy_config["custom_params"]["runtime_contract_source"] == "strategy_package_loop_config"
