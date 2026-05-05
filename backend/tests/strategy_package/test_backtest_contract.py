from __future__ import annotations

import pytest

from backend.services.strategy_package.backtest_contract import normalize_runtime_config_with_backtest_contract
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _paper_manifest(*, custom_params: dict | None = None, topk: int = 50):
    base = make_manifest()
    strategy_config = dict(base.strategy_config)
    if custom_params is not None:
        strategy_config["custom_params"] = custom_params
    return freeze_manifest(
        base.model_copy(
            update={
                "package_status": PackageStatus.PAPER_ENABLED,
                "portfolio_policy": PortfolioPolicy(topk=topk, n_drop=5),
                "strategy_config": strategy_config,
            }
        )
    )


def test_backtest_contract_populates_hmm_runtime_from_qe_config() -> None:
    manifest = _paper_manifest(
        custom_params={
            "enable_sector_hmm": True,
            "hmm_model_version_id": "hmm_snapshot_001",
            "hmm_signal_preset": "preset_A",
            "hmm_coefficients_file": "hmm_sector_coefficients.json",
        }
    )

    config = normalize_runtime_config_with_backtest_contract(manifest, {}, include_contract=True)

    hmm_profile = config["runtime_profile"]["hmm"]
    assert hmm_profile == {
        "enabled": True,
        "model_snapshot_id": "hmm_snapshot_001",
        "signal_preset": "preset_A",
        "coefficients_path": "hmm_sector_coefficients.json",
    }
    assert config["qe_backtest_runtime_contract"]["runtime_features"]["hmm"]["enabled"] is True


def test_backtest_contract_rejects_runtime_topk_and_hmm_mismatch() -> None:
    manifest = _paper_manifest(
        topk=50,
        custom_params={
            "enable_sector_hmm": True,
            "hmm_model_version_id": "hmm_snapshot_001",
            "hmm_signal_preset": "preset_A",
            "hmm_coefficients_file": "hmm_sector_coefficients.json",
        },
    )

    with pytest.raises(StrategyPackageValidationError, match="top_k must match"):
        normalize_runtime_config_with_backtest_contract(
            manifest,
            {"runtime_profile": {"selection": {"top_k": 20}}},
        )
    with pytest.raises(StrategyPackageValidationError, match="HMM model_snapshot_id must match"):
        normalize_runtime_config_with_backtest_contract(
            manifest,
            {
                "runtime_profile": {
                    "hmm": {
                        "enabled": True,
                        "model_snapshot_id": "another_snapshot",
                        "signal_preset": "preset_A",
                        "coefficients_path": "hmm_sector_coefficients.json",
                    }
                }
            },
        )


def test_backtest_contract_populates_blacklist_and_rejects_conflict() -> None:
    manifest = _paper_manifest(
        custom_params={
            "sector_blacklist": ["Bank", "Broker"],
            "filter_suspended_on_signal": False,
        }
    )

    config = normalize_runtime_config_with_backtest_contract(manifest, {})
    assert config["runtime_profile"]["industry_blacklist"] == ["Bank", "Broker"]
    assert config["runtime_profile"]["tradability"]["exclude_suspended"] is False

    with pytest.raises(StrategyPackageValidationError, match="industry blacklist must match"):
        normalize_runtime_config_with_backtest_contract(
            manifest,
            {"runtime_profile": {"industry_blacklist": ["Bank"]}},
        )
