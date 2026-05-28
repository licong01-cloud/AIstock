from __future__ import annotations

import pytest

from backend.services.strategy_package.backtest_contract import normalize_runtime_config_with_backtest_contract
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy
from backend.services.trading_core.errors import RuntimeConfigInvalidError
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


def test_backtest_contract_never_reinjects_hmm_runtime_from_qe_config() -> None:
    manifest = _paper_manifest(
        custom_params={
            "enable_sector_hmm": True,
            "hmm_model_version_id": "qe_hmm_snapshot_old",
            "hmm_signal_preset": "qe_preset",
            "hmm_coefficients_file": "qe_hmm_sector_coefficients.json",
        }
    )

    disabled = normalize_runtime_config_with_backtest_contract(manifest, {}, include_contract=True)
    assert disabled["runtime_profile"]["hmm"] == {
        "enabled": False,
        "model_config_id": None,
        "model_snapshot_id": None,
        "signal_preset": None,
        "coefficients_path": None,
        "auto_compute": True,
        "manual_snapshot_required": False,
    }
    assert disabled["qe_backtest_runtime_contract"]["runtime_features"]["hmm"] == {
        "enabled": False,
        "authority": "platform_runtime",
        "feature": "hmm",
        "package_bound": False,
    }

    runtime_hmm = normalize_runtime_config_with_backtest_contract(
        manifest,
        {
            "runtime_profile": {
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "platform_hmm_snapshot_new",
                    "signal_preset": "platform_preset",
                }
            }
        },
    )
    assert runtime_hmm["runtime_profile"]["hmm"]["model_snapshot_id"] == "platform_hmm_snapshot_new"
    assert runtime_hmm["runtime_profile"]["hmm"]["signal_preset"] == "platform_preset"


def test_backtest_contract_allows_audited_runtime_topk_and_platform_hmm() -> None:
    manifest = _paper_manifest(
        topk=50,
        custom_params={
            "enable_sector_hmm": True,
            "hmm_model_version_id": "hmm_snapshot_001",
            "hmm_signal_preset": "preset_A",
            "hmm_coefficients_file": "hmm_sector_coefficients.json",
        },
    )

    config = normalize_runtime_config_with_backtest_contract(
        manifest,
        {
            "runtime_profile": {
                "selection": {"top_k": 20},
                "hmm": {
                    "enabled": True,
                    "model_snapshot_id": "another_snapshot",
                    "signal_preset": "preset_B",
                    "coefficients_path": "platform_coefficients.json",
                }
            }
        },
    )
    assert config["runtime_profile"]["selection"]["top_k"] == 20
    assert config["runtime_profile"]["hmm"]["model_snapshot_id"] == "another_snapshot"
    assert config["runtime_profile"]["hmm"]["signal_preset"] == "preset_B"


def test_backtest_contract_rejects_invalid_runtime_topk_boundaries() -> None:
    manifest = _paper_manifest(topk=50)

    with pytest.raises(RuntimeConfigInvalidError, match="top_k must be between"):
        normalize_runtime_config_with_backtest_contract(
            manifest,
            {"runtime_profile": {"selection": {"top_k": 0}}},
        )


def test_backtest_contract_leaves_platform_blacklist_and_tradability_runtime_owned() -> None:
    manifest = _paper_manifest(
        custom_params={
            "sector_blacklist": ["Bank", "Broker"],
            "filter_suspended_on_signal": False,
        }
    )

    config = normalize_runtime_config_with_backtest_contract(manifest, {})
    assert config["runtime_profile"]["industry_blacklist"] == []
    assert config["runtime_profile"]["tradability"]["exclude_suspended"] is True

    runtime_owned = normalize_runtime_config_with_backtest_contract(
        manifest,
        {"runtime_profile": {"industry_blacklist": ["Bank"], "tradability": {"exclude_suspended": False}}},
    )
    assert runtime_owned["runtime_profile"]["industry_blacklist"] == ["Bank"]
    assert runtime_owned["runtime_profile"]["tradability"]["exclude_suspended"] is False


def test_event_signal_policy_is_platform_runtime_profile_not_strategy_package_config() -> None:
    manifest = _paper_manifest(
        custom_params={
            "event_signal_policy": {
                "enabled": True,
                "event_signal_profile_id": "qe_evt_profile",
                "asof_policy": "effective_trade_date",
                "signal_merge_policy": "block_first",
            }
        }
    )

    default_config = normalize_runtime_config_with_backtest_contract(manifest, {}, include_contract=True)
    risk_policy = default_config["runtime_profile"]["risk_policy"]
    assert risk_policy["enabled"] is False
    assert risk_policy["providers"] == ["st_pit"]
    assert default_config["qe_backtest_runtime_contract"]["runtime_features"]["event_signal_policy"] == {
        "enabled": False,
        "authority": "platform_runtime",
        "feature": "event_signal_policy",
        "package_bound": False,
    }

    runtime_config = normalize_runtime_config_with_backtest_contract(
        manifest,
        {
            "runtime_profile": {
                "risk_policy": {
                    "enabled": True,
                    "providers": ["st_pit", "event_signal_policy"],
                    "event_signal_profile_id": "platform_evt_profile",
                    "event_signal_asof_policy": "effective_trade_date",
                    "event_signal_merge_policy": "block_first",
                }
            }
        },
    )
    assert runtime_config["runtime_profile"]["risk_policy"]["event_signal_profile_id"] == "platform_evt_profile"
