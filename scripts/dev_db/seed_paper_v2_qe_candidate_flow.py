"""Synthetic fixtures for the Paper v2 / QE candidate DEV-DB E2E gate.

The helpers in this file are intentionally side-effect free. Tests import them
to build deterministic manifests and snapshots; the test owns DB writes and
rolls them back.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaMode,
    BacktestSummary,
    ExecutionPolicy,
    FactorAsset,
    MinuteExecutionPolicy,
    ModelAsset,
    PackageStatus,
    PortfolioPolicy,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
    UniversePolicy,
)

SCHEMA_VERSION = "paper_v2_qe_candidate_devdb_e2e_v1"

PHASES = [
    "phase_0_safety_gate",
    "phase_1_schema_gate",
    "phase_2_qe_candidate_seed",
    "phase_3_cross_module_flow",
    "phase_4_invariants_and_report",
]

SYNTHETIC_EXPERIMENT_ID = "qe_devdb_candidate_e2e_exp"
SYNTHETIC_ARCHIVE_RUN_ID = "qear_devdb_candidate_e2e_run"


def build_devdb_e2e_manifest() -> StrategyPackageManifest:
    """Build a minimal frozen StrategyPackage manifest for cross-module tests."""

    component = AlphaComponent(
        alpha_id="alpha_devdb_e2e",
        alpha_name="devdb_e2e_alpha",
        component_weight=1.0,
        factor_ids=["factor_devdb_e2e"],
        model_id="model_devdb_e2e",
        model_ref="artifact://qe/devdb_e2e/model.pkl",
        holding_period="1day",
        rebalance_frequency="1day",
        score_direction="higher_better",
    )
    manifest = StrategyPackageManifest(
        package_name="devdb_candidate_e2e",
        source=StrategyPackageSource(
            source_type=SourceType.QE_EXPERIMENT,
            source_id=SYNTHETIC_EXPERIMENT_ID,
            run_id=SYNTHETIC_ARCHIVE_RUN_ID,
        ),
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        alpha_components=[component],
        alpha_combination_policy=AlphaCombinationPolicy(
            method="identity",
            weights={"alpha_devdb_e2e": 1.0},
        ),
        factor_set=[
            FactorAsset(
                factor_id="factor_devdb_e2e",
                factor_name="devdb_e2e_factor",
                artifact_ref="artifact://qe/devdb_e2e/factors.py",
            )
        ],
        model_asset=ModelAsset(
            model_id="model_devdb_e2e",
            model_ref="artifact://qe/devdb_e2e/model.pkl",
            model_type="CATBOOST",
        ),
        strategy_config={
            "strategy_id": "score_weighted_topk_v2",
            "seed_contract": {"seed_policy": "unset_legacy"},
            "hyperparameters_available": False,
            "_precomputed_hmm_coefficients_json": {"source": "qe_backtest_snapshot"},
            "hmm_snapshot_id": "qe_backtest_hmm_snapshot_should_not_lock",
        },
        universe_policy=UniversePolicy(
            stock_pool="paper_v2_platform_latest",
            st_pit_snapshot_id="qe_backtest_st_pit_snapshot_should_not_lock",
            st_pit_start_date="2018-01-01",
            st_pit_end_date="2026-05-12",
        ),
        portfolio_policy=PortfolioPolicy(topk=2, n_drop=0),
        execution_policy=ExecutionPolicy(backtest_freq="1min"),
        minute_execution_policy=MinuteExecutionPolicy(
            algo_code="TWAP",
            algo_config={"max_participation_rate": 0.1},
        ),
        backtest_summary=BacktestSummary(
            ic=0.052,
            rank_ic=0.047,
            annual_return=0.18,
            max_drawdown=-0.08,
            raw_metrics={"IC": 0.052, "RankIC": 0.047},
            sample_start=date(2018, 1, 1),
            sample_end=date(2026, 5, 12),
        ),
        package_status=PackageStatus.BACKTEST_APPROVED,
    )
    return freeze_manifest(manifest)


def build_devdb_candidate_snapshot(manifest: StrategyPackageManifest) -> dict[str, Any]:
    """Return user-requirement-focused candidate snapshot metadata."""

    return {
        "snapshot_config": {
            "qe_experiment_id": SYNTHETIC_EXPERIMENT_ID,
            "qe_archive_run_id": SYNTHETIC_ARCHIVE_RUN_ID,
            "warehouse_storage_policy": "lightweight_metadata_only",
        },
        "factor_manifest": {
            "factor_source": "qe_experiment",
            "factor_set_hash": "sha256:devdb-e2e-factor-set",
        },
        "model_manifest": {
            "training_reproducibility": {
                "seed_policy": "unset_legacy",
                "master_seed_available": False,
                "hyperparameters_available": False,
                "blocks_live_approval": True,
            },
        },
        "strategy_manifest": {
            "qe_strategy_config_shared": True,
            "minute_execution_algo": manifest.minute_execution_policy.algo_code,
            "tail_handling_policy": "qe_compatible",
        },
        "metric_snapshot": {
            "ic": manifest.backtest_summary.ic,
            "rank_ic": manifest.backtest_summary.rank_ic,
            "annual_return": manifest.backtest_summary.annual_return,
        },
        "artifact_refs": {
            "model_weight_uri": "artifact://qe/devdb_e2e/model.pkl",
            "factor_code_uri": "artifact://qe/devdb_e2e/factors.py",
            "qe_archive_run_uri": f"qe_archive://run/{SYNTHETIC_ARCHIVE_RUN_ID}",
            "storage_policy": "uri_and_hash_only",
        },
        "completeness": {
            "strategy_package_manifest_available": False,
            "seed_available": False,
            "hyperparameters_available": False,
            "live_approval_ready": False,
        },
        "eligibility": {
            "selection_supported": True,
            "paper_simulation_supported": True,
            "live_approval_supported": False,
        },
        "audit_context": {
            "manual_action": True,
            "paper_enabled": False,
            "live_approved": False,
            "schema_version": SCHEMA_VERSION,
        },
    }


def write_report(path: Path, payload: dict[str, Any]) -> None:
    """Write a deterministic JSON report for the nox session artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
