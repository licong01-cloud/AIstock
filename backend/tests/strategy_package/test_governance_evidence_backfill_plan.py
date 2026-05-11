from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.governance_production_apply_plan as apply_plan
import scripts.strategy_package_governance_evidence_backfill_plan as backfill_plan


def _package(package_id: str, *, package_status: str = "BACKTEST_APPROVED") -> dict:
    manifest_sha256 = f"sha256-{package_id}"
    return {
        "package_id": package_id,
        "package_status": package_status,
        "manifest_sha256": manifest_sha256,
        "assets": [
            {
                "asset_type": "model_weight",
                "asset_ref": f"models/{package_id}/weights.pkl",
                "asset_sha256": f"sha256-weight-{package_id}",
                "protected_asset": True,
            }
        ],
        "validation_runs": [
            {
                "validation_run_id": f"vr_{package_id}_original",
                "manifest_sha256": manifest_sha256,
                "validation_type": "original_fixed_weight",
                "retrain_mode": "no_retrain",
                "status": "PASSED",
                "metrics_json": {"annual_return": 0.12},
                "artifact_manifest_json": {"artifact_sha256": f"sha256-original-{package_id}"},
                "evidence_json": {
                    "regime_metrics": {
                        "bull": {"annual_return": 0.101},
                        "bear": {"annual_return": 0.102},
                    }
                },
                "completed_at": "2026-05-11T00:00:00+00:00",
            },
            {
                "validation_run_id": f"vr_{package_id}_seed101",
                "manifest_sha256": manifest_sha256,
                "validation_type": "original_retrain",
                "retrain_mode": "fixed_seed_retrain",
                "seed_policy": "fixed",
                "random_seed": 101,
                "status": "PASSED",
                "metrics_json": {"annual_return": 0.101},
                "artifact_manifest_json": {"artifact_sha256": f"sha256-seed101-{package_id}"},
                "completed_at": "2026-05-11T00:00:00+00:00",
            },
            {
                "validation_run_id": f"vr_{package_id}_seed202",
                "manifest_sha256": manifest_sha256,
                "validation_type": "original_retrain",
                "retrain_mode": "fixed_seed_retrain",
                "seed_policy": "fixed",
                "random_seed": 202,
                "status": "PASSED",
                "metrics_json": {"annual_return": 0.102},
                "artifact_manifest_json": {"artifact_sha256": f"sha256-seed202-{package_id}"},
                "completed_at": "2026-05-11T00:00:00+00:00",
            },
        ],
        "runtime_variants": [
            {
                "variant_id": f"rtv_{package_id}",
                "manifest_sha256": manifest_sha256,
                "locked_core_hash": f"core-{package_id}",
                "variant_name": "risk cap",
                "variant_kind": "risk_policy",
                "variant_config": {"risk_policy": {"max_position_weight": 0.04}},
                "variant_hash": f"variant-{package_id}",
                "validation_status": "VALIDATION_PASSED",
                "paper_candidate": True,
                "validation_evidence": {"validation_run_id": f"vr_{package_id}_runtime"},
            }
        ],
        "seed_fragility_score": {
            "manifest_sha256": manifest_sha256,
            "seed_policy": "fixed",
            "seed_sequence": [101, 202],
            "rank_stability": 0.99,
            "seed_fragile": False,
        },
    }


def _bundle() -> dict:
    return {
        "schema_version": backfill_plan.SCHEMA_VERSION,
        "packages": [_package(f"pkg_{index}") for index in range(1, 5)],
    }


def test_backfill_plan_requires_exactly_four_packages() -> None:
    payload = {
        "schema_version": backfill_plan.SCHEMA_VERSION,
        "packages": [_package("pkg_1"), _package("pkg_2"), _package("pkg_3")],
    }

    with pytest.raises(backfill_plan.GovernanceEvidenceBackfillPlanError, match="exactly 4 packages"):
        backfill_plan.build_plan(payload)


def test_backfill_plan_builds_rows_without_db_or_service_calls() -> None:
    report = backfill_plan.build_plan(_bundle(), requested_ids=[f"pkg_{index}" for index in range(1, 5)])

    assert report["status"] == "passed"
    assert report["package_count"] == 4
    assert report["db_connection_opened"] is False
    assert report["db_writes_executed"] is False
    assert report["service_calls_executed"] is False
    assert report["blocked_packages"] == {}
    first = report["packages"][0]
    assert first["required_gates"] == {
        "manifest_identity": True,
        "protected_assets": True,
        "original_fixed_weight_retest": True,
        "seed_stability_evidence": True,
        "regime_stability_evidence": True,
        "runtime_variant_candidate": True,
    }
    assert set(first["tables"]) == {
        "strategy_pkg.package_asset",
        "strategy_pkg.package_runtime_variant",
        "strategy_pkg.package_validation_run",
        "strategy_pkg.seed_fragility_score",
    }


def test_backfill_plan_refuses_paper_candidate_without_passed_evidence() -> None:
    payload = _bundle()
    payload["packages"][0]["runtime_variants"][0]["validation_status"] = "DRAFT"

    with pytest.raises(backfill_plan.GovernanceEvidenceBackfillPlanError, match="VALIDATION_PASSED"):
        backfill_plan.build_plan(payload)


def test_backfill_plan_refuses_passed_validation_without_artifacts() -> None:
    payload = _bundle()
    payload["packages"][0]["validation_runs"][0]["artifact_manifest_json"] = {}

    with pytest.raises(backfill_plan.GovernanceEvidenceBackfillPlanError, match="artifact_manifest_json"):
        backfill_plan.build_plan(payload)


def test_backfill_plan_marks_disallowed_package_status_as_blocked() -> None:
    payload = _bundle()
    payload["packages"][0]["package_status"] = "DRAFT"

    report = backfill_plan.build_plan(payload)

    assert report["status"] == "blocked"
    assert report["blocked_packages"]["pkg_1"] == ["package_status=DRAFT"]


def test_backfill_cli_dry_run_does_not_open_db(tmp_path: Path) -> None:
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(json.dumps(_bundle()), encoding="utf-8")

    assert backfill_plan.main(["--evidence-bundle", str(bundle_path), "--json"]) == 0


def test_production_apply_plan_default_is_static_preview() -> None:
    report = apply_plan.build_plan()

    assert report["status"] == "passed"
    assert report["mode"] == "static_preview"
    assert report["ddl_executed"] is False
    assert report["db_writes_executed"] is False
    assert report["migration_apply_order"][-1] == "model_registry_phase5_20260509.sql"


def test_production_apply_plan_prepared_mode_requires_token_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    args = type("Args", (), {"confirm_production_plan": ""})()

    with pytest.raises(apply_plan.GovernanceProductionApplyPlanError, match="confirm-production-plan"):
        apply_plan._require_production_plan_safety(args)

    args.confirm_production_plan = apply_plan.CONFIRM_PRODUCTION_PLAN
    monkeypatch.delenv(apply_plan.ENV_PRODUCTION_PLAN, raising=False)
    with pytest.raises(apply_plan.GovernanceProductionApplyPlanError, match=apply_plan.ENV_PRODUCTION_PLAN):
        apply_plan._require_production_plan_safety(args)

    monkeypatch.setenv(apply_plan.ENV_PRODUCTION_PLAN, "true")
    apply_plan._require_production_plan_safety(args)
