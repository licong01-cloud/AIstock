from __future__ import annotations

import os

import pytest

from backend.services.strategy_package.manifest import compute_manifest_json_sha256, freeze_manifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.tests.strategy_package.test_manifest_v1 import make_manifest
from scripts import strategy_package_manifest_hash_repair as repair_script


def _legacy_schema_manifest_sha(record) -> str:
    payload = record.current_manifest().model_dump(mode="json")
    for key in ("source_evidence", "backtest_context"):
        if payload.get(key) == {}:
            payload.pop(key)
    return compute_manifest_json_sha256(payload)


def _repo_with_drift(*, dirty: bool = False) -> tuple[InMemoryStrategyPackageRepository, str, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg", "package_name": "pkg"}))
    repo.save_manifest(manifest)
    if dirty:
        stored = "d" * 64
    else:
        stored = _legacy_schema_manifest_sha(repo.records["pkg"])
    repo.records["pkg"] = repo.records["pkg"].model_copy(update={"manifest_sha256": stored})
    return repo, stored, manifest.manifest_sha256 or ""


def test_manifest_hash_repair_dry_run_reports_repairable_a_class() -> None:
    repo, stored, computed = _repo_with_drift()

    result = repair_script.build_dry_run_report(repo.validate_manifest_integrity())

    assert result["mode"] == "dry_run"
    assert result["drifted_count"] == 1
    assert result["filtered_drifted_count"] == 1
    assert result["repairable_count"] == 1
    assert result["blocked_count"] == 0
    assert result["repairable"][0]["package_id"] == "pkg"
    assert result["repairable"][0]["stored_sha256"] == stored
    assert result["repairable"][0]["computed_sha256"] == computed
    assert result["repairable"][0]["classification"] == "A_schema_evolution_stale_hash"


def test_manifest_hash_repair_dry_run_blocks_b_class() -> None:
    repo, stored, _computed = _repo_with_drift(dirty=True)

    result = repair_script.build_dry_run_report(repo.validate_manifest_integrity())

    assert result["repairable_count"] == 0
    assert result["blocked_count"] == 1
    assert result["blocked"][0]["stored_sha256"] == stored
    assert result["blocked"][0]["classification"] == "B_manifest_json_dirty_or_unknown"


def test_manifest_hash_apply_repairs_and_is_idempotent() -> None:
    repo, _stored, computed = _repo_with_drift()
    report = repo.validate_manifest_integrity()

    first = repair_script.apply_repairs(repo, report, operator="unit_test")
    second = repair_script.apply_repairs(repo, repo.validate_manifest_integrity(), operator="unit_test")

    assert first["mode"] == "apply"
    assert first["repaired_count"] == 1
    assert first["repaired"][0] == {"package_id": "pkg", "manifest_sha256": computed}
    assert first["after_drifted_count"] == 0
    assert second["repaired_count"] == 0
    assert second["after_drifted_count"] == 0


def test_manifest_hash_apply_refuses_b_class() -> None:
    repo, _stored, _computed = _repo_with_drift(dirty=True)

    with pytest.raises(repair_script.ManifestHashRepairScriptError, match="non-repairable"):
        repair_script.apply_repairs(repo, repo.validate_manifest_integrity(), operator="unit_test")


def test_manifest_hash_dry_run_can_filter_scratch_prefix() -> None:
    repo = InMemoryStrategyPackageRepository()
    for package_id in ("scratch_pkg", "prod_like_pkg"):
        manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": package_id, "package_name": package_id}))
        repo.save_manifest(manifest)
        repo.records[package_id] = repo.records[package_id].model_copy(
            update={"manifest_sha256": _legacy_schema_manifest_sha(repo.records[package_id])}
        )

    result = repair_script.build_dry_run_report(
        repo.validate_manifest_integrity(),
        package_id_prefix="scratch_",
        target={"target_db": "dev"},
    )

    assert result["target"] == {"target_db": "dev"}
    assert result["filter"] == {"package_id_prefix": "scratch_"}
    assert result["drifted_count"] == 2
    assert result["filtered_drifted_count"] == 1
    assert result["repairable_count"] == 1
    assert result["repairable"][0]["package_id"] == "scratch_pkg"


def test_dev_target_config_must_look_like_scratch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TDX_DB_DEV_HOST", "127.0.0.1")
    monkeypatch.setenv("TDX_DB_DEV_PORT", "5433")
    monkeypatch.setenv("TDX_DB_DEV_NAME", "aistock")
    monkeypatch.setenv("TDX_DB_DEV_USER", "postgres")
    monkeypatch.setenv("TDX_DB_DEV_PASSWORD", "secret")

    with pytest.raises(repair_script.ManifestHashRepairScriptError, match="scratch/dev DB"):
        repair_script._db_config(target_db=repair_script.TARGET_DEV)

    monkeypatch.setenv("TDX_DB_DEV_NAME", "aistock_dev")
    cfg = repair_script._db_config(target_db=repair_script.TARGET_DEV)
    assert cfg["dbname"] == "aistock_dev"


@pytest.mark.parametrize(
    "env_value,confirm_prod,confirm_scratch,target_db,apply,should_raise",
    [
        (None, False, False, repair_script.TARGET_PROD, False, False),
        (None, True, False, repair_script.TARGET_PROD, True, True),
        (repair_script.APPLY_CONFIRM_VALUE, False, False, repair_script.TARGET_PROD, True, True),
        (None, False, False, repair_script.TARGET_DEV, True, True),
        (None, False, True, repair_script.TARGET_DEV, True, False),
    ],
)
def test_manifest_hash_main_apply_requires_double_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    env_value: str | None,
    confirm_prod: bool,
    confirm_scratch: bool,
    target_db: str,
    apply: bool,
    should_raise: bool,
) -> None:
    argv = ["prog", "--limit", "1", "--target-db", target_db]
    if apply:
        argv.append("--apply")
    if confirm_prod:
        argv.append("--confirm-production-dml")
    if confirm_scratch:
        argv.append("--confirm-scratch-dml")
    monkeypatch.setattr("sys.argv", argv)
    if env_value is None:
        monkeypatch.delenv(repair_script.APPLY_CONFIRM_ENV, raising=False)
    else:
        monkeypatch.setenv(repair_script.APPLY_CONFIRM_ENV, env_value)
    if should_raise:
        with pytest.raises(repair_script.ManifestHashRepairScriptError):
            args = repair_script._parse_args()
            if args.apply:
                if args.target_db == repair_script.TARGET_PROD:
                    if not args.confirm_production_dml:
                        raise repair_script.ManifestHashRepairScriptError("--apply requires --confirm-production-dml")
                    if os.environ.get(repair_script.APPLY_CONFIRM_ENV) != repair_script.APPLY_CONFIRM_VALUE:
                        raise repair_script.ManifestHashRepairScriptError("env confirmation missing")
                elif not args.confirm_scratch_dml:
                    raise repair_script.ManifestHashRepairScriptError("scratch confirmation missing")
    else:
        args = repair_script._parse_args()
        assert args.target_db == target_db
