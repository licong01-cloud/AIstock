from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

import scripts.governance_migration_smoke as smoke


def test_static_full_stack_smoke_passes_without_db_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("default static governance smoke must not connect to DB")

    monkeypatch.setattr(smoke, "_connect", fail_connect)

    report = smoke.run_static_smoke()

    assert report.status == "passed"
    assert report.mode == "static_dry_run"
    assert report.db_target is None
    assert report.order == [spec.filename for spec in smoke.STACK_SPECS]
    assert set(report.checks) == {spec.filename for spec in smoke.STACK_SPECS}


def test_cli_default_json_is_static_and_lists_all_stack_files(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("default CLI governance smoke must not connect to DB")

    monkeypatch.setattr(smoke, "_connect", fail_connect)

    assert smoke.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "passed"
    assert payload["mode"] == "static_dry_run"
    assert payload["db_target"] is None
    assert payload["order"] == [spec.filename for spec in smoke.STACK_SPECS]
    assert len(payload["files"]) == 6
    for spec in smoke.STACK_SPECS:
        assert str(spec.path) in payload["files"]
        assert payload["checks"][spec.filename]["schema"] == spec.schema


def test_static_smoke_validates_expected_objects_for_each_migration() -> None:
    report = smoke.run_static_smoke()

    model_registry = report.checks["model_registry_phase5_20260509.sql"]
    assert set(model_registry["tables"]) == {
        "model_template",
        "model_spec",
        "model_trial",
        "model_artifact",
        "model_lifecycle_event",
    }
    assert set(model_registry["views"]) == {
        "v_qe_selectable_model_spec",
        "v_model_catalog_compat",
        "v_legacy_aistock_model_catalog_bridge",
    }
    promotion = report.checks["strategy_pkg_promotion_review_20260509.sql"]
    assert set(promotion["tables"]) == {"promotion_review"}
    seed = report.checks["qe_phase4_master_seed_contract_20260509.sql"]
    assert set(seed["tables"]) == {"seed_fragility_score"}
    assert "package.seed_contract_sha256" in seed["alter_columns"]
    runtime = report.checks["strategy_pkg_runtime_variant_20260509.sql"]
    assert set(runtime["tables"]) == {"package_runtime_variant"}
    validation = report.checks["strategy_pkg_validation_run_20260509.sql"]
    assert set(validation["tables"]) == {"package_validation_run"}
    assert "strategy_pkg.package_runtime_variant" in validation["depends_on"]
    assets = report.checks["strategy_pkg_package_asset_20260509.sql"]
    assert set(assets["tables"]) == {"package_asset"}
    assert "package_asset.protected_asset" in assets["alter_columns"]


def test_static_smoke_rejects_missing_stack_file(tmp_path: Path) -> None:
    missing_spec = smoke.MigrationSpec(
        filename="missing_governance_migration.sql",
        phase="missing",
        schema="strategy_pkg",
    )

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="Unexpected migration order"):
        smoke.run_static_smoke((missing_spec,))


def test_spec_validator_rejects_destructive_sql() -> None:
    spec = smoke.STACK_SPECS[1]
    sql = spec.path.read_text(encoding="utf-8") + "\nDROP TABLE strategy_pkg.package;\n"

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="destructive SQL"):
        smoke._validate_spec(spec, sql)


def test_order_validator_rejects_stack_reordering() -> None:
    reordered = (smoke.STACK_SPECS[1], smoke.STACK_SPECS[0], *smoke.STACK_SPECS[2:])

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="Unexpected migration order"):
        smoke.run_static_smoke(reordered)


def test_db_transaction_check_requires_confirmation_and_env_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(smoke.ENV_DB_CHECK_ENABLED, raising=False)
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check="",
        confirm_apply="",
    )
    target = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock_dev", user="postgres")

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="confirm-db-check"):
        smoke._require_db_execution_safety(args, target)

    args.confirm_db_check = smoke.CONFIRM_DB_CHECK
    with pytest.raises(smoke.GovernanceMigrationSmokeError, match=smoke.ENV_DB_CHECK_ENABLED):
        smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_refuses_production_like_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(smoke.ENV_DB_CHECK_ENABLED, "true")
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check=smoke.CONFIRM_DB_CHECK,
        confirm_apply="",
    )
    target = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock", user="postgres")

    with pytest.raises(smoke.GovernanceMigrationSmokeError, match="production-like target"):
        smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_allows_explicit_dev_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(smoke.ENV_DB_CHECK_ENABLED, "true")
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check=smoke.CONFIRM_DB_CHECK,
        confirm_apply="",
    )
    target = smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres")

    smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_rolls_back_and_never_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.last_sql = ""

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc_info: object) -> None:
            return None

        def execute(self, sql: str) -> None:
            self.last_sql = sql

        def fetchone(self) -> tuple[object, ...]:
            if "to_regclass('strategy_pkg.package')" in self.last_sql:
                return ("strategy_pkg.package", "public.aistock_model_catalog")
            raise AssertionError(f"unexpected fetchone SQL: {self.last_sql}")

        def fetchall(self) -> list[tuple[str]]:
            if "information_schema.tables" in self.last_sql and "model_registry" in self.last_sql:
                names = []
                for spec in smoke.STACK_SPECS:
                    if spec.schema == "model_registry":
                        names.extend(spec.tables)
                return [(name,) for name in names]
            if "information_schema.tables" in self.last_sql and "strategy_pkg" in self.last_sql:
                names = []
                for spec in smoke.STACK_SPECS:
                    if spec.schema == "strategy_pkg":
                        names.extend(spec.tables)
                return [(name,) for name in names]
            if "information_schema.views" in self.last_sql:
                names = []
                for spec in smoke.STACK_SPECS:
                    names.extend(spec.views)
                return [(name,) for name in names]
            raise AssertionError(f"unexpected fetchall SQL: {self.last_sql}")

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = True
            self.commit_count = 0
            self.rollback_count = 0
            self.close_count = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commit_count += 1

        def rollback(self) -> None:
            self.rollback_count += 1

        def close(self) -> None:
            self.close_count += 1

    fake_conn = FakeConnection()
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)

    report = smoke.run_db_execution(
        target=smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres"),
        apply=False,
    )

    assert report.mode == "db_transaction_check_rolled_back"
    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 1
    assert fake_conn.close_count == 1
