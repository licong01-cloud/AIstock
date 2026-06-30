from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

from backend.services.strategy_package.package_asset_backfill import (
    STATUS_PLANNED_FREEZE,
    STATUS_UNRECOVERABLE,
    PackageAssetBackfillItem,
    PackageAssetBackfillPlan,
)
from backend.services.trading_core.errors import PackageAssetInvalidError
from scripts import strategy_package_asset_backfill as cli


class FakeBackfillService:
    def __init__(self, plan: PackageAssetBackfillPlan) -> None:
        self.plan = plan
        self.apply_calls: list[str] = []
        self.build_kwargs = {}

    def build_plan(self, **kwargs):  # noqa: ANN001, ANN201
        self.build_kwargs = dict(kwargs)
        return self.plan

    def apply_plan(self, plan, *, operator: str):  # noqa: ANN001, ANN201
        assert plan is self.plan
        self.apply_calls.append(operator)
        return PackageAssetBackfillPlan(
            items=[
                PackageAssetBackfillItem(
                    package_id="pkg_ok",
                    package_name="ok",
                    alpha_mode="single_alpha",
                    old_manifest_sha256="a" * 64,
                    new_manifest_sha256="b" * 64,
                    status="applied",
                    asset_count=2,
                )
            ],
            mode="apply",
        )


def test_prod_apply_requires_flag_and_env_token(monkeypatch: pytest.MonkeyPatch) -> None:
    args = cli.argparse.Namespace(apply=True, target_db=cli.TARGET_PROD, confirm_production_dml=False, confirm_scratch_dml=False)

    with pytest.raises(cli.AssetBackfillScriptError, match="confirm-production-dml"):
        cli._validate_apply_gate(args)  # noqa: SLF001

    args.confirm_production_dml = True
    monkeypatch.delenv(cli.APPLY_CONFIRM_ENV, raising=False)
    with pytest.raises(cli.AssetBackfillScriptError, match=cli.APPLY_CONFIRM_ENV):
        cli._validate_apply_gate(args)  # noqa: SLF001

    monkeypatch.setenv(cli.APPLY_CONFIRM_ENV, cli.APPLY_CONFIRM_VALUE)
    cli._validate_apply_gate(args)  # noqa: SLF001


def test_dev_apply_requires_scratch_confirm() -> None:
    args = cli.argparse.Namespace(apply=True, target_db=cli.TARGET_DEV, confirm_production_dml=False, confirm_scratch_dml=False)

    with pytest.raises(cli.AssetBackfillScriptError, match="confirm-scratch-dml"):
        cli._validate_apply_gate(args)  # noqa: SLF001

    args.confirm_scratch_dml = True
    cli._validate_apply_gate(args)  # noqa: SLF001


def test_apply_gate_noop_for_dry_run() -> None:
    args = cli.argparse.Namespace(apply=False, target_db=cli.TARGET_PROD, confirm_production_dml=False, confirm_scratch_dml=False)

    cli._validate_apply_gate(args)  # noqa: SLF001


def test_prod_db_config_reports_missing_and_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("TDX_DB_HOST", "TDX_DB_PORT", "TDX_DB_NAME", "TDX_DB_USER", "TDX_DB_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(cli.AssetBackfillScriptError, match="missing database environment keys"):
        cli._db_config(target_db=cli.TARGET_PROD)  # noqa: SLF001

    monkeypatch.setenv("TDX_DB_HOST", "db.example")
    monkeypatch.setenv("TDX_DB_PORT", "5432")
    monkeypatch.setenv("TDX_DB_NAME", "aistock")
    monkeypatch.setenv("TDX_DB_USER", "robot")
    monkeypatch.setenv("TDX_DB_PASSWORD", "secret")
    cfg = cli._db_config(target_db=cli.TARGET_PROD)  # noqa: SLF001
    meta = cli._target_metadata(cfg, target_db=cli.TARGET_PROD)  # noqa: SLF001

    assert cfg["port"] == 5432
    assert meta == {
        "target_db": "prod",
        "host": "db.example",
        "port": 5432,
        "dbname": "aistock",
        "user": "robot",
        "password_configured": True,
    }


def test_dev_db_config_refuses_non_local_or_non_scratch(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "TDX_DB_DEV_HOST": "10.0.0.1",
        "TDX_DB_DEV_PORT": "5432",
        "TDX_DB_DEV_NAME": "aistock",
        "TDX_DB_DEV_USER": "u",
        "TDX_DB_DEV_PASSWORD": "p",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    with pytest.raises(cli.AssetBackfillScriptError, match="local scratch/dev DB"):
        cli._db_config(target_db=cli.TARGET_DEV)  # noqa: SLF001

    monkeypatch.setenv("TDX_DB_DEV_HOST", "127.0.0.1")
    monkeypatch.setenv("TDX_DB_DEV_NAME", "aistock_scratch")
    cfg = cli._db_config(target_db=cli.TARGET_DEV)  # noqa: SLF001

    assert cfg["host"] == "127.0.0.1"
    assert cfg["dbname"] == "aistock_scratch"


def test_dev_db_config_reports_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("TDX_DB_DEV_HOST", "TDX_DB_DEV_PORT", "TDX_DB_DEV_NAME", "TDX_DB_DEV_USER", "TDX_DB_DEV_PASSWORD"):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(cli.AssetBackfillScriptError, match="missing dev database environment keys"):
        cli._db_config(target_db=cli.TARGET_DEV)  # noqa: SLF001


def test_dry_run_store_does_not_write_new_blob(tmp_path) -> None:  # noqa: ANN001
    root = tmp_path / "asset_store"
    store = cli.DryRunPackageAssetStore(cli.LocalPackageAssetStore(root))

    blob = store.put(b"payload", kind="factor_code")

    assert blob.uri.startswith("aistock-package-asset://blobs/")
    assert store.exists(blob.uri)
    assert store.get(blob.uri) == b"payload"
    assert store.exists(blob.uri + "?kind=factor_code&logical_name=factor")
    assert store.get(blob.uri + "?kind=factor_code&logical_name=factor") == b"payload"
    assert list(root.rglob("*")) == [] if root.exists() else True


def test_dry_run_store_validates_sha_and_delegates_existing_blob(tmp_path) -> None:  # noqa: ANN001
    delegate = cli.LocalPackageAssetStore(tmp_path / "asset_store")
    existing = delegate.put(b"existing", kind="model_weight")
    store = cli.DryRunPackageAssetStore(delegate)

    assert store.exists(existing.uri)
    assert store.get(existing.uri) == b"existing"
    with pytest.raises(PackageAssetInvalidError) as excinfo:
        store.put(b"payload", kind="factor_code", sha256="0" * 64)
    assert excinfo.value.context["reason_code"] == "strategy_package_asset_sha_mismatch"
    assert excinfo.value.context["dry_run"] is True


def test_env_loader_does_not_override_existing_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ANN001
    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_HOST=file-host\n", encoding="utf-8")
    monkeypatch.setenv("TDX_DB_HOST", "existing-host")

    cli._load_env_file(env_file)  # noqa: SLF001

    assert os.environ["TDX_DB_HOST"] == "existing-host"


def test_env_loader_ignores_missing_comments_and_malformed_lines(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: ANN001
    cli._load_env_file(None)  # noqa: SLF001
    cli._load_env_file(tmp_path / "missing.env")  # noqa: SLF001
    env_file = tmp_path / ".env"
    env_file.write_text("\n# comment\nBAD\nTDX_DB_NAME='scratch'\n", encoding="utf-8")
    monkeypatch.delenv("TDX_DB_NAME", raising=False)

    cli._load_env_file(env_file)  # noqa: SLF001

    assert os.environ["TDX_DB_NAME"] == "scratch"


def test_env_conn_factory_sets_readonly_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    class Conn:
        def __init__(self) -> None:
            self.session_args = None
            self.closed = False

        def set_session(self, **kwargs):  # noqa: ANN001, ANN201
            self.session_args = kwargs

        def close(self) -> None:
            self.closed = True

    conn = Conn()
    monkeypatch.setattr(cli, "_db_config", lambda *, target_db: {"host": "h"})  # noqa: SLF001
    monkeypatch.setattr(cli.psycopg2, "connect", lambda **_cfg: conn)

    with cli._env_conn_factory(env_file=None, target_db=cli.TARGET_PROD, readonly=True) as got:  # noqa: SLF001
        assert got is conn

    assert conn.session_args == {"readonly": True, "autocommit": True}
    assert conn.closed is True


def test_repo_and_service_factories_wire_readonly_store(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def fake_conn_factory(**_kwargs):  # noqa: ANN001
        yield object()

    monkeypatch.setattr(cli, "_env_conn_factory", fake_conn_factory)  # noqa: SLF001

    repo = cli._repo_from_env(env_file=None, target_db=cli.TARGET_PROD, readonly=True)  # noqa: SLF001
    service = cli._service_from_env(env_file=None, target_db=cli.TARGET_PROD, readonly=True)  # noqa: SLF001

    assert repo is not None
    assert isinstance(service.asset_freezer.asset_store, cli.DryRunPackageAssetStore)


def test_build_report_dry_run_apply_and_apply_blocked() -> None:
    ok_plan = PackageAssetBackfillPlan(
        items=[
            PackageAssetBackfillItem(
                package_id="pkg_ok",
                package_name="ok",
                alpha_mode="single_alpha",
                old_manifest_sha256="a" * 64,
                status=STATUS_PLANNED_FREEZE,
                asset_count=2,
            )
        ]
    )
    service = FakeBackfillService(ok_plan)

    dry = cli.build_report(
        service,
        mode="dry_run",
        limit=7,
        target={"target_db": "dev"},
        package_ids=["pkg_ok"],
        package_id_prefix="pkg_",
        operator="unit",
    )
    applied = cli.build_report(service, mode="apply", limit=7, target={}, operator="unit")

    assert dry["mode"] == "dry_run"
    assert dry["filter"] == {"package_ids": ["pkg_ok"], "package_id_prefix": "pkg_"}
    assert service.build_kwargs["limit"] == 7
    assert applied["mode"] == "apply"
    assert service.apply_calls == ["unit"]

    blocked_plan = PackageAssetBackfillPlan(
        items=[
            PackageAssetBackfillItem(
                package_id="pkg_bad",
                package_name="bad",
                alpha_mode="single_alpha",
                old_manifest_sha256=None,
                status=STATUS_UNRECOVERABLE,
                reason_code="missing",
            )
        ]
    )
    blocked = cli.build_report(FakeBackfillService(blocked_plan), mode="apply", limit=1, operator="unit")

    assert blocked["mode"] == "apply_blocked"
    assert blocked["apply_blocked_reason"] == "unrecoverable_packages_present"


def test_parse_args_and_main_write_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--limit", "3", "--target-db", "dev", "--package-id", "pkg_1", "--output", str(output)],
    )
    args = cli._parse_args()  # noqa: SLF001
    assert args.limit == 3
    assert args.package_id == ["pkg_1"]

    service = FakeBackfillService(PackageAssetBackfillPlan(items=[]))
    monkeypatch.setattr(cli, "_db_config", lambda *, target_db: {"host": "127.0.0.1", "port": 5432, "dbname": "scratch", "user": "u", "password": "p"})  # noqa: SLF001
    monkeypatch.setattr(cli, "_service_from_env", lambda **_kwargs: service)  # noqa: SLF001

    code = cli.main()

    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8"))["mode"] == "dry_run"
    assert '"mode": "dry_run"' in capsys.readouterr().out


def test_main_rejects_bad_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "--limit", "0"])

    with pytest.raises(cli.AssetBackfillScriptError, match="limit must be positive"):
        cli.main()


def test_main_returns_two_when_unrecoverable(monkeypatch: pytest.MonkeyPatch) -> None:
    bad_plan = PackageAssetBackfillPlan(
        items=[
            PackageAssetBackfillItem(
                package_id="pkg_bad",
                package_name="bad",
                alpha_mode="single_alpha",
                old_manifest_sha256=None,
                status=STATUS_UNRECOVERABLE,
                reason_code="missing",
            )
        ]
    )
    monkeypatch.setattr(sys, "argv", ["prog"])
    monkeypatch.setattr(cli, "_db_config", lambda *, target_db: {"host": "h", "port": 1, "dbname": "d", "user": "u", "password": "p"})  # noqa: SLF001
    monkeypatch.setattr(cli, "_service_from_env", lambda **_kwargs: FakeBackfillService(bad_plan))  # noqa: SLF001

    assert cli.main() == 2
