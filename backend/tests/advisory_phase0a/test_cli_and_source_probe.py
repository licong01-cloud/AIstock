from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import hashlib
import json
from types import SimpleNamespace

import pytest

import scripts.advisory_phase0a_audit as cli

from backend.services.advisory_phase0a.resolvers import PostgresReadOnlySourceProbe
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> tuple[date]:
        return (date(2026, 1, 5),)

    def fetchall(self) -> list[tuple[date]]:
        return [(date(2026, 1, 5),), (date(2026, 1, 6),)]


class _Connection:
    def __init__(self) -> None:
        self.cursor_instance = _Cursor()
        self.session_args: dict[str, object] | None = None
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def set_session(self, **kwargs: object) -> None:
        self.session_args = kwargs

    def close(self) -> None:
        self.closed = True


def test_source_probe_is_fixed_select_allowlist() -> None:
    connection = _Connection()

    @contextmanager
    def factory():
        yield connection

    rows = PostgresReadOnlySourceProbe(factory).probe(decision_date=date(2026, 1, 5))
    trading_days = PostgresReadOnlySourceProbe(factory).list_trading_days(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 6),
    )

    assert len(rows) == 5
    assert trading_days == [date(2026, 1, 5), date(2026, 1, 6)]
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _params in connection.cursor_instance.executed)
    assert all(row.status.value == "PARTIAL" for row in rows)


def test_cli_connection_factory_sets_readonly_and_closes(monkeypatch, tmp_path) -> None:
    connection = _Connection()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TDX_DB_HOST=prod-host\nTDX_DB_PORT=5432\nTDX_DB_NAME=aistock\nTDX_DB_USER=user\nTDX_DB_PASSWORD=secret\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli.psycopg2, "connect", lambda **_config: connection)

    with cli._env_conn_factory(env_file=env_file, target_db=cli.TARGET_PROD) as got:  # noqa: SLF001
        assert got is connection

    assert connection.session_args == {"readonly": True, "autocommit": False}
    assert connection.cursor_instance.executed == [("SET LOCAL statement_timeout = %s", (cli.READ_ONLY_STATEMENT_TIMEOUT_MS,))]
    assert connection.closed is True


def test_cli_dev_config_uses_explicit_env_without_localhost_or_name_guess() -> None:
    values = {
        "TDX_DB_DEV_HOST": "10.20.30.40",
        "TDX_DB_DEV_PORT": "5544",
        "TDX_DB_DEV_NAME": "research_catalog",
        "TDX_DB_DEV_USER": "research_user",
        "TDX_DB_DEV_PASSWORD": "secret",
    }
    config = cli._db_config(target_db=cli.TARGET_DEV, env_values=values)  # noqa: SLF001

    assert config["host"] == "10.20.30.40"
    assert config["port"] == 5544
    assert config["dbname"] == "research_catalog"


def test_cli_missing_env_file_does_not_fall_back_to_process_environment(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TDX_DB_HOST", "inherited-host-must-not-be-used")

    with pytest.raises(cli.AdvisoryPhase0ACommandError, match="does not exist"):
        cli._load_env_file(tmp_path / "missing.env")  # noqa: SLF001


def test_cli_dev_connection_verifies_exact_database_identity(monkeypatch, tmp_path) -> None:
    config = {
        "host": "10.20.30.40",
        "port": 5544,
        "dbname": "research_catalog",
        "user": "research_user",
        "password": "secret",
    }
    environment_hash = cli.canonical_json_sha256(
        {
            "target_label": TargetLabel.DEV.value,
            "host": config["host"],
            "port": config["port"],
            "database": config["dbname"],
            "user": config["user"],
        }
    )
    identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database=config["dbname"],
        server_address=config["host"],
        server_port=config["port"],
        server_version_num=160000,
        current_user_hash=hashlib.sha256(config["user"].encode("utf-8")).hexdigest(),
        environment_contract_hash=environment_hash,
    )

    class Cursor(_Cursor):
        def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
            self.executed.append((sql, params))

        def fetchone(self):
            return (
                identity.current_database,
                identity.server_address,
                identity.server_port,
                identity.server_version_num,
                config["user"],
                "on",
            )

    connection = _Connection()
    connection.cursor_instance = Cursor()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                f"TDX_DB_DEV_HOST={config['host']}",
                f"TDX_DB_DEV_PORT={config['port']}",
                f"TDX_DB_DEV_NAME={config['dbname']}",
                f"TDX_DB_DEV_USER={config['user']}",
                f"TDX_DB_DEV_PASSWORD={config['password']}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli.psycopg2, "connect", lambda **_config: connection)

    with cli._env_conn_factory(  # noqa: SLF001
        env_file=env_file,
        target_db=cli.TARGET_DEV,
        expected_database_identity_hash=cli.database_identity_hash(identity),
    ) as got:
        assert got is connection

    assert connection.closed is True


def test_cli_audit_receipt_store_is_content_addressed_and_idempotent(monkeypatch, tmp_path) -> None:
    receipt = SimpleNamespace(audit_id="audit-o3", audit_manifest_hash="a" * 64)
    payloads = {"receipt.json": {"ok": True}, "summary.md": "summary\n"}
    monkeypatch.setattr(cli, "receipt_artifact_payloads", lambda **_kwargs: payloads)

    request = SimpleNamespace()
    policy = SimpleNamespace()

    first, first_idempotent = cli._write_content_addressed_receipt(  # noqa: SLF001
        receipt=receipt,
        request=request,
        policy=policy,
        output_root=tmp_path,
    )
    second, second_idempotent = cli._write_content_addressed_receipt(  # noqa: SLF001
        receipt=receipt,
        request=request,
        policy=policy,
        output_root=tmp_path,
    )

    assert first == second
    assert first.parts[-4:-1] == ("audit-receipts", "aa", "a" * 64)
    assert first_idempotent is False
    assert second_idempotent is True

    (first / "unexpected.txt").write_text("unexpected", encoding="utf-8")
    with pytest.raises(cli.AdvisoryPhase0ACommandError, match="file closure differs"):
        cli._write_content_addressed_receipt(  # noqa: SLF001
            receipt=receipt,
            request=request,
            policy=policy,
            output_root=tmp_path,
        )


def test_cli_audit_receipt_store_rejects_path_escape(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cli, "receipt_artifact_payloads", lambda **_kwargs: {"receipt.json": {"ok": True}})

    with pytest.raises(cli.AdvisoryPhase0ACommandError, match="safe path segment"):
        cli._write_content_addressed_receipt(  # noqa: SLF001
            receipt=SimpleNamespace(audit_id="../escape", audit_manifest_hash="a" * 64),
            request=SimpleNamespace(),
            policy=SimpleNamespace(),
            output_root=tmp_path,
        )


def test_cli_rejects_audit_without_explicit_env_and_output_root(tmp_path, capsys) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(
            {
                "audit_id": "audit_validation_only",
                "policy_registry_id": "advisory_phase0a",
                "audit_policy_version": "v1",
                "policy_registry_content_hash": "68538d81784294f9b6a6d09c46df274438fb1f34ce0ff5d6da68cb3dbdf86d64",
                "targets": [
                    {
                        "audit_target_id": "target_1",
                        "program_id": "program_1",
                        "package_id": "package_1",
                        "manifest_sha256": "a" * 64,
                        "expected_alpha_mode": "single_alpha",
                        "decision_date_range": {"start_date": "2026-02-05", "end_date": "2026-02-05"},
                        "decision_dates": ["2026-02-05"],
                        "selection_evidence_ids_by_decision_date": {"2026-02-05": "dse_1"},
                        "style_family": "SHORT_REBOUND",
                        "audit_policy_version": "v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert cli.main(["--request", str(request_path)]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["ok"] is False
    assert "explicit --env-file and --output-root" in payload["error"]


def test_cli_validates_repo_tracked_policy_registry(capsys) -> None:
    assert cli.main(
        [
            "validate-policy-registry",
            "--policy-registry-id",
            "advisory_phase0a",
            "--policy-version",
            "v1",
        ]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "policy_registry_validated"
    assert payload["registry_content_hash"] == "68538d81784294f9b6a6d09c46df274438fb1f34ce0ff5d6da68cb3dbdf86d64"


def test_cli_has_no_acknowledgement_or_approval_gate() -> None:
    help_text = cli.build_parser().format_help()
    assert "--execute-readonly" not in help_text
    assert "--confirm" not in help_text
    assert "--approval" not in help_text


def test_cli_execute_uses_versioned_request_without_approval_reference(monkeypatch, tmp_path, capsys) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text(
        json.dumps(
            {
                "audit_id": "audit_execute_without_approval",
                "policy_registry_id": "advisory_phase0a",
                "audit_policy_version": "v1",
                "policy_registry_content_hash": "68538d81784294f9b6a6d09c46df274438fb1f34ce0ff5d6da68cb3dbdf86d64",
                "targets": [
                    {
                        "audit_target_id": "target_1",
                        "program_id": "program_1",
                        "package_id": "package_1",
                        "manifest_sha256": "a" * 64,
                        "expected_alpha_mode": "single_alpha",
                        "decision_date_range": {"start_date": "2026-02-05", "end_date": "2026-02-05"},
                        "decision_dates": ["2026-02-05"],
                        "selection_evidence_ids_by_decision_date": {"2026-02-05": "dse_1"},
                        "style_family": "SHORT_REBOUND",
                        "audit_policy_version": "v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    class _Service:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def audit(self, request: object) -> object:
            return SimpleNamespace(
                audit_id=getattr(request, "audit_id"),
                audit_manifest_hash="b" * 64,
            )

    monkeypatch.setattr(cli, "_readers_from_env", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "AdvisoryPhase0AAuditService", _Service)
    monkeypatch.setattr(
        cli,
        "_write_content_addressed_receipt",
        lambda **_kwargs: (tmp_path / "receipt", False),
    )
    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_HOST=unused\n", encoding="utf-8")
    output_root = tmp_path / "audit-output"

    assert cli.main(
        [
            "--request",
            str(request_path),
            "--env-file",
            str(env_file),
            "--output-root",
            str(output_root),
        ]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "read_only_audit"
    assert payload["audit_id"] == "audit_execute_without_approval"
