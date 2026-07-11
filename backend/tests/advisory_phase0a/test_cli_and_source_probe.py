from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
from types import SimpleNamespace

import scripts.advisory_phase0a_audit as cli

from backend.services.advisory_phase0a.resolvers import PostgresReadOnlySourceProbe


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
    monkeypatch.setattr(cli, "_load_env_file", lambda _path: None)
    monkeypatch.setattr(cli, "_db_config", lambda *, target_db: {"host": "localhost"})
    monkeypatch.setattr(cli.psycopg2, "connect", lambda **_config: connection)

    with cli._env_conn_factory(env_file=tmp_path / "missing.env", target_db=cli.TARGET_PROD) as got:  # noqa: SLF001
        assert got is connection

    assert connection.session_args == {"readonly": True, "autocommit": False}
    assert connection.cursor_instance.executed == [("SET LOCAL statement_timeout = %s", (cli.READ_ONLY_STATEMENT_TIMEOUT_MS,))]
    assert connection.closed is True


def test_cli_validates_request_without_opening_database(tmp_path, capsys) -> None:
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

    assert cli.main(["--request", str(request_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "validated_only"


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
    monkeypatch.setattr(cli, "write_receipt_artifacts", lambda **_kwargs: tmp_path / "receipt")

    assert cli.main(["--request", str(request_path), "--execute-readonly"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "read_only_audit"
    assert payload["audit_id"] == "audit_execute_without_approval"
