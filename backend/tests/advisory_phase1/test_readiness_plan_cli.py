from __future__ import annotations

import json

import scripts.advisory_phase1e_readiness_plan as cli
from backend.services.advisory_phase1.readiness_plan_store import ContentAddressedPlanStore


HASH = "a" * 64


def test_verify_and_inspect_cli_use_configured_external_store(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setenv("AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT", str(tmp_path))
    store = ContentAddressedPlanStore.from_environment(policy_hash=HASH)
    store.publish(kind="plan", identity=HASH, payload={"plan": "research-only"}, semantic_hash=HASH)

    assert cli.main([
        "verify-plan",
        "--kind", "plan",
        "--identity", HASH,
        "--semantic-hash", HASH,
        "--artifact-store-policy-hash", HASH,
    ]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    assert cli.main([
        "inspect-plan",
        "--kind", "plan",
        "--identity", HASH,
        "--semantic-hash", HASH,
        "--artifact-store-policy-hash", HASH,
    ]) == 0
    assert json.loads(capsys.readouterr().out)["payload"] == {"plan": "research-only"}


def test_readonly_connection_sets_session_before_any_sql(monkeypatch) -> None:
    class Connection:
        def __init__(self) -> None:
            self.session: dict[str, object] | None = None
            self.closed = False

        def set_session(self, **kwargs) -> None:
            self.session = kwargs

        def cursor(self):
            raise AssertionError("connection setup must not execute SQL before projection snapshot setup")

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(cli.psycopg2, "connect", lambda **_kwargs: connection)
    for name, value in {
        "TDX_DB_DEV_HOST": "host",
        "TDX_DB_DEV_PORT": "5432",
        "TDX_DB_DEV_NAME": "db",
        "TDX_DB_DEV_USER": "user",
        "TDX_DB_DEV_PASSWORD": "password",
    }.items():
        monkeypatch.setenv(name, value)

    with cli._readonly_connection(env_file=None, target_db="dev") as opened:
        assert opened is connection

    assert connection.session == {"readonly": True, "autocommit": False, "isolation_level": "REPEATABLE READ"}
    assert connection.closed is True


def test_compile_batch_returns_nonzero_for_reported_scope_failures(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "_compile",
        lambda _args: {"ok": False, "status": "partial", "failed_input_scope_count": 1, "failed_input_scopes": [{"reason_code": "missing"}]},
    )

    result = cli.main(
        [
            "compile-batch",
            "--request",
            "request.json",
            "--source-requirement-registry",
            "registry.json",
            "--capacity-request",
            "capacity-request.json",
            "--capacity-receipt",
            "capacity-receipt.json",
            "--policy-registry-id",
            "policy",
            "--policy-version",
            "v1",
        ]
    )

    assert result == 3
    assert json.loads(capsys.readouterr().out)["status"] == "partial"
