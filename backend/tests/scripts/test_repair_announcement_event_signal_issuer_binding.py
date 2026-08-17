import datetime as dt
from copy import deepcopy
from pathlib import Path

import pytest

import scripts.repair_announcement_event_signal_issuer_binding as repair
from scripts.repair_announcement_event_signal_issuer_binding import (
    _target_config,
    _target_identity_sha256,
    build_plan_from_batches,
    select_repair_rows,
)


def _row(classification_id: int, status: str, digest: str) -> dict:
    return {
        "classification_id": classification_id,
        "ann_id": classification_id + 100,
        "ts_code": "000001.SZ",
        "source_rule_version": "announcement_rules_v1",
        "time_mode": "backtest",
        "event_type": "stock_delisting_confirmed",
        "risk_level": "P0_BLOCK",
        "effective_trade_date": dt.date(2020, 1, 2),
        "issuer_binding_decision": {
            "binding_digest": digest,
            "status": status,
            "fact_status": "ACTIVE" if status == "EXACT" else "UNKNOWN",
            "signal_status": "ACTIVE" if status == "EXACT" else "SUPPRESSED",
        },
    }


def test_plan_digest_and_counts_are_deterministic() -> None:
    batches = [[_row(1, "EXACT", "a" * 64)], [_row(2, "UNRESOLVED", "b" * 64)]]

    first = build_plan_from_batches(
        batches,
        target="dev",
        target_identity_sha256="1" * 64,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 7, 31),
    )
    second = build_plan_from_batches(
        batches,
        target="dev",
        target_identity_sha256="1" * 64,
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 7, 31),
    )

    assert first.plan_digest == second.plan_digest
    assert first.binding_counts == {"EXACT": 1, "UNRESOLVED": 1}
    assert first.repair_row_count == 1
    assert first.event_type_binding_counts == {
        "stock_delisting_confirmed": {"EXACT": 1, "UNRESOLVED": 1}
    }
    assert first.raw_rows_deleted == 0
    assert first.signal_rows_deleted == 0


def test_plan_digest_binds_target_and_every_repair_write_input() -> None:
    source = _row(2, "UNRESOLVED", "b" * 64)
    changed = deepcopy(source)
    changed["risk_level"] = "P1_WARN"

    base = build_plan_from_batches(
        [[source]],
        target="dev",
        target_identity_sha256="1" * 64,
        start_date=None,
        end_date=dt.date(2026, 7, 31),
    )
    changed_business_input = build_plan_from_batches(
        [[changed]],
        target="dev",
        target_identity_sha256="1" * 64,
        start_date=None,
        end_date=dt.date(2026, 7, 31),
    )
    changed_target = build_plan_from_batches(
        [[source]],
        target="production",
        target_identity_sha256="2" * 64,
        start_date=None,
        end_date=dt.date(2026, 7, 31),
    )

    assert base.plan_digest != changed_business_input.plan_digest
    assert base.plan_digest != changed_target.plan_digest


def test_dev_target_refuses_non_dev_identity(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                "TDX_DB_DEV_HOST=127.0.0.1",
                "TDX_DB_DEV_PORT=5432",
                "TDX_DB_DEV_NAME=aistock",
                "TDX_DB_DEV_USER=postgres",
                "TDX_DB_DEV_PASSWORD=secret",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="BUG1114_DEV_TARGET_IDENTITY_INVALID"):
        _target_config(env, "dev")


def test_target_config_never_returns_env_key_names_as_password(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                "TDX_DB_DEV_HOST=127.0.0.1",
                "TDX_DB_DEV_PORT=5433",
                "TDX_DB_DEV_NAME=aistock_dev",
                "TDX_DB_DEV_USER=postgres",
                "TDX_DB_DEV_PASSWORD=secret",
            ]
        ),
        encoding="utf-8",
    )

    config = _target_config(env, "dev")

    assert config["dbname"] == "aistock_dev"
    assert config["port"] == 5433
    assert "TDX_DB_DEV_PASSWORD" not in config.values()


def test_production_target_refuses_dev_identity(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                "TDX_DB_HOST=127.0.0.1",
                "TDX_DB_PORT=5433",
                "TDX_DB_NAME=aistock_dev",
                "TDX_DB_USER=postgres",
                "TDX_DB_PASSWORD=secret",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="BUG1114_PRODUCTION_TARGET_IDENTITY_INVALID"):
        _target_config(env, "production")


def test_repair_write_set_excludes_exact_rows_and_keeps_all_fail_closed_rows() -> None:
    exact = _row(1, "EXACT", "a" * 64)
    unresolved = _row(2, "UNRESOLVED", "b" * 64)
    terminal_unconfirmed = _row(3, "TERMINAL_EVIDENCE_UNCONFIRMED", "c" * 64)

    selected = select_repair_rows([exact, unresolved, terminal_unconfirmed])

    assert [row["classification_id"] for row in selected] == [2, 3]


class _FakeConnection:
    def __init__(self) -> None:
        self.session_calls: list[dict] = []
        self.rollback_calls = 0
        self.commit_calls = 0
        self.closed = False

    def set_session(self, **kwargs) -> None:
        self.session_calls.append(kwargs)

    def rollback(self) -> None:
        self.rollback_calls += 1

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


def _dev_env(tmp_path) -> str:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                "TDX_DB_DEV_HOST=127.0.0.1",
                "TDX_DB_DEV_PORT=5433",
                "TDX_DB_DEV_NAME=aistock_dev",
                "TDX_DB_DEV_USER=postgres",
                "TDX_DB_DEV_PASSWORD=secret",
            ]
        ),
        encoding="utf-8",
    )
    return str(env)


def _empty_plan() -> repair.RepairPlan:
    return build_plan_from_batches(
        [],
        target="dev",
        target_identity_sha256="1" * 64,
        start_date=None,
        end_date=dt.date(2026, 7, 31),
    )


def test_apply_rejects_confirmation_before_planning_or_writing(tmp_path, monkeypatch) -> None:
    plan_called = False
    connect_called = False

    def unexpected_plan(*_args, **_kwargs):
        nonlocal plan_called
        plan_called = True
        return _empty_plan()

    def unexpected_connect(**_kwargs):
        nonlocal connect_called
        connect_called = True
        raise AssertionError("invalid confirmation must be rejected before DB connect")

    monkeypatch.setattr(repair.psycopg2, "connect", unexpected_connect)
    monkeypatch.setattr(repair, "_build_plan", unexpected_plan)

    with pytest.raises(RuntimeError, match="BUG1114_APPLY_CONFIRMATION_INVALID"):
        repair.main(
            [
                "apply",
                "--target",
                "dev",
                "--env-file",
                _dev_env(tmp_path),
                "--confirm",
                "WRONG",
                "--expected-plan-digest",
                "0" * 64,
            ]
        )

    assert plan_called is False
    assert connect_called is False


def test_apply_rejects_plan_drift_before_write(tmp_path, monkeypatch) -> None:
    connection = _FakeConnection()
    apply_called = False
    env_file = _dev_env(tmp_path)
    identity = _target_identity_sha256(_target_config(Path(env_file), "dev"))

    def unexpected_apply(*_args, **_kwargs):
        nonlocal apply_called
        apply_called = True
        return {}

    monkeypatch.setattr(repair.psycopg2, "connect", lambda **_kwargs: connection)
    monkeypatch.setattr(repair, "_build_plan", lambda *_args, **_kwargs: _empty_plan())
    monkeypatch.setattr(repair, "_apply", unexpected_apply)

    with pytest.raises(RuntimeError, match="BUG1114_PLAN_DIGEST_DRIFT"):
        repair.main(
            [
                "apply",
                "--target",
                "dev",
                "--env-file",
                env_file,
                "--confirm",
                repair.CONFIRMATIONS["dev"],
                "--expected-plan-digest",
                "f" * 64,
                "--expected-target-identity-sha256",
                identity,
            ]
        )

    assert apply_called is False
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 1
    assert connection.closed is True


def test_apply_rejects_target_identity_drift_before_connect(tmp_path, monkeypatch) -> None:
    connect_called = False

    def unexpected_connect(**_kwargs):
        nonlocal connect_called
        connect_called = True
        raise AssertionError("target drift must be rejected before DB connect")

    monkeypatch.setattr(repair.psycopg2, "connect", unexpected_connect)

    with pytest.raises(RuntimeError, match="BUG1114_TARGET_IDENTITY_DRIFT"):
        repair.main(
            [
                "apply",
                "--target",
                "dev",
                "--env-file",
                _dev_env(tmp_path),
                "--confirm",
                repair.CONFIRMATIONS["dev"],
                "--expected-plan-digest",
                "f" * 64,
                "--expected-target-identity-sha256",
                "0" * 64,
            ]
        )

    assert connect_called is False


def test_plan_is_read_only_repeatable_read_and_never_commits(tmp_path, monkeypatch, capsys) -> None:
    connection = _FakeConnection()
    monkeypatch.setattr(repair.psycopg2, "connect", lambda **_kwargs: connection)
    monkeypatch.setattr(repair, "_build_plan", lambda *_args, **_kwargs: _empty_plan())

    assert repair.main(
        ["plan", "--target", "dev", "--env-file", _dev_env(tmp_path)]
    ) == 0

    assert connection.session_calls == [
        {"readonly": True, "isolation_level": "REPEATABLE READ"}
    ]
    assert connection.rollback_calls == 1
    assert connection.commit_calls == 0
    assert connection.closed is True
    assert '"apply_status":"not_requested"' in capsys.readouterr().out
