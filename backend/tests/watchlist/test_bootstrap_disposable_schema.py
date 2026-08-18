from __future__ import annotations

from contextlib import contextmanager

import pytest

from backend.tests.watchlist import bootstrap_disposable_schema as bootstrap


_VALID_ENV = {
    "GITHUB_ACTIONS": "true",
    "AISTOCK_CI_DISPOSABLE_POSTGRES": "1",
    "TDX_DB_HOST": "127.0.0.1",
    "TDX_DB_PORT": "5433",
    "TDX_DB_NAME": "aistock_dev",
    "TDX_DB_USER": "aistock",
}


def _set_valid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _VALID_ENV.items():
        monkeypatch.setenv(key, value)


@pytest.mark.parametrize(
    ("key", "value", "message"),
    (
        ("GITHUB_ACTIONS", "false", "GitHub Actions"),
        ("AISTOCK_CI_DISPOSABLE_POSTGRES", "0", "explicit disposable target"),
        ("TDX_DB_HOST", "database.internal", "loopback"),
        ("TDX_DB_PORT", "5432", "disposable PostgreSQL port"),
        ("TDX_DB_NAME", "aistock", "fixed disposable database"),
        ("TDX_DB_USER", "postgres", "fixed disposable user"),
    ),
)
def test_disposable_target_contract_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: str,
    message: str,
) -> None:
    _set_valid_env(monkeypatch)
    monkeypatch.setenv(key, value)

    with pytest.raises(RuntimeError, match=message):
        bootstrap._require_disposable_ci_target()


def test_bootstrap_uses_repository_schema_and_test_only_analysis_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_env(monkeypatch)
    calls: list[str] = []

    class Cursor:
        def execute(self, sql: str) -> None:
            calls.append(sql)

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    @contextmanager
    def connection():
        yield Connection()

    monkeypatch.setattr(bootstrap, "init_watchlist_schema", lambda: calls.append("watchlist_schema"))
    monkeypatch.setattr(bootstrap, "get_conn", connection)

    bootstrap.bootstrap_disposable_schema()

    assert calls[0] == "watchlist_schema"
    assert "CREATE TABLE IF NOT EXISTS app.analysis_records" in calls[1]
