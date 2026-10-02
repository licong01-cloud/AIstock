from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from backend.services.simulation_runtime.localsim_cutover_inventory import LocalSimLegacyInventoryReader
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError


ACCOUNT_ID = "paper_keep_1"
AUTHORITY_DATE = date(2026, 8, 31)


def _row(
    *,
    binding_id: str,
    effective_from: date,
    binding_manifest_sha256: str = "b" * 64,
    release_manifest_sha256: str = "b" * 64,
    admission_receipt_id: str | None = "receipt_keep",
) -> dict[str, Any]:
    return {
        "legacy_account_id": ACCOUNT_ID,
        "account_name": "Retained LocalSIM",
        "portfolio_package_id": "pkg_keep",
        "portfolio_manifest_sha256": "a" * 64,
        "initial_cash": 1_000_000.0,
        "portfolio_status": "READY",
        "auto_run_enabled": False,
        "release_id": f"release_{binding_id}",
        "release_package_id": "pkg_keep",
        "release_manifest_sha256": release_manifest_sha256,
        "release_hash": "c" * 64,
        "binding_id": binding_id,
        "binding_package_id": "pkg_keep",
        "binding_manifest_sha256": binding_manifest_sha256,
        "binding_release_hash": "c" * 64,
        "binding_hash": "d" * 64,
        "broker_account_id": ACCOUNT_ID,
        "binding_config_json": {
            "metadata": {"admission_receipt_id": admission_receipt_id} if admission_receipt_id else {}
        },
        "binding_effective_from": effective_from,
        "binding_effective_to": effective_from,
        "binding_created_at": datetime.combine(effective_from, datetime.min.time(), tzinfo=UTC),
        "ledger_scope_id": ACCOUNT_ID,
        "scope_kind": "LEGACY_PORTFOLIO",
        "source_identity": ACCOUNT_ID,
        "native_account_id": None,
    }


class _Cursor:
    def __init__(self, inventory_rows: list[dict[str, Any]], daily_payload: dict[str, Any] | None = None) -> None:
        self.inventory_rows = inventory_rows
        self.daily_payload = daily_payload
        self.rows: list[dict[str, Any]] = []
        self.inventory_params: tuple[list[str], date] | None = None
        self.executed_queries: list[str] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        normalized = " ".join(query.split())
        self.executed_queries.append(normalized)
        if normalized.startswith("SET TRANSACTION"):
            self.rows = []
        elif "SELECT portfolio.portfolio_id AS legacy_account_id" in normalized:
            assert params is not None
            self.inventory_params = (list(params[0]), params[1])
            self.rows = list(self.inventory_rows)
        elif " AS payload FROM paper_v2.simulation_daily_run AS scoped" in normalized:
            self.rows = [{"payload": self.daily_payload}] if self.daily_payload is not None else []
        elif "SELECT to_jsonb(scoped) AS payload" in normalized:
            self.rows = []
        elif normalized.startswith("SELECT count(*) AS count FROM paper_v2.simulation_daily_run"):
            self.rows = [{"count": 0}]
        else:
            raise AssertionError(f"unexpected query: {normalized}")

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self.rows)

    def fetchone(self) -> dict[str, Any]:
        return self.rows[0]

    def __iter__(self):
        return iter(self.rows)


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_value = cursor

    def cursor(self, **kwargs: object) -> _Cursor:
        del kwargs
        return self.cursor_value


def _read_with_cursor(
    rows: list[dict[str, Any]], *, daily_payload: dict[str, Any] | None = None
) -> tuple[tuple[Any, ...], _Cursor]:
    cursor = _Cursor(rows, daily_payload=daily_payload)
    result = LocalSimLegacyInventoryReader(_Connection(cursor)).read(
        [ACCOUNT_ID],
        authority_trade_date=AUTHORITY_DATE,
    )
    assert cursor.inventory_params == ([ACCOUNT_ID], AUTHORITY_DATE)
    return result, cursor


def _read(rows: list[dict[str, Any]]) -> tuple[Any, ...]:
    result, _cursor = _read_with_cursor(rows)
    return result


def test_inventory_selects_latest_daily_binding_without_deleting_history() -> None:
    result = _read(
        [
            _row(binding_id="binding_prior", effective_from=date(2026, 8, 28)),
            _row(binding_id="binding_current", effective_from=AUTHORITY_DATE),
            _row(binding_id="binding_old", effective_from=date(2026, 6, 4)),
        ]
    )

    assert len(result) == 1
    assert result[0].binding_id == "binding_current"
    assert result[0].release_id == "release_binding_current"
    assert result[0].manifest_sha256 == "b" * 64


def test_inventory_rejects_two_bindings_on_latest_authority_date() -> None:
    with pytest.raises(DataUnavailableError, match="daily binding authority is missing or ambiguous"):
        _read(
            [
                _row(binding_id="binding_duplicate_a", effective_from=AUTHORITY_DATE),
                _row(binding_id="binding_duplicate_b", effective_from=AUTHORITY_DATE),
                _row(binding_id="binding_prior", effective_from=date(2026, 8, 28)),
            ]
        )


def test_inventory_rejects_when_no_binding_exists_at_or_before_cutoff() -> None:
    with pytest.raises(DataUnavailableError, match="daily binding authority is missing or ambiguous"):
        _read([])


def test_inventory_uses_release_binding_manifest_and_rejects_their_drift() -> None:
    with pytest.raises(InvalidStateTransitionError, match="release or binding authority is inconsistent"):
        _read(
            [
                _row(
                    binding_id="binding_drift",
                    effective_from=AUTHORITY_DATE,
                    binding_manifest_sha256="e" * 64,
                    release_manifest_sha256="b" * 64,
                )
            ]
        )


def test_economic_hash_scopes_fills_through_authoritative_run_ledger_scope() -> None:
    _result, cursor = _read_with_cursor([_row(binding_id="binding_current", effective_from=AUTHORITY_DATE)])

    fills_query = next(query for query in cursor.executed_queries if "FROM paper_v2.fills AS scoped" in query)
    assert "JOIN paper_v2.run AS owner ON owner.run_id = scoped.run_id" in fills_query
    assert "WHERE owner.portfolio_id = %s" in fills_query
    assert "WHERE portfolio_id = %s" not in fills_query


def test_economic_hash_uses_canonical_daily_run_projection_without_retry_observation_churn() -> None:
    _result, cursor = _read_with_cursor([_row(binding_id="binding_current", effective_from=AUTHORITY_DATE)])

    daily_query = next(
        query for query in cursor.executed_queries if "FROM paper_v2.simulation_daily_run AS scoped" in query
    )
    assert "to_jsonb(scoped) - 'updated_at'" in daily_query
    assert "run_payload_json - 'pre_run_failure'" in daily_query
    assert "- 'simulation_scheduler_retry_control_v1'" in daily_query
    assert "- 'submit_failure'" in daily_query


def test_fallback_admission_receipt_is_stable_when_economic_facts_change() -> None:
    row = _row(
        binding_id="binding_current",
        effective_from=AUTHORITY_DATE,
        admission_receipt_id=None,
    )
    first, _cursor = _read_with_cursor([row], daily_payload={"status": "READY"})
    second, _cursor = _read_with_cursor([row], daily_payload={"status": "SUCCEEDED"})

    assert first[0].economic_facts_sha256 != second[0].economic_facts_sha256
    assert first[0].admission_receipt_id == second[0].admission_receipt_id
    assert first[0].admission_receipt_id.startswith("legacy_cutover_")
