from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Any, Iterator, Mapping

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime
from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256
from backend.services.qmt_strategy_ledger.tca_read_repository import (
    ExecutionTcaDetail,
    ExecutionTcaParentPage,
    ExecutionTcaSelection,
)
from backend.services.qmt_strategy_ledger.tca_read_service import (
    TcaActiveReadVersion,
    TcaReadError,
    TcaReadRuntimeConfig,
)
from backend.services.simulation_runtime.tca_read_api import ExecutionTcaReadService
from backend.services.simulation_runtime.tca_read_api import render_canonical_evidence_export
from scripts.export_miniqmt_execution_tca_evidence import main as export_evidence_main


TRADE_DATE = date(2026, 7, 10)
SOURCE_TIME = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)


def test_service_projects_pseudonymous_evidence_without_raw_payload_or_mutation() -> None:
    repository = _ReadOnlyRepository()
    service = ExecutionTcaReadService(repository=repository, config_provider=_config)

    payload = service.get_execution_tca(
        parent_intent_id="parent-1",
        parent_revision="1",
        snapshot_kind="RECONCILED_FINAL",
    )

    rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    assert payload["trade_observations"][0]["observation_role"] == "CORE"
    assert payload["trade_observations"][0]["selected_content_sha256"] == "s" * 64
    assert payload["trade_observations"][0]["trade_account_pseudonym"].startswith("acct_test-v1_")
    assert payload["marks"][0]["trade_account_pseudonym"].startswith("acct_test-v1_")
    assert "account-raw" not in rendered
    assert "raw payload must never escape" not in rendered
    assert "normalized payload must never escape" not in rendered
    assert "raw_payload" not in rendered
    assert repository.calls == ["read_snapshot", "get_tca_detail"]


def test_service_signs_cursor_and_rejects_cross_filter_reuse() -> None:
    repository = _ReadOnlyRepository()
    service = ExecutionTcaReadService(repository=repository, config_provider=_config)

    page = service.list_execution_parents(binding_id="binding-1", trade_date=TRADE_DATE, limit="1")

    assert page["next_cursor"]
    assert page["parents"][0]["account_pseudonym"].startswith("acct_test-v1_")
    with pytest.raises(TcaReadError) as wrong_filter:
        service.list_execution_parents(
            binding_id="different-binding",
            trade_date=TRADE_DATE,
            cursor=page["next_cursor"],
        )

    assert wrong_filter.value.reason_code == "ADAPTIVE_IS_TCA_CURSOR_INVALID"
    assert wrong_filter.value.http_status == 400
    assert "rebuild" not in repository.calls
    assert "scheduler_tick" not in repository.calls


def test_router_calls_only_read_service_and_preserves_stable_tca_error_status() -> None:
    service = _RouterReadService()
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    app.dependency_overrides[simulation_runtime.get_execution_tca_read_service] = lambda: service
    client = TestClient(app)

    listing = client.get(
        "/api/v1/simulation-runtime/execution-parents",
        params={"binding_id": "binding-1", "trade_date": "2026-07-10"},
    )
    parent = client.get("/api/v1/simulation-runtime/execution-parents/parent-1", params={"revision": "1"})
    tca = client.get(
        "/api/v1/simulation-runtime/execution-parents/parent-1/tca",
        params={"revision": "1", "snapshot_kind": "RECONCILED_FINAL"},
    )
    service.error = TcaReadError(
        "ADAPTIVE_IS_TCA_CHAIN_FORK",
        "series fork",
        http_status=409,
        stage="TCA_READ_REPOSITORY",
    )
    fork = client.get(
        "/api/v1/simulation-runtime/execution-parents/parent-1/tca",
        params={"revision": "1", "snapshot_kind": "RECONCILED_FINAL"},
    )

    assert listing.status_code == 200
    assert parent.status_code == 200
    assert tca.status_code == 200
    assert fork.status_code == 409
    assert fork.json()["detail"]["error_code"] == "ADAPTIVE_IS_TCA_CHAIN_FORK"
    assert service.calls == ["list", "parent", "tca", "tca"]


@pytest.mark.parametrize(
    ("http_status", "reason_code"),
    (
        (400, "ADAPTIVE_IS_TCA_REQUEST_INVALID"),
        (404, "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND"),
        (503, "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_MISSING"),
    ),
)
def test_router_preserves_read_error_contract(http_status: int, reason_code: str) -> None:
    service = _RouterReadService()
    service.error = TcaReadError(
        reason_code,
        "stable TCA read failure",
        http_status=http_status,
        stage="TCA_READ_REPOSITORY",
    )
    app = FastAPI()
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    app.dependency_overrides[simulation_runtime.get_execution_tca_read_service] = lambda: service
    client = TestClient(app)

    response = client.get(
        "/api/v1/simulation-runtime/execution-parents/parent-1/tca",
        params={"revision": "1", "snapshot_kind": "RECONCILED_FINAL"},
    )

    assert response.status_code == http_status
    assert response.json()["detail"]["error_code"] == reason_code
    assert response.json()["detail"]["context"]["stage"] == "TCA_READ_REPOSITORY"


def test_evidence_export_and_cli_are_deterministic_pseudonymized_and_read_only(tmp_path) -> None:
    repository = _ExportRepository()
    service = ExecutionTcaReadService(repository=repository, config_provider=_config)

    first = service.export_execution_evidence(binding_id="binding-1", trade_date=TRADE_DATE)
    second = service.export_execution_evidence(binding_id="binding-1", trade_date=TRADE_DATE)
    rendered_json = render_canonical_evidence_export(first, output_format="json")
    rendered_ndjson = render_canonical_evidence_export(first, output_format="ndjson")
    output = tmp_path / "execution-evidence.ndjson"
    exit_code = export_evidence_main(
        [
            "--binding-id",
            "binding-1",
            "--trade-date",
            "2026-07-10",
            "--output",
            str(output),
            "--format",
            "ndjson",
        ],
        service_factory=lambda: ExecutionTcaReadService(repository=_ExportRepository(), config_provider=_config),
    )

    assert first.manifest == second.manifest
    assert first.records == second.records
    assert first.manifest["account_pseudonym_key_version"] == "test-v1"
    assert first.manifest["manifest_sha256"]
    assert first.manifest["records_sha256"]
    assert rendered_json.endswith("\n")
    assert rendered_ndjson.endswith("\n")
    assert "account-raw" not in rendered_json
    assert "raw payload must never escape" not in rendered_json
    assert "normalized payload must never escape" not in rendered_json
    assert exit_code == 0
    assert output.read_text(encoding="utf-8") == rendered_ndjson
    assert "rebuild" not in repository.calls
    assert "scheduler_tick" not in repository.calls


def _config() -> TcaReadRuntimeConfig:
    version = _version().as_mapping()
    return TcaReadRuntimeConfig.from_environ(
        {
            "MINIQMT_TCA_ACTIVE_READ_VERSION": json.dumps(
                {**version, "config_sha256": canonical_json_sha256(version)}, sort_keys=True
            ),
            "AISTOCK_TCA_EXPORT_HMAC_KEY": "test-hmac-key",
            "AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION": "test-v1",
        }
    )


def _version() -> TcaActiveReadVersion:
    values = {
        "calculator_version": "calculator-v1",
        "formula_version": "formula-v1",
        "schema_version": "schema-v1",
        "query_version": "query-v1",
        "benchmark_policy_version": "benchmark-v1",
        "mark_policy_version": "mark-v1",
        "fee_policy_version": "fee-v1",
        "trade_provenance_policy_version": "trade-v1",
    }
    return TcaActiveReadVersion.from_mapping({**values, "config_sha256": canonical_json_sha256(values)})


def _parent(*, terminal_state: str = "COMPLETED_BY_DEADLINE") -> dict[str, Any]:
    return {
        "parent_intent_id": "parent-1",
        "parent_revision": 1,
        "account_id": "account-raw",
        "trade_date": TRADE_DATE,
        "environment": "SIM",
        "symbol": "000001.SZ",
        "side": "BUY",
        "currency": "CNY",
        "binding_id": "binding-1",
        "eligibility_class": "ELIGIBLE_NOW",
        "eligible_quantity": 100,
        "terminal_state": terminal_state,
        "latest_tca_result_id": "result-1",
        "latest_tca_snapshot_kind": "RECONCILED_FINAL",
        "raw_evidence": {"account_id": "account-raw", "raw_payload": "must not escape"},
    }


def _result() -> dict[str, Any]:
    version = _version().as_mapping()
    result_series_key = canonical_json_sha256(
        {
            "parent_intent_id": "parent-1",
            "parent_revision": 1,
            "snapshot_kind": "RECONCILED_FINAL",
            **version,
        }
    )
    return {
        "tca_result_id": "result-1",
        "result_series_key": result_series_key,
        "result_generation": 1,
        "supersedes_tca_result_id": None,
        "parent_intent_id": "parent-1",
        "parent_revision": 1,
        "snapshot_kind": "RECONCILED_FINAL",
        "result_status": "FINAL",
        "source_snapshot_started_at": SOURCE_TIME,
        "source_snapshot_completed_at": SOURCE_TIME,
        "completed_receipt_ids": ["receipt-1"],
        "fee_breakdown": {
            "account_id": "account-raw",
            "raw_payload": "raw payload must never escape",
            "amount": "1.00",
        },
        "metric_validity": {"normal": True},
        **version,
    }


class _ReadOnlyRepository:
    def __init__(self) -> None:
        self.calls: list[str] = []
        result = _result()
        self.detail = ExecutionTcaDetail(
            selection=ExecutionTcaSelection(
                result=result,
                result_series=(result,),
                selection_mode="ACTIVE_HEAD",
            ),
            marks=(
                {
                    "tca_result_id": "result-1",
                    "mark_id": "mark-1",
                    "mark_role": "DEADLINE",
                    "trade_account_id": "account-raw",
                    "raw_quote_sha256": "q" * 64,
                },
            ),
            trade_observations=(
                {
                    "tca_result_id": "result-1",
                    "trade_observation_id": "observation-1",
                    "trade_account_id": "account-raw",
                    "trade_date": TRADE_DATE,
                    "trade_id": "trade-1",
                    "observation_role": "CORE",
                    "selected_content_sha256": "s" * 64,
                    "membership_hash": "m" * 64,
                    "raw_payload": "raw payload must never escape",
                    "normalized_payload": "normalized payload must never escape",
                },
            ),
        )

    @contextmanager
    def read_snapshot(self) -> Iterator[object]:
        self.calls.append("read_snapshot")
        yield object()

    def get_parent(self, **_: Any) -> Mapping[str, Any] | None:
        self.calls.append("get_parent")
        return _parent()

    def get_tca(self, **_: Any) -> ExecutionTcaSelection:
        self.calls.append("get_tca")
        return self.detail.selection

    def get_tca_detail(self, **_: Any) -> ExecutionTcaDetail:
        self.calls.append("get_tca_detail")
        return self.detail

    def list_parents(self, **_: Any) -> ExecutionTcaParentPage:
        self.calls.append("list_parents")
        return ExecutionTcaParentPage(
            parents=(_parent(), _parent(terminal_state="DEADLINE_RESIDUAL")),
            next_key=(TRADE_DATE, "parent-1", 1),
        )

    def rebuild(self, **_: Any) -> None:
        raise AssertionError("read service must never call rebuild")

    def scheduler_tick(self, **_: Any) -> None:
        raise AssertionError("read service must never call scheduler_tick")

    def write(self, **_: Any) -> None:
        raise AssertionError("read service must never call write")


class _ExportRepository(_ReadOnlyRepository):
    def list_parents(self, **_: Any) -> ExecutionTcaParentPage:
        self.calls.append("list_parents")
        return ExecutionTcaParentPage(parents=(_parent(),), next_key=None)

    def get_tca_detail(self, *, snapshot_kind: str, **_: Any) -> ExecutionTcaDetail:
        self.calls.append(f"get_tca_detail:{snapshot_kind}")
        if snapshot_kind == "DEADLINE":
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND",
                "no deadline result",
                http_status=404,
                stage="TCA_READ_REPOSITORY",
            )
        return self.detail


class _RouterReadService:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.error: TcaReadError | None = None

    def list_execution_parents(self, **_: Any) -> dict[str, Any]:
        self.calls.append("list")
        return {"parents": [], "next_cursor": None}

    def get_execution_parent(self, **_: Any) -> dict[str, Any]:
        self.calls.append("parent")
        return {"parent": {"parent_intent_id": "parent-1"}, "latest_tca": None}

    def get_execution_tca(self, **_: Any) -> dict[str, Any]:
        self.calls.append("tca")
        if self.error is not None:
            raise self.error
        return {"result": {"tca_result_id": "result-1"}}

    def rebuild(self, **_: Any) -> None:
        raise AssertionError("router must never call rebuild")

    def scheduler_tick(self, **_: Any) -> None:
        raise AssertionError("router must never call scheduler_tick")

    def write(self, **_: Any) -> None:
        raise AssertionError("router must never call write")
