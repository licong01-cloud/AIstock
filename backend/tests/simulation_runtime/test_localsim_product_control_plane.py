from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import simulation_runtime as simulation_runtime_router
from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_product_control import (
    LocalSimAccountCreateRequestV1,
    LocalSimBulkLifecycleRequestV1,
    LocalSimHistoricalSourceResolutionV1,
    LocalSimProductControlPlaneService,
    LocalSimReplayCreateRequestV1,
    LocalSimSelectionLinkContextV1,
)
from backend.services.simulation_runtime.localsim_query import LocalSimQueryService
from backend.services.simulation_runtime.localsim_runtime_profile_repository import (
    InMemoryLocalSimRuntimeProfileRepository,
)
from backend.services.simulation_runtime.successor_repository import InMemoryLocalSimSuccessorRepository
from backend.services.strategy_package.execution_policy import ValidatedExecutionPolicy
from backend.services.trading_core.errors import InvalidStateTransitionError


NOW = datetime(2026, 8, 31, 5, 0, tzinfo=UTC)
MANIFEST = "a" * 64


class _Readiness:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.calls = 0

    def require_ready(self) -> None:
        self.calls += 1
        if not self.ready:
            raise RuntimeError("cutover not ready")


class _Authority:
    def __init__(self) -> None:
        self.policy = ValidatedExecutionPolicy(
            policy_id="execpol_twap",
            package_id="pkg_alpha",
            manifest_sha256=MANIFEST,
            policy_name="TWAP",
            policy_json={
                "execution_level": "minute",
                "bar_freq": "1m",
                "algo_code": "TWAP",
                "algo_config": {},
                "fallback_algo_code": None,
                "fallback_policy": {"on_algo_error": "fail"},
            },
            source_backtest_id="bt_001",
            source_backtest_status="SUCCEEDED",
        )

    def resolve_product(self, *, package_id: str, runtime_profile_version_id: str, execution_policy_version_id: str):
        assert package_id == "pkg_alpha"
        assert runtime_profile_version_id == "lsrpv_001"
        assert execution_policy_version_id == self.policy.policy_id
        profile = SimpleNamespace(profile_id="lsrprof_001")
        version = SimpleNamespace(
            profile_version_id="lsrpv_001",
            config_sha256="b" * 64,
            daily_strategy_profile_version_id="lsdaily_001",
            config_json={"schema_version": "localsim_runtime_profile_config_v1", "daily_strategy": {"top_k": 20}},
        )
        return SimpleNamespace(
            package_id=package_id,
            manifest_sha256=MANIFEST,
            admission_receipt_id="lsadm_001",
            runtime_profile=profile,
            runtime_profile_version=version,
            execution_policy=self.policy,
            tail_policy_version_id="lstail_001",
            tail_policy_sha256="c" * 64,
            release_validation_evidence=lambda: {
                "schema_version": "localsim_product_authority_evidence_v1",
                "admission_receipt": {"receipt_id": "lsadm_001", "receipt_hash": "d" * 64, "payload": {}},
            },
        )


class _HistoricalAuthority:
    def resolve(self, *, historical_source_id: str, start_trade_date: date, end_trade_date: date):
        assert historical_source_id == "market.kline_minute_raw.v1"
        return LocalSimHistoricalSourceResolutionV1(
            historical_source_id=historical_source_id,
            historical_source_sha256="e" * 64,
            trading_days=(start_trade_date, date(2026, 8, 27), end_trade_date),
            current_trading_date=date(2026, 8, 31),
            latest_completed_trade_date=end_trade_date,
        )


class _FailReplayRepository(InMemoryLocalSimSuccessorRepository):
    def save_replay_job(self, job):
        raise RuntimeError("injected replay insert failure")


class _FailAfterAccountBundleRepository(InMemoryLocalSimSuccessorRepository):
    def create_account_bundle(self, **kwargs):
        super().create_account_bundle(**kwargs)
        raise RuntimeError("injected selection link insert failure")


def _service(repository=None):
    repository = repository or InMemoryLocalSimSuccessorRepository()
    readiness = _Readiness()
    control = LocalSimControlPlaneService(repository=repository, clock=lambda: NOW)
    return (
        LocalSimProductControlPlaneService(
            repository=repository,
            control=control,
            authority=_Authority(),
            readiness=readiness,
            historical_source_authority=_HistoricalAuthority(),
        ),
        repository,
        readiness,
    )


def _account_request() -> LocalSimAccountCreateRequestV1:
    return LocalSimAccountCreateRequestV1(
        account_name="LocalSIM current package",
        package_id="pkg_alpha",
        initial_capital="1000000.0000",
        runtime_profile_version_id="lsrpv_001",
        execution_policy_version_id="execpol_twap",
        effective_from=date(2026, 8, 31),
        created_reason="new successor account",
        requested_execution_policy_audit={"requested_policy": "V25_TWO_STAGE"},
    )


def _replay_request() -> LocalSimReplayCreateRequestV1:
    return LocalSimReplayCreateRequestV1(
        account_name="LocalSIM six month replay",
        package_id="pkg_alpha",
        initial_capital="1000000.0000",
        runtime_profile_version_id="lsrpv_001",
        execution_policy_version_id="execpol_twap",
        effective_from=date(2026, 2, 27),
        effective_to=date(2026, 8, 28),
        start_trade_date=date(2026, 2, 27),
        end_trade_date=date(2026, 8, 28),
        historical_source_id="market.kline_minute_raw.v1",
    )


def test_account_product_resolves_all_authority_and_freezes_evidence() -> None:
    service, repository, readiness = _service()
    result = service.create_account(_account_request(), created_by="test")

    assert result.ok is True
    assert result.account is not None and result.account.manifest_sha256 == MANIFEST
    assert result.ledger_scope is not None and result.ledger_scope.native_account_id == result.account.account_id
    assert result.release is not None
    assert result.release.execution_policy_version_id == "execpol_twap"
    assert result.release.validation_evidence["admission_receipt"]["receipt_id"] == "lsadm_001"
    assert result.release.release_config_json["metadata"]["requested_execution_policy_audit"][
        "consulted_for_execution"
    ] is False
    assert len(repository.accounts) == len(repository.releases) == len(repository.bindings) == 1
    assert readiness.calls == 1


def test_replay_product_commits_five_entities_in_one_repository_transaction() -> None:
    service, repository, _ = _service()
    result = service.create_replay(_replay_request(), created_by="test")

    assert result.replay is not None
    assert result.account is not None and result.replay.simulation_account_id == result.account.account_id
    assert result.release is not None and result.replay.release_id == result.release.release_id
    assert result.binding is not None and result.binding.effective_to == result.replay.end_trade_date
    assert len(repository.accounts) == 1
    assert len(repository.ledger_scopes) == 1
    assert len(repository.releases) == 1
    assert len(repository.bindings) == 1
    assert len(repository.replay_jobs) == 1


def test_replay_product_rolls_back_account_scope_release_and_binding_when_job_insert_fails() -> None:
    repository = _FailReplayRepository()
    service, _, _ = _service(repository)
    with pytest.raises(RuntimeError, match="injected replay insert failure"):
        service.create_replay(_replay_request(), created_by="test")

    assert repository.accounts == {}
    assert repository.ledger_scopes == {}
    assert repository.releases == {}
    assert repository.bindings == {}
    assert repository.replay_jobs == {}


def test_selection_product_commits_account_and_neutral_link_atomically() -> None:
    service, repository, _ = _service()
    response, link = service.create_account_from_selection(
        _account_request(),
        link_context=LocalSimSelectionLinkContextV1(
            run_id="sel_001",
            trade_date=date(2026, 8, 28),
            data_source="QLIB_HISTORICAL",
            runtime_config={"schema_version": "selection_simulation_account_link_v1"},
        ),
        created_by="test",
    )
    assert response.account is not None
    assert link["simulation_account_id"] == response.account.account_id
    assert len(repository.selection_links) == 1


def test_selection_link_failure_rolls_back_product_bundle() -> None:
    repository = _FailAfterAccountBundleRepository()
    service, _, _ = _service(repository)
    with pytest.raises(RuntimeError, match="selection link insert failure"):
        service.create_account_from_selection(
            _account_request(),
            link_context=LocalSimSelectionLinkContextV1(
                run_id="sel_001",
                trade_date=date(2026, 8, 28),
                data_source="QLIB_HISTORICAL",
                runtime_config={"schema_version": "selection_simulation_account_link_v1"},
            ),
            created_by="test",
        )
    assert repository.accounts == {}
    assert repository.ledger_scopes == {}
    assert repository.releases == {}
    assert repository.bindings == {}
    assert repository.selection_links == {}


def test_account_request_rejects_server_authority_fields() -> None:
    payload = _account_request().model_dump(mode="json")
    payload["manifest_sha256"] = MANIFEST
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        LocalSimAccountCreateRequestV1.model_validate(payload)


def test_bulk_lifecycle_is_all_or_nothing() -> None:
    service, repository, _ = _service()
    first = service.create_account(_account_request(), created_by="test").account
    second_request = _account_request().model_copy(update={"account_name": "LocalSIM second account"})
    second = service.create_account(second_request, created_by="test").account
    assert first is not None and second is not None

    invalid = LocalSimBulkLifecycleRequestV1(
        action="pause",
        items=[
            {"account_id": first.account_id, "expected_version": first.version},
            {"account_id": second.account_id, "expected_version": second.version + 1},
        ],
    )
    with pytest.raises(InvalidStateTransitionError, match="precondition failed"):
        service.transition_accounts_bulk(invalid)
    assert repository.get_account(first.account_id).status.value == "ACTIVE"
    assert repository.get_account(second.account_id).status.value == "ACTIVE"

    valid = LocalSimBulkLifecycleRequestV1(
        action="pause",
        items=[
            {"account_id": first.account_id, "expected_version": first.version},
            {"account_id": second.account_id, "expected_version": second.version},
        ],
    )
    result = service.transition_accounts_bulk(valid)
    assert [account.status.value for account in result.accounts] == ["PAUSED", "PAUSED"]


def test_product_router_exposes_only_successor_commands_and_stable_cursor_queries() -> None:
    product_service, repository, readiness = _service()
    query_service = LocalSimQueryService(
        repository=repository,
        profile_repository=InMemoryLocalSimRuntimeProfileRepository(),
    )
    app = FastAPI()
    app.include_router(simulation_runtime_router.router, prefix="/api/v1")
    app.dependency_overrides[simulation_runtime_router.get_localsim_product_service] = lambda: product_service
    app.dependency_overrides[simulation_runtime_router.get_localsim_query_service] = lambda: query_service
    app.dependency_overrides[simulation_runtime_router.get_localsim_readiness] = lambda: readiness
    client = TestClient(app)

    response = client.post(
        "/api/v1/simulation-runtime/localsim/accounts",
        json=_account_request().model_dump(mode="json"),
    )
    assert response.status_code == 200
    assert response.json()["schema_version"] == "localsim_control_response_v1"
    assert response.json()["account"]["package_id"] == "pkg_alpha"

    forged = _account_request().model_dump(mode="json")
    forged["manifest_sha256"] = MANIFEST
    assert client.post("/api/v1/simulation-runtime/localsim/accounts", json=forged).status_code == 422

    listed = client.get("/api/v1/simulation-runtime/localsim/accounts", params={"limit": 1})
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 1
    assert client.post("/api/v1/simulation-runtime/scheduler/start").status_code == 404
    assert client.post("/api/v1/simulation-runtime/scheduler/stop").status_code == 404
    assert client.post("/api/v1/simulation-runtime/scheduler/tick").status_code == 404
