from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest

from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.services.qmt_strategy_ledger.package_binding import PackageBindingRequest, QmtStrategyPackageBindingService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.selection_center.models import SelectionMode, SelectionRun, SelectionRunStatus
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.trading_core.errors import DataUnavailableError, StrategyPackageValidationError
from backend.services.trading_core.errors import InvalidStateTransitionError


ACCOUNT_ID = "62266303"
TRADE_DATE = date(2026, 5, 18)


@dataclass
class FakePackageRecord:
    package_id: str
    package_status: PackageStatus
    manifest_sha256: str


@dataclass
class FakePackageReader:
    record: FakePackageRecord

    def get(self, package_id: str) -> FakePackageRecord:
        assert package_id == self.record.package_id
        return self.record


@dataclass
class FakeSelectionReader:
    run: SelectionRun

    def get_run(self, run_id: str) -> SelectionRun:
        assert run_id == self.run.run_id
        return self.run


def _repo() -> InMemoryQmtStrategyLedgerRepository:
    repo = InMemoryQmtStrategyLedgerRepository()
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat_a",
            strategy_name="poc_strategy_a",
            display_name="POC Strategy A",
            account_id=ACCOUNT_ID,
            mode="SIM",
            initial_cash=Decimal("10000000"),
            cash=Decimal("10000000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    return repo


def _run(
    *,
    run_id: str = "sel_a",
    package_ids: list[str] | None = None,
    manifest_sha: str = "sha_a",
    status: SelectionRunStatus = SelectionRunStatus.SUCCEEDED,
    trade_date: date = TRADE_DATE,
) -> SelectionRun:
    return SelectionRun(
        run_id=run_id,
        mode=SelectionMode.SINGLE_PACKAGE,
        trade_date=trade_date,
        data_source="DB_HISTORICAL",
        package_ids=package_ids or ["pkg_a"],
        status=status,
        manifest_sha256_by_package={"pkg_a": manifest_sha},
    )


def _artifact_repo(
    *,
    source_type: str = AUTHORITATIVE_SELECTION_SOURCE_TYPE,
    authority_scope: str = AUTHORITATIVE_SELECTION_SCOPE,
    runtime_config: dict | None = None,
    scores: list[dict] | None = None,
) -> InMemorySelectionScoreArtifactRepository:
    repo = InMemorySelectionScoreArtifactRepository()
    rows = scores or [{"symbol": "300604.SZ", "score": 0.9, "rank": 1, "target_weight": 0.02, "reference_price": 10.0}]
    repo.save(
        SelectionScoreArtifact(
            package_id="pkg_a",
            manifest_sha256="sha_a",
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            runtime_config_hash=selection_artifact_runtime_hash(runtime_config or {}),
            scores_json=rows,
            score_count=len(rows),
            universe_count=len(rows),
            top_score_symbol=rows[0]["symbol"] if rows else None,
            metadata={
                "source_type": source_type,
                "authority_scope": authority_scope,
                "runtime_workspace": "F:/AIstock/runtime_cache/pkg_a/sha_a",
                "model_params_origin": "node",
            },
        )
    )
    return repo


def test_package_binding_creates_active_binding_with_manifest_evidence() -> None:
    repo = _repo()
    service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
    )

    binding = service.bind(
        PackageBindingRequest(
            strategy_id="strat_a",
            package_id="pkg_a",
            selection_run_id="sel_a",
            target_weight=Decimal("0.02"),
            top_k=20,
        )
    )

    assert binding.package_id == "pkg_a"
    assert binding.manifest_sha256 == "sha_a"
    assert binding.trade_date == TRADE_DATE
    assert repo.get_active_package_binding("strat_a") == binding


def test_package_binding_captures_frozen_selection_asset_evidence() -> None:
    repo = _repo()
    runtime_config = {"selection_artifact_config": {"cutoff_date": "2026-05-17"}}
    service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
        artifact_repository=_artifact_repo(runtime_config=runtime_config),
    )

    binding = service.bind(
        PackageBindingRequest(
            strategy_id="strat_a",
            package_id="pkg_a",
            selection_run_id="sel_a",
            runtime_config=runtime_config,
        )
    )

    evidence = binding.runtime_config["frozen_runtime_asset"]
    assert evidence["asset_authority"] == "frozen_selection_score_artifact"
    assert evidence["manifest_sha256"] == "sha_a"
    assert evidence["selection_run_id"] == "sel_a"
    assert evidence["trade_date"] == TRADE_DATE.isoformat()
    assert evidence["runtime_config_hash"] == selection_artifact_runtime_hash(runtime_config)
    assert evidence["source_type"] == AUTHORITATIVE_SELECTION_SOURCE_TYPE
    assert evidence["authority_scope"] == AUTHORITATIVE_SELECTION_SCOPE
    assert evidence["artifact_sha256"]


def test_package_binding_fails_fast_when_frozen_asset_missing_or_not_authoritative() -> None:
    repo = _repo()
    service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
        artifact_repository=InMemorySelectionScoreArtifactRepository(),
    )

    with pytest.raises(DataUnavailableError, match="frozen MiniQMT selection artifact is missing") as missing:
        service.bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))
    assert missing.value.context["asset_stage"] == "package_binding"
    assert missing.value.context["runtime_config_hash"] == selection_artifact_runtime_hash({})

    service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
        artifact_repository=_artifact_repo(source_type="qe_mlruns_pred_pkl_v1", authority_scope="diagnostic_backtest_only"),
    )

    with pytest.raises(DataUnavailableError, match="not authoritative") as not_authoritative:
        service.bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))
    assert not_authoritative.value.context["required_source_type"] == AUTHORITATIVE_SELECTION_SOURCE_TYPE


def test_package_binding_same_selection_is_idempotent_without_duplicate_active_row() -> None:
    repo = _repo()
    service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
    )

    first = service.bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))
    second = service.bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))

    assert second == first
    assert repo.get_active_package_binding("strat_a") == first
    assert repo.list_package_bindings("strat_a") == [first]


def test_package_binding_requires_explicit_rollover_for_different_selection() -> None:
    repo = _repo()
    first = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run()),
    ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))
    rollover_service = QmtStrategyPackageBindingService(
        repository=repo,
        package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
        selection_reader=FakeSelectionReader(_run(run_id="sel_b", trade_date=date(2026, 5, 19))),
    )

    with pytest.raises(InvalidStateTransitionError, match="replace_active=true"):
        rollover_service.bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_b"))

    result = rollover_service.bind_with_result(
        PackageBindingRequest(
            strategy_id="strat_a",
            package_id="pkg_a",
            selection_run_id="sel_b",
            replace_active=True,
            replacement_reason="next_trading_day_selection",
        )
    )

    historical = repo.list_package_bindings("strat_a")
    retired = repo.get_package_binding(first.binding_id)
    assert result.action == "replaced_active"
    assert result.replaced_binding == retired
    assert result.binding.selection_run_id == "sel_b"
    assert result.binding.trade_date == date(2026, 5, 19)
    assert result.binding.runtime_config["binding_lifecycle"]["replaces_binding_id"] == first.binding_id
    assert retired.binding_status.value == "RETIRED"
    assert retired.runtime_config["binding_lifecycle"]["replaced_by_binding_id"] == result.binding.binding_id
    assert repo.get_active_package_binding("strat_a") == result.binding
    assert [binding.binding_status.value for binding in historical] == ["RETIRED", "ACTIVE"]


def test_package_binding_rejects_unavailable_package_selection_and_manifest_mismatch() -> None:
    repo = _repo()

    with pytest.raises(StrategyPackageValidationError, match="not enabled"):
        QmtStrategyPackageBindingService(
            repository=repo,
            package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.DRAFT, "sha_a")),
            selection_reader=FakeSelectionReader(_run()),
        ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))

    with pytest.raises(DataUnavailableError, match="selection run is not succeeded"):
        QmtStrategyPackageBindingService(
            repository=repo,
            package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
            selection_reader=FakeSelectionReader(_run(status=SelectionRunStatus.FAILED)),
        ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))

    with pytest.raises(StrategyPackageValidationError, match="manifest hash"):
        QmtStrategyPackageBindingService(
            repository=repo,
            package_reader=FakePackageReader(FakePackageRecord("pkg_a", PackageStatus.SELECTION_ENABLED, "sha_a")),
            selection_reader=FakeSelectionReader(_run(manifest_sha="sha_other")),
        ).bind(PackageBindingRequest(strategy_id="strat_a", package_id="pkg_a", selection_run_id="sel_a"))
