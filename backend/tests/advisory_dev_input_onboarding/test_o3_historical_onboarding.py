from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AlphaMode,
    HistoricalProgramResult,
    HistoricalProgramSpec,
    HistoricalProgramStatus,
    RealDevHistoricalRunReceipt,
    RealDevHistoricalRunRequest,
    RealDevOnboardingError,
    database_identity_hash,
)
from backend.services.advisory_dev_input_onboarding.historical_onboarding import (
    ExactDevConnectionFactory,
    ExactDevHMMSnapshotProvider,
    ExactDevStPitRiskDecisionProvider,
    ExactDevSymbolNameResolver,
    ExactDevWslInferenceProvider,
    HistoricalOnboardingEvidenceStore,
    HistoricalResearchExecutionProhibitedPortfolioService,
    RealDevHistoricalOnboardingService,
    _database_identity,
    repository_code_release,
    target_package_asset_root_hash,
)
from backend.services.advisory_dev_input_onboarding.store import RealDevOnboardingEvidenceStore
from backend.services.advisory_phase0a.historical_research import (
    HistoricalResearchInputUnavailable,
    HistoricalResearchRunStatus,
    REASON_HISTORICAL_DATE_REQUIRED,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import DatabaseConnectionConfig
from backend.services.advisory_program import AdvisoryProgramService, InMemoryAdvisoryProgramRepository
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.prospective_evidence import canonical_evidence_json_sha256
from backend.services.selection_center.risk_policy import StPitRiskDecisionProvider
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_UNIVERSE_KEY, StockUniversePitError
from backend.services.selection_center.runtime_profile import (
    mark_non_trading_preview_runtime_config,
    normalize_selection_runtime_config,
    refresh_generated_runtime_profile_binding,
)
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
CODE_RELEASE_ID = "1" * 40
CODE_RELEASE_HASH = canonical_json_sha256({"git_commit": CODE_RELEASE_ID})


class _Calendar:
    @staticmethod
    def next_trading_day(anchor_date: date, *, inclusive: bool = False) -> date:
        return anchor_date if inclusive else anchor_date + timedelta(days=1)


def _historical_request(onboarding_request, onboarding_ref, asset_root: Path, *, target_identity_hash: str = "d" * 64):
    specs = tuple(
        HistoricalProgramSpec(
            program_id=item.program_id,
            program_name=f"Historical {item.style}",
            package_id=item.package_id,
            alpha_mode=item.alpha_mode,
            style=item.style,
            target_count=item.target_count,
            review_policy=item.review_policy,
            runtime_config={
                "runtime_profile": {
                    "selection": {"top_k": item.target_count},
                    "hmm": {"enabled": False},
                    "risk_policy": {"enabled": False},
                    "tradability": {"exclude_suspended": False},
                    "industry_blacklist": [],
                },
                "selection_artifact_config": {
                    "auto_generate": True,
                    "inference_backend": "wsl",
                    "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
                },
            },
        )
        for item in onboarding_request.target_dev_program_specs
    )
    return RealDevHistoricalRunRequest(
        onboarding_request_ref=onboarding_ref,
        onboarding_request_hash=onboarding_request.request_hash,
        target_database_identity_hash=target_identity_hash,
        target_package_asset_root_hash=target_package_asset_root_hash(asset_root),
        program_specs=specs,
        binding_effective_from_trade_date=onboarding_request.binding_effective_from_trade_date,
        decision_trade_date=onboarding_request.decision_trade_date,
        policy_registry_id=onboarding_request.policy_registry_id,
        policy_registry_version=onboarding_request.policy_registry_version,
        policy_registry_hash=onboarding_request.policy_registry_hash,
        code_release_id=CODE_RELEASE_ID,
        code_release_hash=CODE_RELEASE_HASH,
    )


def test_historical_request_is_hash_closed_and_sorted(tmp_path: Path, onboarding_request) -> None:
    root = tmp_path / "assets"
    root.mkdir()
    evidence = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    stored = evidence.publish(onboarding_request)
    request = _historical_request(onboarding_request, stored.ref, root)

    assert [item.program_id for item in request.program_specs] == sorted(item.program_id for item in request.program_specs)
    assert len(request.historical_request_hash) == 64
    payload = request.model_dump(mode="python")
    payload["code_release_id"] = "different"
    with pytest.raises(ValueError, match="historical_request_hash"):
        RealDevHistoricalRunRequest.model_validate(payload)


def test_historical_receipt_rejects_batch_program_status_mismatch() -> None:
    results = tuple(
        HistoricalProgramResult(
            program_id=f"program-{index}",
            package_id=f"package-{index}",
            alpha_mode=mode,
            status=HistoricalProgramStatus.WAITING_INPUT,
            reason_codes=("ADVISORY_INPUT_PENDING",),
        )
        for index, mode in ((1, AlphaMode.SINGLE), (2, AlphaMode.MULTI))
    )

    with pytest.raises(ValueError, match="aggregate Program status"):
        RealDevHistoricalRunReceipt(
            historical_request_hash="a" * 64,
            target_database_identity_hash="b" * 64,
            target_package_asset_root_hash="c" * 64,
            batch_id="batch-o3",
            batch_key="d" * 64,
            batch_status="COMPLETE",
            formal_batch_receipt_hash="e" * 64,
            program_results=results,
            started_at=NOW,
            finished_at=NOW,
        )


def test_target_root_and_repository_release_reject_invalid_inputs(tmp_path: Path, monkeypatch) -> None:
    file_path = tmp_path / "not-a-directory"
    file_path.write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="directory"):
        target_package_asset_root_hash(file_path)

    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout="", stderr="failed"),
    )
    with pytest.raises(RealDevOnboardingError, match="code release"):
        repository_code_release(Path.cwd())


def test_repository_release_rejects_dirty_worktree(monkeypatch) -> None:
    responses = iter(
        (
            SimpleNamespace(returncode=0, stdout=CODE_RELEASE_ID + "\n", stderr=""),
            SimpleNamespace(returncode=0, stdout=" M backend/example.py\n", stderr=""),
        )
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.subprocess.run",
        lambda *_args, **_kwargs: next(responses),
    )

    with pytest.raises(RealDevOnboardingError, match="clean repository worktree"):
        repository_code_release(Path.cwd())


def test_repository_release_accepts_clean_exact_head_and_rejects_status_failure(monkeypatch) -> None:
    clean = iter(
        (
            SimpleNamespace(returncode=0, stdout=CODE_RELEASE_ID + "\n", stderr=""),
            SimpleNamespace(returncode=0, stdout="", stderr=""),
        )
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.subprocess.run",
        lambda *_args, **_kwargs: next(clean),
    )
    assert repository_code_release(Path.cwd()) == (CODE_RELEASE_ID, CODE_RELEASE_HASH)

    failed = iter(
        (
            SimpleNamespace(returncode=0, stdout=CODE_RELEASE_ID + "\n", stderr=""),
            SimpleNamespace(returncode=1, stdout="", stderr="status failed"),
        )
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.subprocess.run",
        lambda *_args, **_kwargs: next(failed),
    )
    with pytest.raises(RealDevOnboardingError, match="verify repository worktree"):
        repository_code_release(Path.cwd())


def test_o3_program_adapter_preserves_exact_identity_and_rerun(tmp_path: Path, onboarding_request) -> None:
    root = tmp_path / "assets"
    root.mkdir()
    evidence = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, evidence.publish(onboarding_request).ref, root)
    spec = request.program_specs[0]
    repository = InMemoryAdvisoryProgramRepository()
    program_service = AdvisoryProgramService(
        repository=repository,
        selection_service=object(),
        calendar_provider=_Calendar(),
        symbol_name_resolver=object(),
        now_provider=lambda: NOW,
    )
    components = SimpleNamespace(program_repository=repository, program_service=program_service)
    service = RealDevHistoricalOnboardingService(now_provider=lambda: NOW)

    first_program, first_binding = service._ensure_program(  # noqa: SLF001
        spec=spec,
        effective_from=request.binding_effective_from_trade_date,
        components=components,
    )
    second_program, second_binding = service._ensure_program(  # noqa: SLF001
        spec=spec,
        effective_from=request.binding_effective_from_trade_date,
        components=components,
    )

    assert first_program.program_id == spec.program_id
    assert second_program == first_program
    assert second_binding == first_binding
    assert len(repository.programs) == 1
    assert len(repository.binding_versions) == 1

    changed = spec.model_copy(update={"runtime_config": {"different": True}})
    with pytest.raises(RealDevOnboardingError, match="binding payload conflicts"):
        service._ensure_program(  # noqa: SLF001
            spec=changed,
            effective_from=request.binding_effective_from_trade_date,
            components=components,
        )


def test_wsl_provider_exports_only_explicit_dev_database_values(monkeypatch) -> None:
    monkeypatch.setenv("TDX_DB_HOST", "production-host")
    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5544,
        database="aistock_dev",
        user="dev-user",
        password="fixture",
        environment_contract_hash="a" * 64,
    )
    exports = ExactDevWslInferenceProvider(database=config, repo_root=Path.cwd())._build_env_exports()  # noqa: SLF001

    assert "production-host" not in exports
    assert "dev-host" in exports
    assert "aistock_dev" in exports
    assert "dev-user" in exports
    monkeypatch.setenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS", "120000")
    assert "AISTOCK_PG_STATEMENT_TIMEOUT_MS" in ExactDevWslInferenceProvider(
        database=config,
        repo_root=Path.cwd(),
    )._build_env_exports()  # noqa: SLF001


def test_exact_dev_connection_factory_commits_and_rolls_back(monkeypatch) -> None:
    events: list[str] = []

    class Connection:
        def set_session(self, **_kwargs):
            events.append("set_session")

        def commit(self):
            events.append("commit")

        def rollback(self):
            events.append("rollback")

        def close(self):
            events.append("close")

    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="a" * 64,
    )
    identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="10.0.0.2",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="b" * 64,
        environment_contract_hash=config.environment_contract_hash,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding._database_identity",
        lambda **_kwargs: identity,
    )
    factory = ExactDevConnectionFactory(
        config,
        expected_database_identity_hash=database_identity_hash(identity),
        connector=lambda **_kwargs: Connection(),
    )

    with factory() as connection:
        assert isinstance(connection, Connection)
    assert events == ["set_session", "commit", "close"]

    events.clear()
    with pytest.raises(RuntimeError, match="boom"):
        with factory():
            raise RuntimeError("boom")
    assert events == ["set_session", "rollback", "close"]


def test_exact_dev_connection_factory_rejects_identity_drift(monkeypatch) -> None:
    events: list[str] = []

    class Connection:
        def set_session(self, **_kwargs):
            events.append("set_session")

        def rollback(self):
            events.append("rollback")

        def close(self):
            events.append("close")

    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="a" * 64,
    )
    drifted = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="different_dev",
        server_address="10.0.0.3",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="b" * 64,
        environment_contract_hash=config.environment_contract_hash,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding._database_identity",
        lambda **_kwargs: drifted,
    )
    factory = ExactDevConnectionFactory(
        config,
        expected_database_identity_hash="c" * 64,
        connector=lambda **_kwargs: Connection(),
    )

    with pytest.raises(RealDevOnboardingError, match="identity differs"):
        with factory():
            pass
    assert events == ["set_session", "rollback", "close"]


def test_database_identity_reads_exact_writable_connection() -> None:
    class Cursor:
        def __init__(self, row):
            self.row = row

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql):
            return None

        def fetchone(self):
            return self.row

    class Connection:
        def __init__(self, row):
            self.row = row

        def cursor(self, **_kwargs):
            return Cursor(self.row)

    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="a" * 64,
    )
    identity = _database_identity(
        connection=Connection(
            {
                "current_database": "aistock_dev",
                "server_address": "10.0.0.2",
                "server_port": 5432,
                "server_version_num": 160000,
                "current_user": "dev-user",
            }
        ),
        config=config,
    )
    assert identity.current_database == "aistock_dev"
    assert identity.server_address == "10.0.0.2"

    with pytest.raises(RealDevOnboardingError, match="returned no row"):
        _database_identity(connection=Connection(None), config=config)


def test_exact_dev_hmm_and_symbol_providers_use_injected_connection() -> None:
    class Cursor:
        def __init__(self):
            self.sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, _params):
            self.sql = sql

        def fetchall(self):
            if "model_train_snapshots" in self.sql:
                return [
                    {
                        "snapshot_id": "snapshot-1",
                        "config_id": "config-1",
                        "trained_at": NOW,
                        "model_path": "model.pkl",
                        "sector_count": 2,
                        "status": "ready",
                        "metrics_json": {},
                        "config_display_name": "Config",
                    }
                ]
            if "market.stock_basic" in self.sql:
                return [("000001.SZ", "Ping An")]
            return [("000002.SZ", "Vanke")]

    class Connection:
        def cursor(self, **_kwargs):
            return Cursor()

    @contextmanager
    def factory():
        yield Connection()

    hmm = ExactDevHMMSnapshotProvider(factory)
    names = ExactDevSymbolNameResolver(factory)

    assert hmm.get_snapshot("snapshot-1")["status"] == "ready"
    assert hmm.list_snapshots("config-1")[0]["snapshot_id"] == "snapshot-1"
    assert names.resolve(["000001.SZ", "000002.SZ"]) == {
        "000001.SZ": "Ping An",
        "000002.SZ": "Vanke",
    }
    assert names.resolve(["000001.SZ"]) == {"000001.SZ": "Ping An"}
    assert names.resolve([]) == {}


def test_exact_dev_hmm_and_symbol_provider_empty_error_paths(caplog) -> None:
    class EmptyCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql, _params):
            return None

        def fetchall(self):
            return []

    class EmptyConnection:
        def cursor(self, **_kwargs):
            return EmptyCursor()

    @contextmanager
    def empty_factory():
        yield EmptyConnection()

    assert ExactDevHMMSnapshotProvider(empty_factory).get_snapshot("missing") is None
    resolver = ExactDevSymbolNameResolver(empty_factory)
    assert resolver.resolve(["000001.SZ"]) == {}
    with pytest.raises(ValueError, match="unsupported"):
        resolver._query("market.other", ["000001.SZ"])  # noqa: SLF001

    @contextmanager
    def broken_factory():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    assert ExactDevSymbolNameResolver(broken_factory).resolve(["000001.SZ"]) == {}
    assert "symbol_name_lookup_failed" in caplog.text


def test_exact_dev_st_pit_provider_preserves_shared_decision_semantics(monkeypatch) -> None:
    queries: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, sql, _params):
            queries.append(sql)

        def fetchone(self):
            return ("ready", False, date(2020, 1, 1), date(2030, 1, 1), None)

        def fetchall(self):
            return [("000001.SZ", date(2020, 1, 1), date(2030, 1, 1), "listed", None, "v1", {})]

    class Connection:
        def cursor(self):
            return Cursor()

    @contextmanager
    def factory():
        yield Connection()

    profile = SimpleNamespace(
        strict_data_ready=True,
        st_universe_key=DEFAULT_ST_PIT_UNIVERSE_KEY,
        hard_actions=["block_buy", "force_exit"],
        policy_version="v1",
    )
    kwargs = {
        "symbols": ["000001.SZ", "000002.SZ"],
        "trade_date": date(2026, 7, 21),
        "profile": profile,
        "current_positions": {"000002.SZ": {"quantity": 100}},
    }
    decisions = ExactDevStPitRiskDecisionProvider(factory).evaluate(**kwargs)
    monkeypatch.setattr("backend.services.selection_center.risk_policy.get_conn", factory)
    shared = StPitRiskDecisionProvider().evaluate(**kwargs)

    assert {key: value.model_dump(mode="json") for key, value in decisions.items()} == {
        key: value.model_dump(mode="json") for key, value in shared.items()
    }

    assert decisions["000001.SZ"].can_buy is True
    assert decisions["000002.SZ"].can_buy is False
    assert decisions["000002.SZ"].force_exit is True
    assert any("stock_universe_pit_state" in sql for sql in queries)
    assert any("stock_universe_pit_spans" in sql for sql in queries)


def test_exact_dev_st_pit_empty_and_readiness_failures() -> None:
    @contextmanager
    def unused_factory():
        raise AssertionError("empty symbols must not query")
        yield  # pragma: no cover

    profile = SimpleNamespace(
        strict_data_ready=False,
        st_universe_key=DEFAULT_ST_PIT_UNIVERSE_KEY,
        hard_actions=[],
        policy_version="v1",
    )
    assert ExactDevStPitRiskDecisionProvider(unused_factory).evaluate(
        symbols=[],
        trade_date=date(2026, 7, 21),
        profile=profile,
    ) == {}

    class Cursor:
        def __init__(self, row):
            self.row = row

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql, _params):
            return None

        def fetchone(self):
            return self.row

    def factory_for(row):
        @contextmanager
        def factory():
            yield SimpleNamespace(cursor=lambda: Cursor(row))

        return factory

    with pytest.raises(DataUnavailableError, match="missing"):
        ExactDevStPitRiskDecisionProvider(factory_for(None))._require_ready(  # noqa: SLF001
            universe_key=profile.st_universe_key,
            trade_date=date(2026, 7, 21),
        )
    with pytest.raises(DataUnavailableError, match="not ready"):
        ExactDevStPitRiskDecisionProvider(
            factory_for(("building", True, None, None, "pending"))
        )._require_ready(  # noqa: SLF001
            universe_key=profile.st_universe_key,
            trade_date=date(2026, 7, 21),
        )
    with pytest.raises(DataUnavailableError, match="does not cover"):
        ExactDevStPitRiskDecisionProvider(
            factory_for(("ready", False, date(2020, 1, 1), date(2021, 12, 31), None))
        )._require_ready(  # noqa: SLF001
            universe_key=profile.st_universe_key,
            trade_date=date(2026, 7, 21),
        )

    @contextmanager
    def broken_factory():
        raise RuntimeError("db unavailable")
        yield  # pragma: no cover

    with pytest.raises(DataUnavailableError, match="readiness check failed"):
        ExactDevStPitRiskDecisionProvider(broken_factory)._require_ready(  # noqa: SLF001
            universe_key=profile.st_universe_key,
            trade_date=date(2026, 7, 21),
        )
    with pytest.raises(DataUnavailableError, match="lookup failed"):
        ExactDevStPitRiskDecisionProvider(broken_factory).evaluate(
            symbols=["000001.SZ"],
            trade_date=date(2026, 7, 21),
            profile=SimpleNamespace(**{**profile.__dict__, "strict_data_ready": False}),
        )
    with pytest.raises(StockUniversePitError, match="authoritative rolling universe"):
        ExactDevStPitRiskDecisionProvider(unused_factory).evaluate(
            symbols=["000001.SZ"],
            trade_date=date(2026, 7, 21),
            profile=SimpleNamespace(**{**profile.__dict__, "st_universe_key": "shsz_st_pit_qe_dataset_fixture"}),
        )


def test_default_component_builder_is_constructor_only_and_exact_dev(tmp_path: Path) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="a" * 64,
    )

    def connector(**_kwargs):
        raise AssertionError("component construction must not connect")

    components = RealDevHistoricalOnboardingService(connector=connector)._build_components(  # noqa: SLF001
        config=config,
        expected_database_identity_hash="b" * 64,
        target_package_asset_root=asset_root,
        repository_root=Path.cwd(),
    )

    assert components.conn_factory.config == config
    assert components.program_repository._conn_factory is components.conn_factory  # noqa: SLF001
    assert components.artifact_repository._conn_factory is components.conn_factory  # noqa: SLF001
    assert isinstance(
        components.selection_center.paper_portfolio_service,
        HistoricalResearchExecutionProhibitedPortfolioService,
    )


def test_historical_portfolio_boundary_fails_loudly() -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="cannot create a Paper portfolio"):
        HistoricalResearchExecutionProhibitedPortfolioService.create_portfolio()


def test_existing_v2_evidence_is_reused_without_new_selection(tmp_path: Path, onboarding_request, monkeypatch) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    spec = request.program_specs[0]
    evidence = SimpleNamespace(evidence_id="existing")
    components = SimpleNamespace(
        program_resolver=SimpleNamespace(resolve=lambda **_kwargs: object()),
        evidence_adapter=SimpleNamespace(load=lambda **_kwargs: evidence),
        conn_factory=object(),
    )

    service = RealDevHistoricalOnboardingService()
    monkeypatch.setattr(service, "_assert_evidence_code_release", lambda **_kwargs: None)
    actual, selection_run_id = service._ensure_prospective_evidence(  # noqa: SLF001
        request=request,
        spec=spec,
        program=object(),
        binding=object(),
        components=components,
    )

    assert actual is evidence
    assert selection_run_id is None


def test_missing_v2_evidence_runs_public_selection_and_requires_complete_capture(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    spec = request.program_specs[0]
    final_evidence = SimpleNamespace(evidence_id="generated")
    calls = {"load": 0}

    def load(**_kwargs):
        calls["load"] += 1
        if calls["load"] == 1:
            raise HistoricalResearchInputUnavailable("missing")
        return final_evidence

    program_service = SimpleNamespace(
        _review_runtime_config=lambda _program, runtime: runtime,
        _with_advisory_date_context=lambda runtime, **_kwargs: runtime,
    )
    components = SimpleNamespace(
        program_resolver=SimpleNamespace(resolve=lambda **_kwargs: object()),
        evidence_adapter=SimpleNamespace(load=load),
        calendar_service=SimpleNamespace(next_trading_day=lambda *_args, **_kwargs: date(2026, 7, 22)),
        program_service=program_service,
        selection_service=object(),
        artifact_service=SimpleNamespace(generate_from_live_inference=lambda **_kwargs: object()),
        selection_center=SimpleNamespace(
            run_single_package=lambda **_kwargs: SimpleNamespace(
                run_id="selection-run",
                runtime_config={
                    "daily_selection_evidence": {
                        "evidence_capture_status": "COMPLETE",
                        "evidence_schema_version_by_package": {spec.package_id: "daily_selection_evidence_v2"},
                    }
                },
            )
        ),
        conn_factory=object(),
    )
    service = RealDevHistoricalOnboardingService()
    monkeypatch.setattr(service, "_prepare_package_config", lambda **_kwargs: spec.runtime_config)
    monkeypatch.setattr(service, "_preflight_stages", lambda **_kwargs: {})
    monkeypatch.setattr(service, "_prospective_context", lambda **_kwargs: object())
    monkeypatch.setattr(service, "_validate_prospective_assembly", lambda **_kwargs: None)
    monkeypatch.setattr(service, "_assert_evidence_code_release", lambda **_kwargs: None)

    actual, selection_run_id = service._ensure_prospective_evidence(  # noqa: SLF001
        request=request,
        spec=spec,
        program=object(),
        binding=SimpleNamespace(runtime_config_json=spec.runtime_config),
        components=components,
    )

    assert actual is final_evidence
    assert selection_run_id == "selection-run"


def test_existing_dse_requires_exact_code_release(tmp_path: Path, onboarding_request) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)

    class Cursor:
        def __init__(self, row):
            self.row = row

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, _sql, _params):
            return None

        def fetchone(self):
            return self.row

    def factory_for(row):
        @contextmanager
        def conn_factory():
            yield SimpleNamespace(cursor=lambda **_kwargs: Cursor(row))

        return conn_factory

    exact = {
        "producer_code_release_id": request.code_release_id,
        "producer_code_release_hash": request.code_release_hash,
        "config_code_release_id": request.code_release_id,
        "config_code_release_hash": request.code_release_hash,
    }
    RealDevHistoricalOnboardingService._assert_evidence_code_release(  # noqa: SLF001
        evidence_id="evidence-exact-release",
        request=request,
        conn_factory=factory_for(exact),
    )

    with pytest.raises(RealDevOnboardingError, match="code release differs"):
        RealDevHistoricalOnboardingService._assert_evidence_code_release(  # noqa: SLF001
            evidence_id="evidence-old-release",
            request=request,
            conn_factory=factory_for(
                {
                    "producer_code_release_id": "2" * 40,
                    "producer_code_release_hash": "a" * 64,
                    "config_code_release_id": "2" * 40,
                    "config_code_release_hash": "a" * 64,
                }
            ),
        )
    with pytest.raises(RealDevOnboardingError, match="disappeared"):
        RealDevHistoricalOnboardingService._assert_evidence_code_release(  # noqa: SLF001
            evidence_id="evidence-missing",
            request=request,
            conn_factory=factory_for(None),
        )


def test_missing_v2_evidence_rejects_failed_capture_and_v1(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    spec = request.program_specs[0]

    def components_for(*, capture_status: str, schema: str):
        def load(**_kwargs):
            raise HistoricalResearchInputUnavailable("missing")

        return SimpleNamespace(
            program_resolver=SimpleNamespace(resolve=lambda **_kwargs: object()),
            evidence_adapter=SimpleNamespace(load=load),
            calendar_service=SimpleNamespace(next_trading_day=lambda *_args, **_kwargs: date(2026, 7, 22)),
            program_service=SimpleNamespace(
                _review_runtime_config=lambda _program, runtime: runtime,
                _with_advisory_date_context=lambda runtime, **_kwargs: runtime,
            ),
            selection_service=object(),
            artifact_service=SimpleNamespace(generate_from_live_inference=lambda **_kwargs: object()),
            selection_center=SimpleNamespace(
                run_single_package=lambda **_kwargs: SimpleNamespace(
                    run_id="selection-run",
                    runtime_config={
                        "daily_selection_evidence": {
                            "evidence_capture_status": capture_status,
                            "evidence_reason_codes": ["capture_failed"],
                            "evidence_schema_version_by_package": {spec.package_id: schema},
                        }
                    },
                )
            ),
        )

    service = RealDevHistoricalOnboardingService()
    monkeypatch.setattr(service, "_prepare_package_config", lambda **_kwargs: spec.runtime_config)
    monkeypatch.setattr(service, "_preflight_stages", lambda **_kwargs: {})
    monkeypatch.setattr(service, "_prospective_context", lambda **_kwargs: object())
    monkeypatch.setattr(service, "_validate_prospective_assembly", lambda **_kwargs: None)
    common = {
        "request": request,
        "spec": spec,
        "program": object(),
        "binding": SimpleNamespace(runtime_config_json=spec.runtime_config),
    }

    with pytest.raises(RealDevOnboardingError, match="complete DSE v2"):
        service._ensure_prospective_evidence(  # noqa: SLF001
            **common,
            components=components_for(capture_status="FAILED", schema="daily_selection_evidence_v1"),
        )
    with pytest.raises(RealDevOnboardingError, match="non-v2"):
        service._ensure_prospective_evidence(  # noqa: SLF001
            **common,
            components=components_for(capture_status="COMPLETE", schema="daily_selection_evidence_v1"),
        )


def test_reason_code_uses_context_then_fallback() -> None:
    assert RealDevHistoricalOnboardingService._reason_code(  # noqa: SLF001
        SimpleNamespace(context={"reason_code": "context_reason"}),
        fallback="fallback",
    ) == "context_reason"
    assert RealDevHistoricalOnboardingService._reason_code(Exception("x"), fallback="fallback") == "fallback"  # noqa: SLF001


def test_historical_store_is_content_addressed_and_idempotent(tmp_path: Path, onboarding_request) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    store = HistoricalOnboardingEvidenceStore(root=base.root)

    first = store.publish(request)
    second = store.publish(request)

    assert first.semantic_hash == request.historical_request_hash
    assert second.idempotent is True
    assert first.relative_path == second.relative_path
    (base.root / first.relative_path).write_text("{}", encoding="utf-8")
    with pytest.raises(RealDevOnboardingError, match="collision"):
        store.publish(request)


def test_historical_store_concurrent_same_identity_is_no_replace(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    store = HistoricalOnboardingEvidenceStore(root=base.root)

    def concurrent_publish(*, source: Path, target: Path) -> bool:
        shutil.copyfile(source, target)
        return False

    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding._publish_no_replace",
        concurrent_publish,
    )
    stored = store.publish(request)

    assert stored.idempotent is True
    assert (base.root / stored.relative_path).is_file()


def test_prospective_context_is_fully_typed_before_selection(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    spec = request.program_specs[0]
    repository = InMemoryAdvisoryProgramRepository()
    program_service = AdvisoryProgramService(
        repository=repository,
        selection_service=object(),
        calendar_provider=_Calendar(),
        symbol_name_resolver=object(),
        now_provider=lambda: NOW,
    )
    components = SimpleNamespace(program_repository=repository, program_service=program_service, conn_factory=object())
    service = RealDevHistoricalOnboardingService(now_provider=lambda: NOW)
    _program, binding = service._ensure_program(  # noqa: SLF001
        spec=spec,
        effective_from=request.binding_effective_from_trade_date,
        components=components,
    )
    package_config = normalize_selection_runtime_config(
        mark_non_trading_preview_runtime_config(spec.runtime_config, reason="historical advisory research evidence")
    )
    package_config = refresh_generated_runtime_profile_binding(package_config)
    package_config["point_in_time_context"] = {
        "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
        "trade_date": "2026-07-22",
        "cutoff_date": request.decision_trade_date.isoformat(),
        "score_trade_date": request.decision_trade_date.isoformat(),
        "reference_price_trade_date": request.decision_trade_date.isoformat(),
    }
    source_rows = [
        {
            "source_role": "daily_market",
            "dataset_id": "market.kline_daily_raw",
            "row_count": 10,
            "available_at": NOW,
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        }
    ]
    universe_hash = canonical_evidence_json_sha256(["000001.SZ"])
    candidate = SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1)
    artifact = SimpleNamespace(
        artifact_id="artifact-o3",
        artifact_payload_sha256="b" * 64,
        artifact_contract_version="selection_score_artifact_v2",
        package_id=spec.package_id,
        manifest_sha256="a" * 64,
        universe_count=1,
        source_revision_set_hash=canonical_evidence_json_sha256(source_rows),
        metadata={"source_read_receipts": source_rows, "artifact_input_context": {"universe_input_hash": universe_hash}},
    )
    receipt = SimpleNamespace(input_count=1, output_count=1, receipt_hash="f" * 64)
    preflight = {
        "signal": SimpleNamespace(snapshot=SimpleNamespace(candidates=[candidate])),
        "risk": SimpleNamespace(candidates=[candidate], exclusions=[], receipt=receipt),
        "tradability": SimpleNamespace(candidates=[candidate], exclusions=[], receipt=receipt),
    }
    monkeypatch.setattr(
        service,
        "_calendar_payload",
        lambda **_kwargs: [
            {"cal_date": request.decision_trade_date, "is_trading": True},
            {"cal_date": date(2026, 7, 22), "is_trading": True},
        ],
    )

    context = service._prospective_context(  # noqa: SLF001
        request=request,
        binding=binding,
        package_config=package_config,
        artifact=artifact,
        preflight=preflight,
        target_trade_date=date(2026, 7, 22),
        components=components,
    )

    assert context.capture_mode.value == "PROSPECTIVE"
    assert context.execution_origin.value == "ADVISORY_RUN"
    assert context.decision_clock_seed["decision_cutoff_ts"] == NOW
    assert context.decision_clock_seed["decision_clock_hash"]
    assert context.effective_config_seed["chain_hash"]
    layers = context.source_watermark_seed["universe_evidence"]["layers"]
    assert len(layers) == 6
    assert [item["status"] for item in layers] == [
        "NOT_APPLICABLE",
        "NOT_APPLICABLE",
        "NOT_APPLICABLE",
        "RESEARCH_ONLY",
        "FORMAL_READY",
        "FORMAL_READY",
    ]
    assert all(
        not item.get("source_revision_refs")
        for item in layers[:3]
    )
    assert all("not_materialized_in_o3_projection" not in item["exclusion_reason_counts"] for item in layers)


def test_prospective_context_rejects_source_observed_after_historical_cutoff(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    spec = request.program_specs[0]
    repository = InMemoryAdvisoryProgramRepository()
    historical_replay_at = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)
    program_service = AdvisoryProgramService(
        repository=repository,
        selection_service=object(),
        calendar_provider=_Calendar(),
        symbol_name_resolver=object(),
        now_provider=lambda: NOW,
    )
    components = SimpleNamespace(program_repository=repository, program_service=program_service, conn_factory=object())
    service = RealDevHistoricalOnboardingService(now_provider=lambda: historical_replay_at)
    _program, binding = service._ensure_program(  # noqa: SLF001
        spec=spec,
        effective_from=request.binding_effective_from_trade_date,
        components=components,
    )
    package_config = normalize_selection_runtime_config(
        mark_non_trading_preview_runtime_config(spec.runtime_config, reason="historical advisory research evidence")
    )
    package_config = refresh_generated_runtime_profile_binding(package_config)
    package_config["point_in_time_context"] = {
        "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
        "trade_date": "2026-07-22",
        "cutoff_date": request.decision_trade_date.isoformat(),
        "score_trade_date": request.decision_trade_date.isoformat(),
        "reference_price_trade_date": request.decision_trade_date.isoformat(),
    }
    observed_after_cutoff = datetime(2026, 7, 22, 2, 0, tzinfo=UTC)
    source_rows = [
        {
            "source_role": "daily_market",
            "dataset_id": "market.kline_daily_raw",
            "row_count": 10,
            "available_at": observed_after_cutoff,
            "admissibility": "PROSPECTIVE_FIRST_OBSERVED",
        }
    ]
    universe_hash = canonical_evidence_json_sha256(["000001.SZ"])
    candidate = SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1)
    artifact = SimpleNamespace(
        artifact_id="artifact-o3-late",
        artifact_payload_sha256="b" * 64,
        artifact_contract_version="selection_score_artifact_v2",
        package_id=spec.package_id,
        manifest_sha256="a" * 64,
        universe_count=1,
        source_revision_set_hash=canonical_evidence_json_sha256(source_rows),
        metadata={"source_read_receipts": source_rows, "artifact_input_context": {"universe_input_hash": universe_hash}},
    )
    receipt = SimpleNamespace(input_count=1, output_count=1, receipt_hash="f" * 64)
    preflight = {
        "signal": SimpleNamespace(snapshot=SimpleNamespace(candidates=[candidate])),
        "risk": SimpleNamespace(candidates=[candidate], exclusions=[], receipt=receipt),
        "tradability": SimpleNamespace(candidates=[candidate], exclusions=[], receipt=receipt),
    }
    monkeypatch.setattr(
        service,
        "_calendar_payload",
        lambda **_kwargs: [
            {"cal_date": request.decision_trade_date, "is_trading": True},
            {"cal_date": date(2026, 7, 22), "is_trading": True},
        ],
    )

    with pytest.raises(HistoricalResearchInputUnavailable) as excinfo:
        service._prospective_context(  # noqa: SLF001
            request=request,
            binding=binding,
            package_config=package_config,
            artifact=artifact,
            preflight=preflight,
            target_trade_date=date(2026, 7, 22),
            components=components,
        )

    assert excinfo.value.reason_code == "ADVISORY_DEV_ONBOARDING_INPUT_PENDING"
    assert excinfo.value.context["decision_cutoff_ts"] == "2026-07-22T09:25:00+08:00"
    assert excinfo.value.context["data_available_at"] == observed_after_cutoff.isoformat()


def test_universe_evidence_rejects_missing_sources_and_unreconciled_exclusions(
    tmp_path: Path,
    onboarding_request,
) -> None:
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    request = _historical_request(onboarding_request, base.publish(onboarding_request).ref, asset_root)
    candidate = SelectionCandidate(symbol="000001.SZ", score=0.9, rank=1)
    universe_hash = canonical_evidence_json_sha256([candidate.symbol])
    source_rows = [{"dataset_id": "market.kline_daily_raw", "available_at": NOW.isoformat()}]

    def artifact(source_receipts):
        return SimpleNamespace(
            artifact_id="artifact-o3",
            artifact_payload_sha256="b" * 64,
            artifact_contract_version="selection_score_artifact_v2",
            package_id=request.program_specs[0].package_id,
            manifest_sha256="a" * 64,
            universe_count=1,
            source_revision_set_hash=canonical_evidence_json_sha256(source_receipts),
            metadata={
                "source_read_receipts": source_receipts,
                "artifact_input_context": {"universe_input_hash": universe_hash},
            },
        )

    receipt = SimpleNamespace(input_count=1, output_count=1, receipt_hash="f" * 64)
    preflight = {
        "signal": SimpleNamespace(snapshot=SimpleNamespace(candidates=[candidate])),
        "risk": SimpleNamespace(candidates=[candidate], exclusions=[], receipt=receipt),
        "tradability": SimpleNamespace(candidates=[candidate], exclusions=[], receipt=receipt),
    }
    with pytest.raises(RealDevOnboardingError, match="no authoritative source receipts"):
        RealDevHistoricalOnboardingService._universe_evidence(  # noqa: SLF001
            request=request,
            artifact=artifact([]),
            preflight=preflight,
            available_at=NOW,
            policy_available_at=NOW,
        )

    preflight["risk"] = SimpleNamespace(
        candidates=[candidate],
        exclusions=[],
        receipt=SimpleNamespace(input_count=2, output_count=1, receipt_hash="e" * 64),
    )
    with pytest.raises(RealDevOnboardingError, match="exclusion reasons do not reconcile"):
        RealDevHistoricalOnboardingService._universe_evidence(  # noqa: SLF001
            request=request,
            artifact=artifact(source_rows),
            preflight=preflight,
            available_at=NOW,
            policy_available_at=NOW,
        )


def test_run_keeps_program_failures_independent_and_publishes_formal_status(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.repository_code_release",
        lambda _root: (CODE_RELEASE_ID, CODE_RELEASE_HASH),
    )
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    onboarding_ref = base.publish(onboarding_request).ref
    identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="10.0.0.2",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="a" * 64,
        environment_contract_hash="b" * 64,
    )
    request = _historical_request(
        onboarding_request,
        onboarding_ref,
        asset_root,
        target_identity_hash=database_identity_hash(identity),
    )

    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="b" * 64,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.resolve_database_connection",
        lambda **_kwargs: config,
    )

    @contextmanager
    def readonly(*_args, **_kwargs):
        yield object()

    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.readonly_onboarding_connection",
        readonly,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.FixedReadOnlyProjection",
        lambda _connection, _config: SimpleNamespace(identity=lambda: identity),
    )

    failed_spec, complete_spec = request.program_specs
    waiting_run = SimpleNamespace(
        program_id=failed_spec.program_id,
        status=HistoricalResearchRunStatus.WAITING_INPUT,
        program_payload_sha256=None,
        binding_version_id=None,
        binding_payload_hash=None,
        evidence_id=None,
        evidence_hash=None,
        artifact_id=None,
        artifact_payload_hash=None,
        program_run_id="run-waiting",
        reason_codes=["ADVISORY_PHASE0A2D_PROGRAM_INPUT_UNAVAILABLE"],
    )
    complete_run = SimpleNamespace(
        program_id=complete_spec.program_id,
        status=HistoricalResearchRunStatus.COMPLETE,
        program_payload_sha256="1" * 64,
        binding_version_id="binding-complete",
        binding_payload_hash="2" * 64,
        evidence_id="evidence-complete",
        evidence_hash="3" * 64,
        artifact_id="artifact-complete",
        artifact_payload_hash="4" * 64,
        program_run_id="run-complete",
        reason_codes=[],
    )
    formal = SimpleNamespace(
        batch_id="batch-o3",
        batch_key="5" * 64,
        status=HistoricalResearchRunStatus.WAITING_INPUT,
        receipt_hash="6" * 64,
        program_runs=[waiting_run, complete_run],
    )
    components = SimpleNamespace(
        historical_runner=SimpleNamespace(run=lambda _request: formal),
        trading_date_resolver=SimpleNamespace(require_completed_historical_trading_date=lambda **_kwargs: None),
        calendar_service=SimpleNamespace(ensure_trading_day=lambda _date: None),
    )
    attempted: list[str] = []

    class Service(RealDevHistoricalOnboardingService):
        def _build_components(self, **_kwargs):
            return components

        def _ensure_program(self, *, spec, **_kwargs):
            attempted.append(spec.program_id)
            return SimpleNamespace(program_id=spec.program_id), SimpleNamespace(binding_version_id=f"b-{spec.program_id}")

        def _ensure_prospective_evidence(self, *, spec, **_kwargs):
            if spec.program_id == failed_spec.program_id:
                raise RuntimeConfigInvalidError(
                    "selection failed before DSE publication",
                    context={"reason_code": "ADVISORY_O3_SELECTION_FAILED"},
                )
            return SimpleNamespace(evidence_id="evidence-complete"), "selection-complete"

    receipt, stored = Service(now_provider=lambda: NOW).run(
        request=request,
        env_file=tmp_path / ".env",
        evidence_root=base.root,
        target_package_asset_root=asset_root,
        repository_root=Path.cwd(),
    )

    assert attempted == [item.program_id for item in request.program_specs]
    assert receipt.batch_status == "FAILED"
    assert {item.program_id: item.status for item in receipt.program_results} == {
        failed_spec.program_id: HistoricalProgramStatus.FAILED,
        complete_spec.program_id: HistoricalProgramStatus.COMPLETE,
    }
    assert "ADVISORY_O3_SELECTION_FAILED" in next(
        item.reason_codes for item in receipt.program_results if item.program_id == failed_spec.program_id
    )
    assert stored.relative_path.startswith("historical-receipts/")


def test_run_keeps_program_provisioning_failures_independent(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.repository_code_release",
        lambda _root: (CODE_RELEASE_ID, CODE_RELEASE_HASH),
    )
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="10.0.0.2",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="a" * 64,
        environment_contract_hash="b" * 64,
    )
    request = _historical_request(
        onboarding_request,
        base.publish(onboarding_request).ref,
        asset_root,
        target_identity_hash=database_identity_hash(identity),
    )
    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="b" * 64,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.resolve_database_connection",
        lambda **_kwargs: config,
    )

    @contextmanager
    def readonly(*_args, **_kwargs):
        yield object()

    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.readonly_onboarding_connection",
        readonly,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.FixedReadOnlyProjection",
        lambda _connection, _config: SimpleNamespace(identity=lambda: identity),
    )
    failed_spec, complete_spec = request.program_specs
    failed_run = SimpleNamespace(
        program_id=failed_spec.program_id,
        status=HistoricalResearchRunStatus.FAILED,
        program_payload_sha256=None,
        binding_version_id=None,
        binding_payload_hash=None,
        evidence_id=None,
        evidence_hash=None,
        artifact_id=None,
        artifact_payload_hash=None,
        program_run_id="run-provisioning-failed",
        reason_codes=["ADVISORY_PHASE0A2D_PROGRAM_FAILED"],
    )
    complete_run = SimpleNamespace(
        program_id=complete_spec.program_id,
        status=HistoricalResearchRunStatus.COMPLETE,
        program_payload_sha256="1" * 64,
        binding_version_id="binding-complete",
        binding_payload_hash="2" * 64,
        evidence_id="evidence-complete",
        evidence_hash="3" * 64,
        artifact_id="artifact-complete",
        artifact_payload_hash="4" * 64,
        program_run_id="run-complete",
        reason_codes=[],
    )
    formal = SimpleNamespace(
        batch_id="batch-provisioning",
        batch_key="5" * 64,
        status=HistoricalResearchRunStatus.FAILED,
        receipt_hash="6" * 64,
        program_runs=[failed_run, complete_run],
    )
    components = SimpleNamespace(
        historical_runner=SimpleNamespace(run=lambda _request: formal),
        trading_date_resolver=SimpleNamespace(require_completed_historical_trading_date=lambda **_kwargs: None),
        calendar_service=SimpleNamespace(ensure_trading_day=lambda _date: None),
    )
    attempted_evidence: list[str] = []

    class Service(RealDevHistoricalOnboardingService):
        def _build_components(self, **_kwargs):
            return components

        def _ensure_program(self, *, spec, **_kwargs):
            if spec.program_id == failed_spec.program_id:
                raise RealDevOnboardingError("ADVISORY_O3_PROGRAM_PROVISION_FAILED", "program provisioning failed")
            return SimpleNamespace(program_id=spec.program_id), SimpleNamespace(binding_version_id=f"b-{spec.program_id}")

        def _ensure_prospective_evidence(self, *, spec, **_kwargs):
            attempted_evidence.append(spec.program_id)
            return SimpleNamespace(evidence_id="evidence-complete"), "selection-complete"

    receipt, stored = Service(now_provider=lambda: NOW).run(
        request=request,
        env_file=tmp_path / ".env",
        evidence_root=base.root,
        target_package_asset_root=asset_root,
        repository_root=Path.cwd(),
    )

    assert attempted_evidence == [complete_spec.program_id]
    assert receipt.batch_status == "FAILED"
    assert {item.program_id: item.status for item in receipt.program_results} == {
        failed_spec.program_id: HistoricalProgramStatus.FAILED,
        complete_spec.program_id: HistoricalProgramStatus.COMPLETE,
    }
    assert "ADVISORY_O3_PROGRAM_PROVISION_FAILED" in next(
        item.reason_codes for item in receipt.program_results if item.program_id == failed_spec.program_id
    )
    assert stored.relative_path.startswith("historical-receipts/")


def test_first_run_provisions_future_bindings_before_returning_input_pending(
    tmp_path: Path,
    onboarding_request,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.repository_code_release",
        lambda _root: (CODE_RELEASE_ID, CODE_RELEASE_HASH),
    )
    asset_root = tmp_path / "assets"
    asset_root.mkdir()
    base = RealDevOnboardingEvidenceStore(root=tmp_path / "evidence")
    identity = DatabaseIdentity(
        target_label=TargetLabel.DEV,
        current_database="aistock_dev",
        server_address="10.0.0.2",
        server_port=5432,
        server_version_num=160000,
        current_user_hash="a" * 64,
        environment_contract_hash="b" * 64,
    )
    request = _historical_request(
        onboarding_request,
        base.publish(onboarding_request).ref,
        asset_root,
        target_identity_hash=database_identity_hash(identity),
    )
    config = DatabaseConnectionConfig(
        target_label=TargetLabel.DEV,
        host="dev-host",
        port=5432,
        database="aistock_dev",
        user="dev-user",
        password="secret",
        environment_contract_hash="b" * 64,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.resolve_database_connection",
        lambda **_kwargs: config,
    )

    @contextmanager
    def readonly(*_args, **_kwargs):
        yield object()

    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.readonly_onboarding_connection",
        readonly,
    )
    monkeypatch.setattr(
        "backend.services.advisory_dev_input_onboarding.historical_onboarding.FixedReadOnlyProjection",
        lambda _connection, _config: SimpleNamespace(identity=lambda: identity),
    )
    repository = InMemoryAdvisoryProgramRepository()
    program_service = AdvisoryProgramService(
        repository=repository,
        selection_service=object(),
        calendar_provider=_Calendar(),
        symbol_name_resolver=object(),
        now_provider=lambda: NOW,
    )

    def pending(**_kwargs):
        raise RuntimeConfigInvalidError(
            "decision date is not completed",
            context={"reason_code": REASON_HISTORICAL_DATE_REQUIRED},
        )

    components = SimpleNamespace(
        program_repository=repository,
        program_service=program_service,
        calendar_service=SimpleNamespace(ensure_trading_day=lambda _date: None),
        trading_date_resolver=SimpleNamespace(require_completed_historical_trading_date=pending),
    )

    class Service(RealDevHistoricalOnboardingService):
        def _build_components(self, **_kwargs):
            return components

    with pytest.raises(RealDevOnboardingError) as excinfo:
        Service(now_provider=lambda: NOW).run(
            request=request,
            env_file=tmp_path / ".env",
            evidence_root=base.root,
            target_package_asset_root=asset_root,
            repository_root=Path.cwd(),
        )

    assert excinfo.value.reason_code == "ADVISORY_DEV_ONBOARDING_INPUT_PENDING"
    assert set(repository.programs) == {item.program_id for item in request.program_specs}
    assert len(repository.binding_versions) == 2
