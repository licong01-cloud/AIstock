"""LocalSIM source ownership and durable replay routing, without economic writes."""
from copy import deepcopy
from datetime import UTC, date, datetime
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.services.advisory_historical_range import input_source_plan as sources
from backend.services.simulation_runtime import localsim_input_source_plan as inputs
from backend.services.simulation_runtime.localsim_file_inputs import (
    capture_file_input_worker, current_file_dataset, resolve_replay_source,
)
from backend.services.simulation_runtime.localsim_product_control import LocalSimReplayCreateRequestV1
from backend.services.simulation_runtime.localsim_replay_runtime import LocalSimHistoricalReplayDayExecutor
from backend.tests.advisory_historical_range.test_input_source_plan import (
    CALENDAR, POLICY, _clocks, _plan, _snapshot,
)
from backend.tests.simulation_runtime.test_localsim_product_control_plane import _replay_request, _service


def _local_plan(tmp_path, mode="FILE_THEN_DATABASE"):
    return _plan(mode, _snapshot(tmp_path) if mode != "DATABASE_ONLY" else None,
                 consumer="LOCALSIM", run="localsim-own-source")


def test_old_missing_and_null_mode_preserve_request_json_and_fingerprint():
    old = _replay_request().model_dump(mode="json")
    assert "selection_input_source_mode" not in old
    parsed = LocalSimReplayCreateRequestV1.model_validate({**old, "selection_input_source_mode": None})
    assert parsed.model_dump(mode="json") == old
    assert inputs.request_identity(parsed) == inputs.request_identity(_replay_request())
    changed = parsed.model_copy(update={"selection_input_source_mode": "DATABASE_ONLY"})
    assert inputs.request_identity(changed) != inputs.request_identity(parsed)


def test_explicit_source_plan_reports_missing_native_pit_contract_without_inventing_policy():
    with pytest.raises(sources.SourcePlanError, match="LOCALSIM_SOURCE_NATIVE_PIT_LEASE_REQUIRED"):
        inputs.native_policy({})


def test_committed_recovery_preserves_complete_artifact_json_array_shape(tmp_path):
    from backend.tests.simulation_runtime.test_localsim_shared_selection_signal import _computed, _commit
    from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository

    scope, computed, program, _ = _computed(tmp_path, unscored=True)
    original = scope.artifacts[program.package_id]
    original.metadata["native_input_receipts"] = [{"roles": ["pit_universe", "market_history"]}]
    repository = InMemorySimulationRuntimeRepository()
    service, _, identity = _commit(scope, computed, program, repository)
    recovered = service._recover(scope.root.parent, identity)
    assert recovered.score_artifacts_by_package[program.package_id] == original
    assert service._recover(scope.root.parent, identity).score_artifacts_by_package == recovered.score_artifacts_by_package
    assert len(repository.daily_selection_evidences) == 1


def test_file_last_and_first_db_keep_local_C_D_T_and_full_window(tmp_path):
    plan = _local_plan(tmp_path)
    a, b = (inputs.day_route(plan, date.fromisoformat(v)) for v in CALENDAR[2:])
    assert a["source"] == "DATASET_FILES" and b["source"] == "DATABASE"
    assert a["data_cutoff"] == CALENDAR[1] and b["data_cutoff"] == CALENDAR[2]
    assert a["decision_date"] == a["target_trade_date"] == CALENDAR[2]
    assert b["history_start"] == CALENDAR[0]
    assert plan["first_database_day"] == CALENDAR[3]
    with pytest.raises(sources.SourcePlanError, match="COVERAGE_EXHAUSTED"):
        _local_plan(tmp_path / "file-only", "FILE_ONLY")


def test_file_and_db_worker_scope_readers_survive_parent_exit_and_do_not_leak(tmp_path):
    plan = _local_plan(tmp_path)
    workers = []
    for row in plan["days"]:
        day = date.fromisoformat(row["decision_date"])
        with inputs.localsim_plan_inputs(plan, trade_date=day):
            parent = current_file_dataset()
            def read():
                assert inputs.current_source_plan()["run_id"] == plan["run_id"]
                dataset = current_file_dataset()
                assert dataset is None or dataset is not parent
                return inputs.current_source_day(), dataset is not None
            workers.append(capture_file_input_worker(read, worker_id=str(day)))
        assert inputs.current_source_plan() is None and current_file_dataset() is None
    with ThreadPoolExecutor(max_workers=1) as executor:
        results = [executor.submit(worker).result() for worker in workers]
    assert results == [(date.fromisoformat(v), i < 2) for i, v in enumerate(CALENDAR[1:])]


def test_scope_policy_tamper_cross_consumer_and_input_holes_are_errors(tmp_path):
    plan = _local_plan(tmp_path)
    bad = deepcopy(plan)
    bad["consumer_scope"] = "ADVISORY"
    bad["plan_hash"] = sources.digest({k: v for k, v in bad.items() if k != "plan_hash"})
    with pytest.raises(sources.SourcePlanError, match="IDENTITY"):
        with inputs.localsim_plan_inputs(bad, trade_date=date.fromisoformat(CALENDAR[1])):
            pytest.fail("foreign consumer was admitted")
    with pytest.raises(sources.SourcePlanError, match="DAY_MISSING"):
        inputs.day_route(plan, date(2026, 2, 1))
    snapshot = _snapshot(tmp_path / "hole", gap="minute_calendar")
    with pytest.raises(sources.SourcePlanError, match="MINUTE_DAY_GAP"):
        _plan("FILE_THEN_DATABASE", snapshot, consumer="LOCALSIM", run="own")


def _planned_service(monkeypatch):
    service, repository, _ = _service()
    request = _replay_request().model_copy(update={"historical_source_id": inputs.SOURCE_PLAN_ID,
        "selection_input_source_mode": "DATABASE_ONLY"})
    service.authority.package_repository = SimpleNamespace(get=lambda _: SimpleNamespace(current_manifest=lambda: object()))
    service.historical_source_authority.calendar = SimpleNamespace(list_trading_days=lambda *_: [
        date(2026, 2, 26), request.start_trade_date, date(2026, 8, 27), request.end_trade_date])
    service.historical_source_authority.calendar.calendar_service = SimpleNamespace(status=lambda **_: {
        "cache": {"coverage_start": "2026-02-26"}})
    def freeze(**kwargs):
        lease = kwargs["runtime_config"]["canonical_pit_authority"]
        monkeypatch.setattr(sources, "freeze_runtime_pit_identity", lambda: {"mode": "canonical_v2_rolling_runtime", **lease})
        dates = kwargs["calendar"]
        return sources.build_source_plan(mode=kwargs["mode"], consumer_scope="LOCALSIM", run_id=kwargs["run_id"],
            days=[{"decision_date": str(d), "data_cutoff": str(dates[dates.index(d) - 1]),
                "target_trade_date": str(d), "history_start": str(date(2026, 2, 26))} for d in kwargs["trading_days"]],
            trading_calendar=[str(v) for v in kwargs["calendar"]], native_policy=inputs.native_policy(kwargs["runtime_config"]))
    monkeypatch.setattr(inputs, "freeze_replay_plan", freeze)
    return service, repository, request


def test_new_mode_atomic_creation_and_retry_reads_original_before_any_authority(monkeypatch):
    service, repository, request = _planned_service(monkeypatch)
    first = service.create_replay(request, created_by="source-contract")
    assert len(repository.accounts) == len(repository.replay_jobs) == 1
    saved = inputs.frozen_source_plan(first.release, job=first.replay)
    assert first.replay.historical_source_sha256 == saved["plan_hash"]
    service.authority.resolve_product = lambda **_: pytest.fail("create retry resolved current product")
    service.historical_source_authority.resolve = lambda **_: pytest.fail("create retry resolved current source")
    monkeypatch.setattr(sources, "freeze_file_source", lambda **_: pytest.fail("create retry resolved active file"))
    monkeypatch.setattr(sources, "freeze_runtime_pit_identity", lambda: pytest.fail("create retry resolved active PIT"))
    second = service.create_replay(request, created_by="source-contract")
    assert second == first and inputs.frozen_source_plan(second.release) == saved


def test_bundle_failure_rolls_back_account_plan_binding_and_job(monkeypatch):
    from backend.tests.simulation_runtime.test_localsim_product_control_plane import _FailReplayRepository
    service, _repository, request = _planned_service(monkeypatch)
    repository = _FailReplayRepository()
    service.repository = service.control.repository = repository
    with pytest.raises(RuntimeError, match="insert failure"):
        service.create_replay(request, created_by="source-contract")
    assert not repository.accounts and not repository.releases and not repository.bindings and not repository.replay_jobs


def test_file_to_database_creation_accepts_complete_plan_at_native_release_boundary(tmp_path, monkeypatch):
    from backend.services.simulation_runtime.localsim_product_control import LocalSimHistoricalSourceResolutionV1
    service, repository, request = _planned_service(monkeypatch)
    days = tuple(date.fromisoformat(value) for value in CALENDAR[1:])
    request = request.model_copy(update={"selection_input_source_mode": "FILE_THEN_DATABASE",
        "start_trade_date": days[0], "end_trade_date": days[-1]})
    source = LocalSimHistoricalSourceResolutionV1("market.kline_minute_raw.v1", "c" * 64, days,
        date(2026, 8, 31), date(2026, 8, 28))
    service.historical_source_authority.resolve = lambda **_: source
    def freeze(**kwargs):
        lease = kwargs["runtime_config"]["canonical_pit_authority"]
        monkeypatch.setattr(sources, "freeze_runtime_pit_identity", lambda: {"mode": "canonical_v2_rolling_runtime", **lease})
        policy = inputs.native_policy(kwargs["runtime_config"])
        file_source = _snapshot(tmp_path / "published", policy=policy)
        return sources.build_source_plan(mode=kwargs["mode"], consumer_scope="LOCALSIM", run_id=kwargs["run_id"],
            days=[{"decision_date": value, "data_cutoff": CALENDAR[index], "target_trade_date": value,
                   "history_start": CALENDAR[0]} for index, value in enumerate(CALENDAR[1:])],
            trading_calendar=CALENDAR, native_policy=policy, file_source=file_source)
    monkeypatch.setattr(inputs, "freeze_replay_plan", freeze)
    first = service.create_replay(request, created_by="source-contract")
    plan = inputs.frozen_source_plan(first.release, job=first.replay)
    assert [row["source"] for row in plan["routes"]] == ["DATASET_FILES", "DATASET_FILES", "DATABASE"]
    assert repository.get_release(first.release.release_id) == first.release
    monkeypatch.setattr(inputs, "freeze_replay_plan", lambda **_: pytest.fail("retry rebuilt file plan"))
    assert service.create_replay(request, created_by="source-contract") == first


def test_frozen_restore_does_not_call_live_source_or_extend_plan(monkeypatch):
    service, _, request = _planned_service(monkeypatch)
    first = service.create_replay(request, created_by="source-contract")
    authority = SimpleNamespace(clock=lambda: datetime(2026, 9, 2, tzinfo=UTC),
        calendar=SimpleNamespace(latest_trading_day_on_or_before=lambda _: date(2026, 9, 1)),
        resolve=lambda **_: pytest.fail("restore called live source resolver"))
    restored = resolve_replay_source(authority, job=first.replay, release=first.release)
    assert restored.trading_days[0] == request.start_trade_date
    assert restored.historical_source_sha256 == first.replay.historical_source_sha256
    with pytest.raises(ValueError, match="extended"):
        resolve_replay_source(authority, job=first.replay, release=first.release, end_trade_date=date(2026, 9, 1))


def test_formal_recovery_binds_saved_source_plan_without_resolving_active(monkeypatch):
    from backend.services.simulation_runtime.localsim_replay_recovery import LocalSimReplayRecoveryService, ECONOMIC_TABLES
    service, _, request = _planned_service(monkeypatch)
    first = service.create_replay(request, created_by="source-contract")
    job = first.replay.model_dump(mode="json")
    original_job = deepcopy(job)
    job.update(status="FAILED_TERMINAL", failure_code="TimeoutExpired")
    snapshot = {"job": job, "release": first.release.model_dump(mode="json"),
        "binding": first.binding.model_dump(mode="json"), "account": first.account.model_dump(mode="json"),
        "run": None, "account_in_flight_runs": [], "economic_counts": {key: 0 for key in ECONOMIC_TABLES}}
    authority = SimpleNamespace(clock=lambda: datetime(2026, 9, 2, tzinfo=UTC),
        calendar=SimpleNamespace(latest_trading_day_on_or_before=lambda _: date(2026, 9, 1)),
        resolve=lambda **_: pytest.fail("recover resolved current source"))
    recovery = LocalSimReplayRecoveryService(repository=object(), source_authority=authority,
        market_provider=object(), clock=lambda: datetime(2026, 9, 2, tzinfo=UTC))
    def inspect_preselection(_snapshot):
        assert inputs.current_source_plan()["plan_hash"] == first.replay.historical_source_sha256
        return {"recovery_phase": "PRESELECTION"}
    recovery._inspect_preselection = inspect_preselection
    assert recovery.evaluate(snapshot).ready
    assert inputs.current_source_plan() is None
    foreign = deepcopy(inputs.frozen_source_plan(first.release))
    foreign["run_id"] += "-foreign"
    foreign["plan_hash"] = sources.digest({key: value for key, value in foreign.items() if key != "plan_hash"})
    with inputs.localsim_plan_inputs(foreign, trade_date=first.replay.next_trade_date):
        assert not recovery.evaluate(snapshot).ready
    snapshot["job"]["historical_source_sha256"] = "f" * 64
    assert not recovery.evaluate(snapshot).ready
    assert first.replay.model_dump(mode="json") == original_job


def test_day_executor_routes_each_day_without_modifying_job_or_economics(tmp_path, monkeypatch):
    plan = _local_plan(tmp_path)
    release = SimpleNamespace(release_config_json={"metadata": {inputs.PLAN_KEY: plan, "selection_runtime_config": {}}})
    monkeypatch.setattr(inputs, "native_policy", lambda _: POLICY)
    job = SimpleNamespace(historical_source_id=inputs.SOURCE_PLAN_ID, historical_source_sha256=plan["plan_hash"],
        start_trade_date=date.fromisoformat(CALENDAR[1]), end_trade_date=date.fromisoformat(CALENDAR[-1]), release_id="owned-release")
    scheduler = SimpleNamespace(repository=SimpleNamespace(get_strategy_runtime_release=lambda _: release))
    executor = LocalSimHistoricalReplayDayExecutor(lifecycle_scheduler=scheduler)
    calls = []
    executor._run_day = lambda original, d, source: calls.append((original is job, d, source, current_file_dataset() is not None))
    for value in CALENDAR[1:]:
        executor(job, date.fromisoformat(value))
    assert [c[2:] for c in calls] == [("DATASET_FILES", True), ("DATASET_FILES", True), ("DB_HISTORICAL", False)]
    assert all(c[0] for c in calls) and inputs.current_source_plan() is None


def test_planned_signal_role_keeps_physical_route_and_does_not_retag_artifacts(tmp_path, monkeypatch):
    from backend.services.simulation_runtime.localsim_selection_signal import LocalSimSelectionSignalService
    from backend.services.simulation_runtime.localsim_selection_inference import own_scope
    from backend.services.simulation_signal.strategy_package_selection import StrategyPackageSelectionService
    from backend.services.selection_center.models import SelectionMode

    plan = _local_plan(tmp_path / "source")
    service = LocalSimSelectionSignalService.__new__(LocalSimSelectionSignalService)
    service._file_bound = True
    service.package_repository = SimpleNamespace(get=lambda _: SimpleNamespace(manifest_sha256="a" * 64))
    service.temporary_root = tmp_path / "selection"
    service._recover = lambda *_, **__: None
    calls = []
    def native(_self, **kwargs):
        scope = own_scope()
        calls.append((kwargs["data_source"], current_file_dataset() is not None,
                      scope.clocks.to_dict(), scope.source_plan["plan_hash"]))
        return SimpleNamespace(status="native")
    monkeypatch.setattr(StrategyPackageSelectionService, "run_selection", native)
    for row in plan["days"]:
        day = date.fromisoformat(row["decision_date"])
        route = inputs.day_route(plan, day)
        physical = "DATASET_FILES" if route["source"] == "DATASET_FILES" else "DB_HISTORICAL"
        with inputs.localsim_plan_inputs(plan, trade_date=day):
            service.run_selection(package_ids=["own-package"], trade_date=day, data_source=physical,
                                  mode=SelectionMode.SINGLE_PACKAGE, runtime_config={})
    assert [call[:2] for call in calls] == [("DB_HISTORICAL", True), ("DB_HISTORICAL", True), ("DB_HISTORICAL", False)]
    assert [call[2] for call in calls] == [{k: row[k] for k in ("decision_date", "data_cutoff", "target_trade_date")}
                                         for row in plan["days"]]
    assert all(call[3] == plan["plan_hash"] for call in calls)


def test_actual_input_receipt_and_full_score_drift_are_rejected(tmp_path):
    plan = _local_plan(tmp_path, "DATABASE_ONLY")
    route = inputs.day_route(plan, date.fromisoformat(CALENDAR[2]))
    context = {"effective_trade_date": route["data_cutoff"], "window_start_date": route["history_start"]}
    payload = {"input_context": context, "scores": [{"symbol": "600000.SH", "score": 1}], "universe_count": 2,
        "metadata": {"universe_members": ["600000.SH", "600001.SH"], "unscored_members": ["600001.SH"]},
        "source_read_receipts": [{"source_role": r, "content_hash": "a" * 64} for r in
            ("pit_universe", "market_history", "fundamental_moneyflow", "trading_calendar")]}
    payload["input_context"]["source_plan_receipt"] = sources.input_receipt(route, payload)
    inputs.validate_payload(plan, _clocks(plan["days"][1]), payload)
    for field in ("scores", "source_read_receipts", "universe_count", "metadata"):
        changed = deepcopy(payload)
        if field == "scores":
            changed[field][0]["score"] = 2
        elif field == "source_read_receipts":
            changed[field][0]["content_hash"] = "b" * 64
            changed["input_context"]["source_plan_receipt"] = sources.input_receipt(route, changed)
        elif field == "universe_count":
            changed[field] = 3
        else:
            changed[field]["unscored_members"] = []
        with pytest.raises(sources.SourcePlanError, match="DRIFT"):
            sources.verify_retry_inputs(payload, changed)


def test_planned_source_never_discards_explicit_manifest_inference_identity(tmp_path, monkeypatch):
    from backend.infra.wsl_qlib_runner import win_to_wsl_path
    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
    from backend.services.simulation_runtime.localsim_selection_inference import (
        LOCALSIM_INFERENCE_SCOPE, LocalSimInferenceScope, LocalSimReferencedAssetResolver,
    )
    from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
    from backend.tests.strategy_package.test_runtime_package_assets_batch2 import _frozen_manifest
    from backend.tests.simulation_runtime.test_localsim_native_context import _bundle, _canonical_profile, _canonical_resolver

    *_, release, _ = _bundle(_canonical_profile(), pit_authority_resolver=_canonical_resolver())
    config = release.release_config_json["metadata"]["selection_runtime_config"]
    declared = {"mode": "canonical_v2_frozen_candidate", "release_id": "another-frozen-source"}
    store = LocalPackageAssetStore(tmp_path / "assets")
    manifest = _frozen_manifest(store)
    manifest = manifest.model_copy(update={"source_evidence": {**manifest.source_evidence, "pit_identity": declared}})
    day = date(2026, 1, 5)
    scope = LocalSimInferenceScope("own-source", tmp_path / "run", SelectionSignalDayV1(day, day, day), {},
                                   source_plan={"run_id": "own-plan"})
    closed = []
    monkeypatch.setattr(sources, "resolve_day_source", lambda *_: {"source": "DATABASE"})
    monkeypatch.setattr(sources, "create_input_session", lambda *_, **__: SimpleNamespace(close=lambda: closed.append(True)))
    def reject(_session, *, request_identity, manifest_identity):
        assert manifest_identity == declared
        assert request_identity is None
        raise sources.SourcePlanError("SOURCE_DECLARED_PIT_IDENTITY_MISMATCH")
    monkeypatch.setattr(sources, "session_pit_identity", reject)
    resolver = LocalSimReferencedAssetResolver(asset_store=store, cache_root=tmp_path / "cache")
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        source = resolver.load_source_for_strategy_package(manifest=manifest, package_id=manifest.package_id)
        with pytest.raises(sources.SourcePlanError, match="SOURCE_DECLARED_PIT_IDENTITY_MISMATCH"):
            resolver.prepare_workspace(package_id=manifest.package_id, manifest_sha256=manifest.manifest_sha256,
                source=source, path_converter=win_to_wsl_path, runtime_config=config)
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    assert closed == [True]


def test_catchup_freezes_a_new_segment_and_live_drops_historical_plan(monkeypatch):
    from backend.services.simulation_runtime.localsim_replay_runtime import LocalSimReplayLifecycleOwner
    from backend.services.simulation_runtime.successor_models import LocalSimReplayStatus
    service, repository, request = _planned_service(monkeypatch)
    first = service.create_replay(request, created_by="source-contract")
    original_plan = inputs.frozen_source_plan(first.release, job=first.replay)
    dates = [date(2026, 2, 25)] + [date.fromisoformat(v) for v in original_plan["trading_calendar"]] + [date(2026, 8, 31), date(2026, 9, 1)]
    calendar = service.historical_source_authority.calendar
    calendar.calendar_service.status = lambda **_: {"cache": {"coverage_start": "2026-02-25"}}
    calendar.list_trading_days = lambda start, end: [d for d in dates if start <= d <= end]
    original = repository.transition_replay_job(replay_job_id=first.replay.replay_job_id,
        expected_version=first.replay.version, update={"status": LocalSimReplayStatus.CAUGHT_UP,
            "completed_trade_date": first.replay.end_trade_date, "next_trade_date": None},
        updated_at=datetime(2026, 9, 2, tzinfo=UTC))
    owner = LocalSimReplayLifecycleOwner(repository=repository, control=service.control, product_authority=service.authority,
        historical_source_authority=service.historical_source_authority, historical_day_executor=lambda *_: None,
        safe_boundary_authority=SimpleNamespace())
    child = owner._create_catchup_successor(job=original, latest_completed_trade_date=date(2026, 9, 1),
        as_of_time=datetime(2026, 9, 2, tzinfo=UTC))
    child_release = repository.get_release(child.release_id)
    child_plan = inputs.frozen_source_plan(child_release, job=child)
    assert child.simulation_account_id == first.account.account_id
    assert child_plan["run_id"] != original_plan["run_id"]
    assert child_plan["plan_hash"] != original_plan["plan_hash"]
    assert child_plan["trading_calendar"][0] == "2026-02-25"
    assert child.start_trade_date == date(2026, 8, 31) and child.end_trade_date == date(2026, 9, 1)
    assert inputs.REQUEST_KEY not in child_release.release_config_json["metadata"]
    assert inputs.frozen_source_plan(repository.get_release(first.release.release_id)) == original_plan
    _, _, live, _ = owner._build_successor_bundle(job=child, effective_from=date(2026, 9, 2))
    assert inputs.PLAN_KEY not in live.release_config_json["metadata"]
    assert inputs.REQUEST_KEY not in live.release_config_json["metadata"]


def test_committed_recovery_refuses_scores_from_a_different_source_plan(tmp_path):
    from backend.services.simulation_runtime.localsim_input_source_plan import validate_artifact_source
    from backend.tests.simulation_runtime.test_localsim_shared_selection_signal import _computed
    scope, _, program, _ = _computed(tmp_path / "raw")
    artifact = scope.artifacts[program.package_id]
    plan = _local_plan(tmp_path / "source", "DATABASE_ONLY")
    # An existing committed native artifact is not permission to use another
    # task's source plan, even with identical models and nominal dates.
    with pytest.raises(sources.SourcePlanError, match="SOURCE_PLAN_CONFLICT"):
        validate_artifact_source(plan, scope.clocks, artifact)


def test_kernel_handoff_preserves_admitted_canonical_lease_for_native_risk(tmp_path):
    from dataclasses import replace
    from backend.tests.simulation_runtime.test_localsim_shared_selection_signal import _computed
    from backend.tests.simulation_runtime.test_localsim_native_context import _canonical_resolver
    from backend.services.selection_center.canonical_pit_runtime import CANONICAL_PIT_RUNTIME_LEASE_KEY, SelectionPitRuntimeLease
    from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
    from backend.services.selection_center.models import SelectionMode
    from backend.services.strategy_package.selection_computation import StrategyPackageSelectionComputationRequestV1, selection_runtime_profile_sha256
    from backend.services.simulation_runtime.localsim_selection_signal import LocalSimKernelComputation
    from backend.services.simulation_runtime.localsim_selection_inference import LOCALSIM_INFERENCE_SCOPE

    scope, _, program, producer = _computed(tmp_path)
    lease = SelectionPitRuntimeLease.from_binding(_canonical_resolver().binding).as_dict()
    profile = parse_selection_runtime_profile({**program.runtime_config, CANONICAL_PIT_RUNTIME_LEASE_KEY: lease})
    assert "canonical_pit_runtime_lease" not in profile.model_dump(mode="json")["risk_policy"]
    providers = producer._signal_kernel._providers
    observed = []
    class ObservedRisk:
        def evaluate(self, **kwargs):
            observed.append(kwargs["profile"].canonical_pit_runtime_lease)
            return providers.risk_policy.evaluate(**kwargs)
        def apply_to_candidates_with_receipt(self, **kwargs):
            return providers.risk_policy.apply_to_candidates_with_receipt(**kwargs)
    request = StrategyPackageSelectionComputationRequestV1(trade_date=scope.clocks.decision_date,
        data_source="DB_HISTORICAL", selection_mode=SelectionMode.SINGLE_PACKAGE,
        ordered_package_ids=(program.package_id,), package_runtime_profiles={program.package_id: profile},
        package_runtime_profile_hashes={program.package_id: selection_runtime_profile_sha256(profile)},
        package_top_k={program.package_id: 2})
    token = LOCALSIM_INFERENCE_SCOPE.set(scope)
    try:
        result = LocalSimKernelComputation().compute(request=request,
            prepared_signals={program.package_id: producer._signal_preparation.result.prepared_signal},
            providers=replace(providers, risk_policy=ObservedRisk()))
    finally:
        LOCALSIM_INFERENCE_SCOPE.reset(token)
    assert observed == [lease]
    assert len(result.aggregate_results) == 2


def test_pit_migration_cannot_silently_rebind_a_frozen_source_plan(monkeypatch):
    from backend.services.simulation_runtime.localsim_pit_migration import build_pit_successor
    from backend.tests.simulation_runtime.test_localsim_native_context import _canonical_resolver
    from backend.services.trading_core.errors import InvalidStateTransitionError
    service, _, request = _planned_service(monkeypatch)
    first = service.create_replay(request, created_by="source-contract")
    with pytest.raises(InvalidStateTransitionError) as raised:
        build_pit_successor(base=first.release, binding=first.binding, start=first.replay.start_trade_date,
            end=first.replay.end_trade_date, resolver=_canonical_resolver(), evidence={}, explicit_migration=True)
    assert raised.value.context["reason_code"] == "LOCALSIM_PIT_FROZEN_SOURCE_PLAN_REBIND_REQUIRED"


@pytest.mark.parametrize("corruption", [None, "member", "digest", "count", "parameters"])
def test_frozen_compact_receipt_is_validated_against_complete_native_request(monkeypatch, corruption):
    from backend.services.canonical_pit_inference_boundary import resolve_inference_pit_identity
    from backend.services.selection_center.canonical_pit_runtime import SelectionPitRuntimeLease
    from backend.services.simulation_runtime.localsim_selection_inference import _require_native_pit_receipt
    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
    from backend.tests.simulation_runtime.test_localsim_native_context import _canonical_resolver
    lease = SelectionPitRuntimeLease.from_binding(_canonical_resolver().binding)
    codes = ["000001.SZ", "600000.SH"]
    full = {"mode": "canonical_v2_frozen_candidate", "authority_id": lease.authority_id,
        "rule_version": lease.rule_version, "rule_parameters_digest": lease.rule_parameters_digest,
        "release_id": "localsim-test-file", "cutoff": "2026-08-31", "snapshot_digest": "a" * 64,
        "universe_as_of": "2026-08-28", "universe_codes": codes}
    native = resolve_inference_pit_identity(full, version_tag="localsim", allow_active_canonical_pointer=False)
    assert "universe_codes" not in native.as_dict()
    binding_hash = "b" * 64
    clocks = SelectionSignalDayV1(date(2026, 8, 31), date(2026, 8, 28), date(2026, 8, 31))
    payload = {"universe_count": 2, "metadata": {"universe_members": codes.copy()}, "input_context": {
        "pit_identity": native.as_dict(), "pit_mode": native.receipt_mode, "frozen_file_binding_hash": binding_hash,
        "universe_input_hash": "c" * 64}, "source_read_receipts": [{"source_role": "pit_universe",
            "dataset_id": "dataset_files.day_selection_universe", "partition_ref": "2026-08-28",
            "query_template_id": "FrozenDataset.day_selection_universe", "query_template_version": "frozen_market_dataset_files_v1",
            "row_count": 2, "content_hash": "c" * 64,
            "parameter_hash": sources.digest({"native_parameters": sources.digest({"trade_date": "2026-08-28",
                "ensure": False, "pit_identity": native.as_dict()}), "file_binding_hash": binding_hash})}]}
    if corruption == "member":
        payload["metadata"]["universe_members"][1] = "600001.SH"
    elif corruption == "digest":
        payload["input_context"]["pit_identity"]["universe_codes_digest"] = "d" * 64
    elif corruption == "count":
        payload["universe_count"] = 1
    elif corruption == "parameters":
        payload["source_read_receipts"][0]["parameter_hash"] = "e" * 64
    if corruption is None:
        _require_native_pit_receipt(payload, lease.as_dict(), clocks, file_binding_hash=binding_hash, planned_identity=full)
    else:
        with pytest.raises(ValueError):
            _require_native_pit_receipt(payload, lease.as_dict(), clocks, file_binding_hash=binding_hash, planned_identity=full)


@pytest.mark.parametrize("mutation", ["same_day_C", "target_T", "day_gap"])
def test_local_clocks_reject_other_consumers_legal_but_wrong_date_policy(tmp_path, mutation):
    plan = _local_plan(tmp_path, "DATABASE_ONLY")
    if mutation == "same_day_C":
        plan["days"][0]["data_cutoff"] = plan["days"][0]["decision_date"]
    elif mutation == "target_T":
        plan["days"][0]["target_trade_date"] = plan["days"][0]["data_cutoff"]
    else:
        plan["days"].pop(1)
    with pytest.raises(sources.SourcePlanError, match="CLOCK_MISMATCH|DAY_GAP"):
        inputs.validate_local_clocks(plan)


def test_existing_dev_source_plan_registration_readback_retry_and_insert_rollback(monkeypatch):
    import os
    if os.getenv("AISTOCK_DEV_DB_E2E") != "1":
        pytest.skip("existing DEV integration is an explicit gate")
    import psycopg2
    from contextlib import contextmanager
    from uuid import uuid4
    from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn
    from backend.tests.simulation_runtime.test_localsim_successor_core_postgres import _package_identity, _RuntimeProfileAuthority
    from backend.services.simulation_runtime.localsim_runtime_profile_service import LocalSimRuntimeProfileService
    from backend.services.simulation_runtime.localsim_runtime_profile_repository import LocalSimRuntimeProfileRepository
    from backend.services.simulation_runtime.successor_repository import LocalSimSuccessorRepository

    dsn = _dev_dsn()
    package, manifest = _package_identity(dsn)
    conn = psycopg2.connect(**dsn)
    marker = "test_int_source_plan_" + uuid4().hex
    @contextmanager
    def factory():
        savepoint = "source_plan_" + uuid4().hex
        with conn.cursor() as cur:
            cur.execute(f"SAVEPOINT {savepoint}")
        try:
            yield conn
        except Exception:
            with conn.cursor() as cur:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            raise
        finally:
            with conn.cursor() as cur:
                cur.execute(f"RELEASE SAVEPOINT {savepoint}")
    try:
        service, _, request = _planned_service(monkeypatch)
        original_resolved = service.authority.resolve_product(package_id="pkg_alpha", runtime_profile_version_id="lsrpv_001",
                                                             execution_policy_version_id="execpol_twap")
        profiles = LocalSimRuntimeProfileService(repository=LocalSimRuntimeProfileRepository(conn_factory=factory),
            authority=_RuntimeProfileAuthority(), clock=lambda: datetime(2026, 8, 31, tzinfo=UTC))
        profile = profiles.create_profile(package_id=package, manifest_sha256=manifest, profile_name=marker, created_by=marker)
        from backend.services.simulation_runtime.localsim_runtime_profile import LocalSimRuntimeProfileConfigRequestV1
        profile_config = {k: v for k, v in original_resolved.runtime_profile_version.config_json.items()
                          if k in LocalSimRuntimeProfileConfigRequestV1.model_fields}
        _, version = profiles.create_version(profile_id=profile.profile_id, expected_profile_version=profile.version,
            config_json={**profile_config,
                         "schema_version": "localsim_runtime_profile_config_request_v1"}, created_by=marker)
        resolved = SimpleNamespace(**vars(original_resolved))
        resolved.package_id, resolved.manifest_sha256 = package, manifest
        resolved.runtime_profile, resolved.runtime_profile_version = profile, version
        service.authority.resolve_product = lambda **_: resolved
        request = request.model_copy(update={"account_name": marker, "package_id": package,
            "runtime_profile_version_id": version.profile_version_id})
        repository = LocalSimSuccessorRepository(conn_factory=factory)
        service.repository = service.control.repository = repository
        first = service.create_replay(request, created_by=marker)
        readback = repository.find_replay_by_source_request(inputs.request_identity(request))
        assert readback == first.replay
        assert inputs.frozen_source_plan(repository.get_release(first.release.release_id), job=readback)["plan_hash"] == readback.historical_source_sha256
        saved_resolve = service.authority.resolve_product
        service.authority.resolve_product = lambda **_: pytest.fail("durable retry resolved active")
        assert service.create_replay(request, created_by=marker) == first
        service.authority.resolve_product = saved_resolve
        class BrokenRepository(LocalSimSuccessorRepository):
            @staticmethod
            def _insert_replay(_cur, _job):
                raise RuntimeError("injected SQL source-plan job insert failure")
        broken = BrokenRepository(conn_factory=factory)
        service.repository = service.control.repository = broken
        bad_request = request.model_copy(update={"account_name": marker + "_rollback"})
        with pytest.raises(RuntimeError, match="SQL source-plan"):
            service.create_replay(bad_request, created_by=marker)
        assert repository.find_replay_by_source_request(inputs.request_identity(bad_request)) is None
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM paper_v2.simulation_account_v1 WHERE account_name = %s", (bad_request.account_name,))
            assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()
