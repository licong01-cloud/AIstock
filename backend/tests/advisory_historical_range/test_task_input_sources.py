"""Real task boundaries; public source-plan mathematics have their own tests."""
from copy import deepcopy
from datetime import date

import pytest

from backend.services.advisory_historical_range.api_models import HistoricalRangeCreateRequest
from backend.services.advisory_historical_range.models import HistoricalRangeResearchBatchRequestV1
from backend.tests.advisory_historical_range.conftest import frozen_program, research_spec


@pytest.fixture
def authority(monkeypatch):
    from backend.services.canonical_equity_pit import (
        CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION,
        CanonicalPitAuthorityResolver, PitConsumerBinding, PitAuthorityStatus, canonical_rule_parameters_digest,
    )
    binding = PitConsumerBinding(authority_id=CANONICAL_PIT_AUTHORITY_ID,
        authority_status=PitAuthorityStatus.ACTIVE_CANONICAL, universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION, rule_parameters_digest=canonical_rule_parameters_digest(),
        activation_generation=1, activation_envelope_digest="a" * 64, expected_source_commit="fixture",
        state_source_digest="b" * 64, coverage_start=date(2018, 8, 1), coverage_end=date(2026, 10, 19))
    calls = []
    monkeypatch.setattr(CanonicalPitAuthorityResolver, "resolve_live_binding", lambda _: calls.append(1) or binding)
    return calls


def _task(mode, *, dates=("2026-01-20", "2026-01-21", "2026-01-22")):
    from backend.tests.advisory_historical_range.conftest import date_plan

    spec = research_spec()
    request = HistoricalRangeResearchBatchRequestV1(program_specs=(spec,), client_idempotency_key="source-task",
        start_trade_date=dates[0], end_trade_date=dates[-1], selection_input_source_mode=mode)
    program = frozen_program(spec)
    original = date_plan(trade_dates=tuple(map(date.fromisoformat, dates)), research_program_ids=(program.research_program_id,))
    payload = original.model_dump(mode="json")
    payload["date_plan_hash"] = payload["per_program_input_warmup_ranges_hash"] = None
    for warmup in payload["per_program_input_warmup_ranges"].values():
        for component in warmup["components"]:
            component["warmup_start_trade_date"] = "2026-01-19"
            for window in component["day_windows"]:
                window["window_start_trade_date"] = "2026-01-19"
    return request, program, type(original).model_validate(payload)


@pytest.mark.parametrize("mode", ["FILE_ONLY", "DATABASE_ONLY", "FILE_THEN_DATABASE"])
def test_creation_accepts_explicit_selection_mode_and_binds_user_identity(mode):
    spec = research_spec()
    fields = dict(program_specs=(spec,), start_trade_date=date(2026, 6, 1),
                  end_trade_date=date(2026, 6, 3), client_idempotency_key="own-create")
    legacy = HistoricalRangeResearchBatchRequestV1(**fields)
    planned = HistoricalRangeResearchBatchRequestV1(**fields, selection_input_source_mode=mode)
    assert planned.user_request_semantic_hash != legacy.user_request_semantic_hash
    api = HistoricalRangeCreateRequest(program_specs=[spec.model_dump(mode="json")],
        start_trade_date=fields["start_trade_date"], end_trade_date=fields["end_trade_date"],
        selection_input_source_mode=mode)
    assert api.selection_input_source_mode == mode
    assert "selection_input_source_mode" not in legacy.semantic_payload()
    assert "selection_input_source_mode" not in legacy.model_dump(mode="json")


def test_legacy_program_bytes_and_hash_survive_round_trip_without_new_plan():
    program = frozen_program(research_spec())
    payload = program.model_dump(mode="json")
    assert "selection_input_source_plan" not in payload
    assert type(program).model_validate(payload).frozen_program_hash == program.frozen_program_hash


def test_unknown_source_mode_is_not_a_silent_database_fallback():
    with pytest.raises(ValueError):
        HistoricalRangeCreateRequest(program_specs=[research_spec().model_dump(mode="json")],
            start_trade_date="2026-06-01", end_trade_date="2026-06-03",
            selection_input_source_mode="TRY_FILE_OTHERWISE_DB")


@pytest.mark.parametrize("mode,sources_expected", [
    ("FILE_ONLY", ["DATASET_FILES", "DATASET_FILES"]),
    ("DATABASE_ONLY", ["DATABASE", "DATABASE", "DATABASE"]),
    ("FILE_THEN_DATABASE", ["DATASET_FILES", "DATASET_FILES", "DATABASE"]),
])
def test_actual_frozen_program_persists_modes_and_retry_never_resolves_active(
        tmp_path, monkeypatch, authority, mode, sources_expected):
    from backend.services.advisory_historical_range import input_source_plan as sources
    from backend.services.advisory_historical_range.task_input_sources import (
        freeze_program_input_sources, validate_program_source_plan,
    )
    from backend.tests.advisory_historical_range.test_input_source_plan import _snapshot, _canonical_policy

    dates = ("2026-01-20", "2026-01-21") if mode == "FILE_ONLY" else ("2026-01-20", "2026-01-21", "2026-01-22")
    request, program, dates = _task(mode, dates=dates)
    if mode == "DATABASE_ONLY":
        monkeypatch.setattr(sources, "freeze_file_source", lambda **_: pytest.fail("database mode read active file profile"))
    else:
        snapshot = _snapshot(tmp_path, policy=_canonical_policy())
        monkeypatch.setattr(sources, "freeze_file_source", lambda **_: deepcopy(snapshot))
    result, = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=(program,))
    assert result.frozen_program_hash != program.frozen_program_hash
    plan = result.selection_input_source_plan
    assert [r["source"] for r in plan["routes"]] == sources_expected
    assert all(r["history_start"] == "2026-01-19" for r in plan["routes"])
    assert plan["run_id"].startswith("ahrinput_")
    frozen = type(result).model_validate(result.model_dump(mode="json"))
    monkeypatch.setattr(sources, "freeze_file_source", lambda **_: pytest.fail("restore re-read active"))
    assert validate_program_source_plan(program=frozen, date_plan=dates, request=request) == plan
    assert authority == ([] if mode == "FILE_ONLY" else [1])
    # A different task request or omitted date is never repaired from active.
    other = request.model_copy(update={"selection_input_source_mode": "DATABASE_ONLY" if mode != "DATABASE_ONLY" else "FILE_ONLY"})
    with pytest.raises(ValueError, match="TASK_REQUEST_MISMATCH"):
        validate_program_source_plan(program=frozen, date_plan=dates, request=other)


def test_planned_catalog_owns_declarations_not_fake_database_market_content(authority):
    from backend.services.advisory_historical_range.requirement_planner import HistoricalRangeSourceRequirementPlanner
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources
    from backend.tests.advisory_historical_range.conftest import digest

    request, program, dates = _task("DATABASE_ONLY")
    programs = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=(program,))
    plan = HistoricalRangeSourceRequirementPlanner().build(request=request, date_plan=dates, frozen_programs=programs,
        calendar_identity_hash=digest("calendar"), code_release_hash=program.code_release_hash)
    forbidden = {"market_history", "fundamental_moneyflow", "pit_universe", "trading_calendar", "suspend", "st_risk", "industry"}
    assert not forbidden.intersection(r.source_role for r in plan.requirements)
    declarations = [r for r in plan.requirements if r.source_role == "selection_input_plan"]
    assert len(declarations) == len(dates.ordered_trade_dates)
    assert all(r.parameter_template["content_hash"] == programs[0].selection_input_source_plan["plan_hash"]
               and r.parameter_template["identity_kind"] == "FROZEN_PLAN_DECLARATION_NOT_MARKET_CONTENT"
               for r in declarations)
    assert {"decision_mark_daily_market", "decision_mark_market_state"}.issubset(r.source_role for r in plan.requirements)
    from types import SimpleNamespace
    from backend.services.advisory_historical_range.catalog_postgres import _member_matches_research_program
    from backend.services.advisory_historical_range.source_roles import _matches_program_day

    declaration = declarations[0]
    member = SimpleNamespace(source_role=declaration.source_role, bound_parameters=declaration.parameter_template,
        decision_trade_date=declaration.decision_trade_date, package_id=program.package_id, component_id=None)
    for program_id, expected in ((program.research_program_id, True), ("another-program-same-package", False)):
        assert _member_matches_research_program(member, research_program_id=program_id) is expected
        assert _matches_program_day(member, research_program_id=program_id, package_id=program.package_id,
            component_ids=frozenset({"alpha"}), decision_trade_date=declaration.decision_trade_date) is expected


def test_exact_creation_retry_returns_durable_operations_before_any_resolver(authority):
    from types import SimpleNamespace
    from backend.services.advisory_historical_range.planning_service import HistoricalRangePlanningService

    request, _, _ = _task("DATABASE_ONLY")
    existing = SimpleNamespace(batch_id="own-batch", catalog_operation_id="original-op", idempotent=True)
    service = object.__new__(HistoricalRangePlanningService)
    service._repository = SimpleNamespace(find_existing_source_planning_batch=lambda **_: existing)
    # No other dependency is populated: accessing one would fail this exact retry.
    assert service.create(request) is existing
    assert authority == []


def _planned_producer(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from dataclasses import replace
    from types import SimpleNamespace
    from backend.services.advisory_historical_range import input_source_plan as sources
    from backend.services.advisory_historical_range.canonical import canonical_json_sha256 as digest
    from backend.services.advisory_historical_range.models import ResolvedHistoricalRangeRequestV1
    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources
    from backend.tests.advisory_historical_range.test_r2b_candidate_producer import _fixture, TRADE_DATE

    producer, old, verifier, program = _fixture(tmp_path)
    baseline = producer.produce(request_payload=old, research_program_id=program.research_program_id, decision_trade_date=TRADE_DATE)
    signal = producer._signal_preparation.result
    context = deepcopy(signal.raw.artifact.metadata["artifact_input_context"])
    dates = old.resolved_request.date_plan.model_dump(mode="json")
    dates["date_plan_hash"] = dates["per_program_input_warmup_ranges_hash"] = None
    for component in dates["per_program_input_warmup_ranges"][program.research_program_id]["components"]:
        component["warmup_start_trade_date"] = context["window_start_date"]
        component["day_windows"][0]["window_start_trade_date"] = context["window_start_date"]
    dates = type(old.resolved_request.date_plan).model_validate(dates)
    request = HistoricalRangeResearchBatchRequestV1(program_specs=(research_spec(package_id=program.package_id),),
        client_idempotency_key="new-source-producer", start_trade_date=TRADE_DATE, end_trade_date=TRADE_DATE,
        selection_input_source_mode="DATABASE_ONLY")
    program, = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=(program,))
    plan = program.selection_input_source_plan
    route = sources.resolve_day_source(plan, SelectionSignalDayV1(TRADE_DATE, TRADE_DATE, TRADE_DATE))
    context["source_plan_receipt"] = sources.input_receipt(route,
        {"source_read_receipts": list(signal.raw.source_read_receipts), "input_context": context})
    metadata = deepcopy(signal.raw.artifact.metadata)
    # Match the real native artifact boundary: its v2 normalized context does
    # not carry arbitrary source-plan fields. Preserve the provider's original
    # context separately; never rewrite the normalized v2 identity to add them.
    metadata["artifact_input_context"] = {k: v for k, v in context.items()
        if k not in {"source_plan_receipt", "frozen_file_binding_hash"}}
    metadata["provider_metadata"] = {"selection_native_input_context": context}
    context_hash = digest(metadata["artifact_input_context"])
    artifact = signal.raw.artifact.model_copy(update={"metadata": metadata, "artifact_input_context_hash": context_hash})
    payload_hash = digest(artifact.canonical_v2_header(score_hash=artifact.artifact_sha256))
    artifact = artifact.model_copy(update={"artifact_payload_sha256": payload_hash})
    raw = replace(signal.raw, artifact=artifact, semantic_header={**signal.raw.semantic_header,
        "artifact_input_context_hash": context_hash})
    prepared = replace(signal.prepared_signal, input_context_hash=context_hash, artifact_header=replace(
        signal.prepared_signal.artifact_header, artifact_input_context_hash=context_hash, artifact_payload_sha256=payload_hash))
    producer._signal_preparation.result = replace(signal, raw=raw, prepared_signal=prepared)
    resolved = ResolvedHistoricalRangeRequestV1(batch_id="ahrb_source_task", request=request,
        frozen_programs=(program,), date_plan=dates, source_revision_catalog_hash=old.source_revision_catalog.catalog_hash,
        selection_semantics_version=program.selection_semantics_version, selection_semantics_hash=program.selection_semantics_hash,
        list_semantics_version=program.list_semantics_version, list_semantics_hash=program.list_semantics_hash)
    payload = type(old)(resolved_request=resolved, source_revision_catalog=old.source_revision_catalog)
    calls = []

    @contextmanager
    def batch_dates(**kwargs):
        calls.append(kwargs)
        yield

    producer._batch_inference_provider = SimpleNamespace(batch_dates=batch_dates)
    # The synthetic catalog fixture owns all refs; live catalog filtering has
    # its own selector tests. No test database or model execution occurs here.
    return producer, payload, program, baseline, calls


def test_single_and_batch_task_use_real_kernel_same_scores_and_detect_resume_drift(tmp_path, monkeypatch, authority):
    from backend.tests.advisory_historical_range.test_r2b_candidate_producer import TRADE_DATE
    producer, payload, program, baseline, calls = _planned_producer(tmp_path, monkeypatch)
    arguments = dict(request_payload=payload, research_program_id=program.research_program_id)
    first = producer.produce(**arguments, decision_trade_date=TRADE_DATE)
    batched = producer.produce_dates(**arguments, decision_trade_dates=(TRADE_DATE,), current_trade_date=TRADE_DATE)[TRADE_DATE]
    assert first.candidate_artifact_ref == batched.candidate_artifact_ref
    assert [(x.symbol, x.alpha_raw_score, x.selection_effective_rank, x.membership_status) for x in first.candidates] == [
        (x.symbol, x.alpha_raw_score, x.selection_effective_rank, x.membership_status) for x in baseline.candidates]
    assert all(c["source_plan"] == program.selection_input_source_plan and
               c["run_id"] == program.selection_input_source_plan["run_id"] for c in calls)
    document = producer._artifact_store.load(first.candidate_artifact_ref).payload
    assert document["raw_signal_semantic_header"]["provider_clock_evidence"]["contract_version"] == "selection_provider_clocks_v1"
    receipt = document["raw_inference_receipt"]["selection_input_evidence"]
    assert receipt["component_receipts"]["alpha"]["identity_kind"] == "ACTUAL_CONSUMED_INPUT_DIGESTS"
    assert producer.revalidate_candidate(**arguments, decision_trade_date=TRADE_DATE, request_artifact_ref=None,
        candidate_ref=first.candidate_artifact_ref) == first.candidate_artifact_ref
    # An old-source artifact is never a valid checkpoint for this new task.
    old_bytes = producer._artifact_store.load(baseline.candidate_artifact_ref).model_dump(mode="json")
    with pytest.raises(ValueError, match="SOURCE_INPUT_DRIFT"):
        producer.revalidate_candidate(**arguments, decision_trade_date=TRADE_DATE, request_artifact_ref=None,
            candidate_ref=baseline.candidate_artifact_ref)
    assert producer._artifact_store.load(baseline.candidate_artifact_ref).model_dump(mode="json") == old_bytes
    producer._signal_preparation.result.raw.artifact.metadata["provider_metadata"]["selection_native_input_context"]["source_plan_receipt"]["actual_inputs"][0]["content_hash"] = "0" * 64
    with pytest.raises(ValueError, match="ACTUAL_RECEIPT_MISMATCH"):
        producer.produce(**arguments, decision_trade_date=TRADE_DATE)
    producer._signal_preparation.result.raw.artifact.metadata["artifact_input_context"]["window_start_date"] = "2000-01-01"
    with pytest.raises(ValueError, match="NATIVE_CONTEXT_MISMATCH"):
        producer.produce(**arguments, decision_trade_date=TRADE_DATE)


def test_source_errors_keep_owner_reason_and_kernel_cause_in_executor():
    from backend.services.advisory_historical_range.executor import _classify_failure
    from backend.services.advisory_historical_range.input_source_plan import SourcePlanError
    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalBatchError, SelectionSignalDayV1

    error = SourcePlanError("SOURCE_INPUT_DRIFT", owner="Data")
    wrapped = SelectionSignalBatchError(cause=error, stage="native_selection_computation",
        day=SelectionSignalDayV1(date(2026, 6, 1), date(2026, 6, 1), date(2026, 6, 1)), completed=())
    assert _classify_failure(wrapped)[1] == ("SOURCE_INPUT_DRIFT",)


@pytest.mark.parametrize("source,resolution,valid", [
    ("DATASET_FILES", "frozen_file_calendar", True),
    ("DATASET_FILES", "trading_calendar", False),
    ("DATABASE", "trading_calendar", True),
    ("DATABASE", "frozen_file_calendar", False),
])
def test_native_calendar_lineage_is_checked_against_actual_source(tmp_path, monkeypatch, authority, source, resolution, valid):
    from backend.services.advisory_historical_range import input_source_plan as sources
    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
    from backend.services.advisory_historical_range.task_input_sources import validate_task_raw_inputs
    from backend.tests.advisory_historical_range.test_r2b_candidate_producer import TRADE_DATE

    producer, payload, program, _, _ = _planned_producer(tmp_path, monkeypatch)
    prepared = producer._signal_preparation.result
    plan = program.selection_input_source_plan
    route = {**sources.resolve_day_source(plan, SelectionSignalDayV1(TRADE_DATE, TRADE_DATE, TRADE_DATE)),
        "source": source, "file_binding_hash": "f" * 64 if source == "DATASET_FILES" else None}
    # Unit-isolate the lineage adapter; complete file routing has separate native tests.
    monkeypatch.setattr(sources, "resolve_day_source", lambda *_: route)
    metadata = prepared.raw.artifact.metadata
    context = metadata["provider_metadata"]["selection_native_input_context"]
    context.update(window_resolution=resolution, frozen_file_binding_hash=route["file_binding_hash"])
    metadata["artifact_input_context"]["window_resolution"] = resolution
    context["source_plan_receipt"] = sources.input_receipt(route,
        {"input_context": context, "source_read_receipts": list(prepared.raw.source_read_receipts)})
    args = dict(plan=plan, prepared=prepared, program=program, date_plan=payload.resolved_request.date_plan,
        decision_trade_date=TRADE_DATE)
    if valid:
        assert validate_task_raw_inputs(**args)["component_receipts"][program.admitted_package_projection.components[0].component_id]
    else:
        with pytest.raises(ValueError, match="ACTUAL_RECEIPT_MISMATCH"):
            validate_task_raw_inputs(**args)


def test_executor_rechecks_uncommitted_source_before_marks_or_economic_commit(tmp_path, monkeypatch, authority):
    from types import SimpleNamespace
    from backend.services.advisory_historical_range import executor
    from backend.services.advisory_historical_range.input_source_plan import SourcePlanError
    from backend.services.advisory_historical_range.semantics import canonical_list_semantics_v2
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources
    from backend.tests.advisory_historical_range.test_r3_executor import _Repository, _sealed_execution_context

    request, program, days = _task("DATABASE_ONLY")
    program, = freeze_program_input_sources(request=request, date_plan=days, frozen_programs=(program,))
    store, batch, run, claim, payload, _, candidate_ref, _, _ = _sealed_execution_context(tmp_path)
    repository = _Repository(batch=batch, run=run, claim=claim)
    repository.load_reusable_candidate_ref = lambda **_: candidate_ref
    # This focused executor branch test stops before payload validation; the
    # real producer/kernel and complete legacy execution are covered separately.
    monkeypatch.setattr(executor, "_program_for_claim", lambda **_: program)
    calls = []

    def revalidate(**kwargs):
        calls.append(kwargs)
        raise SourcePlanError("SOURCE_INPUT_DRIFT", owner="Data")

    consumer = executor.HistoricalRangeDayExecutor(repository=repository, artifact_store=store,
        candidate_producer=SimpleNamespace(revalidate_candidate=revalidate), decision_mark_provider=SimpleNamespace())
    result = consumer._execute_claimed_day(claim=claim, request_payload=payload,
        semantics=canonical_list_semantics_v2(), lease_seconds=60, candidate_prefetch_per_program=1)
    assert result.status.value == "FAILED" and calls[0]["candidate_ref"] == candidate_ref
    assert repository.commit_calls == [] and len(repository.failure_calls) == 1


def test_create_application_passes_mode_and_reports_source_owner_without_dispatch():
    from types import SimpleNamespace
    from backend.services.advisory_historical_range.input_source_plan import SourcePlanError
    from backend.services.advisory_historical_range.service import HistoricalRangeApplicationService, HistoricalRangeServiceError

    seen = []

    def create(request):
        seen.append(request)
        raise SourcePlanError("SOURCE_FILE_COVERAGE_EXHAUSTED", owner="Data")

    service = object.__new__(HistoricalRangeApplicationService)
    service._mutation_runtime_factory = lambda: SimpleNamespace(planning=SimpleNamespace(create=create))
    request = HistoricalRangeCreateRequest(program_specs=[research_spec().model_dump(mode="json")],
        start_trade_date="2026-06-01", end_trade_date="2026-06-03", selection_input_source_mode="FILE_ONLY")
    with pytest.raises(HistoricalRangeServiceError) as error:
        service.create_batch(request, idempotency_key="own-api", background_tasks=None)
    assert seen[0].selection_input_source_mode == "FILE_ONLY"
    assert error.value.reason_code == "SOURCE_FILE_COVERAGE_EXHAUSTED"
    assert error.value.context["owner"] == "Data"


def test_durable_creation_retry_reads_saved_plan_and_operations_without_sql_writes(tmp_path, monkeypatch, authority):
    from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
    from backend.services.advisory_historical_range.models import HistoricalRangeContractError
    from backend.services.advisory_historical_range.repository import PostgresHistoricalRangeRepository
    from backend.services.advisory_historical_range.requirement_planner import HistoricalRangeSourceRequirementPlanner
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources
    from backend.tests.advisory_historical_range.conftest import digest
    from backend.tests.advisory_historical_range.test_repository import (
        _CreateDatabase, _CreateConnection, _CreateCursor, _planning_bindings,
    )

    request, program, dates = _task("DATABASE_ONLY")
    programs = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=(program,))
    plan = HistoricalRangeSourceRequirementPlanner().build(request=request, date_plan=dates,
        frozen_programs=programs, calendar_identity_hash=digest("calendar"), code_release_hash=program.code_release_hash)
    database, store = _CreateDatabase(), HistoricalRangeArtifactStore(root=tmp_path / "cas")
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)
    first = repository.create_planning_batch(plan=plan, artifacts=_planning_bindings(plan, store))
    calls = []
    original = _CreateCursor.execute

    def readonly(self, query, params=None):
        assert query.lstrip().startswith("SELECT"), "retry performed a write or lock"
        calls.append("SELECT")
        return original(self, query, params)

    monkeypatch.setattr(_CreateConnection, "set_session", lambda _, **kw: calls.append(kw), raising=False)
    monkeypatch.setattr(_CreateConnection, "rollback", lambda _: calls.append("rollback"), raising=False)
    monkeypatch.setattr(_CreateCursor, "execute", readonly)
    assert repository.find_existing_source_planning_batch(request=request).catalog_operation_id == first.catalog_operation_id
    assert calls[0]["readonly"] is True and calls[-1] == "rollback"
    assert authority == [1]  # Retry did not resolve the now-changeable authority.
    different = type(request).model_validate({**request.model_dump(mode="json"),
        "selection_input_source_mode": "FILE_ONLY", "user_request_semantic_hash": None})
    with pytest.raises(HistoricalRangeContractError, match="different source-mode"):
        repository.find_existing_source_planning_batch(request=different)
    database.batches[first.batch_id]["requirement_plan_artifact_hash"] = "0" * 64
    with pytest.raises(HistoricalRangeContractError, match="different planning semantics"):
        repository.find_existing_source_planning_batch(request=request)


def test_nested_task_scopes_keep_prefetch_and_native_checkpoint_retry_is_not_overwritten(tmp_path, monkeypatch, authority):
    import hashlib
    import json
    import shlex
    from types import SimpleNamespace
    from backend.services.advisory_historical_range.batch_inference_provider import AdvisoryBatchInferenceProvider
    from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
    from backend.services.advisory_historical_range.task_input_sources import freeze_program_input_sources
    from backend.tests.advisory_historical_range.test_batch_inference_provider import _fixture
    from scripts.strategy_package_live_inference import _atomic_json

    provider, workspace, _, _ = _fixture(tmp_path, monkeypatch)
    request, program, dates = _task("DATABASE_ONLY")
    program, = freeze_program_input_sources(request=request, date_plan=dates, frozen_programs=(program,))
    plan = program.selection_input_source_plan
    kwargs = dict(run_id=plan["run_id"], source_revision=plan["plan_hash"], source_plan=plan)
    with provider.batch_dates(trade_dates=dates.ordered_trade_dates, **kwargs):
        original = provider._scope.get()
        with provider.batch_dates(trade_dates=dates.ordered_trade_dates[:1], **kwargs):
            assert provider._scope.get() is original
        with pytest.raises(ValueError, match="IDENTITY_MISMATCH"):
            with provider.batch_dates(trade_dates=dates.ordered_trade_dates[1:], **{
                    **kwargs, "run_id": "wrong-run"}):
                pytest.fail("invalid source scope entered")
        clocks = (SelectionSignalDayV1(*(dates.ordered_trade_dates[0],) * 3),)
        commands = []

        def run(command, **_):
            args = shlex.split(command[-1])
            output = args[args.index("--output-path") + 1]
            path = type(tmp_path)("X:/" + output.removeprefix("/mnt/x/"))
            commands.append(args)
            _atomic_json(path, {"status": "COMPLETE", "ok": True, "results": [{}], "binding": "native-test-binding",
                "consumer_scope": "ADVISORY", "run_id": plan["run_id"]})
            return SimpleNamespace(returncode=0)

        monkeypatch.setattr(provider, "_build_env_exports", lambda **_: "")
        monkeypatch.setattr("backend.services.advisory_historical_range.batch_inference_provider.subprocess.run", run)
        root, _ = AdvisoryBatchInferenceProvider._execute(provider, workspace.workspace_path, clocks, original)
        pins = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in ("plan.json", "source-plan.json")}
        # Crash after an own partial checkpoint, before final index publication.
        (root / "batch.json").unlink()
        (root / "batch_dates").mkdir()
        again, _ = AdvisoryBatchInferenceProvider._execute(provider, workspace.workspace_path, clocks, original)
        assert root == again and "--resume" not in commands[0] and "--resume" in commands[1]
        assert pins == {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in pins}
        (root / "source-plan.json").write_text(json.dumps({"tampered": True}), encoding="utf-8")
        with pytest.raises(ValueError, match="not overwritten"):
            AdvisoryBatchInferenceProvider._execute(provider, workspace.workspace_path, clocks, original)


def test_file_day_uses_native_file_filter_without_hidden_sql_or_fallback(tmp_path, monkeypatch):
    import psycopg2
    from backend.data_service.frozen_dataset import FrozenDatasetError
    from backend.services.advisory_historical_range import input_source_plan as sources
    from backend.services.advisory_historical_range.task_input_sources import selection_day_providers
    from backend.services.selection_center.models import SelectionCandidate
    from backend.tests.advisory_historical_range.test_input_source_plan import _snapshot, _plan
    from backend.tests.data_service.test_frozen_dataset import DAYS, SYMBOL, HALT

    plan = _plan("FILE_ONLY", _snapshot(tmp_path), days=_plan()["days"][:2])
    # The task's C=D=T mapping, not the shared test fixture's earlier C.
    rows = [{**row, "data_cutoff": row["decision_date"]} for row in plan["days"]]
    plan = sources.build_source_plan(mode="FILE_ONLY", consumer_scope="ADVISORY", run_id="test-run",
        days=rows, trading_calendar=plan["trading_calendar"], native_policy=plan["native_policy"], file_source=plan["file_source"])
    monkeypatch.setattr(psycopg2, "connect", lambda *_, **__: pytest.fail("file selection queried SQL"))
    with selection_day_providers(plan=plan, day=DAYS[1], database_providers=None) as providers:
        result = providers.tradability.filter_candidates_with_receipt(
            candidates=[SelectionCandidate(symbol=HALT, score=2, rank=1), SelectionCandidate(symbol=SYMBOL, score=1, rank=2)],
            trade_date=DAYS[1], top_k=1, package_id="fixture-not-whitelisted", manifest_sha256="a" * 64,
            enabled=True, industry_blacklist=[], exhaustive_evidence=True)
        assert [x.symbol for x in result.candidates] == [SYMBOL]
        assert any(x.symbol == HALT for x in result.exclusions)
    missing = tmp_path / plan["file_source"]["suspend_meta_pin"]["path"]
    missing.write_text("corrupt", encoding="utf-8")
    with pytest.raises(FrozenDatasetError):
        # No exception-driven switch to the deliberately unusable DB provider.
        with selection_day_providers(plan=plan, day=DAYS[1], database_providers=None):
            pytest.fail("corrupt file release entered native selection")
