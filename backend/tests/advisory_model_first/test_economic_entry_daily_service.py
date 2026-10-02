from datetime import date, datetime, timezone
from dataclasses import replace
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_daily_service import AdvisoryEconomicEntryDailyServiceV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_entry_daily_source import _loaded
from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryCandidateProjectionV1
from backend.services.advisory_model_first.economic_entry_daily_contracts import build_economic_entry_value_role_v1
from backend.services.advisory_model_first.economic_entry_daily_service import EconomicEntryValueReadonlyRoleStoreV1
from backend.services.advisory_model_first.economic_entry_serving_bundle import (
    EconomicEntryConfirmedServingManifestV1, LoadedEconomicEntryConfirmedBundleV1, build_economic_entry_confirmation_request_v1,
)
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, _json_bytes
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.services.advisory_model_first.economic_entry_daily_source import (
    project_economic_frozen_candidate_roster_v1, prepare_native_economic_research_day_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_economic_entry_daily_bundle import _native_training_scope, _confirmation_request

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def _formal_consumer(tmp_path, study, *, empty=False):
    # Synthetic consumer-only fixture: no real native/confirmation/activation claim.
    parameters, native, _ = _native_training_scope(tmp_path, study)
    args = _loaded(study)[1]
    values = _confirmation_request(tmp_path, study).model_dump(exclude={"experiment_id"})
    confirmation = build_economic_entry_confirmation_request_v1(**{**values, "scope": native.projection.scope})
    reference = evidence_reference_for_file(parameters["original"] / "prepared/native_applicability.json", role="aligned_plan")
    manifest = EconomicEntryConfirmedServingManifestV1(aligned_plan_ref=reference, aligned_trained_manifest_ref=reference.model_copy(update={"role": "aligned_trained"}),
        native_training_scope_ref=reference.model_copy(update={"role": "ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"}),
        confirmation_request_ref=reference.model_copy(update={"role": "ECONOMIC_ENTRY_VALUE_CONFIRMATION_REQUEST"}),
        confirmation_manifest_ref=reference.model_copy(update={"role": "ECONOMIC_ENTRY_VALUE_CONFIRMATION_MANIFEST"}), scope=native.projection.scope)
    identity = parameters["identity"]
    fitted_request = args["fitted"].request.model_copy(update={"source_request": args["fitted"].request.source_request.model_copy(
        update={"input_identity_sha256": identity.identity_sha256})})
    fitted = replace(args["fitted"], request=fitted_request)
    loaded = LoadedEconomicEntryConfirmedBundleV1(manifest, fitted, SimpleNamespace(training_request=fitted_request), native, confirmation,
        {"status": "ECONOMIC_GATES_PASSED"}, parameters["original"], {"identity.json": identity.model_dump_json().encode()})
    consumer = tmp_path / "consumer"
    manifest_path = consumer / "serving_bundles" / manifest.bundle_id / "preregistered/serving_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")
    D, T = date(2025, 3, 31), date(2025, 4, 1)
    clock = datetime(2025, 3, 31, 9, tzinfo=timezone.utc)
    role = build_economic_entry_value_role_v1(program_id="program", binding_version_id="binding", bundle_id=manifest.bundle_id,
        qualified_manifest=evidence_reference_for_file(manifest_path, role="ECONOMIC_ENTRY_VALUE_SERVING_MANIFEST"), scope=manifest.scope,
        confirmation_request_sha256=confirmation.request_sha256, effective_from_target_date=T, created_at=clock)
    directory = EconomicEntryValueReadonlyRoleStoreV1.directory(consumer_root=consumer, program_id=role.program_id, binding_version_id=role.binding_version_id)
    publish_stage(study_root=directory / "versions" / role.role_sha256, stage="preregistered", plan_sha256=role.role_sha256,
        parent_sha256=None, artifacts={"role.json": _json_bytes(role.model_dump(mode="json"))})
    functional = {"schema_version": "economic_entry_value_role_pointer_v1", "role_sha256": role.role_sha256, "enabled": True,
        "activated_at": clock.isoformat(), "authorization_ref": "UNIT_ONLY_NO_ACTIVATION", "activation_registry_entry_id": "advtrial_" + "a" * 24}
    pointer = {**functional, "pointer_sha256": sha(functional)}
    (directory / "active.json").write_text(json.dumps(pointer), encoding="utf-8")
    row = SimpleNamespace(symbol="000001.SZ", rank=1, score=.32, component_scores={
        "unit-leg-a": {"normalized_score": .2, "weight": .4}, "unit-leg-b": {"normalized_score": .4, "weight": .6}})
    candidates = project_economic_frozen_candidate_roster_v1(rows=[] if empty else [row], decision_date=D, target_date=T,
        component_roles=native.projection.component_roles.model_dump(), terminal_weights=native.projection.terminal_weights, candidate_group_size=0 if empty else 1)
    feature_values = {**args["decision_features"], "parent_combined_score": .32, "parent_rank_pct": 1., "leg_norm_score_gap": -.2}
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    features = pd.DataFrame([] if empty else [{**candidates.iloc[0][keys].to_dict(), **feature_values}], columns=[*keys, *feature_values])
    members = ["000001.SZ", "000002.SZ"]
    source_receipt = {"outcomes_read": False, "pit_universe_key": native.pit_universe_key, "shared_builder_sha256": native.shared_builder_sha256,
        "bar_policy_sha256": native.bar_policy_sha256, "source_window_contract": {"candidate_sessions": 20, "benchmark_sessions": 20, "breadth_sessions": 2}}
    if empty:
        source_receipt.update(status="NO_CANDIDATES", query_count=0)
    observed = {"decision_date": D, "target_date": T, "program_id": "program", "binding_version_id": "binding", "selection_run_id": "unit-run",
        "list_version_id": "unit-list", "source_members": members, "captured_at": clock, "candidates": candidates, "features": features,
        "price_contexts": {} if empty else {row.symbol: replace(args["context"], decision_price_trade_date=D)}, "price_context_unavailable": (),
        "trading_calendar": (D, T), "source_receipt": source_receipt,
        "candidate_receipt": {"projection_sha256": native.projection.projection_sha256, "model_scope_qualified": False,
            "native_receipt_created": False, "outcomes_read": False, "admitted_universe_members_sha256": sha(members),
            "pit_universe_key": native.pit_universe_key, "pit_rule_version": native.pit_rule_version,
            "source_evidence_limitations": ("UNIT_ONLY_NOT_REAL_NATIVE_SOURCE",)}}
    program = SimpleNamespace(program_id="program", package_ids=[manifest.scope.package_id], review_policy_sha256=native.projection.review_policy_sha256,
        target_count=20, status="ENABLED", review_schedule={"frequency": "daily_after_close"})
    binding = SimpleNamespace(binding_version_id="binding", program_id="program", activation_status="ACTIVE", package_ids=program.package_ids,
        runtime_config_json={"universe_selection": native.projection.universe_selection.model_dump(mode="json")})
    programs = SimpleNamespace(get_program=lambda name: program, get_active_binding_version=lambda name: binding)
    class Source:
        calls = 0
        def load_day(self, **kwargs):
            self.calls += 1
            return observed
    source = Source()
    class UnitRoleStore(EconomicEntryValueReadonlyRoleStoreV1):
        def verify_activation(self, **kwargs):
            # Real registry consumption is tested separately; no real activation.
            return None
    service = AdvisoryEconomicEntryDailyServiceV1(consumer_root=consumer, aligned_output_root=tmp_path,
        confirmed_bundle_loader=lambda **kwargs: loaded, role_store=UnitRoleStore(), program_provider=lambda session: programs,
        candidate_source_factory=lambda *args: source, now_provider=lambda: clock)
    return service, loaded, role, pointer, source, observed, programs


@pytest.mark.parametrize("empty", [False, True])
def test_formal_native_capture_read_and_exact_retry_use_independent_risk_contract(tmp_path, study, empty, monkeypatch):
    from backend.services.advisory_model_first.entry_price_daily_service import EntryWorkBudget, BoundedEntryReadSession
    service, loaded, role, pointer, source, observed, programs = _formal_consumer(tmp_path, study, empty=empty)
    T = observed["target_date"]
    assert service.status(program_id="program", target_date=T)["status"] == "NOT_CAPTURED"
    budget = EntryWorkBudget()
    session = BoundedEntryReadSession(budget)
    parameters = dict(role=role, pointer=pointer, loaded=loaded, target=T, budget=budget, session=session)
    from backend.services.advisory_model_first.entry_price_daily_service import EntryReadOnlyCalendar
    monkeypatch.setattr(EntryReadOnlyCalendar, "next_trading_day", lambda *args, **kwargs: T)
    assert source.calls == 0  # GET never captures, including a configured role.
    cycle = service.run_once()
    assert cycle["new_artifacts"] == 1, cycle
    first = cycle["results"][0]
    assert first["status"] == ("NO_CANDIDATES" if empty else "PUBLISHED") and source.calls == 1
    status = service.status(program_id="program", target_date=T)
    assert status["status"] == first["status"] and status["deployable"] is True
    assert all(row["risk_budget"] == loaded.confirmation_request.business_risk.model_dump(mode="json") for row in status["advice"])
    assert all(row["evidence_state"] == "CONFIRMED_ENTRY_VALUE" and row["decision_use"] == "ADVISORY_ONLY" for row in status["advice"])
    capsule = json.loads((service._formal_path(role, T) / "prepared/native_d_capsule.json").read_text(encoding="utf-8"))
    assert capsule["captured_at"] == observed["captured_at"].isoformat() and capsule["source_members"] == observed["source_members"]
    assert [record["decision_features"] for record in capsule["inputs"]] == ([] if empty else [feature_values for feature_values in
        observed["features"][list(loaded.manifest.scope.feature_names[:-1])].to_dict("records")])
    assert service._capture_formal(**parameters)["status"] == "EXACT_RETRY" and source.calls == 1
    assert service.run_once()["new_artifacts"] == 0 and source.calls == 1
    assert service.status(program_id="program")["resolved_target_date"] == T.isoformat()
    service._now = lambda: datetime(2025, 4, 1, 2, tzinfo=timezone.utc)
    assert service.status(program_id="program", target_date=T)["status"] == "STALE"
    assert service.status(program_id="program", target_date=T)["deployable"] is False
    programs.get_program("program").status = "PAUSED"
    assert service.status(program_id="program", target_date=T)["status"] == "DISABLED"
    programs.get_program("program").status = "ENABLED"
    programs.get_program("program").review_policy_sha256 = "f" * 64
    with pytest.raises(AdvisoryModelFirstError, match="current economic role"):
        service.status(program_id="program", target_date=T)


def test_actual_readonly_role_store_requires_separate_ACTIVATION_registry_evidence(tmp_path, study):
    from pathlib import Path
    from backend.services.advisory_model_first.research_control_contracts import build_trial_record
    service, loaded, role, pointer, _, _, _ = _formal_consumer(tmp_path, study)
    store = EconomicEntryValueReadonlyRoleStoreV1()
    actual, persisted_pointer, role_path = store.read(consumer_root=service._root, program_id=role.program_id, binding_version_id=role.binding_version_id)
    assert actual == role and persisted_pointer == pointer
    fields = dict(experiment_id="UNIT_ONLY_ACTIVATION_NOT_REAL", attempt_id="unit-attempt", research_stage="ACTIVATED", study_type="ACTIVATION",
        hypothesis_family_id="unit-role", parent_lineage=(loaded.confirmation_request.experiment_id,), unique_variable="activate_role_not_model_trial",
        objective_contract=role.objective_contract, dataset_identity=loaded.confirmation_request.dataset_identity,
        schema_identity=role.scope.feature_schema_sha256, policy_identity=loaded.confirmation_request.policy_identity,
        planned_trial_count=0, generated_trial_count=0, evaluated_trial_count=0, selected_trial_count=0, consumed_windows=(),
        result_class="ACTIVATED", decision_use="ACTIVATION_EVIDENCE", recorded_at=role.created_at,
        evidence_refs=(evidence_reference_for_file(role_path, role="ECONOMIC_ENTRY_VALUE_ROLE"), role.qualified_manifest))
    registry = Path(loaded.manifest.confirmation_request_ref.artifact_uri).parent.parent / "trial_registry.jsonl"
    def verify(values):
        record = build_trial_record(**values)
        registry.write_text(record.model_dump_json() + "\n", encoding="utf-8")
        changed = {**pointer, "activation_registry_entry_id": record.registry_entry_id}
        return store.verify_activation(loaded=loaded, role=role, pointer=changed, role_path=role_path)
    assert verify(fields).study_type == "ACTIVATION"
    for values in ({**fields, "study_type": "CONFIRMATION", "result_class": "CONFIRMED", "decision_use": "DIRECTION_GATE"},
            {**fields, "parent_lineage": ("unrelated-confirmation",)}, {**fields, "policy_identity": "different-policy"},
            {**fields, "planned_trial_count": 1, "generated_trial_count": 1, "evaluated_trial_count": 1, "selected_trial_count": 1}):
        with pytest.raises(AdvisoryModelFirstError, match="activation record differs"):
            verify(values)
    # A read cannot repair or accept a corrupted pointer checksum.
    directory = store.directory(consumer_root=service._root, program_id=role.program_id, binding_version_id=role.binding_version_id)
    (directory / "active.json").write_text(json.dumps({**pointer, "role_sha256": "0" * 64}), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="pointer content"):
        store.read(consumer_root=service._root, program_id=role.program_id, binding_version_id=role.binding_version_id)


@pytest.mark.parametrize("poison", [{"captured_at": "2025-04-01T02:00:00+00:00"},
    {"captured_at": "2025-03-31T09:00:00"}, {"run_id": ""}, {"decision_date": "2025-04-01"}])
def test_empty_formal_batch_still_requires_native_identity_and_prospective_clock(tmp_path, study, monkeypatch, poison):
    from backend.services.advisory_model_first.entry_price_daily_service import EntryReadOnlyCalendar
    import backend.services.advisory_model_first.economic_entry_daily_service as module
    service, _, _, _, _, observed, _ = _formal_consumer(tmp_path, study, empty=True)
    T = observed["target_date"]
    monkeypatch.setattr(EntryReadOnlyCalendar, "next_trading_day", lambda *args, **kwargs: T)
    assert service.run_once()["new_artifacts"] == 1
    assert service.status(program_id="program", target_date=T)["status"] == "NO_CANDIDATES"
    # Inject matching decoded payload/capsule contents after the existing file
    # hash gate: integrity alone cannot prove an empty batch's native clock.
    batch_reader = module._readonly_daily_batch
    capsule_reader = module._read_economic_metadata_reference
    monkeypatch.setattr(module, "_readonly_daily_batch", lambda *args, **kwargs: {**batch_reader(*args, **kwargs), **poison})
    monkeypatch.setattr(module, "_read_economic_metadata_reference", lambda *args, **kwargs: {**capsule_reader(*args, **kwargs), **poison})
    with pytest.raises(AdvisoryModelFirstError, match="native batch identity or prospective clock"):
        service.status(program_id="program", target_date=T)


@pytest.mark.parametrize("broken_stage", ["role", "existing_daily_artifact"])
def test_formal_source_failure_cannot_publish_or_starve_a_different_program(tmp_path, study, monkeypatch, broken_stage):
    from backend.services.advisory_model_first.entry_price_daily_service import EntryReadOnlyCalendar
    service, loaded, role, pointer, source, observed, programs = _formal_consumer(tmp_path, study)
    monkeypatch.setattr(EntryReadOnlyCalendar, "next_trading_day", lambda *args, **kwargs: observed["target_date"])
    original_rule = observed["candidate_receipt"]["pit_rule_version"]
    observed["candidate_receipt"]["pit_rule_version"] = "foreign-rule"
    result = service.run_once()
    assert result["new_artifacts"] == 0 and result["results"][0]["status"] == "INPUT_UNAVAILABLE"
    assert not service._formal_path(role, observed["target_date"]).exists()
    observed["candidate_receipt"]["pit_rule_version"] = original_rule
    # Malformed configuration is isolated; the other eligible role can progress.
    other = service._root / "entry_value_roles" / "aaa-broken-program"
    other.mkdir()
    resolved = service._resolved_role
    def resolver(**kwargs):
        if kwargs["program_id"] == other.name:
            if broken_stage == "role":
                raise AdvisoryModelFirstError("unit malformed role", reason_code="UNIT_BROKEN_ROLE")
            return role.model_copy(update={"program_id": other.name}), pointer, loaded, programs.get_program("program")
        return resolved(**kwargs)
    monkeypatch.setattr(service, "_resolved_role", resolver)
    read_formal = service._read_formal
    def read_daily(**kwargs):
        if kwargs["role"].program_id == other.name:
            raise AdvisoryModelFirstError("unit corrupted daily artifact", reason_code="UNIT_BROKEN_ROLE")
        return read_formal(**kwargs)
    monkeypatch.setattr(service, "_read_formal", read_daily)
    result = service.run_once()
    assert result["new_artifacts"] == 1 and result["results"][-1]["status"] == "PUBLISHED"
    assert result["results"][0]["reason_code"] == "UNIT_BROKEN_ROLE"
    assert source.calls == 2


@pytest.mark.parametrize("failure_stage", ["existing_daily_artifact", "capture"])
def test_program_failure_isolation_cannot_swallow_global_budget(tmp_path, study, monkeypatch, failure_stage):
    from backend.services.advisory_model_first.entry_price_daily_service import EntryReadOnlyCalendar, EntryWorkBudget
    import backend.services.advisory_model_first.economic_entry_daily_service as module
    service, _, _, _, _, observed, _ = _formal_consumer(tmp_path, study)
    clock = [0.]
    monkeypatch.setattr(module, "EntryWorkBudget", lambda: EntryWorkBudget(monotonic=lambda: clock[0]))
    monkeypatch.setattr(EntryReadOnlyCalendar, "next_trading_day", lambda *args, **kwargs: observed["target_date"])
    def fail_after_deadline(**kwargs):
        clock[0] = 31.
        raise AdvisoryModelFirstError("unit failure after deadline", reason_code="UNIT_ONLY")
    monkeypatch.setattr(service, "_read_formal", fail_after_deadline if failure_stage == "existing_daily_artifact" else lambda **kwargs: None)
    monkeypatch.setattr(service, "_capture_formal", fail_after_deadline)
    with pytest.raises(AdvisoryModelFirstError, match="bounded budget"):
        service.run_once()
    assert not (service._root / "entry_value_daily").exists()


def test_research_capture_readback_and_retry_never_refit_recapture_or_promote(tmp_path, study):
    loaded, args = _loaded(study)
    loaded.fitted = args["fitted"]
    loaded.manifest.bundle_id = "adveserve_" + "a" * 24
    loaded.manifest.manifest_sha256 = "c" * 64
    class UnavailablePriceSource:
        calls = 0
        def load(self, *, symbols, **kwargs):
            self.calls += 1
            return {}, tuple({"symbol": symbol, "reason_code": "PIT_ATTRIBUTE_UNAVAILABLE"} for symbol in symbols)
    source = UnavailablePriceSource()
    service = AdvisoryEconomicEntryDailyServiceV1(consumer_root=tmp_path / "consumer", aligned_output_root=tmp_path / "aligned",
        bundle_loader=lambda **kwargs: loaded)
    target = args["prediction_input"].target_date
    assert service.read_research(program_id="program", bundle_id=loaded.manifest.bundle_id, target_date=target)["status"] == "NOT_CAPTURED"
    first = service.capture_frozen_research_day(bundle_id=loaded.manifest.bundle_id,
        decision_date=args["prediction_input"].decision_date, context_source=source)
    projected = service.read_research(program_id="program", bundle_id=loaded.manifest.bundle_id, target_date=target)
    assert len(projected["advice"]) == 6 and all(value["recommendation_status"] == "UNAVAILABLE" for value in projected["advice"])
    assert projected["deployable"] is False and projected["original_batch_sha256"] == first["batch_sha256"]
    assert all("query_nodes" not in value for value in projected["advice"])
    retry = service.capture_frozen_research_day(bundle_id=loaded.manifest.bundle_id,
        decision_date=args["prediction_input"].decision_date, context_source=source)
    assert retry["status"] == "EXACT_RETRY" and retry["batch_sha256"] == first["batch_sha256"] and source.calls == 1
    with pytest.raises(AdvisoryModelFirstError, match="registered next trading day|new or sealed"):
        service.read_research(program_id="program", bundle_id=loaded.manifest.bundle_id, target_date="2026-08-31")
    with pytest.raises(AdvisoryModelFirstError, match="different original Program"):
        service.read_research(program_id="another-program", bundle_id=loaded.manifest.bundle_id, target_date=target)


def test_unbound_production_status_and_hook_have_zero_data_or_artifact_side_effects(tmp_path):
    service = AdvisoryEconomicEntryDailyServiceV1(consumer_root=tmp_path / "consumer", aligned_output_root=tmp_path / "aligned",
        bundle_loader=lambda **kwargs: pytest.fail("unbound role must not load a model"))
    assert service.status(program_id="program")["status"] == "NOT_CONFIGURED"
    assert service.run_once()["new_artifacts"] == 0
    assert not (tmp_path / "consumer").exists()


def test_batch_rejects_sealed_or_duplicate_dates_before_source_factory(tmp_path, study):
    loaded, args = _loaded(study)
    service = AdvisoryEconomicEntryDailyServiceV1(consumer_root=tmp_path, aligned_output_root=tmp_path,
        bundle_loader=lambda **kwargs: loaded)
    for dates in (["2026-08-31"], [args["prediction_input"].decision_date] * 2):
        with pytest.raises(AdvisoryModelFirstError):
            service.capture_frozen_research_days(bundle_id="adveserve_" + "a" * 24, decision_dates=dates,
                context_source_factory=lambda budget: pytest.fail("invalid plan cannot open a source"))


@pytest.mark.parametrize("native", [False, True])
def test_daily_read_budgets_precede_hashing_and_reject_extra_files(tmp_path, native):
    from backend.services.advisory_model_first.economic_entry_daily_service import _daily_stage_preflight
    files = {"daily_batch.json": b"{}"}
    if native:
        files["native_d_capsule.json"] = b"{}"
    stage = publish_stage(study_root=tmp_path / "consumer", stage="prepared", plan_sha256="a" * 64,
        parent_sha256=None, artifacts=files)
    _daily_stage_preflight(stage, native=native)
    name = "native_d_capsule.json" if native else "daily_batch.json"
    maximum = 4194304 if native else 67108864
    with (stage / name).open("r+b") as stream:
        stream.truncate(maximum + 1)  # X-drive sparse fixture; no large allocation/read/hash.
    with pytest.raises(AdvisoryModelFirstError, match="upfront file budget"):
        _daily_stage_preflight(stage, native=native)
    with (stage / name).open("r+b") as stream:
        stream.truncate(2)
    manifest_path = stage / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"]["unregistered.json"] = {"sha256": "c" * 64, "size_bytes": 1}
    manifest_path.write_bytes(_json_bytes(manifest))
    with pytest.raises(AdvisoryModelFirstError, match="unexpected or missing"):
        _daily_stage_preflight(stage, native=native)


@pytest.mark.parametrize("fail", [False, True])
def test_batch_loads_one_model_and_closes_every_factory_reader(tmp_path, study, monkeypatch, fail):
    loaded, args = _loaded(study)
    loads, readers = [], []
    def loader(**kwargs):
        loads.append("one-frozen-model")
        return loaded
    service = AdvisoryEconomicEntryDailyServiceV1(consumer_root=tmp_path, aligned_output_root=tmp_path, bundle_loader=loader)
    class Reader:
        closed = False
        def close(self): self.closed = True
    def factory(budget):
        assert all(reader.closed for reader in readers)
        reader = Reader()
        readers.append(reader)
        return reader
    def capture(*args, **kwargs):
        if fail:
            raise AdvisoryModelFirstError("unit failed bounded capture", reason_code="UNIT_ONLY")
        return {"status": "EXACT_RETRY", "new_input_reads": 0}
    monkeypatch.setattr(service, "_capture_loaded", capture)
    D = args["prediction_input"].decision_date
    days = [D, (pd.Timestamp(D) + pd.offsets.BDay()).date()]
    if fail:
        with pytest.raises(AdvisoryModelFirstError, match="unit failed"):
            service.capture_frozen_research_days(bundle_id="adveserve_" + "a" * 24, decision_dates=days, context_source_factory=factory)
    else:
        result = service.capture_frozen_research_days(bundle_id="adveserve_" + "a" * 24, decision_dates=days, context_source_factory=factory)
        assert len(result["results"]) == 2 and result["new_model_fits"] == 0
    assert loads == ["one-frozen-model"] and readers and all(reader.closed for reader in readers)


@pytest.mark.parametrize("empty", [False, True])
def test_native_preview_retains_real_references_without_confirmation_or_source_replacement(tmp_path, study, empty):
    loaded, args = _loaded(study)
    loaded.fitted = args["fitted"]
    loaded.manifest.bundle_id, loaded.manifest.manifest_sha256 = "adveserve_" + "a" * 24, "c" * 64
    projection = EconomicEntryCandidateProjectionV1(scope=loaded.manifest.scope,
        component_roles={"lstm": "unit-a", "fund": "unit-b"}, terminal_weights={"unit-a": .4, "unit-b": .6},
        universe_selection={"mode": "stock_universe", "pool_ids": []}, review_policy_sha256="a" * 64)
    D, T = args["prediction_input"].decision_date, args["prediction_input"].target_date
    row = SimpleNamespace(symbol="000001.SZ", rank=1, score=.32, component_scores={
        "unit-a": {"normalized_score": .2, "weight": .4}, "unit-b": {"normalized_score": .4, "weight": .6}})
    candidates = project_economic_frozen_candidate_roster_v1(rows=[] if empty else [row], decision_date=D,
        target_date=T, component_roles=projection.component_roles.model_dump(), terminal_weights=projection.terminal_weights,
        candidate_group_size=0 if empty else 1)
    feature_values = {**args["decision_features"], "parent_combined_score": .32, "parent_rank_pct": 1., "leg_norm_score_gap": -.2}
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    features = pd.DataFrame([] if empty else [{**candidates.iloc[0][keys].to_dict(), **feature_values}], columns=[*keys, *feature_values])
    members = ["000001.SZ", "000002.SZ"]
    observed = {"decision_date": D, "target_date": T, "program_id": "program", "binding_version_id": "binding",
        "selection_run_id": "unit-original-run", "list_version_id": "unit-original-list", "source_members": members,
        "captured_at": datetime.now(timezone.utc), "candidates": candidates, "features": features,
        "price_contexts": {} if empty else {row.symbol: args["context"]}, "price_context_unavailable": (),
        "source_receipt": {"outcomes_read": False, "training_temporal_parity": "UNPROVEN"},
        "candidate_receipt": {"projection_sha256": projection.projection_sha256, "model_scope_qualified": False,
            "native_receipt_created": False, "outcomes_read": False, "admitted_universe_members_sha256": sha(members),
            "source_evidence_limitations": ("UNIT_ONLY_INJECTED_READER_NOT_NATIVE_DB_EVIDENCE",)}}
    for poisoned in ({**observed, "source_receipt": {"outcomes_read": True}},
                     {**observed, "captured_at": datetime(2025, 1, 28)},
                     {**observed, "source_members": [members[0]]}):
        with pytest.raises(AdvisoryModelFirstError):
            prepare_native_economic_research_day_v1(loaded=loaded, observed=poisoned, projection=projection)
    if not empty:
        with pytest.raises(AdvisoryModelFirstError, match="every original candidate key"):
            prepare_native_economic_research_day_v1(loaded=loaded,
                observed={**observed, "features": features.assign(target_trade_date=pd.Timestamp("2026-08-31"))}, projection=projection)
    calls = []
    class Source:
        def load_day(self, **kwargs):
            calls.append(kwargs)
            return observed
    service = AdvisoryEconomicEntryDailyServiceV1(consumer_root=tmp_path / "consumer", aligned_output_root=tmp_path / "aligned",
        bundle_loader=lambda **kwargs: loaded)
    arguments = dict(bundle_id=loaded.manifest.bundle_id, target_date=T, projection=projection,
                     candidate_source_factory=lambda budget: Source(), frozen_list_id="unit-original-list")
    first = service.capture_native_research_day(**arguments)
    assert first["deployable"] is False and len(calls) == 1
    read = service.read_research(program_id="program", bundle_id=loaded.manifest.bundle_id, target_date=T)
    assert read["run_id"] == observed["selection_run_id"] and read["list_id"] == observed["list_version_id"]
    assert read["status"] == ("NO_CANDIDATES" if empty else "RESEARCH_NAVIGATION")
    assert all(value["prediction_input"]["source_evidence"] == "RECOVERED_LIMITED" for value in read["advice"])
    assert service.capture_native_research_day(**arguments)["status"] == "EXACT_RETRY" and len(calls) == 1
    for target, expected_list in (("2026-08-31", "unit-original-list"), (T, "wrong-list")):
        with pytest.raises(AdvisoryModelFirstError):
            service.capture_native_research_day(**{**arguments, "target_date": target, "frozen_list_id": expected_list})
    with pytest.raises(AdvisoryModelFirstError, match="belongs to native-source"):
        service.capture_frozen_research_day(bundle_id=loaded.manifest.bundle_id, decision_date=D, context_source=object())
    assert len(calls) == 1
