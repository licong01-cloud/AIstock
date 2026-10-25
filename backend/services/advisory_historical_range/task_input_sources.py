"""Advisory task adapter to the public source plan, not a second input algorithm.

Only selection inputs are routed here. Private decision marks/outcomes keep
their own explicitly registered sources. No active pointer is read on restore.
"""
from contextlib import contextmanager
from datetime import date

from . import input_source_plan as sources
from .canonical import canonical_json_sha256
from .selection_signal_kernel import SelectionSignalDayV1


PLAN_SOURCE_ROLES = frozenset({"code_release", "package_runtime_assets", "hmm_frozen_evidence",
                               "selection_input_plan"})


def _source_run_id(request, program, date_plan):
    return "ahrinput_" + canonical_json_sha256({
        "client_idempotency_key": request.client_idempotency_key,
        "user_request_semantic_hash": request.user_request_semantic_hash,
        "research_program_id": program.research_program_id,
        "manifest": program.manifest_sha256, "runtime": program.runtime_config_hash,
        "code": program.code_release_hash, "date_plan": date_plan.date_plan_hash,
    })[:32]


def native_input_policy(program):
    from backend.services.canonical_equity_pit import CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION
    from backend.services.strategy_package.selection_computation import (
        parse_selection_runtime_profile_for_computation, selection_runtime_profile_sha256,
    )
    profile = parse_selection_runtime_profile_for_computation(program.runtime_config)
    required = ["daily", "minute", "factor", "pit"]
    if profile.tradability.exclude_suspended:
        required.append("suspend")
    if profile.industry_blacklist:
        required.append("sector")
    return {"policy_hash": selection_runtime_profile_sha256(profile),
            "universe_key": CANONICAL_PIT_UNIVERSE_KEY, "rule_version": CANONICAL_PIT_RULE_VERSION,
            "reader_contract": sources.READER_CONTRACT, "required_components": required}


def _days(date_plan, program):
    warmup = date_plan.per_program_input_warmup_ranges[program.research_program_id]
    return [{"decision_date": str(day), "data_cutoff": str(day), "target_trade_date": str(day),
             "history_start": str(min(window.window_start_trade_date for component in warmup.components
                                      for window in component.day_windows if window.decision_trade_date == day))}
            for day in date_plan.ordered_trade_dates]


def freeze_program_input_sources(*, request, date_plan, frozen_programs):
    """Freeze once before publishing the planning artifact; no model/price read."""
    mode = request.selection_input_source_mode
    if mode is None:
        return frozen_programs
    snapshot = (None if mode == "DATABASE_ONLY" else sources.freeze_file_source(
        consumer_scope="ADVISORY", run_id="creation-" + request.user_request_semantic_hash))
    programs = []
    for program in frozen_programs:
        plan = sources.build_source_plan(mode=mode, consumer_scope="ADVISORY",
            run_id=_source_run_id(request, program, date_plan), days=_days(date_plan, program),
            trading_calendar=[str(day) for day in date_plan.ordered_trade_dates],
            native_policy=native_input_policy(program), file_source=snapshot)
        payload = program.model_dump(mode="json")
        payload.update(selection_input_source_plan=plan, frozen_program_hash=None)
        programs.append(type(program).model_validate(payload))
    return tuple(programs)


def validate_program_source_plan(*, program, date_plan=None, request=None):
    plan = program.selection_input_source_plan
    if plan is None:
        return None
    plan = sources.validate_source_plan(plan, consumer_scope="ADVISORY", run_id=plan["run_id"])
    if (canonical_json_sha256(program.semantic_payload()) != program.frozen_program_hash
            or plan["native_policy"] != native_input_policy(program)):
        raise sources.SourcePlanError("SOURCE_TASK_PROGRAM_IDENTITY_MISMATCH")
    if date_plan is not None and plan["days"] != _days(date_plan, program):
        raise sources.SourcePlanError("SOURCE_TASK_DATE_PLAN_MISMATCH")
    if request is not None and (plan["run_id"] != _source_run_id(request, program, date_plan)
                                or plan["mode"] != request.selection_input_source_mode):
        raise sources.SourcePlanError("SOURCE_TASK_REQUEST_MISMATCH")
    return plan


def validate_task_raw_inputs(*, plan, prepared, decision_trade_date, program, date_plan):
    clocks = SelectionSignalDayV1(decision_trade_date, decision_trade_date, decision_trade_date)
    route = sources.resolve_day_source(plan, clocks)
    metadata = prepared.raw.artifact.metadata
    components = program.admitted_package_projection.components
    legs = metadata.get("component_artifacts")
    if legs is None and len(components) == 1:
        context = metadata.get("provider_metadata", {}).get("selection_native_input_context")
        normalized = metadata.get("artifact_input_context")
        common_fields = ("requested_trade_date", "effective_trade_date", "score_trade_date", "pit_mode",
                         "calendar_version", "calendar_hash", "calendar_source", "universe_input_hash",
                         "window_start_date", "required_window", "window_resolution", "window_lineage_hash")
        if (not isinstance(context, dict) or not isinstance(normalized, dict)
                or any(context.get(k) != normalized.get(k) for k in common_fields)):
            raise sources.SourcePlanError("SOURCE_TASK_NATIVE_CONTEXT_MISMATCH")
        legs = {components[0].component_id: {
            "input_context": context,
            "source_read_receipts": list(prepared.raw.source_read_receipts)}}
    if not isinstance(legs, dict) or set(legs) != {c.component_id for c in components}:
        raise sources.SourcePlanError("SOURCE_TASK_COMPONENT_RECEIPTS_MISSING")
    warmup = date_plan.per_program_input_warmup_ranges[program.research_program_id]
    windows = {c.component_id: next(w.window_start_trade_date for w in c.day_windows
                                  if w.decision_trade_date == decision_trade_date) for c in warmup.components}
    actual = {}
    for component in components:
        payload = legs[component.component_id]
        context = payload.get("input_context") or {}
        receipt = sources.input_receipt(route, payload)
        resolution = ("frozen_file_calendar" if route["source"] == "DATASET_FILES"
                      and component.window_resolution == "trading_calendar" else component.window_resolution)
        if (context.get("source_plan_receipt") != receipt
                or context.get("window_start_date") != str(windows[component.component_id])
                or context.get("required_window") != component.required_window
                or context.get("window_resolution") != resolution):
            raise sources.SourcePlanError("SOURCE_TASK_ACTUAL_RECEIPT_MISMATCH", component=component.component_id)
        actual[component.component_id] = receipt
    return {"schema_version": "advisory_task_selection_input_evidence_v1", "plan_hash": plan["plan_hash"],
            "source_run_id": plan["run_id"], "clocks": clocks.to_dict(), "component_receipts": actual}


@contextmanager
def selection_day_providers(*, plan, day: date, database_providers):
    """Reuse the existing shared file adapters and unique native policy engine."""
    route = sources.resolve_day_source(plan, SelectionSignalDayV1(day, day, day))
    if route["source"] == "DATABASE":
        yield database_providers
        return
    from backend.data_service.frozen_dataset import FrozenDataset, content_pin
    from backend.services.selection_center.risk_policy import (
        AnnouncementRiskDecisionProvider, StPitRiskDecisionProvider, StockRiskPolicyService,
    )
    from backend.services.simulation_data.frozen_file_adapters import FileTradabilityFilter
    from backend.services.strategy_package.selection_computation import StrategyPackageSelectionReadOnlyProvidersV1

    dataset = FrozenDataset(plan["file_source"]["binding"], consumer_scope="ADVISORY", run_id=plan["run_id"])
    try:
        pin = plan["file_source"]["suspend_meta_pin"]
        content_pin(dataset.root, pin["path"], expected=pin["sha256"])
        yield StrategyPackageSelectionReadOnlyProvidersV1(
            risk_policy=StockRiskPolicyService(providers={
                "st_pit": StPitRiskDecisionProvider(require_ready_state=False, span_loader=dataset.spans),
                "announcement_risk": AnnouncementRiskDecisionProvider(),
            }), tradability=FileTradabilityFilter(dataset))
        dataset.assert_unchanged()
    finally:
        dataset.close()
