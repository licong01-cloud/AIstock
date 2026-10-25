"""LocalSIM ownership/date adapter to the public frozen input-source contract."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from copy import deepcopy
from datetime import date
from pathlib import Path

from backend.services.advisory_historical_range import input_source_plan as sources
from backend.services.advisory_historical_range.selection_signal_kernel import SelectionSignalDayV1
from .models import canonical_json_sha256

PLAN_KEY = "localsim_task_input_source_plan_v1"
REQUEST_KEY = "localsim_source_plan_request_v1"
SOURCE_PLAN_ID = "localsim.task_input_source_plan.v1"
_PLAN = ContextVar("localsim_input_source_plan", default=None)
_DAY = ContextVar("localsim_input_source_day", default=None)


def current_source_plan():
    return _PLAN.get()


def current_source_day():
    return _DAY.get()


def request_identity(request):
    return canonical_json_sha256(request.model_dump(mode="json", exclude_none=True))


def freeze_source_calendar(calendar, end_trade_date):
    status = calendar.calendar_service.status(as_of_date=end_trade_date)
    start = date.fromisoformat(status["cache"]["coverage_start"])
    return tuple(calendar.list_trading_days(start, end_trade_date))


def native_policy(runtime_config):
    from backend.services.strategy_package.selection_computation import (
        parse_selection_runtime_profile_for_computation, selection_runtime_profile_sha256,
    )
    from backend.services.selection_center.canonical_pit_runtime import (
        CANONICAL_PIT_RUNTIME_LEASE_KEY, SelectionPitRuntimeLease,
    )

    profile = parse_selection_runtime_profile_for_computation(runtime_config)
    lease_payload = runtime_config.get(CANONICAL_PIT_RUNTIME_LEASE_KEY)
    if not isinstance(lease_payload, dict):
        raise sources.SourcePlanError("LOCALSIM_SOURCE_NATIVE_PIT_LEASE_REQUIRED", owner="LocalSIM")
    lease = SelectionPitRuntimeLease.from_mapping(lease_payload)
    required = ["daily", "minute", "factor", "pit"]
    if profile.tradability.exclude_suspended:
        required.append("suspend")
    if profile.industry_blacklist:
        required.append("sector")
    return {"policy_hash": selection_runtime_profile_sha256(profile), "universe_key": lease.universe_key,
            "rule_version": lease.rule_version, "reader_contract": sources.READER_CONTRACT,
            "required_components": required,
            "pit_policy": {k: getattr(lease, k) for k in ("authority_id", "rule_parameters_digest")}}


def freeze_replay_plan(*, mode, run_id, manifest, runtime_config, calendar, trading_days,
                       sequence_reader=None, file_source=None):
    """Only plan bounds; native InferenceEngine still assembles/resolves its inputs."""
    from backend.services.strategy_package.advisory_input_projection import (
        get_strategy_package_inference_required_window, project_historical_range_inputs,
    )
    if sequence_reader is None:
        from backend.services.advisory_historical_range.inference_window import FrozenSequenceLengthReader
        from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
        sequence_reader = FrozenSequenceLengthReader(
            asset_store=LocalPackageAssetStore(), repository_root=Path(__file__).resolve().parents[3],
            temporary_root=Path("X:/AIstock_temp/LOCALSIM/source-plan-metadata"))
    projection = project_historical_range_inputs(manifest)
    lengths = sequence_reader(manifest)
    if set(lengths) != {leg.alpha_component_id for leg in projection.legs}:
        raise sources.SourcePlanError("LOCALSIM_SOURCE_MODEL_WINDOW_IDENTITY_MISMATCH", owner="LocalSIM")
    if any(type(value) is not int or value < 1 for value in lengths.values()):
        raise sources.SourcePlanError("LOCALSIM_SOURCE_MODEL_WINDOW_INVALID", owner="LocalSIM")
    window = max(get_strategy_package_inference_required_window(leg.factor_order)
                 + lengths[leg.alpha_component_id] - 1 + 5 for leg in projection.legs)
    dates = sorted(set(calendar))
    rows = []
    for day in trading_days:
        if day not in dates or dates.index(day) < window:
            raise sources.SourcePlanError("LOCALSIM_SOURCE_CALENDAR_WARMUP_UNCOVERED", owner="LocalSIM")
        index = dates.index(day)
        rows.append({"decision_date": str(day), "target_trade_date": str(day),
                     "data_cutoff": str(dates[index - 1]), "history_start": str(dates[index - window])})
    plan = sources.build_source_plan(mode=mode, consumer_scope="LOCALSIM", run_id=run_id,
        days=rows, trading_calendar=[str(v) for v in dates], native_policy=native_policy(runtime_config),
        file_source=file_source)
    validate_local_clocks(plan)
    return plan


def validate_local_clocks(plan):
    days = plan["days"]
    calendar = plan["trading_calendar"]
    if [row["decision_date"] for row in days] != [d for d in calendar
            if days[0]["decision_date"] <= d <= days[-1]["decision_date"]]:
        raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_DAY_GAP", owner="LocalSIM")
    for row in days:
        index = calendar.index(row["decision_date"])
        if (not index or row["decision_date"] != row["target_trade_date"]
                or row["data_cutoff"] != calendar[index - 1]):
            raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_CLOCK_MISMATCH", owner="LocalSIM")


def frozen_source_plan(release, *, job=None):
    metadata = release.release_config_json.get("metadata") or {}
    plan = metadata.get(PLAN_KEY)
    if plan is None:
        if job is not None and job.historical_source_id == SOURCE_PLAN_ID:
            raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_MISSING", owner="LocalSIM")
        return None
    plan = sources.validate_source_plan(plan, consumer_scope="LOCALSIM", run_id=plan.get("run_id"))
    validate_local_clocks(plan)
    if plan["native_policy"] != native_policy(metadata["selection_runtime_config"]):
        raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_POLICY_MISMATCH", owner="LocalSIM")
    if plan.get("runtime_pit_identity") is not None:
        from backend.services.canonical_pit_inference_boundary import resolve_inference_pit_identity
        from backend.services.selection_center.canonical_pit_runtime import (
            CANONICAL_PIT_RUNTIME_LEASE_KEY, SelectionPitRuntimeLease,
        )
        planned = resolve_inference_pit_identity(plan["runtime_pit_identity"], version_tag="localsim",
                                               allow_active_canonical_pointer=False)
        if SelectionPitRuntimeLease.from_binding(planned.binding) != SelectionPitRuntimeLease.from_mapping(
                metadata["selection_runtime_config"][CANONICAL_PIT_RUNTIME_LEASE_KEY]):
            raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_PIT_LEASE_MISMATCH", owner="LocalSIM")
    if job is not None:
        days = [row["decision_date"] for row in plan["days"]]
        if (job.historical_source_id != SOURCE_PLAN_ID or job.historical_source_sha256 != plan["plan_hash"]
                or days[0] != str(job.start_trade_date) or days[-1] != str(job.end_trade_date)):
            raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_JOB_MISMATCH", owner="LocalSIM")
    return plan


def day_route(plan, trade_date):
    rows = [row for row in plan["days"] if row["decision_date"] == str(trade_date)]
    if len(rows) != 1:
        raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_DAY_MISSING", owner="LocalSIM")
    row = rows[0]
    clocks = SelectionSignalDayV1(*(date.fromisoformat(row[key]) for key in
                                   ("decision_date", "data_cutoff", "target_trade_date")))
    return sources.resolve_day_source(plan, clocks)


@contextmanager
def localsim_plan_inputs(plan, *, trade_date):
    """Reader lifetimes are per day/worker, never shared across concurrent jobs."""
    from .localsim_file_inputs import localsim_file_inputs
    from backend.data_service.frozen_dataset import content_pin

    plan = sources.validate_source_plan(plan, consumer_scope="LOCALSIM", run_id=plan.get("run_id"))
    validate_local_clocks(plan)
    if current_source_plan() is not None:
        raise sources.SourcePlanError("LOCALSIM_SOURCE_PLAN_SCOPE_ALREADY_BOUND", owner="LocalSIM")
    route = day_route(plan, trade_date)
    context = (localsim_file_inputs(plan["file_source"]["binding"], run_id=plan["run_id"])
               if route["source"] == "DATASET_FILES" else nullcontext(None))
    token = _PLAN.set(deepcopy(plan))
    day_token = _DAY.set(trade_date)
    try:
        with context as dataset:
            if dataset is not None:
                pin = plan["file_source"]["suspend_meta_pin"]
                content_pin(dataset.root, pin["path"], expected=pin["sha256"])
            yield route
    finally:
        _DAY.reset(day_token)
        _PLAN.reset(token)


def validate_payload(plan, clocks, payload):
    route = sources.resolve_day_source(plan, clocks)
    receipt = sources.input_receipt(route, payload)
    if payload.get("input_context", {}).get("source_plan_receipt") != receipt:
        raise sources.SourcePlanError("LOCALSIM_SOURCE_ACTUAL_RECEIPT_MISMATCH", owner="LocalSIM")
    return receipt


def validate_artifact_source(plan, clocks, artifact):
    owner = artifact.model_dump(mode="json")["metadata"].get("localsim_signal_run", {})
    if (owner.get("source_plan_hash") != plan["plan_hash"] or owner.get("source_run_id") != plan["run_id"]
            or not owner.get("native_inputs")):
        raise sources.SourcePlanError("LOCALSIM_FROZEN_RAW_SOURCE_PLAN_CONFLICT", owner="LocalSIM")
    for item in owner["native_inputs"]:
        validate_payload(plan, clocks, item)
