"""Consumer-owned source planning; no selection, account, DB write or fallback.

Metadata registration is separate from execution. Restore the saved plan, not
the active profile. DB input receipts describe actual reads, not table names.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any

from .selection_signal_kernel import SelectionSignalDayV1

SCHEMA = "selection_input_source_plan_v1"
READER_CONTRACT = "native_raw_share_cny_cutoff_qfq_v1"
MODES = {"FILE_ONLY", "DATABASE_ONLY", "FILE_THEN_DATABASE"}


class SourcePlanError(ValueError):
    def __init__(self, reason_code: str, *, owner="Advisory", **context):
        self.reason_code = reason_code
        self.receipt = {"status": "FAILED", "reason_code": reason_code, "owner": owner, **context}
        super().__init__(json.dumps(self.receipt, ensure_ascii=False, sort_keys=True))


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _day(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise SourcePlanError("SOURCE_DATE_NOT_CANONICAL")
    return parsed


def _sha(value: str) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _policy_fields(policy: dict) -> dict | None:
    from backend.services.canonical_equity_pit import (
        CANONICAL_PIT_AUTHORITY_ID, CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION,
        canonical_rule_parameters_digest,
    )
    if policy["universe_key"] != CANONICAL_PIT_UNIVERSE_KEY:
        return None  # Existing explicitly frozen noncanonical plans retain their exact contract.
    expected = {"authority_id": CANONICAL_PIT_AUTHORITY_ID,
                "rule_parameters_digest": canonical_rule_parameters_digest()}
    if (policy["rule_version"] != CANONICAL_PIT_RULE_VERSION
            or policy.get("pit_policy", expected) != expected):
        raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Advisory")
    return expected


def freeze_runtime_pit_identity() -> dict:
    """Read the authority once at registration, never on source-plan retry."""
    from backend.services.canonical_equity_pit import (
        CanonicalPitAuthorityResolver, require_canonical_new_task_binding,
    )
    from backend.services.canonical_pit_inference_boundary import InferencePitIdentity, InferencePitMode
    binding = require_canonical_new_task_binding(
        CanonicalPitAuthorityResolver().resolve_live_binding(), consumer="inference")
    return InferencePitIdentity(mode=InferencePitMode.ROLLING_RUNTIME, binding=binding).as_dict()


def freeze_file_source(*, profile_path=None, binding=None, root_override=None,
                       consumer_scope="ADVISORY", run_id="source-metadata-registration") -> dict:
    """Use the existing authority resolver, no copying and no data generation."""
    from backend.data_service.frozen_dataset import (
        FrozenDataset, content_pin, freeze_dataset_binding, inside,
    )
    binding = deepcopy(binding) if binding is not None else freeze_dataset_binding(profile_path)
    dataset = FrozenDataset(binding, consumer_scope=consumer_scope, run_id=run_id,
                            root_override=root_override)
    try:
        manifest = json.loads(dataset.file("manifest").read_bytes())
        names = ("day_meta_export", "minute_meta_export", "factor_meta")
        metadata = {name: json.loads(dataset.file(name).read_bytes()) for name in names}
        # The existing binding omits suspension metadata; pin this small file
        # from the same manifest without modifying the shared data module.
        pin = manifest["components"].get("suspend_meta")
        if not pin:
            raise SourcePlanError("SOURCE_COMPONENT_COVERAGE_MISSING", owner="Data", component="suspend_meta")
        pin = content_pin(dataset.root, pin["path"], expected=pin["sha256"])
        suspend = json.loads(inside(dataset.root, pin["path"]).read_bytes())
        metadata["suspend_meta"] = suspend
        minute_days = sorted({line[:10] for line in dataset.file("minute_calendar").read_text(
            encoding="utf-8-sig").splitlines() if line.strip()})
        dataset.assert_unchanged()
        return {"binding": binding, "metadata": metadata, "suspend_meta_pin": pin,
                "sector_coverage": manifest["components"]["sector_membership_spans"],
                "pit_coverage": manifest["st_pit_manifest"],
                "calendar": [str(v) for v in dataset.calendar], "minute_days": minute_days}
    finally:
        dataset.close()


def _coverage(snapshot: dict, policy: dict) -> dict:
    meta = snapshot["metadata"]
    day, minute, factors = (meta[name] for name in ("day_meta_export", "minute_meta_export", "factor_meta"))
    for name, entry in (("daily", day), ("minute", minute), ("pit", snapshot["pit_coverage"])):
        if (entry.get("rule_version") != policy["rule_version"]
                or entry.get("universe_key") != policy["universe_key"]):
            raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Data", component=name)
    if factors.get("universe_key") != policy["universe_key"]:
        raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Data", component="factors")
    # Coverage endpoints are producer declarations, not observed last rows:
    # suspension is sparse and cannot use its last event as its coverage end.
    result = {"daily": [day["start"], day["end"]],
              "minute": [minute["start"], minute["end"]],
              "factor": [factors["start"], factors["end"]],
              "pit": [day["pit_ensure"]["start_date"], snapshot["pit_coverage"]["cutoff_trade_date"]],
              "suspend": [meta["suspend_meta"]["start"], meta["suspend_meta"]["end"]],
              "sector": [snapshot["sector_coverage"]["start"], snapshot["sector_coverage"]["end"]]}
    for component, (start, end) in result.items():
        if _day(start) > _day(end):
            raise SourcePlanError("SOURCE_COMPONENT_COVERAGE_INVALID", owner="Data", component=component)
    required = policy.get("required_components", list(result))
    if (not isinstance(required, list) or len(required) != len(set(required))
            or not {"daily", "minute", "factor", "pit"}.issubset(required) or not set(required).issubset(result)):
        raise SourcePlanError("SOURCE_REQUIRED_COMPONENTS_INVALID")
    return {key: result[key] for key in required}


def _routes(body: dict) -> list[dict]:
    policy = body["native_policy"]
    if (not _sha(policy.get("policy_hash")) or not policy.get("rule_version") or not policy.get("universe_key")
            or policy.get("reader_contract") != READER_CONTRACT):
        raise SourcePlanError("SOURCE_NATIVE_POLICY_INVALID")
    days = body["days"]
    calendar = body["trading_calendar"]
    if not calendar or sorted(set(calendar)) != calendar:
        raise SourcePlanError("SOURCE_CALENDAR_INVALID")
    for value in calendar:
        _day(value)
    if not days or any(a["decision_date"] >= b["decision_date"] for a, b in zip(days, days[1:])):
        raise SourcePlanError("SOURCE_DAYS_INVALID")
    file = body["file_source"]
    coverage = _coverage(file, policy) if file is not None else None
    if (file is None) != (body["mode"] == "DATABASE_ONLY"):
        raise SourcePlanError("SOURCE_MODE_BINDING_MISMATCH")
    file_end = min(end for _, end in coverage.values()) if coverage else None
    result = []
    for row in days:
        clocks = SelectionSignalDayV1(*(_day(row[k]) for k in ("decision_date", "data_cutoff", "target_trade_date")))
        if any(value not in calendar for value in clocks.to_dict().values()):
            raise SourcePlanError("SOURCE_DAY_NOT_IN_CALENDAR", clocks=clocks.to_dict())
        if _day(row["history_start"]) > clocks.data_cutoff:
            raise SourcePlanError("SOURCE_HISTORY_WINDOW_INVALID", clocks=clocks.to_dict())
        use_file = file_end is not None and row["target_trade_date"] <= file_end
        if body["mode"] == "FILE_ONLY" and not use_file:
            raise SourcePlanError("SOURCE_FILE_COVERAGE_EXHAUSTED", owner="Data", clocks=clocks.to_dict(), file_end=file_end)
        if use_file:
            # Expiry selects DB; internal holes / insufficient warmup do NOT.
            for component, (start, _) in coverage.items():
                required_start = (row["history_start"] if component in {"daily", "factor"} else
                                  row["target_trade_date"] if component == "minute" else
                                  row["decision_date"] if component == "suspend" else row["data_cutoff"])
                if required_start < start:
                    raise SourcePlanError("SOURCE_FILE_HISTORY_UNCOVERED", owner="Data", component=component, clocks=clocks.to_dict())
            expected = [v for v in calendar if row["history_start"] <= v <= row["target_trade_date"]]
            if not set(expected).issubset(file["calendar"]):
                raise SourcePlanError("SOURCE_FILE_CALENDAR_GAP", owner="Data", clocks=clocks.to_dict())
            if row["target_trade_date"] not in file["minute_days"]:
                raise SourcePlanError("SOURCE_FILE_MINUTE_DAY_GAP", owner="Data", clocks=clocks.to_dict())
        result.append({**row, "source": "DATASET_FILES" if use_file else "DATABASE",
                       "file_binding_hash": file["binding"]["binding_hash"] if use_file else None})
    if any(a["source"] == "DATABASE" and b["source"] == "DATASET_FILES" for a, b in zip(result, result[1:])):
        raise SourcePlanError("SOURCE_PLAN_NOT_SINGLE_SWITCH")
    return result


def build_source_plan(*, mode: str, consumer_scope: str, run_id: str, days: list[dict],
                      trading_calendar: list[str], native_policy: dict,
                      profile_path=None, file_source=None) -> dict:
    """Task-creation API. Supply native history bounds; never infer model types."""
    if mode not in MODES or consumer_scope not in {"ADVISORY", "LOCALSIM"} or not run_id:
        raise SourcePlanError("SOURCE_PLAN_SCOPE_INVALID")
    if mode == "DATABASE_ONLY" and (file_source is not None or profile_path is not None):
        raise SourcePlanError("SOURCE_DATABASE_ONLY_HAS_FILE_BINDING")
    file = (None if mode == "DATABASE_ONLY" else
            deepcopy(file_source) if file_source is not None else freeze_file_source(
                profile_path=profile_path, consumer_scope=consumer_scope, run_id=run_id))
    if file is not None and file_source is not None:
        # Supplied metadata must still be the authority's bytes, not an edited
        # coverage claim attached to an otherwise valid binding. No H5 recopy
        # or rehash: this checks the small pinned metadata/calendar only.
        observed = freeze_file_source(binding=file["binding"], consumer_scope=consumer_scope, run_id=run_id)
        if observed != file:
            raise SourcePlanError("SOURCE_FILE_METADATA_DRIFT", owner="Data")
    body = {"schema_version": SCHEMA, "mode": mode, "consumer_scope": consumer_scope, "run_id": run_id,
            "native_policy": deepcopy(native_policy), "trading_calendar": list(trading_calendar),
            "days": deepcopy(days), "file_source": file}
    body["component_coverage"] = _coverage(file, native_policy) if file is not None else {}
    body["routes"] = _routes(body)
    body["first_database_day"] = next((v["decision_date"] for v in body["routes"] if v["source"] == "DATABASE"), None)
    if _policy_fields(native_policy) is not None and body["first_database_day"] is not None:
        body["runtime_pit_identity"] = freeze_runtime_pit_identity()
        _validate_runtime_identity(body)
    return {**body, "plan_hash": digest(body)}


def _validate_runtime_identity(body: dict) -> None:
    if _policy_fields(body["native_policy"]) is None or body["first_database_day"] is None:
        return
    from backend.services.canonical_pit_inference_boundary import resolve_inference_pit_identity, InferencePitMode
    from backend.services.canonical_equity_pit import require_canonical_new_task_binding
    identity = resolve_inference_pit_identity(body.get("runtime_pit_identity"), version_tag="historical")
    if identity.mode is not InferencePitMode.ROLLING_RUNTIME:
        raise SourcePlanError("SOURCE_RUNTIME_PIT_IDENTITY_INVALID", owner="Selection/PIT")
    for row in body["routes"]:
        if row["source"] == "DATABASE":
            require_canonical_new_task_binding(identity.binding, consumer="inference", trade_date=_day(row["data_cutoff"]))


def validate_source_plan(plan: dict, *, consumer_scope: str, run_id: str) -> dict:
    body = deepcopy({k: v for k, v in plan.items() if k != "plan_hash"})
    if (body.get("schema_version") != SCHEMA or digest(body) != plan.get("plan_hash")
            or body.get("consumer_scope") != consumer_scope or body.get("run_id") != run_id
            or body.get("mode") not in MODES):
        raise SourcePlanError("SOURCE_PLAN_IDENTITY_MISMATCH")
    if _routes(body) != body["routes"]:
        raise SourcePlanError("SOURCE_PLAN_ROUTES_MISMATCH")
    coverage = _coverage(body["file_source"], body["native_policy"]) if body["file_source"] is not None else {}
    if coverage != body.get("component_coverage"):
        raise SourcePlanError("SOURCE_PLAN_COVERAGE_MISMATCH")
    first = next((v["decision_date"] for v in body["routes"] if v["source"] == "DATABASE"), None)
    if first != body["first_database_day"]:
        raise SourcePlanError("SOURCE_PLAN_BOUNDARY_MISMATCH")
    _validate_runtime_identity(body)
    return {**body, "plan_hash": plan["plan_hash"]}


def resolve_day_source(plan: dict, clocks: SelectionSignalDayV1) -> dict:
    rows = [row for row in plan["routes"] if row["decision_date"] == str(clocks.decision_date)]
    if len(rows) != 1 or any(rows[0].get(k) != v for k, v in clocks.to_dict().items()):
        raise SourcePlanError("SOURCE_DAY_CLOCK_MISMATCH", clocks=clocks.to_dict())
    return {**deepcopy(rows[0]), "plan_hash": plan["plan_hash"], "policy_hash": plan["native_policy"]["policy_hash"],
            "reader_contract": READER_CONTRACT, "universe_key": plan["native_policy"]["universe_key"],
            "rule_version": plan["native_policy"]["rule_version"],
            "pit_policy": _policy_fields(plan["native_policy"]),
            "runtime_pit_identity": deepcopy(plan.get("runtime_pit_identity")) if rows[0]["source"] == "DATABASE" else None}


def create_input_session(plan: dict, clocks: SelectionSignalDayV1, *, consumer_scope: str,
                         run_id: str, max_bytes: int, root_override=None):
    from .inference_inputs import database_input_session, file_input_session
    plan = validate_source_plan(plan, consumer_scope=consumer_scope, run_id=run_id)
    route = resolve_day_source(plan, clocks)
    if route["source"] == "DATASET_FILES":
        session = file_input_session(consumer_scope=consumer_scope, run_id=run_id,
                                     binding=plan["file_source"]["binding"], max_bytes=max_bytes,
                                     root_override=root_override)
        # Recheck the extra coverage authority against the same release; this
        # does not inspect today's active profile or create a new binding.
        from backend.data_service.frozen_dataset import content_pin
        pin = plan["file_source"]["suspend_meta_pin"]
        try:
            content_pin(session.file_dataset.root, pin["path"], expected=pin["sha256"])
        except Exception:
            session.close()
            raise
    else:
        # Fresh DB session per day: no old revision masquerades as current data.
        session = database_input_session(consumer_scope=consumer_scope, run_id=run_id,
                                         source_identity=digest(route), max_bytes=max_bytes)
    session.source_plan_route = route
    session.source_identity = digest(route)
    return session


def input_receipt(route: dict, payload: dict) -> dict:
    """Bind actual input digests; calendar resolution is NOT a table-content pin."""
    reads = payload.get("source_read_receipts", [])
    required = {"pit_universe", "market_history", "fundamental_moneyflow", "trading_calendar"}
    if len(reads) != len(required) or {r.get("source_role") for r in reads} != required:
        raise SourcePlanError("SOURCE_ACTUAL_INPUT_RECEIPT_MISSING")
    inputs = []
    for item in reads:
        if not _sha(item.get("content_hash")):
            raise SourcePlanError("SOURCE_ACTUAL_INPUT_HASH_MISSING", component=item.get("source_role"))
        inputs.append({k: item.get(k) for k in ("source_role", "dataset_id", "partition_ref", "row_count", "content_hash",
                                               "query_template_id", "query_template_version", "parameter_hash", "admissibility")})
    context = payload.get("input_context", {})
    start = context.get("window_start_date")
    if (context.get("effective_trade_date") != route["data_cutoff"] or not isinstance(start, str)
            or not route["history_start"] <= start <= route["data_cutoff"]):
        raise SourcePlanError("SOURCE_NATIVE_WINDOW_MISMATCH", planned=route, observed=context)
    if route["source"] == "DATASET_FILES" and context.get("frozen_file_binding_hash") != route["file_binding_hash"]:
        raise SourcePlanError("SOURCE_FILE_INPUT_IDENTITY_MISMATCH")
    if route["source"] == "DATABASE" and context.get("frozen_file_binding_hash"):
        raise SourcePlanError("SOURCE_DATABASE_INPUT_IDENTITY_MISMATCH")
    body = {"route": deepcopy(route), "actual_inputs": sorted(inputs, key=lambda r: r["source_role"]),
            "native_input_context_hash": digest({k: v for k, v in context.items() if k != "source_plan_receipt"}),
            "identity_kind": "ACTUAL_CONSUMED_INPUT_DIGESTS", "database_revision": None,
            "calendar_evidence_kind": "ACTUAL_WINDOW_RESOLUTION_NOT_FULL_TABLE_HASH"}
    return {**body, "source_receipt_hash": digest(body)}


def validate_native_window(route: dict, *, start: date, cutoff: date, decision: date, pit_binding) -> None:
    """Guard only source/date identity; native code alone resolves window size."""
    if (str(cutoff) != route["data_cutoff"] or str(decision) != route["decision_date"]
            or not _day(route["history_start"]) <= start <= cutoff):
        raise SourcePlanError("SOURCE_NATIVE_WINDOW_MISMATCH", route=route, observed_start=str(start))
    canonical = route.get("pit_policy")
    if canonical is not None:
        from backend.services.canonical_equity_pit import require_same_canonical_policy, CanonicalPitContractError
        try:
            # Native helper validates canonical authority/parameters and frozen
            # release completeness, independently of the physical storage name.
            require_same_canonical_policy(pit_binding, pit_binding, consumer="inference")
        except CanonicalPitContractError as exc:
            raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Selection/PIT") from exc
        if (pit_binding.rule_version != route["rule_version"]
                or any(getattr(pit_binding, key) != value for key, value in canonical.items())):
            raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Advisory")
    elif (pit_binding.universe_key != route["universe_key"] or pit_binding.rule_version != route["rule_version"]):
        raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Data")


def session_pit_identity(session, *, request_identity, manifest_identity):
    """Translate producer-owned membership into the EXISTING native envelope.

    No activation receipt, stock eligibility rule or live pointer is fabricated.
    A supplied declaration is checked, never silently replaced with new members.
    """
    route = getattr(session, "source_plan_route", None)
    supplied = request_identity if request_identity is not None else manifest_identity
    if route is None or route.get("pit_policy") is None:
        validate_file_pit_declaration(route, request_identity=request_identity, manifest_identity=manifest_identity)
        return request_identity
    from backend.services.canonical_pit_inference_boundary import resolve_inference_pit_identity
    if route["source"] == "DATASET_FILES":
        dataset = session.file_dataset
        if dataset is None or dataset.identity != route["file_binding_hash"]:
            raise SourcePlanError("SOURCE_FILE_INPUT_IDENTITY_MISMATCH")
        if dataset.universe_key != route["universe_key"] or dataset.rule_version != route["rule_version"]:
            raise SourcePlanError("SOURCE_PIT_POLICY_MISMATCH", owner="Data")
        binding = dataset.binding
        expected = {"mode": "canonical_v2_frozen_candidate", **route["pit_policy"],
                    "rule_version": route["rule_version"], "release_id": binding["profile"]["release_id"],
                    "cutoff": binding["profile"]["cutoff"], "universe_as_of": route["data_cutoff"],
                    "snapshot_digest": binding["files"]["day_selection_universe"]["sha256"],
                    "universe_codes": dataset.members(_day(route["data_cutoff"]), universe_key=dataset.universe_key)}
    else:
        expected = route.get("runtime_pit_identity")
        if expected is None:
            raise SourcePlanError("SOURCE_RUNTIME_PIT_IDENTITY_REQUIRED", owner="Selection/PIT")
    parsed = resolve_inference_pit_identity(expected, version_tag="historical")
    if supplied is not None:
        observed = resolve_inference_pit_identity(supplied, version_tag="historical")
        if observed != parsed:
            raise SourcePlanError("SOURCE_DECLARED_PIT_IDENTITY_MISMATCH")
    return expected


def validate_file_pit_declaration(route: dict | None, *, request_identity, manifest_identity) -> None:
    # File-only inference cannot consult a live DB pointer for its PIT identity.
    # The existing native parser validates the explicitly frozen declaration.
    if (route is not None and route["source"] == "DATASET_FILES"
            and request_identity is None and manifest_identity is None):
        raise SourcePlanError("SOURCE_FILE_NATIVE_PIT_IDENTITY_REQUIRED", owner="Selection/PIT")


def verify_retry_inputs(previous: dict, current: dict) -> None:
    if (previous.get("input_context", {}).get("source_plan_receipt") != current.get("input_context", {}).get("source_plan_receipt")
            or not previous.get("input_context", {}).get("source_plan_receipt")
            or any(previous.get(k) != current.get(k) for k in ("scores", "universe_count"))
            or any(previous.get("metadata", {}).get(k) != current.get("metadata", {}).get(k)
                   for k in ("universe_members", "unscored_members"))):
        raise SourcePlanError("SOURCE_INPUT_DRIFT", owner="Data")


def gap_receipt(exc: Exception, *, stage: str, clocks: SelectionSignalDayV1, plan_hash: str) -> dict:
    """Keep native error identity; do not expose SQL/credentials or invent gaps."""
    context = getattr(exc, "context", {})
    code = getattr(exc, "reason_code", None) or context.get("reason_code") or type(exc).__name__
    supplied = getattr(exc, "receipt", {})
    return {"status": "FAILED", "reason_code": code, "owner": supplied.get("owner", "REQUIRES_TRIAGE"),
            "stage": stage, "clocks": clocks.to_dict(), "plan_hash": plan_hash,
            "component": supplied.get("component", context.get("component")), "fallback_performed": False}


def load_source_plan(path: str | Path, *, consumer_scope: str, run_id: str) -> dict:
    path = Path(path)
    if path.stat().st_size > 8 * 1024**2:
        raise SourcePlanError("SOURCE_PLAN_METADATA_BUDGET_EXCEEDED")
    return validate_source_plan(json.loads(path.read_bytes()), consumer_scope=consumer_scope, run_id=run_id)
