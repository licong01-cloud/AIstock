"""Immutable research price sets and independent read-only consumer status."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo
from time import monotonic

import pandas as pd

from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1, EconomicEntryValueRoleV1
from backend.services.advisory_model_first.economic_entry_daily_inference import (
    build_economic_daily_price_set_v1, project_economic_daily_advice_v1, daily_decision_feature_values_v1,
)
from backend.services.advisory_model_first.economic_entry_daily_source import (
    prepare_frozen_economic_research_day_v1, prepare_native_economic_research_day_v1, prepare_native_economic_formal_day_v1,
    EconomicEntryReadonlyCandidateSourceV1,
)
from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryCandidateProjectionV1
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_entry_serving_bundle import (
    load_research_serving_bundle_v1, load_confirmed_economic_entry_serving_bundle_v1, _economic_metadata_json, _read_economic_metadata_reference,
    _economic_metadata_file_reference,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryRiskBudgetV2
from backend.services.advisory_model_first.entry_price_daily_service import EntryWorkBudget, BoundedEntryReadSession
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.services.advisory_model_first.research_control import evidence_reference_for_file, AdvisoryResearchTrialRegistryV1


def _role_open(target):
    return datetime.combine(_day(target), time(9, 30), ZoneInfo("Asia/Shanghai"))


def _readonly_role_metadata(path):
    path = Path(path)
    if (not path.is_absolute() or path.resolve() != path.absolute() or path.drive.upper() == "C:"
            or not path.is_file() or not 0 < path.stat().st_size <= 65536):
        _fail("economic role metadata path/size is invalid")
    raw = path.read_bytes()
    if len(raw) > 65536:
        _fail("economic role metadata exceeds its bounded read")
    return _economic_metadata_json(raw)


def _readonly_daily_batch(path):
    path = Path(path)
    if (not path.is_absolute() or path.resolve() != path.absolute() or path.drive.upper() == "C:"
            or not path.is_file() or not 0 < path.stat().st_size <= 67108864):
        _fail("economic daily batch path/size is invalid")
    data = path.read_bytes()
    if len(data) > 67108864:
        _fail("economic daily batch exceeds its bounded read")
    return _economic_metadata_json(data)


def _daily_stage_preflight(path, *, native):
    """Reject oversized/unexpected files before the shared stage reader hashes them."""
    path = Path(path)
    manifest = _readonly_role_metadata(path / "manifest.json")
    limits = {"daily_batch.json": 67108864}
    if native:
        limits["native_d_capsule.json"] = 4194304
    if not isinstance(manifest.get("files"), dict) or set(manifest["files"]) != set(limits):
        _fail("economic daily stage has unexpected or missing artifacts")
    for name, maximum in limits.items():
        file = path / name
        if file.resolve() != file.absolute() or not file.is_file() or not 0 < file.stat().st_size <= maximum:
            _fail("economic daily stage exceeds its upfront file budget or is redirected")


class EconomicEntryValueReadonlyRoleStoreV1:
    """Consume separately authorized pointers. Construction/GET never enable a role."""

    @staticmethod
    def directory(*, consumer_root, program_id, binding_version_id):
        root = _durable_root(consumer_root)
        if any(not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is None
               for value in (program_id, binding_version_id)):
            _fail("economic role program/binding path identity is malformed")
        target = root / "entry_value_roles" / program_id / binding_version_id
        if target.resolve() != target.absolute():
            _fail("economic role path is redirected")
        return target

    def read(self, *, consumer_root, program_id, binding_version_id):
        directory = self.directory(consumer_root=consumer_root, program_id=program_id, binding_version_id=binding_version_id)
        active = directory / "active.json"
        if active.resolve() != active.absolute():
            _fail("economic active role pointer is redirected")
        if not active.exists():
            return None
        value = _readonly_role_metadata(active)
        functional = {key: item for key, item in value.items() if key != "pointer_sha256"}
        if (set(functional) != {"schema_version", "role_sha256", "enabled", "activated_at", "authorization_ref", "activation_registry_entry_id"}
                or functional["schema_version"] != "economic_entry_value_role_pointer_v1"
                or canonical_json_sha256(functional) != value.get("pointer_sha256") or type(functional["enabled"]) is not bool
                or not isinstance(functional["authorization_ref"], str) or not functional["authorization_ref"].strip()
                or not isinstance(functional["role_sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", functional["role_sha256"]) is None
                or not isinstance(functional["activation_registry_entry_id"], str)
                or re.fullmatch(r"advtrial_[0-9a-f]{24}", functional["activation_registry_entry_id"]) is None):
            _fail("economic active role pointer content or authority is invalid")
        role_path = directory / "versions" / functional["role_sha256"] / "preregistered/role.json"
        role = EconomicEntryValueRoleV1.model_validate(_readonly_role_metadata(role_path))
        read_stage(role_path.parent, stage="preregistered", plan_sha256=role.role_sha256, parent_sha256=None)
        try:
            clock = datetime.fromisoformat(functional["activated_at"])
        except (TypeError, ValueError):
            _fail("economic activation clock malformed")
        if (clock.utcoffset() is None or role.created_at > clock or clock >= _role_open(role.effective_from_target_date)
                or role.role_sha256 != functional["role_sha256"] or role.program_id != program_id
                or role.binding_version_id != binding_version_id):
            _fail("economic role pointer has foreign identity or an invalid effective clock")
        return role, value, role_path

    def verify_activation(self, *, loaded, role, pointer, role_path):
        path = Path(role.qualified_manifest.artifact_uri)
        if (role.scope != loaded.manifest.scope or role.bundle_id != loaded.manifest.bundle_id
                or role.confirmation_request_sha256 != loaded.confirmation_request.request_sha256
                or evidence_reference_for_file(path, role="ECONOMIC_ENTRY_VALUE_SERVING_MANIFEST") != role.qualified_manifest):
            _fail("economic role does not bind the actual qualified bundle and confirmation")
        registry_path = Path(loaded.manifest.confirmation_request_ref.artifact_uri).parent.parent / "trial_registry.jsonl"
        if (registry_path.resolve() != registry_path.absolute() or not registry_path.is_file()
                or not 0 < registry_path.stat().st_size <= 16777216):
            _fail("economic activation registry is unavailable, redirected or oversized")
        records = [record for record in AdvisoryResearchTrialRegistryV1(registry_path).read()
                   if record.registry_entry_id == pointer["activation_registry_entry_id"]]
        role_ref = evidence_reference_for_file(role_path, role="ECONOMIC_ENTRY_VALUE_ROLE")
        if len(records) != 1:
            _fail("economic role lacks its actual activation registry record")
        record = records[0]
        request = loaded.confirmation_request
        if (record.study_type != "ACTIVATION" or record.result_class != "ACTIVATED" or record.decision_use != "ACTIVATION_EVIDENCE"
                or record.research_stage != "ACTIVATED" or record.objective_contract != role.objective_contract
                or record.dataset_identity != request.dataset_identity or record.policy_identity != request.policy_identity
                or record.schema_identity != role.scope.feature_schema_sha256 or request.experiment_id not in record.parent_lineage
                or record.recorded_at.utcoffset() is None or not role.created_at <= record.recorded_at
                    <= datetime.fromisoformat(pointer["activated_at"])
                or any((record.planned_trial_count, record.generated_trial_count, record.evaluated_trial_count, record.selected_trial_count))
                or record.consumed_windows or role_ref not in record.evidence_refs or role.qualified_manifest not in record.evidence_refs):
            _fail("economic activation record differs from role, confirmation, scope or authorization clock")
        return record


def _durable_root(value):
    path = Path(value)
    if not path.is_absolute() or path.resolve().drive.upper() == "C:" or path.resolve() != path.absolute():
        _fail("economic consumer root must be an explicit non-C nonredirected path")
    return path


def _bundle(value):
    if not isinstance(value, str) or not re.fullmatch(r"adveserve_[0-9a-f]{24}", value):
        _fail("economic consumer bundle ID malformed")
    return value


def _day(value):
    if type(value) is date:
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        _fail("economic consumer date must be an exact trade date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        _fail("economic consumer date malformed")


def build_environment_economic_entry_daily_service_v1():
    return AdvisoryEconomicEntryDailyServiceV1(
        consumer_root=os.getenv("AISTOCK_ADVISORY_ECONOMIC_ENTRY_ROOT", "").strip() or None,
        aligned_output_root=os.getenv("AISTOCK_ADVISORY_ECONOMIC_ALIGNED_ROOT", "").strip() or None)


def _frozen_calendar(loaded):
    values = pd.DatetimeIndex(json.loads(loaded.original_input_files["calendar.json"]))
    if values.empty or not values.is_unique or not values.is_monotonic_increasing or values.tz is not None or not values.normalize().equals(values):
        _fail("economic immutable calendar is not unique chronological trade dates")
    return tuple(values.date)


def _authorize_target(loaded, target):
    target = _day(target)
    calendar = _frozen_calendar(loaded)
    if target not in calendar or calendar.index(target) == 0:
        _fail("economic read target is not a registered next trading day")
    decision = calendar[calendar.index(target) - 1]
    request = loaded.source_plan.training_request.source_request
    if not request.test_start <= decision <= request.test_end:
        _fail("economic read cannot consume a new or sealed window")
    return decision


def _batch_payload(prepared, loaded, budget, *, formal_role=None):
    sources = tuple(EconomicEntryDailyInputV1.model_validate(value.model_dump()) for value in prepared.inputs)
    if (len(sources) > 20 or len(sources) != len(prepared.decision_features)
            or len(sources) != len(prepared.price_contexts)):
        _fail("economic prepared batch violates its upfront 20-candidate/100000-node budget")
    original = json.loads(loaded.original_input_files["identity.json"])
    formal = formal_role is not None
    common = {"schema_version": "economic_entry_daily_batch_v1", "bundle_id": loaded.manifest.bundle_id,
        "model_bundle_sha256": loaded.manifest.manifest_sha256, "model_scope_sha256": loaded.manifest.scope.scope_sha256,
        "program_id": formal_role.program_id if formal else original["program_id"],
        "binding_version_id": formal_role.binding_version_id if formal else original["binding_version_id"],
        "evidence_state": "CONFIRMED_ENTRY_VALUE" if formal else "RESEARCH_NAVIGATION",
        "decision_use": "ADVISORY_ONLY" if formal else "NAVIGATION_ONLY", "deployable": formal,
        "role_binding_sha256": formal_role.role_sha256 if formal else None,
        "objective_contract": "RISK_MANAGED_ADVISORY", "source_receipt": prepared.source_receipt}
    if not sources:
        return {**common, "status": "NO_CANDIDATES", "advice": []}
    first = sources[0]
    bound = (first.decision_date, first.target_date, first.candidate_roster_sha256, first.restored_cohort_sha256,
             first.captured_at, first.run_id, first.list_id)
    symbols, ranks = [], []
    for source in sources:
        if (source.scope != loaded.manifest.scope or source.program_id != common["program_id"]
                or source.binding_version_id != common["binding_version_id"]
                or source.source_evidence != ("NATIVE_COMPLETE" if formal else "RECOVERED_LIMITED")
                or source.evidence_level != ("PROSPECTIVE_INPUT" if formal else "HISTORICAL_REPLAY")
                or (source.decision_date, source.target_date, source.candidate_roster_sha256, source.restored_cohort_sha256,
                    source.captured_at, source.run_id, source.list_id) != bound):
            _fail("economic research batch mixes program, cohort, clock or evidence identities")
        symbols.append(source.instrument)
        ranks.append(source.selection_rank)
    if len(set(symbols)) != len(symbols) or len(set(ranks)) != len(ranks) or ranks != sorted(ranks):
        _fail("economic research batch must preserve unique candidates and original rank order")
    common.update(decision_date=first.decision_date.isoformat(), target_date=first.target_date.isoformat(),
        captured_at=first.captured_at.isoformat(), candidate_roster_sha256=first.candidate_roster_sha256,
        restored_cohort_sha256=first.restored_cohort_sha256, run_id=first.run_id, list_id=first.list_id)
    if first.run_id is not None and prepared.source_receipt.get("candidate_source", {}).get("native_receipt_created") is not False:
        _fail("economic native research must retain its original archive and limited evidence")
    risk = loaded.confirmation_request.business_risk if formal else EntryRiskBudgetV2(maximum_loss_bps=loaded.fitted.request.risk_reference_bps,
        reference_use="FIXED_RESEARCH_STOP_REFERENCE",
        configuration_sha256=loaded.source_plan.training_request.source_request.request_sha256)
    advice = []
    for source, features, context in zip(sources, prepared.decision_features, prepared.price_contexts, strict=True):
        budget.check()
        advice.append(build_economic_daily_price_set_v1(fitted=loaded.fitted, model_scope=loaded.manifest.scope,
            model_bundle_sha256=loaded.manifest.manifest_sha256, prediction_input=source, context=context,
            decision_features=features, trading_calendar=prepared.trading_calendar, risk_budget=risk,
            qualified_bundle=loaded if formal else None, role_binding=formal_role))
    budget.check()
    return {**common, "status": "PUBLISHED" if formal else "RESEARCH_NAVIGATION", "advice": advice}


class AdvisoryEconomicEntryDailyServiceV1:
    """GET never captures or writes. Current research weights are never auto-enabled."""

    def __init__(self, *, consumer_root=None, aligned_output_root=None, bundle_loader=None,
                 confirmed_bundle_loader=None, role_store=None, program_provider=None, candidate_source_factory=None, now_provider=None):
        self._root = Path(consumer_root) if consumer_root else None
        self._aligned_root = Path(aligned_output_root) if aligned_output_root else None
        self._loader = bundle_loader or load_research_serving_bundle_v1
        self._confirmed_loader = confirmed_bundle_loader or load_confirmed_economic_entry_serving_bundle_v1
        self._roles = role_store or EconomicEntryValueReadonlyRoleStoreV1()
        self._program_provider = program_provider
        self._candidate_source_factory = candidate_source_factory
        self._now = now_provider or (lambda: datetime.now(timezone.utc))
        self._last_attempt = {}

    def _program_state(self, program_id, session):
        if self._program_provider is not None:
            programs = self._program_provider(session)
            return programs.get_program(program_id), programs.get_active_binding_version(program_id)
        from backend.services.advisory_program import AdvisoryProgramPGRepository
        with session.connection() as connection:
            @contextmanager
            def pinned_connection():
                session.budget.check()
                yield connection
                session.budget.check()
            programs = AdvisoryProgramPGRepository(conn_factory=pinned_connection)
            return programs.get_program(program_id), programs.get_active_binding_version(program_id)

    @staticmethod
    def _check_program_scope(program, binding, role, loaded):
        projection = loaded.native_training_scope.projection
        if (binding is None or binding.binding_version_id != role.binding_version_id or binding.program_id != role.program_id
                or binding.activation_status != "ACTIVE" or list(binding.package_ids) != [role.scope.package_id]
                or list(program.package_ids) != [role.scope.package_id] or program.program_id != role.program_id
                or program.review_policy_sha256 != projection.review_policy_sha256 or program.target_count != 20
                or (binding.runtime_config_json or {}).get("universe_selection") != projection.universe_selection.model_dump(mode="json")):
            _fail("current economic role program/binding/package/review/universe differs from confirmed scope")
        return program, binding

    def _resolved_role(self, *, program_id, session, budget):
        self._validate_roots()
        if self._root is None or self._aligned_root is None:
            return None
        if not isinstance(program_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", program_id) is None:
            _fail("economic role program identity malformed")
        directory = self._root / "entry_value_roles" / program_id
        if directory.resolve() != directory.absolute():
            _fail("economic program role directory is redirected")
        if not directory.is_dir():
            return None
        program, binding = self._program_state(program_id, session)
        if binding is None:
            _fail("economic configured role has no active Program binding")
        value = self._roles.read(consumer_root=self._root, program_id=program_id, binding_version_id=binding.binding_version_id)
        if value is None:
            return None
        role, pointer, role_path = value
        now = self._now()
        if not isinstance(now, datetime) or now.utcoffset() is None or datetime.fromisoformat(pointer["activated_at"]) > now:
            _fail("economic role pointer contains a future or unproven activation clock")
        expected_path = self._root / "serving_bundles" / role.bundle_id / "preregistered/serving_manifest.json"
        if Path(role.qualified_manifest.artifact_uri) != expected_path or expected_path.resolve() != expected_path.absolute():
            _fail("economic role qualified manifest is outside its own consumer namespace")
        if not pointer["enabled"] or program.status != "ENABLED":
            return role, pointer, None, program
        budget.check()
        loaded = self._confirmed_loader(manifest_path=expected_path, aligned_output_root=self._aligned_root)
        self._roles.verify_activation(loaded=loaded, role=role, pointer=pointer, role_path=role_path)
        self._check_program_scope(program, binding, role, loaded)
        budget.check()
        return role, pointer, loaded, program

    def _validate_roots(self):
        for value in (self._root, self._aligned_root):
            if value is not None:
                _durable_root(value)

    def status(self, *, program_id, target_date=None):
        if not isinstance(program_id, str) or not program_id.strip():
            _fail("economic status requires a program identity")
        self._validate_roots()
        target = _day(target_date).isoformat() if target_date is not None else None
        common = {"schema_version": "economic_entry_consumer_status_v1", "program_id": program_id,
            "role": "ENTRY_VALUE", "objective_contract": "RISK_MANAGED_ADVISORY", "status": "NOT_CONFIGURED",
            "reason_code": "NO_QUALIFIED_ENTRY_VALUE_ROLE_BOUND", "target_date": target,
            "evidence_state": "UNCONFIRMED", "deployable": False, "advice": [],
            "qualification_gaps": ["independent_economic_confirmation", "native_training_scope", "explicit_business_risk_contract"],
            "automatic_capture_enabled": False, "research_namespace_configured": self._root is not None and self._aligned_root is not None}
        budget = EntryWorkBudget()
        session = BoundedEntryReadSession(budget)
        try:
            resolved = self._resolved_role(program_id=program_id, session=session, budget=budget)
            if resolved is None:
                return common
            role, pointer, loaded, program = resolved
            common.update(binding_version_id=role.binding_version_id, bundle_id=role.bundle_id, role_binding_sha256=role.role_sha256,
                pointer_sha256=pointer["pointer_sha256"], qualification_gaps=[])
            if not pointer["enabled"] or program.status != "ENABLED":
                return {**common, "status": "DISABLED", "reason_code": "ENTRY_VALUE_ROLE_DISABLED" if not pointer["enabled"] else "PROGRAM_NOT_ENABLED"}
            chosen = _day(target_date) if target_date is not None else self._latest_formal_target(role)
            common.update(evidence_state="CONFIRMED_ENTRY_VALUE", automatic_capture_enabled=True,
                          resolved_target_date=chosen.isoformat() if chosen else None)
            if chosen is None:
                return {**common, "status": "NOT_CAPTURED", "reason_code": "NO_EXISTING_NATIVE_D_ARTIFACT"}
            if chosen < role.effective_from_target_date:
                return {**common, "status": "NOT_EFFECTIVE", "reason_code": "TARGET_PRECEDES_QUALIFIED_ROLE"}
            payload = self._read_formal(role=role, loaded=loaded, target=chosen)
            if payload is None:
                return {**common, "status": "NOT_CAPTURED", "reason_code": "NO_EXISTING_NATIVE_D_ARTIFACT"}
            now = self._now()
            if not isinstance(now, datetime) or now.utcoffset() is None:
                _fail("economic consumer now clock must be aware")
            current = now < _role_open(chosen)
            advice = [project_economic_daily_advice_v1(value) for value in payload["advice"]]
            current_role = self._roles.read(consumer_root=self._root, program_id=role.program_id, binding_version_id=role.binding_version_id)
            if current_role is None or current_role[0] != role or current_role[1] != pointer:
                _fail("economic role pointer changed during advice readback")
            budget.check()
            return {**common, "status": payload["status"] if current else "STALE", "reason_code": None if current else "D_ADVICE_EXPIRED_AT_T_OPEN",
                "deployable": current, "advice": advice, "decision_date": payload["decision_date"], "captured_at": payload["captured_at"],
                "original_batch_sha256": canonical_json_sha256(payload), "decision_use": "ADVISORY_ONLY"}
        finally:
            session.close()

    def run_once(self):
        self._validate_roots()
        directory = self._root / "entry_value_roles" if self._root is not None else None
        if directory is None or not directory.is_dir():
            return {"status": "NOT_CONFIGURED", "reason_code": "NO_QUALIFIED_ENTRY_VALUE_ROLE_BOUND", "new_artifacts": 0}
        budget, session = EntryWorkBudget(), None
        try:
            if directory.resolve() != directory.absolute():
                _fail("economic role namespace is redirected")
            session = BoundedEntryReadSession(budget)
            from backend.services.advisory_model_first.entry_price_daily_service import EntryReadOnlyCalendar
            now = self._now()
            if not isinstance(now, datetime) or now.utcoffset() is None:
                _fail("economic capture cycle clock must be aware")
            local = now.astimezone(ZoneInfo("Asia/Shanghai"))
            target = None
            # At most one capture per hook; completed dates are not recalculated.
            failures = []
            for program_directory in sorted(directory.iterdir(), key=lambda value: (self._last_attempt.get(value.name, 0), value.name)):
                budget.check()
                if not program_directory.is_dir():
                    continue
                try:
                    resolved = self._resolved_role(program_id=program_directory.name, session=session, budget=budget)
                except Exception as exc:
                    budget.check()
                    self._last_attempt[program_directory.name] = monotonic()
                    failures.append({"program_id": program_directory.name, "status": "INPUT_UNAVAILABLE",
                        "reason_code": getattr(exc, "reason_code", "ECONOMIC_ROLE_UNAVAILABLE")})
                    continue
                if resolved is None or not resolved[1]["enabled"]:
                    continue
                role, pointer, loaded, program = resolved
                if program.status != "ENABLED" or (program.review_schedule or {}).get("frequency") != "daily_after_close":
                    continue
                if target is None:
                    target = EntryReadOnlyCalendar(session.connection).next_trading_day(local.date(), inclusive=local.time() < time(9, 30))
                if target < role.effective_from_target_date:
                    continue
                try:
                    if self._read_formal(role=role, loaded=loaded, target=target) is not None:
                        continue
                except Exception as exc:
                    # Corrupt artifacts fail closed for this Program, but must
                    # not prevent another eligible Program from progressing.
                    # A global budget failure still aborts the entire cycle.
                    budget.check()
                    self._last_attempt[role.program_id] = monotonic()
                    failures.append({"program_id": role.program_id, "target_date": target.isoformat(), "status": "INPUT_UNAVAILABLE",
                                     "reason_code": getattr(exc, "reason_code", "ECONOMIC_CAPTURE_UNAVAILABLE")})
                    continue
                self._last_attempt[role.program_id] = monotonic()
                try:
                    result = self._capture_formal(role=role, pointer=pointer, loaded=loaded, target=target, budget=budget, session=session)
                except Exception as exc:
                    budget.check()
                    result = {"program_id": role.program_id, "target_date": target.isoformat(), "status": "INPUT_UNAVAILABLE",
                              "reason_code": getattr(exc, "reason_code", "ECONOMIC_CAPTURE_UNAVAILABLE")}
                return {"status": "CHECKED", "new_artifacts": int(result["status"] in {"PUBLISHED", "NO_CANDIDATES"}), "results": [*failures, result]}
            return {"status": "CHECKED", "new_artifacts": 0, "results": failures}
        finally:
            if session is not None:
                session.close()

    def _formal_path(self, role, target):
        directory = self._roles.directory(consumer_root=self._root, program_id=role.program_id, binding_version_id=role.binding_version_id)
        path = self._root / "entry_value_daily" / role.role_sha256 / _day(target).isoformat()
        if directory.resolve() != directory.absolute() or path.resolve() != path.absolute():
            _fail("economic formal daily namespace is redirected")
        return path

    def _latest_formal_target(self, role):
        root = self._formal_path(role, role.effective_from_target_date).parent
        if not root.is_dir():
            return None
        dates = []
        for entry in root.iterdir():
            if len(dates) >= 10000:
                _fail("economic daily namespace exceeds its read budget")
            if entry.is_dir() and (entry / "prepared/daily_batch.json").is_file():
                dates.append(_day(entry.name))
        return max(dates) if dates else None

    def _read_formal(self, *, role, loaded, target):
        path = self._formal_path(role, target) / "prepared"
        if not path.exists():
            return None
        _daily_stage_preflight(path, native=True)
        file = path / "daily_batch.json"
        read_stage(path, stage="prepared", plan_sha256=role.role_sha256, parent_sha256=loaded.manifest.manifest_sha256)
        payload = _readonly_daily_batch(file)
        if (payload.get("schema_version") != "economic_entry_daily_batch_v1" or payload.get("program_id") != role.program_id
                or payload.get("binding_version_id") != role.binding_version_id or payload.get("bundle_id") != role.bundle_id
                or payload.get("role_binding_sha256") != role.role_sha256 or payload.get("model_scope_sha256") != role.scope.scope_sha256
                or payload.get("model_bundle_sha256") != loaded.manifest.manifest_sha256 or payload.get("target_date") != _day(target).isoformat()
                or payload.get("status") not in {"PUBLISHED", "NO_CANDIDATES"} or payload.get("evidence_state") != "CONFIRMED_ENTRY_VALUE"
                or payload.get("decision_use") != "ADVISORY_ONLY" or payload.get("deployable") is not True
                or payload.get("source_receipt", {}).get("source_evidence") != "NATIVE_COMPLETE"
                or payload.get("source_receipt", {}).get("outcomes_read") is not False
                or payload.get("source_receipt", {}).get("sealed_accessed") is not False
                or not isinstance(payload.get("advice"), list) or len(payload["advice"]) > 20
                or (payload["status"] == "NO_CANDIDATES") != (not payload["advice"])):
            _fail("economic formal batch differs from its actual role/model/native identities")
        # Empty candidate batches have no row-level input validator. Validate
        # their real native identities and capture window just like nonempty
        # batches; matching hashes cannot establish a valid prospective clock.
        try:
            decision = _day(payload.get("decision_date"))
            captured = datetime.fromisoformat(payload.get("captured_at"))
        except (TypeError, ValueError):
            _fail("economic native batch identity or prospective clock differs")
        if (any(not isinstance(payload.get(name), str) or not payload[name].strip() for name in ("run_id", "list_id"))
                or decision >= _day(target) or _day(target) < role.effective_from_target_date
                or decision <= max(loaded.fitted.request.source_request.label_cutoff, loaded.native_training_scope.latest_upstream_training_date)
                or captured.utcoffset() is None
                or not max(role.created_at, datetime.combine(decision, time(15), ZoneInfo("Asia/Shanghai"))) <= captured < _role_open(_day(target))):
            _fail("economic native batch identity or prospective clock differs")
        symbols, ranks = [], []
        source_receipt = payload["source_receipt"]
        roster = source_receipt.get("candidate_roster")
        members = source_receipt.get("admitted_universe_members")
        if (not isinstance(roster, list) or canonical_json_sha256(roster) != payload.get("candidate_roster_sha256")
                or not isinstance(members, list) or not members or any(not isinstance(value, str) for value in members)
                or members != sorted(set(members)) or len(roster) != len(payload["advice"])
                or canonical_json_sha256(members) != source_receipt.get("candidate_source", {}).get("admitted_universe_members_sha256")):
            _fail("economic formal batch lost its complete original roster or membership")
        capsule = _read_economic_metadata_reference(_economic_metadata_file_reference(path / "native_d_capsule.json",
            role="ECONOMIC_ENTRY_NATIVE_D_CAPSULE", maximum_bytes=4194304),
            role="ECONOMIC_ENTRY_NATIVE_D_CAPSULE", maximum_bytes=4194304)
        bound_names = ("program_id", "binding_version_id", "run_id", "list_id", "decision_date", "target_date",
                       "captured_at", "candidate_roster_sha256", "source_receipt")
        if (set(capsule) != {*bound_names, "schema_version", "scope_sha256", "roster", "source_members", "inputs", "price_contexts"}
                or capsule.get("schema_version") != "economic_entry_native_D_capsule_v1" or capsule.get("scope_sha256") != role.scope.scope_sha256
                or any(canonical_json_sha256(capsule[name]) != canonical_json_sha256(payload[name]) for name in bound_names)
                or capsule.get("roster") != roster or capsule.get("source_members") != members
                or not isinstance(capsule.get("inputs"), list) or not isinstance(capsule.get("price_contexts"), list)
                or len(capsule["inputs"]) != len(roster) or len(capsule["price_contexts"]) != len(roster)):
            _fail("economic formal batch differs from its complete original D capsule")
        for advice, record, context in zip(payload["advice"], capsule["inputs"], capsule["price_contexts"], strict=True):
            source = EconomicEntryDailyInputV1.model_validate(advice["prediction_input"])
            if (not isinstance(record, dict) or set(record) != {"prediction_input", "decision_features"}
                    or record["prediction_input"] != advice["prediction_input"]
                    or source.feature_values_sha256 != canonical_json_sha256(daily_decision_feature_values_v1(record["decision_features"], role.scope.feature_names))
                    or (context is not None and not isinstance(context, dict))
                    or source.price_context_sha256 != (canonical_json_sha256(context) if context is not None else None)):
                _fail("economic formal batch lost original D feature or price context content")
            project_economic_daily_advice_v1(advice)
            if (source.scope != role.scope or source.program_id != role.program_id or source.binding_version_id != role.binding_version_id
                    or source.source_evidence != "NATIVE_COMPLETE" or source.evidence_level != "PROSPECTIVE_INPUT"
                    or source.target_date != _day(target) or source.decision_date.isoformat() != payload.get("decision_date")
                    or source.captured_at.isoformat() != payload.get("captured_at") or source.captured_at < role.created_at
                    or source.run_id != payload.get("run_id") or source.list_id != payload.get("list_id")
                    or source.candidate_roster_sha256 != payload.get("candidate_roster_sha256")
                    or source.universe_membership_sha256 != canonical_json_sha256(members) or source.instrument not in members
                    or advice.get("role_binding_sha256") != role.role_sha256 or advice.get("model_bundle_sha256") != loaded.manifest.manifest_sha256
                    or advice.get("evidence_state") != "CONFIRMED_ENTRY_VALUE" or advice.get("model_scope_sha256") != role.scope.scope_sha256
                    or advice.get("deployable") is not True or advice.get("decision_use") != "ADVISORY_ONLY"
                    or advice.get("risk_budget") != loaded.confirmation_request.business_risk.model_dump(mode="json")
                    or len(advice.get("query_nodes", [])) > 5000):
                _fail("economic formal advice differs from frozen scope/clock/candidate or risk")
            symbols.append(source.instrument)
            ranks.append(source.selection_rank)
        if len(set(symbols)) != len(symbols) or len(set(ranks)) != len(ranks) or ranks != sorted(ranks):
            _fail("economic formal batch candidate uniqueness/order differs")
        if roster != [{"instrument": symbol, "selection_rank": rank} for symbol, rank in zip(symbols, ranks, strict=True)]:
            _fail("economic formal advice differs from the exact frozen candidate roster")
        return payload

    def _capture_formal(self, *, role, pointer, loaded, target, budget, session):
        target = _day(target)
        existing = self._read_formal(role=role, loaded=loaded, target=target)
        if existing is not None:
            return {"status": "EXACT_RETRY", "target_date": target.isoformat(), "new_inferences": 0, "batch_sha256": canonical_json_sha256(existing)}
        now = self._now()
        if now >= _role_open(target):
            return {"status": "MISSED_CAPTURE", "target_date": target.isoformat(), "new_inferences": 0}
        if now < role.created_at or target < role.effective_from_target_date:
            return {"status": "NOT_EFFECTIVE", "target_date": target.isoformat(), "new_inferences": 0}
        factory = self._candidate_source_factory
        source = factory(session, loaded.native_training_scope, budget) if factory else EconomicEntryReadonlyCandidateSourceV1(
            read_session=session, pit_universe_key=loaded.native_training_scope.pit_universe_key, now=self._now)
        try:
            observed = source.load_day(program_id=role.program_id, binding_version_id=role.binding_version_id, target_date=target,
                projection=loaded.native_training_scope.projection)
        except Exception as exc:
            if getattr(exc, "reason_code", None) != "ADVISORY_ECONOMIC_FROZEN_LIST_NOT_READY":
                raise
            return {"status": "DEFERRED", "program_id": role.program_id, "target_date": target.isoformat(),
                    "reason_code": "ADVISORY_ECONOMIC_FROZEN_LIST_NOT_READY", "new_inferences": 0}
        prepared = prepare_native_economic_formal_day_v1(loaded=loaded, observed=observed, role=role)
        payload = _batch_payload(prepared, loaded, budget, formal_role=role)
        payload.update(decision_date=_day(observed["decision_date"]).isoformat(), target_date=target.isoformat(),
            run_id=observed["selection_run_id"], list_id=observed["list_version_id"], captured_at=observed["captured_at"].isoformat(),
            candidate_roster_sha256=prepared.source_receipt["roster_sha256"])
        budget.check()
        # Fresh metadata check after inference, not a duplicate generation check
        # inside the same repeatable-read quote snapshot.
        program, binding = self._program_state(role.program_id, session)
        self._check_program_scope(program, binding, role, loaded)
        if program.status != "ENABLED":
            _fail("economic Program was disabled during native capture")
        current = self._roles.read(consumer_root=self._root, program_id=role.program_id, binding_version_id=role.binding_version_id)
        if current is None or current[0] != role or current[1] != pointer:
            _fail("economic role pointer changed during native capture")
        if self._now() >= _role_open(target):
            return {"status": "MISSED_CAPTURE", "target_date": target.isoformat(), "new_inferences": 0}
        # Capture complete model-independent D inputs now, never reconstruct old
        # features or backdate a future confirmation producer's prediction time.
        capsule = {name: payload[name] for name in ("program_id", "binding_version_id", "run_id", "list_id",
            "decision_date", "target_date", "captured_at", "candidate_roster_sha256", "source_receipt")}
        capsule.update(schema_version="economic_entry_native_D_capsule_v1", scope_sha256=role.scope.scope_sha256,
            roster=prepared.source_receipt["candidate_roster"], source_members=prepared.source_receipt["admitted_universe_members"],
            inputs=[{"prediction_input": item.model_dump(mode="json"), "decision_features": features}
                for item, features in zip(prepared.inputs, prepared.decision_features, strict=True)],
            price_contexts=[{name: value.isoformat() if isinstance(value, date) else value for name, value in asdict(context).items()}
                if context is not None else None for context in prepared.price_contexts])
        publish_stage(study_root=self._formal_path(role, target), stage="prepared", plan_sha256=role.role_sha256,
            parent_sha256=loaded.manifest.manifest_sha256,
            artifacts={"daily_batch.json": _json_bytes(payload), "native_d_capsule.json": _json_bytes(capsule)})
        return {"status": payload["status"], "program_id": role.program_id, "target_date": target.isoformat(),
                "batch_sha256": canonical_json_sha256(payload), "candidate_count": len(payload["advice"])}

    def _loaded(self, bundle_id):
        self._validate_roots()
        if self._root is None or self._aligned_root is None:
            _fail("economic research consumer roots are not configured")
        manifest = self._root / "serving_bundles" / _bundle(bundle_id) / "preregistered/serving_manifest.json"
        if manifest.resolve() != manifest:
            _fail("economic research serving path is redirected")
        return self._loader(manifest_path=manifest, aligned_output_root=self._aligned_root)

    def _path(self, bundle_id, target):
        path = self._root / "research_daily" / _bundle(bundle_id) / _day(target).isoformat()
        if path.resolve() != path:
            _fail("economic daily artifact path is redirected")
        return path

    def _read(self, *, loaded, target, program_id):
        decision = _authorize_target(loaded, target)
        path = self._path(loaded.manifest.bundle_id, target) / "prepared"
        if not path.exists():
            return None
        _daily_stage_preflight(path, native=False)
        read_stage(path, stage="prepared", plan_sha256=loaded.manifest.manifest_sha256, parent_sha256=None)
        payload = _readonly_daily_batch(path / "daily_batch.json")
        if (payload.get("schema_version") != "economic_entry_daily_batch_v1" or payload.get("program_id") != program_id
                or payload.get("bundle_id") != loaded.manifest.bundle_id
                or payload.get("model_scope_sha256") != loaded.manifest.scope.scope_sha256
                or payload.get("model_bundle_sha256") != loaded.manifest.manifest_sha256
                or payload.get("binding_version_id") != json.loads(loaded.original_input_files["identity.json"])["binding_version_id"]
                or payload.get("decision_date") != decision.isoformat()
                or payload.get("target_date") != _day(target).isoformat()
                or payload.get("evidence_state") != "RESEARCH_NAVIGATION" or payload.get("decision_use") != "NAVIGATION_ONLY"
                or payload.get("deployable") is not False or not isinstance(payload.get("advice"), list) or len(payload["advice"]) > 20
                or payload.get("source_receipt", {}).get("outcomes_read") is not False
                or payload.get("source_receipt", {}).get("sealed_accessed") is not False):
            _fail("economic immutable daily batch identity or evidence differs")
        symbols, ranks = [], []
        for advice in payload["advice"]:
            source = EconomicEntryDailyInputV1.model_validate(advice["prediction_input"])
            project_economic_daily_advice_v1(advice)
            if (source.program_id != program_id or source.binding_version_id != payload["binding_version_id"]
                    or source.decision_date != decision or source.target_date != _day(target) or source.scope != loaded.manifest.scope
                    or source.source_evidence != "RECOVERED_LIMITED" or source.evidence_level != "HISTORICAL_REPLAY"
                    or source.run_id != payload.get("run_id") or source.list_id != payload.get("list_id")
                    or source.candidate_roster_sha256 != payload["candidate_roster_sha256"]
                    or source.restored_cohort_sha256 != payload["restored_cohort_sha256"] or source.captured_at.isoformat() != payload["captured_at"]
                    or advice["model_bundle_sha256"] != loaded.manifest.manifest_sha256 or advice["deployable"] is not False
                    or advice["decision_use"] != "NAVIGATION_ONLY" or len(advice.get("query_nodes", [])) > 5000):
                _fail("economic immutable advice belongs to another program, target or model")
            symbols.append(source.instrument)
            ranks.append(source.selection_rank)
        if len(set(symbols)) != len(symbols) or len(set(ranks)) != len(ranks) or ranks != sorted(ranks):
            _fail("economic immutable batch changed candidate uniqueness or order")
        return payload

    def read_research(self, *, program_id, bundle_id, target_date):
        loaded = self._loaded(bundle_id)
        original = json.loads(loaded.original_input_files["identity.json"])
        if original["program_id"] != program_id:
            _fail("economic research bundle belongs to a different original Program")
        payload = self._read(loaded=loaded, target=target_date, program_id=program_id)
        if payload is None:
            return {"status": "NOT_CAPTURED", "program_id": program_id, "bundle_id": bundle_id,
                    "target_date": _day(target_date).isoformat(), "deployable": False, "advice": []}
        projection = {key: value for key, value in payload.items() if key != "advice"}
        projection.update(schema_version="economic_entry_daily_batch_projection_v1", original_batch_sha256=canonical_json_sha256(payload),
            advice=[project_economic_daily_advice_v1(value) for value in payload["advice"]])
        return {**projection, "projection_sha256": canonical_json_sha256(projection)}

    def capture_frozen_research_day(self, *, bundle_id, decision_date, context_source):
        """Explicit developer workflow only; never invoked by public GET or production hook."""
        budget = EntryWorkBudget()
        loaded = self._loaded(bundle_id)
        budget.check()
        return self._capture_loaded(loaded, decision_date, context_source, budget=budget)

    def capture_native_research_day(self, *, bundle_id, target_date, projection, candidate_source_factory, frozen_list_id=None):
        """Explicit approved-window preview; never a public GET or live role hook."""
        budget = EntryWorkBudget()
        loaded = self._loaded(bundle_id)
        decision = _authorize_target(loaded, target_date)
        projection = EconomicEntryCandidateProjectionV1.model_validate(projection.model_dump())
        if projection.scope != loaded.manifest.scope:
            _fail("economic native research projection differs from the verified model scope")
        original = json.loads(loaded.original_input_files["identity.json"])
        existing = self._read(loaded=loaded, target=target_date, program_id=original["program_id"])
        if existing is not None:
            receipt = existing["source_receipt"].get("candidate_source", {})
            if (receipt.get("projection_sha256") != projection.projection_sha256
                    or (existing["advice"] and existing.get("run_id") is None)
                    or (frozen_list_id is not None and existing.get("list_id") != frozen_list_id)):
                _fail("economic immutable target already belongs to another source or projection")
            return {"status": "EXACT_RETRY", "target_date": _day(target_date).isoformat(),
                    "batch_sha256": canonical_json_sha256(existing), "new_inferences": 0, "new_input_reads": 0}
        budget.check()
        observed = candidate_source_factory(budget).load_day(program_id=original["program_id"],
            binding_version_id=original["binding_version_id"], target_date=_day(target_date),
            projection=projection, frozen_list_id=frozen_list_id)
        prepared = prepare_native_economic_research_day_v1(loaded=loaded, observed=observed, projection=projection)
        payload = _batch_payload(prepared, loaded, budget)
        payload.update(decision_date=decision.isoformat(), target_date=_day(target_date).isoformat(),
                       run_id=observed["selection_run_id"], list_id=observed["list_version_id"])
        budget.check()
        publish_stage(study_root=self._path(loaded.manifest.bundle_id, target_date), stage="prepared",
            plan_sha256=loaded.manifest.manifest_sha256, parent_sha256=None, artifacts={"daily_batch.json": _json_bytes(payload)})
        return {"status": payload["status"], "target_date": _day(target_date).isoformat(),
                "candidate_count": len(payload["advice"]), "batch_sha256": canonical_json_sha256(payload), "deployable": False}

    def capture_frozen_research_days(self, *, bundle_id, decision_dates, context_source_factory):
        """One verified model/input load, same day kernel; no per-day worktree or training."""
        dates = [_day(value) for value in decision_dates]
        if not dates or len(dates) > 405 or len(set(dates)) != len(dates) or dates != sorted(dates):
            _fail("economic historical batch requires a bounded, unique chronological plan")
        loading_budget = EntryWorkBudget()
        loaded = self._loaded(bundle_id)
        loading_budget.check()
        request = loaded.source_plan.training_request.source_request
        calendar = _frozen_calendar(loaded)
        if any(not request.test_start <= value <= request.test_end or value not in calendar or calendar.index(value) + 1 >= len(calendar) for value in dates):
            _fail("economic historical batch cannot consume a new or sealed window")
        results = []
        for value in dates:
            budget = EntryWorkBudget()
            context_source = context_source_factory(budget)
            try:
                results.append(self._capture_loaded(loaded, value, context_source, budget=budget))
            finally:
                # Factory owns a bounded input reader, not 81 idle connections.
                # Also close the zero-read source created for an exact retry.
                closer = getattr(context_source, "close", None)
                if callable(closer):
                    closer()
        return {"status": "RESEARCH_NAVIGATION", "results": results, "new_model_fits": 0, "deployable": False}

    def _capture_loaded(self, loaded, decision_date, context_source, *, budget=None):
        budget = budget or EntryWorkBudget()
        source = loaded.source_plan.training_request.source_request
        decision = _day(decision_date)
        if not source.test_start <= decision <= source.test_end:
            _fail("economic capture is outside its consumed development window")
        calendar = _frozen_calendar(loaded)
        if decision not in calendar or calendar.index(decision) + 1 >= len(calendar):
            _fail("economic daily calendar lacks the registered decision/target")
        target = calendar[calendar.index(decision) + 1]
        original = json.loads(loaded.original_input_files["identity.json"])
        existing = self._read(loaded=loaded, target=target, program_id=original["program_id"])
        if existing is not None:
            if "candidate_source" in existing["source_receipt"]:
                _fail("economic immutable target already belongs to native-source research")
            return {"status": "EXACT_RETRY", "target_date": target.isoformat(), "batch_sha256": canonical_json_sha256(existing),
                    "new_inferences": 0, "new_input_reads": 0}
        budget.check()
        prepared = prepare_frozen_economic_research_day_v1(loaded=loaded, decision_date=decision, context_source=context_source)
        payload = _batch_payload(prepared, loaded, budget)
        payload.update(decision_date=decision.isoformat(), target_date=target.isoformat())
        publish_stage(study_root=self._path(loaded.manifest.bundle_id, target), stage="prepared", plan_sha256=loaded.manifest.manifest_sha256,
                      parent_sha256=None, artifacts={"daily_batch.json": _json_bytes(payload)})
        return {"status": payload["status"], "target_date": target.isoformat(), "batch_sha256": canonical_json_sha256(payload),
                "candidate_count": len(payload["advice"]), "deployable": False}
