"""Read-only serving views of real aligned weights; never promote old evidence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
from pathlib import Path
from types import MappingProxyType
from collections.abc import Mapping
from typing import Literal
from statistics import NormalDist, stdev

from pydantic import Field, TypeAdapter, model_validator

from backend.services.advisory_model_first.economic_entry_aligned_pipeline import (
    _sources, load_aligned_study_v3, load_fitted_aligned_study_v3,
)
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _verify_reference, publish_stage, read_stage
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2, FrozenContract
from backend.services.advisory_model_first.economic_entry_daily_contracts import (
    EconomicEntryCandidateProjectionV1, economic_D_feature_semantics_v1,
)
from backend.services.advisory_model_first.economic_entry_contracts import SHA256
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryRiskBudgetV2
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class EconomicEntryServingManifestV1(FrozenContract):
    aligned_plan_ref: EvidenceReferenceV1
    aligned_trained_manifest_ref: EvidenceReferenceV1
    original_input_identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope: EconomicModelScopeV2
    source_evidence: Literal["NATIVE_COMPLETE", "RECOVERED_LIMITED"]
    evidence_limitations: tuple[str, ...]
    feature_semantics_qualification: Literal["FROZEN_COLUMN_CONTRACT_ONLY", "NATIVE_DAILY_CONTRACT_VERIFIED"]
    schema_version: Literal["economic_entry_serving_manifest_v1"] = "economic_entry_serving_manifest_v1"
    evidence_state: Literal["RESEARCH_NAVIGATION"] = "RESEARCH_NAVIGATION"
    deployable: Literal[False] = False

    @property
    def manifest_sha256(self):
        return canonical_json_sha256(self.model_dump(mode="json"))

    @property
    def bundle_id(self):
        return f"adveserve_{self.manifest_sha256[:24]}"


@dataclass(frozen=True)
class LoadedEconomicEntryServingBundleV1:
    manifest: EconomicEntryServingManifestV1
    fitted: object
    source_plan: object
    original_study_root: Path
    original_input_files: Mapping[str, bytes]
    model_available_at: datetime


class EconomicEntryNativeTrainingScopeV1(FrozenContract):
    """Must already belong to original prepared inputs; never retroactive repair."""
    schema_version: Literal["economic_entry_native_training_scope_v1"] = "economic_entry_native_training_scope_v1"
    input_identity_sha256: str = Field(pattern=SHA256)
    projection: EconomicEntryCandidateProjectionV1
    shared_builder_sha256: str = Field(pattern=SHA256)
    bar_policy_sha256: str = Field(pattern=SHA256)
    pit_universe_key: str = Field(min_length=1, pattern=r".*\S.*")
    pit_rule_version: str = Field(min_length=1, pattern=r".*\S.*")
    feature_semantic_review_ref: EvidenceReferenceV1
    latest_upstream_training_date: date

    @model_validator(mode="after")
    def require_native_semantics(self):
        if self.projection.scope.universe_definition_evidence != "NATIVE_VERIFIED":
            raise ValueError("qualified training must prove its original native universe definition")
        semantics = economic_D_feature_semantics_v1(component_roles=self.projection.component_roles.model_dump(),
            terminal_weights=self.projection.terminal_weights, shared_builder_sha256=self.shared_builder_sha256,
            bar_policy_sha256=self.bar_policy_sha256, pit_universe_key=self.pit_universe_key, pit_rule_version=self.pit_rule_version)
        if canonical_json_sha256(semantics) != self.projection.scope.feature_schema_sha256:
            raise ValueError("native training feature semantics differ from its actual scope")
        return self


class EconomicEntryConfirmationCriteriaV1(FrozenContract):
    """No imported opening-coverage gate and no result-dependent threshold changes."""
    minimum_decision_days: int = Field(ge=2, le=10000, strict=True)
    minimum_intervention_days: int = Field(ge=1, le=10000, strict=True)
    minimum_intervention_fraction: float = Field(gt=0, le=1, allow_inf_nan=False, strict=True)
    minimum_increment_bps: float = Field(ge=0, allow_inf_nan=False, strict=True)
    maximum_additional_drawdown_bps: float = Field(ge=0, lt=10000, allow_inf_nan=False, strict=True)
    block_days: int = Field(ge=1, le=100, strict=True)
    bootstrap_samples: int = Field(ge=1000, le=10000, strict=True)
    bootstrap_seed: int = Field(ge=0, le=2147483647, strict=True)

    @model_validator(mode="after")
    def check_power_and_block(self):
        if self.minimum_intervention_days > self.minimum_decision_days or self.block_days > self.minimum_decision_days:
            raise ValueError("confirmation intervention/block counts exceed its minimum day support")
        return self


class EconomicEntryConfirmationRequestV1(FrozenContract):
    schema_version: Literal["economic_entry_value_confirmation_request_v1"] = "economic_entry_value_confirmation_request_v1"
    experiment_id: str = Field(pattern=r"^adveconfirm_[0-9a-f]{24}$")
    study_type: Literal["CONFIRMATION"] = "CONFIRMATION"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["DIRECTION_GATE"] = "DIRECTION_GATE"
    model_content_sha256: str = Field(pattern=SHA256)
    native_training_scope_ref: EvidenceReferenceV1
    scope: EconomicModelScopeV2
    business_risk: EntryRiskBudgetV2
    criteria: EconomicEntryConfirmationCriteriaV1
    target_calendar: tuple[date, ...] = Field(min_length=2, max_length=10000)
    performance_calendar: tuple[date, ...] = Field(min_length=2, max_length=10040)
    dataset_identity: str = Field(min_length=1)
    policy_identity: str = Field(min_length=1)
    registered_at: datetime
    evidence_level: Literal["LOCKED_HISTORICAL_OOT", "NATURAL_FORWARD"]
    # The separately approved protocol includes frontier choice and power/MDE.
    approved_protocol_ref: EvidenceReferenceV1
    unknown_action_policy: Literal["UNAVAILABLE_EMPTY_SLOT_NOT_BASELINE_BUY"] = "UNAVAILABLE_EMPTY_SLOT_NOT_BASELINE_BUY"

    @model_validator(mode="after")
    def validate_request(self):
        if (self.scope.universe_definition_evidence != "NATIVE_VERIFIED"
                or self.native_training_scope_ref.role != "ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"
                or self.approved_protocol_ref.role != "ECONOMIC_ENTRY_VALUE_APPROVED_PROTOCOL"
                or self.business_risk.reference_use != "EXPLICIT_BUSINESS_CONFIGURATION"
                or self.business_risk.maximum_loss_bps != 800
                or self.registered_at.utcoffset() is None or tuple(sorted(set(self.target_calendar))) != self.target_calendar
                or tuple(sorted(set(self.performance_calendar))) != self.performance_calendar
                or self.performance_calendar[:len(self.target_calendar)] != self.target_calendar
                or len(self.target_calendar) < self.criteria.minimum_decision_days
                or (self.evidence_level == "NATURAL_FORWARD" and self.registered_at >=
                    datetime.combine(self.target_calendar[0], time(9, 30), ZoneInfo("Asia/Shanghai")))):
            raise ValueError("economic confirmation requires sufficient native scope, explicit risk and a legitimate evidence-level clock")
        expected = "adveconfirm_" + self.request_sha256[:24]
        if self.experiment_id != expected:
            raise ValueError("economic confirmation identity differs from frozen request")
        return self

    @property
    def request_sha256(self):
        return canonical_json_sha256(self.model_dump(mode="json", exclude={"experiment_id"}))


class EconomicEntryConfirmedServingManifestV1(FrozenContract):
    schema_version: Literal["economic_entry_confirmed_serving_manifest_v1"] = "economic_entry_confirmed_serving_manifest_v1"
    aligned_plan_ref: EvidenceReferenceV1
    aligned_trained_manifest_ref: EvidenceReferenceV1
    native_training_scope_ref: EvidenceReferenceV1
    confirmation_request_ref: EvidenceReferenceV1
    confirmation_manifest_ref: EvidenceReferenceV1
    scope: EconomicModelScopeV2
    evidence_state: Literal["CONFIRMED_ENTRY_VALUE"] = "CONFIRMED_ENTRY_VALUE"
    # Qualification alone cannot authorize capture, binding or a user's orders.
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def require_qualified_namespace(self):
        if (self.scope.universe_definition_evidence != "NATIVE_VERIFIED"
                or self.aligned_plan_ref.role != "aligned_plan" or self.aligned_trained_manifest_ref.role != "aligned_trained"
                or self.native_training_scope_ref.role != "ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"
                or self.confirmation_request_ref.role != "ECONOMIC_ENTRY_VALUE_CONFIRMATION_REQUEST"
                or self.confirmation_manifest_ref.role != "ECONOMIC_ENTRY_VALUE_CONFIRMATION_MANIFEST"):
            raise ValueError("qualified economic manifest requires native scope and distinct economic evidence roles")
        return self

    @property
    def manifest_sha256(self):
        return canonical_json_sha256(self.model_dump(mode="json"))

    @property
    def bundle_id(self):
        return "advecserve_" + self.manifest_sha256[:24]


def build_economic_entry_confirmation_request_v1(**values):
    fields = EconomicEntryConfirmationRequestV1.model_fields
    if set(values) - (set(fields) - {"experiment_id"}):
        _fail("economic confirmation request has unknown fields or a caller-chosen identity")
    functional = {}
    for name, field in fields.items():
        if name == "experiment_id":
            continue
        adapter = TypeAdapter(field.rebuild_annotation())
        value = values[name] if name in values else field.get_default(call_default_factory=True)
        functional[name] = adapter.dump_python(adapter.validate_python(value), mode="json")
    digest = canonical_json_sha256(functional)
    return EconomicEntryConfirmationRequestV1.model_validate({**functional, "experiment_id": "adveconfirm_" + digest[:24]})


@dataclass(frozen=True)
class LoadedEconomicEntryConfirmedBundleV1:
    manifest: EconomicEntryConfirmedServingManifestV1
    fitted: object
    source_plan: object
    native_training_scope: EconomicEntryNativeTrainingScopeV1
    confirmation_request: EconomicEntryConfirmationRequestV1
    confirmation_result: Mapping
    original_study_root: Path
    original_input_files: Mapping[str, bytes]


def economic_entry_confirmation_metrics_v1(*, request, matched_days):
    """Read frozen paired vectors; no price/model/source can be called here."""
    import numpy as np
    request = EconomicEntryConfirmationRequestV1.model_validate(request.model_dump())
    if not isinstance(matched_days, list) or len(matched_days) != len(request.performance_calendar):
        _fail("economic confirmation must preserve the full preregistered matched calendar")
    dates, baseline, model, interventions = [], [], [], []
    decision_dates = set(request.target_calendar)
    for row in matched_days:
        if not isinstance(row, dict) or set(row) != {"target_date", "baseline_net_return", "model_net_return", "intervened"}:
            _fail("economic confirmation vector contains foreign or missing fields")
        try:
            target = date.fromisoformat(row["target_date"])
        except (TypeError, ValueError):
            _fail("economic confirmation vector date malformed")
        values = row["baseline_net_return"], row["model_net_return"]
        if (type(row["intervened"]) is not bool or any(type(value) not in (float, int) or not math.isfinite(value)
                or not -1 < value <= 1 for value in values)):
            _fail("economic confirmation vector requires finite explicit net returns and actual interventions")
        dates.append(target)
        if row["intervened"] and target not in decision_dates:
            _fail("economic entry intervention cannot be assigned to a post-decision settlement-only day")
        baseline.append(float(values[0]))
        model.append(float(values[1]))
        interventions.append(row["intervened"])
    if tuple(dates) != request.performance_calendar:
        _fail("economic confirmation vector dropped, reordered or added dates")
    baseline, model = np.array(baseline), np.array(model)
    delta = (model - baseline) * 10000.
    rules = request.criteria
    random = np.random.default_rng(rules.bootstrap_seed)
    means = np.empty(rules.bootstrap_samples)
    blocks = int(np.ceil(len(delta) / rules.block_days))
    cumulative = np.r_[0., np.cumsum(delta)]
    full_block_sums = cumulative[rules.block_days:] - cumulative[:-rules.block_days]
    last_width = len(delta) - (blocks - 1) * rules.block_days
    for index in range(rules.bootstrap_samples):
        starts = random.integers(0, len(delta) - rules.block_days + 1, size=blocks)
        # Exactly the same block resampling, without N tiny slices per sample.
        tail = cumulative[starts[-1] + last_width] - cumulative[starts[-1]]
        means[index] = (full_block_sums[starts[:-1]].sum() + tail) / len(delta)
    ci = [float(value) for value in np.quantile(means, [.025, .975])]
    def performance(returns):
        log_path = np.r_[0., np.cumsum(np.log1p(returns))]
        drawdown = -np.expm1(log_path - np.maximum.accumulate(log_path))
        # Avoid overflow: the sign of cumulative return and MDD suffice here.
        return float(log_path[-1]), float(drawdown.max()) * 10000.
    baseline_log, baseline_mdd = performance(baseline)
    model_log, model_mdd = performance(model)
    count = sum(interventions)
    failures = []
    if count < rules.minimum_intervention_days or count / len(request.target_calendar) < rules.minimum_intervention_fraction:
        failures.append("INSUFFICIENT_ACTUAL_INTERVENTION_SUPPORT")
    if ci[0] <= rules.minimum_increment_bps:
        failures.append("NET_INCREMENT_CONFIDENCE_FLOOR_NOT_PASSED")
    if model_log <= 0:
        failures.append("MODEL_ABSOLUTE_NET_RETURN_NOT_POSITIVE")
    if model_mdd - baseline_mdd > rules.maximum_additional_drawdown_bps:
        failures.append("ADDITIONAL_DRAWDOWN_EXCEEDS_FROZEN_CONTRACT")
    return {"schema_version": "economic_entry_value_confirmation_metrics_v1", "request_sha256": request.request_sha256,
        "objective_contract": request.objective_contract, "decision_days": len(request.target_calendar), "portfolio_days": len(delta),
        "intervention_days": count, "intervention_fraction": count / len(request.target_calendar),
        "mean_increment_bps": float(delta.mean()), "ci95_increment_bps": ci,
        "baseline_log_net_return": baseline_log, "model_log_net_return": model_log,
        "baseline_max_drawdown_bps": baseline_mdd, "model_max_drawdown_bps": model_mdd,
        "matched_vector_sha256": canonical_json_sha256(matched_days), "failure_reasons": failures,
        "status": "ECONOMIC_GATES_PASSED" if not failures else "NOT_CONFIRMED",
        "interpretation": "PROVENANCE_REGISTRY_AND_NATIVE_SCOPE_REQUIRED_SEPARATELY"}


def _original_prepared_member(original, prepared_manifest, name, *, reference=None):
    """Only a member already bound before training, not an appended sidecar."""
    descriptor = prepared_manifest.get("files", {}).get(name)
    path = original / "prepared" / name
    if (not original.is_absolute() or original.resolve().drive.upper() == "C:" or not descriptor
            or path.resolve() != path.absolute() or not path.is_file()):
        _fail("native economic qualification is not an original prepared-stage member")
    if type(descriptor.get("size_bytes")) is not int or not 0 < descriptor["size_bytes"] <= 65536 or path.stat().st_size != descriptor["size_bytes"]:
        _fail("native economic qualification changed or exceeds its bounded metadata size")
    data = path.read_bytes()
    if len(data) > 65536 or len(data) != descriptor["size_bytes"] or hashlib.sha256(data).hexdigest() != descriptor["sha256"]:
        _fail("native economic qualification changed or exceeds its bounded metadata size")
    if reference is not None and evidence_reference_for_file(path, role=reference.role) != reference:
        _fail("native economic qualification reference differs from original prepared source")
    return _economic_metadata_json(data)


def _verified_native_training_scope_v1(*, original, prepared_manifest, identity, basis_scope, source_request, input_files):
    if "native_applicability.json" not in prepared_manifest.get("files", {}):
        return None
    if identity.source_evidence != "NATIVE_COMPLETE":
        _fail("a recovered economic identity cannot be upgraded by a native sidecar")
    proof = EconomicEntryNativeTrainingScopeV1.model_validate(
        _original_prepared_member(original, prepared_manifest, "native_applicability.json"))
    scope = proof.projection.scope
    for field in ("package_id", "manifest_sha256", "selection_runtime_semantics_hash", "feature_names",
                  "shadow_policy_sha256", "cost_policy_sha256", "coordinate_algorithm_sha256"):
        if getattr(scope, field) != getattr(basis_scope, field):
            _fail("native economic scope differs from actual model/source lineage")
    if (proof.input_identity_sha256 != identity.identity_sha256 or source_request.input_identity_sha256 != identity.identity_sha256
            or scope.universe_definition_sha256 != identity.universe_identity_sha256):
        _fail("native economic scope was not bound to this original training identity/universe")
    from backend.services.advisory_model_first import shared_feature_builder, suspension_aware_bar_policy
    if (proof.shared_builder_sha256 != hashlib.sha256(Path(shared_feature_builder.__file__).read_bytes()).hexdigest()
            or proof.bar_policy_sha256 != suspension_aware_bar_policy.BAR_POLICY_HASH):
        _fail("native economic daily feature implementation differs from frozen training")
    review = _original_prepared_member(original, prepared_manifest, "feature_semantic_review.json",
                                       reference=proof.feature_semantic_review_ref)
    if proof.feature_semantic_review_ref.role != "ECONOMIC_TRAINING_FEATURE_SEMANTICS":
        _fail("native economic semantic review has a different evidence role")
    import io
    import pandas as pd
    features = pd.read_parquet(io.BytesIO(input_files["features.parquet"]))
    frozen_request = _original_prepared_member(original, prepared_manifest, "frozen_request.json")
    if (frozen_request.get("baseline_policy_sha256") != proof.projection.review_policy_sha256
            or frozen_request.get("selection_runtime_semantics_hash") != scope.selection_runtime_semantics_hash
            or frozen_request.get("terminal_weights") != proof.projection.terminal_weights):
        _fail("native economic projection review/runtime policy differs from original frozen request")
    clocks = pd.to_datetime(features["decision_as_of_trade_date"], errors="coerce")
    visible = pd.to_datetime(features["feature_visible_through"], errors="coerce")
    if (clocks.isna().any() or visible.isna().any() or (visible > clocks).any()
            or (clocks.dt.date <= proof.latest_upstream_training_date).any()
            or not features["feature_source_sha256"].eq(source_request.feature_source_sha256).all()):
        _fail("native economic training contains future/foreign features or upstream in-sample scores")
    expected = {"schema_version": "economic_entry_feature_semantic_review_v1",
        "input_identity_sha256": identity.identity_sha256, "feature_source_sha256": source_request.feature_source_sha256,
        "features_file_sha256": prepared_manifest["files"]["features.parquet"]["sha256"],
        "feature_schema_sha256": scope.feature_schema_sha256, "compared_candidate_rows": len(features),
        "compared_D_columns": list(scope.feature_names[:-1]), "mismatched_candidate_rows": 0,
        "missing_values_preserved": True, "training_temporal_parity": "EXACT_DAILY_CONTRACT"}
    if (review != expected or not len(features)
            or type(review.get("compared_candidate_rows")) is not int
            or type(review.get("mismatched_candidate_rows")) is not int
            or review.get("missing_values_preserved") is not True):
        _fail("native economic training feature parity does not cover its full original candidate cohort")
    return proof


def _verified_source_view(plan_path, aligned_output_root):
    plan, root, registered = load_aligned_study_v3(plan_path=plan_path, output_root=aligned_output_root)
    read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"])
    fitted = load_fitted_aligned_study_v3(plan_path=plan_path, output_root=aligned_output_root)
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1
    ledger = root.parent / "trial_registry.jsonl"
    if ledger.resolve() != ledger.absolute() or not ledger.is_file() or ledger.stat().st_size > 16777216:
        _fail("economic model availability ledger is missing, redirected or exceeds its budget")
    records = [row for row in AdvisoryResearchTrialRegistryV1(ledger).read()
        if row.experiment_id == plan.experiment_id and row.research_stage == "TRAINED"]
    trained_ref = evidence_reference_for_file(root / "trained/manifest.json", role="aligned_trained")
    if (len(records) != 1 or records[0].evidence_refs != (trained_ref,)
            or records[0].recorded_at.utcoffset() is None):
        _fail("economic model availability is not bound to its actual trained ledger entry")
    available_at = records[0].recorded_at
    v2, _, original, _, _, _, _, _ = _sources(plan.v2_plan_ref, plan.v2_prepared_manifest_ref)
    prepared_manifest = json.loads(_verify_reference(v2.parent_prepared_manifest_ref).read_text(encoding="utf-8"))
    inputs = {}
    for name in ("identity.json", "calendar.json", "frozen_rankings.parquet", "features.parquet"):
        path = original / "prepared" / name
        if path.resolve() != path:
            _fail("economic frozen consumer source is redirected")
        content = path.read_bytes()
        descriptor = prepared_manifest["files"][name]
        if len(content) != descriptor["size_bytes"] or hashlib.sha256(content).hexdigest() != descriptor["sha256"]:
            _fail("economic frozen consumer source changed during serving verification")
        inputs[name] = content
    identity = EconomicEntryInputIdentityV1.model_validate_json(inputs["identity.json"])
    # This names the column contract only, not proof of original feature vintage
    # or a native pool. Formal serving cannot be inferred from matching columns.
    schema = canonical_json_sha256({"schema_version": "economic_feature_column_contract_v1",
                                    "names": list(plan.training_request.source_request.feature_names)})
    coordinate = canonical_json_sha256({"raw_price_basis": "raw_cny", "target_reference": "D_visible_provider_compatible_v2",
                                        "research_training_coordinate": "constant_per_symbol_times_db_adj_factor_all_frozen_endpoints"})
    scope = EconomicModelScopeV2(package_id=identity.package_id, manifest_sha256=identity.manifest_sha256,
        selection_runtime_semantics_hash=identity.selection_runtime_semantics_hash, feature_schema_sha256=schema,
        shadow_policy_sha256=identity.shadow_policy_sha256, cost_policy_sha256=identity.cost_policy_sha256,
        coordinate_algorithm_sha256=coordinate, universe_definition_evidence="UNPROVEN")
    native = _verified_native_training_scope_v1(original=original, prepared_manifest=prepared_manifest, identity=identity,
        basis_scope=scope, source_request=plan.training_request.source_request, input_files=inputs)
    if native is not None:
        scope = native.projection.scope
    limitations = (*identity.evidence_limitations, "TRAINING_UNIVERSE_DEFINITION_UNPROVEN",
                  "FEATURE_COLUMN_CONTRACT_NOT_NATIVE_SEMANTIC_QUALIFICATION") if native is None else identity.evidence_limitations
    manifest = EconomicEntryServingManifestV1(
        aligned_plan_ref=evidence_reference_for_file(Path(plan_path), role="aligned_plan"),
        aligned_trained_manifest_ref=evidence_reference_for_file(root / "trained/manifest.json", role="aligned_trained"),
        original_input_identity_sha256=identity.identity_sha256, scope=scope, source_evidence=identity.source_evidence,
        evidence_limitations=limitations,
        feature_semantics_qualification="FROZEN_COLUMN_CONTRACT_ONLY" if native is None else "NATIVE_DAILY_CONTRACT_VERIFIED",
    )
    return LoadedEconomicEntryServingBundleV1(manifest, fitted, plan, original, MappingProxyType(inputs), available_at)


def publish_research_serving_bundle_v1(*, plan_path, aligned_output_root, serving_root):
    loaded = _verified_source_view(plan_path, aligned_output_root)
    root = Path(serving_root)
    if not root.is_absolute() or root.resolve().drive.upper() == "C:" or root.resolve() != root.absolute():
        _fail("economic serving root must be an explicit non-C durable path without redirection")
    manifest = loaded.manifest
    published = publish_stage(study_root=root / manifest.bundle_id, stage="preregistered",
        plan_sha256=manifest.manifest_sha256, parent_sha256=None,
        artifacts={"serving_manifest.json": _json_bytes(manifest.model_dump(mode="json"))})
    return published / "serving_manifest.json"


def load_research_serving_bundle_v1(*, manifest_path, aligned_output_root):
    path = Path(manifest_path)
    reference = _economic_metadata_file_reference(path, role="ECONOMIC_ENTRY_RESEARCH_SERVING_MANIFEST")
    manifest = EconomicEntryServingManifestV1.model_validate(_read_economic_metadata_reference(reference,
        role="ECONOMIC_ENTRY_RESEARCH_SERVING_MANIFEST"))
    if path.parent.name != "preregistered" or path.parent.parent.name != manifest.bundle_id:
        _fail("economic serving manifest is not its immutable bundle path")
    read_stage(path.parent, stage="preregistered", plan_sha256=manifest.manifest_sha256, parent_sha256=None)
    loaded = _verified_source_view(manifest.aligned_plan_ref.artifact_uri, aligned_output_root)
    if loaded.manifest != manifest:
        _fail("economic serving view differs from actual source/model scope or evidence")
    return loaded


def _economic_metadata_file_reference(path, *, role, maximum_bytes=65536):
    path = Path(path)
    if (not path.is_absolute() or path.resolve() != path.absolute() or path.drive.upper() == "C:"
            or not path.is_file() or not 0 < path.stat().st_size <= maximum_bytes):
        _fail("economic qualification metadata path/size must be checked before hashing")
    return evidence_reference_for_file(path, role=role)


def _read_economic_metadata_reference(reference, *, role, maximum_bytes=65536):
    reference = EvidenceReferenceV1.model_validate(reference)
    path = Path(reference.artifact_uri)
    if (reference.role != role or not path.is_absolute() or path.resolve() != path.absolute()
            or path.resolve().drive.upper() == "C:" or reference.size_bytes > maximum_bytes):
        _fail("economic qualification metadata role/path/size is invalid")
    # Preflight before read_bytes: a small declared ref must not cause a huge read.
    if not path.is_file() or path.stat().st_size != reference.size_bytes:
        _fail("economic qualification metadata differs from its immutable reference")
    data = path.read_bytes()
    if len(data) != reference.size_bytes or hashlib.sha256(data).hexdigest() != reference.sha256:
        _fail("economic qualification metadata differs from its immutable reference")
    return _economic_metadata_json(data)


def _economic_metadata_json(data):
    def unique_pairs(pairs):
        values = {}
        for name, value in pairs:
            if name in values:
                _fail("economic qualification metadata contains duplicate JSON keys")
            values[name] = value
        return values
    value = json.loads(data, object_pairs_hook=unique_pairs,
        parse_constant=lambda value: _fail("economic qualification metadata is nonfinite"))
    if not isinstance(value, dict):
        _fail("economic qualification metadata must be a JSON object")
    return value


def _read_economic_native_prediction_day_v1(*, reference, request, native, expected_date, prediction):
    """Read the exact frozen input, not a caller's NATIVE_COMPLETE assertion.

    No DB lookup or capture occurs here. The producer's immutable payload binds
    the real run/list, roster, members, D features and price context references.
    This is necessary provenance, not independent economic confirmation.
    """
    from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1
    from backend.services.advisory_model_first.economic_entry_daily_inference import daily_decision_feature_values_v1
    value = _read_economic_metadata_reference(reference, role="ECONOMIC_ENTRY_NATIVE_PREDICTION_DAY", maximum_bytes=1048576)
    required = {"schema_version", "model_content_sha256", "scope_sha256", "program_id", "binding_version_id", "run_id", "list_id",
                "decision_date", "target_date", "captured_at", "candidate_roster_sha256", "roster", "source_members",
                "source_receipt", "inputs", "price_contexts", "original_D_capsule_ref"}
    if (set(value) != required or value.get("schema_version") != "economic_entry_native_prediction_day_v1"
            or value.get("model_content_sha256") != request.model_content_sha256
            or value.get("scope_sha256") != request.scope.scope_sha256 or value.get("target_date") != expected_date.isoformat()
            or any(value.get(name) != prediction.get(name) for name in ("decision_date", "target_date", "captured_at"))
            or any(not isinstance(value.get(name), str) or not value[name].strip()
                for name in ("program_id", "binding_version_id", "run_id", "list_id"))):
        _fail("economic confirmation native input has different model, clock, run/list or scope")
    receipt, members, roster, inputs = (value[name] for name in ("source_receipt", "source_members", "roster", "inputs"))
    candidate_receipt = receipt.get("candidate_source", receipt) if isinstance(receipt, dict) else {}
    if (not isinstance(receipt, dict) or receipt.get("outcomes_read") is not False
            or receipt.get("new_selection_runs") != 0 or type(receipt.get("new_selection_runs")) is not int
            or candidate_receipt.get("pit_universe_key") != native.pit_universe_key or candidate_receipt.get("pit_rule_version") != native.pit_rule_version
            or candidate_receipt.get("projection_sha256") != native.projection.projection_sha256
            or not isinstance(members, list) or not members
            or any(not isinstance(symbol, str) or re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", symbol) is None for symbol in members)
            or members != sorted(set(members))
            or not isinstance(roster, list) or not isinstance(inputs, list) or len(roster) != len(inputs) or len(inputs) > 20
            or not isinstance(value.get("price_contexts"), list) or len(value["price_contexts"]) != len(inputs)
            or canonical_json_sha256(roster) != value.get("candidate_roster_sha256")):
        _fail("economic confirmation native input lacks frozen PIT, roster or complete members")
    archive = _read_economic_metadata_reference(candidate_receipt.get("original_archive_reference"), role="SELECTION_EXECUTION_INPUTS", maximum_bytes=33554432)
    try:
        archive_clock = datetime.fromisoformat(archive["observed_at"])
        capture_clock = datetime.fromisoformat(value["captured_at"])
    except (KeyError, TypeError, ValueError):
        _fail("economic confirmation native archive has no real aware capture clock")
    if (archive.get("schema_version") != "advisory_selection_input_archive_v1"
            or archive.get("historical_capture_backfilled") is not False
            or archive_clock.utcoffset() is None or capture_clock.utcoffset() is None or archive_clock > capture_clock
            or archive.get("selection_run_id") != value["run_id"] or archive.get("program_id") != value["program_id"]
            or archive.get("binding_version_id") != value["binding_version_id"]
            or archive.get("decision_as_of_trade_date") != value["decision_date"] or archive.get("target_trade_date") != value["target_date"]
            or archive.get("review_policy_sha256") != native.projection.review_policy_sha256
            or archive.get("manifest_sha256_by_package") != {request.scope.package_id: request.scope.manifest_sha256}
            or not isinstance(archive.get("full_source_universe_members"), list)
            or any(not isinstance(symbol, str) for symbol in archive["full_source_universe_members"])
            or archive["full_source_universe_members"] != sorted(set(archive["full_source_universe_members"]))
            or set(members) - set(archive["full_source_universe_members"])
            or canonical_json_sha256(archive["full_source_universe_members"]) != archive.get("source_universe_members_sha256")
            or candidate_receipt.get("source_universe_members_sha256") != archive.get("source_universe_members_sha256")):
        _fail("economic confirmation native input differs from its original Selection archive")
    D = date.fromisoformat(value["decision_date"])
    if (D >= expected_date or not datetime.combine(D, time(15), ZoneInfo("Asia/Shanghai")) <= capture_clock
            < datetime.combine(expected_date, time(9, 30), ZoneInfo("Asia/Shanghai"))):
        _fail("economic confirmation original native D input was not actually captured before T")
    packages = archive.get("package_inputs")
    if not isinstance(packages, dict) or set(packages) != {request.scope.package_id}:
        _fail("economic confirmation original package artifact partition differs")
    item = packages[request.scope.package_id]
    artifact = item.get("artifact") if isinstance(item, dict) else None
    context = (artifact.get("metadata") or {}).get("artifact_input_context") if isinstance(artifact, dict) else None
    if (not isinstance(artifact, dict) or not isinstance(context, dict)
            or canonical_json_sha256(artifact) != item.get("artifact_content_sha256")
            or artifact.get("package_id") != request.scope.package_id or artifact.get("manifest_sha256") != request.scope.manifest_sha256
            or artifact.get("trade_date") != value["target_date"] or artifact.get("status") != "SUCCEEDED"
            or artifact.get("universe_count") != len(archive["full_source_universe_members"])
            or context.get("cutoff_date") != value["decision_date"] or context.get("score_trade_date") != value["decision_date"]
            or context.get("requested_trade_date") != value["target_date"]
            or context.get("universe_input_hash") != archive["source_universe_members_sha256"]
            or canonical_json_sha256(context) != artifact.get("artifact_input_context_hash")):
        _fail("economic confirmation original package artifact, D-1 or content hash differs")
    feature_receipt = receipt.get("D_source")
    if (not isinstance(feature_receipt, dict) or feature_receipt.get("outcomes_read") is not False
            or (inputs and (feature_receipt.get("decision_date") != value["decision_date"] or feature_receipt.get("target_date") != value["target_date"]
            or feature_receipt.get("shared_builder_sha256") != native.shared_builder_sha256
            or feature_receipt.get("bar_policy_sha256") != native.bar_policy_sha256
            or feature_receipt.get("pit_universe_key") != native.pit_universe_key
            or feature_receipt.get("raw_price_unit_divisor") != 1000.
            or {name: feature_receipt.get("source_window_contract", {}).get(name) for name in
                ("candidate_sessions", "benchmark_sessions", "breadth_sessions")}
                != {"candidate_sessions": 20, "benchmark_sessions": 20, "breadth_sessions": 2}))
            or (not inputs and (feature_receipt.get("status") != "NO_CANDIDATES"
                or type(feature_receipt.get("query_count")) is not int or feature_receipt["query_count"] != 0))):
        _fail("economic confirmation native feature semantics differ from original qualified training")
    if request.evidence_level == "LOCKED_HISTORICAL_OOT":
        if value.get("original_D_capsule_ref") is None:
            _fail("historical confirmation requires an original immutable D capsule; recovered input cannot be upgraded")
        capsule = _read_economic_metadata_reference(value.get("original_D_capsule_ref"),
            role="ECONOMIC_ENTRY_NATIVE_D_CAPSULE", maximum_bytes=4194304)
        expected_capsule = {name: value[name] for name in required - {"schema_version", "model_content_sha256", "original_D_capsule_ref"}}
        expected_capsule["schema_version"] = "economic_entry_native_D_capsule_v1"
        if canonical_json_sha256(capsule) != canonical_json_sha256(expected_capsule):
            _fail("historical confirmation must consume unchanged original D features and native capture, not reconstructed input")
    elif value.get("original_D_capsule_ref") is not None:
        _fail("natural confirmation cannot borrow a historical D capsule")
    member_sha = canonical_json_sha256(members)
    feature_source_sha = canonical_json_sha256({"D_source": feature_receipt, "candidate_source": candidate_receipt}) if "candidate_source" in receipt else canonical_json_sha256(receipt)
    symbols, ranks = [], []
    for frozen_row, record, context in zip(roster, inputs, value["price_contexts"], strict=True):
        if (not isinstance(record, dict) or set(record) != {"prediction_input", "decision_features"}
                or not isinstance(frozen_row, dict) or set(frozen_row) != {"instrument", "selection_rank"}):
            _fail("economic confirmation native input row schema differs")
        source = EconomicEntryDailyInputV1.model_validate(record["prediction_input"])
        features = daily_decision_feature_values_v1(record["decision_features"], request.scope.feature_names)
        if (source.scope != request.scope or source.source_evidence != "NATIVE_COMPLETE" or source.evidence_level != "PROSPECTIVE_INPUT"
                or source.restored_cohort_sha256 is not None
                or any(getattr(source, name) != value[name] for name in ("program_id", "binding_version_id", "run_id", "list_id"))
                or source.decision_date.isoformat() != value["decision_date"] or source.target_date != expected_date
                or source.captured_at.isoformat() != value["captured_at"]
                or (request.evidence_level == "NATURAL_FORWARD" and source.captured_at < request.registered_at)
                or source.universe_membership_sha256 != member_sha or source.instrument not in members
                or source.candidate_roster_sha256 != value["candidate_roster_sha256"]
                or source.feature_source_sha256 != feature_source_sha
                or source.feature_values_sha256 != canonical_json_sha256(features)
                or (context is not None and not isinstance(context, dict))
                or source.price_context_sha256 != (canonical_json_sha256(context) if context is not None else None)
                or frozen_row != {"instrument": source.instrument, "selection_rank": source.selection_rank}):
            _fail("economic confirmation native input content, membership, feature or candidate identity differs")
        symbols.append(source.instrument)
        ranks.append(source.selection_rank)
    if len(set(symbols)) != len(symbols) or len(set(ranks)) != len(ranks) or ranks != sorted(ranks):
        _fail("economic confirmation native input changed original candidate uniqueness or order")
    return value


def _economic_confirmation_candidate_id(*, request):
    return "adve_candidate_" + canonical_json_sha256({
        "model": request.model_content_sha256, "scope": request.scope.scope_sha256,
        "policy": request.policy_identity, "risk": request.business_risk.model_dump(mode="json"),
        "criteria": request.criteria.model_dump(mode="json"), "unknown_action": request.unknown_action_policy})


def _read_economic_confirmation_window_v1(*, protocol, request, source, native, approved_at):
    """Consume existing N0 metadata; NEVER authorize/write a sealed access receipt."""
    from backend.services.advisory_model_first.research_control_contracts import (
        AdvisoryResearchWindowContractV1, ResearchWindowAccessRequestV1, SealedHoldoutConsumptionReceiptV1,
    )
    values = {}
    for name, model, role in (
            ("window_contract_ref", AdvisoryResearchWindowContractV1, "ECONOMIC_ENTRY_CONFIRMATION_WINDOW_CONTRACT"),
            ("window_access_request_ref", ResearchWindowAccessRequestV1, "ECONOMIC_ENTRY_CONFIRMATION_WINDOW_ACCESS"),
            ("window_consumption_receipt_ref", SealedHoldoutConsumptionReceiptV1, "ECONOMIC_ENTRY_CONFIRMATION_WINDOW_CONSUMPTION")):
        if name not in protocol:
            _fail("economic confirmation lacks its existing canonical window authorization")
        reference = EvidenceReferenceV1.model_validate(protocol[name])
        values[name] = model.model_validate(_read_economic_metadata_reference(reference, role=role))
    contract, access, receipt = (values[name] for name in
        ("window_contract_ref", "window_access_request_ref", "window_consumption_receipt_ref"))
    sealed = next(window for window in contract.windows if window.state == "SEALED_UNCONSUMED")
    frontier = source.source_plan.experiment_id
    available_at = getattr(source, "model_available_at", None)
    if (contract.package_id != request.scope.package_id or contract.manifest_sha256 != request.scope.manifest_sha256
            or contract.runtime_semantics_hash != request.scope.selection_runtime_semantics_hash
            or contract.baseline_policy_sha256 != native.projection.review_policy_sha256
            or contract.shadow_policy_sha256 != request.scope.shadow_policy_sha256
            or contract.cost_policy_sha256 != request.scope.cost_policy_sha256
            or access.contract_sha256 != contract.contract_sha256 or access.study_type != "CONFIRMATION"
            or access.objective_contract != request.objective_contract or access.decision_use != "DIRECTION_GATE"
            or access.dataset_identity != request.dataset_identity or access.policy_identity != request.policy_identity
            or (access.start_date, access.end_date) != (request.target_calendar[0], request.performance_calendar[-1])
            or (sealed.start_date, sealed.end_date, sealed.dataset_identity) != (access.start_date, access.end_date, access.dataset_identity)
            or access.frontier_id != frontier or access.candidate_id != _economic_confirmation_candidate_id(request=request)
            or receipt.contract_sha256 != contract.contract_sha256 or receipt.request_sha256 != access.request_sha256
            or receipt.window_id != sealed.window_id or receipt.dataset_identity != access.dataset_identity
            or receipt.objective_contract != access.objective_contract or receipt.policy_identity != access.policy_identity
            or receipt.frontier_id != access.frontier_id or receipt.candidate_id != access.candidate_id
            or Path(protocol["window_consumption_receipt_ref"]["artifact_uri"]) != Path(contract.sealed_consumption_receipt_uri)
            or contract.created_at.utcoffset() is None or receipt.consumed_at.utcoffset() is None
            or not isinstance(available_at, datetime) or available_at.utcoffset() is None or available_at > approved_at
            or not contract.created_at <= approved_at <= receipt.consumed_at <= request.registered_at):
        _fail("economic confirmation canonical window, candidate, policy or consumption clock differs")
    return contract, access, receipt


def _read_economic_confirmation_power_v1(*, protocol, request, contract, source, approved_at):
    power = protocol.get("power_analysis")
    if (not isinstance(power, dict) or power.get("method") != "DEVELOPMENT_BLOCK_NORMAL_APPROX_V1"
            or power.get("alpha") != .05 or type(power.get("target_power")) not in (int, float)
            or not .8 <= power["target_power"] < 1 or type(power.get("anticipated_increment_bps")) not in (int, float)
            or not math.isfinite(power["anticipated_increment_bps"])
            or not request.criteria.minimum_increment_bps < power["anticipated_increment_bps"] <= 20000):
        _fail("economic confirmation lacks its fixed quantitative power analysis")
    reference = EvidenceReferenceV1.model_validate(power.get("development_measurement_ref", {}))
    measure = _read_economic_metadata_reference(reference, role="ECONOMIC_ENTRY_VALUE_DEVELOPMENT_BLOCKS")
    try:
        calendar = tuple(date.fromisoformat(value) for value in measure["calendar"])
        measured_at = datetime.fromisoformat(measure["recorded_at"])
    except (KeyError, TypeError, ValueError):
        _fail("economic power measurement calendar or real clock malformed")
    blocks = measure.get("block_means_bps")
    if (measure.get("schema_version") != "economic_entry_value_development_blocks_v1"
            or measure.get("model_content_sha256") != request.model_content_sha256
            or measure.get("scope_sha256") != request.scope.scope_sha256 or measure.get("policy_identity") != request.policy_identity
            or type(measure.get("block_days")) is not int or measure["block_days"] != request.criteria.block_days
            or not isinstance(blocks, list) or not 3 <= len(blocks) <= 2000
            or any(type(value) not in (int, float) or not math.isfinite(value) or abs(value) > 20000 for value in blocks)
            or len(calendar) != len(blocks) * request.criteria.block_days or tuple(sorted(set(calendar))) != calendar
            or calendar[-1] > source.fitted.request.source_request.label_cutoff
            or measured_at.utcoffset() is None or not contract.created_at <= measured_at <= approved_at
            or not any(window.state != "SEALED_UNCONSUMED" and window.dataset_identity == measure.get("dataset_identity")
                and window.start_date <= calendar[0] <= calendar[-1] <= window.end_date for window in contract.windows)):
        _fail("economic power measurement is not its full approved development block series")
    sigma = stdev(blocks)
    if sigma <= 0 or not math.isfinite(sigma):
        _fail("economic power cannot claim confirmation from unexplained zero block variance")
    factor = NormalDist().inv_cdf(.975) + NormalDist().inv_cdf(power["target_power"])
    margin = power["anticipated_increment_bps"] - request.criteria.minimum_increment_bps
    required = max(request.criteria.minimum_decision_days, math.ceil((factor * sigma / margin) ** 2) * request.criteria.block_days)
    count = len(request.performance_calendar) // request.criteria.block_days
    mde = request.criteria.minimum_increment_bps + factor * sigma / math.sqrt(count)
    if (type(protocol.get("minimum_required_portfolio_days")) is not int or protocol["minimum_required_portfolio_days"] != required
            or type(power.get("mde_increment_bps")) not in (float, int) or not math.isfinite(power["mde_increment_bps"])
            or not math.isclose(power["mde_increment_bps"], mde, rel_tol=1e-9, abs_tol=1e-9)
            or len(request.performance_calendar) < required or power["anticipated_increment_bps"] < mde):
        _fail("economic confirmation is underpowered or its required days/MDE were changed")


def _read_economic_confirmation_v1(*, request_reference, manifest_reference, source, native):
    """Approved evidence consumption only. This function never launches a study."""
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, research_policy_identity
    request = EconomicEntryConfirmationRequestV1.model_validate(_read_economic_metadata_reference(
        request_reference, role="ECONOMIC_ENTRY_VALUE_CONFIRMATION_REQUEST"))
    root = Path(request_reference.artifact_uri).parent
    if root.name != request.experiment_id or Path(request_reference.artifact_uri).name != "request.json":
        _fail("economic confirmation request does not occupy its immutable experiment path")
    model_sha = canonical_json_sha256({"aligned_plan_sha256": source.manifest.aligned_plan_ref.sha256,
                                      "aligned_trained_manifest_sha256": source.manifest.aligned_trained_manifest_ref.sha256})
    native_reference = evidence_reference_for_file(source.original_study_root / "prepared/native_applicability.json",
                                                   role="ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE")
    if (request.model_content_sha256 != model_sha or request.scope != native.projection.scope
            or request.native_training_scope_ref != native_reference
            or request.target_calendar[0] <= max(source.fitted.request.source_request.label_cutoff, native.latest_upstream_training_date)
            or request.policy_identity != research_policy_identity(baseline_policy_sha256=native.projection.review_policy_sha256,
                shadow_policy_sha256=request.scope.shadow_policy_sha256, cost_policy_sha256=request.scope.cost_policy_sha256)):
        _fail("economic confirmation model/scope/policy or independent-window identity differs")
    protocol = _read_economic_metadata_reference(request.approved_protocol_ref, role="ECONOMIC_ENTRY_VALUE_APPROVED_PROTOCOL")
    try:
        approved_at = datetime.fromisoformat(protocol["approved_at"])
    except (KeyError, TypeError, ValueError):
        _fail("economic confirmation approved protocol lacks its real aware clock")
    if (protocol.get("schema_version") != "economic_entry_value_confirmation_protocol_v1"
            or protocol.get("model_content_sha256") != model_sha or protocol.get("scope_sha256") != request.scope.scope_sha256
            or protocol.get("criteria_sha256") != canonical_json_sha256(request.criteria.model_dump(mode="json"))
            or protocol.get("business_risk_sha256") != canonical_json_sha256(request.business_risk.model_dump(mode="json"))
            or protocol.get("calendar_sha256") != canonical_json_sha256({"decision": [d.isoformat() for d in request.target_calendar],
                "performance": [d.isoformat() for d in request.performance_calendar]})
            or protocol.get("power_status") != "CONFIRMATORY" or type(protocol.get("minimum_required_portfolio_days")) is not int
            or not 2 <= protocol["minimum_required_portfolio_days"] <= len(request.performance_calendar)
            or not isinstance(protocol.get("authorization_ref"), str) or not protocol["authorization_ref"].strip()
            or approved_at.utcoffset() is None or approved_at > request.registered_at):
        _fail("economic confirmation lacks its fixed approved power/frontier/risk protocol")
    window_contract, window_access, consumption = _read_economic_confirmation_window_v1(
        protocol=protocol, request=request, source=source, native=native, approved_at=approved_at)
    _read_economic_confirmation_power_v1(protocol=protocol, request=request, contract=window_contract, source=source, approved_at=approved_at)
    manifest = _read_economic_metadata_reference(manifest_reference, role="ECONOMIC_ENTRY_VALUE_CONFIRMATION_MANIFEST")
    if Path(manifest_reference.artifact_uri) != root / "manifest.json":
        _fail("economic confirmation manifest belongs to a different experiment")
    functional = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    expected_names = {"request.json", "prediction.json", "settlement.json", "evaluation.json"}
    if (manifest.get("schema_version") != "economic_entry_value_confirmation_manifest_v1"
            or manifest.get("producer_version") != "economic_entry_value_confirmation_v1"
            or manifest.get("request_sha256") != request.request_sha256
            or canonical_json_sha256(functional) != manifest.get("manifest_sha256")
            or set(manifest.get("files", {})) != expected_names):
        _fail("economic confirmation producer/hash chain is incomplete")
    if EvidenceReferenceV1.model_validate(manifest["files"]["request.json"]) != request_reference:
        _fail("economic confirmation request was changed after preregistration")
    registry_path = root.parent / "trial_registry.jsonl"
    if registry_path.resolve() != registry_path.absolute() or not registry_path.is_file() or registry_path.stat().st_size > 16777216:
        _fail("economic confirmation registry is missing, redirected or exceeds its metadata budget")
    all_records = AdvisoryResearchTrialRegistryV1(registry_path).read()
    if any(row.study_type == "CONFIRMATION" and row.experiment_id != request.experiment_id
            and window_access.frontier_id in row.parent_lineage for row in all_records):
        _fail("economic confirmation frontier already belongs to another experiment; only exact retry is allowed")
    records = [row for row in all_records if row.experiment_id == request.experiment_id]
    verified_records = []
    for stage, result_class, reference in (("PREREGISTERED", "CONTROL_READY", request_reference),
                                           ("EVALUATED", "CONFIRMED", manifest_reference)):
        matches = [row for row in records if row.research_stage == stage]
        if (len(matches) != 1 or matches[0].study_type != "CONFIRMATION" or matches[0].objective_contract != request.objective_contract
                or matches[0].decision_use != "DIRECTION_GATE" or matches[0].result_class != result_class
                or matches[0].dataset_identity != request.dataset_identity or matches[0].policy_identity != request.policy_identity
                or reference not in matches[0].evidence_refs or matches[0].planned_trial_count != 1
                or EvidenceReferenceV1.model_validate(protocol["window_consumption_receipt_ref"]) not in matches[0].evidence_refs
                or window_access.frontier_id not in matches[0].parent_lineage
                or matches[0].schema_identity != request.scope.feature_schema_sha256
                or matches[0].recorded_at.utcoffset() is None
                or (matches[0].generated_trial_count, matches[0].evaluated_trial_count, matches[0].selected_trial_count)
                    != ((0, 0, 0) if stage == "PREREGISTERED" else (1, 1, 1))
                or len(matches[0].consumed_windows) != 1
                or matches[0].consumed_windows[0].window_id != consumption.window_id
                or matches[0].consumed_windows[0].dataset_identity != request.dataset_identity
                or matches[0].consumed_windows[0].start_date != request.target_calendar[0]
                or matches[0].consumed_windows[0].end_date != request.performance_calendar[-1]):
            _fail("economic confirmation differs from actual preregistered/confirmed registry evidence")
        verified_records.append(matches[0])
    preregistered, evaluated = verified_records
    if (preregistered.attempt_id != evaluated.attempt_id or preregistered.parent_lineage != evaluated.parent_lineage
            or preregistered.hypothesis_family_id != evaluated.hypothesis_family_id
            or preregistered.unique_variable != evaluated.unique_variable
            or preregistered.consumed_windows != evaluated.consumed_windows
            or preregistered.recorded_at > request.registered_at or evaluated.recorded_at < request.registered_at
            or evaluated.recorded_at.date() < request.performance_calendar[-1]):
        _fail("economic confirmation registry changed lineage, attempt, window or real clock")
    parent, stages = request.request_sha256, {}
    phase_clock = request.registered_at
    for name, phase in (("prediction.json", "PREDICTED"), ("settlement.json", "SETTLED"), ("evaluation.json", "EVALUATED")):
        reference = EvidenceReferenceV1.model_validate(manifest["files"][name])
        if Path(reference.artifact_uri) != root / name:
            _fail("economic confirmation phase references a different experiment")
        payload = _read_economic_metadata_reference(reference, role=f"ECONOMIC_ENTRY_VALUE_{phase}", maximum_bytes=4194304)
        functional = {key: value for key, value in payload.items() if key != "stage_sha256"}
        if (payload.get("stage") != phase or payload.get("request_sha256") != request.request_sha256
                or payload.get("parent_sha256") != parent or canonical_json_sha256(functional) != payload.get("stage_sha256")):
            _fail("economic confirmation phase identity/parent/hash differs")
        try:
            recorded_at = datetime.fromisoformat(payload["recorded_at"])
        except (KeyError, TypeError, ValueError):
            _fail("economic confirmation phase lacks its actual aware clock")
        if recorded_at.utcoffset() is None or recorded_at < phase_clock or recorded_at > evaluated.recorded_at:
            _fail("economic confirmation phase clock differs from preregistration or evaluation")
        phase_clock = recorded_at
        stages[phase], parent = payload, payload["stage_sha256"]
    predictions = stages["PREDICTED"].get("days")
    if not isinstance(predictions, list) or len(predictions) != len(request.target_calendar):
        _fail("economic confirmation omitted native frozen prediction days")
    for expected_date, row in zip(request.target_calendar, predictions, strict=True):
        try:
            D = date.fromisoformat(row["decision_date"])
            captured = datetime.fromisoformat(row["captured_at"])
            predicted = datetime.fromisoformat(row["predicted_at"])
        except (KeyError, TypeError, ValueError):
            _fail("economic confirmation prediction clock unavailable")
        if (row.get("target_date") != expected_date.isoformat() or row.get("scope_sha256") != request.scope.scope_sha256
                or row.get("model_content_sha256") != model_sha or row.get("source_evidence") != "NATIVE_COMPLETE"
                or captured.utcoffset() is None or predicted.utcoffset() is None or D >= expected_date
                or not request.registered_at <= predicted <= datetime.fromisoformat(stages["PREDICTED"]["recorded_at"])
                or captured > predicted
                or (request.evidence_level == "NATURAL_FORWARD" and (captured < request.registered_at
                    or predicted >= datetime.combine(expected_date, time(9, 30), ZoneInfo("Asia/Shanghai"))))
                or not datetime.combine(D, time(15), ZoneInfo("Asia/Shanghai")) <= captured
                    < datetime.combine(expected_date, time(9, 30), ZoneInfo("Asia/Shanghai"))):
            _fail("economic confirmation prediction was not native and actually frozen before T")
        native_day_ref = row.get("native_input_ref")
        if native_day_ref is None:
            _fail("economic confirmation prediction omitted its immutable native input")
        _read_economic_native_prediction_day_v1(reference=native_day_ref, request=request, native=native,
            expected_date=expected_date, prediction=row)
    settlement = stages["SETTLED"]
    try:
        outcomes_started = datetime.fromisoformat(settlement["outcomes_first_read_at"])
    except (KeyError, TypeError, ValueError):
        _fail("economic confirmation must record actual first outcome access separately from prediction")
    if (outcomes_started.utcoffset() is None or not datetime.fromisoformat(stages["PREDICTED"]["recorded_at"]) < outcomes_started
            <= datetime.fromisoformat(settlement["recorded_at"])):
        _fail("economic confirmation outcomes were accessed before immutable predictions were frozen")
    if (settlement.get("scope_sha256") != request.scope.scope_sha256 or settlement.get("policy_identity") != request.policy_identity
            or settlement.get("unknown_action_policy") != request.unknown_action_policy
            or settlement.get("simulator_sha256") != source.source_plan.simulator_sha256):
        _fail("economic confirmation settlement policy, unknown controls or simulator differs")
    result = economic_entry_confirmation_metrics_v1(request=request, matched_days=settlement.get("matched_days"))
    if result != stages["EVALUATED"].get("metrics") or result["status"] != "ECONOMIC_GATES_PASSED":
        _fail("economic confirmation does not pass recomputed fixed economic gates")
    return request, result


def load_confirmed_economic_entry_serving_bundle_v1(*, manifest_path, aligned_output_root):
    path = Path(manifest_path)
    reference = _economic_metadata_file_reference(path, role="ECONOMIC_ENTRY_VALUE_SERVING_MANIFEST")
    manifest = EconomicEntryConfirmedServingManifestV1.model_validate(_read_economic_metadata_reference(
        reference, role="ECONOMIC_ENTRY_VALUE_SERVING_MANIFEST"))
    if path.parent.name != "preregistered" or path.parent.parent.name != manifest.bundle_id:
        _fail("qualified economic serving manifest has a different immutable bundle path")
    read_stage(path.parent, stage="preregistered", plan_sha256=manifest.manifest_sha256, parent_sha256=None)
    source = _verified_source_view(manifest.aligned_plan_ref.artifact_uri, aligned_output_root)
    if (source.manifest.aligned_plan_ref != manifest.aligned_plan_ref
            or source.manifest.aligned_trained_manifest_ref != manifest.aligned_trained_manifest_ref
            or source.manifest.source_evidence != "NATIVE_COMPLETE"
            or source.manifest.feature_semantics_qualification != "NATIVE_DAILY_CONTRACT_VERIFIED"
            or source.manifest.scope != manifest.scope):
        _fail("qualified economic bundle lacks actual native training semantics or model/scope identity")
    native_path = source.original_study_root / "prepared/native_applicability.json"
    if evidence_reference_for_file(native_path, role="ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE") != manifest.native_training_scope_ref:
        _fail("qualified economic native scope is not the original pre-training source")
    native = EconomicEntryNativeTrainingScopeV1.model_validate(_read_economic_metadata_reference(
        manifest.native_training_scope_ref, role="ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"))
    request, result = _read_economic_confirmation_v1(request_reference=manifest.confirmation_request_ref,
        manifest_reference=manifest.confirmation_manifest_ref, source=source, native=native)
    if native.projection.scope != manifest.scope or request.scope != manifest.scope:
        _fail("qualified economic scope changed during confirmation consumption")
    return LoadedEconomicEntryConfirmedBundleV1(manifest, source.fitted, source.source_plan, native, request, MappingProxyType(result),
        source.original_study_root, source.original_input_files)


def publish_confirmed_economic_entry_serving_bundle_v1(*, plan_path, aligned_output_root, serving_root,
                                                      confirmation_request_path, confirmation_manifest_path):
    """Publish a view of ALREADY confirmed native weights. Never confirm/activate.

    Verifies all evidence before creating a directory; old recovered weights
    are rejected before reading any confirmation window or writing a view.
    """
    destination = Path(serving_root)
    if (not destination.is_absolute() or destination.resolve() != destination.absolute()
            or destination.drive.upper() == "C:"):
        _fail("qualified economic serving root must be explicit, non-C and nonredirected")
    source = _verified_source_view(plan_path, aligned_output_root)
    if (source.manifest.source_evidence != "NATIVE_COMPLETE"
            or source.manifest.feature_semantics_qualification != "NATIVE_DAILY_CONTRACT_VERIFIED"):
        _fail("unconfirmed recovered weights cannot be promoted to qualified serving")
    native_ref = evidence_reference_for_file(source.original_study_root / "prepared/native_applicability.json",
                                             role="ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE")
    native = EconomicEntryNativeTrainingScopeV1.model_validate(_read_economic_metadata_reference(
        native_ref, role="ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"))
    request_ref = evidence_reference_for_file(confirmation_request_path, role="ECONOMIC_ENTRY_VALUE_CONFIRMATION_REQUEST")
    confirmation_ref = evidence_reference_for_file(confirmation_manifest_path, role="ECONOMIC_ENTRY_VALUE_CONFIRMATION_MANIFEST")
    request, _ = _read_economic_confirmation_v1(request_reference=request_ref, manifest_reference=confirmation_ref,
                                               source=source, native=native)
    if native.projection.scope != source.manifest.scope or request.scope != source.manifest.scope:
        _fail("qualified economic evidence differs from its actual model scope")
    manifest = EconomicEntryConfirmedServingManifestV1(aligned_plan_ref=source.manifest.aligned_plan_ref,
        aligned_trained_manifest_ref=source.manifest.aligned_trained_manifest_ref, native_training_scope_ref=native_ref,
        confirmation_request_ref=request_ref, confirmation_manifest_ref=confirmation_ref, scope=request.scope)
    target = destination / "serving_bundles" / manifest.bundle_id
    if target.resolve() != target.absolute():
        _fail("qualified economic immutable publication path is redirected")
    publish_stage(study_root=target, stage="preregistered", plan_sha256=manifest.manifest_sha256, parent_sha256=None,
        artifacts={"serving_manifest.json": _json_bytes(manifest.model_dump(mode="json"))})
    return target / "preregistered/serving_manifest.json"
