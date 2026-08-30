from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Iterator, Sequence
from uuid import uuid4

from pydantic import ValidationError

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource, sha256_file
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryParentPredictionExtensionReceiptV1,
    AdvisoryResearchTrialRecordV1,
    AdvisoryResearchWindowContractV1,
    AdvisoryResearchWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    N0CompletionReceiptV1,
    ObjectiveContract,
    ParentLegEvidenceV1,
    ParentPredictionExtensionStatus,
    PostCutoffInferenceEvidenceV1,
    ResearchResultClass,
    ResearchStudyType,
    ResearchWindowAccessRequestV1,
    ResearchWindowState,
    SealedHoldoutConsumptionReceiptV1,
    build_holdout_consumption_receipt,
    build_n0_completion_receipt,
    build_parent_extension_receipt,
    build_trial_record,
    build_window_contract,
)
from backend.services.advisory_model_first.target_binding import (
    EXPECTED_RUNTIME_SEMANTICS_HASH,
    LEG_IDS,
    MANIFEST_SHA256,
    PACKAGE_ID,
    REPRESENTATIVE_MODEL_ASSET_SHA256,
    REPRESENTATIVE_SEED_RUN_IDS,
    RUNTIME_SEMANTICS_ID,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


BASELINE_POLICY_SHA256 = "cd48c76688db9f53d7544e3a21dfbf4da0c69161dba5b8ce3a1c8d3908d43507"
SHADOW_POLICY_SHA256 = "8bc008c97b45d5851ed9df03bc91248f31f2d822c580f3bc37e8bab75c255898"
COST_POLICY_SHA256 = "fbef59ed802af567c72f64687289ad2addb2d19085b22a1c83746a527edbc000"
SPLIT_POLICY_SHA256 = "d4b16009a7d16c2d0154744e066fabd981e69ab402782dfbba99ff4d863c00ee"
P0C_DATASET_IDENTITY = "81e2c9bac5ce1f8e2fdc5a6174bc948dfbe984cf5028726c89ea72eb59fc69bd"
P0C_POLICY_IDENTITY = "b5e2d0bb96cf49e2a10ecc0cd056bd0f7aaa4c2a78a0f22289a038f67bcaed2f"
FEATURE_SCHEMA_IDENTITY = "advisory_feature_schema_v2_suspension_aware"
P0_FAMILY_EXPERIMENT_IDS = tuple(f"ADVISORY-P0-{letter}" for letter in "DEFGHIJKL")
INITIAL_SEED_EXPERIMENT_IDS = (
    *P0_FAMILY_EXPERIMENT_IDS,
    "ADVISORY-M1-M5-80D-TEST",
    "ADVISORY-P0-D-HISTORICAL-REPLAY",
    "ADVISORY-P0-E-HISTORICAL-REPLAY",
    "ADVISORY-H0-V6-GOLDEN",
)
N0_EXPERIMENT_ID = "ADVISORY-N0-RESEARCH-CONTROL-20260830"
N1_ORACLE_EXPERIMENT_ID = "ADVISORY-N1-TIER1-ORACLE"
N1_LEARNABILITY_EXPERIMENT_ID = "ADVISORY-N1-TIER1-LEARNABILITY"
N1_EXPERIMENT_IDS = (N1_ORACLE_EXPERIMENT_ID, N1_LEARNABILITY_EXPERIMENT_ID)

if P0C_POLICY_IDENTITY != canonical_json_sha256(
    {
        "baseline_policy_sha256": BASELINE_POLICY_SHA256,
        "cost_policy_sha256": COST_POLICY_SHA256,
        "shadow_policy_sha256": SHADOW_POLICY_SHA256,
        "split_policy_sha256": SPLIT_POLICY_SHA256,
    }
):
    raise RuntimeError("P0-C policy-set identity constant drift")


def research_policy_identity(
    *,
    baseline_policy_sha256: str = BASELINE_POLICY_SHA256,
    shadow_policy_sha256: str = SHADOW_POLICY_SHA256,
    cost_policy_sha256: str = COST_POLICY_SHA256,
) -> str:
    return canonical_json_sha256(
        {
            "baseline_policy_sha256": baseline_policy_sha256,
            "cost_policy_sha256": cost_policy_sha256,
            "shadow_policy_sha256": shadow_policy_sha256,
        }
    )


class AdvisoryResearchTrialRegistryV1:
    """Strict, locked, append-only JSONL research registry."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    def read(self) -> tuple[AdvisoryResearchTrialRecordV1, ...]:
        return self._read_unlocked()

    def append_batch(
        self, records: Sequence[AdvisoryResearchTrialRecordV1]
    ) -> dict[str, Any]:
        candidates = tuple(
            item
            if isinstance(item, AdvisoryResearchTrialRecordV1)
            else AdvisoryResearchTrialRecordV1.model_validate(item)
            for item in records
        )
        self._validate_record_set(candidates)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with _exclusive_file_lock(self.lock_path):
            existing = self._read_unlocked()
            existing_by_id = {item.registry_entry_id: item for item in existing}
            combined = list(existing)
            pending: list[AdvisoryResearchTrialRecordV1] = []
            for candidate in candidates:
                prior = existing_by_id.get(candidate.registry_entry_id)
                if prior is not None:
                    if prior.record_sha256 != candidate.record_sha256:
                        _raise(
                            "registry entry id conflicts with different content",
                            "ADVISORY_RESEARCH_REGISTRY_CONFLICT",
                            registry_entry_id=candidate.registry_entry_id,
                        )
                    continue
                pending.append(candidate)
                existing_by_id[candidate.registry_entry_id] = candidate
                combined.append(candidate)
            self._validate_record_set(combined)
            if pending:
                # ALGO-COMPLEXITY-001: one append request is an already validated,
                # bounded registry batch (the N0 bootstrap is exactly 13 rows); this
                # is O(batch bytes), performs no market-data join, and writes once.
                payload = "".join(
                    json.dumps(
                        item.model_dump(mode="json"),
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                    for item in pending
                ).encode("utf-8")
                with self.path.open("ab") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
        return {
            "status": "ok",
            "registry_path": self.path.as_posix(),
            "requested_count": len(candidates),
            "appended_count": len(pending),
            "duplicate_noop_count": len(candidates) - len(pending),
            "total_count": len(combined),
            "registry_sha256": sha256_file(self.path) if self.path.exists() else None,
        }

    def _read_unlocked(self) -> tuple[AdvisoryResearchTrialRecordV1, ...]:
        if not self.path.exists():
            return ()
        try:
            raw = self.path.read_bytes()
        except OSError as exc:
            _raise_registry_invalid("registry cannot be read", error_type=type(exc).__name__)
        if raw and not raw.endswith(b"\n"):
            _raise_registry_invalid("registry has a truncated final line")
        records: list[AdvisoryResearchTrialRecordV1] = []
        for line_number, raw_line in enumerate(raw.splitlines(), start=1):
            if not raw_line.strip():
                _raise_registry_invalid("registry contains a blank line", line_number=line_number)
            try:
                payload = json.loads(raw_line.decode("utf-8"))
                records.append(AdvisoryResearchTrialRecordV1.model_validate(payload))
            except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
                _raise_registry_invalid(
                    "registry line is invalid",
                    line_number=line_number,
                    error_type=type(exc).__name__,
                )
        self._validate_record_set(records)
        return tuple(records)

    @staticmethod
    def _validate_record_set(records: Sequence[AdvisoryResearchTrialRecordV1]) -> None:
        by_entry: dict[str, str] = {}
        by_attempt_stage: dict[tuple[str, str, str], str] = {}
        experiment_identity: dict[str, tuple[Any, ...]] = {}
        evidence_by_uri: dict[str, tuple[str, int]] = {}
        navigation_only_evidence_uris = {
            ref.artifact_uri
            for item in records
            if item.decision_use == DecisionUse.NAVIGATION_ONLY
            for ref in item.evidence_refs
        }
        for item in records:
            if item.registry_entry_id in by_entry:
                _raise_registry_invalid(
                    "registry contains a duplicate/conflicting entry id",
                    registry_entry_id=item.registry_entry_id,
                )
            by_entry[item.registry_entry_id] = item.record_sha256
            attempt_stage = (item.experiment_id, item.attempt_id, item.research_stage)
            previous_attempt_record = by_attempt_stage.setdefault(
                attempt_stage, item.registry_entry_id
            )
            if previous_attempt_record != item.registry_entry_id:
                _raise(
                    "one experiment attempt/stage cannot have multiple registry records",
                    "ADVISORY_RESEARCH_REGISTRY_CONFLICT",
                    experiment_id=item.experiment_id,
                    attempt_id=item.attempt_id,
                    research_stage=item.research_stage,
                )
            identity = (
                item.hypothesis_family_id,
                item.parent_lineage,
                item.unique_variable,
                item.objective_contract.value,
                item.dataset_identity,
                item.schema_identity,
                item.policy_identity,
            )
            previous_identity = experiment_identity.setdefault(item.experiment_id, identity)
            if previous_identity != identity:
                _raise(
                    "experiment identity drift detected",
                    "ADVISORY_RESEARCH_REGISTRY_CONFLICT",
                    experiment_id=item.experiment_id,
                )
            for evidence in item.evidence_refs:
                evidence_identity = (evidence.sha256, evidence.size_bytes)
                previous_evidence = evidence_by_uri.setdefault(
                    evidence.artifact_uri, evidence_identity
                )
                if previous_evidence != evidence_identity:
                    _raise(
                        "one evidence URI cannot drift to different content",
                        "ADVISORY_RESEARCH_REGISTRY_CONFLICT",
                        artifact_uri=evidence.artifact_uri,
                    )
                if (
                    item.study_type == ResearchStudyType.ACTIVATION
                    and evidence.artifact_uri in navigation_only_evidence_uris
                ):
                    _raise(
                        "navigation-only evidence cannot be reused as activation evidence",
                        "ADVISORY_RESEARCH_REGISTRY_CONFLICT",
                        artifact_uri=evidence.artifact_uri,
                    )


def resolve_evidence_reference(
    *, artifact_root: str | Path, relative_path: str, role: str
) -> EvidenceReferenceV1:
    root = Path(artifact_root).resolve()
    candidate = Path(relative_path)
    if candidate.is_absolute():
        _raise(
            "seed evidence path must be relative to the explicit artifact root",
            "ADVISORY_RESEARCH_EVIDENCE_MISSING",
            relative_path=relative_path,
        )
    target = (root / candidate).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        _raise(
            "seed evidence path escapes the explicit artifact root",
            "ADVISORY_RESEARCH_EVIDENCE_MISSING",
            relative_path=relative_path,
        )
    return evidence_reference_for_file(target, role=role)


def evidence_reference_for_file(path: str | Path, *, role: str) -> EvidenceReferenceV1:
    target = Path(path)
    if not target.is_file():
        _raise(
            "evidence file is missing",
            "ADVISORY_RESEARCH_EVIDENCE_MISSING",
            artifact_uri=target.as_posix(),
            role=role,
        )
    try:
        return EvidenceReferenceV1(
            role=role,
            artifact_uri=target.resolve().as_posix(),
            sha256=sha256_file(target),
            size_bytes=target.stat().st_size,
        )
    except OSError as exc:
        _raise(
            "evidence file cannot be hashed",
            "ADVISORY_RESEARCH_EVIDENCE_MISSING",
            artifact_uri=target.as_posix(),
            error_type=type(exc).__name__,
        )


def bootstrap_registry_from_seed(
    *,
    seed_path: str | Path,
    artifact_root: str | Path,
    registry_path: str | Path,
) -> dict[str, Any]:
    seed = _read_json(Path(seed_path), reason_code="ADVISORY_RESEARCH_REGISTRY_INVALID")
    if seed.get("schema_version") != "advisory_research_control_seed_v1":
        _raise_registry_invalid("research seed schema_version is invalid")
    raw_records = seed.get("records")
    if not isinstance(raw_records, list) or not raw_records:
        _raise_registry_invalid("research seed must contain records")
    records: list[AdvisoryResearchTrialRecordV1] = []
    for index, raw in enumerate(raw_records):
        if not isinstance(raw, dict):
            _raise_registry_invalid("research seed record is not an object", record_index=index)
        item = dict(raw)
        evidence_path = str(item.pop("evidence_path", ""))
        evidence_role = str(item.pop("evidence_role", "experiment_manifest"))
        if not evidence_path:
            _raise_registry_invalid("research seed record has no evidence_path", record_index=index)
        item["evidence_refs"] = (
            resolve_evidence_reference(
                artifact_root=artifact_root,
                relative_path=evidence_path,
                role=evidence_role,
            ),
        )
        try:
            records.append(build_trial_record(**item))
        except (ValidationError, ValueError, KeyError) as exc:
            _raise_registry_invalid(
                "research seed record violates the registry contract",
                record_index=index,
                error_type=type(exc).__name__,
            )
    seeded_ids = [item.experiment_id for item in records]
    missing = sorted(set(INITIAL_SEED_EXPERIMENT_IDS) - set(seeded_ids))
    extra = sorted(set(seeded_ids) - set(INITIAL_SEED_EXPERIMENT_IDS))
    if missing or extra or len(seeded_ids) != len(INITIAL_SEED_EXPERIMENT_IDS):
        _raise_registry_invalid(
            "research seed roster differs from the frozen N0 backfill",
            missing=missing,
            extra=extra,
            actual_count=len(seeded_ids),
        )
    p0_records = [item for item in records if item.experiment_id in P0_FAMILY_EXPERIMENT_IDS]
    if any(
        item.dataset_identity != P0C_DATASET_IDENTITY
        or item.schema_identity != FEATURE_SCHEMA_IDENTITY
        or item.policy_identity != P0C_POLICY_IDENTITY
        or item.objective_contract != ObjectiveContract.ALPHA_RANKING
        or item.decision_use != DecisionUse.NAVIGATION_ONLY
        for item in p0_records
    ) or any(item.decision_use != DecisionUse.NAVIGATION_ONLY for item in records):
        _raise_registry_invalid(
            "research seed identity or navigation-only boundary has drifted"
        )
    summary = AdvisoryResearchTrialRegistryV1(registry_path).append_batch(records)
    summary["seed_path"] = Path(seed_path).resolve().as_posix()
    summary["artifact_root"] = Path(artifact_root).resolve().as_posix()
    return summary


def inspect_parent_prediction_extension(
    *,
    prediction_store_root: str | Path,
    runtime_asset_root: str | Path,
    post_cutoff_evidence_path: str | Path,
    comparison_state_path: str | Path,
    target_extension_start: str | date,
    target_extension_end: str | date,
    output_path: str | Path | None = None,
    retrain_receipt_path: str | Path | None = None,
) -> AdvisoryParentPredictionExtensionReceiptV1:
    started = time.perf_counter()
    source = ExactPredictionSource(prediction_store_root)
    try:
        descriptors = {
            leg_id: source.describe(REPRESENTATIVE_SEED_RUN_IDS[leg_id]) for leg_id in LEG_IDS
        }
    except AdvisoryModelFirstError as exc:
        _raise(
            "representative Prediction Store evidence is unavailable",
            "ADVISORY_RESEARCH_EVIDENCE_MISSING",
            source_reason_code=exc.reason_code,
        )
    legs = tuple(
        _inspect_parent_leg(
            leg_id=leg_id,
            descriptor=descriptors[leg_id],
            runtime_asset_root=Path(runtime_asset_root),
        )
        for leg_id in LEG_IDS
    )
    common_cutoff = min(item.prediction_date_end for item in legs)
    capability_gaps = tuple(
        sorted(
            gap
            for leg in legs
            for gap in (
                (f"{leg.leg_id}:{item}" for item in leg.missing_runtime_assets)
                if not leg.runtime_ready
                else ()
            )
        )
    )
    post_cutoff, post_gap = _inspect_post_cutoff_evidence(
        evidence_path=Path(post_cutoff_evidence_path),
        comparison_state_path=Path(comparison_state_path),
        common_cutoff=common_cutoff,
    )
    if post_gap:
        capability_gaps = tuple(sorted((*capability_gaps, post_gap)))

    explicit_retrain_ref: EvidenceReferenceV1 | None = None
    if retrain_receipt_path is not None:
        explicit_retrain_ref = _validate_retrain_receipt(
            Path(retrain_receipt_path),
            target_start=_as_date(target_extension_start),
            target_end=_as_date(target_extension_end),
        )
        status = ParentPredictionExtensionStatus.RETRAIN_NEW_LINEAGE_REQUIRED
    elif not capability_gaps and post_cutoff is not None:
        status = ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER
    else:
        status = ParentPredictionExtensionStatus.HISTORICAL_PREDICTION_ONLY

    receipt = build_parent_extension_receipt(
        status=status,
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA256,
        runtime_semantics_id=RUNTIME_SEMANTICS_ID,
        runtime_semantics_hash=EXPECTED_RUNTIME_SEMANTICS_HASH,
        common_historical_prediction_cutoff=common_cutoff,
        target_extension_start=_as_date(target_extension_start),
        target_extension_end=_as_date(target_extension_end),
        legs=legs,
        post_cutoff_evidence=post_cutoff,
        explicit_retrain_ref=explicit_retrain_ref,
        capability_gaps=capability_gaps,
        scan_duration_seconds=round(time.perf_counter() - started, 6),
    )
    if output_path is not None:
        return _write_immutable_model(
            Path(output_path),
            receipt,
            model_type=AdvisoryParentPredictionExtensionReceiptV1,
            hash_field="receipt_sha256",
            conflict_reason="ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID",
        )
    return receipt


def _inspect_parent_leg(*, leg_id: str, descriptor: Any, runtime_asset_root: Path) -> ParentLegEvidenceV1:
    if str(descriptor.run_id) != REPRESENTATIVE_SEED_RUN_IDS[leg_id]:
        _raise(
            "prediction descriptor run identity does not match the representative seed",
            "ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH",
            leg_id=leg_id,
            expected_run_id=REPRESENTATIVE_SEED_RUN_IDS[leg_id],
            actual_run_id=str(descriptor.run_id),
        )
    prediction_ref = EvidenceReferenceV1(
        role=f"{leg_id}:representative_prediction",
        artifact_uri=str(descriptor.artifact_uri),
        sha256=str(descriptor.artifact_sha256),
        size_bytes=int(descriptor.size_bytes),
    )
    matches = sorted(
        runtime_asset_root.glob(f"{MANIFEST_SHA256[:16]}__leg_{leg_id}"), key=lambda item: item.name
    )
    if len(matches) > 1:
        _raise(
            "multiple runtime asset roots match one representative leg",
            "ADVISORY_PARENT_RUNTIME_ASSET_INVALID",
            leg_id=leg_id,
            matches=[item.as_posix() for item in matches],
        )
    if not matches:
        return ParentLegEvidenceV1(
            leg_id=leg_id,
            representative_run_id=REPRESENTATIVE_SEED_RUN_IDS[leg_id],
            prediction_ref=prediction_ref,
            prediction_row_count=int(descriptor.row_count),
            prediction_date_start=_as_date(descriptor.date_start),
            prediction_date_end=_as_date(descriptor.date_end),
            runtime_asset_root=runtime_asset_root.resolve().as_posix(),
            runtime_ready=False,
            runtime_refs=(),
            missing_runtime_assets=("runtime_asset_root",),
        )
    leg_root = matches[0]
    manifest_path = leg_root / "manifest.json"
    if not manifest_path.is_file():
        return _unready_leg(
            leg_id=leg_id,
            descriptor=descriptor,
            prediction_ref=prediction_ref,
            leg_root=leg_root,
            missing=("manifest.json",),
        )
    manifest = _read_json(manifest_path, reason_code="ADVISORY_PARENT_RUNTIME_ASSET_INVALID")
    diagnostics = manifest.get("diagnostics") or {}
    if (
        manifest.get("schema_version") != 1
        or manifest.get("task_id") != PACKAGE_ID
        or manifest.get("source") != "live_qe_model_inference_v1"
        or diagnostics.get("package_id") != PACKAGE_ID
        or diagnostics.get("package_manifest_sha256") != MANIFEST_SHA256
    ):
        _raise(
            "runtime asset manifest identity does not match the target package",
            "ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH",
            leg_id=leg_id,
        )
    assets = manifest.get("assets") or {}
    required = {
        "model_weight": assets.get("model_weight"),
        "factor_entry": assets.get("factor_entry"),
        "factor_order": assets.get("factor_order"),
    }
    missing = tuple(
        name for name, relative in required.items() if not relative or not (leg_root / relative).is_file()
    )
    if missing:
        return _unready_leg(
            leg_id=leg_id,
            descriptor=descriptor,
            prediction_ref=prediction_ref,
            leg_root=leg_root,
            missing=missing,
        )
    model_path = leg_root / str(required["model_weight"])
    model_sha = sha256_file(model_path)
    if model_sha != REPRESENTATIVE_MODEL_ASSET_SHA256[leg_id]:
        _raise(
            "runtime model weight hash does not match target binding",
            "ADVISORY_PARENT_RUNTIME_ASSET_INVALID",
            leg_id=leg_id,
            expected_sha256=REPRESENTATIVE_MODEL_ASSET_SHA256[leg_id],
            actual_sha256=model_sha,
        )
    factor_order_path = leg_root / str(required["factor_order"])
    factor_order = _read_json(
        factor_order_path, reason_code="ADVISORY_PARENT_RUNTIME_ASSET_INVALID"
    )
    factor_names = factor_order.get("factor_order")
    if (
        factor_order.get("package_id") != PACKAGE_ID
        or factor_order.get("source") != "live_qe_model_inference_v1"
        or not isinstance(factor_names, list)
        or not factor_names
        or int(factor_order.get("total_factors", -1)) != len(factor_names)
        or int(assets.get("factors_count", -1)) != len(factor_names)
        or factor_order.get("is_aligned") is not True
    ):
        _raise(
            "runtime factor order is incomplete or not aligned",
            "ADVISORY_PARENT_RUNTIME_ASSET_INVALID",
            leg_id=leg_id,
        )
    refs = (
        evidence_reference_for_file(manifest_path, role=f"{leg_id}:runtime_manifest"),
        evidence_reference_for_file(model_path, role=f"{leg_id}:model_weight"),
        evidence_reference_for_file(
            leg_root / str(required["factor_entry"]), role=f"{leg_id}:factor_entry"
        ),
        evidence_reference_for_file(factor_order_path, role=f"{leg_id}:factor_order"),
    )
    return ParentLegEvidenceV1(
        leg_id=leg_id,
        representative_run_id=REPRESENTATIVE_SEED_RUN_IDS[leg_id],
        prediction_ref=prediction_ref,
        prediction_row_count=int(descriptor.row_count),
        prediction_date_start=_as_date(descriptor.date_start),
        prediction_date_end=_as_date(descriptor.date_end),
        runtime_asset_root=leg_root.resolve().as_posix(),
        runtime_ready=True,
        runtime_refs=refs,
        missing_runtime_assets=(),
    )


def _unready_leg(
    *, leg_id: str, descriptor: Any, prediction_ref: EvidenceReferenceV1, leg_root: Path, missing: tuple[str, ...]
) -> ParentLegEvidenceV1:
    return ParentLegEvidenceV1(
        leg_id=leg_id,
        representative_run_id=REPRESENTATIVE_SEED_RUN_IDS[leg_id],
        prediction_ref=prediction_ref,
        prediction_row_count=int(descriptor.row_count),
        prediction_date_start=_as_date(descriptor.date_start),
        prediction_date_end=_as_date(descriptor.date_end),
        runtime_asset_root=leg_root.resolve().as_posix(),
        runtime_ready=False,
        runtime_refs=(),
        missing_runtime_assets=tuple(sorted(missing)),
    )


def _inspect_post_cutoff_evidence(
    *, evidence_path: Path, comparison_state_path: Path, common_cutoff: date
) -> tuple[PostCutoffInferenceEvidenceV1 | None, str | None]:
    if not evidence_path.is_file():
        return None, "post_cutoff_evidence_missing"
    if not comparison_state_path.is_file():
        return None, "comparison_state_missing"
    artifact = _read_json(
        evidence_path, reason_code="ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID"
    )
    state = _read_json(
        comparison_state_path, reason_code="ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID"
    )
    if (
        artifact.get("schema_version") != "advisory_historical_model_challenger_artifact_v1"
        or artifact.get("producer_contract_version")
        != "advisory_historical_model_challenger_v1"
        or artifact.get("package_id") != PACKAGE_ID
        or artifact.get("manifest_sha256") != MANIFEST_SHA256
        or artifact.get("selection_runtime_semantics_hash")
        != EXPECTED_RUNTIME_SEMANTICS_HASH
    ):
        _raise(
            "post-cutoff artifact identity does not match target binding",
            "ADVISORY_PARENT_PREDICTION_IDENTITY_MISMATCH",
        )
    decision_date = _as_date(artifact.get("decision_trade_date"))
    target_date = _as_date(artifact.get("target_trade_date"))
    candidate_count = int(artifact.get("candidate_count") or 0)
    parent_artifact_hash = str(artifact.get("parent_candidate_artifact_hash") or "")
    parent_set_hash = str(artifact.get("parent_candidate_set_hash") or "")
    if (
        decision_date <= common_cutoff
        or candidate_count <= 0
        or len(artifact.get("candidates") or []) != candidate_count
        or not _is_sha256(parent_artifact_hash)
        or not _is_sha256(parent_set_hash)
    ):
        _raise(
            "post-cutoff artifact does not prove non-empty executable parent candidates",
            "ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID",
        )
    days = state.get("days") or {}
    day = days.get(decision_date.isoformat())
    if not isinstance(day, dict):
        _raise(
            "comparison state does not contain the evidence decision date",
            "ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID",
            decision_trade_date=decision_date.isoformat(),
        )
    artifact_ref = day.get("artifact_ref") or {}
    actual_file_sha = sha256_file(evidence_path)
    if (
        state.get("schema_version") != "advisory_historical_model_challenger_state_v2"
        or state.get("status") != "COMPLETED"
        or state.get("bundle_id") != artifact.get("bundle_id")
        or state.get("parent_range_run_id") != artifact.get("parent_range_run_id")
        or day.get("status") != "COMPLETE"
        or int(day.get("candidate_count") or 0) != candidate_count
        or day.get("target_trade_date") != target_date.isoformat()
        or day.get("parent_candidate_artifact_hash") != parent_artifact_hash
        or artifact_ref.get("file_sha256") != actual_file_sha
        or artifact_ref.get("artifact_hash") != artifact.get("artifact_hash")
        or Path(str(artifact_ref.get("relative_path") or "")).name != evidence_path.name
        or float(day.get("duration_seconds") or 0) <= 0
    ):
        _raise(
            "comparison state and post-cutoff artifact do not agree",
            "ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID",
            decision_trade_date=decision_date.isoformat(),
        )
    return (
        PostCutoffInferenceEvidenceV1(
            artifact_ref=evidence_reference_for_file(
                evidence_path, role="post_cutoff_parent_inference"
            ),
            comparison_state_ref=evidence_reference_for_file(
                comparison_state_path, role="post_cutoff_comparison_state"
            ),
            decision_trade_date=decision_date,
            target_trade_date=target_date,
            candidate_count=candidate_count,
            parent_candidate_artifact_hash=parent_artifact_hash,
            parent_candidate_set_hash=parent_set_hash,
            observed_duration_seconds=float(day["duration_seconds"]),
        ),
        None,
    )


def _validate_retrain_receipt(
    path: Path, *, target_start: date, target_end: date
) -> EvidenceReferenceV1:
    payload = _read_json(path, reason_code="ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID")
    if (
        payload.get("schema_version") != "advisory_parent_retrain_requirement_v1"
        or payload.get("status") != "RETRAIN_NEW_LINEAGE_REQUIRED"
        or payload.get("package_id") != PACKAGE_ID
        or payload.get("manifest_sha256") != MANIFEST_SHA256
        or payload.get("runtime_semantics_hash") != EXPECTED_RUNTIME_SEMANTICS_HASH
        or _as_date(payload.get("target_extension_start")) != target_start
        or _as_date(payload.get("target_extension_end")) != target_end
        or not payload.get("reason_code")
    ):
        _raise(
            "retrain requirement receipt is not an exact typed identity",
            "ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID",
        )
    return evidence_reference_for_file(path, role="explicit_retrain_requirement")


def freeze_default_research_windows(
    *,
    output_path: str | Path | None = None,
    artifact_root_uri: str | Path | None = None,
) -> AdvisoryResearchWindowContractV1:
    if output_path is not None:
        artifact_root = Path(output_path).resolve().parent
        if artifact_root_uri is not None and Path(artifact_root_uri).resolve() != artifact_root:
            _raise(
                "window output and artifact root disagree",
                "ADVISORY_RESEARCH_WINDOW_CONFLICT",
            )
    else:
        artifact_root = Path(
            artifact_root_uri
            or "F:/Dev/AIstock_model_artifacts/advisory_n0_research_control_20260830"
        ).resolve()
    artifact_root_text = artifact_root.as_posix()
    consumption_receipt_uri = (
        artifact_root / "sealed_holdout_consumption_receipt.json"
    ).as_posix()
    sealed_dataset_identity = canonical_json_sha256(
        {
            "dataset": "ADVISORY_SEALED_HOLDOUT_2026Q4_V1",
            "end_date": "2026-11-30",
            "manifest_sha256": MANIFEST_SHA256,
            "package_id": PACKAGE_ID,
            "runtime_semantics_hash": EXPECTED_RUNTIME_SEMANTICS_HASH,
            "source_policy": "PIT_DAILY_PARENT_PREDICTIONS_AND_MARKET_OUTCOMES_V1",
            "start_date": "2026-08-31",
        }
    )
    contract = build_window_contract(
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA256,
        runtime_semantics_hash=EXPECTED_RUNTIME_SEMANTICS_HASH,
        baseline_policy_sha256=BASELINE_POLICY_SHA256,
        shadow_policy_sha256=SHADOW_POLICY_SHA256,
        cost_policy_sha256=COST_POLICY_SHA256,
        source_policy="PIT_DAILY_PARENT_PREDICTIONS_AND_MARKET_OUTCOMES_V1",
        artifact_root_uri=artifact_root_text,
        sealed_consumption_receipt_uri=consumption_receipt_uri,
        windows=(
            AdvisoryResearchWindowV1(
                window_id="P0C_DEVELOPMENT_V1",
                dataset_identity=P0C_DATASET_IDENTITY,
                start_date=date(2024, 7, 4),
                end_date=date(2026, 3, 10),
                state=ResearchWindowState.DEVELOPMENT_CONSUMED,
                purpose="consumed development and CPCV selection window",
            ),
            AdvisoryResearchWindowV1(
                window_id="M1_M5_FROZEN_TEST_V1",
                dataset_identity="advisory_m1_m5_frozen_test_80d_v1",
                start_date=date(2025, 11, 7),
                end_date=date(2026, 3, 10),
                state=ResearchWindowState.FROZEN_TEST_CONSUMED,
                purpose="consumed 80-trading-day frozen test",
            ),
            AdvisoryResearchWindowV1(
                window_id="HISTORICAL_REPLAY_V1",
                dataset_identity="advisory_historical_replay_44d_24matured_v1",
                start_date=date(2026, 5, 15),
                end_date=date(2026, 7, 16),
                state=ResearchWindowState.HISTORICAL_REPLAY_CONSUMED,
                purpose="consumed historical replay and post-cutoff capability context",
            ),
            AdvisoryResearchWindowV1(
                window_id="ADVISORY_SEALED_HOLDOUT_2026Q4_V1",
                dataset_identity=sealed_dataset_identity,
                start_date=date(2026, 8, 31),
                end_date=date(2026, 11, 30),
                state=ResearchWindowState.SEALED_UNCONSUMED,
                purpose="prospective one-time confirmation only",
            ),
        ),
    )
    if output_path is not None:
        return _write_immutable_model(
            Path(output_path),
            contract,
            model_type=AdvisoryResearchWindowContractV1,
            hash_field="contract_sha256",
            conflict_reason="ADVISORY_RESEARCH_WINDOW_CONFLICT",
        )
    return contract


def authorize_research_window_access(
    *,
    contract: AdvisoryResearchWindowContractV1,
    request: ResearchWindowAccessRequestV1,
    consume_receipt_path: str | Path | None = None,
) -> dict[str, Any]:
    if request.contract_sha256 != contract.contract_sha256:
        _raise(
            "window access request targets another contract",
            "ADVISORY_RESEARCH_WINDOW_CONFLICT",
        )
    if request.study_type == ResearchStudyType.ACTIVATION:
        _raise(
            "activation may read only an already confirmed evidence receipt, not raw research windows",
            "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED",
        )
    sealed = next(
        item for item in contract.windows if item.state == ResearchWindowState.SEALED_UNCONSUMED
    )
    overlaps_sealed = _ranges_overlap(
        request.start_date, request.end_date, sealed.start_date, sealed.end_date
    )
    if not overlaps_sealed:
        if request.study_type == ResearchStudyType.CONFIRMATION:
            _raise(
                "confirmation cannot consume development or previously consumed windows",
                "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED",
            )
        matching_windows = [
            item
            for item in contract.windows
            if item.state != ResearchWindowState.SEALED_UNCONSUMED
            and item.start_date <= request.start_date
            and request.end_date <= item.end_date
            and item.dataset_identity == request.dataset_identity
        ]
        expected_policy = research_policy_identity(
            baseline_policy_sha256=contract.baseline_policy_sha256,
            shadow_policy_sha256=contract.shadow_policy_sha256,
            cost_policy_sha256=contract.cost_policy_sha256,
        )
        if len(matching_windows) != 1 or request.policy_identity != expected_policy:
            _raise(
                "development access must be contained by one declared window and exact identity",
                "ADVISORY_RESEARCH_WINDOW_CONFLICT",
            )
        return {
            "status": "AUTHORIZED_DEVELOPMENT_ONLY",
            "request_id": request.request_id,
            "sealed_holdout_accessed": False,
            "consumption_receipt": None,
        }
    if request.study_type != ResearchStudyType.CONFIRMATION:
        _raise(
            "this study type cannot access the sealed holdout",
            "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED",
            study_type=request.study_type.value,
        )
    expected_policy = research_policy_identity(
        baseline_policy_sha256=contract.baseline_policy_sha256,
        shadow_policy_sha256=contract.shadow_policy_sha256,
        cost_policy_sha256=contract.cost_policy_sha256,
    )
    if (
        request.start_date != sealed.start_date
        or request.end_date != sealed.end_date
        or request.dataset_identity != sealed.dataset_identity
        or request.policy_identity != expected_policy
    ):
        _raise(
            "confirmation must use the exact sealed dataset/date/policy identity",
            "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED",
        )
    if consume_receipt_path is None:
        _raise(
            "confirmation requires the canonical consume-once receipt path",
            "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED",
        )
    receipt_path = Path(consume_receipt_path)
    if receipt_path.resolve().as_posix() != Path(
        contract.sealed_consumption_receipt_uri
    ).resolve().as_posix():
        _raise(
            "confirmation consume receipt path differs from the contract canonical path",
            "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED",
            expected_receipt_uri=contract.sealed_consumption_receipt_uri,
        )
    receipt = build_holdout_consumption_receipt(
        contract=contract, request=request, window_id=sealed.window_id
    )
    written = _write_consumption_once(receipt_path, receipt)
    return {
        "status": "AUTHORIZED_SEALED_HOLDOUT_ONCE",
        "request_id": request.request_id,
        "sealed_holdout_accessed": True,
        "consumption_receipt": written.model_dump(mode="json"),
    }


def load_window_contract(path: str | Path) -> AdvisoryResearchWindowContractV1:
    try:
        return AdvisoryResearchWindowContractV1.model_validate(
            _read_json(Path(path), reason_code="ADVISORY_RESEARCH_WINDOW_CONFLICT")
        )
    except (ValidationError, ValueError) as exc:
        _raise(
            "research window contract is invalid",
            "ADVISORY_RESEARCH_WINDOW_CONFLICT",
            error_type=type(exc).__name__,
        )


def generate_current_route(
    *,
    registry_path: str | Path,
    parent_spike_path: str | Path,
    window_contract_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    registry = AdvisoryResearchTrialRegistryV1(registry_path)
    records = registry.read()
    experiment_ids = {item.experiment_id for item in records}
    missing_p0 = sorted(set(P0_FAMILY_EXPERIMENT_IDS) - experiment_ids)
    if missing_p0 or N0_EXPERIMENT_ID not in experiment_ids:
        _raise(
            "registry lacks the frozen P0 family or N0 control record",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            missing_p0=missing_p0,
            n0_present=N0_EXPERIMENT_ID in experiment_ids,
        )
    p0_records = [item for item in records if item.experiment_id in P0_FAMILY_EXPERIMENT_IDS]
    if any(
        item.decision_use != DecisionUse.NAVIGATION_ONLY
        or item.result_class in {ResearchResultClass.CONFIRMED, ResearchResultClass.ACTIVATED}
        for item in p0_records
    ) or not any(
        item.experiment_id == "ADVISORY-P0-L"
        and item.result_class == ResearchResultClass.FAMILY_FROZEN
        for item in p0_records
    ):
        _raise(
            "registry does not prove the historical P0 family is navigation-only and frozen",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
        )
    n0_records = [item for item in records if item.experiment_id == N0_EXPERIMENT_ID]
    if len(n0_records) != 1 or n0_records[0].result_class != ResearchResultClass.CONTROL_READY:
        _raise(
            "registry does not contain exactly one CONTROL_READY N0 record",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
        )
    try:
        spike = AdvisoryParentPredictionExtensionReceiptV1.model_validate(
            _read_json(
                Path(parent_spike_path),
                reason_code="ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            )
        )
        window = load_window_contract(window_contract_path)
    except ValidationError as exc:
        _raise(
            "route input receipt is invalid",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            error_type=type(exc).__name__,
        )
    if spike.package_id != window.package_id or spike.manifest_sha256 != window.manifest_sha256:
        _raise(
            "route inputs do not share the same target package",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
        )
    n0_record = n0_records[0]
    parent_ref = evidence_reference_for_file(parent_spike_path, role="n0_parent_spike")
    window_ref = evidence_reference_for_file(window_contract_path, role="n0_window_contract")
    expected_n0_refs = {
        (parent_ref.artifact_uri, parent_ref.sha256),
        (window_ref.artifact_uri, window_ref.sha256),
    }
    actual_n0_refs = {(item.artifact_uri, item.sha256) for item in n0_record.evidence_refs}
    if (
        actual_n0_refs != expected_n0_refs
        or n0_record.policy_identity != window.contract_sha256
        or n0_record.dataset_identity != "CONTROL_ONLY_NO_MARKET_DATA_READ"
    ):
        _raise(
            "N0 control record does not bind the supplied parent/window receipts",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
        )
    n1_records = [item for item in records if item.experiment_id in N1_EXPERIMENT_IDS]
    n1_present = {item.experiment_id for item in n1_records}
    if n1_present and n1_present != set(N1_EXPERIMENT_IDS):
        _raise(
            "registry contains only part of the atomic N1 diagnostic pair",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            present_n1=sorted(n1_present),
            missing_n1=sorted(set(N1_EXPERIMENT_IDS) - n1_present),
        )
    if n1_present:
        _validate_n1_route_records(n1_records, window=window)
        next_task = "N2_ENTRY_EXIT_QE_PREPARATION"
        n1_state = "COMPLETE"
    else:
        next_task = (
            "N1_TIER1_ORACLE_LEARNABILITY"
            if spike.status == ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER
            else "PARENT_PREDICTION_EXTENSION_DECISION"
        )
        n1_state = "PENDING"
    registry_sha = sha256_file(registry_path)
    # ALGO-COMPLEXITY-001: the route is a fixed-size projection; no row join,
    # market-data loop, or result-sized materialization occurs here.
    text = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            "> schema: `advisory_research_route_v1`  ",
            f"> registry_sha256: `{registry_sha}`  ",
            f"> window_contract_sha256: `{window.contract_sha256}`  ",
            "",
            "| 项目 | 当前事实 |",
            "|---|---|",
            "| P0-D..P0-L | `FAMILY_FROZEN`；不得派生 P0-M |",
            "| N0 | `COMPLETE`；registry、父包 spike、窗口合同已具备 |",
            f"| N1 Tier-1 oracle + learnability | `{n1_state}` |",
            f"| 父包延伸能力 | `{spike.status.value}` |",
            "| active main research line | `NONE` |",
            "| active auxiliary research line | `NONE` |",
            f"| next task | `{next_task}` |",
            "| runtime activation | `NOOP` |",
            "",
            "该页面只由 registry 与 receipt 派生，不构成独立状态权威。",
            "",
        )
    )
    _write_derived_text(Path(output_path), text)
    return {
        "status": "ok",
        "schema_version": "advisory_research_route_v1",
        "output_path": Path(output_path).resolve().as_posix(),
        "output_sha256": sha256_file(output_path),
        "parent_prediction_status": spike.status.value,
        "n1_state": n1_state,
        "next_task": next_task,
    }


def _validate_n1_route_records(
    records: Sequence[AdvisoryResearchTrialRecordV1],
    *,
    window: AdvisoryResearchWindowContractV1,
) -> None:
    if len(records) != 2 or {item.experiment_id for item in records} != set(
        N1_EXPERIMENT_IDS
    ):
        _raise(
            "registry does not contain exactly one record for each N1 diagnostic",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
        )
    by_id = {item.experiment_id: item for item in records}
    oracle = by_id[N1_ORACLE_EXPERIMENT_ID]
    learnability = by_id[N1_LEARNABILITY_EXPERIMENT_ID]
    shared_identity = (
        "attempt_id",
        "dataset_identity",
        "policy_identity",
        "objective_contract",
        "consumed_windows",
        "parent_lineage",
    )
    mismatches = [
        field
        for field in shared_identity
        if getattr(oracle, field) != getattr(learnability, field)
    ]
    if (
        mismatches
        or oracle.study_type != ResearchStudyType.ORACLE_DIAGNOSTIC
        or learnability.study_type != ResearchStudyType.LEARNABILITY_AUDIT
        or oracle.objective_contract != ObjectiveContract.ALPHA_RANKING
        or oracle.planned_trial_count != 1
        or learnability.planned_trial_count != 1
        or oracle.evaluated_trial_count != 1
        or learnability.evaluated_trial_count != 1
        or len(oracle.consumed_windows) != 1
        or oracle.consumed_windows[0].window_id != "P0C_DEVELOPMENT_V1"
        or oracle.consumed_windows[0].dataset_identity != P0C_DATASET_IDENTITY
        or any(
            item.decision_use == DecisionUse.ACTIVATION_EVIDENCE
            or item.result_class
            in {ResearchResultClass.CONFIRMED, ResearchResultClass.ACTIVATED}
            or len(item.evidence_refs) != 1
            for item in records
        )
    ):
        _raise(
            "N1 diagnostic records violate the atomic route contract",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            mismatched_identity_fields=mismatches,
        )
    expected_policy_identity = research_policy_identity(
        baseline_policy_sha256=window.baseline_policy_sha256,
        shadow_policy_sha256=window.shadow_policy_sha256,
        cost_policy_sha256=window.cost_policy_sha256,
    )
    if oracle.policy_identity != expected_policy_identity:
        _raise(
            "N1 diagnostic policy identity differs from the research window",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
        )


def complete_n0(
    *,
    registry_path: str | Path,
    parent_spike_path: str | Path,
    window_contract_path: str | Path,
    route_path: str | Path,
    output_path: str | Path,
) -> N0CompletionReceiptV1:
    spike_ref = evidence_reference_for_file(parent_spike_path, role="n0_parent_spike")
    window_ref = evidence_reference_for_file(window_contract_path, role="n0_window_contract")
    spike = AdvisoryParentPredictionExtensionReceiptV1.model_validate(
        _read_json(Path(parent_spike_path), reason_code="ADVISORY_RESEARCH_ROUTE_INCONSISTENT")
    )
    window = load_window_contract(window_contract_path)
    n0_record = build_trial_record(
        experiment_id=N0_EXPERIMENT_ID,
        attempt_id="N0-FORMAL-V1",
        research_stage="N0_RESEARCH_CONTROL",
        study_type=ResearchStudyType.EXPLORATORY_SCREEN,
        hypothesis_family_id="ADVISORY_RESEARCH_CONTROL_V1",
        parent_lineage=P0_FAMILY_EXPERIMENT_IDS,
        unique_variable="CONTROL_PLANE_ONLY_NO_ECONOMIC_HYPOTHESIS",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity="CONTROL_ONLY_NO_MARKET_DATA_READ",
        schema_identity="advisory_research_control_contracts_v1",
        policy_identity=window.contract_sha256,
        planned_trial_count=0,
        generated_trial_count=0,
        evaluated_trial_count=0,
        selected_trial_count=0,
        consumed_windows=(),
        result_class=ResearchResultClass.CONTROL_READY,
        decision_use=DecisionUse.NAVIGATION_ONLY,
        evidence_refs=(spike_ref, window_ref),
    )
    AdvisoryResearchTrialRegistryV1(registry_path).append_batch((n0_record,))
    route_summary = generate_current_route(
        registry_path=registry_path,
        parent_spike_path=parent_spike_path,
        window_contract_path=window_contract_path,
        output_path=route_path,
    )
    receipt = build_n0_completion_receipt(
        registry_ref=evidence_reference_for_file(registry_path, role="n0_trial_registry"),
        route_ref=evidence_reference_for_file(route_path, role="n0_current_route"),
        parent_spike_ref=spike_ref,
        window_contract_ref=window_ref,
        next_task=route_summary["next_task"],
    )
    written = _write_immutable_model(
        Path(output_path),
        receipt,
        model_type=N0CompletionReceiptV1,
        hash_field="receipt_sha256",
        conflict_reason="ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
    )
    if route_summary["next_task"] != written.next_task:
        _raise(
            "N0 completion and route disagree on next task",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            parent_prediction_status=spike.status.value,
        )
    return written


@contextmanager
def _exclusive_file_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_immutable_model(
    path: Path,
    model: Any,
    *,
    model_type: type[Any],
    hash_field: str,
    conflict_reason: str,
) -> Any:
    with _exclusive_file_lock(path.with_suffix(path.suffix + ".lock")):
        if path.exists():
            try:
                prior = model_type.model_validate(_read_json(path, reason_code=conflict_reason))
            except (ValidationError, ValueError) as exc:
                _raise(
                    "existing immutable receipt is invalid",
                    conflict_reason,
                    artifact_uri=path.as_posix(),
                    error_type=type(exc).__name__,
                )
            if getattr(prior, hash_field) != getattr(model, hash_field):
                _raise(
                    "existing immutable receipt has different functional content",
                    conflict_reason,
                    artifact_uri=path.as_posix(),
                )
            return prior
        payload = json.dumps(model.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n"
        _write_atomic_text(path, payload, replace_existing=False)
        return model


def _write_consumption_once(
    path: Path, receipt: SealedHoldoutConsumptionReceiptV1
) -> SealedHoldoutConsumptionReceiptV1:
    with _exclusive_file_lock(path.with_suffix(path.suffix + ".lock")):
        if path.exists():
            prior_id = "invalid_existing_receipt"
            try:
                prior = SealedHoldoutConsumptionReceiptV1.model_validate(
                    _read_json(
                        path, reason_code="ADVISORY_SEALED_HOLDOUT_ALREADY_CONSUMED"
                    )
                )
                prior_id = prior.consumption_id
            except (ValidationError, ValueError, AdvisoryModelFirstError):
                pass
            _raise(
                "sealed holdout already has a consumption receipt",
                "ADVISORY_SEALED_HOLDOUT_ALREADY_CONSUMED",
                consumption_id=prior_id,
            )
        payload = json.dumps(receipt.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n"
        _write_atomic_text(path, payload, replace_existing=False)
        return receipt


def _write_atomic_text(path: Path, text: str, *, replace_existing: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        if not replace_existing and path.exists():
            _raise(
                "immutable output appeared during publication",
                "ADVISORY_RESEARCH_REGISTRY_CONFLICT",
                artifact_uri=path.as_posix(),
            )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_derived_text(path: Path, text: str) -> None:
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == text:
                return
        except (OSError, UnicodeDecodeError) as exc:
            _raise(
                "existing derived artifact cannot be read",
                "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
                artifact_uri=path.as_posix(),
                error_type=type(exc).__name__,
            )
    _write_atomic_text(path, text)


def _read_json(path: Path, *, reason_code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _raise(
            "JSON artifact cannot be read",
            reason_code,
            artifact_uri=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise("JSON artifact must be an object", reason_code, artifact_uri=path.as_posix())
    return payload


def _as_date(value: Any) -> date:
    try:
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        _raise(
            "date value is invalid",
            "ADVISORY_PARENT_EXTENSION_EVIDENCE_INVALID",
            value=str(value),
            error_type=type(exc).__name__,
        )


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _ranges_overlap(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    return a_start <= b_end and b_start <= a_end


def _raise_registry_invalid(message: str, **context: Any) -> None:
    _raise(message, "ADVISORY_RESEARCH_REGISTRY_INVALID", **context)


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)
