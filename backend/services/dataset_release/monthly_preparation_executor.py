"""Physical unpublished preparation using the existing bounded BUILD writers.

This executor is not a release producer. Its records cannot satisfy SOURCE or
BUILD. Production composition must also install final-source adoption before
enabling preparation; preparation alone is deliberately not an entry point.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable, Mapping

from .artifact_ready_build_source import ArtifactReadyBuildSource, ArtifactReadyPreparationBuildSource
from .build_stage import BuildStageInvocation, PRIVATE_STAGE_SCHEMA, run_preparation_build_stage
from .canonical import canonical_json_bytes, digest_named_fields, ensure_sha256
from .cas_store import CASStore
from .contracts import Component, ComponentAction
from .decision import DECISION_SCHEMA_VERSION
from .monthly_component_preparation import (
    ComponentInputIdentity,
    ComponentPreparationError,
    _file_ref,
    _plain_path,
    seal_prepared_component,
    validate_prepared_component,
)
from .monthly_mature_build_runner import MonthlyBuildExecutionScopeFactory
from .monthly_worker import ProducerContext
from .profile import DatasetProfile


PRIVATE_PHYSICAL_PLAN_SCHEMA = "aistock_monthly_private_physical_plan_v1"
PREPARATION_RECORD_SCHEMA = "aistock_monthly_preparation_record_v1"
_PHYSICAL = {
    Component.DAILY_BIN: ("day", "daily_bin"),
    Component.MINUTE_BIN: ("minute", "minute_bin"),
    Component.DOMESTIC_INDEX_CONTEXT: ("index", "index_context"),
}
_DIRECTORIES = {name: directory for name, directory in _PHYSICAL.values()}
_DIRECTORIES.update({name: name for name in ("stock_pools", "benchmark", "suspend", "sector_context")})
_CODE_FILES = (
    "monthly_preparation_executor.py",
    "monthly_preparation_composition.py",
    "monthly_component_preparation.py",
    "monthly_preparation_artifacts.py",
    "artifact_ready_source.py",
    "artifact_ready_build_source.py",
    "build_stage.py",
    "canonical_stock_transformer.py",
    "daily_minute_materializer.py",
    "index_materializer.py",
    "index_contract.py",
    "stock_schema.py",
    "canonical.py",
    "streaming_artifacts.py",
    "external_ordered_rows.py",
    "monthly_supervised_build.py",
    "monthly_production.py",
    "subprocess_runner.py",
    "wsl_resource_guardian.py",
    "log_store.py",
    "resource_supervisor.py",
    "resource_gate.py",
)


def physical_preparation_identity(
    *,
    source: ArtifactReadyBuildSource,
    component: Component,
    predecessor_profile_sha256: str,
) -> ComponentInputIdentity:
    """Use the formal effective root, not the unrelated global source root.

    Producer bytes are checked in addition to versioned profile contracts.
    Conservative producer invalidation is safe; equal row counts are not.
    The same function must be used after complete SOURCE before adoption.
    """
    if component not in _PHYSICAL or component not in source.component_manifests:
        raise ComponentPreparationError("private physical identity component differs")
    manifest = source.component_manifests[component]
    profile = source.profile
    code_root = Path(__file__).parent
    code = {name: _file_ref(code_root, name) for name in _CODE_FILES}
    qfq = source.qfq_authority.digest if component is not Component.DOMESTIC_INDEX_CONTEXT else None
    return ComponentInputIdentity(
        component=_PHYSICAL[component][0],
        cutoff=source.cutoff,
        predecessor_profile_sha256=predecessor_profile_sha256,
        effective_source_sha256=str(manifest.get("component_effective_content_root") or ""),
        pit_sha256=source.pit_snapshot.spans_sha256,
        qfq_sha256=qfq,
        producer_sha256=digest_named_fields(
            "aistock_monthly_private_physical_producer_v1",
            {
                "code": code,
                "toolchain_digest": profile.qlib_toolchain.digest,
            },
        ),
        schema_sha256=digest_named_fields(
            "aistock_monthly_private_physical_schema_v1",
            {
                "schema_digests": sorted({str(row["schema_digest"]) for row in manifest["effective_partitions"]}),
                "stock_schema": profile.qlib_stock_schema_digest,
            },
        ),
        build_parameters_sha256=digest_named_fields(
            "aistock_monthly_private_physical_parameters_v1",
            {
                "semantic_profile_digest": profile.semantic_profile_digest,
                "pressure_rung": 0,
            },
        ),
        validation_policy_sha256=digest_named_fields(
            "aistock_monthly_private_physical_validation_v1",
            {
                "semantic_profile_digest": profile.semantic_profile_digest,
                "private_stage_schema": PRIVATE_STAGE_SCHEMA,
                "full_source_required_before_adoption": True,
            },
        ),
    )


def _private_plan(source: ArtifactReadyPreparationBuildSource) -> Mapping[str, Any]:
    actions = [
        {
            "component": component.value,
            "partition_key": "full",
            "action": ComponentAction.FULL_REBUILD.value,
        }
        for component in sorted(Component, key=lambda value: value.value)
    ]
    return {
        "schema_version": PRIVATE_PHYSICAL_PLAN_SCHEMA,
        "target_cutoff": source.cutoff.isoformat(),
        "actions": actions,
        "action_plan_digest": digest_named_fields(DECISION_SCHEMA_VERSION, {"actions": actions}),
        "build_inputs": {"schema_version": PRIVATE_PHYSICAL_PLAN_SCHEMA, "baseline": None},
        "publication_allowed": False,
    }


def _write_exclusive(path: Path, value: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = canonical_json_bytes(value) + b"\n"
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    return {"path": path.name, "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}


def _mkdir_private(path: Path, *, root: Path) -> Path:
    if not path.is_relative_to(root):
        raise ComponentPreparationError("private directory escaped catalog")
    current = _plain_path(root, root=root, directory=True)
    for part in path.relative_to(root).parts:
        current /= part
        current.mkdir(exist_ok=True)
        _plain_path(current, root=root, directory=True)
    return current


@dataclass(frozen=True, slots=True)
class VerifiedPreparationCheckpoint:
    record: Mapping[str, Any]
    receipt: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MonthlyPrivatePhysicalPreparationExecutor:
    profile: DatasetProfile
    cas: CASStore
    project_root: Path
    execution_scope_factory: MonthlyBuildExecutionScopeFactory

    def execute(
        self,
        *,
        context: ProducerContext,
        source: ArtifactReadyPreparationBuildSource,
        source_snapshot_id: str,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> Mapping[str, Any]:
        """Materialize and seal selected components; recover only pinned files.

        No provider/database is opened here. A failed component has no completed
        record; components sealed earlier remain recoverable by a later fence.
        """
        selected = set(source.component_manifests)
        if (
            context.stage != "SOURCE"
            or type(context.attempt) is not int
            or context.attempt < 1
            or re.fullmatch(r"dmr_[0-9a-f]{32}", context.operation_id) is None
            or not isinstance(source, ArtifactReadyPreparationBuildSource)
            or source.contract.get("operation_id") != context.operation_id
            or source.cutoff.isoformat() != context.plan.get("target_cutoff")
            or self.profile.profile != source.profile.profile
            or self.profile.semantic_profile_digest != source.profile.semantic_profile_digest
            or self.profile.qlib_toolchain.digest != source.profile.qlib_toolchain.digest
            or not selected
            or not selected <= set(_PHYSICAL)
            or not isinstance(source_snapshot_id, str)
            or not source_snapshot_id.startswith("postgres:")
            or not source_snapshot_id[9:]
        ):
            raise ComponentPreparationError("private physical execution binding differs")
        predecessor = context.plan.get("predecessor")
        if not isinstance(predecessor, Mapping):
            raise ComponentPreparationError("private preparation predecessor is missing")
        predecessor_sha = ensure_sha256(str(predecessor.get("profile_sha256") or ""), field="predecessor profile")
        root = _plain_path(Path(self.profile.candidate_root), root=Path(self.profile.candidate_root), directory=True)
        private = _mkdir_private(root / ".staging", root=root)
        records = _mkdir_private(private / "preparation-records" / context.operation_id, root=root)
        identities = {
            component: physical_preparation_identity(
                source=source,
                component=component,
                predecessor_profile_sha256=predecessor_sha,
            )
            for component in selected
        }
        recovered = {}
        for component in sorted(selected, key=lambda value: value.value):
            checkpoint()
            previous = self._recover(
                root=root,
                records=records,
                context=context,
                identity=identities[component],
            )
            if previous is not None:
                recovered[component] = previous.record
        remaining = selected - set(recovered)
        if remaining:
            # Daily CSV preparation consumes the same frozen index CSV; it is
            # a physical dependency even when that index is already prepared.
            if Component.DAILY_BIN in remaining:
                remaining.add(Component.DOMESTIC_INDEX_CONTEXT)
                if Component.DOMESTIC_INDEX_CONTEXT not in selected:
                    raise ComponentPreparationError("private daily execution lacks index dependency")
            source_view = self._select(source, remaining)
            staging = private / f"{context.operation_id}-preparation-attempt-{context.attempt}"
            plan = {**_private_plan(source_view), "preparation_predecessor_profile_sha256": predecessor_sha}
            digest = digest_named_fields(
                PRIVATE_PHYSICAL_PLAN_SCHEMA,
                {
                    "operation_id": context.operation_id,
                    "identities": {component.value: identities[component].digest for component in remaining},
                },
            )
            common = dict(
                run_id=context.operation_id,
                attempt_id=f"{context.operation_id}-preparation-{context.attempt}",
                attempt_fence=context.attempt,
                pressure_rung=0,
                stage_timeout_seconds=self.profile.stage_timeouts_seconds["full_build"],
                release_id=str(context.plan.get("release_id") or ""),
                release_digest=digest,
                staging_relative_path=staging.relative_to(root).as_posix(),
                project_root=self.project_root,
                candidate_root=root,
                staging_root=staging,
                profile=self.profile,
                cas=self.cas,
                plan=plan,
            )
            if not common["release_id"]:
                raise ComponentPreparationError("private execution release identity is missing")

            def seal_component(component: Component, domain_refs: Mapping[str, Any]) -> None:
                checkpoint()
                if component not in remaining or not domain_refs:
                    raise ComponentPreparationError("private domain completion component differs")
                if identities[component] != physical_preparation_identity(
                    source=source,
                    component=component,
                    predecessor_profile_sha256=predecessor_sha,
                ):
                    raise ComponentPreparationError("private producer/input changed during materialization")
                if component in recovered:
                    return  # A pinned index dependency was copied, not recomputed.
                from .cas_store import CASRef

                primary = domain_refs.get("domain_receipt_ref")
                if not isinstance(primary, Mapping):
                    raise ComponentPreparationError("private completion domain receipt is missing")
                for raw_ref in domain_refs.values():
                    self.cas.verify(CASRef.from_value(raw_ref))
                domain = self.cas.get_json_bounded(CASRef.from_value(primary), max_bytes=32 * 1024 * 1024)
                if not isinstance(domain, Mapping) or domain.get("status") != "PASS":
                    raise ComponentPreparationError("private component domain validation did not pass")
                logical, directory = _PHYSICAL[component]
                component_root = _plain_path(staging / directory, root=root, directory=True)
                validation = _write_exclusive(
                    component_root / "private-domain-validation.json",
                    {
                        "schema_version": "aistock_monthly_private_physical_validation_v1",
                        "operation_id": context.operation_id,
                        "identity_digest": identities[component].digest,
                        "normalization_artifact_ref": source_view.contract_ref.as_dict(),
                        "domain_refs": dict(domain_refs),
                        "publication_allowed": False,
                    },
                )
                outputs = []
                for path in component_root.rglob("*"):
                    checkpoint()
                    if path.is_dir():
                        _plain_path(path, root=component_root, directory=True)
                    elif path != component_root / "private-domain-validation.json":
                        outputs.append(_plain_path(path, root=component_root).relative_to(component_root).as_posix())
                receipt = seal_prepared_component(
                    preparation_root=root,
                    component_root=component_root,
                    operation_id=context.operation_id,
                    source_snapshot_id=source_snapshot_id,
                    identity=identities[component],
                    output_paths=outputs,
                    validation_paths=[str(validation["path"])],
                )
                body = {
                    "schema_version": PREPARATION_RECORD_SCHEMA,
                    "operation_id": context.operation_id,
                    "attempt": context.attempt,
                    "component": logical,
                    "component_root": component_root.relative_to(root).as_posix(),
                    "identity_digest": identities[component].digest,
                    "receipt_ref": receipt,
                    "publication_allowed": False,
                }
                record = {**body, "canonical_digest": digest_named_fields(PREPARATION_RECORD_SCHEMA, body)}
                _write_exclusive(records / f"attempt-{context.attempt}-{logical}.json", record)
                recovered[component] = record

            # Existing attempt-bound writer owns any dump children; SOURCE is
            # still the operation stage and its full checkpoint remains false.
            with self.execution_scope_factory(replace(context, stage="BUILD")) as tools:
                prepared = run_preparation_build_stage(
                    BuildStageInvocation(stage="prepare", prerequisites={}, **common),
                    source=source_view,
                    checkpoint=checkpoint,
                    component_completed=seal_component,
                )
                operations = prepared.get("qlib_dump_operations")
                if not isinstance(operations, list):
                    raise ComponentPreparationError("private writer operation set is missing")
                prerequisites = {"prepare": self.cas.verify(self.cas.put_json(dict(prepared))).sha256}
                for operation in operations:
                    checkpoint()
                    if not isinstance(operation, Mapping) or operation.get("operation_id") not in {"daily", "minute"}:
                        raise ComponentPreparationError("private writer operation identity differs")
                    key = f"qlib_dump_{operation['operation_id']}"
                    if key in prerequisites:
                        raise ComponentPreparationError("private writer operation is duplicated")
                    result = tools.qlib_writer.execute(
                        context=replace(context, stage="BUILD"),
                        staging_root=staging,
                        operation=operation,
                    )
                    prerequisites[key] = self.cas.verify(self.cas.put_json(dict(result))).sha256
                run_preparation_build_stage(
                    BuildStageInvocation(stage="finalize-bins", prerequisites=prerequisites, **common),
                    source=source_view,
                    checkpoint=checkpoint,
                    component_completed=seal_component,
                )
            if remaining - set(recovered):
                raise ComponentPreparationError("private stage did not seal all completed component outputs")
        return {
            "status": "COMPONENTS_PREPARED_UNPUBLISHED",
            "operation_id": context.operation_id,
            "prepared_components": {component.value: value for component, value in recovered.items()},
            "prepared_component_count": len(recovered),
            "consistent_input_set_complete": False,
            "publication_allowed": False,
            "database_write_performed": False,
        }

    @staticmethod
    def _select(
        source: ArtifactReadyPreparationBuildSource, selected: set[Component]
    ) -> ArtifactReadyPreparationBuildSource:
        # All graph validation already ran in the public constructor. This
        # restricted view cannot add data/capabilities or become a full graph.
        import copy

        view = copy.copy(source)
        view.component_manifests = {component: source.component_manifests[component] for component in selected}
        return view

    @staticmethod
    def _recover(
        *,
        root: Path,
        records: Path,
        context: ProducerContext,
        identity: ComponentInputIdentity,
    ) -> VerifiedPreparationCheckpoint | None:
        candidates = []
        for path in records.iterdir():
            match = re.fullmatch(rf"attempt-([1-9][0-9]*)-{identity.component}[.]json", path.name)
            if match and int(match[1]) <= context.attempt:
                candidates.append((int(match[1]), path))
        for attempt, path in sorted(candidates, reverse=True):
            path = _plain_path(path, root=root)
            if path.stat().st_size > 64 * 1024:
                raise ComponentPreparationError("private preparation record exceeds control bound")
            raw = path.read_bytes()
            try:
                record = json.loads(raw)
            except (ValueError, UnicodeError) as exc:
                raise ComponentPreparationError("private preparation record is invalid JSON") from exc
            fields = {
                "schema_version",
                "operation_id",
                "attempt",
                "component",
                "component_root",
                "identity_digest",
                "receipt_ref",
                "publication_allowed",
                "canonical_digest",
            }
            if not isinstance(record, dict) or set(record) != fields:
                raise ComponentPreparationError("private preparation record schema differs")
            unsigned = {key: value for key, value in record.items() if key != "canonical_digest"}
            if (
                raw != canonical_json_bytes(record) + b"\n"
                or record["canonical_digest"] != digest_named_fields(PREPARATION_RECORD_SCHEMA, unsigned)
                or record["schema_version"] != PREPARATION_RECORD_SCHEMA
                or record["operation_id"] != context.operation_id
                or record["component"] != identity.component
                or type(record["attempt"]) is not int
                or record["attempt"] != attempt
                or record["publication_allowed"] is not False
            ):
                raise ComponentPreparationError("private preparation record identity differs")
            if record["identity_digest"] != identity.digest:
                continue  # Exact input/producer drift invalidates this component.
            relative = record["component_root"]
            if identity.component not in _DIRECTORIES:
                raise ComponentPreparationError("private component recovery domain differs")
            expected = f".staging/{context.operation_id}-preparation-attempt-{attempt}/{_DIRECTORIES[identity.component]}"
            if relative != expected:
                raise ComponentPreparationError("private component root binding differs")
            reference = record["receipt_ref"]
            if (
                not isinstance(reference, dict)
                or set(reference) != {"path", "sha256", "size"}
                or reference["path"] != f"{relative}/prepared-component.json"
                or type(reference["size"]) is not int
                or _file_ref(root, reference["path"]) != reference
            ):
                raise ComponentPreparationError("private component receipt ref differs")
            receipt = validate_prepared_component(
                preparation_root=root,
                component_root=root / relative,
                receipt_sha256=reference["sha256"],
                operation_id=context.operation_id,
                expected_identity=identity,
            )
            return VerifiedPreparationCheckpoint(record, receipt)
        return None


def _copy_pinned_component(
    *,
    root: Path,
    destination: Path,
    verified: VerifiedPreparationCheckpoint,
    checkpoint: Callable[[], None],
    selected_paths: set[str] | None = None,
) -> None:
    previous = root / verified.record["component_root"]
    if selected_paths is not None and (
        not selected_paths or not selected_paths <= {ref["path"] for ref in verified.receipt["output_refs"]}
    ):
        raise ComponentPreparationError("prepared adoption selection is not pinned")
    if destination.exists():
        raise ComponentPreparationError("prepared adoption target already exists")
    _mkdir_private(destination.parent, root=root)
    destination.mkdir()
    for ref in verified.receipt["output_refs"]:
        if selected_paths is not None and ref["path"] not in selected_paths:
            continue
        checkpoint()
        target = destination / ref["path"]
        _mkdir_private(target.parent, root=destination)
        original = _plain_path(previous / ref["path"], root=previous)
        digest = hashlib.sha256()
        byte_count = 0
        with original.open("rb") as reader, target.open("xb") as writer:
            for block in iter(lambda: reader.read(1024 * 1024), b""):
                checkpoint()
                writer.write(block)
                digest.update(block)
                byte_count += len(block)
            writer.flush()
            os.fsync(writer.fileno())
        if digest.hexdigest() != ref["sha256"] or byte_count != ref["size"]:
            raise ComponentPreparationError("prepared output changed during adoption")


def adopt_pinned_shared_file(
    *, root: Path, verified: VerifiedPreparationCheckpoint, relative_path: str,
    destination: Path, checkpoint: Callable[[], None] = lambda: None,
) -> Path:
    """Create one independent final staging file from a recovered exact pin."""
    candidates = [ref for ref in verified.receipt["output_refs"] if ref["path"] == relative_path]
    if len(candidates) != 1:
        raise ComponentPreparationError("shared adoption file is not pinned")
    ref = candidates[0]
    _mkdir_private(destination.parent, root=root)
    previous = root / verified.record["component_root"]
    original = _plain_path(previous / relative_path, root=previous)
    digest = hashlib.sha256()
    count = 0
    with original.open("rb") as reader, destination.open("xb") as writer:
        for block in iter(lambda: reader.read(1024 * 1024), b""):
            checkpoint()
            writer.write(block)
            digest.update(block)
            count += len(block)
        writer.flush()
        os.fsync(writer.fileno())
    if count != ref["size"] or digest.hexdigest() != ref["sha256"]:
        raise ComponentPreparationError("shared prepared file changed during adoption")
    return destination


def reuse_private_index_dependency(
    invocation: BuildStageInvocation,
    *,
    source: ArtifactReadyPreparationBuildSource,
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any] | None:
    """Reuse the frozen index CSV needed by unfinished private day preparation.

    This may only create another unpublished copy. It is deliberately not the
    final SOURCE adoption path and has no permission to publish a component.
    """
    predecessor = invocation.plan.get("preparation_predecessor_profile_sha256")
    if predecessor is None:
        return None
    if (
        not isinstance(source, ArtifactReadyPreparationBuildSource)
        or invocation.stage != "prepare"
        or invocation.run_id != source.contract.get("operation_id")
        or source.cutoff.isoformat() != invocation.plan.get("target_cutoff")
    ):
        raise ComponentPreparationError("private index dependency reuse binding differs")
    root = _plain_path(invocation.candidate_root, root=invocation.candidate_root, directory=True)
    _plain_path(invocation.staging_root, root=root, directory=True)
    records = root / ".staging" / "preparation-records" / invocation.run_id
    if not records.exists():
        return None
    _plain_path(records, root=root, directory=True)
    identity = physical_preparation_identity(
        source=source,
        component=Component.DOMESTIC_INDEX_CONTEXT,
        predecessor_profile_sha256=str(predecessor),
    )
    verified = MonthlyPrivatePhysicalPreparationExecutor._recover(
        root=root,
        records=records,
        context=ProducerContext("SOURCE", invocation.run_id, invocation.attempt_fence, {}, {}, {}),
        identity=identity,
    )
    if verified is None:
        return None
    _copy_pinned_component(
        root=root,
        destination=invocation.staging_root / "index_context",
        verified=verified,
        checkpoint=checkpoint,
    )
    return {
        "schema_version": "aistock_monthly_private_dependency_reuse_v1",
        "identity_digest": identity.digest,
        "prepared_receipt_ref": verified.record["receipt_ref"],
        "publication_allowed": False,
    }


def adopt_prepared_physical_components(
    invocation: BuildStageInvocation,
    *,
    source: ArtifactReadyBuildSource,
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[Component, Mapping[str, Any]]:
    """Copy only pinned output bytes after full-source validation.

    Called only by formal prepare, after its existing strict _build_source
    loader. Private graphs are rejected even if they have identical rows. This
    is a computation reuse proof, never a SOURCE/BUILD/candidate PASS receipt.
    """
    predecessor = invocation.plan.get("preparation_predecessor_profile_sha256")
    if predecessor is None:
        return {}  # Existing legacy BUILD has no private preparation lineage.
    if (
        invocation.stage != "prepare"
        or isinstance(source, ArtifactReadyPreparationBuildSource)
        or not isinstance(source, ArtifactReadyBuildSource)
        or set(source.component_manifests) != set(Component)
        or source.cutoff.isoformat() != invocation.build_inputs.get("cutoff")
        or source.profile.semantic_profile_digest != invocation.profile.semantic_profile_digest
        or re.fullmatch(r"dmr_[0-9a-f]{32}", invocation.run_id) is None
        or type(invocation.attempt_fence) is not int
        or invocation.attempt_fence < 1
    ):
        raise ComponentPreparationError("final-source preparation adoption binding differs")
    root = _plain_path(invocation.candidate_root, root=invocation.candidate_root, directory=True)
    staging = _plain_path(invocation.staging_root, root=root, directory=True)
    if staging.parent.name != ".staging" or staging.parent.parent != root:
        raise ComponentPreparationError("final preparation adoption staging differs")
    records = root / ".staging" / "preparation-records" / invocation.run_id
    if not records.exists():
        return {}
    _plain_path(records, root=root, directory=True)
    context = ProducerContext("BUILD", invocation.run_id, invocation.attempt_fence, {}, {}, {})
    result = {}
    for component, (logical, directory) in _PHYSICAL.items():
        checkpoint()
        # An already-proven unchanged baseline is cheaper than recopying the
        # private full component. Incremental/selective plans keep their own
        # mutation/lineage validation and cannot be replaced by this shortcut.
        actions = {item["component"]: item["action"] for item in invocation.plan["actions"]}
        if actions.get(component.value) != ComponentAction.FULL_REBUILD.value:
            continue
        identity = physical_preparation_identity(
            source=source,
            component=component,
            predecessor_profile_sha256=str(predecessor),
        )
        verified = MonthlyPrivatePhysicalPreparationExecutor._recover(
            root=root,
            records=records,
            context=context,
            identity=identity,
        )
        if verified is None:
            continue
        record, receipt = verified.record, verified.receipt
        destination = staging / directory
        _copy_pinned_component(root=root, destination=destination, verified=verified, checkpoint=checkpoint)
        result[component] = {
            "schema_version": "aistock_monthly_preparation_adoption_v1",
            "operation_id": invocation.run_id,
            "component": logical,
            "identity_digest": identity.digest,
            "prepared_receipt_ref": record["receipt_ref"],
            "full_artifact_ready_content_root": source.artifact_ready_content_root,
            "full_source_content_root": source.source_content_root,
            "output_count": len(receipt["output_refs"]),
            "adopted_bytes": sum(ref["size"] for ref in receipt["output_refs"]),
            "full_candidate_validation_required": True,
            "publication_allowed": False,
        }
    return result
