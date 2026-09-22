"""Durable worker and registered producer boundary for monthly releases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Callable, Mapping, Protocol, Sequence

from .monthly_unified import (
    ActionAuthorizationStore,
    COMPONENTS,
    CONSUMER_READBACK_SCHEMA,
    NODE_REGISTRATION_SCHEMA,
    RELEASE_CLOSURE_SCHEMA,
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
    SOURCE_GATES,
    STAGES,
    MonthlyPipeline,
    MonthlyReleaseError,
    MonthlyReleaseService,
    MonthlyReleaseSourceBlocked,
    SourceChange,
    build_stage_receipt,
    classify_component_actions,
)
from .canonical import canonical_json_bytes


PRODUCER_EVIDENCE_SCHEMA = "aistock_monthly_release_producer_evidence_v1"


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _require_plain_path_under(root: Path, path: Path, *, label: str) -> None:
    requested = path.expanduser().absolute()
    try:
        relative = requested.relative_to(root)
    except ValueError as exc:
        raise MonthlyProducerError(f"{label} artifact escaped registered roots") from exc
    current = root
    if _is_link_or_junction(current):
        raise MonthlyProducerError(f"{label} registered root is linked")
    for part in relative.parts:
        current = current / part
        if _is_link_or_junction(current):
            raise MonthlyProducerError(f"{label} artifact traverses a link or junction")


def _is_content_ref(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"id", "sha256", "size"}:
        return False
    ref_id = str(value.get("id") or "")
    digest = str(value.get("sha256") or "")
    return bool(
        ref_id
        and not Path(ref_id).is_absolute()
        and ".." not in Path(ref_id).parts
        and "\\" not in ref_id
        and len(digest) == 64
        and all(char in "0123456789abcdef" for char in digest)
        and type(value.get("size")) is int
        and value["size"] >= 0
    )


class MonthlyProducerError(MonthlyReleaseError):
    code = "MONTHLY_RELEASE_PRODUCER_INVALID"


@dataclass(frozen=True, slots=True)
class ProducerContext:
    stage: str
    operation_id: str
    attempt: int
    request: Mapping[str, Any]
    plan: Mapping[str, Any]
    prior_receipts: Mapping[str, Mapping[str, Any]]


class StageProducer(Protocol):
    producer_id: str
    producer_version: str

    def produce(self, context: ProducerContext) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class SubprocessStageProducer:
    """Run one registered producer command; the API caller cannot supply it."""

    producer_id: str
    producer_version: str
    command: tuple[str, ...]
    operation_state_root: Path
    project_root: Path
    environment_allowlist: tuple[str, ...] = ()
    timeout_seconds: int = 6 * 60 * 60

    def __post_init__(self) -> None:
        if not self.command or not Path(self.command[0]).is_absolute():
            raise ValueError("registered producer executable must be absolute")
        if not Path(self.command[0]).is_file():
            raise ValueError("registered producer executable is unavailable")
        if not self.operation_state_root.is_absolute() or not self.project_root.is_absolute():
            raise ValueError("registered producer roots must be absolute")
        if _is_link_or_junction(self.project_root) or not self.project_root.is_dir():
            raise ValueError("registered producer project root must be a regular directory")
        if self.timeout_seconds < 60:
            raise ValueError("registered producer timeout is too small")

    def contract_identity(self) -> Mapping[str, Any]:
        """Bind resume checkpoints to the executable command and script bytes."""

        file_refs: list[dict[str, Any]] = []
        for token in self.command:
            path = Path(token)
            if not path.is_absolute() or not path.is_file() or _is_link_or_junction(path):
                continue
            file_refs.append(
                {
                    "path": str(path.resolve(strict=True)),
                    "sha256": _file_sha256(path),
                    "size": path.stat().st_size,
                }
            )
        return {
            "id": self.producer_id,
            "version": self.producer_version,
            "command": list(self.command),
            "file_refs": file_refs,
            "environment_allowlist": list(self.environment_allowlist),
            "timeout_seconds": self.timeout_seconds,
        }

    def produce(self, context: ProducerContext) -> Mapping[str, Any]:
        operation_root = self.operation_state_root / "monthly" / context.operation_id
        exchange = operation_root / "producer-exchange" / f"attempt-{context.attempt}"
        exchange.mkdir(parents=True, exist_ok=True)
        context_path = exchange / f"{context.stage.lower()}-context.json"
        output_path = exchange / f"{context.stage.lower()}-evidence.json"
        if output_path.exists():
            raise MonthlyProducerError("producer evidence target already exists")
        payload = {
            "schema_version": "aistock_monthly_release_producer_context_v1",
            "stage": context.stage,
            "operation_id": context.operation_id,
            "attempt": context.attempt,
            "request": dict(context.request),
            "plan": dict(context.plan),
            "prior_receipt_digests": {
                name: str(receipt["canonical_sha256"]) for name, receipt in sorted(context.prior_receipts.items())
            },
        }
        with context_path.open("xb") as handle:
            handle.write(canonical_json_bytes(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        values = {"context": str(context_path), "output": str(output_path), "stage": context.stage}
        command = [part.format_map(values) for part in self.command]
        environment = {
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "SYSTEMROOT", "WINDIR", "PYTHONPATH", *self.environment_allowlist}
        }
        completed = subprocess.run(
            command,
            cwd=self.project_root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.timeout_seconds,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        (exchange / f"{context.stage.lower()}-stdout.log").write_text(
            completed.stdout[-1024 * 1024 :], encoding="utf-8"
        )
        (exchange / f"{context.stage.lower()}-stderr.log").write_text(
            completed.stderr[-1024 * 1024 :], encoding="utf-8"
        )
        if completed.returncode != 0:
            raise MonthlyProducerError(
                "registered producer failed",
                context={"stage": context.stage, "returncode": completed.returncode},
            )
        try:
            value = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MonthlyProducerError("registered producer evidence is unreadable") from exc
        if not isinstance(value, Mapping):
            raise MonthlyProducerError("registered producer evidence must be an object")
        return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_refs(value: Any, *, roots: Sequence[Path], label: str) -> tuple[list[dict[str, Any]], dict[str, Path]]:
    if not isinstance(value, list):
        raise MonthlyProducerError(f"{label} must be a list")
    resolved_roots = tuple(root.resolve(strict=True) for root in roots if root.exists())
    result: list[dict[str, Any]] = []
    paths: dict[str, Path] = {}
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {"id", "path"}:
            raise MonthlyProducerError(f"{label} reference fields differ")
        path = Path(str(item["path"]))
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise MonthlyProducerError(f"{label} artifact is unreadable") from exc
        if _is_link_or_junction(path) or not resolved.is_file():
            raise MonthlyProducerError(f"{label} artifact must be a regular non-link file")
        matching_roots = [root for root in resolved_roots if resolved.is_relative_to(root)]
        if resolved_roots and not matching_roots:
            raise MonthlyProducerError(f"{label} artifact escaped registered roots")
        ref_id = str(item["id"]).replace("\\", "/")
        if Path(ref_id).is_absolute() or ".." in Path(ref_id).parts or not ref_id:
            raise MonthlyProducerError(f"{label} artifact id is not portable")
        if matching_roots:
            root = max(matching_roots, key=lambda value: len(value.parts))
            _require_plain_path_under(root, path, label=label)
            if ref_id != resolved.relative_to(root).as_posix():
                raise MonthlyProducerError(f"{label} artifact id differs from its registered-root path")
        if ref_id in paths:
            raise MonthlyProducerError(f"{label} artifact ids are duplicated")
        paths[ref_id] = resolved
        result.append({"id": ref_id, "sha256": _file_sha256(resolved), "size": resolved.stat().st_size})
    return result, paths


def _canonical_json_artifact(path: Path, *, label: str) -> Mapping[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyProducerError(f"{label} artifact is not readable JSON") from exc
    if not isinstance(value, Mapping):
        raise MonthlyProducerError(f"{label} artifact must be an object")
    if raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyProducerError(f"{label} artifact bytes are not canonical JSON")
    return value


def _validate_semantics(
    stage: str,
    evidence: Mapping[str, Any],
    plan: Mapping[str, Any],
    prior_receipts: Mapping[str, Mapping[str, Any]],
    input_refs: Sequence[Mapping[str, Any]],
    output_refs: Sequence[Mapping[str, Any]],
    output_paths: Mapping[str, Path],
) -> None:
    scope = evidence.get("scope")
    counts = evidence.get("counts")
    if not isinstance(scope, Mapping) or not isinstance(counts, Mapping):
        raise MonthlyProducerError("producer evidence scope/counts are invalid")
    if type(counts.get("unexplained_gap_count")) is not int:
        raise MonthlyProducerError("producer unexplained gap count is invalid")
    if counts["unexplained_gap_count"] != 0:
        error_type = MonthlyReleaseSourceBlocked if stage == "SOURCE" else MonthlyProducerError
        raise error_type("producer evidence contains unexplained gaps")
    outputs_by_id = {str(ref["id"]): dict(ref) for ref in output_refs}
    if len(outputs_by_id) != len(output_refs):
        raise MonthlyProducerError("producer output references are duplicated")

    def require_pinned(ref: Any, *, field: str) -> Mapping[str, Any]:
        if not _is_content_ref(ref):
            raise MonthlyProducerError(f"{field} is not a complete content reference")
        normalized = dict(ref)
        if outputs_by_id.get(str(normalized["id"])) != normalized:
            raise MonthlyProducerError(f"{field} is not pinned by the producer output bytes")
        return normalized

    def require_json(
        ref: Any,
        *,
        field: str,
        expected: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        normalized = require_pinned(ref, field=field)
        path = output_paths.get(str(normalized["id"]))
        if path is None:
            raise MonthlyProducerError(f"{field} output path is unavailable")
        value = _canonical_json_artifact(path, label=field)
        if expected is not None and dict(value) != dict(expected):
            raise MonthlyProducerError(f"{field} artifact content differs from its readback")
        return value

    def require_pinned_list(value: Any, *, field: str) -> list[Mapping[str, Any]]:
        if not isinstance(value, list) or not value:
            raise MonthlyProducerError(f"{field} must contain content references")
        return [require_pinned(item, field=field) for item in value]

    def require_json_list(value: Any, *, field: str) -> list[Mapping[str, Any]]:
        if not isinstance(value, list) or not value:
            raise MonthlyProducerError(f"{field} must contain content references")
        return [require_json(item, field=field) for item in value]

    if stage == "SOURCE":
        gates = scope.get("gates")
        changes = scope.get("changes")
        if set(gates or ()) != set(SOURCE_GATES) or not isinstance(changes, list):
            raise MonthlyProducerError("source evidence does not close gates and changes")
        if type(scope.get("database_read_performed")) is not bool or scope.get("database_write_performed") is not False:
            raise MonthlyProducerError("source database access disclosure is incomplete")
        try:
            source_as_of = datetime.fromisoformat(str(scope.get("source_as_of") or ""))
        except ValueError as exc:
            raise MonthlyProducerError("source source_as_of is invalid") from exc
        if source_as_of.tzinfo is None:
            raise MonthlyProducerError("source source_as_of must include a timezone")
        if not str(scope.get("snapshot_group_id") or "").strip():
            raise MonthlyProducerError("source snapshot_group_id is empty")
        if scope.get("consistent_input_set_complete") is not True:
            raise MonthlyProducerError("source input set did not close one consistent snapshot")
        snapshot_evidence = require_json_list(scope.get("snapshot_identity_refs"), field="snapshot_identity_refs")
        if len(snapshot_evidence) != 1 or snapshot_evidence[0] != {
            "schema_version": "aistock_monthly_source_snapshot_identity_v1",
            "snapshot_group_id": scope["snapshot_group_id"],
            "snapshot_id": snapshot_evidence[0].get("snapshot_id"),
            "source_as_of": scope["source_as_of"],
            "repair_watermark": snapshot_evidence[0].get("repair_watermark"),
        }:
            raise MonthlyProducerError("source snapshot identity artifact differs")
        if (
            not str(snapshot_evidence[0].get("snapshot_id") or "").strip()
            or not str(snapshot_evidence[0].get("repair_watermark") or "").strip()
        ):
            raise MonthlyProducerError("source snapshot identity is incomplete")
        repair_evidence = require_json_list(scope.get("repair_overlap_check_refs"), field="repair_overlap_check_refs")
        if len(repair_evidence) != 1 or repair_evidence[0] != {
            "schema_version": "aistock_monthly_repair_overlap_check_v1",
            "snapshot_group_id": scope["snapshot_group_id"],
            "initial_repair_watermark": snapshot_evidence[0]["repair_watermark"],
            "overlapping_repair_ids": [],
            "status": "PASS",
        }:
            raise MonthlyProducerError("source repair overlap artifact differs")
        change_evidence = require_json_list(scope.get("change_scope_refs"), field="change_scope_refs")
        if len(change_evidence) != 1 or change_evidence[0] != {
            "schema_version": "aistock_monthly_source_change_scope_v1",
            "changes": changes,
            "component_actions": scope.get("component_actions"),
        }:
            raise MonthlyProducerError("source change scope artifact differs")
        gate_evidence = require_json_list(scope.get("source_gate_refs"), field="source_gate_refs")
        if len(gate_evidence) != len(SOURCE_GATES):
            raise MonthlyProducerError("source gate artifact coverage differs")
        observed_gates: set[str] = set()
        pinned_artifact_ids = {str(item["id"]) for item in (*input_refs, *output_refs)}
        for gate in gate_evidence:
            if (
                set(gate)
                != {
                    "schema_version",
                    "gate_id",
                    "snapshot_group_id",
                    "expectation_contract_ref",
                    "readback_ref",
                    "expected_count",
                    "observed_count",
                    "explained_missing_count",
                    "unexplained_missing_count",
                    "duplicate_count",
                    "invalid_value_count",
                    "status",
                    "exception_refs",
                }
                or gate.get("schema_version") != "aistock_monthly_source_gate_v2"
            ):
                raise MonthlyProducerError("source gate artifact schema differs")
            gate_id = str(gate.get("gate_id") or "")
            count_fields = (
                "expected_count",
                "observed_count",
                "explained_missing_count",
                "unexplained_missing_count",
                "duplicate_count",
                "invalid_value_count",
            )
            if (
                gate_id not in SOURCE_GATES
                or gate_id in observed_gates
                or gate.get("snapshot_group_id") != scope["snapshot_group_id"]
                or not str(gate.get("expectation_contract_ref") or "").strip()
                or not str(gate.get("readback_ref") or "").strip()
                or gate.get("expectation_contract_ref") not in pinned_artifact_ids
                or gate.get("readback_ref") not in pinned_artifact_ids
                or gate.get("status") != "PASS"
                or any(type(gate.get(name)) is not int or gate[name] < 0 for name in count_fields)
                or gate["observed_count"] + gate["explained_missing_count"] + gate["unexplained_missing_count"]
                != gate["expected_count"]
                or gate["unexplained_missing_count"] != 0
                or gate["duplicate_count"] != 0
                or gate["invalid_value_count"] != 0
                or not isinstance(gate.get("exception_refs"), list)
                or bool(gate["explained_missing_count"]) != bool(gate["exception_refs"])
            ):
                raise MonthlyProducerError("source gate artifact did not close its domain")
            observed_gates.add(gate_id)
        if observed_gates != set(SOURCE_GATES):
            raise MonthlyProducerError("source gate artifact identities differ")
        contract_evidence = require_json_list(scope.get("producer_contract_refs"), field="producer_contract_refs")
        if any(
            set(item) != {"schema_version", "producer_id", "producer_version", "contract_sha256"}
            or item.get("schema_version") != "aistock_monthly_source_producer_contract_v1"
            or not str(item.get("producer_id") or "").strip()
            or not str(item.get("producer_version") or "").strip()
            or len(str(item.get("contract_sha256") or "")) != 64
            or any(char not in "0123456789abcdef" for char in str(item.get("contract_sha256") or ""))
            for item in contract_evidence
        ):
            raise MonthlyProducerError("source producer contract artifact differs")
        repair_receipts = scope.get("repair_receipts")
        if not isinstance(repair_receipts, list):
            raise MonthlyProducerError("source repair receipt index is missing")
        allowed_repairs = set(str(value) for value in plan.get("repair_authorization_refs", ()))
        for repair in repair_receipts:
            if not isinstance(repair, Mapping) or set(repair) != {
                "repair_id",
                "target",
                "dev_validation_sha256",
                "authorization_ref",
                "apply_sha256",
                "readback_sha256",
                "rows_modified",
            }:
                raise MonthlyProducerError("source repair receipt fields differ")
            for field in ("dev_validation_sha256", "apply_sha256", "readback_sha256"):
                digest = str(repair[field] or "")
                if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                    raise MonthlyProducerError("source repair receipt hash is invalid")
            if repair["target"] == "PRODUCTION" and repair["authorization_ref"] not in allowed_repairs:
                raise MonthlyProducerError("production repair lacks request-scoped authorization")
            if repair["target"] not in {"DEV", "PRODUCTION"} or type(repair["rows_modified"]) is not int:
                raise MonthlyProducerError("source repair receipt target/count is invalid")
        actions = scope.get("component_actions")
        if not isinstance(actions, Mapping) or set(actions) != set(COMPONENTS):
            raise MonthlyProducerError("source evidence lacks the exact component action plan")
        parsed_changes: list[SourceChange] = []
        try:
            for item in changes:
                if not isinstance(item, Mapping):
                    raise TypeError("change is not an object")
                parsed_changes.append(
                    SourceChange(
                        dataset=str(item["dataset"]),
                        fields=tuple(str(value) for value in item["fields"]),
                        instruments=tuple(str(value) for value in item["instruments"]),
                        start=date.fromisoformat(str(item["start"])),
                        end=date.fromisoformat(str(item["end"])),
                        kind=str(item["kind"]),
                        source_receipt_sha256=str(item["source_receipt_sha256"]),
                    )
                )
        except (KeyError, TypeError, ValueError) as exc:
            raise MonthlyProducerError("source change evidence is invalid") from exc
        if dict(actions) != classify_component_actions(parsed_changes):
            raise MonthlyProducerError("source component actions were not derived from changes")
    else:
        manifest = str(scope.get("dataset_manifest_sha256") or "")
        if len(manifest) != 64 or any(char not in "0123456789abcdef" for char in manifest):
            raise MonthlyProducerError("producer evidence manifest identity is invalid")
    if stage == "BUILD":
        actions = scope.get("component_actions")
        if not isinstance(actions, Mapping) or set(actions) != set(COMPONENTS):
            raise MonthlyProducerError("build evidence component actions differ")
        source = plan.get("predecessor")
        if not isinstance(source, Mapping) or scope.get("predecessor_manifest_sha256") != source.get(
            "dataset_manifest_sha256"
        ):
            raise MonthlyProducerError("build evidence predecessor identity differs")
        source_receipt = prior_receipts.get("SOURCE")
        if not isinstance(source_receipt, Mapping) or scope.get("component_actions") != source_receipt.get(
            "scope", {}
        ).get("component_actions"):
            raise MonthlyProducerError("build actions differ from the frozen source plan")
        manifest = require_json(scope.get("dataset_manifest_ref"), field="dataset_manifest_ref")
        if manifest.get("dataset_manifest_sha256") != scope.get("dataset_manifest_sha256"):
            raise MonthlyProducerError("dataset manifest content identity differs")
    elif stage == "DERIVE":
        if scope.get("hmm_fit_count") != 0 or scope.get("training_started") is not False:
            raise MonthlyProducerError("derive producer attempted training")
        derived_assets = require_pinned_list(scope.get("derived_assets"), field="derived_assets")
        registry_ref = require_pinned(scope.get("derived_asset_registry_ref"), field="derived_asset_registry_ref")
        if scope.get("source_dataset_manifest_sha256") != scope.get("dataset_manifest_sha256"):
            raise MonthlyProducerError("derived assets target another dataset manifest")
        digest = str(scope.get("derived_asset_registry_sha256") or "")
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise MonthlyProducerError("derived asset registry identity is invalid")
        if registry_ref["sha256"] != digest:
            raise MonthlyProducerError("derived asset registry hash differs from output bytes")
        registry = require_json(scope.get("derived_asset_registry_ref"), field="derived_asset_registry_ref")
        if registry.get("schema_version") != "aistock_dataset_derived_asset_registry_v1" or registry.get(
            "source_dataset_manifest_sha256"
        ) != scope.get("dataset_manifest_sha256"):
            raise MonthlyProducerError("derived asset registry content differs")
        registry_assets = registry.get("assets")
        if not isinstance(registry_assets, list) or not registry_assets:
            raise MonthlyProducerError("derived asset registry is empty")
        registered_assets: list[dict[str, Any]] = []
        asset_ids: set[str] = set()
        for asset in registry_assets:
            if not isinstance(asset, Mapping) or set(asset) != {
                "asset_id",
                "path",
                "sha256",
                "size",
                "schema_version",
            }:
                raise MonthlyProducerError("derived asset registry row fields differ")
            asset_id = str(asset.get("asset_id") or "")
            if not asset_id or asset_id in asset_ids or not str(asset.get("schema_version") or "").strip():
                raise MonthlyProducerError("derived asset registry identity differs")
            asset_ids.add(asset_id)
            registered_assets.append(
                {
                    "path": str(asset.get("path") or ""),
                    "sha256": str(asset.get("sha256") or ""),
                    "size": asset.get("size"),
                }
            )
        candidate_root = Path(str(plan.get("candidate_root") or ""))
        if not candidate_root.is_absolute():
            raise MonthlyProducerError("derived asset candidate root is invalid")
        try:
            resolved_candidate = candidate_root.resolve(strict=True)
        except OSError as exc:
            raise MonthlyProducerError("derived asset candidate root is unavailable") from exc
        registry_path = output_paths.get(str(registry_ref["id"]))
        if registry_path is None:
            raise MonthlyProducerError("derived asset registry output path is unavailable")
        registry_root = registry_path.parent.resolve(strict=True)
        if not registry_root.is_relative_to(resolved_candidate):
            raise MonthlyProducerError("derived asset registry escaped the candidate")
        unmatched = [dict(item) for item in derived_assets]
        for asset in registered_assets:
            relative = Path(asset["path"])
            if relative.is_absolute() or not relative.parts or ".." in relative.parts:
                raise MonthlyProducerError("derived asset registry path is invalid")
            matches = [
                ref
                for ref in unmatched
                if ref["sha256"] == asset["sha256"] and ref["size"] == asset["size"]
            ]
            if len(matches) != 1:
                raise MonthlyProducerError(
                    "derived asset registry files differ from producer outputs"
                )
            matched = matches[0]
            output_path = output_paths.get(str(matched["id"]))
            expected_path = registry_root / relative
            if output_path is None or output_path.resolve(strict=True) != expected_path:
                raise MonthlyProducerError("derived asset registry candidate path differs")
            unmatched.remove(matched)
        if unmatched:
            raise MonthlyProducerError("derived asset registry files differ from producer outputs")
    elif stage == "LOCAL_VALIDATE":
        required = {
            "stock_universe",
            "csi300",
            "csi500",
            "csi1000",
            "star50",
            "star100",
        }
        pools = scope.get("pool_gap_counts")
        if not isinstance(pools, Mapping) or set(pools) != required or any(pools[name] != 0 for name in required):
            raise MonthlyProducerError("local validation does not close all six PIT pools")
        if scope.get("dataset_identity_complete") is not True:
            raise MonthlyProducerError("local dataset identity is incomplete")
        closure = scope.get("release_closure")
        if not isinstance(closure, Mapping) or closure.get("schema_version") != RELEASE_CLOSURE_SCHEMA:
            raise MonthlyProducerError("release closure schema differs")
        required_closure = {
            "schema_version",
            "dataset_manifest_ref",
            "derived_asset_refs",
            "consumer_contract_refs",
            "source_readiness_refs",
            "component_validation_refs",
            "lineage_ref",
            "canonical_sha256",
        }
        if set(closure) != required_closure:
            raise MonthlyProducerError("release closure fields differ")
        if not _is_content_ref(closure.get("dataset_manifest_ref")):
            raise MonthlyProducerError("release closure manifest reference is invalid")
        for field in (
            "derived_asset_refs",
            "consumer_contract_refs",
            "source_readiness_refs",
            "component_validation_refs",
        ):
            value = closure.get(field)
            if not isinstance(value, list) or not value or any(not _is_content_ref(item) for item in value):
                raise MonthlyProducerError(f"release closure {field} is invalid")
        if not _is_content_ref(closure.get("lineage_ref")):
            raise MonthlyProducerError("release closure lineage reference is invalid")
        unsigned = dict(closure)
        claimed = str(unsigned.pop("canonical_sha256") or "")
        if hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest() != claimed:
            raise MonthlyProducerError("release closure canonical digest differs")
        closure_ref = require_pinned(scope.get("release_closure_ref"), field="release_closure_ref")
        if closure_ref["sha256"] != str(scope.get("release_closure_file_sha256") or ""):
            raise MonthlyProducerError("release closure hash differs from output bytes")
        require_json(
            scope.get("release_closure_ref"),
            field="release_closure_ref",
            expected=closure,
        )
        candidate_root = Path(str(plan.get("candidate_root") or ""))
        if not candidate_root.is_absolute():
            raise MonthlyProducerError("release closure candidate root is invalid")
        try:
            resolved_candidate = candidate_root.resolve(strict=True)
        except OSError as exc:
            raise MonthlyProducerError("release closure candidate root is unavailable") from exc
        closure_path = output_paths.get(str(closure_ref["id"]))
        expected_closure = resolved_candidate / "release_closure_receipt.json"
        if closure_path is None or closure_path.resolve(strict=True) != expected_closure:
            raise MonthlyProducerError("release closure is not candidate-local")

        def validate_candidate_ref(
            raw: Any,
            *,
            field: str,
            require_provenance: bool = False,
        ) -> str:
            if not _is_content_ref(raw):
                raise MonthlyProducerError(f"release closure {field} reference is invalid")
            relative = Path(str(raw["id"]))
            if relative.is_absolute() or not relative.parts or ".." in relative.parts:
                raise MonthlyProducerError(f"release closure {field} path is invalid")
            target = resolved_candidate / relative
            _require_plain_path_under(resolved_candidate, target, label=f"release closure {field}")
            resolved = target.resolve(strict=True)
            if require_provenance and not resolved.is_relative_to(
                resolved_candidate / "provenance"
            ):
                raise MonthlyProducerError(
                    f"release closure {field} must be candidate-local provenance"
                )
            if _file_sha256(resolved) != raw["sha256"] or resolved.stat().st_size != raw["size"]:
                raise MonthlyProducerError(f"release closure {field} bytes differ")
            return relative.as_posix()

        if validate_candidate_ref(
            closure["dataset_manifest_ref"], field="dataset manifest"
        ) != "qe_dataset_manifest.json":
            raise MonthlyProducerError("release closure manifest path differs")
        for raw in closure["derived_asset_refs"]:
            validate_candidate_ref(raw, field="derived asset")
        for field in (
            "consumer_contract_refs",
            "source_readiness_refs",
            "component_validation_refs",
        ):
            for raw in closure[field]:
                validate_candidate_ref(raw, field=field, require_provenance=True)
        validate_candidate_ref(
            closure["lineage_ref"], field="lineage", require_provenance=True
        )
    elif stage == "DEPLOY":
        nodes = scope.get("nodes")
        node_hashes = scope.get("node_manifest_sha256")
        if set(nodes or ()) != set(REQUIRED_NODES) or not isinstance(node_hashes, Mapping):
            raise MonthlyProducerError("deployment evidence nodes differ")
        if set(node_hashes) != set(REQUIRED_NODES) or len(set(node_hashes.values())) != 1:
            raise MonthlyProducerError("deployment node manifest identities differ")
        registrations = scope.get("node_registrations")
        if not isinstance(registrations, Mapping) or set(registrations) != set(REQUIRED_NODES):
            raise MonthlyProducerError("node release registrations differ")
        for node_id, registration in registrations.items():
            registration_fields = {
                "schema_version",
                "node_id",
                "release_id",
                "dataset_manifest_ref",
                "closure_ref",
                "candidate_root",
                "relative_file_refs",
                "deployment_receipt_ref",
            }
            if not isinstance(registration, Mapping) or registration.get("schema_version") != NODE_REGISTRATION_SCHEMA:
                raise MonthlyProducerError(f"node release registration schema differs: {node_id}")
            if set(registration) != registration_fields:
                raise MonthlyProducerError(f"node release registration fields differ: {node_id}")
            if registration.get("node_id") != node_id or registration.get("release_id") != plan.get("release_id"):
                raise MonthlyProducerError(f"node release registration identity differs: {node_id}")
            for field in ("dataset_manifest_ref", "closure_ref", "deployment_receipt_ref"):
                if not _is_content_ref(registration.get(field)):
                    raise MonthlyProducerError(f"node release registration {field} is invalid: {node_id}")
            refs = registration.get("relative_file_refs")
            if not isinstance(refs, list) or not refs or any(not _is_content_ref(item) for item in refs):
                raise MonthlyProducerError(f"node release registration files differ: {node_id}")
            if not str(registration.get("candidate_root") or ""):
                raise MonthlyProducerError(f"node release registration root is empty: {node_id}")
        registration_refs = scope.get("node_registration_refs")
        if not isinstance(registration_refs, Mapping) or set(registration_refs) != set(REQUIRED_NODES):
            raise MonthlyProducerError("node release registration references differ")
        for node_id, ref in registration_refs.items():
            require_json(
                ref,
                field=f"node_registration_refs.{node_id}",
                expected=registrations[node_id],
            )
    elif stage == "CONSUMER_VALIDATE":
        consumers = scope.get("consumers")
        readbacks = scope.get("consumer_readbacks")
        if set(consumers or ()) != set(REQUIRED_CONSUMERS) or not isinstance(readbacks, Mapping):
            raise MonthlyProducerError("consumer evidence coverage differs")
        if set(readbacks) != set(REQUIRED_CONSUMERS):
            raise MonthlyProducerError("consumer readback set differs")
        for name, readback in readbacks.items():
            readback_fields = {
                "schema_version",
                "consumer_id",
                "node_id",
                "binding_ref",
                "required_window",
                "resolved_component_refs",
                "derived_asset_refs",
                "coverage_counts",
                "command_or_adapter_version",
                "result_ref",
                "side_effect_flags",
                "dataset_manifest_sha256",
            }
            if not isinstance(readback, Mapping) or readback.get("schema_version") != CONSUMER_READBACK_SCHEMA:
                raise MonthlyProducerError(f"consumer readback schema differs: {name}")
            if set(readback) != readback_fields:
                raise MonthlyProducerError(f"consumer readback fields differ: {name}")
            if readback.get("consumer_id") != name or readback.get("dataset_manifest_sha256") != scope.get(
                "dataset_manifest_sha256"
            ):
                raise MonthlyProducerError(f"consumer identity differs: {name}")
            if not str(readback.get("node_id") or "") or not _is_content_ref(readback.get("binding_ref")):
                raise MonthlyProducerError(f"consumer binding readback differs: {name}")
            for field in ("resolved_component_refs", "derived_asset_refs"):
                refs = readback.get(field)
                if not isinstance(refs, list) or not refs or any(not _is_content_ref(item) for item in refs):
                    raise MonthlyProducerError(f"consumer {field} differs: {name}")
            if not isinstance(readback.get("required_window"), Mapping) or not isinstance(
                readback.get("coverage_counts"), Mapping
            ):
                raise MonthlyProducerError(f"consumer coverage readback differs: {name}")
            if not str(readback.get("command_or_adapter_version") or "") or not _is_content_ref(
                readback.get("result_ref")
            ):
                raise MonthlyProducerError(f"consumer result readback differs: {name}")
            side_effects = readback.get("side_effect_flags")
            if not isinstance(side_effects, Mapping) or any(
                side_effects.get(flag) is not False
                for flag in ("outcomes_read", "training_started", "experiment_started", "runtime_action_performed")
            ):
                raise MonthlyProducerError(f"consumer side effects differ: {name}")
        readback_refs = scope.get("consumer_readback_refs")
        if not isinstance(readback_refs, Mapping) or set(readback_refs) != set(REQUIRED_CONSUMERS):
            raise MonthlyProducerError("consumer readback references differ")
        for name, ref in readback_refs.items():
            require_json(
                ref,
                field=f"consumer_readback_refs.{name}",
                expected=readbacks[name],
            )
        forbidden = ("outcomes_read", "training_started", "experiment_started", "runtime_action_performed")
        if any(scope.get(key) is not False for key in forbidden):
            raise MonthlyProducerError("consumer validation performed a forbidden action")


class RegisteredMonthlyPipeline(MonthlyPipeline):
    """Invoke only code-registered producers and derive receipts from artifacts."""

    def __init__(self, producers: Mapping[str, StageProducer], *, artifact_roots: Sequence[Path]) -> None:
        if set(producers) != set(STAGES):
            raise ValueError("producer registry must cover all monthly stages exactly")
        roots: list[Path] = []
        for raw_root in artifact_roots:
            if not raw_root.is_absolute() or _is_link_or_junction(raw_root) or not raw_root.is_dir():
                raise ValueError("artifact roots must be existing regular absolute directories")
            roots.append(raw_root.resolve(strict=True))
        if not roots or len(set(roots)) != len(roots):
            raise ValueError("artifact roots must be non-empty and unique")
        if any(
            left != right and (left.is_relative_to(right) or right.is_relative_to(left))
            for index, left in enumerate(roots)
            for right in roots[index + 1 :]
        ):
            raise ValueError("artifact roots must not overlap")
        self.producers = dict(producers)
        self.artifact_roots = tuple(roots)

    def stage_identity(self, stage: str) -> str:
        producer = self.producers[stage]
        identity_reader = getattr(producer, "contract_identity", None)
        identity = (
            dict(identity_reader())
            if callable(identity_reader)
            else {"id": producer.producer_id, "version": producer.producer_version}
        )
        return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()

    def checkpoint_is_valid(self, stage: str, receipt: Mapping[str, Any]) -> bool:
        del stage
        for group in ("input_refs", "output_refs"):
            for ref in receipt.get(group, []):
                relative = Path(str(ref["id"]))
                matches = [(root, root / relative) for root in self.artifact_roots if (root / relative).is_file()]
                if len(matches) != 1:
                    return False
                root, path = matches[0]
                try:
                    _require_plain_path_under(root, path, label="checkpoint")
                except MonthlyProducerError:
                    return False
                if (
                    _is_link_or_junction(path)
                    or path.stat().st_size != ref["size"]
                    or _file_sha256(path) != ref["sha256"]
                ):
                    return False
        return True

    def run_stage(
        self,
        *,
        stage: str,
        operation_id: str,
        attempt: int,
        request: Mapping[str, Any],
        plan: Mapping[str, Any],
        prior_receipts: Mapping[str, Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        producer = self.producers[stage]
        started = datetime.now(UTC).isoformat()
        evidence = dict(
            producer.produce(
                ProducerContext(
                    stage=stage,
                    operation_id=operation_id,
                    attempt=attempt,
                    request=request,
                    plan=plan,
                    prior_receipts=prior_receipts,
                )
            )
        )
        if evidence.get("schema_version") != PRODUCER_EVIDENCE_SCHEMA or set(evidence) != {
            "schema_version",
            "scope",
            "input_artifacts",
            "output_artifacts",
            "counts",
            "errors",
        }:
            raise MonthlyProducerError("producer evidence fields differ")
        if evidence["errors"]:
            error_type = MonthlyReleaseSourceBlocked if stage == "SOURCE" else MonthlyProducerError
            raise error_type("producer evidence contains errors")
        input_refs, _input_paths = _artifact_refs(evidence["input_artifacts"], roots=self.artifact_roots, label="input")
        output_refs, output_paths = _artifact_refs(
            evidence["output_artifacts"], roots=self.artifact_roots, label="output"
        )
        if not output_refs:
            raise MonthlyProducerError("producer evidence has no output artifact")
        _validate_semantics(
            stage,
            evidence,
            plan,
            prior_receipts,
            input_refs,
            output_refs,
            output_paths,
        )
        return build_stage_receipt(
            operation_id=operation_id,
            attempt_id=f"{operation_id}:{attempt}",
            stage=stage,
            producer_id=producer.producer_id,
            producer_version=producer.producer_version,
            scope=dict(evidence["scope"]),
            input_refs=input_refs,
            output_refs=output_refs,
            counts={str(key): int(value) for key, value in evidence["counts"].items()},
            started_at=started,
            finished_at=datetime.now(UTC).isoformat(),
        )


class MonthlyReleaseWorker:
    """Poll the durable store; process at most one operation per call."""

    def __init__(
        self,
        service: MonthlyReleaseService,
        *,
        authorization_store: ActionAuthorizationStore | None = None,
        activation_verifier: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    ) -> None:
        self.service = service
        self.authorization_store = authorization_store
        self.activation_verifier = activation_verifier

    def run_once(self) -> dict[str, Any] | None:
        pending = self.service.store.pending_operation_ids()
        if not pending:
            return None
        operation_id = pending[0]
        state = self.service.run(operation_id)
        request = self.service.store.read_request(operation_id)
        if state.get("status") == "READY_TO_ACTIVATE" and request.get("activation_mode") == "activate_when_ready":
            if self.authorization_store is None or self.activation_verifier is None:
                return self.service.store.update_state(
                    operation_id,
                    last_error={
                        "code": "MONTHLY_RELEASE_AUTO_ACTIVATION_UNAVAILABLE",
                        "message": "worker lacks the activation authorization resolver/readback verifier",
                    },
                )
            return self.service.activate(
                operation_id,
                authorization_store=self.authorization_store,
                authorization_ref=str(request.get("activation_authorization_ref") or ""),
                principal=str(request.get("requested_by") or ""),
                verify_after=self.activation_verifier,
            )
        return state


__all__: Sequence[str] = (
    "MonthlyProducerError",
    "MonthlyReleaseWorker",
    "PRODUCER_EVIDENCE_SCHEMA",
    "ProducerContext",
    "RegisteredMonthlyPipeline",
    "StageProducer",
    "SubprocessStageProducer",
)
