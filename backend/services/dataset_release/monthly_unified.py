"""Durable orchestration contracts for the shared monthly dataset release.

This module is intentionally data-source agnostic.  Producer adapters return
content-addressed receipts; the service owns idempotency, state transitions,
resume/cancel, readiness closure and compare-and-swap activation.  It never
accepts a caller supplied PASS flag as evidence and never starts an experiment.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence
import uuid

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_error_diagnostics import monthly_error_diagnostics, monthly_error_message


REQUEST_SCHEMA = "aistock_monthly_release_request_v1"
PLAN_SCHEMA = "aistock_monthly_release_plan_v2"
STATE_SCHEMA = "aistock_monthly_release_state_v1"
CHECKPOINT_SCHEMA = "aistock_monthly_checkpoint_v2"
STAGE_RECEIPT_SCHEMA = "aistock_monthly_release_stage_receipt_v1"
RELEASE_CLOSURE_SCHEMA = "aistock_release_closure_v1"
NODE_REGISTRATION_SCHEMA = "aistock_node_release_registration_v2"
CONSUMER_READBACK_SCHEMA = "aistock_monthly_consumer_readback_v2"
CONSUMER_VALIDATION_BINDING_SCHEMA = (
    "aistock_monthly_consumer_validation_binding_v1"
)
READY_SCHEMA = "aistock_monthly_ready_v2"
ADOPTED_READY_SCHEMA = "aistock_monthly_adopted_ready_v1"
ACTIVATION_SCHEMA = "aistock_monthly_activation_v2"
AUTHORIZATION_SCHEMA = "aistock_dataset_action_authorization_v1"
OPERATION_ID_RE = re.compile(r"^dmr_[0-9a-f]{32}$")
AUTHORIZATION_ID_RE = re.compile(r"^dsauth_[0-9a-f]{32}$")

SOURCE_GATES = (
    "calendar_lifecycle",
    "daily_price",
    "minute_price",
    "adj_factor_history",
    "daily_basic_required_fields",
    "financial_moneyflow",
    "suspend_limit",
    "pit_stock_pools",
    "sector_authority",
)
COMPONENTS = (
    "day",
    "minute",
    "factor",
    "index",
    "suspend",
    "benchmark",
    "stock_pools",
    "sector_context",
)
REQUIRED_CONSUMERS = (
    "qe_single",
    "qe_custom",
    "qe_multi_alpha",
    "qe_p10",
    "qe_p11",
    "hmm_file_only",
    "factor_research",
    "selection",
    "advisory",
    "position_timing",
    "unified_backtest",
)
REQUIRED_NODES = ("controller", "wsl2-5080", "rdagent-node1")
STAGES = ("SOURCE", "BUILD", "DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE")
TELEMETRY_COUNT_FIELDS = (
    "unexplained_gap_count",
    "source_rows_read",
    "computed_rows",
    "files_written",
    "bytes_written",
    "bytes_transferred",
    "bytes_hashed",
    "elapsed_ms",
)


class MonthlyReleaseError(RuntimeError):
    code = "MONTHLY_RELEASE_ERROR"
    retryable = False

    def __init__(self, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = dict(context or {})


class MonthlyReleaseConflict(MonthlyReleaseError):
    code = "ACTIVE_PROFILE_CONFLICT"


class MonthlyReleaseBusy(MonthlyReleaseConflict):
    code = "MONTHLY_RELEASE_BUSY"
    retryable = True


class MonthlyReleaseNotReady(MonthlyReleaseError):
    code = "MONTHLY_RELEASE_NOT_READY"


class MonthlyReleaseSourceBlocked(MonthlyReleaseError):
    code = "SOURCE_INCOMPLETE"


class MonthlyReleaseCancelled(MonthlyReleaseError):
    code = "MONTHLY_RELEASE_CANCELLED"


class MonthlyReleaseAuthorizationError(MonthlyReleaseError):
    code = "MONTHLY_RELEASE_ACTION_NOT_AUTHORIZED"


class MonthlyReleaseRequestInvalid(MonthlyReleaseError):
    code = "MONTHLY_RELEASE_REQUEST_INVALID"


class ReleaseState(str, Enum):
    PLANNED = "PLANNED"
    CHECKING_SOURCE = "CHECKING_SOURCE"
    SOURCE_BLOCKED = "SOURCE_BLOCKED"
    SOURCE_READY = "SOURCE_READY"
    BUILDING = "BUILDING"
    DATA_SEALED = "DATA_SEALED"
    DERIVING = "DERIVING"
    VALIDATING = "VALIDATING"
    DEPLOYING = "DEPLOYING"
    READY_TO_ACTIVATE = "READY_TO_ACTIVATE"
    ACTIVATING = "ACTIVATING"
    ACTIVATED = "ACTIVATED"
    ACTIVATED_VERIFIED = "ACTIVATED_VERIFIED"
    ACTIVATED_VERIFY_FAILED = "ACTIVATED_VERIFY_FAILED"
    ROLLBACK_VERIFY_FAILED = "ROLLBACK_VERIFY_FAILED"
    ROLLBACK_VERIFIED = "ROLLBACK_VERIFIED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATES = {
    ReleaseState.READY_TO_ACTIVATE,
    ReleaseState.ACTIVATED_VERIFIED,
    ReleaseState.ACTIVATED_VERIFY_FAILED,
    ReleaseState.ROLLBACK_VERIFY_FAILED,
    ReleaseState.ROLLBACK_VERIFIED,
    ReleaseState.FAILED,
    ReleaseState.CANCELLED,
}


class ComponentAction(str, Enum):
    REUSE = "REUSE"
    INCREMENTAL = "INCREMENTAL"
    SELECTIVE_REBUILD = "SELECTIVE_REBUILD"
    COMPONENT_REBUILD = "COMPONENT_REBUILD"


@dataclass(frozen=True, slots=True)
class MonthlyReleaseRequest:
    target_cutoff: date
    product_profile: str
    idempotency_key: str
    activation_mode: str = "prepare_only"
    activation_authorization_ref: str | None = None
    repair_authorization_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{1,63}", self.product_profile):
            raise ValueError("product_profile is invalid")
        if not self.idempotency_key.strip() or len(self.idempotency_key) > 256:
            raise ValueError("idempotency_key must contain 1..256 characters")
        if self.activation_mode not in {"prepare_only", "activate_when_ready"}:
            raise ValueError("activation_mode is invalid")
        if self.activation_mode == "prepare_only" and self.activation_authorization_ref is not None:
            raise ValueError("prepare_only cannot carry activation authorization")
        if self.activation_mode == "activate_when_ready" and not self.activation_authorization_ref:
            raise ValueError("activate_when_ready requires authorization")
        if len(set(self.repair_authorization_refs)) != len(self.repair_authorization_refs):
            raise ValueError("repair authorization refs must be unique")

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": REQUEST_SCHEMA,
            "target_cutoff": self.target_cutoff.isoformat(),
            "product_profile": self.product_profile,
            "idempotency_key": self.idempotency_key,
            "activation_mode": self.activation_mode,
            "activation_authorization_ref": self.activation_authorization_ref,
            "repair_authorization_refs": list(self.repair_authorization_refs),
        }

    @property
    def semantic_digest(self) -> str:
        payload = self.payload()
        payload.pop("idempotency_key")
        return _digest(payload)


@dataclass(frozen=True, slots=True)
class ActiveProfileSnapshot:
    path: Path
    file_sha256: str
    generation: str
    release_id: str
    cutoff: date
    dataset_manifest_sha256: str
    dataset_manifest_path: Path
    dataset_manifest_file_sha256: str
    candidate_root: str
    raw: Mapping[str, Any]

    @classmethod
    def read(cls, path: Path) -> "ActiveProfileSnapshot":
        requested = path.expanduser().absolute()
        _require_plain_existing_chain(requested, label="active profile")
        if not requested.is_file():
            raise MonthlyReleaseError("active profile must be a regular non-link file")
        resolved = requested.resolve(strict=True)
        payload = resolved.read_bytes()
        try:
            value = json.loads(payload.decode("utf-8"))
            components = value["components"]
            controller = value["controller_paths"]
            candidate_root = Path(str(controller["candidate_root"])).expanduser().absolute()
            _require_plain_existing_chain(candidate_root, label="controller candidate root")
            if not candidate_root.is_dir():
                raise ValueError("controller candidate root must be a regular non-link directory")
            requested_manifest = candidate_root / "qe_dataset_manifest.json"
            _require_plain_existing_chain(requested_manifest, label="dataset manifest")
            if not requested_manifest.is_file():
                raise ValueError("dataset manifest must be a regular non-link file")
            manifest_path = requested_manifest.resolve(strict=True)
            manifest_file_sha256 = _file_sha256(manifest_path)
            pinned_file_sha256 = components.get("dataset_manifest_file_sha256")
            if pinned_file_sha256 is not None and ensure_sha256(
                str(pinned_file_sha256), field="dataset_manifest_file_sha256"
            ) != manifest_file_sha256:
                raise ValueError("dataset manifest file hash differs from profile")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_identity = ensure_sha256(
                str(components["dataset_manifest_sha256"]), field="dataset_manifest_sha256"
            )
            if manifest.get("dataset_manifest_sha256") != manifest_identity:
                raise ValueError("dataset manifest identity differs from profile")
            result = cls(
                path=resolved,
                file_sha256=hashlib.sha256(payload).hexdigest(),
                generation=str(value["generation"]),
                release_id=str(value["release_id"]),
                cutoff=date.fromisoformat(str(value["cutoff"])),
                dataset_manifest_sha256=manifest_identity,
                dataset_manifest_path=manifest_path,
                dataset_manifest_file_sha256=manifest_file_sha256,
                candidate_root=str(candidate_root),
                raw=value,
            )
        except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MonthlyReleaseError("active profile identity is invalid") from exc
        if result.cutoff >= date.max:
            raise MonthlyReleaseError("active profile cutoff is invalid")
        return result


@dataclass(frozen=True, slots=True)
class SourceChange:
    dataset: str
    fields: tuple[str, ...]
    instruments: tuple[str, ...]
    start: date
    end: date
    kind: str
    source_receipt_sha256: str

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError("source change end precedes start")
        if self.kind not in {
            "TAIL_APPEND",
            "HISTORICAL_REPAIR",
            "NEW_SECURITY_HISTORY",
            "ADJ_DENOMINATOR_CHANGE",
            "ADJ_HISTORY_RESTATEMENT",
            "PIT_REVISION",
            "SCHEMA_CHANGE",
        }:
            raise ValueError("source change kind is invalid")
        ensure_sha256(self.source_receipt_sha256, field="source_receipt_sha256")

    def payload(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "fields": list(self.fields),
            "instruments": list(self.instruments),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "kind": self.kind,
            "source_receipt_sha256": self.source_receipt_sha256,
        }


def classify_component_actions(changes: Sequence[SourceChange]) -> dict[str, str]:
    """Compile exact source changes into the smallest safe component actions."""

    actions = {name: ComponentAction.REUSE for name in COMPONENTS}

    def promote(component: str, action: ComponentAction) -> None:
        order = {
            ComponentAction.REUSE: 0,
            ComponentAction.INCREMENTAL: 1,
            ComponentAction.SELECTIVE_REBUILD: 2,
            ComponentAction.COMPONENT_REBUILD: 3,
        }
        current = ComponentAction(actions[component])
        if order[action] > order[current]:
            actions[component] = action

    for change in changes:
        kind = change.kind
        dataset = change.dataset
        if kind == "SCHEMA_CHANGE":
            for component in COMPONENTS:
                if dataset in _component_source_datasets(component):
                    promote(component, ComponentAction.COMPONENT_REBUILD)
            continue
        if dataset in {"kline_daily_raw", "adj_factor", "stk_limit", "stock_basic"}:
            promote("day", ComponentAction.INCREMENTAL)
            promote("benchmark", ComponentAction.INCREMENTAL)
        if dataset in {"kline_minute_raw", "adj_factor", "stk_limit", "suspend_d", "stock_basic"}:
            promote("minute", ComponentAction.INCREMENTAL)
        if dataset in {
            "kline_daily_raw",
            "daily_basic",
            "moneyflow",
            "bak_basic",
            "cyq_perf",
            "margin_detail",
            "adj_factor",
            "industry_classification",
            "sw_daily",
        }:
            promote("factor", ComponentAction.INCREMENTAL)
        if dataset in {"index_daily"}:
            promote("index", ComponentAction.INCREMENTAL)
        if dataset in {"suspend_d", "stk_limit", "stock_basic"}:
            promote("suspend", ComponentAction.INCREMENTAL)
        if dataset in {"stock_universe_pit", "index_membership_pit", "stock_basic"}:
            promote("stock_pools", ComponentAction.SELECTIVE_REBUILD)
        if dataset in {"industry_classification", "sw_daily", "stock_universe_pit"}:
            promote("sector_context", ComponentAction.SELECTIVE_REBUILD)
        if kind in {"HISTORICAL_REPAIR", "NEW_SECURITY_HISTORY", "PIT_REVISION"}:
            # A repair is scoped to its own source domain, not every tail
            # encountered earlier in the batch. Otherwise daily_basic repairs
            # rebuild unrelated minute/index data and depend on record order.
            for component in COMPONENTS:
                if dataset in _component_source_datasets(component) or (
                    dataset == "stock_universe_pit"
                    and component in {"day", "minute", "factor", "suspend"}
                ):
                    promote(component, ComponentAction.SELECTIVE_REBUILD)
        if kind == "ADJ_DENOMINATOR_CHANGE":
            for component in ("day", "minute", "factor"):
                promote(component, ComponentAction.SELECTIVE_REBUILD)
        if kind == "ADJ_HISTORY_RESTATEMENT":
            promote("day", ComponentAction.SELECTIVE_REBUILD)
            promote("minute", ComponentAction.SELECTIVE_REBUILD)
            promote("factor", ComponentAction.SELECTIVE_REBUILD)
    return {key: str(value.value if isinstance(value, ComponentAction) else value) for key, value in actions.items()}


def _component_source_datasets(component: str) -> set[str]:
    values = {
        "day": {"kline_daily_raw", "adj_factor", "stk_limit", "stock_basic"},
        "minute": {"kline_minute_raw", "adj_factor", "stk_limit", "suspend_d", "stock_basic"},
        "factor": {
            "kline_daily_raw",
            "daily_basic",
            "moneyflow",
            "bak_basic",
            "cyq_perf",
            "margin_detail",
            "adj_factor",
            "industry_classification",
            "sw_daily",
        },
        "index": {"index_daily"},
        "suspend": {"suspend_d", "stk_limit", "stock_basic"},
        "benchmark": {"kline_daily_raw", "adj_factor"},
        "stock_pools": {"stock_universe_pit", "index_membership_pit", "stock_basic"},
        "sector_context": {"industry_classification", "sw_daily", "stock_universe_pit"},
    }
    return values[component]


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _require_plain_existing_chain(path: Path, *, label: str) -> None:
    """Reject links/reparse points in every existing path segment."""

    requested = path.expanduser().absolute()
    current = Path(requested.anchor)
    for part in requested.parts[1:]:
        current /= part
        if not current.exists():
            raise MonthlyReleaseError(f"{label} path is unavailable")
        if _is_link_or_junction(current):
            raise MonthlyReleaseError(f"{label} path traverses a link or junction")


def _content_ref(path: Path, *, ref_id: str | None = None) -> dict[str, Any]:
    if _is_link_or_junction(path):
        raise MonthlyReleaseError("content reference must resolve to a regular non-link file")
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise MonthlyReleaseError("content reference must resolve to a regular non-link file")
    return {
        "id": ref_id or resolved.name,
        "sha256": _file_sha256(resolved),
        "size": resolved.stat().st_size,
    }


def _require_content_ref(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"id", "sha256", "size"}:
        raise MonthlyReleaseNotReady(f"{field} is not a complete content reference")
    ref_id = str(value["id"] or "")
    if not ref_id or Path(ref_id).is_absolute() or ".." in Path(ref_id).parts or "\\" in ref_id:
        raise MonthlyReleaseNotReady(f"{field}.id must be a portable relative path")
    digest = ensure_sha256(str(value["sha256"]), field=f"{field}.sha256")
    size = value["size"]
    if type(size) is not int or size < 0:
        raise MonthlyReleaseNotReady(f"{field}.size is invalid")
    return {"id": ref_id, "sha256": digest, "size": size}


def _canonical_payload(value: Mapping[str, Any]) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def _write_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(_canonical_payload(value))
        handle.flush()
        os.fsync(handle.fileno())


def _replace_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_payload(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyReleaseError(f"{label} is not readable UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise MonthlyReleaseError(f"{label} must be an object")
    return value


@contextmanager
def _exclusive_lock(path: Path, *, blocking: bool = True) -> Iterator[None]:
    """Cross-platform advisory lock over one product/operation lock file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    acquired = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - exercised by WSL/CI Linux
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except OSError as exc:
            if not blocking and exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise MonthlyReleaseBusy("monthly operation writer is already owned") from exc
            raise
        acquired = True
        yield
    finally:
        try:
            if acquired and os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            elif acquired:  # pragma: no cover
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


class MonthlyOperationStore:
    """Small file-backed store with create-exclusive requests and atomic state."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("monthly release state root must be absolute")
        self.root = root
        self.operations = root / "monthly"
        self.locks = root / "locks"

    def operation_root(self, operation_id: str) -> Path:
        if not OPERATION_ID_RE.fullmatch(operation_id):
            raise ValueError("operation_id is invalid")
        return self.operations / operation_id

    def create(
        self,
        *,
        request: MonthlyReleaseRequest,
        predecessor: ActiveProfileSnapshot,
        generation: str,
        revision: str,
        candidate_root: Path,
        profile_candidate: Path,
        node_roots: Mapping[str, str],
        requested_by: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        self.operations.mkdir(parents=True, exist_ok=True)
        self.locks.mkdir(parents=True, exist_ok=True)
        if set(node_roots) != {"wsl2-5080", "rdagent-node1"}:
            raise ValueError("node roots must cover WSL and node1 exactly")
        if not candidate_root.is_absolute() or not profile_candidate.is_absolute():
            raise ValueError("candidate/profile targets must be absolute")
        if candidate_root.exists() or profile_candidate.exists():
            raise FileExistsError("candidate/profile target must be create-exclusive")
        if not requested_by.strip():
            raise ValueError("monthly release requester identity is empty")
        lock_path = self.locks / f"{request.product_profile}.lock"
        with _exclusive_lock(lock_path):
            for child in self.operations.iterdir():
                request_path = child / "request.json"
                if not request_path.is_file():
                    continue
                existing = _read_json(request_path, label="existing request")
                if (
                    existing.get("product_profile") == request.product_profile
                    and existing.get("idempotency_key") == request.idempotency_key
                ):
                    if existing.get("semantic_digest") != request.semantic_digest:
                        raise MonthlyReleaseConflict("idempotency key is bound to another request")
                    return self.read_state(str(existing["operation_id"]))
                if existing.get("product_profile") != request.product_profile:
                    continue
                existing_state = self.read_state(str(existing["operation_id"]))
                if existing.get("target_cutoff") == request.target_cutoff.isoformat():
                    raise MonthlyReleaseConflict("target cutoff is already bound to another operation")
                if ReleaseState(str(existing_state["status"])) not in TERMINAL_STATES:
                    raise MonthlyReleaseConflict("another monthly operation is active for this product")
            operation_id = f"dmr_{uuid.uuid4().hex}"
            root = self.operation_root(operation_id)
            root.mkdir(parents=False, exist_ok=False)
            (root / "checkpoints").mkdir()
            (root / "receipts").mkdir()
            (root / "logs").mkdir()
            request_payload = {
                **request.payload(),
                "operation_id": operation_id,
                "semantic_digest": request.semantic_digest,
                "requested_by": requested_by,
                "created_at": observed.isoformat(),
            }
            plan = {
                "schema_version": PLAN_SCHEMA,
                "operation_id": operation_id,
                "plan_state": "AWAITING_SOURCE_SNAPSHOT",
                "target_cutoff": request.target_cutoff.isoformat(),
                "cutoff": request.target_cutoff.isoformat(),
                "product_profile": request.product_profile,
                "generation": generation,
                "revision": revision,
                "release_id": f"qe_hmm_full_v2_{request.target_cutoff:%Y%m%d}",
                "predecessor": {
                    "profile_path": str(predecessor.path),
                    "profile_sha256": predecessor.file_sha256,
                    "generation": predecessor.generation,
                    "release_id": predecessor.release_id,
                    "cutoff": predecessor.cutoff.isoformat(),
                    "dataset_manifest_sha256": predecessor.dataset_manifest_sha256,
                    "candidate_root": predecessor.candidate_root,
                },
                "predecessor_profile_ref": {
                    "id": predecessor.path.name,
                    "sha256": predecessor.file_sha256,
                    "size": predecessor.path.stat().st_size,
                },
                "predecessor_manifest_ref": {
                    "id": "qe_dataset_manifest.json",
                    "sha256": predecessor.dataset_manifest_file_sha256,
                    "size": predecessor.dataset_manifest_path.stat().st_size,
                    "dataset_manifest_sha256": predecessor.dataset_manifest_sha256,
                },
                "candidate_root": str(candidate_root.absolute()),
                "profile_candidate": str(profile_candidate.absolute()),
                "node_roots": dict(sorted(node_roots.items())),
                "target_roots": {
                    "controller": str(candidate_root.absolute()),
                    **dict(sorted(node_roots.items())),
                },
                "required_source_gates": list(SOURCE_GATES),
                "required_consumers": list(REQUIRED_CONSUMERS),
                "repair_authorization_refs": list(request.repair_authorization_refs),
                "source_as_of": None,
                "source_snapshot_group_id": None,
                "snapshot_identity_ref": None,
                "repair_overlap_check_ref": None,
                "change_scope_refs": [],
                "actions": {component: "PENDING_SOURCE" for component in COMPONENTS},
                "producer_contract_refs": [],
            }
            state = {
                "schema_version": STATE_SCHEMA,
                "operation_id": operation_id,
                "status": ReleaseState.PLANNED.value,
                "attempt": 0,
                "current_stage": None,
                "cancel_requested": False,
                "last_error": None,
                "created_at": observed.isoformat(),
                "updated_at": observed.isoformat(),
                "plan_sha256": _digest(plan),
                "candidate_root": plan["candidate_root"],
                "profile_candidate": plan["profile_candidate"],
            }
            _write_exclusive(root / "request.json", request_payload)
            _write_exclusive(root / "plan.json", plan)
            _write_exclusive(root / "state.json", state)
            return state

    def find_idempotent(self, *, request: MonthlyReleaseRequest) -> dict[str, Any] | None:
        """Resolve a prior request before re-reading a possibly advanced active profile."""

        if not self.operations.is_dir():
            return None
        lock_path = self.locks / f"{request.product_profile}.lock"
        with _exclusive_lock(lock_path):
            for child in self.operations.iterdir():
                request_path = child / "request.json"
                if not request_path.is_file():
                    continue
                existing = _read_json(request_path, label="existing request")
                if (
                    existing.get("product_profile") == request.product_profile
                    and existing.get("idempotency_key") == request.idempotency_key
                ):
                    if existing.get("semantic_digest") != request.semantic_digest:
                        raise MonthlyReleaseConflict("idempotency key is bound to another request")
                    return self.read_state(str(existing["operation_id"]))
        return None

    def pending_operation_ids(self) -> tuple[str, ...]:
        """Return runnable operations; blocked sources wait for explicit resume."""

        pending: list[tuple[str, str]] = []
        if not self.operations.is_dir():
            return ()
        for child in self.operations.iterdir():
            state_path = child / "state.json"
            if not state_path.is_file():
                continue
            state = _read_json(state_path, label="monthly state")
            status = ReleaseState(str(state["status"]))
            if status == ReleaseState.SOURCE_BLOCKED and state.get("cancel_requested") is not True:
                continue
            if status == ReleaseState.READY_TO_ACTIVATE:
                request = _read_json(child / "request.json", label="monthly request")
                if request.get("activation_mode") != "activate_when_ready":
                    continue
            elif status in TERMINAL_STATES or status == ReleaseState.ACTIVATED:
                continue
            pending.append((str(state.get("created_at") or ""), str(state["operation_id"])))
        return tuple(operation_id for _, operation_id in sorted(pending))

    def read_request(self, operation_id: str) -> dict[str, Any]:
        return _read_json(self.operation_root(operation_id) / "request.json", label="monthly request")

    def read_plan(self, operation_id: str) -> dict[str, Any]:
        return _read_json(self.operation_root(operation_id) / "plan.json", label="monthly plan")

    def replace_plan(self, operation_id: str, plan: Mapping[str, Any]) -> dict[str, Any]:
        root = self.operation_root(operation_id)
        with _exclusive_lock(root / ".operation.lock"):
            current = self.read_plan(operation_id)
            if current.get("operation_id") != plan.get("operation_id"):
                raise MonthlyReleaseError("replacement plan operation identity differs")
            payload = dict(plan)
            _replace_json(root / "plan.json", payload)
            state = self.read_state(operation_id)
            state["plan_sha256"] = _digest(payload)
            state["updated_at"] = datetime.now(UTC).isoformat()
            _replace_json(root / "state.json", state)
            return payload

    def read_state(self, operation_id: str) -> dict[str, Any]:
        return _read_json(self.operation_root(operation_id) / "state.json", label="monthly state")

    def update_state(self, operation_id: str, **changes: Any) -> dict[str, Any]:
        root = self.operation_root(operation_id)
        with _exclusive_lock(root / ".operation.lock"):
            state = self.read_state(operation_id)
            state.update(changes)
            state["updated_at"] = datetime.now(UTC).isoformat()
            _replace_json(root / "state.json", state)
            return state

    def request_cancel(self, operation_id: str) -> dict[str, Any]:
        root = self.operation_root(operation_id)
        with _exclusive_lock(root / ".operation.lock"):
            state = self.read_state(operation_id)
            status = ReleaseState(state["status"])
            if status in TERMINAL_STATES:
                return state
            state["cancel_requested"] = True
            if status == ReleaseState.SOURCE_BLOCKED:
                state["status"] = ReleaseState.CANCELLED.value
                state["current_stage"] = None
            state["updated_at"] = datetime.now(UTC).isoformat()
            _replace_json(root / "state.json", state)
            return state

    def write_checkpoint(
        self,
        operation_id: str,
        *,
        stage: str,
        attempt: int,
        receipt: Mapping[str, Any],
        request_digest: str,
        plan_digest: str,
        input_set_digest: str,
        producer_digest: str | None = None,
    ) -> Path:
        if stage not in STAGES:
            raise ValueError("stage is invalid")
        root = self.operation_root(operation_id)
        receipt_payload = validate_stage_receipt(receipt, operation_id=operation_id, stage=stage)
        receipt_path = root / "receipts" / f"{stage.lower()}-{receipt_payload['canonical_sha256']}.json"
        if not receipt_path.exists():
            _write_exclusive(receipt_path, receipt_payload)
        checkpoint = {
            "schema_version": CHECKPOINT_SCHEMA,
            "operation_id": operation_id,
            "stage": stage,
            "attempt": attempt,
            "request_digest": ensure_sha256(request_digest, field="checkpoint.request_digest"),
            "plan_digest": ensure_sha256(plan_digest, field="checkpoint.plan_digest"),
            "input_set_digest": ensure_sha256(
                input_set_digest, field="checkpoint.input_set_digest"
            ),
            "producer_digest": ensure_sha256(
                producer_digest or _digest(receipt_payload["producer"]),
                field="checkpoint.producer_digest",
            ),
            "completed_units": [ref["id"] for ref in receipt_payload["output_refs"]],
            "unit_output_refs": receipt_payload["output_refs"],
            "consistent_input_set_complete": True,
            "receipt": {
                "path": str(receipt_path.relative_to(root)).replace("\\", "/"),
                "sha256": _file_sha256(receipt_path),
                "canonical_sha256": receipt_payload["canonical_sha256"],
            },
        }
        checkpoint["canonical_sha256"] = _digest(checkpoint)
        with _exclusive_lock(root / ".operation.lock"):
            if self.read_state(operation_id).get("cancel_requested") is True:
                raise MonthlyReleaseCancelled("cancelled before checkpoint publication")
            _replace_json(root / "checkpoints" / f"{stage.lower()}.json", checkpoint)
        return receipt_path

    def read_checkpoint(
        self,
        operation_id: str,
        stage: str,
        *,
        request_digest: str | None = None,
        plan_digest: str | None = None,
        producer_digest: str | None = None,
        input_set_digest: str | None = None,
    ) -> dict[str, Any] | None:
        path = self.operation_root(operation_id) / "checkpoints" / f"{stage.lower()}.json"
        if not path.is_file():
            return None
        checkpoint = _read_json(path, label=f"{stage} checkpoint")
        required = {
            "schema_version",
            "operation_id",
            "stage",
            "attempt",
            "request_digest",
            "plan_digest",
            "input_set_digest",
            "producer_digest",
            "completed_units",
            "unit_output_refs",
            "consistent_input_set_complete",
            "receipt",
            "canonical_sha256",
        }
        if set(checkpoint) != required or checkpoint.get("schema_version") != CHECKPOINT_SCHEMA:
            raise MonthlyReleaseError("checkpoint schema differs")
        claimed = ensure_sha256(
            str(checkpoint["canonical_sha256"]), field="checkpoint.canonical_sha256"
        )
        unsigned = dict(checkpoint)
        unsigned.pop("canonical_sha256")
        if _digest(unsigned) != claimed:
            raise MonthlyReleaseError("checkpoint canonical digest differs")
        if (
            checkpoint.get("operation_id") != operation_id
            or checkpoint.get("stage") != stage
            or checkpoint.get("consistent_input_set_complete") is not True
        ):
            raise MonthlyReleaseError("checkpoint identity differs")
        expectations = {
            "request_digest": request_digest,
            "plan_digest": plan_digest,
            "producer_digest": producer_digest,
            "input_set_digest": input_set_digest,
        }
        if any(expected is not None and checkpoint.get(field) != expected for field, expected in expectations.items()):
            return None
        root = self.operation_root(operation_id)
        receipt_meta = checkpoint.get("receipt")
        if not isinstance(receipt_meta, Mapping):
            raise MonthlyReleaseError("checkpoint receipt reference is invalid")
        receipt_path = root / str(receipt_meta.get("path") or "")
        if not receipt_path.resolve(strict=True).is_relative_to(root.resolve(strict=True)):
            raise MonthlyReleaseError("checkpoint receipt escaped operation root")
        if _file_sha256(receipt_path) != receipt_meta.get("sha256"):
            raise MonthlyReleaseError("checkpoint receipt hash differs")
        receipt = _read_json(receipt_path, label=f"{stage} receipt")
        validate_stage_receipt(receipt, operation_id=operation_id, stage=stage)
        return receipt


def validate_stage_receipt(
    value: Mapping[str, Any], *, operation_id: str, stage: str
) -> dict[str, Any]:
    required = {
        "schema_version",
        "operation_id",
        "attempt_id",
        "stage",
        "status",
        "producer",
        "scope",
        "input_refs",
        "output_refs",
        "counts",
        "errors_ref",
        "errors",
        "started_at",
        "finished_at",
        "canonical_sha256",
    }
    if set(value) != required:
        raise MonthlyReleaseError("stage receipt fields differ", context={"stage": stage})
    if (
        value.get("schema_version") != STAGE_RECEIPT_SCHEMA
        or value.get("operation_id") != operation_id
        or value.get("stage") != stage
        or value.get("status") != "PASS"
        or not str(value.get("attempt_id") or "").strip()
    ):
        raise MonthlyReleaseError("stage receipt identity differs", context={"stage": stage})
    if not isinstance(value.get("producer"), Mapping) or set(value["producer"]) != {"id", "version"}:
        raise MonthlyReleaseError("stage producer identity is invalid")
    for ref_group in ("input_refs", "output_refs"):
        refs = value.get(ref_group)
        if not isinstance(refs, list):
            raise MonthlyReleaseError(f"{ref_group} must be a list")
        for ref in refs:
            if not isinstance(ref, Mapping) or set(ref) != {"id", "sha256", "size"}:
                raise MonthlyReleaseError(f"{ref_group} contains an invalid reference")
            ensure_sha256(str(ref["sha256"]), field=f"{ref_group}.sha256")
            if not str(ref["id"]).strip() or type(ref["size"]) is not int or ref["size"] < 0:
                raise MonthlyReleaseError(f"{ref_group} reference identity is invalid")
            ref_id = str(ref["id"])
            if Path(ref_id).is_absolute() or ".." in Path(ref_id).parts or "\\" in ref_id:
                raise MonthlyReleaseError(f"{ref_group} reference id must be a portable relative path")
    if not isinstance(value.get("counts"), Mapping) or not isinstance(value.get("errors"), list):
        raise MonthlyReleaseError("stage counts/errors are invalid")
    if set(value["counts"]) != set(TELEMETRY_COUNT_FIELDS) or any(
        type(value["counts"][field]) is not int or value["counts"][field] < 0
        for field in TELEMETRY_COUNT_FIELDS
    ):
        raise MonthlyReleaseError("stage telemetry counts differ")
    if value["errors"]:
        raise MonthlyReleaseSourceBlocked("stage receipt contains unresolved errors", context={"stage": stage})
    if int(value["counts"].get("unexplained_gap_count", 0)) != 0:
        raise MonthlyReleaseSourceBlocked("stage receipt contains unexplained gaps", context={"stage": stage})
    if value.get("errors_ref") is not None:
        ensure_sha256(str(value["errors_ref"]), field="errors_ref")
    payload = dict(value)
    claimed = ensure_sha256(str(payload.pop("canonical_sha256")), field="canonical_sha256")
    actual = _digest(payload)
    if claimed != actual:
        raise MonthlyReleaseError("stage receipt canonical digest differs", context={"stage": stage})
    return dict(value)


def build_stage_receipt(
    *,
    operation_id: str,
    attempt_id: str,
    stage: str,
    producer_id: str,
    producer_version: str,
    scope: Mapping[str, Any],
    input_refs: Sequence[Mapping[str, Any]],
    output_refs: Sequence[Mapping[str, Any]],
    counts: Mapping[str, int],
    errors: Sequence[Mapping[str, Any]] = (),
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": STAGE_RECEIPT_SCHEMA,
        "operation_id": operation_id,
        "attempt_id": attempt_id,
        "stage": stage,
        "status": "PASS",
        "producer": {"id": producer_id, "version": producer_version},
        "scope": dict(scope),
        "input_refs": [dict(item) for item in input_refs],
        "output_refs": [dict(item) for item in output_refs],
        "counts": dict(counts),
        "errors_ref": None,
        "errors": [dict(item) for item in errors],
        "started_at": started_at,
        "finished_at": finished_at,
    }
    payload["canonical_sha256"] = _digest(payload)
    return payload


class MonthlyPipeline(Protocol):
    def run_stage(
        self,
        *,
        stage: str,
        operation_id: str,
        attempt: int,
        request: Mapping[str, Any],
        plan: Mapping[str, Any],
        prior_receipts: Mapping[str, Mapping[str, Any]],
    ) -> Mapping[str, Any]: ...


class MonthlyReleaseService:
    """Execute one durable monthly release through registered stage adapters."""

    def __init__(
        self,
        store: MonthlyOperationStore,
        *,
        active_profile: Path,
        pipeline: MonthlyPipeline,
        candidate_root_factory: Callable[[MonthlyReleaseRequest], Path],
        profile_candidate_factory: Callable[[MonthlyReleaseRequest], Path],
        node_root_factory: Callable[[MonthlyReleaseRequest], Mapping[str, str]],
        generation_factory: Callable[[MonthlyReleaseRequest], str],
        revision_factory: Callable[[MonthlyReleaseRequest], str],
        allowed_cutoff_resolver: Callable[[], date] | None = None,
    ) -> None:
        self.store = store
        self.active_profile = active_profile
        self.pipeline = pipeline
        self.candidate_root_factory = candidate_root_factory
        self.profile_candidate_factory = profile_candidate_factory
        self.node_root_factory = node_root_factory
        self.generation_factory = generation_factory
        self.revision_factory = revision_factory
        self.allowed_cutoff_resolver = allowed_cutoff_resolver

    def _validate_target_cutoff(self, requested: date) -> None:
        if self.allowed_cutoff_resolver is None:
            return
        allowed = self.allowed_cutoff_resolver()
        if requested != allowed:
            raise MonthlyReleaseRequestInvalid(
                "target cutoff is not the official last trading day of the latest completed month",
                context={"requested": requested.isoformat(), "allowed": allowed.isoformat()},
            )

    def submit(
        self,
        request: MonthlyReleaseRequest,
        *,
        principal: str = "dataset-release-service",
    ) -> dict[str, Any]:
        existing = self.store.find_idempotent(request=request)
        if existing is not None:
            return existing
        self._validate_target_cutoff(request.target_cutoff)
        predecessor = ActiveProfileSnapshot.read(self.active_profile)
        if request.target_cutoff <= predecessor.cutoff:
            raise MonthlyReleaseConflict("target cutoff must be later than the active cutoff")
        return self.store.create(
            request=request,
            predecessor=predecessor,
            generation=self.generation_factory(request),
            revision=self.revision_factory(request),
            candidate_root=self.candidate_root_factory(request),
            profile_candidate=self.profile_candidate_factory(request),
            node_roots=self.node_root_factory(request),
            requested_by=principal,
        )

    def preview(self, request: MonthlyReleaseRequest) -> dict[str, Any]:
        self._validate_target_cutoff(request.target_cutoff)
        predecessor = ActiveProfileSnapshot.read(self.active_profile)
        if request.target_cutoff <= predecessor.cutoff:
            raise MonthlyReleaseConflict("target cutoff must be later than the active cutoff")
        candidate_root = self.candidate_root_factory(request)
        profile_candidate = self.profile_candidate_factory(request)
        if not candidate_root.is_absolute() or not profile_candidate.is_absolute():
            raise MonthlyReleaseError("candidate/profile targets must be absolute")
        return {
            "schema_version": PLAN_SCHEMA,
            "plan_state": "PREVIEW_AWAITING_SOURCE_SNAPSHOT",
            "target_cutoff": request.target_cutoff.isoformat(),
            "cutoff": request.target_cutoff.isoformat(),
            "product_profile": request.product_profile,
            "generation": self.generation_factory(request),
            "revision": self.revision_factory(request),
            "release_id": f"qe_hmm_full_v2_{request.target_cutoff:%Y%m%d}",
            "predecessor": {
                "profile_path": str(predecessor.path),
                "profile_sha256": predecessor.file_sha256,
                "generation": predecessor.generation,
                "release_id": predecessor.release_id,
                "cutoff": predecessor.cutoff.isoformat(),
                "dataset_manifest_sha256": predecessor.dataset_manifest_sha256,
                "candidate_root": predecessor.candidate_root,
            },
            "predecessor_profile_ref": {
                "id": predecessor.path.name,
                "sha256": predecessor.file_sha256,
                "size": predecessor.path.stat().st_size,
            },
            "predecessor_manifest_ref": {
                "id": "qe_dataset_manifest.json",
                "sha256": predecessor.dataset_manifest_file_sha256,
                "size": predecessor.dataset_manifest_path.stat().st_size,
                "dataset_manifest_sha256": predecessor.dataset_manifest_sha256,
            },
            "candidate_root": str(candidate_root),
            "profile_candidate": str(profile_candidate),
            "node_roots": dict(sorted(self.node_root_factory(request).items())),
            "target_roots": {
                "controller": str(candidate_root),
                **dict(sorted(self.node_root_factory(request).items())),
            },
            "source_as_of": None,
            "source_snapshot_group_id": None,
            "snapshot_identity_ref": None,
            "repair_overlap_check_ref": None,
            "change_scope_refs": [],
            "actions": {component: "PENDING_SOURCE" for component in COMPONENTS},
            "producer_contract_refs": [],
            "required_consumers": list(REQUIRED_CONSUMERS),
            "database_write_performed": False,
            "active_profile_write": False,
            "runtime_action_performed": False,
        }

    def adopt_existing_successor(
        self,
        *,
        idempotency_key: str,
        product_profile: str,
        target_cutoff: date,
        candidate_root: Path,
        profile_candidate: Path,
        predecessor_profile_sha256: str,
        target_profile_sha256: str,
        dataset_manifest_sha256: str,
        dataset_manifest_file_sha256: str,
        node_manifest_file_sha256: Mapping[str, str],
        evidence_refs: Sequence[Mapping[str, str]],
        controller_release_root: Path,
        profile_candidate_root: Path,
        expected_node_roots: Mapping[str, str],
        principal: str,
    ) -> dict[str, Any]:
        """Adopt a verified immutable v3 hotfix without rebuilding its bytes."""

        if not idempotency_key.strip() or len(idempotency_key) > 256:
            raise MonthlyReleaseRequestInvalid("adoption idempotency key is invalid")
        if product_profile != "qe_hmm_full_v2":
            raise MonthlyReleaseRequestInvalid("adoption product profile is invalid")
        if not principal.strip():
            raise MonthlyReleaseAuthorizationError("adoption principal is empty")
        predecessor = ActiveProfileSnapshot.read(self.active_profile)
        expected_predecessor = ensure_sha256(
            predecessor_profile_sha256, field="predecessor_profile_sha256"
        )
        if predecessor.file_sha256 != expected_predecessor:
            raise MonthlyReleaseConflict("active profile differs from the adoption predecessor")
        if target_cutoff != predecessor.cutoff:
            raise MonthlyReleaseConflict(
                "legacy successor adoption is limited to a same-cutoff immutable hotfix"
            )

        candidate = candidate_root.expanduser().absolute()
        profile_path = profile_candidate.expanduser().absolute()
        controller_parent = controller_release_root.resolve(strict=True)
        profile_parent = profile_candidate_root.resolve(strict=True)
        _require_plain_existing_chain(candidate, label="adopted candidate root")
        _require_plain_existing_chain(profile_path, label="adopted profile candidate")
        if (
            not candidate.is_dir()
            or candidate.resolve(strict=True).parent != controller_parent
            or not profile_path.is_file()
            or profile_path.resolve(strict=True).parent != profile_parent
        ):
            raise MonthlyReleaseConflict("adoption target escaped the registered release roots")

        target_profile_hash = ensure_sha256(target_profile_sha256, field="target_profile_sha256")
        manifest_identity = ensure_sha256(
            dataset_manifest_sha256, field="dataset_manifest_sha256"
        )
        manifest_file_hash = ensure_sha256(
            dataset_manifest_file_sha256, field="dataset_manifest_file_sha256"
        )
        if _file_sha256(profile_path) != target_profile_hash:
            raise MonthlyReleaseConflict("adopted profile hash differs")
        manifest_path = candidate / "qe_dataset_manifest.json"
        _require_plain_existing_chain(manifest_path, label="adopted dataset manifest")
        if not manifest_path.is_file() or _file_sha256(manifest_path) != manifest_file_hash:
            raise MonthlyReleaseConflict("adopted manifest file hash differs")
        profile = _read_json(profile_path, label="adopted profile candidate")
        manifest = _read_json(manifest_path, label="adopted dataset manifest")
        components = profile.get("components")
        controller_paths = profile.get("controller_paths")
        node_bindings = profile.get("node_bindings")
        if (
            profile.get("schema_version") != "aistock_active_dataset_profile_v3"
            or not isinstance(components, Mapping)
            or not isinstance(controller_paths, Mapping)
            or not isinstance(node_bindings, Mapping)
            or profile.get("cutoff") != target_cutoff.isoformat()
            or components.get("dataset_manifest_sha256") != manifest_identity
            or components.get("dataset_manifest_file_sha256") != manifest_file_hash
            or Path(str(controller_paths.get("candidate_root") or "")).expanduser().absolute()
            != candidate
        ):
            raise MonthlyReleaseConflict("adopted profile identity is incomplete or inconsistent")
        if (
            manifest.get("dataset_manifest_sha256") != manifest_identity
            or manifest.get("cutoff_trade_date") != target_cutoff.isoformat()
            or manifest.get("release_id") != profile.get("release_id")
        ):
            raise MonthlyReleaseConflict("adopted manifest identity differs from the profile")
        if set(expected_node_roots) != {"wsl2-5080", "rdagent-node1"}:
            raise MonthlyReleaseConflict("configured adoption node roots are incomplete")
        actual_node_roots = {
            node: str((node_bindings.get(node) or {}).get("candidate_root") or "")
            for node in ("wsl2-5080", "rdagent-node1")
        }
        if actual_node_roots != dict(expected_node_roots):
            raise MonthlyReleaseConflict("adopted profile node roots differ from configuration")
        node_hashes = {
            node: ensure_sha256(value, field=f"node_manifest_file_sha256.{node}")
            for node, value in node_manifest_file_sha256.items()
        }
        if set(node_hashes) != set(REQUIRED_NODES) or set(node_hashes.values()) != {
            manifest_file_hash
        }:
            raise MonthlyReleaseConflict("adopted release node manifest hashes differ")

        normalized_evidence: list[dict[str, Any]] = []
        schemas: set[str] = set()
        for raw in evidence_refs:
            relative = str(raw.get("relative_path") or "")
            relative_path = Path(relative)
            if (
                not relative
                or relative_path.is_absolute()
                or ".." in relative_path.parts
                or relative_path.as_posix() != relative.replace("\\", "/")
            ):
                raise MonthlyReleaseConflict("adoption evidence path is invalid")
            path = candidate / relative_path
            _require_plain_existing_chain(path, label="adoption evidence")
            if not path.is_file():
                raise MonthlyReleaseConflict("adoption evidence is unavailable")
            expected_hash = ensure_sha256(str(raw.get("sha256") or ""), field="evidence sha256")
            if _file_sha256(path) != expected_hash:
                raise MonthlyReleaseConflict("adoption evidence hash differs")
            value = _read_json(path, label="adoption evidence")
            schema = str(raw.get("schema_version") or "")
            if value.get("schema_version") != schema:
                raise MonthlyReleaseConflict("adoption evidence schema differs")
            if value.get("status") not in {None, "PASS"}:
                raise MonthlyReleaseNotReady("adoption evidence is not PASS")
            if schema == "qe_moneyflow_alias_coverage_receipt_v1" and (
                int(value.get("unknown", -1)) != 0
                or int(value.get("nonfinite", -1)) != 0
                or int(value.get("mismatched", -1)) != 0
                or int(value.get("expected", -1))
                != int(value.get("resolved", -2)) + int(value.get("provider_absence", -3))
                or value.get("database_write") is not False
            ):
                raise MonthlyReleaseNotReady("moneyflow alias adoption evidence is incomplete")
            if schema == "hmm_risk_rotation_l2_input_bundle_v1" and (
                not isinstance(value.get("identity"), Mapping)
                or value["identity"].get("manifest_sha256") != manifest_identity
            ):
                raise MonthlyReleaseNotReady("HMM adoption evidence binds another manifest")
            schemas.add(schema)
            normalized_evidence.append(
                {
                    "id": relative_path.as_posix(),
                    "sha256": expected_hash,
                    "size": path.stat().st_size,
                    "schema_version": schema,
                }
            )
        required_schemas = {
            "qe_moneyflow_alias_coverage_receipt_v1",
            "hmm_risk_rotation_l2_input_bundle_v1",
        }
        if not required_schemas.issubset(schemas):
            raise MonthlyReleaseNotReady("adoption evidence does not cover required release gates")

        semantic = {
            "schema_version": "aistock_monthly_existing_successor_adoption_v1",
            "target_cutoff": target_cutoff.isoformat(),
            "product_profile": product_profile,
            "candidate_root": str(candidate),
            "profile_candidate": str(profile_path),
            "predecessor_profile_sha256": expected_predecessor,
            "target_profile_sha256": target_profile_hash,
            "dataset_manifest_sha256": manifest_identity,
            "dataset_manifest_file_sha256": manifest_file_hash,
            "node_manifest_file_sha256": dict(sorted(node_hashes.items())),
            "evidence_refs": normalized_evidence,
        }
        semantic_digest = _digest(semantic)
        observed = datetime.now(UTC)
        self.store.operations.mkdir(parents=True, exist_ok=True)
        self.store.locks.mkdir(parents=True, exist_ok=True)
        with _exclusive_lock(self.store.locks / f"{product_profile}.lock"):
            for child in self.store.operations.iterdir():
                request_path = child / "request.json"
                if not request_path.is_file():
                    continue
                existing = _read_json(request_path, label="existing request")
                if (
                    existing.get("product_profile") == product_profile
                    and existing.get("idempotency_key") == idempotency_key
                ):
                    if existing.get("semantic_digest") != semantic_digest:
                        raise MonthlyReleaseConflict("adoption idempotency key is already bound")
                    return self.store.read_state(str(existing["operation_id"]))
                if (
                    existing.get("product_profile") == product_profile
                    and existing.get("target_cutoff") == target_cutoff.isoformat()
                ):
                    raise MonthlyReleaseConflict("target cutoff is already bound to another operation")

            operation_id = f"dmr_{uuid.uuid4().hex}"
            root = self.store.operation_root(operation_id)
            root.mkdir(parents=False, exist_ok=False)
            (root / "checkpoints").mkdir()
            (root / "receipts").mkdir()
            (root / "logs").mkdir()
            request_payload = {
                **semantic,
                "operation_id": operation_id,
                "idempotency_key": idempotency_key,
                "semantic_digest": semantic_digest,
                "requested_by": principal,
                "created_at": observed.isoformat(),
            }
            adoption = {
                "schema_version": "aistock_monthly_existing_successor_adoption_receipt_v1",
                "operation_id": operation_id,
                "predecessor_profile_ref": {
                    "id": predecessor.path.name,
                    "sha256": predecessor.file_sha256,
                    "size": predecessor.path.stat().st_size,
                },
                "target_profile_ref": _content_ref(profile_path, ref_id=profile_path.name),
                "dataset_manifest_ref": _content_ref(
                    manifest_path, ref_id="qe_dataset_manifest.json"
                ),
                "node_manifest_file_sha256": dict(sorted(node_hashes.items())),
                "evidence_refs": normalized_evidence,
                "candidate_write_performed": False,
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            adoption["canonical_sha256"] = _digest(adoption)
            _write_exclusive(root / "receipts" / "adoption.json", adoption)
            predecessor_ref = adoption["predecessor_profile_ref"]
            plan = {
                "schema_version": PLAN_SCHEMA,
                "operation_id": operation_id,
                "plan_state": "ADOPTED_VERIFIED_SUCCESSOR",
                "target_cutoff": target_cutoff.isoformat(),
                "cutoff": target_cutoff.isoformat(),
                "product_profile": product_profile,
                "generation": str(profile["generation"]),
                "revision": str(manifest.get("revision") or "legacy-hotfix"),
                "release_id": str(profile["release_id"]),
                "predecessor": {
                    "profile_path": str(predecessor.path),
                    "profile_sha256": predecessor.file_sha256,
                    "generation": predecessor.generation,
                    "release_id": predecessor.release_id,
                    "cutoff": predecessor.cutoff.isoformat(),
                    "dataset_manifest_sha256": predecessor.dataset_manifest_sha256,
                    "candidate_root": predecessor.candidate_root,
                },
                "predecessor_profile_ref": predecessor_ref,
                "candidate_root": str(candidate),
                "profile_candidate": str(profile_path),
                "node_roots": actual_node_roots,
                "target_roots": {"controller": str(candidate), **actual_node_roots},
                "required_consumers": list(REQUIRED_CONSUMERS),
                "actions": {component: ComponentAction.REUSE.value for component in COMPONENTS},
                "adoption_ref": _content_ref(
                    root / "receipts" / "adoption.json", ref_id="receipts/adoption.json"
                ),
            }
            ready = {
                "schema_version": ADOPTED_READY_SCHEMA,
                "operation_id": operation_id,
                "status": ReleaseState.READY_TO_ACTIVATE.value,
                "request_digest": semantic_digest,
                "plan_digest": _digest(plan),
                "dataset_manifest_sha256": manifest_identity,
                "generation": str(profile["generation"]),
                "release_id": str(profile["release_id"]),
                "cutoff": target_cutoff.isoformat(),
                "candidate_root": str(candidate),
                "profile_candidate": str(profile_path),
                "candidate_profile_ref": adoption["target_profile_ref"],
                "predecessor_profile_ref": predecessor_ref,
                "adoption_ref": plan["adoption_ref"],
                "node_manifest_file_sha256": dict(sorted(node_hashes.items())),
                "unresolved_count": 0,
                "candidate_write_performed": False,
                "database_write_performed": False,
                "production_ddl_performed": False,
                "production_dml_performed": False,
                "runtime_action_performed": False,
                "active_profile_write": False,
            }
            ready["canonical_sha256"] = _digest(ready)
            state = {
                "schema_version": STATE_SCHEMA,
                "operation_id": operation_id,
                "status": ReleaseState.READY_TO_ACTIVATE.value,
                "attempt": 0,
                "current_stage": None,
                "cancel_requested": False,
                "last_error": None,
                "created_at": observed.isoformat(),
                "updated_at": observed.isoformat(),
                "plan_sha256": _digest(plan),
                "candidate_root": str(candidate),
                "profile_candidate": str(profile_path),
            }
            _write_exclusive(root / "request.json", request_payload)
            _write_exclusive(root / "plan.json", plan)
            _write_exclusive(root / "receipts" / "ready.json", ready)
            _write_exclusive(root / "state.json", state)
            return state

    def issue_action_authorization(
        self,
        operation_id: str,
        *,
        authorization_store: "ActionAuthorizationStore",
        action: str,
        principal: str,
    ) -> dict[str, Any]:
        if action != "ACTIVATE":
            raise MonthlyReleaseAuthorizationError("only ACTIVATE can be issued here")
        state = self.store.read_state(operation_id)
        if state.get("status") != ReleaseState.READY_TO_ACTIVATE.value:
            raise MonthlyReleaseNotReady("operation is not ready for authorization")
        plan = self.store.read_plan(operation_id)
        ready = _read_json(
            self.store.operation_root(operation_id) / "receipts" / "ready.json",
            label="ready receipt",
        )
        if ready.get("operation_id") != operation_id or ready.get("status") != state.get("status"):
            raise MonthlyReleaseConflict("ready receipt differs from operation state")
        predecessor_hash = str(plan["predecessor"]["profile_sha256"])
        if _file_sha256(self.active_profile) != predecessor_hash:
            raise MonthlyReleaseConflict("active profile changed before authorization")
        target_hash = _file_sha256(Path(str(plan["profile_candidate"])))
        if target_hash != str(ready["candidate_profile_ref"]["sha256"]):
            raise MonthlyReleaseConflict("target profile changed before authorization")
        return authorization_store.issue(
            operation_id=operation_id,
            action=action,
            principal=principal,
            target_cutoff=str(plan["target_cutoff"]),
            predecessor_profile_sha256=predecessor_hash,
            target_profile_sha256=target_hash,
        )

    def status(self, operation_id: str) -> dict[str, Any]:
        state = self.store.read_state(operation_id)
        checkpoints = {
            stage: self.store.read_checkpoint(operation_id, stage) is not None for stage in STAGES
        }
        return {**state, "checkpoints": checkpoints}

    def cancel(self, operation_id: str) -> dict[str, Any]:
        return self.store.request_cancel(operation_id)

    def resume(self, operation_id: str) -> dict[str, Any]:
        state = self.store.read_state(operation_id)
        status = ReleaseState(str(state["status"]))
        if status not in {
            ReleaseState.SOURCE_BLOCKED,
            ReleaseState.FAILED,
            ReleaseState.CANCELLED,
        }:
            raise MonthlyReleaseConflict("operation is not resumable")
        return self.store.update_state(
            operation_id,
            status=ReleaseState.PLANNED.value,
            current_stage=None,
            cancel_requested=False,
            last_error=None,
        )

    def bind_repair_inputs(self, operation_id: str, *, inputs: Mapping[str, Any], principal: str) -> dict[str, Any]:
        """Bind exact existing repair assets before SOURCE, never rewrite a seal.

        This is an authenticated preparation command, not an activation grant
        or READY attestation. The same worker lock serializes it with run().
        """
        from .monthly_legacy_prefix import load_legacy_prefix
        from .monthly_repair_inputs import validate_monthly_repair_inputs
        root = self.store.operation_root(operation_id)
        with _exclusive_lock(root / ".writer.lock", blocking=False):
            plan = self.store.read_plan(operation_id)
            existing = plan.get("monthly_repair_inputs")
            if existing is not None:
                if canonical_json_bytes(existing) != canonical_json_bytes(inputs):
                    raise MonthlyReleaseConflict("monthly repair inputs are already bound to different assets")
                reference = _require_content_ref(plan["monthly_repair_inputs_ref"], field="monthly repair input binding")
                path = root / reference["id"]
                receipt = _read_json(path, label="repair input binding")
                if path.stat().st_size != reference["size"] or _file_sha256(path) != reference["sha256"] or receipt.get("inputs") != existing:
                    raise MonthlyReleaseConflict("monthly repair input binding bytes differ")
                return receipt
            state = self.store.read_state(operation_id)
            if state["status"] not in {ReleaseState.PLANNED.value, ReleaseState.FAILED.value,
                    ReleaseState.SOURCE_BLOCKED.value, ReleaseState.CANCELLED.value} or any(
                self.store.read_checkpoint(operation_id, stage) is not None for stage in STAGES
            ) or plan.get("plan_state") != "AWAITING_SOURCE_SNAPSHOT":
                raise MonthlyReleaseConflict("monthly repair inputs can only bind before any SOURCE seal")
            predecessor = plan["predecessor"]
            if _file_sha256(self.active_profile) != predecessor["profile_sha256"]:
                raise MonthlyReleaseConflict("active predecessor changed before repair binding")
            try:
                prefix = load_legacy_prefix(Path(predecessor["candidate_root"]),
                    expected_manifest_sha256=predecessor["dataset_manifest_sha256"],
                    expected_file_sha256=plan["predecessor_manifest_ref"]["sha256"],
                    expected_cutoff=date.fromisoformat(predecessor["cutoff"]), expected_release_id=predecessor["release_id"])
                normalized = validate_monthly_repair_inputs(inputs, predecessor=prefix,
                    target_cutoff=date.fromisoformat(plan["target_cutoff"]))
            except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
                raise MonthlyReleaseRequestInvalid("monthly repair input validation failed", context={"cause": str(exc)}) from exc
            if _file_sha256(self.active_profile) != predecessor["profile_sha256"]:
                raise MonthlyReleaseConflict("active predecessor changed during repair binding")
            digest = _digest(normalized)
            relative = f"inputs/repair-{digest}.json"
            path = root / relative
            receipt = {"schema_version": "aistock_monthly_repair_input_binding_v1", "operation_id": operation_id,
                "input_sha256": digest, "inputs": normalized, "bound_by": principal,
                "bound_at": datetime.now(UTC).isoformat(), "database_write": False,
                "active_profile_write": False, "runtime_action": False, "publication_allowed": False}
            if path.exists():
                receipt = _read_json(path, label="existing repair input binding")
                if receipt.get("inputs") != normalized or receipt.get("operation_id") != operation_id:
                    raise MonthlyReleaseConflict("existing repair input binding differs")
            else:
                _write_exclusive(path, receipt)
            self.store.replace_plan(operation_id, {**plan, "monthly_repair_inputs": normalized,
                "monthly_repair_inputs_ref": {"id": relative, "sha256": _file_sha256(path), "size": path.stat().st_size}})
            return receipt

    def bind_source_quality_inputs(self, operation_id: str, *, inputs: Mapping[str, Any], principal: str) -> dict[str, Any]:
        """Bind finite parity warnings to this operation before SOURCE seals."""
        from .monthly_source_quality import (
            BINDING_SCHEMA, acceptance_file_sha256, load_bound_source_quality, validate_source_quality_acceptance,
        )
        root = self.store.operation_root(operation_id)
        with _exclusive_lock(root / ".writer.lock", blocking=False):
            plan = self.store.read_plan(operation_id)
            predecessor = plan["predecessor"]
            try:
                normalized = validate_source_quality_acceptance(inputs, operation_id=operation_id,
                    predecessor_manifest_sha256=predecessor["dataset_manifest_sha256"],
                    target_cutoff=date.fromisoformat(plan["target_cutoff"]))
            except (ValueError, KeyError, TypeError, OSError) as exc:
                raise MonthlyReleaseRequestInvalid("source quality input validation failed", context={"cause": str(exc)}) from exc
            existing = plan.get("monthly_source_quality_inputs")
            if existing is not None or plan.get("monthly_source_quality_inputs_ref") is not None:
                try:
                    bound = load_bound_source_quality(plan, operation_id=operation_id, operation_root=root)
                except (ValueError, KeyError, TypeError, OSError) as exc:
                    raise MonthlyReleaseConflict("source quality binding bytes differ") from exc
                if bound != normalized:
                    raise MonthlyReleaseConflict("source quality inputs are already bound to different observations")
                return _read_json(root / plan["monthly_source_quality_inputs_ref"]["id"], label="source quality binding")
            state = self.store.read_state(operation_id)
            if (state["status"] not in {ReleaseState.PLANNED.value, ReleaseState.FAILED.value,
                    ReleaseState.SOURCE_BLOCKED.value, ReleaseState.CANCELLED.value}
                    or any(self.store.read_checkpoint(operation_id, stage) is not None for stage in STAGES)
                    or plan.get("plan_state") != "AWAITING_SOURCE_SNAPSHOT"):
                raise MonthlyReleaseConflict("source quality inputs can only bind before any SOURCE seal")
            if _file_sha256(self.active_profile) != predecessor["profile_sha256"]:
                raise MonthlyReleaseConflict("active predecessor changed before quality binding")
            digest = acceptance_file_sha256(normalized)
            relative = f"inputs/quality-{digest}.json"
            path = root / relative
            receipt = {"schema_version": BINDING_SCHEMA, "operation_id": operation_id,
                "input_sha256": digest, "inputs": normalized, "bound_by": principal,
                "bound_at": datetime.now(UTC).isoformat(), "database_write": False,
                "active_profile_write": False, "runtime_action": False, "publication_allowed": False}
            if path.exists():
                receipt = _read_json(path, label="existing source quality binding")
                if (receipt.get("inputs") != normalized or receipt.get("operation_id") != operation_id
                        or receipt.get("schema_version") != BINDING_SCHEMA or receipt.get("input_sha256") != digest
                        or not receipt.get("bound_by")
                        or any(receipt.get(flag) is not False for flag in ("database_write", "active_profile_write", "runtime_action", "publication_allowed"))):
                    raise MonthlyReleaseConflict("existing source quality binding differs")
            else:
                _write_exclusive(path, receipt)
            if _file_sha256(self.active_profile) != predecessor["profile_sha256"]:
                raise MonthlyReleaseConflict("active predecessor changed during quality binding")
            self.store.replace_plan(operation_id, {**plan, "monthly_source_quality_inputs": normalized,
                "monthly_source_quality_binding_path": str(path),
                "monthly_source_quality_inputs_ref": {"id": relative, "sha256": _file_sha256(path), "size": path.stat().st_size}})
            return receipt

    def _finalize_plan_from_source(
        self,
        operation_id: str,
        *,
        plan: Mapping[str, Any],
        source_receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        scope = source_receipt.get("scope")
        if not isinstance(scope, Mapping):
            raise MonthlyReleaseError("source receipt scope is invalid")
        actions = scope.get("component_actions")
        if not isinstance(actions, Mapping) or set(actions) != set(COMPONENTS):
            raise MonthlyReleaseError("source receipt does not contain a complete action plan")
        if any(str(value) not in {item.value for item in ComponentAction} for value in actions.values()):
            raise MonthlyReleaseError("source receipt contains an unknown component action")
        source_as_of = str(scope.get("source_as_of") or "")
        try:
            parsed_source_as_of = datetime.fromisoformat(source_as_of)
        except ValueError as exc:
            raise MonthlyReleaseError("source receipt source_as_of is invalid") from exc
        if parsed_source_as_of.tzinfo is None:
            raise MonthlyReleaseError("source receipt source_as_of must include a timezone")

        output_refs = {
            str(item["id"]): dict(item) for item in source_receipt.get("output_refs", ())
        }

        def refs(field: str) -> list[dict[str, Any]]:
            raw = scope.get(field)
            if not isinstance(raw, list) or not raw:
                raise MonthlyReleaseError(f"source receipt {field} is missing")
            normalized = [_require_content_ref(item, field=f"source.{field}") for item in raw]
            if any(item["id"] not in output_refs or output_refs[item["id"]] != item for item in normalized):
                raise MonthlyReleaseError(f"source receipt {field} is not pinned by output_refs")
            return normalized

        finalized = dict(plan)
        proposed = {
            "plan_state": "SOURCE_SNAPSHOT_FROZEN",
            "source_as_of": parsed_source_as_of.astimezone(UTC).isoformat(),
            "source_snapshot_group_id": str(scope.get("snapshot_group_id") or ""),
            "snapshot_identity_ref": refs("snapshot_identity_refs")[0],
            "repair_overlap_check_ref": refs("repair_overlap_check_refs")[0],
            "change_scope_refs": refs("change_scope_refs"),
            "source_gate_refs": refs("source_gate_refs"),
            "producer_contract_refs": refs("producer_contract_refs"),
            "actions": {name: str(actions[name]) for name in COMPONENTS},
        }
        if plan.get("plan_state") == "SOURCE_SNAPSHOT_FROZEN":
            if any(plan.get(key) != value for key, value in proposed.items()):
                raise MonthlyReleaseConflict("source snapshot differs from the frozen release plan")
            return dict(plan)
        if plan.get("plan_state") != "AWAITING_SOURCE_SNAPSHOT":
            raise MonthlyReleaseError("monthly plan state is invalid")
        if not proposed["source_snapshot_group_id"]:
            raise MonthlyReleaseError("source snapshot group identity is empty")
        finalized.update(proposed)
        return self.store.replace_plan(operation_id, finalized)

    def _stage_control(
        self, operation_id: str, *, attempt: int, stage: str,
    ) -> tuple[Callable[[], None], Callable[[Mapping[str, Any]], None]]:
        # Diagnostic state only. It cannot seal a checkpoint, authorize reuse,
        # or participate in release identity. Never infer totals/ETA from I/O.
        component_counts = {
            "instrument_count", "completed_instruments", "total_instruments", "completed_feature_files",
            "physical_rows_read", "indexed_tail_requests", "inherited_rows_serialized",
            "inherited_bytes_copied", "month_rows_written",
        }
        component_rates = {"component_elapsed_seconds", "instruments_per_second"}
        component_metrics = component_counts | component_rates
        observation: dict[str, Any] = {
            "phase": stage, "query_id": None, "partition_key": None,
            "rows_validated": None, "rows_sealed": None, "partitions_sealed": None,
            "dataset": None, "frequency": None, **dict.fromkeys(component_metrics),
        }
        last_write: float | None = None
        started_at = time.monotonic()

        def require_current(state: Mapping[str, Any]) -> None:
            if state.get("attempt") != attempt or state.get("current_stage") != stage:
                raise MonthlyReleaseConflict("stage control identity changed")
            if state.get("cancel_requested") is True:
                raise MonthlyReleaseCancelled("monthly release cancellation requested")

        def checkpoint() -> None:
            nonlocal last_write
            require_current(self.store.read_state(operation_id))
            now = time.monotonic()
            if last_write is not None and now - last_write < 15:
                return
            root = self.store.operation_root(operation_id)
            with _exclusive_lock(root / ".operation.lock"):
                state = self.store.read_state(operation_id)
                require_current(state)
                observed_at = datetime.now(UTC).isoformat()
                elapsed = max(0.0, now - started_at)
                state["stage_progress"] = {
                    "schema_version": "aistock_monthly_stage_progress_v1",
                    "operation_id": operation_id, "attempt": attempt, "stage": stage,
                    "observed_at": observed_at, **observation,
                    "total_rows": None, "total_partitions": None,
                    "elapsed_seconds": round(elapsed, 3),
                    "rows_validated_per_second": (
                        observation["rows_validated"] / elapsed
                        if observation["rows_validated"] is not None and elapsed > 0 else None
                    ),
                    "rows_sealed_per_second": (
                        observation["rows_sealed"] / elapsed
                        if observation["rows_sealed"] is not None and elapsed > 0 else None
                    ),
                }
                state["updated_at"] = observed_at
                _replace_json(root / "state.json", state)
            last_write = now

        def progress(value: Mapping[str, Any]) -> None:
            nonlocal last_write
            if not isinstance(value, Mapping):
                raise MonthlyReleaseError("stage progress fields differ")
            value = dict(value)
            if "elapsed_seconds" in value:
                # The writer's duration is not the whole stage's clock.
                value["component_elapsed_seconds"] = value.pop("elapsed_seconds")
            if set(value) - observation.keys():
                raise MonthlyReleaseError("stage progress fields differ")
            for field, item in value.items():
                if field in {"rows_validated", "rows_sealed", "partitions_sealed"}:
                    if type(item) is not int or item < 0 or (
                        observation[field] is not None and item < observation[field]
                    ):
                        raise MonthlyReleaseError("stage progress counts are invalid")
                elif field in component_counts:
                    if type(item) is not int or item < 0:
                        raise MonthlyReleaseError("stage progress counts are invalid")
                elif field in component_rates:
                    if type(item) not in (int, float) or not math.isfinite(item) or item < 0:
                        raise MonthlyReleaseError("stage progress rate is invalid")
                elif (item is None and field == "phase") or (
                    item is not None and (not isinstance(item, str) or not item or len(item) > 512)
                ):
                    raise MonthlyReleaseError("stage progress label is invalid")
            new_dataset = "dataset" in value and value["dataset"] != observation["dataset"]
            new_frequency = "frequency" in value and value["frequency"] != observation["frequency"]
            if new_dataset or new_frequency:
                observation.update(dict.fromkeys(component_metrics))
                if new_dataset:
                    observation["frequency"] = None
                last_write = None
            if value.get("phase", observation["phase"]) != observation["phase"]:
                last_write = None  # Flush phase boundaries even for a short stage.
            observation.update(value)
            checkpoint()

        return checkpoint, progress

    def run(self, operation_id: str) -> dict[str, Any]:
        root = self.store.operation_root(operation_id)
        with _exclusive_lock(root / ".writer.lock", blocking=False):
            request = self.store.read_request(operation_id)
            plan = self.store.read_plan(operation_id)
            state = self.store.read_state(operation_id)
            if ReleaseState(state["status"]) == ReleaseState.SOURCE_BLOCKED:
                # A stale queue observation must not turn a blocked source into
                # an implicit retry. A concurrent cancellation needs no producer.
                if state.get("cancel_requested") is True:
                    return self.store.update_state(
                        operation_id, status=ReleaseState.CANCELLED.value, current_stage=None
                    )
                return state
            if ReleaseState(state["status"]) in {
                ReleaseState.READY_TO_ACTIVATE,
                ReleaseState.ACTIVATED,
                ReleaseState.ACTIVATED_VERIFIED,
            }:
                return state
            attempt = int(state.get("attempt") or 0) + 1
            self.store.update_state(operation_id, attempt=attempt, last_error=None, stage_progress=None)
            prior: dict[str, Mapping[str, Any]] = {}
            request_digest = str(request["semantic_digest"])
            for stage in STAGES:
                plan = self.store.read_plan(operation_id)
                plan_digest = _digest(plan)
                identity_reader = getattr(self.pipeline, "stage_identity", None)
                current_producer_digest = (
                    str(identity_reader(stage)) if callable(identity_reader) else None
                )
                current_input_set_digest = _digest(
                    {
                        "prior_receipts": {
                            name: str(receipt["canonical_sha256"])
                            for name, receipt in sorted(prior.items())
                        }
                    }
                )
                existing = self.store.read_checkpoint(
                    operation_id,
                    stage,
                    request_digest=request_digest,
                    plan_digest=plan_digest,
                    producer_digest=current_producer_digest,
                    input_set_digest=current_input_set_digest,
                )
                checkpoint_validator = getattr(self.pipeline, "checkpoint_is_valid", None)
                if existing is not None and callable(checkpoint_validator):
                    if not checkpoint_validator(stage, existing):
                        existing = None
                if existing is not None:
                    prior[stage] = existing
                    continue
                current = self.store.read_state(operation_id)
                if current.get("cancel_requested") is True:
                    return self.store.update_state(
                        operation_id,
                        status=ReleaseState.CANCELLED.value,
                        current_stage=None,
                    )
                entered = {
                    "SOURCE": ReleaseState.CHECKING_SOURCE,
                    "BUILD": ReleaseState.BUILDING,
                    "DERIVE": ReleaseState.DERIVING,
                    "LOCAL_VALIDATE": ReleaseState.VALIDATING,
                    "DEPLOY": ReleaseState.DEPLOYING,
                    "CONSUMER_VALIDATE": ReleaseState.VALIDATING,
                }[stage]
                self.store.update_state(
                    operation_id,
                    status=entered.value,
                    current_stage=stage,
                    stage_progress=None,
                )
                try:
                    checkpoint, progress = self._stage_control(operation_id, attempt=attempt, stage=stage)
                    arguments = dict(
                        stage=stage,
                        operation_id=operation_id,
                        attempt=attempt,
                        request=request,
                        plan=plan,
                        prior_receipts=prior,
                    )
                    controlled_runner = getattr(self.pipeline, "run_stage_with_control", None)
                    # Legacy/non-registered pipelines keep their original call
                    # signature; production's registered worker supports hooks.
                    checkpoint()
                    if callable(controlled_runner):
                        receipt = controlled_runner(**arguments, checkpoint=checkpoint, progress=progress)
                    else:
                        receipt = self.pipeline.run_stage(**arguments)
                    checkpoint()
                    if stage == "SOURCE":
                        plan = self._finalize_plan_from_source(
                            operation_id,
                            plan=plan,
                            source_receipt=receipt,
                        )
                        plan_digest = _digest(plan)
                    self.store.write_checkpoint(
                        operation_id,
                        stage=stage,
                        attempt=attempt,
                        receipt=receipt,
                        request_digest=request_digest,
                        plan_digest=plan_digest,
                        input_set_digest=current_input_set_digest,
                        producer_digest=current_producer_digest,
                    )
                    prior[stage] = dict(receipt)
                    if stage == "SOURCE":
                        self.store.update_state(operation_id, status=ReleaseState.SOURCE_READY.value)
                    elif stage == "BUILD":
                        self.store.update_state(operation_id, status=ReleaseState.DATA_SEALED.value)
                except MonthlyReleaseCancelled:
                    return self.store.update_state(
                        operation_id, status=ReleaseState.CANCELLED.value,
                        current_stage=None, last_error=None,
                    )
                except MonthlyReleaseSourceBlocked as exc:
                    return self.store.update_state(
                        operation_id,
                        status=ReleaseState.SOURCE_BLOCKED.value,
                        current_stage=stage,
                        last_error={"code": exc.code, "message": str(exc), "context": exc.context},
                    )
                except Exception as exc:
                    code = exc.code if isinstance(exc, MonthlyReleaseError) else "MONTHLY_RELEASE_STAGE_FAILED"
                    return self.store.update_state(
                        operation_id,
                        status=ReleaseState.FAILED.value,
                        current_stage=stage,
                        last_error={"code": code, "message": monthly_error_message(exc), "type": type(exc).__name__,
                                    "exception_chain": monthly_error_diagnostics(exc)},
                    )
            try:
                ready = self._close_ready(
                    operation_id, request=request, plan=plan, receipts=prior
                )
                ready_path = root / "receipts" / "ready.json"
                _replace_json(ready_path, ready)
            except Exception as exc:
                code = (
                    exc.code
                    if isinstance(exc, MonthlyReleaseError)
                    else "MONTHLY_RELEASE_CLOSURE_FAILED"
                )
                return self.store.update_state(
                    operation_id,
                    status=ReleaseState.FAILED.value,
                    current_stage="RELEASE_CLOSURE",
                    last_error={
                        "code": code,
                        "message": monthly_error_message(exc),
                        "type": type(exc).__name__,
                        "exception_chain": monthly_error_diagnostics(exc),
                    },
                )
            return self.store.update_state(
                operation_id,
                status=ReleaseState.READY_TO_ACTIVATE.value,
                current_stage=None,
                ready_receipt_sha256=_file_sha256(ready_path),
            )

    def _close_ready(
        self,
        operation_id: str,
        *,
        request: Mapping[str, Any],
        plan: Mapping[str, Any],
        receipts: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        if set(receipts) != set(STAGES):
            raise MonthlyReleaseNotReady("not all stages have durable receipts")
        source_scope = receipts["SOURCE"].get("scope")
        if not isinstance(source_scope, Mapping) or set(source_scope.get("gates") or ()) != set(SOURCE_GATES):
            raise MonthlyReleaseNotReady("source receipt does not close every required gate")
        deploy_scope = receipts["DEPLOY"].get("scope")
        if not isinstance(deploy_scope, Mapping) or set(deploy_scope.get("nodes") or ()) != set(REQUIRED_NODES):
            raise MonthlyReleaseNotReady("deployment receipt does not close every required node")
        consumer_scope = receipts["CONSUMER_VALIDATE"].get("scope")
        if not isinstance(consumer_scope, Mapping) or set(consumer_scope.get("consumers") or ()) != set(
            REQUIRED_CONSUMERS
        ):
            raise MonthlyReleaseNotReady("consumer receipt does not close every required consumer")
        manifest_identities = {
            str(receipt.get("scope", {}).get("dataset_manifest_sha256") or "")
            for stage, receipt in receipts.items()
            if stage != "SOURCE"
        }
        if len(manifest_identities) != 1:
            raise MonthlyReleaseNotReady("stage manifest identities differ")
        manifest_identity = ensure_sha256(manifest_identities.pop(), field="dataset_manifest_sha256")
        node_hashes = deploy_scope.get("node_manifest_sha256")
        if not isinstance(node_hashes, Mapping) or set(node_hashes) != set(REQUIRED_NODES) or any(
            value != manifest_identity for value in node_hashes.values()
        ):
            raise MonthlyReleaseNotReady("node registrations do not bind the ready manifest")
        node_registration_refs = deploy_scope.get("node_registration_refs")
        if not isinstance(node_registration_refs, Mapping) or set(node_registration_refs) != set(
            REQUIRED_NODES
        ):
            raise MonthlyReleaseNotReady("node registration references are incomplete")
        normalized_node_refs = {
            node: _require_content_ref(ref, field=f"node_registration_refs.{node}")
            for node, ref in sorted(node_registration_refs.items())
        }
        readbacks = consumer_scope.get("consumer_readbacks")
        readback_refs = consumer_scope.get("consumer_readback_refs")
        if not isinstance(readbacks, Mapping) or set(readbacks) != set(REQUIRED_CONSUMERS):
            raise MonthlyReleaseNotReady("consumer readback coverage is incomplete")
        if not isinstance(readback_refs, Mapping) or set(readback_refs) != set(REQUIRED_CONSUMERS):
            raise MonthlyReleaseNotReady("consumer readback references are incomplete")
        for consumer, readback in readbacks.items():
            if not isinstance(readback, Mapping) or readback.get(
                "dataset_manifest_sha256"
            ) != manifest_identity:
                raise MonthlyReleaseNotReady(f"consumer readback manifest differs: {consumer}")
        normalized_consumer_refs = {
            consumer: _require_content_ref(ref, field=f"consumer_readback_refs.{consumer}")
            for consumer, ref in sorted(readback_refs.items())
        }
        local_scope = receipts["LOCAL_VALIDATE"].get("scope")
        if not isinstance(local_scope, Mapping):
            raise MonthlyReleaseNotReady("local validation scope is invalid")
        closure_ref = _require_content_ref(
            local_scope.get("release_closure_ref"), field="release_closure_ref"
        )
        source_as_of = str(source_scope.get("source_as_of") or "")
        if not source_as_of or plan.get("source_as_of") != source_as_of:
            raise MonthlyReleaseNotReady("source_as_of differs from the frozen release plan")
        profile_path = Path(str(plan["profile_candidate"]))
        if not profile_path.is_absolute():
            raise MonthlyReleaseNotReady("candidate profile is not a regular absolute file")
        try:
            _require_plain_existing_chain(profile_path, label="candidate profile")
        except MonthlyReleaseError as exc:
            raise MonthlyReleaseNotReady(str(exc)) from exc
        if not profile_path.is_file():
            raise MonthlyReleaseNotReady("candidate profile is not a regular absolute file")
        try:
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MonthlyReleaseNotReady("candidate profile is unreadable") from exc
        profile_components = profile.get("components") if isinstance(profile, Mapping) else None
        if (
            not isinstance(profile, Mapping)
            or profile.get("schema_version") != "aistock_active_dataset_profile_v4"
            or not isinstance(profile_components, Mapping)
            or profile_components.get("dataset_manifest_sha256") != manifest_identity
            or profile_components.get("release_closure_sha256") != closure_ref["sha256"]
            or not str(profile_components.get("derived_asset_registry_sha256") or "")
        ):
            raise MonthlyReleaseNotReady("candidate profile does not bind the ready manifest")
        candidate_profile_ref = _content_ref(profile_path, ref_id=profile_path.name)
        predecessor_profile_ref = _require_content_ref(
            plan.get("predecessor_profile_ref"), field="predecessor_profile_ref"
        )
        payload = {
            "schema_version": READY_SCHEMA,
            "operation_id": operation_id,
            "status": ReleaseState.READY_TO_ACTIVATE.value,
            "request_digest": str(request["semantic_digest"]),
            "plan_digest": _digest(plan),
            "dataset_manifest_sha256": manifest_identity,
            "generation": str(plan["generation"]),
            "release_id": str(plan["release_id"]),
            "cutoff": str(plan["target_cutoff"]),
            "candidate_root": str(plan["candidate_root"]),
            "profile_candidate": str(plan["profile_candidate"]),
            "candidate_profile_ref": candidate_profile_ref,
            "closure_ref": closure_ref,
            "predecessor_profile_ref": predecessor_profile_ref,
            "node_registration_refs": normalized_node_refs,
            "consumer_readback_refs": normalized_consumer_refs,
            "source_as_of": source_as_of,
            "unresolved_count": 0,
            "stage_receipt_digests": {
                stage: str(receipts[stage]["canonical_sha256"]) for stage in STAGES
            },
            "telemetry": {
                stage: {field: int(receipts[stage]["counts"][field]) for field in TELEMETRY_COUNT_FIELDS}
                for stage in STAGES
            },
            "database_write_performed": False,
            "production_ddl_performed": False,
            "production_dml_performed": False,
            "candidate_write_performed": True,
            "candidate_deployed": True,
            "training_or_experiment": False,
            "runtime_action_performed": False,
            "active_profile_write": False,
        }
        payload["canonical_sha256"] = _digest(payload)
        return payload

    def activate(
        self,
        operation_id: str,
        *,
        authorization_store: "ActionAuthorizationStore",
        authorization_ref: str,
        principal: str,
        verify_after: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    ) -> dict[str, Any]:
        root = self.store.operation_root(operation_id)
        with _exclusive_lock(self.store.locks / "active-profile.lock"):
            state = self.store.read_state(operation_id)
            if state.get("status") not in {
                ReleaseState.READY_TO_ACTIVATE.value,
                ReleaseState.ACTIVATED.value,
                ReleaseState.ACTIVATED_VERIFY_FAILED.value,
            }:
                raise MonthlyReleaseNotReady("operation is not ready for activation")
            plan = self.store.read_plan(operation_id)
            ready = _read_json(root / "receipts" / "ready.json", label="ready receipt")
            ready_canonical = str(ready.get("canonical_sha256") or "")
            ready_without_digest = dict(ready)
            ready_without_digest.pop("canonical_sha256", None)
            if (
                ready.get("schema_version") not in {READY_SCHEMA, ADOPTED_READY_SCHEMA}
                or ready.get("operation_id") != operation_id
                or ready.get("status") != ReleaseState.READY_TO_ACTIVATE.value
                or ready_canonical != _digest(ready_without_digest)
            ):
                raise MonthlyReleaseConflict("ready receipt identity is invalid")
            profile_candidate = Path(str(plan["profile_candidate"]))
            candidate_profile_ref = _content_ref(
                profile_candidate, ref_id=profile_candidate.name
            )
            if candidate_profile_ref != ready.get("candidate_profile_ref"):
                raise MonthlyReleaseConflict(
                    "profile candidate changed after the READY closure"
                )
            authorization = authorization_store.require(
                authorization_ref,
                operation_id=operation_id,
                action="ACTIVATE",
                principal=principal,
                target_cutoff=str(plan["target_cutoff"]),
                predecessor_profile_sha256=str(plan["predecessor"]["profile_sha256"]),
                target_profile_sha256=candidate_profile_ref["sha256"],
            )
            expected_old = str(plan["predecessor"]["profile_sha256"])
            expected_new = candidate_profile_ref["sha256"]
            activation_path = root / "receipts" / "activation.json"
            ready_path = root / "receipts" / "ready.json"
            activation = {
                "schema_version": ACTIVATION_SCHEMA,
                "operation_id": operation_id,
                "status": ReleaseState.ACTIVATING.value,
                "old_profile_ref": _require_content_ref(
                    plan["predecessor_profile_ref"], field="old_profile_ref"
                ),
                "new_profile_ref": _content_ref(
                    profile_candidate,
                    ref_id=profile_candidate.name,
                ),
                "ready_ref": _content_ref(ready_path, ref_id="receipts/ready.json"),
                "authorization_ref": authorization_ref,
                "authorization_evidence": authorization,
                "intent_at": datetime.now(UTC).isoformat(),
                "apply_state": "PENDING",
                "readback_state": "PENDING",
                "rollback_ref": None,
                "dataset_manifest_sha256": ready["dataset_manifest_sha256"],
                "database_write_performed": False,
                "runtime_action_performed": False,
            }
            activation["canonical_sha256"] = _digest(activation)
            if activation_path.exists():
                prior_activation = _read_json(activation_path, label="activation intent")
                for field in (
                    "old_profile_ref",
                    "new_profile_ref",
                    "ready_ref",
                    "authorization_ref",
                    "dataset_manifest_sha256",
                ):
                    if prior_activation.get(field) != activation[field]:
                        raise MonthlyReleaseConflict("activation intent differs from the durable intent")
                activation["intent_at"] = prior_activation["intent_at"]
            _replace_json(activation_path, activation)
            actual = _file_sha256(self.active_profile)
            if actual == expected_new:
                apply_state = "ALREADY_APPLIED"
            elif actual != expected_old:
                raise MonthlyReleaseConflict("active profile differs from predecessor and target")
            else:
                self.store.update_state(operation_id, status=ReleaseState.ACTIVATING.value)
                previous_profile = root / "receipts" / "previous-profile.json"
                if not previous_profile.exists():
                    predecessor_bytes = self.active_profile.read_bytes()
                    if hashlib.sha256(predecessor_bytes).hexdigest() != expected_old:
                        raise MonthlyReleaseConflict(
                            "active profile changed before predecessor snapshot"
                        )
                    with previous_profile.open("xb") as handle:
                        handle.write(predecessor_bytes)
                        handle.flush()
                        os.fsync(handle.fileno())
                _atomic_copy_cas(
                    source=Path(str(plan["profile_candidate"])),
                    target=self.active_profile,
                    expected_current_sha256=expected_old,
                    expected_source_sha256=expected_new,
                )
                apply_state = "APPLIED"
            self.store.update_state(operation_id, status=ReleaseState.ACTIVATED.value)
            activation.update(
                {
                    "status": ReleaseState.ACTIVATED.value,
                    "apply_state": apply_state,
                    "readback_state": "PENDING",
                }
            )
            activation.pop("canonical_sha256", None)
            activation["canonical_sha256"] = _digest(activation)
            _replace_json(activation_path, activation)
            try:
                verification = dict(verify_after(ready))
                if verification.get("status") != "PASS" or verification.get(
                    "dataset_manifest_sha256"
                ) != ready["dataset_manifest_sha256"]:
                    raise MonthlyReleaseError("activation readback identity differs")
            except Exception as exc:
                activation.update(
                    {
                        "status": ReleaseState.ACTIVATED_VERIFY_FAILED.value,
                        "readback_state": "FAILED",
                    }
                )
                activation.pop("canonical_sha256", None)
                activation["canonical_sha256"] = _digest(activation)
                _replace_json(activation_path, activation)
                return self.store.update_state(
                    operation_id,
                    status=ReleaseState.ACTIVATED_VERIFY_FAILED.value,
                    last_error={
                        "code": "ACTIVATION_READBACK_FAILED",
                        "message": str(exc),
                        "type": type(exc).__name__,
                    },
                )
            _replace_json(root / "receipts" / "activation-readback.json", verification)
            activation.update(
                {
                    "status": ReleaseState.ACTIVATED_VERIFIED.value,
                    "readback_state": "PASS",
                }
            )
            activation.pop("canonical_sha256", None)
            activation["canonical_sha256"] = _digest(activation)
            _replace_json(activation_path, activation)
            return self.store.update_state(
                operation_id,
                status=ReleaseState.ACTIVATED_VERIFIED.value,
                last_error=None,
            )

    def rollback(
        self,
        operation_id: str,
        *,
        authorization_store: "ActionAuthorizationStore",
        authorization_ref: str,
        principal: str,
        verify_after: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Restore the exact predecessor pointer without deleting referenced releases."""

        root = self.store.operation_root(operation_id)
        with _exclusive_lock(self.store.locks / "active-profile.lock"):
            state = self.store.read_state(operation_id)
            if state.get("status") not in {
                ReleaseState.ACTIVATED.value,
                ReleaseState.ACTIVATED_VERIFIED.value,
                ReleaseState.ACTIVATED_VERIFY_FAILED.value,
                ReleaseState.ROLLBACK_VERIFY_FAILED.value,
            }:
                raise MonthlyReleaseNotReady("operation has not been activated")
            plan = self.store.read_plan(operation_id)
            previous_profile = root / "receipts" / "previous-profile.json"
            if not previous_profile.is_file():
                raise MonthlyReleaseNotReady("predecessor profile snapshot is unavailable")
            current_sha = _file_sha256(self.active_profile)
            activated_sha = _file_sha256(Path(str(plan["profile_candidate"])))
            target_sha = _file_sha256(previous_profile)
            if current_sha not in {activated_sha, target_sha}:
                raise MonthlyReleaseConflict("active profile no longer equals this operation target")
            authorization = authorization_store.require(
                authorization_ref,
                operation_id=operation_id,
                action="ROLLBACK",
                principal=principal,
                target_cutoff=str(plan["target_cutoff"]),
                predecessor_profile_sha256=activated_sha,
                target_profile_sha256=target_sha,
            )
            receipt = {
                "schema_version": "aistock_monthly_release_rollback_v2",
                "operation_id": operation_id,
                "authorization": authorization,
                "restored_profile_sha256": target_sha,
                "superseded_profile_sha256": activated_sha,
                "apply_state": "PENDING",
                "readback_state": "PENDING",
                "referenced_releases_deleted": False,
                "runtime_action_performed": False,
            }
            receipt["canonical_sha256"] = _digest(receipt)
            rollback_path = root / "receipts" / "rollback.json"
            _replace_json(rollback_path, receipt)
            activation_path = root / "receipts" / "activation.json"

            def pin_rollback_receipt() -> None:
                activation = _read_json(activation_path, label="activation receipt")
                activation["rollback_ref"] = _content_ref(
                    rollback_path, ref_id="receipts/rollback.json"
                )
                activation.pop("canonical_sha256", None)
                activation["canonical_sha256"] = _digest(activation)
                _replace_json(activation_path, activation)

            pin_rollback_receipt()
            if current_sha == target_sha:
                apply_state = "ALREADY_APPLIED"
            else:
                _atomic_copy_cas(
                    source=previous_profile,
                    target=self.active_profile,
                    expected_current_sha256=current_sha,
                    expected_source_sha256=target_sha,
                )
                apply_state = "APPLIED"
            receipt.update({"apply_state": apply_state, "readback_state": "PENDING"})
            receipt.pop("canonical_sha256", None)
            receipt["canonical_sha256"] = _digest(receipt)
            _replace_json(rollback_path, receipt)
            pin_rollback_receipt()
            try:
                verification = dict(verify_after(plan["predecessor"]))
                if verification.get("profile_sha256") != target_sha:
                    raise MonthlyReleaseError("rollback readback identity differs")
            except Exception as exc:
                receipt["readback_state"] = "FAILED"
                receipt.pop("canonical_sha256", None)
                receipt["canonical_sha256"] = _digest(receipt)
                _replace_json(rollback_path, receipt)
                pin_rollback_receipt()
                return self.store.update_state(
                    operation_id,
                    status=ReleaseState.ROLLBACK_VERIFY_FAILED.value,
                    last_error={
                        "code": "ROLLBACK_READBACK_FAILED",
                        "message": str(exc),
                        "type": type(exc).__name__,
                    },
                )
            receipt["readback_state"] = "PASS"
            receipt.pop("canonical_sha256", None)
            receipt["canonical_sha256"] = _digest(receipt)
            _replace_json(rollback_path, receipt)
            pin_rollback_receipt()
            _replace_json(root / "receipts" / "rollback-readback.json", verification)
            self.store.update_state(
                operation_id,
                status=ReleaseState.ROLLBACK_VERIFIED.value,
                last_error=None,
            )
            return receipt


class ActionAuthorizationStore:
    """Resolve exact action grants; arbitrary non-empty strings are rejected."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("authorization root must be absolute")
        self.root = root

    def issue(
        self,
        *,
        operation_id: str,
        action: str,
        principal: str,
        target_cutoff: str,
        predecessor_profile_sha256: str,
        target_profile_sha256: str,
    ) -> dict[str, Any]:
        if not OPERATION_ID_RE.fullmatch(operation_id):
            raise MonthlyReleaseAuthorizationError("operation id is invalid")
        if action not in {"ACTIVATE", "ROLLBACK"} or not principal.strip():
            raise MonthlyReleaseAuthorizationError("authorization scope is invalid")
        self.root.mkdir(parents=True, exist_ok=True)
        _require_plain_existing_chain(self.root, label="action authorization root")
        authorization_id = f"dsauth_{uuid.uuid4().hex}"
        value = {
            "schema_version": AUTHORIZATION_SCHEMA,
            "authorization_id": authorization_id,
            "operation_id": operation_id,
            "action": action,
            "principal": principal,
            "target_cutoff": target_cutoff,
            "predecessor_profile_sha256": ensure_sha256(
                predecessor_profile_sha256, field="predecessor_profile_sha256"
            ),
            "target_profile_sha256": ensure_sha256(
                target_profile_sha256, field="target_profile_sha256"
            ),
            "status": "APPROVED",
            "approved_at": datetime.now(UTC).isoformat(),
        }
        value["canonical_sha256"] = _digest(value)
        path = self.root / f"{authorization_id}.json"
        _write_exclusive(path, value)
        return {
            "authorization_id": authorization_id,
            "authorization_file_sha256": _file_sha256(path),
            "canonical_sha256": value["canonical_sha256"],
            "principal": principal,
        }

    def require(
        self,
        authorization_ref: str,
        *,
        operation_id: str,
        action: str,
        principal: str,
        target_cutoff: str,
        predecessor_profile_sha256: str,
        target_profile_sha256: str,
    ) -> dict[str, Any]:
        if not AUTHORIZATION_ID_RE.fullmatch(authorization_ref):
            raise MonthlyReleaseAuthorizationError("authorization reference format is invalid")
        path = self.root / f"{authorization_ref}.json"
        if path.parent.resolve(strict=True) != self.root.resolve(strict=True):
            raise MonthlyReleaseAuthorizationError("authorization escaped registered root")
        try:
            _require_plain_existing_chain(path, label="action authorization")
        except MonthlyReleaseError as exc:
            raise MonthlyReleaseAuthorizationError(str(exc)) from exc
        value = _read_json(path, label="action authorization")
        required = {
            "schema_version",
            "authorization_id",
            "operation_id",
            "action",
            "principal",
            "target_cutoff",
            "predecessor_profile_sha256",
            "target_profile_sha256",
            "status",
            "approved_at",
            "canonical_sha256",
        }
        if set(value) != required:
            raise MonthlyReleaseAuthorizationError("authorization fields differ")
        expected = {
            "schema_version": AUTHORIZATION_SCHEMA,
            "authorization_id": authorization_ref,
            "operation_id": operation_id,
            "action": action,
            "principal": principal,
            "target_cutoff": target_cutoff,
            "predecessor_profile_sha256": predecessor_profile_sha256,
            "target_profile_sha256": target_profile_sha256,
            "status": "APPROVED",
        }
        if any(value.get(key) != expected_value for key, expected_value in expected.items()):
            raise MonthlyReleaseAuthorizationError("authorization scope differs")
        payload = dict(value)
        claimed = ensure_sha256(str(payload.pop("canonical_sha256")), field="canonical_sha256")
        if _digest(payload) != claimed:
            raise MonthlyReleaseAuthorizationError("authorization digest differs")
        return {
            "authorization_id": authorization_ref,
            "authorization_file_sha256": _file_sha256(path),
            "canonical_sha256": claimed,
            "principal": principal,
        }


def _atomic_copy_cas(
    *, source: Path, target: Path, expected_current_sha256: str, expected_source_sha256: str
) -> None:
    _require_plain_existing_chain(source, label="profile candidate")
    _require_plain_existing_chain(target, label="active profile")
    expected_source = ensure_sha256(expected_source_sha256, field="expected_source_sha256")
    expected_current = ensure_sha256(expected_current_sha256, field="expected_current_sha256")
    source_bytes = source.read_bytes()
    if hashlib.sha256(source_bytes).hexdigest() != expected_source:
        raise MonthlyReleaseConflict("profile candidate identity changed")
    if _file_sha256(target) != expected_current:
        raise MonthlyReleaseConflict("active profile changed before activation")
    descriptor, raw = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(source_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        if _file_sha256(temporary) != expected_source:
            raise MonthlyReleaseError("profile temporary copy identity differs")
        if _file_sha256(target) != expected_current:
            raise MonthlyReleaseConflict("active profile changed during activation")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    if _file_sha256(target) != expected_source:
        raise MonthlyReleaseError("active profile readback differs after activation")


__all__: Sequence[str] = (
    "ADOPTED_READY_SCHEMA",
    "ACTIVATION_SCHEMA",
    "AUTHORIZATION_SCHEMA",
    "ActionAuthorizationStore",
    "ActiveProfileSnapshot",
    "COMPONENTS",
    "CONSUMER_READBACK_SCHEMA",
    "CONSUMER_VALIDATION_BINDING_SCHEMA",
    "ComponentAction",
    "MonthlyOperationStore",
    "MonthlyPipeline",
    "MonthlyReleaseAuthorizationError",
    "MonthlyReleaseBusy",
    "MonthlyReleaseCancelled",
    "MonthlyReleaseConflict",
    "MonthlyReleaseError",
    "MonthlyReleaseNotReady",
    "MonthlyReleaseRequest",
    "MonthlyReleaseRequestInvalid",
    "MonthlyReleaseService",
    "MonthlyReleaseSourceBlocked",
    "NODE_REGISTRATION_SCHEMA",
    "READY_SCHEMA",
    "RELEASE_CLOSURE_SCHEMA",
    "REQUIRED_CONSUMERS",
    "REQUIRED_NODES",
    "ReleaseState",
    "SOURCE_GATES",
    "STAGE_RECEIPT_SCHEMA",
    "STAGES",
    "TELEMETRY_COUNT_FIELDS",
    "SourceChange",
    "build_stage_receipt",
    "classify_component_actions",
    "validate_stage_receipt",
)
