"""公共运行验收的纯语义合同；不执行 HTTP、进程控制或数据库操作。"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sys
import urllib.parse
from datetime import datetime, timezone
from typing import Any

BUSINESS_SMOKE_SEMANTIC_SCHEMA = "aistock_business_smoke_semantic_verdict_v1"
SCHEDULER_VERIFICATION_STATUS_SCHEMA = "simulation_scheduler_verification_status_v1"
SCHEDULER_VERIFICATION_SCOPE_SCHEMA = "simulation_scheduler_verification_scope_v1"
_SCHEDULER_VERIFICATION_RUN_ID_RE = re.compile(r"^simrun_[0-9a-f]{16}$")
EXPECTED_TERMINAL_OUTCOME_SCHEMA = "aistock_expected_terminal_outcome_v1"
SIMULATION_RUN_TERMINAL_EVIDENCE_PAYLOAD_SCHEMA = "simulation_run_terminal_evidence_v1"
_EXPECTATION_CAPABLE_CONTRACT_IDS = frozenset({"run_terminal_evidence"})
_EXPECTED_TERMINAL_OUTCOME_FIELDS = (
    "schema_version",
    "contract_id",
    "expected_status",
    "expected_previous_status",
    "expected_reason_code",
    "expected_evidence_schema",
    "expected_run_id",
)
_EXPECTED_REASON_CODE_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
_EXPECTED_EVIDENCE_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]{2,127}$")
_EXPECTED_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{2,127}$")

_RUN_STATUS_SUCCESS = frozenset({
    "APPLIED",
    "CLOSED",
    "COMPLETED",
    "DONE",
    "FILLED",
    "PASSED",
    "READY",
    "SUCCESS",
    "SUCCEEDED",
})
_RUN_STATUS_FAILURE = frozenset({
    "ABORTED",
    "BLOCKED",
    "CANCELED",
    "CANCELLED",
    "DEAD",
    "ERROR",
    "FAILED",
    "FAILED_RETRYABLE",
    "FAILED_TERMINAL",
    "REJECTED",
    "TIMED_OUT",
    "TIMEOUT",
})
_HEALTH_FAILURE_STATUSES = _RUN_STATUS_FAILURE | {"DEGRADED", "DOWN", "STALLED", "UNHEALTHY"}
_HEALTH_SUCCESS_STATUSES = frozenset({"healthy", "ok", "passed", "ready", "success", "succeeded", "up"})
_CONTAINER_KEYS = ("run", "result", "batch", "task", "plan", "operation", "data", "scheduler", "summary")
_STATUS_FIELD_NAMES = ("status", "state", "verdict")

_COLLECTION_LIST_KEYS = (
    "items",
    "runs",
    "tasks",
    "nodes",
    "schedules",
    "entries",
    "results",
    "rows",
    "records",
    "batches",
    "experiments",
    "strategies",
    "programs",
    "data",
)
_SCHEDULER_BLOCKING_LIST_KEYS = frozenset({
    "blockers",
    "blocking_reasons",
    "errors",
    "failure_reasons",
    "last_result_errors",
})
_SCHEDULER_CURRENT_TRADE_DATE_BLOCKERS_KEY = "current_trade_date_blockers"
_SCHEDULER_BLOCKING_VALUE_KEYS = frozenset({"blocking_result", "last_blocking_result"})
_SCHEDULER_REASON_KEYS = frozenset({
    "blocked_reason",
    "blocking_reason",
    "failure_reason",
    "recovery_failure_reason",
})


def _collect_status_fields(payload: Any) -> dict[str, str]:
    """Collect status/state/verdict fields from a payload and known containers."""
    found: dict[str, str] = {}
    queue: list[tuple[str, Any]] = [("", payload)]
    depth = 0
    while queue and depth <= 4:
        depth += 1
        label, container = queue.pop(0)
        if not isinstance(container, dict):
            continue
        for field in _STATUS_FIELD_NAMES:
            value = container.get(field)
            if isinstance(value, str) and value.strip():
                name = f"{label}.{field}" if label else field
                found.setdefault(name, value.strip())
        for key in _CONTAINER_KEYS:
            nested = container.get(key)
            if isinstance(nested, dict):
                nested_label = f"{label}.{key}" if label else key
                queue.append((nested_label, nested))
    return found


def _normalize_expected_terminal_outcome(raw: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Validate and normalize a declared expected terminal outcome.

    The declaration is the only channel through which a failure-class terminal
    status may satisfy a run-class business-smoke semantic contract. It is
    fail-closed: any schema violation yields blocking errors and no
    expectation. Normalization emits the fixed field set in canonical order so
    digests are stable.
    """
    if raw is None:
        return None, []
    if not isinstance(raw, dict):
        return None, ["expected_terminal_outcome must be an object"]
    errors: list[str] = []
    unknown_fields = sorted(set(raw) - set(_EXPECTED_TERMINAL_OUTCOME_FIELDS))
    if unknown_fields:
        errors.append(f"expected_terminal_outcome has unknown fields: {unknown_fields}")
    if raw.get("schema_version") != EXPECTED_TERMINAL_OUTCOME_SCHEMA:
        errors.append(f"expected_terminal_outcome schema_version must be {EXPECTED_TERMINAL_OUTCOME_SCHEMA}")
    contract_id = str(raw.get("contract_id") or "").strip()
    if contract_id not in _EXPECTATION_CAPABLE_CONTRACT_IDS:
        errors.append(
            "expected_terminal_outcome contract_id is not expectation-capable: "
            f"{contract_id or 'missing'}"
        )
    expected_status = str(raw.get("expected_status") or "").strip().upper()
    if expected_status not in _RUN_STATUS_FAILURE:
        errors.append(
            "expected_terminal_outcome expected_status must be a declared failure-class "
            f"terminal status: {expected_status or 'missing'}"
        )
    expected_previous_status = str(raw.get("expected_previous_status") or "").strip().upper()
    if expected_previous_status not in (_RUN_STATUS_FAILURE | _RUN_STATUS_SUCCESS):
        errors.append(
            "expected_terminal_outcome expected_previous_status must be a known terminal "
            f"status: {expected_previous_status or 'missing'}"
        )
    expected_reason_code = str(raw.get("expected_reason_code") or "").strip()
    if not _EXPECTED_REASON_CODE_RE.fullmatch(expected_reason_code):
        errors.append(
            "expected_terminal_outcome expected_reason_code must be an upper snake-case "
            f"reason code: {expected_reason_code or 'missing'}"
        )
    expected_evidence_schema = str(raw.get("expected_evidence_schema") or "").strip()
    if not _EXPECTED_EVIDENCE_SCHEMA_RE.fullmatch(expected_evidence_schema):
        errors.append(
            "expected_terminal_outcome expected_evidence_schema must be a lower snake-case "
            f"schema id: {expected_evidence_schema or 'missing'}"
        )
    expected_run_id = str(raw.get("expected_run_id") or "").strip()
    if not _EXPECTED_RUN_ID_RE.fullmatch(expected_run_id):
        errors.append(
            "expected_terminal_outcome expected_run_id must be a non-empty run id "
            f"(alphanumeric, underscore or dash): {expected_run_id or 'missing'}"
        )
    if errors:
        return None, errors
    normalized = {
        "schema_version": EXPECTED_TERMINAL_OUTCOME_SCHEMA,
        "contract_id": contract_id,
        "expected_status": expected_status,
        "expected_previous_status": expected_previous_status,
        "expected_reason_code": expected_reason_code,
        "expected_evidence_schema": expected_evidence_schema,
        "expected_run_id": expected_run_id,
    }
    return normalized, []


def _expectation_outcome_digest(expectation: dict[str, Any] | None) -> str | None:
    if expectation is None:
        return None
    encoded = json.dumps(expectation, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_run_terminal_success(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Run-class payloads must prove terminal target closure."""
    if not isinstance(payload, dict):
        return "failed", "run payload must be a JSON object", {}
    if payload.get("ok") is False:
        return "failed", "run payload reports ok=false", {}
    statuses = _collect_status_fields(payload)
    if not statuses:
        return "failed", "run payload is missing a status/state field to prove target closure", {}
    normalized = {name: value.upper() for name, value in statuses.items()}
    failures = {name: value for name, value in normalized.items() if value in _RUN_STATUS_FAILURE}
    if failures:
        detail = ", ".join(f"{name}={value}" for name, value in sorted(failures.items()))
        return "failed", f"run payload reports failure status: {detail}", {"statuses": normalized}
    open_states = {name: value for name, value in normalized.items() if value not in _RUN_STATUS_SUCCESS}
    if open_states:
        detail = ", ".join(f"{name}={value}" for name, value in sorted(open_states.items()))
        return (
            "failed",
            f"run payload has not reached terminal target closure: {detail}",
            {"statuses": normalized},
        )
    return "passed", None, {"statuses": normalized}


def _validate_run_terminal_evidence(
    payload: Any,
    *,
    expectation: dict[str, Any] | None = None,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bounded terminal-evidence payloads prove an exact expected terminal outcome.

    Without a declared expectation this contract is exactly as strict as
    ``run_terminal_success``: every status/state/verdict field in the payload
    and its known containers is scanned, and failure-class or non-terminal
    values fail closed. With a declared expectation the verdict passes only
    when the subject run id, the run status, the evidence carrier schema, the
    terminal reason code and the previous status all match the declaration
    exactly; every mismatch fails closed with the specific drift.
    """
    if not isinstance(payload, dict):
        return "failed", "run terminal-evidence payload must be a JSON object", {}
    if payload.get("ok") is False:
        return "failed", "run terminal-evidence payload reports ok=false", {}
    if payload.get("schema_version") != SIMULATION_RUN_TERMINAL_EVIDENCE_PAYLOAD_SCHEMA:
        return (
            "failed",
            "run terminal-evidence payload schema mismatch: "
            f"expected {SIMULATION_RUN_TERMINAL_EVIDENCE_PAYLOAD_SCHEMA}, "
            f"observed {payload.get('schema_version')!r}",
            {},
        )
    run = payload.get("run")
    if not isinstance(run, dict):
        return "failed", "run terminal-evidence payload is missing the run object", {}
    raw_status = run.get("status")
    if not isinstance(raw_status, str) or not raw_status.strip():
        return "failed", "run terminal-evidence payload is missing run.status", {}
    status = raw_status.strip().upper()
    facts: dict[str, Any] = {"statuses": {"run.status": status}}
    if expectation is None:
        statuses = _collect_status_fields(payload)
        statuses.setdefault("run.status", status)
        normalized = {name: value.upper() for name, value in statuses.items()}
        facts = {"statuses": normalized}
        failures = {name: value for name, value in normalized.items() if value in _RUN_STATUS_FAILURE}
        if failures:
            detail = ", ".join(f"{name}={value}" for name, value in sorted(failures.items()))
            return "failed", f"run payload reports failure status: {detail}", facts
        open_states = {name: value for name, value in normalized.items() if value not in _RUN_STATUS_SUCCESS}
        if open_states:
            detail = ", ".join(f"{name}={value}" for name, value in sorted(open_states.items()))
            return (
                "failed",
                f"run payload has not reached terminal target closure: {detail}",
                facts,
            )
        return "passed", None, facts
    expected_status = str(expectation.get("expected_status") or "").strip().upper()
    if expected_status not in _RUN_STATUS_FAILURE:
        return (
            "failed",
            "declared expected terminal status is not failure-class: "
            f"{expected_status or 'missing'}",
            facts,
        )
    expected_run_id = str(expectation.get("expected_run_id") or "").strip()
    observed_run_id = str(run.get("run_id") or "").strip()
    facts["run_id"] = observed_run_id
    if not expected_run_id or observed_run_id != expected_run_id:
        return (
            "failed",
            "run id does not match the declared expected terminal outcome subject: "
            f"observed={observed_run_id or 'missing'} expected={expected_run_id or 'missing'}",
            facts,
        )
    if status != expected_status:
        return (
            "failed",
            f"run status does not match the declared expected terminal status: "
            f"run.status={status} expected={expected_status}",
            facts,
        )
    carriers = run.get("terminal_evidence")
    if not isinstance(carriers, list):
        return (
            "failed",
            "run terminal-evidence payload is missing the terminal_evidence carrier list",
            facts,
        )
    expected_schema = str(expectation.get("expected_evidence_schema") or "").strip()
    carrier = next(
        (
            item
            for item in carriers
            if isinstance(item, dict) and str(item.get("schema_version") or "").strip() == expected_schema
        ),
        None,
    )
    if carrier is None:
        return (
            "failed",
            f"expected terminal evidence carrier is absent: schema={expected_schema}",
            facts,
        )
    facts["matched_evidence"] = {
        "schema_version": expected_schema,
        "reason_code": carrier.get("reason_code"),
        "previous_status": carrier.get("previous_status"),
        "terminal_status": carrier.get("terminal_status"),
    }
    expected_reason = str(expectation.get("expected_reason_code") or "").strip()
    if str(carrier.get("reason_code") or "").strip() != expected_reason:
        return (
            "failed",
            f"terminal evidence reason code does not match the declaration: "
            f"observed={carrier.get('reason_code')!r} expected={expected_reason}",
            facts,
        )
    expected_previous = str(expectation.get("expected_previous_status") or "").strip().upper()
    observed_previous = str(carrier.get("previous_status") or "").strip().upper()
    if observed_previous != expected_previous:
        return (
            "failed",
            f"terminal evidence previous status does not match the declaration: "
            f"observed={observed_previous or 'missing'} expected={expected_previous}",
            facts,
        )
    observed_terminal = str(carrier.get("terminal_status") or "").strip().upper()
    if observed_terminal != expected_status:
        return (
            "failed",
            f"terminal evidence terminal status does not match the declaration: "
            f"observed={observed_terminal or 'missing'} expected={expected_status}",
            facts,
        )
    return "passed", None, facts


def _structured_current_trade_date_blockers_are_clear(value: Any) -> bool:
    """Accept only the scheduler's explicit, internally consistent CLEAR shape."""
    if not isinstance(value, dict):
        return False
    return bool(
        str(value.get("status") or "").strip().upper() == "CLEAR"
        and type(value.get("blocker_count")) is int
        and value["blocker_count"] == 0
        and type(value.get("observed_blocker_count")) is int
        and value["observed_blocker_count"] == 0
        and value.get("blockers") == []
        and value.get("execution_gate") is False
        and value.get("truncated") is False
    )


def _scheduler_failure_markers(payload: Any) -> list[str]:
    """Scan a scheduler/health-class payload for blocking or failure markers."""
    markers: list[str] = []
    queue: list[tuple[str, Any]] = [("", payload)]
    visited = 0
    while queue and visited < 400:
        prefix, node = queue.pop(0)
        if isinstance(node, dict):
            visited += 1
            for key, value in node.items():
                name = f"{prefix}.{key}" if prefix else str(key)
                lowered = str(key).lower()
                if lowered == _SCHEDULER_CURRENT_TRADE_DATE_BLOCKERS_KEY:
                    if isinstance(value, list):
                        if value:
                            markers.append(name)
                    elif not _structured_current_trade_date_blockers_are_clear(value):
                        markers.append(name)
                elif lowered in _SCHEDULER_BLOCKING_LIST_KEYS and value:
                    markers.append(name)
                elif lowered in _SCHEDULER_BLOCKING_VALUE_KEYS and isinstance(value, dict) and value:
                    markers.append(name)
                elif lowered in _SCHEDULER_REASON_KEYS and isinstance(value, str) and value.strip():
                    markers.append(name)
                elif lowered in _STATUS_FIELD_NAMES and isinstance(value, str) and value.strip().upper() in _HEALTH_FAILURE_STATUSES:
                    markers.append(f"{name}={value.strip()}")
                if isinstance(value, (dict, list)) and len(str(prefix).split(".")) < 8:
                    queue.append((name, value))
        elif isinstance(node, list):
            visited += 1
            for index, value in enumerate(node):
                if isinstance(value, (dict, list)):
                    queue.append((f"{prefix}[{index}]", value))
    return sorted(set(markers))


def _validate_scheduler_status(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Scheduler/health-class payloads must prove ok=true without blockers."""
    if not isinstance(payload, dict):
        return "failed", "scheduler payload must be a JSON object", {}
    if payload.get("ok") is not True:
        if "ok" not in payload:
            return "failed", "scheduler payload is missing the required ok=true envelope", {}
        return "failed", "scheduler payload reports ok=false", {}
    markers = _scheduler_failure_markers(payload)
    if markers:
        return (
            "failed",
            "scheduler payload exposes blocking/failure markers: " + ", ".join(markers[:8]),
            {"markers": markers},
        )
    return "passed", None, {}


def _validate_scheduler_verification_status(
    payload: Any,
    *,
    url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bind a scoped scheduler verdict to the exact broker/run subject in the probe URL."""

    verdict, reason, facts = _validate_scheduler_status(payload)
    if verdict != "passed":
        return verdict, reason, facts
    scheduler = payload.get("scheduler") if isinstance(payload, dict) else None
    if not isinstance(scheduler, dict):
        return "failed", "scheduler verification payload is missing scheduler object", {}
    if scheduler.get("schema_version") != SCHEDULER_VERIFICATION_STATUS_SCHEMA:
        return "failed", "scheduler verification payload schema_version is invalid", {}
    if scheduler.get("read_only") is not True:
        return "failed", "scheduler verification payload is not read-only", {}
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    unknown_query = sorted(set(query) - {"broker_backend", "run_id"})
    if unknown_query:
        return "failed", f"scheduler verification probe has unknown subject query fields: {unknown_query}", {}
    if not query or any(len(values) != 1 for values in query.values()):
        return "failed", "scheduler verification probe must declare one value per broker/run subject field", {}
    broker_backend = query.get("broker_backend", [None])[0]
    run_id = query.get("run_id", [None])[0]
    if broker_backend not in {None, "local_sim", "minqmt_sim"}:
        return "failed", "scheduler verification probe broker_backend is invalid", {}
    if run_id is not None and _SCHEDULER_VERIFICATION_RUN_ID_RE.fullmatch(run_id) is None:
        return "failed", "scheduler verification probe run_id is invalid", {}
    if broker_backend is None and run_id is None:
        return "failed", "scheduler verification probe subject is missing", {}
    scope = scheduler.get("verification_scope")
    blockers = scheduler.get("current_trade_date_blockers")
    blocker_scope = blockers.get("verification_scope") if isinstance(blockers, dict) else None
    if not isinstance(scope, dict) or scope.get("schema_version") != SCHEDULER_VERIFICATION_SCOPE_SCHEMA:
        return "failed", "scheduler verification response scope schema is invalid", {}
    if scope.get("active") is not True:
        return "failed", "scheduler verification response scope is not active", {}
    observed_scope = {
        "broker_backend": scope.get("broker_backend"),
        "run_id": scope.get("run_id"),
    }
    if broker_backend is None and observed_scope["broker_backend"] not in {"local_sim", "minqmt_sim"}:
        return "failed", "scheduler verification response resolved broker_backend is invalid", {}
    expected_scope = {
        "broker_backend": broker_backend or observed_scope["broker_backend"],
        "run_id": run_id,
    }
    if observed_scope != expected_scope:
        return (
            "failed",
            "scheduler verification response scope does not match probe subject: "
            f"observed={observed_scope} expected={expected_scope}",
            {"verification_scope": observed_scope},
        )
    if blocker_scope != scope:
        return "failed", "scheduler verification blocker scope does not match response scope", {}
    return "passed", None, {"verification_scope": observed_scope}


def _validate_health_ok(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Health endpoints must report ok=true or an explicitly healthy status."""
    if not isinstance(payload, dict):
        return "failed", "health payload must be a JSON object", {}
    if payload.get("ok") is False:
        return "failed", "health payload reports ok=false", {}
    status = payload.get("status") or payload.get("state")
    if isinstance(status, str) and status.strip().upper() in _HEALTH_FAILURE_STATUSES:
        return "failed", f"health payload reports unhealthy status: {status.strip()}", {}
    errors = payload.get("errors")
    if errors:
        return "failed", "health payload reports errors", {}
    if payload.get("ok") is True:
        return "passed", None, {}
    if isinstance(status, str) and status.strip().lower() in _HEALTH_SUCCESS_STATUSES:
        return "passed", None, {"status": status.strip()}
    return "failed", "health payload is missing ok=true or an explicitly healthy status", {}


def _validate_collection_payload(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Collection endpoints must return a list or a non-error list envelope."""
    if isinstance(payload, list):
        return "passed", None, {"kind": "array", "size": len(payload)}
    if not isinstance(payload, dict):
        return "failed", "collection payload must be a JSON array or object", {}
    if payload.get("ok") is False:
        return "failed", "collection payload reports ok=false", {}
    errors = payload.get("errors")
    if errors:
        return "failed", "collection payload reports errors", {}
    for field in _STATUS_FIELD_NAMES:
        value = payload.get(field)
        if isinstance(value, str) and value.strip().upper() in _RUN_STATUS_FAILURE:
            return "failed", f"collection payload reports failure {field}={value.strip()}", {}
    for key in _COLLECTION_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return "passed", None, {"items_key": key, "size": len(value)}
    if payload.get("ok") is True:
        return "passed", None, {"ok": True}
    for field in _STATUS_FIELD_NAMES:
        value = payload.get(field)
        if isinstance(value, str) and value.strip().lower() in _HEALTH_SUCCESS_STATUSES:
            return "passed", None, {field: value.strip()}
    return "failed", "collection payload is missing an items array or an explicit ok/success marker", {}


def _validate_tdx_raw_kline(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """TDX facts, not HTTP success or an empty source, prove the read smoke."""
    if not isinstance(payload, dict) or type(payload.get('code')) is not int or payload['code'] != 0:
        return 'failed', 'TDX response reports failure or lacks wrapper', {}
    data = payload.get('data')
    if not isinstance(data, dict) or not isinstance(data.get('list'), list) or not data['list']:
        return 'failed', 'TDX source fact list is empty or malformed', {}
    rows = data['list']
    if type(data.get('count')) is not int or data['count'] != len(rows):
        return 'failed', 'TDX count differs from fact list', {}
    prior = None
    for row in rows:
        if not isinstance(row, dict):
            return 'failed', 'TDX fact is not an object', {}
        try:
            stamp = datetime.fromisoformat(row['Time'])
        except (KeyError, ValueError, TypeError):
            return 'failed', 'TDX fact timestamp is invalid', {}
        if stamp.tzinfo is None or (prior is not None and stamp <= prior):
            return 'failed', 'TDX timestamps lack offset or are duplicate/unordered', {}
        prior = stamp
        fields = ('Open', 'High', 'Low', 'Close', 'Volume', 'Amount')
        if any(type(row.get(k)) not in (int, float) or abs(row[k]) > 2**63 - 1 or not math.isfinite(row[k]) for k in fields):
            return 'failed', 'TDX fact lacks finite OHLCV/amount', {}
        if (row['Low'] <= 0 or row['High'] < row['Low'] or not row['Low'] <= row['Open'] <= row['High']
                or not row['Low'] <= row['Close'] <= row['High'] or row['Volume'] < 0 or row['Amount'] < 0):
            return 'failed', 'TDX fact OHLCV/amount violates units or bounds', {}
        if 'VolumeShares' in row and (type(row['VolumeShares']) is not int or row['VolumeShares'] < 0
                                     or row['VolumeShares'] // 100 != row['Volume']):
            return 'failed', 'TDX share precision differs from whole hands', {}
    return 'passed', None, {'row_count': len(rows), 'first_time': rows[0]['Time'], 'last_time': rows[-1]['Time']}


def _validate_object_liveness(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Object endpoints must return a non-error object or array payload."""
    if isinstance(payload, list):
        return "passed", None, {"kind": "array", "size": len(payload)}
    if not isinstance(payload, dict):
        return "failed", "object payload must be a JSON object or array", {}
    if payload.get("ok") is False:
        return "failed", "object payload reports ok=false", {}
    errors = payload.get("errors")
    if errors:
        return "failed", "object payload reports errors", {}
    for field in _STATUS_FIELD_NAMES:
        value = payload.get(field)
        if isinstance(value, str) and value.strip().upper() in _RUN_STATUS_FAILURE:
            return "failed", f"object payload reports failure {field}={value.strip()}", {}
    return "passed", None, {"kind": "object"}


def _validate_advisory_entry_price_status(
    payload: Any,
    *,
    url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bind the Entry Price status readback to the requested Advisory program."""

    if not isinstance(payload, dict):
        return "failed", "Entry Price status payload must be a JSON object", {}
    if payload.get("ok") is not True or payload.get("errors"):
        return "failed", "Entry Price status payload must report ok=true without errors", {}
    if payload.get("schema_version") != "advisory_entry_price_status_v1":
        return "failed", "Entry Price status schema_version is invalid", {}

    path = urllib.parse.urlsplit(url).path
    match = re.fullmatch(r"/api/v1/advisory/programs/([^/]+)/entry-price/status", path)
    if match is None:
        return "failed", "Entry Price status probe path is invalid", {}
    requested_program_id = urllib.parse.unquote(match.group(1))
    observed_program_id = payload.get("program_id")
    if not isinstance(observed_program_id, str) or observed_program_id != requested_program_id:
        return "failed", "Entry Price status program_id does not match the requested program", {}

    configured = payload.get("configured")
    database_written = payload.get("database_written")
    status = payload.get("status")
    if type(configured) is not bool:
        return "failed", "Entry Price status configured must be boolean", {}
    if database_written is not False:
        return "failed", "Entry Price status must prove database_written=false", {}
    if not isinstance(status, str):
        return "failed", "Entry Price status must be a string", {}
    if configured:
        if status not in {"CONFIGURED", "QUALITY_REVIEW_REQUIRED"}:
            return "failed", "configured Entry Price status is invalid", {}
        if payload.get("binding_activated") is not False:
            return "failed", "configured Entry Price status must prove binding_activated=false", {}
    elif status != "NOT_CONFIGURED":
        return "failed", "unconfigured Entry Price status must be NOT_CONFIGURED", {}

    return (
        "passed",
        None,
        {
            "program_id": observed_program_id,
            "configured": configured,
            "status": status,
            "database_written": database_written,
        },
    )


def _validate_advisory_sector_entry_original_list(payload: Any, *, url: str) -> tuple[str, str | None, dict[str, Any]]:
    """Validate a bound M1 readback, including proven zero candidates, not profitability."""
    def failed(reason):
        return "failed", reason, {}
    if not isinstance(payload, dict) or payload.get("ok") is not True or payload.get("errors"):
        return failed("sector entry must report ok=true without errors")
    if (payload.get("schema_version") != "economic_sector_daily_service_v1"
            or payload.get("model_family") != "M1_SECTOR_PRICE_VALUE_V1"
            or payload.get("status") not in {"NO_CANDIDATES", "COMPUTED"}):
        return failed("sector entry needs a configured M1 computation, not an empty/unconfigured response")
    parsed = urllib.parse.urlsplit(url)
    match = re.fullmatch(r"/api/v1/advisory/programs/([^/]+)/sector-entry-price", parsed.path)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if match is None or set(query) != {"target_trade_date", "list_version_id"} or any(
        len(values) != 1 or not values[0].strip() for values in query.values()
    ):
        return failed("sector entry probe requires one explicit target_trade_date and list_version_id")
    program = urllib.parse.unquote(match.group(1))
    day, list_id = query["target_trade_date"][0], query["list_version_id"][0]
    receipt = payload.get("candidate_receipt")
    if not isinstance(receipt, dict):
        return failed("sector entry is missing the original published candidate receipt")
    if (payload.get("program_id") != program or receipt.get("program_id") != program
            or payload.get("requested_target_date") != day or payload.get("target_date") != day
            or receipt.get("target_date") != day or payload.get("requested_list_version_id") != list_id
            or receipt.get("list_version_id") != list_id):
        return failed("sector entry program/date/list identity does not match the probe")
    if receipt.get("source_evidence") != "CURRENT_DB_ORIGINAL_PUBLISHED_LIST_NOT_ORIGINAL_DATA_CAPTURE":
        return failed("sector entry must use the original published-list readback, not synthetic/reselected evidence")
    try:
        decision = datetime.strptime(payload.get("decision_date"), "%Y-%m-%d").date()
        target = datetime.strptime(day, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return failed("sector entry dates must be ISO dates")
    if (decision.isoformat() != payload["decision_date"] or target.isoformat() != day
            or decision >= target or receipt.get("decision_date") != decision.isoformat()):
        return failed("sector entry decision must precede its bound target date")
    for key in ("database_written", "outcomes_read", "package_qualification_rechecked"):
        if payload.get(key) is not False or receipt.get(key) is not False:
            return failed(f"sector entry must prove {key}=false")
    if (type(payload.get("fit_count")) is not int or payload["fit_count"] != 0
            or type(receipt.get("new_selection_runs")) is not int or receipt["new_selection_runs"] != 0
            or receipt.get("native_receipt_created") is not False or payload.get("deployable") is not False
            or payload.get("decision_use") != "NAVIGATION_ONLY"
            or payload.get("economic_effectiveness") != "NOT_CONFIRMED"):
        return failed("sector entry must remain zero-fit, zero-selection, non-activating navigation")
    for key in ("binding_version_id", "review_run_id", "selection_run_id"):
        if not isinstance(receipt.get(key), str) or not receipt[key].strip():
            return failed(f"sector entry original receipt is missing {key}")
    for key in ("model_sha256", "bundle_sha256", "config_sha256", "projection_sha256",
                "model_parent_policy_identity", "model_value_policy_identity"):
        if not isinstance(payload.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", payload[key]) is None:
            return failed(f"sector entry is missing a valid {key}")
    policy = receipt.get("source_review_policy_sha256")
    if payload.get("source_review_policy_sha256") != policy or (
        policy is not None and (not isinstance(policy, str) or re.fullmatch(r"[0-9a-f]{64}", policy) is None)
    ):
        return failed("sector entry original review policy identity differs")
    candidates, unmodeled = payload.get("candidates"), payload.get("unmodeled_items")
    count, original_count = receipt.get("candidate_count"), receipt.get("original_list_item_count")
    if (not isinstance(candidates, list) or not isinstance(unmodeled, list)
            or type(count) is not int or not 0 <= count <= 20 or count != len(candidates)
            or type(original_count) is not int or original_count != count + len(unmodeled)
            or receipt.get("unmodeled_items") != unmodeled or receipt.get("candidate_scope") != "ORIGINAL_PUBLISHED_TOP20"
            or (payload["status"] == "NO_CANDIDATES") != (count == 0)):
        return failed("sector entry status/count/original-list partition is inconsistent")
    roster_hash = receipt.get("candidate_roster_sha256")
    if not isinstance(roster_hash, str) or re.fullmatch(r"[0-9a-f]{64}", roster_hash) is None:
        return failed("sector entry original candidate roster hash is missing")
    if count == 0 and roster_hash != hashlib.sha256(b"[]").hexdigest():
        return failed("sector entry empty roster must have the canonical empty-list hash")
    symbols = [row.get("instrument") if isinstance(row, dict) else None for row in candidates]
    if any(not isinstance(symbol, str) or not symbol.strip() for symbol in symbols) or len(set(symbols)) != count:
        return failed("sector entry candidates need unique non-empty instruments")
    try:
        projection = {key: value for key, value in payload.items() if key not in {"ok", "projection_sha256"}}
        encoded = json.dumps(projection, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        projection_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    except (ValueError, TypeError):
        return failed("sector entry projection is not canonical JSON")
    if projection_hash != payload["projection_sha256"]:
        return failed("sector entry projection hash does not match its readback")
    return "passed", None, dict(program_id=program, target_date=day, list_version_id=list_id,
                                status=payload["status"], candidate_count=count, original_list_item_count=original_count,
                                model_sha256=payload["model_sha256"], projection_sha256=projection_hash,
                                database_written=False, fit_count=0)


def _validate_hmm_rotation_l2_overview(
    payload: Any,
    *,
    url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bind Rotation L2 overview readback to one complete persisted run."""

    if (
        not isinstance(payload, dict)
        or payload.get("status") != "ok"
        or payload.get("ok") is False
        or payload.get("errors")
    ):
        return "failed", "Rotation L2 overview must report status=ok", {}
    data = payload.get("data")
    if not isinstance(data, dict):
        return "failed", "Rotation L2 overview is missing data", {}

    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    requested_values = query.get("run_id") or []
    if len(requested_values) != 1 or not str(requested_values[0]).strip():
        return "failed", "Rotation L2 overview probe requires exactly one non-empty run_id query value", {}
    requested_run_id = str(requested_values[0]).strip()
    if re.fullmatch(r"[0-9a-f]{64}", requested_run_id) is None:
        return "failed", "Rotation L2 overview probe run_id must be a lowercase SHA-256", {}
    if data.get("run_id") != requested_run_id:
        return "failed", "Rotation L2 overview run_id does not match the requested run", {}

    facts: dict[str, Any] = {"run_id": requested_run_id}
    for field in ("model_hash", "canonical_row_sha256"):
        value = data.get(field)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            return "failed", f"Rotation L2 overview data.{field} must be a lowercase SHA-256", facts
        facts[field] = value

    parsed_dates: dict[str, str] = {}
    for field in ("trade_date", "as_of_date"):
        value = data.get(field)
        if not isinstance(value, str):
            return "failed", f"Rotation L2 overview data.{field} must be an ISO date", facts
        try:
            parsed_dates[field] = datetime.strptime(value, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return "failed", f"Rotation L2 overview data.{field} must be an ISO date", facts
        if parsed_dates[field] != value:
            return "failed", f"Rotation L2 overview data.{field} must be an ISO date", facts
    if parsed_dates["as_of_date"] >= parsed_dates["trade_date"]:
        return "failed", "Rotation L2 overview as_of_date must precede trade_date", facts

    sector_count = data.get("sector_count")
    available_count = data.get("available_count")
    if type(sector_count) is not int or sector_count != 131:
        return "failed", "Rotation L2 overview must contain the complete 131-sector catalog", facts
    if type(available_count) is not int or not 0 <= available_count <= sector_count:
        return "failed", "Rotation L2 overview available_count is outside the sector catalog", facts

    facts.update(
        {
            **parsed_dates,
            "sector_count": sector_count,
            "available_count": available_count,
        }
    )
    return "passed", None, facts


def _validate_hmm_workers(payload: Any, *, url: str) -> tuple[str, str | None, dict[str, Any]]:
    """Read one explicitly identified worker; never infer a reload from controller health."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    owners = query.get("owner_id") or []
    if len(owners) != 1 or not owners[0].strip():
        return "failed", "worker probe requires exactly one non-empty owner_id", {}
    if not isinstance(payload, dict) or payload.get("status") != "ok" or payload.get("errors") or payload.get("ok") is False:
        return "failed", "HMM worker readback must report status=ok", {}
    data = payload.get("data")
    rows = data.get("workers") if isinstance(data, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return "failed", "HMM worker readback is missing worker records", {}
    matches = [row for row in rows if row.get("owner_id") == owners[0]]
    if len(matches) != 1:
        return "failed", "worker owner_id must match exactly one durable record", {}
    row = matches[0]
    facts = {"owner_id": owners[0], "pid": row.get("pid"), "started_at": row.get("started_at")}
    if (row.get("runtime_status") != "running" or row.get("health") != "healthy"
            or type(row.get("pid")) is not int or row["pid"] < 1
            or not isinstance(row.get("host"), str) or not row["host"].strip()
            or row.get("shutdown_at") is not None or row.get("exit_code") is not None):
        return "failed", "requested worker is not an active healthy process", facts
    try:
        started = datetime.fromisoformat(str(row.get("started_at", "")).replace("Z", "+00:00"))
        poll = datetime.fromisoformat(str(row.get("last_poll_at", "")).replace("Z", "+00:00"))
        if started.tzinfo is None or poll.tzinfo is None:
            raise ValueError("timezone missing")
        age = (datetime.now(timezone.utc) - poll).total_seconds()
        if poll < started or not 0 <= age <= 120 or row.get("healthy_max_poll_age_seconds") != 120:
            raise ValueError("stale/inconsistent heartbeat")
    except (ValueError, TypeError):
        return "failed", "requested worker heartbeat is stale or inconsistent", facts
    return "passed", None, {**facts, "last_poll_at": row["last_poll_at"], "scope": "worker liveness, not code reload or evaluation completion"}


def _validate_hmm_risk_l2_overview(payload: Any, *, url: str) -> tuple[str, str | None, dict[str, Any]]:
    """Read a complete identity-bound Risk L2 run with its formal surface receipt."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    runs = query.get("run_id") or []
    if len(runs) != 1 or re.fullmatch(r"[0-9a-f]{64}", runs[0]) is None:
        return "failed", "Risk L2 probe requires exactly one lowercase SHA-256 run_id", {}
    if not isinstance(payload, dict) or payload.get("status") != "ok" or payload.get("errors") or payload.get("ok") is False:
        return "failed", "Risk L2 overview must report status=ok", {}
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("run_id") != runs[0]:
        return "failed", "Risk L2 overview does not match requested run_id", {}
    facts = {"run_id": runs[0]}
    for field in ("model_hash", "input_hash", "acceptance_hash"):
        value = data.get(field)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            return "failed", f"Risk L2 overview is missing {field} identity", facts
        facts[field] = value
    compact, summary = data.get("compact_summary"), data.get("day_summary")
    if not isinstance(compact, dict) or not isinstance(summary, dict):
        return "failed", "Risk L2 overview is missing persisted row/day evidence", facts
    row_hash = compact.get("row_hash")
    if not isinstance(row_hash, str) or re.fullmatch(r"[0-9a-f]{64}", row_hash) is None:
        return "failed", "Risk L2 overview has invalid canonical row identity", facts
    fields = ("sector_count", "available_count", "unavailable_count", "warning_count", "unknown_warning_count")
    if (any(type(summary.get(key)) is not int or not 0 <= summary[key] <= 131 for key in fields)
            or summary["sector_count"] != 131 or summary["available_count"] + summary["unavailable_count"] != 131
            or summary["warning_count"] + summary["unknown_warning_count"] > 131):
        return "failed", "Risk L2 overview must contain balanced 131-sector counts", facts
    try:
        trade = datetime.strptime(data["trade_date"], "%Y-%m-%d").date()
        as_of = datetime.strptime(data["as_of_date"], "%Y-%m-%d").date()
        dates = data["dates"]
        if (trade.isoformat() != data["trade_date"] or as_of.isoformat() != data["as_of_date"]
                or as_of >= trade or not isinstance(dates, list) or not dates or dates[-1] != trade.isoformat()):
            raise ValueError("date mismatch")
    except (KeyError, ValueError, TypeError):
        return "failed", "Risk L2 overview latest-date/PIT evidence is inconsistent", facts
    if data.get("tail_accessed") is not False or data.get("research_surface_status") != "AVAILABLE_EXPERIMENTAL":
        return "failed", "Risk L2 requires formal surface validation and no tail access", facts
    return "passed", None, {**facts, "row_hash": row_hash, "trade_date": trade.isoformat(),
                            "research_surface_status": data["research_surface_status"], **summary}


def _validate_research_pipeline_health(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """A static readiness route cannot prove HMM recording behavior."""
    ready = (isinstance(payload, dict) and payload.get("status") == "success"
             and payload.get("data") == {"service": "research-pipeline", "status": "ok"})
    return "failed", "research health is readiness only; use identity-bound experiments/{id}/backtest-records", {"route_ready": ready}


def _validate_hmm_research_records(payload: Any, *, url: str) -> tuple[str, str | None, dict[str, Any]]:
    parsed = urllib.parse.urlsplit(url)
    experiment = parsed.path.split("/")[-2]
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    tasks = query.get("source_task_id") or []
    if len(tasks) != 1 or not tasks[0].strip() or query.get("research_domain") != ["hmm"]:
        return "failed", "HMM research probe requires explicit source_task_id and research_domain=hmm", {}
    if not isinstance(payload, dict) or payload.get("status") != "success" or payload.get("errors") or payload.get("ok") is False:
        return "failed", "HMM research records must report status=success", {}
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return "failed", "HMM research readback must contain persisted records, not an empty list", {}
    keys = []
    for row in rows:
        if (not isinstance(row, dict) or row.get("experiment_id") != experiment or row.get("source_task_id") != tasks[0]
                or row.get("research_domain") != "hmm" or row.get("pipeline_type") != "hmm_research"
                or row.get("record_version") != "hmm_backtest_record_v1"):
            return "failed", "HMM research record does not match requested experiment/source identity", {}
        for field, pattern in (("record_key_sha256", r"[0-9a-f]{64}"),
                               ("hmm_config_sig", r"[0-9a-f]{12}"),
                               ("non_hmm_config_sig", r"[0-9a-f]{12}")):
            if not isinstance(row.get(field), str) or re.fullmatch(pattern, row[field]) is None:
                return "failed", f"HMM research record is missing {field}", {}
        keys.append(row["record_key_sha256"])
    if len(set(keys)) != len(keys):
        return "failed", "HMM research readback contains duplicate canonical records", {}
    return "passed", None, {"experiment_id": experiment, "source_task_id": tasks[0], "record_count": len(rows),
                            "scope": "existing record readback, not authorization or proof of a new write"}


def _validate_qe_dataset_profile(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """QE dataset-profile must identify one usable active profile."""
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return "failed", "QE dataset profile must report ok=true", {}
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("mode") != "active_profile":
        return "failed", "QE dataset profile must contain data.mode=active_profile", {}
    required = {}
    for field in ("generation", "release_id", "cutoff"):
        value = data.get(field)
        if not isinstance(value, str) or not value.strip():
            return "failed", f"QE dataset profile is missing non-empty data.{field}", {}
        required[field] = value.strip()
    universes = data.get("universes")
    if not isinstance(universes, list) or not universes:
        return "failed", "QE dataset profile must contain a non-empty universes list", required
    pool_ids: list[str] = []
    for universe in universes:
        if not isinstance(universe, dict):
            return "failed", "QE dataset profile contains a malformed universe", required
        pool_id = universe.get("pool_id")
        gap_count = universe.get("gap_count")
        if not isinstance(pool_id, str) or not pool_id.strip():
            return "failed", "QE dataset profile contains a universe without pool_id", required
        if type(gap_count) is not int or gap_count != 0:
            return "failed", f"QE dataset profile universe {pool_id.strip()} has non-zero or invalid gap_count", required
        pool_ids.append(pool_id.strip())
    if len(pool_ids) != len(set(pool_ids)):
        return "failed", "QE dataset profile contains duplicate pool_id values", required
    return "passed", None, {**required, "universe_count": len(pool_ids)}


def _validate_openapi_document(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """The OpenAPI document smoke must prove the app serves its route schema."""
    if not isinstance(payload, dict):
        return "failed", "openapi payload must be a JSON object", {}
    version = payload.get("openapi")
    if not isinstance(version, str) or not version.strip():
        return "failed", "openapi payload is missing the openapi version field", {}
    paths = payload.get("paths")
    if not isinstance(paths, dict) or not paths:
        return "failed", "openapi payload is missing a non-empty paths mapping", {}
    return "passed", None, {"openapi": version.strip(), "paths": len(paths)}


def _validate_correlation_status(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Correlation status reports idle/computing plus explicit refresh errors."""
    if not isinstance(payload, dict):
        return "failed", "correlation status payload must be a JSON object", {}
    status = payload.get("status")
    if not isinstance(status, str) or not status.strip():
        return "failed", "correlation status payload is missing the status field", {}
    normalized = status.strip()
    if normalized.upper() in _HEALTH_FAILURE_STATUSES:
        return "failed", f"correlation status payload reports failure status: {normalized}", {}
    refresh_errors = payload.get("refresh_errors")
    if refresh_errors:
        count = len(refresh_errors) if isinstance(refresh_errors, list) else 1
        return "failed", "correlation status payload reports refresh errors", {"refresh_errors": count}
    if normalized.lower() in {"idle", "computing"}:
        return "passed", None, {"status": normalized}
    return "failed", f"correlation status payload reports unknown status: {normalized}", {}


def _validate_factor_metrics_results(
    payload: Any, *, url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Verify a bound metrics readback, not offline algorithm acceptance."""
    if (not isinstance(payload, dict) or payload.get("ok") is not True
            or payload.get("domain") != "factor_metrics.result" or payload.get("errors")
            or payload.get("error") or payload.get("success") is False
            or ("status" in payload and payload["status"] not in ("ok", "success", "completed"))
            or payload.get("summary_first") is not True):
        return "failed", "factor metrics results require a successful summary envelope", {}
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    bindings = {
        "factor_name": "factor_name", "calc_batch_id": "calc_batch_id", "eval_window": "eval_window",
        "snapshot_date": "expected_snapshot_date", "universe": "expected_universe",
        "return_horizon": "expected_return_horizon",
    }
    expected: dict[str, str] = {}
    for field, key in bindings.items():
        values = query.get(key) or []
        if len(values) != 1 or not values[0].strip():
            return "failed", f"factor metrics probe requires exactly one non-empty {key}", {}
        expected[field] = values[0]
    try:
        snapshot = datetime.strptime(expected["snapshot_date"], "%Y-%m-%d").date()
    except ValueError:
        return "failed", "factor metrics expected_snapshot_date must be an ISO date", {}
    if snapshot.isoformat() != expected["snapshot_date"]:
        return "failed", "factor metrics expected_snapshot_date must be an ISO date", {}
    limits = query.get("limit") or []
    offsets = query.get("offset", ["0"])
    if (len(limits) != 1 or not re.fullmatch(r"[1-9][0-9]{0,2}", limits[0])
            or not 1 <= int(limits[0]) <= 100 or offsets != ["0"]):
        return "failed", "factor metrics probe requires limit=1..100 and offset=0", {}
    limit = int(limits[0])
    items, total, page = payload.get("items"), payload.get("total"), payload.get("pagination")
    if not isinstance(items, list) or not items or type(total) is not int or total <= 0:
        return "failed", "factor metrics readback must contain non-empty persisted results", {}
    if not isinstance(page, dict):
        return "failed", "factor metrics readback is missing pagination", {}
    for field, value in {"limit": limit, "offset": 0, "next_offset": limit, "total": total}.items():
        if type(page.get(field)) is not int or page[field] != value:
            return "failed", f"factor metrics pagination.{field} contradicts the probe/results", {}
    if page.get("has_more") is not (total > limit) or len(items) != min(limit, total):
        return "failed", "factor metrics pagination contradicts item count", {}
    row_ids: set[int] = set()
    for row in items:
        if not isinstance(row, dict):
            return "failed", "factor metrics result must be an object", {}
        for field, value in expected.items():
            if row.get(field) != value:
                return "failed", f"factor metrics {field} does not match the declared probe", {}
        row_id, days = row.get("id"), row.get("n_trading_days")
        if type(row_id) is not int or row_id <= 0 or row_id in row_ids:
            return "failed", "factor metrics ids must be unique positive integers", {}
        row_ids.add(row_id)
        if type(days) is not int or days <= 0:
            return "failed", "factor metrics n_trading_days must be positive", {}
        for field, value in row.items():
            if type(value) in {int, float} and (abs(value) > sys.float_info.max or not math.isfinite(value)):
                return "failed", f"factor metrics {field} must be finite", {}
        for field, bounds in {
            "ic_mean": (-1, 1), "rank_ic_mean": (-1, 1),
            "ic_positive_ratio": (0, 1), "coverage": (0, 1),
            "icir": None, "rank_icir": None,
        }.items():
            value = row.get(field)
            if (type(value) not in {int, float} or abs(value) > sys.float_info.max
                    or not math.isfinite(value) or (bounds and not bounds[0] <= value <= bounds[1])):
                return "failed", f"factor metrics {field} is missing, non-finite or out of range", {}
        calculated_at = row.get("calculated_at")
        try:
            calculated = datetime.fromisoformat(calculated_at.replace("Z", "+00:00"))
        except (AttributeError, TypeError, ValueError):
            return "failed", "factor metrics calculated_at must be a timezone-aware timestamp", {}
        if calculated.tzinfo is None or calculated.utcoffset() is None or calculated.date() < snapshot:
            return "failed", "factor metrics calculated_at precedes the snapshot or lacks timezone", {}
    return "passed", None, {
        **expected, "row_ids": sorted(row_ids), "row_count": len(items), "total": total,
        "acceptance_scope": "bound_metrics_readback_only",
        "offline_algorithm_acceptance": "requires_separate_bug_specific_evidence",
    }


def _validate_factor_lifecycle_detail(
    payload: Any,
    *,
    url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bind factor-library detail readback to an explicit lifecycle expectation."""

    if not isinstance(payload, dict):
        return "failed", "factor detail payload must be a JSON object", {}
    if payload.get("ok") is not True or payload.get("domain") != "factor_library":
        return "failed", "factor detail payload must report ok=true and domain=factor_library", {}
    factor = payload.get("factor")
    if not isinstance(factor, dict):
        return "failed", "factor detail payload is missing factor", {}

    parsed = urllib.parse.urlsplit(url)
    path_match = re.fullmatch(r"/api/v1/factor-library/factors/([^/]+)", parsed.path)
    if path_match is None:
        return "failed", "factor detail probe path is invalid", {}
    requested_name = urllib.parse.unquote(path_match.group(1)).strip()
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    def required_query_value(name: str) -> tuple[str | None, str | None]:
        values = query.get(name) or []
        if len(values) != 1 or not str(values[0]).strip():
            return None, f"factor detail probe requires exactly one non-empty {name} query value"
        return str(values[0]).strip(), None

    requested_source, query_error = required_query_value("source")
    if query_error:
        return "failed", query_error, {}
    expected_available_text, query_error = required_query_value("expected_is_available")
    if query_error:
        return "failed", query_error, {}
    expected_available_normalized = str(expected_available_text).lower()
    if expected_available_normalized not in {"true", "false"}:
        return "failed", "factor detail expected_is_available must be true or false", {}
    expected_available = expected_available_normalized == "true"

    factor_id = factor.get("id")
    factor_name = factor.get("factor_name")
    source = factor.get("source")
    available = factor.get("is_available")
    if type(factor_id) is not int or factor_id <= 0:
        return "failed", "factor detail id must be a positive integer", {}
    if not isinstance(factor_name, str) or factor_name.strip() != requested_name:
        return "failed", "factor detail factor_name does not match the requested factor", {}
    if not isinstance(source, str) or source.strip() != requested_source:
        return "failed", "factor detail source does not match the requested source", {}
    if type(available) is not bool:
        return "failed", "factor detail is_available must be boolean", {}
    if available is not expected_available:
        return (
            "failed",
            f"factor detail availability mismatch: expected={expected_available} observed={available}",
            {},
        )

    facts: dict[str, Any] = {
        "factor_id": factor_id,
        "factor_name": factor_name.strip(),
        "source": source.strip(),
        "is_available": available,
    }
    expected_reason_code_values = query.get("expected_disable_reason_code") or []
    expected_batch_id_values = query.get("expected_disable_batch_id") or []
    if expected_available:
        if expected_reason_code_values or expected_batch_id_values:
            return "failed", "available factor detail must not declare disable expectations", facts
        return "passed", None, facts

    expected_reason_code, query_error = required_query_value("expected_disable_reason_code")
    if query_error:
        return "failed", query_error, facts
    expected_batch_id, query_error = required_query_value("expected_disable_batch_id")
    if query_error:
        return "failed", query_error, facts
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,127}", str(expected_reason_code)):
        return "failed", "factor detail expected_disable_reason_code is invalid", facts
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.\-]{2,127}", str(expected_batch_id)):
        return "failed", "factor detail expected_disable_batch_id is invalid", facts

    disable_reason = factor.get("disable_reason")
    disable_batch_id = factor.get("disable_batch_id")
    disable_at = factor.get("disable_at")
    rehab_candidate = factor.get("rehab_candidate")
    if not isinstance(disable_reason, str) or not disable_reason.strip():
        return "failed", "disabled factor detail is missing disable_reason", facts
    reason_prefix = f"{expected_reason_code}:"
    if not disable_reason.strip().startswith(reason_prefix):
        return "failed", "factor detail disable reason code does not match the declared expectation", facts
    observed_reason_code = expected_reason_code
    if not isinstance(disable_batch_id, str) or disable_batch_id.strip() != expected_batch_id:
        return "failed", "factor detail disable_batch_id does not match the declared expectation", facts
    if not isinstance(disable_at, str) or not disable_at.strip():
        return "failed", "disabled factor detail is missing disable_at", facts
    try:
        parsed_disable_at = datetime.fromisoformat(disable_at.strip().replace("Z", "+00:00"))
    except ValueError:
        return "failed", "factor detail disable_at must be a timezone-aware timestamp", facts
    if parsed_disable_at.tzinfo is None or parsed_disable_at.utcoffset() is None:
        return "failed", "factor detail disable_at must be a timezone-aware timestamp", facts
    if rehab_candidate is not False:
        return "failed", "quarantined factor detail must report rehab_candidate=false", facts

    facts.update(
        {
            "disable_reason_code": observed_reason_code,
            "disable_batch_id": disable_batch_id.strip(),
            "disable_at": parsed_disable_at.isoformat(),
            "rehab_candidate": rehab_candidate,
        }
    )
    return "passed", None, facts


_LOCALSIM_CUTOVER_REQUIRED_RELATIONS = frozenset({
    "paper_v2.simulation_account_v1",
    "paper_v2.legacy_localsim_account_lineage_v1",
    "paper_v2.localsim_replay_job_v1",
    "paper_v2.localsim_runtime_profile_v1",
    "paper_v2.localsim_runtime_profile_version_v1",
    "paper_v2.simulation_ledger_scope_v1",
    "strategy_pkg.strategy_runtime_release",
    "paper_v2.simulation_release_binding",
    "paper_v2.simulation_daily_run",
    "paper_v2.run",
    "paper_v2.intraday_snapshots",
})


def _validate_localsim_cutover_readiness(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Require the complete LocalSIM cutover authority to report a safe boundary."""
    if not isinstance(payload, dict):
        return "failed", "LocalSIM cutover-readiness payload must be a JSON object", {}
    if payload.get("ok") is not True:
        return "failed", "LocalSIM cutover-readiness payload must report ok=true", {}
    readiness = payload.get("readiness")
    if not isinstance(readiness, dict):
        return "failed", "LocalSIM cutover-readiness payload is missing readiness", {}
    if readiness.get("schema_version") != "localsim_cutover_readiness_v1":
        return "failed", "LocalSIM cutover-readiness schema_version is invalid", {}
    checked_at = readiness.get("checked_at")
    if not isinstance(checked_at, str) or not checked_at.strip():
        return "failed", "LocalSIM cutover-readiness checked_at must be a timezone-aware timestamp", {}
    try:
        parsed_checked_at = datetime.fromisoformat(checked_at.strip().replace("Z", "+00:00"))
    except ValueError:
        return "failed", "LocalSIM cutover-readiness checked_at must be a timezone-aware timestamp", {}
    if parsed_checked_at.tzinfo is None:
        return "failed", "LocalSIM cutover-readiness checked_at must be a timezone-aware timestamp", {}

    ready = readiness.get("ready")
    blockers = readiness.get("blockers")
    if type(ready) is not bool:
        return "failed", "LocalSIM cutover-readiness ready must be boolean", {}
    if not isinstance(blockers, list) or any(not isinstance(item, str) or not item.strip() for item in blockers):
        return "failed", "LocalSIM cutover-readiness blockers must be a list of non-empty strings", {}
    if ready != (not blockers):
        return "failed", "LocalSIM cutover-readiness ready and blockers are inconsistent", {}

    relation_presence = readiness.get("relation_presence")
    if (
        not isinstance(relation_presence, dict)
        or not relation_presence
        or any(not isinstance(name, str) or type(present) is not bool for name, present in relation_presence.items())
    ):
        return "failed", "LocalSIM cutover-readiness relation_presence is invalid", {}
    relation_names = set(relation_presence)
    if relation_names != _LOCALSIM_CUTOVER_REQUIRED_RELATIONS:
        missing = sorted(_LOCALSIM_CUTOVER_REQUIRED_RELATIONS - relation_names)
        unexpected = sorted(relation_names - _LOCALSIM_CUTOVER_REQUIRED_RELATIONS)
        return (
            "failed",
            f"LocalSIM cutover-readiness relation_presence keys are invalid: missing={missing}, unexpected={unexpected}",
            {},
        )
    missing_relations = sorted(name for name, present in relation_presence.items() if not present)

    count_fields = (
        "runtime_fk_count",
        "orphan_ledger_scope_count",
        "invalid_ledger_scope_count",
        "legacy_active_session_count",
        "legacy_auto_run_count",
        "legacy_sentinel_count",
        "in_flight_economic_run_count",
    )
    counts: dict[str, int] = {}
    for field in count_fields:
        value = readiness.get(field)
        if type(value) is not int or value < 0:
            return "failed", f"LocalSIM cutover-readiness {field} must be a non-negative integer", {}
        counts[field] = value

    retained_accounts = readiness.get("retained_legacy_account_ids")
    missing_lineages = readiness.get("missing_lineage_account_ids")
    for field, value in (
        ("retained_legacy_account_ids", retained_accounts),
        ("missing_lineage_account_ids", missing_lineages),
    ):
        if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
            return "failed", f"LocalSIM cutover-readiness {field} must be a list of non-empty strings", {}

    facts = {
        "ready": ready,
        "checked_at": parsed_checked_at.isoformat(),
        "blockers": list(blockers),
        "relation_count": len(relation_presence),
        "missing_relations": missing_relations,
        **counts,
        "retained_legacy_account_count": len(retained_accounts),
        "missing_lineage_account_count": len(missing_lineages),
    }
    invariant_failures: list[str] = []
    if missing_relations:
        invariant_failures.append(f"missing_relations={missing_relations}")
    if counts["runtime_fk_count"] != 2:
        invariant_failures.append(f"runtime_fk_count={counts['runtime_fk_count']}")
    for field in count_fields[1:]:
        if counts[field]:
            invariant_failures.append(f"{field}={counts[field]}")
    if missing_lineages:
        invariant_failures.append(f"missing_lineage_account_count={len(missing_lineages)}")
    if blockers:
        invariant_failures.append(f"blockers={blockers}")
    if invariant_failures:
        return "failed", "LocalSIM cutover is not ready: " + "; ".join(invariant_failures), facts
    return "passed", None, facts


def _validate_local_data_freshness(payload: Any, *, url: str) -> tuple[str, str | None, dict[str, Any]]:
    """Verify freshness evidence, not overall health or a cached physical MAX.

    Overview collections are capped by the producer; counters cover all rows.
    Unknown audit evidence and unrelated red alerts are legitimate readback,
    not proof of missing prices and not grounds to fabricate a green result.
    """
    path = urllib.parse.urlsplit(url).path
    overview = path.endswith('/overview')
    operation = 'local_data_health_overview' if overview else 'local_data_list_data_stats'
    if not isinstance(payload, dict) or payload.get('success') is not True:
        return 'failed', 'local-data requires success=true', {}
    if payload.get('operation') != operation or payload.get('risk_level') != 'read_only':
        return 'failed', 'local-data operation/read-only identity differs', {}
    data = payload.get('data')
    if not isinstance(data, dict):
        return 'failed', 'local-data data object missing', {}
    rows = data.get('datasets' if overview else 'items')
    if not isinstance(rows, list) or not rows:
        return 'failed', 'local-data dataset evidence missing', {}
    seen: set[str] = set()
    counts = {'stale': 0, 'unknown': 0, 'quality_blocked': 0}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get('data_kind'), str) or not row['data_kind']:
            return 'failed', 'local-data dataset identity missing', {}
        if row['data_kind'] in seen:
            return 'failed', 'local-data duplicate dataset identity', {}
        seen.add(row['data_kind'])
        required = ('stats_max_date', 'audit_ready_date', 'ready_date', 'physical_max_date',
                    'physical_max_date_source', 'stats_date_source', 'readiness_source',
                    'cache_state', 'readiness_status', 'operator_action_required')
        if any(key not in row for key in required):
            return 'failed', 'local-data freshness fields missing', {}
        if (row['stats_date_source'] != 'data_stats_cache'
                or row['readiness_source'] != 'dataset_date_refresh_audit'
                or row['physical_max_date_source'] != 'not_probed'
                or row['physical_max_date'] is not None):
            return 'failed', 'local-data cache/audit was misrepresented as physical evidence', {}
        for key in ('stats_max_date', 'audit_ready_date'):
            value = row[key]
            if value is not None:
                try:
                    if not isinstance(value, str) or datetime.strptime(value, '%Y-%m-%d').strftime('%Y-%m-%d') != value:
                        raise ValueError('non-canonical date')
                except ValueError:
                    return 'failed', 'local-data date evidence invalid', {}
        ready, cached = row['audit_ready_date'], row['stats_max_date']
        quality = row.get('audit_quality_status')
        if quality is not None and not isinstance(quality, str):
            return 'failed', 'local-data audit quality evidence invalid', {}
        expected_readiness = ('unknown' if not ready else 'quality_blocked'
                              if quality in {'error', 'empty_invalid', 'low_coverage', 'unproven'} else 'audit_success')
        expected_cache = ('fresh' if cached >= ready else 'stale') if ready and cached else (
            'stale' if ready else 'audit_missing' if cached else 'unknown')
        if (row['ready_date'] != ready or row['cache_state'] != expected_cache
                or row['readiness_status'] != expected_readiness
                or type(row['operator_action_required']) is not bool):
            return 'failed', 'local-data cache/readiness semantics differ', {}
        counts['stale'] += expected_cache == 'stale'
        counts['unknown'] += expected_readiness == 'unknown'
        counts['quality_blocked'] += expected_readiness == 'quality_blocked'
    facts: dict[str, Any] = {'dataset_evidence_count': len(rows), **counts}
    if overview:
        fields = ('dataset_count', 'stale_dataset_count', 'stale_stats_cache_count',
                  'readiness_unknown_count', 'quality_blocked_dataset_count',
                  'running_job_count', 'active_alert_count', 'blocked_target_count', 'retry_target_count')
        if any(type(data.get(k)) is not int or data[k] < 0 for k in fields):
            return 'failed', 'local-data overview counters invalid', {}
        if (data['dataset_count'] < len(rows)
                or data['stale_dataset_count'] != data['stale_stats_cache_count']):
            return 'failed', 'local-data overview cache counters differ', {}
        for name, key in [('stale', 'stale_stats_cache_count'), ('unknown', 'readiness_unknown_count'),
                          ('quality_blocked', 'quality_blocked_dataset_count')]:
            if not counts[name] <= data[key] <= data['dataset_count'] or (
                    len(rows) == data['dataset_count'] and counts[name] != data[key]):
                return 'failed', 'local-data overview counters contradict dataset evidence', {}
        status = data.get('status')
        if not isinstance(status, str) or status not in {'green', 'yellow', 'red'} or (
                (data['blocked_target_count'] or data['quality_blocked_dataset_count']) and status != 'red') or (
                status == 'green' and any(data[k] for k in fields if k not in {'dataset_count', 'running_job_count'})):
            return 'failed', 'local-data overview health status contradicts blockers', {}
        facts.update({key: data[key] for key in fields})
        facts['health_status'] = status
    return 'passed', None, facts


def _validate_monthly_release_ready(payload: Any, *, url: str) -> tuple[str, str | None, dict[str, Any]]:
    """A readable operation is not a successfully prepared monthly release."""
    from backend.services.dataset_release.monthly_unified import STATE_SCHEMA, STAGES

    if not isinstance(payload, dict) or payload.get("schema_version") != "aistock_monthly_release_status_v1":
        return "failed", "monthly release status schema differs", {}
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("schema_version") != STATE_SCHEMA:
        return "failed", "monthly release state schema differs", {}
    operation_id = urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1]
    if data.get("operation_id") != operation_id:
        return "failed", "monthly release operation identity differs", {}
    facts = {"operation_id": operation_id, "status": data.get("status")}
    if not isinstance(data.get("status"), str) or data["status"] not in {"READY_TO_ACTIVATE", "ACTIVATED_VERIFIED"}:
        return "failed", "monthly release has not completed prepare/verification", facts
    if (
        data.get("cancel_requested") is not False
        or "last_error" not in data or data["last_error"] not in (None, {})
        or "current_stage" not in data or data["current_stage"] is not None
        or type(data.get("attempt")) is not int or data["attempt"] < 0
    ):
        return "failed", "monthly release ready state contradicts cancellation/error/progress", facts
    for key in ("plan_sha256", "ready_receipt_sha256"):
        value = data.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            return "failed", "monthly release ready content identity missing", facts
        facts[key] = value
    checkpoints = data.get("checkpoints")
    if (
        not isinstance(checkpoints, dict) or set(checkpoints) != set(STAGES)
        or any(value is not True for value in checkpoints.values())
    ):
        return "failed", "monthly release does not have all six successful checkpoints", facts
    facts.update(attempt=data["attempt"], completed_stage_count=len(STAGES))
    return "passed", None, facts
