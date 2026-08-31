"""Governed terminalization for retained-package historical simulation failures.

This module does not delete runs, orders, trades, plans, releases, or bindings.
It changes only historical ``FAILED_RETRYABLE`` run status and its JSON carrier,
under an exact cutoff/package/digest contract.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Sequence

import psycopg2.extras


ADVISORY_LOCK_KEY = 1_165_202_608_24
TERMINALIZATION_CARRIER_KEY = "historical_failed_retryable_terminalization_v1"
RETRY_CONTROL_KEY = "simulation_scheduler_retry_control_v1"
SIDE_EFFECT_EVIDENCE_KEYS = (
    "broker_order_handles",
    "qmt_batch_id",
    "qmt_batch_result",
    "local_sim_persistence",
    "local_sim_projection_outbox_v1",
    "reconcile_after_submit",
    "sync_after_submit",
    "tail_handling",
    "broker_side_effect_state",
    "miniqmt_side_effect_state",
    "miniqmt_submit_timeout",
)


class TerminalizationSafetyError(RuntimeError):
    """Raised when a historical terminalization cannot be proven safe."""


@dataclass(frozen=True)
class FailedRunTerminalizationRequest:
    package_ids: tuple[str, ...]
    cutoff: date

    @classmethod
    def build(cls, package_ids: Iterable[str], cutoff: date) -> "FailedRunTerminalizationRequest":
        normalized = tuple(sorted({str(item).strip() for item in package_ids if str(item).strip()}))
        if len(normalized) < 2:
            raise TerminalizationSafetyError("at least two explicit retained package IDs are required")
        return cls(package_ids=normalized, cutoff=cutoff)


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def classify_historical_failed_run(payload: Mapping[str, Any]) -> dict[str, str]:
    """Return the terminal status without treating missing evidence as no side effect."""

    broker_called = payload.get("broker_called")
    submitted = payload.get("submitted_intents")
    explicit_zero_submitted = type(submitted) is int and submitted == 0
    side_effect_evidence_keys = sorted(key for key in SIDE_EFFECT_EVIDENCE_KEYS if key in payload)
    none_proven = broker_called is False and explicit_zero_submitted and not side_effect_evidence_keys
    if none_proven:
        return {
            "terminal_status": "CANCELLED",
            "side_effect_state": "NONE_PROVEN",
            "reason_code": "SIMULATION_HISTORICAL_RETRY_WINDOW_EXPIRED_NO_SIDE_EFFECT",
        }
    return {
        "terminal_status": "FAILED_TERMINAL",
        "side_effect_state": "PRESENT_OR_UNKNOWN_PRESERVED",
        "reason_code": "SIMULATION_HISTORICAL_RETRY_WINDOW_EXPIRED_SIDE_EFFECT_PRESENT_OR_UNKNOWN",
    }


def _database_identity(cur: Any) -> dict[str, Any]:
    cur.execute(
        "SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, 'local'), "
        "inet_server_port(), current_setting('server_version_num')"
    )
    database, user, server, port, version = cur.fetchone()
    return {
        "database": database,
        "user": user,
        "server": server,
        "port": port,
        "server_version_num": version,
    }


def _require_packages(cur: Any, package_ids: Sequence[str]) -> None:
    cur.execute(
        "SELECT package_id FROM strategy_pkg.package WHERE package_id=ANY(%s) ORDER BY package_id",
        (list(package_ids),),
    )
    found = {str(row[0]) for row in cur.fetchall()}
    missing = sorted(set(package_ids) - found)
    if missing:
        raise TerminalizationSafetyError(f"retained package IDs do not all exist: {missing}")


def _candidate_rows(cur: Any, request: FailedRunTerminalizationRequest, *, for_update: bool) -> list[dict[str, Any]]:
    lock = " FOR UPDATE" if for_update else ""
    cur.execute(
        """
        SELECT run_id,trade_date,strategy_id,broker_backend,package_id,release_id,binding_id,
               execution_plan_id,status,run_payload_json,updated_at
        FROM paper_v2.simulation_daily_run
        WHERE status='FAILED_RETRYABLE' AND trade_date < %s AND package_id=ANY(%s)
        ORDER BY broker_backend,trade_date,run_id
        """
        + lock,
        (request.cutoff, list(request.package_ids)),
    )
    columns = (
        "run_id",
        "trade_date",
        "strategy_id",
        "broker_backend",
        "package_id",
        "release_id",
        "binding_id",
        "execution_plan_id",
        "status",
        "run_payload_json",
        "updated_at",
    )
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def build_terminalization_plan_from_rows(
    *,
    database_identity: Mapping[str, Any],
    request: FailedRunTerminalizationRequest,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    counts = {"CANCELLED": 0, "FAILED_TERMINAL": 0}
    backend_counts: dict[str, int] = {}
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            str(row.get("broker_backend") or ""),
            str(row.get("trade_date") or ""),
            str(row.get("run_id") or ""),
        ),
    )
    for raw in ordered_rows:
        payload = raw.get("run_payload_json")
        if not isinstance(payload, Mapping):
            raise TerminalizationSafetyError(f"run payload is not an object: run_id={raw.get('run_id')}")
        if raw.get("status") != "FAILED_RETRYABLE":
            raise TerminalizationSafetyError(f"candidate status drift: run_id={raw.get('run_id')}")
        package_id = str(raw.get("package_id") or "")
        if package_id not in request.package_ids:
            raise TerminalizationSafetyError(
                f"candidate package is outside retained set: run_id={raw.get('run_id')} package_id={package_id}"
            )
        try:
            trade_date = (
                raw["trade_date"]
                if isinstance(raw.get("trade_date"), date)
                else date.fromisoformat(str(raw.get("trade_date") or ""))
            )
        except (KeyError, ValueError) as exc:
            raise TerminalizationSafetyError(f"candidate trade date is invalid: run_id={raw.get('run_id')}") from exc
        if trade_date >= request.cutoff:
            raise TerminalizationSafetyError(
                f"candidate is not before the exclusive cutoff: run_id={raw.get('run_id')} trade_date={trade_date}"
            )
        backend = str(raw.get("broker_backend") or "")
        if backend not in {"local_sim", "minqmt_sim"}:
            raise TerminalizationSafetyError(
                f"candidate backend is outside SIM scope: run_id={raw.get('run_id')} backend={backend}"
            )
        if TERMINALIZATION_CARRIER_KEY in payload:
            raise TerminalizationSafetyError(
                f"retryable run already has terminalization carrier: run_id={raw.get('run_id')}"
            )
        classification = classify_historical_failed_run(payload)
        counts[classification["terminal_status"]] += 1
        backend_counts[backend] = backend_counts.get(backend, 0) + 1
        retry_control = payload.get(RETRY_CONTROL_KEY)
        candidates.append(
            {
                "run_id": str(raw["run_id"]),
                "trade_date": trade_date.isoformat(),
                "strategy_id": str(raw["strategy_id"]),
                "broker_backend": backend,
                "package_id": package_id,
                "release_id": str(raw["release_id"]),
                "binding_id": str(raw["binding_id"]),
                "execution_plan_id": raw.get("execution_plan_id"),
                "previous_status": "FAILED_RETRYABLE",
                "terminal_status": classification["terminal_status"],
                "side_effect_state": classification["side_effect_state"],
                "reason_code": classification["reason_code"],
                "source_payload_sha256": canonical_sha256(payload),
                "source_updated_at": str(raw["updated_at"]),
                "retry_control_present": RETRY_CONTROL_KEY in payload,
                "retry_control_sha256": canonical_sha256(retry_control) if RETRY_CONTROL_KEY in payload else None,
            }
        )
    plan = {
        "schema_version": "aistock_stale_simulation_run_terminalization_plan_v1",
        "database_identity": dict(database_identity),
        "retained_package_ids": list(request.package_ids),
        "exclusive_cutoff": request.cutoff.isoformat(),
        "candidate_count": len(candidates),
        "terminal_status_counts": counts,
        "broker_backend_counts": dict(sorted(backend_counts.items())),
        "candidates": candidates,
        "mutation_scope": {
            "table": "paper_v2.simulation_daily_run",
            "orders_mutated": False,
            "trades_mutated": False,
            "plans_mutated": False,
            "runs_deleted": False,
            "payload_preserved_except": ["last_stage", RETRY_CONTROL_KEY, TERMINALIZATION_CARRIER_KEY],
        },
    }
    return {**plan, "plan_sha256": canonical_sha256(plan)}


def build_terminalization_plan(cur: Any, request: FailedRunTerminalizationRequest) -> dict[str, Any]:
    _require_packages(cur, request.package_ids)
    return build_terminalization_plan_from_rows(
        database_identity=_database_identity(cur),
        request=request,
        rows=_candidate_rows(cur, request, for_update=False),
    )


def terminalized_payload(
    *, candidate: Mapping[str, Any], source_payload: Mapping[str, Any], plan_sha256: str, applied_at: datetime
) -> dict[str, Any]:
    if canonical_sha256(source_payload) != candidate.get("source_payload_sha256"):
        raise TerminalizationSafetyError(f"source payload drift: run_id={candidate.get('run_id')}")
    payload = dict(source_payload)
    retry_control = payload.pop(RETRY_CONTROL_KEY, None)
    carrier = {
        "schema_version": "historical_failed_retryable_terminalization_v1",
        "run_id": candidate["run_id"],
        "trade_date": candidate["trade_date"],
        "broker_backend": candidate["broker_backend"],
        "package_id": candidate["package_id"],
        "binding_id": candidate["binding_id"],
        "execution_plan_id": candidate.get("execution_plan_id"),
        "previous_status": "FAILED_RETRYABLE",
        "terminal_status": candidate["terminal_status"],
        "side_effect_state": candidate["side_effect_state"],
        "reason_code": candidate["reason_code"],
        "source_payload_sha256": candidate["source_payload_sha256"],
        "retry_control_removed": retry_control is not None,
        "retry_control_sha256": canonical_sha256(retry_control) if retry_control is not None else None,
        "plan_sha256": plan_sha256,
        "orders_mutated": False,
        "trades_mutated": False,
        "broker_replayed": False,
        "execution_replayed": False,
        "historical_evidence_deleted": False,
        "terminalized_at": applied_at.isoformat(),
    }
    payload["last_stage"] = candidate["terminal_status"]
    payload[TERMINALIZATION_CARRIER_KEY] = carrier
    return payload


def apply_terminalization_plan(
    cur: Any,
    request: FailedRunTerminalizationRequest,
    expected_plan_sha256: str,
    *,
    applied_at: datetime,
) -> tuple[dict[str, Any], dict[str, int], list[dict[str, Any]]]:
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
    _require_packages(cur, request.package_ids)
    identity = _database_identity(cur)
    locked_rows = _candidate_rows(cur, request, for_update=True)
    plan = build_terminalization_plan_from_rows(
        database_identity=identity,
        request=request,
        rows=locked_rows,
    )
    if plan["plan_sha256"] != expected_plan_sha256:
        raise TerminalizationSafetyError(
            f"terminalization plan digest mismatch: expected={expected_plan_sha256} observed={plan['plan_sha256']}"
        )

    rows_by_id = {str(row["run_id"]): row for row in locked_rows}
    updated_counts = {"CANCELLED": 0, "FAILED_TERMINAL": 0}
    for candidate in plan["candidates"]:
        source_row = rows_by_id[candidate["run_id"]]
        payload = terminalized_payload(
            candidate=candidate,
            source_payload=source_row["run_payload_json"],
            plan_sha256=plan["plan_sha256"],
            applied_at=applied_at,
        )
        cur.execute(
            """
            UPDATE paper_v2.simulation_daily_run
            SET status=%s,run_payload_json=%s,updated_at=%s
            WHERE run_id=%s AND status='FAILED_RETRYABLE' AND updated_at=%s
            """,
            (
                candidate["terminal_status"],
                psycopg2.extras.Json(payload),
                applied_at,
                candidate["run_id"],
                source_row["updated_at"],
            ),
        )
        if cur.rowcount != 1:
            raise TerminalizationSafetyError(f"terminalization CAS failed: run_id={candidate['run_id']}")
        updated_counts[candidate["terminal_status"]] += 1

    remaining = _candidate_rows(cur, request, for_update=False)
    if remaining:
        raise TerminalizationSafetyError(
            f"terminalization readback still has retryable candidates: {[row['run_id'] for row in remaining[:10]]}"
        )
    readback: list[dict[str, Any]] = []
    for candidate in plan["candidates"]:
        cur.execute(
            "SELECT status,run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id=%s",
            (candidate["run_id"],),
        )
        row = cur.fetchone()
        if row is None or row[0] != candidate["terminal_status"] or not isinstance(row[1], Mapping):
            raise TerminalizationSafetyError(f"terminalization status readback failed: run_id={candidate['run_id']}")
        carrier = row[1].get(TERMINALIZATION_CARRIER_KEY)
        if not isinstance(carrier, Mapping) or carrier.get("plan_sha256") != plan["plan_sha256"]:
            raise TerminalizationSafetyError(f"terminalization carrier readback failed: run_id={candidate['run_id']}")
        if RETRY_CONTROL_KEY in row[1]:
            raise TerminalizationSafetyError(
                f"retry control remained after terminalization: run_id={candidate['run_id']}"
            )
        readback.append(
            {
                "run_id": candidate["run_id"],
                "status": row[0],
                "carrier_plan_sha256": carrier["plan_sha256"],
                "source_payload_sha256": carrier["source_payload_sha256"],
            }
        )
    if updated_counts != plan["terminal_status_counts"]:
        raise TerminalizationSafetyError(
            f"terminalization count mismatch: expected={plan['terminal_status_counts']} actual={updated_counts}"
        )
    return plan, updated_counts, readback
