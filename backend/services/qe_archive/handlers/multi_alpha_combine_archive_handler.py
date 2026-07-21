"""Archive handler for multi-alpha combine-backtest completion events."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..models import ArchiveJobRecord, ClaimedOutboxEvent
from ..models import sha256_json
from ..repository import QEArchiveRepository
from ..multi_alpha_provenance import MultiAlphaProvenanceResolver
from .contract import ArchiveHandler, ArchiveResult, HandlerStatus, PayloadValidationError


MULTI_ALPHA_COMBINE_EVENT_TYPE = "qe.multi_alpha.combine.completed"
MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V1 = "multi_alpha_combine_completed_v1"
MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2 = "multi_alpha_combine_completed_v2"
# Backward-compatible import name for existing v1 handlers/callers. New P0-2
# payloads are selected explicitly by the source snapshot, never by fallback.
MULTI_ALPHA_COMBINE_SCHEMA_VERSION = MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V1
TERMINAL_MACB_STATUSES = {"succeeded", "partial_failed", "partial_recovered", "failed", "cancelled"}


@dataclass(frozen=True)
class MultiAlphaArchiveBundle:
    archive_run: dict[str, Any]
    run_header: dict[str, Any]
    run_source: dict[str, Any]
    legs: list[dict[str, Any]]
    leg_sources: list[dict[str, Any]]
    schemes: list[dict[str, Any]]
    loo: list[dict[str, Any]]
    recovery_children: list[dict[str, Any]]
    recovery_attempts: list[dict[str, Any]]
    stats: dict[str, Any]


class MultiAlphaCombineArchiveHandler(ArchiveHandler):
    """Materialize macb business rows into qe_archive as a sidecar."""

    event_type = MULTI_ALPHA_COMBINE_EVENT_TYPE
    supported_schema_versions = (
        MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V1,
        MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2,
    )

    def __init__(
        self,
        *,
        repository: QEArchiveRepository | None = None,
        provenance_resolver: MultiAlphaProvenanceResolver | None = None,
        clock: Any | None = None,
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._resolver = provenance_resolver or MultiAlphaProvenanceResolver(self._repository)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def validate_payload(self, payload: Mapping[str, Any]) -> None:
        super().validate_payload(payload)
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            raise PayloadValidationError("multi-alpha combine archive payload missing run_id")

    def handle(self, event: ClaimedOutboxEvent, archive_job: ArchiveJobRecord) -> ArchiveResult:
        payload = dict(event.payload or {})
        self.validate_payload(payload)
        run_id = str(payload.get("run_id") or event.source_id or "").strip()
        report = self.archive_run(run_id)
        if report.get("skipped_reason"):
            return ArchiveResult(
                status=HandlerStatus.NOOP,
                rows_inserted=0,
                rows_upserted=0,
                stats=report,
            )
        return ArchiveResult(
            status=HandlerStatus.SUCCESS,
            rows_inserted=0,
            rows_upserted=int(report.get("rows_upserted") or 0),
            stats=report,
        )

    def archive_run(self, run_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        run_id = str(run_id or "").strip()
        if not run_id:
            return {"run_id": run_id, "dry_run": dry_run, "skipped_reason": "multi_alpha_run_id_missing"}
        source = self._repository.fetch_multi_alpha_combine_run(run_id)
        if not source:
            return {"run_id": run_id, "dry_run": dry_run, "skipped_reason": "multi_alpha_source_run_missing"}
        bundle = self.build_bundle(source)
        stats = dict(bundle.stats)
        stats["dry_run"] = dry_run
        if dry_run:
            stats["written"] = False
            stats["rows_upserted"] = 0
            return stats
        write_stats = self._repository.archive_multi_alpha_bundle(
            run_header=bundle.run_header,
            archive_run=bundle.archive_run,
            run_source=bundle.run_source,
            legs=bundle.legs,
            leg_sources=bundle.leg_sources,
            schemes=bundle.schemes,
            loo=bundle.loo,
            recovery_children=bundle.recovery_children,
            recovery_attempts=bundle.recovery_attempts,
        )
        stats.update(write_stats)
        stats["written"] = True
        stats["rows_upserted"] = sum(
            int(write_stats.get(key) or 0)
            for key in (
                "run_rows",
                "leg_rows",
                "leg_source_rows",
                "scheme_rows",
                "loo_rows",
                "recovery_child_rows",
                "recovery_attempt_rows",
            )
        )
        return stats

    def build_bundle(self, source: Mapping[str, Any]) -> MultiAlphaArchiveBundle:
        run = dict(source.get("run") or {})
        if not run:
            raise ValueError("multi-alpha archive source bundle missing run row")
        run_id = str(run.get("id") or "").strip()
        if not run_id:
            raise ValueError("multi-alpha archive source run is missing id")
        status, logical_status = _terminal_status(run)
        if status not in TERMINAL_MACB_STATUSES:
            raise ValueError(f"multi-alpha combine run is not terminal: run_id={run_id!r} status={run.get('status')!r}")

        roster = _ensure_list(run.get("roster_json"))
        archived_at = self._clock()
        scheme_rows = _build_scheme_rows(source.get("scheme_results") or (), run_id=run_id, status=status)
        leg_rows: list[dict[str, Any]] = []
        source_rows: list[dict[str, Any]] = []
        resolved_count = 0
        for leg_order, raw_leg in enumerate(roster):
            leg, sources = self._build_leg_rows(run_id=run_id, leg_order=leg_order, raw_leg=raw_leg)
            leg_rows.append(leg)
            source_rows.extend(sources)
            resolved_count += sum(1 for item in sources if item.get("resolved"))
        total_sources = len(source_rows)
        complete_leg_count = sum(1 for item in leg_rows if item.get("provenance_complete"))
        reason = _ensure_mapping(run.get("reason"))
        source_children = [_ensure_mapping(item) for item in _ensure_list(source.get("children"))]
        source_attempts = [_ensure_mapping(item) for item in _ensure_list(source.get("attempts"))]
        execution_identity = _extract_execution_identity(
            run=run,
            children=source_children,
            attempts=source_attempts,
        )
        execution_identity_evidence = _extract_execution_identity_evidence(
            run=run,
            children=source_children,
            attempts=source_attempts,
            execution_identity=execution_identity,
        )
        archive_schema_version = (
            MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2
            if _requires_archive_v2(run=run, status=status, execution_identity=execution_identity)
            else MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V1
        )
        if archive_schema_version == MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2 and execution_identity_evidence is None:
            raise ValueError(
                "archive_recovery_snapshot_incomplete: v2 multi-alpha run has no execution identity evidence"
            )
        recovery_children, recovery_attempts = _build_recovery_snapshots(
            run_id=run_id,
            archived_at=archived_at,
            children=source_children,
            attempts=source_attempts,
            schema_version=archive_schema_version,
        )
        archive_run = {
            "run_id": run_id,
            "logical_experiment_id": run_id,
            "attempt_no": 1,
            "is_latest_attempt": True,
            "source_system": "multi_alpha",
            "run_type": "multi_alpha_combine",
            "status": status,
            "research_valid": status == "succeeded",
            "invalid_reason": None if status == "succeeded" else f"multi_alpha_{status}",
            "archived_at": archived_at,
            "source_created_at": run.get("created_at"),
            "source_updated_at": run.get("updated_at"),
        }
        run_header = {
            "run_id": run_id,
            "roster_hash": run.get("roster_hash"),
            "oos_start": run.get("oos_start"),
            "oos_end": run.get("oos_end"),
            "normalize_method": run.get("normalize_method"),
            "walk_forward_json": _ensure_mapping(run.get("walk_forward_json")),
            "baseline_leg_id": run.get("baseline_leg_id"),
            "leg_count": len(roster),
            "status": status,
            "logical_status": logical_status,
            "reason_json": reason,
            "archive_schema_version": "v2" if archive_schema_version == MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2 else "v1",
            "retry_of_run_id": run.get("retry_of_run_id"),
            "recovery_kind": run.get("recovery_kind"),
            "recovery_scope_json": (
                _ensure_mapping(run.get("recovery_scope_json"))
                if run.get("recovery_scope_json") not in (None, {})
                else None
            ),
            "recovery_scope_hash": run.get("recovery_scope_hash"),
            "execution_identity_json": execution_identity,
            "execution_identity_hash": sha256_json(execution_identity) if execution_identity is not None else None,
            "execution_identity_evidence_json": execution_identity_evidence,
            "source_created_at": run.get("created_at"),
            "archived_at": archived_at,
        }
        run_source = {
            "run_id": run_id,
            "source_system": "multi_alpha",
            "source_type": "combine_backtest_run",
            "source_id": run_id,
            "source_status": status,
            "metadata": {
                "schema_version": archive_schema_version,
                "roster_hash": run.get("roster_hash"),
                "business_status": run.get("status"),
                "logical_status": logical_status,
                "scheme_count": len(scheme_rows),
                "loo_count": len(source.get("loo") or ()),
                "recovery_child_count": len(recovery_children),
                "recovery_attempt_count": len(recovery_attempts),
            },
        }
        loo_rows = [_build_loo_row(row, run_id=run_id) for row in _ensure_list(source.get("loo"))]
        stats = {
            "run_id": run_id,
            "status": status,
            "logical_status": logical_status,
            "roster_hash": run.get("roster_hash"),
            "leg_count": len(leg_rows),
            "scheme_count": len(scheme_rows),
            "loo_count": len(loo_rows),
            "leg_source_count": total_sources,
            "resolved_source_count": resolved_count,
            "unresolved_source_count": total_sources - resolved_count,
            "provenance_resolve_rate": resolved_count / total_sources if total_sources else None,
            "provenance_complete_leg_count": complete_leg_count,
            "provenance_complete": complete_leg_count == len(leg_rows) and bool(leg_rows),
            "archive_schema_version": archive_schema_version,
            "recovery_child_count": len(recovery_children),
            "recovery_attempt_count": len(recovery_attempts),
        }
        return MultiAlphaArchiveBundle(
            archive_run=archive_run,
            run_header=run_header,
            run_source=run_source,
            legs=leg_rows,
            leg_sources=source_rows,
            schemes=scheme_rows,
            loo=loo_rows,
            recovery_children=recovery_children,
            recovery_attempts=recovery_attempts,
            stats=stats,
        )

    def _build_leg_rows(self, *, run_id: str, leg_order: int, raw_leg: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        leg_payload = _ensure_mapping(raw_leg)
        leg_id = str(leg_payload.get("leg_id") or leg_payload.get("id") or f"leg_{leg_order + 1}").strip()
        metadata = _ensure_mapping(leg_payload.get("metadata"))
        seed_run_ids = [str(item).strip() for item in _ensure_list(leg_payload.get("seed_run_ids") or leg_payload.get("run_ids")) if str(item).strip()]
        provenances = [self._resolver.resolve_seed(seed_ref) for seed_ref in seed_run_ids]
        source_rows = [
            provenance.to_leg_source_row(run_id=run_id, leg_id=leg_id, source_seq=index)
            for index, provenance in enumerate(provenances, start=1)
        ]
        factor_names = _extract_factor_names(metadata)
        if not factor_names:
            factor_names = _common_sequence([item.source_factor_names for item in provenances])
        factor_set_hash = _first_text(
            metadata,
            "factor_set_hash",
            "factor_hash",
            default=_common_value([item.source_factor_set_hash for item in provenances]),
        )
        model_type = _first_text(
            metadata,
            "model_type",
            "model_id",
            default=_common_value([item.source_model_type for item in provenances]),
        )
        model_family = _first_text(
            metadata,
            "model_family",
            default=_common_value([item.source_model_family for item in provenances]),
        )
        freq = _first_text(
            metadata,
            "freq",
            "frequency",
            "backtest_freq",
            default=_common_value([item.source_freq for item in provenances]),
        )
        label_horizon = _first_int(
            metadata,
            "label_horizon",
            default=_common_value([item.source_label_horizon for item in provenances]),
        )
        missing_items = []
        if not factor_names:
            missing_items.append("factor_names")
        if not factor_set_hash:
            missing_items.append("factor_set_hash")
        if not model_type:
            missing_items.append("model_type")
        unresolved = [item.to_meta() for item in provenances if not item.resolved]
        provenance_complete = not unresolved and not missing_items and bool(seed_run_ids)
        source_run_meta = {
            "schema_version": MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V1,
            "leg_metadata": metadata,
            "sources": [item.to_meta() for item in provenances],
            "missing_materialization": missing_items,
            "unresolved_sources": unresolved,
        }
        leg = {
            "run_id": run_id,
            "leg_id": leg_id,
            "leg_order": leg_order,
            "seed_run_ids": seed_run_ids,
            "factor_set_hash": factor_set_hash,
            "factor_names": factor_names,
            "factor_count": len(factor_names),
            "model_type": model_type,
            "model_family": model_family,
            "freq": freq,
            "label_horizon": label_horizon,
            "seed_count": len(seed_run_ids),
            "source_run_meta": source_run_meta,
            "provenance_complete": provenance_complete,
        }
        return leg, source_rows


def _terminal_status(run: Mapping[str, Any]) -> tuple[str, str | None]:
    reason = _ensure_mapping(run.get("reason"))
    raw_status = str(run.get("status") or "").strip().lower()
    logical_status = str(reason.get("logical_status") or raw_status or "").strip().lower() or None
    if logical_status == "partial_failed":
        return "partial_failed", logical_status
    return raw_status, logical_status


def _requires_archive_v2(
    *,
    run: Mapping[str, Any],
    status: str,
    execution_identity: Mapping[str, Any] | None,
) -> bool:
    return bool(run.get("recovery_kind")) or status in {"cancelled", "partial_recovered"} or execution_identity is not None


def _extract_execution_identity(
    *,
    run: Mapping[str, Any],
    children: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    run_identity = _ensure_mapping(run.get("execution_identity_json"))
    if run_identity:
        candidates.append(run_identity)
    for child in children:
        identity = _ensure_mapping(_ensure_mapping(child.get("input_manifest_json")).get("execution_identity"))
        if identity:
            candidates.append(identity)
    for attempt in attempts:
        identity = _ensure_mapping(_ensure_mapping(attempt.get("artifact_manifest_json")).get("execution_identity"))
        if identity:
            candidates.append(identity)
    if not candidates:
        return None
    first = candidates[0]
    expected_hash = sha256_json(first)
    if any(sha256_json(candidate) != expected_hash for candidate in candidates[1:]):
        raise ValueError("archive_recovery_snapshot_incomplete: execution identity conflicts across source manifests")
    persisted_hash = run.get("execution_identity_hash")
    if persisted_hash is not None and str(persisted_hash) != expected_hash:
        raise ValueError("archive_recovery_snapshot_incomplete: run execution identity hash mismatch")
    return first


def _extract_execution_identity_evidence(
    *,
    run: Mapping[str, Any],
    children: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
    execution_identity: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    run_evidence = _ensure_mapping(run.get("execution_identity_evidence_json"))
    if run_evidence:
        candidates.append(run_evidence)
    for child in children:
        evidence = _ensure_mapping(
            _ensure_mapping(child.get("input_manifest_json")).get("execution_identity_evidence")
        )
        if evidence:
            candidates.append(evidence)
    for attempt in attempts:
        evidence = _ensure_mapping(
            _ensure_mapping(attempt.get("artifact_manifest_json")).get("execution_identity_evidence")
        )
        if evidence:
            candidates.append(evidence)
    if not candidates:
        return {
            "schema_version": "multi_alpha_execution_identity_evidence_v1",
            "complete": execution_identity is not None,
            "reason_code": None if execution_identity is not None else "legacy_execution_identity_incomplete",
            "missing": [] if execution_identity is not None else ["execution_identity_payload"],
            "acquisition_suggestions": (
                []
                if execution_identity is not None
                else ["locate immutable dataset/runtime/code manifests for the historical run"]
            ),
        }
    first = candidates[0]
    complete = first.get("complete")
    if not isinstance(complete, bool):
        raise ValueError("archive_recovery_snapshot_incomplete: execution identity evidence has no boolean complete")
    if any(sha256_json(candidate) != sha256_json(first) for candidate in candidates[1:]):
        raise ValueError("archive_recovery_snapshot_incomplete: execution identity evidence conflicts across source manifests")
    if complete != (execution_identity is not None):
        raise ValueError("archive_recovery_snapshot_incomplete: execution identity evidence completeness conflicts with identity")
    return first


def _build_recovery_snapshots(
    *,
    run_id: str,
    archived_at: datetime,
    children: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
    schema_version: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if schema_version == MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V1:
        return [], []
    attempts_by_child: dict[str, list[dict[str, Any]]] = {}
    for raw_attempt in attempts:
        attempt = _ensure_mapping(raw_attempt)
        child_id = str(attempt.get("child_id") or "").strip()
        if not child_id:
            raise ValueError("archive_recovery_snapshot_incomplete: attempt has no child_id")
        attempts_by_child.setdefault(child_id, []).append(attempt)

    child_rows: list[dict[str, Any]] = []
    attempt_rows: list[dict[str, Any]] = []
    for raw_child in children:
        child = _ensure_mapping(raw_child)
        child_id = str(child.get("child_id") or "").strip()
        input_manifest = _ensure_mapping(child.get("input_manifest_json"))
        input_manifest_hash = str(child.get("input_manifest_hash") or "").strip()
        if not child_id or not input_manifest_hash:
            raise ValueError("archive_recovery_snapshot_incomplete: child identity or input manifest hash is missing")
        status = str(child.get("status") or "").strip()
        disposition = str(child.get("execution_disposition") or "execute").strip()
        selected_attempt_id = child.get("selected_attempt_id")
        if status == "not_recovered" and selected_attempt_id is not None:
            raise ValueError("archive_recovery_snapshot_incomplete: not_recovered child has selected attempt")
        lineage = child.get("source_lineage_json")
        lineage_json = _ensure_mapping(lineage) if lineage is not None else None
        lineage_hash = child.get("source_lineage_hash")
        if lineage_json is None:
            if lineage_hash is not None:
                raise ValueError("archive_recovery_snapshot_incomplete: source lineage hash has no lineage payload")
        else:
            computed_lineage_hash = sha256_json(lineage_json)
            if lineage_hash is not None and str(lineage_hash) != computed_lineage_hash:
                raise ValueError("archive_recovery_snapshot_incomplete: source lineage hash mismatch")
            lineage_hash = computed_lineage_hash
        child_rows.append(
            {
                "run_id": run_id,
                "child_id": child_id,
                "child_key": child.get("child_key"),
                "child_kind": child.get("child_kind"),
                "status": status,
                "execution_disposition": disposition,
                "selected_attempt_id": selected_attempt_id,
                "source_child_id": child.get("source_child_id"),
                "source_lineage_json": lineage_json,
                "source_lineage_hash": lineage_hash,
                "input_manifest_json": input_manifest,
                "input_manifest_hash": input_manifest_hash,
                "prediction_artifact_uri": child.get("prediction_artifact_uri"),
                "prediction_artifact_hash": child.get("prediction_artifact_hash"),
                "archived_at": archived_at,
            }
        )
        for attempt in attempts_by_child.get(child_id, []):
            result_manifest = _ensure_mapping(attempt.get("result_manifest_json"))
            result_hash = attempt.get("result_manifest_hash")
            if result_manifest:
                computed_result_hash = sha256_json(result_manifest)
                if result_hash is not None and str(result_hash) != computed_result_hash:
                    raise ValueError("archive_recovery_snapshot_incomplete: result manifest hash mismatch")
                result_hash = computed_result_hash
            execution_kind = str(attempt.get("execution_kind") or "remote_execution")
            if execution_kind != "remote_execution" and not result_hash:
                raise ValueError("archive_recovery_snapshot_incomplete: reference/derived attempt lacks result hash")
            attempt_rows.append(
                {
                    "run_id": run_id,
                    "child_id": child_id,
                    "attempt_id": attempt.get("attempt_id"),
                    "attempt_no": attempt.get("attempt_no"),
                    "retry_mode": attempt.get("retry_mode"),
                    "execution_kind": execution_kind,
                    "status": attempt.get("status"),
                    "source_attempt_id": attempt.get("source_attempt_id"),
                    "artifact_manifest_json": _ensure_mapping(attempt.get("artifact_manifest_json")),
                    "result_manifest_json": result_manifest,
                    "result_manifest_hash": result_hash,
                    "archived_at": archived_at,
                }
            )
    return child_rows, attempt_rows


def _build_scheme_rows(rows: Sequence[Any], *, run_id: str, status: str) -> list[dict[str, Any]]:
    source_rows = [_ensure_mapping(row) for row in rows]
    eligible = [row for row in source_rows if not bool(row.get("skipped"))]
    best_scheme = None
    if status == "succeeded" and eligible:
        best_row = max(eligible, key=lambda row: (_float_sort(row.get("sharpe")), _float_sort(row.get("calmar"))))
        best_scheme = str(best_row.get("weighting_scheme"))
    return [
        {
            "run_id": run_id,
            "weighting_scheme": row.get("weighting_scheme"),
            "scheme_algorithm": _scheme_algorithm(row.get("weighting_scheme")),
            "weights_json": _ensure_mapping(row.get("weights_json")),
            "per_window_weights_json": _ensure_list(row.get("per_window_weights_json")),
            "cagr": row.get("cagr"),
            "max_drawdown": row.get("max_drawdown"),
            "sharpe": row.get("sharpe"),
            "calmar": row.get("calmar"),
            "topk_return_20": row.get("topk_return_20"),
            "topk_hit_rate_20": row.get("topk_hit_rate_20"),
            "turnover": row.get("turnover"),
            "vs_baseline_sharpe_delta": row.get("vs_baseline_sharpe_delta"),
            "vs_baseline_calmar_delta": row.get("vs_baseline_calmar_delta"),
            "pred_persisted": bool(row.get("pred_persisted")),
            "skipped": bool(row.get("skipped")),
            "skipped_reason": row.get("skipped_reason"),
            "is_best": str(row.get("weighting_scheme")) == best_scheme,
        }
        for row in source_rows
    ]


def _build_loo_row(row: Any, *, run_id: str) -> dict[str, Any]:
    payload = _ensure_mapping(row)
    return {
        "run_id": run_id,
        "weighting_scheme": payload.get("weighting_scheme"),
        "dropped_leg_id": payload.get("dropped_leg_id"),
        "marginal_cagr": payload.get("marginal_cagr"),
        "marginal_sharpe": payload.get("marginal_sharpe"),
        "marginal_calmar": payload.get("marginal_calmar"),
    }


def _scheme_algorithm(value: Any) -> str:
    scheme = str(value or "").strip().lower()
    if scheme.startswith("rank_fusion"):
        return "rank_fusion"
    return scheme or "unknown"


def _extract_factor_names(metadata: Mapping[str, Any]) -> list[str]:
    raw = None
    for key in ("factor_names", "factor_list", "factors"):
        if key in metadata:
            raw = metadata.get(key)
            break
    names: list[str] = []
    for item in _ensure_list(raw):
        if isinstance(item, Mapping):
            value = item.get("factor_name") or item.get("name") or item.get("key")
        else:
            value = item
        text = str(value or "").strip()
        if text:
            names.append(text)
    return names


def _ensure_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return []


def _first_text(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> str | None:
    for key in keys:
        value = mapping.get(key)
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    text = str(default).strip() if default is not None else ""
    return text or None


def _first_int(mapping: Mapping[str, Any], *keys: str, default: Any = None) -> int | None:
    for key in keys:
        parsed = _int_or_none(mapping.get(key))
        if parsed is not None:
            return parsed
    return _int_or_none(default)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _common_value(values: Sequence[Any]) -> Any | None:
    cleaned = [value for value in values if value not in (None, "")]
    if not cleaned:
        return None
    first = cleaned[0]
    return first if all(value == first for value in cleaned) else None


def _common_sequence(values: Sequence[Sequence[Any] | None]) -> list[str]:
    cleaned = [[str(item).strip() for item in value if str(item).strip()] for value in values if value]
    if not cleaned:
        return []
    first = cleaned[0]
    return first if all(value == first for value in cleaned) else []


def _float_sort(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float("-inf")
    return parsed
