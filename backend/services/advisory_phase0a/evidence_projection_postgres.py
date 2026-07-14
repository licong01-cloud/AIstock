"""Fixed-SQL PostgreSQL projection for Advisory evidence consumers only.

The projection is intentionally read-only and owns no business repository.  It
maps persisted rows into Advisory DTOs so Phase 0A/Phase 1E cannot import or
invoke Selection, Simulation, StrategyPackage, Paper, QE, or inference code.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Callable, Iterator
from uuid import UUID

import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256, canonicalize
from backend.services.advisory_phase0a.historical_research import (
    HistoricalResearchBatch,
    HistoricalResearchBatchReceipt,
    HistoricalResearchCandidate,
    HistoricalResearchProgramRun,
    HistoricalResearchRunStatus,
)
from backend.services.advisory_phase0a.models import SourceAvailability, SourceAvailabilityStatus
from backend.services.advisory_phase1.source_ledger import (
    SourceAvailabilityEvent,
    SourceAvailabilityEventInput,
    SourceAvailabilityEventType,
)

from .evidence_projection import (
    AdvisoryEvidenceProjectionError,
    ProjectedAdvisoryProgram,
    ProjectedAlphaComponent,
    ProjectedAlphaMode,
    ProjectedBindingVersion,
    ProjectedCombinationPolicy,
    ProjectedDailySelectionEvidence,
    ProjectedManifestAsset,
    ProjectedPackage,
    ProjectedPackageAsset,
    ProjectedPackageAssetType,
    ProjectedPackageManifest,
    ProjectedSelectionCandidate,
    ProjectedSelectionExclusion,
    ProjectedSelectionRun,
    ProjectedSelectionScoreArtifact,
)


ConnFactory = Callable[[], Iterator[Any]]


class AdvisoryProjectionReadOnlyError(AdvisoryEvidenceProjectionError):
    """The Advisory projection cannot safely consume a PostgreSQL snapshot."""


_SQL: dict[str, str] = {
    "transaction_state": """
        SELECT current_setting('transaction_read_only') AS transaction_read_only,
               current_setting('transaction_isolation') AS transaction_isolation
    """,
    "postgres_now": "SELECT clock_timestamp() AS postgres_now",
    "program": """
        SELECT program_id, target_count, review_policy
        FROM app.advisory_program
        WHERE program_id = %s
    """,
    "bindings": """
        SELECT binding_version_id, program_id, package_mode, package_ids,
               runtime_config_json, effective_from_trade_date, effective_to_trade_date,
               activation_status, binding_payload_json
        FROM app.advisory_strategy_binding_version
        WHERE program_id = %s
        ORDER BY created_at DESC, binding_version_id DESC
    """,
    "package": """
        SELECT package_id, source_id, manifest_json, manifest_sha256, alpha_mode,
               data_vintage
        FROM strategy_pkg.package
        WHERE package_id = %s
    """,
    "package_assets_all": """
        SELECT asset_id, package_id, asset_type, asset_ref, asset_sha256, metadata,
               asset_role, created_at
        FROM strategy_pkg.package_asset
        WHERE package_id = %s
        ORDER BY created_at DESC, asset_id DESC
    """,
    "package_assets_protected": """
        SELECT asset_id, package_id, asset_type, asset_ref, asset_sha256, metadata,
               asset_role, created_at
        FROM strategy_pkg.package_asset
        WHERE package_id = %s AND protected_asset = TRUE
        ORDER BY created_at DESC, asset_id DESC
    """,
    "daily_selection_evidence": """
        SELECT evidence_id, target_trade_date, cutoff_date, package_id, manifest_sha256,
               runtime_profile_version_id, runtime_profile_hash, source_type, data_source,
               candidate_count, artifact_hash, evidence_payload_json, created_at
        FROM selection.daily_selection_evidence
        WHERE evidence_id = %s
    """,
    "selection_run": """
        SELECT run_id, mode, trade_date, data_source, package_ids, runtime_config, status,
               valid_no_candidate, no_candidate_reason, error_json, created_at, completed_at
        FROM selection.run
        WHERE run_id = %s
    """,
    "selection_run_package_results": """
        SELECT package_id, manifest_sha256, symbol, score, rank, target_weight,
               target_quantity, reference_price, component_scores, reason,
               suggested_entry_price_band, suggested_stop_loss_zone, guidance_status,
               price_guard_policy_sha256
        FROM selection.package_result
        WHERE run_id = %s
        ORDER BY package_id ASC, rank ASC, symbol ASC
    """,
    "selection_run_aggregate_results": """
        SELECT symbol, score, rank, target_weight, target_quantity, reference_price,
               source_package_ids, explanation
        FROM selection.aggregate_result
        WHERE run_id = %s
        ORDER BY rank ASC, symbol ASC
    """,
    "selection_run_excluded_results": """
        SELECT package_id, manifest_sha256, symbol, score, raw_rank,
               reason, source, context
        FROM selection.excluded_result
        WHERE run_id = %s
        ORDER BY package_id ASC, raw_rank ASC, symbol ASC, reason ASC
    """,
    "selection_run_manifest_lineage": """
        SELECT package_id, manifest_sha256
        FROM selection.package_result
        WHERE run_id = %s
        UNION
        SELECT package_id, manifest_sha256
        FROM selection.excluded_result
        WHERE run_id = %s
        ORDER BY package_id ASC, manifest_sha256 ASC
    """,
    "selection_artifacts_manifest": """
        SELECT artifact_id, package_id, manifest_sha256, trade_date, data_source,
               runtime_config_hash, scores_json, artifact_sha256, score_count,
               universe_count, top_score_symbol, status, metadata,
               artifact_contract_version, artifact_payload_sha256,
               artifact_input_context_hash, source_revision_set_hash,
               asset_closure_hash, created_at
        FROM strategy_pkg.selection_score_artifact
        WHERE package_id = %s AND manifest_sha256 = %s
        ORDER BY trade_date DESC, created_at DESC
        LIMIT %s
    """,
    "selection_artifacts_package": """
        SELECT artifact_id, package_id, manifest_sha256, trade_date, data_source,
               runtime_config_hash, scores_json, artifact_sha256, score_count,
               universe_count, top_score_symbol, status, metadata,
               artifact_contract_version, artifact_payload_sha256,
               artifact_input_context_hash, source_revision_set_hash,
               asset_closure_hash, created_at
        FROM strategy_pkg.selection_score_artifact
        WHERE package_id = %s
        ORDER BY trade_date DESC, created_at DESC
        LIMIT %s
    """,
    "selection_artifact_by_id": """
        SELECT artifact_id, package_id, manifest_sha256, trade_date, data_source,
               runtime_config_hash, scores_json, artifact_sha256, score_count,
               universe_count, top_score_symbol, status, metadata,
               artifact_contract_version, artifact_payload_sha256,
               artifact_input_context_hash, source_revision_set_hash,
               asset_closure_hash, created_at
        FROM strategy_pkg.selection_score_artifact
        WHERE artifact_id = %s
    """,
    "source_events": """
        SELECT availability_event_id, append_request_hash, dataset_name, source_role,
               partition_key, partition_key_hash, partition_chain_key, revision_id,
               event_revision_no, event_type, predecessor_event_hash, provider_job_id,
               refresh_job_id, provider_published_at, first_observed_at,
               formal_available_at, schema_fingerprint, row_count,
               partition_content_hash, quality_status, reason_codes,
               event_content_hash, created_by_service_principal
        FROM app.advisory_source_availability_event
        WHERE dataset_name = %s AND source_role = %s AND partition_key_hash = %s
        ORDER BY event_revision_no ASC
    """,
    "historical_receipt_by_receipt_id": """
        SELECT receipt_id, batch_id, batch_key, status, receipt_hash, program_run_ids,
               receipt_payload_json, created_at
        FROM app.advisory_research_batch_receipt
        WHERE receipt_id = %s
    """,
    "historical_receipt_by_batch_id": """
        SELECT receipt_id, batch_id, batch_key, status, receipt_hash, program_run_ids,
               receipt_payload_json, created_at
        FROM app.advisory_research_batch_receipt
        WHERE batch_id = %s
    """,
    "historical_batch": """
        SELECT batch_id, request_id, batch_key, decision_trade_date, program_ids,
               data_source, origin, request_payload_sha256, research_scope,
               execution_prohibited, status, created_at, updated_at
        FROM app.advisory_research_batch
        WHERE batch_id = %s
    """,
    "historical_runs": """
        SELECT program_run_id, program_id, decision_trade_date, research_scope, status,
               program_payload_sha256, binding_version_id, binding_payload_hash,
               package_id, manifest_sha256, policy_hash,
               effective_runtime_config_hash, source_watermark_hash, evidence_id,
               evidence_hash, artifact_id, artifact_payload_hash,
               research_list_version_id, research_candidates_json, candidate_outcome,
               reason_codes, error_json, created_at, updated_at
        FROM app.advisory_research_program_run
        WHERE program_run_id = ANY(%s)
    """,
    "calendar_days": """
        SELECT cal_date
        FROM market.trading_calendar
        WHERE is_trading = TRUE AND cal_date >= %s AND cal_date <= %s
        ORDER BY cal_date ASC
    """,
    "source_probe_market_kline_daily_raw": """
        SELECT MAX(trade_date) AS watermark_date
        FROM market.kline_daily_raw
        WHERE trade_date <= %s
    """,
    "source_probe_market_daily_basic": """
        SELECT MAX(trade_date) AS watermark_date
        FROM market.daily_basic
        WHERE trade_date <= %s
    """,
    "source_probe_market_moneyflow_ts": """
        SELECT MAX(trade_date) AS watermark_date
        FROM market.moneyflow_ts
        WHERE trade_date <= %s
    """,
    "source_probe_market_sector_data": """
        SELECT MAX(trade_date) AS watermark_date
        FROM market.sector_data
        WHERE trade_date <= %s
    """,
    "source_probe_market_trading_calendar": """
        SELECT MAX(cal_date) AS watermark_date
        FROM market.trading_calendar
        WHERE is_trading = TRUE AND cal_date <= %s
    """,
}


def _assert_fixed_select_registry() -> None:
    for query_id, sql in _SQL.items():
        normalized = " ".join(sql.strip().upper().split())
        if not normalized.startswith("SELECT ") or " FOR UPDATE" in normalized or " FOR SHARE" in normalized:
            raise AdvisoryProjectionReadOnlyError(f"projection query registry is not read-only: {query_id}")


_assert_fixed_select_registry()


def _mapping(row: Any) -> dict[str, Any]:
    if row is None:
        raise AdvisoryProjectionReadOnlyError("required immutable evidence row is missing")
    return dict(row)


def _object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _text(value: Any, *, field_name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise AdvisoryProjectionReadOnlyError(f"persisted {field_name} is missing")
    return result


def _alpha_mode(value: Any) -> ProjectedAlphaMode:
    try:
        return ProjectedAlphaMode(str(getattr(value, "value", value)))
    except ValueError as exc:
        raise AdvisoryProjectionReadOnlyError(f"unsupported persisted alpha mode: {value!r}") from exc


def _asset_type(value: Any) -> ProjectedPackageAssetType:
    try:
        return ProjectedPackageAssetType(str(getattr(value, "value", value)))
    except ValueError:
        return ProjectedPackageAssetType.OTHER


def _strict_object_or_empty(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise AdvisoryProjectionReadOnlyError(f"{field_name} must be a JSON object")
    return dict(value)


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _display_fields(component_scores: dict[str, Any]) -> dict[str, Any]:
    display = component_scores.get("selection_result_display")
    return dict(display) if isinstance(display, dict) else {}


def _guidance_fields(component_scores: dict[str, Any]) -> dict[str, Any]:
    guidance = component_scores.get("selection_price_guidance")
    if not isinstance(guidance, dict):
        return {}
    return {
        "signal_ref_price": guidance.get("signal_ref_price"),
        "suggested_entry_price_band": guidance.get("entry_band"),
        "suggested_stop_loss_zone": guidance.get("stop_loss_zone"),
        "guidance_status": guidance.get("guidance_status"),
        "price_guard_policy_sha256": guidance.get("price_guard_policy_sha256"),
    }


def _selection_candidate_from_row(row: dict[str, Any], *, aggregate: bool) -> ProjectedSelectionCandidate:
    component_key = "explanation" if aggregate else "component_scores"
    component_scores = _strict_object_or_empty(row.get(component_key), field_name=component_key)
    display = _display_fields(component_scores)
    guidance = _guidance_fields(component_scores)
    entry_price = display.get("selection_entry_price")
    reference_price = entry_price if entry_price is not None else row.get("reference_price")
    return ProjectedSelectionCandidate(
        symbol=_text(row.get("symbol"), field_name="selection candidate symbol"),
        score=float(row["score"]),
        rank=int(row["rank"]),
        target_weight=_optional_float(row.get("target_weight")),
        target_quantity=int(row["target_quantity"]) if row.get("target_quantity") is not None else None,
        reference_price=_optional_float(reference_price),
        stock_name=display.get("stock_name"),
        selection_entry_price=_optional_float(entry_price),
        selection_entry_price_source=display.get("selection_entry_price_source"),
        selection_entry_price_time=display.get("selection_entry_price_time"),
        signal_ref_price=_optional_float(guidance.get("signal_ref_price")),
        previous_close=_optional_float(display.get("previous_close")),
        volume=_optional_float(display.get("volume")),
        current_price=_optional_float(display.get("current_price")),
        current_price_source=display.get("current_price_source"),
        current_price_time=display.get("current_price_time"),
        suggested_entry_price_band=(
            guidance.get("suggested_entry_price_band")
            if aggregate
            else row.get("suggested_entry_price_band") or guidance.get("suggested_entry_price_band")
        ),
        suggested_stop_loss_zone=(
            guidance.get("suggested_stop_loss_zone")
            if aggregate
            else row.get("suggested_stop_loss_zone") or guidance.get("suggested_stop_loss_zone")
        ),
        guidance_status=(
            guidance.get("guidance_status")
            if aggregate
            else row.get("guidance_status") or guidance.get("guidance_status")
        ),
        price_guard_policy_sha256=(
            guidance.get("price_guard_policy_sha256")
            if aggregate
            else row.get("price_guard_policy_sha256") or guidance.get("price_guard_policy_sha256")
        ),
        component_scores=component_scores,
        reason=None if aggregate else row.get("reason"),
    )


def _declared_runtime_assets(manifest: dict[str, Any]) -> tuple[ProjectedManifestAsset, ...]:
    explicit = manifest.get("phase0a_asset_closure") or manifest.get("asset_closure")
    items: list[ProjectedManifestAsset] = []
    if isinstance(explicit, list):
        for raw in explicit:
            if not isinstance(raw, dict):
                continue
            asset_type = str(raw.get("asset_type") or raw.get("type") or "other").strip()
            asset_ref = str(raw.get("asset_ref") or raw.get("ref") or "").strip()
            asset_sha256 = str(raw.get("asset_sha256") or raw.get("sha256") or "").strip()
            if asset_ref and asset_sha256:
                items.append(ProjectedManifestAsset(asset_type=asset_type, asset_ref=asset_ref, asset_sha256=asset_sha256))
    if items:
        return tuple(sorted(items, key=lambda item: (item.asset_type, item.asset_ref, item.asset_sha256)))

    for raw in _list(manifest.get("factor_set")):
        if isinstance(raw, dict) and raw.get("asset_ref") and raw.get("sha256"):
            items.append(ProjectedManifestAsset("factor_code", str(raw["asset_ref"]), str(raw["sha256"])))
    model_asset = manifest.get("model_asset")
    for raw_model in _list(model_asset) if isinstance(model_asset, list) else [model_asset]:
        if not isinstance(raw_model, dict):
            continue
        if raw_model.get("asset_ref") and raw_model.get("sha256"):
            items.append(ProjectedManifestAsset("model_weight", str(raw_model["asset_ref"]), str(raw_model["sha256"])))
        for raw_code in _list(raw_model.get("model_code_assets")):
            if isinstance(raw_code, dict) and raw_code.get("asset_ref") and raw_code.get("sha256"):
                items.append(ProjectedManifestAsset("model_code", str(raw_code["asset_ref"]), str(raw_code["sha256"])))
    runtime_assets = _object(manifest.get("runtime_assets"))
    alpha158 = _object(runtime_assets.get("alpha158"))
    if alpha158.get("enabled") and alpha158.get("asset_ref") and alpha158.get("sha256"):
        items.append(ProjectedManifestAsset("factor_schema", str(alpha158["asset_ref"]), str(alpha158["sha256"])))
    return tuple(sorted(items, key=lambda item: (item.asset_type, item.asset_ref, item.asset_sha256)))


def _package_from_row(row: dict[str, Any]) -> ProjectedPackage:
    manifest_payload = _object(row.get("manifest_json"))
    package_id = _text(row.get("package_id"), field_name="package_id")
    manifest_sha256 = _text(row.get("manifest_sha256"), field_name="manifest_sha256")
    manifest_mode = _alpha_mode(manifest_payload.get("alpha_mode") or row.get("alpha_mode"))
    components: list[ProjectedAlphaComponent] = []
    for raw in _list(manifest_payload.get("alpha_components")):
        if not isinstance(raw, dict):
            continue
        alpha_id = str(raw.get("alpha_id") or "").strip()
        if not alpha_id:
            continue
        components.append(
            ProjectedAlphaComponent(
                alpha_id=alpha_id,
                factor_ids=[str(item) for item in _list(raw.get("factor_ids"))],
                model_id=str(raw.get("model_id") or "").strip() or None,
                model_ref=str(raw.get("model_ref") or "").strip() or None,
                component_weight=raw.get("component_weight"),
                score_direction=str(raw.get("score_direction") or "").strip() or None,
                score_normalization=str(raw.get("score_normalization") or "").strip() or None,
                holding_period=raw.get("holding_period"),
                rebalance_frequency=str(raw.get("rebalance_frequency") or "").strip() or None,
            )
        )
    combination_payload = _object(manifest_payload.get("alpha_combination_policy"))
    combination = None
    if combination_payload:
        method = str(combination_payload.get("method") or "").strip()
        if method:
            combination = ProjectedCombinationPolicy(method=method, payload=combination_payload)
    manifest = ProjectedPackageManifest(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=manifest_mode,
        style_family=(
            str(
                manifest_payload.get("style_family")
                or manifest_payload.get("strategy_style")
                or _object(manifest_payload.get("source_evidence")).get("style_family")
                or ""
            ).strip()
            or None
        ),
        source_evidence=_object(manifest_payload.get("source_evidence")),
        alpha_components=tuple(components),
        alpha_combination_policy=combination,
        backtest_context=_object(manifest_payload.get("backtest_context")),
        declared_runtime_assets=_declared_runtime_assets(manifest_payload),
    )
    source_evidence = manifest.source_evidence
    return ProjectedPackage(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=manifest_mode,
        source_id=_text(row.get("source_id"), field_name="package source_id"),
        manifest=manifest,
        data_vintage=row.get("data_vintage"),
        asset_closure_hash=str(source_evidence.get("asset_closure_hash") or "").strip() or None,
        lineage_hash=str(source_evidence.get("lineage_hash") or "").strip() or None,
    )


class AdvisoryPostgresEvidenceProjection:
    """Open one PostgreSQL `REPEATABLE READ READ ONLY` projection snapshot."""

    def __init__(self, conn_factory: ConnFactory) -> None:
        self._conn_factory = conn_factory

    @contextmanager
    def snapshot(self) -> Iterator["AdvisoryPostgresEvidenceSnapshot"]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cur.execute(_SQL["transaction_state"])
                row = _mapping(cur.fetchone())
                if str(row.get("transaction_read_only") or "").lower() not in {"on", "true", "1"}:
                    raise AdvisoryProjectionReadOnlyError("PostgreSQL did not confirm transaction_read_only=on")
                if str(row.get("transaction_isolation") or "").lower() != "repeatable read":
                    raise AdvisoryProjectionReadOnlyError("PostgreSQL did not confirm transaction_isolation=repeatable read")
                yield AdvisoryPostgresEvidenceSnapshot(cur)


class AdvisoryPostgresEvidenceSnapshot:
    """Reader facade backed exclusively by the fixed SELECT registry."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def _one(self, query_id: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        self._cursor.execute(_SQL[query_id], params)
        row = self._cursor.fetchone()
        return dict(row) if row is not None else None

    def _many(self, query_id: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        self._cursor.execute(_SQL[query_id], params)
        return [dict(row) for row in self._cursor.fetchall()]

    def postgres_now(self) -> datetime:
        row = self._one("postgres_now", ())
        if row is None or row.get("postgres_now") is None:
            raise AdvisoryProjectionReadOnlyError("PostgreSQL snapshot time is unavailable")
        return row["postgres_now"]

    def get_program(self, program_id: str) -> ProjectedAdvisoryProgram:
        row = self._one("program", (program_id,))
        if row is None:
            raise AdvisoryProjectionReadOnlyError(f"advisory program is missing: {program_id}")
        return ProjectedAdvisoryProgram(
            program_id=_text(row.get("program_id"), field_name="program_id"),
            target_count=int(row.get("target_count") or 0),
            review_policy=_object(row.get("review_policy")),
        )

    def list_binding_versions(self, program_id: str) -> list[ProjectedBindingVersion]:
        rows = self._many("bindings", (program_id,))
        bindings: list[ProjectedBindingVersion] = []
        for row in rows:
            payload = _object(row.get("binding_payload_json"))
            bindings.append(
                ProjectedBindingVersion(
                    binding_version_id=_text(row.get("binding_version_id"), field_name="binding_version_id"),
                    program_id=_text(row.get("program_id"), field_name="binding program_id"),
                    package_mode=_text(row.get("package_mode"), field_name="binding package_mode"),
                    package_ids=[str(item) for item in _list(row.get("package_ids"))],
                    effective_from_trade_date=row.get("effective_from_trade_date"),
                    effective_to_trade_date=row.get("effective_to_trade_date"),
                    activation_status=_text(row.get("activation_status"), field_name="binding activation_status"),
                    binding_payload_hash=(
                        canonical_json_sha256(canonicalize(payload)) if payload else None
                    ),
                    runtime_config_json=_object(row.get("runtime_config_json")),
                )
            )
        return bindings

    def get(self, package_id: str) -> ProjectedPackage:
        row = self._one("package", (package_id,))
        if row is None:
            raise AdvisoryProjectionReadOnlyError(f"strategy package is missing: {package_id}")
        return _package_from_row(row)

    def list_package_assets(self, package_id: str, *, protected_only: bool = False) -> list[ProjectedPackageAsset]:
        query = "package_assets_protected" if protected_only else "package_assets_all"
        return [
            ProjectedPackageAsset(
                package_id=_text(row.get("package_id"), field_name="asset package_id"),
                asset_type=_asset_type(row.get("asset_type")),
                asset_ref=_text(row.get("asset_ref"), field_name="asset_ref"),
                asset_sha256=str(row.get("asset_sha256") or "").strip() or None,
                metadata=_object(row.get("metadata")),
                asset_role=str(row.get("asset_role") or "governed_asset").strip() or "governed_asset",
                asset_id=int(row["asset_id"]) if row.get("asset_id") is not None else None,
                created_at=row.get("created_at"),
            )
            for row in self._many(query, (package_id,))
        ]

    def get_daily_selection_evidence(self, evidence_id: str) -> ProjectedDailySelectionEvidence:
        row = self._one("daily_selection_evidence", (evidence_id,))
        if row is None:
            raise AdvisoryProjectionReadOnlyError(f"daily selection evidence is missing: {evidence_id}")
        return ProjectedDailySelectionEvidence(
            evidence_id=_text(row.get("evidence_id"), field_name="evidence_id"),
            target_trade_date=row["target_trade_date"],
            cutoff_date=row.get("cutoff_date"),
            package_id=_text(row.get("package_id"), field_name="evidence package_id"),
            manifest_sha256=_text(row.get("manifest_sha256"), field_name="evidence manifest_sha256"),
            runtime_profile_version_id=_text(row.get("runtime_profile_version_id"), field_name="runtime_profile_version_id"),
            runtime_profile_hash=_text(row.get("runtime_profile_hash"), field_name="runtime_profile_hash"),
            source_type=_text(row.get("source_type"), field_name="source_type"),
            data_source=_text(row.get("data_source"), field_name="data_source"),
            candidate_count=int(row.get("candidate_count") or 0),
            artifact_hash=_text(row.get("artifact_hash"), field_name="evidence artifact_hash"),
            evidence_payload_json=_object(row.get("evidence_payload_json")),
            created_at=row.get("created_at"),
        )

    def get_run(self, run_id: str) -> ProjectedSelectionRun:
        row = self._one("selection_run", (run_id,))
        if row is None:
            raise AdvisoryProjectionReadOnlyError(f"selection run is missing: {run_id}")
        package_rows: dict[str, list[ProjectedSelectionCandidate]] = {}
        for package_row in self._many("selection_run_package_results", (run_id,)):
            package_id = _text(package_row.get("package_id"), field_name="selection result package_id")
            package_rows.setdefault(package_id, []).append(
                _selection_candidate_from_row(package_row, aggregate=False)
            )
        excluded_rows: dict[str, list[ProjectedSelectionExclusion]] = {}
        for exclusion_row in self._many("selection_run_excluded_results", (run_id,)):
            package_id = _text(exclusion_row.get("package_id"), field_name="selection exclusion package_id")
            excluded_rows.setdefault(package_id, []).append(
                ProjectedSelectionExclusion(
                    symbol=_text(exclusion_row.get("symbol"), field_name="selection exclusion symbol"),
                    score=float(exclusion_row["score"]),
                    rank=int(exclusion_row["raw_rank"]),
                    reason=_text(exclusion_row.get("reason"), field_name="selection exclusion reason"),
                    source=_text(exclusion_row.get("source"), field_name="selection exclusion source"),
                    context=_strict_object_or_empty(
                        exclusion_row.get("context"),
                        field_name="selection exclusion context",
                    ),
                )
            )
        manifest_sha256_by_package: dict[str, str] = {}
        for lineage_row in self._many("selection_run_manifest_lineage", (run_id, run_id)):
            package_id = _text(lineage_row.get("package_id"), field_name="selection lineage package_id")
            manifest_sha256 = _text(lineage_row.get("manifest_sha256"), field_name="selection lineage manifest_sha256")
            prior = manifest_sha256_by_package.get(package_id)
            if prior is not None and prior != manifest_sha256:
                raise AdvisoryProjectionReadOnlyError(
                    f"selection run has conflicting manifest lineage for package: {package_id}"
                )
            manifest_sha256_by_package[package_id] = manifest_sha256
        aggregate_rows = [
            _selection_candidate_from_row(aggregate_row, aggregate=True)
            for aggregate_row in self._many("selection_run_aggregate_results", (run_id,))
        ]
        stored_package_ids = [str(item) for item in _list(row.get("package_ids"))]
        package_ids = stored_package_ids or sorted(set(package_rows) | set(excluded_rows))
        return ProjectedSelectionRun(
            run_id=_text(row.get("run_id"), field_name="selection run_id"),
            mode=_text(row.get("mode"), field_name="selection run mode"),
            trade_date=row["trade_date"],
            status=_text(row.get("status"), field_name="selection run status"),
            package_ids=package_ids,
            package_results=package_rows,
            aggregate_results=aggregate_rows,
            excluded_results=excluded_rows,
            manifest_sha256_by_package=manifest_sha256_by_package,
            data_source=_text(row.get("data_source"), field_name="selection run data_source"),
            runtime_config=_strict_object_or_empty(row.get("runtime_config"), field_name="selection runtime_config"),
            valid_no_candidate=bool(row.get("valid_no_candidate")),
            no_candidate_reason=str(row.get("no_candidate_reason") or "").strip() or None,
            error=(
                _strict_object_or_empty(row.get("error_json"), field_name="selection error_json")
                if row.get("error_json") is not None
                else None
            ),
            created_at=row.get("created_at"),
            completed_at=row.get("completed_at"),
        )

    def list(self, *, package_id: str, manifest_sha256: str | None = None, limit: int = 100) -> list[ProjectedSelectionScoreArtifact]:
        if limit <= 0:
            raise AdvisoryProjectionReadOnlyError("selection artifact list limit must be positive")
        query = "selection_artifacts_manifest" if manifest_sha256 is not None else "selection_artifacts_package"
        params = (package_id, manifest_sha256, limit) if manifest_sha256 is not None else (package_id, limit)
        return [self._artifact_from_row(row) for row in self._many(query, params)]

    def get_selection_score_artifact(self, artifact_id: str) -> ProjectedSelectionScoreArtifact:
        row = self._one("selection_artifact_by_id", (artifact_id,))
        if row is None:
            raise AdvisoryProjectionReadOnlyError(f"selection score artifact is missing: {artifact_id}")
        return self._artifact_from_row(row)

    @staticmethod
    def _artifact_from_row(row: dict[str, Any]) -> ProjectedSelectionScoreArtifact:
        return ProjectedSelectionScoreArtifact(
            artifact_id=_text(row.get("artifact_id"), field_name="artifact_id"),
            package_id=_text(row.get("package_id"), field_name="artifact package_id"),
            manifest_sha256=_text(row.get("manifest_sha256"), field_name="artifact manifest_sha256"),
            trade_date=row["trade_date"],
            data_source=_text(row.get("data_source"), field_name="artifact data_source"),
            runtime_config_hash=_text(row.get("runtime_config_hash"), field_name="runtime_config_hash"),
            scores_json=[_object(item) for item in _list(row.get("scores_json")) if isinstance(item, dict)],
            artifact_sha256=str(row.get("artifact_sha256") or "").strip() or None,
            score_count=int(row.get("score_count") or 0),
            universe_count=int(row.get("universe_count") or 0),
            top_score_symbol=str(row.get("top_score_symbol") or "").strip() or None,
            status=_text(row.get("status"), field_name="artifact status"),
            metadata=_object(row.get("metadata")),
            artifact_contract_version=str(row.get("artifact_contract_version") or "").strip() or None,
            artifact_payload_sha256=str(row.get("artifact_payload_sha256") or "").strip() or None,
            artifact_input_context_hash=str(row.get("artifact_input_context_hash") or "").strip() or None,
            source_revision_set_hash=str(row.get("source_revision_set_hash") or "").strip() or None,
            asset_closure_hash=str(row.get("asset_closure_hash") or "").strip() or None,
            created_at=row.get("created_at"),
        )

    def list_source_events(self, *, dataset_name: str, source_role: str, partition_key: dict[str, Any]) -> list[SourceAvailabilityEvent]:
        partition_key_hash = canonical_json_sha256(canonicalize(partition_key))
        rows = self._many("source_events", (dataset_name, source_role, partition_key_hash))
        events: list[SourceAvailabilityEvent] = []
        for row in rows:
            item = SourceAvailabilityEventInput(
                dataset_name=_text(row.get("dataset_name"), field_name="source dataset_name"),
                source_role=_text(row.get("source_role"), field_name="source role"),
                partition_key=_object(row.get("partition_key")),
                partition_chain_key=_text(row.get("partition_chain_key"), field_name="partition_chain_key"),
                append_request_hash=_text(row.get("append_request_hash"), field_name="append_request_hash"),
                revision_id=_text(row.get("revision_id"), field_name="revision_id"),
                event_revision_no=int(row.get("event_revision_no") or 0),
                event_type=SourceAvailabilityEventType(_text(row.get("event_type"), field_name="event_type")),
                predecessor_event_hash=str(row.get("predecessor_event_hash") or "").strip() or None,
                provider_job_id=str(row.get("provider_job_id") or "").strip() or None,
                refresh_job_id=str(row.get("refresh_job_id") or "").strip() or None,
                provider_published_at=row.get("provider_published_at"),
                first_observed_at=row.get("first_observed_at"),
                schema_fingerprint=_text(row.get("schema_fingerprint"), field_name="schema_fingerprint"),
                row_count=int(row.get("row_count") or 0),
                partition_content_hash=_text(row.get("partition_content_hash"), field_name="partition_content_hash"),
                quality_status=_text(row.get("quality_status"), field_name="quality_status"),
                reason_codes=tuple(str(item) for item in _list(row.get("reason_codes"))),
                created_by_service_principal=_text(row.get("created_by_service_principal"), field_name="created_by_service_principal"),
            )
            event = SourceAvailabilityEvent.from_input(item)
            if event.availability_event_id != str(row.get("availability_event_id")) or event.event_content_hash != str(row.get("event_content_hash")):
                raise AdvisoryProjectionReadOnlyError("persisted source availability event hash readback mismatch")
            events.append(event)
        return events

    def get_historical_receipt(self, receipt_ref: str) -> tuple[HistoricalResearchBatch, HistoricalResearchBatchReceipt] | None:
        by_receipt_id = self._one("historical_receipt_by_receipt_id", (receipt_ref,))
        by_batch_id = self._one("historical_receipt_by_batch_id", (receipt_ref,))
        if (
            by_receipt_id is not None
            and by_batch_id is not None
            and str(by_receipt_id.get("receipt_id")) != str(by_batch_id.get("receipt_id"))
        ):
            raise AdvisoryProjectionReadOnlyError("historical receipt reference is ambiguous between receipt and batch identity")
        receipt_row = by_receipt_id or by_batch_id
        if receipt_row is None:
            return None
        batch_row = self._one("historical_batch", (str(receipt_row["batch_id"]),))
        if batch_row is None:
            raise AdvisoryProjectionReadOnlyError("historical receipt references a missing batch")
        run_ids = [str(value) for value in _list(receipt_row.get("program_run_ids"))]
        rows = self._many("historical_runs", (run_ids,))
        runs_by_id = {str(row["program_run_id"]): self._historical_run_from_row(row) for row in rows}
        if len(runs_by_id) != len(run_ids):
            raise AdvisoryProjectionReadOnlyError("historical receipt references a missing Program run")
        batch = HistoricalResearchBatch(
            batch_id=_text(batch_row.get("batch_id"), field_name="historical batch_id"),
            request_id=UUID(str(batch_row["request_id"])),
            batch_key=_text(batch_row.get("batch_key"), field_name="historical batch_key"),
            decision_trade_date=batch_row["decision_trade_date"],
            program_ids=[str(value) for value in _list(batch_row.get("program_ids"))],
            data_source=_text(batch_row.get("data_source"), field_name="historical data_source"),
            origin=_text(batch_row.get("origin"), field_name="historical origin"),
            request_payload_sha256=_text(batch_row.get("request_payload_sha256"), field_name="historical request hash"),
            research_scope=_text(batch_row.get("research_scope"), field_name="historical research_scope"),
            execution_prohibited=bool(batch_row.get("execution_prohibited")),
            status=HistoricalResearchRunStatus(_text(batch_row.get("status"), field_name="historical batch status")),
            created_at=batch_row["created_at"],
            updated_at=batch_row["updated_at"],
        )
        receipt = HistoricalResearchBatchReceipt(
            receipt_id=_text(receipt_row.get("receipt_id"), field_name="historical receipt_id"),
            batch_id=_text(receipt_row.get("batch_id"), field_name="historical receipt batch_id"),
            batch_key=_text(receipt_row.get("batch_key"), field_name="historical receipt batch_key"),
            status=HistoricalResearchRunStatus(_text(receipt_row.get("status"), field_name="historical receipt status")),
            program_runs=[runs_by_id[run_id] for run_id in run_ids],
            receipt_hash=_text(receipt_row.get("receipt_hash"), field_name="historical receipt hash"),
            created_at=receipt_row["created_at"],
        )
        return batch, receipt

    @staticmethod
    def _historical_run_from_row(row: dict[str, Any]) -> HistoricalResearchProgramRun:
        return HistoricalResearchProgramRun(
            program_run_id=_text(row.get("program_run_id"), field_name="historical program_run_id"),
            program_id=_text(row.get("program_id"), field_name="historical program_id"),
            decision_trade_date=row["decision_trade_date"],
            research_scope=_text(row.get("research_scope"), field_name="historical run scope"),
            status=HistoricalResearchRunStatus(_text(row.get("status"), field_name="historical run status")),
            program_payload_sha256=str(row.get("program_payload_sha256") or "").strip() or None,
            binding_version_id=str(row.get("binding_version_id") or "").strip() or None,
            binding_payload_hash=str(row.get("binding_payload_hash") or "").strip() or None,
            package_id=str(row.get("package_id") or "").strip() or None,
            manifest_sha256=str(row.get("manifest_sha256") or "").strip() or None,
            policy_hash=str(row.get("policy_hash") or "").strip() or None,
            effective_runtime_config_hash=str(row.get("effective_runtime_config_hash") or "").strip() or None,
            source_watermark_hash=str(row.get("source_watermark_hash") or "").strip() or None,
            evidence_id=str(row.get("evidence_id") or "").strip() or None,
            evidence_hash=str(row.get("evidence_hash") or "").strip() or None,
            artifact_id=str(row.get("artifact_id") or "").strip() or None,
            artifact_payload_hash=str(row.get("artifact_payload_hash") or "").strip() or None,
            research_list_version_id=str(row.get("research_list_version_id") or "").strip() or None,
            research_candidates=[HistoricalResearchCandidate.model_validate(item) for item in _list(row.get("research_candidates_json"))],
            candidate_outcome=str(row.get("candidate_outcome") or "").strip() or None,
            reason_codes=[str(value) for value in _list(row.get("reason_codes"))],
            error_json=_object(row.get("error_json")) or None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def probe(self, *, decision_date: date) -> list[SourceAvailability]:
        mappings = (
            ("source_probe_market_kline_daily_raw", "market_kline_daily_raw", "daily_market"),
            ("source_probe_market_daily_basic", "market_daily_basic", "daily_basic"),
            ("source_probe_market_moneyflow_ts", "market_moneyflow_ts", "moneyflow"),
            ("source_probe_market_sector_data", "market_sector_data", "sector"),
            ("source_probe_market_trading_calendar", "market_trading_calendar", "trading_calendar"),
        )
        rows: list[SourceAvailability] = []
        for query_id, source_id, capability in mappings:
            row = self._one(query_id, (decision_date,)) or {}
            watermark = row.get("watermark_date")
            rows.append(
                SourceAvailability(
                    source_id=source_id,
                    capability=capability,
                    decision_date=decision_date,
                    status=SourceAvailabilityStatus.PARTIAL if watermark is not None else SourceAvailabilityStatus.MISSING,
                    owner="AIstock local PostgreSQL",
                    authoritative_for=[capability],
                    schema_or_artifact=source_id.replace("_", ".", 1),
                    event_time_field="trade_date",
                    revision_rule="watermark_only_not_historical_available_at",
                    pit_join_predicate="trade_date <= decision_date",
                    watermark_date=watermark,
                    data_cutoff=watermark,
                    source_query_id=query_id,
                    query_template_version="advisory_phase0a_source_probe_v1",
                    query_hash=canonical_json_sha256({"query_id": query_id, "sql": _SQL[query_id]}),
                    parameter_hash=canonical_json_sha256({"decision_date": decision_date}),
                    row_count=1 if watermark is not None else 0,
                    is_point_in_time=False,
                    reason_codes=["ADVISORY_PHASE0A_SOURCE_AVAILABLE_AT_MISSING"],
                )
            )
        return rows

    def list_trading_days(self, *, start_date: date, end_date: date) -> list[date]:
        if end_date < start_date:
            return []
        return [row["cal_date"] for row in self._many("calendar_days", (start_date, end_date))]
