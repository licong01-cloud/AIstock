"""Read-only adapters and fail-closed evidence resolvers for Advisory Phase 0A."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Protocol

from backend.services.advisory_program import (
    BINDING_STATUS_DRAFT,
    PACKAGE_MODE_SINGLE,
    REASON_ADVISORY_MANUAL_MULTI_PACKAGE_DEPRECATED,
    AdvisoryProgram,
    AdvisoryStrategyBindingVersion,
)
from backend.services.selection_center.models import SelectionRun, SelectionRunStatus
from backend.services.simulation_runtime.models import DailySelectionEvidence
from backend.services.strategy_package.package_asset import StrategyPackageAssetRecord
from backend.services.strategy_package.repository import StrategyPackageRecord, manifest_asset_keys
from backend.services.strategy_package.selection_artifact import SelectionScoreArtifact

from .models import (
    AssetLedgerEntry,
    AuditTarget,
    CandidateDepthEvidence,
    CandidateAuthorityReport,
    CandidateAuthorityStatus,
    CandidateStage,
    DecisionClockEvidence,
    ExpectedAlphaMode,
    HMMVintageEvidence,
    RiskPolicyEvidence,
    RuntimeSemanticsEvidence,
    SourceAvailability,
    SourceAvailabilityStatus,
    StageCapability,
    StageCapabilityStatus,
    UniverseLayerEvidence,
    UniverseSurvivorshipEvidence,
)
from .policy import (
    REASON_BACKTEST_VINTAGE_FORBIDDEN,
    REASON_BINDING_HISTORICAL_MISSING,
    REASON_CANDIDATE_AUTHORITY_MISSING,
    REASON_HMM_HISTORICAL_MISSING,
    REASON_NO_CANDIDATE_AUTHORITY_MISSING,
    REASON_RUNTIME_HISTORICAL_MISSING,
    REASON_SOURCE_PIT_MISSING,
    canonical_json_sha256,
    normalized_reason_codes,
    stable_identifier,
)


REASON_BINDING_CONFLICT = "ADVISORY_PHASE0A_HISTORICAL_BINDING_CONFLICT"
REASON_BINDING_PACKAGE_MISMATCH = "ADVISORY_PHASE0A_BINDING_PACKAGE_MISMATCH"
REASON_PACKAGE_ALPHA_MODE_MISMATCH = "ADVISORY_PHASE0A_PACKAGE_ALPHA_MODE_MISMATCH"
REASON_PACKAGE_MANIFEST_MISSING = "ADVISORY_PHASE0A_PACKAGE_MANIFEST_MISSING"
REASON_ASSET_AVAILABLE_AT_MISSING = "ADVISORY_PHASE0A_ASSET_AVAILABLE_AT_MISSING"
REASON_ASSET_AVAILABLE_AFTER_DECISION = "ADVISORY_PHASE0A_ASSET_AVAILABLE_AFTER_DECISION"
REASON_ASSET_HASH_MISSING = "ADVISORY_PHASE0A_ASSET_HASH_MISSING"
REASON_ASSET_CLOSURE_MISSING = "ADVISORY_PHASE0A_ASSET_CLOSURE_MISSING"
REASON_SELECTION_EVIDENCE_MISSING = "ADVISORY_PHASE0A_SELECTION_EVIDENCE_MISSING"
REASON_SELECTION_EVIDENCE_MISMATCH = "ADVISORY_PHASE0A_SELECTION_EVIDENCE_MISMATCH"
REASON_SELECTION_LINEAGE_MISSING = "ADVISORY_PHASE0A_SELECTION_LINEAGE_MISSING"
REASON_SELECTION_RUN_MISMATCH = "ADVISORY_PHASE0A_SELECTION_RUN_MISMATCH"
REASON_SCORE_ARTIFACT_MISMATCH = "ADVISORY_PHASE0A_SELECTION_SCORE_ARTIFACT_MISMATCH"
REASON_SOURCE_TYPE_MISMATCH = "ADVISORY_PHASE0A_AUTHORITATIVE_SOURCE_TYPE_MISMATCH"
REASON_RUNTIME_GENERATED_FORBIDDEN = "ADVISORY_PHASE0A_RUNTIME_GENERATED_BINDING_FORBIDDEN"
REASON_RUNTIME_CONFIG_CHAIN_INCOMPLETE = "ADVISORY_PHASE0A_RUNTIME_CONFIG_CHAIN_INCOMPLETE"
REASON_HMM_DYNAMIC_LATEST_FORBIDDEN = "ADVISORY_PHASE0A_HMM_DYNAMIC_LATEST_FORBIDDEN"
REASON_SOURCE_AVAILABLE_AT_MISSING = "ADVISORY_PHASE0A_SOURCE_AVAILABLE_AT_MISSING"
REASON_STAGE_EVIDENCE_PARTIAL = "ADVISORY_PHASE0A_STAGE_EVIDENCE_PARTIAL"
REASON_CLOCK_INCOMPLETE = "ADVISORY_PHASE0A_FORMAL_CLOCK_INCOMPLETE"
REASON_CLOCK_NOT_IMMEDIATE = "ADVISORY_PHASE0A_TARGET_NOT_NEXT_TRADING_DAY"
REASON_CLOCK_TIMEZONE_MISSING = "ADVISORY_PHASE0A_DECISION_TIMESTAMP_TIMEZONE_MISSING"
REASON_DEPTH_REQUEST_MISSING = "ADVISORY_PHASE0A_REQUESTED_TOPK_MISSING"
REASON_DEPTH_OUT_OF_RANGE = "ADVISORY_PHASE0A_TOPK_OUT_OF_RANGE"
REASON_DEPTH_DISPLAY_MISSING = "ADVISORY_PHASE0A_DISPLAY_DEPTH_MISSING"
REASON_DEPTH_MANIFEST_MISSING = "ADVISORY_PHASE0A_MANIFEST_TOPK_MISSING"
REASON_DEPTH_ARTIFACT_MISSING = "ADVISORY_PHASE0A_ARTIFACT_TOPK_MISSING"
REASON_DEPTH_EFFECTIVE_SELECTION_MISSING = "ADVISORY_PHASE0A_EFFECTIVE_SELECTION_TOPK_MISSING"
REASON_DEPTH_INSUFFICIENT = "ADVISORY_PHASE0A_AUTHORITATIVE_DEPTH_INSUFFICIENT"
REASON_MULTI_ALPHA_TOPK_MISMATCH = "multi_alpha_topk_runtime_mismatch"
REASON_UNIVERSE_EVIDENCE_MISSING = "ADVISORY_PHASE0A_PIT_UNIVERSE_EVIDENCE_MISSING"
REASON_COHORT_SURVIVORSHIP_RISK = "ADVISORY_PHASE0A_PACKAGE_COHORT_SURVIVORSHIP_RISK"
REASON_RISK_EVIDENCE_PARTIAL = "ADVISORY_PHASE0A_RISK_POLICY_EVIDENCE_PARTIAL"

SINGLE_ALPHA_SOURCE_TYPE = "live_qe_model_inference_v1"
MULTI_ALPHA_SOURCE_TYPE = "live_multi_alpha_inference_v1"
AUTHORITATIVE_SELECTION_SCOPE = "authoritative_selection"
_GENERATED_RUNTIME_SOURCES = {
    "",
    "platform_default",
    "generated_effective_runtime_config",
    "ad_hoc_non_trading_preview",
}


class AdvisoryProgramReader(Protocol):
    def get_program(self, program_id: str) -> AdvisoryProgram: ...

    def list_binding_versions(self, program_id: str) -> list[AdvisoryStrategyBindingVersion]: ...


class StrategyPackageReader(Protocol):
    def get(self, package_id: str) -> StrategyPackageRecord: ...

    def list_package_assets(self, package_id: str, *, protected_only: bool = False) -> list[StrategyPackageAssetRecord]: ...


class DailySelectionEvidenceReader(Protocol):
    def get_daily_selection_evidence(self, evidence_id: str) -> DailySelectionEvidence: ...


class SelectionScoreArtifactReader(Protocol):
    def list(
        self,
        *,
        package_id: str,
        manifest_sha256: str | None = None,
        limit: int = 100,
    ) -> list[SelectionScoreArtifact]: ...


class SelectionRunReader(Protocol):
    def get_run(self, run_id: str) -> SelectionRun: ...


class SourceProbe(Protocol):
    def probe(self, *, decision_date: date) -> list[SourceAvailability]: ...


class TradingCalendarReader(Protocol):
    def list_trading_days(self, *, start_date: date, end_date: date) -> list[date]: ...


_SOURCE_PROBE_SQL: dict[str, tuple[str, str]] = {
    "market_kline_daily_raw": (
        "SELECT MAX(trade_date) AS watermark_date FROM market.kline_daily_raw WHERE trade_date <= %s",
        "daily_market",
    ),
    "market_daily_basic": (
        "SELECT MAX(trade_date) AS watermark_date FROM market.daily_basic WHERE trade_date <= %s",
        "daily_basic",
    ),
    "market_moneyflow_ts": (
        "SELECT MAX(trade_date) AS watermark_date FROM market.moneyflow_ts WHERE trade_date <= %s",
        "moneyflow",
    ),
    "market_sector_data": (
        "SELECT MAX(trade_date) AS watermark_date FROM market.sector_data WHERE trade_date <= %s",
        "sector",
    ),
    "market_trading_calendar": (
        "SELECT MAX(cal_date) AS watermark_date FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date <= %s",
        "trading_calendar",
    ),
}
SOURCE_PROBE_TEMPLATE_VERSION = "advisory_phase0a_source_probe_v1"


class PostgresReadOnlySourceProbe:
    """Run only a fixed SELECT allowlist and report watermarks without claiming historical availability."""

    def __init__(self, conn_factory: Any) -> None:
        self._conn_factory = conn_factory

    def probe(self, *, decision_date: date) -> list[SourceAvailability]:
        rows: list[SourceAvailability] = []
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                for query_id, (sql, capability) in _SOURCE_PROBE_SQL.items():
                    if not sql.lstrip().upper().startswith("SELECT"):
                        raise RuntimeError("Phase 0A source probe allowlist must contain SELECT only")
                    cur.execute(sql, (decision_date,))
                    row = cur.fetchone()
                    if isinstance(row, dict):
                        watermark = _coerce_date(row.get("watermark_date"))
                    elif row:
                        watermark = _coerce_date(row[0])
                    else:
                        watermark = None
                    rows.append(
                        SourceAvailability(
                            source_id=query_id,
                            capability=capability,
                            decision_date=decision_date,
                            status=SourceAvailabilityStatus.PARTIAL if watermark is not None else SourceAvailabilityStatus.MISSING,
                            owner="AIstock local PostgreSQL",
                            authoritative_for=[capability],
                            schema_or_artifact=query_id.replace("_", ".", 1),
                            event_time_field="trade_date",
                            revision_rule="watermark_only_not_historical_available_at",
                            pit_join_predicate="trade_date <= decision_date",
                            watermark_date=watermark,
                            data_cutoff=watermark,
                            source_query_id=query_id,
                            query_template_version=SOURCE_PROBE_TEMPLATE_VERSION,
                            query_hash=canonical_json_sha256({"query_id": query_id, "sql": sql}),
                            parameter_hash=canonical_json_sha256({"decision_date": decision_date}),
                            row_count=1 if row else 0,
                            is_point_in_time=False,
                            reason_codes=[REASON_SOURCE_AVAILABLE_AT_MISSING],
                        )
                    )
        return rows

    def list_trading_days(self, *, start_date: date, end_date: date) -> list[date]:
        if end_date < start_date:
            return []
        sql = (
            "SELECT cal_date FROM market.trading_calendar "
            "WHERE is_trading = TRUE AND cal_date >= %s AND cal_date <= %s ORDER BY cal_date ASC"
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (start_date, end_date))
                rows = cur.fetchall()
        return [_coerce_date(row[0] if not isinstance(row, dict) else row.get("cal_date")) for row in rows if _coerce_date(row[0] if not isinstance(row, dict) else row.get("cal_date")) is not None]


@dataclass(frozen=True)
class AuditReaders:
    """Only read-only repository methods are exposed to the audit service."""

    advisory: AdvisoryProgramReader
    package: StrategyPackageReader
    evidence: DailySelectionEvidenceReader
    score_artifact: SelectionScoreArtifactReader
    selection_run: SelectionRunReader
    source_probe: SourceProbe | None = None
    calendar: TradingCalendarReader | None = None


@dataclass(frozen=True)
class BindingResolution:
    binding: AdvisoryStrategyBindingVersion | None
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResolvedAuditDay:
    target: AuditTarget
    decision_date: date
    program: AdvisoryProgram | None = None
    binding: AdvisoryStrategyBindingVersion | None = None
    package: StrategyPackageRecord | None = None
    package_assets: tuple[StrategyPackageAssetRecord, ...] = ()
    evidence: DailySelectionEvidence | None = None
    phase0a_reason_codes: tuple[str, ...] = ()
    upstream_reason_codes: tuple[str, ...] = ()

    def with_reason_codes(self, *reason_codes: str) -> "ResolvedAuditDay":
        return ResolvedAuditDay(
            target=self.target,
            decision_date=self.decision_date,
            program=self.program,
            binding=self.binding,
            package=self.package,
            package_assets=self.package_assets,
            evidence=self.evidence,
            phase0a_reason_codes=tuple(normalized_reason_codes([*self.phase0a_reason_codes, *reason_codes])),
            upstream_reason_codes=self.upstream_reason_codes,
        )


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    return None


def _coerce_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            return date.fromisoformat(candidate[:10])
        except ValueError:
            return None
    return None


def _reason_from_exception(exc: Exception, fallback: str) -> list[str]:
    context = getattr(exc, "context", None)
    if isinstance(context, dict) and context.get("reason_code"):
        return [fallback, str(context["reason_code"])]
    return [fallback]


def resolve_as_of_binding(
    *,
    bindings: list[AdvisoryStrategyBindingVersion],
    decision_date: date,
    target: AuditTarget,
) -> BindingResolution:
    """Resolve exactly one explicit historical binding; never fall back to active/latest."""

    candidates: list[AdvisoryStrategyBindingVersion] = []
    for binding in bindings:
        if binding.activation_status == BINDING_STATUS_DRAFT:
            continue
        if binding.effective_from_trade_date is None:
            continue
        if binding.effective_from_trade_date > decision_date:
            continue
        if binding.effective_to_trade_date is not None and binding.effective_to_trade_date <= decision_date:
            continue
        candidates.append(binding)
    if not candidates:
        return BindingResolution(None, (REASON_BINDING_HISTORICAL_MISSING,))
    if len(candidates) != 1:
        return BindingResolution(None, (REASON_BINDING_CONFLICT,))
    binding = candidates[0]
    reasons: list[str] = []
    if binding.package_mode != PACKAGE_MODE_SINGLE or len(binding.package_ids) != 1:
        reasons.append(REASON_ADVISORY_MANUAL_MULTI_PACKAGE_DEPRECATED)
    elif binding.package_ids[0] != target.package_id:
        reasons.append(REASON_BINDING_PACKAGE_MISMATCH)
    if reasons:
        return BindingResolution(None, tuple(normalized_reason_codes(reasons)))
    return BindingResolution(binding)


def resolve_audit_day(*, readers: AuditReaders, target: AuditTarget, decision_date: date) -> ResolvedAuditDay:
    """Load all available source records without invoking a service that can write or generate."""

    resolved = ResolvedAuditDay(target=target, decision_date=decision_date)
    try:
        program = readers.advisory.get_program(target.program_id)
    except Exception as exc:  # Source failures become auditable unavailable results.
        return resolved.with_reason_codes(*_reason_from_exception(exc, REASON_BINDING_HISTORICAL_MISSING))
    resolved = ResolvedAuditDay(target=target, decision_date=decision_date, program=program)
    try:
        binding_resolution = resolve_as_of_binding(
            bindings=readers.advisory.list_binding_versions(target.program_id),
            decision_date=decision_date,
            target=target,
        )
    except Exception as exc:
        return resolved.with_reason_codes(*_reason_from_exception(exc, REASON_BINDING_HISTORICAL_MISSING))
    if binding_resolution.binding is None:
        return resolved.with_reason_codes(*binding_resolution.reason_codes)
    resolved = ResolvedAuditDay(
        target=target,
        decision_date=decision_date,
        program=program,
        binding=binding_resolution.binding,
    )
    try:
        package = readers.package.get(target.package_id)
    except Exception as exc:
        return resolved.with_reason_codes(*_reason_from_exception(exc, REASON_PACKAGE_MANIFEST_MISSING))
    if (
        package.manifest_sha256 != package.manifest.manifest_sha256
        or package.manifest_sha256 != target.manifest_sha256
    ):
        return ResolvedAuditDay(
            target=target,
            decision_date=decision_date,
            program=program,
            binding=binding_resolution.binding,
            package=package,
        ).with_reason_codes(REASON_PACKAGE_MANIFEST_MISSING)
    expected_mode = target.expected_alpha_mode.value
    actual_mode = package.alpha_mode.value
    if actual_mode != expected_mode:
        return ResolvedAuditDay(
            target=target,
            decision_date=decision_date,
            program=program,
            binding=binding_resolution.binding,
            package=package,
        ).with_reason_codes(REASON_PACKAGE_ALPHA_MODE_MISMATCH)
    try:
        assets = tuple(readers.package.list_package_assets(target.package_id, protected_only=True))
    except Exception as exc:
        assets = ()
        resolved = ResolvedAuditDay(
            target=target,
            decision_date=decision_date,
            program=program,
            binding=binding_resolution.binding,
            package=package,
        ).with_reason_codes(*_reason_from_exception(exc, REASON_ASSET_AVAILABLE_AT_MISSING))
    else:
        resolved = ResolvedAuditDay(
            target=target,
            decision_date=decision_date,
            program=program,
            binding=binding_resolution.binding,
            package=package,
            package_assets=assets,
        )
    evidence_id = target.selection_evidence_ids_by_decision_date.get(decision_date)
    if not evidence_id:
        return resolved.with_reason_codes(REASON_SELECTION_EVIDENCE_MISSING)
    try:
        evidence = readers.evidence.get_daily_selection_evidence(evidence_id)
    except Exception as exc:
        return resolved.with_reason_codes(*_reason_from_exception(exc, REASON_SELECTION_EVIDENCE_MISSING))
    return ResolvedAuditDay(
        target=resolved.target,
        decision_date=resolved.decision_date,
        program=resolved.program,
        binding=resolved.binding,
        package=resolved.package,
        package_assets=resolved.package_assets,
        evidence=evidence,
        phase0a_reason_codes=resolved.phase0a_reason_codes,
        upstream_reason_codes=resolved.upstream_reason_codes,
    )


def build_asset_ledger(resolved: ResolvedAuditDay) -> list[AssetLedgerEntry]:
    """Record explicit asset availability only; created_at and backtest data_vintage never promote formality."""

    if resolved.package is None:
        return []
    entries: list[AssetLedgerEntry] = []
    manifest = resolved.package.manifest
    source_evidence = manifest.source_evidence if isinstance(manifest.source_evidence, dict) else {}
    parent_vintage = source_evidence.get("phase0a_parent_vintage")
    parent_vintage = parent_vintage if isinstance(parent_vintage, dict) else {}
    parent_available_at = _coerce_datetime(parent_vintage.get("available_at"))
    parent_cutoff = _coerce_date(parent_vintage.get("information_cutoff") or parent_vintage.get("data_cutoff"))
    parent_reasons = []
    if not resolved.package.manifest_sha256:
        parent_reasons.append(REASON_ASSET_HASH_MISSING)
    if parent_available_at is None:
        parent_reasons.append(REASON_ASSET_AVAILABLE_AT_MISSING)
    elif parent_available_at.date() > resolved.decision_date:
        parent_reasons.append(REASON_ASSET_AVAILABLE_AFTER_DECISION)
    entries.append(
        AssetLedgerEntry(
            package_id=resolved.package.package_id,
            asset_type="parent_package",
            asset_ref=f"manifest:{resolved.package.manifest_sha256}",
            asset_id=resolved.package.package_id,
            asset_sha256=resolved.package.manifest_sha256,
            asset_role="parent_package",
            parent_or_lineage_ids=[resolved.package.source_id],
            available_at=parent_available_at,
            data_cutoff=parent_cutoff,
            information_cutoff_ts=_coerce_datetime(parent_vintage.get("information_cutoff_ts")),
            frozen_at=_coerce_datetime(parent_vintage.get("frozen_at")),
            promoted_or_activated_at=_coerce_datetime(parent_vintage.get("promoted_or_activated_at")),
            evidence_source_type="strategy_package_manifest",
            evidence_ref=resolved.package.package_id,
            evidence_hash=resolved.package.manifest_sha256,
            admissibility="FORMAL" if not parent_reasons else "RETROSPECTIVE_ONLY",
            reason_codes=normalized_reason_codes(parent_reasons),
        )
    )
    leg_vintages = source_evidence.get("phase0a_alpha_leg_vintages")
    leg_vintages = leg_vintages if isinstance(leg_vintages, dict) else {}
    for component in manifest.alpha_components:
        leg_vintage = leg_vintages.get(component.alpha_id)
        leg_vintage = leg_vintage if isinstance(leg_vintage, dict) else {}
        available_at = _coerce_datetime(leg_vintage.get("available_at"))
        cutoff = _coerce_date(leg_vintage.get("information_cutoff") or leg_vintage.get("data_cutoff"))
        reasons: list[str] = []
        if available_at is None:
            reasons.append(REASON_ASSET_AVAILABLE_AT_MISSING)
        elif available_at.date() > resolved.decision_date:
            reasons.append(REASON_ASSET_AVAILABLE_AFTER_DECISION)
        leg_payload = {
            "alpha_id": component.alpha_id,
            "factor_ids": component.factor_ids,
            "model_id": component.model_id,
            "model_ref": component.model_ref,
            "component_weight": component.component_weight,
            "score_direction": component.score_direction,
            "score_normalization": component.score_normalization,
            "holding_period": component.holding_period,
            "rebalance_frequency": component.rebalance_frequency,
        }
        entries.append(
            AssetLedgerEntry(
                package_id=resolved.package.package_id,
                asset_type="alpha_leg",
                asset_ref=f"alpha:{component.alpha_id}",
                asset_id=component.alpha_id,
                asset_sha256=canonical_json_sha256(leg_payload),
                asset_role="alpha_leg",
                parent_or_lineage_ids=[resolved.package.package_id],
                available_at=available_at,
                data_cutoff=cutoff,
                information_cutoff_ts=_coerce_datetime(leg_vintage.get("information_cutoff_ts")),
                training_data_end_ts=_coerce_datetime(leg_vintage.get("training_data_end_ts")),
                model_selection_decision_ts=_coerce_datetime(leg_vintage.get("model_selection_decision_ts")),
                research_decision_ts=_coerce_datetime(leg_vintage.get("research_decision_ts")),
                frozen_at=_coerce_datetime(leg_vintage.get("frozen_at")),
                evidence_source_type="strategy_package_manifest_alpha_component",
                evidence_ref=component.alpha_id,
                evidence_hash=canonical_json_sha256(leg_payload),
                admissibility="FORMAL" if not reasons else "RETROSPECTIVE_ONLY",
                reason_codes=normalized_reason_codes(reasons),
            )
        )
    if manifest.alpha_mode.value == ExpectedAlphaMode.MULTI_ALPHA.value:
        weight_vintage = source_evidence.get("phase0a_multi_alpha_weight_vintage")
        weight_vintage = weight_vintage if isinstance(weight_vintage, dict) else {}
        available_at = _coerce_datetime(weight_vintage.get("available_at"))
        cutoff = _coerce_date(weight_vintage.get("information_cutoff") or weight_vintage.get("data_cutoff"))
        reasons = []
        if available_at is None:
            reasons.append(REASON_ASSET_AVAILABLE_AT_MISSING)
        elif available_at.date() > resolved.decision_date:
            reasons.append(REASON_ASSET_AVAILABLE_AFTER_DECISION)
        weight_payload = manifest.alpha_combination_policy.model_dump(mode="python")
        weight_hash = canonical_json_sha256(weight_payload)
        entries.append(
            AssetLedgerEntry(
                package_id=resolved.package.package_id,
                asset_type="multi_alpha_weight",
                asset_ref=manifest.alpha_combination_policy.method,
                asset_id="multi_alpha_combination_policy",
                asset_sha256=weight_hash,
                asset_role="multi_alpha_weight",
                parent_or_lineage_ids=[resolved.package.package_id],
                available_at=available_at,
                data_cutoff=cutoff,
                information_cutoff_ts=_coerce_datetime(weight_vintage.get("information_cutoff_ts")),
                frozen_at=_coerce_datetime(weight_vintage.get("frozen_at")),
                evidence_source_type="strategy_package_manifest_combination_policy",
                evidence_ref=manifest.alpha_combination_policy.method,
                evidence_hash=weight_hash,
                admissibility="FORMAL" if not reasons else "RETROSPECTIVE_ONLY",
                reason_codes=normalized_reason_codes(reasons),
            )
        )
    for asset in sorted(resolved.package_assets, key=lambda item: (item.asset_type.value, item.asset_ref, item.asset_sha256 or "")):
        metadata = asset.metadata or {}
        available_at = _coerce_datetime(metadata.get("available_at"))
        data_cutoff = _coerce_date(metadata.get("data_cutoff"))
        reasons: list[str] = []
        if not asset.asset_sha256:
            reasons.append(REASON_ASSET_HASH_MISSING)
        if available_at is None:
            reasons.append(REASON_ASSET_AVAILABLE_AT_MISSING)
        elif available_at.date() > resolved.decision_date:
            reasons.append(REASON_ASSET_AVAILABLE_AFTER_DECISION)
        admissibility = "FORMAL" if not reasons else "RETROSPECTIVE_ONLY"
        entries.append(
            AssetLedgerEntry(
                package_id=resolved.package.package_id,
                asset_type=asset.asset_type.value,
                asset_ref=asset.asset_ref,
                asset_id=str(asset.asset_id) if asset.asset_id is not None else None,
                asset_sha256=asset.asset_sha256,
                asset_role=asset.asset_role,
                created_at=asset.created_at,
                available_at=available_at,
                data_cutoff=data_cutoff,
                information_cutoff_ts=_coerce_datetime(metadata.get("information_cutoff_ts")),
                training_data_end_ts=_coerce_datetime(metadata.get("training_data_end_ts")),
                model_selection_decision_ts=_coerce_datetime(metadata.get("model_selection_decision_ts")),
                research_decision_ts=_coerce_datetime(metadata.get("research_decision_ts")),
                frozen_at=_coerce_datetime(metadata.get("frozen_at")),
                promoted_or_activated_at=_coerce_datetime(metadata.get("promoted_or_activated_at")),
                evidence_source_type=str(metadata.get("evidence_source_type") or "").strip() or None,
                evidence_ref=str(metadata.get("evidence_ref") or "").strip() or None,
                evidence_hash=str(metadata.get("evidence_hash") or "").strip() or None,
                admissibility=admissibility,
                reason_codes=normalized_reason_codes(reasons),
            )
        )
    actual_keys = {
        (asset.asset_type, asset.asset_ref, asset.asset_sha256)
        for asset in resolved.package_assets
        if asset.asset_sha256
    }
    try:
        expected_keys = manifest_asset_keys(resolved.package.manifest)
    except Exception:
        entries.append(
            AssetLedgerEntry(
                package_id=resolved.package.package_id,
                asset_type="manifest_runtime_asset_closure",
                asset_ref="strategy_package_manifest",
                asset_role="required_runtime_closure",
                admissibility="UNAVAILABLE",
                reason_codes=[REASON_ASSET_CLOSURE_MISSING],
            )
        )
    else:
        for asset_type, asset_ref, asset_sha256 in sorted(
            expected_keys - actual_keys,
            key=lambda item: (item[0].value, item[1], item[2]),
        ):
            entries.append(
                AssetLedgerEntry(
                    package_id=resolved.package.package_id,
                    asset_type=asset_type.value,
                    asset_ref=asset_ref,
                    asset_sha256=asset_sha256,
                    asset_role="required_runtime_closure",
                    admissibility="UNAVAILABLE",
                    reason_codes=[REASON_ASSET_CLOSURE_MISSING],
                )
            )
    if resolved.package.data_vintage is not None:
        entries.append(
            AssetLedgerEntry(
                package_id=resolved.package.package_id,
                asset_type="backtest_derived_data_vintage",
                asset_ref="strategy_pkg.package.data_vintage",
                asset_role="forbidden_research_metadata",
                data_cutoff=resolved.package.data_vintage,
                admissibility="FORBIDDEN",
                reason_codes=[REASON_BACKTEST_VINTAGE_FORBIDDEN],
            )
        )
    return entries


def resolve_runtime_semantics(resolved: ResolvedAuditDay) -> RuntimeSemanticsEvidence:
    if resolved.evidence is None:
        return RuntimeSemanticsEvidence(
            decision_date=resolved.decision_date,
            package_id=resolved.target.package_id,
            reason_codes=[REASON_RUNTIME_HISTORICAL_MISSING],
        )
    payload = resolved.evidence.evidence_payload_json or {}
    binding = payload.get("runtime_profile_binding")
    binding = binding if isinstance(binding, dict) else {}
    config_chain = payload.get("phase0a_effective_config_chain")
    config_chain = config_chain if isinstance(config_chain, dict) else {}
    required_chain_keys = (
        "binding_base_config",
        "request_override_config",
        "date_enforced_config",
        "selection_normalized_config",
        "package_effective_config",
        "runtime_variant_id",
        "selection_adapter_version",
        "query_template_version",
    )
    config_hashes: dict[str, str | None] = {}
    for key in required_chain_keys:
        value = config_chain.get(key)
        if value is None:
            config_hashes[key] = None
        elif isinstance(value, str) and key.endswith(("_id", "_version")):
            config_hashes[key] = value.strip() or None
        else:
            config_hashes[key] = canonical_json_sha256(value)
    runtime_profile_payload = payload.get("runtime_profile")
    runtime_profile_payload = runtime_profile_payload if isinstance(runtime_profile_payload, dict) else {}
    final_profile_matches = bool(
        config_chain.get("selection_normalized_config") is not None
        and canonical_json_sha256(config_chain.get("selection_normalized_config"))
        == canonical_json_sha256(runtime_profile_payload)
    )
    chain_complete = all(config_hashes.values()) and final_profile_matches
    runtime_semantics_id = canonical_json_sha256(
        {
            "runtime_profile_version_id": resolved.evidence.runtime_profile_version_id,
            "runtime_profile_hash": resolved.evidence.runtime_profile_hash,
            "source": binding.get("source"),
            "config_hashes": config_hashes,
        }
    )
    source = str(binding.get("source") or "").strip()
    available_at = _coerce_datetime(
        binding.get("available_at")
        or binding.get("activated_at")
        or payload.get("runtime_profile_available_at")
    )
    is_historical = bool(
        source not in _GENERATED_RUNTIME_SOURCES
        and resolved.evidence.runtime_profile_version_id
        and resolved.evidence.runtime_profile_hash
        and chain_complete
        and available_at is not None
        and available_at.date() <= resolved.decision_date
    )
    reasons: list[str] = []
    if source in _GENERATED_RUNTIME_SOURCES:
        reasons.append(REASON_RUNTIME_GENERATED_FORBIDDEN)
    if not is_historical:
        reasons.append(REASON_RUNTIME_HISTORICAL_MISSING)
    if not chain_complete:
        reasons.append(REASON_RUNTIME_CONFIG_CHAIN_INCOMPLETE)
    return RuntimeSemanticsEvidence(
        decision_date=resolved.decision_date,
        package_id=resolved.target.package_id,
        evidence_id=resolved.evidence.evidence_id,
        runtime_profile_version_id=resolved.evidence.runtime_profile_version_id,
        runtime_profile_hash=resolved.evidence.runtime_profile_hash,
        runtime_binding_source=source or None,
        selection_runtime_semantics_id=runtime_semantics_id,
        effective_config_hashes=config_hashes,
        effective_config_chain_complete=chain_complete,
        historical_available_at=available_at,
        is_historical_binding=is_historical,
        source_payload_hash=canonical_json_sha256(binding) if binding else None,
        reason_codes=normalized_reason_codes(reasons),
    )


def _has_explicit_timezone(value: Any) -> bool:
    if isinstance(value, datetime):
        return value.tzinfo is not None
    if isinstance(value, str):
        candidate = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(candidate).tzinfo is not None
        except ValueError:
            return False
    return False


def resolve_decision_clock(resolved: ResolvedAuditDay) -> DecisionClockEvidence:
    """Preserve the true T/T+1 identity and fail closed when its calendar/time proof is absent."""

    payload = resolved.evidence.evidence_payload_json if resolved.evidence is not None else {}
    payload = payload or {}
    point_in_time = payload.get("point_in_time_context")
    point_in_time = point_in_time if isinstance(point_in_time, dict) else {}
    decision_cutoff_raw = point_in_time.get("decision_cutoff_ts") or payload.get("decision_cutoff_ts")
    data_available_raw = point_in_time.get("data_available_at") or payload.get("data_available_at")
    generated_raw = point_in_time.get("decision_generated_at") or payload.get("decision_generated_at")
    selection_as_of = _coerce_date(point_in_time.get("selection_as_of_trade_date") or point_in_time.get("cutoff_date"))
    effective_cutoff = _coerce_date(point_in_time.get("cutoff_date") or (resolved.evidence.cutoff_date if resolved.evidence else None))
    target_trade_date = resolved.evidence.target_trade_date if resolved.evidence is not None else None
    score_trade_date = _coerce_date(point_in_time.get("score_trade_date") or effective_cutoff)
    reference_price_trade_date = _coerce_date(point_in_time.get("reference_price_trade_date") or effective_cutoff)
    requested_selection_as_of = _coerce_date(point_in_time.get("requested_selection_as_of_trade_date"))
    immediate_value = point_in_time.get("is_immediately_previous_trade_date")
    immediate = immediate_value if isinstance(immediate_value, bool) else None
    timezone_name = str(point_in_time.get("timezone") or "").strip() or "Asia/Shanghai"
    decision_cutoff = _coerce_datetime(decision_cutoff_raw)
    data_available = _coerce_datetime(data_available_raw)
    decision_generated = _coerce_datetime(generated_raw)
    reasons: list[str] = []
    if immediate is not True:
        reasons.append(REASON_CLOCK_NOT_IMMEDIATE)
    if not (_has_explicit_timezone(decision_cutoff_raw) and _has_explicit_timezone(data_available_raw)):
        reasons.append(REASON_CLOCK_TIMEZONE_MISSING)
    formal = bool(
        selection_as_of == resolved.decision_date
        and effective_cutoff == resolved.decision_date
        and target_trade_date is not None
        and target_trade_date > resolved.decision_date
        and score_trade_date == resolved.decision_date
        and reference_price_trade_date == resolved.decision_date
        and immediate is True
        and timezone_name == "Asia/Shanghai"
        and decision_cutoff is not None
        and data_available is not None
        and data_available <= decision_cutoff
    )
    if not formal:
        reasons.append(REASON_CLOCK_INCOMPLETE)
    return DecisionClockEvidence(
        decision_as_of_trade_date=resolved.decision_date,
        selection_as_of_trade_date=selection_as_of,
        target_trade_date=target_trade_date,
        selection_run_trade_date=target_trade_date,
        score_trade_date=score_trade_date,
        reference_price_trade_date=reference_price_trade_date,
        effective_entry_trade_date=target_trade_date,
        requested_selection_as_of_trade_date=requested_selection_as_of,
        effective_cutoff_date=effective_cutoff,
        decision_cutoff_ts=decision_cutoff,
        data_available_at=data_available,
        decision_generated_at=decision_generated,
        timezone=timezone_name,
        calendar_version=str(point_in_time.get("calendar_version") or "").strip() or None,
        calendar_hash=str(point_in_time.get("calendar_hash") or "").strip() or None,
        is_immediately_previous_trade_date=immediate,
        is_formal_canonical_clock=formal,
        reason_codes=normalized_reason_codes(reasons),
    )


def resolve_hmm_vintage(resolved: ResolvedAuditDay) -> HMMVintageEvidence:
    payload = resolved.evidence.evidence_payload_json if resolved.evidence is not None else {}
    payload = payload or {}
    runtime_profile = payload.get("runtime_profile")
    runtime_profile = runtime_profile if isinstance(runtime_profile, dict) else {}
    hmm = runtime_profile.get("hmm")
    hmm = hmm if isinstance(hmm, dict) else {}
    enabled = bool(hmm.get("enabled"))
    if not enabled:
        return HMMVintageEvidence(
            decision_date=resolved.decision_date,
            package_id=resolved.target.package_id,
            enabled=False,
            status="NOT_APPLICABLE",
        )
    metadata = payload.get("phase0a_hmm_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    snapshot_id = str(hmm.get("model_snapshot_id") or metadata.get("model_snapshot_id") or "").strip() or None
    config_id = str(hmm.get("model_config_id") or metadata.get("model_config_id") or "").strip() or None
    signal_preset = str(hmm.get("signal_preset") or metadata.get("signal_preset") or "").strip() or None
    model_artifact_sha256 = str(metadata.get("model_artifact_sha256") or "").strip() or None
    coefficient_sha256 = str(metadata.get("coefficient_sha256") or "").strip() or None
    available_at = _coerce_datetime(metadata.get("available_at"))
    snapshot_trained_at = _coerce_datetime(metadata.get("snapshot_trained_at"))
    training_information_cutoff = _coerce_date(metadata.get("training_information_cutoff"))
    as_of_trade_date = _coerce_date(metadata.get("as_of_trade_date"))
    effective_trade_date = _coerce_date(metadata.get("effective_trade_date"))
    generation_mode = str(metadata.get("generation_mode") or "").strip() or None
    input_data_max_dates = metadata.get("input_data_max_dates")
    input_data_max_dates_hash = canonical_json_sha256(input_data_max_dates) if input_data_max_dates else None
    freshness_lag = _coerce_int(metadata.get("freshness_lag"))
    formal = bool(
        snapshot_id
        and signal_preset
        and model_artifact_sha256
        and coefficient_sha256
        and snapshot_trained_at is not None
        and available_at is not None
        and training_information_cutoff is not None
        and as_of_trade_date == resolved.decision_date
        and effective_trade_date == (resolved.evidence.target_trade_date if resolved.evidence is not None else None)
        and generation_mode
        and input_data_max_dates_hash
        and available_at.date() <= resolved.decision_date
    )
    reasons: list[str] = []
    if not snapshot_id and config_id:
        reasons.append(REASON_HMM_DYNAMIC_LATEST_FORBIDDEN)
    if not formal:
        reasons.append(REASON_HMM_HISTORICAL_MISSING)
    return HMMVintageEvidence(
        decision_date=resolved.decision_date,
        package_id=resolved.target.package_id,
        enabled=True,
        status="FORMAL" if formal else "UNAVAILABLE",
        model_snapshot_id=snapshot_id,
        model_config_id=config_id,
        signal_preset=signal_preset,
        model_artifact_sha256=model_artifact_sha256,
        coefficient_sha256=coefficient_sha256,
        snapshot_trained_at=snapshot_trained_at,
        available_at=available_at,
        training_information_cutoff=training_information_cutoff,
        as_of_trade_date=as_of_trade_date,
        effective_trade_date=effective_trade_date,
        generation_mode=generation_mode,
        input_data_max_dates_hash=input_data_max_dates_hash,
        freshness_lag=freshness_lag,
        reason_codes=normalized_reason_codes(reasons),
    )


_UNIVERSE_LAYERS = (
    "listed_universe",
    "seasoned_universe",
    "pit_st_delist_risk_universe",
    "package_eligible_universe",
    "risk_can_buy_universe",
    "tradability_industry_universe",
)


def _source_status(value: Any, *, fallback: SourceAvailabilityStatus) -> SourceAvailabilityStatus:
    try:
        return SourceAvailabilityStatus(str(value))
    except ValueError:
        return fallback


def resolve_universe_survivorship(resolved: ResolvedAuditDay) -> UniverseSurvivorshipEvidence:
    """Read only explicit PIT universe evidence; never derive it from current listings or final candidates."""

    payload = resolved.evidence.evidence_payload_json if resolved.evidence is not None else {}
    payload = payload or {}
    source = payload.get("phase0a_universe_evidence")
    if isinstance(source, dict):
        raw_layers = source.get("layers")
        cohort = source.get("package_cohort")
    else:
        raw_layers = None
        cohort = None
    raw_layers = raw_layers if isinstance(raw_layers, list) else []
    by_layer = {
        str(item.get("layer") or "").strip(): item
        for item in raw_layers
        if isinstance(item, dict) and str(item.get("layer") or "").strip()
    }
    layers: list[UniverseLayerEvidence] = []
    reasons: list[str] = []
    for name in _UNIVERSE_LAYERS:
        item = by_layer.get(name)
        if item is None:
            layers.append(
                UniverseLayerEvidence(
                    layer=name,
                    status=SourceAvailabilityStatus.MISSING,
                    reason_codes=[REASON_UNIVERSE_EVIDENCE_MISSING],
                )
            )
            reasons.append(REASON_UNIVERSE_EVIDENCE_MISSING)
            continue
        available_at = _coerce_datetime(item.get("available_at"))
        policy_available_at = _coerce_datetime(item.get("policy_available_at"))
        status = _source_status(item.get("status"), fallback=SourceAvailabilityStatus.PARTIAL)
        required_universe_fields = (
            item.get("policy_hash"),
            item.get("input_count"),
            item.get("output_count"),
            item.get("excluded_count"),
            item.get("symbol_set_hash"),
            available_at,
            policy_available_at,
        )
        if (
            available_at is None
            or available_at.date() > resolved.decision_date
            or policy_available_at is None
            or policy_available_at.date() > resolved.decision_date
        ):
            status = SourceAvailabilityStatus.RESEARCH_ONLY if status == SourceAvailabilityStatus.FORMAL_READY else status
            item_reasons = [*item.get("reason_codes", []), REASON_UNIVERSE_EVIDENCE_MISSING]
        elif status == SourceAvailabilityStatus.FORMAL_READY and not all(value not in (None, "") for value in required_universe_fields):
            status = SourceAvailabilityStatus.PARTIAL
            item_reasons = [*item.get("reason_codes", []), REASON_UNIVERSE_EVIDENCE_MISSING]
        else:
            item_reasons = list(item.get("reason_codes", []))
        layers.append(
            UniverseLayerEvidence(
                layer=name,
                status=status,
                policy_hash=str(item.get("policy_hash") or "").strip() or None,
                input_count=_coerce_int(item.get("input_count")),
                output_count=_coerce_int(item.get("output_count")),
                excluded_count=_coerce_int(item.get("excluded_count")),
                exclusion_reason_counts={str(key): int(value) for key, value in (item.get("exclusion_reason_counts") or {}).items()},
                symbol_set_hash=str(item.get("symbol_set_hash") or "").strip() or None,
                available_at=available_at,
                policy_available_at=policy_available_at,
                reason_codes=normalized_reason_codes(item_reasons),
            )
        )
        reasons.extend(item_reasons)
    cohort = cohort if isinstance(cohort, dict) else {}
    cohort_status = _source_status(cohort.get("status"), fallback=SourceAvailabilityStatus.MISSING)
    cohort_reasons = list(cohort.get("reason_codes", []))
    if not cohort:
        cohort_reasons.append(REASON_COHORT_SURVIVORSHIP_RISK)
    return UniverseSurvivorshipEvidence(
        decision_date=resolved.decision_date,
        package_id=resolved.target.package_id,
        layers=layers,
        package_cohort_status=cohort_status,
        package_cohort_reason_codes=normalized_reason_codes(cohort_reasons),
        reason_codes=normalized_reason_codes([*reasons, *cohort_reasons]),
    )


def resolve_risk_policy_evidence(resolved: ResolvedAuditDay) -> RiskPolicyEvidence:
    payload = resolved.evidence.evidence_payload_json if resolved.evidence is not None else {}
    payload = payload or {}
    profile = payload.get("runtime_profile")
    profile = profile if isinstance(profile, dict) else {}
    risk_policy = profile.get("risk_policy")
    risk_policy = risk_policy if isinstance(risk_policy, dict) else {}
    tradability = profile.get("tradability")
    tradability = tradability if isinstance(tradability, dict) else {}
    industry_blacklist = profile.get("industry_blacklist")
    industry_blacklist = industry_blacklist if isinstance(industry_blacklist, list) else []
    stage_evidence = payload.get("phase0a_stage_evidence")
    stage_evidence = stage_evidence if isinstance(stage_evidence, dict) else {}
    has_explicit_risk_stage = isinstance(stage_evidence.get("risk_policy_adjusted"), dict)
    risk_metadata = payload.get("phase0a_risk_policy_metadata")
    risk_metadata = risk_metadata if isinstance(risk_metadata, dict) else {}
    enabled = bool(risk_policy.get("enabled"))
    configured_filter = enabled or bool(industry_blacklist) or bool(tradability)
    if not configured_filter:
        status = StageCapabilityStatus.NOT_APPLICABLE
        reasons: list[str] = []
    elif has_explicit_risk_stage:
        status = StageCapabilityStatus.FULL
        reasons = []
    else:
        status = StageCapabilityStatus.PARTIAL
        reasons = [REASON_RISK_EVIDENCE_PARTIAL]
    return RiskPolicyEvidence(
        decision_date=resolved.decision_date,
        package_id=resolved.target.package_id,
        risk_policy_hash=canonical_json_sha256(risk_policy) if risk_policy else None,
        risk_policy_enabled=enabled,
        industry_blacklist_hash=canonical_json_sha256(industry_blacklist) if industry_blacklist else None,
        tradability_policy_hash=canonical_json_sha256(tradability) if tradability else None,
        policy_available_at=_coerce_datetime(risk_metadata.get("policy_available_at")),
        status=status,
        reason_codes=normalized_reason_codes(reasons),
    )


def _manifest_top_k_contract(resolved: ResolvedAuditDay) -> tuple[int | None, list[int], str | None]:
    if resolved.package is None:
        return None, [], None
    context = resolved.package.manifest.backtest_context
    context = context if isinstance(context, dict) else {}
    daily = context.get("daily_strategy")
    daily = daily if isinstance(daily, dict) else {}
    manifest_top_k = _coerce_int(daily.get("topk") or context.get("topk"))
    raw_variants = daily.get("topk_variants") or context.get("topk_variants") or []
    if not isinstance(raw_variants, list):
        raw_variants = [raw_variants]
    secondary = daily.get("secondary_topk") or context.get("secondary_topk")
    if secondary is not None:
        raw_variants.append(secondary)
    variants = sorted({value for value in (_coerce_int(item) for item in raw_variants) if value is not None})
    if manifest_top_k is not None and manifest_top_k not in variants:
        variants.append(manifest_top_k)
    contract = {
        "manifest_top_k": manifest_top_k,
        "allowed_top_k_variants": sorted(variants),
        "alpha_mode": resolved.package.alpha_mode.value,
        "daily_strategy": daily,
    }
    return manifest_top_k, sorted(variants), canonical_json_sha256(contract)


def resolve_depth_evidence(
    *,
    resolved: ResolvedAuditDay,
    artifact: SelectionScoreArtifact | None,
) -> CandidateDepthEvidence:
    """Audit depth explicitly, never infer a deep pool from a display or final candidate count."""

    payload = resolved.evidence.evidence_payload_json if resolved.evidence is not None else {}
    payload = payload or {}
    config = payload.get("selection_artifact_config")
    config = config if isinstance(config, dict) else {}
    profile = payload.get("runtime_profile")
    profile = profile if isinstance(profile, dict) else {}
    selection = profile.get("selection")
    selection = selection if isinstance(selection, dict) else {}
    requested_top_k = _coerce_int(config.get("requested_top_k") or selection.get("top_k") or config.get("top_k"))
    display_top_n = _coerce_int(config.get("display_top_n") or config.get("display_top_k"))
    program_target_count = resolved.program.target_count if resolved.program is not None else None
    review_policy = resolved.program.review_policy if resolved.program is not None else {}
    review_depth = _coerce_int((review_policy or {}).get("rank_exit_threshold"))
    requested_depth_values = [item for item in (requested_top_k, program_target_count, review_depth) if item is not None]
    requested_depth = max(requested_depth_values) if requested_depth_values else None
    manifest_top_k, variants, contract_hash = _manifest_top_k_contract(resolved)
    runtime_variant_id = str(config.get("runtime_variant_id") or "").strip() or None
    metadata = artifact.metadata if artifact is not None else {}
    artifact_top_k = _coerce_int(metadata.get("top_k") if isinstance(metadata, dict) else None)
    effective_artifact_top_k = _coerce_int(metadata.get("effective_top_k") if isinstance(metadata, dict) else None)
    artifact_score_count = artifact.score_count if artifact is not None else None
    artifact_universe_count = artifact.universe_count if artifact is not None else None
    ranks = [
        _coerce_int(row.get("rank"))
        for row in (artifact.scores_json if artifact is not None else [])
        if isinstance(row, dict)
    ]
    observed_max_rank = max((rank for rank in ranks if rank is not None), default=None)
    effective_selection_top_k = _coerce_int(config.get("effective_selection_top_k"))
    selection_depth = resolved.evidence.candidate_count if resolved.evidence is not None else None
    reasons: list[str] = []
    if requested_top_k is None:
        reasons.append(REASON_DEPTH_REQUEST_MISSING)
    elif not 1 <= requested_top_k <= 50:
        reasons.append(REASON_DEPTH_OUT_OF_RANGE)
    if display_top_n is None:
        reasons.append(REASON_DEPTH_DISPLAY_MISSING)
    if manifest_top_k is None:
        reasons.append(REASON_DEPTH_MANIFEST_MISSING)
    if artifact is None or artifact_top_k is None or effective_artifact_top_k is None:
        reasons.append(REASON_DEPTH_ARTIFACT_MISSING)
    if effective_selection_top_k is None:
        reasons.append(REASON_DEPTH_EFFECTIVE_SELECTION_MISSING)
    if resolved.package is not None and resolved.package.alpha_mode.value == ExpectedAlphaMode.MULTI_ALPHA.value:
        if requested_top_k is None or not variants or requested_top_k not in variants:
            reasons.append(REASON_MULTI_ALPHA_TOPK_MISMATCH)
    if artifact is None or artifact_top_k is None or effective_artifact_top_k is None:
        depth_satisfied = None
    else:
        required_rows = min(requested_depth or artifact_top_k, artifact_universe_count or artifact_top_k)
        depth_satisfied = artifact_score_count is not None and artifact_score_count >= required_rows
        if artifact_top_k < (requested_top_k or artifact_top_k) or effective_artifact_top_k < (requested_top_k or effective_artifact_top_k):
            reasons.append(REASON_DEPTH_INSUFFICIENT)
        if depth_satisfied is False:
            reasons.append(REASON_DEPTH_INSUFFICIENT)
    return CandidateDepthEvidence(
        requested_top_k=requested_top_k,
        requested_observation_depth=requested_depth,
        display_top_n=display_top_n,
        manifest_top_k=manifest_top_k,
        allowed_top_k_variants=variants,
        runtime_variant_id=runtime_variant_id,
        contract_top_k=manifest_top_k,
        artifact_top_k=artifact_top_k,
        effective_artifact_top_k=effective_artifact_top_k,
        alpha_artifact_row_count=artifact_score_count,
        hmm_input_depth=_coerce_int((payload.get("phase0a_stage_evidence") or {}).get("hmm_adjusted", {}).get("input_count"))
        if isinstance((payload.get("phase0a_stage_evidence") or {}).get("hmm_adjusted"), dict)
        else None,
        effective_selection_top_k=effective_selection_top_k,
        selection_effective_depth=selection_depth,
        artifact_score_count=artifact_score_count,
        artifact_universe_count=artifact_universe_count,
        observed_max_rank=observed_max_rank,
        depth_satisfied=depth_satisfied,
        contract_hash=contract_hash,
        reason_codes=normalized_reason_codes(reasons),
    )


def resolve_source_availability(resolved: ResolvedAuditDay, *, source_probe: SourceProbe | None) -> list[SourceAvailability]:
    """Use immutable evidence payloads and optional allowlisted probes; never infer available_at from query time."""

    rows: list[SourceAvailability] = []
    if resolved.evidence is not None:
        payload = resolved.evidence.evidence_payload_json or {}
        pit = payload.get("point_in_time_context")
        pit = pit if isinstance(pit, dict) else {}
        cutoff = _coerce_date(pit.get("cutoff_date") or resolved.evidence.cutoff_date)
        target_trade_date = _coerce_date(pit.get("effective_trade_date") or resolved.evidence.target_trade_date)
        pit_mode = str(pit.get("pit_mode") or "").strip().upper()
        is_pit = bool(
            cutoff == resolved.decision_date
            and target_trade_date is not None
            and target_trade_date > resolved.decision_date
            and pit_mode not in {"", "NONE"}
        )
        rows.append(
            SourceAvailability(
                source_id=str(pit.get("calendar_source") or "selection_pit_context"),
                capability="selection_point_in_time_clock",
                decision_date=resolved.decision_date,
                status=SourceAvailabilityStatus.FORMAL_READY if is_pit else SourceAvailabilityStatus.MISSING,
                data_cutoff=cutoff,
                is_point_in_time=is_pit,
                reason_codes=[] if is_pit else [REASON_SOURCE_PIT_MISSING],
            )
        )
    else:
        rows.append(
            SourceAvailability(
                source_id="selection_pit_context",
                capability="selection_point_in_time_clock",
                decision_date=resolved.decision_date,
                status=SourceAvailabilityStatus.MISSING,
                is_point_in_time=False,
                reason_codes=[REASON_SOURCE_PIT_MISSING],
            )
        )
    if source_probe is not None:
        try:
            rows.extend(source_probe.probe(decision_date=resolved.decision_date))
        except Exception:
            rows.append(
                SourceAvailability(
                    source_id="phase0a_source_probe",
                    capability="source_watermark",
                    decision_date=resolved.decision_date,
                    status=SourceAvailabilityStatus.MISSING,
                    reason_codes=[REASON_SOURCE_PIT_MISSING],
                )
            )
    return rows


def _lineage_payload(evidence: DailySelectionEvidence) -> dict[str, Any]:
    payload = evidence.evidence_payload_json or {}
    lineage = payload.get("phase0a_candidate_lineage")
    return lineage if isinstance(lineage, dict) else {}


def _find_score_artifact(
    *,
    readers: AuditReaders,
    package_id: str,
    manifest_sha256: str,
    lineage: dict[str, Any],
) -> SelectionScoreArtifact | None:
    artifact_id = str(lineage.get("selection_score_artifact_id") or "").strip()
    artifact_sha256 = str(lineage.get("selection_score_artifact_sha256") or "").strip()
    if not artifact_id and not artifact_sha256:
        return None
    candidates = readers.score_artifact.list(package_id=package_id, manifest_sha256=manifest_sha256, limit=1000)
    matches = [
        artifact
        for artifact in candidates
        if (not artifact_id or artifact.artifact_id == artifact_id)
        and (not artifact_sha256 or artifact.artifact_sha256 == artifact_sha256)
    ]
    return matches[0] if len(matches) == 1 else None


def _stage_rows(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return None
    return [dict(item) for item in value]


def _stage_content_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(item: dict[str, Any]) -> tuple[int, str]:
        rank = _coerce_int(item.get("rank"))
        return (rank if rank is not None else 2**31 - 1, str(item.get("symbol") or ""))

    return [
        {
            "symbol": str(item.get("symbol") or "").strip(),
            "rank": _coerce_int(item.get("rank")),
            "score": item.get("score"),
            "reason": item.get("reason"),
            "component_scores": item.get("component_scores") if isinstance(item.get("component_scores"), dict) else None,
        }
        for item in sorted(rows, key=sort_key)
    ]


def _stage_capability(
    *,
    stage: CandidateStage,
    status: StageCapabilityStatus,
    rows: list[dict[str, Any]] | None,
    input_count: int | None = None,
    output_count: int | None = None,
    excluded_count: int | None = None,
    artifact_hash: str | None = None,
    semantic_payload: dict[str, Any] | None = None,
    reason_codes: list[str] | None = None,
) -> StageCapability:
    content_hash = canonical_json_sha256(_stage_content_payload(rows)) if rows is not None else None
    return StageCapability(
        stage=stage,
        status=status,
        input_count=input_count,
        output_count=output_count if output_count is not None else (len(rows) if rows is not None else None),
        excluded_count=excluded_count,
        candidate_count=len(rows) if rows is not None else None,
        artifact_hash=artifact_hash,
        content_hash=content_hash,
        semantic_hash=canonical_json_sha256(semantic_payload or {"stage": stage.value}),
        reason_codes=normalized_reason_codes(reason_codes or []),
    )


def _stage_capabilities(
    *,
    evidence: DailySelectionEvidence | None,
    artifact: SelectionScoreArtifact | None,
    hmm: HMMVintageEvidence,
    risk: RiskPolicyEvidence,
) -> list[StageCapability]:
    payload = evidence.evidence_payload_json if evidence is not None else {}
    payload = payload or {}
    stage_evidence = payload.get("phase0a_stage_evidence")
    stage_evidence = stage_evidence if isinstance(stage_evidence, dict) else {}
    raw_rows = _stage_rows(artifact.scores_json) if artifact is not None else None
    raw = _stage_capability(
        stage=CandidateStage.ALPHA_RAW,
        status=StageCapabilityStatus.FULL if raw_rows is not None else StageCapabilityStatus.UNAVAILABLE,
        rows=raw_rows,
        input_count=artifact.universe_count if artifact is not None else None,
        output_count=artifact.score_count if artifact is not None else None,
        artifact_hash=artifact.artifact_sha256 if artifact is not None else None,
        semantic_payload={"source_type": artifact.metadata.get("source_type") if artifact is not None else None},
        reason_codes=[] if raw_rows is not None else [REASON_STAGE_EVIDENCE_PARTIAL],
    )
    hmm_payload = stage_evidence.get(CandidateStage.HMM_ADJUSTED.value)
    hmm_payload = hmm_payload if isinstance(hmm_payload, dict) else None
    if not hmm.enabled:
        hmm_stage = _stage_capability(
            stage=CandidateStage.HMM_ADJUSTED,
            status=StageCapabilityStatus.NOT_APPLICABLE,
            rows=None,
            semantic_payload={"hmm": "HMM_DISABLED"},
        )
    elif hmm_payload is None:
        hmm_stage = _stage_capability(
            stage=CandidateStage.HMM_ADJUSTED,
            status=StageCapabilityStatus.PARTIAL,
            rows=None,
            semantic_payload={"hmm": hmm.model_snapshot_id, "coefficient": hmm.coefficient_sha256},
            reason_codes=[REASON_STAGE_EVIDENCE_PARTIAL],
        )
    else:
        hmm_rows = _stage_rows(hmm_payload.get("candidates"))
        hmm_stage = _stage_capability(
            stage=CandidateStage.HMM_ADJUSTED,
            status=StageCapabilityStatus.FULL if hmm_rows is not None else StageCapabilityStatus.PARTIAL,
            rows=hmm_rows,
            input_count=_coerce_int(hmm_payload.get("input_count")),
            output_count=_coerce_int(hmm_payload.get("output_count")),
            excluded_count=_coerce_int(hmm_payload.get("excluded_count")),
            semantic_payload={"hmm": hmm.model_snapshot_id, "coefficient": hmm.coefficient_sha256, "payload": hmm_payload.get("semantic")},
            reason_codes=[] if hmm_rows is not None else [REASON_STAGE_EVIDENCE_PARTIAL],
        )
    risk_payload = stage_evidence.get(CandidateStage.RISK_POLICY_ADJUSTED.value)
    risk_payload = risk_payload if isinstance(risk_payload, dict) else None
    if risk.status == StageCapabilityStatus.NOT_APPLICABLE:
        risk_stage = _stage_capability(
            stage=CandidateStage.RISK_POLICY_ADJUSTED,
            status=StageCapabilityStatus.NOT_APPLICABLE,
            rows=None,
            semantic_payload={"risk_policy_hash": risk.risk_policy_hash, "status": "NOT_APPLICABLE"},
        )
    elif risk_payload is None:
        risk_stage = _stage_capability(
            stage=CandidateStage.RISK_POLICY_ADJUSTED,
            status=StageCapabilityStatus.PARTIAL,
            rows=None,
            semantic_payload={"risk_policy_hash": risk.risk_policy_hash},
            reason_codes=[REASON_STAGE_EVIDENCE_PARTIAL],
        )
    else:
        risk_rows = _stage_rows(risk_payload.get("candidates"))
        risk_stage = _stage_capability(
            stage=CandidateStage.RISK_POLICY_ADJUSTED,
            status=StageCapabilityStatus.FULL if risk_rows is not None else StageCapabilityStatus.PARTIAL,
            rows=risk_rows,
            input_count=_coerce_int(risk_payload.get("input_count")),
            output_count=_coerce_int(risk_payload.get("output_count")),
            excluded_count=_coerce_int(risk_payload.get("excluded_count")),
            semantic_payload={"risk_policy_hash": risk.risk_policy_hash, "payload": risk_payload.get("semantic")},
            reason_codes=[] if risk_rows is not None else [REASON_STAGE_EVIDENCE_PARTIAL],
        )
    selected_rows = _stage_rows(payload.get("selected_candidates"))
    excluded_rows = _stage_rows(payload.get("excluded_candidates"))
    effective = _stage_capability(
        stage=CandidateStage.SELECTION_EFFECTIVE,
        status=(
            StageCapabilityStatus.FULL
            if selected_rows is not None and evidence is not None and len(selected_rows) == evidence.candidate_count
            else StageCapabilityStatus.PARTIAL if evidence is not None else StageCapabilityStatus.UNAVAILABLE
        ),
        rows=selected_rows,
        input_count=_coerce_int((stage_evidence.get(CandidateStage.SELECTION_EFFECTIVE.value) or {}).get("input_count"))
        if isinstance(stage_evidence.get(CandidateStage.SELECTION_EFFECTIVE.value), dict)
        else None,
        output_count=evidence.candidate_count if evidence is not None else None,
        excluded_count=len(excluded_rows) if excluded_rows is not None else None,
        artifact_hash=evidence.artifact_hash if evidence is not None else None,
        semantic_payload={
            "runtime_profile": payload.get("runtime_profile"),
            "exclusions": _stage_content_payload(excluded_rows) if excluded_rows is not None else None,
        },
        reason_codes=[] if selected_rows is not None else [REASON_STAGE_EVIDENCE_PARTIAL],
    )
    advisory = _stage_capability(
        stage=CandidateStage.ADVISORY_MODEL,
        status=StageCapabilityStatus.NOT_APPLICABLE,
        rows=None,
        semantic_payload={"status": "NOT_IMPLEMENTED"},
        reason_codes=["ADVISORY_PHASE0A_ADVISORY_MODEL_NOT_IMPLEMENTED"],
    )
    return [raw, hmm_stage, risk_stage, effective, advisory]


def _selection_run_content_hash(run: SelectionRun | None) -> str | None:
    if run is None:
        return None
    payload = run.model_dump(mode="python")
    for key in ("run_id", "created_at", "completed_at"):
        payload.pop(key, None)
    return canonical_json_sha256(payload)


def _hmm_evidence_hash(hmm: HMMVintageEvidence) -> str:
    return canonical_json_sha256(
        {
            "enabled": hmm.enabled,
            "status": hmm.status,
            "model_snapshot_id": hmm.model_snapshot_id,
            "model_config_id": hmm.model_config_id,
            "signal_preset": hmm.signal_preset,
            "model_artifact_sha256": hmm.model_artifact_sha256,
            "coefficient_sha256": hmm.coefficient_sha256,
            "snapshot_trained_at": hmm.snapshot_trained_at,
            "available_at": hmm.available_at,
            "training_information_cutoff": hmm.training_information_cutoff,
            "as_of_trade_date": hmm.as_of_trade_date,
            "effective_trade_date": hmm.effective_trade_date,
            "generation_mode": hmm.generation_mode,
            "input_data_max_dates_hash": hmm.input_data_max_dates_hash,
        }
    )


def _same_stage_content(left: list[dict[str, Any]] | None, right: list[dict[str, Any]] | None) -> bool:
    return left is not None and right is not None and _stage_content_payload(left) == _stage_content_payload(right)


def resolve_candidate_authority(
    *,
    readers: AuditReaders,
    resolved: ResolvedAuditDay,
    hmm: HMMVintageEvidence,
    clock: DecisionClockEvidence,
    risk: RiskPolicyEvidence,
    universe: UniverseSurvivorshipEvidence,
) -> CandidateAuthorityReport:
    """Require immutable SSA -> SelectionRun -> DSE linkage before formal authority."""

    package = resolved.package
    evidence = resolved.evidence
    if package is None or evidence is None:
        return CandidateAuthorityReport(
            decision_date=resolved.decision_date,
            package_id=resolved.target.package_id,
            manifest_sha256=package.manifest_sha256 if package is not None else None,
            signal_context_hash=None,
            status=CandidateAuthorityStatus.NONE,
            decision_clock=clock,
            risk_policy=risk,
            stage_capabilities=_stage_capabilities(evidence=evidence, artifact=None, hmm=hmm, risk=risk),
            phase0a_reason_codes=normalized_reason_codes(
                [*resolved.phase0a_reason_codes, REASON_CANDIDATE_AUTHORITY_MISSING]
            ),
        )
    reasons: list[str] = list(resolved.phase0a_reason_codes)
    upstream: list[str] = []
    if evidence.package_id != package.package_id or evidence.manifest_sha256 != package.manifest_sha256:
        reasons.append(REASON_SELECTION_EVIDENCE_MISMATCH)
    if evidence.candidate_count == 0:
        reasons.append(REASON_NO_CANDIDATE_AUTHORITY_MISSING)
        return CandidateAuthorityReport(
            decision_date=resolved.decision_date,
            package_id=package.package_id,
            manifest_sha256=package.manifest_sha256,
            signal_context_hash=_signal_context_hash(
                resolved=resolved,
                hmm=hmm,
                artifact=None,
                run=None,
                clock=clock,
                risk=risk,
                universe=universe,
                depth=None,
                stages=[],
            ),
            status=CandidateAuthorityStatus.NONE,
            evidence_id=evidence.evidence_id,
            source_type=evidence.source_type,
            effective_depth=evidence.candidate_count,
            decision_clock=clock,
            risk_policy=risk,
            stage_capabilities=_stage_capabilities(evidence=evidence, artifact=None, hmm=hmm, risk=risk),
            phase0a_reason_codes=normalized_reason_codes(reasons),
            upstream_reason_codes=normalized_reason_codes(upstream),
        )
    expected_source = SINGLE_ALPHA_SOURCE_TYPE if package.alpha_mode.value == ExpectedAlphaMode.SINGLE_ALPHA.value else MULTI_ALPHA_SOURCE_TYPE
    if evidence.source_type != expected_source:
        reasons.append(REASON_SOURCE_TYPE_MISMATCH)
    payload = evidence.evidence_payload_json or {}
    upstream_values = payload.get("upstream_reason_codes")
    if isinstance(upstream_values, list):
        upstream.extend(str(value) for value in upstream_values)
    lineage = _lineage_payload(evidence)
    run_id = str(lineage.get("selection_run_id") or "").strip() or None
    artifact: SelectionScoreArtifact | None = None
    run: SelectionRun | None = None
    if not run_id:
        reasons.append(REASON_SELECTION_LINEAGE_MISSING)
    else:
        try:
            run = readers.selection_run.get_run(run_id)
        except Exception:
            reasons.append(REASON_SELECTION_RUN_MISMATCH)
    if run is not None:
        if (
            run.status != SelectionRunStatus.SUCCEEDED
            or package.package_id not in run.package_ids
            or run.manifest_sha256_by_package.get(package.package_id) != package.manifest_sha256
        ):
            reasons.append(REASON_SELECTION_RUN_MISMATCH)
    try:
        artifact = _find_score_artifact(
            readers=readers,
            package_id=package.package_id,
            manifest_sha256=package.manifest_sha256,
            lineage=lineage,
        )
    except Exception:
        artifact = None
    if artifact is None:
        reasons.append(REASON_SCORE_ARTIFACT_MISMATCH)
    elif (
        artifact.trade_date != resolved.decision_date
        or artifact.status.value != "SUCCEEDED"
        or artifact.metadata.get("source_type") != expected_source
        or artifact.metadata.get("authority_scope") != AUTHORITATIVE_SELECTION_SCOPE
    ):
        reasons.append(REASON_SCORE_ARTIFACT_MISMATCH)
    if evidence.cutoff_date != resolved.decision_date or evidence.target_trade_date <= resolved.decision_date:
        reasons.append(REASON_SELECTION_EVIDENCE_MISMATCH)
    depth = resolve_depth_evidence(resolved=resolved, artifact=artifact)
    reasons.extend(depth.reason_codes)
    reasons.extend(clock.reason_codes)
    selected_rows = _stage_rows(payload.get("selected_candidates"))
    artifact_rows = _stage_rows(artifact.scores_json) if artifact is not None else None
    run_rows = None
    if run is not None:
        package_rows = run.package_results.get(package.package_id)
        if package_rows is not None:
            run_rows = [item.model_dump(mode="python") for item in package_rows]
        elif run.aggregate_results:
            run_rows = [item.model_dump(mode="python") for item in run.aggregate_results]
    if selected_rows is None or len(selected_rows) != evidence.candidate_count:
        reasons.append(REASON_SELECTION_EVIDENCE_MISMATCH)
    if artifact_rows is not None and selected_rows is not None:
        raw_symbols = {str(item.get("symbol") or "").strip() for item in artifact_rows}
        selected_symbols = {str(item.get("symbol") or "").strip() for item in selected_rows}
        if not selected_symbols.issubset(raw_symbols):
            reasons.append(REASON_SELECTION_EVIDENCE_MISMATCH)
    if run is not None and not _same_stage_content(selected_rows, run_rows):
        reasons.append(REASON_SELECTION_RUN_MISMATCH)
    stages = _stage_capabilities(evidence=evidence, artifact=artifact, hmm=hmm, risk=risk)
    formal = not reasons and run is not None and artifact is not None
    status = CandidateAuthorityStatus.FORMAL if formal else CandidateAuthorityStatus.RETROSPECTIVE
    if not formal:
        reasons.append(REASON_CANDIDATE_AUTHORITY_MISSING)
    signal_context_hash = _signal_context_hash(
        resolved=resolved,
        hmm=hmm,
        artifact=artifact,
        run=run,
        clock=clock,
        risk=risk,
        universe=universe,
        depth=depth,
        stages=stages,
    )
    observation_id = stable_identifier(
        "sigobs",
        {
            "decision_as_of_trade_date": resolved.decision_date,
            "target_trade_date": evidence.target_trade_date,
            "signal_context_hash": signal_context_hash,
        },
    )
    return CandidateAuthorityReport(
        decision_date=resolved.decision_date,
        package_id=package.package_id,
        manifest_sha256=package.manifest_sha256,
        signal_context_hash=signal_context_hash,
        canonical_signal_observation_id=observation_id,
        status=status,
        evidence_id=evidence.evidence_id,
        selection_run_id=run_id,
        selection_run_content_hash=_selection_run_content_hash(run),
        selection_score_artifact_id=artifact.artifact_id if artifact is not None else None,
        selection_score_artifact_sha256=artifact.artifact_sha256 if artifact is not None else None,
        daily_selection_evidence_hash=evidence.artifact_hash,
        hmm_evidence_hash=_hmm_evidence_hash(hmm),
        source_type=evidence.source_type,
        requested_top_k=depth.requested_top_k,
        display_top_k=depth.display_top_n,
        artifact_depth=depth.artifact_score_count,
        effective_depth=depth.selection_effective_depth,
        depth_evidence=depth,
        decision_clock=clock,
        risk_policy=risk,
        stage_capabilities=stages,
        phase0a_reason_codes=normalized_reason_codes(reasons),
        upstream_reason_codes=normalized_reason_codes(upstream),
    )


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _signal_context_hash(
    *,
    resolved: ResolvedAuditDay,
    hmm: HMMVintageEvidence,
    artifact: SelectionScoreArtifact | None,
    run: SelectionRun | None,
    clock: DecisionClockEvidence,
    risk: RiskPolicyEvidence,
    universe: UniverseSurvivorshipEvidence,
    depth: CandidateDepthEvidence | None,
    stages: list[StageCapability],
) -> str:
    """Canonical signal identity deliberately excludes Program and binding lineage identifiers."""

    evidence = resolved.evidence
    payload = evidence.evidence_payload_json if evidence is not None else {}
    payload = payload or {}
    runtime_profile = payload.get("runtime_profile") if isinstance(payload, dict) else None
    universe_layers = [
        {
            "layer": layer.layer,
            "policy_hash": layer.policy_hash,
            "symbol_set_hash": layer.symbol_set_hash,
            "status": layer.status,
        }
        for layer in universe.layers
    ]
    depth_payload = depth.model_dump(mode="python") if depth is not None else None
    if depth_payload is not None:
        depth_payload.pop("reason_codes", None)
    clock_payload = clock.model_dump(mode="python")
    clock_payload.pop("reason_codes", None)
    return canonical_json_sha256(
        {
            "schema_version": "advisory_phase0a_signal_context_v1",
            "decision_as_of_trade_date": resolved.decision_date,
            "target_trade_date": evidence.target_trade_date if evidence is not None else None,
            "package_id": resolved.package.package_id if resolved.package is not None else resolved.target.package_id,
            "manifest_sha256": resolved.package.manifest_sha256 if resolved.package is not None else None,
            "selection_runtime_semantics_id": canonical_json_sha256(runtime_profile) if isinstance(runtime_profile, dict) else None,
            "effective_selection_profile_hash": evidence.runtime_profile_hash if evidence is not None else None,
            "selection_score_artifact": {
                "artifact_id": artifact.artifact_id if artifact is not None else None,
                "artifact_hash": artifact.artifact_sha256 if artifact is not None else None,
            },
            "selection_run_content_hash": _selection_run_content_hash(run),
            "daily_selection_evidence_hash": evidence.artifact_hash if evidence is not None else None,
            "eligible_universe": universe_layers,
            "hmm_evidence_hash": _hmm_evidence_hash(hmm) if hmm.enabled else "HMM_DISABLED",
            "risk_policy_hash": risk.risk_policy_hash,
            "depth": depth_payload,
            "decision_clock": clock_payload,
            "stage_content_hashes": {stage.stage.value: stage.content_hash for stage in stages},
        }
    )
