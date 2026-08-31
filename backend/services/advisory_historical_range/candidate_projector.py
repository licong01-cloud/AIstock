"""Project one closed StrategyPackage stage trace into Phase 1R candidate facts."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from math import isfinite
from typing import Any, Iterable, Mapping

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCandidateFactV1,
    HistoricalRangeFrozenProgramV1,
    derive_prefixed_id,
)
from backend.services.strategy_package.selection_computation import (
    PreparedPackageSignalV1,
    SelectionCandidate,
    SelectionStageTrace,
    StageEvidenceReceipt,
    StrategyPackageSelectionComputationResultV1,
)
from backend.services.strategy_package.selection_signal_preparation import PreparedRawSelectionArtifactV2
from backend.services.trading_core.errors import ArtifactGenerationFailedError


class HistoricalRangeCandidateProjector:
    def project(
        self,
        *,
        frozen_program: HistoricalRangeFrozenProgramV1,
        day_run_id: str,
        prepared_signal: PreparedPackageSignalV1,
        raw_artifact: PreparedRawSelectionArtifactV2,
        computation: StrategyPackageSelectionComputationResultV1,
        runtime_profile_hash: str,
    ) -> tuple[tuple[HistoricalRangeCandidateFactV1, ...], dict[str, Any]]:
        package_id = frozen_program.package_id
        trace = computation.stage_trace_by_package.get(package_id)
        if trace is None:
            raise ArtifactGenerationFailedError(
                "historical candidate computation omitted the package stage trace",
                context={"package_id": package_id, "day_run_id": day_run_id},
            )
        if package_id not in computation.package_results or package_id not in computation.excluded_results:
            raise ArtifactGenerationFailedError(
                "historical candidate computation omitted package results",
                context={"package_id": package_id, "day_run_id": day_run_id},
            )
        stage_trace = _stage_trace_payload(trace)
        alpha = _candidate_map(prepared_signal.alpha_raw_candidates, stage="alpha_raw")
        hmm = _candidate_map(prepared_signal.hmm_adjusted_candidates, stage="hmm_adjusted")
        risk = _receipt_candidate_map(trace.risk_policy_adjusted, stage="risk_policy_adjusted")
        selected = _receipt_candidate_map(trace.selection_effective, stage="selection_effective")
        raw_symbols = set(alpha)
        for stage_name, rows in (("hmm_adjusted", hmm), ("risk_policy_adjusted", risk), ("selection_effective", selected)):
            unexpected = sorted(set(rows) - raw_symbols)
            if unexpected:
                raise ArtifactGenerationFailedError(
                    "a later candidate stage introduced symbols absent from alpha_raw",
                    context={"package_id": package_id, "stage": stage_name, "symbols": unexpected[:20]},
                )
        exclusions = _exclusion_lineage(trace)
        exclusion_symbols = set(exclusions)
        unexpected_exclusions = sorted(exclusion_symbols - raw_symbols)
        if unexpected_exclusions:
            raise ArtifactGenerationFailedError(
                "candidate exclusions contain symbols absent from alpha_raw",
                context={"package_id": package_id, "symbols": unexpected_exclusions[:20]},
            )
        component_projection = [item.model_dump(mode="json") for item in frozen_program.admitted_package_projection.components]
        common_lineage = {
            "schema_version": "advisory_historical_range_candidate_component_lineage_v1",
            "package_id": frozen_program.package_id,
            "package_version": frozen_program.package_version,
            "manifest_sha256": frozen_program.manifest_sha256,
            "alpha_mode": frozen_program.alpha_mode.value,
            "components": component_projection,
            "raw_signal_semantic_header": dict(raw_artifact.semantic_header),
            "per_leg_window_lineage": _per_leg_window_lineage(raw_artifact),
            "stage_receipt_hashes": {
                stage_name: stage_trace[stage_name]["receipt_hash"]
                for stage_name in ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
            },
            "runtime_profile_hash": runtime_profile_hash,
        }
        facts: list[HistoricalRangeCandidateFactV1] = []
        for symbol in sorted(raw_symbols):
            symbol_lineage = {
                **common_lineage,
                "symbol": symbol,
                "stage_exclusions": exclusions.get(symbol, []),
                "component_scores": _identity_metadata(alpha[symbol].component_scores),
            }
            lineage_hash = canonical_json_sha256(symbol_lineage)
            facts.append(
                HistoricalRangeCandidateFactV1(
                    candidate_id=derive_prefixed_id("ahc", {"day_run_id": day_run_id, "symbol": symbol}),
                    day_run_id=day_run_id,
                    symbol=symbol,
                    membership_status="INCLUDED" if symbol in selected else "EXCLUDED",
                    alpha_raw_rank=alpha[symbol].rank,
                    alpha_raw_score=_decimal(alpha[symbol].score, field="alpha_raw_score", symbol=symbol),
                    hmm_adjusted_rank=hmm[symbol].rank if symbol in hmm else None,
                    hmm_adjusted_score=(
                        _decimal(hmm[symbol].score, field="hmm_adjusted_score", symbol=symbol)
                        if symbol in hmm
                        else None
                    ),
                    risk_policy_adjusted_rank=risk[symbol].rank if symbol in risk else None,
                    risk_policy_adjusted_score=(
                        _decimal(risk[symbol].score, field="risk_policy_adjusted_score", symbol=symbol)
                        if symbol in risk
                        else None
                    ),
                    selection_effective_rank=selected[symbol].rank if symbol in selected else None,
                    selection_effective_score=(
                        _decimal(selected[symbol].score, field="selection_effective_score", symbol=symbol)
                        if symbol in selected
                        else None
                    ),
                    advisory_model_rank=None,
                    advisory_model_score=None,
                    component_lineage_json=symbol_lineage,
                    component_lineage_hash=lineage_hash,
                )
            )
        if set(selected) != {item.symbol for item in facts if item.membership_status == "INCLUDED"}:
            raise ArtifactGenerationFailedError(
                "candidate projection does not close the final selection output",
                context={"package_id": package_id, "day_run_id": day_run_id},
            )
        return tuple(facts), stage_trace


def _stage_trace_payload(trace: SelectionStageTrace) -> dict[str, Any]:
    return {
        "alpha_raw": _receipt_payload(trace.alpha_raw),
        "hmm_adjusted": _receipt_payload(trace.hmm_adjusted),
        "risk_policy_adjusted": _receipt_payload(trace.risk_policy_adjusted),
        "selection_effective": _receipt_payload(trace.selection_effective),
        "metadata": {
            "hmm": _identity_metadata(trace.hmm_metadata),
            "risk": _identity_metadata(trace.risk_metadata),
            "universe": _identity_metadata(trace.universe_metadata),
        },
    }


def _receipt_payload(receipt: StageEvidenceReceipt) -> dict[str, Any]:
    payload = _identity_metadata(receipt.model_dump(mode="json"))
    content_hash = canonical_json_sha256(payload["candidates"])
    semantic_hash = canonical_json_sha256(payload["semantic_payload"])
    payload["receipt_hash"] = canonical_json_sha256(
        {
            "stage": payload["stage"],
            "status": payload["status"],
            "input_count": payload["input_count"],
            "output_count": payload["output_count"],
            "excluded_count": payload["excluded_count"],
            "content_hash": content_hash,
            "semantic_hash": semantic_hash,
            "exclusions": payload["exclusions"],
            "reason_codes": sorted(payload["reason_codes"]),
        }
    )
    payload["content_hash"] = content_hash
    payload["semantic_hash"] = semantic_hash
    return payload


def _candidate_map(rows: Iterable[SelectionCandidate], *, stage: str) -> dict[str, SelectionCandidate]:
    result: dict[str, SelectionCandidate] = {}
    for row in rows:
        if row.symbol in result:
            raise ArtifactGenerationFailedError(
                "candidate stage contains duplicate symbols",
                context={"stage": stage, "symbol": row.symbol},
            )
        result[row.symbol] = row
    return result


def _receipt_candidate_map(receipt: StageEvidenceReceipt, *, stage: str) -> dict[str, SelectionCandidate]:
    rows = [SelectionCandidate.model_validate(item) for item in receipt.candidates]
    return _candidate_map(rows, stage=stage)


def _exclusion_lineage(trace: SelectionStageTrace) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for stage_name, receipt in (
        ("hmm_adjusted", trace.hmm_adjusted),
        ("risk_policy_adjusted", trace.risk_policy_adjusted),
        ("selection_effective", trace.selection_effective),
    ):
        for raw in receipt.exclusions:
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol:
                raise ArtifactGenerationFailedError(
                    "candidate exclusion is missing symbol",
                    context={"stage": stage_name},
                )
            result.setdefault(symbol, []).append(
                {
                    "stage": stage_name,
                    "reason": raw.get("reason"),
                    "source": raw.get("source"),
                    "context": _identity_metadata(raw.get("context") or {}),
                    "rank": raw.get("rank"),
                    "score": raw.get("score"),
                }
            )
    return result


def _per_leg_window_lineage(raw: PreparedRawSelectionArtifactV2) -> Any:
    input_context = (raw.artifact.metadata or {}).get("artifact_input_context")
    if not isinstance(input_context, Mapping):
        return None
    return input_context.get("per_leg_window_lineage") or input_context.get("window_lineage")


def _decimal(value: Any, *, field: str, symbol: str) -> Decimal:
    try:
        number = float(value)
        decimal = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ArtifactGenerationFailedError(
            "candidate stage score is not numeric",
            context={"field": field, "symbol": symbol, "value": value},
        ) from exc
    if not isfinite(number) or not decimal.is_finite():
        raise ArtifactGenerationFailedError(
            "candidate stage score is not finite",
            context={"field": field, "symbol": symbol},
        )
    return decimal


def _identity_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        excluded = {
            "first_observed_at",
            "observed_at",
            "created_at",
            "model_path",
            "coefficients_path",
            "diagnostic_output_path",
            "temporary_workspace",
        }
        return {
            str(key): _identity_metadata(item)
            for key, item in value.items()
            if str(key) not in excluded
            and not str(key).endswith("_local_path")
            and not str(key).endswith("_workspace_path")
        }
    if isinstance(value, (list, tuple)):
        return [_identity_metadata(item) for item in value]
    return value
