"""Prepare frozen Phase 1 inputs from the Phase 0 read-only data source."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Protocol, Sequence

import pandas as pd

from backend.services.hmm_data_source import BacktestDataSource

from .candidate_artifact import CandidateArtifactResolver
from .errors import ArtifactHashMismatchError, InvalidSpecError, SourceUnavailableError
from .evaluator import (
    EVALUATOR_VERSION,
    CandidateCoefficients,
    DateCoveragePlan,
    resolve_batch_common_dates,
)
from .market_repository import HMMMarketReturnRepository, MarketReturnRead, MarketWatermark
from .models import CandidateRecord, EvaluationPlan, EvaluationSpec
from .source_manifest import build_source_manifest


class Phase0BacktestSource(Protocol):
    async def __aenter__(self) -> "Phase0BacktestSource": ...

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None: ...

    async def get_predictions(self, start_date: date, end_date: date) -> pd.DataFrame: ...

    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame: ...

    def get_artifact_source_info(self) -> dict[str, dict[str, Any]]: ...


@dataclass(frozen=True)
class PreparedBatchInputs:
    plans: tuple[EvaluationPlan, ...]
    predictions: pd.DataFrame
    labels: pd.DataFrame
    candidates: Mapping[str, CandidateCoefficients]
    date_plan: DateCoveragePlan
    market_returns: pd.DataFrame | None
    market_watermark: MarketWatermark | None
    market_read: MarketReturnRead | None


@dataclass(frozen=True)
class EvaluationExecutionInputs:
    predictions: pd.DataFrame
    labels: pd.DataFrame
    coefficients: CandidateCoefficients
    evaluation_dates: tuple[date, ...]
    date_coverage_evidence: Mapping[str, Any]
    market_returns: pd.DataFrame | None


class HMMEvaluationInputAdapter:
    """Load each shared input once and freeze durable evaluation identities."""

    def __init__(
        self,
        *,
        candidate_resolver: CandidateArtifactResolver,
        market_repository: HMMMarketReturnRepository | None = None,
        source_factory: Callable[[EvaluationSpec, str], Phase0BacktestSource] | None = None,
    ) -> None:
        self._candidate_resolver = candidate_resolver
        self._market_repository = market_repository or HMMMarketReturnRepository()
        self._source_factory = source_factory or self._default_source_factory

    async def prepare_batch(
        self,
        *,
        candidates: Sequence[CandidateRecord],
        evaluation_spec: EvaluationSpec,
    ) -> PreparedBatchInputs:
        if not 1 <= len(candidates) <= 50:
            raise InvalidSpecError("an HMM evaluation batch must contain 1..50 candidates")
        candidate_ids = [candidate.candidate_id for candidate in candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise InvalidSpecError("an HMM evaluation batch cannot contain duplicate candidates")

        coefficient_inputs: dict[str, CandidateCoefficients] = {}
        for candidate in candidates:
            resolved = await self._candidate_resolver.resolve_registered_candidate(candidate)
            coefficient_inputs[candidate.candidate_id] = CandidateCoefficients.from_payload(
                resolved.payload
            )

        source = self._source_factory(evaluation_spec, "prediction_store_first")
        try:
            async with source:
                predictions = await source.get_predictions(
                    evaluation_spec.window_start,
                    evaluation_spec.window_end,
                )
                labels = await source.get_labels(
                    evaluation_spec.window_start,
                    evaluation_spec.window_end,
                    horizon_days=evaluation_spec.label_horizon_days,
                )
                artifact_source_info = source.get_artifact_source_info()
        except SourceUnavailableError:
            raise
        except Exception as exc:
            raise SourceUnavailableError(
                "failed to load frozen Phase 0 prediction and label inputs",
                context={"error_type": type(exc).__name__},
            ) from exc

        date_plan = resolve_batch_common_dates(
            predictions=predictions,
            labels=labels,
            candidates=coefficient_inputs,
            window_start=evaluation_spec.window_start,
            window_end=evaluation_spec.window_end,
            policy=evaluation_spec.date_coverage_policy,
        )
        market_mode = str(evaluation_spec.market_forward_return["mode"])
        requested_date = _requested_as_of_date(evaluation_spec)
        market_watermark: MarketWatermark | None = None
        market_read: MarketReturnRead | None = None
        market_returns: pd.DataFrame | None = None
        if market_mode == "required":
            market_watermark = self._market_repository.resolve_watermark(
                policy=str(evaluation_spec.as_of["policy"]),
                requested_date=requested_date,
            )
            resolved_as_of_date = market_watermark.resolved_as_of_date
            symbols = sorted({str(item).strip() for item in predictions["symbol"] if str(item).strip()})
            market_read = self._market_repository.read_forward_returns(
                symbols=symbols,
                trade_dates=date_plan.evaluation_dates,
                horizon_trading_days=int(
                    evaluation_spec.market_forward_return["horizon_trading_days"]
                ),
                as_of_date=resolved_as_of_date,
            )
            market_returns = market_read.returns
            market_evidence = {
                "mode": "required",
                "horizon_trading_days": 10,
                **market_watermark.as_manifest_evidence(),
                **market_read.as_manifest_evidence(),
            }
        else:
            resolved_as_of_date = requested_date or max(date_plan.evaluation_dates)
            market_evidence = {
                "mode": "disabled",
                "horizon_trading_days": 10,
                "requested_policy": str(evaluation_spec.as_of["policy"]),
                "requested_date": requested_date.isoformat() if requested_date else None,
                "resolved_as_of_date": resolved_as_of_date.isoformat(),
                "query_executed": False,
            }
        if resolved_as_of_date < evaluation_spec.window_end:
            raise InvalidSpecError(
                "resolved as-of date precedes the requested evaluation window",
                context={
                    "resolved_as_of_date": resolved_as_of_date.isoformat(),
                    "window_end": evaluation_spec.window_end.isoformat(),
                },
            )

        warnings: list[dict[str, Any]] = []
        if date_plan.degraded:
            warnings.append(
                {
                    "code": "hmm_evolution_common_date_intersection",
                    "message": "batch inputs were reduced to their common date intersection",
                    "context": date_plan.as_evidence(),
                }
            )
        plans: list[EvaluationPlan] = []
        for candidate in candidates:
            source_manifest = build_source_manifest(
                base_loop_ref=evaluation_spec.base_loop_ref,
                predictions=predictions,
                labels=labels,
                artifact_source_info=artifact_source_info,
                candidate=candidate,
                date_plan=date_plan,
                label_horizon_days=evaluation_spec.label_horizon_days,
                market_forward_return=market_evidence,
                warnings=warnings,
            )
            universe = source_manifest["universe"]
            plans.append(
                EvaluationPlan.build(
                    candidate_id=candidate.candidate_id,
                    candidate_manifest_hash=candidate.manifest_hash,
                    source_manifest=source_manifest,
                    evaluation_spec=evaluation_spec,
                    evaluator_version=EVALUATOR_VERSION,
                    resolved_as_of_date=resolved_as_of_date,
                    universe_id="prediction_artifact_all",
                    universe_hash=str(universe["universe_hash"]),
                )
            )
        return PreparedBatchInputs(
            plans=tuple(plans),
            predictions=predictions,
            labels=labels,
            candidates=coefficient_inputs,
            date_plan=date_plan,
            market_returns=market_returns,
            market_watermark=market_watermark,
            market_read=market_read,
        )

    async def load_evaluation(
        self,
        *,
        evaluation: Mapping[str, Any],
        candidate: CandidateRecord,
        checkpoint: Callable[[str], None] | None = None,
    ) -> EvaluationExecutionInputs:
        """Replay one durable evaluation from its frozen manifest and spec."""

        spec = EvaluationSpec.model_validate(evaluation["evaluation_spec"])
        source_manifest = dict(evaluation["source_manifest"])
        if checkpoint:
            checkpoint("before_candidate_artifact")
        resolved_candidate = await self._candidate_resolver.resolve_registered_candidate(candidate)
        coefficients = CandidateCoefficients.from_payload(resolved_candidate.payload)
        if checkpoint:
            checkpoint("after_candidate_artifact")

        preference = _replay_source_preference(source_manifest)
        source = self._source_factory(spec, preference)
        try:
            async with source:
                predictions = await source.get_predictions(spec.window_start, spec.window_end)
                labels = await source.get_labels(
                    spec.window_start,
                    spec.window_end,
                    horizon_days=spec.label_horizon_days,
                )
                current_source_info = source.get_artifact_source_info()
        except Exception as exc:
            raise SourceUnavailableError(
                "failed to replay frozen Phase 0 prediction and label inputs",
                context={"error_type": type(exc).__name__},
            ) from exc
        _verify_artifact_receipts(source_manifest, current_source_info, predictions, labels)
        _verify_universe(source_manifest, predictions)
        date_coverage = dict(source_manifest.get("date_coverage") or {})
        try:
            evaluation_dates = tuple(
                date.fromisoformat(str(item)) for item in date_coverage["evaluation_dates"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactHashMismatchError(
                "frozen source manifest has invalid evaluation dates",
                context={"candidate_id": candidate.candidate_id},
            ) from exc
        if not evaluation_dates:
            raise ArtifactHashMismatchError("frozen source manifest has no evaluation dates")
        if checkpoint:
            checkpoint("after_shared_inputs")

        market = dict(source_manifest.get("market_forward_return") or {})
        market_returns: pd.DataFrame | None = None
        if market.get("mode") == "required":
            if checkpoint:
                checkpoint("before_market_returns")
            try:
                resolved_as_of_date = date.fromisoformat(str(market["resolved_as_of_date"]))
                horizon = int(market["horizon_trading_days"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ArtifactHashMismatchError(
                    "frozen market return manifest is invalid",
                    context={"candidate_id": candidate.candidate_id},
                ) from exc
            symbols = sorted(
                {str(item).strip() for item in predictions["symbol"] if str(item).strip()}
            )
            market_read = self._market_repository.read_forward_returns(
                symbols=symbols,
                trade_dates=evaluation_dates,
                horizon_trading_days=horizon,
                as_of_date=resolved_as_of_date,
            )
            if len(market_read.returns) != int(market.get("return_row_count", -1)):
                raise ArtifactHashMismatchError(
                    "market return row count changed since evaluation enqueue",
                    context={
                        "expected_row_count": market.get("return_row_count"),
                        "actual_row_count": len(market_read.returns),
                    },
                )
            market_returns = market_read.returns
            if checkpoint:
                checkpoint("after_market_returns")
        elif market.get("mode") != "disabled":
            raise ArtifactHashMismatchError("frozen market return mode is invalid")
        return EvaluationExecutionInputs(
            predictions=predictions,
            labels=labels,
            coefficients=coefficients,
            evaluation_dates=evaluation_dates,
            date_coverage_evidence=date_coverage,
            market_returns=market_returns,
        )

    @staticmethod
    def _default_source_factory(spec: EvaluationSpec, preference: str) -> Phase0BacktestSource:
        return BacktestDataSource(
            base_loop_ref=spec.base_loop_ref,
            label_horizon_days=spec.label_horizon_days,
            artifact_source_preference=preference,
        )


def _requested_as_of_date(spec: EvaluationSpec) -> date | None:
    raw = spec.as_of.get("requested_date")
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    try:
        return date.fromisoformat(str(raw))
    except ValueError as exc:
        raise InvalidSpecError(
            "as_of.requested_date must be ISO YYYY-MM-DD",
            context={"requested_date": str(raw)},
        ) from exc


def _replay_source_preference(source_manifest: Mapping[str, Any]) -> str:
    artifacts = list(source_manifest.get("artifacts") or [])
    sources = {str(item.get("source")) for item in artifacts if isinstance(item, Mapping)}
    if sources == {"prediction_store"}:
        return "prediction_store_only"
    if sources == {"qe_workspace_cache"}:
        return "workspace_only"
    raise ArtifactHashMismatchError(
        "frozen source manifest contains unsupported mixed artifact sources",
        context={"sources": sorted(sources)},
    )


def _verify_artifact_receipts(
    source_manifest: Mapping[str, Any],
    current_source_info: Mapping[str, Mapping[str, Any]],
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
) -> None:
    frozen = {
        str(item["artifact_name"]): dict(item)
        for item in source_manifest.get("artifacts") or []
        if isinstance(item, Mapping) and item.get("artifact_name")
    }
    for name, frame in (("pred.pkl", predictions), ("label.pkl", labels)):
        expected = frozen.get(name)
        current = current_source_info.get(name)
        if expected is None or current is None:
            raise ArtifactHashMismatchError(
                "artifact receipt is missing during evaluation replay",
                context={"artifact_name": name},
            )
        comparisons = {
            "source": str(current.get("source") or ""),
            "uri": str(current.get("uri") or ""),
            "sha256": str(current.get("sha256") or "").lower(),
            "size_bytes": int(current.get("size_bytes") or -1),
            "row_count": len(frame),
            "zero_copy": bool(current.get("zero_copy", False)),
            "fallback": bool(current.get("fallback", False)),
        }
        mismatches = {
            key: {"expected": expected.get(key), "actual": value}
            for key, value in comparisons.items()
            if expected.get(key) != value
        }
        if mismatches:
            raise ArtifactHashMismatchError(
                "artifact receipt changed since evaluation enqueue",
                context={"artifact_name": name, "mismatches": mismatches},
            )


def _verify_universe(source_manifest: Mapping[str, Any], predictions: pd.DataFrame) -> None:
    universe = dict(source_manifest.get("universe") or {})
    symbols = sorted({str(item).strip() for item in predictions["symbol"] if str(item).strip()})
    from .models import canonical_json_sha256

    actual_hash = canonical_json_sha256(symbols)
    if universe.get("universe_hash") != actual_hash or universe.get("symbol_count") != len(symbols):
        raise ArtifactHashMismatchError(
            "prediction universe changed since evaluation enqueue",
            context={
                "expected_symbol_count": universe.get("symbol_count"),
                "actual_symbol_count": len(symbols),
            },
        )
