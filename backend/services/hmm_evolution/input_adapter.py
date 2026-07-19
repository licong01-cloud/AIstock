"""Prepare frozen Phase 1 inputs from the Phase 0 read-only data source."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Mapping, Protocol, Sequence

import pandas as pd

from backend.services.hmm_data_source import BacktestDataSource

from .candidate_artifact import CandidateArtifactResolver
from .errors import (
    ArtifactHashMismatchError,
    HMMEvolutionError,
    InvalidSpecError,
    SourceUnavailableError,
)
from .evaluator import (
    EVALUATOR_VERSION,
    CandidateCoefficients,
    DateCoveragePlan,
    resolve_batch_common_dates,
)
from .market_repository import (
    MARKET_RETURN_CALCULATOR_VERSION,
    HMMMarketReturnRepository,
    MarketReturnRead,
    MarketWatermark,
    market_return_content_hash,
)
from .models import CandidateRecord, EvaluationPlan, EvaluationSpec
from .source_manifest import SOURCE_MANIFEST_VERSION, build_source_manifest
from .universe import QEExecutionUniverseResolver


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
    market_missing_evidence: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class BatchExecutionInputs:
    """Candidate-specific views over one shared, verified batch input bundle."""

    inputs_by_eval_id: Mapping[str, EvaluationExecutionInputs]
    errors_by_eval_id: Mapping[str, HMMEvolutionError]


class HMMEvaluationInputAdapter:
    """Load each shared input once and freeze durable evaluation identities."""

    def __init__(
        self,
        *,
        candidate_resolver: CandidateArtifactResolver,
        market_repository: HMMMarketReturnRepository | None = None,
        source_factory: Callable[[EvaluationSpec, str], Phase0BacktestSource] | None = None,
        universe_resolver: QEExecutionUniverseResolver | None = None,
    ) -> None:
        self._candidate_resolver = candidate_resolver
        self._market_repository = market_repository or HMMMarketReturnRepository()
        self._source_factory = source_factory or self._default_source_factory
        self._universe_resolver = universe_resolver or QEExecutionUniverseResolver()

    async def prepare_batch(
        self,
        *,
        candidates: Sequence[CandidateRecord],
        evaluation_spec: EvaluationSpec,
        checkpoint: Callable[[str], None] | None = None,
    ) -> PreparedBatchInputs:
        if not 1 <= len(candidates) <= 50:
            raise InvalidSpecError("an HMM evaluation batch must contain 1..50 candidates")
        candidate_ids = [candidate.candidate_id for candidate in candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise InvalidSpecError("an HMM evaluation batch cannot contain duplicate candidates")

        coefficient_inputs: dict[str, CandidateCoefficients] = {}
        for candidate in candidates:
            if checkpoint:
                checkpoint(f"before_candidate_artifact_{candidate.candidate_id}")
            resolved = await self._candidate_resolver.resolve_registered_candidate(candidate)
            coefficient_inputs[candidate.candidate_id] = CandidateCoefficients.from_payload(resolved.payload)
            if checkpoint:
                checkpoint(f"after_candidate_artifact_{candidate.candidate_id}")

        source = self._source_factory(evaluation_spec, "prediction_store_first")
        if checkpoint:
            checkpoint("before_shared_source_inputs")
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
        if checkpoint:
            checkpoint("after_shared_source_inputs")

        if evaluation_spec.schema_version != "hmm_evaluation_spec_v2":
            raise InvalidSpecError(
                "new HMM evaluations must use the source-loop stock pool and ST-PIT universe contract"
            )
        universe_read = await asyncio.to_thread(
            self._universe_resolver.resolve,
            evaluation_spec=evaluation_spec,
            predictions=predictions,
            labels=labels,
        )
        predictions = universe_read.predictions
        labels = universe_read.labels
        if checkpoint:
            checkpoint("after_universe_resolution")

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
            if checkpoint:
                checkpoint("before_market_watermark")
            market_watermark = await asyncio.to_thread(
                self._market_repository.resolve_watermark,
                policy=str(evaluation_spec.as_of["policy"]),
                requested_date=requested_date,
            )
            resolved_as_of_date = market_watermark.resolved_as_of_date
            symbols = sorted({str(item).strip() for item in predictions["symbol"] if str(item).strip()})
            market_read = await asyncio.to_thread(
                self._market_repository.read_forward_returns,
                symbols=symbols,
                trade_dates=date_plan.evaluation_dates,
                horizon_trading_days=int(evaluation_spec.market_forward_return["horizon_trading_days"]),
                as_of_date=resolved_as_of_date,
            )
            market_returns = market_read.returns
            if checkpoint:
                checkpoint("after_market_returns")
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
                universe_evidence=universe_read.evidence,
                warnings=warnings,
            )
            universe = dict(source_manifest["universe"])
            plans.append(
                EvaluationPlan.build(
                    candidate_id=candidate.candidate_id,
                    candidate_manifest_hash=candidate.manifest_hash,
                    source_manifest=source_manifest,
                    evaluation_spec=evaluation_spec,
                    evaluator_version=EVALUATOR_VERSION,
                    resolved_as_of_date=resolved_as_of_date,
                    universe_id=str(universe["universe_id"]),
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

        prepared = await self.load_batch_evaluations(
            evaluations=((evaluation, candidate),),
            candidate_concurrency=1,
            checkpoint=checkpoint,
        )
        eval_id = str(evaluation["eval_id"])
        if eval_id in prepared.errors_by_eval_id:
            raise prepared.errors_by_eval_id[eval_id]
        return prepared.inputs_by_eval_id[eval_id]

    async def load_batch_evaluations(
        self,
        *,
        evaluations: Sequence[tuple[Mapping[str, Any], CandidateRecord]],
        candidate_concurrency: int,
        checkpoint: Callable[[str], None] | None = None,
    ) -> BatchExecutionInputs:
        """Replay shared source/market inputs once for a claimed batch slice."""

        if not evaluations:
            raise InvalidSpecError("batch input replay requires at least one evaluation")
        if not 1 <= candidate_concurrency <= 4:
            raise InvalidSpecError("candidate_concurrency must be between one and four")

        records = [(dict(evaluation), candidate) for evaluation, candidate in evaluations]
        eval_ids = [str(evaluation.get("eval_id") or "").strip() for evaluation, _ in records]
        if any(not eval_id for eval_id in eval_ids) or len(eval_ids) != len(set(eval_ids)):
            raise InvalidSpecError("claimed evaluations must have unique non-empty eval_id values")

        specs = {
            eval_id: EvaluationSpec.model_validate(evaluation["evaluation_spec"])
            for eval_id, (evaluation, _) in zip(eval_ids, records, strict=True)
        }
        first_eval_id = eval_ids[0]
        first_spec = specs[first_eval_id]
        if first_spec.schema_version != "hmm_evaluation_spec_v2":
            raise InvalidSpecError(
                "legacy HMM evaluation v1 records are view-only and cannot be executed or retried"
            )
        first_spec_payload = first_spec.model_dump(mode="json")
        for eval_id, spec in specs.items():
            if spec.model_dump(mode="json") != first_spec_payload:
                raise ArtifactHashMismatchError(
                    "claimed batch evaluations do not share one frozen evaluation spec",
                    context={"first_eval_id": first_eval_id, "mismatched_eval_id": eval_id},
                )

        manifests = {
            eval_id: dict(evaluation["source_manifest"])
            for eval_id, (evaluation, _) in zip(eval_ids, records, strict=True)
        }
        preferences = {_replay_source_preference(manifest) for manifest in manifests.values()}
        if len(preferences) != 1:
            raise ArtifactHashMismatchError(
                "claimed batch evaluations use different frozen artifact sources",
                context={"preferences": sorted(preferences)},
            )

        if checkpoint:
            checkpoint("before_shared_source_inputs")
        source = self._source_factory(first_spec, next(iter(preferences)))
        try:
            async with source:
                predictions = await source.get_predictions(
                    first_spec.window_start,
                    first_spec.window_end,
                )
                labels = await source.get_labels(
                    first_spec.window_start,
                    first_spec.window_end,
                    horizon_days=first_spec.label_horizon_days,
                )
                current_source_info = source.get_artifact_source_info()
        except SourceUnavailableError:
            raise
        except Exception as exc:
            raise SourceUnavailableError(
                "failed to replay frozen Phase 0 prediction and label inputs",
                context={"error_type": type(exc).__name__},
            ) from exc
        if checkpoint:
            checkpoint("after_shared_source_inputs")

        universe_read = await asyncio.to_thread(
            self._universe_resolver.resolve,
            evaluation_spec=first_spec,
            predictions=predictions,
            labels=labels,
        )
        predictions = universe_read.predictions
        labels = universe_read.labels

        evaluation_dates_by_id: dict[str, tuple[date, ...]] = {}
        date_evidence_by_id: dict[str, Mapping[str, Any]] = {}
        for eval_id, manifest in manifests.items():
            _verify_artifact_receipts(manifest, current_source_info, predictions, labels)
            _verify_universe(
                manifest,
                predictions,
                actual_evidence=universe_read.evidence,
            )
            date_coverage = dict(manifest.get("date_coverage") or {})
            try:
                evaluation_dates = tuple(date.fromisoformat(str(item)) for item in date_coverage["evaluation_dates"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ArtifactHashMismatchError(
                    "frozen source manifest has invalid evaluation dates",
                    context={"eval_id": eval_id},
                ) from exc
            if not evaluation_dates:
                raise ArtifactHashMismatchError(
                    "frozen source manifest has no evaluation dates",
                    context={"eval_id": eval_id},
                )
            evaluation_dates_by_id[eval_id] = evaluation_dates
            date_evidence_by_id[eval_id] = date_coverage

        market_returns: pd.DataFrame | None = None
        market_missing_evidence: tuple[Mapping[str, Any], ...] = ()
        market_manifests = {
            eval_id: dict(manifest.get("market_forward_return") or {}) for eval_id, manifest in manifests.items()
        }
        market_modes = {str(market.get("mode") or "") for market in market_manifests.values()}
        if len(market_modes) != 1:
            raise ArtifactHashMismatchError(
                "claimed batch evaluations use different market return modes",
                context={"modes": sorted(market_modes)},
            )
        market_mode = next(iter(market_modes))
        if market_mode == "required":
            identities: set[tuple[date, int, str]] = set()
            for eval_id, market in market_manifests.items():
                try:
                    if manifests[eval_id].get("schema_version") != SOURCE_MANIFEST_VERSION:
                        raise ValueError("legacy source manifest is view-only")
                    calculator_version = str(market["market_return_calculator_version"])
                    if calculator_version != MARKET_RETURN_CALCULATOR_VERSION:
                        raise ValueError("unsupported market return calculator version")
                    identities.add(
                        (
                            date.fromisoformat(str(market["resolved_as_of_date"])),
                            int(market["horizon_trading_days"]),
                            calculator_version,
                        )
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise ArtifactHashMismatchError(
                        "frozen market return manifest is invalid",
                        context={"eval_id": eval_id},
                    ) from exc
            if len(identities) != 1:
                raise ArtifactHashMismatchError("claimed batch evaluations use different market return identities")
            if checkpoint:
                checkpoint("before_shared_market_returns")
            resolved_as_of_date, horizon, _calculator_version = next(iter(identities))
            symbols = sorted({str(item).strip() for item in predictions["symbol"] if str(item).strip()})
            union_dates = tuple(sorted({item for dates in evaluation_dates_by_id.values() for item in dates}))
            market_read = await asyncio.to_thread(
                self._market_repository.read_forward_returns,
                symbols=symbols,
                trade_dates=union_dates,
                horizon_trading_days=horizon,
                as_of_date=resolved_as_of_date,
            )
            market_returns = market_read.returns
            market_missing_evidence = market_read.missing_evidence
            for eval_id, evaluation_dates in evaluation_dates_by_id.items():
                expected_count = int(market_manifests[eval_id].get("return_row_count", -1))
                actual_count = len(market_returns.loc[market_returns["trade_date"].isin(evaluation_dates)])
                if actual_count != expected_count:
                    raise ArtifactHashMismatchError(
                        "market return row count changed since evaluation enqueue",
                        context={
                            "eval_id": eval_id,
                            "expected_row_count": expected_count,
                            "actual_row_count": actual_count,
                        },
                    )
                date_values = {item.isoformat() for item in evaluation_dates}
                actual_missing = tuple(
                    item
                    for item in market_missing_evidence
                    if str(item.get("trade_date") or "") in date_values
                )
                if "missing_return_count" in market_manifests[eval_id]:
                    expected_missing_count = int(
                        market_manifests[eval_id]["missing_return_count"]
                    )
                    expected_reason_counts = dict(
                        market_manifests[eval_id].get("missing_return_reason_counts") or {}
                    )
                    actual_reason_counts = dict(
                        sorted(
                            Counter(
                                str(item.get("reason") or "unknown")
                                for item in actual_missing
                            ).items()
                        )
                    )
                    if (
                        len(actual_missing) != expected_missing_count
                        or actual_reason_counts != expected_reason_counts
                    ):
                        raise ArtifactHashMismatchError(
                            "market return missing-evidence changed since evaluation enqueue",
                            context={
                                "eval_id": eval_id,
                                "expected_missing_count": expected_missing_count,
                                "actual_missing_count": len(actual_missing),
                            },
                        )
                date_mask = market_returns["trade_date"].isin(evaluation_dates)
                actual_content_hash = market_return_content_hash(
                    market_returns.loc[date_mask].copy(),
                    actual_missing,
                )
                expected_content_hash = str(
                    market_manifests[eval_id].get("market_return_content_hash") or ""
                )
                if actual_content_hash != expected_content_hash:
                    raise ArtifactHashMismatchError(
                        "market return values changed since evaluation enqueue",
                        context={
                            "eval_id": eval_id,
                            "expected_content_hash": expected_content_hash,
                            "actual_content_hash": actual_content_hash,
                            "market_return_calculator_version": MARKET_RETURN_CALCULATOR_VERSION,
                        },
                    )
            if checkpoint:
                checkpoint("after_shared_market_returns")
        elif market_mode != "disabled":
            raise ArtifactHashMismatchError("frozen market return mode is invalid")

        semaphore = asyncio.Semaphore(candidate_concurrency)

        async def resolve_coefficients(
            eval_id: str,
            candidate: CandidateRecord,
        ) -> tuple[str, CandidateCoefficients]:
            async with semaphore:
                if checkpoint:
                    checkpoint(f"before_candidate_artifact_{eval_id}")
                resolved = await self._candidate_resolver.resolve_registered_candidate(candidate)
                coefficients = CandidateCoefficients.from_payload(resolved.payload)
                if checkpoint:
                    checkpoint(f"after_candidate_artifact_{eval_id}")
                return eval_id, coefficients

        coefficient_results = await asyncio.gather(
            *(
                resolve_coefficients(eval_id, candidate)
                for eval_id, (_, candidate) in zip(eval_ids, records, strict=True)
            ),
            return_exceptions=True,
        )
        resolved_coefficients: dict[str, CandidateCoefficients] = {}
        coefficient_errors: dict[str, HMMEvolutionError] = {}
        for eval_id, result in zip(eval_ids, coefficient_results, strict=True):
            if isinstance(result, BaseException):
                if isinstance(result, HMMEvolutionError):
                    coefficient_errors[eval_id] = result
                else:
                    coefficient_errors[eval_id] = HMMEvolutionError(
                        "unexpected candidate artifact preparation failure",
                        context={"eval_id": eval_id, "error_type": type(result).__name__},
                    )
                continue
            resolved_eval_id, coefficients = result
            resolved_coefficients[resolved_eval_id] = coefficients
        return BatchExecutionInputs(
            inputs_by_eval_id={
                eval_id: EvaluationExecutionInputs(
                    predictions=predictions,
                    labels=labels,
                    coefficients=resolved_coefficients[eval_id],
                    evaluation_dates=evaluation_dates_by_id[eval_id],
                    date_coverage_evidence=date_evidence_by_id[eval_id],
                    market_returns=market_returns,
                    market_missing_evidence=market_missing_evidence,
                )
                for eval_id in eval_ids
                if eval_id in resolved_coefficients
            },
            errors_by_eval_id=coefficient_errors,
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
            "row_count": int(current.get("row_count") or -1),
            "selected_row_count": len(frame),
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


def _verify_universe(
    source_manifest: Mapping[str, Any],
    predictions: pd.DataFrame,
    *,
    actual_evidence: Mapping[str, Any] | None,
) -> None:
    universe = dict(source_manifest.get("universe") or {})
    if actual_evidence is not None:
        if universe != dict(actual_evidence):
            raise ArtifactHashMismatchError(
                "source-loop stock pool or ST-PIT universe changed since evaluation enqueue",
                context={
                    "expected_universe_hash": universe.get("universe_hash"),
                    "actual_universe_hash": actual_evidence.get("universe_hash"),
                },
            )
        return
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
