"""Shared signal orchestration, not another stock-selection algorithm.

No result discovery, repository, account, Advisory model, DB, or worker control
is allowed here. Existing native preparation/computation providers are injected;
both consumers execute them independently. Pure envelopes import on Python 3.10.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime
import hashlib
import json
from typing import Any


@dataclass(frozen=True)
class SelectionSignalDayV1:
    decision_date: date
    data_cutoff: date
    target_trade_date: date

    def __post_init__(self) -> None:
        if any(not isinstance(value, date) or isinstance(value, datetime)
               for value in (self.decision_date, self.data_cutoff, self.target_trade_date)):
            raise ValueError("C/D/T must be explicit calendar dates")
        if self.data_cutoff > self.decision_date or self.target_trade_date < self.decision_date:
            raise ValueError("invalid C/D/T order; no date is corrected or omitted")

    def to_dict(self) -> dict[str, str]:
        return {name: getattr(self, name).isoformat()
                for name in ("decision_date", "data_cutoff", "target_trade_date")}


@dataclass(frozen=True)
class SelectionSignalRequestV1:
    consumer_scope: str
    run_id: str
    date_plan: tuple[SelectionSignalDayV1, ...]
    ordered_package_ids: tuple[str, ...]
    manifest_hashes: Mapping[str, str]
    runtime_configs: Mapping[str, Mapping[str, Any]]
    package_top_k: Mapping[str, int]
    source_identity: str
    selection_mode: str = "single_package"
    package_weights: Mapping[str, float] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "date_plan", tuple(self.date_plan))
        object.__setattr__(self, "ordered_package_ids", tuple(self.ordered_package_ids))
        if self.consumer_scope not in {"ADVISORY", "LOCALSIM"} or not self.run_id or not self.source_identity:
            raise ValueError("signal request requires explicit consumer/run/source identity")
        if not self.date_plan or any(left.decision_date >= right.decision_date
                                     for left, right in zip(self.date_plan, self.date_plan[1:])):
            raise ValueError("signal date plan must be nonempty, unique and increasing")
        packages = set(self.ordered_package_ids)
        if not packages or len(packages) != len(self.ordered_package_ids) or any(not x for x in packages):
            raise ValueError("package order must contain unique nonempty identities")
        for field in (self.manifest_hashes, self.runtime_configs, self.package_top_k):
            if set(field) != packages:
                raise ValueError("signal package metadata must cover the exact requested identities")
        if any(isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= 50 for k in self.package_top_k.values()):
            raise ValueError("TopK must preserve the native 1..50 contract")
        # Never retain the caller's mutable dictionaries or share another run's
        # configs/results. The native computation validates policy/weight hashes.
        for name in ("manifest_hashes", "runtime_configs", "package_top_k", "package_weights"):
            object.__setattr__(self, name, deepcopy(getattr(self, name)))


@dataclass(frozen=True)
class SelectionSignalDayResultV1:
    consumer_scope: str
    run_id: str
    clocks: SelectionSignalDayV1
    semantic_identity: str
    storage_identity: str
    raw_scores: Mapping[str, tuple[Mapping[str, Any], ...]]
    input_contexts: Mapping[str, Mapping[str, Any]]
    unscored_members: Mapping[str, tuple[Mapping[str, str], ...]]
    membership_status: Mapping[str, str]
    computation: Any


@dataclass(frozen=True)
class SelectionSignalCompletedDayV1:
    clocks: SelectionSignalDayV1
    semantic_identity: str
    storage_identity: str


class SelectionSignalBatchError(RuntimeError):
    def __init__(self, *, cause: Exception, stage: str, day: SelectionSignalDayV1,
                 completed: tuple[SelectionSignalDayResultV1 | SelectionSignalCompletedDayV1, ...]) -> None:
        self.cause = cause
        context = getattr(cause, "context", {})
        context = context if isinstance(context, Mapping) else {}
        self.reason_code = getattr(cause, "reason_code", None) or context.get("reason_code") or type(cause).__name__
        self.failed_stage, self.clocks, self.completed = stage, day, completed
        super().__init__(f"signal batch failed at {stage}: {self.reason_code}; {day.to_dict()}")


class _CutoffBoundProvider:
    """Bind the native provider's data date without changing its policy.

    Native computation labels the artifact D; its read-only risk/tradability
    adapters must observe C, not read D when the explicit request says C<D.
    Legacy C=D calls preserve the exact original provider invocation.
    """
    def __init__(self, provider: Any, clocks: SelectionSignalDayV1):
        self.provider, self.clocks = provider, clocks

    def __getattr__(self, name: str):
        method = getattr(self.provider, name)
        def bound(**kwargs):
            if kwargs.get("trade_date") != self.clocks.decision_date:
                raise ValueError("native policy read does not match the requested decision date")
            return method(**{**kwargs, "trade_date": self.clocks.data_cutoff})
        return bound


class SelectionSignalKernel:
    def __init__(self, *, computation: Any, providers: Any) -> None:
        if computation is None or providers is None:
            raise ValueError("signal kernel requires explicit native read-only computation providers")
        self._computation, self._providers = computation, providers

    def compute_prepared(self, *, request: Any, prepared_signals: Mapping[str, Any]) -> Any:
        """Native same-type compatibility entry; only ONE policy/sort algorithm."""
        return self._computation.compute(request=request, prepared_signals=prepared_signals, providers=self._providers)

    def run(self, *, request: SelectionSignalRequestV1,
            prepare_day: Callable[..., Any]) -> tuple[SelectionSignalDayResultV1, ...]:
        """Materializing convenience API; long runs use iter_results instead."""
        results = []
        try:
            for result in self.iter_results(request=request, prepare_day=prepare_day):
                results.append(result)
        except SelectionSignalBatchError as exc:
            exc.completed = tuple(results)
            raise
        return tuple(results)

    def iter_results(self, *, request: SelectionSignalRequestV1, prepare_day: Callable[..., Any]):
        """Yield full daily results; retain only small completed identity summaries.

        The consumer's own writer must publish/checkpoint them. Computation
        completion is not a declaration of business publication or accounting.
        """
        from backend.services.strategy_package.selection_computation import (
            SelectionMode, StrategyPackageSelectionComputationRequestV1,
            parse_selection_runtime_profile_for_computation, selection_runtime_profile_sha256,
        )

        request = replace(request)  # Freeze a run-local copy against caller mutation.
        completed: list[SelectionSignalCompletedDayV1] = []
        for day in request.date_plan:
            stage = "raw_preparation"
            try:
                prepared = {package_id: prepare_day(
                    package_id=package_id, clocks=day,
                    runtime_config=deepcopy(request.runtime_configs[package_id]),
                    consumer_scope=request.consumer_scope, run_id=request.run_id,
                ) for package_id in request.ordered_package_ids}
                for package_id, value in prepared.items():
                    artifact = value.raw.artifact
                    if (artifact.package_id != package_id or artifact.manifest_sha256 != request.manifest_hashes[package_id]
                            or artifact.trade_date != day.decision_date or artifact.data_source != "DB_HISTORICAL"):
                        raise ValueError("native prepared signal package/date/source identity mismatch")
                    context = artifact.metadata.get("artifact_input_context")
                    if not isinstance(context, dict) or str(context.get("effective_trade_date"))[:10] != day.data_cutoff.isoformat():
                        raise ValueError("native prepared signal cutoff identity mismatch")
                profiles = {p: parse_selection_runtime_profile_for_computation(request.runtime_configs[p])
                            for p in request.ordered_package_ids}
                stage = "native_selection_computation"
                providers = self._providers if day.data_cutoff == day.decision_date else replace(
                    self._providers, risk_policy=_CutoffBoundProvider(self._providers.risk_policy, day),
                    tradability=_CutoffBoundProvider(self._providers.tradability, day))
                computation = self._computation.compute(
                    request=StrategyPackageSelectionComputationRequestV1(
                        trade_date=day.decision_date, data_source="DB_HISTORICAL",
                        selection_mode=SelectionMode(request.selection_mode),
                        ordered_package_ids=request.ordered_package_ids,
                        package_runtime_profiles=profiles,
                        package_runtime_profile_hashes={p: selection_runtime_profile_sha256(profiles[p]) for p in profiles},
                        package_top_k=request.package_top_k, package_weights=request.package_weights,
                        exhaustive_selection_evidence=True,
                    ), prepared_signals={p: value.prepared_signal for p, value in prepared.items()}, providers=providers,
                )
                contexts = {p: deepcopy(value.raw.artifact.metadata["artifact_input_context"]) for p, value in prepared.items()}
                scores = {p: tuple(deepcopy(value.raw.artifact.scores_json)) for p, value in prepared.items()}
                unscored, membership_status = {}, {}
                for p, context in contexts.items():
                    # Do not invent native reasons when a source does not publish
                    # its complete original membership list.
                    metadata = prepared[p].raw.artifact.metadata
                    members = metadata.get("universe_members", metadata.get("provider_metadata", {}).get("universe_members", context.get("universe_members")))
                    scored = {row["symbol"] for row in scores[p]}
                    if members is not None and (not isinstance(members, list) or len(members) != len(set(members))
                                                or len(members) != prepared[p].raw.artifact.universe_count or not scored.issubset(members)):
                        raise ValueError("native published membership is incomplete or conflicts with scores")
                    membership_status[p] = "COMPLETE" if members is not None else "NOT_PUBLISHED"
                    unscored[p] = tuple({"symbol": str(member), "availability": "UNKNOWN"}
                                        for member in members if member not in scored) if isinstance(members, list) else ()
                identity = {"clocks": day.to_dict(), "source": request.source_identity,
                            "package_order": request.ordered_package_ids, "mode": request.selection_mode,
                            "top_k": dict(request.package_top_k), "weights": request.package_weights,
                            "runtime_profiles": {p: selection_runtime_profile_sha256(profiles[p]) for p in profiles},
                            "native_stages": {p: {stage: getattr(trace, stage).semantic_hash for stage in (
                                "alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")}
                                for p, trace in computation.stage_trace_by_package.items()},
                            "headers": {p: dict(value.raw.semantic_header) for p, value in prepared.items()}}
                semantic_hash = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
                storage_hash = hashlib.sha256(json.dumps([request.consumer_scope, request.run_id, day.to_dict()], sort_keys=True).encode()).hexdigest()
                result = SelectionSignalDayResultV1(
                    consumer_scope=request.consumer_scope, run_id=request.run_id, clocks=day,
                    semantic_identity=semantic_hash, storage_identity=storage_hash,
                    raw_scores=scores, input_contexts=contexts, unscored_members=unscored,
                    membership_status=membership_status, computation=computation,
                )
                completed.append(SelectionSignalCompletedDayV1(day, semantic_hash, storage_hash))
                yield result
            except Exception as exc:
                raise SelectionSignalBatchError(cause=exc, stage=stage, day=day, completed=tuple(completed)) from exc
