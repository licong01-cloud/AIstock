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


PROVIDER_CLOCK_CONTRACT = "selection_provider_clocks_v1"


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
    provider_clock_evidence: Mapping[str, Any]


@dataclass(frozen=True)
class SelectionSignalCompletedDayV1:
    clocks: SelectionSignalDayV1
    semantic_identity: str
    storage_identity: str
    provider_clock_contract: str = PROVIDER_CLOCK_CONTRACT


class SelectionSignalBatchError(RuntimeError):
    def __init__(self, *, cause: Exception, stage: str, day: SelectionSignalDayV1,
                 completed: tuple[SelectionSignalDayResultV1 | SelectionSignalCompletedDayV1, ...]) -> None:
        self.cause = cause
        context = getattr(cause, "context", {})
        context = context if isinstance(context, Mapping) else {}
        self.reason_code = getattr(cause, "reason_code", None) or context.get("reason_code") or type(cause).__name__
        self.failed_stage, self.clocks, self.completed = stage, day, completed
        super().__init__(f"signal batch failed at {stage}: {self.reason_code}; {day.to_dict()}")


class _RiskCutoffProvider:
    """Risk evidence stays at C; this adapter contains no risk policy."""

    def __init__(self, provider: Any, clocks: SelectionSignalDayV1, calls: list):
        self.provider, self.clocks, self.calls = provider, clocks, calls

    def _call(self, method: str, kwargs: dict):
        if kwargs.get("trade_date") != self.clocks.decision_date:
            raise ValueError("native risk read does not match the requested decision date")
        self.calls.append({"capability": "risk_policy", "method": method,
                           "requested_date": self.clocks.decision_date.isoformat(),
                           "provider_date": self.clocks.data_cutoff.isoformat(),
                           "package_id": kwargs.get("package_id")})
        return getattr(self.provider, method)(**{**kwargs, "trade_date": self.clocks.data_cutoff})

    def evaluate(self, **kwargs):
        return self._call("evaluate", kwargs)

    def apply_to_candidates_with_receipt(self, **kwargs):
        result = self._call("apply_to_candidates_with_receipt", kwargs)
        return _with_clock_evidence(result, "risk_metadata", {
            "provider_clock_contract": PROVIDER_CLOCK_CONTRACT,
            "requested_date": self.clocks.decision_date.isoformat(),
            "provider_observation_dates": {"risk_policy": self.clocks.data_cutoff.isoformat()},
        })


class _TradabilityClockProvider:
    """Native suspension D, native industry C, native TopK unchanged."""

    def __init__(self, provider: Any, clocks: SelectionSignalDayV1, calls: list):
        self.provider, self.clocks, self.calls = provider, clocks, calls

    def _call(self, method: str, kwargs: dict):
        if kwargs.get("trade_date") != self.clocks.decision_date:
            raise ValueError("native tradability read does not match the requested decision date")
        filtering = method == "filter_candidates_with_receipt"
        suspension = filtering and kwargs.get("enabled", True)
        industry = filtering and bool(kwargs.get("industry_blacklist"))
        if industry and self.clocks.data_cutoff != self.clocks.decision_date:
            # Optional native contract, never a consumer-side filter or lookup.
            kwargs = {**kwargs, "industry_trade_date": self.clocks.data_cutoff}
        dates = {"suspension": self.clocks.decision_date.isoformat() if suspension else None,
                 "industry": self.clocks.data_cutoff.isoformat() if industry else None}
        self.calls.append({"capability": "tradability", "method": method,
                           "requested_date": self.clocks.decision_date.isoformat(),
                           "provider_date": self.clocks.decision_date.isoformat(),
                           "lookup_dates": dates, "package_id": kwargs.get("package_id")})
        result = getattr(self.provider, method)(**kwargs)
        return _with_clock_evidence(result, "universe_metadata", {
            "provider_clock_contract": PROVIDER_CLOCK_CONTRACT,
            "requested_date": self.clocks.decision_date.isoformat(),
            "provider_observation_dates": dates,
        })

    def filter_candidates_with_receipt(self, **kwargs):
        return self._call("filter_candidates_with_receipt", kwargs)

    def select_top_k_with_receipt(self, **kwargs):
        return self._call("select_top_k_with_receipt", kwargs)


def _with_clock_evidence(result: Any, metadata_field: str, evidence: dict) -> Any:
    """Extend native receipts only; candidate values, counts and reasons stay native."""
    return result.model_copy(update={
        metadata_field: {**getattr(result, metadata_field), **evidence},
        "receipt": result.receipt.model_copy(update={
            "semantic_payload": {**result.receipt.semantic_payload, **evidence}}),
    })


def _observation_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()


def _prepared_clock_evidence(value: Any, clocks: SelectionSignalDayV1, *, hmm_enabled: bool) -> dict:
    """Validate already prepared observations, without loading/generating HMM assets."""
    price_observations = set()
    for row in value.raw.artifact.scores_json:
        component = row.get("component_scores", {})
        price_date = component.get("reference_price_trade_date")
        availability = component.get("reference_price_availability") or {}
        if ((price_date is not None and _observation_date(price_date) != clocks.data_cutoff)
                or (availability.get("as_of_date") is not None
                    and _observation_date(availability["as_of_date"]) != clocks.data_cutoff)
                or (availability.get("quote_date") is not None
                    and _observation_date(availability["quote_date"]) > clocks.data_cutoff)):
            raise ValueError("reference price observation exceeds or differs from requested C")
        if price_date is not None:
            price_observations.add(_observation_date(price_date).isoformat())
        if availability.get("as_of_date") is not None:
            price_observations.add(_observation_date(availability["as_of_date"]).isoformat())
    evidence = {"model_input_cutoff": clocks.data_cutoff.isoformat(),
                "reference_price_limit": clocks.data_cutoff.isoformat(),
                "reference_price_observation_dates": sorted(price_observations),
                "hmm": {"status": "NOT_APPLICABLE"}}
    if not hmm_enabled or not value.prepared_signal.alpha_raw_candidates:
        return evidence
    metadata = value.prepared_signal.hmm_metadata
    inputs = metadata.get("input_data_max_dates")
    if not isinstance(inputs, Mapping) or not inputs:
        raise ValueError("HMM input observation dates are missing for the requested C")
    if (any(_observation_date(v) > clocks.data_cutoff for v in inputs.values())
            or _observation_date(metadata.get("training_information_cutoff")) > clocks.data_cutoff):
        raise ValueError("HMM input or training information exceeds requested C")
    for field in ("snapshot_trained_at", "available_at"):
        if metadata.get(field) is not None and _observation_date(metadata[field]) > clocks.decision_date:
            raise ValueError("HMM asset is not available by requested D")
    as_of = _observation_date(metadata.get("as_of_trade_date"))
    effective = _observation_date(metadata.get("effective_trade_date"))
    # The native frozen policy owns validity/freshness. Do not invent a new
    # minimum-date gate or relabel a legal older snapshot as C/D.
    if (as_of > clocks.decision_date or effective > clocks.target_trade_date
            or (metadata.get("coefficient_trade_date") is not None
                and _observation_date(metadata["coefficient_trade_date"]) != as_of)):
        raise ValueError("HMM native observation/effective dates differ from requested C/D/T")
    evidence["hmm"] = {"status": "OBSERVED", "as_of_trade_date": as_of.isoformat(),
                       "effective_trade_date": effective.isoformat(),
                       "input_data_max_dates": dict(inputs),
                       "training_information_cutoff": metadata["training_information_cutoff"]}
    return evidence


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
                clock_evidence = {
                    "contract_version": PROVIDER_CLOCK_CONTRACT,
                    "clocks": day.to_dict(),
                    "prepared": {p: _prepared_clock_evidence(value, day, hmm_enabled=profiles[p].hmm.enabled)
                                 for p, value in prepared.items()},
                    "provider_calls": [],
                }
                stage = "native_selection_computation"
                providers = replace(
                    self._providers, risk_policy=_RiskCutoffProvider(self._providers.risk_policy, day, clock_evidence["provider_calls"]),
                    tradability=_TradabilityClockProvider(self._providers.tradability, day, clock_evidence["provider_calls"]))
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
                            "provider_clock_evidence": clock_evidence,
                            "package_order": request.ordered_package_ids, "mode": request.selection_mode,
                            "top_k": dict(request.package_top_k), "weights": request.package_weights,
                            "runtime_profiles": {p: selection_runtime_profile_sha256(profiles[p]) for p in profiles},
                            "native_stages": {p: {stage: getattr(trace, stage).receipt_hash for stage in (
                                "alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")}
                                for p, trace in computation.stage_trace_by_package.items()},
                            "headers": {p: dict(value.raw.semantic_header) for p, value in prepared.items()}}
                semantic_hash = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
                storage_hash = hashlib.sha256(json.dumps(
                    [request.consumer_scope, request.run_id, day.to_dict(), semantic_hash], sort_keys=True).encode()).hexdigest()
                result = SelectionSignalDayResultV1(
                    consumer_scope=request.consumer_scope, run_id=request.run_id, clocks=day,
                    semantic_identity=semantic_hash, storage_identity=storage_hash,
                    raw_scores=scores, input_contexts=contexts, unscored_members=unscored,
                    membership_status=membership_status, computation=computation,
                    provider_clock_evidence=deepcopy(clock_evidence),
                )
                completed.append(SelectionSignalCompletedDayV1(day, semantic_hash, storage_hash))
                yield result
            except Exception as exc:
                raise SelectionSignalBatchError(cause=exc, stage=stage, day=day, completed=tuple(completed)) from exc
