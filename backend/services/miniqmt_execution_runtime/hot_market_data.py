"""Runtime actor for process-local hot market-data routing and bounded retry."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import RLock
from typing import Any, Callable

from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataTargetV1,
    HotMarketDataViewV1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise TypeError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _safe_exception_message(exc: BaseException) -> str:
    try:
        rendered = str(exc)
    except Exception as rendering_error:
        return f"<unrenderable:{type(rendering_error).__name__}>"
    return rendered[:512]


class HotMarketDataDispositionV1(StrEnum):
    NO_TARGET = "NO_TARGET"
    NO_EFFECT = "NO_EFFECT"
    EFFECT_COMMITTED = "EFFECT_COMMITTED"
    EFFECT_PENDING = "EFFECT_PENDING"
    STALE = "STALE"
    DUPLICATE = "DUPLICATE"


class HotMarketDataIngressError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = context
        super().__init__(message)


class HotMarketDataEffectTerminalError(RuntimeError):
    """An economic effect reached a durable terminal failure and must not retry."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = context
        super().__init__(message)


class HotMarketDataEffectRetryableError(RuntimeError):
    """An explicit allowlisted transient before a durable economic commit."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = context
        super().__init__(message)


@dataclass(frozen=True)
class HotMarketDataIngressReceiptV1:
    disposition: HotMarketDataDispositionV1
    runtime_id: str
    symbol: str
    evaluated_target_count: int
    committed_effect_count: int
    pending_effect_count: int


@dataclass
class _PendingHotMarketEffectV1:
    effect: HotMarketDataEconomicEffectV1
    target: HotMarketDataTargetV1
    failure_count: int
    next_retry_at_utc: datetime


@dataclass
class HotMarketDataIngressV1:
    runtime_id: str
    effect_committer: Callable[[HotMarketDataEconomicEffectV1], Any]
    _targets_by_symbol: dict[str, tuple[HotMarketDataTargetV1, ...]] = field(default_factory=dict, init=False)
    _registered_algo_ids: set[str] = field(default_factory=set, init=False)
    _generation_by_symbol: dict[str, int] = field(default_factory=dict, init=False)
    _last_sequence_by_symbol: dict[str, int] = field(default_factory=dict, init=False)
    _pending_by_algo: dict[str, _PendingHotMarketEffectV1] = field(default_factory=dict, init=False)
    _inflight_by_algo: dict[str, HotMarketDataEconomicEffectV1] = field(default_factory=dict, init=False)
    _isolated_row_version_by_algo: dict[str, int] = field(default_factory=dict, init=False)
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.runtime_id) is not str or not self.runtime_id or self.runtime_id != self.runtime_id.strip():
            raise TypeError("runtime_id must be a canonical identity")
        if not callable(self.effect_committer):
            raise TypeError("effect_committer must be callable")

    def replace_targets_v1(self, targets: tuple[HotMarketDataTargetV1, ...]) -> None:
        if type(targets) is not tuple:
            raise TypeError("targets must be an exact tuple")
        by_symbol: dict[str, list[HotMarketDataTargetV1]] = {}
        identities: set[str] = set()
        for target in targets:
            if (
                getattr(target, "runtime_id", None) != self.runtime_id
                or type(getattr(target, "algo_instance_id", None)) is not str
                or not target.algo_instance_id
                or target.algo_instance_id != target.algo_instance_id.strip()
                or type(getattr(target, "symbol", None)) is not str
                or not target.symbol
                or target.symbol != target.symbol.strip()
                or not callable(getattr(target, "evaluate_hot_market_data_v1", None))
                or not callable(getattr(target, "accept_committed_effect_v1", None))
            ):
                raise TypeError("hot target does not expose the exact process-local contract")
            if target.algo_instance_id in identities:
                raise ValueError("hot target contains duplicate algo identity")
            identities.add(target.algo_instance_id)
            by_symbol.setdefault(target.symbol, []).append(target)
        with self._lock:
            if self._pending_by_algo or self._inflight_by_algo:
                raise HotMarketDataIngressError(
                    "MINIQMT_HOT_MARKET_TARGET_REPLACEMENT_PENDING_EFFECT",
                    "hot target generation cannot change while an economic effect is uncommitted",
                    context={
                        "runtime_id": self.runtime_id,
                        "pending_algo_ids": sorted(self._pending_by_algo),
                        "inflight_algo_ids": sorted(self._inflight_by_algo),
                    },
                )
            filtered: dict[str, list[HotMarketDataTargetV1]] = {}
            for symbol, items in by_symbol.items():
                for target in items:
                    isolated_row_version = self._isolated_row_version_by_algo.get(target.algo_instance_id)
                    target_row_version = getattr(getattr(target, "algo", None), "row_version", None)
                    if isolated_row_version is not None:
                        if type(target_row_version) is int and target_row_version > isolated_row_version:
                            self._isolated_row_version_by_algo.pop(target.algo_instance_id, None)
                        else:
                            continue
                    filtered.setdefault(symbol, []).append(target)
            frozen = {
                symbol: tuple(sorted(items, key=lambda item: item.algo_instance_id))
                for symbol, items in filtered.items()
            }
            self._registered_algo_ids = identities
            self._targets_by_symbol = frozen

    def target_algo_instance_ids_v1(self) -> tuple[str, ...]:
        """Return only currently eligible process-local target identities."""

        with self._lock:
            return tuple(
                sorted(target.algo_instance_id for targets in self._targets_by_symbol.values() for target in targets)
            )

    def registered_algo_instance_ids_v1(self) -> tuple[str, ...]:
        """Return durable target owners, including a row-version-isolated predecessor."""

        with self._lock:
            return tuple(sorted(self._registered_algo_ids))

    def has_uncommitted_effects_v1(self) -> bool:
        """Expose whether target refresh must wait without touching the repository."""

        with self._lock:
            return bool(self._pending_by_algo or self._inflight_by_algo)

    def _isolate_target_locked_v1(
        self,
        *,
        symbol: str,
        algo_instance_id: str,
        expected_algo_row_version: int,
    ) -> None:
        self._isolated_row_version_by_algo[algo_instance_id] = expected_algo_row_version
        targets = self._targets_by_symbol.get(symbol, ())
        retained = tuple(target for target in targets if target.algo_instance_id != algo_instance_id)
        if retained:
            self._targets_by_symbol[symbol] = retained
        else:
            self._targets_by_symbol.pop(symbol, None)

    def _uncommitted_count_locked_v1(self) -> int:
        return len(set(self._pending_by_algo) | set(self._inflight_by_algo))

    def ingest_v1(self, view: HotMarketDataViewV1) -> HotMarketDataIngressReceiptV1:
        if not isinstance(view, HotMarketDataViewV1):
            raise TypeError("view must be HotMarketDataViewV1")
        if view.runtime_id != self.runtime_id:
            raise HotMarketDataIngressError(
                "MINIQMT_HOT_MARKET_RUNTIME_OWNER_CONFLICT",
                "hot market view crosses runtime owner",
                context={"expected_runtime_id": self.runtime_id, "actual_runtime_id": view.runtime_id},
            )
        reservations: list[tuple[HotMarketDataTargetV1, HotMarketDataEconomicEffectV1]] = []
        with self._lock:
            generation = self._generation_by_symbol.get(view.symbol)
            if generation is not None and view.generation < generation:
                return HotMarketDataIngressReceiptV1(
                    HotMarketDataDispositionV1.STALE,
                    self.runtime_id,
                    view.symbol,
                    0,
                    0,
                    self._uncommitted_count_locked_v1(),
                )
            if generation is None or view.generation > generation:
                self._generation_by_symbol[view.symbol] = view.generation
                self._last_sequence_by_symbol[view.symbol] = view.sequence
                previous = None
            else:
                previous = self._last_sequence_by_symbol.get(view.symbol)
            if previous is not None and view.sequence <= previous:
                disposition = (
                    HotMarketDataDispositionV1.DUPLICATE
                    if view.sequence == previous
                    else HotMarketDataDispositionV1.STALE
                )
                return HotMarketDataIngressReceiptV1(
                    disposition,
                    self.runtime_id,
                    view.symbol,
                    0,
                    0,
                    self._uncommitted_count_locked_v1(),
                )
            self._last_sequence_by_symbol[view.symbol] = view.sequence
            targets = self._targets_by_symbol.get(view.symbol, ())
            if not targets:
                return HotMarketDataIngressReceiptV1(
                    HotMarketDataDispositionV1.NO_TARGET,
                    self.runtime_id,
                    view.symbol,
                    0,
                    0,
                    self._uncommitted_count_locked_v1(),
                )
            effects = 0
            for target in targets:
                if (
                    target.algo_instance_id in self._pending_by_algo
                    or target.algo_instance_id in self._inflight_by_algo
                ):
                    effects += 1
                    continue
                effect = target.evaluate_hot_market_data_v1(view)
                if effect is None:
                    continue
                effects += 1
                if effect.runtime_id != self.runtime_id or effect.algo_instance_id != target.algo_instance_id:
                    raise HotMarketDataIngressError(
                        "MINIQMT_HOT_MARKET_EFFECT_OWNER_CONFLICT",
                        "hot economic effect differs from its target owner",
                        context={"runtime_id": self.runtime_id, "algo_instance_id": target.algo_instance_id},
                    )
                reservations.append((target, effect))
            for target, effect in reservations:
                self._inflight_by_algo[target.algo_instance_id] = effect

        committed = 0
        for index, (target, effect) in enumerate(reservations):
            try:
                readback = self.effect_committer(effect)
            except Exception as exc:
                with self._lock:
                    self._inflight_by_algo.pop(target.algo_instance_id, None)
                    for later_target, _later_effect in reservations[index + 1 :]:
                        self._inflight_by_algo.pop(later_target.algo_instance_id, None)
                    if isinstance(exc, HotMarketDataEffectTerminalError):
                        self._isolate_target_locked_v1(
                            symbol=view.symbol,
                            algo_instance_id=target.algo_instance_id,
                            expected_algo_row_version=effect.expected_algo_row_version,
                        )
                    elif isinstance(exc, HotMarketDataEffectRetryableError):
                        self._pending_by_algo[target.algo_instance_id] = _PendingHotMarketEffectV1(
                            effect=effect,
                            target=target,
                            failure_count=1,
                            next_retry_at_utc=view.observed_at_utc + timedelta(seconds=1),
                        )
                    else:
                        self._isolate_target_locked_v1(
                            symbol=view.symbol,
                            algo_instance_id=target.algo_instance_id,
                            expected_algo_row_version=effect.expected_algo_row_version,
                        )
                if isinstance(exc, HotMarketDataEffectTerminalError):
                    raise HotMarketDataIngressError(
                        exc.reason_code,
                        "hot economic effect closed as a durable terminal failure",
                        context={
                            **exc.context,
                            "runtime_id": self.runtime_id,
                            "algo_instance_id": target.algo_instance_id,
                            "effect_identity": effect.effect_identity,
                            "broker_called": False,
                            "retryable": False,
                        },
                    ) from exc
                if isinstance(exc, HotMarketDataEffectRetryableError):
                    raise HotMarketDataIngressError(
                        "MINIQMT_HOT_MARKET_EFFECT_COMMIT_FAILED",
                        "hot economic effect did not commit; broker remains uncalled",
                        context={
                            **exc.context,
                            "runtime_id": self.runtime_id,
                            "algo_instance_id": target.algo_instance_id,
                            "effect_identity": effect.effect_identity,
                            "broker_called": False,
                            "retryable": True,
                            "exception_type": type(exc).__name__,
                            "exception_message": _safe_exception_message(exc),
                        },
                    ) from exc
                raise HotMarketDataIngressError(
                    "MINIQMT_HOT_MARKET_EFFECT_COMMIT_NON_RETRYABLE",
                    "hot economic effect commit failed outside the transient retry allowlist",
                    context={
                        "runtime_id": self.runtime_id,
                        "algo_instance_id": target.algo_instance_id,
                        "effect_identity": effect.effect_identity,
                        "broker_called": False,
                        "retryable": False,
                        "exception_type": type(exc).__name__,
                        "exception_message": _safe_exception_message(exc),
                    },
                ) from exc
            try:
                target.accept_committed_effect_v1(effect, readback)
            except Exception as exc:
                with self._lock:
                    self._inflight_by_algo.pop(target.algo_instance_id, None)
                    self._isolate_target_locked_v1(
                        symbol=view.symbol,
                        algo_instance_id=target.algo_instance_id,
                        expected_algo_row_version=effect.expected_algo_row_version,
                    )
                    for later_target, _later_effect in reservations[index + 1 :]:
                        self._inflight_by_algo.pop(later_target.algo_instance_id, None)
                raise HotMarketDataIngressError(
                    "MINIQMT_HOT_MARKET_EFFECT_COMMIT_ACK_INVALID",
                    "committed hot economic effect failed exact successor acknowledgement",
                    context={
                        "runtime_id": self.runtime_id,
                        "algo_instance_id": target.algo_instance_id,
                        "effect_identity": effect.effect_identity,
                        "broker_called": False,
                        "retryable": False,
                        "exception_type": type(exc).__name__,
                        "exception_message": _safe_exception_message(exc),
                    },
                ) from exc
            with self._lock:
                self._inflight_by_algo.pop(target.algo_instance_id, None)
            committed += 1
        disposition = (
            HotMarketDataDispositionV1.NO_EFFECT
            if effects == 0
            else HotMarketDataDispositionV1.EFFECT_COMMITTED
            if committed == effects
            else HotMarketDataDispositionV1.EFFECT_PENDING
        )
        with self._lock:
            pending_count = self._uncommitted_count_locked_v1()
        return HotMarketDataIngressReceiptV1(
            disposition,
            self.runtime_id,
            view.symbol,
            len(targets),
            committed,
            pending_count,
        )

    def retry_pending_v1(self, *, observed_at_utc: datetime) -> HotMarketDataIngressReceiptV1:
        """Retry pending effects only from scheduler cadence, never Tick cadence."""

        observed = _aware_utc(observed_at_utc, field_name="observed_at_utc")
        committed = 0
        failures: list[dict[str, object]] = []
        with self._lock:
            due_algo_instance_ids = tuple(
                algo_instance_id
                for algo_instance_id in sorted(self._pending_by_algo)
                if observed >= self._pending_by_algo[algo_instance_id].next_retry_at_utc
            )
        for algo_instance_id in due_algo_instance_ids:
            with self._lock:
                pending = self._pending_by_algo.pop(algo_instance_id, None)
                if pending is None or algo_instance_id in self._inflight_by_algo:
                    continue
                self._inflight_by_algo[algo_instance_id] = pending.effect
            try:
                readback = self.effect_committer(pending.effect)
            except Exception as exc:
                with self._lock:
                    self._inflight_by_algo.pop(algo_instance_id, None)
                    if isinstance(exc, HotMarketDataEffectTerminalError):
                        self._isolate_target_locked_v1(
                            symbol=pending.target.symbol,
                            algo_instance_id=algo_instance_id,
                            expected_algo_row_version=pending.effect.expected_algo_row_version,
                        )
                    elif isinstance(exc, HotMarketDataEffectRetryableError):
                        pending.failure_count += 1
                        delay_seconds = min(60, 2 ** min(pending.failure_count - 1, 6))
                        pending.next_retry_at_utc = observed + timedelta(seconds=delay_seconds)
                        self._pending_by_algo[algo_instance_id] = pending
                    else:
                        self._isolate_target_locked_v1(
                            symbol=pending.target.symbol,
                            algo_instance_id=algo_instance_id,
                            expected_algo_row_version=pending.effect.expected_algo_row_version,
                        )
                if isinstance(exc, HotMarketDataEffectTerminalError):
                    raise HotMarketDataIngressError(
                        exc.reason_code,
                        "pending hot economic effect closed as a durable terminal failure",
                        context={
                            **exc.context,
                            "runtime_id": self.runtime_id,
                            "algo_instance_id": algo_instance_id,
                            "effect_identity": pending.effect.effect_identity,
                            "broker_called": False,
                            "retryable": False,
                        },
                    ) from exc
                if not isinstance(exc, HotMarketDataEffectRetryableError):
                    raise HotMarketDataIngressError(
                        "MINIQMT_HOT_MARKET_EFFECT_COMMIT_NON_RETRYABLE",
                        "pending hot economic effect failed outside the transient retry allowlist",
                        context={
                            "runtime_id": self.runtime_id,
                            "algo_instance_id": algo_instance_id,
                            "effect_identity": pending.effect.effect_identity,
                            "broker_called": False,
                            "retryable": False,
                            "exception_type": type(exc).__name__,
                            "exception_message": _safe_exception_message(exc),
                        },
                    ) from exc
                failures.append(
                    {
                        "algo_instance_id": algo_instance_id,
                        "effect_identity": pending.effect.effect_identity,
                        "failure_count": pending.failure_count,
                        "next_retry_at_utc": pending.next_retry_at_utc.isoformat().replace("+00:00", "Z"),
                        "exception_type": type(exc).__name__,
                        "exception_message": _safe_exception_message(exc),
                    }
                )
                continue
            try:
                pending.target.accept_committed_effect_v1(pending.effect, readback)
            except Exception as exc:
                with self._lock:
                    self._inflight_by_algo.pop(algo_instance_id, None)
                    self._isolate_target_locked_v1(
                        symbol=pending.target.symbol,
                        algo_instance_id=algo_instance_id,
                        expected_algo_row_version=pending.effect.expected_algo_row_version,
                    )
                raise HotMarketDataIngressError(
                    "MINIQMT_HOT_MARKET_EFFECT_COMMIT_ACK_INVALID",
                    "committed pending hot economic effect failed exact successor acknowledgement",
                    context={
                        "runtime_id": self.runtime_id,
                        "algo_instance_id": algo_instance_id,
                        "effect_identity": pending.effect.effect_identity,
                        "broker_called": False,
                        "retryable": False,
                        "exception_type": type(exc).__name__,
                        "exception_message": _safe_exception_message(exc),
                    },
                ) from exc
            with self._lock:
                self._inflight_by_algo.pop(algo_instance_id, None)
            committed += 1
        if failures:
            retained_failures = failures[:64]
            omitted_failures = failures[64:]
            raise HotMarketDataIngressError(
                "MINIQMT_HOT_MARKET_EFFECT_RETRY_FAILED",
                "one or more pending economic effects remain uncommitted after bounded scheduler retry",
                context={
                    "runtime_id": self.runtime_id,
                    "broker_called": False,
                    "failures": retained_failures,
                    "failure_count": len(failures),
                    "failures_truncated": bool(omitted_failures),
                    "omitted_failure_count": len(omitted_failures),
                    "omitted_failure_set_sha256": (
                        hash_hex_v1("miniqmt_hot_market_omitted_failure_set_v1", omitted_failures)
                        if omitted_failures
                        else None
                    ),
                },
            )
        with self._lock:
            pending_count = self._uncommitted_count_locked_v1()
        return HotMarketDataIngressReceiptV1(
            HotMarketDataDispositionV1.EFFECT_COMMITTED
            if committed
            else HotMarketDataDispositionV1.EFFECT_PENDING
            if pending_count
            else HotMarketDataDispositionV1.NO_EFFECT,
            self.runtime_id,
            "*",
            0,
            committed,
            pending_count,
        )


__all__ = [
    "HotMarketDataEffectRetryableError",
    "HotMarketDataEffectTerminalError",
    "HotMarketDataDispositionV1",
    "HotMarketDataIngressError",
    "HotMarketDataIngressReceiptV1",
    "HotMarketDataIngressV1",
]
