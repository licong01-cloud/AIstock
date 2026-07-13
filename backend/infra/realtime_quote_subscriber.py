"""Realtime quote subscription helpers for xtquant."""

from __future__ import annotations

import logging
import threading
import time
import uuid
import weakref
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Callable, Dict, List, Mapping, Optional

from backend.execution_algos.adaptive_is.contracts import QuoteSourceMethod
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode, quote_contract_error

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PhaseOneQuoteDelivery:
    """One generation-bound delivery to a logical Phase 1 quote consumer.

    ``payload`` deliberately remains an opaque callback value here.  The
    consumer must capture it immediately into ``RawQuoteFrame`` before it can
    cross into any queue or worker.  Keeping the hand-off at this small
    boundary makes the legacy callback registry and Phase 1 lease registry
    independently evolvable.
    """

    data_session_key: str
    lease_id: str
    owner: str
    consumer_id: str
    symbol: str
    payload: Any
    generation: int
    ingress_sequence: int
    source_method: QuoteSourceMethod
    received_at_utc: datetime
    received_monotonic_ns: int


@dataclass(frozen=True)
class PhaseOneLeaseCallbacks:
    """Lifecycle callbacks owned by one logical Phase 1 quote consumer."""

    on_quote: Callable[[PhaseOneQuoteDelivery], bool]
    on_generation_prepared: Callable[[str, int], bool]
    on_generation_published: Callable[[str, int], None]
    on_generation_fenced: Callable[[str, int], None]
    on_loud_failure: Callable[[QuoteContractError], None]


@dataclass(frozen=True)
class PhaseOneQuoteLease:
    """Independent logical ownership over a shared physical quote feed."""

    lease_id: str
    data_session_key: str
    owner: str
    consumer_id: str
    symbols: tuple[str, ...]
    generation: int
    status: str
    physical_subscription_id: int | None


@dataclass
class _PhaseOneLeaseState:
    lease: PhaseOneQuoteLease
    callbacks: PhaseOneLeaseCallbacks


@dataclass
class _PhaseOnePhysicalFeed:
    data_session_key: str
    owner: str
    generation: int
    symbols: tuple[str, ...]
    bootstrap_fetcher: Callable[[List[str]], Mapping[str, Mapping[str, Any]]]
    physical_subscription_id: int | None = None
    leases: dict[str, _PhaseOneLeaseState] = field(default_factory=dict)
    pending_leases: dict[str, _PhaseOneLeaseState] = field(default_factory=dict)
    next_callback_sequence: int = 1
    fenced: bool = False
    bootstrap_covered_symbols: tuple[str, ...] = ()
    bootstrap_coverage_ratio: float = 0.0
    callback_total: int = 0

    def all_states_for_symbol(self, symbol: str) -> tuple[_PhaseOneLeaseState, ...]:
        return tuple(
            state
            for state in (*self.leases.values(), *self.pending_leases.values())
            if symbol in state.lease.symbols
        )

try:
    import xtquant.xtdata as xtdata  # type: ignore[import-not-found]

    XTDATA_AVAILABLE = True
except ImportError:
    xtdata = None  # type: ignore[assignment]
    XTDATA_AVAILABLE = False
    logger.warning("xtquant.xtdata is not available; realtime quote subscription is disabled")


def _load_xtdata():
    """Load xtdata lazily after callers have configured xtquant paths."""

    global xtdata, XTDATA_AVAILABLE
    if XTDATA_AVAILABLE and xtdata is not None:
        return xtdata
    try:
        import xtquant.xtdata as xtdata_mod  # type: ignore[import-not-found]
    except ImportError:
        XTDATA_AVAILABLE = False
        return None
    xtdata = xtdata_mod
    XTDATA_AVAILABLE = True
    return xtdata_mod


class RealtimeQuoteSubscriber:
    """Process-local whole-quote subscription manager."""

    _phase_one_process_owner_lock = threading.RLock()
    _phase_one_process_owner_by_session: dict[str, tuple[weakref.ReferenceType["RealtimeQuoteSubscriber"], str]] = {}
    _phase_one_process_failure_by_session: dict[str, dict[str, Any]] = {}

    def __init__(self):
        self.subscriptions: Dict[int, List[str]] = {}  # seq -> stocks
        self.callbacks: Dict[str, List[Callable]] = {}  # stock -> callbacks
        self.managed_subscriptions: Dict[str, Dict] = {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        # Phase 1 quote-ingress ownership is intentionally separate from the
        # historical ``subscriptions/callbacks/managed_subscriptions`` maps.
        # LEGACY_B0 continues to use those original maps and health keys.
        self._phase_one_active: dict[str, _PhaseOnePhysicalFeed] = {}
        self._phase_one_preparing: dict[tuple[str, int], _PhaseOnePhysicalFeed] = {}
        self._phase_one_owner_by_session: dict[str, str] = {}
        self._phase_one_failure_samples: dict[str, dict[str, Any]] = {}
        self._phase_one_last_failure: dict[str, Any] | None = None
        self._phase_one_capacity_rejected_total = 0
        self._phase_one_operation_locks: dict[str, threading.RLock] = {}

    def _phase_one_operation_lock(self, data_session_key: str) -> threading.RLock:
        with self._lock:
            return self._phase_one_operation_locks.setdefault(data_session_key, threading.RLock())

    def subscribe(self, stocks: List[str], callback: Callable) -> Optional[int]:
        """Subscribe to realtime quotes and return the xtdata sequence id."""

        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            logger.error("xtquant.xtdata is not available; cannot subscribe realtime quotes")
            return None

        try:
            seq = xtdata_mod.subscribe_whole_quote(
                code_list=stocks,
                callback=self._on_quote,
            )

            if seq > 0:
                with self._lock:
                    self.subscriptions[seq] = stocks
                    for stock in stocks:
                        if stock not in self.callbacks:
                            self.callbacks[stock] = []
                        self.callbacks[stock].append(callback)
                logger.info("subscribed realtime quotes: stocks=%s seq=%s", stocks, seq)
                return seq
            logger.error("realtime quote subscription failed: stocks=%s seq=%s", stocks, seq)
            return None

        except Exception as e:  # noqa: BLE001
            logger.error("realtime quote subscription raised: %s", e, exc_info=True)
            return None

    def ensure_subscription(
        self,
        *,
        key: str,
        stocks: List[str],
        callback: Callable,
        force: bool = False,
    ) -> Dict:
        """Ensure one managed whole-quote subscription is active.

        This path is used by MiniQMT pre-trade quote reads and is intentionally
        loud: subscribe failures are surfaced instead of silently using stale
        xtdata cache rows.
        """

        normalized = list(dict.fromkeys(str(stock or "").strip() for stock in stocks if str(stock or "").strip()))
        if not normalized:
            raise RuntimeError("MINIQMT_QUOTE_SUBSCRIPTION_SYMBOLS_EMPTY: no stocks requested")
        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            raise RuntimeError("MINIQMT_QUOTE_SUBSCRIPTION_UNAVAILABLE: xtquant.xtdata is not available")
        self.start()
        requested = set(normalized)
        target_stocks = list(normalized)
        with self._lock:
            existing = self.managed_subscriptions.get(key)
            existing_stocks = [str(stock) for stock in (existing or {}).get("stocks") or []]
            if existing_stocks:
                target_stocks = list(dict.fromkeys([*existing_stocks, *normalized]))
            if (
                not force
                and existing
                and int(existing.get("seq") or 0) in self.subscriptions
                and requested.issubset(set(existing_stocks))
            ):
                return {
                    **existing,
                    "status": "active",
                    "forced": False,
                    "requested_stocks": normalized,
                    "subscription_reused": True,
                }
            old_seq = int(existing.get("seq") or 0) if existing else None
        if old_seq:
            self.unsubscribe(old_seq)
        seq = self.subscribe(target_stocks, callback)
        if not isinstance(seq, int) or seq <= 0:
            raise RuntimeError(
                "MINIQMT_QUOTE_SUBSCRIPTION_FAILED: xtdata.subscribe_whole_quote did not return a positive seq"
            )
        payload = {
            "key": key,
            "seq": seq,
            "stocks": target_stocks,
            "status": "active",
            "forced": bool(force),
            "requested_stocks": normalized,
            "subscription_reused": False,
            "subscribed_at": datetime.now(UTC).isoformat(),
        }
        with self._lock:
            self.managed_subscriptions[key] = dict(payload)
        return payload

    # ------------------------------------------------------------------
    # Phase 1 logical lease registry.  This deliberately does not reuse the
    # legacy ``callbacks`` map: legacy unsubscribe removes every callback for
    # a symbol and therefore cannot provide independent consumer ownership.
    # ------------------------------------------------------------------

    def acquire_phase_one_lease(
        self,
        *,
        data_session_key: str,
        owner: str,
        consumer_id: str,
        symbols: List[str],
        callbacks: PhaseOneLeaseCallbacks,
        bootstrap_fetcher: Callable[[List[str]], Mapping[str, Mapping[str, Any]]],
        max_symbols: int,
    ) -> PhaseOneQuoteLease:
        """Acquire one independent consumer lease without touching LEGACY_B0.

        The physical feed is replaced only after its successor has a positive
        xtdata id and complete bootstrap coverage.  A failed replacement leaves
        the old physical feed and its active leases intact.
        """

        with self._phase_one_operation_lock(data_session_key):
            return self._acquire_phase_one_lease(
                data_session_key=data_session_key,
                owner=owner,
                consumer_id=consumer_id,
                symbols=symbols,
                callbacks=callbacks,
                bootstrap_fetcher=bootstrap_fetcher,
                max_symbols=max_symbols,
            )

    def _acquire_phase_one_lease(
        self,
        *,
        data_session_key: str,
        owner: str,
        consumer_id: str,
        symbols: List[str],
        callbacks: PhaseOneLeaseCallbacks,
        bootstrap_fetcher: Callable[[List[str]], Mapping[str, Mapping[str, Any]]],
        max_symbols: int,
    ) -> PhaseOneQuoteLease:
        normalized = self._normalize_phase_one_symbols(symbols)
        self._validate_phase_one_request(
            data_session_key=data_session_key,
            owner=owner,
            consumer_id=consumer_id,
            callbacks=callbacks,
            bootstrap_fetcher=bootstrap_fetcher,
            max_symbols=max_symbols,
        )
        lease_id = uuid.uuid4().hex
        with self._lock:
            self._assert_phase_one_owner_locked(data_session_key, owner)
            active = self._phase_one_active.get(data_session_key)
            if active is not None and set(normalized).issubset(set(active.symbols)):
                pending = _PhaseOneLeaseState(
                    lease=PhaseOneQuoteLease(
                        lease_id=lease_id,
                        data_session_key=data_session_key,
                        owner=owner,
                        consumer_id=consumer_id,
                        symbols=normalized,
                        generation=active.generation,
                        status="PREPARING",
                        physical_subscription_id=active.physical_subscription_id,
                    ),
                    callbacks=callbacks,
                )
                active.pending_leases[lease_id] = pending
                self._phase_one_owner_by_session[data_session_key] = owner
                same_physical_feed = True
            else:
                current_states = tuple(active.leases.values()) if active is not None else ()
                candidate_states = (*current_states,)
                desired_symbols = self._union_phase_one_symbols(
                    *(state.lease.symbols for state in candidate_states),
                    normalized,
                )
                self._assert_phase_one_capacity(desired_symbols, max_symbols=max_symbols, data_session_key=data_session_key)
                generation = (active.generation + 1) if active is not None else 1
                pending = _PhaseOneLeaseState(
                    lease=PhaseOneQuoteLease(
                        lease_id=lease_id,
                        data_session_key=data_session_key,
                        owner=owner,
                        consumer_id=consumer_id,
                        symbols=normalized,
                        generation=generation,
                        status="PREPARING",
                        physical_subscription_id=None,
                    ),
                    callbacks=callbacks,
                )
                prepared_states = (*candidate_states, pending)
                same_physical_feed = False
            self._claim_phase_one_process_owner_locked(data_session_key, owner)

        if same_physical_feed:
            try:
                self._bootstrap_phase_one_states(active, (pending,))
            except QuoteContractError as error:
                with self._lock:
                    active.pending_leases.pop(lease_id, None)
                    if not active.leases:
                        self._phase_one_owner_by_session.pop(data_session_key, None)
                self._emit_phase_one_failure((pending,), error)
                raise
            if not self._safe_generation_prepared(pending, data_session_key, active.generation):
                with self._lock:
                    active.pending_leases.pop(lease_id, None)
                raise quote_contract_error(
                    QuoteContractReasonCode.CONSUMER_FAILURE,
                    "Phase 1 quote consumer did not acknowledge generation publication",
                    context={"data_session_key": data_session_key, "generation": active.generation, "lease_id": lease_id},
                )
            with self._lock:
                active.pending_leases.pop(lease_id, None)
                pending.lease = replace(pending.lease, status="ACTIVE")
                active.leases[lease_id] = pending
            if not self._safe_generation_published(pending, data_session_key, active.generation):
                with self._lock:
                    active.leases.pop(lease_id, None)
                self._safe_generation_fenced(pending, data_session_key, active.generation)
                raise quote_contract_error(
                    QuoteContractReasonCode.CONSUMER_FAILURE,
                    "Phase 1 quote consumer failed while activating an existing generation",
                    context={"data_session_key": data_session_key, "generation": active.generation, "lease_id": lease_id},
                )
            return pending.lease

        return self._replace_phase_one_feed(
            data_session_key=data_session_key,
            owner=owner,
            old_feed=active,
            candidate_states=prepared_states,
            desired_symbols=desired_symbols,
            bootstrap_fetcher=bootstrap_fetcher,
        ).leases[lease_id].lease

    def rebuild_phase_one_leases(self, *, data_session_key: str, owner: str, max_symbols: int) -> int:
        """Create a fenced successor generation for all current logical leases."""

        with self._phase_one_operation_lock(data_session_key):
            return self._rebuild_phase_one_leases(
                data_session_key=data_session_key,
                owner=owner,
                max_symbols=max_symbols,
            )

    def _rebuild_phase_one_leases(self, *, data_session_key: str, owner: str, max_symbols: int) -> int:

        with self._lock:
            self._assert_phase_one_owner_locked(data_session_key, owner)
            active = self._phase_one_active.get(data_session_key)
            if active is None or not active.leases:
                raise quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "cannot rebuild Phase 1 quote leases without an active physical feed",
                    context={"data_session_key": data_session_key},
                )
            candidate_states = tuple(active.leases.values())
            desired_symbols = active.symbols
            self._assert_phase_one_capacity(desired_symbols, max_symbols=max_symbols, data_session_key=data_session_key)
        replacement = self._replace_phase_one_feed(
            data_session_key=data_session_key,
            owner=owner,
            old_feed=active,
            candidate_states=candidate_states,
            desired_symbols=desired_symbols,
            bootstrap_fetcher=active.bootstrap_fetcher,
        )
        return replacement.generation

    def release_phase_one_lease(self, *, data_session_key: str, lease_id: str, max_symbols: int) -> bool:
        """Release only one logical consumer; other consumers remain active."""

        with self._phase_one_operation_lock(data_session_key):
            return self._release_phase_one_lease(
                data_session_key=data_session_key,
                lease_id=lease_id,
                max_symbols=max_symbols,
            )

    def _release_phase_one_lease(self, *, data_session_key: str, lease_id: str, max_symbols: int) -> bool:

        with self._lock:
            active = self._phase_one_active.get(data_session_key)
            if active is None:
                return False
            pending = active.pending_leases.get(lease_id)
            if pending is not None:
                should_fence_only = True
                released = pending
                remaining_states: tuple[_PhaseOneLeaseState, ...] = tuple(active.leases.values())
            else:
                released = active.leases.get(lease_id)
                if released is None:
                    return False
                remaining_states = tuple(
                    state for active_lease_id, state in active.leases.items() if active_lease_id != lease_id
                )
                should_fence_only = bool(remaining_states) and self._union_phase_one_symbols(
                    *(state.lease.symbols for state in remaining_states)
                ) == active.symbols
            if not remaining_states:
                self._phase_one_active.pop(data_session_key, None)
                self._phase_one_owner_by_session.pop(data_session_key, None)
                active.fenced = True
                physical_to_unsubscribe = active.physical_subscription_id
            else:
                physical_to_unsubscribe = None

        if not remaining_states:
            self._safe_generation_fenced(released, data_session_key, active.generation)
            self._release_phase_one_process_owner_if_idle(data_session_key)
            self._unsubscribe_phase_one_physical(
                physical_to_unsubscribe,
                data_session_key=data_session_key,
                generation=active.generation,
                states=remaining_states,
            )
            return True
        if should_fence_only:
            with self._lock:
                active.pending_leases.pop(lease_id, None)
                active.leases.pop(lease_id, None)
            self._safe_generation_fenced(released, data_session_key, active.generation)
            return True

        desired_symbols = self._union_phase_one_symbols(*(state.lease.symbols for state in remaining_states))
        self._assert_phase_one_capacity(desired_symbols, max_symbols=max_symbols, data_session_key=data_session_key)
        self._replace_phase_one_feed(
            data_session_key=data_session_key,
            owner=active.owner,
            old_feed=active,
            candidate_states=remaining_states,
            desired_symbols=desired_symbols,
            bootstrap_fetcher=active.bootstrap_fetcher,
        )
        return True

    def phase_one_health(self, *, data_session_key: str) -> dict[str, Any]:
        """Return Phase 1-only diagnostics without exposing legacy health state."""

        with self._lock:
            with self._phase_one_process_owner_lock:
                process_last_failure = self._phase_one_process_failure_by_session.get(data_session_key)
            active = self._phase_one_active.get(data_session_key)
            preparing = [
                feed
                for (session_key, _generation), feed in self._phase_one_preparing.items()
                if session_key == data_session_key
            ]
            if active is None:
                return {
                    "data_session_key": data_session_key,
                    "status": "PREPARING" if preparing else "INACTIVE",
                    "owner": self._phase_one_owner_by_session.get(data_session_key),
                    "generation": preparing[-1].generation if preparing else None,
                    "physical_subscription_id": preparing[-1].physical_subscription_id if preparing else None,
                    "symbols": list(preparing[-1].symbols) if preparing else [],
                    "lease_count": len(preparing[-1].leases) + len(preparing[-1].pending_leases) if preparing else 0,
                    "bootstrap_covered_symbols": list(preparing[-1].bootstrap_covered_symbols) if preparing else [],
                    "bootstrap_coverage_ratio": preparing[-1].bootstrap_coverage_ratio if preparing else 0.0,
                    "callback_total": preparing[-1].callback_total if preparing else 0,
                    "capacity_rejected_total": self._phase_one_capacity_rejected_total,
                    "last_failure": self._phase_one_last_failure,
                    "process_last_failure": process_last_failure,
                }
            failed_lease_count = sum(state.lease.status == "FAILED" for state in active.leases.values())
            return {
                "data_session_key": data_session_key,
                "status": "DEGRADED" if failed_lease_count else "ACTIVE",
                "owner": active.owner,
                "generation": active.generation,
                "physical_subscription_id": active.physical_subscription_id,
                "symbols": list(active.symbols),
                "lease_count": len(active.leases),
                "pending_lease_count": len(active.pending_leases),
                "failed_lease_count": failed_lease_count,
                "preparing_generation": preparing[-1].generation if preparing else None,
                "bootstrap_covered_symbols": list(active.bootstrap_covered_symbols),
                "bootstrap_coverage_ratio": active.bootstrap_coverage_ratio,
                "callback_total": active.callback_total,
                "capacity_rejected_total": self._phase_one_capacity_rejected_total,
                "last_failure": self._phase_one_last_failure,
                "process_last_failure": process_last_failure,
            }

    def get_phase_one_lease(self, *, data_session_key: str, lease_id: str) -> PhaseOneQuoteLease | None:
        """Return the current immutable lease snapshot for health/reporting callers."""

        with self._lock:
            active = self._phase_one_active.get(data_session_key)
            if active is not None:
                state = active.leases.get(lease_id) or active.pending_leases.get(lease_id)
                if state is not None:
                    return state.lease
            for (session_key, _generation), feed in self._phase_one_preparing.items():
                if session_key == data_session_key and lease_id in feed.leases:
                    return feed.leases[lease_id].lease
        return None

    def shutdown_phase_one_leases(self, *, data_session_key: str | None = None) -> None:
        """Fence Phase 1 callbacks before unsubscribing physical feeds."""

        if data_session_key is not None:
            with self._phase_one_operation_lock(data_session_key):
                self._shutdown_phase_one_leases(data_session_key=data_session_key)
            return
        with self._lock:
            session_keys = sorted(
                set(self._phase_one_active) | {session_key for session_key, _ in self._phase_one_preparing}
            )
        for session_key in session_keys:
            with self._phase_one_operation_lock(session_key):
                self._shutdown_phase_one_leases(data_session_key=session_key)

    def _shutdown_phase_one_leases(self, *, data_session_key: str) -> None:

        with self._lock:
            session_keys = [data_session_key]
            active_feeds = [self._phase_one_active.pop(key) for key in session_keys if key in self._phase_one_active]
            preparing_keys = [
                key for key in self._phase_one_preparing if key[0] == data_session_key
            ]
            preparing_feeds = [self._phase_one_preparing.pop(key) for key in preparing_keys]
            for feed in (*active_feeds, *preparing_feeds):
                feed.fenced = True
                self._phase_one_owner_by_session.pop(feed.data_session_key, None)
            released_session_keys = {feed.data_session_key for feed in (*active_feeds, *preparing_feeds)}
        for session_key in released_session_keys:
            self._release_phase_one_process_owner_if_idle(session_key)
        for feed in (*active_feeds, *preparing_feeds):
            states = tuple((*feed.leases.values(), *feed.pending_leases.values()))
            for state in states:
                self._safe_generation_fenced(state, feed.data_session_key, feed.generation)
            self._unsubscribe_phase_one_physical(
                feed.physical_subscription_id,
                data_session_key=feed.data_session_key,
                generation=feed.generation,
                states=states,
            )

    def _replace_phase_one_feed(
        self,
        *,
        data_session_key: str,
        owner: str,
        old_feed: _PhaseOnePhysicalFeed | None,
        candidate_states: tuple[_PhaseOneLeaseState, ...],
        desired_symbols: tuple[str, ...],
        bootstrap_fetcher: Callable[[List[str]], Mapping[str, Mapping[str, Any]]],
    ) -> _PhaseOnePhysicalFeed:
        generation = (old_feed.generation + 1) if old_feed is not None else 1
        preparing_states = tuple(
            _PhaseOneLeaseState(
                lease=replace(
                    state.lease,
                    generation=generation,
                    status="PREPARING",
                    physical_subscription_id=None,
                ),
                callbacks=state.callbacks,
            )
            for state in candidate_states
        )
        preparing = _PhaseOnePhysicalFeed(
            data_session_key=data_session_key,
            owner=owner,
            generation=generation,
            symbols=desired_symbols,
            bootstrap_fetcher=bootstrap_fetcher,
            leases={state.lease.lease_id: state for state in preparing_states},
        )
        with self._lock:
            self._assert_phase_one_owner_locked(data_session_key, owner)
            if (data_session_key, generation) in self._phase_one_preparing:
                raise quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "a Phase 1 quote lease rebuild is already preparing",
                    context={"data_session_key": data_session_key, "generation": generation},
                )
            self._phase_one_preparing[(data_session_key, generation)] = preparing
            self._phase_one_owner_by_session[data_session_key] = owner
        try:
            xtdata_mod = _load_xtdata()
            if xtdata_mod is None:
                raise quote_contract_error(
                    QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE,
                    "xtquant.xtdata is not available for Phase 1 quote ingress",
                    context={"data_session_key": data_session_key, "generation": generation},
                )
            self.start()
            sequence = xtdata_mod.subscribe_whole_quote(
                code_list=list(desired_symbols),
                callback=lambda datas, bound_session=data_session_key, bound_generation=generation: self._on_phase_one_quote(
                    bound_session,
                    bound_generation,
                    datas,
                ),
            )
            if not isinstance(sequence, int) or sequence <= 0:
                raise quote_contract_error(
                    QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE,
                    "xtdata.subscribe_whole_quote did not return a positive subscription id",
                    context={"data_session_key": data_session_key, "generation": generation, "subscription_id": sequence},
                )
            preparing.physical_subscription_id = sequence
            self._bootstrap_phase_one_states(preparing, tuple(preparing.leases.values()))
        except QuoteContractError as error:
            self._discard_preparing_phase_one_feed(preparing)
            self._emit_phase_one_failure(tuple(preparing.leases.values()), error)
            raise
        except Exception as exc:  # noqa: BLE001 - xtdata failures must be converted to a stable loud failure
            error = quote_contract_error(
                QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                "Phase 1 quote lease rebuild raised unexpectedly",
                context={
                    "data_session_key": data_session_key,
                    "generation": generation,
                    "exception_type": type(exc).__name__,
                },
            )
            self._discard_preparing_phase_one_feed(preparing)
            self._emit_phase_one_failure(tuple(preparing.leases.values()), error, exc_info=True)
            raise error from exc

        preparation_failed = [
            state
            for state in preparing.leases.values()
            if not self._safe_generation_prepared(state, data_session_key, generation)
        ]
        if preparation_failed:
            self._discard_preparing_phase_one_feed(preparing)
            raise quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote generation preparation was not acknowledged by every consumer",
                context={
                    "data_session_key": data_session_key,
                    "generation": generation,
                    "failed_lease_ids": [state.lease.lease_id for state in preparation_failed],
                },
            )

        with self._lock:
            ownership_lost = self._phase_one_preparing.get((data_session_key, generation)) is not preparing
            if ownership_lost:
                error = quote_contract_error(
                    QuoteContractReasonCode.LEASE_REBUILD_FAILED,
                    "Phase 1 quote lease rebuild lost its preparation ownership",
                    context={"data_session_key": data_session_key, "generation": generation},
                )
            else:
                error = None
            if not ownership_lost:
                for state in preparing.leases.values():
                    state.lease = replace(
                        state.lease,
                        status="ACTIVE",
                        physical_subscription_id=preparing.physical_subscription_id,
                    )
                self._phase_one_active[data_session_key] = preparing
                self._phase_one_preparing.pop((data_session_key, generation), None)
        if ownership_lost:
            self._discard_preparing_phase_one_feed(preparing)
            self._emit_phase_one_failure(tuple(preparing.leases.values()), error)
            raise error
        for state in preparing.leases.values():
            if not self._safe_generation_published(state, data_session_key, generation):
                with self._lock:
                    state.lease = replace(state.lease, status="FAILED")
        if old_feed is not None:
            old_feed.fenced = True
            for state in old_feed.leases.values():
                self._safe_generation_fenced(state, data_session_key, old_feed.generation)
            self._unsubscribe_phase_one_physical(
                old_feed.physical_subscription_id,
                data_session_key=data_session_key,
                generation=old_feed.generation,
                states=tuple(old_feed.leases.values()),
            )
        return preparing

    def _bootstrap_phase_one_states(
        self,
        feed: _PhaseOnePhysicalFeed,
        states: tuple[_PhaseOneLeaseState, ...],
    ) -> None:
        try:
            snapshots = feed.bootstrap_fetcher(list(feed.symbols))
        except QuoteContractError:
            raise
        except Exception as exc:  # noqa: BLE001 - broker read must surface a typed bootstrap failure
            raise quote_contract_error(
                QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
                "get_full_tick bootstrap raised",
                context={
                    "data_session_key": feed.data_session_key,
                    "generation": feed.generation,
                    "exception_type": type(exc).__name__,
                },
            ) from exc
        if not isinstance(snapshots, Mapping):
            raise quote_contract_error(
                QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
                "get_full_tick bootstrap must return a mapping keyed by symbol",
                context={"data_session_key": feed.data_session_key, "generation": feed.generation},
            )
        missing_symbols = [symbol for symbol in feed.symbols if not isinstance(snapshots.get(symbol), Mapping)]
        if missing_symbols:
            raise quote_contract_error(
                QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
                "get_full_tick bootstrap did not cover every desired active symbol",
                context={
                    "data_session_key": feed.data_session_key,
                    "generation": feed.generation,
                    "missing_symbols": missing_symbols,
                    "desired_symbols": list(feed.symbols),
                },
            )
        for symbol in feed.symbols:
            captured_at = datetime.now(UTC)
            captured_monotonic_ns = time.monotonic_ns()
            acknowledged_callbacks: set[Callable[[PhaseOneQuoteDelivery], bool]] = set()
            for state in states:
                if symbol not in state.lease.symbols:
                    continue
                if state.callbacks.on_quote in acknowledged_callbacks:
                    continue
                delivery = PhaseOneQuoteDelivery(
                    data_session_key=feed.data_session_key,
                    lease_id=state.lease.lease_id,
                    owner=state.lease.owner,
                    consumer_id=state.lease.consumer_id,
                    symbol=symbol,
                    payload=snapshots[symbol],
                    generation=feed.generation,
                    # Bootstrap is deliberately lower than every callback in
                    # the generation, so a pre-publication callback cannot be
                    # overwritten by a later bootstrap snapshot.
                    ingress_sequence=0,
                    source_method=QuoteSourceMethod.BOOTSTRAP_FULL_TICK,
                    received_at_utc=captured_at,
                    received_monotonic_ns=captured_monotonic_ns,
                )
                if not self._safe_phase_one_quote_callback(state, delivery):
                    raise quote_contract_error(
                        QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
                        "get_full_tick bootstrap frame was rejected by a Phase 1 quote consumer",
                        context={
                            "data_session_key": feed.data_session_key,
                            "generation": feed.generation,
                            "lease_id": state.lease.lease_id,
                            "consumer_id": state.lease.consumer_id,
                            "symbol": symbol,
                        },
                    )
                acknowledged_callbacks.add(state.callbacks.on_quote)
        with self._lock:
            feed.bootstrap_covered_symbols = tuple(feed.symbols)
            feed.bootstrap_coverage_ratio = 1.0

    def _on_phase_one_quote(self, data_session_key: str, generation: int, datas: Any) -> None:
        """Dispatch one immutable-capture opportunity; never throw to xtdata."""

        captured_at = datetime.now(UTC)
        captured_monotonic_ns = time.monotonic_ns()
        if not isinstance(datas, Mapping):
            error = quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "xtdata whole-quote callback payload must be a mapping",
                context={"data_session_key": data_session_key, "generation": generation, "payload_type": type(datas).__name__},
            )
            self._emit_phase_one_failure((), error)
            return
        with self._lock:
            active = self._phase_one_active.get(data_session_key)
            feed = active if active is not None and active.generation == generation else self._phase_one_preparing.get((data_session_key, generation))
            if feed is None or feed.fenced:
                stale_error = quote_contract_error(
                    QuoteContractReasonCode.ORDERING_REJECTED,
                    "received a fenced or stale Phase 1 quote callback generation",
                    context={
                        "event": "STALE_GENERATION",
                        "data_session_key": data_session_key,
                        "generation": generation,
                    },
                )
                self._emit_phase_one_failure((), stale_error)
                return
            deliveries: list[tuple[_PhaseOneLeaseState, PhaseOneQuoteDelivery]] = []
            scheduled_callbacks: set[tuple[str, Callable[[PhaseOneQuoteDelivery], bool]]] = set()
            for raw_symbol, payload in datas.items():
                feed.callback_total += 1
                symbol = str(raw_symbol or "").strip()
                states = feed.all_states_for_symbol(symbol)
                if not states:
                    unexpected_error = quote_contract_error(
                        QuoteContractReasonCode.UNEXPECTED_SYMBOL,
                        "xtdata whole-quote callback contained a symbol without an admitted lease",
                        context={
                            "data_session_key": data_session_key,
                            "generation": generation,
                            "symbol": symbol,
                        },
                    )
                    self._emit_phase_one_failure((), unexpected_error)
                    continue
                sequence = feed.next_callback_sequence
                feed.next_callback_sequence += 1
                for state in states:
                    callback_key = (symbol, state.callbacks.on_quote)
                    if callback_key in scheduled_callbacks:
                        continue
                    scheduled_callbacks.add(callback_key)
                    deliveries.append(
                        (
                            state,
                            PhaseOneQuoteDelivery(
                                data_session_key=data_session_key,
                                lease_id=state.lease.lease_id,
                                owner=state.lease.owner,
                                consumer_id=state.lease.consumer_id,
                                symbol=symbol,
                                payload=payload,
                                generation=generation,
                                ingress_sequence=sequence,
                                source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
                                received_at_utc=captured_at,
                                received_monotonic_ns=captured_monotonic_ns,
                            ),
                        )
                    )
        for state, delivery in deliveries:
            self._safe_phase_one_quote_callback(state, delivery)

    def _discard_preparing_phase_one_feed(self, feed: _PhaseOnePhysicalFeed) -> None:
        with self._lock:
            self._phase_one_preparing.pop((feed.data_session_key, feed.generation), None)
            if self._phase_one_active.get(feed.data_session_key) is None:
                self._phase_one_owner_by_session.pop(feed.data_session_key, None)
            feed.fenced = True
        self._release_phase_one_process_owner_if_idle(feed.data_session_key)
        for state in (*feed.leases.values(), *feed.pending_leases.values()):
            self._safe_generation_fenced(state, feed.data_session_key, feed.generation)
        self._unsubscribe_phase_one_physical(
            feed.physical_subscription_id,
            data_session_key=feed.data_session_key,
            generation=feed.generation,
            states=tuple((*feed.leases.values(), *feed.pending_leases.values())),
        )

    def _unsubscribe_phase_one_physical(
        self,
        physical_subscription_id: int | None,
        *,
        data_session_key: str,
        generation: int,
        states: tuple[_PhaseOneLeaseState, ...],
    ) -> None:
        if not isinstance(physical_subscription_id, int) or physical_subscription_id <= 0:
            return
        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            error = quote_contract_error(
                QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE,
                "xtdata is unavailable while unsubscribing a fenced Phase 1 quote feed",
                context={
                    "data_session_key": data_session_key,
                    "generation": generation,
                    "subscription_id": physical_subscription_id,
                },
            )
            self._emit_phase_one_failure(states, error)
            return
        try:
            xtdata_mod.unsubscribe_quote(physical_subscription_id)
        except Exception as exc:  # noqa: BLE001 - a new generation is already fenced and remains valid
            error = quote_contract_error(
                QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE,
                "xtdata failed while unsubscribing a fenced Phase 1 quote feed",
                context={
                    "data_session_key": data_session_key,
                    "generation": generation,
                    "subscription_id": physical_subscription_id,
                    "exception_type": type(exc).__name__,
                },
            )
            self._emit_phase_one_failure(states, error, exc_info=True)

    @staticmethod
    def _normalize_phase_one_symbols(symbols: List[str]) -> tuple[str, ...]:
        normalized = tuple(dict.fromkeys(str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()))
        if not normalized:
            raise quote_contract_error(
                QuoteContractReasonCode.SYMBOL_INVALID,
                "Phase 1 quote lease requires at least one non-empty symbol",
            )
        return normalized

    @staticmethod
    def _union_phase_one_symbols(*symbol_groups: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(symbol for group in symbol_groups for symbol in group))

    @staticmethod
    def _validate_phase_one_request(
        *,
        data_session_key: str,
        owner: str,
        consumer_id: str,
        callbacks: PhaseOneLeaseCallbacks,
        bootstrap_fetcher: Callable[[List[str]], Mapping[str, Mapping[str, Any]]],
        max_symbols: int,
    ) -> None:
        if not data_session_key.strip() or not owner.strip() or not consumer_id.strip():
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "Phase 1 quote lease requires non-empty data_session_key, owner, and consumer_id",
            )
        if not isinstance(max_symbols, int) or isinstance(max_symbols, bool) or max_symbols <= 0:
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "Phase 1 quote lease max_symbols must be a positive integer",
                context={"max_symbols": max_symbols},
            )
        if not isinstance(callbacks, PhaseOneLeaseCallbacks):
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "Phase 1 quote lease callbacks must use the registered callback contract",
                context={"callback_type": type(callbacks).__name__},
            )
        if not callable(bootstrap_fetcher):
            raise quote_contract_error(
                QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
                "Phase 1 quote lease requires a callable get_full_tick bootstrap fetcher",
            )

    def _assert_phase_one_owner_locked(self, data_session_key: str, owner: str) -> None:
        existing_owner = self._phase_one_owner_by_session.get(data_session_key)
        if existing_owner is not None and existing_owner != owner:
            error = quote_contract_error(
                QuoteContractReasonCode.OWNER_CONFLICT,
                "a different scheduler owner already holds the Phase 1 quote session",
                context={
                    "data_session_key": data_session_key,
                    "existing_owner": existing_owner,
                    "requested_owner": owner,
                },
            )
            self._phase_one_process_failure_by_session[data_session_key] = error.as_loud_payload()
            raise error

    def _claim_phase_one_process_owner_locked(self, data_session_key: str, owner: str) -> None:
        """Enforce one physical Phase 1 feed per process/session, not per object."""

        with self._phase_one_process_owner_lock:
            registered = self._phase_one_process_owner_by_session.get(data_session_key)
            registered_subscriber = registered[0]() if registered is not None else None
            if registered_subscriber is None:
                self._phase_one_process_owner_by_session.pop(data_session_key, None)
                self._phase_one_process_owner_by_session[data_session_key] = (weakref.ref(self), owner)
                return
            registered_owner = registered[1]
            if registered_subscriber is self and registered_owner == owner:
                return
            error = quote_contract_error(
                QuoteContractReasonCode.OWNER_CONFLICT,
                "another subscriber instance already owns this Phase 1 quote session in the current process",
                context={
                    "data_session_key": data_session_key,
                    "existing_owner": registered_owner,
                    "requested_owner": owner,
                    "same_subscriber_instance": registered_subscriber is self,
                },
            )
            self._phase_one_process_failure_by_session[data_session_key] = error.as_loud_payload()
            raise error

    def _release_phase_one_process_owner_if_idle(self, data_session_key: str) -> None:
        with self._lock:
            if data_session_key in self._phase_one_active or any(
                session_key == data_session_key for session_key, _generation in self._phase_one_preparing
            ):
                return
        with self._phase_one_process_owner_lock:
            registered = self._phase_one_process_owner_by_session.get(data_session_key)
            if registered is not None and registered[0]() is self:
                self._phase_one_process_owner_by_session.pop(data_session_key, None)

    def _assert_phase_one_capacity(self, symbols: tuple[str, ...], *, max_symbols: int, data_session_key: str) -> None:
        if len(symbols) > max_symbols:
            with self._lock:
                self._phase_one_capacity_rejected_total += 1
            raise quote_contract_error(
                QuoteContractReasonCode.CAPACITY_EXCEEDED,
                "Phase 1 quote lease symbol union exceeds configured capacity",
                context={
                    "data_session_key": data_session_key,
                    "requested_symbol_count": len(symbols),
                    "max_symbols": max_symbols,
                },
            )

    def _safe_phase_one_quote_callback(self, state: _PhaseOneLeaseState, delivery: PhaseOneQuoteDelivery) -> bool:
        try:
            acknowledged = state.callbacks.on_quote(delivery)
            if acknowledged is not True:
                raise RuntimeError("consumer callback did not return an explicit True capture acknowledgment")
        except Exception as exc:  # noqa: BLE001 - xtdata callback must remain isolated from consumers
            error = quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote consumer failed while capturing a callback delivery",
                context={
                    "data_session_key": delivery.data_session_key,
                    "lease_id": delivery.lease_id,
                    "consumer_id": delivery.consumer_id,
                    "symbol": delivery.symbol,
                    "generation": delivery.generation,
                    "ingress_sequence": delivery.ingress_sequence,
                    "exception_type": type(exc).__name__,
                },
            )
            self._emit_phase_one_failure((state,), error, exc_info=True)
            return False
        return True

    def _safe_generation_prepared(self, state: _PhaseOneLeaseState, data_session_key: str, generation: int) -> bool:
        try:
            acknowledged = state.callbacks.on_generation_prepared(data_session_key, generation)
            if acknowledged is not True:
                raise RuntimeError("consumer callback did not return an explicit True generation preparation acknowledgment")
        except Exception as exc:  # noqa: BLE001 - lifecycle callback cannot poison a shared feed
            error = quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote consumer failed while preparing a generation",
                context={
                    "data_session_key": data_session_key,
                    "lease_id": state.lease.lease_id,
                    "consumer_id": state.lease.consumer_id,
                    "generation": generation,
                    "exception_type": type(exc).__name__,
                },
            )
            self._emit_phase_one_failure((state,), error, exc_info=True)
            return False
        return True

    def _safe_generation_published(self, state: _PhaseOneLeaseState, data_session_key: str, generation: int) -> bool:
        try:
            state.callbacks.on_generation_published(data_session_key, generation)
        except Exception as exc:  # noqa: BLE001 - a failed consumer is isolated and recovered by its supervisor
            error = quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote consumer failed while activating a published generation",
                context={
                    "data_session_key": data_session_key,
                    "lease_id": state.lease.lease_id,
                    "consumer_id": state.lease.consumer_id,
                    "generation": generation,
                    "exception_type": type(exc).__name__,
                },
            )
            self._emit_phase_one_failure((state,), error, exc_info=True)
            return False
        return True

    def _safe_generation_fenced(self, state: _PhaseOneLeaseState, data_session_key: str, generation: int) -> None:
        try:
            state.callbacks.on_generation_fenced(data_session_key, generation)
        except Exception as exc:  # noqa: BLE001 - fenced callbacks cannot poison shared teardown
            error = quote_contract_error(
                QuoteContractReasonCode.CONSUMER_FAILURE,
                "Phase 1 quote consumer failed while fencing a generation",
                context={
                    "data_session_key": data_session_key,
                    "lease_id": state.lease.lease_id,
                    "consumer_id": state.lease.consumer_id,
                    "generation": generation,
                    "exception_type": type(exc).__name__,
                },
            )
            self._emit_phase_one_failure((state,), error, exc_info=True)

    def _emit_phase_one_failure(
        self,
        states: tuple[_PhaseOneLeaseState, ...],
        error: QuoteContractError,
        *,
        exc_info: bool = False,
    ) -> None:
        with self._lock:
            payload = error.as_loud_payload()
            context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
            sample_key = f"{payload['reason_code']}:{context.get('data_session_key', '')}:{context.get('symbol', '')}"
            now = datetime.now(UTC).isoformat()
            sample = self._phase_one_failure_samples.get(sample_key)
            if sample is None:
                sample = {"first_observed_at": now, "occurrence_count": 0}
                self._phase_one_failure_samples[sample_key] = sample
            sample["last_observed_at"] = now
            sample["occurrence_count"] = int(sample["occurrence_count"]) + 1
            enriched_payload = {**payload, **sample}
            self._phase_one_last_failure = enriched_payload
        logger.error("Phase 1 quote ingress loud failure: %s", enriched_payload, exc_info=exc_info)
        for state in states:
            try:
                state.callbacks.on_loud_failure(error)
            except Exception as exc:  # noqa: BLE001 - the loud sink itself is isolated
                logger.error(
                    "Phase 1 quote ingress loud-sink failure: reason=%s lease_id=%s exception_type=%s",
                    error.reason_code.value,
                    state.lease.lease_id,
                    type(exc).__name__,
                    exc_info=True,
                )

    def _on_quote(self, datas: Dict):
        """Dispatch quote callbacks."""

        try:
            for stock_code, quote in datas.items():
                with self._lock:
                    callbacks = self.callbacks.get(stock_code, [])

                for callback in callbacks:
                    try:
                        callback(stock_code, quote)
                    except Exception as e:  # noqa: BLE001
                        logger.error("quote callback failed for %s: %s", stock_code, e, exc_info=True)
        except Exception as e:  # noqa: BLE001
            logger.error("quote callback dispatch raised: %s", e, exc_info=True)

    def unsubscribe(self, seq: int) -> bool:
        """Unsubscribe one xtdata quote sequence."""

        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            return False

        try:
            with self._lock:
                if seq in self.subscriptions:
                    stocks = self.subscriptions[seq]
                    xtdata_mod.unsubscribe_quote(seq)
                    del self.subscriptions[seq]
                    for key, payload in list(self.managed_subscriptions.items()):
                        if int(payload.get("seq") or 0) == seq:
                            del self.managed_subscriptions[key]

                    for stock in stocks:
                        if stock in self.callbacks:
                            del self.callbacks[stock]

                    logger.info("unsubscribed realtime quotes: stocks=%s seq=%s", stocks, seq)
                    return True
                logger.warning("quote subscription seq not found: seq=%s", seq)
                return False

        except Exception as e:  # noqa: BLE001
            logger.error("unsubscribe realtime quote raised: %s", e, exc_info=True)
            return False

    def start(self):
        """Start xtdata event loop in a daemon thread."""

        if self.running:
            return

        if _load_xtdata() is None:
            logger.error("xtquant.xtdata is not available; cannot start realtime quote subscriber")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run, name="realtime-quote-subscriber", daemon=True)
        self.thread.start()
        logger.info("realtime quote subscriber started")

    def stop(self):
        """Stop subscription service and unsubscribe all known sequences."""

        if not self.running and not self._phase_one_active and not self._phase_one_preparing:
            return

        self.running = False

        with self._lock:
            for seq in list(self.subscriptions.keys()):
                self.unsubscribe(seq)

        self.shutdown_phase_one_leases()

        logger.info("realtime quote subscriber stopped")

    def _run(self):
        """Run xtdata callback event loop."""

        try:
            xtdata_mod = _load_xtdata()
            if xtdata_mod is None:
                raise RuntimeError("xtquant.xtdata is not available")
            xtdata_mod.run()
        except Exception as e:  # noqa: BLE001
            logger.error("realtime quote subscriber loop raised: %s", e, exc_info=True)
        finally:
            self.running = False

    def get_latest_quote(self, stock_code: str) -> Optional[Dict]:
        """Fetch latest cached quote via xtdata.get_market_data."""

        xtdata_mod = _load_xtdata()
        if xtdata_mod is None:
            return None

        try:
            data = xtdata_mod.get_market_data(
                field_list=["time", "lastPrice", "open", "high", "low", "volume", "amount"],
                stock_list=[stock_code],
                period="tick",
                count=1,
            )

            if data and "lastPrice" in data:
                df_price = data["lastPrice"]
                df_time = data.get("time")
                df_volume = data.get("volume")

                if not df_price.empty:
                    quote = {
                        "time": int(df_time.iloc[0, 0]) if df_time is not None and not df_time.empty else None,
                        "lastPrice": float(df_price.iloc[0, 0]),
                        "close": float(df_price.iloc[0, 0]),
                        "volume": float(df_volume.iloc[0, 0]) if df_volume is not None and not df_volume.empty else None,
                        "open": float(data.get("open").iloc[0, 0]) if "open" in data and not data["open"].empty else None,
                        "high": float(data.get("high").iloc[0, 0]) if "high" in data and not data["high"].empty else None,
                        "low": float(data.get("low").iloc[0, 0]) if "low" in data and not data["low"].empty else None,
                        "amount": float(data.get("amount").iloc[0, 0]) if "amount" in data and not data["amount"].empty else None,
                    }
                    return quote

        except Exception as e:  # noqa: BLE001
            logger.error("get latest quote failed for %s: %s", stock_code, e, exc_info=True)

        return None


_subscriber_instance: Optional[RealtimeQuoteSubscriber] = None


def get_realtime_quote_subscriber() -> RealtimeQuoteSubscriber:
    """Return the process-wide realtime quote subscriber."""

    global _subscriber_instance
    if _subscriber_instance is None:
        _subscriber_instance = RealtimeQuoteSubscriber()
    return _subscriber_instance
