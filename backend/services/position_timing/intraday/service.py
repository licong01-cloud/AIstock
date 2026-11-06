"""Human-only application service; bounded source work and filesystem state."""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timedelta
from typing import Callable

from .contracts import (CHINA, ClaimRequest, Decision, EvaluationRequest, ExecutionView, MarketView,
                        Plan, PlanWrite, Policy, State, digest, payload, utc)
from .core import evaluate
from .inputs import LiveInputs
from .store import IntradayError, IntradayStore


class IntradayService:
    def __init__(self, *, store: IntradayStore, source_commit: str,
                 market_loader: Callable[[Plan], MarketView], now_provider: Callable[[], datetime],
                 source_resolver: Callable[[Plan], bool] | None = None, batch_budget_seconds: float = 15):
        self.store, self.source_commit, self.market_loader, self.now = store, source_commit, market_loader, now_provider
        self.source_resolver, self.budget = source_resolver, batch_budget_seconds
        self._pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="timing-minute")
        self._cache_lock = threading.Lock()
        self._inflight: dict[tuple, Future] = {}
        self._cache: dict[tuple, tuple[float, MarketView]] = {}

    def _source_valid(self, plan: Plan) -> bool:
        if plan.source_kind == "MANUAL":
            return True
        if plan.source_kind != "ADVISORY" or self.source_resolver is None:
            return False
        return self.source_resolver(plan) is True

    def _claim_source_valid(self, plan: Plan) -> bool:
        if plan.source_kind == "MANUAL":
            return True
        # Reuse the same bounded worker slots; a future source bridge cannot
        # turn a claim into an unbounded request or grow an unbounded queue.
        key = ("claim-source", plan.plan_id, plan.revision)
        with self._cache_lock:
            future = self._inflight.get(key)
            if future is None:
                if len(self._inflight) >= 4:
                    return False
                future = self._pool.submit(self._source_valid, plan)
                self._inflight[key] = future
        def completed(done):
            with self._cache_lock:
                if self._inflight.get(key) is done:
                    self._inflight.pop(key, None)
        future.add_done_callback(completed)
        try:
            return future.result(timeout=5) is True
        except Exception as exc:
            raise IntradayError("SOURCE_UNVERIFIED", 503) from exc

    def put_plan(self, plan_id: str, request: PlanWrite) -> dict:
        if request.plan.source_kind == "FROZEN_REPLAY":
            raise IntradayError("FROZEN_REPLAY_IS_CLI_ONLY", 422)
        if request.plan.source_kind == "ADVISORY" and self.source_resolver is None:
            raise IntradayError("SOURCE_ADAPTER_NOT_CONFIGURED", 422)
        key, request_hash = "put-" + request.idempotency_key, digest(request)
        with self.store.lock(plan_id):
            head = self.store.head(plan_id)
            retry = self.store.reserve_request(plan_id, key, request_hash)
            old = self._prior(head, key, request_hash) if retry else None
            if old is not None:
                return old
            revision = head["plan"]["revision"] if head else 0
            if revision != request.expected_revision:
                raise IntradayError("PLAN_REVISION_CONFLICT")
            policy = Policy(policy_id=request.plan.policy_id, allow_split=request.plan.allow_split, source_commit=self.source_commit)
            plan = Plan(**request.plan.model_dump(), plan_id=plan_id, revision=revision + 1,
                        policy=policy, policy_sha256=digest(policy))
            response = {"plan": payload(plan), "plan_sha256": digest(plan), "orders_created": False}
            self.store.commit(plan=plan, state=None, decision=None, input_value={"request": request},
                              request_key=key, request_sha256=request_hash, response=response)
            return response

    def _prior(self, head: dict | None, key: str, request_hash: str) -> dict | None:
        previous = self.store.find_request(head, key)
        if previous is None:
            return None
        if previous["request_sha256"] != request_hash:
            raise IntradayError("IDEMPOTENCY_CONTENT_CONFLICT")
        return previous["response"]

    def _load(self, plan: Plan) -> tuple[MarketView, bool]:
        if not self._source_valid(plan):
            raise IntradayError("SOURCE_UNVERIFIED", 503)
        return self.market_loader(plan), True

    def _future(self, plan: Plan) -> Future | None:
        # Include the completed-bar boundary so a cache never hides a new slot.
        boundary = int((utc(self.now()) - timedelta(seconds=5)).timestamp() // 60)
        key = (plan.symbol, plan.target_trade_date, boundary)
        with self._cache_lock:
            self._cache = {k: v for k, v in self._cache.items() if time.monotonic() - v[0] < 5 and k[2] == boundary}
            if key in self._cache and plan.source_kind == "MANUAL":
                future = Future()
                future.set_result((self._cache[key][1], True))
                return future
            # Source authority is per-plan; only MANUAL plans may share its work.
            task_key = key if plan.source_kind == "MANUAL" else (*key, plan.plan_id, plan.revision)
            if task_key in self._inflight:
                return self._inflight[task_key]
            if len(self._inflight) >= 4:
                return None
            future = self._pool.submit(self._load, plan)
            self._inflight[task_key] = future

        def completed(done):
            with self._cache_lock:
                self._inflight.pop(task_key, None)
                if done.exception() is None and plan.source_kind == "MANUAL":
                    self._cache[key] = (time.monotonic(), done.result()[0])

        future.add_done_callback(completed)
        return future

    def evaluate_batch(self, request: EvaluationRequest) -> dict:
        request_hash = digest(request)
        reservation = self.store.path("batches", request.idempotency_key + "-request")
        final_path = self.store.path("batches", request.idempotency_key + "-response")
        with self.store.lock("batch-" + request.idempotency_key):
            existing = self.store.read(reservation)
            if existing is not None and existing["request_sha256"] != request_hash:
                raise IntradayError("IDEMPOTENCY_CONTENT_CONFLICT")
            self.store.write(reservation, {"request_sha256": request_hash, "request": payload(request)})
            completed = self.store.read(final_path)
            if completed is not None:
                return completed
        deadline = time.monotonic() + self.budget
        items, pending, todo = {}, {}, list(request.plans)
        key = "eval-" + request.idempotency_key
        while (todo or pending) and time.monotonic() < deadline:
            remaining = []
            for ref in todo:
                head = self.store.head(ref.plan_id)
                if head is None:
                    items[ref.plan_id] = self._failure(ref, "PLAN_NOT_FOUND")
                    continue
                # New batch keys have no committed item; only recovery scans
                # history, rather than every ordinary minute polling request.
                old = self._prior(head, key, request_hash) if existing is not None else None
                if old is not None:
                    items[ref.plan_id] = old
                    continue
                plan = Plan.model_validate(head["plan"])
                if plan.revision != ref.revision:
                    items[ref.plan_id] = self._failure(ref, "PLAN_REVISION_CONFLICT")
                    continue
                now = utc(self.now())
                if plan.lifecycle != "ACTIVE" or not plan.valid_from <= now < plan.valid_until:
                    items[ref.plan_id] = self._failure(ref, "PLAN_INACTIVE_OR_OUTSIDE_VALIDITY", status="EXPIRED" if now >= plan.valid_until or plan.lifecycle != "ACTIVE" else "WAIT")
                    continue
                future = self._future(plan)
                if future is None:
                    remaining.append(ref)
                else:
                    pending.setdefault(future, []).append((ref, plan))
            todo = remaining
            if not pending:
                break
            ready, _ = wait(pending, timeout=max(0, deadline - time.monotonic()), return_when=FIRST_COMPLETED)
            if not ready:
                break
            for future in ready:
                for ref, plan in pending.pop(future):
                    if time.monotonic() >= deadline:
                        todo.append(ref)
                        continue
                    try:
                        market, source_verified = future.result()
                    except Exception as exc:
                        # Per-item external source failures stay visible and never
                        # turn into an empty successful batch or another source.
                        code = exc.code if isinstance(exc, IntradayError) else "INPUT_UNAVAILABLE_" + type(exc).__name__
                        if isinstance(exc, IntradayError) and exc.status == 500:
                            raise
                        items[ref.plan_id] = self._persist_failure(ref, code, key, request_hash, deadline)
                        continue
                    try:
                        items[ref.plan_id] = self._evaluate_one(plan, market, key, request_hash, source_verified, deadline)
                    except IntradayError as exc:
                        if exc.status == 500:
                            raise
                        items[ref.plan_id] = self._persist_failure(ref, exc.code, key, request_hash, deadline)
        for ref in request.plans:
            if ref.plan_id not in items:
                items[ref.plan_id] = self._failure(ref, "DEFERRED_BUDGET", status="DEFERRED")
        response = {"schema_version": "position_timing_intraday_batch_v1", "request_sha256": request_hash,
                    "items": [items[r.plan_id] for r in request.plans],
                    "partial": any(v["status"] == "DEFERRED" for v in items.values()),
                    "evaluated_at": utc(self.now()).isoformat(), "orders_created": False}
        with self.store.lock("batch-" + request.idempotency_key):
            existing = self.store.read(final_path)
            if existing is not None:
                return existing
            self.store.write(final_path, response)
        return response

    def _failure(self, ref, reason: str, *, status: str = "UNAVAILABLE") -> dict:
        return {"plan_id": ref.plan_id, "revision": ref.revision, "status": status,
                "reason_codes": [reason], "evaluated_at": utc(self.now()).isoformat(), "decision": None}

    def _persist_failure(self, ref, code: str, key: str, request_hash: str, deadline: float) -> dict:
        response = self._failure(ref, code)
        with self.store.lock(ref.plan_id):
            if time.monotonic() >= deadline:
                return self._failure(ref, "DEFERRED_BUDGET", status="DEFERRED")
            head = self.store.head(ref.plan_id)
            if head is None or head["plan"]["revision"] != ref.revision:
                return response
            retry = self.store.reserve_request(ref.plan_id, key, request_hash)
            old = self._prior(head, key, request_hash) if retry else None
            if old is not None:
                return old
            plan = Plan.model_validate(head["plan"])
            state = State.model_validate(head["state"]) if head["state"] else None
            if state is not None:
                state = state.model_copy(update={"last_condition": False})
            self.store.commit(plan=plan, state=state, decision=None, input_value={"source_failure": code},
                              request_key=key, request_sha256=request_hash, response=response)
        return response

    def _evaluate_one(self, plan: Plan, market: MarketView, key: str, request_hash: str, source_verified: bool, deadline: float) -> dict:
        with self.store.lock(plan.plan_id):
            if time.monotonic() >= deadline:
                return self._failure(plan, "DEFERRED_BUDGET", status="DEFERRED")
            head = self.store.head(plan.plan_id)
            retry = self.store.reserve_request(plan.plan_id, key, request_hash)
            old = self._prior(head, key, request_hash) if retry else None
            if old is not None:
                return old
            if head is None or head["plan"] != payload(plan):
                raise IntradayError("PLAN_REVISION_CONFLICT")
            if not source_verified:
                raise IntradayError("SOURCE_UNVERIFIED", 503)
            as_of = utc(self.now())
            state = State.model_validate(head["state"]) if head["state"] else None
            decision, next_state = evaluate(plan, market, ExecutionView(), plan.policy, state, as_of)
            response = {"plan_id": plan.plan_id, "revision": plan.revision, "status": decision.status,
                        "reason_codes": list(decision.reason_codes), "evaluated_at": as_of.isoformat(), "decision": payload(decision)}
            if time.monotonic() >= deadline:
                return self._failure(plan, "DEFERRED_BUDGET", status="DEFERRED")
            self.store.commit(plan=plan, state=next_state, decision=decision, input_value=market,
                              request_key=key, request_sha256=request_hash, response=response)
            return response

    def status(self, plan_id: str) -> dict:
        head = self.store.head(plan_id)
        if head is None:
            raise IntradayError("PLAN_NOT_FOUND", 404)
        plan = Plan.model_validate(head["plan"])
        now = utc(self.now())
        decision = Decision.model_validate(head["decision"]) if head["decision"] else None
        status, reason = ((decision.status, list(decision.reason_codes)) if decision else
                          (head["response"].get("status", "WAIT"), head["response"].get("reason_codes", ["NOT_EVALUATED"])))
        if plan.lifecycle != "ACTIVE" or now >= plan.valid_until:
            status, reason = "EXPIRED", ["PLAN_" + (plan.lifecycle if plan.lifecycle != "ACTIVE" else "EXPIRED")]
        elif decision and now >= decision.valid_until:
            status, reason = "UNAVAILABLE", ["DECISION_EXPIRED"]
        claimed = bool(decision and self.store.read(self.store.path("claims", decision.opportunity_id)))
        return {"plan": payload(plan), "plan_sha256": digest(plan), "status": status, "reason_codes": reason,
                "decision": payload(decision) if decision else None, "already_alerted": claimed,
                "source_last_verified_at": decision.as_of.isoformat() if decision else None,
                "source_verification": "LAST_EVALUATION_ONLY", "orders_created": False}

    def alerts(self, plan_ids: list[str], offset: int = 0, limit: int = 50) -> dict:
        items = []
        for plan_id in plan_ids[offset:offset + limit]:
            try:
                items.append(self.status(plan_id))
            except IntradayError as exc:
                if exc.status != 404:
                    raise
                items.append({"plan_id": plan_id, "status": "UNAVAILABLE", "reason_codes": [exc.code]})
        return {"items": items, "next_offset": offset + limit if offset + limit < len(plan_ids) else None}

    def claim(self, opportunity_id: str, request: ClaimRequest) -> dict:
        head = self.store.head(request.plan_id)
        if head is None:
            raise IntradayError("PLAN_NOT_FOUND", 404)
        plan = Plan.model_validate(head["plan"])
        verified_at = utc(self.now())
        if not self._claim_source_valid(plan):
            raise IntradayError("SOURCE_UNVERIFIED", 503)
        with self.store.lock(request.plan_id):
            head = self.store.head(request.plan_id)
            if head is None or head["plan"]["revision"] != request.expected_revision or payload(plan) != head["plan"]:
                raise IntradayError("PLAN_REVISION_CONFLICT")
            self.store.reserve_request(plan.plan_id, "claim-" + request.idempotency_key, digest(request))
            view = self.status(request.plan_id)
            decision = Decision.model_validate(head["decision"]) if head["decision"] else None
            if (view["status"] != "OPPORTUNITY" or decision is None or decision.decision_id != request.decision_id
                    or decision.opportunity_id != opportunity_id or utc(self.now()) - verified_at > timedelta(seconds=5)):
                raise IntradayError("OPPORTUNITY_NO_LONGER_ELIGIBLE")
            path = self.store.path("claims", opportunity_id)
            existing = self.store.read(path)
            if existing is not None:
                return {"authorized": False, "already_authorized": True, "event": existing}
            event = {"event_type": "ALERT_EMISSION_AUTHORIZED", "opportunity_id": opportunity_id,
                     "plan_id": plan.plan_id, "revision": plan.revision, "plan_sha256": digest(plan),
                     "decision_id": decision.decision_id, "policy_sha256": plan.policy_sha256,
                     "authorized_at": utc(self.now()).isoformat(), "request_sha256": digest(request),
                     "delivery_confirmed": False, "execution_confirmed": False}
            self.store.write(path, event)
            return {"authorized": True, "already_authorized": False, "event": event}


def build_intraday_service() -> IntradayService:
    from backend.services.position_timing.service import _source_commit

    def now():
        return datetime.now(CHINA)
    return IntradayService(store=IntradayStore(), source_commit=_source_commit(), market_loader=LiveInputs(now), now_provider=now)
