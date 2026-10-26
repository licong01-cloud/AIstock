"""Deterministic account-scoped human actions; no order, account or timing rules."""

from __future__ import annotations

from datetime import datetime, timedelta
import heapq
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from backend.services.position_timing.intraday.contracts import digest, payload, utc


class HumanOperationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["localsim_human_operation_policy_v1"] = "localsim_human_operation_policy_v1"
    policy_id: Literal["localsim_human_operation_v1"] = "localsim_human_operation_v1"
    operator_scope: Literal["ACCOUNT"] = "ACCOUNT"
    reaction_delay_seconds: Literal[0, 60, 180, 300]
    minimum_action_interval_seconds: Literal[60] = 60
    maximum_actions_per_dispatch: Literal[1] = 1
    queue_order: Literal["CANCEL_THEN_SELL_THEN_BUY_READY_FIFO"] = "CANCEL_THEN_SELL_THEN_BUY_READY_FIFO"
    allow_split: Literal[False] = False
    automatic_reprice: Literal[False] = False
    new_order_requires_known_account: Literal[True] = True
    random_response_model: Literal["disabled"] = "disabled"

    @field_validator("reaction_delay_seconds", "minimum_action_interval_seconds", "maximum_actions_per_dispatch", mode="before")
    @classmethod
    def integers_only(cls, value):
        if type(value) is not int:
            raise ValueError("LOCALSIM_HUMAN_INTEGER_REQUIRED")
        return value

    @field_validator("allow_split", "automatic_reprice", "new_order_requires_known_account", mode="before")
    @classmethod
    def booleans_only(cls, value):
        if type(value) is not bool:
            raise ValueError("LOCALSIM_HUMAN_BOOLEAN_REQUIRED")
        return value


def operation_snapshot(policy: HumanOperationPolicy | None = None) -> dict:
    return {"schema_version": "localsim_operation_snapshot_v1",
            "operation_mode": "HUMAN_SIM" if policy is not None else "NATIVE",
            "human_policy": policy.model_dump(mode="json") if policy is not None else None}


def validate_operation_snapshot(value: dict | None, *, legacy: bool = False) -> dict:
    """Legacy read compatibility never writes defaults back into an old identity."""
    if value is None and legacy:
        return operation_snapshot()
    if not isinstance(value, dict) or set(value) != {"schema_version", "operation_mode", "human_policy"}:
        raise ValueError("LOCALSIM_OPERATION_SNAPSHOT_REQUIRED")
    if value["schema_version"] != "localsim_operation_snapshot_v1":
        raise ValueError("LOCALSIM_OPERATION_SCHEMA_UNSUPPORTED")
    if value["operation_mode"] == "NATIVE" and value["human_policy"] is None:
        return operation_snapshot()
    if value["operation_mode"] == "HUMAN_SIM" and isinstance(value["human_policy"], dict):
        policy = HumanOperationPolicy.model_validate(value["human_policy"])
        canonical = operation_snapshot(policy)
        if digest(canonical) == digest(value):
            return canonical
    raise ValueError("LOCALSIM_OPERATION_POLICY_INVALID")


def validate_operation_configuration(value: dict | None, *, algo_code: str, legacy: bool = False) -> dict:
    operation = validate_operation_snapshot(value, legacy=legacy)
    if (algo_code not in {"TWAP", "POSITION_TIMING_INTRADAY_V1"}
            or operation["operation_mode"] == "HUMAN_SIM" and algo_code != "POSITION_TIMING_INTRADAY_V1"):
        raise ValueError("LOCALSIM_OPERATION_EXECUTION_COMBINATION_UNSUPPORTED")
    return operation


class HumanAction(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action_id: str
    plan_id: str
    revision: int
    opportunity_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    kind: Literal["SUBMIT", "CANCEL"]
    visible_at: datetime
    ready_at: datetime
    valid_until: datetime
    policy_sha256: str
    status: Literal["QUEUED", "DISPATCHING", "COMPLETED", "INVALIDATED"] = "QUEUED"
    dispatch_at: datetime | None = None
    outcome: dict[str, Any] | None = None

    @field_validator("visible_at", "ready_at", "valid_until", "dispatch_at", mode="before")
    @classmethod
    def aware_clock(cls, value):
        return utc(value) if value is not None else None


class HumanOperationQueue:
    """One virtual operator per account; rejected attempts consume its capacity."""

    def __init__(self, *, account_id: str, policy: HumanOperationPolicy):
        if not account_id:
            raise ValueError("LOCALSIM_HUMAN_ACCOUNT_REQUIRED")
        self.account_id, self.policy = account_id, policy
        self.actions: dict[str, HumanAction] = {}
        self.last_action_at: datetime | None = None
        self.last_clock: datetime | None = None
        self.inflight: str | None = None
        self.maximum_queue = 0
        self._future: list[tuple[datetime, str]] = []
        self._ready: list[tuple[int, datetime, str, str]] = []
        self._expiry: list[tuple[datetime, str]] = []

    def enqueue(self, *, plan_id: str, revision: int, opportunity_id: str, symbol: str,
                side: str, visible_at: datetime, valid_until: datetime, kind: str = "SUBMIT") -> HumanAction:
        visible_at, valid_until = utc(visible_at), utc(valid_until)
        identity = [self.account_id, digest(self.policy), plan_id, revision, opportunity_id, kind]
        key = "lsha_" + digest(identity)
        if key in self.actions:
            prior = self.actions[key]
            if (prior.plan_id, prior.revision, prior.symbol, prior.side, prior.kind) != (
                    plan_id, revision, symbol, side, kind):
                raise ValueError("LOCALSIM_HUMAN_ACTION_IDENTITY_CONFLICT")
            return prior  # Polling never extends the first opportunity's clock.
        if revision < 1 or valid_until <= visible_at:
            raise ValueError("LOCALSIM_HUMAN_ACTION_WINDOW_INVALID")
        action = HumanAction(action_id=key, plan_id=plan_id, revision=revision,
                             opportunity_id=opportunity_id, symbol=symbol, side=side, kind=kind,
                             visible_at=visible_at,
                             ready_at=visible_at + timedelta(seconds=self.policy.reaction_delay_seconds),
                             valid_until=valid_until, policy_sha256=digest(self.policy))
        self.actions[key] = action
        heapq.heappush(self._future, (action.ready_at, key))
        heapq.heappush(self._expiry, (action.valid_until, key))
        self.maximum_queue = max(self.maximum_queue, self.pending_count)
        return action

    @property
    def pending_count(self) -> int:
        return sum(a.status in {"QUEUED", "DISPATCHING"} for a in self.actions.values())

    def invalidate(self, action_id: str, reason: str) -> None:
        action = self.actions[action_id]
        if action.status == "QUEUED":
            self.actions[action_id] = action.model_copy(update={
                "status": "INVALIDATED", "outcome": {"reason": reason}})

    def invalidate_plan(self, plan_id: str, *, revision: int | None = None,
                        kind: str | None = "SUBMIT", reason: str) -> None:
        for action in list(self.actions.values()):
            if (action.plan_id == plan_id and (revision is None or action.revision == revision)
                    and (kind is None or action.kind == kind)):
                self.invalidate(action.action_id, reason)

    def claim(self, as_of: datetime, *, in_trading_session: bool) -> HumanAction | None:
        now = utc(as_of)
        if self.last_clock is not None and now < self.last_clock:
            raise ValueError("LOCALSIM_HUMAN_CLOCK_REVERSED")
        self.last_clock = now
        if self.inflight is not None:
            return self.actions[self.inflight]  # Native readback, never a new attempt.
        while self._expiry and self._expiry[0][0] <= now:
            _, key = heapq.heappop(self._expiry)
            self.invalidate(key, "OPPORTUNITY_EXPIRED_IN_QUEUE")
        if not in_trading_session:
            return None
        if self.last_action_at is not None and now < self.last_action_at + timedelta(
                seconds=self.policy.minimum_action_interval_seconds):
            return None
        while self._future and self._future[0][0] <= now:
            _, key = heapq.heappop(self._future)
            action = self.actions[key]
            if action.status == "QUEUED":
                priority = 0 if action.kind == "CANCEL" else 1 if action.side == "SELL" else 2
                heapq.heappush(self._ready, (priority, action.visible_at, action.symbol, key))
        while self._ready:
            *_, key = heapq.heappop(self._ready)
            action = self.actions[key]
            if action.status != "QUEUED":
                continue
            self.last_action_at, self.inflight = now, key
            action = action.model_copy(update={"status": "DISPATCHING", "dispatch_at": now})
            self.actions[key] = action
            return action
        return None

    def acknowledge(self, action_id: str, outcome: dict) -> None:
        action = self.actions[action_id]
        if action.status == "COMPLETED":
            if digest(action.outcome) != digest(outcome):
                raise ValueError("LOCALSIM_HUMAN_ACTION_ACK_CONFLICT")
            return
        if action.status != "DISPATCHING" or self.inflight != action_id:
            raise ValueError("LOCALSIM_HUMAN_ACTION_ACK_WITHOUT_DISPATCH")
        self.actions[action_id] = action.model_copy(update={"status": "COMPLETED", "outcome": outcome})
        self.inflight = None

    def snapshot(self) -> dict:
        state = {"schema_version": "localsim_human_operation_state_v1", "account_id": self.account_id,
                 "policy": self.policy.model_dump(mode="json"),
                 "actions": [a.model_dump(mode="json") for a in self.actions.values()],
                 "last_action_at": payload(self.last_action_at), "last_clock": payload(self.last_clock),
                 "inflight": self.inflight, "maximum_queue": self.maximum_queue}
        return {**state, "state_sha256": digest(state)}

    @classmethod
    def restore(cls, snapshot: dict) -> "HumanOperationQueue":
        state = {k: v for k, v in snapshot.items() if k != "state_sha256"}
        if (state.get("schema_version") != "localsim_human_operation_state_v1"
                or set(state) != {"schema_version", "account_id", "policy", "actions", "last_action_at",
                                  "last_clock", "inflight", "maximum_queue"}
                or digest(state) != snapshot.get("state_sha256")):
            raise ValueError("LOCALSIM_HUMAN_STATE_IDENTITY_CONFLICT")
        queue = cls(account_id=state["account_id"], policy=HumanOperationPolicy.model_validate(state["policy"]))
        queue.last_action_at = utc(state["last_action_at"]) if state["last_action_at"] else None
        queue.last_clock = utc(state["last_clock"]) if state["last_clock"] else None
        queue.inflight, queue.maximum_queue = state["inflight"], state["maximum_queue"]
        if (type(queue.maximum_queue) is not int or queue.maximum_queue < 0
                or queue.last_action_at is not None and (queue.last_clock is None or queue.last_action_at > queue.last_clock)):
            raise ValueError("LOCALSIM_HUMAN_RESTORE_CLOCK_CONFLICT")
        for row in state["actions"]:
            action = HumanAction.model_validate(row)
            expected = "lsha_" + digest([queue.account_id, digest(queue.policy), action.plan_id,
                                         action.revision, action.opportunity_id, action.kind])
            if action.action_id != expected or action.action_id in queue.actions or action.policy_sha256 != digest(queue.policy):
                raise ValueError("LOCALSIM_HUMAN_RESTORE_ACTION_CONFLICT")
            if (action.revision < 1 or action.ready_at != action.visible_at + timedelta(seconds=queue.policy.reaction_delay_seconds)
                    or action.valid_until <= action.visible_at
                    or action.status in {"QUEUED", "INVALIDATED"} and action.dispatch_at is not None
                    or action.status in {"DISPATCHING", "COMPLETED"} and action.dispatch_at is None
                    or action.dispatch_at is not None and not action.ready_at <= action.dispatch_at < action.valid_until
                    or action.status in {"COMPLETED", "INVALIDATED"} and action.outcome is None
                    or action.status in {"QUEUED", "DISPATCHING"} and action.outcome is not None):
                raise ValueError("LOCALSIM_HUMAN_RESTORE_ACTION_STATE_CONFLICT")
            queue.actions[action.action_id] = action
            if action.status == "QUEUED":
                heapq.heappush(queue._future, (action.ready_at, action.action_id))
                heapq.heappush(queue._expiry, (action.valid_until, action.action_id))
        dispatched = [a.action_id for a in queue.actions.values() if a.status == "DISPATCHING"]
        if dispatched != ([queue.inflight] if queue.inflight else []):
            raise ValueError("LOCALSIM_HUMAN_RESTORE_INFLIGHT_CONFLICT")
        attempts = sorted(a.dispatch_at for a in queue.actions.values() if a.dispatch_at is not None)
        if (queue.last_action_at != (attempts[-1] if attempts else None)
                or any((right-left).total_seconds() < queue.policy.minimum_action_interval_seconds
                       for left, right in zip(attempts, attempts[1:]))
                or queue.maximum_queue < queue.pending_count or queue.maximum_queue > len(queue.actions)):
            raise ValueError("LOCALSIM_HUMAN_RESTORE_CAPACITY_CONFLICT")
        return queue
