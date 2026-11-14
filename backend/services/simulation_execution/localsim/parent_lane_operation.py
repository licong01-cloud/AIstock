"""Opt-in offline parent capacity; each lane delegates to the native queue.

There is one account, never a cash book or a matching pass per lane. This
adapter is not a production HumanOperationPolicy or a single-queue snapshot.
"""

from backend.services.position_timing.intraday.contracts import digest, payload, utc

from .human_operation import HumanOperationPolicy, HumanOperationQueue, operation_snapshot

FUNDING_SHA = "6042ea05494e58844261b63caa46874f672e34af6e71af55333ea9934615608d"


def capacity_control():
    return {"schema_version": "position_timing_parent_lane_capacity_control_v1",
        "control_id": "C2_PARENT_LANE_CAPACITY_PROBE_V1",
        "baseline_capacity": "ACCOUNT_SINGLE_OPERATOR", "capacity": "PARENT_PLAN_INDEPENDENT_OPERATORS",
        "lane_key": ["account_id", "parent_plan_id"], "reaction_delay_seconds": 60,
        "minimum_action_interval_seconds": 60, "maximum_actions_per_lane_dispatch": 1,
        "queue_order": "CANCEL_THEN_SELL_THEN_BUY_READY_FIFO", "one_inflight_per_lane": True,
        "same_clock_matching_passes": 1, "shared_cash_and_positions": True,
        "recovery_unit": "ATOMIC_COMPLETED_DAY", "production_default_changed": False}


def validate_capacity_control(control):
    if control != capacity_control() or digest(control) != digest(capacity_control()):
        raise ValueError("LOCALSIM_PARENT_LANE_CONTROL_CONFLICT")


def operation_envelope(policy, control):
    validate_capacity_control(control)
    return {"schema_version": "localsim_dispatch_capacity_operation_v1",
        "base_operation": operation_snapshot(policy), "capacity_control": control,
        "capacity_control_sha256": digest(control)}


class ParentLaneOperationQueue:
    """Bounded daily lanes, stable across revisions/opportunities of a parent."""

    def __init__(self, *, account_id, policy, control, parent_plan_ids):
        validate_capacity_control(control)
        if policy != HumanOperationPolicy(reaction_delay_seconds=60):
            raise ValueError("LOCALSIM_PARENT_LANE_POLICY_CONFLICT")
        if (not isinstance(account_id, str) or not account_id or not isinstance(parent_plan_ids, (list, tuple, set, frozenset))
                or any(not isinstance(key, str) or not key for key in parent_plan_ids)):
            raise ValueError("LOCALSIM_PARENT_LANE_IDENTITY_CONFLICT")
        self.account_id, self.policy, self.control = account_id, policy, control
        self.parents = frozenset(parent_plan_ids)
        self.lanes = {}
        self.maximum_queue = 0

    def _lane(self, plan_id):
        if plan_id not in self.parents:
            raise ValueError("LOCALSIM_PARENT_LANE_UNKNOWN_PARENT")
        if plan_id not in self.lanes:
            self.lanes[plan_id] = HumanOperationQueue(account_id=self.account_id, policy=self.policy)
        return self.lanes[plan_id]

    @property
    def actions(self):
        return {key: action for lane in self.lanes.values() for key, action in lane.actions.items()}

    @property
    def pending_count(self):
        return sum(lane.pending_count for lane in self.lanes.values())

    @property
    def dispatch_limit(self):
        return len(self.lanes)

    def enqueue(self, **kwargs):
        action = self._lane(kwargs["plan_id"]).enqueue(**kwargs)
        self.maximum_queue = max(self.maximum_queue, self.pending_count)
        return action

    def revalidate_opportunity(self, action_id, **kwargs):
        action = self.actions[action_id]
        return self.lanes[action.plan_id].revalidate_opportunity(action_id, **kwargs)

    def invalidate_plan(self, plan_id, **kwargs):
        if plan_id in self.lanes:
            self.lanes[plan_id].invalidate_plan(plan_id, **kwargs)

    def claim(self, as_of, *, in_trading_session):
        now = utc(as_of)
        candidates = []
        for lane in self.lanes.values():
            action = lane.peek(now, in_trading_session=in_trading_session)
            if action is not None:
                if lane.inflight is not None:
                    raise ValueError("LOCALSIM_PARENT_LANE_ACK_REQUIRED")
                priority = 0 if action.kind == "CANCEL" else 1 if action.side == "SELL" else 2
                candidates.append((priority, action.visible_at, action.symbol, action.action_id, action.plan_id))
        if not candidates:
            return None
        plan_id = min(candidates)[-1]
        return self.lanes[plan_id].claim(now, in_trading_session=in_trading_session)

    def acknowledge(self, action_id, outcome):
        self.lanes[self.actions[action_id].plan_id].acknowledge(action_id, outcome)

    def snapshot(self):
        lanes = [{"parent_plan_id": key, "action_ids": list(lane.actions),
            "last_action_at": payload(lane.last_action_at), "queue": lane.snapshot()}
            for key, lane in sorted(self.lanes.items())]
        state = {"schema_version": "localsim_parent_lane_operation_state_v1", "account_id": self.account_id,
            "capacity_control_sha256": digest(self.control), "actions": payload([a for _, a in sorted(self.actions.items())]),
            "lanes": lanes, "maximum_queue": self.maximum_queue}
        return state | {"state_sha256": digest(state)}

    @classmethod
    def restore(cls, snapshot, *, policy, control, parent_plan_ids):
        body = {k: v for k, v in snapshot.items() if k != "state_sha256"}
        if (body.get("schema_version") != "localsim_parent_lane_operation_state_v1"
                or set(body) != {"schema_version", "account_id", "capacity_control_sha256", "actions", "lanes", "maximum_queue"}
                or digest(body) != snapshot.get("state_sha256") or body["capacity_control_sha256"] != digest(control)):
            raise ValueError("LOCALSIM_PARENT_LANE_STATE_CONFLICT")
        restored = cls(account_id=body["account_id"], policy=policy, control=control, parent_plan_ids=parent_plan_ids)
        for lane in body["lanes"]:
            key = lane["parent_plan_id"]
            if key not in restored.parents or key in restored.lanes:
                raise ValueError("LOCALSIM_PARENT_LANE_POPULATION_CONFLICT")
            queue = HumanOperationQueue.restore(lane["queue"])
            if (queue.account_id != restored.account_id or queue.policy != policy
                    or any(a.plan_id != key for a in queue.actions.values())
                    or lane["action_ids"] != list(queue.actions) or lane["last_action_at"] != payload(queue.last_action_at)):
                raise ValueError("LOCALSIM_PARENT_LANE_MEMBERSHIP_CONFLICT")
            restored.lanes[key] = queue
        restored.maximum_queue = body["maximum_queue"]
        if (type(restored.maximum_queue) is not int or not restored.pending_count <= restored.maximum_queue <= len(restored.actions)
                or restored.snapshot() != snapshot):
            raise ValueError("LOCALSIM_PARENT_LANE_RESTORE_CONFLICT")
        return restored
