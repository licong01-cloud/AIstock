"""Model-aware GPU training policy and per-node shared/exclusive phase gate."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any, Mapping


GPU_TRAINING_POLICY_EXCLUSIVE = "exclusive"
GPU_TRAINING_POLICY_PARALLEL = "parallel"
GPU_TRAINING_POLICIES = {
    GPU_TRAINING_POLICY_EXCLUSIVE,
    GPU_TRAINING_POLICY_PARALLEL,
}

GPU_TRAINING_POLICY_INVALID_REASON = "QE_GPU_TRAINING_POLICY_INVALID"
GPU_TRAINING_POLICY_CONFLICT_REASON = "QE_GPU_TRAINING_POLICY_CONFLICT"

_GAT_IDENTITIES = {"gat", "gats", "efficientgat", "efficientgats"}


class QEGPUTrainingPolicyError(ValueError):
    def __init__(self, reason_code: str, message: str):
        super().__init__(f"{reason_code}: {message}")
        self.reason_code = reason_code
        self.message = message


def _normalized_identity(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _identity_tokens(value: Any) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", str(value or "").strip().lower())
        if token
    }


def _is_known_gat(model_info: Mapping[str, Any] | None) -> bool:
    if not model_info:
        return False
    model_config = model_info.get("model_config")
    config = model_config if isinstance(model_config, Mapping) else {}
    exact_candidates = {
        _normalized_identity(model_info.get("model_name")),
        _normalized_identity(model_info.get("model_type")),
        _normalized_identity(config.get("class")),
    }
    if exact_candidates & _GAT_IDENTITIES:
        return True
    token_candidates: set[str] = set()
    for value in (
        model_info.get("model_id"),
        config.get("module_path"),
        config.get("pt_model_uri"),
    ):
        token_candidates.update(_identity_tokens(value))
    return bool(token_candidates & _GAT_IDENTITIES)


def resolve_gpu_training_policy(model_info: Mapping[str, Any] | None) -> str:
    """Resolve the two-class GPU policy from catalog metadata.

    Known GAT identities are always exclusive. Non-GAT models default to
    parallel and may be explicitly tightened to exclusive through
    ``model_config.gpu_training_policy``.
    """

    model_config = (model_info or {}).get("model_config")
    config = model_config if isinstance(model_config, Mapping) else {}
    explicit_value = config.get("gpu_training_policy")
    if explicit_value is None and model_info:
        explicit_value = model_info.get("gpu_training_policy")
    explicit = str(explicit_value or "").strip().lower() or None
    if explicit is not None and explicit not in GPU_TRAINING_POLICIES:
        raise QEGPUTrainingPolicyError(
            GPU_TRAINING_POLICY_INVALID_REASON,
            f"unsupported gpu_training_policy={explicit_value!r}",
        )

    known_gat = _is_known_gat(model_info)
    if known_gat and explicit == GPU_TRAINING_POLICY_PARALLEL:
        raise QEGPUTrainingPolicyError(
            GPU_TRAINING_POLICY_CONFLICT_REASON,
            "known GAT/EfficientGATs models cannot use parallel GPU training policy",
        )
    if known_gat:
        return GPU_TRAINING_POLICY_EXCLUSIVE
    return explicit or GPU_TRAINING_POLICY_PARALLEL


@dataclass
class GPUPhaseLease:
    """One idempotently releasable shared or exclusive gate lease."""

    gate: "ModelAwareGPUPhaseGate"
    policy: str
    _released: bool = field(default=False, init=False)

    @property
    def released(self) -> bool:
        return self._released

    async def release(self) -> None:
        if self._released:
            return
        await self.gate._release(self.policy)
        self._released = True


class ModelAwareGPUPhaseGate:
    """Writer-preferring reader/writer gate for one compute node.

    ``exclusive`` is the writer used by GAT-family training. ``parallel`` is
    the shared reader used by all other models. The existing task-level
    ``node_parallelism`` remains the shared concurrency limit.
    """

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._exclusive_active = False
        self._active_parallel = 0
        self._waiting_exclusive = 0

    @property
    def exclusive_active(self) -> bool:
        return self._exclusive_active

    @property
    def active_parallel(self) -> int:
        return self._active_parallel

    @property
    def waiting_exclusive(self) -> int:
        return self._waiting_exclusive

    async def acquire(self, policy: str) -> GPUPhaseLease:
        normalized = str(policy or "").strip().lower()
        if normalized not in GPU_TRAINING_POLICIES:
            raise QEGPUTrainingPolicyError(
                GPU_TRAINING_POLICY_INVALID_REASON,
                f"unsupported gpu training lease policy={policy!r}",
            )

        async with self._condition:
            if normalized == GPU_TRAINING_POLICY_EXCLUSIVE:
                self._waiting_exclusive += 1
                try:
                    await self._condition.wait_for(
                        lambda: not self._exclusive_active and self._active_parallel == 0
                    )
                finally:
                    self._waiting_exclusive -= 1
                self._exclusive_active = True
            else:
                await self._condition.wait_for(
                    lambda: not self._exclusive_active and self._waiting_exclusive == 0
                )
                self._active_parallel += 1
        return GPUPhaseLease(self, normalized)

    async def _release(self, policy: str) -> None:
        async with self._condition:
            if policy == GPU_TRAINING_POLICY_EXCLUSIVE:
                if not self._exclusive_active:
                    raise RuntimeError("exclusive GPU phase lease is not active")
                self._exclusive_active = False
            else:
                if self._active_parallel <= 0:
                    raise RuntimeError("parallel GPU phase lease is not active")
                self._active_parallel -= 1
            self._condition.notify_all()
