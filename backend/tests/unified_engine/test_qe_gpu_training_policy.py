from __future__ import annotations

import asyncio

import pytest

from backend.services.quantevolver.qe_gpu_training_policy import (
    GPU_TRAINING_POLICY_CONFLICT_REASON,
    GPU_TRAINING_POLICY_EXCLUSIVE,
    GPU_TRAINING_POLICY_INVALID_REASON,
    GPU_TRAINING_POLICY_PARALLEL,
    ModelAwareGPUPhaseGate,
    QEGPUTrainingPolicyError,
    resolve_gpu_training_policy,
)


@pytest.mark.parametrize(
    "model_info",
    [
        {"model_id": "__seed_GATs_default_v1__", "model_name": "GATs"},
        {
            "model_id": "custom-efficient",
            "model_name": "custom",
            "model_config": {"class": "EfficientGATs"},
        },
        {
            "model_id": "custom-gat-uri",
            "model_name": "custom",
            "model_config": {"module_path": "qlib.contrib.model.pytorch_gats_ts"},
        },
    ],
)
def test_known_gat_models_are_always_exclusive(model_info):
    assert resolve_gpu_training_policy(model_info) == GPU_TRAINING_POLICY_EXCLUSIVE


@pytest.mark.parametrize("model_name", ["LSTM", "ALSTM", "TCN", "Transformer", "GRU", "LGBModel"])
def test_non_gat_models_default_to_parallel(model_name):
    assert resolve_gpu_training_policy({"model_name": model_name}) == GPU_TRAINING_POLICY_PARALLEL


def test_non_gat_model_can_be_explicitly_tightened_to_exclusive():
    model_info = {
        "model_name": "LargeResidentTransformer",
        "model_config": {"gpu_training_policy": "exclusive"},
    }
    assert resolve_gpu_training_policy(model_info) == GPU_TRAINING_POLICY_EXCLUSIVE


def test_known_gat_cannot_be_overridden_to_parallel():
    with pytest.raises(QEGPUTrainingPolicyError) as exc_info:
        resolve_gpu_training_policy(
            {
                "model_name": "GATs",
                "model_config": {"gpu_training_policy": "parallel"},
            }
        )
    assert exc_info.value.reason_code == GPU_TRAINING_POLICY_CONFLICT_REASON


def test_invalid_policy_fails_fast():
    with pytest.raises(QEGPUTrainingPolicyError) as exc_info:
        resolve_gpu_training_policy(
            {
                "model_name": "LSTM",
                "model_config": {"gpu_training_policy": "auto"},
            }
        )
    assert exc_info.value.reason_code == GPU_TRAINING_POLICY_INVALID_REASON


def test_parallel_leases_can_coexist():
    async def scenario():
        gate = ModelAwareGPUPhaseGate()
        first = await gate.acquire(GPU_TRAINING_POLICY_PARALLEL)
        second = await gate.acquire(GPU_TRAINING_POLICY_PARALLEL)

        assert gate.active_parallel == 2
        assert gate.exclusive_active is False

        await first.release()
        await second.release()
        assert gate.active_parallel == 0

    asyncio.run(scenario())


def test_exclusive_lease_waits_for_parallel_and_blocks_new_parallel():
    async def scenario():
        gate = ModelAwareGPUPhaseGate()
        first = await gate.acquire(GPU_TRAINING_POLICY_PARALLEL)
        second = await gate.acquire(GPU_TRAINING_POLICY_PARALLEL)

        exclusive_task = asyncio.create_task(gate.acquire(GPU_TRAINING_POLICY_EXCLUSIVE))
        await asyncio.sleep(0)
        assert gate.waiting_exclusive == 1

        late_parallel_task = asyncio.create_task(gate.acquire(GPU_TRAINING_POLICY_PARALLEL))
        await asyncio.sleep(0)
        assert late_parallel_task.done() is False

        await first.release()
        await second.release()
        exclusive = await asyncio.wait_for(exclusive_task, timeout=1)
        assert gate.exclusive_active is True
        assert late_parallel_task.done() is False

        await exclusive.release()
        late_parallel = await asyncio.wait_for(late_parallel_task, timeout=1)
        assert gate.active_parallel == 1
        await late_parallel.release()

    asyncio.run(scenario())


def test_lease_release_is_idempotent():
    async def scenario():
        gate = ModelAwareGPUPhaseGate()
        lease = await gate.acquire(GPU_TRAINING_POLICY_EXCLUSIVE)

        await lease.release()
        await lease.release()

        assert gate.exclusive_active is False

    asyncio.run(scenario())
