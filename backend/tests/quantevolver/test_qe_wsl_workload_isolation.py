import hashlib
import json
from types import SimpleNamespace

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import QEWorkspaceSubmissionPayload
from backend.services.quantevolver.qe_wsl_workload_isolation import isolate_payload, memory_slot_limit, GIB


@pytest.mark.parametrize("cd", ["cd -- '/mnt/f/job with spaces'", "cd '/mnt/f/job with spaces'"])
def test_envelope_preserves_entire_scientific_command_and_frozen_inputs(cd):
    command = cd + " && exec /bin/bash -c 'python prepare_factors.py && python qrun_limit_minute.py conf.yaml'"
    p = QEWorkspaceSubmissionPayload(
        "qe_example", 2, {"model_id": "LSTM"}, {"factors/f.py": "original", "conf.yaml": "original"}, command
    )
    source = SimpleNamespace(backtest_only=False, submission_intent_hash="a" * 64)
    out = isolate_payload(p, source=source, compute_kind="gpu")
    policy = json.loads(out.experiment_files["qe_wsl_workload_policy.json"])
    assert out.experiment_files["qe_wsl_workload_command.sh"] == command
    assert policy["original_command_sha256"] == hashlib.sha256(command.encode()).hexdigest()
    assert policy["peak_bytes"] == 24 * GIB and policy["budget_bytes"] == 40 * GIB
    assert out.config == p.config and out.experiment_files["factors/f.py"] == "original"
    assert p.experiment_files == {"factors/f.py": "original", "conf.yaml": "original"}
    assert out.wsl_command.startswith("cd -- '/mnt/f/job with spaces' && exec /bin/bash --noprofile --norc -c ")
    assert "exec /usr/bin/python3 qe_wsl_workload_guard.py" in out.wsl_command
    assert "compgen -e" in out.wsl_command


def test_actual_coordinator_wraps_wsl_node_before_transport_without_altering_remote():
    import asyncio
    from unittest.mock import Mock
    from backend.services.quantevolver.qe_active_execution_capacity import QEWorkspaceSubmissionCoordinator
    from backend.tests.multi_alpha.test_qe_submission_coordinator import _source

    class CapturedTransport(Exception):
        pass

    seen = []

    class Client:
        async def submit_loop(self, *args, **kwargs):
            seen.append(args)
            raise CapturedTransport

    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=Mock())
    for node in ["wsl2-5080", "rdagent-node1"]:
        payload = QEWorkspaceSubmissionPayload(
            "qe_capture", 1, {}, {"conf.yaml": "frozen"}, "cd -- /tmp/job && exec python qrun.py"
        )
        with pytest.raises(CapturedTransport):
            asyncio.run(
                coordinator._post_with_reconciliation(
                    client=Client(),
                    source=_source(node_id=node, backtest_only=True),
                    payload=payload,
                    spec=None,
                    acquired=None,
                    reservation={},
                    token=None,
                )
            )
    assert seen[0][3]["qe_wsl_workload_command.sh"] == "cd -- /tmp/job && exec python qrun.py"
    assert "qe_wsl_workload_guard.py" in seen[0][4]
    assert seen[1][3] == {"conf.yaml": "frozen"} and seen[1][4] == "cd -- /tmp/job && exec python qrun.py"


@pytest.mark.parametrize("command", ["python train.py", "cd -- /tmp/../unsafe && exec x", "cd -- /tmp/job ; exec x"])
def test_unsupported_or_ambiguous_workspace_fails_before_post(command):
    p = QEWorkspaceSubmissionPayload("qe_example", 1, {}, {}, command)
    with pytest.raises(ValueError):
        isolate_payload(
            p, source=SimpleNamespace(backtest_only=True, submission_intent_hash="b" * 64), compute_kind=None
        )


def test_unknown_model_uses_conservative_peak_and_replay_is_separate():
    assert memory_slot_limit(None, backtest_only=False) == 1
    assert memory_slot_limit("gpu", backtest_only=False) == 1
    assert memory_slot_limit("cpu", backtest_only=False) == 4
    assert memory_slot_limit(None, backtest_only=True) == 6
    p = QEWorkspaceSubmissionPayload(
        "qe_example", 1, {}, {"qe_wsl_workload_policy.json": "existing"}, "cd -- /tmp/job && exec x"
    )
    with pytest.raises(ValueError, match="collision"):
        isolate_payload(
            p, source=SimpleNamespace(backtest_only=True, submission_intent_hash="b" * 64), compute_kind=None
        )
