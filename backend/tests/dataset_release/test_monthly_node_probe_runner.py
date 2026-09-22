from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any, Mapping

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerValidationError,
)
from backend.services.dataset_release.monthly_node_probe import (
    NODE_PROBE_REQUEST_SCHEMA,
    NODE_PROBE_RESULT_SCHEMA,
)
from backend.services.dataset_release.monthly_node_probe_runner import (
    CodeOwnedSubprocessNodeProbeRunner,
    MONTHLY_CONSUMER_NODES,
    NodeAttestedMonthlyConsumerProbe,
    build_node_attested_consumer_validation_executor,
    build_ssh_node_probe_runner,
    build_wsl_node_probe_runner,
)
from backend.services.dataset_release.monthly_unified import REQUIRED_CONSUMERS


MANIFEST = "a" * 64


def _json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _binding(tmp_path: Path) -> tuple[Path, Path]:
    manifest = _json(
        tmp_path / "candidate" / "qe_dataset_manifest.json",
        {"dataset_manifest_sha256": MANIFEST},
    )
    universe = tmp_path / "candidate" / "stock_pools" / "stock_universe.txt"
    universe.parent.mkdir(parents=True)
    universe.write_text("000001.SZ\n", encoding="utf-8")
    binding = _json(
        tmp_path / "attempt" / "factor_research-binding.json",
        {
            "resolved_component_refs": [
                {
                    "id": "qe_dataset_manifest.json",
                    "sha256": _sha(manifest),
                    "size": manifest.stat().st_size,
                },
                {
                    "id": "stock_pools/stock_universe.txt",
                    "sha256": _sha(universe),
                    "size": universe.stat().st_size,
                },
            ],
            "derived_asset_refs": [],
        },
    )
    return binding, manifest.parent


def _request(tmp_path: Path) -> ConsumerProbeRequest:
    binding, candidate = _binding(tmp_path)
    return ConsumerProbeRequest(
        consumer_id="factor_research",
        node_id="rdagent-node1",
        dataset_manifest_sha256=MANIFEST,
        binding_path=binding,
        profile_path=tmp_path / "profile.json",
        controller_candidate_root=candidate,
        node_candidate_root="/home/lc999/data/releases/candidate",
        resolved_component_paths=(),
        derived_asset_paths=(),
    )


@dataclass
class ControllerProbe:
    root: Path
    probe_id: str = "controller.factor-research"
    probe_version: str = "1"

    def run(self, request: ConsumerProbeRequest) -> Path:
        return _json(
            self.root / "controller.json",
            {
                "schema_version": CONSUMER_PROBE_RESULT_SCHEMA,
                "status": "PASS",
                "consumer_id": request.consumer_id,
                "node_id": request.node_id,
                "dataset_manifest_sha256": request.dataset_manifest_sha256,
                "binding_sha256": _sha(request.binding_path),
                "required_window": {
                    "test_start": "2024-07-01",
                    "test_end": "2026-08-31",
                    "backtest_end": "2026-08-28",
                },
                "coverage_counts": {"unresolved_count": 0, "controller_files": 2},
                "adapter": {"id": self.probe_id, "version": self.probe_version},
                "evidence_refs": [{"id": "binding.json", "sha256": "f" * 64, "size": 1}],
                "side_effect_flags": {
                    "outcomes_read": False,
                    "training_started": False,
                    "experiment_started": False,
                    "runtime_action_performed": False,
                },
            },
        )


@dataclass
class FakeRunner:
    node_id: str = "rdagent-node1"
    runner_id: str = "fake.remote"
    runner_version: str = "1"
    drift: str | None = None

    def run(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        result: dict[str, Any] = {
            "schema_version": NODE_PROBE_RESULT_SCHEMA,
            "status": "PASS",
            "consumer_id": request["consumer_id"],
            "node_id": request["node_id"],
            "candidate_root": request["candidate_root"],
            "dataset_manifest_sha256": request["dataset_manifest_sha256"],
            "request_sha256": hashlib.sha256(canonical_json_bytes(request)).hexdigest(),
            "coverage_counts": {"unresolved_count": 0, "verified_file_count": 2},
            "file_refs_sha256": hashlib.sha256(
                canonical_json_bytes({"file_refs": request["file_refs"]})
            ).hexdigest(),
            "side_effect_flags": {
                "database_access": False,
                "outcomes_read": False,
                "training_started": False,
                "experiment_started": False,
                "runtime_action_performed": False,
                "silent_fallback": False,
            },
        }
        if self.drift == "node":
            result["node_id"] = "wsl2-5080"
        if self.drift == "side_effect":
            result["side_effect_flags"]["database_access"] = True
        return result


def test_subprocess_runner_sends_canonical_stdin_without_shell() -> None:
    observed: dict[str, Any] = {}
    request = {
        "schema_version": NODE_PROBE_REQUEST_SCHEMA,
        "consumer_id": "factor_research",
    }
    response = {"status": "PASS"}

    def execute(command, *, payload, timeout_seconds):  # type: ignore[no-untyped-def]
        observed.update(command=command, payload=payload, timeout=timeout_seconds)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=canonical_json_bytes(response) + b"\n",
            stderr=b"",
        )

    runner = CodeOwnedSubprocessNodeProbeRunner(
        node_id="wsl2-5080",
        command=("python", "-m", "probe"),
        runner_id="test.runner",
        executor=execute,
    )

    assert runner.run(request) == response
    assert observed == {
        "command": ("python", "-m", "probe"),
        "payload": canonical_json_bytes(request) + b"\n",
        "timeout": 1_800,
    }


def test_subprocess_runner_rejects_noncanonical_stdout() -> None:
    def execute(command, *, payload, timeout_seconds):  # type: ignore[no-untyped-def]
        del payload, timeout_seconds
        return subprocess.CompletedProcess(command, 0, stdout=b'{"status": "PASS"}\n', stderr=b"")

    runner = CodeOwnedSubprocessNodeProbeRunner(
        node_id="wsl2-5080",
        command=("python",),
        runner_id="test.runner",
        executor=execute,
    )
    with pytest.raises(MonthlyConsumerValidationError, match="not canonical"):
        runner.run({"value": 1})


def test_code_owned_wsl_and_ssh_commands_are_fixed() -> None:
    wsl = build_wsl_node_probe_runner(
        distro="Ubuntu-24.04",
        project_root="/mnt/f/Dev/AIstock",
        python_executable="/opt/conda/envs/rdagent-gpu/bin/python",
    )
    ssh = build_ssh_node_probe_runner(
        host="rdagent-node1",
        project_root="/home/lc999/AIstock",
        python_executable="/home/lc999/miniconda3/envs/rdagent-gpu/bin/python",
    )

    assert wsl.command[:4] == ("wsl.exe", "-d", "Ubuntu-24.04", "--cd")
    assert wsl.command[-2:] == (
        "-m",
        "backend.services.dataset_release.monthly_node_probe",
    )
    assert ssh.command[:7] == (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=30",
        "rdagent-node1",
        "cd -- /home/lc999/AIstock && exec /home/lc999/miniconda3/envs/rdagent-gpu/bin/python -m backend.services.dataset_release.monthly_node_probe",
    )


def test_node_attested_probe_closes_controller_and_remote_identity(tmp_path: Path) -> None:
    request = _request(tmp_path)
    probe = NodeAttestedMonthlyConsumerProbe(
        controller_probe=ControllerProbe(tmp_path / "controller"),
        runner=FakeRunner(),
    )

    result_path = probe.run(request)
    result = json.loads(result_path.read_text(encoding="utf-8"))

    assert result["status"] == "PASS"
    assert result["adapter"] == {
        "id": "aistock.monthly.consumer.node_attested",
        "version": "1",
    }
    assert result["coverage_counts"] == {
        "unresolved_count": 0,
        "controller_controller_files": 2,
        "node_verified_file_count": 2,
    }
    assert len(result["evidence_refs"]) == 2


@pytest.mark.parametrize("drift", ["node", "side_effect"])
def test_node_attested_probe_rejects_remote_drift(tmp_path: Path, drift: str) -> None:
    probe = NodeAttestedMonthlyConsumerProbe(
        controller_probe=ControllerProbe(tmp_path / "controller"),
        runner=FakeRunner(drift=drift),
    )
    with pytest.raises(MonthlyConsumerValidationError, match="identity differs"):
        probe.run(_request(tmp_path))


def test_node_attested_registry_is_exact_and_requires_both_nodes(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    runners = {
        "wsl2-5080": FakeRunner(node_id="wsl2-5080"),
        "rdagent-node1": FakeRunner(),
    }

    executor = build_node_attested_consumer_validation_executor(
        artifact_root=artifact_root,
        runners=runners,
    )

    assert tuple(executor.probes) == REQUIRED_CONSUMERS
    assert executor.consumer_nodes == MONTHLY_CONSUMER_NODES
    assert set(executor.consumer_nodes.values()) == {"wsl2-5080", "rdagent-node1"}
    with pytest.raises(ValueError, match="registry is incomplete"):
        build_node_attested_consumer_validation_executor(
            artifact_root=artifact_root,
            runners={"wsl2-5080": runners["wsl2-5080"]},
        )
