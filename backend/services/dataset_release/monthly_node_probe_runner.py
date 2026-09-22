"""Remote node execution and attestation for monthly consumer probes."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes
from .monthly_consumer_registry import monthly_controller_preflight_probes
from .monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerProbe,
    MonthlyConsumerValidationError,
    RegisteredMonthlyConsumerValidationExecutor,
)
from .monthly_node_probe import (
    NODE_PROBE_REQUEST_SCHEMA,
    NODE_PROBE_RESULT_SCHEMA,
)
from .monthly_unified import REQUIRED_CONSUMERS


_RUNNER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_HOST_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,255}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_NODE_RESULT_FIELDS = {
    "schema_version",
    "status",
    "consumer_id",
    "node_id",
    "candidate_root",
    "dataset_manifest_sha256",
    "request_sha256",
    "coverage_counts",
    "file_refs_sha256",
    "side_effect_flags",
}
_NODE_FLAGS = {
    "database_access",
    "outcomes_read",
    "training_started",
    "experiment_started",
    "runtime_action_performed",
    "silent_fallback",
}
MONTHLY_CONSUMER_NODES: Mapping[str, str] = MappingProxyType(
    {
        "qe_single": "wsl2-5080",
        "qe_custom": "rdagent-node1",
        "qe_multi_alpha": "rdagent-node1",
        "qe_p10": "wsl2-5080",
        "qe_p11": "rdagent-node1",
        "hmm_file_only": "wsl2-5080",
        "factor_research": "rdagent-node1",
        "selection": "wsl2-5080",
        "advisory": "rdagent-node1",
        "position_timing": "wsl2-5080",
        "unified_backtest": "rdagent-node1",
    }
)
_HMM_PATH_ROLES = {
    "components/factor_h5_static_candidate_v2/sector_data.h5": "sector_data_h5",
    "components/index_context/index_daily.h5": "index_daily_h5",
    "components/sector_context_candidate_v1/sector_code_map.json": "sector_code_map_json",
    "components/sector_context_candidate_v1/market_context.parquet": "market_context_parquet",
    "components/sector_context_candidate_v1/sector_membership_spans.parquet": "sector_membership_spans_parquet",
    "components/sector_context_candidate_v1/sector_quote_availability.json": "sector_quote_availability_json",
}
_SHARED_PATHS = {
    "qe_dataset_manifest.json",
    "components/daily_bin_candidate/calendars/day.txt",
    "components/daily_bin_candidate/instruments/all.txt",
    "components/daily_bin_candidate/meta_export.json",
    "components/minute_bin_candidate/calendars/1min.txt",
    "components/minute_bin_candidate/instruments/all.txt",
    "components/minute_bin_candidate/meta_export.json",
    "components/factor_h5_static_candidate_v2/meta.json",
    "components/factor_h5_static_candidate_v2/static_factors.parquet",
    "components/index_context/meta.json",
    "components/index_context/index_daily.h5",
    "components/suspend_d_daily_candidate_v2/meta.json",
    "components/suspend_d_daily_candidate_v2/suspend_d.parquet",
    "components/daily_bin_candidate/instruments/benchmark.txt",
    "reports/qe_index_pool_coverage_receipt.json",
    "stock_pools/stock_universe.txt",
    "components/sector_context_candidate_v1/sector_code_map.json",
    "components/sector_context_candidate_v1/sector_membership_spans.parquet",
    "components/sector_context_candidate_v1/sector_quote_availability.json",
}


class MonthlyNodeProbeRunner(Protocol):
    node_id: str
    runner_id: str
    runner_version: str

    def run(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


def _run_subprocess(
    command: Sequence[str],
    *,
    payload: bytes,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        list(command),
        input=payload,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout_seconds,
        shell=False,
    )


@dataclass(frozen=True, slots=True)
class CodeOwnedSubprocessNodeProbeRunner:
    node_id: str
    command: tuple[str, ...]
    runner_id: str
    runner_version: str = "1"
    timeout_seconds: int = 1_800
    executor: Callable[..., subprocess.CompletedProcess[bytes]] = _run_subprocess

    def __post_init__(self) -> None:
        if self.node_id not in {"wsl2-5080", "rdagent-node1"}:
            raise ValueError("monthly node runner id is invalid")
        if (
            not self.command
            or any(not isinstance(item, str) or not item or "\x00" in item for item in self.command)
            or _RUNNER_ID_RE.fullmatch(self.runner_id) is None
            or _RUNNER_ID_RE.fullmatch(self.runner_version) is None
            or type(self.timeout_seconds) is not int
            or self.timeout_seconds < 60
        ):
            raise ValueError("monthly node runner configuration is invalid")

    def run(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        payload = canonical_json_bytes(request) + b"\n"
        completed = self.executor(
            self.command,
            payload=payload,
            timeout_seconds=self.timeout_seconds,
        )
        if completed.returncode != 0:
            message = completed.stderr.decode("utf-8", errors="replace").strip()[-2_000:]
            raise MonthlyConsumerValidationError(
                f"monthly node probe failed: {self.node_id}: {message}"
            )
        try:
            value = json.loads(completed.stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MonthlyConsumerValidationError("monthly node probe returned invalid JSON") from exc
        if completed.stdout != canonical_json_bytes(value) + b"\n" or not isinstance(value, Mapping):
            raise MonthlyConsumerValidationError("monthly node probe result is not canonical")
        return dict(value)


def _posix_path(value: str, *, field: str) -> str:
    path = PurePosixPath(value)
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise ValueError(f"{field} must be a canonical absolute POSIX path")
    return str(path)


def build_wsl_node_probe_runner(
    *,
    distro: str,
    project_root: str,
    python_executable: str,
) -> CodeOwnedSubprocessNodeProbeRunner:
    if _RUNNER_ID_RE.fullmatch(distro) is None:
        raise ValueError("WSL distro is invalid")
    project = _posix_path(project_root, field="project_root")
    python = _posix_path(python_executable, field="python_executable")
    return CodeOwnedSubprocessNodeProbeRunner(
        node_id="wsl2-5080",
        command=(
            "wsl.exe",
            "-d",
            distro,
            "--cd",
            project,
            "--",
            python,
            "-m",
            "backend.services.dataset_release.monthly_node_probe",
        ),
        runner_id="aistock.monthly.node_probe.wsl",
    )


def build_ssh_node_probe_runner(
    *,
    host: str,
    project_root: str,
    python_executable: str,
) -> CodeOwnedSubprocessNodeProbeRunner:
    if _HOST_RE.fullmatch(host) is None:
        raise ValueError("SSH host is invalid")
    project = _posix_path(project_root, field="project_root")
    python = _posix_path(python_executable, field="python_executable")
    remote = "cd -- " + shlex.quote(project) + " && exec " + shlex.quote(python) + (
        " -m backend.services.dataset_release.monthly_node_probe"
    )
    return CodeOwnedSubprocessNodeProbeRunner(
        node_id="rdagent-node1",
        command=(
            "ssh",
            "-F",
            "NUL",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=30",
            host,
            remote,
        ),
        runner_id="aistock.monthly.node_probe.ssh",
    )


def _path_role(path: str, consumer_id: str) -> str | None:
    if path == "qe_dataset_manifest.json":
        return "dataset_manifest"
    if consumer_id == "hmm_file_only":
        return _HMM_PATH_ROLES.get(path)
    if path not in _SHARED_PATHS:
        return None
    return "sentinel_" + re.sub(r"[^a-z0-9]+", "_", path.lower()).strip("_")


def _node_request(request: ConsumerProbeRequest, window: Mapping[str, Any]) -> dict[str, Any]:
    binding = json.loads(request.binding_path.read_text(encoding="utf-8"))
    component_refs = binding.get("resolved_component_refs")
    derived_refs = binding.get("derived_asset_refs")
    if not isinstance(component_refs, list) or not isinstance(derived_refs, list):
        raise MonthlyConsumerValidationError("node probe binding file refs are unavailable")
    selected: list[dict[str, Any]] = []
    observed_roles: set[str] = set()
    for raw in [*component_refs, *derived_refs]:
        if not isinstance(raw, Mapping):
            raise MonthlyConsumerValidationError("node probe binding ref is invalid")
        path = str(raw.get("id") or "")
        role = _path_role(path, request.consumer_id)
        if role is None and request.consumer_id in {"qe_p10", "qe_p11"} and (
            path.startswith("derived/") and "preset_A" in Path(path).name
        ):
            role = "hmm_coefficients_preset_a"
        if role is None:
            continue
        if role in observed_roles:
            raise MonthlyConsumerValidationError(f"node probe role is duplicated: {role}")
        digest = str(raw.get("sha256") or "")
        size = raw.get("size")
        if _SHA_RE.fullmatch(digest) is None or type(size) is not int or size <= 0:
            raise MonthlyConsumerValidationError("node probe binding ref identity is invalid")
        selected.append({"role": role, "path": path, "sha256": digest, "size": size})
        observed_roles.add(role)
    required_roles = {"dataset_manifest"}
    if request.consumer_id == "hmm_file_only":
        required_roles.update(_HMM_PATH_ROLES.values())
    if request.consumer_id in {"qe_p10", "qe_p11"}:
        required_roles.add("hmm_coefficients_preset_a")
    if not required_roles.issubset(observed_roles):
        raise MonthlyConsumerValidationError(
            f"node probe refs are incomplete: {sorted(required_roles - observed_roles)}"
        )
    return {
        "schema_version": NODE_PROBE_REQUEST_SCHEMA,
        "consumer_id": request.consumer_id,
        "node_id": request.node_id,
        "candidate_root": request.node_candidate_root,
        "dataset_manifest_sha256": request.dataset_manifest_sha256,
        "required_window": dict(window),
        "file_refs": selected,
    }


def _validate_node_result(
    value: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
) -> dict[str, int]:
    counts = value.get("coverage_counts")
    flags = value.get("side_effect_flags")
    if (
        set(value) != _NODE_RESULT_FIELDS
        or value.get("schema_version") != NODE_PROBE_RESULT_SCHEMA
        or value.get("status") != "PASS"
        or value.get("consumer_id") != request["consumer_id"]
        or value.get("node_id") != request["node_id"]
        or value.get("candidate_root") != request["candidate_root"]
        or value.get("dataset_manifest_sha256") != request["dataset_manifest_sha256"]
        or value.get("request_sha256") != hashlib.sha256(canonical_json_bytes(request)).hexdigest()
        or value.get("file_refs_sha256")
        != hashlib.sha256(canonical_json_bytes({"file_refs": request["file_refs"]})).hexdigest()
        or not isinstance(counts, Mapping)
        or counts.get("unresolved_count") != 0
        or any(type(item) is not int or item < 0 for item in counts.values())
        or not isinstance(flags, Mapping)
        or set(flags) != _NODE_FLAGS
        or any(flags[name] is not False for name in _NODE_FLAGS)
    ):
        raise MonthlyConsumerValidationError("monthly node probe result identity differs")
    return dict(counts)


def _write_canonical(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        if not path.is_file() or path.is_symlink() or path.read_bytes() != payload:
            raise MonthlyConsumerValidationError("node-attested probe output bytes differ") from exc
    return path


@dataclass(frozen=True, slots=True)
class NodeAttestedMonthlyConsumerProbe:
    controller_probe: MonthlyConsumerProbe
    runner: MonthlyNodeProbeRunner
    probe_id: str = "aistock.monthly.consumer.node_attested"
    probe_version: str = "1"

    def __post_init__(self) -> None:
        if self.runner.node_id not in {"wsl2-5080", "rdagent-node1"}:
            raise ValueError("node-attested probe runner is invalid")

    def run(self, request: ConsumerProbeRequest) -> Path:
        if request.node_id != self.runner.node_id:
            raise MonthlyConsumerValidationError("consumer node and node runner differ")
        controller_path = self.controller_probe.run(request)
        controller = json.loads(controller_path.read_text(encoding="utf-8"))
        window = controller.get("required_window")
        controller_counts = controller.get("coverage_counts")
        if (
            not isinstance(window, Mapping)
            or not isinstance(controller_counts, Mapping)
            or controller_counts.get("unresolved_count") != 0
        ):
            raise MonthlyConsumerValidationError("controller preflight result is incomplete")
        node_request = _node_request(request, window)
        node_result = dict(self.runner.run(node_request))
        node_counts = _validate_node_result(node_result, request=node_request)
        node_path = _write_canonical(
            request.binding_path.parent / f"{request.consumer_id}-{request.node_id}-node-result.json",
            node_result,
        )
        coverage = {
            "unresolved_count": 0,
            **{
                f"controller_{key}": int(value)
                for key, value in controller_counts.items()
                if key != "unresolved_count"
            },
            **{
                f"node_{key}": int(value)
                for key, value in node_counts.items()
                if key != "unresolved_count"
            },
        }
        result = {
            "schema_version": CONSUMER_PROBE_RESULT_SCHEMA,
            "status": "PASS",
            "consumer_id": request.consumer_id,
            "node_id": request.node_id,
            "dataset_manifest_sha256": request.dataset_manifest_sha256,
            "binding_sha256": hashlib.sha256(request.binding_path.read_bytes()).hexdigest(),
            "required_window": dict(window),
            "coverage_counts": coverage,
            "adapter": {"id": self.probe_id, "version": self.probe_version},
            "evidence_refs": [
                {
                    "id": controller_path.name,
                    "sha256": hashlib.sha256(controller_path.read_bytes()).hexdigest(),
                    "size": controller_path.stat().st_size,
                },
                {
                    "id": node_path.name,
                    "sha256": hashlib.sha256(node_path.read_bytes()).hexdigest(),
                    "size": node_path.stat().st_size,
                },
            ],
            "side_effect_flags": {
                "outcomes_read": False,
                "training_started": False,
                "experiment_started": False,
                "runtime_action_performed": False,
            },
        }
        return _write_canonical(
            request.binding_path.parent / f"{request.consumer_id}-node-attested-result.json",
            result,
        )


def build_node_attested_consumer_validation_executor(
    *,
    artifact_root: Path,
    runners: Mapping[str, MonthlyNodeProbeRunner],
) -> RegisteredMonthlyConsumerValidationExecutor:
    if set(runners) != {"wsl2-5080", "rdagent-node1"} or any(
        runner.node_id != node_id for node_id, runner in runners.items()
    ):
        raise ValueError("monthly node runner registry is incomplete")
    controller = monthly_controller_preflight_probes()
    probes = {
        consumer_id: NodeAttestedMonthlyConsumerProbe(
            controller_probe=controller[consumer_id],
            runner=runners[MONTHLY_CONSUMER_NODES[consumer_id]],
        )
        for consumer_id in REQUIRED_CONSUMERS
    }
    return RegisteredMonthlyConsumerValidationExecutor(
        artifact_root=artifact_root,
        probes=probes,
        consumer_nodes=MONTHLY_CONSUMER_NODES,
    )


__all__: Sequence[str] = (
    "CodeOwnedSubprocessNodeProbeRunner",
    "MONTHLY_CONSUMER_NODES",
    "MonthlyNodeProbeRunner",
    "NodeAttestedMonthlyConsumerProbe",
    "build_node_attested_consumer_validation_executor",
    "build_ssh_node_probe_runner",
    "build_wsl_node_probe_runner",
)
