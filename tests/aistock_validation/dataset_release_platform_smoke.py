#!/usr/bin/env python
"""Runner-owned platform smoke for dataset-release resource enforcement.

This script creates only a fresh temporary control/candidate pair.  It never
opens a production candidate, database, provider, exporter, service, or Worker.
Unit/fixture tests are intentionally not reported as platform evidence here.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import replace
from pathlib import Path, PureWindowsPath
from typing import Any, Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROFILE_PATH = REPOSITORY_ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml"
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.dataset_release.profile import (  # noqa: E402
    DatasetProfile,
    load_dataset_profile,
)
from backend.services.dataset_release.control_store import ControlStore  # noqa: E402
from backend.services.dataset_release.resource_budget import (  # noqa: E402
    HostTelemetrySampler,
)
from backend.services.dataset_release.resource_gate import ResourceGate  # noqa: E402
from backend.services.dataset_release.resource_supervisor import (  # noqa: E402
    ResourceSupervisor,
    WslSupervisedOptions,
)


class SmokeFailure(RuntimeError):
    pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run isolated Windows Job and optional WSL cgroup platform smoke.")
    parser.add_argument("--wsl-distro", default=os.environ.get("DATASET_RELEASE_WSL_DISTRO"))
    parser.add_argument(
        "--skip-wsl",
        action="store_true",
        help="run only Windows Job smoke; result records WSL as NOT_REQUESTED",
    )
    return parser


def _profile_for_temp(base: Path) -> tuple[DatasetProfile, Path, Path]:
    control = (base / "control").resolve()
    candidate = (base / "candidate").resolve()
    control.mkdir(parents=False, exist_ok=False)
    candidate.mkdir(parents=False, exist_ok=False)
    ControlStore.initialize(control)
    profile = load_dataset_profile(PROFILE_PATH)
    return (
        replace(
            profile,
            control_root=PureWindowsPath(str(control)),
            candidate_root=PureWindowsPath(str(candidate)),
        ),
        control,
        candidate,
    )


def _gate(profile: DatasetProfile, sampler: HostTelemetrySampler) -> ResourceGate:
    return ResourceGate(profile, host_probe=sampler, predicted_new_bytes=1024**2)


def _portable_receipt(receipt) -> dict[str, object]:
    payload = receipt.as_dict()
    gate = payload["resource_gate_receipt"]
    if not isinstance(gate, Mapping):
        raise SmokeFailure("resource gate receipt is missing")
    if (
        payload["returncode"] != 0
        or payload["active_processes"] != 0
        or gate.get("final_status") != "READY"
        or gate.get("disk_same_volume") is not True
        or gate.get("data_scope_changed") is not False
    ):
        raise SmokeFailure("supervised platform receipt failed its contract")
    return {
        "runtime": payload["runtime"],
        "returncode": payload["returncode"],
        "active_processes": payload["active_processes"],
        "job_peak_commit_bytes": payload["job_peak_commit_bytes"],
        "sample_count": gate.get("sample_count"),
        "host_min_available_bytes": gate.get("host_min_available_bytes"),
        "host_min_commit_headroom_bytes": gate.get("host_min_commit_headroom_bytes"),
        "disk_min_effective_free_bytes": gate.get("disk_min_effective_free_bytes"),
        "wsl_cgroup_peak_current_bytes": gate.get("wsl_cgroup_peak_current_bytes"),
        "wsl_min_available_bytes": gate.get("wsl_min_available_bytes"),
        "wsl_memory_events": gate.get("wsl_memory_events"),
    }


def _windows_smoke(*, profile: DatasetProfile, control: Path, sampler: HostTelemetrySampler) -> dict[str, object]:
    command = [
        sys.executable,
        "-c",
        (
            "import os,pathlib; "
            "p=pathlib.Path(os.environ['DATASET_RESOURCE_CHECKPOINT_FILE']); "
            "assert p.is_absolute(); "
            "assert 'SECRET_TOKEN' not in os.environ; "
            "assert 'DATASET_RELEASE_OPERATOR_TOKEN_FILE' not in os.environ; "
            "assert 'TUSHARE_TOKEN' not in os.environ; "
            "print('dataset-release-windows-job-smoke')"
        ),
    ]
    with ResourceSupervisor(
        attempt_id="platform-windows",
        fence=1,
        control_root=control,
        policy=profile.resource_policy,
        hybrid_wsl=False,
        resource_gate=_gate(profile, sampler),
    ) as supervisor:
        receipt = supervisor.run_supervised(
            command,
            execution_id="tiny-windows",
            cwd=REPOSITORY_ROOT,
            timeout_seconds=30,
            cooperative_grace_seconds=5,
        )
    return {"status": "PASS", "evidence": _portable_receipt(receipt)}


def _run_control(command: Sequence[str], *, timeout: float = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(part) for part in command],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def _wsl_distros() -> tuple[str, ...]:
    result = _run_control(["wsl.exe", "--list", "--quiet"])
    if result.returncode != 0:
        raise OSError(f"wsl distro list failed rc={result.returncode}")
    return tuple(
        line.replace("\x00", "").strip() for line in result.stdout.splitlines() if line.replace("\x00", "").strip()
    )


def _wsl_path(distro: str, windows_path: Path) -> str:
    portable_windows_path = str(windows_path).replace("\\", "/")
    result = _run_control(
        [
            "wsl.exe",
            "-d",
            distro,
            "--",
            "wslpath",
            "-a",
            "-u",
            portable_windows_path,
        ]
    )
    value = result.stdout.strip()
    if result.returncode != 0 or not value.startswith("/") or "\x00" in value:
        raise OSError("wslpath conversion failed")
    return value


def _wsl_python(distro: str) -> str:
    result = _run_control(
        [
            "wsl.exe",
            "-d",
            distro,
            "--",
            "/usr/bin/env",
            "python3",
            "-c",
            "import os,sys; print(os.path.realpath(sys.executable))",
        ]
    )
    value = result.stdout.strip()
    if result.returncode != 0 or not value.startswith("/"):
        raise OSError("absolute WSL python path is unavailable")
    return value


def _wsl_preflight(distro: str, profile: DatasetProfile) -> dict[str, object]:
    if distro not in _wsl_distros():
        raise OSError(f"requested WSL distro is unavailable: {distro}")
    systemd = _run_control(["wsl.exe", "-d", distro, "--", "systemctl", "--user", "is-system-running"])
    state = systemd.stdout.strip()
    if state not in {"running", "degraded"}:
        raise OSError(f"WSL user systemd is unavailable: rc={systemd.returncode} state={state!r}")
    controllers = _run_control(["wsl.exe", "-d", distro, "--", "/usr/bin/cat", "/sys/fs/cgroup/cgroup.controllers"])
    if controllers.returncode != 0 or "memory" not in controllers.stdout.split():
        raise OSError("WSL delegated cgroup v2 memory controller is unavailable")
    unit = f"aistock-dataset-smoke-preflight-{os.getpid()}-{time.time_ns() % 1_000_000}"
    transient = _run_control(
        [
            "wsl.exe",
            "-d",
            distro,
            "--",
            "systemd-run",
            "--user",
            "--wait",
            "--collect",
            "--service-type=exec",
            f"--unit={unit}.service",
            f"--property=MemoryHigh={profile.resource_policy.wsl_memory_high_bytes}",
            f"--property=MemoryMax={profile.resource_policy.wsl_memory_max_bytes}",
            f"--property=MemorySwapMax={profile.resource_policy.wsl_swap_max_bytes}",
            "--property=OOMPolicy=kill",
            "/usr/bin/true",
        ],
        timeout=30,
    )
    if transient.returncode != 0:
        tail = transient.stderr[-500:].replace("\r", " ").replace("\n", " ")
        raise OSError(f"WSL transient cgroup properties are unavailable: rc={transient.returncode} {tail}")
    return {"systemd_state": state, "memory_controller": True}


def _wsl_smoke(
    *,
    distro: str,
    profile: DatasetProfile,
    control: Path,
    sampler: HostTelemetrySampler,
) -> dict[str, object]:
    preflight = _wsl_preflight(distro, profile)
    python_wsl = _wsl_python(distro)
    repo_wsl = _wsl_path(distro, REPOSITORY_ROOT)
    control_wsl = _wsl_path(distro, control)
    attempt_id = "platform-wsl"
    fence = 2
    execution_id = "tiny-wsl"
    execution_root_wsl = f"{control_wsl.rstrip('/')}/attempt_runs/{attempt_id}-{fence}/{execution_id}"
    heartbeat_wsl = f"{control_wsl.rstrip('/')}/heartbeats/{attempt_id}-{fence}.json"
    options = WslSupervisedOptions(
        distro=distro,
        guardian_python=python_wsl,
        guardian_script_wsl=(f"{repo_wsl}/backend/services/dataset_release/wsl_resource_guardian.py"),
        heartbeat_path_wsl=heartbeat_wsl,
        runner_python_wsl=python_wsl,
        runner_script_wsl=(f"{repo_wsl}/backend/services/dataset_release/subprocess_runner.py"),
        task_cwd_wsl=repo_wsl,
        execution_root_wsl=execution_root_wsl,
    )
    command = [
        python_wsl,
        "-c",
        (
            "import os,pathlib,time; "
            "p=pathlib.Path(os.environ['DATASET_RESOURCE_CHECKPOINT_FILE']); "
            "assert p.is_absolute(); assert 'SECRET_TOKEN' not in os.environ; "
            "assert 'DATASET_RELEASE_OPERATOR_TOKEN_FILE' not in os.environ; "
            "assert 'TUSHARE_TOKEN' not in os.environ; "
            "print('dataset-release-wsl-cgroup-smoke'); "
            "time.sleep(2)"
        ),
    ]
    with ResourceSupervisor(
        attempt_id=attempt_id,
        fence=fence,
        control_root=control,
        policy=profile.resource_policy,
        hybrid_wsl=True,
        resource_gate=_gate(profile, sampler),
    ) as supervisor:
        receipt = supervisor.run_supervised(
            command,
            execution_id=execution_id,
            cwd=REPOSITORY_ROOT,
            runtime="wsl",
            timeout_seconds=45,
            cooperative_grace_seconds=10,
            wsl=options,
        )
    evidence = _portable_receipt(receipt)
    if not isinstance(evidence.get("wsl_memory_events"), Mapping):
        raise SmokeFailure("WSL memory.events did not reach the parent receipt")
    return {"status": "PASS", "preflight": preflight, "evidence": evidence}


def _exception_evidence(exc: BaseException) -> dict[str, str]:
    chain: list[str] = []
    current: BaseException | None = exc
    while current is not None and len(chain) < 4:
        chain.append(f"{type(current).__name__}:{str(current)[:500]}")
        current = current.__cause__
    return {"exception_chain": " <- ".join(chain)}


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if os.name != "nt":
        print(
            json.dumps(
                {
                    "schema_version": "dataset_release_platform_smoke_v1",
                    "ok": False,
                    "windows": {"status": "BLOCKED_BY_ENV", "reason": "requires Windows"},
                    "wsl": {"status": "NOT_REQUESTED"},
                },
                sort_keys=True,
            )
        )
        return 2
    with tempfile.TemporaryDirectory(prefix="aistock-dataset-release-smoke-") as raw:
        base = Path(raw).resolve(strict=True)
        profile, control, _candidate = _profile_for_temp(base)
        sampler: HostTelemetrySampler | None = None
        result: dict[str, Any] = {
            "schema_version": "dataset_release_platform_smoke_v1",
            "safety": {
                "temporary_root": str(base),
                "database_access": 0,
                "provider_access": 0,
                "dataset_exports": 0,
                "production_writes": 0,
                "service_controls": 0,
            },
        }
        injected_environment = {
            "SECRET_TOKEN": "platform-smoke-unrelated-secret",
            "DATASET_RELEASE_OPERATOR_TOKEN_FILE": str(base / "operator-token.txt"),
            "TUSHARE_TOKEN": "platform-smoke-source-only-secret",
        }
        previous_environment = {key: os.environ.get(key) for key in injected_environment}
        os.environ.update(injected_environment)
        try:
            sampler = HostTelemetrySampler()
            try:
                result["windows"] = _windows_smoke(profile=profile, control=control, sampler=sampler)
            except BaseException as exc:
                result["windows"] = {
                    "status": "FAIL",
                    **_exception_evidence(exc),
                }
            if args.skip_wsl:
                result["wsl"] = {"status": "NOT_REQUESTED"}
            else:
                distro = args.wsl_distro or "Ubuntu"
                try:
                    result["wsl"] = _wsl_smoke(
                        distro=distro,
                        profile=profile,
                        control=control,
                        sampler=sampler,
                    )
                except OSError as exc:
                    result["wsl"] = {
                        "status": "BLOCKED_BY_ENV",
                        "distro": distro,
                        **_exception_evidence(exc),
                    }
                except BaseException as exc:
                    result["wsl"] = {
                        "status": "FAIL",
                        "distro": distro,
                        **_exception_evidence(exc),
                    }
        finally:
            if sampler is not None:
                sampler.close()
            for key, previous in previous_environment.items():
                if previous is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous
        result["ok"] = result.get("windows", {}).get("status") == "PASS" and result.get("wsl", {}).get("status") in {
            "PASS",
            "BLOCKED_BY_ENV",
            "NOT_REQUESTED",
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
