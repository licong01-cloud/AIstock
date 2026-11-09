"""Prepare exact WSL resource units. Never restarts services or migrates PIDs."""

from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

UNITS = {
    "/etc/systemd/system/qe-control.slice": "[Unit]\nDescription=QE control API only\n\n[Slice]\nMemoryAccounting=yes\nMemoryHigh=infinity\nMemoryMax=2G\nMemorySwapMax=1G\nMemoryLow=512M\n",
    "/etc/systemd/system/rdagent-qe-api.service.d/70-qe-control.conf": "[Service]\nSlice=qe-control.slice\nMemoryAccounting=yes\nMemoryHigh=infinity\nMemoryMax=2G\nMemorySwapMax=1G\nMemoryLow=512M\n",
    "/etc/systemd/user/qe-training.slice": "[Unit]\nDescription=QE isolated workloads; API excluded\n\n[Slice]\nMemoryAccounting=yes\nMemoryHigh=infinity\nMemoryMax=40G\nMemorySwapMax=8G\n",
}


def plan():
    return {
        "schema": "qe_wsl_isolation_deployment_v1",
        "unit_files": UNITS,
        "precondition": "WSL9000 API inactive, MainPID0, original service file hash matched; all existing workers already finished and reconciled",
        "backend_activation_order": [
            "prepare source and exact units",
            "finish/reconcile old workers",
            "user stops WSL9000 API",
            "install units with expected service SHA",
            "user reloads system and user systemd managers and starts WSL9000 API",
            "verify API/control cgroup and training slice",
            "user restarts local8001 to load wrapped submissions",
        ],
        "limits": {
            "api_memory_gib": 2,
            "training_memory_gib": 40,
            "training_swap_gib": 8,
            "soft_limits": "infinity",
            "gpu_peak_gib": 24,
            "cpu_peak_gib": 10,
            "replay_peak_gib": 6,
        },
        "migration_of_active_processes": False,
        "service_control": False,
        "docker_or_database_operations": False,
    }


def install(expected_service_sha, *, root=Path("/"), runner=subprocess.run):
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("qe_wsl_install_requires_explicit_WSL_root")
    service = root / "etc/systemd/system/rdagent-qe-api.service"
    if hashlib.sha256(service.read_bytes()).hexdigest() != expected_service_sha:
        raise RuntimeError("qe_wsl_api_unit_changed_replan_required")
    r = runner(
        ["systemctl", "show", "rdagent-qe-api.service", "--property=ActiveState,MainPID"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    values = dict(s.split("=", 1) for s in r.stdout.splitlines() if "=" in s)
    if values.get("ActiveState") not in {"inactive", "failed"} or values.get("MainPID") != "0":
        raise RuntimeError("qe_wsl_api_or_workers_active_no_changes")
    old_group = root / "sys/fs/cgroup/qe.slice/qe-execution.slice/rdagent-qe-api.service"
    if old_group.exists() and any(p.read_text().strip() for p in old_group.rglob("cgroup.procs")):
        raise RuntimeError("qe_wsl_existing_workers_not_drained")
    for name, text in UNITS.items():
        path = root / name.lstrip("/")
        if any(parent.is_symlink() for parent in [path, *path.parents] if parent != root) or (
            path.exists() and path.read_bytes() != text.encode()
        ):
            raise RuntimeError("qe_wsl_configuration_conflict: " + name)
    for name, text in UNITS.items():
        path = root / name.lstrip("/")
        if path.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(text.encode())
    if any((root / name.lstrip("/")).read_bytes() != text.encode() for name, text in UNITS.items()):
        raise RuntimeError("qe_wsl_configuration_changed_during_install")
    return {
        **plan(),
        "state": "UNIT_FILES_INSTALLED_USER_RELOAD_AND_START_PENDING",
        "readback": {name: hashlib.sha256((root / name.lstrip("/")).read_bytes()).hexdigest() for name in UNITS},
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    p.add_argument("--expected-api-unit-sha256")
    args = p.parse_args()
    if args.apply and not args.expected_api_unit_sha256:
        p.error("--apply requires the inspected --expected-api-unit-sha256")
    result = install(args.expected_api_unit_sha256) if args.apply else plan()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
