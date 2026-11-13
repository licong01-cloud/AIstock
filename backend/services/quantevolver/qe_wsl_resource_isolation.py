"""Prepare exact WSL resource units. Never restarts services or migrates PIDs."""

from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

if __package__:
    from .qe_wsl_workload_guard import GIB, MEMORY_BYTES, SWAP_BYTES, JOB_SWAP_BYTES, PEAK_BYTES, SCHEMA
else:  # Standalone deployment uses the inspected adjacent standard-library-only guard.
    import importlib.util

    _spec = importlib.util.spec_from_file_location("qe_resource_profile", Path(__file__).with_name("qe_wsl_workload_guard.py"))
    _profile = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_profile)
    GIB, MEMORY_BYTES, SWAP_BYTES, JOB_SWAP_BYTES, PEAK_BYTES, SCHEMA = (
        getattr(_profile, name) for name in ("GIB", "MEMORY_BYTES", "SWAP_BYTES", "JOB_SWAP_BYTES", "PEAK_BYTES", "SCHEMA")
    )

UNITS = {
    "/etc/systemd/system/qe-control.slice": "[Unit]\nDescription=QE control API only\n\n[Slice]\nMemoryAccounting=yes\nMemoryHigh=infinity\nMemoryMax=2G\nMemorySwapMax=1G\nMemoryLow=512M\n",
    "/etc/systemd/system/rdagent-qe-api.service.d/70-qe-control.conf": "[Service]\nSlice=qe-control.slice\nMemoryAccounting=yes\nMemoryHigh=infinity\nMemoryMax=2G\nMemorySwapMax=1G\nMemoryLow=512M\n",
    "/etc/systemd/user/qe-training.slice": "[Unit]\nDescription=QE isolated workloads; API excluded\n\n[Slice]\nMemoryAccounting=yes\nMemoryHigh=infinity\nMemoryMax=56G\nMemorySwapMax=32G\n",
}
TRAINING_UNIT = "/etc/systemd/user/qe-training.slice"
LEGACY_TRAINING_UNIT = UNITS[TRAINING_UNIT].replace("MemoryMax=56G", "MemoryMax=40G").replace("MemorySwapMax=32G", "MemorySwapMax=8G")


def plan():
    return {
        "schema": "qe_wsl_isolation_deployment_v2",
        "workload_schema": SCHEMA,
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
            "training_memory_gib": MEMORY_BYTES // GIB,
            "training_swap_gib": SWAP_BYTES // GIB,
            "job_swap_gib": JOB_SWAP_BYTES // GIB,
            "soft_limits": "infinity",
            **{kind + "_peak_gib": value // GIB for kind, value in PEAK_BYTES.items()},
        },
        "migration_of_active_processes": False,
        "service_control": False,
        "docker_or_database_operations": False,
        "existing_isolation_upgrade": {
            "target": TRAINING_UNIT,
            "api_restart_required_for_units": False,
            "precondition": "All old scopes and waiting guards finished; inspect target SHA. Never rewrite frozen job envelopes.",
            "steps": [
                "inspect existing training unit SHA and drain/reconcile old workers",
                "run --upgrade-training-profile --apply --expected-training-unit-sha256 <SHA>",
                "operator reloads only the lc999 user systemd manager and applies MemoryHigh=infinity MemoryMax=56G MemorySwapMax=32G to qe-training.slice",
                "read back all three training limits; API/control limits stay2G/1G",
                "user restarts local8001 to load source; WSL9000 need not restart",
                "fresh Composer confirms v2 envelope, original command SHA and two/four capacity",
                "verify real overlap/throughput using the next authorized jobs; no duplicate training for this purpose",
            ],
        },
    }


def upgrade_training_profile(expected_sha, *, root=Path("/")):
    """CAS only the exact training unit after drain; no daemon reload or process control."""
    if sys.platform != "linux" or os.geteuid() != 0:
        raise RuntimeError("qe_wsl_install_requires_explicit_WSL_root")
    path = root / TRAINING_UNIT.lstrip("/")
    if any(p.is_symlink() for p in [path, *path.parents] if p != root):
        raise RuntimeError("qe_wsl_training_unit_symlink")
    before = path.read_bytes()
    if hashlib.sha256(before).hexdigest() != expected_sha:
        raise RuntimeError("qe_wsl_training_unit_changed_replan_required")
    if before not in (LEGACY_TRAINING_UNIT.encode(), UNITS[TRAINING_UNIT].encode()):
        raise RuntimeError("qe_wsl_unknown_training_unit_no_overwrite")
    user_tree = root / "sys/fs/cgroup/user.slice"
    for group in user_tree.glob("user-*.slice/user@*.service/qe.slice/qe-training.slice"):
        if any(p.read_text().strip() for p in group.rglob("cgroup.procs")):
            raise RuntimeError("qe_wsl_training_workers_not_drained")
    # Waiting wrappers have not entered a training scope yet, so inspect those too.
    for command in (root / "proc").glob("[0-9]*/cmdline"):
        try:
            content = command.read_bytes()
        except FileNotFoundError:
            continue
        if any(part.rsplit(b"/", 1)[-1] == b"qe_wsl_workload_guard.py" for part in content.split(b"\0")):
            raise RuntimeError("qe_wsl_waiting_guard_not_drained")
    after = UNITS[TRAINING_UNIT].encode()
    if before != after:
        temporary = path.with_name(path.name + ".BUG-2217.tmp")
        with temporary.open("xb") as stream:
            stream.write(after)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(path.stat().st_mode & 0o777)
        if path.read_bytes() != before:
            raise RuntimeError("qe_wsl_training_unit_changed_during_upgrade")
        os.replace(temporary, path)
    if path.read_bytes() != after:
        raise RuntimeError("qe_wsl_training_profile_readback_failed")
    return {
        "state": "TRAINING_UNIT_INSTALLED_LIVE_LIMITS_AND_LOCAL8001_ACTIVATION_PENDING",
        "path": TRAINING_UNIT,
        "before_sha256": expected_sha,
        "after_sha256": hashlib.sha256(after).hexdigest(),
        "limits": plan()["limits"],
        "service_control": False,
        "api_units_modified": False,
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
    p.add_argument("--upgrade-training-profile", action="store_true")
    p.add_argument("--expected-training-unit-sha256")
    args = p.parse_args()
    if args.upgrade_training_profile and args.expected_api_unit_sha256:
        p.error("training upgrade never changes the API unit")
    if args.apply and args.upgrade_training_profile and not args.expected_training_unit_sha256:
        p.error("training upgrade requires the inspected --expected-training-unit-sha256")
    if args.apply and not args.upgrade_training_profile and not args.expected_api_unit_sha256:
        p.error("--apply requires the inspected --expected-api-unit-sha256")
    if args.apply and args.upgrade_training_profile:
        result = upgrade_training_profile(args.expected_training_unit_sha256)
    else:
        result = install(args.expected_api_unit_sha256) if args.apply else plan()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
