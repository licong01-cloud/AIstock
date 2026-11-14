"""Node-local, locked memory admission and a systemd user scope per QE job.

Standard library only. Never starts/stops an API, edits a dataset, or kills a job.
"""

from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

GIB = 1024**3
SLICE = "qe-training.slice"
API_GROUP = "/qe.slice/qe-control.slice/rdagent-qe-api.service"
LIVE_STATES = {"active", "activating", "deactivating", "reloading"}
SCHEMA = "qe_wsl_workload_envelope_v2"
MEMORY_BYTES = 56 * GIB
SWAP_BYTES = 32 * GIB
JOB_SWAP_BYTES = 16 * GIB
HOST_RESERVE_BYTES = 4 * GIB
MAX_JOBS = 4
MAX_GPU_JOBS = 2
PEAK_BYTES = {"gpu": 32 * GIB, "cpu": 20 * GIB, "replay": 20 * GIB}


def show_unit(unit, env):
    r = subprocess.run(
        [
            "systemctl",
            "--user",
            "show",
            unit,
            "--property=ActiveState,ControlGroup,MemoryHigh,MemoryMax,MemorySwapMax,LoadState",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    return dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line)


def validate_slice(values):
    if values.get("LoadState") != "loaded" or values.get("MemoryHigh") != "infinity":
        raise RuntimeError("qe_wsl_training_slice_not_configured")
    if values.get("MemoryMax") != str(MEMORY_BYTES) or values.get("MemorySwapMax") != str(SWAP_BYTES):
        raise RuntimeError("qe_wsl_training_limits_mismatch")
    group = values.get("ControlGroup", "")
    if group and ("/user.slice/" not in group or not group.endswith("/qe-training.slice")):
        raise RuntimeError("qe_wsl_training_slice_not_independent")


def process_start(pid):
    try:
        # comm may contain whitespace or parentheses; fields start after its
        # final ')'. starttime is field22, index19 from field3.
        return Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def live_records(records, *, start_reader=process_start, unit_reader):
    live = {}
    for key, row in records.items():
        same_process = start_reader(row["pid"]) == row["start"]
        unit = unit_reader(row["unit"])
        if unit.get("ActiveState") in LIVE_STATES or same_process:
            live[key] = row
        elif unit.get("ActiveState") not in {"inactive", "failed"}:
            raise RuntimeError("qe_wsl_memory_lease_liveness_unknown")
    return live


def admits(records, requested, budget):
    if type(requested) is not int or requested <= 0 or requested > budget:
        raise RuntimeError("qe_wsl_invalid_peak_budget")
    if any(type(row.get("peak_bytes")) is not int or row["peak_bytes"] <= 0 for row in records.values()):
        raise RuntimeError("qe_wsl_corrupt_memory_lease")
    return sum(row["peak_bytes"] for row in records.values()) + requested <= budget


def host_admits(records, requested, available, training_current):
    unfilled_reservations = max(0, sum(row["peak_bytes"] for row in records.values()) - training_current)
    return available - unfilled_reservations - requested >= 4 * GIB


def validate_policy(policy):
    expected = {
        "schema": SCHEMA,
        "budget_bytes": MEMORY_BYTES + SWAP_BYTES,
        "resident_budget_bytes": MEMORY_BYTES,
        "swap_bytes": SWAP_BYTES,
        "job_swap_bytes": JOB_SWAP_BYTES,
    }
    if any(type(policy.get(k)) is not type(v) or policy[k] != v for k, v in expected.items()):
        raise RuntimeError("qe_wsl_unapproved_resource_profile")
    if policy.get("kind") not in PEAK_BYTES or type(policy.get("peak_bytes")) is not int:
        raise RuntimeError("qe_wsl_invalid_workload_kind")
    if policy["peak_bytes"] != PEAK_BYTES[policy["kind"]]:
        raise RuntimeError("qe_wsl_invalid_workload_reservation")


def admission_reason(records, policy, snapshot):
    """Bound swap separately; it never substitutes for the physical host reserve."""
    validate_policy(policy)
    for row in records.values():
        if row.get("kind") not in PEAK_BYTES or row.get("peak_bytes") != PEAK_BYTES[row["kind"]]:
            raise RuntimeError("qe_wsl_memory_lease_profile_mismatch_drain_required")
    integer_fields = ("available", "swap_free", "training_memory", "training_swap")
    if any(type(snapshot.get(k)) is not int or snapshot[k] < 0 for k in integer_fields):
        raise RuntimeError("qe_wsl_invalid_memory_snapshot")
    for key in ("memory_full_avg10", "io_full_avg10"):
        value = snapshot.get(key)
        if type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 100:
            raise RuntimeError("qe_wsl_invalid_pressure_snapshot")
    if len(records) >= MAX_JOBS:
        return "job_limit"
    if policy["kind"] == "gpu" and sum(r["kind"] == "gpu" for r in records.values()) >= MAX_GPU_JOBS:
        return "gpu_job_limit"
    if not admits(records, policy["peak_bytes"], policy["budget_bytes"]):
        return "bounded_memory_and_swap_budget"
    if snapshot["memory_full_avg10"] >= 10 or snapshot["io_full_avg10"] >= 20:
        return "sustained_memory_or_io_pressure"
    if snapshot["available"] < HOST_RESERVE_BYTES:
        return "physical_host_reserve"
    # Only unused QE swap allowance can back another reservation, never all WSL swap.
    swap_headroom = min(
        max(0, snapshot["swap_free"] - HOST_RESERVE_BYTES),
        max(0, SWAP_BYTES - snapshot["training_swap"]),
    )
    promised = sum(r["peak_bytes"] for r in records.values())
    unfilled = max(0, promised - snapshot["training_memory"] - snapshot["training_swap"])
    capacity = snapshot["available"] - HOST_RESERVE_BYTES + swap_headroom
    if capacity < unfilled + policy["peak_bytes"]:
        return "host_capacity_after_existing_reservations"
    return None


def pressure_full(path):
    for line in path.read_text().splitlines():
        fields = line.split()
        if fields and fields[0] == "full":
            return float(dict(item.split("=", 1) for item in fields[1:])["avg10"])
    raise RuntimeError("qe_wsl_pressure_full_unavailable")


def memory_snapshot(group):
    meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    def kib(name):
        return int(meminfo[name].split()[0]) * 1024
    if kib("MemTotal") < MEMORY_BYTES + 6 * GIB or kib("SwapTotal") < SWAP_BYTES + HOST_RESERVE_BYTES:
        raise RuntimeError("qe_wsl_host_too_small_for_approved_profile")
    cg = Path("/sys/fs/cgroup") / group.lstrip("/")
    return {
        "available": kib("MemAvailable"),
        "swap_free": kib("SwapFree"),
        "training_memory": int((cg / "memory.current").read_text()),
        "training_swap": int((cg / "memory.swap.current").read_text()),
        "memory_full_avg10": max(pressure_full(Path("/proc/pressure/memory")), pressure_full(cg / "memory.pressure")),
        "io_full_avg10": max(pressure_full(Path("/proc/pressure/io")), pressure_full(cg / "io.pressure")),
    }


def atomic_json(path, value):
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    with tmp.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def inside_scope():
    """Verify kernel enforcement before the original scientific command starts."""
    root = Path.cwd()
    policy = json.loads((root / "qe_wsl_workload_policy.json").read_text())
    validate_policy(policy)
    identity = policy["identity"]
    intent = json.loads((root / "qe_wsl_workload_launch_intent.json").read_text())
    incarnation = {"pid": os.getpid(), "start": process_start(os.getpid()), "pgid": os.getpgrp(), "sid": os.getsid(0)}
    if intent["process"] != incarnation:
        raise RuntimeError("qe_wsl_original_process_identity_changed")
    group = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]
    expected = "/qe-training.slice/qe-job-" + identity[:40] + ".scope"
    if "/user.slice/" not in group or not group.endswith(expected):
        raise RuntimeError("qe_wsl_scope_actual_identity_mismatch")
    cg = Path("/sys/fs/cgroup") / group.lstrip("/")
    actual = {k: (cg / k).read_text().strip() for k in ["memory.high", "memory.max", "memory.swap.max"]}
    parent = {k: (cg.parent / k).read_text().strip() for k in ["memory.high", "memory.max", "memory.swap.max"]}
    if actual != {"memory.high": "max", "memory.max": str(policy["peak_bytes"]), "memory.swap.max": str(JOB_SWAP_BYTES)}:
        raise RuntimeError("qe_wsl_scope_kernel_limits_mismatch")
    if parent != {"memory.high": "max", "memory.max": str(MEMORY_BYTES), "memory.swap.max": str(SWAP_BYTES)}:
        raise RuntimeError("qe_wsl_aggregate_kernel_limits_mismatch")
    command = root / "qe_wsl_workload_command.sh"
    if hashlib.sha256(command.read_bytes()).hexdigest() != policy["original_command_sha256"]:
        raise RuntimeError("qe_wsl_original_command_changed")
    with (root / "qe_wsl_workload_started.json").open("x") as f:
        json.dump(
            {
                "identity": identity,
                "pid": os.getpid(),
                "start": process_start(os.getpid()),
                "control_group": group,
                "kernel_limits": actual,
                "aggregate_limits": parent,
                "original_command_sha256": policy["original_command_sha256"],
            },
            f,
        )
    os.execv("/bin/bash", ["/bin/bash", "--noprofile", "--norc", str(command)])


def main():
    import fcntl

    root = Path.cwd()
    policy = json.loads((root / "qe_wsl_workload_policy.json").read_text())
    validate_policy(policy)
    if os.getpgrp() != os.getpid() or os.getsid(0) != os.getpid():
        raise RuntimeError("qe_wsl_requires_original_QE_process_session_leader")
    identity = policy["identity"]
    if len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
        raise RuntimeError("qe_wsl_invalid_job_identity")
    command = root / "qe_wsl_workload_command.sh"
    if hashlib.sha256(command.read_bytes()).hexdigest() != policy["original_command_sha256"]:
        raise RuntimeError("qe_wsl_original_command_changed")
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != policy["guard_sha256"]:
        raise RuntimeError("qe_wsl_guard_changed")
    env = dict(os.environ)
    runtime = Path(f"/run/user/{os.getuid()}")
    env.update(XDG_RUNTIME_DIR=str(runtime), DBUS_SESSION_BUS_ADDRESS=f"unix:path={runtime}/bus")
    # Existing API must have been activated separately by the operator. No
    # fallback to running an unisolated training subprocess is permitted.
    ancestor = os.getppid()
    api_found = False
    for _ in range(32):
        p = Path(f"/proc/{ancestor}")
        if not p.exists():
            break
        if (p / "cgroup").read_text().strip() == "0::" + API_GROUP:
            api_found = True
            break
        stat = (p / "stat").read_text().rsplit(")", 1)[1].split()
        ancestor = int(stat[1])
        if ancestor <= 1:
            break
    if not api_found:
        raise RuntimeError("qe_wsl_control_api_not_isolated")
    api_cgroup = Path("/sys/fs/cgroup") / API_GROUP.lstrip("/")
    limits = {k: (api_cgroup / k).read_text().strip() for k in ["memory.high", "memory.max", "memory.swap.max"]}
    if limits != {"memory.high": "max", "memory.max": str(2 * GIB), "memory.swap.max": str(GIB)}:
        raise RuntimeError("qe_wsl_control_api_limits_not_activated")
    validate_slice(show_unit(SLICE, env))
    state_dir = runtime / "aistock-qe-memory"
    state_dir.mkdir(mode=0o700, exist_ok=True)
    if state_dir.is_symlink() or state_dir.stat().st_uid != os.getuid():
        raise RuntimeError("qe_wsl_invalid_memory_state_owner")
    lease_path = state_dir / "leases.json"
    lock_path = state_dir / "leases.lock"
    unit = "qe-job-" + identity[:40] + ".scope"
    record = {"pid": os.getpid(), "start": process_start(os.getpid()), "unit": unit, "peak_bytes": policy["peak_bytes"], "kind": policy["kind"]}
    receipt_path = root / "qe_wsl_workload_receipt.json"
    while True:
        with lock_path.open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            records = json.loads(lease_path.read_text()) if lease_path.exists() else {}
            records = live_records(records, unit_reader=lambda u: show_unit(u, env))
            if identity in records:
                raise RuntimeError("qe_wsl_existing_lease_reconcile_only")
            group = show_unit(SLICE, env).get("ControlGroup", "")
            snapshot = memory_snapshot(group)
            reason = admission_reason(records, policy, snapshot)
            if reason is None:
                records[identity] = record
                atomic_json(lease_path, records)
                break
            atomic_json(lease_path, records)
        atomic_json(
            receipt_path,
            {"state": "WAITING_MEMORY", "reason": reason, "snapshot": snapshot, "identity": identity, "peak_bytes": policy["peak_bytes"], "at": time.time()},
        )
        time.sleep(5)
    # One immutable launch intent per workspace. A crash never permits this
    # guard to relaunch the scientific command as a second attempt.
    with (root / "qe_wsl_workload_launch_intent.json").open("x") as f:
        json.dump(
            {
                "identity": identity,
                "unit": unit,
                "command_sha256": policy["original_command_sha256"],
                "process": {
                    "pid": os.getpid(),
                    "start": process_start(os.getpid()),
                    "pgid": os.getpgrp(),
                    "sid": os.getsid(0),
                },
            },
            f,
        )
    cmd = [
        "systemd-run",
        "--user",
        "--scope",
        "--quiet",
        "--same-dir",
        f"--slice={SLICE}",
        f"--unit={unit}",
        "--property=MemoryHigh=infinity",
        f"--property=MemoryMax={policy['peak_bytes']}",
        f"--property=MemorySwapMax={JOB_SWAP_BYTES}",
        "--",
        "/usr/bin/python3",
        str(Path(__file__).resolve()),
        "--inside-scope",
    ]
    atomic_json(receipt_path, {"state": "SUBMITTING_SCOPE", "identity": identity, "unit": unit, "at": time.time()})
    # Exec from the API's existing session leader. Do not introduce a wrapper
    # parent or a second process group: typed QE PID/PGID/start-time cancellation
    # must still identify the actual scientific job. inside_scope re-verifies
    # that exact incarnation before any model/factor command is allowed.
    # Canonical QE completion provides the terminal receipt. A later admission
    # reclaims this lease only after both PID incarnation and scope are gone.
    os.execve("/usr/bin/systemd-run", cmd, env)


if __name__ == "__main__":
    if sys.argv[1:] == ["--inside-scope"]:
        inside_scope()
    elif not sys.argv[1:]:
        raise SystemExit(main())
    else:
        raise SystemExit("unsupported QE workload guard arguments")
